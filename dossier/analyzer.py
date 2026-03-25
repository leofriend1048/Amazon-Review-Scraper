"""
Creative Intelligence Dossier — Analysis Pipeline.

Phases:
1. Pre-processing: segment reviews, add metadata, weight by helpfulness
2. ML Layer: n-gram extraction, sentence embeddings, clustering
3. AI Layer: Claude API passes for deep psychological mining
4. Synthesis: combine all signals into the 10 dossier sections
"""

import os
os.environ["TOKENIZERS_PARALLELISM"] = "false"

import re
import json
import time
import logging
import sqlite3
from collections import Counter
from typing import List, Dict, Optional, Any
from concurrent.futures import ThreadPoolExecutor, as_completed

import numpy as np
import pandas as pd
from sklearn.feature_extraction.text import CountVectorizer

import anthropic

from . import prompts

logger = logging.getLogger(__name__)

# Max reviews to send per Claude API call
BATCH_SIZE = 250
# Max reviews for embedding (sample if larger)
MAX_EMBED_REVIEWS = 5000
# Helpful vote weight multiplier
HELPFUL_WEIGHT = 3


def _truncate_reviews_for_prompt(reviews_df: pd.DataFrame, max_chars: int = 80000) -> str:
    """Format reviews for a prompt, truncating to fit context window."""
    lines = []
    total = 0
    # Prioritize high-helpful reviews
    sorted_df = reviews_df.sort_values("helpful_votes", ascending=False)
    for _, row in sorted_df.iterrows():
        line = (
            f"[{row['rating']}* | {row.get('helpful_votes', 0)} helpful | "
            f"ID:{row['review_id'][:12]}] "
            f"{row.get('title', '')} — {row.get('body', '')}"
        )
        if total + len(line) > max_chars:
            break
        lines.append(line)
        total += len(line)
    return "\n\n".join(lines)


def _safe_json_parse(text: str) -> dict:
    """Parse JSON from Claude response, handling markdown code fences and edge cases."""
    text = text.strip()
    # Strip markdown code fences
    if "```" in text:
        text = re.sub(r"```(?:json)?\s*\n?", "", text)
        text = text.strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass

    # Try to find the outermost JSON object by matching braces
    depth = 0
    start = None
    for i, c in enumerate(text):
        if c == '{':
            if depth == 0:
                start = i
            depth += 1
        elif c == '}':
            depth -= 1
            if depth == 0 and start is not None:
                try:
                    return json.loads(text[start:i+1])
                except json.JSONDecodeError:
                    start = None

    # Last resort: try regex for largest JSON block
    match = re.search(r'\{[\s\S]*\}', text)
    if match:
        try:
            return json.loads(match.group())
        except json.JSONDecodeError:
            pass

    logger.warning("Failed to parse JSON from Claude response")
    return {"raw_response": text}


class DossierAnalyzer:
    """
    Full analysis pipeline for generating a Creative Intelligence Dossier.

    Usage:
        analyzer = DossierAnalyzer("B08N5WRWNW")
        results = analyzer.run_full_analysis()
    """

    def __init__(self, asin: str, db_path: str = None,
                 model: str = "sonnet", progress_callback=None):
        self.asin = asin.upper().strip()
        self.model = model
        self.progress_callback = progress_callback

        # Resolve model names
        self._bulk_model = "claude-sonnet-4-20250514"
        self._synth_model = (
            "claude-sonnet-4-20250514" if model == "opus"
            else "claude-sonnet-4-20250514"
        )

        # Load reviews from SQLite
        if db_path is None:
            db_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data")
            db_path = os.path.join(db_dir, f"{self.asin}.db")

        if not os.path.exists(db_path):
            raise FileNotFoundError(f"No review database found at {db_path}")

        self.conn = sqlite3.connect(db_path)
        self.df = pd.read_sql_query(
            "SELECT * FROM reviews WHERE asin = ? ORDER BY helpful_votes DESC",
            self.conn, params=[self.asin]
        )

        if len(self.df) == 0:
            raise ValueError(f"No reviews found for ASIN {self.asin}")

        logger.info(f"Loaded {len(self.df)} reviews for {self.asin}")

        # Load meta
        try:
            meta = pd.read_sql_query(
                "SELECT * FROM scrape_meta WHERE asin = ?",
                self.conn, params=[self.asin]
            )
            self.meta = meta.iloc[0].to_dict() if len(meta) > 0 else {}
        except Exception:
            self.meta = {}

        # Initialize Claude client
        self.client = anthropic.Anthropic()

        # Storage for intermediate results
        self._results = {}

    def _progress(self, phase: str, step: str, pct: float = 0):
        """Report progress."""
        if self.progress_callback:
            self.progress_callback(phase, step, pct)
        logger.info(f"[{phase}] {step}")

    # ── Phase 1: Pre-processing ───────────────────────────────────────

    def _segment_reviews(self) -> Dict[int, pd.DataFrame]:
        """Split reviews into 5 star-based psychological cohorts."""
        cohorts = {}
        for star in [1, 2, 3, 4, 5]:
            cohort = self.df[self.df["rating"] == star].copy()
            cohorts[star] = cohort
            logger.info(f"  {star}-star: {len(cohort)} reviews")
        return cohorts

    def _add_metadata(self):
        """Add computed columns for analysis."""
        # Review length category
        self.df["body_length"] = self.df["body"].fillna("").str.len()
        self.df["length_category"] = pd.cut(
            self.df["body_length"],
            bins=[0, 100, 300, 1000, float("inf")],
            labels=["short", "medium", "long", "very_long"]
        )

        # Has comparison to another product
        comparison_pattern = r"(?i)(?:compared to|vs\.?|better than|worse than|switched from|unlike|competitor)"
        self.df["has_comparison"] = self.df["body"].fillna("").str.contains(
            comparison_pattern, regex=True
        )

        # Has "tried everything" signal
        tried_pattern = r"(?i)(?:tried everything|tried .* products|nothing worked|last resort|gave up|desperate)"
        self.df["has_tried_everything"] = self.df["body"].fillna("").str.contains(
            tried_pattern, regex=True
        )

        # Helpful weight (for prioritization)
        self.df["helpful_weight"] = 1 + (
            self.df["helpful_votes"].fillna(0).clip(upper=50) * (HELPFUL_WEIGHT - 1) / 50
        )

    # ── Phase 2: ML Layer ─────────────────────────────────────────────

    def _run_ngram_analysis(self, cohorts: dict) -> dict:
        """Extract bigrams and trigrams per star cohort."""
        self._progress("ML", "Running n-gram analysis...")
        results = {}

        for star, cohort_df in cohorts.items():
            texts = cohort_df["body"].fillna("").tolist()
            if len(texts) < 5:
                results[star] = {"bigrams": [], "trigrams": []}
                continue

            # Bigrams
            try:
                bi_vec = CountVectorizer(
                    ngram_range=(2, 2), stop_words="english",
                    max_features=100, min_df=2
                )
                bi_matrix = bi_vec.fit_transform(texts)
                bi_freqs = zip(bi_vec.get_feature_names_out(), bi_matrix.sum(axis=0).A1)
                bigrams = sorted(bi_freqs, key=lambda x: x[1], reverse=True)[:30]
            except Exception:
                bigrams = []

            # Trigrams
            try:
                tri_vec = CountVectorizer(
                    ngram_range=(3, 3), stop_words="english",
                    max_features=100, min_df=2
                )
                tri_matrix = tri_vec.fit_transform(texts)
                tri_freqs = zip(tri_vec.get_feature_names_out(), tri_matrix.sum(axis=0).A1)
                trigrams = sorted(tri_freqs, key=lambda x: x[1], reverse=True)[:30]
            except Exception:
                trigrams = []

            results[star] = {
                "bigrams": [(phrase, int(count)) for phrase, count in bigrams],
                "trigrams": [(phrase, int(count)) for phrase, count in trigrams],
            }

        return results

    def _run_clustering(self, cohorts: dict) -> dict:
        """Sentence embedding + UMAP + HDBSCAN for micro-segment discovery."""
        self._progress("ML", "Running semantic clustering...")

        try:
            from sentence_transformers import SentenceTransformer
            import umap
            import hdbscan
        except ImportError as e:
            logger.warning(f"Clustering dependencies missing: {e}")
            return {"clusters": [], "error": str(e)}

        # Sample reviews for embedding
        texts = self.df["body"].fillna("").tolist()
        ids = self.df["review_id"].tolist()
        ratings = self.df["rating"].tolist()

        if len(texts) > MAX_EMBED_REVIEWS:
            indices = np.random.choice(len(texts), MAX_EMBED_REVIEWS, replace=False)
            texts = [texts[i] for i in indices]
            ids = [ids[i] for i in indices]
            ratings = [ratings[i] for i in indices]

        # Filter out very short reviews
        valid = [(t, i, r) for t, i, r in zip(texts, ids, ratings) if len(t) > 30]
        if len(valid) < 50:
            return {"clusters": [], "note": "Too few reviews for meaningful clustering"}

        texts, ids, ratings = zip(*valid)

        self._progress("ML", f"Embedding {len(texts)} reviews...")
        model = SentenceTransformer("all-MiniLM-L6-v2")
        embeddings = model.encode(list(texts), show_progress_bar=False, batch_size=256)

        self._progress("ML", "Running UMAP dimensionality reduction...")
        reducer = umap.UMAP(
            n_components=10, n_neighbors=15, min_dist=0.0,
            metric="cosine", random_state=42
        )
        reduced = reducer.fit_transform(embeddings)

        self._progress("ML", "Running HDBSCAN clustering...")
        clusterer = hdbscan.HDBSCAN(
            min_cluster_size=max(10, len(texts) // 100),
            min_samples=5, metric="euclidean"
        )
        labels = clusterer.fit_predict(reduced)

        # Analyze clusters
        clusters = []
        unique_labels = set(labels)
        unique_labels.discard(-1)  # Remove noise label

        for label in sorted(unique_labels):
            mask = labels == label
            cluster_texts = [texts[i] for i in range(len(texts)) if mask[i]]
            cluster_ratings = [ratings[i] for i in range(len(texts)) if mask[i]]
            cluster_ids = [ids[i] for i in range(len(texts)) if mask[i]]

            # Get representative reviews (closest to centroid)
            cluster_embeddings = embeddings[mask]
            centroid = cluster_embeddings.mean(axis=0)
            dists = np.linalg.norm(cluster_embeddings - centroid, axis=1)
            top_indices = dists.argsort()[:3]

            clusters.append({
                "cluster_id": int(label),
                "size": int(mask.sum()),
                "avg_rating": round(float(np.mean(cluster_ratings)), 2),
                "representative_reviews": [cluster_texts[i][:300] for i in top_indices],
                "representative_ids": [cluster_ids[i] for i in top_indices],
            })

        clusters.sort(key=lambda c: c["size"], reverse=True)

        return {
            "clusters": clusters[:20],  # Top 20 clusters
            "total_clustered": int((labels != -1).sum()),
            "noise_count": int((labels == -1).sum()),
            "num_clusters": len(clusters),
        }

    # ── Phase 3: Claude API Layer ─────────────────────────────────────

    def _call_claude(self, prompt: str, model: str = None, max_tokens: int = 4096) -> dict:
        """Call Claude API and parse JSON response."""
        model = model or self._bulk_model
        for attempt in range(3):
            try:
                # Prepend system instruction to ensure clean JSON
                response = self.client.messages.create(
                    model=model,
                    max_tokens=max_tokens,
                    system="You are a data analyst. Always respond with valid JSON only. No markdown, no code fences, no explanatory text before or after the JSON. Start your response with { and end with }.",
                    messages=[{"role": "user", "content": prompt}],
                )
                text = response.content[0].text
                return _safe_json_parse(text)
            except anthropic.RateLimitError:
                wait = (attempt + 1) * 30
                logger.warning(f"Rate limited, waiting {wait}s...")
                time.sleep(wait)
            except Exception as e:
                logger.error(f"Claude API error (attempt {attempt+1}): {e}")
                if attempt < 2:
                    time.sleep(5)
        return {"error": "API call failed after 3 attempts"}

    def _run_nightmare_mining(self, cohorts: dict) -> dict:
        """Mine 1-2 star reviews for nightmare scenarios."""
        self._progress("AI", "Mining nightmare scenarios from negative reviews...")
        negative = pd.concat([cohorts.get(1, pd.DataFrame()), cohorts.get(2, pd.DataFrame())])
        if len(negative) == 0:
            return {"error": "No 1-2 star reviews found"}

        reviews_text = _truncate_reviews_for_prompt(negative)
        prompt = prompts.NIGHTMARE_MINING_PROMPT.format(
            asin=self.asin,
            review_count=len(negative),
            reviews=reviews_text,
        )
        return self._call_claude(prompt)

    def _run_transformation_mining(self, cohorts: dict) -> dict:
        """Mine 5-star reviews for transformation language."""
        self._progress("AI", "Mining transformation language from 5-star reviews...")
        five_star = cohorts.get(5, pd.DataFrame())
        if len(five_star) == 0:
            return {"error": "No 5-star reviews found"}

        # Prioritize high-helpful reviews
        five_star = five_star.sort_values("helpful_votes", ascending=False)
        reviews_text = _truncate_reviews_for_prompt(five_star)
        prompt = prompts.TRANSFORMATION_MINING_PROMPT.format(
            asin=self.asin,
            review_count=len(five_star),
            reviews=reviews_text,
        )
        return self._call_claude(prompt)

    def _run_objection_archaeology(self, cohorts: dict) -> dict:
        """Mine 3-star reviews for objections."""
        self._progress("AI", "Mining objections from 3-star reviews...")
        three_star = cohorts.get(3, pd.DataFrame())
        if len(three_star) == 0:
            return {"error": "No 3-star reviews found"}

        reviews_text = _truncate_reviews_for_prompt(three_star)
        prompt = prompts.OBJECTION_ARCHAEOLOGY_PROMPT.format(
            asin=self.asin,
            review_count=len(three_star),
            reviews=reviews_text,
        )
        return self._call_claude(prompt)

    def _run_comparison_mining(self) -> dict:
        """Mine comparison reviews for competitive intelligence."""
        self._progress("AI", "Mining competitive intelligence...")
        comparison_reviews = self.df[self.df["has_comparison"] == True]
        if len(comparison_reviews) == 0:
            return {"error": "No comparison reviews found"}

        reviews_text = _truncate_reviews_for_prompt(comparison_reviews)
        prompt = prompts.COMPARISON_INTELLIGENCE_PROMPT.format(
            asin=self.asin,
            review_count=len(comparison_reviews),
            reviews=reviews_text,
        )
        return self._call_claude(prompt)

    def _run_four_star_analysis(self, cohorts: dict) -> dict:
        """Analyze 4-star reviews for hidden objections."""
        self._progress("AI", "Analyzing 4-star 'almost perfect' reviews...")
        four_star = cohorts.get(4, pd.DataFrame())
        if len(four_star) == 0:
            return {}

        reviews_text = _truncate_reviews_for_prompt(four_star, max_chars=40000)
        prompt = f"""Analyze these 4-star reviews. They represent satisfied but not evangelical buyers.
Identify: (1) What stopped them from giving 5 stars? (2) The hidden objection in each.
(3) "Almost perfect" language patterns.

ASIN: {self.asin}
REVIEWS ({len(four_star)}):
{reviews_text}

Return JSON: {{"hidden_objections": ["..."], "almost_perfect_phrases": ["..."], "missing_piece": "..."}}"""
        return self._call_claude(prompt)

    # ── Phase 4: Synthesis ────────────────────────────────────────────

    def _synthesize_market_snapshot(self, ngrams: dict, nightmare: dict,
                                     transformation: dict, objection: dict) -> dict:
        self._progress("Synthesis", "Generating Market Snapshot...")

        star_dist = {s: len(self.df[self.df["rating"] == s]) for s in range(1, 6)}
        five_star_phrases = [p for p, c in ngrams.get(5, {}).get("trigrams", [])[:10]]
        one_star_phrases = [p for p, c in ngrams.get(1, {}).get("trigrams", [])[:10]]

        prompt = prompts.MARKET_SNAPSHOT_PROMPT.format(
            asin=self.asin,
            total_reviews=len(self.df),
            avg_rating=round(self.df["rating"].mean(), 2),
            star_distribution=json.dumps(star_dist),
            top_themes="(derived from n-gram clusters)",
            five_star_phrases=json.dumps(five_star_phrases),
            one_star_phrases=json.dumps(one_star_phrases),
            root_emotion=nightmare.get("root_emotion", "unknown"),
            transformation_outcomes=json.dumps(
                [w.get("outcome", "") for w in transformation.get("unexpected_wins", [])[:5]]
            ),
            key_objections=json.dumps(
                [o.get("objection", "") for o in objection.get("objections", [])[:5]]
            ),
        )
        return self._call_claude(prompt, model=self._synth_model)

    def _synthesize_avatar_monologue(self, nightmare: dict, transformation: dict,
                                       objection: dict, market_snapshot: dict) -> dict:
        self._progress("Synthesis", "Writing Avatar Monologue...")

        prompt = prompts.AVATAR_MONOLOGUE_PROMPT.format(
            asin=self.asin,
            problem_phrases=json.dumps(nightmare.get("failed_solution_phrases", [])[:10]),
            failed_solution_phrases=json.dumps(nightmare.get("failed_solution_phrases", [])[:10]),
            desired_outcome_phrases=json.dumps(
                [p.get("after", "") for p in transformation.get("before_after_pairs", [])[:10]]
            ),
            identity_language=json.dumps(
                [s.get("new_identity", "") for s in transformation.get("identity_shifts", [])[:5]]
            ),
            skepticism_phrases=json.dumps(
                objection.get("qualification_phrases", [])[:10]
            ),
            root_emotion=nightmare.get("root_emotion", market_snapshot.get("dominant_emotional_current", "")),
            before_state=market_snapshot.get("before_state", ""),
            after_state=market_snapshot.get("after_state", ""),
        )
        return self._call_claude(prompt, model=self._synth_model, max_tokens=2048)

    def _synthesize_language_bible(self, ngrams: dict) -> dict:
        self._progress("Synthesis", "Compiling Language Bible...")

        # Prepare n-gram data
        ngram_text = ""
        for star in [1, 2, 3, 4, 5]:
            data = ngrams.get(star, {})
            ngram_text += f"\n{star}-STAR TRIGRAMS: {json.dumps(data.get('trigrams', [])[:15])}\n"
            ngram_text += f"{star}-STAR BIGRAMS: {json.dumps(data.get('bigrams', [])[:15])}\n"

        # Sample reviews across ratings
        samples = []
        for star in range(1, 6):
            cohort = self.df[self.df["rating"] == star]
            samples.append(cohort.nlargest(min(20, len(cohort)), "helpful_votes"))
        sample = pd.concat(samples).reset_index(drop=True)
        reviews_text = _truncate_reviews_for_prompt(sample, max_chars=60000)

        prompt = prompts.LANGUAGE_BIBLE_PROMPT.format(
            asin=self.asin,
            ngram_data=ngram_text,
            reviews=reviews_text,
        )
        return self._call_claude(prompt, max_tokens=8192)

    def _synthesize_headline_bank(self, language_bible: dict, nightmare: dict,
                                    transformation: dict, ngrams: dict) -> dict:
        self._progress("Synthesis", "Building Headline Bank (50 candidates)...")

        ngram_text = ""
        for star in [1, 5]:
            data = ngrams.get(star, {})
            ngram_text += f"{star}-star trigrams: {json.dumps(data.get('trigrams', [])[:10])}\n"

        prompt = prompts.HEADLINE_BANK_PROMPT.format(
            asin=self.asin,
            language_bible=json.dumps(language_bible, default=str)[:8000],
            nightmare_data=json.dumps(nightmare, default=str)[:4000],
            transformation_data=json.dumps(transformation, default=str)[:4000],
            ngram_data=ngram_text,
        )
        return self._call_claude(prompt, max_tokens=8192)

    def _synthesize_objection_sequence(self, objection: dict, nightmare: dict,
                                        four_star: dict) -> dict:
        self._progress("Synthesis", "Mapping Objection Sequence...")

        prompt = prompts.OBJECTION_SEQUENCE_PROMPT.format(
            asin=self.asin,
            objection_data=json.dumps(objection, default=str)[:6000],
            nightmare_data=json.dumps(nightmare, default=str)[:4000],
            skepticism_phrases=json.dumps(objection.get("qualification_phrases", [])),
            four_star_data=json.dumps(four_star, default=str)[:3000],
        )
        return self._call_claude(prompt, model=self._synth_model)

    def _synthesize_angle_matrix(self, language_bible: dict, headline_bank: dict,
                                   transformation: dict, nightmare: dict) -> dict:
        self._progress("Synthesis", "Building Angle Matrix...")

        prompt = prompts.ANGLE_MATRIX_PROMPT.format(
            asin=self.asin,
            language_bible=json.dumps(language_bible, default=str)[:6000],
            headline_data=json.dumps(
                headline_bank.get("headlines", [])[:15], default=str
            )[:4000],
            transformation_data=json.dumps(transformation, default=str)[:3000],
            nightmare_data=json.dumps(nightmare, default=str)[:3000],
        )
        return self._call_claude(prompt, model=self._synth_model)

    def _synthesize_proof_architecture(self, transformation: dict, objection: dict) -> dict:
        self._progress("Synthesis", "Building Proof Architecture...")

        # Get conversion stories (5-star reviews mentioning skepticism)
        skeptic_converts = self.df[
            (self.df["rating"] == 5) &
            (self.df["body"].fillna("").str.contains(
                r"(?i)(?:skeptic|hesitant|didn.t think|almost didn.t|surprised|wasn.t sure)",
                regex=True
            ))
        ]
        convert_text = _truncate_reviews_for_prompt(skeptic_converts, max_chars=20000)

        prompt = prompts.PROOF_ARCHITECTURE_PROMPT.format(
            asin=self.asin,
            conversion_stories=convert_text,
            skepticism_phrases=json.dumps(objection.get("qualification_phrases", [])),
            transformation_data=json.dumps(transformation, default=str)[:4000],
        )
        return self._call_claude(prompt, model=self._synth_model)

    def _synthesize_competitive_map(self, comparison: dict, ngrams: dict) -> dict:
        self._progress("Synthesis", "Building Competitive Positioning Map...")

        one_star_themes = ngrams.get(1, {}).get("trigrams", [])[:10]

        prompt = prompts.COMPETITIVE_MAP_PROMPT.format(
            asin=self.asin,
            comparison_data=json.dumps(comparison, default=str)[:6000],
            one_star_themes=json.dumps(one_star_themes),
            competitor_mentions=json.dumps(
                comparison.get("comparisons", [])[:10], default=str
            ),
        )
        return self._call_claude(prompt, model=self._synth_model)

    def _synthesize_conversion_blueprint(self, cohorts: dict, objection: dict) -> dict:
        self._progress("Synthesis", "Building 3-to-5 Star Conversion Blueprint...")

        three_star = cohorts.get(3, pd.DataFrame())
        five_star = cohorts.get(5, pd.DataFrame())

        prompt = prompts.CONVERSION_BLUEPRINT_PROMPT.format(
            asin=self.asin,
            three_star_reviews=_truncate_reviews_for_prompt(three_star, max_chars=30000),
            five_star_reviews=_truncate_reviews_for_prompt(five_star, max_chars=30000),
            objection_data=json.dumps(objection, default=str)[:4000],
        )
        return self._call_claude(prompt, model=self._synth_model)

    def _synthesize_creative_briefs(self, market_snapshot: dict, headline_bank: dict,
                                      angle_matrix: dict, objection_seq: dict,
                                      proof_arch: dict, avatar: dict) -> dict:
        self._progress("Synthesis", "Writing 3 Swipe-Ready Creative Briefs...")

        prompt = prompts.CREATIVE_BRIEFS_PROMPT.format(
            asin=self.asin,
            market_snapshot=json.dumps(market_snapshot, default=str)[:3000],
            top_headlines=json.dumps(
                headline_bank.get("headlines", [])[:10], default=str
            )[:3000],
            angle_matrix=json.dumps(
                angle_matrix.get("matrix", [])[:6], default=str
            )[:3000],
            objection_sequence=json.dumps(
                objection_seq.get("objection_sequence", []), default=str
            )[:3000],
            proof_architecture=json.dumps(proof_arch, default=str)[:2000],
            avatar_themes=json.dumps(avatar.get("key_phrases_used", []), default=str)[:2000],
        )
        return self._call_claude(prompt, model=self._synth_model, max_tokens=4096)

    # ── Main Pipeline ─────────────────────────────────────────────────

    def run_full_analysis(self) -> dict:
        """Run all analysis phases and return structured results for rendering."""

        self._progress("Setup", "Pre-processing reviews...")

        # Phase 1: Pre-processing
        self._add_metadata()
        cohorts = self._segment_reviews()

        # Phase 2: ML Layer
        self._progress("ML", "Starting ML analysis...")
        ngrams = self._run_ngram_analysis(cohorts)
        clustering = self._run_clustering(cohorts)

        # Phase 3: AI Analysis (parallel where possible)
        self._progress("AI", "Starting AI analysis passes...")

        # Run the 4 independent mining passes in parallel
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = {
                executor.submit(self._run_nightmare_mining, cohorts): "nightmare",
                executor.submit(self._run_transformation_mining, cohorts): "transformation",
                executor.submit(self._run_objection_archaeology, cohorts): "objection",
                executor.submit(self._run_comparison_mining): "comparison",
            }
            ai_results = {}
            for future in as_completed(futures):
                key = futures[future]
                try:
                    ai_results[key] = future.result()
                except Exception as e:
                    logger.error(f"AI pass '{key}' failed: {e}")
                    ai_results[key] = {"error": str(e)}

        nightmare = ai_results.get("nightmare", {})
        transformation = ai_results.get("transformation", {})
        objection = ai_results.get("objection", {})
        comparison = ai_results.get("comparison", {})

        # 4-star analysis
        four_star = self._run_four_star_analysis(cohorts)

        # Phase 4: Synthesis (sequential — each builds on previous)
        self._progress("Synthesis", "Starting synthesis...")

        # Section 1: Market Snapshot
        market_snapshot = self._synthesize_market_snapshot(
            ngrams, nightmare, transformation, objection
        )

        # Section 2: Avatar Monologue
        avatar = self._synthesize_avatar_monologue(
            nightmare, transformation, objection, market_snapshot
        )

        # Section 3: Language Bible
        language_bible = self._synthesize_language_bible(ngrams)

        # Section 4: Headline Bank
        headline_bank = self._synthesize_headline_bank(
            language_bible, nightmare, transformation, ngrams
        )

        # Section 5: Objection Sequence
        objection_seq = self._synthesize_objection_sequence(
            objection, nightmare, four_star
        )

        # Section 6: Angle Matrix
        angle_matrix = self._synthesize_angle_matrix(
            language_bible, headline_bank, transformation, nightmare
        )

        # Section 7: Proof Architecture
        proof_arch = self._synthesize_proof_architecture(transformation, objection)

        # Section 8: Competitive Map
        competitive_map = self._synthesize_competitive_map(comparison, ngrams)

        # Section 9: 3→5 Star Conversion Blueprint
        conversion_blueprint = self._synthesize_conversion_blueprint(cohorts, objection)

        # Section 10: Creative Briefs
        creative_briefs = self._synthesize_creative_briefs(
            market_snapshot, headline_bank, angle_matrix,
            objection_seq, proof_arch, avatar
        )

        self._progress("Done", "Analysis complete!")

        # Compile final results
        star_dist = {s: int(len(self.df[self.df["rating"] == s])) for s in range(1, 6)}

        return {
            "asin": self.asin,
            "total_reviews": len(self.df),
            "avg_rating": round(float(self.df["rating"].mean()), 2),
            "star_distribution": star_dist,
            "meta": self.meta,

            # ML results
            "ngrams": ngrams,
            "clustering": clustering,

            # AI analysis
            "nightmare_mining": nightmare,
            "transformation_mining": transformation,
            "objection_archaeology": objection,
            "comparison_intelligence": comparison,
            "four_star_analysis": four_star,

            # Synthesized sections
            "sections": {
                "market_snapshot": market_snapshot,
                "avatar_monologue": avatar,
                "language_bible": language_bible,
                "headline_bank": headline_bank,
                "objection_sequence": objection_seq,
                "angle_matrix": angle_matrix,
                "proof_architecture": proof_arch,
                "competitive_map": competitive_map,
                "conversion_blueprint": conversion_blueprint,
                "creative_briefs": creative_briefs,
            }
        }

    def close(self):
        """Clean up database connection."""
        try:
            self.conn.close()
        except Exception:
            pass
