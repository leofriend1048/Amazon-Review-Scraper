"""
Claude API prompt templates for the Creative Intelligence Dossier.

Each prompt is designed for a specific analysis pass over review data.
Prompts use {placeholders} for review data injection.
"""

# ── Phase 3: AI Analysis Passes ──────────────────────────────────────

NIGHTMARE_MINING_PROMPT = """You are a world-class direct response copywriter analyzing negative product reviews.

PRODUCT ASIN: {asin}
REVIEW COHORT: 1-star and 2-star reviews (maximum frustration / specific disappointment)

Your job is NOT to summarize these reviews. Your job is to identify:

1. **The exact internal monologue of a frustrated buyer** — what do they tell themselves about this product and about their own situation? Write it as stream-of-consciousness.

2. **The specific promise they believed before buying** — what marketing claim or expectation led them to purchase? What did they think would happen?

3. **The nightmare scenario** — what is the worst-case outcome these buyers experienced? Not just "it didn't work" — the specific, visceral, embarrassing, or costly consequence.

4. **Failed solution patterns** — what phrases do they use to describe things that didn't work? ("I wasted," "I tried everything," "it just sits there," etc.)

5. **Five candidate headlines** — sentences from these reviews that, if used as a headline, would make another skeptical buyer stop scrolling and say "that's exactly my fear." For each, explain the psychological mechanism it activates.

Format your response as JSON:
{{
    "internal_monologue": "...",
    "believed_promises": ["..."],
    "nightmare_scenarios": ["..."],
    "failed_solution_phrases": ["..."],
    "headline_candidates": [
        {{"headline": "...", "source_snippet": "...", "psychological_mechanism": "..."}}
    ],
    "root_emotion": "The single deepest emotion driving these complaints (not surface anger — the root)"
}}

REVIEWS ({review_count} reviews):
{reviews}"""

TRANSFORMATION_MINING_PROMPT = """You are a world-class direct response copywriter analyzing the most helpful 5-star product reviews.

PRODUCT ASIN: {asin}
REVIEW COHORT: 5-star reviews with highest helpful votes (these are the market's curated wisdom — hundreds of people agreed these capture their experience)

Ignore generic praise ("great product!", "love it!"). Extract ONLY:

1. **Before/After Language** — exact phrases describing the state BEFORE using the product and AFTER. Not your words — their words.

2. **Identity Shift Language** — moments where the reviewer describes themselves differently as a result of the product. "I used to be..." → "Now I..." patterns.

3. **Unexpected Win Language** — outcomes they got that they didn't expect and were delighted by. These are your best ad angles because they're genuine surprise.

4. **Transformation Timeline** — how long did it take? What was the "moment of knowing" when they realized it worked?

5. **Evangelism Triggers** — what specifically made them write a review? What pushed them from satisfied to evangelical?

Format your response as JSON:
{{
    "before_after_pairs": [
        {{"before": "...", "after": "...", "source_review_snippet": "..."}}
    ],
    "identity_shifts": [
        {{"old_identity": "...", "new_identity": "...", "source_review_snippet": "..."}}
    ],
    "unexpected_wins": [
        {{"outcome": "...", "source_review_snippet": "..."}}
    ],
    "transformation_timeline": {{
        "typical_timeframe": "...",
        "moment_of_knowing_phrases": ["..."]
    }},
    "evangelism_triggers": ["..."]
}}

REVIEWS ({review_count} reviews):
{reviews}"""

OBJECTION_ARCHAEOLOGY_PROMPT = """You are a world-class direct response copywriter analyzing 3-star product reviews.

PRODUCT ASIN: {asin}
REVIEW COHORT: 3-star reviews — people who are genuinely conflicted. Something was good, something wasn't. These are the most analytically rich reviews in any dataset.

Identify:

1. **The Exact Objection** — what specific thing stopped this from being a 5-star experience? Not vague — the precise friction point.

2. **Qualification Language** — how they hedge their recommendation. "It's good BUT..." / "I like it, however..." / "Would be great if..." — these are your objection map.

3. **The 3→5 Star Gap** — what would they have needed to see, feel, know, or experience to push them to 5 stars? This is your conversion blueprint.

4. **Expectation Mismatches** — where did reality diverge from what they expected? These are landing page / ad messaging failures.

5. **The "Almost" Phrases** — "almost perfect," "nearly great," "so close" — what's the missing piece?

Format your response as JSON:
{{
    "objections": [
        {{"objection": "...", "frequency": "how_common", "source_snippet": "...", "severity": "high/medium/low"}}
    ],
    "qualification_phrases": ["..."],
    "three_to_five_gap": [
        {{"what_was_missing": "...", "what_would_fix_it": "...", "source_snippet": "..."}}
    ],
    "expectation_mismatches": [
        {{"expected": "...", "got": "...", "source_snippet": "..."}}
    ],
    "almost_phrases": ["..."]
}}

REVIEWS ({review_count} reviews):
{reviews}"""

COMPARISON_INTELLIGENCE_PROMPT = """You are a competitive intelligence analyst examining product reviews that mention competitors or comparisons.

PRODUCT ASIN: {asin}

Extract comparison claims being made. For each:
- What attribute was compared (price, results, ease of use, durability, etc.)
- Who won in the reviewer's mind
- What language did they use to explain why
- What this reveals about unmet needs in the market

Also identify:
1. **Competitor weaknesses** mentioned (from reviews of THIS product that switched from competitors)
2. **This product's advantages** that reviewers highlight vs alternatives
3. **Unfulfilled promises** from competitors that created the opportunity for this product
4. **Category fatigue language** — signs the buyer is exhausted with the whole category

Format as JSON:
{{
    "comparisons": [
        {{
            "competitor_or_alternative": "...",
            "attribute_compared": "...",
            "winner": "this_product/competitor/tie",
            "reviewer_language": "...",
            "source_snippet": "..."
        }}
    ],
    "competitor_weaknesses": [
        {{"competitor": "...", "weakness": "...", "frequency": "...", "source_snippet": "..."}}
    ],
    "this_product_advantages": ["..."],
    "unfulfilled_competitor_promises": ["..."],
    "category_fatigue_signals": ["..."]
}}

REVIEWS WITH COMPARISONS ({review_count} reviews):
{reviews}"""

# ── Phase 4: Synthesis Prompts ────────────────────────────────────────

MARKET_SNAPSHOT_PROMPT = """You are creating the executive Market Snapshot for a Creative Intelligence Dossier.

PRODUCT ASIN: {asin}
TOTAL REVIEWS ANALYZED: {total_reviews}
AVERAGE RATING: {avg_rating}
STAR DISTRIBUTION: {star_distribution}

ML ANALYSIS RESULTS:
- Top themes from topic modeling: {top_themes}
- Most frequent phrases (5-star): {five_star_phrases}
- Most frequent phrases (1-star): {one_star_phrases}

AI ANALYSIS RESULTS:
- Root emotion from negative reviews: {root_emotion}
- Top transformation outcomes: {transformation_outcomes}
- Key objections: {key_objections}

Based on this data, produce:

1. **Product Category + Awareness Stage** (1-5 on Schwartz awareness scale) with a 2-sentence justification
2. **The Dominant Emotional Current** — not the surface emotion, the ROOT one running through this market
3. **Before State** — one sentence describing the customer's life before this product
4. **After State** — one sentence describing the customer's life after
5. **3 Conviction Beliefs** — beliefs this customer holds with absolute conviction before seeing any ad
6. **#1 Category Distrust Factor** — what has made her distrust this entire category

Format as JSON:
{{
    "product_category": "...",
    "awareness_stage": 3,
    "awareness_justification": "...",
    "dominant_emotional_current": "...",
    "before_state": "...",
    "after_state": "...",
    "conviction_beliefs": ["...", "...", "..."],
    "category_distrust_factor": "..."
}}"""

AVATAR_MONOLOGUE_PROMPT = """You are writing the Avatar Monologue for a Creative Intelligence Dossier. This is the single most important piece of creative infrastructure in the entire document.

Write a 500-700 word FIRST PERSON stream of consciousness from the perspective of the ideal customer. Not a persona card with demographics. A real person narrating their own life.

She should talk about:
- Her frustration with the problem (using the EXACT language from the reviews below)
- What she's tried before and why it failed
- What she secretly hopes exists
- What makes her skeptical that it does
- How this problem affects her identity and daily life
- The moment she almost gave up

USE THE ACTUAL PHRASES AND LANGUAGE FROM THESE REVIEWS. Every sentence should sound like it could be the next line in a real review.

PRODUCT ASIN: {asin}

KEY LANGUAGE FROM REVIEWS:
- Problem phrases: {problem_phrases}
- Failed solution phrases: {failed_solution_phrases}
- Desired outcome phrases: {desired_outcome_phrases}
- Identity language: {identity_language}
- Skepticism phrases: {skepticism_phrases}

ROOT EMOTION: {root_emotion}
BEFORE STATE: {before_state}
AFTER STATE: {after_state}

Write the monologue now. First person. Her voice. Raw. Real. No marketing polish.

Return as JSON:
{{
    "monologue": "...",
    "key_phrases_used": ["list of exact review phrases woven into the monologue"]
}}"""

LANGUAGE_BIBLE_PROMPT = """You are compiling the Language Bible for a Creative Intelligence Dossier. This is the raw vocabulary of the market — no interpretation, just the exact words organized by category.

Every entry MUST be sourced to its original review. You keep the receipts.

From the review data below, organize phrases into these categories:

1. **Problem Description Phrases** — exact words buyers use to describe their pain (not clinical, not marketing — THEIR words)
2. **Failed Solution Language** — how they describe things that didn't work
3. **Desired Outcome Phrases** — the specific result words they use when happy
4. **Identity Shift Language** — before/after self-description
5. **Skepticism Phrases** — exact internal objections, word-for-word
6. **Comparison Language** — how they reference competitors and alternatives
7. **Unexpected Win Phrases** — outcomes they didn't expect and were delighted by

PRODUCT ASIN: {asin}

N-GRAM ANALYSIS (most frequent phrases by star rating):
{ngram_data}

REVIEW SAMPLES (across all star ratings):
{reviews}

Format as JSON:
{{
    "problem_description": [{{"phrase": "...", "source_review_id": "...", "source_snippet": "..."}}],
    "failed_solution": [{{"phrase": "...", "source_review_id": "...", "source_snippet": "..."}}],
    "desired_outcome": [{{"phrase": "...", "source_review_id": "...", "source_snippet": "..."}}],
    "identity_shift": [{{"phrase": "...", "source_review_id": "...", "source_snippet": "..."}}],
    "skepticism": [{{"phrase": "...", "source_review_id": "...", "source_snippet": "..."}}],
    "comparison": [{{"phrase": "...", "source_review_id": "...", "source_snippet": "..."}}],
    "unexpected_win": [{{"phrase": "...", "source_review_id": "...", "source_snippet": "..."}}]
}}

Aim for 15-25 entries per category. Prioritize phrases with high helpful votes."""

HEADLINE_BANK_PROMPT = """You are building the Headline Bank for a Creative Intelligence Dossier. Generate 50 headline candidates ready to test.

Each headline must be:
- Written in REVIEW LANGUAGE, not marketing language
- Close to deployable as-is
- Sourced from actual review data

Organize by awareness level (problem-aware, solution-aware, product-aware, most-aware) and emotional angle.

For each headline provide:
1. The headline itself
2. The source review snippet it was derived from
3. The psychological mechanism it activates
4. The awareness stage it's best suited for
5. A "stress test" — the internal objection a skeptic would raise, and whether the headline survives it

PRODUCT ASIN: {asin}

LANGUAGE BIBLE DATA:
{language_bible}

NIGHTMARE MINING DATA:
{nightmare_data}

TRANSFORMATION DATA:
{transformation_data}

TOP N-GRAMS:
{ngram_data}

Format as JSON:
{{
    "headlines": [
        {{
            "headline": "...",
            "source_snippet": "...",
            "psychological_mechanism": "...",
            "awareness_stage": "problem_aware|solution_aware|product_aware|most_aware",
            "emotional_angle": "fear|aspiration|identity|social_proof|novelty|frustration",
            "stress_test": {{
                "skeptic_objection": "...",
                "survives": true,
                "why": "..."
            }}
        }}
    ]
}}"""

OBJECTION_SEQUENCE_PROMPT = """You are mapping the complete Objection Sequence for a Creative Intelligence Dossier.

This is NOT a FAQ. It's a narrative map of how skepticism moves through the buyer's mind from first encounter to purchase decision.

Map it as a timeline:

1. **Objection 1 (first 3 seconds)**: Gut reaction before she's even engaged. What does she tell herself?
2. **Objection 2 (after the hook)**: First intellectual block that surfaces
3. **Objection 3 (mid-consideration)**: What past failure is she comparing this to?
4. **Objection 4 (at the offer)**: What makes her hesitate at price or commitment?
5. **Objection 5 (post-purchase consideration)**: What would make her regret it?

For each: the objection in HER words, the psychological source, and the specific proof or copy move that neutralizes it.

PRODUCT ASIN: {asin}

OBJECTION DATA FROM 3-STAR REVIEWS:
{objection_data}

NIGHTMARE DATA FROM 1-2 STAR REVIEWS:
{nightmare_data}

SKEPTICISM PHRASES:
{skepticism_phrases}

4-STAR "ALMOST PERFECT" DATA:
{four_star_data}

Format as JSON:
{{
    "objection_sequence": [
        {{
            "stage": "first_3_seconds|after_hook|mid_consideration|at_offer|post_purchase",
            "timing": "...",
            "objection_in_her_words": "...",
            "psychological_source": "fear|past_failure|category_exhaustion|price_sensitivity|regret_aversion",
            "source_review_snippets": ["..."],
            "neutralizer": {{
                "proof_type": "...",
                "copy_move": "...",
                "example": "..."
            }}
        }}
    ]
}}"""

ANGLE_MATRIX_PROMPT = """You are building the Angle Matrix for a Creative Intelligence Dossier — the campaign strategy grid.

Create a matrix with 5 emotional angles on one axis and 3 awareness levels on the other (15 cells total).

Emotional angles: Fear, Aspiration, Identity, Social Proof, Novelty
Awareness levels: Problem-Aware, Solution-Aware, Product-Aware

Each cell contains:
1. The primary hook approach
2. 2-3 specific review-sourced phrases that fit this cell
3. The creative format best suited (UGC, talking head, text-based, before/after, demo)
4. An example hook written out in full

PRODUCT ASIN: {asin}

LANGUAGE BIBLE:
{language_bible}

HEADLINE CANDIDATES:
{headline_data}

TRANSFORMATION DATA:
{transformation_data}

NIGHTMARE DATA:
{nightmare_data}

Format as JSON:
{{
    "matrix": [
        {{
            "emotional_angle": "fear|aspiration|identity|social_proof|novelty",
            "awareness_level": "problem_aware|solution_aware|product_aware",
            "hook_approach": "...",
            "review_phrases": ["...", "...", "..."],
            "best_format": "UGC|talking_head|text_based|before_after|demo",
            "example_hook": "..."
        }}
    ]
}}"""

PROOF_ARCHITECTURE_PROMPT = """You are building the Proof Architecture Brief for a Creative Intelligence Dossier.

Determine what kind of proof actually MOVES this market — not generic, but specific to what converted skeptics in the review data.

Analyze:

1. **Proof Type Ranking** — rank these by effectiveness in this specific market:
   - Clinical claims / data / studies
   - Before/after visuals
   - "Someone like me" testimonials
   - Expert authority
   - Specific numbers and metrics
   - Duration/timeline claims
   - Social proof (volume)

2. **Outcome Metrics That Resonate** — not "great results" but the specific metrics ("in 3 weeks," "after just one use," "saved me $X")

3. **Trusted Testimonial Profile** — who does this market trust? What demographic/psychographic profile converts best?

4. **Red Flags** — proof formats that INCREASE skepticism in this category

PRODUCT ASIN: {asin}

5-STAR CONVERSION STORIES (what converted skeptics):
{conversion_stories}

SKEPTICISM PHRASES:
{skepticism_phrases}

TRANSFORMATION TIMELINE:
{transformation_data}

Format as JSON:
{{
    "proof_type_ranking": [
        {{"type": "...", "rank": 1, "effectiveness_reason": "...", "review_evidence": "..."}}
    ],
    "resonant_outcome_metrics": [
        {{"metric": "...", "frequency": "...", "source_snippets": ["..."]}}
    ],
    "trusted_testimonial_profile": {{
        "demographic": "...",
        "psychographic": "...",
        "key_traits": ["..."],
        "evidence": "..."
    }},
    "red_flags": [
        {{"proof_format": "...", "why_it_backfires": "...", "evidence": "..."}}
    ]
}}"""

COMPETITIVE_MAP_PROMPT = """You are building the Competitive Positioning Map for a Creative Intelligence Dossier.

Using comparison language from reviews, build:

1. **Competitor Weakness Table**:
   - Competitor name/type
   - Their weakness (from review language)
   - Frequency in reviews
   - The unfulfilled promise
   - Your positioning opportunity

2. **White Space Narrative** — the 2-3 positioning claims that are:
   (a) Desired by the market (review evidence)
   (b) Not owned by any competitor
   (c) This is your moat

PRODUCT ASIN: {asin}

COMPARISON INTELLIGENCE DATA:
{comparison_data}

1-STAR REVIEW THEMES:
{one_star_themes}

COMPETITOR MENTIONS IN REVIEWS:
{competitor_mentions}

Format as JSON:
{{
    "competitor_weaknesses": [
        {{
            "competitor": "...",
            "weakness": "...",
            "frequency": "high|medium|low",
            "unfulfilled_promise": "...",
            "positioning_opportunity": "..."
        }}
    ],
    "white_space_claims": [
        {{
            "claim": "...",
            "market_desire_evidence": "...",
            "competitor_gap": "...",
            "moat_strength": "strong|moderate|emerging"
        }}
    ],
    "positioning_narrative": "..."
}}"""

CONVERSION_BLUEPRINT_PROMPT = """You are building the 3→5 Star Conversion Blueprint for a Creative Intelligence Dossier.

The delta between 3-star and 5-star experiences reveals marketing opportunities — not product feedback.

Analyze:

1. **Knowledge Gap** — what did 5-star buyers KNOW before purchasing that 3-star buyers didn't?
2. **Framing Gap** — what framing in pre-purchase content would have shifted the 3-star experience?
3. **Expectation Gap** — where did 3-star buyers' expectations diverge from what the product actually delivers?
4. **Communication Opportunities** — landing page gaps, email onboarding gaps, ad expectation-setting failures

PRODUCT ASIN: {asin}

3-STAR REVIEWS (conflicted):
{three_star_reviews}

5-STAR REVIEWS (evangelical):
{five_star_reviews}

OBJECTION DATA:
{objection_data}

Format as JSON:
{{
    "knowledge_gaps": [
        {{"what_5star_knew": "...", "what_3star_didnt": "...", "evidence": "..."}}
    ],
    "framing_gaps": [
        {{"current_framing_issue": "...", "better_framing": "...", "evidence": "..."}}
    ],
    "expectation_gaps": [
        {{"expectation": "...", "reality": "...", "messaging_fix": "..."}}
    ],
    "communication_opportunities": {{
        "landing_page": ["..."],
        "email_onboarding": ["..."],
        "ad_messaging": ["..."],
        "post_purchase": ["..."]
    }}
}}"""

CREATIVE_BRIEFS_PROMPT = """You are writing 3 "Swipe Ready" Creative Briefs for a Creative Intelligence Dossier. These are NOT angle ideas — they are actual briefs ready to hand off to a creative team.

Each brief must include:
1. **Campaign Hypothesis** — the specific insight from review data being tested
2. **Target Avatar** — which micro-segment from the analysis
3. **Hook** — written out, sourced to the Language Bible
4. **Core Narrative Arc** — 3-sentence story structure
5. **The Single Objection It Must Neutralize**
6. **Proof Asset Required** — specific type of proof needed
7. **Success Metric** — what a win looks like

The 3 briefs should target different awareness levels and emotional angles.

PRODUCT ASIN: {asin}

FULL ANALYSIS CONTEXT:
- Market Snapshot: {market_snapshot}
- Top Headlines: {top_headlines}
- Angle Matrix Highlights: {angle_matrix}
- Objection Sequence: {objection_sequence}
- Proof Architecture: {proof_architecture}
- Avatar Monologue Key Themes: {avatar_themes}

Format as JSON:
{{
    "briefs": [
        {{
            "brief_number": 1,
            "brief_name": "...",
            "campaign_hypothesis": "...",
            "target_avatar": "...",
            "hook": "...",
            "hook_source": "...",
            "narrative_arc": ["sentence 1", "sentence 2", "sentence 3"],
            "objection_to_neutralize": "...",
            "proof_asset_required": "...",
            "success_metric": "...",
            "recommended_format": "UGC|VSL|advertorial|static_ad|email",
            "awareness_level": "...",
            "emotional_angle": "..."
        }}
    ]
}}"""
