# Amazon Review Intelligence System

Turn any Amazon product's reviews into a full-funnel marketing weapon. This system scrapes up to 30K+ reviews from any ASIN, runs them through a multi-stage ML pipeline, then layers on AI-powered psychological analysis to produce a 10-section Creative Intelligence Dossier — the kind of competitive and consumer insight document that agencies charge five figures for.

One command. One ASIN. A complete creative strategy grounded in what real buyers actually say, feel, and fear.

## What the Dossier Generates

The output isn't a summary of reviews. It's 10 interconnected sections designed to be immediately actionable across the marketing funnel — from top-of-funnel awareness ads to post-purchase retention emails:

| # | Section | What It Delivers | Funnel Stage |
|---|---------|-----------------|--------------|
| 1 | **Market Snapshot** | Awareness stage classification (Schwartz scale), dominant emotional current, before/after states, conviction beliefs, category distrust factors | Strategy / Positioning |
| 2 | **Avatar Monologue** | A 500-700 word first-person stream of consciousness from the ideal buyer — not a persona card, a living voice built from actual review language | Creative Briefing |
| 3 | **Language Bible** | 100+ categorized phrases pulled verbatim from reviews: problem descriptions, failed solutions, desired outcomes, identity shifts, skepticism language, comparison language, unexpected wins — each sourced to its original review | Copywriting / Ad Creative |
| 4 | **Headline Bank** | 50 test-ready headlines organized by awareness level and emotional angle, each stress-tested against a skeptic objection, each traceable to a real review snippet | Top-of-Funnel Ads |
| 5 | **Objection Sequence** | A temporal map of how skepticism moves through the buyer's mind — from gut reaction (first 3 seconds) through mid-consideration to post-purchase anxiety — with specific neutralizers for each stage | Landing Pages / VSLs |
| 6 | **Angle Matrix** | A 15-cell strategy grid crossing 5 emotional angles (fear, aspiration, identity, social proof, novelty) with 3 awareness levels, each cell containing hooks, review-sourced phrases, and recommended creative formats | Campaign Planning |
| 7 | **Proof Architecture** | Ranked proof types by actual effectiveness in this specific market, resonant outcome metrics, trusted testimonial profiles, and red flags — proof formats that *increase* skepticism in this category | Mid-Funnel / Conversion |
| 8 | **Competitive Positioning Map** | Competitor weakness table, unfulfilled promises from alternatives, white-space positioning claims with moat strength ratings — all derived from what buyers say when they compare | Positioning / Differentiation |
| 9 | **3-to-5 Star Conversion Blueprint** | The knowledge, framing, and expectation gaps between conflicted and evangelical buyers, translated into specific landing page fixes, email onboarding sequences, and ad messaging corrections | CRO / Retention |
| 10 | **Swipe-Ready Creative Briefs** | 3 complete briefs ready to hand to a creative team — each with a campaign hypothesis, target micro-segment, written hook, narrative arc, objection to neutralize, required proof asset, and success metric | Execution |

Every section builds on the ones before it. The Headline Bank draws from the Language Bible. The Creative Briefs synthesize the Angle Matrix, Objection Sequence, and Proof Architecture. Nothing exists in isolation.

## How It Works: The Intelligence Pipeline

The system runs four phases, each feeding the next. Here's what happens under the hood — both the intuition and the technical detail.

### Phase 1: Pre-Processing

Reviews are segmented into five psychological cohorts by star rating. Each review gets enriched metadata: body length classification, comparison-mention detection (regex pattern matching for "compared to," "switched from," "better than," etc.), "tried everything" signal detection, and a helpfulness weight that amplifies reviews the community has validated. High-helpful-vote reviews get up to 3x weighting priority in downstream analysis — the crowd has already curated these as the most representative voices.

### Phase 2: ML Analysis

**N-gram extraction** — For each star cohort independently, the system runs sklearn's `CountVectorizer` to extract the most frequent bigrams and trigrams, filtering stopwords, requiring minimum document frequency of 2, and capping at the top 100 per cohort. This surfaces the recurring *phrases* — not just words — that define each segment's vocabulary. A 1-star "doesn't work" and a 5-star "changed my life" live in completely different linguistic universes, and the n-gram layer maps both.

**Semantic clustering** — All reviews (sampled to 5,000 for large datasets) are embedded using `all-MiniLM-L6-v2` sentence transformers, producing 384-dimensional dense vectors that capture meaning, not just word overlap. These embeddings are then reduced to 10 dimensions via `UMAP` (cosine metric, 15 neighbors, 0.0 min_dist — tuned for cluster separation over visual aesthetics). Finally, `HDBSCAN` identifies natural topic clusters without requiring a pre-specified cluster count — the data decides how many themes exist. For each cluster, the system identifies the 3 reviews closest to the centroid as representative exemplars.

The result: an unsupervised map of what people actually talk about, organized by semantic similarity rather than star rating. A cluster might reveal that "texture complaints" span 1-star through 4-star reviews with subtly different framing at each level — insight that keyword search alone would miss.

### Phase 3: AI Analysis (4 Parallel Passes)

Four independent Claude API calls run concurrently, each targeting a different psychological layer of the review data:

- **Nightmare Mining** (1-2 star reviews) — Extracts the internal monologue of frustrated buyers, the specific promises they believed before purchasing, visceral nightmare scenarios, failed-solution language patterns, and headline candidates that activate fear-based scroll-stopping
- **Transformation Mining** (5-star reviews, weighted by helpful votes) — Captures before/after language pairs, identity shift patterns ("I used to be..." to "Now I..."), unexpected wins, transformation timelines, and the specific triggers that pushed buyers from satisfied to evangelical
- **Objection Archaeology** (3-star reviews) — Maps precise friction points, qualification hedges ("it's good BUT..."), the 3-to-5-star gap, expectation mismatches, and "almost perfect" language — these conflicted reviews are the most analytically rich in any dataset
- **Comparison Intelligence** (reviews mentioning competitors) — Extracts attribute-level competitive comparisons, competitor weaknesses, category fatigue signals, and unfulfilled promises from alternatives

### Phase 4: Synthesis (10 Sequential Builds)

Ten Claude calls execute in sequence, each building on every previous section's output. The Market Snapshot informs the Avatar Monologue. The Language Bible feeds the Headline Bank. The Objection Sequence and Proof Architecture converge in the Creative Briefs. This chain-of-thought architecture means the final output carries the full analytical context forward — later sections don't just reference earlier ones, they're *conditioned* on them.

## Quick Start

```bash
pip install -r requirements.txt
playwright install chromium

# Optional: Tor for IP rotation
brew install tor  # macOS
```

Set your Anthropic API key for dossier generation:

```bash
export ANTHROPIC_API_KEY="sk-ant-..."
```

## Usage

```bash
# 1. Login to Amazon (once — saves session)
python scrape.py login

# 2. Scrape reviews
python scrape.py fetch B08N5WRWNW                        # all reviews
python scrape.py fetch B08N5WRWNW --limit 5000            # cap at 5K
python scrape.py fetch B08N5WRWNW --stars 1,2 -o pain.csv # just negative
python scrape.py fetch B08N5WRWNW --no-tor                # skip Tor

# 3. Check progress / export
python scrape.py stats B08N5WRWNW
python scrape.py export B08N5WRWNW -o reviews.csv

# 4. Generate Creative Intelligence Dossier
python scrape.py dossier B08N5WRWNW                    # HTML output
python scrape.py dossier B08N5WRWNW -f pdf              # PDF output
python scrape.py dossier B08N5WRWNW -f both -m opus     # both formats, use Claude Opus
```

## Scraping Engine

The scraper exists because Amazon's review data isn't accessible via API. Getting tens of thousands of reviews reliably requires solving several hard problems simultaneously:

- **Stealth browser automation** — Playwright with anti-detection patches. Amazon fingerprints TLS handshakes and requires JS execution; curl_cffi handles TLS impersonation but still gets sign-in redirected. A real browser with stealth patches is the only reliable approach.
- **Star-splitting strategy** — Amazon limits each filtered view to ~5K reviews. By splitting across 5 star ratings x 2 sort orders (recent, helpful), the system reaches a ~50K theoretical ceiling per product.
- **Checkpoint/resume** — Progress saves after every page load. Kill the process anytime; it picks up exactly where it left off.
- **Local CAPTCHA solving** — amazoncaptcha's CNN model achieves ~98% accuracy on Amazon's text CAPTCHAs without external API calls.
- **Tor IP rotation** — Optional circuit management for rate-limit evasion on large scrapes.
- **SQLite deduplication** — Reviews are deduplicated by review_id at the database level (INSERT OR IGNORE), so overlapping star/sort combinations never inflate counts.

## Architecture

```
scrape.py              CLI entry point (Click + Rich)
scraper/
  orchestrator.py      Main pipeline — plans tasks, executes batches, handles retries
  browser_engine.py    Stealth Playwright browser with anti-detection patches
  parser.py            HTML → structured Review objects (BeautifulSoup)
  storage.py           SQLite backend — reviews, checkpoints, metadata
  engine.py            curl_cffi HTTP engine (TLS fingerprint impersonation)
  auth.py              Interactive Amazon login + session persistence
  captcha.py           Local CAPTCHA solving (amazoncaptcha CNN)
  tor.py               Tor circuit management for IP rotation
  google_cache.py      Fallback: scrape reviews from Google's cache
  product_page.py      Fallback: scrape reviews from product page
dossier/
  analyzer.py          Full analysis pipeline (ML + Claude API)
  prompts.py           Claude prompt templates
  renderer.py          HTML/PDF output generation
  template.html        Jinja2 dossier template
data/                  SQLite databases, session files (gitignored)
```

## Requirements

- Python 3.10+
- `ANTHROPIC_API_KEY` environment variable (for dossier generation)
- Chromium browser (via `playwright install chromium`)
- Optional: Tor (for IP rotation during scraping)
- Optional: `pango` + `gobject-introspection` for PDF output (`brew install pango gobject-introspection`)
