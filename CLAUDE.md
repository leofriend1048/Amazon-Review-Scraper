# Amazon Review Intelligence System

## What This Is
A local-first Amazon review scraper + AI-powered Creative Intelligence Dossier generator. Scrapes up to 30K+ reviews from any ASIN using stealth browser automation, then runs ML clustering + Claude API analysis to produce a 10-section marketing intelligence report.

## Architecture

```
scrape.py          CLI entry point (Click + Rich)
scraper/
  orchestrator.py  Main scrape pipeline — plans tasks, executes batches, handles retries
  browser_engine.py Stealth Playwright browser with anti-detection patches
  parser.py        HTML → structured Review objects (BeautifulSoup + data-hook selectors)
  storage.py       SQLite backend — reviews, checkpoints, metadata
  engine.py        curl_cffi HTTP engine (TLS fingerprint impersonation) — used by google_cache
  auth.py          Interactive Amazon login + session persistence
  captcha.py       Local CAPTCHA solving (amazoncaptcha CNN)
  tor.py           Tor circuit management for IP rotation
  google_cache.py  Fallback: scrape reviews from Google's cache
  product_page.py  Fallback: scrape reviews from product page (no login needed)
dossier/
  analyzer.py      Full analysis pipeline (ML + Claude API)
  prompts.py       All Claude prompt templates
  renderer.py      HTML/PDF output generation
  template.html    Jinja2 template for the dossier
data/              SQLite databases, session files (gitignored)
```

## Setup

```bash
pip install -r requirements.txt
playwright install chromium

# Optional: install Tor for IP rotation
brew install tor  # macOS
```

## Usage Flow

```bash
# 1. Login to Amazon (once — saves session)
python scrape.py login

# 2. Scrape reviews
python scrape.py fetch B08N5WRWNW                    # scrape all reviews
python scrape.py fetch B08N5WRWNW --limit 5000       # cap at 5K
python scrape.py fetch B08N5WRWNW --stars 1,2 -o pain.csv  # just negative reviews
python scrape.py fetch B08N5WRWNW --no-tor            # skip Tor

# 3. Check progress / export
python scrape.py stats B08N5WRWNW
python scrape.py export B08N5WRWNW -o reviews.csv

# 4. Generate Creative Intelligence Dossier
python scrape.py dossier B08N5WRWNW                   # HTML output
python scrape.py dossier B08N5WRWNW -f pdf             # PDF output
python scrape.py dossier B08N5WRWNW -f both -m opus    # both formats, higher quality
```

## Key Design Decisions

- **Playwright over requests**: Amazon fingerprints TLS and requires JS execution. curl_cffi handles TLS but still gets sign-in redirected. Real browser with stealth patches is the only reliable approach.
- **"Show More" clicking over pagination**: Amazon's review pages now use infinite-scroll-style "Show More" buttons instead of traditional page numbers.
- **Star-splitting strategy**: Amazon limits each filter view to ~5K reviews. By splitting 5 stars × 2 sort orders, we can reach ~50K theoretical max.
- **SQLite over CSV**: Deduplication via review_id primary key, checkpoint/resume, efficient stats queries.
- **Batch restart every 50 clicks**: Prevents DOM memory bloat and session staleness.
- **Local CAPTCHA solving**: amazoncaptcha's CNN gets ~98% accuracy on Amazon's text CAPTCHAs without any API calls.

## Dossier Pipeline

The dossier generator runs 4 phases:
1. **Pre-processing**: segment by stars, add metadata (length, comparisons, helpful weight)
2. **ML Layer**: n-gram extraction (sklearn), sentence embeddings (sentence-transformers), UMAP + HDBSCAN clustering
3. **AI Layer**: 4 parallel Claude API passes (nightmare mining, transformation mining, objection archaeology, comparison intelligence)
4. **Synthesis**: 10 sequential Claude calls building each dossier section, each informed by previous sections

## Environment Requirements

- Python 3.10+
- ANTHROPIC_API_KEY environment variable (for dossier generation)
- Chromium browser (installed via `playwright install chromium`)
- Optional: Tor (for IP rotation during scraping)

## Data Storage

All data lives in `data/`:
- `{ASIN}.db` — SQLite database with reviews, checkpoints, metadata
- `amazon_storage_state.json` — saved browser session
- `amazon_cookies.json` — saved cookies

## Development Notes

- The orchestrator uses a shared Playwright browser instance across tasks, but creates fresh contexts for each batch
- Reviews are deduplicated by review_id at the SQLite level (INSERT OR IGNORE)
- Progress is saved after every click via checkpoints — safe to Ctrl+C anytime
- The dossier analyzer processes reviews in batches of 250 for Claude API calls
- For clustering, reviews are sampled to 5000 if the dataset is larger
