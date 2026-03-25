# Amazon Review Intelligence System

A local-first Amazon review scraper + AI-powered Creative Intelligence Dossier generator. Scrapes up to 30K+ reviews from any ASIN using stealth browser automation, then runs ML clustering + Claude API analysis to produce a 10-section marketing intelligence report.

## Features

- **Stealth scraping** — Playwright with anti-detection patches, TLS fingerprint impersonation, optional Tor IP rotation
- **Scale** — Star-splitting strategy reaches up to ~50K reviews per product
- **Resilient** — Checkpoint/resume on every click, auto-retry, local CAPTCHA solving (~98% accuracy)
- **ML clustering** — N-gram extraction, sentence embeddings (MiniLM), UMAP + HDBSCAN topic clustering
- **AI analysis** — 4 parallel Claude API passes + 10 sequential synthesis sections
- **Output** — HTML and PDF dossier reports

## Quick Start

```bash
# Install dependencies
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
python scrape.py fetch B08N5WRWNW                        # scrape all reviews
python scrape.py fetch B08N5WRWNW --limit 5000            # cap at 5K
python scrape.py fetch B08N5WRWNW --stars 1,2 -o pain.csv # just negative reviews
python scrape.py fetch B08N5WRWNW --no-tor                # skip Tor

# 3. Check progress / export
python scrape.py stats B08N5WRWNW
python scrape.py export B08N5WRWNW -o reviews.csv

# 4. Generate Creative Intelligence Dossier
python scrape.py dossier B08N5WRWNW                    # HTML output
python scrape.py dossier B08N5WRWNW -f pdf              # PDF output
python scrape.py dossier B08N5WRWNW -f both -m opus     # both formats, higher quality
```

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

## Dossier Pipeline

The dossier generator runs 4 phases:

1. **Pre-processing** — Segment by stars, add metadata (length, comparisons, helpful weight)
2. **ML Layer** — N-gram extraction (sklearn), sentence embeddings (sentence-transformers), UMAP + HDBSCAN clustering
3. **AI Layer** — 4 parallel Claude API passes: nightmare mining, transformation mining, objection archaeology, comparison intelligence
4. **Synthesis** — 10 sequential Claude calls building each dossier section, each informed by previous sections

## Requirements

- Python 3.10+
- `ANTHROPIC_API_KEY` environment variable (for dossier generation)
- Chromium browser (via `playwright install chromium`)
- Optional: Tor (for IP rotation during scraping)
- Optional: `pango` + `gobject-introspection` (for PDF output — `brew install pango gobject-introspection`)
