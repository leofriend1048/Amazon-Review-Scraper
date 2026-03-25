"""
Google Cache fallback strategy.

When Amazon blocks us directly, we can try to pull review pages from
Google's cache. The reviews may be slightly stale (hours to days old)
but for competitive research this is perfectly fine.

This is the "thinking outside the box" escape hatch when Amazon
gets aggressive with blocks.
"""

import re
import time
import random
import logging
from typing import Optional, List

from .engine import RequestEngine
from .parser import parse_reviews, Review

logger = logging.getLogger(__name__)

GOOGLE_CACHE_URL = "https://webcache.googleusercontent.com/search?q=cache:{url}"
GOOGLE_SEARCH_URL = "https://www.google.com/search?q={query}&num=10"


def fetch_from_google_cache(asin: str, page: int, sort: str = "recent",
                            star_filter: Optional[int] = None,
                            engine: Optional[RequestEngine] = None) -> Optional[str]:
    """
    Try to fetch an Amazon review page from Google's cache.
    Returns HTML or None.
    """
    star_map = {1: "one_star", 2: "two_star", 3: "three_star", 4: "four_star", 5: "five_star"}

    # Build the Amazon URL we want the cached version of
    amazon_url = f"https://www.amazon.com/product-reviews/{asin}?pageNumber={page}&sortBy={sort}"
    if star_filter and star_filter in star_map:
        amazon_url += f"&filterByStar={star_map[star_filter]}"

    cache_url = GOOGLE_CACHE_URL.format(url=amazon_url)

    if engine is None:
        engine = RequestEngine()

    try:
        html = engine.get(cache_url, max_retries=2)
        if html and not html.startswith("__CAPTCHA__"):
            # Google cache wraps content — extract the actual page
            if "product-reviews" in html and "review-body" in html:
                logger.info(f"Got cached page for {asin} page {page}")
                return html
    except Exception as e:
        logger.debug(f"Google cache fetch failed: {e}")

    return None


def search_google_for_reviews(asin: str, engine: Optional[RequestEngine] = None) -> List[str]:
    """
    Search Google for cached Amazon review pages for this ASIN.
    Returns list of cached URLs found.
    """
    if engine is None:
        engine = RequestEngine()

    query = f"site:amazon.com/product-reviews/{asin}"
    search_url = GOOGLE_SEARCH_URL.format(query=query)

    try:
        html = engine.get(search_url, max_retries=2)
        if html:
            # Extract Amazon review URLs from search results
            urls = re.findall(
                rf'https?://www\.amazon\.com/[^"]*product-reviews/{asin}[^"]*',
                html
            )
            # Deduplicate
            unique_urls = list(dict.fromkeys(urls))
            logger.info(f"Found {len(unique_urls)} Google-indexed review pages for {asin}")
            return unique_urls
    except Exception:
        pass

    return []


class GoogleCacheScraper:
    """
    Fallback scraper that pulls reviews from Google's cache.
    Use this when direct Amazon scraping hits persistent blocks.
    """

    def __init__(self, asin: str):
        self.asin = asin
        self.engine = RequestEngine()

    def scrape_cached_pages(self, max_pages: int = 50,
                            sort: str = "recent",
                            star_filter: Optional[int] = None) -> List[Review]:
        """
        Scrape review pages from Google's cache.
        Much slower than direct scraping but bypasses Amazon entirely.
        """
        all_reviews = []
        seen_ids = set()

        for page in range(1, max_pages + 1):
            html = fetch_from_google_cache(
                self.asin, page, sort, star_filter, self.engine
            )

            if not html:
                logger.info(f"Google cache: no more pages after {page}")
                break

            reviews = parse_reviews(html, self.asin)
            new_reviews = [r for r in reviews if r.review_id not in seen_ids]
            for r in new_reviews:
                seen_ids.add(r.review_id)
            all_reviews.extend(new_reviews)

            logger.info(f"Google cache page {page}: {len(new_reviews)} new reviews")

            # Be gentle with Google too
            time.sleep(random.uniform(3, 8))

        return all_reviews

    def close(self):
        self.engine.close()
