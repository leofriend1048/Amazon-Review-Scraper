"""
Product page review scraper — no login required.

When Amazon requires sign-in for /product-reviews/, we can still get
reviews from the product page (/dp/). Reviews are lazy-loaded as the
user scrolls down, so we need to scroll and wait for them to appear.

This approach is limited to the reviews visible on the product page
(typically 8-10 reviews), but we can use AJAX pagination within the
review widget to load more.
"""

import time
import random
import logging
from typing import List, Optional

from .browser_engine import StealthBrowser
from .parser import Review

logger = logging.getLogger(__name__)


def extract_reviews_from_product_page(browser: StealthBrowser, asin: str,
                                       max_reviews: int = 100) -> List[Review]:
    """
    Scrape reviews from the product page by scrolling to the review section
    and clicking 'load more' / pagination within the review widget.
    """
    url = f"https://www.amazon.com/dp/{asin}"
    html = browser.get_page_html(url)

    if not html or html.startswith("__CAPTCHA__"):
        logger.error("Could not load product page")
        return []

    page = browser._page
    reviews = []
    seen_ids = set()

    # Scroll down to trigger lazy loading of the review section
    logger.info("Scrolling to review section...")
    for scroll_pct in [30, 50, 60, 70, 75, 80, 85, 90, 95, 100]:
        page.evaluate(f"window.scrollTo(0, document.body.scrollHeight * {scroll_pct / 100})")
        time.sleep(random.uniform(0.3, 0.8))

    # Wait for review section to load
    time.sleep(2)

    # Try to find and parse reviews
    html = page.content()

    from .parser import parse_reviews
    page_reviews = parse_reviews(html, asin)

    for r in page_reviews:
        if r.review_id not in seen_ids:
            seen_ids.add(r.review_id)
            reviews.append(r)

    logger.info(f"Found {len(reviews)} reviews on product page")

    # Look for "See more reviews" button within the page (AJAX load)
    # Amazon sometimes has a "Next page" within the review widget
    attempts = 0
    while len(reviews) < max_reviews and attempts < 20:
        next_btn = page.query_selector(
            'li.a-last a, '
            '[data-hook="see-all-reviews-link-foot"], '
            'a:has-text("Next page")'
        )

        if not next_btn:
            break

        try:
            next_btn.scroll_into_view_if_needed()
            next_btn.click()
            time.sleep(random.uniform(2, 4))

            html = page.content()
            page_reviews = parse_reviews(html, asin)

            new_count = 0
            for r in page_reviews:
                if r.review_id not in seen_ids:
                    seen_ids.add(r.review_id)
                    reviews.append(r)
                    new_count += 1

            if new_count == 0:
                break

            logger.info(f"Loaded {new_count} more reviews (total: {len(reviews)})")
            attempts += 1

        except Exception:
            break

    return reviews
