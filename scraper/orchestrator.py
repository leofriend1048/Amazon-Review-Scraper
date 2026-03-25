"""
Main scraping orchestrator.

Two pagination strategies:

1. **Page-number pagination** (primary): Navigate to sequential URLs
   `product-reviews/{asin}?pageNumber=X&...` — each page has ~10 reviews.
   Can reach up to 500 pages (5,000 reviews) per filter combination.
   Much higher ceiling than Show More clicking (~100 reviews).

2. **"Show More" clicking** (fallback): Click the Show More button to
   load the next 10 reviews onto the same page. Caps out at ~100 reviews
   per filter but works as a fallback if pagination fails.

Strategy for large scrapes (30K+):
- Prefer page-number pagination (reaches 50x more reviews per filter)
- Fall back to Show More if pagination fails
- Split by star rating to get past Amazon's per-filter limits
- Use both sort orders (recent + helpful) for max unique coverage
- Adaptive pacing: 2-4s between pages, longer pauses periodically
- Retry failed tasks with exponential backoff
- CAPTCHA solving via local ML model
- Concurrent task execution for speed
"""

import logging
import time
import random
from dataclasses import dataclass, field
from typing import List, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

from playwright.sync_api import sync_playwright

from .browser_engine import StealthBrowser
from .parser import parse_reviews, parse_review_summary, ReviewPageInfo
from .storage import ReviewStorage

logger = logging.getLogger(__name__)

AMAZON_REVIEW_URL = "https://www.amazon.com/product-reviews/{asin}"

STAR_FILTERS = {
    1: "one_star",
    2: "two_star",
    3: "three_star",
    4: "four_star",
    5: "five_star",
}

# How many clicks before we restart the page (prevents memory bloat + session staleness)
CLICKS_PER_BATCH = 50

# Pagination settings
MAX_PAGES_PER_FILTER = 500          # Amazon caps at 500 pages per filter
PAGES_PER_BROWSER_SESSION = 50      # Restart browser every N pages to stay fresh
CONSECUTIVE_EMPTY_PAGES_LIMIT = 3   # Stop after N consecutive pages with 0 new reviews

# Retry settings
MAX_TASK_RETRIES = 3
RETRY_BACKOFF_SECONDS = [30, 60, 120]

# Global cooldown: if N consecutive tasks fail, pause before continuing
CONSECUTIVE_FAIL_THRESHOLD = 3
GLOBAL_COOLDOWN_SECONDS = 60


@dataclass
class ScrapeTask:
    star_filter: Optional[int]
    sort_by: str
    estimated_reviews: int = 5000
    priority: int = 0

    @property
    def task_key(self) -> str:
        star = self.star_filter or "all"
        return f"stars_{star}_{self.sort_by}"

    def build_url(self, asin: str) -> str:
        params = [f"sortBy={self.sort_by}"]
        if self.star_filter:
            params.append(f"filterByStar={STAR_FILTERS[self.star_filter]}")
        return f"{AMAZON_REVIEW_URL.format(asin=asin)}?{'&'.join(params)}"

    def build_page_url(self, asin: str, page_number: int) -> str:
        """Build a URL for page-number pagination."""
        params = [
            f"pageNumber={page_number}",
            f"sortBy={self.sort_by}",
        ]
        if self.star_filter:
            params.append(f"filterByStar={STAR_FILTERS[self.star_filter]}")
        return f"{AMAZON_REVIEW_URL.format(asin=asin)}?{'&'.join(params)}"

    @property
    def pagination_task_key(self) -> str:
        """Separate checkpoint key for page-number pagination."""
        star = self.star_filter or "all"
        return f"pages_{star}_{self.sort_by}"


@dataclass
class ScrapePlan:
    asin: str
    tasks: List[ScrapeTask] = field(default_factory=list)
    target_count: int = 0
    review_info: Optional[ReviewPageInfo] = None


class Orchestrator:
    def __init__(self, asin: str, limit: Optional[int] = None,
                 sort: str = "all", stars: Optional[List[int]] = None,
                 use_tor: bool = True, workers: int = 3,
                 headless: bool = True, progress_callback=None):
        self.asin = asin.upper().strip()
        self.limit = limit
        self.sort = sort
        self.stars = stars
        self.use_tor = use_tor
        self.workers = workers
        self.headless = headless
        self.progress_callback = progress_callback

        self.storage = ReviewStorage(self.asin)
        self._stop = False
        self._total_new = 0
        self._consecutive_failures = 0
        self._pw = None
        self._browser_instance = None

    def _get_shared_browser(self):
        if self._pw is None:
            self._pw = sync_playwright().start()
            self._browser_instance = self._pw.chromium.launch(
                headless=self.headless,
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--disable-features=IsolateOrigins,site-per-process",
                    "--no-first-run",
                    "--no-default-browser-check",
                ],
            )
        return self._pw, self._browser_instance

    def _make_browser(self) -> StealthBrowser:
        pw, browser = self._get_shared_browser()
        sb = StealthBrowser(
            headless=self.headless,
            playwright_instance=pw,
            browser_instance=browser,
        )
        sb.start()
        return sb

    def plan(self) -> ScrapePlan:
        """Phase 1: Recon — get review counts and build scrape plan."""
        plan = ScrapePlan(asin=self.asin)

        browser = self._make_browser()
        url = f"{AMAZON_REVIEW_URL.format(asin=self.asin)}?sortBy=recent"
        html = browser.get_page_html(url)
        browser.close()

        if not html or html.startswith("__CAPTCHA__"):
            logger.error(f"Failed to fetch review page for {self.asin}")
            return plan

        # Check for error pages
        if len(html) < 10000 and ("Page Not Found" in html or "page not found" in html.lower()):
            logger.error("Got 'Page Not Found' — session may be stale. Try: python scrape.py login")
            return plan

        info = parse_review_summary(html, self.asin)
        if info:
            plan.review_info = info
            self.storage.save_meta(
                info.total_ratings, info.total_reviews,
                info.average_rating, info.star_counts
            )
            logger.info(
                f"ASIN {self.asin}: {info.total_ratings} ratings, "
                f"~{info.total_reviews} reviews, avg {info.average_rating}*"
            )

        initial_reviews = parse_reviews(html, self.asin)
        if initial_reviews:
            self.storage.save_reviews(initial_reviews)

        target = self.limit or (info.total_reviews if info else 30000)
        plan.target_count = target

        if self.stars:
            star_list = self.stars
        elif target > 500:
            # Star-splitting unlocks pagination's full potential:
            # 500 pages * 10 reviews * 5 stars * 2 sorts = 50,000 max
            star_list = [1, 2, 3, 4, 5]
        else:
            star_list = [None]

        if self.sort == "recent":
            sort_list = ["recent"]
        elif self.sort == "helpful":
            sort_list = ["helpful"]
        else:
            # For >500 reviews, use both sort orders with pagination
            # to maximize unique coverage across the full review corpus
            sort_list = ["recent", "helpful"] if target > 500 else ["recent"]

        for star in star_list:
            for sort_by in sort_list:
                est = 5000
                if info and star and star in info.star_counts:
                    est = min(info.star_counts[star], 5000)

                task = ScrapeTask(
                    star_filter=star, sort_by=sort_by,
                    estimated_reviews=est, priority=est,
                )
                plan.tasks.append(task)

        plan.tasks.sort(key=lambda t: t.priority, reverse=True)
        return plan

    def _click_to_position(self, page, target_clicks: int):
        """Fast-forward by clicking show-more to resume position."""
        for i in range(target_clicks):
            if self._stop:
                break
            btn = page.query_selector('[data-hook="show-more-button"]')
            if not btn:
                break
            try:
                btn.scroll_into_view_if_needed()
                btn.click()
                # Fast clicking for resume — just need to get to the right spot
                time.sleep(random.uniform(0.3, 0.8))
            except Exception:
                break

    def _try_solve_captcha(self, browser: StealthBrowser) -> bool:
        """Attempt to solve a CAPTCHA on the current page."""
        try:
            solved = browser.solve_captcha_on_page()
            if solved:
                logger.info("CAPTCHA solved successfully — continuing")
                return True
            else:
                logger.warning("CAPTCHA solving failed")
                return False
        except Exception as e:
            logger.warning(f"CAPTCHA solving error: {e}")
            return False

    def _scrape_batch(self, task: ScrapeTask, browser: StealthBrowser,
                      start_click: int) -> tuple:
        """
        Scrape one batch of reviews (up to CLICKS_PER_BATCH clicks).
        Returns (new_reviews_count, last_click, should_continue).
        """
        url = task.build_url(self.asin)
        html = browser.get_page_html(url)

        if not html:
            return 0, start_click, False

        # Handle CAPTCHA
        if html.startswith("__CAPTCHA__"):
            logger.warning(f"{task.task_key}: CAPTCHA encountered, attempting solve...")
            if self._try_solve_captcha(browser):
                html = browser._page.content() if browser._page else None
                if not html:
                    return 0, start_click, False
            else:
                # Cool down and signal retry
                time.sleep(random.uniform(30, 60))
                return 0, start_click, False

        # Check for error/block
        if len(html) < 10000:
            if "Page Not Found" in html or "page not found" in html.lower():
                logger.warning(f"{task.task_key}: Page Not Found — cooling down 30s...")
                time.sleep(30)
                return 0, start_click, False
            if "signin" in html.lower() or "authportal" in html:
                logger.warning(f"{task.task_key}: Session expired — need re-login")
                return 0, start_click, False

        page = browser._page

        # Wait for reviews to render
        try:
            page.wait_for_selector('[data-hook="review"]', timeout=15000)
        except Exception:
            logger.warning(f"{task.task_key}: Reviews didn't render on page")
            return 0, start_click, False

        # Fast-forward to resume position if needed
        if start_click > 0:
            logger.info(f"{task.task_key}: Resuming from click {start_click}...")
            self._click_to_position(page, start_click)
            time.sleep(1)

        # Save initial reviews on page
        initial_html = page.content()
        initial_reviews = parse_reviews(initial_html, self.asin)
        new_reviews = self.storage.save_reviews(initial_reviews)
        self._total_new += new_reviews

        click_count = start_click
        empty_clicks = 0
        max_empty = 8  # Increased from 5 — some empty clicks are normal with dedup

        for _ in range(CLICKS_PER_BATCH):
            if self._stop:
                break

            # Check limit
            if self.limit:
                current = self.storage.get_review_count()
                if current >= self.limit:
                    self._stop = True
                    break

            btn = page.query_selector('[data-hook="show-more-button"]')
            if not btn:
                logger.info(f"{task.task_key}: no more 'Show More' after {click_count} clicks")
                return new_reviews, click_count, False

            try:
                btn.scroll_into_view_if_needed()
                time.sleep(random.uniform(0.3, 0.6))
                btn.click()
                click_count += 1

                # Adaptive delay: 2-3.5s normally, longer pause every ~20 clicks
                if click_count % 20 == 0:
                    time.sleep(random.uniform(5, 12))
                else:
                    time.sleep(random.uniform(2.0, 3.5))

                # Check for CAPTCHA after click
                current_html = page.content()
                if "captcha" in current_html.lower() and "Type the characters" in current_html:
                    logger.warning(f"{task.task_key}: CAPTCHA after click {click_count}")
                    if self._try_solve_captcha(browser):
                        current_html = page.content()
                    else:
                        return new_reviews, click_count, False

                # Parse and save
                all_page_reviews = parse_reviews(current_html, self.asin)
                saved = self.storage.save_reviews(all_page_reviews)

                if saved > 0:
                    new_reviews += saved
                    self._total_new += saved
                    empty_clicks = 0
                else:
                    empty_clicks += 1

                self.storage.save_checkpoint(task.task_key, click_count)

                if self.progress_callback:
                    total = self.storage.get_review_count()
                    self.progress_callback(task.task_key, click_count, saved, total)

                if empty_clicks >= max_empty:
                    logger.info(f"{task.task_key}: {max_empty} empty clicks in a row, stopping batch")
                    return new_reviews, click_count, False

            except Exception as e:
                logger.warning(f"{task.task_key}: Click error at {click_count}: {e}")
                empty_clicks += 1
                if empty_clicks >= max_empty:
                    return new_reviews, click_count, False
                time.sleep(3)

        # Batch complete — more to do
        self.storage.save_checkpoint(task.task_key, click_count)
        return new_reviews, click_count, True

    def _scrape_task_pagination(self, task: ScrapeTask) -> int:
        """
        Scrape reviews using page-number URL pagination.

        Navigates to sequential URLs:
          /product-reviews/{asin}?pageNumber=1&sortBy=...&filterByStar=...
          /product-reviews/{asin}?pageNumber=2&sortBy=...&filterByStar=...
          ...

        Each page has ~10 reviews. Amazon allows up to 500 pages per filter,
        giving access to ~5,000 reviews per filter — far more than the
        Show More button approach (~100 reviews).

        Returns the number of new reviews saved.
        """
        task_key = task.pagination_task_key
        checkpoint = self.storage.get_checkpoint(task_key)
        if checkpoint == -1:
            logger.info(f"Pagination task {task_key} already completed, skipping")
            return 0

        total_new = 0
        # Resume from checkpoint page (0-based checkpoint = last completed page number)
        start_page = (checkpoint or 0) + 1 if checkpoint else 1
        max_pages = min(task.estimated_reviews // 10 + 10, MAX_PAGES_PER_FILTER)
        consecutive_empty = 0      # Pages with zero reviews (past the end)
        consecutive_dupes = 0      # Pages with reviews but all already in DB
        max_consecutive_dupes = 20 # Stop after this many all-dupe pages (no new content left)
        pages_in_session = 0
        last_page = start_page

        browser = self._make_browser()
        try:
            for page_num in range(start_page, max_pages + 1):
                last_page = page_num
                if self._stop:
                    break

                # Check limit
                if self.limit:
                    current = self.storage.get_review_count()
                    if current >= self.limit:
                        self._stop = True
                        break

                # Restart browser session periodically to stay fresh
                if pages_in_session >= PAGES_PER_BROWSER_SESSION:
                    logger.info(
                        f"{task_key}: Restarting browser after {pages_in_session} pages "
                        f"({total_new} new reviews so far)..."
                    )
                    browser.close()
                    time.sleep(random.uniform(3, 8))
                    browser = self._make_browser()
                    pages_in_session = 0

                url = task.build_page_url(self.asin, page_num)
                html = browser.get_page_html(url)

                if not html:
                    logger.warning(f"{task_key}: No HTML for page {page_num}")
                    consecutive_empty += 1
                    if consecutive_empty >= CONSECUTIVE_EMPTY_PAGES_LIMIT:
                        logger.info(f"{task_key}: {consecutive_empty} consecutive empty pages, stopping")
                        break
                    continue

                # Handle CAPTCHA
                if html.startswith("__CAPTCHA__"):
                    logger.warning(f"{task_key}: CAPTCHA on page {page_num}, attempting solve...")
                    if self._try_solve_captcha(browser):
                        html = browser._page.content() if browser._page else None
                        if not html:
                            consecutive_empty += 1
                            if consecutive_empty >= CONSECUTIVE_EMPTY_PAGES_LIMIT:
                                break
                            continue
                    else:
                        # Cool down and retry with fresh browser
                        logger.warning(f"{task_key}: CAPTCHA solve failed, cooling down...")
                        time.sleep(random.uniform(30, 60))
                        browser.close()
                        browser = self._make_browser()
                        pages_in_session = 0
                        consecutive_empty += 1
                        if consecutive_empty >= CONSECUTIVE_EMPTY_PAGES_LIMIT:
                            break
                        continue

                # Check for error/block pages
                if len(html) < 10000:
                    if "Page Not Found" in html or "page not found" in html.lower():
                        logger.warning(f"{task_key}: Page Not Found on page {page_num}")
                        consecutive_empty += 1
                        if consecutive_empty >= CONSECUTIVE_EMPTY_PAGES_LIMIT:
                            break
                        time.sleep(random.uniform(15, 30))
                        continue
                    if "signin" in html.lower() or "authportal" in html:
                        logger.warning(f"{task_key}: Session expired on page {page_num}")
                        break

                # Parse reviews from this page
                reviews = parse_reviews(html, self.asin)

                if not reviews:
                    # Truly empty page = past the last page of reviews
                    consecutive_empty += 1
                    logger.info(f"{task_key}: No reviews on page {page_num} ({consecutive_empty} empty in a row)")
                    if consecutive_empty >= CONSECUTIVE_EMPTY_PAGES_LIMIT:
                        logger.info(f"{task_key}: {consecutive_empty} truly empty pages, stopping")
                        break
                    continue

                # Page had reviews — reset empty counter even if all are dupes
                consecutive_empty = 0
                saved = self.storage.save_reviews(reviews)

                if saved > 0:
                    total_new += saved
                    self._total_new += saved
                    consecutive_dupes = 0
                    logger.info(f"{task_key}: Page {page_num}: {saved} new reviews ({len(reviews)} on page)")
                else:
                    consecutive_dupes += 1
                    logger.debug(f"{task_key}: Page {page_num}: {len(reviews)} reviews (all dupes, {consecutive_dupes} in a row)")
                    if consecutive_dupes >= max_consecutive_dupes:
                        logger.info(f"{task_key}: {consecutive_dupes} consecutive all-dupe pages, no new content left — stopping")
                        break

                pages_in_session += 1

                # Save checkpoint as current page number
                self.storage.save_checkpoint(task_key, page_num)

                if self.progress_callback:
                    total = self.storage.get_review_count()
                    self.progress_callback(task_key, page_num, saved, total)

                # Human-like delay between pages: 2-4s normally
                if page_num % 15 == 0:
                    # Occasional longer pause every ~15 pages
                    pause = random.uniform(8, 18)
                    logger.debug(f"{task_key}: Longer pause ({pause:.1f}s) after page {page_num}")
                    time.sleep(pause)
                elif page_num % 5 == 0:
                    # Medium pause every ~5 pages
                    time.sleep(random.uniform(4, 7))
                else:
                    time.sleep(random.uniform(2, 4))

        finally:
            browser.close()

        self.storage.save_checkpoint(task_key, last_page, completed=True)
        logger.info(f"Pagination task {task_key}: {total_new} new reviews across pages {start_page}-{last_page}")
        return total_new

    def _scrape_task(self, task: ScrapeTask) -> int:
        """
        Execute a scrape task: try page-number pagination first (higher ceiling),
        then fall back to Show More clicking if pagination yields nothing.
        """
        # --- Phase 1: Page-number pagination (primary strategy) ---
        pagination_checkpoint = self.storage.get_checkpoint(task.pagination_task_key)
        pagination_new = 0

        if pagination_checkpoint != -1:
            logger.info(f"{task.task_key}: Trying page-number pagination (higher ceiling)...")
            try:
                pagination_new = self._scrape_task_pagination(task)
            except Exception as e:
                logger.warning(f"{task.task_key}: Pagination failed: {e}")
                pagination_new = 0

            if pagination_new > 0:
                logger.info(
                    f"{task.task_key}: Pagination got {pagination_new} new reviews"
                )
        else:
            logger.info(f"{task.task_key}: Pagination already completed")

        if self._stop:
            return pagination_new

        # --- Phase 2: Show More fallback (catches stragglers) ---
        showmore_checkpoint = self.storage.get_checkpoint(task.task_key)
        if showmore_checkpoint == -1:
            logger.info(f"Show More task {task.task_key} already completed, skipping")
            return pagination_new

        # Only run Show More if pagination got nothing or was skipped
        if pagination_new > 0:
            logger.info(
                f"{task.task_key}: Skipping Show More fallback — pagination succeeded"
            )
            self.storage.save_checkpoint(task.task_key, 0, completed=True)
            return pagination_new

        logger.info(f"{task.task_key}: Falling back to Show More clicking...")

        total_new = 0
        current_click = showmore_checkpoint or 0
        max_clicks = task.estimated_reviews // 10 + 10

        while current_click < max_clicks and not self._stop:
            browser = self._make_browser()
            try:
                batch_new, last_click, should_continue = self._scrape_batch(
                    task, browser, current_click
                )
                total_new += batch_new
                current_click = last_click

                if not should_continue:
                    break

                # Brief pause between batches
                logger.info(
                    f"{task.task_key}: batch done at click {current_click}, "
                    f"{total_new} new reviews, restarting page..."
                )
                time.sleep(random.uniform(3, 8))
            finally:
                browser.close()

        self.storage.save_checkpoint(task.task_key, current_click, completed=True)
        logger.info(f"Task {task.task_key}: {total_new} new reviews via Show More in {current_click} clicks")
        return pagination_new + total_new

    def _scrape_task_with_retry(self, task: ScrapeTask) -> int:
        """Wrap _scrape_task with retry logic and exponential backoff."""
        for attempt in range(MAX_TASK_RETRIES):
            try:
                result = self._scrape_task(task)
                if result > 0:
                    self._consecutive_failures = 0
                return result
            except Exception as e:
                logger.error(
                    f"Task {task.task_key} failed (attempt {attempt+1}/{MAX_TASK_RETRIES}): {e}"
                )
                if attempt < MAX_TASK_RETRIES - 1:
                    backoff = RETRY_BACKOFF_SECONDS[min(attempt, len(RETRY_BACKOFF_SECONDS)-1)]
                    logger.info(f"Retrying in {backoff}s...")
                    time.sleep(backoff)

        self._consecutive_failures += 1
        logger.error(f"Task {task.task_key} failed after {MAX_TASK_RETRIES} attempts")
        return 0

    def execute(self, plan: ScrapePlan) -> dict:
        """Phase 2: Execute scrape plan."""
        existing = self.storage.get_review_count()
        if existing > 0:
            logger.info(f"Resuming: {existing} reviews already in database")

        start_time = time.time()

        for task in plan.tasks:
            if self._stop:
                break

            # Global cooldown if too many consecutive failures
            if self._consecutive_failures >= CONSECUTIVE_FAIL_THRESHOLD:
                logger.warning(
                    f"{self._consecutive_failures} consecutive task failures — "
                    f"global cooldown {GLOBAL_COOLDOWN_SECONDS}s"
                )
                time.sleep(GLOBAL_COOLDOWN_SECONDS)
                self._consecutive_failures = 0

            logger.info(f"Starting task: {task.task_key}")
            self._scrape_task_with_retry(task)

        elapsed = time.time() - start_time
        total = self.storage.get_review_count()
        stats = self.storage.get_stats()
        self.storage.mark_complete()

        return {
            "asin": self.asin,
            "total_reviews": total,
            "new_reviews": self._total_new,
            "elapsed_seconds": round(elapsed, 1),
            "reviews_per_minute": round(total / (elapsed / 60), 1) if elapsed > 0 else 0,
            "stats": stats,
        }

    def run(self) -> dict:
        plan = self.plan()
        if not plan.tasks:
            return {"error": "No review tasks generated — check ASIN"}
        return self.execute(plan)

    def cleanup(self):
        try:
            if self._browser_instance:
                self._browser_instance.close()
            if self._pw:
                self._pw.stop()
        except Exception:
            pass

    def stop(self):
        self._stop = True
