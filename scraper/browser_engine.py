"""
Stealth browser engine using Playwright with anti-detection.

Amazon's bot detection requires real JavaScript execution, proper
cookie handling, and authentic browser fingerprints. curl_cffi alone
gets sign-in redirected. A real browser with stealth patches handles this.

Uses playwright-stealth to patch out common bot detection signals:
- navigator.webdriver flag
- Chrome DevTools Protocol detection
- Missing plugin/mime types
- Incorrect viewport/screen dimensions
"""

import random
import time
import logging
from typing import Optional
from dataclasses import dataclass

from playwright.sync_api import sync_playwright, Browser, BrowserContext, Page

try:
    from playwright_stealth import stealth_sync
    HAS_STEALTH = True
except ImportError:
    HAS_STEALTH = False

logger = logging.getLogger(__name__)

# Viewport configurations that look like real monitors
VIEWPORTS = [
    {"width": 1920, "height": 1080},
    {"width": 1440, "height": 900},
    {"width": 1536, "height": 864},
    {"width": 1366, "height": 768},
    {"width": 2560, "height": 1440},
]

USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
]

LOCALES = ["en-US", "en-GB", "en-US"]
TIMEZONES = ["America/New_York", "America/Chicago", "America/Los_Angeles", "America/Denver"]


@dataclass
class BrowserStats:
    total_requests: int = 0
    successful: int = 0
    captchas: int = 0
    blocks: int = 0
    consecutive_successes: int = 0


class StealthBrowser:
    """
    A stealth Playwright browser that looks like a real user to Amazon.
    Handles session warmup, cookie persistence, and human-like navigation.
    """

    def __init__(self, proxy: Optional[str] = None, headless: bool = True,
                 use_auth: bool = True, playwright_instance=None, browser_instance=None):
        self.proxy = proxy
        self.headless = headless
        self.use_auth = use_auth
        self.stats = BrowserStats()
        self._playwright = playwright_instance
        self._browser: Optional[Browser] = browser_instance
        self._owns_playwright = playwright_instance is None
        self._owns_browser = browser_instance is None
        self._context: Optional[BrowserContext] = None
        self._page: Optional[Page] = None
        self._warmed_up = False
        self._request_count = 0

        # Adaptive delay
        self._base_delay = 2.5
        self._current_delay = self._base_delay

    def start(self):
        """Launch the stealth browser."""
        if self._playwright is None:
            self._playwright = sync_playwright().start()
            self._owns_playwright = True

        viewport = random.choice(VIEWPORTS)
        ua = random.choice(USER_AGENTS)
        locale = random.choice(LOCALES)
        tz = random.choice(TIMEZONES)

        if self._browser is None:
            launch_args = {
                "headless": self.headless,
                "args": [
                    "--disable-blink-features=AutomationControlled",
                    "--disable-features=IsolateOrigins,site-per-process",
                    "--no-first-run",
                    "--no-default-browser-check",
                    f"--window-size={viewport['width']},{viewport['height']}",
                ],
            }

            if self.proxy:
                launch_args["proxy"] = {"server": self.proxy}

            self._browser = self._playwright.chromium.launch(**launch_args)
            self._owns_browser = True

        # If we have a saved Amazon session, use it
        context_kwargs = dict(
            viewport=viewport,
            user_agent=ua,
            locale=locale,
            timezone_id=tz,
            color_scheme="light",
            java_script_enabled=True,
            has_touch=False,
            is_mobile=False,
            device_scale_factor=random.choice([1, 1, 2]),
        )

        if self.use_auth:
            from .auth import has_saved_session, STORAGE_FILE
            if has_saved_session():
                context_kwargs["storage_state"] = STORAGE_FILE
                logger.info("Using saved Amazon session (logged in)")

        self._context = self._browser.new_context(**context_kwargs)

        # Block unnecessary resources to speed things up
        self._context.route("**/*.{png,jpg,jpeg,gif,svg,ico,woff,woff2}", lambda route: route.abort())
        self._context.route("**/ads/**", lambda route: route.abort())
        self._context.route("**/analytics/**", lambda route: route.abort())

        self._page = self._context.new_page()

        # Apply stealth patches
        if HAS_STEALTH:
            stealth_sync(self._page)

        # Additional stealth: override webdriver detection
        self._page.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
            window.chrome = { runtime: {} };
            const originalQuery = window.navigator.permissions.query;
            window.navigator.permissions.query = (parameters) =>
                parameters.name === 'notifications'
                    ? Promise.resolve({ state: Notification.permission })
                    : originalQuery(parameters);
            Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3, 4, 5] });
            Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });
        """)

        logger.info("Stealth browser started")

    def _warmup(self):
        """Visit Amazon homepage to establish cookies and session."""
        if self._warmed_up:
            return

        try:
            logger.info("Warming up session — visiting Amazon homepage...")
            self._page.goto("https://www.amazon.com/", wait_until="domcontentloaded", timeout=30000)
            time.sleep(random.uniform(2, 4))

            # Simulate a bit of scrolling like a real user
            self._page.evaluate("window.scrollTo(0, Math.random() * 500)")
            time.sleep(random.uniform(0.5, 1.5))

            self._warmed_up = True
            logger.info("Session warmed up — cookies established")
        except Exception as e:
            logger.warning(f"Warmup failed: {e}")
            self._warmed_up = True  # Don't retry endlessly

    def _human_delay(self):
        """Wait with human-like timing."""
        if self.stats.consecutive_successes > 15:
            self._current_delay = max(1.5, self._current_delay * 0.95)

        jitter = self._current_delay * random.uniform(-0.3, 0.3)
        delay = self._current_delay + jitter

        # Occasional longer pause
        if random.random() < 0.05:
            delay += random.uniform(3, 10)

        time.sleep(max(1.0, delay))

    def get_page_html(self, url: str, max_retries: int = 3) -> Optional[str]:
        """
        Navigate to a URL and return the page HTML.
        Handles CAPTCHAs, blocks, and retries.
        """
        self._warmup()

        for attempt in range(max_retries):
            self._human_delay()
            self._request_count += 1

            try:
                response = self._page.goto(url, wait_until="domcontentloaded", timeout=30000)
                self.stats.total_requests += 1

                if response is None:
                    continue

                # Wait a moment for dynamic content
                time.sleep(random.uniform(0.5, 1.5))

                html = self._page.content()

                # Check for CAPTCHA
                if "captcha" in html.lower() or "Type the characters you see" in html:
                    self.stats.captchas += 1
                    self.stats.consecutive_successes = 0
                    logger.warning(f"CAPTCHA detected (attempt {attempt+1})")
                    return f"__CAPTCHA__|{html}"

                # Check for sign-in redirect / block
                current_url = self._page.url
                if "ap/signin" in current_url or "authportal" in html:
                    self.stats.blocks += 1
                    self.stats.consecutive_successes = 0
                    logger.warning(f"Sign-in redirect (attempt {attempt+1})")
                    # Go back to homepage to reset
                    self._warmed_up = False
                    self._warmup()
                    time.sleep(random.uniform(5, 15))
                    continue

                # Check for bot block
                if response.status == 503 or "automated access" in html.lower():
                    self.stats.blocks += 1
                    self.stats.consecutive_successes = 0
                    time.sleep(random.uniform(10, 30))
                    continue

                # Check for "Page Not Found" (IP rate limit)
                if len(html) < 10000 and ("Page Not Found" in html or "page not found" in html.lower()):
                    self.stats.blocks += 1
                    self.stats.consecutive_successes = 0
                    logger.warning(f"Page Not Found — IP rate limited (attempt {attempt+1})")
                    time.sleep(random.uniform(15, 30))
                    continue

                if response.status == 200:
                    # Simulate human reading behavior
                    self._page.evaluate("window.scrollTo(0, Math.random() * 300)")
                    self.stats.successful += 1
                    self.stats.consecutive_successes += 1
                    return html

            except Exception as e:
                logger.warning(f"Browser request failed: {e}")
                if attempt < max_retries - 1:
                    time.sleep(random.uniform(3, 8))

        return None

    def solve_captcha_on_page(self) -> bool:
        """
        Attempt to solve a CAPTCHA currently displayed on the page.
        Uses the amazoncaptcha local ML model.
        """
        try:
            from .captcha import solve_captcha_from_url

            # Find the CAPTCHA image
            img = self._page.query_selector("img[src*='captcha'], img[src*='Captcha']")
            if not img:
                return False

            image_url = img.get_attribute("src")
            if not image_url:
                return False

            solution = solve_captcha_from_url(image_url)
            if not solution:
                return False

            # Type the solution
            input_field = self._page.query_selector("input[name='field-keywords'], input#captchacharacters")
            if input_field:
                input_field.fill(solution)
                time.sleep(random.uniform(0.5, 1.0))

                # Click submit
                submit = self._page.query_selector("button[type='submit'], input[type='submit']")
                if submit:
                    submit.click()
                    time.sleep(2)

                    # Check if we got past it
                    html = self._page.content()
                    if "captcha" not in html.lower():
                        logger.info("CAPTCHA solved successfully!")
                        return True

            return False
        except Exception as e:
            logger.error(f"CAPTCHA solving failed: {e}")
            return False

    def restart_context(self):
        """Close and recreate the browser context + page for a fresh session."""
        try:
            if self._context:
                self._context.close()
                self._context = None
                self._page = None
        except Exception:
            pass

        self._warmed_up = False
        self._request_count = 0
        self._current_delay = self._base_delay
        self.stats = BrowserStats()

        # Recreate context and page
        viewport = random.choice(VIEWPORTS)
        ua = random.choice(USER_AGENTS)
        locale = random.choice(LOCALES)
        tz = random.choice(TIMEZONES)

        context_kwargs = dict(
            viewport=viewport,
            user_agent=ua,
            locale=locale,
            timezone_id=tz,
            color_scheme="light",
            java_script_enabled=True,
            has_touch=False,
            is_mobile=False,
            device_scale_factor=random.choice([1, 1, 2]),
        )

        if self.use_auth:
            from .auth import has_saved_session, STORAGE_FILE
            if has_saved_session():
                context_kwargs["storage_state"] = STORAGE_FILE

        self._context = self._browser.new_context(**context_kwargs)
        self._context.route("**/*.{png,jpg,jpeg,gif,svg,ico,woff,woff2}", lambda route: route.abort())
        self._context.route("**/ads/**", lambda route: route.abort())
        self._context.route("**/analytics/**", lambda route: route.abort())

        self._page = self._context.new_page()
        if HAS_STEALTH:
            stealth_sync(self._page)

        self._page.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
            window.chrome = { runtime: {} };
            const originalQuery = window.navigator.permissions.query;
            window.navigator.permissions.query = (parameters) =>
                parameters.name === 'notifications'
                    ? Promise.resolve({ state: Notification.permission })
                    : originalQuery(parameters);
            Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3, 4, 5] });
            Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });
        """)

        logger.info("Browser context restarted with fresh identity")

    def close(self):
        """Clean up browser resources. Only closes what this instance owns."""
        try:
            if self._context:
                self._context.close()
                self._context = None
                self._page = None
            if self._browser and self._owns_browser:
                self._browser.close()
                self._browser = None
            if self._playwright and self._owns_playwright:
                self._playwright.stop()
                self._playwright = None
        except Exception:
            pass
