"""
HTTP request engine with TLS fingerprint impersonation and anti-detection.

Uses curl_cffi to impersonate real browser TLS fingerprints — this is the
single most important anti-detection measure. Amazon fingerprints TLS
handshakes and blocks known bot signatures.
"""

import random
import time
from dataclasses import dataclass, field
from typing import Optional

from curl_cffi import requests as curl_requests

# Real browser fingerprints that curl_cffi can impersonate
BROWSER_FINGERPRINTS = [
    "chrome120",
    "chrome119",
    "chrome116",
    "chrome110",
    "chrome107",
    "chrome104",
    "chrome101",
    "chrome100",
    "edge101",
    "edge99",
    "safari15_5",
    "safari15_3",
    "safari17_0",
]

# Realistic desktop user agents (matched to fingerprints)
USER_AGENTS = {
    "chrome": [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36",
    ],
    "edge": [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36 Edg/119.0.0.0",
    ],
    "safari": [
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.5 Safari/605.1.15",
    ],
}

# Accept-Language variations
ACCEPT_LANGUAGES = [
    "en-US,en;q=0.9",
    "en-US,en;q=0.9,es;q=0.8",
    "en-US,en;q=0.8",
    "en-GB,en;q=0.9,en-US;q=0.8",
    "en-US,en;q=0.9,fr;q=0.8",
]

# Amazon-specific referer patterns
REFERERS = [
    "https://www.amazon.com/",
    "https://www.amazon.com/s?k=",
    "https://www.amazon.com/gp/browse.html",
    None,  # Sometimes no referer is more natural
]


@dataclass
class RequestStats:
    """Track request statistics for adaptive rate limiting."""
    total_requests: int = 0
    successful: int = 0
    captchas: int = 0
    blocks: int = 0
    errors: int = 0
    last_captcha_at: int = 0
    last_block_at: int = 0
    consecutive_successes: int = 0

    @property
    def block_rate(self) -> float:
        if self.total_requests == 0:
            return 0.0
        return (self.captchas + self.blocks) / self.total_requests

    @property
    def is_healthy(self) -> bool:
        return self.consecutive_successes >= 5 and self.block_rate < 0.1


@dataclass
class SessionIdentity:
    """A complete browser identity for a scraping session."""
    fingerprint: str
    user_agent: str
    accept_language: str
    viewport_width: int
    viewport_height: int
    proxy: Optional[str] = None

    @classmethod
    def random(cls, proxy: Optional[str] = None) -> "SessionIdentity":
        fp = random.choice(BROWSER_FINGERPRINTS)
        if "chrome" in fp:
            ua = random.choice(USER_AGENTS["chrome"])
        elif "edge" in fp:
            ua = random.choice(USER_AGENTS["edge"])
        else:
            ua = random.choice(USER_AGENTS["safari"])

        widths = [1366, 1440, 1536, 1920, 2560]
        heights = [768, 900, 864, 1080, 1440]
        idx = random.randint(0, len(widths) - 1)

        return cls(
            fingerprint=fp,
            user_agent=ua,
            accept_language=random.choice(ACCEPT_LANGUAGES),
            viewport_width=widths[idx],
            viewport_height=heights[idx],
            proxy=proxy,
        )


class RequestEngine:
    """
    HTTP engine with TLS fingerprint impersonation, session rotation,
    and adaptive rate limiting.
    """

    def __init__(self, proxy: Optional[str] = None):
        self.proxy = proxy
        self.identity = SessionIdentity.random(proxy)
        self.session = self._create_session()
        self.stats = RequestStats()
        self._request_count = 0
        self._session_max = random.randint(40, 80)  # Rotate session every 40-80 requests
        self._warmed_up = False

        # Adaptive delay parameters (seconds)
        self._base_delay = 2.0
        self._min_delay = 1.5
        self._max_delay = 15.0
        self._current_delay = self._base_delay

    def _create_session(self) -> curl_requests.Session:
        session = curl_requests.Session(impersonate=self.identity.fingerprint)
        session.headers.update(self._build_headers())
        if self.identity.proxy:
            session.proxies = {
                "http": self.identity.proxy,
                "https": self.identity.proxy,
            }
        return session

    def _build_headers(self) -> dict:
        headers = {
            "User-Agent": self.identity.user_agent,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": self.identity.accept_language,
            "Accept-Encoding": "gzip, deflate, br",
            "DNT": "1",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "same-origin",
            "Sec-Fetch-User": "?1",
            "Cache-Control": "max-age=0",
            "sec-ch-ua-platform": '"Windows"' if "Windows" in self.identity.user_agent else '"macOS"',
        }
        referer = random.choice(REFERERS)
        if referer:
            headers["Referer"] = referer
        return headers

    def _warmup(self):
        """
        Visit Amazon's homepage first to establish cookies and session.
        Without this, Amazon redirects review pages to a sign-in page.
        This mimics how a real user would browse — land on homepage first.
        """
        if self._warmed_up:
            return

        try:
            # First request: looks like typing amazon.com in the URL bar
            self.session.headers.update({
                "Sec-Fetch-Site": "none",
                "Sec-Fetch-User": "?1",
                "Referer": "",
            })
            self.session.get("https://www.amazon.com/", timeout=15)
            time.sleep(random.uniform(1, 3))

            # Reset headers back to same-origin navigation
            self.session.headers.update({
                "Sec-Fetch-Site": "same-origin",
                "Referer": "https://www.amazon.com/",
            })
            self._warmed_up = True
        except Exception:
            pass  # Best effort — proceed even if warmup fails

    def rotate_session(self):
        """Create a new browser identity and session."""
        try:
            self.session.close()
        except Exception:
            pass
        self.identity = SessionIdentity.random(self.proxy)
        self.session = self._create_session()
        self._request_count = 0
        self._session_max = random.randint(40, 80)
        self._warmed_up = False

    def _adaptive_delay(self):
        """Human-like delay that adapts to detection signals."""
        if self.stats.consecutive_successes > 20:
            # Things are going well, can speed up slightly
            self._current_delay = max(self._min_delay, self._current_delay * 0.95)
        elif self.stats.blocks > 0 or self.stats.captchas > 0:
            recent_trouble = (
                self.stats.total_requests - self.stats.last_block_at < 10
                or self.stats.total_requests - self.stats.last_captcha_at < 10
            )
            if recent_trouble:
                self._current_delay = min(self._max_delay, self._current_delay * 1.5)

        # Add human-like jitter: ±30%
        jitter = self._current_delay * random.uniform(-0.3, 0.3)
        delay = self._current_delay + jitter

        # Occasional longer pause (mimics reading a page)
        if random.random() < 0.05:
            delay += random.uniform(5, 15)

        time.sleep(max(0.5, delay))

    def get(self, url: str, max_retries: int = 3) -> Optional[str]:
        """
        Fetch a URL with full anti-detection, retries, and adaptive delays.
        Returns HTML string or None on failure.
        """
        # Warmup: visit Amazon homepage first to get session cookies
        self._warmup()

        for attempt in range(max_retries):
            # Rotate session if we've used it enough
            self._request_count += 1
            if self._request_count >= self._session_max:
                self.rotate_session()

            self._adaptive_delay()

            try:
                response = self.session.get(url, timeout=30)
                self.stats.total_requests += 1
                html = response.text

                # Check for CAPTCHA
                if "captcha" in html.lower() or "Type the characters you see" in html:
                    self.stats.captchas += 1
                    self.stats.last_captcha_at = self.stats.total_requests
                    self.stats.consecutive_successes = 0
                    return f"__CAPTCHA__|{html}"

                # Check for bot block / sign-in redirect
                is_blocked = (
                    response.status_code == 503
                    or "automated access" in html.lower()
                    or ("authportal" in html and "signIn" in html)
                    or "ap_email" in html
                )
                if is_blocked:
                    self.stats.blocks += 1
                    self.stats.last_block_at = self.stats.total_requests
                    self.stats.consecutive_successes = 0
                    # Back off and retry with new session
                    time.sleep(random.uniform(10, 30))
                    self.rotate_session()
                    continue

                if response.status_code == 404:
                    return None

                if response.status_code == 200:
                    self.stats.successful += 1
                    self.stats.consecutive_successes += 1
                    return html

                # Other error
                self.stats.errors += 1
                self.stats.consecutive_successes = 0
                time.sleep(random.uniform(3, 8))

            except Exception as e:
                self.stats.errors += 1
                self.stats.consecutive_successes = 0
                if attempt < max_retries - 1:
                    time.sleep(random.uniform(5, 15))

        return None

    def close(self):
        try:
            self.session.close()
        except Exception:
            pass
