"""
Amazon authentication via saved browser session.

Instead of dealing with username/password programmatically (which Amazon
blocks with 2FA, CAPTCHAs, etc.), we let the user log in once in a real
browser window and then save the cookies for all future scraping sessions.

This is the most reliable approach — once logged in, /product-reviews/
pages work without any sign-in redirects.
"""

import os
import json
import time
import logging
from pathlib import Path
from typing import Optional

from playwright.sync_api import sync_playwright

logger = logging.getLogger(__name__)

COOKIES_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data")
COOKIES_FILE = os.path.join(COOKIES_DIR, "amazon_cookies.json")
STORAGE_FILE = os.path.join(COOKIES_DIR, "amazon_storage_state.json")


def has_saved_session() -> bool:
    """Check if we have a saved login session."""
    return os.path.exists(STORAGE_FILE)


def login_interactive() -> bool:
    """
    Open a real browser window for the user to log into Amazon.
    Once logged in, saves the full browser state (cookies, localStorage)
    for future use.

    Returns True if login was successful.
    """
    os.makedirs(COOKIES_DIR, exist_ok=True)

    print("\n" + "=" * 60)
    print("  Amazon Login")
    print("=" * 60)
    print()
    print("A browser window will open to Amazon's login page.")
    print("Please log in normally (including any 2FA if needed).")
    print()
    print("Once you're on the Amazon homepage and fully logged in,")
    print("come back here and press ENTER to save the session.")
    print()
    print("=" * 60)

    pw = sync_playwright().start()
    browser = pw.chromium.launch(
        headless=False,
        args=[
            "--disable-blink-features=AutomationControlled",
            "--no-first-run",
            "--start-maximized",
        ],
    )

    context = browser.new_context(
        viewport={"width": 1440, "height": 900},
        user_agent=(
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        locale="en-US",
    )

    page = context.new_page()

    # Navigate to Amazon login
    page.goto("https://www.amazon.com/ap/signin?openid.pape.max_auth_age=0&openid.return_to=https%3A%2F%2Fwww.amazon.com%2F&openid.identity=http%3A%2F%2Fspecs.openid.net%2Fauth%2F2.0%2Fidentifier_select&openid.assoc_handle=usflex&openid.mode=checkid_setup&openid.claimed_id=http%3A%2F%2Fspecs.openid.net%2Fauth%2F2.0%2Fidentifier_select&openid.ns=http%3A%2F%2Fspecs.openid.net%2Fauth%2F2.0")

    # Wait for user to log in
    input("\n>>> Press ENTER after you've logged in to Amazon... ")

    # Verify login by checking current page
    current_url = page.url
    html = page.content()

    if "amazon.com" in current_url and "signin" not in current_url:
        # Save the full browser state
        context.storage_state(path=STORAGE_FILE)

        # Also save cookies separately for flexibility
        cookies = context.cookies()
        with open(COOKIES_FILE, "w") as f:
            json.dump(cookies, f, indent=2)

        logger.info(f"Session saved to {STORAGE_FILE}")
        print(f"\nSession saved! ({len(cookies)} cookies)")
        print("You won't need to log in again until the session expires.")

        browser.close()
        pw.stop()
        return True
    else:
        print("\nLogin doesn't appear complete. Please try again.")
        browser.close()
        pw.stop()
        return False


def create_authenticated_context(pw_instance, browser, headless: bool = True):
    """
    Create a browser context with the saved Amazon session.
    Returns a context with all cookies/storage pre-loaded.
    """
    if not has_saved_session():
        raise RuntimeError("No saved session. Run 'python scrape.py login' first.")

    context = browser.new_context(
        storage_state=STORAGE_FILE,
        viewport={"width": 1440, "height": 900},
        user_agent=(
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        locale="en-US",
    )
    return context


def clear_session():
    """Remove saved session data."""
    for f in [COOKIES_FILE, STORAGE_FILE]:
        if os.path.exists(f):
            os.remove(f)
            logger.info(f"Removed {f}")
    print("Session cleared.")
