"""
Local CAPTCHA solving using the amazoncaptcha ML model.

The amazoncaptcha library ships a pre-trained CNN that solves Amazon's
text CAPTCHAs with ~98% accuracy. No external API calls needed —
runs entirely on your machine.
"""

import re
import logging
from typing import Optional, Tuple
from io import BytesIO

from bs4 import BeautifulSoup

try:
    from amazoncaptcha import AmazonCaptcha
    CAPTCHA_AVAILABLE = True
except ImportError:
    CAPTCHA_AVAILABLE = False

logger = logging.getLogger(__name__)


def extract_captcha_info(html: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Extract CAPTCHA image URL and form action from a CAPTCHA page.
    Returns (image_url, form_action) or (None, None) if not found.
    """
    soup = BeautifulSoup(html, "lxml")

    # Find the CAPTCHA image
    img = soup.find("img", src=re.compile(r"captcha"))
    if not img:
        # Try alternative selectors
        img = soup.find("img", attrs={"alt": re.compile(r"captcha", re.I)})
    if not img:
        imgs = soup.find_all("img")
        for i in imgs:
            src = i.get("src", "")
            if "captcha" in src.lower() or "Captcha" in src:
                img = i
                break

    image_url = img.get("src") if img else None

    # Find the form action URL for submitting the solution
    form = soup.find("form", action=True)
    form_action = form.get("action") if form else None

    # Find the hidden amzn field
    amzn_field = None
    if form:
        hidden = form.find("input", attrs={"name": "amzn"})
        if hidden:
            amzn_field = hidden.get("value")

    return image_url, form_action, amzn_field if form else (image_url, form_action, None)


def solve_captcha_from_url(image_url: str) -> Optional[str]:
    """
    Solve an Amazon CAPTCHA given the image URL.
    Uses the local ML model — no API calls.
    Returns the solution string or None if solving fails.
    """
    if not CAPTCHA_AVAILABLE:
        logger.warning("amazoncaptcha not installed — cannot solve CAPTCHAs locally")
        return None

    try:
        captcha = AmazonCaptcha.fromlink(image_url)
        solution = captcha.solve()

        if solution and solution != "Not solved":
            logger.info(f"CAPTCHA solved locally: {solution}")
            return solution
        else:
            logger.warning("CAPTCHA solving failed — model returned no solution")
            return None

    except Exception as e:
        logger.error(f"CAPTCHA solving error: {e}")
        return None


def handle_captcha(html: str, engine) -> Optional[str]:
    """
    Full CAPTCHA handling pipeline:
    1. Extract CAPTCHA image URL from the page
    2. Solve it with local ML model
    3. Submit the solution
    4. Return the resulting page HTML

    Args:
        html: The CAPTCHA page HTML
        engine: RequestEngine instance to submit the solution

    Returns:
        HTML of the page after CAPTCHA submission, or None on failure
    """
    result = extract_captcha_info(html)
    if len(result) == 3:
        image_url, form_action, amzn_value = result
    else:
        image_url, form_action = result
        amzn_value = None

    if not image_url:
        logger.error("Could not find CAPTCHA image on page")
        return None

    # Solve the CAPTCHA locally
    solution = solve_captcha_from_url(image_url)
    if not solution:
        return None

    # Submit the solution
    if form_action:
        try:
            # Build the submission URL
            if form_action.startswith("/"):
                submit_url = f"https://www.amazon.com{form_action}"
            elif form_action.startswith("http"):
                submit_url = form_action
            else:
                submit_url = f"https://www.amazon.com/{form_action}"

            params = {"field-keywords": solution}
            if amzn_value:
                params["amzn"] = amzn_value

            # Use the engine's session directly for cookie continuity
            import time
            time.sleep(1)  # Brief pause before submitting
            response = engine.session.get(submit_url, params=params, timeout=30)

            if response.status_code == 200:
                result_html = response.text
                # Check if we got another CAPTCHA (wrong answer)
                if "captcha" in result_html.lower() and "Type the characters" in result_html:
                    logger.warning("CAPTCHA solution was incorrect — got another CAPTCHA")
                    return None
                return result_html

        except Exception as e:
            logger.error(f"Failed to submit CAPTCHA solution: {e}")
            return None

    return None
