"""
Amazon review HTML parser.

Extracts structured review data from Amazon product review pages.
Uses data-hook attributes which are Amazon's own testing hooks —
more stable than class names which change frequently.
"""

import re
from dataclasses import dataclass, asdict
from typing import List, Optional
from datetime import datetime

from bs4 import BeautifulSoup


@dataclass
class Review:
    """A single Amazon product review."""
    review_id: str
    asin: str
    title: str
    body: str
    rating: int
    date: str
    date_raw: str
    verified_purchase: bool
    helpful_votes: int
    author: str
    variant: str  # e.g., "Color: Blue, Size: Large"
    image_count: int

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class ReviewPageInfo:
    """Metadata about a review listing page."""
    total_reviews: int
    total_ratings: int
    star_counts: dict  # {1: count, 2: count, ...}
    average_rating: float
    asin: str


def parse_review_count(text: str) -> int:
    """Extract number from strings like '1,234 global ratings' or '12,345'."""
    if not text:
        return 0
    nums = re.findall(r'[\d,]+', text.replace(',', '').replace('.', ''))
    if nums:
        return int(nums[0])
    return 0


def parse_helpful_votes(text: str) -> int:
    """Extract count from 'X people found this helpful' or 'One person found this helpful'."""
    if not text:
        return 0
    text = text.strip().lower()
    if "one person" in text:
        return 1
    nums = re.findall(r'([\d,]+)', text)
    if nums:
        return int(nums[0].replace(',', ''))
    return 0


def parse_date(text: str) -> tuple:
    """
    Parse Amazon date string like 'Reviewed in the United States on January 15, 2024'
    Returns (iso_date_string, raw_text)
    """
    if not text:
        return ("", "")
    match = re.search(r'on\s+(\w+\s+\d{1,2},\s+\d{4})', text)
    if match:
        date_str = match.group(1)
        try:
            dt = datetime.strptime(date_str, "%B %d, %Y")
            return (dt.strftime("%Y-%m-%d"), date_str)
        except ValueError:
            return ("", date_str)
    return ("", text.strip())


def parse_rating(element) -> int:
    """Extract star rating from the rating element."""
    if not element:
        return 0
    # Rating is in class like "a-star-4" or in the text "4.0 out of 5 stars"
    class_str = " ".join(element.get("class", []))
    match = re.search(r'a-star-(\d)', class_str)
    if match:
        return int(match.group(1))
    text = element.get_text()
    match = re.search(r'(\d)\.0 out of', text)
    if match:
        return int(match.group(1))
    return 0


def parse_reviews(html: str, asin: str) -> List[Review]:
    """
    Parse all reviews from an Amazon review page HTML.
    Returns list of Review objects.
    """
    soup = BeautifulSoup(html, "lxml")
    reviews = []

    # Each review has data-hook="review" — can be <div> or <li> depending on page
    review_divs = soup.find_all(attrs={"data-hook": "review"})

    for div in review_divs:
        try:
            # Review ID
            review_id = div.get("id", "")

            # Title — in <a data-hook="review-title"> or <span data-hook="review-title">
            title_el = div.find(attrs={"data-hook": "review-title"})
            title = ""
            if title_el:
                # The title span is usually the last span inside the link
                spans = title_el.find_all("span")
                if spans:
                    title = spans[-1].get_text(strip=True)
                else:
                    title = title_el.get_text(strip=True)

            # Rating
            rating_el = div.find("i", attrs={"data-hook": "review-star-rating"})
            if not rating_el:
                rating_el = div.find("i", attrs={"data-hook": "cmps-review-star-rating"})
            rating = parse_rating(rating_el)

            # Review body
            body_el = div.find("span", attrs={"data-hook": "review-body"})
            body = ""
            if body_el:
                body = body_el.get_text(strip=True)

            # Date
            date_el = div.find("span", attrs={"data-hook": "review-date"})
            date_text = date_el.get_text(strip=True) if date_el else ""
            date_iso, date_raw = parse_date(date_text)

            # Verified purchase
            verified_el = div.find("span", attrs={"data-hook": "avp-badge"})
            verified = verified_el is not None

            # Helpful votes
            helpful_el = div.find("span", attrs={"data-hook": "helpful-vote-statement"})
            helpful_votes = parse_helpful_votes(helpful_el.get_text() if helpful_el else "")

            # Author
            author_el = div.find("span", class_="a-profile-name")
            author = author_el.get_text(strip=True) if author_el else ""

            # Product variant (color, size, etc.)
            variant_el = div.find("a", attrs={"data-hook": "format-strip"})
            variant = variant_el.get_text(strip=True) if variant_el else ""

            # Image count
            image_els = div.find_all("img", attrs={"data-hook": "review-image-tile"})
            image_count = len(image_els)

            review = Review(
                review_id=review_id,
                asin=asin,
                title=title,
                body=body,
                rating=rating,
                date=date_iso,
                date_raw=date_raw,
                verified_purchase=verified,
                helpful_votes=helpful_votes,
                author=author,
                variant=variant,
                image_count=image_count,
            )
            reviews.append(review)

        except Exception:
            # Skip malformed reviews rather than crashing
            continue

    return reviews


def parse_review_summary(html: str, asin: str) -> Optional[ReviewPageInfo]:
    """
    Parse the review summary from the top of the review listing page.
    Gets total counts, star distribution, average rating.
    """
    soup = BeautifulSoup(html, "lxml")

    try:
        # Average rating
        avg_el = soup.find("span", attrs={"data-hook": "rating-out-of-text"})
        avg_rating = 0.0
        if avg_el:
            match = re.search(r'([\d.]+)\s+out of', avg_el.get_text())
            if match:
                avg_rating = float(match.group(1))

        # Total ratings count
        total_el = soup.find("div", attrs={"data-hook": "total-review-count"})
        total_ratings = 0
        if total_el:
            total_ratings = parse_review_count(total_el.get_text())

        # Star distribution — histogram rows
        star_counts = {}
        histogram = soup.find_all("tr", class_=re.compile("histogram"))
        if not histogram:
            # Alternative: look for the percentage bars
            histogram = soup.find_all("a", title=re.compile(r"\d+ star"))

        for row in histogram:
            text = row.get_text()
            # Match patterns like "5 star 70%" or "5 star\n70%"
            match = re.search(r'(\d)\s*star.*?(\d+)%', text, re.DOTALL)
            if match:
                stars = int(match.group(1))
                pct = int(match.group(2))
                star_counts[stars] = int(total_ratings * pct / 100)

        # Total reviews (with text, may differ from total ratings)
        review_count_el = soup.find("div", attrs={"data-hook": "cr-filter-info-review-rating-count"})
        total_reviews = total_ratings
        if review_count_el:
            text = review_count_el.get_text()
            # "1,234 total ratings, 567 with reviews"
            match = re.search(r'([\d,]+)\s+with reviews', text.replace(',', ''))
            if match:
                total_reviews = int(match.group(1))
            else:
                match = re.search(r'([\d,]+)\s+total ratings', text.replace(',', ''))
                if match:
                    total_reviews = int(match.group(1))

        return ReviewPageInfo(
            total_reviews=total_reviews,
            total_ratings=total_ratings,
            star_counts=star_counts,
            average_rating=avg_rating,
            asin=asin,
        )

    except Exception:
        return None


def has_next_page(html: str) -> bool:
    """Check if there's a next page of reviews."""
    soup = BeautifulSoup(html, "lxml")

    # New Amazon layout: "show-more-button" data-hook
    show_more = soup.find(attrs={"data-hook": "show-more-button"})
    if show_more:
        return True

    # Old layout: li.a-last with an active link
    next_btn = soup.find("li", class_="a-last")
    if next_btn:
        link = next_btn.find("a")
        return link is not None and "a-disabled" not in " ".join(next_btn.get("class", []))

    # Fallback: check if pageNumber+1 URL exists anywhere
    if "paging_btm_next" in html or "pageNumber=" in html:
        return True

    return False
