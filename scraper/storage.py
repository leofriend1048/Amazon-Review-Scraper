"""
SQLite storage for reviews with checkpoint/resume and export.

All scraped reviews are saved to SQLite as they're collected.
If the scraper crashes or is interrupted, it resumes from where it left off.
"""

import os
import json
import sqlite3
import logging
from typing import List, Optional, Set, Tuple
from pathlib import Path

from .parser import Review

logger = logging.getLogger(__name__)

DB_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data")


def get_db_path(asin: str) -> str:
    os.makedirs(DB_DIR, exist_ok=True)
    return os.path.join(DB_DIR, f"{asin}.db")


class ReviewStorage:
    """SQLite-backed review storage with deduplication and checkpointing."""

    def __init__(self, asin: str):
        self.asin = asin
        self.db_path = get_db_path(asin)
        self.conn = sqlite3.connect(self.db_path)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self._create_tables()

    def _create_tables(self):
        self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS reviews (
                review_id TEXT PRIMARY KEY,
                asin TEXT NOT NULL,
                title TEXT,
                body TEXT,
                rating INTEGER,
                date TEXT,
                date_raw TEXT,
                verified_purchase BOOLEAN,
                helpful_votes INTEGER DEFAULT 0,
                author TEXT,
                variant TEXT,
                image_count INTEGER DEFAULT 0,
                scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS checkpoints (
                task_key TEXT PRIMARY KEY,
                last_page INTEGER,
                completed BOOLEAN DEFAULT FALSE,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS scrape_meta (
                asin TEXT PRIMARY KEY,
                total_ratings INTEGER,
                total_reviews INTEGER,
                average_rating REAL,
                star_counts TEXT,
                started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                completed_at TIMESTAMP
            );

            CREATE INDEX IF NOT EXISTS idx_reviews_rating ON reviews(rating);
            CREATE INDEX IF NOT EXISTS idx_reviews_date ON reviews(date);
            CREATE INDEX IF NOT EXISTS idx_reviews_asin ON reviews(asin);
        """)
        self.conn.commit()

    def save_reviews(self, reviews: List[Review]) -> int:
        """
        Save reviews, skipping duplicates. Returns number of NEW reviews saved.
        """
        count_before = self.get_review_count()
        for review in reviews:
            try:
                self.conn.execute("""
                    INSERT OR IGNORE INTO reviews
                    (review_id, asin, title, body, rating, date, date_raw,
                     verified_purchase, helpful_votes, author, variant, image_count)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    review.review_id, review.asin, review.title, review.body,
                    review.rating, review.date, review.date_raw,
                    review.verified_purchase, review.helpful_votes,
                    review.author, review.variant, review.image_count,
                ))
            except sqlite3.IntegrityError:
                pass
        self.conn.commit()
        count_after = self.get_review_count()
        return count_after - count_before

    def get_review_count(self) -> int:
        cursor = self.conn.execute("SELECT COUNT(*) FROM reviews WHERE asin = ?", (self.asin,))
        return cursor.fetchone()[0]

    def get_existing_ids(self) -> Set[str]:
        cursor = self.conn.execute("SELECT review_id FROM reviews WHERE asin = ?", (self.asin,))
        return {row[0] for row in cursor.fetchall()}

    def get_checkpoint(self, task_key: str) -> Optional[int]:
        """Get the last completed page for a task (e.g., 'stars_5_recent')."""
        cursor = self.conn.execute(
            "SELECT last_page, completed FROM checkpoints WHERE task_key = ?",
            (task_key,)
        )
        row = cursor.fetchone()
        if row:
            if row[1]:  # completed
                return -1  # Signal that this task is done
            return row[0]
        return None

    def save_checkpoint(self, task_key: str, page: int, completed: bool = False):
        self.conn.execute("""
            INSERT INTO checkpoints (task_key, last_page, completed, updated_at)
            VALUES (?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(task_key) DO UPDATE SET
                last_page = excluded.last_page,
                completed = excluded.completed,
                updated_at = CURRENT_TIMESTAMP
        """, (task_key, page, completed))
        self.conn.commit()

    def save_meta(self, total_ratings: int, total_reviews: int,
                  average_rating: float, star_counts: dict):
        self.conn.execute("""
            INSERT INTO scrape_meta (asin, total_ratings, total_reviews, average_rating, star_counts)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(asin) DO UPDATE SET
                total_ratings = excluded.total_ratings,
                total_reviews = excluded.total_reviews,
                average_rating = excluded.average_rating,
                star_counts = excluded.star_counts
        """, (self.asin, total_ratings, total_reviews, average_rating, json.dumps(star_counts)))
        self.conn.commit()

    def mark_complete(self):
        self.conn.execute("""
            UPDATE scrape_meta SET completed_at = CURRENT_TIMESTAMP WHERE asin = ?
        """, (self.asin,))
        self.conn.commit()

    def export_csv(self, output_path: str, star_filter: Optional[List[int]] = None):
        """Export reviews to CSV."""
        import pandas as pd
        query = "SELECT * FROM reviews WHERE asin = ?"
        params = [self.asin]
        if star_filter:
            placeholders = ",".join("?" * len(star_filter))
            query += f" AND rating IN ({placeholders})"
            params.extend(star_filter)
        query += " ORDER BY date DESC"

        df = pd.read_sql_query(query, self.conn, params=params)
        df.to_csv(output_path, index=False, encoding="utf-8")
        return len(df)

    def export_json(self, output_path: str, star_filter: Optional[List[int]] = None):
        """Export reviews to JSON."""
        import pandas as pd
        query = "SELECT * FROM reviews WHERE asin = ?"
        params = [self.asin]
        if star_filter:
            placeholders = ",".join("?" * len(star_filter))
            query += f" AND rating IN ({placeholders})"
            params.extend(star_filter)
        query += " ORDER BY date DESC"

        df = pd.read_sql_query(query, self.conn, params=params)
        df.to_json(output_path, orient="records", indent=2)
        return len(df)

    def export_parquet(self, output_path: str, star_filter: Optional[List[int]] = None):
        """Export reviews to Parquet (best for large datasets)."""
        import pandas as pd
        query = "SELECT * FROM reviews WHERE asin = ?"
        params = [self.asin]
        if star_filter:
            placeholders = ",".join("?" * len(star_filter))
            query += f" AND rating IN ({placeholders})"
            params.extend(star_filter)
        query += " ORDER BY date DESC"

        df = pd.read_sql_query(query, self.conn, params=params)
        df.to_parquet(output_path, index=False)
        return len(df)

    def get_stats(self) -> dict:
        """Get summary statistics for the scraped reviews."""
        cursor = self.conn.execute("""
            SELECT
                COUNT(*) as total,
                AVG(rating) as avg_rating,
                SUM(CASE WHEN rating = 5 THEN 1 ELSE 0 END) as five_star,
                SUM(CASE WHEN rating = 4 THEN 1 ELSE 0 END) as four_star,
                SUM(CASE WHEN rating = 3 THEN 1 ELSE 0 END) as three_star,
                SUM(CASE WHEN rating = 2 THEN 1 ELSE 0 END) as two_star,
                SUM(CASE WHEN rating = 1 THEN 1 ELSE 0 END) as one_star,
                SUM(CASE WHEN verified_purchase THEN 1 ELSE 0 END) as verified,
                MIN(date) as earliest,
                MAX(date) as latest
            FROM reviews WHERE asin = ?
        """, (self.asin,))
        row = cursor.fetchone()
        if row:
            return {
                "total": row[0],
                "avg_rating": round(row[1], 2) if row[1] else 0,
                "five_star": row[2],
                "four_star": row[3],
                "three_star": row[4],
                "two_star": row[5],
                "one_star": row[6],
                "verified": row[7],
                "earliest_date": row[8],
                "latest_date": row[9],
            }
        return {}

    def close(self):
        self.conn.close()
