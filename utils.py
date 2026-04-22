"""
utils.py — Shared utilities for Sathya Agency review scraper.
Handles: date assignment, deduplication, JSON I/O, deletion detection.
"""

import json
import os
import re
import hashlib
import random
from datetime import date, datetime, timedelta
from pathlib import Path

# ─── Paths ────────────────────────────────────────────────────────────────────
BASE_DIR      = Path(__file__).parent
REV_FILE      = BASE_DIR / "rev.json"
DELETED_FILE  = BASE_DIR / "deleted.json"
DELETION_META = BASE_DIR / ".deletion_meta.json"   # tracks last deletion-check date

# ─── Runtime tags ─────────────────────────────────────────────────────────────
# Maps cron hour (UTC+5:30 → IST) to a label used in logs
RUNTIME_LABELS = {6: "morning", 12: "afternoon", 18: "evening", 0: "midnight"}


def get_review_date(ist_hour: int) -> str:
    """
    Return the date string (YYYY-MM-DD) to stamp on a scraped review.
    - 12 AM run  → previous day's date
    - All others → today's date
    """
    today = date.today()
    if ist_hour == 0:
        return (today - timedelta(days=1)).isoformat()
    return today.isoformat()


def parse_relative_time(text: str) -> bool:
    """
    Return True if the relative-time string is within the last ~23 hours.
    Accepted patterns: 'Just now', 'X minute(s) ago', 'X hour(s) ago'
    """
    if not text:
        return False
    text = text.strip().lower()
    if text in ("just now", "a moment ago"):
        return True
    m = re.match(r"(\d+)\s*(minute|hour)s?\s*ago", text)
    if m:
        n, unit = int(m.group(1)), m.group(2)
        if unit == "minute" and n <= 1439:   # up to 23 h 59 m
            return True
        if unit == "hour" and n <= 23:
            return True
    return False


def make_review_id(branch_id: int, author: str, text: str, stars: int, date: str) -> str:
    """
    Stable hash used as a dedup key.
    Uses: branch_id + author (name only, strip profile noise) + text + stars + date.
    
    IMPORTANT: relative_time ("4 hours ago", "7 hours ago"...) is intentionally
    EXCLUDED because it changes every run and would create duplicate entries for
    the same real review scraped at different times of day.
    """
    author_clean = author.split("\n")[0].strip()
    raw = f"{branch_id}||{author_clean}||{text}||{stars}||{date}"
    return hashlib.sha1(raw.encode()).hexdigest()[:16]


# ─── JSON helpers ──────────────────────────────────────────────────────────────
def _load_json(path: Path) -> dict:
    if path.exists():
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def _save_json(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def load_reviews() -> dict:
    return _load_json(REV_FILE)


def save_reviews(data: dict) -> None:
    _save_json(REV_FILE, data)


def load_deleted() -> dict:
    return _load_json(DELETED_FILE)


def save_deleted(data: dict) -> None:
    _save_json(DELETED_FILE, data)


# ─── Deduplication ────────────────────────────────────────────────────────────
def add_reviews(existing: dict, new_reviews: list) -> tuple[dict, int]:
    """
    Merge new_reviews into existing dict (keyed by review_id).
    Returns (updated_dict, count_added).

    Each review dict must have keys:
        review_id, branch_id, branch_name, agm, author, stars,
        relative_time, text, date, scraped_at, method
    """
    added = 0
    for rev in new_reviews:
        rid = rev["review_id"]
        if rid not in existing:
            existing[rid] = rev
            added += 1
    return existing, added


# ─── Deletion detection ───────────────────────────────────────────────────────
def should_check_deletions() -> bool:
    """
    Returns True if we haven't done a deletion check in the last 5–10 days
    (randomised to spread load).
    """
    meta = _load_json(DELETION_META)
    last_str = meta.get("last_check")
    if not last_str:
        return True
    last = date.fromisoformat(last_str)
    interval = random.randint(5, 10)
    return (date.today() - last).days >= interval


def record_deletion_check() -> None:
    _save_json(DELETION_META, {"last_check": date.today().isoformat()})


def find_deleted_reviews(current_place_ids: list[str], existing: dict) -> list[dict]:
    """
    Compare existing stored reviews for each branch against the freshly
    scraped place_id list. Any review whose review_id is no longer seen
    in a fresh full-scrape is considered deleted.

    NOTE: This function expects `current_place_ids` to be the set of
    review_ids that were just scraped (full history, not just recent).
    Reviews in `existing` NOT in that set are marked deleted.
    """
    deleted = []
    current_set = set(current_place_ids)
    for rid, rev in existing.items():
        if rid not in current_set:
            deleted.append({**rev, "detected_deleted_on": date.today().isoformat()})
    return deleted


def save_newly_deleted(deleted_reviews: list[dict]) -> int:
    """Append newly detected deleted reviews to deleted.json. Returns count added."""
    existing = load_deleted()
    added = 0
    for rev in deleted_reviews:
        rid = rev["review_id"]
        if rid not in existing:
            existing[rid] = rev
            added += 1
    save_deleted(existing)
    return added


# ─── Google Maps URL builder ───────────────────────────────────────────────────
def maps_url(place_id: str) -> str:
    return f"https://www.google.com/maps/place/?q=place_id:{place_id}"


def maps_reviews_url(place_id: str) -> str:
    """Direct deep-link to the reviews tab."""
    return (
        f"https://www.google.com/maps/search/?api=1"
        f"&query=sathya+agency&query_place_id={place_id}"
    )


# ─── Logging helper ───────────────────────────────────────────────────────────
def log(msg: str) -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)
