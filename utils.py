"""
utils.py — Shared utilities for Sathya Agency review scraper.
"""

import json
import re
import hashlib
from datetime import date, datetime, timedelta
from pathlib import Path

BASE_DIR     = Path(__file__).parent
REV_FILE     = BASE_DIR / "rev.json"
DELETED_FILE = BASE_DIR / "deleted.json"

RUNTIME_LABELS = {6: "morning", 12: "afternoon", 18: "evening", 0: "midnight"}


def get_review_date(ist_hour: int) -> str:
    """
    Date to stamp on each scraped review.
    Midnight run (ist_hour=0) uses yesterday's date because those reviews
    were posted before midnight.
    """
    today = date.today()
    if ist_hour == 0:
        return (today - timedelta(days=1)).isoformat()
    return today.isoformat()


def parse_relative_time(text: str) -> bool:
    """Return True if relative-time string is within last ~23 hours."""
    if not text:
        return False
    text = text.strip().lower()
    if text in ("just now", "a moment ago"):
        return True
    m = re.match(r"(\d+)\s*(minute|hour)s?\s*ago", text)
    if m:
        n, unit = int(m.group(1)), m.group(2)
        if unit == "minute" and n <= 1439:
            return True
        if unit == "hour" and n <= 23:
            return True
    return False


def make_review_id(branch_id: int, author: str, text: str, stars: int) -> str:
    """
    Stable content-hash — the identity of a review across ALL runs and dates.

    Fields: branch_id | author (name only) | text | stars

    BUG FIX: `rev_date` was previously included in the hash. This caused
    the same review posted on Day1 to get a NEW id on Day2's 6am run
    (when it still appears as '20 hours ago'), creating cross-day duplicates.

    Date is intentionally EXCLUDED from the hash. The same real review
    always gets the same ID regardless of which run or which day scraped it.
    The `date` field is still stored on the record for display purposes.
    """
    author_clean = author.split("\n")[0].strip()
    raw = f"{branch_id}||{author_clean}||{text}||{stars}"
    return hashlib.sha1(raw.encode()).hexdigest()[:16]


# ─── JSON helpers ─────────────────────────────────────────────────────────────
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
def add_reviews(existing: dict, new_reviews: list) -> tuple:
    """
    Merge new_reviews into existing rev.json dict.
    Same review scraped across multiple runs/days = same hash = stored once.
    Returns (updated_dict, count_added).
    """
    added = 0
    for rev in new_reviews:
        rid = rev["review_id"]
        if rid not in existing:
            existing[rid] = rev
            added += 1
    return existing, added


# ─── Deletion detection ───────────────────────────────────────────────────────
def check_deletions_for_branch(
    branch_id: int,
    scraped_ids_this_run: set,
    rev_data: dict,
) -> list:
    """
    Detect deleted reviews for a single branch.

    BUG FIX: Previous version filtered stored reviews by date (date=rev_date).
    This meant yesterday's reviews (stored under yesterday's date) were never
    checked against today's scrape → deletions never detected.

    CORRECT LOGIC:
    - Filter stored reviews for this branch by `scraped_at` within last 25 hours.
      Why 25h? We scrape up to 23h-old reviews. A review scraped at the previous
      run is at most ~6h old (run interval). 25h covers all reviews that SHOULD
      still be visible on Google Maps right now.
    - If such a "recently scraped" review is NOT in today's scraped_ids → deleted.
    - If scraped_ids is empty (scrape failed) → skip, no false positives.

    Per-branch isolation: only compares within branch_id, so Branch A's reviews
    are never flagged when Branch B is scraped.
    """
    if not scraped_ids_this_run:
        return []

    now = datetime.utcnow()
    cutoff = now - timedelta(hours=25)

    # Find stored reviews for this branch scraped within the last 25 hours
    recently_stored = {}
    for rid, rev in rev_data.items():
        if rev.get("branch_id") != branch_id:
            continue
        try:
            scraped_at = datetime.fromisoformat(rev["scraped_at"])
        except (KeyError, ValueError):
            continue
        if scraped_at >= cutoff:
            recently_stored[rid] = rev

    deleted = []
    for rid, rev in recently_stored.items():
        if rid not in scraped_ids_this_run:
            deleted.append({
                **rev,
                "detected_deleted_on": date.today().isoformat(),
            })
    return deleted


def move_to_deleted(deleted_reviews: list, rev_data: dict) -> int:
    """
    MOVE deleted reviews: remove from rev_data (rev.json) → add to deleted.json.
    Dedup enforced — a review already in deleted.json is never added again.
    Returns count actually moved.
    """
    existing_deleted = load_deleted()
    moved = 0
    for rev in deleted_reviews:
        rid = rev["review_id"]
        if rid not in existing_deleted:
            existing_deleted[rid] = rev
            moved += 1
        # Always remove from rev.json even if already in deleted.json
        rev_data.pop(rid, None)
    if moved:
        save_deleted(existing_deleted)
    return moved


# ─── Google Maps URL ──────────────────────────────────────────────────────────
def maps_url(place_id: str) -> str:
    return f"https://www.google.com/maps/place/?q=place_id:{place_id}"


# ─── Logging ──────────────────────────────────────────────────────────────────
def log(msg: str) -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


# ─── Reactivation: deleted → rev.json (review reappears on Google) ────────────
def reactivate_reviews(scraped_ids_this_run: set, rev_data: dict) -> int:
    """
    If a review in deleted.json is found AGAIN in the current scrape,
    it means Google restored it (or it was a false deletion).
    Move it back to rev.json and remove from deleted.json.

    Call this BEFORE check_deletions_for_branch so the reactivated review
    is in rev_data and won't be flagged as deleted again.

    Returns count of reviews reactivated.
    """
    deleted = load_deleted()
    if not deleted:
        return 0

    reactivated = 0
    to_remove = []

    for rid, rev in deleted.items():
        if rid in scraped_ids_this_run and rid not in rev_data:
            rev_data[rid] = {k: v for k, v in rev.items() if k != "detected_deleted_on"}
            rev_data[rid]["reactivated_on"] = date.today().isoformat()
            to_remove.append(rid)
            reactivated += 1

    if reactivated:
        for rid in to_remove:
            del deleted[rid]
        save_deleted(deleted)

    return reactivated
