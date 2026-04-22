"""
utils.py — Shared utilities for Sathya Agency review scraper.
Handles: date assignment, deduplication, JSON I/O, deletion detection.

Deletion logic (per-run, per-branch):
  After each branch is scraped, we compare the freshly-scraped review IDs
  for that branch against what is stored in rev.json for that same branch
  on the same date. Any stored review no longer visible = deleted.
"""

import json
import re
import hashlib
from datetime import date, datetime, timedelta
from pathlib import Path

# ─── Paths ────────────────────────────────────────────────────────────────────
BASE_DIR     = Path(__file__).parent
REV_FILE     = BASE_DIR / "rev.json"
DELETED_FILE = BASE_DIR / "deleted.json"

# ─── IST hour labels (for logging only) ───────────────────────────────────────
RUNTIME_LABELS = {6: "morning", 12: "afternoon", 18: "evening", 0: "midnight"}


def get_review_date(ist_hour: int) -> str:
    """
    Return the date string (YYYY-MM-DD) to stamp on a scraped review.
      - 12 AM run (ist_hour == 0)  -> previous day's date
      - All other runs (6, 12, 18) -> today's date
    """
    today = date.today()
    if ist_hour == 0:
        return (today - timedelta(days=1)).isoformat()
    return today.isoformat()


def parse_relative_time(text: str) -> bool:
    """
    Return True if the relative-time string is within the last ~23 hours.
    Patterns: 'Just now', 'X minute(s) ago', 'X hour(s) ago'
    """
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


def make_review_id(branch_id: int, author: str, text: str, stars: int, rev_date: str) -> str:
    """
    Stable content-hash used as a dedup key across all 4 daily runs.

    Fields used:  branch_id | author (name only) | text | stars | rev_date
    NOT included: relative_time — it increments every run ("4 hours ago" ->
                  "7 hours ago") and would generate a new ID each time for
                  the same real review, causing duplicates.
    """
    author_clean = author.split("\n")[0].strip()
    raw = f"{branch_id}||{author_clean}||{text}||{stars}||{rev_date}"
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
def add_reviews(existing: dict, new_reviews: list) -> tuple[dict, int]:
    """
    Merge new_reviews into existing rev.json dict (keyed by stable review_id).
    Same real review scraped at 6am, 12pm, 6pm, midnight -> same ID -> stored once.
    Returns (updated_dict, count_of_newly_added).
    """
    added = 0
    for rev in new_reviews:
        rid = rev["review_id"]
        if rid not in existing:
            existing[rid] = rev
            added += 1
    return existing, added


# ─── Deletion detection (per-branch, per-run) ─────────────────────────────────
def check_deletions_for_branch(
    branch_id: int,
    scraped_ids_this_run: set,
    rev_date: str,
    existing: dict,
) -> list:
    """
    For a single branch: compare what we just scraped against what is
    stored in rev.json for that branch on the same date.

    How it works:
      stored = all reviews in rev.json with branch_id=X AND date=rev_date
      If a stored review ID is NOT in scraped_ids_this_run
          -> Google Maps no longer shows it -> mark as deleted

    Why per-branch?
      We only compare within the same branch so a review for Branch A
      is never falsely flagged because Branch B was scraped and didn't see it.

    Why same date?
      We only compare reviews from the same calendar date, so reviews from
      yesterday or earlier are never flagged as deleted.

    NOTE: Only flag a deletion if we scraped at least 1 review for that branch
    this run. If we got 0 (scrape failed), we skip to avoid false positives.
    """
    if not scraped_ids_this_run:
        # Scrape failed for this branch — don't flag anything as deleted
        return []

    stored_for_branch = {
        rid: rev
        for rid, rev in existing.items()
        if rev.get("branch_id") == branch_id and rev.get("date") == rev_date
    }

    deleted = []
    for rid, rev in stored_for_branch.items():
        if rid not in scraped_ids_this_run:
            deleted.append({
                **rev,
                "detected_deleted_on": date.today().isoformat(),
            })
    return deleted


def move_to_deleted(deleted_reviews: list, rev_data: dict) -> int:
    """
    MOVE deleted reviews from rev.json -> deleted.json.

    Q1 fix: reviews are REMOVED from rev_data (rev.json) and ADDED to
    deleted.json. After this call the review exists only in deleted.json,
    not in both files.

    Q2: dedup is enforced — a review already in deleted.json is never
    added again (it was already moved in a previous run).

    Args:
        deleted_reviews : list of review dicts detected as deleted
        rev_data        : the live rev.json dict (mutated in-place)

    Returns count of reviews actually moved (newly added to deleted.json).
    """
    existing_deleted = load_deleted()
    moved = 0
    for rev in deleted_reviews:
        rid = rev["review_id"]
        # Add to deleted.json (dedup — skip if already there)
        if rid not in existing_deleted:
            existing_deleted[rid] = rev
            moved += 1
        # Always remove from rev.json, whether it was newly moved or not
        rev_data.pop(rid, None)
    if moved:
        save_deleted(existing_deleted)
    return moved


# ─── Google Maps URL builder ──────────────────────────────────────────────────
def maps_url(place_id: str) -> str:
    return f"https://www.google.com/maps/place/?q=place_id:{place_id}"


# ─── Logging helper ───────────────────────────────────────────────────────────
def log(msg: str) -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)
