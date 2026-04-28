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

RUNTIME_LABELS = {10: "morning", 16: "afternoon", 20: "evening", 0: "midnight"}


def get_review_date(ist_hour: int) -> str:
    """
    Date to stamp on each scraped review.
    Midnight run (ist_hour=0) uses yesterday's date because the reviews
    seen at midnight belong to the closing of the previous calendar day.
    """
    today = date.today()
    if ist_hour == 0:
        return (today - timedelta(days=1)).isoformat()
    return today.isoformat()


def parse_relative_time(text: str) -> bool:
    """Return True if relative-time string is within last ~24 hours."""
    if not text:
        return False
    text = text.strip().lower()
    if text in ("just now", "a moment ago", "now"):
        return True
    # Handle "X minute(s) ago", "X hour(s) ago", "X day(s) ago" patterns
    m = re.match(r"(\d+)\s*(minute|hour|day)s?\s*ago", text)
    if m:
        n, unit = int(m.group(1)), m.group(2)
        if unit == "minute" and n <= 1439:
            return True
        if unit == "hour" and n <= 24:
            return True
        if unit == "day" and n <= 1:   # "1 day ago" is within 24h window
            return True
    # Also match compact forms like "5m ago" or "2h ago"
    m2 = re.match(r"(\d+)([mhd])\s*ago", text)
    if m2:
        n, unit = int(m2.group(1)), m2.group(2)
        if unit == "m" and n <= 1439:
            return True
        if unit == "h" and n <= 24:
            return True
        if unit == "d" and n <= 1:
            return True
    return False


def make_review_id(branch_id: int, author: str, text: str, stars: int) -> str:
    """
    Stable content-hash — identity of a review across ALL runs and dates.
    Fields: branch_id | author (name only, no profile suffix) | text | stars
    Date is intentionally excluded so the same review always gets the same ID.
    """
    author_clean = author.split("\n")[0].strip()
    raw = f"{branch_id}||{author_clean}||{text}||{stars}"
    return hashlib.sha1(raw.encode()).hexdigest()[:16]


# ─── JSON helpers ─────────────────────────────────────────────────────────────
def _load_json(path: Path) -> dict:
    """
    Load a JSON file as a dict.
    BUG FIX: If the file contains a list [] instead of an object {},
    or is empty/corrupt, return {} instead of crashing.
    This was the cause of: 'list indices must be integers or slices, not str'
    """
    if not path.exists():
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            return data
        # File was somehow saved as a list — reset to empty dict
        log(f"[utils] WARNING: {path.name} contained a list, resetting to {{}}")
        return {}
    except (json.JSONDecodeError, Exception) as e:
        log(f"[utils] WARNING: could not read {path.name}: {e} — starting fresh")
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
    Same review scraped across multiple runs/days = same stable hash = stored once.
    Returns (updated_dict, count_newly_added).
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
    ist_hour: int,
) -> list:
    """
    Detect deleted reviews for a single branch.

    Which stored reviews are we checking?
    ┌────────────┬──────────────────────────────────────────────────────────┐
    │  Run       │  Check reviews with rev["date"] ==                       │
    ├────────────┼──────────────────────────────────────────────────────────┤
    │ 10am       │  yesterday only (first run of day, Google shows fresh    │
    │            │  reviews; if yesterday's review is gone it's deleted)    │
    ├────────────┼──────────────────────────────────────────────────────────┤
    │ 4pm / 8pm  │  yesterday AND today (today's 10am reviews + yesterday   │
    │            │  carry-overs all need to be confirmed still present)     │
    ├────────────┼──────────────────────────────────────────────────────────┤
    │ midnight   │  yesterday only (midnight stamps as yesterday, so we     │
    │            │  check yesterday-dated reviews not seen this scrape)     │
    └────────────┴──────────────────────────────────────────────────────────┘

    If a previously-visible review for this branch is NOT in
    scraped_ids_this_run → it has disappeared → mark as deleted.
    """
    if not scraped_ids_this_run:
        return []

    today     = date.today().isoformat()
    yesterday = (date.today() - timedelta(days=1)).isoformat()

    # Determine which date-buckets to check
    if ist_hour == 10:
        check_dates = {yesterday}
    elif ist_hour in (16, 20):
        check_dates = {today, yesterday}
    elif ist_hour == 0:
        # midnight run stamps reviews as yesterday
        check_dates = {yesterday}
    else:
        # Fallback for ad-hoc / unknown hours — check both
        check_dates = {today, yesterday}

    recently_stored = {
        rid: rev
        for rid, rev in rev_data.items()
        if rev.get("branch_id") == branch_id
        and rev.get("date", "") in check_dates
    }

    deleted = []
    for rid, rev in recently_stored.items():
        if rid not in scraped_ids_this_run:
            deleted.append({
                **rev,
                "detected_deleted_on": today,
            })
    return deleted


def move_to_deleted(deleted_reviews: list, rev_data: dict) -> int:
    """
    MOVE deleted reviews from rev.json → deleted.json.
    - Removes from rev_data in-place (caller must call save_reviews() after)
    - Appends to deleted.json (deduped — same review never added twice)
    Returns count actually moved.
    """
    if not deleted_reviews:
        return 0
    existing_deleted = load_deleted()
    moved = 0
    for rev in deleted_reviews:
        rid = rev["review_id"]
        if rid not in existing_deleted:
            existing_deleted[rid] = rev
            moved += 1
        # Always remove from rev_data even if it was already in deleted.json
        rev_data.pop(rid, None)
    if moved:
        save_deleted(existing_deleted)
    return moved


# ─── Reactivation: deleted.json → rev.json ────────────────────────────────────
def reactivate_reviews(scraped_ids_this_run: set, rev_data: dict) -> int:
    """
    If a review from deleted.json re-appears in a current scrape,
    Google has restored it (or it was a false-positive deletion).

    Moves the review back into rev.json with:
      - 'reactivated_on' set to today
      - 'date' updated to today (so future deletion checks use the right window)
      - 'detected_deleted_on' stripped out

    MUST be called BEFORE check_deletions_for_branch so the reactivated
    review is present in rev_data and won't be immediately re-flagged.

    Returns count of reviews reactivated.
    """
    if not scraped_ids_this_run:
        return 0

    deleted = load_deleted()
    if not deleted:
        return 0

    reactivated = 0
    to_remove   = []

    for rid, rev in deleted.items():
        if rid in scraped_ids_this_run and rid not in rev_data:
            clean = {k: v for k, v in rev.items()
                     if k != "detected_deleted_on"}
            clean["reactivated_on"] = date.today().isoformat()
            clean["date"]           = date.today().isoformat()
            rev_data[rid] = clean
            to_remove.append(rid)
            reactivated += 1

    if reactivated:
        for rid in to_remove:
            del deleted[rid]
        save_deleted(deleted)

    return reactivated


# ─── Deletion meta helpers (kept for pyautogui backward-compat) ───────────────
def should_check_deletions() -> bool:
    """Always returns True — deletion check runs every scrape."""
    return True


def record_deletion_check() -> None:
    """Mark that a deletion check was performed today."""
    meta_file = BASE_DIR / ".deletion_meta.json"
    data = {"last_check": date.today().isoformat()}
    _save_json(meta_file, data)


# ─── Legacy deletion helpers (used by pyautogui scraper) ─────────────────────
def find_deleted_reviews(all_scraped_ids: list, rev_data: dict,
                         ist_hour: int = -1) -> list:
    """
    Legacy helper: find reviews that were previously stored but absent
    from the current scrape.

    ist_hour must now be passed explicitly (was incorrectly a free variable).
    """
    deleted = []
    today     = date.today().isoformat()
    yesterday = (date.today() - timedelta(days=1)).isoformat()

    if ist_hour == 0:
        check_dates = {yesterday}          # midnight stamps as yesterday
    elif ist_hour == 10:
        check_dates = {yesterday}          # morning: first run, check prev day
    else:
        check_dates = {today, yesterday}   # afternoon/evening: check both

    for rid, rev in rev_data.items():
        if rev.get("date") in check_dates and rid not in all_scraped_ids:
            deleted.append({**rev, "detected_deleted_on": today})
    return deleted


def save_newly_deleted(deleted_reviews: list, rev_data: dict) -> int:
    """Legacy: move newly deleted reviews from rev.json → deleted.json."""
    if not deleted_reviews:
        return 0
    existing_deleted = load_deleted()
    moved = 0
    for rev in deleted_reviews:
        rid = rev["review_id"]
        if rid not in existing_deleted:
            existing_deleted[rid] = rev
            moved += 1
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
