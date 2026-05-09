"""
scraper.py — Sathya Agency review scraper. Single midnight run.
Usage: python scraper.py [--force]
"""

import sys
import re
import json
import time
import hashlib
import argparse
import datetime
import subprocess
from pathlib import Path
from datetime import date, timedelta

# ── Config ────────────────────────────────────────────────────────────────────
BASE_DIR        = Path(__file__).parent
REV_FILE        = BASE_DIR / "rev.json"
DELETED_FILE    = BASE_DIR / "deleted.json"

MAX_SCROLLS     = 2000
STALL_LIMIT     = 5
SCROLL_PX       = 2000
SCROLL_DELAY    = 700          # ms between scrolls
MAX_RUN_SECS    = 170 * 60    # 170 min total budget   ← was MAX_RUN)SECS (syntax error)
MAX_BRANCH_SECS = 4 * 60      # 4 min per branch

BRAVE_PATHS = [
    "/usr/bin/brave-browser",
    "/usr/bin/brave",
    "/opt/brave.com/brave/brave",
    "/Applications/Brave Browser.app/Contents/MacOS/Brave Browser",
    r"C:\Program Files\BraveSoftware\Brave-Browser\Application\brave.exe",
]

from branches import BRANCHES, AGM_MAP  # noqa: E402  (project-local)

# ── Logging ───────────────────────────────────────────────────────────────────
def log(msg: str) -> None:
    print(f"[{datetime.datetime.now():%Y-%m-%d %H:%M:%S}] {msg}", flush=True)

# ── Date helpers ──────────────────────────────────────────────────────────────
def get_review_date() -> str:
    """Yesterday's date as ISO string (reviews posted 'today' show yesterday
    at midnight because the cron fires just after midnight IST)."""
    return (date.today() - timedelta(days=1)).isoformat()

def parse_relative_time(t: str) -> bool:
    """Return True if the timestamp string means 'within the last 24 hours'."""
    t = (t or "").strip().lower()
    if t in ("just now", "a moment ago", "now"):
        return True
    # "N minutes/hours ago" — long form
    m = re.match(r"(\d+)\s*(minute|hour)s?\s*ago", t)
    if m:
        n, unit = int(m.group(1)), m.group(2)
        return unit == "minute" or (unit == "hour" and n <= 23)
    # "Nm/Nh ago" — short form
    m = re.match(r"(\d+)([mh])\s*ago", t)
    if m:
        n, unit = int(m.group(1)), m.group(2)
        return unit == "m" or (unit == "h" and n <= 23)
    return False

def is_day_old(t: str) -> bool:
    """Return True if the timestamp string means 'about 1–2 days ago'.
    Used to detect reviews that were present yesterday so we can spot deletions."""
    t = (t or "").strip().lower()
    return bool(
        re.match(r"(a|1)\s*day\s*ago", t)
        or re.match(r"2\s*days?\s*ago", t)
        or re.match(r"[12]d\s*ago", t)
    )

# ── ID & fingerprint helpers ──────────────────────────────────────────────────
def make_review_id(branch_id: str, author: str, text: str, stars: int) -> str:
    """Stable 16-char SHA-1 hex ID.  Uses only the first line of author
    so that 'Name\\nN reviews' variants still hash identically."""
    author_line = author.split("\n")[0].strip()
    raw = f"{branch_id}||{author_line}||{text}||{stars}"
    return hashlib.sha1(raw.encode()).hexdigest()[:16]

def _norm(s: str) -> str:
    """Normalise text for duplicate detection: collapse whitespace, lowercase,
    strip trailing punctuation."""
    return re.sub(r"[\s.,!?…]+$", "", re.sub(r"\s+", " ", s.lower().strip()))

# ── Persistence ───────────────────────────────────────────────────────────────
def _load(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}

def _save(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8"
    )

def load_reviews() -> dict:  return _load(REV_FILE)
def save_reviews(d: dict):   _save(REV_FILE, d)
def load_deleted() -> dict:  return _load(DELETED_FILE)
def save_deleted(d: dict):   _save(DELETED_FILE, d)

# ── Review lifecycle ──────────────────────────────────────────────────────────
def add_reviews(existing: dict, new_reviews: list) -> tuple[dict, int]:
    """Merge new_reviews into existing; return (merged_dict, count_added)."""
    added = 0
    for r in new_reviews:
        if r["review_id"] not in existing:
            existing[r["review_id"]] = r
            added += 1
    return existing, added


def reactivate_reviews(scraped_ids: set, rev_data: dict) -> int:
    """If a review we just saw is sitting in deleted.json, move it back to
    rev_data (it was un-deleted by the author or restored by Google)."""
    deleted = load_deleted()
    to_restore, to_remove = {}, []

    for rid, rev in deleted.items():
        if rid in scraped_ids and rid not in rev_data:
            clean = {k: v for k, v in rev.items() if k != "detected_deleted_on"}
            clean["reactivated_on"] = date.today().isoformat()
            clean["date"]           = date.today().isoformat()
            to_restore[rid] = clean
            to_remove.append(rid)

    if to_restore:
        rev_data.update(to_restore)
        for rid in to_remove:
            del deleted[rid]
        save_deleted(deleted)

    return len(to_restore)


def check_deletions(branch_id: str, day_old_ids: set, rev_data: dict) -> list:
    """Compare yesterday's stored reviews against the IDs that showed up as
    'day-old' during the scrape.  Any stored review that is missing from the
    live page is considered deleted.

    BUG FIX vs original: the original compared rev["date"] == yesterday, which
    also matched reviews that were *just added* in the same run (they also carry
    yesterday's date).  We now only look at reviews whose scraped_at timestamp
    predates today — i.e. they genuinely existed in a previous run."""
    today     = date.today().isoformat()
    yesterday = (date.today() - timedelta(days=1)).isoformat()

    potentially_deleted = []
    for rid, rev in rev_data.items():
        if rev.get("branch_id") != branch_id:
            continue
        if rev.get("date") != yesterday:
            continue
        # Only flag reviews that were saved in a *previous* run.
        scraped_at = rev.get("scraped_at", "")
        if scraped_at.startswith(today):
            # Added in this very run — cannot be deleted yet.
            continue
        if rid not in day_old_ids:
            potentially_deleted.append({**rev, "detected_deleted_on": today})

    return potentially_deleted


def move_to_deleted(deleted_revs: list, rev_data: dict) -> int:
    """Move confirmed-deleted reviews from rev_data into deleted.json."""
    if not deleted_revs:
        return 0
    existing = load_deleted()
    moved = 0
    for rev in deleted_revs:
        rid = rev["review_id"]
        if rid not in existing:
            existing[rid] = rev
            moved += 1
        rev_data.pop(rid, None)
    if moved:
        save_deleted(existing)
    return moved

# ── Migration ─────────────────────────────────────────────────────────────────
def needs_migration() -> bool:
    """Return True if any stored review has a stale/duplicate ID."""
    data = load_reviews()
    seen: set = set()
    for rev in data.values():
        try:
            nid = make_review_id(
                rev["branch_id"], rev["author"], rev["text"], rev["stars"]
            )
        except Exception:
            continue
        if rev.get("review_id") != nid or nid in seen:
            return True
        seen.add(nid)
    return False

def run_migration() -> None:
    script = BASE_DIR / "migrate_clean.py"
    if script.exists():
        subprocess.run([sys.executable, str(script)], check=False)

# ── Google Maps URL ───────────────────────────────────────────────────────────
def maps_url(place_id: str) -> str:
    return f"https://www.google.com/maps/place/?q=place_id:{place_id}"

# ── Playwright helpers ────────────────────────────────────────────────────────
TIME_SELS = [
    "span.XfOne",
    'div[class*="DUxS3d"]',
    ".rsqaWe",
    'span[aria-label*="ago"]',
    'span[aria-label*="now"]',
]

CARD_SELS = [
    "div[data-review-id]",
    'div[class*="MyEned"]',
    'div[jslog*="review"]',
    'div[jscontroller][class*="review"]',
]


def _card_time(card) -> str:
    """Extract the relative-time string from a review card element."""
    for sel in TIME_SELS:
        try:
            el = card.locator(sel).first
            if el.count():
                return el.inner_text(timeout=1000).strip()
        except Exception:
            pass
    return ""


def _card_count(page) -> int:
    """Best-effort count of review cards visible on the page."""
    best = 0
    for sel in CARD_SELS:
        try:
            best = max(best, page.locator(sel).count())
        except Exception:
            pass
    return best


def _all_cards(page) -> list:
    """Return de-duplicated list of review card elements across all selectors."""
    cards: list = []
    seen: set   = set()
    for sel in CARD_SELS:
        try:
            for card in page.locator(sel).all():
                try:
                    # Use data-review-id when present, else a prefix of outerHTML.
                    key = card.evaluate(
                        "el => el.dataset.reviewId || el.outerHTML.substring(0, 200)"
                    )
                    if key not in seen:
                        seen.add(key)
                        cards.append(card)
                except Exception:
                    pass
        except Exception:
            pass
    return cards


def _open_branch(page, place_id: str) -> None:
    """Navigate to the place page and switch to the 'Newest' reviews tab."""
    page.goto(maps_url(place_id), wait_until="domcontentloaded", timeout=30_000)
    page.wait_for_timeout(3000)

    # Click the Reviews tab.
    for sel in ['button[aria-label*="Reviews"]', '[data-tab-index="1"]']:
        try:
            tab = page.locator(sel).first
            if tab.is_visible(timeout=3000):
                tab.click()
                page.wait_for_timeout(2000)
                break
        except Exception:
            pass

    # Switch sort order to Newest.
    try:
        sort_btn = page.locator(
            'button[aria-label*="Sort"],[data-value="Sort"]'
        ).first
        if sort_btn.is_visible(timeout=3000):
            sort_btn.click()
            page.wait_for_timeout(800)
            newest = page.locator('li[aria-label*="Newest"],[data-index="1"]').first
            if newest.is_visible(timeout=3000):
                newest.click()
                page.wait_for_timeout(2000)
    except Exception:
        pass


def _parse_card(card, page, branch_id: str, branch_name: str,
                place_id: str, review_date: str, agm: str,
                snapshot: dict):
    """Parse one review card.  Returns a review dict or None."""
    try:
        rt = _card_time(card)
        if not rt:
            return None

        is_fresh = parse_relative_time(rt)
        is_old   = is_day_old(rt)
        if not is_fresh and not is_old:
            return None  # Older than our window — skip.

        # ── Author ──────────────────────────────────────────────────────────
        author = "Anonymous"
        for sel in ['div[class*="d4r55"]', ".WNxzHc button", "a.al6Kxe"]:
            try:
                el = card.locator(sel).first
                if el.count():
                    author = el.inner_text(timeout=2000).strip()
                    break
            except Exception:
                pass

        # ── Star rating ──────────────────────────────────────────────────────
        stars = 0
        for sel in [
            'span[aria-label*="star"]',
            'span[aria-label*="Star"]',
            'div[aria-label*="star"]',
        ]:
            try:
                lbl = card.locator(sel).first.get_attribute("aria-label", timeout=1000) or ""
                digits = "".join(filter(str.isdigit, lbl.split("star")[0][-2:]))
                if digits:
                    stars = min(int(digits), 5)
                    break
            except Exception:
                pass

        # ── Expand "See more" ────────────────────────────────────────────────
        try:
            more_btn = card.locator(
                'button[aria-label*="See more"],button.w8nwRe'
            ).first
            if more_btn.is_visible(timeout=500):
                more_btn.click()
                page.wait_for_timeout(400)
        except Exception:
            pass

        # ── Review text ──────────────────────────────────────────────────────
        text = ""
        try:
            text = card.locator('span[class*="wiI7pd"],.MyEned span').first.inner_text(
                timeout=2000
            ).strip()
        except Exception:
            pass

        if not stars and not text:
            return None  # Nothing useful parsed.

        # ── De-duplicate against in-memory snapshot ──────────────────────────
        author_key = author.split("\n")[0].strip().lower()
        text_key   = _norm(text)

        rid = make_review_id(branch_id, author, text, stars)

        # If the same logical review exists under a different ID (e.g. text
        # was edited slightly), reuse the stored ID.
        existing_match = next(
            (
                r for r in snapshot.values()
                if r.get("branch_id") == branch_id
                and r.get("author", "").split("\n")[0].strip().lower() == author_key
                and _norm(r.get("text", "")) == text_key
            ),
            None,
        )
        if existing_match:
            rid = existing_match["review_id"]

        return {
            "review_id":     rid,
            "_fp":           (author_key, branch_id, text_key, rt),
            "_old":          is_old,
            "branch_id":     branch_id,
            "branch_name":   branch_name,
            "place_id":      place_id,
            "agm":           agm,
            "author":        author,
            "stars":         stars,
            "relative_time": rt,
            "text":          text,
            "date":          review_date,
            "scraped_at":    datetime.datetime.now().isoformat(),
            "method":        "playwright",
        }
    except Exception:
        return None


def scrape_branch(page, branch_id: str, branch_name: str,
                  place_id: str, review_date: str,
                  snapshot: dict) -> tuple[list, set]:
    """
    Scroll through a branch's review panel, collect fresh (<24 h) and
    day-old (24-48 h) reviews.

    Returns:
        fresh_reviews  — list of review dicts ready to store
        day_old_ids    — set of review IDs seen with a 'day-old' timestamp
                         (used to detect deletions)
    """
    log(f"  → {branch_name}")

    try:
        _open_branch(page, place_id)
    except Exception as exc:
        log(f"  ERROR opening {branch_name}: {exc}")
        return [], set()

    # Locate the scrollable reviews panel (optional — fall back to keyboard).
    panel = None
    try:
        candidate = page.locator('div[aria-label*="Reviews"]').first
        if candidate.count():
            panel = candidate
    except Exception:
        pass

    # ── Scroll until stalled or time budget exhausted ─────────────────────
    branch_start = time.time()
    # BUG FIX: original used `prev_pos = prev_n = stall = -1, 0, 0`
    # which assigned the tuple (-1, 0, 0) to *all three* variables.
    prev_pos: int = -1
    prev_n:   int = 0
    stall:    int = 0

    for i in range(MAX_SCROLLS):
        if time.time() - branch_start > MAX_BRANCH_SECS:
            log(f"  {branch_name}: branch time-budget reached at scroll {i + 1}")
            break

        # Scroll inside the panel element when possible; otherwise use End key.
        if panel:
            try:
                panel.evaluate(f"el => el.scrollTop += {SCROLL_PX}")
            except Exception:
                page.keyboard.press("End")
        else:
            page.keyboard.press("End")

        page.wait_for_timeout(SCROLL_DELAY)

        # Measure scroll position and card count to detect stalling.
        current_pos: int = 0
        if panel:
            try:
                current_pos = int(panel.evaluate("el => el.scrollTop") or 0)
            except Exception:
                current_pos = 0

        current_n = _card_count(page)

        if current_pos == prev_pos and current_n == prev_n:
            stall += 1
            if stall >= STALL_LIMIT:
                log(f"  {branch_name}: stalled at scroll {i + 1}")
                break
        else:
            stall = 0

        prev_pos = current_pos
        prev_n   = current_n

    # ── Parse cards ───────────────────────────────────────────────────────
    agm         = AGM_MAP.get(branch_name, "Unknown")
    fresh:      list = []
    day_old_ids: set = set()
    seen_fps:   set  = set()

    for card in _all_cards(page):
        r = _parse_card(
            card, page, branch_id, branch_name,
            place_id, review_date, agm, snapshot
        )
        if r is None:
            continue
        fp = r["_fp"]
        if fp in seen_fps:
            continue
        seen_fps.add(fp)

        if r["_old"]:
            day_old_ids.add(r["review_id"])

        if parse_relative_time(r["relative_time"]):
            fresh.append({k: v for k, v in r.items() if not k.startswith("_")})

    log(f"  {branch_name}: {len(fresh)} fresh, {len(day_old_ids)} day-old")
    return fresh, day_old_ids


# ── Main run ──────────────────────────────────────────────────────────────────
def run() -> list:
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        log("playwright is not installed — run: pip install playwright && playwright install")
        return []

    review_date = get_review_date()
    log(f"[scraper] midnight run — review_date={review_date}")

    brave_exe = next((p for p in BRAVE_PATHS if Path(p).exists()), None)
    if brave_exe:
        log(f"[scraper] using Brave: {brave_exe}")

    existing  = load_reviews()
    snapshot  = dict(existing)   # frozen copy used for duplicate-detection
    all_new:  list = []
    total_added = total_deleted = total_reactivated = 0
    run_start   = time.time()

    with sync_playwright() as pw:
        launch_kwargs: dict = {
            "headless": True,
            "args": [
                "--no-sandbox",
                "--disable-blink-features=AutomationControlled",
                "--disable-dev-shm-usage",
            ],
        }
        if brave_exe:
            launch_kwargs["executable_path"] = brave_exe

        browser = pw.chromium.launch(**launch_kwargs)
        ctx     = browser.new_context(
            viewport   = {"width": 1280, "height": 900},
            user_agent = (
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            ),
            locale = "en-US",
        )

        # Three parallel browser pages — one per branch in each batch.
        pages = [ctx.new_page() for _ in range(3)]

        for batch_start in range(0, len(BRANCHES), 3):
            if time.time() - run_start > MAX_RUN_SECS:
                log("[scraper] total time budget reached — stopping early")
                break

            batch = BRANCHES[batch_start : batch_start + 3]

            for tab_idx, (branch_id, branch_name, place_id) in enumerate(batch):
                fresh, day_old_ids = scrape_branch(
                    pages[tab_idx],
                    branch_id, branch_name, place_id,
                    review_date, snapshot,
                )
                all_scraped_ids = {r["review_id"] for r in fresh} | day_old_ids

                # 1. Reactivate any previously-deleted reviews we saw again.
                nr = reactivate_reviews(all_scraped_ids, existing)
                total_reactivated += nr
                if nr:
                    log(f"  {branch_name}: {nr} reactivated")

                # 2. Add genuinely new reviews.
                existing, added = add_reviews(existing, fresh)
                total_added += added
                all_new.extend(fresh)

                # 3. Detect and archive deleted reviews.
                deleted_revs = check_deletions(branch_id, day_old_ids, existing)
                nd = move_to_deleted(deleted_revs, existing)
                total_deleted += nd
                if nd:
                    log(f"  {branch_name}: {nd} moved to deleted.json")

            time.sleep(1)  # polite pause between batches

        browser.close()

    save_reviews(existing)
    log(
        f"[scraper] done — scraped={len(all_new)} added={total_added} "
        f"deleted={total_deleted} reactivated={total_reactivated}"
    )
    return all_new


# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sathya Agency review scraper")
    parser.add_argument("--force", action="store_true",
                        help="Run outside the midnight window")
    args = parser.parse_args()

    now_ist  = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(
        hours=5, minutes=30
    )
    ist_hour = now_ist.hour
    if not args.force and ist_hour != 0:
        log(f"Not midnight IST (hour={ist_hour}). Use --force to override.")
        sys.exit(0)

    if needs_migration():
        log("[scraper] running migration …")
        run_migration()

    results = run()
    sys.exit(0 if results is not None else 1)
