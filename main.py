"""
main.py — Orchestrator for Sathya Agency review scraper.

Run order: Playwright → Selenium → PyAutoGUI (stops at first success).
Also runs migrate_clean automatically if rev.json still has old-format
duplicate entries (detectable by checking if any two entries share the
same content-hash under the new scheme).

Usage:
    python main.py                    # auto-detect IST hour
    python main.py --hour 10          # 10 AM run
    python main.py --hour 16          # 4 PM run
    python main.py --hour 20          # 8 PM run
    python main.py --hour 0           # midnight run
    python main.py --method playwright
"""

import sys
import argparse
import datetime
from pathlib import Path

from utils import log, load_reviews, make_review_id

# Valid IST hours for the 4 scheduled runs
VALID_IST_HOURS = {0, 10, 16, 20}


def get_ist_hour() -> int:
    return (datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=5, minutes=30)).hour


def needs_migration() -> bool:
    """
    Detect if rev.json still has duplicates from the old hash (date was included).
    If any two entries share the same new stable hash → migration needed.
    Also detect if any entry's review_id doesn't match the new hash scheme.
    """
    data = load_reviews()
    if not data:
        return False

    seen_hashes = set()
    for rev in data.values():
        try:
            new_id = make_review_id(
                rev["branch_id"], rev["author"], rev["text"], rev["stars"]
            )
        except (KeyError, TypeError):
            continue
        # If stored review_id doesn't match new stable hash → old format
        if rev.get("review_id") != new_id:
            return True
        # If two entries would produce the same new hash → duplicates exist
        if new_id in seen_hashes:
            return True
        seen_hashes.add(new_id)
    return False


def run_migration():
    """Run migrate_clean.py automatically."""
    import subprocess
    script = Path(__file__).parent / "migrate_clean.py"
    if not script.exists():
        log("[main] migrate_clean.py not found, skipping migration")
        return
    log("[main] Running migrate_clean.py to clean up old-format duplicates...")
    result = subprocess.run(
        [sys.executable, str(script)],
        capture_output=False
    )
    if result.returncode == 0:
        log("[main] Migration completed successfully")
    else:
        log(f"[main] Migration exited with code {result.returncode}")


def main():
    parser = argparse.ArgumentParser(description="Sathya Agency Review Scraper")
    parser.add_argument("--hour", type=int, default=None,
                        help="IST hour: 0 (midnight), 10 (10am), 16 (4pm), 20 (8pm)")
    parser.add_argument("--method", type=str, default=None,
                        choices=["playwright", "selenium", "pyautogui"])
    args = parser.parse_args()

    ist_hour = args.hour if args.hour is not None else get_ist_hour()

    # Warn if an unexpected hour was passed (not one of the 4 scheduled ones)
    if ist_hour not in VALID_IST_HOURS:
        log(f"[main] WARNING: ist_hour={ist_hour} is not a standard run hour "
            f"{sorted(VALID_IST_HOURS)}. Proceeding anyway.")

    log(f"[main] IST hour = {ist_hour}")

    # ── Auto-migration check ────────────────────────────────────────────────
    if needs_migration():
        log("[main] Old-format duplicates detected in rev.json — running migration")
        run_migration()
    else:
        log("[main] rev.json is clean, no migration needed")

    # ── Run scrapers in priority order ──────────────────────────────────────
    methods = [args.method] if args.method else ["playwright", "selenium", "pyautogui"]

    for method in methods:
        log(f"[main] ── Trying method: {method} ──")
        try:
            if method == "playwright":
                import scraper_playwright
                results = scraper_playwright.run(ist_hour)
            elif method == "selenium":
                import scraper_selenium
                results = scraper_selenium.run(ist_hour)
            elif method == "pyautogui":
                import scraper_pyautogui
                results = scraper_pyautogui.run(ist_hour)
            else:
                results = []

            if results is not None:
                log(f"[main] ✓ Method '{method}' completed. "
                    f"{len(results)} reviews scraped this run.")
                sys.exit(0)

        except Exception as e:
            log(f"[main] ✗ Method '{method}' raised an exception: {e}")
            continue

    log("[main] ✗ All methods failed.")
    sys.exit(1)


if __name__ == "__main__":
    main()
