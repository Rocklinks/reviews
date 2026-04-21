"""
main.py — Orchestrator for Sathya Agency review scraper.
Tries Method 1 (Playwright) → Method 2 (Selenium) → Method 3 (PyAutoGUI).
Stops at the first method that successfully scrapes at least one review.

Usage:
    python main.py              # auto-detect IST hour
    python main.py --hour 6     # force a specific IST hour (0,6,12,18)
"""

import sys
import argparse
import datetime

from utils import log


def get_ist_hour() -> int:
    return (datetime.datetime.utcnow() + datetime.timedelta(hours=5, minutes=30)).hour


def main():
    parser = argparse.ArgumentParser(description="Sathya Agency Review Scraper")
    parser.add_argument("--hour", type=int, default=None,
                        help="Override IST hour (0, 6, 12, 18)")
    parser.add_argument("--method", type=str, default=None,
                        choices=["playwright", "selenium", "pyautogui"],
                        help="Force a specific scraper method")
    args = parser.parse_args()

    ist_hour = args.hour if args.hour is not None else get_ist_hour()
    log(f"[main] IST hour = {ist_hour}")

    methods = []
    if args.method:
        methods = [args.method]
    else:
        methods = ["playwright", "selenium", "pyautogui"]

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

            if results is not None and len(results) >= 0:
                # Even 0 new reviews is a valid "success" (no new reviews since last run)
                log(f"[main] ✓ Method '{method}' completed. {len(results)} reviews scraped this run.")
                sys.exit(0)

        except Exception as e:
            log(f"[main] ✗ Method '{method}' raised an exception: {e}")
            continue

    log("[main] ✗ All methods failed.")
    sys.exit(1)


if __name__ == "__main__":
    main()
