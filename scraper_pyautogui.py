"""
scraper_pyautogui.py — Method 3: PyAutoGUI + keyboard/mouse OS-level automation.
Last resort fallback. Opens Brave via subprocess, controls via keyboard shortcuts,
and reads page source via pyperclip + clipboard hack.

NOTE: Requires a real display (DISPLAY env var or Xvfb on Linux).
      On GitHub Actions use: sudo apt-get install -y xvfb && Xvfb :99 &
"""

import sys
import time
import subprocess
import datetime
import re
import json
import pyperclip
from pathlib import Path

from branches import BRANCHES, AGM_MAP
from utils import (
    log, get_review_date, parse_relative_time, make_review_id,
    load_reviews, save_reviews, add_reviews, maps_url,
    find_deleted_reviews, save_newly_deleted
)

BRAVE_BINS = [
    "brave-browser",
    "brave",
    "/opt/brave.com/brave/brave",
    "/Applications/Brave Browser.app/Contents/MacOS/Brave Browser",
    r"C:\Program Files\BraveSoftware\Brave-Browser\Application\brave.exe",
]


def find_brave_bin() -> str:
    import shutil
    for b in BRAVE_BINS:
        if shutil.which(b) or Path(b).exists():
            return b
    return None


def open_brave_with_tabs(urls: list[str]) -> subprocess.Popen | None:
    """Launch Brave with multiple URLs (each becomes a tab)."""
    brave = find_brave_bin()
    if not brave:
        log("[pyautogui] Brave not found.")
        return None
    cmd = [brave, "--new-window"] + urls
    try:
        proc = subprocess.Popen(cmd)
        time.sleep(4)  # wait for window to appear
        return proc
    except Exception as e:
        log(f"[pyautogui] Failed to open Brave: {e}")
        return None


def focus_brave():
    """Try to bring Brave window to foreground using xdotool (Linux)."""
    try:
        subprocess.run(
            ["xdotool", "search", "--name", "Brave", "windowactivate", "--sync"],
            timeout=5, capture_output=True
        )
    except Exception:
        pass


def get_page_source_via_clipboard() -> str:
    """
    Press Ctrl+U (view-source), Ctrl+A, Ctrl+C to grab page source via clipboard.
    Very hacky but works as absolute last resort.
    """
    import pyautogui
    # Open view-source
    pyautogui.hotkey("ctrl", "u")
    time.sleep(2)
    # Select all text
    pyautogui.hotkey("ctrl", "a")
    time.sleep(0.5)
    # Copy
    pyautogui.hotkey("ctrl", "c")
    time.sleep(0.5)
    source = pyperclip.paste()
    # Close the view-source tab
    pyautogui.hotkey("ctrl", "w")
    time.sleep(0.5)
    return source


def navigate_to(url: str):
    """Use Ctrl+L to focus address bar and type URL."""
    import pyautogui
    focus_brave()
    pyautogui.hotkey("ctrl", "l")
    time.sleep(0.4)
    pyautogui.hotkey("ctrl", "a")
    pyautogui.typewrite(url, interval=0.03)
    pyautogui.press("enter")
    time.sleep(4)


def switch_to_tab(n: int):
    """Switch to tab n (1-indexed) using Ctrl+1..3."""
    import pyautogui
    focus_brave()
    pyautogui.hotkey("ctrl", str(n))
    time.sleep(0.5)


def sort_by_newest_keystrokes():
    """Attempt to click Sort > Newest via Tab/Enter navigation. Best-effort."""
    import pyautogui
    # Press Tab multiple times to try reaching sort button — very fragile
    # This is last-resort; we do a simpler: look for the text in source
    pass


def parse_reviews_from_source(source: str, branch_id: int, branch_name: str,
                               place_id: str, review_date: str) -> list[dict]:
    """
    Parse review data from raw page HTML/JSON source.
    Google Maps embeds review data as JSON in <script> tags.
    """
    reviews = []
    agm = AGM_MAP.get(branch_name, "Unknown")

    # Pattern 1: look for review JSON blobs
    # Google often embeds: ["John Doe",null,null,null,["2 hours ago",...],5,...]
    # We do a rough heuristic parse
    time_patterns = [
        r'"(Just now)"',
        r'"(\d+\s+(?:minute|hour)s?\s+ago)"',
        r'"(a moment ago)"',
    ]

    found_times = []
    for pat in time_patterns:
        for m in re.finditer(pat, source, re.IGNORECASE):
            found_times.append(m.group(1))

    # Try to extract author names near time strings
    # Look for pattern: ["AuthorName", ... "X hours ago" ... starRating]
    review_blocks = re.findall(
        r'\["([^"]{2,50})",null,null,null,\["([^"]+ago[^"]*|Just now)"',
        source
    )

    seen = set()
    for author, rel_time in review_blocks:
        if not parse_relative_time(rel_time):
            continue
        if (author, rel_time) in seen:
            continue
        seen.add((author, rel_time))

        # Try to get star rating nearby (look for integer 1-5 in vicinity)
        stars = 0
        review_id = make_review_id(branch_id, author, "", stars)
        reviews.append({
            "review_id":     review_id,
            "branch_id":     branch_id,
            "branch_name":   branch_name,
            "place_id":      place_id,
            "agm":           agm,
            "author":        author,
            "stars":         stars,
            "relative_time": rel_time,
            "text":          "",   # hard to parse from raw source
            "date":          review_date,
            "scraped_at":    datetime.datetime.now().isoformat(),
            "method":        "pyautogui",
        })

    return reviews


def scrape_branch_pyautogui(tab_n: int, branch_id: int, branch_name: str,
                              place_id: str, review_date: str) -> list[dict]:
    import pyautogui
    url = maps_url(place_id)
    log(f"  [pyautogui] → {branch_name} (tab {tab_n})")

    switch_to_tab(tab_n)
    navigate_to(url)

    # Try clicking Reviews tab via keyboard (Tab x N, Enter)
    # This is fragile; we just wait and grab source
    time.sleep(3)

    # Scroll down to load reviews
    for _ in range(6):
        pyautogui.hotkey("ctrl", "end") if sys.platform == "win32" else None
        pyautogui.press("end")
        time.sleep(1)

    # Get page source via clipboard
    source = get_page_source_via_clipboard()
    reviews = parse_reviews_from_source(source, branch_id, branch_name, place_id, review_date)
    log(f"  [pyautogui] {branch_name}: {len(reviews)} recent reviews")
    return reviews


def run(ist_hour: int | None = None) -> list[dict]:
    try:
        import pyautogui
        import pyperclip
    except ImportError:
        log("[pyautogui] pyautogui/pyperclip not installed. Skipping.")
        return []

    # Set failsafe
    import pyautogui as pag
    pag.FAILSAFE = False
    pag.PAUSE = 0.3

    if ist_hour is None:
        ist_hour = (datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=5, minutes=30)).hour

    review_date = get_review_date(ist_hour)
    log(f"[pyautogui] Starting. IST hour={ist_hour}, review_date={review_date}")

    all_new_reviews = []
    do_deletion_check = should_check_deletions()
    all_scraped_ids: set[str] = set()

    # Process branches in batches of 3 tabs
    for i in range(0, len(BRANCHES), 3):
        batch = BRANCHES[i:i+3]
        urls = [maps_url(pid) for _, _, pid in batch]

        # Open Brave with 3 tabs for this batch
        proc = open_brave_with_tabs(urls)
        if proc is None:
            log("[pyautogui] Cannot open Brave. Skipping batch.")
            continue

        time.sleep(3)

        for tab_idx, (bid, name, pid) in enumerate(batch, start=1):
            revs = scrape_branch_pyautogui(tab_idx, bid, name, pid, review_date)
            all_new_reviews.extend(revs)
            for r in revs:
                all_scraped_ids.add(r["review_id"])

        # Close Brave window
        try:
            import pyautogui as pag
            pag.hotkey("ctrl", "shift", "w")  # close window
            time.sleep(1)
            proc.terminate()
        except Exception:
            pass
        time.sleep(2)

    existing = load_reviews()
    existing, added = add_reviews(existing, all_new_reviews)
    save_reviews(existing)
    log(f"[pyautogui] Done. {added} new reviews added.")

    if all_scraped_ids:
        log("[pyautogui] Running deletion check…")
        deleted = find_deleted_reviews(list(all_scraped_ids), existing, ist_hour)
        n = save_newly_deleted(deleted, existing)   # ← pass existing
        save_reviews(existing)                       # ← persist the removals
        log(f"[pyautogui] Deletion check: {n} newly deleted reviews saved")

    return all_new_reviews


if __name__ == "__main__":
    results = run()
    print(f"Total reviews scraped: {len(results)}")
