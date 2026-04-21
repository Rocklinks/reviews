"""
scraper_playwright.py — Method 1: Playwright-based Google Maps review scraper.
Opens Brave browser (falls back to Chromium), navigates to each branch,
scrapes reviews from the last 23 hours, saves to rev.json.
"""

import sys
import json
import time
import datetime
from pathlib import Path

# Local imports
from branches import BRANCHES, AGM_MAP
from utils import (
    log, get_review_date, parse_relative_time, make_review_id,
    load_reviews, save_reviews, add_reviews, maps_url,
    should_check_deletions, record_deletion_check,
    find_deleted_reviews, save_newly_deleted
)

# ─── Brave binary locations by OS ─────────────────────────────────────────────
BRAVE_PATHS = [
    # Linux (GitHub Actions / Ubuntu)
    "/usr/bin/brave-browser",
    "/usr/bin/brave",
    "/opt/brave.com/brave/brave",
    # macOS
    "/Applications/Brave Browser.app/Contents/MacOS/Brave Browser",
    # Windows
    r"C:\Program Files\BraveSoftware\Brave-Browser\Application\brave.exe",
    r"C:\Program Files (x86)\BraveSoftware\Brave-Browser\Application\brave.exe",
]


def find_brave() -> str | None:
    for p in BRAVE_PATHS:
        if Path(p).exists():
            return p
    return None


# ─── Per-branch scraping ───────────────────────────────────────────────────────
def scrape_branch_playwright(page, branch_id: int, branch_name: str,
                              place_id: str, review_date: str) -> list[dict]:
    """Navigate to a branch's Google Maps page and collect recent reviews."""
    reviews = []
    url = maps_url(place_id)
    agm = AGM_MAP.get(branch_name, "Unknown")

    try:
        log(f"  [playwright] → {branch_name} ({place_id})")
        page.goto(url, wait_until="domcontentloaded", timeout=30000)
        page.wait_for_timeout(3000)

        # Click the "Reviews" tab if present
        try:
            reviews_tab = page.locator('button[aria-label*="Reviews"], [data-tab-index="1"]').first
            if reviews_tab.is_visible(timeout=5000):
                reviews_tab.click()
                page.wait_for_timeout(2000)
        except Exception:
            pass

        # Sort by Newest
        try:
            sort_btn = page.locator('button[aria-label*="Sort"], [data-value="Sort"]').first
            if sort_btn.is_visible(timeout=5000):
                sort_btn.click()
                page.wait_for_timeout(1000)
                newest_opt = page.locator('li[aria-label*="Newest"], [data-index="1"]').first
                if newest_opt.is_visible(timeout=3000):
                    newest_opt.click()
                    page.wait_for_timeout(2000)
        except Exception:
            pass

        # Scroll to load reviews
        for _ in range(5):
            page.keyboard.press("End")
            page.wait_for_timeout(1500)

        # Extract review cards
        # Google Maps uses JDIV/aria structure; these selectors target review blocks
        review_cards = page.locator('div[data-review-id], div[jscontroller][class*="review"]').all()

        # Fallback: grab by relative time text presence
        if not review_cards:
            review_cards = page.locator('div[class*="MyEned"], div[jslog*="review"]').all()

        for card in review_cards:
            try:
                # Relative time
                rel_time_el = card.locator('span[class*="dehysf"], .rsqaWe, span[aria-label*="ago"], span[aria-label*="now"]').first
                rel_time = rel_time_el.inner_text(timeout=2000).strip()

                if not parse_relative_time(rel_time):
                    continue

                # Author name
                try:
                    author_el = card.locator('div[class*="d4r55"], .WNxzHc button, a.al6Kxe').first
                    author = author_el.inner_text(timeout=2000).strip()
                except Exception:
                    author = "Anonymous"

                # Star rating
                try:
                    stars_el = card.locator('span[aria-label*="star"]').first
                    stars_label = stars_el.get_attribute("aria-label", timeout=2000) or ""
                    stars = int(''.join(filter(str.isdigit, stars_label.split("star")[0][-2:])) or "0")
                except Exception:
                    stars = 0

                # Review text
                try:
                    # Expand "More" if present
                    more_btn = card.locator('button[aria-label*="See more"], button.w8nwRe').first
                    if more_btn.is_visible(timeout=1000):
                        more_btn.click()
                        page.wait_for_timeout(500)
                    text_el = card.locator('span[class*="wiI7pd"], .MyEned span').first
                    text = text_el.inner_text(timeout=2000).strip()
                except Exception:
                    text = ""

                review_id = make_review_id(branch_id, author, rel_time, stars)
                reviews.append({
                    "review_id":   review_id,
                    "branch_id":   branch_id,
                    "branch_name": branch_name,
                    "place_id":    place_id,
                    "agm":         agm,
                    "author":      author,
                    "stars":       stars,
                    "relative_time": rel_time,
                    "text":        text,
                    "date":        review_date,
                    "scraped_at":  datetime.datetime.now().isoformat(),
                    "method":      "playwright",
                })

            except Exception as e:
                log(f"    [playwright] card parse error: {e}")
                continue

    except Exception as e:
        log(f"  [playwright] ERROR on {branch_name}: {e}")

    log(f"  [playwright] {branch_name}: {len(reviews)} recent reviews")
    return reviews


# ─── Main ──────────────────────────────────────────────────────────────────────
def run(ist_hour: int | None = None) -> list[dict]:
    """
    Run the Playwright scraper for all branches.
    Returns list of new review dicts that were actually added.
    """
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        log("[playwright] playwright not installed. Skipping.")
        return []

    if ist_hour is None:
        ist_hour = (datetime.datetime.utcnow() + datetime.timedelta(hours=5, minutes=30)).hour

    review_date = get_review_date(ist_hour)
    log(f"[playwright] Starting. IST hour={ist_hour}, review_date={review_date}")

    brave_path = find_brave()
    all_new_reviews = []

    # Decide whether to do a deletion check this run
    do_deletion_check = should_check_deletions()
    all_scraped_ids: set[str] = set()

    with sync_playwright() as pw:
        launch_kwargs = {
            "headless": True,
            "args": [
                "--no-sandbox",
                "--disable-blink-features=AutomationControlled",
                "--disable-dev-shm-usage",
            ],
        }
        if brave_path:
            launch_kwargs["executable_path"] = brave_path
            log(f"[playwright] Using Brave at {brave_path}")
        else:
            log("[playwright] Brave not found, using bundled Chromium")

        browser = pw.chromium.launch(**launch_kwargs)
        context = browser.new_context(
            viewport={"width": 1280, "height": 900},
            user_agent=(
                "Mozilla/5.0 (X11; Linux x86_64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            locale="en-US",
        )
        # Open 3 tabs
        pages = [context.new_page() for _ in range(3)]

        # Process branches in batches of 3 (one per tab)
        for i in range(0, len(BRANCHES), 3):
            batch = BRANCHES[i:i+3]
            batch_reviews = []
            for tab_idx, (bid, name, pid) in enumerate(batch):
                page = pages[tab_idx]
                revs = scrape_branch_playwright(page, bid, name, pid, review_date)
                batch_reviews.extend(revs)
                for r in revs:
                    all_scraped_ids.add(r["review_id"])
                time.sleep(1)
            all_new_reviews.extend(batch_reviews)

        browser.close()

    # Merge into rev.json
    existing = load_reviews()
    existing, added = add_reviews(existing, all_new_reviews)
    save_reviews(existing)
    log(f"[playwright] Done. {added} new reviews added to rev.json")

    # Deletion check
    if do_deletion_check and all_scraped_ids:
        log("[playwright] Running deletion check…")
        deleted = find_deleted_reviews(list(all_scraped_ids), existing)
        n = save_newly_deleted(deleted)
        record_deletion_check()
        log(f"[playwright] Deletion check: {n} newly deleted reviews saved")

    return all_new_reviews


if __name__ == "__main__":
    results = run()
    print(f"Total reviews scraped: {len(results)}")
