"""
scraper_playwright.py — Method 1: Playwright-based Google Maps review scraper.
Opens Brave (falls back to Chromium), 3 tabs at a time per batch.

Deletion logic: after each branch is scraped, compare freshly-scraped IDs
against what is stored in rev.json for that branch on the same date.
Reviews no longer visible on Google -> saved to deleted.json.
"""

import sys
import time
import datetime
from pathlib import Path

from branches import BRANCHES, AGM_MAP
from utils import (
    log, get_review_date, parse_relative_time, make_review_id,
    load_reviews, save_reviews, add_reviews, maps_url,
    check_deletions_for_branch, move_to_deleted, reactivate_reviews,
)

BRAVE_PATHS = [
    "/usr/bin/brave-browser",
    "/usr/bin/brave",
    "/opt/brave.com/brave/brave",
    "/Applications/Brave Browser.app/Contents/MacOS/Brave Browser",
    r"C:\Program Files\BraveSoftware\Brave-Browser\Application\brave.exe",
    r"C:\Program Files (x86)\BraveSoftware\Brave-Browser\Application\brave.exe",
]


def find_brave() -> str | None:
    for p in BRAVE_PATHS:
        if Path(p).exists():
            return p
    return None


def scrape_branch_playwright(page, branch_id: int, branch_name: str,
                              place_id: str, review_date: str) -> list:
    reviews = []
    agm = AGM_MAP.get(branch_name, "Unknown")
    url = maps_url(place_id)

    try:
        log(f"  [playwright] -> {branch_name} ({place_id})")
        page.goto(url, wait_until="domcontentloaded", timeout=30000)
        page.wait_for_timeout(3000)

        # Click Reviews tab
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

        # Extract review cards - use multiple selector strategies
        selectors = [
            'div[data-review-id]',
            'div[jscontroller][class*="review"]',
            'div[class*="MyEned"]',
            'div[jslog*="review"]',
            'div[class*="review-text"]',
            'div[aria-label*="Review"]',
        ]
        review_cards = []
        for sel in selectors:
            try:
                cards = page.locator(sel).all()
                review_cards.extend(cards)
            except Exception:
                pass
        # Dedupe by element handle
        seen = set()
        unique_cards = []
        for card in review_cards:
            try:
                handle = card.evaluate("el => el.dataset.reviewId || el.dataset.jslog || el.outerHTML.substring(0, 200)")
                if handle not in seen:
                    seen.add(handle)
                    unique_cards.append(card)
            except Exception:
                pass
        # Use unique_cards
        for card in unique_cards:
            try:
                rel_time = ""
                for sel in [
                    'span.XfOne', 'div[class*="DUxS3d"]', '.rsqaWe',
                    'span[aria-label*="ago"]', 'span[aria-label*="now"]'
                ]:
                    try:
                        el = card.locator(sel).first
                        if el.count() > 0:
                            rel_time = el.inner_text(timeout=1500).strip()
                            break
                    except Exception:
                        continue

                if not rel_time or not parse_relative_time(rel_time):
                    continue

                try:
                    author_el = card.locator('div[class*="d4r55"], .WNxzHc button, a.al6Kxe').first
                    author = author_el.inner_text(timeout=2000).strip()
                except Exception:
                    author = "Anonymous"

                try:
                    stars_el = card.locator('span[aria-label*="star"]').first
                    stars_label = stars_el.get_attribute("aria-label", timeout=2000) or ""
                    stars = int(''.join(filter(str.isdigit, stars_label.split("star")[0][-2:])) or "0")
                except Exception:
                    stars = 0

                try:
                    more_btn = card.locator('button[aria-label*="See more"], button.w8nwRe').first
                    if more_btn.is_visible(timeout=1000):
                        more_btn.click()
                        page.wait_for_timeout(500)
                    text_el = card.locator('span[class*="wiI7pd"], .MyEned span').first
                    text = text_el.inner_text(timeout=2000).strip()
                except Exception:
                    text = ""

                review_id = make_review_id(branch_id, author, text, stars)
                reviews.append({
                    "review_id":     review_id,
                    "branch_id":     branch_id,
                    "branch_name":   branch_name,
                    "place_id":      place_id,
                    "agm":           agm,
                    "author":        author,
                    "stars":         stars,
                    "relative_time": rel_time,
                    "text":          text,
                    "date":          review_date,
                    "scraped_at":    datetime.datetime.now().isoformat(),
                    "method":        "playwright",
                })

            except Exception as e:
                log(f"    [playwright] card parse error: {e}")

    except Exception as e:
        log(f"  [playwright] ERROR on {branch_name}: {e}")

    log(f"  [playwright] {branch_name}: {len(reviews)} recent reviews")
    return reviews


def run(ist_hour: int | None = None) -> list:
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        log("[playwright] playwright not installed. Skipping.")
        return []

    if ist_hour is None:
        ist_hour = (datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=5, minutes=30)).hour

    review_date = get_review_date(ist_hour)
    log(f"[playwright] Starting. IST hour={ist_hour}, review_date={review_date}")

    brave_path = find_brave()
    all_new_reviews = []

    # Load existing reviews ONCE before the scrape loop
    existing = load_reviews()
    total_deleted = 0
    total_added = 0

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
        pages = [context.new_page() for _ in range(3)]

        for i in range(0, len(BRANCHES), 3):
            batch = BRANCHES[i:i+3]
            for tab_idx, (bid, name, pid) in enumerate(batch):
                page = pages[tab_idx]
                revs = scrape_branch_playwright(page, bid, name, pid, review_date)

                # ── Per-branch dedup + deletion check ─────────────────────────
                scraped_ids = {r["review_id"] for r in revs}

                # Reactivate reviews that came back to Google
                n_react = reactivate_reviews(scraped_ids, existing)
                if n_react:
                    log(f"  [playwright] {name}: {n_react} reviews reactivated from deleted.json")

                # 1. Add new reviews to existing (dedup by stable ID)
                existing, added = add_reviews(existing, revs)
                total_added += added
                all_new_reviews.extend(revs)

                # 2. Deletion check: stored reviews for this branch+date
                #    that are NOT in this run's scraped IDs = deleted
                deleted = check_deletions_for_branch(bid, scraped_ids, existing, ist_hour)
                if deleted:
                    n = move_to_deleted(deleted, existing)   # moves: out of rev, into deleted
                    total_deleted += n
                    if n:
                        log(f"  [playwright] {name}: {n} reviews moved to deleted.json")
                # ──────────────────────────────────────────────────────────────

                time.sleep(1)

        browser.close()

    # Save final rev.json once after all branches
    save_reviews(existing)
    log(f"[playwright] Done. {len(all_new_reviews)} scraped this run, "
        f"{total_added} new added to rev.json, "
        f"{total_deleted} moved to deleted.json")

    return all_new_reviews


if __name__ == "__main__":
    results = run()
    print(f"Total reviews scraped this run: {len(results)}")
