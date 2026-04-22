"""
scraper_selenium.py — Method 2: Selenium-based Google Maps review scraper.
Fallback when Playwright fails. Opens Brave (or Chrome/Chromium) with 3 tabs.
"""

import sys
import time
import datetime
from pathlib import Path

from branches import BRANCHES, AGM_MAP
from utils import (
    log, get_review_date, parse_relative_time, make_review_id,
    load_reviews, save_reviews, add_reviews, maps_url,
    should_check_deletions, record_deletion_check,
    find_deleted_reviews, save_newly_deleted
)

BRAVE_PATHS = [
    "/usr/bin/brave-browser",
    "/usr/bin/brave",
    "/opt/brave.com/brave/brave",
    "/Applications/Brave Browser.app/Contents/MacOS/Brave Browser",
    r"C:\Program Files\BraveSoftware\Brave-Browser\Application\brave.exe",
    r"C:\Program Files (x86)\BraveSoftware\Brave-Browser\Application\brave.exe",
]

CHROMEDRIVER_PATHS = [
    "/usr/bin/chromedriver",
    "/usr/local/bin/chromedriver",
    "chromedriver",  # assumes in PATH
]


def find_brave() -> str | None:
    for p in BRAVE_PATHS:
        if Path(p).exists():
            return p
    return None


def build_driver():
    """Build a Selenium WebDriver using Brave or Chrome/Chromium."""
    try:
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.chrome.service import Service
    except ImportError:
        log("[selenium] selenium not installed.")
        return None

    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--window-size=1280,900")
    options.add_argument("--lang=en-US")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)
    options.add_argument(
        "user-agent=Mozilla/5.0 (X11; Linux x86_64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )

    brave_path = find_brave()
    if brave_path:
        options.binary_location = brave_path
        log(f"[selenium] Using Brave at {brave_path}")
    else:
        log("[selenium] Brave not found, using default Chrome/Chromium")

    # Try webdriver-manager first, then fall back to system chromedriver
    try:
        from webdriver_manager.chrome import ChromeDriverManager
        service = Service(ChromeDriverManager().install())
    except Exception:
        service = None
        for p in CHROMEDRIVER_PATHS:
            if Path(p).exists():
                service = Service(p)
                break

    try:
        if service:
            driver = webdriver.Chrome(service=service, options=options)
        else:
            driver = webdriver.Chrome(options=options)
        return driver
    except Exception as e:
        log(f"[selenium] Failed to launch driver: {e}")
        return None


def scrape_branch_selenium(driver, branch_id: int, branch_name: str,
                            place_id: str, review_date: str) -> list[dict]:
    """Open a new tab, scrape reviews, return list."""
    from selenium.webdriver.common.by import By
    from selenium.webdriver.common.keys import Keys
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC

    reviews = []
    agm = AGM_MAP.get(branch_name, "Unknown")
    url = maps_url(place_id)

    try:
        log(f"  [selenium] → {branch_name}")
        driver.get(url)
        time.sleep(3)

        wait = WebDriverWait(driver, 10)

        # Click Reviews tab
        try:
            tabs = driver.find_elements(By.CSS_SELECTOR, 'button[role="tab"]')
            for tab in tabs:
                if "review" in tab.get_attribute("aria-label", "").lower():
                    tab.click()
                    time.sleep(2)
                    break
        except Exception:
            pass

        # Sort by Newest
        try:
            sort_btn = driver.find_element(By.CSS_SELECTOR, 'button[aria-label*="Sort"]')
            sort_btn.click()
            time.sleep(1)
            newest = driver.find_element(By.CSS_SELECTOR, 'li[aria-label*="Newest"]')
            newest.click()
            time.sleep(2)
        except Exception:
            pass

        # Scroll reviews panel
        scrollable = None
        try:
            scrollable = driver.find_element(By.CSS_SELECTOR, 'div[aria-label*="Reviews"]')
        except Exception:
            scrollable = driver.find_element(By.TAG_NAME, "body")

        for _ in range(5):
            driver.execute_script("arguments[0].scrollTop += 1000", scrollable)
            time.sleep(1.2)

        # Parse cards
        cards = driver.find_elements(By.CSS_SELECTOR, 'div[data-review-id]')
        if not cards:
            cards = driver.find_elements(By.CSS_SELECTOR, 'div[jscontroller][class*="review"]')

        for card in cards:
            try:
                # Relative time
                rel_el = card.find_element(By.CSS_SELECTOR,
                    'span[class*="dehysf"], .rsqaWe, span[aria-label*="ago"]')
                rel_time = rel_el.text.strip()
                if not parse_relative_time(rel_time):
                    continue

                # Author
                try:
                    author = card.find_element(
                        By.CSS_SELECTOR, 'div[class*="d4r55"], .WNxzHc button').text.strip()
                except Exception:
                    author = "Anonymous"

                # Stars
                try:
                    star_el = card.find_element(By.CSS_SELECTOR, 'span[aria-label*="star"]')
                    label = star_el.get_attribute("aria-label") or ""
                    stars = int(''.join(filter(str.isdigit, label.split("star")[0][-2:])) or "0")
                except Exception:
                    stars = 0

                # Text
                try:
                    try:
                        more = card.find_element(By.CSS_SELECTOR, 'button[aria-label*="See more"]')
                        driver.execute_script("arguments[0].click()", more)
                        time.sleep(0.5)
                    except Exception:
                        pass
                    text = card.find_element(
                        By.CSS_SELECTOR, 'span[class*="wiI7pd"], .MyEned span').text.strip()
                except Exception:
                    text = ""

                review_id = make_review_id(branch_id, author, text, stars, review_date)
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
                    "method":      "selenium",
                })
            except Exception as e:
                log(f"    [selenium] card parse error: {e}")

    except Exception as e:
        log(f"  [selenium] ERROR on {branch_name}: {e}")

    log(f"  [selenium] {branch_name}: {len(reviews)} recent reviews")
    return reviews


def run(ist_hour: int | None = None) -> list[dict]:
    try:
        from selenium import webdriver
    except ImportError:
        log("[selenium] selenium not installed. Skipping.")
        return []

    if ist_hour is None:
        ist_hour = (datetime.datetime.utcnow() + datetime.timedelta(hours=5, minutes=30)).hour

    review_date = get_review_date(ist_hour)
    log(f"[selenium] Starting. IST hour={ist_hour}, review_date={review_date}")

    all_new_reviews = []
    do_deletion_check = should_check_deletions()
    all_scraped_ids: set[str] = set()

    # Open 3 separate driver instances to simulate 3 tabs
    # (Selenium windows are safer than tabs for parallel use)
    drivers = []
    for _ in range(3):
        d = build_driver()
        if d:
            drivers.append(d)

    if not drivers:
        log("[selenium] Could not create any driver. Aborting.")
        return []

    try:
        for i in range(0, len(BRANCHES), len(drivers)):
            batch = BRANCHES[i:i + len(drivers)]
            for idx, (bid, name, pid) in enumerate(batch):
                drv = drivers[idx % len(drivers)]
                revs = scrape_branch_selenium(drv, bid, name, pid, review_date)
                all_new_reviews.extend(revs)
                for r in revs:
                    all_scraped_ids.add(r["review_id"])
                time.sleep(1)
    finally:
        for d in drivers:
            try:
                d.quit()
            except Exception:
                pass

    existing = load_reviews()
    existing, added = add_reviews(existing, all_new_reviews)
    save_reviews(existing)
    log(f"[selenium] Done. {added} new reviews added.")

    if do_deletion_check and all_scraped_ids:
        log("[selenium] Running deletion check…")
        deleted = find_deleted_reviews(list(all_scraped_ids), existing)
        n = save_newly_deleted(deleted)
        record_deletion_check()
        log(f"[selenium] Deletion check: {n} newly deleted reviews saved")

    return all_new_reviews


if __name__ == "__main__":
    results = run()
    print(f"Total reviews scraped: {len(results)}")
