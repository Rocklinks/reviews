"""
scraper_selenium.py — Method 2: Selenium-based Google Maps review scraper.
Fallback when Playwright fails. 3 driver instances (simulating 3 tabs).
Deletion logic: same per-branch, per-run check as playwright scraper.
"""

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
    "/usr/bin/brave-browser", "/usr/bin/brave",
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


def build_driver():
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

    try:
        from webdriver_manager.chrome import ChromeDriverManager
        service = Service(ChromeDriverManager().install())
    except Exception:
        service = None
        for p in ["/usr/bin/chromedriver", "/usr/local/bin/chromedriver"]:
            if Path(p).exists():
                service = Service(p)
                break

    try:
        return webdriver.Chrome(service=service, options=options) if service \
               else webdriver.Chrome(options=options)
    except Exception as e:
        log(f"[selenium] Failed to launch driver: {e}")
        return None


def scrape_branch_selenium(driver, branch_id: int, branch_name: str,
                            place_id: str, review_date: str) -> list:
    from selenium.webdriver.common.by import By
    reviews = []
    agm = AGM_MAP.get(branch_name, "Unknown")

    try:
        log(f"  [selenium] -> {branch_name}")
        driver.get(maps_url(place_id))
        time.sleep(3)

        # Click Reviews tab
        try:
            for tab in driver.find_elements(By.CSS_SELECTOR, 'button[role="tab"]'):
                if "review" in (tab.get_attribute("aria-label") or "").lower():
                    tab.click(); time.sleep(2); break
        except Exception:
            pass

        # Sort by Newest
        try:
            driver.find_element(By.CSS_SELECTOR, 'button[aria-label*="Sort"]').click()
            time.sleep(1)
            driver.find_element(By.CSS_SELECTOR, 'li[aria-label*="Newest"]').click()
            time.sleep(2)
        except Exception:
            pass

        # Scroll
        try:
            scrollable = driver.find_element(By.CSS_SELECTOR, 'div[aria-label*="Reviews"]')
        except Exception:
            scrollable = driver.find_element(By.TAG_NAME, "body")
        for _ in range(5):
            driver.execute_script("arguments[0].scrollTop += 1000", scrollable)
            time.sleep(1.2)

        cards = driver.find_elements(By.CSS_SELECTOR, 'div[data-review-id]')
        if not cards:
            cards = driver.find_elements(By.CSS_SELECTOR, 'div[jscontroller][class*="review"]')

        for card in cards:
            try:
                rel_el = card.find_element(By.CSS_SELECTOR,
                    'span[class*="dehysf"], .rsqaWe, span[aria-label*="ago"]')
                rel_time = rel_el.text.strip()
                if not parse_relative_time(rel_time):
                    continue

                try:
                    author = card.find_element(
                        By.CSS_SELECTOR, 'div[class*="d4r55"], .WNxzHc button').text.strip()
                except Exception:
                    author = "Anonymous"

                try:
                    label = card.find_element(
                        By.CSS_SELECTOR, 'span[aria-label*="star"]').get_attribute("aria-label") or ""
                    stars = int(''.join(filter(str.isdigit, label.split("star")[0][-2:])) or "0")
                except Exception:
                    stars = 0

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

                review_id = make_review_id(branch_id, author, text, stars)
                reviews.append({
                    "review_id": review_id, "branch_id": branch_id,
                    "branch_name": branch_name, "place_id": place_id,
                    "agm": agm, "author": author, "stars": stars,
                    "relative_time": rel_time, "text": text,
                    "date": review_date,
                    "scraped_at": datetime.datetime.now().isoformat(),
                    "method": "selenium",
                })
            except Exception as e:
                log(f"    [selenium] card parse error: {e}")

    except Exception as e:
        log(f"  [selenium] ERROR on {branch_name}: {e}")

    log(f"  [selenium] {branch_name}: {len(reviews)} recent reviews")
    return reviews


def run(ist_hour: int | None = None) -> list:
    try:
        from selenium import webdriver  # noqa: F401
    except ImportError:
        log("[selenium] selenium not installed. Skipping.")
        return []

    if ist_hour is None:
        ist_hour = (datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=5, minutes=30)).hour

    review_date = get_review_date(ist_hour)
    log(f"[selenium] Starting. IST hour={ist_hour}, review_date={review_date}")

    all_new_reviews = []
    total_added = 0
    total_deleted = 0
    existing = load_reviews()

    drivers = [d for d in [build_driver() for _ in range(3)] if d]
    if not drivers:
        log("[selenium] Could not create any driver. Aborting.")
        return []

    try:
        for i in range(0, len(BRANCHES), len(drivers)):
            batch = BRANCHES[i:i + len(drivers)]
            for idx, (bid, name, pid) in enumerate(batch):
                drv = drivers[idx % len(drivers)]
                revs = scrape_branch_selenium(drv, bid, name, pid, review_date)

                scraped_ids = {r["review_id"] for r in revs}

                # Reactivate any reviews that came back to Google
                n_react = reactivate_reviews(scraped_ids, existing)
                if n_react:
                    log(f"  [selenium] {name}: {n_react} reviews reactivated from deleted.json")

                existing, added = add_reviews(existing, revs)
                total_added += added
                all_new_reviews.extend(revs)

                deleted = check_deletions_for_branch(bid, scraped_ids, existing)
                if deleted:
                    n = move_to_deleted(deleted, existing)
                    total_deleted += n
                    if n:
                        log(f"  [selenium] {name}: {n} reviews moved to deleted.json")
                time.sleep(1)
    finally:
        for d in drivers:
            try: d.quit()
            except Exception: pass

    save_reviews(existing)
    log(f"[selenium] Done. {len(all_new_reviews)} scraped this run, "
        f"{total_added} new added to rev.json, "
        f"{total_deleted} moved to deleted.json")
    return all_new_reviews


if __name__ == "__main__":
    results = run()
    print(f"Total reviews scraped this run: {len(results)}")
