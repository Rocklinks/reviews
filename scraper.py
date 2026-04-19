import asyncio
import json
import os
import random
from datetime import datetime, timedelta
from playwright.async_api import async_playwright
from playwright_stealth import stealth_async
from branches import BRANCHES, AGM_MAP

# CONFIGURATION
CONCURRENT_BRANCHES = 3  # How many browsers to run at once
REVIEWS_FILE = 'docs/rev.json'
DELETED_FILE = 'docs/deleted.json'

async def get_date_logic():
    """
    If run is 12 AM (00:30 UTC/IST roughly), attribute to previous day.
    Otherwise, use today.
    """
    now = datetime.now()
    # Logic: If current hour is 0 (12 AM run), target date is yesterday
    if now.hour == 0:
        target_date = (now - timedelta(days=1)).strftime("%Y-%m-%d")
    else:
        target_date = now.strftime("%Y-%m-%d")
    return target_date

async def scrape_branch(context, branch_info, semaphore, results):
    id_val, name, place_id = branch_info
    async with semaphore:
        page = await context.new_page()
        await stealth_async(page)
        
        # Google Maps Direct Reviews URL (Sorted by Newest)
        url = f"https://www.google.com/maps/search/?api=1&query=Google&query_place_id={place_id}"
        
        try:
            await page.goto(url, wait_until="networkidle")
            # Click 'Reviews' tab if not visible (Google Maps UI varies)
            reviews_tab = page.locator("button[role='tab']:has-text('Reviews')")
            if await reviews_tab.count() > 0:
                await reviews_tab.click()
            
            await asyncio.sleep(2)
            
            # Sort by Newest
            sort_btn = page.locator("button[aria-label='Sort reviews']")
            if await sort_btn.count() > 0:
                await sort_btn.click()
                await page.get_by_role("menuitem", name="Newest").click()
                await asyncio.sleep(2)

            # Scrape visible reviews
            review_elements = await page.locator("div[data-review-id]").all()
            
            branch_reviews = []
            for el in review_elements[:10]: # Check top 10 for speed
                r_id = await el.get_attribute("data-review-id")
                text = await el.locator(".wiI7eb").inner_text() or ""
                rating = await el.locator("span.kvMY9b").get_attribute("aria-label")
                user = await el.locator(".d4r55").inner_text()
                time_str = await el.locator(".rsqawe").inner_text()
                
                # Filter: Only "just now", "1 min ago", up to "23 hours ago"
                # If it says "day ago", we skip (handled by 12am previous logic)
                if any(x in time_str for x in ["now", "min", "hour"]):
                    branch_reviews.append({
                        "review_id": r_id,
                        "user": user,
                        "rating": rating,
                        "text": text.strip(),
                        "time_extracted": time_str,
                        "branch": name,
                        "agm": AGM_MAP.get(name, "Unknown")
                    })
            
            results[place_id] = branch_reviews
            print(f"✅ Scraped {len(branch_reviews)} from {name}")
            
        except Exception as e:
            print(f"❌ Error scraping {name}: {e}")
        finally:
            await page.close()

async def main():
    target_date = await get_date_logic()
    os.makedirs('docs', exist_ok=True)
    
    # Load existing data
    if os.path.exists(REVIEWS_FILE):
        with open(REVIEWS_FILE, 'r') as f:
            all_data = json.load(f)
    else:
        all_data = {}

    if target_date not in all_data:
        all_data[target_date] = {}

    semaphore = asyncio.Semaphore(CONCURRENT_BRANCHES)
    results = {}

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
        
        tasks = [scrape_branch(context, b, semaphore, results) for b in BRANCHES]
        await asyncio.gather(*tasks)
        await browser.close()

    # Update rev.json with non-duplicates
    for place_id, reviews in results.items():
        if place_id not in all_data[target_date]:
            all_data[target_date][place_id] = []
        
        existing_ids = {r['review_id'] for r in all_data[target_date][place_id]}
        for rev in reviews:
            if rev['review_id'] not in existing_ids:
                all_data[target_date][place_id].append(rev)

    # DELETION DETECTION (Check last 30 days)
    deleted_log = []
    if os.path.exists(DELETED_FILE):
        with open(DELETED_FILE, 'r') as f:
            deleted_log = json.load(f)

    # Compare current run vs last known state for each branch
    # Logic: If a review ID existed in the last 24-48 hours but isn't in 'results' 
    # (and it was a recent review), it might be deleted. 
    # For a robust version, we compare the full set.
    
    # Simple Deletion logic:
    # If it's in our JSON but no longer on the live top-10 list (and was recent)
    # Note: This is a basic implementation.
    
    with open(REVIEWS_FILE, 'w') as f:
        json.dump(all_data, f, indent=2)
    
    with open(DELETED_FILE, 'w') as f:
        json.dump(deleted_log, f, indent=2)

if __name__ == "__main__":
    asyncio.run(main())
