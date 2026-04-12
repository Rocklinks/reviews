"""
Sathya Mobiles — Individual Review Scraper
Scrapes every review text + rating + author from all branches.
Pushes rev.json + deleted.json to Hugging Face dataset.
Run via GitHub Actions at 07:10 UTC (12:40 AM IST).
"""

import re
import json
import os
import asyncio
import traceback
import sys
from datetime import datetime, timedelta
from pathlib import Path
from playwright.async_api import async_playwright
from huggingface_hub import HfApi

# ====================== CONFIG ======================
HF_REPO_ID     = "RocklinKS/sathya-reviews"   # your HF dataset repo
MAX_CONCURRENT = 5                              # parallel branches
MAX_REVIEWS    = 200                            # per branch cap
# ====================================================

BRANCHES = [
    {"id": 1,  "name": "Tuticorin1",      "place_id": "ChIJuwNfBb7vAzsR1Gk8166QIVE", "agm": "Tamilselvan J"},
    {"id": 2,  "name": "Tuticorin2",      "place_id": "ChIJUfzbg4L7AzsR4ikUKtp_sx4", "agm": "Tamilselvan J"},
    {"id": 3,  "name": "Thisayanvilai1",  "place_id": "ChIJJfTo4pN_BDsR7pbTj8_dhEU", "agm": "Tamilselvan J"},
    {"id": 4,  "name": "Eral1",           "place_id": "ChIJkyXwiO6NAzsR6Wmmcpg5axg", "agm": "Tamilselvan J"},
    {"id": 5,  "name": "Sattur2",         "place_id": "ChIJFbxGS_XLBjsRPyxhjRSDW1A", "agm": "Tamilselvan J"},
    {"id": 6,  "name": "Villathikullam1", "place_id": "ChIJueDIMftbATsR5FHkWT0DMtY", "agm": "Tamilselvan J"},
    {"id": 7,  "name": "Tenkasi1",        "place_id": "ChIJX-SiDHopBDsR9WQZBK9_y-Q", "agm": "Ashok Kumar"},
    {"id": 8,  "name": "Surandai1",       "place_id": "ChIJhXjnmVqdBjsRYdhg7Z2Use0", "agm": "Ashok Kumar"},
    {"id": 9,  "name": "Ambasamudram1",   "place_id": "ChIJLReO2yI5BDsRJUI3MdjudKU", "agm": "Ashok Kumar"},
    {"id": 10, "name": "Rajapalayam1",    "place_id": "ChIJM6i7syvoBjsROzyHWZO4iDw", "agm": "Ashok Kumar"},
    {"id": 11, "name": "Virudunagar1",    "place_id": "ChIJpVZPddUtATsRNNu8qXIS6eQ", "agm": "Ashok Kumar"},
    {"id": 12, "name": "Puliyangudi1",    "place_id": "ChIJPWqGUIKRBjsR3pR0lzk8zk4", "agm": "Ashok Kumar"},
    {"id": 13, "name": "Sankarankovil1",  "place_id": "ChIJ9wmKdpGXBjsRhtEpPmbpYys", "agm": "Ashok Kumar"},
    {"id": 14, "name": "Sivakasi1",       "place_id": "ChIJwdC-rYvPBjsRx0PfQwzW3hw", "agm": "Ashok Kumar"},
    {"id": 15, "name": "Sivakasi2",       "place_id": "ChIJZ2o0g9nPBjsRgCcmzN1Colk", "agm": "Ashok Kumar"},
    {"id": 16, "name": "Tirunelveli1",    "place_id": "ChIJhbSc2X_3AzsR9HvY0PLuBlo", "agm": "Senthil"},
    {"id": 17, "name": "Tirunelveli2",    "place_id": "ChIJkdCXuEsRBDsR9A-LXevyGx0", "agm": "Senthil"},
    {"id": 18, "name": "Valliyur1",       "place_id": "ChIJqa9AFoNnBDsR8pKyv1BnCK4", "agm": "Senthil"},
    {"id": 19, "name": "Nagercoil1",      "place_id": "ChIJqZLlE__xBDsRADMABwteyfA", "agm": "Senthil"},
    {"id": 20, "name": "Nagercoil2",      "place_id": "ChIJOwGck17xBDsRQOFyQQvObdg", "agm": "Senthil"},
    {"id": 21, "name": "Marthandam",      "place_id": "ChIJqQL4BARVBDsRCIedlksC1fg", "agm": "Senthil"},
]


# ─────────────────────────────────────────────
# HF HELPERS
# ─────────────────────────────────────────────

def hf_download_json(api: HfApi, filename: str) -> list | dict | None:
    """Download a JSON file from HF dataset. Returns None if not found."""
    import tempfile
    try:
        local = api.hf_hub_download(
            repo_id=HF_REPO_ID,
            filename=filename,
            repo_type="dataset",
            local_dir=tempfile.mkdtemp(),
        )
        with open(local, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        if "404" in str(e) or "not found" in str(e).lower() or "Entry" in str(e):
            print(f" [HF] {filename} not found — starting fresh")
            return None
        print(f" [HF] Download error for {filename}: {e}")
        return None


def hf_upload_json(api: HfApi, data, filename: str, commit_msg: str):
    """Upload a JSON file to HF dataset."""
    import tempfile
    tmp = tempfile.NamedTemporaryFile(
        mode="w", suffix=".json", delete=False, encoding="utf-8"
    )
    json.dump(data, tmp, ensure_ascii=False, indent=2)
    tmp.close()
    api.upload_file(
        path_or_fileobj=tmp.name,
        path_in_repo=filename,
        repo_id=HF_REPO_ID,
        repo_type="dataset",
        commit_message=commit_msg,
    )
    os.unlink(tmp.name)
    print(f" [HF] ✅ {filename} uploaded ({commit_msg})")


# ─────────────────────────────────────────────
# SCRAPE INDIVIDUAL REVIEWS
# ─────────────────────────────────────────────

async def scrape_branch_reviews(context, branch: dict, snap_date: str) -> list[dict]:
    """
    Open the Google Maps page for a branch, click Reviews tab,
    scroll to load all reviews, expand "More" buttons, extract every card.
    Returns a list of review dicts.
    """
    reviews = []
    page    = None
    name    = branch["name"]
    bid     = branch["id"]

    try:
        page = await context.new_page()
        url  = f"https://www.google.com/maps/place/?q=place_id:{branch['place_id']}"
        await page.goto(url, wait_until="domcontentloaded", timeout=40000)
        await page.wait_for_timeout(3500)

        # ── Click the Reviews tab ──────────────────────────────────────────
        tab_clicked = False
        for sel in [
            'button[aria-label*="Reviews"]',
            'button[aria-label*="reviews"]',
            '[data-tab-index="1"]',
            'button:has-text("Reviews")',
        ]:
            try:
                el = page.locator(sel).first
                if await el.is_visible(timeout=2000):
                    await el.click()
                    await page.wait_for_timeout(2000)
                    tab_clicked = True
                    break
            except Exception:
                continue

        if not tab_clicked:
            print(f"   [{name}] Reviews tab not found — trying anyway")

        # ── Sort by Newest (optional but keeps data fresh) ─────────────────
        try:
            sort_btn = page.locator('button[aria-label*="Sort"], button[data-value*="sort"]').first
            if await sort_btn.is_visible(timeout=2000):
                await sort_btn.click()
                await page.wait_for_timeout(800)
                newest = page.locator('li[role="menuitemradio"]:has-text("Newest")').first
                if await newest.is_visible(timeout=1500):
                    await newest.click()
                    await page.wait_for_timeout(2000)
        except Exception:
            pass

        # ── Scroll to load reviews ─────────────────────────────────────────
        scrollable = None
        for scroll_sel in [
            'div[role="main"]',
            'div[aria-label*="Results"]',
            'div.m6QErb',
        ]:
            try:
                el = page.locator(scroll_sel).first
                if await el.is_visible(timeout=1500):
                    scrollable = el
                    break
            except Exception:
                continue

        if scrollable:
            prev_count = 0
            for _ in range(30):                         # up to 30 scroll passes
                await scrollable.evaluate("el => el.scrollBy(0, 1500)")
                await page.wait_for_timeout(600)
                cards = await page.locator('div[data-review-id]').count()
                if cards >= MAX_REVIEWS:
                    break
                if cards == prev_count:
                    # Try one more pass with a long wait before giving up
                    await page.wait_for_timeout(1200)
                    cards2 = await page.locator('div[data-review-id]').count()
                    if cards2 == prev_count:
                        break
                prev_count = cards

        # ── Expand truncated reviews ───────────────────────────────────────
        for btn_sel in [
            'button[aria-label="See more"]',
            'button.w8nwRe',
            'button[jsaction*="pane.review.expandReview"]',
        ]:
            btns = await page.locator(btn_sel).all()
            for btn in btns[:60]:
                try:
                    await btn.click()
                    await page.wait_for_timeout(100)
                except Exception:
                    pass

        # ── Extract review cards ───────────────────────────────────────────
        cards = await page.locator('div[data-review-id]').all()
        print(f"   [{name}] Found {len(cards)} review cards")

        for card in cards[:MAX_REVIEWS]:
            try:
                author = ""
                text   = ""
                rating = 0
                time_str = ""

                # Author name
                for asel in ['[class*="d4r55"]', '[class*="DU9Pgb"]', 'button[aria-label*="Photo of"]']:
                    el = card.locator(asel).first
                    if await el.count() > 0:
                        raw = await el.inner_text()
                        author = raw.strip()
                        if author:
                            break

                # Review text
                for tsel in ['span[class*="wiI7pd"]', 'span[class*="HPa7od"]', '.review-full-text']:
                    el = card.locator(tsel).first
                    if await el.count() > 0:
                        raw = await el.inner_text()
                        text = raw.strip()
                        if text:
                            break

                # Star rating
                for rsel in ['span[role="img"][aria-label*="star"]', 'span[aria-label*="Star"]']:
                    el = card.locator(rsel).first
                    if await el.count() > 0:
                        label = await el.get_attribute("aria-label") or ""
                        m = re.search(r"(\d)", label)
                        if m:
                            rating = int(m.group(1))
                            break

                # Relative time
                for tsel in ['span[class*="rsqaWe"]', 'span[class*="dehysf"]']:
                    el = card.locator(tsel).first
                    if await el.count() > 0:
                        raw = await el.inner_text()
                        time_str = raw.strip()
                        if time_str:
                            break

                # Review ID (used as unique key)
                review_id = await card.get_attribute("data-review-id") or ""

                if author or text:
                    reviews.append({
                        "review_id":   review_id,
                        "branch_id":   bid,
                        "branch_name": name,
                        "agm":         branch["agm"],
                        "author":      author,
                        "rating":      rating,
                        "text":        text,
                        "time":        time_str,
                        "snap_date":   snap_date,
                    })
            except Exception as e:
                continue

    except Exception as e:
        print(f"   [{name}] ERROR: {e}")
    finally:
        if page:
            try:
                await page.close()
            except Exception:
                pass

    return reviews


# ─────────────────────────────────────────────
# DETECT DELETED REVIEWS
# ─────────────────────────────────────────────

def find_deleted(old_reviews: list, new_reviews: list) -> list:
    """
    A review is considered deleted if it existed yesterday
    (by review_id) but is absent today.
    """
    new_ids = {r["review_id"] for r in new_reviews if r.get("review_id")}
    deleted = []
    for r in old_reviews:
        rid = r.get("review_id", "")
        if rid and rid not in new_ids:
            deleted.append({**r, "deleted_on": datetime.utcnow().strftime("%Y-%m-%d")})
    return deleted


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

async def run():
    IST_OFFSET = timedelta(hours=5, minutes=30)
    now_ist    = datetime.utcnow() + IST_OFFSET
    snap_date  = now_ist.date().strftime("%Y-%m-%d")
    run_time   = datetime.utcnow().isoformat()

    hf_token = os.environ.get("HF_TOKEN", "")
    if not hf_token:
        print("[FATAL] HF_TOKEN not set — cannot push to Hugging Face")
        sys.exit(1)

    api = HfApi(token=hf_token)

    print("=" * 60)
    print(" SATHYA MOBILES — Individual Review Scraper")
    print(f" Snap date  : {snap_date} (IST)")
    print(f" Branches   : {len(BRANCHES)} | Concurrency: {MAX_CONCURRENT}")
    print(f" Max reviews: {MAX_REVIEWS} per branch")
    print("=" * 60)

    # ── Load previous rev.json from HF ────────────────────────────────────
    print("\n Loading previous rev.json from HF...")
    old_reviews: list = hf_download_json(api, "rev.json") or []
    old_deleted: list = hf_download_json(api, "deleted.json") or []
    print(f" Previous   : {len(old_reviews)} reviews, {len(old_deleted)} deleted")

    # ── Launch browser ─────────────────────────────────────────────────────
    all_new_reviews = []
    success_branches = 0
    failed_branches  = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-blink-features=AutomationControlled",
                "--disable-extensions",
                "--disable-gpu",
            ],
        )
        context = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/134.0.0.0 Safari/537.36"
            ),
            locale="en-IN",
            viewport={"width": 1280, "height": 800},
        )

        # Warm-up hit
        try:
            wp = await context.new_page()
            await wp.goto("https://www.google.com/maps", wait_until="domcontentloaded", timeout=30000)
            await wp.wait_for_timeout(2000)
            await wp.close()
            print(" [warm-up] Browser ready ✓\n")
        except Exception:
            print(" [warm-up] Skipped\n")

        semaphore = asyncio.Semaphore(MAX_CONCURRENT)

        async def bounded(branch):
            nonlocal success_branches
            async with semaphore:
                name = branch["name"]
                print(f" Scraping [{branch['id']:02d}/{len(BRANCHES)}] {name}...", flush=True)
                reviews = await scrape_branch_reviews(context, branch, snap_date)
                if reviews:
                    all_new_reviews.extend(reviews)
                    success_branches += 1
                    print(f"   [{name}] ✓ {len(reviews)} reviews collected")
                else:
                    failed_branches.append(name)
                    print(f"   [{name}] ✗ 0 reviews — marking failed")
                await asyncio.sleep(0.8)

        await asyncio.gather(*[bounded(b) for b in BRANCHES])
        await browser.close()

    # ── Deleted review detection ───────────────────────────────────────────
    print(f"\n Detecting deleted reviews...")
    # Only compare reviews from branches that succeeded today
    scraped_branch_ids = {r["branch_id"] for r in all_new_reviews}
    old_for_scraped    = [r for r in old_reviews if r["branch_id"] in scraped_branch_ids]
    newly_deleted      = find_deleted(old_for_scraped, all_new_reviews)
    print(f" Newly deleted : {len(newly_deleted)}")

    # Merge with historical deleted list (deduplicate by review_id)
    existing_del_ids = {r.get("review_id") for r in old_deleted if r.get("review_id")}
    for r in newly_deleted:
        if r.get("review_id") not in existing_del_ids:
            old_deleted.append(r)
            existing_del_ids.add(r["review_id"])

    # ── Build final rev.json ───────────────────────────────────────────────
    # Keep old reviews from branches that FAILED today (don't lose their data)
    failed_branch_ids = set()
    scraped_ids_set   = {b["id"] for b in BRANCHES} - {b["id"] for b in BRANCHES if b["name"] in failed_branches}

    kept_old  = [r for r in old_reviews if r["branch_id"] not in scraped_branch_ids]
    final_rev = kept_old + all_new_reviews

    # Sort: newest snap_date first, then by branch
    final_rev.sort(key=lambda r: (r.get("snap_date",""), r.get("branch_id",0)), reverse=True)

    # ── Summary ────────────────────────────────────────────────────────────
    print(f"\n{'─' * 60}")
    print(f" ✅ Scraped   : {success_branches}/{len(BRANCHES)} branches")
    if failed_branches:
        print(f" ❌ Failed    : {', '.join(failed_branches)}")
    print(f" Reviews     : {len(all_new_reviews)} new collected")
    print(f" Total in DB : {len(final_rev)}")
    print(f" Deleted     : {len(old_deleted)} total archived")
    print(f"{'─' * 60}")

    # ── Push to HF ────────────────────────────────────────────────────────
    print("\n Pushing to Hugging Face...")
    stamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")

    hf_upload_json(api, final_rev,  "rev.json",     f"Update rev.json {stamp}")
    hf_upload_json(api, old_deleted, "deleted.json", f"Update deleted.json {stamp}")

    print(f"\n✅ Done — {len(all_new_reviews)} reviews pushed for {snap_date}")

    if success_branches < len(BRANCHES) * 0.5:
        print("❌ Over 50% branches failed — marking run as failed.")
        sys.exit(1)


if __name__ == "__main__":
    try:
        asyncio.run(run())
    except Exception as e:
        print(f"\n[FATAL] Scraper crashed: {e}")
        traceback.print_exc()
        sys.exit(1)
