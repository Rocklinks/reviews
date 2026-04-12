"""
Sathya Mobiles — Full Individual Review Scraper
Scrapes EVERY review (no cap) for all 36 branches using Playwright scroll loop.
Pushes rev.json + deleted.json to Hugging Face dataset.
GitHub Actions: 07:10 UTC = 12:40 AM IST daily.
"""

import re
import json
import os
import asyncio
import traceback
import sys
import tempfile
from datetime import datetime, timedelta
from playwright.async_api import async_playwright
from huggingface_hub import HfApi

# ====================== CONFIG ======================
HF_REPO_ID     = "RocklinKS/reviews"  # change to your actual HF dataset repo
MAX_CONCURRENT = 3      # 3 branches in parallel — each already takes heavy resources
STALL_PASSES   = 5      # scrolls with zero new cards before we call it done
STALL_WAIT_MS  = 2000   # extra wait per stall pass (ms)
SCROLL_STEP    = 2500   # pixels per scroll tick
# ====================================================

BRANCHES = [
    {"id":1,  "name":"Tuticorin-1",      "place_id":"ChIJ5zJNoJfvAzsR-bJE_3bbNYw", "agm":"Siva"},
    {"id":2,  "name":"Tuticorin-2",      "place_id":"ChIJH6gY4-PvAzsRJ50skTlx3cs", "agm":"Siva"},
    {"id":3,  "name":"Thiruchendur-1",   "place_id":"ChIJeXA4vJKRAzsRBovAtv6lMuQ", "agm":"Siva"},
    {"id":4,  "name":"Thisayanvilai-1",  "place_id":"ChIJVWkvdfh_BDsRdvtimKCLS5Y", "agm":"Siva"},
    {"id":5,  "name":"Eral-2",           "place_id":"ChIJbwAA0KGMAzsRkQilW5PceeA", "agm":"Siva"},
    {"id":6,  "name":"Udankudi",         "place_id":"ChIJPQAAACyEAzsRgjznQ1GLom0", "agm":"Siva"},
    {"id":7,  "name":"Tirunelveli-1",    "place_id":"ChIJ2RU2NvQRBDsRq-Fw7IVwx7k", "agm":"John"},
    {"id":8,  "name":"Valliyur-1",       "place_id":"ChIJcVNk6TtnBDsRBoP4zpExt5k", "agm":"John"},
    {"id":9,  "name":"Ambasamudram-1",   "place_id":"ChIJ9SGeIi85BDsRZk4QdyW9BSY", "agm":"John"},
    {"id":10, "name":"Anjugramam-1",     "place_id":"ChIJ4yeJebLtBDsRDceoxujdGyc", "agm":"John"},
    {"id":11, "name":"Nagercoil",        "place_id":"ChIJe1LZBiTxBDsRJFLjlbgZoIs", "agm":"Jeeva"},
    {"id":12, "name":"Marthandam",       "place_id":"ChIJcWptCRdVBDsRlJh2q0-rnfY", "agm":"Jeeva"},
    {"id":13, "name":"Thuckalay-1",      "place_id":"ChIJc9QgEub4BDsRoyDR4Wd6tYA", "agm":"Jeeva"},
    {"id":14, "name":"Colachel-1",       "place_id":"ChIJgRkBLw39BDsR58D0lwNo5Ts", "agm":"Jeeva"},
    {"id":15, "name":"Kulasekharam-1",   "place_id":"ChIJw0Ep-kNXBDsRe5ad32jAeAk", "agm":"Jeeva"},
    {"id":16, "name":"Monday Market",    "place_id":"ChIJTceRGAD5BDsR65i3YNTcYHk", "agm":"Jeeva"},
    {"id":17, "name":"Karungal-1",       "place_id":"ChIJfTP5ASr_BDsRgsBaeQltkw4", "agm":"Jeeva"},
    {"id":18, "name":"Kovilpatti",       "place_id":"ChIJHY0o-26yBjsRt7wbXB1pDUE", "agm":"Seenivasan"},
    {"id":19, "name":"Ramnad",           "place_id":"ChIJNVVVVaGiATsRnunSgOTvbE8", "agm":"Seenivasan"},
    {"id":20, "name":"Paramakudi",       "place_id":"ChIJ-dgjBzQHATsRf27FWAJgmsA", "agm":"Seenivasan"},
    {"id":21, "name":"Sayalkudi-1",      "place_id":"ChIJRTqudn9lATsR2fYyMmxlOrw", "agm":"Seenivasan"},
    {"id":22, "name":"Villathikullam",   "place_id":"ChIJi_wAkwVbATsRtFl3_V5rGrY", "agm":"Seenivasan"},
    {"id":23, "name":"Sattur-2",         "place_id":"ChIJNVVVVcHKBjsR7xMX97RFn8Q", "agm":"Seenivasan"},
    {"id":24, "name":"Sankarankovil-1",  "place_id":"ChIJE1mKnhSXBjsRKMQ-9JKQf_c", "agm":"Seenivasan"},
    {"id":25, "name":"Kayathar-1",       "place_id":"ChIJx5ebtUgRBDsRMquPZNUJVpw", "agm":"Seenivasan"},
    {"id":26, "name":"Thenkasi",         "place_id":"ChIJuaqqquEpBDsRVITw0MMYklc", "agm":"Muthuselvam"},
    {"id":27, "name":"Thenkasi-2",       "place_id":"ChIJiwqLye6DBjsRo9v1mWXaycI", "agm":"Muthuselvam"},
    {"id":28, "name":"Surandai-1",       "place_id":"ChIJPb1_eEOdBjsRjL9IVCVJhi8", "agm":"Muthuselvam"},
    {"id":29, "name":"Puliyankudi-1",    "place_id":"ChIJjZqoc46RBjsRQTGHnNC8xxA", "agm":"Muthuselvam"},
    {"id":30, "name":"Sengottai-1",      "place_id":"ChIJw3zzKiaBBjsR9KDyGpn1nXU", "agm":"Muthuselvam"},
    {"id":31, "name":"Rajapalayam",      "place_id":"ChIJW2ot-NDpBjsRMTfMF2IV-xE", "agm":"Muthuselvam"},
    {"id":32, "name":"Virudhunagar",     "place_id":"ChIJN3jzNJgsATsRCU3nrB5ntKE", "agm":"Venkatesh"},
    {"id":33, "name":"Virudhunagar-2",   "place_id":"ChIJPezaX7wtATsR9sHhFOG6A1c", "agm":"Venkatesh"},
    {"id":34, "name":"Aruppukottai",     "place_id":"ChIJy6qqqgYwATsRbcp-hXnoruM", "agm":"Venkatesh"},
    {"id":35, "name":"Aruppukottai-2",   "place_id":"ChIJY04wY58xATsRuoJSichVQQE", "agm":"Venkatesh"},
    {"id":36, "name":"Sivakasi",         "place_id":"ChIJI2JvEePOBjsREh8b-x4WF4U", "agm":"Venkatesh"},
]


# ─────────────────────────────────────────────
# HF HELPERS
# ─────────────────────────────────────────────

def hf_load_json(api: HfApi, filename: str):
    """Download JSON from HF dataset. Returns None if not found."""
    try:
        tmpdir = tempfile.mkdtemp()
        local  = api.hf_hub_download(
            repo_id=HF_REPO_ID,
            filename=filename,
            repo_type="dataset",
            local_dir=tmpdir,
        )
        with open(local, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        msg = str(e)
        if any(x in msg for x in ["404", "not found", "Entry", "Repository"]):
            print(f" [HF] {filename} not found — starting fresh")
        else:
            print(f" [HF] Download error ({filename}): {e}")
        return None


def hf_save_json(api: HfApi, data, filename: str, msg: str):
    """Upload JSON to HF dataset."""
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
        commit_message=msg,
    )
    os.unlink(tmp.name)
    print(f" [HF] ✅ {filename} → {len(data) if isinstance(data, list) else '?'} records  ({msg})")


# ─────────────────────────────────────────────
# CORE: SCRAPE ALL REVIEWS FOR ONE BRANCH
# ─────────────────────────────────────────────

async def scrape_all_reviews(context, branch: dict, snap_date: str) -> list[dict]:
    """
    Open the Google Maps listing, navigate to the Reviews tab,
    scroll until NO new review cards appear for STALL_PASSES consecutive
    passes, expand every truncated review, then extract all cards.

    Returns a flat list of review dicts.
    """
    name = branch["name"]
    bid  = branch["id"]
    page = None

    try:
        page = await context.new_page()
        url  = f"https://www.google.com/maps/place/?q=place_id:{branch['place_id']}"
        await page.goto(url, wait_until="domcontentloaded", timeout=45000)
        await page.wait_for_timeout(4000)

        # ── 1. Click the Reviews tab ───────────────────────────────────────
        clicked = False
        for sel in [
            'button[aria-label*="Reviews"]',
            'button[aria-label*="reviews"]',
            '[data-tab-index="1"]',
            'button:has-text("Reviews")',
            'button[jsaction*="pane.rating.moreReviews"]',
        ]:
            try:
                el = page.locator(sel).first
                if await el.is_visible(timeout=2500):
                    await el.click()
                    await page.wait_for_timeout(2500)
                    clicked = True
                    break
            except Exception:
                continue

        if not clicked:
            print(f"   [{name}] ⚠ Reviews tab not found — proceeding anyway")

        # ── 2. Find the scrollable reviews panel ──────────────────────────
        scrollable = None
        for sel in [
            'div[role="main"]',
            'div.m6QErb[aria-label]',
            'div.m6QErb',
            'div[aria-label*="Reviews"]',
        ]:
            try:
                el = page.locator(sel).first
                if await el.is_visible(timeout=1500):
                    scrollable = el
                    break
            except Exception:
                continue

        # ── 3. Scroll until exhausted ──────────────────────────────────────
        # Strategy: scroll → count cards → if count unchanged N times → done
        prev_count   = 0
        stalls       = 0

        if scrollable:
            while True:
                await scrollable.evaluate(
                    f"el => el.scrollBy(0, {SCROLL_STEP})"
                )
                await page.wait_for_timeout(800)
                current = await page.locator('div[data-review-id]').count()

                if current == prev_count:
                    stalls += 1
                    # Give the network more time before counting this as a stall
                    await page.wait_for_timeout(STALL_WAIT_MS)
                    current = await page.locator('div[data-review-id]').count()
                    if current == prev_count:
                        if stalls >= STALL_PASSES:
                            break        # genuinely no more reviews
                    else:
                        stalls = 0      # cards appeared after longer wait
                else:
                    stalls = 0

                prev_count = current

                # Progress log every 200 cards
                if current > 0 and current % 200 == 0:
                    print(f"   [{name}] ... {current} cards loaded", flush=True)
        else:
            # Fallback: scroll the whole page body
            for _ in range(60):
                await page.evaluate(f"window.scrollBy(0, {SCROLL_STEP})")
                await page.wait_for_timeout(900)

        total_loaded = await page.locator('div[data-review-id]').count()
        print(f"   [{name}] Scrolling done — {total_loaded} cards in DOM")

        # ── 4. Expand ALL "See more" / truncated texts ─────────────────────
        for btn_sel in [
            'button[aria-label="See more"]',
            'button.w8nwRe',
            'button[jsaction*="pane.review.expandReview"]',
        ]:
            btns = await page.locator(btn_sel).all()
            for btn in btns:
                try:
                    await btn.click()
                    await page.wait_for_timeout(60)   # just enough for DOM update
                except Exception:
                    pass

        # ── 5. Extract every card ──────────────────────────────────────────
        cards   = await page.locator('div[data-review-id]').all()
        reviews = []

        for card in cards:
            try:
                author   = ""
                text     = ""
                rating   = 0
                time_str = ""
                review_id = await card.get_attribute("data-review-id") or ""

                # Author
                for sel in ['[class*="d4r55"]', '[class*="DU9Pgb"]', '[class*="TSUbDb"]']:
                    el = card.locator(sel).first
                    if await el.count() > 0:
                        raw = (await el.inner_text()).strip()
                        if raw:
                            author = raw
                            break

                # Review text (already expanded above)
                for sel in ['span[class*="wiI7pd"]', 'span[class*="HPa7od"]', '[class*="review-full-text"]']:
                    el = card.locator(sel).first
                    if await el.count() > 0:
                        raw = (await el.inner_text()).strip()
                        if raw:
                            text = raw
                            break

                # Star rating
                for sel in ['span[role="img"][aria-label*="star"]', 'span[aria-label*="Star"]', '[class*="kvMYJc"]']:
                    el = card.locator(sel).first
                    if await el.count() > 0:
                        label = await el.get_attribute("aria-label") or ""
                        m = re.search(r"(\d)", label)
                        if m:
                            rating = int(m.group(1))
                            break

                # Relative time
                for sel in ['span[class*="rsqaWe"]', 'span[class*="dehysf"]', '[class*="DU9Pgb"]']:
                    el = card.locator(sel).first
                    if await el.count() > 0:
                        raw = (await el.inner_text()).strip()
                        # time strings look like "2 weeks ago", "a month ago" etc.
                        if raw and ("ago" in raw or "week" in raw or "month" in raw or "year" in raw or "day" in raw):
                            time_str = raw
                            break

                # Only store if we got at least an author or text
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

            except Exception:
                continue   # skip one bad card, keep going

        return reviews

    except Exception as e:
        print(f"   [{name}] ❌ Fatal error: {e}")
        traceback.print_exc()
        return []

    finally:
        if page:
            try:
                await page.close()
            except Exception:
                pass


# ─────────────────────────────────────────────
# DELETED REVIEW DETECTION
# ─────────────────────────────────────────────

def find_deleted(prev: list, curr: list) -> list:
    """
    Reviews present in prev but absent in curr (by review_id)
    are considered deleted. Only compare branches that were
    successfully scraped today.
    """
    curr_ids          = {r["review_id"] for r in curr if r.get("review_id")}
    scraped_branch_ids = {r["branch_id"] for r in curr}

    deleted = []
    for r in prev:
        # Skip branches we didn't even attempt today — don't falsely mark them deleted
        if r.get("branch_id") not in scraped_branch_ids:
            continue
        if r.get("review_id") and r["review_id"] not in curr_ids:
            deleted.append({
                **r,
                "deleted_on": datetime.utcnow().strftime("%Y-%m-%d"),
            })
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
        print("[FATAL] HF_TOKEN secret not set")
        sys.exit(1)

    api = HfApi(token=hf_token)

    print("=" * 62)
    print(" SATHYA MOBILES — Full Individual Review Scraper")
    print(f" Snap date  : {snap_date} (IST)")
    print(f" Branches   : {len(BRANCHES)} | Concurrency: {MAX_CONCURRENT}")
    print(f" Scroll     : unlimited (stalls={STALL_PASSES} × {STALL_WAIT_MS}ms)")
    print("=" * 62)

    # ── Load previous data from HF ─────────────────────────────────────
    print("\n[1/4] Loading previous rev.json from HF...")
    prev_reviews: list = hf_load_json(api, "rev.json") or []
    prev_deleted: list = hf_load_json(api, "deleted.json") or []
    print(f"      Previous : {len(prev_reviews):,} reviews | {len(prev_deleted):,} deleted")

    # ── Launch browser ─────────────────────────────────────────────────
    print(f"\n[2/4] Scraping {len(BRANCHES)} branches...")
    all_new   = []
    succeeded = []
    failed    = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-blink-features=AutomationControlled",
                "--disable-gpu",
                "--disable-extensions",
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

        # Warm-up
        try:
            wp = await context.new_page()
            await wp.goto("https://www.google.com/maps", wait_until="domcontentloaded", timeout=30000)
            await wp.wait_for_timeout(2000)
            await wp.close()
            print(" [warm-up] ✓ Browser ready\n")
        except Exception:
            print(" [warm-up] Skipped\n")

        semaphore = asyncio.Semaphore(MAX_CONCURRENT)

        async def bounded(branch):
            async with semaphore:
                name = branch["name"]
                num  = branch["id"]
                print(f" [{num:02d}/{len(BRANCHES)}] {name}", flush=True)
                reviews = await scrape_all_reviews(context, branch, snap_date)
                if reviews:
                    all_new.extend(reviews)
                    succeeded.append(name)
                    print(f"   [{name}] ✅ {len(reviews):,} reviews collected\n", flush=True)
                else:
                    failed.append(name)
                    print(f"   [{name}] ❌ 0 reviews — failed\n", flush=True)
                await asyncio.sleep(1.0)   # polite gap between branches

        await asyncio.gather(*[bounded(b) for b in BRANCHES])
        await browser.close()

    # ── Detect deleted reviews ─────────────────────────────────────────
    print("[3/4] Detecting deleted reviews...")
    newly_deleted = find_deleted(prev_reviews, all_new)
    print(f"      Newly deleted this run : {len(newly_deleted)}")

    # Merge into historical deleted list (deduplicate by review_id)
    existing_del_ids = {r.get("review_id") for r in prev_deleted if r.get("review_id")}
    for r in newly_deleted:
        if r.get("review_id") not in existing_del_ids:
            prev_deleted.append(r)
            existing_del_ids.add(r["review_id"])

    # Keep branches that FAILED today — don't wipe their old data
    succeeded_ids = {r["branch_id"] for r in all_new}
    kept_old      = [r for r in prev_reviews if r["branch_id"] not in succeeded_ids]
    final_reviews = kept_old + all_new
    final_reviews.sort(
        key=lambda r: (r.get("snap_date", ""), r.get("branch_id", 0)),
        reverse=True,
    )

    # ── Summary ────────────────────────────────────────────────────────
    print(f"\n{'─' * 62}")
    print(f" Branches   : {len(succeeded)}/{len(BRANCHES)} succeeded")
    if failed:
        print(f" Failed     : {', '.join(failed)}")
    print(f" Reviews    : {len(all_new):,} collected today")
    print(f" Total DB   : {len(final_reviews):,}")
    print(f" Deleted    : {len(prev_deleted):,} archived total")
    print(f"{'─' * 62}")

    # ── Push to HF ────────────────────────────────────────────────────
    print(f"\n[4/4] Pushing to Hugging Face ({HF_REPO_ID})...")
    stamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    hf_save_json(api, final_reviews, "rev.json",     f"Update rev.json {stamp}")
    hf_save_json(api, prev_deleted,  "deleted.json", f"Update deleted.json {stamp}")

    print(f"\n✅ Done — {len(all_new):,} reviews pushed for {snap_date}")

    if len(succeeded) < len(BRANCHES) * 0.5:
        print("❌ Over 50% of branches failed — marking run as failed.")
        sys.exit(1)


if __name__ == "__main__":
    try:
        asyncio.run(run())
    except Exception as e:
        print(f"\n[FATAL] {e}")
        traceback.print_exc()
        sys.exit(1)
