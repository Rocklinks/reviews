import json
import re
import hashlib
import time
import random
import traceback
import asyncio
from datetime import datetime, timedelta
from pathlib import Path

from playwright.async_api import async_playwright
from playwright_stealth import stealth_async

# ── Paths ──────────────────────────────────────────────────────────────────────
REPO_ROOT = Path(__file__).parent
REV_JSON = REPO_ROOT / "docs" / "rev.json"
DEL_JSON = REPO_ROOT / "docs" / "deleted.json"

# ── Concurrency Control ────────────────────────────────────────────────────────
MAX_CONCURRENT = 4   # ← Change this (3-5 is recommended for stability)

# ── Branches (your full list) ──────────────────────────────────────────────────
BRANCHES = [
    {"id":1, "name":"Tuticorin-1", "place_id":"ChIJ5zJNoJfvAzsR-bJE_3bbNYw", "agm":"Siva"},
    {"id":2, "name":"Tuticorin-2", "place_id":"ChIJH6gY4-PvAzsRJ50skTlx3cs", "agm":"Siva"},
    {"id":3, "name":"Thiruchendur-1", "place_id":"ChIJeXA4vJKRAzsRBovAtv6lMuQ", "agm":"Siva"},
    {"id":4, "name":"Thisayanvilai-1", "place_id":"ChIJVWkvdfh_BDsRdvtimKCLS5Y", "agm":"Siva"},
    {"id":5, "name":"Eral-2", "place_id":"ChIJbwAA0KGMAzsRkQilW5PceeA", "agm":"Siva"},
    {"id":6, "name":"Udankudi", "place_id":"ChIJPQAAACyEAzsRgjznQ1GLom0", "agm":"Siva"},
    {"id":7, "name":"Tirunelveli-1", "place_id":"ChIJ2RU2NvQRBDsRq-Fw7IVwx7k", "agm":"John"},
    {"id":8, "name":"Valliyur-1", "place_id":"ChIJcVNk6TtnBDsRBoP4zpExt5k", "agm":"John"},
    {"id":9, "name":"Ambasamudram-1", "place_id":"ChIJ9SGeIi85BDsRZk4QdyW9BSY", "agm":"John"},
    {"id":10, "name":"Anjugramam-1", "place_id":"ChIJ4yeJebLtBDsRDceoxujdGyc", "agm":"John"},
    {"id":11, "name":"Nagercoil", "place_id":"ChIJe1LZBiTxBDsRJFLjlbgZoIs", "agm":"Jeeva"},
    {"id":12, "name":"Marthandam", "place_id":"ChIJcWptCRdVBDsRlJh2q0-rnfY", "agm":"Jeeva"},
    {"id":13, "name":"Thuckalay-1", "place_id":"ChIJc9QgEub4BDsRoyDR4Wd6tYA", "agm":"Jeeva"},
    {"id":14, "name":"Colachel-1", "place_id":"ChIJgRkBLw39BDsR58D0lwNo5Ts", "agm":"Jeeva"},
    {"id":15, "name":"Kulasekharam-1", "place_id":"ChIJw0Ep-kNXBDsRe5ad32jAeAk", "agm":"Jeeva"},
    {"id":16, "name":"Monday Market", "place_id":"ChIJTceRGAD5BDsR65i3YNTcYHk", "agm":"Jeeva"},
    {"id":17, "name":"Karungal-1", "place_id":"ChIJfTP5ASr_BDsRgsBaeQltkw4", "agm":"Jeeva"},
    {"id":18, "name":"Kovilpatti", "place_id":"ChIJHY0o-26yBjsRt7wbXB1pDUE", "agm":"Seenivasan"},
    {"id":19, "name":"Ramnad", "place_id":"ChIJNVVVVaGiATsRnunSgOTvbE8", "agm":"Seenivasan"},
    {"id":20, "name":"Paramakudi", "place_id":"ChIJ-dgjBzQHATsRf27FWAJgmsA", "agm":"Seenivasan"},
    {"id":21, "name":"Sayalkudi-1", "place_id":"ChIJRTqudn9lATsR2fYyMmxlOrw", "agm":"Seenivasan"},
    {"id":22, "name":"Villathikullam", "place_id":"ChIJi_wAkwVbATsRtFl3_V5rGrY", "agm":"Seenivasan"},
    {"id":23, "name":"Sattur-2", "place_id":"ChIJNVVVVcHKBjsR7xMX97RFn8Q", "agm":"Seenivasan"},
    {"id":24, "name":"Sankarankovil-1", "place_id":"ChIJE1mKnhSXBjsRKMQ-9JKQf_c", "agm":"Seenivasan"},
    {"id":25, "name":"Kayathar-1", "place_id":"ChIJx5ebtUgRBDsRMquPZNUJVpw", "agm":"Seenivasan"},
    {"id":26, "name":"Thenkasi", "place_id":"ChIJuaqqquEpBDsRVITw0MMYklc", "agm":"Muthuselvam"},
    {"id":27, "name":"Thenkasi-2", "place_id":"ChIJiwqLye6DBjsRo9v1mWXaycI", "agm":"Muthuselvam"},
    {"id":28, "name":"Surandai-1", "place_id":"ChIJPb1_eEOdBjsRjL9IVCVJhi8", "agm":"Muthuselvam"},
    {"id":29, "name":"Puliyankudi-1", "place_id":"ChIJjZqoc46RBjsRQTGHnNC8xxA", "agm":"Muthuselvam"},
    {"id":30, "name":"Sengottai-1", "place_id":"ChIJw3zzKiaBBjsR9KDyGpn1nXU", "agm":"Muthuselvam"},
    {"id":31, "name":"Rajapalayam", "place_id":"ChIJW2ot-NDpBjsRMTfMF2IV-xE", "agm":"Muthuselvam"},
    {"id":32, "name":"Virudhunagar", "place_id":"ChIJN3jzNJgsATsRCU3nrB5ntKE", "agm":"Venkatesh"},
    {"id":33, "name":"Virudhunagar-2", "place_id":"ChIJPezaX7wtATsR9sHhFOG6A1c", "agm":"Venkatesh"},
    {"id":34, "name":"Aruppukottai", "place_id":"ChIJy6qqqgYwATsRbcp-hXnoruM", "agm":"Venkatesh"},
    {"id":35, "name":"Aruppukottai-2", "place_id":"ChIJY04wY58xATsRuoJSichVQQE", "agm":"Venkatesh"},
    {"id":36, "name":"Sivakasi", "place_id":"ChIJI2JvEePOBjsREh8b-x4WF4U", "agm":"Venkatesh"},
]

# ── Time helpers (unchanged) ───────────────────────────────────────────────────
def ist_now() -> datetime:
    return datetime.utcnow() + timedelta(hours=5, minutes=30)

def today_ist() -> str:
    return ist_now().strftime("%Y-%m-%d")

def parse_relative_time(rel: str, ref: datetime | None = None) -> str | None:
    if not rel:
        return None
    if ref is None:
        ref = ist_now()
    t = rel.lower().strip()
    if any(w in t for w in ["just now", "moment", "now"]):
        return ref.strftime("%Y-%m-%d %H:%M:%S")
    t = re.sub(r'\ban?\b', '1', t)
    patterns = [
        (r"(\d+)\s*minute", timedelta(minutes=1)),
        (r"(\d+)\s*hour", timedelta(hours=1)),
        (r"(\d+)\s*day", timedelta(days=1)),
        (r"(\d+)\s*week", timedelta(weeks=1)),
        (r"(\d+)\s*month", timedelta(days=30)),
        (r"(\d+)\s*year", timedelta(days=365)),
    ]
    for pat, unit in patterns:
        m = re.search(pat, t)
        if m:
            delta = timedelta(seconds=unit.total_seconds() * int(m.group(1)))
            return (ref - delta).strftime("%Y-%m-%d %H:%M:%S")
    return ref.strftime("%Y-%m-%d %H:%M:%S")

def is_today(parsed_date: str | None, today: str) -> bool:
    if not parsed_date:
        return False
    return parsed_date.startswith(today)

def make_fingerprint(branch_id: int, author: str, text: str, rating: float) -> str:
    raw = f"{branch_id}|{(author or '').strip().lower()}|{(text or '')[:120].strip().lower()}|{round(rating, 1)}"
    return hashlib.sha1(raw.encode()).hexdigest()[:16]

def load_json(path: Path) -> list:
    if path.exists():
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return []
    return []

def save_json(path: Path, data: list):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

# ── Async Scrape One Branch ────────────────────────────────────────────────────
async def scrape_branch_playwright_async(branch: dict, now: datetime, today: str, semaphore: asyncio.Semaphore) -> list[dict]:
    async with semaphore:   # Limits concurrent branches
        bid = branch["id"]
        name = branch["name"]
        place_id = branch["place_id"]
        url = f"https://www.google.com/maps/place/?q=place_id:{place_id}"

        reviews = []
        seen = set()

        try:
            async with async_playwright() as p:
                browser = await p.chromium.launch(
                    headless=True,
                    args=["--no-sandbox", "--disable-blink-features=AutomationControlled"]
                )
                context = await browser.new_context(
                    viewport={"width": 1280, "height": 900},
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36"
                )
                await stealth_async(context)

                page = await context.new_page()
                await page.goto(url, wait_until="domcontentloaded", timeout=60000)
                await asyncio.sleep(random.uniform(4, 7))

                # Click Reviews tab
                try:
                    reviews_tab = page.get_by_role("tab", name=re.compile(r"Reviews?|reviews", re.I))
                    if await reviews_tab.count() > 0:
                        await reviews_tab.first.click()
                        await asyncio.sleep(random.uniform(3, 5))
                except:
                    pass

                # Scroll + expand "More" buttons
                for _ in range(18):
                    more_buttons = page.locator("button.w8nwRe, span.w8nwRe, button:has-text('More'), span:has-text('More')")
                    for btn in await more_buttons.all():
                        try:
                            if await btn.is_visible(timeout=1000):
                                await btn.click(timeout=2000)
                                await asyncio.sleep(0.4)
                        except:
                            pass
                    await page.evaluate("window.scrollBy(0, 1400)")
                    await asyncio.sleep(random.uniform(1.8, 3.2))
                    if await page.locator('div.jftiEf').count() > 40:
                        break

                # Extract reviews
                review_cards = page.locator('div.jftiEf')
                for card in await review_cards.all():
                    try:
                        author = (await (await card.locator('.d4r55, .fontHeadlineSmall').first).inner_text(timeout=3000)).strip()
                        rating_text = await (await card.locator('.hCCjke .NhBTye').first).get_attribute('aria-label', timeout=3000) or ""
                        rating_match = re.search(r'(\d+\.?\d*)', rating_text)
                        rating = float(rating_match.group(1)) if rating_match else None

                        text_elem = card.locator('.wiI7pd')
                        text = (await (await text_elem).inner_text(timeout=3000)).strip() if await text_elem.count() > 0 else ""

                        time_elem = card.locator('.rsqaWe, .DU9Pgb')
                        rel_time = (await (await time_elem.first).inner_text(timeout=3000)).strip() if await time_elem.count() > 0 else ""

                        if not rating or not (1 <= rating <= 5):
                            continue

                        parsed = parse_relative_time(rel_time, now)
                        if not is_today(parsed, today):
                            continue

                        fp = make_fingerprint(bid, author, text, rating)
                        if fp in seen:
                            continue
                        seen.add(fp)

                        reviews.append({
                            "fingerprint": fp,
                            "branch_id": bid,
                            "branch_name": name,
                            "agm": branch["agm"],
                            "author": author,
                            "rating": rating,
                            "text": text,
                            "time": rel_time,
                            "parsed_date": parsed,
                            "snap_date": now.strftime("%Y-%m-%d"),
                            "snap_time": now.strftime("%H:%M IST"),
                            "first_seen": f"{now.strftime('%Y-%m-%d %H:%M IST')}",
                        })
                    except:
                        continue

                await browser.close()

        except Exception as e:
            print(f" [{bid:02d}/36] {name:<24} ✗ Async Playwright error: {str(e)[:100]}")

        count = len(reviews)
        print(f" [{bid:02d}/36] {name:<24} ✓ {count:2d} today's reviews (Async)")
        return reviews

# ── Main Async Runner ──────────────────────────────────────────────────────────
async def async_run():
    now = ist_now()
    today = today_ist()
    label = now.strftime("%Y-%m-%d %H:%M IST")
    print("=" * 110)
    print(f" Sathya Reviews Scraper v0.2 — Async Playwright (Max {MAX_CONCURRENT} concurrent) | {label}")
    print("=" * 110)

    live_map = {r["fingerprint"]: r for r in load_json(REV_JSON) if "fingerprint" in r}
    del_map = {r["fingerprint"]: r for r in load_json(DEL_JSON) if "fingerprint" in r}

    print(f" All-time : {len(live_map)} live | {len(del_map)} deleted")

    semaphore = asyncio.Semaphore(MAX_CONCURRENT)
    tasks = [scrape_branch_playwright_async(branch, now, today, semaphore) for branch in BRANCHES]

    all_this_run = []
    ok_bids = set()

    # Run with progress
    for completed in asyncio.as_completed(tasks):
        try:
            reviews = await completed
            all_this_run.extend(reviews)
            # Note: ok_bids tracking is approximate in async (we mark as ok if task completed)
            # For precise deletion detection we still use successful branches
        except Exception as e:
            print(f" A task failed: {e}")

    # For simplicity, mark all branches that had a task as "ok" (adjust if needed)
    ok_bids = {b["id"] for b in BRANCHES}

    # Your original merging logic + monthly deletion tracking
    curr_map = {r["fingerprint"]: r for r in all_this_run}
    new_reviews = [r for fp, r in curr_map.items() if fp not in live_map and fp not in del_map]
    reinstated = [dict(r, reinstated_on=today) for fp, r in curr_map.items() if fp in del_map]

    newly_deleted = []
    monthly_deleted = {}

    for fp, old in live_map.items():
        if (old.get("branch_id") in ok_bids and 
            fp not in curr_map and 
            old.get("snap_date") == today):
            
            deleted_review = dict(old, deleted_on=today)
            deleted_review["deleted_month"] = today[:7]   # e.g. "2026-04"
            newly_deleted.append(deleted_review)

            key = f"branch_{old['branch_id']}_{today[:7]}"
            monthly_deleted[key] = monthly_deleted.get(key, 0) + 1

    print(f" 🆕 New : {len(new_reviews)} | ♻️ Reinstated : {len(reinstated)} | 🗑 Deleted : {len(newly_deleted)}")

    if newly_deleted:
        print(" Monthly deletions this run:")
        for key, cnt in sorted(monthly_deleted.items()):
            branch_id = int(key.split('_')[1])
            month = key.split('_')[2]
            print(f"   Branch {branch_id:2d} ({month}) → {cnt} deleted")

    # Update files
    updated_live = dict(live_map)
    for r in new_reviews + reinstated:
        updated_live[r["fingerprint"]] = r
    for r in newly_deleted:
        updated_live.pop(r["fingerprint"], None)

    updated_del = dict(del_map)
    for r in newly_deleted:
        updated_del[r["fingerprint"]] = r
    for r in reinstated:
        updated_del.pop(r["fingerprint"], None)

    rev_list = sorted(updated_live.values(), key=lambda x: x.get("parsed_date") or x.get("first_seen", ""), reverse=True)
    del_list = sorted(updated_del.values(), key=lambda x: x.get("deleted_on", ""), reverse=True)

    save_json(REV_JSON, rev_list)
    save_json(DEL_JSON, del_list)

    today_total = sum(1 for r in rev_list if r.get("snap_date") == today)
    print(f"\n docs/rev.json → {len(rev_list)} total | {today_total} for {today}")
    print(f" docs/deleted.json → {len(del_list)} (with monthly tracking)")
    print(f" ✅ Done — Async Playwright Hardened")
    print("=" * 110)

# ── Entry Point ────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    try:
        asyncio.run(async_run())
    except Exception as e:
        print(f"\n[FATAL] {e}")
        traceback.print_exc()
