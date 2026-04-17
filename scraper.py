"""
scraper.py – Sathya Review Scraper (production-grade, anti-detection, concurrent).

Run schedule (GitHub Actions, UTC → IST):
  00:30 UTC → 06:00 IST  [morning]
  06:30 UTC → 12:00 IST  [noon]
  12:30 UTC → 18:00 IST  [evening]
  18:30 UTC → 00:00 IST  [midnight]  ← reviews attributed to YESTERDAY's date

Deduplication:  fingerprint = sha256(rating|author[:30]|text[:200])
Deletion logic: a review absent from the CURRENT full scrape but present in
                rev.json is moved to deleted.json (tracked for 30 days).
"""

import asyncio
import random
import re
import traceback
from datetime import timedelta
from pathlib import Path

from playwright.async_api import async_playwright, TimeoutError as PWTimeout

from detection import CHROMIUM_ARGS, human_delay, jitter, make_stealth_context, micro_delay
from utils import (
    get_fingerprint,
    is_within_24h,
    ist_now,
    list_to_fp_map,
    load_json,
    parse_relative_time,
    save_json,
    current_run_slot,
    snap_date_for_run,
    IST,
)

# ── Paths ──────────────────────────────────────────────────────────────────────
DOCS_DIR = Path(__file__).parent / "docs"
REV_JSON = DOCS_DIR / "rev.json"
DEL_JSON = DOCS_DIR / "deleted.json"

# ── Concurrency ────────────────────────────────────────────────────────────────
MAX_CONCURRENT = 3          # 3 parallel browsers; stays below Google's radar
SCROLL_ROUNDS   = 12        # how many scroll iterations per branch
MAX_RETRIES     = 2         # retry once on failure before giving up

# ── Branch data ────────────────────────────────────────────────────────────────
BRANCHES = [
    {"id": 1,  "name": "Tuticorin-1",      "place_id": "ChIJ5zJNoJfvAzsR-bJE_3bbNYw", "agm": "Siva"},
    {"id": 2,  "name": "Tuticorin-2",      "place_id": "ChIJH6gY4-PvAzsRJ50skTlx3cs", "agm": "Siva"},
    {"id": 3,  "name": "Thiruchendur-1",   "place_id": "ChIJeXA4vJKRAzsRBovAtv6lMuQ", "agm": "Siva"},
    {"id": 4,  "name": "Thisayanvilai-1",  "place_id": "ChIJVWkvdfh_BDsRdvtimKCLS5Y", "agm": "Siva"},
    {"id": 5,  "name": "Eral-2",           "place_id": "ChIJbwAA0KGMAzsRkQilW5PceeA", "agm": "Siva"},
    {"id": 6,  "name": "Udankudi",         "place_id": "ChIJPQAAACyEAzsRgjznQ1GLom0", "agm": "Siva"},
    {"id": 7,  "name": "Tirunelveli-1",    "place_id": "ChIJ2RU2NvQRBDsRq-Fw7IVwx7k", "agm": "John"},
    {"id": 8,  "name": "Valliyur-1",       "place_id": "ChIJcVNk6TtnBDsRBoP4zpExt5k", "agm": "John"},
    {"id": 9,  "name": "Ambasamudram-1",   "place_id": "ChIJ9SGeIi85BDsRZk4QdyW9BSY", "agm": "John"},
    {"id": 10, "name": "Anjugramam-1",     "place_id": "ChIJ4yeJebLtBDsRDceoxujdGyc", "agm": "John"},
    {"id": 11, "name": "Nagercoil",        "place_id": "ChIJe1LZBiTxBDsRJFLjlbgZoIs", "agm": "Jeeva"},
    {"id": 12, "name": "Marthandam",       "place_id": "ChIJcWptCRdVBDsRlJh2q0-rnfY", "agm": "Jeeva"},
    {"id": 13, "name": "Thuckalay-1",      "place_id": "ChIJc9QgEub4BDsRoyDR4Wd6tYA", "agm": "Jeeva"},
    {"id": 14, "name": "Colachel-1",       "place_id": "ChIJgRkBLw39BDsR58D0lwNo5Ts", "agm": "Jeeva"},
    {"id": 15, "name": "Kulasekharam-1",   "place_id": "ChIJw0Ep-kNXBDsRe5ad32jAeAk", "agm": "Jeeva"},
    {"id": 16, "name": "Monday Market",    "place_id": "ChIJTceRGAD5BDsR65i3YNTcYHk", "agm": "Jeeva"},
    {"id": 17, "name": "Karungal-1",       "place_id": "ChIJfTP5ASr_BDsRgsBaeQltkw4", "agm": "Jeeva"},
    {"id": 18, "name": "Kovilpatti",       "place_id": "ChIJHY0o-26yBjsRt7wbXB1pDUE", "agm": "Seenivasan"},
    {"id": 19, "name": "Ramnad",           "place_id": "ChIJNVVVVaGiATsRnunSgOTvbE8", "agm": "Seenivasan"},
    {"id": 20, "name": "Paramakudi",       "place_id": "ChIJ-dgjBzQHATsRf27FWAJgmsA", "agm": "Seenivasan"},
    {"id": 21, "name": "Sayalkudi-1",      "place_id": "ChIJRTqudn9lATsR2fYyMmxlOrw", "agm": "Seenivasan"},
    {"id": 22, "name": "Villathikullam",   "place_id": "ChIJi_wAkwVbATsRtFl3_V5rGrY", "agm": "Seenivasan"},
    {"id": 23, "name": "Sattur-2",         "place_id": "ChIJNVVVVcHKBjsR7xMX97RFn8Q", "agm": "Seenivasan"},
    {"id": 24, "name": "Sankarankovil-1",  "place_id": "ChIJE1mKnhSXBjsRKMQ-9JKQf_c", "agm": "Seenivasan"},
    {"id": 25, "name": "Kayathar-1",       "place_id": "ChIJx5ebtUgRBDsRMquPZNUJVpw", "agm": "Seenivasan"},
    {"id": 26, "name": "Thenkasi",         "place_id": "ChIJuaqqquEpBDsRVITw0MMYklc", "agm": "Muthuselvam"},
    {"id": 27, "name": "Thenkasi-2",       "place_id": "ChIJiwqLye6DBjsRo9v1mWXaycI", "agm": "Muthuselvam"},
    {"id": 28, "name": "Surandai-1",       "place_id": "ChIJPb1_eEOdBjsRjL9IVCVJhi8", "agm": "Muthuselvam"},
    {"id": 29, "name": "Puliyankudi-1",    "place_id": "ChIJjZqoc46RBjsRQTGHnNC8xxA", "agm": "Muthuselvam"},
    {"id": 30, "name": "Sengottai-1",      "place_id": "ChIJw3zzKiaBBjsR9KDyGpn1nXU", "agm": "Muthuselvam"},
    {"id": 31, "name": "Rajapalayam",      "place_id": "ChIJW2ot-NDpBjsRMTfMF2IV-xE", "agm": "Muthuselvam"},
    {"id": 32, "name": "Virudhunagar",     "place_id": "ChIJN3jzNJgsATsRCU3nrB5ntKE", "agm": "Venkatesh"},
    {"id": 33, "name": "Virudhunagar-2",   "place_id": "ChIJPezaX7wtATsR9sHhFOG6A1c", "agm": "Venkatesh"},
    {"id": 34, "name": "Aruppukottai",     "place_id": "ChIJy6qqqgYwATsRbcp-hXnoruM", "agm": "Venkatesh"},
    {"id": 35, "name": "Aruppukottai-2",   "place_id": "ChIJY04wY58xATsRuoJSichVQQE", "agm": "Venkatesh"},
    {"id": 36, "name": "Sivakasi",         "place_id": "ChIJI2JvEePOBjsREh8b-x4WF4U", "agm": "Venkatesh"},
]


# ══════════════════════════════════════════════════════════════════════════════
# Core scraping logic – one branch, one browser context
# ══════════════════════════════════════════════════════════════════════════════

async def _scrape_branch_once(branch: dict, snap_date: str) -> list[dict]:
    """
    Opens Maps for one branch, scrolls to load recent reviews, parses them.
    Returns a list of review dicts (only reviews within the last 24 h).
    """
    place_id = branch["place_id"]
    url = f"https://www.google.com/maps/place/?q=place_id:{place_id}"
    reviews: list[dict] = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=CHROMIUM_ARGS,
        )
        try:
            context = await make_stealth_context(browser)
            page = await context.new_page()

            # ── Navigate ──────────────────────────────────────────────────────
            await page.goto(url, wait_until="domcontentloaded", timeout=30_000)
            await human_delay(2, 4)

            # ── Accept cookie/consent dialogs if present ───────────────────────
            for selector in ['button[aria-label*="Accept"]', 'button[jsname="higCR"]']:
                try:
                    btn = page.locator(selector).first
                    if await btn.is_visible(timeout=2000):
                        await btn.click()
                        await micro_delay()
                except Exception:
                    pass

            # ── Click the Reviews tab ─────────────────────────────────────────
            review_tab_selectors = [
                'div[role="tab"][aria-label*="Review"]',
                'button[aria-label*="Review"]',
                'div[data-tab-index="1"]',
            ]
            for sel in review_tab_selectors:
                try:
                    tab = page.locator(sel).first
                    if await tab.is_visible(timeout=3000):
                        await tab.click()
                        await human_delay(2, 3)
                        break
                except Exception:
                    continue

            # ── Sort by Newest ────────────────────────────────────────────────
            try:
                sort_btn = page.locator('button[aria-label*="Sort"]').first
                if await sort_btn.is_visible(timeout=3000):
                    await sort_btn.click()
                    await micro_delay()
                    # Choose "Newest" in the dropdown
                    newest_option = page.locator('[data-index="1"]').first
                    if await newest_option.is_visible(timeout=2000):
                        await newest_option.click()
                        await human_delay(2, 3)
            except Exception:
                pass  # Can't sort — still fine, we filter by time anyway

            # ── Find scrollable reviews container ─────────────────────────────
            scroll_target = None
            for container_sel in [
                'div[data-review-id]',
                'div.m6QErb[aria-label]',
                'div.DxyBCb',
            ]:
                try:
                    el = page.locator(container_sel).first
                    if await el.count():
                        # Walk up to find the scrollable parent
                        scroll_target = el
                        break
                except Exception:
                    continue

            # ── Scroll loop ───────────────────────────────────────────────────
            found_old = False  # stop early if we see a review > 24 h
            for _ in range(SCROLL_ROUNDS):
                if found_old:
                    break

                # Expand 'More' buttons on visible cards
                more_buttons = page.locator('button[aria-label="See more"], button span:text("More")')
                for i in range(await more_buttons.count()):
                    try:
                        btn = more_buttons.nth(i)
                        if await btn.is_visible(timeout=500):
                            await btn.click()
                            await micro_delay()
                    except Exception:
                        continue

                # Check the last visible timestamp – stop if older than 24 h
                time_els = page.locator('.rsqaWe, .DU9Pgb')
                count = await time_els.count()
                if count:
                    last_text = await time_els.nth(count - 1).inner_text(timeout=1000)
                    if last_text and not is_within_24h(last_text):
                        found_old = True

                # Scroll the panel (or full page as fallback)
                if scroll_target:
                    await page.evaluate(
                        "(el) => el.scrollBy(0, el.scrollHeight)",
                        await scroll_target.element_handle(),
                    )
                else:
                    await page.evaluate("window.scrollBy(0, document.body.scrollHeight)")

                await human_delay(jitter(2.0), jitter(3.5))

            # ── Parse review cards ────────────────────────────────────────────
            now = ist_now()
            cards = await page.locator("div.jftiEf").all()

            for card in cards:
                try:
                    # Author
                    author_el = card.locator(".d4r55, .fontHeadlineSmall").first
                    author = (await author_el.inner_text(timeout=2000)).strip() if await author_el.count() else "Unknown"

                    # Star rating  (aria-label like "5 stars")
                    rating_el = card.locator(".kvMYJc, .hCCjke span[aria-label]").first
                    rating = 0.0
                    if await rating_el.count():
                        raw = await rating_el.get_attribute("aria-label", timeout=2000) or ""
                        m = re.search(r"(\d+\.?\d*)", raw)
                        if m:
                            rating = float(m.group(1))

                    # Text
                    text_el = card.locator(".wiI7pd")
                    text = ""
                    if await text_el.count():
                        text = (await text_el.inner_text(timeout=2000)).replace("\n", " ").strip()

                    # Relative time
                    time_el = card.locator(".rsqaWe, .DU9Pgb").first
                    rel_time = (await time_el.inner_text(timeout=2000)).strip() if await time_el.count() else ""

                    # Only keep reviews posted within the past 24 hours
                    if not is_within_24h(rel_time):
                        continue

                    parsed_date = parse_relative_time(rel_time, now)
                    fp = get_fingerprint(rating, author, text)

                    reviews.append({
                        "fingerprint":  fp,
                        "branch_id":    branch["id"],
                        "branch_name":  branch["name"],
                        "agm":          branch["agm"],
                        "author":       author,
                        "rating":       rating,
                        "text":         text,
                        "rel_time":     rel_time,
                        "parsed_date":  parsed_date,
                        "snap_date":    snap_date,    # ← correctly set by caller
                        "first_seen":   now.strftime("%Y-%m-%d %H:%M"),
                    })

                except Exception:
                    continue

        finally:
            await browser.close()

    return reviews


async def scrape_branch(branch: dict, semaphore: asyncio.Semaphore, snap_date: str) -> list[dict]:
    """Wrapper with retry logic and semaphore-based concurrency control."""
    async with semaphore:
        for attempt in range(1, MAX_RETRIES + 2):
            try:
                reviews = await _scrape_branch_once(branch, snap_date)
                print(f"  ✅ {branch['name']:25s} → {len(reviews):2d} review(s)")
                return reviews
            except Exception as exc:
                msg = str(exc)[:80]
                if attempt <= MAX_RETRIES:
                    wait = 5 * attempt + random.uniform(2, 5)
                    print(f"  ⚠️  {branch['name']} attempt {attempt} failed ({msg}). Retrying in {wait:.0f}s…")
                    await asyncio.sleep(wait)
                else:
                    print(f"  ❌ {branch['name']} gave up after {MAX_RETRIES + 1} attempts. Last error: {msg}")
                    if "TRACEBACK" in msg or True:  # always print for CI logs
                        traceback.print_exc()
        return []


# ══════════════════════════════════════════════════════════════════════════════
# Deletion tracking
# ══════════════════════════════════════════════════════════════════════════════

DELETION_WINDOW_DAYS = 30


def process_deletions(
    old_live: dict,       # fp → review (from rev.json)
    old_del: dict,        # fp → review (from deleted.json)
    current_fps: set,     # fingerprints found in this full scrape
) -> tuple[dict, dict]:
    """
    Returns (updated_live, updated_deleted).

    Rules:
    • fp in old_live but NOT in current_fps → mark deleted (move to deleted.json)
    • fp in old_del but NOW in current_fps  → reinstated (move back to live)
    • Deleted items older than DELETION_WINDOW_DAYS are purged from deleted.json
    """
    now_str = ist_now().strftime("%Y-%m-%d %H:%M")
    updated_live: dict = {}
    updated_deleted: dict = dict(old_del)  # start with existing deleted items

    # 1. Reinstatements
    for fp in current_fps:
        if fp in old_del:
            item = dict(old_del[fp])
            item.pop("deleted_on", None)
            item["reinstated_on"] = now_str
            updated_live[fp] = item
            updated_deleted.pop(fp, None)
            print(f"    ♻️  Reinstated: {item.get('branch_name')} – {item.get('author')}")

    # 2. Deletions (was live, now absent)
    for fp, item in old_live.items():
        if fp not in current_fps:
            del_item = dict(item)
            del_item["deleted_on"] = now_str
            updated_deleted[fp] = del_item
            print(f"    🗑️  Deleted:    {item.get('branch_name')} – {item.get('author')}")

    # 3. Purge old deletions (> 30 days)
    cutoff = ist_now() - __import__("datetime").timedelta(days=DELETION_WINDOW_DAYS)
    purged = 0
    for fp in list(updated_deleted.keys()):
        del_on_str = updated_deleted[fp].get("deleted_on", "")
        try:
            del_on = __import__("datetime").datetime.strptime(del_on_str, "%Y-%m-%d %H:%M").replace(
                tzinfo=IST
            )
            if del_on < cutoff:
                del updated_deleted[fp]
                purged += 1
        except Exception:
            pass
    if purged:
        print(f"    🧹 Purged {purged} deletion record(s) older than {DELETION_WINDOW_DAYS} days")

    return updated_live, updated_deleted


# ══════════════════════════════════════════════════════════════════════════════
# Main
# ══════════════════════════════════════════════════════════════════════════════

async def main():
    slot = current_run_slot()
    snap_date = snap_date_for_run(slot)
    print(f"\n{'='*60}")
    print(f"  Sathya Review Scraper  |  slot={slot}  |  snap_date={snap_date}")
    print(f"{'='*60}\n")

    # ── Load existing data ─────────────────────────────────────────────────────
    old_live_list = load_json(REV_JSON)
    old_del_list  = load_json(DEL_JSON)
    old_live = list_to_fp_map(old_live_list)
    old_del  = list_to_fp_map(old_del_list)
    print(f"  Loaded: {len(old_live)} live, {len(old_del)} deleted\n")

    # ── Scrape all branches ────────────────────────────────────────────────────
    semaphore = asyncio.Semaphore(MAX_CONCURRENT)
    tasks = [scrape_branch(b, semaphore, snap_date) for b in BRANCHES]
    results = await asyncio.gather(*tasks)

    # Flatten and deduplicate within this batch (same fp from two branches is impossible,
    # but within one branch we guard anyway)
    seen_fps: set[str] = set()
    fresh_reviews: list[dict] = []
    for batch in results:
        for r in batch:
            if r["fingerprint"] not in seen_fps:
                seen_fps.add(r["fingerprint"])
                fresh_reviews.append(r)

    current_fps = {r["fingerprint"] for r in fresh_reviews}
    print(f"\n  Scraped {len(fresh_reviews)} unique reviews this run.\n")

    # ── Merge with existing live data ──────────────────────────────────────────
    # Build new live map: keep old reviews not in today's scope, add fresh ones
    # (We don't evict older snap_dates from rev.json — they accumulate as history)
    merged_live: dict = dict(old_live)

    # Add / update freshly scraped reviews
    for r in fresh_reviews:
        fp = r["fingerprint"]
        if fp not in merged_live:
            merged_live[fp] = r  # new review
        else:
            # Preserve first_seen; update rel_time & parsed_date with latest
            existing = dict(merged_live[fp])
            existing["rel_time"]    = r["rel_time"]
            existing["parsed_date"] = r["parsed_date"]
            merged_live[fp] = existing

    # ── Deletion tracking ──────────────────────────────────────────────────────
    # Only detect deletions against reviews that match today's snap_date
    # (so we don't falsely mark yesterday's reviews as deleted when today's run starts)
    todays_live_fps = {
        fp for fp, item in old_live.items() if item.get("snap_date") == snap_date
    }

    _upd_live_today, updated_deleted = process_deletions(
        old_live={fp: old_live[fp] for fp in todays_live_fps},
        old_del=old_del,
        current_fps=current_fps,
    )
    # Merge deletion results back into merged_live (reinstatements)
    for fp, item in _upd_live_today.items():
        merged_live[fp] = item

    # Remove from merged_live any fps that are now in updated_deleted
    for fp in list(updated_deleted.keys()):
        merged_live.pop(fp, None)

    # ── Sort and save ──────────────────────────────────────────────────────────
    final_live = sorted(
        merged_live.values(),
        key=lambda x: x.get("parsed_date", ""),
        reverse=True,
    )
    final_del = sorted(
        updated_deleted.values(),
        key=lambda x: x.get("deleted_on", ""),
        reverse=True,
    )

    save_json(REV_JSON, final_live)
    save_json(DEL_JSON, final_del)

    print(f"\n{'='*60}")
    print(f"  ✅ Saved {len(final_live)} live  |  {len(final_del)} deleted")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    asyncio.run(main())
