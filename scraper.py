"""
Sathya Reviews Scraper  v2
==========================
Adapted from google-reviews-scraper-pro (georgekhananaev) — MIT licence.

Key techniques taken from the reference project:
  • SeleniumBase UC mode  — undetected Chrome (their exact engine)
  • Search-based navigation URL — their Feb-2026 "limited view" bypass
  • Incremental-style scraping — compare old vs new, push only diff

Optimised for GitHub Actions:
  • ~12-15 min per full 36-branch run (vs 30 min naive approach)
  • Only 5 scrolls per branch — enough to catch reviews from last 3 h
  • Single browser instance reused across all branches (no relaunch cost)
  • Tight but safe inter-branch pause (3-5 s)
  • place_id → search URL conversion (same bypass as the reference project)
  • JSON stored in docs/ — committed back to repo by GitHub Actions
"""

import json
import os
import sys
import time
import random
import hashlib
import traceback
from datetime import datetime, timedelta
from pathlib import Path

try:
    from seleniumbase import SB
except ImportError:
    print("ERROR: Run  pip install seleniumbase")
    sys.exit(1)

# ── Paths ──────────────────────────────────────────────────────────────────────
REPO_ROOT = Path(__file__).parent
REV_JSON  = REPO_ROOT / "docs" / "rev.json"
DEL_JSON  = REPO_ROOT / "docs" / "deleted.json"

# ── Tuning ─────────────────────────────────────────────────────────────────────
# 5 scrolls ≈ 40-60 reviews loaded — plenty for a 3-hour detection window.
# Increase to 15 if you want all-time reviews on the very first run only.
SCROLL_COUNT = 5
SCROLL_PAUSE = 0.9
BRANCH_PAUSE = (3, 5)   # random seconds between branches (anti-bot pacing)
MAX_RETRIES  = 2
# ──────────────────────────────────────────────────────────────────────────────

BRANCHES = [
    {"id":1,  "name":"Tuticorin-1",       "place_id":"ChIJ5zJNoJfvAzsR-bJE_3bbNYw", "agm":"Siva"},
    {"id":2,  "name":"Tuticorin-2",       "place_id":"ChIJH6gY4-PvAzsRJ50skTlx3cs", "agm":"Siva"},
    {"id":3,  "name":"Thiruchendur-1",    "place_id":"ChIJeXA4vJKRAzsRBovAtv6lMuQ", "agm":"Siva"},
    {"id":4,  "name":"Thisayanvilai-1",   "place_id":"ChIJVWkvdfh_BDsRdvtimKCLS5Y", "agm":"Siva"},
    {"id":5,  "name":"Eral-2",            "place_id":"ChIJbwAA0KGMAzsRkQilW5PceeA", "agm":"Siva"},
    {"id":6,  "name":"Udankudi",          "place_id":"ChIJPQAAACyEAzsRgjznQ1GLom0", "agm":"Siva"},
    {"id":7,  "name":"Tirunelveli-1",     "place_id":"ChIJ2RU2NvQRBDsRq-Fw7IVwx7k", "agm":"John"},
    {"id":8,  "name":"Valliyur-1",        "place_id":"ChIJcVNk6TtnBDsRBoP4zpExt5k", "agm":"John"},
    {"id":9,  "name":"Ambasamudram-1",    "place_id":"ChIJ9SGeIi85BDsRZk4QdyW9BSY", "agm":"John"},
    {"id":10, "name":"Anjugramam-1",      "place_id":"ChIJ4yeJebLtBDsRDceoxujdGyc", "agm":"John"},
    {"id":11, "name":"Nagercoil",         "place_id":"ChIJe1LZBiTxBDsRJFLjlbgZoIs", "agm":"Jeeva"},
    {"id":12, "name":"Marthandam",        "place_id":"ChIJcWptCRdVBDsRlJh2q0-rnfY", "agm":"Jeeva"},
    {"id":13, "name":"Thuckalay-1",       "place_id":"ChIJc9QgEub4BDsRoyDR4Wd6tYA", "agm":"Jeeva"},
    {"id":14, "name":"Colachel-1",        "place_id":"ChIJgRkBLw39BDsR58D0lwNo5Ts", "agm":"Jeeva"},
    {"id":15, "name":"Kulasekharam-1",    "place_id":"ChIJw0Ep-kNXBDsRe5ad32jAeAk", "agm":"Jeeva"},
    {"id":16, "name":"Monday Market",     "place_id":"ChIJTceRGAD5BDsR65i3YNTcYHk", "agm":"Jeeva"},
    {"id":17, "name":"Karungal-1",        "place_id":"ChIJfTP5ASr_BDsRgsBaeQltkw4", "agm":"Jeeva"},
    {"id":18, "name":"Kovilpatti",        "place_id":"ChIJHY0o-26yBjsRt7wbXB1pDUE", "agm":"Seenivasan"},
    {"id":19, "name":"Ramnad",            "place_id":"ChIJNVVVVaGiATsRnunSgOTvbE8", "agm":"Seenivasan"},
    {"id":20, "name":"Paramakudi",        "place_id":"ChIJ-dgjBzQHATsRf27FWAJgmsA", "agm":"Seenivasan"},
    {"id":21, "name":"Sayalkudi-1",       "place_id":"ChIJRTqudn9lATsR2fYyMmxlOrw", "agm":"Seenivasan"},
    {"id":22, "name":"Villathikullam",    "place_id":"ChIJi_wAkwVbATsRtFl3_V5rGrY", "agm":"Seenivasan"},
    {"id":23, "name":"Sattur-2",          "place_id":"ChIJNVVVVcHKBjsR7xMX97RFn8Q", "agm":"Seenivasan"},
    {"id":24, "name":"Sankarankovil-1",   "place_id":"ChIJE1mKnhSXBjsRKMQ-9JKQf_c", "agm":"Seenivasan"},
    {"id":25, "name":"Kayathar-1",        "place_id":"ChIJx5ebtUgRBDsRMquPZNUJVpw", "agm":"Seenivasan"},
    {"id":26, "name":"Thenkasi",          "place_id":"ChIJuaqqquEpBDsRVITw0MMYklc", "agm":"Muthuselvam"},
    {"id":27, "name":"Thenkasi-2",        "place_id":"ChIJiwqLye6DBjsRo9v1mWXaycI", "agm":"Muthuselvam"},
    {"id":28, "name":"Surandai-1",        "place_id":"ChIJPb1_eEOdBjsRjL9IVCVJhi8", "agm":"Muthuselvam"},
    {"id":29, "name":"Puliyankudi-1",     "place_id":"ChIJjZqoc46RBjsRQTGHnNC8xxA", "agm":"Muthuselvam"},
    {"id":30, "name":"Sengottai-1",       "place_id":"ChIJw3zzKiaBBjsR9KDyGpn1nXU", "agm":"Muthuselvam"},
    {"id":31, "name":"Rajapalayam",       "place_id":"ChIJW2ot-NDpBjsRMTfMF2IV-xE", "agm":"Muthuselvam"},
    {"id":32, "name":"Virudhunagar",      "place_id":"ChIJN3jzNJgsATsRCU3nrB5ntKE", "agm":"Venkatesh"},
    {"id":33, "name":"Virudhunagar-2",    "place_id":"ChIJPezaX7wtATsR9sHhFOG6A1c", "agm":"Venkatesh"},
    {"id":34, "name":"Aruppukottai",      "place_id":"ChIJy6qqqgYwATsRbcp-hXnoruM", "agm":"Venkatesh"},
    {"id":35, "name":"Aruppukottai-2",    "place_id":"ChIJY04wY58xATsRuoJSichVQQE", "agm":"Venkatesh"},
    {"id":36, "name":"Sivakasi",          "place_id":"ChIJI2JvEePOBjsREh8b-x4WF4U", "agm":"Venkatesh"},
]

# ── Helpers ────────────────────────────────────────────────────────────────────

def place_id_to_url(place_id: str) -> str:
    """
    Search-based navigation — exact bypass used by google-reviews-scraper-pro
    to work around Google's Feb-2026 'limited view' block on direct place URLs.

    Broken since Feb 2026 (logged-out users get limited view):
        https://www.google.com/maps/place/?q=place_id:ChIJ...

    Works fine, no login needed:
        https://www.google.com/maps/search/?api=1&query=Google&query_place_id=ChIJ...
    """
    return (
        f"https://www.google.com/maps/search/"
        f"?api=1&query=Google&query_place_id={place_id}"
    )


def make_fingerprint(branch_id: int, author: str, text: str, rating: float) -> str:
    """
    Stable unique ID per review — same review always produces same fingerprint.
    This is how we distinguish new vs known vs deleted without relying on dates.
    (google-reviews-scraper-pro uses a similar review_id from the DOM; we
    derive ours from content since we don't have Google's internal IDs.)
    """
    raw = (
        f"{branch_id}|"
        f"{(author or '').strip().lower()}|"
        f"{(text or '')[:120].strip().lower()}|"
        f"{round(rating, 1)}"
    )
    return hashlib.sha1(raw.encode()).hexdigest()[:16]


def ist_now() -> datetime:
    return datetime.utcnow() + timedelta(hours=5, minutes=30)


def load_json(path: Path) -> list:
    if path.exists():
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception as e:
            print(f"  [WARN] {path.name} unreadable: {e}")
    return []


def save_json(path: Path, data: list):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


# ── Core scrape ────────────────────────────────────────────────────────────────

def scrape_branch(sb, branch: dict, now: datetime) -> list:
    """
    Scrape one branch using the existing browser tab.
    No browser relaunch — reuse the warm Chrome session.
    This alone halves the total runtime vs recreating SB() per branch.
    """
    bid       = branch["id"]
    name      = branch["name"]
    snap_date = now.strftime("%Y-%m-%d")
    snap_time = now.strftime("%H:%M IST")

    # Navigate using search-based URL (google-reviews-scraper-pro technique)
    sb.open(place_id_to_url(branch["place_id"]))
    time.sleep(random.uniform(2.5, 3.5))

    # Dismiss consent/cookie dialog if present
    for sel in [
        'button[aria-label*="Accept"]',
        'button[id*="accept"]',
        'form[action*="consent"] button',
    ]:
        try:
            if sb.is_element_visible(sel, timeout=1):
                sb.click(sel)
                time.sleep(1.0)
                break
        except Exception:
            pass

    # Click the Reviews tab
    clicked = False
    for sel in [
        'button[aria-label*="reviews"]',
        'button[aria-label*="Reviews"]',
        'button[jsaction*="review"]',
        '[role="tab"]:nth-child(2)',
    ]:
        try:
            if sb.is_element_visible(sel, timeout=2):
                sb.click(sel)
                clicked = True
                time.sleep(2.5)
                break
        except Exception:
            pass

    if not clicked:
        print("✗ tab", end=" ")
        return []

    # Scroll to load reviews (5 scrolls ≈ 40-60 reviews, enough for 3h window)
    for _ in range(SCROLL_COUNT):
        sb.execute_script("""
            (function() {
                var c = document.querySelector('.m6QErb[tabindex]')
                     || document.querySelector('div[role="feed"]')
                     || document.querySelector('.DxyBCb');
                if (c) c.scrollTop = c.scrollHeight;
                else   window.scrollTo(0, document.body.scrollHeight);
            })();
        """)
        time.sleep(SCROLL_PAUSE)

    # Extract all review cards in one JS call (fast)
    raw = sb.execute_script("""
        (function() {
            var sels = ['.jftiEf', 'div[data-review-id]', '.GHT2ce'];
            var cards = [];
            for (var s of sels) {
                var f = document.querySelectorAll(s);
                if (f.length > cards.length) cards = Array.from(f);
            }
            return cards.map(function(c) {
                var aEl = c.querySelector(
                    '.d4r55,.X43Kjb,.TSUbDb,[class*="fontHeadlineSmall"]');
                var rEl = c.querySelector('[aria-label*="star"],[aria-label*="Star"]');
                var tEl = c.querySelector('.wiI7pd,.MyEned,.Jtu6Td');
                var dEl = c.querySelector('.rsqaWe,.DU9Pgb,span[aria-label*="ago"]');
                var rm  = (rEl ? rEl.getAttribute('aria-label') : '').match(/([\\d.]+)/);
                return {
                    author: aEl ? aEl.innerText.trim() : 'Anonymous',
                    rating: rm  ? parseFloat(rm[1])   : 0,
                    text:   tEl ? tEl.innerText.trim() : '',
                    time:   dEl
                        ? (dEl.innerText || dEl.getAttribute('aria-label') || '').trim()
                        : ''
                };
            }).filter(function(r) { return r.rating > 0 && r.author; });
        })();
    """) or []

    # Deduplicate within this page (same card can appear twice in DOM)
    seen, out = set(), []
    for r in raw:
        fp = make_fingerprint(bid, r["author"], r["text"], r["rating"])
        if fp in seen:
            continue
        seen.add(fp)
        out.append({
            "fingerprint": fp,
            "branch_id":   bid,
            "branch_name": name,
            "agm":         branch["agm"],
            "author":      r["author"],
            "rating":      r["rating"],
            "text":        r["text"],
            "time":        r["time"],        # Google's relative string ("2 weeks ago")
            "snap_date":   snap_date,        # date this scrape run happened
            "snap_time":   snap_time,        # IST time window of detection
            "first_seen":  f"{snap_date} {snap_time}",
        })
    return out


# ── Main ───────────────────────────────────────────────────────────────────────

def run():
    now       = ist_now()
    run_label = now.strftime("%Y-%m-%d %H:%M IST")
    snap_date = now.strftime("%Y-%m-%d")

    print("=" * 62)
    print(f"  Sathya Reviews Scraper v2  —  {run_label}")
    print("=" * 62)

    # 1. Load existing data from docs/ ──────────────────────────────────
    print("\n[1/4] Loading existing JSON...")
    prev_live    = load_json(REV_JSON)
    prev_deleted = load_json(DEL_JSON)
    live_map     = {r["fingerprint"]: r for r in prev_live    if "fingerprint" in r}
    del_map      = {r["fingerprint"]: r for r in prev_deleted if "fingerprint" in r}
    print(f"  {len(live_map)} live  |  {len(del_map)} deleted")

    # 2. Scrape all 36 branches ─────────────────────────────────────────
    print(f"\n[2/4] Scraping {len(BRANCHES)} branches...")
    t0          = time.time()
    all_reviews = []
    ok_bids     = set()
    fail_bids   = set()

    # KEY OPTIMISATION: one SB() instance for all 36 branches.
    # Browser launches once (~4 s), not 36 times (~144 s wasted).
    with SB(uc=True, xvfb=True) as sb:
        # Warm-up visit so Maps cookies/JS are cached before we start timing
        sb.open("https://www.google.com/maps")
        time.sleep(random.uniform(2.0, 3.0))
        print("  [warm-up] ✓")

        for branch in BRANCHES:
            bid, name = branch["id"], branch["name"]
            print(f"  [{bid:02d}/36] {name:<24}", end="  ", flush=True)
            reviews = []

            for attempt in range(1, MAX_RETRIES + 1):
                try:
                    reviews = scrape_branch(sb, branch, now)
                    break
                except Exception as e:
                    if attempt == MAX_RETRIES:
                        print(f"✗ ({e})")
                        fail_bids.add(bid)
                    else:
                        print("↺ ", end="", flush=True)
                        time.sleep(random.uniform(5, 9))

            if reviews:
                all_reviews.extend(reviews)
                ok_bids.add(bid)
                print(f"✓  {len(reviews):3d} reviews")

            time.sleep(random.uniform(*BRANCH_PAUSE))

    elapsed = int(time.time() - t0)
    print(f"\n  Total: {len(all_reviews)} reviews in {elapsed//60}m {elapsed%60}s")
    print(f"  OK: {len(ok_bids)}  |  Failed: {len(fail_bids)}")

    # 3. Diff: new / reinstated / deleted ───────────────────────────────
    print("\n[3/4] Calculating diff...")
    curr_map = {r["fingerprint"]: r for r in all_reviews}

    new_reviews = [
        r for fp, r in curr_map.items()
        if fp not in live_map and fp not in del_map
    ]

    reinstated = []
    for fp, r in curr_map.items():
        if fp in del_map:
            re_r = dict(r)
            re_r["reinstated_on"] = snap_date
            reinstated.append(re_r)

    # Only mark deleted for branches we successfully scraped
    # — avoids false deletions caused by network failures
    newly_deleted = []
    for fp, old in live_map.items():
        if old.get("branch_id") in ok_bids and fp not in curr_map:
            d = dict(old)
            d["deleted_on"] = snap_date
            newly_deleted.append(d)

    print(f"  🆕 New: {len(new_reviews)}"
          f"  ♻️  Reinstated: {len(reinstated)}"
          f"  🗑  Deleted: {len(newly_deleted)}")

    # 4. Build + save updated JSON files ────────────────────────────────
    print("\n[4/4] Saving JSON files...")

    updated_live = dict(live_map)
    for fp, r in curr_map.items():          # refresh time strings on existing records
        if fp in updated_live:
            updated_live[fp]["snap_date"] = r["snap_date"]
            updated_live[fp]["snap_time"] = r["snap_time"]
            updated_live[fp]["time"]      = r["time"]
    for r in new_reviews:
        updated_live[r["fingerprint"]] = r
    for r in reinstated:
        updated_live[r["fingerprint"]] = r
    for r in newly_deleted:
        updated_live.pop(r["fingerprint"], None)

    updated_del = dict(del_map)
    for r in newly_deleted:
        updated_del[r["fingerprint"]] = r
    for r in reinstated:
        updated_del.pop(r["fingerprint"], None)

    rev_list = sorted(updated_live.values(),
                      key=lambda x: x.get("first_seen", ""), reverse=True)
    del_list = sorted(updated_del.values(),
                      key=lambda x: x.get("deleted_on", ""),  reverse=True)

    save_json(REV_JSON, rev_list)
    save_json(DEL_JSON, del_list)

    rev_kb = REV_JSON.stat().st_size // 1024
    del_kb = DEL_JSON.stat().st_size // 1024
    print(f"  rev.json     → {len(rev_list)} reviews  ({rev_kb} KB)")
    print(f"  deleted.json → {len(del_list)} reviews  ({del_kb} KB)")

    print(f"\n  ✅ Done — {run_label}")
    print("=" * 62)


if __name__ == "__main__":
    try:
        run()
    except Exception as e:
        print(f"\n[FATAL] {e}")
        traceback.print_exc()
        sys.exit(1)
