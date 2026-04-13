"""
Sathya Reviews Scraper v2.7
- Direct Business Profile URL
- Only reviews ≤ 23 hours old
- Stores reviewer name, rating, text, time (relative), parsed_date
- Full deduplication + deleted/reinstated logic
"""

import json
import time
import random
import hashlib
import re
import traceback
from datetime import datetime, timedelta
from pathlib import Path

try:
    from seleniumbase import SB
except ImportError:
    print("ERROR: Run 'pip install seleniumbase'")
    exit(1)

# ── Paths ──────────────────────────────────────────────────────────────────────
REPO_ROOT = Path(__file__).parent
REV_JSON = REPO_ROOT / "docs" / "rev.json"
DEL_JSON = REPO_ROOT / "docs" / "deleted.json"

# ── Tuning ─────────────────────────────────────────────────────────────────────
SCROLL_COUNT = 10
SCROLL_PAUSE = 1.1
BRANCH_PAUSE = (3.5, 6.0)
MAX_RETRIES = 2

# ── 36 Branches ────────────────────────────────────────────────────────────────
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

# ── Direct Business Profile URL ───────────────────────────────────────────────
def place_id_to_url(place_id: str) -> str:
    return f"https://www.google.com/maps/place/?q=place_id:{place_id}"


def make_fingerprint(branch_id: int, author: str, text: str, rating: float) -> str:
    raw = f"{branch_id}|{(author or '').strip().lower()}|{(text or '')[:120].strip().lower()}|{round(rating, 1)}"
    return hashlib.sha1(raw.encode()).hexdigest()[:16]


def ist_now() -> datetime:
    return datetime.utcnow() + timedelta(hours=5, minutes=30)


def is_review_within_23_hours(relative_time: str) -> bool:
    """Only keep reviews that are 23 hours or less old"""
    if not relative_time:
        return False
    text = relative_time.lower().strip()

    # Accept "51 minutes ago", "2 hours ago", "just now", etc.
    if any(word in text for word in ["just now", "minute", "moments", "hour"]):
        return True

    # Reject anything "a day ago" or older
    if any(word in text for word in ["day ago", "days ago", "week", "month", "year"]):
        return False

    # Numeric hour check
    match = re.search(r'(\d+)\s*hour', text)
    if match:
        return int(match.group(1)) <= 23

    return False


def parse_relative_time(relative_str: str, reference_date: datetime = None) -> str | None:
    if not relative_str:
        return None
    if reference_date is None:
        reference_date = ist_now()

    text = relative_str.lower().strip()

    if any(w in text for w in ["just now", "minute", "moments"]):
        return reference_date.strftime("%Y-%m-%d %H:%M:%S")
    if "yesterday" in text:
        return (reference_date - timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")

    patterns = [
        (r'(\d+)\s*hour', timedelta(hours=1)),
        (r'(\d+)\s*day', timedelta(days=1)),
        (r'(\d+)\s*week', timedelta(weeks=1)),
        (r'(\d+)\s*month', timedelta(days=30)),
        (r'(\d+)\s*year', timedelta(days=365)),
    ]
    for pattern, unit in patterns:
        m = re.search(pattern, text)
        if m:
            num = int(m.group(1))
            delta = timedelta(seconds=unit.total_seconds() * num)
            return (reference_date - delta).strftime("%Y-%m-%d %H:%M:%S")
    return reference_date.strftime("%Y-%m-%d %H:%M:%S")


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


# ── Scrape One Branch ──────────────────────────────────────────────────────────
def scrape_branch(sb, branch: dict, now: datetime) -> list:
    bid = branch["id"]
    name = branch["name"]
    snap_date = now.strftime("%Y-%m-%d")
    snap_time = now.strftime("%H:%M IST")

    sb.open(place_id_to_url(branch["place_id"]))
    time.sleep(random.uniform(3.5, 5.0))

    # Click Reviews tab
    for sel in ['button[aria-label*="Reviews"]', 'button[aria-label*="reviews"]', '[role="tab"]:nth-child(2)']:
        try:
            if sb.is_element_visible(sel, timeout=5):
                sb.click(sel)
                time.sleep(3.0)
                break
        except:
            pass

    # Sort by Newest
    sorted_newest = False
    try:
        if sb.is_element_visible('button[aria-label*="Sort"]', timeout=4):
            sb.click('button[aria-label*="Sort"]')
            time.sleep(2.2)
            if sb.is_element_visible('//li[@data-index="1"]', by="xpath", timeout=3):
                sb.click('//li[@data-index="1"]', by="xpath")
                time.sleep(2.8)
                sorted_newest = True
            elif sb.is_element_visible('span:contains("Newest")', timeout=2):
                sb.click('span:contains("Newest")')
                time.sleep(2.5)
                sorted_newest = True
    except:
        pass

    sort_status = "(Newest)" if sorted_newest else "(Default)"
    print(f"  [{bid:02d}/36] {name:<24} {sort_status}", end="  ", flush=True)

    # Scroll
    for _ in range(SCROLL_COUNT):
        sb.execute_script("""
            var c = document.querySelector('.m6QErb[tabindex]') || document.querySelector('div[role="feed"]');
            if (c) c.scrollTop = c.scrollHeight; else window.scrollTo(0, document.body.scrollHeight);
        """)
        time.sleep(SCROLL_PAUSE)

    # Extract reviews
    raw = sb.execute_script("""
        (function() {
            var sels = ['.jftiEf', 'div[data-review-id]', '.GHT2ce'];
            var cards = [];
            for (var s of sels) {
                var f = document.querySelectorAll(s);
                if (f.length > cards.length) cards = Array.from(f);
            }
            return cards.map(function(c) {
                var aEl = c.querySelector('.d4r55,.X43Kjb,.TSUbDb,[class*="fontHeadlineSmall"]');
                var rEl = c.querySelector('[aria-label*="star"]');
                var tEl = c.querySelector('.wiI7pd,.MyEned,.Jtu6Td');
                var dEl = c.querySelector('.rsqaWe,.DU9Pgb,span[aria-label*="ago"]');
                var rm = (rEl ? rEl.getAttribute('aria-label') : '').match(/([\\d.]+)/);
                return {
                    author: aEl ? aEl.innerText.trim() : 'Anonymous',
                    rating: rm ? parseFloat(rm[1]) : 0,
                    text: tEl ? tEl.innerText.trim() : '',
                    time: dEl ? (dEl.innerText || dEl.getAttribute('aria-label') || '').trim() : ''
                };
            }).filter(r => r.rating > 0 && r.author);
        })();
    """) or []

    seen, out = set(), []
    for r in raw:
        if not is_review_within_23_hours(r.get("time", "")):
            continue

        fp = make_fingerprint(bid, r["author"], r["text"], r["rating"])
        if fp in seen:
            continue
        seen.add(fp)

        out.append({
            "fingerprint": fp,
            "branch_id": bid,
            "branch_name": name,
            "agm": branch["agm"],
            "author": r["author"],           # Important for HTML display
            "rating": r["rating"],
            "text": r["text"],
            "time": r["time"],               # Original: "51 minutes ago"
            "parsed_date": parse_relative_time(r["time"], now),
            "snap_date": snap_date,
            "snap_time": snap_time,
            "first_seen": f"{snap_date} {snap_time}",
        })

    print(f"✓  {len(out):2d} new/recent reviews")
    return out


# ── Main ───────────────────────────────────────────────────────────────────────
def run():
    now = ist_now()
    run_label = now.strftime("%Y-%m-%d %H:%M IST")

    print("=" * 90)
    print(f"  Sathya Reviews Scraper v2.7 — Direct URL + 23h Filter — {run_label}")
    print("=" * 90)

    print("\n[1/4] Loading existing data...")
    live_map = {r["fingerprint"]: r for r in load_json(REV_JSON) if "fingerprint" in r}
    del_map = {r["fingerprint"]: r for r in load_json(DEL_JSON) if "fingerprint" in r}
    print(f"  Live: {len(live_map)} | Deleted: {len(del_map)}")

    print(f"\n[2/4] Scraping {len(BRANCHES)} branches...")
    t0 = time.time()
    all_reviews = []
    ok_bids = set()

    with SB(uc=True, xvfb=True) as sb:
        sb.open("https://www.google.com/maps")
        time.sleep(3.0)
        print("  [warm-up] ✓\n")

        for branch in BRANCHES:
            for attempt in range(1, MAX_RETRIES + 1):
                try:
                    reviews = scrape_branch(sb, branch, now)
                    all_reviews.extend(reviews)
                    ok_bids.add(branch["id"])
                    break
                except Exception:
                    if attempt == MAX_RETRIES:
                        print(f"  [{branch['id']:02d}] Failed")
                    else:
                        time.sleep(random.uniform(6, 10))
            time.sleep(random.uniform(*BRANCH_PAUSE))

    elapsed = int(time.time() - t0)
    print(f"\n[3/4] Processing changes... Total extracted: {len(all_reviews)} reviews")

    curr_map = {r["fingerprint"]: r for r in all_reviews}

    new_reviews = [r for fp, r in curr_map.items() if fp not in live_map and fp not in del_map]
    reinstated = [dict(r, reinstated_on=now.strftime("%Y-%m-%d")) for fp, r in curr_map.items() if fp in del_map]

    newly_deleted = []
    for fp, old in live_map.items():
        if old.get("branch_id") in ok_bids and fp not in curr_map:
            d = dict(old)
            d["deleted_on"] = now.strftime("%Y-%m-%d")
            newly_deleted.append(d)

    print(f"  🆕 New: {len(new_reviews)} | ♻️ Reinstated: {len(reinstated)} | 🗑 Deleted: {len(newly_deleted)}")

    print("\n[4/4] Saving JSON files...")
    updated_live = dict(live_map)
    for fp, r in curr_map.items():
        if fp in updated_live:
            updated_live[fp].update({
                "snap_date": r["snap_date"],
                "snap_time": r["snap_time"],
                "time": r["time"],
                "parsed_date": r.get("parsed_date")
            })
    for r in new_reviews + reinstated:
        updated_live[r["fingerprint"]] = r
    for r in newly_deleted:
        updated_live.pop(r["fingerprint"], None)

    updated_del = dict(del_map)
    for r in newly_deleted:
        updated_del[r["fingerprint"]] = r
    for r in reinstated:
        updated_del.pop(r["fingerprint"], None)

    # Sort by parsed_date (most recent first)
    rev_list = sorted(updated_live.values(), key=lambda x: x.get("parsed_date") or x.get("first_seen", ""), reverse=True)
    del_list = sorted(updated_del.values(), key=lambda x: x.get("deleted_on", ""), reverse=True)

    save_json(REV_JSON, rev_list)
    save_json(DEL_JSON, del_list)

    print(f"  rev.json     → {len(rev_list)} reviews")
    print(f"  deleted.json → {len(del_list)} reviews")
    print(f"\n  ✅ Done — {run_label}")
    print("=" * 90)


if __name__ == "__main__":
    try:
        run()
    except Exception as e:
        print(f"\n[FATAL] {e}")
        traceback.print_exc()
        exit(1)
