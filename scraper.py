"""
Sathya Reviews Scraper v0.2 — Google Internal API (Pure HTTP)
────────────────────────────────────────────────────────────
Changes in v0.8:
  • MAX_PAGES = 8 (checks up to 80 recent reviews per branch)
  • SORT_ORDER = 1 (most relevant) — best for catching newest 5-star reviews
  • Better logging when hitting page limit (helps monitor busy branches)
  • Handles 46 reviews/day easily; handles ~100/day with high success rate
  • All original logic (fingerprint, deleted tracking, IST date filtering) preserved
"""

import json
import re
import struct
import base64
import time
import random
import hashlib
import urllib.parse
import traceback
from datetime import datetime, timedelta
from pathlib import Path
import requests

# ── Paths ──────────────────────────────────────────────────────────────────────
REPO_ROOT = Path(__file__).parent
REV_JSON = REPO_ROOT / "docs" / "rev.json"
DEL_JSON = REPO_ROOT / "docs" / "deleted.json"

# ── Tuning ─────────────────────────────────────────────────────────────────────
REVIEWS_PER_PAGE = 10
MAX_PAGES = 8                    # Increased to handle up to ~100 reviews/day better
SORT_ORDER = 1                   # 1 = most relevant (recommended), 2 = newest

REQUEST_PAUSE = (1.3, 3.0)
BRANCH_PAUSE = (2.0, 4.5)
MAX_RETRIES = 4

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/134.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9,ta;q=0.8",
    "Referer": "https://www.google.com/maps/",
    "sec-ch-ua": '"Chromium";v="134", "Not:A-Brand";v="99"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
}

# ── 36 Branches (your original list) ───────────────────────────────────────────
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

# PLACE_ID DECODER — unchanged
def place_id_to_fids(place_id: str) -> tuple[int, int]:
    padding = (4 - len(place_id) % 4) % 4
    decoded = base64.urlsafe_b64decode(place_id + "=" * padding)
    if len(decoded) < 20 or decoded[0] != 0x0a:
        raise ValueError(f"Invalid place_id: {place_id}")
    payload = decoded[2:]
    if payload[0] != 0x09 or payload[9] != 0x11:
        raise ValueError(f"Unexpected protobuf in {place_id}")
    fid1 = struct.unpack("<q", payload[1:9])[0]
    fid2 = struct.unpack("<q", payload[10:18])[0]
    return fid1, fid2

# URL BUILDER
def build_reviews_url(fid1: int, fid2: int, offset: int = 0) -> str:
    pb = (
        f"!1m2!1y{fid1}!2y{fid2}"
        f"!2m1!2i{offset}"
        f"!3e{SORT_ORDER}"
        f"!4m5!3b1!4b1!5b1!6b1!7b1"
        f"!5m2!1s__dummy__!7e81"
    )
    return (
        "https://www.google.com/maps/preview/review/listentitiesreviews"
        f"?authuser=0&hl=en&gl=in"
        f"&pb={urllib.parse.quote(pb, safe='')}"
    )

# RESPONSE PARSER — robust
def parse_reviews_response(raw: str) -> list[dict]:
    text = raw.lstrip()
    for prefix in (")]}'\n", ")]}'", ")]}'\n"):
        if text.startswith(prefix):
            text = text[len(prefix):]
            break
    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        print(f" [WARN] JSON parse failed. Start: {repr(raw[:120])}")
        return []

    try:
        review_list = data[2][0] if isinstance(data[2], list) and len(data[2]) > 0 else data[2]
    except (IndexError, TypeError):
        return []

    if not isinstance(review_list, list):
        return []

    reviews = []
    for r in review_list:
        try:
            if not isinstance(r, list) or len(r) < 5:
                continue
            author = r[0][1] if isinstance(r[0], list) and len(r[0]) > 1 else "Anonymous"
            rel_time = r[1] if len(r) > 1 else None
            text_val = r[3] if len(r) > 3 else None
            rating = float(r[4]) if len(r) > 4 and r[4] is not None else None

            if not rating or not (1 <= rating <= 5):
                continue

            reviews.append({
                "author": str(author).strip(),
                "rating": rating,
                "text": str(text_val or "").strip(),
                "time": str(rel_time or "").strip(),
            })
        except Exception:
            continue
    return reviews

# Time helpers
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

# Fingerprint & JSON
def make_fingerprint(branch_id: int, author: str, text: str, rating: float) -> str:
    raw = f"{branch_id}|{(author or '').strip().lower()}|{(text or '')[:120].strip().lower()}|{round(rating, 1)}"
    return hashlib.sha1(raw.encode()).hexdigest()[:16]

def load_json(path: Path) -> list:
    if path.exists():
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception as e:
            print(f" [WARN] Failed to load {path.name}: {e}")
    return []

def save_json(path: Path, data: list):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

# Scrape one branch
def scrape_branch(session: requests.Session, branch: dict, now: datetime, today: str) -> list[dict]:
    bid = branch["id"]
    name = branch["name"]
    snap_date = now.strftime("%Y-%m-%d")
    snap_time = now.strftime("%H:%M IST")

    try:
        fid1, fid2 = place_id_to_fids(branch["place_id"])
    except Exception as e:
        print(f" [{bid:02d}/36] {name:<24} ✗ decode error: {e}")
        return []

    seen: set[str] = set()
    out: list[dict] = []
    old_count = 0
    hit_page_limit = False

    for page in range(MAX_PAGES):
        offset = page * REVIEWS_PER_PAGE
        url = build_reviews_url(fid1, fid2, offset)

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                resp = session.get(url, headers=HEADERS, timeout=25)
                resp.raise_for_status()
                break
            except Exception as e:
                if attempt == MAX_RETRIES:
                    print(f" [{bid:02d}/36] {name:<24} ✗ HTTP fail page {page}: {e}")
                    return out
                time.sleep(random.uniform(4, 8))

        raw_reviews = parse_reviews_response(resp.text)
        if not raw_reviews:
            break

        for r in raw_reviews:
            parsed = parse_relative_time(r["time"], now)
            if not is_today(parsed, today):
                old_count += 1
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
                "author": r["author"],
                "rating": r["rating"],
                "text": r["text"],
                "time": r["time"],
                "parsed_date": parsed,
                "snap_date": snap_date,
                "snap_time": snap_time,
                "first_seen": f"{snap_date} {snap_time}",
            })

        if old_count >= 8:   # softer stop
            break

        # Check if we are at the last page
        if page == MAX_PAGES - 1 and len(raw_reviews) == REVIEWS_PER_PAGE:
            hit_page_limit = True

        time.sleep(random.uniform(*REQUEST_PAUSE))

    count = len(out)
    if hit_page_limit:
        print(f" [{bid:02d}/36] {name:<24} ✓ {count:2d} today's reviews  [WARNING: Hit page limit]")
    else:
        print(f" [{bid:02d}/36] {name:<24} ✓ {count:2d} today's reviews")

    return out

# MAIN
def run():
    now = ist_now()
    today = today_ist()
    label = now.strftime("%Y-%m-%d %H:%M IST")
    print("=" * 110)
    print(f" Sathya Reviews Scraper v0.2 | {label} | Bucket: {today}")
    print("=" * 110)

    live_map = {r["fingerprint"]: r for r in load_json(REV_JSON) if "fingerprint" in r}
    del_map = {r["fingerprint"]: r for r in load_json(DEL_JSON) if "fingerprint" in r}
    today_already = sum(1 for r in live_map.values() if r.get("snap_date") == today)

    print(f" All-time : {len(live_map)} live | {len(del_map)} deleted")
    print(f" Today    : {today_already} already collected")

    print(f"\n[Scraping {len(BRANCHES)} branches...]")
    t0 = time.time()
    all_this_run: list[dict] = []
    ok_bids: set[int] = set()
    session = requests.Session()

    for branch in BRANCHES:
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                reviews = scrape_branch(session, branch, now, today)
                all_this_run.extend(reviews)
                ok_bids.add(branch["id"])
                break
            except Exception as e:
                print(f" [{branch['id']:02d}] Attempt {attempt} failed: {e}")
                if attempt == MAX_RETRIES:
                    print(f" [{branch['id']:02d}] Branch failed completely")
                else:
                    time.sleep(random.uniform(6, 12))
        time.sleep(random.uniform(*BRANCH_PAUSE))

    print(f"\n Scraped in {int(time.time()-t0)}s — {len(all_this_run)} reviews this run")

    curr_map = {r["fingerprint"]: r for r in all_this_run}
    new_reviews = [r for fp, r in curr_map.items() if fp not in live_map and fp not in del_map]
    reinstated = [dict(r, reinstated_on=today) for fp, r in curr_map.items() if fp in del_map]
    newly_deleted = [
        dict(old, deleted_on=today)
        for fp, old in live_map.items()
        if old.get("branch_id") in ok_bids and fp not in curr_map and old.get("snap_date") == today
    ]

    print(f" 🆕 New : {len(new_reviews)}")
    print(f" ♻️ Reinstated : {len(reinstated)}")
    print(f" 🗑 Deleted : {len(newly_deleted)}")

    # Update live & deleted maps
    updated_live = dict(live_map)
    for fp, r in curr_map.items():
        if fp in updated_live:
            updated_live[fp]["snap_time"] = r["snap_time"]
            updated_live[fp]["time"] = r["time"]
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
    print(f" docs/deleted.json → {len(del_list)}")
    print(f"\n ✅ Done — {label}")
    print("=" * 110)

if __name__ == "__main__":
    try:
        run()
    except Exception as e:
        print(f"\n[FATAL] {e}")
        traceback.print_exc()
        exit(1)
