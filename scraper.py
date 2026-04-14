"""
Sathya Reviews Scraper v3 — Google Internal API (No Browser / No Selenium)
────────────────────────────────────────────────────────────────────────────
HOW IT WORKS
  Google exposes an internal RPC endpoint:
    https://www.google.com/maps/preview/review/listentitiesreviews
  
  It accepts a protobuf-style `pb` query string that controls:
    • which place to fetch (via signed-int64 feature-ID halves)
    • sort order  → 2 = NEWEST (we always use this)
    • page offset → 0, 10, 20 … for pagination

  The place_id (e.g. ChIJ5zJNoJfvAzsR-bJE_3bbNYw) encodes the feature ID.
  We decode it with base64 → bytes → two big-endian signed int64 values.

REQUIREMENTS
  pip install requests

NO Selenium / Playwright / browser needed.
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
REV_JSON  = REPO_ROOT / "docs" / "rev.json"
DEL_JSON  = REPO_ROOT / "docs" / "deleted.json"

# ── Tuning ─────────────────────────────────────────────────────────────────────
REVIEWS_PER_PAGE  = 10       # Google returns up to 10 per request
MAX_PAGES         = 3        # 3 pages = up to 30 reviews per branch
SORT_NEWEST       = 2        # 1=relevant 2=newest 3=highest 4=lowest
REQUEST_PAUSE     = (1.2, 2.5)
BRANCH_PAUSE      = (2.0, 4.0)
MAX_RETRIES       = 3

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.google.com/maps/",
}

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


# ══════════════════════════════════════════════════════════════════════════════
#  CORE: place_id → feature_id → fid1, fid2
# ══════════════════════════════════════════════════════════════════════════════

def place_id_to_fids(place_id: str) -> tuple[int, int]:
    """
    Decode a Google Maps place_id (base64url) into the two signed int64
    halves that the reviews RPC endpoint expects.

    place_id  e.g.  ChIJ5zJNoJfvAzsR-bJE_3bbNYw
    After base64url-decode we get 16 raw bytes.
    Split into two 8-byte chunks, each interpreted as big-endian signed int64.
    """
    # Pad to multiple of 4
    padding = (4 - len(place_id) % 4) % 4
    decoded = base64.urlsafe_b64decode(place_id + "=" * padding)

    if len(decoded) < 16:
        raise ValueError(f"Decoded place_id too short ({len(decoded)} bytes): {place_id}")

    fid1 = struct.unpack(">q", decoded[0:8])[0]
    fid2 = struct.unpack(">q", decoded[8:16])[0]
    return fid1, fid2


def build_reviews_url(fid1: int, fid2: int, offset: int = 0, sort: int = SORT_NEWEST) -> str:
    """
    Build the internal Google Maps review RPC URL.
    sort: 1=relevant  2=newest  3=highest  4=lowest
    """
    pb = (
        f"!1m2!1y{fid1}!2y{fid2}"
        f"!2m1!2i{offset}"
        f"!3e{sort}"
        f"!4m5!3b1!4b1!5b1!6b1!7b1"
        f"!5m2!1s__dummy__!7e81"
    )
    return (
        "https://www.google.com/maps/preview/review/listentitiesreviews"
        f"?authuser=0&hl=en&gl=in"
        f"&pb={urllib.parse.quote(pb, safe='')}"
    )


# ══════════════════════════════════════════════════════════════════════════════
#  PARSING
# ══════════════════════════════════════════════════════════════════════════════

def parse_reviews_response(raw: str) -> list[dict]:
    """
    Parse the raw Google RPC response (starts with )]}'\n).
    Returns a list of review dicts with: author, rating, text, time.
    """
    # Strip anti-XSSI prefix
    text = raw.lstrip()
    for prefix in (")]}'\n", ")]}'"):
        if text.startswith(prefix):
            text = text[len(prefix):]
            break

    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        return []

    reviews = []

    # Reviews live at data[2] as a list of review arrays
    try:
        review_list = data[2]
    except (IndexError, TypeError):
        return []

    if not review_list:
        return []

    for r in review_list:
        try:
            # r[0][1]  = author name
            # r[4]     = rating (1–5)
            # r[3]     = review text  (may be None)
            # r[1]     = relative time string e.g. "2 hours ago"
            author = None
            rating = None
            text   = None
            rel_time = None

            # Author
            try:
                author = r[0][1]
            except Exception:
                pass

            # Rating
            try:
                rating = float(r[4])
            except Exception:
                pass

            # Text
            try:
                text = r[3]
            except Exception:
                pass

            # Relative time
            try:
                rel_time = r[1]
            except Exception:
                pass

            if rating and rating > 0:
                reviews.append({
                    "author":   author or "Anonymous",
                    "rating":   rating,
                    "text":     text   or "",
                    "time":     rel_time or "",
                })
        except Exception:
            continue

    return reviews


# ══════════════════════════════════════════════════════════════════════════════
#  TIME HELPERS  (unchanged from v2)
# ══════════════════════════════════════════════════════════════════════════════

def ist_now() -> datetime:
    return datetime.utcnow() + timedelta(hours=5, minutes=30)


def is_within_23_hours(relative_time: str) -> bool:
    if not relative_time:
        return False
    t = relative_time.lower().strip()
    if any(w in t for w in ["just now", "minute", "moment", "hour"]):
        return True
    if any(w in t for w in ["day", "week", "month", "year"]):
        return False
    m = re.search(r"(\d+)\s*hour", t)
    if m:
        return int(m.group(1)) <= 23
    return False


def parse_relative_time(rel: str, ref: datetime | None = None) -> str | None:
    if not rel:
        return None
    if ref is None:
        ref = ist_now()
    t = rel.lower().strip()
    if any(w in t for w in ["just now", "minute", "moment"]):
        return ref.strftime("%Y-%m-%d %H:%M:%S")
    if "yesterday" in t:
        return (ref - timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")
    patterns = [
        (r"(\d+)\s*hour",  timedelta(hours=1)),
        (r"(\d+)\s*day",   timedelta(days=1)),
        (r"(\d+)\s*week",  timedelta(weeks=1)),
        (r"(\d+)\s*month", timedelta(days=30)),
        (r"(\d+)\s*year",  timedelta(days=365)),
    ]
    for pat, unit in patterns:
        m = re.search(pat, t)
        if m:
            return (ref - timedelta(seconds=unit.total_seconds() * int(m.group(1)))).strftime("%Y-%m-%d %H:%M:%S")
    return ref.strftime("%Y-%m-%d %H:%M:%S")


def make_fingerprint(branch_id: int, author: str, text: str, rating: float) -> str:
    raw = f"{branch_id}|{(author or '').strip().lower()}|{(text or '')[:120].strip().lower()}|{round(rating,1)}"
    return hashlib.sha1(raw.encode()).hexdigest()[:16]


# ══════════════════════════════════════════════════════════════════════════════
#  JSON I/O
# ══════════════════════════════════════════════════════════════════════════════

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


# ══════════════════════════════════════════════════════════════════════════════
#  SCRAPE ONE BRANCH  (pure HTTP, no browser)
# ══════════════════════════════════════════════════════════════════════════════

def scrape_branch(
    session:  requests.Session,
    branch:   dict,
    now:      datetime,
) -> list[dict]:

    bid       = branch["id"]
    name      = branch["name"]
    snap_date = now.strftime("%Y-%m-%d")
    snap_time = now.strftime("%H:%M IST")

    # Decode place_id → fid1, fid2
    try:
        fid1, fid2 = place_id_to_fids(branch["place_id"])
    except Exception as e:
        print(f"  [{bid:02d}/36] {name:<24} ✗  place_id decode failed: {e}")
        return []

    seen: set[str] = set()
    out:  list[dict] = []

    for page in range(MAX_PAGES):
        offset = page * REVIEWS_PER_PAGE
        url    = build_reviews_url(fid1, fid2, offset=offset)

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                resp = session.get(url, headers=HEADERS, timeout=15)
                resp.raise_for_status()
                break
            except Exception as e:
                if attempt == MAX_RETRIES:
                    print(f"  [{bid:02d}/36] {name:<24} ✗  HTTP error p{page}: {e}")
                    return out
                time.sleep(random.uniform(3, 6))

        raw_reviews = parse_reviews_response(resp.text)

        if not raw_reviews:
            break   # No more reviews on this page

        new_on_page = 0
        for r in raw_reviews:
            if not is_within_23_hours(r["time"]):
                continue  # Only keep ≤23 h reviews

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
                "time":        r["time"],
                "parsed_date": parse_relative_time(r["time"], now),
                "snap_date":   snap_date,
                "snap_time":   snap_time,
                "first_seen":  f"{snap_date} {snap_time}",
            })
            new_on_page += 1

        # If ALL reviews on this page are older than 23 h, stop paginating
        if new_on_page == 0 and page > 0:
            break

        time.sleep(random.uniform(*REQUEST_PAUSE))

    status = f"✓  {len(out):2d} new/recent reviews"
    print(f"  [{bid:02d}/36] {name:<24} {status}")
    return out


# ══════════════════════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════════════════════

def run():
    now       = ist_now()
    run_label = now.strftime("%Y-%m-%d %H:%M IST")

    print("=" * 90)
    print(f"  Sathya Reviews Scraper v3 — Internal API + No Browser — {run_label}")
    print("=" * 90)

    print("\n[1/4] Loading existing data...")
    live_map = {r["fingerprint"]: r for r in load_json(REV_JSON) if "fingerprint" in r}
    del_map  = {r["fingerprint"]: r for r in load_json(DEL_JSON) if "fingerprint" in r}
    print(f"  Live: {len(live_map)} | Deleted: {len(del_map)}")

    print(f"\n[2/4] Scraping {len(BRANCHES)} branches (HTTP only, sorted by Newest)...")
    t0 = time.time()

    all_reviews: list[dict] = []
    ok_bids:     set[int]   = set()

    session = requests.Session()

    for branch in BRANCHES:
        reviews = []
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                reviews = scrape_branch(session, branch, now)
                ok_bids.add(branch["id"])
                break
            except Exception:
                if attempt == MAX_RETRIES:
                    print(f"  [{branch['id']:02d}] Failed after {MAX_RETRIES} attempts")
                else:
                    time.sleep(random.uniform(5, 10))

        all_reviews.extend(reviews)
        time.sleep(random.uniform(*BRANCH_PAUSE))

    elapsed = int(time.time() - t0)
    print(f"\n  Done in {elapsed}s — {len(all_reviews)} total recent reviews extracted")

    print("\n[3/4] Processing changes...")
    curr_map = {r["fingerprint"]: r for r in all_reviews}

    new_reviews    = [r for fp, r in curr_map.items() if fp not in live_map and fp not in del_map]
    reinstated     = [dict(r, reinstated_on=now.strftime("%Y-%m-%d")) for fp, r in curr_map.items() if fp in del_map]
    newly_deleted  = []

    for fp, old in live_map.items():
        if old.get("branch_id") in ok_bids and fp not in curr_map:
            d = dict(old, deleted_on=now.strftime("%Y-%m-%d"))
            newly_deleted.append(d)

    print(f"  🆕 New: {len(new_reviews)} | ♻️  Reinstated: {len(reinstated)} | 🗑  Deleted: {len(newly_deleted)}")

    print("\n[4/4] Saving JSON files...")
    updated_live = dict(live_map)
    for fp, r in curr_map.items():
        if fp in updated_live:
            updated_live[fp].update({
                "snap_date":   r["snap_date"],
                "snap_time":   r["snap_time"],
                "time":        r["time"],
                "parsed_date": r.get("parsed_date"),
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

    rev_list = sorted(
        updated_live.values(),
        key=lambda x: x.get("parsed_date") or x.get("first_seen", ""),
        reverse=True,
    )
    del_list = sorted(
        updated_del.values(),
        key=lambda x: x.get("deleted_on", ""),
        reverse=True,
    )

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
