"""
Sathya Reviews Scraper v1 — Google Internal API
────────────────────────────────────────────────
• Hits Google's internal reviews RPC endpoint directly (sorted Newest always)
• Filters to TODAY's reviews only (IST calendar date)
• Deduplicates by fingerprint — safe to run multiple times per day
• All reviews in one rev.json, each with snap_date field

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
REVIEWS_PER_PAGE = 10      # Google returns max 10 per request
MAX_PAGES        = 3       # fetch up to 3 pages = 30 reviews per branch
SORT_NEWEST      = 2       # 1=relevant  2=newest  3=highest  4=lowest
REQUEST_PAUSE    = (1.0, 2.0)   # seconds between page requests per branch
BRANCH_PAUSE     = (1.5, 3.0)  # seconds between branches
MAX_RETRIES      = 3

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
#  TIME HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def ist_now() -> datetime:
    """Current datetime in IST (UTC+5:30)."""
    return datetime.utcnow() + timedelta(hours=5, minutes=30)


def today_ist() -> str:
    """Today's IST date string, e.g. '2026-04-14'."""
    return ist_now().strftime("%Y-%m-%d")


def parse_relative_time(rel: str, ref: datetime | None = None) -> str | None:
    """
    Convert Google's relative time string to an absolute IST datetime string.

    Examples:
      '3 hours ago'    → '2026-04-14 08:30:00'
      '45 minutes ago' → '2026-04-14 11:15:00'
      'just now'       → '2026-04-14 11:59:00'
      'a day ago'      → '2026-04-13 11:59:00'
    """
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
            delta = timedelta(seconds=unit.total_seconds() * int(m.group(1)))
            return (ref - delta).strftime("%Y-%m-%d %H:%M:%S")

    return ref.strftime("%Y-%m-%d %H:%M:%S")


def is_today(parsed_date: str | None, today: str) -> bool:
    """
    Return True ONLY if parsed_date falls on today's IST calendar date.

    Why calendar date and not '< 23 hours':
      A review posted yesterday at 11:50 PM is only 10 min old at midnight,
      so it would wrongly pass a 23h check — but it belongs to YESTERDAY.
      Checking startswith(today) is exact and never wrong.

      today='2026-04-14'
        '2026-04-14 03:15:00' → True   ✅
        '2026-04-13 23:50:00' → False  ❌  (yesterday)
        '2026-04-14 23:29:00' → True   ✅
    """
    if not parsed_date:
        return False
    return parsed_date.startswith(today)


# ══════════════════════════════════════════════════════════════════════════════
#  FINGERPRINT + JSON I/O
# ══════════════════════════════════════════════════════════════════════════════

def make_fingerprint(branch_id: int, author: str, text: str, rating: float) -> str:
    """
    Stable unique ID for a review. Same review scraped 10× = same fingerprint.
    Based on branch + author + first 120 chars of text + rating.
    """
    raw = (
        f"{branch_id}|"
        f"{(author or '').strip().lower()}|"
        f"{(text   or '')[:120].strip().lower()}|"
        f"{round(rating, 1)}"
    )
    return hashlib.sha1(raw.encode()).hexdigest()[:16]


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
#  GOOGLE INTERNAL API
# ══════════════════════════════════════════════════════════════════════════════

def place_id_to_fids(place_id: str) -> tuple[int, int]:
    """
    Decode a Google Maps place_id (base64url) into two signed int64 values.

    Google's place_id is base64url-encoded bytes. Decoded = 16 bytes.
    Split into two 8-byte chunks → each big-endian signed int64.
    These (fid1, fid2) identify the business in the reviews RPC.

    e.g. 'ChIJ5zJNoJfvAzsR-bJE_3bbNYw'
          → bytes → fid1=-5756223995023699225, fid2=-8341338698765386119
    """
    padding = (4 - len(place_id) % 4) % 4
    decoded = base64.urlsafe_b64decode(place_id + "=" * padding)
    if len(decoded) < 16:
        raise ValueError(f"place_id decoded to only {len(decoded)} bytes: {place_id}")
    fid1 = struct.unpack(">q", decoded[0:8])[0]
    fid2 = struct.unpack(">q", decoded[8:16])[0]
    return fid1, fid2


def build_reviews_url(fid1: int, fid2: int, offset: int = 0) -> str:
    """
    Build Google Maps internal reviews RPC URL.

    Endpoint : /maps/preview/review/listentitiesreviews
    pb param breakdown:
      !1m2!1y{fid1}!2y{fid2}   → which business
      !2m1!2i{offset}           → pagination (0, 10, 20 …)
      !3e2                      → sort = NEWEST (hardcoded)
      !4m5!3b1!4b1!5b1!6b1!7b1 → enable text, photos, badges, language
      !5m2!1s__dummy__!7e81     → session token (dummy is fine)
    """
    pb = (
        f"!1m2!1y{fid1}!2y{fid2}"
        f"!2m1!2i{offset}"
        f"!3e{SORT_NEWEST}"
        f"!4m5!3b1!4b1!5b1!6b1!7b1"
        f"!5m2!1s__dummy__!7e81"
    )
    return (
        "https://www.google.com/maps/preview/review/listentitiesreviews"
        f"?authuser=0&hl=en&gl=in"
        f"&pb={urllib.parse.quote(pb, safe='')}"
    )


def parse_reviews_response(raw: str) -> list[dict]:
    """
    Parse Google's RPC response into a list of raw review dicts.

    Response format:
      )]}'\n   ← anti-XSSI prefix, strip this first
      [...]    ← nested JSON array
        [2]    ← list of review arrays
          [0]  → [author_info, relative_time, ?, text, rating, ...]
    """
    text = raw.lstrip()
    for prefix in (")]}'\n", ")]}'"):
        if text.startswith(prefix):
            text = text[len(prefix):]
            break

    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        return []

    try:
        review_list = data[2]
    except (IndexError, TypeError):
        return []

    if not review_list:
        return []

    reviews = []
    for r in review_list:
        try:
            author   = r[0][1] if r[0] else None
            rel_time = r[1]        if len(r) > 1 else None
            text_val = r[3]        if len(r) > 3 else None
            rating   = float(r[4]) if len(r) > 4 and r[4] else None

            if rating and rating > 0:
                reviews.append({
                    "author": author   or "Anonymous",
                    "rating": rating,
                    "text":   text_val or "",
                    "time":   rel_time or "",   # "3 hours ago"
                })
        except Exception:
            continue

    return reviews


# ══════════════════════════════════════════════════════════════════════════════
#  SCRAPE ONE BRANCH
# ══════════════════════════════════════════════════════════════════════════════

def scrape_branch(
    session: requests.Session,
    branch:  dict,
    now:     datetime,
    today:   str,
) -> list[dict]:
    """
    Fetch today's reviews for one branch.
    Returns list of review dicts ready to merge into rev.json.
    """
    bid       = branch["id"]
    name      = branch["name"]
    snap_date = now.strftime("%Y-%m-%d")
    snap_time = now.strftime("%H:%M IST")

    try:
        fid1, fid2 = place_id_to_fids(branch["place_id"])
    except Exception as e:
        print(f"  [{bid:02d}/36] {name:<24} ✗  place_id error: {e}")
        return []

    seen: set[str]   = set()
    out:  list[dict] = []

    for page in range(MAX_PAGES):
        offset = page * REVIEWS_PER_PAGE
        url    = build_reviews_url(fid1, fid2, offset=offset)

        # HTTP request with retry
        resp = None
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                resp = session.get(url, headers=HEADERS, timeout=15)
                resp.raise_for_status()
                break
            except Exception as e:
                if attempt == MAX_RETRIES:
                    print(f"  [{bid:02d}/36] {name:<24} ✗  HTTP fail p{page}: {e}")
                    return out
                time.sleep(random.uniform(3, 6))

        raw_reviews = parse_reviews_response(resp.text)
        if not raw_reviews:
            break   # no more reviews on this page, stop paginating

        found_old = False
        for r in raw_reviews:
            parsed = parse_relative_time(r["time"], now)

            # Calendar-date filter: skip anything not from today
            if not is_today(parsed, today):
                found_old = True
                continue

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
                "time":        r["time"],           # "3 hours ago"
                "parsed_date": parsed,              # "2026-04-14 08:30:00"
                "snap_date":   snap_date,           # "2026-04-14"
                "snap_time":   snap_time,           # "11:30 IST"
                "first_seen":  f"{snap_date} {snap_time}",
            })

        # Reviews are newest-first. Once we hit one from before today,
        # all subsequent pages will also be old — no need to continue.
        if found_old:
            break

        time.sleep(random.uniform(*REQUEST_PAUSE))

    print(f"  [{bid:02d}/36] {name:<24} ✓  {len(out):2d} today's reviews")
    return out


# ══════════════════════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════════════════════

def run():
    now   = ist_now()
    today = today_ist()
    label = now.strftime("%Y-%m-%d %H:%M IST")

    print("=" * 90)
    print(f"  Sathya Reviews Scraper v1  |  {label}  |  Date: {today}")
    print("=" * 90)

    # ── 1. Load existing data ───────────────────────────────────────────────
    print("\n[1/4] Loading existing data...")
    live_map = {r["fingerprint"]: r for r in load_json(REV_JSON) if "fingerprint" in r}
    del_map  = {r["fingerprint"]: r for r in load_json(DEL_JSON) if "fingerprint" in r}

    today_already = sum(1 for r in live_map.values() if r.get("snap_date") == today)
    print(f"  All-time live : {len(live_map)}")
    print(f"  Deleted       : {len(del_map)}")
    print(f"  Today so far  : {today_already}  (from earlier runs today)")

    # ── 2. Scrape ───────────────────────────────────────────────────────────
    print(f"\n[2/4] Scraping {len(BRANCHES)} branches...")
    t0 = time.time()

    all_this_run: list[dict] = []
    ok_bids:      set[int]   = set()
    session = requests.Session()

    for branch in BRANCHES:
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                reviews = scrape_branch(session, branch, now, today)
                all_this_run.extend(reviews)
                ok_bids.add(branch["id"])
                break
            except Exception:
                if attempt == MAX_RETRIES:
                    print(f"  [{branch['id']:02d}] Failed after {MAX_RETRIES} attempts")
                else:
                    time.sleep(random.uniform(5, 10))
        time.sleep(random.uniform(*BRANCH_PAUSE))

    elapsed = int(time.time() - t0)
    print(f"\n  Scraped in {elapsed}s — {len(all_this_run)} today's reviews found this run")

    # ── 3. Deduplicate and classify ─────────────────────────────────────────
    print("\n[3/4] Processing...")
    curr_map = {r["fingerprint"]: r for r in all_this_run}

    # New: today's review not seen in all-time live or deleted
    new_reviews = [
        r for fp, r in curr_map.items()
        if fp not in live_map and fp not in del_map
    ]

    # Reinstated: was deleted, now visible again
    reinstated = [
        dict(r, reinstated_on=today)
        for fp, r in curr_map.items()
        if fp in del_map
    ]

    # Deleted: was in today's live set in a previous run, now gone
    # (only flag today's reviews as deleted — don't touch older history)
    newly_deleted = [
        dict(old, deleted_on=today)
        for fp, old in live_map.items()
        if old.get("branch_id") in ok_bids
        and fp not in curr_map
        and old.get("snap_date") == today
    ]

    print(f"  🆕 New        : {len(new_reviews)}")
    print(f"  ♻️  Reinstated : {len(reinstated)}")
    print(f"  🗑  Deleted    : {len(newly_deleted)}")

    # ── 4. Save ────────────────────────────────────────────────────────────
    print("\n[4/4] Saving...")

    # Update all-time live map
    updated_live = dict(live_map)

    # Refresh snap_time for reviews seen again this run
    for fp, r in curr_map.items():
        if fp in updated_live:
            updated_live[fp]["snap_time"] = r["snap_time"]
            updated_live[fp]["time"]      = r["time"]

    # Add new and reinstated
    for r in new_reviews + reinstated:
        updated_live[r["fingerprint"]] = r

    # Remove newly deleted from live
    for r in newly_deleted:
        updated_live.pop(r["fingerprint"], None)

    # Update deleted map
    updated_del = dict(del_map)
    for r in newly_deleted:
        updated_del[r["fingerprint"]] = r
    for r in reinstated:
        updated_del.pop(r["fingerprint"], None)

    # Sort rev.json: newest parsed_date first
    rev_list = sorted(
        updated_live.values(),
        key=lambda x: x.get("parsed_date") or x.get("first_seen", ""),
        reverse=True,
    )

    # Sort deleted.json: most recently deleted first
    del_list = sorted(
        updated_del.values(),
        key=lambda x: x.get("deleted_on", ""),
        reverse=True,
    )

    save_json(REV_JSON, rev_list)
    save_json(DEL_JSON, del_list)

    today_total = sum(1 for r in rev_list if r.get("snap_date") == today)

    print(f"  docs/rev.json     → {len(rev_list)} all-time  |  {today_total} for {today}")
    print(f"  docs/deleted.json → {len(del_list)}")
    print(f"\n  ✅ Done — {label}")
    print("=" * 90)


if __name__ == "__main__":
    try:
        run()
    except Exception as e:
        print(f"\n[FATAL] {e}")
        traceback.print_exc()
        exit(1)
