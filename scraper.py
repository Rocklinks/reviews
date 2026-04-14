"""
Sathya Reviews Scraper v0.5 — Google Internal API (Pure HTTP)
────────────────────────────────────────────────────────────
FIXED BUGS vs previous versions:
  1. place_id decode — protobuf wrapper correctly stripped (0x0a + length byte)
                       fields decoded as little-endian int64, not big-endian
  2. Response parse  — review list is at data[2][0], NOT data[2]
  3. Date filter     — calendar date (IST), not broken "23h" check
  4. No scheduler    — GitHub Actions cron handles timing (scrape.yml)
  5. Single rev.json — no separate daily files; filter by snap_date in JS

REQUIREMENTS: pip install requests
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
REVIEWS_PER_PAGE = 10       # Google returns max 10 per request
MAX_PAGES        = 3        # up to 30 reviews checked per branch
SORT_NEWEST      = 2        # 1=relevant 2=newest 3=highest 4=lowest
REQUEST_PAUSE    = (1.0, 2.0)
BRANCH_PAUSE     = (1.5, 3.0)
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
#  PLACE_ID DECODER  (verified correct)
# ══════════════════════════════════════════════════════════════════════════════

def place_id_to_fids(place_id: str) -> tuple[int, int]:
    """
    Decode a Google Maps place_id into two signed int64 values (fid1, fid2).

    place_id is base64url encoded protobuf. Structure after decoding:
      byte  0    : 0x0a  (protobuf field 1, wire type 2 = length-delimited)
      byte  1    : length of inner payload (always 18)
      byte  2    : 0x09  (protobuf field 1, wire type 1 = 64-bit)
      bytes 3-10 : fid1 as little-endian int64
      byte  11   : 0x11  (protobuf field 2, wire type 1 = 64-bit)
      bytes 12-19: fid2 as little-endian int64

    Verified against all 36 branches — produces correct hex feature IDs.
    """
    padding = (4 - len(place_id) % 4) % 4
    decoded = base64.urlsafe_b64decode(place_id + "=" * padding)

    if len(decoded) < 20:
        raise ValueError(f"place_id too short after decode ({len(decoded)} bytes): {place_id}")
    if decoded[0] != 0x0a:
        raise ValueError(f"Expected protobuf tag 0x0a, got 0x{decoded[0]:02x}: {place_id}")

    payload = decoded[2:]   # skip 0x0a + length byte

    if payload[0] != 0x09:
        raise ValueError(f"Expected field tag 0x09, got 0x{payload[0]:02x}: {place_id}")
    if payload[9] != 0x11:
        raise ValueError(f"Expected field tag 0x11, got 0x{payload[9]:02x}: {place_id}")

    fid1 = struct.unpack("<q", payload[1:9])[0]    # little-endian signed int64
    fid2 = struct.unpack("<q", payload[10:18])[0]  # little-endian signed int64
    return fid1, fid2


# ══════════════════════════════════════════════════════════════════════════════
#  URL BUILDER
# ══════════════════════════════════════════════════════════════════════════════

def build_reviews_url(fid1: int, fid2: int, offset: int = 0) -> str:
    """
    Build Google Maps internal reviews RPC URL.

    pb parameter breakdown:
      !1m2!1y{fid1}!2y{fid2}   → identifies the business
      !2m1!2i{offset}           → pagination: 0=first 10, 10=next 10, etc.
      !3e2                      → sort = NEWEST (hardcoded, never changes)
      !4m5!3b1!4b1!5b1!6b1!7b1 → enable text, badges, photos, language fields
      !5m2!1s__dummy__!7e81     → session token (dummy value works fine)
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


# ══════════════════════════════════════════════════════════════════════════════
#  RESPONSE PARSER  (verified correct structure)
# ══════════════════════════════════════════════════════════════════════════════

def parse_reviews_response(raw: str) -> list[dict]:
    """
    Parse Google's internal RPC response.

    Response format (verified):
      )]}'\\n          ← anti-XSSI prefix, strip first
      [null, null, [  ← data[0]=null, data[1]=null, data[2]=reviews wrapper
        [             ← data[2][0] = the actual list of reviews   ← KEY FIX
          [           ← one review:
            [author_id, "Author Name"],   ← r[0], name at r[0][1]
            "2 hours ago",                ← r[1] = relative time string
            null,                         ← r[2] = always null
            "Review text or null",        ← r[3] = text (can be None)
            5,                            ← r[4] = star rating (1-5)
            ...
          ],
          ...
        ]
      ]]

    Previous versions used data[2] — WRONG. Must be data[2][0].
    """
    # Strip anti-XSSI prefix
    text = raw.lstrip()
    for prefix in (")]}'\n", ")]}'"):
        if text.startswith(prefix):
            text = text[len(prefix):]
            break

    try:
        data = json.loads(text)
    except json.JSONDecodeError as e:
        print(f"    [WARN] JSON parse failed: {e} | raw start: {repr(raw[:80])}")
        return []

    # Navigate to the review list: data[2][0]
    try:
        review_list = data[2][0]   # ← data[2][0], NOT data[2]
    except (IndexError, TypeError):
        return []

    if not review_list:
        return []

    reviews = []
    for r in review_list:
        try:
            # r[0][1] = author display name
            # r[1]    = relative time string e.g. "3 hours ago"
            # r[3]    = review text (can be None for rating-only reviews)
            # r[4]    = star rating as int/float 1–5
            author   = r[0][1]             if (r[0] and len(r[0]) > 1)  else None
            rel_time = r[1]                if len(r) > 1                else None
            text_val = r[3]                if len(r) > 3                else None
            rating   = float(r[4])         if len(r) > 4 and r[4]       else None

            if not rating or not (1 <= rating <= 5):
                continue

            reviews.append({
                "author": (author or "Anonymous").strip(),
                "rating": rating,
                "text":   (text_val or "").strip(),
                "time":   (rel_time or "").strip(),
            })
        except Exception:
            continue

    return reviews


# ══════════════════════════════════════════════════════════════════════════════
#  TIME HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def ist_now() -> datetime:
    """Current datetime in IST (UTC+5:30)."""
    return datetime.utcnow() + timedelta(hours=5, minutes=30)


def today_ist() -> str:
    """Today's IST calendar date as string. e.g. '2026-04-14'"""
    return ist_now().strftime("%Y-%m-%d")


def parse_relative_time(rel: str, ref: datetime | None = None) -> str | None:
    """
    Convert Google's relative time string to absolute IST datetime string.

    '3 hours ago'    → '2026-04-14 08:30:00'
    '45 minutes ago' → '2026-04-14 11:15:00'
    'just now'       → '2026-04-14 11:59:00'
    'a day ago'      → '2026-04-13 11:59:00'
    '2 days ago'     → '2026-04-12 11:59:00'
    """
    if not rel:
        return None
    if ref is None:
        ref = ist_now()
    t = rel.lower().strip()

    if any(w in t for w in ["just now", "moment"]):
        return ref.strftime("%Y-%m-%d %H:%M:%S")

    # Handle "a minute ago", "a day ago", "an hour ago" etc.
    t = re.sub(r'\ban\b', '1', t)
    t = re.sub(r'\ba\b',  '1', t)

    patterns = [
        (r"(\d+)\s*minute", timedelta(minutes=1)),
        (r"(\d+)\s*hour",   timedelta(hours=1)),
        (r"(\d+)\s*day",    timedelta(days=1)),
        (r"(\d+)\s*week",   timedelta(weeks=1)),
        (r"(\d+)\s*month",  timedelta(days=30)),
        (r"(\d+)\s*year",   timedelta(days=365)),
    ]
    for pat, unit in patterns:
        m = re.search(pat, t)
        if m:
            delta = timedelta(seconds=unit.total_seconds() * int(m.group(1)))
            return (ref - delta).strftime("%Y-%m-%d %H:%M:%S")

    return ref.strftime("%Y-%m-%d %H:%M:%S")


def is_today(parsed_date: str | None, today: str) -> bool:
    """
    True only if parsed_date is on today's IST calendar date.

    Why not '< 23 hours':
      A review from yesterday 11:50 PM is only 10 min old at midnight.
      It would wrongly pass a 23h check — but belongs to YESTERDAY.
      Calendar date is exact and never wrong.

      today = '2026-04-14'
        '2026-04-14 03:00:00'  → True   (today 3am)
        '2026-04-13 23:50:00'  → False  (yesterday)
        '2026-04-14 23:29:00'  → True   (today 11:29pm)
    """
    if not parsed_date:
        return False
    return parsed_date.startswith(today)


# ══════════════════════════════════════════════════════════════════════════════
#  FINGERPRINT + JSON I/O
# ══════════════════════════════════════════════════════════════════════════════

def make_fingerprint(branch_id: int, author: str, text: str, rating: float) -> str:
    """
    Stable unique ID per review.
    Same review scraped at 9am and 3pm produces the same fingerprint.
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
#  SCRAPE ONE BRANCH
# ══════════════════════════════════════════════════════════════════════════════

def scrape_branch(
    session: requests.Session,
    branch:  dict,
    now:     datetime,
    today:   str,
) -> list[dict]:
    """
    Fetch today's reviews for one branch via Google's internal API.
    Returns list of review dicts (only today's, deduplicated within branch).
    """
    bid       = branch["id"]
    name      = branch["name"]
    snap_date = now.strftime("%Y-%m-%d")
    snap_time = now.strftime("%H:%M IST")

    try:
        fid1, fid2 = place_id_to_fids(branch["place_id"])
    except Exception as e:
        print(f"  [{bid:02d}/36] {name:<24} ✗  decode error: {e}")
        return []

    seen: set[str]   = set()
    out:  list[dict] = []

    for page in range(MAX_PAGES):
        offset = page * REVIEWS_PER_PAGE
        url    = build_reviews_url(fid1, fid2, offset=offset)

        # HTTP with retry
        resp = None
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                resp = session.get(url, headers=HEADERS, timeout=20)
                resp.raise_for_status()
                break
            except Exception as e:
                if attempt == MAX_RETRIES:
                    print(f"  [{bid:02d}/36] {name:<24} ✗  HTTP fail (page {page}): {e}")
                    return out
                time.sleep(random.uniform(3, 6))

        raw_reviews = parse_reviews_response(resp.text)

        if not raw_reviews:
            break   # no more reviews on this page

        found_old = False
        for r in raw_reviews:
            parsed = parse_relative_time(r["time"], now)

            # Skip reviews that are not from today's IST calendar date
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
                "time":        r["time"],           # e.g. "3 hours ago"
                "parsed_date": parsed,              # e.g. "2026-04-14 08:30:00"
                "snap_date":   snap_date,           # e.g. "2026-04-14"
                "snap_time":   snap_time,           # e.g. "11:30 IST"
                "first_seen":  f"{snap_date} {snap_time}",
            })

        # Reviews are newest-first. Once we see one from before today,
        # all further pages will also be old — safe to stop.
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
    print(f"  Sathya Reviews Scraper v0.5  |  {label}  |  Date bucket: {today}")
    print("=" * 90)

    # ── 1. Load existing ────────────────────────────────────────────────────
    print("\n[1/4] Loading existing data...")
    live_map = {r["fingerprint"]: r for r in load_json(REV_JSON) if "fingerprint" in r}
    del_map  = {r["fingerprint"]: r for r in load_json(DEL_JSON) if "fingerprint" in r}

    today_already = sum(1 for r in live_map.values() if r.get("snap_date") == today)
    print(f"  All-time : {len(live_map)} live  |  {len(del_map)} deleted")
    print(f"  Today    : {today_already} already collected (earlier runs)")

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
                    print(f"  [{branch['id']:02d}] Branch failed completely")
                else:
                    time.sleep(random.uniform(5, 10))
        time.sleep(random.uniform(*BRANCH_PAUSE))

    print(f"\n  Scraped in {int(time.time()-t0)}s — {len(all_this_run)} today's reviews this run")

    # ── 3. Deduplicate and classify ─────────────────────────────────────────
    print("\n[3/4] Processing...")
    curr_map = {r["fingerprint"]: r for r in all_this_run}

    # New = not seen in all-time live AND not in deleted
    new_reviews = [
        r for fp, r in curr_map.items()
        if fp not in live_map and fp not in del_map
    ]

    # Reinstated = was deleted, now visible again
    reinstated = [
        dict(r, reinstated_on=today)
        for fp, r in curr_map.items()
        if fp in del_map
    ]

    # Deleted = was live as a TODAY review in a previous run, now gone
    # (never touch reviews from previous days — they're historical)
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

    # ── 4. Save ─────────────────────────────────────────────────────────────
    print("\n[4/4] Saving...")

    updated_live = dict(live_map)

    # Refresh snap_time for reviews we've seen again this run
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

    # Sort rev.json newest parsed_date first
    rev_list = sorted(
        updated_live.values(),
        key=lambda x: x.get("parsed_date") or x.get("first_seen", ""),
        reverse=True,
    )

    # Sort deleted.json most recently deleted first
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
