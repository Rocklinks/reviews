"""
scraper.py — Sathya Agency review scraper. Single midnight run.
Usage: python scraper.py [--force]
"""
import sys, re, json, time, hashlib, argparse, datetime, subprocess
from pathlib import Path
from datetime import date, timedelta

# ── Config ───────────────────────────────────────────────────────────────────
BASE_DIR     = Path(__file__).parent
REV_FILE     = BASE_DIR / "rev.json"
DELETED_FILE = BASE_DIR / "deleted.json"
MAX_SCROLLS  = 2000
STALL_LIMIT  = 5
SCROLL_PX    = 2000
SCROLL_DELAY = 700
MAX_RUN)SECS = 170 * 60  # 170 min
MAX_BRANCH_SECS = 4 * 60  #4 min

BRAVE_PATHS = [
    "/usr/bin/brave-browser", "/usr/bin/brave",
    "/opt/brave.com/brave/brave",
    "/Applications/Brave Browser.app/Contents/MacOS/Brave Browser",
    r"C:\Program Files\BraveSoftware\Brave-Browser\Application\brave.exe",
]

from branches import BRANCHES, AGM_MAP

# ── Utils ────────────────────────────────────────────────────────────────────
def log(msg):
    print(f"[{datetime.datetime.now():%Y-%m-%d %H:%M:%S}] {msg}", flush=True)

def get_review_date():
    return (date.today() - timedelta(days=1)).isoformat()

def make_review_id(branch_id, author, text, stars):
    raw = f"{branch_id}||{author.split(chr(10))[0].strip()}||{text}||{stars}"
    return hashlib.sha1(raw.encode()).hexdigest()[:16]

def parse_relative_time(t):
    t = (t or "").strip().lower()
    if t in ("just now", "a moment ago", "now"): return True
    m = re.match(r"(\d+)\s*(minute|hour)s?\s*ago", t)
    if m:
        n, u = int(m.group(1)), m.group(2)
        return u == "minute" or (u == "hour" and n <= 23)
    m = re.match(r"(\d+)([mh])\s*ago", t)
    if m:
        n, u = int(m.group(1)), m.group(2)
        return u == "m" or (u == "h" and n <= 23)
    return False

def is_day_old(t):
    t = (t or "").strip().lower()
    return bool(re.match(r"(a|1)\s*day\s*ago", t) or
                re.match(r"2\s*days?\s*ago", t) or
                re.match(r"[12]d\s*ago", t))

def _load(path):
    if not path.exists(): return {}
    try:
        d = json.loads(path.read_text(encoding="utf-8"))
        return d if isinstance(d, dict) else {}
    except Exception: return {}

def _save(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

def load_reviews():  return _load(REV_FILE)
def save_reviews(d): _save(REV_FILE, d)
def load_deleted():  return _load(DELETED_FILE)
def save_deleted(d): _save(DELETED_FILE, d)

def add_reviews(existing, new_reviews):
    added = 0
    for r in new_reviews:
        if r["review_id"] not in existing:
            existing[r["review_id"]] = r; added += 1
    return existing, added

def reactivate_reviews(scraped_ids, rev_data):
    deleted = load_deleted()
    restored, remove = {}, []
    for rid, rev in deleted.items():
        if rid in scraped_ids and rid not in rev_data:
            clean = {k: v for k, v in rev.items() if k != "detected_deleted_on"}
            clean.update(reactivated_on=date.today().isoformat(),
                         date=date.today().isoformat())
            restored[rid] = clean; remove.append(rid)
    if restored:
        rev_data.update(restored)
        for rid in remove: del deleted[rid]
        save_deleted(deleted)
    return len(restored)

def check_deletions(branch_id, day_old_ids, rev_data):
    if not day_old_ids: return []
    yesterday = (date.today() - timedelta(days=1)).isoformat()
    today     = date.today().isoformat()
    return [
        {**rev, "detected_deleted_on": today}
        for rid, rev in rev_data.items()
        if rev.get("branch_id") == branch_id
        and rev.get("date") == yesterday
        and rid not in day_old_ids
    ]

def move_to_deleted(deleted_revs, rev_data):
    if not deleted_revs: return 0
    existing = load_deleted(); moved = 0
    for rev in deleted_revs:
        rid = rev["review_id"]
        if rid not in existing: existing[rid] = rev; moved += 1
        rev_data.pop(rid, None)
    if moved: save_deleted(existing)
    return moved

def needs_migration():
    data = load_reviews()
    seen = set()
    for rev in data.values():
        try:
            nid = make_review_id(rev["branch_id"], rev["author"],
                                 rev["text"], rev["stars"])
        except Exception: continue
        if rev.get("review_id") != nid or nid in seen: return True
        seen.add(nid)
    return False

def run_migration():
    script = BASE_DIR / "migrate_clean.py"
    if script.exists():
        subprocess.run([sys.executable, str(script)])

def maps_url(pid): return f"https://www.google.com/maps/place/?q=place_id:{pid}"
def _norm(s): return re.sub(r"[\s.,!?…]+$", "", re.sub(r"\s+", " ", s.lower().strip()))

# ── Scraper ──────────────────────────────────────────────────────────────────
TIME_SELS = ['span.XfOne','div[class*="DUxS3d"]','.rsqaWe',
             'span[aria-label*="ago"]','span[aria-label*="now"]']
CARD_SELS = ['div[data-review-id]','div[class*="MyEned"]',
             'div[jslog*="review"]','div[jscontroller][class*="review"]']

def _card_time(card):
    for s in TIME_SELS:
        try:
            el = card.locator(s).first
            if el.count(): return el.inner_text(timeout=1000).strip()
        except Exception: pass
    return ""

def _card_count(page):
    n = 0
    for s in CARD_SELS:
        try: n = max(n, len(page.locator(s).all()))
        except Exception: pass
    return n

def _open(page, pid):
    page.goto(maps_url(pid), wait_until="domcontentloaded", timeout=30000)
    page.wait_for_timeout(3000)
    for sel in ['button[aria-label*="Reviews"]','[data-tab-index="1"]']:
        try:
            t = page.locator(sel).first
            if t.is_visible(timeout=3000): t.click(); page.wait_for_timeout(2000); break
        except Exception: pass
    try:
        b = page.locator('button[aria-label*="Sort"],[data-value="Sort"]').first
        if b.is_visible(timeout=3000):
            b.click(); page.wait_for_timeout(800)
            o = page.locator('li[aria-label*="Newest"],[data-index="1"]').first
            if o.is_visible(timeout=3000): o.click(); page.wait_for_timeout(2000)
    except Exception: pass

def _all_cards(page):
    cards, seen = [], set()
    for s in CARD_SELS:
        try:
            for c in page.locator(s).all():
                try:
                    h = c.evaluate("el=>el.dataset.reviewId||el.outerHTML.substring(0,200)")
                    if h not in seen: seen.add(h); cards.append(c)
                except Exception: pass
        except Exception: pass
    return cards

def _parse(card, page, bid, name, pid, rdate, agm, snap):
    try:
        rt = _card_time(card)
        if not rt: return None
        fresh, old = parse_relative_time(rt), is_day_old(rt)
        if not fresh and not old: return None

        try: author = card.locator('div[class*="d4r55"],.WNxzHc button,a.al6Kxe').first.inner_text(timeout=2000).strip()
        except Exception: author = "Anonymous"

        stars = 0
        for ss in ['span[aria-label*="star"]','span[aria-label*="Star"]','div[aria-label*="star"]']:
            try:
                lbl = card.locator(ss).first.get_attribute("aria-label", timeout=1000) or ""
                d = "".join(filter(str.isdigit, lbl.split("star")[0][-2:]))
                if d: stars = min(int(d), 5); break
            except Exception: pass

        try:
            mb = card.locator('button[aria-label*="See more"],button.w8nwRe').first
            if mb.is_visible(timeout=500): mb.click(); page.wait_for_timeout(400)
        except Exception: pass
        try: text = card.locator('span[class*="wiI7pd"],.MyEned span').first.inner_text(timeout=2000).strip()
        except Exception: text = ""

        if not stars and not text: return None

        rid = make_review_id(bid, author, text, stars)
        ac, tn = author.split("\n")[0].strip().lower(), _norm(text)
        dup = next((r for r in snap.values()
                    if r.get("branch_id") == bid
                    and r.get("author","").split("\n")[0].strip().lower() == ac
                    and _norm(r.get("text","")) == tn), None)
        if dup: rid = dup["review_id"]

        return {"review_id": rid, "_fp": (ac, bid, tn, rt), "_old": old,
                "branch_id": bid, "branch_name": name, "place_id": pid,
                "agm": agm, "author": author, "stars": stars,
                "relative_time": rt, "text": text, "date": rdate,
                "scraped_at": datetime.datetime.now().isoformat(),
                "method": "playwright"}
    except Exception: return None

def scrape_branch(page, bid, name, pid, rdate, snap):
    log(f"  → {name}")
    try: _open(page, pid)
    except Exception as e: log(f"  ERROR {name}: {e}"); return [], set()

    try: panel = page.locator('div[aria-label*="Reviews"]').first
    except Exception: panel = None
    
    branch_start = time.time()
    prev_pos = prev_n = stall = -1, 0, 0
    for i in range(MAX_SCROLLS):
        if panel:
            try: panel.evaluate(f"el=>el.scrollTop+={SCROLL_PX}")
            except Exception: page.keyboard.press("End")
        else:
            page.keyboard.press("End")
        page.wait_for_timeout(SCROLL_DELAY)

        try: cp = panel.evaluate("el=>el.scrollTop") if panel else 0
        except Exception: cp = 0
        cn = _card_count(page)

        if cp > 0 and cp == prev_pos and cn == prev_n:
            stall += 1
            if stall >= STALL_LIMIT: log(f"  {name}: stall@{i+1}"); break
        else: stall = 0
        prev_pos, prev_n = cp, cn

    fresh, day_old_ids, seen = [], set(), set()
    for c in _all_cards(page):
        r = _parse(c, page, bid, name, pid, rdate, AGM_MAP.get(name,"Unknown"), snap)
        if not r or r["_fp"] in seen: continue
        seen.add(r["_fp"])
        if r["_old"]: day_old_ids.add(r["review_id"])
        if parse_relative_time(r["relative_time"]):
            fresh.append({k: v for k, v in r.items() if not k.startswith("_")})

    log(f"  {name}: {len(fresh)} fresh, {len(day_old_ids)} day-old")
    return fresh, day_old_ids

def run():
    try: from playwright.sync_api import sync_playwright
    except ImportError: log("playwright not installed"); return []

    rdate = get_review_date()
    log(f"[scraper] midnight run. review_date={rdate}")

    brave = next((p for p in BRAVE_PATHS if Path(p).exists()), None)
    existing = load_reviews()
    snap = dict(existing)
    all_new, total_added, total_del, total_react = [], 0, 0, 0
    run_start = time.time()
    with sync_playwright() as pw:
        kw = {"headless": True, "args": ["--no-sandbox",
              "--disable-blink-features=AutomationControlled",
              "--disable-dev-shm-usage"]}
        if brave: kw["executable_path"] = brave
        browser = pw.chromium.launch(**kw)
        ctx = browser.new_context(
            viewport={"width": 1280, "height": 900},
            user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            locale="en-US")
        pages = [ctx.new_page() for _ in range(3)]

        for i in range(0, len(BRANCHES), 3):
            for ti, (bid, name, pid) in enumerate(BRANCHES[i:i+3]):
                fresh, day_old_ids = scrape_branch(
                    pages[ti], bid, name, pid, rdate, snap)
                all_ids = {r["review_id"] for r in fresh} | day_old_ids

                nr = reactivate_reviews(all_ids, existing)
                total_react += nr

                existing, added = add_reviews(existing, fresh)
                total_added += added; all_new.extend(fresh)

                deleted = check_deletions(bid, day_old_ids, existing)
                nd = move_to_deleted(deleted, existing)
                total_del += nd
                if nr: log(f"  {name}: {nr} reactivated")
                if nd: log(f"  {name}: {nd} → deleted.json")
            time.sleep(1)

        browser.close()

    save_reviews(existing)
    log(f"[scraper] done. scraped={len(all_new)} added={total_added} "
        f"deleted={total_del} reactivated={total_react}")
    return all_new

# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--force", action="store_true")
    args = p.parse_args()

    ist_hour = (datetime.datetime.now(datetime.timezone.utc)
                + datetime.timedelta(hours=5, minutes=30)).hour
    if not args.force and ist_hour != 0:
        log(f"Not midnight (hour={ist_hour}). Use --force to override.")
        sys.exit(0)

    if needs_migration(): run_migration()
    results = run()
    sys.exit(0 if results is not None else 1)
