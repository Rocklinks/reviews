"""
scraper.py – Sathya Review Scraper

CONFIRMED DOM (from browser DevTools on live Maps page)
=======================================================
Reviews tab  : <span class="PbOY2e">Reviews</span>
               inside <div role="button" data-tab-index="1">
Sort dropdown: <div class="faMaId"> or <div class="AHwJHf">Sort by</div>
Sort Newest  : <div data-sort="1">   (inside radiogroup)
Review cards : div.jftiEf
Timestamp    : span.rsqaWe
Author       : span.d4r55
Stars        : span[role="img"][aria-label="X stars"]
Text         : span.wiI7pd
Scroll panel : div.m6QErb  (inner scrollable div, NOT window)

SAME DOM FOR ALL 36 BRANCHES — only place_id differs in URL.

RUN SLOTS (UTC → IST)
  00:30 UTC = 06:00 IST  → morning   snap_date = today
  06:30 UTC = 12:00 IST  → noon      snap_date = today
  12:30 UTC = 18:00 IST  → evening   snap_date = today
  18:30 UTC = 00:00 IST  → midnight  snap_date = YESTERDAY
"""

import asyncio, hashlib, json, random, re, traceback
from datetime import datetime, timedelta, timezone
from pathlib import Path
from playwright.async_api import async_playwright, TimeoutError as PWTimeout

# ── paths ──────────────────────────────────────────────────────────────────────
ROOT     = Path(__file__).parent
DOCS     = ROOT / "docs"
DBG_SS   = ROOT / "debug" / "ss"
DBG_DOM  = ROOT / "debug" / "dom"
REV_JSON = DOCS / "rev.json"
DEL_JSON = DOCS / "deleted.json"
for d in [DOCS, DBG_SS, DBG_DOM]: d.mkdir(parents=True, exist_ok=True)

# ── constants ──────────────────────────────────────────────────────────────────
MAX_CONCURRENT    = 2
MAX_RETRIES       = 2
MAX_SCROLL_ROUNDS = 40
STALE_LIMIT       = 4
DELETION_DAYS     = 30
IST = timezone(timedelta(hours=5, minutes=30))

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
]

CHROMIUM_ARGS = [
    "--no-sandbox", "--disable-setuid-sandbox",
    "--disable-blink-features=AutomationControlled",
    "--disable-dev-shm-usage", "--no-first-run",
    "--disable-extensions", "--disable-infobars",
    "--disable-default-apps", "--mute-audio",
    "--disable-gpu", "--disable-software-rasterizer",
    "--window-size=1400,900",
]

STEALTH_JS = """
Object.defineProperty(navigator,'webdriver',{get:()=>undefined});
Object.defineProperty(navigator,'plugins',  {get:()=>[1,2,3,4,5]});
Object.defineProperty(navigator,'languages',{get:()=>['en-US','en']});
window.chrome={runtime:{},loadTimes:()=>{},csi:()=>{}};
try{
  const o=window.navigator.permissions.query;
  window.navigator.permissions.query=p=>
    p.name==='notifications'?Promise.resolve({state:Notification.permission}):o(p);
}catch(e){}
"""

BRANCHES = [
    {"id": 1,  "name": "Tuticorin-1",     "place_id": "ChIJ5zJNoJfvAzsR-bJE_3bbNYw", "agm": "Siva"},
    {"id": 2,  "name": "Tuticorin-2",     "place_id": "ChIJH6gY4-PvAzsRJ50skTlx3cs", "agm": "Siva"},
    {"id": 3,  "name": "Thiruchendur-1",  "place_id": "ChIJeXA4vJKRAzsRBovAtv6lMuQ", "agm": "Siva"},
    {"id": 4,  "name": "Thisayanvilai-1", "place_id": "ChIJVWkvdfh_BDsRdvtimKCLS5Y", "agm": "Siva"},
    {"id": 5,  "name": "Eral-2",          "place_id": "ChIJbwAA0KGMAzsRkQilW5PceeA", "agm": "Siva"},
    {"id": 6,  "name": "Udankudi",        "place_id": "ChIJPQAAACyEAzsRgjznQ1GLom0", "agm": "Siva"},
    {"id": 7,  "name": "Tirunelveli-1",   "place_id": "ChIJ2RU2NvQRBDsRq-Fw7IVwx7k", "agm": "John"},
    {"id": 8,  "name": "Valliyur-1",      "place_id": "ChIJcVNk6TtnBDsRBoP4zpExt5k", "agm": "John"},
    {"id": 9,  "name": "Ambasamudram-1",  "place_id": "ChIJ9SGeIi85BDsRZk4QdyW9BSY", "agm": "John"},
    {"id": 10, "name": "Anjugramam-1",    "place_id": "ChIJ4yeJebLtBDsRDceoxujdGyc", "agm": "John"},
    {"id": 11, "name": "Nagercoil",       "place_id": "ChIJe1LZBiTxBDsRJFLjlbgZoIs", "agm": "Jeeva"},
    {"id": 12, "name": "Marthandam",      "place_id": "ChIJcWptCRdVBDsRlJh2q0-rnfY", "agm": "Jeeva"},
    {"id": 13, "name": "Thuckalay-1",     "place_id": "ChIJc9QgEub4BDsRoyDR4Wd6tYA", "agm": "Jeeva"},
    {"id": 14, "name": "Colachel-1",      "place_id": "ChIJgRkBLw39BDsR58D0lwNo5Ts", "agm": "Jeeva"},
    {"id": 15, "name": "Kulasekharam-1",  "place_id": "ChIJw0Ep-kNXBDsRe5ad32jAeAk", "agm": "Jeeva"},
    {"id": 16, "name": "Monday Market",   "place_id": "ChIJTceRGAD5BDsR65i3YNTcYHk", "agm": "Jeeva"},
    {"id": 17, "name": "Karungal-1",      "place_id": "ChIJfTP5ASr_BDsRgsBaeQltkw4", "agm": "Jeeva"},
    {"id": 18, "name": "Kovilpatti",      "place_id": "ChIJHY0o-26yBjsRt7wbXB1pDUE", "agm": "Seenivasan"},
    {"id": 19, "name": "Ramnad",          "place_id": "ChIJNVVVVaGiATsRnunSgOTvbE8", "agm": "Seenivasan"},
    {"id": 20, "name": "Paramakudi",      "place_id": "ChIJ-dgjBzQHATsRf27FWAJgmsA", "agm": "Seenivasan"},
    {"id": 21, "name": "Sayalkudi-1",     "place_id": "ChIJRTqudn9lATsR2fYyMmxlOrw", "agm": "Seenivasan"},
    {"id": 22, "name": "Villathikullam",  "place_id": "ChIJi_wAkwVbATsRtFl3_V5rGrY", "agm": "Seenivasan"},
    {"id": 23, "name": "Sattur-2",        "place_id": "ChIJNVVVVcHKBjsR7xMX97RFn8Q", "agm": "Seenivasan"},
    {"id": 24, "name": "Sankarankovil-1", "place_id": "ChIJE1mKnhSXBjsRKMQ-9JKQf_c", "agm": "Seenivasan"},
    {"id": 25, "name": "Kayathar-1",      "place_id": "ChIJx5ebtUgRBDsRMquPZNUJVpw", "agm": "Seenivasan"},
    {"id": 26, "name": "Thenkasi",        "place_id": "ChIJuaqqquEpBDsRVITw0MMYklc", "agm": "Muthuselvam"},
    {"id": 27, "name": "Thenkasi-2",      "place_id": "ChIJiwqLye6DBjsRo9v1mWXaycI", "agm": "Muthuselvam"},
    {"id": 28, "name": "Surandai-1",      "place_id": "ChIJPb1_eEOdBjsRjL9IVCVJhi8", "agm": "Muthuselvam"},
    {"id": 29, "name": "Puliyankudi-1",   "place_id": "ChIJjZqoc46RBjsRQTGHnNC8xxA", "agm": "Muthuselvam"},
    {"id": 30, "name": "Sengottai-1",     "place_id": "ChIJw3zzKiaBBjsR9KDyGpn1nXU", "agm": "Muthuselvam"},
    {"id": 31, "name": "Rajapalayam",     "place_id": "ChIJW2ot-NDpBjsRMTfMF2IV-xE", "agm": "Muthuselvam"},
    {"id": 32, "name": "Virudhunagar",    "place_id": "ChIJN3jzNJgsATsRCU3nrB5ntKE", "agm": "Venkatesh"},
    {"id": 33, "name": "Virudhunagar-2",  "place_id": "ChIJPezaX7wtATsR9sHhFOG6A1c", "agm": "Venkatesh"},
    {"id": 34, "name": "Aruppukottai",    "place_id": "ChIJy6qqqgYwATsRbcp-hXnoruM", "agm": "Venkatesh"},
    {"id": 35, "name": "Aruppukottai-2",  "place_id": "ChIJY04wY58xATsRuoJSichVQQE", "agm": "Venkatesh"},
    {"id": 36, "name": "Sivakasi",        "place_id": "ChIJI2JvEePOBjsREh8b-x4WF4U", "agm": "Venkatesh"},
]

# ══════════════════════════════════════════════════════════════════════════════
# Pure helpers
# ══════════════════════════════════════════════════════════════════════════════

def ist_now():  return datetime.now(IST)
def ist_today(): return ist_now().strftime("%Y-%m-%d")
def ist_yesterday(): return (ist_now()-timedelta(days=1)).strftime("%Y-%m-%d")
def slug(s): return re.sub(r"[^a-z0-9]+","_",s.lower()).strip("_")

def get_slot():
    h = datetime.now(timezone.utc).hour
    if h==0: return "morning"
    if h==6: return "noon"
    if h==12: return "evening"
    if h==18: return "midnight"
    ih = ist_now().hour
    if 5<=ih<11: return "morning"
    if 11<=ih<17: return "noon"
    if 17<=ih<23: return "evening"
    return "midnight"

def snap_date(slot): return ist_yesterday() if slot=="midnight" else ist_today()

def fp(rating, author, text):
    raw = f"{round(float(rating),1)}|{(author or '').lower()[:40]}|{(text or '').lower()[:200]}"
    return hashlib.sha256(raw.encode()).hexdigest()[:20]

def load_json(p):
    if p.exists():
        try: return json.loads(p.read_text(encoding="utf-8"))
        except: pass
    return []

def save_json(p, data):
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")

_23H = re.compile(
    r"^(?:just now|a moment ago|moments? ago)$"
    r"|^(\d+)\s*(second|minute|hour)s?\s*ago$", re.I)

def within_23h(rel):
    m = _23H.match((rel or "").strip())
    if not m: return False
    if not m.group(1): return True
    v,u = int(m.group(1)), m.group(2).lower()
    if u=="second": return True
    if u=="minute": return v<=1380
    if u=="hour":   return v<=23
    return False

def rel2abs(rel, ref):
    rel=(rel or "").lower().strip()
    if not rel or "just now" in rel or "moment" in rel:
        return ref.strftime("%Y-%m-%d %H:%M:%S")
    m=re.search(r"(\d+)\s*(second|minute|hour|day|week|month|year)",rel)
    if not m: return ref.strftime("%Y-%m-%d %H:%M:%S")
    v,u=int(m.group(1)),m.group(2)
    d={"second":timedelta(seconds=v),"minute":timedelta(minutes=v),
       "hour":timedelta(hours=v),"day":timedelta(days=v),
       "week":timedelta(weeks=v),"month":timedelta(days=30*v),
       "year":timedelta(days=365*v)}
    return (ref-d.get(u,timedelta())).strftime("%Y-%m-%d %H:%M:%S")

# ══════════════════════════════════════════════════════════════════════════════
# Debug helpers
# ══════════════════════════════════════════════════════════════════════════════

async def dbg(page, name, label):
    sl=slug(name); lb=slug(label)
    try: await page.screenshot(path=str(DBG_SS/f"{sl}_{lb}.png"), full_page=False)
    except: pass
    try:
        lines=[f"=== {name} | {label} ===", f"URL: {page.url}",
               f"Title: {await page.title()}", ""]
        for sel in ["div.jftiEf","span.PbOY2e","span.rsqaWe",
                    "div[data-sort]","div.m6QErb","div[role='main']"]:
            try:
                n=await page.locator(sel).count()
                if n: lines.append(f"  {n:4d}  {sel}")
            except: pass
        try:
            cards=await page.locator("div.jftiEf").all()
            for i,c in enumerate(cards[:3]):
                t=(await c.inner_text(timeout=500)).replace("\n"," ")[:200]
                lines.append(f"  CARD[{i}]: {t!r}")
        except: pass
        (DBG_DOM/f"{sl}_{lb}.txt").write_text("\n".join(lines),encoding="utf-8")
    except: pass

# ══════════════════════════════════════════════════════════════════════════════
# Browser context
# ══════════════════════════════════════════════════════════════════════════════

async def new_ctx(browser):
    ctx = await browser.new_context(
        viewport={"width":1400,"height":900},
        user_agent=random.choice(USER_AGENTS),
        locale="en-US", timezone_id="Asia/Kolkata",
        java_script_enabled=True,
        extra_http_headers={"Accept-Language":"en-US,en;q=0.9"},
    )
    await ctx.add_init_script(STEALTH_JS)
    return ctx

# ══════════════════════════════════════════════════════════════════════════════
# STEP 1 – Navigate
# Uses hl=en to force English UI (critical for span.PbOY2e text matching)
# ══════════════════════════════════════════════════════════════════════════════

async def navigate(page, place_id, name):
    url = f"https://www.google.com/maps/place/?q=place_id:{place_id}&hl=en"

    # Block images/fonts — keeps Maps JS/XHR intact, loads faster
    await page.route(
        re.compile(r"\.(png|jpg|jpeg|gif|webp|svg|ico|woff2?|ttf|mp4|mp3)(\?.*)?$"),
        lambda r: r.abort()
    )

    try:
        resp = await page.goto(url, wait_until="load", timeout=60_000)
        print(f"    [{name}] HTTP {resp.status if resp else '?'}", flush=True)
    except PWTimeout:
        try: await page.goto(url, wait_until="domcontentloaded", timeout=60_000)
        except Exception as e:
            print(f"    [{name}] nav failed: {e!s:.60s}", flush=True)
            return False

    # Handle Google consent page (common on datacenter IPs)
    await asyncio.sleep(1)
    if "consent.google" in page.url:
        print(f"    [{name}] Consent page — dismissing…", flush=True)
        for sel in ["#L2AGLb","button[aria-label*='Accept' i]",
                    "form[action*='consent'] button:last-child"]:
            try:
                b=page.locator(sel).first
                if await b.is_visible(timeout=1500):
                    await b.click(); await asyncio.sleep(2); break
            except: pass

    # Wait until Maps sidebar renders — span.PbOY2e is the Reviews tab span
    # This is the most reliable boot indicator for the confirmed DOM
    deadline = asyncio.get_event_loop().time() + 30
    while asyncio.get_event_loop().time() < deadline:
        for sel in ["span.PbOY2e","div[role='tablist']","div.jftiEf","span.F7nice"]:
            try:
                if await page.locator(sel).count()>0: return True
            except: pass
        await asyncio.sleep(0.6)

    print(f"    [{name}] Sidebar not found after 30s", flush=True)
    return False

# ══════════════════════════════════════════════════════════════════════════════
# STEP 2 – Click Reviews tab
# CONFIRMED: span.PbOY2e contains "Reviews" text
#            parent is div[role="button"][data-tab-index="1"]
# ══════════════════════════════════════════════════════════════════════════════

async def click_reviews_tab(page, name):
    # Already on reviews (e.g. came from !9m1!1b1 URL)
    if await page.locator("div.jftiEf").count() > 0:
        print(f"    [{name}] Reviews already visible", flush=True)
        return True

    await asyncio.sleep(random.uniform(1.5, 2.5))

    # S1 — CONFIRMED selector: span.PbOY2e with "Reviews" text
    try:
        result = await page.evaluate("""
            () => {
                const spans = document.querySelectorAll('span.PbOY2e');
                for (const s of spans) {
                    if (/review/i.test(s.innerText || '')) {
                        // Walk up to role=button parent and click it
                        let el = s;
                        for (let i=0; i<6; i++) {
                            if (!el.parentElement) break;
                            el = el.parentElement;
                            if (el.getAttribute('role') === 'button') {
                                el.click();
                                return 'role=button via PbOY2e';
                            }
                        }
                        s.click();
                        return 'span.PbOY2e direct';
                    }
                }
                return null;
            }
        """)
        if result:
            print(f"    [{name}] ✓ Tab: {result}", flush=True)
            await asyncio.sleep(random.uniform(3, 4))
            return True
    except: pass

    # S2 — data-tab-index="1" (Reviews is always index 1)
    try:
        loc = page.locator("div[role='button'][data-tab-index='1']").first
        if await loc.is_visible(timeout=2000):
            await loc.click()
            print(f"    [{name}] ✓ Tab: data-tab-index=1", flush=True)
            await asyncio.sleep(random.uniform(3, 4))
            return True
    except: pass

    # S3 — any role=button whose text starts with "Review"
    try:
        btns = await page.locator("div[role='button']").all()
        for b in btns:
            try:
                t = (await b.inner_text(timeout=300)).strip().lower()
                if t.startswith("review"):
                    await b.click()
                    print(f"    [{name}] ✓ Tab: role=button text={t!r}", flush=True)
                    await asyncio.sleep(random.uniform(3, 4))
                    return True
            except: continue
    except: pass

    print(f"    [{name}] ✗ Tab click failed — scraping anyway", flush=True)
    return False

# ══════════════════════════════════════════════════════════════════════════════
# STEP 3 – Sort by Newest
# CONFIRMED: div.faMaId / div.AHwJHf opens dropdown
#            div[data-sort="1"] = Newest
# ══════════════════════════════════════════════════════════════════════════════

async def sort_newest(page, name):
    await asyncio.sleep(random.uniform(1.5, 2.0))

    # If sort menu already open and data-sort="1" visible, click directly
    try:
        n = page.locator("div[data-sort='1']").first
        if await n.is_visible(timeout=800):
            await n.click()
            print(f"    [{name}] ✓ Sort: data-sort=1 already visible", flush=True)
            return True
    except: pass

    # Open sort dropdown — confirmed classes from DevTools
    opened = False
    for sel in ["div.faMaId", "div.AHwJHf", "button[aria-label*='Sort' i]",
                "[jsaction*='sort']"]:
        try:
            loc = page.locator(sel).first
            if await loc.is_visible(timeout=1500):
                await loc.click()
                opened = True
                print(f"    [{name}] ✓ Sort trigger: {sel}", flush=True)
                break
        except: continue

    if not opened:
        # JS fallback using confirmed class names
        try:
            r = await page.evaluate("""
                () => {
                    let el = document.querySelector('.faMaId, .AHwJHf');
                    if (el) { el.click(); return el.className; }
                    // radiogroup itself is clickable
                    el = document.querySelector('[role="radiogroup"][aria-label*="Sort" i]');
                    if (el) { el.click(); return 'radiogroup'; }
                    return null;
                }
            """)
            if r: opened = True
        except: pass

    if not opened:
        print(f"    [{name}] ✗ Sort trigger not found", flush=True)
        return False

    await asyncio.sleep(random.uniform(1.0, 1.5))

    # Click Newest = data-sort="1"
    try:
        n = page.locator("div[data-sort='1']").first
        if await n.is_visible(timeout=2000):
            await n.click()
            print(f"    [{name}] ✓ Sort: Newest selected", flush=True)
            await asyncio.sleep(random.uniform(2, 3))
            return True
    except: pass

    # JS fallback
    try:
        r = await page.evaluate("() => { const e=document.querySelector('[data-sort=\"1\"]'); if(e){e.click();return true;}return false; }")
        if r:
            await asyncio.sleep(random.uniform(2, 3))
            return True
    except: pass

    print(f"    [{name}] ✗ Sort Newest not clicked", flush=True)
    return False

# ══════════════════════════════════════════════════════════════════════════════
# STEP 4 – Find scroll panel
# CONFIRMED: div.m6QErb is the inner scrollable reviews container
# ══════════════════════════════════════════════════════════════════════════════

async def find_panel(page):
    for sel in ["div.m6QErb.DxyBCb","div.m6QErb.WNBkOb","div.m6QErb",
                "div[role='feed']","div.section-scrollbox"]:
        try:
            loc = page.locator(sel).first
            if await loc.count():
                box = await loc.bounding_box()
                if box and box["height"] > 150:
                    return await loc.element_handle()
        except: continue
    # JS fallback: largest scrollable div inside role=main
    try:
        h = await page.evaluate_handle("""
            () => {
                const m = document.querySelector('[role="main"]') || document.body;
                let best=null, bh=0;
                for (const el of m.querySelectorAll('div')) {
                    if (el.scrollHeight>el.clientHeight+100 && el.clientHeight>150)
                        if (el.clientHeight>bh){best=el;bh=el.clientHeight;}
                }
                return best;
            }
        """)
        el = h.as_element()
        if el: return el
    except: pass
    return None

# ══════════════════════════════════════════════════════════════════════════════
# STEP 5 – Parse one review card
# CONFIRMED selectors from DevTools
# ══════════════════════════════════════════════════════════════════════════════

async def parse_card(card, branch, sd, ref):
    # Author — confirmed: span.d4r55
    author = "Unknown"
    for sel in ["span.d4r55",".fontHeadlineSmall",".jJc9Ad"]:
        try:
            el=card.locator(sel).first
            if await el.count():
                t=(await el.inner_text(timeout=600)).strip()
                if t: author=t; break
        except: pass

    # Rating — confirmed: span[role="img"][aria-label="X stars"]
    rating = 0.0
    for sel in ["span[role='img'][aria-label]",".kvMYJc","span[aria-label*='star' i]"]:
        try:
            el=card.locator(sel).first
            if await el.count():
                raw=(await el.get_attribute("aria-label",timeout=600)) or ""
                m=re.search(r"(\d+\.?\d*)",raw)
                if m: rating=float(m.group(1)); break
        except: pass

    # Text — confirmed: span.wiI7pd
    text=""
    for sel in ["span.wiI7pd",".wiI7pd",".MyEned span"]:
        try:
            el=card.locator(sel).first
            if await el.count():
                t=(await el.inner_text(timeout=600)).strip().replace("\n"," ")
                if t: text=t; break
        except: pass

    # Timestamp — confirmed: span.rsqaWe
    rel_time=""
    for sel in ["span.rsqaWe",".DU9Pgb","span.dehysf"]:
        try:
            el=card.locator(sel).first
            if await el.count():
                t=(await el.inner_text(timeout=600)).strip()
                if t: rel_time=t; break
        except: pass

    if not within_23h(rel_time):
        return None

    return {
        "fingerprint": fp(rating, author, text),
        "branch_id":   branch["id"],
        "branch_name": branch["name"],
        "agm":         branch["agm"],
        "author":      author,
        "rating":      rating,
        "text":        text,
        "rel_time":    rel_time,
        "parsed_date": rel2abs(rel_time, ref),
        "snap_date":   sd,
        "scraped_at":  ref.strftime("%Y-%m-%d %H:%M"),
    }

# ══════════════════════════════════════════════════════════════════════════════
# Core scrape – one branch
# ══════════════════════════════════════════════════════════════════════════════

async def _scrape_once(branch, sd, attempt=1):
    name = branch["name"]
    place_id = branch["place_id"]
    extra = 2*(attempt-1)
    reviews = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=CHROMIUM_ARGS)
        try:
            ctx  = await new_ctx(browser)
            page = await ctx.new_page()

            # Navigate & wait for sidebar
            ok = await navigate(page, place_id, name)
            await asyncio.sleep(random.uniform(2+extra, 3+extra))

            if not ok:
                await dbg(page, name, "no_sidebar")
                return []

            await dbg(page, name, "01_loaded")

            # Click Reviews tab
            await click_reviews_tab(page, name)
            await asyncio.sleep(random.uniform(2+extra, 3+extra))
            await dbg(page, name, "02_after_tab")

            # Sort by Newest
            await sort_newest(page, name)
            await asyncio.sleep(random.uniform(2+extra, 3+extra))
            await dbg(page, name, "03_after_sort")

            # Find scroll panel
            panel = await find_panel(page)
            prev = stale = 0

            # Scroll until past 23h window or stale
            for rnd in range(MAX_SCROLL_ROUNDS):
                # Expand "More" buttons
                for msel in ["button.w8nwRe","button[aria-label='See more']"]:
                    try:
                        btns=page.locator(msel)
                        for i in range(await btns.count()):
                            try:
                                b=btns.nth(i)
                                if await b.is_visible(timeout=200):
                                    await b.click(); await asyncio.sleep(0.15)
                            except: pass
                    except: pass

                # Stop when we hit a review older than 23h
                ts=page.locator("span.rsqaWe,.DU9Pgb")
                n=await ts.count()
                if n>0:
                    try:
                        last=(await ts.nth(n-1).inner_text(timeout=400)).strip()
                        if last and not within_23h(last): break
                    except: pass

                # Scroll the inner panel div (NOT window scroll)
                scrolled=False
                if panel:
                    try:
                        await page.evaluate("el=>{el.scrollTop=el.scrollHeight;}",panel)
                        scrolled=True
                    except: panel=None
                if not scrolled:
                    await page.evaluate("window.scrollBy(0,3000)")

                await asyncio.sleep(random.uniform(1.8, 2.8))

                curr=await page.locator("div.jftiEf").count()
                if curr==prev:
                    stale+=1
                    if stale>=STALE_LIMIT: break
                else:
                    stale=0; prev=curr

            await dbg(page, name, "04_done")

            # Parse cards
            ref   = ist_now()
            cards = await page.locator("div.jftiEf").all()
            print(f"    [{name}] {len(cards)} cards in DOM", flush=True)

            if not cards:
                await dbg(page, name, "zero_cards")

            for card in cards:
                try:
                    rv = await parse_card(card, branch, sd, ref)
                    if rv: reviews.append(rv)
                except: continue

            print(f"    [{name}] {len(reviews)}/{len(cards)} within 23h", flush=True)

        except Exception as e:
            print(f"    [{name}] EXCEPTION: {e!s:.100s}", flush=True)
            traceback.print_exc()
            try: await dbg(page, name, "exception")
            except: pass
        finally:
            await browser.close()

    return reviews


async def scrape_branch(branch, sem, sd):
    async with sem:
        for attempt in range(1, MAX_RETRIES+2):
            try:
                result = await _scrape_once(branch, sd, attempt=attempt)
                if result or attempt>MAX_RETRIES:
                    icon="✅" if result else "⚪"
                    print(f"  {icon} {branch['name']:22s} → {len(result):3d}", flush=True)
                    return result
                wait=20*attempt+random.uniform(5,10)
                print(f"  ↺  {branch['name']} 0 reviews, retry {attempt+1} in {wait:.0f}s…",flush=True)
                await asyncio.sleep(wait)
            except Exception as e:
                wait=20*attempt+random.uniform(5,10)
                if attempt<=MAX_RETRIES:
                    print(f"  ⚠️  {branch['name']} attempt {attempt} – retry in {wait:.0f}s")
                    await asyncio.sleep(wait)
                else:
                    print(f"  ❌ {branch['name']} gave up.",flush=True)
        return []

# ══════════════════════════════════════════════════════════════════════════════
# Deletion tracking
# ══════════════════════════════════════════════════════════════════════════════

def track_deletions(live, old_del, old_live, fresh_fps, sd):
    now_str=ist_now().strftime("%Y-%m-%d %H:%M")
    del_out=dict(old_del)
    # Reinstatements
    for f in list(del_out.keys()):
        if f in fresh_fps:
            item=dict(del_out.pop(f)); item.pop("deleted_on",None)
            item["reinstated_on"]=now_str; live[f]=item
    # Deletions (scoped to same snap_date only)
    for f,item in {f:v for f,v in old_live.items() if v.get("snap_date")==sd}.items():
        if f not in fresh_fps and f not in del_out:
            di=dict(item); di["deleted_on"]=now_str
            del_out[f]=di; live.pop(f,None)
    # Purge old
    cutoff=ist_now()-timedelta(days=DELETION_DAYS)
    for f in list(del_out.keys()):
        try:
            d=datetime.strptime(del_out[f].get("deleted_on",""),"%Y-%m-%d %H:%M").replace(tzinfo=IST)
            if d<cutoff: del_out.pop(f)
        except: pass
    return live, del_out

# ══════════════════════════════════════════════════════════════════════════════
# Main
# ══════════════════════════════════════════════════════════════════════════════

async def main():
    slot=get_slot(); sd=snap_date(slot)
    print(f"\n{'═'*60}")
    print(f"  Sathya Scraper  |  slot={slot}  |  snap_date={sd}")
    print(f"  IST: {ist_now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'═'*60}\n")

    old_live={r["fingerprint"]:r for r in load_json(REV_JSON)}
    old_del ={r["fingerprint"]:r for r in load_json(DEL_JSON)}
    print(f"  Existing: {len(old_live)} live, {len(old_del)} deleted\n")

    sem=asyncio.Semaphore(MAX_CONCURRENT)
    batches=await asyncio.gather(*[scrape_branch(b,sem,sd) for b in BRANCHES])

    fresh_map={}
    for batch in batches:
        for r in batch:
            if r["fingerprint"] not in fresh_map:
                fresh_map[r["fingerprint"]]=r
    fresh_fps=set(fresh_map.keys())
    print(f"\n  Fresh: {len(fresh_fps)} unique reviews within 23h\n")

    merged=dict(old_live)
    new_c=upd_c=0
    for f,r in fresh_map.items():
        if f not in merged:
            merged[f]=r; new_c+=1
        else:
            ex=dict(merged[f])
            ex.update({"rel_time":r["rel_time"],"parsed_date":r["parsed_date"],"scraped_at":r["scraped_at"]})
            merged[f]=ex; upd_c+=1
    print(f"  Merge: {new_c} new, {upd_c} refreshed")

    merged,merged_del=track_deletions(merged,old_del,old_live,fresh_fps,sd)

    final_live=sorted(merged.values(),key=lambda x:(x.get("snap_date",""),x.get("parsed_date","")),reverse=True)
    final_del =sorted(merged_del.values(),key=lambda x:x.get("deleted_on",""),reverse=True)

    save_json(REV_JSON,final_live)
    save_json(DEL_JSON,final_del)

    print(f"\n{'═'*60}")
    print(f"  ✅  rev.json: {len(final_live)}  |  deleted.json: {len(final_del)}")
    print(f"  📸  debug/ → upload as GH Actions artifact")
    print(f"{'═'*60}\n")

if __name__=="__main__":
    asyncio.run(main())
