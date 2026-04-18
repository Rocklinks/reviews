"""
scraper.py – Sathya Review Scraper (exact-selector build)

DOM SELECTORS CONFIRMED BY BROWSER INSPECTION
==============================================
Reviews tab : div[role="button"] containing <span class="PbOY2e">Reviews</span>
Sort button : div[jscontroller="gljxuc"][role="radiogroup"][aria-label="Sort reviews"]
Sort newest : div[data-sort="1"]   ← Newest is data-sort="1"
Sort options: data-sort="2" Most relevant, "3" Highest, "4" Lowest
Review cards: div.jftiEf  (unchanged)
Timestamps  : .rsqaWe  (unchanged)

RUN SLOTS (UTC → IST)
=====================
  00:30 UTC → 06:00 IST  morning   snap_date = today
  06:30 UTC → 12:00 IST  noon      snap_date = today
  12:30 UTC → 18:00 IST  evening   snap_date = today
  18:30 UTC → 00:00 IST  midnight  snap_date = YESTERDAY
"""

import asyncio
import hashlib
import json
import random
import re
import sys
import traceback
from datetime import datetime, timedelta, timezone
from pathlib import Path

from playwright.async_api import async_playwright, TimeoutError as PWTimeout

# ── Paths ──────────────────────────────────────────────────────────────────────
ROOT     = Path(__file__).parent
DOCS_DIR = ROOT / "docs"
DBG_DIR  = ROOT / "debug"
SS_DIR   = DBG_DIR / "screenshots"
DM_DIR   = DBG_DIR / "dom_dumps"
REV_JSON = DOCS_DIR / "rev.json"
DEL_JSON = DOCS_DIR / "deleted.json"
for d in [DOCS_DIR, SS_DIR, DM_DIR]:
    d.mkdir(parents=True, exist_ok=True)

# ── Config ─────────────────────────────────────────────────────────────────────
MAX_CONCURRENT    = 2
MAX_RETRIES       = 2
MAX_SCROLL_ROUNDS = 40
STALE_LIMIT       = 4
DELETION_DAYS     = 30
IST               = timezone(timedelta(hours=5, minutes=30))

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
]

CHROMIUM_ARGS = [
    "--no-sandbox", "--disable-setuid-sandbox",
    "--disable-blink-features=AutomationControlled",
    "--disable-dev-shm-usage", "--disable-extensions",
    "--disable-infobars", "--no-first-run",
    "--disable-default-apps", "--mute-audio",
    "--disable-translate", "--disable-sync",
    "--disable-background-networking",
    "--disable-gpu", "--disable-software-rasterizer",
    "--window-size=1400,900",
]

STEALTH_JS = """
Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
Object.defineProperty(navigator, 'plugins',   {get: () => [1,2,3,4,5]});
Object.defineProperty(navigator, 'languages', {get: () => ['en-US','en']});
window.chrome = { runtime: {}, loadTimes: function(){}, csi: function(){} };
try {
    const orig = window.navigator.permissions.query;
    window.navigator.permissions.query = p =>
        p.name === 'notifications'
            ? Promise.resolve({state: Notification.permission})
            : orig(p);
} catch(e) {}
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
# Helpers
# ══════════════════════════════════════════════════════════════════════════════

def ist_now() -> datetime:
    return datetime.now(IST)

def ist_today() -> str:
    return ist_now().strftime("%Y-%m-%d")

def ist_yesterday() -> str:
    return (ist_now() - timedelta(days=1)).strftime("%Y-%m-%d")

def get_run_slot() -> str:
    h = datetime.now(timezone.utc).hour
    if h == 0:  return "morning"
    if h == 6:  return "noon"
    if h == 12: return "evening"
    if h == 18: return "midnight"
    ih = ist_now().hour
    if 5  <= ih < 11: return "morning"
    if 11 <= ih < 17: return "noon"
    if 17 <= ih < 23: return "evening"
    return "midnight"

def snap_date_for_slot(slot: str) -> str:
    return ist_yesterday() if slot == "midnight" else ist_today()

def make_fp(rating: float, author: str, text: str) -> str:
    raw = f"{round(rating,1)}|{(author or '').lower().strip()[:40]}|{(text or '').lower().strip()[:200]}"
    return hashlib.sha256(raw.encode()).hexdigest()[:20]

def slug(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")

def load_json(path: Path) -> list:
    if path.exists():
        try: return json.loads(path.read_text(encoding="utf-8"))
        except: return []
    return []

def save_json(path: Path, data: list) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")

_WITHIN_23H = re.compile(
    r"^(?:just now|a moment ago|moments? ago)$"
    r"|^(\d+)\s*(second|minute|hour)s?\s*ago$",
    re.IGNORECASE,
)

def is_within_23h(rel: str) -> bool:
    rel = (rel or "").strip()
    m = _WITHIN_23H.match(rel)
    if not m: return False
    if not m.group(1): return True
    val, unit = int(m.group(1)), m.group(2).lower()
    if unit == "second": return True
    if unit == "minute": return val <= 1380
    if unit == "hour":   return val <= 23
    return False

def rel_to_abs(rel: str, ref: datetime) -> str:
    rel = (rel or "").strip().lower()
    if not rel or "just now" in rel or "moment" in rel:
        return ref.strftime("%Y-%m-%d %H:%M:%S")
    m = re.search(r"(\d+)\s*(second|minute|hour|day|week|month|year)", rel)
    if not m: return ref.strftime("%Y-%m-%d %H:%M:%S")
    val, unit = int(m.group(1)), m.group(2)
    d = {"second": timedelta(seconds=val), "minute": timedelta(minutes=val),
         "hour": timedelta(hours=val), "day": timedelta(days=val),
         "week": timedelta(weeks=val), "month": timedelta(days=30*val),
         "year": timedelta(days=365*val)}
    return (ref - d.get(unit, timedelta())).strftime("%Y-%m-%d %H:%M:%S")


# ══════════════════════════════════════════════════════════════════════════════
# Debug helpers
# ══════════════════════════════════════════════════════════════════════════════

async def save_debug(page, name: str, label: str):
    sl = slug(name)
    lb = re.sub(r"[^a-z0-9]+", "_", label.lower())
    try:
        await page.screenshot(path=str(SS_DIR / f"{sl}_{lb}.png"), full_page=False)
    except: pass
    try:
        lines = [f"=== {name} | {label} ===", f"URL: {page.url}",
                 f"Title: {await page.title()}", ""]
        for sel in ["div.jftiEf", ".rsqaWe", ".DU9Pgb", "span.PbOY2e",
                    "div[role='button']", "div[data-sort]",
                    "div[role='radiogroup']", "div.m6QErb", "div.m6QErb.DxyBCb"]:
            try:
                n = await page.locator(sel).count()
                if n: lines.append(f"  {n:4d}  {sel}")
            except: pass
        # Sample first card inner text
        try:
            cards = await page.locator("div.jftiEf").all()
            for i, c in enumerate(cards[:3]):
                t = (await c.inner_text(timeout=500)).replace("\n"," ")[:200]
                lines.append(f"  CARD[{i}]: {t!r}")
        except: pass
        (DM_DIR / f"{sl}_{lb}.txt").write_text("\n".join(lines), encoding="utf-8")
    except: pass


# ══════════════════════════════════════════════════════════════════════════════
# Browser context
# ══════════════════════════════════════════════════════════════════════════════

async def make_context(browser):
    ctx = await browser.new_context(
        viewport={"width": 1400, "height": 900},
        user_agent=random.choice(USER_AGENTS),
        locale="en-US",
        timezone_id="Asia/Kolkata",
        java_script_enabled=True,
        extra_http_headers={
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
        },
    )
    await ctx.add_init_script(STEALTH_JS)
    return ctx


# ══════════════════════════════════════════════════════════════════════════════
# STEP 1 – Navigate and wait for sidebar
# ══════════════════════════════════════════════════════════════════════════════

async def navigate_and_wait(page, place_id: str, name: str) -> bool:
    """
    Navigate to the Maps place page and wait until the sidebar is interactive.
    Returns True when the sidebar is confirmed loaded.
    """
    url = f"https://www.google.com/maps/place/?q=place_id:{place_id}&hl=en"

    # Block images/fonts to speed up load; keep XHR/JS/CSS
    await page.route(
        re.compile(r"\.(png|jpg|jpeg|gif|webp|svg|ico|woff2?|ttf|otf|mp4|mp3)(\?.*)?$"),
        lambda route: route.abort()
    )

    try:
        await page.goto(url, wait_until="load", timeout=60_000)
    except PWTimeout:
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=60_000)
        except Exception as e:
            print(f"    [{name}] Navigation failed: {e!s:.80s}", flush=True)
            return False

    # Dismiss consent
    for sel in ["#L2AGLb", "button[aria-label*='Accept all' i]",
                "[aria-label*='Before you continue'] button:last-child"]:
        try:
            b = page.locator(sel).first
            if await b.is_visible(timeout=1500):
                await b.click()
                await asyncio.sleep(1)
                break
        except: pass

    # Wait for the Reviews tab button to appear — confirmed class: PbOY2e
    # This is the most reliable indicator that Maps has fully bootstrapped
    found = False
    deadline = asyncio.get_event_loop().time() + 30
    while asyncio.get_event_loop().time() < deadline:
        for sel in [
            "span.PbOY2e",                    # ← CONFIRMED: Reviews tab span class
            "div[role='tablist']",
            "div[data-tab-index]",
            "div[jsdata*='haAclf']",           # ← scroll container parent from Image 2
            "div[jsaction*='pane.reviews']",
        ]:
            try:
                if await page.locator(sel).count() > 0:
                    found = True
                    break
            except: pass
        if found: break
        await asyncio.sleep(0.8)

    if not found:
        print(f"    [{name}] Sidebar never appeared", flush=True)
    return found


# ══════════════════════════════════════════════════════════════════════════════
# STEP 2 – Click the Reviews tab
# CONFIRMED selector: span.PbOY2e containing text "Reviews"
# Parent: div[role="button"] data-tab-index="1" (usually)
# ══════════════════════════════════════════════════════════════════════════════

async def click_reviews_tab(page, name: str) -> bool:
    await asyncio.sleep(random.uniform(1.5, 2.5))

    # S1 ── CONFIRMED: span.PbOY2e with text "Reviews" inside role=button ──────
    try:
        spans = await page.locator("span.PbOY2e").all()
        for sp in spans:
            try:
                txt = (await sp.inner_text(timeout=500)).strip().lower()
                if "review" in txt:
                    # Click the parent role=button
                    parent = page.locator("div[role='button']:has(span.PbOY2e)")
                    if await parent.count():
                        await parent.first.click()
                    else:
                        await sp.click()
                    print(f"    [{name}] ✓ Reviews tab clicked (S1: span.PbOY2e)", flush=True)
                    return True
            except: continue
    except: pass

    # S2 ── data-tab-index attribute (Reviews is usually index 1) ──────────────
    try:
        for idx in ["1", "2"]:
            loc = page.locator(f"div[role='button'][data-tab-index='{idx}']")
            if await loc.count():
                txt = (await loc.first.inner_text(timeout=500)).lower()
                if "review" in txt:
                    await loc.first.click()
                    print(f"    [{name}] ✓ Reviews tab clicked (S2: data-tab-index={idx})", flush=True)
                    return True
    except: pass

    # S3 ── Any role=button whose inner text contains "Reviews" ─────────────────
    try:
        btns = await page.locator("div[role='button']").all()
        for btn in btns:
            try:
                txt = (await btn.inner_text(timeout=400)).strip().lower()
                if txt.startswith("review"):
                    await btn.click()
                    print(f"    [{name}] ✓ Reviews tab clicked (S3: role=button text match)", flush=True)
                    return True
            except: continue
    except: pass

    # S4 ── JS direct click on the confirmed span class ────────────────────────
    try:
        result = await page.evaluate("""
            () => {
                // Find span.PbOY2e with 'Reviews' text and click its button parent
                const spans = document.querySelectorAll('span.PbOY2e');
                for (const s of spans) {
                    if (s.innerText && s.innerText.toLowerCase().includes('review')) {
                        let el = s;
                        while (el && el.getAttribute('role') !== 'button') el = el.parentElement;
                        if (el) { el.click(); return 'clicked role=button parent'; }
                        s.click(); return 'clicked span directly';
                    }
                }
                return null;
            }
        """)
        if result:
            print(f"    [{name}] ✓ Reviews tab clicked (S4: JS – {result})", flush=True)
            return True
    except: pass

    # S5 ── div[data-tab-index] fallback ────────────────────────────────────────
    try:
        tabs = await page.locator("div[data-tab-index]").all()
        for tab in tabs:
            try:
                txt = (await tab.inner_text(timeout=400)).lower()
                if "review" in txt:
                    await tab.click()
                    print(f"    [{name}] ✓ Reviews tab clicked (S5: data-tab-index text)", flush=True)
                    return True
            except: continue
    except: pass

    print(f"    [{name}] ✗ All tab strategies failed", flush=True)
    return False


# ══════════════════════════════════════════════════════════════════════════════
# STEP 3 – Sort by Newest
# CONFIRMED: div[role="radiogroup"][aria-label="Sort reviews"]
#            children: div[data-sort="1"] = Newest
# ══════════════════════════════════════════════════════════════════════════════

async def sort_newest(page, name: str) -> bool:
    await asyncio.sleep(random.uniform(1.5, 2.5))

    # S1 ── CONFIRMED: open sort menu then click data-sort="1" ─────────────────
    # First find and click the sort trigger button
    sort_trigger_selectors = [
        "div.faMaId",                         # ← confirmed class from Image 2
        "div[jsaction*='sort']",
        "button[aria-label*='Sort' i]",
        "div[aria-label*='Sort' i]",
        # The "Sort by" text container
        "div.AHwJHf",                         # ← confirmed class from Image 2
    ]
    trigger_clicked = False
    for sel in sort_trigger_selectors:
        try:
            loc = page.locator(sel).first
            if await loc.is_visible(timeout=2000):
                await loc.click()
                trigger_clicked = True
                print(f"    [{name}] ✓ Sort trigger clicked ({sel})", flush=True)
                break
        except: continue

    if not trigger_clicked:
        # JS fallback: click the "Sort by" text or its container
        try:
            r = await page.evaluate("""
                () => {
                    // Try AHwJHf (Sort by label confirmed in DOM)
                    let el = document.querySelector('.AHwJHf, .faMaId');
                    if (el) { el.click(); return el.className; }
                    // Try any element with 'Sort' text
                    const all = document.querySelectorAll('div, button');
                    for (const e of all) {
                        if (e.childElementCount === 0 &&
                            e.innerText && e.innerText.trim() === 'Sort by') {
                            e.click(); return 'Sort by text';
                        }
                    }
                    return null;
                }
            """)
            if r:
                trigger_clicked = True
                print(f"    [{name}] ✓ Sort trigger clicked (JS: {r})", flush=True)
        except: pass

    if not trigger_clicked:
        print(f"    [{name}] ✗ Sort trigger not found, skipping sort", flush=True)
        return False

    await asyncio.sleep(random.uniform(1.0, 1.8))

    # S2 ── CONFIRMED: click div[data-sort="1"] (Newest) ──────────────────────
    try:
        newest = page.locator("div[data-sort='1'], [data-sort='1']").first
        if await newest.is_visible(timeout=3000):
            await newest.click()
            print(f"    [{name}] ✓ Sorted by Newest (data-sort=1)", flush=True)
            return True
    except: pass

    # S3 ── radiogroup → first radio option ────────────────────────────────────
    try:
        rg = page.locator("div[role='radiogroup'][aria-label*='Sort' i]").first
        if await rg.count():
            first_opt = rg.locator("div[role='radio']").first
            if await first_opt.is_visible(timeout=2000):
                await first_opt.click()
                print(f"    [{name}] ✓ Sorted by Newest (radiogroup first radio)", flush=True)
                return True
    except: pass

    # S4 ── JS direct click on data-sort=1 ─────────────────────────────────────
    try:
        r = await page.evaluate("""
            () => {
                const el = document.querySelector('[data-sort="1"]');
                if (el) { el.click(); return true; }
                return false;
            }
        """)
        if r:
            print(f"    [{name}] ✓ Sorted by Newest (JS data-sort=1)", flush=True)
            return True
    except: pass

    print(f"    [{name}] ✗ Sort Newest not clicked", flush=True)
    return False


# ══════════════════════════════════════════════════════════════════════════════
# STEP 4 – Find the scrollable panel
# CONFIRMED parent: div[jsdata="haAclf"] with scrollbar-width:none overflow-x:auto
# Inner scroll target: div.m6QErb  (unchanged across Maps versions)
# ══════════════════════════════════════════════════════════════════════════════

async def find_scroll_panel(page):
    for sel in [
        "div.m6QErb.DxyBCb",
        "div.m6QErb.WNBkOb",
        "div.m6QErb",
        "div[role='feed']",
        # Confirmed from Image 2: jsdata=haAclf is the scroll parent
        "div[jsdata='haAclf']",
        "div[jsaction*='t3L5Dd']",
        "div.section-scrollbox",
    ]:
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
                const candidates = document.querySelectorAll('div[role="main"] div');
                let best = null, bestH = 0;
                for (const el of candidates) {
                    if (el.scrollHeight > el.clientHeight + 100 && el.clientHeight > 150) {
                        if (el.clientHeight > bestH) { best = el; bestH = el.clientHeight; }
                    }
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
# ══════════════════════════════════════════════════════════════════════════════

async def parse_card(card, branch: dict, snap_date: str, ref: datetime) -> dict | None:
    # Author
    author = "Unknown"
    for sel in [".d4r55", ".fontHeadlineSmall", ".jJc9Ad", ".Vpc5Fe"]:
        try:
            el = card.locator(sel).first
            if await el.count():
                t = (await el.inner_text(timeout=600)).strip()
                if t: author = t; break
        except: pass

    # Rating – aria-label="5 stars" or "Rated 4 out of 5"
    rating = 0.0
    for sel in ["span[role='img'][aria-label]", ".kvMYJc",
                "span[aria-label*='star' i]", ".hCCjke span[aria-label]"]:
        try:
            el = card.locator(sel).first
            if await el.count():
                raw = (await el.get_attribute("aria-label", timeout=600)) or ""
                m = re.search(r"(\d+\.?\d*)", raw)
                if m: rating = float(m.group(1)); break
        except: pass

    # Text
    text = ""
    for sel in [".wiI7pd", "span.wiI7pd", ".MyEned span", ".Jtu6Td"]:
        try:
            el = card.locator(sel).first
            if await el.count():
                t = (await el.inner_text(timeout=600)).strip().replace("\n", " ")
                if t: text = t; break
        except: pass

    # Timestamp – confirmed class: .rsqaWe
    rel_time = ""
    for sel in [".rsqaWe", ".DU9Pgb", "span.dehysf"]:
        try:
            el = card.locator(sel).first
            if await el.count():
                t = (await el.inner_text(timeout=600)).strip()
                if t: rel_time = t; break
        except: pass

    if not is_within_23h(rel_time):
        return None

    return {
        "fingerprint":  make_fp(rating, author, text),
        "branch_id":    branch["id"],
        "branch_name":  branch["name"],
        "agm":          branch["agm"],
        "author":       author,
        "rating":       rating,
        "text":         text,
        "rel_time":     rel_time,
        "parsed_date":  rel_to_abs(rel_time, ref),
        "snap_date":    snap_date,
        "scraped_at":   ref.strftime("%Y-%m-%d %H:%M"),
    }


# ══════════════════════════════════════════════════════════════════════════════
# Core scrape – one branch
# ══════════════════════════════════════════════════════════════════════════════

async def _scrape_once(branch: dict, snap_date: str, attempt: int = 1) -> list[dict]:
    name     = branch["name"]
    place_id = branch["place_id"]
    extra    = 3 * (attempt - 1)   # add wait time on each retry

    reviews: list[dict] = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=CHROMIUM_ARGS)
        try:
            ctx  = await make_context(browser)
            page = await ctx.new_page()

            # ── Navigate ──────────────────────────────────────────────────────
            ok = await navigate_and_wait(page, place_id, name)
            if not ok:
                await save_debug(page, name, "no_sidebar")
                return []

            await asyncio.sleep(random.uniform(2 + extra, 3 + extra))
            await save_debug(page, name, "01_loaded")

            # ── Click Reviews tab ─────────────────────────────────────────────
            await click_reviews_tab(page, name)
            await asyncio.sleep(random.uniform(3 + extra, 4 + extra))
            await save_debug(page, name, "02_after_tab")

            # ── Sort by Newest ────────────────────────────────────────────────
            await sort_newest(page, name)
            await asyncio.sleep(random.uniform(2 + extra, 3 + extra))
            await save_debug(page, name, "03_after_sort")

            # ── Find scroll panel ─────────────────────────────────────────────
            panel = await find_scroll_panel(page)

            # ── Scroll loop ───────────────────────────────────────────────────
            prev_count = stale = 0

            for rnd in range(MAX_SCROLL_ROUNDS):
                # Expand "More" buttons
                for msel in ["button.w8nwRe", "button[aria-label='See more']"]:
                    try:
                        btns = page.locator(msel)
                        for i in range(await btns.count()):
                            try:
                                b = btns.nth(i)
                                if await b.is_visible(timeout=200):
                                    await b.click(); await asyncio.sleep(0.15)
                            except: pass
                    except: pass

                # Stop if we've scrolled past the 23h window
                ts = page.locator(".rsqaWe, .DU9Pgb")
                n  = await ts.count()
                if n > 0:
                    try:
                        last = (await ts.nth(n - 1).inner_text(timeout=400)).strip()
                        if last and not is_within_23h(last): break
                    except: pass

                # Scroll the panel
                scrolled = False
                if panel:
                    try:
                        await page.evaluate("el => { el.scrollTop = el.scrollHeight; }", panel)
                        scrolled = True
                    except:
                        panel = None

                if not scrolled:
                    # Fallback: arrow-down in the feed
                    await page.keyboard.press("End")

                await asyncio.sleep(random.uniform(1.8, 2.8))

                curr = await page.locator("div.jftiEf").count()
                if curr == prev_count:
                    stale += 1
                    if stale >= STALE_LIMIT: break
                else:
                    stale = 0; prev_count = curr

            await save_debug(page, name, "04_after_scroll")

            # ── Parse ─────────────────────────────────────────────────────────
            ref   = ist_now()
            cards = await page.locator("div.jftiEf").all()
            print(f"    [{name}] {len(cards)} cards in DOM", flush=True)

            if not cards:
                await save_debug(page, name, "ZERO_CARDS")

            for card in cards:
                try:
                    rv = await parse_card(card, branch, snap_date, ref)
                    if rv: reviews.append(rv)
                except: continue

        except Exception as e:
            print(f"    [{name}] EXCEPTION: {e!s:.120s}", flush=True)
            traceback.print_exc()
            try: await save_debug(page, name, "exception")
            except: pass
        finally:
            await browser.close()

    return reviews


async def scrape_branch(branch: dict, sem: asyncio.Semaphore, snap_date: str) -> list[dict]:
    async with sem:
        for attempt in range(1, MAX_RETRIES + 2):
            try:
                result = await _scrape_once(branch, snap_date, attempt=attempt)
                if result or attempt > MAX_RETRIES:
                    icon = "✅" if result else "⚪"
                    print(f"  {icon} {branch['name']:22s} → {len(result):3d} review(s)", flush=True)
                    return result
                wait = 20 * attempt + random.uniform(5, 10)
                print(f"  ↺  {branch['name']} 0 reviews, retry {attempt+1} in {wait:.0f}s…", flush=True)
                await asyncio.sleep(wait)
            except Exception as e:
                wait = 20 * attempt + random.uniform(5, 10)
                if attempt <= MAX_RETRIES:
                    print(f"  ⚠️  {branch['name']} attempt {attempt} failed – retry in {wait:.0f}s")
                    await asyncio.sleep(wait)
                else:
                    print(f"  ❌ {branch['name']} gave up.", flush=True)
        return []


# ══════════════════════════════════════════════════════════════════════════════
# Deletion tracking
# ══════════════════════════════════════════════════════════════════════════════

def track_deletions(live_map, old_del_map, old_live_map, fresh_fps, snap_date):
    now_str = ist_now().strftime("%Y-%m-%d %H:%M")
    del_out = dict(old_del_map)
    for fp in list(del_out.keys()):
        if fp in fresh_fps:
            item = dict(del_out.pop(fp))
            item.pop("deleted_on", None)
            item["reinstated_on"] = now_str
            live_map[fp] = item
    scoped = {fp: v for fp, v in old_live_map.items() if v.get("snap_date") == snap_date}
    for fp, item in scoped.items():
        if fp not in fresh_fps and fp not in del_out:
            di = dict(item); di["deleted_on"] = now_str
            del_out[fp] = di; live_map.pop(fp, None)
    cutoff = ist_now() - timedelta(days=DELETION_DAYS)
    for fp in list(del_out.keys()):
        try:
            d = datetime.strptime(del_out[fp].get("deleted_on",""), "%Y-%m-%d %H:%M").replace(tzinfo=IST)
            if d < cutoff: del_out.pop(fp)
        except: pass
    return live_map, del_out


# ══════════════════════════════════════════════════════════════════════════════
# Main
# ══════════════════════════════════════════════════════════════════════════════

async def main():
    slot      = get_run_slot()
    snap_date = snap_date_for_slot(slot)
    print(f"\n{'═'*64}")
    print(f"  Sathya Review Scraper  |  slot={slot}  |  snap_date={snap_date}")
    print(f"  IST: {ist_now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'═'*64}\n")

    old_live_map = {r["fingerprint"]: r for r in load_json(REV_JSON)}
    old_del_map  = {r["fingerprint"]: r for r in load_json(DEL_JSON)}
    print(f"  Existing: {len(old_live_map)} live, {len(old_del_map)} deleted\n")

    sem     = asyncio.Semaphore(MAX_CONCURRENT)
    tasks   = [scrape_branch(b, sem, snap_date) for b in BRANCHES]
    batches = await asyncio.gather(*tasks)

    fresh_map: dict[str, dict] = {}
    for batch in batches:
        for r in batch:
            if r["fingerprint"] not in fresh_map:
                fresh_map[r["fingerprint"]] = r
    fresh_fps = set(fresh_map.keys())
    print(f"\n  This run: {len(fresh_fps)} unique reviews within 23h\n")

    merged = dict(old_live_map)
    new_c = upd_c = 0
    for fp, r in fresh_map.items():
        if fp not in merged:
            merged[fp] = r; new_c += 1
        else:
            ex = dict(merged[fp])
            ex.update({"rel_time": r["rel_time"], "parsed_date": r["parsed_date"],
                        "scraped_at": r["scraped_at"]})
            merged[fp] = ex; upd_c += 1

    print(f"  Merge: {new_c} new, {upd_c} refreshed")
    merged, merged_del = track_deletions(merged, old_del_map, old_live_map, fresh_fps, snap_date)

    final_live = sorted(merged.values(),
                        key=lambda x: (x.get("snap_date",""), x.get("parsed_date","")), reverse=True)
    final_del  = sorted(merged_del.values(),
                        key=lambda x: x.get("deleted_on",""), reverse=True)

    save_json(REV_JSON, final_live)
    save_json(DEL_JSON, final_del)

    print(f"\n{'═'*64}")
    print(f"  ✅  rev.json: {len(final_live)}  |  deleted.json: {len(final_del)}")
    print(f"  📸  Debug screenshots → debug/screenshots/")
    print(f"{'═'*64}\n")

if __name__ == "__main__":
    asyncio.run(main())
