"""
scraper.py  –  Sathya Review Scraper  (self-debugging production build)

SELF-DEBUG SYSTEM
=================
Every branch scrape saves:
  debug/screenshots/<branch>_01_loaded.png
  debug/screenshots/<branch>_02_after_tab.png
  debug/screenshots/<branch>_03_after_sort.png
  debug/screenshots/<branch>_04_after_scroll.png
  debug/dom_dumps/<branch>.txt   (all selectors + their counts)

This tells you EXACTLY what the browser sees at every step in GH Actions.
Upload the debug/ folder as an artifact in scrape.yml to inspect it.

ARCHITECTURE
============
• Uses place_id URL with hl=en to force English UI
• Waits for real DOM elements before acting (not just time.sleep)
• Tab click: tries 8 different strategies including JS click
• Scroll: scrolls the inner feed div via JS evaluate
• Selector cascade: 6+ fallbacks for every field
• Self-heals: if 0 cards found, dumps DOM and retries with longer waits
"""

import asyncio
import hashlib
import json
import os
import random
import re
import sys
import traceback
from datetime import datetime, timedelta, timezone
from pathlib import Path

from playwright.async_api import async_playwright, TimeoutError as PWTimeout

# ── Paths ──────────────────────────────────────────────────────────────────────
ROOT        = Path(__file__).parent
DOCS_DIR    = ROOT / "docs"
DEBUG_DIR   = ROOT / "debug"
SS_DIR      = DEBUG_DIR / "screenshots"
DOM_DIR     = DEBUG_DIR / "dom_dumps"
REV_JSON    = DOCS_DIR / "rev.json"
DEL_JSON    = DOCS_DIR / "deleted.json"

for d in [DOCS_DIR, SS_DIR, DOM_DIR]:
    d.mkdir(parents=True, exist_ok=True)

# ── Config ─────────────────────────────────────────────────────────────────────
MAX_CONCURRENT    = 2
MAX_RETRIES       = 3
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
    "--disable-hang-monitor",
    "--window-size=1400,900",
    # Very important: don't use GPU in CI
    "--disable-gpu", "--disable-software-rasterizer",
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

def load_json(path: Path) -> list:
    if path.exists():
        try: return json.loads(path.read_text(encoding="utf-8"))
        except: return []
    return []

def save_json(path: Path, data: list) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")

def slug(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")

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
         "hour":   timedelta(hours=val),   "day":    timedelta(days=val),
         "week":   timedelta(weeks=val),   "month":  timedelta(days=30*val),
         "year":   timedelta(days=365*val)}
    return (ref - d.get(unit, timedelta())).strftime("%Y-%m-%d %H:%M:%S")


# ══════════════════════════════════════════════════════════════════════════════
# Self-debug: DOM dump
# ══════════════════════════════════════════════════════════════════════════════

async def dump_dom(page, branch_name: str, label: str):
    """Save a full selector inventory + screenshot for debugging."""
    sl = slug(branch_name)
    safe_label = re.sub(r"[^a-z0-9]+", "_", label.lower())

    # Screenshot
    try:
        await page.screenshot(
            path=str(SS_DIR / f"{sl}_{safe_label}.png"),
            full_page=False
        )
    except Exception:
        pass

    # DOM inventory
    lines = [f"=== DOM DUMP: {branch_name} | {label} ===",
             f"URL: {page.url}", f"Title: {await page.title()}", ""]

    check_sels = [
        "button", "div[role='tab']", "div[role='main']",
        "div[role='feed']", "h1", "h2",
        "div.jftiEf",           # review cards
        ".rsqaWe", ".DU9Pgb",  # timestamps
        "div.m6QErb",           # scroll container
        "div.m6QErb.DxyBCb",
        "span.F7nice",          # review count
        ".fontHeadlineSmall",   # reviewer names
        ".wiI7pd",              # review text
        "span[role='img'][aria-label]",  # stars
        ".kvMYJc",
        "[aria-label*='review' i]",
        "[aria-label*='star' i]",
        "button.w8nwRe",        # "more" button
        "div.section-scrollbox",
    ]

    lines.append("--- Element counts ---")
    for sel in check_sels:
        try:
            n = await page.locator(sel).count()
            if n > 0:
                lines.append(f"  {n:4d}  {sel}")
        except:
            pass

    # All buttons text
    lines.append("\n--- All buttons ---")
    try:
        btns = await page.locator("button").all()
        for b in btns[:30]:
            try:
                txt = (await b.inner_text(timeout=400)).strip().replace("\n", " ")[:80]
                lbl = (await b.get_attribute("aria-label", timeout=400) or "")[:80]
                jsn = (await b.get_attribute("jsaction", timeout=400) or "")[:60]
                lines.append(f"  txt={txt!r:50s}  aria={lbl!r:40s}  jsaction={jsn!r}")
            except:
                pass
    except:
        pass

    # All tabs
    lines.append("\n--- All [role=tab] ---")
    try:
        tabs = await page.locator("[role='tab']").all()
        for t in tabs:
            try:
                txt = (await t.inner_text(timeout=400)).strip().replace("\n", " ")[:80]
                lbl = (await t.get_attribute("aria-label", timeout=400) or "")[:80]
                lines.append(f"  txt={txt!r:50s}  aria={lbl!r}")
            except:
                pass
    except:
        pass

    # First 3 review cards if any
    lines.append("\n--- Review cards sample ---")
    try:
        cards = await page.locator("div.jftiEf").all()
        for card in cards[:3]:
            try:
                inner = (await card.inner_text(timeout=500)).replace("\n", " ")[:200]
                lines.append(f"  CARD: {inner!r}")
            except:
                pass
    except:
        pass

    dump_path = DOM_DIR / f"{sl}_{safe_label}.txt"
    dump_path.write_text("\n".join(lines), encoding="utf-8")


# ══════════════════════════════════════════════════════════════════════════════
# Browser setup
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
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        },
    )
    await ctx.add_init_script(STEALTH_JS)
    return ctx


async def wait_for_any(page, selectors: list[str], timeout: int = 15000) -> str | None:
    """Wait until any of the selectors appears. Returns the matched selector."""
    deadline = asyncio.get_event_loop().time() + timeout / 1000
    while asyncio.get_event_loop().time() < deadline:
        for sel in selectors:
            try:
                if await page.locator(sel).count() > 0:
                    return sel
            except:
                pass
        await asyncio.sleep(0.5)
    return None


# ══════════════════════════════════════════════════════════════════════════════
# Tab click: 10 strategies
# ══════════════════════════════════════════════════════════════════════════════

async def click_reviews_tab(page) -> bool:
    """Try every known strategy to click the Reviews tab."""

    # S1: div[role=tab] with text containing "review" (case-insensitive)
    try:
        tabs = await page.locator("div[role='tab']").all()
        for tab in tabs:
            try:
                txt = (await tab.inner_text(timeout=500)).lower()
                if "review" in txt:
                    await tab.click()
                    print("      → Tab clicked via S1 (role=tab text match)")
                    return True
            except:
                continue
    except:
        pass

    # S2: button[role=tab] text match
    try:
        tabs = await page.locator("button[role='tab']").all()
        for tab in tabs:
            try:
                txt = (await tab.inner_text(timeout=500)).lower()
                if "review" in txt:
                    await tab.click()
                    print("      → Tab clicked via S2 (button role=tab text match)")
                    return True
            except:
                continue
    except:
        pass

    # S3: aria-label contains "review"
    try:
        loc = page.locator("[aria-label*='review' i]").first
        if await loc.is_visible(timeout=3000):
            await loc.click()
            print("      → Tab clicked via S3 (aria-label contains review)")
            return True
    except:
        pass

    # S4: Any element whose text is exactly "Reviews"
    try:
        loc = page.get_by_text("Reviews", exact=True).first
        if await loc.is_visible(timeout=3000):
            await loc.click()
            print("      → Tab clicked via S4 (get_by_text exact)")
            return True
    except:
        pass

    # S5: Any element whose text contains "Reviews" and is tab-like
    try:
        for sel in ["button", "div", "span", "li"]:
            els = await page.locator(sel).all()
            for el in els[:50]:
                try:
                    txt = (await el.inner_text(timeout=200)).strip()
                    if re.match(r"^Reviews?(\s*\(\d+\))?$", txt, re.I):
                        await el.click()
                        print(f"      → Tab clicked via S5 ({sel} text={txt!r})")
                        return True
                except:
                    continue
    except:
        pass

    # S6: JS click on the first element with "Review" in innerText
    try:
        result = await page.evaluate("""
            () => {
                const all = document.querySelectorAll('[role="tab"], button, div.Gpq6kf');
                for (const el of all) {
                    if (el.innerText && el.innerText.toLowerCase().includes('review')) {
                        el.click();
                        return el.innerText;
                    }
                }
                return null;
            }
        """)
        if result:
            print(f"      → Tab clicked via S6 (JS evaluate, text={result.strip()!r})")
            return True
    except:
        pass

    # S7: Click the review count span (F7nice contains "X reviews")
    try:
        spans = await page.locator("span.F7nice, div.F7nice").all()
        for sp in spans:
            try:
                txt = (await sp.inner_text(timeout=300)).lower()
                if "review" in txt:
                    await sp.click()
                    print(f"      → Tab clicked via S7 (F7nice span {txt!r})")
                    return True
            except:
                continue
    except:
        pass

    # S8: Tab index 1 (Reviews is almost always the 2nd tab)
    try:
        tabs = await page.locator("[role='tab']").all()
        if len(tabs) >= 2:
            await tabs[1].click()
            print("      → Tab clicked via S8 (2nd tab by index)")
            return True
    except:
        pass

    # S9: Keyboard navigation – Tab key to focus + Enter
    try:
        await page.keyboard.press("Tab")
        await asyncio.sleep(0.3)
        await page.keyboard.press("Tab")
        await asyncio.sleep(0.3)
        await page.keyboard.press("Enter")
        print("      → Tab clicked via S9 (keyboard Tab+Enter)")
        return True
    except:
        pass

    print("      → All tab strategies failed")
    return False


# ══════════════════════════════════════════════════════════════════════════════
# Sort by Newest
# ══════════════════════════════════════════════════════════════════════════════

async def sort_by_newest(page) -> bool:
    sort_selectors = [
        "button[aria-label*='Sort' i]",
        "button[jsaction*='sort' i]",
        "button.g88MCb",
        "div[data-value='Sort']",
        "[jsaction*='pane.reviews.sorter']",
        # JS fallback
    ]

    clicked_sort = False
    for sel in sort_selectors:
        try:
            loc = page.locator(sel).first
            if await loc.is_visible(timeout=2000):
                await loc.click()
                clicked_sort = True
                break
        except:
            continue

    if not clicked_sort:
        # JS fallback: find button near reviews with sort icon
        try:
            clicked_sort = await page.evaluate("""
                () => {
                    const btns = document.querySelectorAll('button');
                    for (const b of btns) {
                        const lbl = (b.getAttribute('aria-label') || '').toLowerCase();
                        if (lbl.includes('sort')) { b.click(); return true; }
                    }
                    return false;
                }
            """)
        except:
            pass

    if not clicked_sort:
        return False

    await asyncio.sleep(random.uniform(1.0, 1.8))

    # Click "Newest" option
    newest_selectors = [
        "li[data-index='1']", "div[data-index='1']", "[data-index='1']",
        "li:nth-child(2)", "div.fxNQSd:nth-child(2)",
    ]
    for sel in newest_selectors:
        try:
            loc = page.locator(sel).first
            if await loc.is_visible(timeout=1500):
                await loc.click()
                return True
        except:
            continue

    # Text-based fallback
    try:
        loc = page.get_by_text("Newest", exact=True).first
        if await loc.is_visible(timeout=1500):
            await loc.click()
            return True
    except:
        pass

    return False


# ══════════════════════════════════════════════════════════════════════════════
# Find scrollable reviews panel
# ══════════════════════════════════════════════════════════════════════════════

async def find_panel(page):
    """Return JS element handle for the scrollable reviews panel."""
    for sel in [
        "div.m6QErb.DxyBCb",
        "div.m6QErb.WNBkOb",
        "div.m6QErb",
        "div[role='feed']",
        "div.section-scrollbox.XiKgde",
        "div.section-scrollbox",
    ]:
        try:
            loc = page.locator(sel).first
            if await loc.count():
                box = await loc.bounding_box()
                if box and box["height"] > 200:
                    return await loc.element_handle()
        except:
            continue

    # JS fallback: find the tallest scrollable div inside role=main
    try:
        handle = await page.evaluate_handle("""
            () => {
                const main = document.querySelector('[role="main"]');
                if (!main) return null;
                let best = null, bestH = 0;
                for (const el of main.querySelectorAll('div')) {
                    if (el.scrollHeight > el.clientHeight + 50 && el.clientHeight > 200) {
                        if (el.clientHeight > bestH) { best = el; bestH = el.clientHeight; }
                    }
                }
                return best;
            }
        """)
        obj = handle.as_element()
        if obj:
            return obj
    except:
        pass

    return None


# ══════════════════════════════════════════════════════════════════════════════
# Parse a single review card
# ══════════════════════════════════════════════════════════════════════════════

async def parse_card(card, branch: dict, snap_date: str, ref_time: datetime) -> dict | None:
    # Author
    author = "Unknown"
    for sel in [".d4r55", ".fontHeadlineSmall", ".jJc9Ad", ".Vpc5Fe", "button[aria-label*='photo']"]:
        try:
            el = card.locator(sel).first
            if await el.count():
                t = (await el.inner_text(timeout=800)).strip()
                if t and t != "Unknown":
                    author = t; break
        except:
            pass

    # Rating
    rating = 0.0
    for sel in ["span[role='img'][aria-label]", ".kvMYJc", ".hCCjke span[aria-label]",
                "span[aria-label*='star' i]", "span[aria-label*='Star']"]:
        try:
            el = card.locator(sel).first
            if await el.count():
                raw = (await el.get_attribute("aria-label", timeout=800)) or ""
                m = re.search(r"(\d+\.?\d*)", raw)
                if m:
                    rating = float(m.group(1)); break
        except:
            pass

    # Review text
    text = ""
    for sel in [".wiI7pd", "span.wiI7pd", ".MyEned span", ".Jtu6Td", ".review-full-text"]:
        try:
            el = card.locator(sel).first
            if await el.count():
                t = (await el.inner_text(timeout=800)).strip().replace("\n", " ")
                if t:
                    text = t; break
        except:
            pass

    # Timestamp
    rel_time = ""
    for sel in [".rsqaWe", ".DU9Pgb", "span.dehysf", "span[class*='dehysf']",
                ".xRkPPb", "span.y3Ibjb"]:
        try:
            el = card.locator(sel).first
            if await el.count():
                t = (await el.inner_text(timeout=800)).strip()
                if t:
                    rel_time = t; break
        except:
            pass

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
        "parsed_date":  rel_to_abs(rel_time, ref_time),
        "snap_date":    snap_date,
        "scraped_at":   ref_time.strftime("%Y-%m-%d %H:%M"),
    }


# ══════════════════════════════════════════════════════════════════════════════
# Core scrape: one branch
# ══════════════════════════════════════════════════════════════════════════════

async def _scrape_once(branch: dict, snap_date: str, attempt: int = 1) -> list[dict]:
    name     = branch["name"]
    place_id = branch["place_id"]
    sl       = slug(name)

    # hl=en forces English UI — critical for text-based selectors
    url = f"https://www.google.com/maps/place/?q=place_id:{place_id}&hl=en"

    reviews: list[dict] = []
    extra_wait = 3 * (attempt - 1)   # longer waits on retries

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=CHROMIUM_ARGS)
        try:
            ctx  = await make_context(browser)
            page = await ctx.new_page()

            # Block images/media to speed up load (keep JS/CSS/XHR)
            await page.route(
                re.compile(r"\.(png|jpg|jpeg|gif|webp|svg|ico|woff2?|ttf|otf|mp4|mp3)(\?.*)?$"),
                lambda route: route.abort()
            )

            # ── 1. Navigate ───────────────────────────────────────────────────
            print(f"    [{name}] Navigating… (attempt {attempt})", flush=True)
            try:
                await page.goto(url, wait_until="load", timeout=60_000)
            except PWTimeout:
                await page.goto(url, wait_until="domcontentloaded", timeout=60_000)

            # ── 2. Wait for Maps app to fully boot ────────────────────────────
            # Maps is a React/JS SPA — DOM is empty until JS runs
            # Wait for ANY of these indicators of a live sidebar
            sidebar_ready = await wait_for_any(page, [
                "div[role='main']",
                "h1.DUwDvf",
                "h1.fontHeadlineLarge",
                "span.F7nice",
                "div.F7nice",
                "button[aria-label*='review' i]",
                "div[role='tab']",
            ], timeout=25_000 + extra_wait * 1000)

            if not sidebar_ready:
                print(f"    [{name}] Sidebar not found after 25s — dumping DOM", flush=True)
                await dump_dom(page, name, "no_sidebar")
                return []

            # Extra boot time on retries
            await asyncio.sleep(random.uniform(3 + extra_wait, 5 + extra_wait))

            # Dismiss consent dialogs
            for consent_sel in [
                "#L2AGLb",                                       # "I agree"
                "button[aria-label*='Accept all' i]",
                "button[aria-label*='Reject all' i]",
                "[aria-label*='Before you continue'] button:last-child",
                "form[action*='consent'] button",
            ]:
                try:
                    b = page.locator(consent_sel).first
                    if await b.is_visible(timeout=1000):
                        await b.click()
                        await asyncio.sleep(1)
                        break
                except:
                    pass

            # Debug: what does the page look like after load?
            await dump_dom(page, name, "01_after_load")

            # ── 3. Click Reviews tab ──────────────────────────────────────────
            clicked = await click_reviews_tab(page)
            await asyncio.sleep(random.uniform(3 + extra_wait, 4 + extra_wait))

            await dump_dom(page, name, "02_after_tab_click")

            # ── 4. Sort by Newest ─────────────────────────────────────────────
            sorted_ok = await sort_by_newest(page)
            if sorted_ok:
                await asyncio.sleep(random.uniform(2, 3))

            await dump_dom(page, name, "03_after_sort")

            # ── 5. Find scroll panel ──────────────────────────────────────────
            panel = await find_panel(page)

            # ── 6. Scroll loop ────────────────────────────────────────────────
            prev_count   = 0
            stale_rounds = 0

            for round_idx in range(MAX_SCROLL_ROUNDS):
                # Expand "More" buttons
                for more_sel in ["button.w8nwRe", "button[aria-label='See more']", "button.lcr4fd"]:
                    try:
                        btns = page.locator(more_sel)
                        for i in range(await btns.count()):
                            try:
                                b = btns.nth(i)
                                if await b.is_visible(timeout=200):
                                    await b.click()
                                    await asyncio.sleep(0.15)
                            except:
                                pass
                    except:
                        pass

                # Check last timestamp — stop if beyond 23h window
                ts_loc = page.locator(".rsqaWe, .DU9Pgb, span.dehysf")
                ts_cnt = await ts_loc.count()
                if ts_cnt > 0:
                    try:
                        last_ts = (await ts_loc.nth(ts_cnt - 1).inner_text(timeout=500)).strip()
                        if last_ts and not is_within_23h(last_ts):
                            print(f"    [{name}] Reached old review at round {round_idx}: {last_ts!r}")
                            break
                    except:
                        pass

                # Scroll
                if panel:
                    try:
                        await page.evaluate("el => { el.scrollTop = el.scrollHeight; }", panel)
                    except:
                        panel = None  # panel handle stale, fall through to window scroll
                if not panel:
                    await page.evaluate("window.scrollBy(0, 3000)")

                await asyncio.sleep(random.uniform(2.0, 3.0))

                curr = await page.locator("div.jftiEf").count()
                if curr == prev_count:
                    stale_rounds += 1
                    if stale_rounds >= STALE_LIMIT:
                        break
                else:
                    stale_rounds = 0
                    prev_count   = curr

            await dump_dom(page, name, "04_after_scroll")

            # ── 7. Parse cards ────────────────────────────────────────────────
            ref  = ist_now()
            cards = await page.locator("div.jftiEf").all()
            print(f"    [{name}] {len(cards)} cards found", flush=True)

            if len(cards) == 0:
                # Self-heal: dump everything and flag for retry
                print(f"    [{name}] ⚠️  Zero cards — will retry with longer waits", flush=True)
                await dump_dom(page, name, "ZERO_CARDS")

            for card in cards:
                try:
                    rv = await parse_card(card, branch, snap_date, ref)
                    if rv:
                        reviews.append(rv)
                except:
                    continue

        except Exception as exc:
            print(f"    [{name}] ERROR: {exc!s:.150s}", flush=True)
            traceback.print_exc()
            try:
                await dump_dom(page, name, "EXCEPTION")
            except:
                pass
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
                # Zero results but no exception — retry with longer wait
                wait = 15 * attempt + random.uniform(5, 10)
                print(f"  ↺  {branch['name']} got 0 reviews, retry {attempt+1} in {wait:.0f}s…", flush=True)
                await asyncio.sleep(wait)
            except Exception as exc:
                wait = 15 * attempt + random.uniform(5, 10)
                if attempt <= MAX_RETRIES:
                    print(f"  ⚠️  {branch['name']} attempt {attempt} exception – retry in {wait:.0f}s")
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
            print(f"    ♻️  Reinstated: {item.get('branch_name')} – {item.get('author')}")

    scoped = {fp: v for fp, v in old_live_map.items() if v.get("snap_date") == snap_date}
    for fp, item in scoped.items():
        if fp not in fresh_fps and fp not in del_out:
            di = dict(item); di["deleted_on"] = now_str
            del_out[fp] = di; live_map.pop(fp, None)
            print(f"    🗑️  Deleted: {item.get('branch_name')} – {item.get('author')}")

    cutoff = ist_now() - timedelta(days=DELETION_DAYS)
    purged = 0
    for fp in list(del_out.keys()):
        try:
            d = datetime.strptime(del_out[fp].get("deleted_on",""), "%Y-%m-%d %H:%M").replace(tzinfo=IST)
            if d < cutoff: del_out.pop(fp); purged += 1
        except: pass
    if purged:
        print(f"    🧹 Purged {purged} old deletion(s)")

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
    print(f"  Python {sys.version.split()[0]}  |  debug → debug/")
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
            ex.update({"rel_time": r["rel_time"], "parsed_date": r["parsed_date"], "scraped_at": r["scraped_at"]})
            merged[fp] = ex; upd_c += 1

    print(f"  Merge: {new_c} new, {upd_c} refreshed")
    merged, merged_del = track_deletions(merged, old_del_map, old_live_map, fresh_fps, snap_date)

    final_live = sorted(merged.values(),     key=lambda x: (x.get("snap_date",""), x.get("parsed_date","")), reverse=True)
    final_del  = sorted(merged_del.values(), key=lambda x: x.get("deleted_on",""), reverse=True)

    save_json(REV_JSON, final_live)
    save_json(DEL_JSON, final_del)

    print(f"\n{'═'*64}")
    print(f"  ✅  rev.json: {len(final_live)}  |  deleted.json: {len(final_del)}")
    print(f"  📁  Debug artifacts saved to debug/")
    print(f"{'═'*64}\n")


if __name__ == "__main__":
    asyncio.run(main())
