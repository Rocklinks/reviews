"""
scraper.py – Sathya Review Scraper (definitive production build)

ROOT CAUSE FIX
==============
Previous versions failed because:
1. wait_until="domcontentloaded" fires BEFORE Maps JS boots → empty page
2. The search URL doesn't auto-open the sidebar in headless Chrome
3. Tab selectors were wrong — Maps renders tabs as <div role="tab"> inside
   a specific parent, AND the aria-label is locale-dependent

CORRECT STRATEGY
================
1. Navigate to the DIRECT place URL (maps/place/?q=place_id:XXX)
   with wait_until="load" + explicit wait for the JS app to mount
2. Wait for the place name heading to confirm the sidebar is loaded
3. Find the Reviews tab by scanning ALL div[role="tab"] elements and
   matching text content (not aria-label, which is locale-dependent)
4. Sort by Newest, then scroll the inner panel div
5. Parse cards using multiple selector fallbacks

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
import traceback
from datetime import datetime, timedelta, timezone
from pathlib import Path

from playwright.async_api import async_playwright, TimeoutError as PWTimeout

# ── Paths ──────────────────────────────────────────────────────────────────────
DOCS_DIR = Path(__file__).parent / "docs"
REV_JSON  = DOCS_DIR / "rev.json"
DEL_JSON  = DOCS_DIR / "deleted.json"
DOCS_DIR.mkdir(parents=True, exist_ok=True)

# ── Config ─────────────────────────────────────────────────────────────────────
MAX_CONCURRENT    = 2
MAX_RETRIES       = 2
MAX_SCROLL_ROUNDS = 40
STALE_LIMIT       = 4    # stop scrolling after N rounds with no new cards
DELETION_DAYS     = 30

IST = timezone(timedelta(hours=5, minutes=30))

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36 Edg/123.0.0.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
]

CHROMIUM_ARGS = [
    "--no-sandbox",
    "--disable-setuid-sandbox",
    "--disable-blink-features=AutomationControlled",
    "--disable-infobars",
    "--disable-extensions",
    "--disable-dev-shm-usage",
    "--no-first-run",
    "--disable-default-apps",
    "--mute-audio",
    "--disable-translate",
    "--disable-sync",
    "--disable-background-networking",
    "--disable-client-side-phishing-detection",
    "--disable-hang-monitor",
    "--disable-prompt-on-repost",
    "--window-size=1280,800",
]

STEALTH_JS = """
Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
Object.defineProperty(navigator, 'plugins',   {get: () => [1,2,3,4,5]});
Object.defineProperty(navigator, 'languages', {get: () => ['en-US','en']});
window.chrome = {runtime: {}};
try {
    const orig = window.navigator.permissions.query;
    window.navigator.permissions.query = p =>
        p.name === 'notifications'
            ? Promise.resolve({state: Notification.permission})
            : orig(p);
} catch(_) {}
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
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return []
    return []

def save_json(path: Path, data: list) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")

# ── Relative-time helpers ──────────────────────────────────────────────────────

_WITHIN_23H = re.compile(
    r"^(?:just now|a moment ago|moments? ago)$"
    r"|^(\d+)\s*(second|minute|hour)s?\s*ago$",
    re.IGNORECASE,
)

def is_within_23h(rel: str) -> bool:
    rel = (rel or "").strip()
    m = _WITHIN_23H.match(rel)
    if not m:
        return False
    if not m.group(1):
        return True   # "just now" family
    val  = int(m.group(1))
    unit = m.group(2).lower()
    if unit == "second": return True
    if unit == "minute": return val <= 1380   # 23 * 60
    if unit == "hour":   return val <= 23
    return False

def rel_to_abs(rel: str, ref: datetime) -> str:
    rel = (rel or "").strip().lower()
    if not rel or "just now" in rel or "moment" in rel:
        return ref.strftime("%Y-%m-%d %H:%M:%S")
    m = re.search(r"(\d+)\s*(second|minute|hour|day|week|month|year)", rel)
    if not m:
        return ref.strftime("%Y-%m-%d %H:%M:%S")
    val, unit = int(m.group(1)), m.group(2)
    d = {"second": timedelta(seconds=val), "minute": timedelta(minutes=val),
         "hour": timedelta(hours=val), "day": timedelta(days=val),
         "week": timedelta(weeks=val), "month": timedelta(days=30*val),
         "year": timedelta(days=365*val)}
    return (ref - d.get(unit, timedelta())).strftime("%Y-%m-%d %H:%M:%S")


# ══════════════════════════════════════════════════════════════════════════════
# Browser helpers
# ══════════════════════════════════════════════════════════════════════════════

async def make_context(browser):
    ctx = await browser.new_context(
        viewport=random.choice([
            {"width": 1280, "height": 800},
            {"width": 1366, "height": 768},
            {"width": 1440, "height": 900},
        ]),
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

async def try_click(page, selectors: list[str], timeout=4000) -> bool:
    """Try a list of selectors, click the first visible one. Returns True on success."""
    for sel in selectors:
        try:
            loc = page.locator(sel).first
            if await loc.is_visible(timeout=timeout):
                await loc.click()
                return True
        except Exception:
            continue
    return False

async def get_text(locator, timeout=2000) -> str:
    try:
        if await locator.count():
            return (await locator.first.inner_text(timeout=timeout)).strip()
    except Exception:
        pass
    return ""

async def get_attr(locator, attr: str, timeout=2000) -> str:
    try:
        if await locator.count():
            v = await locator.first.get_attribute(attr, timeout=timeout)
            return (v or "").strip()
    except Exception:
        pass
    return ""


# ══════════════════════════════════════════════════════════════════════════════
# KEY FIX: Find the Reviews tab by scanning tab text content
# ══════════════════════════════════════════════════════════════════════════════

async def click_reviews_tab(page) -> bool:
    """
    Google Maps renders navigation tabs as div[role='tab'].
    The aria-label is unreliable across locales.
    Instead, scan ALL tabs and match by text content containing 'review'.
    """
    # Strategy 1: text-content match on all tabs
    try:
        tabs = await page.locator('div[role="tab"], button[role="tab"]').all()
        for tab in tabs:
            try:
                text = (await tab.inner_text(timeout=1000)).strip().lower()
                if "review" in text:
                    await tab.click()
                    return True
            except Exception:
                continue
    except Exception:
        pass

    # Strategy 2: aria-label contains "review" (case-insensitive)
    try:
        loc = page.locator('[role="tab"][aria-label*="review" i], [role="tab"][aria-label*="Review"]').first
        if await loc.is_visible(timeout=3000):
            await loc.click()
            return True
    except Exception:
        pass

    # Strategy 3: button with text "Reviews" anywhere
    try:
        loc = page.locator('button').filter(has_text=re.compile(r"^Reviews$", re.I)).first
        if await loc.is_visible(timeout=3000):
            await loc.click()
            return True
    except Exception:
        pass

    # Strategy 4: any element with aria-label exactly matching review count pattern
    # Google often renders it as "123 reviews"
    try:
        loc = page.locator('[aria-label*="reviews" i]').first
        if await loc.is_visible(timeout=3000):
            await loc.click()
            return True
    except Exception:
        pass

    return False


async def sort_by_newest(page) -> bool:
    """Click the sort button and choose Newest."""
    # Find and click sort button
    sort_clicked = await try_click(page, [
        'button[aria-label*="Sort" i]',
        'button[jsaction*="sort" i]',
        'button.g88MCb',
        'div[data-value="Sort"]',
        # fallback: any button near reviews with a sort icon
        'div[role="main"] button:has(span.google-symbols)',
    ], timeout=3000)

    if not sort_clicked:
        return False

    await asyncio.sleep(random.uniform(0.8, 1.5))

    # Click "Newest" option - it's always index 1 in the dropdown
    for sel in [
        'li[data-index="1"]',
        'div[data-index="1"]',
        '[data-index="1"]',
        # text match fallback
        'li:has-text("Newest")',
        'div:has-text("Newest")',
        'span:has-text("Newest")',
    ]:
        try:
            loc = page.locator(sel).first
            if await loc.is_visible(timeout=2000):
                await loc.click()
                return True
        except Exception:
            continue

    return False


async def find_scroll_panel(page):
    """
    Find the scrollable reviews container.
    Returns the element handle or None.
    """
    for sel in [
        "div.m6QErb.DxyBCb",           # most common
        "div.m6QErb[aria-label]",
        "div[role='feed']",
        "div.section-scrollbox.XiKgde",
        "div.section-scrollbox",
        "div.m6QErb",
    ]:
        try:
            loc = page.locator(sel).first
            if await loc.count():
                box = await loc.bounding_box()
                if box and box["height"] > 100:  # must be a real visible panel
                    return await loc.element_handle()
        except Exception:
            continue
    return None


# ══════════════════════════════════════════════════════════════════════════════
# Core scraper
# ══════════════════════════════════════════════════════════════════════════════

async def _scrape_once(branch: dict, snap_date: str) -> list[dict]:
    name     = branch["name"]
    place_id = branch["place_id"]

    # ── URL STRATEGY ───────────────────────────────────────────────────────────
    # Use the place_id URL which loads the sidebar directly.
    # We try THREE URL patterns and use whichever loads the sidebar.
    urls = [
        # Pattern 1: Direct place_id lookup (most reliable)
        f"https://www.google.com/maps/place/?q=place_id:{place_id}",
        # Pattern 2: Maps search with place_id
        f"https://www.google.com/maps/search/?api=1&query_place_id={place_id}",
    ]

    reviews: list[dict] = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=CHROMIUM_ARGS)
        try:
            ctx  = await make_context(browser)
            page = await ctx.new_page()

            # Abort image/font/media requests – speeds up load significantly
            await page.route(
                "**/*.{png,jpg,jpeg,gif,webp,svg,ico,woff,woff2,ttf,otf,mp4,mp3,pdf}",
                lambda route: route.abort()
            )

            sidebar_loaded = False

            for url in urls:
                try:
                    await page.goto(url, wait_until="load", timeout=50_000)
                except Exception:
                    try:
                        await page.goto(url, wait_until="domcontentloaded", timeout=50_000)
                    except Exception:
                        continue

                # Give Maps JS time to fully bootstrap
                await asyncio.sleep(random.uniform(4, 6))

                # ── Dismiss consent dialogs ───────────────────────────────────
                for consent_sel in [
                    'button[aria-label*="Accept all"]',
                    'button[aria-label*="Reject all"]',
                    '[aria-label*="Before you continue"] button:last-child',
                    'form:has(button[jsname]) button:first-child',
                    '#L2AGLb',   # Google's "I agree" button id
                ]:
                    try:
                        b = page.locator(consent_sel).first
                        if await b.is_visible(timeout=1500):
                            await b.click()
                            await asyncio.sleep(1)
                            break
                    except Exception:
                        pass

                # ── Wait for the sidebar to mount ─────────────────────────────
                # The sidebar always contains an h1 with the place name
                # OR a div with role="main"
                sidebar_selectors = [
                    'div[role="main"]',
                    'h1.DUwDvf',           # place name heading
                    'h1.fontHeadlineLarge',
                    'div.bJzME',
                    'div[jsaction*="pane"]',
                    # Review count is always present in the sidebar
                    'span.F7nice',
                    'div.F7nice',
                ]
                for s_sel in sidebar_selectors:
                    try:
                        await page.wait_for_selector(s_sel, timeout=15_000)
                        sidebar_loaded = True
                        break
                    except PWTimeout:
                        continue

                if sidebar_loaded:
                    break

            if not sidebar_loaded:
                print(f"    [FAIL] {name}: sidebar never loaded on any URL", flush=True)
                return []

            await asyncio.sleep(random.uniform(1, 2))

            # ── Click Reviews tab ─────────────────────────────────────────────
            clicked = await click_reviews_tab(page)
            if not clicked:
                # Last resort: look for the reviews count button and click it
                try:
                    loc = page.locator("span.F7nice, div.F7nice").first
                    if await loc.is_visible(timeout=3000):
                        await loc.click()
                        clicked = True
                except Exception:
                    pass

            if clicked:
                await asyncio.sleep(random.uniform(2.5, 3.5))
            else:
                print(f"    [WARN] {name}: Reviews tab not clicked, proceeding anyway", flush=True)

            # ── Sort by newest ────────────────────────────────────────────────
            await sort_by_newest(page)
            await asyncio.sleep(random.uniform(2, 3))

            # ── Find scroll panel ─────────────────────────────────────────────
            panel_handle = await find_scroll_panel(page)
            if not panel_handle:
                print(f"    [WARN] {name}: scroll panel not found, using keyboard scroll", flush=True)

            # ── Scroll loop ───────────────────────────────────────────────────
            prev_count   = 0
            stale_rounds = 0

            for _round in range(MAX_SCROLL_ROUNDS):
                # Expand "More" / "See more" buttons
                for more_sel in [
                    "button.w8nwRe",
                    'button[aria-label="See more"]',
                    "button.lcr4fd",
                    'span:has-text("More"):not(button)',
                ]:
                    btns = page.locator(more_sel)
                    n    = await btns.count()
                    for i in range(n):
                        try:
                            b = btns.nth(i)
                            if await b.is_visible(timeout=300):
                                await b.click()
                                await asyncio.sleep(0.2)
                        except Exception:
                            pass

                # Check last timestamp → stop if past the 23 h window
                ts_loc  = page.locator(".rsqaWe, .DU9Pgb, span.dehysf")
                ts_cnt  = await ts_loc.count()
                if ts_cnt > 0:
                    try:
                        last_ts = (await ts_loc.nth(ts_cnt - 1).inner_text(timeout=800)).strip()
                        if last_ts and not is_within_23h(last_ts):
                            break
                    except Exception:
                        pass

                # Scroll
                if panel_handle:
                    try:
                        await page.evaluate(
                            "el => { el.scrollTop = el.scrollHeight; }",
                            panel_handle,
                        )
                    except Exception:
                        await page.keyboard.press("End")
                else:
                    await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")

                await asyncio.sleep(random.uniform(2.0, 3.2))

                curr = await page.locator("div.jftiEf").count()
                if curr == prev_count:
                    stale_rounds += 1
                    if stale_rounds >= STALE_LIMIT:
                        break
                else:
                    stale_rounds = 0
                    prev_count   = curr

            # ── Parse cards ───────────────────────────────────────────────────
            now   = ist_now()
            cards = await page.locator("div.jftiEf").all()
            print(f"    [{name}] {len(cards)} total cards in DOM", flush=True)

            for card in cards:
                try:
                    # Author
                    author = "Unknown"
                    for a_sel in [".d4r55", ".fontHeadlineSmall", ".jJc9Ad", ".Vpc5Fe"]:
                        t = await get_text(card.locator(a_sel))
                        if t:
                            author = t
                            break

                    # Rating (look for aria-label like "4 stars" or "Rated 5.0 out of 5")
                    rating = 0.0
                    for r_sel in [
                        "span[role='img'][aria-label]",
                        ".kvMYJc",
                        "span[aria-label*='star' i]",
                        "span[aria-label*='Star']",
                        ".hCCjke span",
                    ]:
                        raw = await get_attr(card.locator(r_sel), "aria-label")
                        if raw:
                            m = re.search(r"(\d+\.?\d*)", raw)
                            if m:
                                rating = float(m.group(1))
                                break

                    # Review text
                    text = ""
                    for t_sel in [".wiI7pd", "span.wiI7pd", ".MyEned span", ".Jtu6Td"]:
                        t = await get_text(card.locator(t_sel))
                        if t:
                            text = t.replace("\n", " ").strip()
                            break

                    # Relative timestamp
                    rel_time = ""
                    for ts_sel in [".rsqaWe", ".DU9Pgb", "span.dehysf", "span[class*='dehysf']"]:
                        t = await get_text(card.locator(ts_sel))
                        if t:
                            rel_time = t
                            break

                    # Filter: within 23 hours only
                    if not is_within_23h(rel_time):
                        continue

                    fp          = make_fp(rating, author, text)
                    parsed_date = rel_to_abs(rel_time, now)

                    reviews.append({
                        "fingerprint": fp,
                        "branch_id":   branch["id"],
                        "branch_name": branch["name"],
                        "agm":         branch["agm"],
                        "author":      author,
                        "rating":      rating,
                        "text":        text,
                        "rel_time":    rel_time,
                        "parsed_date": parsed_date,
                        "snap_date":   snap_date,
                        "scraped_at":  now.strftime("%Y-%m-%d %H:%M"),
                    })

                except Exception:
                    continue

        except Exception as exc:
            print(f"    [ERROR] {name}: {exc!s:.120s}", flush=True)
            traceback.print_exc()
        finally:
            await browser.close()

    return reviews


async def scrape_branch(branch: dict, sem: asyncio.Semaphore, snap_date: str) -> list[dict]:
    async with sem:
        for attempt in range(1, MAX_RETRIES + 2):
            try:
                result = await _scrape_once(branch, snap_date)
                icon   = "✅" if result else "⚪"
                print(f"  {icon} {branch['name']:22s} → {len(result):3d} review(s)", flush=True)
                return result
            except Exception as exc:
                wait = 12 * attempt + random.uniform(3, 8)
                if attempt <= MAX_RETRIES:
                    print(f"  ⚠️  {branch['name']} attempt {attempt} failed ({exc!s:.60s}) – retry in {wait:.0f}s")
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

    # Reinstatements
    for fp in list(del_out.keys()):
        if fp in fresh_fps:
            item = dict(del_out.pop(fp))
            item.pop("deleted_on", None)
            item["reinstated_on"] = now_str
            live_map[fp] = item
            print(f"    ♻️  Reinstated: {item.get('branch_name')} – {item.get('author')}")

    # Deletions (scoped to current snap_date only)
    scoped = {fp: v for fp, v in old_live_map.items() if v.get("snap_date") == snap_date}
    for fp, item in scoped.items():
        if fp not in fresh_fps and fp not in del_out:
            di = dict(item)
            di["deleted_on"] = now_str
            del_out[fp] = di
            live_map.pop(fp, None)
            print(f"    🗑️  Deleted: {item.get('branch_name')} – {item.get('author')}")

    # Purge old deletion records
    cutoff = ist_now() - timedelta(days=DELETION_DAYS)
    purged = 0
    for fp in list(del_out.keys()):
        try:
            d = datetime.strptime(del_out[fp].get("deleted_on", ""), "%Y-%m-%d %H:%M").replace(tzinfo=IST)
            if d < cutoff:
                del_out.pop(fp); purged += 1
        except Exception:
            pass
    if purged:
        print(f"    🧹 Purged {purged} old deletion(s)")

    return live_map, del_out


# ══════════════════════════════════════════════════════════════════════════════
# Main
# ══════════════════════════════════════════════════════════════════════════════

async def main():
    slot      = get_run_slot()
    snap_date = snap_date_for_slot(slot)

    print(f"\n{'═'*62}")
    print(f"  Sathya Review Scraper")
    print(f"  Slot: {slot}  |  snap_date: {snap_date}")
    print(f"  IST:  {ist_now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'═'*62}\n")

    old_live_map = {r["fingerprint"]: r for r in load_json(REV_JSON)}
    old_del_map  = {r["fingerprint"]: r for r in load_json(DEL_JSON)}
    print(f"  Loaded: {len(old_live_map)} live, {len(old_del_map)} deleted\n")

    sem     = asyncio.Semaphore(MAX_CONCURRENT)
    tasks   = [scrape_branch(b, sem, snap_date) for b in BRANCHES]
    batches = await asyncio.gather(*tasks)

    # Flatten + dedup within run
    fresh_map: dict[str, dict] = {}
    for batch in batches:
        for r in batch:
            if r["fingerprint"] not in fresh_map:
                fresh_map[r["fingerprint"]] = r

    fresh_fps = set(fresh_map.keys())
    print(f"\n  This run: {len(fresh_fps)} unique reviews within 23h\n")

    # Merge
    merged = dict(old_live_map)
    new_c = updated_c = 0
    for fp, r in fresh_map.items():
        if fp not in merged:
            merged[fp] = r; new_c += 1
        else:
            existing = dict(merged[fp])
            existing.update({"rel_time": r["rel_time"], "parsed_date": r["parsed_date"], "scraped_at": r["scraped_at"]})
            merged[fp] = existing; updated_c += 1

    print(f"  Merge: {new_c} new, {updated_c} refreshed")

    merged, merged_del = track_deletions(merged, old_del_map, old_live_map, fresh_fps, snap_date)

    final_live = sorted(merged.values(),     key=lambda x: (x.get("snap_date",""), x.get("parsed_date","")), reverse=True)
    final_del  = sorted(merged_del.values(), key=lambda x: x.get("deleted_on",""), reverse=True)

    save_json(REV_JSON, final_live)
    save_json(DEL_JSON, final_del)

    print(f"\n{'═'*62}")
    print(f"  ✅  rev.json: {len(final_live)}  |  deleted.json: {len(final_del)}")
    print(f"{'═'*62}\n")


if __name__ == "__main__":
    asyncio.run(main())
