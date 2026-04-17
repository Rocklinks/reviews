"""
scraper.py – Sathya Review Scraper  (complete production rewrite)

HOW IT WORKS
============
1. Opens Google Maps for each branch using the place_id search URL
2. Clicks the "Reviews" tab inside the sidebar panel
3. Sorts reviews by "Newest"
4. Scrolls the reviews panel (not the window!) until a review older than
   23 hours appears – then stops
5. Parses every card that qualifies (just now → 23 hours ago)
6. Deduplicates using a SHA-256 fingerprint of (rating, author, text)
7. Merges with existing rev.json without creating duplicates
8. Detects deleted reviews (were live yesterday, gone now) → deleted.json
9. Purges deleted.json entries older than 30 days

RUN SLOTS  (GitHub Actions cron, UTC → IST)
===========================================
  00:30 UTC → 06:00 IST  "morning"   snap_date = today
  06:30 UTC → 12:00 IST  "noon"      snap_date = today
  12:30 UTC → 18:00 IST  "evening"   snap_date = today
  18:30 UTC → 00:00 IST  "midnight"  snap_date = YESTERDAY
    ↑ The midnight run fires just after midnight IST, so reviews scraped
      then (posted during the previous evening) are dated to yesterday.
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
REV_JSON = DOCS_DIR / "rev.json"
DEL_JSON = DOCS_DIR / "deleted.json"
DOCS_DIR.mkdir(parents=True, exist_ok=True)

# ── Tuning knobs ───────────────────────────────────────────────────────────────
MAX_CONCURRENT    = 2    # parallel browsers (safe for GH Actions 7GB RAM)
MAX_RETRIES       = 2    # retry count before giving up on a branch
SCROLL_PAUSE_MIN  = 2.0  # seconds between scroll steps
SCROLL_PAUSE_MAX  = 3.5
MAX_SCROLL_ROUNDS = 35   # hard limit; stops earlier if old reviews found
STALE_LIMIT       = 3    # stop if card count doesn't grow for this many rounds
DELETION_DAYS     = 30   # keep deleted records for this many days

# ── Time zone ──────────────────────────────────────────────────────────────────
IST = timezone(timedelta(hours=5, minutes=30))

# ── Browser fingerprint pool ───────────────────────────────────────────────────
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36 Edg/123.0.0.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
]
VIEWPORTS = [
    {"width": 1280, "height": 800},
    {"width": 1366, "height": 768},
    {"width": 1440, "height": 900},
    {"width": 1920, "height": 1080},
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
]
# JS injected into every page to hide automation signals
STEALTH_JS = """
Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
Object.defineProperty(navigator, 'plugins',   {get: () => [1,2,3,4,5]});
Object.defineProperty(navigator, 'languages', {get: () => ['en-US','en']});
window.chrome = {runtime: {}};
try {
    const origQuery = window.navigator.permissions.query;
    window.navigator.permissions.query = p =>
        p.name === 'notifications'
            ? Promise.resolve({state: Notification.permission})
            : origQuery(p);
} catch(_) {}
"""

# ── Branch master data ─────────────────────────────────────────────────────────
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
    slots = {0: "morning", 6: "noon", 12: "evening", 18: "midnight"}
    if h in slots:
        return slots[h]
    ih = ist_now().hour
    if 5  <= ih < 11: return "morning"
    if 11 <= ih < 17: return "noon"
    if 17 <= ih < 23: return "evening"
    return "midnight"

def snap_date_for_slot(slot: str) -> str:
    return ist_yesterday() if slot == "midnight" else ist_today()

def make_fingerprint(rating: float, author: str, text: str) -> str:
    raw = f"{round(rating, 1)}|{(author or '').lower().strip()[:40]}|{(text or '').lower().strip()[:200]}"
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

# Matches: "just now", "a moment ago", "5 seconds ago", "3 minutes ago", "2 hours ago"
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
    if not m.group(1):      # "just now" family
        return True
    val  = int(m.group(1))
    unit = m.group(2).lower()
    if unit == "second": return True
    if unit == "minute": return val <= 1380   # 23*60
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
    d = {
        "second": timedelta(seconds=val),
        "minute": timedelta(minutes=val),
        "hour":   timedelta(hours=val),
        "day":    timedelta(days=val),
        "week":   timedelta(weeks=val),
        "month":  timedelta(days=30 * val),
        "year":   timedelta(days=365 * val),
    }
    return (ref - d.get(unit, timedelta())).strftime("%Y-%m-%d %H:%M:%S")


# ══════════════════════════════════════════════════════════════════════════════
# Playwright helpers
# ══════════════════════════════════════════════════════════════════════════════

async def new_stealth_context(browser):
    ctx = await browser.new_context(
        viewport=random.choice(VIEWPORTS),
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

async def safe_click(locator, timeout=3000):
    try:
        if await locator.is_visible(timeout=timeout):
            await locator.click()
            return True
    except Exception:
        pass
    return False

async def safe_text(locator, timeout=2000) -> str:
    try:
        if await locator.count():
            return (await locator.first.inner_text(timeout=timeout)).strip()
    except Exception:
        pass
    return ""

async def safe_attr(locator, attr: str, timeout=2000) -> str:
    try:
        if await locator.count():
            return (await locator.first.get_attribute(attr, timeout=timeout)) or ""
    except Exception:
        pass
    return ""


# ══════════════════════════════════════════════════════════════════════════════
# Core scraper – one branch, one headless browser session
# ══════════════════════════════════════════════════════════════════════════════

async def _scrape_once(branch: dict, snap_date: str) -> list[dict]:
    """
    Full end-to-end scrape of one branch.
    Returns a list of review dicts for reviews posted within the last 23 hours.
    """
    place_id = branch["place_id"]
    name     = branch["name"]

    # This URL reliably opens the correct place card in the Maps sidebar
    url = (
        f"https://www.google.com/maps/search/?api=1"
        f"&query={name.replace(' ', '+')}"
        f"&query_place_id={place_id}"
    )

    reviews: list[dict] = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=CHROMIUM_ARGS)
        try:
            ctx  = await new_stealth_context(browser)
            page = await ctx.new_page()

            # ── Navigate ──────────────────────────────────────────────────────
            await page.goto(url, wait_until="domcontentloaded", timeout=45_000)
            await asyncio.sleep(random.uniform(3, 5))

            # Dismiss GDPR / cookie consent
            for sel in [
                'button[aria-label*="Accept all"]',
                'button[aria-label*="Reject all"]',
                '[aria-label="Before you continue to Google Maps"] button:last-child',
                'form button:last-child',
            ]:
                try:
                    b = page.locator(sel).first
                    if await b.is_visible(timeout=1500):
                        await b.click()
                        await asyncio.sleep(1)
                        break
                except Exception:
                    pass

            # ── Wait for the sidebar / place panel ────────────────────────────
            try:
                await page.wait_for_selector(
                    'div[role="main"], div.bJzME, div[aria-label*="Results for"]',
                    timeout=20_000,
                )
            except PWTimeout:
                print(f"    [WARN] {name}: place panel never appeared")
                return []

            await asyncio.sleep(random.uniform(1, 2))

            # ── Click the Reviews tab ─────────────────────────────────────────
            # Google Maps renders tabs as buttons with aria-label="Reviews" or
            # as div[role="tab"] – try both
            clicked_tab = False
            for sel in [
                'button[aria-label*="Reviews"]',
                'div[role="tab"][aria-label*="Reviews"]',
                'button:has-text("Reviews")',
                'div[role="tab"]:has-text("Reviews")',
            ]:
                try:
                    tab = page.locator(sel).first
                    if await tab.is_visible(timeout=4000):
                        await tab.click()
                        clicked_tab = True
                        await asyncio.sleep(random.uniform(2.5, 3.5))
                        break
                except Exception:
                    continue

            if not clicked_tab:
                print(f"    [WARN] {name}: Reviews tab not found, attempting to continue")

            # ── Sort by Newest ────────────────────────────────────────────────
            for sort_sel in [
                'button[aria-label*="Sort reviews"]',
                'button[data-value*="sort"]',
                'button.g88MCb',
                '[jsaction*="pane.reviews.sort"]',
            ]:
                try:
                    btn = page.locator(sort_sel).first
                    if await btn.is_visible(timeout=3000):
                        await btn.click()
                        await asyncio.sleep(random.uniform(1, 1.5))

                        # Pick "Newest" from the dropdown (index 1 in Google's list)
                        for newest_sel in [
                            '[data-index="1"]',
                            'li[data-index="1"]',
                            'div[data-index="1"]',
                        ]:
                            newest = page.locator(newest_sel).first
                            if await newest.is_visible(timeout=2000):
                                await newest.click()
                                await asyncio.sleep(random.uniform(2, 3))
                                break
                        break
                except Exception:
                    continue

            # ── Find the scrollable reviews container ─────────────────────────
            # CRITICAL: Google Maps reviews are in a scrollable div, NOT the page
            panel_handle = None
            for panel_sel in [
                "div.m6QErb.DxyBCb",
                "div.m6QErb[aria-label]",
                "div[role='feed']",
                "div.section-scrollbox.XiKgde",
                "div.section-scrollbox",
            ]:
                try:
                    el = page.locator(panel_sel).first
                    if await el.count():
                        panel_handle = await el.element_handle()
                        break
                except Exception:
                    continue

            if not panel_handle:
                print(f"    [WARN] {name}: scrollable reviews panel not found, using page scroll")

            # ── Scroll + expand loop ──────────────────────────────────────────
            prev_count   = 0
            stale_rounds = 0

            for round_idx in range(MAX_SCROLL_ROUNDS):

                # Expand "More" buttons (full review text)
                for more_sel in [
                    "button.w8nwRe",           # common Maps CSS class
                    'button[aria-label="See more"]',
                    "button.lcr4fd",
                ]:
                    btns = page.locator(more_sel)
                    for i in range(await btns.count()):
                        try:
                            b = btns.nth(i)
                            if await b.is_visible(timeout=400):
                                await b.click()
                                await asyncio.sleep(0.25)
                        except Exception:
                            pass

                # Check the last visible timestamp – stop if past 23-hour window
                ts_locator = page.locator(".rsqaWe, .DU9Pgb, span.dehysf")
                ts_count   = await ts_locator.count()
                if ts_count > 0:
                    last_ts = await safe_text(ts_locator.nth(ts_count - 1))
                    if last_ts and not is_within_23h(last_ts):
                        # Found a review older than 23 h → all reviews after this
                        # point are also old → stop scrolling
                        break

                # Scroll the panel (preferred) or fall back to page
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

                await asyncio.sleep(random.uniform(SCROLL_PAUSE_MIN, SCROLL_PAUSE_MAX))

                # Stale detection
                curr_count = await page.locator("div.jftiEf").count()
                if curr_count == prev_count:
                    stale_rounds += 1
                    if stale_rounds >= STALE_LIMIT:
                        break
                else:
                    stale_rounds = 0
                    prev_count   = curr_count

            # ── Parse cards ───────────────────────────────────────────────────
            now   = ist_now()
            cards = await page.locator("div.jftiEf").all()
            print(f"    [{name}] {len(cards)} cards in DOM", flush=True)

            for card in cards:
                try:
                    # — Author —
                    author = "Unknown"
                    for a_sel in [".d4r55", ".fontHeadlineSmall", ".jJc9Ad", ".Vpc5Fe"]:
                        t = await safe_text(card.locator(a_sel))
                        if t:
                            author = t
                            break

                    # — Rating —
                    rating = 0.0
                    for r_sel in [
                        "span[role='img'][aria-label]",
                        ".kvMYJc",
                        "span.hCCjke span[aria-label]",
                        "span[aria-label*='star']",
                        "span[aria-label*='Star']",
                    ]:
                        raw = await safe_attr(card.locator(r_sel), "aria-label")
                        if not raw:
                            # try inner text as fallback for numeric stars
                            raw = await safe_text(card.locator(r_sel))
                        m = re.search(r"(\d+\.?\d*)", raw)
                        if m:
                            rating = float(m.group(1))
                            break

                    # — Review text —
                    text = ""
                    for t_sel in [".wiI7pd", "span.wiI7pd", ".MyEned span", ".Jtu6Td"]:
                        t = await safe_text(card.locator(t_sel))
                        if t:
                            text = t.replace("\n", " ").strip()
                            break

                    # — Relative timestamp —
                    rel_time = ""
                    for ts_sel in [".rsqaWe", ".DU9Pgb", "span.dehysf", "span[class*='dehysf']"]:
                        t = await safe_text(card.locator(ts_sel))
                        if t:
                            rel_time = t
                            break

                    # ── Only keep reviews within 23 hours ─────────────────────
                    if not is_within_23h(rel_time):
                        continue

                    fp          = make_fingerprint(rating, author, text)
                    parsed_date = rel_to_abs(rel_time, now)

                    reviews.append({
                        "fingerprint":  fp,
                        "branch_id":    branch["id"],
                        "branch_name":  branch["name"],
                        "agm":          branch["agm"],
                        "author":       author,
                        "rating":       rating,
                        "text":         text,
                        "rel_time":     rel_time,
                        "parsed_date":  parsed_date,
                        "snap_date":    snap_date,
                        "scraped_at":   now.strftime("%Y-%m-%d %H:%M"),
                    })

                except Exception:
                    continue   # skip broken card, keep going

        except Exception as exc:
            print(f"    [ERROR] {name}: {exc!s:.120s}")
            traceback.print_exc()
        finally:
            await browser.close()

    return reviews


async def scrape_branch(branch: dict, sem: asyncio.Semaphore, snap_date: str) -> list[dict]:
    """Semaphore-gated wrapper with automatic retry on failure."""
    async with sem:
        for attempt in range(1, MAX_RETRIES + 2):
            try:
                result = await _scrape_once(branch, snap_date)
                label  = "✅" if result else "⚪"
                print(f"  {label} {branch['name']:22s} → {len(result):3d} review(s)", flush=True)
                return result
            except Exception as exc:
                wait = 10 * attempt + random.uniform(3, 8)
                if attempt <= MAX_RETRIES:
                    print(
                        f"  ⚠️  {branch['name']} attempt {attempt} failed "
                        f"({exc!s:.60s}) – retry in {wait:.0f}s"
                    )
                    await asyncio.sleep(wait)
                else:
                    print(f"  ❌ {branch['name']} gave up after {attempt} attempts.", flush=True)
        return []


# ══════════════════════════════════════════════════════════════════════════════
# Deletion tracking
# ══════════════════════════════════════════════════════════════════════════════

def track_deletions(
    live_map:    dict,   # current merged live map {fp: review}
    old_del_map: dict,   # previously known deleted {fp: review}
    old_live_map: dict,  # what was live before this run {fp: review}
    fresh_fps:   set,    # fingerprints found in this scrape run
    snap_date:   str,    # date scope for deletion checks
) -> tuple[dict, dict]:
    """
    Returns (updated_live_map, updated_del_map).

    Deletion is only checked for reviews that share the current snap_date
    so we never falsely flag previous days' reviews as 'deleted'.
    """
    now_str  = ist_now().strftime("%Y-%m-%d %H:%M")
    del_out  = dict(old_del_map)

    # 1. Reinstatements: was deleted, now visible again
    for fp in list(del_out.keys()):
        if fp in fresh_fps:
            item = dict(del_out.pop(fp))
            item.pop("deleted_on", None)
            item["reinstated_on"] = now_str
            live_map[fp] = item
            print(f"    ♻️  Reinstated: {item.get('branch_name')} – {item.get('author')}")

    # 2. Deletions: was live on same snap_date, now absent
    scoped_old = {fp: v for fp, v in old_live_map.items() if v.get("snap_date") == snap_date}
    for fp, item in scoped_old.items():
        if fp not in fresh_fps and fp not in del_out:
            del_item = dict(item)
            del_item["deleted_on"] = now_str
            del_out[fp] = del_item
            live_map.pop(fp, None)
            print(f"    🗑️  Deleted: {item.get('branch_name')} – {item.get('author')}")

    # 3. Purge stale deletion records (> DELETION_DAYS)
    cutoff = ist_now() - timedelta(days=DELETION_DAYS)
    purged = 0
    for fp in list(del_out.keys()):
        raw = del_out[fp].get("deleted_on", "")
        try:
            d = datetime.strptime(raw, "%Y-%m-%d %H:%M").replace(tzinfo=IST)
            if d < cutoff:
                del_out.pop(fp)
                purged += 1
        except Exception:
            pass
    if purged:
        print(f"    🧹 Purged {purged} deletion record(s) older than {DELETION_DAYS} days")

    return live_map, del_out


# ══════════════════════════════════════════════════════════════════════════════
# Entry point
# ══════════════════════════════════════════════════════════════════════════════

async def main():
    slot      = get_run_slot()
    snap_date = snap_date_for_slot(slot)

    print(f"\n{'═'*62}")
    print(f"  Sathya Review Scraper")
    print(f"  Slot: {slot}  |  snap_date: {snap_date}")
    print(f"  IST:  {ist_now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'═'*62}\n")

    # ── Load existing state ────────────────────────────────────────────────────
    old_live_list = load_json(REV_JSON)
    old_del_list  = load_json(DEL_JSON)
    old_live_map  = {r["fingerprint"]: r for r in old_live_list}
    old_del_map   = {r["fingerprint"]: r for r in old_del_list}
    print(f"  Loaded from disk: {len(old_live_map)} live, {len(old_del_map)} deleted\n")

    # ── Concurrent scrape ──────────────────────────────────────────────────────
    sem     = asyncio.Semaphore(MAX_CONCURRENT)
    tasks   = [scrape_branch(b, sem, snap_date) for b in BRANCHES]
    batches = await asyncio.gather(*tasks)

    # Flatten and deduplicate within this run (first occurrence wins)
    fresh_map: dict[str, dict] = {}
    for batch in batches:
        for r in batch:
            fp = r["fingerprint"]
            if fp not in fresh_map:
                fresh_map[fp] = r

    fresh_fps = set(fresh_map.keys())
    print(f"\n  Fresh reviews this run: {len(fresh_fps)} unique\n")

    # ── Merge fresh into live map ──────────────────────────────────────────────
    merged_live = dict(old_live_map)

    new_count = updated_count = 0
    for fp, r in fresh_map.items():
        if fp not in merged_live:
            merged_live[fp] = r
            new_count += 1
        else:
            # Review already known – just refresh timing fields
            existing = dict(merged_live[fp])
            existing["rel_time"]    = r["rel_time"]
            existing["parsed_date"] = r["parsed_date"]
            existing["scraped_at"]  = r["scraped_at"]
            merged_live[fp]         = existing
            updated_count += 1

    print(f"  Merge: {new_count} new, {updated_count} updated (timing refresh)")

    # ── Track deletions ────────────────────────────────────────────────────────
    merged_live, merged_del = track_deletions(
        live_map=merged_live,
        old_del_map=old_del_map,
        old_live_map=old_live_map,
        fresh_fps=fresh_fps,
        snap_date=snap_date,
    )

    # ── Sort: newest first within each snap_date ───────────────────────────────
    final_live = sorted(
        merged_live.values(),
        key=lambda x: (x.get("snap_date", ""), x.get("parsed_date", "")),
        reverse=True,
    )
    final_del = sorted(
        merged_del.values(),
        key=lambda x: x.get("deleted_on", ""),
        reverse=True,
    )

    # ── Persist ────────────────────────────────────────────────────────────────
    save_json(REV_JSON, final_live)
    save_json(DEL_JSON, final_del)

    print(f"\n{'═'*62}")
    print(f"  ✅  Saved → rev.json: {len(final_live)}  |  deleted.json: {len(final_del)}")
    print(f"{'═'*62}\n")


if __name__ == "__main__":
    asyncio.run(main())
