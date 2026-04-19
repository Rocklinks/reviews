"""
scraper.py  –  Core scraping logic for a single Google Maps branch.

CONFIRMED DOM (from your DevTools inspection of a live Maps page):
  Reviews tab  : <span class="PbOY2e">Reviews</span>
                 parent = <div role="button" data-tab-index="1">
  Sort button  : <div class="faMaId"> or <div class="AHwJHf">Sort by</div>
  Sort dropdown: <div data-sort="1"> = Newest
                 <div data-sort="2"> = Most relevant
  Review card  : <div class="jftiEf">
  Author       : <span class="d4r55">
  Stars        : <span role="img" aria-label="5 stars">
  Text         : <span class="wiI7pd">
  Timestamp    : <span class="rsqaWe">
  Scroll panel : <div class="m6QErb">   ← inner div, NOT window

IMPORTANT: The scroll target is div.m6QErb — an inner scrollable div.
Scrolling window.scrollBy() does NOTHING on Google Maps.

This same DOM structure is used by ALL 36 branches — only the place_id URL changes.
"""

import asyncio
import hashlib
import random
import re
import traceback
from pathlib import Path

from playwright.async_api import async_playwright, TimeoutError as PWTimeout

from stealth import CHROMIUM_ARGS, make_stealth_context
from time_utils import ist_now, is_within_23h, rel_to_abs

# ── Debug output dirs ──────────────────────────────────────────────────────────
_SS  = Path(__file__).parent / "debug" / "screenshots"
_DOM = Path(__file__).parent / "debug" / "dom"
_SS.mkdir(parents=True, exist_ok=True)
_DOM.mkdir(parents=True, exist_ok=True)

def _slug(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", s.lower()).strip("_")

async def _save_debug(page, name: str, label: str) -> None:
    """Save a screenshot + DOM summary for post-run diagnosis."""
    sl = _slug(name); lb = _slug(label)
    try:
        await page.screenshot(path=str(_SS / f"{sl}_{lb}.png"), full_page=False)
    except Exception:
        pass
    try:
        lines = [f"=== {name} | {label} ===",
                 f"URL: {page.url}", f"Title: {await page.title()}", ""]
        for sel in ["div.jftiEf", "span.PbOY2e", "span.rsqaWe", "span.d4r55",
                    "div[data-sort]", "div.m6QErb", "[role='feed']", "[role='main']"]:
            try:
                n = await page.locator(sel).count()
                if n: lines.append(f"  {n:4d}  {sel}")
            except Exception:
                pass
        try:
            cards = await page.locator("div.jftiEf").all()
            for i, c in enumerate(cards[:3]):
                t = (await c.inner_text(timeout=500)).replace("\n", " ")[:200]
                lines.append(f"  CARD[{i}]: {t!r}")
        except Exception:
            pass
        (_DOM / f"{sl}_{lb}.txt").write_text("\n".join(lines), encoding="utf-8")
    except Exception:
        pass


# ══════════════════════════════════════════════════════════════════════════════
# Navigation
# ══════════════════════════════════════════════════════════════════════════════

async def _navigate(page, place_id: str, name: str, extra_wait: int) -> bool:
    """
    Load the Maps place page and wait until the sidebar is mounted.
    Returns True when ready.
    """
    url = f"https://www.google.com/maps/place/?q=place_id:{place_id}&hl=en"

    # Block images/fonts — Maps JS/XHR/CSS stay intact, page loads ~3× faster
    await page.route(
        re.compile(r"\.(png|jpg|jpeg|gif|webp|svg|ico|woff2?|ttf|otf|mp4|mp3)(\?.*)?$"),
        lambda route: route.abort(),
    )

    try:
        resp = await page.goto(url, wait_until="domcontentloaded", timeout=60_000)
        status = resp.status if resp else 0
        print(f"    [{name}] HTTP {status}", flush=True)
    except PWTimeout:
        print(f"    [{name}] Navigation timeout", flush=True)
        return False
    except Exception as e:
        print(f"    [{name}] Navigation error: {e!s:.60s}", flush=True)
        return False

    # Handle Google consent/cookie gate (common on fresh datacenter IPs)
    await asyncio.sleep(1)
    if "consent.google" in page.url:
        print(f"    [{name}] Consent page — dismissing…", flush=True)
        for sel in [
            "#L2AGLb",
            "button[aria-label*='Accept all' i]",
            "button[aria-label*='Agree' i]",
            "form[action*='consent'] button:last-child",
        ]:
            try:
                b = page.locator(sel).first
                if await b.is_visible(timeout=2000):
                    await b.click()
                    await asyncio.sleep(3)
                    break
            except Exception:
                pass

    # Wait until Maps sidebar has mounted. We poll for any of these markers:
    # span.PbOY2e    = Reviews tab label (sidebar fully rendered)
    # [role='feed']  = ARIA reviews feed (reviews panel open)
    # div.jftiEf     = review cards (already on reviews)
    deadline = asyncio.get_event_loop().time() + 30 + extra_wait
    while asyncio.get_event_loop().time() < deadline:
        for sel in ["span.PbOY2e", "[role='feed']", "div.jftiEf",
                    "div[role='tablist']", "span.F7nice"]:
            try:
                if await page.locator(sel).count() > 0:
                    return True
            except Exception:
                pass
        await asyncio.sleep(0.7)

    print(f"    [{name}] Sidebar not mounted after {30+extra_wait}s", flush=True)
    return False


# ══════════════════════════════════════════════════════════════════════════════
# Reviews tab click
# ══════════════════════════════════════════════════════════════════════════════

async def _click_reviews_tab(page, name: str) -> bool:
    """
    Click the Reviews tab to open the reviews panel.

    Skipped if review cards are already in the DOM (rare — happens if Maps
    auto-opens reviews on some URLs).

    Strategy: locate span.PbOY2e (confirmed class for tab labels) whose text
    contains 'review', then walk up to the role=button parent and click it.
    This is robust because it uses the text content, not a fragile index.
    """
    # Already showing reviews?
    if await page.locator("div.jftiEf").count() > 0:
        print(f"    [{name}] Reviews panel already open", flush=True)
        return True

    await asyncio.sleep(random.uniform(1.5, 2.5))

    # Strategy 1 — Confirmed DOM: span.PbOY2e → walk up to role=button
    try:
        result = await page.evaluate("""
            () => {
                const spans = document.querySelectorAll('span.PbOY2e');
                for (const s of spans) {
                    if (/review/i.test(s.innerText || '')) {
                        let el = s;
                        for (let i = 0; i < 8; i++) {
                            if (!el.parentElement) break;
                            el = el.parentElement;
                            if (el.getAttribute('role') === 'button') {
                                el.click();
                                return 'S1:role=button via PbOY2e';
                            }
                        }
                        s.click();
                        return 'S1:span.PbOY2e direct';
                    }
                }
                return null;
            }
        """)
        if result:
            print(f"    [{name}] ✓ Tab: {result}", flush=True)
            await asyncio.sleep(random.uniform(3, 4))
            return True
    except Exception:
        pass

    # Strategy 2 — data-tab-index="1" (Reviews is always tab index 1)
    try:
        loc = page.locator("div[role='button'][data-tab-index='1']").first
        if await loc.is_visible(timeout=2000):
            await loc.click()
            print(f"    [{name}] ✓ Tab: data-tab-index=1", flush=True)
            await asyncio.sleep(random.uniform(3, 4))
            return True
    except Exception:
        pass

    # Strategy 3 — scan all role=button for text starting with "Review"
    try:
        btns = await page.locator("div[role='button']").all()
        for b in btns[:20]:
            try:
                t = (await b.inner_text(timeout=300)).strip().lower()
                if t.startswith("review"):
                    await b.click()
                    print(f"    [{name}] ✓ Tab: role=button text={t!r}", flush=True)
                    await asyncio.sleep(random.uniform(3, 4))
                    return True
            except Exception:
                continue
    except Exception:
        pass

    # Strategy 4 — keyboard: Tab navigation then Enter
    # Maps is keyboard-accessible; pressing Tab cycles through the nav tabs
    try:
        await page.keyboard.press("Tab")
        await asyncio.sleep(0.4)
        await page.keyboard.press("Tab")
        await asyncio.sleep(0.4)
        await page.keyboard.press("Enter")
        await asyncio.sleep(2)
        if await page.locator("[role='feed'], div.jftiEf").count() > 0:
            print(f"    [{name}] ✓ Tab: keyboard navigation", flush=True)
            return True
    except Exception:
        pass

    print(f"    [{name}] ✗ Tab click failed — attempting to scrape anyway", flush=True)
    return False


# ══════════════════════════════════════════════════════════════════════════════
# Sort by Newest
# ══════════════════════════════════════════════════════════════════════════════

async def _sort_newest(page, name: str) -> bool:
    """
    Sort reviews by Newest.

    Confirmed DOM:
      Trigger: div.faMaId  OR  div.AHwJHf (contains "Sort by" text)
      Option:  div[data-sort="1"]  =  Newest
    """
    await asyncio.sleep(random.uniform(1.5, 2.0))

    # Check if already showing data-sort options (dropdown open)
    try:
        n = page.locator("div[data-sort='1']").first
        if await n.is_visible(timeout=800):
            await n.click()
            print(f"    [{name}] ✓ Sort: data-sort=1 immediate", flush=True)
            await asyncio.sleep(random.uniform(2, 3))
            return True
    except Exception:
        pass

    # Open the sort dropdown
    opened = False
    for sel in ["div.faMaId", "div.AHwJHf", "button[aria-label*='Sort' i]",
                "[jsaction*='sort']", "[aria-label*='Sort reviews' i]"]:
        try:
            loc = page.locator(sel).first
            if await loc.is_visible(timeout=1500):
                await loc.click()
                opened = True
                print(f"    [{name}] ✓ Sort trigger: {sel}", flush=True)
                break
        except Exception:
            continue

    if not opened:
        # JS fallback using confirmed class names
        try:
            r = await page.evaluate("""
                () => {
                    // Try confirmed classes first
                    let el = document.querySelector('.faMaId, .AHwJHf');
                    if (el) { el.click(); return el.className; }
                    // Try radiogroup label
                    el = document.querySelector('[aria-label*="Sort" i]');
                    if (el) { el.click(); return 'aria-sort'; }
                    return null;
                }
            """)
            if r:
                opened = True
                print(f"    [{name}] ✓ Sort trigger (JS): {r}", flush=True)
        except Exception:
            pass

    if not opened:
        print(f"    [{name}] ✗ Sort dropdown not found", flush=True)
        return False

    await asyncio.sleep(random.uniform(1.0, 1.5))

    # Click Newest = data-sort="1"
    for sel in ["div[data-sort='1']", "[data-sort='1']"]:
        try:
            loc = page.locator(sel).first
            if await loc.is_visible(timeout=2000):
                await loc.click()
                print(f"    [{name}] ✓ Sort: Newest selected", flush=True)
                await asyncio.sleep(random.uniform(2, 3))
                return True
        except Exception:
            continue

    # JS fallback
    try:
        r = await page.evaluate("""
            () => {
                const el = document.querySelector('[data-sort="1"]');
                if (el) { el.click(); return true; }
                return false;
            }
        """)
        if r:
            await asyncio.sleep(random.uniform(2, 3))
            print(f"    [{name}] ✓ Sort: Newest (JS fallback)", flush=True)
            return True
    except Exception:
        pass

    print(f"    [{name}] ✗ Sort Newest not clicked", flush=True)
    return False


# ══════════════════════════════════════════════════════════════════════════════
# Scroll panel finder
# ══════════════════════════════════════════════════════════════════════════════

async def _find_scroll_panel(page):
    """
    Find the inner scrollable reviews div.
    CRITICAL: Google Maps renders reviews inside a scrollable div, not the page.
    Scrolling window.scrollBy() won't load more reviews.
    Confirmed target: div.m6QErb
    """
    for sel in ["div.m6QErb.DxyBCb", "div.m6QErb.WNBkOb", "div.m6QErb",
                "[role='feed']", "div.section-scrollbox"]:
        try:
            loc = page.locator(sel).first
            if await loc.count():
                box = await loc.bounding_box()
                if box and box["height"] > 150:
                    return await loc.element_handle()
        except Exception:
            continue

    # JS fallback: find the tallest scrollable div inside role=main
    try:
        h = await page.evaluate_handle("""
            () => {
                const root = document.querySelector('[role="main"]') || document.body;
                let best = null, bestH = 0;
                for (const el of root.querySelectorAll('div')) {
                    if (el.scrollHeight > el.clientHeight + 100 && el.clientHeight > 150) {
                        if (el.clientHeight > bestH) { best = el; bestH = el.clientHeight; }
                    }
                }
                return best;
            }
        """)
        el = h.as_element()
        if el:
            return el
    except Exception:
        pass

    return None


# ══════════════════════════════════════════════════════════════════════════════
# Card parser
# ══════════════════════════════════════════════════════════════════════════════

def _make_fp(rating: float, author: str, text: str) -> str:
    raw = f"{round(rating,1)}|{(author or '').lower()[:40]}|{(text or '').lower()[:200]}"
    return hashlib.sha256(raw.encode()).hexdigest()[:20]

async def _parse_card(card, branch: dict, snap_date: str, ref) -> dict | None:
    """
    Parse a single review card (div.jftiEf) and return a review dict,
    or None if the review is older than 23 hours.
    """
    # Author — confirmed: span.d4r55
    author = "Unknown"
    for sel in ["span.d4r55", ".fontHeadlineSmall", ".jJc9Ad"]:
        try:
            el = card.locator(sel).first
            if await el.count():
                t = (await el.inner_text(timeout=600)).strip()
                if t: author = t; break
        except Exception:
            pass

    # Star rating — confirmed: span[role="img"][aria-label="5 stars"]
    rating = 0.0
    for sel in ["span[role='img'][aria-label]", ".kvMYJc",
                "span[aria-label*='star' i]"]:
        try:
            el = card.locator(sel).first
            if await el.count():
                raw = (await el.get_attribute("aria-label", timeout=600)) or ""
                m = re.search(r"(\d+\.?\d*)", raw)
                if m: rating = float(m.group(1)); break
        except Exception:
            pass

    # Review text — confirmed: span.wiI7pd
    text = ""
    for sel in ["span.wiI7pd", ".wiI7pd", ".MyEned span"]:
        try:
            el = card.locator(sel).first
            if await el.count():
                t = (await el.inner_text(timeout=600)).strip().replace("\n", " ")
                if t: text = t; break
        except Exception:
            pass

    # Timestamp — confirmed: span.rsqaWe
    rel_time = ""
    for sel in ["span.rsqaWe", ".DU9Pgb", "span.dehysf"]:
        try:
            el = card.locator(sel).first
            if await el.count():
                t = (await el.inner_text(timeout=600)).strip()
                if t: rel_time = t; break
        except Exception:
            pass

    # Only keep reviews from last 23 hours
    if not is_within_23h(rel_time):
        return None

    return {
        "fingerprint":  _make_fp(rating, author, text),
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
# Main public function: scrape one branch
# ══════════════════════════════════════════════════════════════════════════════

MAX_SCROLL_ROUNDS = 40
STALE_LIMIT       = 4

async def scrape_one_branch(branch: dict, snap_date: str, attempt: int = 1) -> list[dict]:
    """
    Open a headless Chrome, load the Maps reviews page for one branch,
    scroll through all reviews posted in the last 23 hours, and return them.
    """
    name     = branch["name"]
    place_id = branch["place_id"]
    extra    = 2 * (attempt - 1)   # extra wait on retries
    reviews: list[dict] = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=CHROMIUM_ARGS)
        try:
            ctx  = await make_stealth_context(browser)
            page = await ctx.new_page()

            # ── Step 1: Navigate ───────────────────────────────────────────────
            ok = await _navigate(page, place_id, name, extra)
            if not ok:
                await _save_debug(page, name, "FAIL_no_sidebar")
                return []

            await asyncio.sleep(random.uniform(2 + extra, 3 + extra))
            await _save_debug(page, name, "01_loaded")

            # ── Step 2: Open Reviews tab ───────────────────────────────────────
            await _click_reviews_tab(page, name)
            await asyncio.sleep(random.uniform(2 + extra, 3 + extra))
            await _save_debug(page, name, "02_tab_clicked")

            # ── Step 3: Sort by Newest ─────────────────────────────────────────
            await _sort_newest(page, name)
            await asyncio.sleep(random.uniform(2 + extra, 3 + extra))
            await _save_debug(page, name, "03_sorted")

            # ── Step 4: Find scroll panel ──────────────────────────────────────
            panel = await _find_scroll_panel(page)

            # ── Step 5: Scroll until past 23h window or content stops loading ──
            prev_count = stale_rounds = 0

            for _ in range(MAX_SCROLL_ROUNDS):
                # Expand "See more" / "More" buttons so full text is captured
                for more_sel in ["button.w8nwRe", "button[aria-label='See more']"]:
                    try:
                        btns = page.locator(more_sel)
                        for i in range(await btns.count()):
                            try:
                                b = btns.nth(i)
                                if await b.is_visible(timeout=200):
                                    await b.click()
                                    await asyncio.sleep(0.15)
                            except Exception:
                                pass
                    except Exception:
                        pass

                # Check the LAST visible timestamp.
                # If it's already older than 23h, we've scrolled far enough.
                ts = page.locator("span.rsqaWe, .DU9Pgb")
                n  = await ts.count()
                if n > 0:
                    try:
                        last = (await ts.nth(n - 1).inner_text(timeout=400)).strip()
                        if last and not is_within_23h(last):
                            break
                    except Exception:
                        pass

                # Scroll the inner panel div (NOT window!)
                scrolled = False
                if panel:
                    try:
                        await page.evaluate(
                            "el => { el.scrollTop = el.scrollHeight; }",
                            panel,
                        )
                        scrolled = True
                    except Exception:
                        panel = None   # handle stale element

                if not scrolled:
                    # Fallback: scroll the page (less effective but better than nothing)
                    await page.evaluate("window.scrollBy(0, 3000)")

                await asyncio.sleep(random.uniform(1.8, 2.8))

                curr = await page.locator("div.jftiEf").count()
                if curr == prev_count:
                    stale_rounds += 1
                    if stale_rounds >= STALE_LIMIT:
                        break
                else:
                    stale_rounds = 0
                    prev_count = curr

            await _save_debug(page, name, "04_scrolled")

            # ── Step 6: Parse all cards ────────────────────────────────────────
            ref   = ist_now()
            cards = await page.locator("div.jftiEf").all()
            print(f"    [{name}] {len(cards)} cards in DOM", flush=True)

            if not cards:
                await _save_debug(page, name, "ZERO_CARDS")

            for card in cards:
                try:
                    rv = await _parse_card(card, branch, snap_date, ref)
                    if rv:
                        reviews.append(rv)
                except Exception:
                    continue

            print(f"    [{name}] {len(reviews)}/{len(cards)} within 23h", flush=True)

        except Exception as e:
            print(f"    [{name}] EXCEPTION: {e!s:.120s}", flush=True)
            traceback.print_exc()
            try:
                await _save_debug(page, name, "EXCEPTION")
            except Exception:
                pass
        finally:
            await browser.close()

    return reviews
