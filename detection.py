"""
detection.py – Anti-detection helpers for the Sathya Review Scraper.

Applies aggressive stealth patches to Playwright pages so Google Maps
does not flag the headless browser as a bot.
"""

import asyncio
import random


# ── User-Agent pool ────────────────────────────────────────────────────────────

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


# ── Launch args ────────────────────────────────────────────────────────────────

CHROMIUM_ARGS = [
    "--no-sandbox",
    "--disable-setuid-sandbox",
    "--disable-blink-features=AutomationControlled",
    "--disable-infobars",
    "--disable-extensions",
    "--disable-dev-shm-usage",
    "--no-first-run",
    "--no-default-browser-check",
    "--disable-default-apps",
    "--disable-translate",
    "--disable-sync",
    "--metrics-recording-only",
    "--mute-audio",
    "--hide-scrollbars",
]


# ── Stealth JS injected into every page ───────────────────────────────────────

_STEALTH_JS = """
// Remove webdriver flag
Object.defineProperty(navigator, 'webdriver', { get: () => undefined });

// Fake plugins
Object.defineProperty(navigator, 'plugins', {
    get: () => [1, 2, 3, 4, 5],
});

// Fake languages
Object.defineProperty(navigator, 'languages', {
    get: () => ['en-US', 'en'],
});

// Chrome runtime stub
window.chrome = { runtime: {} };

// Permissions stub
const originalQuery = window.navigator.permissions.query;
window.navigator.permissions.query = (parameters) => (
    parameters.name === 'notifications'
        ? Promise.resolve({ state: Notification.permission })
        : originalQuery(parameters)
);
"""


async def make_stealth_context(browser):
    """Return a new BrowserContext with stealth settings applied."""
    context = await browser.new_context(
        viewport=random.choice(VIEWPORTS),
        user_agent=random.choice(USER_AGENTS),
        locale="en-US",
        timezone_id="Asia/Kolkata",
        java_script_enabled=True,
        # Realistic HTTP headers
        extra_http_headers={
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-User": "?1",
            "Sec-Fetch-Dest": "document",
            "Upgrade-Insecure-Requests": "1",
        },
    )
    # Inject stealth JS before every page load
    await context.add_init_script(_STEALTH_JS)
    return context


# ── Human-like delays ──────────────────────────────────────────────────────────

async def human_delay(min_s: float = 1.5, max_s: float = 3.5) -> None:
    await asyncio.sleep(random.uniform(min_s, max_s))


async def micro_delay() -> None:
    await asyncio.sleep(random.uniform(0.3, 0.8))


def jitter(base: float, pct: float = 0.25) -> float:
    """Add ±pct random jitter to a base value."""
    return base * random.uniform(1 - pct, 1 + pct)
