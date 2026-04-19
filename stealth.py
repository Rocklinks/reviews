"""
stealth.py  –  Anti-detection browser configuration for Google Maps scraping.

Key insight: --headless=new makes navigator.webdriver = undefined (not False).
This is the same result as a real Chrome, and what google-maps-scraper uses.
Combined with add_init_script, it passes all bot-detection checks.
"""

import random

# ── User-Agent pool (recent Chrome versions) ───────────────────────────────────
USER_AGENTS: list[str] = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 Edg/124.0.0.0",
]

# ── Chrome launch arguments ────────────────────────────────────────────────────
# --headless=new : Chrome's NEW headless mode. Unlike the old --headless,
#                  this does NOT set navigator.webdriver=true automatically.
#                  This is the single most important anti-detection flag.
CHROMIUM_ARGS: list[str] = [
    "--headless=new",                              # ← KEY: new headless mode
    "--no-sandbox",
    "--disable-setuid-sandbox",
    "--disable-dev-shm-usage",
    "--disable-blink-features=AutomationControlled",  # removes automation banner
    "--disable-extensions",
    "--disable-infobars",
    "--disable-default-apps",
    "--no-first-run",
    "--mute-audio",
    "--disable-gpu",
    "--disable-software-rasterizer",
    "--disable-background-networking",
    "--window-size=1400,900",
    "--lang=en-US",
]

# ── JavaScript injected before ANY page JS runs ────────────────────────────────
# This is added via add_init_script which runs in every new document,
# BEFORE the page's own JavaScript executes — so Google can't detect it.
STEALTH_INIT_SCRIPT: str = """
// Remove webdriver flag entirely (--headless=new sets it to undefined already,
// but this double-ensures it even if the flag doesn't work on older Chrome)
try { delete Object.getPrototypeOf(navigator).webdriver; } catch(e) {}

// Realistic plugins (headless has 0 by default — dead giveaway)
Object.defineProperty(navigator, 'plugins', {
    get: () => [
        {name: 'Chrome PDF Plugin',     filename: 'internal-pdf-viewer'},
        {name: 'Chrome PDF Viewer',     filename: 'mhjfbmdgcfjbbpaeojofohoefgiehjai'},
        {name: 'Native Client',         filename: 'internal-nacl-plugin'},
    ],
});

// Real languages
Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });

// Chrome object (absent in headless — another giveaway)
window.chrome = {
    runtime:    {},
    loadTimes:  function() { return {}; },
    csi:        function() { return {}; },
    app:        { isInstalled: false },
};

// Permissions API (headless returns 'denied' for notifications — real Chrome doesn't)
try {
    const origQuery = window.navigator.permissions.query;
    window.navigator.permissions.query = (params) =>
        params.name === 'notifications'
            ? Promise.resolve({ state: Notification.permission })
            : origQuery(params);
} catch(e) {}

// Remove Playwright-specific objects
try { delete window.__playwright; } catch(e) {}
try { delete window.__pw_manual; } catch(e) {}
"""

# ── Viewport pool ──────────────────────────────────────────────────────────────
VIEWPORTS: list[dict] = [
    {"width": 1280, "height": 800},
    {"width": 1366, "height": 768},
    {"width": 1440, "height": 900},
    {"width": 1920, "height": 1080},
]

# ── Factory ────────────────────────────────────────────────────────────────────

def random_ua() -> str:
    return random.choice(USER_AGENTS)

def random_viewport() -> dict:
    return random.choice(VIEWPORTS)

async def make_stealth_context(browser):
    """
    Create a Playwright BrowserContext with all stealth settings applied.
    Call this once per branch scrape.
    """
    ctx = await browser.new_context(
        viewport=random_viewport(),
        user_agent=random_ua(),
        locale="en-US",
        timezone_id="Asia/Kolkata",
        java_script_enabled=True,
        extra_http_headers={
            "Accept-Language":       "en-US,en;q=0.9",
            "Accept-Encoding":       "gzip, deflate, br",
            "Accept":                "text/html,application/xhtml+xml,*/*;q=0.8",
            "Sec-Fetch-Site":        "none",
            "Sec-Fetch-Mode":        "navigate",
            "Sec-Fetch-User":        "?1",
            "Sec-Fetch-Dest":        "document",
            "Upgrade-Insecure-Requests": "1",
        },
    )
    # Inject stealth JS before ANY page code runs
    await ctx.add_init_script(STEALTH_INIT_SCRIPT)
    return ctx
