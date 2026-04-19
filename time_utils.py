"""
time_utils.py  –  IST time helpers, run-slot detection, relative-time parsing.

Run schedule (UTC → IST):
  00:30 UTC → 06:00 IST  "morning"   snap_date = today
  06:30 UTC → 12:00 IST  "noon"      snap_date = today
  12:30 UTC → 18:00 IST  "evening"   snap_date = today
  18:30 UTC → 00:00 IST  "midnight"  snap_date = YESTERDAY
    ↑ Reviews scraped just after midnight belong to the previous day.
"""

import re
from datetime import datetime, timedelta, timezone

IST = timezone(timedelta(hours=5, minutes=30))

# ── IST clock ─────────────────────────────────────────────────────────────────

def ist_now() -> datetime:
    return datetime.now(IST)

def ist_today() -> str:
    return ist_now().strftime("%Y-%m-%d")

def ist_yesterday() -> str:
    return (ist_now() - timedelta(days=1)).strftime("%Y-%m-%d")

# ── Run-slot detection ─────────────────────────────────────────────────────────

def get_run_slot() -> str:
    """Detect which of the 4 daily cron slots fired based on UTC hour."""
    h = datetime.now(timezone.utc).hour
    if h == 0:  return "morning"    # 00:30 UTC → 06:00 IST
    if h == 6:  return "noon"       # 06:30 UTC → 12:00 IST
    if h == 12: return "evening"    # 12:30 UTC → 18:00 IST
    if h == 18: return "midnight"   # 18:30 UTC → 00:00 IST (next day)
    # Manual / workflow_dispatch – guess from IST hour
    ih = ist_now().hour
    if 5  <= ih < 11: return "morning"
    if 11 <= ih < 17: return "noon"
    if 17 <= ih < 23: return "evening"
    return "midnight"

def get_snap_date(slot: str) -> str:
    """Midnight run → reviews belong to yesterday. All others → today."""
    return ist_yesterday() if slot == "midnight" else ist_today()

# ── Relative-time helpers ──────────────────────────────────────────────────────

# Matches: "just now", "a moment ago", "5 seconds ago", "3 minutes ago", "23 hours ago"
# Does NOT match: "1 day ago", "2 days ago", "a week ago", "3 months ago"
_WITHIN_23H_RE = re.compile(
    r"^(?:just now|a moment ago|moments? ago)$"
    r"|^(\d+)\s*(second|minute|hour)s?\s*ago$",
    re.IGNORECASE,
)

def is_within_23h(rel: str) -> bool:
    """Return True only if the relative timestamp is ≤ 23 hours old."""
    rel = (rel or "").strip()
    m = _WITHIN_23H_RE.match(rel)
    if not m:
        return False
    if not m.group(1):      # "just now" / "a moment ago"
        return True
    val  = int(m.group(1))
    unit = m.group(2).lower()
    if unit == "second": return True
    if unit == "minute": return val <= 1380   # 23 * 60
    if unit == "hour":   return val <= 23
    return False

def rel_to_abs(rel: str, ref: datetime) -> str:
    """Convert '5 minutes ago' → 'YYYY-MM-DD HH:MM:SS' (IST)."""
    rel = (rel or "").strip().lower()
    if not rel or "just now" in rel or "moment" in rel:
        return ref.strftime("%Y-%m-%d %H:%M:%S")
    m = re.search(r"(\d+)\s*(second|minute|hour|day|week|month|year)", rel)
    if not m:
        return ref.strftime("%Y-%m-%d %H:%M:%S")
    val, unit = int(m.group(1)), m.group(2)
    deltas = {
        "second": timedelta(seconds=val),
        "minute": timedelta(minutes=val),
        "hour":   timedelta(hours=val),
        "day":    timedelta(days=val),
        "week":   timedelta(weeks=val),
        "month":  timedelta(days=30 * val),
        "year":   timedelta(days=365 * val),
    }
    return (ref - deltas.get(unit, timedelta())).strftime("%Y-%m-%d %H:%M:%S")
