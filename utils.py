"""
utils.py – Shared helpers for the Sathya Review Scraper.
"""

import hashlib
import json
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path


# ── IST helpers ────────────────────────────────────────────────────────────────

IST = timezone(timedelta(hours=5, minutes=30))


def ist_now() -> datetime:
    return datetime.now(IST)


def ist_today() -> str:
    return ist_now().strftime("%Y-%m-%d")


def ist_yesterday() -> str:
    return (ist_now() - timedelta(days=1)).strftime("%Y-%m-%d")


# ── Run-slot detection ─────────────────────────────────────────────────────────

def current_run_slot() -> str:
    """
    Returns one of: 'morning'|'noon'|'evening'|'midnight'
    based on the UTC hour that GitHub Actions fires the cron.
      00:30 UTC → 06:00 IST → 'morning'
      06:30 UTC → 12:00 IST → 'noon'
      12:30 UTC → 18:00 IST → 'evening'
      18:30 UTC → 00:00 IST+1 → 'midnight'
    """
    utc_hour = datetime.now(timezone.utc).hour
    if utc_hour == 0:
        return "morning"
    elif utc_hour == 6:
        return "noon"
    elif utc_hour == 12:
        return "evening"
    elif utc_hour == 18:
        return "midnight"
    else:
        # Manual / workflow_dispatch – use IST hour to guess
        ist_hour = ist_now().hour
        if 5 <= ist_hour < 11:
            return "morning"
        elif 11 <= ist_hour < 17:
            return "noon"
        elif 17 <= ist_hour < 23:
            return "evening"
        else:
            return "midnight"


def snap_date_for_run(slot: str) -> str:
    """
    The midnight run fires at 00:30 IST which is still the *new* calendar day
    but the reviews being captured (posted in the previous evening) belong to
    the previous day.  All other slots use today's date.
    """
    if slot == "midnight":
        return ist_yesterday()
    return ist_today()


# ── Review time parser ─────────────────────────────────────────────────────────

def parse_relative_time(rel_time_str: str, ref: datetime | None = None) -> str:
    """
    Converts Google Maps relative timestamps like '2 minutes ago', '5 hours ago'
    into 'YYYY-MM-DD HH:MM:SS'.  Falls back to ref (defaults to ist_now()).
    """
    if ref is None:
        ref = ist_now()

    t = (rel_time_str or "").lower().strip()

    if not t or any(x in t for x in ["just now", "a moment", "moments"]):
        return ref.strftime("%Y-%m-%d %H:%M:%S")

    match = re.search(r"(\d+)\s*(second|minute|hour|day|week|month|year)", t)
    if not match:
        return ref.strftime("%Y-%m-%d %H:%M:%S")

    val = int(match.group(1))
    unit = match.group(2)

    delta_map: dict[str, timedelta] = {
        "second": timedelta(seconds=val),
        "minute": timedelta(minutes=val),
        "hour":   timedelta(hours=val),
        "day":    timedelta(days=val),
        "week":   timedelta(weeks=val),
        "month":  timedelta(days=30 * val),
        "year":   timedelta(days=365 * val),
    }

    dt = ref - delta_map.get(unit, timedelta(0))
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def is_within_24h(rel_time_str: str) -> bool:
    """Returns True if the relative time is within the past 24 hours."""
    t = (rel_time_str or "").lower().strip()
    if not t:
        return False
    if any(x in t for x in ["just now", "a moment", "moments", "second"]):
        return True

    match = re.search(r"(\d+)\s*(minute|hour)", t)
    if match:
        val = int(match.group(1))
        unit = match.group(2)
        total_mins = val if unit == "minute" else val * 60
        return total_mins < 1440  # 24 * 60

    return False


# ── Fingerprint ────────────────────────────────────────────────────────────────

def get_fingerprint(rating: float, author: str, text: str) -> str:
    raw = f"{round(rating, 1)}|{(author or '').lower()[:30]}|{(text or '').lower()[:200]}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]  # 16-char prefix is plenty


# ── JSON helpers ───────────────────────────────────────────────────────────────

def load_json(path: Path) -> list:
    if path.exists():
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return []
    return []


def save_json(path: Path, data) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(data, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )


def list_to_fp_map(items: list) -> dict:
    """Turn a list of review dicts into {fingerprint: item} map."""
    return {item["fingerprint"]: item for item in items}
