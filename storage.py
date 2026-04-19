"""
storage.py  –  JSON persistence and review fingerprinting.
"""

import hashlib
import json
from pathlib import Path


def make_fp(rating: float, author: str, text: str) -> str:
    """
    Create a 20-char SHA-256 fingerprint for a review.
    Identical reviews posted by the same person will always produce the same fp.
    This is the deduplication key across scrape runs.
    """
    raw = (
        f"{round(float(rating), 1)}"
        f"|{(author or '').lower().strip()[:40]}"
        f"|{(text or '').lower().strip()[:200]}"
    )
    return hashlib.sha256(raw.encode()).hexdigest()[:20]


def load_json(path: Path) -> list:
    """Load a JSON list from disk. Returns [] if file missing or corrupt."""
    p = Path(path)
    if p.exists():
        try:
            return json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            return []
    return []


def save_json(path: Path, data: list) -> None:
    """Save a JSON list to disk, creating parent dirs as needed."""
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(
        json.dumps(data, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
