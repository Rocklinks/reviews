"""
migrate_clean.py — One-time migration + on-demand cleanup script.

AUTO-RUN: main.py calls this automatically when it detects old-format
duplicate entries in rev.json (entries whose review_id doesn't match
the current stable hash scheme that excludes the date).

MANUAL RUN:
    python migrate_clean.py            # run migration
    python migrate_clean.py --dry-run  # preview only, no changes
    python migrate_clean.py --force    # run even if no duplicates detected

What it does:
  1. Scans rev.json for duplicates under the new hash (no date in hash).
     Keeps the EARLIEST scraped_at entry per unique review.
  2. Re-keys all entries with their correct new stable hash IDs.
  3. Clears deleted.json — entries from before the hash fix are false
     positives (Apr21 ID ≠ Apr22 ID for same review under old hash).
  4. Creates timestamped backups before any changes.
"""

import json, hashlib, shutil, argparse
from pathlib import Path
from datetime import datetime

BASE_DIR     = Path(__file__).parent
REV_FILE     = BASE_DIR / "rev.json"
DELETED_FILE = BASE_DIR / "deleted.json"


def new_stable_id(branch_id: int, author: str, text: str, stars: int) -> str:
    author_clean = author.split("\n")[0].strip()
    raw = f"{branch_id}||{author_clean}||{text}||{stars}"
    return hashlib.sha1(raw.encode()).hexdigest()[:16]


def load(path):
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def save(path, data):
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def needs_migration(rev: dict) -> bool:
    """Return True if rev.json has entries that need migration."""
    if not rev:
        return False
    seen = set()
    for entry in rev.values():
        try:
            nid = new_stable_id(
                entry["branch_id"], entry["author"],
                entry["text"], entry["stars"]
            )
        except (KeyError, TypeError):
            return True   # malformed entry
        if entry.get("review_id") != nid:
            return True   # old hash format
        if nid in seen:
            return True   # duplicate
        seen.add(nid)
    return False


def main():
    parser = argparse.ArgumentParser(description="Migrate rev.json to new hash scheme")
    parser.add_argument("--dry-run", action="store_true", help="Preview only")
    parser.add_argument("--force",   action="store_true", help="Run even if clean")
    args = parser.parse_args()

    print("=" * 60)
    print("migrate_clean.py — Sathya Agency review data migration")
    print("=" * 60)

    rev = load(REV_FILE)
    deleted = load(DELETED_FILE)
    print(f"\nrev.json:     {len(rev)} entries")
    print(f"deleted.json: {len(deleted)} entries")

    # ── Check if migration is needed ────────────────────────────────────────
    migration_needed = needs_migration(rev)
    if not migration_needed and not args.force:
        print("\n✅ rev.json is already clean. No migration needed.")
        print("   (Use --force to run anyway)")
        return

    if not migration_needed and args.force:
        print("\n[--force] Running migration even though data looks clean.")

    # ── Preview ─────────────────────────────────────────────────────────────
    # Group by new stable hash → pick earliest scraped_at
    groups = {}
    for old_id, entry in rev.items():
        try:
            nid = new_stable_id(
                entry["branch_id"], entry["author"],
                entry["text"], entry["stars"]
            )
        except (KeyError, TypeError):
            nid = old_id   # keep as-is if malformed
        groups.setdefault(nid, []).append(entry)

    clean_rev = {}
    for nid, entries in groups.items():
        entries.sort(key=lambda e: e.get("scraped_at", ""))
        winner = dict(entries[0])
        winner["review_id"] = nid
        clean_rev[nid] = winner

    removed = len(rev) - len(clean_rev)
    del_count = len(deleted)

    print(f"\nChanges to make:")
    print(f"  rev.json:     {len(rev)} → {len(clean_rev)} entries ({removed} duplicates removed)")
    print(f"  deleted.json: {del_count} → 0 entries (all were false positives from old hash bug)")

    if args.dry_run:
        print("\n[DRY RUN] No files changed. Remove --dry-run to apply.")
        return

    # ── Backup ──────────────────────────────────────────────────────────────
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    if REV_FILE.exists():
        bak = Path(str(REV_FILE) + f".bak_{ts}")
        shutil.copy2(REV_FILE, bak)
        print(f"\nBackup: {bak.name}")
    if DELETED_FILE.exists() and del_count > 0:
        bak2 = Path(str(DELETED_FILE) + f".bak_{ts}")
        shutil.copy2(DELETED_FILE, bak2)
        print(f"Backup: {bak2.name}")

    # ── Write ────────────────────────────────────────────────────────────────
    save(REV_FILE, clean_rev)
    save(DELETED_FILE, {})

    print(f"\nrev.json:     written ({len(clean_rev)} entries)")
    print(f"deleted.json: cleared")
    print("\n✅ Migration complete. The scraper will work correctly from here.")


if __name__ == "__main__":
    main()
