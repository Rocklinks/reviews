"""
migrate_clean.py — One-time migration script.

Run ONCE after updating to the new code (date removed from make_review_id hash).

What it does:
  1. Deduplicates rev.json using the new stable hash (no date).
     Keeps the EARLIEST scraped_at entry for each unique review.
  2. Clears deleted.json — all existing entries are false positives
     caused by the old bug (date was in hash, so Apr21 ID ≠ Apr22 ID
     for the same review, causing Apr21 entries to be falsely "deleted").
  3. Creates backups: rev.json.bak, deleted.json.bak

Usage:
    python migrate_clean.py
    python migrate_clean.py --dry-run   # preview only, no changes
"""

import json, hashlib, shutil, argparse
from pathlib import Path
from datetime import datetime

BASE_DIR = Path(__file__).parent
REV_FILE     = BASE_DIR / "rev.json"
DELETED_FILE = BASE_DIR / "deleted.json"


def new_stable_id(branch_id: int, author: str, text: str, stars: int) -> str:
    author_clean = author.split("\n")[0].strip()
    raw = f"{branch_id}||{author_clean}||{text}||{stars}"
    return hashlib.sha1(raw.encode()).hexdigest()[:16]


def load(path):
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8"))
    return {}


def save(path, data):
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="Preview only, no changes")
    args = parser.parse_args()
    dry = args.dry_run

    print("=" * 60)
    print("MIGRATION: Clean rev.json duplicates + clear deleted.json")
    print("=" * 60)

    # ── Step 1: Deduplicate rev.json ─────────────────────────────────
    rev = load(REV_FILE)
    print(f"\nrev.json: {len(rev)} entries (before dedup)")

    # Group by new stable hash, keep earliest scraped_at
    groups = {}  # new_id -> list of entries
    for old_id, rev_entry in rev.items():
        nid = new_stable_id(
            rev_entry["branch_id"],
            rev_entry["author"],
            rev_entry["text"],
            rev_entry["stars"],
        )
        groups.setdefault(nid, []).append(rev_entry)

    # Build clean rev with new IDs, keeping earliest scraped_at
    clean_rev = {}
    for nid, entries in groups.items():
        entries.sort(key=lambda e: e.get("scraped_at", ""))
        winner = entries[0]
        winner["review_id"] = nid          # update ID to new stable hash
        # Keep the ORIGINAL date (first time it was scraped)
        clean_rev[nid] = winner

    removed = len(rev) - len(clean_rev)
    print(f"rev.json: {len(clean_rev)} entries after dedup ({removed} duplicates removed)")

    # ── Step 2: Clear deleted.json ────────────────────────────────────
    deleted = load(DELETED_FILE)
    print(f"\ndeleted.json: {len(deleted)} entries — ALL are false positives from old bug")
    print("  Reason: old hash included date → Apr21 ID ≠ Apr22 ID for same review")
    print("  → All Apr21 entries were falsely flagged as 'deleted' on Apr22")
    print("  → Clearing deleted.json entirely")

    if dry:
        print("\n[DRY RUN] No files changed.")
        return

    # ── Backup ────────────────────────────────────────────────────────
    if REV_FILE.exists():
        shutil.copy2(REV_FILE, str(REV_FILE) + ".bak")
        print(f"\nBackup created: {REV_FILE}.bak")
    if DELETED_FILE.exists():
        shutil.copy2(DELETED_FILE, str(DELETED_FILE) + ".bak")
        print(f"Backup created: {DELETED_FILE}.bak")

    # ── Write ─────────────────────────────────────────────────────────
    save(REV_FILE, clean_rev)
    save(DELETED_FILE, {})

    print(f"\nrev.json: written ({len(clean_rev)} entries)")
    print(f"deleted.json: cleared ({})")
    print("\nMigration complete. Run the scraper normally from here.")


if __name__ == "__main__":
    main()
