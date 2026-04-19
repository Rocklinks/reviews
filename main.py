"""
main.py  –  Orchestrator for the Sathya Review Scraper.

Runs all 36 branches concurrently (2 at a time), merges results into
rev.json, tracks deletions in deleted.json.

Usage:
    python main.py
"""

import asyncio
import random
from pathlib import Path

from branches import BRANCHES
from scraper import scrape_one_branch
from time_utils import ist_now, get_run_slot, get_snap_date, IST
from storage import load_json, save_json, make_fp

# ── Paths ──────────────────────────────────────────────────────────────────────
DOCS_DIR = Path(__file__).parent / "docs"
REV_JSON = DOCS_DIR / "rev.json"
DEL_JSON = DOCS_DIR / "deleted.json"
DOCS_DIR.mkdir(parents=True, exist_ok=True)

# ── Config ─────────────────────────────────────────────────────────────────────
MAX_CONCURRENT = 2      # 2 parallel browsers — safe for GH Actions 7GB RAM
MAX_RETRIES    = 2      # retry count before giving up on a branch
DELETION_DAYS  = 30     # keep deleted records for 30 days

from datetime import timedelta


# ══════════════════════════════════════════════════════════════════════════════
# Branch runner with retry
# ══════════════════════════════════════════════════════════════════════════════

async def run_branch(branch: dict, sem: asyncio.Semaphore, snap_date: str) -> list[dict]:
    """
    Runs scrape_one_branch with semaphore-controlled concurrency and automatic
    retry on failure or zero results.
    """
    async with sem:
        for attempt in range(1, MAX_RETRIES + 2):
            try:
                result = await scrape_one_branch(branch, snap_date, attempt=attempt)

                if result:
                    print(f"  ✅ {branch['name']:22s} → {len(result):3d} review(s)", flush=True)
                    return result

                if attempt > MAX_RETRIES:
                    print(f"  ⚪ {branch['name']:22s} → 0 (no recent reviews found)", flush=True)
                    return []

                # Zero results — retry with longer waits
                wait = 20 * attempt + random.uniform(5, 10)
                print(f"  ↺  {branch['name']} got 0, retry {attempt + 1}/{MAX_RETRIES + 1} in {wait:.0f}s…", flush=True)
                await asyncio.sleep(wait)

            except Exception as exc:
                wait = 20 * attempt + random.uniform(5, 10)
                if attempt <= MAX_RETRIES:
                    print(f"  ⚠️  {branch['name']} attempt {attempt} failed ({exc!s:.50s}) — retry in {wait:.0f}s")
                    await asyncio.sleep(wait)
                else:
                    print(f"  ❌ {branch['name']} gave up after {attempt} attempts.", flush=True)

        return []


# ══════════════════════════════════════════════════════════════════════════════
# Deletion tracking
# ══════════════════════════════════════════════════════════════════════════════

def track_deletions(
    live_map:     dict,
    old_del_map:  dict,
    old_live_map: dict,
    fresh_fps:    set,
    snap_date:    str,
) -> tuple[dict, dict]:
    """
    Compare what's freshly scraped against what was previously live.

    Rules:
    • fp in old_del but now in fresh_fps  → reinstated (back to live)
    • fp in old_live (same snap_date) but not in fresh_fps → deleted
    • Deletion records older than DELETION_DAYS are purged
    """
    now_str = ist_now().strftime("%Y-%m-%d %H:%M")
    del_out = dict(old_del_map)

    # ── Reinstatements ─────────────────────────────────────────────────────────
    for fp in list(del_out.keys()):
        if fp in fresh_fps:
            item = dict(del_out.pop(fp))
            item.pop("deleted_on", None)
            item["reinstated_on"] = now_str
            live_map[fp] = item
            print(f"    ♻️  Reinstated: {item.get('branch_name')} – {item.get('author')}")

    # ── Deletions ──────────────────────────────────────────────────────────────
    # Scoped to current snap_date only — we don't flag previous days as deleted
    todays_old = {fp: v for fp, v in old_live_map.items()
                  if v.get("snap_date") == snap_date}
    for fp, item in todays_old.items():
        if fp not in fresh_fps and fp not in del_out:
            di = dict(item)
            di["deleted_on"] = now_str
            del_out[fp] = di
            live_map.pop(fp, None)
            print(f"    🗑️  Deleted: {item.get('branch_name')} – {item.get('author')}")

    # ── Purge old deletion records ─────────────────────────────────────────────
    cutoff = ist_now() - timedelta(days=DELETION_DAYS)
    purged = 0
    for fp in list(del_out.keys()):
        try:
            from datetime import datetime
            d = datetime.strptime(
                del_out[fp].get("deleted_on", ""), "%Y-%m-%d %H:%M"
            ).replace(tzinfo=IST)
            if d < cutoff:
                del_out.pop(fp)
                purged += 1
        except Exception:
            pass
    if purged:
        print(f"    🧹 Purged {purged} deletion record(s) older than {DELETION_DAYS} days")

    return live_map, del_out


# ══════════════════════════════════════════════════════════════════════════════
# Main
# ══════════════════════════════════════════════════════════════════════════════

async def main() -> None:
    slot      = get_run_slot()
    snap_date = get_snap_date(slot)

    print(f"\n{'═' * 64}")
    print(f"  Sathya Review Scraper")
    print(f"  Slot: {slot}  |  snap_date: {snap_date}")
    print(f"  IST:  {ist_now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  Branches: {len(BRANCHES)}")
    print(f"{'═' * 64}\n")

    # ── Load existing data ─────────────────────────────────────────────────────
    old_live_list = load_json(REV_JSON)
    old_del_list  = load_json(DEL_JSON)
    old_live_map  = {r["fingerprint"]: r for r in old_live_list}
    old_del_map   = {r["fingerprint"]: r for r in old_del_list}
    print(f"  Loaded: {len(old_live_map)} live, {len(old_del_map)} deleted\n")

    # ── Scrape all branches concurrently ───────────────────────────────────────
    sem     = asyncio.Semaphore(MAX_CONCURRENT)
    tasks   = [run_branch(b, sem, snap_date) for b in BRANCHES]
    batches = await asyncio.gather(*tasks)

    # Flatten + deduplicate (first occurrence of each fingerprint wins)
    fresh_map: dict[str, dict] = {}
    for batch in batches:
        for r in batch:
            if r["fingerprint"] not in fresh_map:
                fresh_map[r["fingerprint"]] = r

    fresh_fps = set(fresh_map.keys())
    print(f"\n  This run: {len(fresh_fps)} unique reviews within 23h\n")

    # ── Merge fresh reviews into existing live map ─────────────────────────────
    merged = dict(old_live_map)
    new_c = upd_c = 0

    for fp, r in fresh_map.items():
        if fp not in merged:
            # Completely new review
            merged[fp] = r
            new_c += 1
        else:
            # Already known — refresh timing fields, preserve first_seen / snap_date
            existing = dict(merged[fp])
            existing["rel_time"]    = r["rel_time"]
            existing["parsed_date"] = r["parsed_date"]
            existing["scraped_at"]  = r["scraped_at"]
            merged[fp] = existing
            upd_c += 1

    print(f"  Merge: {new_c} new, {upd_c} timing-refreshed")

    # ── Track deletions ────────────────────────────────────────────────────────
    merged, merged_del = track_deletions(
        live_map=merged,
        old_del_map=old_del_map,
        old_live_map=old_live_map,
        fresh_fps=fresh_fps,
        snap_date=snap_date,
    )

    # ── Sort and save ──────────────────────────────────────────────────────────
    final_live = sorted(
        merged.values(),
        key=lambda x: (x.get("snap_date", ""), x.get("parsed_date", "")),
        reverse=True,
    )
    final_del = sorted(
        merged_del.values(),
        key=lambda x: x.get("deleted_on", ""),
        reverse=True,
    )

    save_json(REV_JSON, final_live)
    save_json(DEL_JSON, final_del)

    print(f"\n{'═' * 64}")
    print(f"  ✅  rev.json    : {len(final_live)} reviews")
    print(f"  ✅  deleted.json: {len(final_del)} records")
    print(f"  📸  Debug       : debug/screenshots/  debug/dom/")
    print(f"{'═' * 64}\n")


if __name__ == "__main__":
    asyncio.run(main())
