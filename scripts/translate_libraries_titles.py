#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Batch translate Hebrew-only titles from libraries.csv via Dicta LM 2.0 API.

Reads pending_dicta records from libraries_translations.db, deduplicates
identical title strings, translates each unique string once via Dicta,
and writes results back to the DB for all matching system_numbers.

Phase C of the Translation Master Plan.

Usage:
  python scripts/translate_libraries_titles.py --dry-run        # Count pending
  python scripts/translate_libraries_titles.py --limit 50       # Test with 50
  python scripts/translate_libraries_titles.py                  # Full run
  python scripts/translate_libraries_titles.py --delay 5.0      # Slower throttle
"""

import argparse
import json
import logging
import os
import signal
import sqlite3
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from shared.dicta_client import (
    build_few_shot_prompt,
    load_few_shot_template,
    translate_text,
)

logger = logging.getLogger(__name__)

DEFAULT_DB = str(PROJECT_ROOT / "libraries_translations.db")
DEFAULT_CHECKPOINT = str(PROJECT_ROOT / "translate_libraries_titles_checkpoint.json")
DEFAULT_FEW_SHOT = str(PROJECT_ROOT / "data" / "few_shot_he2en_scholarly.json")
REQUEST_DELAY = 3.0

# Graceful shutdown
_shutdown_requested = False


def _signal_handler(signum, frame):
    global _shutdown_requested
    _shutdown_requested = True
    print("\n[SIGINT] Shutdown requested — saving checkpoint after current item...")


signal.signal(signal.SIGINT, _signal_handler)


# =============================================================================
# Checkpoint
# =============================================================================

def load_checkpoint(path: str) -> set:
    """Load set of completed original_title hashes."""
    if not os.path.isfile(path):
        return set()
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return set(data.get("completed", []))
    except (json.JSONDecodeError, KeyError):
        return set()


def save_checkpoint(path: str, completed: set, stats: dict) -> None:
    """Atomically save checkpoint."""
    data = {
        "completed": list(completed),
        "count": len(completed),
        "stats": stats,
        "saved_at": datetime.now(timezone.utc).isoformat(),
    }
    fd, tmp = tempfile.mkstemp(dir=os.path.dirname(path) or ".", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)
        os.replace(tmp, path)
    except Exception:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


# =============================================================================
# Main Logic
# =============================================================================

def get_pending_unique_titles(db_path: str) -> list[tuple[str, int]]:
    """Get unique Hebrew-only titles that need translation.

    Returns list of (original_title, row_count) sorted by count descending
    (most common first — maximizes dedup value early).
    """
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("""
        SELECT original_title, COUNT(*) as cnt
        FROM title_translations
        WHERE source = 'pending_dicta'
          AND english_title IS NULL
        GROUP BY original_title
        ORDER BY cnt DESC
    """)
    results = cur.fetchall()
    conn.close()
    return results


def apply_translation(db_path: str, original_title: str, english: str) -> int:
    """Write translation to all rows matching this original_title.

    Returns number of rows updated.
    """
    conn = sqlite3.connect(db_path)
    now = datetime.now(timezone.utc).isoformat()
    cur = conn.execute("""
        UPDATE title_translations
        SET english_title = ?,
            source = 'dicta',
            translated_at = ?
        WHERE original_title = ?
          AND source = 'pending_dicta'
    """, (english, now, original_title))
    updated = cur.rowcount
    conn.commit()
    conn.close()
    return updated


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Translate Hebrew-only library titles via Dicta API"
    )
    parser.add_argument("--db", default=DEFAULT_DB,
                        help=f"Path to libraries_translations.db (default: {DEFAULT_DB})")
    parser.add_argument("--checkpoint", default=DEFAULT_CHECKPOINT,
                        help="Checkpoint file path")
    parser.add_argument("--few-shot", default=DEFAULT_FEW_SHOT,
                        help="Few-shot template file")
    parser.add_argument("--delay", type=float, default=REQUEST_DELAY,
                        help=f"Seconds between API calls (default: {REQUEST_DELAY})")
    parser.add_argument("--limit", type=int, default=0,
                        help="Max items to translate (0 = unlimited)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Count pending items without translating")
    parser.add_argument("--batch-size", type=int, default=50,
                        help="Save checkpoint every N items")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    if not os.path.isfile(args.db):
        logger.error(f"Database not found: {args.db}")
        logger.error("Run extract_libraries_english.py first to create it.")
        sys.exit(1)

    # Load few-shot template
    template = load_few_shot_template(args.few_shot)
    few_shot = build_few_shot_prompt(template, direction="he2en")
    logger.info(f"Loaded few-shot template: {args.few_shot}")

    # Get pending unique titles
    pending = get_pending_unique_titles(args.db)
    total_unique = len(pending)
    total_rows = sum(cnt for _, cnt in pending)

    logger.info(f"Pending: {total_unique:,} unique strings covering {total_rows:,} rows")

    if args.dry_run:
        print(f"\n[DRY RUN] {total_unique:,} unique titles to translate")
        print(f"[DRY RUN] Covering {total_rows:,} total rows")
        print(f"[DRY RUN] Estimated time: {total_unique * args.delay / 3600:.1f} hours")
        print(f"\nTop 10 by frequency:")
        for title, cnt in pending[:10]:
            print(f"  {cnt:>6,}x  {title[:80]}")
        return

    # Load checkpoint
    completed = load_checkpoint(args.checkpoint)
    logger.info(f"Checkpoint: {len(completed):,} already completed")

    # Filter out completed
    pending = [(t, c) for t, c in pending if t not in completed]
    logger.info(f"Remaining: {len(pending):,} unique strings")

    if not pending:
        logger.info("Nothing to translate — all done!")
        return

    if args.limit > 0:
        pending = pending[:args.limit]
        logger.info(f"Limited to {len(pending):,} items")

    stats = {
        "translated": 0,
        "failed": 0,
        "rows_updated": 0,
        "api_calls": 0,
    }

    start_time = time.time()

    for i, (title, row_count) in enumerate(pending):
        if _shutdown_requested:
            logger.info("Shutdown requested — saving checkpoint and exiting")
            break

        stats["api_calls"] += 1
        result = translate_text(title, few_shot, direction="he2en")

        if result:
            updated = apply_translation(args.db, title, result)
            completed.add(title)
            stats["translated"] += 1
            stats["rows_updated"] += updated

            if args.verbose or (i + 1) % 100 == 0:
                elapsed = time.time() - start_time
                rate = stats["translated"] / elapsed * 3600 if elapsed > 0 else 0
                remaining = (len(pending) - i - 1) / rate * 3600 if rate > 0 else 0
                logger.info(
                    f"[{i+1}/{len(pending)}] "
                    f"({row_count}x) {title[:50]} -> {result[:50]} "
                    f"[{rate:.0f}/hr, ~{remaining/3600:.1f}h left]"
                )
        else:
            stats["failed"] += 1
            logger.warning(f"[{i+1}] FAILED: {title[:80]}")

        # Save checkpoint periodically
        if (i + 1) % args.batch_size == 0:
            save_checkpoint(args.checkpoint, completed, stats)
            logger.debug(f"Checkpoint saved at item {i+1}")

        # Throttle
        if i < len(pending) - 1:
            time.sleep(args.delay)

    # Final checkpoint
    save_checkpoint(args.checkpoint, completed, stats)

    elapsed = time.time() - start_time
    logger.info(f"\nDone in {elapsed/3600:.1f} hours")
    logger.info(f"  Translated:   {stats['translated']:,} unique strings")
    logger.info(f"  Rows updated: {stats['rows_updated']:,}")
    logger.info(f"  Failed:       {stats['failed']:,}")
    logger.info(f"  API calls:    {stats['api_calls']:,}")

    # Verify DB state
    conn = sqlite3.connect(args.db)
    total = conn.execute("SELECT COUNT(*) FROM title_translations").fetchone()[0]
    eng = conn.execute(
        "SELECT COUNT(*) FROM title_translations WHERE english_title IS NOT NULL"
    ).fetchone()[0]
    still_pending = conn.execute(
        "SELECT COUNT(*) FROM title_translations WHERE source = 'pending_dicta'"
    ).fetchone()[0]
    conn.close()

    logger.info(f"\nDB state: {total:,} total, {eng:,} with English, {still_pending:,} still pending")


if __name__ == "__main__":
    main()
