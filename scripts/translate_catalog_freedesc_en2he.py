#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Batch translate English catalog free descriptions to Hebrew (en2he)
using the Dicta LM 2.0 Translation API.

These are scholarly catalog descriptions from Penn, Halper, Danzig, CUL, etc.
that were originally written in English. The previous batch run misclassified
them as Hebrew and produced useless he2en (English→English) translations.

This script:
1. Finds catalog_free_desc entries from '*Catalog*' sources
2. Skips entries that already have en2he translations
3. Deletes bad he2en translations where original ≈ translated (English→English)
4. Translates English→Hebrew using Dicta LM 2.0

~186K entries, ~13 hours at 5 workers.

Usage:
  python scripts/translate_catalog_freedesc_en2he.py --dry-run
  python scripts/translate_catalog_freedesc_en2he.py --limit 100
  python scripts/translate_catalog_freedesc_en2he.py --workers 5
  python scripts/translate_catalog_freedesc_en2he.py --workers 5 --cleanup
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
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from shared.dicta_client import (
    GOD_MODE,
    MAX_WORKERS,
    build_few_shot_prompt,
    load_few_shot_template,
    translate_text,
)
from shared.translation_service import ensure_fjms_translations_table

logger = logging.getLogger(__name__)

DEFAULT_FJMS_DB = str(PROJECT_ROOT / "fist_data" / "fjms_enrichment.db")
DEFAULT_CHECKPOINT = str(PROJECT_ROOT / "translate_catalog_freedesc_en2he_checkpoint.json")
DEFAULT_FEW_SHOT = str(PROJECT_ROOT / "data" / "few_shot_en2he_scholarly.json")

MAX_RETRIES = 3
RETRY_BASE_DELAY = 1.0
RETRY_MAX_DELAY = 30.0
LOG_INTERVAL = 1000
RECONNECT_INTERVAL = 10000


def load_checkpoint(path: str) -> set:
    if not os.path.isfile(path):
        return set()
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return set(data.get("completed_ids", []))
    except (json.JSONDecodeError, IOError):
        return set()


def save_checkpoint(path: str, completed_ids: set, stats: dict | None = None):
    data = {
        "completed_ids": sorted(completed_ids),
        "count": len(completed_ids),
        "saved_at": datetime.now(timezone.utc).isoformat(),
    }
    if stats:
        data["stats"] = stats
    dir_path = os.path.dirname(os.path.abspath(path))
    os.makedirs(dir_path, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(dir=dir_path, suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)
        os.replace(tmp_path, path)
    except Exception:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


def translate_with_retry(text: str, few_shot_prompt: str, direction: str = "en2he") -> str | None:
    for attempt in range(MAX_RETRIES):
        result = translate_text(text, few_shot_prompt, direction)
        if result is not None:
            return result
        if attempt < MAX_RETRIES - 1:
            delay = min(RETRY_BASE_DELAY * (2 ** attempt), RETRY_MAX_DELAY)
            logger.info("Retry %d/%d after %.1fs", attempt + 2, MAX_RETRIES, delay)
            time.sleep(delay)
    return None


def get_candidates(conn: sqlite3.Connection, min_length: int) -> list[tuple[str, str, str]]:
    """Get English catalog free descriptions needing en2he translation."""
    rows = conn.execute(
        """SELECT fd.SignatureId, fd.AlmaId, fd.FreeDesc
        FROM catalog_free_desc fd
        WHERE fd.SourceName LIKE '%Catalog%'
        AND fd.FreeDesc IS NOT NULL AND length(fd.FreeDesc) >= ?
        AND NOT EXISTS (
            SELECT 1 FROM fjms_translations t
            WHERE t.alma_id = fd.AlmaId
            AND t.field_name = 'FreeDesc'
            AND t.signature_id = fd.SignatureId
            AND t.direction = 'en2he'
        )""",
        (min_length,),
    ).fetchall()
    return [(str(r[0]), r[1], r[2]) for r in rows]


def cleanup_bad_translations(conn: sqlite3.Connection, dry_run: bool = False) -> int:
    """Delete he2en translations where English catalog text was 'translated' to ~same English."""
    # Find he2en FreeDesc translations from catalog sources where text is nearly identical
    rows = conn.execute(
        """SELECT t.id, t.alma_id, t.signature_id, length(t.original_text), length(t.translated_text)
        FROM fjms_translations t
        JOIN catalog_free_desc fd ON fd.AlmaId = t.alma_id AND fd.SignatureId = t.signature_id
        WHERE t.field_name = 'FreeDesc'
        AND t.direction = 'he2en'
        AND fd.SourceName LIKE '%Catalog%'
        AND abs(length(t.original_text) - length(t.translated_text)) < 20"""
    ).fetchall()
    count = len(rows)
    if not dry_run and count > 0:
        ids = [r[0] for r in rows]
        # Delete in batches
        for i in range(0, len(ids), 1000):
            batch = ids[i:i+1000]
            placeholders = ','.join('?' * len(batch))
            conn.execute(f"DELETE FROM fjms_translations WHERE id IN ({placeholders})", batch)
        conn.commit()
    return count


def format_eta(seconds: float) -> str:
    if seconds < 60:
        return f"{seconds:.0f}s"
    minutes = seconds / 60
    if minutes < 60:
        return f"{minutes:.0f}m"
    hours = minutes / 60
    remaining_min = minutes % 60
    return f"{hours:.0f}h {remaining_min:.0f}m"


def run(args: argparse.Namespace) -> None:
    try:
        from tqdm import tqdm
    except ImportError:
        tqdm = None

    checkpoint_path = args.checkpoint_file or DEFAULT_CHECKPOINT

    print(f"Loading English catalog free description candidates from {args.fjms_db}")
    conn = sqlite3.connect(args.fjms_db)

    if args.cleanup:
        bad_count = cleanup_bad_translations(conn, dry_run=args.dry_run)
        print(f"{'Would delete' if args.dry_run else 'Deleted'} {bad_count:,} bad he2en translations")
        if args.dry_run:
            conn.close()
            return

    candidates = get_candidates(conn, args.min_length)
    total_candidates = len(candidates)
    print(f"Found {total_candidates:,} English catalog entries needing en2he translation.")

    if args.dry_run:
        lengths = [len(c[2]) for c in candidates]
        if lengths:
            avg_len = sum(lengths) / len(lengths)
            print(f"\nLength distribution:")
            print(f"  Min: {min(lengths):,} chars")
            print(f"  Max: {max(lengths):,} chars")
            print(f"  Avg: {avg_len:,.0f} chars")
        est_hours = total_candidates * 0.26 / 3600
        print(f"\nEstimated time at 5 workers: ~{est_hours:.0f} hours")
        print("Dry run complete.")
        conn.close()
        return

    # Load checkpoint
    completed_ids = load_checkpoint(checkpoint_path)
    if completed_ids:
        print(f"Resuming from checkpoint: {len(completed_ids):,} already done.")

    pending = [
        (sig_id, alma_id, text)
        for sig_id, alma_id, text in candidates
        if sig_id not in completed_ids
    ]
    if args.limit:
        pending = pending[:args.limit]

    total_pending = len(pending)
    print(f"Pending translations: {total_pending:,}")

    if total_pending == 0:
        print("Nothing to translate.")
        conn.close()
        return

    if not GOD_MODE:
        print("ERROR: GOD_MODE not enabled. Set DICTA_GOD_MODE=1 environment variable.")
        conn.close()
        return

    # Load few-shot template
    print("Loading EN->HE few-shot template...")
    template = load_few_shot_template(DEFAULT_FEW_SHOT)
    few_shot_prompt = build_few_shot_prompt(template, direction="en2he")

    ensure_fjms_translations_table(conn)

    # SIGINT handler
    interrupted = [False]
    def sigint_handler(signum, frame):
        interrupted[0] = True
        print("\n\nSIGINT received. Saving checkpoint...")
    original_handler = signal.signal(signal.SIGINT, sigint_handler)

    translated_count = 0
    failed_count = 0
    start_time = time.time()
    batch_size = 100
    workers = min(args.workers, MAX_WORKERS)

    print(f"\nStarting en2he translation with {workers} workers...")
    print(f"God mode: {GOD_MODE}")

    pbar = tqdm(total=total_pending, desc="Translating", unit="item") if tqdm else None

    try:
        for batch_start in range(0, total_pending, batch_size):
            if interrupted[0]:
                break
            batch = pending[batch_start:batch_start + batch_size]

            with ThreadPoolExecutor(max_workers=workers) as executor:
                future_to_item = {}
                for sig_id, alma_id, text in batch:
                    if interrupted[0]:
                        break
                    f = executor.submit(translate_with_retry, text, few_shot_prompt, "en2he")
                    future_to_item[f] = (sig_id, alma_id, text)

                for future in as_completed(future_to_item):
                    if interrupted[0]:
                        break
                    sig_id, alma_id, text = future_to_item[future]
                    try:
                        result = future.result()
                    except Exception as exc:
                        logger.warning("Translation error for %s: %s", sig_id, exc)
                        failed_count += 1
                        continue

                    if result:
                        write_translation(
                            conn, alma_id, "FreeDesc", sig_id,
                            text, result, "en2he"
                        )
                        translated_count += 1
                        completed_ids.add(sig_id)
                    else:
                        failed_count += 1

                    if pbar:
                        pbar.update(1)

            # Commit + checkpoint after each batch
            conn.commit()
            save_checkpoint(checkpoint_path, completed_ids, {
                "translated": translated_count,
                "failed": failed_count,
                "elapsed_seconds": time.time() - start_time,
            })

            # Progress log
            total_done = translated_count + failed_count
            if total_done > 0 and total_done % LOG_INTERVAL < batch_size:
                elapsed = time.time() - start_time
                rate = translated_count / elapsed if elapsed > 0 else 0
                remaining = (total_pending - total_done) / rate if rate > 0 else 0
                print(
                    f"[{total_done:,}/{total_pending:,}] "
                    f"{translated_count:,} OK, {failed_count:,} failed, "
                    f"{rate:.1f}/s, ETA {format_eta(remaining)}"
                )

            # Reconnect periodically
            if total_done > 0 and total_done % RECONNECT_INTERVAL < batch_size:
                conn.close()
                conn = sqlite3.connect(args.fjms_db)

    finally:
        if pbar:
            pbar.close()
        signal.signal(signal.SIGINT, original_handler)
        conn.commit()
        save_checkpoint(checkpoint_path, completed_ids, {
            "translated": translated_count,
            "failed": failed_count,
            "elapsed_seconds": time.time() - start_time,
        })
        conn.close()

    elapsed = time.time() - start_time
    print(f"\nDone! {translated_count:,} translated, {failed_count:,} failed in {format_eta(elapsed)}")
    print(f"Checkpoint: {checkpoint_path}")


def write_translation(conn, alma_id, field_name, signature_id, original_text, translated_text, direction):
    now = datetime.now(timezone.utc).isoformat()
    sig_id = int(signature_id) if signature_id is not None else None
    conn.execute(
        "INSERT INTO fjms_translations "
        "(alma_id, field_name, signature_id, original_text, translated_text, "
        "direction, translated_at, model_version) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (alma_id, field_name, sig_id, original_text, translated_text, direction, now, "dictalm2.0"),
    )


def main():
    parser = argparse.ArgumentParser(
        description="Translate English catalog free descriptions to Hebrew (en2he)"
    )
    parser.add_argument("--fjms-db", default=DEFAULT_FJMS_DB, help="Path to fjms_enrichment.db")
    parser.add_argument("--checkpoint-file", default=None, help="Checkpoint file path")
    parser.add_argument("--dry-run", action="store_true", help="Count candidates only")
    parser.add_argument("--limit", type=int, default=0, help="Limit number of translations")
    parser.add_argument("--workers", type=int, default=5, help="Number of parallel workers")
    parser.add_argument("--min-length", type=int, default=10, help="Minimum text length")
    parser.add_argument("--cleanup", action="store_true", help="Delete bad he2en translations first")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )

    run(args)


if __name__ == "__main__":
    main()
