#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Batch translate PGP document descriptions from English to Hebrew using the
Dicta LM 2.0 Translation API.

Reads source data from pgp.db (documents table), translates descriptions via
the Dicta API with scholarly few-shot prompts, and writes results to the
pgp_translations table in the same sidecar database.

Document types use the manual PGP_DOCUMENT_TYPE_HE mapping (9 fixed values)
instead of API calls, since these are a small fixed taxonomy where manual
translation is more reliable.

Features:
- Checkpointing: saves progress to a JSON file every batch_size translations
- Resume: skips already-translated rows on restart
- Parallel: ThreadPoolExecutor for concurrent API requests
- Dry-run: count candidates without making API calls
- Progress: tqdm progress bar with ETA

Usage:
  python scripts/translate_pgp_descriptions.py                    # Full run
  python scripts/translate_pgp_descriptions.py --dry-run          # Count candidates
  python scripts/translate_pgp_descriptions.py --limit 50         # Test with 50 items
  python scripts/translate_pgp_descriptions.py --workers 10       # Faster (more concurrent)
"""

import argparse
import json
import logging
import os
import sqlite3
import sys
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

# Ensure project root is on path for imports
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from shared.dicta_client import (
    MAX_WORKERS,
    PGP_DOCUMENT_TYPE_HE,
    build_few_shot_prompt,
    load_few_shot_template,
    translate_text,
)
from shared.translation_service import ensure_pgp_translations_table

logger = logging.getLogger(__name__)

# Default paths relative to project root
DEFAULT_PGP_DB = str(PROJECT_ROOT / "pgp_data" / "pgp.db")
DEFAULT_CHECKPOINT = str(PROJECT_ROOT / "translate_pgp_checkpoint.json")
DEFAULT_FEW_SHOT = str(PROJECT_ROOT / "data" / "few_shot_en2he_scholarly.json")
MAX_RETRIES = 3
RETRY_BASE_DELAY = 2.0  # seconds, exponential backoff


# =============================================================================
# Checkpoint Logic
# =============================================================================


def load_checkpoint(path: str) -> set:
    """Load set of completed pgpids from checkpoint JSON file.

    Args:
        path: Path to the checkpoint JSON file.

    Returns:
        Set of pgpid integers that have been completed.
    """
    if not os.path.isfile(path):
        return set()
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return set(data.get("completed_ids", []))
    except (json.JSONDecodeError, IOError) as e:
        logger.warning("Failed to load checkpoint from %s: %s", path, e)
        return set()


def save_checkpoint(path: str, completed_ids: set) -> None:
    """Atomically save set of completed pgpids to checkpoint JSON file.

    Uses write-to-temp + os.replace for atomic writes (no partial files).

    Args:
        path: Path to the checkpoint JSON file.
        completed_ids: Set of pgpid integers that have been completed.
    """
    data = {
        "completed_ids": sorted(completed_ids),
        "count": len(completed_ids),
        "saved_at": datetime.now(timezone.utc).isoformat(),
    }
    # Write to temp file in same directory, then atomic rename
    dir_path = os.path.dirname(os.path.abspath(path))
    os.makedirs(dir_path, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(dir=dir_path, suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)
        os.replace(tmp_path, path)
    except Exception:
        # Clean up temp file on error
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


# =============================================================================
# Translation with Retry
# =============================================================================


def translate_with_retry(
    text: str, few_shot_prompt: str, direction: str = "en2he"
) -> str | None:
    """Translate text with exponential backoff retry on failure.

    Args:
        text: Text to translate.
        few_shot_prompt: Pre-built few-shot prefix.
        direction: Translation direction.

    Returns:
        Translated text, or None after all retries exhausted.
    """
    for attempt in range(MAX_RETRIES):
        result = translate_text(text, few_shot_prompt, direction)
        if result is not None:
            return result
        if attempt < MAX_RETRIES - 1:
            delay = RETRY_BASE_DELAY * (2 ** attempt)
            logger.info(
                "Retry %d/%d after %.1fs for text: %.50s...",
                attempt + 2, MAX_RETRIES, delay, text[:50],
            )
            time.sleep(delay)
    return None


# =============================================================================
# Main Batch Flow
# =============================================================================


def get_candidates(
    db_path: str, min_length: int
) -> list[tuple[int, str, str | None]]:
    """Read candidate rows from pgp.db documents table.

    Args:
        db_path: Path to pgp.db.
        min_length: Minimum description length to include.

    Returns:
        List of (pgpid, description, document_type) tuples.
    """
    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute(
            "SELECT pgpid, description, document_type FROM documents "
            "WHERE description IS NOT NULL AND description != '' "
            "AND length(description) >= ?",
            (min_length,),
        ).fetchall()
        return [(r[0], r[1], r[2]) for r in rows]
    finally:
        conn.close()


def flush_batch(
    conn: sqlite3.Connection,
    results: list[tuple[int, str, str | None]],
) -> int:
    """Write a batch of translations to pgp_translations table.

    Args:
        conn: Writable connection to pgp.db.
        results: List of (pgpid, description_he, document_type_he) tuples.

    Returns:
        Number of rows written.
    """
    if not results:
        return 0
    now = datetime.now(timezone.utc).isoformat()
    conn.executemany(
        "INSERT OR REPLACE INTO pgp_translations "
        "(pgpid, description_he, document_type_he, translated_at, model_version) "
        "VALUES (?, ?, ?, ?, ?)",
        [(pgpid, desc_he, dtype_he, now, "dictalm2.0") for pgpid, desc_he, dtype_he in results],
    )
    conn.commit()
    return len(results)


def run_batch(args: argparse.Namespace) -> None:
    """Execute the batch translation pipeline.

    Args:
        args: Parsed CLI arguments.
    """
    # Try importing tqdm for progress bar; fall back to no-op if unavailable
    try:
        from tqdm import tqdm
    except ImportError:
        tqdm = None

    # Load candidates
    print(f"Loading candidates from {args.pgp_db} (min_length={args.min_length})...")
    candidates = get_candidates(args.pgp_db, args.min_length)
    total_candidates = len(candidates)
    print(f"Found {total_candidates} candidate descriptions.")

    if args.dry_run:
        # Count document types
        doc_types = {}
        for _, _, dtype in candidates:
            if dtype:
                doc_types[dtype] = doc_types.get(dtype, 0) + 1
        print(f"\nDocument type distribution ({len(doc_types)} types):")
        for dt, count in sorted(doc_types.items(), key=lambda x: -x[1]):
            he = PGP_DOCUMENT_TYPE_HE.get(dt, "?")
            print(f"  {dt}: {count} -> {he}")

        mapped = sum(1 for _, _, dt in candidates if dt and dt in PGP_DOCUMENT_TYPE_HE)
        print(f"\nDocument types with manual HE mapping: {mapped}/{total_candidates}")
        print("Dry run complete. No translations performed.")
        return

    # Load checkpoint
    completed_ids = load_checkpoint(args.checkpoint_file)
    if completed_ids:
        print(f"Checkpoint loaded: {len(completed_ids)} already completed.")

    # Filter out already-completed
    pending = [(pgpid, desc, dtype) for pgpid, desc, dtype in candidates if pgpid not in completed_ids]
    if args.limit:
        pending = pending[: args.limit]

    total_pending = len(pending)
    print(f"Pending translations: {total_pending}")

    if total_pending == 0:
        print("Nothing to translate. All candidates already completed.")
        return

    # Load few-shot template
    few_shot_path = args.few_shot or DEFAULT_FEW_SHOT
    print(f"Loading few-shot template from {few_shot_path}...")
    template = load_few_shot_template(few_shot_path)
    few_shot_prompt = build_few_shot_prompt(template, direction="en2he")

    # Open writable connection for pgp_translations
    write_conn = sqlite3.connect(args.pgp_db)
    ensure_pgp_translations_table(write_conn)

    # Statistics
    translated_count = 0
    skipped_count = 0
    failed_count = 0
    batch_buffer: list[tuple[int, str, str | None]] = []
    start_time = time.time()

    # Progress bar
    pbar = tqdm(total=total_pending, desc="Translating", unit="doc") if tqdm else None

    def process_one(pgpid: int, description: str, document_type: str | None):
        """Translate one document (runs in worker thread)."""
        # Translate description via API
        desc_he = translate_with_retry(description, few_shot_prompt, "en2he")

        # Document type via manual mapping (no API call)
        dtype_he = PGP_DOCUMENT_TYPE_HE.get(document_type, None) if document_type else None

        return pgpid, desc_he, dtype_he

    try:
        with ThreadPoolExecutor(max_workers=min(args.workers, MAX_WORKERS)) as pool:
            futures = {}
            for pgpid, desc, dtype in pending:
                f = pool.submit(process_one, pgpid, desc, dtype)
                futures[f] = pgpid

            for f in as_completed(futures):
                pgpid = futures[f]
                try:
                    result_pgpid, desc_he, dtype_he = f.result()

                    if desc_he is not None:
                        batch_buffer.append((result_pgpid, desc_he, dtype_he))
                        completed_ids.add(result_pgpid)
                        translated_count += 1
                    else:
                        failed_count += 1
                        logger.warning("Translation failed for pgpid=%d", pgpid)

                except Exception as e:
                    failed_count += 1
                    logger.error("Error processing pgpid=%d: %s", pgpid, e)

                # Update progress
                if pbar:
                    pbar.update(1)

                # Flush batch at interval
                if len(batch_buffer) >= args.batch_size:
                    flush_batch(write_conn, batch_buffer)
                    save_checkpoint(args.checkpoint_file, completed_ids)
                    batch_buffer.clear()

    except KeyboardInterrupt:
        print("\n\nInterrupted! Saving checkpoint...")
        # Flush any remaining buffer
        if batch_buffer:
            flush_batch(write_conn, batch_buffer)
        save_checkpoint(args.checkpoint_file, completed_ids)
        print(f"Checkpoint saved with {len(completed_ids)} completed IDs.")
        print("Resume by running the script again.")
        return
    finally:
        if pbar:
            pbar.close()

    # Flush remaining buffer
    if batch_buffer:
        flush_batch(write_conn, batch_buffer)
        save_checkpoint(args.checkpoint_file, completed_ids)

    write_conn.close()

    # Summary
    elapsed = time.time() - start_time
    minutes = elapsed / 60
    print(f"\n{'=' * 60}")
    print("Translation Summary")
    print(f"{'=' * 60}")
    print(f"  Total candidates:   {total_candidates}")
    print(f"  Previously done:    {total_candidates - total_pending}")
    print(f"  Translated now:     {translated_count}")
    print(f"  Failed:             {failed_count}")
    print(f"  Elapsed:            {minutes:.1f} min ({elapsed:.0f}s)")
    if translated_count > 0:
        rate = elapsed / translated_count
        print(f"  Rate:               {rate:.2f}s per document")
        remaining = total_candidates - len(completed_ids)
        if remaining > 0:
            eta_min = (remaining * rate) / 60
            print(f"  Remaining:          {remaining} docs (~{eta_min:.0f} min)")
    print(f"  Checkpoint:         {args.checkpoint_file}")
    print(f"{'=' * 60}")


# =============================================================================
# CLI Entry Point
# =============================================================================


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse command-line arguments.

    Args:
        argv: Optional argument list (for testing). Defaults to sys.argv[1:].

    Returns:
        Parsed arguments namespace.
    """
    parser = argparse.ArgumentParser(
        description="Batch translate PGP descriptions from English to Hebrew via Dicta API.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""Examples:
  python scripts/translate_pgp_descriptions.py                    # Full run
  python scripts/translate_pgp_descriptions.py --dry-run          # Count candidates
  python scripts/translate_pgp_descriptions.py --limit 50         # Test with 50 items
  python scripts/translate_pgp_descriptions.py --workers 10       # Faster (more concurrent)
""",
    )
    parser.add_argument(
        "--pgp-db",
        default=DEFAULT_PGP_DB,
        help=f"Path to pgp.db sidecar (default: {DEFAULT_PGP_DB})",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=MAX_WORKERS,
        metavar="N",
        help=f"Concurrent API workers (default: {MAX_WORKERS}, max: {MAX_WORKERS})",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=100,
        help="Checkpoint every N translations (default: 100)",
    )
    parser.add_argument(
        "--checkpoint-file",
        default=DEFAULT_CHECKPOINT,
        help=f"Checkpoint JSON path (default: {DEFAULT_CHECKPOINT})",
    )
    parser.add_argument(
        "--min-length",
        type=int,
        default=20,
        help="Minimum description length to translate (default: 20 chars)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Count candidates without translating",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Translate only first N rows (for testing)",
    )
    parser.add_argument(
        "--few-shot",
        default=None,
        help=f"Path to few-shot template JSON (default: {DEFAULT_FEW_SHOT})",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose logging",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    """Main entry point."""
    args = parse_args(argv)

    # Configure logging
    level = logging.DEBUG if args.verbose else logging.WARNING
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    run_batch(args)


if __name__ == "__main__":
    main()
