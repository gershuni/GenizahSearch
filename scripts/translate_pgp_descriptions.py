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
- Sequential execution with throttle (safe for Dicta rate limits)
- Dry-run: count candidates without making API calls
- SIGINT: graceful shutdown with checkpoint save

Usage:
  python scripts/translate_pgp_descriptions.py                    # Full run
  python scripts/translate_pgp_descriptions.py --dry-run          # Count candidates
  python scripts/translate_pgp_descriptions.py --limit 50         # Test with 50 items
  python scripts/translate_pgp_descriptions.py --delay 5.0        # Custom throttle
"""

import argparse
import io
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

# Force UTF-8 stdout/stderr on Windows (needed for nohup/redirect with Hebrew text)
if hasattr(sys.stdout, 'buffer'):
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace', line_buffering=True)
if hasattr(sys.stderr, 'buffer'):
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace', line_buffering=True)

# Ensure project root is on path for imports
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from shared.dicta_client import (
    GOD_MODE,
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
REQUEST_DELAY = 0.0 if GOD_MODE else 3.0  # seconds between API calls
MAX_RETRIES = 3
RETRY_BASE_DELAY = 2.0  # seconds, exponential backoff

# Graceful shutdown
_shutdown_requested = False

def _signal_handler(signum, frame):
    global _shutdown_requested
    _shutdown_requested = True
    print("\n[SIGINT] Shutdown requested - saving checkpoint after current item...")

signal.signal(signal.SIGINT, _signal_handler)


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
    db_path: str, min_length: int, retranslate_nulls: bool = False,
) -> list[tuple[int, str, str | None]]:
    """Read candidate rows from pgp.db documents table.

    Args:
        db_path: Path to pgp.db.
        min_length: Minimum description length to include.
        retranslate_nulls: If True, return only rows that exist in
            pgp_translations but have NULL description_he (cleaned-out
            hallucinated translations that need re-translation).

    Returns:
        List of (pgpid, description, document_type) tuples.
    """
    conn = sqlite3.connect(db_path)
    try:
        if retranslate_nulls:
            rows = conn.execute(
                "SELECT d.pgpid, d.description, d.document_type "
                "FROM documents d "
                "JOIN pgp_translations t ON d.pgpid = t.pgpid "
                "WHERE d.description IS NOT NULL AND d.description != '' "
                "AND length(d.description) >= ? "
                "AND (t.description_he IS NULL OR t.description_he = '')",
                (min_length,),
            ).fetchall()
        else:
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
    retranslate = getattr(args, 'retranslate_nulls', False)
    candidates = get_candidates(args.pgp_db, args.min_length, retranslate_nulls=retranslate)
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

    # Filter out already-completed (skip filter for --retranslate-nulls since
    # those pgpids are in the checkpoint but need re-translation)
    if retranslate:
        pending = candidates  # SQL already filtered to NULL description_he
    else:
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

    delay = getattr(args, 'delay', REQUEST_DELAY)

    try:
        for i, (pgpid, desc, dtype) in enumerate(pending):
            if _shutdown_requested:
                logger.info("Shutdown requested - saving checkpoint")
                break

            # Translate description via API
            desc_he = translate_with_retry(desc, few_shot_prompt, "en2he")

            # Document type via manual mapping (no API call)
            dtype_he = PGP_DOCUMENT_TYPE_HE.get(dtype, None) if dtype else None

            if desc_he is not None:
                batch_buffer.append((pgpid, desc_he, dtype_he))
                completed_ids.add(pgpid)
                translated_count += 1
            else:
                failed_count += 1
                logger.warning("Translation failed for pgpid=%d", pgpid)

            # Log progress every 100 items
            if (i + 1) % 100 == 0:
                elapsed = time.time() - start_time
                rate = translated_count / elapsed * 3600 if elapsed > 0 else 0
                remaining_est = (total_pending - i - 1) / rate * 3600 if rate > 0 else 0
                logger.info(
                    "[%d/%d] translated=%d failed=%d [%.0f/hr, ~%.1fh left]",
                    i + 1, total_pending, translated_count, failed_count,
                    rate, remaining_est / 3600,
                )

            # Flush batch at interval
            if len(batch_buffer) >= args.batch_size:
                flush_batch(write_conn, batch_buffer)
                save_checkpoint(args.checkpoint_file, completed_ids)
                batch_buffer.clear()

            # Throttle between API calls
            if delay > 0 and i < total_pending - 1:
                time.sleep(delay)

    except Exception as e:
        logger.error("Fatal error in translation loop: %s", e, exc_info=True)

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
        "--delay",
        type=float,
        default=REQUEST_DELAY,
        help=f"Seconds between API calls (default: {REQUEST_DELAY})",
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
        "--retranslate-nulls",
        action="store_true",
        help="Re-translate only rows with NULL description_he (cleaned hallucinations)",
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

    # Configure logging (console + file)
    log_file = str(PROJECT_ROOT / "translate_pgp_log.txt")
    level = logging.DEBUG if args.verbose else logging.INFO
    handlers = [
        logging.StreamHandler(sys.stderr),
        logging.FileHandler(log_file, encoding='utf-8', mode='a'),
    ]
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=handlers,
    )

    run_batch(args)


if __name__ == "__main__":
    main()
