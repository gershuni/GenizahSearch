#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Batch translate FJMS free descriptions and bibliography entries from Hebrew to
English using the Dicta LM 2.0 Translation API.

Free descriptions (~303K entries, ~22 hours) are the primary mode. Bibliography
translation is scaffolded but clearly marked as deferred (~542K entries,
~40 hours).

Designed for very long-running batch operations with robust error handling:
- Exponential backoff on API errors (1s, 2s, 4s, max 30s, 3 retries)
- Checkpoint every batch_size items (atomic JSON write)
- SIGINT handler saves checkpoint before exit
- Progress logging every 1,000 translations with rate and ETA
- SQLite connection refresh every 10,000 items

Usage:
  python scripts/translate_fjms_free_desc.py --dry-run              # Count candidates
  python scripts/translate_fjms_free_desc.py --limit 100            # Test with 100
  python scripts/translate_fjms_free_desc.py --workers 10           # Full run, faster
  python scripts/translate_fjms_free_desc.py --mode bibliography --dry-run  # Count bib entries
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

# Ensure project root is on path for imports
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from shared.dicta_client import (
    MAX_WORKERS,
    build_few_shot_prompt,
    load_few_shot_template,
    translate_text,
)
from shared.translation_service import ensure_fjms_translations_table

logger = logging.getLogger(__name__)

# Default paths relative to project root
DEFAULT_FJMS_DB = str(PROJECT_ROOT / "fist_data" / "fjms_enrichment.db")
DEFAULT_FREEDESC_CHECKPOINT = str(
    PROJECT_ROOT / "translate_fjms_freedesc_checkpoint.json"
)
DEFAULT_BIB_CHECKPOINT = str(PROJECT_ROOT / "translate_fjms_bib_checkpoint.json")
DEFAULT_FEW_SHOT_HE2EN = str(PROJECT_ROOT / "data" / "few_shot_he2en_scholarly.json")

MAX_RETRIES = 3
RETRY_BASE_DELAY = 1.0  # seconds
RETRY_MAX_DELAY = 30.0  # seconds
LOG_INTERVAL = 1000  # log progress every N translations
RECONNECT_INTERVAL = 10000  # refresh SQLite connection every N items


# =============================================================================
# Checkpoint Logic
# =============================================================================


def load_checkpoint(path: str) -> set:
    """Load set of completed IDs from checkpoint JSON file.

    Args:
        path: Path to checkpoint JSON file.

    Returns:
        Set of completed ID strings.
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


def save_checkpoint(
    path: str, completed_ids: set, stats: dict | None = None
) -> None:
    """Atomically save checkpoint to JSON file.

    Args:
        path: Path to checkpoint JSON file.
        completed_ids: Set of completed ID strings.
        stats: Optional dict of runtime stats to include.
    """
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


# =============================================================================
# Translation with Retry (Exponential Backoff)
# =============================================================================


def translate_with_retry(
    text: str, few_shot_prompt: str, direction: str = "he2en"
) -> str | None:
    """Translate text with exponential backoff retry on failure.

    Backoff delays: 1s, 2s, 4s capped at 30s.

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
            delay = min(RETRY_BASE_DELAY * (2 ** attempt), RETRY_MAX_DELAY)
            logger.info(
                "Retry %d/%d after %.1fs for text: %.50s...",
                attempt + 2, MAX_RETRIES, delay, text[:50],
            )
            time.sleep(delay)
    return None


# =============================================================================
# Free Description Candidates
# =============================================================================


def get_freedesc_candidates(
    conn: sqlite3.Connection, min_length: int
) -> list[tuple[str, str, str]]:
    """Get free description candidates for translation.

    Args:
        conn: SQLite connection to fjms_enrichment.db.
        min_length: Minimum FreeDesc length to include.

    Returns:
        List of (SignatureId as str, AlmaId, FreeDesc) tuples.
    """
    rows = conn.execute(
        "SELECT SignatureId, AlmaId, FreeDesc FROM catalog_free_desc "
        "WHERE FreeDesc IS NOT NULL AND length(FreeDesc) >= ?",
        (min_length,),
    ).fetchall()
    return [(str(r[0]), r[1], r[2]) for r in rows]


def get_already_translated_freedesc(conn: sqlite3.Connection) -> set:
    """Get set of signature_ids already translated.

    Args:
        conn: SQLite connection to fjms_enrichment.db.

    Returns:
        Set of signature_id strings that have FreeDesc translations.
    """
    try:
        rows = conn.execute(
            "SELECT signature_id FROM fjms_translations "
            "WHERE field_name = 'FreeDesc' AND signature_id IS NOT NULL"
        ).fetchall()
        return {str(r[0]) for r in rows}
    except sqlite3.OperationalError:
        # Table may not exist yet
        return set()


# =============================================================================
# Bibliography Candidates
# =============================================================================


def get_bibliography_candidates(
    conn: sqlite3.Connection, min_length: int
) -> list[tuple[str, str, str]]:
    """Get bibliography candidates for translation.

    Bibliography table columns: RunningTitle, ArticleName, ArticleAuthorEng,
    ArticleAuthorHeb, CatalogAcronym. We translate RunningTitle (361K non-empty)
    which is the primary descriptive text.

    Args:
        conn: SQLite connection to fjms_enrichment.db.
        min_length: Minimum text length to include.

    Returns:
        List of (BibliographyId as str, AlmaId, RunningTitle) tuples.
    """
    rows = conn.execute(
        "SELECT rowid, AlmaId, RunningTitle FROM bibliography "
        "WHERE RunningTitle IS NOT NULL AND length(RunningTitle) >= ?",
        (min_length,),
    ).fetchall()
    return [(str(r[0]), r[1], r[2]) for r in rows]


def get_already_translated_bib(conn: sqlite3.Connection) -> set:
    """Get set of rowids already translated for bibliography.

    Args:
        conn: SQLite connection to fjms_enrichment.db.

    Returns:
        Set of rowid strings that have Bibliography translations.
    """
    try:
        rows = conn.execute(
            "SELECT alma_id FROM fjms_translations "
            "WHERE field_name = 'BibRunningTitle'"
        ).fetchall()
        return {str(r[0]) for r in rows}
    except sqlite3.OperationalError:
        return set()


# =============================================================================
# Write Translation
# =============================================================================


def write_translation(
    conn: sqlite3.Connection,
    alma_id: str,
    field_name: str,
    signature_id: str | None,
    original_text: str,
    translated_text: str,
    direction: str,
) -> None:
    """Insert a single translation into fjms_translations.

    Args:
        conn: Writable SQLite connection.
        alma_id: AlmaId from the source table.
        field_name: Field name for the translation.
        signature_id: Optional signature ID (for free descriptions).
        original_text: Source text that was translated.
        translated_text: API translation result.
        direction: Translation direction.
    """
    now = datetime.now(timezone.utc).isoformat()
    sig_id = int(signature_id) if signature_id is not None else None
    conn.execute(
        "INSERT INTO fjms_translations "
        "(alma_id, field_name, signature_id, original_text, translated_text, "
        "direction, translated_at, model_version) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (alma_id, field_name, sig_id, original_text, translated_text, direction, now, "dictalm2.0"),
    )


# =============================================================================
# Main Batch Flow
# =============================================================================


def format_eta(seconds: float) -> str:
    """Format seconds as human-readable duration.

    Args:
        seconds: Number of seconds.

    Returns:
        Formatted string like "2h 15m" or "45m 30s".
    """
    if seconds < 60:
        return f"{seconds:.0f}s"
    minutes = seconds / 60
    if minutes < 60:
        return f"{minutes:.0f}m"
    hours = minutes / 60
    remaining_min = minutes % 60
    return f"{hours:.0f}h {remaining_min:.0f}m"


def run_freedesc(args: argparse.Namespace) -> None:
    """Execute free description translation batch.

    Args:
        args: Parsed CLI arguments.
    """
    try:
        from tqdm import tqdm
    except ImportError:
        tqdm = None

    checkpoint_path = args.checkpoint_file or DEFAULT_FREEDESC_CHECKPOINT

    print(f"Loading free description candidates from {args.fjms_db}")
    print(f"  (min_length={args.min_length})...")

    conn = sqlite3.connect(args.fjms_db)
    candidates = get_freedesc_candidates(conn, args.min_length)
    total_candidates = len(candidates)
    print(f"Found {total_candidates:,} free description candidates.")

    if args.dry_run:
        # Length distribution
        lengths = [len(c[2]) for c in candidates]
        if lengths:
            avg_len = sum(lengths) / len(lengths)
            print(f"\nLength distribution:")
            print(f"  Min: {min(lengths):,} chars")
            print(f"  Max: {max(lengths):,} chars")
            print(f"  Avg: {avg_len:,.0f} chars")
            short = sum(1 for l in lengths if l < 50)
            medium = sum(1 for l in lengths if 50 <= l < 200)
            long = sum(1 for l in lengths if l >= 200)
            print(f"  <50 chars: {short:,}")
            print(f"  50-199 chars: {medium:,}")
            print(f"  200+ chars: {long:,}")
        # Estimate time
        est_minutes = total_candidates * 0.26 / 60  # ~0.26s per item at 5 workers
        est_hours = est_minutes / 60
        print(f"\nEstimated time at 5 workers: ~{est_hours:.0f} hours ({est_minutes:.0f} min)")
        print("Dry run complete. No translations performed.")
        conn.close()
        return

    # Load checkpoint
    completed_ids = load_checkpoint(checkpoint_path)
    already_translated = get_already_translated_freedesc(conn)
    # Merge: both checkpoint and DB-based skip
    skip_ids = completed_ids | already_translated
    if skip_ids:
        print(f"Skipping {len(skip_ids):,} already-translated items.")

    # Filter pending
    pending = [
        (sig_id, alma_id, text)
        for sig_id, alma_id, text in candidates
        if sig_id not in skip_ids
    ]
    if args.limit:
        pending = pending[: args.limit]

    total_pending = len(pending)
    print(f"Pending translations: {total_pending:,}")

    if total_pending == 0:
        print("Nothing to translate. All candidates already completed.")
        conn.close()
        return

    # Load few-shot template
    print(f"Loading HE->EN few-shot template...")
    template = load_few_shot_template(DEFAULT_FEW_SHOT_HE2EN)
    few_shot_prompt = build_few_shot_prompt(template, direction="he2en")

    # Ensure target table exists
    ensure_fjms_translations_table(conn)

    # SIGINT handler
    interrupted = [False]

    def sigint_handler(signum, frame):
        interrupted[0] = True
        print("\n\nSIGINT received. Finishing current batch and saving checkpoint...")

    original_handler = signal.signal(signal.SIGINT, sigint_handler)

    # Statistics
    translated_count = 0
    failed_count = 0
    batch_count = 0
    start_time = time.time()
    items_processed = 0

    pbar = tqdm(total=total_pending, desc="Translating", unit="desc") if tqdm else None

    workers = min(args.workers, MAX_WORKERS)
    try:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {}
            for sig_id, alma_id, text in pending:
                f = pool.submit(translate_with_retry, text, few_shot_prompt, "he2en")
                futures[f] = (sig_id, alma_id, text)

            for f in as_completed(futures):
                if interrupted[0]:
                    # Cancel remaining futures
                    for remaining_f in futures:
                        remaining_f.cancel()
                    break

                sig_id, alma_id, original_text = futures[f]
                items_processed += 1

                try:
                    result = f.result()
                    if result is not None:
                        write_translation(
                            conn, alma_id, "FreeDesc", sig_id,
                            original_text, result, "he2en",
                        )
                        completed_ids.add(sig_id)
                        translated_count += 1
                        batch_count += 1
                    else:
                        failed_count += 1
                except Exception as e:
                    failed_count += 1
                    logger.error("Error processing sig_id=%s: %s", sig_id, e)

                if pbar:
                    pbar.update(1)

                # Periodic logging every LOG_INTERVAL items
                if items_processed % LOG_INTERVAL == 0:
                    elapsed = time.time() - start_time
                    rate = items_processed / (elapsed / 60)  # items/min
                    remaining = total_pending - items_processed
                    eta = (remaining / rate) * 60 if rate > 0 else 0
                    logger.warning(
                        "Progress: %d/%d (%.1f%%) | Rate: %.0f items/min | "
                        "ETA: %s | Translated: %d | Failed: %d",
                        items_processed, total_pending,
                        100 * items_processed / total_pending,
                        rate, format_eta(eta),
                        translated_count, failed_count,
                    )

                # Checkpoint at batch interval
                if batch_count >= args.batch_size:
                    conn.commit()
                    save_checkpoint(checkpoint_path, completed_ids, {
                        "translated": translated_count,
                        "failed": failed_count,
                        "items_processed": items_processed,
                        "elapsed_seconds": time.time() - start_time,
                    })
                    batch_count = 0

                # Refresh SQLite connection periodically
                if items_processed % RECONNECT_INTERVAL == 0:
                    conn.commit()
                    conn.close()
                    conn = sqlite3.connect(args.fjms_db)
                    ensure_fjms_translations_table(conn)
                    logger.info("SQLite connection refreshed at item %d", items_processed)

    finally:
        if pbar:
            pbar.close()
        signal.signal(signal.SIGINT, original_handler)

    # Final flush
    conn.commit()
    save_checkpoint(checkpoint_path, completed_ids, {
        "translated": translated_count,
        "failed": failed_count,
        "items_processed": items_processed,
        "elapsed_seconds": time.time() - start_time,
    })
    conn.close()

    # Summary
    elapsed = time.time() - start_time
    minutes = elapsed / 60
    print(f"\n{'=' * 60}")
    print("FJMS Free Description Translation Summary")
    print(f"{'=' * 60}")
    print(f"  Total candidates:     {total_candidates:,}")
    print(f"  Skipped (already):    {len(skip_ids):,}")
    print(f"  Pending this run:     {total_pending:,}")
    print(f"  Translated now:       {translated_count:,}")
    print(f"  Failed:               {failed_count:,}")
    print(f"  Elapsed:              {format_eta(elapsed)} ({elapsed:.0f}s)")
    if translated_count > 0:
        rate_per_sec = elapsed / translated_count
        rate_per_min = translated_count / (elapsed / 60) if elapsed > 0 else 0
        print(f"  Rate:                 {rate_per_sec:.2f}s per item ({rate_per_min:.0f} items/min)")
        remaining = total_candidates - len(completed_ids)
        if remaining > 0:
            eta = remaining * rate_per_sec
            print(f"  Remaining:            {remaining:,} items (~{format_eta(eta)})")
    if interrupted[0]:
        print(f"  Status:               INTERRUPTED (checkpoint saved)")
    print(f"  Checkpoint:           {checkpoint_path}")
    print(f"{'=' * 60}")


def run_bibliography(args: argparse.Namespace) -> None:
    """Execute bibliography translation batch (deferred).

    Bibliography translation is ~542K entries and takes ~40 hours.
    This mode is scaffolded but requires --force to run without --limit.

    Args:
        args: Parsed CLI arguments.
    """
    try:
        from tqdm import tqdm
    except ImportError:
        tqdm = None

    checkpoint_path = args.checkpoint_file or DEFAULT_BIB_CHECKPOINT

    print("=" * 60)
    print("WARNING: Bibliography translation is ~542K entries (~40 hours).")
    print("This is deferred to a future pass. Consider using --dry-run first.")
    print("=" * 60)

    conn = sqlite3.connect(args.fjms_db)
    candidates = get_bibliography_candidates(conn, args.min_length)
    total_candidates = len(candidates)
    print(f"\nFound {total_candidates:,} bibliography candidates (RunningTitle).")

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
        print("Dry run complete. No translations performed.")
        conn.close()
        return

    # Require --force or --limit for non-dry-run bibliography
    if not args.limit and not args.force:
        print("\nBibliography translation requires --force flag to run without --limit.")
        print("Use --limit N to test with a smaller subset first.")
        conn.close()
        return

    # Load checkpoint
    completed_ids = load_checkpoint(checkpoint_path)
    already_translated = get_already_translated_bib(conn)
    skip_ids = completed_ids | already_translated
    if skip_ids:
        print(f"Skipping {len(skip_ids):,} already-translated items.")

    pending = [
        (bib_id, alma_id, text)
        for bib_id, alma_id, text in candidates
        if bib_id not in skip_ids
    ]
    if args.limit:
        pending = pending[: args.limit]

    total_pending = len(pending)
    print(f"Pending translations: {total_pending:,}")

    if total_pending == 0:
        print("Nothing to translate.")
        conn.close()
        return

    # Load few-shot template
    template = load_few_shot_template(DEFAULT_FEW_SHOT_HE2EN)
    few_shot_prompt = build_few_shot_prompt(template, direction="he2en")
    ensure_fjms_translations_table(conn)

    # SIGINT handler
    interrupted = [False]

    def sigint_handler(signum, frame):
        interrupted[0] = True
        print("\n\nSIGINT received. Saving checkpoint...")

    original_handler = signal.signal(signal.SIGINT, sigint_handler)

    translated_count = 0
    failed_count = 0
    batch_count = 0
    start_time = time.time()
    items_processed = 0

    pbar = tqdm(total=total_pending, desc="Bib translate", unit="entry") if tqdm else None

    bib_workers = min(args.workers, MAX_WORKERS)
    try:
        with ThreadPoolExecutor(max_workers=bib_workers) as pool:
            futures = {}
            for bib_id, alma_id, text in pending:
                f = pool.submit(translate_with_retry, text, few_shot_prompt, "he2en")
                futures[f] = (bib_id, alma_id, text)

            for f in as_completed(futures):
                if interrupted[0]:
                    for remaining_f in futures:
                        remaining_f.cancel()
                    break

                bib_id, alma_id, original_text = futures[f]
                items_processed += 1

                try:
                    result = f.result()
                    if result is not None:
                        write_translation(
                            conn, bib_id, "BibRunningTitle", None,
                            original_text, result, "he2en",
                        )
                        completed_ids.add(bib_id)
                        translated_count += 1
                        batch_count += 1
                    else:
                        failed_count += 1
                except Exception as e:
                    failed_count += 1
                    logger.error("Error processing bib_id=%s: %s", bib_id, e)

                if pbar:
                    pbar.update(1)

                if items_processed % LOG_INTERVAL == 0:
                    elapsed = time.time() - start_time
                    rate = items_processed / (elapsed / 60) if elapsed > 0 else 0
                    remaining = total_pending - items_processed
                    eta = (remaining / rate) * 60 if rate > 0 else 0
                    logger.warning(
                        "Bib progress: %d/%d (%.1f%%) | %.0f items/min | ETA: %s",
                        items_processed, total_pending,
                        100 * items_processed / total_pending,
                        rate, format_eta(eta),
                    )

                if batch_count >= args.batch_size:
                    conn.commit()
                    save_checkpoint(checkpoint_path, completed_ids)
                    batch_count = 0

                if items_processed % RECONNECT_INTERVAL == 0:
                    conn.commit()
                    conn.close()
                    conn = sqlite3.connect(args.fjms_db)
                    ensure_fjms_translations_table(conn)

    finally:
        if pbar:
            pbar.close()
        signal.signal(signal.SIGINT, original_handler)

    conn.commit()
    save_checkpoint(checkpoint_path, completed_ids)
    conn.close()

    elapsed = time.time() - start_time
    print(f"\n{'=' * 60}")
    print("Bibliography Translation Summary")
    print(f"{'=' * 60}")
    print(f"  Total candidates:     {total_candidates:,}")
    print(f"  Translated now:       {translated_count:,}")
    print(f"  Failed:               {failed_count:,}")
    print(f"  Elapsed:              {format_eta(elapsed)}")
    if interrupted[0]:
        print(f"  Status:               INTERRUPTED (checkpoint saved)")
    print(f"  Checkpoint:           {checkpoint_path}")
    print(f"{'=' * 60}")


def run_batch(args: argparse.Namespace) -> None:
    """Route to appropriate batch function based on mode.

    Args:
        args: Parsed CLI arguments.
    """
    if args.mode == "bibliography":
        run_bibliography(args)
    else:
        run_freedesc(args)


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
        description=(
            "Batch translate FJMS free descriptions and bibliography via Dicta API."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""Examples:
  python scripts/translate_fjms_free_desc.py --dry-run              # Count candidates
  python scripts/translate_fjms_free_desc.py --limit 100            # Test with 100
  python scripts/translate_fjms_free_desc.py --workers 10           # Full run, faster
  python scripts/translate_fjms_free_desc.py --mode bibliography --dry-run  # Count bib entries
""",
    )
    parser.add_argument(
        "--fjms-db",
        default=DEFAULT_FJMS_DB,
        help=f"Path to fjms_enrichment.db (default: {DEFAULT_FJMS_DB})",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=MAX_WORKERS,
        help=f"Concurrent API workers (default: {MAX_WORKERS}, max: {MAX_WORKERS})",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=200,
        help="Checkpoint every N translations (default: 200)",
    )
    parser.add_argument(
        "--checkpoint-file",
        default=None,
        help="Checkpoint JSON path (default: auto-selected by mode)",
    )
    parser.add_argument(
        "--min-length",
        type=int,
        default=20,
        help="Minimum description length (default: 20 chars)",
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
        "--mode",
        default="freedesc",
        choices=["freedesc", "bibliography"],
        help="Translation mode: freedesc (default) or bibliography (deferred)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force bibliography mode without --limit (required for full ~40h run)",
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

    level = logging.DEBUG if args.verbose else logging.WARNING
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    run_batch(args)


if __name__ == "__main__":
    main()
