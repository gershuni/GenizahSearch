#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Batch translate FJMS catalog running titles, full texts, and textual frames
using the Dicta LM 2.0 Translation API.

Running titles:   ~107K English entries EN→HE (~8 hours at 5 workers)
Full texts:       ~46K English entries  EN→HE (~3 hours at 5 workers)
Textual frames:   ~127K Hebrew entries  HE→EN (~10 hours at 5 workers)

Designed for very long-running batch operations with robust error handling:
- Exponential backoff on API errors (1s, 2s, 4s, max 30s, 3 retries)
- Checkpoint every batch_size items (atomic JSON write)
- SIGINT handler saves checkpoint before exit
- Progress logging every 1,000 translations with rate and ETA
- SQLite connection refresh every 10,000 items

Usage:
  python scripts/translate_fjms_catalog_text.py --mode runningtitle --dry-run
  python scripts/translate_fjms_catalog_text.py --mode runningtitle --limit 100
  python scripts/translate_fjms_catalog_text.py --mode runningtitle --workers 10
  python scripts/translate_fjms_catalog_text.py --mode fulltext --dry-run
  python scripts/translate_fjms_catalog_text.py --mode fulltext --workers 10
  python scripts/translate_fjms_catalog_text.py --mode textualframe --dry-run
  python scripts/translate_fjms_catalog_text.py --mode textualframe --workers 5
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
    GOD_MODE,
    MAX_WORKERS,
    build_few_shot_prompt,
    load_few_shot_template,
    translate_text,
)
from shared.translation_service import ensure_fjms_translations_table

logger = logging.getLogger(__name__)

# Default paths relative to project root
DEFAULT_FJMS_DB = str(PROJECT_ROOT / "fist_data" / "fjms_enrichment.db")
DEFAULT_RT_CHECKPOINT = str(PROJECT_ROOT / "translate_fjms_runningtitle_checkpoint.json")
DEFAULT_FT_CHECKPOINT = str(PROJECT_ROOT / "translate_fjms_fulltext_checkpoint.json")
DEFAULT_FEW_SHOT_EN2HE = str(PROJECT_ROOT / "data" / "few_shot_en2he_scholarly.json")
DEFAULT_FEW_SHOT_HE2EN = str(PROJECT_ROOT / "data" / "few_shot_he2en_scholarly.json")
DEFAULT_TF_CHECKPOINT = str(PROJECT_ROOT / "translate_fjms_textualframe_checkpoint.json")

MAX_RETRIES = 3
RETRY_BASE_DELAY = 1.0
RETRY_MAX_DELAY = 30.0
LOG_INTERVAL = 1000
RECONNECT_INTERVAL = 10000


# =============================================================================
# Helpers
# =============================================================================


def has_english(text: str, min_latin: int = 10) -> bool:
    """Return True if text contains significant English content worth translating.

    Scholarly catalog descriptions often mix English framing with Hebrew/Arabic
    titles, incipits, and names in Hebrew script. Texts with >=min_latin Latin
    letters contain English scholarly content that should be translated.
    Pure Hebrew texts (below threshold) are left as-is.

    Args:
        text: The text to check.
        min_latin: Minimum Latin letter count. Use 10 for long-form descriptions
            (FreeDesc, FullText) where a few Latin chars may be shelfmark codes.
            Use 3 for short RunningTitles where even "Num." or "Lev." is English.
    """
    latin = sum(1 for c in text if ("A" <= c <= "Z") or ("a" <= c <= "z"))
    return latin >= min_latin


def has_hebrew(text: str, min_hebrew: int = 3) -> bool:
    """Return True if text contains significant Hebrew content worth translating.

    Used for HE→EN translation: identifies Hebrew-language textual frames that
    need English translations. Ignores texts that are already in English.
    """
    hebrew = sum(1 for c in text if "\u0590" <= c <= "\u05FF" or "\uFB1D" <= c <= "\uFB4F")
    return hebrew >= min_hebrew


def format_eta(seconds: float) -> str:
    if seconds < 60:
        return f"{seconds:.0f}s"
    minutes = seconds / 60
    if minutes < 60:
        return f"{minutes:.0f}m"
    hours = minutes / 60
    remaining_min = minutes % 60
    return f"{hours:.0f}h {remaining_min:.0f}m"


# =============================================================================
# Checkpoint Logic
# =============================================================================


def load_checkpoint(path: str) -> set:
    if not os.path.isfile(path):
        return set()
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return set(data.get("completed_ids", []))
    except (json.JSONDecodeError, IOError) as e:
        logger.warning("Failed to load checkpoint from %s: %s", path, e)
        return set()


def save_checkpoint(path: str, completed_ids: set, stats: dict | None = None) -> None:
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
# Translation with Retry
# =============================================================================


def translate_with_retry(
    text: str, few_shot_prompt: str, direction: str = "en2he"
) -> str | None:
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
# Running Title Candidates
# =============================================================================


def get_runningtitle_candidates(
    conn: sqlite3.Connection, min_length: int
) -> list[tuple[str, str, str]]:
    """Get English-only running title candidates for EN→HE translation.

    Returns:
        List of (UnitCatalogRecId as str, AlmaId, RunningTitle) tuples.
    """
    rows = conn.execute(
        "SELECT UnitCatalogRecId, AlmaId, RunningTitle FROM catalog_running_titles "
        "WHERE RunningTitle IS NOT NULL AND length(RunningTitle) >= ?",
        (min_length,),
    ).fetchall()
    return [
        (str(r[0]), r[1], r[2])
        for r in rows
        if has_english(r[2], min_latin=3)  # short titles: even "Num." is English
    ]


def get_already_translated_rt(conn: sqlite3.Connection) -> set:
    try:
        rows = conn.execute(
            "SELECT alma_id || ':' || COALESCE(signature_id, '') FROM fjms_translations "
            "WHERE field_name = 'RunningTitle'"
        ).fetchall()
        return {r[0] for r in rows}
    except sqlite3.OperationalError:
        return set()


# =============================================================================
# Full Text Candidates
# =============================================================================


def get_fulltext_candidates(
    conn: sqlite3.Connection, min_length: int
) -> list[tuple[str, str, str]]:
    """Get English-only full text candidates for EN→HE translation.

    Returns:
        List of (rowid as str, AlmaId, FullText) tuples.
    """
    rows = conn.execute(
        "SELECT rowid, AlmaId, FullText FROM catalog_full_texts "
        "WHERE FullText IS NOT NULL AND length(FullText) >= ?",
        (min_length,),
    ).fetchall()
    return [
        (str(r[0]), r[1], r[2])
        for r in rows
        if has_english(r[2])  # long descriptions: 10+ Latin letters
    ]


def get_already_translated_ft(conn: sqlite3.Connection) -> set:
    try:
        rows = conn.execute(
            "SELECT COALESCE(signature_id, '') FROM fjms_translations "
            "WHERE field_name = 'FullText'"
        ).fetchall()
        return {str(r[0]) for r in rows}
    except sqlite3.OperationalError:
        return set()


# =============================================================================
# Textual Frame Candidates (HE→EN)
# =============================================================================


def get_textualframe_candidates(
    conn: sqlite3.Connection, min_length: int
) -> list[tuple[str, str, str]]:
    """Get Hebrew-only textual frame candidates for HE→EN translation.

    Selects rows where TextualFrameHeb contains Hebrew and either:
    - TextualFrameEng is missing, or
    - TextualFrameEng is identical to TextualFrameHeb (no real English)

    Returns:
        List of (rowid as str, AlmaId, TextualFrameHeb) tuples.
    """
    rows = conn.execute(
        "SELECT rowid, AlmaId, TextualFrameHeb FROM catalog_textual_frames "
        "WHERE TextualFrameHeb IS NOT NULL AND length(TextualFrameHeb) >= ? "
        "AND (TextualFrameEng IS NULL OR TextualFrameEng = '' "
        "     OR TextualFrameEng = TextualFrameHeb)",
        (min_length,),
    ).fetchall()
    return [
        (str(r[0]), r[1], r[2])
        for r in rows
        if has_hebrew(r[2])
    ]


def get_already_translated_tf(conn: sqlite3.Connection) -> set:
    try:
        rows = conn.execute(
            "SELECT COALESCE(signature_id, '') FROM fjms_translations "
            "WHERE field_name = 'TextualFrame'"
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
# Batch Runner (shared logic)
# =============================================================================


def run_batch_translate(
    args: argparse.Namespace,
    mode_name: str,
    field_name: str,
    candidates: list[tuple[str, str, str]],
    already_translated: set,
    checkpoint_path: str,
    id_func,
    direction: str = "en2he",
) -> None:
    """Generic batch translation loop.

    Args:
        args: CLI args.
        mode_name: Display name for logging.
        field_name: Field name for fjms_translations table.
        candidates: List of (id_key, alma_id, text) tuples.
        already_translated: Set of already-translated ID keys.
        checkpoint_path: Path to checkpoint file.
        id_func: Function(id_key, alma_id) -> checkpoint key string.
        direction: Translation direction ("en2he" or "he2en").
    """
    try:
        from tqdm import tqdm
    except ImportError:
        tqdm = None

    total_candidates = len(candidates)

    if args.dry_run:
        lengths = [len(c[2]) for c in candidates]
        if lengths:
            avg_len = sum(lengths) / len(lengths)
            print(f"\nLength distribution:")
            print(f"  Min: {min(lengths):,} chars")
            print(f"  Max: {max(lengths):,} chars")
            print(f"  Avg: {avg_len:,.0f} chars")
            short = sum(1 for l in lengths if l < 50)
            medium = sum(1 for l in lengths if 50 <= l < 200)
            long_ = sum(1 for l in lengths if l >= 200)
            print(f"  <50 chars: {short:,}")
            print(f"  50-199 chars: {medium:,}")
            print(f"  200+ chars: {long_:,}")
        est_hours = total_candidates * 0.26 / 3600
        print(f"\nEstimated time at 5 workers: ~{est_hours:.0f} hours")
        print("Dry run complete. No translations performed.")
        return

    # Load checkpoint
    completed_ids = load_checkpoint(checkpoint_path)
    skip_ids = completed_ids | already_translated
    if skip_ids:
        print(f"Skipping {len(skip_ids):,} already-translated items.")

    # Filter pending
    pending = [
        (id_key, alma_id, text)
        for id_key, alma_id, text in candidates
        if id_func(id_key, alma_id) not in skip_ids
    ]
    if args.limit:
        pending = pending[: args.limit]

    total_pending = len(pending)
    print(f"Pending translations: {total_pending:,}")

    if total_pending == 0:
        print("Nothing to translate. All candidates already completed.")
        return

    # Load few-shot template
    if direction == "he2en":
        print("Loading HE->EN few-shot template...")
        template = load_few_shot_template(DEFAULT_FEW_SHOT_HE2EN)
    else:
        print("Loading EN->HE few-shot template...")
        template = load_few_shot_template(DEFAULT_FEW_SHOT_EN2HE)
    few_shot_prompt = build_few_shot_prompt(template, direction=direction)

    # Ensure target table exists
    conn = sqlite3.connect(args.fjms_db)
    ensure_fjms_translations_table(conn)

    # SIGINT handler
    interrupted = [False]

    def sigint_handler(signum, frame):
        interrupted[0] = True
        print("\n\nSIGINT received. Finishing current batch and saving checkpoint...")

    original_handler = signal.signal(signal.SIGINT, sigint_handler)

    translated_count = 0
    failed_count = 0
    batch_count = 0
    start_time = time.time()
    items_processed = 0

    pbar = tqdm(total=total_pending, desc=f"Translating {mode_name}", unit="item") if tqdm else None

    workers = min(args.workers, MAX_WORKERS)
    try:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {}
            for id_key, alma_id, text in pending:
                f = pool.submit(translate_with_retry, text, few_shot_prompt, direction)
                futures[f] = (id_key, alma_id, text)

            for f in as_completed(futures):
                if interrupted[0]:
                    for remaining_f in futures:
                        remaining_f.cancel()
                    break

                id_key, alma_id, original_text = futures[f]
                items_processed += 1

                try:
                    result = f.result()
                    if result is not None:
                        write_translation(
                            conn, alma_id, field_name, id_key,
                            original_text, result, direction,
                        )
                        completed_ids.add(id_func(id_key, alma_id))
                        translated_count += 1
                        batch_count += 1
                    else:
                        failed_count += 1
                except Exception as e:
                    failed_count += 1
                    logger.error("Error processing %s id=%s: %s", mode_name, id_key, e)

                if pbar:
                    pbar.update(1)

                if items_processed % LOG_INTERVAL == 0:
                    elapsed = time.time() - start_time
                    rate = items_processed / (elapsed / 60) if elapsed > 0 else 0
                    remaining = total_pending - items_processed
                    eta = (remaining / rate) * 60 if rate > 0 else 0
                    logger.warning(
                        "%s progress: %d/%d (%.1f%%) | %.0f items/min | ETA: %s | OK: %d | Fail: %d",
                        mode_name, items_processed, total_pending,
                        100 * items_processed / total_pending,
                        rate, format_eta(eta),
                        translated_count, failed_count,
                    )

                if batch_count >= args.batch_size:
                    conn.commit()
                    save_checkpoint(checkpoint_path, completed_ids, {
                        "translated": translated_count,
                        "failed": failed_count,
                        "items_processed": items_processed,
                        "elapsed_seconds": time.time() - start_time,
                    })
                    batch_count = 0

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

    elapsed = time.time() - start_time
    print(f"\n{'=' * 60}")
    print(f"FJMS {mode_name} Translation Summary")
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
    if interrupted[0]:
        print(f"  Status:               INTERRUPTED (checkpoint saved)")
    print(f"  Checkpoint:           {checkpoint_path}")
    print(f"{'=' * 60}")


# =============================================================================
# Mode Runners
# =============================================================================


def run_runningtitle(args: argparse.Namespace) -> None:
    checkpoint_path = args.checkpoint_file or DEFAULT_RT_CHECKPOINT

    print(f"Loading English running title candidates from {args.fjms_db}")
    print(f"  (min_length={args.min_length})...")

    conn = sqlite3.connect(args.fjms_db)
    candidates = get_runningtitle_candidates(conn, args.min_length)
    already = get_already_translated_rt(conn)
    conn.close()

    print(f"Found {len(candidates):,} English running title candidates.")

    run_batch_translate(
        args,
        mode_name="Running Title",
        field_name="RunningTitle",
        candidates=candidates,
        already_translated=already,
        checkpoint_path=checkpoint_path,
        id_func=lambda id_key, alma_id: f"{alma_id}:{id_key}",
    )


def run_fulltext(args: argparse.Namespace) -> None:
    checkpoint_path = args.checkpoint_file or DEFAULT_FT_CHECKPOINT

    print(f"Loading English full text candidates from {args.fjms_db}")
    print(f"  (min_length={args.min_length})...")

    conn = sqlite3.connect(args.fjms_db)
    candidates = get_fulltext_candidates(conn, args.min_length)
    already = get_already_translated_ft(conn)
    conn.close()

    print(f"Found {len(candidates):,} English full text candidates.")

    run_batch_translate(
        args,
        mode_name="Full Text",
        field_name="FullText",
        candidates=candidates,
        already_translated=already,
        checkpoint_path=checkpoint_path,
        id_func=lambda id_key, alma_id: id_key,
    )


def run_textualframe(args: argparse.Namespace) -> None:
    checkpoint_path = args.checkpoint_file or DEFAULT_TF_CHECKPOINT

    print(f"Loading Hebrew textual frame candidates from {args.fjms_db}")
    print(f"  (min_length={args.min_length})...")

    conn = sqlite3.connect(args.fjms_db)
    candidates = get_textualframe_candidates(conn, args.min_length)
    already = get_already_translated_tf(conn)
    conn.close()

    print(f"Found {len(candidates):,} Hebrew textual frame candidates.")

    run_batch_translate(
        args,
        mode_name="Textual Frame",
        field_name="TextualFrame",
        candidates=candidates,
        already_translated=already,
        checkpoint_path=checkpoint_path,
        id_func=lambda id_key, alma_id: id_key,
        direction="he2en",
    )


# =============================================================================
# CLI
# =============================================================================


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Batch translate FJMS catalog text fields via Dicta API.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""Examples:
  python scripts/translate_fjms_catalog_text.py --mode runningtitle --dry-run
  python scripts/translate_fjms_catalog_text.py --mode runningtitle --workers 10
  python scripts/translate_fjms_catalog_text.py --mode fulltext --dry-run
  python scripts/translate_fjms_catalog_text.py --mode fulltext --workers 10
  python scripts/translate_fjms_catalog_text.py --mode textualframe --dry-run
  python scripts/translate_fjms_catalog_text.py --mode textualframe --workers 5
  python scripts/translate_fjms_catalog_text.py --mode both --workers 10
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
        default=10,
        help="Minimum text length to translate (default: 10 chars)",
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
        default="runningtitle",
        choices=["runningtitle", "fulltext", "textualframe", "both"],
        help="Translation mode (default: runningtitle)",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose logging",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)

    level = logging.DEBUG if args.verbose else logging.WARNING
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    if args.mode == "both":
        print("=== Phase 1: Running Titles ===\n")
        run_runningtitle(args)
        print("\n=== Phase 2: Full Texts ===\n")
        run_fulltext(args)
    elif args.mode == "fulltext":
        run_fulltext(args)
    elif args.mode == "textualframe":
        run_textualframe(args)
    else:
        run_runningtitle(args)


if __name__ == "__main__":
    main()
