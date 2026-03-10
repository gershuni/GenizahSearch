#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Batch translate English library titles to Hebrew using the Dicta LM 2.0 API.

Targets ~21K unique English titles from libraries_translations.db where:
- source = 'extracted' (original bilingual data from libraries.csv)
- Hebrew title is short/generic (<15 chars) while English has real content
- English title is significantly longer (>10 chars more than Hebrew)

These are cases like:
  HE: "מכתבים" → EN: "Letter to David ha-Kohen he-haver"
  HE: "פיוט"   → EN: "Recto: liturgical text. Verso: Arabic letter"

The Hebrew title is a category label; the English is the scholarly description.
Translating EN→HE gives Hebrew users the full information.

Results stored in `english_title_he` column of title_translations table.

Usage:
  python scripts/translate_library_titles_en2he.py --dry-run
  python scripts/translate_library_titles_en2he.py --limit 100
  python scripts/translate_library_titles_en2he.py --workers 5
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

# Ensure project root is on path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from shared.dicta_client import (
    GOD_MODE,
    MAX_WORKERS,
    build_few_shot_prompt,
    load_few_shot_template,
    translate_text,
)

logger = logging.getLogger(__name__)

DEFAULT_DB = str(PROJECT_ROOT / "libraries_translations.db")
DEFAULT_CHECKPOINT = str(PROJECT_ROOT / "translate_lib_titles_en2he_checkpoint.json")
DEFAULT_FEW_SHOT = str(PROJECT_ROOT / "data" / "few_shot_en2he_scholarly.json")

MAX_RETRIES = 3
RETRY_BASE_DELAY = 1.0
RETRY_MAX_DELAY = 30.0
LOG_INTERVAL = 500
RECONNECT_INTERVAL = 5000

# Thresholds for candidate selection
MAX_HE_LEN = 15  # Hebrew title must be shorter than this
MIN_EN_EXTRA = 10  # English must be at least this many chars longer

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def format_eta(seconds: float) -> str:
    if seconds < 60:
        return f"{seconds:.0f}s"
    minutes = seconds / 60
    if minutes < 60:
        return f"{minutes:.0f}m"
    hours = minutes / 60
    remaining_min = minutes % 60
    return f"{hours:.0f}h {remaining_min:.0f}m"


# ---------------------------------------------------------------------------
# Checkpoint Logic
# ---------------------------------------------------------------------------

def load_checkpoint(path: str) -> set:
    if not os.path.isfile(path):
        return set()
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return set(data.get("completed_titles", []))
    except (json.JSONDecodeError, IOError) as e:
        logger.warning("Failed to load checkpoint from %s: %s", path, e)
        return set()


def save_checkpoint(path: str, completed: set, stats: dict | None = None) -> None:
    data = {
        "completed_titles": sorted(completed),
        "count": len(completed),
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


# ---------------------------------------------------------------------------
# Translation with retry
# ---------------------------------------------------------------------------

def translate_with_retry(text: str, few_shot_prompt: str) -> str | None:
    for attempt in range(MAX_RETRIES):
        result = translate_text(text, few_shot_prompt, "en2he")
        if result is not None:
            return result
        if attempt < MAX_RETRIES - 1:
            delay = min(RETRY_BASE_DELAY * (2 ** attempt), RETRY_MAX_DELAY)
            logger.info("Retry %d/%d after %.1fs", attempt + 2, MAX_RETRIES, delay)
            time.sleep(delay)
    return None


# ---------------------------------------------------------------------------
# DB Operations
# ---------------------------------------------------------------------------

def ensure_column(conn: sqlite3.Connection) -> None:
    """Add english_title_he column if it doesn't exist."""
    cols = {r[1] for r in conn.execute("PRAGMA table_info(title_translations)").fetchall()}
    if "english_title_he" not in cols:
        conn.execute("ALTER TABLE title_translations ADD COLUMN english_title_he TEXT")
        conn.commit()
        logger.info("Added english_title_he column")


def get_candidates(conn: sqlite3.Connection) -> list[tuple[str, str]]:
    """Get unique (english_title, system_number) pairs needing translation.

    Returns list of (english_title, sample_system_number) tuples.
    Only includes extracted-source records where HE is short and EN has more content.
    """
    rows = conn.execute("""
        SELECT english_title, MIN(system_number) as sample_sn
        FROM title_translations
        WHERE source = 'extracted'
          AND hebrew_title IS NOT NULL AND hebrew_title != ''
          AND english_title IS NOT NULL AND english_title != ''
          AND hebrew_title != english_title
          AND length(hebrew_title) < ?
          AND length(english_title) > length(hebrew_title) + ?
          AND (english_title_he IS NULL OR english_title_he = '')
        GROUP BY english_title
        ORDER BY COUNT(*) DESC
    """, (MAX_HE_LEN, MIN_EN_EXTRA)).fetchall()
    return [(r[0], r[1]) for r in rows]


def get_already_translated(conn: sqlite3.Connection) -> set:
    """Get set of English titles already translated."""
    try:
        rows = conn.execute("""
            SELECT DISTINCT english_title FROM title_translations
            WHERE english_title_he IS NOT NULL AND english_title_he != ''
        """).fetchall()
        return {r[0] for r in rows}
    except sqlite3.OperationalError:
        return set()


def save_translation(conn: sqlite3.Connection, english_title: str, hebrew: str) -> int:
    """Update all rows matching this english_title with the translation."""
    cursor = conn.execute("""
        UPDATE title_translations
        SET english_title_he = ?
        WHERE english_title = ?
          AND source = 'extracted'
          AND length(hebrew_title) < ?
          AND length(english_title) > length(hebrew_title) + ?
    """, (hebrew, english_title, MAX_HE_LEN, MIN_EN_EXTRA))
    conn.commit()
    return cursor.rowcount


# ---------------------------------------------------------------------------
# Main batch loop
# ---------------------------------------------------------------------------

def run_batch(
    db_path: str,
    checkpoint_path: str,
    few_shot_path: str,
    workers: int,
    limit: int | None,
    dry_run: bool,
) -> None:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    ensure_column(conn)

    # Load few-shot template
    logger.info("Loading EN->HE few-shot template from %s", few_shot_path)
    template = load_few_shot_template(few_shot_path)
    few_shot_prompt = build_few_shot_prompt(template, "placeholder")
    # We'll rebuild per-item, but this validates the template
    logger.info("Few-shot template loaded (%d examples)", len(template.get("prompts", [])))

    # Get candidates
    candidates = get_candidates(conn)
    total_candidates = len(candidates)
    logger.info("Found %d unique English titles needing translation.", total_candidates)

    # Load checkpoint (completed titles from previous runs)
    completed = load_checkpoint(checkpoint_path)
    already_db = get_already_translated(conn)
    skip = completed | already_db

    pending = [(en, sn) for en, sn in candidates if en not in skip]
    logger.info(
        "Skipping %d already-translated items. Pending translations: %d",
        len(skip), len(pending),
    )

    if limit:
        pending = pending[:limit]
        logger.info("Limited to %d items", limit)

    if dry_run:
        logger.info("DRY RUN — showing first 10 candidates:")
        for en, sn in pending[:10]:
            logger.info("  [%s] %s", sn, en[:80])
        logger.info("Would translate %d titles. Exiting.", len(pending))
        conn.close()
        return

    if not pending:
        logger.info("Nothing to translate. Done.")
        conn.close()
        return

    # SIGINT handler
    interrupted = False

    def _sigint_handler(sig, frame):
        nonlocal interrupted
        if interrupted:
            logger.warning("Double SIGINT — force exit")
            sys.exit(1)
        interrupted = True
        logger.warning("SIGINT received — saving checkpoint after current batch...")

    signal.signal(signal.SIGINT, _sigint_handler)

    start_time = time.time()
    ok_count = 0
    fail_count = 0
    rows_updated = 0
    batch_size = 50  # Save checkpoint every N

    from concurrent.futures import ThreadPoolExecutor, as_completed

    def _translate_one(en_title: str) -> tuple[str, str | None]:
        prompt = build_few_shot_prompt(template, en_title)
        result = translate_with_retry(en_title, prompt)
        return (en_title, result)

    effective_workers = min(workers, MAX_WORKERS)
    logger.info(
        "Starting translation: %d titles, %d workers, GOD_MODE=%s",
        len(pending), effective_workers, GOD_MODE,
    )

    batch_queue: list[tuple[str, str]] = []  # (en_title, he_translation)

    with ThreadPoolExecutor(max_workers=effective_workers) as pool:
        futures = {}
        for en_title, _sn in pending:
            if interrupted:
                break
            futures[pool.submit(_translate_one, en_title)] = en_title

        for future in as_completed(futures):
            if interrupted:
                break

            en_title, he_result = future.result()
            total_done = ok_count + fail_count

            if he_result:
                ok_count += 1
                batch_queue.append((en_title, he_result))
                completed.add(en_title)
            else:
                fail_count += 1
                logger.warning("Failed: %s", en_title[:60])

            # Flush batch to DB periodically
            if len(batch_queue) >= batch_size:
                for _en, _he in batch_queue:
                    rows_updated += save_translation(conn, _en, _he)
                batch_queue.clear()

                # Reconnect periodically
                if total_done > 0 and total_done % RECONNECT_INTERVAL == 0:
                    conn.close()
                    conn = sqlite3.connect(db_path)
                    conn.row_factory = sqlite3.Row
                    logger.info("SQLite reconnected at %d items", total_done)

            # Progress logging
            if (total_done + 1) % LOG_INTERVAL == 0:
                elapsed = time.time() - start_time
                rate = (total_done + 1) / (elapsed / 60) if elapsed > 0 else 0
                remaining = len(pending) - total_done - 1
                eta = remaining / rate * 60 if rate > 0 else 0
                logger.warning(
                    "Progress: %d/%d (%.1f%%) | %.0f items/min | ETA: %s | OK: %d | Fail: %d",
                    total_done + 1, len(pending),
                    (total_done + 1) / len(pending) * 100,
                    rate, format_eta(eta), ok_count, fail_count,
                )

            # Checkpoint
            if (total_done + 1) % (batch_size * 10) == 0:
                save_checkpoint(checkpoint_path, completed, {
                    "ok": ok_count, "fail": fail_count,
                    "rows_updated": rows_updated,
                })

    # Final flush
    for _en, _he in batch_queue:
        rows_updated += save_translation(conn, _en, _he)
    batch_queue.clear()

    # Final checkpoint
    save_checkpoint(checkpoint_path, completed, {
        "ok": ok_count, "fail": fail_count,
        "rows_updated": rows_updated,
    })

    elapsed = time.time() - start_time
    logger.warning(
        "DONE: %d translated, %d failed, %d rows updated in %.1f min",
        ok_count, fail_count, rows_updated, elapsed / 60,
    )
    conn.close()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Translate English library titles to Hebrew via Dicta API"
    )
    parser.add_argument("--db", default=DEFAULT_DB, help="Path to libraries_translations.db")
    parser.add_argument("--checkpoint", default=DEFAULT_CHECKPOINT, help="Checkpoint file path")
    parser.add_argument("--few-shot", default=DEFAULT_FEW_SHOT, help="Few-shot template JSON")
    parser.add_argument("--workers", type=int, default=5, help="Parallel workers (max 5)")
    parser.add_argument("--limit", type=int, default=None, help="Limit translations")
    parser.add_argument("--dry-run", action="store_true", help="Show candidates without translating")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    logger.setLevel(logging.INFO)

    run_batch(
        db_path=args.db,
        checkpoint_path=args.checkpoint,
        few_shot_path=args.few_shot,
        workers=args.workers,
        limit=args.limit,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    main()
