#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Batch translate FJMS catalog field gaps from Hebrew to English (and vice versa)
using the Dicta LM 2.0 Translation API.

Reads gap candidates from fjms_enrichment.db (catalog, genizah_titles,
genizah_persons tables), translates via Dicta API with scholarly few-shot
prompts, and writes results to the fjms_translations table in the same sidecar.

Gap-fill ONLY -- never overwrites existing human translations. Only translates
where the target field is NULL or empty.

Categories:
  - titles_he2en: TitleHeb exists but Title is missing (~1,156 items)
  - titles_en2he: Title exists but TitleHeb is missing (~1,720 items)
  - authors:      AuthorText (Hebrew) -> English (~204 distinct values)
  - genizah_titles: OrgTitle exists but EngTitle is missing (~626 items)
  - persons_he2en: HebDesc exists but EngDesc is missing (~1,163 items)
  - persons_en2he: EngDesc exists but HebDesc is missing (~703 items)

Usage:
  python scripts/translate_fjms_catalog.py --dry-run           # Count all gaps
  python scripts/translate_fjms_catalog.py --category titles    # Titles only
  python scripts/translate_fjms_catalog.py --limit 20           # Test with 20 items
"""

import argparse
import json
import logging
import os
import sqlite3
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

# Ensure project root is on path for imports
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from shared.dicta_client import (
    build_few_shot_prompt,
    load_few_shot_template,
    translate_text,
)
from shared.translation_service import ensure_fjms_translations_table

logger = logging.getLogger(__name__)

# Default paths relative to project root
DEFAULT_FJMS_DB = str(PROJECT_ROOT / "fist_data" / "fjms_enrichment.db")
DEFAULT_CHECKPOINT = str(PROJECT_ROOT / "translate_fjms_catalog_checkpoint.json")
DEFAULT_FEW_SHOT_HE2EN = str(PROJECT_ROOT / "data" / "few_shot_he2en_scholarly.json")
DEFAULT_FEW_SHOT_EN2HE = str(PROJECT_ROOT / "data" / "few_shot_en2he_scholarly.json")
REQUEST_DELAY = 3.0  # seconds between API calls to avoid 429


# =============================================================================
# Checkpoint Logic
# =============================================================================


def load_checkpoint(path: str) -> dict[str, set]:
    """Load checkpoint as {category: set_of_completed_ids}.

    Args:
        path: Path to checkpoint JSON file.

    Returns:
        Dict mapping category name to set of completed ID strings.
    """
    if not os.path.isfile(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        result = {}
        for cat, ids in data.get("completed", {}).items():
            result[cat] = set(ids)
        return result
    except (json.JSONDecodeError, IOError) as e:
        logger.warning("Failed to load checkpoint from %s: %s", path, e)
        return {}


def save_checkpoint(path: str, completed: dict[str, set]) -> None:
    """Atomically save checkpoint to JSON file.

    Args:
        path: Path to checkpoint JSON file.
        completed: Dict mapping category name to set of completed ID strings.
    """
    data = {
        "completed": {cat: sorted(ids) for cat, ids in completed.items()},
        "counts": {cat: len(ids) for cat, ids in completed.items()},
        "saved_at": datetime.now(timezone.utc).isoformat(),
    }
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
# Gap Detection Queries
# =============================================================================


def get_title_gaps_he2en(conn: sqlite3.Connection) -> list[tuple[str, str]]:
    """Get catalog rows where TitleHeb exists but Title is missing.

    Returns:
        List of (AlmaId, TitleHeb) tuples.
    """
    rows = conn.execute(
        "SELECT AlmaId, TitleHeb FROM catalog "
        "WHERE TitleHeb IS NOT NULL AND TitleHeb != '' "
        "AND (Title IS NULL OR Title = '')"
    ).fetchall()
    return [(r[0], r[1]) for r in rows]


def get_title_gaps_en2he(conn: sqlite3.Connection) -> list[tuple[str, str]]:
    """Get catalog rows where Title exists in English but TitleHeb is missing.

    Filters to titles that actually contain Latin characters (real English).
    Many Title values are already in Hebrew/JA script and don't need translation.

    Returns:
        List of (AlmaId, Title) tuples.
    """
    rows = conn.execute(
        "SELECT AlmaId, Title FROM catalog "
        "WHERE Title IS NOT NULL AND Title != '' "
        "AND (TitleHeb IS NULL OR TitleHeb = '')"
    ).fetchall()
    # Only include titles with actual Latin chars (real English)
    return [(r[0], r[1]) for r in rows if any("a" <= c.lower() <= "z" for c in r[1])]


def get_author_gaps(conn: sqlite3.Connection) -> list[tuple[str, str]]:
    """Get distinct AuthorText values that contain Hebrew characters.

    Returns:
        List of (AuthorText, AuthorText) tuples (id=text for dedup).
    """
    rows = conn.execute(
        "SELECT DISTINCT AuthorText FROM catalog "
        "WHERE AuthorText IS NOT NULL AND AuthorText != ''"
    ).fetchall()
    # Filter to Hebrew-only (contains at least one Hebrew character)
    hebrew_authors = []
    for (text,) in rows:
        if any("\u0590" <= ch <= "\u05FF" for ch in text):
            hebrew_authors.append((text, text))
    return hebrew_authors


def get_genizah_title_gaps(conn: sqlite3.Connection) -> list[tuple[str, str]]:
    """Get genizah_titles where OrgTitle exists but EngTitle is missing.

    Returns:
        List of (GenizahTitleId as str, OrgTitle) tuples.
    """
    rows = conn.execute(
        "SELECT GenizahTitleId, OrgTitle FROM genizah_titles "
        "WHERE OrgTitle IS NOT NULL AND OrgTitle != '' "
        "AND (EngTitle IS NULL OR EngTitle = '')"
    ).fetchall()
    return [(str(r[0]), r[1]) for r in rows]


def get_person_gaps_he2en(conn: sqlite3.Connection) -> list[tuple[str, str]]:
    """Get genizah_persons where HebDesc exists but EngDesc is missing.

    Returns:
        List of (GenizahPersonId as str, HebDesc) tuples.
    """
    rows = conn.execute(
        "SELECT GenizahPersonId, HebDesc FROM genizah_persons "
        "WHERE HebDesc IS NOT NULL AND HebDesc != '' "
        "AND (EngDesc IS NULL OR EngDesc = '')"
    ).fetchall()
    return [(str(r[0]), r[1]) for r in rows]


def get_person_gaps_en2he(conn: sqlite3.Connection) -> list[tuple[str, str]]:
    """Get genizah_persons where EngDesc exists but HebDesc is missing.

    Returns:
        List of (GenizahPersonId as str, EngDesc) tuples.
    """
    rows = conn.execute(
        "SELECT GenizahPersonId, EngDesc FROM genizah_persons "
        "WHERE EngDesc IS NOT NULL AND EngDesc != '' "
        "AND (HebDesc IS NULL OR HebDesc = '')"
    ).fetchall()
    return [(str(r[0]), r[1]) for r in rows]


# =============================================================================
# Category Definitions
# =============================================================================

# Each category: (name, display_name, getter_func, field_name, direction)
CATEGORIES = [
    ("titles_he2en", "Titles (HE->EN)", get_title_gaps_he2en, "Title", "he2en"),
    ("titles_en2he", "Titles (EN->HE)", get_title_gaps_en2he, "TitleHeb", "en2he"),
    ("authors", "Authors (HE->EN)", get_author_gaps, "AuthorText", "he2en"),
    (
        "genizah_titles",
        "Genizah Titles (HE->EN)",
        get_genizah_title_gaps,
        "GenizahTitleEngTitle",
        "he2en",
    ),
    (
        "persons_he2en",
        "Persons (HE->EN)",
        get_person_gaps_he2en,
        "PersonEngDesc",
        "he2en",
    ),
    (
        "persons_en2he",
        "Persons (EN->HE)",
        get_person_gaps_en2he,
        "PersonHebDesc",
        "en2he",
    ),
]


def get_category_names() -> list[str]:
    """Return list of valid category names plus 'all' and 'titles'."""
    return ["all", "titles"] + [c[0] for c in CATEGORIES]


def resolve_categories(category_arg: str) -> list[tuple]:
    """Resolve --category argument to list of category tuples.

    Args:
        category_arg: Category name or "all" or "titles".

    Returns:
        List of category tuples from CATEGORIES.
    """
    if category_arg == "all":
        return list(CATEGORIES)
    if category_arg == "titles":
        return [c for c in CATEGORIES if c[0].startswith("titles_")]
    return [c for c in CATEGORIES if c[0] == category_arg]


# =============================================================================
# Write Translation to fjms_translations
# =============================================================================


def write_translation(
    conn: sqlite3.Connection,
    alma_id: str,
    field_name: str,
    original_text: str,
    translated_text: str,
    direction: str,
) -> None:
    """Insert a single translation into fjms_translations.

    Args:
        conn: Writable SQLite connection.
        alma_id: AlmaId or entity ID (for persons/titles, use the entity ID).
        field_name: Field name (e.g., 'Title', 'TitleHeb', 'AuthorText').
        original_text: Source text that was translated.
        translated_text: API translation result.
        direction: 'he2en' or 'en2he'.
    """
    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        "INSERT INTO fjms_translations "
        "(alma_id, field_name, signature_id, original_text, translated_text, "
        "direction, translated_at, model_version) "
        "VALUES (?, ?, NULL, ?, ?, ?, ?, ?)",
        (alma_id, field_name, original_text, translated_text, direction, now, "dictalm2.0"),
    )


# =============================================================================
# Main Batch Flow
# =============================================================================


def run_batch(args: argparse.Namespace) -> None:
    """Execute the batch translation pipeline.

    Args:
        args: Parsed CLI arguments.
    """
    try:
        from tqdm import tqdm
    except ImportError:
        tqdm = None

    conn = sqlite3.connect(args.fjms_db)

    # Resolve categories to process
    cats = resolve_categories(args.category)
    if not cats:
        print(f"Unknown category: {args.category}")
        print(f"Valid categories: {', '.join(get_category_names())}")
        conn.close()
        return

    # Dry-run: just report gap counts
    if args.dry_run:
        total = 0
        print("\nFJMS Catalog Gap Analysis")
        print("=" * 60)
        for name, display, getter, field, direction in cats:
            candidates = getter(conn)
            count = len(candidates)
            total += count
            print(f"  {display:30s} {count:>6,} candidates  (field: {field}, dir: {direction})")
        print(f"  {'TOTAL':30s} {total:>6,} candidates")
        print("=" * 60)
        print("Dry run complete. No translations performed.")
        conn.close()
        return

    # Load checkpoint
    checkpoint = load_checkpoint(args.checkpoint_file)
    if checkpoint:
        ckpt_total = sum(len(v) for v in checkpoint.values())
        print(f"Checkpoint loaded: {ckpt_total} items already completed.")

    # Load few-shot templates (one per direction)
    print("Loading few-shot templates...")
    he2en_template = load_few_shot_template(DEFAULT_FEW_SHOT_HE2EN)
    en2he_template = load_few_shot_template(DEFAULT_FEW_SHOT_EN2HE)
    he2en_prompt = build_few_shot_prompt(he2en_template, direction="he2en")
    en2he_prompt = build_few_shot_prompt(en2he_template, direction="en2he")

    # Ensure target table exists
    ensure_fjms_translations_table(conn)

    # Process each category
    grand_translated = 0
    grand_failed = 0
    grand_skipped = 0
    start_time = time.time()

    for cat_name, display, getter, field_name, direction in cats:
        print(f"\n--- {display} ---")
        candidates = getter(conn)
        cat_completed = checkpoint.get(cat_name, set())

        # Filter out completed
        pending = [(cid, text) for cid, text in candidates if cid not in cat_completed]

        # Deduplicate: group by text, translate each unique string once
        text_to_ids: dict[str, list[str]] = {}
        for cid, text in pending:
            text_to_ids.setdefault(text, []).append(cid)
        unique_pending = [(ids[0], text) for text, ids in text_to_ids.items()]

        if args.limit:
            unique_pending = unique_pending[: args.limit]

        skipped = len(candidates) - len(pending)
        grand_skipped += skipped
        print(
            f"  Candidates: {len(candidates)}, Previously done: {skipped}, "
            f"Pending: {len(pending)} ({len(unique_pending)} unique strings)"
        )

        if not unique_pending:
            continue

        few_shot = he2en_prompt if direction == "he2en" else en2he_prompt
        translated = 0
        failed = 0
        batch_count = 0

        pbar = tqdm(total=len(unique_pending), desc=f"  {display}", unit="item") if tqdm else None

        try:
            for rep_id, original_text in unique_pending:
                result = translate_text(original_text, few_shot, direction)
                if result is not None:
                    # Write for all IDs sharing this text
                    all_ids = text_to_ids[original_text]
                    for cid in all_ids:
                        write_translation(
                            conn, cid, field_name,
                            original_text, result, direction,
                        )
                        cat_completed.add(cid)
                    translated += len(all_ids)
                    batch_count += len(all_ids)
                else:
                    failed += 1
                    logger.warning(
                        "Translation failed for %s id=%s", cat_name, rep_id
                    )

                if pbar:
                    pbar.update(1)

                # Checkpoint at batch interval
                if batch_count >= args.batch_size:
                    conn.commit()
                    checkpoint[cat_name] = cat_completed
                    save_checkpoint(args.checkpoint_file, checkpoint)
                    batch_count = 0

                # Throttle to avoid rate limits
                time.sleep(REQUEST_DELAY)

        except KeyboardInterrupt:
            print(f"\n\nInterrupted during {display}! Saving checkpoint...")
            conn.commit()
            checkpoint[cat_name] = cat_completed
            save_checkpoint(args.checkpoint_file, checkpoint)
            print("Checkpoint saved. Resume by running the script again.")
            if pbar:
                pbar.close()
            conn.close()
            return
        finally:
            if pbar:
                pbar.close()

        # Final commit for this category
        conn.commit()
        checkpoint[cat_name] = cat_completed
        save_checkpoint(args.checkpoint_file, checkpoint)

        grand_translated += translated
        grand_failed += failed
        print(f"  Translated: {translated}, Failed: {failed}")

    conn.close()

    # Summary
    elapsed = time.time() - start_time
    minutes = elapsed / 60
    print(f"\n{'=' * 60}")
    print("FJMS Catalog Translation Summary")
    print(f"{'=' * 60}")
    print(f"  Categories processed: {len(cats)}")
    print(f"  Previously done:      {grand_skipped}")
    print(f"  Translated now:       {grand_translated}")
    print(f"  Failed:               {grand_failed}")
    print(f"  Elapsed:              {minutes:.1f} min ({elapsed:.0f}s)")
    if grand_translated > 0:
        rate = elapsed / grand_translated
        print(f"  Rate:                 {rate:.2f}s per item")
    print(f"  Checkpoint:           {args.checkpoint_file}")
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
        description="Batch translate FJMS catalog field gaps via Dicta API.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""Examples:
  python scripts/translate_fjms_catalog.py --dry-run           # Count all gaps
  python scripts/translate_fjms_catalog.py --category titles    # Titles only
  python scripts/translate_fjms_catalog.py --limit 20           # Test with 20 items
""",
    )
    parser.add_argument(
        "--fjms-db",
        default=DEFAULT_FJMS_DB,
        help=f"Path to fjms_enrichment.db (default: {DEFAULT_FJMS_DB})",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=50,
        help="Checkpoint every N translations (default: 50)",
    )
    parser.add_argument(
        "--checkpoint-file",
        default=DEFAULT_CHECKPOINT,
        help=f"Checkpoint JSON path (default: {DEFAULT_CHECKPOINT})",
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
        help="Translate only first N rows per category (for testing)",
    )
    parser.add_argument(
        "--category",
        default="all",
        choices=get_category_names(),
        help="Translate specific category only (default: all)",
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
