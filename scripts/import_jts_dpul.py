#!/usr/bin/env python3
"""
Import JTS/Princeton DPUL catalog data into the NLI crossref sidecar database.

For each unique JTS shelfmark in the crossref data, searches the Princeton
Digital Library (DPUL) cairo_geniza collection to find the corresponding
catalog entry, extracts the ARK identifier and Figgy IIIF manifest URL,
and stores the mapping in a new `jts_dpul` table in nli_data/nli_crossref.db.

Each DPUL item corresponds to a specific leaf-level shelfmark (e.g.,
"ENA 2573.1"), so we search per crossref shelfmark rather than per base
shelfmark. This gives ~44K searches with a ~60-65% match rate.

Usage:
    python scripts/import_jts_dpul.py --dry-run --limit 10    # Test mode
    python scripts/import_jts_dpul.py --limit 500             # Partial import
    python scripts/import_jts_dpul.py --workers 3             # Full import, parallel
    python scripts/import_jts_dpul.py --resume                # Resume interrupted import
"""

import argparse
import json
import os
import re
import sqlite3
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock
from urllib.parse import quote as url_quote

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from tqdm import tqdm

# ── Constants ────────────────────────────────────────────────────────

DPUL_CATALOG_URL = "https://dpul.princeton.edu/cairo_geniza/catalog.json"
DPUL_ITEM_URL = "https://dpul.princeton.edu/cairo_geniza/catalog/{ark_suffix}.json"
DPUL_PAGE_URL = "https://dpul.princeton.edu/cairo_geniza/catalog/{ark_suffix}"
DEFAULT_DELAY = 0.3
MAX_WORKERS = 5
CHECKPOINT_INTERVAL = 100  # Save progress every N shelfmarks

VERSION = "1.2.0"


def create_session() -> requests.Session:
    """Create a requests session with retry logic for 5xx errors."""
    session = requests.Session()
    retry_strategy = Retry(
        total=3,
        backoff_factor=1.0,  # 1s, 2s, 4s
        status_forcelist=[429, 500, 502, 503, 504],
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def search_dpul(session: requests.Session, shelfmark: str, delay: float) -> dict | None:
    """
    Search DPUL catalog for a shelfmark and fetch item details.

    Two-step process:
    1. Search catalog with exact quoted shelfmark
    2. If found, fetch item details for manifest URL and thumbnail

    Args:
        session: Requests session with retry logic.
        shelfmark: The JTS shelfmark to search for.
        delay: Seconds to wait between API calls.

    Returns:
        Dict with keys: ark_suffix, manifest_url, dpul_url, thumbnail_url.
        Returns None if not found or on error.
    """
    # Step A: Search DPUL catalog with exact quoted shelfmark
    try:
        resp = session.get(
            DPUL_CATALOG_URL,
            params={
                "search_field": "all_fields",
                "q": f'"{shelfmark}"',
            },
            timeout=15,
        )
        if resp.status_code != 200:
            return None
        data = resp.json()
    except (requests.exceptions.RequestException, ValueError):
        return None

    items = data.get("data", [])
    if not items:
        return None

    # Take first result -- verify title matches our shelfmark
    first = items[0]
    title_html = first.get("attributes", {}).get("title", "")
    # Strip HTML: <ul><li dir="ltr">ENA 2573.1</li></ul> -> ENA 2573.1
    title_text = re.sub(r"<[^>]+>", "", title_html).strip()

    # Only accept exact matches (case-insensitive)
    if title_text.lower() != shelfmark.lower():
        # If first result doesn't match exactly, search through all results
        found = False
        for item in items:
            t_html = item.get("attributes", {}).get("title", "")
            t_text = re.sub(r"<[^>]+>", "", t_html).strip()
            if t_text.lower() == shelfmark.lower():
                first = item
                found = True
                break
        if not found:
            return None

    # Extract ARK suffix from the self link URL
    link = first.get("links", {}).get("self", "")
    if not link:
        return None
    ark_suffix = link.rsplit("/", 1)[-1]
    if not ark_suffix:
        return None

    dpul_url = DPUL_PAGE_URL.format(ark_suffix=ark_suffix)

    # Rate limit between the two API calls
    time.sleep(delay)

    # Step B: Fetch item details for manifest URL and thumbnail
    manifest_url = None
    thumbnail_url = None

    try:
        detail_url = DPUL_ITEM_URL.format(ark_suffix=ark_suffix)
        resp2 = session.get(detail_url, timeout=15)
        if resp2.status_code == 200:
            detail = resp2.json()
            doc = detail.get("response", {}).get("document", {})
            manifest_url = doc.get("content_metadata_iiif_manifest_field_ssi")
            thumbs = doc.get("thumbnail_ssim", [])
            if thumbs:
                thumbnail_url = thumbs[0]
    except (requests.exceptions.RequestException, ValueError):
        # Non-fatal: we still have the ARK and DPUL URL
        pass

    return {
        "ark_suffix": ark_suffix,
        "manifest_url": manifest_url,
        "dpul_url": dpul_url,
        "thumbnail_url": thumbnail_url,
    }


def get_jts_shelfmarks(db_path: str) -> list[str]:
    """Get all unique JTS shelfmarks from the crossref database."""
    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute(
            "SELECT DISTINCT Shelfmark FROM nli_images "
            "WHERE LibraryAbbrev = 'JTS' AND Shelfmark != '' "
            "ORDER BY Shelfmark"
        ).fetchall()
        return [r[0] for r in rows]
    finally:
        conn.close()


def load_checkpoint(checkpoint_path: Path) -> dict:
    """Load checkpoint data from JSON file."""
    if not checkpoint_path.exists():
        return {"completed": set(), "results": {}}
    try:
        with open(checkpoint_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return {
            "completed": set(data.get("completed", [])),
            "results": data.get("results", {}),
        }
    except (json.JSONDecodeError, IOError):
        return {"completed": set(), "results": {}}


def save_checkpoint(checkpoint_path: Path, completed: set, results: dict):
    """Save checkpoint data to JSON file."""
    data = {
        "completed": sorted(completed),
        "results": results,
    }
    # Write atomically via temp file
    tmp_path = checkpoint_path.with_suffix(".tmp")
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(data, f)
    tmp_path.replace(checkpoint_path)


def create_jts_dpul_table(cursor: sqlite3.Cursor):
    """Create (or recreate) the jts_dpul table."""
    cursor.execute("DROP TABLE IF EXISTS jts_dpul")
    cursor.execute("""
        CREATE TABLE jts_dpul (
            shelfmark TEXT PRIMARY KEY,
            ark_suffix TEXT NOT NULL,
            manifest_url TEXT,
            dpul_url TEXT NOT NULL,
            thumbnail_url TEXT
        )
    """)
    cursor.execute("CREATE INDEX idx_jd_ark ON jts_dpul(ark_suffix)")
    cursor.connection.commit()


def process_shelfmark(session: requests.Session, shelfmark: str, delay: float) -> tuple[str, dict | None]:
    """
    Process a single shelfmark -- search DPUL and return result.

    Returns:
        Tuple of (shelfmark, result_dict or None).
    """
    result = search_dpul(session, shelfmark, delay)
    return (shelfmark, result)


def run_import(
    cursor: sqlite3.Cursor,
    shelfmarks: list[str],
    checkpoint_path: Path,
    delay: float,
    workers: int,
    dry_run: bool,
    resume: bool,
) -> tuple[int, int, int]:
    """
    Run the DPUL import for all shelfmarks.

    Args:
        cursor: SQLite cursor for the sidecar DB.
        shelfmarks: List of JTS shelfmarks to process.
        checkpoint_path: Path to checkpoint JSON file.
        delay: Seconds between API calls.
        workers: Number of parallel workers.
        dry_run: If True, search but don't write to DB.
        resume: If True, skip already-completed shelfmarks.

    Returns:
        Tuple of (searched_count, found_count, manifest_count).
    """
    # Load checkpoint if resuming
    if resume:
        checkpoint = load_checkpoint(checkpoint_path)
        completed = checkpoint["completed"]
        results = checkpoint["results"]
        remaining = [s for s in shelfmarks if s not in completed]
        print(f"  Resuming: {len(completed):,} already done, {len(remaining):,} remaining")
    else:
        completed = set()
        results = {}
        remaining = shelfmarks

    if not remaining:
        print("  All shelfmarks already processed!")
        return len(shelfmarks), len(results), sum(1 for r in results.values() if r.get("manifest_url"))

    searched = len(completed)
    found = sum(1 for r in results.values() if r)
    with_manifest = sum(1 for r in results.values() if r and r.get("manifest_url"))

    db_batch = []
    checkpoint_lock = Lock()
    pbar = tqdm(total=len(shelfmarks), initial=len(completed), desc="  DPUL search", unit=" shelfmarks")

    def process_and_track(session, sm):
        """Process a shelfmark and return result."""
        return process_shelfmark(session, sm, delay)

    if workers <= 1:
        # Sequential processing
        session = create_session()
        for sm in remaining:
            _, result = process_and_track(session, sm)

            completed.add(sm)
            if result:
                results[sm] = result
                found += 1
                if result.get("manifest_url"):
                    with_manifest += 1
                if not dry_run:
                    db_batch.append((
                        sm,
                        result["ark_suffix"],
                        result.get("manifest_url"),
                        result["dpul_url"],
                        result.get("thumbnail_url"),
                    ))
            searched += 1
            pbar.update(1)

            # Checkpoint and DB flush
            if len(completed) % CHECKPOINT_INTERVAL == 0:
                save_checkpoint(checkpoint_path, completed, results)
                if not dry_run and db_batch:
                    cursor.executemany(
                        "INSERT OR REPLACE INTO jts_dpul "
                        "(shelfmark, ark_suffix, manifest_url, dpul_url, thumbnail_url) "
                        "VALUES (?, ?, ?, ?, ?)",
                        db_batch,
                    )
                    cursor.connection.commit()
                    db_batch = []

            time.sleep(delay)
    else:
        # Parallel processing with ThreadPoolExecutor
        sessions = [create_session() for _ in range(workers)]

        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {}
            for i, sm in enumerate(remaining):
                sess = sessions[i % workers]
                future = executor.submit(process_and_track, sess, sm)
                futures[future] = sm

            for future in as_completed(futures):
                sm = futures[future]
                try:
                    _, result = future.result()
                except Exception:
                    result = None

                with checkpoint_lock:
                    completed.add(sm)
                    if result:
                        results[sm] = result
                        found += 1
                        if result.get("manifest_url"):
                            with_manifest += 1
                        if not dry_run:
                            db_batch.append((
                                sm,
                                result["ark_suffix"],
                                result.get("manifest_url"),
                                result["dpul_url"],
                                result.get("thumbnail_url"),
                            ))
                    searched += 1
                    pbar.update(1)

                    # Checkpoint and DB flush
                    if len(completed) % CHECKPOINT_INTERVAL == 0:
                        save_checkpoint(checkpoint_path, completed, results)
                        if not dry_run and db_batch:
                            cursor.executemany(
                                "INSERT OR REPLACE INTO jts_dpul "
                                "(shelfmark, ark_suffix, manifest_url, dpul_url, thumbnail_url) "
                                "VALUES (?, ?, ?, ?, ?)",
                                db_batch,
                            )
                            cursor.connection.commit()
                            db_batch = []

    pbar.close()

    # Final flush
    save_checkpoint(checkpoint_path, completed, results)
    if not dry_run and db_batch:
        cursor.executemany(
            "INSERT OR REPLACE INTO jts_dpul "
            "(shelfmark, ark_suffix, manifest_url, dpul_url, thumbnail_url) "
            "VALUES (?, ?, ?, ?, ?)",
            db_batch,
        )
        cursor.connection.commit()

    return searched, found, with_manifest


def update_meta(cursor: sqlite3.Cursor):
    """Update meta table with JTS DPUL source info and bump version."""
    now = datetime.now(timezone.utc).isoformat()

    cursor.execute(
        "INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)",
        ("version", VERSION),
    )
    cursor.execute(
        "INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)",
        ("source_jts", "DPUL catalog API"),
    )
    cursor.execute(
        "INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)",
        ("jts_imported", now),
    )
    cursor.connection.commit()
    print(f"  Meta table updated (version {VERSION})")


def main():
    parser = argparse.ArgumentParser(
        description="Import JTS/Princeton DPUL catalog data into NLI crossref sidecar database."
    )
    parser.add_argument(
        "--db-path",
        default=None,
        help="Path to nli_crossref.db (default: nli_data/nli_crossref.db)",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=DEFAULT_DELAY,
        help=f"Seconds between API calls (default: {DEFAULT_DELAY})",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=1,
        help=f"Parallel workers (default: 1, max: {MAX_WORKERS})",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Process only first N shelfmarks (for testing)",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Resume from checkpoint (skip already-processed shelfmarks)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Search DPUL but don't write to database",
    )
    args = parser.parse_args()

    # Clamp workers
    if args.workers > MAX_WORKERS:
        args.workers = MAX_WORKERS
    if args.workers < 1:
        args.workers = 1

    # Resolve DB path
    if args.db_path:
        db_path = Path(args.db_path)
    else:
        script_dir = Path(__file__).parent
        project_dir = script_dir.parent
        db_path = project_dir / "nli_data" / "nli_crossref.db"

    if not db_path.exists():
        print(f"ERROR: Sidecar database not found at {db_path}")
        print("Run scripts/import_nli_crossref.py first to create it.")
        sys.exit(1)

    # Checkpoint file
    checkpoint_path = db_path.parent / "jts_dpul_checkpoint.json"

    mode_label = "DRY RUN" if args.dry_run else "IMPORT"
    print(f"JTS/Princeton DPUL Import ({mode_label})")
    print(f"  Database:   {db_path}")
    print(f"  Delay:      {args.delay}s")
    print(f"  Workers:    {args.workers}")
    if args.limit:
        print(f"  Limit:      {args.limit}")
    if args.resume:
        print(f"  Resume:     from checkpoint")
    print()

    # Get unique JTS shelfmarks from crossref
    shelfmarks = get_jts_shelfmarks(str(db_path))
    print(f"  Total unique JTS shelfmarks: {len(shelfmarks):,}")

    if args.limit:
        shelfmarks = shelfmarks[:args.limit]
        print(f"  Limited to first {args.limit}")
    print()

    # Connect to sidecar database
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()

    try:
        # Create table (unless resuming with existing data)
        if not args.dry_run and not args.resume:
            create_jts_dpul_table(cursor)

        # Run import
        searched, found, with_manifest = run_import(
            cursor, shelfmarks, checkpoint_path, args.delay,
            args.workers, args.dry_run, args.resume,
        )

        # Report
        print(f"\n{'='*60}")
        print("Import Statistics")
        print(f"{'='*60}")
        print(f"  Shelfmarks searched:  {searched:>10,}")
        print(f"  Found in DPUL:        {found:>10,}")
        not_found = searched - found
        print(f"  Not found:            {not_found:>10,}")
        print(f"  With manifest URL:    {with_manifest:>10,}")
        if searched > 0:
            print(f"  Match rate:           {found/searched*100:>9.1f}%")

        if args.dry_run:
            print(f"\n  DRY RUN: No data written to database")
        else:
            # Verify stored count
            db_count = cursor.execute(
                "SELECT COUNT(*) FROM jts_dpul"
            ).fetchone()[0]
            print(f"\n  Stored in jts_dpul table: {db_count:,} rows")

            # Update meta table
            update_meta(cursor)

            # Clean up checkpoint on successful completion
            if not args.limit and checkpoint_path.exists():
                checkpoint_path.unlink()
                print("  Checkpoint file cleaned up")

        print(f"{'='*60}")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
