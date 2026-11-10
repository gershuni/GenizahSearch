#!/usr/bin/env python3
"""
Import Manchester LUNA collection IDs into the NLI crossref sidecar database.

Paginates through the entire Manchester LUNA digital collection via the
fetchMediaSearch API, extracts JRL filenames from image URLs, and maps them
to crossref ImageSourceName values. Stores luna_id mappings in a new
`manchester_luna` table in nli_data/nli_crossref.db.

This is the data foundation for Manchester IIIF image integration (v5.9.0).
Manchester manuscripts (~13.5K unique) need LUNA internal IDs to construct
detail page URLs and IIIF manifest URLs.

Usage:
    python scripts/import_manchester_luna.py                  # Full import
    python scripts/import_manchester_luna.py --dry-run        # Fetch without writing
    python scripts/import_manchester_luna.py --batch-size 500 --delay 0.2  # Faster
"""

import argparse
import os
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from tqdm import tqdm

# ── Constants ────────────────────────────────────────────────────────

LUNA_API_URL = "https://luna.manchester.ac.uk/luna/servlet/as/fetchMediaSearch"
LUNA_COLLECTION = "ManchesterDev~95~2"
DEFAULT_BATCH_SIZE = 100
DEFAULT_DELAY = 0.5
CHECKPOINT_INTERVAL = 1000  # Save progress every N items
EXPECTED_TOTAL = 28000  # Approximate total for progress bar

VERSION = "1.1.0"


def create_session() -> requests.Session:
    """Create a requests session with retry logic for 5xx errors."""
    session = requests.Session()
    retry_strategy = Retry(
        total=3,
        backoff_factor=1.0,  # 1s, 2s, 4s
        status_forcelist=[500, 502, 503, 504],
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def extract_jrl_filename(url_size0: str) -> str:
    """
    Extract the JRL filename from a LUNA urlSize0 URL.

    Example input:
        https://luna.manchester.ac.uk/MediaManager/srvr?mediafile=/Size0/ManchesterDev-2-NA/1120/jrl0708074dc.jpg
    Example output:
        jrl0708074dc

    Args:
        url_size0: The urlSize0 URL string from the LUNA API.

    Returns:
        Lowercased JRL filename without extension, or empty string if extraction fails.
    """
    if not url_size0:
        return ""
    try:
        # The filename is in the mediafile parameter value, after the last /
        # URL format: ...?mediafile=/Size0/ManchesterDev-2-NA/{folder}/{filename}.jpg
        # Parse the query string to get the mediafile path
        if "mediafile=" in url_size0:
            media_path = url_size0.split("mediafile=")[-1]
        else:
            # Fallback: try parsing the full URL path
            parsed = urlparse(url_size0)
            media_path = parsed.path

        # Get the last segment and strip the extension
        filename = media_path.rsplit("/", 1)[-1]
        # Strip .jpg or any other extension
        name, _ = os.path.splitext(filename)
        return name.lower()
    except Exception:
        return ""


def fetch_luna_page(session: requests.Session, offset: int, batch_size: int) -> list:
    """
    Fetch a single page of LUNA items.

    Args:
        session: Requests session with retry logic.
        offset: The starting offset (os parameter).
        batch_size: Number of items per page (bs parameter).

    Returns:
        List of item dicts from the API, or empty list on error.
    """
    params = {
        "fullData": "false",
        "q": "",
        "bs": str(batch_size),
        "os": str(offset),
        "lc": LUNA_COLLECTION,
    }
    try:
        resp = session.get(LUNA_API_URL, params=params, timeout=30)
        if resp.status_code != 200:
            print(f"\n  WARNING: LUNA API returned {resp.status_code} at offset {offset}")
            return []
        data = resp.json()
        if not isinstance(data, list):
            print(f"\n  WARNING: Unexpected response type at offset {offset}: {type(data)}")
            return []
        return data
    except requests.exceptions.RequestException as e:
        print(f"\n  WARNING: Request error at offset {offset}: {e}")
        return []
    except ValueError as e:
        print(f"\n  WARNING: JSON decode error at offset {offset}: {e}")
        return []


def create_manchester_luna_table(cursor: sqlite3.Cursor):
    """Create (or recreate) the manchester_luna table."""
    cursor.execute("DROP TABLE IF EXISTS manchester_luna")
    cursor.execute("""
        CREATE TABLE manchester_luna (
            image_source_name TEXT PRIMARY KEY,
            luna_id TEXT NOT NULL,
            url_size0 TEXT
        )
    """)
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_ml_luna_id ON manchester_luna(luna_id)"
    )
    cursor.connection.commit()


def import_luna_items(
    cursor: sqlite3.Cursor,
    session: requests.Session,
    batch_size: int,
    delay: float,
    dry_run: bool,
) -> tuple[int, list]:
    """
    Paginate through all LUNA items and import into the database.

    Args:
        cursor: SQLite cursor for the sidecar DB.
        session: Requests session.
        batch_size: Items per API page.
        delay: Seconds to wait between API calls.
        dry_run: If True, fetch but don't write to DB.

    Returns:
        Tuple of (total_fetched, list of (image_source_name, luna_id, url_size0) tuples).
    """
    offset = 0
    total_fetched = 0
    all_items = []
    db_batch = []
    consecutive_empty = 0

    pbar = tqdm(total=EXPECTED_TOTAL, desc="  LUNA fetch", unit=" items")

    while True:
        items = fetch_luna_page(session, offset, batch_size)

        if not items:
            consecutive_empty += 1
            if consecutive_empty >= 3:
                # Three consecutive empty responses -- we're done
                break
            # Single empty response could be a glitch, try next offset
            offset += batch_size
            time.sleep(delay)
            continue

        consecutive_empty = 0

        for item in items:
            luna_id = item.get("id", "") or item.get("identity", "")
            url_size0 = item.get("urlSize0", "")
            jrl_filename = extract_jrl_filename(url_size0)

            if not luna_id or not jrl_filename:
                continue

            row = (jrl_filename, luna_id, url_size0)
            all_items.append(row)

            if not dry_run:
                db_batch.append(row)

        total_fetched += len(items)
        pbar.update(len(items))

        # Checkpoint: write batch to DB periodically
        if not dry_run and len(db_batch) >= CHECKPOINT_INTERVAL:
            cursor.executemany(
                "INSERT OR REPLACE INTO manchester_luna "
                "(image_source_name, luna_id, url_size0) VALUES (?, ?, ?)",
                db_batch,
            )
            cursor.connection.commit()
            db_batch = []

        offset += batch_size
        time.sleep(delay)

    # Flush remaining batch
    if not dry_run and db_batch:
        cursor.executemany(
            "INSERT OR REPLACE INTO manchester_luna "
            "(image_source_name, luna_id, url_size0) VALUES (?, ?, ?)",
            db_batch,
        )
        cursor.connection.commit()

    pbar.close()
    return total_fetched, all_items


def update_meta(cursor: sqlite3.Cursor):
    """Update meta table with Manchester source info and bump version."""
    now = datetime.now(timezone.utc).isoformat()

    # Update version
    cursor.execute(
        "INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)",
        ("version", VERSION),
    )
    # Add Manchester source
    cursor.execute(
        "INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)",
        ("source_manchester", "LUNA fetchMediaSearch API"),
    )
    # Record import timestamp
    cursor.execute(
        "INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)",
        ("manchester_imported", now),
    )
    cursor.connection.commit()
    print(f"  Meta table updated (version {VERSION})")


def report_match_stats(cursor: sqlite3.Cursor, total_fetched: int, total_stored: int):
    """Report match statistics between manchester_luna and nli_images."""
    print(f"\n{'='*60}")
    print("Match Statistics")
    print(f"{'='*60}")

    print(f"  Total LUNA items fetched:  {total_fetched:>10,}")
    print(f"  Items with JRL filename:   {total_stored:>10,}")

    # Check if nli_images table exists and has Manchester data
    try:
        matched = cursor.execute("""
            SELECT COUNT(DISTINCT i.NLI_AlmaId)
            FROM nli_images i
            JOIN manchester_luna m ON LOWER(i.ImageSourceName) = m.image_source_name
            WHERE i.LibraryAbbrev = 'Manchester'
        """).fetchone()[0]

        total_manchester = cursor.execute("""
            SELECT COUNT(DISTINCT NLI_AlmaId)
            FROM nli_images
            WHERE LibraryAbbrev = 'Manchester'
        """).fetchone()[0]

        total_manchester_images = cursor.execute("""
            SELECT COUNT(*)
            FROM nli_images
            WHERE LibraryAbbrev = 'Manchester'
        """).fetchone()[0]

        luna_matched_images = cursor.execute("""
            SELECT COUNT(*)
            FROM nli_images i
            JOIN manchester_luna m ON LOWER(i.ImageSourceName) = m.image_source_name
            WHERE i.LibraryAbbrev = 'Manchester'
        """).fetchone()[0]

        print(f"\n  Crossref Manchester manuscripts: {total_manchester:>7,}")
        print(f"  Matched to LUNA:                 {matched:>7,}")
        print(f"  Match rate (manuscripts):        {matched/total_manchester*100:>6.1f}%")
        print(f"\n  Crossref Manchester images:      {total_manchester_images:>7,}")
        print(f"  Matched to LUNA (images):        {luna_matched_images:>7,}")
        print(f"  Match rate (images):             {luna_matched_images/total_manchester_images*100:>6.1f}%")

    except Exception as e:
        print(f"  WARNING: Could not compute match stats: {e}")

    print(f"{'='*60}")


def main():
    parser = argparse.ArgumentParser(
        description="Import Manchester LUNA collection IDs into NLI crossref sidecar database."
    )
    parser.add_argument(
        "--db-path",
        default=None,
        help="Path to nli_crossref.db (default: nli_data/nli_crossref.db)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help=f"Items per API page (default: {DEFAULT_BATCH_SIZE})",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=DEFAULT_DELAY,
        help=f"Seconds between API calls (default: {DEFAULT_DELAY})",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch from LUNA but don't write to database",
    )
    args = parser.parse_args()

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

    mode_label = "DRY RUN" if args.dry_run else "IMPORT"
    print(f"Manchester LUNA Import ({mode_label})")
    print(f"  Database:   {db_path}")
    print(f"  Batch size: {args.batch_size}")
    print(f"  Delay:      {args.delay}s")
    print()

    # Connect to sidecar database
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()

    try:
        # Create table (even in dry-run, to allow match stats query)
        if not args.dry_run:
            create_manchester_luna_table(cursor)

        # Create HTTP session with retries
        session = create_session()

        # Paginate through all LUNA items
        total_fetched, all_items = import_luna_items(
            cursor, session, args.batch_size, args.delay, args.dry_run,
        )

        total_stored = len(all_items)

        if args.dry_run:
            print(f"\n  DRY RUN: Would store {total_stored:,} items")
            # Show a few samples
            for row in all_items[:5]:
                print(f"    {row[0]} -> {row[1]}")
            if total_stored > 5:
                print(f"    ... and {total_stored - 5:,} more")
        else:
            # Verify stored count
            db_count = cursor.execute(
                "SELECT COUNT(*) FROM manchester_luna"
            ).fetchone()[0]
            print(f"\n  Stored {db_count:,} items in manchester_luna table")

            # Update meta table
            update_meta(cursor)

            # Report match statistics
            report_match_stats(cursor, total_fetched, db_count)

    finally:
        conn.close()


if __name__ == "__main__":
    main()
