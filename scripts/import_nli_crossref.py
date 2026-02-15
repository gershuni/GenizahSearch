#!/usr/bin/env python3
"""
Import NLI crossreference and Cambridge IIIF data into a SQLite sidecar database.

Reads from nli_crossreference.csv and cambridge_genizah.json, producing
nli_data/nli_crossref.db with the following tables:
  - nli_images:            NLI crossref image records (~815K rows, all 25 columns)
  - cambridge_manifests:   Cambridge IIIF manifest URLs (~141K rows)
  - meta:                  Version and build metadata

This is the data foundation for multi-source image integration (v5.9.0).
"""

import csv
import json
import os
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

from tqdm import tqdm

# Add project root to path so we can import genizah_core
sys.path.insert(0, str(Path(__file__).parent.parent))
from genizah_core import normalize_shelfmark

VERSION = "1.0.0"
BATCH_SIZE = 10_000

# All 25 columns from the NLI crossreference CSV, in order
NLI_COLUMNS = [
    "LibraryNameEng", "LibraryAbbrev", "LibraryCity", "LibraryNameHeb",
    "CollectionName", "Shelfmark", "InventoryId", "OBBox", "OBVolume",
    "OBFolio", "NLI_AlmaId", "CatalogAbbrev", "CatalogEntry",
    "FGPImageNumberId", "FGPNumber", "ImageName", "ImageSourceName",
    "PartOf", "See", "BifolioWith", "NumFolio", "NumBifolio",
    "Material", "Size", "IsNotGenizah",
]


def cudl_label_to_shelfmark(label: str) -> str:
    """
    Convert a CUDL manifest label to a human-readable shelfmark.

    Steps:
    1. Strip 'MS-' prefix
    2. Split by '-'
    3. Strip leading zeros from numeric segments (e.g., '00006' -> '6')
    4. Rejoin: '.' between consecutive numeric parts, space otherwise

    Examples:
        MS-TS-00016-00114 -> 'TS 16.114'
        MS-ADD-00863-00002 -> 'ADD 863.2'
        MS-MOSSERI-II-00292-00002 -> 'MOSSERI II 292.2'
    """
    s = label
    if s.startswith("MS-"):
        s = s[3:]

    parts = s.split("-")
    cleaned = []
    for p in parts:
        if p.isdigit():
            cleaned.append(str(int(p)))  # strip leading zeros
        else:
            cleaned.append(p)

    if not cleaned:
        return label

    result = cleaned[0]
    for i in range(1, len(cleaned)):
        prev_numeric = cleaned[i - 1].isdigit()
        curr_numeric = cleaned[i].isdigit()
        if prev_numeric and curr_numeric:
            result += "." + cleaned[i]
        else:
            result += " " + cleaned[i]

    return result


def import_nli_images(target):
    """Import NLI crossreference image records from CSV."""
    print("Importing NLI crossreference images...")

    script_dir = Path(__file__).parent
    project_dir = script_dir.parent
    csv_path = project_dir / "nli_crossreference.csv"

    if not csv_path.exists():
        print(f"  ERROR: CSV not found: {csv_path}")
        return 0

    target.execute("DROP TABLE IF EXISTS nli_images")

    # Create table with all 25 columns as TEXT
    col_defs = ", ".join(f"{col} TEXT" for col in NLI_COLUMNS)
    target.execute(f"CREATE TABLE nli_images ({col_defs})")

    placeholders = ", ".join(["?"] * len(NLI_COLUMNS))

    batch = []
    total = 0

    with open(csv_path, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in tqdm(reader, desc="  nli_images", unit=" rows"):
            values = tuple(row.get(col, None) for col in NLI_COLUMNS)
            batch.append(values)
            if len(batch) >= BATCH_SIZE:
                target.executemany(
                    f"INSERT INTO nli_images VALUES ({placeholders})", batch
                )
                total += len(batch)
                batch = []

    if batch:
        target.executemany(
            f"INSERT INTO nli_images VALUES ({placeholders})", batch
        )
        total += len(batch)

    # Create indexes for downstream queries
    target.execute("CREATE INDEX idx_nli_alma ON nli_images(NLI_AlmaId)")
    target.execute("CREATE INDEX idx_nli_fgp ON nli_images(FGPImageNumberId)")
    target.execute("CREATE INDEX idx_nli_shelfmark ON nli_images(Shelfmark)")
    target.connection.commit()

    distinct_alma = target.execute(
        "SELECT COUNT(DISTINCT NLI_AlmaId) FROM nli_images"
    ).fetchone()[0]
    print(f"  Imported {total:,} rows ({distinct_alma:,} distinct AlmaIds)")
    return total


def import_cambridge_manifests(target):
    """Import Cambridge IIIF manifest URLs from JSON."""
    print("Importing Cambridge IIIF manifests...")

    script_dir = Path(__file__).parent
    project_dir = script_dir.parent
    json_path = project_dir / "cambridge_genizah.json"

    if not json_path.exists():
        print(f"  ERROR: JSON not found: {json_path}")
        return 0

    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    manifests = data.get("manifests", [])
    if not manifests:
        print("  ERROR: No manifests found in JSON")
        return 0

    target.execute("DROP TABLE IF EXISTS cambridge_manifests")
    target.execute("""
        CREATE TABLE cambridge_manifests (
            label TEXT NOT NULL,
            manifest_url TEXT NOT NULL,
            normalized_shelfmark TEXT
        )
    """)

    batch = []
    total = 0

    for item in tqdm(manifests, desc="  cambridge", unit=" rows"):
        label = item.get("label", "")
        manifest_url = item.get("@id", "")

        # Convert CUDL label to readable shelfmark, then normalize
        readable = cudl_label_to_shelfmark(label)
        normalized = normalize_shelfmark(readable)

        batch.append((label, manifest_url, normalized))
        if len(batch) >= BATCH_SIZE:
            target.executemany(
                "INSERT INTO cambridge_manifests VALUES (?, ?, ?)", batch
            )
            total += len(batch)
            batch = []

    if batch:
        target.executemany(
            "INSERT INTO cambridge_manifests VALUES (?, ?, ?)", batch
        )
        total += len(batch)

    # Create indexes for downstream queries
    target.execute(
        "CREATE INDEX idx_cam_shelfmark ON cambridge_manifests(normalized_shelfmark)"
    )
    target.execute(
        "CREATE INDEX idx_cam_label ON cambridge_manifests(label)"
    )
    target.connection.commit()

    print(f"  Imported {total:,} manifests")
    return total


def create_meta(target):
    """Create meta table with version and build info."""
    target.execute("DROP TABLE IF EXISTS meta")
    target.execute("""
        CREATE TABLE meta (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )
    """)

    now = datetime.now(timezone.utc).isoformat()
    target.executemany(
        "INSERT INTO meta (key, value) VALUES (?, ?)",
        [
            ("version", VERSION),
            ("created", now),
            ("source_nli", "nli_crossreference.csv"),
            ("source_cambridge", "cambridge_genizah.json"),
        ],
    )
    target.connection.commit()
    print(f"  Meta table created (version {VERSION})")


def main():
    script_dir = Path(__file__).parent
    project_dir = script_dir.parent

    target_dir = project_dir / "nli_data"
    target_path = target_dir / "nli_crossref.db"

    # Create output directory if needed
    target_dir.mkdir(exist_ok=True)

    # Delete existing target for idempotent re-runs
    if target_path.exists():
        print(f"Removing existing {target_path.name}...")
        os.remove(target_path)

    print(f"Target: {target_path}")
    print()

    # Create target database
    target_conn = sqlite3.connect(str(target_path))
    target = target_conn.cursor()
    target.execute("PRAGMA journal_mode=WAL")
    target.execute("PRAGMA synchronous=NORMAL")

    try:
        nli_count = import_nli_images(target)
        cam_count = import_cambridge_manifests(target)
        create_meta(target)

        # Compact the database
        print("\nCompacting database...")
        target.execute("PRAGMA journal_mode=DELETE")
        target_conn.commit()
        target.execute("VACUUM")
        target_conn.commit()

        # Summary
        file_size_mb = target_path.stat().st_size / (1024 * 1024)
        print(f"\nImport complete!")
        print(f"  nli_images:          {nli_count:>10,} rows")
        print(f"  cambridge_manifests: {cam_count:>10,} rows")
        print(f"  File size: {file_size_mb:.1f} MB")

    finally:
        target_conn.close()


if __name__ == "__main__":
    main()
