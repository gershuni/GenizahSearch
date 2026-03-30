#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Import visual similarity data from FIST.db into visual_similarity.db sidecar.

Maps FIST.db Image_BestMarkForJoin SVM-scored pairs through the chain:
  DocumentID -> Image_ImageDocument -> dbo_ImgDigitalImage -> dbo_InventoryAlma -> AlmaId

Uses ATTACH DATABASE + in-SQL joins for performance (no Python dict loops).
Uses PRAGMA tuning for write performance on 15M+ row imports.

Usage:
    python scripts/import_visual_similarity.py [--fist-db fist_data/FIST.db] [--output fist_data/visual_similarity.db]
"""

import argparse
import os
import sqlite3
import sys
import time
from datetime import date
from pathlib import Path


def import_visual_similarity(fist_db_path: str, output_db_path: str):
    """Import visual similarity pairs from FIST.db into a new sidecar.

    Uses ATTACH DATABASE + in-SQL mapping (no Python dict loops) per review feedback.
    Uses PRAGMA tuning for write performance.

    Args:
        fist_db_path: Path to source FIST.db.
        output_db_path: Path to output visual_similarity.db (will be created/overwritten).
    """
    if not os.path.isfile(fist_db_path):
        print(f"ERROR: FIST.db not found at {fist_db_path}")
        sys.exit(1)

    # Remove existing output to start fresh
    if os.path.isfile(output_db_path):
        os.remove(output_db_path)

    start = time.time()
    print(f"Importing visual similarity data from {fist_db_path}...")

    dst = sqlite3.connect(output_db_path)

    # PRAGMA tuning for import performance
    dst.execute('PRAGMA journal_mode = WAL')
    dst.execute('PRAGMA synchronous = OFF')
    dst.execute('PRAGMA cache_size = -512000')  # 512MB cache
    dst.execute('PRAGMA temp_store = MEMORY')

    # Create schema
    dst.execute('''CREATE TABLE IF NOT EXISTS visual_suggestions (
        alma_id_a INTEGER NOT NULL,
        alma_id_b INTEGER NOT NULL,
        svm_score REAL NOT NULL,
        PRIMARY KEY (alma_id_a, alma_id_b)
    )''')
    dst.execute('''CREATE TABLE IF NOT EXISTS vs_metadata (
        key TEXT PRIMARY KEY,
        value TEXT
    )''')

    # ATTACH FIST.db and use in-SQL mapping -- no Python dict needed
    dst.execute("ATTACH DATABASE ? AS fist", (fist_db_path,))

    print("Building DocumentID -> AlmaId mapping via in-SQL join...")
    # Build mapping view inside SQL
    dst.execute('''
        CREATE TEMP TABLE doc_alma AS
        SELECT d.DocumentId, ia.AlmaId
        FROM fist.Image_ImageDocument d
        JOIN fist.dbo_ImgDigitalImage img ON img.FGPImageNumberId = d.FGPImageNumberIdRecto
        JOIN fist.dbo_InventoryAlma ia ON ia.InventoryId = img.InventoryId
    ''')
    dst.execute('CREATE INDEX idx_da_doc ON doc_alma(DocumentId)')

    doc_count = dst.execute('SELECT COUNT(*) FROM doc_alma').fetchone()[0]
    print(f"  Mapped {doc_count:,} documents to AlmaIds")

    print("Inserting deduplicated visual similarity pairs (MarkCode IS NULL, self-pairs excluded)...")
    # Single INSERT ... SELECT with dedup (MAX score), self-pair exclusion, MarkCode filter
    dst.execute('''
        INSERT OR REPLACE INTO visual_suggestions (alma_id_a, alma_id_b, svm_score)
        SELECT da.AlmaId, db.AlmaId, MAX(bm.SVMMark)
        FROM fist.Image_BestMarkForJoin bm
        JOIN doc_alma da ON da.DocumentId = bm.DocumentID_A
        JOIN doc_alma db ON db.DocumentId = bm.DocumentID_B
        WHERE bm.MarkCode IS NULL
          AND da.AlmaId != db.AlmaId
        GROUP BY da.AlmaId, db.AlmaId
    ''')

    # Drop temp table and commit before detaching to release locks
    dst.execute('DROP TABLE IF EXISTS doc_alma')
    dst.commit()
    dst.execute("DETACH DATABASE fist")

    # Create indexes after bulk insert for better performance
    print("Creating indexes...")
    dst.execute('CREATE INDEX IF NOT EXISTS idx_vs_a ON visual_suggestions(alma_id_a)')
    dst.execute('CREATE INDEX IF NOT EXISTS idx_vs_b ON visual_suggestions(alma_id_b)')

    # Get counts
    pair_count = dst.execute('SELECT COUNT(*) FROM visual_suggestions').fetchone()[0]
    manuscript_count = dst.execute(
        'SELECT COUNT(DISTINCT alma_id_a) FROM visual_suggestions'
    ).fetchone()[0]

    # Insert metadata
    today = date.today().isoformat()
    metadata = [
        ('version', '1.0.0'),
        ('import_date', today),
        ('source', 'FIST.db Image_BestMarkForJoin'),
        ('pair_count', str(pair_count)),
        ('manuscript_count', str(manuscript_count)),
    ]
    dst.executemany(
        'INSERT OR REPLACE INTO vs_metadata (key, value) VALUES (?, ?)',
        metadata
    )
    dst.commit()

    # Reset PRAGMAs for runtime
    dst.execute('PRAGMA synchronous = NORMAL')
    dst.execute('PRAGMA journal_mode = WAL')

    dst.commit()

    print("Running VACUUM...")
    dst.execute('VACUUM')
    dst.close()

    elapsed = time.time() - start
    file_size = os.path.getsize(output_db_path) / (1024 * 1024)

    print(f"\nImport complete:")
    print(f"  Total pairs stored: {pair_count:,}")
    print(f"  Manuscripts with suggestions: {manuscript_count:,}")
    print(f"  Output file: {output_db_path} ({file_size:.1f} MB)")
    print(f"  Elapsed: {elapsed:.1f}s")

    return pair_count, manuscript_count


def main():
    parser = argparse.ArgumentParser(
        description='Import visual similarity data from FIST.db'
    )
    parser.add_argument(
        '--fist-db', default='fist_data/FIST.db',
        help='Path to FIST.db (default: fist_data/FIST.db)'
    )
    parser.add_argument(
        '--output', default='fist_data/visual_similarity.db',
        help='Path to output visual_similarity.db (default: fist_data/visual_similarity.db)'
    )
    args = parser.parse_args()

    # Ensure output directory exists
    os.makedirs(os.path.dirname(args.output) or '.', exist_ok=True)

    import_visual_similarity(args.fist_db, args.output)


if __name__ == '__main__':
    main()
