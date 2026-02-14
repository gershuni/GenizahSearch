#!/usr/bin/env python3
"""
Export shelfmarks from FIST database for items not in libraries.csv.

This script extracts shelfmark → AlmaId mappings from FIST for:
- Russian National Library (RNL)
- British Library (BL)
- Alliance Israelite Universelle (AIU)
- Penn/Halper (CAJS)
- NLI, HUC
- T-S and CUL items missing from libraries.csv

Output: fist_shelfmarks_supplement.csv
"""

import sqlite3
import csv
from pathlib import Path


def load_libraries_shelfmarks(libraries_path):
    """Load all shelfmarks from libraries.csv for deduplication."""
    shelfs = set()
    with open(libraries_path, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        next(reader)  # Skip header
        for row in reader:
            if len(row) >= 3:
                for v in row[2].split('|'):
                    shelfs.add(v.strip().lower())
    return shelfs


def export_fist_shelfmarks():
    script_dir = Path(__file__).parent
    project_dir = script_dir.parent

    fist_db = project_dir / 'FIST_DB_BACKUP' / 'FIST.db'
    libraries_csv = project_dir / 'libraries.csv'
    output_file = project_dir / 'pgp_data' / 'fist_shelfmarks_supplement.csv'

    # Load existing shelfmarks for deduplication
    print("Loading libraries.csv shelfmarks...")
    existing_shelfs = load_libraries_shelfmarks(libraries_csv)
    print(f"  Loaded {len(existing_shelfs)} existing shelfmarks")

    conn = sqlite3.connect(str(fist_db))
    cursor = conn.cursor()

    # Libraries to export (full export)
    # From CODE_Library: 229=RNL, 238=BL, 169=AIU, 90=Penn CAJS, 211=NLI, 86=HUC
    # Additional: 79=JTS, 235=Bodleian, 242=JRL/Manchester, 177=Mosseri, 248=Lewis-Gibson
    # 255=Vienna (Austrian National Library), 405=Freer Gallery
    libraries = {
        229: 'RNL',
        238: 'BL',
        169: 'AIU',
        90: 'CAJS',
        211: 'NLI',
        86: 'HUC',
        79: 'JTS',
        235: 'Bodleian',
        242: 'Manchester',
        177: 'Mosseri',
        248: 'Lewis-Gibson',
        255: 'Vienna',
        405: 'Freer',
        413: 'Heidelberg',
        240: 'Sassoon',
        407: 'Heidelberg-Papyrology',
        91: 'Penn-Museum',
        408: 'IOM',  # Institute of Oriental Manuscripts, St Petersburg
    }

    print("\nExporting FIST shelfmarks...")

    records = []

    # Export full libraries
    for lib_id, lib_code in libraries.items():
        cursor.execute('''
            SELECT DISTINCT
                i.Shelfmark,
                ia.AlmaId,
                c.CollectionName,
                l.LibraryNameEng
            FROM dbo_Inventory i
            JOIN CODE_Collection c ON i.CollectionId = c.CollectionId
            JOIN CODE_Library l ON c.LibraryId = l.LibraryId
            LEFT JOIN dbo_InventoryAlma ia ON i.InventoryId = ia.InventoryId
            WHERE c.LibraryId = ?
            AND ia.AlmaId IS NOT NULL
        ''', (lib_id,))

        count = 0
        for row in cursor.fetchall():
            records.append({
                'shelfmark': row[0],
                'alma_id': row[1],
                'collection': row[2],
                'library': row[3],
                'library_code': lib_code,
            })
            count += 1

        print(f"  {lib_code}: {count} records")

    # Export CUL items (T-S, Or., Add., etc.) that are missing from libraries.csv
    print("\nExporting CUL items missing from libraries.csv...")
    cursor.execute('''
        SELECT DISTINCT
            i.Shelfmark,
            ia.AlmaId,
            c.CollectionName,
            l.LibraryNameEng
        FROM dbo_Inventory i
        JOIN CODE_Collection c ON i.CollectionId = c.CollectionId
        JOIN CODE_Library l ON c.LibraryId = l.LibraryId
        LEFT JOIN dbo_InventoryAlma ia ON i.InventoryId = ia.InventoryId
        WHERE c.LibraryId = 233
        AND ia.AlmaId IS NOT NULL
    ''')

    cul_count = 0
    cul_added = 0
    for row in cursor.fetchall():
        cul_count += 1
        shelf_lower = row[0].lower() if row[0] else ''
        # Only add if not already in libraries.csv
        if shelf_lower and shelf_lower not in existing_shelfs:
            records.append({
                'shelfmark': row[0],
                'alma_id': row[1],
                'collection': row[2],
                'library': row[3],
                'library_code': 'CUL',
            })
            cul_added += 1

    print(f"  CUL: {cul_count} total, {cul_added} added (missing from libraries.csv)")

    conn.close()

    # Write output
    print(f"\nWriting {output_file}...")
    with open(output_file, 'w', encoding='utf-8-sig', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['shelfmark', 'alma_id', 'collection', 'library', 'library_code'])
        writer.writeheader()
        writer.writerows(records)

    print(f"Exported {len(records)} total records")

    return len(records)


if __name__ == '__main__':
    export_fist_shelfmarks()
