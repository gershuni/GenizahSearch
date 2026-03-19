"""
Generate CSV rows for FIST.db manuscripts missing from libraries.csv.

Reads fist_data/FIST.db (the real 3.2GB database, NOT root FIST.db which is a 0-byte stub)
and produces fist_gap_rows.csv with one row per distinct AlmaId that is not in libraries.csv.

Usage:
    python scripts/generate_fist_gap_csv.py            # Full run: generate + append to libraries.csv
    python scripts/generate_fist_gap_csv.py --dry-run   # Print stats without writing files
    python scripts/generate_fist_gap_csv.py --validate-only  # Validate existing fist_gap_rows.csv
"""

import argparse
import csv
import os
import shutil
import sqlite3
import sys
from collections import defaultdict
from datetime import date

# ── Paths ──────────────────────────────────────────────────────────

FIST_DB = os.path.join("fist_data", "FIST.db")
ENRICHMENT_DB = os.path.join("fist_data", "fjms_enrichment.db")
LIBRARIES_CSV = "libraries.csv"
GAP_CSV = "fist_gap_rows.csv"
MANIFEST_FILE = "fist_gap_manifest.txt"
AMBIGUOUS_TITLES_FILE = "fist_gap_ambiguous_titles.txt"

# ── LibraryId -> library_code mapping ──────────────────────────────
# Built from the actual gap set's distinct LibraryIds.
# Keys are FIST CODE_Library.LibraryId integers.

LIBRARY_ID_MAP = {
    # Major libraries (direct match to existing codes)
    233: 'CUL',          # Cambridge University Library
    79:  'JTS',          # Jewish Theological Seminary of America
    229: 'RNL',          # National Library of Russia (St. Petersburg)
    242: 'Manchester',   # John Rylands University Library
    235: 'Oxford',       # Bodleian Library
    238: 'BL',           # British Library
    177: 'Mosseri',      # Mosseri Collection
    169: 'AIU',          # Alliance Israelite Universelle
    248: 'Westminster',  # Lewis-Gibson (formerly Westminster College)
    147: 'HAS',          # Library of the Hungarian Academy of Sciences
    90:  'Katz',         # Philadelphia CAJS (now Katz Center)
    86:  'HUC',          # Hebrew Union College
    130: 'Senckenberg',  # Frankfurt (Senckenberg Library)
    255: 'Vienna',       # Austrian National Library
    293: 'Geneva',       # Public and University Library of Geneva
    291: 'Sofer',        # D. Sofer Collection
    231: 'RSL',          # Russian State Library
    81:  'Columbia',     # Columbia University
    285: 'Toronto',      # University of Toronto
    170: 'BnF',          # Bibliotheque Nationale de France
    122: 'Munich',       # Bavarian State Library
    240: 'Sassoon',      # Sassoon Collection
    299: 'Schoeyen',     # Martin Schoyen Collection
    210: 'Schocken',     # Schocken Institute
    196: 'Haifa',        # University of Haifa
    400: 'TAU',          # Tel Aviv University
    211: 'NLI',          # National Library of Israel
    168: 'Strasbourg',   # National and University Library (Strasbourg)
    405: 'Freer',        # Smithsonian Freer Gallery
    412: 'InstFrance',   # l'Institut de France
    408: 'IOM',          # Institute of Oriental Manuscripts (St. Petersburg)
    511: 'Duke',         # Duke University (Ashkar collection)
    504: 'Combs',        # Combs Collection
    517: 'Bisno',        # Bisno Collection
    508: 'Lehnardt',     # Lehnardt Collection
    501: 'Lehmann',      # Lehmann Foundation
    404: 'UMich',        # University of Michigan
    402: 'McGill',       # McGill University
    403: 'TCD',          # Trinity College Dublin
    224: 'BarIlan',      # Bar-Ilan University
    502: 'YU',           # Yeshiva University
    92:  'Harvard',      # Harvard University
    300: 'Weiss',        # Steve Weiss Collection
    195: 'Nahum',        # Y. L. Nahum Collection
    209: 'BenZvi',       # Ben Zvi Institute
    503: 'Goldsmith',    # Goldsmith Museum
    516: 'JCErfurt',     # Jewish Community of Erfurt
    103: 'SBB',          # State Library (Berlin)

    # Libraries mapped to closest existing code
    236: 'Birmingham',   # Orchard Learning = Birmingham (Mingana collection)
    292: 'Leeds',        # Brotherton Library / Roth collection = Leeds
    510: 'JCBerlin',    # Jewish Community (Berlin)
    515: 'Warsaw',       # Jewish Community (Warsaw)
    91:  'UPenn',        # Penn University Museum
    407: 'Heidelberg',   # University Institute of Papyrology (Heidelberg)
    518: 'UChicago',     # Oriental Institute Museum (University of Chicago)
    13:  'Turin',        # Biblioteca Nazionale Universitaria (Turin)
    411: 'Chetham',      # Chetham Library (Manchester)
    413: 'Heidelberg',   # Heidelberg University (same code as 407)
    230: 'Harkavy',      # Vernadsky National Library (Harkavi shelfmarks)
    406: 'Princeton',    # Princeton Theological Seminary
    183: 'Turin',        # Italian university (1 record, best match)

    # New codes (libraries not previously in LIBRARY_CODES)
    58:  'Vatican',       # Biblioteca Apostolica (Vatican)
    140: 'Copenhagen',    # Royal Library of Copenhagen
    205: 'Mehlman',       # Mehlman Collection
    401: 'CentralArch',  # Central Archives for the History of the Jewish People
    409: 'JCMainz',      # Jewish Community of Mainz
    410: 'Solomon',       # Solomon Halberstam Collection
    507: 'Chapira',       # Bernard Chapira Collection
    513: 'Reinach',       # Reinach Collection
    514: 'Corwin',        # Temple Israel of Hollywood (Corwin Library)
}

# Cross-library collision overrides: AlmaIds that appear under multiple LibraryIds.
# Manually resolved to the preferred library/shelfmark.
CROSS_LIBRARY_OVERRIDES = {
    '990036375160205171': ('Halper 338', 'Katz'),       # CAJS Halper vs Princeton "No ShelfMark"
    '990001963280205171': ('AS 12', 'HAS'),             # Budapest MTA vs NLI
    '990053938520205171': ('T13', 'HAS'),               # Budapest MTA vs NLI
}


def load_existing_alma_ids():
    """Load existing AlmaIds from libraries.csv (digits only)."""
    existing = set()
    with open(LIBRARIES_CSV, 'r', encoding='utf-8', errors='replace') as f:
        reader = csv.reader(f)
        next(reader, None)  # skip header
        for row in reader:
            if row:
                sys_id = ''.join(ch for ch in str(row[0]) if ch.isdigit())
                if sys_id:
                    existing.add(sys_id)
    return existing


def query_gap_records(existing_alma_ids):
    """Query FIST.db for gap records (AlmaIds not in libraries.csv)."""
    conn = sqlite3.connect(FIST_DB)
    c = conn.cursor()

    c.execute('''
        SELECT CAST(ia.AlmaId AS TEXT) AS alma_id,
               i.Shelfmark,
               c.LibraryId,
               i.InventoryId
        FROM dbo_Inventory i
        JOIN dbo_InventoryAlma ia ON ia.InventoryId = i.InventoryId
        JOIN CODE_Collection c ON i.CollectionId = c.CollectionId
        WHERE i.RecordStatus = 0
          AND ia.AlmaId IS NOT NULL AND ia.AlmaId != ''
    ''')

    # Group by AlmaId
    alma_records = defaultdict(list)
    for alma_id, shelfmark, lib_id, inv_id in c.fetchall():
        if alma_id not in existing_alma_ids:
            alma_records[alma_id].append({
                'shelfmark': shelfmark,
                'library_id': lib_id,
                'inventory_id': inv_id,
            })

    conn.close()
    return alma_records


def query_titles(gap_alma_ids):
    """Query fjms_enrichment.db for titles of gap records."""
    conn = sqlite3.connect(ENRICHMENT_DB)
    c = conn.cursor()

    # Build title map: AlmaId -> set of distinct GenizahTitleOrgTitle values
    title_map = defaultdict(set)
    batch_size = 500

    alma_list = list(gap_alma_ids)
    for i in range(0, len(alma_list), batch_size):
        batch = alma_list[i:i + batch_size]
        placeholders = ','.join('?' * len(batch))
        c.execute(f'''
            SELECT AlmaId, GenizahTitleOrgTitle
            FROM catalog
            WHERE GenizahTitleOrgTitle IS NOT NULL AND GenizahTitleOrgTitle != ''
              AND AlmaId IN ({placeholders})
        ''', batch)
        for alma_id, title in c.fetchall():
            title_map[str(alma_id)].add(title.strip())

    conn.close()
    return title_map


def deduplicate_records(alma_records):
    """
    Deduplicate gap records: one row per AlmaId.

    Rules:
    - Cross-library collisions: use hardcoded override table
    - Same library, multiple rows: pick shortest non-empty shelfmark,
      collapse all variants into pipe-separated call_numbers
    """
    deduped = {}
    cross_lib_log = []
    multi_row_count = 0

    for alma_id, records in alma_records.items():
        # Check cross-library override
        if alma_id in CROSS_LIBRARY_OVERRIDES:
            shelfmark, lib_code = CROSS_LIBRARY_OVERRIDES[alma_id]
            deduped[alma_id] = {
                'shelfmark': shelfmark,
                'call_numbers': shelfmark,
                'library_code': lib_code,
            }
            cross_lib_log.append(f"  {alma_id}: override -> {lib_code} / {shelfmark}")
            continue

        if len(records) > 1:
            multi_row_count += 1

            # Check for cross-library collision (not in override table)
            lib_ids = set(r['library_id'] for r in records)
            if len(lib_ids) > 1:
                cross_lib_log.append(
                    f"  {alma_id}: UNRESOLVED cross-library collision: "
                    f"{[(r['shelfmark'], r['library_id']) for r in records]}"
                )
                # Fall through to pick first library's records

        # All records should be same library (or we pick the first library)
        lib_id = records[0]['library_id']
        lib_code = LIBRARY_ID_MAP.get(lib_id)
        if lib_code is None:
            continue  # skip unmapped libraries

        # Collect distinct non-empty shelfmarks
        shelfmarks = []
        seen = set()
        for r in records:
            sm = r['shelfmark'].strip() if r['shelfmark'] else ''
            if sm and sm not in seen and 'Undefined' not in sm:
                shelfmarks.append(sm)
                seen.add(sm)

        if not shelfmarks:
            continue  # all shelfmarks are empty or undefined

        # Pick shortest as primary, collapse all as call_numbers
        shortest = min(shelfmarks, key=len)
        call_numbers = ' | '.join(shelfmarks) if len(shelfmarks) > 1 else shortest

        deduped[alma_id] = {
            'shelfmark': shortest,
            'call_numbers': call_numbers,
            'library_code': lib_code,
        }

    return deduped, multi_row_count, cross_lib_log


def generate_gap_csv(deduped, title_map, dry_run=False):
    """Generate fist_gap_rows.csv with validated gap records."""
    rows = []
    skipped_undefined = 0
    skipped_unmapped = 0
    ambiguous_titles = []

    lib_dist = defaultdict(int)
    titled_count = 0

    for alma_id, data in sorted(deduped.items()):
        lib_code = data['library_code']
        call_numbers = data['call_numbers']

        # Skip undefined shelfmarks (safety check)
        if 'Undefined' in call_numbers:
            skipped_undefined += 1
            continue

        # Title resolution
        title = ''
        if alma_id in title_map:
            distinct_titles = title_map[alma_id]
            if len(distinct_titles) == 1:
                title = list(distinct_titles)[0]
                titled_count += 1
            else:
                # Ambiguous: multiple distinct titles for this AlmaId
                ambiguous_titles.append(
                    f"{alma_id}: {len(distinct_titles)} titles: "
                    f"{' | '.join(sorted(distinct_titles))}"
                )

        # Build 8-column row: system_number, oxford_part_id, call_numbers, library_code, ,, , titles
        row = [alma_id, '', call_numbers, lib_code, '', '', '', title]
        rows.append(row)
        lib_dist[lib_code] += 1

    return rows, lib_dist, titled_count, ambiguous_titles, skipped_undefined, skipped_unmapped


def validate_gap_csv(rows, valid_codes=None):
    """Validate gap CSV rows."""
    errors = []
    alma_ids_seen = set()

    for i, row in enumerate(rows):
        if len(row) != 8:
            errors.append(f"Row {i}: expected 8 columns, got {len(row)}")

        alma_id = row[0]
        if alma_id in alma_ids_seen:
            errors.append(f"Row {i}: duplicate AlmaId {alma_id}")
        alma_ids_seen.add(alma_id)

        if 'Undefined Shelfmark' in (row[2] or ''):
            errors.append(f"Row {i}: contains 'Undefined Shelfmark'")

        if valid_codes and row[3] not in valid_codes:
            errors.append(f"Row {i}: unknown library_code '{row[3]}'")

    return errors


def validate_existing_file():
    """Validate an existing fist_gap_rows.csv file."""
    if not os.path.exists(GAP_CSV):
        print(f"ERROR: {GAP_CSV} not found")
        return False

    rows = []
    with open(GAP_CSV, 'r', encoding='utf-8', errors='replace') as f:
        reader = csv.reader(f)
        header = next(reader, None)
        for row in reader:
            rows.append(row)

    errors = validate_gap_csv(rows)
    if errors:
        print(f"VALIDATION FAILED: {len(errors)} errors")
        for e in errors[:20]:
            print(f"  {e}")
        return False

    print(f"VALIDATION PASSED: {len(rows)} rows, 0 errors")
    return True


def main():
    parser = argparse.ArgumentParser(description="Generate FIST gap CSV for libraries.csv")
    parser.add_argument('--dry-run', action='store_true', help='Print stats without writing files')
    parser.add_argument('--validate-only', action='store_true', help='Validate existing fist_gap_rows.csv')
    args = parser.parse_args()

    if args.validate_only:
        ok = validate_existing_file()
        sys.exit(0 if ok else 1)

    # Verify FIST.db exists and is not the 0-byte stub
    if not os.path.exists(FIST_DB):
        print(f"ERROR: {FIST_DB} not found. Expected the real FIST.db in fist_data/")
        sys.exit(1)
    fist_size = os.path.getsize(FIST_DB)
    if fist_size < 1_000_000:
        print(f"ERROR: {FIST_DB} is only {fist_size} bytes. Expected 3.2GB database.")
        sys.exit(1)

    # Verify enrichment DB exists
    if not os.path.exists(ENRICHMENT_DB):
        print(f"ERROR: {ENRICHMENT_DB} not found")
        sys.exit(1)

    # Always create backup of libraries.csv (even in dry-run, for safety)
    if os.path.exists(LIBRARIES_CSV):
        backup = f"libraries_backup_{date.today().isoformat()}.csv"
        if not os.path.exists(backup):
            shutil.copy2(LIBRARIES_CSV, backup)
            print(f"Backed up {LIBRARIES_CSV} -> {backup}")
        else:
            print(f"Backup already exists: {backup}")

    print(f"\n=== Loading existing AlmaIds from {LIBRARIES_CSV} ===")
    existing = load_existing_alma_ids()
    print(f"Existing records: {len(existing):,}")

    print(f"\n=== Querying gap records from {FIST_DB} ===")
    alma_records = query_gap_records(existing)
    print(f"Raw gap AlmaIds: {len(alma_records):,}")

    # Assert all LibraryIds in gap are mapped
    unmapped_libs = set()
    for alma_id, records in alma_records.items():
        for r in records:
            if r['library_id'] not in LIBRARY_ID_MAP:
                unmapped_libs.add(r['library_id'])
    if unmapped_libs:
        print(f"ERROR: Unmapped LibraryIds found: {unmapped_libs}")
        print("Add these to LIBRARY_ID_MAP before proceeding.")
        sys.exit(1)

    print(f"\n=== Deduplicating records ===")
    deduped, multi_count, cross_lib_log = deduplicate_records(alma_records)
    print(f"Deduped records: {len(deduped):,}")
    print(f"AlmaIds with multiple inventory rows: {multi_count}")
    if cross_lib_log:
        print("Cross-library collisions:")
        for line in cross_lib_log:
            print(line)

    print(f"\n=== Querying titles from {ENRICHMENT_DB} ===")
    title_map = query_titles(set(deduped.keys()))
    print(f"AlmaIds with title data: {len(title_map):,}")

    print(f"\n=== Generating gap CSV ===")
    rows, lib_dist, titled_count, ambiguous_titles, skip_undef, skip_unmap = \
        generate_gap_csv(deduped, title_map, dry_run=args.dry_run)

    # Validate
    errors = validate_gap_csv(rows)
    if errors:
        print(f"\nVALIDATION FAILED: {len(errors)} errors")
        for e in errors[:20]:
            print(f"  {e}")
        sys.exit(1)
    print(f"Validation: PASSED ({len(rows):,} rows, 0 errors)")

    # Summary
    print(f"\n{'='*60}")
    print(f"SUMMARY")
    print(f"{'='*60}")
    print(f"Total gap AlmaIds (raw):        {len(alma_records):>8,}")
    print(f"After dedup:                     {len(deduped):>8,}")
    print(f"Final rows:                      {len(rows):>8,}")
    print(f"  With titles:                   {titled_count:>8,}")
    print(f"  Without titles:                {len(rows) - titled_count:>8,}")
    print(f"Skipped (undefined shelfmark):   {skip_undef:>8,}")
    print(f"Skipped (unmapped library):      {skip_unmap:>8,}")
    print(f"Multi-row AlmaIds (deduped):     {multi_count:>8,}")
    print(f"Ambiguous titles (skipped):      {len(ambiguous_titles):>8,}")
    print(f"\nLibrary distribution:")
    for code, count in sorted(lib_dist.items(), key=lambda x: -x[1]):
        print(f"  {code:20s} {count:>6,}")

    if args.dry_run:
        print(f"\n[DRY RUN] No files written.")
        return

    # Write gap CSV
    print(f"\n=== Writing {GAP_CSV} ===")
    with open(GAP_CSV, 'w', encoding='utf-8', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['system_number', 'oxford_part_id', 'call_numbers',
                         'library_code', '', '', '', 'titles_non_placeholder'])
        writer.writerows(rows)
    print(f"Wrote {len(rows):,} rows to {GAP_CSV}")

    # Write manifest
    print(f"\n=== Writing {MANIFEST_FILE} ===")
    with open(MANIFEST_FILE, 'w', encoding='utf-8') as f:
        for row in rows:
            f.write(row[0] + '\n')
    print(f"Wrote {len(rows):,} AlmaIds to {MANIFEST_FILE}")

    # Write ambiguous titles log
    if ambiguous_titles:
        print(f"\n=== Writing {AMBIGUOUS_TITLES_FILE} ===")
        with open(AMBIGUOUS_TITLES_FILE, 'w', encoding='utf-8') as f:
            f.write(f"# Ambiguous titles: AlmaIds with multiple distinct GenizahTitleOrgTitle values\n")
            f.write(f"# Total: {len(ambiguous_titles)}\n\n")
            for line in ambiguous_titles:
                f.write(line + '\n')
        print(f"Wrote {len(ambiguous_titles)} ambiguous title entries")

    # Append to libraries.csv
    print(f"\n=== Appending {len(rows):,} rows to {LIBRARIES_CSV} ===")
    with open(LIBRARIES_CSV, 'a', encoding='utf-8', newline='') as f:
        writer = csv.writer(f)
        writer.writerows(rows)
    print(f"libraries.csv now has {len(existing) + len(rows) + 1:,} lines (including header)")

    print(f"\nDone.")


if __name__ == '__main__':
    main()
