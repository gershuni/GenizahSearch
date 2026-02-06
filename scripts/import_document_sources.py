#!/usr/bin/env python3
"""
Document Sources Import Script

Imports all transcription and translation sources from transcriptions_linked.csv
into the document_sources table.

Unlike import_pgp_documents.py which imports unique documents, this script
imports ALL source records including:
- Multiple editions per document (different scholars)
- Translations (Hebrew and English)
- Hybrid types (Edition ; Translation)

Input:
  - pgp_data/transcriptions_linked.csv (all 9,364 source records)

Output:
  - Populated 'document_sources' table in Supabase

Usage:
  python scripts/import_document_sources.py --dry-run  # Validate and report (default)
  python scripts/import_document_sources.py --execute  # Actually import data

Prerequisites:
  1. Run migrations in Supabase SQL Editor:
     - create_document_sources_table.sql
  2. Set SUPABASE_SERVICE_KEY environment variable
"""

import argparse
import csv
import os
import sys
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Tuple

try:
    from tqdm import tqdm
except ImportError:
    print("ERROR: tqdm not installed. Run: pip install tqdm")
    sys.exit(1)

try:
    from supabase import create_client, Client
except ImportError:
    print("ERROR: supabase not installed. Run: pip install supabase")
    sys.exit(1)

# Constants
BATCH_SIZE = 500  # Locked decision from prior imports


def detect_translation_language(content: str) -> str:
    """
    Detect if translation is Hebrew or English based on content.

    Check for Hebrew characters (U+0590-U+05FF) in first 500 chars.
    If >10 Hebrew chars found -> "Hebrew"
    Otherwise -> "English"
    """
    if not content:
        return "English"

    hebrew_count = sum(
        1 for c in content[:500]
        if ord(c) >= 0x0590 and ord(c) <= 0x05FF
    )
    return "Hebrew" if hebrew_count > 10 else "English"


def load_transcriptions(transcriptions_path: str) -> List[Dict]:
    """
    Load ALL records from transcriptions_linked.csv.

    Unlike import_pgp_documents.py which deduplicates by pgpid,
    this loads every source record for multi-source storage.

    Returns: List of all source records
    """
    records = []

    with open(transcriptions_path, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)

        for row in reader:
            # Handle BOM in first column if present
            pgpid_str = row.get('\ufeffpgpid') or row.get('pgpid') or ''

            # Skip if no pgpid
            if not pgpid_str:
                continue

            try:
                pgpid = int(pgpid_str)
            except ValueError:
                continue

            records.append({
                'pgpid': pgpid,
                'source_scholar': row.get('source_scholar', ''),
                'doc_relation': row.get('doc_relation', ''),
                'languages': row.get('languages', ''),
                'content': row.get('content', ''),
                'content_length': row.get('content_length', ''),
            })

    return records


def prepare_document_sources(records: List[Dict]) -> Tuple[List[Dict], Dict]:
    """
    Prepare document_sources records from CSV data.

    For each record:
    - For Digital Translation: detect language (Hebrew vs English)
    - For Digital Edition: use 'languages' field from CSV
    - For hybrid types: treat as Edition with original language

    Also computes sequence_order for same (pgpid, doc_relation) pairs.

    Returns: (prepared_records, stats)
    """
    prepared = []
    stats = {
        'total': 0,
        'digital_editions': 0,
        'digital_translations': 0,
        'translation_hebrew': 0,
        'translation_english': 0,
        'hybrid_types': 0,
        'pgpids_with_multiple_sources': 0,
    }

    # Track sequence order within (pgpid, doc_relation) groups
    sequence_tracker = defaultdict(int)

    # Track pgpids to count those with multiple sources
    pgpid_source_count = defaultdict(int)

    for row in records:
        pgpid = row['pgpid']
        source_scholar = row['source_scholar']
        doc_relation = row['doc_relation']
        languages = row['languages']
        content = row['content']
        content_length = row.get('content_length', '')

        # Compute content_length if missing
        try:
            content_len = int(content_length) if content_length else len(content or '')
        except ValueError:
            content_len = len(content or '')

        # Determine language based on doc_relation
        if doc_relation == 'Digital Translation':
            language = detect_translation_language(content)
            stats['digital_translations'] += 1
            if language == 'Hebrew':
                stats['translation_hebrew'] += 1
            else:
                stats['translation_english'] += 1
        elif doc_relation == 'Digital Edition':
            # Use original document language
            language = languages if languages else None
            stats['digital_editions'] += 1
        else:
            # Hybrid types like "Edition ; Translation"
            language = languages if languages else None
            stats['hybrid_types'] += 1

        # Compute sequence order
        key = (pgpid, doc_relation)
        sequence_tracker[key] += 1
        seq_order = sequence_tracker[key]

        # Track source counts per pgpid
        pgpid_source_count[pgpid] += 1

        prepared.append({
            'pgpid': pgpid,
            'source_scholar': source_scholar,
            'doc_relation': doc_relation,
            'language': language,
            'content': content,
            'content_length': content_len,
            'sequence_order': seq_order,
            # notes and source_url left as NULL for now
        })

        stats['total'] += 1

    # Count pgpids with multiple sources
    stats['pgpids_with_multiple_sources'] = sum(
        1 for count in pgpid_source_count.values() if count > 1
    )

    return prepared, stats


def upsert_in_batches(
    client: Client,
    table_name: str,
    records: List[Dict],
    on_conflict: str,
    dry_run: bool = True
) -> int:
    """
    Upsert records in batches with progress bar.

    Returns: Number of records processed
    """
    if not records:
        return 0

    processed = 0

    for i in tqdm(range(0, len(records), BATCH_SIZE), desc=f"Importing {table_name}"):
        batch = records[i:i + BATCH_SIZE]

        if not dry_run:
            client.table(table_name).upsert(batch, on_conflict=on_conflict).execute()

        processed += len(batch)

    return processed


def main():
    parser = argparse.ArgumentParser(
        description='Import document sources into Supabase',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python scripts/import_document_sources.py --dry-run   # Validate data (default)
  python scripts/import_document_sources.py --execute   # Actually import

Prerequisites:
  1. Run migrations/create_document_sources_table.sql in Supabase
  2. Set SUPABASE_SERVICE_KEY environment variable
        """
    )

    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        '--dry-run', action='store_true', default=True,
        help='Validate data and show what would be imported (default)'
    )
    group.add_argument(
        '--execute', action='store_true',
        help='Actually import data to Supabase'
    )

    args = parser.parse_args()

    # If --execute is specified, dry_run is False
    dry_run = not args.execute

    print("=" * 60)
    print("Document Sources Import")
    print("=" * 60)
    print()
    print(f"Mode: {'DRY RUN (validation only)' if dry_run else 'EXECUTE (writing to database)'}")
    print()

    # Determine paths
    script_dir = Path(__file__).parent
    project_dir = script_dir.parent

    transcriptions_path = project_dir / 'pgp_data' / 'transcriptions_linked.csv'

    # Verify input file exists
    if not transcriptions_path.exists():
        print("ERROR: Missing input file:")
        print(f"  - {transcriptions_path}")
        return 1

    # Check environment for execute mode
    if not dry_run:
        supabase_url = os.environ.get('SUPABASE_URL')
        supabase_key = os.environ.get('SUPABASE_SERVICE_KEY')

        if not supabase_url:
            # Use default from web/supabase_client.py
            supabase_url = 'https://ylcpglwxompwjcufdemz.supabase.co'

        if not supabase_key:
            print("ERROR: SUPABASE_SERVICE_KEY environment variable not set.")
            print("       This is required to bypass RLS for bulk insert.")
            print("       Get it from: Supabase Dashboard -> Settings -> API -> service_role secret")
            return 1

        print(f"Supabase URL: {supabase_url}")
        print()

        # Create Supabase client
        client = create_client(supabase_url, supabase_key)
    else:
        client = None

    # Load data
    print("Step 1: Loading source records from transcriptions_linked.csv...")
    print()

    records = load_transcriptions(str(transcriptions_path))
    print(f"  Loaded {len(records):,} source records")
    print()

    # Prepare records
    print("Step 2: Preparing document_sources records...")
    print()

    prepared_records, stats = prepare_document_sources(records)
    print()

    # Statistics
    print("Statistics:")
    print(f"  Total records to import: {stats['total']:,}")
    print()
    print("  By doc_relation:")
    print(f"    Digital Editions: {stats['digital_editions']:,}")
    print(f"    Digital Translations: {stats['digital_translations']:,}")
    if stats['hybrid_types'] > 0:
        print(f"    Hybrid types: {stats['hybrid_types']:,}")
    print()
    print("  Translation language breakdown:")
    print(f"    Hebrew: {stats['translation_hebrew']:,}")
    print(f"    English: {stats['translation_english']:,}")
    print()
    print(f"  Documents with multiple sources: {stats['pgpids_with_multiple_sources']:,}")
    print()

    # Import data
    if dry_run:
        print("DRY RUN COMPLETE")
        print()
        print(f"Would import {len(prepared_records):,} records to 'document_sources' table")
        print()
        print("To execute import, run with --execute flag")
    else:
        print("Step 3: Importing to Supabase...")
        print()

        # Import document sources with composite unique constraint
        processed = upsert_in_batches(
            client, 'document_sources', prepared_records,
            on_conflict='pgpid,source_scholar,doc_relation', dry_run=False
        )
        print()
        print(f"  Imported {processed:,} document sources")
        print()

        print("IMPORT COMPLETE")
        print()
        print(f"  Total records imported: {processed:,}")
        print(f"  Digital Editions: {stats['digital_editions']:,}")
        print(f"  Digital Translations: {stats['digital_translations']:,}")
        print(f"    Hebrew: {stats['translation_hebrew']:,}")
        print(f"    English: {stats['translation_english']:,}")

    print()
    return 0


if __name__ == '__main__':
    sys.exit(main())
