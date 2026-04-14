#!/usr/bin/env python3
"""
PGP Documents Import Script

Imports PGP (Princeton Geniza Project) documents into Supabase:
- Pass 1: Import documents with metadata and transcriptions
- Pass 2: Create document_fragments links with page_info

Input:
  - pgp_data/transcriptions_linked.csv (transcriptions with sys_id linkage)
  - pgp_data/documents.csv (PGP document metadata)
  - libraries.csv (GenizahSearch shelfmark -> sys_id mapping)

Output:
  - Populated 'documents' table in Supabase
  - Populated 'document_fragments' table in Supabase
  - pgp_data/import_report.csv (detailed issue log)

Usage:
  python scripts/import_pgp_documents.py --dry-run  # Validate and report (default)
  python scripts/import_pgp_documents.py --execute  # Actually import data

Prerequisites:
  1. Run migrations in Supabase SQL Editor:
     - add_pgp_documents_tables.sql
     - add_page_info_column.sql
  2. Set SUPABASE_SERVICE_KEY environment variable
"""

import argparse
import csv
import os
import re
import sys
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Tuple

from dotenv import load_dotenv
load_dotenv()

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

# Import normalize_shelfmark from existing export script
sys.path.insert(0, str(Path(__file__).parent))
from pgp_transcriptions_export import normalize_shelfmark, load_genizahsearch_shelfmarks

# Constants
BATCH_SIZE = 500  # Locked decision from CONTEXT.md


def parse_tags(tags_str: str) -> List[str]:
    """
    Parse comma-separated tags into list.

    Example: "communal, marriage, trade" -> ["communal", "marriage", "trade"]
    """
    if not tags_str or tags_str.strip() == '':
        return []
    return [t.strip() for t in tags_str.split(',') if t.strip()]


def parse_multi_fragment_shelfmark(shelfmark: str, side: str = None) -> List[Dict]:
    """
    Parse a multi-fragment shelfmark into individual fragments.

    Args:
        shelfmark: Combined shelfmark like "T-S 13J35.3 + AIU VII.A.23"
        side: Optional side info like "recto ; verso"

    Returns:
        List of dicts with shelfmark, sequence_order, page_info
    """
    fragments = []
    parts = [p.strip() for p in shelfmark.split(' + ')]

    # Parse side info (split on ' ; ')
    side_parts = []
    if side and side.strip():
        side_parts = [s.strip() for s in side.split(' ; ')]

    for i, part in enumerate(parts):
        fragment = {
            'shelfmark': part,
            'sequence_order': i + 1,
            'page_info': side_parts[i] if i < len(side_parts) else None
        }
        fragments.append(fragment)

    return fragments


def load_transcriptions(transcriptions_path: str) -> Dict[int, Dict]:
    """
    Load transcriptions_linked.csv and create pgpid -> transcription data mapping.

    Returns: Dict mapping pgpid (int) to transcription data
    """
    transcriptions = {}

    with open(transcriptions_path, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)

        for row in reader:
            # Handle BOM in first column if present
            pgpid_str = row.get('\ufeffpgpid') or row.get('pgpid') or row.get('sys_id', '')

            # The actual pgpid column
            if 'pgpid' in row:
                pgpid_str = row['pgpid']
            elif '\ufeffpgpid' in row:
                pgpid_str = row['\ufeffpgpid']

            # Skip if we can't get a pgpid
            if not pgpid_str:
                continue

            try:
                pgpid = int(pgpid_str)
            except ValueError:
                continue

            transcriptions[pgpid] = {
                'sys_id': row.get('sys_id', ''),
                'shelfmark': row.get('shelfmark', ''),
                'content': row.get('content', ''),
                'source_scholar': row.get('source_scholar', ''),
            }

    return transcriptions


def load_documents_metadata(documents_path: str) -> Dict[int, Dict]:
    """
    Load documents.csv and create pgpid -> metadata mapping.

    Returns: Dict mapping pgpid (int) to document metadata
    """
    documents = {}

    with open(documents_path, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)

        for row in reader:
            # Handle BOM in first column
            pgpid_str = row.get('\ufeffpgpid') or row.get('pgpid', '')

            if not pgpid_str:
                continue

            try:
                pgpid = int(pgpid_str)
            except ValueError:
                continue

            documents[pgpid] = {
                'shelfmark': row.get('shelfmark', ''),
                'side': row.get('side', ''),
                'type': row.get('type', ''),
                'tags': row.get('tags', ''),
                'description': row.get('description', ''),
                'doc_date_original': row.get('doc_date_original', ''),
                'doc_date_standard': row.get('doc_date_standard', ''),
                'inferred_date_display': row.get('inferred_date_display', ''),
                'languages_primary': row.get('languages_primary', ''),
                'languages_secondary': row.get('languages_secondary', ''),
                'inferred_date_standard': row.get('inferred_date_standard', ''),
                'inferred_date_rationale': row.get('inferred_date_rationale', ''),
            }

    return documents


def prepare_document_records(
    transcriptions: Dict[int, Dict],
    documents_metadata: Dict[int, Dict]
) -> Tuple[List[Dict], List[Dict]]:
    """
    Prepare document records for import by joining transcription and metadata.

    Returns: (valid_records, issues)
    """
    valid_records = []
    issues = []

    for pgpid, trans_data in transcriptions.items():
        # Get metadata for this document
        meta = documents_metadata.get(pgpid, {})

        # Use shelfmark from transcriptions (it's the matched version)
        # Fall back to metadata shelfmark if needed
        shelfmark = trans_data.get('shelfmark') or meta.get('shelfmark', '')

        # Prepare document record
        doc_record = {
            'pgpid': pgpid,
            'shelfmark_combined': shelfmark,
            'document_type': meta.get('type', ''),
            'tags': parse_tags(meta.get('tags', '')),
            'doc_date_original': meta.get('doc_date_original', '') or None,
            'doc_date_standard': meta.get('doc_date_standard', '') or None,
            'inferred_date_display': meta.get('inferred_date_display', '') or None,
            'description': meta.get('description', '') or None,
            'transcription': trans_data.get('content', '') or None,
            'transcription_source': trans_data.get('source_scholar', '') or None,
            'languages_primary': meta.get('languages_primary', '') or None,
            'languages_secondary': meta.get('languages_secondary', '') or None,
            'inferred_date_standard': meta.get('inferred_date_standard', '') or None,
            'inferred_date_rationale': meta.get('inferred_date_rationale', '') or None,
        }

        valid_records.append(doc_record)

    return valid_records, issues


def prepare_fragment_records(
    transcriptions: Dict[int, Dict],
    documents_metadata: Dict[int, Dict],
    gs_lookup: Dict[str, str]
) -> Tuple[List[Dict], List[Dict]]:
    """
    Prepare document_fragments records by parsing multi-fragment shelfmarks.

    For single-fragment documents: use sys_id from transcriptions_linked.csv
    For multi-fragment documents: look up sys_id for each fragment part

    Returns: (valid_records, issues)
    """
    valid_records = []
    issues = []

    for pgpid, trans_data in transcriptions.items():
        meta = documents_metadata.get(pgpid, {})
        shelfmark = trans_data.get('shelfmark') or meta.get('shelfmark', '')
        side = meta.get('side', '')
        sys_id_from_linked = trans_data.get('sys_id', '')

        if not shelfmark:
            issues.append({
                'pgpid': pgpid,
                'shelfmark': '',
                'issue_type': 'missing_shelfmark',
                'details': 'No shelfmark found for document'
            })
            continue

        # Parse multi-fragment shelfmark
        fragments = parse_multi_fragment_shelfmark(shelfmark, side)

        if len(fragments) == 1:
            # Single fragment document - use sys_id from transcriptions_linked.csv
            if sys_id_from_linked:
                frag = fragments[0]
                valid_records.append({
                    'document_id': pgpid,
                    'sys_id': sys_id_from_linked,
                    'shelfmark': frag['shelfmark'],
                    'sequence_order': 1,
                    'page_info': frag.get('page_info'),
                })
            else:
                issues.append({
                    'pgpid': pgpid,
                    'shelfmark': shelfmark,
                    'issue_type': 'missing_sys_id',
                    'details': 'Single fragment document has no sys_id in transcriptions_linked.csv'
                })
        else:
            # Multi-fragment document - look up sys_id for each part
            for frag in fragments:
                frag_shelfmark = frag['shelfmark']
                normalized = normalize_shelfmark(frag_shelfmark)

                # Look up sys_id
                sys_id = gs_lookup.get(normalized)

                if sys_id:
                    valid_records.append({
                        'document_id': pgpid,
                        'sys_id': sys_id,
                        'shelfmark': frag_shelfmark,
                        'sequence_order': frag['sequence_order'],
                        'page_info': frag.get('page_info'),
                    })
                else:
                    # Try without sub-part (.number at end)
                    base = re.sub(r'\.\d+$', '', normalized)
                    sys_id = gs_lookup.get(base)

                    if sys_id:
                        valid_records.append({
                            'document_id': pgpid,
                            'sys_id': sys_id,
                            'shelfmark': frag_shelfmark,
                            'sequence_order': frag['sequence_order'],
                            'page_info': frag.get('page_info'),
                        })
                    else:
                        issues.append({
                            'pgpid': pgpid,
                            'shelfmark': shelfmark,
                            'issue_type': 'unmatched_fragment',
                            'details': f'Fragment "{frag_shelfmark}" (normalized: "{normalized}") not found in libraries.csv'
                        })

    return valid_records, issues


def upsert_in_batches(
    client: Client,
    table_name: str,
    records: List[Dict],
    on_conflict: str = None,
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
            if on_conflict:
                client.table(table_name).upsert(batch, on_conflict=on_conflict).execute()
            else:
                # For document_fragments, specify the composite unique constraint columns
                client.table(table_name).upsert(batch, on_conflict='document_id,sys_id').execute()

        processed += len(batch)

    return processed


def write_report(issues: List[Dict], report_path: str):
    """Write detailed issue report to CSV."""
    if not issues:
        # Write empty report with just header
        with open(report_path, 'w', encoding='utf-8-sig', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=['pgpid', 'shelfmark', 'issue_type', 'details'])
            writer.writeheader()
        return

    with open(report_path, 'w', encoding='utf-8-sig', newline='') as f:
        fieldnames = ['pgpid', 'shelfmark', 'issue_type', 'details']
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(issues)


def main():
    parser = argparse.ArgumentParser(
        description='Import PGP documents into Supabase',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python scripts/import_pgp_documents.py --dry-run   # Validate data (default)
  python scripts/import_pgp_documents.py --execute   # Actually import

Prerequisites:
  1. Run migrations in Supabase SQL Editor
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
    print("PGP Documents Import")
    print("=" * 60)
    print()
    print(f"Mode: {'DRY RUN (validation only)' if dry_run else 'EXECUTE (writing to database)'}")
    print()

    # Determine paths
    script_dir = Path(__file__).parent
    project_dir = script_dir.parent

    libraries_path = project_dir / 'libraries.csv'
    fist_supplement_path = project_dir / 'pgp_data' / 'fist_shelfmarks_supplement.csv'
    documents_path = project_dir / 'pgp_data' / 'documents.csv'
    transcriptions_path = project_dir / 'pgp_data' / 'transcriptions_linked.csv'
    report_path = project_dir / 'pgp_data' / 'import_report.csv'

    # Verify input files exist
    missing_files = []
    for path, desc in [
        (libraries_path, 'libraries.csv'),
        (documents_path, 'pgp_data/documents.csv'),
        (transcriptions_path, 'pgp_data/transcriptions_linked.csv'),
    ]:
        if not path.exists():
            missing_files.append(desc)

    if missing_files:
        print("ERROR: Missing input files:")
        for f in missing_files:
            print(f"  - {f}")
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
    print("Step 1: Loading data sources...")
    print()

    print("  Loading GenizahSearch shelfmarks from libraries.csv...")
    gs_lookup = load_genizahsearch_shelfmarks(
        str(libraries_path),
        str(fist_supplement_path) if fist_supplement_path.exists() else None
    )
    print(f"    Loaded {len(gs_lookup):,} normalized shelfmarks")

    print("  Loading transcriptions from transcriptions_linked.csv...")
    transcriptions = load_transcriptions(str(transcriptions_path))
    print(f"    Loaded {len(transcriptions):,} transcriptions")

    print("  Loading document metadata from documents.csv...")
    documents_metadata = load_documents_metadata(str(documents_path))
    print(f"    Loaded {len(documents_metadata):,} document records")
    print()

    # Prepare records
    print("Step 2: Preparing records for import...")
    print()

    print("  Preparing document records...")
    doc_records, doc_issues = prepare_document_records(transcriptions, documents_metadata)
    print(f"    Valid documents: {len(doc_records):,}")
    print(f"    Issues: {len(doc_issues):,}")

    print("  Preparing fragment records...")
    frag_records, frag_issues = prepare_fragment_records(transcriptions, documents_metadata, gs_lookup)
    print(f"    Valid fragments: {len(frag_records):,}")
    print(f"    Issues: {len(frag_issues):,}")
    print()

    # Combine issues
    all_issues = doc_issues + frag_issues

    # Statistics
    multi_frag_count = sum(1 for pgpid in transcriptions if ' + ' in (transcriptions[pgpid].get('shelfmark', '') or documents_metadata.get(pgpid, {}).get('shelfmark', '')))
    single_frag_count = len(transcriptions) - multi_frag_count

    print("Statistics:")
    print(f"  Total documents to import: {len(doc_records):,}")
    print(f"    Single-fragment: {single_frag_count:,}")
    print(f"    Multi-fragment: {multi_frag_count:,}")
    print(f"  Total fragment links to create: {len(frag_records):,}")
    print(f"  Total issues found: {len(all_issues):,}")

    # Issue breakdown
    if all_issues:
        issue_types = defaultdict(int)
        for issue in all_issues:
            issue_types[issue['issue_type']] += 1
        print("  Issues by type:")
        for itype, count in sorted(issue_types.items()):
            print(f"    {itype}: {count:,}")
    print()

    # Write report
    print(f"Writing report to {report_path}...")
    write_report(all_issues, str(report_path))
    print(f"  Wrote {len(all_issues):,} issue records")
    print()

    # Import data
    if dry_run:
        print("DRY RUN COMPLETE")
        print()
        print(f"Would import {len(doc_records):,} documents to 'documents' table")
        print(f"Would create {len(frag_records):,} links in 'document_fragments' table")
        print()
        print("To execute import, run with --execute flag")
    else:
        print("Step 3: Importing to Supabase...")
        print()

        # Pass 1: Import documents
        print("Pass 1: Importing documents...")
        docs_processed = upsert_in_batches(
            client, 'documents', doc_records,
            on_conflict='pgpid', dry_run=False
        )
        print(f"  Imported {docs_processed:,} documents")
        print()

        # Pass 2: Create fragment links
        # Deduplicate by (document_id, sys_id) to avoid constraint violations
        seen_keys = set()
        unique_frag_records = []
        for frag in frag_records:
            key = (frag['document_id'], frag['sys_id'])
            if key not in seen_keys:
                seen_keys.add(key)
                unique_frag_records.append(frag)

        print("Pass 2: Creating fragment links...")
        print(f"  (deduplicated: {len(frag_records)} -> {len(unique_frag_records)} unique)")
        frags_processed = upsert_in_batches(
            client, 'document_fragments', unique_frag_records,
            dry_run=False
        )
        print(f"  Created {frags_processed:,} fragment links")
        print()

        print("IMPORT COMPLETE")
        print()
        print(f"  Documents imported: {docs_processed:,}")
        print(f"  Fragment links created: {frags_processed:,}")

    print()
    print(f"Report: {report_path}")

    return 0


if __name__ == '__main__':
    sys.exit(main())
