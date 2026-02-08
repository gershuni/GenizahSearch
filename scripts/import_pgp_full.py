#!/usr/bin/env python3
"""
Full PGP Data Import Script

Comprehensive import pipeline for the complete PGP dataset (~36K documents,
~36K fragments, ~24K footnotes, ~9K document sources). Replaces the two
v1 scripts (import_pgp_documents.py, import_document_sources.py) with a
single multi-pass pipeline.

Input files (in pgp_data/):
  - documents.csv       -- 35,839 PGP document records with full metadata
  - fragments.csv       -- 36,162 fragment records with collection/library metadata
  - footnotes.csv       -- 24,388 scholarship/footnotes records
  - transcriptions_linked.csv -- 9,364 transcriptions with sys_id links

Also requires:
  - libraries.csv       -- GenizahSearch shelfmark -> sys_id mapping
  - pgp_data/fist_shelfmarks_supplement.csv -- FIST supplement for matching

Output:
  - Populated 'documents' table (upsert, on_conflict=pgpid)
  - Populated 'document_sources' table (upsert, on_conflict=pgpid,source_scholar,doc_relation)
  - Populated 'document_footnotes' table (upsert, on_conflict=pgpid,source_slug,doc_relation)
  - Populated 'document_fragments' table (upsert, on_conflict=document_id,sys_id)
  - pgp_data/full_import_report.txt (verification report)

Usage:
  python scripts/import_pgp_full.py --dry-run   # Validate and report (default)
  python scripts/import_pgp_full.py --execute    # Actually import data

Prerequisites:
  1. Run migrations in Supabase SQL Editor:
     - add_pgp_documents_tables.sql
     - add_pgp_metadata_columns.sql
     - add_page_info_column.sql
     - create_document_sources_table.sql
     - add_full_pgp_columns.sql
     - create_footnotes_table.sql
  2. Set SUPABASE_SERVICE_KEY environment variable (for --execute)
"""

import argparse
import csv
import os
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

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
BATCH_SIZE = 500  # Proven in v1 imports


# ============================================
# DATA LOADING FUNCTIONS
# ============================================

def parse_tags(tags_str: str) -> List[str]:
    """
    Parse comma-separated tags into list.

    Example: "communal, marriage, trade" -> ["communal", "marriage", "trade"]
    """
    if not tags_str or tags_str.strip() == '':
        return []
    return [t.strip() for t in tags_str.split(',') if t.strip()]


def load_documents_full(documents_path: str) -> Dict[int, Dict]:
    """
    Load ALL columns from documents.csv.

    Returns: Dict mapping pgpid (int) to full document record
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
                'pgpid': pgpid,
                'shelfmark_combined': row.get('shelfmark', ''),
                'document_type': row.get('type', ''),
                'tags': parse_tags(row.get('tags', '')),
                'description': row.get('description', '') or None,
                'doc_date_original': row.get('doc_date_original', '') or None,
                'doc_date_standard': row.get('doc_date_standard', '') or None,
                'doc_date_calendar': row.get('doc_date_calendar', '') or None,
                'inferred_date_display': row.get('inferred_date_display', '') or None,
                'inferred_date_standard': row.get('inferred_date_standard', '') or None,
                'inferred_date_rationale': row.get('inferred_date_rationale', '') or None,
                'inferred_date_notes': row.get('inferred_date_notes', '') or None,
                'languages_primary': row.get('languages_primary', '') or None,
                'languages_secondary': row.get('languages_secondary', '') or None,
                'language_note': row.get('language_note', '') or None,
                'scholarship_records': row.get('scholarship_records', '') or None,
                'shelfmarks_historic': row.get('shelfmarks_historic', '') or None,
                'has_transcription': row.get('has_transcription', '') == 'Y',
                'has_translation': row.get('has_translation', '') == 'Y',
                'input_by': row.get('input_by', '') or None,
                # Keep raw fields for fragment processing
                '_side': row.get('side', ''),
                '_shelfmark_raw': row.get('shelfmark', ''),
            }

    return documents


def load_fragment_metadata(fragments_path: str) -> Dict[str, Dict]:
    """
    Load fragments.csv into shelfmark -> metadata lookup.

    Each row represents a unique physical fragment with its collection/library
    metadata and pgpid references (semicolon-separated for multi-document fragments).

    Returns: Dict mapping shelfmark to fragment metadata
    """
    fragments = {}

    with open(fragments_path, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)

        for row in reader:
            # Handle BOM in first column
            shelfmark = row.get('\ufeffshelfmark') or row.get('shelfmark', '')

            if not shelfmark:
                continue

            fragments[shelfmark] = {
                'pgpids': row.get('pgpids', ''),
                'collection': row.get('collection', '') or None,
                'library': row.get('library', '') or None,
                'library_abbrev': row.get('library_abbrev', '') or None,
                'url': row.get('url', '') or None,
                'iiif_url': row.get('iiif_url', '') or None,
            }

    return fragments


def load_footnotes(footnotes_path: str) -> List[Dict]:
    """
    Load footnotes.csv into list of footnote records.

    Each row has: document, document_id, source, source_slug, location,
    doc_relation, emendations, notes, url, content.

    Returns: List of footnote dicts (multiple per pgpid)
    """
    footnotes = []

    with open(footnotes_path, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)

        for row in reader:
            # Handle BOM in first column - document column is the URL, document_id is pgpid
            doc_id_str = row.get('\ufeffdocument_id') or row.get('document_id', '')

            # If document_id is empty, try extracting from 'document' URL column
            if not doc_id_str:
                doc_url = row.get('\ufeffdocument') or row.get('document', '')
                # URL format: https://geniza.princeton.edu/documents/1234/
                if doc_url:
                    parts = doc_url.rstrip('/').split('/')
                    doc_id_str = parts[-1] if parts else ''

            if not doc_id_str:
                continue

            try:
                pgpid = int(doc_id_str)
            except ValueError:
                continue

            content = row.get('content', '') or None
            content_length = len(content) if content else None

            # Combine emendations and notes into notes field
            emendations = row.get('emendations', '') or ''
            notes_raw = row.get('notes', '') or ''
            notes_combined = ''
            if emendations and notes_raw:
                notes_combined = f"{emendations}\n{notes_raw}"
            elif emendations:
                notes_combined = emendations
            elif notes_raw:
                notes_combined = notes_raw

            footnotes.append({
                'pgpid': pgpid,
                'source': row.get('source', ''),
                'source_slug': row.get('source_slug', '') or None,
                'doc_relation': row.get('doc_relation', '') or '',
                'location': row.get('location', '') or None,
                'url': row.get('url', '') or None,
                'notes': notes_combined or None,
                'content': content,
                'content_length': content_length,
            })

    return footnotes


def load_transcriptions(transcriptions_path: str) -> List[Dict]:
    """
    Load ALL records from transcriptions_linked.csv.

    Each row is a source record (Digital Edition or Digital Translation)
    with pgpid, source_scholar, doc_relation, languages, content, content_length.

    Returns: List of all source records
    """
    records = []

    with open(transcriptions_path, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)

        for row in reader:
            # Handle BOM in first column
            pgpid_str = row.get('\ufeffpgpid') or row.get('pgpid') or ''

            # The actual column might be sys_id first, pgpid second
            if not pgpid_str:
                pgpid_str = row.get('sys_id', '')

            # In transcriptions_linked.csv, pgpid is the second column
            if 'pgpid' in row:
                pgpid_str = row['pgpid']
            elif '\ufeffpgpid' in row:
                pgpid_str = row['\ufeffpgpid']

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


# ============================================
# RECORD PREPARATION FUNCTIONS
# ============================================

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
        if 0x0590 <= ord(c) <= 0x05FF
    )
    return "Hebrew" if hebrew_count > 10 else "English"


def prepare_document_records(
    documents: Dict[int, Dict],
    transcription_lookup: Dict[int, Dict]
) -> Tuple[List[Dict], List[Dict]]:
    """
    Prepare document records for upsert from full documents.csv data.

    For documents that have transcription content available in
    transcriptions_linked.csv, merges transcription text into the record.
    For documents WITHOUT transcription content, omits transcription/
    transcription_source keys so upsert does not null out existing data.

    Args:
        documents: Dict from load_documents_full()
        transcription_lookup: Dict[pgpid -> {content, source_scholar}] from
                              transcriptions_linked.csv (first Digital Edition per pgpid)

    Returns: (valid_records, issues)
    """
    valid_records = []
    issues = []
    with_transcription = 0
    without_transcription = 0

    for pgpid, doc in documents.items():
        # Build base record with all metadata columns
        doc_record = {
            'pgpid': pgpid,
            'shelfmark_combined': doc['shelfmark_combined'],
            'document_type': doc['document_type'],
            'tags': doc['tags'],
            'description': doc['description'],
            'doc_date_original': doc['doc_date_original'],
            'doc_date_standard': doc['doc_date_standard'],
            'doc_date_calendar': doc['doc_date_calendar'],
            'inferred_date_display': doc['inferred_date_display'],
            'inferred_date_standard': doc['inferred_date_standard'],
            'inferred_date_rationale': doc['inferred_date_rationale'],
            'inferred_date_notes': doc['inferred_date_notes'],
            'languages_primary': doc['languages_primary'],
            'languages_secondary': doc['languages_secondary'],
            'language_note': doc['language_note'],
            'scholarship_records': doc['scholarship_records'],
            'shelfmarks_historic': doc['shelfmarks_historic'],
            'has_transcription': doc['has_transcription'],
            'has_translation': doc['has_translation'],
            'input_by': doc['input_by'],
        }

        # Merge transcription text if available
        trans = transcription_lookup.get(pgpid)
        if trans and trans.get('content'):
            doc_record['transcription'] = trans['content']
            doc_record['transcription_source'] = trans['source_scholar']
            with_transcription += 1
        else:
            # Do NOT set transcription/transcription_source keys
            # so upsert preserves any existing data
            without_transcription += 1

        valid_records.append(doc_record)

    print(f"    Documents with transcription text: {with_transcription:,}")
    print(f"    Documents without transcription text: {without_transcription:,}")

    return valid_records, issues


def prepare_source_records(
    transcription_records: List[Dict]
) -> Tuple[List[Dict], Dict]:
    """
    Prepare document_sources records from transcriptions_linked.csv data.

    Reuses the exact pattern from import_document_sources.py:
    - Detects translation language (Hebrew vs English)
    - Computes sequence_order within (pgpid, doc_relation) groups
    - Maps fields to document_sources schema

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
    pgpid_source_count = defaultdict(int)

    for row in transcription_records:
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
            language = languages if languages else None
            stats['digital_editions'] += 1
        else:
            language = languages if languages else None
            stats['hybrid_types'] += 1

        # Compute sequence order
        key = (pgpid, doc_relation)
        sequence_tracker[key] += 1
        seq_order = sequence_tracker[key]

        pgpid_source_count[pgpid] += 1

        prepared.append({
            'pgpid': pgpid,
            'source_scholar': source_scholar,
            'doc_relation': doc_relation,
            'language': language,
            'content': content,
            'content_length': content_len,
            'sequence_order': seq_order,
        })

        stats['total'] += 1

    stats['pgpids_with_multiple_sources'] = sum(
        1 for count in pgpid_source_count.values() if count > 1
    )

    return prepared, stats


def prepare_footnote_records(
    footnotes: List[Dict],
    valid_pgpids: Optional[set] = None
) -> Tuple[List[Dict], List[Dict]]:
    """
    Prepare document_footnotes records for upsert.

    Deduplicates by (pgpid, source_slug, doc_relation) composite key.
    Filters out footnotes referencing pgpids not in documents table (FK safety).

    Returns: (valid_records, issues)
    """
    valid_records = []
    issues = []
    seen_keys = set()
    duplicate_count = 0
    orphan_count = 0

    for fn in footnotes:
        pgpid = fn['pgpid']
        source = fn.get('source', '')
        source_slug = fn.get('source_slug')
        doc_relation = fn.get('doc_relation', '')

        # Filter out footnotes referencing pgpids not in documents table
        if valid_pgpids is not None and pgpid not in valid_pgpids:
            orphan_count += 1
            continue

        if not source:
            issues.append({
                'pgpid': pgpid,
                'issue_type': 'missing_source',
                'details': 'Footnote has no source text'
            })
            continue

        if not doc_relation:
            issues.append({
                'pgpid': pgpid,
                'issue_type': 'missing_doc_relation',
                'details': f'Footnote has no doc_relation (source: {source[:60]})'
            })
            continue

        # Deduplicate by composite key
        key = (pgpid, source_slug, doc_relation)
        if key in seen_keys:
            duplicate_count += 1
            continue
        seen_keys.add(key)

        valid_records.append({
            'pgpid': pgpid,
            'source': source,
            'source_slug': source_slug,
            'doc_relation': doc_relation,
            'location': fn.get('location'),
            'url': fn.get('url'),
            'notes': fn.get('notes'),
            'content': fn.get('content'),
            'content_length': fn.get('content_length'),
        })

    if orphan_count > 0:
        print(f"    Orphan footnotes (pgpid not in documents): {orphan_count:,}")
    if duplicate_count > 0:
        print(f"    Deduplicated: removed {duplicate_count:,} duplicate footnotes")

    return valid_records, issues


def prepare_fragment_records_from_csv(
    fragment_metadata: Dict[str, Dict],
    gs_lookup: Dict[str, str],
    valid_pgpids: Optional[set] = None
) -> Tuple[List[Dict], List[Dict]]:
    """
    Build document_fragments records from fragments.csv.

    Each fragment row has pgpids (semicolon-separated) and metadata.
    For each fragment+pgpid combination, creates a record with sys_id
    looked up via shelfmark normalization.

    Filters out fragments referencing pgpids not in documents table (FK safety).

    Assigns sequence_order by grouping fragments per pgpid and ordering
    them by order of encounter.

    Returns: (valid_records, issues)
    """
    valid_records = []
    issues = []
    seen_keys = set()
    orphan_count = 0

    # First pass: collect all records grouped by pgpid for sequence ordering
    pgpid_fragments = defaultdict(list)

    for shelfmark, meta in fragment_metadata.items():
        pgpids_str = meta['pgpids']
        normalized = normalize_shelfmark(shelfmark)
        sys_id = gs_lookup.get(normalized)

        if not sys_id:
            issues.append({
                'shelfmark': shelfmark,
                'issue_type': 'unmatched_fragment',
                'details': f'Normalized "{normalized}" not in libraries.csv'
            })
            continue

        for pgpid_str in pgpids_str.split(';'):
            pgpid_str = pgpid_str.strip()
            if not pgpid_str:
                continue
            try:
                pgpid = int(pgpid_str)
            except ValueError:
                continue

            # Filter out fragments referencing pgpids not in documents table
            if valid_pgpids is not None and pgpid not in valid_pgpids:
                orphan_count += 1
                continue

            key = (pgpid, sys_id)
            if key in seen_keys:
                continue
            seen_keys.add(key)

            pgpid_fragments[pgpid].append({
                'sys_id': sys_id,
                'shelfmark': shelfmark,
                'collection': meta.get('collection'),
                'library': meta.get('library'),
                'library_abbrev': meta.get('library_abbrev'),
                'fragment_url': meta.get('url'),
                'iiif_url': meta.get('iiif_url'),
            })

    if orphan_count > 0:
        print(f"    Orphan fragments (pgpid not in documents): {orphan_count:,}")

    # Second pass: assign sequence_order and flatten
    for pgpid, frags in pgpid_fragments.items():
        for seq, frag in enumerate(frags, start=1):
            valid_records.append({
                'document_id': pgpid,
                'sys_id': frag['sys_id'],
                'shelfmark': frag['shelfmark'],
                'sequence_order': seq,
                'collection': frag['collection'],
                'library': frag['library'],
                'library_abbrev': frag['library_abbrev'],
                'fragment_url': frag['fragment_url'],
                'iiif_url': frag['iiif_url'],
            })

    return valid_records, issues


# ============================================
# INFRASTRUCTURE
# ============================================

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


def capture_table_counts(client) -> Dict[str, int]:
    """Capture current row counts for all PGP tables."""
    counts = {}
    for table in ['documents', 'document_fragments', 'document_sources', 'document_footnotes']:
        try:
            response = client.table(table).select('*', count='exact', head=True).execute()
            counts[table] = response.count or 0
        except Exception:
            counts[table] = 0
    return counts


def write_verification_report(
    before: Dict[str, int],
    after: Dict[str, int],
    stats: Dict,
    all_issues: List[Dict],
    report_path: str
):
    """Write comprehensive before/after verification report."""
    with open(report_path, 'w', encoding='utf-8') as f:
        f.write("=" * 60 + "\n")
        f.write("Full PGP Import Verification Report\n")
        f.write("=" * 60 + "\n")
        f.write(f"Generated: {datetime.now().isoformat()}\n\n")

        f.write("Table Row Counts (Before -> After):\n")
        f.write("-" * 50 + "\n")
        for table in ['documents', 'document_fragments', 'document_sources', 'document_footnotes']:
            b = before.get(table, 0)
            a = after.get(table, 0)
            delta = a - b
            f.write(f"  {table:25s}: {b:>8,} -> {a:>8,} (delta: {delta:+,d})\n")
        f.write("\n")

        f.write("Records Prepared Per Pass:\n")
        f.write("-" * 50 + "\n")
        f.write(f"  Pass 1 - Documents:          {stats.get('doc_count', 0):>8,}\n")
        f.write(f"  Pass 2 - Document Sources:   {stats.get('source_count', 0):>8,}\n")
        f.write(f"  Pass 3 - Footnotes:          {stats.get('footnote_count', 0):>8,}\n")
        f.write(f"  Pass 4 - Fragment Links:     {stats.get('fragment_count', 0):>8,}\n")
        f.write("\n")

        f.write("Source Statistics:\n")
        f.write("-" * 50 + "\n")
        f.write(f"  Digital Editions:   {stats.get('digital_editions', 0):>8,}\n")
        f.write(f"  Digital Translations: {stats.get('digital_translations', 0):>6,}\n")
        f.write(f"    Hebrew:           {stats.get('translation_hebrew', 0):>8,}\n")
        f.write(f"    English:          {stats.get('translation_english', 0):>8,}\n")
        f.write("\n")

        f.write("Issues:\n")
        f.write("-" * 50 + "\n")
        f.write(f"  Total issues: {len(all_issues):,}\n")
        if all_issues:
            issue_types = defaultdict(int)
            for issue in all_issues:
                issue_types[issue.get('issue_type', 'unknown')] += 1
            for itype, count in sorted(issue_types.items()):
                f.write(f"    {itype}: {count:,}\n")
        f.write("\n")

        # Success rate
        total_attempted = (
            stats.get('doc_count', 0) +
            stats.get('source_count', 0) +
            stats.get('footnote_count', 0) +
            stats.get('fragment_count', 0)
        )
        total_delta = sum(
            after.get(t, 0) - before.get(t, 0)
            for t in ['documents', 'document_fragments', 'document_sources', 'document_footnotes']
        )
        f.write(f"Success Rate:\n")
        f.write(f"  Total records attempted: {total_attempted:,}\n")
        f.write(f"  Total new/updated rows:  {total_delta:,}\n")


# ============================================
# MAIN PIPELINE
# ============================================

def main():
    parser = argparse.ArgumentParser(
        description='Full PGP data import into Supabase',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python scripts/import_pgp_full.py --dry-run   # Validate data (default)
  python scripts/import_pgp_full.py --execute   # Actually import

Prerequisites:
  1. Run all migrations in Supabase SQL Editor
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
    dry_run = not args.execute

    print("=" * 60)
    print("Full PGP Data Import")
    print("=" * 60)
    print()
    print(f"Mode: {'DRY RUN (validation only)' if dry_run else 'EXECUTE (writing to database)'}")
    print()

    # Determine paths
    script_dir = Path(__file__).parent
    project_dir = script_dir.parent

    documents_path = project_dir / 'pgp_data' / 'documents.csv'
    fragments_path = project_dir / 'pgp_data' / 'fragments.csv'
    footnotes_path = project_dir / 'pgp_data' / 'footnotes.csv'
    transcriptions_path = project_dir / 'pgp_data' / 'transcriptions_linked.csv'
    libraries_path = project_dir / 'libraries.csv'
    fist_supplement_path = project_dir / 'pgp_data' / 'fist_shelfmarks_supplement.csv'
    report_path = project_dir / 'pgp_data' / 'full_import_report.txt'

    # Verify input files exist
    missing_files = []
    for path, desc in [
        (documents_path, 'pgp_data/documents.csv'),
        (fragments_path, 'pgp_data/fragments.csv'),
        (footnotes_path, 'pgp_data/footnotes.csv'),
        (transcriptions_path, 'pgp_data/transcriptions_linked.csv'),
        (libraries_path, 'libraries.csv'),
    ]:
        if not path.exists():
            missing_files.append(desc)

    if missing_files:
        print("ERROR: Missing input files:")
        for f in missing_files:
            print(f"  - {f}")
        return 1

    # Check environment for execute mode
    client = None
    if not dry_run:
        supabase_url = os.environ.get('SUPABASE_URL')
        supabase_key = os.environ.get('SUPABASE_SERVICE_KEY')

        if not supabase_url:
            supabase_url = 'https://ylcpglwxompwjcufdemz.supabase.co'

        if not supabase_key:
            print("ERROR: SUPABASE_SERVICE_KEY environment variable not set.")
            print("       This is required to bypass RLS for bulk insert.")
            print("       Get it from: Supabase Dashboard -> Settings -> API -> service_role secret")
            return 1

        print(f"Supabase URL: {supabase_url}")
        print()

        client = create_client(supabase_url, supabase_key)

    # ============================================
    # STEP 1: LOAD ALL DATA SOURCES
    # ============================================
    print("Step 1: Loading data sources...")
    print()

    print("  Loading GenizahSearch shelfmarks from libraries.csv...")
    gs_lookup = load_genizahsearch_shelfmarks(
        str(libraries_path),
        str(fist_supplement_path) if fist_supplement_path.exists() else None
    )
    print(f"    Loaded {len(gs_lookup):,} normalized shelfmarks")
    print()

    print("  Loading documents from documents.csv...")
    documents = load_documents_full(str(documents_path))
    print(f"    Loaded {len(documents):,} document records")
    print()

    print("  Loading fragment metadata from fragments.csv...")
    fragment_metadata = load_fragment_metadata(str(fragments_path))
    print(f"    Loaded {len(fragment_metadata):,} fragment records")
    print()

    print("  Loading footnotes from footnotes.csv...")
    footnotes = load_footnotes(str(footnotes_path))
    print(f"    Loaded {len(footnotes):,} footnote records")
    print()

    print("  Loading transcriptions from transcriptions_linked.csv...")
    transcription_records = load_transcriptions(str(transcriptions_path))
    print(f"    Loaded {len(transcription_records):,} transcription/source records")
    print()

    # ============================================
    # STEP 2: CAPTURE BEFORE COUNTS (execute only)
    # ============================================
    before_counts = {}
    if not dry_run:
        print("Step 2: Capturing before-counts...")
        before_counts = capture_table_counts(client)
        for table, count in before_counts.items():
            print(f"    {table}: {count:,}")
        print()

    # ============================================
    # STEP 3: BUILD TRANSCRIPTION LOOKUP
    # ============================================
    print("Step 3: Building transcription lookup...")

    # Build lookup: pgpid -> first Digital Edition content
    transcription_lookup = {}
    for rec in transcription_records:
        pgpid = rec['pgpid']
        if pgpid not in transcription_lookup and rec['doc_relation'] == 'Digital Edition':
            transcription_lookup[pgpid] = {
                'content': rec['content'],
                'source_scholar': rec['source_scholar'],
            }

    print(f"    Documents with Digital Edition content: {len(transcription_lookup):,}")
    print()

    # ============================================
    # STEP 4: PREPARE ALL RECORD SETS
    # ============================================
    print("Step 4: Preparing records for import...")
    print()

    print("  Preparing document records...")
    doc_records, doc_issues = prepare_document_records(documents, transcription_lookup)
    print(f"    Valid documents: {len(doc_records):,}")
    if doc_issues:
        print(f"    Issues: {len(doc_issues):,}")
    print()

    print("  Preparing document_sources records...")
    source_records, source_stats = prepare_source_records(transcription_records)
    print(f"    Valid sources: {len(source_records):,}")
    print(f"    Digital Editions: {source_stats['digital_editions']:,}")
    print(f"    Digital Translations: {source_stats['digital_translations']:,}")
    print(f"      Hebrew: {source_stats['translation_hebrew']:,}")
    print(f"      English: {source_stats['translation_english']:,}")
    print(f"    Documents with multiple sources: {source_stats['pgpids_with_multiple_sources']:,}")
    print()

    # Build set of valid pgpids for FK safety filtering
    valid_pgpids = set(documents.keys())

    print("  Preparing footnote records...")
    footnote_records, footnote_issues = prepare_footnote_records(footnotes, valid_pgpids)
    print(f"    Valid footnotes: {len(footnote_records):,}")
    if footnote_issues:
        print(f"    Issues: {len(footnote_issues):,}")
    print()

    print("  Preparing fragment records...")
    frag_records, frag_issues = prepare_fragment_records_from_csv(fragment_metadata, gs_lookup, valid_pgpids)
    print(f"    Valid fragment links: {len(frag_records):,}")
    print(f"    Unmatched fragments: {len(frag_issues):,}")
    if fragment_metadata:
        match_rate = len(frag_records) / (len(frag_records) + len(frag_issues)) * 100 if (len(frag_records) + len(frag_issues)) > 0 else 0
        print(f"    Match rate: {match_rate:.1f}%")
    print()

    # Combine all issues
    all_issues = doc_issues + footnote_issues + frag_issues

    # ============================================
    # STEP 5: REPORT STATISTICS
    # ============================================
    print("=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print()
    print(f"  Documents to upsert:         {len(doc_records):>8,}")
    print(f"  Document sources to upsert:  {len(source_records):>8,}")
    print(f"  Footnotes to upsert:         {len(footnote_records):>8,}")
    print(f"  Fragment links to upsert:    {len(frag_records):>8,}")
    print()
    print(f"  Total issues:                {len(all_issues):>8,}")
    if all_issues:
        issue_types = defaultdict(int)
        for issue in all_issues:
            issue_types[issue.get('issue_type', 'unknown')] += 1
        for itype, count in sorted(issue_types.items()):
            print(f"    {itype}: {count:,}")
    print()

    # ============================================
    # STEP 6: EXECUTE OR DRY-RUN EXIT
    # ============================================
    if dry_run:
        print("DRY RUN COMPLETE")
        print()
        print(f"Would import {len(doc_records):,} documents")
        print(f"Would import {len(source_records):,} document sources")
        print(f"Would import {len(footnote_records):,} footnotes")
        print(f"Would create {len(frag_records):,} fragment links")
        print()
        print("To execute import, run with --execute flag")
        return 0

    # Execute mode
    print("Step 6: Importing to Supabase...")
    print()

    # Pass 1: Documents (no FK dependencies)
    print("Pass 1: Upserting documents...")
    docs_processed = upsert_in_batches(
        client, 'documents', doc_records,
        on_conflict='pgpid', dry_run=False
    )
    print(f"  Processed {docs_processed:,} documents")
    print()

    # Pass 2: Document sources (FK to documents.pgpid)
    print("Pass 2: Upserting document_sources...")
    sources_processed = upsert_in_batches(
        client, 'document_sources', source_records,
        on_conflict='pgpid,source_scholar,doc_relation', dry_run=False
    )
    print(f"  Processed {sources_processed:,} document sources")
    print()

    # Pass 3: Footnotes (FK to documents.pgpid)
    print("Pass 3: Upserting document_footnotes...")
    footnotes_processed = upsert_in_batches(
        client, 'document_footnotes', footnote_records,
        on_conflict='pgpid,source_slug,doc_relation', dry_run=False
    )
    print(f"  Processed {footnotes_processed:,} footnotes")
    print()

    # Pass 4: Fragment links (FK to documents.pgpid)
    print("Pass 4: Upserting document_fragments...")
    frags_processed = upsert_in_batches(
        client, 'document_fragments', frag_records,
        on_conflict='document_id,sys_id', dry_run=False
    )
    print(f"  Processed {frags_processed:,} fragment links")
    print()

    # Capture after-counts
    print("Capturing after-counts...")
    after_counts = capture_table_counts(client)
    for table, count in after_counts.items():
        delta = count - before_counts.get(table, 0)
        print(f"    {table}: {count:,} (delta: {delta:+,d})")
    print()

    # Write verification report
    stats = {
        'doc_count': len(doc_records),
        'source_count': len(source_records),
        'footnote_count': len(footnote_records),
        'fragment_count': len(frag_records),
        'digital_editions': source_stats['digital_editions'],
        'digital_translations': source_stats['digital_translations'],
        'translation_hebrew': source_stats['translation_hebrew'],
        'translation_english': source_stats['translation_english'],
    }

    print(f"Writing verification report to {report_path}...")
    write_verification_report(before_counts, after_counts, stats, all_issues, str(report_path))
    print()

    print("IMPORT COMPLETE")
    print()
    print(f"  Documents processed:    {docs_processed:,}")
    print(f"  Sources processed:      {sources_processed:,}")
    print(f"  Footnotes processed:    {footnotes_processed:,}")
    print(f"  Fragments processed:    {frags_processed:,}")
    print(f"  Issues:                 {len(all_issues):,}")

    return 0


if __name__ == '__main__':
    sys.exit(main())
