#!/usr/bin/env python3
"""
Import PGP Sections from pgp-text HTML Files

Clones the pgp-text GitHub repository, parses HTML files to extract
structured per-canvas section data, and populates the sections JSONB
column on document_sources in Supabase.

The pgp-text repo contains ~7,300 PGPIDs with HTML files that use
<div data-canvas="..."> elements to structurally map transcription
sections to IIIF canvas URLs. This enables reliable canvas-based
recto/verso mapping, replacing regex-based guessing.

Input:
  - pgp-text repo (cloned to pgp_data/pgp-text/)
  - Supabase document_sources table (existing records)

Output:
  - Updated document_sources.sections JSONB column
  - Updated document_sources.source_language TEXT column
  - Updated document_sources.source_direction TEXT column
  - pgp_data/sections_import_report.txt (verification report)

Usage:
  python scripts/import_pgp_sections.py --dry-run     # Parse and report (default)
  python scripts/import_pgp_sections.py --execute      # Actually update database
  python scripts/import_pgp_sections.py --skip-clone   # Skip git clone/pull

Prerequisites:
  1. Run migrations/add_sections_column.sql in Supabase SQL Editor
  2. Set SUPABASE_SERVICE_KEY environment variable (for --execute)
"""

import argparse
import glob
import json
import os
import re
import subprocess
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

# Add project root to path for shared imports
sys.path.insert(0, str(Path(__file__).parent.parent))
from shared.document_service import parse_html_sections

# Constants
BATCH_SIZE = 500
PGP_TEXT_REPO = 'https://github.com/princetongenizalab/pgp-text.git'
FILENAME_RE = re.compile(r'PGPID(\d+)_s(\d+)_(.+)_(transcription|translation)\.html')


# ============================================
# GIT OPERATIONS
# ============================================

def clone_or_update_pgp_text(target_dir: str) -> bool:
    """
    Clone pgp-text repo (shallow) or update if exists.

    On Windows, the annotations directory contains filenames with colons
    (e.g., 'colenda__items_ark:_81431_...') which are invalid on NTFS.
    We use sparse checkout to exclude the annotations directory and
    core.protectNTFS=false to handle any remaining edge cases.

    Returns: True if successful, False on error.
    """
    if os.path.exists(os.path.join(target_dir, '.git')):
        print(f"  pgp-text repo exists at {target_dir}, pulling updates...")
        result = subprocess.run(
            ['git', '-C', target_dir, 'pull'],
            capture_output=True, text=True
        )
        if result.returncode != 0:
            print(f"  WARNING: git pull failed: {result.stderr}")
            print("  Continuing with existing repo state...")
        else:
            print(f"  {result.stdout.strip()}")
        return True
    else:
        print(f"  Cloning pgp-text repo to {target_dir}...")
        print(f"  (This may take a few minutes for the ~68MB repo)")

        # Step 1: Clone without checkout to avoid Windows NTFS issues
        # The annotations/ directory contains filenames with colons which
        # are invalid on Windows NTFS filesystems.
        result = subprocess.run(
            ['git', 'clone', '--depth', '1', '--no-checkout',
             PGP_TEXT_REPO, target_dir],
            capture_output=True, text=True
        )
        if result.returncode != 0:
            print(f"  ERROR: git clone failed: {result.stderr}")
            return False

        # Step 2: Disable NTFS path protection for any remaining edge cases
        subprocess.run(
            ['git', '-C', target_dir, 'config', 'core.protectNTFS', 'false'],
            capture_output=True, text=True
        )

        # Step 3: Set up sparse checkout to exclude annotations directory
        subprocess.run(
            ['git', '-C', target_dir, 'sparse-checkout', 'init', '--cone'],
            capture_output=True, text=True
        )

        # Get list of bucket directories (00000, 01000, ..., 41000)
        # These contain the PGPID HTML files we need
        bucket_dirs = [f'{i:05d}' for i in range(0, 42000, 1000)]
        subprocess.run(
            ['git', '-C', target_dir, 'sparse-checkout', 'set'] + bucket_dirs,
            capture_output=True, text=True
        )

        # Step 4: Checkout with sparse settings
        result = subprocess.run(
            ['git', '-C', target_dir, 'checkout'],
            capture_output=True, text=True
        )
        if result.returncode != 0:
            # May still report warnings about annotations, but files should be OK
            if 'error:' in result.stderr and 'annotations' not in result.stderr:
                print(f"  ERROR: checkout failed: {result.stderr}")
                return False
            elif result.stderr:
                print(f"  Note: checkout completed with warnings (annotations dir excluded)")

        print(f"  Clone complete.")
        return True


# ============================================
# FILE SCANNING
# ============================================

def scan_html_files(repo_dir: str) -> Dict[int, List[Dict]]:
    """
    Scan pgp-text repo for HTML files.

    Returns: Dict mapping pgpid to list of file info dicts:
        {pgpid: [{'path': str, 'source_id': int, 'author_slug': str, 'type': str}]}
    """
    # Scan for all HTML files matching the PGPID pattern
    # Pattern: {bucket}/{pgpid}/PGPID{N}_s{N}_{author}_{type}.html
    pattern = os.path.join(repo_dir, '*', '*', 'PGPID*.html')
    all_files = glob.glob(pattern)

    pgpid_files = defaultdict(list)
    unmatched = 0

    for filepath in all_files:
        basename = os.path.basename(filepath)
        m = FILENAME_RE.match(basename)
        if m:
            pgpid = int(m.group(1))
            pgpid_files[pgpid].append({
                'path': filepath,
                'source_id': int(m.group(2)),
                'author_slug': m.group(3),
                'type': m.group(4),  # 'transcription' or 'translation'
            })
        else:
            unmatched += 1

    if unmatched:
        print(f"  WARNING: {unmatched} HTML files did not match expected filename pattern")

    return dict(pgpid_files)


# ============================================
# PARSING AND MATCHING
# ============================================

def parse_best_file_for_type(files: List[Dict], file_type: str) -> Optional[Dict]:
    """
    Select and parse the best HTML file for a given type (transcription/translation).

    For a given type, if multiple files exist, parse each and use the one
    with the most sections (most canvases) -- this is the most complete version.

    Returns: Parsed result dict {sections, language, direction} or None.
    """
    type_files = [f for f in files if f['type'] == file_type]
    if not type_files:
        return None

    best_result = None
    best_section_count = -1

    for file_info in type_files:
        try:
            with open(file_info['path'], 'r', encoding='utf-8') as f:
                html_content = f.read()

            result = parse_html_sections(html_content)

            section_count = len(result.get('sections', []))
            if section_count > best_section_count:
                best_section_count = section_count
                best_result = result

        except Exception as e:
            print(f"  WARNING: Failed to parse {file_info['path']}: {e}")

    return best_result


def load_existing_sources(client: Optional[Client]) -> Dict[int, List[Dict]]:
    """
    Load all document_sources records from Supabase.

    Returns: Dict mapping pgpid to list of source records.
    """
    if client is None:
        # Dry-run mode without client: return empty
        return {}

    print("  Loading existing document_sources from Supabase...")

    all_records = []
    offset = 0
    page_size = 1000

    while True:
        response = client.table('document_sources').select(
            'pgpid, source_scholar, doc_relation'
        ).range(offset, offset + page_size - 1).execute()

        if not response.data:
            break

        all_records.extend(response.data)
        if len(response.data) < page_size:
            break
        offset += page_size

    # Group by pgpid
    sources_by_pgpid = defaultdict(list)
    for rec in all_records:
        sources_by_pgpid[rec['pgpid']].append(rec)

    print(f"    Loaded {len(all_records):,} source records for {len(sources_by_pgpid):,} PGPIDs")
    return dict(sources_by_pgpid)


def build_update_batches(
    pgpid_files: Dict[int, List[Dict]],
    sources_by_pgpid: Dict[int, List[Dict]]
) -> Tuple[List[Dict], Dict]:
    """
    Match parsed HTML sections to document_sources records and build update batches.

    Strategy per pgpid:
    - For each _transcription.html: parse sections, apply to ALL "Digital Edition"
      sources for that pgpid (same physical manuscript structure)
    - For each _translation.html: parse sections, apply to ALL "Digital Translation"
      sources for that pgpid

    Returns: (update_records, stats)
    """
    updates = []
    stats = {
        'pgpids_with_html': len(pgpid_files),
        'pgpids_matched': 0,
        'pgpids_unmatched_in_db': 0,
        'editions_updated': 0,
        'translations_updated': 0,
        'transcription_files_parsed': 0,
        'translation_files_parsed': 0,
        'parse_errors': 0,
        'unmatched_pgpids': [],  # In pgp-text but not in DB
    }

    for pgpid, files in tqdm(pgpid_files.items(), desc="Matching sections"):
        source_records = sources_by_pgpid.get(pgpid, [])

        if not source_records:
            stats['pgpids_unmatched_in_db'] += 1
            stats['unmatched_pgpids'].append(pgpid)
            continue

        stats['pgpids_matched'] += 1

        # Parse transcription HTML -> apply to all Edition sources
        transcription_result = parse_best_file_for_type(files, 'transcription')
        if transcription_result and transcription_result.get('sections'):
            stats['transcription_files_parsed'] += 1
            edition_sources = [
                s for s in source_records
                if 'Edition' in (s.get('doc_relation') or '')
            ]
            for source in edition_sources:
                updates.append({
                    'pgpid': pgpid,
                    'source_scholar': source['source_scholar'],
                    'doc_relation': source['doc_relation'],
                    'sections': transcription_result['sections'],
                    'source_language': transcription_result.get('language'),
                    'source_direction': transcription_result.get('direction'),
                })
                stats['editions_updated'] += 1

        # Parse translation HTML -> apply to all Translation sources
        translation_result = parse_best_file_for_type(files, 'translation')
        if translation_result and translation_result.get('sections'):
            stats['translation_files_parsed'] += 1
            translation_sources = [
                s for s in source_records
                if 'Translation' in (s.get('doc_relation') or '')
            ]
            for source in translation_sources:
                updates.append({
                    'pgpid': pgpid,
                    'source_scholar': source['source_scholar'],
                    'doc_relation': source['doc_relation'],
                    'sections': translation_result['sections'],
                    'source_language': translation_result.get('language'),
                    'source_direction': translation_result.get('direction'),
                })
                stats['translations_updated'] += 1

    return updates, stats


# ============================================
# DATABASE OPERATIONS
# ============================================

def execute_updates(client: Client, updates: List[Dict]) -> int:
    """
    Execute section updates using individual update+match calls.

    Updates only the sections/source_language/source_direction columns
    on existing records matched by (pgpid, source_scholar, doc_relation).

    Returns: Number of records processed.
    """
    if not updates:
        return 0

    processed = 0
    errors = 0

    for i in tqdm(range(0, len(updates), BATCH_SIZE), desc="Updating sections"):
        batch = updates[i:i + BATCH_SIZE]
        for record in batch:
            try:
                client.table('document_sources').update({
                    'sections': record['sections'],
                    'source_language': record.get('source_language'),
                    'source_direction': record.get('source_direction'),
                }).match({
                    'pgpid': record['pgpid'],
                    'source_scholar': record['source_scholar'],
                    'doc_relation': record['doc_relation'],
                }).execute()
                processed += 1
            except Exception as e:
                errors += 1
                if errors <= 5:
                    print(f"  Error updating pgpid={record['pgpid']}: {e}")

    if errors:
        print(f"  {errors} errors during update")

    return processed


# ============================================
# REPORTING
# ============================================

def write_verification_report(
    pgpid_files: Dict[int, List[Dict]],
    stats: Dict,
    updates: List[Dict],
    report_path: str,
    dry_run: bool = True
):
    """Write comprehensive verification report."""
    with open(report_path, 'w', encoding='utf-8') as f:
        f.write("=" * 60 + "\n")
        f.write("PGP Sections Import Report\n")
        f.write("=" * 60 + "\n")
        f.write(f"Generated: {datetime.now().isoformat()}\n")
        f.write(f"Mode: {'DRY RUN' if dry_run else 'EXECUTE'}\n\n")

        # HTML file statistics
        total_files = sum(len(files) for files in pgpid_files.values())
        transcription_files = sum(
            len([f for f in files if f['type'] == 'transcription'])
            for files in pgpid_files.values()
        )
        translation_files = sum(
            len([f for f in files if f['type'] == 'translation'])
            for files in pgpid_files.values()
        )

        f.write("HTML Files Found:\n")
        f.write("-" * 50 + "\n")
        f.write(f"  Total HTML files:        {total_files:>8,}\n")
        f.write(f"  Transcription files:     {transcription_files:>8,}\n")
        f.write(f"  Translation files:       {translation_files:>8,}\n")
        f.write(f"  Unique PGPIDs:           {len(pgpid_files):>8,}\n")
        f.write("\n")

        f.write("Parsing Results:\n")
        f.write("-" * 50 + "\n")
        f.write(f"  Transcriptions parsed:   {stats['transcription_files_parsed']:>8,}\n")
        f.write(f"  Translations parsed:     {stats['translation_files_parsed']:>8,}\n")
        f.write(f"  Parse errors:            {stats['parse_errors']:>8,}\n")
        f.write("\n")

        f.write("Matching Results:\n")
        f.write("-" * 50 + "\n")
        f.write(f"  PGPIDs with HTML:        {stats['pgpids_with_html']:>8,}\n")
        f.write(f"  PGPIDs matched in DB:    {stats['pgpids_matched']:>8,}\n")
        f.write(f"  PGPIDs unmatched (not in DB): {stats['pgpids_unmatched_in_db']:>5,}\n")
        f.write("\n")

        f.write("Update Statistics:\n")
        f.write("-" * 50 + "\n")
        f.write(f"  Total updates:           {len(updates):>8,}\n")
        f.write(f"  Edition sources updated: {stats['editions_updated']:>8,}\n")
        f.write(f"  Translation sources updated: {stats['translations_updated']:>5,}\n")
        f.write("\n")

        # Unmatched PGPIDs (in pgp-text but not in DB)
        unmatched = stats.get('unmatched_pgpids', [])
        if unmatched:
            f.write(f"Unmatched PGPIDs (in pgp-text, not in DB): {len(unmatched)}\n")
            f.write("-" * 50 + "\n")
            # Show first 20
            for pgpid in sorted(unmatched)[:20]:
                f.write(f"  PGPID {pgpid}\n")
            if len(unmatched) > 20:
                f.write(f"  ... and {len(unmatched) - 20} more\n")
            f.write("\n")

        # Sample parsed sections (first 3 PGPIDs)
        f.write("Sample Parsed Sections (first 3 PGPIDs):\n")
        f.write("-" * 50 + "\n")
        sample_count = 0
        for update in updates:
            if sample_count >= 3:
                break
            # Only show one sample per pgpid
            pgpid = update['pgpid']
            sections = update.get('sections', [])
            f.write(f"\n  PGPID {pgpid} ({update['doc_relation']}):\n")
            f.write(f"    Language: {update.get('source_language')}, Direction: {update.get('source_direction')}\n")
            f.write(f"    Sections: {len(sections)}\n")
            for sec in sections[:3]:
                label = sec.get('label', '(no label)')
                text_preview = (sec.get('text', '') or '')[:80]
                f.write(f"      Canvas {sec.get('canvas_num')}: {label}\n")
                f.write(f"        Text: {text_preview}...\n")
            sample_count += 1

        f.write("\n")
        f.write("=" * 60 + "\n")
        f.write("END OF REPORT\n")


# ============================================
# MAIN PIPELINE
# ============================================

def main(dry_run: bool = True, skip_clone: bool = False):
    print("=" * 60)
    print("PGP Sections Import from pgp-text HTML")
    print("=" * 60)
    print()
    print(f"Mode: {'DRY RUN (parse and report only)' if dry_run else 'EXECUTE (updating database)'}")
    print()

    # Determine paths
    project_dir = Path(__file__).parent.parent
    pgp_text_dir = str(project_dir / 'pgp_data' / 'pgp-text')
    report_path = str(project_dir / 'pgp_data' / 'sections_import_report.txt')

    # Step 1: Clone or update pgp-text repo
    print("Step 1: Getting pgp-text repository...")
    if skip_clone:
        print("  Skipping clone/pull (--skip-clone)")
        if not os.path.exists(pgp_text_dir):
            print(f"  ERROR: pgp-text directory not found at {pgp_text_dir}")
            print("  Run without --skip-clone to clone the repo first.")
            return 1
    else:
        if not clone_or_update_pgp_text(pgp_text_dir):
            return 1
    print()

    # Step 2: Scan for HTML files
    print("Step 2: Scanning for HTML files...")
    pgpid_files = scan_html_files(pgp_text_dir)
    total_files = sum(len(files) for files in pgpid_files.values())
    transcription_count = sum(
        len([f for f in files if f['type'] == 'transcription'])
        for files in pgpid_files.values()
    )
    translation_count = sum(
        len([f for f in files if f['type'] == 'translation'])
        for files in pgpid_files.values()
    )
    print(f"  Found {total_files:,} HTML files across {len(pgpid_files):,} PGPIDs")
    print(f"    Transcription files: {transcription_count:,}")
    print(f"    Translation files:   {translation_count:,}")
    print()

    if not pgpid_files:
        print("ERROR: No HTML files found. Check pgp-text repo at:")
        print(f"  {pgp_text_dir}")
        return 1

    # Step 3: Load existing document_sources from Supabase
    print("Step 3: Loading existing document_sources...")
    client = None
    sources_by_pgpid = {}

    if not dry_run:
        supabase_url = os.environ.get('SUPABASE_URL')
        supabase_key = os.environ.get('SUPABASE_SERVICE_KEY')

        if not supabase_url:
            supabase_url = 'https://ylcpglwxompwjcufdemz.supabase.co'

        if not supabase_key:
            print("  ERROR: SUPABASE_SERVICE_KEY environment variable not set.")
            print("         Get it from: Supabase Dashboard -> Settings -> API -> service_role secret")
            return 1

        client = create_client(supabase_url, supabase_key)
        sources_by_pgpid = load_existing_sources(client)
    else:
        # For dry-run, still try to load sources for matching stats
        try:
            supabase_url = os.environ.get('SUPABASE_URL', 'https://ylcpglwxompwjcufdemz.supabase.co')
            supabase_key = os.environ.get('SUPABASE_SERVICE_KEY') or os.environ.get('SUPABASE_ANON_KEY')
            if supabase_key:
                client = create_client(supabase_url, supabase_key)
                sources_by_pgpid = load_existing_sources(client)
            else:
                print("  No Supabase key available, skipping source matching in dry-run")
                print("  (Set SUPABASE_SERVICE_KEY or SUPABASE_ANON_KEY for full dry-run stats)")
        except Exception as e:
            print(f"  Could not load sources for dry-run matching: {e}")
            print("  Will still parse and report HTML statistics")
    print()

    # Step 4: Match and build update batches
    print("Step 4: Parsing HTML and matching to sources...")
    if sources_by_pgpid:
        updates, stats = build_update_batches(pgpid_files, sources_by_pgpid)
    else:
        # No sources loaded -- just parse files for statistics
        stats = {
            'pgpids_with_html': len(pgpid_files),
            'pgpids_matched': 0,
            'pgpids_unmatched_in_db': 0,
            'editions_updated': 0,
            'translations_updated': 0,
            'transcription_files_parsed': 0,
            'translation_files_parsed': 0,
            'parse_errors': 0,
            'unmatched_pgpids': [],
        }

        # Still parse a sample to verify parsing works
        print("  Parsing sample of HTML files to verify parser...")
        sample_count = 0
        for pgpid, files in list(pgpid_files.items())[:10]:
            result = parse_best_file_for_type(files, 'transcription')
            if result and result.get('sections'):
                stats['transcription_files_parsed'] += 1
                sample_count += 1

            result = parse_best_file_for_type(files, 'translation')
            if result and result.get('sections'):
                stats['translation_files_parsed'] += 1

        print(f"  Parsed {sample_count} sample transcriptions successfully")
        updates = []
    print()

    # Step 5: Summary
    print("=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print()
    print(f"  HTML files found:          {total_files:>8,}")
    print(f"  Unique PGPIDs:             {len(pgpid_files):>8,}")
    if sources_by_pgpid:
        print(f"  PGPIDs matched in DB:      {stats['pgpids_matched']:>8,}")
        print(f"  PGPIDs unmatched:          {stats['pgpids_unmatched_in_db']:>8,}")
        print(f"  Edition updates:           {stats['editions_updated']:>8,}")
        print(f"  Translation updates:       {stats['translations_updated']:>8,}")
        print(f"  Total updates to apply:    {len(updates):>8,}")
    print()

    # Step 6: Execute or dry-run exit
    if dry_run:
        print("DRY RUN COMPLETE")
        print()
        if updates:
            print(f"Would update {len(updates):,} document_sources records")
        else:
            print("No source matching performed (dry-run without DB connection)")
            print("HTML parsing verified successfully on sample files")
        print()
        print("To execute import, run with --execute flag")
    else:
        print("Step 6: Executing updates...")
        processed = execute_updates(client, updates)
        print(f"  Processed {processed:,} updates")
        print()
        print("IMPORT COMPLETE")

    # Write verification report
    print()
    print(f"Writing report to {report_path}...")
    write_verification_report(pgpid_files, stats, updates, report_path, dry_run)
    print("Done.")

    return 0


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Import PGP sections from pgp-text HTML',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python scripts/import_pgp_sections.py --dry-run     # Parse and report (default)
  python scripts/import_pgp_sections.py --execute      # Actually update database
  python scripts/import_pgp_sections.py --skip-clone   # Skip git clone/pull

Prerequisites:
  1. Run migrations/add_sections_column.sql in Supabase SQL Editor
  2. Set SUPABASE_SERVICE_KEY environment variable (for --execute)
        """
    )
    group = parser.add_mutually_exclusive_group()
    group.add_argument('--dry-run', action='store_true', default=True,
                       help='Validate without writing (default)')
    group.add_argument('--execute', action='store_true',
                       help='Actually import data')
    parser.add_argument('--skip-clone', action='store_true',
                        help='Skip git clone/pull (use existing repo)')
    args = parser.parse_args()
    sys.exit(main(dry_run=not args.execute, skip_clone=args.skip_clone))
