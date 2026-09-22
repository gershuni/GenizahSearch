#!/usr/bin/env python3
"""
Update doc_relation column in documents table from transcriptions_linked.csv

This script populates the doc_relation column to distinguish between:
- Digital Edition (7,664 records) - Original transcriptions (Hebrew/Aramaic)
- Digital Translation (1,696 records) - English translations

Users see "PGP Transcription" only for Digital Editions; translations are filtered out.

Usage:
  python scripts/update_doc_relation.py --dry-run  # Validate and report (default)
  python scripts/update_doc_relation.py --execute  # Actually update data

Prerequisites:
  1. Run migration in Supabase SQL Editor:
     - add_doc_relation_column.sql
  2. Set SUPABASE_SERVICE_KEY environment variable
"""

import argparse
import csv
import os
import sys
from collections import defaultdict
from pathlib import Path

# Load .env file if present
def load_env():
    """Load environment variables from .env file if present."""
    env_path = Path(__file__).parent.parent / '.env'
    if env_path.exists():
        with open(env_path, encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    # Only set if not already set in environment
                    if key not in os.environ:
                        os.environ[key] = value

load_env()

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
BATCH_SIZE = 500  # Match import script pattern
TRANSCRIPTIONS_CSV = Path(__file__).parent.parent / 'pgp_data' / 'transcriptions_linked.csv'

# Supabase URL (same as web/supabase_client.py)
SUPABASE_URL = os.environ.get('SUPABASE_URL', 'https://ylcpglwxompwjcufdemz.supabase.co')


def load_doc_relations(csv_path: Path) -> dict:
    """
    Load pgpid -> doc_relation mapping from transcriptions_linked.csv

    An Edition ALWAYS wins over a Translation for the same pgpid. That is not a
    preference, it is the only value consistent with the text actually stored:
    import_pgp_full.py sets documents.transcription from the FIRST 'Digital Edition' row,
    while this used to take the LAST row of any kind. A document with an edition followed
    by a translation therefore held edition text but was labelled a translation -- and
    once doc_relation reaches the sidecar, Browse and Advanced View suppress that text as
    a translation. Measured on the 2026-09-22 corpus: 1,045 documents.

    Returns: Dict mapping pgpid (int) to doc_relation (str)
    """
    relations = {}

    with open(csv_path, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)

        for row in reader:
            # Handle BOM in first column if present
            pgpid_str = row.get('pgpid', '')

            if not pgpid_str:
                continue

            try:
                pgpid = int(pgpid_str)
            except ValueError:
                continue

            doc_relation = row.get('doc_relation', '').strip()
            if not doc_relation:
                continue

            # Same predicate the importer uses to pick the transcription
            # (import_pgp_full.py::is_edition_relation); they must not diverge.
            existing = relations.get(pgpid)
            if existing and 'Edition' in existing and 'Edition' not in doc_relation:
                # Already classified by an edition; a later translation must not demote it.
                continue
            relations[pgpid] = doc_relation

    return relations


def get_supabase_client() -> Client:
    """Create Supabase client with service role key for writes."""
    service_key = os.getenv('SUPABASE_SERVICE_KEY')

    if not service_key:
        print("ERROR: SUPABASE_SERVICE_KEY environment variable not set")
        print("Note: Service key required for database writes (not anon key)")
        print("Set in .env file or environment")
        sys.exit(1)

    return create_client(SUPABASE_URL, service_key)


def batch_update_doc_relations(client: Client, relations: dict) -> tuple:
    """
    Update doc_relation column in batches.

    Returns: (success_count, not_found_count, failed_count)

    `not found` and `failed` used to be one counter, and the epilogue asserted the benign
    reading of it -- so a Supabase outage that failed thousands of updates printed "this
    is expected for multi-fragment documents" and exited 0. They are different events and
    only one of them is normal, so they are counted separately and only one of them is
    fatal.
    """
    success_count = 0
    not_found_count = 0
    failed_count = 0

    # Convert to list of (pgpid, doc_relation) for batching
    items = list(relations.items())

    for i in tqdm(range(0, len(items), BATCH_SIZE), desc="Updating batches"):
        batch = items[i:i + BATCH_SIZE]

        for pgpid, doc_relation in batch:
            try:
                response = client.table('documents').update({
                    'doc_relation': doc_relation
                }).eq('pgpid', pgpid).execute()

                if response.data:
                    success_count += 1
                else:
                    # No row matched this pgpid. Normal: the CSV carries one row per
                    # fragment, so a multi-fragment document appears several times.
                    not_found_count += 1

            except Exception as e:
                print(f"  ERROR updating pgpid {pgpid}: {e}")
                failed_count += 1

    return success_count, not_found_count, failed_count


def main():
    parser = argparse.ArgumentParser(
        description='Update doc_relation column in documents table'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        default=True,
        help='Validate and report without updating (default)'
    )
    parser.add_argument(
        '--execute',
        action='store_true',
        help='Actually update the database'
    )
    args = parser.parse_args()

    # --execute overrides --dry-run
    dry_run = not args.execute

    print("=" * 60)
    print("Update doc_relation column in documents table")
    print("=" * 60)
    print()

    # Load CSV data
    print(f"Loading doc_relation data from {TRANSCRIPTIONS_CSV}...")
    if not TRANSCRIPTIONS_CSV.exists():
        print(f"ERROR: CSV file not found: {TRANSCRIPTIONS_CSV}")
        sys.exit(1)

    relations = load_doc_relations(TRANSCRIPTIONS_CSV)
    print(f"  Loaded {len(relations)} pgpid -> doc_relation mappings")
    print()

    # Report statistics
    counts = defaultdict(int)
    for doc_relation in relations.values():
        counts[doc_relation] += 1

    print("doc_relation distribution:")
    for relation, count in sorted(counts.items(), key=lambda x: -x[1]):
        pct = (count / len(relations)) * 100
        print(f"  {relation}: {count} ({pct:.1f}%)")
    print()

    # Summary
    editions = counts.get('Digital Edition', 0)
    translations = counts.get('Digital Translation', 0)
    other = len(relations) - editions - translations

    print("Summary:")
    print(f"  Digital Editions (transcriptions to show): {editions}")
    print(f"  Digital Translations (to filter out): {translations}")
    if other > 0:
        print(f"  Other types: {other}")
    print()

    if dry_run:
        print("DRY RUN mode - no changes made")
        print()
        print("To update the database, run:")
        print("  python scripts/update_doc_relation.py --execute")
        return 0

    # Execute updates
    print("EXECUTE mode - updating database...")
    print()

    client = get_supabase_client()

    success, not_found, failed = batch_update_doc_relations(client, relations)

    print()
    print("=" * 60)
    print("Update complete")
    print("=" * 60)
    print(f"  Successful updates: {success}")
    print(f"  Not found in database: {not_found}")
    print(f"  FAILED (exceptions): {failed}")

    if not_found > 0:
        print()
        print(f"Note: {not_found} pgpids in the CSV matched no row.")
        print("This is expected for multi-fragment documents, where several CSV rows")
        print("map to a single pgpid.")

    if failed:
        print()
        print(f"ERROR: {failed} update(s) raised. documents.doc_relation is now PARTIALLY",
              file=sys.stderr)
        print("updated, which is the worst of the three states: the next sidecar export",
              file=sys.stderr)
        print("will pass its own row-count and schema checks and ship a mix of fresh and",
              file=sys.stderr)
        print("stale classifications. Re-run this script before exporting.", file=sys.stderr)
        return 1

    return 0


if __name__ == '__main__':
    sys.exit(main())
