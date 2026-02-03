#!/usr/bin/env python3
"""
Extract Library Codes from libraries.csv

This script analyzes the call_numbers column in libraries.csv and extracts
library codes for each manuscript record. It adds a new library_code column
to the CSV file.

Usage:
    python scripts/extract_library_codes.py [--dry-run] [--interactive]

Options:
    --dry-run       Show statistics without modifying the file
    --interactive   Prompt for confirmation before writing
"""

import csv
import re
import sys
import os
from collections import Counter
from typing import Dict, List, Tuple, Optional

# Library detection patterns in priority order
# First match wins
LIBRARY_PATTERNS: List[Tuple[str, str, str]] = [
    # (pattern, library_code, full_name)
    (r'Cambridge University Library', 'CUL', 'Cambridge University Library'),
    (r'Jewish Theological Seminary', 'JTS', 'The Jewish Theological Seminary of America'),
    (r'National Library of Russia', 'RNL', 'The National Library of Russia'),
    (r'Bodleian Libraries', 'Oxford', 'The Bodleian Libraries, University of Oxford'),
    (r'University of Manchester', 'Manchester', 'The University of Manchester Library'),
    (r'British Library', 'BL', 'The British Library'),
    (r'Alliance Israélite', 'AIU', 'Alliance Israélite Universelle'),
    (r'Westminster College', 'Westminster', 'Westminster College'),
    (r'Freer Gallery', 'Freer', 'Freer Gallery of Art'),
    (r'Herbert D\. Katz', 'Katz', 'Katz Center'),
    (r'Katz Center', 'Katz', 'Katz Center'),
    # Additional libraries
    (r'Hebrew Union College', 'HUC', 'Hebrew Union College Library'),
    (r'Institute of France', 'InstFrance', 'Library of the Institute of France'),
    (r'Hungarian Academy of Sciences', 'HAS', 'Library of the Hungarian Academy of Sciences'),
    (r'Academy of Sciences and Literature', 'ASL', 'Academy of Sciences and Literature'),
    (r'Jewish Community of Warsaw', 'Warsaw', 'Jewish Community of Warsaw'),
    (r'Austrian National Library', 'Vienna', 'Austrian National Library'),
    (r'National and University Library of Strasbourg', 'Strasbourg', 'National and University Library of Strasbourg'),
    (r'University of Pennsylvania', 'UPenn', 'University of Pennsylvania'),
    (r'Bibliothèque nationale de France', 'BnF', 'Bibliothèque nationale de France'),
]

# Collection-based detection (fallback when no explicit library name)
# These check the beginning of the call_numbers or shelfmark patterns
COLLECTION_PATTERNS: List[Tuple[str, str, str]] = [
    # (regex_pattern, library_code, description)
    (r'^"?Moss\.', 'Mosseri', 'Mosseri Collection'),
    (r'^"?Mosseri', 'Mosseri', 'Mosseri Collection'),
    (r'^"?Gaster', 'Gaster', 'Gaster Collection'),
    (r'^"?Halper', 'Halper', 'Halper Catalogue'),
    (r'Catalogue Halper', 'Halper', 'Halper Catalogue'),
    # Small private collections
    (r'^"?Schoeyen', 'Schoeyen', 'Schoeyen Collection'),
    (r'^"?Harkavy', 'Harkavy', 'Harkavy Collection'),
    (r'^"?Combs', 'Combs', 'Combs Collection'),
    (r'^"?Lehnardt', 'Lehnardt', 'Lehnardt Collection'),
    (r'^"?Allony', 'Allony', 'Allony Collection'),
    (r'^"?Boesky', 'Boesky', 'Boesky Collection'),
    (r'^"?Bisno', 'Bisno', 'Bisno Collection'),
    # T-S prefix indicates Cambridge
    (r'^T-S\s', 'CUL', 'Cambridge University Library'),
    (r'^Ms\.\s*T-S\s', 'CUL', 'Cambridge University Library'),
    # ENA prefix indicates JTS
    (r'^ENA\s', 'JTS', 'The Jewish Theological Seminary of America'),
    (r'^Ms\.\s*ENA\s', 'JTS', 'The Jewish Theological Seminary of America'),
    # MS heb prefix indicates Oxford
    (r'^MS\s*heb\.', 'Oxford', 'The Bodleian Libraries, University of Oxford'),
    (r'^Ms\.\s*heb\.', 'Oxford', 'The Bodleian Libraries, University of Oxford'),
    # Add/Or prefix indicates Cambridge
    (r'^Add\.', 'CUL', 'Cambridge University Library'),
    (r'^Or\.', 'CUL', 'Cambridge University Library'),
]

# Compile patterns for efficiency
COMPILED_LIBRARY_PATTERNS = [(re.compile(p, re.IGNORECASE), code, name)
                              for p, code, name in LIBRARY_PATTERNS]
COMPILED_COLLECTION_PATTERNS = [(re.compile(p, re.IGNORECASE), code, name)
                                 for p, code, name in COLLECTION_PATTERNS]


def detect_library(call_numbers: str) -> Tuple[str, str]:
    """
    Detect library code from call_numbers field.

    Args:
        call_numbers: The call_numbers value (pipe-separated variants)

    Returns:
        Tuple of (library_code, detection_method)
    """
    if not call_numbers:
        return ('', 'empty')

    # First, try to find explicit library name
    for pattern, code, name in COMPILED_LIBRARY_PATTERNS:
        if pattern.search(call_numbers):
            return (code, 'explicit')

    # If no explicit library, try collection-based detection
    # Check each variant in call_numbers
    variants = [v.strip() for v in call_numbers.split('|')]

    for variant in variants:
        for pattern, code, name in COMPILED_COLLECTION_PATTERNS:
            if pattern.match(variant):
                return (code, 'collection')

    return ('', 'unknown')


def analyze_csv(csv_path: str) -> Tuple[List[Dict], Counter, List[Dict]]:
    """
    Analyze the CSV file and extract library codes.

    Returns:
        Tuple of (all_records, library_counts, unmatched_records)
    """
    records = []
    library_counts = Counter()
    unmatched = []

    with open(csv_path, 'r', encoding='utf-8', errors='replace') as f:
        reader = csv.reader(f)
        header = next(reader)

        for row_num, row in enumerate(reader, start=2):
            if not row or len(row) < 3:
                continue

            sys_id = row[0].strip()
            oxford_part_id = row[1].strip() if len(row) > 1 else ''
            call_numbers = row[2].strip() if len(row) > 2 else ''
            title = row[6].strip() if len(row) > 6 else ''

            library_code, method = detect_library(call_numbers)

            record = {
                'sys_id': sys_id,
                'oxford_part_id': oxford_part_id,
                'call_numbers': call_numbers,
                'title': title,
                'library_code': library_code,
                'method': method,
                'row_num': row_num,
                'original_row': row,
            }
            records.append(record)

            if library_code:
                library_counts[library_code] += 1
            else:
                library_counts['UNKNOWN'] += 1
                unmatched.append(record)

    return records, library_counts, unmatched


def print_statistics(library_counts: Counter, total: int):
    """Print statistics about library distribution."""
    print("\n" + "="*60)
    print("LIBRARY DISTRIBUTION STATISTICS")
    print("="*60)

    # Sort by count descending
    for code, count in library_counts.most_common():
        pct = (count / total) * 100
        print(f"  {code:15s} : {count:>7,d} ({pct:5.2f}%)")

    print("-"*60)
    print(f"  {'TOTAL':15s} : {total:>7,d}")
    print("="*60 + "\n")


def print_unmatched_samples(unmatched: List[Dict], max_samples: int = 20):
    """Print sample of unmatched records for review."""
    if not unmatched:
        print("\nNo unmatched records!")
        return

    print(f"\n{'='*60}")
    print(f"UNMATCHED RECORDS ({len(unmatched)} total, showing first {min(len(unmatched), max_samples)})")
    print("="*60)

    for record in unmatched[:max_samples]:
        # Get first variant for display
        first_variant = record['call_numbers'].split('|')[0].strip()[:50]
        print(f"  Line {record['row_num']:>6d}: {first_variant}...")

    if len(unmatched) > max_samples:
        print(f"\n  ... and {len(unmatched) - max_samples} more")
    print("="*60 + "\n")


def write_updated_csv(records: List[Dict], original_path: str, output_path: str):
    """Write the updated CSV with library_code column."""

    # Read original header
    with open(original_path, 'r', encoding='utf-8', errors='replace') as f:
        reader = csv.reader(f)
        original_header = next(reader)

    # Create new header: insert library_code as column 3 (after call_numbers)
    # Original: system_number, oxford_part_id, call_numbers, ..., titles_non_placeholder
    # New:      system_number, oxford_part_id, call_numbers, library_code, ..., titles_non_placeholder
    new_header = original_header[:3] + ['library_code'] + original_header[3:]

    with open(output_path, 'w', encoding='utf-8', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(new_header)

        for record in records:
            # Insert library_code after column 2 (call_numbers)
            original_row = record['original_row']
            new_row = original_row[:3] + [record['library_code']] + original_row[3:]
            writer.writerow(new_row)

    print(f"\nWrote {len(records):,d} records to {output_path}")


def main():
    # Parse arguments
    dry_run = '--dry-run' in sys.argv
    interactive = '--interactive' in sys.argv

    # Find CSV path
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)
    csv_path = os.path.join(project_root, 'libraries.csv')

    if not os.path.exists(csv_path):
        print(f"Error: libraries.csv not found at {csv_path}")
        sys.exit(1)

    print(f"Analyzing {csv_path}...")
    print("This may take a moment for ~217K records...")

    # Analyze
    records, library_counts, unmatched = analyze_csv(csv_path)
    total = len(records)

    # Print statistics
    print_statistics(library_counts, total)

    # Print unmatched samples
    print_unmatched_samples(unmatched)

    # Dry run stops here
    if dry_run:
        print("Dry run complete. No files modified.")
        return

    # Interactive confirmation
    if interactive:
        print("\nReady to write updated CSV.")
        response = input("Proceed? (y/n): ").strip().lower()
        if response != 'y':
            print("Aborted.")
            return

    # Backup original
    backup_path = csv_path + '.bak'
    if not os.path.exists(backup_path):
        import shutil
        shutil.copy(csv_path, backup_path)
        print(f"Backed up original to {backup_path}")

    # Write updated CSV
    write_updated_csv(records, csv_path, csv_path)

    print("\nDone!")


if __name__ == '__main__':
    main()
