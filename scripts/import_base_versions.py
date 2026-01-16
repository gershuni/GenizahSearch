#!/usr/bin/env python3
"""
Import Base Versions Script

Imports V0.7 and V0.8 transcriptions from text files into the TranscriptionVersion table.
This allows users to switch between different base versions and user corrections.

Usage:
    python scripts/import_base_versions.py [--v08-only] [--v07-only] [--limit N] [--dry-run]
"""
import sys
import os
import re
import argparse
from typing import Optional, Tuple

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.models.database import init_db, SessionLocal
from backend.models.transcription_version import VersionSource
from backend.services.version_service import VersionService
from backend.schemas.version import ImportVersionRequest


# Default paths
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_V08_PATH = os.path.join(BASE_DIR, "Transcriptions.txt")
DEFAULT_V07_PATH = os.path.join(BASE_DIR, "Transcriptions_V0.7.txt")


def parse_header(header: str) -> Tuple[Optional[str], int]:
    """
    Parse a header line to extract sys_id and page_num.

    Args:
        header: The header line (e.g., "==> 990030907670205171_IE..._P000001_FL... <==")

    Returns:
        Tuple of (sys_id, page_num)
    """
    # Extract sys_id (18-digit number starting with 99)
    match = re.search(r'(99\d{16,})', header)
    if not match:
        return None, 1

    sys_id = match.group(1)

    # Try to find page number in different formats:
    # Format 1: _P000001_ (embedded in ID)
    p_match = re.search(r'_P(\d{6})_', header)
    if p_match:
        return sys_id, int(p_match.group(1))

    # Format 2: | 001 <== (at end after pipe)
    p_match = re.search(r'\|\s*(\d{3})\s*(?:<==|$)', header)
    if p_match:
        return sys_id, int(p_match.group(1))

    # Format 3: | FL123 | 001 <== (with FL prefix)
    p_match = re.search(r'\|\s*FL\d+\s*\|\s*(\d{3})\s*(?:<==|$)', header)
    if p_match:
        return sys_id, int(p_match.group(1))

    # Format 4: Simple number after last pipe
    p_match = re.search(r'\|\s*(\d{1,3})\s*(?:<==|$)', header)
    if p_match:
        return sys_id, int(p_match.group(1))

    return sys_id, 1


def import_file(
    filepath: str,
    source: VersionSource,
    limit: int = None,
    dry_run: bool = False,
    progress_interval: int = 100
) -> Tuple[int, int, int]:
    """
    Import transcriptions from a file.

    Args:
        filepath: Path to the transcriptions file
        source: VersionSource (V07 or V08)
        limit: Optional limit on number of pages to import
        dry_run: If True, don't actually import
        progress_interval: How often to print progress

    Returns:
        Tuple of (imported_count, skipped_count, error_count)
    """
    if not os.path.exists(filepath):
        print(f"File not found: {filepath}")
        return 0, 0, 0

    label = source.value
    is_v08 = source == VersionSource.V08
    sep_prefix = "==>" if is_v08 else "###"

    print(f"\nImporting {label} from: {filepath}")

    db = SessionLocal()
    imported = 0
    skipped = 0
    errors = 0

    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            current_id = None
            current_header = None
            current_text = []

            for line_num, line in enumerate(f, 1):
                line = line.strip()
                is_separator = line.startswith(sep_prefix)

                if is_separator:
                    # Process previous page
                    if current_id and current_text:
                        sys_id, page_num = parse_header(current_header)

                        if sys_id:
                            content = "\n".join(current_text)

                            if dry_run:
                                print(f"  [DRY-RUN] Would import: {sys_id} p.{page_num} ({len(content)} chars)")
                                imported += 1
                            else:
                                try:
                                    data = ImportVersionRequest(
                                        sys_id=sys_id,
                                        page_num=page_num,
                                        content=content,
                                        source=source
                                    )
                                    version, error = VersionService.import_base_version(db, data)

                                    if error:
                                        print(f"  Error importing {sys_id} p.{page_num}: {error}")
                                        errors += 1
                                    else:
                                        imported += 1
                                except Exception as e:
                                    print(f"  Exception importing {sys_id} p.{page_num}: {e}")
                                    errors += 1
                        else:
                            skipped += 1

                        if limit and imported >= limit:
                            print(f"  Reached limit of {limit} imports")
                            break

                        if imported % progress_interval == 0 and imported > 0:
                            print(f"  Progress: {imported} imported, {skipped} skipped, {errors} errors")

                    # Start new page
                    if is_v08:
                        current_header = line.replace("==>", "").replace("<==", "").strip()
                    else:
                        current_header = line

                    # Extract unique ID
                    match = re.search(r'(99\d{16,})', line)
                    current_id = match.group(1) if match else None
                    current_text = []
                else:
                    current_text.append(line)

            # Process last page
            if current_id and current_text and (not limit or imported < limit):
                sys_id, page_num = parse_header(current_header)

                if sys_id:
                    content = "\n".join(current_text)

                    if dry_run:
                        print(f"  [DRY-RUN] Would import: {sys_id} p.{page_num} ({len(content)} chars)")
                        imported += 1
                    else:
                        try:
                            data = ImportVersionRequest(
                                sys_id=sys_id,
                                page_num=page_num,
                                content=content,
                                source=source
                            )
                            version, error = VersionService.import_base_version(db, data)

                            if error:
                                errors += 1
                            else:
                                imported += 1
                        except Exception as e:
                            errors += 1

        print(f"\n{label} Import complete:")
        print(f"  - Imported: {imported}")
        print(f"  - Skipped: {skipped}")
        print(f"  - Errors: {errors}")

        return imported, skipped, errors

    finally:
        db.close()


def main():
    parser = argparse.ArgumentParser(description='Import V0.7/V0.8 base versions')
    parser.add_argument('--v08-only', action='store_true', help='Only import V0.8')
    parser.add_argument('--v07-only', action='store_true', help='Only import V0.7')
    parser.add_argument('--limit', type=int, help='Limit number of pages to import')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be imported without importing')
    parser.add_argument('--v08-path', type=str, help='Path to V0.8 file (default: Transcriptions.txt)')
    parser.add_argument('--v07-path', type=str, help='Path to V0.7 file (default: Transcriptions_V0.7.txt)')
    args = parser.parse_args()

    print("=" * 60)
    print("BASE VERSION IMPORT SCRIPT")
    print("=" * 60)

    if args.dry_run:
        print("\n*** DRY RUN MODE - No changes will be made ***\n")

    # Initialize database
    print("Initializing database...")
    init_db()

    # Determine file paths
    v08_path = args.v08_path or DEFAULT_V08_PATH
    v07_path = args.v07_path or DEFAULT_V07_PATH

    total_imported = 0
    total_skipped = 0
    total_errors = 0

    # Import V0.8 (default source)
    if not args.v07_only:
        imported, skipped, errors = import_file(
            v08_path,
            VersionSource.V08,
            limit=args.limit,
            dry_run=args.dry_run
        )
        total_imported += imported
        total_skipped += skipped
        total_errors += errors

    # Import V0.7
    if not args.v08_only:
        imported, skipped, errors = import_file(
            v07_path,
            VersionSource.V07,
            limit=args.limit,
            dry_run=args.dry_run
        )
        total_imported += imported
        total_skipped += skipped
        total_errors += errors

    print("\n" + "=" * 60)
    print("IMPORT SUMMARY")
    print("=" * 60)
    print(f"Total imported: {total_imported}")
    print(f"Total skipped: {total_skipped}")
    print(f"Total errors: {total_errors}")

    if args.dry_run:
        print("\n*** DRY RUN - No changes were made ***")

    return 0 if total_errors == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
