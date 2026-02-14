#!/usr/bin/env python3
"""
PGP Transcriptions Export Script

Exports transcriptions from Princeton Geniza Project (PGP) metadata
and links them to GenizahSearch system_numbers.

Input:
  - pgp_data/documents.csv (PGP document metadata with shelfmarks)
  - pgp_data/footnotes.csv (PGP footnotes containing transcriptions)
  - libraries.csv (GenizahSearch shelfmark → sys_id mapping)

Output:
  - pgp_data/transcriptions_linked.csv (transcriptions with sys_id linkage)
  - pgp_data/transcriptions_unmatched.csv (transcriptions without linkage)
  - pgp_data/export_report.txt (statistics and diagnostics)

Usage:
  python scripts/pgp_transcriptions_export.py
"""

import csv
import re
import os
from collections import defaultdict
from datetime import datetime
from pathlib import Path


def normalize_shelfmark(shelf: str) -> str:
    """
    Normalize a shelfmark for comparison.

    Handles variations like:
    - "Cambridge University Library Ms. T-S 13J35.3" → "t-s 13j35.3"
    - "T-S 13 J 15.3" → "t-s 13j15.3"
    - "Ms. Or. 1080 J 35" → "or.1080 j35"
    - "CUL Or.1080 J70" → "or.1080 j70"
    - "Bodl. MS heb. a 2/22" → "ms heb. a 2.22"
    """
    if not shelf:
        return ""

    # Remove common prefixes
    shelf = re.sub(r'^Cambridge University Library\s*', '', shelf, flags=re.IGNORECASE)
    shelf = re.sub(r'^The University of Manchester Library\s*', '', shelf, flags=re.IGNORECASE)
    shelf = re.sub(r'^Freer Gallery of Art,?\s*Smithsonian Institution\s*', '', shelf, flags=re.IGNORECASE)
    shelf = re.sub(r'^The Jewish Theological Seminary of America\s*', '', shelf, flags=re.IGNORECASE)
    shelf = re.sub(r'^CUL\s*', '', shelf, flags=re.IGNORECASE)
    shelf = re.sub(r'^Bodl\.?\s*', '', shelf, flags=re.IGNORECASE)
    shelf = re.sub(r'^AIU\s*', '', shelf, flags=re.IGNORECASE)  # AIU VII.D.69 → VII.D.69
    # Normalize em-dash to regular dash in AIU shelfmarks
    shelf = shelf.replace('–', '-').replace('—', '-')
    shelf = re.sub(r'^RNL\s*', '', shelf, flags=re.IGNORECASE)  # RNL Yevr → Yevr
    shelf = re.sub(r'^NLI\s*', '', shelf, flags=re.IGNORECASE)  # NLI 577.3/3 → 577.3/3
    shelf = re.sub(r'^HUC\s*', '', shelf, flags=re.IGNORECASE)  # HUC 1037 → 1037
    shelf = re.sub(r'^BL\s+', '', shelf, flags=re.IGNORECASE)  # BL OR 10126 → OR 10126
    # BL Or. 2570 → OR 2570 (remove period, uppercase) - only if followed by space+number (not Or.1080)
    shelf = re.sub(r'^Or\.\s+(\d+)$', r'OR \1', shelf, flags=re.IGNORECASE)
    shelf = re.sub(r'^Or\.\s+(\d+)\s', r'OR \1 ', shelf, flags=re.IGNORECASE)
    shelf = re.sub(r'^PER\s+', '', shelf, flags=re.IGNORECASE)  # PER H 130 → H 130 (Vienna)
    shelf = re.sub(r'^UPenn\s+', '', shelf, flags=re.IGNORECASE)  # UPenn E 16510 → E 16510

    # IOM (Institute of Oriental Manuscripts, St Petersburg) normalization
    # PGP: "IOM D 55.13" → FIST: "D 55/13"
    shelf = re.sub(r'^IOM\s+D\s*(\d+)\.(\d+)', r'D \1/\2', shelf, flags=re.IGNORECASE)

    # JRL (John Rylands Library) → Ms. A/B/C/L format
    # "JRL A 316" → "Ms. A 316", "JRL L 128" → "Ms. L 128"
    shelf = re.sub(r'^JRL\s+([ABCL])\s+(\d+)', r'Ms. \1 \2', shelf, flags=re.IGNORECASE)
    # JRL Gaster → Gaster (remove JRL prefix)
    # "JRL Gaster heb. ms 1760/18" → "Gaster heb. ms 1760/18"
    shelf = re.sub(r'^JRL\s+Gaster\s+', r'Gaster ', shelf, flags=re.IGNORECASE)
    # JRL P 213 → P 213 (remove JRL prefix)
    shelf = re.sub(r'^JRL\s+P\s+(\d+)', r'P \1', shelf, flags=re.IGNORECASE)
    # JRL AF 255 → AF  255 (FIST uses double space!)
    shelf = re.sub(r'^JRL\s+AF\s+(\d+)', r'AF  \1', shelf, flags=re.IGNORECASE)

    # JTS Schechter/Krengel → Ms. Schechter./Krengel. format
    # "JTS Schechter 4" → "Ms. Schechter.4", "JTS: Schechter 1" → "Ms. Schechter.1"
    shelf = re.sub(r'^JTS:?\s*Schechter\s+(\d+)', r'Schechter.\1', shelf, flags=re.IGNORECASE)
    shelf = re.sub(r'^JTS:?\s*Krengel\s+(\d+)', r'Krengel.\1', shelf, flags=re.IGNORECASE)
    # JTSA MS 4391 → MS 4391 (remove JTSA prefix)
    shelf = re.sub(r'^JTSA\s+', r'', shelf, flags=re.IGNORECASE)

    shelf = re.sub(r'^Ms\.?\s*', '', shelf, flags=re.IGNORECASE)

    # Normalize whitespace
    shelf = shelf.strip()
    shelf = re.sub(r'\s+', ' ', shelf)

    # Normalize T-S series formatting
    # "T-S 13 J 35" → "T-S 13J35"
    shelf = re.sub(r'T-S\s+(\d+)\s*J\s*(\d+)', r'T-S \1J\2', shelf, flags=re.IGNORECASE)

    # "T-S K 7" → "T-S K7"
    shelf = re.sub(r'T-S\s+([A-Z]+)\s+(\d+)', r'T-S \1\2', shelf, flags=re.IGNORECASE)

    # Normalize Or.1080 and Or.1081 variants
    # "Or. 1080 J 70" → "Or.1080 J70"
    shelf = re.sub(r'Or\.?\s*1080\s*J\s*(\d+)', r'Or.1080 J\1', shelf, flags=re.IGNORECASE)
    shelf = re.sub(r'Or\.?\s*1080\s+(\d+)', r'Or.1080 \1', shelf, flags=re.IGNORECASE)
    # "Or. 1081 2.25" → "Or.1081 2.25"
    shelf = re.sub(r'Or\.?\s*1081\s+', r'Or.1081 ', shelf, flags=re.IGNORECASE)

    # Normalize Bodleian: "heb. a 2/22" → "heb. a.2.22"
    # PGP: "heb. a 2/22" → libraries: "heb. a.2.22"
    shelf = re.sub(r'heb\.\s*([a-z])\s+(\d+)', r'heb. \1.\2', shelf, flags=re.IGNORECASE)
    shelf = shelf.replace('/', '.')

    # Normalize L-G (Lewis-Gibson): "L-G Ar. I.105" → "L-G Ar.I.105"
    # Remove space between type and Roman numeral
    # "L-G Ar. I.105" → "L-G Ar.I.105"
    # "L-G Misc. 58" → "L-G Misc .58" (FIST uses space before number!)
    # "L-G Lit.II.118" already has no space
    shelf = re.sub(r'L-G\s+(Ar)\.\s+([IVX]+)', r'L-G \1.\2', shelf, flags=re.IGNORECASE)
    shelf = re.sub(r'L-G\s+(Misc|Lit)\.\s*(\d+)', r'L-G \1 .\2', shelf, flags=re.IGNORECASE)

    # Normalize Yevr. (RNL): Various formats to standard "Yevr.-Arab. I 19"
    # Handle "RNL Yevr.-Arab I 19" → "Yevr.-Arab. I 19" (add period after Arab)
    # Note: RNL prefix was already removed above
    # "Yevr.-Arab I 86" → "Yevr.-Arab. I 86" (ensure period after Arab)
    shelf = re.sub(r'Yevr\.-Arab\s+([IVX]+)', r'Yevr.-Arab. \1', shelf, flags=re.IGNORECASE)
    # Also handle "Yevr. Arab." vs "Yevr.-Arab."
    shelf = re.sub(r'Yevr\.\s*Arab\.?\s+', r'Yevr.-Arab. ', shelf, flags=re.IGNORECASE)
    # Handle "Yevr. II C1" → "Yevr. II C 1" (add space before number in C/B series)
    shelf = re.sub(r'Yevr\.\s+([IVX]+)\s+([CB])(\d+)', r'Yevr. \1 \2 \3', shelf, flags=re.IGNORECASE)
    # Handle "Ms Yevr." → "Yevr." (remove "Ms" prefix)
    shelf = re.sub(r'^Ms\s+Yevr\.', r'Yevr.', shelf, flags=re.IGNORECASE)
    # Handle "Yevr. I B 19" → "Yevr. I B 19" (already correct format)

    # Normalize Mosseri: "Moss. V, 39.6" → "Moss. V,39.6" (remove space after comma)
    shelf = re.sub(r'(Moss\.\s*[IVX]+),\s+', r'\1,', shelf, flags=re.IGNORECASE)

    return shelf.lower()


def load_genizahsearch_shelfmarks(libraries_path: str, fist_supplement_path: str = None) -> dict:
    """
    Load GenizahSearch libraries.csv and create shelfmark → sys_id mapping.
    Optionally supplement with FIST shelfmarks for better coverage.

    Returns dict: normalized_shelfmark → system_number
    """
    gs_lookup = {}

    # Load main libraries.csv
    with open(libraries_path, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        header = next(reader)  # Skip header

        for row in reader:
            if len(row) < 3:
                continue

            sys_id = row[0]
            call_numbers = row[2]

            # Split pipe-separated variants and index all
            for variant in call_numbers.split('|'):
                normalized = normalize_shelfmark(variant)
                if normalized:
                    # Keep first match (most specific)
                    if normalized not in gs_lookup:
                        gs_lookup[normalized] = sys_id

    # Load FIST supplement if available
    if fist_supplement_path and os.path.exists(fist_supplement_path):
        with open(fist_supplement_path, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            fist_count = 0
            for row in reader:
                shelfmark = row.get('shelfmark', '')
                alma_id = row.get('alma_id', '')
                if shelfmark and alma_id:
                    normalized = normalize_shelfmark(shelfmark)
                    if normalized and normalized not in gs_lookup:
                        gs_lookup[normalized] = alma_id
                        fist_count += 1
        print(f"  Added {fist_count} shelfmarks from FIST supplement")

    return gs_lookup


def load_pgp_documents(documents_path: str) -> dict:
    """
    Load PGP documents.csv and create pgpid → document info mapping.

    Returns dict: pgpid → {shelfmark, type, tags, ...}
    """
    pgp_docs = {}

    with open(documents_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)

        for row in reader:
            # Handle BOM in first column
            pgpid = row.get('\ufeffpgpid') or row.get('pgpid')
            if pgpid:
                pgp_docs[pgpid] = {
                    'shelfmark': row.get('shelfmark', ''),
                    'type': row.get('type', ''),
                    'tags': row.get('tags', ''),
                    'description': row.get('description', ''),
                    'languages_primary': row.get('languages_primary', ''),
                    'url': row.get('url', ''),
                }

    return pgp_docs


def extract_transcriptions(footnotes_path: str) -> list:
    """
    Extract transcriptions from PGP footnotes.csv.

    Filters for:
    - doc_relation containing 'Edition' or 'Digital'
    - content length > 50 characters

    Returns list of dicts with transcription data
    """
    transcriptions = []

    with open(footnotes_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)

        for row in reader:
            # The document_id column contains the PGPID
            doc_id = row.get('document_id', '')
            doc_relation = row.get('doc_relation', '')
            content = row.get('content', '')

            # Filter for edition/transcription content
            if not ('Edition' in doc_relation or 'Digital' in doc_relation):
                continue

            # Filter for substantial content
            if not content or len(content) < 50:
                continue

            transcriptions.append({
                'pgpid': doc_id,
                'source': row.get('source', ''),
                'source_slug': row.get('source_slug', ''),
                'doc_relation': doc_relation,
                'location': row.get('location', ''),
                'url': row.get('url', ''),
                'content': content,
                'emendations': row.get('emendations', ''),
                'notes': row.get('notes', ''),
            })

    return transcriptions


def match_to_genizahsearch(shelfmark: str, gs_lookup: dict) -> tuple:
    """
    Try to match a PGP shelfmark to GenizahSearch sys_id.

    Handles multi-fragment shelfmarks like "T-S 13J35.3 + AIU VII.A.23"

    Returns: (sys_id or None, matched_part or None)
    """
    # Handle multi-fragment shelfmarks
    parts = [p.strip() for p in shelfmark.split('+')]

    for part in parts:
        normalized = normalize_shelfmark(part)
        if normalized in gs_lookup:
            return gs_lookup[normalized], part

    # Try without sub-part (e.g., "T-S 13J35" instead of "T-S 13J35.3")
    for part in parts:
        normalized = normalize_shelfmark(part)
        # Remove trailing .number
        base = re.sub(r'\.\d+$', '', normalized)
        if base in gs_lookup:
            return gs_lookup[base], part
        # Also try removing just trailing letter (e.g., .10a -> .10)
        base_no_letter = re.sub(r'([a-z])$', '', normalized)
        if base_no_letter in gs_lookup:
            return gs_lookup[base_no_letter], part

    return None, None


def export_transcriptions(
    libraries_path: str,
    documents_path: str,
    footnotes_path: str,
    output_dir: str,
    fist_supplement_path: str = None
):
    """
    Main export function.
    """
    print("=" * 60)
    print("PGP Transcriptions Export")
    print("=" * 60)
    print()

    # Create output directory
    os.makedirs(output_dir, exist_ok=True)

    # Load data
    print("Loading GenizahSearch shelfmarks...")
    gs_lookup = load_genizahsearch_shelfmarks(libraries_path, fist_supplement_path)
    print(f"  Loaded {len(gs_lookup):,} normalized shelfmarks")

    print("Loading PGP documents...")
    pgp_docs = load_pgp_documents(documents_path)
    print(f"  Loaded {len(pgp_docs):,} documents")

    print("Extracting transcriptions from footnotes...")
    transcriptions = extract_transcriptions(footnotes_path)
    print(f"  Found {len(transcriptions):,} transcription records")
    print()

    # Match and export
    linked = []
    unmatched = []
    stats = defaultdict(int)

    for trans in transcriptions:
        pgpid = trans['pgpid']
        doc_info = pgp_docs.get(pgpid, {})
        shelfmark = doc_info.get('shelfmark', '')

        sys_id, matched_part = match_to_genizahsearch(shelfmark, gs_lookup)

        record = {
            'sys_id': sys_id or '',
            'pgpid': pgpid,
            'shelfmark': shelfmark,
            'matched_part': matched_part or '',
            'doc_type': doc_info.get('type', ''),
            'languages': doc_info.get('languages_primary', ''),
            'source_scholar': trans['source'],
            'doc_relation': trans['doc_relation'],
            'content': trans['content'],
            'content_length': len(trans['content']),
            'pgp_url': doc_info.get('url', ''),
        }

        if sys_id:
            linked.append(record)
            stats['linked'] += 1
        else:
            unmatched.append(record)
            stats['unmatched'] += 1

        # Track by doc_relation
        stats[f"relation:{trans['doc_relation']}"] += 1

    # Count unique documents
    linked_pgpids = set(r['pgpid'] for r in linked)
    unmatched_pgpids = set(r['pgpid'] for r in unmatched) - linked_pgpids

    print("Matching results:")
    print(f"  Linked records: {stats['linked']:,}")
    print(f"  Unmatched records: {stats['unmatched']:,}")
    print(f"  Unique linked documents: {len(linked_pgpids):,}")
    print(f"  Unique unmatched documents: {len(unmatched_pgpids):,}")
    print()

    # Write linked transcriptions
    linked_path = os.path.join(output_dir, 'transcriptions_linked.csv')
    print(f"Writing {linked_path}...")

    fieldnames = ['sys_id', 'pgpid', 'shelfmark', 'matched_part', 'doc_type',
                  'languages', 'source_scholar', 'doc_relation', 'content_length',
                  'pgp_url', 'content']

    with open(linked_path, 'w', encoding='utf-8-sig', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(linked)

    print(f"  Wrote {len(linked):,} records")

    # Write unmatched transcriptions
    unmatched_path = os.path.join(output_dir, 'transcriptions_unmatched.csv')
    print(f"Writing {unmatched_path}...")

    with open(unmatched_path, 'w', encoding='utf-8-sig', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(unmatched)

    print(f"  Wrote {len(unmatched):,} records")

    # Write report
    report_path = os.path.join(output_dir, 'export_report.txt')
    print(f"Writing {report_path}...")

    with open(report_path, 'w', encoding='utf-8') as f:
        f.write("PGP Transcriptions Export Report\n")
        f.write("=" * 60 + "\n")
        f.write(f"Generated: {datetime.now().isoformat()}\n\n")

        f.write("Input Files:\n")
        f.write(f"  libraries.csv: {len(gs_lookup):,} shelfmarks\n")
        f.write(f"  documents.csv: {len(pgp_docs):,} documents\n")
        f.write(f"  footnotes.csv: {len(transcriptions):,} transcription records\n\n")

        f.write("Matching Results:\n")
        f.write(f"  Linked records: {stats['linked']:,}\n")
        f.write(f"  Unmatched records: {stats['unmatched']:,}\n")
        f.write(f"  Match rate: {stats['linked']/len(transcriptions)*100:.1f}%\n\n")

        f.write("Unique Documents:\n")
        f.write(f"  Linked: {len(linked_pgpids):,}\n")
        f.write(f"  Unmatched: {len(unmatched_pgpids):,}\n\n")

        f.write("By doc_relation:\n")
        for key, count in sorted(stats.items()):
            if key.startswith('relation:'):
                f.write(f"  {key[9:]}: {count:,}\n")

        # Sample unmatched shelfmarks for debugging
        f.write("\n\nSample Unmatched Shelfmarks (first 50):\n")
        f.write("-" * 60 + "\n")
        seen_shelfs = set()
        for record in unmatched[:200]:
            shelf = record['shelfmark']
            if shelf and shelf not in seen_shelfs:
                seen_shelfs.add(shelf)
                f.write(f"  {shelf}\n")
                if len(seen_shelfs) >= 50:
                    break

    print()
    print("Export complete!")
    print(f"  Linked: {linked_path}")
    print(f"  Unmatched: {unmatched_path}")
    print(f"  Report: {report_path}")

    return {
        'linked_count': len(linked),
        'unmatched_count': len(unmatched),
        'linked_docs': len(linked_pgpids),
        'unmatched_docs': len(unmatched_pgpids),
    }


def main():
    # Determine paths relative to script location
    script_dir = Path(__file__).parent
    project_dir = script_dir.parent

    libraries_path = project_dir / 'libraries.csv'
    fist_supplement_path = project_dir / 'pgp_data' / 'fist_shelfmarks_supplement.csv'
    documents_path = project_dir / 'pgp_data' / 'documents.csv'
    footnotes_path = project_dir / 'pgp_data' / 'footnotes.csv'
    output_dir = project_dir / 'pgp_data'

    # Verify input files exist
    for path in [libraries_path, documents_path, footnotes_path]:
        if not path.exists():
            print(f"ERROR: Input file not found: {path}")
            return 1

    export_transcriptions(
        str(libraries_path),
        str(documents_path),
        str(footnotes_path),
        str(output_dir),
        str(fist_supplement_path) if fist_supplement_path.exists() else None
    )

    return 0


if __name__ == '__main__':
    exit(main())
