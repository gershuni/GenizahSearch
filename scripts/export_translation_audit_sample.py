# -*- coding: utf-8 -*-
"""
Export translation audit samples from all sidecar databases.

Generates stratified samples with automatic QC scoring for human review.
Outputs CSV files under reports/translation_audit/.

Usage:
    python scripts/export_translation_audit_sample.py
    python scripts/export_translation_audit_sample.py --full-qc   # Score ALL translations
    python scripts/export_translation_audit_sample.py --top-titles # Export top-frequency titles only
"""

import argparse
import csv
import logging
import os
import random
import sqlite3
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from shared.translation_qc import run_qc, summarize_qc_results

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
)
logger = logging.getLogger(__name__)

# =============================================================================
# Database Paths (same discovery as TranslationService)
# =============================================================================

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def find_db(relative_path: str) -> Optional[Path]:
    """Find a sidecar database, checking project root and AppData."""
    candidates = [
        PROJECT_ROOT / relative_path,
        Path(os.environ.get('LOCALAPPDATA', '')) / 'GenizahSearchPro' / 'data' / relative_path,
    ]
    for p in candidates:
        if p.exists():
            return p
    return None

OUTPUT_DIR = PROJECT_ROOT / 'reports' / 'translation_audit'

AUDIT_COLUMNS = [
    'dataset', 'record_id', 'field_name', 'direction',
    'source_text', 'translated_text', 'model_version', 'translated_at',
    'source_length', 'target_length', 'length_ratio',
    'qc_score', 'qc_flags', 'sample_reason',
    'review_status', 'review_notes',
]


# =============================================================================
# Sampling Helpers
# =============================================================================

def random_sample(rows: list, n: int, seed: int = 42) -> list:
    """Deterministic random sample."""
    rng = random.Random(seed)
    if len(rows) <= n:
        return rows
    return rng.sample(rows, n)


def make_audit_row(
    dataset: str, record_id: str, field_name: str, direction: str,
    source_text: str, translated_text: str,
    model_version: str = '', translated_at: str = '',
    sample_reason: str = 'random'
) -> dict:
    """Build a single audit row with QC scoring."""
    source_text = source_text or ''
    translated_text = translated_text or ''

    qc = run_qc(source_text, translated_text, direction)

    src_len = len(source_text)
    tgt_len = len(translated_text)
    ratio = round(tgt_len / src_len, 2) if src_len > 0 else 0

    return {
        'dataset': dataset,
        'record_id': str(record_id),
        'field_name': field_name,
        'direction': direction,
        'source_text': source_text[:2000],  # Cap for CSV readability
        'translated_text': translated_text[:2000],
        'model_version': model_version,
        'translated_at': translated_at,
        'source_length': src_len,
        'target_length': tgt_len,
        'length_ratio': ratio,
        'qc_score': qc['qc_score'],
        'qc_flags': '|'.join(qc['qc_flags']),
        'sample_reason': sample_reason,
        'review_status': '',
        'review_notes': '',
    }


# =============================================================================
# PGP Translations
# =============================================================================

def sample_pgp(db_path: Path, n_random: int = 200) -> Tuple[List[dict], Dict]:
    """Sample PGP translations with QC."""
    logger.info("Sampling PGP translations from %s", db_path)
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row

    rows = []

    # Get all translations joined with source descriptions
    cursor = conn.execute("""
        SELECT d.pgpid, d.description, t.description_he,
               t.translated_at, t.model_version
        FROM documents d
        JOIN pgp_translations t ON d.pgpid = t.pgpid
        WHERE t.description_he IS NOT NULL AND t.description_he != ''
    """)
    all_rows = cursor.fetchall()
    logger.info("  Total PGP translations: %d", len(all_rows))

    # Random sample
    sampled = random_sample(all_rows, n_random)
    for r in sampled:
        rows.append(make_audit_row(
            'pgp', r['pgpid'], 'description', 'en2he',
            r['description'] or '', r['description_he'],
            r['model_version'] or '', r['translated_at'] or '',
            'random'
        ))

    # Targeted: longest descriptions
    cursor = conn.execute("""
        SELECT d.pgpid, d.description, t.description_he,
               t.translated_at, t.model_version
        FROM documents d
        JOIN pgp_translations t ON d.pgpid = t.pgpid
        WHERE t.description_he IS NOT NULL
        ORDER BY LENGTH(d.description) DESC LIMIT 20
    """)
    for r in cursor:
        rows.append(make_audit_row(
            'pgp', r['pgpid'], 'description', 'en2he',
            r['description'] or '', r['description_he'],
            r['model_version'] or '', r['translated_at'] or '',
            'longest'
        ))

    # Targeted: shortest descriptions
    cursor = conn.execute("""
        SELECT d.pgpid, d.description, t.description_he,
               t.translated_at, t.model_version
        FROM documents d
        JOIN pgp_translations t ON d.pgpid = t.pgpid
        WHERE t.description_he IS NOT NULL AND LENGTH(d.description) > 5
        ORDER BY LENGTH(d.description) ASC LIMIT 20
    """)
    for r in cursor:
        rows.append(make_audit_row(
            'pgp', r['pgpid'], 'description', 'en2he',
            r['description'] or '', r['description_he'],
            r['model_version'] or '', r['translated_at'] or '',
            'shortest'
        ))

    # Targeted: rows that were previously NULL (retranslated)
    cursor = conn.execute("""
        SELECT d.pgpid, d.description, t.description_he,
               t.translated_at, t.model_version
        FROM documents d
        JOIN pgp_translations t ON d.pgpid = t.pgpid
        WHERE t.description_he IS NULL OR t.description_he = ''
    """)
    null_rows = cursor.fetchall()
    logger.info("  PGP null translations: %d", len(null_rows))

    conn.close()

    # Run QC summary on all sampled rows
    qc_results = [{'qc_score': r['qc_score'], 'qc_flags': r['qc_flags'].split('|') if r['qc_flags'] else [], 'flag_count': len(r['qc_flags'].split('|')) if r['qc_flags'] else 0} for r in rows]
    summary = summarize_qc_results(qc_results) if qc_results else {}

    return rows, summary


# =============================================================================
# FJMS Translations
# =============================================================================

def sample_fjms(db_path: Path, n_free_desc: int = 200, n_short: int = 100) -> Tuple[List[dict], Dict]:
    """Sample FJMS translations with QC."""
    logger.info("Sampling FJMS translations from %s", db_path)
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row

    rows = []

    # Check what field names exist
    cursor = conn.execute("""
        SELECT field_name, direction, COUNT(*) as cnt
        FROM fjms_translations
        GROUP BY field_name, direction
        ORDER BY cnt DESC
    """)
    field_stats = cursor.fetchall()
    logger.info("  FJMS translation fields:")
    for fs in field_stats:
        logger.info("    %s (%s): %d rows", fs['field_name'], fs['direction'], fs['cnt'])

    # Sample free descriptions (FreeDescription field)
    cursor = conn.execute("""
        SELECT id, alma_id, field_name, original_text, translated_text,
               direction, translated_at, model_version
        FROM fjms_translations
        WHERE field_name = 'FreeDescription'
    """)
    free_desc_rows = cursor.fetchall()
    sampled = random_sample(free_desc_rows, n_free_desc)
    for r in sampled:
        rows.append(make_audit_row(
            'fjms', f"{r['alma_id']}:{r['id']}", r['field_name'],
            r['direction'] or 'he2en',
            r['original_text'], r['translated_text'],
            r['model_version'] or '', r['translated_at'] or '',
            'random'
        ))

    # Sample short catalog fields (Title, AuthorText, etc.)
    cursor = conn.execute("""
        SELECT id, alma_id, field_name, original_text, translated_text,
               direction, translated_at, model_version
        FROM fjms_translations
        WHERE field_name != 'FreeDescription'
    """)
    short_rows = cursor.fetchall()
    sampled = random_sample(short_rows, n_short)
    for r in sampled:
        rows.append(make_audit_row(
            'fjms', f"{r['alma_id']}:{r['id']}", r['field_name'],
            r['direction'] or 'he2en',
            r['original_text'], r['translated_text'],
            r['model_version'] or '', r['translated_at'] or '',
            'random_short'
        ))

    # Targeted: longest FJMS texts
    cursor = conn.execute("""
        SELECT id, alma_id, field_name, original_text, translated_text,
               direction, translated_at, model_version
        FROM fjms_translations
        ORDER BY LENGTH(original_text) DESC LIMIT 20
    """)
    for r in cursor:
        rows.append(make_audit_row(
            'fjms', f"{r['alma_id']}:{r['id']}", r['field_name'],
            r['direction'] or 'he2en',
            r['original_text'], r['translated_text'],
            r['model_version'] or '', r['translated_at'] or '',
            'longest'
        ))

    conn.close()

    qc_results = [{'qc_score': r['qc_score'], 'qc_flags': r['qc_flags'].split('|') if r['qc_flags'] else [], 'flag_count': len(r['qc_flags'].split('|')) if r['qc_flags'] else 0} for r in rows]
    summary = summarize_qc_results(qc_results) if qc_results else {}

    return rows, summary


# =============================================================================
# Library Title Translations
# =============================================================================

def sample_titles(db_path: Path, n_random: int = 100, n_top: int = 100) -> Tuple[List[dict], Dict]:
    """Sample library title translations with QC."""
    logger.info("Sampling title translations from %s", db_path)
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row

    rows = []

    # HE→EN titles (hebrew_title → english_title via Dicta)
    cursor = conn.execute("""
        SELECT system_number, original_title, english_title, hebrew_title,
               english_title_he, source, translated_at
        FROM title_translations
        WHERE english_title IS NOT NULL AND english_title != ''
          AND source = 'dicta'
    """)
    dicta_rows = cursor.fetchall()
    logger.info("  Dicta HE→EN titles: %d", len(dicta_rows))

    sampled = random_sample(dicta_rows, n_random)
    for r in sampled:
        rows.append(make_audit_row(
            'titles', r['system_number'], 'english_title', 'he2en',
            r['original_title'] or r['hebrew_title'] or '',
            r['english_title'],
            'dictalm2.0', r['translated_at'] or '',
            'random'
        ))

    # EN→HE backfill titles
    cursor = conn.execute("""
        SELECT system_number, original_title, english_title, hebrew_title,
               english_title_he, source, translated_at
        FROM title_translations
        WHERE english_title_he IS NOT NULL AND english_title_he != ''
    """)
    en2he_rows = cursor.fetchall()
    logger.info("  EN→HE backfill titles: %d", len(en2he_rows))

    sampled = random_sample(en2he_rows, n_random)
    for r in sampled:
        rows.append(make_audit_row(
            'titles', r['system_number'], 'english_title_he', 'en2he',
            r['english_title'] or '', r['english_title_he'],
            'dictalm2.0', r['translated_at'] or '',
            'random_en2he'
        ))

    # Top frequency: find most-repeated english_title values
    cursor = conn.execute("""
        SELECT english_title, COUNT(*) as freq
        FROM title_translations
        WHERE english_title IS NOT NULL AND english_title != ''
          AND source = 'dicta'
        GROUP BY english_title
        ORDER BY freq DESC
        LIMIT ?
    """, (n_top,))
    top_titles = cursor.fetchall()
    logger.info("  Top %d repeated Dicta titles (by frequency)", len(top_titles))

    # For each top title, get one example row
    for tt in top_titles:
        cursor2 = conn.execute("""
            SELECT system_number, original_title, english_title, hebrew_title,
                   source, translated_at
            FROM title_translations
            WHERE english_title = ? AND source = 'dicta'
            LIMIT 1
        """, (tt['english_title'],))
        r = cursor2.fetchone()
        if r:
            rows.append(make_audit_row(
                'titles', r['system_number'], 'english_title', 'he2en',
                r['original_title'] or r['hebrew_title'] or '',
                r['english_title'],
                'dictalm2.0', r['translated_at'] or '',
                f'top_freq:{tt["freq"]}'
            ))

    conn.close()

    qc_results = [{'qc_score': r['qc_score'], 'qc_flags': r['qc_flags'].split('|') if r['qc_flags'] else [], 'flag_count': len(r['qc_flags'].split('|')) if r['qc_flags'] else 0} for r in rows]
    summary = summarize_qc_results(qc_results) if qc_results else {}

    return rows, summary


# =============================================================================
# Full QC Scan (all translations, summary only)
# =============================================================================

def full_qc_scan_pgp(db_path: Path) -> Dict:
    """Run QC on ALL PGP translations and return summary."""
    logger.info("Full QC scan: PGP translations...")
    conn = sqlite3.connect(str(db_path))
    cursor = conn.execute("""
        SELECT d.description, t.description_he
        FROM documents d
        JOIN pgp_translations t ON d.pgpid = t.pgpid
        WHERE t.description_he IS NOT NULL AND t.description_he != ''
    """)
    results = []
    count = 0
    for row in cursor:
        qc = run_qc(row[0] or '', row[1], 'en2he')
        results.append(qc)
        count += 1
        if count % 5000 == 0:
            logger.info("  PGP scanned: %d", count)
    conn.close()
    logger.info("  PGP total scanned: %d", count)
    return summarize_qc_results(results)


def full_qc_scan_fjms(db_path: Path) -> Dict:
    """Run QC on ALL FJMS translations and return summary."""
    logger.info("Full QC scan: FJMS translations...")
    conn = sqlite3.connect(str(db_path))
    cursor = conn.execute("""
        SELECT original_text, translated_text, direction
        FROM fjms_translations
    """)
    results = []
    count = 0
    for row in cursor:
        qc = run_qc(row[0], row[1], row[2] or 'he2en')
        results.append(qc)
        count += 1
        if count % 50000 == 0:
            logger.info("  FJMS scanned: %d", count)
    conn.close()
    logger.info("  FJMS total scanned: %d", count)
    return summarize_qc_results(results)


def full_qc_scan_titles(db_path: Path) -> Dict:
    """Run QC on ALL title translations and return summary."""
    logger.info("Full QC scan: title translations...")
    conn = sqlite3.connect(str(db_path))
    results = []
    count = 0

    # HE→EN
    cursor = conn.execute("""
        SELECT COALESCE(original_title, hebrew_title, ''), english_title
        FROM title_translations
        WHERE english_title IS NOT NULL AND english_title != ''
          AND source = 'dicta'
    """)
    for row in cursor:
        qc = run_qc(row[0], row[1], 'he2en')
        results.append(qc)
        count += 1

    # EN→HE
    cursor = conn.execute("""
        SELECT COALESCE(english_title, ''), english_title_he
        FROM title_translations
        WHERE english_title_he IS NOT NULL AND english_title_he != ''
    """)
    for row in cursor:
        qc = run_qc(row[0], row[1], 'en2he')
        results.append(qc)
        count += 1

    conn.close()
    logger.info("  Titles total scanned: %d", count)
    return summarize_qc_results(results)


# =============================================================================
# Top Titles Export (high-leverage review)
# =============================================================================

def export_top_titles(db_path: Path, top_n: int = 200) -> List[dict]:
    """Export top-frequency Dicta title translations for priority review."""
    logger.info("Exporting top %d repeated titles for review...", top_n)
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row

    cursor = conn.execute("""
        SELECT english_title,
               MIN(original_title) as sample_original,
               MIN(hebrew_title) as sample_hebrew,
               COUNT(*) as frequency,
               MIN(system_number) as sample_sys_id
        FROM title_translations
        WHERE english_title IS NOT NULL AND english_title != ''
          AND source = 'dicta'
        GROUP BY english_title
        ORDER BY frequency DESC
        LIMIT ?
    """, (top_n,))

    rows = []
    cumulative = 0
    for r in cursor:
        freq = r['frequency']
        cumulative += freq
        source = r['sample_original'] or r['sample_hebrew'] or ''
        target = r['english_title']

        qc = run_qc(source, target, 'he2en')

        rows.append({
            'english_title': target,
            'hebrew_source': source,
            'frequency': freq,
            'cumulative_rows': cumulative,
            'sample_sys_id': r['sample_sys_id'],
            'qc_score': qc['qc_score'],
            'qc_flags': '|'.join(qc['qc_flags']),
            'review_status': '',
            'corrected_title': '',
            'review_notes': '',
        })

    conn.close()
    logger.info("  Top titles exported: %d (covering %d rows)", len(rows), cumulative)
    return rows


# =============================================================================
# Write Helpers
# =============================================================================

def write_csv(rows: List[dict], filename: str, columns: List[str] = None):
    """Write rows to a CSV file in the output directory."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    filepath = OUTPUT_DIR / filename
    if not columns:
        columns = list(rows[0].keys()) if rows else []

    with open(filepath, 'w', newline='', encoding='utf-8-sig') as f:
        writer = csv.DictWriter(f, fieldnames=columns, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(rows)

    logger.info("Wrote %d rows to %s", len(rows), filepath)


def write_summary(summaries: Dict[str, Dict], filename: str = 'QC_SUMMARY.txt'):
    """Write a human-readable QC summary report."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    filepath = OUTPUT_DIR / filename

    lines = []
    lines.append(f"Translation QC Summary Report")
    lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append("=" * 60)

    for dataset, summary in summaries.items():
        lines.append(f"\n## {dataset}")
        lines.append(f"  Total: {summary.get('total', 0)}")
        lines.append(f"  Clean: {summary.get('clean', 0)}")
        lines.append(f"  Flagged: {summary.get('flagged', 0)} ({summary.get('flagged_pct', 0)}%)")
        lines.append(f"  Mean QC Score: {summary.get('mean_score', 0)}")
        lines.append(f"  Worst (score < 0.5): {summary.get('worst_count', 0)}")

        flag_dist = summary.get('flag_distribution', {})
        if flag_dist:
            lines.append("  Flag distribution:")
            for flag, count in flag_dist.items():
                lines.append(f"    {flag}: {count}")

    report = '\n'.join(lines)
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(report)
    logger.info("Wrote summary to %s", filepath)

    # Also print to console
    print("\n" + report)


# =============================================================================
# Main
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description='Export translation audit samples with QC')
    parser.add_argument('--full-qc', action='store_true',
                       help='Run QC on ALL translations (slower, summary only)')
    parser.add_argument('--top-titles', action='store_true',
                       help='Export top-frequency titles only (quick, high-leverage)')
    parser.add_argument('--seed', type=int, default=42,
                       help='Random seed for sampling')
    args = parser.parse_args()

    # Find databases
    pgp_db = find_db('pgp_data/pgp.db')
    fjms_db = find_db('fist_data/fjms_enrichment.db')
    titles_db = find_db('libraries_translations.db')

    print(f"\nDatabase discovery:")
    print(f"  PGP:    {pgp_db or 'NOT FOUND'}")
    print(f"  FJMS:   {fjms_db or 'NOT FOUND'}")
    print(f"  Titles: {titles_db or 'NOT FOUND'}")
    print()

    summaries = {}

    if args.top_titles:
        # Quick mode: just the top titles
        if titles_db:
            top_rows = export_top_titles(titles_db, top_n=200)
            write_csv(top_rows, 'top_titles_for_review.csv')
        else:
            print("ERROR: libraries_translations.db not found")
        return

    if args.full_qc:
        # Full scan mode: QC all translations, summary only
        if pgp_db:
            summaries['PGP (full)'] = full_qc_scan_pgp(pgp_db)
        if fjms_db:
            summaries['FJMS (full)'] = full_qc_scan_fjms(fjms_db)
        if titles_db:
            summaries['Titles (full)'] = full_qc_scan_titles(titles_db)
        write_summary(summaries)
        return

    # Default: stratified sample export
    all_rows = []

    if pgp_db:
        pgp_rows, pgp_summary = sample_pgp(pgp_db)
        all_rows.extend(pgp_rows)
        summaries['PGP (sample)'] = pgp_summary
        write_csv(pgp_rows, 'pgp_audit_sample.csv', AUDIT_COLUMNS)
    else:
        logger.warning("PGP database not found, skipping")

    if fjms_db:
        fjms_rows, fjms_summary = sample_fjms(fjms_db)
        all_rows.extend(fjms_rows)
        summaries['FJMS (sample)'] = fjms_summary
        write_csv(fjms_rows, 'fjms_audit_sample.csv', AUDIT_COLUMNS)
    else:
        logger.warning("FJMS database not found, skipping")

    if titles_db:
        title_rows, title_summary = sample_titles(titles_db)
        all_rows.extend(title_rows)
        summaries['Titles (sample)'] = title_summary
        write_csv(title_rows, 'titles_audit_sample.csv', AUDIT_COLUMNS)

        # Also export top titles for priority review
        top_rows = export_top_titles(titles_db)
        write_csv(top_rows, 'top_titles_for_review.csv')
    else:
        logger.warning("Titles database not found, skipping")

    # Combined export
    if all_rows:
        write_csv(all_rows, 'combined_audit_sample.csv', AUDIT_COLUMNS)

    # Write summary
    write_summary(summaries)

    print(f"\nOutput directory: {OUTPUT_DIR}")
    print(f"Total audit rows: {len(all_rows)}")
    print("Next steps:")
    print("  1. Review top_titles_for_review.csv first (highest leverage)")
    print("  2. Spot-check flagged rows in combined_audit_sample.csv")
    print("  3. Run --full-qc for comprehensive scoring of all translations")


if __name__ == '__main__':
    main()
