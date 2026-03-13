#!/usr/bin/env python3
"""
Extract English titles from bilingual libraries.csv records.

Parses the semicolon-delimited titles_non_placeholder field (MARC 245+246)
to separate English and Hebrew title components. Stores results in
libraries_translations.db SQLite sidecar.

Phase A of the Translation Master Plan — no API calls needed.
"""

import csv
import re
import sqlite3
import sys
import os
from pathlib import Path
from datetime import datetime, timezone

PROJECT_ROOT = Path(__file__).resolve().parent.parent
LIBRARIES_CSV = PROJECT_ROOT / "libraries.csv"
OUTPUT_DB = PROJECT_ROOT / "libraries_translations.db"

# Hebrew Unicode range
HEB_RE = re.compile(r'[\u0590-\u05FF]')
LAT_RE = re.compile(r'[A-Za-z]')


def classify_part(text: str) -> str:
    """Classify a semicolon-delimited part as 'hebrew', 'english', or 'mixed'."""
    has_heb = bool(HEB_RE.search(text))
    has_lat = bool(LAT_RE.search(text))
    if has_heb and has_lat:
        return 'mixed'
    elif has_heb:
        return 'hebrew'
    elif has_lat:
        return 'english'
    return 'other'


def extract_english_from_mixed(text: str) -> str | None:
    """Try to extract English runs from mixed Hebrew/English text.

    Handles patterns like:
      Piyyut: "יונת אלם קבעה מעונה"[as no. 23 c]
      Arabic Tafsir: Genesis 18:24 – 20:6[It seems to be...]
    """
    # Split on Hebrew runs and collect English segments
    # Remove Hebrew portions (including surrounding quotes/brackets)
    parts = re.split(r'[\u0590-\u05FF\u0600-\u06FF"״\'׳]+', text)
    english_parts = []
    for p in parts:
        p = p.strip(' ,;:[]().')
        if p and LAT_RE.search(p) and len(p) > 2:
            english_parts.append(p)
    if english_parts:
        return ' '.join(english_parts)
    return None


def extract_title_parts(title: str) -> tuple[str | None, str | None]:
    """Extract English and Hebrew title parts from a semicolon-delimited title.

    Returns (english_title, hebrew_title).
    The title field typically looks like:
      פיוט;תפילה וברכות. ; Piyyut ; פיוט
      פרשנות מקרא. ; Biblical Exegesis: Numbers Qorah ; פרשנות מקרא: במדבר קרח
    """
    if not title or not title.strip():
        return None, None

    title = title.strip()

    # Split on ' ; ' (space-semicolon-space) which separates MARC fields
    # Note: ';' without spaces is used within Hebrew subject lists
    # (e.g., "גלוסאר למשנה;פירושי תלמוד בבלי;תפסיר אלפאט' אלמשנה")
    parts = re.split(r' ; ', title)

    english_parts = []
    pure_hebrew_parts = []  # Parts with only Hebrew characters
    mixed_hebrew_parts = []  # Mixed parts kept as Hebrew fallback
    mixed_english = []

    for part in parts:
        part = part.strip()
        if not part:
            continue

        cls = classify_part(part)
        if cls == 'english':
            # Skip trivial parts like "paper." or "verso"
            if part.rstrip('.') in ('paper', 'verso', 'recto'):
                continue
            english_parts.append(part)
        elif cls == 'hebrew':
            pure_hebrew_parts.append(part)
        elif cls == 'mixed':
            extracted = extract_english_from_mixed(part)
            if extracted:
                mixed_english.append(extracted)
            # Also keep the Hebrew portions as fallback
            heb_only = re.sub(r'[^\u0590-\u05FF\s,.:;()\[\]]+', '', part).strip()
            if heb_only and len(heb_only) > 2:
                mixed_hebrew_parts.append(part)

    english = None
    hebrew = None

    if english_parts:
        english = ' ; '.join(english_parts)
    elif mixed_english:
        english = ' ; '.join(mixed_english)

    all_hebrew = pure_hebrew_parts + mixed_hebrew_parts
    if all_hebrew:
        if english:
            # Bilingual: pick the LONGEST pure-Hebrew part as counterpart to English.
            # Only fall back to mixed parts if no pure Hebrew exists.
            # (prevents picking "Piyyut (Pesah): ..." over "פיוט" when mixed is longer)
            candidates = pure_hebrew_parts if pure_hebrew_parts else mixed_hebrew_parts
            hebrew = max(candidates, key=len)
        else:
            # Pure Hebrew: keep full original title (semicolons are within-language)
            hebrew = title

    return english, hebrew


def create_db(db_path: Path) -> sqlite3.Connection:
    """Create the libraries_translations.db with schema."""
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS title_translations (
            system_number TEXT PRIMARY KEY,
            original_title TEXT,
            english_title TEXT,
            hebrew_title TEXT,
            source TEXT NOT NULL DEFAULT 'extracted',
            translated_at TEXT
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_tt_english
        ON title_translations(english_title) WHERE english_title IS NOT NULL
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS meta (
            key TEXT PRIMARY KEY,
            value TEXT
        )
    """)
    conn.commit()
    return conn


def main():
    if not LIBRARIES_CSV.exists():
        print(f"ERROR: {LIBRARIES_CSV} not found")
        sys.exit(1)

    print(f"Reading {LIBRARIES_CSV}...")

    rows = []
    stats = {
        'total': 0, 'has_title': 0, 'no_title': 0,
        'english_extracted': 0, 'english_from_mixed': 0,
        'hebrew_only': 0, 'english_only': 0, 'already_bilingual': 0,
    }

    with open(LIBRARIES_CSV, encoding='utf-8') as f:
        reader = csv.reader(f)
        header = next(reader)

        for row in reader:
            stats['total'] += 1
            sys_num = row[0]
            title = row[7] if len(row) > 7 else ''

            if not title.strip():
                stats['no_title'] += 1
                continue

            stats['has_title'] += 1
            title = title.strip()

            english, hebrew = extract_title_parts(title)

            # Classify for stats
            has_heb = bool(HEB_RE.search(title))
            has_lat = bool(LAT_RE.search(title))

            if english and has_heb:
                stats['already_bilingual'] += 1
            elif english and not has_heb:
                stats['english_only'] += 1
            elif not english and has_heb:
                stats['hebrew_only'] += 1

            if english:
                stats['english_extracted'] += 1

            # Store all titled records (even Hebrew-only — placeholder for Dicta later)
            source = 'extracted' if english else 'pending_dicta'
            rows.append((sys_num, title, english, hebrew or title, source))

    print(f"\nStats:")
    print(f"  Total records:       {stats['total']:,}")
    print(f"  Has title:           {stats['has_title']:,}")
    print(f"  No title:            {stats['no_title']:,}")
    print(f"  English extracted:   {stats['english_extracted']:,}")
    print(f"  Hebrew-only (Dicta): {stats['hebrew_only']:,}")
    print(f"  English-only:        {stats['english_only']:,}")

    # Write to DB
    print(f"\nWriting {len(rows):,} rows to {OUTPUT_DB}...")

    conn = create_db(OUTPUT_DB)
    now = datetime.now(timezone.utc).isoformat()

    conn.execute("DELETE FROM title_translations")  # Fresh build

    batch_size = 5000
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        conn.executemany(
            """INSERT OR REPLACE INTO title_translations
               (system_number, original_title, english_title, hebrew_title, source, translated_at)
               VALUES (?, ?, ?, ?, ?, ?)""",
            [(r[0], r[1], r[2], r[3], r[4], now if r[2] else None) for r in batch]
        )
        conn.commit()
        if (i + batch_size) % 50000 == 0:
            print(f"  Written {i + batch_size:,} rows...")

    conn.commit()

    # Write meta
    conn.execute("INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)",
                 ('version', '1.0.0'))
    conn.execute("INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)",
                 ('created_at', now))
    conn.execute("INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)",
                 ('total_records', str(len(rows))))
    conn.execute("INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)",
                 ('english_extracted', str(stats['english_extracted'])))
    conn.execute("INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)",
                 ('pending_dicta', str(stats['hebrew_only'])))
    conn.commit()

    # Verify
    count = conn.execute("SELECT COUNT(*) FROM title_translations").fetchone()[0]
    eng_count = conn.execute(
        "SELECT COUNT(*) FROM title_translations WHERE english_title IS NOT NULL"
    ).fetchone()[0]
    pending = conn.execute(
        "SELECT COUNT(*) FROM title_translations WHERE source = 'pending_dicta'"
    ).fetchone()[0]

    print(f"\nVerification:")
    print(f"  Total rows in DB:    {count:,}")
    print(f"  With English title:  {eng_count:,}")
    print(f"  Pending Dicta:       {pending:,}")

    # Sample output
    print(f"\nSample extractions:")
    for row in conn.execute(
        "SELECT system_number, english_title, hebrew_title FROM title_translations "
        "WHERE english_title IS NOT NULL ORDER BY RANDOM() LIMIT 5"
    ):
        print(f"  {row[0]}: EN={row[1][:60]}  |  HE={row[2][:60]}")

    conn.close()

    db_size = OUTPUT_DB.stat().st_size / 1024 / 1024
    print(f"\nDone! {OUTPUT_DB.name}: {db_size:.1f} MB")


if __name__ == '__main__':
    main()
