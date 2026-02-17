#!/usr/bin/env python3
"""
Export PGP reference data from Supabase to a local SQLite sidecar database.

Reads from Supabase PostgreSQL (via REST API) and produces pgp_data/pgp.db
with the following tables:
  - documents:          PGP document metadata (~35,839 rows)
  - document_sources:   Edition/translation content (~9,364 rows)
  - document_footnotes: Footnote references (~22,757 rows)
  - document_fragments: Fragment-to-document links (~36,155 rows)
  - meta:               Version and build metadata

This is the data foundation for the local-data architecture (v6.0.0).
Replaces live Supabase queries with a local SQLite sidecar for read-only
PGP reference data.
"""

import json
import os
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
from supabase import create_client

VERSION = "1.0.0"
PAGE_SIZE = 1000


def serialize_json(value):
    """Serialize a Python object (from Supabase JSONB) to deterministic JSON TEXT.

    Returns None for None values (preserving SQL NULL).
    Uses sorted keys and compact format for deterministic, space-efficient output.
    """
    if value is None:
        return None
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"))


def fetch_all_rows(client, table_name, order_by="id"):
    """Fetch all rows from a Supabase table using .range() pagination.

    PostgREST returns max 1000 rows per request, so we paginate until
    we get an empty or partial page.
    """
    all_records = []
    offset = 0

    while True:
        response = (
            client.table(table_name)
            .select("*")
            .order(order_by)
            .range(offset, offset + PAGE_SIZE - 1)
            .execute()
        )

        if not response.data:
            break

        all_records.extend(response.data)
        if len(response.data) < PAGE_SIZE:
            break
        offset += PAGE_SIZE

    return all_records


def export_documents(client, cursor):
    """Export documents table from Supabase to SQLite."""
    print("Exporting documents...")
    start = time.time()

    cursor.execute("DROP TABLE IF EXISTS documents")
    cursor.execute("""
        CREATE TABLE documents (
            pgpid INTEGER PRIMARY KEY,
            shelfmark_combined TEXT,
            document_type TEXT,
            tags TEXT,
            doc_date_original TEXT,
            doc_date_standard TEXT,
            doc_date_calendar TEXT,
            inferred_date_display TEXT,
            inferred_date_standard TEXT,
            inferred_date_rationale TEXT,
            inferred_date_notes TEXT,
            description TEXT,
            transcription TEXT,
            transcription_source TEXT,
            languages_primary TEXT,
            languages_secondary TEXT,
            language_note TEXT,
            scholarship_records TEXT,
            shelfmarks_historic TEXT,
            has_transcription INTEGER,
            has_translation INTEGER,
            input_by TEXT,
            pgp_url TEXT,
            created_at TEXT
        )
    """)

    rows = fetch_all_rows(client, "documents", order_by="pgpid")

    batch = []
    for row in rows:
        batch.append((
            row.get("pgpid"),
            row.get("shelfmark_combined"),
            row.get("document_type"),
            serialize_json(row.get("tags")),
            row.get("doc_date_original"),
            row.get("doc_date_standard"),
            row.get("doc_date_calendar"),
            row.get("inferred_date_display"),
            row.get("inferred_date_standard"),
            row.get("inferred_date_rationale"),
            row.get("inferred_date_notes"),
            row.get("description"),
            row.get("transcription"),
            row.get("transcription_source"),
            row.get("languages_primary"),
            row.get("languages_secondary"),
            row.get("language_note"),
            row.get("scholarship_records"),
            row.get("shelfmarks_historic"),
            row.get("has_transcription"),
            row.get("has_translation"),
            row.get("input_by"),
            row.get("pgp_url"),
            row.get("created_at"),
        ))
        if len(batch) >= PAGE_SIZE:
            cursor.executemany(
                "INSERT INTO documents VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                batch,
            )
            batch = []

    if batch:
        cursor.executemany(
            "INSERT INTO documents VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            batch,
        )

    cursor.execute("CREATE INDEX idx_doc_type ON documents(document_type)")
    cursor.connection.commit()

    elapsed = time.time() - start
    count = len(rows)
    print(f"  documents: {count:,} rows ({elapsed:.1f}s)")
    return count


def export_sources(client, cursor):
    """Export document_sources table from Supabase to SQLite."""
    print("Exporting document_sources...")
    start = time.time()

    cursor.execute("DROP TABLE IF EXISTS document_sources")
    cursor.execute("""
        CREATE TABLE document_sources (
            id INTEGER PRIMARY KEY,
            pgpid INTEGER NOT NULL,
            source_scholar TEXT NOT NULL,
            doc_relation TEXT NOT NULL,
            language TEXT,
            content TEXT NOT NULL,
            content_length INTEGER,
            source_url TEXT,
            notes TEXT,
            sequence_order INTEGER DEFAULT 1,
            sections TEXT,
            source_language TEXT,
            source_direction TEXT,
            created_at TEXT
        )
    """)

    rows = fetch_all_rows(client, "document_sources", order_by="id")

    batch = []
    for row in rows:
        batch.append((
            row.get("id"),
            row.get("pgpid"),
            row.get("source_scholar"),
            row.get("doc_relation"),
            row.get("language"),
            row.get("content"),
            row.get("content_length"),
            row.get("source_url"),
            row.get("notes"),
            row.get("sequence_order"),
            serialize_json(row.get("sections")),
            row.get("source_language"),
            row.get("source_direction"),
            row.get("created_at"),
        ))
        if len(batch) >= PAGE_SIZE:
            cursor.executemany(
                "INSERT INTO document_sources VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                batch,
            )
            batch = []

    if batch:
        cursor.executemany(
            "INSERT INTO document_sources VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            batch,
        )

    cursor.execute("CREATE INDEX idx_sources_pgpid ON document_sources(pgpid)")
    cursor.execute(
        "CREATE INDEX idx_sources_relation ON document_sources(pgpid, doc_relation)"
    )
    cursor.connection.commit()

    elapsed = time.time() - start
    count = len(rows)
    print(f"  document_sources: {count:,} rows ({elapsed:.1f}s)")
    return count


def export_footnotes(client, cursor):
    """Export document_footnotes table from Supabase to SQLite."""
    print("Exporting document_footnotes...")
    start = time.time()

    cursor.execute("DROP TABLE IF EXISTS document_footnotes")
    cursor.execute("""
        CREATE TABLE document_footnotes (
            id INTEGER PRIMARY KEY,
            pgpid INTEGER NOT NULL,
            source TEXT NOT NULL,
            source_slug TEXT,
            doc_relation TEXT NOT NULL,
            location TEXT,
            url TEXT,
            notes TEXT,
            content TEXT,
            content_length INTEGER,
            created_at TEXT
        )
    """)

    rows = fetch_all_rows(client, "document_footnotes", order_by="id")

    batch = []
    for row in rows:
        batch.append((
            row.get("id"),
            row.get("pgpid"),
            row.get("source"),
            row.get("source_slug"),
            row.get("doc_relation"),
            row.get("location"),
            row.get("url"),
            row.get("notes"),
            row.get("content"),
            row.get("content_length"),
            row.get("created_at"),
        ))
        if len(batch) >= PAGE_SIZE:
            cursor.executemany(
                "INSERT INTO document_footnotes VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                batch,
            )
            batch = []

    if batch:
        cursor.executemany(
            "INSERT INTO document_footnotes VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            batch,
        )

    cursor.execute(
        "CREATE INDEX idx_footnotes_pgpid ON document_footnotes(pgpid)"
    )
    cursor.execute(
        "CREATE INDEX idx_footnotes_relation ON document_footnotes(pgpid, doc_relation)"
    )
    cursor.connection.commit()

    elapsed = time.time() - start
    count = len(rows)
    print(f"  document_footnotes: {count:,} rows ({elapsed:.1f}s)")
    return count


def export_fragments(client, cursor):
    """Export document_fragments table from Supabase to SQLite."""
    print("Exporting document_fragments...")
    start = time.time()

    cursor.execute("DROP TABLE IF EXISTS document_fragments")
    cursor.execute("""
        CREATE TABLE document_fragments (
            id INTEGER PRIMARY KEY,
            document_id INTEGER NOT NULL,
            sys_id TEXT NOT NULL,
            shelfmark TEXT,
            sequence_order INTEGER DEFAULT 1,
            page_info TEXT,
            collection TEXT,
            library TEXT,
            library_abbrev TEXT,
            fragment_url TEXT,
            iiif_url TEXT,
            created_at TEXT
        )
    """)

    rows = fetch_all_rows(client, "document_fragments", order_by="id")

    batch = []
    for row in rows:
        batch.append((
            row.get("id"),
            row.get("document_id"),
            row.get("sys_id"),
            row.get("shelfmark"),
            row.get("sequence_order"),
            row.get("page_info"),
            row.get("collection"),
            row.get("library"),
            row.get("library_abbrev"),
            row.get("fragment_url"),
            row.get("iiif_url"),
            row.get("created_at"),
        ))
        if len(batch) >= PAGE_SIZE:
            cursor.executemany(
                "INSERT INTO document_fragments VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                batch,
            )
            batch = []

    if batch:
        cursor.executemany(
            "INSERT INTO document_fragments VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            batch,
        )

    cursor.execute(
        "CREATE INDEX idx_fragments_sys_id ON document_fragments(sys_id)"
    )
    cursor.execute(
        "CREATE INDEX idx_fragments_document_id ON document_fragments(document_id)"
    )

    # Attempt unique index for data integrity; fall back to non-unique
    # if Supabase has duplicate (document_id, sys_id) pairs
    try:
        cursor.execute(
            "CREATE UNIQUE INDEX idx_fragments_unique ON document_fragments(document_id, sys_id)"
        )
    except sqlite3.IntegrityError:
        print("  WARNING: Duplicate (document_id, sys_id) pairs found, using non-unique index")
        cursor.execute(
            "CREATE INDEX idx_fragments_doc_sys ON document_fragments(document_id, sys_id)"
        )

    cursor.connection.commit()

    elapsed = time.time() - start
    count = len(rows)
    print(f"  document_fragments: {count:,} rows ({elapsed:.1f}s)")
    return count


def create_meta(cursor, doc_count, source_count, footnote_count, frag_count, supabase_url):
    """Create meta table with version and build metadata."""
    cursor.execute("DROP TABLE IF EXISTS meta")
    cursor.execute("""
        CREATE TABLE meta (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )
    """)

    now = datetime.now(timezone.utc).isoformat()
    cursor.executemany(
        "INSERT INTO meta (key, value) VALUES (?, ?)",
        [
            ("version", VERSION),
            ("created", now),
            ("source", "supabase"),
            ("supabase_url", supabase_url),
            ("documents_count", str(doc_count)),
            ("sources_count", str(source_count)),
            ("footnotes_count", str(footnote_count)),
            ("fragments_count", str(frag_count)),
        ],
    )
    cursor.connection.commit()
    print(f"\n  Meta table created (version {VERSION})")


def validate_export(client, conn):
    """Validate pgp.db matches Supabase row counts and JSON round-trips.

    Returns a list of error strings (empty = success).
    """
    print("\nValidating export...")
    errors = []

    # Row count validation
    for table in ["documents", "document_sources", "document_footnotes", "document_fragments"]:
        resp = client.table(table).select("*", count="exact").limit(0).execute()
        supabase_count = resp.count or 0

        sqlite_count = conn.execute(
            f"SELECT COUNT(*) FROM {table}"
        ).fetchone()[0]

        if supabase_count != sqlite_count:
            msg = f"  FAIL: {table}: Supabase={supabase_count:,}, SQLite={sqlite_count:,}"
            print(msg)
            errors.append(msg)
        else:
            print(f"  PASS: {table}: {sqlite_count:,} rows (matches Supabase)")

    # JSON round-trip: sample tags
    cursor = conn.execute(
        "SELECT pgpid, tags FROM documents WHERE tags IS NOT NULL LIMIT 10"
    )
    for row in cursor:
        try:
            parsed = json.loads(row[1])
            if not isinstance(parsed, list):
                msg = f"  FAIL: pgpid {row[0]}: tags round-trip produced {type(parsed).__name__}, expected list"
                print(msg)
                errors.append(msg)
        except (json.JSONDecodeError, TypeError) as e:
            msg = f"  FAIL: pgpid {row[0]}: tags JSON decode error: {e}"
            print(msg)
            errors.append(msg)

    if not errors:
        print("  PASS: tags JSON round-trip (10 samples)")

    # JSON round-trip: sample sections
    cursor = conn.execute(
        "SELECT id, sections FROM document_sources WHERE sections IS NOT NULL LIMIT 10"
    )
    sections_errors = False
    for row in cursor:
        try:
            parsed = json.loads(row[1])
            if not isinstance(parsed, list):
                msg = f"  FAIL: source id {row[0]}: sections round-trip produced {type(parsed).__name__}, expected list"
                print(msg)
                errors.append(msg)
                sections_errors = True
        except (json.JSONDecodeError, TypeError) as e:
            msg = f"  FAIL: source id {row[0]}: sections JSON decode error: {e}"
            print(msg)
            errors.append(msg)
            sections_errors = True

    if not sections_errors:
        print("  PASS: sections JSON round-trip (10 samples)")

    return errors


def main():
    """Export all PGP data from Supabase to pgp_data/pgp.db."""
    # Load environment (defaults match the rest of the codebase)
    load_dotenv()
    supabase_url = os.environ.get(
        "SUPABASE_URL", "https://ylcpglwxompwjcufdemz.supabase.co"
    )
    supabase_key = os.environ.get(
        "SUPABASE_ANON_KEY",
        "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InlsY3BnbHd4b21wd2pjdWZkZW16Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3Njk3Njc0NzUsImV4cCI6MjA4NTM0MzQ3NX0.xKzlyKrBV0MxADYHqD0lyyymoVxTX91hyI4T6TGchpE",
    )

    if not supabase_url or not supabase_key:
        print("ERROR: SUPABASE_URL and SUPABASE_ANON_KEY must be set in .env or as defaults")
        sys.exit(1)

    # Paths
    script_dir = Path(__file__).parent
    project_dir = script_dir.parent
    target_dir = project_dir / "pgp_data"
    target_path = target_dir / "pgp.db"

    print(f"Source: {supabase_url}")
    print(f"Target: {target_path}")
    print()

    # Create output directory if needed
    target_dir.mkdir(exist_ok=True)

    # Delete existing target for idempotent re-runs
    if target_path.exists():
        print(f"Removing existing {target_path.name}...")
        os.remove(target_path)

    # Create Supabase client
    client = create_client(supabase_url, supabase_key)

    # Create target database
    conn = sqlite3.connect(str(target_path))
    cursor = conn.cursor()
    cursor.execute("PRAGMA journal_mode=WAL")
    cursor.execute("PRAGMA synchronous=NORMAL")

    try:
        # Export all 4 tables
        doc_count = export_documents(client, cursor)
        source_count = export_sources(client, cursor)
        footnote_count = export_footnotes(client, cursor)
        frag_count = export_fragments(client, cursor)

        # Create meta table
        create_meta(cursor, doc_count, source_count, footnote_count, frag_count, supabase_url)

        # Validate
        errors = validate_export(client, conn)

        if errors:
            print(f"\nVALIDATION FAILED with {len(errors)} error(s):")
            for err in errors:
                print(f"  {err}")
            conn.close()
            if target_path.exists():
                os.remove(target_path)
            print(f"\nDeleted {target_path.name} due to validation failure.")
            sys.exit(1)

        # Compact the database
        print("\nCompacting database...")
        cursor.execute("PRAGMA journal_mode=DELETE")
        conn.commit()
        cursor.execute("VACUUM")
        conn.commit()

        # Summary
        file_size_mb = target_path.stat().st_size / (1024 * 1024)
        print(f"\nExport complete!")
        print(f"  documents:          {doc_count:>10,} rows")
        print(f"  document_sources:   {source_count:>10,} rows")
        print(f"  document_footnotes: {footnote_count:>10,} rows")
        print(f"  document_fragments: {frag_count:>10,} rows")
        print(f"  File size: {file_size_mb:.1f} MB")

    except Exception:
        conn.close()
        if target_path.exists():
            os.remove(target_path)
            print(f"\nDeleted partial {target_path.name} due to error.")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
