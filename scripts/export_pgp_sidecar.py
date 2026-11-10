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

import argparse
import json
import os
import sqlite3
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
from supabase import create_client

# 1.1.0 (2026-09-22): documents.doc_relation is carried across (it had been dropped
# silently since the first export, so translation-flagged documents rendered as
# transcriptions); locally-generated tables survive a rebuild; and the export is built
# beside the live sidecar and swapped in only after validation, instead of deleting it up
# front. A new fail-closed check refuses to emit a sidecar that is missing any column
# Supabase has -- the defect that hid the doc_relation loss for five months.
VERSION = "1.1.0"
PAGE_SIZE = 1000

# The documents table, as one list, so the CREATE TABLE, the INSERT placeholders and the
# value tuple cannot drift apart again. Order MUST match the CREATE TABLE below; a guard
# asserts it at runtime.
DOCUMENT_COLUMNS = (
    "pgpid", "shelfmark_combined", "document_type", "tags", "doc_date_original",
    "doc_date_standard", "doc_date_calendar", "inferred_date_display",
    "inferred_date_standard", "inferred_date_rationale", "inferred_date_notes",
    "description", "transcription", "transcription_source", "languages_primary",
    "languages_secondary", "language_note", "scholarship_records", "shelfmarks_historic",
    "has_transcription", "has_translation", "input_by", "pgp_url", "created_at",
    "doc_relation",
)
# Columns whose Supabase value is JSONB and must be serialized on the way in.
JSON_COLUMNS = frozenset({"tags"})

# Tables that live ONLY in the sidecar -- generated here, with no upstream to rebuild them
# from. They must survive a refresh. `pgp_translations` is produced by
# scripts/translate_pgp_descriptions.py; the 2026-04-22 rebuild destroyed 34,954 of its
# rows and nobody noticed until 2026-09-21.
CARRYOVER_TABLES = ("pgp_translations",)

# Supabase columns this export deliberately does not carry, each with a reason. Anything
# NOT listed here and not in the sidecar makes the export fail rather than quietly
# shipping a sidecar with a missing field.
KNOWN_UNEXPORTED = {
    # "table.column": "why",
}

# Written by scripts/import_pgp_full.py on a successful --execute. It records the
# upstream commit that was actually PUSHED to Supabase, plus the row counts the import
# left behind.
#
# Deliberately NOT upstream_provenance.json, which only says what this workstation last
# DOWNLOADED. The sidecar is built from Supabase, so stamping a download-time commit could
# label the database with a vintage it does not contain -- if the CSVs were fetched but
# never imported, or if Supabase was refreshed from another machine. A false provenance
# claim is worse than none, because it invites trust.
IMPORT_PROVENANCE_FILENAME = "import_provenance.json"

# The four tables exported from Supabase. Upserts never delete, so none of them can
# legitimately hold fewer rows than the sidecar built from the previous export.
CORE_TABLES = ("documents", "document_sources", "document_footnotes", "document_fragments")


def serialize_json(value):
    """Serialize a Python object (from Supabase JSONB) to deterministic JSON TEXT.

    Returns None for None values (preserving SQL NULL).
    Uses sorted keys and compact format for deterministic, space-efficient output.
    """
    if value is None:
        return None
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"))


def assert_no_dropped_columns(table_name, supabase_rows, cursor):
    """Refuse to build a sidecar that is quietly missing a column Supabase has.

    Every column list in this file is hand-written while ``fetch_all_rows`` selects ``*``,
    so a column added upstream is fetched and then silently discarded by an INSERT that
    never names it. That is exactly how ``documents.doc_relation`` was lost: no error, no
    warning, and five months of translations rendered as transcriptions. Fail instead.
    """
    if not supabase_rows:
        # A core PGP table is never legitimately empty. If one comes back with no rows --
        # an RLS policy change, a rotated or downgraded key, the wrong project -- the
        # exporter would build an empty table, and validate_export() counts through the
        # SAME restricted client, so both sides read zero and the "validated" build
        # replaces the live sidecar. That is silent whole-table data loss, so it is fatal
        # rather than a warning.
        raise RuntimeError(
            "%s returned no rows. A core PGP table is never empty, so this is a "
            "credentials, RLS or wrong-project problem -- not an empty corpus. Refusing "
            "to build a sidecar that would wipe it." % table_name
        )
    supabase_cols = set(supabase_rows[0])
    local_cols = {r[1] for r in cursor.execute("PRAGMA table_info(%s)" % table_name)}
    dropped = sorted(
        col for col in supabase_cols - local_cols
        if "%s.%s" % (table_name, col) not in KNOWN_UNEXPORTED
    )
    if dropped:
        raise RuntimeError(
            "%s: Supabase has %d column(s) this export does not carry: %s.\n"
            "Add each one to the CREATE TABLE and the INSERT in scripts/export_pgp_sidecar.py, "
            "or add '%s.<column>' to KNOWN_UNEXPORTED with a reason."
            % (table_name, len(dropped), ", ".join(dropped), table_name)
        )


def read_carryover_tables(existing_db_path):
    """Read the sidecar-only tables out of the live pgp.db before it is replaced.

    Returns a list of dicts: name, create_sql, indexes, columns, rows. Empty when there is
    no existing sidecar or it holds none of them -- both are normal, not errors.
    """
    carried = []
    if not os.path.exists(existing_db_path):
        return carried

    # mode=ro: a bare connect() on a missing path creates a 0-byte stub that later reads
    # as "no such table", a trap this repo has hit before.
    conn = sqlite3.connect("file:%s?mode=ro" % str(existing_db_path).replace("\\", "/"), uri=True)
    try:
        for name in CARRYOVER_TABLES:
            row = conn.execute(
                "SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (name,)
            ).fetchone()
            if not row or not row[0]:
                continue
            indexes = [
                r[0] for r in conn.execute(
                    "SELECT sql FROM sqlite_master WHERE type='index' AND tbl_name=? "
                    "AND sql IS NOT NULL",
                    (name,),
                )
            ]
            columns = [r[1] for r in conn.execute('PRAGMA table_info("%s")' % name)]
            rows = conn.execute('SELECT * FROM "%s"' % name).fetchall()
            carried.append({
                "name": name,
                "create_sql": row[0],
                "indexes": indexes,
                "columns": columns,
                "rows": rows,
            })
            print("  carrying forward %s: %s rows" % (name, format(len(rows), ",")))
    finally:
        conn.close()

    if not carried:
        print("  (no sidecar-only tables to carry forward)")
    return carried


def write_carryover_tables(cursor, carried):
    """Re-create the carried tables in the freshly built sidecar, rows and indexes intact."""
    for table in carried:
        cursor.execute(table["create_sql"])
        if table["rows"]:
            placeholders = ", ".join("?" * len(table["columns"]))
            cursor.executemany(
                'INSERT INTO "%s" VALUES (%s)' % (table["name"], placeholders),
                table["rows"],
            )
        for index_sql in table["indexes"]:
            cursor.execute(index_sql)

        restored = cursor.execute(
            'SELECT COUNT(*) FROM "%s"' % table["name"]
        ).fetchone()[0]
        if restored != len(table["rows"]):
            raise RuntimeError(
                "%s: carried %d rows but %d landed" % (table["name"], len(table["rows"]), restored)
            )
    cursor.connection.commit()


def read_previous_counts(existing_db_path):
    """Row counts of the core tables in the LIVE sidecar, or {} when there is none.

    Read before the build so the new export can be held to them: the importer only ever
    upserts, so a table that comes back smaller than it was is not a smaller corpus, it is
    a restricted client (rotated key, RLS change, wrong project) returning part of one.
    The empty-table guard catches zero rows; this catches 1 of 36,000.
    """
    if not os.path.exists(existing_db_path):
        return {}
    conn = sqlite3.connect("file:%s?mode=ro" % str(existing_db_path).replace("\\", "/"), uri=True)
    try:
        present = {
            row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
        return {
            table: conn.execute('SELECT COUNT(*) FROM "%s"' % table).fetchone()[0]
            for table in CORE_TABLES if table in present
        }
    finally:
        conn.close()


def assert_no_shrink(previous_counts, exported_counts, allow_shrink=False):
    """Refuse to replace a sidecar with one that holds fewer rows in any core table."""
    shrunk = [
        "%s: %s -> %s" % (table, format(previous_counts[table], ","),
                          format(exported_counts[table], ","))
        for table in CORE_TABLES
        if table in previous_counts and exported_counts.get(table, 0) < previous_counts[table]
    ]
    if not shrunk:
        return
    message = (
        "the export returned FEWER rows than the live sidecar holds (%s). The importer "
        "only upserts, so a core table never legitimately shrinks: this is a credentials, "
        "RLS or wrong-project problem returning part of the corpus, and the row-count "
        "validation cannot see it because it counts through the same restricted client. "
        "Refusing to replace the live sidecar." % "; ".join(shrunk)
    )
    if allow_shrink:
        print("\n  WARNING: %s (--allow-shrink: building anyway)" % message)
        return
    raise RuntimeError(message)


def read_import_provenance(pgp_data_dir):
    """What scripts/import_pgp_full.py last pushed to Supabase, or {} if unknown."""
    path = os.path.join(str(pgp_data_dir), IMPORT_PROVENANCE_FILENAME)
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as fh:
            data = json.load(fh)
        return data if isinstance(data, dict) else {}
    except (OSError, ValueError) as exc:
        print("  WARNING: could not read %s: %s" % (IMPORT_PROVENANCE_FILENAME, exc))
        return {}


def corroborate_provenance(provenance, exported_counts, supabase_url=None):
    """Does the import record describe the snapshot we just exported?

    Returns (ok, reason). Only an `ok` record earns a commit id in meta. This is what
    stops the sidecar claiming a vintage it does not hold -- the row-count validation
    elsewhere compares the sidecar against Supabase and is blind to it, because both
    sides are equally wrong when the import never happened here.

    Matching counts are corroboration, not proof: an import of a different commit that
    happened to leave identical counts would pass (a metadata-correction commit moves no
    count at all; that gap is recorded in docs/OPEN_ISSUES.md). What IS checked, because
    it is cheap: the record must say its inputs were verified -- explicitly, since a record
    without the key is indistinguishable from one written by an older importer -- and it
    must name the same Supabase project this export is reading from.
    """
    if not provenance:
        return False, ("no %s -- the import step did not run on this machine"
                       % IMPORT_PROVENANCE_FILENAME)
    if not provenance.get("upstream_commit"):
        return False, "%s records no upstream commit" % IMPORT_PROVENANCE_FILENAME
    if provenance.get("inputs_verified") is not True:
        return False, ("%s does not state that its inputs were verified (re-run "
                       "scripts/import_pgp_full.py --execute to write a record that does)"
                       % IMPORT_PROVENANCE_FILENAME)
    if supabase_url is not None:
        recorded_url = provenance.get("supabase_url")
        if not recorded_url:
            return False, ("%s names no Supabase project, so it cannot be tied to %s"
                           % (IMPORT_PROVENANCE_FILENAME, supabase_url))
        if recorded_url.rstrip("/") != str(supabase_url).rstrip("/"):
            return False, ("%s describes an import into %s, not %s"
                           % (IMPORT_PROVENANCE_FILENAME, recorded_url, supabase_url))

    recorded = provenance.get("supabase_counts_after") or {}
    if not recorded:
        return False, "%s records no row counts to check against" % IMPORT_PROVENANCE_FILENAME

    mismatches = [
        "%s: exported %s, import recorded %s" % (table, format(count, ","), recorded.get(table))
        for table, count in sorted(exported_counts.items())
        if recorded.get(table) != count
    ]
    if mismatches:
        return False, ("Supabase has changed since that import (%s)"
                       % "; ".join(mismatches))
    return True, "row counts match the recorded import"


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
            created_at TEXT,
            -- Princeton's per-document source classification: 'Digital Edition',
            -- 'Digital Translation', or NULL. Populated in Supabase by
            -- scripts/update_doc_relation.py and dropped by every export before 1.1.0,
            -- which is why web/pages/browse_enrichment.py and web/pages/search_results.py
            -- fell through to "treat it as an edition" for every document.
            doc_relation TEXT
        )
    """)

    # The CREATE TABLE above and DOCUMENT_COLUMNS must stay in lockstep -- the INSERT is
    # generated from the latter, so a mismatch would silently shift every value one
    # column to the left.
    created = tuple(r[1] for r in cursor.execute("PRAGMA table_info(documents)"))
    if created != DOCUMENT_COLUMNS:
        raise RuntimeError(
            "documents schema drift: CREATE TABLE has %r but DOCUMENT_COLUMNS has %r"
            % (created, DOCUMENT_COLUMNS)
        )

    rows = fetch_all_rows(client, "documents", order_by="pgpid")

    insert_sql = "INSERT INTO documents VALUES (%s)" % ", ".join("?" * len(DOCUMENT_COLUMNS))
    batch = []
    for row in rows:
        batch.append(tuple(
            serialize_json(row.get(col)) if col in JSON_COLUMNS else row.get(col)
            for col in DOCUMENT_COLUMNS
        ))
        if len(batch) >= PAGE_SIZE:
            cursor.executemany(insert_sql, batch)
            batch = []

    if batch:
        cursor.executemany(insert_sql, batch)

    assert_no_dropped_columns("documents", rows, cursor)

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

    assert_no_dropped_columns("document_sources", rows, cursor)

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

    assert_no_dropped_columns("document_footnotes", rows, cursor)

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

    assert_no_dropped_columns("document_fragments", rows, cursor)

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


def source_revision():
    """(revision, dirty) for the repository that is building this sidecar.

    `dirty` is True when tracked files differ from HEAD, which means the sidecar was built
    from code that exists in no commit and therefore cannot be pinned to one. Both are
    recorded rather than enforced here: the BUILD is allowed to be exploratory, the DEPLOY
    is what must refuse. Returns (None, True) if git cannot answer -- unknown provenance is
    treated as unpinnable, never as clean.
    """
    repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    try:
        rev = subprocess.run(["git", "rev-parse", "HEAD"], cwd=repo_root,
                             capture_output=True, text=True, timeout=30)
        if rev.returncode != 0:
            return (None, True)
        diff = subprocess.run(["git", "diff", "--quiet", "HEAD"], cwd=repo_root,
                              capture_output=True, text=True, timeout=60)
        if diff.returncode not in (0, 1):
            return (None, True)
        return (rev.stdout.strip() or None, diff.returncode != 0)
    except (OSError, subprocess.SubprocessError):
        return (None, True)


def create_meta(cursor, doc_count, source_count, footnote_count, frag_count, supabase_url,
                carried=(), provenance=None):
    """Create meta table with version, build metadata and upstream provenance."""
    cursor.execute("DROP TABLE IF EXISTS meta")
    cursor.execute("""
        CREATE TABLE meta (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )
    """)

    now = datetime.now(timezone.utc).isoformat()
    entries = [
        ("version", VERSION),
        ("created", now),
        ("source", "supabase"),
        ("supabase_url", supabase_url),
        ("documents_count", str(doc_count)),
        ("sources_count", str(source_count)),
        ("footnotes_count", str(footnote_count)),
        ("fragments_count", str(frag_count)),
    ]

    # Carried tables are recorded so a reader can tell "no translations" from "the
    # translations were lost in the last rebuild" -- indistinguishable until now.
    for table in carried:
        entries.append(("%s_count" % table["name"], str(len(table["rows"]))))

    # Which upstream commit the CSVs came from. `created` already dates the BUILD; this
    # dates the DATA, which is the thing that was actually five months stale.
    for key in ("upstream_repo", "upstream_commit", "upstream_committed", "imported_at"):
        value = (provenance or {}).get(key)
        if value:
            entries.append((key, str(value)))

    # Which revision of THIS repository produced the file, so the deploy can bind the
    # sidecar to the code that must read it. Deriving that at deploy time from the current
    # HEAD is wrong: the database is gitignored, so it survives a later checkout or commit
    # and would be labelled with a revision it was never built from.
    revision, dirty = source_revision()
    entries.append(("source_revision", revision or "unknown"))
    entries.append(("source_dirty", "1" if dirty else "0"))

    cursor.executemany("INSERT INTO meta (key, value) VALUES (?, ?)", entries)
    cursor.connection.commit()
    print(f"\n  Meta table created (version {VERSION}, {len(entries)} keys)")


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


def validate_carryover(conn, carried):
    """Confirm every carried table survived the rebuild with all of its rows.

    Without this a carry-forward that silently wrote nothing would look exactly like the
    data loss it exists to prevent.
    """
    errors = []
    for table in carried:
        expected = len(table["rows"])
        try:
            actual = conn.execute('SELECT COUNT(*) FROM "%s"' % table["name"]).fetchone()[0]
        except sqlite3.Error as exc:
            msg = "  FAIL: %s: carried table missing from the new sidecar (%s)" % (
                table["name"], exc)
            print(msg)
            errors.append(msg)
            continue
        if actual != expected:
            msg = "  FAIL: %s: carried %s rows, found %s" % (
                table["name"], format(expected, ","), format(actual, ","))
            print(msg)
            errors.append(msg)
        else:
            print("  PASS: %s: %s rows carried forward intact"
                  % (table["name"], format(actual, ",")))
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

    parser = argparse.ArgumentParser(description="Export the PGP tables from Supabase to "
                                                 "pgp_data/pgp.db")
    parser.add_argument("--allow-shrink", action="store_true",
                        help="replace the live sidecar even if a core table came back with "
                             "fewer rows than it holds (only after rows were deliberately "
                             "deleted in Supabase)")
    args = parser.parse_args()

    target_dir = Path(__file__).parent.parent / "pgp_data"
    return build_sidecar(create_client(supabase_url, supabase_key), target_dir, supabase_url,
                         allow_shrink=args.allow_shrink)


def build_sidecar(client, target_dir, supabase_url, allow_shrink=False):
    """Build pgp.db from `client` into `target_dir`, swapping it in only once valid.

    Separate from main() so a test can drive the real orchestration -- every guard, the
    carry-forward, the validation and the swap -- against a fake Supabase. Testing only
    the helpers left the CALL SITES unguarded: they could all be deleted with the suite
    still green.
    """
    target_dir = Path(target_dir)
    target_path = target_dir / "pgp.db"
    # Built beside the live sidecar and swapped in only once validation passes. The old
    # code deleted pgp.db first, so an export that failed -- or was interrupted -- left
    # the machine with no sidecar at all.
    build_path = target_dir / "pgp.db.new"

    print(f"Source: {supabase_url}")
    print(f"Target: {target_path}")
    print()

    # Create output directory if needed
    target_dir.mkdir(exist_ok=True)

    # Read the sidecar-only tables out of the LIVE file before anything touches it.
    print("Checking the existing sidecar for locally-generated tables...")
    carried = read_carryover_tables(str(target_path))
    previous_counts = read_previous_counts(str(target_path))
    provenance = read_import_provenance(target_dir)
    if provenance.get("upstream_commit"):
        print("  last import: %s @ %s (%s)"
              % (provenance.get("upstream_repo", "?"),
                 str(provenance["upstream_commit"])[:12],
                 provenance.get("imported_at", "?")))
    print()

    # A stale build file from an interrupted run must not be reused.
    if build_path.exists():
        os.remove(build_path)

    # Create target database
    conn = sqlite3.connect(str(build_path))
    cursor = conn.cursor()
    cursor.execute("PRAGMA journal_mode=WAL")
    cursor.execute("PRAGMA synchronous=NORMAL")

    try:
        # Export all 4 tables
        doc_count = export_documents(client, cursor)
        source_count = export_sources(client, cursor)
        footnote_count = export_footnotes(client, cursor)
        frag_count = export_fragments(client, cursor)

        # Re-create the locally-generated tables BEFORE validation, so their row
        # counts are part of what gets checked rather than an afterthought.
        write_carryover_tables(cursor, carried)

        # Stamp the upstream commit ONLY if the counts we just exported corroborate the
        # recorded import. Otherwise carry no provenance at all, and say why.
        exported_counts = {
            "documents": doc_count,
            "document_sources": source_count,
            "document_footnotes": footnote_count,
            "document_fragments": frag_count,
        }
        # Fewer rows than the live sidecar is a restricted client, not a smaller corpus.
        assert_no_shrink(previous_counts, exported_counts, allow_shrink=allow_shrink)

        corroborated, reason = corroborate_provenance(provenance, exported_counts,
                                                      supabase_url=supabase_url)
        if corroborated:
            print("\n  provenance: %s" % reason)
        else:
            print("\n  provenance: NOT recorded -- %s" % reason)
            print("  The sidecar is still correct; it just cannot honestly say which")
            print("  upstream commit it came from. Run scripts/import_pgp_full.py")
            print("  --execute on this machine to establish that.")
            provenance = {}

        # Create meta table
        create_meta(cursor, doc_count, source_count, footnote_count, frag_count,
                    supabase_url, carried=carried, provenance=provenance)

        # Validate
        errors = validate_export(client, conn)
        errors.extend(validate_carryover(conn, carried))

        if errors:
            print(f"\nVALIDATION FAILED with {len(errors)} error(s):")
            for err in errors:
                print(f"  {err}")
            conn.close()
            if build_path.exists():
                os.remove(build_path)
            print(f"\nDiscarded {build_path.name}; {target_path.name} is unchanged.")
            sys.exit(1)

        # Compact the database
        print("\nCompacting database...")
        cursor.execute("PRAGMA journal_mode=DELETE")
        conn.commit()
        cursor.execute("VACUUM")
        conn.commit()
        conn.close()

        # Swap the validated build in. os.replace is atomic on both platforms; on
        # Windows it fails if something still holds the old file open, which is a
        # clearer outcome than a half-written sidecar.
        try:
            os.replace(str(build_path), str(target_path))
        except OSError as exc:
            print(f"\nERROR: could not replace {target_path.name}: {exc}")
            print(f"The new sidecar is complete and validated at {build_path.name}.")
            print("Close anything holding pgp.db open (a running app, a DB browser) "
                  "and rename it by hand.")
            sys.exit(1)

        # Summary
        file_size_mb = target_path.stat().st_size / (1024 * 1024)
        print(f"\nExport complete!")
        print(f"  documents:          {doc_count:>10,} rows")
        print(f"  document_sources:   {source_count:>10,} rows")
        print(f"  document_footnotes: {footnote_count:>10,} rows")
        print(f"  document_fragments: {frag_count:>10,} rows")
        for table in carried:
            print(f"  {table['name'] + ':':<19} {len(table['rows']):>10,} rows (carried forward)")
        print(f"  File size: {file_size_mb:.1f} MB")
        return 0

    except Exception:
        try:
            conn.close()
        except Exception:
            pass
        if build_path.exists():
            os.remove(build_path)
            print(f"\nDiscarded partial {build_path.name}; {target_path.name} is unchanged.")
        raise
    finally:
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    raise SystemExit(main())
