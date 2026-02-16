# Technology Stack

**Project:** PGP Sidecar Migration + FJMS Full Texts
**Researched:** 2026-02-16
**Focus:** Stack additions/changes for migrating PGP reference data from Supabase to SQLite sidecar, adding FJMS full texts as scholarly sources
**Overall confidence:** HIGH

## Executive Summary

No new Python dependencies are required. The migration from Supabase to SQLite sidecar for PGP reference data uses only Python's built-in `sqlite3` module and follows the proven pattern established by `fjms_enrichment.db` (246 MB) and `nli_crossref.db` (248 MB). The FJMS full text export extends the existing `export_fist_enrichment.py` script. The critical technical decisions are schema design and JSONB-to-TEXT conversion strategy, not library selection.

## Existing Stack (No Changes Needed)

| Technology | Version | Purpose | Status |
|------------|---------|---------|--------|
| Python | 3.11.9 | Runtime | Installed |
| sqlite3 (stdlib) | SQLite 3.45.1 | Local sidecar databases | Built-in, verified |
| supabase-py | 2.27.2 | Cloud DB (auth, corrections, lists -- retains community features) | Installed |
| tantivy-py | 0.25.1 | Full-text search index | Installed, unaffected |
| PyQt6 | latest | Desktop GUI framework | Installed |
| NiceGUI | latest | Web GUI framework | Installed |
| tqdm | latest | Progress bars for export scripts | Installed |

## New Dependencies

**None required.** All capabilities needed are available in the existing Python standard library and installed packages.

---

## Decision 1: PGP Data Target -- New `pgp.db` Sidecar (Not Extending Existing Sidecars)

**Recommendation:** Create a new `pgp_data/pgp.db` sidecar file.

**Why not extend fjms_enrichment.db or nli_crossref.db?**
- Each sidecar has a clear domain boundary (FJMS scholarly metadata, NLI image crossrefs, PGP document data)
- Each has an independent update cycle (FIST.db export vs. PGP CSV re-import vs. NLI crossref scrape)
- Separation enables independent versioning via their `meta` tables
- The fjms_enrichment.db is already 246 MB; adding PGP transcription content (~40-80 MB) would push it past 300 MB

**Why not keep PGP data in Supabase?**
- PGP reference data is read-only system data imported from CSV dumps, not user-generated
- Every PGP lookup currently requires a network round-trip (~50-200ms) vs. SQLite (~0.1ms)
- The desktop app needs offline access; Supabase requires internet
- Supabase free tier has row limits and bandwidth quotas
- The data access pattern is identical to what fjms_service.py and nli_crossref_service.py already do

**What stays in Supabase:**
- User auth (login, registration)
- User corrections (community-submitted corrections, approval workflow)
- User lists (saved manuscript lists)
- User comments/discoveries
- Any data that is multi-user collaborative

**Confidence:** HIGH -- follows proven pattern, two successful precedents in codebase

---

## Decision 2: JSONB Handling Strategy

**Problem:** The Supabase `documents.tags` column is PostgreSQL JSONB (`["communal", "marriage", "trade"]`). SQLite has no native JSONB type but has JSON1 extension functions.

**Recommendation:** Store tags as plain TEXT containing JSON strings. Query with `json_each()` when needed.

**Rationale:**
- SQLite 3.45.1 (our version) has full JSON1 support built-in (verified: `json_array()`, `json_type()` work)
- `json_each()` enables efficient tag queries: `SELECT DISTINCT value FROM documents, json_each(tags)` for tag listing
- For the common query "does this document have tag X?", a simpler `tags LIKE '%"communal"%'` works and is fast enough for 36K rows
- The tag-based search (GIN index equivalent) can use `json_each()` as a table-valued function
- No need for SQLite's newer JSONB blob format -- TEXT JSON is simpler, debuggable, and fast enough for this row count

**Example migration pattern:**
```python
# PostgreSQL JSONB tags: ["communal", "marriage", "trade"]
# -> SQLite TEXT: '["communal", "marriage", "trade"]'
# Stored as-is, queried with json_each() or LIKE

# Efficient tag search (replaces Supabase GIN @> query)
cursor.execute("""
    SELECT d.pgpid FROM documents d, json_each(d.tags) jt
    WHERE jt.value = ?
""", (tag,))

# Or simpler LIKE for single-tag checks (adequate for 36K rows)
cursor.execute("""
    SELECT pgpid FROM documents WHERE tags LIKE ?
""", (f'%"{tag}"%',))
```

**For `document_sources.sections` JSONB:** Store as TEXT JSON. The sections column contains structured arrays like `[{"canvas_url": "...", "canvas_num": 1, "label": null, "text": "..."}]`. In the service layer, parse with Python `json.loads()` on retrieval (same as how the Supabase client returns it -- the service already receives it as a Python list of dicts).

**Confidence:** HIGH -- JSON1 verified available in our SQLite version, pattern documented in official SQLite docs

---

## Decision 3: `document_sources.sections` JSONB Migration

**Problem:** The `sections` column contains per-canvas section data as JSONB arrays. This is currently parsed by `get_section_for_page()` in `document_service.py`.

**Recommendation:** Store sections as TEXT JSON in SQLite. Parse with `json.loads()` in the service layer on retrieval.

**Why not normalize into a separate table?**
- Sections are always read as a complete array for a given source (never queried individually)
- The data is sparse (only ~7,300 PGPIDs have pgp-text HTML sections)
- The current code already handles sections as Python lists -- `json.loads()` returns the same structure
- Normalizing would add a JOIN for zero query benefit

**Implementation:**
```python
# In the new PgpService, when fetching sources:
def get_sources_for_document(self, pgpid: int) -> list[dict]:
    cursor = self._conn.execute(
        "SELECT * FROM document_sources WHERE pgpid = ? ORDER BY doc_relation, sequence_order",
        (pgpid,)
    )
    results = []
    for row in cursor:
        source = dict(row)
        # Parse JSON text back to Python list
        if source.get('sections'):
            source['sections'] = json.loads(source['sections'])
        results.append(source)
    return results
```

**Confidence:** HIGH -- same approach used by every JSON column consumer in the codebase

---

## Decision 4: Schema Design for `pgp.db`

**Recommendation:** Mirror the Supabase schema closely, with JSONB columns converted to TEXT.

### Table: `documents` (~35,839 rows)

```sql
CREATE TABLE documents (
    pgpid INTEGER PRIMARY KEY,
    shelfmark_combined TEXT,
    document_type TEXT,
    tags TEXT DEFAULT '[]',           -- JSON array as TEXT (was JSONB)
    doc_date_original TEXT,
    doc_date_standard TEXT,
    inferred_date_display TEXT,
    description TEXT,
    transcription TEXT,
    transcription_source TEXT,
    languages_primary TEXT,
    languages_secondary TEXT,
    inferred_date_standard TEXT,
    inferred_date_rationale TEXT,
    scholarship_records TEXT,
    shelfmarks_historic TEXT,
    language_note TEXT,
    doc_date_calendar TEXT,
    inferred_date_notes TEXT,
    has_transcription INTEGER DEFAULT 0,  -- BOOLEAN -> INTEGER
    has_translation INTEGER DEFAULT 0,
    input_by TEXT
);
-- pgp_url is computed: omit from SQLite, compute in service layer
```

### Table: `document_fragments` (~36,155 rows)

```sql
CREATE TABLE document_fragments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    document_id INTEGER NOT NULL REFERENCES documents(pgpid),
    sys_id TEXT NOT NULL,
    shelfmark TEXT,
    sequence_order INTEGER DEFAULT 1,
    page_info TEXT,
    collection TEXT,
    library TEXT,
    library_abbrev TEXT,
    fragment_url TEXT,
    iiif_url TEXT,
    UNIQUE(document_id, sys_id)
);
```

### Table: `document_sources` (~9,364 rows)

```sql
CREATE TABLE document_sources (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    pgpid INTEGER NOT NULL REFERENCES documents(pgpid),
    source_scholar TEXT NOT NULL,
    doc_relation TEXT NOT NULL,
    language TEXT,
    content TEXT NOT NULL,
    content_length INTEGER,
    source_url TEXT,
    notes TEXT,
    sequence_order INTEGER DEFAULT 1,
    sections TEXT,             -- JSON array as TEXT (was JSONB)
    source_language TEXT,
    source_direction TEXT,
    UNIQUE(pgpid, source_scholar, doc_relation)
);
```

### Table: `document_footnotes` (~22,757 rows)

```sql
CREATE TABLE document_footnotes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    pgpid INTEGER NOT NULL REFERENCES documents(pgpid),
    source TEXT NOT NULL,
    source_slug TEXT,
    doc_relation TEXT NOT NULL,
    location TEXT,
    url TEXT,
    notes TEXT,
    content TEXT,
    content_length INTEGER,
    UNIQUE(pgpid, source_slug, doc_relation)
);
```

### Indexes

```sql
-- Fragment lookups (most common: find document for a fragment)
CREATE INDEX idx_fragments_sys_id ON document_fragments(sys_id);
CREATE INDEX idx_fragments_document_id ON document_fragments(document_id);

-- Source lookups
CREATE INDEX idx_sources_pgpid ON document_sources(pgpid);
CREATE INDEX idx_sources_relation ON document_sources(pgpid, doc_relation);

-- Footnote lookups
CREATE INDEX idx_footnotes_pgpid ON document_footnotes(pgpid);

-- Document type filtering
CREATE INDEX idx_documents_type ON documents(document_type);
```

### Table: `meta` (version tracking)

```sql
CREATE TABLE meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
-- Entries: version, created, source
```

**Estimated pgp.db size:** ~40-80 MB (transcription text is the bulk; 9,364 sources averaging ~2KB content each = ~19 MB of content alone, plus 35K document descriptions and footnotes).

**Confidence:** HIGH -- directly mirrors working Supabase schema

---

## Decision 5: FJMS Full Text Export (UnitFullText)

**Problem:** The milestone calls for "FJMS full texts (65K transcriptions)". Investigation reveals these are **scholarly catalog descriptions** from `dbo_UnitFullText` (65,332 rows), not machine transcriptions. They contain English-language catalog entries describing manuscript contents (author, content summary, physical description).

**Data characteristics (verified from FIST.db):**
- 65,332 total rows in `dbo_UnitFullText`
- 64,946 linkable to AlmaId (99.4% coverage)
- 85,313 distinct AlmaIds (many manuscripts have multiple entries)
- Average content length: 240 characters
- Max content length: 21,894 characters
- 55,987 rows with content > 100 characters

**Recommendation:** Add a `fulltext` table to `fjms_enrichment.db` (extend existing sidecar, since this is FJMS/FIST data).

```sql
CREATE TABLE fulltext (
    AlmaId TEXT NOT NULL,
    FullText TEXT NOT NULL,
    Page TEXT
);
CREATE INDEX idx_fulltext_alma ON fulltext(AlmaId);
```

**Why extend fjms_enrichment.db (not pgp.db)?**
- This is FIST-sourced data, same domain as domains/joins/catalog
- Same update cycle (FIST.db re-export)
- Same export script can be extended (`export_fist_enrichment.py`)
- Keeps the domain boundary clean: FIST data = fjms_enrichment.db, PGP data = pgp.db

**Export pattern:** Follow the established pattern in `export_fist_enrichment.py`:
```python
def export_fulltext(source, target):
    """Export UnitFullText catalog descriptions from FIST to sidecar."""
    target.execute("DROP TABLE IF EXISTS fulltext")
    target.execute("""
        CREATE TABLE fulltext (
            AlmaId TEXT NOT NULL,
            FullText TEXT NOT NULL,
            Page TEXT
        )
    """)
    cursor = source.execute("""
        SELECT DISTINCT
            TRIM(CAST(alma.AlmaId AS TEXT)) as AlmaId,
            ft.FullText,
            ft.Page
        FROM dbo_UnitFullText ft
        JOIN dbo_Signature sig ON ft.SignatureId = sig.SignatureId
        JOIN dbo_InventorySignature isig ON sig.SetSignatureId = isig.SetSignatureId
        JOIN dbo_Inventory inv ON isig.InventoryId = inv.InventoryId
        JOIN dbo_InventoryAlma alma ON inv.InventoryId = alma.InventoryId
        WHERE ft.FullText IS NOT NULL AND LENGTH(ft.FullText) > 10
    """)
    # ... batch insert pattern (same as other exports)
```

**Size estimate:** ~15-25 MB additional in fjms_enrichment.db (65K rows * avg 240 chars = ~15 MB text + indexes).

**Confidence:** HIGH -- follows exact pattern of 8 existing export functions in same script

---

## Decision 6: PGP Data Export Strategy (Supabase -> SQLite)

**Problem:** Current PGP data lives in Supabase (cloud PostgreSQL). Need a one-time export + script for future re-runs.

**Recommendation:** Write `scripts/export_pgp_sidecar.py` that reads from Supabase API and writes to `pgp_data/pgp.db`.

**Why not export from the CSV source files?**
- The CSV import scripts (`import_pgp_documents.py`, `import_pgp_sections.py`) already processed and enriched the data
- Supabase has the canonical merged state (transcriptions + metadata + sections + footnotes)
- Re-deriving from CSVs would duplicate complex normalization logic

**Why not dump Supabase SQL directly?**
- Supabase free tier does not expose `pg_dump`
- The Supabase Python client (`supabase-py`) provides paginated REST access to all tables
- The export script can fetch all rows in batches (same approach as `get_sys_ids_with_transcriptions` batching)

**Export approach:**
```python
#!/usr/bin/env python3
"""Export PGP reference data from Supabase to pgp.db sidecar."""

from supabase import create_client
import sqlite3
import json
from tqdm import tqdm

def fetch_all_rows(client, table_name, page_size=1000):
    """Paginated fetch of all rows from a Supabase table."""
    all_rows = []
    offset = 0
    while True:
        response = client.table(table_name).select('*').range(
            offset, offset + page_size - 1
        ).execute()
        if not response.data:
            break
        all_rows.extend(response.data)
        offset += page_size
        if len(response.data) < page_size:
            break
    return all_rows
```

**One-time vs. repeatable:** The script should be idempotent (DROP TABLE IF EXISTS + re-create), same as `export_fist_enrichment.py`. This supports re-running when PGP data updates.

**Confidence:** HIGH -- supabase-py pagination is documented and tested in existing codebase

---

## Decision 7: Service Layer Architecture

**Recommendation:** Create `shared/pgp_service.py` as a `PgpService` class following the exact pattern of `FjmsService` and `NliCrossrefService`.

**Pattern to follow (verified from codebase):**
1. Class with `__init__(db_path, thread_safe)` -- same constructor signature
2. `is_available()` method for graceful degradation
3. `get_version()` via meta table
4. Read-only connection via URI mode (`file:{path}?mode=ro`)
5. `sqlite3.Row` row factory for dict-like access
6. `check_same_thread=not thread_safe` for NiceGUI web app
7. Module-level singleton via `get_pgp_service(thread_safe=False)`
8. Batch methods using IN queries with 500-row chunks (SQLite variable limit = 999)

**Methods to implement (direct mapping from current `document_service.py`):**

| Current Function | New PgpService Method | Notes |
|-----------------|----------------------|-------|
| `get_document_for_fragment(sys_id, page_num)` | `get_document_for_fragment(sys_id, page_num)` | Two-step: fragments -> documents |
| `get_fragments_for_document(pgpid)` | `get_fragments_for_document(pgpid)` | Direct SQL |
| `get_transcription_for_document(pgpid)` | `get_transcription_for_document(pgpid)` | Direct SQL |
| `get_document_metadata(pgpid)` | `get_document_metadata(pgpid)` | Direct SQL |
| `get_sources_for_document(pgpid)` | `get_sources_for_document(pgpid)` | + json.loads(sections) |
| `get_all_sources_for_fragment(sys_id)` | `get_all_sources_for_fragment(sys_id)` | Multi-step, same logic |
| `get_editions_for_document(pgpid)` | `get_editions_for_document(pgpid)` | Filter by doc_relation |
| `get_translations_for_document(pgpid)` | `get_translations_for_document(pgpid)` | Filter by doc_relation |
| `get_sys_ids_with_transcriptions(sys_ids)` | `get_sys_ids_with_transcriptions(sys_ids)` | Batched IN query |
| `get_fragments_by_tag(tag)` | `get_fragments_by_tag(tag)` | json_each() or LIKE |
| `get_all_distinct_tags()` | `get_all_distinct_tags()` | json_each() aggregate |

**Pure Python functions (no migration needed):**
- `parse_transcription_sections()` -- regex parsing, no DB access
- `get_section_for_page()` -- uses parsed sections, no DB access
- `parse_html_sections()` -- HTML parser, no DB access
- `PGPHTMLParser` class -- HTML parser, no DB access
- `split_textual_frames()`, `parse_textual_frame()` -- text parsing, no DB access (these are in fjms_service.py)

These pure functions stay in `shared/document_service.py` unchanged.

**Confidence:** HIGH -- 1:1 mapping from existing Supabase queries to SQL equivalents

---

## Decision 8: Deduplication Strategy for PGP/FJMS Overlap

**Problem:** Both PGP sources and FJMS catalog records describe the same manuscripts. Some overlap is expected.

**Recommendation:** No deduplication needed at the database level. Handle at the UI level.

**Why:**
- PGP data is keyed by `pgpid` (PGP document ID), links to manuscripts via `document_fragments.sys_id`
- FJMS data is keyed by `AlmaId` (NLI Alma system ID), which IS the sys_id
- Different granularity: PGP is document-level (one document can span multiple fragments), FJMS is manuscript-level
- Different content: PGP has transcription text and scholarly editions; FJMS has catalog descriptions and bibliographic references
- The overlap enriches rather than duplicates: PGP gives you "what the document says", FJMS gives you "what scholars say about the manuscript"

**UI deduplication approach:**
- When displaying metadata for a manuscript, show PGP and FJMS data in separate sections (already the current UX pattern)
- If both have date info, prefer PGP `doc_date_standard` (more structured) but show FJMS `CopyDate` as supplementary
- If both have descriptions, show both with source attribution

**Confidence:** HIGH -- PGP and FJMS serve complementary purposes, overlap is beneficial

---

## Decision 9: What NOT to Add

| Technology/Library | Why Not |
|--------------------|---------|
| SQLAlchemy | Overkill for read-only SQLite. Built-in `sqlite3` with `Row` factory is sufficient. All three existing services use raw `sqlite3`. |
| datasette | Useful for exploration but not needed in production. The sidecar is a data file, not a server. |
| sqlite-utils | Nice CLI tool but adds a dependency for zero benefit. The export script uses raw `sqlite3` successfully. |
| peewee/pony ORM | Same as SQLAlchemy -- unnecessary abstraction for read-only queries. |
| FTS5 on pgp.db | Tantivy handles full-text search. PGP transcription search should go through Tantivy, not a second search engine. FTS5 is used on fjms_enrichment.db for catalog search only. |
| Redis/memcached | Local SQLite is already fast enough (~0.1ms indexed lookups). No caching layer needed. |
| Connection pooling | Single connection per service instance with thread-safe flag is the established pattern. Works for both desktop (single-threaded) and web (NiceGUI concurrent requests). |

**Confidence:** HIGH -- consistent with architectural decisions across all existing services

---

## Migration Tooling Summary

### New Scripts Needed

| Script | Purpose | Pattern |
|--------|---------|---------|
| `scripts/export_pgp_sidecar.py` | Export PGP data from Supabase to `pgp_data/pgp.db` | Similar to `export_fist_enrichment.py` but reads from Supabase API |

### Scripts to Extend

| Script | Change | Pattern |
|--------|--------|---------|
| `scripts/export_fist_enrichment.py` | Add `export_fulltext()` function, bump VERSION to 3.0.0 | Same batch-insert pattern as existing 8 export functions |

### New Service File

| File | Purpose | Pattern |
|------|---------|---------|
| `shared/pgp_service.py` | SQLite-backed PGP data service (replaces Supabase calls in `document_service.py`) | Identical pattern to `FjmsService` and `NliCrossrefService` |

### Files to Modify

| File | Change |
|------|--------|
| `shared/document_service.py` | Swap Supabase client calls for PgpService calls. Keep pure Python functions unchanged. |
| `web/document_service.py` | Shim stays, re-exports remain unchanged |
| `gui_threads.py` | No changes needed (imports from shared/document_service.py) |
| `web/pages/browse.py` | No changes needed (imports from shared/document_service.py) |
| `web/pages/search.py` | No changes needed (imports from shared/document_service.py) |
| `shared/fjms_service.py` | Add `get_fulltext()` method for FJMS catalog descriptions |

### Files NOT to Modify

| File | Why |
|------|-----|
| `shared/supabase_provider.py` | Still needed for auth, corrections, lists, comments |
| `supabase_corrections_client.py` | Desktop Supabase client for community features |
| `genizah_core.py` | Search engine, no PGP data access |
| `genizah_app.py` | Uses gui_threads.py which imports from document_service.py -- chain unchanged |

---

## Environment Requirements

### Build-Time (Export Script)

```bash
# Already installed
pip install supabase tqdm python-dotenv

# Environment variables needed for export:
# SUPABASE_URL (or use default)
# SUPABASE_ANON_KEY (for read-only export, anon key is sufficient)
```

### Runtime (Both Apps)

```bash
# No new packages needed
# Python 3.11+ with built-in sqlite3 (SQLite 3.45.1)
```

### Desktop Distribution

The `pgp_data/pgp.db` file must be included in the desktop app distribution bundle alongside `fist_data/fjms_enrichment.db` and `nli_data/nli_crossref.db`. Update `build_app.bat` to include the new sidecar.

---

## Sources

- [SQLite JSON1 Functions and Operators](https://sqlite.org/json1.html) -- JSON1 built-in since SQLite 3.38.0, JSONB since 3.45.0 (HIGH confidence)
- [SQLite JSONB Format](https://sqlite.org/jsonb.html) -- binary JSON storage format (not needed for our use case)
- Codebase verification: `shared/fjms_service.py`, `shared/nli_crossref_service.py`, `shared/document_service.py` -- established service patterns (HIGH confidence)
- Codebase verification: `scripts/export_fist_enrichment.py` -- established export pattern (HIGH confidence)
- Codebase verification: `migrations/*.sql` -- full Supabase schema (HIGH confidence)
- FIST.db direct inspection: `dbo_UnitFullText` table structure and row counts (HIGH confidence)
- Python/SQLite version verification: Python 3.11.9, SQLite 3.45.1, JSON1 and FTS5 confirmed available (HIGH confidence)
