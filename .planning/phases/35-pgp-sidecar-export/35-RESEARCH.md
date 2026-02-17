# Phase 35: PGP Sidecar Export - Research

**Researched:** 2026-02-17
**Domain:** SQLite sidecar creation / ETL from Supabase PostgreSQL
**Confidence:** HIGH

## Summary

Phase 35 exports four PGP reference tables from Supabase PostgreSQL into a local SQLite sidecar file (`pgp.db`). The project has two excellent precedents: `fjms_enrichment.db` (Phase 25, 257 MB, 10 tables, exported from FIST.db) and `nli_crossref.db` (Phase 29, 260 MB, 5 tables, imported from CSV/JSON). Both follow identical patterns: standalone Python script in `scripts/`, `DROP TABLE IF EXISTS` + `CREATE TABLE` + batch insert + index creation + meta table with version/created/source keys. The export script compacts with `VACUUM` and opens read-only via URI mode `?mode=ro` at runtime.

This phase is a straightforward adaptation of these patterns to a new data source (Supabase REST API instead of local SQLite or CSV). The main novel challenge is **pagination**: Supabase's PostgREST API returns max 1000 rows per request, requiring `.range()` pagination to fetch all ~36K documents, ~9K sources, ~23K footnotes, and ~36K fragments. The codebase already has this pattern in `import_pgp_sections.py` (line 279-293). JSON columns (`tags` as JSONB array, `sections` as JSONB array of objects) must be serialized to TEXT for SQLite storage, and the round-trip must produce identical Python objects.

**Primary recommendation:** Create `scripts/export_pgp_sidecar.py` following the exact `export_fist_enrichment.py` pattern, storing the sidecar in `pgp_data/pgp.db` (parallel to `fist_data/fjms_enrichment.db` and `nli_data/nli_crossref.db`). Use `.range()` pagination with page_size=1000 for Supabase fetching, `json.dumps(sort_keys=True, ensure_ascii=False)` for JSONB-to-TEXT serialization, and built-in row count + JSON round-trip validation before finalizing.

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions

#### Export script behavior
- Script structure: Claude's discretion (standalone script or shared module -- fit existing codebase conventions)
- Credentials: Use existing SUPABASE_URL and SUPABASE_ANON_KEY environment variables from .env
- Progress output: Print table name, row count, and elapsed time for each of the 4 tables
- Error handling: On failure, delete partial pgp.db -- no corrupt sidecars left behind, clean slate for retry

#### Data fidelity
- Verbatim export from Supabase -- NULLs stay NULL, empty strings stay empty, no transformations or cleanup
- Type mapping: Claude's discretion -- pick explicit mappings that ensure correct round-trips (JSONB->TEXT, TIMESTAMP->TEXT ISO-8601, etc.)
- Index strategy: Claude's discretion -- determine which indexes are needed based on Phase 36's query patterns
- Validation: Claude's discretion -- built-in vs separate, pick whichever is simpler and more reliable

#### JSON determinism
- Idempotency level: Claude's discretion -- pick the level of determinism that's practical (byte-identical vs logically identical)
- JSON key ordering: Claude's discretion -- decide based on what best serves the idempotency requirement
- JSON formatting (compact vs readable): Claude's discretion -- balance file size vs debuggability
- Sections column storage format: Claude's discretion -- pick based on how sections are currently stored and consumed in Supabase

#### Sidecar conventions
- File location: Claude's discretion -- follow existing sidecar patterns (fjms_enrichment.db, nli_crossref.db are in project root)
- Version scheme: Claude's discretion -- follow existing sidecar patterns (fjms_enrichment.db uses independent schema version)
- Distribution: Claude's discretion -- follow existing sidecar pattern (gitignored, bundled with installer/deployment)
- Meta table fields: Claude's discretion -- pick what's useful for debugging and verification

### Claude's Discretion
Many decisions in this phase are delegated to Claude -- this is a pure infrastructure/ETL phase where technical correctness matters more than visual/UX preferences. Key discretion areas:
- Script structure and module organization
- SQLite type mapping strategy
- Index creation during export
- Validation approach (built-in vs separate)
- JSON serialization details (key ordering, formatting, determinism level)
- Sections storage format
- File location, versioning, distribution model
- Meta table content

### Deferred Ideas (OUT OF SCOPE)
None -- discussion stayed within phase scope
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|-----------------|
| MIGR-01 | PGP data (documents, sources, footnotes, fragments) exported to pgp.db sidecar | Full schema mapping documented below; all 4 Supabase tables analyzed with complete column inventories; export pattern proven in two existing sidecars |
| MIGR-04 | JSON data (tags, sections) preserved correctly in SQLite with query parity | tags is JSONB array, sections is JSONB array-of-objects; `json.dumps(sort_keys=True)` ensures deterministic TEXT; `json.loads()` round-trip verified; SQLite `json_each()` enables tag queries |
| MIGR-08 | Export script rebuilds pgp.db from Supabase source data (repeatable) | Delete-and-recreate pattern from export_fist_enrichment.py; deterministic ORDER BY pgpid/id ensures stable row ordering; json sort_keys ensures stable JSON TEXT |
</phase_requirements>

## Standard Stack

### Core
| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| sqlite3 | stdlib | Create and populate pgp.db | Already used in all sidecar scripts; zero dependency |
| supabase-py | (unpinned) | Fetch data from Supabase REST API | Already used in all import scripts and the shared provider |
| json | stdlib | Serialize JSONB columns to TEXT | Standard library, deterministic with sort_keys |
| pathlib | stdlib | File path handling | Consistent with all existing scripts |
| datetime | stdlib | ISO-8601 timestamps for meta table | Used in both export_fist_enrichment.py and import_nli_crossref.py |
| dotenv | (unpinned) | Load .env for SUPABASE_URL/KEY | Already used in import_pgp_full.py |

### Supporting
| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| time | stdlib | Elapsed time per table | Progress output requirement |
| os | stdlib | Delete partial file on failure | Error handling requirement |
| sys | stdlib | Exit codes, path manipulation | Script structure |

### Alternatives Considered
| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| supabase-py client | Direct HTTP to PostgREST | More control but reinvents auth, less maintainable |
| tqdm progress | print() with elapsed time | Decision: tqdm NOT needed here; 4 tables, pagination is the progress indicator; simple print is sufficient |
| argparse | No args | Script has no modes (always exports from scratch), no args needed |

**Installation:**
No new dependencies needed. All libraries are already in the project's requirements.txt or stdlib.

## Architecture Patterns

### Recommended Project Structure
```
scripts/
├── export_pgp_sidecar.py    # New: Phase 35 export script
├── export_fist_enrichment.py # Existing: FJMS sidecar builder (pattern source)
├── import_nli_crossref.py    # Existing: NLI sidecar builder (pattern source)
├── import_pgp_full.py        # Existing: Supabase uploader (data source)
pgp_data/
├── pgp.db                    # New: Output sidecar file
├── documents.csv             # Existing: Original CSV data
├── footnotes.csv             # Existing
├── ...
```

### Pattern 1: Sidecar Export (from export_fist_enrichment.py)
**What:** Delete-and-recreate pattern for idempotent sidecar building
**When to use:** Every sidecar export in this project
**Example:**
```python
# Source: C:/GenizahSearch/scripts/export_fist_enrichment.py
def main():
    target_dir = project_dir / "pgp_data"
    target_path = target_dir / "pgp.db"

    # Delete existing target for idempotent re-runs
    if target_path.exists():
        os.remove(target_path)

    target_conn = sqlite3.connect(str(target_path))
    target = target_conn.cursor()
    target.execute("PRAGMA journal_mode=WAL")
    target.execute("PRAGMA synchronous=NORMAL")

    try:
        # ... export tables ...

        # Compact the database
        target.execute("PRAGMA journal_mode=DELETE")
        target_conn.commit()
        target.execute("VACUUM")
        target_conn.commit()
    except Exception:
        target_conn.close()
        if target_path.exists():
            os.remove(target_path)  # Clean up on failure
        raise
    finally:
        target_conn.close()
```

### Pattern 2: Supabase Pagination (from import_pgp_sections.py)
**What:** Page through Supabase table using `.range()` with 1000-row pages
**When to use:** Any Supabase table with >1000 rows (documents: 35,839; footnotes: 22,757; fragments: 36,155)
**Example:**
```python
# Source: C:/GenizahSearch/scripts/import_pgp_sections.py:279-293
def fetch_all_rows(client, table_name, select='*', order_by='id'):
    all_records = []
    offset = 0
    page_size = 1000

    while True:
        response = client.table(table_name).select(
            select
        ).order(order_by).range(offset, offset + page_size - 1).execute()

        if not response.data:
            break

        all_records.extend(response.data)
        if len(response.data) < page_size:
            break
        offset += page_size

    return all_records
```

### Pattern 3: Meta Table (from both existing sidecars)
**What:** Key-value meta table with version and build metadata
**When to use:** Every sidecar
**Example:**
```python
# Source: C:/GenizahSearch/scripts/export_fist_enrichment.py:505-525
def create_meta(target):
    target.execute("DROP TABLE IF EXISTS meta")
    target.execute("""
        CREATE TABLE meta (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )
    """)
    now = datetime.now(timezone.utc).isoformat()
    target.executemany(
        "INSERT INTO meta (key, value) VALUES (?, ?)",
        [
            ("version", VERSION),
            ("created", now),
            ("source", "supabase"),
        ],
    )
```

### Pattern 4: JSON Column Serialization
**What:** JSONB columns stored as TEXT with deterministic serialization
**When to use:** tags (JSONB array) and sections (JSONB array of objects) columns
**Example:**
```python
def serialize_json(value):
    """Serialize a Python object (from Supabase JSONB) to deterministic JSON TEXT."""
    if value is None:
        return None
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(',', ':'))
```

### Anti-Patterns to Avoid
- **Streaming from Supabase without ORDER BY:** PostgREST does not guarantee row order across paginated requests without explicit ORDER BY. Always add `.order('pgpid')` or `.order('id')` to ensure deterministic pagination.
- **Storing generated columns:** `pgp_url` is `GENERATED ALWAYS AS ('https://geniza.princeton.edu/documents/' || pgpid || '/')` in Supabase. Don't try to SELECT it then INSERT it into SQLite -- either recreate it as a generated column or compute it in the service layer. Recommendation: omit from SQLite, compute in service layer (trivial string concat).
- **Large SELECT * without pagination:** Supabase PostgREST caps at 1000 rows. Even the smallest table (document_sources, 9,364 rows) exceeds this.
- **Using created_at for ordering:** TIMESTAMPTZ has sub-second precision that varies; use the natural primary key (pgpid, id) for deterministic ordering.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Supabase pagination | Custom HTTP requests | supabase-py `.range()` | Auth, headers, error handling already done |
| JSON serialization | Custom JSONB parser | `json.dumps(sort_keys=True)` | Supabase-py already returns Python dicts/lists from JSONB |
| SQLite batch insert | Row-by-row INSERT | `executemany()` with batches | 10-50x faster for large tables |
| File hashing for idempotency | SHA256 of file contents | Row count + JSON round-trip validation | Byte-identical files are impractical due to SQLite internal metadata; logical equivalence is the right level |

**Key insight:** The Supabase Python client already deserializes JSONB into native Python objects. The export script receives `tags` as `["communal", "marriage"]` (Python list) and `sections` as `[{"canvas_url": "...", "canvas_num": 1, ...}]` (Python list of dicts). Serializing to TEXT with `json.dumps(sort_keys=True)` is all that's needed.

## Common Pitfalls

### Pitfall 1: Supabase 1000-Row Default Limit
**What goes wrong:** Fetching a table with `.select('*').execute()` silently returns only the first 1000 rows.
**Why it happens:** PostgREST default page size is 1000 rows.
**How to avoid:** Always use `.range()` pagination loop (Pattern 2 above). Validate final count against Supabase `select('*', count='exact', head=True)`.
**Warning signs:** Table row count in pgp.db is exactly 1000 or less than expected.

### Pitfall 2: Generated Column `pgp_url`
**What goes wrong:** `SELECT * FROM documents` in Supabase returns `pgp_url` as a real column, but it's `GENERATED ALWAYS AS STORED`. Attempting to INSERT this value into a SQLite table that also defines it as generated will fail, or inserting it into a regular TEXT column wastes space.
**Why it happens:** Supabase returns generated columns in `SELECT *` responses.
**How to avoid:** Either exclude `pgp_url` from the SQLite schema (compute in service layer at read time) or store it as a plain TEXT column. Recommendation: store it -- it's a small overhead (40 chars per row x 36K = 1.4 MB) and simplifies Phase 36 by keeping the schema identical.
**Warning signs:** Column mismatch errors during INSERT.

### Pitfall 3: NULL vs Empty String Fidelity
**What goes wrong:** Transforming NULLs to empty strings or vice versa changes downstream behavior (e.g., `if value:` check behaves differently for None vs '').
**Why it happens:** Tempting to "normalize" during export.
**How to avoid:** Locked decision: "Verbatim export -- NULLs stay NULL, empty strings stay empty." Simply pass values through unchanged. SQLite preserves NULL natively.
**Warning signs:** Tests comparing Supabase response dicts to SQLite query results showing mismatches.

### Pitfall 4: JSONB NULL vs Python None
**What goes wrong:** A JSONB column that is SQL NULL (never set) is different from a JSONB column containing the JSON literal `null`. Supabase-py returns Python `None` for both.
**Why it happens:** PostgREST/supabase-py doesn't distinguish SQL NULL from JSON null.
**How to avoid:** For this dataset, SQL NULL means "no value" and JSON `null` is not used in tags or sections. Simply store `None` as SQLite NULL (don't call json.dumps on None).
**Warning signs:** Rows with "null" as literal TEXT instead of SQLite NULL.

### Pitfall 5: Boolean Column Mapping
**What goes wrong:** Supabase returns `has_transcription` and `has_translation` as Python booleans (`True`/`False`). SQLite has no native boolean type -- stores as INTEGER 0/1.
**Why it happens:** Type affinity mismatch between PostgreSQL BOOLEAN and SQLite.
**How to avoid:** Let SQLite handle it naturally -- `sqlite3` module maps Python `True` to 1 and `False` to 0. On read-back, service layer will get 0/1 which is truthy/falsy in Python. For explicit mapping, use `bool(value)` on read.
**Warning signs:** Queries like `WHERE has_transcription = True` failing in SQLite (use `WHERE has_transcription = 1` or `WHERE has_transcription`).

### Pitfall 6: Partial Export Corruption
**What goes wrong:** Export fails mid-way (network timeout, Supabase rate limit), leaving a partial pgp.db that appears valid but has incomplete data.
**Why it happens:** SQLite file exists with some tables populated.
**How to avoid:** Locked decision: "On failure, delete partial pgp.db." Wrap the entire export in try/except, delete on any exception. Add row count validation at the end.
**Warning signs:** pgp.db exists but row counts don't match Supabase.

## Code Examples

### Complete Table Schema: documents
```sql
-- Source: C:/GenizahSearch/supabase_setup.sql + all migration files
CREATE TABLE documents (
    pgpid INTEGER PRIMARY KEY,
    shelfmark_combined TEXT,
    document_type TEXT,
    tags TEXT,                    -- JSONB -> TEXT (json.dumps)
    doc_date_original TEXT,
    doc_date_standard TEXT,
    doc_date_calendar TEXT,       -- from add_full_pgp_columns.sql
    inferred_date_display TEXT,
    inferred_date_standard TEXT,  -- from add_pgp_metadata_columns.sql
    inferred_date_rationale TEXT, -- from add_pgp_metadata_columns.sql
    inferred_date_notes TEXT,     -- from add_full_pgp_columns.sql
    description TEXT,
    transcription TEXT,
    transcription_source TEXT,
    languages_primary TEXT,       -- from add_pgp_metadata_columns.sql
    languages_secondary TEXT,     -- from add_pgp_metadata_columns.sql
    language_note TEXT,           -- from add_full_pgp_columns.sql
    scholarship_records TEXT,     -- from add_full_pgp_columns.sql
    shelfmarks_historic TEXT,     -- from add_full_pgp_columns.sql
    has_transcription INTEGER,    -- BOOLEAN -> INTEGER (0/1)
    has_translation INTEGER,      -- BOOLEAN -> INTEGER (0/1)
    input_by TEXT,                -- from add_full_pgp_columns.sql
    pgp_url TEXT,                 -- GENERATED in Supabase, stored as plain TEXT
    created_at TEXT               -- TIMESTAMPTZ -> TEXT ISO-8601
);
```

### Complete Table Schema: document_sources
```sql
-- Source: C:/GenizahSearch/migrations/create_document_sources_table.sql + add_sections_column.sql
CREATE TABLE document_sources (
    id INTEGER PRIMARY KEY,       -- BIGSERIAL -> INTEGER (auto-increment not needed, IDs come from Supabase)
    pgpid INTEGER NOT NULL,
    source_scholar TEXT NOT NULL,
    doc_relation TEXT NOT NULL,
    language TEXT,
    content TEXT NOT NULL,
    content_length INTEGER,
    source_url TEXT,
    notes TEXT,
    sequence_order INTEGER DEFAULT 1,
    sections TEXT,                -- JSONB -> TEXT (json.dumps)
    source_language TEXT,         -- from add_sections_column.sql
    source_direction TEXT,        -- from add_sections_column.sql
    created_at TEXT               -- TIMESTAMPTZ -> TEXT ISO-8601
);
```

### Complete Table Schema: document_footnotes
```sql
-- Source: C:/GenizahSearch/migrations/create_footnotes_table.sql
CREATE TABLE document_footnotes (
    id INTEGER PRIMARY KEY,       -- BIGSERIAL -> INTEGER
    pgpid INTEGER NOT NULL,
    source TEXT NOT NULL,
    source_slug TEXT,
    doc_relation TEXT NOT NULL,
    location TEXT,
    url TEXT,
    notes TEXT,
    content TEXT,
    content_length INTEGER,
    created_at TEXT               -- TIMESTAMPTZ -> TEXT ISO-8601
);
```

### Complete Table Schema: document_fragments
```sql
-- Source: C:/GenizahSearch/supabase_setup.sql + add_page_info_column.sql + add_full_pgp_columns.sql
CREATE TABLE document_fragments (
    id INTEGER PRIMARY KEY,       -- SERIAL -> INTEGER
    document_id INTEGER NOT NULL,
    sys_id TEXT NOT NULL,
    shelfmark TEXT,
    sequence_order INTEGER DEFAULT 1,
    page_info TEXT,               -- from add_page_info_column.sql
    collection TEXT,              -- from add_full_pgp_columns.sql
    library TEXT,                 -- from add_full_pgp_columns.sql
    library_abbrev TEXT,          -- from add_full_pgp_columns.sql
    fragment_url TEXT,            -- from add_full_pgp_columns.sql
    iiif_url TEXT,                -- from add_full_pgp_columns.sql
    created_at TEXT               -- TIMESTAMPTZ -> TEXT ISO-8601
);
```

### Index Strategy (Phase 36 Query Patterns)
```sql
-- Source: Analysis of C:/GenizahSearch/shared/document_service.py query patterns

-- documents: Primary key (pgpid) covers most lookups
-- Tags: json_each() requires scanning rows; GIN equivalent not possible in SQLite
-- document_type is used for filtering in browse page
CREATE INDEX idx_doc_type ON documents(document_type);

-- document_sources: Primary lookups by pgpid
CREATE INDEX idx_sources_pgpid ON document_sources(pgpid);
CREATE INDEX idx_sources_relation ON document_sources(pgpid, doc_relation);

-- document_footnotes: Primary lookups by pgpid
CREATE INDEX idx_footnotes_pgpid ON document_footnotes(pgpid);
CREATE INDEX idx_footnotes_relation ON document_footnotes(pgpid, doc_relation);

-- document_fragments: Lookups by sys_id (most common: find document for fragment)
-- and by document_id (find fragments for document)
CREATE INDEX idx_fragments_sys_id ON document_fragments(sys_id);
CREATE INDEX idx_fragments_document_id ON document_fragments(document_id);

-- Unique constraint equivalent (for data integrity verification)
CREATE UNIQUE INDEX idx_fragments_unique ON document_fragments(document_id, sys_id);
```

### Pagination + Serialization Example
```python
def fetch_and_export_documents(client, target):
    """Fetch all documents from Supabase and insert into SQLite."""
    start = time.time()
    all_rows = []
    offset = 0
    page_size = 1000

    while True:
        response = client.table('documents').select('*').order(
            'pgpid'
        ).range(offset, offset + page_size - 1).execute()

        if not response.data:
            break
        all_rows.extend(response.data)
        if len(response.data) < page_size:
            break
        offset += page_size

    # Insert with JSON serialization for tags column
    for row in all_rows:
        target.execute(
            "INSERT INTO documents VALUES (?, ?, ?, ?, ...)",
            (
                row['pgpid'],
                row.get('shelfmark_combined'),
                row.get('document_type'),
                serialize_json(row.get('tags')),  # JSONB -> TEXT
                # ... remaining columns ...
            )
        )

    elapsed = time.time() - start
    print(f"  documents: {len(all_rows):,} rows ({elapsed:.1f}s)")
    return len(all_rows)
```

### Validation Pattern
```python
def validate_export(client, target_conn):
    """Validate pgp.db matches Supabase row counts and JSON round-trips."""
    errors = []

    # Row count validation
    for table, sqlite_table in [
        ('documents', 'documents'),
        ('document_sources', 'document_sources'),
        ('document_footnotes', 'document_footnotes'),
        ('document_fragments', 'document_fragments'),
    ]:
        # Supabase count
        resp = client.table(table).select('*', count='exact', head=True).execute()
        supabase_count = resp.count or 0

        # SQLite count
        cursor = target_conn.execute(f"SELECT COUNT(*) FROM {sqlite_table}")
        sqlite_count = cursor.fetchone()[0]

        if supabase_count != sqlite_count:
            errors.append(f"{table}: Supabase={supabase_count}, SQLite={sqlite_count}")
        else:
            print(f"  {table}: {sqlite_count:,} rows (matches)")

    # JSON round-trip: sample tags
    cursor = target_conn.execute(
        "SELECT pgpid, tags FROM documents WHERE tags IS NOT NULL LIMIT 10"
    )
    for row in cursor:
        parsed = json.loads(row[1])
        if not isinstance(parsed, list):
            errors.append(f"pgpid {row[0]}: tags round-trip failed (got {type(parsed)})")

    return errors
```

## Discretion Recommendations

Based on codebase analysis, here are my recommendations for the areas left to Claude's discretion:

### Script Structure
**Recommendation:** Standalone script at `scripts/export_pgp_sidecar.py`
**Rationale:** Both existing sidecar builders (`export_fist_enrichment.py`, `import_nli_crossref.py`) are standalone scripts in `scripts/`. No shared module needed since this is a one-off build tool, not a runtime service.

### File Location
**Recommendation:** `pgp_data/pgp.db`
**Rationale:** FJMS uses `fist_data/fjms_enrichment.db`, NLI uses `nli_data/nli_crossref.db`. Following this pattern, PGP data belongs in `pgp_data/pgp.db`. The `pgp_data/` directory already exists with CSV source files.

### Distribution
**Recommendation:** Add `pgp_data/pgp.db` to `.gitignore` (via `pgp_data/` which is NOT currently gitignored -- need to add it or use a specific entry). Bundle in `build_app.bat` like existing sidecars.
**Note:** `pgp_data/` is NOT in `.gitignore` currently. The directory's CSV files are already tracked as untracked. The `pgp.db` file should be gitignored individually or the sidecar should go in a subdirectory. Simplest: add `pgp_data/pgp.db` to `.gitignore`.

### Version Scheme
**Recommendation:** Start at `"1.0.0"`, independent version like both existing sidecars.
**Rationale:** FJMS is at `2.0.0`, NLI at `1.2.0`. Independent semver for each sidecar.

### Meta Table Fields
**Recommendation:**
```python
[
    ("version", "1.0.0"),
    ("created", datetime.now(timezone.utc).isoformat()),
    ("source", "supabase"),
    ("supabase_url", SUPABASE_URL),  # For audit trail
    ("documents_count", str(doc_count)),
    ("sources_count", str(source_count)),
    ("footnotes_count", str(footnote_count)),
    ("fragments_count", str(frag_count)),
]
```
**Rationale:** Row counts in meta enable quick validation without querying tables. Source URL documents which Supabase instance was used.

### Type Mapping Strategy
**Recommendation:**
| PostgreSQL Type | SQLite Type | Handling |
|----------------|-------------|----------|
| INTEGER / SERIAL / BIGSERIAL | INTEGER | Direct pass-through |
| TEXT | TEXT | Direct pass-through |
| BOOLEAN | INTEGER | Python True/False maps to 1/0 automatically |
| JSONB | TEXT | `json.dumps(sort_keys=True, ensure_ascii=False, separators=(',',':'))` for non-None; None stays NULL |
| TIMESTAMPTZ | TEXT | Stored as-is from Supabase (ISO-8601 string) |
| GENERATED (pgp_url) | TEXT | Store the value returned by Supabase (saves recomputing) |

### JSON Serialization Details
**Recommendation:** Compact format with sorted keys.
- `json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(',', ':'))`
- `sort_keys=True`: Ensures deterministic output for idempotency
- `ensure_ascii=False`: Preserves Hebrew/Arabic characters without escape sequences (smaller, readable)
- `separators=(',', ':')`: Compact format (no spaces) saves ~10% disk for sections column
- `None` -> SQLite NULL (not serialized)

### Idempotency Level
**Recommendation:** Logically identical, not byte-identical.
**Rationale:** SQLite internal metadata (page allocation, WAL state) varies between runs. Byte-identical files would require controlling internal SQLite behavior, which is impractical. Instead: same row counts, same column values, same JSON content (verified by round-trip). Re-running produces functionally equivalent output.

### Sections Storage
**Recommendation:** Store as JSON TEXT, same as tags. The `sections` column in `document_sources` is JSONB in Supabase containing `[{"canvas_url": "...", "canvas_num": 1, "label": null, "text": "...", "subsections": null}]`. Serialize with `json.dumps(sort_keys=True)`. The service layer already does `section.get('canvas_num')` etc., which works identically on `json.loads()` output.

### Index Strategy
**Recommendation:** Create indexes matching Phase 36 query patterns (documented in Code Examples above). Primary focus:
- `document_fragments.sys_id` (most frequent query: find document for fragment)
- `document_fragments.document_id` (find fragments for document)
- `document_sources.pgpid` (find sources for document)
- `document_footnotes.pgpid` (find footnotes for document)
- `documents.document_type` (browse filtering)
- UNIQUE on `document_fragments(document_id, sys_id)` (data integrity)

### Validation Approach
**Recommendation:** Built-in, at end of export script. After all 4 tables exported:
1. Fetch row counts from Supabase using `count='exact', head=True`
2. Compare to SQLite `COUNT(*)`
3. Sample 10 rows with JSON columns, verify `json.loads()` produces list/dict
4. Print pass/fail summary
5. If any validation fails, print error and delete pgp.db

**Rationale:** Built-in is simpler than a separate script, and validation must run every time the export runs (not optionally).

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| Supabase live queries | Local SQLite sidecar | v6.0.0 (this milestone) | Eliminates network dependency for read-only PGP data |
| PostgREST JSONB containment (@>) | SQLite json_each() | v6.0.0 Phase 36 | Different query syntax, same functionality |
| CSV import scripts | Direct Supabase export | This phase | Source data is live database, not static files |

**Deprecated/outdated:**
- The original `import_pgp_documents.py` and `import_document_sources.py` scripts imported FROM CSV TO Supabase. This phase does the reverse: FROM Supabase TO SQLite.

## Open Questions

1. **Supabase Rate Limiting**
   - What we know: Supabase free tier has rate limits. This export makes ~150 requests total (36K documents / 1000 per page = 36 requests, etc.)
   - What's unclear: Whether 150 requests in quick succession triggers any throttling
   - Recommendation: Add a small sleep (0.1s) between pages if rate limiting is observed. Not preemptive -- only if needed.

2. **pgp_url Generated Column Behavior**
   - What we know: Supabase returns `pgp_url` in `SELECT *` responses as a real value
   - What's unclear: Whether the supabase-py client includes generated columns by default
   - Recommendation: Test during implementation. If not included, either exclude from schema or compute during insert. The value is trivially computable: `f'https://geniza.princeton.edu/documents/{pgpid}/'`

## Sources

### Primary (HIGH confidence)
- `C:/GenizahSearch/scripts/export_fist_enrichment.py` - Complete sidecar export pattern
- `C:/GenizahSearch/scripts/import_nli_crossref.py` - Complete sidecar import pattern
- `C:/GenizahSearch/scripts/import_pgp_sections.py:279-293` - Supabase `.range()` pagination pattern
- `C:/GenizahSearch/shared/document_service.py` - All PGP query patterns (Phase 36 consumers)
- `C:/GenizahSearch/supabase_setup.sql` - Core Supabase schema
- `C:/GenizahSearch/migrations/*.sql` - All 8 migration files documenting complete schema evolution
- `C:/GenizahSearch/shared/fjms_service.py` - Sidecar service pattern (read-only, URI mode, meta table)
- `C:/GenizahSearch/shared/nli_crossref_service.py` - Sidecar service pattern
- `C:/GenizahSearch/.gitignore` - Distribution patterns (fist_data/, nli_data/ gitignored)
- `C:/GenizahSearch/build_app.bat` - Desktop bundling pattern

### Secondary (MEDIUM confidence)
- `C:/GenizahSearch/scripts/import_pgp_full.py` - Table counts verification pattern using `count='exact', head=True`
- `C:/GenizahSearch/shared/supabase_provider.py` - Supabase client factory with credentials

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH - All libraries already in use, no new dependencies
- Architecture: HIGH - Two existing sidecars provide proven patterns; complete schema documented from source files
- Pitfalls: HIGH - All pitfalls identified from codebase analysis and Supabase REST API behavior

**Research date:** 2026-02-17
**Valid until:** 2026-03-17 (stable domain, patterns proven in codebase)
