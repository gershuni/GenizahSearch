# Architecture Patterns

**Domain:** PGP sidecar migration + FJMS full text integration
**Researched:** 2026-02-16
**Confidence:** HIGH (evidence-based from full codebase analysis)

## Current Architecture (Before)

```
Web App (NiceGUI)
  |
  +-- web/pages/search.py        --> from web.document_service import ...  (shim)
  +-- web/pages/browse.py        --> from shared.document_service import ...
  +-- web/services.py            --> from shared.nli_crossref_service import ...
  +-- web/components/joins_panel  --> from web.fjms_service import ...  (shim)
  |
  +-- Supabase (PostgreSQL)  <---- PGP data (documents, sources, footnotes, fragments)
  |     via shared/supabase_provider.py -> get_client()
  |
  +-- Tantivy Index (local)  <---- Full-text search
  +-- fist_data/fjms_enrichment.db  <---- FJMS enrichment (SQLite sidecar)
  +-- nli_data/nli_crossref.db      <---- NLI crossref (SQLite sidecar)

Desktop App (PyQt6)
  |
  +-- genizah_app.py             --> from shared.document_service import ...  (lazy)
  +-- genizah_app.py             --> from shared.fjms_service import ...
  |
  +-- Supabase (PostgreSQL)  <---- Same PGP data + community features
  +-- Tantivy Index (local)
  +-- fist_data/fjms_enrichment.db
  +-- nli_data/nli_crossref.db
```

### Current document_service.py API Surface

All functions use `from shared.supabase_provider import get_client`:

| Function | Callers | Tables Hit |
|----------|---------|------------|
| `get_document_for_fragment(sys_id, page_num)` | web search, web browse, desktop (3 sites) | document_fragments, documents |
| `get_fragments_for_document(pgpid)` | web browse, desktop (3 sites) | document_fragments |
| `get_transcription_for_document(pgpid)` | (unused in current code) | documents |
| `get_document_metadata(pgpid)` | (unused in current code) | documents |
| `get_sources_for_document(pgpid)` | web browse | document_sources |
| `get_all_sources_for_fragment(sys_id)` | web search, web browse | document_fragments, document_sources |
| `get_editions_for_document(pgpid)` | (unused in current code) | document_sources |
| `get_translations_for_document(pgpid)` | (unused in current code) | document_sources |
| `get_sys_ids_with_transcriptions(sys_ids)` | web search, desktop gui_threads | document_fragments |
| `get_fragments_by_tag(tag)` | web search | documents, document_fragments |
| `get_all_distinct_tags()` | web search | documents |
| `parse_transcription_sections(text)` | web browse | (pure function, no DB) |
| `get_section_for_page(text, page, sections)` | web search, web browse | (pure function, no DB) |
| `parse_html_sections(html)` | (import/test only) | (pure function, no DB) |

**Critical observation:** 4 functions are unused, 3 are pure (no DB). The actual DB-calling surface is 7 functions that need migration.

### Callers in Each App

**Web app (imports via shim or direct):**
- `web/pages/search.py:19` -- import via `web.document_service` shim (6 functions)
- `web/pages/browse.py` -- import via `shared.document_service` directly (5 sites)

**Desktop app (lazy imports inside methods):**
- `genizah_app.py:3037` -- `get_document_for_fragment, get_fragments_for_document`
- `genizah_app.py:7379` -- `get_fragments_for_document`
- `genizah_app.py:10146` -- `get_document_for_fragment`
- `genizah_app.py:10162` -- `get_fragments_for_document`

**gui_threads.py:531** -- `get_sys_ids_with_transcriptions` (batch check)

## Target Architecture (After)

```
Web App (NiceGUI)
  |
  +-- web/pages/search.py        --> from web.document_service import ...  (shim, UNCHANGED)
  +-- web/pages/browse.py        --> from shared.document_service import ...  (UNCHANGED)
  |
  +-- Supabase (PostgreSQL)  <---- Community ONLY (auth, corrections, lists, comments)
  |     via web/supabase_client.py (community)
  |
  +-- Tantivy Index (local)
  +-- pgp_data/pgp.db               <---- NEW: PGP sidecar (SQLite)
  +-- fist_data/fjms_enrichment.db   <---- UPDATED: + full_texts table
  +-- nli_data/nli_crossref.db

Desktop App (PyQt6)
  |
  +-- genizah_app.py             --> from shared.document_service import ...  (UNCHANGED)
  |
  +-- Supabase                   <---- Community ONLY
  +-- Tantivy Index (local)
  +-- pgp_data/pgp.db
  +-- fist_data/fjms_enrichment.db
  +-- nli_data/nli_crossref.db
```

### Key Change: document_service.py Internal Switch

```
BEFORE:                                   AFTER:
shared/document_service.py                shared/document_service.py
  |                                         |
  +-- from shared.supabase_provider         +-- from shared.pgp_service import get_pgp_service
  |     import get_client                   |
  +-- client.table('documents')...          +-- pgp_svc.get_document(pgpid)
                                            +-- pgp_svc.get_fragments_for_sys_id(sys_id)
```

**The shim pattern (web/document_service.py) and ALL callers remain untouched.** The migration is entirely inside `shared/document_service.py`, swapping Supabase calls for PgpService calls.

## Recommended Architecture

### New Component: PgpService (shared/pgp_service.py)

Follows the exact pattern established by FjmsService and NliCrossrefService:

```python
class PgpService:
    """Service for accessing PGP reference data from the SQLite sidecar."""

    def __init__(self, db_path: str = None, thread_safe: bool = False):
        # Same pattern as FjmsService/NliCrossrefService:
        # - Auto-detect from project root
        # - Read-only URI mode
        # - thread_safe for NiceGUI web
        # - sqlite3.Row row factory
        pass

    def is_available(self) -> bool: ...
    def get_version(self) -> Optional[str]: ...

    # Core queries (replacing Supabase calls):
    def get_document(self, pgpid: int) -> Optional[dict]: ...
    def get_fragments_for_sys_id(self, sys_id: str) -> list[dict]: ...
    def get_fragments_for_document(self, pgpid: int) -> list[dict]: ...
    def get_sources_for_document(self, pgpid: int) -> list[dict]: ...
    def get_sys_ids_with_documents(self, sys_ids: list[str]) -> set[str]: ...
    def get_documents_by_tag(self, tag: str) -> list[dict]: ...
    def get_all_distinct_tags(self) -> list[str]: ...
    def get_footnotes_for_document(self, pgpid: int) -> list[dict]: ...

    def close(self): ...

# Singleton
_default_service = None
def get_pgp_service(thread_safe: bool = False) -> PgpService: ...
```

### Component Boundaries

| Component | Responsibility | Communicates With |
|-----------|---------------|-------------------|
| `shared/pgp_service.py` (NEW) | PGP data access from pgp.db | document_service.py |
| `shared/document_service.py` (MODIFIED) | PGP business logic, section parsing | pgp_service.py (was: supabase_provider) |
| `shared/fjms_service.py` (MODIFIED) | FJMS enrichment + NEW full texts | document_service.py (for source merging) |
| `shared/supabase_provider.py` (REMOVED) | Was: Supabase client factory | Nothing after migration |
| `web/supabase_client.py` (KEPT) | Community features (auth, lists, corrections) | Web app community pages |
| `supabase_corrections_client.py` (KEPT) | Desktop community features | Desktop app corrections |
| `scripts/export_pgp_sidecar.py` (NEW) | Export Supabase -> pgp.db | Run once during migration |
| `scripts/export_fist_texts.py` (NEW) | Export FIST TextualFrame -> fjms_enrichment.db | Run once to add texts |

### Data Flow

**Before (search result enrichment):**
```
User searches -> Tantivy -> result sys_ids
  -> get_sys_ids_with_transcriptions(sys_ids)
     -> Supabase HTTP: document_fragments.select().in_(sys_ids)
     -> Returns set of sys_ids with PGP links
```

**After:**
```
User searches -> Tantivy -> result sys_ids
  -> get_sys_ids_with_transcriptions(sys_ids)
     -> pgp_service.get_sys_ids_with_documents(sys_ids)
        -> SQLite: SELECT DISTINCT sys_id FROM document_fragments WHERE sys_id IN (...)
     -> Returns set of sys_ids with PGP links
```

**Before (browse page source display):**
```
User opens manuscript -> get_all_sources_for_fragment(sys_id)
  -> Supabase HTTP: document_fragments.select().eq(sys_id)
  -> For each pgpid: Supabase HTTP: document_sources.select().eq(pgpid)
  -> Returns list of source dicts
```

**After:**
```
User opens manuscript -> get_all_sources_for_fragment(sys_id)
  -> pgp_service.get_fragments_for_sys_id(sys_id)
     -> SQLite: SELECT * FROM document_fragments WHERE sys_id = ?
  -> pgp_service.get_sources_for_document(pgpid)
     -> SQLite: SELECT * FROM document_sources WHERE pgpid = ?
  -> OPTIONAL: fjms_service.get_full_texts(sys_id)
     -> SQLite: SELECT * FROM full_texts WHERE AlmaId = ?
  -> Merge PGP + FJMS sources, deduplicate
  -> Returns combined list of source dicts
```

## pgp.db Schema Design

### Table: documents

```sql
CREATE TABLE documents (
    pgpid INTEGER PRIMARY KEY,
    shelfmark_combined TEXT,
    document_type TEXT,
    tags TEXT,                       -- JSON array string: '["communal","marriage"]'
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
    doc_relation TEXT,
    languages_primary TEXT,
    languages_secondary TEXT,
    language_note TEXT,
    scholarship_records TEXT,
    shelfmarks_historic TEXT,
    has_transcription INTEGER,       -- 0/1 (SQLite boolean)
    has_translation INTEGER,         -- 0/1
    input_by TEXT
);
-- pgp_url is computed: no need to store, generate in service layer
```

**Rationale for tags as TEXT:** Supabase uses JSONB but SQLite has no native JSONB. Store as JSON string, parse with `json.loads()` in the service layer. The json1 extension is available but for read-only tag queries, Python-side parsing is simpler and more portable.

### Table: document_fragments

```sql
CREATE TABLE document_fragments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    document_id INTEGER NOT NULL REFERENCES documents(pgpid),
    sys_id TEXT NOT NULL,
    shelfmark TEXT,
    sequence_order INTEGER DEFAULT 1,
    page_info TEXT,                   -- 'recto' or 'verso'
    collection TEXT,
    library TEXT,
    library_abbrev TEXT,
    fragment_url TEXT,
    iiif_url TEXT,
    UNIQUE(document_id, sys_id)
);

CREATE INDEX idx_fragments_sys_id ON document_fragments(sys_id);
CREATE INDEX idx_fragments_document_id ON document_fragments(document_id);
```

### Table: document_sources

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
    sections TEXT,                    -- JSON string (was JSONB in Supabase)
    source_language TEXT,
    source_direction TEXT,
    sequence_order INTEGER DEFAULT 1
);

CREATE INDEX idx_sources_pgpid ON document_sources(pgpid);
CREATE INDEX idx_sources_relation ON document_sources(pgpid, doc_relation);
```

### Table: document_footnotes

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
    content_length INTEGER
);

CREATE INDEX idx_footnotes_pgpid ON document_footnotes(pgpid);
CREATE INDEX idx_footnotes_relation ON document_footnotes(pgpid, doc_relation);
```

### Table: meta

```sql
CREATE TABLE meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
-- Stores: version, created, source
```

### Indexes

The indexes above directly mirror the Supabase indexes. Key queries to optimize:

1. `SELECT * FROM document_fragments WHERE sys_id = ?` -- most common (every browse/search)
2. `SELECT DISTINCT sys_id FROM document_fragments WHERE sys_id IN (...)` -- batch check
3. `SELECT * FROM documents WHERE pgpid = ?` -- document lookup
4. `SELECT * FROM document_sources WHERE pgpid = ?` -- source listing
5. Tag search via `json_each()` or LIKE pattern

For tag search, two options:
- **Option A:** `json_each()` with json1 extension (cleaner but requires extension)
- **Option B:** Tags as separate normalized table
- **Recommendation:** Option A with json1. SQLite's json1 is built-in since Python 3.9+ and `json_each()` with a simple JOIN is fast enough for 35K documents. But if performance is an issue, a normalized tags table can be added later.

### Estimated Sizes

| Table | Rows | Est. Size |
|-------|------|-----------|
| documents | 35,839 | ~50 MB (transcription TEXT is bulk) |
| document_fragments | 36,155 | ~3 MB |
| document_sources | 9,364 | ~40 MB (content TEXT is bulk) |
| document_footnotes | 22,757 | ~15 MB |
| meta | 3 | <1 KB |
| **Total** | **104,118** | **~110 MB** |

This is small enough to ship as a single file with the desktop app.

## FJMS Full Texts Integration

### New Table in fjms_enrichment.db: full_texts

```sql
CREATE TABLE full_texts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    AlmaId TEXT NOT NULL,
    content TEXT NOT NULL,
    content_length INTEGER,
    source_name TEXT,            -- Scholar/source attribution from FIST
    source_name_heb TEXT,
    language TEXT,               -- Detected language
    UNIQUE(AlmaId, content_length)   -- Dedup by content length per AlmaId
);

CREATE INDEX idx_full_texts_alma ON full_texts(AlmaId);
```

### Source Merging Strategy

FJMS full texts appear alongside PGP sources in the version selector. The merge order:

```
1. PGP Digital Editions (primary scholarly transcriptions)
2. PGP Digital Translations
3. FJMS Full Texts (marked with "FJMS" badge, distinct from PGP)
4. MiDRASH auto-transcriptions (existing V0.8/V0.7)
5. User corrections (pending, shown to submitter only)
```

### Deduplication

Many FJMS texts may overlap with PGP transcriptions. Dedup approach:

```python
def merge_sources(pgp_sources: list, fjms_texts: list) -> list:
    """Merge PGP and FJMS sources, deduplicating overlaps."""
    # Track PGP content fingerprints
    pgp_fingerprints = set()
    for src in pgp_sources:
        if src.get('content'):
            # Normalize: strip whitespace, collapse spaces
            fp = normalize_for_dedup(src['content'])
            pgp_fingerprints.add(fp)

    merged = list(pgp_sources)  # PGP first
    for text in fjms_texts:
        fp = normalize_for_dedup(text['content'])
        if fp not in pgp_fingerprints:
            merged.append({
                'source_scholar': text.get('source_name', 'FJMS'),
                'doc_relation': 'FJMS Edition',
                'content': text['content'],
                'content_length': text.get('content_length'),
                'language': text.get('language'),
                'source': 'fjms',  # Marker for UI badge
            })
    return merged
```

**Fingerprint approach:** Normalize whitespace + strip diacritics + lowercase. If the first 200 chars match, consider it a duplicate. This handles minor formatting differences between PGP and FJMS versions of the same text.

## Patterns to Follow

### Pattern 1: Sidecar Service (Established)

**What:** SQLite read-only service with singleton, thread-safety toggle, auto-detect
**When:** Any new reference data source
**Example:** FjmsService, NliCrossrefService -- PgpService follows identically

```python
class PgpService:
    def __init__(self, db_path=None, thread_safe=False):
        # Auto-detect: project_root / pgp_data / pgp.db
        # Read-only URI: file:{path}?mode=ro
        # check_same_thread=not thread_safe
        # row_factory = sqlite3.Row
```

### Pattern 2: Shim Re-export (Established)

**What:** web/document_service.py re-exports from shared/document_service.py
**When:** Maintaining backward compatibility during migration
**Why:** Zero changes to web/pages/search.py import line

### Pattern 3: Lazy Import in Desktop (Established)

**What:** Desktop app uses `from shared.X import Y` inside methods, not at module level
**When:** Desktop features that may not always need the service
**Example:** `genizah_app.py:3037` -- import inside try block

### Pattern 4: Graceful Degradation (Established)

**What:** Service returns empty/None when .db file missing
**When:** Always -- user may not have the sidecar file yet

## Anti-Patterns to Avoid

### Anti-Pattern 1: Breaking the Shim Contract

**What:** Changing function signatures in shared/document_service.py
**Why bad:** web/pages/search.py imports 6 functions by name. If signatures change, both apps break.
**Instead:** Keep all existing function signatures identical. Change only the internal implementation (Supabase -> SQLite).

### Anti-Pattern 2: Mixed Supabase+SQLite in Transition

**What:** Having document_service.py call Supabase for some functions and SQLite for others
**Why bad:** Two connection types, two failure modes, two test approaches
**Instead:** Clean cutover. All functions switch to PgpService in one phase.

### Anti-Pattern 3: Storing Computed pgp_url in SQLite

**What:** Storing `https://geniza.princeton.edu/documents/{pgpid}/` as a column
**Why bad:** Supabase had it as GENERATED ALWAYS, but it wastes space and is trivially computed
**Instead:** Generate in PgpService: `def get_pgp_url(pgpid): return f"https://geniza.princeton.edu/documents/{pgpid}/"`

### Anti-Pattern 4: Premature Normalization of Tags

**What:** Creating a normalized tags table before proving json_each is too slow
**Why bad:** Extra complexity, extra export logic, extra join in queries -- for 35K rows, unnecessary
**Instead:** Start with JSON text + json_each(). Add normalized table only if profiling shows a bottleneck.

## Scalability Considerations

| Concern | Current (35K docs) | At 100K docs | At 500K docs |
|---------|---------------------|--------------|-------------|
| pgp.db file size | ~110 MB | ~300 MB | ~1.5 GB |
| Batch sys_id check | <50ms (SQLite IN clause, 500 batch) | <100ms | Consider FTS or separate index |
| Tag search | <100ms (json_each on 35K rows) | May need normalized tags table | Definitely need normalized table |
| Desktop app startup | <1s (connection only) | Same | Same (lazy queries) |
| Web concurrent reads | Thread-safe mode, read-only | Same | Same (read-only, no WAL needed) |

**Note:** Since the database is read-only (opened with `?mode=ro`), WAL mode is not needed. Multiple readers can proceed without contention.

## Sources

- `shared/document_service.py` -- 742 lines, 14 functions (7 DB-calling, 4 unused, 3 pure)
- `shared/fjms_service.py` -- 961 lines, FjmsService pattern
- `shared/nli_crossref_service.py` -- 845 lines, NliCrossrefService pattern
- `shared/supabase_provider.py` -- 45 lines, will be removed
- `scripts/import_pgp_full.py` -- Full Supabase schema and import logic
- `migrations/*.sql` -- 9 migration files defining Supabase schema
- `docs/plans/FIST_INTEGRATION_DESIGN.md` -- FIST data architecture
- `.planning/PROJECT.md` -- v6.0.0 scope definition
