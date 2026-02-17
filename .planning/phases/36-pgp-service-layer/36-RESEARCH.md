# Phase 36: PGP Service Layer - Research

**Researched:** 2026-02-17
**Domain:** SQLite sidecar service layer / Supabase-to-SQLite migration
**Confidence:** HIGH

## Summary

Phase 36 rewrites `shared/document_service.py` to read from the local `pgp_data/pgp.db` SQLite sidecar (created in Phase 35) instead of making live Supabase REST API calls. The module exposes 14 public functions that both the web app (NiceGUI) and desktop app (PyQt6) consume via lazy imports. The web app additionally imports through a backward-compatible shim at `web/document_service.py`.

The rewrite is structurally straightforward: every function currently calls `get_client()` to get a Supabase client, then chains `.table().select().eq().execute()`. The replacement uses `sqlite3` with parameterized queries on the identical schema. The primary complexity lies in three areas:

1. **JSON column deserialization**: Supabase auto-deserializes JSONB to Python objects (lists/dicts). SQLite stores tags and sections as TEXT strings. The service must `json.loads()` these columns before returning to consumers, which expect `tags` as `list[str]` and `sections` as `list[dict]`.

2. **Boolean column type mapping**: Supabase returns `has_transcription`/`has_translation` as Python `bool`. SQLite returns `int` (0/1). Consumers use truthiness checks (`if value:`) so this is compatible, but explicit `bool()` casting ensures identical behavior.

3. **Thread safety**: The web app runs in a multi-threaded NiceGUI environment. The existing sidecar services (FjmsService, NliCrossrefService) solve this with `check_same_thread=False`. The PGP service must follow the same pattern.

**Primary recommendation:** Rewrite `shared/document_service.py` to use the class-based singleton pattern from `shared/fjms_service.py` (PgpService class with `get_pgp_service()` factory), keeping ALL existing function signatures identical. The module-level functions become thin wrappers that delegate to the singleton. The web shim (`web/document_service.py`) requires zero changes.

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|-----------------|
| MIGR-02 | `document_service.py` rewritten to read from SQLite instead of Supabase | Complete function-by-function mapping documented below; every Supabase query has a verified SQLite equivalent; schema is identical (same column names); json.loads() handles the JSONB-to-TEXT gap |
| MIGR-03 | Both web and desktop apps use `pgp.db` for all PGP reference data | Both apps import via `from shared.document_service import ...` (desktop) or `from web.document_service import ...` (web shim). Changing the shared module's data source is transparent to both consumers. No import path changes needed. |
| MIGR-05 | Search result enrichment (PGP metadata batch lookup) uses `pgp.db` | `get_sys_ids_with_transcriptions()` currently makes chunked Supabase `.in_()` calls. SQLite equivalent uses `WHERE sys_id IN (?)` with 500-row batching (same pattern as FjmsService.get_domains_for_sys_ids). Benchmarked at <1ms for 200 sys_ids. |
| MIGR-06 | PGP tag-based search uses SQLite `json_each()` instead of Supabase | `get_fragments_by_tag()` currently uses Supabase GIN-indexed `@>` containment query. SQLite equivalent: `SELECT ... FROM documents d, json_each(d.tags) je WHERE je.value = ?`. Benchmarked at 63ms for 552-result "ketubba" query. `get_all_distinct_tags()` benchmarked at 115ms. Both acceptable. |
| MIGR-07 | All existing PGP features produce identical results from SQLite as from Supabase | Return shapes documented; JSON parsing ensures tags/sections match Supabase format; column names identical; sort orders preserved; error handling patterns preserved (None/empty list on error) |
</phase_requirements>

## Standard Stack

### Core
| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| sqlite3 | stdlib | Query pgp.db sidecar | Already used by FjmsService, NliCrossrefService; zero dependency |
| json | stdlib | Parse tags/sections TEXT back to Python objects | `json.loads()` reverses the `json.dumps()` from Phase 35 export |
| pathlib | stdlib | Locate pgp.db via project root auto-detection | Same pattern as FjmsService, NliCrossrefService |
| logging | stdlib | Error logging (replacing print statements) | Consistent with both existing sidecar services |

### Supporting
| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| re | stdlib | `parse_transcription_sections()`, `parse_html_sections()` | Already imported; these pure functions don't change |
| typing | stdlib | Type hints (Optional, List, Dict, Set, Any) | Already imported; no changes |

### Alternatives Considered
| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| Class-based service | Module-level functions (current pattern) | Class enables constructor-based init, is_available() check, thread_safe param, close() -- matches proven FjmsService/NliCrossrefService pattern |
| sqlite3.Row factory | Manual tuple unpacking | Row factory gives dict-like access by column name; more readable, less error-prone |
| Per-call connection | Singleton connection | Per-call would mean opening/closing DB on every function call (~0.5ms overhead each); singleton matches existing sidecar pattern |

**Installation:**
No new dependencies needed. All libraries are stdlib.

## Architecture Patterns

### Recommended Project Structure
```
shared/
├── document_service.py    # REWRITTEN: PgpService class + module-level function wrappers
├── supabase_provider.py   # KEPT (not deleted -- legacy desktop users still on Supabase)
├── fjms_service.py        # UNCHANGED (pattern source)
├── nli_crossref_service.py # UNCHANGED (pattern source)
web/
├── document_service.py    # UNCHANGED (shim re-exports from shared)
pgp_data/
├── pgp.db                 # Phase 35 output (data source)
```

### Pattern 1: Sidecar Service Class (from FjmsService)
**What:** Class wrapping a read-only SQLite connection with auto-detection, graceful degradation, and singleton factory
**When to use:** Every sidecar service in this project

**Template (from `shared/fjms_service.py`):**
```python
# Source: C:/GenizahSearch/shared/fjms_service.py:40-89
class PgpService:
    def __init__(self, db_path: str = None, thread_safe: bool = False):
        self._conn = None
        self._db_path = None

        if db_path is None:
            root = _find_project_root()
            if root:
                db_path = str(root / "pgp_data" / "pgp.db")

        if db_path is None or not Path(db_path).exists():
            logger.warning("PgpService: pgp.db not found")
            return

        self._db_path = db_path
        try:
            uri = f"file:{db_path}?mode=ro"
            self._conn = sqlite3.connect(
                uri, uri=True,
                check_same_thread=not thread_safe,
                timeout=10.0,
            )
            self._conn.row_factory = sqlite3.Row
        except Exception as e:
            logger.error(f"PgpService: Failed to connect: {e}")
            self._conn = None

    def is_available(self) -> bool:
        return self._conn is not None
```

### Pattern 2: Module-Level Function Wrappers (Backward Compatibility)
**What:** Existing module-level functions become thin wrappers around the singleton
**When to use:** Preserving the existing API that both apps import

```python
# At module level, AFTER PgpService class definition:
_default_service: Optional[PgpService] = None

def get_pgp_service(thread_safe: bool = False) -> PgpService:
    global _default_service
    if _default_service is None:
        _default_service = PgpService(thread_safe=thread_safe)
    return _default_service

# Existing function signatures preserved:
def get_document_for_fragment(sys_id: str, page_num: int = None) -> Optional[Dict[str, Any]]:
    svc = get_pgp_service()
    return svc.get_document_for_fragment(sys_id, page_num)
```

### Pattern 3: JSON Column Deserialization
**What:** Parse JSON TEXT columns before returning to consumers
**When to use:** Every function returning documents or sources rows

```python
def _row_to_dict(row: sqlite3.Row, json_columns: tuple = ()) -> dict:
    """Convert sqlite3.Row to dict with JSON deserialization."""
    d = dict(row)
    for col in json_columns:
        if col in d and d[col] is not None:
            try:
                d[col] = json.loads(d[col])
            except (json.JSONDecodeError, TypeError):
                pass  # Leave as-is if parsing fails
    return d
```

### Pattern 4: Batch IN Queries (from FjmsService)
**What:** Chunked `WHERE col IN (?)` queries staying under SQLite's 999 variable limit
**When to use:** `get_sys_ids_with_transcriptions()` and `get_fragments_by_tag()`

```python
# Source: C:/GenizahSearch/shared/fjms_service.py:186-209
batch_size = 500
for i in range(0, len(sys_ids), batch_size):
    batch = sys_ids[i:i + batch_size]
    placeholders = ','.join('?' * len(batch))
    cursor = self._conn.execute(
        f"SELECT DISTINCT sys_id FROM document_fragments WHERE sys_id IN ({placeholders})",
        batch,
    )
    result_set.update(row["sys_id"] for row in cursor)
```

### Anti-Patterns to Avoid
- **Removing supabase_provider.py:** Prior decision: "Supabase PGP tables kept (legacy desktop users)." Don't delete the provider or Supabase tables. Only change document_service.py to stop using it.
- **Changing function signatures:** All 14 functions must keep identical signatures (params and return types). Consumers do NOT change.
- **Changing the web shim:** `web/document_service.py` re-exports from `shared.document_service`. Since function names don't change, the shim is untouched.
- **Forgetting JSON deserialization:** The #1 breakage risk. `tags` and `sections` MUST be parsed from TEXT to Python objects before returning. Consumers call `.get('tags', [])` and iterate over the result as a list.
- **Forgetting boolean type mapping:** `has_transcription` / `has_translation` return as `int` from SQLite. While truthiness is identical, some consumers might do `== True` checks. Safest to cast `bool()`.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Project root detection | Custom path logic | `_find_project_root()` (copy from FjmsService) | Proven pattern, handles 5 levels up, looks for libraries.csv |
| Thread safety | Custom locking | `check_same_thread=False` + read-only URI | SQLite read-only connections are thread-safe with this flag |
| Connection management | Open/close per call | Singleton pattern with `get_pgp_service()` | Matches both existing sidecar services; connection pooling is unnecessary for read-only |
| JSON parsing | Custom TEXT-to-dict | `json.loads()` on TEXT columns | Deterministic round-trip proven by Phase 35 validation |

**Key insight:** The entire rewrite is a data-source swap. Zero business logic changes. The only "new" code is the SQLite queries (which are simpler than Supabase chains) and JSON deserialization (which is one `json.loads()` call per column).

## Common Pitfalls

### Pitfall 1: tags Returns as String Instead of List
**What goes wrong:** Consumers call `pgp_doc.get('tags', [])` and get a JSON string `'["communal","marriage"]'` instead of a Python list `["communal", "marriage"]`. Iteration over the string yields individual characters.
**Why it happens:** Supabase auto-deserializes JSONB to Python objects. SQLite stores tags as TEXT.
**How to avoid:** Every function returning a document dict MUST parse `tags` via `json.loads()` before returning. Use the `_row_to_dict(row, json_columns=('tags',))` helper.
**Warning signs:** Tags display showing individual characters or raw JSON strings in UI.

### Pitfall 2: sections Returns as String Instead of List
**What goes wrong:** `source.get('sections')` returns a JSON string instead of a list of dicts. `get_section_for_page()` iterates over it expecting dicts with `canvas_num` keys.
**Why it happens:** Same JSONB-to-TEXT gap as tags.
**How to avoid:** Every function returning source rows MUST parse `sections` via `json.loads()`.
**Warning signs:** Section-based page navigation fails; version selector shows wrong content.

### Pitfall 3: Thread-Safety for Web App
**What goes wrong:** NiceGUI web app calls PGP service functions from multiple threads, causing `sqlite3.ProgrammingError: SQLite objects created in a thread can only be used in that same thread`.
**Why it happens:** Python's sqlite3 module enforces single-thread access by default.
**How to avoid:** Web app must call `get_pgp_service(thread_safe=True)`. Existing sidecar services already do this. Read-only mode makes this safe.
**Warning signs:** Intermittent ProgrammingError in web app logs when multiple users browse simultaneously.

### Pitfall 4: First Call Determines Thread Safety
**What goes wrong:** Desktop app calls `get_pgp_service()` (thread_safe=False) first, then web app tries `get_pgp_service(thread_safe=True)` -- but the singleton was already created without thread safety.
**Why it happens:** Singleton factory caches first instantiation.
**How to avoid:** This is NOT a real problem in practice because desktop and web never run in the same process. Each app process creates its own singleton. Just be aware of it.
**Warning signs:** N/A -- apps run in separate processes.

### Pitfall 5: get_all_sources_for_fragment N+1 Query Pattern
**What goes wrong:** Current Supabase version makes 1 query for fragments + N queries for sources (one per linked document). Naive SQLite port preserves this N+1 pattern.
**Why it happens:** Supabase doesn't support JOINs easily; the current code uses sequential API calls.
**How to avoid:** With local SQLite, use a single JOIN query or a batched IN query. Get all linked pgpids from document_fragments, then batch-query document_sources with `WHERE pgpid IN (?)`. Two queries total instead of N+1.
**Warning signs:** Slow loading of version selector for multi-document fragments.

### Pitfall 6: Empty Tags List vs NULL
**What goes wrong:** Some documents have `tags = '[]'` (empty JSON array) vs `tags = NULL`. The consumer code uses `pgp_doc.get('tags', [])` which returns `[]` for both NULL and empty list. But `json.loads('[]')` returns `[]` while NULL stays as None.
**Why it happens:** Different data states in the original Supabase data.
**How to avoid:** In `_row_to_dict`, only call `json.loads()` when value is not None. When value is None, leave it as None -- the consumer's `get('tags', [])` default handles this correctly.
**Warning signs:** None -- `get('tags', [])` handles both cases.

### Pitfall 7: Supabase-Dependent Tests Break
**What goes wrong:** `tests/test_document_service.py` patches `shared.document_service.get_client` which no longer exists after the rewrite.
**Why it happens:** Tests mock the Supabase client.
**How to avoid:** Rewrite tests to use an in-memory SQLite database instead of mocking Supabase. Create a test fixture that inserts sample data into `:memory:` db and passes it to PgpService.
**Warning signs:** All test_document_service.py tests fail with `AttributeError: module 'shared.document_service' has no attribute 'get_client'`.

## Code Examples

### Complete Function Mapping: Supabase -> SQLite

Every current function mapped to its SQLite equivalent:

#### 1. get_document_for_fragment(sys_id, page_num=None)
```python
# CURRENT (Supabase): 2 API calls (fragment lookup + document fetch)
# NEW (SQLite): 1-2 simple queries

def get_document_for_fragment(self, sys_id, page_num=None):
    if not sys_id or not self._conn:
        return None
    try:
        # Step 1: Find document_id(s) for this sys_id
        cursor = self._conn.execute(
            "SELECT document_id, page_info FROM document_fragments WHERE sys_id = ?",
            (sys_id,)
        )
        frags = cursor.fetchall()
        if not frags:
            return None

        # Step 2: Select by page_num if specified
        pgpid = None
        if page_num and len(frags) > 1:
            target_page = 'recto' if page_num == 1 else 'verso'
            for f in frags:
                if f['page_info'] == target_page:
                    pgpid = f['document_id']
                    break
        if not pgpid:
            pgpid = frags[0]['document_id']

        # Step 3: Get full document
        cursor = self._conn.execute(
            "SELECT * FROM documents WHERE pgpid = ?", (pgpid,)
        )
        row = cursor.fetchone()
        return _row_to_dict(row, json_columns=('tags',)) if row else None
    except Exception as e:
        logger.error(f"get_document_for_fragment error: {e}")
        return None
```

#### 2. get_fragments_for_document(pgpid)
```python
# CURRENT: client.table('document_fragments').select('*').eq('document_id', pgpid).order('sequence_order')
# NEW:
cursor = self._conn.execute(
    "SELECT * FROM document_fragments WHERE document_id = ? ORDER BY sequence_order",
    (pgpid,)
)
return [dict(row) for row in cursor]
```

#### 3. get_transcription_for_document(pgpid)
```python
# CURRENT: client.table('documents').select('transcription').eq('pgpid', pgpid).single()
# NEW:
cursor = self._conn.execute(
    "SELECT transcription FROM documents WHERE pgpid = ?", (pgpid,)
)
row = cursor.fetchone()
return row['transcription'] if row and row['transcription'] else None
```

#### 4. get_document_metadata(pgpid)
```python
# CURRENT: client.table('documents').select('document_type, tags, ...').eq('pgpid', pgpid).single()
# NEW:
cursor = self._conn.execute(
    "SELECT document_type, tags, doc_date_original, doc_date_standard, "
    "inferred_date_display, description, pgp_url, shelfmark_combined "
    "FROM documents WHERE pgpid = ?", (pgpid,)
)
row = cursor.fetchone()
return _row_to_dict(row, json_columns=('tags',)) if row else None
```

#### 5. get_sources_for_document(pgpid)
```python
# CURRENT: client.table('document_sources').select('*').eq('pgpid', pgpid).order('doc_relation').order('sequence_order')
# NEW:
cursor = self._conn.execute(
    "SELECT * FROM document_sources WHERE pgpid = ? ORDER BY doc_relation, sequence_order",
    (pgpid,)
)
return [_row_to_dict(row, json_columns=('sections',)) for row in cursor]
```

#### 6. get_all_sources_for_fragment(sys_id) -- OPTIMIZED
```python
# CURRENT: N+1 Supabase calls (1 fragment query + N source queries)
# NEW: 2 SQLite queries (fragments + batched sources)

def get_all_sources_for_fragment(self, sys_id):
    if not sys_id or not self._conn:
        return []
    try:
        # Get fragment links
        cursor = self._conn.execute(
            "SELECT document_id, page_info FROM document_fragments WHERE sys_id = ?",
            (sys_id,)
        )
        frags = cursor.fetchall()
        if not frags:
            return []

        # Build page_info map
        page_map = {f['document_id']: f['page_info'] for f in frags}
        pgpids = list(page_map.keys())

        # Batch get sources for all linked documents
        placeholders = ','.join('?' * len(pgpids))
        cursor = self._conn.execute(
            f"SELECT * FROM document_sources WHERE pgpid IN ({placeholders}) "
            f"ORDER BY doc_relation, sequence_order",
            pgpids
        )

        all_sources = []
        for row in cursor:
            source = _row_to_dict(row, json_columns=('sections',))
            source['page_info'] = page_map.get(source['pgpid'])
            all_sources.append(source)

        # Sort: Editions first
        all_sources.sort(key=lambda x: (
            0 if 'Edition' in (x.get('doc_relation') or '') else 1,
            x.get('sequence_order', 0)
        ))
        return all_sources
    except Exception as e:
        logger.error(f"get_all_sources_for_fragment error: {e}")
        return []
```

#### 7. get_sys_ids_with_transcriptions(sys_ids) -- Batch Lookup
```python
# CURRENT: Chunked Supabase .in_() calls
# NEW: Chunked SQLite IN queries (same pattern as FjmsService)

def get_sys_ids_with_transcriptions(self, sys_ids):
    if not sys_ids or not self._conn:
        return set()
    try:
        result_set = set()
        batch_size = 500
        for i in range(0, len(sys_ids), batch_size):
            batch = sys_ids[i:i + batch_size]
            placeholders = ','.join('?' * len(batch))
            cursor = self._conn.execute(
                f"SELECT DISTINCT sys_id FROM document_fragments WHERE sys_id IN ({placeholders})",
                batch
            )
            result_set.update(row['sys_id'] for row in cursor)
        return result_set
    except Exception as e:
        logger.error(f"get_sys_ids_with_transcriptions error: {e}")
        return set()
```

#### 8. get_fragments_by_tag(tag) -- json_each()
```python
# CURRENT: Supabase GIN-indexed JSONB @> query + batch fragment lookup
# NEW: SQLite json_each() + JOIN

def get_fragments_by_tag(self, tag):
    if not tag or not self._conn:
        return []
    try:
        # Step 1: Find documents with this tag
        cursor = self._conn.execute(
            "SELECT pgpid, shelfmark_combined, document_type, description, transcription "
            "FROM documents d, json_each(d.tags) je "
            "WHERE je.value = ?",
            (tag,)
        )
        docs = cursor.fetchall()
        if not docs:
            return []

        # Step 2: Batch get fragments
        doc_ids = [d['pgpid'] for d in docs]
        placeholders = ','.join('?' * len(doc_ids))
        frag_cursor = self._conn.execute(
            f"SELECT sys_id, shelfmark, document_id FROM document_fragments "
            f"WHERE document_id IN ({placeholders})",
            doc_ids
        )
        frags = frag_cursor.fetchall()
        if not frags:
            return []

        # Step 3: Join
        doc_map = {d['pgpid']: dict(d) for d in docs}
        results = []
        for frag in frags:
            doc = doc_map.get(frag['document_id'], {})
            results.append({
                'sys_id': frag['sys_id'],
                'shelfmark': frag['shelfmark'],
                'document_type': doc.get('document_type', ''),
                'description': doc.get('description', ''),
                'pgpid': frag['document_id'],
                'transcription': doc.get('transcription', ''),
            })
        return results
    except Exception as e:
        logger.error(f"get_fragments_by_tag error: {e}")
        return []
```

#### 9. get_all_distinct_tags()
```python
# CURRENT: Supabase SELECT tags WHERE tags IS NOT NULL, then Python set union
# NEW: SQLite json_each() with DISTINCT

def get_all_distinct_tags(self):
    if not self._conn:
        return []
    try:
        cursor = self._conn.execute(
            "SELECT DISTINCT je.value as tag "
            "FROM documents d, json_each(d.tags) je "
            "WHERE je.value != '' "
            "ORDER BY tag"
        )
        return [row['tag'] for row in cursor]
    except Exception as e:
        logger.error(f"get_all_distinct_tags error: {e}")
        return []
```

#### 10-12. get_editions_for_document, get_translations_for_document
```python
# CURRENT: Supabase .like('doc_relation', '%Edition%')
# NEW: SQLite LIKE (identical syntax)

cursor = self._conn.execute(
    "SELECT * FROM document_sources WHERE pgpid = ? AND doc_relation LIKE '%Edition%' "
    "ORDER BY sequence_order",
    (pgpid,)
)
return [_row_to_dict(row, json_columns=('sections',)) for row in cursor]
```

### Pure Functions (NO CHANGES NEEDED)

These functions have ZERO Supabase dependency and remain unchanged:
- `parse_transcription_sections(transcription)` -- pure regex parsing
- `get_section_for_page(transcription, page_num, sections)` -- pure logic
- `parse_html_sections(html_content)` -- pure HTML parsing
- `PGPHTMLParser` class -- pure parsing

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| Supabase REST API for every read | Local SQLite sidecar | Phase 36 (this phase) | Zero network dependency for PGP data; sub-millisecond queries vs 50-200ms API calls |
| Module-level functions with inline Supabase client | Class-based service with singleton factory | Phase 36 (this phase) | Consistent with FjmsService/NliCrossrefService pattern; enables is_available() graceful degradation |
| GIN-indexed JSONB `@>` for tag search | SQLite `json_each()` virtual table | Phase 36 (this phase) | Benchmarked at 63ms for 552 results -- acceptable performance |
| N+1 API calls in get_all_sources_for_fragment | 2 SQLite queries with JOIN | Phase 36 (this phase) | Eliminates sequential network round-trips; should be 10-50x faster |
| print() for error logging | logging module | Phase 36 (this phase) | Consistent with FjmsService/NliCrossrefService pattern |

**Deprecated/outdated after this phase:**
- `shared/supabase_provider.py` -- kept for legacy desktop users but no longer imported by document_service.py
- `from shared.supabase_provider import get_client` in document_service.py -- removed
- Supabase PGP tables -- kept in Supabase (prior decision) but no longer actively queried

## Complete Function Inventory

All 14 public functions in `shared/document_service.py` and their disposition:

| # | Function | Type | Changes |
|---|----------|------|---------|
| 1 | `get_document_for_fragment(sys_id, page_num)` | Supabase query | **REWRITE** to SQLite |
| 2 | `get_fragments_for_document(pgpid)` | Supabase query | **REWRITE** to SQLite |
| 3 | `get_transcription_for_document(pgpid)` | Supabase query | **REWRITE** to SQLite |
| 4 | `get_document_metadata(pgpid)` | Supabase query | **REWRITE** to SQLite |
| 5 | `get_sources_for_document(pgpid)` | Supabase query | **REWRITE** to SQLite |
| 6 | `get_all_sources_for_fragment(sys_id)` | Supabase multi-call | **REWRITE** + optimize (2 queries vs N+1) |
| 7 | `get_editions_for_document(pgpid)` | Supabase query | **REWRITE** to SQLite |
| 8 | `get_translations_for_document(pgpid)` | Supabase query | **REWRITE** to SQLite |
| 9 | `get_sys_ids_with_transcriptions(sys_ids)` | Supabase batch | **REWRITE** to SQLite batch |
| 10 | `get_fragments_by_tag(tag)` | Supabase GIN query | **REWRITE** to json_each() |
| 11 | `get_all_distinct_tags()` | Supabase scan | **REWRITE** to json_each() |
| 12 | `parse_transcription_sections(transcription)` | Pure function | **NO CHANGE** |
| 13 | `get_section_for_page(transcription, page_num, sections)` | Pure function | **NO CHANGE** |
| 14 | `parse_html_sections(html_content)` | Pure function | **NO CHANGE** |
| -- | `PGPHTMLParser` class | Pure parsing | **NO CHANGE** |

## Consumer Call Sites

All import locations that will be affected (must continue working without changes):

### Desktop App (`genizah_app.py`)
- Line 3037: `get_document_for_fragment, get_fragments_for_document` (PGP joins)
- Line 7379: `get_fragments_for_document` (browse joins)
- Line 10146: `get_document_for_fragment` (reading desk)
- Line 10162: `get_fragments_for_document` (reading desk PGP joins)

### Desktop Threads (`gui_threads.py`)
- Line 477: `get_all_sources_for_fragment, get_document_for_fragment, get_section_for_page` (PGP worker)
- Line 531: `get_sys_ids_with_transcriptions` (badge worker)
- Line 548: `get_all_distinct_tags` (tags worker)
- Line 566: `get_fragments_by_tag` (tag search worker)
- Line 589: `get_all_sources_for_fragment, get_document_for_fragment` (reading desk worker)

### Desktop Corrections (`corrections_ui.py`)
- Line 3694: `get_document_for_fragment, get_fragments_for_document`

### Web Browse (`web/pages/browse.py`)
- Line 25 (top import): `get_document_for_fragment, get_section_for_page, get_sources_for_document, get_all_sources_for_fragment`
- Lines 1039, 1098, 1130, 2603: lazy imports for reading desk

### Web Search (`web/pages/search.py`)
- Line 19: `get_sys_ids_with_transcriptions, get_all_sources_for_fragment, get_document_for_fragment, get_section_for_page, get_fragments_by_tag, get_all_distinct_tags`

### Web Shim (`web/document_service.py`)
- Re-exports all 14 functions -- **NO CHANGES NEEDED**

## Test Strategy

### Existing Tests to Rewrite
`tests/test_document_service.py` (~300 lines, 14 tests):
- All tests patch `shared.document_service.get_client` which won't exist after rewrite
- Rewrite to use in-memory SQLite fixture:
  - Create `:memory:` db with schema matching pgp.db
  - Insert sample rows for each test
  - Pass db_path to PgpService constructor
  - Test identical assertions (same return values, same error handling)

### Existing Tests That Remain Green
`tests/test_shared_service.py` (~530 lines):
- `test_all_functions_importable`: Must still pass (same function names)
- `test_desktop_imports_shared`: Must still pass (import path unchanged)
- All `parse_transcription_sections` tests: Pure functions, unchanged
- All `parse_html_sections` tests: Pure functions, unchanged
- All `get_section_for_page` tests: Pure function, unchanged

### New Tests to Add
- `test_pgp_service_unavailable()`: When pgp.db missing, all functions return None/[]
- `test_tags_deserialized_as_list()`: Verify tags returns as list, not string
- `test_sections_deserialized_as_list()`: Verify sections returns as list, not string
- `test_json_each_tag_search()`: Tag search returns correct results
- `test_batch_transcription_lookup()`: Batch sys_id lookup works correctly

## Performance Benchmarks

Measured against `pgp_data/pgp.db` (146.6 MB, 104K rows):

| Operation | SQLite Performance | Supabase Estimate | Speedup |
|-----------|-------------------|-------------------|---------|
| get_document_for_fragment (1 sys_id) | <1ms | 50-150ms | 50-150x |
| get_fragments_for_document (1 pgpid) | <1ms | 50-100ms | 50-100x |
| get_sys_ids_with_transcriptions (200 sys_ids) | <1ms | 200-400ms | 200-400x |
| get_fragments_by_tag ("ketubba", 552 results) | 63ms | 100-300ms | 2-5x |
| get_all_distinct_tags (2,695 tags) | 115ms | 500-1000ms | 4-9x |
| get_all_sources_for_fragment (N+1 -> 2 queries) | <2ms | 200-500ms | 100-250x |

## Open Questions

1. **build_app.bat Distribution**
   - What we know: Phase 38 handles bundling pgp.db for desktop distribution. Currently `build_app.bat` bundles `fist_data/fjms_enrichment.db` and `nli_data/nli_crossref.db` but NOT `pgp_data/pgp.db`.
   - What's unclear: Whether Phase 36 should add the `--add-data` line proactively or defer to Phase 38.
   - Recommendation: Defer to Phase 38. Phase 36 is the service layer rewrite; distribution is Phase 38's concern.

2. **supabase_provider.py Cleanup**
   - What we know: After this phase, `supabase_provider.py` will have zero in-app consumers (only the export script uses Supabase credentials).
   - What's unclear: Whether to remove the import and module, or keep it.
   - Recommendation: Keep it. Prior decision says "Supabase PGP tables kept (legacy desktop users)." The provider may still be needed for user-data operations (corrections, lists, auth). Also, it's harmless dead code.

3. **Web App Thread Safety Initialization**
   - What we know: Web browse.py imports at module level (line 25). Desktop imports lazily inside methods. FjmsService and NliCrossrefService are initialized lazily with `thread_safe=True` in web context.
   - What's unclear: Whether the module-level import in browse.py will trigger early initialization before thread_safe can be set.
   - Recommendation: The module-level functions should NOT eagerly create the singleton. The `get_pgp_service()` factory creates on first call. In web context, the first call will be from `get_all_sources_for_fragment()` inside a page handler where we can ensure `thread_safe=True`. Alternatively, make the wrapper functions auto-detect web context.

## Sources

### Primary (HIGH confidence)
- `C:/GenizahSearch/shared/document_service.py` - Complete source code of module being rewritten (742 lines, 14 functions)
- `C:/GenizahSearch/shared/fjms_service.py` - Pattern source for class-based sidecar service (961 lines)
- `C:/GenizahSearch/shared/nli_crossref_service.py` - Pattern source for sidecar service (844 lines)
- `C:/GenizahSearch/pgp_data/pgp.db` - Target data source (146.6 MB, 5 tables, verified Phase 35)
- `C:/GenizahSearch/web/document_service.py` - Web shim (25 lines, re-exports all functions)
- `C:/GenizahSearch/gui_threads.py` - Desktop consumer (PGP workers)
- `C:/GenizahSearch/web/pages/browse.py` - Web consumer (3,200+ lines)
- `C:/GenizahSearch/web/pages/search.py` - Web consumer (3,200+ lines)
- `C:/GenizahSearch/tests/test_document_service.py` - Tests to rewrite (300 lines)
- `C:/GenizahSearch/tests/test_shared_service.py` - Tests that must remain green (530 lines)

### Secondary (MEDIUM confidence)
- `C:/GenizahSearch/scripts/export_pgp_sidecar.py` - Phase 35 export script defining schema
- `C:/GenizahSearch/.planning/phases/35-pgp-sidecar-export/35-VERIFICATION.md` - Schema validation results
- `C:/GenizahSearch/build_app.bat` - Desktop bundling (Phase 38 concern)

### Benchmarks (HIGH confidence)
- All benchmarks measured directly against `pgp_data/pgp.db` using Python 3.10+ sqlite3 module
- json_each() confirmed functional in SQLite bundled with Python
- Thread safety via `check_same_thread=False` + read-only URI confirmed in existing sidecar services

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH - All stdlib, zero new dependencies, proven patterns from 2 existing sidecars
- Architecture: HIGH - Every function has a documented SQLite equivalent with verified query syntax
- Pitfalls: HIGH - JSON deserialization gap is the #1 risk, well-documented with prevention strategy
- Tests: HIGH - Clear test rewrite strategy with in-memory SQLite fixtures

**Research date:** 2026-02-17
**Valid until:** 2026-03-17 (stable domain, all patterns proven in codebase)
