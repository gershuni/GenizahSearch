# Domain Pitfalls: PGP Sidecar Migration + FJMS Full Texts

**Domain:** PostgreSQL-to-SQLite migration for reference data, FJMS transcription integration
**Researched:** 2026-02-16
**Confidence:** HIGH (based on direct codebase analysis of all 4 Supabase tables, 2 existing SQLite sidecars, and 15+ consumer call sites)

This document supersedes the v5.6.0 pitfalls (shared service extraction). Those pitfalls were addressed -- `shared/document_service.py` was extracted with a `web/document_service.py` shim (Phase 8), and `shared/supabase_provider.py` handles the client singleton. This document covers pitfalls specific to the NEXT milestone: migrating PGP data from Supabase to a SQLite sidecar, and adding 65K FJMS transcriptions as a scholarly source.

---

## Critical Pitfalls

Mistakes that cause data loss, broken production deployments, or require rewrites.

---

### Pitfall 1: JSONB-to-SQLite Data Loss on `tags` and `sections` Columns

**What goes wrong:** The `documents.tags` column stores JSONB arrays (e.g., `["communal", "marriage", "trade"]`) and `document_sources.sections` stores JSONB arrays of objects (e.g., `[{"canvas_url": "...", "canvas_num": 1, "label": null, "text": "..."}]`). Migrating these to SQLite requires storing them as TEXT (JSON strings). If the export/import pipeline doesn't serialize properly, data is silently corrupted or lost.

**Why it happens:**
1. PostgreSQL JSONB is a binary format with O(1) lookup; SQLite stores JSON as TEXT with O(N) parsing. The formats are NOT binary-compatible despite SQLite 3.45+ calling its binary format "JSONB" too.
2. Supabase PostgREST returns JSONB as Python dicts/lists, but inserting into SQLite requires explicit `json.dumps()`. Forgetting this produces `"{'key': 'value'}"` (Python repr, not JSON) which breaks `json_extract()`.
3. NULL vs empty: PostgreSQL distinguishes `NULL` JSONB from empty array `[]`. If the migration normalizes both to `NULL`, queries like `get_all_distinct_tags()` break because they rely on `NOT NULL` filtering.

**Consequences:**
- `get_fragments_by_tag()` stops working entirely -- it uses `filter('tags', 'cs', json.dumps([tag]))` which translates to PostgreSQL's `@>` operator. The SQLite equivalent (`json_each` + `WHERE value = ?`) requires completely different SQL.
- `get_section_for_page()` gets `None` for sections because `json.loads()` fails on malformed strings.
- Tag search in both apps produces zero results with no error (silent failure).

**Prevention:**
1. Serialize all JSONB columns with `json.dumps()` during export, verify round-trip with `json.loads()` on every row.
2. Write a validation pass after SQLite import: `SELECT COUNT(*) FROM documents WHERE json_valid(tags) = 0` to catch any malformed JSON.
3. For `tags`, store as JSON TEXT and query with `json_each()`:
   ```sql
   -- PostgreSQL: filter('tags', 'cs', '["communal"]')
   -- SQLite equivalent:
   SELECT d.* FROM documents d, json_each(d.tags) AS t WHERE t.value = ?
   ```
4. For `sections`, store as JSON TEXT and parse in Python after retrieval (as the current code already does -- `get_section_for_page` iterates the list in Python, not SQL).
5. Preserve the distinction between `NULL` and `[]` in tags -- the `get_all_distinct_tags()` function filters with `.not_.is_('tags', 'null')`.

**Detection:** Automated test: export all documents, import into SQLite, verify `json_valid()` on every JSON column, assert row counts match. Run `get_all_distinct_tags()` against both backends and compare output.

**Which phase should address it:** The sidecar build script phase (likely Phase 1 or 2). The build script must include a JSON validation pass as part of its verification report, mirroring the pattern in `import_nli_crossref.py`.

---

### Pitfall 2: PostgreSQL GIN Index Features Have No SQLite Equivalent

**What goes wrong:** Three query patterns in `document_service.py` rely on PostgreSQL-specific features that have no direct SQLite equivalent:

| Query | PostgreSQL Feature | Current Code |
|-------|-------------------|--------------|
| Tag search | GIN index with `@>` operator | `filter('tags', 'cs', json.dumps([tag]))` |
| Edition filter | `LIKE` on text column | `.like('doc_relation', '%Edition%')` |
| Null JSON check | `IS NOT NULL` on JSONB | `.not_.is_('tags', 'null')` |

**Why it happens:** The migration naturally translates Supabase PostgREST calls to SQLite queries, but developers assume 1:1 mapping. The GIN-indexed `@>` containment operator has no SQLite equivalent -- `json_each()` is a table-valued function that requires a subquery or join, and it cannot use an index.

**Consequences:**
- Tag-based search (`get_fragments_by_tag`) either crashes with SQL error or returns wrong results.
- If the developer uses `LIKE '%tag%'` on the JSON TEXT column as a shortcut, it matches partial tag names (searching "war" matches "software").
- Performance: scanning all 35K documents with `json_each()` on every tag search may be acceptable for reference data, but if the 65K FJMS records also have tags, the scan becomes slow.

**Prevention:**
1. For tag search: either use `json_each()` join (correct but slower) or pre-compute a separate `document_tags` junction table:
   ```sql
   CREATE TABLE document_tags (pgpid INTEGER, tag TEXT);
   CREATE INDEX idx_doc_tags ON document_tags(tag);
   -- Populated during sidecar build from documents.tags JSON
   ```
   This gives indexed tag lookup matching the GIN index performance.
2. For `LIKE` on `doc_relation`: this works identically in SQLite, no change needed.
3. For null JSON check: SQLite's `IS NOT NULL` works on TEXT columns, but also need `AND tags != 'null'` to handle the string literal "null" that might appear.
4. Benchmark the `json_each()` approach on 35K rows before deciding if a junction table is needed.

**Detection:** Run the full test suite against the SQLite backend. The `get_fragments_by_tag('Legal')` call must return the same set of sys_ids as the Supabase version.

**Which phase should address it:** Schema design phase. The decision between `json_each()` and a junction table must be made before building the sidecar.

---

### Pitfall 3: Removing Supabase Read Paths While User-Data Writes Still Depend on It

**What goes wrong:** The migration moves READ-ONLY reference data (documents, document_fragments, document_sources, document_footnotes) from Supabase to SQLite. But Supabase still hosts user-generated data (corrections, comments, discoveries, fragment_joins, lists). If the developer removes the Supabase dependency entirely or accidentally breaks the Supabase client singleton during refactoring, all user features break.

**Why it happens:** There are currently TWO Supabase client singletons:
1. `shared/supabase_provider.py` -- used by `shared/document_service.py` (PGP data reads)
2. `web/supabase_client.py` -- used for auth, lists, corrections, comments, discoveries

When `shared/document_service.py` is refactored to use SQLite instead of Supabase, the developer may:
- Remove `shared/supabase_provider.py` thinking it's no longer needed (but `supabase_corrections_client.py` for desktop also imports from Supabase)
- Change `shared/document_service.py` to stop importing from `shared/supabase_provider.py`, then remove the provider, not realizing other desktop code uses it
- Test only the web app and miss that the desktop `supabase_corrections_client.py` (1,800+ lines) still needs the Supabase client for corrections, discoveries, and joins

**Consequences:**
- Desktop app: corrections, comments, discoveries, and join proposals all break with ImportError or connection failure.
- Web app: auth still works (uses `web/supabase_client.py`), but if someone refactors to share the provider, it could break auth flow.
- Partial deployment: web app works fine, desktop app silently fails on all user features.

**Prevention:**
1. Map every Supabase dependency BEFORE removing any:
   - `shared/document_service.py` -> `shared/supabase_provider.py` (being migrated away)
   - `supabase_corrections_client.py` -> direct Supabase client (KEEP)
   - `web/supabase_client.py` -> direct Supabase client (KEEP)
   - `lists_sync.py` -> Supabase client (KEEP)
2. Do NOT remove `shared/supabase_provider.py` -- other code may depend on it. Instead, remove only the `document_service.py` import of it.
3. Mark the refactoring as "remove Supabase from document reads" not "remove Supabase dependency."
4. Run both apps end-to-end after migration: verify search, PGP display, AND corrections/lists.

**Detection:** `grep -r "supabase_provider\|get_client" shared/ web/ supabase_corrections_client.py` before and after migration. The count should decrease only for document_service.py references.

**Which phase should address it:** Must be documented in the cutover phase. A dependency map should be produced before any Supabase read paths are removed.

---

### Pitfall 4: Forgetting `check_same_thread=False` or Blocking the Async Event Loop

**What goes wrong:** The NiceGUI web app runs async with uvicorn. SQLite calls are synchronous and will block the event loop if called directly. The existing sidecar services (`fjms_service.py`, `nli_crossref_service.py`) use `check_same_thread=False` for the web app's thread pool, but the new PGP sidecar service might not follow this pattern.

**Why it happens:**
1. Developer copies the `document_service.py` function signatures but implements them with direct SQLite calls inside `async` handlers, blocking the event loop.
2. Developer forgets the `thread_safe: bool = False` constructor parameter that both existing services implement.
3. The web app calls PGP functions via `await run.io_bound(...)` (see `web/pages/search.py:2005` and 15+ other sites). If the new service is NOT thread-safe, concurrent requests crash with `ProgrammingError: SQLite objects created in a thread can only be used in that same thread`.

**Consequences:**
- Without `check_same_thread=False`: crash under concurrent load, intermittent `ProgrammingError` that only appears in production with multiple users.
- Without `run.io_bound()`: UI freezes during SQLite queries, WebSocket timeouts, poor user experience.
- Both are hard to catch in single-user development testing.

**Prevention:**
1. Follow the exact pattern from `fjms_service.py:43-79`:
   ```python
   def __init__(self, db_path=None, thread_safe=False):
       uri = f"file:{db_path}?mode=ro"
       self._conn = sqlite3.connect(
           uri, uri=True,
           check_same_thread=not thread_safe,
           timeout=10.0,
       )
       self._conn.row_factory = sqlite3.Row
   ```
2. Web app initialization MUST pass `thread_safe=True` (matches existing pattern in both sidecar services).
3. All web page calls to the service MUST use `await run.io_bound(service.method, args)` to offload to the thread pool.
4. Desktop app should leave `thread_safe=False` (single-threaded) and use QThread for background operations (matches existing `gui_threads.py` pattern).

**Detection:** Load test with 3+ concurrent browser tabs hitting search results with PGP data. If it crashes, thread safety is wrong.

**Which phase should address it:** Service implementation phase. The service constructor MUST be reviewed against the existing pattern before any web integration.

---

## Moderate Pitfalls

Issues that cause incorrect behavior, poor performance, or significant rework.

---

### Pitfall 5: Deduplication Between PGP Editions and FJMS Transcriptions

**What goes wrong:** Both PGP (via `document_sources`) and FJMS contain scholarly transcriptions of the same Genizah manuscripts. The same text -- e.g., a Goitein transcription of T-S 13J6.13 -- may appear in both sources with slight variations (different formatting, different section boundaries, different attribution strings). Without deduplication, the UI shows duplicate entries in the transcription selector.

**Why it happens:**
1. PGP organizes by `pgpid` (document ID), FJMS organizes by `AlmaId` (sys_id). The same manuscript has different identifiers in each system.
2. PGP's `source_scholar` field (e.g., "S.D. Goitein") and FJMS's scholar attribution may not match exactly (name variants, initials vs full names).
3. PGP's `doc_relation` uses "Digital Edition" while FJMS may use different terminology.
4. The text content itself differs: PGP stores cleaned transcription text, FJMS may store the same text with different Unicode normalization, different line breaks, or different section markers.

**Consequences:**
- User sees the same transcription twice with different labels, causing confusion.
- If both are treated as independent sources, the "source count" badges are inflated.
- If dedup is too aggressive, distinct scholarly editions are incorrectly merged (scholar A's reading vs scholar B's reading of the same manuscript).

**Prevention:**
1. Dedup by (sys_id, normalized_scholar_name, relation_type) -- not by text content. Two different scholars' transcriptions of the same manuscript are legitimately different.
2. Build a scholar name normalization map: "S.D. Goitein" = "Goitein, S. D." = "Goitein" for matching purposes.
3. When overlap is detected, prefer PGP's version (it's more recently curated and has structured section data) but surface FJMS as an alternative if it has additional content (e.g., FJMS might have a fuller transcription or different sections).
4. Add a `source_system` field to the unified data model: "pgp" or "fjms" so the UI can indicate provenance.
5. Test with known-overlapping manuscripts: find 10 sys_ids that appear in both PGP `document_fragments` and FJMS, manually verify the dedup logic produces correct results.

**Detection:** After sidecar build, query: `SELECT sys_id, COUNT(*) FROM (all sources) GROUP BY sys_id, scholar_normalized HAVING COUNT(*) > 1` -- any results indicate unresolved duplicates.

**Which phase should address it:** Must be addressed in the sidecar build phase when combining data from both sources. The dedup logic must be part of the build script, not the runtime service.

---

### Pitfall 6: `get_all_distinct_tags()` Full Table Scan on 35K Rows

**What goes wrong:** The current `get_all_distinct_tags()` fetches ALL documents' `tags` column from Supabase and aggregates in Python. In Supabase, this is acceptable because PostgREST streams results. In SQLite, this requires reading 35K rows and parsing JSON for each. If additionally 65K FJMS documents have tags, this becomes 100K rows.

**Why it happens:** The current implementation (line 561-575 of `document_service.py`) does:
```python
response = client.table('documents').select('tags').not_.is_('tags', 'null').execute()
all_tags = set()
for row in (response.data or []):
    tags = row.get('tags', [])
    for tag in tags:
        all_tags.add(tag)
return sorted(all_tags)
```

This pattern works with PostgREST (returns all rows as JSON array in HTTP response), but in SQLite it's better expressed as a single SQL query using `json_each()`.

**Prevention:**
1. Replace with a pure SQL query:
   ```sql
   SELECT DISTINCT t.value FROM documents, json_each(documents.tags) AS t
   WHERE documents.tags IS NOT NULL AND documents.tags != '[]'
   ORDER BY t.value
   ```
2. Or, if using the junction table from Pitfall 2: `SELECT DISTINCT tag FROM document_tags ORDER BY tag`
3. Cache the result (tag list changes only when sidecar is rebuilt, not at runtime).

**Detection:** Benchmark the `get_all_distinct_tags()` call. If > 100ms, optimize.

**Which phase should address it:** Service implementation phase, when rewriting `document_service.py` queries.

---

### Pitfall 7: `document_sources.sections` JSONB Contains Nested Objects with Large Text

**What goes wrong:** The `sections` column on `document_sources` stores arrays of objects, each containing a `text` field that can be hundreds of lines. In PostgreSQL, JSONB provides efficient storage. In SQLite, storing 9,364 rows where each row's `sections` field might be 10KB+ of JSON text dramatically increases the sidecar file size and memory usage.

**Why it happens:** The `sections` column was designed for PostgreSQL JSONB which has binary compression. In SQLite TEXT storage, the JSON is uncompressed, and parsing it requires reading the entire string.

**Consequences:**
- The sidecar database could be 200MB+ instead of 50MB if sections are stored as JSON text.
- Loading sections for a document requires parsing a potentially large JSON blob for every source row.
- If the developer attempts to index into the JSON for canvas-based lookups, SQLite's O(N) JSON parsing makes each lookup slow.

**Prevention:**
1. Consider normalizing sections into a separate table:
   ```sql
   CREATE TABLE source_sections (
       source_id INTEGER REFERENCES document_sources(id),
       canvas_num INTEGER,
       canvas_url TEXT,
       label TEXT,
       text TEXT,
       PRIMARY KEY (source_id, canvas_num)
   );
   ```
   This allows direct canvas-based lookup without parsing JSON, and SQLite handles large TEXT fields efficiently when they're individual rows.
2. If keeping JSON: accept the size trade-off and parse in Python (current `get_section_for_page` already does this). The JSON parsing per-document is fast enough for single-document lookups.
3. Profile the sidecar size with and without normalized sections before deciding.

**Detection:** Check sidecar file size after build. If > 300MB, consider normalization.

**Which phase should address it:** Schema design phase. Decision on sections storage format affects the entire data model.

---

### Pitfall 8: SQLite Variable Limit (999) with `IN` Queries for Batch Lookups

**What goes wrong:** The existing batch lookup pattern uses `.in_()` for enriching search results (up to 500 IDs per batch). Both existing sidecar services (`fjms_service.py:188`, `nli_crossref_service.py:206`) handle this correctly with `batch_size = 500`. But the new PGP sidecar service must implement the same batching, and it must handle the additional dimension of multi-table lookups (documents + fragments + sources).

**Why it happens:** SQLite has a compile-time limit of 999 variables per query (SQLITE_MAX_VARIABLE_NUMBER). A query like `SELECT * FROM documents WHERE pgpid IN (?, ?, ..., ?)` with 1000 IDs fails. The existing services already know this (they use 500 as batch size), but the new service's batch lookups are more complex: one batch for documents, one for fragments, one for sources.

**Consequences:**
- `OperationalError: too many SQL variables` crashes search results when > 999 results.
- If batch lookups aren't parallelized across tables, the enrichment step becomes 3x slower (serial document + fragment + source queries).

**Prevention:**
1. Copy the proven batch pattern from `fjms_service.py:186-209`:
   ```python
   batch_size = 500
   for i in range(0, len(sys_ids), batch_size):
       batch = sys_ids[i:i + batch_size]
       placeholders = ','.join('?' * len(batch))
       cursor = self._conn.execute(
           f"SELECT ... WHERE sys_id IN ({placeholders})", batch
       )
   ```
2. For multi-table enrichment: do ONE batch query per table, then join in Python. Do NOT do nested queries (document -> fragments -> sources per document) as this causes N+1 query patterns.
3. The `get_sys_ids_with_transcriptions()` function currently chunks at 200 for Supabase URL length limits. SQLite doesn't have URL limits, so the chunk size can increase to 500, matching the other services.

**Detection:** Test with a search returning 1000+ results. If enrichment crashes or takes > 5 seconds, the batching is wrong.

**Which phase should address it:** Service implementation phase. Each batch method must be reviewed for the 999-variable limit.

---

### Pitfall 9: `pgp_url` Generated Column Cannot Be Replicated in SQLite

**What goes wrong:** The `documents` table has a generated column: `pgp_url TEXT GENERATED ALWAYS AS ('https://geniza.princeton.edu/documents/' || pgpid || '/') STORED`. SQLite supports generated columns (since 3.31.0), but the syntax differs slightly and some tools don't handle them well.

**Why it happens:** Developer copies the PostgreSQL schema directly, and either:
1. SQLite accepts the `GENERATED ALWAYS AS` syntax but requires slightly different syntax for the expression.
2. Or the developer forgets the generated column entirely and the `pgp_url` field is NULL everywhere.

**Consequences:**
- Links to PGP website don't work in the UI.
- Minor but confusing: the URL is easy to compute in Python, so this is low-risk.

**Prevention:**
1. Either define the generated column in SQLite (which DOES support it):
   ```sql
   pgp_url TEXT GENERATED ALWAYS AS ('https://geniza.princeton.edu/documents/' || pgpid || '/') STORED
   ```
2. Or compute it in the service layer (simpler, less surprising):
   ```python
   def _add_pgp_url(doc: dict) -> dict:
       doc['pgp_url'] = f"https://geniza.princeton.edu/documents/{doc['pgpid']}/"
       return doc
   ```
3. Prefer option 2 -- the service already transforms database rows into dicts, so adding one computed field is trivial.

**Detection:** Check that PGP links work in both apps after migration.

**Which phase should address it:** Schema design phase (trivial).

---

### Pitfall 10: Data Staleness and Sidecar Update Strategy

**What goes wrong:** Unlike Supabase (which can be updated in real-time via the import scripts), a SQLite sidecar is a static file distributed with the application. Once shipped, the PGP data is frozen until the next sidecar rebuild and distribution.

**Why it happens:** The existing FJMS and NLI sidecars update infrequently (the underlying data changes rarely). But PGP data does change -- new transcriptions are added to the Princeton Geniza Project, new document links are created, tag corrections are made. If the sidecar is built from a snapshot and never updated, the data drifts from the PGP source.

**Consequences:**
- New PGP documents are invisible to GenizahSearch users until the next sidecar rebuild.
- Corrections made on PGP's side don't propagate.
- Users may report "stale data" or "missing documents."

**Prevention:**
1. Document the sidecar build frequency (e.g., "rebuilt monthly" or "rebuilt per release").
2. Include a `meta` table with build date and source version (matching existing pattern in fjms_service and nli_crossref_service).
3. Display the sidecar version/date in the UI (e.g., "PGP data: February 2026") so users understand the data currency.
4. Keep the sidecar build script as a standalone tool that can be run by the maintainer to refresh data.
5. Consider a future hybrid approach: SQLite for bulk reads, Supabase for "hot" updates -- but this adds complexity and should NOT be in v1 of the migration.

**Detection:** Compare sidecar row counts against live PGP data periodically.

**Which phase should address it:** Build script phase. The meta table and version display should be part of the initial implementation.

---

## Minor Pitfalls

Issues that cause minor bugs, slight inefficiencies, or developer confusion.

---

### Pitfall 11: `SERIAL`/`BIGSERIAL` Primary Keys Don't Exist in SQLite

**What goes wrong:** The Supabase schema uses `SERIAL` and `BIGSERIAL` auto-increment types. SQLite uses `INTEGER PRIMARY KEY AUTOINCREMENT` (or just `INTEGER PRIMARY KEY` which auto-increments without the `AUTOINCREMENT` keyword).

**Prevention:** Use `INTEGER PRIMARY KEY` in SQLite schema. For the `documents` table, `pgpid INTEGER PRIMARY KEY` is sufficient (it's a natural key, not auto-generated). For junction tables like `document_fragments`, use `INTEGER PRIMARY KEY AUTOINCREMENT` or omit the ID entirely if it's not needed (look up by composite key instead).

**Which phase should address it:** Schema design phase (trivial).

---

### Pitfall 12: `TIMESTAMPTZ` Columns Don't Exist in SQLite

**What goes wrong:** Supabase uses `TIMESTAMPTZ DEFAULT NOW()` for `created_at` columns. SQLite has no native timestamp type -- it stores text, real, or integer.

**Prevention:** For a read-only sidecar, `created_at` is informational only. Either:
1. Store as TEXT in ISO 8601 format: `'2026-02-16T12:00:00Z'`
2. Or omit entirely -- the sidecar is a snapshot, so creation timestamps are meaningless.

**Which phase should address it:** Schema design phase (trivial).

---

### Pitfall 13: Foreign Key Enforcement Off by Default in SQLite

**What goes wrong:** SQLite does not enforce foreign key constraints by default. The `document_fragments.document_id REFERENCES documents(pgpid)` constraint is parsed but not enforced unless `PRAGMA foreign_keys = ON` is set.

**Prevention:**
1. For a read-only sidecar, foreign keys are informational -- the data integrity was validated during the build process.
2. If desired, add `PRAGMA foreign_keys = ON` in the build script to validate during import, then remove for runtime (slightly faster reads).
3. The existing sidecar services do NOT enable foreign keys (they have flat tables), so this is consistent.

**Which phase should address it:** Build script phase (optional, not critical).

---

### Pitfall 14: Desktop App File Locking on Windows

**What goes wrong:** On Windows, the SQLite database file is locked while open. If the desktop app holds a connection and the user tries to update the sidecar file (e.g., replacing `pgp_data.db` with a new version), the file replacement fails with a "file in use" error.

**Why it happens:** The existing sidecar services open a connection at startup and hold it for the app's lifetime (singleton pattern). On Windows NTFS, this prevents other processes from writing to the file.

**Prevention:**
1. Open the sidecar in read-only mode (already done: `?mode=ro` in URI). This allows concurrent readers on most platforms but still locks on Windows.
2. Document the update procedure: close the desktop app, replace the sidecar file, restart.
3. This is consistent with the existing FJMS and NLI sidecar behavior -- no special handling needed.

**Which phase should address it:** Documentation phase (not a code change).

---

### Pitfall 15: Supabase PostgREST `.single()` Has No SQLite Equivalent

**What goes wrong:** Several queries in `document_service.py` use `.single().execute()` which tells PostgREST to expect exactly one row and return it as a dict (not a list). In SQLite, `cursor.fetchone()` returns a single row, but if multiple rows exist, `.single()` throws an error while `fetchone()` silently returns only the first.

**Prevention:**
1. For queries that should return exactly one row (e.g., `get_document_metadata`), use `fetchone()` and accept that it silently returns the first match.
2. Add a defensive check if needed: `rows = cursor.fetchall(); assert len(rows) <= 1`
3. For `get_document_for_fragment()`, which navigates fragment -> document with potentially multiple matches (recto/verso), the existing logic already handles this by iterating results. No change needed.

**Which phase should address it:** Service implementation phase (straightforward).

---

## Phase-Specific Warnings

| Phase Topic | Likely Pitfall | Mitigation |
|-------------|---------------|------------|
| Schema design | JSONB columns need TEXT + json_each() (Pitfall 1, 2) | Design tag junction table early, validate JSON on import |
| Sidecar build script | Data export from Supabase must handle pagination (> 1000 rows) | Use `.range()` pagination pattern from `import_pgp_sections.py:282` |
| Sidecar build script | sections column bloats file size (Pitfall 7) | Profile both normalized and JSON approaches |
| Service implementation | Thread safety for web app (Pitfall 4) | Copy constructor pattern from fjms_service.py exactly |
| Service implementation | Batch lookups need 500-item chunking (Pitfall 8) | Copy batch pattern from existing services |
| Service implementation | PostgREST-specific operators need SQL rewrite (Pitfall 2) | Map every Supabase call to SQL equivalent before coding |
| FJMS integration | Deduplication between PGP and FJMS (Pitfall 5) | Build scholar name normalization, dedup by (sys_id, scholar, type) |
| Cutover / migration | Supabase user data must remain untouched (Pitfall 3) | Dependency map before removing any Supabase imports |
| Cutover / migration | Dual-read period needed for validation | Run both backends in parallel, compare results, then switch |

---

## Supabase Query to SQLite Query Translation Guide

This table maps every Supabase PostgREST pattern used in `document_service.py` to its SQLite equivalent. Useful as a reference during service implementation.

| Supabase Pattern | SQLite Equivalent | Used In |
|------------------|-------------------|---------|
| `.select('*').eq('pgpid', v).single()` | `SELECT * FROM documents WHERE pgpid = ?` + `fetchone()` | `get_document_for_fragment`, `get_document_metadata` |
| `.select('document_id, page_info').eq('sys_id', v)` | `SELECT document_id, page_info FROM document_fragments WHERE sys_id = ?` | `get_document_for_fragment` |
| `.select('*').eq('document_id', v).order('sequence_order')` | `SELECT * FROM document_fragments WHERE document_id = ? ORDER BY sequence_order` | `get_fragments_for_document` |
| `.select('transcription').eq('pgpid', v).single()` | `SELECT transcription FROM documents WHERE pgpid = ?` + `fetchone()` | `get_transcription_for_document` |
| `.select('*').eq('pgpid', v).order('doc_relation').order('sequence_order')` | `SELECT * FROM document_sources WHERE pgpid = ? ORDER BY doc_relation, sequence_order` | `get_sources_for_document` |
| `.like('doc_relation', '%Edition%')` | `WHERE doc_relation LIKE '%Edition%'` (identical) | `get_editions_for_document` |
| `.like('doc_relation', '%Translation%')` | `WHERE doc_relation LIKE '%Translation%'` (identical) | `get_translations_for_document` |
| `.in_('sys_id', chunk)` | `WHERE sys_id IN (?,?,...)` with batching | `get_sys_ids_with_transcriptions` |
| `.filter('tags', 'cs', json.dumps([tag]))` | `FROM documents d, json_each(d.tags) t WHERE t.value = ?` | `get_fragments_by_tag` |
| `.select('tags').not_.is_('tags', 'null')` | `SELECT tags FROM documents WHERE tags IS NOT NULL AND tags != '[]'` | `get_all_distinct_tags` |

---

## Sources

- [SQLite JSON Functions And Operators](https://sqlite.org/json1.html) -- official reference for json_each(), json_extract(), json_valid()
- [SQLite JSONB Format](https://sqlite.org/jsonb.html) -- confirms SQLite JSONB is NOT binary-compatible with PostgreSQL JSONB
- [Using SQLite In Multi-Threaded Applications](https://www.sqlite.org/threadsafe.html) -- official thread-safety documentation
- [SQLite Write-Ahead Logging](https://sqlite.org/wal.html) -- WAL mode for concurrent reads
- [SQLite json_each() Tutorial](https://www.sqlitetutorial.net/sqlite-json-functions/sqlite-json_each-function/) -- table-valued function for JSON array iteration
- Codebase analysis: `shared/fjms_service.py` (proven SQLite sidecar pattern), `shared/nli_crossref_service.py` (proven thread-safe singleton), `shared/document_service.py` (all Supabase queries being migrated), `web/supabase_client.py` (user data -- NOT being migrated)
