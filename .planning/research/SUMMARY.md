# Project Research Summary

**Project:** PGP Sidecar Migration + FJMS Full Texts
**Domain:** Database migration (cloud-to-local), SQLite sidecar architecture, scholarly text integration
**Researched:** 2026-02-16
**Confidence:** HIGH

## Executive Summary

This milestone migrates all PGP reference data (104K rows across 4 tables: documents, document_sources, document_fragments, document_footnotes) from Supabase PostgreSQL to a local SQLite sidecar, following the proven pattern established by fjms_enrichment.db and nli_crossref.db. Concurrently, it integrates 65K FJMS catalog descriptions (scholarly identification texts, not line-by-line transcriptions) as additional sources in the version selector. Together these changes eliminate cloud dependency for browsing, reduce query latency from 50-200ms to <1ms, enable offline desktop usage, and unify all reference data in local sidecars while preserving Supabase for community features (auth, corrections, lists, comments).

The architecture is straightforward: create shared/pgp_service.py following the FjmsService/NliCrossrefService pattern, rewrite shared/document_service.py to swap Supabase calls for PgpService calls (preserving the API surface so all callers remain unchanged), and extend fjms_enrichment.db with a full_texts table. No new dependencies are required — Python's built-in sqlite3 module handles everything. The critical technical challenge is JSONB-to-TEXT conversion for the documents.tags and document_sources.sections columns, which requires careful JSON serialization and a query translation from PostgreSQL's GIN-indexed @> operator to SQLite's json_each() table function.

The main risks are: (1) silently corrupting JSON data during migration, (2) accidentally breaking user-data features by removing Supabase imports needed by desktop corrections/lists, (3) poor thread-safety causing crashes under concurrent web load, and (4) duplicate sources appearing when both PGP and FJMS have overlapping content. All four are mitigatable with proven patterns from existing codebase migrations and strict adherence to the established sidecar service architecture.

## Key Findings

### Recommended Stack

No new dependencies required. The migration uses Python 3.11's built-in sqlite3 module (SQLite 3.45.1) with JSON1 extension support verified available. The existing sidecar services (fjms_enrichment.db at 246 MB, nli_crossref.db at 248 MB) prove the pattern scales. The pgp.db sidecar will be ~110 MB (35,839 documents + 36,155 fragments + 9,364 sources + 22,757 footnotes). Supabase remains for community features only (auth, corrections, lists, comments).

**Core technologies:**
- **sqlite3 (stdlib)**: Read-only sidecar database — proven by 2 existing services, thread-safe mode for web, graceful degradation
- **Supabase (retained)**: Community features only — auth, corrections, lists, comments (user-generated data requiring shared state)
- **JSON1 extension**: Built-in json_each() for tag queries — replaces PostgreSQL GIN index with table-valued function
- **Python json module**: Parse sections JSONB on retrieval — same pattern as Supabase client already returns dicts/lists

**Critical decision from STACK.md:** New pgp_data/pgp.db sidecar (not extending existing sidecars) because each has distinct domain boundary (FJMS=scholarly metadata, NLI=image crossrefs, PGP=document data), independent update cycles, and independent versioning. The 110 MB size is manageable for distribution.

### Expected Features

From FEATURES.md analysis, 9 table-stakes features and 4 differentiators identified:

**Must have (table stakes):**
- **TS-1: PGP Data in SQLite Sidecar** — All 4 Supabase tables exported to pgp.db, 104K rows, foundation for everything
- **TS-2: document_service.py Rewritten for SQLite** — Same 12-function API, different backend (Supabase to PgpService), preserves shim contract
- **TS-3: Version Selector Continues Working** — PGP editions/translations display unchanged if API surface preserved
- **TS-4: Search Result PGP Indicators** — Batch lookup of sys_ids with transcriptions (currently Supabase IN, becomes SQLite IN with 500 batch)
- **TS-5: Tag-Based Search** — documents.tags JSONB to TEXT, query with json_each() or normalized junction table
- **TS-6: PGP Footnotes Display** — 22,757 bibliography footnotes (simple table migration)
- **TS-7: Multi-Fragment Navigation** — Joined manuscripts (get_fragments_for_document with ORDER BY)
- **TS-8: Graceful Degradation** — is_available() pattern when sidecar missing
- **TS-9: Remove Supabase Tables** — After verification, drop 4 PGP tables from Supabase (keep user-data tables)

**Should have (competitive):**
- **D-1: FJMS Full Texts in Version Selector** — 65K catalog descriptions from dbo_UnitFullText as scholarly sources (NOT line-by-line transcriptions — clarification: these are English/Hebrew identification texts like "Contains Genesis 39:20-41:8 with vowel points")
- **D-2: FJMS Source Badges** — Purple badge in version selector for FJMS content (consistent visual language)
- **D-3: Offline Browsing** — Desktop app browses all PGP+FJMS data without internet (only images/community need network)
- **D-4: Faster Queries** — SQLite <1ms vs Supabase 50-200ms latency (free benefit of migration)

**Defer (v2+):**
- D-5: FJMS Bibliography Transcription Indicators (nice-to-have)
- AF-7: FJMS Full-Text Search (separate milestone)

**Anti-features (explicitly not building):**
- AF-1: Line-by-Line FJMS Transcription Import (dbo_UnitTranscription has 56K external file references, copyright concerns)
- AF-2: Real-Time Supabase-SQLite Sync (static exports sufficient for infrequent updates)
- AF-6: Bidirectional Editing in Sidecar (sidecars are read-only, corrections go through Supabase)

### Architecture Approach

From ARCHITECTURE.md: Migration is internal to shared/document_service.py — web/pages and desktop/genizah_app.py callers remain untouched. The shim pattern (web/document_service.py re-exports from shared/) ensures zero web import changes. Desktop already uses lazy imports inside methods.

**Major components:**
1. **PgpService (NEW)** — SQLite accessor for pgp.db, 8 methods mirroring current Supabase queries (get_document, get_fragments_for_sys_id, get_sources_for_document, get_sys_ids_with_documents, etc.), handles JSON parsing for tags/sections
2. **document_service.py (MODIFIED)** — Swap Supabase client calls for PgpService calls, pure functions (parse_transcription_sections, get_section_for_page, parse_html_sections) stay unchanged
3. **fjms_service.py (EXTENDED)** — Add get_full_texts() method for FJMS catalog descriptions from new full_texts table
4. **export_pgp_sidecar.py (NEW)** — Paginated export from Supabase to pgp.db with JSON serialization validation
5. **export_fist_enrichment.py (EXTENDED)** — Add export_fulltext() for dbo_UnitFullText to fjms_enrichment.db

**Data flow change (from ARCHITECTURE.md):**
- BEFORE: get_sys_ids_with_transcriptions to Supabase HTTP IN query to 50-200ms
- AFTER: get_sys_ids_with_transcriptions to pgp_service to SQLite IN with 500 batch to <1ms

**Schema design (from STACK.md Decision 4):**
- documents: pgpid PK, tags as TEXT JSON, transcription as TEXT (~50 MB)
- document_fragments: sys_id indexed, document_id FK (~3 MB)
- document_sources: pgpid FK, sections as TEXT JSON, content as TEXT (~40 MB)
- document_footnotes: pgpid FK (~15 MB)
- meta: version tracking (matches existing sidecar pattern)
- Total: ~110 MB pgp.db

**JSONB handling (STACK.md Decision 2):** Store as TEXT JSON strings, parse with json.loads() in service layer (same as Supabase client returns), query tags with json_each() or LIKE for single checks. The sections column parsing already exists in get_section_for_page() — just changes source from Supabase JSON to SQLite TEXT JSON.

### Critical Pitfalls

From PITFALLS.md, 5 critical issues identified with mitigations:

1. **JSONB-to-TEXT Data Loss (tags, sections)** — PostgreSQL JSONB binary format NOT compatible with SQLite. Must serialize with json.dumps(), validate with json_valid() on every row post-import. Tags query requires json_each() table function (not GIN @> operator). Sections column is 10KB+ JSON per row, consider normalizing into separate table if file size > 300MB. PREVENTION: Validation pass after export, test round-trip on every JSONB column.

2. **Removing Supabase While User-Data Depends On It** — Two Supabase clients exist: shared/supabase_provider.py (document data, being migrated) and web/supabase_client.py + supabase_corrections_client.py (user data, MUST STAY). Desktop corrections (1,800+ lines), lists, joins, discoveries all use Supabase. PREVENTION: Dependency map before removing ANY imports, test both apps end-to-end for corrections/lists after migration.

3. **Thread Safety for NiceGUI Web** — Web app is async, SQLite calls are blocking. Requires check_same_thread=False in PgpService constructor and await run.io_bound() at all call sites (15+ in web/pages). Without this: ProgrammingError under concurrent load. PREVENTION: Copy fjms_service.py constructor pattern exactly, load test with 3+ concurrent tabs.

4. **PostgreSQL GIN Index to SQLite json_each()** — Tag search uses filter('tags', 'cs', [tag]) which is GIN @> operator. SQLite equivalent: SELECT FROM documents, json_each(tags) WHERE value = ? (table-valued function, no index). Consider normalized document_tags junction table if > 100ms. PREVENTION: Benchmark json_each() on 35K rows before deciding.

5. **Deduplication Between PGP/FJMS Sources** — Same manuscript may have PGP edition + FJMS catalog description. Dedup by (sys_id, normalized_scholar_name, relation_type), NOT content similarity. Build scholar name normalization map (e.g., "S.D. Goitein" = "Goitein"). Prefer PGP for transcriptions, show FJMS as alternative. PREVENTION: Test with 10 known-overlapping sys_ids, verify version selector shows distinct sources correctly.

Additional moderate pitfalls (PITFALLS.md Pitfall 6-10): full table scan for tags, sections storage size, SQLite 999 variable limit for batch IN queries, generated pgp_url column, data staleness strategy. All have documented mitigations.

## Implications for Roadmap

Based on research, suggested 4-phase structure with clear dependencies:

### Phase 1: PGP Sidecar Foundation
**Rationale:** All features depend on the sidecar existing. Export script must be bulletproof — JSON validation, pagination, batch inserts. This establishes the data layer.
**Delivers:** pgp_data/pgp.db with 104K rows, export_pgp_sidecar.py script, meta table with version tracking
**Addresses:** TS-1 (PGP Sidecar), avoids Pitfall 1 (JSONB loss) via validation pass
**Key decision:** Tags storage (json_each vs junction table) must be made here — affects schema and service queries
**Research flag:** LOW — follows proven export_fist_enrichment.py pattern (8 existing export functions)

### Phase 2: PGP Service Layer
**Rationale:** Service isolates SQLite access, enables parallel development of FJMS integration. Thread-safety and batch patterns must be correct here — testing is critical.
**Delivers:** shared/pgp_service.py with 8 methods, thread_safe flag, singleton pattern, is_available() degradation
**Addresses:** TS-2 (document_service rewrite), TS-8 (graceful degradation), avoids Pitfall 3 (thread safety) and Pitfall 4 (GIN index)
**Uses:** sqlite3 (stdlib), json module for sections parsing
**Implements:** Read-only sidecar service pattern (proven by FjmsService 961 lines, NliCrossrefService 845 lines)
**Research flag:** MEDIUM — Query translation from Supabase to SQLite needs careful implementation (PITFALLS.md has translation guide)

### Phase 3: FJMS Full Texts Integration (parallel with Phase 2)
**Rationale:** Independent of PGP migration — extends existing fjms_enrichment.db. Can proceed in parallel with Phase 2 to speed delivery.
**Delivers:** full_texts table in fjms_enrichment.db (65K rows, ~15-25 MB), export_fulltext() in export_fist_enrichment.py, get_full_texts() in fjms_service.py
**Addresses:** D-1 (FJMS catalog descriptions), D-2 (purple badges in version selector)
**Key clarification:** These are catalog identification texts (English/Hebrew descriptions of manuscript content), NOT line-by-line transcriptions
**Research flag:** LOW — extends existing service, standard pattern

### Phase 4: Verification and Cutover
**Rationale:** All table-stakes features must work before removing Supabase tables. Parallel reads validate correctness. Dependency map prevents breaking user-data features.
**Delivers:** TS-3 through TS-7 verified (version selector, search indicators, tags, footnotes, navigation), TS-9 (Supabase table removal), documentation of offline capabilities
**Addresses:** Avoids Pitfall 2 (user-data dependency) via explicit mapping, Pitfall 10 (staleness) via version display
**Testing:** Both apps end-to-end (search, browse, corrections, lists), load test web concurrency, verify 10 overlapping PGP/FJMS sources
**Research flag:** MEDIUM — Requires analysis of which Supabase tables can be safely removed (PITFALLS.md Pitfall 3 has dependency mapping guidance)

### Phase Ordering Rationale

- Phase 1 first: Sidecar is foundation, export script with validation prevents data corruption (PITFALLS.md Pitfall 1)
- Phase 2 follows: Service isolates SQLite complexity, enables testing before UI integration
- Phase 3 parallel: FJMS is independent data source, no PGP migration dependency (can run concurrently with Phase 2)
- Phase 4 last: Verification phase prevents premature cutover, protects user-data features (PITFALLS.md Pitfall 2)

**Critical path:** Phase 1 to Phase 2 to Phase 4 (PGP migration)
**Parallel track:** Phase 3 (FJMS full texts, can start anytime after Phase 1 data pipeline established)

**Dependencies identified:**
- Tags storage decision (Phase 1) affects service queries (Phase 2)
- Thread-safe constructor (Phase 2) required before web integration (Phase 4)
- Supabase dependency map (Phase 4) required before table removal
- FJMS export (Phase 1) required for Phase 3 UI integration

### Research Flags

**Phases needing deeper research during planning:**
- **Phase 2:** Query translation from Supabase PostgREST to SQLite — PITFALLS.md has full translation guide (Pitfall 4 for GIN index, table at end for all patterns). Needs careful implementation of json_each() for tags.
- **Phase 4:** Dependency mapping to identify which Supabase imports can be safely removed — PITFALLS.md Pitfall 3 lists all current Supabase clients and their purposes.

**Phases with standard patterns (skip research-phase):**
- **Phase 1:** Export script follows export_fist_enrichment.py pattern (8 existing export functions, proven pagination)
- **Phase 3:** Service extension follows existing fjms_service.py pattern

**Validation checkpoints during execution:**
- Phase 1: JSON round-trip test, row count verification, json_valid() on all JSONB columns
- Phase 2: Thread-safety load test (3+ concurrent web tabs), batch query correctness (500-item chunks)
- Phase 4: Both apps end-to-end, corrections/lists still work, version selector shows PGP+FJMS correctly

## Confidence Assessment

| Area | Confidence | Notes |
|------|------------|-------|
| Stack | HIGH | No new dependencies, sqlite3 built-in verified, JSON1 available, proven by 2 existing sidecars (246 MB, 248 MB) |
| Features | HIGH | All 9 table-stakes mapped to existing Supabase queries (1:1 SQL translation documented), 4 differentiators are natural consequences |
| Architecture | HIGH | PgpService pattern proven by FjmsService/NliCrossrefService (961 lines, 845 lines), shim pattern preserves all 11 consumer files |
| Pitfalls | HIGH | Based on direct codebase analysis (all 4 Supabase tables, 15+ call sites, 2 existing sidecars), 15 pitfalls catalogued with proven mitigations |

**Overall confidence:** HIGH

### Gaps to Address

- **Tags query performance**: Benchmark json_each() on 35K rows before deciding between it and normalized junction table. Decision affects Phase 1 schema and Phase 2 service queries. Mitigation: Start with json_each() (simpler), add junction table in Phase 4 if profiling shows > 100ms.

- **Sections storage size**: Profile pgp.db file size with sections as JSON TEXT vs normalized table. 9,364 sources * avg 2KB = ~19 MB of content, sections could add 10-50 MB depending on sparsity. Mitigation: Build with JSON first (STACK.md Decision 3 rationale: sections always read as complete array), normalize in Phase 4 only if > 300 MB total.

- **FJMS/PGP overlap extent**: Unknown how many sys_ids have both PGP sources AND FJMS catalog descriptions. Affects deduplication complexity. Mitigation: Query both datasets during Phase 1 to get overlap count, design dedup strategy in Phase 4 based on actual numbers (PITFALLS.md Pitfall 5 has dedup approach).

- **Supabase pagination for export**: 35,839 documents exceed single-page fetch. Must use .range() pagination (1000 rows/page pattern from import_pgp_sections.py:282). Mitigation: STACK.md Decision 6 documents exact pagination pattern.

- **Desktop app file distribution**: pgp.db must be bundled with desktop installer. Update build_app.bat to include pgp_data/ alongside fist_data/ and nli_data/. Mitigation: Add to Phase 4 verification checklist (STACK.md Decision 6 notes distribution requirement).

## Sources

### Primary (HIGH confidence)
- **Codebase analysis**: shared/document_service.py (742 lines, 14 functions, 7 Supabase-calling), shared/fjms_service.py (961 lines, proven pattern), shared/nli_crossref_service.py (845 lines, thread-safe singleton), migrations/*.sql (9 files, full Supabase schema), web/components/version_selector.py (440 lines)
- **Data verification**: Supabase table counts (35,839 documents, 36,155 fragments, 9,364 sources, 22,757 footnotes), FIST.db dbo_UnitFullText (65,332 rows, 85,313 AlmaIds, ~56K > 100 chars, avg 240 chars)
- **Python/SQLite verification**: Python 3.11.9, SQLite 3.45.1, JSON1 functions (json_array, json_type, json_each) confirmed working
- **SQLite documentation**: [sqlite.org/json1.html](https://sqlite.org/json1.html) (JSON1 functions), [sqlite.org/jsonb.html](https://sqlite.org/jsonb.html) (JSONB incompatibility with PostgreSQL), [sqlite.org/threadsafe.html](https://www.sqlite.org/threadsafe.html) (check_same_thread)

### Secondary (MEDIUM confidence)
- **Princeton Geniza Project**: [geniza.princeton.edu](https://geniza.princeton.edu/en/) — PGP handles multiple editions per document with scholar attribution, validates dedup approach
- **Friedberg Research Platform**: Multiple transcription sources per manuscript (validates multi-source display pattern)
- **Supabase offline discussion**: Real-time CDC complexity (PowerSync, RxDB) — confirms static export strategy appropriate for infrequent updates

---
*Research completed: 2026-02-16*
*Ready for roadmap: yes*
