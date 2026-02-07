# Project Research Summary

**Project:** v5.6.0 Desktop Parity & Transcription Search
**Domain:** Dual-app manuscript research platform (NiceGUI web + PyQt6 desktop)
**Researched:** 2026-02-07
**Confidence:** HIGH

## Executive Summary

GenizahSearch is a mature dual-application project with a fully-featured web app (NiceGUI) that integrated 9,364 Princeton Geniza Project transcriptions in v1, and a powerful desktop app (PyQt6) that has zero PGP integration. The web's document service layer (`web/document_service.py`, 507 lines) is currently trapped inside the web package and cannot be consumed by the desktop app without extraction.

**The recommended approach is a clean, low-risk extraction.** Create a `shared/` package at project root containing `supabase_provider.py` (unified Supabase client factory) and `document_service.py` (moved from web/). Leave a re-export shim at `web/document_service.py` so all existing web imports continue working. Both apps then use the shared service for PGP data access. No new external dependencies are required—the existing stack (supabase-py 2.27.2, tantivy-py 0.25.1, PyQt6) has all capabilities needed.

**The key risk is breaking web imports during extraction.** The web has 15+ import sites for document_service functions across search.py, browse.py, joins_panel.py, and tests. The shim pattern prevents this entirely: `web/document_service.py` becomes a 15-line re-export that maintains backward compatibility while the real implementation moves to `shared/document_service.py`. Desktop integration requires QThread workers for all Supabase calls (UI thread blocking is certain without this), and the Tantivy index rebuild must use a build-in-temp-then-swap pattern to avoid destroying users' existing indexes on failure.

## Key Findings

### Recommended Stack

**No new dependencies required.** The existing stack is sufficient for all milestone goals.

**Core technologies:**
- **supabase-py 2.27.2**: Already handles all PGP queries; sync client is thread-safe and proven across web and desktop
- **tantivy-py 0.25.1**: Multi-field search works natively; adding `transcription` field and `content_type` filter field extends existing schema cleanly
- **PyQt6**: QThread pattern already used 14 times in `gui_threads.py`; same pattern applies to PGP data loading

**Critical findings from STACK.md:**
- Tantivy boolean fields are NOT queryable via parse_query() (verified experimentally). Use text field with `raw` tokenizer for filter values instead: `content_type:pgp_edition`, `content_type:htr`.
- The `default` tokenizer handles mixed Hebrew/English PGP transcriptions better than the `whitespace` tokenizer used for HTR content.
- Async Supabase client exists but is NOT recommended—would require qasync integration into PyQt6's event loop, adding complexity for zero benefit since HTTP calls are the bottleneck, not async scheduling.
- Module-level client registration pattern (`set_client()`) is simpler than dependency injection for this use case. Each app calls `set_client()` once at startup; the shared service reads from the registered singleton.

### Expected Features

**Must have (table stakes for desktop parity):**
- **TS-1: PGP transcription display** — when viewing a manuscript, show PGP edition as default if available (replaces V0.8)
- **TS-2: Multi-source version selector** — dropdown showing PGP editions by scholar, translations by language, plus V0.8 and user corrections
- **TS-3: PGP metadata display** — document type, tags, description, dates in a collapsible QGroupBox
- **TS-4: Search result transcription indicators** — green icon in search results table for fragments with PGP transcriptions
- **TS-5: Tag-based search** — search by PGP tags (marriage, commercial, letter, etc.); clickable tags in metadata panel
- **TS-6: Related fragments/joins panel** — extend existing JoinsDialog to merge PGP multi-fragment joins with user joins
- **TS-7: Shared document service layer** — prerequisite for all above; extract to shared/ package

**Estimated effort for table stakes:** 20-27 hours total. TS-7 unlocks everything else.

**Should have (desktop differentiators):**
- **D-2: Keyboard-driven version switching** — Ctrl+Shift+P for PGP, Ctrl+Shift+O for V0.8 (low effort, high value for researchers)
- **D-3: Transcription search with filter toggles** — persistent filter panel with "Has PGP Transcription" checkbox, document type filter, date range
- **D-1: Persistent PGP info panel** — QDockWidget for always-visible metadata (floatable, dockable, persists across navigation)

**Defer (v2+):**
- **D-4: Side-by-side edition comparison** — diff highlighting between scholars' editions (medium-high complexity, niche use case)
- **D-5: Offline PGP data cache** — SQLite cache for offline access (medium-high complexity, requires expiry logic)
- **D-6: Tag cloud/tag browser** — frequency-weighted tag list (low value, discovery feature not core workflow)
- Full-text search within PGP transcriptions (requires Tantivy index rebuild with PGP content; tag search provides 80% of discovery value at 20% of cost)

### Architecture Approach

**The core pattern is extraction with backward-compatible shimming.** The web's document_service imports from `web.supabase_client.get_client()`. The desktop has its own separate Supabase client in `supabase_corrections_client.py`. A third client exists in `lists_sync.py`. All three use the same hardcoded credentials. The solution is a unified provider pattern.

**Major components:**
1. **shared/supabase_provider.py** (new) — Unified client factory with module-level singleton; single source of truth for SUPABASE_URL and SUPABASE_ANON_KEY
2. **shared/document_service.py** (moved from web/) — All 12 PGP data access functions; changes one import line (`from shared.supabase_provider import get_client`)
3. **web/document_service.py** (becomes shim) — Re-exports from shared/ to maintain backward compatibility for 15+ import sites
4. **PGPDataThread(QThread)** (new in gui_threads.py) — Wraps all Supabase calls for desktop to avoid UI thread blocking

**Dependency graph is a clean DAG:**
```
shared/supabase_provider.py  (foundation, no deps)
    |
    +---> shared/document_service.py
    |         |
    |         +---> web/document_service.py (shim)
    |                   |
    |                   +---> web/pages/browse.py, search.py
    |
    +---> web/supabase_client.py (re-exports get_client)
    |
    +---> supabase_corrections_client.py (uses shared config)
```

**Critical design decisions from ARCHITECTURE.md:**
- Place `shared/` at project root (not inside `web/`) so both root-level desktop modules and web/ modules can import it without sys.path hacks
- Desktop will have TWO Supabase client instances by design: one from shared provider (anon key, PGP reads), one from SupabaseCorrectionsClient (authenticated, community operations). This separation is correct—PGP data is public, community data needs auth.
- Pure functions remain in shared service (parse_transcription_sections, get_section_for_page). Only UI rendering differs between apps.

### Critical Pitfalls

1. **Breaking web app import chains during service extraction** (P1) — Moving document_service.py breaks 15+ import sites. **Prevention:** Re-export shim at `web/document_service.py` maintains all existing imports. Smoke test: `python -c "from web.pages.browse import *"` after extraction.

2. **Supabase client singleton conflict** (P2) — Three separate client initialization paths exist today (web, desktop, lists_sync). If shared service connects to wrong client, auth tokens are not shared or PGP queries fail. **Prevention:** Single `shared/supabase_provider.py` with `get_client()`. Both apps use it for PGP reads (anon key sufficient—documents table has public RLS policy).

3. **Blocking PyQt6 UI thread with synchronous Supabase calls** (P3) — All document_service functions are sync. Calling from desktop main thread freezes UI for 100-500ms per call. Windows shows "Not Responding" after ~5s. **Prevention:** QThread workers for ALL Supabase calls. Follow existing pattern: 14 QThread subclasses already in gui_threads.py. Create PGPDataThread with operation dispatch and signals.

4. **Tantivy index rebuild destroys existing index without rollback** (P4) — Current `create_index()` does `shutil.rmtree(db_path)` before building new index. If rebuild fails midway (OOM, corrupted file, power loss), user has NO index. **Prevention:** Build in temp directory (`tantivy_db_new/`), verify doc count, atomic swap via `os.rename()`, keep backup until verified.

5. **Duplicating UI logic instead of sharing display logic** (P6) — Temptation to copy-paste transcription parsing into desktop. Creates maintenance nightmare—bug fixes applied to one UI but not the other. **Prevention:** Business logic (parse_transcription_sections, get_section_for_page) stays in shared service as pure functions. Only rendering (QTextBrowser vs ui.html) differs.

## Implications for Roadmap

Based on research, suggested phase structure:

### Phase 1: Foundation — Shared Service Extraction
**Rationale:** Everything depends on extracting document_service to a shared location. This must come first. Zero new dependencies, low risk with shim pattern, enables all desktop PGP work.

**Delivers:**
- `shared/supabase_provider.py` with unified client factory
- `shared/document_service.py` with all 12 PGP functions
- `web/document_service.py` shim for backward compatibility
- Smoke tests confirming web app still works

**Avoids:** Pitfall 1 (broken imports via shim), Pitfall 2 (client confusion via provider pattern), Pitfall 9 (removes sys.path hack)

**Risk:** LOW. Adding new package, leaving shim. If anything breaks, it's a typo in shim.

**Research flag:** Skip research-phase. Pattern is clear from architecture analysis.

### Phase 2: Desktop PGP Core — Transcription Display
**Rationale:** Highest user value. Researchers want to see PGP transcriptions in desktop just like web. Extends existing ResultDialog and browse tab patterns.

**Delivers:**
- TS-1: PGP transcription display in manuscript viewer
- TS-2: Multi-source version selector (extends existing rd_version_combo)
- PGPDataThread in gui_threads.py for async loading

**Uses:** shared/document_service functions via QThread workers

**Implements:** QThread pattern (already proven 14 times in gui_threads.py)

**Avoids:** Pitfall 3 (UI thread blocking via QThread), Pitfall 6 (duplicates logic—uses shared parser functions)

**Risk:** MEDIUM. New UI work in PyQt6. QThread pattern is proven but requires careful signal/slot wiring.

**Research flag:** Skip research-phase. Desktop patterns are well-established in codebase.

### Phase 3: Desktop PGP Metadata — Enriched Viewer
**Rationale:** Completes the viewer experience. Researchers need document type, tags, dates, description to understand what they're viewing. Enables tag-based discovery.

**Delivers:**
- TS-3: PGP metadata display (QGroupBox in viewer)
- TS-5: Tag-based search (new search mode)
- TS-6: Related fragments/joins (extends existing JoinsDialog)

**Addresses:** Table stakes TS-3, TS-5, TS-6 from FEATURES.md

**Avoids:** Pitfall 6 (reuses shared service functions), Pitfall 7 (offline handling—connectivity check before load)

**Risk:** MEDIUM. Tag search is a new search code path. Joins extension touches existing JoinsDialog.

**Research flag:** Skip research-phase. Tag search uses existing get_fragments_by_tag() from shared service.

### Phase 4: Desktop PGP Discovery — Search Integration
**Rationale:** Makes PGP data discoverable in search results. Low effort (batch API exists), high visibility (green indicators scannable in results table).

**Delivers:**
- TS-4: Search result transcription indicators (green icon in table)
- D-2: Keyboard version switching (Ctrl+Shift+P shortcuts)

**Uses:** Batch function get_sys_ids_with_transcriptions() from shared service

**Avoids:** Pitfall 12 (N+1 queries—uses batch function, not loop)

**Risk:** LOW. Batch API exists. QTableWidget column addition is straightforward.

**Research flag:** Skip research-phase. Batch enrichment pattern proven in web search.py.

### Phase 5: Tantivy Transcription Index — Full-Text Search
**Rationale:** Extends search to include PGP transcription content. Requires schema change (full rebuild). Should come after desktop UI features are working so users have immediate value before triggering rebuild.

**Delivers:**
- Tantivy schema extension (transcription field + content_type field)
- PGP transcription indexing during build
- Search filter modes (Everything, Transcriptions Only, HTR Only)
- D-3: Filter toggles in desktop search tab

**Uses:** tantivy-py 0.25.1 multi-field search (verified working)

**Implements:** Build-in-temp-then-swap pattern for safe rebuild

**Avoids:** Pitfall 4 (destructive rebuild via temp dir), Pitfall 8 (schema version check for graceful degradation)

**Risk:** MEDIUM. Index rebuild is minutes-long process. Schema version mismatch could confuse users with old indexes.

**Research flag:** Skip research-phase. Tantivy patterns are verified experimentally in STACK.md.

### Phase Ordering Rationale

- **Phase 1 first:** Foundation. Nothing works without shared service extraction. Lowest risk, highest unblocking value.
- **Phases 2-4 before Phase 5:** Desktop gets immediate PGP parity without waiting for index rebuild. Users can view transcriptions, metadata, and see indicators in search results using existing Supabase queries. Index rebuild (Phase 5) takes minutes and requires all users to rebuild—defer until desktop features prove valuable.
- **Phase 3 after Phase 2:** Metadata display depends on transcription display working. Tag search depends on metadata panel (clickable tags).
- **Phase 4 is independent:** Can happen anytime after Phase 1. Placed here because it is low effort and high visibility.
- **Phase 5 last:** Full-text transcription search is nice-to-have. Tag search (Phase 3) provides 80% of discovery value. Index rebuild is risky (Pitfall 4) and should be validated thoroughly.

### Research Flags

**Phases with standard patterns (skip research-phase):**
- **Phase 1:** Python module extraction patterns are well-understood. Re-export shim is standard practice.
- **Phase 2:** QThread pattern used 14 times already in gui_threads.py. Extending existing version combo and ResultDialog.
- **Phase 3:** Tag search uses existing shared service function. JoinsDialog extension follows existing desktop patterns.
- **Phase 4:** Batch enrichment proven in web search.py. QTableWidget manipulation is standard PyQt6.
- **Phase 5:** Tantivy multi-field search verified experimentally. Schema fields tested locally.

**No phases need research-phase.** All patterns are either proven in the existing codebase or experimentally verified during project research.

## Confidence Assessment

| Area | Confidence | Notes |
|------|------------|-------|
| Stack | HIGH | All capabilities verified against installed packages (supabase-py 2.27.2, tantivy-py 0.25.1). Tantivy boolean field limitation confirmed experimentally. |
| Features | HIGH | Based on direct codebase analysis of web/document_service.py (507 lines), web/pages/browse.py, web/pages/search.py. Every web feature mapped to desktop equivalent. |
| Architecture | HIGH | Import chains traced across 15+ files. Client singleton patterns analyzed in web/supabase_client.py, supabase_corrections_client.py, lists_sync.py. Dependency graph is clean DAG. |
| Pitfalls | HIGH | Every pitfall verified against actual code with line numbers. QThread requirement verified by inspecting PyQt6 event loop. Index rebuild risk verified by reading Indexer.create_index() (genizah_core.py:3897-3899). |

**Overall confidence:** HIGH

### Gaps to Address

**Recto/verso section header stripping (known tech debt from v1):** The parse_transcription_sections() function strips section headers like "Recto" and "Verso - right margin". Noted in MEMORY.md as existing tech debt. **Handling:** Fix during Phase 1 extraction by adding include_headers parameter. Low effort, opportunistic fix.

**No integration tests for E2E flows (known tech debt from v1):** No tests verify web imports work after extraction. **Handling:** Write minimal smoke tests in Phase 1 before moving any code. Test imports from both shared/ and web/ (shim).

**Desktop offline handling:** Desktop may run without internet. Supabase calls will fail silently. **Handling:** Add connectivity check in Phase 2 (reuse is_server_available() from corrections client). Show "PGP data unavailable (offline)" rather than empty sections. Cache successfully loaded data.

**Schema version mismatch during Tantivy rebuild:** Users with old indexes may not rebuild immediately. Query code referencing new fields will fail. **Handling:** Add schema version file in Phase 5. Check on startup, prompt for rebuild if mismatch. Graceful degradation: check available_fields before using transcription field.

## Sources

### Primary (HIGH confidence)

**Direct codebase analysis (all verified 2026-02-07):**
- `web/document_service.py` — 507 lines, 12 functions, all analyzed
- `web/supabase_client.py` — 1190 lines, client singleton pattern at line 30-43
- `supabase_corrections_client.py` — 1834 lines, separate client at line 292-307, sys.path hack at line 21
- `lists_sync.py` — third client initialization at line 78
- `genizah_core.py` — Indexer class at line 3882, schema at 3902-3909, SearchEngine at 4158+
- `gui_threads.py` — 14 QThread subclasses analyzed (IndexerThread, SearchThread, EnrichMetadataThread, etc.)
- `genizah_app.py` — 15,800 lines, ResultDialog, ManuscriptViewerWidget, corrections integration
- `web/pages/browse.py` — PGP transcription display (lines 883-950), metadata panel (1753-1818)
- `web/pages/search.py` — Tag search (2310-2404), transcription indicators (1212-1217)
- `web/components/version_selector.py` — 376 lines, multi-source selection logic
- `web/components/joins_panel.py` — 884 lines, PGP joins integration

**Experimental verification:**
- tantivy-py 0.25.1 installed locally via `pip show tantivy`
- Tantivy boolean field limitation verified (boolean fields with fast=True are NOT queryable via parse_query())
- Tantivy text field with raw tokenizer verified (exact-match filtering works)
- Multi-field default query verified (parse_query with default_field_names=['content', 'transcription'])
- supabase-py 2.27.2 sync client verified (Client.execute() is not a coroutine, no async/await needed)

**Project memory:**
- MEMORY.md: "shared service layer (Option C)" — user's chosen approach
- MEMORY.md: Recto/verso headers stripped, no integration tests, TODO at document_service.py:253

### Secondary (MEDIUM confidence)

- [Tantivy Issue #301](https://github.com/quickwit-oss/tantivy/issues/301) — confirms no in-place schema changes
- Qt QDockWidget documentation — desktop differentiator patterns
- Real Python PyQt QThread patterns — confirms signal/slot async pattern

### No tertiary sources needed

All findings verified against installed packages or direct codebase analysis. No external research was required beyond confirming one Tantivy limitation via GitHub issue.

---
*Research completed: 2026-02-07*
*Ready for roadmap: yes*
