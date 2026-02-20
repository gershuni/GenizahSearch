# Project State

## Project Reference

See: .planning/PROJECT.md (updated 2026-02-16)

**Core value:** Researchers can find what they need in the Genizah corpus
**Current focus:** Phase 39 (Bug Fixing, Cleanup, Performance Improving)

## Current Position

Phase: 39 of 39 (Bug Fixing, Cleanup, Performance Improving)
Plan: 8 of 8 in current phase (COMPLETE)
Status: Phase 39 Complete
Last activity: 2026-02-20 - Completed 39-08: Page navigation speed (parallel queries + async loading)

Progress: [##########] 100% (Phase 39: 8/8 plans)

## Performance Metrics

**Velocity:**
- Total plans completed: 115 (across 7 milestones)
- Average duration: ~8 min
- Total execution time: ~10 hours

**By Milestone:**

| Milestone | Phases | Plans | Total Time |
|-----------|--------|-------|------------|
| v1 | 1-7 | 18 | 173 min |
| v5.6.0 | 8-12 | 25 | ~134 min |
| v5.7.0 | 14-17 | 14 | ~140 min |
| v5.7.2 | 18-21 | 11 | ~1 day |
| v5.7.3 | 22-24 | 3 | 6 min |
| v5.8.0 | 25-28 | 12 | 57 min |
| v5.9.0 | 29-34 | 22 | ~90 min |
| v6.0.0 | 35-38 | 8 | 68 min |
| Phase 39 P08 | 14min | 2 tasks | 3 files |

## Accumulated Context

### Decisions

See PROJECT.md Key Decisions table for full history.
Recent decisions affecting current work:

- v6.0.0: New pgp.db sidecar (not extending existing sidecars) -- distinct domain boundary
- v6.0.0: Tags stored as TEXT JSON, queried with json_each() -- start simple, optimize if >100ms
- v6.0.0: Supabase PGP tables kept (legacy desktop users) -- cutover deferred to future milestone
- Phase 35: Hardcoded Supabase URL/anon key defaults matching codebase pattern
- Phase 35: pgp_url stored as plain TEXT from Supabase generated column
- Phase 35: Compact JSON (sort_keys, no spaces) for deterministic sidecar serialization
- Phase 36: get_pgp_service() defaults to thread_safe=True (read-only SQLite safe across threads)
- Phase 36: get_all_sources_for_fragment optimized from N+1 to 2 queries
- Phase 36: _row_to_dict helper centralizes JSON deserialization for tags/sections columns
- Phase 36: Temp file SQLite fixtures (not :memory:) for testing -- PgpService requires real file for read-only URI mode
- Phase 36: Inline dict assignment (no helper extraction) for FL ID path pgp_metadata -- matches existing load_page pattern
- Phase 37: REVERTED v1; v2 re-planned with enriched FIST.db export (4 new tables)
- Phase 37-01: Contentless FTS5 (content='') for cross-table aggregation (catalog + running titles + free desc)
- Phase 37-01: catalog_free_desc joins via SignatureId (not UnitCatalogRecId) per FIST schema
- Phase 37-01: catalog_fields resolves categories via CODE_FullCode -> CODE_FCDTable two-hop JOIN
- Phase 37-02: get_catalog_detail() wraps each child-table sub-query in try/except for backward compat with old sidecars
- Phase 37-02: New v3.0.0 columns accessed via col_names membership check for backward compat
- Phase 37-03: show_catalog_dialog() creates+opens in one call (simpler than create+open bibliography pattern)
- Phase 37-03: Batch catalog source counts fetched during search execution via get_catalog_source_counts()
- Phase 37-04: HTML table in QTextBrowser mirrors web dialog approach for consistent rendering with RTL support
- Phase 37-04: Catalog detail cached per browse/result to avoid repeated DB queries on button click
- Phase 38-01: GenizahSearchPro.spec gitignored (PyInstaller-generated) -- build_app.bat is source of truth for build config
- Phase 38-02: Import inspection via extracted import lines (not raw source grep) to avoid false positives from docstrings
- Phase 38-03: Sidecar updates download to LOCALAPPDATA (safe for read-only bundled locations)
- Phase 38-03: Service __init__ checks LOCALAPPDATA first, falls back to project root (minimal change)
- Phase 38-03: Sequential download queue with singleton reset after completion
- Phase 39-04: Double-checked locking for thread-safe hierarchy caching (not lru_cache) -- explicit semantics, avoids sqlite3.Row pickling issues
- Phase 39-04: COUNT(*) replaces COUNT(DISTINCT AlmaId) in domain hierarchy query -- no duplicate tuples exist in domains table
- Phase 39-03: PostHog maskAllInputs + identified_only for privacy -- researchers' search inputs not in session replays
- Phase 39-03: Env-var-gated PostHog (empty string when POSTHOG_API_KEY not set) -- zero cost when disabled
- Phase 39-02: PAGE_SIZE=50 for pagination (not 100+ to stay within WebSocket comfort zone)
- Phase 39-02: Storage persistence cap raised from 200 to 1000 (20 pages of refresh recovery)
- Phase 39-01: sip.isdeleted() guards inside set_status_message/update_text_pos (protects all callers uniformly)
- Phase 39-01: Only one unsafe res['uid'] bracket access found -- all others already use safe .get()
- Phase 39-05: Custom Screen fixture bypasses NiceGUI inipath requirement (request=None + override start_server)
- Phase 39-05: App-level E2E tests start actual web/main.py via runpy (not stub pages)
- Phase 39-05: selenium + pytest-selenium as dev dependencies, skip logic for CI environments without ChromeDriver
- Phase 39-06: scrollTo queued before render_results (JS executes client-side even after Python element deletion)
- Phase 39-06: Ctrl+wheel for zoom, plain wheel for scroll (matches standard app convention)
- Phase 39-06: pytest.importorskip over try/except (raises pytest.skip during collection, before module-level imports)
- Phase 39-07: CSS extracted verbatim to static file (no rule changes, indentation removed)
- Phase 39-07: Lazy dialog uses nonlocal pattern (simple closure, no new dependencies)
- Phase 39-08: asyncio.gather + run.io_bound for parallel off-thread queries (search enrichment, discoveries initial load)
- Phase 39-08: Pre-fetch FJMS in load_page, read from state.fjms_data in update_content (separates I/O from rendering)
- Phase 39-08: Pure-UI render helpers (_render_stat_cards, _render_feed_result) for testable data-to-UI separation

### Blockers/Concerns

- Phase 13 (Transcription Search) still deferred -- needs server-side index architecture
- Tags json_each() benchmarked: 115ms for get_all_distinct_tags (2695 tags), 63ms for tag search -- acceptable
- FJMS/PGP overlap extent unknown -- affects dedup strategy in Phase 38
- Phase 37 fully complete (all 4 plans: export enrichment, service layer, web UI, desktop UI)

### Quick Tasks Completed

| # | Description | Date | Commit | Directory |
|---|-------------|------|--------|-----------|
| 14 | Fix domain filtering for misc categories that appear under multiple parents | 2026-02-18 | 9bc50777 | [14-fix-domain-filtering-for-misc-categories](./quick/14-fix-domain-filtering-for-misc-categories/) |

### Roadmap Evolution

- Phase 39 added: bug fixing, cleanup, performance improving
- Phase 40 added: Performance Optimization (profiling-driven — parallelize NLI calls, defer catalog queries, async domain enrichment, variant cache unification, FL ID index, browse crossref parallelization)

### Pending Todos

- JA diacritic dots normalization in search
- Migrate desktop corrections fetch to shared corrections_service
- Domain click behavior in browse metadata
- Pre-search domain filtering optimization
- FJMS export script extended with RunningTitle/Size/Field/FreeDesc/GenizahTitle (37-01 complete)

## Session Continuity

Last session: 2026-02-20
Stopped at: Completed 39-08-PLAN.md (Page navigation speed - parallel queries)
Resume file: .planning/phases/39-bug-fixing-cleanup-performance-improving/39-08-SUMMARY.md
Notes: Phase 39 complete (8/8 plans). Parallelized search enrichment, batched FJMS browse metadata, async discoveries loading.
