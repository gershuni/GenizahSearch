---
gsd_state_version: 1.0
milestone: v5.6
milestone_name: milestone
status: unknown
stopped_at: Completed 45-05-PLAN.md (Browse-to-search navigation & Hebrew translations)
last_updated: "2026-03-04T05:03:37.567Z"
last_activity: 2026-03-03 - Completed 45-05 (Browse-to-search navigation & Hebrew translations)
progress:
  total_phases: 8
  completed_phases: 4
  total_plans: 20
  completed_plans: 20
---

---
gsd_state_version: 1.0
milestone: v5.6
milestone_name: milestone
status: unknown
last_updated: "2026-03-02T12:00:00.000Z"
progress:
  total_phases: 46
  completed_phases: 44
  total_plans: 150
  completed_plans: 150
---

# Project State

## Project Reference

See: .planning/PROJECT.md (updated 2026-02-22)

**Core value:** Researchers can find what they need in the Genizah corpus
**Current focus:** v6.5.0 Search UX & Filtered Search — Phase 46 in progress (1/5 plans done)

## Current Position

Milestone: v6.5.0 Search UX & Filtered Search
Phase: 46 of 47 (Dicta Translation) -- Plan 1 of 5 complete
Status: Phase 46 In Progress
Last activity: 2026-03-04 - Completed 46-01 (Dicta API client, few-shot templates, TranslationService)

Progress: [#####░░░░░] 5/5 phases (Phase 42 complete, Phase 43 complete, Phase 44 complete, Phase 45 complete, Phase 46 in progress)

## Performance Metrics

**Velocity:**
- Total plans completed: ~115 (across 8 milestones)
- Average duration: ~12 min (historical)
- Total execution time: ~23 hours (historical)

**By Phase:**

| Phase | Plans | Total | Avg/Plan |
|-------|-------|-------|----------|
| 41-01 | 1 | 4min | 4min |
| 41-02 | 1 | 6min | 6min |
| 41-03 | 1 | 9min | 9min |
| 41-04 | 1 | 45min | 45min |
| 42-01 | 1 | 7min | 7min |
| 42-02 | 1 | 22min | 22min |
| 42-03 | 1 | 17min | 17min |
| 42-04 | 1 | 7min | 7min |
| 42-05 | 1 | 8min | 8min |
| 42-06 | 1 | 4min | 4min |
| 42-07 | 1 | 3min | 3min |
| 42-08 | 1 | 4min | 4min |
| 42-09 | 1 | 2min | 2min |
| 43-01 | 1 | 7min | 7min |
| 43-02 | 1 | 4min | 4min |
| 43-04 | 1 | 8min | 8min |
| 44-01 | 1 | 4min | 4min |
| 44-02 | 1 | 5min | 5min |
| 45-01 | 1 | 5min | 5min |
| 45-02 | 1 | 34min | 34min |
| 45-03 | 1 | 32min | 32min |
| 45-04 | 1 | 9min | 9min |
| 45-05 | 1 | 9min | 9min |
| 46-01 | 1 | 11min | 11min |

**Recent Trend:**
- v6.0.0: 21 plans, 6 phases, 6 days
- Trend: Stable

*Updated after each plan completion*

## Accumulated Context

### Decisions

See PROJECT.md Key Decisions table for full history.

Recent decisions affecting current work:
- Phase 13 deferred (v5.6.0): Transcription index build too slow for desktop -- revisited in v7.0.0 Phase 47-49
- Post-search domain filtering chosen over pre-search (v5.8.0) -- Phase 45 now adds bidirectional filtered search
- v6.5.0 scoped (2026-03-01): UX first (42-44), then filtered search (45), then Dicta translation (46). Transcription deferred to v7.0.0
- CreationType badge added to Phase 42 scope (print vs manuscript visibility)
- 42-01: Elapsed timer + ETA pattern: time.time() on state, 2s smoothing for ETA. Summary persists until next search.
- 42-02: Cancel with partial results: InterruptedError catch in chunk loop, cancel_flag on threads, collapsible excluded sections, filter_reason annotation.
- 42-03: Printed badge uses FragmentMaterial=Printed from catalog_fields (12,421 AlmaIds). Red attention color, parallel enrichment lookup.
- 42-04: comp_summary_text persists across display_comp_results resets. Dedicated comp_col_printed column. progress_callback every chunk for cancel responsiveness.
- 42-05: 3-state printed filter toggle (all/hide/only) layered on domain exclusions. 16 Hebrew translations for Phase 42 strings. Excluded section overflow fixed.
- 42-06: SearchThread cancel_flag (safe cancel), progress_callback every 5 hits, excluded section reason sub-headers, Printed column Fixed 55px and filterable.
- 42-07: Clickable excluded items in web, "Filter Printed" label, desktop 3-state printed filter on search results, 5 missing Hebrew translation keys.
- 42-08: Bare 'Searching' translation key for desktop status, 'Found in source text'/'High frequency' excluded reason sub-header translations, partial results statusbar notification on cancel.
- 42-09: Composition tree 3-state printed filter (matching regular search), web cancel skips enrichment, lab mode InterruptedError propagation.
- 43-01: JSON session persistence with atomic writes (tempfile+os.replace). 500ms debounced saves via QTimer. 200ms deferred restore after startup. Results capped at 5K. excluded_raw_entries persisted for full restore.
- 43-02: _persist() helper gates new storage writes behind session_persistence_enabled setting. Parallels has no printed_filter toggle, only badges.
- 43-04: Search history deduplicates by query+mode, composition by title. History entries store results capped at 500. Lazy menu refresh on button click.
- 44-01: Sleep prevention via SetThreadExecutionState in 4 search threads with try/finally. Toast notification via QSystemTrayIcon when unfocused. Copy context menu (Shelfmark/Title/Library/SysID/Row).
- 44-02: LIBRARY_CODES_HE (81 entries) with lang param on get_library_display(). Desktop uses CURRENT_LANG auto. Web callers pass get_language() explicitly. Fallback: HE->EN->code.
- 45-01: get_filter_sys_ids() returns None (no filters) or set of matching sys_ids. restrict_sys_ids param on execute_search/search_composition_logic skips manuscripts BEFORE regex.search(). Material filters via catalog_fields subquery.
- 45-02: Collapsible Advanced Filters panel (domain/author/work/date/material) on web search page. Removable chip bar with manuscript count. restrict_sys_ids wired to execute_search. Word search per-result exclusion. incoming_filters consumption for Path B. Filter-aware search history. from_browse URL param on route.
- 45-03: PreSearchFilterDialog with domain/author/work/date/material. FilterCountWorker for async count. Chip bar on both tabs. SearchThread/CompositionThread pass restrict_sys_ids. Per-result word search exclusion. Filter state in session persistence.
- 45-04: Collapsible Advanced Filters panel on web parallels page (domain/author/work/date/material). Removable chip bar with manuscript count. restrict_sys_ids wired to search_composition_logic. Per-manuscript exclude buttons on each group. Auto-exclude source manuscript. Import exclusions from word search. Filter-aware composition history. Excluded manuscripts in separate collapsible section.
- 45-05: Browse-to-search buttons on web and desktop catalog browse. Web: incoming_filters via app.storage.user + /search?from_browse=1 or /parallels. Desktop: pre_search_filters + restrict_sys_ids + tab switch. 20 Hebrew translations for Phase 45 strings.
- 46-01: Dicta API client (shared/dicta_client.py) with translate_text, build_few_shot_prompt, batch_translate, PGP_DOCUMENT_TYPE_HE. TranslationService (shared/translation_service.py) with sidecar queries. Scholarly few-shots validated on 20 samples vs defaults -- scholarly adopted for domain consistency. Schema helpers for pgp_translations/fjms_translations.

### Pending Todos

- JA diacritic dots normalization in search
- Migrate desktop corrections fetch to shared corrections_service
- CUT-01: Remove read-only PGP tables from Supabase (legacy desktop users depend on them)
- Date range filter using CopyToDate (21K rows) — show "from-to" date display
- Creation type filter via code_values (CreationTypeCode, 69K rows) — Original/Copy/Commentary/Tafsir
- Display scholarly Comment (100K rows) and Colophon (789 rows) in expanded detail rows
- Script/vocalization/cantillation filters for paleography researchers
- Copyist name browse axis (CopyName, 1.6K rows)
- OrgCreation/OrgAuthor cross-refs for commentary identification display

### Blockers/Concerns

- FIST.db access required for v7.0.0 Phase 47 (FJMS transcription import) -- confirm file available
- v7.0.0 Phase 48 extends Tantivy schema -- need backward-compatible index upgrade strategy
- Phase 45 bidirectional filtered search must not break parallels search mode
- Dicta Translation (Phase 46) must handle already-bilingual fields carefully

### Quick Tasks Completed

| # | Description | Date | Commit | Directory |
|---|-------------|------|--------|-----------|
| 15 | Move catalog/bib buttons to page nav pane in Browse; fix FJMS button in advanced mode | 2026-02-22 | da8cd4ab | [15-move-catalog-bib-buttons-to-page-nav-pan](./quick/15-move-catalog-bib-buttons-to-page-nav-pan/) |

## Session Continuity

Last session: 2026-03-04
Stopped at: Completed 46-01-PLAN.md (Dicta API client, few-shot templates, TranslationService)
Resume file: .planning/phases/46-dicta-translation/46-01-SUMMARY.md
Notes: Phase 46 plan 1 of 5 complete. API client, service layer, few-shot templates validated. Next: 46-02 (PGP batch translation).
