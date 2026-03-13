---
gsd_state_version: 1.0
milestone: v5.6
milestone_name: milestone
status: in_progress
stopped_at: Completed 46-06-PLAN.md
last_updated: "2026-03-13T06:35:39.347Z"
last_activity: 2026-03-10 - Completed 46-05, wrapping up v6.5.0
progress:
  total_phases: 9
  completed_phases: 5
  total_plans: 26
  completed_plans: 26
  percent: 100
---

---
gsd_state_version: 1.0
milestone: v6.5.0
milestone_name: Search UX & Filtered Search
status: in_progress
stopped_at: Phase 46 complete (all 5 plans done). Citation popup shipped. Working on personal handlist source mapping.
last_updated: "2026-03-10T18:00:00.000Z"
last_activity: 2026-03-12 - Citation popup done. Personal handlist source mapping for 43K NULL SourceName records.
progress:
  [██████████] 100%
  completed_phases: 5
  total_plans: 25
  completed_plans: 25
  percent: 100
---

# Project State

## Project Reference

See: .planning/PROJECT.md (updated 2026-02-22)

**Core value:** Researchers can find what they need in the Genizah corpus
**Current focus:** v6.5.0 Search UX & Filtered Search — all phases complete, personal handlist source mapping + Round 3 merge pending

## Current Position

Milestone: v6.5.0 Search UX & Filtered Search
Phase: 46 of 46 (Dicta Translation) -- COMPLETE (all 5 plans done)
Status: All phases complete. Citation popup done. Personal handlist mapping + Round 3 merge before release.
Last activity: 2026-03-10 - Completed 46-05, wrapping up v6.5.0

Progress: [██████████] 5/5 phases (42-46 all complete, 25/25 plans)

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
| 46-02 | 1 | 3min | 3min |
| 46-03 | 1 | 5min | 5min |
| 46-04 | 1 | 14min | 14min |

**Recent Trend:**
- v6.0.0: 21 plans, 6 phases, 6 days
- Trend: Stable

*Updated after each plan completion*
| Phase 46 P06 | 2min | 1 tasks | 1 files |

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
- 46-02: PGP batch translation script (scripts/translate_pgp_descriptions.py) with checkpointing, resume, parallel API calls, retry with backoff. 34,954 candidates. Document types via manual PGP_DOCUMENT_TYPE_HE mapping. Atomic checkpoint writes (tempfile+os.replace). 8 new integration tests.
- 46-03: FJMS catalog gap-fill script (6 categories, ~5,546 items) and free description script (~255K items, ~18h). Bibliography scaffold deferred. RunningTitle column (not BibDesc). SIGINT handler, SQLite reconnect every 10K items. Gap-fill only -- never overwrites existing human translations. Rebuilt few-shot with 16 real genizah_titles pairs (JA transliteration), dedup (265→20 unique for titles), sequential+3s throttle, 429 retry in dicta_client. **ALL BATCH TRANSLATIONS COMPLETE (2026-03-07)**: Libraries 184,514 | PGP 34,954 | FJMS catalog 3,830 | FJMS free desc 254,835 | Total: ~478K translations, 0 failures.
- 46-04: Web translation integration: global toggle (show_translations user pref), translated match badge (light blue), clickable Translated/Original toggle badges, sys_id-based translation lookup via document_fragments JOIN (batched 400), 5th parallel enrichment query in asyncio.gather. MyMemory replaced with Dicta API + lazy few-shot singleton. Browse page shelfmark URL param + sys_id detection. 12 new translation UI strings. 10 new tests (35 total).
- 46 (Round 2 batch): Created translate_fjms_catalog_text.py for EN->HE translation of FJMS catalog running titles (107K English) and full texts/scholarly descriptions (46K English). Uses Dicta LM 2.0 with en2he_scholarly few-shot. Running on server 2026-03-08 (~11h total). New field_names: 'RunningTitle', 'FullText' in fjms_translations.
- 46-05 (extraction fix): Semicolon split changed from `\s*;\s*` to ` ; ` (MARC separator). Longest pure-Hebrew part preferred over mixed. 87K records fixed, 58K Hebrew values improved, zero data loss. libraries_translations.db rebuilt.
- 46-05 (search-in-translation): Removed translated-match badges from main search (both web and desktop). Translation search belongs only in browse catalog text filter (FTS5), not in main search results. TranslationService methods retained for future browse integration.
- [Phase 46]: 46-06: Per-record RunningTitle translation via get_fjms_translations_by_signature_ids with inline NiceGUI toggle badges

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

### Roadmap Evolution

- Phase 50 added: Pre-built index distribution with in-app download

### Blockers/Concerns

- FIST.db access required for v7.0.0 Phase 47 (FJMS transcription import) -- confirm file available
- v7.0.0 Phase 48 extends Tantivy schema -- need backward-compatible index upgrade strategy
- Phase 45 bidirectional filtered search must not break parallels search mode
- Dicta Translation (Phase 46) must handle already-bilingual fields carefully

### Quick Tasks Completed

| # | Description | Date | Commit | Directory |
|---|-------------|------|--------|-----------|
| 15 | Move catalog/bib buttons to page nav pane in Browse; fix FJMS button in advanced mode | 2026-02-22 | da8cd4ab | [15-move-catalog-bib-buttons-to-page-nav-pan](./quick/15-move-catalog-bib-buttons-to-page-nav-pan/) |
| 16 | Fix installer: show directory selection on upgrades, update filename to v6.2.0 | 2026-03-10 | ebb7e2f0 | [16-fix-desktop-installer-add-directory-sele](./quick/16-fix-desktop-installer-add-directory-sele/) |
| 17 | Create bump_version.py script, fix version_info.txt (6.1.1->6.2.0), document in CLAUDE.md | 2026-03-10 | 45e6d801 | [17-create-bump-version-py-script-and-fix-ve](./quick/17-create-bump-version-py-script-and-fix-ve/) |

## Session Continuity

Last session: 2026-03-13T06:35:39.344Z
Stopped at: Completed 46-06-PLAN.md
Resume file: None
Notes:
  - All 5 phases (42-46) complete, 25/25 plans done
  - Handlist source fix: 43,233 NULL SourceName records fixed (5 named handlists + preliminary)
  - Site user attribution pending: ~5,800 SourceId=850 records need SubId→user name mapping
  - Round 3 gap-closing IN PROGRESS: 206K rows running on server (ETA ~01:00 UTC 2026-03-13)
  - After Round 3: download results -> merge -> QC -> stats -> upload DB -> deploy -> v6.5.0
  - Full handoff: .planning/phases/46-dicta-translation/.continue-here.md
