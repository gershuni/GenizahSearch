# Project State

## Project Reference

See: .planning/PROJECT.md (updated 2026-02-22)

**Core value:** Researchers can find what they need in the Genizah corpus
**Current focus:** v6.5.0 Search UX & Filtered Search — Phase 42 complete (9/9 plans), Phase 43 next

## Current Position

Milestone: v6.5.0 Search UX & Filtered Search
Phase: 42 of 47 (Search UX & Composition Polish) -- Plan 8 of 9 complete (42-08 pending)
Status: Phase 42 in gap closure
Last activity: 2026-03-01 - Completed 42-09 (comp 3-state printed filter, web cancel enrichment skip, lab mode except fix)

Progress: [##░░░░░░░░] 1/5 phases (Phase 42: 8/9 plans, 42-08 pending)

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
| 42-09 | 1 | 2min | 2min |

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
- 42-09: Composition tree 3-state printed filter (matching regular search), web cancel skips enrichment, lab mode InterruptedError propagation.

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

Last session: 2026-03-01
Stopped at: Completed 42-09-PLAN.md (comp 3-state printed filter, web cancel enrichment skip, lab mode except fix)
Resume file: .planning/phases/42-search-ux-composition-polish/42-09-SUMMARY.md
Notes: 8 of 9 Phase 42 plans complete (42-01 through 42-07, 42-09). Plan 42-08 (Hebrew translations + desktop notification) still pending. After 42-08: Phase 43.
