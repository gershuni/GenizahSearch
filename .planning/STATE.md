# Project State

## Project Reference

See: .planning/PROJECT.md (updated 2026-02-22)

**Core value:** Researchers can find what they need in the Genizah corpus
**Current focus:** v6.5.0 Search UX & Filtered Search — Phase 42 next

## Current Position

Milestone: v6.5.0 Search UX & Filtered Search
Phase: 42 of 47 (Search UX & Composition Polish) — Plan 3 of 3 complete
Status: Executing Phase 42
Last activity: 2026-03-01 - Completed 42-03 (printed badge)

Progress: [░░░░░░░░░░] 0/5 phases (Phase 42 plan 3/3 complete)

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
| 42-03 | 1 | 17min | 17min |

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
- 42-03: Printed badge uses FragmentMaterial=Printed from catalog_fields (12,421 AlmaIds). Red attention color, parallel enrichment lookup.

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
Stopped at: Completed 42-03-PLAN.md (printed badge)
Resume file: .planning/phases/42-search-ux-composition-polish/42-03-SUMMARY.md
Notes: Plan 42-03 complete. Printed badge added to all result views (web search, parallels, catalog browse, desktop search, desktop composition). Uses FragmentMaterial=Printed from catalog_fields (12,421 AlmaIds). Next: 42-02 (cancel with partial results) if not yet done.
