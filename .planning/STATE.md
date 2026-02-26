# Project State

## Project Reference

See: .planning/PROJECT.md (updated 2026-02-22)

**Core value:** Researchers can find what they need in the Genizah corpus
**Current focus:** Phase 41 - Catalog Browse & Navigation

## Current Position

Phase: 41 of 46 (Catalog Browse & Navigation)
Plan: 3 of 4 in current phase
Status: Executing
Last activity: 2026-02-26 - Completed 41-03: Desktop Catalog Browse Tab

Progress: [#######░░░] 75%

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

**Recent Trend:**
- v6.0.0: 21 plans, 6 phases, 6 days
- Trend: Stable

*Updated after each plan completion*

## Accumulated Context

### Decisions

See PROJECT.md Key Decisions table for full history.

Recent decisions affecting current work:
- Phase 13 deferred (v5.6.0): Transcription index build too slow for desktop -- revisited in Phase 45-46
- Post-search domain filtering chosen over pre-search (v5.8.0) -- Phase 43 now adds pre-search option
- Phase 41-01: MAX(CASE WHEN) aggregation for browse result dedup; batch domain post-query for performance
- Phase 41-02: Single-pass NiceGUI layout with dict refs; history.replaceState deep linking; unclassified bucket informational only
- Phase 41-03: Merged lazy-load into tab creation; inline get_fjms_service() matching existing desktop pattern; English domain key in UserRole for queries

### Pending Todos

- JA diacritic dots normalization in search
- Migrate desktop corrections fetch to shared corrections_service
- CUT-01: Remove read-only PGP tables from Supabase (legacy desktop users depend on them)

### Blockers/Concerns

- FIST.db access required for Phase 44 (FJMS transcription import) -- confirm file available
- Phase 45 extends Tantivy schema -- need backward-compatible index upgrade strategy
- Phase 43 pre-search filtering must not break parallels search mode

### Quick Tasks Completed

| # | Description | Date | Commit | Directory |
|---|-------------|------|--------|-----------|
| 15 | Move catalog/bib buttons to page nav pane in Browse; fix FJMS button in advanced mode | 2026-02-22 | da8cd4ab | [15-move-catalog-bib-buttons-to-page-nav-pan](./quick/15-move-catalog-bib-buttons-to-page-nav-pan/) |

## Session Continuity

Last session: 2026-02-26
Stopped at: Completed 41-03-PLAN.md
Resume file: None
Notes: Plan 41-03 (desktop catalog browse tab) complete. Next: 41-04 (cross-links).
