# Project State

## Project Reference

See: .planning/PROJECT.md (updated 2026-02-10)

**Core value:** Researchers can find what they need in the Genizah corpus
**Current focus:** Planning next milestone

## Current Position

Phase: None (between milestones)
Plan: N/A
Status: **v5.7.0 shipped. Ready for next milestone.**
Last activity: 2026-02-10 - Completed v5.7.0 Responsa Search milestone

## Milestone History

- **v5.7.0 Responsa Search** -- Shipped 2026-02-10 (git tag v5.7.0)
  - 4 phases (14-17), 14 plans, 221 Responsa tests
  - See: .planning/milestones/v5.7.0-ROADMAP.md

- **v5.6.0 Desktop Parity & PGP Integration** -- Shipped 2026-02-09 (git tag v5.6.0)
  - 5 phases (8-12), 25 plans, ~134 min execution
  - Phase 13 (Transcription Search) deferred
  - See: .planning/milestones/v5.6.0-ROADMAP.md

- **v1 External Data Integration** -- Shipped 2026-02-07 (git tag v5.5.0)
  - 9 phases, 18 plans, 173 min execution
  - See: .planning/milestones/v1-ROADMAP.md

## Accumulated Context

### Decisions

Decisions are logged in PROJECT.md Key Decisions table.

### Data State

- documents table: 35,839 records (all PGP documents with full metadata)
- document_sources: 9,364 records (7,664 editions + 1,696 translations)
- document_footnotes: 22,757 records (bibliography/scholarship)
- document_fragments: 36,155 records (with collection/library/URL metadata)

### Blockers/Concerns

- Recto/verso section headers stripped during parsing (v1 tech debt, not blocking)
- Phase 13 (Transcription Search) deferred -- needs server-side index architecture
- 13 pre-existing test failures (boundary_search, export_service, shelfmark_normalization -- not regressions)
- JA diacritic dots normalization -- todo captured, not yet addressed

### Future Improvements

- **Show user's pending corrections in browse page**: indicator + corrected text display
- **Remove AI search component**: obsoleted by Responsa mode
- **JA diacritic dots normalization**: normalize dots above/below Hebrew letters in search

### Quick Tasks Completed

| # | Description | Date | Commit | Directory |
|---|-------------|------|--------|-----------|
| 8 | Fix web corrections singleton Supabase client bug + improve desktop login errors | 2026-02-10 | 787c236 | [8-fix-web-corrections-singleton-supabase-c](./quick/8-fix-web-corrections-singleton-supabase-c/) |

## Session Continuity

Last session: 2026-02-10
Stopped at: v5.7.0 milestone archived
Resume file: None
Notes: v5.7.0 shipped. Next step: /gsd:new-milestone to define next milestone scope.
