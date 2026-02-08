# Project State

## Project Reference

See: .planning/PROJECT.md (updated 2026-02-07)

**Core value:** Users can view and search PGP's human-curated transcriptions alongside manuscript images -- in both web and desktop apps
**Current focus:** v5.6.0 Desktop Parity & Transcription Search

## Current Position

Phase: 9 of 13 (Data Import)
Plan: 1 of 2
Status: In progress
Last activity: 2026-02-08 -- Completed 09-01-PLAN.md

Progress: [███░░░░░░░░░░░░░░░░░] 21% (3/14 plans)

## Milestone History

- **v1 External Data Integration** -- Shipped 2026-02-07 (git tag v5.5.0)
  - 9 phases, 18 plans, 173 min execution
  - See: .planning/milestones/v1-ROADMAP.md

## Performance Metrics

**Velocity:**
- Total plans completed: 3 (v5.6.0)
- Average duration: ~4 min
- Total execution time: ~12 min

**By Phase:**

| Phase | Plans | Total | Avg/Plan |
|-------|-------|-------|----------|
| 8. Foundation | 2/2 | ~5 min | ~2.5 min |
| 9. Data Import | 1/2 | ~7 min | ~7 min |

*Updated after each plan completion*

## Accumulated Context

### Decisions

Decisions are logged in PROJECT.md Key Decisions table.
Recent decisions affecting current work:

- Shared service layer (Option C): Both apps consume same Supabase functions
- Reshape service API during extraction: Fix TODO, clean up naming
- Tantivy boolean workaround: Use text field with raw tokenizer for content_type filter
- DEC-08-01-01: Keep shared/document_service.py API identical during extraction
- DEC-08-02-01: Fixed pre-existing mock chain bug in test (Rule 1 auto-fix)
- DEC-09-01-01: Excluded 177 footnotes with empty doc_relation (NOT NULL constraint)
- DEC-09-01-02: Footnote dedup removes 1,442 duplicates (24,383 -> 22,764 valid)

### Pending Todos

- Multi-fragment document view: add image controls for each image (user feedback during Phase 8 verification, relevant to Phase 11)

### Blockers/Concerns

- Recto/verso section headers stripped during parsing (v1 tech debt)

## Session Continuity

Last session: 2026-02-08
Stopped at: Completed 09-01-PLAN.md
Resume file: None
Notes: Schema migrations and import script created. Next: 09-02 (execute import with migrations + script)
