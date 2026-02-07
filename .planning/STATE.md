# Project State

## Project Reference

See: .planning/PROJECT.md (updated 2026-02-07)

**Core value:** Users can view and search PGP's human-curated transcriptions alongside manuscript images -- in both web and desktop apps
**Current focus:** v5.6.0 Desktop Parity & Transcription Search

## Current Position

Phase: 8 of 13 (Foundation)
Plan: 1 of 2
Status: In progress
Last activity: 2026-02-08 -- Completed 08-01-PLAN.md

Progress: [█░░░░░░░░░░░░░░░░░░░] 7% (1/14 plans)

## Milestone History

- **v1 External Data Integration** -- Shipped 2026-02-07 (git tag v5.5.0)
  - 9 phases, 18 plans, 173 min execution
  - See: .planning/milestones/v1-ROADMAP.md

## Performance Metrics

**Velocity:**
- Total plans completed: 1 (v5.6.0)
- Average duration: ~2 min
- Total execution time: ~2 min

**By Phase:**

| Phase | Plans | Total | Avg/Plan |
|-------|-------|-------|----------|
| 8. Foundation | 1/2 | ~2 min | ~2 min |

*Updated after each plan completion*

## Accumulated Context

### Decisions

Decisions are logged in PROJECT.md Key Decisions table.
Recent decisions affecting current work:

- Shared service layer (Option C): Both apps consume same Supabase functions
- Reshape service API during extraction: Fix TODO, clean up naming
- Tantivy boolean workaround: Use text field with raw tokenizer for content_type filter
- DEC-08-01-01: Keep shared/document_service.py API identical during extraction (zero-risk migration)

### Pending Todos

None yet.

### Blockers/Concerns

- Recto/verso section headers stripped during parsing (v1 tech debt) -- fix opportunistically in Phase 8
- No integration tests for E2E flows (v1 tech debt) -- add smoke tests in Phase 8
- Pre-existing test failure: `test_get_document_for_fragment_not_found` in tests/test_document_service.py

## Session Continuity

Last session: 2026-02-08
Stopped at: Completed 08-01-PLAN.md
Resume file: None
Notes: Next step is execute 08-02-PLAN.md (web re-export shim, test updates, smoke tests)
