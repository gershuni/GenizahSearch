# Project State

## Project Reference

See: .planning/PROJECT.md (updated 2026-02-07)

**Core value:** Users can view and search PGP's human-curated transcriptions alongside manuscript images -- in both web and desktop apps
**Current focus:** v5.6.0 Desktop Parity & Transcription Search

## Current Position

Phase: 10 of 13 (Desktop PGP Core)
Plan: 1 of 2
Status: In progress
Last activity: 2026-02-08 -- Completed 10-01-PLAN.md

Progress: [█████░░░░░░░░░░░░░░░] 36% (5/14 plans)

## Milestone History

- **v1 External Data Integration** -- Shipped 2026-02-07 (git tag v5.5.0)
  - 9 phases, 18 plans, 173 min execution
  - See: .planning/milestones/v1-ROADMAP.md

## Performance Metrics

**Velocity:**
- Total plans completed: 5 (v5.6.0)
- Average duration: ~5 min
- Total execution time: ~26 min

**By Phase:**

| Phase | Plans | Total | Avg/Plan |
|-------|-------|-------|----------|
| 8. Foundation | 2/2 | ~5 min | ~2.5 min |
| 9. Data Import | 2/2 | ~14 min | ~7 min |
| 10. Desktop PGP Core | 1/2 | ~7 min | ~7 min |

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
- DEC-09-02-01: Filter orphan pgpids before footnote/fragment upsert (28 footnote + 6 fragment pgpids reference deleted PGP documents)
- DEC-10-01-01: PGP worker runs after community status; corrections saved and re-added on PGP combo rebuild
- DEC-10-01-02: Per-source directionality (editions RTL, English translations LTR)
- DEC-10-01-03: Combo width 240px for both selectors

### Data State

- documents table: 35,839 records (all PGP documents with full metadata)
- document_sources: 9,364 records (7,664 editions + 1,696 translations)
- document_footnotes: 22,757 records (bibliography/scholarship)
- document_fragments: 36,155 records (with collection/library/URL metadata)

### Pending Todos

- Multi-fragment document view: add image controls for each image (user feedback during Phase 8 verification, relevant to Phase 11)

### Blockers/Concerns

- Recto/verso section headers stripped during parsing (v1 tech debt)

## Session Continuity

Last session: 2026-02-08
Stopped at: Completed 10-01-PLAN.md
Resume file: None
Notes: Browse tab PGP integration complete. PGPSourceWorker and shared helpers ready for Plan 02 (ResultDialog). Next: Phase 10 Plan 02 (ResultDialog PGP Integration)
