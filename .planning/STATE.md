# Project State

## Project Reference

See: .planning/PROJECT.md (updated 2026-02-07)

**Core value:** Users can view and search PGP's human-curated transcriptions alongside manuscript images -- in both web and desktop apps
**Current focus:** v5.6.0 Desktop Parity & Transcription Search

## Current Position

Phase: 11 of 13 (Virtual Reading Desk)
Plan: 2 of 2
Status: In progress (checkpoint: human-verify)
Last activity: 2026-02-08 -- 11-02 Task 1 complete, awaiting verification

Progress: [████████░░░░░░░░░░░░] 57% (8/14 plans)

## Milestone History

- **v1 External Data Integration** -- Shipped 2026-02-07 (git tag v5.5.0)
  - 9 phases, 18 plans, 173 min execution
  - See: .planning/milestones/v1-ROADMAP.md

## Performance Metrics

**Velocity:**
- Total plans completed: 8 (v5.6.0)
- Average duration: ~7 min
- Total execution time: ~52 min

**By Phase:**

| Phase | Plans | Total | Avg/Plan |
|-------|-------|-------|----------|
| 8. Foundation | 2/2 | ~5 min | ~2.5 min |
| 9. Data Import | 2/2 | ~14 min | ~7 min |
| 10. Desktop PGP Core | 2/2 | ~22 min | ~11 min |
| 11. Virtual Reading Desk | 1/2 | ~4 min | ~4 min |

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
- DEC-10-02-01: Replace insertSeparator with disabled text dividers for visibility
- DEC-10-02-02: Disconnect old PGP worker signals before creating new workers
- DEC-10-02-03: Exclude PGP sources from corrections filter during combo rebuild
- DEC-11-01-01: Use teal color scheme for reading desk to distinguish from browse (green)

### Data State

- documents table: 35,839 records (all PGP documents with full metadata)
- document_sources: 9,364 records (7,664 editions + 1,696 translations)
- document_footnotes: 22,757 records (bibliography/scholarship)
- document_fragments: 36,155 records (with collection/library/URL metadata)

### Pending Todos

- Multi-fragment document view: add image controls for each image -- DONE (Phase 11-01 adds per-image zoom/rotate controls)

### Blockers/Concerns

- Recto/verso section headers stripped during parsing (v1 tech debt)

## Session Continuity

Last session: 2026-02-08
Stopped at: 11-02 Task 1 complete, checkpoint pending
Resume file: None
Notes: Desktop ReadingDeskDialog and ReadingDeskWorker created. Entry points wired from Browse tab, JoinsDialog, and Lists context menu. Awaiting human verification of both web and desktop reading desk features.
