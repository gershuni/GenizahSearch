# Project State

## Project Reference

See: .planning/PROJECT.md (updated 2026-02-07)

**Core value:** Users can view and search PGP's human-curated transcriptions alongside manuscript images -- in both web and desktop apps
**Current focus:** v5.6.0 Desktop Parity & Transcription Search

## Current Position

Phase: 11 of 13 (Virtual Reading Desk)
Plan: 0 of TBD (replanning)
Status: Replanning -- first attempt reverted after user feedback
Last activity: 2026-02-08 -- Reverted Phase 11 execution, replanning

Progress: [███████░░░░░░░░░░░░░] 50% (7/14 plans)

## Milestone History

- **v1 External Data Integration** -- Shipped 2026-02-07 (git tag v5.5.0)
  - 9 phases, 18 plans, 173 min execution
  - See: .planning/milestones/v1-ROADMAP.md

## Performance Metrics

**Velocity:**
- Total plans completed: 7 (v5.6.0)
- Average duration: ~7 min
- Total execution time: ~48 min

**By Phase:**

| Phase | Plans | Total | Avg/Plan |
|-------|-------|-------|----------|
| 8. Foundation | 2/2 | ~5 min | ~2.5 min |
| 9. Data Import | 2/2 | ~14 min | ~7 min |
| 10. Desktop PGP Core | 2/2 | ~22 min | ~11 min |

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

### Data State

- documents table: 35,839 records (all PGP documents with full metadata)
- document_sources: 9,364 records (7,664 editions + 1,696 translations)
- document_footnotes: 22,757 records (bibliography/scholarship)
- document_fragments: 36,155 records (with collection/library/URL metadata)

### Pending Todos

- Multi-fragment document view: add image controls for each image (user feedback during Phase 8 verification, relevant to Phase 11)

### Blockers/Concerns

- Recto/verso section headers stripped during parsing (v1 tech debt)

### Phase 11 Replan Context (CRITICAL -- read before replanning)

First attempt (reverted commit 7230bd3) created a separate /reading-desk page (web) and ReadingDeskDialog (desktop). User rejected this approach during verification:

**Bugs found:**
- Web: Only showed one black image box, no text, no multiple manuscripts
- Web: Add by shelfmark broken
- Desktop: Didn't fetch PGP joins, only showed one manuscript

**Architecture pivot (user direction):**
- Do NOT create a separate page/dialog for reading desk
- ENHANCE the existing "View All Fragments" inline view in the Browse tab (both apps)
- Add ability to add more manuscripts by search/shelfmark to the existing joined view
- Add ability to add from personal lists
- Desktop: integrate with existing lists panel in browse tab (direct "add to view")
- The reading desk IS the browse tab's multi-fragment view, enhanced with add/remove
- Keep shared ReadingDeskEntry/ReadingDeskState model concept for data containers

## Session Continuity

Last session: 2026-02-08
Stopped at: Reverted Phase 11, needs replanning with new approach
Resume file: None
Notes: Phase 11 first attempt reverted. User wants enhanced browse tab approach, not separate pages/dialogs. Run /gsd:plan-phase 11 to replan.
