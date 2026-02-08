# Project State

## Project Reference

See: .planning/PROJECT.md (updated 2026-02-07)

**Core value:** Users can view and search PGP's human-curated transcriptions alongside manuscript images -- in both web and desktop apps
**Current focus:** v5.6.0 Desktop Parity & Transcription Search

## Current Position

Phase: 11 of 13 (Virtual Reading Desk)
Plan: 0 of TBD (replanning v3)
Status: Replanning -- second attempt reverted after user feedback on UX vision
Last activity: 2026-02-08 -- Reverted Phase 11 v2 execution, replanning with synchronized dual-pane vision

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
- DEC-08-01-01: Keep shared/document_service.py API identical during extraction
- DEC-09-02-01: Filter orphan pgpids before footnote/fragment upsert
- DEC-10-01-01: PGP worker runs after community status
- DEC-10-01-02: Per-source directionality (editions RTL, English translations LTR)
- DEC-10-02-01: Replace insertSeparator with disabled text dividers for visibility

### Data State

- documents table: 35,839 records (all PGP documents with full metadata)
- document_sources: 9,364 records (7,664 editions + 1,696 translations)
- document_footnotes: 22,757 records (bibliography/scholarship)
- document_fragments: 36,155 records (with collection/library/URL metadata)

### Pending Todos

- Multi-fragment document view: add image controls for each image (relevant to Phase 11)

### Blockers/Concerns

- Recto/verso section headers stripped during parsing (v1 tech debt)

### Phase 11 Replan Context (CRITICAL -- read before replanning)

**Two prior attempts reverted.** Key lessons from each:

**Attempt 1** (reverted 7230bd3): Separate /reading-desk page (web) + ReadingDeskDialog (desktop). Rejected -- user wants enhancement of existing browse tab, not new pages/dialogs.

**Attempt 2** (reverted 8012e03): Enhanced existing browse tab joined view with add/remove. Closer but still wrong UX:

**User feedback on attempt 2 (web):**
- Image controls missing rotate -- must reuse EXISTING image controls (zoom/rotate per image)
- No text version selector per fragment (PGP edition/translation chooser)
- Fragment titles should be links to view that fragment individually (exit joined view, navigate there)
- "Add by sys_id" is redundant -- remove it, shelfmark is enough
- Need proper "Add from personal list" dialog (not just a button)
- Header above "Document #..." is irrelevant in All Fragments view (relates to only one fragment)
- Switching language exits the view -- must preserve view state
- No entry point to add manuscripts if no join link exists -- need standalone entry point

**User feedback on attempt 2 (desktop):**
- "Reading Desk" button in nav bar is not discoverable enough
- Entry point should be "Add to View" button near the main shelfmark/Go field
- Lists should have hoverable "+" (add to view) icons on each item
- View MUST show images alongside text in BOTH panes -- synchronized scrolling
- Each image in front of each text, scrollable, lazy loaded, correlated side-by-side

**THE KEY ARCHITECTURAL INSIGHT (v3 vision):**
- BOTH panes (image viewer + text area) become scrollable multi-manuscript views
- All manuscripts' images stacked in the image pane (with lazy loading, zoom/rotate per image)
- All manuscripts' texts stacked in the text pane (with per-fragment version selector)
- **SYNCHRONIZED SCROLLING**: When you scroll past a fragment boundary in one pane, the other pane auto-scrolls to the matching fragment header
- Version selector and metadata update to reflect the currently-visible fragment
- This applies to BOTH web and desktop apps

**Entry points (both apps):**
- "Add to View" button near Go/shelfmark field -- starts reading desk with that manuscript
- "+" icons on list items -- adds to reading desk without navigating
- Joins "View All" -- existing entry point, enters reading desk with all joined fragments
- Shelfmark input in toolbar to add more while in reading desk mode

## Session Continuity

Last session: 2026-02-08
Stopped at: Reverted Phase 11 v2, needs replanning with synchronized dual-pane vision
Resume file: None
Notes: Third attempt at Phase 11. Core insight: synchronized scrolling between image and text panes, not just enhanced text view. Run /gsd:plan-phase 11 to replan.
