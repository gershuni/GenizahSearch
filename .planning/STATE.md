# Project State

## Project Reference

See: .planning/PROJECT.md (updated 2026-02-07)

**Core value:** Users can view and search PGP's human-curated transcriptions alongside manuscript images -- in both web and desktop apps
**Current focus:** v5.6.0 Desktop Parity & Transcription Search

## Current Position

Phase: 11 of 13 (Virtual Reading Desk) -- Phase complete
Plan: 11 of 11 (7 original + 4 UAT gap closure plans; all complete)
Status: Phase complete -- ready for Phase 12
Last activity: 2026-02-08 -- Completed 11-10-PLAN.md (per-manuscript selection in Add from List dialog)

Progress: [██████████████████░░] 100% (18/18 plans across phases 8-11)

## Milestone History

- **v1 External Data Integration** -- Shipped 2026-02-07 (git tag v5.5.0)
  - 9 phases, 18 plans, 173 min execution
  - See: .planning/milestones/v1-ROADMAP.md

## Performance Metrics

**Velocity:**
- Total plans completed: 17 (v5.6.0)
- Average duration: ~6 min
- Total execution time: ~94 min

**By Phase:**

| Phase | Plans | Total | Avg/Plan |
|-------|-------|-------|----------|
| 8. Foundation | 2/2 | ~5 min | ~2.5 min |
| 9. Data Import | 2/2 | ~14 min | ~7 min |
| 10. Desktop PGP Core | 2/2 | ~22 min | ~11 min |
| 11. Virtual Reading Desk | 11/11 | ~57 min | ~5 min |

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
- DEC-11-01-01: Per-image JS state (rdViewers) for responsiveness
- DEC-11-01-02: IntersectionObserver for sync scrolling (not scroll events)
- DEC-11-02-01: Persist reading desk state to app.storage.user for language-switch preservation
- DEC-11-02-02: Header shows 'Document #X' for join-context, 'Reading Desk' for standalone entry
- DEC-11-03-01: mouseReleaseEvent + anchorAt() for QTextEdit link clicks (no anchorClicked signal)
- DEC-11-03-02: Proportional scroll ratio sync between text/image panes (PyQt equivalent of IntersectionObserver)
- DEC-11-03-03: QInputDialog for per-fragment version selection (matches desktop UX patterns)
- DEC-11-08-01: Inline !important for non-buttons, Quasar text-color prop for buttons (Light Mode fix)
- DEC-11-11-01: Create QScrollArea once at reading desk entry, repopulate container on re-render
- DEC-11-11-02: Targeted disconnect(handler) instead of blanket disconnect() for scroll sync
- DEC-11-09-01: Persist language to app.storage.user, restore in create_layout()
- DEC-11-09-02: Set comparison of sys_ids for language-switch vs cross-page navigation detection

### Data State

- documents table: 35,839 records (all PGP documents with full metadata)
- document_sources: 9,364 records (7,664 editions + 1,696 translations)
- document_footnotes: 22,757 records (bibliography/scholarship)
- document_fragments: 36,155 records (with collection/library/URL metadata)

### Pending Todos

- Multi-fragment document view: image controls for each image -- DONE in 11-01

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

### Phase 11 Verification Issues (from 11-05 human testing)

**Web:**
- W1: Add from List dialog shows list names only, not manuscripts inside
- W2: "Back to Page View" button invisible in Light Mode
- W3: Fragment count badge invisible in Dark Mode
- W4: Language switch loses reading desk state
- W5: Missing word wrap in reading desk text pane

**Desktop:**
- D1: Scroll sync broken — scrolling text pane only moves images pane
- D2: Toolbar "Add" button confusing/redundant UX
- D4: "Add to View" button should be right after Go button

**Out of scope (Phase 12):** D3: PGP joins not visible in desktop

## Session Continuity

Last session: 2026-02-08
Stopped at: Completed 11-10-PLAN.md -- Phase 11 complete
Resume file: None
Notes: Phase 11 Virtual Reading Desk fully complete (all 11 plans, including 4 UAT gap closure). Ready for Phase 12.
