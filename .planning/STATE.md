# Project State

## Project Reference

See: .planning/PROJECT.md (updated 2026-02-07)

**Core value:** Users can view and search PGP's human-curated transcriptions alongside manuscript images -- in both web and desktop apps
**Current focus:** v5.6.0 Desktop Parity & Transcription Search

## Current Position

Phase: 12 of 13 (Desktop PGP Discovery)
Plan: 1 of 3
Status: In progress
Last activity: 2026-02-08 -- Completed 12-01-PLAN.md (PGP Extended Info)

Progress: [█████████████████████] 81% (21/26 plans across phases 8-13)

## Milestone History

- **v1 External Data Integration** -- Shipped 2026-02-07 (git tag v5.5.0)
  - 9 phases, 18 plans, 173 min execution
  - See: .planning/milestones/v1-ROADMAP.md

## Performance Metrics

**Velocity:**
- Total plans completed: 21 (v5.6.0)
- Average duration: ~5 min
- Total execution time: ~112 min

**By Phase:**

| Phase | Plans | Total | Avg/Plan |
|-------|-------|-------|----------|
| 8. Foundation | 2/2 | ~5 min | ~2.5 min |
| 9. Data Import | 2/2 | ~14 min | ~7 min |
| 10. Desktop PGP Core | 2/2 | ~22 min | ~11 min |
| 11. Virtual Reading Desk | 13/13 | ~67 min | ~5 min |
| 12. Desktop PGP Discovery | 1/3 | ~8 min | ~8 min |

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
- DEC-12-01-01: PGP section uses green left border (#27ae60) distinct from Oxford blue (#3498db)
- DEC-12-01-02: Tag clicks use exact search as interim; _pending_tag_search flag for Plan 12-02
- DEC-12-01-03: Three-case race condition handling in ResultDialog (enriched first, PGP first, PGP-only)

### Data State

- documents table: 35,839 records (all PGP documents with full metadata)
- document_sources: 9,364 records (7,664 editions + 1,696 translations)
- document_footnotes: 22,757 records (bibliography/scholarship)
- document_fragments: 36,155 records (with collection/library/URL metadata)

### Pending Todos

- Multi-fragment document view: image controls for each image -- DONE in 11-01

### Blockers/Concerns

- Recto/verso section headers stripped during parsing (v1 tech debt)

### Phase 11 Notes (Complete)

Phase 11 required 3 iterations of gap closure (13 total plans). Key lessons preserved in MEMORY.md.
D3 (PGP joins not visible in desktop) deferred to Phase 12.

## Session Continuity

Last session: 2026-02-08
Stopped at: Completed 12-01-PLAN.md (PGP Extended Info in Browse + ResultDialog)
Resume file: None
Notes: Plan 12-01 complete. PGP metadata section added to both Browse tab and ResultDialog extended info. Tag click navigation to Search tab wired. Ready for Plan 12-02 (Search Result Indicators + Tag Search + Filter).
