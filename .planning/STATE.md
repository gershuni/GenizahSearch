# Project State

## Project Reference

See: .planning/PROJECT.md (updated 2026-02-07)

**Core value:** Users can view and search PGP's human-curated transcriptions alongside manuscript images -- in both web and desktop apps
**Current focus:** v5.6.0 Desktop Parity & Transcription Search

## Current Position

Phase: 12 of 12 (Desktop PGP Discovery) -- COMPLETE
Plan: 5 of 5 (all plans including gap closure complete)
Status: Milestone complete (Phase 13 deferred)
Last activity: 2026-02-09 -- Phase 13 (Transcription Search) reverted and deferred

Progress: [████████████████████] 100% (25/25 plans across phases 8-12)

### Phase 13 Deferral

Phase 13 (Transcription Search) was fully implemented (3 plans + 9 bugfixes, 25 commits) but reverted.
**Reason:** Index build too slow for desktop; will revisit with server-side index build architecture.
**Documentation:** docs/archive/PHASE_13_TRANSCRIPTION_SEARCH_DEFERRED.md
**Note:** Lab index (fingerprints) also needs PGP when this is re-implemented.

## Milestone History

- **v1 External Data Integration** -- Shipped 2026-02-07 (git tag v5.5.0)
  - 9 phases, 18 plans, 173 min execution
  - See: .planning/milestones/v1-ROADMAP.md

## Performance Metrics

**Velocity:**
- Total plans completed: 25 (v5.6.0)
- Average duration: ~5 min
- Total execution time: ~134 min

**By Phase:**

| Phase | Plans | Total | Avg/Plan |
|-------|-------|-------|----------|
| 8. Foundation | 2/2 | ~5 min | ~2.5 min |
| 9. Data Import | 2/2 | ~14 min | ~7 min |
| 10. Desktop PGP Core | 2/2 | ~22 min | ~11 min |
| 11. Virtual Reading Desk | 13/13 | ~67 min | ~5 min |
| 12. Desktop PGP Discovery | 5/5 | ~30 min | ~6 min |

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
- DEC-12-02-01: PGP badge column (COL_PGP=9) added after SRC column, 40px fixed width
- DEC-12-02-02: PGP controls on separate row3 in desktop to avoid crowding row2
- DEC-12-02-03: Web PGP text badge styled like library badge (success-100/700 colors)
- DEC-12-02-04: Tag search results converted to search result format for table display
- DEC-12-03-01: Synchronous PGP join lookup in JoinsDialog (2 fast queries, acceptable latency)
- DEC-12-03-02: Null join ID convention for PGP joins (prevents deletion)

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
D3 (PGP joins not visible in desktop) deferred to Phase 12 -- RESOLVED in 12-03.

### Phase 12 Notes (Complete)

Phase 12 completed 3 original plans + 2 gap closure plans:
- 12-01: PGP info in desktop browse/results with tag display and link styling
- 12-02: PGP badges, filters, and tag search in both apps
- 12-03: PGP joins in desktop JoinsDialog
- 12-04: Tag search gap closure (navigation, browse state, ResultDialog crash, Hebrew snippets)
- 12-05: Web filters toggle, PGP joins in dropdowns, Hebrew i18n, Browse enrichment data

## Session Continuity

Last session: 2026-02-09
Stopped at: Reverted Phase 13, milestone v5.6.0 complete (phases 8-12)
Resume file: None
Notes: Phase 13 reverted and deferred. v5.6.0 covers phases 8-12 (desktop parity). Transcription search deferred to future milestone with server-side index architecture.
