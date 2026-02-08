---
phase: 10-desktop-pgp-core
plan: 01
subsystem: desktop-browse
tags: [pyqt6, qthread, pgp, version-selector, browse-tab]
requires:
  - phase-08 (shared/document_service.py)
  - phase-09 (document_sources data in Supabase)
provides:
  - PGPSourceWorker QThread for background PGP data fetching
  - Shared _populate_pgp_combo and _auto_select_pgp_edition helpers
  - Browse tab PGP integration with grouped version selector
affects:
  - phase-10-plan-02 (ResultDialog PGP integration reuses helpers)
  - phase-12 (metadata panel may reference PGP doc data)
tech-stack:
  added: []
  patterns:
    - PGPSourceWorker QThread with lazy import for Supabase isolation
    - Combo population helpers shared between Browse and ResultDialog
    - Stale-request guard pattern for async PGP loading
    - Corrections preservation on combo rebuild
key-files:
  created: []
  modified:
    - gui_threads.py
    - genizah_app.py
key-decisions:
  - DEC-10-01-01: PGP worker runs after community status check; corrections are saved and re-added when PGP combo rebuilds
  - DEC-10-01-02: Text directionality handled per source -- editions RTL, English translations LTR, V0.8/corrections RTL
  - DEC-10-01-03: Combo width increased from 180px to 240px for both Browse and ResultDialog selectors
duration: ~7 min
completed: 2026-02-08
---

# Phase 10 Plan 01: Browse Tab PGP Integration Summary

PGPSourceWorker QThread in gui_threads.py with lazy document_service import; Browse tab auto-selects PGP editions with grouped version selector (Editions > Translations > V0.8 > Corrections) and RTL/LTR directionality per source type.

## Performance

- Duration: ~7 minutes
- Tasks: 2/2 complete
- Files modified: 2

## Accomplishments

1. **PGPSourceWorker QThread** -- New worker class in gui_threads.py that fetches PGP editions and translations from shared/document_service in a background thread. Uses lazy import to prevent Supabase initialization issues. Emits finished_signal(sys_id, page_sources, pgp_doc) with stale-request guard.

2. **Shared combo population helpers** -- `_populate_pgp_combo` builds grouped combo items matching the web app pattern: disabled header items ("-- PGP Editions --", "-- Translations --"), separators between groups, scholar names as labels, translator name + language for translations. `_auto_select_pgp_edition` finds and selects the first edition item. Both are methods on GenizahApp to be reused by ResultDialog in Plan 02.

3. **Browse tab PGP integration** -- PGP worker starts in `on_browse_enriched_loaded` after community status check. When PGP data arrives, existing corrections are preserved, combo is rebuilt with PGP items + V0.8 + corrections, first edition auto-selected and displayed. Page changes within the same manuscript re-fetch PGP for correct recto/verso content.

4. **Version switching with directionality** -- `_browse_load_version` handles pgp_edition (RTL), pgp_translation (LTR for English, RTL otherwise), and original/correction sources. `_browse_display_pgp_text` sets both layout direction and HTML dir attribute. Switching back to V0.8 or corrections restores RTL.

## Task Commits

| # | Task | Type | Commit | Key Changes |
|---|------|------|--------|-------------|
| 1 | Create PGPSourceWorker and shared combo helpers | feat | 84735ee | PGPSourceWorker in gui_threads.py, _populate_pgp_combo, _auto_select_pgp_edition, combo width 240px |
| 2 | Wire PGP data loading into Browse tab flow | feat | 0c4522f | _on_browse_pgp_loaded, _browse_display_pgp_text, _browse_refresh_pgp_for_page, version switching |

## Files Modified

| File | Changes |
|------|---------|
| `gui_threads.py` | Added PGPSourceWorker class (52 lines) with finished_signal, error_signal, lazy document_service import |
| `genizah_app.py` | Added PGPSourceWorker import, 3 shared helpers, 4 Browse PGP methods, extended _browse_load_version, _browse_change_version, browse_navigate, on_browse_page_combo_changed |

## Decisions Made

| ID | Decision | Rationale |
|----|----------|-----------|
| DEC-10-01-01 | PGP worker runs after community status; corrections saved and re-added on PGP combo rebuild | Avoids duplicate network calls for corrections; lightweight approach with brief V0.8 display before PGP loads |
| DEC-10-01-02 | Per-source directionality (editions RTL, English translations LTR) | Matches web app behavior; English text is unreadable in RTL mode |
| DEC-10-01-03 | Combo width 240px for both selectors | Scholar names like "Goitein, S.D." + language need more space than "V0.8" |

## Deviations from Plan

None -- plan executed exactly as written.

## Issues Encountered

None.

## Next Phase Readiness

- Plan 02 (ResultDialog PGP integration) can proceed immediately -- the shared helpers (_populate_pgp_combo, _auto_select_pgp_edition) and PGPSourceWorker are ready for reuse
- ResultDialog combo width already updated to 240px

## Self-Check: PASSED
