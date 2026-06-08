---
phase: 109-visual-similarity-merge-soft-retire
plan: "10"
subsystem: desktop/ui
tags: [visual-similarity, soft-retire, browse, result-dialog, join-workbench, tdd]

# Dependency graph
requires:
  - phase: "109-08"
    provides: "i18n keys (visual similarity etc.) pre-seeded"
  - phase: "109-09"
    provides: "eye badge + visibly-ON toggle in desktop/join_workbench.py"
provides:
  - "btn_b_visual_sim (Browse-tab) removed from genizah_app.py with zero dangling references"
  - "btn_rd_visual_sim (ResultDialog) removed from desktop/result_dialog.py with zero dangling references"
  - "_browse_view_visual_similarity retained with D-11 removable marker (Phase 109 G-07)"
  - "_rd_search_visual_similarity retained with D-11 removable marker (Phase 109 G-07)"
  - "open_joins_workbench(source='visual') stays defined (dormant, no live caller)"
  - "test_browse_resultdialog_vs_buttons_removed static scan (both VS buttons gone, both Find-Joins present)"
affects:
  - "Future cleanup phase: physical deletion of btn_b_visual_sim/btn_rd_visual_sim reroute handlers"
  - "109-HUMAN-UAT.md parity sign-off gates the actual handler deletion"

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "D-11 one-cycle soft-retire: delete widget + wiring, retain handler with removable-marker comment"
    - "Static source scan test (headless, no Qt) for structural widget removal assertions"
    - "TDD RED gate committed before GREEN implementation (separate commits)"

key-files:
  created:
    - "tests/test_join_workbench_vs.py — extended with test_browse_resultdialog_vs_buttons_removed"
  modified:
    - "genizah_app.py — btn_b_visual_sim construction + visibility wiring deleted; _browse_view_visual_similarity marked removable"
    - "desktop/result_dialog.py — btn_rd_visual_sim construction + visibility wiring deleted; _rd_search_visual_similarity marked removable"

key-decisions:
  - "Delete buttons entirely (widget + connect + addWidget + visibility wiring), NOT just hide them — avoids NameError risk and is the cleanest removal (G-07)"
  - "Retain handlers one cycle with D-11 removable marker — safety net while UAT (109-HUMAN-UAT.md) is still pending"
  - "open_joins_workbench(source='visual') stays defined dormant — still referenced by the marked-removable handlers; no live callers"
  - "Static scan test (headless) asserts both VS buttons gone AND both Find-Joins buttons survive — single authoritative gate"

patterns-established:
  - "Soft-retire pattern: delete widget, retain handler with marker, test with static scan"

requirements-completed: [JWB-12]

# Metrics
duration: ~10min
completed: "2026-06-08"
---

# Phase 109 Plan 10: Remove Duplicate VS Buttons (G-07) Summary

**Duplicate Visual-Similarity buttons (Browse + ResultDialog) deleted with zero dangling references; Find-Joins is now the single Workbench entry; reroute handlers retained one cycle with D-11 removable markers.**

## Performance

- **Duration:** ~10 min
- **Started:** 2026-06-08
- **Completed:** 2026-06-08
- **Tasks:** 2 (Task 1 auto + Task 2 TDD)
- **Files modified:** 3

## Accomplishments

- `btn_b_visual_sim` (Browse tab, `genizah_app.py`) fully deleted: construction block (8 lines), visibility/enable wiring (5 lines including `_vs_has` computation). Zero references remain.
- `btn_rd_visual_sim` (ResultDialog, `desktop/result_dialog.py`) fully deleted: construction block (8 lines), visibility wiring (5 lines including `_parent`/`_vs_has_rd` computation). Zero references remain.
- `btn_b_find_joins` and `btn_rd_find_joins` untouched — "Find Joins" is the surviving single entry into the Workbench.
- `_browse_view_visual_similarity` and `_rd_search_visual_similarity` retained with D-11 removable-marker comment blocks (Phase 109 G-07). No live caller, safety net for one cycle.
- `open_joins_workbench(source="visual")` stays defined (dormant, referenced by retained handlers).
- TDD gate: RED test (`test_browse_resultdialog_vs_buttons_removed`) committed before GREEN; confirms both VS buttons gone and both Find-Joins buttons survive.

## Task Commits

| Task | Name | Commit | Files |
|------|------|--------|-------|
| 1 | Remove btn_b_visual_sim + mark handler removable | `62a228b9` | genizah_app.py |
| 2 (RED) | Add failing test_browse_resultdialog_vs_buttons_removed | `dc76e9bd` | tests/test_join_workbench_vs.py |
| 2 (GREEN) | Remove btn_rd_visual_sim + mark handler removable | `c852d45a` | desktop/result_dialog.py |

## Files Created/Modified

- `genizah_app.py` — btn_b_visual_sim construction + visibility wiring deleted; `_browse_view_visual_similarity` marked DEPRECATED/removable (Phase 109 G-07, D-11)
- `desktop/result_dialog.py` — btn_rd_visual_sim construction + visibility wiring deleted; `_rd_search_visual_similarity` marked DEPRECATED/removable (Phase 109 G-07, D-11)
- `tests/test_join_workbench_vs.py` — `test_browse_resultdialog_vs_buttons_removed` added (Plan 10 G-07 gate); 27 tests pass

## Decisions Made

- Deleted `_vs_has` computation (genizah_app.py) and `_parent`/`_vs_has_rd` computation (result_dialog.py) after confirming via grep that these variables fed ONLY the deleted buttons and were not used elsewhere in the same block.
- Did NOT do a file-wide find/replace on `_parent` — that name is reused in other methods (e.g., result_dialog.py:2086) and the Codex caution was respected: deleted only the block adjacent to `_vs_has_rd`/`btn_rd_visual_sim.setVisible`.

## Deviations from Plan

None — plan executed exactly as written.

## Issues Encountered

None.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

- SC#2 (soft-retire layer, G-07) complete: both normal-mode VS affordances removed, "Find Joins" is the single Workbench entry.
- The retained handlers (`_browse_view_visual_similarity`, `_rd_search_visual_similarity`) can be physically deleted after `109-HUMAN-UAT.md` parity sign-off.
- Phase 109 gap-closure plans G-08 through G-13 still pending per ROADMAP.

---
*Phase: 109-visual-similarity-merge-soft-retire*
*Completed: 2026-06-08*
