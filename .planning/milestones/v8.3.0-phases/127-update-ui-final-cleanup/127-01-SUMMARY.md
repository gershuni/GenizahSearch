---
phase: 127-update-ui-final-cleanup
plan: 01
subsystem: testing
tags: [ast, pytest, guard, desktop, genizah_core, facade, pyqt6, sidecar]

# Dependency graph
requires:
  - phase: 126-desktop-panels
    provides: MOVE-and-shim recipe proven for desktop/*.py; D1 guard files as templates
  - phase: 125-search-engine-extraction
    provides: shared.search_engine + shared.lab_engine + shared.lab_settings facades confirmed
provides:
  - GUARD-04 AST guard for desktop/*.py back-edges (tests/test_no_back_edges_desktop.py)
  - SC#3 permanent genizah_core facade identity contract (tests/test_genizah_core_facade.py)
  - DESK-08 behavioral tests for sidecar coordination methods (tests/test_update_ui_coordination.py)
  - desktop/update_ui.py pre-registered in DESKTOP_MODULES with skip-until-exists guard
affects: [127-02, 127-03]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "GenizahGUI.__new__ + attribute stubs for testing methods in place without QApplication"
    - "Pre-register not-yet-created files in DESKTOP_MODULES with skip-until-exists guard (auto-enforces on file creation)"
    - "One test-function-per-shared-module for facade identity (named test_<module>_facade_identity)"

key-files:
  created:
    - tests/test_no_back_edges_desktop.py
    - tests/test_genizah_core_facade.py
    - tests/test_update_ui_coordination.py
  modified: []

key-decisions:
  - "Conftest _GUI_TEST_FILES unchanged — coordination tests use __new__ + Mock, no QApplication event loop needed"
  - "test_genizah_core_facade.py duplicates identity assertions from test_no_back_edges_core.py by design (ROADMAP SC#3 requires dedicated named file; both pass; original unchanged)"
  - "Empty-queue branch tested by patching genizah_app.QMessageBox (not genizah_app.QMessageBox.information alone) — bare __new__ object is not a real QWidget"

patterns-established:
  - "GUARD-04 pattern: scope-aware AST guard for desktop/*.py back-edges mirrors GUARD-01 pattern for shared/*.py"

requirements-completed: [DESK-08, GUARD-04]

# Metrics
duration: 4min
completed: 2026-06-26
---

# Phase 127 Plan 01: Update UI Final Cleanup — Wave 0 Test Scaffolds

**Three Nyquist scaffold test files (GUARD-04 AST guard + SC#3 facade identity + DESK-08 behavioral tests) established as green oracles before extraction waves begin — 40 passed, 1 skipped (desktop/update_ui.py skip-until-exists)**

## Performance

- **Duration:** ~4 min
- **Started:** 2026-06-26T14:26:44Z
- **Completed:** 2026-06-26T14:30:39Z
- **Tasks:** 2
- **Files modified:** 3 (created)

## Accomplishments

- `tests/test_no_back_edges_desktop.py` — GUARD-04 scope-aware AST guard for 19 desktop/*.py modules; `desktop/update_ui.py` pre-registered with skip-until-exists (auto-enforces once Wave 1 creates it); self-test verifies try:-guarded import IS caught and function-body lazy import is NOT (Pitfall 3 guard for join_workbench.py:4135)
- `tests/test_genizah_core_facade.py` — 13 per-module identity test functions asserting all 27 `shared.X.Name is genizah_core.Name` names; dedicated SC#3 contract file per ROADMAP; `test_no_back_edges_core.py` unchanged
- `tests/test_update_ui_coordination.py` — 7 behavioral tests for `_reset_sidecar_connections` (3 services + catalog filter), `_download_next_sidecar` (queue pop/FIFO/empty-queue reset), `_on_sidecar_download_finished` (success+failure both advance queue); tested via `GenizahGUI.__new__` + stubs, no QApplication required; conftest unchanged

## Task Commits

1. **Task 1: GUARD-04 desktop back-edge guard + SC#3 facade identity tests** - `caf9e0f9` (test)
2. **Task 2: DESK-08 behavioral tests for sidecar coordination methods** - `0c399609` (test)

## Files Created/Modified

- `tests/test_no_back_edges_desktop.py` — GUARD-04 AST guard; DESKTOP_MODULES 19 entries; `_has_module_level_genizah_app_import`; 2 guard self-tests; 19 parametrized tests (18 pass, 1 skip)
- `tests/test_genizah_core_facade.py` — 13 test functions / 27 identity assertions for permanent genizah_core SC#3 facade
- `tests/test_update_ui_coordination.py` — 7 behavioral tests covering 3 sidecar coordination methods via `GenizahGUI.__new__` duck

## Decisions Made

- Conftest `_GUI_TEST_FILES` NOT modified — the `__new__` + Mock pattern means no QApplication event loop is needed at collection time; no QApplication errors appeared
- `test_genizah_core_facade.py` intentionally duplicates identity assertions from `test_no_back_edges_core.py` per ROADMAP SC#3 directive (dedicated named file required); both files pass green
- Empty-queue branch in `_download_next_sidecar` tests by patching `genizah_app.QMessageBox` (the whole class mock, not just `.information`) since the bare `__new__` object cannot be used as a real QWidget parent

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Removed 3 unused `import genizah_app` statements in test_update_ui_coordination.py**
- **Found during:** Task 2 ruff check
- **Issue:** Three test functions had `import genizah_app` copied from the research pattern, but `_make_gui_coordinator()` handles the import internally — ruff F401 caught them
- **Fix:** Removed the three unused local imports (the functions already called `_make_gui_coordinator()` which imports `genizah_app` internally)
- **Files modified:** `tests/test_update_ui_coordination.py`
- **Verification:** ruff clean; 7 tests still pass
- **Committed in:** `0c399609` (Task 2 commit)

---

**Total deviations:** 1 auto-fixed (Rule 1 unused import)
**Impact on plan:** Trivial cosmetic fix; no logic change.

## Issues Encountered

None.

## Self-Check: PASSED

- `tests/test_no_back_edges_desktop.py` — FOUND
- `tests/test_genizah_core_facade.py` — FOUND
- `tests/test_update_ui_coordination.py` — FOUND
- Commit `caf9e0f9` — FOUND
- Commit `0c399609` — FOUND
- Full run: 40 passed, 1 skipped (desktop/update_ui.py skip-until-exists as expected)

## Next Phase Readiness

- Wave 1 (Plan 02) can now create `desktop/update_ui.py` — GUARD-04 will auto-enforce the no-back-edge contract on file creation
- DESK-08 behavioral tests are the oracle for confirming coordination methods work after any refactor
- SC#3 facade test is the permanent contract guard for the genizah_core re-export shims

---
*Phase: 127-update-ui-final-cleanup*
*Completed: 2026-06-26*
