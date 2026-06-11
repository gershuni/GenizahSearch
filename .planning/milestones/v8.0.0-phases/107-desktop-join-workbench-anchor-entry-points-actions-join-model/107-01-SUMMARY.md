---
phase: 107-desktop-join-workbench-anchor-entry-points-actions-join-model
plan: "01"
subsystem: desktop-join-workbench
tags: [i18n, pure-helpers, ast-guards, join-workbench, desktop]
dependency_graph:
  requires: [shared/joins_lab.py (Phase 106), genizah_translations.py, genizah_core.tr]
  provides: [desktop/join_workbench.py (helpers), Phase-107 i18n bootstrap, SC#5/SC#6 AST guards]
  affects: [plans 107-02 and 107-03 depend on helpers and i18n bootstrap]
tech_stack:
  added: []
  patterns: [TRANSLATIONS.update() i18n bootstrap, AST-guard test pattern, pure-helper module pattern]
key_files:
  created:
    - desktop/join_workbench.py
    - tests/test_join_workbench.py
    - tests/test_join_workbench_no_private.py
    - tests/test_join_workbench_i18n.py
  modified:
    - genizah_translations.py
decisions:
  - "Used _BADGE_CONFIG with label_kind tokens instead of frozen tr() values at import time (must-fix #9) — badge labels are resolved at call time so CURRENT_LANG switches are reflected"
  - "Plan-01 Task-1 bootstraps ALL 11 NEW phase keys in a single closed block so Plans 02/03 never touch genizah_translations.py"
  - "Scoped host-key check in i18n AST guard uses pytest.xfail(strict=False) during Plan 01/02; self-activates to pass after Plan 03 adds tr('Find joins') to host files (must-fix #10)"
metrics:
  duration: "~6 minutes (2026-06-04T08:13:28Z – 2026-06-04T08:18:51Z)"
  completed_date: "2026-06-04"
  tasks_completed: 4
  files_changed: 5
---

# Phase 107 Plan 01: Join Workbench i18n Foundation + Pure Helpers + AST Guards Summary

Single-sentence summary: Phase-107 closed i18n bootstrap (11 NEW keys), headless `desktop/join_workbench.py` pure-helper module, and 3 Wave-0 test files (54 passing + 1 xfail) establishing SC#5/SC#6 gates for Plans 02/03.

## Tasks Completed

| Task | Name | Commit | Files |
|------|------|--------|-------|
| 1 | Add Phase-107 i18n bootstrap (11 NEW keys) | cc57752f | genizah_translations.py |
| 2 | Create desktop/join_workbench.py pure helpers | c7ac449b | desktop/join_workbench.py |
| 3 | Create tests/test_join_workbench.py (Tier-1 unit suite) | fc9d679c | tests/test_join_workbench.py |
| 4 | Create AST guards (SC#5 no _vs_* + SC#6 tr-key coverage) | e9e682c6 | tests/test_join_workbench_no_private.py, tests/test_join_workbench_i18n.py |

## Verification Results

- `pytest tests/test_join_workbench.py tests/test_join_workbench_no_private.py tests/test_join_workbench_i18n.py -x` → 54 passed, 1 xfailed
- `python -c "import desktop.join_workbench"` → exits 0 (headless, no QApplication)
- `python -c "from genizah_translations import TRANSLATIONS"` → all 12 phase keys resolve (11 NEW + reused "Add as Join")
- `python -m ruff check desktop/join_workbench.py tests/test_join_workbench.py tests/test_join_workbench_no_private.py tests/test_join_workbench_i18n.py` → All checks passed

## Success Criteria Status

- [x] ALL Phase-107 i18n keys present in TRANSLATIONS with Hebrew values: 11 NEW keys + reused "Add as Join" (:3226) — Plans 02/03 add no new keys
- [x] `desktop/join_workbench.py` exists with 4 pure helpers + 5 result accessors, imports headlessly, NO Qt window code; badge labels resolve via tr() at call time (must-fix #9)
- [x] 3 Wave-0 test files exist and pass; AST guards enforce SC#5 + SC#6; i18n guard scopes a check over Plan-03 host strings (must-fix #10)

## Deviations from Plan

None — plan executed exactly as written.

The one xfail (`test_phase107_host_keys_translated_and_wrapped`) is planned behavior: it self-activates to pass after Plan 03 adds `tr("Find joins")` to genizah_app.py / result_dialog.py (must-fix #10 design documented in the test file).

## Known Stubs

None. This plan contains only pure helpers and test files. No UI components, no data sources, no stubs.

## Threat Flags

No new threat surface. Plan 01 is pure-Python helpers + tests + a translation dict. No network, no storage, no SQL, no new endpoints.

## Self-Check: PASSED

Files exist:
- `desktop/join_workbench.py` — FOUND
- `tests/test_join_workbench.py` — FOUND
- `tests/test_join_workbench_no_private.py` — FOUND
- `tests/test_join_workbench_i18n.py` — FOUND

Commits exist (verified via `git log --oneline -5`):
- cc57752f — FOUND (Task 1: i18n bootstrap)
- c7ac449b — FOUND (Task 2: pure helpers)
- fc9d679c — FOUND (Task 3: unit tests)
- e9e682c6 — FOUND (Task 4: AST guards)
