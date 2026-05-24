---
phase: 96-completing-my-library-feature-add-features-and-fix-bugs
plan: 07
subsystem: ui
tags: [phase-96, my-library, desktop, cleanup, redundancy-removal, result-dialog]

# Dependency graph
requires:
  - phase: 96-01
    provides: "xfail(strict=True) skeleton tests that flip green when button is removed"
provides:
  - "NEW-1: btn_rd_open_browse removed from desktop/result_dialog.py (declaration + handler + visibility branches)"
  - "4 stable negative-assertion regression guards in test_local_browse_panel.py and test_result_dialog_local_button_removed.py"
affects: [96-09]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "xfail-flip pattern: wave-0 plants strict xfail with inverted assertion; removal wave strips decorator to promote test to stable guard"

key-files:
  created: []
  modified:
    - desktop/result_dialog.py
    - tests/test_local_browse_panel.py
    - tests/test_result_dialog_local_button_removed.py

key-decisions:
  - "Removed btn_rd_open_browse entirely (not gated by _is_local_hit) — the עיין Browse button (btn_view_transcription) already covers the use case per CONTEXT D-11 and user smoke testing on v7.14.0"
  - "Preserved _is_local_hit branch structure in load_result() — only the btn_rd_open_browse.setVisible lines were deleted; btn_rd_open_file visibility logic kept intact"

patterns-established:
  - "xfail-flip pattern: reusable for future feature-removal plans. Wave N: plant xfail(strict=True) with negated assertion. Wave N+k: remove the feature, then strip the xfail decorator in the same plan. The strict=True ensures the executor is forced to act."

requirements-completed: [NEW-1]

# Metrics
duration: 15min
completed: 2026-05-24
---

# Phase 96 Plan 07: Remove Redundant `צפה בדפדוף` Button Summary

**Removed redundant `btn_rd_open_browse` widget + handler + visibility branches from `desktop/result_dialog.py`; 4 xfail(strict=True) tests promoted to stable negative-assertion regression guards.**

## Performance

- **Duration:** ~15 min
- **Started:** 2026-05-24T10:02:00Z
- **Completed:** 2026-05-24T10:17:20Z
- **Tasks:** 2 of 2
- **Files modified:** 3

## Accomplishments

- Deleted 3 distinct code regions in `desktop/result_dialog.py` totalling ~46 lines:
  - **Region 1** (lines 339-352): `btn_rd_open_browse` widget declaration + `.clicked.connect(_rd_open_in_browse)` + `action_row.addWidget`
  - **Region 2** (lines 1922-1943): `_rd_open_in_browse` handler method (22 lines)
  - **Region 3** (lines 2002-2014): `btn_rd_open_browse.setVisible(True/False)` in the `_is_local_hit` branch and `except` block
- `btn_view_transcription` (the `עיין` Browse button at line 248) is untouched — it covers the use case
- Promoted 4 `xfail(strict=True)` tests to stable regression guards by stripping the decorator; all 4 now PASS

## The xfail-Flip Pattern

This plan demonstrates a reusable pattern for feature-removal plans:

1. **Wave 0 (plan 96-01):** Plant tests with **inverted assertions** (assert the feature is ABSENT) and mark `@pytest.mark.xfail(strict=True)`. They XFAIL because the feature is still present — confirming the pre-state.
2. **Removal wave (this plan):** Remove the feature. The tests now XPASS (unexpected pass). `strict=True` turns XPASS into a TEST FAILURE — forcing the executor to act.
3. **Flip:** Strip the `@pytest.mark.xfail(...)` decorator. Tests become stable PASS — permanent regression guards that catch re-introduction.

## Task Commits

1. **Task 1: Remove btn_rd_open_browse declaration, handler, and visibility branches** - `5464171b` (feat)
2. **Task 2: Flip xfail decorators to stable assertions on 4 tests** - `1618f8e8` (test)

## Files Created/Modified

- `desktop/result_dialog.py` — 46 lines deleted: widget declaration (Region 1), `_rd_open_in_browse` handler (Region 2), `.setVisible` calls (Region 3)
- `tests/test_local_browse_panel.py` — 3 `@pytest.mark.xfail(strict=True)` decorators removed from `test_result_dialog_has_view_in_browse_button`, `test_result_dialog_has_open_in_browse_handler`, `test_result_dialog_show_view_in_browse_for_local_only`
- `tests/test_result_dialog_local_button_removed.py` — 1 `@pytest.mark.xfail(strict=True)` decorator removed from `test_btn_rd_open_browse_removed`

## Verification Results

```
pytest tests/test_local_browse_panel.py tests/test_result_dialog_local_button_removed.py -v
=> 12 passed in 3.34s

pytest tests/test_local_*.py tests/test_web_library_options_no_local.py tests/test_no_raw_storage_access.py -q
=> 187 passed, 4 skipped, 2 xfailed in 24.98s

python -m ruff check desktop/result_dialog.py tests/test_local_browse_panel.py tests/test_result_dialog_local_button_removed.py
=> All checks passed!

python -c "import ast; ast.parse(open('desktop/result_dialog.py', encoding='utf-8').read()); print('OK')"
=> OK syntactically valid
```

## Deviations from Plan

None — plan executed exactly as written. The 3 regions were precisely located and removed; visibility branch structure preserved for `btn_rd_open_file` which still needs it.

## Known Stubs

None.

## Threat Flags

None — this is a UI cleanup (button removal), no new network endpoints or auth paths introduced.

## Self-Check: PASSED

- `desktop/result_dialog.py` exists and contains `btn_view_transcription`: confirmed
- `btn_rd_open_browse` absent from `desktop/result_dialog.py`: confirmed
- Commits `5464171b` and `1618f8e8` exist in git log: confirmed
- All 4 ex-xfail tests PASS: confirmed (12 passed in target suite)
