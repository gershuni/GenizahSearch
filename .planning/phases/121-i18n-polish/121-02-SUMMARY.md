---
phase: 121-i18n-polish
plan: "02"
subsystem: web/tests
tags: [i18n, testing, guard, rtl, ast-guard, render-smoke]
dependency_graph:
  requires: [121-01]
  provides: [FND-07-guards]
  affects: [tests/test_joins_lab_i18n.py, tests/render_smoke/test_joins_lab_render_smoke.py]
tech_stack:
  added: []
  patterns: [AST-guard, explicit-badge-string-list, scoped-host-key-check, NiceGUI-render-smoke-RTL]
key_files:
  created:
    - tests/test_joins_lab_i18n.py
  modified:
    - tests/render_smoke/test_joins_lab_render_smoke.py
decisions:
  - "HEBREW_LITERAL_ALLOWLIST: 8 D-04 operator-tuple literals from joins_builder.py:344-351 (byte-for-byte copy)"
  - "BADGE_STRINGS: explicit static list of 3 badge_and_tooltip() keys (tr(variable) gap — AST cannot detect)"
  - "ENTRY_POINT_KEYS: 4 scoped host keys — joins_panel.py (3) and lists.py (1)"
  - "Pagination row RTL test: 25 stub results passed to force total_pages=2 > PAGE_SIZE=24"
  - "Compare nav bar identified by triple-token signature (flex-row-reverse+justify-between+flex-wrap) per REVIEWS #3 MEDIUM"
  - "set_language restore in finally block for test isolation (Pitfall 4)"
metrics:
  duration: "~20min"
  completed: "2026-06-21"
  tasks: 2
  files: 2
---

# Phase 121 Plan 02: Permanent i18n Guards + RTL Render-Smoke Summary

Shipped two automated layers that lock FND-07 forever: a permanent CI AST guard and RTL structural assertions in the render-smoke harness.

## What Was Built

**Task 1 — `tests/test_joins_lab_i18n.py` (new, 347 lines)**

Permanent dual-check CI guard with five tests:

- `test_target_files_exist`: guards all 8 FULL_SCAN_FILES exist (structural safety net)
- `test_no_raw_hebrew_literals`: SC#3 — zero raw Hebrew literals outside `tr()` in the 8 dedicated files, with the D-04 HEBREW_LITERAL_ALLOWLIST (8 exact operator-tuple literals from joins_builder.py:344-351) and structural docstring exclusion (Pitfall 3)
- `test_all_tr_keys_covered`: SC#1 — every `tr("literal")` key in the 8 files resolves in TRANSLATIONS
- `test_badge_strings_covered`: SC#1 — 3 badge_and_tooltip() strings (`'Anchor fragment'`, `'Found via other side'`, `'Visually similar'`) resolve in TRANSLATIONS (explicit static list, not AST-catchable)
- `test_entry_point_keys`: SC#1 consistency — 4 scoped entry-point keys (`'Find Joins in the Joins Lab'`, `'Joined Fragments'`, `'Go to Joins Lab to find more joins'`, `'Open in Joins Lab'`) both resolve in TRANSLATIONS AND appear as `tr("key")` in their host files

**Task 2 — `tests/render_smoke/test_joins_lab_render_smoke.py` (extended, +144 lines)**

New `test_rtl_flex_row_reverse_pagination_and_compare()` function:

- Assertion 1 (candidate_grid.py:1363 `_pg_dir`): with 25 stub results (> PAGE_SIZE=24, total_pages=2), the pagination row carries `flex-row-reverse` in its `_classes` in HE mode
- Assertion 2 (compare_modal.py:792 `_nav_dir_class`): the Compare verdict/nav bar carries the triple-token signature (`flex-row-reverse` + `justify-between` + `flex-wrap`) in HE mode — NOT a "not-pagination" heuristic (REVIEWS #3 MEDIUM)
- `set_language('he')` called before `user.open('/joins-lab')`, restored in `finally` (Pitfall 4)
- `_make_stub_results(25)` helper generates 25 distinct raw result dicts

## Sanity Injection (SC#3 proof)

Temporarily inserted `_SANITY_INJECTION_HEBREW = "שלום"` (outside `tr()`) in `web/components/joins_panel.py`:
- `test_no_raw_hebrew_literals` **FAILED** with: `web/components/joins_panel.py:2: 'שלום'`
- Removing the injection: test returns **GREEN**
- Guard proven to bite.

## Test Results

All tests pass:

```
python -m pytest tests/test_joins_lab_i18n.py -x -q
5 passed in 0.42s

python -m pytest tests/render_smoke/test_joins_lab_render_smoke.py -q
35 passed, 1 skipped in 66.90s

python -m pytest tests/test_joins_lab_i18n.py tests/render_smoke/ -q
40 passed, 1 skipped in 67.74s
```

## Deviations from Plan

None — plan executed exactly as written.

The sanity injection test was performed and reverted as required by the acceptance criteria, and is documented above rather than committed.

## Threat Flags

None — this plan adds static test guards only; no new production code, no new endpoints, no auth/crypto changes.

## Self-Check: PASSED

- `tests/test_joins_lab_i18n.py` exists at expected path
- `tests/render_smoke/test_joins_lab_render_smoke.py` contains `flex-row-reverse` and `set_language`
- Task 1 commit `7907c323` exists in git log
- Task 2 commit `bfb18861` exists in git log
- conftest.py unchanged (no diff)
- All 40 combined tests pass
