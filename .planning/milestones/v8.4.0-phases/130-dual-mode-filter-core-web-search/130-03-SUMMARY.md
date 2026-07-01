---
phase: 130-dual-mode-filter-core-web-search
plan: "03"
subsystem: web-search-filter
tags: [library-filter, dual-mode, tests, tdd, DMF]
dependency_graph:
  requires: [130-01, 130-02]
  provides:
    - 24-test dual-mode behavior suite (mode-branch, migration, edge-states, dual-writer guard, LOCAL-shortlist)
    - revised test_libfilter_web_search.py (3-state label assertions, mode-scoped guard, docstring filter fix)
    - locked DMF-01/02/05/06/10 invariants via AST scans + pure mirrors
  affects:
    - tests/test_dual_mode_library_filter.py
    - tests/test_libfilter_web_search.py
tech_stack:
  added: []
  patterns:
    - pure-mirror + AST-scan test pattern (matching test_libfilter_web_search.py structure)
    - dual-mode filter mirror (_apply_library_filter_dual, _migrate_library_filter, _shortlist_codes)
    - nested-function line-based AST extraction for deeply nested closures
    - docstring-aware non-comment filter (avoids false positives in docstrings)
key_files:
  created:
    - tests/test_dual_mode_library_filter.py
  modified:
    - tests/test_libfilter_web_search.py
decisions:
  - "Pure mirrors defined at module level: _apply_library_filter_dual, _migrate_library_filter, _shortlist_codes — mirrors the test_libfilter_web_search pattern"
  - "AST extraction for nested closures uses line-based _extract_function_lines (not ast.get_source_segment which can't reach nested defs)"
  - "test_no_script_in_library_dialog_html docstring filter extended to skip triple-quoted strings — the redesigned dialog docstring documents BUG-B inline"
metrics:
  duration_minutes: 8
  completed_date: "2026-06-30"
  tasks_completed: 3
  tasks_total: 3
  files_changed: 2
---

# Phase 130 Plan 03: Dual-Mode Library Filter Test Coverage Summary

**One-liner:** 24-test dual-mode library filter suite covering mode-branch filter, legacy list migration, edge states, dual-writer dict-shape guard, LOCAL-shortlist exclusion, and AST source contracts; plus surgical revision of 3 stale inclusion-only assertions in the existing libfilter test.

## Tasks Completed

| Task | Name | Commit | Files |
|------|------|--------|-------|
| 1 | Write tests/test_dual_mode_library_filter.py (RED -> GREEN) | 6c70c559 | tests/test_dual_mode_library_filter.py |
| 2 | Revise stale inclusion-only assertions in tests/test_libfilter_web_search.py | f469988f | tests/test_libfilter_web_search.py |
| 3 | Full guard + regression run | (verified via Task 1+2 commits) | both test files |

## What Was Built

### Task 1 — New dual-mode test file (tests/test_dual_mode_library_filter.py)

**24 tests across 4 categories:**

**Pure mirror behavior tests (7 tests):**
- Mode-branch filter: Show-only keeps IN set, Hide keeps NOT-IN set
- Empty Show-only = show all (D-08); Empty Hide = show all (D-05)
- Full Hide-set = 0 results (DMF-06)
- Hide intent persists when NEW library appears (DMF-02 — mirror doesn't recompute set)

**Migration tests (6 tests):**
- Non-empty list `['CUL','JTS']` → `{mode:'show_only', codes:['CUL','JTS']}` (DMF-05)
- Empty list `[]` → `{mode:'hide', codes:[]}` (DMF-05)
- Dict valid roundtrip (both modes)
- Bogus mode in dict → falls back to `'hide'`
- `['CUL','LOCAL','ZZZ']` → `codes=['CUL']` (LOCAL + unknown dropped — DMF-10)
- None/int/str → `{mode:'hide', codes:[]}` without raising

**Shortlist/LOCAL exclusion (1 test):**
- `facets={'CUL':3,'LOCAL':9,'RNL':1}` → shortlist `['CUL','RNL']`, LOCAL absent (HIGH-2)

**AST source-contract scans (10 tests):**
- `_apply_library_filter` reads `library_mode`/`'show_only'`
- Restore region has `isinstance(_lib_raw, list)` + `isinstance(_lib_raw, dict)`
- Apply handler sanitizes with `c in LIBRARY_CODES and c != 'LOCAL'` (HIGH-3)
- Apply handler writes `persist_value('search_library_filter', {...dict...})` (D-09)
- Dialog shortlist build carries `c != 'LOCAL'` at least twice (HIGH-2)
- `_update_library_btn` has `tr('Showing')`, `tr('Hiding')`, `tr('Filter by library')`, `library_mode` (3-state)
- Dialog + restore both contain the LOCAL guard
- BOTH writers (search.py Apply + filter_panel.consume_incoming_filters) persist dict shape (HIGH-1)
- filter_panel stamps `'show_only'` + `c != 'LOCAL'` guard (HIGH-1)
- No `persist_value('search_library_filter', [...)` bare-list write in either file
- Persistence chokepoint: `persist_value`/`_safe_get` for the key; no raw `app.storage.user` near `library_filter`

**Pure mirrors defined:**
- `_apply_library_filter_dual(results_list, mode, codes)` — Show-only/Hide + empty pass-through
- `_migrate_library_filter(raw, valid_codes)` — list/dict/else branches + LOCAL sanitize
- `_shortlist_codes(facets, valid_codes)` — count-sorted, LOCAL excluded

### Task 2 — Revised stale assertions in tests/test_libfilter_web_search.py

**Three surgical changes (no deletions):**

**1. `test_chip_renders_when_library_only` (~line 428) — stale active-label:**
- Old assertion: `'({shown}/{total})' in update_btn_src or ...` (v8.3.0 single-state label)
- New assertion: requires all three dual-mode tokens — `tr('Showing')`, `tr('Hiding')`, `tr('Filter by library')` — and `library_mode` reference; fails if any is absent
- Added "Phase 130 (dual-mode)" comment

**2. `test_all_unchecked_guard` (~line 582) — Show-only mode scoping:**
- Updated docstring to document dual-mode scoping: guard is for Show-only, not universal
- Added assertion that mode-aware JS (`data-libmode`/`show_only`) is present
- Revised guard failure messages to note the Phase-130 Show-only scoping
- Updated `Select None` comment to note that empty Hide IS valid after dual-mode
- All "Phase 130 (dual-mode)" comments added

**3. `test_no_script_in_library_dialog_html` (~line 815) — docstring false positive:**
- Old filter: stripped `# comment` lines only
- New filter: also skips triple-quoted docstring lines (`"""..."""`) which the redesigned dialog uses to document the BUG-B requirement inline
- This fixes a false positive where `"""BUG-B: NO <script> inside ui.html()"""` in the function docstring was triggering the assertion
- "Phase 130 (dual-mode)" comment added

**Untouched:** `test_chip_placement_post_search_container` — contains NO active-label assertion (as noted in plan); only no-chip/chip-bar invariants. Left functionally unchanged.

### Task 3 — Full guard + regression run

All 5 named test files passed together:

```
pytest tests/test_dual_mode_library_filter.py \
       tests/test_libfilter_web_search.py \
       tests/test_web_library_options_no_local.py \
       tests/test_phase_97_invariants.py \
       tests/test_no_raw_storage_access.py
52 passed in 4.03s
```

ruff clean on both test files.

## Verification Results

- `pytest tests/test_dual_mode_library_filter.py -x -q` — 24/24 passed
- `pytest tests/test_libfilter_web_search.py -q` — 15/15 passed
- `pytest <all 5 named files> -q` — 52/52 passed
- `python -m ruff check tests/test_dual_mode_library_filter.py tests/test_libfilter_web_search.py` — all checks passed
- No production file modified in this plan (confirmed by git status)

## Deviations from Plan

**[Rule 1 - Bug] Fixed false-positive in test_no_script_in_library_dialog_html**

- Found during: Task 2 — the test was failing on `test_libfilter_web_search.py`
- Issue: The BUG-B guard's "non-comment lines" filter stripped `# comment` lines but not `"""docstring"""` lines. The Phase 130-02 dialog redesign added inline BUG-B documentation in the function docstring, which contains `<script>` references for documentation purposes. The guard fired on these docstring lines even though no actual `<script>` tag exists in `ui.html()` calls.
- Fix: Extended the non-comment filter to also skip triple-quoted docstring body lines using a simple in_docstring state toggle. The logic skips lines between `"""` delimiters so docstring content is never checked.
- Files modified: tests/test_libfilter_web_search.py (Task 2 file — within scope)
- Commit: f469988f

None other — plan executed exactly as written for all 3 tasks. The `test_chip_placement_post_search_container` was left functionally unchanged as specified.

## Known Stubs

None. This is a tests-only plan; no production functionality was added or stubbed.

## Plan-02 Verification Failure Surface

No production gaps were discovered. All AST scans passed on the first run:
- `_apply_library_filter` correctly reads `library_mode`/`'show_only'` (test 14)
- Restore region has both `isinstance(_lib_raw, list)` + dict branches (test 15)
- Apply handler sanitizes with `c in LIBRARY_CODES and c != 'LOCAL'` (test 16)
- Apply handler persists dict literal (test 17)
- Dialog shortlist carries `c != 'LOCAL'` twice (test 18)
- `_update_library_btn` has all 3-state labels + `library_mode` (test 19)
- BOTH writers persist dict shape (test 21)
- filter_panel stamps `'show_only'` (test 22)
- No bare-list writer (test 23)
- Persistence chokepoint clean (test 24)

All Plan-02 implementation claims in 130-02-SUMMARY.md are verified by passing AST scans.

## Threat Surface Scan

No new network endpoints, auth paths, or schema changes. This is a test-only plan. The threat register items from the plan were mitigated by the passing tests:
- T-130-03-01: LOCAL exclusion + migration sanitize + shortlist — GREEN (tests 11, 13, 18, 20)
- T-130-03-02: dual writer dict-shape — GREEN (tests 17, 21, 23)
- T-130-03-03: safe_storage chokepoint — GREEN (test 24 + test_no_raw_storage_access.py)

## Self-Check: PASSED

- `tests/test_dual_mode_library_filter.py` — exists, 24 tests pass
- `tests/test_libfilter_web_search.py` — exists, 15 tests pass (revised)
- Commit `6c70c559` — exists in git log (Task 1)
- Commit `f469988f` — exists in git log (Task 2)
- All 52 guard tests pass together
- ruff clean on both test files
- No production file modified
