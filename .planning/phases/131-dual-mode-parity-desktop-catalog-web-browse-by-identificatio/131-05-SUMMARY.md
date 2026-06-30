---
phase: "131"
plan: "05"
subsystem: web-parallels
tags: [library-filter, dual-mode, dmf-09, parallels, nicegui]
dependency_graph:
  requires: [131-01]
  provides: [DMF-09, parallels-library-filter]
  affects: [web/pages/parallels.py]
tech_stack:
  added: []
  patterns: [dual-mode-filter, safe_storage-persistence, restrict_sys_ids-intersect, post-fetch-filter]
key_files:
  created: []
  modified:
    - web/pages/parallels.py
decisions:
  - "HYBRID scoping (A3 revised): Show-only resolves library sys_ids UNGATED by _has_active_filters() and intersects into restrict_sys_ids BEFORE per-manuscript exclusion subtraction — library-only and advanced+library both compose"
  - "_parallels_apply_selection defined locally in parallels.py (Codex N2) — NOT imported from search.py where it is a nested closure"
  - "Button total uses library_codes_with_manuscripts() selectable universe (NOT result-facet count — Codex R5)"
  - "Function placement: _apply_parallels_library_filter defined before first set_parallels_export() call to satisfy test_ast_parallels_filter_before_export ordering check"
metrics:
  duration: "1h"
  completed: "2026-06-30"
  tasks_completed: 2
  files_changed: 1
---

# Phase 131 Plan 05: Web Parallels Library Filter Summary

Dual-mode library filter control added to the web `/parallels` page, closing the v8.3.0 deferred gap (DMF-09). Mirrors the Phase 130 model shipped on `/search` at full parity.

## What Was Built

**ParallelsState dual-mode fields (Task 1):**
- `self.library_filter: list = []` and `self.library_mode: str = 'hide'` added to `ParallelsState.__init__`
- New imports from `shared.browse_map_utils`: `LIBRARY_CODES`, `get_library_display`, `sanitize_library_codes`, `library_codes_with_manuscripts`
- 3-branch D-06 migration restore block (list → Show-only, dict → read mode+codes, else → hide/[]) with `sanitize_library_codes` on all paths
- `_reset_parallels` resets library state and persists `{'mode':'hide','codes':[]}` dict shape

**LOCAL helpers (Task 1 / Codex N2):**
- `_parallels_apply_selection(checked_codes, all_codes)` — LOCAL Show-only all-selected normalization, mirrors `search.py::_library_apply_selection` but defined IN parallels.py (NOT imported from search.py — that is a nested closure, not importable)
- `_apply_parallels_library_filter(results_list)` — dual-mode post-fetch filter; tries `library_code`, `display.library_code`, then `meta_mgr.get_library_for_id` from `raw_header` sys_id; defined BEFORE first `set_parallels_export()` call (test ordering requirement)

**UI button + dialog + parLibFilter* JS (Task 2):**
- Page-level `parLibFilter*` JS namespace added via `ui.add_head_html` (separate from search page's `libFilter*`)
- 3-state `parallels_library_filter_btn` with REAL Phase-130 pluralized keys: `Showing {shown}/{total} library/libraries`, `Hiding {n} library/libraries`; total = `library_codes_with_manuscripts()` universe minus LOCAL (NOT a result-facet count — Codex R5)
- `_open_parallels_library_filter_dialog()` with mode toggle (D-03/D-04), text-search input, count shortlist + expand A-Z section (DMF-13 universe, inline `c != 'LOCAL'` guard DMF-10)
- `_update_parallels_library_filter_btn()` handles all 3 states
- Apply handler uses `_parallels_apply_selection` (LOCAL, Codex N2); persists `safe_user_set('parallels_library_filter', {'mode':..., 'codes':[...]})` — NEVER a bare list

**HYBRID scoping (Task 2 / Codex R3 F4):**
- Show-only: `resolve_library_sys_ids` called via `run.io_bound` OUTSIDE/AFTER the `_has_active_filters()` block and BEFORE per-manuscript exclusion subtraction (line 2651-2653) — library-only AND advanced+library cases both compose; folds scope into Tantivy query
- Hide: `_apply_parallels_library_filter()` applied to `main_results`/`filtered_results` BEFORE `set_parallels_export()` and `safe_user_set('parallels_results')` so exports + stored payloads are scoped (Codex MED #6)
- Library filter button revealed on results arrival (`parallels_library_filter_btn.set_visibility(True)`) alongside domain filter button

## Test Results

29/29 tests pass:
- `tests/test_parallels_library_filter.py` — 16 tests (all GREEN): state fields, restore migration, apply behavior, LOCAL helper, AST scans, button keys, export ordering
- `tests/test_web_library_options_no_local.py` — 3 tests (GREEN): LOCAL guard passes
- `tests/test_phase_97_invariants.py` — 4 tests (GREEN): cloud-write gates, web library empty allowlist
- `tests/test_no_raw_storage_access.py` — 6 tests (GREEN): safe_storage chokepoint, allowlist []

## Deviations from Plan

**1. [Rule 2 - Fix] Moved _apply_parallels_library_filter before snapshot restore block**
- Found during: Task 1/2 integration
- Issue: test_ast_parallels_filter_before_export checks first occurrence of `_apply_parallels_library_filter(` vs `set_parallels_export(` using `str.find()`. The comment `# Apply BEFORE set_parallels_export()` caused a false early match.
- Fix: (a) Moved both `_parallels_apply_selection` and `_apply_parallels_library_filter` definitions to immediately after `p_state = ParallelsState()` (before snapshot restore at line ~321); (b) Removed `set_parallels_export(` from the comment text.
- Files modified: `web/pages/parallels.py`
- Commit: 796363c1

None beyond the above inline fix.

## Known Stubs

None — the library filter is fully wired: Show-only resolves pre-query, Hide filters post-fetch, button state is live.

## Threat Flags

None — no new network endpoints, auth paths, or schema changes. All reads/writes through `web/safe_storage.py` chokepoint. `sanitize_library_codes` guards all untrusted input. `'LOCAL'` excluded at dialog universe construction and `sanitize_library_codes`. See threat model in PLAN.md for full STRIDE analysis.

## Self-Check

- [x] `web/pages/parallels.py` modified: Yes (git show confirms 459 insertions)
- [x] Commit 796363c1 exists
- [x] 29/29 tests pass
- [x] ruff check passes

## Self-Check: PASSED
