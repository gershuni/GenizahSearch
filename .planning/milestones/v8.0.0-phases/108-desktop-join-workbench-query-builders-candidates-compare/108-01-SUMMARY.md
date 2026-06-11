---
phase: 108-desktop-join-workbench-query-builders-candidates-compare
plan: "01"
subsystem: desktop-joins-lab
tags: [joins-lab, measurements, rtl-fix, test-scaffolding, headless-tests]
dependency_graph:
  requires: [106-joins-lab-pure-logic, 107-join-workbench-shell]
  provides:
    - get_measurement_summaries_batch with size_category (column-guarded)
    - RTL-chrome fix on TabularQueryBuilderDialog
    - Wave-0 test scaffolds for builder OR/hoist contracts and triage keying
  affects: [shared/fjms_service.py, genizah_app.py, tests/]
tech_stack:
  added: []
  patterns:
    - column-existence guard mirroring avg_line_height_mm (RR-11 pattern)
    - PER-ROW modifier HOIST: single-box decorate, multi-box group then hoist outside (RR-13)
    - AST guard without QApplication (headless static analysis)
key_files:
  created:
    - tests/test_fjms_service.py (TestGetMeasurementSummariesBatch class appended)
    - tests/test_tabular_builder_rtl.py
    - tests/test_join_workbench_builder.py
    - tests/test_join_workbench_triage.py
  modified:
    - shared/fjms_service.py (get_measurement_summaries_batch extended with size_category)
    - genizah_app.py (dialog-level setLayoutDirection(RightToLeft) removed from TabularQueryBuilderDialog.__init__)
decisions:
  - "RR-11: size_category added behind column-existence guard (has_size_category), not unconditionally — old sidecars degrade to size_category:None rather than failing the whole SELECT"
  - "RR-6: extended EXISTING get_measurement_summaries_batch, no parallel get_measurements_batch added"
  - "D-06/R-04: removed single line self.setLayoutDirection(RightToLeft) from TabularQueryBuilderDialog.__init__; per-input RTL at line 1779 and preview-label RTL at line 1703 preserved"
  - "RR-13: test pins the HOISTED group form (#(a/b), -(a/b), (a/b)*, (a/b)#, %(a/b)) NOT the parser-broken per-box (#a/%b) round-2 design"
  - "R-05: triage keyed by sys_id (per-fragment), dedup keyed by (sys_id, page) — deliberate split documented in tests"
metrics:
  duration: "~9 minutes"
  completed: "2026-06-05T11:22:26Z"
  tasks_completed: 3
  tasks_total: 3
  files_changed: 6
  tests_added: 56
---

# Phase 108 Plan 01: Wave-0 Foundations — Batch Measurements + RTL Fix + Test Scaffolds Summary

**One-liner:** Column-guarded size_category extension to the existing batch measurements API, RTL-chrome one-line fix on TabularQueryBuilderDialog, and four headless Wave-0 test surfaces pinning the slash-group OR (RR-1) + per-row modifier HOIST (RR-13) + triage-keying (R-05) contracts.

## What Was Built

### Task 1: Extended `get_measurement_summaries_batch` with `size_category` (D-21/R-03/RR-6/RR-11)

`shared/fjms_service.py::get_measurement_summaries_batch` extended additively:
- Added `has_size_category = 'size_category' in cols` and `sc_col = ", size_category" if has_size_category else ""` mirroring the existing `has_line_height` / `lh_col` guard at lines 3023-3024.
- Spliced `{sc_col}` after `{lh_col}` in the SELECT column list.
- Conditional `col_names.append('size_category')` in the positional-fallback list (after lh, matching SELECT order).
- Returned dict adds `'size_category': r.get('size_category') if has_size_category else None`.
- The COALESCE(catalog_width_cm, max_computed_width_cm) fallback, the 500-batch loop, the dedup, and all existing keys are unchanged.

**RR-11 load-bearing property:** When `size_category` is absent from an old sidecar schema, the SELECT never references it and the batch returns normally (size_category: None per dict). Without the guard, the whole SELECT would fail and return an empty batch.

`TestGetMeasurementSummariesBatch` (9 tests added to `tests/test_fjms_service.py`):
- `test_with_size_category_returns_value` — seeded value returned in WITH-column case
- `test_existing_keys_unchanged` — all prior keys still present and correct (regression)
- `test_coalesce_computed_width_fallback` — COALESCE for computed-only rows preserved
- `test_absent_column_degrades_to_none_not_empty_batch` — RR-11 load-bearing: old sidecar returns full batch with size_category: None
- `test_absent_column_legacy_values_correct` — legacy values correct in absent-column case
- `test_missing_sys_id_absent_from_result` — missing id not in result dict (not None)
- `test_batch_boundary_500_returns_all_matches` — 600 ids return all 600 rows
- `test_empty_sys_ids_returns_empty` — edge case
- `test_no_parallel_get_measurements_batch_method` — RR-6 regression: no parallel method added

### Task 2: Remove dialog-level RTL-chrome bug + AST guard (D-06/R-04)

`genizah_app.py:1555` — removed `self.setLayoutDirection(Qt.LayoutDirection.RightToLeft)` from `TabularQueryBuilderDialog.__init__`. This line mirrored the QHBoxLayout arrangement (checkboxes appeared on wrong side, labels clipped).

Per-input RTL at line 1779 (`inp.setLayoutDirection(RightToLeft)`) and preview-label RTL at line 1703 (`self._preview_label.setLayoutDirection(RightToLeft)`) remain unchanged.

`tests/test_tabular_builder_rtl.py`: AST guard that parses genizah_app.py, walks `TabularQueryBuilderDialog.__init__`, and asserts zero `self.setLayoutDirection(...RightToLeft...)` calls at the dialog level.

### Task 3: Headless test scaffolds (RR-1, RR-13, R-05)

**`tests/test_join_workbench_builder.py`** (39 tests, NO PyQt6 import):
- `build_term(tokens, mods)` helper — exact Wave-2 `build_side_query` HOIST logic (RR-13)
- `side_from_rows(rows_spec, ...)` helper — builds SideQuery from box/mods tuples
- `TestEmptyAndBare`: empty compose, bare term, single-box returns bare string
- `TestOrSlashGroup`: two-box → `(פירוש/פירש)` term; **RR-1 parser-level regression**: `parse_responsa_query` asserts `.words = [פירוש, פירש]` (not a `|` substring check); `(עץ/אילן)` sanity
- `TestSingleBoxModifiers`: all 7 modifier modes (`#prefix`, `suffix#`, `%plene`, `*suffix`, `*prefix`, `-negation`) build_term AND parse round-trips; full builder→compose→parser chain for prefix
- `TestMultiBoxHoistedModifiers`: **RR-13 HOISTED group forms** — `#(a/b)` / `%(a/b)` / `-(a/b)` / `(a/b)#` / `(a/b)*` — build_term, parse-only, and full-chain assertions; wildcard-PREFIX not hoistable on multi-box; three-box group
- `TestLineAnchors`: line_start/line_end markers in two-row compose
- `TestPagePosition`: page_position forwarded and validated (ValueError on empty-row anchors)
- `test_no_pipe_or_assertion_in_this_file`: RR-1 self-check guards against reintroducing the `|`.join OR pattern

**`tests/test_join_workbench_triage.py`** (6 tests, NO PyQt6 import):
- `test_same_sys_id_different_pages_shares_triage`: R-05 core — marking via page 3 → page 4 lookup also sees the mark
- `test_dedup_key_differs_between_pages`: `Candidate.key = (sys_id, page)` differs, `sys_id` is same
- `test_clearing_triage_drops_all_lookups`: re-anchor behavior
- `test_different_sys_ids_have_independent_triage`: independence
- `test_candidate_key_property_is_sys_id_page_pair`: Candidate.key contract
- `test_via_other_side_candidate_same_sys_id_shares_triage`: cross-side OR neighbor at neighbor page shares triage

## Deviations from Plan

None — plan executed exactly as written.

## Threat Flags

No new security-relevant surface introduced. The `size_category` extension adds one additive, column-guarded constant string fragment to an existing parameterized SQLite read (T-108-01 disposition: mitigate via existing `?` placeholders, preserved). The sc_col/lh_col fragments are static string literals from a boolean column-existence check — no value interpolation.

## Self-Check: PASSED

Files exist:
- `shared/fjms_service.py` — FOUND (has_size_category guard confirmed)
- `tests/test_fjms_service.py` (TestGetMeasurementSummariesBatch) — FOUND
- `tests/test_tabular_builder_rtl.py` — FOUND
- `tests/test_join_workbench_builder.py` — FOUND
- `tests/test_join_workbench_triage.py` — FOUND
- `genizah_app.py` (RTL line removed) — FOUND

Commits exist:
- `06fa8739` (Task 1: fjms_service + measurements batch tests) — FOUND
- `454a58a0` (Task 2: RTL fix + AST guard) — FOUND
- `fa0b2a38` (Task 3: builder/triage headless tests) — FOUND

Tests pass: 56/56 green (plan verification suite + joins_lab regression 70/70).
ruff: All checks passed on all modified/created files.
