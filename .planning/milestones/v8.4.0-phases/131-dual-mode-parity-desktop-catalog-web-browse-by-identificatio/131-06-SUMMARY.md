---
phase: 131-dual-mode-parity-desktop-catalog-web-browse-by-identificatio
plan: "06"
subsystem: desktop-catalog-library-filter
tags: [desktop, catalog, library-filter, facets, qthread, dmt-07, dmf-12, gap-closure]
dependency_graph:
  requires: [131-01, 131-02, 131-03, 131-04, 131-05]
  provides: [desktop-library-facet-counts]
  affects: [desktop/dialogs_filter.py, genizah_app.py, tests/test_libfilter_desktop.py]
tech_stack:
  added: []
  patterns: [QThread-worker-off-ui-thread, QEventLoop-synchronous-wait, facets-dict-param]
key_files:
  created: []
  modified:
    - desktop/dialogs_filter.py
    - genizah_app.py
    - tests/test_libfilter_desktop.py
decisions:
  - "Used QEventLoop synchronous-wait pattern (preserves existing dlg.exec() flow) rather than splitting _open_catalog_library_dialog into async callback chains"
  - "_CatalogFacetWorker placed at module level (not nested) so pyqtSignal works reliably in PyQt6, mirroring _CatalogRefreshWorker"
  - "Fail-open: worker emits {} on any exception so dialog always opens (name-only rows) even when DB is unavailable"
  - "WaitCursor shown during facet computation (try/finally ensures restore even on error)"
metrics:
  duration: "~15 minutes"
  completed: "2026-06-30T15:20:49Z"
  tasks_completed: 3
  files_changed: 3
---

# Phase 131 Plan 06: Desktop Catalog Library Facet Counts Summary

Closed the single Phase 131 UAT gap: desktop catalog `LibraryFilterDialog` now shows per-library manuscript counts (e.g. `CUL (1,234)`) that are DYNAMIC — they honor the catalog's active PGP-Only / Scholarly-Editions / domain / date / text filters, at parity with web `/catalog` Browse-by-Identification.

## Tasks Completed

| Task | Name | Commit | Files |
|------|------|--------|-------|
| 1 | Add optional facets param + 'Name (count)' rendering to LibraryFilterDialog | `08812bce` | `desktop/dialogs_filter.py` |
| 2 | Compute dynamic facets OFF the UI thread and pass them into the catalog dialog | `c3c12d1a` | `genizah_app.py` |
| 3 | Extend tests/test_libfilter_desktop.py — count rendering, dynamic counts, fallback, LOCAL, off-thread | `8f156334` | `tests/test_libfilter_desktop.py` |

## What Was Built

### Task 1 — LibraryFilterDialog facets param (desktop/dialogs_filter.py)

Added `facets: dict | None = None` keyword-only param to `LibraryFilterDialog.__init__`. Stored defensively as `self._facets = facets if isinstance(facets, dict) else {}` so None/non-dict values fall back to name-only rows. In the row-build loop: when `self._facets.get(code)` is an `int >= 0`, the row renders `f"{base_label}  ({count:,})"` (localized thousands separator matching the author/work list rows at lines 10095/10131). Otherwise: name-only. `UserRole` data stays the bare code — `get_checked_codes()` contract unchanged. All existing dual-mode behavior preserved.

### Task 2 — Off-UI-thread facet wiring (genizah_app.py)

Added `_CatalogFacetWorker(QThread)` at module level (after `_CatalogRefreshWorker`, before `_format_list_star`). Its `run()` mirrors `web/pages/catalog_browse.py::_fetch_library_facets_blocking`: calls `fjms.get_browse_library_facets(...)` with all active non-library filters; calls `_get_catalog_filter_sets()` only when PGP/Editions filter is active; passes `meta_mgr.get_library_for_id` as the full-corpus `sys_id_to_library` resolver (None-guarded). Emits `{}` on any exception (fail-open).

`_open_catalog_library_dialog` now: constructs `_CatalogFacetWorker` with the active filter values; uses `QEventLoop` to wait synchronously (preserving the existing `dlg.exec()` flow); sets WaitCursor during the wait; passes `facets=facets` into `LibraryFilterDialog`. Worker stored as `self._catalog_facet_worker` to prevent GC mid-run.

### Task 3 — New tests (tests/test_libfilter_desktop.py)

5 new tests added (total: 22 → 27):
1. `test_dialog_renders_facet_counts` — CUL row contains `(1,234)` when `facets={'CUL': 1234}`; absent code renders name-only
2. `test_dialog_facets_none_fallback` — both `facets=None` and no kwarg render name-only; `get_checked_codes()` returns bare codes
3. `test_facet_worker_dynamic_pgp_filter` — monkeypatched worker call asserts `pgp_filter='has_pgp'`, non-None `pgp_sys_ids`, and `meta_mgr.get_library_for_id` are passed through
4. `test_dialog_excludes_local_even_in_facets` — `facets={'LOCAL': 99, 'CUL': 5}` never produces a LOCAL row
5. `test_facet_computation_off_ui_thread` — `issubclass(_CatalogFacetWorker, QThread)` + AST scan proves `get_browse_library_facets` in `run()` body and NOT in `_open_catalog_library_dialog` body

## Verification Results

```
GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/test_libfilter_desktop.py -q
27 passed, 1 warning in 2.04s

python -m ruff check desktop/dialogs_filter.py genizah_app.py tests/test_libfilter_desktop.py
All checks passed!
```

## Deviations from Plan

None — plan executed exactly as written.

## Known Stubs

None. All facet-count rendering is wired to the real `get_browse_library_facets` shared engine. The live PyQt6 render-smoke (open Catalog tab → library filter → confirm rows show counts that change with PGP-Only toggled; UI does not freeze) is `human_needed` — Qt cannot be rendered headless in CI. Recorded in phase UAT.

## Known Human-Needed Gap

The live render-smoke verification is not automatable headless:
- Open Catalog tab → click library filter button
- Confirm rows show "CUL (N,NNN)" style counts
- Toggle PGP-Only on → confirm counts change dynamically
- Confirm UI does not freeze during facet computation (WaitCursor appears briefly)

## Self-Check: PASSED

- `desktop/dialogs_filter.py` modified: confirmed
- `genizah_app.py` modified: confirmed (`_CatalogFacetWorker` present, `_open_catalog_library_dialog` wired)
- `tests/test_libfilter_desktop.py` extended: confirmed (27 tests)
- Commits: `08812bce`, `c3c12d1a`, `8f156334` — all present in git log
