---
phase: 129-library-filter-search-browse-by-identification-seed-026
plan: "04"
subsystem: genizah_app/desktop-catalog
tags: [library-filter, desktop, catalog, seed-026, libfilter-03, guard-02]
dependency_graph:
  requires:
    - 129-01 (resolve_library_sys_ids + get_browse_results library args)
  provides:
    - genizah_app.py::_CatalogRefreshWorker (library_filter + meta_mgr ctor args + run() resolution)
    - genizah_app.py::_catalog_library_filter state + dropdown widget + chips
    - tests/test_libfilter_desktop.py (3 LIBFILTER-03 gui-marked tests)
  affects:
    - Desktop catalog Browse-by-Identification now filters by library code at parity with web
tech_stack:
  added: []
  patterns:
    - _CatalogRefreshWorker explicit meta_mgr ctor arg (OQ-2 resolution)
    - resolve_library_sys_ids called inside run() (background QThread, never UI thread)
    - QPushButton + QMenu with checkable QActions (library multi-select dropdown)
    - Per-code removable chips in _catalog_update_chips
key_files:
  created:
    - tests/test_libfilter_desktop.py
  modified:
    - genizah_app.py
    - tests/conftest.py
decisions:
  - "OQ-1: _catalog_refresh() is defined but never called by any user-facing action — it is dead code. Left unmodified. The only active path is _catalog_start_async_refresh -> _CatalogRefreshWorker, which received the library filter in Task 2."
  - "OQ-2: meta_mgr passed as an explicit ctor arg (self._meta_mgr), NOT via self.parent().meta_mgr — parent is passed to QThread.__init__ and may be None in tests; the explicit arg is always available."
  - "Pitfall 5: empty _catalog_library_filter passes library_codes=None and library_sys_ids=None (not an empty set), so no 0-row filter is applied."
  - "LIBRARY_CODES added to the genizah_core import line in genizah_app.py (re-exported there from shared/browse_map_utils.py via the compat facade)."
metrics:
  duration: 16m
  completed: "2026-06-28"
  tasks_completed: 2
  files_modified: 3
---

# Phase 129 Plan 04: Desktop Catalog Library Filter (LIBFILTER-03) — Summary

**One-liner:** Desktop catalog Browse-by-Identification gains a library multi-select filter at parity with web via an explicit-meta_mgr _CatalogRefreshWorker extension + QPushButton/QMenu dropdown + removable chips.

## What Was Built

### Task 1 — Test scaffold (Task 1, RED then fixed in Task 2)

Created `tests/test_libfilter_desktop.py` with 3 gui-marked tests (LIBFILTER-03):

1. `test_worker_threads_library_filter_into_get_browse_results` — worker passes `library_codes=["CUL"]` and a non-None `library_sys_ids` set resolved from `csv_bank` to `get_browse_results`.
2. `test_worker_empty_library_filter_passes_none` — empty/None `library_filter` passes `library_codes=None` and `library_sys_ids=None` (Pitfall 5 guard).
3. `test_worker_resolution_uses_shared_helper` — asserts `resolve_library_sys_ids` is called exactly once from `run()` with the correct `library_codes` and explicit `meta_mgr`.

All 3 tests construct `_CatalogRefreshWorker(None, ..., library_filter=..., meta_mgr=fake_meta)` — `parent=None` (mirror of `test_catalog_availability_filter.py:88`) plus the new explicit `meta_mgr=` ctor arg.

Registered `"test_libfilter_desktop.py"` in `_GUI_TEST_FILES` in `tests/conftest.py` so it runs under the `gui` marker in CI's fresh-process gui-tests job.

### Task 2 — Implementation (GREEN)

Modified `genizah_app.py`:

**Worker wiring (LIBFILTER-03, OQ-2):**
- Added `library_filter=None` and `meta_mgr=None` params to `_CatalogRefreshWorker.__init__` (after `editions_filter`); stores `self._library_filter = library_filter or []` and `self._meta_mgr = meta_mgr`.
- In `run()`: after the PGP/Editions set resolution, if `self._library_filter` is truthy, imports and calls `resolve_library_sys_ids(self._library_filter, self._meta_mgr)` on the worker thread (OQ-2). Passes `library_codes=(self._library_filter or None)` and `library_sys_ids=(library_sys_ids or None)` to `get_browse_results` (Pitfall 5: empty list becomes None, never an empty set that filters to 0 rows).
- Added `LIBRARY_CODES` to the `from genizah_core import ...` line (re-exported via the compat facade from `shared/browse_map_utils.py`).

**State:**
- Added `self._catalog_library_filter = []` beside the SEED-023 filter state vars (lines 9582-9583).

**Widget (D-01, D-03):**
- Added a "Library" section label and a `QPushButton` opening a `QMenu` of checkable `QActions` (one per `LIBRARY_CODES` key), placed after the SEED-023 availability buttons in the left filter panel.
- Action labels via `get_library_display(code, short=False)` (auto-detects `CURRENT_LANG`, no English leak under Hebrew UI).
- Each action's `toggled` signal calls `_catalog_toggle_library(code, checked)`.

**State threading:**
- `_catalog_start_async_refresh` passes `library_filter=self._catalog_library_filter` AND `meta_mgr=self.meta_mgr` to `_CatalogRefreshWorker`.

**New helpers:**
- `_catalog_toggle_library(code, checked)` — adds/removes code from `self._catalog_library_filter`, calls `_catalog_update_library_filter_btn()` and triggers async refresh.
- `_catalog_update_library_filter_btn()` — updates button label: "All Libraries" when empty, "Libraries (N)" when N codes selected.
- `_sync_library_menu_checks()` — syncs QAction checked states with `_catalog_library_filter` (called from remove paths to keep menu in sync).

**Chips (_catalog_update_chips):**
- After the editions chip, renders one removable chip per selected library code labeled `get_library_display(code, short=False)  ×`; click calls `_catalog_remove_filter("library", library_code=code)`.

**_catalog_remove_filter:**
- Added `library_code=None` param.
- New `"library"` branch: removes one code (if `library_code` provided) or clears all; calls `_catalog_update_library_filter_btn()` + `_sync_library_menu_checks()`.
- `"all"` branch extended to also clear `self._catalog_library_filter` and sync the menu.

**OQ-1 resolution:**
- `_catalog_refresh()` (line 9946) is defined but never called by any user-facing code path — confirmed by grep returning only the definition. It is dead code (no filter action reaches it). Left unmodified. The only active browse-refresh path is `_catalog_start_async_refresh → _CatalogRefreshWorker`, which received the library filter.

## Verification Results

```
LIBFILTER-03: tests/test_libfilter_desktop.py     3/3  PASS
GUARD-02:     tests/test_catalog_availability_filter.py  4/4  PASS
Total: 7 passed / 0 failed
ruff check genizah_app.py tests/test_libfilter_desktop.py: All checks passed
```

## Commits

| Task | Commit | Description |
|------|--------|-------------|
| Task 1 | `28a3f830` | test(129-04): scaffold LIBFILTER-03 desktop tests + conftest gui registration (Task 1 RED) |
| Task 2 | `e64c426f` | feat(129-04): desktop catalog library filter — worker wiring, widget, chips (LIBFILTER-03) |

## Deviations from Plan

None — plan executed exactly as written.

## OQ-1 Decision (documented per plan requirement)

`_catalog_refresh()` is a synchronous non-worker path that calls `fjms.get_browse_results()` directly on the UI thread. It does NOT accept PGP/editions filter args (the SEED-023 implementation note confirms this was intentional — SEED-023 also did not modify it). A grep of the entire `genizah_app.py` confirms it is only referenced by its own `def` line — no method, signal, or button connects to it. It is dead code. Decision: **leave unmodified**. If it were ever wired up in the future, it would need the library filter args threaded through just like the worker path.

## Known Stubs

None. The widget is wired end-to-end: user toggles a QAction → `_catalog_toggle_library` → `_catalog_start_async_refresh` → `_CatalogRefreshWorker` with `library_filter` and `meta_mgr` → `resolve_library_sys_ids` on background thread → `get_browse_results(library_codes=, library_sys_ids=)` → filtered results returned via `done` signal.

## Threat Surface Scan

No new network endpoints, auth paths, or schema changes. The library filter uses the existing `_ensure_filter_temp` parameterized-insert pattern (T-129-08 mitigated: codes validated against `LIBRARY_CODES` keys inside `resolve_library_sys_ids`; T-129-09 mitigated: O(255K) resolution runs on the background QThread via explicit `self._meta_mgr`). No new packages.

## Self-Check: PASSED

- `tests/test_libfilter_desktop.py` exists: YES
- `tests/conftest.py` contains `"test_libfilter_desktop.py"` in `_GUI_TEST_FILES`: YES
- `genizah_app.py` contains `self._catalog_library_filter`: YES
- `genizah_app.py` `_CatalogRefreshWorker.__init__` accepts `library_filter` AND `meta_mgr`: YES
- `genizah_app.py` run() uses `self._meta_mgr` (not `self.parent().meta_mgr`): YES
- `_catalog_start_async_refresh` passes `library_filter=self._catalog_library_filter` AND `meta_mgr=self.meta_mgr`: YES
- `_catalog_update_chips` and `_catalog_remove_filter` have library branch: YES
- `get_library_display(...short=False...)` used in chips and menu: YES
- Commits `28a3f830` and `e64c426f` exist: YES
- 7 tests pass: YES
- ruff clean: YES
