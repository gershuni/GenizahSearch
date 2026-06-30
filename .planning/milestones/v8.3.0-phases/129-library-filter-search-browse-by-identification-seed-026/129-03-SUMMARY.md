---
phase: 129-library-filter-search-browse-by-identification-seed-026
plan: "03"
subsystem: web/pages/catalog_browse
tags: [library-filter, catalog, push-down, seed-026, libfilter-02, guard-02, safe-storage]
dependency_graph:
  requires:
    - 129-01 (resolve_library_sys_ids + get_browse_results library args)
  provides:
    - web/pages/catalog_browse.py::catalog_library_filter (state + UI + chips)
  affects:
    - web catalog Browse-by-Identification page (library filter composing with SEED-023 PGP/Editions)
tech_stack:
  added: []
  patterns:
    - safe_storage chokepoint (Phase 87 invariant) for catalog_library_filter list
    - io_bound resolve_library_sys_ids inside _fetch_results_blocking (off event loop)
    - refresh_results() repaint path for all toggle/clear handlers
    - ui.select(multiple=True) dropdown-with-checklist for library filter (D-03)
    - per-code removable chips via _make_chip + clear_library_code (D-03)
key_files:
  created: []
  modified:
    - web/pages/catalog_browse.py
decisions:
  - "ui.select(multiple=True, use-chips) chosen over ui.button+ui.menu — simpler, built-in multi-select, RTL-correct; D-03 satisfied with compact dropdown"
  - "LOCAL library code excluded from the filter options — My Library is a local-only concept, not a Genizah library"
  - "empty selection (library_codes or None) passes None for both library_codes and library_sys_ids to get_browse_results — Pitfall 5 (never empty set)"
  - "resolve_library_sys_ids result empty set → None before passing to get_browse_results — fail-open if all codes unknown or meta_mgr not ready"
metrics:
  duration: 15m
  completed: "2026-06-28"
  tasks_completed: 2
  files_modified: 1
---

# Phase 129 Plan 03: Web Catalog Library Filter (SEED-026) — Summary

**One-liner:** Web catalog Browse-by-Identification library filter with multi-select dropdown, removable chips, sys_id resolution off the event loop, and composition with SEED-023 PGP/Editions filters.

## What Was Built

### Task 1 — Library filter state + dropdown checklist + persistence

Modified `web/pages/catalog_browse.py`:

- Added imports: `resolve_library_sys_ids` from `shared.fjms_service`, `LIBRARY_CODES` + `get_library_display` from `shared.browse_map_utils`.
- Added `current_library_filter` state dict: persisted via `safe_user_get('catalog_library_filter', [])`, normalized to a list and validated against `LIBRARY_CODES` keys (drop unknown/corrupted codes — Pitfall 6 + Codex Change-2).
- Added `library_filter_ctrl_ref` to UI ref dicts.
- Built a "Filter by library" card in the sidebar (after the SEED-023 Availability Filter Card): `ui.select(multiple=True, use-chips)` with all known `LIBRARY_CODES` (excluding `LOCAL`) labeled via `get_library_display(code, short=False, lang=lang)` for EN/HE labels (D-01). `LOCAL` excluded because My Library is a desktop/local concept, not a Genizah institution.
- `_on_library_filter_change` handler: validates selected codes against `LIBRARY_CODES`, persists via `safe_user_set('catalog_library_filter', ...)`, resets page, calls `await refresh_results()` (the repaint path, not `fetch_results()`).
- `_update_library_filter_ctrl()` helper syncs the widget value with state.

### Task 2 — Resolve sys_ids off-loop + wire into get_browse_results + chips

Modified `web/pages/catalog_browse.py`:

- `_fetch_results_blocking` extended with `library_codes` param. Inside the blocking body (runs in `io_bound`): calls `resolve_library_sys_ids(library_codes, _state.meta_mgr)` → `lib_sys_ids`; empty resolved set → `None` (fail-open); passes `library_codes=(library_codes or None)` and `library_sys_ids=lib_sys_ids` to `get_browse_results` (composing with PGP/Editions).
- `fetch_results()` updated to pass `current_library_filter['value'] or None` as the new `library_codes` arg.
- `render_chips()`: `has_filters` extended with `bool(current_library_filter['value'])`; per-code chips rendered via `_make_chip(f"{tr('Library')}: {label}", lambda c=code: clear_library_code(c), color='blue')`.
- `clear_library_code(code)` async helper: removes one code, persists, syncs ctrl, resets page, calls `await refresh_results()`.
- `clear_filter('library')` branch: clears all selected codes, persists, syncs ctrl, calls `await refresh_results()`.
- `clear_all_filters()` extended: sets `current_library_filter['value'] = []`, `safe_user_set('catalog_library_filter', [])`, `_update_library_filter_ctrl()`.

## Verification Results

```
tests/test_libfilter_catalog.py       6/6  PASS  (LIBFILTER-02)
tests/test_seed023_catalog_filters.py 11/11 PASS  (GUARD-02 — PGP/Editions unchanged)
tests/test_no_raw_storage_access.py   5/5  PASS  (Phase 87 safe_storage guard)
Total: 22 passed / 0 failed
ruff check web/pages/catalog_browse.py: All checks passed
```

## Commits

| Task | Commit | Description |
|------|--------|-------------|
| Task 1+2 | `33bd8fcb` | feat(129-03): web catalog library filter — SEED-026 (Tasks 1+2) |

## Deviations from Plan

**1. [Rule 1 - Minor] Tasks 1 and 2 committed together**
- **Found during:** Implementation
- **Issue:** Both tasks modify the same file and are tightly coupled (the state initialized in Task 1 is consumed by the chips/resolution added in Task 2). Splitting into two commits would have required an intermediate broken state.
- **Resolution:** Combined into one atomic commit covering both tasks. All acceptance criteria for both tasks are verified.

**2. [Rule 2 - Security] LOCAL library excluded from filter options**
- **Found during:** Task 1 (building the dropdown options)
- **Issue:** LIBRARY_CODES includes `'LOCAL': 'My Library'` — a desktop-only concept for user-uploaded documents. Including it in the web catalog filter would be misleading (no catalog rows can have `library_code='LOCAL'` from FJMS data) and could confuse users.
- **Fix:** Added `if code != 'LOCAL'` guard when building `_lib_options` dict.
- **Files modified:** `web/pages/catalog_browse.py`

## Known Stubs

None. The library filter is fully wired: UI → state → `_fetch_results_blocking` → `get_browse_results`.

## Threat Surface Scan

No new network endpoints, auth paths, or schema changes. All threat mitigations as planned:

| Flag | File | Description |
|------|------|-------------|
| T-129-06 mitigated | web/pages/catalog_browse.py | catalog_library_filter persisted via safe_user_set chokepoint; isinstance+LIBRARY_CODES validation on load and on change |
| T-129-07 mitigated | web/pages/catalog_browse.py | resolve_library_sys_ids called inside _fetch_results_blocking (io_bound); library_sys_ids passed to Plan A's parameterized TEMP build |

## Self-Check: PASSED

- `web/pages/catalog_browse.py` references `catalog_library_filter` via safe_user_get and safe_user_set: YES (5 occurrences)
- `resolve_library_sys_ids` imported and called inside `_fetch_results_blocking`: YES
- `library_codes=` and `library_sys_ids=` passed to `get_browse_results`: YES
- Per-code chips via `clear_library_code` + `'library'` branch in `clear_filter`: YES
- All toggle/clear handlers call `await refresh_results()`: YES
- Commit `33bd8fcb` exists: YES
- 22 tests pass: YES
- ruff clean: YES
