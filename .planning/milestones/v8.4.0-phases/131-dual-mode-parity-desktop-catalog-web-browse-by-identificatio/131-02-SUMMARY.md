---
phase: "131"
plan: "02"
subsystem: shared/fjms_service
tags: [library-filter, dual-mode, fjms, shared, backend, sql]
dependency_graph:
  requires: [131-01]
  provides: [get_browse_library_facets, library_mode_param, _build_browse_conditions]
  affects: [shared/fjms_service.py, tests/test_fjms_browse_library_mode.py]
tech_stack:
  added: []
  patterns:
    - "_build_browse_conditions extracted helper — shared WHERE condition factory for get_browse_results and get_browse_library_facets"
    - "EXISTS/NOT EXISTS SQL keyword dispatch via library_mode == 'hide' strict equality check"
    - "SELECT DISTINCT c.AlmaId — SQL-bounded per-library DISTINCT-manuscript counting"
    - "Caller-supplied CALLABLE sys_id_to_library pattern — service does not own the sys_id->library mapping"
key_files:
  created:
    - tests/test_fjms_browse_library_mode.py
  modified:
    - shared/fjms_service.py
decisions:
  - "Factored _build_browse_conditions as a private helper so both get_browse_results and get_browse_library_facets share one source of truth for the non-library WHERE conditions (domain/author/work/date/text/pgp/editions)"
  - "library_mode uses strict equality ('hide') fallback to show_only for any unrecognized value (fail-safe)"
  - "_build_browse_conditions intentionally excludes the library filter — facets count across ALL reachable libraries under the current non-library filters"
  - "sys_id_to_library is a CALLABLE (not a dict): matches the bound-method form Plan 04 passes (meta_mgr.get_library_for_id)"
  - "SELECT DISTINCT c.AlmaId instead of GROUP BY — mirrors COUNT(DISTINCT) in the live browse query"
metrics:
  duration: "~20 minutes"
  completed: "2026-06-30"
  tasks_completed: 3
  files_changed: 2
---

# Phase 131 Plan 02: Shared library_mode param + get_browse_library_facets Summary

Backward-compatible `library_mode` parameter on `get_browse_results` (EXISTS/NOT EXISTS SQL dispatch) plus a new `get_browse_library_facets` callable-mapper method returning true full-set per-library DISTINCT-AlmaId counts.

## What Was Built

### Task 1: RED test scaffold (18 tests)
- SQL-shape contract tests asserting `library_mode == 'hide'` gates NOT EXISTS on the `_browse_filter_library` temp table
- Facet contract tests: `_build_browse_conditions` helper exists and is shared; `get_browse_library_facets` defined; `DISTINCT c.AlmaId` counting; `sys_id_to_library` parameter accepted
- Behavioral tests: duplicate AlmaId counted once, off-page libraries surface with correct counts, CALLABLE mapper contract (dict.get bound method + recording closure), LOCAL never a key, empty/None codes skipped, non-callable mapper -> {}

### Task 2: library_mode param on get_browse_results (GREEN for SQL-shape tests)
- Added `library_mode: str = 'show_only'` to the signature (backward-compatible — existing callers pass nothing and retain Show-only/EXISTS behavior)
- SQL condition uses `_exists_kw = "NOT EXISTS" if library_mode == "hide" else "EXISTS"` — unrecognized values fall through to EXISTS (fail-safe)
- Fail-open `elif library_codes and not library_sys_ids` path unchanged
- Extended docstring to describe the new param

### Task 3: _build_browse_conditions helper + get_browse_library_facets (all 18 tests GREEN)
- Extracted `_build_browse_conditions(domain, author, work, date, text, pgp, editions)` private helper from the `get_browse_results` body — pure structural extraction, zero behavior change
- `get_browse_results` now calls the helper — all 4 existing `test_catalog_availability_filter.py` tests still pass
- Added `get_browse_library_facets(... sys_id_to_library=None)` that:
  - Calls `_build_browse_conditions` WITHOUT the library filter (so facets count ALL reachable libraries under active non-library filters)
  - Runs `SELECT DISTINCT c.AlmaId FROM catalog c{where}` — SQL-bounded, mirrors `COUNT(DISTINCT c.AlmaId)` counting
  - Invokes `sys_id_to_library(alma_id)` CALLABLE per distinct AlmaId
  - Guards: `_conn is None -> {}`, `sys_id_to_library is None -> {}`, `not callable(...) -> {}`, `LOCAL` and falsy codes skipped

## Deviations from Plan

None — plan executed exactly as written. The one minor adaptation was updating source-scan test assertions to match actual Python quote style (`"hide"` double-quotes vs `'hide'` single-quotes) and to handle the case where the method docstring mentions `_browse_filter_library` as an explanatory comment without applying it as a filter.

## Verification

- `pytest tests/test_fjms_browse_library_mode.py -q` — 18/18 passed
- `pytest tests/test_catalog_availability_filter.py -q` — 4/4 passed (get_browse_results unchanged)
- `python -m ruff check shared/fjms_service.py tests/test_fjms_browse_library_mode.py` — clean

## Known Stubs

None. This plan implements shared service layer code — no UI stubs.

## Threat Flags

None. No new network endpoints, auth paths, or schema changes. The `library_mode` string controls only a fixed SQL keyword (EXISTS vs NOT EXISTS), never free-text SQL interpolation. The `sys_id_to_library` callable guard (`callable(...)`) is in place.

## Self-Check
