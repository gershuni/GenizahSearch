---
phase: 132-public-api-dual-mode-api-search-api-parallels
plan: "02"
subsystem: public-api
tags: [tdd, green, library-filter, api, dmf-11, pydantic, fjms_service]
dependency_graph:
  requires:
    - phase: 132-01
      provides: tests/test_search_api_library_mode.py (9 RED tests — the contract)
  provides:
    - FiltersModel.library_filter_mode field (Optional[Literal['include','exclude']], default=None)
    - resolve_library_complement_sys_ids helper in shared/fjms_service.py
    - _intersect_library_filter exclude branch in web/search_api.py
  affects:
    - web/search_api.py
    - shared/fjms_service.py
tech-stack:
  added: []
  patterns:
    - "Optional[Literal[...]] = Field(default=None) for backward-compat enum fields that must be absent from echo when omitted"
    - "run_in_executor off-event-loop pattern for both include and exclude resolve helpers"
    - "single-pass complement helper (not in excl_set) mirrors include helper (in valid_code_set)"
key-files:
  created: []
  modified:
    - shared/fjms_service.py
    - web/search_api.py
key-decisions:
  - "default=None (NOT 'include') so model_dump(exclude_none=True) drops the field when omitted — echo stays byte-for-byte identical for existing callers (Codex R1 HIGH)"
  - "_intersect_library_filter reads mode AFTER the `if not libs` short-circuit so exclude+empty-library is a no-op without any resolver call"
  - "resolve_library_complement_sys_ids is a module-level function in shared/fjms_service.py (not inlined) for testability and naming symmetry with resolve_library_sys_ids"
  - "exclude branch late-binds via _fjms_module (same as include) so monkeypatching works in tests"
requirements-completed: [DMF-11]
duration: 18min
completed: 2026-07-01
---

# Phase 132 Plan 02: Wave 2 Implementation Summary

**Dual-mode library filter on both public API endpoints: `library_filter_mode='exclude'` routes a single-pass complement helper off the event loop, backward-compatibly via `default=None` on `FiltersModel`.**

## Performance

- **Duration:** 18 min
- **Started:** 2026-07-01T12:50:00Z
- **Completed:** 2026-07-01T13:08:00Z
- **Tasks:** 2
- **Files modified:** 2

## Accomplishments

- `resolve_library_complement_sys_ids` added to `shared/fjms_service.py` — single O(255K) pass over csv_bank with `not in excl_set`, full corpus on empty/unknown codes, fail-open on ImportError
- `FiltersModel.library_filter_mode: Optional[Literal['include','exclude']] = Field(default=None)` added to `web/search_api.py`, covering both `/api/search` and `/api/parallels` (shared model, D1)
- `_intersect_library_filter` exclude branch runs complement helper via `run_in_executor` (same off-event-loop pattern as include); include path unchanged
- Wave 0 suite (9 tests) turned fully GREEN; all 165 regression tests remain GREEN

## Task Commits

1. **Task 1: Add resolve_library_complement_sys_ids** — `ae232d49` (feat)
2. **Task 2: Add library_filter_mode field + wire exclude branch** — `dc0c5033` (feat)

## Files Created/Modified

- `shared/fjms_service.py` — new `resolve_library_complement_sys_ids` function (56 lines added after `resolve_library_sys_ids`)
- `web/search_api.py` — `library_filter_mode` field on `FiltersModel` + updated `library` field description + exclude branch in `_intersect_library_filter`

## Decisions Made

- `default=None` (not `'include'`): ensures `model_dump(exclude_none=True)` drops the key when omitted, keeping the request echo byte-for-byte identical. `_intersect_library_filter` normalizes `None → 'include'` internally via `or 'include'`.
- Separate module-level function (not inlined): mirrors the naming convention of `resolve_library_sys_ids`, is independently testable, and the helper docstring calls out the threading requirement and the `exclude` context explicitly.
- Mode read AFTER `if not libs` short-circuit: `library_filter_mode='exclude'` with no `library` list is a clean no-op — neither resolver is called.

## Deviations from Plan

None — plan executed exactly as written.

## Issues Encountered

None.

## Known Stubs

None. Both helpers are fully wired; no placeholder values.

## Threat Flags

None — no new network endpoints or trust boundaries introduced. The `library_filter_mode` field is validated structurally by Pydantic `Literal` + `extra='forbid'` (T-132-01 mitigated). The complement scan has the same O(255K) cost profile as the include scan (T-132-02 accepted).

## Next Phase Readiness

- Wave 0 RED suite is now GREEN (9/9)
- Backward-compat: existing callers unaffected (echo byte-for-byte unchanged)
- Phase 132 Plan 03 (docs + skill contract update) can proceed
- DMF-11 implementation complete; docs/SEARCH_API.md + skills/cairo-genizah-research/references/api_contract.md remain to be updated (Plan 03)

## Self-Check: PASSED

- [x] `shared/fjms_service.py` modified (resolve_library_complement_sys_ids exists — `grep -c "def resolve_library_complement_sys_ids" shared/fjms_service.py` == 1)
- [x] `web/search_api.py` modified (library_filter_mode field — `grep -c "library_filter_mode" web/search_api.py` == 4)
- [x] Commit `ae232d49` exists (Task 1)
- [x] Commit `dc0c5033` exists (Task 2)
- [x] `tests/test_search_api_library_mode.py` — 9/9 GREEN
- [x] `tests/test_search_api_v2.py` + `tests/test_parallels_api.py` + `tests/test_parallels_library_filter.py` — 158 passed, 7 skipped (regressions GREEN)
- [x] `ruff check web/search_api.py shared/fjms_service.py` — clean

---
*Phase: 132-public-api-dual-mode-api-search-api-parallels*
*Completed: 2026-07-01*
