---
phase: 132-public-api-dual-mode-api-search-api-parallels
plan: "01"
subsystem: public-api
tags: [tdd, red-scaffold, library-filter, api, dmf-11]
dependency_graph:
  requires: []
  provides: [tests/test_search_api_library_mode.py]
  affects: [web/search_api.py, shared/fjms_service.py]
tech_stack:
  added: []
  patterns: [pytest-fixture-isolation, lazy-import-guard, monkeypatch-setattr-raising-false]
key_files:
  created:
    - tests/test_search_api_library_mode.py
  modified: []
decisions:
  - "test_omit_mode_equals_include asserts echo contains NO library_filter_mode key (default=None drops it via exclude_none=True) — backward-compat pin per Codex R1 HIGH"
  - "resolve_library_complement_sys_ids imported lazily inside test bodies (not at module top) so collection does not error before Plan 02 creates the helper"
  - "monkeypatch.setattr(..., raising=False) used for complement helper stub installs — the attr does not yet exist; raising=False installs the stub without AttributeError"
  - "test_invalid_mode_returns_400 is incidentally GREEN pre-Plan-02 because extra='forbid' rejects unknown key as invalid_request — this is correct behavior for the right reason after Plan 02 adds the Literal field"
metrics:
  duration: "14 minutes"
  completed: "2026-07-01"
  tasks_completed: 2
  files_created: 1
  files_modified: 0
---

# Phase 132 Plan 01: Wave 0 RED Test Scaffold Summary

Wave 0 RED test scaffold for `library_filter_mode` (DMF-11): creates the pytest contract file before any production code is written.

## What Was Built

`tests/test_search_api_library_mode.py` — 9 named tests covering all DMF-11 behaviors on both `/api/search` and `/api/parallels`. Tests are RED until Plan 02 ships the `FiltersModel.library_filter_mode` field, `resolve_library_complement_sys_ids` helper, and `_intersect_library_filter` exclude branch.

## Task Results

### Task 1: Endpoint-level dual-mode tests

Created 7 endpoint tests:

| Test | Covers | Status |
|------|--------|--------|
| `test_include_mode_is_default_same_as_omitted` | DMF-11-1: explicit include == omitted; both route through resolve_library_sys_ids | RED (field missing — extra='forbid' rejects it) |
| `test_omit_mode_equals_include` | DMF-11-1: backward-compat pin — echo has NO `library_filter_mode` key when field omitted | GREEN (echo already byte-for-byte correct; field doesn't exist so it's never injected) |
| `test_exclude_restricts_to_complement` | DMF-11-2: exclude → complement helper called, include helper NOT called | RED (field missing) |
| `test_include_vs_exclude_disjoint` | DMF-11-2: same library=['CUL'] under include vs exclude produces disjoint sets | RED (field missing) |
| `test_parallels_exclude_mode` | DMF-11-2: /api/parallels honors mode at parity with /api/search | RED (field missing) |
| `test_invalid_mode_returns_400` | DMF-11-3: invalid mode value → 400 invalid_request on BOTH endpoints | GREEN (extra='forbid' catches unknown key as invalid_request — correct for right reason) |
| `test_mode_without_library_is_noop` | DMF-11-3: mode without library list → no filter, still 200 | RED (field missing — extra='forbid' rejects it before noop logic runs) |

### Task 2: Complement-helper unit tests

Created 2 helper-level unit tests:

| Test | Covers | Status |
|------|--------|--------|
| `test_resolve_library_complement_sys_ids` | Helper correctness: exact NOT-in-set semantics, empty→set(), exact-complement invariant (union==all, intersection==∅) | RED (helper does not exist yet) |
| `test_intersect_helper_exclude_branch` | `_intersect_library_filter` exclude branch: routes to complement helper, returns intersection, does NOT call include helper | RED (exclude branch not wired yet) |

## RED Run Results (Wave 0 — expected)

```
9 tests collected, 0 collection errors
7 FAILED, 2 passed
```

**7 RED (expected):** The production code does not yet exist. Plan 02 will make all 9 tests green.

**2 incidentally green:**
- `test_omit_mode_equals_include` — backward-compat already holds (field never injected into echo)
- `test_invalid_mode_returns_400` — `FiltersModel.extra='forbid'` already rejects unknown keys as `invalid_request`; once Plan 02 adds the Literal field, an invalid Literal value still yields 400 via `PydanticValidationError`

## Verification

- `python -m pytest tests/test_search_api_library_mode.py --co -q` → 9 tests collected, 0 collection errors
- `/api/search` path present: yes
- `/api/parallels` path present: yes  
- No LOCAL handling in any test: confirmed (no `'LOCAL'` string in file)
- `grep -c "def test_" tests/test_search_api_library_mode.py` → 9 (>= 8 per plan requirement)

## Commits

| Task | Hash | Message |
|------|------|---------|
| Tasks 1+2 (both in one commit — same file) | 0da02e11 | test(132-01): add Wave 0 RED test scaffold for library_filter_mode (DMF-11) |

## Deviations from Plan

None — plan executed exactly as written.

The two tests that are incidentally green (`test_omit_mode_equals_include`, `test_invalid_mode_returns_400`) were anticipated in the plan commentary and are acceptable pre-Plan-02 behavior. They will remain green after Plan 02 for the right reasons.

## Known Stubs

None. This is a test-only plan; no production stubs were created.

## Threat Flags

None — test scaffolding only; no new network surface introduced.

## Self-Check: PASSED

- [x] `tests/test_search_api_library_mode.py` exists (created)
- [x] Commit `0da02e11` exists in git log
- [x] 9 tests collect cleanly
- [x] 7 RED, 2 green (as expected for Wave 0)
