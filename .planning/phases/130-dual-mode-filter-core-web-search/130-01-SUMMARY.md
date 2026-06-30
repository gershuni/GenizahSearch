---
phase: 130-dual-mode-filter-core-web-search
plan: "01"
subsystem: web-search-state
tags: [library-filter, dual-mode, state-model, safe_storage]
dependency_graph:
  requires: []
  provides: [library_mode model field, mode-aware clear_search_snapshot reset]
  affects: [web/pages/search_state.py]
tech_stack:
  added: []
  patterns: [sibling-filter-field pattern (printed_filter/pgp_filter analog), safe_storage chokepoint]
key_files:
  created: []
  modified:
    - web/pages/search_state.py
decisions:
  - "library_mode defaults to 'hide' (D-05: Hide mode with empty set = show all for fresh users)"
  - "clear_search_snapshot resets search_library_filter to {'mode':'hide','codes':[]} (D-09 dict shape, D-06 DMF-06)"
  - "library_filter comment updated to reflect dual-mode semantics (active code set for either mode)"
metrics:
  duration_minutes: 5
  completed_date: "2026-06-30"
  tasks_completed: 2
  tasks_total: 2
  files_changed: 1
---

# Phase 130 Plan 01: SearchUIState library_mode field + clear_search_snapshot reset Summary

**One-liner:** Added `library_mode: str = 'hide'` to SearchUIState and updated `clear_search_snapshot` to reset the library filter as a `{'mode':'hide','codes':[]}` dict (D-09 persistence shape, D-05/DMF-06 defaults).

## Tasks Completed

| Task | Name | Commit | Files |
|------|------|--------|-------|
| 1 | Add library_mode field to SearchUIState | 65eeca9f | web/pages/search_state.py |
| 2 | Make clear_search_snapshot reset to (mode+set) shape | 4b258c71 | web/pages/search_state.py |

## What Was Built

**Task 1 — library_mode field:**
- Added `self.library_mode: str = 'hide'` immediately after the existing `library_filter` declaration (~line 62)
- Updated the `library_filter` comment to reflect dual-mode semantics: "active code set for the current mode (hide-set in Hide mode, allowlist in Show-only mode)"
- Inline comment documents valid values `'show_only'` | `'hide'` and the D-05 fresh-user default
- Mirrors the sibling `printed_filter`/`pgp_filter` string-with-sentinel pattern exactly

**Task 2 — clear_search_snapshot reset shape:**
- Changed `'search_library_filter': []` to `'search_library_filter': {'mode': 'hide', 'codes': []}` in the `defaults` dict
- D-09 single-key dict shape: both mode and code set reset in one stored value
- D-05 fresh default: Hide mode with empty hide-set = show all
- All writes remain on the `safe_user_set` chokepoint (Phase 87 invariant preserved)

## Verification Results

- `SearchUIState().library_mode == 'hide'` — PASSED
- `SearchUIState().library_filter == []` — PASSED
- New reset shape `{'mode': 'hide', 'codes': []}` present in source — PASSED
- `pytest tests/test_no_raw_storage_access.py` — 6 passed (allowlist stays `[]`)
- `pytest tests/test_phase_97_invariants.py` — 4 passed

## Deviations from Plan

None — plan executed exactly as written.

## Known Stubs

None. This plan defines model-layer state shape only; no behavior is wired.

## Threat Surface Scan

No new network endpoints, auth paths, file access patterns, or schema changes introduced. The only change is a default value in per-user device-local safe_storage (LOW severity, T-130-01-01/T-130-01-02 in plan threat register — both mitigated by chokepoint routing and Plan 02's sanitize path).

## Self-Check: PASSED

- `web/pages/search_state.py` — modified (exists, verified)
- Commit `65eeca9f` — exists in git log
- Commit `4b258c71` — exists in git log
- All acceptance criteria met
