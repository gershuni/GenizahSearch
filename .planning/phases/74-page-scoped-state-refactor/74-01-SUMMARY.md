---
phase: 74
plan: 01
subsystem: web
tags: [web, state, persistence, refactor, WEBM-03]
requirements: [WEBM-03]
dependency-graph:
  requires: []
  provides:
    - "persist_search_snapshot, restore_search_snapshot, clear_search_snapshot on web/pages/search_state.py"
    - "persist_browse_snapshot, restore_browse_snapshot, clear_browse_snapshot on web/pages/browse_state.py"
    - "_SEARCH_SNAPSHOT_VERSION=1, _BROWSE_SNAPSHOT_VERSION=1 schema stamps"
    - "Wave 0 test stubs: tests/test_search_state.py (3 tests green), tests/test_browse_bootstrap.py (3 skipped stubs), tests/e2e/test_browse_flow.py::test_shelfmark_navigation_updates_url (skipped stub)"
  affects:
    - web/pages/search.py
    - web/pages/browse.py
tech-stack:
  added: []
  patterns:
    - "Page-scoped snapshot helper triple (restore_/persist_/clear_) as sole owner of restorable storage keys"
    - "Version-stamped snapshots - stale version silently discards and resets"
key-files:
  created:
    - tests/test_search_state.py
    - tests/test_browse_bootstrap.py
  modified:
    - web/pages/search_state.py
    - web/pages/browse_state.py
    - web/pages/search.py
    - web/pages/browse.py
    - tests/e2e/test_browse_flow.py
decisions:
  - "persist_search_snapshot owns the cap-at-1000 + strip-full_text logic (moved from inline at search.py:4171-4190) to keep all snapshot serialization in one place"
  - "clear_browse_snapshot in exit_joined_view also drops browse_position (intentional - state reset on view exit)"
  - "_persist_reading_desk_state() kept as thin shim calling persist_browse_snapshot(state, state.current_page) - preserves existing call sites without churn"
metrics:
  duration: "~15m executor time"
  tasks: 3
  files_touched: 6
  commits: 3
  completed: 2026-04-17
---

# Phase 74 Plan 01: Persistence Boundary Helpers Summary

Introduced the Phase 74 persistence boundary: page-scoped snapshot helper triples on `search_state.py` and `browse_state.py`, version-stamped snapshots (D-04 stale-version discard), and migration of all in-scope direct `app.storage.user[...]` writes to go through those helpers. Legacy storage key names preserved (D-08).

## What Was Built

### Wave 0 Test Stubs (Task 1 - commit `03b71edc`)

Three new/augmented test files driving Task 2 implementation and Plans 74-02 / 74-03:

- **`tests/test_search_state.py` (new, 3 tests)** - `test_persist_and_restore_round_trip`, `test_clear_snapshot_wipes_all_keys`, `test_stale_version_discards_snapshot`. All 3 pass after Task 2.
- **`tests/test_browse_bootstrap.py` (new, 3 tests)** - `test_explicit_sys_id_beats_saved_position`, `test_blank_browse_restores_saved_position`, `test_reading_desk_restore_wins_over_position`. Currently skip gracefully (`resolve_browse_bootstrap` not yet implemented, Plan 74-02 target).
- **`tests/e2e/test_browse_flow.py`** - added `test_shelfmark_navigation_updates_url` stub inside existing `TestBrowseNavigation` class. Skips with `"Cat-1 conversion pending - Plan 74-03"`.

### Snapshot Helper Triples (Task 2 - commit `73a71a4d`)

**`web/pages/search_state.py`:**
- `_SEARCH_SNAPSHOT_VERSION = 1`
- `restore_search_snapshot(state)` - hydrates state from storage; discards on version mismatch
- `persist_search_snapshot(state)` - serializes restorable fields (includes cap-at-1000 + strip-full_text for `results`)
- `clear_search_snapshot()` - wipes snapshot + filter keys with correct live-field-type defaults (`filter_include_mode: True`, `filter_measurement_material: []`)
- Key buckets documented inline (`_SEARCH_SNAPSHOT_KEYS`, `_SEARCH_FILTER_KEYS`, `_SEARCH_FILTER_MEASUREMENT_KEYS`)

**`web/pages/browse_state.py`:**
- `_BROWSE_SNAPSHOT_VERSION = 1`
- `restore_browse_snapshot(state) -> (saved_position, saved_desk)` - returns both raw dicts for Plan 74-02's `resolve_browse_bootstrap` to consume without double-reading storage (Gemini review-revision #15)
- `persist_browse_snapshot(state, page=None)` - serializes `browse_position` + `reading_desk_state`
- `clear_browse_snapshot()` - drops both keys + version stamp
- `logger` + `from nicegui import app` added to module

### Write-Site Migration (Task 3 - commit `19c5464c`)

**`web/pages/search.py`** call sites migrated:

| Line (before) | Old write | New call |
|---|---|---|
| 130 | `app.storage.user['domain_exclusions'] = []` (initial_domain clear) | `persist_search_snapshot(search_state)` |
| 806-832 | 23-line filter reset block (in `_clear_all_adv_filters`) | `clear_search_snapshot()` (measurement state resets on `search_state` kept inline) |
| 2019-2025 | 7-line reset block on New Search | `clear_search_snapshot()` + 2 bootstrap-input writes kept (`search_query = ''`, `search_mode = 'exact'`) |
| 3012 | `app.storage.user['domain_exclusions'] = list(excluded)` (apply_filter) | `persist_search_snapshot(search_state)` |
| 4171-4199 | 20-line cap-and-strip + `search_results` write | `persist_search_snapshot(search_state)` (logic moved into helper) |

**`web/pages/browse.py`** call sites migrated:

| Line (before) | Old write | New call |
|---|---|---|
| 777-785 | `browse_position` dict write in `load_page` | `persist_browse_snapshot(state, page)` |
| 981-984 | `app.storage.user.pop('reading_desk_state')` in `exit_joined_view` | `clear_browse_snapshot()` (also drops `browse_position`, intentional on view exit) |
| 1056-1074 | full `_persist_reading_desk_state()` body | thin shim: `persist_browse_snapshot(state, state.current_page)` |

## Remaining Direct `app.storage.user[...]` Writes (In Scope, Intentional)

**`web/pages/search.py`** (9 writes, all bootstrap-input keys or UX):
- `search_query`, `search_mode`, `search_preset`, `search_max_changes`, `search_gap`, `search_text_position` - bootstrap-input keys, not `SearchUIState` fields; feed `resolve_search_bootstrap`. Explicitly out of scope per `must_haves.truths[0]`.
- search.py:2009-2010 `search_query=''` / `search_mode='exact'` on New Search - kept per plan for UX (wipe query bar).

**`web/pages/browse.py`** (1 write):
- `browse_export_data` (line 1151) - ephemeral cross-page handoff signal, classified `runtime_only` per RESEARCH §1.2.

## Test Results

- `pytest tests/test_search_state.py -x` - **3 passed** (round-trip, clear, stale-version all green)
- `pytest tests/test_browse_bootstrap.py` - **3 skipped** (resolve_browse_bootstrap not yet implemented, Plan 74-02)
- `pytest tests/ --ignore=tests/e2e` - **1070 passed, 8 skipped** (baseline 1067 passed, 5 skipped + 3 new passing + 3 browse_bootstrap skipping)
- Web smoke (deferred - Windows dev box; user to verify)

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Cap-and-strip semantics moved into helper instead of direct override**

- **Found during:** Task 3, wiring search.py:4171-4199 (the results persistence block)
- **Issue:** The existing code wrote a capped+stripped copy of `results` directly to `app.storage.user['search_results']`. The plan's `persist_search_snapshot` wrote `state.results` verbatim, which would (a) explode the WS message size since `state.results` holds full-text, and (b) require a post-helper direct override that violates the plan's "helpers are sole owners" rule.
- **Fix:** Moved the cap-at-1000 + strip-full_text logic into `persist_search_snapshot` itself. The helper is now the single owner of `search_results` storage, and callers pass `search_state` without needing to know about the stripping.
- **Files modified:** `web/pages/search_state.py` (added capping inside helper), `web/pages/search.py` (call site reduced to `persist_search_snapshot(search_state)`)
- **Commit:** `19c5464c`

## Notes for Plans 74-02 / 74-03

- `restore_search_snapshot` exists but is NOT yet called from `search.py:95-161` - that session-restore block still reads bootstrap-input keys + snapshot keys directly (by design per `must_haves.truths[1]` - the split is intentional). A future consolidation is possible but not required.
- `restore_browse_snapshot` returns `(saved_position, saved_desk)` tuple - ready for Plan 74-02 to thread into `resolve_browse_bootstrap`. Current browse.py bootstrap block at ~4471 still reads storage directly; that's 74-02's scope.
- Version stamps are live on any new persist; a snapshot written by this plan will survive a restart, but pre-existing snapshots from older sessions have no `search_snapshot_schema_version` key and will be silently discarded on first restore (graceful degradation, no user-visible breakage).

## Self-Check: PASSED

Verified commits exist in git log:
- `03b71edc` test(74-01): add Wave 0 test stubs for snapshot helpers
- `73a71a4d` feat(74-01): add snapshot helper triples to search_state and browse_state
- `19c5464c` refactor(74-01): route snapshot writes through helpers in search.py/browse.py

Verified files exist:
- `tests/test_search_state.py` FOUND
- `tests/test_browse_bootstrap.py` FOUND
- `web/pages/search_state.py` contains `_SEARCH_SNAPSHOT_VERSION = 1`, `persist_search_snapshot`, `restore_search_snapshot`, `clear_search_snapshot`
- `web/pages/browse_state.py` contains `_BROWSE_SNAPSHOT_VERSION = 1`, `persist_browse_snapshot`, `restore_browse_snapshot`, `clear_browse_snapshot`
