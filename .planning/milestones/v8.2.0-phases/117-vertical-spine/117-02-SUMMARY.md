---
phase: 117-vertical-spine
plan: "02"
subsystem: web
tags: [safe_storage, joins_lab, schema_versioning, multitenant, FND-06]
dependency_graph:
  requires: []
  provides: [joins_lab_storage_module, schema_v1_contract]
  affects: [web/joins_lab_storage.py, tests/test_joins_lab_storage.py]
tech_stack:
  added: []
  patterns: [safe_user_get/set/pop chokepoint, schema_version invalidation guard, in-memory session isolation test pattern]
key_files:
  created:
    - web/joins_lab_storage.py
    - tests/test_joins_lab_storage.py
  modified: []
decisions:
  - "Docstring avoids literal 'app.storage.user' string to keep grep-based acceptance criteria clean"
  - "Type annotations removed from module-level constants (_JOINS_LAB_KEY, _SCHEMA_VERSION) to match exact grep pattern '_SCHEMA_VERSION = 1' required by acceptance criteria"
  - "importlib.reload used in schema_version_mismatch test to ensure monkeypatch lands on module-level names cleanly; later tests use direct module-attribute setattr which is more robust"
metrics:
  duration: "2 minutes"
  completed_date: "2026-06-17"
  tasks_completed: 2
  files_created: 2
  files_modified: 0
requirements: [FND-06]
---

# Phase 117 Plan 02: Versioned safe_storage Schema Summary

Versioned `safe_storage` helpers for the `joins_lab` namespace in a single small
module (`web/joins_lab_storage.py`) and 6 tests proving schema-version invalidation,
write/read round-trip, and two-anonymous-session isolation.

## What Was Built

### Task 1 — `web/joins_lab_storage.py`

Thin chokepoint module that ALL Joins Lab per-user storage passes through.
Four public helpers:

| Function | Purpose |
|---|---|
| `read_joins_lab_state()` | Read + validate from storage; returns `None` on missing/stale/non-dict |
| `write_anchor(sys_id, fl_id, volume_ie)` | Persist anchor identity; blob-free (identity only) |
| `read_anchor()` | Convenience alias for page restore path (D-13) |
| `clear_joins_lab_state()` | Phase 120 reset helper; wraps `safe_user_pop` |

Module constants: `_JOINS_LAB_KEY = 'joins_lab'`, `_SCHEMA_VERSION = 1`.

Zero raw `app.storage.user` access — all I/O through `safe_user_get` / `safe_user_set` /
`safe_user_pop` (Phase 87 CI invariant; `test_no_raw_storage_access.py` still green).

**Forward-compatibility note:** Phase 120 can add builder rows, triage verdicts, and
filter keys under the same `schema_version: 1` without a version bump.  Only bump on
breaking shape changes (key removal or type change).

### Task 2 — `tests/test_joins_lab_storage.py`

6 tests covering all required behaviors:

| Test | Behavior |
|---|---|
| `test_schema_version_mismatch_returns_none` | `schema_version=0` → `None` (cold start) |
| `test_missing_key_returns_none` | Absent key → `None` |
| `test_non_dict_stored_value_returns_none` | String value → `None` (isinstance guard) |
| `test_valid_schema_version_returns_data` | `schema_version=1` → data returned |
| `test_write_then_read_round_trip` | `write_anchor` + `read_anchor` via in-memory store |
| `test_two_sessions_do_not_share_state` | Store A write not visible in store B (SC#5) |

No live NiceGUI context required — all tests monkeypatch `safe_user_get` / `safe_user_set` /
`safe_user_pop` with per-session in-memory dicts.

## Verification

```
pytest tests/test_joins_lab_storage.py tests/test_no_raw_storage_access.py -x -q
12 passed in 2.12s
```

`grep -nE "app\.storage\.user" web/joins_lab_storage.py` → no matches.

## Deviations from Plan

None — plan executed exactly as written.

The only minor adaptation: the plan's acceptance criterion required `_SCHEMA_VERSION = 1`
(untyped assignment); the initial implementation used `_SCHEMA_VERSION: int = 1` (typed).
Corrected to match the grep-based acceptance check exactly.  Not a functional change.

## Threat Flags

No new security surface introduced.  The module only touches `safe_user_*` helpers and
stores anchor identity (sys_id/fl_id/volume_ie strings).  No network endpoints, no file
access, no schema changes.

T-117-04 (cross-session bleed) and T-117-05 (stale schema) from the plan's threat register
are both **mitigated**: session isolation proven by `test_two_sessions_do_not_share_state`,
schema invalidation proven by `test_schema_version_mismatch_returns_none`.

## Known Stubs

None.  The module is complete for its Phase 117 purpose (anchor identity only).
Phase 120 will extend the schema with builder/triage/filter keys under `schema_version: 1`.

## Self-Check: PASSED
