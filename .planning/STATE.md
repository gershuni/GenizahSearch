---
gsd_state_version: 1.0
milestone: v7.12
milestone_name: Multitenant Architecture
status: executing
stopped_at: Completed 87-01-VALIDATION-FOUNDATION-PLAN.md
last_updated: "2026-05-13T05:03:54.627Z"
last_activity: 2026-05-13
progress:
  total_phases: 6
  completed_phases: 0
  total_plans: 8
  completed_plans: 1
  percent: 13
---

# Project State

## Project Reference

See: .planning/PROJECT.md (updated 2026-05-13)

**Core value:** Researchers can find what they need in the Genizah corpus
**Current focus:** Phase 87 — Foundations -- Session UUID and Safe Storage Chokepoint

## Current Position

Phase: 87 (Foundations -- Session UUID and Safe Storage Chokepoint) — EXECUTING
Plan: 2 of 8 (next: 87-02-SESSION-UUID-HELPERS)
Status: Executing Phase 87
Last activity: 2026-05-13 -- Plan 87-01 complete (Wave 0 failing-test gate)

Progress: [█░░░░░░░░░] 13%

## Phase Queue (v7.12)

| Phase | Name | Requirements | Status |
|-------|------|--------------|--------|
| 87 | Foundations -- Session UUID and Safe Storage Chokepoint | FOUND-01..05 | Pending |
| 88 | State Separation by Deletion | STATE-01..06 | Pending |
| 89 | Lists Cache Per-Request | LISTS-01..04 | Pending |
| 90 | Auth Caching Rewrite -- No set_session | AUTHC-01..05 | Pending |
| 91 | Atomic Auth State Writes | AUTHW-01..06 | Pending |
| 92 | Final Sweep and Acceptance | SWEEP-01..06 | Pending |

**Dependency order:** 87 must complete first (all others depend on it). 91 also depends on 90. 92 depends on all of 87-91.

## Performance Metrics

**Velocity:**

- Total plans completed: 0
- Average duration: --
- Total execution time: --

**By Phase:** (none yet)

*Updated after each plan completion*

## Accumulated Context

### Decisions (v7.12-relevant)

- Phase 87 first: subsequent phases need stable `_session_uuid` cache key + zero-raw-storage invariant before auth/lists can be safely rewritten
- State separation by deletion, not migration: dual-write through singleton mirrors invites regression; `web/export_state.py` becomes the only path
- Lists cache goes per-request: 10s TTL was a perf optimization, not load-bearing; not worth preserving during multitenant safety work
- NO `auth.set_session()` mid-flight: Codex verified `gotrue_client.py:713` -- `set_session()` is networked, not local state mutation
- Refresh-only locking keyed by `_session_uuid`: UUID-keyed locks are stable across token rotation; no cached authenticated client objects
- `_TEST_BACKEND` shim removed: tests use real session storage with proper fixtures or adapter injection
- Phase 87-01 Wave 0 gate established: 10 failing test stubs + 6-test AST lint scanner + 4-entry allowlist YAML. PyYAML 6.0.3 confirmed. test_safe_storage.py byte-unchanged (FOUND-05 invariant SHA256 = e165bf0e...)

### Carryover from hold commits (master-main at cca23db3)

- KEEP: `web/safe_storage.py` module + `safe_user_get/set/pop` helpers (aab16e6d)
- KEEP: `safe_user_get` migrations in search.py, parallels.py, filter_panel.py (8ac93eff)
- KEEP: `persist_value` safe-wrap in filter_panel.py + more bootstrap-read migrations (cca23db3)
- DISCARD: `UserListsManager._cache_entry` tuple (22b45f68 -- superseded by per-request)
- DISCARD: access_token-keyed client cache (8ac93eff -- superseded by refresh-only UUID-keyed locking)
- DISCARD: auth-resurrection guard (cca23db3 -- obsolete once `get_user_client` cache is gone)

### Blockers/Concerns

- Server is on detached HEAD at `v7.11.1` (commit `242664d3`). Do NOT run `deploy.sh` until Path B is ready -- it will pull master-main and move prod to `cca23db3` (recall-grade per Codex).

## Session Continuity

Last session: 2026-05-13T05:03:54.621Z
Stopped at: Completed 87-01-VALIDATION-FOUNDATION-PLAN.md
Resume file: None
Next step: `/gsd-discuss-phase 87` (Foundations: session UUID + safe_storage chokepoint)
