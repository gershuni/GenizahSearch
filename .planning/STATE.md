---
gsd_state_version: 1.0
milestone: v7.12
milestone_name: Multitenant Architecture
status: executing
stopped_at: Roadmap created -- ROADMAP.md, STATE.md, REQUIREMENTS.md traceability filled
last_updated: "2026-05-13T02:53:41.403Z"
last_activity: 2026-05-13 -- Phase 87 planning complete
progress:
  total_phases: 6
  completed_phases: 0
  total_plans: 8
  completed_plans: 0
  percent: 0
---

# Project State

## Project Reference

See: .planning/PROJECT.md (updated 2026-05-13)

**Core value:** Researchers can find what they need in the Genizah corpus
**Current focus:** v7.12 Multitenant Architecture (Path B) -- Phase 87: Foundations (pending discussion)

## Current Position

Phase: 87 of 92 (Foundations -- Session UUID and Safe Storage Chokepoint)
Plan: --
Status: Ready to execute
Last activity: 2026-05-13 -- Phase 87 planning complete

Progress: [░░░░░░░░░░] 0%

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

Last session: 2026-05-13
Stopped at: Roadmap created -- ROADMAP.md, STATE.md, REQUIREMENTS.md traceability filled
Resume file: None
Next step: `/gsd-discuss-phase 87` (Foundations: session UUID + safe_storage chokepoint)
