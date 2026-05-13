---
gsd_state_version: 1.0
milestone: v7.12
milestone_name: Multitenant Architecture
status: executing
stopped_at: Completed 87-03-LEAF-FILE-MIGRATIONS-PLAN.md
last_updated: "2026-05-13T05:23:12.008Z"
last_activity: 2026-05-13
progress:
  total_phases: 6
  completed_phases: 0
  total_plans: 8
  completed_plans: 3
  percent: 38
---

# Project State

## Project Reference

See: .planning/PROJECT.md (updated 2026-05-13)

**Core value:** Researchers can find what they need in the Genizah corpus
**Current focus:** Phase 87 — Foundations -- Session UUID and Safe Storage Chokepoint

## Current Position

Phase: 87 (Foundations -- Session UUID and Safe Storage Chokepoint) — EXECUTING
Plan: 4 of 8 (next: 87-04-MAIN-AND-ALIAS-MIGRATIONS)
Status: Executing Phase 87
Last activity: 2026-05-13 -- Plan 87-03 complete (5 leaf files migrated to safe_storage; 16 raw access sites eliminated)

Progress: [████░░░░░░] 38%

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
- Phase 87-02 complete: get_session_uuid/ensure_session_uuid added to web/safe_storage.py with M5 strict regex validation (^[0-9a-f]{32}$); ensure_session_uuid wired at create_layout (L349), reset_hints_route (L1288), auth_callback_route (L1450); /privacy-extension intentionally skipped (zero storage access — AST-confirmed). 17/17 phase 87 tests pass (6 safe_storage + 11 session_uuid). FOUND-05 SHA-256 invariant preserved. T-87-01 mitigation verified (100 sessions, 0 collisions); T-87-02 mitigation verified (4 dedicated regex-validation tests).
- Phase 87-03 complete: 5 leaf files migrated to web.safe_storage helpers (text_editor.py 3 sites, translation_report.py 1 site, home.py 2 sites, settings.py 7 sites, search_results.py 3 sites — 16 total raw access sites eliminated). AST scanner confirms 0 violations across all 5 files. M3 defensive-wrapper audit: 13 wrappers encountered, all Class A (only absorbed prune-race AssertionError); 0 Class B wrappers needed preservation. The outer `try: ocr_banner.delete() except Exception: return` wrapper in home.py:_auto_dismiss_ocr preserved (non-storage UI failure mode). Removed now-unused `app` alias from `from nicegui import` line in all 5 files. 17/17 Phase 87 tests still GREEN; FOUND-05 SHA-256 invariant preserved (test_safe_storage.py byte-unchanged). FOUND-02 satisfied.

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

Last session: 2026-05-13T05:23:07.308Z
Stopped at: Completed 87-03-LEAF-FILE-MIGRATIONS-PLAN.md
Resume file: None
Next step: `/gsd-execute-phase 87` continues with `87-04-MAIN-AND-ALIAS-MIGRATIONS-PLAN.md` (main.py non-OAuth paths + alias-using sites; allowlisted OAuth atomic write at L1458/1460/1463 stays out of scope)
