---
gsd_state_version: 1.0
milestone: v7.12
milestone_name: Multitenant Architecture
status: executing
stopped_at: Phase 90 complete -- ready for Phase 91
last_updated: "2026-05-15T10:30:00.000Z"
last_activity: 2026-05-15 -- Phase 90 complete (Auth Caching Rewrite -- No set_session)
progress:
  total_phases: 10
  completed_phases: 4
  total_plans: 17
  completed_plans: 15
  percent: 88
---

# Project State

## Project Reference

See: .planning/PROJECT.md (updated 2026-05-13)

**Core value:** Researchers can find what they need in the Genizah corpus
**Current focus:** Phase 90 complete (2/2 plans, 5/5 SCs, 5/5 AUTHC reqs verified) — ready for Phase 91

## Current Position

Phase: 90 (Auth Caching Rewrite -- No set_session) — COMPLETE (2/2 plans shipped, all 5 ROADMAP SCs + 5 AUTHC reqs verified)
Next: Phase 91 (Atomic Auth State Writes)
Status: Ready to discuss
Last activity: 2026-05-15 -- Phase 90 complete (Auth Caching Rewrite -- No set_session)

Progress: [██████░░░░] 67%

## Phase Queue (v7.12)

| Phase | Name | Requirements | Status |
|-------|------|--------------|--------|
| 87 | Foundations -- Session UUID and Safe Storage Chokepoint | FOUND-01..05 | Complete |
| 88 | State Separation by Deletion | STATE-01..06 | Complete |
| 89 | Lists Cache Per-Request | LISTS-01..04 | Complete |
| 90 | Auth Caching Rewrite -- No set_session | AUTHC-01..05 | Complete |
| 91 | Atomic Auth State Writes | AUTHW-01..06 | Pending |
| 92 | Final Sweep and Acceptance | SWEEP-01..06 | Pending |

**Dependency order:** 87 must complete first (all others depend on it). 91 also depends on 90. 92 depends on all of 87-91.

## Performance Metrics

**Velocity:**

- Total plans completed: 11
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
- Phase 87-04 complete: 3 central files (web/main.py 14 inline + 18 caller routings + 2 helper deletions; web/api.py 3 nicegui_app alias sites; web/supabase_client.py 1 sign_out site) migrated to safe_storage helpers. 18 raw access sites eliminated. OAuth allowlist (3 sites at main.py + 1 at supabase_client.py:111) preserved verbatim. B1 bootstrap wiring preserved (ensure_session_uuid still called in create_layout/reset_hints_route/auth_callback_route). 21/21 Phase 87 tests + targeted regression 421/421 GREEN. FOUND-02 advanced.
- Phase 87-05 complete: 3 browse-cluster files (web/pages/browse.py 4 sites, web/pages/browse_state.py 10 sites, web/pages/catalog_browse.py 3 sites) migrated — 17 raw access sites eliminated. M2 independent-read semantics preserved in restore_browse_snapshot; M3 Class B inner wrappers preserved in persist_browse_snapshot (Fix 4) and browse.py:1122. B3 monkeypatch fix applied to tests/test_browse_state.py (7 patches swapped to web.safe_storage.app). 28/28 relevant tests GREEN. FOUND-02 advanced.
- Phase 87-06 complete: 3 search-cluster files (web/pages/parallels.py 35 sites, web/pages/search.py 14 sites, web/pages/search_state.py 31 sites) migrated — 80 raw access sites eliminated. Single largest migration in Phase 87. Codex round 4 MEDIUM-2 deferred-restore callback at parallels.py:3520 migrated with explicit documenting comment. M2 independent-read semantics preserved; M3 Class B wrappers preserved in persist_search_snapshot (both outer and inner per Fix 4). B3 dual-patch fix applied to tests/test_search_state.py. 31/31 Phase 87 + search-state tests GREEN. FOUND-02 advanced.
- Phase 87-07 complete: lint acceptance gate closed. All 6 lint scanner tests pass GREEN against production code (test_allowlist_well_formed, test_lint_rejects_synthetic_violation, test_lint_handles_aliased_imports, test_lint_does_not_double_report_nested_nodes, test_allowlist_counts_exact, test_no_raw_storage_access_outside_allowlist). Allowlist preserved verbatim from Plan 01: 4 entries (auth_state, main, supabase_client, export_state) / 13 patterns / 14 expected_count nodes — exactly matching the AST-counted reality after Plans 03-06 migrated 131 sites across 14 files. Zero code changes needed in Plan 07 (outcome A: Wave 2 already brought codebase to target state). Full suite 1879/1879 pass, ruff clean, check_docs healthy. All 5 ROADMAP Phase 87 Success Criteria satisfied via automated test commands. FOUND-04 satisfied; lint scanner is now the permanent CI guard.
- Phase 87-08 Task 1 complete (Task 2 pending user smoke-check): docs updates committed across CLAUDE.md "Recently Changed" (new Phase 87 entry citing 131 sites / 14 files / 8 plans / 22 tests / lint scanner / allowlist 4 entries / Codex round 4 MEDIUM-2 closure / B3 monkeypatch fixes), docs/OPEN_ISSUES.md (Last Updated 2026-05-13 with Phase 87 framing; narrow-scope `/browse 500 AssertionError` hotfix from line 82 now subsumed by broader chokepoint), .planning/STATE.md frontmatter (completed_phases 0→1, completed_plans 7→8, percent 88→13 reflecting full v7.12 milestone scope of 6 phases) + Phase 87 row marked Complete + Current Position advanced to Phase 88, .planning/ROADMAP.md Phase 87 row marked Complete with 8/8 plans + plan list populated with 8 [x] entries. check_docs green; full pytest suite green at Plan 07 close. Task 2 (human smoke-check confirming real-NiceGUI session storage behavior per B1) returned as checkpoint to orchestrator — NOT yet user-approved.
- Plan 07 lint scanner (tests/test_no_raw_storage_access.py) is the permanent CI guard against raw app.storage.user regression — all new code must use safe_storage helpers or add an allowlist entry with justification AND expected_count (H1 schema from 87-REVIEWS.md)
- Bootstrap wiring (B1): ensure_session_uuid() is called from web/main.py:create_layout() (covers 17 of 19 routes), web/main.py:reset_hints_route, and web/main.py:auth_callback_route. /privacy-extension is AST-confirmed exempt (zero app.storage.user access). This is the canonical mint surface — DO NOT add per-page mint calls; downstream code can rely on _session_uuid being present in storage at every storage-touching page entry.
- Phase 90 complete (2026-05-15): Auth caching rewrite shipped in 2 plans/2 waves. Plan 90-01 rewrote `web/supabase_client.py` to request-scoped auth — `get_user_client()` builds an authenticated Client per call via local header mutation (`_apply_user_auth_to_client` writes 4 headers: postgrest.auth, functions.set_auth, storage `Authorization`+`apikey`); proactive refresh (`_refresh_user_session`) gated by `_refresh_locks` keyed by `_session_uuid` (NOT access tokens); 4 reactive JWT-expired retry blocks at lines ~516/756/935/1101 call `_refresh_user_session()` instead of `reset_client()`; 5 auth-mutating helpers (sign_in/sign_up/set_session_from_url/exchange_code_for_session/get_oauth_url) all use throwaway clients to sidestep the supabase event-listener leak vector at `supabase/_sync/client.py:338-346` (D-10 invariant); `sign_out` calls `throwaway.auth.admin.sign_out(jwt, "global")` for real server-side revocation (Codex round-3 P1 — high-level `sign_out` on throwaway is a no-op per `gotrue_client.py:789-793`); `clear_auth` reordered to revoke-before-pop with finally-block local cleanup (D-11b); `profile.py:149-150` migrated to new `change_password` REST helper (D-02 direct httpx with explicit apikey header); standalone dead-code `refresh_session()` at lines 325-339 DELETED. Phase 87 allowlist entry for `supabase_client.py` self-eliminated by the `safe_user_get('auth_session')` migration — count 3→2. Plan 90-02 atomically deleted 6 dead-code names (`_client_cache`, `_session_locks`, `_locks_guard`, `_CLIENT_CACHE_TTL`, `_clear_stale_auth`, `_prune_session_client_cache`) and installed 3 permanent CI guards in a SINGLE commit (Phase 89 D-09 discipline): `tests/test_no_set_session_outside_oauth.py` (D-15 static AST scanner — Class A `set_session`/`exchange_code_for_session` outside allowed helpers + Class B `get_client().auth.<mutating>(...)` resurrection ban, 13 seed-trap snippets, 16 tests), `tests/test_no_client_cache_globals.py` (D-16 runtime attr-absence parametrized over 6 names, 6 tests), `tests/test_refresh_lock_per_session.py` (D-17 deterministic behavioral — Test A same-uuid serialization, Test B distinct-uuid parallelism via `_ThreadRoutedApp` + `_ConcurrencyRecorder.max_concurrent == 2` proving real parallelism not trivial pass, Test C stale-snapshot short-circuit, 3 tests). Codex review history: 4 rounds (1 storage/auth/reactive/leak, 2 sign_out/headers/alias/count, 3 throwaway.auth.sign_out is no-op, 4 plan-checker D-10 helper set 3→5 + dead-code deletion). Full pytest green at each plan boundary (1948→1949 passed, 0 failures). Verification 5/5 PASS — both SC #1 (no cache globals) and SC #2 (set_session only at `web/supabase_client.py:609` inside `set_session_from_url`) confirmed via word-boundary grep. Plan 90-02 Task 5 (L1 perf sanity check) deferred per `feedback_no_background_webserver.md` — does not affect SC closure.
- Phase 90 deviation: Plan 90-01 pulled AUTHW-03 + AUTHW-04 forward from Phase 91 per Codex round-2 P1 because `sign_out` needed actual revocation immediately. Phase 91 scope adjusted — AUTHW-01 (auth_state.py allowlist migration), AUTHW-02 (OAuth callback atomicity at main.py:1458-1463), AUTHW-05 (test_auth_callback_resilience.py), AUTHW-06 (filter_panel persist_value retention) remain; AUTHW-03/AUTHW-04 mark "subsumed by Phase 90".

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

Last session: 2026-05-15T10:30:00.000Z
Stopped at: Phase 90 complete — Auth Caching Rewrite shipped (2/2 plans, 5/5 SCs verified)
Resume file: .planning/phases/90-auth-caching-rewrite-no-set-session/90-VERIFICATION.md
Next step: `/gsd-discuss-phase 91` (Atomic Auth State Writes). Phase 91 scope reduced after Plan 90-01 pulled AUTHW-03 + AUTHW-04 forward (sign_out server-side revocation already shipped). Remaining: AUTHW-01 (auth_state.py raw-storage migration through safe_storage helpers — closes Phase 87 allowlist entry #1), AUTHW-02 (OAuth callback main.py:1458-1463 atomicity — closes Phase 87 allowlist entry #2), AUTHW-05 (tests/test_auth_callback_resilience.py for mid-flight prune), AUTHW-06 (filter_panel persist_value retention check).
