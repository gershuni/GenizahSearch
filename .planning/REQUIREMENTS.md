# Requirements — Milestone v7.12: Multitenant Architecture (Path B)

**Goal:** Refactor GenizahSearch's web layer off the desktop-inherited single-user mental model so per-user state, auth, and caches cannot leak across concurrent sessions sharing one Python process.

**Hard constraint (Codex finding):** No mid-flight `auth.set_session()` calls — verified at `gotrue_client.py:713` that `set_session()` is networked (calls `get_user(access_token)` when JWT valid, `_refresh_access_token(refresh_token)` when expired), not local-only.

---

## Foundations — Phase 87

Land the primitives the rest of the milestone depends on. **This phase must complete before any subsequent phase can start.**

- [ ] **FOUND-01**: `_session_uuid` minted on first request to any page, stored in `app.storage.user['_session_uuid']`, stable across token refresh
- [ ] **FOUND-02**: `web/safe_storage.py` adopted and finalized as the single chokepoint adapter for all per-user state (carried forward from hold-commit `aab16e6d`, audited for completeness)
- [ ] **FOUND-03**: Explicit allowlist of permitted raw `app.storage.user` access sites with per-entry justification (e.g. bootstrap code that pre-dates session existence)
- [ ] **FOUND-04**: CI/lint guard (grep-based check or ruff custom rule) rejects new raw `app.storage.user.get/pop/[key] = ...` outside the allowlist
- [ ] **FOUND-05**: All 6 existing `safe_storage` tests (`tests/test_safe_storage.py`) pass without modification

## State Separation — Phase 88

Delete singleton mirrors; `web/export_state.py` becomes the only path for per-user export state. **Migration is by deletion — no dual-writing through both AppState and export_state.**

- [ ] **STATE-01**: 10 per-user fields deleted from `web/state.py:AppState` (`last_results`, `current_search_query`, `current_search_mode`, `current_search_gap`, `last_filters_applied`, `last_search_warnings`, `last_selected_uids`, `parallels_results`, `parallels_filtered`, `parallels_search_meta`)
- [ ] **STATE-02**: All writer sites (`search.py`, `search_results.py`, `parallels.py`) migrated to write exclusively through `web/export_state.py`
- [ ] **STATE-03**: All reader sites (`api.py` export handlers, others discovered during migration) migrated to read exclusively through `web/export_state.py`
- [ ] **STATE-04**: `_TEST_BACKEND` shim removed from `web/export_state.py`; replaced with proper fixture or adapter injection
- [ ] **STATE-05**: `tests/test_export_cross_user_isolation.py` rewritten to assert against per-session storage directly, without the shim
- [ ] **STATE-06**: `tests/test_export_state_selection.py`, `tests/test_api_export_json.py`, `tests/test_api_legacy_unchanged.py` updated to drop `state.*` setup and use only `export_state` helpers

## Lists Cache — Phase 89

Drop the singleton + 10s TTL plumbing. Per-request instantiation is the simpler safe pattern.

- [ ] **LISTS-01**: `UserListsManager` instance singleton on `AppState._user_lists_mgr` removed
- [ ] **LISTS-02**: `UserListsManager` instantiated per-request in page handlers that need it
- [ ] **LISTS-03**: `_cache_entry` tuple and 10s TTL plumbing removed (originated in `22b45f68`, evolved in `8ac93eff`); user-id-key plumbing removed alongside
- [ ] **LISTS-04**: `tests/test_user_lists_cache_isolation.py` rewritten against the per-request model

## Auth Caching — Phase 90

Replace the process-wide cache with request-scoped auth that respects Codex's `set_session()` finding.

- [ ] **AUTHC-01**: `_client_cache`, `_session_locks`, `_locks_guard`, `_CLIENT_CACHE_TTL` deleted from `web/supabase_client.py`
- [ ] **AUTHC-02**: Request-scoped auth strategy implemented that does NOT call `auth.set_session()` solely to set request headers (per Codex finding: `gotrue_client.py:713` proves `set_session()` makes a network call)
- [ ] **AUTHC-03**: Refresh-only locking keyed by `_session_uuid` from Phase 87; no cached authenticated `supabase.Client` objects
- [ ] **AUTHC-04**: Auth-resurrection guard added in `cca23db3` removed (obsolete once `get_user_client()` cache is gone)
- [ ] **AUTHC-05**: Code comment near the auth path documents WHY `set_session()` is avoided (Codex finding cited) so future contributors don't reintroduce it

## Auth State Writes — Phase 91

Make the multi-step writes across the auth boundary atomic and safe-storage-aware.

- [ ] **AUTHW-01**: `web/auth_state.py:set_auth`, `clear_auth`, `do_login` migrated to safe_storage helpers
- [ ] **AUTHW-02**: OAuth callback in `web/main.py:1456+` migrated to safe_storage helpers
- [ ] **AUTHW-03**: `sign_out`/server-side revocation happens BEFORE popping `auth_session`; local auth keys popped in a `finally` block so cleanup is atomic even when revocation fails
- [ ] **AUTHW-04**: `sign_out` uses the user's authenticated client (not the anonymous singleton) so the token is actually revoked server-side
- [ ] **AUTHW-05**: Tests for OAuth callback prune-mid-flight resilience (`AssertionError` on pruned session must not 500 the callback)
- [ ] **AUTHW-06**: `persist_value` safe-wrap from `cca23db3` retained in `web/components/filter_panel.py`

## Final Sweep + Acceptance — Phase 92

Prove the invariants hold end-to-end and document the architecture so it survives the next contributor.

- [ ] **SWEEP-01**: `web/` audited for any remaining raw `app.storage.user.get/pop` and `app.storage.user[key] = ...` accesses
- [ ] **SWEEP-02**: `parallels.py:3520` and `text_editor.py` auto-save (the two deferred-callback sites Codex round 4 flagged) confirmed migrated to safe_storage
- [ ] **SWEEP-03**: Phase 87 allowlist re-audited; every entry has explicit justification; new entries require code-review approval
- [ ] **SWEEP-04**: 4 Codex review transcripts (`_tmp/codex_*_response.txt`) re-read against final state; each previously-flagged issue confirmed addressed or explicitly waived with rationale
- [ ] **SWEEP-05**: Production smoke-test plan executed: cross-user concurrent `/search` + `/browse` + `/lists` + xlsx export in two browser sessions; no leakage observed
- [ ] **SWEEP-06**: `docs/guides/MULTITENANT.md` written documenting the architecture (safe_storage chokepoint, `_session_uuid`, request-scoped auth, per-request lists, deletion-not-migration discipline) for future contributors

---

## Future Requirements (deferred)

- Per-session rate limiting keyed by `_session_uuid` (currently per-IP; could be tightened post-v7.12 if abuse appears)
- Server-side cache for read-mostly per-user data with TTL (`_session_uuid` makes this safe to add later if perf needs it)
- Desktop equivalent of multitenancy work — NOT NEEDED (desktop is genuinely single-user; this milestone is web-only)

## Out of Scope

- **Desktop app changes** — Path B is web-only. Desktop is single-user by design and unaffected.
- **Migrating Supabase client choice** — keep supabase-py; the issue is how WE use it, not which library.
- **Async session storage** — NiceGUI `app.storage.user` is synchronous; switching to an async store is a separate, larger refactor.
- **Multi-process safety** — single Uvicorn process today; cross-process locking only matters if/when we horizontally scale. Out of scope for v7.12.
- **Rewriting `web/safe_storage.py`** — the module landed in `aab16e6d` and is adequate. Phase 87 is about ADOPTING it as the chokepoint, not rewriting it.

## Traceability

(Filled by roadmapper after roadmap creation.)

---

*Last updated: 2026-05-13 — initial draft for v7.12 milestone*
