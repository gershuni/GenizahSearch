# Requirements — Milestone v7.12: Multitenant Architecture (Path B)

**Goal:** Refactor GenizahSearch's web layer off the desktop-inherited single-user mental model so per-user state, auth, and caches cannot leak across concurrent sessions sharing one Python process.

**Hard constraint (Codex finding):** No mid-flight `auth.set_session()` calls — verified at `gotrue_client.py:713` that `set_session()` is networked (calls `get_user(access_token)` when JWT valid, `_refresh_access_token(refresh_token)` when expired), not local-only.

---

## Foundations — Phase 87

Land the primitives the rest of the milestone depends on. **This phase must complete before any subsequent phase can start.**

- [x] **FOUND-01**: `_session_uuid` minted on first request to any page, stored in `app.storage.user['_session_uuid']`, stable across token refresh
- [x] **FOUND-02**: `web/safe_storage.py` adopted and finalized as the single chokepoint adapter for all per-user state (carried forward from hold-commit `aab16e6d`, audited for completeness)
- [x] **FOUND-03**: Explicit allowlist of permitted raw `app.storage.user` access sites with per-entry justification (e.g. bootstrap code that pre-dates session existence)
- [x] **FOUND-04**: CI/lint guard (grep-based check or ruff custom rule) rejects new raw `app.storage.user.get/pop/[key] = ...` outside the allowlist
- [x] **FOUND-05**: All 6 existing `safe_storage` tests (`tests/test_safe_storage.py`) pass without modification

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

## Reader-Client Retrofit — Phase 92.1

Close the P0 RLS-reachability regression introduced by Phase 90 D-09/D-10 — the singleton-anonymous-only invariant correctly stopped the SIGNED_IN-event-listener leak but left ~12 reader functions in `web/supabase_client.py` using the anonymous singleton, so RLS-`TO authenticated USING (auth.uid()=user_id)` SELECT policies filter out every row for logged-in users. **Phase inserted 2026-05-17 after Phase 92 SWEEP-05 smoke run 1 FAILED at R0 baseline.**

- [ ] **READER-01**: Migrate user-scoped readers in `web/supabase_client.py` from `get_client()` to `get_user_client()`. After migration, exactly 6 runtime call sites of `client = get_client()` remain in the file body (lines 490, 502 = auth-API inspection helpers; 711, 721 = `TO public`-only profile/corrections-count fast paths; 1326 = legitimate Exception fallback after get_user_client raised; 1579 = `get_feed_items` whose every branch is public-filtered per the verified RLS evidence — only SELECT policy on discoveries is `TO public USING (is_hidden=false)`, no admin SELECT branch exists, so hidden rows are invisible to BOTH client roles and migration would NOT surface additional rows; Reviews R2-1 Option C, 2026-05-17). All other readers must use `get_user_client()` which falls back internally to `get_client()` for anonymous browsers.
- [ ] **READER-02**: Each migrated reader audited against its `docs/guides/SUPABASE_GUIDE.md` RLS SELECT policy — disposition (MIGRATE / KEEP / FALLBACK) documented in Plan 92.1-01 PLAN.md reader_disposition_table. Anonymous-OK paths (`TO public` tables: profiles, fragment_joins) are preserved via get_user_client()'s internal fallback to the anon singleton when no auth_session is present.
- [ ] **READER-03**: Diagnose and fix Symptom 3 (`safe_user_get('auth_session') unexpected failure: app.storage.user can only be used within a UI context` from search-results → Add to list → Create new list → Save path). The fix MUST be at the call-site / context-binding layer, NOT a log-level downgrade in safe_storage.py.
- [ ] **READER-04**: Install a permanent AST-scanner CI guard at `tests/test_no_anonymous_reads_on_authenticated_tables.py` that bans `get_client().table('<user_scoped_table>')...` patterns. Mirror the structure of `tests/test_no_set_session_outside_oauth.py` (REPO_ROOT, SCAN_DIRS, EXEMPT_FILES, parametrized seed-trap block, _iter_py_files helper). BANNED_TABLES set includes at minimum: user_lists, list_items, recent_items, projects. Also document the BANNED_TABLES extension protocol in `docs/guides/SUPABASE_GUIDE.md` near the RLS section so future schema changes know to extend the scanner.
- [ ] **READER-05**: Behavioral regression tests at `tests/test_supabase_client_reader_rls.py` covering at minimum 5 migrated readers (get_user_lists, get_list_items, get_recent_items, get_projects, get_deleted_lists). Each test seeds `web.safe_storage.app` with a SimpleNamespace storage containing `auth_session={'access_token': 'good.jwt', 'refresh_token': 'good-refresh'}` and asserts `_apply_user_auth_to_client` was invoked with the expected access_token. Uses `monkeypatch.setattr` pattern from `tests/test_auth_revocation_and_headers.py:149-203`.
- [x] **READER-06**: Closeout docs — `.planning/ROADMAP.md` Phase 92.1 finalized; `docs/OPEN_ISSUES.md` P1 entry marked `Fixed in code; verification pending SWEEP-05 smoke run 2` (full `✅ Fixed (YYYY-MM-DD)` only after Hillel commits smoke PASS verdict per Reviews M4); `CLAUDE.md` "Recently Changed" gains one Phase 92.1 line under the active v7.12 milestone; `.planning/phases/92.1-reader-client-retrofit/92.1-SUMMARY.md` written summarizing migrated readers, scanner, and symptom-3 fix.

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

| Requirement | Phase | Status |
|-------------|-------|--------|
| FOUND-01 | Phase 87 | Complete |
| FOUND-02 | Phase 87 | Complete |
| FOUND-03 | Phase 87 | Complete |
| FOUND-04 | Phase 87 | Complete |
| FOUND-05 | Phase 87 | Complete |
| STATE-01 | Phase 88 | Pending |
| STATE-02 | Phase 88 | Pending |
| STATE-03 | Phase 88 | Pending |
| STATE-04 | Phase 88 | Pending |
| STATE-05 | Phase 88 | Pending |
| STATE-06 | Phase 88 | Pending |
| LISTS-01 | Phase 89 | Pending |
| LISTS-02 | Phase 89 | Pending |
| LISTS-03 | Phase 89 | Pending |
| LISTS-04 | Phase 89 | Pending |
| AUTHC-01 | Phase 90 | Pending |
| AUTHC-02 | Phase 90 | Pending |
| AUTHC-03 | Phase 90 | Pending |
| AUTHC-04 | Phase 90 | Pending |
| AUTHC-05 | Phase 90 | Pending |
| AUTHW-01 | Phase 91 | Pending |
| AUTHW-02 | Phase 91 | Pending |
| AUTHW-03 | Phase 91 | Pending |
| AUTHW-04 | Phase 91 | Pending |
| AUTHW-05 | Phase 91 | Pending |
| AUTHW-06 | Phase 91 | Pending |
| SWEEP-01 | Phase 92 | Pending |
| SWEEP-02 | Phase 92 | Pending |
| SWEEP-03 | Phase 92 | Pending |
| SWEEP-04 | Phase 92 | Pending |
| SWEEP-05 | Phase 92 | Pending |
| SWEEP-06 | Phase 92 | Pending |
| READER-01 | Phase 92.1 | Pending |
| READER-02 | Phase 92.1 | Pending |
| READER-03 | Phase 92.1 | Pending |
| READER-04 | Phase 92.1 | Pending |
| READER-05 | Phase 92.1 | Pending |
| READER-06 | Phase 92.1 | Complete |

---

*Last updated: 2026-05-17 -- Phase 92.1 reader-client retrofit inserted; 38/38 requirements mapped across 7 phases*
