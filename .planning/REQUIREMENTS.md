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

- [x] **STATE-01**: 10 per-user fields deleted from `web/state.py:AppState` (`last_results`, `current_search_query`, `current_search_mode`, `current_search_gap`, `last_filters_applied`, `last_search_warnings`, `last_selected_uids`, `parallels_results`, `parallels_filtered`, `parallels_search_meta`)
- [x] **STATE-02**: All writer sites (`search.py`, `search_results.py`, `parallels.py`) migrated to write exclusively through `web/export_state.py`
- [x] **STATE-03**: All reader sites (`api.py` export handlers, others discovered during migration) migrated to read exclusively through `web/export_state.py`
- [x] **STATE-04**: `_TEST_BACKEND` shim removed from `web/export_state.py`; replaced with proper fixture or adapter injection
- [x] **STATE-05**: `tests/test_export_cross_user_isolation.py` rewritten to assert against per-session storage directly, without the shim
- [x] **STATE-06**: `tests/test_export_state_selection.py`, `tests/test_api_export_json.py`, `tests/test_api_legacy_unchanged.py` updated to drop `state.*` setup and use only `export_state` helpers

## Lists Cache — Phase 89

Drop the singleton + 10s TTL plumbing. Per-request instantiation is the simpler safe pattern.

- [x] **LISTS-01**: `UserListsManager` instance singleton on `AppState._user_lists_mgr` removed
- [x] **LISTS-02**: `UserListsManager` instantiated per-request in page handlers that need it
- [x] **LISTS-03**: `_cache_entry` tuple and 10s TTL plumbing removed (originated in `22b45f68`, evolved in `8ac93eff`); user-id-key plumbing removed alongside
- [x] **LISTS-04**: `tests/test_user_lists_cache_isolation.py` rewritten against the per-request model

## Auth Caching — Phase 90

Replace the process-wide cache with request-scoped auth that respects Codex's `set_session()` finding.

- [x] **AUTHC-01**: `_client_cache`, `_session_locks`, `_locks_guard`, `_CLIENT_CACHE_TTL` deleted from `web/supabase_client.py`
- [x] **AUTHC-02**: Request-scoped auth strategy implemented that does NOT call `auth.set_session()` solely to set request headers (per Codex finding: `gotrue_client.py:713` proves `set_session()` makes a network call)
- [x] **AUTHC-03**: Refresh-only locking keyed by `_session_uuid` from Phase 87; no cached authenticated `supabase.Client` objects
- [x] **AUTHC-04**: Auth-resurrection guard added in `cca23db3` removed (obsolete once `get_user_client()` cache is gone)
- [x] **AUTHC-05**: Code comment near the auth path documents WHY `set_session()` is avoided (Codex finding cited) so future contributors don't reintroduce it

## Auth State Writes — Phase 91

Make the multi-step writes across the auth boundary atomic and safe-storage-aware.

- [x] **AUTHW-01**: `web/auth_state.py:set_auth`, `clear_auth`, `do_login` migrated to safe_storage helpers
- [x] **AUTHW-02**: OAuth callback in `web/main.py:1456+` migrated to safe_storage helpers
- [x] **AUTHW-03**: `sign_out`/server-side revocation happens BEFORE popping `auth_session`; local auth keys popped in a `finally` block so cleanup is atomic even when revocation fails
- [x] **AUTHW-04**: `sign_out` uses the user's authenticated client (not the anonymous singleton) so the token is actually revoked server-side
- [x] **AUTHW-05**: Tests for OAuth callback prune-mid-flight resilience (`AssertionError` on pruned session must not 500 the callback)
- [x] **AUTHW-06**: `persist_value` safe-wrap from `cca23db3` retained in `web/components/filter_panel.py`

## Final Sweep + Acceptance — Phase 92

Prove the invariants hold end-to-end and document the architecture so it survives the next contributor.

- [x] **SWEEP-01**: `web/` audited for any remaining raw `app.storage.user.get/pop` and `app.storage.user[key] = ...` accesses (5-surface widened audit per Gemini D-03: `app.storage.user` clean + `app.storage.browser` documented + `app.storage.client` documented + `shared/puzzle_service.py`/joins.db N/A community-share + `web/analytics.py` JS-injection-only verified — see `92-SWEEP-01-AUDIT.md`)
- [x] **SWEEP-02**: `parallels.py:3520` and `text_editor.py` auto-save (the two deferred-callback sites Codex round 4 flagged) confirmed migrated to safe_storage
- [x] **SWEEP-03**: Phase 87 allowlist re-audited; terminal empty state `allowed_raw_access: []` preserved from Phase 91; new entries require code-review approval per BANNED_TABLES extension protocol in `docs/guides/SUPABASE_GUIDE.md`
- [x] **SWEEP-04**: 4 Codex review transcripts (`_tmp/codex_*_response.txt`) re-read against final state per D-05 thematic walk; 23 raw findings deduped into >=8 unique issues; every addressed disposition cites git short hash + phase-plan pointer per D-07 — see `92-SWEEP-04-TRANSCRIPT-AUDIT.md`
- [x] **SWEEP-05**: Production smoke-test plan executed: smoke run 2 PASS 2026-05-18; R0/R1 exercised end-to-end PASS; R2 invariant (`per-_session_uuid` refresh-lock keying) covered by Phase 90 unit-test suite `tests/test_refresh_lock_per_session.py` (D-17 behavioral test proving distinct-`_session_uuid` parallelism); manual e2e JWT-tamper procedure (Paths A/B/C) deferred as future-debt and does not gate v7.12 closure; R3 = N/A (joins.db community-share per D-04); Overall = PASS — see `92-SWEEP-05-SMOKE.md`
- [x] **SWEEP-06**: `docs/guides/MULTITENANT.md` written documenting the architecture (safe_storage chokepoint, `_session_uuid`, request-scoped auth, per-request lists, deletion-not-migration discipline, §7 D-09 WARNING callout for `set_auth(profile=None)` clears-stale semantics with CM6-refined wording cross-referencing `tests/test_auth_callback_resilience.py:T-F`, §8 enforcement layer documenting both `tests/test_no_raw_storage_access.py` and the 3 manual-audit surfaces per D-10) for future contributors

## Reader-Client Retrofit — Phase 92.1

Close the P0 RLS-reachability regression introduced by Phase 90 D-09/D-10 — the singleton-anonymous-only invariant correctly stopped the SIGNED_IN-event-listener leak but left ~12 reader functions in `web/supabase_client.py` using the anonymous singleton, so RLS-`TO authenticated USING (auth.uid()=user_id)` SELECT policies filter out every row for logged-in users. **Phase inserted 2026-05-17 after Phase 92 SWEEP-05 smoke run 1 FAILED at R0 baseline.**

- [x] **READER-01**: Migrate user-scoped readers in `web/supabase_client.py` from `get_client()` to `get_user_client()`. After migration, exactly 6 runtime call sites of `client = get_client()` remain in the file body (lines 490, 502 = auth-API inspection helpers; 711, 721 = `TO public`-only profile/corrections-count fast paths; 1326 = legitimate Exception fallback after get_user_client raised; 1579 = `get_feed_items` whose every branch is public-filtered per the verified RLS evidence — only SELECT policy on discoveries is `TO public USING (is_hidden=false)`, no admin SELECT branch exists, so hidden rows are invisible to BOTH client roles and migration would NOT surface additional rows; Reviews R2-1 Option C, 2026-05-17). All other readers must use `get_user_client()` which falls back internally to `get_client()` for anonymous browsers.
- [x] **READER-02**: Each migrated reader audited against its `docs/guides/SUPABASE_GUIDE.md` RLS SELECT policy — disposition (MIGRATE / KEEP / FALLBACK) documented in Plan 92.1-01 PLAN.md reader_disposition_table. Anonymous-OK paths (`TO public` tables: profiles, fragment_joins) are preserved via get_user_client()'s internal fallback to the anon singleton when no auth_session is present.
- [x] **READER-03**: Diagnose and fix Symptom 3 (`safe_user_get('auth_session') unexpected failure: app.storage.user can only be used within a UI context` from search-results → Add to list → Create new list → Save path). The fix MUST be at the call-site / context-binding layer, NOT a log-level downgrade in safe_storage.py.
- [x] **READER-04**: Install a permanent AST-scanner CI guard at `tests/test_no_anonymous_reads_on_authenticated_tables.py` that bans `get_client().table('<user_scoped_table>')...` patterns. Mirror the structure of `tests/test_no_set_session_outside_oauth.py` (REPO_ROOT, SCAN_DIRS, EXEMPT_FILES, parametrized seed-trap block, _iter_py_files helper). BANNED_TABLES set includes at minimum: user_lists, list_items, recent_items, projects. Also document the BANNED_TABLES extension protocol in `docs/guides/SUPABASE_GUIDE.md` near the RLS section so future schema changes know to extend the scanner.
- [x] **READER-05**: Behavioral regression tests at `tests/test_supabase_client_reader_rls.py` covering at minimum 5 migrated readers (get_user_lists, get_list_items, get_recent_items, get_projects, get_deleted_lists). Each test seeds `web.safe_storage.app` with a SimpleNamespace storage containing `auth_session={'access_token': 'good.jwt', 'refresh_token': 'good-refresh'}` and asserts `_apply_user_auth_to_client` was invoked with the expected access_token. Uses `monkeypatch.setattr` pattern from `tests/test_auth_revocation_and_headers.py:149-203`.
- [x] **READER-06**: Closeout docs — `.planning/ROADMAP.md` Phase 92.1 finalized; `docs/OPEN_ISSUES.md` P1 entry marked `Fixed in code; verification pending SWEEP-05 smoke run 2` (full `✅ Fixed (YYYY-MM-DD)` only after Hillel commits smoke PASS verdict per Reviews M4); `CLAUDE.md` "Recently Changed" gains one Phase 92.1 line under the active v7.12 milestone; `.planning/phases/92.1-reader-client-retrofit/92.1-SUMMARY.md` written summarizing migrated readers, scanner, and symptom-3 fix.

---

## Backlog — small phases shipped after the v7.12 milestone close

These are stand-alone backlog phases (`999.x`) that don't belong to a milestone. Tracked here for FOLIO-XX / PGP-FILTER-XX / PGP-EXPORT-XX / LINE-NUM-XX traceability.

### Search Results by Folio — Phase 999.1

- [x] **FOLIO-01**: Surface `result['display']['img']` (page/image number) inline after the shelfmark on each web `/search` result card for desktop COL_IMG parity. Theme-aware chip using existing `var(--bg-tertiary)` / `var(--text-muted)` tokens; falsy value renders nothing. Descriptive tooltip `tr('Image number')` / "מספר תמונה" added post-smoke-check per D-05 revision 2026-05-18.

### Line Numbering — Phase 999.4

WEB pillar (Plan 999.4-01):

- [x] **LINE-NUM-01**: `_render_line_numbered_html(text, highlight_html, line_height, font_size, show_line_numbers)` helper at module scope in `web/pages/browse.py` rendering a CSS-grid two-column layout (gutter span + body div) per source line.
- [x] **LINE-NUM-02**: Helper wired into `render_text_content` (Browse single-page + version views) AND `render_text_section` (Quick View) AND Full Manuscript View loop (post-smoke-check scope extension).
- [x] **LINE-NUM-03**: Numbers restart at 1 when navigating to a new sys_id/folio/version (each `_render_line_numbered_html` call counts independently).
- [x] **LINE-NUM-04**: Toggle button (`format_list_numbered` icon, `tr('Toggle line numbers')` tooltip) in Browse `version_row` AND Quick View view-mode header row.
- [x] **LINE-NUM-05**: Persistence via `safe_user_get/safe_user_set` under key `ui.show_line_numbers`; default True per D-07.
- [x] **LINE-NUM-06**: D-04 copy-paste invariant — gutter span has `user-select: none` AND lives in a separate CSS-grid column; pasted text from the body contains zero line-number digits.

DESKTOP pillar (Plan 999.4-02):

- [x] **LINE-NUM-07**: New module `desktop/widgets/line_number_text_edit.py` with `LineNumberArea(QWidget)` sibling gutter + `apply_line_numbered_text(widget, html, *, source_text, pages, is_html)` helper.
- [x] **LINE-NUM-08**: Helper wired into `genizah_app.py:self.browse_text` at 6 transcription render sites + `desktop/result_dialog.py:self.text_ms` at 4 setHtml sites. Toggle button (`# Lines`, `checkable=True`) in Browse find-row AND ResultDialog find-row. Persistence via `load_app_config('show_line_numbers', True)` / `save_app_config({'show_line_numbers': bool})`.

SHARED:

- [x] **LINE-NUM-09**: `text.split('\n')` numbering invariant (D-10) — line count uses `split('\n')` NOT `splitlines()`, so blank lines and trailing empties get their own number; aligned with Responsa `L<N>:` parser at `genizah_core.py:7679-7691`. Verified by `tests/test_line_numbers_web.py::test_render_line_numbered_html_blank_count_matches` AND `tests/test_line_numbers_desktop.py::test_line_number_area_line_count_matches_split`.
- [x] **LINE-NUM-10**: D-04 copy-paste invariant on desktop — `LineNumberArea` is a SIBLING `QWidget` (not part of `QTextDocument`), so Qt's text cursor cannot extend into it; `widget.toPlainText()` contains zero gutter digits. Verified structurally by `tests/test_line_numbers_desktop.py::test_clipboard_isolation_invariant`.

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
| STATE-01 | Phase 88 | Complete |
| STATE-02 | Phase 88 | Complete |
| STATE-03 | Phase 88 | Complete |
| STATE-04 | Phase 88 | Complete |
| STATE-05 | Phase 88 | Complete |
| STATE-06 | Phase 88 | Complete |
| LISTS-01 | Phase 89 | Complete |
| LISTS-02 | Phase 89 | Complete |
| LISTS-03 | Phase 89 | Complete |
| LISTS-04 | Phase 89 | Complete |
| AUTHC-01 | Phase 90 | Complete |
| AUTHC-02 | Phase 90 | Complete |
| AUTHC-03 | Phase 90 | Complete |
| AUTHC-04 | Phase 90 | Complete |
| AUTHC-05 | Phase 90 | Complete |
| AUTHW-01 | Phase 91 | Complete |
| AUTHW-02 | Phase 91 | Complete |
| AUTHW-03 | Phase 91 | Complete |
| AUTHW-04 | Phase 91 | Complete |
| AUTHW-05 | Phase 91 | Complete |
| AUTHW-06 | Phase 91 | Complete |
| SWEEP-01 | Phase 92 | Complete |
| SWEEP-02 | Phase 92 | Complete |
| SWEEP-03 | Phase 92 | Complete |
| SWEEP-04 | Phase 92 | Complete |
| SWEEP-05 | Phase 92 | Complete |
| SWEEP-06 | Phase 92 | Complete |
| READER-01 | Phase 92.1 | Complete |
| READER-02 | Phase 92.1 | Complete |
| READER-03 | Phase 92.1 | Complete |
| READER-04 | Phase 92.1 | Complete |
| READER-05 | Phase 92.1 | Complete |
| READER-06 | Phase 92.1 | Complete |
| FOLIO-01 | Phase 999.1 (backlog) | Complete |
| LINE-NUM-01 | Phase 999.4 Plan 01 (backlog) | Complete |
| LINE-NUM-02 | Phase 999.4 Plan 01 (backlog) | Complete |
| LINE-NUM-03 | Phase 999.4 Plan 01 (backlog) | Complete |
| LINE-NUM-04 | Phase 999.4 Plan 01 (backlog) | Complete |
| LINE-NUM-05 | Phase 999.4 Plan 01 (backlog) | Complete |
| LINE-NUM-06 | Phase 999.4 Plan 01 (backlog) | Complete |
| LINE-NUM-07 | Phase 999.4 Plan 02 (backlog) | Complete |
| LINE-NUM-08 | Phase 999.4 Plan 02 (backlog) | Complete |
| LINE-NUM-09 | Phase 999.4 Plan 01+02 (backlog) | Complete |
| LINE-NUM-10 | Phase 999.4 Plan 02 (backlog) | Complete |

---

*Last updated: 2026-05-18 -- v7.12 Path B Multitenant Architecture milestone SHIPPED (38/38 across phases 87, 88, 89, 90, 91, 92, 92.1; Plan 92.2 = internal perf sub-phase, no new requirements). Backlog phases shipped 2026-05-18 alongside v7.12: 999.1 (FOLIO-01 via commits 8368a962 + 9db7b18e), 999.4 (LINE-NUM-01..10 across 2 plans / 9 commits — web at 69a48986/ba666564/9bde739e/e63d0e91, desktop at 30cc144e/7a93d4eb/b3a491a9/346546ad/cbb4a3fb/0c164687/05a5740b). Both backlog phases are independent of the v7.12 milestone scope.*
