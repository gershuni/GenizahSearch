# GenizahSearch - Claude Context

> This file provides context for AI assistants working on the GenizahSearch project.

## Project Overview

GenizahSearch is a collaborative research platform for the Cairo Genizah, featuring:
- **Web Application** (NiceGUI) - https://genizahsearch.com
- **Desktop Application** (PyQt6) - Windows executable
- **Supabase Backend** - Cloud database for user data

## Architecture

```
Web App (NiceGUI) ──────────────┐
         │                       │
         ├── Tantivy Index       ├──► Supabase (PostgreSQL)
         │   (local search)      │    - User auth
         │                       │    - Lists, corrections
         ├── pgp.db (SQLite)     │    - Comments, discoveries
         │   (PGP reference data)│
         ├── fjms_enrichment.db  │
         │   (FJMS scholarly)    │
         ├── nli_crossref.db     │
         │   (NLI images/meta)   │
         ├── joins.db (SQLite)   │
         │   (saved puzzle joins)│
Desktop App (PyQt6) ────────────┘
```

The FastAPI backend was removed in January 2026. All read-only reference data is now served from local SQLite sidecars. Supabase is retained only for community features (auth, corrections, lists, comments).

## Key Files

### Core
- `genizah_core.py` - Search engine, data models, core logic
- `genizah_app.py` - Desktop application (PyQt6)

### Web
- `web/main.py` - Web app entry point
- `web/pages/` - Page components (search.py, browse.py, lists.py, etc.)
- `web/components/` - Reusable UI components
- `web/supabase_client.py` - Supabase integration
- `web/safe_storage.py` - Chokepoint for per-user state (post-Phase 87)

### Puzzle (Fragment Puzzle / Join Documents)
- `shared/puzzle_model.py` - PuzzleDocument/PuzzleFragment dataclasses
- `shared/puzzle_service.py` - SQLite CRUD for joins.db sidecar
- `shared/puzzle_export.py` - Composite PNG export, thumbnail generation
- `shared/puzzle_image_service.py` - IIIF image fetch + background removal + cache versioning
- `shared/background_removal.py` - HSV-based background removal engine
- `web/pages/puzzle.py` - Web puzzle page (Fabric.js canvas + unified image loader)
- `web/puzzle_tokens.py` - HMAC upload token generation/verification

### Browser Extension (GenizahSearch Image Helper)
- `extension/manifest.json` - Chrome MV3 manifest with NLI host permissions
- `extension/manifest.firefox.json` - Firefox MV3 manifest
- `extension/background.js` - Service worker fetching NLI images as binary
- `extension/content_script.js` - Page↔background bridge + extension detection
- `extension/build.py` - Builds Chrome and Firefox ZIP packages

### Desktop
- `supabase_corrections_client.py` - Desktop Supabase client
- `lists_sync.py` - Cloud sync for lists

## Common Tasks

### Running the Web App
```bash
python -m web.main
```
Opens on port 8080 or 8081.

### Running the Desktop App
```bash
python genizah_app.py
```

## Important Conventions

1. **Hebrew RTL** - Many strings are in Hebrew, text is right-to-left
2. **Shelfmarks** - Manuscript identifiers like "T-S 12.123", "MS Heb c 57"
3. **sys_id** - Internal unique identifier for manuscripts
4. **fl_id** - Fragment/leaf identifier (e.g., "T-S 12.123.1r" for recto)
5. **library_code** - Abbreviated library identifier (e.g., "CUL", "JTS", "Oxford")
6. **Supabase Data API grants** - Every migration/provisioning script that creates a `public` table intended for `supabase-js`/PostgREST/GraphQL access must include explicit `GRANT` statements for the needed roles, in addition to RLS and policies. This is required for new projects from 2026-05-30 and existing projects from 2026-10-30.

## Data Files

### libraries.csv
Master metadata file for ~255,000 manuscript records.

**Structure:**
```csv
system_number,oxford_part_id,call_numbers,library_code,,,,titles_non_placeholder
```

| Column | Index | Description |
|--------|-------|-------------|
| system_number | 0 | Unique sys_id |
| oxford_part_id | 1 | Oxford part identifier (optional) |
| call_numbers | 2 | Pipe-separated shelfmark variants |
| library_code | 3 | Library abbreviation (CUL, JTS, etc.) |
| titles_non_placeholder | 7 | Hebrew title |

**Library Codes:** CUL (~128K), JTS (~30K), RNL (~17K), Oxford (~13K), Manchester (~12K), BL (~8K), AIU, Mosseri, Gaster, Halper, NLI, and others (see `genizah_core.LIBRARY_CODES`).

### joins.db (SQLite sidecar)
Stores saved puzzle/join documents in `joins_data/`.
- `join_documents` - Document records (id, title, notes, fragments_json, thumbnail_b64)
- `join_document_fragments` - Fragment index for reverse lookups (doc_id, fl_id, sys_id)

## Documentation

See `docs/DOCUMENTATION_INDEX.md` for full documentation structure:
- `docs/guides/` - Admin and deployment guides
- `docs/plans/` - Implementation plans
- `docs/specs/` - Technical specifications
- `docs/archive/` - Historical documents
- `CHANGELOG.md` - Full release history

## Code Style

- Python 3.10+
- NiceGUI for web UI
- PyQt6 for desktop UI
- Type hints encouraged
- Hebrew comments are acceptable

## Environment Variables

```
SUPABASE_URL=https://xxxxx.supabase.co
SUPABASE_ANON_KEY=eyJ...
POSTHOG_API_KEY=phc_xxxxx (optional - enables PostHog analytics)
WEB_PUZZLE_ENABLED=true (default: true)
PUZZLE_UPLOAD_SECRET=xxx (optional - HMAC secret for puzzle upload tokens; auto-generated if unset)
POSTHOG_IP_SALT=xxx (optional - HMAC salt for hashing client IPs; auto-generated if unset, production should set explicitly so hashes survive restarts)

# Search API (Phase 77-83 public HTTP/JSON API over the corpus)
SEARCH_API_MODE=open                  # open | localhost-only | disabled (flippable per request, no restart)
SEARCH_API_RATE_LIMIT=30              # per-IP requests/minute; shared ceiling but each endpoint has its own bucket
SEARCH_API_POSTHOG_SAMPLE_N=1         # capture every Nth API request to PostHog
SEARCH_API_BROWSE_TIMEOUT=1.0         # per-source enrichment timeout (PGP/FJMS/NLI), seconds
SEARCH_API_BROWSE_CORE_TIMEOUT=2.0    # core BrowsePage fetch timeout, seconds
SEARCH_API_BROWSE_TEXT_CAP=4000       # default char cap for transcription text; ?text_cap=N override bounded [100, 10000]

# Skill-side (cairo-genizah-research skill consumer)
GENIZAH_API_BASE=https://genizahsearch.com    # overrides --base-url CLI flag (env wins)
GENIZAH_SKILL_REQ_PER_MIN=24                  # skill self-throttle, leaves 6 rpm headroom under server's 30 rpm
```

## Testing

```bash
pytest tests/
```

## Common Issues

1. **Search not working** - Check if Tantivy index exists in `Genizah_Index/`
2. **User data not syncing** - Check Supabase connection and credentials
3. **Images not loading** - NLI/Cambridge IIIF APIs may be down

## Documentation Maintenance (Important!)

**AI agents working on this codebase should keep documentation updated.**

### Open Issues Tracker (REQUIRED)

**`docs/OPEN_ISSUES.md`** is the central issue tracking document. **You MUST maintain it:**

1. **At session start:** Read `docs/OPEN_ISSUES.md` to understand current status
2. **After fixing any bug:** Mark the issue as `✅ Fixed (YYYY-MM-DD)` with date
3. **After finding new bugs:** Add to appropriate section with `❌ Open` status
4. **At session end:** Update the "Last Updated" timestamp and summary counts

### When to Update Docs

| If you change... | Update these docs |
|------------------|-------------------|
| Architecture/infrastructure | `CLAUDE.md`, `docs/guides/DEPLOYMENT_TECHNICAL.md` |
| Supabase schema (tables, RLS) | `docs/guides/SUPABASE_GUIDE.md` |
| Web app pages/components | `docs/CODE_INDEX.md` |
| Environment variables | `CLAUDE.md`, `docs/guides/DEVELOPER_GUIDE.md` |
| Major features | `CHANGELOG.md`, `README.md` |
| **App version** | Run `python scripts/bump_version.py X.Y.Z` |

### Version Bumping (REQUIRED for releases)

Run the automated script — it updates all version files at once:
```bash
python scripts/bump_version.py 6.3.0          # apply changes
python scripts/bump_version.py 6.3.0 --dry-run # preview only
```

**Files updated automatically:** `version.py` (source of truth), `version_info.txt` (Windows EXE metadata), `CompileScriptGenizah.iss` (Inno Setup), `README.md` (header line).

**Manual steps after running the script:**
1. `CHANGELOG.md` — add `## [X.Y.Z]` section with release notes
2. `CLAUDE.md` "Recently Changed" — add one-line entry
3. `README.md` "What's New" section — update feature description

### Before Finishing a Session

```bash
python scripts/check_docs.py
```

Fix any reported issues before committing.

### Outdated Terms to Avoid

- `FastAPI` / `backend server` - Removed in Jan 2026
- `genizah-backend` service - No longer exists
- `DATABASE_URL` - Replaced by `SUPABASE_URL`
- `port 8000` - Backend port no longer used

## Recently Changed

For full release history see `CHANGELOG.md`. Most recent:

- **May 2026: v7.12 Phase 92.1 (Reader-Client Retrofit)** — internal milestone, not a release. Inserted sub-phase between Phase 92 and Phase 93 after Phase 92 SWEEP-05 smoke run 1 FAILED at R0 baseline (2026-05-17). Closed the P0 reader-client RLS-reachability regression introduced by Phase 90 D-09/D-10 (singleton-anonymous-only invariant). 3 plans / 3 waves: Plan 92.1-01 migrated 12 reader call sites in `web/supabase_client.py` from the anonymous singleton `get_client()` to per-request authenticated `get_user_client()` (6 KEEP sites annotated per Reviews M2 reconciliation: `get_current_user` line 490 + `get_session` line 502 = auth-API inspection of the anonymous singleton; `get_profile` line 711 + `get_user_corrections_count` line 721 = `TO public`-only fast-paths with Reviews M-GEMINI-1 forward-looking "MUST migrate if private fields added" caveats; `get_fragment_joins` line 1326 = legitimate Exception fallback after get_user_client raised; `get_feed_items` line 1579 = flipped MIGRATE→KEEP per Reviews M2 after Codex verified every branch is public-filtered at lines 1622 `status='approved'` and 1644 `is_public=True`, then KEEP retained with strengthened RLS evidence per round-2 Reviews R2-1 Option C — verified `docs/guides/SUPABASE_GUIDE.md:498-502` + `scripts/fix_rls_policies.sql:60-95` show the ONLY SELECT policy on discoveries is `TO public USING (is_hidden=false)`, NO admin SELECT branch exists, so anon and authenticated client roles return IDENTICAL rows regardless of `include_hidden=is_admin` — migration would NOT surface hidden rows), registered READER-01..READER-06 in REQUIREMENTS.md, installed permanent CI guards `tests/test_no_anonymous_reads_on_authenticated_tables.py` (AST scanner mirroring Phase 90 D-15 style: bans `get_client().table(<BANNED_TABLE>)` literal chains AND aliased `c = get_client(); c.table(<BANNED>)` patterns where BANNED_TABLES = {user_lists, list_items, recent_items, projects}; 6 seed-trap snippets + positive control + exempt-files self-check + partial-auth-table test + 2 xfail-strict blind-spot tests per Reviews M scanner: module-level alias + wrapper helper documented as known scanner blind spots) and `tests/test_supabase_client_reader_rls.py` (6 behavioral tests: 5 parametrized readers asserting `_apply_user_auth_to_client(access_token='good.future.jwt')` fires when storage has auth_session, + 1 anonymous-fallback negative control with Reviews L1 `get_client_mock.called is True` assertion to prove the anon fallback was actually taken), plus the BANNED_TABLES extension protocol in `docs/guides/SUPABASE_GUIDE.md` per Reviews M scanner. Plan 92.1-02 diagnosed Symptom 3 (`safe_user_get('auth_session') unexpected failure: app.storage.user can only be used within a UI context` in search-results → Add to list → Create new list → Save path) via INSTRUMENTED 3-point `request_contextvar` capture per round-2 Reviews R2-2 (P1 dialog-entry / P2 button-registration / P3 handler-firing — without all 3 readings, `contextvars.copy_context()` could capture None and be a silent no-op) and verified by Codex at `venv/Lib/site-packages/nicegui/storage.py:109-113` that `app.storage.user` is gated by `request_contextvar.get() is None`, NOT the NiceGUI client slot stack (Reviews C2). **NO-REPRO branch authorized** by Revision Blocker 3: Hillel's 2026-05-17 09:01-09:21 UTC reproduction yielded ALL THREE required evidence pieces — 3 successful list creations (`92.1-diag-1`/`92.1-norepro-2`/`92.1-norepro-3` persisted to Supabase `user_lists` rows 279/280/281 with `user_id=f8371f69-b7b8-49e9-bbd9-d79dfeecd7b8`), ZERO `safe_user_get('auth_session') unexpected failure` WARNING lines in server log, contextvar bound stably across P1/P2/P3/P3.5 capture points with same `id()`. Symptom 3 was eliminated as a side-effect of Plan 92.1-01's reader migration (the `user_lists.py` / `supabase_client.py` reader path now consistently uses `get_user_client()` which binds correctly through the dialog/click lifecycle in the Phase 90 architecture). Revision Blocker 2 mandatory `_create_and_add_handler` module-level helper extraction LOCKED-IN regardless of diagnosis outcome (the regression test cannot reliably assert correctness if the handler is buried inside a closure requiring the full NiceGUI dialog harness to instantiate); installed 5-test regression suite `tests/test_add_to_list_dialog_ui_context.py` exercising the REAL `lists_mgr.create_list → get_user_client → safe_user_get('auth_session')` chain per Reviews H1 AND controlling `nicegui.storage.request_contextvar` directly via `.set(None)` vs `.set(fake_request)` per round-2 Reviews R2-3 (5 tests: happy-path-real-chain + request_contextvar_none_does_not_warn + bound-contextvar-no-auth-session + error-path + empty-name) — NOT a mock of `lists_mgr.create_list` which would have bypassed the failing chain, AND NOT a downstream `RuntimeError` stub which would have bypassed the real gating mechanism. Case-driven binding fix (cases a/b/c/d per Reviews R2-2 — `copy_context()` wrapping for case-b/c or retry-on-AssertionError fallback for case-d) correctly SKIPPED — speculative wrap logic would constitute Rule 4 architectural change without evidence AND could mask future regressions by hiding the precise Symptom-3 WARNING signal under retry/wrap fallback. NOT a token snapshot replay per Reviews H-AGREED-1 (Gemini + Codex both round 1 flagged the captured-`auth_session` snapshot as a stale-token / logout-after-dialog-open footgun). `web/supabase_client.py` UNCHANGED in Plan 92.1-02. Plan 92.1-03 (this docs commit) flips status flags but marks the OPEN_ISSUES.md P1 entry `Fixed in code; verification pending SWEEP-05 smoke run 2` per Reviews M4 (the full `✅ Fixed (date)` + Open-count decrement happen in the smoke run 2 PASS commit, NOT here). Smoking gun in Phase 90 planning artifacts: `.planning/phases/90-auth-caching-rewrite-no-set-session/90-DISCUSSION-LOG.md:147` recorded the false assumption "`get_user_lists` reads work anonymously; only writes need auth" — but the `user_lists` SELECT RLS policy is `TO authenticated USING (auth.uid() = user_id)` per `docs/guides/SUPABASE_GUIDE.md:429-432`, so the anonymous role gets 0 rows post-Phase 90 (before Phase 90 the Supabase SIGNED_IN event listener auto-authenticated the singleton — Phase 90 correctly closed that channel for the multitenant-safety reason but missed the reader migration). Why no gate caught this: Phase 87 AST lint scanner only checks raw `app.storage.user` access (not RLS reachability); Phase 92-01 SWEEP-01 AST scan verified the same invariant correctly; Phase 90 tests mocked Supabase or exercised write paths; no live cross-user smoke test was run between Phase 90 ship (2026-05-14) and Phase 92-01 closeout (2026-05-17). Cross-AI review history: Plans 92.1-01..03 first passed internal `gsd-plan-checker` (revision iter 1: 4 BLOCKER + 6 WARNING → iter 2: 0 BLOCKER + 2 INFO), then went through round 1 of `/gsd-review --phase 92.1 --all` (Gemini MEDIUM + Codex HIGH) which caught 2 CRITICAL items the internal checker missed: count contradiction (Plan 92.1-01 said 13/5 but Plan 92.1-03 + SUMMARY template still said 15/3 — reconciled to 12/6 after applying Reviews M2 `get_feed_items` flip) and invalid NiceGUI fix mechanism (slot-stack rebinding does NOT control `request_contextvar`). A round-1 revision pass via `/gsd-plan-phase 92.1 --reviews` applied 4 MUST-FIX + 5 SHOULD-FIX + 3 LOW items. Round 2 of `/gsd-review` (Gemini LOW + Codex HIGH) caught 3 NEW HIGH-severity round-2 concerns: R2-1 (`get_feed_items` KEEP rationale incomplete for the admin `include_hidden=is_admin` path → resolved Option C with explicit RLS evidence citation), R2-2 (3-point `request_contextvar` capture mandatory before applying `copy_context()` — without it, the fix can capture None and be a silent no-op), R2-3 (regression test must directly control `request_contextvar.set(None|fake_request)`, NOT just simulate a downstream RuntimeError stub). A round-2 revision pass via `/gsd-plan-phase 92.1 --reviews` applied all 3 MUST-FIX + 4 SHOULD-FIX + 3 LOW round-2 items in-place before execution. Full pytest plan-boundary green at each plan: 1963 (Phase 91-03 close) → 1977 (Plan 92.1-01) → 1982 (Plan 92.1-02) → 1982 (Plan 92.1-03 docs-only). All 6 READER-XX requirements closed. Hand-off: Phase 92 Plan 92-02 (v7.12 closeout docs + MULTITENANT.md) remains blocked until Hillel re-runs SWEEP-05 smoke run 2 manually and commits PASS verdict — then Plan 92-02 Task 0 pre-flight gate flips OPEN_ISSUES.md P1 to `✅ Fixed (date)` with Open-count decrement and unblocks closeout. `deploy.sh` stays blocked until Plan 92-02 ships. Performance observation logged: `/lists` page rendering slowness observed 2026-05-17 during Plan 92.1-02 reproduction (UNRELATED to Symptom 3; possibly related to Plan 92.1-01 per-request authenticated client overhead); new P2 Medium row added to OPEN_ISSUES.md for follow-up investigation; NOT a blocker for Phase 92.1 ship or for SWEEP-05 smoke run 2. Zero user-visible behavior change EXCEPT: logged-in users now correctly see their lists / projects / recent items / private comments / own pending corrections — the very bug class this phase closed. Web-only. (web)
- **May 2026: v7.12 Phase 91 (Atomic Auth State Writes)** — internal milestone, not a release. Fifth phase of v7.12 Multitenant Architecture (Path B) refactor. Migrated the last 12 raw `app.storage.user` access sites (9 in `web/auth_state.py`, 3 in `web/main.py:complete_login`) to `safe_storage` chokepoint helpers. `set_auth` now returns `bool` with SYMMETRIC user/profile 2-key rollback (pops BOTH auth_user AND auth_profile on profile-write failure; does NOT own auth_session — round-2 NEW-M1 wording) AND treats `profile is None` as "clear stale auth_profile" (round-1 Revision MUST-2 from cross-AI review — Codex HIGH catch: stale auth_profile leaks role via GlobalAuthState.get_role()/is_admin()/is_editor() which read profile independently of user); `do_login` uses session-first multi-write ordering with DEFENSIVE 3-key caller-level cleanup on set_auth failure AND method-tagged posthog telemetry (`'method': 'password'` round-2 NEW-L2 parity with `_oauth_complete_login`'s `'google_oauth'`); OAuth `complete_login` factored out into module-level `_oauth_complete_login` helper (pattern-mapper testability seam) with D-06 multi-write rollback + DEFENSIVE 3-key caller cleanup + `show_error` UX on partial-write failure. Round-2 NEW-H1 module-top `from web.auth_state import GlobalAuthState` import added to `web/main.py` to prevent NameError at runtime; round-2 NEW-H2 (drop `app` from `from nicegui import` in `web/auth_state.py` for ruff F401) NOT applied — Plan 91-01 Rule-1 deviation: `create_login_dialog` still uses `app.storage.browser.*` for "Remember me" persistence (separate NiceGUI storage backend outside Phase 87 lint scope), so `app` is still legitimately referenced and ruff is clean. Phase 87 allowlist self-eliminates BOTH remaining entries (`web/auth_state.py` + `web/main.py`), taking the allowlist 2 → 0 (`allowed_raw_access: []`) — Phase 87 lint scanner now enforces zero raw `app.storage.user` accesses anywhere under `web/`. Installed two permanent CI guards: `tests/test_auth_callback_resilience.py` (7 tests — T-A prune-pre-write returns show_error WITHOUT navigate, T-B happy-path persists all 3 keys + navigates, T-C `GlobalAuthState.get_user()` under pruned storage returns None without AssertionError, **T-D set_auth SYMMETRIC 2-key partial-write rollback with stale-auth_profile pre-seed proving CLEAR-not-merely-no-write** (round-1 Revision MUST-3 + round-2 NEW-H4), **T-E _oauth_complete_login DEFENSIVE 3-key caller cleanup with stale-auth_user+auth_profile pre-seed proving CLEAR-not-merely-no-write** (round-1 Revision MUST-3 + round-2 NEW-H5), **T-F set_auth(profile=None) clears stale profile** (round-1 Revision MUST-3), plus 1 companion positive get_user test; uses `asyncio.run()` not `@pytest.mark.asyncio` to avoid pytest-asyncio dependency per Revision MUST-1; does NOT top-level `import pytest` per round-2 NEW-H3 ruff F401 closure) and `tests/test_persist_value_uses_safe_storage.py` (6 tests — 3 production AST assertions with **STRICT args check** verifying `safe_user_set(<first_param>, <second_param>)` Name references per Revision SHOULD-6, **1 BEHAVIORAL test** monkeypatching safe_storage backend to verify `session_persistence_enabled` False/True actually conditions the write per Revision MUST-5, plus 2 seed-trap sanity tests). Cross-AI review history: round 1 (Gemini + Codex, 2026-05-13) caught composite-key consolidation RMW race surface (F1 — pivoted to "keep 3 keys, swap raw → safe_user_*"; reduced surface ~70%), Phase 87 lint scanner empty-allowlist hard-assert (F3 — replaced with explanatory comment per D-07), and stale-auth_profile security/correctness leak (HIGH — encoded as Revision MUST-2 SYMMETRIC 2-key rollback + profile-is-None semantics). Round 2 (Codex only 2026-05-15; Gemini failed with HTTP 429 / quota exhaustion and was skipped per user direction) caught 5 NEW-H execution blockers (NEW-H1 module-top GlobalAuthState NameError, NEW-H2 ruff F401 on unused nicegui.app — false alarm; deviation logged in 91-01-SUMMARY.md, NEW-H3 ruff F401 on unused pytest import, NEW-H4 T-D test missing stale pre-seed, NEW-H5 T-E test missing stale pre-seed), 3 NEW-M scope/wording (NEW-M1 SYMMETRIC 2-key vs DEFENSIVE 3-key clarification, NEW-M2 PowerShell-friendly rg/Python verification, NEW-M3 plan-split into 91-02 strict + 91-03 closeout-docs), 2 NEW-L telemetry-polish (NEW-L1 double-posthog single-event consolidation deferred, NEW-L2 method:password tag consistency). Plan-revision pass via `/gsd-plan-phase --reviews` applied all MUST + SHOULD + NEW-H + NEW-M + NEW-L items in-place before execution. AUTHW-03 + AUTHW-04 already shipped in Phase 90 (sign_out server-side revocation via throwaway client, clear_auth revoke-before-pop with `finally:` cleanup); Phase 91 inherits unchanged. Plan-boundary pytest green at each plan (1949 → 1956 → 1962 → 1963 docs-only). 3 commits: Plan 91-01 (migration + tests), Plan 91-02 (retention guard), Plan 91-03 (this closeout docs commit). Zero user-visible behavior change EXCEPT: a prune-race during login now shows an error instead of silently leaving a half-logged-in state — security/correctness improvement; AND `GlobalAuthState.get_role()` can no longer return stale admin/editor role after a partial-write rollback or after a profile=None login establishment. Web-only milestone. Hand-off: Phase 92 (Final Sweep + Acceptance) does the cross-user concurrent smoke test, MULTITENANT.md docs write, Codex transcripts re-audit, AND addresses Revision MAY-8 (update_profile_cache cross-user write safety check) + NEW-L1 deferred posthog consolidation polish. (web)
- **May 2026: v7.11.2** — Composition Search Bug Fixes. Desktop-only patch. (1) `Min chunks` filter no longer inflated by repeated source phrases or cross-Tantivy-segment duplicates — `chunk_count` is now derived post-hoc from unique chunk_hits contents via `_count_unique_chunks` in both `search_composition_logic` and `lab_composition_search` (latter also had a latent always-zero `hits_count` bug at the full-mode filter, now fixed). (2) Expanded result view now scrolls to first highlighted match in both source and manuscript panes via `_scroll_to_first_highlight` in `desktop/result_dialog.py` (QTimer-deferred `QTextCursor.setPosition` + `ensureCursorVisible`). Power-user report after v7.11.1. Bundles v7.12 Path B refactor foundations (Phases 87-89) with zero user-visible change. (desktop)
- **May 2026: v7.12 Path B Phase 88 (State Separation by Deletion)** — internal milestone, not a release. Second phase of v7.12 Multitenant Architecture (Path B) refactor. Deletes the 10 per-user export-state singleton mirror fields from `web/state.py:AppState` (`last_results`, `current_search_query`, `current_search_mode`, `current_search_gap`, `last_filters_applied`, `last_search_warnings`, `last_selected_uids`, `parallels_results`, `parallels_filtered`, `parallels_search_meta`). Plan 88-01 migrated 13 writer sites across `web/pages/search.py`, `web/pages/search_results.py`, `web/pages/parallels.py` from `state.X = value` to local-variable threading through the existing `web.export_state.set_*` / `update_*` / `clear_*` calls (Codex round-5 catch: reorder of original plan ordering required because `set_search_export(...)` calls passed `state.current_search_gap` etc. as kwargs two lines below their assignments). Plan 88-02 rewrote `web/export_state.py` to route through Phase 87 `web.safe_storage` chokepoint helpers, deleted the production-code test-backend shim + selector helper (per D-09 + Phase 87 chokepoint discipline), hardened `update_*` functions with `isinstance(payload, dict)` guard (D-11) + copy-on-update (D-12), hardened getters `get_search_export()` / `get_parallels_export()` with isinstance guard (Refinement 4 — cross-AI review), folded `parallels_source_text` into the `set_parallels_export(meta={'source_text': ...})` payload (D-13), deleted the reader-side fallback at `web/api.py:1928-1931, 1962-1964, 2063-2066` (D-14), rewrote 4 affected test files (`test_export_cross_user_isolation`, `test_export_state_selection`, `test_api_export_json`, `test_api_legacy_unchanged`) to `monkeypatch.setattr('web.safe_storage.app', ...)` pattern with `SimpleNamespace`-based instance-isolated stubs (D-01, D-02, Refinement 6) — deleting the legacy proxy wrapper, dropping all `state.X =` fixture setup, adding a strengthened source_text cross-user leak regression test exercising a POSITIVE export path (D-15, Refinement 2), and deleted the `web/export_state.py` entry from `.planning/phase87_storage_allowlist.yaml` (allowlist count: 4 → 3 entries). Plan 88-03 deleted the 10 fields from `AppState.init()`, installed two permanent CI regression guards: runtime attr-absence test `tests/test_no_appstate_export_fields.py` (11 tests = 10 parametrized + 1 survivor sanity per D-06) and static AST scanner `tests/test_no_deleted_state_references.py` (D-07 — walks `web/` + `tests/` for `state.<deleted_field>` / `setattr(state, ...)` / `getattr(state, ...)` AND aliased imports per Refinement 5 (`from web.state import state as s`, `import web.state as web_state`), 4 tests = 3 seed-traps + 1 production scan), refreshed stale docstring/comment mentions per D-16 at `web/api.py:1846-1851` and `web/search_api.py:1198-1204`. Codex round-5 review reshaped plan ordering (locals-first instead of fields-first per D-04 + D-05) to eliminate the data-loss window where deletion-first ordering would feed stale defaults into `set_search_export(...)` kwargs. Cross-AI plan review (Gemini + Codex, 2026-05-13) refined plans pre-execution with 7 targeted improvements (scoped greps to `web/`+`tests/`, strengthened D-15 positive-path test, audit of `set_parallels_export(..., meta=None)` paths, getter isinstance guards, alias-import scanner coverage, SimpleNamespace stubs, Windows-tooling fallback notes). Full pytest suite green at each plan boundary (D-05). All 6 STATE-XX requirements satisfied. Zero user-visible behavior change. Web-only milestone — desktop unaffected. Hand-off chain: Phase 89 deletes `UserListsManager._cache_entry` singleton (LISTS-01..04); Phase 90 deletes `_client_cache` / `_session_locks` / `_CLIENT_CACHE_TTL` and the auth `_app.storage.user` allowlist entry (AUTHC-01..05); Phase 91 deletes the `web/auth_state.py` + `web/main.py` OAuth allowlist entries (AUTHW-01..06); Phase 92 final sweep + acceptance (SWEEP-01..06). (web)
- **May 2026: v7.12 Phase 87** (internal milestone) — Foundations for Path B multitenant refactor. `web/safe_storage.py` is now THE chokepoint for per-user state via `_session_uuid` lazy-mint helpers. Migrated 131 raw `app.storage.user.*` access sites across 14 files. AST-based pytest lint scanner (`tests/test_no_raw_storage_access.py`) is the permanent CI guard. Allowlist YAML covers bootstrap sites Phases 88-92 will delete. Zero user-visible change. (web)
- **May 2026: v7.11.0** — CUDL Coverage & Synthetic Inventories. Phases 84-86. FIST↔CUDL shelfmark bridge recovers thousands of CUDL classmarks. 108 image-bearing synthetic manuscripts injected (incl. T-S NS 329.96). **Deploy posture codified: scp DBs FIRST, then push code** (after 2026-05-11 incident). (both apps)
- **May 2026: v7.10.0** — Search API Public Release. Phases 77-83. Public HTTP/JSON endpoints `POST /api/search`, `GET /api/browse`, `POST /api/parallels`. OpenAPI at `/api/openapi.json`, Swagger at `/api/docs`. Reference skill `cairo-genizah-research`. `docs/SEARCH_API.md` is the public contract. (web)
- **May 2026: v7.9.4** — NLI Library Code Fix. Data-only: 461 manuscripts flipped Oxford → NLI in libraries.csv. (both apps)
- **April 2026: v7.9.3** — Visual Similarity Dialog Fixes. Firefox scroll, modifier-click open-in-new-tab, copy-paste includes shelfmarks. (web)
- **April 2026: v7.9.2** — PGP Data Refresh. pgp.db re-imported from upstream. (both apps)
- **April 2026: v7.9.1** — Catalog Attribution & Reading Desk Polish. FJMS Institution migration, JTS source-switch fix, CUL CUDL/NLI alignment helper, ilike injection sanitization. (both apps)
- **April 2026: v7.9.0** — Bundles v7.8 Structural Foundation + v7.9 Decomposition. Back-nav state-loss bugfix, CUL paired-leaf folio-label fix. Mostly internal refactor. (both apps)
- **April 2026: v7.7.2** — PageSpeed (a11y 85→96, perf 90→98). (web)
- **April 2026: v7.7.1** — SEO Round 2 (bilingual meta, JSON-LD, deferred PostHog). (web)
- **April 2026: v7.7.0** — Volume-Aware Browse. 3,193 multi-IE manuscripts get volume selector. (both apps)

## v7.12 Path B Milestone (active)

6 phases (87-92) + 1 inserted sub-phase (92.1) refactoring web for multitenant safety. `deploy.sh` BLOCKED until ships.
- Phase 87 ✅ done (Foundations — session UUID + safe_storage chokepoint)
- Phase 88 ✅ done (State separation by deletion — 10 AppState mirror fields gone; D-06 runtime + D-07 static AST regression guards installed; allowlist 4→3)
- Phase 89 ✅ done (Lists cache per-request — UserListsManager._cache_entry deleted; LISTS-01..04 closed)
- Phase 90 ✅ done (Auth caching rewrite — request-scoped auth via local header mutation; 6 dead-code names + 3 CI guards atomic-installed; allowlist 3→2; AUTHW-03/04 pulled forward)
- Phase 91 ✅ done (Atomic auth-state writes — 12 raw accesses migrated; SYMMETRIC 2-key set_auth + DEFENSIVE 3-key caller cleanup; allowlist 2→0; 7+6 CI guards; AUTHW-01..06 closed)
- Phase 92.1 ✅ done (code; verification pending) — Reader-Client Retrofit inserted 2026-05-17 after Phase 92 SWEEP-05 smoke run 1 FAILED. 3 plans: reader migration (12 sites → get_user_client(); 6 KEEP annotated per Reviews M2; Reviews R2-1 Option C strengthened the 1579 KEEP with explicit RLS evidence) + AST scanner + 5+6+5 CI guards + Symptom-3 NO-REPRO branch + `_create_and_add_handler` mandatory refactor + 5-test regression suite. READER-01..06 closed. OPEN_ISSUES.md P1 entry marked `Fixed in code; verification pending SWEEP-05 smoke run 2` per Reviews M4. Awaiting Hillel's smoke run 2 PASS commit.
- Phase 92: final sweep + acceptance (SWEEP-01..06; cross-user smoke test + MULTITENANT.md docs) — Plan 92-02 BLOCKED behind Hillel's manual SWEEP-05 smoke run 2 PASS commit; after PASS, Plan 92-02 Task 0 pre-flight gate flips OPEN_ISSUES P1 to ✅ Fixed (date) with Open-count decrement and proceeds with v7.12 closeout docs.
