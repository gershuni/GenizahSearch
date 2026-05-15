# Roadmap: GenizahSearch

## Milestones

- **v1 External Data Integration** -- Phases 1-7 (shipped 2026-02-07)
- **v5.6.0 Desktop Parity & PGP Integration** -- Phases 8-12 (shipped 2026-02-09)
- **v5.7.0 Responsa Search** -- Phases 14-17 (shipped 2026-02-10)
- **v5.7.2 Cleanup, Normalization & Sections** -- Phases 18-21 (shipped 2026-02-11)
- **v5.7.3 Pending Corrections Visibility** -- Phases 22-24 (shipped 2026-02-11)
- **v5.8.0 FJMS Integration** -- Phases 25-28 (shipped 2026-02-15)
- **v5.9.0 Multi-Source Image & Metadata Integration** -- Phases 29-34 (shipped 2026-02-16)
- **v6.0.0 Local Data Architecture** -- Phases 35-40 (shipped 2026-02-22)
- **v6.1.0 Catalog Browse & Navigation** -- Phase 41 (shipped 2026-02-27)
- **v6.5.0 Search UX & Filtered Search** -- Phases 42-46 (shipped 2026-03-14)
- **v7.0.0 Fragment Puzzle** -- Phases 47-52 (shipped 2026-03-17)
- **v7.1.0 FIST Gap Fill** -- Phase 53 (shipped 2026-03-19)
- **v7.6 Search Refinement & Scholarly Joins** -- Phases 54-57 (shipped 2026-03-31)
- **v7.7 Volume-Aware Browse** -- Phases 58-61 (shipped 2026-04-01)
- **v7.8 Structural Foundation** -- Phases 63-66 (shipped 2026-04-15)
- **v7.9 Decomposition** -- Phases 67-76 (complete 2026-04-17)
- **v7.10 Search API** -- Phases 77-83 (shipped 2026-05-05)
- **v7.11 CUDL Coverage & Synthetic Inventories** -- Phases 84-86 (shipped 2026-05-12)
- **v7.12 Multitenant Architecture (Path B)** -- Phases 87-92 (active 2026-05-13)

## Phases

<details>
<summary>v1 External Data Integration (Phases 1-7) -- SHIPPED 2026-02-07</summary>

See: .planning/milestones/v1-ROADMAP.md

9 phases (including inserted 7.1, 7.2), 18 plans, 173 min total execution.
Imported 7,090 PGP documents with 9,364 transcription/translation sources.
Full PGP feature set in web app.

</details>

<details>
<summary>v5.6.0 Desktop Parity & PGP Integration (Phases 8-12) -- SHIPPED 2026-02-09</summary>

See: .planning/milestones/v5.6.0-ROADMAP.md

5 phases, 25 plans, ~134 min total execution.
Desktop PGP feature parity, Virtual Reading Desk, 35,839 PGP documents imported.
Phase 13 (Transcription Search) deferred -- index build too slow for desktop.

</details>

<details>
<summary>v5.7.0 Responsa Search (Phases 14-17) -- SHIPPED 2026-02-10</summary>

See: .planning/milestones/v5.7.0-ROADMAP.md

4 phases, 14 plans.
Responsa Project-style advanced search with syntax parsing, JA expansion, tabular query builder, explosion guards.
25/25 requirements satisfied. 221 automated Responsa tests.

</details>

<details>
<summary>v5.7.2 Cleanup, Normalization & Sections (Phases 18-21) -- SHIPPED 2026-02-11</summary>

See: .planning/milestones/v5.7.2-ROADMAP.md

4 phases, 11 plans.
Dead AI code removed, Unicode search normalization, full green test suite (447 tests),
structural HTML section parser for PGP transcriptions.
13/13 requirements satisfied.

</details>

<details>
<summary>v5.7.3 Pending Corrections Visibility (Phases 22-24) -- SHIPPED 2026-02-11</summary>

See: .planning/milestones/v5.7.3-ROADMAP.md

3 phases, 3 plans.
Pending corrections visible as selectable version in web and desktop version selectors.
Shared corrections service, amber styling (web), emoji labels (desktop).
6/6 requirements satisfied. 20 milestone-specific tests.

</details>

<details>
<summary>v5.8.0 FJMS Integration (Phases 25-28) -- SHIPPED 2026-02-15</summary>

See: .planning/milestones/v5.8.0-ROADMAP.md

4 phases, 12 plans.
FJMS scholarly metadata (domains, joins, catalog) integrated via SQLite sidecar.
Domain filtering, scientific joins with scholar attribution, catalog enrichment in both apps.
19/19 requirements satisfied. 38+ tests covering service layer and integration.

</details>

<details>
<summary>v5.9.0 Multi-Source Image & Metadata Integration (Phases 29-34) -- SHIPPED 2026-02-16</summary>

See: .planning/milestones/v5.9.0-ROADMAP.md

6 phases, 22 plans (including 3 gap closure plans), 76 commits.
NLI crossref sidecar (815K records), Cambridge IIIF (141K), Manchester LUNA (28K), JTS/Princeton Figgy (453).
Multi-source image viewing with folio navigation, bibliography (542K), catalog refs (64K), physical metadata.
11/14 requirements satisfied, 1 invalidated (FGP!=FL), 2 deferred (REL-01/REL-02).

</details>

<details>
<summary>v6.0.0 Local Data Architecture (Phases 35-40) -- SHIPPED 2026-02-22</summary>

See: .planning/milestones/v6.0.0-ROADMAP.md

6 phases, 21 plans (8 core + 8 bug-fix/cleanup + 5 performance optimization), 155 commits.
PGP data migrated to local pgp.db sidecar (147MB). FJMS catalog descriptions expanded (4 new tables, ~1.7M rows).
Desktop offline PGP browsing. All desktop crashes fixed. Paginated search (PAGE_SIZE=50).
Performance: parallel NLI fetch, browse crossref parallelization, FL ID index, variant cache unification.
14/14 requirements satisfied (audit passed).

</details>

<details>
<summary>v6.1.0 Catalog Browse & Navigation (Phase 41) -- SHIPPED 2026-02-27</summary>

1 phase, 4 plans.
Faceted browsing by domain hierarchy, author, and work title in both apps.
FIST v5.0.0 enrichment (genizah_persons, genizah_titles, code_values), FTS5+domain text filter,
cross-links between browse and catalog browse pages. 72 tests.

</details>

<details>
<summary>v6.5.0 Search UX & Filtered Search (Phases 42-46) -- SHIPPED 2026-03-14</summary>

See: .planning/milestones/v6.5.0-ROADMAP.md

5 phases, 26 plans, 244 commits.
Search UX overhaul (timer, ETA, partial results, printed filter), session persistence,
Hebrew library names, bidirectional filtered search (domain/author/work/date/material),
~580K Dicta translations for multilingual access. Origin: power user feedback letter (17 requests).

</details>

<details>
<summary>v7.0.0 Fragment Puzzle (Phases 47-52) -- SHIPPED 2026-03-17</summary>

6 phases, 15 plans.
Visual jigsaw tool for assembling physical joins from manuscript fragment images with background removal,
DPI calibration, recto/verso views, join document persistence, and community publishing --
in both web (NiceGUI + Fabric.js) and desktop (PyQt6 + QGraphicsScene).

</details>

<details>
<summary>v7.1.0 FIST Gap Fill (Phase 53) -- SHIPPED 2026-03-19</summary>

1 phase, 2 plans.
Added 38,673 Genizah manuscripts from FIST.db that were missing from libraries.csv.
Browsable with images and FJMS enrichment. Metadata search guard fix. 7 new library codes.

</details>

<details>
<summary>v7.6 Search Refinement & Scholarly Joins (Phases 54-57) -- SHIPPED 2026-03-31</summary>

See: .planning/milestones/v7.6-ROADMAP.md

5 phases (+ 55.1 inserted), 17 plans, 206 commits, 151 files changed (+28K/-3.7K lines).
Manuscript dimensions display + filtering, search within results with breadcrumb chain,
exclude known manuscripts (lists/files/paste), FIST visual similarity browse + search mode,
lightweight browse first-render. 14/14 requirements satisfied.

</details>

<details>
<summary>v7.7 Volume-Aware Browse (Phases 58-61) -- SHIPPED 2026-04-01</summary>

4 phases, 8 plans, 13 commits.
Fixed multi-IE image/text mismatch for 3,193 manuscripts (1.5%) by making search->browse->paging
IE-aware across both apps. IE volume data infrastructure, web + desktop volume selector dropdown,
per-IE paging, volume-correct images for external providers (Manchester/Oxford/Cambridge/JTS),
auto-default to external sources when NLI is down, session persistence for active volume,
community writes (corrections/comments) include IE context.

</details>

<details>
<summary>v7.8 Structural Foundation (Phases 63-66) -- SHIPPED 2026-04-15</summary>

See: .planning/milestones/v7.8-ROADMAP.md

4 phases, 9 plans, 64 commits, 173 files changed (+6,269/-828 lines).
CI safety net with GitHub Actions (Ubuntu + Windows matrix, ruff + check_docs + pytest),
two-file dependency pinning (14 direct + 115 transitive), Supabase auth migration
(gotrue -> supabase_auth, PKCE-only OAuth), 205+ silent exception handlers audited across
76 first-party files, isolated NiceGUI monkey-patches with version guards, repo root
cleanup (.gitignore 50->126 lines, untracked root 67->1), documentation refresh
(CODE_INDEX, OPEN_ISSUES, DEVELOPER_GUIDE). 12/12 requirements satisfied.
Zero user-visible behavior changes.

</details>

<details>
<summary>v7.9 Decomposition (Phases 67-76) -- COMPLETE 2026-04-17</summary>

10 phases, 23 plans.
Decomposition of largest source files into focused modules. Desktop split: ResultDialog,
filter/scholarly dialogs, image viewers (ManuscriptViewerWidget, FullscreenImageWindow),
puzzle canvas, VS cache, widgets extracted into desktop/ package. Web split:
search.py -> search_state.py + search_results.py; browse.py -> browse_state.py + browse_enrichment.py.
Page-scoped state refactor reducing app.storage.user sprawl. Back-navigation state-loss bugfix
(regression from 2026-03-27 commit 829cd7cf). Zero user-visible behavior change except the
back-nav bugfix.

</details>

<details>
<summary>v7.10 Search API (Phases 77-83) -- SHIPPED 2026-05-05</summary>

See: .planning/milestones/v7.10-ROADMAP.md

8 phases (77, 78, 79, 80, 81A, 81B, 82, 83), 37 plans.
Public HTTP/JSON research-automation API over the Genizah corpus: `/api/search` (keyword/Responsa/title/shelfmark with rate limiting, mode gating, error envelope), `/api/browse` (stateless drill-down returning text + metadata + image URLs), `/api/parallels` (composition matching). Security hardening (XFF spoofing, fail-closed filter validation, MAX_EXPANDED_TERMS=500, HMAC-hashed PostHog telemetry). OpenAPI auto-generated at `/api/openapi.json` + Swagger at `/api/docs`. Reference Claude skill `cairo-genizah-research` (file-locked token-bucket throttling, browse-honesty annotations). 36/36 in-traceability requirements + 8 PUBLIC-* satisfied (deployed to production 2026-05-05). Web-only release: no git tag, no GitHub Release.

</details>

<details>
<summary>v7.11 CUDL Coverage & Synthetic Inventories (Phases 84-86) -- SHIPPED 2026-05-12</summary>

3 phases, 14 plans (84: 5/5, 85: 5/5, 86: 4/5 executed + optional release plan).
FIST-CUDL bridge (shared/fist_cudl_bridge.py + shared/shelfmark_bridge.py) with normalizers for Mosseri label form, Cambridge Or. numeric collapse, CUL slash/comma/dot/leading-zero fixes; 6 wiring call sites. Synthetic libraries.csv infrastructure: is_synthetic_sys_id helper, Option-2 18-digit format, browse hide-NLI gates, is_synthetic on API responses, corrections-write reject. 108 image-bearing synthetic manuscripts injected (101 CUL + 7 Mosseri). T-S NS 329.96 (originating case) resolved. 5-tier CUDL coverage audit (96.23% phase84_hit, 0.08% synthetic, 1.13% residue needing human-in-loop). Deploy posture codified: scp DBs FIRST, then push code.

</details>

### v7.12 Multitenant Architecture (Path B) (Phases 87-92) -- ACTIVE

**Milestone Goal:** Refactor GenizahSearch's web layer off the desktop-inherited single-user mental model so per-user state, auth, and caches cannot leak across concurrent sessions sharing one Python process. The cross-user xlsx export leak (v7.11.1) was one instance of a class of bugs surfaced across 4 rounds of Codex review spanning AppState singleton mirrors, UserListsManager instance caching, process-wide auth client cache, and raw `app.storage.user` access at 30+ bootstrap sites.

**Hard constraint:** No mid-flight `auth.set_session()` calls. Codex verified at `gotrue_client.py:713` that `set_session()` is networked (calls `get_user(access_token)` when JWT valid, `_refresh_access_token(refresh_token)` when expired), not a local state mutation. All auth code in this milestone must respect this finding.

**Scope:** Web-only (desktop is genuinely single-user; unaffected).

## Phase Details

### Phase 87: Foundations -- Session UUID and Safe Storage Chokepoint
**Goal**: Land `_session_uuid` and adopt `web/safe_storage.py` as the single chokepoint adapter for per-user state, so all subsequent phases have a stable cache key and a zero-raw-storage invariant to build on.
**Depends on**: Nothing (first phase of this milestone; Phase 86 complete)
**Requirements**: FOUND-01, FOUND-02, FOUND-03, FOUND-04, FOUND-05
**Success Criteria** (what must be TRUE):
  1. A second concurrent browser session never receives the same `_session_uuid` as the first session across 100 simulated independent requests — each session's UUID is minted once, stored in that session's `app.storage.user`, and never shared.
  2. A static grep of `web/` for raw `app.storage.user.get(`, `app.storage.user.pop(`, and `app.storage.user[` returns only entries that appear in the Phase 87 allowlist file — every other call site has been migrated to a `safe_storage` helper.
  3. The allowlist file contains a per-entry justification comment for every remaining raw access (e.g., bootstrap code that runs before session existence is guaranteed).
  4. The CI lint check (grep-based or ruff custom rule) added in FOUND-04 rejects a synthetic test file containing a raw `app.storage.user.get(` call outside the allowlist, and passes the production code unchanged.
  5. All 6 existing `tests/test_safe_storage.py` tests pass without modification.
**Plans**: 8 plans (87-01 through 87-08)
- [x] 87-01-VALIDATION-FOUNDATION-PLAN.md -- Failing test stubs + allowlist scaffold (H1 schema)
- [x] 87-02-SESSION-UUID-HELPERS-PLAN.md -- get_session_uuid + ensure_session_uuid helpers + B1 bootstrap wiring
- [x] 87-03-LEAF-FILE-MIGRATIONS-PLAN.md -- 5 simple files (text_editor, translation_report, home, settings, search_results)
- [x] 87-04-MAIN-AND-ALIAS-MIGRATIONS-PLAN.md -- main.py + api.py (nicegui_app alias) + supabase_client.py (_app alias)
- [x] 87-05-BROWSE-CLUSTER-MIGRATIONS-PLAN.md -- browse.py + browse_state.py + catalog_browse.py + tests/test_browse_state.py (B3)
- [x] 87-06-SEARCH-CLUSTER-MIGRATIONS-PLAN.md -- parallels.py + search.py + search_state.py + tests/test_search_state.py (B3)
- [x] 87-07-LINT-FINALIZATION-PLAN.md -- Lint scanner + allowlist finalization (H1)
- [x] 87-08-ACCEPTANCE-AND-DOCS-PLAN.md -- Docs + STATE.md + human smoke-check (confirms B1)

### Phase 88: State Separation by Deletion
**Goal**: Delete singleton mirrors on `AppState` so `web/export_state.py` is the only path for per-user export state, with the `_TEST_BACKEND` shim replaced by proper test fixtures.
**Depends on**: Phase 87
**Requirements**: STATE-01, STATE-02, STATE-03, STATE-04, STATE-05, STATE-06
**Success Criteria** (what must be TRUE):
  1. Static grep of `web/state.py:AppState` returns zero matches for the 10 deleted per-user fields (`last_results`, `current_search_query`, `current_search_mode`, `current_search_gap`, `last_filters_applied`, `last_search_warnings`, `last_selected_uids`, `parallels_results`, `parallels_filtered`, `parallels_search_meta`) — they do not exist on the class in any form.
  2. A user opens two concurrent browser sessions, searches in session A, then triggers an xlsx export in session B; the exported file contains session B's result set (or an empty/error response if B has no results) — never session A's results.
  3. Static grep of `web/export_state.py` returns zero matches for `_TEST_BACKEND` — the shim is gone and tests use proper fixture or adapter injection.
  4. `tests/test_export_cross_user_isolation.py` passes and asserts cross-user isolation directly against per-session storage, with no reference to `_TEST_BACKEND`.
  5. `tests/test_export_state_selection.py`, `tests/test_api_export_json.py`, and `tests/test_api_legacy_unchanged.py` all pass after dropping any `state.*` setup — they use only `export_state` helpers.
**Plans**: 3 plans (88-01 through 88-03)
- [x] 88-01-writer-migration-PLAN.md -- Migrate 13 writer sites to local variables (state.X = → local_X = ; thread through export_state setters)
- [x] 88-02-export-state-rewrite-PLAN.md -- Rewrite export_state.py via safe_storage chokepoint; delete _TEST_BACKEND + reader-side parallels_source_text fallback; rewrite 4 test files; remove allowlist entry
- [x] 88-03-appstate-deletion-and-enforcement-PLAN.md -- Delete 10 AppState fields; install runtime + static AST regression guards (D-06 + D-07); refresh stale docs (D-16)

### Phase 89: Lists Cache Per-Request
**Goal**: Drop the `UserListsManager` singleton and 10s TTL plumbing entirely; per-request instantiation becomes the simpler safe pattern.
**Depends on**: Phase 87
**Requirements**: LISTS-01, LISTS-02, LISTS-03, LISTS-04
**Success Criteria** (what must be TRUE):
  1. Static grep of `web/state.py:AppState` returns zero matches for `_user_lists_mgr` — the singleton attribute is gone.
  2. Static grep of `web/user_lists.py` returns zero matches for `_cache_entry` and the 10s TTL constant — the time-based cache plumbing does not exist in the codebase.
  3. A user logged in as User A opens the lists page; User B (different session, different user account) opens the lists page within what would have been the 10s TTL window; User B sees their own lists, not User A's.
  4. `tests/test_user_lists_cache_isolation.py` passes and is written against the per-request model (no references to cache TTL, user_id keys, or singleton behavior).
**Plans**: 2 plans (89-01 through 89-02)
- [x] 89-01-PLAN.md -- Per-access factory + stateless fetch + delegation audit + test rewrite (LISTS-02, LISTS-03, LISTS-04)
- [x] 89-02-PLAN.md -- Singleton deletion + Phase 88 survivor-test fix (D-09) + static AST guard + runtime attr-absence test (LISTS-01, LISTS-03)

### Phase 90: Auth Caching Rewrite -- No set_session
**Goal**: Replace the process-wide auth client cache with request-scoped auth that does NOT call `auth.set_session()` to set headers; refresh locking keyed by `_session_uuid` with no cached client objects.
**Depends on**: Phase 87
**Requirements**: AUTHC-01, AUTHC-02, AUTHC-03, AUTHC-04, AUTHC-05
**Success Criteria** (what must be TRUE):
  1. Static grep of `web/supabase_client.py` returns zero matches for `_client_cache`, `_session_locks`, `_locks_guard`, and `_CLIENT_CACHE_TTL` — the process-wide cache is gone.
  2. Static grep for `.auth.set_session(` across `web/` returns matches only inside the explicitly allowed OAuth bootstrap helper/path (the one place a session is legitimately established from an authorization code) — zero matches elsewhere. Substring `set_session(` alone is too broad and would false-fail on the legitimate OAuth helper; precision matters.
  3. Refresh-only locks are keyed by `_session_uuid` values (not access tokens, not storage object IDs) — a token refresh mid-flight does not orphan the lock or create a second lock for the same session.
  4. Static grep returns zero matches for `auth_resurrection` or the resurrection guard function name introduced in commit `cca23db3` — the guard is removed because the cache that made it necessary no longer exists.
  5. A code comment in the auth path documents the Codex finding (citing `gotrue_client.py:713`) explaining why `set_session()` is not called mid-flight, visible to future contributors without requiring them to find the Codex transcripts.
**Plans**: 2 plans (90-01 through 90-02)
- [x] 90-01-PLAN.md -- Behavior rewrite (get_user_client/sign_in/sign_out/set_session_from_url/exchange_code_for_session/4 retry blocks/clear_auth reorder/profile.py change_password) + Codex round-1/2/3 fixes + AUTHC-05 docstring + allowlist 3->2 (AUTHC-02, AUTHC-03, AUTHC-04, AUTHC-05)
- [x] 90-02-PLAN.md -- Delete 4 globals + 2 helpers + atomic-commit install of 3 permanent CI guards: static AST scanner with 13 seed traps (D-15), runtime attr-absence over 6 names (D-16), behavioral refresh-lock test Tests A/B/C (D-17) (AUTHC-01, AUTHC-03, AUTHC-04)

### Phase 91: Atomic Auth State Writes
**Goal**: Migrate auth state writes through safe_storage helpers; `sign_out` revokes server-side on the user's authenticated client before popping `auth_session`.
**Depends on**: Phase 87, Phase 90
**Requirements**: AUTHW-01, AUTHW-02, AUTHW-03, AUTHW-04, AUTHW-05, AUTHW-06
**Success Criteria** (what must be TRUE):
  1. Static grep of `web/auth_state.py` functions `set_auth`, `clear_auth`, and `do_login` returns zero matches for raw `app.storage.user[` or `app.storage.user.pop(` — every write goes through a `safe_storage` helper.
  2. The `sign_out` flow calls `client.auth.sign_out()` using the user's own authenticated client (verified by the client carrying the user's access token) before any local auth keys are popped from storage; local key cleanup happens in a `finally` block so it runs even when server revocation fails.
  3. A mid-flight session prune (NiceGUI `AssertionError` on pruned storage) during the OAuth callback does not produce a 500 to the browser — the callback handles the prune gracefully and returns a meaningful error page or redirect.
  4. `tests/test_auth_callback_resilience.py` (or AUTHW-05 equivalent) passes, asserting that a simulated prune during OAuth callback does not propagate an `AssertionError`.
  5. The `persist_value` safe-wrap in `web/components/filter_panel.py` introduced in commit `cca23db3` is retained and passes the existing filter-panel tests.
**Plans**: 2 plans (91-01 through 91-02)
- [x] 91-01-PLAN.md -- Migration of 12 raw access sites + multi-write rollback (set_auth bool/do_login session-first/complete_login show_error UX) + complete_login factored out of auth_callback_route for testability + tests/test_auth_callback_resilience.py with 3 D-08 tests + allowlist self-elimination 2->0 + Phase 87 lint scanner empty-allowlist fix per D-07 (Codex F3) (AUTHW-01, AUTHW-02, AUTHW-05; AUTHW-03/AUTHW-04 inherited from Phase 90)
- [x] 91-02-PLAN.md -- tests/test_persist_value_uses_safe_storage.py AST retention guard for filter_panel.py:persist_value (3 production assertions + 2 seed-trap sanity tests) atomic-CI-guard commit, no production code touched (AUTHW-06)

### Phase 92: Final Sweep and Acceptance
**Goal**: Audit `web/` for any remaining raw `app.storage.user` accesses, re-validate against the 4 Codex transcripts, run cross-user smoke tests, and document the architecture in `docs/guides/MULTITENANT.md`.
**Depends on**: Phase 87, Phase 88, Phase 89, Phase 90, Phase 91
**Requirements**: SWEEP-01, SWEEP-02, SWEEP-03, SWEEP-04, SWEEP-05, SWEEP-06
**Success Criteria** (what must be TRUE):
  1. A full `grep -r "app\.storage\.user" web/` scan produces only entries that appear in the Phase 87 allowlist — the count of unallowlisted raw accesses is zero.
  2. The two Codex round-4 deferred sites -- `parallels.py:3520` (deferred-callback raw access) and `text_editor.py` (auto-save raw access) -- are confirmed migrated to `safe_storage` helpers, verified by static grep showing neither file contains raw storage access outside the allowlist.
  3. Two concurrent browser sessions execute the full research workflow (search → browse → lists → xlsx export) simultaneously; inspection of each session's exported xlsx and list contents shows no cross-session data; the test is documented in `SWEEP-05` smoke-test plan with pass/fail checkboxes.
  4. Each issue previously flagged in the 4 Codex review transcripts (`_tmp/codex_*_response.txt`) is either marked "addressed" with a pointer to the commit/phase that fixed it, or "waived" with an explicit written rationale -- no issue is left silently unaddressed.
  5. `docs/guides/MULTITENANT.md` exists and documents: the `safe_storage` chokepoint pattern, the `_session_uuid` stable cache key, the request-scoped auth strategy with the `set_session()` prohibition, the per-request lists instantiation, and the deletion-not-migration discipline -- sufficient for a future contributor to understand and extend the architecture without reading the Codex transcripts.
**Plans**: TBD

## Progress

| Phase | Milestone | Plans Complete | Status | Completed |
|-------|-----------|----------------|--------|-----------|
| 84. CUDL Shelfmark Normalization | v7.11 | 5/5 | Complete | 2026-05-12 |
| 85. Synthetic FJMS Inventory Rows | v7.11 | 5/5 | Complete | 2026-05-12 |
| 86. CUDL Coverage Audit + Synthetic Re-attempt | v7.11 | 4/5 | Complete | 2026-05-12 |
| 87. Foundations -- Session UUID and Safe Storage Chokepoint | v7.12 | 8/8 | Complete    | 2026-05-13 |
| 88. State Separation by Deletion | v7.12 | 3/3 | Complete    | 2026-05-14 |
| 89. Lists Cache Per-Request | v7.12 | 2/2 | Complete   | 2026-05-15 |
| 90. Auth Caching Rewrite -- No set_session | v7.12 | 2/2 | Complete   | 2026-05-15 |
| 91. Atomic Auth State Writes | v7.12 | 2/3 | In Progress|  |
| 92. Final Sweep and Acceptance | v7.12 | 0/TBD | Not started | - |

## Backlog

### Phase 999.1: Search results by folio (BACKLOG — PLANNED)

**Goal:** Web search-result parity with desktop — surface the page/image number (`display['img']`, same field desktop's `COL_IMG` renders at `genizah_app.py:16111`) on each search-result card so users see which folio a hit came from without opening Quick View. Strict parity scope; folio labels (`1r`/`2v`), grouping, sorting, and parallels-list extension are all explicitly deferred.
**Requirements:** FOLIO-01
**Plans:** 2/3 plans executed

Plans:
- [ ] 999.1-01-PLAN.md — Render display.img inline after shelfmark on result card title line; human smoke-check (FOLIO-01)

### Phase 999.2: Filtering by PGP (BACKLOG — PLANNED)

**Goal:** Add a 3-state post-search PGP filter toggle (`All` / `Has PGP` / `No PGP`) to the web `/search` results toolbar, modeled directly on the existing `printed_filter` pattern at `web/pages/search.py:1402-1434`. Plus an active-filter chip in the results header (co-located with `exclusion_chips_row`) and session persistence via `web/safe_storage.py`. Web only — parallels and desktop explicitly out of scope (D-12). The filter operates on the in-memory result list using `search_state.transcription_sys_ids` (the same set that drives the green PGP badge in `search_results.py:397-400`); no search-pipeline changes.
**Requirements:** PGP-FILTER-01, PGP-FILTER-02, PGP-FILTER-03, PGP-FILTER-04, PGP-FILTER-05
**Plans:** 1 plan

Plans:
- [ ] 999.2-01-PLAN.md — pgp_filter field on SearchUIState + bootstrap read + cycle handler + label/color updater + button after printed_filter_btn + cascade integration (printed → PGP → measurement) + active-filter chip with click-to-clear + post-enrichment visibility flip + New Search reset + human smoke-check (PGP-FILTER-01..05)

### Phase 999.3: Adding PGP to downloaded data (BACKLOG — PLANNED)

**Goal:** Extend the Excel **and** JSON search-results exports with PGP scholarly metadata so researchers can sort, filter, and cite PGP data directly from the downloaded artifact (spreadsheet OR JSON). Excel gains 6 columns (`PGP URL`, `PGP Description`, `PGP Type`, `PGP Date`, `PGP Languages`, `PGP Tags`) appended after `Full Text`. JSON gains a per-item `pgp` subobject using the canonical 10-key `_build_pgp_subset` projection — or `null` when no PGP record. Both surfaces consume ONE shared helper (`shared/search_serializer.py:_pgp_subset_for_sys_id`) so they never drift. Word, list, and parallels exports explicitly OUT OF SCOPE per D-10; parallels JSON envelope also untouched (injection happens at the `serialize_search_payload` loop layer, not inside the shared `_serialize_item`, so `_to_parallels_envelope_item` does not inherit it). Always-English (D-04), pipe-delimited multi-values with no spaces (D-05), empty cells / `null` (NOT `{}`) for missing data (D-06). Also fixes a latent character-iteration bug in `languages_primary`/`languages_secondary` projection (pgp.db stores these as comma-separated TEXT, not JSON) via the new `_split_pgp_languages` internal — without modifying `_build_pgp_subset` so browse stays stable.
**Requirements:** PGP-EXPORT-01, PGP-EXPORT-02, PGP-EXPORT-03, PGP-EXPORT-04, PGP-EXPORT-05
**Plans:** 1 plan

Plans:
- [ ] 999.3-01-PLAN.md — `_pgp_subset_for_sys_id` shared helper (with languages-string-to-list fix) + `serialize_search_payload` per-item `pgp` injection at loop layer (parallels JSON untouched) + 6 Excel columns appended after `Full Text` in `export_search_results_excel` + 23 new pytest cases (11 helper + 6 JSON + 6 Excel) + full-suite regression net + human smoke-check on real Excel/JSON downloads (PGP-EXPORT-01..05)

### Phase 999.4: Line numbering (BACKLOG — PLANNED)

**Goal:** Display a right-side (RTL leading-edge) line-number gutter next to the transcription text on 5 surfaces: web Browse single-page viewer (`web/pages/browse.py:4206 render_text_content`), web Browse PGP/translation/V0.x version views (same render path via `handle_version_change`), web Quick View dialog (`web/pages/search_results.py:1768 render_text_section`), desktop Browse tab `browse_text` QTextEdit (5 transcription render call sites in `genizah_app.py`: `:3635, :8580, :9314, :9947, :21470`), and desktop ResultDialog `text_ms` QTextBrowser (4 setHtml sites at `:1260, :1270, :1931, :2120`). Numbering semantics anchored to `text.split('\n')` matching the existing Responsa `L<N>:word` search syntax at `genizah_core.py:4976-4994` (blank lines counted, 1-based, restart per folio). Toggle in transcription header (icon `format_list_numbered`), default ON, persisted per-user (web: `safe_storage` key `ui.show_line_numbers`; desktop: `load_app_config` key `show_line_numbers`). Hard requirement (D-04): the gutter must be a SEPARATE DOM element (web: CSS-grid sibling column with `user-select: none`) / SIBLING widget (desktop: LineNumberArea attached to the body widget's viewport) so user mouse-drag-select + Ctrl+C captures body text WITHOUT the line numbers. Display-only — no click handlers, no deep links, no `L<N>:` injection (D-12). Explicitly OUT OF SCOPE: web search-results inline accordion at `search_results.py:688` (D-03), Parallels/Composition results, cross-folio cumulative numbering, web Browse `view_all` Full Manuscript multi-page scroll at `:2587-2598` (not in D-01).
**Requirements:** LINE-NUM-01..10
**Plans:** 2 plans

Plans:
- [ ] 999.4-01-PLAN.md — WEB: `_render_line_numbered_html(text, highlight_html, line_height, font_size, show_line_numbers)` helper added module-scope in `web/pages/browse.py` (CSS-grid two-column with `user-select: none` on `.line-number-gutter` span; counts via `text.split('\n')`; preserves `<mark>` tags from pre-built highlight HTML; XSS-safe when `highlight_html=None` via `html.escape`). Wired into `render_text_content` at `:4206-4225` (Browse single-page incl. PGP/translation/V0.x version views — same render path). Toggle button (icon `format_list_numbered`, tooltip via `tr('Toggle line numbers')`) injected into `version_row` at `:4254` with persistence via `safe_user_get/safe_user_set` under key `ui.show_line_numbers`. Imported and reused in `render_text_section` at `web/pages/search_results.py:1768` (Quick View receives pre-built HTML; reads `current_display_text['value']` for line counting). Toggle button injected into view-mode header row at `:1859-1862`. 12 structural tests in `tests/test_line_numbers_web.py` cover: line count, blank lines, copy-paste invariant (regex-strip the gutter span — body unaffected), XSS safety, RTL direction, line_height parameter, disabled passthrough, pre-built-HTML highlight survival, `<br>`-normalization, Quick View pre-built-HTML path. Human-verify checkpoint covers all 3 web surfaces, copy-paste invariant (D-04), RTL, persistence across reload, folio reset. Phase 87 lint scanner (`tests/test_no_raw_storage_access.py`) MUST stay green. (LINE-NUM-01..06, LINE-NUM-09)
- [ ] 999.4-02-PLAN.md — DESKTOP: new file `desktop/widgets/line_number_text_edit.py` (~150 lines) — `LineNumberArea(QWidget)` painted as sibling of body widget (canonical Qt code-editor line-number-area pattern); `apply_line_numbered_text(widget, html_or_text, *, source_text, is_html)` helper attaches the LineNumberArea on first call, recomputes line count on subsequent calls; `is_line_numbers_enabled()` / `set_line_numbers_enabled(bool)` / `refresh_visibility(widget)` API. RTL flip places gutter on visual right via `setViewportMargins` + manual geometry. Persistence via `load_app_config()`/`save_app_config({'show_line_numbers': bool})` (mirrors existing `show_translations` precedent at `genizah_app.py:2237-2245`). Default ON per D-07. 8 headless Qt tests in `tests/test_line_numbers_desktop.py` (skip cleanly if PyQt6 missing): attaches, line count matches `text.split('\n')`, clipboard isolation (`toPlainText` excludes numbers — D-04 trivially true via sibling-widget design), toggle hides/shows, config persistence default True, works for QTextBrowser too, recompute on repeated call, RTL layout. Plan 02 wires `genizah_app.py:6576 self.browse_text` through helper at 6 call sites (5 transcription HTML renders at `:3635, :8580, :9314, :9947, :21470` + edit-cancel restore at `:3365`; loading-state setText calls at `:9823, :9851, :21100` left unchanged — non-transcription content); adds `# Lines` toolbar QPushButton (`checkable=True`, tooltip `tr('Toggle line numbers')`). Wires `desktop/result_dialog.py:452 self.text_ms` through helper at 4 setHtml call sites (`:1260, :1270, :1931, :2120`) with explicit `source_text=` passing the raw text variable in each site's scope (`text` for `_rd_display_text`/`_rd_display_pgp_text`; `ms_raw` and `raw_text` for the other two — executor must verify variable scope per site). Toggle button in dialog find-row. Shared `show_line_numbers` config key — toggling in one surface affects the other on NEXT render (live cross-surface signal-bus is out of scope, follow-up). Human-verify checkpoint covers both desktop surfaces, D-04 copy-paste invariant (Ctrl+A+Ctrl+C from body produces no digits), RTL, cross-surface persistence after app restart, cross-app parity with web. (LINE-NUM-07, LINE-NUM-08, LINE-NUM-09, LINE-NUM-10)

---
*Roadmap created: 2026-02-09*
*Last updated: 2026-05-15 -- Phase 91 (Atomic Auth State Writes) planned: 2 plans across 2 waves, both autonomous=true. 91-01 (wave 1): 5 tasks migrating the last 12 raw `app.storage.user` access sites (9 in `web/auth_state.py` lines 42/50/95/97/117/138/139/140/187, 3 in `web/main.py:complete_login` lines 1441/1443/1446) to `safe_storage` helpers; `set_auth` returns `bool` with multi-write rollback per D-04; `do_login` uses session-first ordering with rollback per D-05; `complete_login` factored out of `auth_callback_route` into module-level `_oauth_complete_login` helper per pattern-mapper finding 1 (testability seam); D-06 `show_error` UX on partial-write failure; `tests/test_no_raw_storage_access.py:200` empty-allowlist assertion fix per D-07 (Codex F3 catch); both file-entry blocks deleted from `.planning/phase87_storage_allowlist.yaml` per D-07b (allowlist 2 -> 0, final state `allowed_raw_access: []`); `tests/test_auth_callback_resilience.py` installs 3 D-08 tests (T-A prune-pre-write returns show_error without navigate, T-B happy-path persists all 3 keys + navigates, T-C `GlobalAuthState.get_user()` under pruned storage returns None) + 1 companion positive test (closes AUTHW-01, AUTHW-02, AUTHW-05; AUTHW-03 + AUTHW-04 inherited from Phase 90 D-11/D-11b). 91-02 (wave 2, depends_on 91-01): 2 tasks installing `tests/test_persist_value_uses_safe_storage.py` AST retention guard for `web/components/filter_panel.py:220:persist_value` safe-wrap (originally landed in commit cca23db3, 2026-05-12 Codex 3rd-pass CRITICAL fix) — 3 production assertions per D-09 (imports safe_user_get + safe_user_set, gates on session_persistence_enabled, writes via safe_user_set with NO raw `app.storage.user[k] = v`) + 2 seed-trap sanity tests (passing snippet passes all 3 checks; failing snippet trips raw-subscript check). Single-test-file atomic-commit per Phase 89 D-09 / Phase 90 D-13 discipline, no production code touched. Plus STATE.md/ROADMAP.md/CLAUDE.md updates flipping Phase 91 to Complete (closes AUTHW-06). Codex round 1 review caught 3 BLOCKING + 5 MEDIUM findings: F1 (composite-key consolidation creates new RMW race surface via refresh path — pivoted to keep-3-keys architecture, reduced surface ~70%), F2 (migration helper bypassed by supabase_client.py readers — mooted by F1), F3 (Phase 87 lint scanner hard-asserts empty allowlist — encoded as D-07), M2 (do_login multi-write return-value check — encoded as D-05), M3 (T-C navigate-mock not invoked — encoded as D-08 T-C reshape), M5 (AUTHW-06 AST guard justified over grep — encoded as D-09). After Phase 91 ships, Phase 87 lint scanner enforces zero raw `app.storage.user` accesses anywhere under `web/` — Phase 92's SWEEP-01/SWEEP-02 becomes verification rather than discovery. Earlier note: Phase 999.4 (Line numbering, backlog) planned: 2 plans across 1 wave, autonomous=true for 999.4-01 (web; 3 tasks: helper + browse wiring + Quick View wiring with final human-verify checkpoint) and autonomous=false for 999.4-02 (desktop; 4 tasks: shared LineNumberArea widget + browse_text wiring + ResultDialog text_ms wiring + final human-verify checkpoint). 999.4-01 (wave 1, web): builds `_render_line_numbered_html(text, highlight_html, line_height, font_size, show_line_numbers)` module-scope helper in `web/pages/browse.py` using CSS-grid two-column layout (gutter `<span class="line-number-gutter">` with `user-select: none` + body `<div class="line-numbered-body">`); D-04 copy-paste invariant achieved structurally by gutter living in a separate grid column with `user-select: none`. Wires `render_text_content` at `:4206-4225` (covers single-page + all version views via `handle_version_change`) AND imports the same helper into `render_text_section` at `web/pages/search_results.py:1768` (Quick View). Toggle button (icon `format_list_numbered`) in `version_row` (browse) + view-mode header row (Quick View). Persistence via `safe_user_get/safe_user_set` under key `ui.show_line_numbers` (default True per D-07). 12 structural tests in `tests/test_line_numbers_web.py` covering line count, blank-line numbering (D-10 — `text.split('\n')` invariant aligned to Responsa `L<N>:` parser at genizah_core.py:7679-7691), gutter copy-paste invariant (regex-strip the span and the remainder equals body), XSS safety, RTL direction, line_height parameter, disabled passthrough, `<br>`-normalization for callers that pre-converted `\n→<br>`, Quick View pre-built-HTML highlight survival. Web Browse `view_all` Full-Manuscript-View at `:2587-2598` NOT touched (D-01 scopes only `render_text_content`); flagged in human-verify checkpoint as follow-up. Web search-results inline accordion at `search_results.py:688` explicitly out of scope (D-03). 999.4-02 (wave 1, desktop): builds new file `desktop/widgets/line_number_text_edit.py` (~150 lines) with `LineNumberArea(QWidget)` (canonical Qt code-editor line-number-area pattern adapted for RTL via `setViewportMargins(0,0,gutter_w,0)` in RTL or `(gutter_w,0,0,0)` in LTR + manual area-widget geometry) and `apply_line_numbered_text(widget, rendered, *, source_text, is_html)` helper. D-04 copy-paste invariant achieved trivially: LineNumberArea is a SIBLING QWidget (not part of QTextDocument), so Qt text cursor cannot extend into it; `toPlainText()` of body excludes numbers (Test 3). Wires `genizah_app.py:6576 self.browse_text` at 6 call sites (5 HTML transcription renders `:3635/:8580/:9314/:9947/:21470` + 1 edit-cancel `:3365`; loading-state `:9823/:9851/:21100` setText calls intentionally left raw — non-transcription messages). Wires `desktop/result_dialog.py:452 self.text_ms` at 4 setHtml call sites (`:1260/:1270/:1931/:2120`) with explicit `source_text=` from raw variables in each scope (`text`/`text`/`ms_raw`/`raw_text`). Toolbar QPushButton (`# Lines`, `checkable=True`, tooltip `tr('Toggle line numbers')`) in Browse tab AND in ResultDialog find-row. Persistence via `load_app_config()`/`save_app_config({'show_line_numbers': bool})` (mirrors existing `show_translations` precedent at genizah_app.py:2237-2245). Default ON per D-07. Shared config key means toggling in one desktop surface affects the other on next render (live signal-bus deferred). 8 headless Qt tests in `tests/test_line_numbers_desktop.py` (skip cleanly if PyQt6 missing): widget attaches, line count == `text.split('\n')` count, clipboard isolation via `toPlainText`, toggle hides/shows, config persistence default True invariant, works for QTextBrowser too, recompute on repeated call updates existing LineNumberArea (idempotent), RTL layout positions area on visual right. Mints requirements LINE-NUM-01..10 (LINE-NUM-01..06+09 = web in Plan 01; LINE-NUM-07/08/09/10 = desktop in Plan 02; LINE-NUM-09 — `text.split('\n')` numbering invariant — shared across both plans). Out of scope per CONTEXT.md Deferred Ideas: web search-results inline accordion (D-03), Parallels/Composition results, click-to-deep-link, click-to-`L<N>:`-insert, continuous cross-folio numbering. Earlier note: Phase 999.3 (Adding PGP to downloaded data, backlog) planned: 1 plan, wave 1, autonomous=false (one human-verify checkpoint at end). 999.3-01 (wave 1): 5 tasks shipping PGP metadata into BOTH Excel and JSON search-results exports via ONE shared helper `_pgp_subset_for_sys_id` in `shared/search_serializer.py` (mirrors `_build_pgp_subset`'s 10-key shape, adds `_split_pgp_languages` to fix latent character-iteration bug in `languages_primary`/`languages_secondary` projection — comma-separated TEXT in pgp.db, NOT JSON). Task 1: helper + 11 unit tests covering B1-B9 contract (short-circuit, exception resilience, language-format fixes, no-transcription guarantee). Task 2: `'pgp'` injection at `serialize_search_payload` loop layer (NOT inside `_serialize_item`, so `_to_parallels_envelope_item` does not inherit it — D-10 holds for parallels JSON) + 6 envelope-shape tests including explicit parallels-JSON-untouched regression. Task 3: 6 Excel columns appended after `Full Text` at `web/export_service.py:286-355` — `PGP URL | PGP Description | PGP Type | PGP Date | PGP Languages | PGP Tags` — with pipe-delimited multi-values (D-05 no spaces), English-only data (D-04 no `get_language()`), empty cells for missing PGP (D-06), `inferred_date_display → doc_date_standard → doc_date_original` fallback chain, all-LTR alignment for new cells, plus 6 Excel-shape tests. Task 4: full-suite regression + ruff + static-grep audit. Task 5: human smoke-check across Excel/JSON downloads + negative D-10 checks (Word/list/parallels exports untouched). D-09 short-circuit supported by helper signature (`available_sys_ids` kwarg) but NOT wired through `set_search_export()` in this phase — deferred follow-up; helper currently does 1 SQLite lookup per result on download (bounded by 200-result page cap × ~1ms). Mints requirements PGP-EXPORT-01..05. Touches `shared/search_serializer.py` + `web/export_service.py` + 2 test files only. Web-only — desktop untouched per phase scope. Earlier note: Phase 999.2 (Filtering by PGP, backlog) planned: 1 plan, wave 1, autonomous=false (one human-verify checkpoint at end). 999.2-01 (wave 1): 7 tasks adding a 3-state PGP filter toggle to web `/search` results toolbar mirroring the existing `printed_filter` pattern end-to-end — bootstrap read at :148, cycle handler + label/color updater + button construction after `printed_filter_btn` at :1430-1434, `_apply_pgp_filter` predicate wired into both `_apply_printed_filter_and_render` and `_apply_domain_exclusions` cascades per D-11, active-filter chip co-located with `exclusion_chips_row` at :1448-1449 per D-08 with click-to-clear, post-enrichment visibility flip in `_apply_enrichment_to_ui` at :4436-4444, New Search reset at :2042-2050. Mints requirements PGP-FILTER-01..05. Persistence goes through `safe_storage` chokepoint (Phase 87 lint preserved). Parallels page and desktop app explicitly OUT OF SCOPE per D-12. Earlier note: Phase 999.1 (Search results by folio, backlog) planned: 1 plan, wave 1, autonomous=false (single human-verify checkpoint). 999.1-01 (wave 1): single-task UI render addition in `web/pages/search_results.py:468` surfacing `display['img']` as `· {num}` after the shelfmark — strict desktop-parity scope per locked decisions D-01..D-05 in 999.1-CONTEXT.md. Mints requirement FOLIO-01. Earlier note: Phase 90 (Auth Caching Rewrite -- No set_session) planned: 2 plans across 2 waves. 90-01 (wave 1): behavior rewrite (get_user_client/sign_in/sign_out/set_session_from_url/exchange_code_for_session/4 retry blocks/profile.py change_password helper/clear_auth revoke-before-pop reorder) per Codex round-1/2/3 fixes + AUTHC-05 docstring + Phase 87 allowlist self-elimination (3->2) (closes AUTHC-02, AUTHC-03, AUTHC-04, AUTHC-05). 90-02 (wave 2, depends_on 90-01): atomic deletion of 4 globals + 2 helpers + install of 3 permanent CI guards (static AST scanner D-15 with 10 seed traps, runtime attr-absence D-16, behavioral refresh-lock test D-17 Tests A/B/C) (closes AUTHC-01, finalizes AUTHC-03, closes AUTHC-04). All 5 AUTHC-XX requirements covered.*
