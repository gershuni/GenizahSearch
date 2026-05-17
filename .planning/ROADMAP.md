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
**Plans**: 3 plans (91-01 through 91-03; 91-01 + 91-02 close requirements; 91-03 is closeout docs)
- [x] 91-01-PLAN.md -- Migration + SYMMETRIC 2-key set_auth + DEFENSIVE 3-key caller cleanup + AUTHW-05 test (T-A/T-B/T-C/T-D/T-E/T-F + companion = 7 tests) + allowlist self-elimination + Phase 87 lint fix + round-2 NEW-H1/H2/H3/H4/H5 + NEW-M1/M2 + NEW-L2 (AUTHW-01, AUTHW-02, AUTHW-05; inherits AUTHW-03/AUTHW-04 from Phase 90)
- [x] 91-02-PLAN.md -- AUTHW-06 retention guard for filter_panel.py:persist_value (3 AST STRICT + 1 BEHAVIORAL + 2 seed-trap = 6 tests); strict single-test-file per round-2 NEW-M3 plan-split (AUTHW-06)
- [x] 91-03-PLAN.md -- Closeout docs flip (STATE.md / ROADMAP.md / CLAUDE.md / OPEN_ISSUES.md) per round-2 NEW-M3; no AUTHW-XX requirements

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
**Plans**: 2 plans (92-01 + 92-02)
- [x] 92-01-PLAN.md -- SWEEP-01..05: AST scan + 5-surface audit + thematic transcript audit + smoke scaffold (closes SWEEP-01, SWEEP-02, SWEEP-03, SWEEP-04, SWEEP-05)
- [ ] 92-02-PLAN.md -- SWEEP-06: docs/guides/MULTITENANT.md + closeout docs (depends_on 92-01; gated on human smoke PASS commit per D-02; closes SWEEP-06)

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
| 91. Atomic Auth State Writes | v7.12 | 3/3 | Complete    | 2026-05-15 |
| 92. Final Sweep and Acceptance | v7.12 | 1/2 | In Progress|  |
| 92.1. Reader-Client Retrofit | v7.12 | 3/3 | Complete   | 2026-05-17 |

## Backlog

### Phase 92.2: lists-performance-investigation (INSERTED)

**Goal:** Close the `/lists` 36s warm-render regression introduced by Phase 92.1 (12 readers moved to per-request authenticated `get_user_client()` triggered an `~4 + L + 2*S` Supabase fanout + ~30 Client builds per render). Two plans / two waves. Plan 92.2-01 is instrumentation-only and commits a permanent forensic JSON baseline artifact (D-VER-01..05). Plan 92.2-02 applies the Codex-adjusted fix: task-scoped `WeakKeyDictionary` memo on `get_user_client()` keyed by `asyncio.current_task()` + `(_session_uuid, access_token)` (D-MEMO-01..04); `data`+`counts` threaded once through `/lists` render path (D-FANOUT-01); batched Supabase RPC `get_list_item_counts_for_user(uuid)` replaces per-list fanout (D-FANOUT-02); NLI cache `[WinError 5]` race fix (D-NLI-01); SUPABASE_GUIDE.md wording fix (D-DOC-01). HARD GATE: `/lists` ≤5s warm before Plan 92-02 closeout can run. `deploy.sh` stays blocked until this ships AND SWEEP-05 smoke run 2 passes.
**Requirements**: None minted (urgent insert; must_haves derived from CONTEXT.md `<domain>` Phase Boundary)
**Depends on:** Phase 92
**Plans:** 2/2 plans complete

Plans:
- [x] 92.2-01-PLAN.md — Instrumentation-only: commit baseline JSON forensic artifact for `/lists` warm render on Hillel’s real environment (D-VER-01..05)
- [x] 92.2-02-PLAN.md — Fix: task-scoped memo (D-MEMO-01..04) + threaded data/counts (D-FANOUT-01) + batched RPC (D-FANOUT-02) + NLI cache lock+retry (D-NLI-01) + SUPABASE_GUIDE.md wording fix (D-DOC-01); HARD GATE `/lists` ≤5s warm

### Phase 92.1: Reader-Client Retrofit (INSERTED)

**Goal:** Close the P0 reader-client RLS-reachability regression introduced by Phase 90 D-09/D-10. Migrate ~12 reader functions in `web/supabase_client.py` from the anonymous singleton `get_client()` to the per-request authenticated `get_user_client()`; install an AST-scanner CI guard and behavioral regression tests; trace and fix the secondary `safe_user_get('auth_session')` UI-context console error in the add-to-list-dialog "Create new list" path. After ship, Phase 92 SWEEP-05 smoke run 2 must PASS R0 before Plan 92-02 closeout docs can run.
**Requirements:** READER-01, READER-02, READER-03, READER-04, READER-05, READER-06
**Depends on:** Phase 92
**Plans:** 3/3 plans complete
**Status:** Complete (code; smoke pending) -- 2026-05-17

Plans:
- [x] 92.1-01-PLAN.md — Reader migration in web/supabase_client.py + AST scanner CI guard + behavioral regression tests + READER-XX registration in REQUIREMENTS.md (READER-01, READER-02, READER-04, READER-05)
- [x] 92.1-02-PLAN.md — Diagnose Symptom 3 (NO-REPRO branch — bug eliminated as side-effect of 92.1-01) + mandatory _create_and_add_handler refactor + 5-test regression guard (READER-03)
- [x] 92.1-03-PLAN.md — Closeout docs: ROADMAP/STATE/OPEN_ISSUES/CLAUDE.md "Recently Changed" + SUMMARY.md (READER-06)

### Phase 999.1: Search results by folio (BACKLOG — PLANNED)

**Goal:** Web search-result parity with desktop — surface the page/image number (`display['img']`, same field desktop's `COL_IMG` renders at `genizah_app.py:16111`) on each search-result card so users see which folio a hit came from without opening Quick View. Strict parity scope; folio labels (`1r`/`2v`), grouping, sorting, and parallels-list extension are all explicitly deferred.
**Requirements:** FOLIO-01
**Plans:** 1/2 plans executed

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
**Requirements:** METADATA-EXPORT-01, METADATA-EXPORT-02, METADATA-EXPORT-03, METADATA-EXPORT-04, METADATA-EXPORT-05, METADATA-EXPORT-06, METADATA-EXPORT-07 (formerly PGP-EXPORT-01..05 — INVALIDATED by 2026-05-17 scope expansion; full goal text + audit notes will be rewritten in Plan 03)
**Plans:** 3 plans across 3 waves

Plans:
- [ ] 999.3-01-PLAN.md — State plumbing + JSON additions. Extends `set_search_export(...)` with 3 enrichment kwargs; adds `update_search_export_enrichment(...)` sibling; wires 3 callsites in `web/pages/search.py` + 1 post-enrichment update; extends `_serialize_item` + `serialize_search_payload` with conditional emission of `has_pgp`/`is_printed`; `/api/export/json` passes the cast sets through. Parallels JSON D-10 preserved. 17 new tests (METADATA-EXPORT-01..04).
- [ ] 999.3-02-PLAN.md — Xlsx restructure + 4 lookup helpers + 3 sheets. Creates NEW `shared/export_dossier.py` with 4 helpers (PGP/NLI/Catalog/Bibliography) + `_split_pgp_languages` bug fix. Restructures `export_search_results_excel` into 3-sheet builder (`Genizah Results` + `Manuscripts` + `Bibliography`). Endpoint passes session enrichment through. 38 new tests (METADATA-EXPORT-05..06).
- [ ] 999.3-03-PLAN.md — Human smoke verification on real Excel + JSON downloads (4 test scenarios, D-04/D-05/D-06/D-10 invariant checks); REQUIREMENTS.md update with INVALIDATED predecessor note; ROADMAP.md final-shape update. autonomous=false. (METADATA-EXPORT-07).

### Phase 999.4: Line numbering (BACKLOG — PLANNED)

**Goal:** Display a right-side (RTL leading-edge) line-number gutter next to the transcription text on 5 surfaces: web Browse single-page viewer (`web/pages/browse.py:4206 render_text_content`), web Browse PGP/translation/V0.x version views (same render path via `handle_version_change`), web Quick View dialog (`web/pages/search_results.py:1768 render_text_section`), desktop Browse tab `browse_text` QTextEdit (5 transcription render call sites in `genizah_app.py`: `:3635, :8580, :9314, :9947, :21470`), and desktop ResultDialog `text_ms` QTextBrowser (4 setHtml sites at `:1260, :1270, :1931, :2120`). Numbering semantics anchored to `text.split('\n')` matching the existing Responsa `L<N>:word` search syntax at `genizah_core.py:4976-4994` (blank lines counted, 1-based, restart per folio). Toggle in transcription header (icon `format_list_numbered`), default ON, persisted per-user (web: `safe_storage` key `ui.show_line_numbers`; desktop: `load_app_config` key `show_line_numbers`). Hard requirement (D-04): the gutter must be a SEPARATE DOM element (web: CSS-grid sibling column with `user-select: none`) / SIBLING widget (desktop: LineNumberArea attached to the body widget's viewport) so user mouse-drag-select + Ctrl+C captures body text WITHOUT the line numbers. Display-only — no click handlers, no deep links, no `L<N>:` injection (D-12). Explicitly OUT OF SCOPE: web search-results inline accordion at `search_results.py:688` (D-03), Parallels/Composition results, cross-folio cumulative numbering, web Browse `view_all` Full Manuscript multi-page scroll at `:2587-2598` (not in D-01).
**Requirements:** LINE-NUM-01..10
**Plans:** 2 plans

Plans:
- [ ] 999.4-01-PLAN.md — WEB: `_render_line_numbered_html(text, highlight_html, line_height, font_size, show_line_numbers)` helper added module-scope in `web/pages/browse.py` (CSS-grid two-column with `user-select: none` on `.line-number-gutter` span; counts via `text.split('\n')`; preserves `<mark>` tags from pre-built highlight HTML; XSS-safe when `highlight_html=None` via `html.escape`). Wired into `render_text_content` at `:4206-4225` (Browse single-page incl. PGP/translation/V0.x version views — same render path). Toggle button (icon `format_list_numbered`, tooltip via `tr('Toggle line numbers')`) injected into `version_row` at `:4254` with persistence via `safe_user_get/safe_user_set` under key `ui.show_line_numbers`. Imported and reused in `render_text_section` at `web/pages/search_results.py:1768` (Quick View receives pre-built HTML; reads `current_display_text['value']` for line counting). Toggle button injected into view-mode header row at `:1859-1862`. 12 structural tests in `tests/test_line_numbers_web.py` cover: line count, blank lines, copy-paste invariant (regex-strip the gutter span — body unaffected), XSS safety, RTL direction, line_height parameter, disabled passthrough, pre-built-HTML highlight survival, `<br>`-normalization, Quick View pre-built-HTML path. Human-verify checkpoint covers all 3 web surfaces, copy-paste invariant (D-04), RTL, persistence across reload, folio reset. Phase 87 lint scanner (`tests/test_no_raw_storage_access.py`) MUST stay green. (LINE-NUM-01..06, LINE-NUM-09)
- [ ] 999.4-02-PLAN.md — DESKTOP: new file `desktop/widgets/line_number_text_edit.py` (~150 lines) — `LineNumberArea(QWidget)` painted as sibling of body widget (canonical Qt code-editor line-number-area pattern); `apply_line_numbered_text(widget, html_or_text, *, source_text, is_html)` helper attaches the LineNumberArea on first call, recomputes line count on subsequent calls; `is_line_numbers_enabled()` / `set_line_numbers_enabled(bool)` / `refresh_visibility(widget)` API. RTL flip places gutter on visual right via `setViewportMargins` + manual geometry. Persistence via `load_app_config()`/`save_app_config({'show_line_numbers': bool})` (mirrors existing `show_translations` precedent at `genizah_app.py:2237-2245`). Default ON per D-07. 8 headless Qt tests in `tests/test_line_numbers_desktop.py` (skip cleanly if PyQt6 missing): attaches, line count matches `text.split('\n')`, clipboard isolation (`toPlainText` excludes numbers — D-04 trivially true via sibling-widget design), toggle hides/shows, config persistence default True, works for QTextBrowser too, recompute on repeated call, RTL layout. Plan 02 wires `genizah_app.py:6576 self.browse_text` through helper at 6 call sites (5 transcription HTML renders at `:3635, :8580, :9314, :9947, :21470` + edit-cancel restore at `:3365`; loading-state setText calls at `:9823, :9851, :21100` left unchanged — non-transcription content); adds `# Lines` toolbar QPushButton (`checkable=True`, tooltip `tr('Toggle line numbers')`). Wires `desktop/result_dialog.py:452 self.text_ms` through helper at 4 setHtml call sites (`:1260, :1270, :1931, :2120`) with explicit `source_text=` passing the raw text variable in each site's scope (`text` for `_rd_display_text`/`_rd_display_pgp_text`; `ms_raw` and `raw_text` for the other two — executor must verify variable scope per site). Toggle button in dialog find-row. Shared `show_line_numbers` config key — toggling in one surface affects the other on NEXT render (live cross-surface signal-bus is out of scope, follow-up). Human-verify checkpoint covers both desktop surfaces, D-04 copy-paste invariant (Ctrl+A+Ctrl+C from body produces no digits), RTL, cross-surface persistence after app restart, cross-app parity with web. (LINE-NUM-07, LINE-NUM-08, LINE-NUM-09, LINE-NUM-10)

---
*Roadmap created: 2026-02-09*
*Last updated: 2026-05-17 -- Phase 92.1 (Reader-Client Retrofit) code shipped: 3 plans / 3 waves closing the P0 reader-client RLS-reachability regression introduced by Phase 90 D-09/D-10. Plan 92.1-01 migrated 12 reader call sites in `web/supabase_client.py` from anonymous singleton `get_client()` to per-request authenticated `get_user_client()` (6 KEEP sites annotated per Reviews M2 reconciliation; final 12 MIGRATE / 6 KEEP split after Reviews R2-1 Option C strengthened the 1579 `get_feed_items` KEEP rationale with explicit RLS evidence — verified the ONLY SELECT policy on discoveries is `TO public USING (is_hidden=false)` with NO admin SELECT branch); installed AST scanner CI guard `tests/test_no_anonymous_reads_on_authenticated_tables.py` (BANNED_TABLES = {user_lists, list_items, recent_items, projects}; 6 seed-trap snippets + positive control + exempt-files self-check + partial-auth-table test + 2 xfail-strict blind-spot tests per Reviews M scanner) + behavioral regression `tests/test_supabase_client_reader_rls.py` (5 parametrized readers + 1 anonymous-fallback negative control with Reviews L1 `get_client_mock.called is True` assertion) + BANNED_TABLES extension protocol in `docs/guides/SUPABASE_GUIDE.md`. Plan 92.1-02 diagnosed Symptom 3 via instrumented 3-point `request_contextvar` capture per round-2 Reviews R2-2 — NO-REPRO branch authorized: Hillel's 2026-05-17 09:01-09:21 UTC reproduction yielded 3 successful list creations (`92.1-diag-1`/`92.1-norepro-2`/`92.1-norepro-3` persisted at Supabase `user_lists` rows 279/280/281 with correct user_id) + ZERO `safe_user_get('auth_session') unexpected failure` WARNINGs + contextvar bound stably across P1/P2/P3/P3.5 with same `id()`. Symptom 3 was eliminated as a side-effect of Plan 92.1-01's reader migration. Mandatory `_create_and_add_handler` module-level helper refactor locked-in regardless (Revision Blocker 2) + 5-test regression suite `tests/test_add_to_list_dialog_ui_context.py` installed (exercises REAL `lists_mgr.create_list → get_user_client → safe_user_get('auth_session')` chain per Reviews H1 + directly controls `nicegui.storage.request_contextvar` via `.set(None)` vs `.set(fake_request)` per Reviews R2-3). Case-driven binding fix (cases a/b/c/d) correctly SKIPPED — speculative wrap would mask future regressions. `web/supabase_client.py` UNCHANGED in 92.1-02. Plan 92.1-03 (this closeout) flipped STATE/ROADMAP/OPEN_ISSUES/CLAUDE.md to reflect code-ship state; OPEN_ISSUES.md P1 entry marked `Fixed in code; verification pending SWEEP-05 smoke run 2` per Reviews M4 (NOT `✅ Fixed (date)`; full ✅-status + Open-count decrement happen in smoke run 2 PASS commit owned by Plan 92-02's Task 0 pre-flight gate). Cross-AI review history: round 1 caught C1 count contradiction + C2 slot-stack-vs-request_contextvar mechanism + H-AGREED-1 stale-token footgun + H1 mock-bypasses-failing-chain; round 2 caught R2-1 `get_feed_items` admin-path evidence + R2-2 3-point-capture mandate + R2-3 regression-test-must-control-request_contextvar-directly. All MUST + SHOULD items applied via `/gsd-plan-phase --reviews` before execution. Full pytest plan-boundary green: 1963 → 1977 → 1982 → 1982 (docs-only). All 6 READER-XX requirements closed. Phase 92 Plan 92-02 (v7.12 closeout docs + MULTITENANT.md) remains BLOCKED behind Hillel's SWEEP-05 smoke run 2 PASS commit per `.planning/phases/92-final-sweep-and-acceptance/92-SWEEP-05-SMOKE.md`. `deploy.sh` stays blocked until Plan 92-02 ships and v7.12 milestone is declared shipped. Earlier note: 2026-05-15 -- Phase 91 (Atomic Auth State Writes) shipped: 3 plans (2 closing AUTHW-XX + 1 closeout-docs) across 3 waves; AUTHW-01..06 closed; Phase 87 allowlist 2 -> 0 (final state allowed_raw_access: []); permanent CI guards installed for AUTHW-05 (7 tests in test_auth_callback_resilience.py incl. Revision MUST-3 T-D/T-E/T-F partial-rollback with round-2 NEW-H4/H5 stale pre-seeding) and AUTHW-06 (6 tests in test_persist_value_uses_safe_storage.py incl. Revision MUST-5 behavioral test). Cross-AI review revisions integrated pre-execution: round 1 MUST-1/2/3/4/5 + SHOULD-6 (Gemini + Codex 2026-05-13); round 2 NEW-H1/H2/H3/H4/H5 + NEW-M1/M2/M3 + NEW-L1/L2 (Codex only 2026-05-15; Gemini 429 skipped). Codex round 1 catches encoded: F1 architectural pivot (keep 3 keys, no composite consolidation), F3 empty-allowlist assertion fix, HIGH stale-auth_profile security/correctness fix. Round 2 catches: 5 execution blockers (NameError, ruff F401 x2, T-D/T-E stale-seed gaps -- NEW-H2 ruff F401 on nicegui.app NOT applied per Plan 91-01 Rule-1 deviation because `app.storage.browser.*` still referenced in `create_login_dialog`) + 3 scope-discipline + 2 telemetry-polish. Next: Phase 92 (Final Sweep and Acceptance) -- SWEEP-01..06. Earlier note: Phase 999.4 (Line numbering, backlog) planned: 2 plans across 1 wave, autonomous=true for 999.4-01 (web; 3 tasks: helper + browse wiring + Quick View wiring with final human-verify checkpoint) and autonomous=false for 999.4-02 (desktop; 4 tasks: shared LineNumberArea widget + browse_text wiring + ResultDialog text_ms wiring + final human-verify checkpoint). 999.4-01 (wave 1, web): builds `_render_line_numbered_html(text, highlight_html, line_height, font_size, show_line_numbers)` module-scope helper in `web/pages/browse.py` using CSS-grid two-column layout (gutter `<span class="line-number-gutter">` with `user-select: none` + body `<div class="line-numbered-body">`); D-04 copy-paste invariant achieved structurally by gutter living in a separate grid column with `user-select: none`. Wires `render_text_content` at `:4206-4225` (covers single-page + all version views via `handle_version_change`) AND imports the same helper into `render_text_section` at `web/pages/search_results.py:1768` (Quick View). Toggle button (icon `format_list_numbered`) in `version_row` (browse) + view-mode header row (Quick View). Persistence via `safe_user_get/safe_user_set` under key `ui.show_line_numbers` (default True per D-07). 12 structural tests in `tests/test_line_numbers_web.py` covering line count, blank-line numbering (D-10 — `text.split('\n')` invariant aligned to Responsa `L<N>:` parser at genizah_core.py:7679-7691), gutter copy-paste invariant (regex-strip the span and the remainder equals body), XSS safety, RTL direction, line_height parameter, disabled passthrough, `<br>`-normalization for callers that pre-converted `\n→<br>`, Quick View pre-built-HTML highlight survival. Web Browse `view_all` Full-Manuscript-View at `:2587-2598` NOT touched (D-01 scopes only `render_text_content`); flagged in human-verify checkpoint as follow-up. Web search-results inline accordion at `search_results.py:688` explicitly out of scope (D-03). 999.4-02 (wave 1, desktop): builds new file `desktop/widgets/line_number_text_edit.py` (~150 lines) with `LineNumberArea(QWidget)` (canonical Qt code-editor line-number-area pattern adapted for RTL via `setViewportMargins(0,0,gutter_w,0)` in RTL or `(gutter_w,0,0,0)` in LTR + manual area-widget geometry) and `apply_line_numbered_text(widget, rendered, *, source_text, is_html)` helper. D-04 copy-paste invariant achieved trivially: LineNumberArea is a SIBLING QWidget (not part of QTextDocument), so Qt text cursor cannot extend into it; `toPlainText()` of body excludes numbers (Test 3). Wires `genizah_app.py:6576 self.browse_text` at 6 call sites (5 HTML transcription renders `:3635/:8580/:9314/:9947/:21470` + 1 edit-cancel `:3365`; loading-state `:9823/:9851/:21100` setText calls intentionally left raw — non-transcription messages). Wires `desktop/result_dialog.py:452 self.text_ms` at 4 setHtml call sites (`:1260/:1270/:1931/:2120`) with explicit `source_text=` from raw variables in each scope (`text`/`text`/`ms_raw`/`raw_text`). Toolbar QPushButton (`# Lines`, `checkable=True`, tooltip `tr('Toggle line numbers')`) in Browse tab AND in ResultDialog find-row. Persistence via `load_app_config()`/`save_app_config({'show_line_numbers': bool})` (mirrors existing `show_translations` precedent at genizah_app.py:2237-2245). Default ON per D-07. Shared config key means toggling in one desktop surface affects the other on next render (live signal-bus deferred). 8 headless Qt tests in `tests/test_line_numbers_desktop.py` (skip cleanly if PyQt6 missing): widget attaches, line count == `text.split('\n')` count, clipboard isolation via `toPlainText`, toggle hides/shows, config persistence default True invariant, works for QTextBrowser too, recompute on repeated call updates existing LineNumberArea (idempotent), RTL layout positions area on visual right. Mints requirements LINE-NUM-01..10 (LINE-NUM-01..06+09 = web in Plan 01; LINE-NUM-07/08/09/10 = desktop in Plan 02; LINE-NUM-09 — `text.split('\n')` numbering invariant — shared across both plans). Out of scope per CONTEXT.md Deferred Ideas: web search-results inline accordion (D-03), Parallels/Composition results, click-to-deep-link, click-to-`L<N>:`-insert, continuous cross-folio numbering. Earlier note: Phase 999.3 (Adding PGP to downloaded data, backlog) planned: 1 plan, wave 1, autonomous=false (one human-verify checkpoint at end). 999.3-01 (wave 1): 5 tasks shipping PGP metadata into BOTH Excel and JSON search-results exports via ONE shared helper `_pgp_subset_for_sys_id` in `shared/search_serializer.py` (mirrors `_build_pgp_subset`'s 10-key shape, adds `_split_pgp_languages` to fix latent character-iteration bug in `languages_primary`/`languages_secondary` projection — comma-separated TEXT in pgp.db, NOT JSON). Task 1: helper + 11 unit tests covering B1-B9 contract (short-circuit, exception resilience, language-format fixes, no-transcription guarantee). Task 2: `'pgp'` injection at `serialize_search_payload` loop layer (NOT inside `_serialize_item`, so `_to_parallels_envelope_item` does not inherit it — D-10 holds for parallels JSON) + 6 envelope-shape tests including explicit parallels-JSON-untouched regression. Task 3: 6 Excel columns appended after `Full Text` at `web/export_service.py:286-355` — `PGP URL | PGP Description | PGP Type | PGP Date | PGP Languages | PGP Tags` — with pipe-delimited multi-values (D-05 no spaces), English-only data (D-04 no `get_language()`), empty cells for missing PGP (D-06), `inferred_date_display → doc_date_standard → doc_date_original` fallback chain, all-LTR alignment for new cells, plus 6 Excel-shape tests. Task 4: full-suite regression + ruff + static-grep audit. Task 5: human smoke-check across Excel/JSON downloads + negative D-10 checks (Word/list/parallels exports untouched). D-09 short-circuit supported by helper signature (`available_sys_ids` kwarg) but NOT wired through `set_search_export()` in this phase — deferred follow-up; helper currently does 1 SQLite lookup per result on download (bounded by 200-result page cap × ~1ms). Mints requirements PGP-EXPORT-01..05. Touches `shared/search_serializer.py` + `web/export_service.py` + 2 test files only. Web-only — desktop untouched per phase scope. Earlier note: Phase 999.2 (Filtering by PGP, backlog) planned: 1 plan, wave 1, autonomous=false (one human-verify checkpoint at end). 999.2-01 (wave 1): 7 tasks adding a 3-state PGP filter toggle to web `/search` results toolbar mirroring the existing `printed_filter` pattern end-to-end — bootstrap read at :148, cycle handler + label/color updater + button construction after `printed_filter_btn` at :1430-1434, `_apply_pgp_filter` predicate wired into both `_apply_printed_filter_and_render` and `_apply_domain_exclusions` cascades per D-11, active-filter chip co-located with `exclusion_chips_row` at :1448-1449 per D-08 with click-to-clear, post-enrichment visibility flip in `_apply_enrichment_to_ui` at :4436-4444, New Search reset at :2042-2050. Mints requirements PGP-FILTER-01..05. Persistence goes through `safe_storage` chokepoint (Phase 87 lint preserved). Parallels page and desktop app explicitly OUT OF SCOPE per D-12. Earlier note: Phase 999.1 (Search results by folio, backlog) planned: 1 plan, wave 1, autonomous=false (single human-verify checkpoint). 999.1-01 (wave 1): single-task UI render addition in `web/pages/search_results.py:468` surfacing `display['img']` as `· {num}` after the shelfmark — strict desktop-parity scope per locked decisions D-01..D-05 in 999.1-CONTEXT.md. Mints requirement FOLIO-01. Earlier note: Phase 90 (Auth Caching Rewrite -- No set_session) planned: 2 plans across 2 waves. 90-01 (wave 1): behavior rewrite (get_user_client/sign_in/sign_out/set_session_from_url/exchange_code_for_session/4 retry blocks/profile.py change_password helper/clear_auth revoke-before-pop reorder) per Codex round-1/2/3 fixes + AUTHC-05 docstring + Phase 87 allowlist self-elimination (3->2) (closes AUTHC-02, AUTHC-03, AUTHC-04, AUTHC-05). 90-02 (wave 2, depends_on 90-01): atomic deletion of 4 globals + 2 helpers + install of 3 permanent CI guards (static AST scanner D-15 with 10 seed traps, runtime attr-absence D-16, behavioral refresh-lock test D-17 Tests A/B/C) (closes AUTHC-01, finalizes AUTHC-03, closes AUTHC-04). All 5 AUTHC-XX requirements covered.*
