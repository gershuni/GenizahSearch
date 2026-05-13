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
**Plans**: 8 plans
- [x] 87-01-VALIDATION-FOUNDATION-PLAN.md — Failing test stubs + allowlist scaffold (Wave 0)
- [x] 87-02-SESSION-UUID-HELPERS-PLAN.md — get_session_uuid + ensure_session_uuid in web/safe_storage.py (Wave 1)
- [ ] 87-03-LEAF-FILE-MIGRATIONS-PLAN.md — text_editor, translation_report, home, settings, search_results (Wave 1)
- [ ] 87-04-MAIN-AND-ALIAS-MIGRATIONS-PLAN.md — main.py + api.py (nicegui_app alias) + supabase_client.py (_app alias) (Wave 2)
- [ ] 87-05-BROWSE-CLUSTER-MIGRATIONS-PLAN.md — browse.py + browse_state.py + catalog_browse.py (Wave 2)
- [ ] 87-06-SEARCH-CLUSTER-MIGRATIONS-PLAN.md — parallels.py + search.py + search_state.py (Wave 2)
- [ ] 87-07-LINT-FINALIZATION-PLAN.md — Lint scanner + allowlist finalization (Wave 3)
- [ ] 87-08-ACCEPTANCE-AND-DOCS-PLAN.md — Docs + STATE.md + human smoke-check (Wave 4)

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
**Plans**: TBD

### Phase 89: Lists Cache Per-Request
**Goal**: Drop the `UserListsManager` singleton and 10s TTL plumbing entirely; per-request instantiation becomes the simpler safe pattern.
**Depends on**: Phase 87
**Requirements**: LISTS-01, LISTS-02, LISTS-03, LISTS-04
**Success Criteria** (what must be TRUE):
  1. Static grep of `web/state.py:AppState` returns zero matches for `_user_lists_mgr` — the singleton attribute is gone.
  2. Static grep of `web/user_lists.py` returns zero matches for `_cache_entry` and the 10s TTL constant — the time-based cache plumbing does not exist in the codebase.
  3. A user logged in as User A opens the lists page; User B (different session, different user account) opens the lists page within what would have been the 10s TTL window; User B sees their own lists, not User A's.
  4. `tests/test_user_lists_cache_isolation.py` passes and is written against the per-request model (no references to cache TTL, user_id keys, or singleton behavior).
**Plans**: TBD

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
**Plans**: TBD

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
**Plans**: TBD

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
| 87. Foundations -- Session UUID and Safe Storage Chokepoint | v7.12 | 2/8 | In Progress|  |
| 88. State Separation by Deletion | v7.12 | 0/TBD | Not started | - |
| 89. Lists Cache Per-Request | v7.12 | 0/TBD | Not started | - |
| 90. Auth Caching Rewrite -- No set_session | v7.12 | 0/TBD | Not started | - |
| 91. Atomic Auth State Writes | v7.12 | 0/TBD | Not started | - |
| 92. Final Sweep and Acceptance | v7.12 | 0/TBD | Not started | - |

---
*Roadmap created: 2026-02-09*
*Last updated: 2026-05-13 -- v7.12 Multitenant Architecture (Path B) roadmap added. Phases 87-92.*
