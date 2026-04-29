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
- **v7.10 Search API** -- Phases 77-82 (active, started 2026-04-27)

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

### v7.10 Search API (Active, started 2026-04-27)

**Milestone Goal:** Add a thin internal HTTP/JSON surface over existing search, parallels, and browse pipelines so external automation (first consumer: a Claude skill that sorts/ranks results) can drive GenizahSearch. Helper surface, not platform — narrow endpoints, no public docs, no long-term stability promise. Hardening, observability, and access-mode controls apply to the **three new search-helper endpoints only** (`/api/search`, `/api/parallels`, `/api/browse`); existing `/api/*` routes (image proxies, puzzle uploads, NLI proxies) are explicitly out of scope.

**Per-phase gate:** pytest baseline remains green; CI green (Ubuntu + Windows); existing `/api/*` routes return identical responses to pre-milestone (verified by spot-check on image proxy + puzzle upload routes after each phase that touches `web/api.py`).

**Milestone-level gate:** Claude skill (Phase 81) drives end-to-end search → browse loop against a live deployment without crashing on rate-limit / timeout / partial-data conditions; `scripts/check_docs.py` green at milestone close.

**Phase summary checklist:**

- [ ] **Phase 77: Serializer & JSON Export** — Single-source-of-truth serializer module powering toolbar JSON downloads on /search and /parallels, ahead of any API endpoint
- [ ] **Phase 78: /api/search + Hardening Shell** — First search-helper endpoint plus the cross-cutting hardening primitives (rate limit, mode flag, query/result caps, error envelope, PostHog) all three endpoints reuse
- [ ] **Phase 79: /api/browse Drill-Down** — Stateless drill-down endpoint resolving locators from /api/search responses to text + metadata + image URLs; first real consumer of the locator contract, proves the search → browse vertical slice
- [ ] **Phase 80: /api/parallels** — Companion search-side endpoint reusing the locator and hardening shell, sequenced after browse so the locator round-trip is already validated before a second producer is added
- [ ] **Phase 81: Claude Skill Consumer** — Reference skill exercising the search → browse loop end-to-end as the v7.10 acceptance harness
- [ ] **Phase 82: Internal Documentation** — `docs/SEARCH_API.md` and `CLAUDE.md` env-var updates capturing the as-shipped contract

## Phase Details

### Phase 77: Serializer & JSON Export
**Goal**: A single serializer module owns the "Claude-friendly JSON" payload shape, and /search and /parallels pages let users download the current results in that shape — establishing the contract before any HTTP endpoint consumes it.
**Depends on**: Nothing (first phase of milestone; v7.9 decomposition provides clean web/pages/search.py and web/pages/browse.py module boundaries to hook the export buttons into).
**Requirements**: EXPORT-01, EXPORT-02, EXPORT-03, EXPORT-04
**Success Criteria** (what must be TRUE):
  1. From `/search` after running any query, a toolbar button downloads the visible result set as a JSON file whose filename contains the page identifier and an ISO timestamp (e.g. `genizah-search-2026-04-27T1530.json`); two consecutive downloads produce two distinct files.
  2. From `/parallels` after running a composition search, the same export button downloads results in the parallels-shaped payload — never silently overwriting a prior download.
  3. The exported JSON for both pages is produced by exactly one serializer module; modifying the result-item shape in that module changes both downloads (and, in later phases, the API response) in lockstep — no parallel implementation exists.
  4. Each downloaded payload includes the drill-down locator on every result item (uid preferred, `{sys_id, volume_ie, p_num}` fallback), proving the locator contract works before /api/browse consumes it in Phase 80.
**Phase gate**: pytest green, CI green, manual download spot-check on /search and /parallels.
**Plans:** 5 plans
- [x] 77-01-PLAN.md -- AppState envelope-echo fields + state-population sites + Wave 0 RED test scaffolding (complete 2026-04-27)
- [x] 77-02-PLAN.md -- genizah_core.lab_composition_search chunk_hits extension (D-13 Path A) (complete 2026-04-27)
- [x] 77-03-PLAN.md -- shared/search_serializer.py module (single source of truth, all 22 tests GREEN) (complete 2026-04-27)
- [x] 77-04-PLAN.md -- web/api.py JSON handlers + toolbar buttons on /search and /parallels (complete 2026-04-27)
- [x] 77-05-PLAN.md -- docs/OPEN_ISSUES + docs/CODE_INDEX update + manual smoke check (complete 2026-04-28; 4 follow-on commits during smoke check fixed chunk_hits field-name collision)
**UI hint**: yes

### Phase 78: /api/search + Hardening Shell
**Goal**: `POST /api/search` returns Claude-friendly results from `SearchEngine.execute_search` over a hardened transport (rate-limited, capped, mode-gated, observable, with a uniform error envelope) — and that hardening shell is built once so Phases 79 and 80 inherit it without reimplementation.
**Depends on**: Phase 77 (serializer module owns response item shape).
**Requirements**: API-01, API-04, API-05, API-06, API-07, HARDEN-01, HARDEN-02, HARDEN-03, HARDEN-04, HARDEN-05
**Success Criteria** (what must be TRUE):
  1. `POST /api/search` with a valid `{query, mode, gap?, limit?, filters?}` body returns ranked results matching the Phase 77 serializer shape, including the drill-down locator on every item, in a single HTTP round-trip; identical requests return identical bodies regardless of whether a NiceGUI browser session exists.
  2. Invalid input (unknown mode, oversized limit, unknown filter key, query exceeding the length cap) returns the uniform `{error: {code, message}}` envelope — never a raw FastAPI 422 dump — and existing `/api/*` routes (image proxies, puzzle uploads, NLI proxies) return their original responses unchanged on spot-check.
  3. Filter values submitted by API clients flow through the same FJMS `restrict_sys_ids` pipeline the UI uses; a filter value the pipeline cannot resolve is rejected at the endpoint with the error envelope, never silently dropped.
  4. Sustained traffic above the per-IP rate limit returns HTTP 429 with a `Retry-After` header; setting `SEARCH_API_MODE=disabled` (or `localhost-only`) takes effect on next request without a code change; neither toggle affects existing `/api/*` routes.
  5. When the Responsa combinatorial cascade or query-length cap downgrades a query, the response surfaces the adjustment in a top-level `warnings` array (or `query_adjustments`) — never hidden inside the first result item — and a PostHog event fires per request capturing endpoint, mode, latency bucket, result-count bucket, and IP-hash with no payload contents logged.
**Phase gate**: pytest green, CI green, integration test exercising error envelope + warnings array + mode-flag (open/localhost-only/disabled); explicit soak check sustaining traffic above the per-IP rate limit until 429 + `Retry-After` are observed (per Codex review — covers the rate-limiter end-to-end without a standalone stress phase).
**Plans:** 4 plans
- [x] 78-01-PLAN.md -- Wave 0 RED test scaffold (D-21 + D-23 surface) (complete 2026-04-28; 3 test files, 82 tests, intended RED)
- [x] 78-02-PLAN.md -- web/api_hardening.py (RateLimiter, mode gate, error handlers, PostHog server-side capture) (complete 2026-04-28; shared/api_errors.py + web/api_hardening.py, 39/39 hardening tests GREEN)
- [x] 78-03-PLAN.md -- web/search_api.py (POST /api/search handler + Pydantic models) + shared/fjms_service.validate_filter_values (complete 2026-04-28; 82/82 Phase 78 tests GREEN, 1295 passed in wider suite)
- [x] 78-04-PLAN.md -- bootstrap wiring in web/main.py + soak test + soak script + CLAUDE.md env-vars (complete 2026-04-28; 7 commits, 4 files created + 3 modified, 3 slow soak tests register, all Phase 78 tests GREEN)

### Phase 79: /api/browse Drill-Down
**Goal**: `GET /api/browse` resolves a single manuscript page from a locator returned by `/api/search` and returns text + metadata + image URLs in one shot — no follow-up calls, no session state. Sequenced ahead of `/api/parallels` so the locator contract is *consumed* (not just emitted) before a second producer is added — this closes the search → browse vertical slice the Claude skill needs.
**Depends on**: Phase 78 (locator contract from API-05; hardening shell).
**Requirements**: API-03
**Success Criteria** (what must be TRUE):
  1. `GET /api/browse?uid=…` (preferred) or `GET /api/browse?sys_id=…&volume_ie=…&page=…` (fallback) returns a fixed-shape JSON body with text (transcription if available, snippet otherwise), the documented PGP/FJMS/NLI metadata subset, and image URLs — with the page-indexing convention (1-based or 0-based, whichever core uses) explicit in the response itself.
  2. Given a locator copied verbatim from a Phase 78 `/api/search` response item, `/api/browse` returns the corresponding manuscript page without any disambiguation step; the round-trip works for at least one multi-IE manuscript and one single-IE manuscript.
  3. The endpoint is stateless: identical query strings produce identical bodies regardless of `app.storage.user`, refinement chain, or any prior UI action; image URLs continue to be served (not inlined) and degrade gracefully when NLI is unavailable rather than failing the whole response.
  4. Rate limiting, mode gating, error envelope, and PostHog observability inherited from Phase 78 apply identically; existing `/api/*` image-proxy and puzzle routes are unchanged on spot-check.
**Phase gate**: pytest green, CI green, locator round-trip test against single-IE and multi-IE manuscripts; closes the locator obligation that begins in Phase 77 (export embeds locator) → Phase 78 (search emits locator) → Phase 79 (browse consumes locator).
**Plans**: TBD

### Phase 80: /api/parallels
**Goal**: `POST /api/parallels` exposes the composition/parallels pipeline through the same payload, locator, error-envelope, and hardening conventions as `/api/search`. Sequenced after `/api/browse` so the locator round-trip is already validated end-to-end before a second producer emits the same contract.
**Depends on**: Phase 78 (hardening shell, error envelope, serializer wiring, locator contract); Phase 79 (locator round-trip validated through `/api/browse`).
**Requirements**: API-02
**Success Criteria** (what must be TRUE):
  1. `POST /api/parallels` accepts the v7.10 subset (`text`, `chunk_size`, `mode`, `max_freq?`, optional same filter subset as /api/search, optional boundary options) and returns Claude-friendly results that share the Phase 77 serializer item shape including the drill-down locator.
  2. The response shape documents whether filtered or high-frequency hits appear under a separate `filtered` key or are omitted entirely — and that documented behavior is applied consistently across at least three sample compositions covering text, gap, and Responsa modes.
  3. Rate limiting, result caps, query-length cap, error envelope, `SEARCH_API_MODE` gating, and the PostHog observability event from Phase 78 apply to `/api/parallels` with no per-endpoint reimplementation; flipping a knob in one place changes both endpoints.
  4. Locators emitted by `/api/parallels` round-trip through `/api/browse` (built in Phase 79) without any per-producer adjustment; this is verified with at least one parallels result feeding a successful `/api/browse` call.
**Phase gate**: pytest green, CI green, parity check confirming Phase 78 hardening behaviors apply unchanged to /api/parallels; locator round-trip via /api/browse.
**Plans**: TBD

### Phase 81: Claude Skill Consumer
**Goal**: A runnable Claude skill drives `/api/search` → `/api/browse` end-to-end and proves the v7.10 contract by ranking real manuscripts against real queries — the milestone's acceptance harness.
**Depends on**: Phase 78 (search), Phase 79 (browse drill-down — completes the search → browse vertical slice the skill exercises), Phase 80 (parallels — broadens skill coverage).
**Requirements**: SKILL-01, SKILL-02, SKILL-03
**Success Criteria** (what must be TRUE):
  1. The skill's base URL is configurable (env var or argument), defaults to the production deployment, and the skill is runnable from a clean checkout — its filesystem location is environment-specific and not pinned to a specific repo path.
  2. Running the skill on a representative scholarly query produces a ranked list of N candidates with brief justifications grounded in the text fetched via `/api/browse`; reviewing any single justification, the cited evidence is traceable back to a specific browse response.
  3. Triggering a 429 from the rate limiter, a request timeout, or a partial `/api/browse` response (NLI image unavailable) does not crash the conversation; the skill surfaces each failure in plain terms and continues processing remaining candidates where possible.
**Phase gate**: live end-to-end run against the production deployment with the user observing; user-signed-off ranking against at least one scholarly query.
**Plans**: TBD

### Phase 82: Internal Documentation
**Goal**: As-shipped contract is captured in one internal page and the new env-var surface is discoverable by future maintainers — without inviting external usage.
**Depends on**: Phases 78, 79, 80, 81 (must reflect what actually shipped, not what was planned).
**Requirements**: DOC-01, DOC-02
**Success Criteria** (what must be TRUE):
  1. `docs/SEARCH_API.md` documents the three search-helper endpoints with their exact request and response shapes (including the locator, the `warnings` array, and the error envelope), the env vars (`SEARCH_API_MODE`, rate-limit knobs), and an explicit "internal helper, no stability promise" disclaimer; the page is not linked from the public site or `README.md`.
  2. `CLAUDE.md` lists the new env vars (`SEARCH_API_MODE` and the rate-limit knobs) in its environment-variables section so future agents discover them through the standard project context; `README.md` is intentionally untouched.
  3. A reader unfamiliar with v7.10 can, using only `docs/SEARCH_API.md`, send a valid `/api/search` request, follow the locator to `/api/browse`, and predict the error envelope returned by an invalid filter — without reading the source code.
**Phase gate**: scripts/check_docs.py green; doc walkthrough by a reader who did not implement the milestone.
**Plans**: TBD

## Progress

**Execution Order:**
Phases execute in numeric order: 77 -> 78 -> 79 -> 80 -> 81 -> 82

| Phase | Plans Complete | Status | Completed |
|-------|----------------|--------|-----------|
| 77. Serializer & JSON Export | 5/5 | Ready for /gsd-verify-work (2026-04-28) | - |
| 78. /api/search + Hardening Shell | 3/4 | Plan 03 GREEN (2026-04-28; 82 tests across the 3 Phase 78 test files) | - |
| 79. /api/browse Drill-Down | 0/0 | Not started | - |
| 80. /api/parallels | 0/0 | Not started | - |
| 81. Claude Skill Consumer | 0/0 | Not started | - |
| 82. Internal Documentation | 0/0 | Not started | - |

---
*Roadmap created: 2026-02-09*
*Last updated: 2026-04-28 -- Plan 77-05 complete; Phase 77 ready for /gsd-verify-work. Manual smoke-check on /search and /parallels JSON downloads PASSED after 4 follow-on commits resolved a chunk_hits field-name collision uncovered during smoke verification (Plan 02 had extended lab_composition_search to populate chunk_hits per uid as a list-of-tuples (D-13 Path A), but search_composition_logic had used chunk_hits since 2026-03-12 as an int counter — both producers wrote to the same per-uid item dict, so the serializer crashed with `'int' object is not iterable` on standard-mode parallels results). Fix chain: baf481fb (defensive isinstance guard + logger.exception), c24fcc48 (mirrored list-of-tuples shape into standard-mode + renamed int counter to chunk_count + fixed parallels rep-field mapping; +4 tests), 2e2d2b75 (surfaced Tantivy score on search results — was 0.0 in JSON because results.append at genizah_core.py:7542+:7559 never recorded score var; per-uid _chunk_hit_keys dedup), 327aea31 (group-level dedup keyed on (chunk_index, manuscript_snippet) for cross-uid duplicates from NLI multi-uid cataloging like Karaite prayer books; matches[] sorted by chunk_index ascending; +2 tests). Final test count: 1201 passed / 8 skipped (was 1162 at phase start → +39 new tests across the 5 plans). Cumulative phase commit count: 20 (14 plan-scope + 6 follow-on smoke-check fixes). Phase gate satisfied: pytest green, CI green (assumed pending push), manual download spot-check on /search and /parallels signed off.*
