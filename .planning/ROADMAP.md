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
- **v7.12 Multitenant Architecture (Path B)** -- Phases 87-92 + 92.1 + 92.2 + promoted 999.1/999.4 (shipped 2026-05-18)

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

<details>
<summary>v7.12 Multitenant Architecture (Path B) (Phases 87-92 + 92.1 + 92.2 + promoted 999.1/999.4) -- SHIPPED 2026-05-18</summary>

See: .planning/milestones/v7.12-ROADMAP.md

10 phases (87, 88, 89, 90, 91, 92, 92.1 INSERTED, 92.2 INSERTED, 999.1 promoted, 999.4 promoted), 28 plans, 49/49 requirements satisfied (38 v7.12 core + 11 promoted backlog).
Refactored GenizahSearch's web layer off the desktop-inherited single-user mental model. 131 raw `app.storage.user` accesses migrated through `web/safe_storage.py` chokepoint with allowlist driven to 0 entries. State separation by deletion (10 AppState mirror fields gone), per-request `UserListsManager`, request-scoped auth with NO `set_session()` mid-flight (Codex constraint at `gotrue_client.py:713` respected), `_session_uuid`-keyed refresh locks, real server-side `sign_out` revocation. Phase 92.1 (INSERTED) closed P0 RLS-reachability regression by migrating 12 reader call sites from anonymous singleton to `get_user_client()`. Phase 92.2 (INSERTED) closed `/lists` 36s warm-render regression via task-scoped `WeakKeyDictionary` memo (19.3x mean speedup). 5-surface SWEEP audit clean. `docs/guides/MULTITENANT.md` shipped as architecture reference. Promoted backlog: Phase 999.1 (search-result folio chip parity) + Phase 999.4 (line-number gutter in both apps).

</details>

## Backlog

### Phase 999.2: Filtering by PGP (PLANNED)

**Goal:** Add a 3-state post-search PGP filter toggle (`All` / `Has PGP` / `No PGP`) to the web `/search` results toolbar, modeled directly on the existing `printed_filter` pattern at `web/pages/search.py:1402-1434`. Plus an active-filter chip in the results header (co-located with `exclusion_chips_row`) and session persistence via `web/safe_storage.py`. Web only — parallels and desktop explicitly out of scope (D-12). The filter operates on the in-memory result list using `search_state.transcription_sys_ids` (the same set that drives the green PGP badge in `search_results.py:397-400`); no search-pipeline changes.
**Requirements:** PGP-FILTER-01, PGP-FILTER-02, PGP-FILTER-03, PGP-FILTER-04, PGP-FILTER-05
**Plans:** 1 plan

Plans:
- [ ] 999.2-01-PLAN.md — pgp_filter field on SearchUIState + bootstrap read + cycle handler + label/color updater + button after printed_filter_btn + cascade integration (printed → PGP → measurement) + active-filter chip with click-to-clear + post-enrichment visibility flip + New Search reset + human smoke-check (PGP-FILTER-01..05)

### Phase 999.3: Adding PGP to downloaded data (PLANNED)

**Goal:** Extend the Excel **and** JSON search-results exports with PGP scholarly metadata so researchers can sort, filter, and cite PGP data directly from the downloaded artifact (spreadsheet OR JSON). Excel gains 6 columns (`PGP URL`, `PGP Description`, `PGP Type`, `PGP Date`, `PGP Languages`, `PGP Tags`) appended after `Full Text`. JSON gains a per-item `pgp` subobject using the canonical 10-key `_build_pgp_subset` projection — or `null` when no PGP record. Both surfaces consume ONE shared helper (`shared/search_serializer.py:_pgp_subset_for_sys_id`) so they never drift. Word, list, and parallels exports explicitly OUT OF SCOPE per D-10; parallels JSON envelope also untouched (injection happens at the `serialize_search_payload` loop layer, not inside the shared `_serialize_item`, so `_to_parallels_envelope_item` does not inherit it). Always-English (D-04), pipe-delimited multi-values with no spaces (D-05), empty cells / `null` (NOT `{}`) for missing data (D-06). Also fixes a latent character-iteration bug in `languages_primary`/`languages_secondary` projection (pgp.db stores these as comma-separated TEXT, not JSON) via the new `_split_pgp_languages` internal — without modifying `_build_pgp_subset` so browse stays stable.
**Requirements:** METADATA-EXPORT-01, METADATA-EXPORT-02, METADATA-EXPORT-03, METADATA-EXPORT-04, METADATA-EXPORT-05, METADATA-EXPORT-06, METADATA-EXPORT-07 (formerly PGP-EXPORT-01..05 — INVALIDATED by 2026-05-17 scope expansion; full goal text + audit notes will be rewritten in Plan 03)
**Plans:** 3 plans across 3 waves

Plans:
- [ ] 999.3-01-PLAN.md — State plumbing + JSON additions. Extends `set_search_export(...)` with 3 enrichment kwargs; adds `update_search_export_enrichment(...)` sibling; wires 3 callsites in `web/pages/search.py` + 1 post-enrichment update; extends `_serialize_item` + `serialize_search_payload` with conditional emission of `has_pgp`/`is_printed`; `/api/export/json` passes the cast sets through. Parallels JSON D-10 preserved. 17 new tests (METADATA-EXPORT-01..04).
- [ ] 999.3-02-PLAN.md — Xlsx restructure + 4 lookup helpers + 3 sheets. Creates NEW `shared/export_dossier.py` with 4 helpers (PGP/NLI/Catalog/Bibliography) + `_split_pgp_languages` bug fix. Restructures `export_search_results_excel` into 3-sheet builder (`Genizah Results` + `Manuscripts` + `Bibliography`). Endpoint passes session enrichment through. 38 new tests (METADATA-EXPORT-05..06).
- [ ] 999.3-03-PLAN.md — Human smoke verification on real Excel + JSON downloads (4 test scenarios, D-04/D-05/D-06/D-10 invariant checks); REQUIREMENTS.md update with INVALIDATED predecessor note; ROADMAP.md final-shape update. autonomous=false. (METADATA-EXPORT-07).

---

*Roadmap created: 2026-02-09*
*Last updated: 2026-05-18 — v7.12 Path B Multitenant Architecture milestone SHIPPED and archived to `.planning/milestones/v7.12-ROADMAP.md`. 49/49 requirements satisfied across 10 phases / 28 plans. Promoted backlog phases 999.1 (FOLIO-01) and 999.4 (LINE-NUM-01..10) merged into the v7.12 archive because they shipped alongside the milestone close. Phase 999.2 (PGP filter) and Phase 999.3 (PGP downloads) remain planned in Backlog. `deploy.sh` UNBLOCKED. Git tag deferred to `/release` (web + desktop bundle).*
