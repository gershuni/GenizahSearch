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
- **v7.13 Research-Grade Downloads & PGP Filter** -- Phases 93-94 (ACTIVE; started 2026-05-19; Phase 93 web-only COMPLETE 2026-05-19; Phase 94 web + desktop xlsx COMPLETE 2026-05-21; milestone closeable)
- **v7.14 My Library** -- Phases 95-96 (ACTIVE; started 2026-05-20; Phase 95 shipped 2026-05-24 as v7.14.0; Phase 96 added 2026-05-24 to complete the feature with follow-ups and bug fixes)

## Phases

## Roadmap v7.14: My Library

### Phase 95: My Library — Local Document Indexing

**Goal:** Desktop users can point GenizahSearch at folders of `.docx` / `.pdf` / `.txt` files and have those documents indexed into a SEPARATE Tantivy side-index merged into normal search / Composition Search / Parallels results with a clear `LOCAL` badge and a three-state filter button. Personal corpora NEVER leak to the cloud — three regression tests pin the cloud-write boundaries (`/api/search`, `lists_sync.sync_item_to_cloud`, corrections submit). Productizes Yehuda Seewald's external prototype (`seewald_addition/`) as a first-class in-app feature — no second installation, no `Program Files` UAC patching, no shared sys_id namespace collision, no web / API / Supabase exposure.

**Depends on:** Nothing — independent feature

**Source CONTEXT:** `.planning/phases/95-my-library/95-CONTEXT.md` (46 locked decisions, post-Codex critique; AUTHORITATIVE)
**Source SPEC:** `.planning/phases/95-my-library/95-SPEC.md` (10 requirements + 22 acceptance criteria — AUTHORITATIVE)

**Plans:** 9/9 plans complete
- [ ] 95-01-PLAN.md — Wave 0: 26 red-stub tests + requirements.txt pymupdf pin + GenizahSearchPro.spec collect_all(pymupdf) + D-44 Hebrew fixture + conftest fixtures
- [ ] 95-02-PLAN.md — Wave 1: shared/local_sys_id.py + parse_header_smart/parse_full_id_components generalization (Codex D-13 P0) + LIBRARY_CODES extension + Config.LOCAL_*_DIR
- [ ] 95-03-PLAN.md — Wave 1: shared/local_indexer.py core (PyMuPDF + python-docx + TXT + RTL helpers as dead code + LOCAL Tantivy schema with tokenizer_name="raw" on unique_id + SQLite cache + two-phase commit + delete-by-uid + folder overlap detection + unavailable-folder handling)
- [ ] 95-04-PLAN.md — Wave 1: Three cloud-write gates — shared/search_serializer.py + corrections_client.py + lists_sync.py (Codex D-30 P0 — gate at TOP of sync_item_to_cloud BEFORE _get_client())
- [ ] 95-05-PLAN.md — Wave 2: Main search merger via RRF k=60 POST-_deduplicate (Codex D-08 P0) + D-37 corrupt-index fallback
- [ ] 95-06-PLAN.md — Wave 2: LOCAL LAB side-index + weights_hash invalidation contract (D-09 + D-38); custom fingerprint scoring preserved
- [ ] 95-07-PLAN.md — Wave 3: desktop/my_library_tab.py (MyLibraryTab as 7th tab — Pitfall #4) + LocalIndexerWorker QThread + QMutex serialization (D-25) + mid-file cancellation (D-24 Codex P1) + pre-scan ceiling dialog (D-26 + D-41)
- [ ] 95-08-PLAN.md — Wave 3: COL_SRC LOCAL badge in blue (D-11) + three-state LOCAL filter on Search/Composition/Parallels (REQ-6 + D-10 + D-39) + D-10 P1 no-op chip + LOCAL hit click -> Browse panel text-only + Open file (D-27 + D-28)
- [ ] 95-09-PLAN.md — Wave 4: Help + About docs (D-31 + D-32 + D-33 cleartext disclosure, EN + HE both apps) + export_dossier skip_local kwarg (D-45) + web LIBRARY_CODES static AST guard (D-46) + PyInstaller packaging smoke (D-43 @pytest.mark.packaging) + OPEN_ISSUES/CHANGELOG/CLAUDE.md bookkeeping

**Wave structure:** 0 (01) -> 1 (3 parallel: 02, 03, 04) -> 2 (05) -> 3 (2 parallel: 06, 07) -> 4 (08) -> 5 (09). Note: 06 bumped to wave 3 to avoid genizah_core.py overlap with 05; 08 bumped to wave 4 to avoid genizah_app.py overlap with 07; 09 bumped to wave 5 because it depends on 08.

**UI hint:** yes — new 7th desktop tab + result-list LOCAL badge + three-state filter button mirroring Phase 93 PGP pattern.

### Phase 96: Completing My Library feature: add features and fix bugs

**Goal:** Take Phase 95 (My Library) from "shipped MVP" (v7.14.0 public release, 2026-05-24) to "feature-complete" by closing the P1 highlight regression (D-F5), the confirmed PDF-extraction bug (D-F4), and adding the per-file opt-in/out drill-down feature (D-F1). Also remove the redundant `צפה בדפדוף` button (NEW-1) and add next/prev navigation + "View All" (הכל) for LOCAL hits in both ResultDialog and the Browse panel (NEW-2). Phase 95 invariants (RRF POST-dedup, three cloud-write gates at TOP, web LIBRARY_CODES `[]`, multitenant `[]`) preserved throughout.

**Scope items (D-XX/NEW-X from CONTEXT.md — Phase 96 has no REQ-IDs):**
- D-F5 — LOCAL highlight P1 regression (plan 96-03; engine-side via `_build_local_result_dict` normalization, Option A from RESEARCH §1)
- D-F4 — PDF one-word-per-line extraction bug (plan 96-02; detect-then-fallback in `extract_pdf_pages`)
- D-F1 — Per-file opt-in/out drill-down (plans 96-04 persistence + 96-05 cascade + 96-06 tree widget UI)
- NEW-1 — Remove redundant `צפה בדפדוף` button (plan 96-07)
- NEW-2 — Next/prev + View-All for LOCAL (plan 96-03 engine primitive + plan 96-08 UI wiring)
- NEW-3 — Freestyle polish bucket (plan 96-09 — capped per CONTEXT D-15)

**Deferred to v7.15+:** D-F2 (PDF OCR) and D-F3 (side-by-side PDF page rendering) — explicitly out per CONTEXT D-01.

**Depends on:** Phase 95 (shipped)

**Source CONTEXT:** `.planning/phases/96-completing-my-library-feature-add-features-and-fix-bugs/96-CONTEXT.md` (15 locked decisions; D-08 REVISED 2026-05-24 from QSettings → session JSON)
**Source RESEARCH:** `.planning/phases/96-completing-my-library-feature-add-features-and-fix-bugs/96-RESEARCH.md`
**Source PATTERNS:** `.planning/phases/96-completing-my-library-feature-add-features-and-fix-bugs/96-PATTERNS.md`
**Source VALIDATION:** `.planning/phases/96-completing-my-library-feature-add-features-and-fix-bugs/96-VALIDATION.md`

**Plans:** 9/9 plans complete

Plans:
- [x] 96-01-PLAN.md — Wave 0: D-F4 pathological PDF fixture + 6 new skeleton test files + cascade AST extension + NEW-1 xfail(strict=True) flips on existing test_local_browse_panel.py tests
- [x] 96-02-PLAN.md — Wave 1: D-F4 detect-then-fallback in `shared/local_indexer.py::extract_pdf_pages` (0.70 single-word-ratio threshold, `get_text("text", sort=True)` fallback)
- [x] 96-03-PLAN.md — Wave 1: D-F5 normalize LOCAL hit dict shape in `genizah_core.py::_build_local_result_dict` (Option A) + NEW-2 engine primitive `SearchEngine.get_local_browse_page` with per-sys_id cache
- [x] 96-04-PLAN.md — Wave 1: D-F1 persistence layer — `genizah_app.py` `_local_file_optouts` attribute + session-JSON save/restore (top-level cross-surface key) + `desktop/my_library_tab.py::_prune_optouts_to_disk` pure helper
- [x] 96-05-PLAN.md — Wave 2: D-F1 cascade — `genizah_app.py::_apply_local_optout_filter` + wiring at BOTH cascade joinpoints (`_apply_results_table_filters` and `_apply_comp_tree_filters`) with `_local_filter_active` OR'd with opt-out activity
- [x] 96-06-PLAN.md — Wave 3: D-F1 tree widget UI — `desktop/my_library_tab.py::_OptoutTreeWidget` with Qt-native tri-state + bottom-panel `QSplitter(Horizontal)` containing [tree, status_table] (RESEARCH §3 Option 1) + 150ms debounce + rescan-prune wiring + `genizah_app.py::_reapply_filters_for_optout_change`
- [x] 96-07-PLAN.md — Wave 3: NEW-1 button removal — delete `btn_rd_open_browse` declaration + `_rd_open_in_browse` handler + visibility branches from `desktop/result_dialog.py`; flip 4 xfail(strict=True) decorators to stable regression guards
- [x] 96-08-PLAN.md — Wave 4: NEW-2 UI wiring — `desktop/result_dialog.py::load_local_page` sibling with `is_local_sys_id` dispatch + `genizah_app.py::_aggregate_local_pages_with_separators` (page/chunk separators, EN + HE) + Browse panel View-All / Per-Page toggle persisted in session JSON
- [x] 96-09-PLAN.md — Wave 5: NEW-3 freestyle polish + docs (close D-F1/D-F4/D-F5 in OPEN_ISSUES.md; CHANGELOG.md + CLAUDE.md "Recently Changed"; optional version bump v7.14.1 / v7.15.0; pre-release pre-flight ruff + check_docs + full pytest)

**Wave structure:** 0 (01) → 1 (3 parallel: 02 [local_indexer.py], 03 [genizah_core.py], 04 [genizah_app.py + my_library_tab.py]) → 2 (05 [genizah_app.py — cascade]) → 3 (2 parallel: 06 [my_library_tab.py — tree widget], 07 [result_dialog.py — button removal]) → 4 (08 [result_dialog.py + genizah_app.py — NEW-2 UI]) → 5 (09 [docs + polish]).

**File ownership notes:** 96-04 and 96-06 both touch `desktop/my_library_tab.py` — 96-04 ships the pure helper (Wave 1), 96-06 wires the tree widget UI (Wave 3). 96-04 and 96-05 both touch `genizah_app.py` — 96-04 ships persistence (Wave 1), 96-05 ships cascade (Wave 2). 96-07 and 96-08 both touch `desktop/result_dialog.py` — 96-07 deletes the redundant button (Wave 3), 96-08 adds `load_local_page` dispatch (Wave 4). 96-05 and 96-08 both touch `genizah_app.py` — 96-05 ships cascade (Wave 2), 96-08 ships View-All helper + Browse toggle (Wave 4).

**Checkpoints:** Wave 3 (plan 96-06 D-F1 tree widget UI) and Wave 4 (plan 96-08 NEW-2 navigation UI) require human-verify (visual / RTL / Qt-themed rendering concerns). Wave 5 (plan 96-09) has a decision checkpoint for the version-bump strategy.

**UI hint:** yes — new horizontal splitter in MyLibraryTab bottom panel + tri-state checkboxes + new View-All/Per-Page toggle button in Browse panel + removed redundant button in ResultDialog.

<details>
<summary>v7.13 Research-Grade Downloads & PGP Filter (Phases 93-94) -- BOTH PHASES COMPLETE (Phase 93 2026-05-19; Phase 94 2026-05-21; milestone closeable)</summary>

See: .planning/milestones/v7.13-ROADMAP.md

2 phases, 5 plans (Phase 93: 1; Phase 94: 4 waves), 14/14 requirements satisfied (5 PGP-FILTER + 9 EXPORT-META). Both phases promoted from backlog (999.2 + 999.3). Phase 93 (PGP Filter on `/search`, web-only) shipped 2026-05-19 with 4/5 PGP-FILTER reqs directly satisfied; PGP-FILTER-03 (chip) superseded by user smoke direction (colored button label already conveys state). Phase 94 (Research-Grade Export Metadata, web + desktop xlsx) shipped 2026-05-21 after a 4-wave implementation (94-01 shared dossier primitives → 94-02 web state plumbing + JSON envelope → 94-03 web xlsx restructure → 94-04 desktop xlsx parity + smoke verification + docs closeout). The final workbook is 4-sheet (`Search Results` + `Manuscripts` + `Bibliography` + `Credits and Info`) bilingual (lang='he' produces Hebrew sheet titles + headers + Hebrew-preferred metadata, lang='en' produces English everywhere) with clickable Manuscripts URLs, deduped Domains, int Image/Page values, and a 4th Credits-and-Info sheet carrying search metadata + GenizahSearch.com hyperlink + Creator credit — refined across 6 rounds of smoke-verification patches (bilingual headers + source-language metadata + sheet rename + 4th sheet + label realignment + clickable URLs + Domains dedupe + Image/Page int coercion). On web only, JSON gains 3 additive per-item flags (`has_pgp`, `is_printed`, `domains`) with envelope `schema_version` unchanged; desktop has no JSON export. CONTEXT D-04 was REVERSED 2026-05-20 for the row content layer only (the D-02 transcription-text prohibition + D-10 parallels-envelope strip + conditional RTL view-direction are UNCHANGED). v7.12 multitenant invariants carry forward (zero raw `app.storage.user` under `web/`; allowlist still `[]`).

</details>

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

Phases 999.2 and 999.3 were promoted into v7.13 as Phase 93 (PGP filter) and Phase 94 (research-grade exports) on 2026-05-19. No active backlog entries remain at this milestone boundary.

### Phase 97: More LOCAL features

**Goal:** Make My Library usable at the scale Seewald'''s prototype already serves (13K files / 43 GB, target ceiling 50K / 50 GB) by adding crash-recovery semantics, durable text cache, and atomic Tantivy rebuild — and extend the file-format set with three light textual formats (.html / .xlsx / .csv). Does NOT add reading-experience features (OCR, side-by-side PDF) and does NOT touch web LOCAL exposure.
**Requirements**: D-NEW-1, R-03, R-02, R-04, R-01, C-02, C-05, D-NEW-8, F-01, F-02, F-03, F-04, F-05, F-06, C-01, C-03, C-04, C-06, U-01, U-02, U-03, U-04, D-NEW-2, D-NEW-3, D-NEW-4, D-NEW-5, D-NEW-6, D-NEW-7
**Depends on:** Phase 96
**Plans:** 6/6 plans complete

Plans:
- [x] 97-01-PLAN.md — Wave A: SQLite migration v1->v2 + cached_text (zstd) + atomic Tantivy rebuild + WAL+FULL durability bracket + recovery UX gate
- [x] 97-02-PLAN.md — Wave B: byte/count/time commit policy (NO heap-sampling per RESEARCH Issue #1) + 100 MB raw cap + zip-bomb defense for .docx/.xlsx + mtime_ns incremental audit
- [x] 97-03-PLAN.md — Wave C: HTML (lxml.html, NOT BeautifulSoup) + XLSX (openpyxl streaming) + CSV extractors with encoding chains; F-06 RTL-metadata-only invariant
- [x] 97-04-PLAN.md — Wave D: ceiling 50K/50GB soft warning + pre-scan worker thread + persisted folder counters + disk indicator with merge headroom
- [x] 97-05-PLAN.md — Wave E: phase-aware ETA + scan_run_id (mutated-rows-only per RESEARCH Issue #4) + FolderWalkWorker QThread + View All 500-cap incremental render
- [x] 97-06-PLAN.md — Wave F: network drive semantics + file-change-during-index + supported-extension row policy + chunk_locator per format + bilingual EN+HE privacy disclosure + 4 invariant CI guards

### Phase 98: NLI Resilience — circuit-breaker and bounded-timeout hardening for all NLI/IIIF code paths

**Goal:** Prevent any single NLI/IIIF upstream slowdown from hanging `genizah-web`. Bound the per-request blocking budget on every NLI-touching code path via (a) a shared circuit breaker (`shared/nli_circuit_breaker.py`) that short-circuits requests when NLI is degraded and (b) shorter env-configurable read timeouts. The 2026-05-25 production outage (7 minutes unresponsive, SIGTERM hung 90s, SIGKILL required) is the trigger and the regression test.

**Scope items (D-XX from CONTEXT.md — Phase 98 has no REQ-IDs; the 28 locked decisions D-01..D-28 are the spec):**
- D-01..D-09 — Shared breaker module (single global key, module-level singleton, threading.Lock, time.monotonic, env-driven knobs)
- D-10 — Drop `NLI_SEMAPHORE_TIMEOUT` default from 20 → 1
- D-11/D-12 — Circuit check before AND after semaphore acquisition in `web/api.py::fetch_fl_ids_from_nli`
- D-13..D-23 — Wire breaker into all 10 NLI fetch sites (5 in web/api.py, 3 in puzzle, 2 wired + 2 migrated in genizah_core.py)
- D-24/D-25 — PostHog telemetry on open/close (server-side, fire-and-forget, never raises)
- D-26/D-27 — Concurrency test (20 threads vs hanging session must complete <10s); lock-correctness test (N=50 simultaneous record_failure → consecutive_failures == 50)
- D-28 — Telemetry emission test

**Out of scope (explicitly):** Async refactor to httpx, event-loop watchdog, multi-worker uvicorn (per CONTEXT.md `<deferred>`).

**Depends on:** Phase 97

**Source CONTEXT:** `.planning/phases/98-nli-resilience-circuit-breaker-and-bounded-timeout-hardening/98-CONTEXT.md` (28 locked decisions)
**Source RESEARCH:** `.planning/phases/98-nli-resilience-circuit-breaker-and-bounded-timeout-hardening/98-RESEARCH.md` (HIGH confidence)
**Source VALIDATION:** `.planning/phases/98-nli-resilience-circuit-breaker-and-bounded-timeout-hardening/98-VALIDATION.md`

**Plans:** 5/6 plans executed (98-06 autonomous tasks done, canary checkpoint pending)

Plans:
- [x] 98-01-PLAN.md — Wave 1: shared/posthog_server.py (factored telemetry helper — Option (a) per RESEARCH); fire-and-forget queue+daemon idiom factored out of web/api_hardening.py
- [x] 98-02-PLAN.md — Wave 2: shared/nli_circuit_breaker.py (module-level singleton, threading.Lock, time.monotonic); tests/test_nli_circuit_breaker.py (Nyquist-critical D-26 + D-27 lock correctness + AST guards); tests/conftest.py autouse fixture
- [x] 98-03-PLAN.md — Wave 3: web/api.py (5 call sites D-11..D-18) — drop NLI_SEMAPHORE_TIMEOUT 20→1; wire fetch_fl_ids_from_nli with pre+post semaphore guards; nli_image / _fetch_nli_image_bytes / proxy_image (NLI-host-conditional)
- [x] 98-04-PLAN.md — Wave 3 parallel: shared/puzzle_image_service.py (_fetch_iiif_image unconditional + _fetch_direct_url host-conditional); web/pages/puzzle.py::_resolve_folios; D-19, D-20, D-21
- [x] 98-05-PLAN.md — Wave 3 parallel: genizah_core.py — migrate fetch_iiif_manifest + fetch_marc_data off class-attribute breaker; wire 2 new sites _fetch_single_worker (D-22) + _fetch_fl_ids (D-23); REMOVE legacy class-attribute breaker per RESEARCH Pitfall 5
- [ ] 98-06-PLAN.md — Wave 4: cross-module invariant tests; CLAUDE.md env var docs; docs/OPEN_ISSUES.md closeout; .planning/ROADMAP.md self-update; CHANGELOG.md entry; production canary checkpoint (human-verify — PENDING)

**Wave structure:** 1 (98-01 posthog_server) → 2 (98-02 breaker module + tests) → 3 (3 parallel: 98-03 [web/api.py], 98-04 [puzzle], 98-05 [genizah_core.py]) → 4 (98-06 cross-module integration + docs + canary).

**UI hint:** no — pure resilience infrastructure, no user-facing changes.

---

*Roadmap created: 2026-02-09*
*Last updated: 2026-05-25 — Phase 98 PLANNED (6 plans across 4 waves). Incident-driven; closes 2026-05-25 NLI hang per docs/INCIDENT-2026-05-25-CODEX-CRITIQUE.md. Shared `shared/nli_circuit_breaker.py` (module-level singleton, time.monotonic, threading.Lock) wired into all 10 NLI fetch sites; 6 new env knobs; NLI_SEMAPHORE_TIMEOUT default dropped 20→1; PostHog telemetry via factored `shared/posthog_server.py` (Option (a) — shared/ no longer depends on web/). Phase 97 PLANNED (6 plans across 6 waves). Recovery foundation lands BEFORE ceiling lift (Codex P0 sequencing). 4 RESEARCH plan-time issues encoded: tantivy-py 0.25.1 commit policy is bytes/count/time only (no heap-sampling); lxml.html substitutes for BeautifulSoup (no new dep); R-02 atomic swap closes SearchEngine reader before os.rename (Windows os error 5 fix); scan_run_id is written ONLY on rows mutated this run (not cache-hit skips). Phase 96 PLANNED (9 plans across 6 waves). Closes v7.14 milestone: D-F5 LOCAL highlight P1 fix, D-F4 PDF extraction detect-then-fallback, D-F1 per-file opt-out tree with session-JSON persistence, NEW-1 redundant button removal, NEW-2 LOCAL navigation primitive + View-All separator. D-F2 (OCR) + D-F3 (side-by-side PDF) explicitly deferred to v7.15+. Phase 95 invariants (RRF POST-dedup, 3 cloud-write gates at TOP, web LIBRARY_CODES `[]`, multitenant `[]`) preserved.*
