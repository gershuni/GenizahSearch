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
- **v7.13 Research-Grade Downloads & PGP Filter** -- Phases 93-94 (shipped 2026-05-21)
- **v7.14 My Library — Local Document Search** -- Phases 95-98 (shipped 2026-05-24; closed 2026-05-27)
- 🚧 **v7.15 My Library Visual** -- Phases 99-100 (in progress)

## Phases

### 🚧 v7.15 My Library Visual (In Progress)

**Milestone Goal:** Show the source PDF *page image* alongside extracted text for LOCAL ("My Library") results in the desktop app, closing deferred item D-F3. Desktop-only — web "My Library" does not exist, so the dual-app maintenance rule does not apply. The work splits in two: a shared on-demand PDF page renderer + off-thread worker (with graceful failure handling), then wiring that renderer into the two desktop surfaces that show LOCAL hits (`ResultDialog` + Browse panel). Rendering is lazy and ephemeral — the 10K×500-page corpus is never bulk-rendered, and no rendered page image is ever written to disk. Non-PDF LOCAL files stay text-only.

- [x] **Phase 99: PDF Page Renderer** - Shared on-demand PyMuPDF page renderer + off-thread worker + graceful failure handling
 (completed 2026-05-27)
- [x] **Phase 100: LOCAL PDF Image in ResultDialog + Browse** - Wire the renderer into both desktop surfaces; non-PDF files stay text-only (completed 2026-05-27)

#### Phase 99: PDF Page Renderer
**Goal**: A single PDF page can be rendered to a QImage on demand, off the UI thread, without ever loading or bulk-rendering the corpus — and any render failure degrades gracefully instead of hanging or crashing the app.
**Depends on**: Phase 98 (last v7.14 phase); builds on the v7.14 `local_indexer.py` + PyMuPDF dependency
**Requirements**: PDFIMG-01, PDFIMG-02, PDFIMG-06
**Success Criteria** (what must be TRUE):
  1. Given a PDF filepath (from `get_filepath(sys_id)`) and a 1-based `page_num`, the renderer returns the QImage for exactly that page (`fitz` page index = `page_num - 1`), without loading the rest of the document.
  2. Rendering happens on a background worker mirroring the existing `ImageLoaderThread` QThread pattern, so the UI never blocks while a page renders.
  3. Repeated renders reuse a bounded LRU of open `fitz.Document` handles; no rendered page image is written to disk and only the currently displayed page(s) are held in memory.
  4. A missing file, corrupt/encrypted PDF, out-of-range page, or render exception returns a graceful failure result (placeholder signal + log entry) rather than raising into the UI.
**Plans**: 2 plans (2 waves)
**UI hint**: yes

Plans:
- [x] 99-01-PLAN.md (Wave 1) — Render core: PdfRenderFailure enum, DocLRU, single-page render+copy, Wave 0 tests + fixtures
- [x] 99-02-PLAN.md (Wave 2) — PdfRenderWorker: long-lived queue-driven QThread, tokenized signals, no-crash envelope, clean shutdown

#### Phase 100: LOCAL PDF Image in ResultDialog + Browse
**Goal**: Researchers see the actual scanned/typeset PDF page next to the extracted text for LOCAL hits, in both the desktop ResultDialog and the desktop Browse panel, with the image staying in sync as they navigate — while non-PDF LOCAL files remain cleanly text-only.
**Depends on**: Phase 99
**Requirements**: PDFIMG-03, PDFIMG-04, PDFIMG-05
**Success Criteria** (what must be TRUE):
  1. Opening a LOCAL PDF search result in `ResultDialog` shows the rendered page image alongside the highlighted extracted text; moving prev/next between results re-renders the image for the newly shown hit.
  2. Opening a LOCAL PDF result in the Browse panel reveals the (previously hidden) image pane showing the rendered page; prev/next *page* navigation updates the image to the matching page in sync with the text.
  3. Opening a non-PDF LOCAL file (`.docx`/`.html`/`.xlsx`/`.csv`/`.txt`) in either surface keeps the view text-only — the image pane stays hidden, gated on file extension, with no render attempt.
  4. A LOCAL PDF that fails to render shows a visible placeholder in the image pane (per Phase 99) without freezing or crashing either surface.
**Plans**: 3 plans (2 waves)
**UI hint**: yes

Plans:
- [x] 100-01-PLAN.md (Wave 1) — Shared PdfImageController (token + latest-wins + 150ms debounce + ~8s watchdog + extension gate + per-reason localized placeholder map) + GenizahGUI worker ownership/shutdown + unit tests
- [x] 100-02-PLAN.md (Wave 2) — ResultDialog wiring: reveal external pane + render LOCAL PDF page in sync with prev/next result and within-document page nav (PDFIMG-03/05)
- [x] 100-03-PLAN.md (Wave 2) — Browse wiring: gate image pane on .pdf in _open_local_browse_page + render page in sync with prev/next page nav (PDFIMG-04/05)

<details>
<summary>✅ v7.14 My Library — Local Document Search (Phases 95-98) — SHIPPED 2026-05-24, closed 2026-05-27</summary>

See: .planning/milestones/v7.14-ROADMAP.md

6 phases (95, 96, 97, 97.2 INSERTED, 97.3 INSERTED, 98), 37 plans. Desktop-first "My Library" tab indexing user folders of `.docx`/`.pdf`/`.txt`/`.html`/`.xlsx`/`.csv` into a separate Tantivy side-index merged into Search/Composition/Parallels via RRF k=60 POST-dedup, with a `LOCAL` badge, a corpus selector, and three cloud-write gates keeping personal corpora off the cloud. Public v7.14.0 release 2026-05-24 (Phase 95 MVP + Phase 96 completion). Internal hotfix chain through 2026-05-27: Phase 97 (scale to 13K files / 43 GB + `.html`/`.xlsx`/`.csv` + atomic rebuild + crash recovery), Phase 97.2 INSERTED (recovery cascade + Reset My Library), Phase 97.3 INSERTED (mega-folder UI-thread stability). Phase 98 (web infra) added a shared NLI circuit breaker wired into all 10 NLI/IIIF fetch sites, dropping worst-case per-request blocking 45s → ~9s (closes the 2026-05-25 production hang). v7.12 multitenant invariants preserved (zero raw `app.storage.user` under `web/`; LOCAL never reaches web/API/Supabase).

</details>

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

## Progress

**Execution Order:**
v7.15 phases execute in numeric order: 99 → 100

| Phase | Milestone | Plans Complete | Status | Completed |
|-------|-----------|----------------|--------|-----------|
| 99. PDF Page Renderer | v7.15 | 2/2 | Complete    | 2026-05-27 |
| 100. LOCAL PDF Image in ResultDialog + Browse | v7.15 | 3/3 | Complete    | 2026-05-27 |

## Backlog

Phases 999.2 and 999.3 were promoted into v7.13 as Phase 93 (PGP filter) and Phase 94 (research-grade exports) on 2026-05-19. No active backlog entries remain at this milestone boundary.

---

*Roadmap created: 2026-02-09*
*Last updated: 2026-05-27 — v7.15 My Library Visual roadmap created (`/gsd-roadmap`). 2 phases (99 PDF Page Renderer + 100 LOCAL PDF Image in ResultDialog + Browse), 6/6 PDFIMG-* requirements mapped. Desktop-only milestone closing deferred item D-F3 (side-by-side PDF). Numbering continues from v7.14's last phase 98. v7.13 + v7.14 milestones remain CLOSED (shipped v7.13.0 2026-05-21; v7.14.0 2026-05-24; reconciled 2026-05-27). Archives: `.planning/milestones/v7.13-ROADMAP.md` / `v7.13-REQUIREMENTS.md` / `v7.14-ROADMAP.md`.*
