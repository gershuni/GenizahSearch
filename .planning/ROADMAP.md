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
- **v7.15 My Library Visual** -- Phases 99-101 (shipped 2026-05-28). See `milestones/v7.15-ROADMAP.md`

## Phases

<details>
<summary>✅ v7.15 My Library Visual (Phases 99-101) — SHIPPED 2026-05-28</summary>

- [x] Phase 99: PDF Page Renderer (2/2 plans) — completed 2026-05-27
- [x] Phase 100: LOCAL PDF Image in ResultDialog + Browse (3/3 plans) — completed 2026-05-27
- [x] Phase 101: LOCAL PDF Text Extraction RTL Fix + Phase 100 Remnant Cleanup (2/2 plans + UAT follow-ons) — completed 2026-05-28

</details>

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

### Phase 101: LOCAL PDF text extraction RTL fix and Phase 100 remnant cleanup

**Goal:** Clear the remnant issues blocking a clean v7.15 release. Primary: fix RTL/bidi word-order reversal in LOCAL PDF text extraction so Hebrew/Judeo-Arabic PDF transcriptions read in correct reading order (each line currently shows last-word-first). Secondary: close the Phase 100 code-review remnants and a pre-existing test-isolation flake.

**Requirements**: D-01/D-03/D-05 (RTL word-order fix), D-04 (auto-reindex via extractor-version bump), D-06 (real Hebrew fixture — inbound asset, skip-if-absent), D-07 (WR-01 single lookup), D-08 (WR-02 regression test), D-09 (batch-order flake fix) — tracked via CONTEXT.md decision IDs (no formal REQ-IDs for this pre-release polish phase)
**Depends on:** Phase 100

**Scope (remnant items):**
1. **RTL PDF text extraction** (P3, `docs/OPEN_ISSUES.md`; surfaced in Phase 100 UAT) — LOCAL PDF text extraction reverses word order per line on some RTL books. Likely PyMuPDF `get_text` returning glyph runs in visual order without bidi reordering. Fix: bidi-aware reorder of extracted lines (e.g. `python-bidi`) or x-coordinate-aware span reordering for RTL pages. Affects search indexing quality for LOCAL Hebrew PDFs (image rendering is unaffected — only the text layer).
2. **Code review WR-01** (`genizah_app.py::_open_local_browse_page`) — Browse panel computes `is_pdf` from one `_lookup_local_filepath` call but re-looks-up `filepath` separately; if the two diverge the image pane reveals empty. Fix: compute `filepath` once, derive `is_pdf` from it.
3. **Code review WR-02** — add a regression test asserting `PdfImageController._pending` is empty immediately after `discard_scope` (callback-retention guard).
4. **Test-isolation flake** — `tests/test_local_indexer.py::test_txt_undecodable_marked_encoding_error` passes in isolation but fails in batch ordering (global-state pollution from a sibling test). Phase 100 did not touch this file; pre-existing.

**Plans:** 2/2 plans complete

Plans:
- [x] 101-01-PLAN.md (Wave 1) — RTL word-order fix in extract_pdf_pages sort=True fallback + extractor-version auto-reindex + Wave 0 RTL/version tests + F-06 AST guard update + D-09 flake fix + fixture provenance README
- [x] 101-02-PLAN.md (Wave 1) — WR-01 single-lookup _open_local_browse_page + WR-02 discard_scope regression test + OPEN_ISSUES.md bookkeeping

### Phase 102: LOCAL PDF Text-Layer Extraction Rewrite (RTL-gated reorder + letter-spacing de-collapse)

**Goal:** Rewrite the LOCAL PDF text-layer extractor in `shared/local_indexer.py` onto a `page.get_text("rawdict")` (per-glyph bbox) foundation that produces clean, searchable plain text for the Tantivy indexer. Validated against real corpus by Spike 001 (`.planning/spikes/001-meiri-glyph-reorder-vs-current/`). The rewrite must address the failure-mode catalog the spike found (F-A..F-G):
- **RTL-gated segment reorder** adapted from Ephraim Meiri's `ephraim_meiri_pdf_converter/pdf_to_docx.py::_normalize_span_dir` + `_regroup_lines` + `_fix_visual_brackets` — fixes word/segment order, headers (F-F), digit/ref placement (F-A), reversed parens (F-C). **CRITICAL: gate reorder to RTL/Hebrew content only** — Meiri's reorder HURTS Latin/LTR text (spike: NW Semitic Dictionary was better in the current extractor), so LTR lines/blocks must stay on the current/non-reordered path. **No LTR regression.**
- **Adaptive per-line letter-spacing de-collapse** (F-D/F-E) — the spike's biggest finding: justified Hebrew typesetting (e.g. אוצר הגאונים 46% single-letter tokens, רמבם 21%) shatters words into single letters; neither current nor Meiri fixes it. Re-derive word spacing from glyph bboxes using a per-line adaptive threshold (~1.8× median inter-glyph gap), ignoring embedded space glyphs. Must run BEFORE reorder so de-spaced words can be reordered (F-E). Prototyped working in the spike.
- **Punctuation normalization** (F-B) — no space before punctuation.
- **Corrupt-encoding detection** (F-G) — detect garbage text layers (bad/missing ToUnicode cmap, e.g. `Israeli_Vilna_shabbat_part_2.pdf`) and flag/skip rather than indexing garbage (these are future OCR consumers).

Closes **D-F13**; reframes **D-F14** (adopt Meiri's reorder *core*, RTL-gated — NOT wholesale, NOT DOCX pipeline).
**Requirements**: D-F13, D-F14, + spike failure modes F-A..F-G (catalog in Spike 001 README)
**Depends on:** Phase 101 · **Evidence:** Spike 001 (verdict PARTIAL — reshaped this phase)
**Scope:** Desktop-only (My Library), text-layer PDFs. **Out of scope:** OCR for image-only scans (D-F2 — deferred as an optional opt-in extension, seeded; a large share of the real library is image-only but common users won't need OCR and off-the-shelf pre-OCR exists); P3 View All renderer cleanups (D-F8/D-F10/D-F7).
**Verification:** Regression fixtures for each failure mode — letter-spaced page (אוצר הגאונים-style), letter-spaced+reversed line, RTL header, AND an LTR/Latin PDF that must NOT regress; existing `tests/fixtures/local_indexer/single_word_per_line.pdf` guard still passes.
**Note:** First piece of v7.16 work, appended after shipped v7.15 (Phases 99-101).
**Plans:** 4/5 plans executed

Plans:
- [x] 102-01-PLAN.md (Wave 1) — RTL helpers: baseline line-grouping, RTL classify, adaptive 1.8x-median de-space (word-unit bbox-unions), Meiri reorder core, bracket/punctuation fix + glyph-trace fixtures
- [x] 102-02-PLAN.md (Wave 2) — extract_pdf_pages rawdict rewrite (D-01/D-11) + LTR-damage guard (D-03) + corrupt-encoding (D-07) + multi-column-suspected (D-09) detectors
- [x] 102-03-PLAN.md (Wave 3) — D-06 strip nikud once in _write_page_doc for ALL LOCAL formats (content == cached_text, both consonantal — NO divergence; un-vocalized search matches vocalized text) + extraction_format_version 1->2 + buffer-then-decide corrupt flow + corrupt_encoding wired into 3 in-file status surfaces. Non-PDF nikud DISPLAY deferred (SEED-004).
- [x] 102-04-PLAN.md (Wave 1) — D-08 surface 4 desktop tree label/color + migration 2->3 (corrupt_encoding kept, NO auto-flip per D-10)
- [ ] 102-05-PLAN.md (Wave 4) — end-to-end fixtures (letter-spaced, RTL header, corrupt-encoding, LTR no-regression) + e2e extract/index/query tests covering F-A..F-G + D-06 + OPEN_ISSUES.md bookkeeping (D-F13 fixed, D-F14/D-F16 addressed)

---

*Roadmap created: 2026-02-09*
*Last updated: 2026-05-27 — v7.15 My Library Visual roadmap created (`/gsd-roadmap`). 2 phases (99 PDF Page Renderer + 100 LOCAL PDF Image in ResultDialog + Browse), 6/6 PDFIMG-* requirements mapped. Desktop-only milestone closing deferred item D-F3 (side-by-side PDF). Numbering continues from v7.14's last phase 98. v7.13 + v7.14 milestones remain CLOSED (shipped v7.13.0 2026-05-21; v7.14.0 2026-05-24; reconciled 2026-05-27). Archives: `.planning/milestones/v7.13-ROADMAP.md` / `v7.13-REQUIREMENTS.md` / `v7.14-ROADMAP.md`.*
