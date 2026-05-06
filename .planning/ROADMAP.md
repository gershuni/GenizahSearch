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
- **v7.11 CUDL Coverage & Synthetic Inventories** -- Phases 84-86 (active 2026-05-05)

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

### v7.11 CUDL Coverage & Synthetic Inventories (Phases 84-86) -- ACTIVE

Goal: Close the gap between CUDL's ~141K classmark catalogue and GenizahSearch's libraries.csv so users searching for any CUDL-catalogued shelfmark land on a usable record. Bridge layer normalization (Mosseri/Or/CUL fixes) recovers thousands of already-existing rows; synthetic-row mechanism adds independent libraries.csv entries for the residue of FJMS-only inventories that have no NLI Alma record (e.g. T-S NS 329.96).

#### Phase 84 -- CUDL Shelfmark Normalization

**Goal:** Cross-system shelfmark normalization that bridges CUDL's classmark form (e.g. `mosseriiii27o`, `tsar48.211`, `tsf8.2`) to libraries.csv's variants (`Moss. III,27O`, `T-S Ar. 48.211`, `T-S F 8/002`).

**Requirements:** NORM-01, NORM-02, NORM-03, NORM-04

**Success criteria:**
1. CUDL Mosseri classmarks resolve to existing `library_code=Mosseri` rows for ≥98% of the 3,883-classmark CUDL Mosseri set.
2. Cambridge Or. classmarks resolve to existing libraries.csv rows for the `or<num>j<sub>` (letter-suffix) and `or<num>.<collapsed>` (numeric-collapse) patterns.
3. Slash, comma, dot-after-letter, and leading-zero patterns in CUL shelfmarks normalize uniformly across all sub-collections.
4. `scripts/scan_cudl_orphans.py` re-run reports a substantially reduced orphan count (target: ≤300 residue) with no regression on the 140K already-matching CUL rows.
5. Existing browse external-link buttons (CUDL, Manchester, JTS) and shelfmark search produce identical results to v7.10 for non-Mosseri/non-Or shelfmarks.

**Plans:** 5 plans

Plans:
- [ ] 84-01-PLAN.md -- Bridge module foundation (cudl_normalize) + leading-zero collision audit (NORM-03)
- [ ] 84-02-PLAN.md -- Mosseri reverse alias index reusing construct_mosseri_cudl_label (NORM-01)
- [ ] 84-03-PLAN.md -- Or. patterns + numeric-collapse helper + collision exclusion + forward shelfmark_to_cudl_label (NORM-02, NORM-03)
- [ ] 84-04-PLAN.md -- Wire bridge into 4 D-08 call sites + runtime alias-index hook (NORM-01, NORM-02, NORM-03)
- [ ] 84-05-PLAN.md -- NORM-04 regression guard: golden fixture + scan diff + canonical-untouched assertion

#### Phase 85 -- Synthetic FJMS Inventory Rows

**Goal:** Independent libraries.csv rows for the ~93 T-S FJMS-only inventories (and any residue from Mosseri/Or post-Phase-84) using Option-2 18-digit synthetic sys_id format (`99` + InventoryId-padded-10 + `000000`).

**Requirements:** SYNTH-01, SYNTH-02, SYNTH-03, SYNTH-04, SYNTH-05, SYNTH-06

**Success criteria:**
1. `is_synthetic_sys_id()` helper plus encode/decode utilities exist in shared code, with a test suite covering boundary cases (real Alma, synthetic, malformed).
2. The Tantivy index includes synthetic rows so `T-S NS 329.96` and similar shelfmarks return search results in all modes (text/title/shelfmark/Responsa).
3. Browse renders synthetic-row pages with FJMS catalogue + bibliography + measurements + CUDL manifest images, gracefully handling absent NLI fields (no empty placeholders, no console errors, no broken links).
4. FJMS enrichment dialogs (catalogue, bibliography, measurements, free description) populate via InventoryId fallback when sys_id is synthetic.
5. Lists, exclusions, parallels, comments, corrections round-trip synthetic sys_ids without crashes or silent data loss; web and desktop parity preserved.

#### Phase 86 -- CUDL Coverage Audit

**Goal:** Confirm the milestone closed the gap; produce a durable report and a regression-safe state.

**Requirements:** AUDIT-01, AUDIT-02, AUDIT-03

**Success criteria:**
1. `scripts/scan_cudl_orphans.py` re-run after Phase 85 reports fewer than 200 truly-orphan CUDL classmarks with reasoned categorization for each.
2. `reports/cudl_coverage.md` documents the post-milestone breakdown by collection (matched-by-normalization, synthetic-row, residual-unmatched) with methodology and re-run instructions.
3. The 461 NLI Oxford-mislabel rows fixed in v7.9.4 still resolve correctly; no library_code attribution regressions detected.
4. Both apps build and pass test suite green; check_docs green; no PostHog error spike post-deploy.

## Progress

**Phase 84 — CUDL Shelfmark Normalization (next)**

Run `/gsd-discuss-phase 84` (or `/gsd-plan-phase 84` to skip discussion).

---
*Roadmap created: 2026-02-09*
*Last updated: 2026-05-05 -- v7.11 CUDL Coverage milestone scoped. v7.10 Search API archived at .planning/milestones/v7.10-ROADMAP.md.*
