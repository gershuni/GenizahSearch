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

**Plans:** 5/5 plans complete

Plans:
- [x] 84-01-PLAN.md -- Bridge module foundation (cudl_normalize) + leading-zero collision audit (NORM-03)
- [x] 84-02-PLAN.md -- Mosseri reverse alias index reusing construct_mosseri_cudl_label (NORM-01)
- [x] 84-03-PLAN.md -- Or. patterns + numeric-collapse helper + collision exclusion + forward shelfmark_to_cudl_label (NORM-02, NORM-03)
- [x] 84-04-PLAN.md -- Wire bridge into 4 D-08 call sites + runtime alias-index hook (NORM-01, NORM-02, NORM-03)
- [x] 84-05-PLAN.md -- NORM-04 regression guard: golden fixture + scan diff + canonical-untouched assertion

#### Phase 85 -- Synthetic FJMS Inventory Rows

**Goal:** Independent libraries.csv rows for the ~93 T-S FJMS-only inventories (and any residue from Mosseri/Or post-Phase-84) using Option-2 18-digit synthetic sys_id format (`99` + InventoryId-padded-10 + `000000`).

**Requirements:** SYNTH-01, SYNTH-02, SYNTH-03, SYNTH-04, SYNTH-05, SYNTH-06

**Success criteria:**
1. `is_synthetic_sys_id()` helper plus encode/decode utilities exist in shared code, with a test suite covering boundary cases (real Alma, synthetic, malformed).
2. The Tantivy index includes synthetic rows so `T-S NS 329.96` and similar shelfmarks return search results in all modes (text/title/shelfmark/Responsa).
3. Browse renders synthetic-row pages with FJMS catalogue + bibliography + measurements + CUDL manifest images, gracefully handling absent NLI fields (no empty placeholders, no console errors, no broken links).
4. FJMS enrichment dialogs (catalogue, bibliography, measurements, free description) populate via InventoryId fallback when sys_id is synthetic.
5. Lists, exclusions, parallels, comments, corrections round-trip synthetic sys_ids without crashes or silent data loss; web and desktop parity preserved.

**Plans:** 5/5 plans complete

Plans:
- [x] 85-01-PLAN.md -- shared/synthetic_sys_id.py helper module (is_synthetic / encode / decode) + golden fixtures (SYNTH-01)
- [x] 85-02-PLAN.md -- scripts/generate_synthetic_rows.py + libraries.csv marker block + csv_bank loader marker tolerance + audit artifacts (SYNTH-02, SYNTH-03)
- [x] 85-03-PLAN.md -- scripts/export_fist_enrichment.py UNION ALL synthetic AlmaId rows in 11+ enrichment tables (SYNTH-05)
- [x] 85-04-PLAN.md -- Browse hide-NLI + CUDL-default + D-14 network-call guards (web + desktop parity, ~22-26 sites) (SYNTH-04)
- [x] 85-05-PLAN.md -- Public API is_synthetic field + PostHog property + community round-trip + corrections-write deferral (SYNTH-06)

**Outcome (2026-05-09):** All five plans shipped (infrastructure intact); the actual synthetic-row population was reverted during UAT. Plan 02's "inclusive coverage stance" qualified InventoryIds with ANY FJMS signal, but the resulting 5,035 synthetic rows had only bibliography pointers (no text, image, or catalog description) — useless for actual research without the underlying manuscript. Additionally, 175 of those rows shadowed real-Alma series-children (e.g. synthetic `T-S NS 161` shadowed 1,009 real `T-S NS 161.x` rows). User-decision-revert: `libraries.csv` synthetic block emptied (markers preserved), `fist_data/synthetic_manifest.json` set to `[]`, `fjms_enrichment.db` restored from gz backup. Infrastructure (helper module, browse gates, /api `is_synthetic`, corrections-write reject) stays dormant; no synthetic sys_ids in production. **Phase 86 will re-attempt with stricter qualification criteria (CUDL-image-only, possibly relaxing D-05a for unambiguous overlaps).**

#### Phase 86 -- CUDL Coverage Audit + Synthetic Re-attempt

**Goal:** Confirm Phase 84 closed the normalization gap; re-attempt synthetic rows with image-bearing-only criteria; produce a durable report.

**Requirements:** AUDIT-01, AUDIT-02, AUDIT-03

**Success criteria:**
1. `scripts/scan_cudl_orphans.py` re-run after Phase 84 reports fewer than 200 truly-orphan CUDL classmarks with reasoned categorization for each.
2. `reports/cudl_coverage.md` documents the post-Phase-84 breakdown by collection (matched-by-normalization, residual-unmatched) with methodology and re-run instructions.
3. The 461 NLI Oxford-mislabel rows fixed in v7.9.4 still resolve correctly; no library_code attribution regressions detected.
4. Both apps build and pass test suite green; check_docs green; no PostHog error spike post-deploy.
5. **Synthetic re-attempt with image-bearing criteria.** Modify `scripts/generate_synthetic_rows.py::_build_qualifying_inventories` to ONLY emit synthetic rows where:
   (a) The InventoryId has a CUDL manifest in `nli_crossref.db.cambridge_manifests` (Tier 1 or Tier 2), AND
   (b) No real-Alma row in libraries.csv has a child shelfmark of this synthetic's leaf (filter from `reports/synthetic_parent_shelfmarks.csv`), AND
   (c) Optionally relax D-05a STRICT for known-safe multi_signature cases — when all FIST SignatureIds resolve to the same canonical_shelfmark + library_code, pick lowest SignatureId per the existing tie-break logic (so T-S NS 329.96 with 12 multi_signature entries CAN be synthesized). Baseline: ~100-500 image-bearing synthetics expected (instead of 5,035 bib-only). The originating user case (T-S NS 329.96) closes here.
6. **Carry-forward residue artifacts:** keep `reports/synthetic_ambiguity_residue.csv` and `reports/synthetic_parent_shelfmarks.csv` as documentation of the Plan 02 attempt. Document the Phase 85 outcome in CHANGELOG so users + future maintainers understand why infrastructure exists with no production data.

**Plans:** 2/5 plans executed

Plans:
- [x] 86-01-PLAN.md -- shared/fist_cudl_bridge.py sibling module + 4 D-02a normalizers (Mosseri Roman, prefix-strip, (N) series-strip, Or. dot-fix) + alias-index builder + unit tests (AUDIT-01)
- [x] 86-02-PLAN.md -- _build_qualifying_inventories CUDL-walked rewrite + D-04 multi_signature relax + D-06 parent-shadow filter + pattern_guess residue column + T-S NS 329.96 closure fixture (AUDIT-01)
- [ ] 86-03-PLAN.md -- 86-RESIDUE-PATTERNS.md D-02c human-in-the-loop adjudication artifact + scripts/build_residue_patterns_artifact.py + CHECKPOINT + accepted-rule integration into bridge (AUDIT-01)
- [ ] 86-04-PLAN.md -- Operational sequence (generate --apply / export / scan) + reports/cudl_coverage.md (AUDIT-02) + scripts/audit_nli_attribution.py + tests/test_nli_oxford_attribution.py (AUDIT-03) + HUMAN-UAT + OPEN_ISSUES.md update (AUDIT-01, AUDIT-02, AUDIT-03)
- [ ] 86-05-PLAN.md (optional) -- Release coordination: bump_version.py 7.11.0 + CHANGELOG + CLAUDE.md Recently Changed + README What's New; web-only deploy per feedback_no_github_release_for_web_only.md

## Progress

**Phase 84 — CUDL Shelfmark Normalization (next)**

Run `/gsd-discuss-phase 84` (or `/gsd-plan-phase 84` to skip discussion).

---
*Roadmap created: 2026-02-09*
*Last updated: 2026-05-05 -- v7.11 CUDL Coverage milestone scoped. v7.10 Search API archived at .planning/milestones/v7.10-ROADMAP.md.*
