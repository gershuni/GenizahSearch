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
- **v7.7 Volume-Aware Browse** -- Phases 58-59

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

### v7.7 Volume-Aware Browse (Phases 58-61)

**Goal:** Fix multi-IE image/text mismatch by making search→browse→paging IE-aware across both apps.

**Scope:** Web + Desktop. 3,193 affected manuscripts (1.5%).

#### Phase 58 — IE Volume Data Infrastructure

**Goal:** Per-IE page data exists so browse can address each IE independently.

**Requirements:** DATA-01, DATA-02, DATA-03, REG-02, REG-04

**Success Criteria:**
1. `ie_volume_map.json` contains IE-to-IIIF-suffix mapping for all 3,193 multi-IE manuscripts (from MARC 907 field order)
2. `browse_map` provides per-IE page lists — no cross-IE dedup — so each IE's pages are independently addressable
3. Single-IE manuscripts produce identical browse_map output as before (zero regression)
4. Tantivy index and desktop app are completely untouched

**Key files:** `scripts/build_primary_ie_map.py`, `genizah_core.py:1906` (`dedupe_browse_map`), `primary_ie_map.json`

#### Phase 59 — Volume-Aware Web Browse

**Goal:** Users always see matching text and images for multi-IE manuscripts.

**Requirements:** NAV-01, NAV-02, NAV-03, IMG-01, IMG-02, IMG-03, PAG-01–PAG-05, REG-01, REG-03

**Success Criteria:**
1. Clicking a search result from IE X opens browse showing IE X's images and text
2. Image and displayed text always belong to the same IE — no mismatch
3. Prev/next stays within active IE; page count reflects that IE only
4. Multi-IE manuscripts show a simple volume dropdown (IE label + page count); single-IE show no selector
5. All single-IE browse/search/URL/session and community features unchanged

**Key files:** `web/api.py:322` (`fetch_fl_ids_from_nli`), `web/pages/browse.py:481` (`BrowseState`), `web/services.py:97` (`BrowsePage`), `genizah_core.py:7682` (`get_browse_page`)

#### Phase 60 — Desktop Volume-Aware Browse

**Goal:** Desktop browse matches web: search→browse propagates IE, volume selector in Browse tab, per-IE paging.

**Requirements:** DSK-01, DSK-02, DSK-03

**Success Criteria:**
1. Desktop search result → Browse tab opens with the correct IE's images and text
2. Volume selector dropdown in Browse tab (matching web design — label + page count per volume)
3. Prev/next navigation stays within active IE; page count reflects that IE only
4. Single-IE manuscripts completely unchanged in desktop

**Key files:** `genizah_app.py` (Browse tab, search result navigation), `genizah_core.py:7745` (`get_browse_page`)

**Status:** COMPLETE (1/1 plans, 2026-03-31)

#### Phase 61 — Volume Session, Community Context & Corpus Validation

**Goal:** Volume state persists across sessions, community writes include IE context, mapping validated corpus-wide.

**Requirements:** URL-01, URL-02, CW-01, CW-02, VAL-01

**Success Criteria:**
1. Web browse URL includes `volume_ie` parameter for shareable links (already partially done)
2. Session restore preserves active volume across browser refresh and desktop restart
3. Corrections submitted from a volume-aware browse include the active IE in the payload
4. Comments reference the specific volume/IE they were made on
5. Automated validation script confirms 907→suffix mapping against live IIIF manifests for a sample of manuscripts

**Key files:** `web/pages/browse.py` (URL/session), `shared/corrections_service.py`, `web/supabase_client.py`, `scripts/build_ie_volume_map.py`

## Progress

**Total milestones shipped:** 13 (through v7.6)
**Total phases completed:** 60 (Phases 1-60, including 58-60 of v7.7)
**Total plans completed:** ~193

---
*Roadmap created: 2026-02-09*
*Last updated: 2026-03-31 after Phase 60 complete*
