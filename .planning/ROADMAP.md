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
- **v7.3 Search Refinement & Scholarly Joins** -- Phases 54-57 (planned)

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

### v7.3 Search Refinement & Scholarly Joins (Planned)

**Milestone Goal:** Search refinement tools and scholarly join discovery to help researchers narrow results and find related fragments -- dimensions display and filtering, search within results, exclude known manuscripts, and FIST joins enrichment with a dedicated search mode. Both web (NiceGUI) and desktop (PyQt6).

**v7.3 Search Refinement & Scholarly Joins (Phases 54-57):**
- [ ] **Phase 54: Dimensions Display & Filtering** - Unit verification, batch size service, display in browse/results, pre-search and post-search dimension range filters, outlier clamping (both apps)
- [ ] **Phase 55: Search Within Results** - Restrict second query to current result sys_ids, breadcrumb chain display, one-click clear, intersection with existing filters (both apps)
- [ ] **Phase 56: Exclude Known Manuscripts** - Supabase list picker, shelfmark file import with resolution report, post-search exclusion filter, count display with source breakdown (both apps)
- [ ] **Phase 57: FIST Joins Browse & Search Mode** - Clickable join partners in browse, "Has joins" search filter, post-search join partner enrichment with group capping (both apps)

## Phase Details

### Phase 47: Foundation + Background Removal
**Goal**: Researchers have a shared data model for puzzle state and a working background removal engine that isolates parchment from solid-color library scanning backgrounds
**Depends on**: Nothing (first phase of milestone)
**Requirements**: BGRM-01, BGRM-02, BGRM-03
**Success Criteria** (what must be TRUE):
  1. A fragment image from NLI/Cambridge/Manchester can be loaded and its solid-color background removed, revealing the parchment shape with transparent surroundings — including low-saturation (gray/cream) backgrounds
  2. Background removal module exposes toggle (original vs stripped) and adjustable threshold that Phases 48-49 will wire into app UI
  3. A shared image resolver/cache resolves fragment images by (sys_id, folio, size, threshold) with cache invalidation on threshold change — usable by both web and desktop
  4. PuzzleDocument/PuzzleFragment data model serializes and deserializes correctly with proper nested dataclass rehydration (roundtrip test)
  5. joins.db SQLite sidecar is created with WAL mode and an explicit concurrency model for NiceGUI multi-request safety
**Plans**: 4 plans
Plans:
- [x] 47-01-PLAN.md — Data model + joins.db sidecar with concurrency and fragment index
- [x] 47-02-PLAN.md — Background removal engine with low-saturation fallback
- [x] 47-03-PLAN.md — Shared image resolver/cache service
- [x] 47-04-PLAN.md — Visual preview tool + quality checkpoint

### Phase 48: Desktop Canvas
**Goal**: Researchers can visually arrange manuscript fragment images on a desktop canvas with full spatial manipulation
**Depends on**: Phase 47
**Requirements**: CANV-01, CANV-03, CANV-04, CANV-05, CANV-06, PLAT-02
**Success Criteria** (what must be TRUE):
  1. User can type a shelfmark and add that fragment's image to the puzzle canvas in the desktop app
  2. User can drag a fragment to any position, rotate it to any angle, flip it horizontally or vertically, and resize it independently -- all with smooth visual feedback
  3. Multiple fragments (3+) can coexist on the canvas simultaneously without performance degradation
  4. Background-removed fragments overlay correctly, showing parchment shapes rather than overlapping rectangles
  5. The desktop puzzle is accessible from the app's main navigation
**Plans**: 3 plans
Plans:
- [x] 48-01-PLAN.md — Core canvas building blocks (PuzzleFragmentItem, PuzzleCanvasView, PuzzleImageLoaderThread)
- [x] 48-02-PLAN.md — PuzzleCanvasWindow with toolbar, shelfmark autocomplete, and singleton pattern
- [x] 48-03-PLAN.md — Integration buttons (Browse, ResultDialog, Lists) + visual checkpoint

### Phase 49: Web Canvas
**Goal**: Researchers can perform the same fragment assembly in the web app using Fabric.js, with full manipulation parity to desktop
**Depends on**: Phase 47, Phase 48 (validated interaction model)
**Requirements**: CANV-07, CANV-08, PLAT-01
**Success Criteria** (what must be TRUE):
  1. The web puzzle canvas provides the same drag, rotate, flip, and resize manipulation as the desktop version
  2. User can navigate folios (next/prev image) within a fragment's shelfmark on the canvas
  3. Snap guides appear when dragging a fragment near the edge or center of another fragment
  4. All IIIF images load correctly through the server proxy without CORS errors
  5. The web puzzle is accessible from the app's main navigation
**Plans**: 2 plans
Plans:
- [x] 49-01-PLAN.md — API endpoints + Fabric.js canvas page with full manipulation
- [x] 49-02-PLAN.md — Folio navigation, snap guides, entry points + visual checkpoint

### Phase 50: Join Documents
**Goal**: Researchers can save their puzzle arrangements as persistent join documents and export composite images for publication
**Depends on**: Phase 48, Phase 49
**Requirements**: JDOC-01, JDOC-02, JDOC-03, JDOC-04, JDOC-05
**Success Criteria** (what must be TRUE):
  1. User can save the current puzzle arrangement and reload it later with all fragment positions, rotations, scales, and flip states preserved exactly
  2. User can maintain multiple saved join documents and switch between them
  3. User can export a composite PNG image of the assembled join (background-removed fragments composited at full resolution)
  4. User can add and edit metadata on a join document: title, free-text notes, and fragment identifiers
**Plans**: 3 plans
Plans:
- [x] 50-01-PLAN.md — Shared layer: model cleanup, schema v2 migration, composite export service
- [x] 50-02-PLAN.md — Desktop UI: QDockWidget side panel, save/load/export, auto-save
- [x] 50-03-PLAN.md — Web UI: left drawer, API endpoints, save/load/export, auto-save

### Phase 51: Recto/Verso (COMPLETE — pre-built)
**Goal**: Researchers can view both sides of an assembled join, with verso auto-generated from the recto arrangement
**Depends on**: Phase 50
**Requirements**: RVRS-01, RVRS-02
**Status**: Already implemented during Phases 48-49. Recto/verso toggle and folio navigation were built into both desktop and web canvas from the start. No separate plans needed.
**Success Criteria** (what must be TRUE):
  1. ~~User can toggle between recto and verso views of the assembled join~~ Folio navigation in both apps
  2. ~~Verso view is auto-generated by mirroring the recto fragment arrangement (horizontal flip of positions)~~ Flip all + per-fragment flip
  3. ~~Each fragment in verso view displays the correct verso image~~ Folio nav loads correct verso image
**Plans**: None needed (0/0)

### Phase 52: Community + Integration
**Goal**: Researchers can manage a personal puzzle workspace and share join documents with the community; puzzle is accessible from existing browse/search workflows
**Depends on**: Phase 50
**Requirements**: CANV-02, COMM-01, COMM-02, COMM-03
**Success Criteria** (what must be TRUE):
  1. User can add fragments to the puzzle directly from personal lists, browse results, or search results (not just by typing a shelfmark)
  2. Join documents are saved to a personal workspace by default (private, only visible to the creator)
  3. User can publish a join document for community review, making it visible to all users
  4. Published join documents are browsable by other users with fragment identifiers, join type, and notes visible
**Plans**: 3 plans
Plans:
- [x] 52-01-PLAN.md — Shared publish service + Supabase schema + tests
- [x] 52-02-PLAN.md — Web publish UI + Discoveries feed + joins panel community section
- [x] 52-03-PLAN.md — Desktop publish + Discoveries feed + JoinsDialog community section

### Phase 53: Fill Missing Genizah Manuscripts from FIST
**Goal**: All 38,673 Genizah manuscripts in FIST.db that are missing from libraries.csv become browsable in GenizahSearch with images, FJMS enrichment, and metadata search — without requiring Tantivy index changes
**Depends on**: Nothing (standalone data phase)
**Requirements**: [GAP-01, GAP-02, GAP-03, GAP-04, GAP-05, GAP-06, GAP-07]
**Success Criteria** (what must be TRUE):
  1. (GAP-01) libraries.csv grows from 216,942 to ~255,615 records with correctly formatted rows for all FIST-only manuscripts
  2. (GAP-02) New records appear in catalog browse with correct library codes, domain facets, and FJMS enrichment
  3. (GAP-03) NLI images load correctly for new records (100% have images in nli_crossref.db)
  4. (GAP-04) Shelfmark/title metadata search returns new records (execute_search metadata guard adjusted)
  5. (GAP-05) Text search (Responsa, word search) correctly excludes metadata-only records (no transcription text)
  6. (GAP-06) 12 new library codes registered in LIBRARY_CODES for small FIST-only collections
  7. (GAP-07) Shelfmark normalization handles Yevr->EVR and Halper->Genizah aliases
**Plans**: 2 plans
Plans:
- [x] 53-01-PLAN.md -- CSV generation from FIST.db + library codes + shelfmark normalization
- [x] 53-02-PLAN.md -- Metadata search guard fix + validation tests + visual verification

### Phase 54: Dimensions Display & Filtering
**Goal**: Researchers can see manuscript physical dimensions in browse and search results, and filter searches by size range to find fragments of matching dimensions
**Depends on**: Nothing (first phase of v7.3 milestone)
**Requirements**: DIM-01, DIM-02, DIM-03, DIM-04
**Success Criteria** (what must be TRUE):
  1. User can see manuscript dimensions (e.g., "15.2 x 22.1 cm") in the browse page detail panel and in search result cards for manuscripts that have size data in catalog_sizes (105K of 255K manuscripts)
  2. User can set min/max width and height values in the pre-search filter panel, and search results are restricted to manuscripts within that size range (both apps)
  3. User can filter within existing search results by dimension range as a post-search refinement, narrowing displayed results without re-running the search (both apps)
  4. Dimensions are displayed consistently in centimeters regardless of the source unit (mm/cm/inch), with the normalization handled in a shared service method
  5. Dimension filter slider/input bounds are clamped to sensible ranges (P5/P95 percentiles) so that outlier values (0.7mm, 7230mm) do not make the controls unusable
**Plans**: TBD
**UI hint**: yes

### Phase 55: Search Within Results
**Goal**: Researchers can progressively refine their search by running a second query restricted to the manuscripts from their current result set
**Depends on**: Phase 54 (dimensions visible when refining enriches the combined experience; not a hard dependency)
**Requirements**: SRCH-01, SRCH-02, SRCH-03
**Success Criteria** (what must be TRUE):
  1. User can click "Search within these N results" in the search results header, enter a new query, and see results restricted to only the sys_ids from the previous result set (both apps)
  2. A breadcrumb or chip displays the refinement chain showing the original query, making the active scope visible at all times (both apps)
  3. User can click a clear button on the breadcrumb to return to unrestricted search, removing the restrict set in one action (both apps)
  4. Search-within correctly intersects with any active pre-search filters (domain, dimensions, etc.) -- the restrict set narrows further, never replaces existing filters
**Plans**: TBD
**UI hint**: yes

### Phase 56: Exclude Known Manuscripts
**Goal**: Researchers can hide manuscripts they have already reviewed from search results, using either a saved cloud list or an imported shelfmark file
**Depends on**: Phase 55 (reuses breadcrumb/chip UX patterns for exclusion display; not a hard dependency)
**Requirements**: EXCL-01, EXCL-02, EXCL-03, EXCL-04
**Success Criteria** (what must be TRUE):
  1. User can select a saved Supabase list from a picker dialog and all manuscripts in that list are hidden from subsequent search results (both apps)
  2. User can import a text or CSV file of shelfmarks, and after resolution a report shows how many resolved vs. failed (e.g., "Resolved 80/100 shelfmarks. 20 not found: [list]") (both apps)
  3. Shelfmark resolution handles variant conventions (CUL T-S vs T-S, full library names, Yevr/EVR aliases) using the existing normalize_shelfmark pipeline (both apps)
  4. Search results show the exclusion count (e.g., "3 excluded") with a breakdown by source (list name vs. imported file) and per-source clear buttons (both apps)
  5. Exclusion state persists within the session (web: SearchUIState; desktop: session state JSON) so that switching between searches does not lose the active exclude set
**Plans**: TBD
**UI hint**: yes

### Phase 57: FIST Joins Browse & Search Mode
**Goal**: Researchers can discover scholarly join relationships while browsing and can restrict searches to only manuscripts with known FIST joins, seeing join partners alongside their search results
**Depends on**: Phase 54 (dimensions can appear for join partners); Phase 56 (exclude can filter join group members)
**Requirements**: JOIN-01, JOIN-02, JOIN-03
**Success Criteria** (what must be TRUE):
  1. User can see FIST join group partners in the browse enrichment panel with clickable shelfmarks that navigate to the partner manuscript's browse view (both apps)
  2. User can enable a "Has joins" toggle in the search filter panel that restricts results to the ~20K manuscripts with FIST join records, and the toggle shows the available manuscript count before searching (both apps)
  3. Search results for manuscripts with joins display an expandable join partners section showing up to 10 partners inline with "and N more..." for larger groups, including join type and scholar attribution (both apps)
  4. Join partner enrichment uses batch lookup (single SQL query for all result sys_ids) rather than per-result queries, keeping search performance comparable to unfiltered search
**Plans**: TBD

## Progress

**Total milestones shipped:** 12 (through v7.1.0)
**Total phases completed:** 53 (Phases 1-53)
**Total plans completed:** ~175

| Phase | Plans Complete | Status | Completed |
|-------|----------------|--------|-----------|
| 47. Foundation + Background Removal | 4/4 | Complete    | 2026-03-16 |
| 48. Desktop Canvas | 3/3 | Complete    | 2026-03-16 |
| 49. Web Canvas | 2/2 | Complete    | 2026-03-16 |
| 50. Join Documents | 3/3 | Complete    | 2026-03-16 |
| 51. Recto/Verso | 0/0 | Complete (pre-built) | 2026-03-17 |
| 52. Community + Integration | 3/3 | Complete   | 2026-03-17 |
| 53. Fill Missing Genizah MSS from FIST | 2/2 | Complete    | 2026-03-19 |
| 54. Dimensions Display & Filtering | 0/? | Not started | - |
| 55. Search Within Results | 0/? | Not started | - |
| 56. Exclude Known Manuscripts | 0/? | Not started | - |
| 57. FIST Joins Browse & Search Mode | 0/? | Not started | - |

---
*Roadmap created: 2026-02-09*
*Last updated: 2026-03-26 after v7.3 roadmap creation (Phases 54-57)*
