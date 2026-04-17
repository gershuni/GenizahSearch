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
- **v7.9 Decomposition** -- Phases 67-76 (in progress)

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

### v7.9 Decomposition (In Progress)

**Milestone Goal:** Reduce structural debt by decomposing the largest source files -- `genizah_app.py` (~32,800 lines), `web/pages/search.py` (~6,700 lines), `web/pages/browse.py` (~5,100 lines) -- into focused modules. Zero user-visible behavior changes. Leverages v7.8 CI safety net (ruff + pytest on Ubuntu + Windows).
**Per-phase gate:** current pytest baseline remains green, CI green (Ubuntu + Windows) after each phase.
**Milestone-level gate:** `scripts/check_docs.py` green at milestone close (Phase 76).

- [x] **Phase 67: ResultDialog Extraction** - Extract ResultDialog class from genizah_app.py into desktop/result_dialog.py (completed 2026-04-15)
- [x] **Phase 68: Desktop Dialog Extractions** - Extract ExcludeDialog, filter dialogs, FJMS/NLI/bibliography dialogs into dedicated modules (completed 2026-04-16)
- [x] **Phase 69: Image Viewer Extraction** - Extract ManuscriptViewerWidget, FullscreenImageWindow, and image viewer classes into desktop/viewers.py (completed 2026-04-16)
- [x] **Phase 70: Puzzle Extraction** - Extract PuzzleCanvasWindow and puzzle-related classes into desktop/puzzle.py (completed 2026-04-16)
- [x] **Phase 71: GenizahGUI Consolidation & Smoke Tests** - Verify GenizahGUI is a clean orchestrator importing from extracted modules; run desktop smoke-test suite (completed 2026-04-16)
- [x] **Phase 72: Search Page Split** - Split web/pages/search.py into state, UI, and results modules (completed 2026-04-16)
- [ ] **Phase 73: Browse Page Split** - Split web/pages/browse.py into state, UI, and enrichment modules
- [ ] **Phase 74: Page-Scoped State Refactor** - Reduce app.storage.user sprawl and detached asyncio.ensure_future with page-scoped state objects
- [ ] **Phase 75: Non-Regression Verification** - Manual qualitative verification of search/browse responsiveness in both apps
- [ ] **Phase 76: Documentation Close** - Refresh CODE_INDEX.md, OPEN_ISSUES.md, and path references for all moved files

## Phase Details

### Phase 67: ResultDialog Extraction
**Goal**: ResultDialog and its helper classes live in their own module, imported by genizah_app.py
**Depends on**: Nothing (first phase of milestone; CI safety net from v7.8 is prerequisite)
**Requirements**: DESK-01
**Success Criteria** (what must be TRUE):
  1. `ResultDialog` class is defined in a new `desktop/result_dialog.py` module (not in `genizah_app.py`)
  2. `genizah_app.py` imports `ResultDialog` from the new module and all existing call sites work unchanged
  3. Any helper classes/functions used exclusively by ResultDialog move with it; shared helpers remain accessible
  4. current pytest baseline remains green
**Phase gate**: pytest green, CI green
**Plans**: 3 plans
Plans:
- [x] 67-01-PLAN.md -- Create desktop/ package, move ActionsHoverWidget + _format_add_to_list_label to widgets.py
- [x] 67-02-PLAN.md -- Delete browse dead code, move helpers to cohesive modules, extract ResultDialog (additive copy then cut-over), update tests
- [x] 67-03-PLAN.md -- Rename self.parent() to self._app + manual desktop smoke test
**Risk**: ResultDialog likely references GenizahGUI methods (e.g., for browse navigation callbacks). These cross-references need careful handling -- callback injection or signal-based decoupling. Discuss during planning.

### Phase 68: Desktop Dialog Extractions
**Goal**: All filter and scholarly dialogs live in dedicated modules, not in genizah_app.py
**Depends on**: Phase 67 (establishes extraction pattern and module layout)
**Requirements**: DESK-04, DESK-05
**Success Criteria** (what must be TRUE):
  1. `ExcludeDialog` and filter dialog classes are defined in a new `desktop/dialogs_filter.py` module (not in `genizah_app.py`)
  2. FJMS catalog dialog, NLI crossref dialog, bibliography dialog, and measurement dialog classes are defined in a new `desktop/dialogs_scholarly.py` module (not in `genizah_app.py`)
  3. `genizah_app.py` imports all dialog classes from their new modules and all existing call sites work unchanged
  4. current pytest baseline remains green
**Phase gate**: pytest green, CI green
**Plans**: 2 plans
Plans:
- [x] 68-01-PLAN.md -- Extract 4 scholarly dialogs to desktop/dialogs_scholarly.py, retarget result_dialog.py lazy imports, add re-exports
- [x] 68-02-PLAN.md -- Move FilterCountWorker to gui_threads.py, extract 3 filter dialogs to desktop/dialogs_filter.py, delete self-imports, add re-exports
**Note**: These are leaf dialogs with minimal cross-dependencies -- grouping them is safe. If any dialog has unexpected coupling to ResultDialog or viewers, split into a separate plan.

### Phase 69: Image Viewer Extraction
**Goal**: All manuscript image viewing classes live in their own module
**Depends on**: Phase 68 (dialog extractions complete, reducing genizah_app.py surface area)
**Requirements**: DESK-03
**Success Criteria** (what must be TRUE):
  1. `ManuscriptViewerWidget`, `FullscreenImageWindow`, and related image viewer classes are defined in a new `desktop/viewers.py` module (not in `genizah_app.py`)
  2. Image-loading helper functions shared between viewers and puzzle are accessible from the new module (or extracted to a shared `desktop/image_utils.py`)
  3. `genizah_app.py` imports viewer classes from the new module and all existing call sites work unchanged
  4. current pytest baseline remains green
**Phase gate**: pytest green, CI green
**Plans**: 1 plan
Plans:
- [x] 69-01-PLAN.md -- Extract 3 image viewer classes to desktop/viewers.py, retarget result_dialog.py lazy import, add re-exports
**Note**: Smaller scope than Phase 68 (3 classes, ~1160 lines, single target module). D-02 confirmed no shared image helpers exist between viewers and puzzle -- no desktop/image_utils.py needed.

### Phase 70: Puzzle Extraction
**Goal**: All puzzle/join canvas classes live in their own module
**Depends on**: Phase 69 (image viewer extraction resolves shared image helper placement)
**Requirements**: DESK-02
**Success Criteria** (what must be TRUE):
  1. `PuzzleCanvasWindow` and all puzzle-related classes (fragment items, toolbar, etc.) are defined in a new `desktop/puzzle.py` module (not in `genizah_app.py`)
  2. Puzzle classes import image helpers from `desktop/viewers.py` or `desktop/image_utils.py` (no circular imports)
  3. `genizah_app.py` imports puzzle classes from the new module and all existing call sites work unchanged
  4. current pytest baseline remains green
**Phase gate**: pytest green, CI green
**Plans**: 1 plan
Plans:
- [x] 70-01-PLAN.md -- Extract 5 puzzle classes to desktop/puzzle.py, add re-exports
**Note**: Largest single extraction (~2642 lines, 5 classes). D-10 confirmed no viewer dependency -- no circular import risk. ShelfmarkCompleter imported lazily from genizah_app.py (D-04).

### Phase 71: GenizahGUI Consolidation & Smoke Tests
**Goal**: genizah_app.py is a clean orchestrator and all desktop extractions pass smoke tests
**Depends on**: Phase 70 (all desktop extractions complete)
**Requirements**: DESK-06, DESK-07
**Success Criteria** (what must be TRUE):
  1. `genizah_app.py` contains `GenizahGUI` as the top-level coordinator, with all dialog/viewer/puzzle implementations imported from `desktop/` modules
  2. All substantial dialog, viewer, and puzzle implementations are extracted to `desktop/` modules; small coordination helpers and thin wrappers may remain alongside `GenizahGUI`
  3. Desktop smoke-test suite passes: app starts, basic search executes, browse navigation changes pages, ResultDialog opens/closes, puzzle window opens and loads a fragment
  4. current pytest baseline remains green
  5. No import cycles between `desktop/` modules (verified by ruff or manual inspection)
**Phase gate**: pytest green, CI green, smoke tests pass
**Plans:** 2/2 plans complete
Plans:
- [x] 71-01-PLAN.md -- Extract DesktopVSCache trio to desktop/vs_cache.py, fix OPEN_ISSUES path
- [x] 71-02-PLAN.md -- Create desktop smoke checklist, user walkthrough verification

### Phase 72: Search Page Split
**Goal**: web/pages/search.py is decomposed into focused modules for state, UI, and results
**Depends on**: Phase 71 (desktop complete; web phases are independent but sequenced after desktop to avoid context switching)
**Requirements**: WEBM-01
**Success Criteria** (what must be TRUE):
  1. Search state management (query parameters, refinement chain, session persistence) lives in a dedicated module (e.g., `web/pages/search_state.py`)
  2. Search results rendering (result cards, pagination, export) lives in a dedicated module (e.g., `web/pages/search_results.py`)
  3. `web/pages/search.py` remains the entry point / page registration but delegates to the split modules
  4. current pytest baseline remains green
**Phase gate**: pytest green, CI green
**UI hint**: yes
**Plans:** 2/2 plans complete
Plans:
- [x] 72-01-PLAN.md -- Extract SearchUIState, AdvancedViewState, SearchPageRefs, search history helpers to search_state.py
- [x] 72-02-PLAN.md -- Extract toggle_expansion, render_results, create_result_card, open_advanced_dialog to search_results.py + web smoke test

### Phase 73: Browse Page Split
**Goal**: web/pages/browse.py is decomposed into focused modules for state, UI, and enrichment
**Depends on**: Phase 72 (establishes web split pattern)
**Requirements**: WEBM-02
**Success Criteria** (what must be TRUE):
  1. Browse state management (current manuscript, volume, navigation history) lives in a dedicated module (e.g., `web/pages/browse_state.py`)
  2. Browse enrichment logic (Phase A/B deferred loading, crossref, Oxford, Cambridge) lives in a dedicated module (e.g., `web/pages/browse_enrichment.py`)
  3. `web/pages/browse.py` remains the entry point / page registration but delegates to the split modules
  4. current pytest baseline remains green
**Phase gate**: pytest green, CI green
**UI hint**: yes
**Plans:** 2 plans
Plans:
- [x] 73-01-PLAN.md -- Extract BrowseState, BrowsePageRefs, _crossref_cache to browse_state.py
- [ ] 73-02-PLAN.md -- Extract enrichment functions to browse_enrichment.py + web smoke test

### Phase 74: Page-Scoped State Refactor
**Goal**: Search and browse pages use page-scoped state objects instead of app.storage.user sprawl and detached async flows
**Depends on**: Phase 73 (split modules provide clean boundaries for state refactoring)
**Requirements**: WEBM-03
**Success Criteria** (what must be TRUE):
  1. Search page state is managed through a page-scoped object rather than scattered `app.storage.user` keys for live page state (session persistence keys may remain in app.storage.user)
  2. Browse page state is managed through a page-scoped object rather than scattered `app.storage.user` keys for live page state
  3. Detached `asyncio.ensure_future` calls in search and browse are replaced with page-scoped handlers or NiceGUI background_tasks where practical (some may remain with justification)
  4. current pytest baseline remains green
  5. Web smoke check: app starts; `/` search page loads; a basic search returns results; `/browse` loads for at least one manuscript; shelfmark navigation between manuscripts works
**Phase gate**: pytest green, CI green, web smoke check passes
**UI hint**: yes
**Note**: This is the most architectural web change in the milestone. It touches the runtime behavior of state management, not just file organization. Plan carefully -- the split modules from Phases 72-73 provide natural boundaries, but the state refactoring changes how data flows at runtime. Page-scoped objects should be lightweight wrappers, not a state management framework. The web smoke check catches behavioral regressions earlier than Phase 75's full verification.

### Phase 75: Non-Regression Verification
**Goal**: Both apps behave identically to pre-refactor in all user-facing interactions
**Depends on**: Phase 74 (all code changes complete)
**Requirements**: NREG-01
**Success Criteria** (what must be TRUE):
  1. Web search: initial render, result paging, result interaction (expand, browse, export) show no obvious slowdown versus pre-refactor
  2. Web browse: manuscript loading, enrichment panel population, volume switching show no obvious slowdown versus pre-refactor
  3. Desktop search: basic search, composition search, result dialog interaction show no obvious slowdown versus pre-refactor
  4. Desktop browse: page navigation, image loading, folio switching show no obvious slowdown versus pre-refactor
  5. `pytest tests/` baseline green (1067 passed, 8 skipped)
**Phase gate**: pytest green, qualitative sign-off from user
**Verification method**: Manual checklist only -- no benchmark suite. Executor walks through the web and desktop surfaces listed in criteria 1-4, then the user signs off on each surface (explicit yes/no per surface). No quantitative thresholds; the bar is "no obvious slowdown vs pre-refactor."

### Phase 76: Documentation Close
**Goal**: Project documentation accurately reflects the decomposed codebase
**Depends on**: Phase 75 (all code and verification complete)
**Requirements**: (milestone deliverable, not a numbered requirement)
**Success Criteria** (what must be TRUE):
  1. `docs/CODE_INDEX.md` lists all new `desktop/` modules and updated `web/pages/` module structure with accurate descriptions
  2. `docs/OPEN_ISSUES.md` includes any decomposition findings, deferred cleanup items, or import-cycle concerns discovered during execution
  3. Any docs that reference specific file paths or line numbers in `genizah_app.py`, `web/pages/search.py`, or `web/pages/browse.py` are updated to reflect new locations
  4. `scripts/check_docs.py` passes green
**Phase gate**: check_docs green, CI green

## Progress

**Execution Order:**
Phases execute in numeric order: 67 -> 68 -> 69 -> 70 -> 71 -> 72 -> 73 -> 74 -> 75 -> 76

| Phase | Plans Complete | Status | Completed |
|-------|----------------|--------|-----------|
| 67. ResultDialog Extraction | 3/3 | Complete    | 2026-04-15 |
| 68. Desktop Dialog Extractions | 2/2 | Complete    | 2026-04-16 |
| 69. Image Viewer Extraction | 1/1 | Complete    | 2026-04-16 |
| 70. Puzzle Extraction | 1/1 | Complete    | 2026-04-16 |
| 71. GenizahGUI Consolidation & Smoke Tests | 2/2 | Complete    | 2026-04-16 |
| 72. Search Page Split | 2/2 | Complete    | 2026-04-16 |
| 73. Browse Page Split | 1/2 | In progress | - |
| 74. Page-Scoped State Refactor | 2/3 | In Progress|  |
| 75. Non-Regression Verification | 0/TBD | Not started | - |
| 76. Documentation Close | 0/TBD | Not started | - |

---
*Roadmap created: 2026-02-09*
*Last updated: 2026-04-16 -- Phase 73 planned (2 plans in 2 waves)*
