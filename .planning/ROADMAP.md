# Roadmap: GenizahSearch

## Milestones

- ✅ **v1 External Data Integration** -- Phases 1-7 (shipped 2026-02-07)
- ✅ **v5.6.0 Desktop Parity & PGP Integration** -- Phases 8-12 (shipped 2026-02-09)
- ✅ **v5.7.0 Responsa Search** -- Phases 14-17 (shipped 2026-02-10)
- ✅ **v5.7.2 Cleanup, Normalization & Sections** -- Phases 18-21 (shipped 2026-02-11)
- ✅ **v5.7.3 Pending Corrections Visibility** -- Phases 22-24 (shipped 2026-02-11)
- ✅ **v5.8.0 FJMS Integration** -- Phases 25-28 (shipped 2026-02-15)
- ✅ **v5.9.0 Multi-Source Image & Metadata Integration** -- Phases 29-34 (shipped 2026-02-16)
- ✅ **v6.0.0 Local Data Architecture** -- Phases 35-40 (shipped 2026-02-22)
- **v7.0.0 Catalog Navigation & Transcription Search** -- Phases 41-46 (in progress)

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

### v7.0.0 Catalog Navigation & Transcription Search (In Progress)

**Milestone Goal:** Enable researchers to browse and search the scholarly catalog (authors, works, domains) and search within transcription text alongside OCR, with pre-search domain scoping.

- [ ] **Phase 41: Catalog Browse & Navigation** - Faceted browsing of manuscripts by domain, author, and work hierarchies in both apps
- [ ] **Phase 42: Catalog Search** - Full-text and structured field search across FJMS and PGP catalog data as a search mode
- [ ] **Phase 43: Pre-Search Domain Filtering** - Include/exclude domains, identifications, and authors before Tantivy search across all modes
- [ ] **Phase 44: Transcription Import** - FJMS ~30K transcriptions from FIST.db into fjms_enrichment.db with shared service access
- [ ] **Phase 45: Transcription Indexing & Search** - Unified Tantivy index over PGP + FJMS + user transcription text with source badges and ranking
- [ ] **Phase 46: Index Distribution & Upgrade** - Pre-built index download, desktop upgrade path, and fresh install support

## Phase Details

### Phase 41: Catalog Browse & Navigation
**Goal**: Researchers can explore the manuscript corpus through structured scholarly categories -- browsing by domain hierarchy, author, or work title -- and combine these axes to narrow results
**Depends on**: Nothing (builds on existing fjms_enrichment.db catalog data and FjmsService)
**Requirements**: BROWSE-01, BROWSE-02, BROWSE-03, BROWSE-04, BROWSE-05, BROWSE-06
**Success Criteria** (what must be TRUE):
  1. User can navigate a domain hierarchy (e.g., Bible > Torah > Genesis) and see manuscripts classified under each level
  2. User can browse manuscripts by author name and see all works attributed to that author
  3. User can browse manuscripts by work/title and see all fragments containing that work
  4. User can combine axes (e.g., domain + author) to narrow browse results, with results showing shelfmark, library, domain, and identification
  5. Catalog browse is fully functional in both web (NiceGUI) and desktop (PyQt6) apps
**Plans:** 2/4 plans executed

Plans:
- [ ] 41-01-PLAN.md -- Service layer: browse methods for FjmsService (authors, works, results, combined filtering)
- [ ] 41-02-PLAN.md -- Web catalog browse page with domain tree, filtering, pagination, deep linking
- [ ] 41-03-PLAN.md -- Desktop catalog browse tab with matching functionality
- [ ] 41-04-PLAN.md -- Cross-links between browse pages + visual verification

### Phase 42: Catalog Search
**Goal**: Researchers can search across catalog metadata (titles, authors, dates, descriptions, identifications) using free-text or field-specific queries, and navigate from results to manuscript browse view
**Depends on**: Phase 41 (browse view must exist for result navigation)
**Requirements**: CAT-01, CAT-02, CAT-03, CAT-04, CAT-05
**Success Criteria** (what must be TRUE):
  1. User can type a free-text query and get results matching across all catalog fields (titles, authors, dates, descriptions, identifications)
  2. User can filter catalog search by specific fields (author name, work title, date range) for precision queries
  3. Catalog search is accessible as a mode/option within the main search interface (not a separate page)
  4. Clicking a catalog search result navigates to the manuscript browse view for that record
  5. Catalog search works in both web and desktop apps
**Plans**: TBD

### Phase 43: Pre-Search Domain Filtering
**Goal**: Researchers can scope their text searches by domain, identification, or author before executing, so only manuscripts matching those scholarly categories are searched
**Depends on**: Phase 41 (catalog data access patterns established)
**Requirements**: FILT-01, FILT-02, FILT-03, FILT-04, FILT-05
**Success Criteria** (what must be TRUE):
  1. User can include or exclude FJMS domain classifications before running a text search, and only manuscripts matching the filter appear in results
  2. User can include or exclude catalog identifications (work/title) and author attributions before running a text search
  3. Pre-search filters work correctly with all search modes: regular search, Responsa, PGP tags, and parallels
  4. Pre-search filtering is available and functional in both web and desktop apps
**Plans**: TBD

### Phase 44: Transcription Import
**Goal**: FJMS transcription text (~30K transcriptions) is imported from FIST.db into fjms_enrichment.db and accessible through the shared service layer for both apps
**Depends on**: Nothing (data pipeline, independent of catalog UI phases)
**Requirements**: TRANS-01, TRANS-02
**Success Criteria** (what must be TRUE):
  1. FJMS transcriptions (~30K) are stored in fjms_enrichment.db with proper schema (shelfmark linkage, scholar attribution, text content)
  2. Both web and desktop apps can retrieve transcription text for a given manuscript through the shared FjmsService
**Plans**: TBD

### Phase 45: Transcription Indexing & Search
**Goal**: Researchers searching for text find matches in human transcriptions (PGP, FJMS, user corrections) alongside OCR results, with transcription hits clearly identified and ranked higher
**Depends on**: Phase 44 (FJMS transcriptions must be imported before indexing)
**Requirements**: SRCH-01, SRCH-02, SRCH-03, SRCH-04, SRCH-05, SRCH-06, SRCH-07
**Success Criteria** (what must be TRUE):
  1. Text searches return matches found in PGP editions/translations, FJMS transcription text, and user corrections -- not just OCR text
  2. Search results display a badge/icon indicating the source of the match (OCR vs. transcription) so users know the provenance
  3. Transcription-sourced hits are ranked higher than OCR-only hits in the results list
  4. Transcription search works with all search modes (regular and Responsa) in both web and desktop apps
**Plans**: TBD

### Phase 46: Index Distribution & Upgrade
**Goal**: Desktop users (both new and existing) receive a complete Tantivy index that includes transcription fields, with automatic detection and upgrade for users with older index formats
**Depends on**: Phase 45 (index must include transcription fields before distribution)
**Requirements**: DIST-01, DIST-02, DIST-03, DIST-04
**Success Criteria** (what must be TRUE):
  1. A pre-built Tantivy index (with transcription fields) is available for download from the web server
  2. Desktop app detects when the existing local index lacks transcription fields and offers an upgrade path
  3. Fresh desktop installs receive the complete index with transcription data included
  4. Existing users with older index formats can upgrade without losing their index or needing a full reinstall
**Plans**: TBD

## Progress

**Execution Order:**
Phases execute in numeric order: 41 -> 42 -> 43 -> 44 -> 45 -> 46

Note: Phases 41-43 (catalog) and Phase 44 (import) are independent chains. Phase 44 can execute in parallel with 41-43 if desired, but Phase 45 requires Phase 44 complete.

| Phase | Plans Complete | Status | Completed |
|-------|----------------|--------|-----------|
| 41. Catalog Browse & Navigation | 2/4 | In Progress|  |
| 42. Catalog Search | 0/TBD | Not started | - |
| 43. Pre-Search Domain Filtering | 0/TBD | Not started | - |
| 44. Transcription Import | 0/TBD | Not started | - |
| 45. Transcription Indexing & Search | 0/TBD | Not started | - |
| 46. Index Distribution & Upgrade | 0/TBD | Not started | - |

**Total phases completed:** 40 (from previous milestones)
**Total plans completed:** ~115 (from previous milestones)
**Total milestones shipped:** 8

---
*Roadmap created: 2026-02-09*
*Last updated: 2026-02-26 after Phase 41 planning complete*
