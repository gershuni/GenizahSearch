# Roadmap: GenizahSearch

## Milestones

- ✅ **v1 External Data Integration** -- Phases 1-7 (shipped 2026-02-07)
- ✅ **v5.6.0 Desktop Parity & PGP Integration** -- Phases 8-12 (shipped 2026-02-09)
- ✅ **v5.7.0 Responsa Search** -- Phases 14-17 (shipped 2026-02-10)
- ✅ **v5.7.2 Cleanup, Normalization & Sections** -- Phases 18-21 (shipped 2026-02-11)
- ✅ **v5.7.3 Pending Corrections Visibility** -- Phases 22-24 (shipped 2026-02-11)
- ✅ **v5.8.0 FJMS Integration** -- Phases 25-28 (shipped 2026-02-15)

## Phases

<details>
<summary>✅ v1 External Data Integration (Phases 1-7) -- SHIPPED 2026-02-07</summary>

See: .planning/milestones/v1-ROADMAP.md

9 phases (including inserted 7.1, 7.2), 18 plans, 173 min total execution.
Imported 7,090 PGP documents with 9,364 transcription/translation sources.
Full PGP feature set in web app.

</details>

<details>
<summary>✅ v5.6.0 Desktop Parity & PGP Integration (Phases 8-12) -- SHIPPED 2026-02-09</summary>

See: .planning/milestones/v5.6.0-ROADMAP.md

5 phases, 25 plans, ~134 min total execution.
Desktop PGP feature parity, Virtual Reading Desk, 35,839 PGP documents imported.
Phase 13 (Transcription Search) deferred -- index build too slow for desktop.

</details>

<details>
<summary>✅ v5.7.0 Responsa Search (Phases 14-17) -- SHIPPED 2026-02-10</summary>

See: .planning/milestones/v5.7.0-ROADMAP.md

4 phases, 14 plans.
Responsa Project-style advanced search with syntax parsing, JA expansion, tabular query builder, explosion guards.
25/25 requirements satisfied. 221 automated Responsa tests.

</details>

<details>
<summary>✅ v5.7.2 Cleanup, Normalization & Sections (Phases 18-21) -- SHIPPED 2026-02-11</summary>

See: .planning/milestones/v5.7.2-ROADMAP.md

4 phases, 11 plans.
Dead AI code removed, Unicode search normalization, full green test suite (447 tests),
structural HTML section parser for PGP transcriptions.
13/13 requirements satisfied.

</details>

<details>
<summary>✅ v5.7.3 Pending Corrections Visibility (Phases 22-24) -- SHIPPED 2026-02-11</summary>

See: .planning/milestones/v5.7.3-ROADMAP.md

3 phases, 3 plans.
Pending corrections visible as selectable version in web and desktop version selectors.
Shared corrections service, amber styling (web), emoji labels (desktop).
6/6 requirements satisfied. 20 milestone-specific tests.

</details>

<details>
<summary>✅ v5.8.0 FJMS Integration (Phases 25-28) -- SHIPPED 2026-02-15</summary>

See: .planning/milestones/v5.8.0-ROADMAP.md

4 phases, 12 plans.
FJMS scholarly metadata (domains, joins, catalog) integrated via SQLite sidecar.
Domain filtering, scientific joins with scholar attribution, catalog enrichment in both apps.
19/19 requirements satisfied. 38+ tests covering service layer and integration.

</details>

### v5.9.0 Multi-Source Image & Metadata Integration (In Progress)

**Milestone Goal:** Import NLI crossreference data (815K image-level records) and Cambridge IIIF manifests (141K URLs) into the SQLite sidecar, enabling direct image access for 75+ libraries without runtime manifest fetching, physical metadata display, fragment relationship surfacing, and library-specific viewer links in both apps.

- [x] **Phase 29: Data Infrastructure** - Import NLI crossref and Cambridge IIIF into sidecar with shared service layer (completed 2026-02-15)
- [x] **Phase 30: Direct Image Access** - Bypass NLI manifest fetch using pre-resolved image URLs from crossref and CUDL (completed 2026-02-15)
- [x] **Phase 31: Image Navigation & Indicators** - Page-level image navigation and source availability indicators (completed 2026-02-15)
- [ ] **Phase 32: Metadata Display** - Physical metadata and catalog links on browse page
- [ ] **Phase 33: Fragment Relationships** - NLI PartOf and See cross-references in Related Fragments panel
- [ ] **Phase 34: Library IIIF Fallback** - JTS, Manchester, and British Library IIIF as alternative image sources

## Phase Details

### Phase 29: Data Infrastructure
**Goal**: Both apps can access NLI crossreference data and Cambridge IIIF manifest URLs through a shared service backed by the SQLite sidecar database
**Depends on**: Nothing (first phase of milestone)
**Requirements**: DATA-01, DATA-02, DATA-03
**Success Criteria** (what must be TRUE):
  1. Running the import script produces new tables in the sidecar with 815K NLI crossref records joined by NLI_AlmaId to libraries.csv system_number
  2. Cambridge IIIF manifest URLs (141K records) are stored in the sidecar with normalized shelfmarks that match libraries.csv call_numbers
  3. A shared NliCrossrefService provides image lookup, metadata queries, and relationship queries callable from both web and desktop apps
  4. The service is thread-safe for NiceGUI concurrent access and gracefully returns empty results when sidecar is missing
**Plans:** 2 plans
  - [x] 29-01-PLAN.md -- Import NLI crossref CSV and Cambridge IIIF JSON into SQLite sidecar
  - [x] 29-02-PLAN.md -- NliCrossrefService shared service layer, web shim, and unit tests

### Phase 30: Direct Image Access
**Goal**: Users see manuscript images load faster because image URLs are resolved locally instead of fetching NLI IIIF manifests at runtime
**Depends on**: Phase 29
**Requirements**: IMG-01, IMG-02
**Success Criteria** (what must be TRUE):
  1. When viewing any manuscript with NLI crossref coverage (766K records across 75 libraries), images load using FGPImageNumberId-constructed URLs without making an NLI manifest API call
  2. When viewing a Cambridge manuscript, images load via locally stored CUDL IIIF manifest URLs without contacting NLI
  3. Image loading is observably faster than the current NLI manifest fetch path (no network round-trip for URL resolution)
**Plans:** 2 plans
  - [x] 30-01-PLAN.md -- Web API local image resolution (FL IDs from crossref sidecar)
  - [x] 30-02-PLAN.md -- Desktop enrich_metadata local image resolution + tests

### Phase 31: Image Navigation & Indicators
**Goal**: Users can navigate between individual pages/folios of a manuscript and see at a glance which digital image sources are available
**Depends on**: Phase 30
**Requirements**: IMG-03, IMG-04
**Success Criteria** (what must be TRUE):
  1. On the browse page, an indicator shows which digital image sources exist for the current manuscript (e.g., NLI/FGP, Cambridge CUDL, library-specific)
  2. The image availability indicator appears in both web and desktop apps
  3. Users can navigate between individual pages (recto/verso, folios) using the crossref ImageName ordering to step through leaf/folio/side sequences
**Plans:** 3/3 plans complete
  - [x] 31-01-PLAN.md -- Folio label parsing in service layer + web browse page folio navigation and source indicators
  - [x] 31-02-PLAN.md -- Desktop browse tab folio navigation and source indicators
  - [ ] 31-03-PLAN.md -- Gap closure: web source switching between NLI and Cambridge images

### Phase 32: Metadata Display
**Goal**: Users see physical manuscript metadata and can navigate to external catalog pages for the manuscripts they are viewing
**Depends on**: Phase 29
**Requirements**: META-01, META-02, META-03, META-04
**Success Criteria** (what must be TRUE):
  1. When viewing a manuscript with NLI crossref data, the browse page shows material type (paper/parchment) and folio count (from NumFolio/NumBifolio)
  2. A clickable NLI catalog link (constructed from NLI_AlmaId) on the browse page opens the NLI KTIV viewer for the manuscript
  3. A clickable link to the manuscript's holding library digital collection (CUDL, Princeton DPUL, Manchester LUNA, BL viewer) appears on the browse page
  4. All metadata and links display in both web and desktop apps
**Plans**: TBD

### Phase 33: Fragment Relationships
**Goal**: Users discover related manuscripts through NLI PartOf groupings and See cross-references alongside existing PGP and FJMS joins
**Depends on**: Phase 29
**Requirements**: REL-01, REL-02
**Success Criteria** (what must be TRUE):
  1. When viewing a manuscript that has NLI PartOf relationships (from 424K records), related fragments appear in the Related Fragments panel alongside PGP and FJMS joins
  2. When viewing a manuscript that has NLI See cross-references (from 19K records), those references appear in the Related Fragments panel
  3. NLI relationship entries are visually distinguished from PGP and FJMS sources (e.g., badge or label)
  4. Fragment relationships display in both web and desktop apps
**Plans**: TBD

### Phase 34: Library IIIF Fallback
**Goal**: Users can view images for manuscripts from JTS, Manchester, and British Library even when NLI/FGP images are unavailable, by falling back to library-specific IIIF endpoints
**Depends on**: Phase 30
**Requirements**: IMG-05
**Success Criteria** (what must be TRUE):
  1. When a JTS manuscript has no FGP image, the system attempts to load images from Princeton Figgy IIIF
  2. When a Manchester manuscript has no FGP image, the system attempts to load images from Manchester LUNA/MDC IIIF
  3. When a British Library manuscript has no FGP image, the system attempts to load images from the BL IIIF viewer
  4. Fallback is transparent to the user -- images appear in the same viewer regardless of source
**Plans**: TBD

## Progress

**Execution Order:**
Phases execute in numeric order: 29 -> 30 -> 31 -> 32 -> 33 -> 34
(Phases 30, 32, 33 all depend on 29. Phase 31 depends on 30. Phase 34 depends on 30.)
Priority sequence: Data Infrastructure -> Image Access -> Navigation -> Metadata -> Relationships -> Fallback

| Phase | Milestone | Plans Complete | Status | Completed |
|-------|-----------|----------------|--------|-----------|
| 1-7 | v1 | 18/18 | Complete | 2026-02-07 |
| 8-12 | v5.6.0 | 25/25 | Complete | 2026-02-09 |
| 14-17 | v5.7.0 | 14/14 | Complete | 2026-02-10 |
| 18-21 | v5.7.2 | 11/11 | Complete | 2026-02-11 |
| 22-24 | v5.7.3 | 3/3 | Complete | 2026-02-11 |
| 25-28 | v5.8.0 | 12/12 | Complete | 2026-02-15 |
| 29. Data Infrastructure | v5.9.0 | 2/2 | Complete | 2026-02-15 |
| 30. Direct Image Access | v5.9.0 | 2/2 | Complete | 2026-02-15 |
| 31. Image Nav & Indicators | v5.9.0 | 3/3 | Complete | 2026-02-15 |
| 32. Metadata Display | v5.9.0 | 0/TBD | Not started | - |
| 33. Fragment Relationships | v5.9.0 | 0/TBD | Not started | - |
| 34. Library IIIF Fallback | v5.9.0 | 0/TBD | Not started | - |

---
*Roadmap created: 2026-02-09*
*Last updated: 2026-02-15 after Phase 31 complete*
