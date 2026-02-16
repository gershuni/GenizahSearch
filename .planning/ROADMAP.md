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
- [x] **Phase 31: Image Navigation & Indicators** - Page-level image navigation and source availability indicators (completed 2026-02-15)
- [x] **Phase 32: Metadata Display** - Physical metadata and catalog links on browse page (completed 2026-02-16)
- [x] **Phase 33: Metadata Enrichment** - FIST bibliography/catalog export + NLI/FJMS metadata display in both apps (completed 2026-02-16)
- [x] **Phase 34: Library IIIF Fallback** - JTS, Manchester, and British Library IIIF as alternative image sources (completed 2026-02-16)

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
  - [x] 31-03-PLAN.md -- Gap closure: web source switching between NLI and Cambridge images

### Phase 32: Metadata Display
**Goal**: Users see physical manuscript metadata and can navigate to external catalog pages for the manuscripts they are viewing
**Depends on**: Phase 29
**Requirements**: META-01, META-02, META-03, META-04
**Success Criteria** (what must be TRUE):
  1. When viewing a manuscript with NLI crossref data, the browse page shows material type (paper/parchment) and folio count (from NumFolio/NumBifolio)
  2. A clickable NLI catalog link (constructed from NLI_AlmaId) on the browse page opens the NLI KTIV viewer for the manuscript
  3. A clickable link to the manuscript's holding library digital collection (CUDL, Princeton DPUL, Manchester LUNA, BL viewer) appears on the browse page
  4. All metadata and links display in both web and desktop apps
**Plans:** 3/3 plans complete
  - [x] 32-01-PLAN.md -- Service layer additions (library viewer URL, physical metadata enrichment) + web browse metadata display
  - [x] 32-02-PLAN.md -- Desktop browse extended info panel with physical metadata and library links
  - [ ] 32-03-PLAN.md -- Gap closure: fix broken library URL patterns for Manchester, BL, JTS

### Phase 33: Metadata Enrichment
**Goal**: Users see comprehensive scholarly metadata -- FIST bibliography (733K references with scholar attribution), catalog cross-references (78K entries across 80 published catalogs), NLI collection/storage references, Neubauer-Cowley catalog numbers, IsNotGenizah flags, and FJMS source classifications -- on the browse page in both apps
**Depends on**: Phase 29
**Requirements**: REL-01 (repurposed), META-05 (new)
**Success Criteria** (what must be TRUE):
  1. FIST bibliography data (733K rows) is exported from FIST.db, imported into fjms_enrichment.db sidecar, and displayed as a bibliography section on the browse page showing publication, page reference, mention type, and transcription/translation availability
  2. FIST catalog cross-references (78K rows across 80 scholarly catalogs) are exported and displayed as structured catalog entry references (e.g., "Baker/Polliack #1234")
  3. NLI Neubauer-Cowley catalog numbers (2,919 Oxford entries) appear in the metadata section
  4. NLI IsNotGenizah flag shows as a visual badge for the 29,081 flagged items in our corpus
  5. FJMS SourceName, NLI CollectionName, and physical storage references (OBBox/Volume/Folio) are displayed as secondary metadata
  6. All metadata displays in both web and desktop apps
**Plans:** 4/4 plans complete
Plans:
- [ ] 33-01-PLAN.md -- FIST bibliography, catalog cross-refs, and reference tables export to sidecar
- [ ] 33-02-PLAN.md -- Service layer methods + enrich_metadata wiring + tests
- [ ] 33-03-PLAN.md -- Web browse page metadata display (bibliography, catalog refs, badges)
- [ ] 33-04-PLAN.md -- Desktop browse extended info metadata display

### Phase 34: Library IIIF Integration (Manchester + JTS/Princeton)
**Goal**: Users see high-res images and rich metadata from Manchester LUNA and JTS/Princeton Figgy directly in the app, with detail page links instead of search links, by pre-importing library-specific identifiers into the sidecar
**Depends on**: Phase 29, Phase 30
**Requirements**: IMG-05
**Success Criteria** (what must be TRUE):
  1. Manchester LUNA internal IDs (~29K images) are pre-imported into sidecar by paginating the LUNA fetchMediaSearch API using ImageSourceName from crossref
  2. JTS/Princeton ARK IDs and Figgy manifest URLs (~43K manuscripts) are pre-imported into sidecar by searching DPUL catalog API per shelfmark
  3. Manchester library links open the LUNA detail page (not search) showing rich metadata and high-res viewer
  4. JTS library links open the DPUL catalog page (not search) with embedded IIIF viewer
  5. Manchester IIIF manifests (from LUNA) available as image source in both apps' viewers alongside NLI
  6. JTS/Princeton IIIF manifests (from Figgy) available as image source in both apps' viewers alongside NLI
  7. BL links remain as searcharchives.bl.uk search (BL IIIF API still down from cyber attack -- revisit when recovered)
**Plans:** 5/5 plans complete
Plans:
- [ ] 34-01-PLAN.md -- Manchester LUNA bulk import script + sidecar table
- [ ] 34-02-PLAN.md -- JTS/Princeton DPUL import script + sidecar table
- [ ] 34-03-PLAN.md -- Service layer + enrich_metadata + library URLs + tests
- [ ] 34-04-PLAN.md -- Web proxy endpoints + browse source chips
- [ ] 34-05-PLAN.md -- Desktop ManuscriptViewer source switching

**API Discovery (confirmed live 2026-02-16):**
- Manchester LUNA: `luna.manchester.ac.uk/luna/servlet/as/fetchMediaSearch?fullData=false&q={ImageSourceName}&lc=ManchesterDev~95~2` → returns `identity` field = LUNA ID
- Manchester IIIF: `luna.manchester.ac.uk/luna/servlet/iiif/m/{luna_id}/manifest` → standard IIIF v2
- Manchester detail: `luna.manchester.ac.uk/luna/servlet/detail/{luna_id}`
- JTS/Princeton DPUL: `dpul.princeton.edu/cairo_geniza/catalog.json?q={shelfmark}` → returns ARK ID
- JTS/Princeton item: `dpul.princeton.edu/cairo_geniza/catalog/{ark_id}.json` → contains `content_metadata_iiif_manifest_field_ssi` = Figgy manifest URL
- JTS/Princeton IIIF: `figgy.princeton.edu/concern/scanned_resources/{uuid}/manifest` → standard IIIF v2, CC0

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
| 32. Metadata Display | v5.9.0 | Complete    | 2026-02-16 | - |
| 33. Metadata Enrichment | v5.9.0 | Complete    | 2026-02-16 | - |
| 34. Library IIIF Fallback | v5.9.0 | Complete    | 2026-02-16 | - |

---
*Roadmap created: 2026-02-09*
*Last updated: 2026-02-16 after Phase 33 planning complete*
