# Roadmap: GenizahSearch

## Milestones

- ✅ **v1 External Data Integration** -- Phases 1-7 (shipped 2026-02-07)
- ✅ **v5.6.0 Desktop Parity & PGP Integration** -- Phases 8-12 (shipped 2026-02-09)
- ✅ **v5.7.0 Responsa Search** -- Phases 14-17 (shipped 2026-02-10)
- ✅ **v5.7.2 Cleanup, Normalization & Sections** -- Phases 18-21 (shipped 2026-02-11)
- ✅ **v5.7.3 Pending Corrections Visibility** -- Phases 22-24 (shipped 2026-02-11)
- 🚧 **v5.8.0 FJMS Integration** -- Phases 25-28 (in progress)

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

- [x] Phase 22: Pending Corrections Data Layer -- shared service function + tests
- [x] Phase 23: Web Pending Corrections Display -- amber-styled entries in version selector
- [x] Phase 24: Desktop Pending Corrections Display -- verified in Browse tab and Reading Desk

</details>

### 🚧 v5.8.0 FJMS Integration (In Progress)

**Milestone Goal:** Integrate FJMS scholarly metadata (domain classifications, scientific joins, catalog records) into GenizahSearch via a SQLite sidecar database, enabling subject-based filtering and enriched manuscript display in both apps.

- [ ] **Phase 25: Data Infrastructure** - Export FIST.db into SQLite sidecar with shared service layer
- [ ] **Phase 26: Scientific Joins** - FJMS join groups with scholar attribution in both apps
- [ ] **Phase 27: Domain Classifications** - Domain badges and search filtering in both apps
- [ ] **Phase 28: Catalog Enrichment** - FJMS titles, authors, dates on browse page in both apps

## Phase Details

### Phase 25: Data Infrastructure
**Goal**: Both apps can access FJMS enrichment data through a shared service backed by a SQLite sidecar database
**Depends on**: Nothing (first phase of milestone)
**Requirements**: DATA-01, DATA-02, DATA-03, DATA-04, DATA-05
**Success Criteria** (what must be TRUE):
  1. Running the export script against FIST.db produces `fjms_enrichment.db` with populated domains, joins, and catalog tables
  2. The sidecar includes an FTS5 virtual table over catalog descriptions (queryable via SQL, no UI yet)
  3. A meta table exists in the sidecar with a version number that can be checked programmatically
  4. Both web and desktop apps can import and use `FjmsService` to query domain, join, and catalog data for a given sys_id
  5. The web app's SQLite connection is thread-safe and read-only (no write errors under concurrent NiceGUI requests)
**Plans**: TBD

Plans:
- [ ] 25-01: Export script and sidecar schema
- [ ] 25-02: Shared FjmsService class

### Phase 26: Scientific Joins
**Goal**: Users can see scholarly join groups with attribution and navigate between related fragments
**Depends on**: Phase 25
**Requirements**: JOIN-01, JOIN-02, JOIN-03, JOIN-04, JOIN-05
**Success Criteria** (what must be TRUE):
  1. When viewing a manuscript that belongs to an FJMS join group, the user sees the other group members listed in the Related Fragments panel
  2. Each join entry shows who identified the join (scholar name) and the join type (Physical Join, Codex Join, etc.)
  3. The user can click a join group member to navigate to that fragment's browse page
  4. FJMS join information appears in the Related Fragments panel in both web and desktop apps
**Plans**: TBD

Plans:
- [ ] 26-01: Join data integration and UI (both apps)

### Phase 27: Domain Classifications
**Goal**: Users can see what subject a manuscript belongs to and filter search results by domain
**Depends on**: Phase 25
**Requirements**: DOM-01, DOM-02, DOM-03, DOM-04
**Success Criteria** (what must be TRUE):
  1. When viewing a manuscript on the browse page, the user sees domain classification badges (e.g., "Piyyut", "Bible Texts", "Letters")
  2. When searching, the user can select a domain filter and see only results from manuscripts classified under that domain
  3. Domain badges show hierarchy when applicable (e.g., "Piyyut > Kedushtaot" or parent alongside child)
  4. Domain display and search filtering work identically in both web and desktop apps
**Plans**: TBD

Plans:
- [ ] 27-01: Domain display on browse page (both apps)
- [ ] 27-02: Domain filtering in search (both apps)

### Phase 28: Catalog Enrichment
**Goal**: Users see FJMS catalog metadata alongside existing PGP metadata when browsing manuscripts
**Depends on**: Phase 25
**Requirements**: CAT-01, CAT-02, CAT-03, CAT-04, CAT-05
**Success Criteria** (what must be TRUE):
  1. When viewing a manuscript with FJMS catalog data, the user sees the Hebrew and/or English catalog title on the browse page
  2. Author information from the FJMS catalog is displayed when available
  3. Copy date and copy place from the FJMS catalog are displayed when available
  4. FJMS description appears alongside PGP description (not replacing it) so the user can see both scholarly perspectives
  5. All catalog enrichment fields display in both web and desktop apps
**Plans**: TBD

Plans:
- [ ] 28-01: Catalog enrichment display (both apps)

## Progress

**Execution Order:**
Phases execute in numeric order: 25 -> 26 -> 27 -> 28
(Phases 26, 27, 28 all depend on 25 but are sequenced per user priority: Joins -> Domains -> Catalog)

| Phase | Milestone | Plans Complete | Status | Completed |
|-------|-----------|----------------|--------|-----------|
| 1-7 | v1 | 18/18 | Complete | 2026-02-07 |
| 8-12 | v5.6.0 | 25/25 | Complete | 2026-02-09 |
| 14-17 | v5.7.0 | 14/14 | Complete | 2026-02-10 |
| 18-21 | v5.7.2 | 11/11 | Complete | 2026-02-11 |
| 22-24 | v5.7.3 | 3/3 | Complete | 2026-02-11 |
| 25. Data Infrastructure | v5.8.0 | 0/2 | Not started | - |
| 26. Scientific Joins | v5.8.0 | 0/1 | Not started | - |
| 27. Domain Classifications | v5.8.0 | 0/2 | Not started | - |
| 28. Catalog Enrichment | v5.8.0 | 0/1 | Not started | - |

---
*Roadmap created: 2026-02-09*
*Last updated: 2026-02-12 after v5.8.0 roadmap created*
