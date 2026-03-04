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
- ✅ **v6.1.0 Catalog Browse & Navigation** -- Phase 41 (shipped 2026-02-27)
- **v6.5.0 Search UX & Filtered Search** -- Phases 42-47 (in progress)
- **v7.0.0 Transcription Search** -- Phases 48-50 (planned)

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

### v6.5.0 Search UX & Filtered Search (In Progress)

**Milestone Goal:** Improve the daily search experience (composition UX, session persistence, quick wins), add CreationType visibility, implement bidirectional filtered search (pre-search filtering from search page + "search within" from catalog browse), and translate all data via Dicta API for multilingual access.

**Origin:** Power user feedback letter (2026-02-27, 17 requests) + existing roadmap items.

- [x] **Phase 42: Search UX & Composition Polish** - Search duration display, ETA, partial results on cancel, chunk count, min-chunks filter, result/excluded separator, CreationType badge on search & browse results (complete)
- [x] **Phase 43: Session Persistence & Search History** - Restore state + exclusions on reopen (critical: user lost 5K exclusions), search history with saved results
- [x] **Phase 44: Quick UX Wins** - Desktop notification on search completion, prevent sleep during search, Hebrew library names, copy from compact results (complete)
- [x] **Phase 45: Filtered Search Context** - Shared filter set (domain/author/work/date/CreationType), bidirectional: search page pre-filter (Path A) + catalog browse "search within" (Path B), works with all search modes including parallels (completed 2026-03-02)
- [ ] **Phase 46: Dicta Translation** - Translate all data (PGP metadata, identifications, catalog, bibliography) via Dicta API for multilingual display and search completeness, careful handling of already-bilingual fields

### v7.0.0 Transcription Search (Planned)

**Milestone Goal:** Import FJMS transcriptions and build a unified searchable index over all human transcription text (PGP + FJMS + user corrections) alongside OCR, with source badges, ranking, and desktop distribution.

- [ ] **Phase 47: Transcription Import** - FJMS ~30K transcriptions from FIST.db into fjms_enrichment.db with shared service access
- [ ] **Phase 48: Transcription Indexing & Search** - Unified Tantivy index over PGP + FJMS + user transcription text with source badges and ranking
- [ ] **Phase 49: Index Distribution & Upgrade** - Pre-built index download, desktop upgrade path, and fresh install support

## Phase Details

### Phase 42: Search UX & Composition Polish
**Goal**: Improve the composition/parallels search experience with progress feedback, partial results, and visual polish; add CreationType badge to all result views
**Depends on**: Nothing (UX improvements on existing search infrastructure)
**Requirements**: UX-01 (א duration display), UX-02 (ב ETA), UX-03 (ג partial results on cancel), UX-04 (ו separator between results/excluded), UX-05 (ז chunk count display), UX-06 (ח min-chunks filter for regular search), UX-07 (טו CreationType badge)
**Success Criteria** (what must be TRUE):
  1. User sees elapsed search duration while a search is running (both apps)
  2. Long composition searches show estimated time remaining or progress indication
  3. Cancelling a search preserves and displays results found so far
  4. Chunk count is visible during/after search
  5. Min-chunks filter available for regular chunk search (not just composition mode)
  6. Clear visual separator between included results and excluded results in expanded view
  7. CreationType badge (Original/Copy/Print/etc.) visible on search results and browse results in both apps
**Plans**: 9 plans
Plans:
- [x] 42-01-PLAN.md — Progress display: elapsed timer, ETA, chunk count, summary line, min-chunks filter (both apps)
- [x] 42-02-PLAN.md — Cancel with partial results, collapsible excluded section with reasons (both apps)
- [x] 42-03-PLAN.md — CreationType badge on all result views (service layer + both apps)
- [x] 42-04-PLAN.md — UAT gap closure: desktop persistent summary, search timer, cancel responsiveness, excluded grouping, printed column
- [x] 42-05-PLAN.md — UAT gap closure: web excluded width, printed filter toggle, Hebrew translations
- [x] 42-06-PLAN.md — UAT gap closure: SearchThread cancel_flag, excluded reason sub-headers, printed column narrow/filterable
- [x] 42-07-PLAN.md — UAT gap closure: web excluded clickable, printed filter label, desktop printed filter, translations
- [x] 42-08-PLAN.md — UAT R3 gap closure: missing Hebrew translations (Searching, reason headers) + desktop partial results notification
- [x] 42-09-PLAN.md — UAT R3 gap closure: comp 3-state printed filter + web cancel enrichment skip + lab mode except fix

### Phase 43: Session Persistence & Search History
**Goal**: Users never lose search state (exclusions, filters, results) when the app restarts, and can recall past searches
**Depends on**: Nothing (independent of other phases)
**Requirements**: SESS-01 (טז session persistence), SESS-02 (יב search history)
**Success Criteria** (what must be TRUE):
  1. Desktop app restores all search state on reopen: active exclusions, domain filters, current results, search parameters
  2. Web app preserves search state across page reloads and browser sessions (via storage)
  3. Users can view a history of past searches with their result counts and re-execute them
  4. Session persistence works in both web and desktop apps
**Plans**: 4 plans
Plans:
- [x] 43-01-PLAN.md -- Desktop session persistence service + save/restore hooks (SESS-01)
- [x] 43-02-PLAN.md -- Web session persistence expansion + settings toggles (SESS-01)
- [x] 43-03-PLAN.md -- Desktop search history dropdowns + resume interrupted search (SESS-02)
- [x] 43-04-PLAN.md -- Web search history dropdowns for search and parallels pages (SESS-02)

### Phase 44: Quick UX Wins
**Goal**: Batch of small, high-value UX improvements across both apps
**Depends on**: Nothing (independent quick fixes)
**Requirements**: QUX-01 (ה desktop notification), QUX-02 (יד prevent sleep), QUX-03 (ט Hebrew library names), QUX-04 (יז copy from compact results)
**Success Criteria** (what must be TRUE):
  1. Desktop app shows system notification when a long search completes (user may have switched to another app)
  2. OS sleep is prevented while a search is running (desktop)
  3. Hebrew library names displayed alongside English names in both apps
  4. Users can select and copy text from compact/collapsed search results without expanding
**Plans**: 2 plans
Plans:
- [x] 44-01-PLAN.md -- Desktop notification, sleep prevention, copy context menu (QUX-01, QUX-02, QUX-04)
- [x] 44-02-PLAN.md -- Hebrew library names (QUX-03)

### Phase 45: Filtered Search Context
**Goal**: Researchers can constrain text searches by scholarly categories (domain, author, work, date, CreationType) either before searching (Path A: search page pre-filter) or after browsing (Path B: catalog browse "search within these results"), with the same shared filter mechanism
**Depends on**: Phase 41 (catalog browse UI and service methods), Phase 42 (CreationType badge data)
**Requirements**: FILT-01 (domain include/exclude), FILT-02 (author/work include/exclude), FILT-03 (CreationType filter), FILT-04 (all search modes), FILT-05 (Path B: browse→search), FILT-06 (both apps)
**Success Criteria** (what must be TRUE):
  1. User can include or exclude FJMS domains, authors, works, date ranges, and CreationType before running a text search (Path A)
  2. User can narrow results in catalog browse, then click "Search within" to execute a text/parallels search constrained to those manuscripts (Path B)
  3. Filters work correctly with all search modes: regular, Responsa, PGP tags, and parallels
  4. Filtered search context is available and functional in both web and desktop apps
  5. Exclude manuscripts in word search works the same as in composition search (ד)
**Plans**: 5 plans
Plans:
- [x] 45-01-PLAN.md — Filter service method (get_filter_sys_ids) + core engine restrict_sys_ids parameter (FILT-01, FILT-02, FILT-03, FILT-04)
- [x] 45-02-PLAN.md — Web search page: Advanced Filters panel, chip bar, word search per-result exclusion, search history filter support (FILT-01, FILT-02, FILT-03, FILT-04, FILT-06)
- [x] 45-03-PLAN.md — Desktop: PreSearchFilterDialog, chip bar, SearchThread/CompositionThread restrict_sys_ids, word search exclusion, session persistence (FILT-01, FILT-02, FILT-03, FILT-04, FILT-06)
- [x] 45-04-PLAN.md — Web parallels: filter panel, per-manuscript exclusion, auto-exclude source, import exclusions, restrict_sys_ids integration (FILT-01, FILT-02, FILT-03, FILT-04, FILT-06)
- [x] 45-05-PLAN.md — Path B: browse-to-search navigation buttons (web + desktop), Hebrew translations (FILT-05, FILT-06)

### Phase 46: Dicta Translation
**Goal**: All scholarly data is available in multiple languages via Dicta Translate API, enabling non-Hebrew/non-English speakers to use the platform and improving search completeness across languages
**Depends on**: Nothing (independent data enrichment, but benefits from all UI being in place)
**Requirements**: TRANS-01 (translate PGP metadata), TRANS-02 (translate identifications/catalog), TRANS-03 (translate bibliography), TRANS-04 (handle bilingual fields), TRANS-05 (search across translations)
**Success Criteria** (what must be TRUE):
  1. PGP document metadata (descriptions, tags, types) available in both Hebrew and English
  2. FJMS catalog data (identifications, domains, descriptions) translated where not already bilingual
  3. Already-bilingual fields are preserved and not double-translated
  4. Translated data improves search coverage (searching in either language finds results regardless of original language)
  5. Translation pipeline is repeatable for future data updates
**Plans**: 5 plans
Plans:
- [x] 46-01-PLAN.md — Dicta API client, few-shot templates, TranslationService, schema definitions, tests (TRANS-01, TRANS-02, TRANS-04)
- [ ] 46-02-PLAN.md — PGP batch translation script: 35K descriptions EN->HE + document_type mapping (TRANS-01, TRANS-04)
- [ ] 46-03-PLAN.md — FJMS batch translation scripts: catalog gap-fill + 303K free descriptions HE->EN + bibliography scaffold (TRANS-02, TRANS-03, TRANS-04)
- [ ] 46-04-PLAN.md — Web search integration, translation toggle, translated match badges, replace MyMemory with Dicta (TRANS-05, TRANS-01, TRANS-02)
- [ ] 46-05-PLAN.md — Desktop translation toggle, translated text display, Hebrew UI strings (TRANS-05, TRANS-01, TRANS-02)

### Phase 47: Transcription Import
**Goal**: FJMS transcription text (~30K transcriptions) is imported from FIST.db into fjms_enrichment.db and accessible through the shared service layer for both apps
**Depends on**: Nothing (data pipeline, independent of v6.5.0 phases)
**Requirements**: TIMP-01, TIMP-02
**Success Criteria** (what must be TRUE):
  1. FJMS transcriptions (~30K) are stored in fjms_enrichment.db with proper schema (shelfmark linkage, scholar attribution, text content)
  2. Both web and desktop apps can retrieve transcription text for a given manuscript through the shared FjmsService
**Plans**: TBD

### Phase 48: Transcription Indexing & Search
**Goal**: Researchers searching for text find matches in human transcriptions (PGP, FJMS, user corrections) alongside OCR results, with transcription hits clearly identified and ranked higher
**Depends on**: Phase 47 (FJMS transcriptions must be imported before indexing)
**Requirements**: TSRCH-01, TSRCH-02, TSRCH-03, TSRCH-04
**Success Criteria** (what must be TRUE):
  1. Text searches return matches found in PGP editions/translations, FJMS transcription text, and user corrections -- not just OCR text
  2. Search results display a badge/icon indicating the source of the match (OCR vs. transcription) so users know the provenance
  3. Transcription-sourced hits are ranked higher than OCR-only hits in the results list
  4. Transcription search works with all search modes (regular and Responsa) in both web and desktop apps
**Plans**: TBD

### Phase 49: Index Distribution & Upgrade
**Goal**: Desktop users (both new and existing) receive a complete Tantivy index that includes transcription fields, with automatic detection and upgrade for users with older index formats
**Depends on**: Phase 48 (index must include transcription fields before distribution)
**Requirements**: IDIST-01, IDIST-02, IDIST-03, IDIST-04
**Success Criteria** (what must be TRUE):
  1. A pre-built Tantivy index (with transcription fields) is available for download from the web server
  2. Desktop app detects when the existing local index lacks transcription fields and offers an upgrade path
  3. Fresh desktop installs receive the complete index with transcription data included
  4. Existing users with older index formats can upgrade without losing their index or needing a full reinstall
**Plans**: TBD

## Progress

**Execution Order:**
v6.5.0: Phases 42 -> 43 -> 44 -> 45 -> 46 (UX first, then filtering, then translation)
v7.0.0: Phases 47 -> 48 -> 49 (import -> index -> distribute)

| Phase | Plans Complete | Status | Completed |
|-------|----------------|--------|-----------|
| 41. Catalog Browse & Navigation | 4/4 | Complete | 2026-02-27 |
| 42. Search UX & Composition Polish | 9/9 | Complete | 2026-03-01 |
| 43. Session Persistence & History | 3/4 | Complete    | 2026-03-02 |
| 44. Quick UX Wins | 2/2 | Complete | 2026-03-02 |
| 45. Filtered Search Context | 5/5 | Complete   | 2026-03-02 |
| 46. Dicta Translation | 2/5 | In Progress|  |
| 47. Transcription Import | 0/TBD | Not started | - |
| 48. Transcription Indexing & Search | 0/TBD | Not started | - |
| 49. Index Distribution & Upgrade | 0/TBD | Not started | - |

**Total phases completed:** 42 (Phase 42 complete, 9/9 plans)
**Total plans completed:** ~130 (Phase 42 fully complete)
**Total milestones shipped:** 9

---
*Roadmap created: 2026-02-09*
*Last updated: 2026-03-04 after Phase 46 planning complete (5 plans)*
