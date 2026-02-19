# Roadmap: GenizahSearch

## Milestones

- ✅ **v1 External Data Integration** -- Phases 1-7 (shipped 2026-02-07)
- ✅ **v5.6.0 Desktop Parity & PGP Integration** -- Phases 8-12 (shipped 2026-02-09)
- ✅ **v5.7.0 Responsa Search** -- Phases 14-17 (shipped 2026-02-10)
- ✅ **v5.7.2 Cleanup, Normalization & Sections** -- Phases 18-21 (shipped 2026-02-11)
- ✅ **v5.7.3 Pending Corrections Visibility** -- Phases 22-24 (shipped 2026-02-11)
- ✅ **v5.8.0 FJMS Integration** -- Phases 25-28 (shipped 2026-02-15)
- ✅ **v5.9.0 Multi-Source Image & Metadata Integration** -- Phases 29-34 (shipped 2026-02-16)
- 🚧 **v6.0.0 Local Data Architecture** -- Phases 35-38 (in progress)

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

<details>
<summary>✅ v5.9.0 Multi-Source Image & Metadata Integration (Phases 29-34) -- SHIPPED 2026-02-16</summary>

See: .planning/milestones/v5.9.0-ROADMAP.md

6 phases, 22 plans (including 3 gap closure plans), 76 commits.
NLI crossref sidecar (815K records), Cambridge IIIF (141K), Manchester LUNA (28K), JTS/Princeton Figgy (453).
Multi-source image viewing with folio navigation, bibliography (542K), catalog refs (64K), physical metadata.
11/14 requirements satisfied, 1 invalidated (FGP!=FL), 2 deferred (REL-01/REL-02).

</details>

### v6.0.0 Local Data Architecture (In Progress)

**Milestone Goal:** Migrate all PGP reference data from Supabase to a local SQLite sidecar and add FJMS catalog descriptions as scholarly sources, making browsing fully offline-capable and eliminating cloud dependency for read-only data.

- [x] **Phase 35: PGP Sidecar Export** - Export all PGP data from Supabase to pgp.db with validated JSON serialization (completed 2026-02-17)
- [x] **Phase 36: PGP Service Layer** - Rewrite document_service.py to read from SQLite, integrating both apps with pgp.db (completed 2026-02-17)
- [x] **Phase 37: FJMS Catalog Descriptions** - Export enriched FJMS catalog data (4 new tables, ~1.7M rows) and surface via dedicated dialog in both apps (completed 2026-02-17)
- [x] **Phase 38: Distribution and Verification** - Bundle pgp.db for distribution and verify offline browsing works end-to-end (completed 2026-02-18)

## Phase Details

### Phase 35: PGP Sidecar Export
**Goal**: PGP reference data exists as a validated, reproducible SQLite sidecar
**Depends on**: Nothing (first phase of v6.0.0)
**Requirements**: MIGR-01, MIGR-04, MIGR-08
**Success Criteria** (what must be TRUE):
  1. Running the export script produces pgp.db with all 4 tables (documents, sources, footnotes, fragments) and correct row counts matching Supabase
  2. JSON columns (tags, sections) survive the round-trip: exported to TEXT, parsed back to identical Python objects
  3. Re-running the export script from scratch produces an identical pgp.db (repeatable, idempotent)
  4. Meta table tracks version and export timestamp (consistent with existing sidecar pattern)
**Plans**: 1 plan
Plans:
- [ ] 35-01-PLAN.md — Export script + pgp.db generation with validation

### Phase 36: PGP Service Layer
**Goal**: Both apps read all PGP data from local SQLite instead of Supabase, with identical behavior
**Depends on**: Phase 35
**Requirements**: MIGR-02, MIGR-03, MIGR-05, MIGR-06, MIGR-07
**Success Criteria** (what must be TRUE):
  1. User browses any PGP document in web app and sees the same metadata, transcriptions, footnotes, and fragments as before the migration
  2. User browses any PGP document in desktop app and sees the same metadata, transcriptions, footnotes, and fragments as before the migration
  3. Search results show PGP transcription indicators (batch lookup) with results identical to Supabase-backed version
  4. PGP tag-based search returns the same results as before (using SQLite json_each instead of Supabase GIN)
  5. Version selector displays all PGP editions and translations with correct section parsing
**Plans**: 3 plans
Plans:
- [x] 36-01-PLAN.md — PgpService class rewrite (Supabase to SQLite) + web shim update
- [x] 36-02-PLAN.md — Test suite rewrite (in-memory SQLite fixtures) + import smoke tests
- [ ] 36-03-PLAN.md — Gap closure: FL ID browse path missing pgp_metadata assignment

### Phase 37: FJMS Catalog Descriptions
**Goal**: Researchers can access rich FJMS scholarly descriptions (content, physical metadata, source attribution) from the browse page in both apps
**Depends on**: Nothing (can run parallel to Phase 36; only depends on fjms_enrichment.db existing, which shipped in v5.8.0)
**Requirements**: FJMS-01, FJMS-02, FJMS-03
**Status**: Re-planned (v2) with enriched export scope
**Success Criteria** (what must be TRUE):
  1. FJMS catalog descriptions include rich structured data (content identification + physical metadata) beyond what browse already shows
  2. User clicks a dedicated button in the browse metadata panel and sees the FJMS scholarly description for that manuscript
  3. Each description shows source attribution (catalog name and/or scholar)
**Plans**: 5 plans
Plans:
- [ ] 37-01-PLAN.md — Export script enrichment (4 new tables + catalog v2 schema + FTS5 rebuild)
- [ ] 37-02-PLAN.md — Service layer methods + tests + translation keys
- [ ] 37-03-PLAN.md — Web catalog dialog + browse/search button wiring
- [ ] 37-04-PLAN.md — Desktop catalog dialog + Browse tab/ResultDialog button wiring
- [ ] 37-05-PLAN.md — Gap closure: free description source attribution + desktop RTL fix

### Phase 38: Distribution and Verification
**Goal**: pgp.db is bundled for both distribution channels and desktop PGP browsing works without internet
**Depends on**: Phase 36, Phase 37
**Requirements**: DIST-01, DIST-02, PERF-01
**Success Criteria** (what must be TRUE):
  1. Desktop installer includes pgp.db and the app launches with full PGP browsing from local data
  2. Web server deployment includes pgp.db and serves PGP data from local sidecar
  3. Desktop app with internet disconnected can browse PGP metadata, transcriptions, footnotes, and fragment navigation (images excluded)
**Plans**: 3 plans
Plans:
- [ ] 38-01-PLAN.md — Bundle pgp.db in desktop build + deployment docs + PgpService.get_version()
- [ ] 38-02-PLAN.md — Offline verification tests (all three sidecars)
- [ ] 38-03-PLAN.md — Sidecar update mechanism + About screen data sources

## Progress

**Execution Order:**
Phases execute in numeric order: 35 -> 36 -> 37 -> 38
Note: Phase 37 can run in parallel with Phase 36 (independent data source).

| Phase | Plans Complete | Status | Completed |
|-------|----------------|--------|-----------|
| 35. PGP Sidecar Export | 1/1 | Complete    | 2026-02-17 |
| 36. PGP Service Layer | 2/3 | Complete    | 2026-02-17 |
| 37. FJMS Catalog Descriptions | 4/5 | Gap closure | 2026-02-17 |
| 38. Distribution and Verification | 3/3 | Complete    | 2026-02-18 |

### Phase 39: Bug Fixing, Cleanup, Performance Improving

**Goal:** Stabilize and polish the app: fix all desktop crashes, add server-side pagination, integrate PostHog analytics, optimize web performance, and add Playwright E2E + performance tests
**Depends on:** Phase 38
**Plans:** 2/5 plans executed

Plans:
- [ ] 39-01-PLAN.md — Fix all desktop crash types (sip.isdeleted guards + rare crash verification) + archive crash log
- [ ] 39-02-PLAN.md — Replace 200-result cap with paginated search results (PAGE_SIZE=50, all 11 sites)
- [ ] 39-03-PLAN.md — Integrate PostHog analytics alongside Google Analytics (env-var gated)
- [ ] 39-04-PLAN.md — Cache domain hierarchy to eliminate ~5s filter dialog lag
- [ ] 39-05-PLAN.md — Add NiceGUI Screen E2E tests for search/browse + performance tests

---
*Roadmap created: 2026-02-09*
*Last updated: 2026-02-19 after Phase 39 replanning (5 plans, verified)*
