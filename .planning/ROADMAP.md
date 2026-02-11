# Roadmap: GenizahSearch

## Milestones

- v1 External Data Integration - Phases 1-7 (shipped 2026-02-07)
- v5.6.0 Desktop Parity & PGP Integration - Phases 8-12 (shipped 2026-02-09, Phase 13 deferred)
- v5.7.0 Responsa Search - Phases 14-17 (shipped 2026-02-10)
- v5.7.1 Cleanup & Polish - Phases 18-20 (shipped 2026-02-11)

## Phases

<details>
<summary>v1 External Data Integration (Phases 1-7) - SHIPPED 2026-02-07</summary>

See: .planning/milestones/v1-ROADMAP.md

9 phases (including inserted 7.1, 7.2), 18 plans, 173 min total execution.
Imported 7,090 PGP documents with 9,364 transcription/translation sources.
Full PGP feature set in web app.

</details>

<details>
<summary>v5.6.0 Desktop Parity & PGP Integration (Phases 8-12) - SHIPPED 2026-02-09</summary>

See: .planning/milestones/v5.6.0-ROADMAP.md

5 phases, 25 plans, ~134 min total execution.
Desktop PGP feature parity, Virtual Reading Desk, 35,839 PGP documents imported.
Phase 13 (Transcription Search) deferred -- index build too slow for desktop.

- [x] Phase 8: Foundation -- Shared service layer extraction
- [x] Phase 9: Data Import -- Remaining ~34K PGP documents
- [x] Phase 10: Desktop PGP Core -- Transcription display and version selector
- [x] Phase 11: Virtual Reading Desk -- Multi-manuscript viewer (both apps)
- [x] Phase 12: Desktop PGP Discovery -- Metadata, search indicators, tag search, joins

</details>

<details>
<summary>v5.7.0 Responsa Search (Phases 14-17) - SHIPPED 2026-02-10</summary>

See: .planning/milestones/v5.7.0-ROADMAP.md

4 phases, 14 plans.
Responsa Project-style advanced search with syntax parsing, JA expansion, tabular query builder, explosion guards.
25/25 requirements satisfied. 221 automated Responsa tests.

- [x] Phase 14: Responsa Core Engine -- Parser, expansions, Tantivy/regex integration
- [x] Phase 15: Search UI (Both Apps) -- Dropdown mode with sub-options and syntax legend
- [x] Phase 16: Tabular Query Builder -- Dialog (web + desktop) with 2-4 components
- [x] Phase 17: Integration Testing & Polish -- Parity tests, regression, edge cases, UAT gap closure

</details>

### v5.7.1 Cleanup & Polish (In Progress)

**Milestone Goal:** Remove dead AI code, add diacritics normalization for Judeo-Arabic search, and resolve all pre-existing test failures.

- [x] **Phase 18: Dead Code Removal** - Remove all AI Search artifacts from both apps, help docs, and core (completed 2026-02-11)
- [x] **Phase 19: Search Normalization** - Diacritics and geresh/gershayim stripping with mark-tolerant highlighting (completed 2026-02-11)
- [x] **Phase 20: Test Suite Green** - Fix broken tests, delete obsolete files, achieve zero failures (completed 2026-02-11)

## Phase Details

### Phase 18: Dead Code Removal
**Goal**: AI Search artifacts fully removed -- both apps launch and function with no trace of AI features
**Depends on**: Nothing (standalone cleanup)
**Requirements**: CLEAN-01, CLEAN-02, CLEAN-03, CLEAN-04
**Success Criteria** (what must be TRUE):
  1. Desktop app launches without AI-related classes (AIManager, AIDialog, AIWorkerThread) and no AI button or Settings panel section exists
  2. Web app starts without AI import or initialization code
  3. Help documentation in both apps contains no mention of AI Search features
  4. genizah_core.py has no google-genai import and no AI_PROVIDER_ENDPOINTS constant
  5. Both apps start and function normally after removal (no import errors, no missing references)
**Plans**: 2 plans

Plans:
- [x] 18-01-PLAN.md -- Remove AI from core + desktop (genizah_core.py, gui_threads.py, genizah_app.py, Help.html)
- [x] 18-02-PLAN.md -- Remove AI from web + dependencies (web/main.py, web/state.py, web/pages/help.py, requirements.txt, CHANGELOG.md)

### Phase 19: Search Normalization
**Goal**: Users searching with diacritical marks or geresh/gershayim get correct results, and highlighting works even when source text contains combining marks
**Depends on**: Nothing (independent of Phase 18)
**Requirements**: NORM-01, NORM-02, NORM-03, NORM-04
**Success Criteria** (what must be TRUE):
  1. A search query containing combining diacritical marks (U+0300-U+036F) returns the same results as the query without them
  2. A search query containing Hebrew geresh or gershayim returns the same results as the query without them
  3. Search result highlighting correctly marks matched text even when the source text contains combining marks between base letters
  4. All existing search modes (standard, variants, Responsa, PGP Tags) produce identical results to before normalization was added
**Plans**: 3 plans

Plans:
- [x] 19-01-PLAN.md -- TDD: Core normalization functions (strip_search_diacritics, make_mark_tolerant_pattern) with unit tests
- [x] 19-02-PLAN.md -- Wire normalization into search pipeline (execute_search, lab_search, build_regex_pattern)
- [x] 19-03-PLAN.md -- Gap closure: Add ASCII apostrophe and curly quotes to normalization patterns

### Phase 20: Test Suite Green
**Goal**: Full test suite passes with zero failures -- all pre-existing broken tests fixed and obsolete test files removed
**Depends on**: Phase 18, Phase 19 (TEST-05 requires all code changes complete)
**Requirements**: TEST-01, TEST-02, TEST-03, TEST-04, TEST-05
**Success Criteria** (what must be TRUE):
  1. Export filename tests pass with underscore-separated expectations (4 tests)
  2. Boundary search tests pass with aligned expectations (2 tests)
  3. Excel column index test passes with correct assertion (1 test)
  4. Obsolete backend test files (test_api_flow.py, test_corrections_api.py, test_corrections_integration.py) no longer exist
  5. `pytest` runs to completion with zero failures across the entire test suite
**Plans**: 2 plans

Plans:
- [x] 20-01-PLAN.md -- Delete obsolete backend tests, fix export service + boundary search test expectations
- [x] 20-02-PLAN.md -- Fix responsa integration + shelfmark normalization test expectations, achieve full green suite

## Progress

**Execution Order:**
Phases 18 and 19 are independent and can execute in either order. Phase 20 executes last.

| Phase | Milestone | Plans Complete | Status | Completed |
|-------|-----------|----------------|--------|-----------|
| 1-7 | v1 | 18/18 | Complete | 2026-02-07 |
| 8-12 | v5.6.0 | 25/25 | Complete | 2026-02-09 |
| 14-17 | v5.7.0 | 14/14 | Complete | 2026-02-10 |
| 18. Dead Code Removal | v5.7.1 | 2/2 | Complete | 2026-02-11 |
| 19. Search Normalization | v5.7.1 | 3/3 | Complete | 2026-02-11 |
| 20. Test Suite Green | v5.7.1 | 2/2 | Complete | 2026-02-11 |

### Phase 21: Debug PGP Integration

**Goal:** Fix PGP transcription section parsing so recto/verso markers are correctly recognized and text is split across the right manuscript images
**Depends on:** Phase 20
**Plans:** 1 plan

Plans:
- [ ] 21-01-PLAN.md -- TDD: Fix section parsing regex + add comprehensive marker variant tests

---
*Roadmap created: 2026-02-09*
*Last updated: 2026-02-11 after Phase 21 planning complete*
