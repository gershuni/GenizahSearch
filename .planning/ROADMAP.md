# Roadmap: GenizahSearch

## Milestones

- v1 External Data Integration - Phases 1-7 (shipped 2026-02-07)
- v5.6.0 Desktop Parity & PGP Integration - Phases 8-12 (shipped 2026-02-09, Phase 13 deferred)
- v5.7.0 Responsa Search - Phases 14-17 (shipped 2026-02-10)
- v5.7.2 Cleanup, Normalization & Sections - Phases 18-21 (shipped 2026-02-11)
- v5.7.3 Pending Corrections Visibility - Phases 22-24 (in progress)

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

</details>

<details>
<summary>v5.7.0 Responsa Search (Phases 14-17) - SHIPPED 2026-02-10</summary>

See: .planning/milestones/v5.7.0-ROADMAP.md

4 phases, 14 plans.
Responsa Project-style advanced search with syntax parsing, JA expansion, tabular query builder, explosion guards.
25/25 requirements satisfied. 221 automated Responsa tests.

</details>

<details>
<summary>v5.7.2 Cleanup, Normalization & Sections (Phases 18-21) - SHIPPED 2026-02-11</summary>

See: .planning/milestones/v5.7.2-ROADMAP.md

4 phases, 11 plans.
Dead AI code removed, Unicode search normalization, full green test suite (447 tests),
structural HTML section parser for PGP transcriptions.
13/13 requirements satisfied.

- [x] Phase 18: Dead Code Removal -- AI artifacts purged from both apps
- [x] Phase 19: Search Normalization -- Diacritics, geresh, apostrophe stripping with mark-tolerant highlighting
- [x] Phase 20: Test Suite Green -- 447 tests passing, 0 failures
- [x] Phase 21: Debug PGP Integration -- Structural HTML section parser, sections JSONB schema, cross-app display

</details>

### v5.7.3 Pending Corrections Visibility (In Progress)

**Milestone Goal:** Let users see their own pending (unapproved) corrections as a selectable version in the version selector while browsing manuscripts, in both web and desktop apps.

- [x] **Phase 22: Pending Corrections Data Layer** - Shared service to fetch user's own pending corrections per page (completed 2026-02-11)
- [x] **Phase 23: Web Pending Corrections Display** - Web version selector shows pending corrections with visual distinction (completed 2026-02-11)
- [ ] **Phase 24: Desktop Pending Corrections Display** - Desktop version selector shows pending corrections with visual distinction

## Phase Details

### Phase 22: Pending Corrections Data Layer
**Goal**: Both apps can retrieve a user's own pending corrections for any manuscript page through a shared service function
**Depends on**: Nothing (first phase of milestone)
**Requirements**: CORR-03
**Success Criteria** (what must be TRUE):
  1. A shared service function exists that returns pending corrections for a given sys_id + page_number, filtered to the authenticated user only
  2. The function returns corrections with statuses `draft`, `pending`, and `under_review` (all pre-approval states)
  3. When no user is authenticated, the function returns no pending corrections (empty result)
  4. When a different user is authenticated, they do not see another user's pending corrections
**Plans:** 1 plan

Plans:
- [x] 22-01-PLAN.md -- Shared corrections service function + tests

### Phase 23: Web Pending Corrections Display
**Goal**: Logged-in users see their own pending corrections as a selectable, visually distinct version in the web app's version selector
**Depends on**: Phase 22
**Requirements**: CORR-01, CORR-02, CORR-04, CORR-05
**Success Criteria** (what must be TRUE):
  1. When a logged-in user browses a manuscript page where they have pending corrections, those corrections appear as entries in the version selector
  2. Pending corrections are visually distinct from approved corrections (different label, styling, or indicator showing their pending status)
  3. User can select a pending correction and see its full text displayed, same as selecting an approved correction
  4. When the user is not logged in, no pending corrections appear in the version selector
  5. When the user has no pending corrections for the current page, the version selector looks and behaves exactly as before
**Plans:** 1 plan

Plans:
- [x] 23-01-PLAN.md -- Add pending corrections to web version selector with visual distinction + tests

### Phase 24: Desktop Pending Corrections Display
**Goal**: Logged-in users see their own pending corrections as a selectable, visually distinct version in the desktop app's version selector
**Depends on**: Phase 22
**Requirements**: CORR-06
**Success Criteria** (what must be TRUE):
  1. When a logged-in user browses a manuscript page where they have pending corrections, those corrections appear as entries in the desktop version selector
  2. Pending corrections are visually distinct from approved corrections (different label, styling, or indicator showing their pending status)
  3. User can select a pending correction and see its full text displayed, same as selecting an approved correction
  4. When the user is not logged in or has no pending corrections for the current page, the version selector behaves exactly as before
**Plans:** 1 plan

Plans:
- [ ] 24-01-PLAN.md -- Verify existing desktop pending corrections via automated tests

## Progress

**Execution Order:**
Phases execute in numeric order: 22 -> 23 -> 24

| Phase | Milestone | Plans Complete | Status | Completed |
|-------|-----------|----------------|--------|-----------|
| 1-7 | v1 | 18/18 | Complete | 2026-02-07 |
| 8-12 | v5.6.0 | 25/25 | Complete | 2026-02-09 |
| 14-17 | v5.7.0 | 14/14 | Complete | 2026-02-10 |
| 18-21 | v5.7.2 | 11/11 | Complete | 2026-02-11 |
| 22. Data Layer | v5.7.3 | 1/1 | Complete | 2026-02-11 |
| 23. Web Display | v5.7.3 | 1/1 | Complete | 2026-02-11 |
| 24. Desktop Display | v5.7.3 | 0/1 | Not started | - |

---
*Roadmap created: 2026-02-09*
*Last updated: 2026-02-11 after Phase 24 planning complete*
