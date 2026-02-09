# Roadmap: GenizahSearch

## Milestones

- v1 External Data Integration - Phases 1-7 (shipped 2026-02-07)
- v5.6.0 Desktop Parity & PGP Integration - Phases 8-12 (shipped 2026-02-09, Phase 13 deferred)
- v5.7.0 Responsa Search - Phases 14-17 (in progress)

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

5 phases, 25 plans, ~134 min total execution.
Desktop PGP feature parity, Virtual Reading Desk, 35,839 PGP documents imported.
Phase 13 (Transcription Search) deferred -- index build too slow for desktop.

- [x] Phase 8: Foundation -- Shared service layer extraction
- [x] Phase 9: Data Import -- Remaining ~34K PGP documents
- [x] Phase 10: Desktop PGP Core -- Transcription display and version selector
- [x] Phase 11: Virtual Reading Desk -- Multi-manuscript viewer (both apps)
- [x] Phase 12: Desktop PGP Discovery -- Metadata, search indicators, tag search, joins

</details>

### v5.7.0 Responsa Search (In Progress)

**Milestone Goal:** Add Responsa Project-style search to both web and desktop apps -- syntax parsing, Judeo-Arabic/flexible spacing support, tabular query builder, and combinatorial explosion guards.

**Phase Numbering:** Continues from v5.6.0 milestone (Phase 12). Phases 14-17.

- [x] **Phase 14: Responsa Core Engine** - Parse Responsa syntax, expand prefixes/JA, integrate with Tantivy and Regex (completed 2026-02-09)
- [x] **Phase 15: Search UI (Both Apps)** - Checkboxes, mode switching, wiring to core engine (completed 2026-02-09)
- [ ] **Phase 16: Tabular Query Builder** - Expansion panel (web) and QDialog (desktop) for visual query construction
- [ ] **Phase 17: Integration Testing & Polish** - Cross-app verification, edge cases, performance testing

## Phase Details

### Phase 14: Responsa Core Engine
**Goal**: genizah_core.py can parse Responsa syntax, expand grammatical/JA prefixes, build Tantivy OR groups and regex patterns with wildcards, and enforce combinatorial explosion guards
**Depends on**: Nothing (first phase of v5.7.0)
**Requirements**: CORE-01, CORE-02, CORE-03, CORE-04, CORE-05, CORE-06, CORE-07, CORE-08, XAPP-02
**Success Criteria** (what must be TRUE):
  1. `parse_responsa_query("#(שלום/שלומות) עולם*")` returns a list of `ResponsaComponent` objects with correct words, prefixes, and wildcard flags
  2. `expand_grammatical_prefixes("שלום")` returns ~25 Hebrew prefix forms including single-letter (ושלום, השלום, בשלום, ...) and compound (והשלום, כשלום, ...)
  3. `expand_judeo_arabic("כלמה")` returns 8-14 forms including אלכלמה, ואלכלמה, sun letter assimilation where applicable
  4. `build_tantivy_query()` with Responsa components produces OR groups with ^5 boosting on exact terms
  5. `build_regex_pattern()` with wildcard components produces `\S*` suffix/prefix patterns and `\s*` per-char flexible spacing on original terms
  6. Bidirectional gap produces `(forward)|(backward)` regex alternation
  7. When expanded terms exceed 500, downgrade cascade fires (variants->basic->off->JA off->error) and returns a warning message
  8. When Responsa mode is ON, `parse_query_syntax()` prefix shortcuts are bypassed -- query goes to `parse_responsa_query()` instead
**Plans:** 2 plans

Plans:
- [x] 14-01-PLAN.md -- TDD: ResponsaComponent dataclass, parser, prefix expansion, JA expansion, explosion guard
- [x] 14-02-PLAN.md -- TDD: Wire Responsa components into build_tantivy_query, build_regex_pattern, parse_query_syntax, execute_search

### Phase 15: Search UI (Both Apps)
**Goal**: Both web and desktop apps have Responsa checkboxes that control search behavior, with proper mode interaction and state management
**Depends on**: Phase 14 (core engine must be functional)
**Requirements**: WEB-01, WEB-02, WEB-03, WEB-06, WEB-07, DESK-01, DESK-02, DESK-03, DESK-06, DESK-07, DESK-08
**Success Criteria** (what must be TRUE):
  1. Web search page shows Responsa Mode, Variants, Judeo-Arabic, Flexible Spacing checkboxes; Bidirectional Gap in Advanced Options
  2. Desktop search tab shows equivalent checkboxes in a new row
  3. When Responsa Mode checked, mode dropdown hides in both apps; when unchecked, dropdown returns
  4. Searching with Responsa Mode ON + `#שלום` finds results with Hebrew prefix forms (ושלום, השלום, etc.)
  5. Searching with JA checkbox ON + `#כלמה` finds results with Judeo-Arabic article forms (אלכלמה, etc.)
  6. URL in web reflects checkbox state: `?responsa=1&variants=1&ja=1&flex_spaces=1`
  7. In PGP Tags mode, all Responsa checkboxes are hidden (not disabled)
  8. Desktop checkboxes reset to defaults on app startup
  9. SearchThread receives `responsa_options` parameter without breaking existing callers
**Plans:** 2 plans

Plans:
- [x] 15-01-PLAN.md -- Web UI: Responsa checkboxes, mode interaction, URL state, explosion warning, expanded term count, core expanded count addition
- [x] 15-02-PLAN.md -- Desktop UI: SearchThread extension, Responsa checkboxes, mode interaction, warning display, expanded term count

### Phase 16: Tabular Query Builder
**Goal**: Users can visually construct Responsa queries using a tabular interface with 2-3 component columns, which generates syntax text inserted into the search field
**Depends on**: Phase 15 (checkboxes must be wired to core engine)
**Requirements**: WEB-04, WEB-05, DESK-04, DESK-05
**Success Criteria** (what must be TRUE):
  1. Web has a collapsible expansion panel with 2-3 component columns, each with word inputs and distance controls
  2. Desktop has a "Query Builder" button that opens a QDialog with equivalent functionality
  3. Filling in components and clicking "Build"/"Apply" inserts Responsa syntax into the search field
  4. Component columns support: multiple words (OR alternatives), distance between components, per-component checkboxes (prefixes #, wildcard *)
  5. The tabular UI is one-way: changes in the builder update the text field, not vice versa
**Plans:** TBD

### Phase 17: Integration Testing & Polish
**Goal**: Both apps produce identical results for Responsa queries, edge cases are handled, and performance is verified on the full corpus
**Depends on**: Phase 15, Phase 16
**Requirements**: XAPP-01
**Success Criteria** (what must be TRUE):
  1. The same Responsa query produces identical result sets in web and desktop apps
  2. Combinatorial explosion guard triggers correctly for complex queries (verified with test cases)
  3. Search performance is acceptable: Responsa query with variants + JA completes in <5 seconds on full corpus
  4. Edge cases handled: empty query, single-character terms with flex spacing (min 3 chars), `#` in Shelfmark mode vs Responsa mode
  5. Existing search modes (Exact, Variants, Fuzzy, Regex, Shelfmark, Title, PGP Tags) all work unchanged when Responsa checkbox is OFF
**Plans:** TBD

## Progress

**Execution Order:** 14 -> 15 -> 16 -> 17

| Phase | Milestone | Plans Complete | Status | Completed |
|-------|-----------|----------------|--------|-----------|
| 14. Responsa Core Engine | v5.7.0 | 2/2 | Complete | 2026-02-09 |
| 15. Search UI (Both Apps) | v5.7.0 | 2/2 | Complete | 2026-02-09 |
| 16. Tabular Query Builder | v5.7.0 | 0/? | Not started | - |
| 17. Integration Testing & Polish | v5.7.0 | 0/? | Not started | - |

---
*Roadmap created: 2026-02-09*
