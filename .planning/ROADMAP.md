# Roadmap: GenizahSearch

## Milestones

- v1 External Data Integration - Phases 1-7 (shipped 2026-02-07)
- v5.6.0 Desktop Parity & PGP Integration - Phases 8-12 (shipped 2026-02-09, Phase 13 deferred)
- v5.7.0 Responsa Search - Phases 14-17 (shipped 2026-02-10)

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

## Progress

| Phase | Milestone | Plans Complete | Status | Completed |
|-------|-----------|----------------|--------|-----------|
| 1-7 | v1 | 18/18 | Complete | 2026-02-07 |
| 8-12 | v5.6.0 | 25/25 | Complete | 2026-02-09 |
| 14. Responsa Core Engine | v5.7.0 | 2/2 | Complete | 2026-02-09 |
| 15. Search UI (Both Apps) | v5.7.0 | 4/4 | Complete | 2026-02-10 |
| 16. Tabular Query Builder | v5.7.0 | 3/3 | Complete | 2026-02-10 |
| 17. Integration Testing & Polish | v5.7.0 | 5/5 | Complete | 2026-02-10 |

---
*Roadmap created: 2026-02-09*
*Last updated: 2026-02-10 after v5.7.0 shipped*
