# Roadmap: GenizahSearch

## Milestones

- v1 External Data Integration - Phases 1-7 (shipped 2026-02-07)
- v5.6.0 Desktop Parity & PGP Integration - Phases 8-12 (shipped 2026-02-09, Phase 13 deferred)
- v5.7.0 Responsa Search - Phases 14-17 (shipped 2026-02-10)
- v5.7.2 Cleanup, Normalization & Sections - Phases 18-21 (shipped 2026-02-11)

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

## Progress

| Phase | Milestone | Plans Complete | Status | Completed |
|-------|-----------|----------------|--------|-----------|
| 1-7 | v1 | 18/18 | Complete | 2026-02-07 |
| 8-12 | v5.6.0 | 25/25 | Complete | 2026-02-09 |
| 14-17 | v5.7.0 | 14/14 | Complete | 2026-02-10 |
| 18-21 | v5.7.2 | 11/11 | Complete | 2026-02-11 |

---
*Roadmap created: 2026-02-09*
*Last updated: 2026-02-11 after v5.7.2 milestone completion*
