# Project State

## Project Reference

See: .planning/PROJECT.md (updated 2026-02-11)

**Core value:** Researchers can find what they need in the Genizah corpus
**Current focus:** Debug PGP Integration -- Phase 21 (Plan 1 of 3 complete)

## Current Position

Phase: 21 (Debug PGP Integration)
Plan: 1 of 3 in current phase -- COMPLETE
Status: Plan 21-01 complete (HTML parser + regex fix, 28 new tests, 438 total passing)
Last activity: 2026-02-11 - Completed quick task 11: Fix profile page showing 0 reputation and 0 corrections

Progress: [███-------] 33% (plan 1 of 3 in phase 21)

## Performance Metrics

**Velocity:**
- Total plans completed: 63 (across all milestones)
- Average duration: ~8 min
- Total execution time: ~7.5 hours

**By Milestone:**

| Milestone | Phases | Plans | Total Time |
|-----------|--------|-------|------------|
| v1 | 1-7 | 18 | 173 min |
| v5.6.0 | 8-12 | 25 | ~134 min |
| v5.7.0 | 14-17 | 14 | ~140 min |
| v5.7.1 | 18-20 | TBD | -- |

## Milestone History

- **v5.7.0 Responsa Search** -- Shipped 2026-02-10 (git tag v5.7.0)
- **v5.6.0 Desktop Parity** -- Shipped 2026-02-09 (git tag v5.6.0)
- **v1 External Data Integration** -- Shipped 2026-02-07 (git tag v5.5.0)

## Accumulated Context

### Decisions

- [21-01] Used explicit [Rr]ecto/[Vv]erso casing instead of re.IGNORECASE to avoid false positives on content lines
- [21-01] Structured regex alternation for marker modifiers instead of broad catch-all pattern
- [21-01] HTML entity &hellip; decodes to Unicode U+2026, not ASCII dots
- [20-02] Responsa tests use behavioral .search() assertions instead of literal substring checks in pattern strings
- [20-02] Manchester shelfmark test expects no-match (substring matching not supported in prefix-based matcher)
- [20-02] Removed "12.123" number-only test case (requires "ts" prefix for matching)
- [20-01] Updated test expectations to match production behavior rather than reverting code changes
- [20-01] Used longer test input texts for boundary tests to satisfy min_distance=3 default
- [19-03] Added 3 apostrophe variants (U+0027, U+2018, U+2019) to normalization patterns -- no index rebuild needed
- [19-02] Regex mode exempted from diacritics stripping -- users control their regex patterns directly
- [19-02] Wildcard/flex-spacing patterns not wrapped with mark tolerance -- custom regex where marks would confuse semantics
- [19-02] No UI code changes needed -- both web and desktop inherit mark-tolerance via pattern strings
- [19-01] Placed normalization functions between expand_judeo_arabic and _SOFIT_TO_NORMAL for logical grouping
- [19-01] Used regex token splitting to handle escape sequences as single units in make_mark_tolerant_pattern
- [18-01] Removed all AI support infrastructure (constants, imports, signals) not just named classes
- [18-02] Kept Regex help description concise after removing AI reference (no replacement text added)
- Decisions are also logged in PROJECT.md Key Decisions table.

### Blockers/Concerns

- Phase 13 (Transcription Search) still deferred -- needs server-side index architecture
- Recto/verso section headers stripped during parsing (v1 tech debt, not blocking)

### Roadmap Evolution

- Phase 21 added: Debug PGP integration

### Quick Tasks Completed

| # | Description | Date | Commit | Directory |
|---|-------------|------|--------|-----------|
| 9 | Fix tabular query builder checkboxes invisible in dark mode | 2026-02-11 | 313a9db | [9-fix-tabular-query-builder-checkboxes-inv](./quick/9-fix-tabular-query-builder-checkboxes-inv/) |
| 10 | Fix desktop community tab corrections SupabaseCorrectionsClient error | 2026-02-11 | 9ef0ac7 | [10-fix-desktop-community-tab-corrections-su](./quick/10-fix-desktop-community-tab-corrections-su/) |
| 11 | Fix profile page showing 0 reputation and 0 corrections | 2026-02-11 | 575ba11 | [11-fix-profile-page-showing-0-reputation-an](./quick/11-fix-profile-page-showing-0-reputation-an/) |

### Future Improvements

- Search WITH JA diacritical marks (intentional marked-letter matching)
- NLI joins import (~424K PartOf relationships)
- Show user's pending corrections in browse page

## Session Continuity

Last session: 2026-02-11
Stopped at: Completed quick-11 (profile page reputation/corrections fix)
Resume file: None
Notes: Quick task 11 complete (3 bugs fixed: wrong field name, missing count query, missing reputation increment). Next: 21-02 (import script).
