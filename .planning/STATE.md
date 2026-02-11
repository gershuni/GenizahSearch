# Project State

## Project Reference

See: .planning/PROJECT.md (updated 2026-02-11)

**Core value:** Researchers can find what they need in the Genizah corpus
**Current focus:** v5.7.1 Cleanup & Polish -- Phase 19 (Search Normalization)

## Current Position

Phase: 19 of 20 (Search Normalization) -- COMPLETE
Plan: 2 of 2 in current phase -- ALL DONE
Status: Phase 19 complete (both plans executed)
Last activity: 2026-02-11 -- Completed 19-02 (Search Pipeline Integration)

Progress: [█████░░░░░] 50%

## Performance Metrics

**Velocity:**
- Total plans completed: 60 (across all milestones)
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

### Future Improvements

- Search WITH JA diacritical marks (intentional marked-letter matching)
- NLI joins import (~424K PartOf relationships)
- Show user's pending corrections in browse page

## Session Continuity

Last session: 2026-02-11
Stopped at: Completed 19-02-PLAN.md (Search Pipeline Integration) -- Phase 19 complete
Resume file: None
Notes: Phase 19 complete (2 plans, ~4 min total). Search normalization fully wired. Ready for Phase 20 or UAT.
