# Project State

## Project Reference

See: .planning/PROJECT.md (updated 2026-02-12)

**Core value:** Researchers can find what they need in the Genizah corpus
**Current focus:** v5.8.0 FJMS Integration -- Phase 25 (Data Infrastructure)

## Current Position

Phase: 25 of 28 (Data Infrastructure) -- first of 4 in v5.8.0 -- COMPLETE
Plan: 2 of 2 in current phase (all complete)
Status: Phase 25 complete, ready for Phase 26
Last activity: 2026-02-12 -- Completed 25-02 (Loader Service)

Progress: [███░░░░░░░] 33% (2/6 plans)

## Performance Metrics

**Velocity:**
- Total plans completed: 73 (across all milestones)
- Average duration: ~8 min
- Total execution time: ~8.5 hours

**By Milestone:**

| Milestone | Phases | Plans | Total Time |
|-----------|--------|-------|------------|
| v1 | 1-7 | 18 | 173 min |
| v5.6.0 | 8-12 | 25 | ~134 min |
| v5.7.0 | 14-17 | 14 | ~140 min |
| v5.7.2 | 18-21 | 11 | ~1 day |
| v5.7.3 | 22-24 | 3 | 6 min |
| v5.8.0 | 25-28 | 2/6 | 6 min |

## Accumulated Context

### Decisions

- SQLite sidecar (`fjms_enrichment.db`) chosen over CSV+dict, Supabase, Tantivy (see FIST_STORAGE_ARCHITECTURE_DECISION.md)
- FTS5 schema included in sidecar now, UI deferred to future milestone
- Phase ordering: Data infra -> Joins -> Domains -> Catalog (user priority)
- AlmaId maps directly to libraries.csv system_number (no normalization needed)
- Catalog table has 322K rows (vs 243K estimated) due to richer join paths; more data is better
- fist_data/ and FIST_DB_BACKUP/ added to .gitignore (large binary files)
- Read-only SQLite via URI mode (file:path?mode=ro) enforces immutability at database level
- Module-level singleton pattern (get_fjms_service) matches existing shared service conventions
- Column names mapped to snake_case keys in returned dicts for Python convention

### Blockers/Concerns

- Phase 13 (Transcription Search) still deferred -- needs server-side index architecture
- FIST.db is 13 GB; export script completed in ~20 seconds with batched inserts
- Thread-safe SQLite for NiceGUI web app (concurrent requests) -- RESOLVED in 25-02 (FjmsService thread_safe=True)

### Future Improvements

- FTS5 catalog search UI (schema ready, deferred to future milestone)
- NLI joins import (~424K PartOf relationships)
- Transcription search (Phase 13, needs server-side index architecture)

## Session Continuity

Last session: 2026-02-12
Stopped at: Completed 25-02-PLAN.md (Loader Service) -- Phase 25 complete
Resume file: None
Notes: Phase 25 complete (2/2 plans). FjmsService ready with 8 query methods and 27 tests. Next: Phase 26 (Joins Integration).
