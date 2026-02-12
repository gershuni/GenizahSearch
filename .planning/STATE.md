# Project State

## Project Reference

See: .planning/PROJECT.md (updated 2026-02-12)

**Core value:** Researchers can find what they need in the Genizah corpus
**Current focus:** v5.8.0 FJMS Integration -- Phase 26 (Scientific Joins)

## Current Position

Phase: 26 of 28 (Scientific Joins) -- second of 4 in v5.8.0 -- COMPLETE
Plan: 3 of 3 in current phase (all complete, including gap closure)
Status: Phase 26 fully complete (gap closure 26-03 done), ready for Phase 27
Last activity: 2026-02-12 -- Completed 26-03 (Dual Badge Display)

Progress: [███████░░░] 71% (5/7 plans)

## Performance Metrics

**Velocity:**
- Total plans completed: 76 (across all milestones)
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
| v5.8.0 | 25-28 | 5/7 | 23 min |

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
- FJMS joins merged as third source after user and PGP in fetch_connected_fragments
- Purple badge for FJMS source distinction (user=none, PGP=blue, FJMS=purple)
- Three-way join merge pipeline: user -> PGP -> FJMS with dedup at each stage
- GROUP BY + GROUP_CONCAT(DISTINCT) for multi-group join dedup at SQL level (not Python post-processing)
- Sources stored as list ('sources') instead of string ('source') in formatted_joins for multi-source badge support
- Merge-on-collision dedup: append source to existing entry instead of dropping overlapping FJMS entries

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
Stopped at: Completed 26-03-PLAN.md (Dual Badge Display) -- Phase 26 gap closure complete
Resume file: None
Notes: Phase 26 fully complete (3/3 plans including gap closure). Dual PGP+FJMS badges in web, "PGP, FJMS" in desktop. 510 passed (3 new tests). Next: Phase 27 (Domain Classification).
