# Project State

## Project Reference

See: .planning/PROJECT.md (updated 2026-02-16)

**Core value:** Researchers can find what they need in the Genizah corpus
**Current focus:** v6.0.0 Local Data Architecture -- Phase 37 (FJMS Catalog Descriptions)

## Current Position

Phase: 36 of 38 (PGP Service Layer)
Plan: 3 of 3 in current phase (COMPLETE)
Status: Phase 36 COMPLETE
Last activity: 2026-02-17 -- Completed 36-03 FL ID browse path pgp_metadata fix

Progress: [##########] 100% (Phase 36)

## Performance Metrics

**Velocity:**
- Total plans completed: 108 (across 7 milestones)
- Average duration: ~8 min
- Total execution time: ~10 hours

**By Milestone:**

| Milestone | Phases | Plans | Total Time |
|-----------|--------|-------|------------|
| v1 | 1-7 | 18 | 173 min |
| v5.6.0 | 8-12 | 25 | ~134 min |
| v5.7.0 | 14-17 | 14 | ~140 min |
| v5.7.2 | 18-21 | 11 | ~1 day |
| v5.7.3 | 22-24 | 3 | 6 min |
| v5.8.0 | 25-28 | 12 | 57 min |
| v5.9.0 | 29-34 | 22 | ~90 min |
| v6.0.0 | 35-38 | 4+ | 18 min |

## Accumulated Context

### Decisions

See PROJECT.md Key Decisions table for full history.
Recent decisions affecting current work:

- v6.0.0: New pgp.db sidecar (not extending existing sidecars) -- distinct domain boundary
- v6.0.0: Tags stored as TEXT JSON, queried with json_each() -- start simple, optimize if >100ms
- v6.0.0: FJMS descriptions in browse metadata panel button (NOT version selector) -- catalog descriptions, not transcriptions
- v6.0.0: Supabase PGP tables kept (legacy desktop users) -- cutover deferred to future milestone
- Phase 35: Hardcoded Supabase URL/anon key defaults matching codebase pattern
- Phase 35: pgp_url stored as plain TEXT from Supabase generated column
- Phase 35: Compact JSON (sort_keys, no spaces) for deterministic sidecar serialization
- Phase 36: get_pgp_service() defaults to thread_safe=True (read-only SQLite safe across threads)
- Phase 36: get_all_sources_for_fragment optimized from N+1 to 2 queries
- Phase 36: _row_to_dict helper centralizes JSON deserialization for tags/sections columns
- Phase 36: Temp file SQLite fixtures (not :memory:) for testing -- PgpService requires real file for read-only URI mode
- Phase 36: Inline dict assignment (no helper extraction) for FL ID path pgp_metadata -- matches existing load_page pattern

### Blockers/Concerns

- Phase 13 (Transcription Search) still deferred -- needs server-side index architecture
- Tags json_each() benchmarked: 115ms for get_all_distinct_tags (2695 tags), 63ms for tag search -- acceptable
- FJMS/PGP overlap extent unknown -- affects dedup strategy in Phase 38

### Pending Todos

- JA diacritic dots normalization in search
- Migrate desktop corrections fetch to shared corrections_service
- Domain click behavior in browse metadata
- Pre-search domain filtering optimization

## Session Continuity

Last session: 2026-02-17
Stopped at: Phase 37 context gathered
Resume file: .planning/phases/37-fjms-catalog-descriptions/37-CONTEXT.md
Notes: Phase 37 context captured. Ready for /gsd:plan-phase 37.
