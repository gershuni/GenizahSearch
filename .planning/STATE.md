# Project State

## Project Reference

See: .planning/PROJECT.md (updated 2026-02-16)

**Core value:** Researchers can find what they need in the Genizah corpus
**Current focus:** v6.0.0 Local Data Architecture -- Phase 35 (PGP Sidecar Export)

## Current Position

Phase: 35 of 38 (PGP Sidecar Export)
Plan: 1 of 1 in current phase (COMPLETE)
Status: Phase 35 complete
Last activity: 2026-02-17 -- Completed 35-01 PGP sidecar export

Progress: [##########] 100% (Phase 35)

## Performance Metrics

**Velocity:**
- Total plans completed: 105 (across 7 milestones)
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
| v6.0.0 | 35-38 | 1+ | 6 min |

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

### Blockers/Concerns

- Phase 13 (Transcription Search) still deferred -- needs server-side index architecture
- Tags json_each() performance on 35K rows needs benchmarking during Phase 36
- FJMS/PGP overlap extent unknown -- affects dedup strategy in Phase 38

### Pending Todos

- JA diacritic dots normalization in search
- Migrate desktop corrections fetch to shared corrections_service
- Domain click behavior in browse metadata
- Pre-search domain filtering optimization

## Session Continuity

Last session: 2026-02-17
Stopped at: Completed 35-01-PLAN.md
Resume file: .planning/phases/35-pgp-sidecar-export/35-01-SUMMARY.md
Notes: Phase 35 complete (1 plan). pgp.db sidecar (146.6 MB, 104K rows) ready for Phase 36 service rewrite.
