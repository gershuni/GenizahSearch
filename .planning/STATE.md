# Project State

## Project Reference

See: .planning/PROJECT.md (updated 2026-02-15)

**Core value:** Researchers can find what they need in the Genizah corpus
**Current focus:** v5.9.0 Multi-Source Image & Metadata Integration

## Current Position

Phase: 31 of 34 (Image Navigation & Indicators)
Plan: 1 of 2 in current phase
Status: Plan 31-01 complete, ready for Plan 31-02
Last activity: 2026-02-15 -- Completed 31-01 (Folio navigation bar and source indicators)

Progress: [███░░░░░░░] 27%

## Performance Metrics

**Velocity:**
- Total plans completed: 88 (across all milestones)
- Average duration: ~8 min
- Total execution time: ~9.5 hours

**By Milestone:**

| Milestone | Phases | Plans | Total Time |
|-----------|--------|-------|------------|
| v1 | 1-7 | 18 | 173 min |
| v5.6.0 | 8-12 | 25 | ~134 min |
| v5.7.0 | 14-17 | 14 | ~140 min |
| v5.7.2 | 18-21 | 11 | ~1 day |
| v5.7.3 | 22-24 | 3 | 6 min |
| v5.8.0 | 25-28 | 12 | 57 min |
| v5.9.0 | 29-34 | 4 | 12 min |
| Phase 31 P01 | 5min | 2 tasks | 5 files |

## Accumulated Context

### Decisions

See PROJECT.md Key Decisions table for full history.

- 29-01: Separate sidecar file (nli_crossref.db) rather than adding to fjms_enrichment.db -- different provenance and update cycles
- 29-01: All 25 NLI CSV columns stored as TEXT -- no filtering per user decision
- 29-01: CUDL label normalization: strip MS- prefix, split by dash, strip leading zeros, rejoin with dots between numerics
- 29-02: Followed FJMS service pattern exactly for NliCrossrefService -- same _find_project_root(), URI read-only mode, thread_safe param, singleton
- 29-02: get_image_sources combines NLI FGP and Cambridge checks in single call for efficient UI badge rendering
- 30-01: FGPImageNumberId values used directly as FL IDs (no transformation needed)
- 30-01: Local sidecar resolution added as first-try path, all existing network fallback logic preserved unchanged
- 30-02: Single crossref_svc initialization in enrich_metadata serves both Cambridge supplement and NLI FL ID paths
- 30-02: Cambridge supplement sets external_url on current_meta when found from sidecar, feeding into existing CUDL fetch logic
- [Phase 31]: Folio label falls back: crossref folio_label -> extract_folio_number -> Page N
- [Phase 31]: Source indicator chips are styled flat buttons with colored borders, not NiceGUI chip component

### Blockers/Concerns

- Phase 13 (Transcription Search) still deferred -- needs server-side index architecture
- FIST catalogs at unit (codex) level, not individual leaf level -- upstream FIST data design
- IMG-05 (library IIIF fallback) depends on discovering external IIIF endpoints for JTS/Manchester/BL -- may need research during planning

### Future Improvements

- FTS5 catalog search UI (schema ready in sidecar, deferred to future milestone)
- FJMS structured metadata search -- leverage TextualFrame tags with FTS5
- Transcription search (Phase 13, needs server-side index architecture)

## Session Continuity

Last session: 2026-02-15
Stopped at: Completed 31-01-PLAN.md
Resume file: None
Notes: 6 milestones shipped. v5.9.0 roadmap: 6 phases (29-34), 15 requirements. Phases 29-30 complete, Phase 31 Plan 01 done -- 5 plans delivered.
