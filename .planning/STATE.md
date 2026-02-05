# Project State

## Project Reference

See: .planning/PROJECT.md (updated 2026-02-05)

**Core value:** Users can search and view PGP's human-curated transcriptions alongside existing content
**Current focus:** Phase 3 - Document Service (COMPLETE)

## Current Position

Phase: 3 of 7 (Document Service)
Plan: 1 of 1 in current phase (COMPLETE)
Status: Phase complete
Last activity: 2026-02-05 - Completed 03-01-PLAN.md

Progress: [████░░░░░░] 57%

## Performance Metrics

**Velocity:**
- Total plans completed: 4
- Average duration: 11.3 min
- Total execution time: 45 min

**By Phase:**

| Phase | Plans | Total | Avg/Plan |
|-------|-------|-------|----------|
| 01-database-schema | 1 | 8 min | 8 min |
| 02-pgp-data-import | 2 | 33 min | 16.5 min |
| 03-document-service | 1 | 4 min | 4 min |

**Recent Trend:**
- Last 5 plans: 01-01 (8 min), 02-01 (3 min), 02-02 (30 min), 03-01 (4 min)
- Trend: Service layer plan was fast (no authentication gates, straightforward implementation)

*Updated after each plan completion*

## Accumulated Context

### Decisions

Decisions are logged in PROJECT.md Key Decisions table.
Recent decisions affecting current work:

- Document entity for joins only (single-fragment manuscripts unchanged)
- PGP transcription as version source (extends existing selector)
- Sequential images for joined docs (simpler than tabs)
- PGP joins only for v1 (NLI BifolioWith deferred - image-level, not fragment joins)
- Transcription search deferred to v2 (display-only for v1)

**01-01 Decisions:**
- pgpid as natural PRIMARY KEY (matches PGP data source)
- GENERATED pgp_url column (computed URL avoids duplication)
- JSONB tags with GIN index (flexible filtering without join table)
- Denormalized shelfmark in document_fragments (display optimization)

**02-01 Decisions:**
- IF NOT EXISTS pattern via DO block for safe migration re-runs

**02-02 Decisions:**
- Batch size 500 for Supabase upserts (optimal per research)
- Deduplicate fragments by (document_id, sys_id) before import
- Single-fragment docs use sys_id from transcriptions_linked.csv directly
- Multi-fragment docs look up sys_id for each fragment part
- Two-pass import pattern: documents first, then FK-dependent fragments

**03-01 Decisions:**
- Two-step document lookup (fragment -> document tables)
- None semantics for missing/empty data (consistent API)
- Service layer pattern isolates query details from UI

### Pending Todos

None.

### Blockers/Concerns

None - document service is ready for UI integration phases.

## Data Import Summary

PGP data successfully imported to Supabase:
- **Documents:** 7,090 (with transcriptions and metadata)
- **Document fragments:** 7,764 (sys_id links to GenizahSearch)
- **Unmatched fragments:** 15 (edge cases with unusual shelfmark patterns)

## Service Layer Summary

Document service provides 4 functions for accessing PGP data:
- `get_document_for_fragment(sys_id)` - Look up document by fragment sys_id
- `get_fragments_for_document(pgpid)` - Get all fragments in sequence order
- `get_transcription_for_document(pgpid)` - Get transcription text
- `get_document_metadata(pgpid)` - Get document metadata (type, tags, dates, etc.)

All functions handle errors gracefully (return None/empty list, never raise).

## Session Continuity

Last session: 2026-02-05 19:43
Stopped at: Completed 03-01-PLAN.md (Document Service phase complete)
Resume file: None
