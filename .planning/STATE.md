# Project State

## Project Reference

See: .planning/PROJECT.md (updated 2026-02-05)

**Core value:** Users can search and view PGP's human-curated transcriptions alongside existing content
**Current focus:** Phase 4 - Transcription Display (In progress)

## Current Position

Phase: 4 of 7 (Transcription Display)
Plan: 1 of 2 in current phase (COMPLETE)
Status: In progress
Last activity: 2026-02-05 - Completed 04-01-PLAN.md

Progress: [█████░░░░░] 71%

## Performance Metrics

**Velocity:**
- Total plans completed: 5
- Average duration: 9.8 min
- Total execution time: 49 min

**By Phase:**

| Phase | Plans | Total | Avg/Plan |
|-------|-------|-------|----------|
| 01-database-schema | 1 | 8 min | 8 min |
| 02-pgp-data-import | 2 | 33 min | 16.5 min |
| 03-document-service | 1 | 4 min | 4 min |
| 04-transcription-display | 1 | 4 min | 4 min |

**Recent Trend:**
- Last 5 plans: 02-01 (3 min), 02-02 (30 min), 03-01 (4 min), 04-01 (4 min)
- Trend: UI integration plans executing quickly due to well-prepared service layer

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

**04-01 Decisions:**
- PGP as first menu item (above V0.8) when available
- Green verified icon and styling for PGP version
- Auto-select PGP on page load as default
- pgp_transcription dict structure: {content, attribution, pgp_url, pgpid}

### Pending Todos

None.

### Blockers/Concerns

None - version selector integration complete, ready for multi-fragment display.

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

## UI Integration Summary

Version selector now supports PGP transcriptions:
- PGP option appears first in menu with verified icon when available
- Auto-selects PGP as default version on page load
- Attribution (scholar name) displayed in menu and notification
- Hebrew translations added for all PGP UI strings

## Session Continuity

Last session: 2026-02-05 20:10
Stopped at: Completed 04-01-PLAN.md (Version Selector Integration)
Resume file: None
