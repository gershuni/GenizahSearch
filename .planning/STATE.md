# Project State

## Project Reference

See: .planning/PROJECT.md (updated 2026-02-05)

**Core value:** Users can search and view PGP's human-curated transcriptions alongside existing content
**Current focus:** Phase 5 - Search Integration (Ready to start)

## Current Position

Phase: 5 of 7 (Search Integration)
Plan: 0 of 1 in current phase
Status: Ready to start
Last activity: 2026-02-05 - Completed 04-03-PLAN.md (recto/verso splitting)

Progress: [██████░░░░] 86%

## Performance Metrics

**Velocity:**
- Total plans completed: 7
- Average duration: 7.7 min
- Total execution time: 56 min

**By Phase:**

| Phase | Plans | Total | Avg/Plan |
|-------|-------|-------|----------|
| 01-database-schema | 1 | 8 min | 8 min |
| 02-pgp-data-import | 2 | 33 min | 16.5 min |
| 03-document-service | 1 | 4 min | 4 min |
| 04-transcription-display | 3 | 11 min | 3.7 min |

**Recent Trend:**
- Last 5 plans: 02-02 (30 min), 03-01 (4 min), 04-01 (4 min), 04-02 (4 min), 04-03 (3 min)
- Trend: UI integration and gap closure plans executing quickly

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

**04-02 Decisions:**
- External link icon in PGP menu item for TRANS-03
- stop_propagation() to prevent menu close when clicking link
- Tooltip with tr('View on PGP') for accessibility

**04-03 Decisions:**
- Preamble text (before first marker) goes to recto by default
- Pages beyond 2 show full transcription as fallback for multi-fragment
- Store full_content alongside filtered content for future reference

### Pending Todos

None.

### Blockers/Concerns

None - Phase 4 (Transcription Display) complete, ready for Phase 5.

## Data Import Summary

PGP data successfully imported to Supabase:
- **Documents:** 7,090 (with transcriptions and metadata)
- **Document fragments:** 7,764 (sys_id links to GenizahSearch)
- **Unmatched fragments:** 15 (edge cases with unusual shelfmark patterns)

## Service Layer Summary

Document service provides 6 functions for accessing PGP data:
- `get_document_for_fragment(sys_id)` - Look up document by fragment sys_id
- `get_fragments_for_document(pgpid)` - Get all fragments in sequence order
- `get_transcription_for_document(pgpid)` - Get transcription text
- `get_document_metadata(pgpid)` - Get document metadata (type, tags, dates, etc.)
- `parse_transcription_sections(transcription)` - Parse by Recto/Verso markers
- `get_section_for_page(transcription, page_num)` - Get section for page number

All functions handle errors gracefully (return None/empty list, never raise).

## UI Integration Summary

Version selector now supports PGP transcriptions:
- PGP option appears first in menu with verified icon when available
- Auto-selects PGP as default version on page load
- Attribution (scholar name) displayed in menu and notification
- Clickable "View on PGP" link opens original PGP document in new tab
- Recto/verso splitting shows page-appropriate content
- Hebrew translations added for all PGP UI strings

**Requirements Satisfied:**
- TRANS-01: User can view PGP transcription on browse page
- TRANS-02: User sees transcription source attribution
- TRANS-03: User can click through to original PGP document page
- PGP transcription displays correctly per page (recto/verso split)

## Session Continuity

Last session: 2026-02-05 21:03
Stopped at: Completed 04-03-PLAN.md
Resume file: None
