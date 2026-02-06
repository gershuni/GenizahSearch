# Project State

## Project Reference

See: .planning/PROJECT.md (updated 2026-02-05)

**Core value:** Users can search and view PGP's human-curated transcriptions alongside existing content
**Current focus:** Phase 4.2 - Multi-Source Import (In progress)

## Current Position

Phase: 4.2 of 9 (Multi-Source Import)
Plan: 2 of 3 in current phase
Status: In progress
Last activity: 2026-02-06 - Completed 04.2-02: Import document sources

Progress: [████████░░] 91%

## Performance Metrics

**Velocity:**
- Total plans completed: 10
- Average duration: 8.6 min
- Total execution time: 86 min

**By Phase:**

| Phase | Plans | Total | Avg/Plan |
|-------|-------|-------|----------|
| 01-database-schema | 1 | 8 min | 8 min |
| 02-pgp-data-import | 2 | 33 min | 16.5 min |
| 03-document-service | 1 | 4 min | 4 min |
| 04-transcription-display | 3 | 11 min | 3.7 min |
| 04.1-separate-translations | 1 | 18 min | 18 min |
| 04.2-multi-source-import | 2 | 12 min | 6 min |

**Recent Trend:**
- Last 5 plans: 04-02 (4 min), 04-03 (3 min), 04.1-01 (18 min), 04.2-01 (5 min), 04.2-02 (7 min)
- Trend: Import plans run efficiently with batch upserts

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

**04.1-01 Decisions:**
- Filter at service layer (simplest fix, translations just don't appear)
- doc_relation='Digital Edition' required to show transcription
- 'Edition' type (2 records) treated as non-transcription for safety

**04.2-01 Decisions:**
- BIGSERIAL for id column (supports large scale)
- Composite unique on (pgpid, source_scholar, doc_relation) for deduplication
- content_length column for sorting/filtering without full-text scan
- sequence_order for ordering multiple sources of same type

**04.2-02 Decisions:**
- Import all 9,364 records (not deduplicated by pgpid) to preserve multiple editions
- Language detection via Hebrew character count (>10 Hebrew chars = Hebrew translation)
- Use languages field for Digital Edition, detect for Digital Translation

### Pending Todos

None.

### Blockers/Concerns

None - Phase 4.2 progressing well (document_sources populated with 9,364 records).

### Quick Tasks Completed

| # | Description | Date | Commit | Directory |
|---|-------------|------|--------|-----------|
| 001 | Move Recently Viewed list to top of sidebar | 2026-02-06 | a1d72ae | [001-recently-viewed-list-on-top](./quick/001-recently-viewed-list-on-top/) |
| 002 | Page loading progress bar | 2026-02-06 | 034dd67 | [002-browse-loading-progress-bar](./quick/002-browse-loading-progress-bar/) |

### Roadmap Evolution

- Phase 4.1 completed: Translations now filtered from transcription display
  - Added doc_relation column to documents table
  - Service layer filters out Digital Translations (961 records)
  - Only Digital Editions (6,127) show as "PGP Transcription"
  - UAT Gap 3 resolved

## Data Import Summary

PGP data successfully imported to Supabase:
- **Documents:** 7,090 (with transcriptions and metadata)
- **Document fragments:** 7,764 (sys_id links to GenizahSearch)
- **Unmatched fragments:** 15 (edge cases with unusual shelfmark patterns)

**Multi-Source Import (04.2-02):**
- **Document sources:** 9,364 total records
  - Digital Editions: 7,664
  - Digital Translations: 1,696 (Hebrew: 733, English: 963)
- **Documents with multiple sources:** 1,716 (24.2%)

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

Last session: 2026-02-06
Stopped at: Completed 04.2-02-PLAN.md (Document sources import)
Resume file: None
