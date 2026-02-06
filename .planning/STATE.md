# Project State

## Project Reference

See: .planning/PROJECT.md (updated 2026-02-05)

**Core value:** Users can search and view PGP's human-curated transcriptions alongside existing content
**Current focus:** Phase 7 - Joins UI

## Current Position

Phase: 7 of 9 (Joins UI)
Plan: 1 of 2 in current phase
Status: In progress
Last activity: 2026-02-06 - Completed 07-01-PLAN.md (Unified Joins Data Layer)

Progress: [################] ~89%

## Performance Metrics

**Velocity:**
- Total plans completed: 16
- Average duration: 9.3 min
- Total execution time: 153 min

**By Phase:**

| Phase | Plans | Total | Avg/Plan |
|-------|-------|-------|----------|
| 01-database-schema | 1 | 8 min | 8 min |
| 02-pgp-data-import | 2 | 33 min | 16.5 min |
| 03-document-service | 1 | 4 min | 4 min |
| 04-transcription-display | 3 | 11 min | 3.7 min |
| 04.1-separate-translations | 1 | 18 min | 18 min |
| 04.2-multi-source-import | 3 | 37 min | 12.3 min |
| 05-search-integration | 1 | 4 min | 4 min |
| 06-metadata-display | 3 | 35 min | 11.7 min |
| 07-joins-ui | 1 | 3 min | 3 min |

**Recent Trend:**
- Last 5 plans: 05-01 (4 min), 06-01 (5 min), 06-02 (15 min), 06-03 (15 min), 07-01 (3 min)
- Trend: Pure data-layer plans execute fastest; interactive UI plans take longer

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

**04.2-03 Decisions:**
- Group menu into Transcriptions and Translations sections
- Filter sources by page content (not just page number)
- Show translations on all pages with editions (full-document content)
- Return None for pages without content (UI can filter appropriately)

**05-01 Decisions:**
- Batch lookup via `.in_()` query for performance (not N+1)
- Set return type for O(1) membership checks
- Icon after library badge, before shelfmark
- Green "description" icon consistent with PGP styling

### Pending Todos

None.

**06-01 Decisions:**
- ALTER TABLE IF NOT EXISTS for safe re-runnable migrations
- Empty strings converted to None for clean NULL storage

**06-02 Decisions:**
- PGP button next to Ktiv in header (user preference)
- Full description (no truncation)
- `or ''` pattern for None-safe dict.get
- Translate buttons on metadata text fields

**06-03 Decisions:**
- filter('tags', 'cs', json.dumps([tag])) for JSONB contains (Supabase client bug workaround)
- Filter tag results to only browseable fragments (429/7218 not in local index)
- Tag results use viewer pane with text preview (not direct navigation)
- Translate buttons on description in cards and viewer

**07-01 Decisions:**
- PGP joins use id=None to prevent admin delete button display
- Single-fragment PGP documents filtered by unique sys_id count > 1
- Lazy import of document_service inside function body to avoid circular imports
- Cache key includes pgpid for proper separation; prefix-based invalidation

### Blockers/Concerns

None - Phase 7 Plan 01 complete. Ready for Plan 02.

### Quick Tasks Completed

| # | Description | Date | Commit | Directory |
|---|-------------|------|--------|-----------|
| 001 | Move Recently Viewed list to top of sidebar | 2026-02-06 | a1d72ae | [001-recently-viewed-list-on-top](./quick/001-recently-viewed-list-on-top/) |
| 002 | Page loading progress bar | 2026-02-06 | 034dd67 | [002-browse-loading-progress-bar](./quick/002-browse-loading-progress-bar/) |
| 003 | Fix progress bar navigation + parallels | 2026-02-06 | 7188c92 | [003-fix-progress-bar-navigation-parallels](./quick/003-fix-progress-bar-navigation-parallels/) |
| 004 | Clean up server management script | 2026-02-06 | 8550302 | [004-clean-up-server-management-script](./quick/004-clean-up-server-management-script/) |
| 005 | Find unused functions report | 2026-02-06 | daef528 | [005-find-unused-functions](./quick/005-find-unused-functions/) |

### Roadmap Evolution

- Phase 7 Plan 01 completed: Unified joins data layer
  - fetch_connected_fragments merges user pairwise + PGP multi-fragment joins
  - pgpid threaded from browse state to joins button (eliminates redundant query)
  - Single-fragment PGP documents filtered out (no false "Related Fragments")
  - fragment_details populated for dialog shelfmark-to-docid lookup
  - JOIN-01 through JOIN-05 requirements satisfied

- Phase 6 completed: Metadata display with tag-based search
  - 4 new metadata columns added (languages, dates)
  - PGP metadata section in browse page (type, tags, description, dates)
  - PGP button in header bar + external links
  - Tag-based search via /search?tag=X with GIN-indexed JSONB query
  - Translate buttons on metadata fields
  - META-01 through META-04 requirements satisfied

- Phase 5 Plan 01 completed: PGP transcription indicator in search results
  - Batch lookup function added to document_service.py
  - Green icon appears on search results with transcriptions
  - Hebrew translation added
  - TRANS-04 requirement satisfied

- Phase 4.2 completed: Multi-source UI with transcriptions and translations
  - Version selector shows all available transcriptions grouped by scholar
  - Translations section shows Hebrew/English options
  - Page-aware filtering ensures correct content per page
  - UAT verified and approved

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

Document service provides 11 functions for accessing PGP data:
- `get_document_for_fragment(sys_id)` - Look up document by fragment sys_id
- `get_fragments_for_document(pgpid)` - Get all fragments in sequence order
- `get_transcription_for_document(pgpid)` - Get transcription text
- `get_document_metadata(pgpid)` - Get document metadata (type, tags, dates, etc.)
- `parse_transcription_sections(transcription)` - Parse by Recto/Verso markers
- `get_section_for_page(transcription, page_num)` - Get section for page number
- `get_sources_for_document(pgpid)` - Get all sources (editions + translations)
- `get_editions_for_document(pgpid)` - Get Digital Editions only
- `get_translations_for_document(pgpid)` - Get Digital Translations only
- `get_sys_ids_with_transcriptions(sys_ids)` - Batch check for transcription availability
- `get_fragments_by_tag(tag)` - Find all fragments with a given PGP tag

All functions handle errors gracefully (return None/empty list/set, never raise).

## UI Integration Summary

Version selector now supports PGP transcriptions with multi-source display:
- PGP Transcriptions section groups all editions by scholar name
- Translations section shows Hebrew/English options separately
- Clicking sources switches displayed content
- Page-aware filtering shows editions only on pages with content
- Translations display on all pages (full-document content)
- Auto-selects first PGP edition as default on page load
- Hebrew translations added for all multi-source UI strings

**Requirements Satisfied:**
- TRANS-01: User can view PGP transcription on browse page
- TRANS-02: User sees transcription source attribution
- TRANS-03: User can click through to original PGP document page
- TRANS-04: User sees "has transcription" indicator in search results
- MULTI-01: User can switch between multiple scholars' editions
- MULTI-02: User can view Hebrew/English translations
- PGP transcription displays correctly per page (recto/verso split)
- META-01: Document type displays on browse page
- META-02: Date information displays when available
- META-03: English description displays when available
- META-04: Tags display and are clickable (tag-based search)

## Session Continuity

Last session: 2026-02-06
Stopped at: Completed 07-01-PLAN.md (Unified Joins Data Layer)
Resume file: None
Notes: Phase 7 Plan 01 complete. Plan 02 (inline metadata panel) is next.
