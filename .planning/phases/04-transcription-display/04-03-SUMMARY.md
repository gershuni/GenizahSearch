---
phase: 04-transcription-display
plan: 03
subsystem: ui
tags: [regex, text-parsing, recto-verso, transcription]

# Dependency graph
requires:
  - phase: 04-01
    provides: PGP transcription integration in version selector
  - phase: 04-02
    provides: Clickable PGP link in version selector
provides:
  - Section parsing utility for recto/verso markers
  - Page-filtered transcription display
affects: [05-search-integration, 06-metadata-display]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Regex-based section parsing for transcription text"
    - "Page number to section type mapping"

key-files:
  created: []
  modified:
    - web/document_service.py
    - web/pages/browse.py

key-decisions:
  - "Preamble text (before first marker) goes to recto by default"
  - "Pages beyond 2 show full transcription as fallback for multi-fragment"
  - "Store full_content alongside filtered content for future reference"

patterns-established:
  - "Section parsing separates Recto/Verso including margin variants"
  - "Page 1 = recto, Page 2 = verso for single-fragment documents"

# Metrics
duration: 3min
completed: 2026-02-05
---

# Phase 4 Plan 03: Section Parsing Summary

**Regex-based recto/verso section parsing added to document_service.py, integrated into browse.py for page-filtered transcription display**

## Performance

- **Duration:** 3 min
- **Started:** 2026-02-05T21:00:18Z
- **Completed:** 2026-02-05T21:02:49Z
- **Tasks:** 2
- **Files modified:** 2

## Accomplishments
- Added `parse_transcription_sections()` function to parse PGP transcriptions by Recto/Verso markers
- Added `get_section_for_page()` function to map page numbers to appropriate section content
- Updated browse.py to filter transcription content based on current page number
- Handles complex section patterns (margin notes, address lines) correctly

## Task Commits

Each task was committed atomically:

1. **Task 1: Add section parsing utility to document_service.py** - `a222b1a` (feat)
2. **Task 2: Filter transcription by page in browse.py** - `d23fa28` (feat)

**Plan metadata:** (pending)

## Files Created/Modified
- `web/document_service.py` - Added parse_transcription_sections and get_section_for_page functions with regex pattern matching
- `web/pages/browse.py` - Updated import and PGP transcription loading to filter by page.p_num

## Decisions Made
- **Preamble handling:** Text before first section marker goes to recto by default
- **Fallback behavior:** Pages beyond 2 (multi-fragment case) show full transcription for now
- **Data structure:** Store both `full_content` and filtered `content` in pgp_transcription dict for flexibility

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered

None

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness
- Phase 4 (Transcription Display) now complete with all 3 plans
- All success criteria satisfied:
  - PGP transcription shows in version selector with verified icon
  - Source attribution displays ("Transcription by [scholar name]")
  - Clickable "View on PGP" link opens original document
  - Recto/verso splitting correctly filters content per page
- Ready for Phase 5 (Search Integration)

---
*Phase: 04-transcription-display*
*Completed: 2026-02-05*
