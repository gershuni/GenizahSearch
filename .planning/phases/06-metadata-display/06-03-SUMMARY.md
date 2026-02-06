---
phase: 06-metadata-display
plan: 03
subsystem: search
tags: [tags, search, jsonb, gin-index]

requires:
  - phase: 06-01
    provides: "Tags column with GIN index"
provides:
  - "Tag-based search via /search?tag=X"
  - "get_fragments_by_tag() service function"
  - "Tag results with viewer pane preview"
affects: [web/document_service.py, web/main.py, web/pages/search.py, genizah_translations.py]

tech-stack:
  added: []
  patterns: ["filter('tags', 'cs', json.dumps([tag])) for JSONB contains query"]

key-files:
  created: []
  modified:
    - web/document_service.py
    - web/main.py
    - web/pages/search.py
    - genizah_translations.py

key-decisions:
  - "Use filter('tags', 'cs', json.dumps([tag])) instead of .contains() (Supabase Python client bug)"
  - "Filter tag results to only browseable fragments (429/7218 not in local index)"
  - "Tag results use viewer pane with text preview (not direct navigation)"
  - "Translate buttons on description in both result cards and preview pane"

duration: 15min
completed: 2026-02-06
---

# Phase 6 Plan 03: Tag-Based Search Summary

**Clicking a PGP tag navigates to search page with filtered results, viewer pane preview with manuscript text**

## Performance

- **Duration:** 15 min (including debugging and user feedback)
- **Tasks:** 2 (1 auto + 1 human-verify checkpoint)
- **Files modified:** 4

## Accomplishments
- Added get_fragments_by_tag() service function with GIN-indexed JSONB query
- Search route accepts ?tag=X parameter
- Tag results render as clickable cards with shelfmark, document type, description
- Clicking a result loads preview in viewer pane with first page text
- Preview pane has tabs (Full Text + Metadata) matching regular search UX
- Filter out non-browseable fragments (~6% of PGP fragments not in local index)
- Translate buttons on description text in cards and viewer
- Hebrew translations for tag search labels

## Task Commits

1. **Task 1: Implement tag search** - `d668c8a` (feat)
2. **Fix: JSONB query syntax** - `5d940f4` (fix)
3. **Fix: Filter non-browseable fragments** - `79c7d76` (fix)
4. **Fix: Use viewer pane** - `5e03d68` (fix)
5. **Fix: Show manuscript text in viewer** - `9cb665f` (fix)
6. **Add translate buttons** - `2540d5c` (feat)

## Files Modified
- `web/document_service.py` - get_fragments_by_tag() function
- `web/main.py` - Search route with tag parameter
- `web/pages/search.py` - Tag results rendering, viewer pane, translate buttons
- `genizah_translations.py` - Hebrew translations for tag search

## Decisions Made
- json.dumps workaround for Supabase Python client JSONB contains
- Filter to local index for browseable results only
- Viewer pane pattern (not direct navigation) for consistent UX
- Translate buttons on description fields

## Issues Encountered

1. **Supabase .contains() bug** - `.contains('tags', ['communal'])` sends invalid JSON. Fixed with `.filter('tags', 'cs', json.dumps([tag]))`
2. **Non-browseable fragments** - 429 of 7,218 PGP fragments not in libraries.csv. Filtered out to prevent dead links.

---
*Phase: 06-metadata-display*
*Completed: 2026-02-06*
