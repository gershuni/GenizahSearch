---
phase: 56-lightweight-browse-first-render
plan: 01
subsystem: web
tags: [browse, performance, tantivy, csv_bank, crossref, enrichment, phase-b]

# Dependency graph
requires:
  - phase: 55-search-within-results
    provides: search infrastructure and browse page foundation
provides:
  - Slim Phase A browse service layer (zero SQLite calls)
  - Phase B browse enrichment in _load_enrichment (parallel with PGP/FJMS)
  - Graceful degradation for deferred fields (attribution, folio labels, source badges)
affects: [browse, search, seo-crawlers]

# Tech tracking
tech-stack:
  added: []
  patterns: [two-phase-browse-render, deferred-enrichment-pattern]

key-files:
  created: []
  modified:
    - web/services.py
    - web/pages/browse.py

key-decisions:
  - "Re-render update_content() after Phase B enrichment instead of per-field enrichment_refs containers -- simpler and guarantees no visual regression"
  - "Deduplicate physical_metadata by reading from existing fetch_crossref() result instead of separate query in fetch_browse_enrichment()"
  - "Move Oxford translations from Phase A to Phase B since oxford_part_metadata is now deferred"

patterns-established:
  - "Two-phase browse render: Phase A = Tantivy + csv_bank (microseconds), Phase B = SQLite enrichment (crossref, Oxford, Cambridge, attribution)"
  - "fetch_browse_enrichment() runs in asyncio.gather alongside fetch_pgp/fetch_fjms/fetch_crossref"

requirements-completed: [BROWSE-PERF-01, BROWSE-PERF-02, BROWSE-PERF-03, BROWSE-PERF-04, BROWSE-PERF-05]

# Metrics
duration: 15min
completed: 2026-03-29
---

# Phase 56 Plan 01: Lightweight Browse First Render Summary

**Split browse page into fast Phase A (Tantivy + csv_bank, zero SQLite) and deferred Phase B (crossref + Oxford + Cambridge + attribution enrichment)**

## Performance

- **Duration:** 15 min
- **Started:** 2026-03-29T06:47:35Z
- **Completed:** 2026-03-29T07:02:55Z
- **Tasks:** 2
- **Files modified:** 2

## Accomplishments
- Removed all SQLite/crossref calls from browse service hot path (get_browse_page, get_metadata_only_browse_page, get_browse_page_by_fl) -- net removal of 344 lines
- Added fetch_browse_enrichment() to Phase B that fetches attribution cascade, Oxford codicological, Cambridge MARC, crossref image sources/folios/viewer URLs, and external images in parallel
- Deferred Oxford translations to Phase B since oxford_part_metadata is no longer available in Phase A
- First paint now shows image + shelfmark + title + NLI attribution with zero I/O beyond Tantivy + csv_bank

## Task Commits

Each task was committed atomically:

1. **Task 1: Slim service layer -- remove SQLite/crossref from hot path** - `73233f05` (feat)
2. **Task 2: Expand Phase B enrichment and add UI graceful degradation** - `3fd15f4b` (feat)

## Files Created/Modified
- `web/services.py` - Slimmed get_browse_page, get_metadata_only_browse_page, get_browse_page_by_fl to return only Tantivy + csv_bank data
- `web/pages/browse.py` - Added fetch_browse_enrichment() to _load_enrichment(), apply enrichment to page, re-render after Phase B

## Decisions Made
- Used update_content() re-render after Phase B instead of individual enrichment_refs containers for each deferred field -- this is simpler and guarantees no visual regression since the full UI rebuilds with all data populated
- Deduplicated physical_metadata by reading from crossref_data (existing fetch_crossref) rather than adding a separate get_physical_metadata call in fetch_browse_enrichment
- Moved Oxford translation fetch from Phase A to Phase B since oxford_part_metadata is now empty in Phase A

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 2 - Missing Critical] Re-render approach instead of per-field enrichment_refs**
- **Found during:** Task 2 (Phase B enrichment)
- **Issue:** Plan called for adding 7+ enrichment_refs containers (attribution_label, oxford_part_container, library_viewer_container, external_links_container, folio_label, folio_selector_container, image_source_container, physical_metadata_container) with individual update logic in _update_enrichment_sections(). This would require modifying 200+ lines of update_content() and duplicating complex rendering logic.
- **Fix:** After applying enrichment to state.current_page, call update_content() for a full re-render with complete data, then _update_enrichment_sections() for PGP/FJMS containers as before. This guarantees correctness with minimal code changes.
- **Files modified:** web/pages/browse.py
- **Verification:** Import check passes, BrowsePage defaults verified
- **Committed in:** 3fd15f4b

---

**Total deviations:** 1 auto-fixed (1 missing critical)
**Impact on plan:** Deviation simplifies implementation while achieving same outcome (fast Phase A, complete Phase B). No functionality loss.

## Issues Encountered
None

## User Setup Required
None - no external service configuration required.

## Known Stubs
None -- all data paths are wired. Phase A shows defaults, Phase B populates full data.

## Next Phase Readiness
- Browse page now has two-phase rendering optimized for crawlers and fast first paint
- Ready for production deployment
- SEO crawlers hitting 255K sitemap URLs will get fast Phase A responses

---
*Phase: 56-lightweight-browse-first-render*
*Completed: 2026-03-29*
