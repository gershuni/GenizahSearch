---
phase: 57-fist-joins-browse-search-mode
plan: 03
subsystem: desktop, web-api, settings
tags: [visual-similarity, desktop-cache, download, d-10, search-restrict]

requires:
  - phase: 57-fist-joins-browse-search-mode
    provides: VisualSimilarityService, frozen API contract, visual_similarity.db sidecar
provides:
  - Desktop VS dialog with orange theme, ranked suggestions, sort, browse/puzzle actions
  - Desktop versioned cache (DesktopVSCache) with server staleness detection
  - "Search in VS" and "Browse VS" modes integrated with pre_search_restrict_sys_ids
  - D-10 cross-cutting VS access from ResultDialog
  - Robust full DB download with SHA256 checksum, disk-space check, PRAGMA integrity_check
  - Download API endpoint with Content-Length and X-Checksum-SHA256 headers
affects: [browse enrichment, search execution, ResultDialog, settings page]

tech-stack:
  added: [vs_cache.db desktop cache sidecar]
  patterns: [versioned desktop cache with server staleness check, QThread download with 6-step robustness]

key-files:
  created: []
  modified:
    - genizah_app.py
    - genizah_translations.py
    - web/api.py
    - web/pages/settings.py

key-decisions:
  - "Always-enabled VS button in browse toolbar (on-demand server fetch, no pre-check needed)"
  - "VS restriction via pre_search_restrict_sys_ids (reuses existing compute_effective_restrict pipeline)"
  - "Browse VS implemented as restricted shelfmark wildcard search rather than custom result display"
  - "Desktop cache uses SQLite with version tracking table and full cache invalidation on version change"

requirements-completed: [JOIN-01, JOIN-02, JOIN-03]

duration: 11min
completed: 2026-03-30
---

# Phase 57 Plan 03: Desktop Visual Similarity + Settings Download Summary

**Desktop VS dialog with versioned cache, search/browse restriction modes, D-10 cross-cutting access from ResultDialog, robust full DB download with checksum/disk-space/integrity checks**

## Performance

- **Duration:** 11 min
- **Started:** 2026-03-30T04:23:12Z
- **Completed:** 2026-03-30T04:34:12Z
- **Tasks:** 3 (Tasks 1a, 1b, 2 auto-completed; Task 3 checkpoint pending)
- **Files modified:** 4

## Accomplishments

- DesktopVSCache: SQLite-backed versioned cache with server staleness detection (check_and_update_version on startup)
- VSFetchThread: non-blocking server fetch for per-manuscript visual suggestions
- Visual Similarity dialog: orange-themed QDialog with ranked table, sort by Rank/Library/Domain, Browse/Puzzle action buttons per row
- "Search in visual suggestions": restricts search to VS partner pool via pre_search_restrict_sys_ids with orange breadcrumb
- "Browse suggestions": shows VS partner pool as search results via restricted shelfmark wildcard search
- D-10 cross-cutting: VS action button in ResultDialog (orange themed, triggers _search_in_visual_suggestions)
- VSDownloadThread: 6-step robustness (HEAD size check, disk-space pre-check, streaming download with progress, SHA256 checksum verification, PRAGMA integrity_check, atomic rename)
- Download API endpoint (/api/visual_similarity_db) with Content-Length and X-Checksum-SHA256 headers
- Desktop settings: VS download section with progress bar, error handling, reset_vs_service on success
- Web settings: VS database status display showing pair/manuscript counts
- 17 Hebrew translations for all new VS strings

## Task Commits

1. **Task 1a: Desktop VS cache, fetch thread, dialog** - `be40fe3d` (feat)
2. **Task 1b: Search/Browse VS modes + D-10 access** - `9bb87717` (feat)
3. **Task 2: Settings download with robustness** - `62b684c5` (feat)

## Files Modified

- `genizah_app.py` - DesktopVSCache, VSFetchThread, VSDownloadThread, VS dialog, search/browse modes, D-10 ResultDialog access, settings download section
- `genizah_translations.py` - 17 Hebrew translations for VS feature strings
- `web/api.py` - /api/visual_similarity_db download endpoint with checksum headers
- `web/pages/settings.py` - VS database status display in status tab

## Decisions Made

- Always-enabled VS button in browse toolbar: since we support on-demand server fetch, no pre-check is needed to determine if VS data exists
- VS restriction via pre_search_restrict_sys_ids: reuses the existing compute_effective_restrict pipeline (no new restriction mechanism needed)
- Browse VS mode uses restricted shelfmark wildcard search rather than custom result display to leverage existing paginated search infrastructure
- Desktop cache invalidates entirely on server version change (simpler than per-manuscript staleness)

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] _populate_search_results does not exist**
- **Found during:** Task 1b (Browse VS implementation)
- **Issue:** Plan specified calling `_populate_search_results(results)` but this method does not exist in the desktop app
- **Fix:** Changed Browse VS to use restricted shelfmark wildcard search (`run_search()` with `pre_search_restrict_sys_ids`) which leverages existing paginated result display
- **Files modified:** genizah_app.py

**2. [Rule 2 - Scope] List items context menu not wired for D-10**
- **Found during:** Task 1b
- **Issue:** Plan specified adding VS to list items context menu, but list items have no context menu (only click/double-click handlers)
- **Decision:** Deferred -- adding a full context menu to list items would be an architectural change (Rule 4 territory). ResultDialog and browse dialog cover the primary D-10 use cases.

---

**Total deviations:** 2 (1 auto-fixed blocking, 1 scope decision)
**Impact on plan:** Browse VS mode works differently from plan (wildcard search vs custom display) but achieves same user outcome. List items D-10 deferred.

## Checkpoint Pending

**Task 3 (checkpoint:human-verify)** is pending user verification. The checkpoint covers:
- Visual Similarity dialog rendering and functionality
- Search/Browse VS restriction modes
- D-10 cross-cutting access from ResultDialog
- Desktop cache version staleness detection
- Settings download with robustness checks
- Hebrew translations

## Known Stubs

None -- all methods and UI elements are fully implemented.

## Self-Check: PASSED

All 4 modified files verified on disk. All 3 task commits verified in git log.

---
*Phase: 57-fist-joins-browse-search-mode*
*Completed: 2026-03-30 (Tasks 1a, 1b, 2; Task 3 checkpoint pending)*
