---
phase: 07-joins-ui
plan: 01
subsystem: joins-data-layer
tags: [joins, PGP, document-fragments, data-merge, supabase]
requires:
  - 01-database-schema (document_fragments table)
  - 02-pgp-data-import (492 multi-fragment documents, 7,764 fragment links)
  - 03-document-service (get_document_for_fragment, get_fragments_for_document)
provides:
  - Unified joins data merging user pairwise + PGP multi-fragment joins
  - pgpid threaded from browse state to joins UI (eliminates redundant Supabase query)
affects:
  - 07-02 (inline metadata panel can leverage unified joins data)
tech-stack:
  added: []
  patterns:
    - Lazy import to avoid circular dependencies (document_service in joins_panel)
    - Normalized shelfmark deduplication (upper case comparison)
    - Prefix-based cache key invalidation
key-files:
  created: []
  modified:
    - web/components/joins_panel.py
    - web/pages/browse.py
key-decisions:
  - id: JOIN-01
    description: "PGP joins use id=None to prevent admin delete button display"
  - id: JOIN-02
    description: "Single-fragment PGP documents filtered by unique sys_id count > 1"
  - id: JOIN-03
    description: "Lazy import of document_service inside function body to avoid circular imports"
  - id: JOIN-04
    description: "Cache key includes pgpid for proper separation of cached results"
  - id: JOIN-05
    description: "Prefix-based cache invalidation handles variable pgpid in cache keys"
duration: 3 min
completed: 2026-02-06
---

# Phase 7 Plan 1: Unified Joins Data Layer Summary

**One-liner:** Merge PGP multi-fragment document joins with user pairwise joins in fetch_connected_fragments, with pgpid threaded from browse state.

## Performance

- Duration: ~3 minutes
- Autonomous execution, no checkpoints needed
- 2 tasks, 2 commits

## Accomplishments

1. **Extended `fetch_connected_fragments()`** to query both `fragment_joins` (user-created) and `document_fragments` (PGP multi-fragment) tables, merging results with source attribution and deduplication.

2. **Threaded `pgpid` from browse page state** through `create_joins_button`, `create_joins_dialog`, and `create_joins_indicator` to eliminate a redundant `get_document_for_fragment()` Supabase call on every joins load.

3. **Added `fragment_details` field** to the return dict, populating shelfmark-to-docid mapping that enables the dialog's title lookup for PGP fragments.

4. **Single-fragment PGP document filtering** ensures fragments that are the sole member of a PGP document do not show false "Related Fragments" in the UI.

5. **Updated cache invalidation** to handle the new cache key format with pgpid suffix using prefix-based matching.

## Task Commits

| Task | Name | Commit | Files |
|------|------|--------|-------|
| 1 | Extend fetch_connected_fragments to merge PGP document joins | f08b39d | web/components/joins_panel.py |
| 2 | Thread pgpid from browse page state to joins button | 10d3e76 | web/pages/browse.py |

## Files Modified

- `web/components/joins_panel.py` - Extended fetch_connected_fragments with PGP joins merge, updated all public functions (create_joins_button, create_joins_dialog, create_joins_indicator) with pgpid parameter, updated cache key format and invalidation logic
- `web/pages/browse.py` - Added pgpid_for_joins extraction from state.pgp_metadata and passed to create_joins_button

## Decisions Made

1. **JOIN-01: PGP joins use id=None** - PGP-sourced joins have `id: None` in the formatted join entry, which prevents the admin delete button from appearing (the delete button only shows when `direct_join_id` is truthy).

2. **JOIN-02: Single-fragment filter** - Only PGP documents with more than 1 unique `sys_id` in their fragments produce join entries. This prevents 6,598 single-fragment PGP documents from creating false "Related Fragments" indicators.

3. **JOIN-03: Lazy import pattern** - `get_document_for_fragment` and `get_fragments_for_document` are imported inside the function body to avoid circular import issues between joins_panel and document_service.

4. **JOIN-04: Cache key includes pgpid** - Format changed from `doc:{id}` to `doc:{id}:pgp:{pgpid}` to properly separate cached results when the same document has different PGP associations.

5. **JOIN-05: Prefix-based cache invalidation** - `invalidate_joins_cache` now uses prefix matching to clear all cache entries for a given document_id or shelfmark regardless of pgpid suffix.

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered

None.

## Next Phase Readiness

- Plan 07-02 (inline metadata panel) can now leverage the unified joins data
- The `fragment_details` field in the return dict provides shelfmark-to-docid mappings needed by the dialog
- All existing user-created joins continue to display unchanged (backward compatible)

## Self-Check: PASSED
