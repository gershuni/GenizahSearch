---
phase: 52-community-integration
plan: 01
subsystem: api
tags: [supabase, storage, rls, puzzle, publish]

requires:
  - phase: 50-pre-built-index-distribution-with-in-app-download
    provides: PuzzleDocument model, puzzle_export compose functions
provides:
  - Puzzle publish service (publish, unpublish, list, detail, fork, resolve_author_profiles)
  - Supabase schema SQL for published_joins and published_join_fragments tables
  - Storage bucket configuration for puzzle-images
affects: [52-02, 52-03, web-puzzle-page, desktop-puzzle-page]

tech-stack:
  added: []
  patterns: [client-parameter injection for web/desktop compatibility, batch profile resolution]

key-files:
  created:
    - shared/puzzle_publish_service.py
    - tests/test_puzzle_publish.py
  modified:
    - docs/guides/SUPABASE_GUIDE.md

key-decisions:
  - "Client-parameter pattern: each function accepts a client arg (no direct import of web/supabase_client.py) for desktop+web reuse"
  - "No separate update_published_join function: publish_join handles re-publish via upsert"
  - "Fragment index rebuilt on each publish (delete + insert) for simplicity and correctness"

patterns-established:
  - "Client injection: publish service functions accept Supabase client as first arg"
  - "Batch profile resolution: resolve_author_profiles() used by list/detail/fragment-lookup"

requirements-completed: [COMM-02, COMM-03]

duration: 3min
completed: 2026-03-17
---

# Phase 52 Plan 01: Publish Service + Schema Summary

**Supabase publish service with 7 CRUD functions, 9 mocked tests, and full RLS schema for community puzzle join sharing**

## Performance

- **Duration:** 3 min
- **Started:** 2026-03-17T10:19:11Z
- **Completed:** 2026-03-17T10:22:26Z
- **Tasks:** 1 of 2 (Task 2 is human-action checkpoint)
- **Files modified:** 3

## Accomplishments
- Implemented shared/puzzle_publish_service.py with all 7 exported functions
- All 9 unit tests pass with fully mocked Supabase client
- Documented complete Supabase schema SQL with RLS policies for both tables + storage bucket
- Batch query pattern in get_published_joins_for_fragment (no N+1)

## Task Commits

Each task was committed atomically:

1. **Task 1 RED:** Failing tests for publish service - `4eab271a` (test)
2. **Task 1 GREEN:** Implement publish service + schema docs - `ae2c4d74` (feat)

_TDD task: test-first, then implementation._

## Files Created/Modified
- `shared/puzzle_publish_service.py` - All publish/unpublish/list/detail/fork/resolve operations
- `tests/test_puzzle_publish.py` - 9 unit tests with mocked Supabase
- `docs/guides/SUPABASE_GUIDE.md` - Schema SQL, RLS policies, storage bucket config

## Decisions Made
- Client-parameter injection pattern for web + desktop compatibility (no web-specific imports)
- No separate update function; publish_join uses upsert for both create and re-publish
- Fragment index rebuilt on each publish via delete + insert cycle

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered

None.

## User Setup Required

**Supabase tables and storage bucket must be created manually.** Task 2 (human-action checkpoint) provides step-by-step instructions for running the SQL in Supabase Dashboard.

## Next Phase Readiness
- Publish service ready for integration into web puzzle page (52-02)
- Supabase tables must be created before integration testing
- Storage bucket puzzle-images must be created with public read access

---
*Phase: 52-community-integration*
*Completed: 2026-03-17*
