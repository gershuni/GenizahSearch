---
phase: 47-foundation-background-removal
plan: 01
subsystem: database
tags: [sqlite, dataclass, sidecar, puzzle, joins, concurrency]

requires: []
provides:
  - PuzzleDocument and PuzzleFragment dataclasses with JSON roundtrip
  - joins.db sidecar service with CRUD, concurrency-safe writes, fragment index
affects: [47-02, 47-03, 48-canvas-desktop, 49-canvas-web, 50-index-distribution]

tech-stack:
  added: []
  patterns: [puzzle-document-model, joins-db-sidecar, write-lock-concurrency]

key-files:
  created:
    - shared/puzzle_model.py
    - shared/puzzle_service.py
    - tests/test_puzzle_model.py
    - tests/test_puzzle_service.py
  modified: []

key-decisions:
  - "Explicit path validation: auto-create dirs only for auto-detected paths, fail gracefully for invalid explicit paths"
  - "thread_safe=True required for concurrent writes (check_same_thread=False), matching nli_crossref_service pattern"
  - "Fragment index rebuild on save (delete+re-insert) for simplicity over incremental diff"

patterns-established:
  - "PuzzleDocument/PuzzleFragment: shared dataclass model for puzzle state, JSON serializable"
  - "PuzzleService singleton: get_puzzle_service()/reset_puzzle_service() with graceful degradation"
  - "Write lock pattern: threading.Lock for all SQLite writes, WAL mode for concurrent reads"

requirements-completed: [BGRM-02]

duration: 3min
completed: 2026-03-16
---

# Phase 47 Plan 01: Data Model and Persistence Summary

**PuzzleDocument/PuzzleFragment dataclasses with joins.db SQLite sidecar, concurrency-safe CRUD, and fragment reverse-index lookups**

## Performance

- **Duration:** 3 min
- **Started:** 2026-03-16T03:07:41Z
- **Completed:** 2026-03-16T03:10:57Z
- **Tasks:** 2
- **Files modified:** 4

## Accomplishments
- PuzzleFragment dataclass with 10 fields (position, rotation, scale, flip, bg_removal_threshold)
- PuzzleDocument dataclass with UUID id, title, notes, join_type, fragments list, timestamps, JSON roundtrip
- joins.db sidecar service with WAL mode, busy_timeout=5000, foreign_keys=ON
- Fragment index table (join_document_fragments) for reverse lookups by fl_id or sys_id
- 17 tests total: 6 model tests + 11 service tests including 3-thread concurrent write test

## Task Commits

Each task was committed atomically:

1. **Task 1: PuzzleDocument/PuzzleFragment data model with JSON roundtrip** - `90d9d138` (feat)
2. **Task 2: joins.db sidecar service with CRUD, concurrency, and fragment index** - `dd6a205a` (feat)

## Files Created/Modified
- `shared/puzzle_model.py` - PuzzleFragment and PuzzleDocument dataclasses with to_json/from_json
- `shared/puzzle_service.py` - PuzzleService with joins.db CRUD, write lock, fragment index, singleton
- `tests/test_puzzle_model.py` - 6 tests for model fields, defaults, roundtrip serialization
- `tests/test_puzzle_service.py` - 11 tests for schema, CRUD, concurrency, fragment index, degradation

## Decisions Made
- Explicit path validation: auto-create dirs only for auto-detected paths, fail gracefully for invalid explicit paths (Windows path resolution quirk)
- thread_safe=True required for concurrent writes, matching existing nli_crossref_service pattern
- Fragment index uses delete+re-insert on save for simplicity

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Fixed graceful degradation test for Windows path resolution**
- **Found during:** Task 2
- **Issue:** `/nonexistent/path` resolves to existing root on Windows, making PuzzleService succeed unexpectedly
- **Fix:** Used `Z:/nonexistent/path` (non-existent drive letter) and added parent-dir validation to service
- **Files modified:** shared/puzzle_service.py, tests/test_puzzle_service.py
- **Committed in:** dd6a205a

---

**Total deviations:** 1 auto-fixed (1 bug)
**Impact on plan:** Minor platform fix. No scope creep.

## Issues Encountered
None beyond the Windows path resolution addressed above.

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- Data model and persistence layer ready for all subsequent puzzle phases
- PuzzleService can be imported by canvas implementations (desktop Phase 48, web Phase 49)
- Fragment index enables "show joins containing this fragment" feature

---
*Phase: 47-foundation-background-removal*
*Completed: 2026-03-16*
