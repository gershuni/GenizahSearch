---
phase: 57-fist-joins-browse-search-mode
plan: 01
subsystem: api, database
tags: [sqlite, svm, visual-similarity, sidecar, thread-safe, frozen-contract]

requires:
  - phase: 25-data-infrastructure
    provides: FjmsService singleton pattern, ThreadLocalConnection
provides:
  - VisualSimilarityService with get_suggestions, has_suggestions, batch_has_suggestions, get_suggestion_partners, get_db_version
  - Import script (FIST.db -> visual_similarity.db) with ATTACH + in-SQL dedup
  - Three frozen-contract API endpoints for Wave 2 consumers
  - 14 unit tests covering service and import
affects: [57-02, 57-03, browse enrichment, desktop visual similarity cache]

tech-stack:
  added: [visual_similarity.db sidecar]
  patterns: [ATTACH DATABASE for cross-db import, frozen API contract with version endpoint]

key-files:
  created:
    - shared/visual_similarity_service.py
    - scripts/import_visual_similarity.py
    - tests/test_visual_similarity.py
  modified:
    - web/api.py

key-decisions:
  - "Separate visual_similarity.db sidecar (not in fjms_enrichment.db which is already 941MB)"
  - "ATTACH DATABASE + in-SQL joins for import performance instead of Python dict loops"
  - "Version endpoint before wildcard route to avoid FastAPI path capture"
  - "INTEGER AlmaIds in sidecar for compactness, string conversion at service boundary"

patterns-established:
  - "Frozen API contract: FROZEN CONTRACT comment + explicit JSON schema in docstrings"
  - "ATTACH + in-SQL import: single INSERT...SELECT with GROUP BY dedup, no Python intermediary"
  - "Version endpoint for desktop cache invalidation"

requirements-completed: [JOIN-01, JOIN-03]

duration: 7min
completed: 2026-03-30
---

# Phase 57 Plan 01: Visual Similarity Data Pipeline Summary

**VisualSimilarityService with ATTACH-based import from FIST.db SVM pairs, frozen API contract for browse/search consumers, 14 tests**

## Performance

- **Duration:** 7 min
- **Started:** 2026-03-30T04:12:21Z
- **Completed:** 2026-03-30T04:19:21Z
- **Tasks:** 3
- **Files modified:** 4

## Accomplishments
- VisualSimilarityService with thread-safe singleton, 7 query methods (suggestions, existence, batch, union/intersection, version)
- Import script using PRAGMA tuning (synchronous=OFF, 512MB cache) + ATTACH DATABASE for in-SQL mapping from FIST.db
- Three API endpoints with frozen response contract: per-manuscript enriched suggestions, batch check (500 limit), version metadata
- 14 tests covering all service methods and import edge cases (dedup, self-pair exclusion, MarkCode filtering)

## Task Commits

Each task was committed atomically:

1. **Task 0: Create test stubs** - `df06d0cb` (test)
2. **Task 1: Import script + VisualSimilarityService + tests** - `ead246ae` (feat)
3. **Task 2: API endpoints with frozen contract** - `44e93e4d` (feat)

## Files Created/Modified
- `shared/visual_similarity_service.py` - Service class with singleton, thread-safe SQLite queries
- `scripts/import_visual_similarity.py` - One-time import from FIST.db using ATTACH + in-SQL joins
- `tests/test_visual_similarity.py` - 14 tests covering service and import
- `web/api.py` - Three new /api/visual_suggestions/ endpoints with frozen contract

## Decisions Made
- Separate visual_similarity.db sidecar to avoid bloating fjms_enrichment.db (already 941 MB)
- ATTACH DATABASE + in-SQL joins instead of Python dict loops for import performance on 15M+ rows
- Version endpoint placed before wildcard `{sys_id}` route to prevent path capture
- INTEGER AlmaIds in sidecar for compactness (~30% savings); string conversion at service boundary

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Fixed DETACH DATABASE lock error in import script**
- **Found during:** Task 1 (import tests)
- **Issue:** DETACH DATABASE failed with "database fist is locked" because temp table doc_alma still referenced attached DB
- **Fix:** Added DROP TABLE + COMMIT before DETACH
- **Files modified:** scripts/import_visual_similarity.py
- **Verification:** All 14 tests pass
- **Committed in:** ead246ae (Task 1 commit)

---

**Total deviations:** 1 auto-fixed (1 blocking)
**Impact on plan:** Essential fix for import script correctness. No scope creep.

## Issues Encountered
None beyond the DETACH lock (documented above).

## Known Stubs
None -- all service methods and API endpoints are fully implemented.

## User Setup Required
None - no external service configuration required. Import script runs against local FIST.db.

## Next Phase Readiness
- Service layer ready for Wave 2 consumers (browse dialog, search integration)
- API endpoints serve frozen-contract JSON for desktop on-demand fetch
- Version endpoint enables desktop cache staleness detection
- Import script ready to run against production FIST.db to generate visual_similarity.db

## Self-Check: PASSED

All 3 created files verified on disk. All 3 task commits verified in git log.

---
*Phase: 57-fist-joins-browse-search-mode*
*Completed: 2026-03-30*
