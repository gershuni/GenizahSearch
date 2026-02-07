---
phase: 08-foundation
plan: 02
subsystem: shared-services
tags: [shim, re-export, smoke-tests, backward-compatibility]
dependency-graph:
  requires:
    - phase: 08-01
      provides: shared/ package with supabase_provider and document_service
  provides: [web-shim, updated-tests, smoke-tests, desktop-importability-proven]
  affects: [10-01, 11-01, 12-01]
tech-stack:
  added: []
  patterns: [re-export-shim, identity-forwarding]
key-files:
  created:
    - tests/test_shared_service.py
  modified:
    - web/document_service.py
    - tests/test_document_service.py
decisions:
  - id: DEC-08-02-01
    description: "Fixed pre-existing mock chain bug in test_get_document_for_fragment_not_found"
    rationale: "Mock used .limit() which actual code doesn't call, causing false positive. Rule 1 auto-fix."
metrics:
  duration: ~3 min
  completed: 2026-02-08
---

# Phase 8 Plan 2: Web re-export shim, test updates, and smoke tests

**Replaced web/document_service.py (508 lines) with 22-line re-export shim forwarding all 12 PGP functions from shared.document_service, plus smoke tests proving desktop importability**

## Performance

- **Duration:** ~3 min
- **Tasks:** 2/2 (1 auto + 1 checkpoint:human-verify)
- **Files modified:** 3

## Accomplishments

1. Replaced `web/document_service.py` with 22-line re-export shim -- all 12 functions forwarded from `shared.document_service`
2. Updated all test patch targets from `web.document_service.get_client` to `shared.document_service.get_client`
3. Created `tests/test_shared_service.py` with smoke tests (shared package, shim re-exports identity check, desktop import)
4. Fixed pre-existing mock chain bug in `test_get_document_for_fragment_not_found`
5. All 24 tests pass; manual web app walkthrough approved by user

## Task Commits

| Task | Name | Commit | Key Files |
|------|------|--------|-----------|
| 1 | Replace web/document_service.py with re-export shim and update tests | d85bb5e | web/document_service.py, tests/test_document_service.py, tests/test_shared_service.py |
| 2 | Manual verification checkpoint | -- | User approved: web app PGP features work correctly |

## Files Created/Modified

- `web/document_service.py` -- Replaced 508-line module with 22-line re-export shim
- `tests/test_document_service.py` -- Patch targets updated to shared.document_service.get_client
- `tests/test_shared_service.py` -- New smoke tests (6 test cases across 5 classes)

## Decisions Made

| ID | Decision | Rationale |
|----|----------|-----------|
| DEC-08-02-01 | Fix pre-existing mock chain bug (Rule 1 auto-fix) | Mock used .limit() which actual code doesn't call |

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Fixed mock chain in test_get_document_for_fragment_not_found**
- **Found during:** Task 1 (updating test patch targets)
- **Issue:** Mock chain had `.eq.return_value.limit.return_value.execute` but code has no `.limit()` call
- **Fix:** Removed `.limit` from mock chain
- **Verification:** Test now correctly passes

---

**Total deviations:** 1 auto-fixed (1 bug)
**Impact on plan:** Essential for test correctness. No scope creep.

## Issues Encountered

None.

## User Feedback During Verification

User noted: "In multi-fragment document view, should add image controls for each image." Captured as pending todo for Phase 11 (Virtual Reading Desk).

## Next Phase Readiness

- Phase 8 Foundation complete: shared service layer extracted, wired, tested, verified
- Desktop app can import shared.document_service directly
- Ready for Phase 9 (Data Import) or Phase 10 (Desktop PGP Core)

**Blockers:** None
**Concerns:** None

---
*Phase: 08-foundation*
*Completed: 2026-02-08*
