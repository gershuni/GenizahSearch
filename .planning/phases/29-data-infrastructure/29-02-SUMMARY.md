---
phase: 29-data-infrastructure
plan: 02
subsystem: database
tags: [sqlite, sidecar, nli, cambridge, iiif, service-layer, thread-safe, batch-query]

# Dependency graph
requires:
  - "29-01: nli_data/nli_crossref.db sidecar with nli_images + cambridge_manifests tables"
provides:
  - "shared/nli_crossref_service.py NliCrossrefService with 12 query methods for image, Cambridge, metadata, relationship, and availability data"
  - "web/nli_crossref_service.py backward-compatible shim for web imports"
  - "tests/test_nli_crossref_service.py 25 unit tests covering all methods and edge cases"
affects: [30-image-resolution, 31-metadata-display, 32-relationships, 33-service-layer, 34-ui-integration]

# Tech tracking
tech-stack:
  added: []
  patterns: [nli-crossref-service-singleton, batch-image-lookup, image-source-availability-check]

key-files:
  created:
    - shared/nli_crossref_service.py
    - web/nli_crossref_service.py
    - tests/test_nli_crossref_service.py
  modified: []

key-decisions:
  - "Followed FJMS service pattern exactly -- same _find_project_root(), URI read-only mode, thread_safe param, singleton"
  - "Image queries return snake_case dict keys mapped from PascalCase SQL columns -- consistent with FjmsService convention"
  - "get_image_sources combines NLI FGP and Cambridge checks in single call for efficient UI badge rendering"

patterns-established:
  - "NliCrossrefService: thread-safe SQLite service for NLI image and Cambridge IIIF data"
  - "get_images_batch: batched IN queries (batch_size=500) for search result enrichment"
  - "get_image_sources: combined availability check returning {nli_fgp, cambridge, image_count}"

# Metrics
duration: 3min
completed: 2026-02-15
---

# Phase 29 Plan 02: NLI Crossref Service Layer Summary

**NliCrossrefService with 12 methods providing image lookup, Cambridge IIIF manifests, physical metadata, relationship queries, and availability indicators for both web and desktop apps**

## Performance

- **Duration:** 3 min
- **Started:** 2026-02-15T13:19:32Z
- **Completed:** 2026-02-15T13:22:29Z
- **Tasks:** 2
- **Files modified:** 3

## Accomplishments
- Created NliCrossrefService (430 lines) with 12 public methods covering all downstream phase needs (30-34)
- Built comprehensive test suite (25 tests, 358 lines) with temp SQLite fixtures covering images, Cambridge IIIF, metadata, relationships, availability, graceful degradation, and thread-safety
- Web shim enables backward-compatible `from web.nli_crossref_service import NliCrossrefService`
- Service follows established FjmsService patterns exactly: URI read-only mode, thread_safe parameter, module-level singleton, graceful degradation

## Task Commits

Each task was committed atomically:

1. **Task 1: Create shared NliCrossrefService class** - `ef0127a` (feat)
2. **Task 2: Create web shim and unit tests** - `78b87d0` (test)

## Files Created/Modified
- `shared/nli_crossref_service.py` - NliCrossrefService class with 12 methods: is_available, get_version, get_images, get_images_batch, get_cambridge_manifest, get_cambridge_manifest_by_label, get_physical_metadata, get_part_of, get_see_references, get_bifolio_partners, get_image_sources, close
- `web/nli_crossref_service.py` - One-line shim re-exporting NliCrossrefService and get_nli_crossref_service
- `tests/test_nli_crossref_service.py` - 25 unit tests with temporary SQLite fixture covering all service methods and edge cases

## Decisions Made
- Followed FJMS service pattern exactly -- same `_find_project_root()`, URI read-only connection, `thread_safe` parameter, `sqlite3.Row` factory, module-level singleton
- Image queries return snake_case dict keys mapped from PascalCase SQL columns (e.g., FGPImageNumberId -> fgp_image_number_id), consistent with FjmsService convention
- `get_image_sources()` combines NLI FGP existence check and Cambridge manifest lookup in a single method call, designed for efficient UI badge rendering in downstream phases

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered
None

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- NliCrossrefService ready for image URL resolution (Phase 30: IMG-01, IMG-02)
- get_image_sources ready for availability badges (Phase 31: IMG-03)
- get_physical_metadata ready for metadata display (Phase 32: META-01, META-02)
- get_part_of, get_bifolio_partners ready for relationship UI (Phase 33: REL-01, REL-02)
- Phase 29 (Data Infrastructure) complete -- both plans (import + service) delivered

## Self-Check: PASSED

- FOUND: shared/nli_crossref_service.py (430 lines, >= 150 min)
- FOUND: web/nli_crossref_service.py (9 lines)
- FOUND: tests/test_nli_crossref_service.py (358 lines, >= 100 min)
- FOUND: .planning/phases/29-data-infrastructure/29-02-SUMMARY.md
- FOUND: commit ef0127a
- FOUND: commit 78b87d0

---
*Phase: 29-data-infrastructure*
*Completed: 2026-02-15*
