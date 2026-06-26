---
phase: 124-core-metadata-index
plan: 01
subsystem: genizah_core decomposition
tags: [refactor, extraction, shared-modules, no-behavior-change]
dependency_graph:
  requires: [123-01]
  provides: [shared/metadata_manager.py, shared/indexer.py]
  affects: [genizah_core.py]
tech_stack:
  added: []
  patterns: [same-object re-export shim, inline _tr() lazy import, inline private helper copy]
key_files:
  created:
    - shared/metadata_manager.py
    - shared/indexer.py
  modified:
    - genizah_core.py
    - tests/test_no_back_edges_core.py
    - tests/test_desktop_folio_navigation.py
    - tests/test_browse_synthetic.py
    - tests/test_genizah_core_nli_breaker_migration.py
    - tests/test_direct_image_resolution.py
decisions:
  - inline _tr() helper in shared/indexer.py (lazy CURRENT_LANG import, GUARD-01 safe)
  - inline _strip_brackets copy in shared/indexer.py (original stays in genizah_core for SearchEngine)
  - _get_crossref_service and _get_fjms_service added to shim (required by test_direct_image_resolution)
  - NLI circuit breaker imports kept in genizah_core with noqa F401 (re-exported for test compat)
  - GUARD-01 registry grown 8 -> 10 entries
metrics:
  duration: ~90 minutes
  completed: 2026-06-26T01:01:00Z
  tasks_completed: 2
  files_created: 2
  files_modified: 6
---

# Phase 124 Plan 01: Core Metadata & Index Extraction Summary

## One-liner

Extracted MetadataManager + Indexer classes from genizah_core.py into `shared/metadata_manager.py` and `shared/indexer.py` behind permanent `# noqa: F401` same-object re-export shims — zero behavior change, 10-entry GUARD-01 registry green.

## Tasks Completed

| Task | Description | Commit | Files |
|------|-------------|--------|-------|
| 1 | Extract MetadataManager + pre-cluster → shared/metadata_manager.py | b63411c1 | shared/metadata_manager.py (+~700 lines), genizah_core.py (~2,050 lines removed), 5 test files retargeted |
| 2 | Extract Indexer → shared/indexer.py | b9e4578a | shared/indexer.py (+~440 lines), genizah_core.py (~445 lines removed), test_no_back_edges_core.py (+30 lines) |

## What Was Built

**Task 1 — MetadataManager extraction:**

`shared/metadata_manager.py` contains all of MetadataManager's pre-cluster items and the class itself:
- `_CUDL_LABEL_RE`, `_parse_cudl_label`
- `_BRIDGE_IMPORT_WARNED`, `_warn_bridge_import_failed`
- `_nli_crossref_svc`, `_get_crossref_service`, `_fjms_svc`, `_get_fjms_service` (process-level singletons)
- `_NLI_CACHE_MAX_ENTRIES`, `_BoundedLRUCache`
- `MARC_FUTURE_TIMEOUT`, `NLI_IIIF_FUTURE_TIMEOUT`, `EXTERNAL_IIIF_HTTP_TIMEOUT`
- `class MetadataManager` (full body)

`genizah_core.py` Phase 124 shim re-exports: `_NLI_CACHE_MAX_ENTRIES`, `_BoundedLRUCache`, `MARC_FUTURE_TIMEOUT`, `NLI_IIIF_FUTURE_TIMEOUT`, `EXTERNAL_IIIF_HTTP_TIMEOUT`, `MetadataManager`, `_get_crossref_service`, `_get_fjms_service`.

**Task 2 — Indexer extraction:**

`shared/indexer.py` contains:
- Inline `_tr()` helper with lazy `from genizah_core import CURRENT_LANG` inside function body (GUARD-01 safe)
- Inline `_strip_brackets()` private copy (original stays in genizah_core.py for SearchEngine)
- `class Indexer` (full body, `tr(` → `_tr(` substitution at the single call site in `create_index`)

`genizah_core.py` adds: `from shared.indexer import Indexer  # noqa: F401`

## Verification

- Full pytest suite at Commit 1 boundary: 14 pre-existing failures, 0 new failures
- Full pytest suite at Commit 2 boundary: same 14 pre-existing failures, 0 new failures (GUARD-02)
- `tests/test_no_back_edges_core.py` all 32 tests pass (GUARD-01, 10-entry registry)
- Identity tests: `genizah_core.MetadataManager is shared.metadata_manager.MetadataManager` (CORE-08)
- Identity tests: `genizah_core.Indexer is shared.indexer.Indexer` (CORE-09/CORE-10)
- Per-file `ruff check shared/indexer.py genizah_core.py tests/test_no_back_edges_core.py` → all checks passed

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Accidental deletion of genizah_core.py LOGGING section**
- **Found during:** Task 1
- **Issue:** The deletion range for MetadataManager pre-cluster (intended lines 2099-4150) accidentally included lines 2135-2277 — the `configure_logger`, `LOGGER`, `tr`, `CURRENT_LANG`, `load_app_config`, `save_app_config` module-level functions that must stay in genizah_core.py.
- **Fix:** Restored all LOGGING section lines in genizah_core.py between the tombstone comment and the INDEXER section.
- **Files modified:** genizah_core.py

**2. [Rule 3 - Blocking] `json` import removed from metadata_manager.py was actually unused**
- **Found during:** Task 1 ruff check
- **Issue:** PATTERNS.md module header included `import json` but MetadataManager only calls `resp.json()` (method on requests Response objects), not the `json` module itself.
- **Fix:** Removed `import json` from shared/metadata_manager.py.
- **Files modified:** shared/metadata_manager.py

**3. [Rule 1 - Bug] `_get_crossref_service` and `_get_fjms_service` missing from shim**
- **Found during:** Task 1 test run (`test_direct_image_resolution.py` import error)
- **Issue:** `test_direct_image_resolution.py` imports `_get_crossref_service` from `genizah_core` directly; initial shim only exported 6 names (omitting these two).
- **Fix:** Added `_get_crossref_service` and `_get_fjms_service` to the Phase 124 shim block.
- **Files modified:** genizah_core.py

**4. [Rule 3 - Blocking] 11 ruff F401 errors after MetadataManager removal**
- **Found during:** Task 1 ruff check
- **Issue:** Imports that MetadataManager used (`requests`, `xml.etree.ElementTree`, `OrderedDict`, `ThreadPoolExecutor`, `as_completed`) became unused in genizah_core.py after extraction. NLI circuit breaker imports needed `# noqa: F401` (re-exported for test compat).
- **Fix:** Removed unused imports; added `# noqa: F401` to the NLI circuit breaker block.
- **Files modified:** genizah_core.py

**5. [Rule 1 - Bug] GUARD-03 retargets — 4 test files scanning genizah_core.py for patterns now in shared/**
- **Found during:** Task 1 test run
- **Tests affected:**
  - `test_browse_synthetic.py`: `PNX_MANUSCRIPTS{system_id}` and `NLI_IIIF_BASE}/marc/bib/{system_id}` → retargeted to `shared/metadata_manager.py`
  - `test_genizah_core_nli_breaker_migration.py`: `_nli_circuit_is_open()`, `path='fetch_iiif_manifest'` etc. → `_read_source()` updated to concatenate both files
  - `test_desktop_folio_navigation.py`: `_extract_method(genizah_core_source, 'enrich_metadata')` → new `metadata_manager_source` fixture, retargeted
  - `test_direct_image_resolution.py`: fixed by shim (deviation 3 above)
- **Files modified:** 4 test files

## Known Stubs

None. This plan is a pure mechanical refactor — no data wiring, no UI, no stubs.

## Threat Flags

None. This plan only moves code between modules with no new network endpoints, auth paths, or schema changes.

## Self-Check: PASSED

| Item | Status |
|------|--------|
| shared/metadata_manager.py | FOUND |
| shared/indexer.py | FOUND |
| 124-01-SUMMARY.md | FOUND |
| Commit b63411c1 (Task 1) | FOUND |
| Commit b9e4578a (Task 2) | FOUND |
