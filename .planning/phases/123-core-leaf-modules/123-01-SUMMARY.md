---
phase: "123"
plan: "01"
subsystem: "shared/core-decomposition"
tags: [extraction, refactor, shared-modules, god-file-decomposition]
dependency_graph:
  requires: ["122-config-enabler"]
  provides: ["shared/browse_map_utils", "shared/text_normalize", "shared/variants", "shared/responsa", "shared/codicological", "shared/joins_manager", "shared/lists_manager"]
  affects: ["genizah_core.py", "web/*", "genizah_app.py", "shared/*"]
tech_stack:
  added: []
  patterns: ["same-object re-export shim (# noqa: F401)", "lazy function-body import for mutable globals (Pitfall 2)", "inline _tr() helper for tr()-dependent modules (GUARD-01 safe)"]
key_files:
  created:
    - shared/browse_map_utils.py
    - shared/text_normalize.py
    - shared/variants.py
    - shared/responsa.py
    - shared/codicological.py
    - shared/joins_manager.py
    - shared/lists_manager.py
  modified:
    - genizah_core.py
    - tests/test_no_back_edges_core.py
    - shared/exclusion_service.py
    - shared/local_indexer.py
    - shared/nli_crossref_service.py
    - shared/search_serializer.py
decisions:
  - "Engine-side helpers (_add_bracket_variants, _query_has_brackets, _strip_brackets, _index_has_field, content_search_staleness_messages, MARK_TOLERANT_INSERTER, make_mark_tolerant_pattern) STAY in genizah_core.py — they depend on the Tantivy engine context and cannot be isolated without architectural change"
  - "Inline _tr() helper pattern used in responsa.py and lists_manager.py instead of importing tr() — lazy CURRENT_LANG import inside function body satisfies GUARD-01 (no module-level back-edge)"
  - "Pre-existing flaky test test_round_trip_search_type_fuzzy (asyncio event loop closed) documented as known; passes in isolation, fails only in full-suite run due to event loop contamination from upstream tests"
metrics:
  duration_minutes: 63
  completed_date: "2026-06-25"
  tasks_completed: 7
  tasks_total: 7
  files_created: 7
  files_modified: 6
---

# Phase 123 Plan 01: Core Leaf Module Extraction Summary

**One-liner:** Extracted 7 cohesive clusters (4,304 lines) from genizah_core.py into permanent shared/ modules behind same-object re-export shims, reducing genizah_core.py from ~12,500 to ~8,453 lines with zero behavior change.

## Tasks Completed

| Task | Name | Commit | Key Files |
|------|------|--------|-----------|
| 1 | browse_map_utils extraction + D-01 retargets | `1d77d90a` | shared/browse_map_utils.py, genizah_core.py, 3 shared/ retargets |
| 2 | text_normalize extraction + local_indexer retargets | `1c3930d0` | shared/text_normalize.py, genizah_core.py, shared/local_indexer.py |
| 3 | variants extraction | `74b46e6c` | shared/variants.py, genizah_core.py, tests/ |
| 4 | responsa extraction | `57023501` | shared/responsa.py, genizah_core.py, tests/ |
| 5 | codicological extraction | `5bf8b335` | shared/codicological.py, genizah_core.py, tests/ |
| 6 | joins_manager extraction | `746176f4` | shared/joins_manager.py, genizah_core.py, tests/ |
| 7 | lists_manager extraction | `3fca9bd1` | shared/lists_manager.py, genizah_core.py, tests/ |

## What Was Built

Seven cohesive clusters extracted from genizah_core.py:

1. **shared/browse_map_utils.py** (520 lines) — LIBRARY_CODES, normalize_shelfmark, natural_sort_key, get_library_display, dedupe_browse_map, IE volume map helpers
2. **shared/text_normalize.py** (66 lines) — NIKUD_PATTERN, strip_nikud, COMBINING_DIACRITICALS_PATTERN, strip_search_diacritics
3. **shared/variants.py** (417 lines) — VariantManager class (Hebrew variant expansion engine)
4. **shared/responsa.py** (1,146 lines) — Full Responsa query parsing cluster: GRAMMATICAL_PREFIXES/SUFFIXES, ResponsaComponent dataclass, parse_responsa_query, _tokenize_responsa_query, expand_* functions, LineGroup, _parse_line_break_query, explosion guard
5. **shared/codicological.py** (386 lines) — CodicologicalManager class (manuscript measurement/dimension analysis)
6. **shared/joins_manager.py** (556 lines) — JoinsManager class (joins puzzle cache management)
7. **shared/lists_manager.py** (1,213 lines) — ListsManager class (user reading lists CRUD)

**genizah_core.py** reduced from ~12,500 → 8,453 lines. All removed code replaced with permanent `from shared.X import ... # noqa: F401` re-export shims so all existing callers continue importing from genizah_core unchanged.

## Verification

- **GUARD-01** (no module-level back-edges): 26 tests pass (8 no-back-edge params + 8 identity + 8 smoke + 2 lists_manager)
- **GUARD-02** (full suite green): 1,917+ passed; 1 pre-existing asyncio flaky failure (see Deferred Issues)
- **GUARD-04** (per-file ruff): All 7 new modules + genizah_core.py clean

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Engine-side helpers accidentally removed during responsa extraction (Task 4)**
- **Found during:** Task 4
- **Issue:** The RESPONSA QUERY COMPONENTS section in genizah_core.py interspersed 7 engine-side helpers (_add_bracket_variants, _query_has_brackets, _strip_brackets, _index_has_field, content_search_staleness_messages, MARK_TOLERANT_INSERTER, make_mark_tolerant_pattern) within the cluster. The extraction script captured the entire section including these helpers.
- **Fix:** Identified helpers from git history, re-inserted them into genizah_core.py under RESPONSA REGEX HELPERS section before committing Task 4.
- **Files modified:** genizah_core.py
- **Commit:** `57023501`

**2. [Rule 1 - Bug] ListsManager extraction captured wrong size (Task 7)**
- **Found during:** Task 7
- **Issue:** First extraction attempt captured 438K chars instead of ~43K (the full trailing content of genizah_core.py from the class start position).
- **Fix:** Rewrote extraction with explicit size verification logic (correctly found 43,481 chars).
- **Files modified:** shared/lists_manager.py
- **Commit:** `3fca9bd1`

**3. [Rule 2 - Missing Critical] D-01 retargets for pre-existing shared/ files (Tasks 1-2)**
- **Found during:** Tasks 1-2 (identified in plan)
- **Issue:** Pre-existing shared/ files (exclusion_service.py, nli_crossref_service.py, search_serializer.py, local_indexer.py) lazily imported from genizah_core for symbols now in the new shared/ modules. While not strictly GUARD-01 violations (lazy imports), retargeting them is required for correctness and to avoid circular dependency risk.
- **Fix:** Retargeted all lazy imports to the new shared/ homes as specified in the plan.
- **Files modified:** shared/exclusion_service.py, shared/nli_crossref_service.py, shared/search_serializer.py, shared/local_indexer.py
- **Commits:** `1d77d90a`, `1c3930d0`

## Deferred Issues

**Pre-existing flaky test: `tests/test_joins_builder.py::TestBuilderStateRoundTrip::test_round_trip_search_type_fuzzy`**
- Fails with `RuntimeError: Event loop is closed` in full-suite runs only
- Passes in isolation (verified before and after our changes)
- Pre-existing issue unrelated to this extraction work
- Event loop contamination from upstream tests in the full suite run
- Tracked in OPEN_ISSUES.md

## Known Stubs

None. This plan performs pure code movement with no new UI or data-wiring.

## Threat Flags

None. This plan moves existing code between modules without adding new network endpoints, auth paths, file access patterns, or schema changes.

## Self-Check: PASSED

- shared/browse_map_utils.py: FOUND
- shared/text_normalize.py: FOUND
- shared/variants.py: FOUND
- shared/responsa.py: FOUND
- shared/codicological.py: FOUND
- shared/joins_manager.py: FOUND
- shared/lists_manager.py: FOUND
- Commit 1d77d90a: FOUND
- Commit 1c3930d0: FOUND
- Commit 74b46e6c: FOUND
- Commit 57023501: FOUND
- Commit 5bf8b335: FOUND
- Commit 746176f4: FOUND
- Commit 3fca9bd1: FOUND
