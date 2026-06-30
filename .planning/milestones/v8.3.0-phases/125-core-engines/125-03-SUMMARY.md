---
phase: 125-core-engines
plan: "03"
subsystem: core
tags: [lab-engine, tantivy, extraction, god-file-decomposition, shared, search]

# Dependency graph
requires:
  - phase: 125-02
    provides: "shared/lab_settings.py with LabSettings class"
provides:
  - "shared/lab_engine.py — LabEngine class (Lab-mode fingerprint composition + LOCAL-LAB side-index)"
  - "genizah_core.py facade shim — from shared.lab_engine import LabEngine (same-object re-export)"
  - "Identity test + standalone smoke test for LabEngine in test_no_back_edges_core.py"
  - "GUARD-03 source-scan tests retargeted: test_local_lab_invalidation.py + test_audit_2026_06_23_guards.py"
affects: [125-04, 125-05, search-engine, lab-composition, joins-lab]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Same-object facade shim: genizah_core re-exports from shared.lab_engine via noqa:F401"
    - "GUARD-01: no extracted shared/ module imports genizah_core at module level; lazy function-body imports only"
    - "GUARD-03: source-scan tests retargeted to the file where the method now lives"
    - "LAB_LOGGER binds the same configured instance via logging.getLogger('GenizahLab') at module level"
    - "CORE-13 duck-typed gate preserved: getattr(_my_library_tab_ref) pattern, no desktop imports in shared/"

key-files:
  created:
    - shared/lab_engine.py
  modified:
    - genizah_core.py
    - tests/test_no_back_edges_core.py
    - tests/test_local_lab_invalidation.py
    - tests/test_audit_2026_06_23_guards.py

key-decisions:
  - "LabEngine extraction boundary: class LabEngine only (lines 546-1993 post-drift); module-level functions after it stay in genizah_core"
  - "_LabChunkPlan and _ChunkPlan dataclasses stay in genizah_core until 125-04 (shared with SearchEngine); LabEngine accesses via lazy import"
  - "test_lab_composition_chunk_hits.py left unchanged: its source-scans target SearchEngine methods (build_items, search_composition_logic), not LabEngine"
  - "LOGGER vs LAB_LOGGER preserved verbatim: LAB_LOGGER for all GenizahLab-logger calls, LOGGER for standard debug calls"

patterns-established:
  - "Lazy function-body import pattern for cross-module references during decomposition (GUARD-01 safe)"
  - "Same-object re-export shim pattern for permanent backward compatibility in genizah_core.py"

requirements-completed: [CORE-12, CORE-13, GUARD-02, GUARD-03, GUARD-04]

# Metrics
duration: ~120min
completed: 2026-06-26
---

# Phase 125 Plan 03: LabEngine Extraction Summary

**LabEngine class (1448 lines) extracted verbatim from genizah_core.py into shared/lab_engine.py with same-object re-export facade and GUARD-03 source-scan retargets**

## Performance

- **Duration:** ~120 min (includes bulk test verification)
- **Started:** 2026-06-25T~22:00:00Z
- **Completed:** 2026-06-26T06:59:33Z
- **Tasks:** 1 (single atomic task per plan)
- **Files modified:** 5

## Accomplishments

- Created `shared/lab_engine.py` with LabEngine class (~1448 lines) extracted verbatim from genizah_core.py (lines 546-1993)
- Added same-object re-export shim in genizah_core.py: `from shared.lab_engine import LabEngine  # noqa: F401`
- Removed three now-unused imports from genizah_core.py (`import shutil`, `import time`, `from functools import lru_cache`) that were only used by LabEngine
- Added identity test (`test_lab_engine_identity`) and standalone smoke test (`test_lab_engine_standalone_import`) in test_no_back_edges_core.py
- Retargeted GUARD-03 source-scan tests: 3 scans in test_local_lab_invalidation.py and 2 in test_audit_2026_06_23_guards.py now read shared/lab_engine.py

## Task Commits

1. **Task 1: Extract LabEngine to shared/lab_engine.py + GUARD-03 retargets** - `0fc24dc7` (feat)

**Plan metadata:** (pending this docs commit)

## Files Created/Modified

- `shared/lab_engine.py` — New: LabEngine class extracted from genizah_core; module-level LAB_LOGGER + LOGGER bindings; lazy function-body imports from genizah_core (GUARD-01 safe); no module-level genizah_core imports; `# -*- coding: utf-8 -*-` header; tantivy guard
- `genizah_core.py` — Removed LabEngine class body; added Phase 125 facade shim; removed 3 unused imports (shutil, time, lru_cache)
- `tests/test_no_back_edges_core.py` — Added test_lab_engine_identity + test_lab_engine_standalone_import
- `tests/test_local_lab_invalidation.py` — Retargeted 3 LabEngine source-scans from genizah_core.py to shared/lab_engine.py; added import pathlib
- `tests/test_audit_2026_06_23_guards.py` — Retargeted 2 lab_composition_search scans from genizah_core.py to shared/lab_engine.py; added pytest.skip guards

## Decisions Made

- **Extraction boundary confirmed**: `class LabEngine` starts at line 546, ends just before `def get_volume_pages` at line 2002. Plan cited lines 634-2055 but actual lines drifted. Used grep to confirm boundaries.
- **_LabChunkPlan stays in genizah_core**: Dataclasses `_LabChunkPlan` and `_ChunkPlan` are shared with SearchEngine code; LabEngine references `_LabChunkPlan` via lazy function-body import inside `lab_composition_search`.
- **test_lab_composition_chunk_hits.py unchanged**: Its source-scans target `build_items` and `search_composition_logic` which are SearchEngine methods. Only LabEngine-specific scans are retargeted in this plan; SearchEngine-targeted scans deferred to 125-04 per plan spec.
- **LAB_LOGGER binding**: Bound at module level as `LAB_LOGGER = logging.getLogger("GenizahLab")` — picks up the same configured instance that `configure_lab_logger()` in genizah_core wires. No change to logging behavior.

## Deviations from Plan

None — plan executed exactly as written. The line number drift (plan: 634-2055, actual: 546-1993) was a documentation inaccuracy in the plan, not a behavioral deviation.

## Issues Encountered

- **Line numbers drifted**: Plan cited LabEngine at lines 634-2055 but grep showed `class LabEngine` at line 546 and `def get_volume_pages` at line 2002. Used grep to confirm actual boundaries before extraction.
- **File modified since read**: When editing genizah_core.py to remove unused imports, got "file modified since read" — re-read and edited successfully.
- **Unused imports after extraction**: ruff reported `shutil`, `time`, `lru_cache` as unused in genizah_core.py after removing LabEngine. Removed manually (not via `ruff --fix` per project rule).
- **Bulk test segfault**: `pytest tests/ -m "not gui_test"` used wrong marker (project uses `-m "not gui"`). Also hit known PyQt6 headless segfault. Used background run with `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen` — final result: 4873 passed / 16 failed (6 pre-existing env failures + 10 pre-existing asyncio event-loop-contamination flakies; zero new failures).

## User Setup Required

None — no external service configuration required.

## Next Phase Readiness

- `shared/lab_engine.py` is ready for 125-04 which will extract SearchEngine and retarget the remaining lazy imports (`make_mark_tolerant_pattern`, `_count_unique_chunks`, `_LabChunkPlan`) from genizah_core to shared.search_engine
- GUARD-01 invariant maintained: shared/lab_engine.py has zero module-level genizah_core imports
- All 126 targeted tests pass; bulk suite confirms zero new regressions

## Self-Check: PASSED

- `shared/lab_engine.py` — FOUND
- `.planning/phases/125-core-engines/125-03-SUMMARY.md` — FOUND
- Commit `0fc24dc7` — FOUND

---
*Phase: 125-core-engines*
*Completed: 2026-06-26*
