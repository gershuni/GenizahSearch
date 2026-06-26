---
phase: "125"
plan: "04"
subsystem: shared/search_engine
tags: [decomposition, extraction, search-engine, facade, guard-01, guard-03]
dependency_graph:
  requires:
    - 125-01  # SEED-011 dedup landed first
    - 125-02  # LabSettings extracted
    - 125-03  # LabEngine extracted (lab_engine.py retargeted here)
  provides:
    - shared/search_engine.py  # SearchEngine + pre-cluster
  affects:
    - genizah_core.py            # stripped, 20-name facade shim added
    - shared/lab_engine.py       # 3 lazy imports retargeted
    - tests/test_no_back_edges_core.py  # CORE-10 tests added
    - tests/test_seed011_composition_dedup.py  # _ChunkPlan patch retargeted
    - tests/test_responsa_integration.py        # parse_responsa_query patches retargeted
    - tests/test_responsa_parity.py             # _apply_explosion_guard patch retargeted
    - tests/test_responsa_edge_cases.py         # parse_responsa_query patch retargeted
    - 8 additional test files (GUARD-03 source-scan retargets)
tech_stack:
  added:
    - shared/search_engine.py (3,934 lines — full SearchEngine extraction)
  patterns:
    - same-object facade shim (from shared.search_engine import X  # noqa: F401)
    - function-body lazy imports (# noqa: PLC0415) for cycle-breaking
    - _tr() helper for lazy CURRENT_LANG import (GUARD-01 safe)
    - GUARD-01: no module-level genizah_core imports in any shared/ module
    - GUARD-03: source-scan tests retargeted to new home file
key_files:
  created:
    - shared/search_engine.py
  modified:
    - genizah_core.py
    - shared/lab_engine.py
    - tests/test_no_back_edges_core.py
    - tests/test_local_post_dedup_merge.py
    - tests/test_phase_97_invariants.py
    - tests/test_local_lab_invalidation.py
    - tests/test_lab_composition_chunk_hits.py
    - tests/test_audit_27_28_a11y_statement_and_stale_index.py
    - tests/test_local_index_open_fallback.py
    - tests/test_seed011_composition_dedup.py
    - tests/test_responsa_edge_cases.py
    - tests/test_responsa_integration.py
    - tests/test_responsa_parity.py
decisions:
  - "GUARD-04 facade: 20 names shimmed (SearchEngine + 19 pre-cluster names incl. all 6 _LAST_RESPONSA_DOWNGRADE thread-local names)"
  - "7 lazy function-body imports in search_engine.py break cycles to genizah_core and lab_engine"
  - "MARK_TOLERANT_INSERTER: module-level regex compiled from make_mark_tolerant_pattern result (not lazy)"
  - "_tr() lazy helper mirrors tr() pattern from Phase 123 (CURRENT_LANG import inside body)"
  - "LAB_LOGGER = logging.getLogger('GenizahLab') at module level — same singleton, no handler duplication"
metrics:
  duration: "~90m (resumed from prior session that completed draft, fixed ruff errors)"
  completed: "2026-06-26"
  tasks_completed: 1
  files_changed: 14
---

# Phase 125 Plan 04: SearchEngine Extraction Summary

Extracted the `SearchEngine` class (3,490+ lines) and its pre-cluster from `genizah_core.py` into `shared/search_engine.py` as the final and largest extraction of the v8.3.0 God-File Decomposition. `genizah_core.py` retains a permanent 20-name same-object re-export facade.

## What Was Built

`shared/search_engine.py` (3,934 lines) contains:

- Full `SearchEngine` class (execute_search, search_composition_logic, get_browse_page, reload_index, and ~49 other methods)
- Pre-cluster in order: `_LAST_RESPONSA_DOWNGRADE` thread-local channel (6 names), `_count_unique_chunks`, `_ChunkPlan`/`_LabChunkPlan` dataclasses, RESPONSA REGEX HELPERS (5 fns), SEED-006 compat gate helpers (4 items: `_index_has_field`, `content_search_staleness_messages`, `MARK_TOLERANT_INSERTER`, `make_mark_tolerant_pattern`), `RRF_K = 60`
- `_tr()` helper (lazy CURRENT_LANG import — GUARD-01 safe)
- `LAB_LOGGER = logging.getLogger("GenizahLab")` — same singleton as the one configured in genizah_core

## Critical Hazards Preserved

- **Hazard A (BrowseMap class-level cache):** `_shared_browse_map = None` and `_browse_map_lock = threading.Lock()` remain as class-level attributes, moved with the class
- **Hazard B (SEED-006 content_search compat gates):** `_index_has_field`, `content_search_staleness_messages`, `_has_content_search` instance attribute — all present and functional
- **Hazard C (_LAST_RESPONSA_DOWNGRADE thread-local):** All 6 names in the thread-local cluster moved together and shimmed individually in the facade
- **CORE-13 (attach_my_library_tab duck-typed gate):** `_my_library_tab_ref` uses `weakref`/duck-typing — no shared→desktop import needed

## GUARD-01 Status

Zero module-level `from genizah_core import` in `shared/search_engine.py`. Seven function-body lazy imports (all `# noqa: PLC0415`):
1. `from shared.lab_engine import LabEngine` (in `_normalize_text`)
2. `from genizah_core import text_to_fingerprint` (in `_compute_fingerprint_dyn`)
3. `from genizah_core import text_to_fingerprint, HEBREW_FREQ` (in `_compute_fingerprint_static`)
4. `from genizah_core import get_boundary_stats, get_crossed_boundaries` (in `search_composition_logic`)
5. `from genizah_core import calculate_boundary_quality, calculate_final_score_with_boost` (in build_items nested fn)
6. `from genizah_core import get_volume_pages` (in `get_browse_page`)
7. `from genizah_core import get_volume_pages` (in `_build_fl_result`)

## Facade Completeness (GUARD-04)

All 20 names shimmed in `genizah_core.py`:
- `SearchEngine` (class)
- `RRF_K` (constant)
- 5 RESPONSA REGEX HELPER fns: `_make_flex_spacing_pattern`, `_build_wildcard_regex`, `_add_bracket_variants`, `_query_has_brackets`, `_strip_brackets`
- 4 SEED-006 items: `_index_has_field`, `content_search_staleness_messages`, `MARK_TOLERANT_INSERTER`, `make_mark_tolerant_pattern`
- `_count_unique_chunks`
- `_ChunkPlan`, `_LabChunkPlan`
- 6 thread-local channel names: `_LAST_RESPONSA_DOWNGRADE`, `_LAST_RESPONSA_DOWNGRADE_META`, `_set_last_responsa_downgrade`, `_consume_last_responsa_downgrade`, `_set_last_responsa_downgrade_meta`, `_consume_last_responsa_downgrade_meta`

## Deviations from Plan

### Auto-fixed Issues (GUARD-03 scope — extra retargets beyond plan's 5 files)

**1. [Rule 1 - Bug] test_seed011_composition_dedup.py: _ChunkPlan patches targeted wrong module**
- **Found during:** Test run after extraction
- **Issue:** Tests patched `genizah_core._ChunkPlan` but `search_composition_logic` is now in `shared.search_engine` and uses its own module's `_ChunkPlan` — facade shim patch doesn't reach live code
- **Fix:** Retargeted both `patch.object(genizah_core, "_ChunkPlan", ...)` calls to `patch.object(_se, "_ChunkPlan", ...)` (where `_se = shared.search_engine`)
- **Files modified:** `tests/test_seed011_composition_dedup.py`
- **Commit:** 4902a8b7

**2. [Rule 1 - Bug] test_responsa_integration.py / test_responsa_parity.py / test_responsa_edge_cases.py: parse_responsa_query and expansion function patches targeted wrong module**
- **Found during:** Test run after extraction (5 failures in test_responsa_integration.py, 1 each in the others)
- **Issue:** 9 `patch('genizah_core.parse_responsa_query')` / `patch('genizah_core.expand_*')` / `patch('genizah_core._apply_explosion_guard')` calls patched `genizah_core`'s namespace but `execute_search` in `shared.search_engine` uses its own module imports
- **Fix:** Retargeted all 9 patches to `shared.search_engine.*`
- **Files modified:** `tests/test_responsa_integration.py`, `tests/test_responsa_parity.py`, `tests/test_responsa_edge_cases.py`
- **Commit:** 4902a8b7

### Import corrections (ruff F401/F821 fixes on search_engine.py draft)

- Removed unused module-level: `import hashlib` (re-imported locally in method as `_hashlib`), `import time` (unused), `from typing import List, Dict, Set, Any` (no type annotations use them), `from dataclasses import field` (shadows parameter name)
- Added missing: `import html` (used in `format_snippet`), `from typing import Optional` (pre-cluster return types), `from shared.indexer import Indexer` (`_validate_position_match`), `from shared.browse_map_utils import _extract_ie_from_header`, `from shared.responsa import extract_per_pair_gaps, _expand_inline_alternation`
- Added `# noqa: F401` to `ResponsaComponent` import (type used in docstrings/callable)

## Tests

- 364 targeted tests (core extraction + all GUARD-03 targets + responsa + composition): **all pass**
- ruff on all 14 modified/created files: **clean**
- GUARD-01 scan: **0 violations**
- GUARD-04 facade: **20/20 names present, identity confirmed** (`gc.SearchEngine is se_mod.SearchEngine == True`)

## Self-Check: PASSED

- `shared/search_engine.py` exists: FOUND
- `genizah_core.py` stripped to 755 lines with facade: FOUND
- Commit `4902a8b7` exists: FOUND
- 14 files changed, 4061 insertions, 3909 deletions

## Known Stubs

None — extraction preserves all method bodies from the original genizah_core.py SearchEngine.
