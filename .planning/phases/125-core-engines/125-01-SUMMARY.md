---
phase: 125-core-engines
plan: "01"
subsystem: genizah_core
tags: [seed-011, composition-dedup, chunk-plan, lab-engine, search-engine, guard-01]
dependency_graph:
  requires: []
  provides:
    - _ChunkPlan (genizah_core module-level dataclass)
    - _LabChunkPlan (genizah_core module-level dataclass)
    - search_composition_logic dedup refactor
    - lab_composition_search dedup refactor
    - EXTRACTED_MODULES registry at 13 entries with skip-until-exists guard
  affects:
    - genizah_core.SearchEngine.search_composition_logic
    - genizah_core.LabEngine.lab_composition_search
    - tests/test_seed011_composition_dedup.py
    - tests/test_no_back_edges_core.py
tech_stack:
  added:
    - dataclasses (@dataclass decorator, stdlib — new import in genizah_core.py)
  patterns:
    - Pre-pass dataclass pattern: build list of plan objects once, consume in two loops
    - None-sentinel pattern for skipped chunks in LabChunkPlan pre-pass
    - Skip-until-exists pytest guard for pre-registered registry entries
key_files:
  created:
    - tests/test_seed011_composition_dedup.py
  modified:
    - genizah_core.py (_ChunkPlan + _LabChunkPlan dataclasses + two refactored loops)
    - tests/test_no_back_edges_core.py (registry 10 -> 13 + skip guard)
decisions:
  - "Two query strings per _ChunkPlan (genizah_query_str + local_query_str): LOCAL flavor applies strip_search_diacritics (SEED-006 M1); NOT collapsed to one string — that would change results."
  - "final_query_str (Genizah source boost) NOT on _LabChunkPlan — index-local; built from plan.core_query inside the Genizah-LAB loop."
  - "None-sentinel in lab_chunk_plans for statistically-weak/too-short chunks: both loops do `if plan is None: continue` without re-evaluating the predicate."
  - "EXTRACTED_MODULES pre-grown to 13 in Wave 0 (125-01) with pytest.skip guard so 125b/c/d don't need to patch the registry mid-extraction."
metrics:
  completed_date: "2026-06-26"
  tasks: 3
  files_modified: 4
  commits: 3
---

# Phase 125 Plan 01: SEED-011 Composition Double-Prep Dedup Summary

**One-liner:** Per-chunk `_ChunkPlan` / `_LabChunkPlan` dataclasses eliminate the double structural iteration of `chunks_data` in `corpus_scope='all'` composition; fingerprint prep in `lab_composition_search` drops from 2*N to N `text_to_fingerprint` calls per run.

## Tasks Completed

| # | Name | Commit | Files |
|---|------|--------|-------|
| 1 | Add RED guard tests (Wave 0) | `42ed2477` | tests/test_seed011_composition_dedup.py |
| 2 | SEED-011 Finding 1 — _ChunkPlan dedup for search_composition_logic | `d003a533` | genizah_core.py |
| 3 | SEED-011 Finding 2 — _LabChunkPlan dedup + registry pre-grow | `8b35b1a2` | genizah_core.py, tests/test_no_back_edges_core.py, tests/test_seed011_composition_dedup.py |

## What Was Built

### _ChunkPlan dataclass (genizah_core module-level)

Fields: `token_idx`, `chunk`, `chunk_crossed_bounds`, `genizah_query_str`, `compiled_regex_genizah`, `local_query_str`, `compiled_regex_local`, `local_chunk_q`.

The plan carries TWO query strings and TWO compiled regexes — one Genizah-flavor (raw chunk, `content_search_field=_cs_field`) and one LOCAL-flavor (diacritic-folded via `strip_search_diacritics`, no content_search_field). The two flavors are genuinely different (SEED-006 M1) so `build_tantivy_query` / `build_regex_pattern` remain called once per (chunk x flavor) — the 2*N count is unchanged and correct. What the dedup removes is the structural double-iteration of `chunks_data`.

### search_composition_logic refactor

A pre-pass over `chunks_data` now builds `chunk_plans` before the `try:` block. Both the Genizah loop and the LOCAL loop iterate `enumerate(chunk_plans)` and consume the pre-built plan attributes instead of re-computing them inline. The `_cs_field` / `_local_has_cs_prepass` flags are computed once before the pre-pass (they depend only on `self` attributes, not on individual chunks).

### _LabChunkPlan dataclass (genizah_core module-level)

Fields: `token_start_idx`, `chunk_tokens`, `chunk_text`, `chunk_crossed_bounds`, `fp_str`, `fp_list`, `needed_unique_fps`, `core_query`.

`final_query_str` is NOT stored on the plan — it is index-local (Genizah-LAB adds `AND (source:"V0.8"^10 OR source:"V0.7")` while LOCAL-LAB uses `core_query` directly). Each loop computes `final_query_str` from `plan.core_query`.

### lab_composition_search refactor

A pre-pass over `chunks_data` builds `lab_chunk_plans` (a list of `_LabChunkPlan | None`) before the `try:` block. `None` sentinels represent chunks that failed `_is_phrase_statistically_weak` or the `len < 4` gate — both loops do `if plan is None: continue`. Both the Genizah-LAB and LOCAL-LAB loops iterate `enumerate(lab_chunk_plans)` and consume the pre-built fields.

`text_to_fingerprint` is called exactly N times (once per qualifying chunk), down from 2*N (once per chunk per loop). All exception handlers, log statements, and gating logic preserved verbatim.

### GUARD-01 registry pre-grown to 13

`tests/test_no_back_edges_core.py` EXTRACTED_MODULES extended with `shared/lab_settings.py`, `shared/lab_engine.py`, `shared/search_engine.py`. The parametrized `test_no_module_level_genizah_core_import` now calls `pytest.skip()` when the file does not exist yet, so 125b/c/d don't need to touch the registry.

## Test Results

| Test Suite | Result |
|------------|--------|
| test_seed011_composition_dedup.py (3 tests) | 3/3 GREEN |
| test_comp_corpus_scope.py | 25/25 GREEN |
| test_local_post_dedup_merge.py | GREEN |
| test_phase_97_invariants.py | GREEN |
| test_lab_composition_chunk_hits.py | GREEN |
| test_local_lab_invalidation.py | GREEN |
| test_audit_2026_06_23_guards.py | GREEN |
| test_no_back_edges_core.py | 32 passed, 3 skipped |
| Full bulk suite (`not gui and not render_smoke`) | 4841 passed, 7 pre-existing failures (confirmed at base HEAD) |

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] lab_composition_search has no max_freq parameter**
- **Found during:** Task 3 test run
- **Issue:** test_lab_composition_search_no_double_prep called `lab_composition_search(max_freq=100, ...)` but the method signature does not accept `max_freq` (it belongs to `search_composition_logic` only)
- **Fix:** Removed spurious `max_freq=100` kwarg from the test call
- **Files modified:** tests/test_seed011_composition_dedup.py
- **Commit:** 8b35b1a2

None otherwise — plan executed exactly as specified with this one self-correction.

## Invariants Verified

- GUARD-02 (zero behavior change): composition results identical before and after (all behavior-preserving tests green; verified via test_comp_corpus_scope, test_local_post_dedup_merge, test_phase_97_invariants)
- No BOM reintroduced: `raw[:3] != b'\xef\xbb\xbf'` confirmed at each commit
- `ruff check genizah_core.py` reports zero findings at each commit
- EXTRACTED_MODULES at 13; back-edge scan: 32 passed, 3 skipped (expected)

## Known Stubs

None. This plan is a behavior-preserving refactor with no new features or data wiring.

## Threat Flags

None. This plan performs code movement only — no new network endpoints, auth paths, schema changes, or trust boundary crossings.

## Self-Check: PASSED

- `genizah_core.py` exists and contains `class _ChunkPlan` and `class _LabChunkPlan`: FOUND
- `tests/test_seed011_composition_dedup.py` exists: FOUND
- `tests/test_no_back_edges_core.py` updated (13 entries + skip guard): FOUND
- Commit `42ed2477` (Task 1): FOUND
- Commit `d003a533` (Task 2): FOUND
- Commit `8b35b1a2` (Task 3): FOUND
- All 3 dedup tests GREEN: VERIFIED (3 passed)
- No new failures vs base: VERIFIED (7 pre-existing failures confirmed at base HEAD)
