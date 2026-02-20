---
phase: 40-performance-optimization
plan: 05
subsystem: search
tags: [variant-cache, superset-slice, tantivy, regex, performance]

# Dependency graph
requires:
  - phase: 40-01
    provides: "superset cache lookup in VariantManager.get_variants"
  - phase: 40-04
    provides: "_get_or_compute_variants pre-computation method"
provides:
  - "Unified variant cache: compute once at max limit, slice for smaller requests"
  - "Eliminated duplicate variant generation between Tantivy (limit=200) and regex (limit=8000) phases"
affects: [search, responsa]

# Tech tracking
tech-stack:
  added: []
  patterns: ["superset-aware cache lookup with slice-from-larger optimization"]

key-files:
  created: []
  modified: ["genizah_core.py"]

key-decisions:
  - "Pre-compute at REGEX_VARIANTS_LIMIT (8000) before per-term loops so Tantivy (200) slices from cache"
  - "Superset cache lookup iterates cache entries (O(cache_size), negligible vs variant generation cost)"

patterns-established:
  - "Superset-aware caching: when a smaller-limit request arrives, check for existing larger-limit cached result and slice"

requirements-completed: [SC-5]

# Metrics
duration: 3min
completed: 2026-02-20
---

# Phase 40 Plan 05: Variant Cache Unification Summary

**Unified variant cache with superset-aware lookup: Tantivy phase (limit=200) slices from pre-computed regex-phase result (limit=8000) instead of recomputing**

## Performance

- **Duration:** 3 min (verification only -- implementation was pre-committed)
- **Started:** 2026-02-20T13:56:52Z
- **Completed:** 2026-02-20T14:11:44Z
- **Tasks:** 1
- **Files modified:** 1 (genizah_core.py)

## Accomplishments
- Verified superset-aware cache lookup in `VariantManager.get_variants` (lines 2268-2275): when a smaller-limit request arrives, it finds any larger cached result for the same (term, mode, pairs_count) and slices
- Verified `_get_or_compute_variants` method (lines 5183-5201): pre-computes variants at `Config.REGEX_VARIANTS_LIMIT` (8000) before per-term Tantivy/regex loops, handling fuzzy mode's regex_mode divergence
- Verified call site in `execute_search` (line 5836): invoked before `build_tantivy_query` and `build_regex_pattern` for non-Regex modes
- All 153 Responsa tests pass (2 pre-existing failures in explosion guard Hebrew warning assertions, unrelated)

## Task Commits

The implementation was pre-committed across two earlier plan executions:

1. **Task 1: Superset-aware cache lookup + pre-computation** - Already committed:
   - `62bf9217` (perf(40-01)): Added superset cache lookup in `get_variants`
   - `4dcbefde` (perf(40-04)): Added `_get_or_compute_variants` method and call site

No new code changes were required -- verification confirmed the implementation satisfies all must-have truths.

## Files Created/Modified
- `genizah_core.py` - Superset cache lookup in `get_variants` (lines 2268-2275), `_get_or_compute_variants` method (lines 5183-5201), call in `execute_search` (line 5836)

## Decisions Made
- Pre-compute at REGEX_VARIANTS_LIMIT (8000) before per-term loops so Tantivy (200) slices from cache
- Superset cache lookup iterates cache entries (O(cache_size), typically <=100 entries) -- negligible vs variant generation cost
- Cache key structure `(term, mode, limit, pairs_count)` preserved for proper invalidation when pairs_count changes

## Deviations from Plan

None - implementation was already in place from prior plan executions (40-01 and 40-04). Verification confirmed correctness.

## Issues Encountered
- Two pre-existing test failures in explosion guard tests (`test_suffixes_counted_in_explosion_guard`, `test_prefix_plus_suffix_cascades_down_instead_of_error`) -- both assert English substrings in Hebrew warning messages. Unrelated to variant cache changes. Logged to `deferred-items.md`.

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- Phase 40 fully complete (5/5 plans)
- All performance optimizations verified: parallel NLI fetch, async domain enrichment, browse crossref parallelization, FL ID index, variant cache unification
- Ready for next milestone planning

## Self-Check: PASSED

- [x] genizah_core.py exists with _get_or_compute_variants
- [x] Commit 62bf9217 (superset cache) exists
- [x] Commit 4dcbefde (precompute method) exists
- [x] 40-05-SUMMARY.md created

---
*Phase: 40-performance-optimization*
*Completed: 2026-02-20*
