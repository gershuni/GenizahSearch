---
phase: 115-performance-metrics
plan: 02
subsystem: telemetry
tags: [posthog, desktop, performance, accumulator, privacy, consent]

# Dependency graph
requires:
  - phase: 115-01
    provides: test scaffold (tests/test_telemetry_phase115.py + updated test_no_dynamic_telemetry_strings.py)
  - phase: 114-usage-analytics
    provides: desktop/telemetry.py chokepoint, DesktopEvent enum, _ALLOWED_PROPS, set_consent, _reset_for_tests

provides:
  - accumulate_performance() — ingest-only, consent-gated, mode/corpus-normalized
  - _flush_perf_summary() — single summary event with session_id, D-06 reset, D-03 buckets
  - flush_perf_if_due() — periodic flush with validated GENIZAH_PERF_FLUSH_INTERVAL
  - flush_perf_unconditionally() — close flush
  - _clear_perf_accumulator() — zeroes accumulator, wired into set_consent(False)
  - DesktopEvent.INDEXING_COMPLETE enum member
  - perf_summary / operation_kind / doc_count_bucket added to _ALLOWED_PROPS
  - _normalize_mode / _normalize_corpus / _normalize_operation_kind / _normalize_flush_reason
  - _perf_env_int / _perf_env_float — validated env-knob readers
  - _percentile() — exact percentile over float list

affects: [115-03, 115-04, genizah_app.py, desktop/my_library_tab.py]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "accumulate-never-emit: accumulate_performance() writes to in-memory dict only; flush emits once"
    - "normalize-before-key: mode/corpus normalized to fixed sets before becoming nested dict keys (REVIEWS finding 3)"
    - "validated-env-readers: _perf_env_int/_perf_env_float clamp/default invalid values (REVIEWS finding 8)"
    - "consent-08-parity: set_consent(False) clears perf accumulator alongside queue drain (REVIEWS finding 1)"
    - "single-source-of-truth reset: _clear_perf_accumulator() used by both set_consent(False) and _reset_for_tests()"

key-files:
  created: []
  modified:
    - desktop/telemetry.py

key-decisions:
  - "corpus_unknown omitted from flushed stats dict (only genizah/local/all emitted) — matches test_perf_summary_buckets_only spec"
  - "session_id sourced from _current_distinct_id or _install_id (no separate module-level session_id) — satisfies REVIEWS finding 4 non-empty assertion"
  - "import time added at module top (telemetry.py previously used time only in __main__ block)"
  - "test scaffold files ported from master-main merge commit 640dbace (not in worktree branch from c9571f0d)"

patterns-established:
  - "Phase 115 accumulator: dict keyed by normalized mode; per-mode: durations_ms, result_counts, zero_result_count, corpus_counts"
  - "D-03 bucket logic inlined in _flush_perf_summary with comment pointing to genizah_app._telemetry_result_bucket() as canonical source"
  - "Env-knob discipline: only _perf_env_int/_perf_env_float read GENIZAH_PERF_SAMPLE_N/GENIZAH_PERF_FLUSH_INTERVAL; no bare int()/float()"

requirements-completed: [PERF-01, PERF-02, PERF-03]

# Metrics
duration: 8min
completed: 2026-06-16
---

# Phase 115 Plan 02: Performance Accumulator + Flush Machinery Summary

**In-memory perf accumulator + one-summary-per-session flush engine added to desktop/telemetry.py, with REVIEWS privacy fixes: opt-out clears the buffer, mode/corpus normalized before use as dict keys, env knobs validated against clamp/default**

## Performance

- **Duration:** 8 min
- **Started:** 2026-06-16T07:46:06Z
- **Completed:** 2026-06-16T07:54:13Z
- **Tasks:** 3
- **Files modified:** 1 (desktop/telemetry.py) + 2 test files ported from plan-01

## Accomplishments

- DesktopEvent.INDEXING_COMPLETE added; `perf_summary`/`operation_kind`/`doc_count_bucket` allowlisted
- accumulate_performance() ingests consent-gated, mode/corpus-normalized, sample_n-throttled perf records — never emits
- _flush_perf_summary() builds one aggregate event with session_id + D-03 coarse buckets + D-06 reset; flush_perf_if_due() + flush_perf_unconditionally() are the public entry-points
- All three REVIEWS HIGH fixes applied: opt-out clears accumulator (finding 1), mode/corpus normalized before keying (finding 3), env knobs validated via dedicated readers (finding 8)
- session_id attaches to summary (REVIEWS finding 4 / MEDIUM)

## Task Commits

Each task was committed atomically:

1. **Test scaffold port (plan-01 prerequisites)** - `a32c69ed` (test)
2. **Tasks 1-3: accumulator + flush machinery in desktop/telemetry.py** - `7775a0be` (feat)

## Files Created/Modified

- `desktop/telemetry.py` — Added 288 lines: INDEXING_COMPLETE enum member, _ALLOWED_PROPS expansion, import time, 3 accumulator globals, _clear_perf_accumulator(), 4 normalizer functions, _perf_env_int/_perf_env_float, _percentile(), accumulate_performance(), _flush_perf_summary(), flush_perf_if_due(), flush_perf_unconditionally(); set_consent(False) wired; _reset_for_tests() extended
- `tests/test_telemetry_phase115.py` — Created (ported from plan-01 merge on master-main)
- `tests/test_no_dynamic_telemetry_strings.py` — Updated (ported from plan-01 merge on master-main)

## Decisions Made

- `corpus_unknown` is tracked in the accumulator's `corpus_counts` dict but NOT emitted in the per-mode stats in the flushed summary (test_perf_summary_buckets_only spec does not include it in `_ALLOWED_STAT_KEYS`)
- `session_id` sourced from `_current_distinct_id or _install_id` (both set after `set_consent(True)`) — no separate module-level session_id global needed; satisfies the non-empty assertion in the test
- Test files (test_telemetry_phase115.py + test_no_dynamic_telemetry_strings.py update) were committed to plan-01 on master-main at commit 640dbace but were not present in this worktree (created from c9571f0d before that merge); ported here as the first commit

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] corpus_unknown emitted in stats but not in test's allowed-stat-keys set**
- **Found during:** Task 3 (test_perf_summary_buckets_only)
- **Issue:** Implementation emitted `corpus_unknown` in the per-mode stats dict but the test's `_ALLOWED_STAT_KEYS` does not include it (the test is the spec from plan-01)
- **Fix:** Removed `corpus_unknown` from `_flush_perf_summary` per-mode stats dict; the `unknown` bucket is retained in the accumulator for correctness but not emitted
- **Files modified:** desktop/telemetry.py
- **Committed in:** 7775a0be

---

**Total deviations:** 1 auto-fixed (Rule 1 spec alignment)
**Impact on plan:** Minor, no scope creep. corpus_unknown still tracked internally; just not emitted.

## Issues Encountered

- Plan-01 test scaffold not present in worktree (worktree created from c9571f0d, plan-01 merged to master-main in 640dbace after worktree creation); resolved by porting files from that commit

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

- Plan 03 can now wire `perf_signal` on SearchThread/LabSearchThread/CompositionThread/LabCompositionThread and connect to `accumulate_performance()` in genizah_app.py
- Plan 04 can wire INDEXING_COMPLETE in my_library_tab.py using `track_performance()`
- `test_search_thread_emits_perf_signal` remains RED until plan 03 adds `perf_signal` to SearchThread

---
*Phase: 115-performance-metrics*
*Completed: 2026-06-16*
