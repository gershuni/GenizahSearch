---
phase: 115-performance-metrics
plan: 01
subsystem: testing
tags: [telemetry, posthog, desktop, pytest, ast-guard, phase115]

# Dependency graph
requires:
  - phase: 111-telemetry-foundation
    provides: desktop.telemetry module, _reset_for_tests(), posthog_server queue seam
  - phase: 114-telemetry-usage-events
    provides: test_telemetry_consent_gate.py autouse fixture shape, _scrub_props/_validate_props

provides:
  - tests/test_telemetry_phase115.py — 11-case Wave 0 scaffold (RED until plans 02-04 land producers)
  - tests/test_no_dynamic_telemetry_strings.py extended — my_library_tab.py + track_performance/accumulate_performance covered

affects: [115-02-PLAN, 115-03-PLAN, 115-04-PLAN]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Lazy symbol reference in RED tests: import inside test body so collection succeeds before producers exist"
    - "autouse fixture with monkeypatched in-memory config + fresh queue.Queue per test (copied from Phase 111)"
    - "Surgical TARGET_FILES/EMIT_HELPERS extension for AST guard (never weaken argument-scoped visitor)"

key-files:
  created:
    - tests/test_telemetry_phase115.py
  modified:
    - tests/test_no_dynamic_telemetry_strings.py

key-decisions:
  - "All 11 test symbols (accumulate_performance, _flush_perf_summary, etc.) referenced lazily inside test bodies — no top-level import — so pytest collection succeeds before plan 02 lands"
  - "test_no_per_search_events also asserts session_id is present on the summary (REVIEWS finding 4 inclusion)"
  - "test_perf_summary_buckets_only uses _ALLOWED_STAT_KEYS set to verify no unexpected keys, not just bucket presence"
  - "Task 3: only my_library_tab.py added to TARGET_FILES (telemetry.py itself is the chokepoint, not a caller)"

patterns-established:
  - "Wave 0 TDD scaffold: write test file first, keep RED, turn GREEN as producers land in subsequent plans"
  - "D-17 guard extension pattern: add new callers to TARGET_FILES + new emit helpers to EMIT_HELPERS; add non-vacuous synthetic test to prove detection"

requirements-completed: [PERF-01, PERF-02, PERF-03]

# Metrics
duration: 3min
completed: 2026-06-16
---

# Phase 115 Plan 01: Performance Metrics Wave 0 Test Scaffold Summary

**11-case pytest Wave 0 scaffold (RED) + extended D-17 AST guard covering Phase 115 perf call sites in my_library_tab.py**

## Performance

- **Duration:** 3 min
- **Started:** 2026-06-16T07:37:33Z
- **Completed:** 2026-06-16T07:40:50Z
- **Tasks:** 3 (Tasks 1+2 in one file, Task 3 separate)
- **Files modified:** 2

## Accomplishments

- Created `tests/test_telemetry_phase115.py` with autouse reset fixture, `_enable_telemetry` helper, and all 11 named test cases (8 original + 3 REVIEWS additions: opt-out-clears-accumulator, unknown-mode-normalized, env-knob-clamped); collection succeeds with 11 tests
- Extended `tests/test_no_dynamic_telemetry_strings.py` to scan `desktop/my_library_tab.py` and recognize `track_performance`/`accumulate_performance` in EMIT_HELPERS (REVIEWS finding 7), plus added non-vacuous `test_lint_rejects_perf_accessor_violation` synthetic test
- All 6 guard tests pass green immediately; phase115 tests are correctly RED (producers absent) until plans 02-04 land

## Task Commits

1. **Tasks 1+2: autouse fixture + 11 test cases** - `e47321a1` (test)
2. **Task 3: extend D-17 guard** - `cfcccad0` (test)

**Plan metadata:** (committed below with SUMMARY.md)

## Files Created/Modified

- `tests/test_telemetry_phase115.py` — New file: Wave 0 scaffold with autouse fixture, `_enable_telemetry` helper, and 11 test functions covering PERF-01/02/03 + D-05/06/07/09 + CONSENT-08 + REVIEWS findings 1/3/4/8
- `tests/test_no_dynamic_telemetry_strings.py` — Extended: TARGET_FILES += `desktop/my_library_tab.py`; EMIT_HELPERS += `track_performance`, `accumulate_performance`; new `test_lint_rejects_perf_accessor_violation` synthetic test

## Decisions Made

- Symbols that don't exist yet (`accumulate_performance`, `_flush_perf_summary`, `DesktopEvent.INDEXING_COMPLETE`, `perf_signal`, etc.) are imported lazily inside test bodies — this is the load-bearing design that allows collection to succeed while keeping tests RED.
- `test_no_per_search_events` includes the REVIEWS finding 4 assertion (`session_id` present on summary) so plan 02 implementers know it is required.
- `test_perf_summary_buckets_only` defines `_ALLOWED_STAT_KEYS` inline to catch any unexpected extra keys that might leak raw data.
- Task 3 did NOT add `desktop/telemetry.py` to TARGET_FILES — it is the chokepoint (legitimate home of helper definitions), not a caller; guard checks callers only.

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered

- Windows `cp1255` console encoding caused `ast.parse(open(...).read())` to fail without `PYTHONUTF8=1`; all verification commands used `PYTHONUTF8=1` flag. This is a known Windows issue (see `reference_check_docs_utf8` memory) and does not affect the file or tests.

## Known Stubs

None — this plan writes only test files. No stubs in test scaffolding.

## Threat Flags

None — test files only; no new network endpoints, auth paths, or schema changes.

## Self-Check: PASSED

- `tests/test_telemetry_phase115.py` exists: FOUND
- `tests/test_no_dynamic_telemetry_strings.py` exists: FOUND (modified)
- Commit `e47321a1` exists: FOUND (git log confirmed)
- Commit `cfcccad0` exists: FOUND (git log confirmed)
- Collection: 11 tests collected, exit 0
- Guard tests: 6 passed, exit 0

## Next Phase Readiness

- `tests/test_telemetry_phase115.py` is ready to serve as the RED/GREEN feedback loop for plans 02-04
- Plans 02 (accumulator + flush in telemetry.py) will turn tests 2/3/4/5/6/7/8/9/10/11 green
- Plan 03 (perf_signal in gui_threads.py) will turn test 1 green
- Plan 04 (my_library_tab.py call sites) will be validated by the extended D-17 guard

---
*Phase: 115-performance-metrics*
*Completed: 2026-06-16*
