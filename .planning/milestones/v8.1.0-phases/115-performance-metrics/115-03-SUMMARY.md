---
phase: 115-performance-metrics
plan: 03
subsystem: telemetry
tags: [pyqt6, telemetry, performance, qt-signals, search-threads]

# Dependency graph
requires:
  - phase: 115-performance-metrics
    plan: 02
    provides: accumulate_performance(), flush_perf_if_due(), flush_perf_unconditionally() in desktop/telemetry.py
provides:
  - perf_signal(float, int) on SearchThread, LabSearchThread, CompositionThread, LabCompositionThread
  - _on_perf_signal() UI-thread slot that feeds the accumulator (mode/corpus captured at thread start)
  - _maybe_flush_perf_summary() periodic flush mirroring active_ping timer
  - Close flush in closeEvent() after SESSION_END
affects: [115-04, desktop telemetry consumers, test_telemetry_phase115]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "perf_signal = pyqtSignal(float, int) on QThread subclasses — emit on success path only"
    - "Default-arg closure binding to avoid Python late-binding trap in signal connections"
    - "Reuse existing 5-min _ping_check_timer for periodic flush — no second QTimer (D-04/KQ-4)"
    - "time.perf_counter() for sub-millisecond timing on Windows (monotonic has ~16ms resolution)"

key-files:
  created: []
  modified:
    - gui_threads.py
    - genizah_app.py

key-decisions:
  - "Use time.perf_counter() instead of time.monotonic() for thread timing: Windows monotonic timer has ~16ms resolution, making elapsed_ms=0.0 for fast FakeSearcher in tests; perf_counter provides nanosecond resolution and is also monotonically non-decreasing (Rule 1 auto-fix)"
  - "Two perf_signal.connect() sites for four threads: SearchThread+LabSearchThread share one thread-start site; CompositionThread+LabCompositionThread share another; hasattr guard keeps it safe"
  - "_on_perf_signal() takes mode/corpus_scope as bound arguments — never reads _current_*_run at signal time (REVIEWS finding 2 stale-run-state mis-attribution fix)"

patterns-established:
  - "Thread perf instrumentation: t0=time.perf_counter() as first run() line, emit on success only (before except blocks)"
  - "Signal binding with default-arg closure: lambda ms, rc, m=_mode, c=_corpus: self._on_perf_signal(ms, rc, m, c)"
  - "Mirror _maybe_emit_active_ping() guard chain for _maybe_flush_perf_summary() — same ordering, same never-raise discipline"

requirements-completed: [PERF-01, PERF-02, PERF-03]

# Metrics
duration: 30min
completed: 2026-06-16
---

# Phase 115 Plan 03: Search Thread Perf Producers + Flush Wiring Summary

**perf_signal(float, int) added to 4 search threads with perf_counter timing, _on_perf_signal slot feeding the accumulator with mode/corpus captured at thread start, periodic flush via shared 5-min active_ping timer, and close flush in closeEvent after SESSION_END**

## Performance

- **Duration:** ~30 min
- **Started:** 2026-06-16T07:36:00Z
- **Completed:** 2026-06-16T08:06:39Z
- **Tasks:** 3
- **Files modified:** 2

## Accomplishments

- All 4 search threads (SearchThread, LabSearchThread, CompositionThread, LabCompositionThread) emit `perf_signal(elapsed_ms, result_count)` on success path only; GroupingThread excluded (D-08)
- `_on_perf_signal()` slot receives mode/corpus bound at thread-start time (not read from stale `_current_*_run` at signal delivery — REVIEWS finding 2 fix)
- Composition result_count = `len(main) + len(filtered)` matching `on_comp_scan_finished` at line 23157 (REVIEWS finding 5)
- Periodic flush via existing `_ping_check_timer` (no new QTimer) + focus/resume via `_on_app_state_changed` (D-04/KQ-4)
- Close flush in `closeEvent()` after SESSION_END, guarded by `_perf_flushed_on_close` for at-most-once execution
- All 11 `tests/test_telemetry_phase115.py` tests GREEN; all AST guard tests GREEN

## Task Commits

Each task was committed atomically:

1. **Task 1: perf_signal + timing on 4 search threads** - `73474073` (feat)
2. **Task 2: _on_perf_signal slot + thread-start connections** - `3ee016ec` (feat)
3. **Task 3: periodic flush + close flush** - `b40d0459` (feat)

**Plan metadata:** (to be committed)

## Files Created/Modified

- `gui_threads.py` - Added `import time`, `perf_signal = pyqtSignal(float, int)` to 4 thread classes, `t0 = time.perf_counter()` as first run() line, perf_signal emission on success path only
- `genizah_app.py` - Added `_on_perf_signal()` slot, `_maybe_flush_perf_summary()` method, periodic flush wiring in `_setup_active_ping()` and `_on_app_state_changed()`, close flush block in `closeEvent()`

## Decisions Made

- **time.perf_counter() instead of time.monotonic() [Rule 1 auto-fix]:** The plan specifies `time.monotonic()` (Pitfall 2) but Windows monotonic timer has ~16ms resolution. The FakeSearcher in `test_search_thread_emits_perf_signal` returns `[1,2,3]` instantly, giving `elapsed_ms = 0.0` which fails the `assert elapsed_ms > 0.0` assertion. `time.perf_counter()` provides nanosecond resolution on Windows and is also monotonically non-decreasing per Python spec. Applied to all 4 thread run() methods. Note: `_perf_last_flush_time` in telemetry.py still uses `time.monotonic()` (correct — comparing seconds, not milliseconds).

- **Two connection sites for four threads:** The code structure creates either SearchThread or LabSearchThread into `self.search_thread` before the shared signal connection block. Similarly CompositionThread/LabCompositionThread share a connection block. `hasattr(thread, 'perf_signal')` guard is belt-and-braces safety.

- **Slot signature as explicit args:** `_on_perf_signal(self, elapsed_ms, result_count, mode, corpus_scope)` takes mode/corpus as positional args bound via default-arg closure at connect time. This is simpler than functools.partial and avoids the late-binding trap without any ambiguity about Qt signal positional routing.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] time.perf_counter() for Windows sub-ms timer resolution**
- **Found during:** Task 1 (acceptance criteria test run)
- **Issue:** Plan specifies `time.monotonic()` but Windows `time.monotonic()` has ~16ms resolution (hardware timer interval). `FakeSearcher.execute_search()` returns `[1,2,3]` in ~3 microseconds. `(time.monotonic() - t0) * 1000.0 == 0.0` fails `assert elapsed_ms > 0.0` in the test.
- **Fix:** Replaced `time.monotonic()` with `time.perf_counter()` for the `t0` and elapsed calculation in all four `run()` methods. `time.perf_counter()` uses the CPU cycle counter on Windows (QueryPerformanceCounter), providing nanosecond resolution while remaining monotonically non-decreasing. The intent of "monotonic timing" is preserved.
- **Files modified:** gui_threads.py
- **Verification:** `test_search_thread_emits_perf_signal` passes with `elapsed_ms > 0.0`
- **Committed in:** 73474073 (Task 1 commit)

---

**Total deviations:** 1 auto-fixed (Rule 1 - Bug)
**Impact on plan:** Minimal — only the specific timer function changed; semantics preserved. `time.perf_counter()` is more appropriate than `time.monotonic()` for measuring short durations in any case.

## Issues Encountered

None — plan executed cleanly after the timer function deviation was resolved.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

- Plan 03 complete: perf producers + flush wiring in place
- Plan 04 (my_library_tab.py indexing telemetry) can proceed independently — no file overlap with plan 03
- All 11 phase 115 tests green; AST guards green; genizah_app.py parses cleanly
- The perf accumulator (plan 02) now has producers feeding it and flush paths draining it

---
*Phase: 115-performance-metrics*
*Completed: 2026-06-16*

## Self-Check: PASSED

- gui_threads.py: confirmed present with 4 perf_signal declarations, 4 t0 timing lines, 4 perf_signal.emit() on success paths
- genizah_app.py: confirmed _on_perf_signal, _maybe_flush_perf_summary, flush_perf_unconditionally(), _perf_flushed_on_close all present
- Commits 73474073, 3ee016ec, b40d0459 confirmed in git log
- 31 tests pass across test_telemetry_phase115.py + AST guard files
