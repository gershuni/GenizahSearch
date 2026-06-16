---
phase: 115-performance-metrics
plan: 04
subsystem: telemetry
tags: [telemetry, posthog, qt, threading, my-library, indexing, performance]

# Dependency graph
requires:
  - phase: 115-performance-metrics
    plan: 02
    provides: "desktop/telemetry.py accumulator, track_performance, INDEXING_COMPLETE enum, _normalize_operation_kind, _ALLOWED_PROPS with operation_kind/doc_count_bucket"

provides:
  - "desktop/my_library_tab.py: LocalIndexerWorker timed monotonically with _elapsed_ms + _operation_kind constructor arg"
  - "desktop/my_library_tab.py: _start_worker threads operation_kind (incremental_add default, reindex_all from reindex-all path)"
  - "desktop/my_library_tab.py: _queued_action lambda binds operation_kind at capture time (race-free per REVIEWS finding 6)"
  - "desktop/my_library_tab.py: _on_worker_finished stashes elapsed_ms/operation_kind from finished worker before None-clear; emits INDEXING_COMPLETE after UI teardown and before queued-action dispatch"
  - "desktop/my_library_tab.py: LabRebuildWorker.finished_signal updated to pyqtSignal(float, int)"
  - "desktop/my_library_tab.py: LabRebuildWorker.run() timed monotonically, emits (elapsed_ms, 0)"
  - "desktop/my_library_tab.py: _on_lab_rebuild_finished slot updated to (elapsed_ms, total_docs); emits INDEXING_COMPLETE with operation_kind='lab_rebuild'"

affects:
  - 115-performance-metrics (plans 05+, if any)

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Phase 115 timing pattern: t0 = time.monotonic() as FIRST line of run(); _elapsed_ms stored on instance before finished_signal.emit (Pitfall 2)"
    - "Race-free operation_kind: stash from finished worker into locals BEFORE self._worker = None; emit BEFORE _queued_action dispatch (REVIEWS finding 6)"
    - "Queued-action operation_kind preservation: lambda ok=operation_kind captures value at binding time, not at call time"
    - "LabRebuildWorker signal upgrade: pyqtSignal() -> pyqtSignal(float, int); slot signature updated to match; PyQt6 auto-routes args"
    - "Literal-constant discipline: operation_kind values are hardcoded string literals (incremental_add/reindex_all/lab_rebuild), never UI accessor values (D-02/D-04)"
    - "Lazy telemetry import: `from desktop import telemetry` inside try/except; emit only when telemetry.is_enabled(); except Exception: pass"

key-files:
  created: []
  modified:
    - desktop/my_library_tab.py

key-decisions:
  - "initial_scan defined in enum (D-02) but intentionally NOT populated this phase (REVIEWS finding 10) — non-reindex scans emit incremental_add; first-scan detection deferred to a future pass"
  - "LabRebuildWorker doc count emits 0 as the unknown sentinel (rebuild path returns no count); doc_count_bucket='0' is acceptable per the plan spec"
  - "operation_kind stashed from finished worker into locals (not a tab-level attribute) to prevent the queued-action re-entry race (REVIEWS finding 6)"
  - "Emit placed AFTER UI teardown but BEFORE _queued_action dispatch so the event belongs to the just-finished run, not the queued one"

patterns-established:
  - "Indexing telemetry: emit from UI-thread completion slot (not from worker run()) using stashed locals from the finished worker"
  - "Race prevention: stash mutable worker state before clearing self._worker; dispatch queued actions after emit"

requirements-completed: [PERF-01, PERF-02]

# Metrics
duration: 25min
completed: 2026-06-16
---

# Phase 115 Plan 04: Indexing Duration Telemetry — LocalIndexerWorker + LabRebuildWorker Summary

**Monotonic timing + race-free operation_kind threading in my_library_tab.py; INDEXING_COMPLETE emitted from UI-thread slots with coarse doc_count_bucket via track_performance chokepoint**

## Performance

- **Duration:** ~25 min
- **Started:** 2026-06-16T00:00:00Z (approx)
- **Completed:** 2026-06-16
- **Tasks:** 2
- **Files modified:** 1

## Accomplishments

- LocalIndexerWorker now carries `_elapsed_ms` (monotonic, stored in `run()` before emit) and `_operation_kind` constructor arg (literal constant, default `incremental_add`)
- `_start_worker` threads `operation_kind` parameter through to worker construction; `_queued_action` lambda binds the value at capture time (not call time) so a queued `reindex_all` cannot be silently re-tagged `incremental_add` (REVIEWS finding 6)
- `_on_reindex_all_clicked` passes `operation_kind='reindex_all'` — the only entry-point for that operation
- `_on_worker_finished` stashes `_elapsed_ms` and `_operation_kind` from the finished worker into locals BEFORE `self._worker = None`; emits `INDEXING_COMPLETE` AFTER all UI teardown but BEFORE `_queued_action` dispatch (race-free)
- `LabRebuildWorker.finished_signal` upgraded from `pyqtSignal()` to `pyqtSignal(float, int)`; `run()` times monotonically and emits `(elapsed_ms, 0)` (0 = unknown-doc-count sentinel)
- `_on_lab_rebuild_finished` slot signature updated to `(elapsed_ms: float, total_docs: int)`; emits `INDEXING_COMPLETE` with `operation_kind='lab_rebuild'` (literal constant) after existing reload logic
- 21 tests green: `test_telemetry_phase115.py` (11), `test_no_dynamic_telemetry_strings.py` (6), `test_telemetry_allowlist.py` (4); all literal-constant discipline enforced by D-17 AST guard (REVIEWS finding 7)

## Task Commits

Each task was committed atomically:

1. **Task 1: Time LocalIndexerWorker; thread operation_kind through _start_worker + _queued_action; emit INDEXING_COMPLETE in _on_worker_finished** - `055891e0` (feat)
2. **Task 2: Time LabRebuildWorker (signal → (float,int)) + emit INDEXING_COMPLETE in _on_lab_rebuild_finished** - `a476c7b8` (feat)

**Plan metadata:** (this commit — docs)

## Files Created/Modified

- `desktop/my_library_tab.py` — added `import time`; LocalIndexerWorker timing + operation_kind; _start_worker operation_kind param + queued-action lambda fix; _on_reindex_all_clicked reindex_all literal; _on_worker_finished stash + emit; LabRebuildWorker signal upgrade + timing; _on_lab_rebuild_finished slot signature + emit

## Decisions Made

- `initial_scan` is defined in `DesktopEvent` enum (plan 02) but intentionally not populated this phase (REVIEWS finding 10) — a comment notes it is "reserved for a future first-scan-detection pass"; non-reindex scans emit `incremental_add`
- LabRebuildWorker signals option (a) chosen (Pitfall 5): change `finished_signal` to `(float, int)` rather than storing elapsed on instance; the only connected slot is `_on_lab_rebuild_finished` so the change is low-risk
- Doc count for LAB rebuild is 0 (unknown sentinel) — `rebuild_local_lab_index` returns no count; `doc_count_bucket='0'` is documented as acceptable
- operation_kind stashed from finished worker into locals (not a tab-level `_last_operation_kind` attribute) per REVIEWS finding 6 — prevents queued-action re-entry race

## Deviations from Plan

None - plan executed exactly as written.

The plan's PATTERNS.md showed an alternative using `self._worker_last_elapsed_ms.value` / `self._last_operation_kind` tab attributes — these were NOT used. Instead, the plan's own <action> section (and REVIEWS finding 6) correctly specifies reading from the finished worker into locals before `self._worker = None`. The PATTERNS.md snippet was an earlier draft; the task <action> text is authoritative and was followed.

## Issues Encountered

None.

## Threat Surface Scan

No new network endpoints, auth paths, file access patterns, or schema changes introduced. The only new surface is:
- `T-115-PRIV-7` (mitigated): doc count bucketed (0/1-9/10-99/100+); raw `indexed` integer never placed on event
- `T-115-PRIV-8` (mitigated): operation_kind is a producer-side literal constant routed through `_normalize_operation_kind`; D-17 AST guard enforces this
- `T-115-PRIV-9` (mitigated): emit from UI-thread slots only
- `T-115-INTEG-2` (mitigated): race-free via local stash before worker clear + lambda default-arg binding

## Known Stubs

None.

## Next Phase Readiness

Phase 115 plan 04 complete. All four plans of Phase 115 are now complete:
- Plan 01: D-17 dynamic-string guard extended to cover my_library_tab.py + track_performance/accumulate_performance
- Plan 02: telemetry.py accumulator, flush functions, INDEXING_COMPLETE enum, _normalize_operation_kind
- Plan 03: gui_threads.py perf_signal + genizah_app.py wiring + flush hooks
- Plan 04: my_library_tab.py indexing producers (this plan)

Ready for `/gsd:verify-work 115` or `/gsd:complete-milestone`.

## Self-Check: PASSED

- `desktop/my_library_tab.py` exists on disk: FOUND
- Task 1 commit `055891e0`: FOUND (git log confirmed)
- Task 2 commit `a476c7b8`: FOUND (git log confirmed)
- `DesktopEvent.INDEXING_COMPLETE` in my_library_tab.py: FOUND (line 1871)
- `self._operation_kind` in my_library_tab.py: FOUND (line 732)
- `self._elapsed_ms` in my_library_tab.py: FOUND (line 731)
- `operation_kind='reindex_all'` in _on_reindex_all_clicked: FOUND (line 2376)
- `finished_signal = pyqtSignal(float, int)` in LabRebuildWorker: FOUND (line 783)
- `'lab_rebuild'` in _on_lab_rebuild_finished: FOUND (line 1247)
- `_on_lab_rebuild_finished(self, elapsed_ms: float, total_docs: int)`: FOUND (line 1223)
- 21 tests green (11 + 6 + 4): PASSED
- ast.parse OK: PASSED

---
*Phase: 115-performance-metrics*
*Completed: 2026-06-16*
