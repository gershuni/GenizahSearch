---
phase: 111-telemetry-foundation
plan: 01
subsystem: infra
tags: [posthog, telemetry, python, threading, tdd]

# Dependency graph
requires: []
provides:
  - "shared/posthog_server.py with 6 backward-compatible neutral additions: set_default_distinct_id, register_scrub_hook, set_capture_api_key, set_capture_host, _flush_before_exit, _drain_and_discard"
  - "tests/test_telemetry_posthog_server_ext.py with 18 behavioral tests (RED/GREEN TDD cycle)"
  - "Transport-layer foundation for Phase 111 Plan 02 (desktop/telemetry.py) and Phase 113 (crash hook)"
affects: [111-02, 111-03, 113, phase-111]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Module-level override globals guarded by a single shared lock (_capture_config_lock covers both _api_key_override and _host_override)"
    - "_resolve_api_key() / _resolve_capture_url() internal helpers that drain loop + flush both call, resolving (override or env)"
    - "Fail-closed scrub hook: hook exception drops event, never sends raw data (Pitfall 3)"
    - "TRUE deadline in _flush_before_exit: per-POST timeout = min(remaining, 2.0), stops POSTing when budget exhausted"
    - "Belt-and-suspenders test fixture: autouse from test_posthog_server.py + explicit new-global resets"

key-files:
  created:
    - tests/test_telemetry_posthog_server_ext.py
  modified:
    - shared/posthog_server.py

key-decisions:
  - "Key/host resolution is per-iteration in _drain_posthog_queue (long-lived loop can pick up a key set after daemon started) but once at flush-start in _flush_before_exit (one-shot bounded drain — stable during single flush)"
  - "One lock (_capture_config_lock) guards both _api_key_override and _host_override (they're always read/written together)"
  - "Fail-closed on hook exception: drop event rather than risk sending raw data (Pitfall 3 / T-111-02)"
  - "Drain-only (no POST) after deadline exhausted in _flush_before_exit — queue is drained completely but no HTTP after budget"

patterns-established:
  - "Transport config override: module global + dedicated lock + resolver helper (no os.environ mutation)"
  - "TDD RED/GREEN: test file committed at ERROR state, then implementation commits make all 18 tests GREEN"
  - "Backward-compatible neutral additions: new globals after _dropped_events_lock, new functions after existing helpers"

requirements-completed: [INFRA-03, INFRA-04, INFRA-05, CONSENT-08]

# Metrics
duration: 4min
completed: 2026-06-14
---

# Phase 111 Plan 01: PostHog Server Neutral Additions Summary

**6 backward-compatible neutral functions added to shared/posthog_server.py — desktop key override, default distinct_id injection, scrub hook slot, bounded flush, and opt-out drain — via TDD RED/GREEN cycle with 18 new tests all green**

## Performance

- **Duration:** 4 min
- **Started:** 2026-06-14T09:31:41Z
- **Completed:** 2026-06-14T09:35:25Z
- **Tasks:** 2 (RED + GREEN)
- **Files modified:** 2

## Accomplishments

- Added 6 new public functions to `shared/posthog_server.py` without changing any existing signature or behavior (INFRA-03)
- Closed REVIEWS HIGH-1: desktop API key override (`set_capture_api_key`) reaches the transport via `_resolve_api_key()` without mutating `os.environ`, so web POSTHOG_API_KEY env is completely unaffected (D-04)
- Closed REVIEWS HIGH-2: `_reset_for_tests()` now clears all 4 new module globals under their locks, eliminating test-order dependence
- Closed REVIEWS MEDIUM: `_flush_before_exit` enforces a TRUE per-POST deadline (`remaining = deadline - time.monotonic()` computed before each POST; stops POSTing drain-only once budget exhausted; `timeout=min(remaining, 2.0)`)
- Closed Pitfall 3: `_scrub_hook` is called BEFORE `_event_queue.put_nowait` so raw data never enters the queue; hook exception drops event (fail-closed)
- All 84 targeted tests pass (18 new + 66 existing posthog/nli); ruff clean on both files

## Task Commits

Each task was committed atomically:

1. **Task 1: RED — write tests/test_telemetry_posthog_server_ext.py** - `c9bc0d74` (test)
2. **Task 2: GREEN — implement the 5 neutral additions in shared/posthog_server.py** - `ce75e788` (feat)

**Plan metadata:** (docs commit follows)

_TDD tasks have multiple commits (test → feat)_

## Files Created/Modified

- `tests/test_telemetry_posthog_server_ext.py` — 18 behavioral tests for the 6 new functions (autouse fixture mirrors test_posthog_server.py; covers REVIEWS HIGH-1/MEDIUM/HIGH-2/D-04/Pitfall-3)
- `shared/posthog_server.py` — Extended with 6 neutral functions + 4 new module globals + 2 resolver helpers + extended _reset_for_tests + extended __all__; _drain_posthog_queue updated to resolve key+URL per-iteration

## Decisions Made

- Per-iteration key resolution in `_drain_posthog_queue` (long-lived loop) vs. once-at-start in `_flush_before_exit` (one-shot): both satisfy REVIEWS HIGH-1; the `_drain_posthog_queue` per-iteration approach is more robust for late key injection at desktop startup
- One shared `_capture_config_lock` for both `_api_key_override` and `_host_override` (they are always read/written atomically together)
- `Callable` imported from `typing` (not `collections.abc`) for Python 3.10 compatibility

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered

None - both RED and GREEN phases worked cleanly. All 84 tests passed on first run after implementation.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

- `shared/posthog_server.py` neutral additions are ready for Plan 02 (`desktop/telemetry.py`) which imports `set_default_distinct_id`, `register_scrub_hook`, `set_capture_api_key`/`set_capture_host`, and `_drain_and_discard`
- `_flush_before_exit` is ready for Phase 113 crash hook to call
- INFRA-03/04/05 + CONSENT-08 mechanism all satisfied

## TDD Gate Compliance

- RED gate: `test(111-01)` commit `c9bc0d74` — 18 tests all FAIL with AttributeError
- GREEN gate: `feat(111-01)` commit `ce75e788` — all 84 tests PASS

## Known Stubs

None — all functions are fully implemented and tested.

## Threat Flags

No new threat surface beyond the plan's `<threat_model>`. The 6 new functions are all process-local globals; no new network endpoints, auth paths, file access patterns, or schema changes.

## Self-Check: PASSED

- `shared/posthog_server.py` exists: FOUND
- `tests/test_telemetry_posthog_server_ext.py` exists: FOUND
- Commit `c9bc0d74` (RED): present in git log
- Commit `ce75e788` (GREEN): present in git log
- 84 tests pass: VERIFIED
- ruff clean: VERIFIED
- No _telemetry_enabled global in posthog_server.py (D-04): VERIFIED (grep returns 0)
- _api_key_override appears 8 times (>=2): VERIFIED
- enqueue_event signature unchanged: VERIFIED

---
*Phase: 111-telemetry-foundation*
*Completed: 2026-06-14*
