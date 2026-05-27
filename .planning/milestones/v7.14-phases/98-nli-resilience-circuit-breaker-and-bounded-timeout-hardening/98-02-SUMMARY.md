---
phase: 98-nli-resilience-circuit-breaker-and-bounded-timeout-hardening
plan: 02
subsystem: resilience
tags: [circuit-breaker, threading, monotonic-time, posthog, nyquist, shared-module]

# Dependency graph
requires:
  - phase: 98-01
    provides: [shared.posthog_server.enqueue_event (fire-and-forget telemetry queue)]
provides:
  - shared.nli_circuit_breaker.is_open() -> bool
  - shared.nli_circuit_breaker.record_failure(failure_type, path) -> None
  - shared.nli_circuit_breaker.record_success(path='') -> None
  - shared.nli_circuit_breaker._state_snapshot() / _reset_for_tests() (test seams)
  - Env-driven constants: NLI_CIRCUIT_THRESHOLD, NLI_CIRCUIT_WINDOW,
    NLI_CONNECT_TIMEOUT, NLI_IIIF_READ_TIMEOUT, NLI_MARC_READ_TIMEOUT,
    NLI_IMAGE_READ_TIMEOUT
  - tests/conftest.py autouse fixture _reset_nli_breaker_state (project-wide)
affects: [98-03, 98-04, 98-05, 98-06]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "module-level singleton guarded by threading.Lock"
    - "time.monotonic() for breaker windows (NTP-immune)"
    - "telemetry emission OUTSIDE the lock (deadlock-safety per Pitfall 2)"
    - "autouse pytest fixture for module-state reset between tests"
    - "AST static guard for D-04 monotonic-time invariant"
    - "two-batch-spawn concurrency idiom with threading.Event for Nyquist test"

key-files:
  created:
    - shared/nli_circuit_breaker.py
    - tests/test_nli_circuit_breaker.py
  modified:
    - tests/conftest.py

key-decisions:
  - "Deferred-import of shared.posthog_server inside _safe_emit_* helpers (call-time resolution allows monkeypatching by tests via setattr on the ph module)"
  - "Auto-recovery semantics: window-elapse does NOT reset failure counter (safer for flapping NLI; next failure re-trips after 1 increment)"
  - "Telemetry helpers private (_safe_emit_opened, _safe_emit_closed); never raise into caller per D-25"
  - "All env knobs floored by max(1, int(...)) to defend against malformed values"

patterns-established:
  - "Module-level singleton with threading.Lock + time.monotonic for resilience primitives in shared/"
  - "Two-batch spawn + threading.Event gate for strict Nyquist concurrency tests (replaces weaker sleep-then-race patterns)"
  - "Test-seam pattern: _state_snapshot() returns dict for assertions; _reset_for_tests() resets all module state"
  - "Autouse fixture in tests/conftest.py for project-wide module-state isolation"

requirements-completed: [D-01, D-02, D-03, D-04, D-05, D-06, D-07, D-08, D-09, D-24, D-25, D-26, D-27, D-28]

# Metrics
duration: 6min
completed: 2026-05-25
---

# Phase 98 Plan 02: Shared NLI Circuit Breaker Module Summary

**Module-level NLI circuit breaker (`shared/nli_circuit_breaker.py`) with threading.Lock-guarded state, time.monotonic() windows, fire-and-forget PostHog telemetry, and a strict 20-thread two-batch Nyquist test proving the 2026-05-25 hang cannot recur.**

## Performance

- **Duration:** ~6 min (3 task commits between 17:18:29 and 17:24:21 local)
- **Started:** 2026-05-25T17:14:00+03:00 (approximate — after base reset)
- **Completed:** 2026-05-25T17:24:21+03:00
- **Tasks:** 3
- **Files created:** 2 (shared/nli_circuit_breaker.py, tests/test_nli_circuit_breaker.py)
- **Files modified:** 1 (tests/conftest.py)
- **Tests added:** 27 (5 test classes)
- **Combined Wave 1+2 suite:** 47 tests pass in 2.60s

## Accomplishments

- Single source of truth for "is NLI degraded right now?" — consumed by Wave 3 (Plans 98-03/04/05) at 10 NLI call sites
- D-04 monotonic-time invariant pinned by AST guard test (`time.time()` cannot accidentally re-enter the module)
- D-26 STRENGTHENED concurrency test (per REVIEWS Issue 2) proves at most `THRESHOLD=3` network calls hit the wire even with 20 workers ready to fire — the exact property that prevents the 2026-05-25 outage
- D-27 lock-serialization test with 50 threads + `_ConcurrencyRecorder` instrumented lock proves `max_concurrent == 1` inside the critical region and no lost increments
- D-25 telemetry never raises into caller (monkeypatch raises RuntimeError from `enqueue_event` → breaker still works)
- Auto-recovery semantics encoded in module docstring + test: window-elapse does NOT reset counter (safer for flapping NLI)
- Project-wide autouse fixture in `tests/conftest.py` prevents module-state pollution across the existing 2500+ test suite

## Task Commits

1. **Task 1: Create shared/nli_circuit_breaker.py module** — `f3b99960` (feat)
   - 229-line module: public API (3 functions) + test seams (2 functions) + telemetry helpers (2 private) + 6 env-driven constants + Literal failure type + threading.Lock state
2. **Task 2: Add autouse fixture to tests/conftest.py** — `1f55ff83` (test)
   - 32-line append: project-wide `_reset_nli_breaker_state` autouse fixture (pre-yield + post-yield) with ImportError-safe defensive try/except
3. **Task 3: Create tests/test_nli_circuit_breaker.py** — `88ed7491` (test)
   - 524-line test file: 5 test classes / 27 tests including the strict D-26 two-batch Nyquist test (passes in ~2.3s vs 10s budget)

## Files Created/Modified

- `shared/nli_circuit_breaker.py` (created, 229 lines) — Module-level singleton. Public API: `is_open()`, `record_failure(failure_type, path)`, `record_success(path='')`. Test seams: `_state_snapshot()`, `_reset_for_tests()`. 6 env-driven constants exported via `__all__`. Uses `time.monotonic()` exclusively; no `web/` imports.
- `tests/test_nli_circuit_breaker.py` (created, 524 lines) — 5 test classes covering D-01..D-09, D-24..D-28 + AST/static guards + strict Nyquist two-batch concurrency test.
- `tests/conftest.py` (modified, +32 lines) — Appended `_reset_nli_breaker_state` autouse fixture. Existing Phase 85/95/97 fixtures and bridges untouched.

## Decisions Made

- **Deferred-import of `shared.posthog_server`** inside `_safe_emit_opened` / `_safe_emit_closed` rather than at module top — lets tests monkeypatch `ph.enqueue_event` and have the breaker pick up the patched version at call time. Plan-prescribed.
- **Auto-recovery does NOT reset counter** (RESEARCH Open Q2 recommendation): after a 60s window elapses without explicit `record_success`, the counter stays at N — the next failure re-trips after a single additional increment. Trade-off documented in module docstring + pinned by `test_auto_recovery_does_not_reset_counter`.
- **Telemetry helpers private (`_safe_emit_*`)** to keep the public surface tight; both wrap `enqueue_event` in `try/except Exception` and log at DEBUG so D-25 ("telemetry never raises") is satisfied structurally, not by convention.
- **Env knobs floored by `max(1, int(...))`** — malformed values (e.g., `NLI_CIRCUIT_THRESHOLD=0` or empty string handled to 1; non-numeric raises ValueError at import = fail-fast V5 input validation).
- **Two ThreadPoolExecutor matches in test file** (1 import + 1 use) vs. acceptance criterion saying "exactly 1 match" — the import line at top + the single instantiation in `test_20_threads_saturate_then_short_circuit` is the intended structure; only one ThreadPoolExecutor instance is created at runtime.

## Deviations from Plan

None — plan executed exactly as written. The plan provided complete, copy-ready code for all three tasks (the implementation, the autouse fixture, and the strengthened D-26 test). The strengthened D-26 test passes on the first attempt at ~2.3s, well within the 10s budget.

## Issues Encountered

- **Initial Write to wrong path:** The first Write call placed `shared/nli_circuit_breaker.py` in the main repo (`C:/GenizahSearch/shared/`) instead of the worktree (`C:/GenizahSearch/.claude/worktrees/agent-af7771d4cd58af518/shared/`). Corrected immediately by rewriting under the worktree path and removing the stray file from the main repo. No commits were affected because the wrong-path file was never staged.

## Verification

- `pytest tests/test_nli_circuit_breaker.py -x --tb=short` → 27 passed in 2.70s
- `pytest tests/test_nli_circuit_breaker.py::TestNliCircuitBreakerConcurrency::test_20_threads_saturate_then_short_circuit -v` → 1 passed in 2.34s (strict D-26 Nyquist test)
- `pytest tests/test_posthog_server.py tests/test_nli_circuit_breaker.py -x` → 47 passed in 2.60s (combined Wave 1+2 suite)
- `python -c "from shared.nli_circuit_breaker import is_open, record_failure, record_success; print('OK')"` → OK
- `python -c "from shared.nli_circuit_breaker import NLI_IIIF_READ_TIMEOUT, NLI_MARC_READ_TIMEOUT, NLI_IMAGE_READ_TIMEOUT, NLI_CONNECT_TIMEOUT; print(...)"` → `5 3 5 3`
- AST guard: no `time.time()` attribute access in module source (only docstring mention)
- No `web/` imports in module source

## Self-Check: PASSED

**Files exist:**
- FOUND: shared/nli_circuit_breaker.py
- FOUND: tests/test_nli_circuit_breaker.py
- FOUND: tests/conftest.py (modified)

**Commits exist:**
- FOUND: f3b99960 (Task 1)
- FOUND: 1f55ff83 (Task 2)
- FOUND: 88ed7491 (Task 3)

## Next Phase Readiness

Wave 3 (Plans 98-03, 98-04, 98-05) can now consume the breaker at all 10 NLI call sites:

```python
from shared.nli_circuit_breaker import (
    is_open, record_failure, record_success,
    NLI_CONNECT_TIMEOUT, NLI_IIIF_READ_TIMEOUT, NLI_MARC_READ_TIMEOUT, NLI_IMAGE_READ_TIMEOUT,
)
```

**Decisions closed by this plan:** D-01..D-09 (state model + config), D-24..D-28 (telemetry + tests). The remaining D-10..D-23 are call-site decisions and belong to Wave 3.

**Open considerations for Wave 3:**
- Codex REVIEWS Issue 3 (HIGH) — call-site plans must include breaker rechecks at fallback boundaries (MARC after IIIF, Rosetta after IIIF, FL-ID iteration loops, retry loops). The breaker itself is ready; the plans for 98-03/04/05 should already have this baked in per the re-plan.
- The breaker is process-local (D-05). If `genizah-web` ever migrates to multi-worker uvicorn, each worker gets its own breaker — acceptable degradation per CONTEXT.

---
*Phase: 98-nli-resilience-circuit-breaker-and-bounded-timeout-hardening*
*Completed: 2026-05-25*
