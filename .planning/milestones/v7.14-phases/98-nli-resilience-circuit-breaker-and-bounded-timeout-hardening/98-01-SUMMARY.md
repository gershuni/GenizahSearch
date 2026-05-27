---
phase: 98-nli-resilience-circuit-breaker-and-bounded-timeout-hardening
plan: 01
subsystem: infra
tags: [resilience, telemetry, posthog, shared-module, threading, queue]

# Dependency graph
requires:
  - phase: 78-search-helper-api
    provides: web/api_hardening.py:524-567 — the verbatim PostHog queue+daemon idiom that this plan factored out
provides:
  - shared.posthog_server.enqueue_event (fire-and-forget, thread-safe)
  - shared.posthog_server.get_dropped_event_count
  - shared.posthog_server.POSTHOG_CAPTURE_URL / POSTHOG_HOST
  - shared.posthog_server._reset_for_tests (test seam)
affects: [98-02 nli_circuit_breaker (Plan 02 will be first consumer); future: any background-thread telemetry needs]

# Tech tracking
tech-stack:
  added: []  # no new third-party dependencies — pure stdlib + already-vendored requests
  patterns:
    - "Module-level queue + daemon drain (verbatim from web/api_hardening.py:524-567)"
    - "Autouse pytest fixture for module-state reset + per-test queue swap (avoids daemon race)"
    - "Defensive copy of caller-supplied dict before enqueue"
    - "Static source-string pin for maxsize invariant (in addition to runtime assert)"

key-files:
  created:
    - shared/posthog_server.py
    - tests/test_posthog_server.py
    - .planning/phases/98-nli-resilience-circuit-breaker-and-bounded-timeout-hardening/98-01-SUMMARY.md
  modified: []  # NO existing files modified — REVIEWS.md Issue 5 Option A boundary holds

key-decisions:
  - "Option A (from REVIEWS.md Issue 5): web/api_hardening.py is NOT modified. Two separate PostHog queues + two drop counters coexist. Operators must monitor BOTH counters. Trade-off: preserves the 5 monkeypatches in tests/test_api_hardening.py + tests/test_search_api_v2.py against web.api_hardening._event_queue. A future cleanup plan could unify."
  - "Daemon thread name 'posthog-shared-drain' (vs web layer's 'posthog-api-drain') for unambiguous journalctl / ps output."
  - "Defensive dict() copy of caller's properties before queue.put_nowait so mutation after enqueue cannot corrupt in-flight events."
  - "__all__ enforced public API surface — drain loop and lazy-start helper are private (leading underscore)."

patterns-established:
  - "Server-side fire-and-forget telemetry from non-UI contexts: use shared.posthog_server.enqueue_event instead of web/analytics.posthog_capture which silently no-ops outside a NiceGUI client context (RESEARCH Pitfall 1)."
  - "Per-test queue swap via autouse fixture: when a module's lazy-started daemon thread actively drains a module-level queue, tests that need to observe enqueued items must monkeypatch the queue attribute to a fresh, drain-free queue.Queue. The daemon harmlessly drains the orphaned original."
  - "Static source-string invariant pinning: complement runtime asserts with a pathlib.Path(...).read_text() + substring check so the literal source line cannot drift silently."

requirements-completed: [D-24, D-25, D-28]

# Metrics
duration: 5min
completed: 2026-05-25
---

# Phase 98 Plan 01: shared/posthog_server.py Summary

**Factored the proven server-side PostHog queue+daemon idiom out of `web/api_hardening.py:524-567` into a new shared module so the upcoming NLI circuit breaker (Plan 98-02) can emit telemetry from background threads without depending on `web/`.**

## Performance

- **Duration:** ~5 min
- **Started:** 2026-05-25T14:06:24Z
- **Completed:** 2026-05-25T14:11:41Z
- **Tasks:** 2
- **Files created:** 3 (1 module + 1 test file + this SUMMARY.md)
- **Files modified:** 0

## Accomplishments

- New shared module `shared/posthog_server.py` (164 lines) exposing `enqueue_event`, `get_dropped_event_count`, `POSTHOG_CAPTURE_URL`, `POSTHOG_HOST`, `_reset_for_tests` via `__all__`.
- 20 behavioral tests in `tests/test_posthog_server.py` covering: payload shape, distinct_id override, defensive dict copy, drop counter under `queue.Full`, telemetry never raises when `POSTHOG_API_KEY` unset/empty, daemon-thread safety, 50-thread concurrent enqueue via `threading.Barrier`, reset-seam drain + zero + idempotence, public API surface, no-web-imports static guard, maxsize=10000 source pin.
- Per `REVIEWS.md` Issue 5 Option A: `web/api_hardening.py` is untouched. All 44 pre-existing api_hardening tests still pass.
- Architectural boundary holds: `shared/posthog_server.py` has zero `web/` imports (asserted statically by `TestPublicAPI.test_no_web_dependencies`).

## Task Commits

Each task was committed atomically:

1. **Task 1: Create shared/posthog_server.py with module-level queue + drain thread** — `ba19a701` (feat)
2. **Task 2: Create tests/test_posthog_server.py with behavior tests + autouse reset fixture** — `2d629d43` (test)

## Files Created/Modified

- `shared/posthog_server.py` (NEW) — Module-level `queue.Queue(maxsize=10000)` + lazy-started daemon `posthog-shared-drain` thread + `_dropped_events` counter + `enqueue_event` / `get_dropped_event_count` / `_reset_for_tests` public API.
- `tests/test_posthog_server.py` (NEW) — 7 test classes, 20 test methods, autouse fixture for state + queue isolation.
- `.planning/phases/98-nli-resilience-circuit-breaker-and-bounded-timeout-hardening/98-01-SUMMARY.md` (NEW) — this document.

## Decisions Made

- **Option A from REVIEWS.md Issue 5:** Do NOT modify `web/api_hardening.py`. Operators monitor two drop counters. Preserves 5 test monkeypatches against `web.api_hardening._event_queue` without churn.
- **Distinct daemon thread name** (`posthog-shared-drain` vs `posthog-api-drain`): differentiates the two drain threads in `ps` / `journalctl` output.
- **Defensive properties-dict copy** before `put_nowait`: protects against caller mutating the dict after enqueue.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Test design raced the lazy-started drain daemon**
- **Found during:** Task 2 (running `pytest tests/test_posthog_server.py -x`)
- **Issue:** The plan's `<behavior>` Test 3 suggested filling the real `_event_queue` to `maxsize=10000` then asserting the next `enqueue_event` increments the drop counter. But `enqueue_event` lazy-starts a daemon thread that calls `_event_queue.get(timeout=60)` in a tight loop — once started (from any prior test in the session), the daemon actively drains the queue in the background, so the precondition "queue stays full" cannot hold deterministically. The first run of the plan-as-written failed: `expected drop counter to increment, got 0 -> 0`. Similarly, the concurrent-enqueue test that put N events and then called `_event_queue.get(timeout=1.0)` raced the daemon (the daemon's blocking `.get()` won the race for some items, leaving the test's `.get()` to time out with `_queue.Empty`).
- **Fix:** Restructured the autouse fixture to also monkeypatch `_event_queue` with a fresh `queue.Queue(maxsize=10000)` per test. The daemon thread, once started, remains bound to its original queue object (now orphaned) and harmlessly drains nothing. Each test sees a clean queue that nothing is racing against. For the specific drop-counter tests, the fixture's fresh queue is further overridden with a `maxsize=1` pre-filled queue to force `queue.Full` deterministically. The contract under test (drop counter increments on `queue.Full` and never raises) is preserved. The module-load `maxsize=10000` value is separately pinned by `TestModuleConstants.test_event_queue_maxsize_in_source_is_10000` (static `pathlib.Path(...).read_text()` substring check).
- **Files modified:** `tests/test_posthog_server.py` (added autouse `monkeypatch.setattr(ph, '_event_queue', fresh_q)`; added 2 tests in new `TestModuleConstants` class for source-string + host pinning)
- **Verification:** 20/20 tests pass in 0.21s; `pytest tests/test_api_hardening.py` (44/44) untouched.
- **Committed in:** `2d629d43` (Task 2 commit)

---

**Total deviations:** 1 auto-fixed (Rule 1 — test isolation bug exposed by the lazy-daemon pattern)
**Impact on plan:** No scope change. The plan's intent — "the drop counter increments on `queue.Full` and `enqueue_event` never raises" — is fully verified. The fix is purely about test isolation, not module behavior. Added an extra static-source guard (test_event_queue_maxsize_in_source_is_10000) so the maxsize literal cannot be silently changed without a corresponding test bump. Test count grew from the plan's "12+" target to 20, all in the spirit of the plan's `<behavior>` enumeration.

## Issues Encountered

- The autouse fixture must be `before AND after` (`_reset_for_tests()` on both sides of `yield`) so failing tests don't poison subsequent ones. Standard pytest pattern; followed.
- Plan's acceptance criterion specified `threading.Barrier` "exactly 1 match" via `grep -F`. Initial draft had 2 occurrences (one in a docstring + one in the call). Reworded the docstring to use the phrase "synchronization barrier" instead, leaving exactly 1 occurrence of the literal `threading.Barrier`.

## User Setup Required

None — no external service configuration required. The module reads `POSTHOG_API_KEY` from `os.environ` (already set on production). No new env vars introduced by this plan.

## Threat Surface Scan

No new network endpoints, auth paths, file access, or schema changes. The plan's `<threat_model>` (STRIDE register T-98-01-01 through T-98-01-06) is fully mitigated by the implementation:
- T-98-01-01 (information disclosure via properties dict): caller-owned content; defensive `dict()` copy prevents post-enqueue mutation tampering.
- T-98-01-02 (DoS via unbounded queue): `maxsize=10000` bound + drop counter.
- T-98-01-03 (DoS via slow PostHog): `timeout=2.0` per request, fire-and-forget catch-all `except Exception: pass`.
- T-98-01-04 / T-98-01-05 (env-var & distinct_id trust): accepted per threat register.
- T-98-01-06 (silent drops audit trail): `get_dropped_event_count()`.

No threat flags surfaced (no new attack surface introduced).

## Next Phase Readiness

- `shared/posthog_server.py` is ready for consumption by Plan 98-02 (`shared/nli_circuit_breaker.py`). Plan 02 will be the first code consumer of `enqueue_event` (for `nli_breaker_opened` / `nli_breaker_closed` events per CONTEXT.md D-24 / D-25 / D-28).
- Architectural Option (a) per RESEARCH §Open Question 1 is preserved: `shared/` does not depend on `web/`. The clean boundary is asserted at runtime by `TestPublicAPI.test_no_web_dependencies` so any future drift will fail CI.
- `web/api_hardening.py` is unchanged (REVIEWS.md Issue 5 Option A); operators must monitor both `web.api_hardening.get_dropped_event_count()` and `shared.posthog_server.get_dropped_event_count()` per 98-VALIDATION.md §Observability Invariants (Plan 98-06 will surface this in CLAUDE.md).

## Self-Check: PASSED

- `shared/posthog_server.py` exists — FOUND
- `tests/test_posthog_server.py` exists — FOUND
- Task 1 commit `ba19a701` in `git log --all` — FOUND
- Task 2 commit `2d629d43` in `git log --all` — FOUND
- `pytest tests/test_posthog_server.py -x` → 20 passed — VERIFIED
- `python -c "import shared.posthog_server"` → OK — VERIFIED
- `grep -E "^(from|import) web" shared/posthog_server.py` → 0 matches — VERIFIED
- `web/api_hardening.py` unchanged (`git diff master-main -- web/api_hardening.py` shows no changes) — VERIFIED

---
*Phase: 98-nli-resilience-circuit-breaker-and-bounded-timeout-hardening*
*Plan: 01*
*Completed: 2026-05-25*
