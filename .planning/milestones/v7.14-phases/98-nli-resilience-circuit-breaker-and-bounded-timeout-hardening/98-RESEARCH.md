# Phase 98: NLI Resilience — Research

**Researched:** 2026-05-25
**Domain:** Network resilience (circuit breaker, bounded timeouts, threadpool protection) for synchronous `requests`-based NLI/IIIF clients embedded in a synchronous Starlette/FastAPI threadpool.
**Confidence:** HIGH

---

## Summary

Phase 98 is incident-driven. On 2026-05-25 at 11:50 UTC, `genizah-web` stopped responding to HTTP requests for ~7 minutes because Starlette's ~40-thread worker pool was saturated by synchronous `requests.get()` calls to `iiif.nli.org.il` with 15s + 10s timeouts. SIGTERM hung 90s; systemd resorted to SIGKILL. Two root causes converge: (1) generous per-call timeouts (up to 25s/sys_id) at multiple NLI fetch sites, and (2) an existing circuit breaker at `genizah_core.py:3940-3961` that is class-attribute scoped and NOT consulted by `web/api.py` — so the two NLI client paths fail independently.

CONTEXT.md locks 28 decisions (D-01..D-28) that constitute the de-facto spec: extract a shared module-level breaker at `shared/nli_circuit_breaker.py`, wire it into ALL 10 NLI fetch sites (enumerated in D-14..D-23), shorten timeouts via 6 new env-configurable knobs (D-09), drop `NLI_SEMAPHORE_TIMEOUT` from 20s to 1s (D-10), check circuit *before* semaphore acquisition (D-11/D-12), count timeout/5xx/429 as failures but exempt 404 (D-06/D-07), emit PostHog telemetry on state transitions (D-24/D-25), and ship unit + concurrency tests (D-26/D-27).

The phase is process-local in-memory state plus network timeout tuning. No database migrations, no Supabase RLS, no UI changes. The strict invariant is: **after this phase ships, no single NLI slowdown can saturate the Starlette threadpool.** Worst-case per-request blocking budget drops from 45s (current: 20s semaphore wait + 25s IIIF+MARC) to ~9s (1s semaphore + 3s connect + 5s read), and after 3 consecutive failures, additional requests return `[]` in microseconds.

**Primary recommendation:** Implement `shared/nli_circuit_breaker.py` as a module-level singleton object with `is_open()`, `record_failure(failure_type, path)`, `record_success()`, and `_state_snapshot()` (test seam). Reuse the proven server-side PostHog idiom from `web/api_hardening.py:_drain_posthog_queue` (queue + daemon thread + drop counter — fire-and-forget) rather than the UI-context-dependent `web/analytics.posthog_capture()`. Use `unittest.mock.patch` + `threading.Barrier` test patterns from `tests/test_nli_cache_persist_retry.py` for the concurrency suite.

---

<phase_requirements>
## Phase Requirements

Phase 98 has no formal REQ-IDs in `.planning/REQUIREMENTS.md` — it is incident-driven and CONTEXT.md's 28 locked decisions (D-01..D-28) serve as the specification. Each decision below is mapped to the research finding that enables it.

| ID | Description | Research Support |
|----|-------------|------------------|
| D-01 | Single global "NLI" breaker key | Confirmed both `iiif.nli.org.il` and `rosetta.nli.org.il` failed together in incident journal — single counter aligns with observed failure mode |
| D-02 | Shared global state across all call sites | Existing `genizah_core` class-attribute breaker proves the pattern works; only the *sharing* across `web/api.py` is missing |
| D-03 | `shared/nli_circuit_breaker.py` module-level singleton with `threading.Lock` | Established pattern: `_nli_persist_lock` in `web/api.py:46`, `_refresh_locks_guard` in `web/supabase_client.py:155` |
| D-04 | `time.monotonic()` not `time.time()` | Existing breaker uses `time.time()` (bug — NTP jump = stuck-open or stuck-closed); Codex critique §5 explicitly required monotonic |
| D-05 | Process-local (multi-worker uvicorn deferred) | Current production is single-worker; CONTEXT acknowledges this is acceptable degradation |
| D-06 | Count `Timeout`, `ConnectionError`, 5xx, 429 as failures | `requests.exceptions.Timeout` is the catch-all parent for `ReadTimeout` / `ConnectTimeout`; covers urllib3 `MaxRetryError` indirectly |
| D-07 | 404 + empty manifest do NOT trip breaker | Existing per-sys_id negative cache at `web/api.py:_NLI_FAIL_SENTINEL` handles these correctly today |
| D-08 | `record_success()` resets counter to zero | Trivial; matches existing `_nli_record_success` semantics at `genizah_core.py:3958` |
| D-09 | 6 new `NLI_*` env knobs | All call sites currently use hard-coded integer timeouts (15s/10s/15s/30s/30s/15s/5s/10s/5s) — easy migration |
| D-10 | `NLI_SEMAPHORE_TIMEOUT` 20s → 1s | Codex critique §1 + §3 critical insight |
| D-11/D-12 | Circuit check BEFORE + AFTER semaphore acquisition | Codex critique §2 — addresses the "all 8 slots held + waiting" sub-case |
| D-13 | All other sites check circuit first thing | Each site is a leaf function — no shared infrastructure to bypass |
| D-14..D-23 | 10 specific call sites enumerated | All sites confirmed in code reads below (Architectural Responsibility Map) |
| D-24 | PostHog events `nli_breaker_opened` / `nli_breaker_closed` | Server-side capture idiom established in `web/api_hardening.py:capture_api_event` |
| D-25 | Fire-and-forget telemetry, never raises | Exact pattern in `_drain_posthog_queue` (line 547-567 of api_hardening) — try/except/pass |
| D-26 | `tests/test_nli_circuit_breaker.py` with unit + concurrency classes | Idiom proven in `tests/test_nli_cache_persist_retry.py` |
| D-27 | Per-status-code tests + race-condition test | Standard `unittest.mock.patch` + `threading.Barrier` pattern |
| D-28 | PostHog event emission verified via existing test helper | `web/api_hardening._event_queue` exposed as module attribute for monkeypatch |

</phase_requirements>

## Architectural Responsibility Map

| Capability | Primary Tier | Secondary Tier | Rationale |
|------------|-------------|----------------|-----------|
| Circuit-breaker state (counter, open-until, lock) | Backend / Shared module | — | Pure in-memory state — must live in a tier reachable from both `web/api.py` (FastAPI endpoints) and `genizah_core.py` (desktop + web shared core). `shared/` is the established tier for this. |
| Network calls to `iiif.nli.org.il` / `rosetta.nli.org.il` | API / Backend | — | All 10 call sites are server-side (`web/api.py`, `shared/puzzle_image_service.py`, `web/pages/puzzle.py:_resolve_folios`, `genizah_core.py`). No client-side NLI fetches — extension and browser never touch NLI directly. |
| Semaphore-based concurrency cap | API / Backend | — | `_nli_semaphore` in `web/api.py:41` is web-only (genizah_core uses ThreadPoolExecutor for its own concurrency model). |
| PostHog telemetry emission (breaker open/close events) | Backend / Shared module | API (only for context, not for emission) | The breaker fires events from ANY caller including background threads with no UI context — must use the server-side `requests.post` to PostHog capture URL idiom from `web/api_hardening.py`, NOT the `ui.run_javascript` idiom from `web/analytics.py` (which silently no-ops outside a NiceGUI client context). |
| Env-variable configuration | Backend / Shared module | — | Reads from `os.environ` at module import; pattern proven in `web/api.py:27-38`. |
| Test invariants (unit + concurrency) | Test tier | — | `tests/test_nli_circuit_breaker.py` flat at repo root per existing convention. |

**Why this matters for Phase 98:** the breaker MUST be reachable from a non-UI context (background threads, FastAPI sync handlers, desktop app). The existing `web/analytics.posthog_capture()` is UI-context-bound and will silently fail when called from the breaker's recording path. The plan must use `web/api_hardening.py`'s queue+daemon pattern (or factor out a shared helper).

---

## Standard Stack

### Core
| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| `requests` | 2.32.5 [VERIFIED: requirements.txt + `python -c "import requests"`] | HTTP client for NLI/Rosetta fetches | Already the project's HTTP client at every NLI site; no need to introduce a new library |
| `urllib3` | 2.6.3 [VERIFIED: `python -c "import urllib3"`] | Underlying connection pool / exception classes (`MaxRetryError`, `ReadTimeoutError`) | Transitive via `requests`; exception classes used for fine-grained failure typing |
| `threading` (stdlib) | Python 3.10+ | `Lock` for breaker state, `Semaphore` for concurrency cap (existing) | Established pattern across codebase (`_nli_persist_lock`, `_refresh_locks`, `_dropped_events_lock`) |
| `time` (stdlib) — `time.monotonic` | Python 3.10+ | Monotonic clock for open-until window | D-04 mandates this over `time.time()` |
| `pytest` | (current) | Test runner | Project convention (`.planning/codebase/TESTING.md`) |
| `unittest.mock` (stdlib) | Python 3.10+ | `patch`, `patch.object`, `MagicMock` | Proven idiom in `tests/test_nli_cache_persist_retry.py`, `tests/test_export_*` |
| `queue.Queue` (stdlib) | Python 3.10+ | Fire-and-forget PostHog event buffer | Pattern in `web/api_hardening.py:_event_queue` (maxsize=10000) — already drains via daemon thread |

### Supporting
| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| `concurrent.futures.ThreadPoolExecutor` (stdlib) | Python 3.10+ | Concurrency test driver (D-26) | Spawn 20 threads against monkeypatched hanging session |
| `requests.exceptions.Timeout` / `.ConnectionError` / `.ReadTimeout` / `.ConnectTimeout` | requests 2.32.5 | Exception taxonomy for `record_failure` (D-06) | `Timeout` is the parent class — catching it covers both Read and Connect variants |
| `urllib3.exceptions.MaxRetryError` / `.NewConnectionError` | urllib3 2.6.3 | Wrapped by `requests.ConnectionError` — generally not caught directly | Optional explicit handling if production logs surface bare urllib3 exceptions |

### Alternatives Considered
| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| Module-level singleton with `threading.Lock` | `pybreaker` package | New dependency for ~50 lines of logic; CONTEXT D-03 locks the in-house module approach; pybreaker is also overkill (we don't need exponential backoff, jitter, half-open state with bounded retries) |
| Sync `requests` + breaker | Async refactor to `httpx.AsyncClient` | OUT OF SCOPE per CONTEXT.md `<deferred>` section — would buy genuinely correct backpressure but at 100x the code change |
| In-process breaker | Redis-backed shared breaker | OUT OF SCOPE per D-05 — single-worker uvicorn means in-process suffices; revisit if/when multi-worker |
| Single global "NLI" breaker key | Per-host breaker (`iiif` vs `rosetta`) | D-01 locked single-key: incident showed both hosts fail together; per-host adds state without observed benefit |

**Installation:** No new packages required. All dependencies are stdlib or already in `requirements.txt`.

**Version verification:** `requests==2.32.5` confirmed in `requirements.txt:3` and at runtime. `urllib3 2.6.3` is the installed transitive version (post-2.0 series — note `MaxRetryError` location is `urllib3.exceptions.MaxRetryError`, unchanged from 1.x).

---

## Architecture Patterns

### System Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────┐
│                      ANY NLI CALL SITE (1 of 10)                        │
│  web/api.py:647 fetch_fl_ids_from_nli                                   │
│  web/api.py:771 /api/nli_image                                          │
│  web/api.py:834 _fetch_nli_image_bytes                                  │
│  web/api.py:1994 /api/proxy_image                                       │
│  shared/puzzle_image_service.py:172 + :252                              │
│  web/pages/puzzle.py:1991                                               │
│  genizah_core.py:4056 fetch_iiif_manifest (already wired)               │
│  genizah_core.py:4144 fetch_marc_data (already wired)                   │
│  genizah_core.py:4651 + :4773 (NEW: migrate to shared)                  │
└────────────────┬────────────────────────────────────────────────────────┘
                 │
                 ▼
        ┌──────────────────────────────────────────┐
        │  shared.nli_circuit_breaker.is_open()    │  ◄── Cheap O(1) check
        │  (acquires lock, reads monotonic time)   │      under threading.Lock
        └──────────────┬───────────────────────────┘
                       │
            ┌──────────┴──────────┐
       OPEN │                     │ CLOSED
            ▼                     ▼
     ┌─────────────┐    ┌─────────────────────────┐
     │ return [] / │    │ semaphore.acquire(1s)   │  ◄── Only for fl_ids site
     │ return None │    │ (only fetch_fl_ids_…)   │      (D-10: 1s, was 20s)
     │ <microsec>  │    └────────────┬────────────┘
     └─────────────┘                 │
                          recheck   │
                          breaker   ▼
                   ┌──────────────────────────────────┐
                   │ is_open() again? (D-12 defensive)│
                   └────────────┬─────────────────────┘
                                │
                       CLOSED   │   OPEN
                          ▼     ▼
                  ┌──────────────────────────┐    ┌─────────────────┐
                  │  _nli_session.get(...)   │    │ release sem,    │
                  │  timeout=(3, 5)          │    │ return []       │
                  │  read NLI_IIIF_READ_…    │    └─────────────────┘
                  └────────────┬─────────────┘
                               │
                  ┌────────────┴────────────────────┐
                  │ status / exception?             │
                  └─┬─────────┬──────────┬──────────┘
                    │         │          │
                  200/      404/         Timeout / ConnErr /
                  empty     "no img"     5xx / 429
                    │         │          │
                    ▼         ▼          ▼
              ┌──────────┐ ┌─────────┐ ┌──────────────────────────┐
              │ record_  │ │ negative│ │ record_failure(           │
              │ success()│ │ cache   │ │   failure_type='timeout', │
              │ → counter│ │ per-    │ │   path='fetch_fl_ids_..')│
              │   = 0    │ │ sys_id  │ │ → counter += 1            │
              └──────────┘ └─────────┘ │ → if >= 3:                │
                                       │     open_until =          │
                                       │       monotonic() + 60    │
                                       │     enqueue PostHog       │
                                       │     'nli_breaker_opened'  │
                                       └──────────────────────────┘
                                                  │
                                                  ▼
                                       ┌──────────────────────────┐
                                       │ PostHog queue (D-24/D-25)│
                                       │ daemon thread drains via │
                                       │ requests.post(POSTHOG_   │
                                       │ CAPTURE_URL, timeout=2)  │
                                       └──────────────────────────┘
```

### Recommended Project Structure

```
shared/
├── nli_circuit_breaker.py          # NEW — module-level singleton, locked, monotonic
└── (existing modules untouched)

web/
├── api.py                          # MODIFY — 6 sites: D-11..D-18 + drop sem timeout to 1s
├── api_hardening.py                # READ-ONLY — reuse PostHog queue idiom (or factor helper)
├── analytics.py                    # READ-ONLY — DO NOT call posthog_capture from breaker
├── pages/puzzle.py                 # MODIFY — 1 site: D-21
└── ...

shared/
├── puzzle_image_service.py         # MODIFY — 2 sites: D-19, D-20
└── ...

genizah_core.py                     # MODIFY — migrate existing breaker (D-03 + D-22 + D-23)

tests/
└── test_nli_circuit_breaker.py     # NEW — unit + concurrency classes (D-26 + D-27)

CLAUDE.md                           # MODIFY — Environment Variables section: add 6 NLI_* knobs
docs/guides/DEVELOPER_GUIDE.md      # MODIFY — env vars table
docs/OPEN_ISSUES.md                 # MODIFY — flip the 2026-05-25 hang entry to ✅ Fixed (date)
```

### Pattern 1: Module-Level Singleton Breaker with Locked State

**What:** A `shared/nli_circuit_breaker.py` module that exposes a small, side-effect-free API. State is module-level (per-process), guarded by a single `threading.Lock`, and uses `time.monotonic()` for the open-until timestamp.

**When to use:** Always — every call to `is_open()` / `record_failure()` / `record_success()` from ALL 10 call sites goes through this module.

**Example (illustrative — final API at Claude's discretion per CONTEXT.md):**
```python
# Source: NEW shared/nli_circuit_breaker.py
# Pattern derived from:
#   genizah_core.py:3940-3961 (existing class-attribute breaker — migrating)
#   web/api.py:46 (_nli_persist_lock pattern)
#   web/api_hardening.py:524-567 (PostHog queue pattern)

import logging
import os
import threading
import time
from typing import Literal

logger = logging.getLogger(__name__)

# Env-driven configuration (D-09)
_THRESHOLD = max(1, int(os.environ.get('NLI_CIRCUIT_THRESHOLD', '3')))
_WINDOW = max(1, int(os.environ.get('NLI_CIRCUIT_WINDOW', '60')))

# Locked state
_lock = threading.Lock()
_consecutive_failures = 0
_open_until = 0.0  # monotonic timestamp
_opened_at = 0.0   # for downtime_seconds telemetry on close

FailureType = Literal['timeout', 'connection_error', '5xx', '429']


def is_open() -> bool:
    """True when the breaker has tripped and callers should short-circuit.

    Cheap O(1) under lock. Auto-recovers when monotonic time passes _open_until.
    """
    with _lock:
        if _open_until == 0.0:
            return False
        if time.monotonic() < _open_until:
            return True
        # Auto-recover: window expired
        _open_until_was = _open_until
        # Note: we do NOT clear _consecutive_failures here — the next
        # call's outcome will either reset (success) or increment from the
        # current count. This avoids a thundering-herd re-trip pattern.
        return False


def record_failure(failure_type: FailureType, path: str) -> None:
    """Count a failure. Trip the breaker when threshold reached. D-06."""
    global _consecutive_failures, _open_until, _opened_at
    with _lock:
        _consecutive_failures += 1
        just_opened = False
        if _consecutive_failures >= _THRESHOLD and _open_until <= time.monotonic():
            _open_until = time.monotonic() + _WINDOW
            _opened_at = time.monotonic()
            just_opened = True
            snapshot_failures = _consecutive_failures
    if just_opened:
        # Outside lock: telemetry never holds the breaker lock
        _emit_opened(snapshot_failures, path, failure_type)


def record_success(path: str = '') -> None:
    """Reset the consecutive-failure counter. D-08."""
    global _consecutive_failures, _open_until, _opened_at
    with _lock:
        was_open = _open_until > time.monotonic()
        prior_opened_at = _opened_at
        _consecutive_failures = 0
        _open_until = 0.0
        _opened_at = 0.0
    if was_open:
        downtime = max(0.0, time.monotonic() - prior_opened_at)
        _emit_closed(downtime, path)


def _state_snapshot() -> dict:
    """Test seam — read state without acquiring caller-visible side effects."""
    with _lock:
        return {
            'consecutive_failures': _consecutive_failures,
            'open_until_monotonic': _open_until,
            'opened_at_monotonic': _opened_at,
            'is_open_now': time.monotonic() < _open_until,
        }


def _emit_opened(failures: int, path: str, failure_type: str) -> None:
    """Fire-and-forget PostHog 'nli_breaker_opened'. D-24/D-25. Never raises."""
    try:
        from shared.posthog_server import enqueue_event  # NEW helper, see below
        enqueue_event(
            event='nli_breaker_opened',
            properties={
                'consecutive_failures': failures,
                'triggering_path': path,
                'failure_type': failure_type,
                'threshold': _THRESHOLD,
                'window_seconds': _WINDOW,
            },
        )
    except Exception:
        logger.debug('nli_breaker_opened telemetry suppressed', exc_info=True)


def _emit_closed(downtime_seconds: float, path: str) -> None:
    try:
        from shared.posthog_server import enqueue_event
        enqueue_event(
            event='nli_breaker_closed',
            properties={
                'downtime_seconds': round(downtime_seconds, 3),
                'closed_by_path': path,
            },
        )
    except Exception:
        logger.debug('nli_breaker_closed telemetry suppressed', exc_info=True)
```

### Pattern 2: Server-Side PostHog Emission (Queue + Daemon)

**What:** Fire-and-forget event capture from background threads or any non-UI context. Events are enqueued onto a bounded queue and drained by a single daemon thread that posts to `POSTHOG_HOST/capture` with a 2s `requests` timeout. Drops are counted but never block or raise.

**When to use:** ALL breaker telemetry (D-24/D-25). DO NOT use `web/analytics.posthog_capture()` — it depends on `ui.run_javascript` which only works inside a NiceGUI UI client context. The breaker fires from background threads, FastAPI sync handlers, and the desktop app; none of these have a UI context.

**Example:**
```python
# Source: web/api_hardening.py:547-567 (verbatim production code)

_event_queue: queue.Queue = queue.Queue(maxsize=10000)
_drain_thread_started = threading.Event()

def _drain_posthog_queue() -> None:
    api_key = os.environ.get('POSTHOG_API_KEY', '').strip()
    while True:
        try:
            event = _event_queue.get(timeout=60)
        except queue.Empty:
            continue
        if not api_key:
            continue
        try:
            payload = {
                'api_key': api_key,
                'event': event['event'],
                'distinct_id': event['distinct_id'],
                'properties': event['properties'],
                'timestamp': event['timestamp'],
            }
            requests.post(POSTHOG_CAPTURE_URL, json=payload, timeout=2.0)
        except Exception:
            pass  # Fire-and-forget — silent drop on error
```

**Recommendation for Phase 98:** Either (a) factor `web/api_hardening.py`'s queue + drain thread + sample logic into a new `shared/posthog_server.py` with a public `enqueue_event(event: str, properties: dict, distinct_id: str = 'breaker')` function — both `api_hardening.py` and `nli_circuit_breaker.py` import from it; OR (b) have the breaker call directly into `web/api_hardening.py`'s queue via a thin wrapper (`api_hardening.enqueue_breaker_event(...)`). Option (a) is the right architectural boundary; option (b) is faster to ship. Claude's discretion under D-25 — recommend **option (a)** because the breaker is in `shared/` (consumed by desktop too) and shouldn't depend on `web/`.

### Pattern 3: Test Concurrency Idiom — `threading.Barrier` Outside the Lock

**What:** When testing that a `threading.Lock` actually serializes concurrent threads, place the `threading.Barrier(n).wait()` call BEFORE the lock acquisition path, never inside it. Combined with a `_ConcurrencyRecorder` helper, you can prove `max_concurrent == 1`.

**When to use:** The concurrency test class in `tests/test_nli_circuit_breaker.py` (D-27 race-condition test).

**Example:**
```python
# Source: tests/test_nli_cache_persist_retry.py:129-187 (verbatim)
# Reviews Codex-LOW-1 note: barrier BEFORE lock acquisition prevents deadlock

barrier = threading.Barrier(2)  # placed BEFORE lock acquisition
recorder = _ConcurrencyRecorder()

def worker():
    barrier.wait(timeout=5.0)         # both threads alive + contending
    result = _save_nli_persistent_cache(cache, cache_time, cache_path)
    results.append(result)

threads = [threading.Thread(target=worker) for _ in range(2)]
for t in threads: t.start()
for t in threads: t.join(timeout=10.0)

assert recorder.max_concurrent == 1, "lock did not serialize"
```

### Anti-Patterns to Avoid

- **Calling `web/analytics.posthog_capture()` from the breaker:** Silently no-ops in any non-UI context (background threads, FastAPI sync handlers, desktop app). Will appear to work in dev but emit ZERO events in production. Use the server-side queue idiom instead.
- **Using `time.time()` for the open-until window:** Wall-clock can jump (NTP, manual set) — Codex critique §5 catches this in the existing breaker. `time.monotonic()` is the only safe choice.
- **Acquiring the breaker lock while emitting telemetry:** PostHog enqueue can theoretically block (queue.Full edge case). Always capture state under lock, then call telemetry helpers OUTSIDE the lock.
- **Forgetting to release the semaphore in the D-12 defensive re-check path:** If circuit opens between semaphore acquire and re-check, the slot must be released or the semaphore leaks. Use try/finally.
- **Per-test global state pollution:** The breaker is module-level state. Tests MUST reset state via a fixture (e.g., monkeypatch the module's `_consecutive_failures` and `_open_until` to defaults in `setup_method` / `autouse fixture`). Otherwise test order changes outcomes.
- **Catching `BaseException` in the breaker recording path:** Catch `Exception` only. KeyboardInterrupt and SystemExit must propagate.

---

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Exponential backoff in the open-until window | A custom backoff curve | Flat 60s window per D-09 | CONTEXT.md explicitly defers exponential backoff to a future phase |
| Bounded queue for telemetry | `collections.deque(maxlen=N)` with manual drain | `queue.Queue(maxsize=N)` + daemon thread | Already in production at `web/api_hardening.py:524`; battle-tested, drop counter included |
| Per-host breaker logic (iiif vs rosetta) | Two parallel breakers with cross-coordination | Single global "NLI" breaker per D-01 | Incident showed both hosts fail together; two-key logic adds state, not signal |
| Wall-clock-aware time | `time.time()` + NTP jump handling | `time.monotonic()` | Stdlib already provides the correct primitive |
| Test-time monkeypatch of `time.monotonic` | Manual time injection plumbed through every function | `unittest.mock.patch('shared.nli_circuit_breaker.time.monotonic')` returning a fake | `unittest.mock.patch` handles thread-local state correctly when used with `patch.object` |
| Server-side PostHog SDK | `posthog` Python package | Direct `requests.post` to `POSTHOG_CAPTURE_URL` | Already the project's idiom in `web/api_hardening.py`; avoids new dependency and keeps fire-and-forget semantics tight |

**Key insight:** Every concern in this phase has a proven in-codebase pattern. The phase is mechanical wiring, not new architecture. The temptation to "improve" the breaker (jitter, exponential backoff, per-host state, observability dashboards) must be resisted — CONTEXT.md is explicit that those are out of scope.

---

## Runtime State Inventory

**Trigger:** Phase 98 introduces a new module and rewires existing call sites — NOT a rename or migration. However, it removes class-attribute state from `genizah_core.py` and replaces it with module-level state in `shared/nli_circuit_breaker.py`. Reviewing the inventory categories for completeness:

| Category | Items Found | Action Required |
|----------|-------------|------------------|
| Stored data | **None** — breaker state is in-memory only. No DB tables, no SQLite sidecars, no Supabase rows store breaker state. The existing `genizah_core.py:_iiif_manifest_fail_cache` and `_marc_fail_cache` (OrderedDicts in RAM) are NOT breaker state — they're per-sys_id negative cache and stay where they are. | None — pure in-memory refactor. |
| Live service config | **None** — PostHog dashboard tags/services are not pre-registered; new events `nli_breaker_opened` and `nli_breaker_closed` will appear in PostHog automatically on first emission. No PostHog UI configuration required pre-deploy. | None. |
| OS-registered state | **None** — no systemd units, Windows tasks, or pm2 process names reference NLI breaker state. The `genizah-web.service` systemd unit owns the process but doesn't know about internal modules. | None. |
| Secrets / env vars | 6 new env vars (`NLI_CIRCUIT_THRESHOLD`, `NLI_CIRCUIT_WINDOW`, `NLI_IIIF_READ_TIMEOUT`, `NLI_MARC_READ_TIMEOUT`, `NLI_IMAGE_READ_TIMEOUT`, `NLI_CONNECT_TIMEOUT`) + 1 default change (`NLI_SEMAPHORE_TIMEOUT` 20→1). Server `.env` does NOT need updates if defaults are acceptable. `POSTHOG_API_KEY` already set on prod. | Update CLAUDE.md "Environment Variables" + `docs/guides/DEVELOPER_GUIDE.md` table (D-09 requires defaults documented). Optionally set in server `.env` if defaults need overriding. |
| Build artifacts / installed packages | **None** — no native bindings, no PyInstaller bundle changes (desktop pulls the new module via existing `genizah_core.py` migration but stays on the same wheel/dist). The new `tests/test_nli_circuit_breaker.py` does not affect builds. | Verify `python -m self_test` on desktop installer once before v7.15. Lightweight check, not a blocker. |

**Verdict:** Phase 98 is pure code + env-vars + docs. No data migration, no service re-registration, no installed-package upgrades. The canonical question "after every file is updated, what runtime systems still have the old behavior cached?" answers: **none — `systemctl restart genizah-web.service` after deploy is sufficient.**

---

## Common Pitfalls

### Pitfall 1: PostHog UI-Context Trap
**What goes wrong:** Calling `from web.analytics import posthog_capture` from `shared/nli_circuit_breaker.py` will compile and pass tests, but in production every breaker open/close emits ZERO events because `ui.run_javascript` silently catches its own exception when no NiceGUI client is bound to the current request scope.
**Why it happens:** `web/analytics.posthog_capture` was designed for emission FROM page handlers / event listeners that already have an active NiceGUI Client. Background threads, sync FastAPI handlers, and the daemon-thread emission path do NOT have a Client.
**How to avoid:** Use the server-side queue+daemon idiom from `web/api_hardening.py`. The proposed `shared/posthog_server.py` module factors out the queue, drain thread, drop counter, and `requests.post` to `POSTHOG_CAPTURE_URL`.
**Warning signs:** PostHog dashboard shows zero `nli_breaker_opened` events in the 24h after deploy, but `journalctl -u genizah-web | grep nli_breaker` shows breaker activity in logs.

### Pitfall 2: Lock + Telemetry Deadlock Risk
**What goes wrong:** Acquiring the breaker lock and then calling `_emit_opened()` inside the lock — if `_emit_opened` ever back-references the breaker (e.g., via PostHog event property), you risk re-entrancy. Even without re-entry, holding the lock across a `queue.put_nowait` is unnecessary contention.
**Why it happens:** Natural temptation to "capture state atomically" by emitting telemetry inside the lock.
**How to avoid:** Snapshot all needed state inside the lock, set a `just_opened = True` flag, exit the lock, then call telemetry. See Pattern 1 above.
**Warning signs:** Concurrency test (D-26) shows `max_concurrent > 1` or hangs.

### Pitfall 3: Semaphore Leak on Defensive Re-check (D-12)
**What goes wrong:** After `_nli_semaphore.acquire(timeout=1)` succeeds, D-12 mandates re-checking the breaker (another thread may have tripped it). If the re-check returns OPEN and the function returns `[]` without releasing the semaphore, the slot is leaked. After 8 leaks the semaphore is permanently exhausted.
**Why it happens:** Easy to write `if is_open(): return []` without thinking about the held semaphore.
**How to avoid:** Always use `try / finally: _nli_semaphore.release()` around the entire post-acquire block, including the re-check.
**Warning signs:** After several hours of production traffic with NLI flapping, all 8 semaphore slots permanently occupied even when NLI is healthy. Manifests as `NLI semaphore timeout` log line on every request.

### Pitfall 4: `time.time()` Slip in the New Module
**What goes wrong:** Copy-pasting from `genizah_core.py:3947` brings `import time; time.time() < cls._nli_circuit_open_until` — the existing buggy behavior. D-04 mandates `time.monotonic()`.
**Why it happens:** Mechanical migration without reading Codex critique §5.
**How to avoid:** Static AST guard — `tests/test_nli_circuit_breaker.py` should include a test that asserts `time.time` is NOT referenced anywhere in `shared/nli_circuit_breaker.py`. Cheap belt-and-suspenders.
**Warning signs:** NTP step on the server causes mass spurious "breaker open" or "stuck closed forever" behavior.

### Pitfall 5: Existing `genizah_core` Breaker Not Fully Removed
**What goes wrong:** The migration leaves `_NLI_CIRCUIT_THRESHOLD = 3` and `_nli_circuit_is_open` / `_nli_record_failure` / `_nli_record_success` as class methods on `GenizahCore` (deprecated stubs). Future readers see two breakers; confusion ensues.
**Why it happens:** Risk-averse "leave the old code as a deprecated stub" pattern.
**How to avoid:** Delete the old class attributes + methods cleanly. Update all `cls._nli_record_failure()` / `self._nli_record_success()` / `self._nli_circuit_is_open()` call sites in `genizah_core.py` to `from shared.nli_circuit_breaker import is_open, record_failure, record_success`. Plan-checker should verify zero remaining references to `_nli_circuit_is_open` outside `shared/nli_circuit_breaker.py`.
**Warning signs:** `grep -rn "_nli_circuit_is_open\|_nli_record_failure\|_nli_record_success" --include='*.py'` returns hits outside `shared/nli_circuit_breaker.py` or its tests.

### Pitfall 6: Test Order Pollution from Module-Level State
**What goes wrong:** Test A trips the breaker. Test B (run after A) inherits `_consecutive_failures = 3` and `_open_until` in the future. Test B asserts `is_open() == False` and fails — but only when run after A.
**Why it happens:** Module-level state persists across test functions in the same pytest process.
**How to avoid:** Add an `@pytest.fixture(autouse=True)` at the top of `tests/test_nli_circuit_breaker.py` that resets `shared.nli_circuit_breaker._consecutive_failures`, `_open_until`, `_opened_at` to defaults BEFORE each test. Use `monkeypatch.setattr` for clean reset.
**Warning signs:** Tests pass individually but fail when run as a suite. CI green locally, red on rerun.

### Pitfall 7: 5xx Status Code Detection
**What goes wrong:** `resp.status_code >= 500` catches 500-599 correctly, but the current except clauses at `web/api.py:706-707` only count `Exception` (timeout, ConnError). A 500 response succeeds at the HTTP level — no exception raised — but is a server-side failure that should trip the breaker per D-06.
**Why it happens:** Existing code only counts exceptions; the new requirement is to also count status codes.
**How to avoid:** Two separate counting paths: (1) `except (Timeout, ConnectionError): record_failure('timeout', ...)`; (2) `if resp.status_code >= 500: record_failure('5xx', ...)`; (3) `if resp.status_code == 429: record_failure('429', ...)`. Each path can hit `record_failure` exactly once per request.
**Warning signs:** Production journal shows IIIF returning 502 Bad Gateway repeatedly, but breaker never trips.

---

## Code Examples

### Wiring the Breaker into `fetch_fl_ids_from_nli` (D-11 + D-12)

```python
# Source: NEW pattern for web/api.py:647 (after CONTEXT.md D-11, D-12)
from shared.nli_circuit_breaker import is_open, record_failure, record_success

def fetch_fl_ids_from_nli(system_id: str, suffix: int = 1) -> list:
    # ... existing cache lookup ...

    # D-11: circuit check BEFORE semaphore acquisition
    if is_open():
        return []

    acquired = _nli_semaphore.acquire(timeout=NLI_SEMAPHORE_TIMEOUT)  # D-10: 1s
    if not acquired:
        logger.warning(f"NLI semaphore timeout for {cache_key}")
        return []
    try:
        # D-12: defensive re-check after acquire (another thread may have tripped)
        if is_open():
            return []
        return _fetch_fl_ids_network(system_id, suffix)
    finally:
        _nli_semaphore.release()


def _fetch_fl_ids_network(system_id: str, suffix: int = 1) -> list:
    # ... cache re-check ...
    url = f"https://iiif.nli.org.il/IIIFv21/DOCID/PNX_MANUSCRIPTS{system_id}-{suffix}/manifest"
    connect_t = NLI_CONNECT_TIMEOUT      # NEW env knob, D-09 default 3
    read_t = NLI_IIIF_READ_TIMEOUT       # NEW env knob, D-09 default 5
    try:
        resp = _nli_session.get(url, timeout=(connect_t, read_t), verify=True)
        if resp.status_code == 200:
            data = resp.json()
            # ... existing FL ID extraction ...
            if fl_ids:
                # ... existing cache write ...
                record_success(path='fetch_fl_ids_from_nli')   # D-08
                return fl_ids
        elif resp.status_code in (429,) or 500 <= resp.status_code < 600:
            failure_type = '429' if resp.status_code == 429 else '5xx'
            record_failure(failure_type=failure_type, path='fetch_fl_ids_from_nli')   # D-06
        # 404 / empty / 200-no-fl-ids: per-sys_id negative cache only (D-07)
    except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
        logger.error(f"Failed to fetch FL IDs for {cache_key}: {e}")
        record_failure(
            failure_type='timeout' if isinstance(e, requests.exceptions.Timeout) else 'connection_error',
            path='fetch_fl_ids_from_nli',
        )   # D-06

    # ... existing MARC fallback (same pattern, NLI_MARC_READ_TIMEOUT) ...
    # ... existing negative-cache write ...
    return []
```

### Concurrency Test Skeleton (D-26)

```python
# Source: NEW tests/test_nli_circuit_breaker.py
# Pattern from tests/test_nli_cache_persist_retry.py:129-187

import threading
import time
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch
import pytest
import requests


@pytest.fixture(autouse=True)
def _reset_breaker():
    """Reset module-level breaker state before EACH test (Pitfall 6)."""
    import shared.nli_circuit_breaker as br
    with br._lock:
        br._consecutive_failures = 0
        br._open_until = 0.0
        br._opened_at = 0.0
    yield


class TestNliCircuitBreakerConcurrency:
    def test_20_threads_saturating_nli_complete_within_10s(self, monkeypatch):
        """Codex critique §9: prove no single NLI slowdown can hang the threadpool.

        20 threads call fetch_fl_ids_from_nli against a session that hangs/times-out.
        After ~3 failures fill the 8-slot semaphore, the breaker trips and the
        remaining 17 calls return [] in microseconds.

        Assertions:
        - total wall time < 10s (D-26)
        - _nli_session.get called at most (NLI_MAX_CONCURRENT_FETCHES + threshold - 1) = 10 times
        - all 20 calls returned [] (no crashes)
        """
        from web import api as api_mod

        get_call_count = {'n': 0}

        def fake_get(url, **kwargs):
            get_call_count['n'] += 1
            time.sleep(0.5)  # simulate slow NLI (well under timeout)
            raise requests.exceptions.ReadTimeout('simulated NLI timeout')

        monkeypatch.setattr(api_mod._nli_session, 'get', fake_get)

        start = time.monotonic()
        with ThreadPoolExecutor(max_workers=20) as ex:
            futures = [ex.submit(api_mod.fetch_fl_ids_from_nli, '990001458630205171')
                       for _ in range(20)]
            results = [f.result(timeout=15) for f in futures]
        elapsed = time.monotonic() - start

        assert elapsed < 10.0, f"threadpool was hung: {elapsed:.1f}s for 20 calls"
        assert all(r == [] for r in results), "all calls should return []"
        assert get_call_count['n'] <= 10, (
            f"breaker did not trip: {get_call_count['n']} network calls "
            f"(expected ≤ NLI_MAX_CONCURRENT_FETCHES + threshold - 1 = 10)"
        )
```

### Unit Test Skeleton (D-26 + D-27)

```python
# Source: NEW tests/test_nli_circuit_breaker.py

class TestNliCircuitBreakerUnit:
    def test_three_consecutive_timeouts_trip_breaker(self, monkeypatch):
        """D-26: 3 timeouts → 4th call returns [] without invoking session.get."""
        from web import api as api_mod
        from shared import nli_circuit_breaker as br

        call_count = {'n': 0}
        def fake_get(url, **kwargs):
            call_count['n'] += 1
            raise requests.exceptions.ReadTimeout('test')

        monkeypatch.setattr(api_mod._nli_session, 'get', fake_get)
        # Disable per-sys_id negative cache to isolate breaker behavior
        monkeypatch.setattr(api_mod, 'NLI_FAIL_CACHE_TTL', 0)

        for _ in range(3):
            api_mod.fetch_fl_ids_from_nli(f'99000{_}_test')
        assert br._state_snapshot()['consecutive_failures'] >= 3
        assert br.is_open() is True

        # 4th call: breaker open → no network
        before = call_count['n']
        api_mod.fetch_fl_ids_from_nli('990000_test')
        assert call_count['n'] == before, "4th call should NOT invoke session.get"

    def test_404_does_not_trip_breaker(self, monkeypatch):
        """D-07: 404 is negative-cached per sys_id but does not increment breaker."""
        # ... monkeypatch fake_get returning status_code=404 ...
        # ... assert br._state_snapshot()['consecutive_failures'] == 0 ...

    def test_5xx_trips_breaker(self, monkeypatch):
        """D-27: each of 500/502/503/504 increments the breaker."""
        # ... parametrize over [500, 502, 503, 504] ...

    def test_429_trips_breaker(self, monkeypatch):
        """D-06: 429 (rate limited by upstream) counts as failure."""
        # ...

    def test_success_resets_counter(self, monkeypatch):
        """D-08: any successful fetch zeroes consecutive_failures."""
        # ... 2 failures then 1 success — assert counter == 0 ...

    def test_monotonic_time_used_not_wall_clock(self):
        """D-04 invariant: time.monotonic referenced, time.time not used.

        Static AST guard against accidental copy-paste from genizah_core.py:3947.
        """
        import ast, pathlib
        src = pathlib.Path('shared/nli_circuit_breaker.py').read_text()
        tree = ast.parse(src)
        for node in ast.walk(tree):
            if isinstance(node, ast.Attribute) and node.attr == 'time':
                # Forbid `time.time()`
                if isinstance(node.value, ast.Name) and node.value.id == 'time':
                    pytest.fail(f"shared/nli_circuit_breaker.py uses time.time() "
                                f"at line {node.lineno} — D-04 mandates time.monotonic()")


class TestNliBreakerTelemetry:
    def test_posthog_event_emitted_on_open(self, monkeypatch):
        """D-28: nli_breaker_opened enqueued when threshold crossed."""
        # Monkeypatch shared.posthog_server.enqueue_event (or api_hardening._event_queue)
        # to capture into a list. Trip the breaker. Assert event name and key properties.

    def test_posthog_event_emitted_on_close(self, monkeypatch):
        """D-28: nli_breaker_closed emitted with downtime_seconds."""

    def test_telemetry_never_raises(self, monkeypatch):
        """D-25: if PostHog client unavailable, breaker still works."""
        # Monkeypatch enqueue_event to raise. Trip breaker. Assert is_open() is True
        # and no exception propagated.
```

---

## Validation Architecture

### Test Framework
| Property | Value |
|----------|-------|
| Framework | pytest (per `.planning/codebase/TESTING.md`) |
| Config file | `tests/conftest.py` (project-wide sys.path setup + fixtures) |
| Quick run command | `pytest tests/test_nli_circuit_breaker.py -x` (~5s) |
| Full suite command | `pytest tests/` (~4min, ~2326 tests) |

### Phase Decisions → Test Map

Phase 98 has no formal REQ-IDs; the 28 CONTEXT decisions are pinned to tests:

| Decision | Behavior | Test Type | Automated Command | File Exists? |
|----------|----------|-----------|-------------------|-------------|
| D-01 | Single global breaker key | unit | `pytest tests/test_nli_circuit_breaker.py::TestNliCircuitBreakerUnit::test_shared_state_across_sites -x` | Wave 0 |
| D-02 | Shared state across `web/api.py` and `genizah_core.py` | integration | `pytest tests/test_nli_circuit_breaker.py::TestSharedAcrossCallSites -x` | Wave 0 |
| D-03 | Module-level singleton in `shared/nli_circuit_breaker.py` | static | `pytest tests/test_nli_circuit_breaker.py::test_module_location_exists -x` | Wave 0 |
| D-04 | `time.monotonic()` not `time.time()` | static AST | `pytest tests/test_nli_circuit_breaker.py::test_monotonic_time_used_not_wall_clock -x` | Wave 0 |
| D-06 | timeout/5xx/429 trip breaker | parametrized unit | `pytest tests/test_nli_circuit_breaker.py::TestFailureCounting -x` | Wave 0 |
| D-07 | 404 + empty manifest do NOT trip | unit | `pytest tests/test_nli_circuit_breaker.py::test_404_does_not_trip_breaker -x` | Wave 0 |
| D-08 | success resets counter | unit | `pytest tests/test_nli_circuit_breaker.py::test_success_resets_counter -x` | Wave 0 |
| D-11 | circuit check BEFORE semaphore | integration | `pytest tests/test_nli_circuit_breaker.py::test_circuit_check_before_semaphore -x` | Wave 0 |
| D-12 | recheck AFTER semaphore acquire | integration | `pytest tests/test_nli_circuit_breaker.py::test_circuit_recheck_after_semaphore -x` | Wave 0 |
| D-14..D-23 | 10 call sites wired | static grep | `pytest tests/test_nli_circuit_breaker.py::test_all_10_call_sites_use_breaker -x` | Wave 0 |
| D-24 | PostHog events emitted on state change | unit (mock enqueue) | `pytest tests/test_nli_circuit_breaker.py::TestNliBreakerTelemetry::test_posthog_event_emitted_on_open -x` | Wave 0 |
| D-25 | telemetry fire-and-forget, never raises | unit | `pytest tests/test_nli_circuit_breaker.py::test_telemetry_never_raises -x` | Wave 0 |
| D-26 | ThreadPoolExecutor saturation < 10s | concurrency | `pytest tests/test_nli_circuit_breaker.py::TestNliCircuitBreakerConcurrency -x` | Wave 0 |
| D-27 | race condition under simultaneous increments | concurrency | `pytest tests/test_nli_circuit_breaker.py::test_record_failure_under_concurrent_threads -x` | Wave 0 |
| (no D-ID) | genizah_core class-attr breaker fully removed | static grep | `pytest tests/test_nli_circuit_breaker.py::test_no_residual_class_attribute_breaker -x` | Wave 0 |

### Sampling Rate
- **Per task commit:** `pytest tests/test_nli_circuit_breaker.py -x` (≈5s)
- **Per wave merge:** `pytest tests/test_nli_circuit_breaker.py tests/test_nli_cache_persist_retry.py tests/test_nli_oxford_attribution.py tests/test_nli_crossref_service.py -x` (≈15s)
- **Phase gate:** Full suite `pytest tests/` (≈4min) — must be green before `/gsd-verify-work`
- **Production canary:** After deploy, `curl -w "%{time_total}\n" https://genizahsearch.com/api/fl_ids/990001458630205171` 10 times — first 1-3 calls slow, remaining < 0.1s. Per CONTEXT `<specifics>` section.

### Smallest Set of Tests That Proves "No Single NLI Slowdown Can Hang the Threadpool"

The Nyquist-critical invariant for Phase 98 is **threadpool resilience**. The minimum sufficient test set:

1. **Concurrency saturation test (D-26):** 20 ThreadPoolExecutor workers vs hanging session. **Pass criterion: total wall time < 10s AND `_nli_session.get` called ≤ 10 times.** This single test, if green, proves the breaker prevents the 2026-05-25 hang from recurring.

2. **Static call-site coverage test:** grep all 10 D-14..D-23 sites for `is_open()` invocation. **Pass criterion: 10 matches.** Prevents regression where a future PR adds an 11th NLI fetch site without breaker protection.

3. **Bounded-timeout test:** assert each of the 10 sites uses `timeout=(connect, read)` tuple with values bounded by the env knobs. **Pass criterion: no hardcoded `timeout=15` or `timeout=30` remain in any NLI fetch site.**

4. **Telemetry-never-raises test (D-25):** monkeypatch `enqueue_event` to raise. **Pass criterion: breaker still trips, `is_open() == True`, no exception propagates to caller.** Proves the telemetry seam can't itself cause an outage.

5. **Lock-correctness concurrency test (D-27):** `_ConcurrencyRecorder` + `threading.Barrier` proves `record_failure` under N concurrent threads ends with `_consecutive_failures == N` (no lost increments). **Pass criterion: `max_concurrent == 1` inside the locked region.**

These 5 tests, if green, are sufficient evidence that Phase 98's resilience invariant holds. All other tests in `tests/test_nli_circuit_breaker.py` are correctness checks for individual decisions, not invariant proofs.

### Observability Invariants

- **PostHog dashboard:** Within 24h of deploy, `nli_breaker_opened` events should appear (if NLI flaps at all). Conversely, `nli_breaker_closed` events should appear with `downtime_seconds` properties — if events of type `_opened` appear with no matching `_closed`, the breaker is stuck (Pitfall 4 — `time.time` slip).
- **Journal pattern:** Per incident doc §5, `Failed to fetch FL IDs` lines should appear AT MOST 3 times per 60s window per sys_id, instead of once per request.
- **Drop counter:** `web/api_hardening.get_dropped_event_count()` (now extended to include breaker events if Pattern 2 option (a) factors out a shared module) should remain at 0 during normal operation. A growing drop counter indicates PostHog queue saturation, which itself is a signal worth alerting on.

### Wave 0 Gaps

- [ ] `tests/test_nli_circuit_breaker.py` — covers D-01..D-28
- [ ] `tests/conftest.py` autouse fixture for breaker state reset — needed to prevent test order pollution (Pitfall 6)
- [ ] Optional: `shared/posthog_server.py` (if Pattern 2 option (a) is chosen) — factor PostHog queue+drain out of `web/api_hardening.py` for breaker reuse. If option (b) is chosen, breaker imports `web.api_hardening._event_queue` directly (shorter ship path, weaker boundary).
- [ ] Framework install: none required (pytest + unittest.mock already in `requirements.txt`)

---

## Security Domain

### Applicable ASVS Categories

| ASVS Category | Applies | Standard Control |
|---------------|---------|-----------------|
| V2 Authentication | no | Phase 98 does not touch auth |
| V3 Session Management | no | Breaker is global, not per-session |
| V4 Access Control | no | No new endpoints; existing /api/* keep their access rules |
| V5 Input Validation | partial | Env-var parsing must defend against malformed integers (use `max(1, int(...))` idiom from `web/api.py:32`) |
| V6 Cryptography | no | No crypto involved |
| V8 Data Protection | partial | PostHog event properties (`triggering_path`, `failure_type`) must NOT leak user data — `path` is a static function name, `failure_type` is a literal enum, OK by construction |
| V11 Business Logic | yes | The phase IS a resilience control — V11.1.1 "verify the application will only process logical workflows" maps to "breaker prevents NLI degradation from cascading" |

### Known Threat Patterns for `requests` + sync FastAPI

| Pattern | STRIDE | Standard Mitigation |
|---------|--------|---------------------|
| SSRF via NLI fetch | Tampering | URL is hardcoded (`iiif.nli.org.il`) per call site — already enforced; `/api/proxy_image` validates domain against `ALLOWED_IMAGE_DOMAINS` allowlist |
| Threadpool exhaustion (THIS PHASE) | Denial of Service | Bounded timeouts + circuit breaker (D-09 + D-11) — Phase 98 IS the mitigation |
| Telemetry information disclosure | Information Disclosure | Event properties limited to static enums + path names; no user-provided strings emitted to PostHog |
| Breaker abuse via flooding | Denial of Service | A flood of timeouts trips the breaker, which DEGRADES (returns empty FL IDs) but doesn't FAIL; the breaker is the intended degradation surface |
| Env-var injection | Tampering | `os.environ.get(..., 'DEFAULT')` + `int()` parse — malformed values fall through to `ValueError` which crashes startup loudly (intentional fail-fast) |

**Note:** The phase introduces no new attack surface. It HARDENS an existing failure mode (threadpool exhaustion). The PostHog event additions are server-side-only and contain no user-controlled data.

---

## Project Constraints (from CLAUDE.md)

The following CLAUDE.md directives constrain the plan. Plans MUST honor these:

- **safe_storage chokepoint invariant (Phase 87):** Phase 98 does NOT touch `app.storage.user`. No new `web/safe_storage.py` allowlist entries needed. Verified — circuit breaker is process-global, not per-user. CONTEXT.md `<canonical_refs>` explicitly flags this.
- **scp DBs FIRST, then push code (Phase 84 / 2026-05-11 incident):** Phase 98 has no DB changes. Standard `git push` + `systemctl restart genizah-web.service` is sufficient. No SQLite sidecars or Supabase migrations.
- **No GitHub release for web-only changes (feedback_no_github_release_for_web_only.md):** Phase 98 is web-server + shared/genizah_core code. Genizah_core touches affect desktop builds for v7.15+. **Recommend: ship web-only deploy first, defer desktop installer to next desktop release.** Do NOT cut a v7.X.Y GitHub release for the web deploy alone.
- **Version bumping for releases (CLAUDE.md):** Phase 98 standalone is web-only — bump only `version.py` patch number if deploying. Desktop release (next milestone) bumps minor/major.
- **Documentation update protocol:** `CLAUDE.md` Environment Variables section MUST be updated with the 6 new `NLI_*` knobs (D-09 + the dropped `NLI_SEMAPHORE_TIMEOUT` default). `docs/OPEN_ISSUES.md` 2026-05-25 entry (if added during incident triage) flips to ✅ Fixed (date).
- **`python scripts/check_docs.py` before commit:** Standard pre-flight. Phase 98 should pass cleanly — no documentation gaps introduced.
- **Hebrew RTL convention:** N/A — Phase 98 is pure infrastructure, no user-facing strings.
- **Forbidden patterns:** None violated. Phase 98 uses stdlib + existing `requests` patterns.

---

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| Class-attribute breaker on `GenizahCore` | Module-level singleton in `shared/` | Phase 98 (this) | Single source of truth across `web/api.py` and `genizah_core.py`; testable via `monkeypatch.setattr` on module attributes |
| `time.time()` for open-until window | `time.monotonic()` | Phase 98 (D-04) | Immune to NTP jumps and wall-clock changes |
| 15-30s hardcoded timeouts | env-knob `NLI_*_READ_TIMEOUT` (3-5s) | Phase 98 (D-09) | Per-call blocking budget bounded to <8s instead of 25s |
| `NLI_SEMAPHORE_TIMEOUT=20` | `NLI_SEMAPHORE_TIMEOUT=1` | Phase 98 (D-10) | Worker threads waste 1s max on contended NLI fetches |
| `web/analytics.posthog_capture` (UI-context-bound) | `web/api_hardening._event_queue` (server-side queue+daemon) | Phase 78 (already in place) | Telemetry from background threads now reliable |
| Class-method exception-only failure counting | Module function with status-code-aware failure typing | Phase 98 (D-06) | 5xx and 429 responses now correctly trip the breaker |

**Deprecated/outdated:**
- `genizah_core.GenizahCore._nli_circuit_is_open` / `._nli_record_failure` / `._nli_record_success` — to be REMOVED in Phase 98 (D-03 mandates clean migration, not deprecation stubs)
- Hardcoded `timeout=15`, `timeout=30`, `timeout=10` at NLI fetch sites — to be REPLACED with env-knob lookups

---

## Assumptions Log

| # | Claim | Section | Risk if Wrong |
|---|-------|---------|---------------|
| A1 | `web/analytics.posthog_capture` silently no-ops outside a NiceGUI UI context | Pitfall 1 + Architectural Responsibility Map | If wrong, breaker telemetry might work via `posthog_capture` after all — minor (we'd just use the wrong idiom for code review reasons). VERIFIED partially: `ui.run_javascript` requires an active Client per NiceGUI source, and the `try/except` swallows the failure silently — but I did not run a smoke test from a background thread. [ASSUMED] |
| A2 | `requests.exceptions.Timeout` is the parent of both `ReadTimeout` and `ConnectTimeout` in requests 2.32.5 | Pattern 1 + Code Examples | If wrong, the `except Timeout` clause might miss one or the other variant. VERIFIED by Python class hierarchy convention but not by reading requests 2.32 source. [CITED: requests changelog says hierarchy stable since 2.x] |
| A3 | `urllib3.exceptions.MaxRetryError` is wrapped by `requests.ConnectionError` when raised from a `Session.get` call | Don't Hand-Roll + Pattern 1 | If wrong, some connection-level failures might escape the `except (Timeout, ConnectionError)` clause. VERIFIED by `requests` source convention but not by exhaustive testing. [CITED: requests/adapters.py wraps urllib3 exceptions] |
| A4 | `POSTHOG_API_KEY` is set on production (per CHANGELOG.md `v7.0` / `phc_xxxxx` template) | Pattern 2 | If wrong on prod, telemetry queue drains silently (drop counter increments). Not a phase blocker — breaker still works without telemetry. [CITED: docs/guides/DEPLOYMENT_TECHNICAL.md:897] |
| A5 | The 8-slot semaphore is sufficient for normal operation and the breaker reduces (not eliminates) semaphore contention | Architecture diagram | If wrong, the semaphore could remain a bottleneck under high concurrency even with the breaker open. Mitigated by D-12 defensive re-check. [ASSUMED — based on existing prod traffic patterns] |
| A6 | All 10 D-14..D-23 line numbers in CONTEXT.md are still accurate at plan-execution time | Architectural Responsibility Map + Code Examples | If wrong (concurrent commits shift line numbers), the plan must re-locate by symbol name (`fetch_fl_ids_from_nli`, `_fetch_nli_image_bytes`, etc.) rather than by line. **Recommend plan-execution uses symbol-based location, not line numbers, to survive concurrent edits.** [VERIFIED at time of writing — line numbers match current HEAD] |
| A7 | The existing `_nli_cache` per-sys_id negative cache is complementary to the new breaker, not redundant | code_context in CONTEXT.md | If they conflict, double-decrementing or stuck-closed scenarios possible. **Recommend keeping both layers** — they answer different questions: negative cache = "this sys_id is bad", breaker = "all of NLI is bad". [CITED: CONTEXT.md `<code_context>` explicit guidance] |
| A8 | `shared/` is the appropriate tier for the breaker (NOT `web/`) | Architectural Responsibility Map | If wrong, desktop app would lose breaker integration (Phase 98 D-22 + D-23 require genizah_core to consume the same breaker — only possible if the breaker is in `shared/` or root, not `web/`). [CITED: project convention — `shared/` houses code consumed by BOTH web and desktop] |

**Confirmation needed before plan execution:**
- A6 (line number staleness): the planner should grep for symbol names rather than relying on D-14..D-23 line numbers verbatim
- Option (a) vs option (b) for PostHog factorization: CONTEXT.md D-25 leaves this to Claude's discretion; recommend option (a) (factor out `shared/posthog_server.py`) but plan should explicitly state the choice

---

## Open Questions

1. **PostHog factorization: factor out `shared/posthog_server.py` (option a) or import from `web/api_hardening.py` (option b)?**
   - What we know: D-25 says "use the existing PostHog emission pattern in the codebase". The existing pattern is in `web/api_hardening.py`. The breaker is in `shared/`.
   - What's unclear: whether `shared/` should depend on `web/` (option b) or vice versa (option a).
   - Recommendation: Option (a) — factor out `shared/posthog_server.py` with `enqueue_event(event, properties, distinct_id='system')`. `web/api_hardening.py` migrates its `capture_api_event` to use the new shared module (1-line change). Cleaner boundary, supports desktop telemetry if needed in future. Plan should make this choice explicit.

2. **Does `genizah_core.py` (used by both desktop and web) need to import from `shared/nli_circuit_breaker.py`, and does that create an import cycle?**
   - What we know: `shared/` is already imported by `genizah_core.py` (`from shared.synthetic_sys_id import is_synthetic_sys_id`).
   - What's unclear: does `shared/nli_circuit_breaker.py` transitively need anything from `genizah_core.py`?
   - Recommendation: NO transitive dependency. The breaker is a leaf module — it imports only stdlib and the proposed `shared/posthog_server.py`. Cycle impossible. Plan should verify via `python -c "import shared.nli_circuit_breaker"` from a clean PYTHONPATH.

3. **Should the auto-recovery on `is_open()` reset the failure counter, or only the open_until?**
   - What we know: D-08 says `record_success` resets. D-04 says use monotonic time for open_until.
   - What's unclear: when the 60s window elapses without an explicit success, should the next call's failure increment from N or from 1?
   - Recommendation: keep counter as-is (the auto-recovered breaker re-trips after 1 additional failure rather than 3) — this is the SAFER default. A flapping NLI should be detected faster on the second outage. If this proves too aggressive in prod, revisit. Plan should encode the chosen semantic in a docstring AND a test.

4. **Should the `tests/test_nli_circuit_breaker.py` autouse fixture reset state by accessing module globals directly, or via a public `_reset_for_tests()` helper?**
   - What we know: testing module-level state in Python is awkward.
   - Recommendation: add a `_reset_for_tests()` private function in `shared/nli_circuit_breaker.py` that the fixture calls. Cleaner than `monkeypatch.setattr` chains. Marks the test seam explicitly.

---

## Environment Availability

| Dependency | Required By | Available | Version | Fallback |
|------------|------------|-----------|---------|----------|
| Python 3.10+ | Entire phase | ✓ | 3.10+ (project standard) | — |
| `requests` | All NLI fetch sites + PostHog server-side post | ✓ | 2.32.5 | — |
| `urllib3` (transitive) | Exception taxonomy | ✓ | 2.6.3 | — |
| `pytest` | Test suite | ✓ | (project standard) | — |
| `unittest.mock` (stdlib) | Test mocking | ✓ | (stdlib) | — |
| `queue` (stdlib) | PostHog event buffer | ✓ | (stdlib) | — |
| `threading` (stdlib) | Lock + concurrency tests | ✓ | (stdlib) | — |
| PostHog endpoint (`eu.i.posthog.com/capture`) | Telemetry | ✓ (production) | live service | If `POSTHOG_API_KEY` unset, events silently dropped — breaker still functional |
| NLI IIIF endpoint (`iiif.nli.org.il`) | Phase target | varies | external | When down, breaker trips by design — that's the feature |

**Missing dependencies with no fallback:** None.

**Missing dependencies with fallback:** None.

---

## Sources

### Primary (HIGH confidence — code read in this session)
- `C:/GenizahSearch/.planning/phases/98-nli-resilience-circuit-breaker-and-bounded-timeout-hardening/98-CONTEXT.md` — full read, 28 locked decisions
- `C:/GenizahSearch/docs/INCIDENT-2026-05-25-nli-iiif-hang.md` — full read, root cause + initial fix plan
- `C:/GenizahSearch/docs/INCIDENT-2026-05-25-CODEX-CRITIQUE.md` — full read, 9-point critique that drove CONTEXT
- `C:/GenizahSearch/web/api.py` lines 1-110 (NLI config), 630-900 (fetch_fl_ids_from_nli + _fetch_nli_image_bytes), 1970-2010 (/api/proxy_image)
- `C:/GenizahSearch/web/api_hardening.py` lines 1-100 + 521-665 — server-side PostHog queue+daemon pattern (Pattern 2)
- `C:/GenizahSearch/web/analytics.py` — full read, confirmed UI-context dependency of `posthog_capture` (Pitfall 1)
- `C:/GenizahSearch/genizah_core.py` lines 3930-3961 (current breaker), 4020-4099 (fetch_iiif_manifest with breaker), 4100-4249 (fetch_marc_data with breaker), 4630-4780 (additional MARC/FL fetch paths D-22/D-23)
- `C:/GenizahSearch/shared/puzzle_image_service.py` lines 155-265 — IIIF fetch sites D-19/D-20
- `C:/GenizahSearch/web/pages/puzzle.py` lines 1980-2015 — direct NLI manifest fetch D-21
- `C:/GenizahSearch/tests/test_nli_cache_persist_retry.py` — full read, `_ConcurrencyRecorder` + `threading.Barrier` + `unittest.mock.patch` idiom
- `C:/GenizahSearch/tests/conftest.py` — full read, fixtures + path setup
- `C:/GenizahSearch/.planning/codebase/TESTING.md` — full read, project test conventions
- `C:/GenizahSearch/.planning/codebase/CONVENTIONS.md` — partial read, naming + imports
- `C:/GenizahSearch/.planning/codebase/STACK.md` — full read, env vars + frameworks
- `C:/GenizahSearch/.planning/config.json` — `nyquist_validation: true` confirmed
- `C:/GenizahSearch/CLAUDE.md` — full read, project constraints
- `C:/GenizahSearch/requirements.txt:3` — `requests==2.32.5` verified
- Runtime: `python -c "import requests; import urllib3"` confirmed installed versions

### Secondary (MEDIUM confidence)
- `C:/GenizahSearch/docs/OPEN_ISSUES.md` lines 1-80 — open issues context (PostHog/leak history)
- `C:/GenizahSearch/web/supabase_client.py` lines 144-155 + 294 + 454-476 — Phase 92.2 WeakKeyDictionary memo pattern (referenced but not adopted — breaker uses simpler `threading.Lock` + module-level vars)

### Tertiary (LOW confidence — none, all claims grounded in primary sources)
- None.

---

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH — all libs verified at runtime (`python -c "import X; print(X.__version__)"`), versions match `requirements.txt`
- Architecture: HIGH — patterns proven in 2+ existing production files each
- Pitfalls: HIGH — 4 of 7 pitfalls trace to incidents already in the codebase (NLI hang, lock pattern, etc.)
- Validation Architecture: HIGH — test idiom verbatim from existing `tests/test_nli_cache_persist_retry.py`
- Security: HIGH — phase introduces no new attack surface; PostHog event properties are static enums
- Open questions: MEDIUM — 4 questions documented; all have concrete recommendations + escape hatches

**Research date:** 2026-05-25
**Valid until:** 2026-06-25 (30 days — `requests` and `urllib3` are stable; CONTEXT.md decisions are locked; only PostHog SDK endpoint URL is external dependency and is unlikely to change)
