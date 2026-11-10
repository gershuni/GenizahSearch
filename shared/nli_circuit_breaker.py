"""Phase 98 — Shared NLI circuit breaker (module-level singleton).

Trips after N consecutive failures across ANY NLI call site; short-circuits
further calls for `NLI_CIRCUIT_WINDOW` seconds. Process-local (D-05).

Decisions encoded:
- D-01: single global "NLI" breaker key (not per-host)
- D-02: shared global state across all call sites
- D-03: module-level singleton with threading.Lock-guarded state
- D-04: time.monotonic() (NEVER time.time())
- D-06: Timeout / ConnectionError / 5xx / 429 trip the breaker
- D-07: 404 and empty-manifest do NOT trip (caller responsibility)
- D-08: record_success resets counter
- D-09: env-driven thresholds + read timeouts
- D-24/D-25: fire-and-forget PostHog telemetry on state transitions

Auto-recovery semantics (RESEARCH Open Question 2):
- When `_open_until` elapses WITHOUT explicit record_success, the counter is
  NOT reset. The next failure re-trips after 1 additional increment.
- Rationale: flapping NLI should be detected faster on the second outage.
- Trade-off: a single transient failure post-recovery re-trips immediately;
  acceptable per CONTEXT (flat 60s window, no half-open state).

Usage from any NLI call site:

    from shared.nli_circuit_breaker import (
        is_open, record_failure, record_success,
        NLI_CONNECT_TIMEOUT, NLI_IIIF_READ_TIMEOUT,
    )

    if is_open():
        return []  # short-circuit
    try:
        resp = session.get(url, timeout=(NLI_CONNECT_TIMEOUT, NLI_IIIF_READ_TIMEOUT))
        if resp.status_code == 200:
            record_success(path='my_call_site')
            return ...
        elif resp.status_code == 429 or 500 <= resp.status_code < 600:
            failure_type = '429' if resp.status_code == 429 else '5xx'
            record_failure(failure_type=failure_type, path='my_call_site')
        # 404 / empty -> per-sys_id negative cache only; do NOT call record_failure
    except requests.exceptions.Timeout:
        record_failure(failure_type='timeout', path='my_call_site')
    except requests.exceptions.ConnectionError:
        record_failure(failure_type='connection_error', path='my_call_site')
"""

import logging
import os
import threading
import time
from typing import Literal

logger = logging.getLogger(__name__)

# -----------------------------------------------------------------------------
# Env-driven configuration (D-09). Read at module import. max(1, ...) defends
# against malformed values per V5 input validation (Security Domain).
# -----------------------------------------------------------------------------
NLI_CIRCUIT_THRESHOLD = max(1, int(os.environ.get('NLI_CIRCUIT_THRESHOLD', '3')))
NLI_CIRCUIT_WINDOW = max(1, int(os.environ.get('NLI_CIRCUIT_WINDOW', '60')))

# Read-timeout knobs — exported as module-level constants so callers in
# web/api.py, shared/puzzle_image_service.py, web/pages/puzzle.py, and
# genizah_core.py all import from here (single source of truth).
NLI_IIIF_READ_TIMEOUT = max(1, int(os.environ.get('NLI_IIIF_READ_TIMEOUT', '5')))
NLI_MARC_READ_TIMEOUT = max(1, int(os.environ.get('NLI_MARC_READ_TIMEOUT', '3')))
NLI_IMAGE_READ_TIMEOUT = max(1, int(os.environ.get('NLI_IMAGE_READ_TIMEOUT', '5')))
NLI_CONNECT_TIMEOUT = max(1, int(os.environ.get('NLI_CONNECT_TIMEOUT', '3')))

# -----------------------------------------------------------------------------
# Locked module-level state. Per D-03, this is a process-local singleton.
# -----------------------------------------------------------------------------
_lock = threading.Lock()
_consecutive_failures = 0
_open_until = 0.0  # monotonic timestamp (D-04)
_opened_at = 0.0   # monotonic timestamp at the moment of opening (for downtime telemetry)

# Type alias for failure-typing per D-06
FailureType = Literal['timeout', 'connection_error', '5xx', '429']


def is_open() -> bool:
    """True iff the breaker has tripped and callers should short-circuit.

    Auto-recovers when monotonic time passes `_open_until`. Auto-recovery
    does NOT reset the failure counter — see module docstring for rationale.
    Cheap O(1) under lock.
    """
    with _lock:
        if _open_until <= 0.0:
            return False
        return time.monotonic() < _open_until


def record_failure(failure_type: FailureType, path: str) -> None:
    """Count an NLI failure. Trips the breaker when threshold reached. D-06.

    Args:
        failure_type: one of 'timeout' | 'connection_error' | '5xx' | '429'
        path: short identifier of the calling code site (e.g. 'fetch_fl_ids_from_nli');
              used in PostHog telemetry only

    Side effects:
        - Increments `_consecutive_failures` under lock
        - When the increment crosses THRESHOLD and the breaker is currently
          CLOSED, sets `_open_until = monotonic + WINDOW` and emits
          `nli_breaker_opened` (outside the lock, per Pitfall 2)
    """
    global _consecutive_failures, _open_until, _opened_at
    just_opened = False
    snapshot_failures = 0
    with _lock:
        _consecutive_failures += 1
        now = time.monotonic()
        # Only "open" if currently closed (open_until in past or zero) AND threshold met.
        # If already open, additional failures just bump the counter (telemetry not re-emitted).
        if _consecutive_failures >= NLI_CIRCUIT_THRESHOLD and _open_until <= now:
            _open_until = now + NLI_CIRCUIT_WINDOW
            _opened_at = now
            just_opened = True
            snapshot_failures = _consecutive_failures
    if just_opened:
        _safe_emit_opened(snapshot_failures, path, failure_type)


def record_success(path: str = '') -> None:
    """Reset the consecutive-failure counter on any successful NLI fetch. D-08.

    Side effects:
        - Sets `_consecutive_failures = 0`, `_open_until = 0.0`, `_opened_at = 0.0`
        - If the breaker was previously OPEN (open_until in future), emits
          `nli_breaker_closed` (outside the lock, per Pitfall 2) with the
          downtime_seconds property computed from `_opened_at`.
    """
    global _consecutive_failures, _open_until, _opened_at
    was_open = False
    prior_opened_at = 0.0
    with _lock:
        now = time.monotonic()
        if _open_until > now:
            was_open = True
            prior_opened_at = _opened_at
        _consecutive_failures = 0
        _open_until = 0.0
        _opened_at = 0.0
    if was_open:
        downtime = max(0.0, time.monotonic() - prior_opened_at)
        _safe_emit_closed(downtime, path)


# -----------------------------------------------------------------------------
# Test seams. _reset_for_tests is called by the autouse fixture in
# tests/conftest.py. _state_snapshot lets tests assert internal state
# without grabbing the lock manually.
# -----------------------------------------------------------------------------
def _state_snapshot() -> dict:
    """Return a snapshot of breaker state. For TESTS only."""
    with _lock:
        return {
            'consecutive_failures': _consecutive_failures,
            'open_until_monotonic': _open_until,
            'opened_at_monotonic': _opened_at,
            'is_open_now': _open_until > time.monotonic(),
            'threshold': NLI_CIRCUIT_THRESHOLD,
            'window_seconds': NLI_CIRCUIT_WINDOW,
        }


def _reset_for_tests() -> None:
    """Reset all module state to fresh-import values. For TESTS only."""
    global _consecutive_failures, _open_until, _opened_at
    with _lock:
        _consecutive_failures = 0
        _open_until = 0.0
        _opened_at = 0.0


# -----------------------------------------------------------------------------
# Telemetry — D-24, D-25. NEVER raises into caller; logs at DEBUG on failure.
# Imports the shared posthog_server module (Plan 98-01) — NOT web/analytics.
# -----------------------------------------------------------------------------
def _safe_emit_opened(failures: int, path: str, failure_type: str) -> None:
    try:
        from shared.posthog_server import enqueue_event
        enqueue_event(
            event='nli_breaker_opened',
            properties={
                'consecutive_failures': failures,
                'triggering_path': path,
                'failure_type': failure_type,
                'threshold': NLI_CIRCUIT_THRESHOLD,
                'window_seconds': NLI_CIRCUIT_WINDOW,
            },
        )
    except Exception:
        logger.debug('nli_breaker_opened telemetry suppressed', exc_info=True)


def _safe_emit_closed(downtime_seconds: float, path: str) -> None:
    try:
        from shared.posthog_server import enqueue_event
        enqueue_event(
            event='nli_breaker_closed',
            properties={
                'downtime_seconds': round(downtime_seconds, 3),
                'closed_by_path': path or '<unspecified>',
            },
        )
    except Exception:
        logger.debug('nli_breaker_closed telemetry suppressed', exc_info=True)


__all__ = [
    # Public API
    'is_open',
    'record_failure',
    'record_success',
    # Env-driven timeout constants for callers
    'NLI_CIRCUIT_THRESHOLD',
    'NLI_CIRCUIT_WINDOW',
    'NLI_CONNECT_TIMEOUT',
    'NLI_IIIF_READ_TIMEOUT',
    'NLI_MARC_READ_TIMEOUT',
    'NLI_IMAGE_READ_TIMEOUT',
    # Test seams (private by convention but exported for explicit access)
    '_state_snapshot',
    '_reset_for_tests',
]
