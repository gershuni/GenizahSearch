# -*- coding: utf-8 -*-
"""Server-side PostHog event emission — safe from ANY thread context.

Factored out of web/api_hardening.py:524-567 (Phase 78 server-side capture
idiom) so non-web modules can emit telemetry without depending on web/.

Consumers:
- shared/nli_circuit_breaker.py (Phase 98 Plan 02) — breaker open/close events
- Future: any background-thread or Qt-main-loop telemetry needs

Design contract:
- Fire-and-forget: NEVER blocks, NEVER raises into the caller
- Drops counted via _dropped_events on queue.Full
- POSTHOG_API_KEY unset → events silently dropped at drain time (still queued)
- Daemon thread starts lazily on first enqueue_event() call

Phase 98 REVIEWS.md Issue 5 (Option A chosen): this module does NOT replace
web/api_hardening.py's queue. The web layer keeps its own _event_queue +
_dropped_events for backward compatibility with the 5 monkeypatches in
tests/test_api_hardening.py and tests/test_search_api_v2.py. The trade-off
is two separate queues + two drop counters; operators must monitor BOTH
web.api_hardening.get_dropped_event_count() AND
shared.posthog_server.get_dropped_event_count(). A future cleanup plan
could migrate api_hardening to consume this module — out of scope for
Phase 98.

DO NOT use web/analytics.posthog_capture() from background threads — that
helper depends on ui.run_javascript() which silently no-ops outside a
NiceGUI client context (see Pitfall 1 in 98-RESEARCH.md).
"""

import logging
import os
import queue
import threading
from datetime import datetime, timezone

import requests

logger = logging.getLogger(__name__)

# Endpoint — matches web/api_hardening.py:59-60 exactly
POSTHOG_HOST = 'https://eu.i.posthog.com'
POSTHOG_CAPTURE_URL = f'{POSTHOG_HOST}/capture'

# Module-level state — guarded by locks for thread safety
_event_queue: queue.Queue = queue.Queue(maxsize=10000)
_drain_thread_started = threading.Event()

_dropped_events: int = 0
_dropped_events_lock = threading.Lock()


def get_dropped_event_count() -> int:
    """Return the count of events dropped due to queue saturation (queue.Full).

    Monotonic — does not reset across process lifetime. Use for diagnostics.
    Note: this counter is DISTINCT from web.api_hardening.get_dropped_event_count()
    (Phase 98 REVIEWS.md Issue 5 Option A). Operators must monitor both.
    """
    with _dropped_events_lock:
        return _dropped_events


def enqueue_event(
    event: str,
    properties: dict,
    distinct_id: str = 'system',
) -> None:
    """Fire-and-forget PostHog event enqueue. NEVER blocks. NEVER raises.

    Args:
        event: PostHog event name (e.g. 'nli_breaker_opened')
        properties: dict of event properties (must be JSON-serializable)
        distinct_id: PostHog distinct_id — default 'system' for non-user events

    Side effects:
        - Lazy-starts the drain daemon thread on first call (process-lifetime)
        - On queue.Full, increments _dropped_events (visible via get_dropped_event_count)
        - On any other exception, silently catches and logs at DEBUG
    """
    global _dropped_events
    _start_drain_thread_once()
    try:
        payload = {
            'event': event,
            'distinct_id': distinct_id,
            'properties': dict(properties) if properties else {},
            'timestamp': datetime.now(timezone.utc).isoformat(),
        }
        try:
            _event_queue.put_nowait(payload)
        except queue.Full:
            with _dropped_events_lock:
                _dropped_events += 1
    except Exception:
        logger.debug('posthog_server.enqueue_event silently dropped', exc_info=True)


def _drain_posthog_queue() -> None:
    """Daemon thread loop: drain _event_queue, POST each event to PostHog.

    Verbatim from web/api_hardening.py:547-567 — proven idiom, do NOT modify
    without a corresponding update to that module's drain loop.
    """
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


def _start_drain_thread_once() -> None:
    """Lazy-start the drain daemon. Idempotent (Event-guarded)."""
    if _drain_thread_started.is_set():
        return
    _drain_thread_started.set()
    t = threading.Thread(
        target=_drain_posthog_queue,
        name='posthog-shared-drain',
        daemon=True,
    )
    t.start()


def _reset_for_tests() -> None:
    """Test seam — drain queue + reset drop counter. NOT for production use.

    Does NOT stop the drain thread (Python threads aren't cleanly stoppable
    without flags; the daemon dies with the process). Tests that need queue
    isolation should monkeypatch _event_queue or assert via the drop counter.
    """
    global _dropped_events
    # Drain queue
    while True:
        try:
            _event_queue.get_nowait()
        except queue.Empty:
            break
    with _dropped_events_lock:
        _dropped_events = 0


__all__ = [
    'POSTHOG_HOST',
    'POSTHOG_CAPTURE_URL',
    'enqueue_event',
    'get_dropped_event_count',
    '_reset_for_tests',
]
