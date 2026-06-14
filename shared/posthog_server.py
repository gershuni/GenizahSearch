# -*- coding: utf-8 -*-
"""Server-side PostHog event emission — safe from ANY thread context.

Factored out of web/api_hardening.py:524-567 (Phase 78 server-side capture
idiom) so non-web modules can emit telemetry without depending on web/.

Consumers:
- shared/nli_circuit_breaker.py (Phase 98 Plan 02) — breaker open/close events
- desktop/telemetry.py (Phase 111) — desktop capture via set_capture_api_key + key setters
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

Phase 111 neutral additions (backward-compatible):
- set_default_distinct_id: inject default distinct_id for desktop events
- register_scrub_hook: optional defence-in-depth scrub hook (before queue put)
- set_capture_api_key: desktop key override (NO os.environ mutation — D-04)
- set_capture_host: host override for non-default EU PostHog endpoint
- _flush_before_exit: bounded synchronous drain+POST for crash/atexit paths
- _drain_and_discard: drain queue without sending (CONSENT-08 opt-out)
"""

import logging
import os
import queue
import threading
import time
from datetime import datetime, timezone
from typing import Callable

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

# Phase 111 neutral additions — guarded by dedicated locks
# (same pattern as _dropped_events_lock above)
_default_distinct_id: str | None = None
_default_distinct_id_lock = threading.Lock()

_scrub_hook: Callable[[dict], 'dict | None'] | None = None
_scrub_hook_lock = threading.Lock()

# Transport config overrides — desktop sets these; web never calls these setters
# so web behavior (POSTHOG_API_KEY from env) is completely unchanged (D-04).
_api_key_override: str | None = None
_host_override: str | None = None
_capture_config_lock = threading.Lock()


def get_dropped_event_count() -> int:
    """Return the count of events dropped due to queue saturation (queue.Full).

    Monotonic — does not reset across process lifetime. Use for diagnostics.
    Note: this counter is DISTINCT from web.api_hardening.get_dropped_event_count()
    (Phase 98 REVIEWS.md Issue 5 Option A). Operators must monitor both.
    """
    with _dropped_events_lock:
        return _dropped_events


def set_default_distinct_id(uid: str | None) -> None:
    """Set a module-level default distinct_id injected when caller passes 'system'.

    Called once by desktop/telemetry.py after consent is granted. Web callers
    always pass an explicit distinct_id so this never changes their behavior.
    Thread-safe via _default_distinct_id_lock.
    """
    global _default_distinct_id
    with _default_distinct_id_lock:
        _default_distinct_id = uid


def register_scrub_hook(fn: Callable[[dict], 'dict | None'] | None) -> None:
    """Register an optional scrub hook called inside enqueue_event before queue put.

    fn(payload) -> payload (modified) or None (drop event).
    Defence-in-depth — desktop/telemetry.py's _scrub_props() is the PRIMARY layer.
    Web callers do not register a hook; this is a no-op for them.
    Pass None to unregister.
    Thread-safe via _scrub_hook_lock.
    """
    global _scrub_hook
    with _scrub_hook_lock:
        _scrub_hook = fn


def set_capture_api_key(key: str | None) -> None:
    """Set the transport API key override for the desktop app.

    Desktop installs its embedded GENIZAH_TELEMETRY_KEY here; this does NOT
    mutate os.environ so the web server's POSTHOG_API_KEY environment variable
    is completely unaffected (D-04 / REVIEWS HIGH-1). The transport resolves
    the key as (_api_key_override or os.environ.get('POSTHOG_API_KEY', '')).

    Pass None to revert to the env-variable-only resolution (web behavior).
    Thread-safe via _capture_config_lock.
    """
    global _api_key_override
    with _capture_config_lock:
        _api_key_override = key


def set_capture_host(host: str | None) -> None:
    """Set the PostHog capture host override.

    Desktop GENIZAH_TELEMETRY_HOST path (D-03). None falls back to POSTHOG_HOST.
    Does NOT affect the web server which hard-codes eu.i.posthog.com in its own
    client-side JS init.
    Thread-safe via _capture_config_lock.
    """
    global _host_override
    with _capture_config_lock:
        _host_override = host


def _resolve_api_key() -> str:
    """Resolve the effective API key: override or env fallback."""
    with _capture_config_lock:
        override = _api_key_override
    return (override or os.environ.get('POSTHOG_API_KEY', '')).strip()


def _resolve_capture_url() -> str:
    """Resolve the effective capture URL: host override or POSTHOG_HOST fallback."""
    with _capture_config_lock:
        host = _host_override
    effective_host = (host or POSTHOG_HOST).rstrip('/')
    return f'{effective_host}/capture'


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

    Phase 111 additions (signature unchanged — D-04):
        - If distinct_id == 'system' and _default_distinct_id is set, the default
          is substituted (web callers always pass explicit distinct_id, so unaffected)
        - Optional _scrub_hook called BEFORE queue put (raw data never enters queue)
    """
    global _dropped_events
    # Phase 111: resolve distinct_id from module default if caller passed 'system'
    if distinct_id == 'system':
        with _default_distinct_id_lock:
            if _default_distinct_id is not None:
                distinct_id = _default_distinct_id
    _start_drain_thread_once()
    try:
        payload = {
            'event': event,
            'distinct_id': distinct_id,
            'properties': dict(properties) if properties else {},
            'timestamp': datetime.now(timezone.utc).isoformat(),
        }
        # Phase 111: optional scrub hook (defence-in-depth; primary scrubbing in desktop/telemetry.py)
        # Hook runs BEFORE _event_queue.put_nowait so raw data NEVER enters the queue (Pitfall 3).
        with _scrub_hook_lock:
            hook = _scrub_hook
        if hook is not None:
            try:
                payload = hook(payload)
                if payload is None:
                    return  # hook elected to drop this event
            except Exception:
                return  # fail-closed: drop event rather than risk sending raw data
        try:
            _event_queue.put_nowait(payload)
        except queue.Full:
            with _dropped_events_lock:
                _dropped_events += 1
    except Exception:
        logger.debug('posthog_server.enqueue_event silently dropped', exc_info=True)


def _drain_posthog_queue() -> None:
    """Daemon thread loop: drain _event_queue, POST each event to PostHog.

    Phase 111: key and URL are resolved per-iteration via _resolve_api_key() and
    _resolve_capture_url() so a key set via set_capture_api_key() after the
    daemon started still applies (important for desktop startup ordering).
    """
    while True:
        try:
            event = _event_queue.get(timeout=60)
        except queue.Empty:
            continue
        api_key = _resolve_api_key()
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
            requests.post(_resolve_capture_url(), json=payload, timeout=2.0)
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


def _drain_and_discard() -> None:
    """Empty the in-memory queue WITHOUT sending events to PostHog.

    Called by desktop/telemetry.py::set_consent(False) to purge already-queued
    events when the user opts out (CONSENT-08, Phase 112). A drain on an empty
    queue is a no-op. Does NOT reset _dropped_events (unlike _reset_for_tests).
    Not called by any web code.
    """
    while True:
        try:
            _event_queue.get_nowait()
        except queue.Empty:
            break


def _flush_before_exit(timeout: float = 0.5) -> None:
    """Drain the event queue synchronously before process exit, within a hard deadline.

    Called from:
    - sys.excepthook wrapper in desktop/telemetry.py (crash events — atexit does NOT
      run on unhandled exceptions in CPython)
    - atexit handler (clean exits)
    NOT called by web code — web is a long-lived process that doesn't need exit flush.

    Respects a TRULY bounded wall-time deadline (REVIEWS MEDIUM): computes
    remaining = deadline - time.monotonic() before each POST and stops POSTing
    (drain-only) once the budget is exhausted. Per-POST timeout = min(remaining, 2.0).

    Bypasses the daemon thread (which may be dying at crash exit).
    """
    deadline = time.monotonic() + timeout
    api_key = _resolve_api_key()
    url = _resolve_capture_url()
    while True:
        try:
            event = _event_queue.get_nowait()
        except queue.Empty:
            break
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            # Budget exhausted: keep draining (discard) but no more POSTs
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
            requests.post(url, json=payload, timeout=min(remaining, 2.0))
        except Exception:
            pass  # Fire-and-forget — silent drop on error


def _reset_for_tests() -> None:
    """Test seam — drain queue + reset drop counter + clear Phase 111 globals.

    NOT for production use. Clears: _dropped_events, _default_distinct_id,
    _scrub_hook, _api_key_override, _host_override. Does NOT stop the drain thread
    (Python threads aren't cleanly stoppable without flags; the daemon dies with
    the process). Tests that need queue isolation should monkeypatch _event_queue
    or assert via the drop counter.
    """
    global _dropped_events, _default_distinct_id, _scrub_hook, _api_key_override, _host_override
    # Drain queue
    while True:
        try:
            _event_queue.get_nowait()
        except queue.Empty:
            break
    with _dropped_events_lock:
        _dropped_events = 0
    # Phase 111 additions: clear new globals under their locks (REVIEWS HIGH-2)
    with _default_distinct_id_lock:
        _default_distinct_id = None
    with _scrub_hook_lock:
        _scrub_hook = None
    with _capture_config_lock:
        _api_key_override = None
        _host_override = None


__all__ = [
    'POSTHOG_HOST',
    'POSTHOG_CAPTURE_URL',
    'enqueue_event',
    'get_dropped_event_count',
    '_reset_for_tests',
    # Phase 111 neutral additions:
    'set_default_distinct_id',
    'register_scrub_hook',
    'set_capture_api_key',
    'set_capture_host',
    '_flush_before_exit',
    '_drain_and_discard',
]
