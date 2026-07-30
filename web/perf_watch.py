# -*- coding: utf-8 -*-
"""Lightweight always-on performance watch for the web app (2026-07-30).

Why this exists
---------------
A slowness investigation on 2026-07-30 could not distinguish two very different
causes because nothing on the box recorded per-request server time:

  * nginx's access log uses the default `combined` format -- no `$request_time`
    and no `$upstream_response_time`, so origin latency is invisible in logs.
  * the only in-app timing was `_ListsPerfRouteTimingMiddleware`, scoped to the
    single `/lists` path and dormant unless an env flag is set.

So a request that took 9 seconds at the edge left no trace server-side, and the
diagnosis had to be inferred from outside. This module closes that gap with the
two signals that actually discriminate:

1. ``SlowRequestTimingMiddleware`` -- wall-clock per HTTP request, entry to
   response-body flush, logged only above a threshold.

2. ``start_event_loop_lag_monitor`` -- how late a timer that asked to sleep N
   seconds actually woke up. This is the decisive one. uvicorn runs a SINGLE
   worker (``ui.run`` in web/main.py passes no ``workers=``), and NiceGUI invokes
   sync page builders and sync event callbacks directly on the event loop, so one
   blocking Supabase/NLI call stalls EVERY concurrent request -- including
   unrelated static assets. Because a blocked-on-I/O process burns no CPU, this
   is invisible in load average and `top`: during the investigation the box read
   load 0.03 while serving multi-second responses. Loop lag makes it visible.

Both are deliberately quiet: nothing is logged while the app behaves. Defaults
are conservative so enabling this in production adds log lines only when there is
a real stall to look at.

Env knobs
---------
``GENIZAH_PERF_WATCH``          1/true (default) to enable; 0 disables both signals.
``GENIZAH_SLOW_REQUEST_MS``     log HTTP requests slower than this (default 1500).
``GENIZAH_LOOP_LAG_MS``         log event-loop stalls above this (default 300).
``GENIZAH_LOOP_LAG_INTERVAL``   monitor tick in seconds (default 1.0).
``GENIZAH_PERF_SUMMARY_SECONDS`` periodic summary interval (default 300; 0 = off).
"""

from __future__ import annotations

import asyncio
import logging
import os
import time

logger = logging.getLogger(__name__)

_TRUTHY = {'1', 'true', 'yes', 'on'}


def _env_flag(name: str, default: bool = True) -> bool:
    raw = os.environ.get(name)
    if raw is None or raw == '':
        return default
    return raw.strip().lower() in _TRUTHY


def _env_float(name: str, default: float, minimum: float = 0.0) -> float:
    """Read a float env var, clamped to `minimum`; fall back on any bad value."""
    raw = os.environ.get(name)
    if raw is None or raw == '':
        return default
    try:
        value = float(raw)
    except (TypeError, ValueError):
        logger.warning("perf_watch: %s=%r is not a number; using %s", name, raw, default)
        return default
    return max(minimum, value)


def perf_watch_enabled() -> bool:
    """True unless explicitly disabled. Read live so it can be flipped per restart."""
    return _env_flag('GENIZAH_PERF_WATCH', True)


def slow_request_threshold_ms() -> float:
    return _env_float('GENIZAH_SLOW_REQUEST_MS', 1500.0, minimum=1.0)


def loop_lag_threshold_ms() -> float:
    return _env_float('GENIZAH_LOOP_LAG_MS', 300.0, minimum=1.0)


def loop_lag_interval_seconds() -> float:
    # Floor of 0.1s: a tighter tick would itself become measurable overhead.
    return _env_float('GENIZAH_LOOP_LAG_INTERVAL', 1.0, minimum=0.1)


def summary_interval_seconds() -> float:
    return _env_float('GENIZAH_PERF_SUMMARY_SECONDS', 300.0, minimum=0.0)


# ---------------------------------------------------------------------------
# Rolling counters (process-local; cheap, no locking needed on one event loop)
# ---------------------------------------------------------------------------

class _Stats:
    def __init__(self) -> None:
        self.requests = 0
        self.slow_requests = 0
        self.max_request_ms = 0.0
        self.max_request_path = ''
        self.lag_breaches = 0
        self.max_lag_ms = 0.0

    def reset(self) -> None:
        self.__init__()  # noqa: PLC2801 - deliberate full reset


_stats = _Stats()


def get_stats_snapshot() -> dict:
    """Current counters. Exposed for tests and ad-hoc inspection."""
    return {
        'requests': _stats.requests,
        'slow_requests': _stats.slow_requests,
        'max_request_ms': round(_stats.max_request_ms, 1),
        'max_request_path': _stats.max_request_path,
        'lag_breaches': _stats.lag_breaches,
        'max_lag_ms': round(_stats.max_lag_ms, 1),
    }


def reset_stats() -> None:
    _stats.reset()


# ---------------------------------------------------------------------------
# 1. Per-request timing
# ---------------------------------------------------------------------------

class SlowRequestTimingMiddleware:
    """Time every HTTP request; log the ones over the threshold.

    Placed as an ASGI middleware rather than a NiceGUI/FastAPI route decorator so
    it also covers static-file and mounted sub-app routes, which is exactly where
    the unexplained latency showed up.
    """

    def __init__(self, asgi_app):
        self.asgi_app = asgi_app

    async def __call__(self, scope, receive, send):
        if scope.get('type') != 'http' or not perf_watch_enabled():
            await self.asgi_app(scope, receive, send)
            return

        started = time.perf_counter()
        status_code: int | None = None
        finished = False

        async def send_wrapper(message):
            nonlocal status_code, finished
            msg_type = message.get('type')
            if msg_type == 'http.response.start':
                status_code = message.get('status')
            elif msg_type == 'http.response.body' and not message.get('more_body', False):
                finished = True
            await send(message)

        try:
            await self.asgi_app(scope, receive, send_wrapper)
        finally:
            # `finally` so a raising or client-aborted request is still recorded;
            # instrumentation must never change request outcomes.
            try:
                elapsed_ms = (time.perf_counter() - started) * 1000.0
                self._record(scope, elapsed_ms, status_code, finished)
            except Exception:  # pragma: no cover - defensive
                pass

    @staticmethod
    def _record(scope, elapsed_ms: float, status_code, finished: bool) -> None:
        _stats.requests += 1
        path = scope.get('path') or '?'
        if elapsed_ms > _stats.max_request_ms:
            _stats.max_request_ms = elapsed_ms
            _stats.max_request_path = path
        if elapsed_ms < slow_request_threshold_ms():
            return
        _stats.slow_requests += 1
        logger.warning(
            "[perf] slow request %.0f ms  %s %s  status=%s%s  (loop-lag breaches so far: %d, "
            "max %.0f ms) -- a slow CHEAP path usually means the event loop was blocked "
            "elsewhere, not that this route is expensive",
            elapsed_ms,
            scope.get('method', '?'),
            path,
            status_code if status_code is not None else '-',
            '' if finished else ' (incomplete)',
            _stats.lag_breaches,
            _stats.max_lag_ms,
        )


# ---------------------------------------------------------------------------
# 2. Event-loop lag monitor
# ---------------------------------------------------------------------------

async def _loop_lag_monitor() -> None:
    """Measure how late our own timer wakes up; that overshoot IS the stall."""
    interval = loop_lag_interval_seconds()
    loop = asyncio.get_running_loop()
    last_summary = loop.time()
    logger.info(
        "[perf] event-loop lag monitor started (tick %.1fs, warn above %.0f ms)",
        interval, loop_lag_threshold_ms(),
    )
    while True:
        before = loop.time()
        await asyncio.sleep(interval)
        lag_ms = (loop.time() - before - interval) * 1000.0

        if lag_ms > _stats.max_lag_ms:
            _stats.max_lag_ms = lag_ms
        if lag_ms >= loop_lag_threshold_ms():
            _stats.lag_breaches += 1
            logger.warning(
                "[perf] event loop BLOCKED for %.0f ms -- every concurrent request "
                "(including static files) waited this long. Single uvicorn worker: "
                "look for synchronous Supabase/NLI I/O on the loop.",
                lag_ms,
            )

        summary_every = summary_interval_seconds()
        if summary_every and (loop.time() - last_summary) >= summary_every:
            last_summary = loop.time()
            if _stats.lag_breaches or _stats.slow_requests:
                logger.info("[perf] summary: %s", get_stats_snapshot())


def start_event_loop_lag_monitor() -> asyncio.Task | None:
    """Launch the monitor as a background task. Returns None when disabled.

    Never raises: a diagnostic must not be able to prevent startup.
    """
    if not perf_watch_enabled():
        logger.info("[perf] perf watch disabled via GENIZAH_PERF_WATCH")
        return None
    try:
        return asyncio.create_task(_loop_lag_monitor(), name='perf-loop-lag-monitor')
    except Exception as e:  # pragma: no cover - defensive
        logger.warning("[perf] could not start event-loop lag monitor: %s", e)
        return None
