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
``GENIZAH_NOT_SCHEDULED_MS``    above this, a tick that used almost no CPU is
                                reported as "not scheduled", not as a stall
                                (default 60000).
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from collections import deque

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


def not_scheduled_threshold_ms() -> float:
    """Above this, a tick that burned no CPU means the PROCESS stopped running.

    A laptop that sleeps, or a Windows console paused by a QuickEdit selection,
    suspends the whole process; the monitor then wakes up minutes or hours late
    and the naive reading is a stall of that length. One such tick was logged as
    ``event loop BLOCKED for 3069031 ms`` -- 51 minutes, which no handler did.
    Reporting that as a stall is not a cosmetic problem: it poisons the all-time
    maximum every other diagnostic line quotes.
    """
    return _env_float('GENIZAH_NOT_SCHEDULED_MS', 60_000.0, minimum=1000.0)


# ---------------------------------------------------------------------------
# Rolling counters (process-local; cheap, no locking needed on one event loop)
# ---------------------------------------------------------------------------

# How many recent stalls to keep for the "was the loop blocked DURING this
# request?" question. Bounded so the ring can never grow: only the last few
# seconds matter to an in-flight request, and 64 covers a minute of ticks.
_STALL_RING_MAX = 64


class _Stats:
    def __init__(self) -> None:
        self.requests = 0
        self.slow_requests = 0
        self.max_request_ms = 0.0
        self.max_request_path = ''
        self.lag_breaches = 0
        self.max_lag_ms = 0.0
        self.not_scheduled_events = 0
        self.max_not_scheduled_ms = 0.0
        # (window_start, window_end, lag_ms) in loop-clock seconds, one per
        # breach. Kept so a slow request can ask whether a stall actually
        # OVERLAPPED it rather than comparing itself to an all-time maximum.
        self.stalls: deque = deque(maxlen=_STALL_RING_MAX)
        # The tick currently in flight, in loop-clock seconds. A stall is only
        # appended to the ring once the monitor wakes up and measures it, and
        # the monitor cannot run while the loop is blocked -- so a request that
        # ENDS inside a stall would find an empty ring and wrongly conclude the
        # route was expensive. This deadline lets that case be read directly.
        self.tick_deadline = 0.0

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
        'not_scheduled_events': _stats.not_scheduled_events,
        'max_not_scheduled_ms': round(_stats.max_not_scheduled_ms, 1),
    }


def reset_stats() -> None:
    _stats.reset()


def max_lag_overlapping(start: float, end: float) -> float:
    """Worst stall whose window overlaps ``[start, end]`` on the loop clock.

    This is the number that can answer "was this request slow because the loop
    was blocked?". The all-time maximum cannot: on 2026-08-19 a single 4094 ms
    stall at startup made every later slow request advertise "loop lag is
    comparable to this request, so suspect the loop was blocked elsewhere",
    including seven consecutive /computed-identifications builds during which
    the breach counter never moved off 1 -- i.e. the loop was demonstrably NOT
    blocked and the route really was that expensive. The module already carries
    a comment saying a diagnostic that misattributes is worse than a quiet one;
    comparing against a monotonic all-time maximum is that same bug one step in.
    """
    worst = 0.0
    for window_start, window_end, lag_ms in _stats.stalls:
        overlap = min(window_end, end) - max(window_start, start)
        if overlap <= 0:
            continue
        # A 4 s stall cannot have cost a 1 s request more than 1 s. Take the
        # smaller of the stall and the intersection, so the reported number can
        # never exceed the request it is being offered as an explanation for.
        contribution = min(lag_ms, overlap * 1000.0)
        if contribution > worst:
            worst = contribution

    # Plus the stall still in progress, which by definition is not in the ring:
    # the monitor is itself stuck behind whatever is blocking the loop, so it
    # cannot have recorded it yet. If the tick is already past its deadline, the
    # loop has been unresponsive since that deadline.
    if _stats.tick_deadline > 0.0:
        overshoot = end - _stats.tick_deadline
        if overshoot * 1000.0 >= loop_lag_threshold_ms():
            # Only the part inside the request counts toward explaining it.
            in_flight_ms = min(overshoot, max(end - start, 0.0)) * 1000.0
            worst = max(worst, in_flight_ms)
    return worst


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
        # Loop-clock bounds too: the stall ring is timestamped on the loop clock,
        # and mixing clocks to test overlap would silently never match.
        loop = asyncio.get_running_loop()
        loop_started = loop.time()
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
                self._record(scope, elapsed_ms, status_code, finished,
                             loop_started, loop.time())
            except Exception:  # pragma: no cover - defensive
                pass

    @staticmethod
    def _record(scope, elapsed_ms: float, status_code, finished: bool,
                loop_started: float = 0.0, loop_ended: float = 0.0) -> None:
        _stats.requests += 1
        path = scope.get('path') or '?'
        if elapsed_ms > _stats.max_request_ms:
            _stats.max_request_ms = elapsed_ms
            _stats.max_request_path = path
        if elapsed_ms < slow_request_threshold_ms():
            return
        _stats.slow_requests += 1
        # Only offer the loop-blocked explanation when the numbers can actually
        # support it. The old text asserted it unconditionally, so a 12,849 ms
        # request was advertised as "the loop was blocked elsewhere" on the same
        # line that reported ONE 313 ms breach — three orders of magnitude short
        # of explaining it, and a real cause (sequential upstream fetches, each
        # with its own read timeout) went unnamed while the reader was pointed
        # at the loop. A diagnostic that misattributes is worse than a quiet one.
        #
        # That fix was half a fix: it still compared against the ALL-TIME maximum
        # lag, so one stall at startup re-armed the same misattribution for every
        # slow request afterwards. What decides the question is whether a stall
        # overlapped THIS request, so that is what is measured.
        stalled_ms = max_lag_overlapping(loop_started, loop_ended)
        if stalled_ms >= elapsed_ms * 0.5:
            hint = ("-- the loop was stalled %.0f ms DURING this request, enough to "
                    "explain it: suspect whatever blocked the loop, not this route"
                    % stalled_ms)
        elif stalled_ms > 0:
            hint = ("-- the loop stalled only %.0f ms during this request, so most of "
                    "the time is this route's own (suspect upstream I/O or an "
                    "expensive query)" % stalled_ms)
        else:
            hint = ("-- the loop was NOT stalled during this request, so this route "
                    "really did spend the time (suspect upstream I/O, its retries, "
                    "or an expensive query)")
        logger.warning(
            "[perf] slow request %.0f ms  %s %s  status=%s%s  (all-time: %d lag "
            "breaches, max %.0f ms) %s",
            elapsed_ms,
            scope.get('method', '?'),
            path,
            status_code if status_code is not None else '-',
            '' if finished else ' (incomplete)',
            _stats.lag_breaches,
            _stats.max_lag_ms,
            hint,
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
        cpu_before = time.process_time()
        _stats.tick_deadline = before + interval
        await asyncio.sleep(interval)
        after = loop.time()
        # Cleared immediately: past this point the tick is no longer in flight,
        # and leaving a stale deadline behind would make every later request
        # believe the loop is still stalled.
        _stats.tick_deadline = 0.0
        lag_ms = (after - before - interval) * 1000.0
        # CPU actually consumed by this PROCESS across the tick. This is the
        # discriminator the first version lacked: it separates the two causes
        # the warning used to name only one of, and it separates both from a
        # process that simply was not running.
        cpu_ms = (time.process_time() - cpu_before) * 1000.0
        span_ms = (after - before) * 1000.0
        on_cpu = (cpu_ms / span_ms) if span_ms > 0 else 0.0

        if lag_ms < loop_lag_threshold_ms():
            # Sub-threshold jitter still moves the maximum, as it always has --
            # only the suspension case below is kept out of it.
            if lag_ms > _stats.max_lag_ms:
                _stats.max_lag_ms = lag_ms
        elif lag_ms >= not_scheduled_threshold_ms() and on_cpu < 0.05:
            # Minutes late having burned no CPU: the process was suspended, not
            # busy. Counted separately so it never inflates max_lag_ms.
            _stats.not_scheduled_events += 1
            if lag_ms > _stats.max_not_scheduled_ms:
                _stats.max_not_scheduled_ms = lag_ms
            logger.warning(
                "[perf] monitor NOT SCHEDULED for %.0f s (%.0f ms of CPU used) -- the "
                "process stopped running rather than the loop being blocked: machine "
                "asleep, container throttled, or a Windows console paused by a "
                "QuickEdit selection. Not counted as a loop stall.",
                lag_ms / 1000.0, cpu_ms,
            )
        else:
            _stats.lag_breaches += 1
            if lag_ms > _stats.max_lag_ms:
                _stats.max_lag_ms = lag_ms
            _stats.stalls.append((before, after, lag_ms))
            if on_cpu >= 0.5:
                cause = ("the process was ON CPU for %.0f ms of that, so this is "
                         "CPU-bound Python holding the GIL -- a run.io_bound worker "
                         "counts, it is the same process" % cpu_ms)
            else:
                cause = ("the process used only %.0f ms of CPU, so this is BLOCKING "
                         "I/O on the loop -- look for a synchronous Supabase/NLI call "
                         "in an async handler" % cpu_ms)
            logger.warning(
                "[perf] event loop BLOCKED for %.0f ms -- every concurrent request "
                "(including static files) waited this long. Single uvicorn worker; %s.",
                lag_ms, cause,
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
