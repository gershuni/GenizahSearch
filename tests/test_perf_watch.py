# -*- coding: utf-8 -*-
"""Tests for web/perf_watch.py -- the always-on slow-request + event-loop-lag watch.

The point of these signals is diagnostic honesty: before them, a 9-second
response left NO server-side trace (nginx logs the default `combined` format with
no $request_time, and the only in-app timing was /lists-scoped and flag-gated).

The behaviours that matter and are pinned here:
  * instrumentation must never alter a request's outcome, including when the app
    raises (the timing lives in a `finally`);
  * it must stay silent below threshold and speak above it;
  * `GENIZAH_PERF_WATCH=0` must fully disable it;
  * a malformed env value must fall back to the default, never crash startup.
"""

import asyncio
import logging

import pytest

from web import perf_watch


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    for var in (
        'GENIZAH_PERF_WATCH',
        'GENIZAH_SLOW_REQUEST_MS',
        'GENIZAH_LOOP_LAG_MS',
        'GENIZAH_LOOP_LAG_INTERVAL',
        'GENIZAH_PERF_SUMMARY_SECONDS',
    ):
        monkeypatch.delenv(var, raising=False)
    perf_watch.reset_stats()
    yield
    perf_watch.reset_stats()


# ---------------------------------------------------------------------------
# Env handling
# ---------------------------------------------------------------------------

def test_enabled_by_default():
    assert perf_watch.perf_watch_enabled() is True


@pytest.mark.parametrize('value,expected', [
    ('0', False), ('false', False), ('no', False), ('off', False), ('', True),
    ('1', True), ('true', True), ('YES', True), ('On', True),
])
def test_flag_parsing(monkeypatch, value, expected):
    monkeypatch.setenv('GENIZAH_PERF_WATCH', value)
    assert perf_watch.perf_watch_enabled() is expected


def test_bad_numeric_env_falls_back_to_default(monkeypatch):
    """A typo in an env var must not take the app down."""
    monkeypatch.setenv('GENIZAH_SLOW_REQUEST_MS', 'not-a-number')
    assert perf_watch.slow_request_threshold_ms() == 1500.0


def test_numeric_env_is_clamped(monkeypatch):
    monkeypatch.setenv('GENIZAH_LOOP_LAG_INTERVAL', '0.001')
    assert perf_watch.loop_lag_interval_seconds() == 0.1


def test_numeric_env_override(monkeypatch):
    monkeypatch.setenv('GENIZAH_SLOW_REQUEST_MS', '250')
    assert perf_watch.slow_request_threshold_ms() == 250.0


# ---------------------------------------------------------------------------
# Middleware
# ---------------------------------------------------------------------------

def _http_scope(path='/search', method='GET'):
    return {'type': 'http', 'path': path, 'method': method}


async def _ok_app(scope, receive, send):
    await send({'type': 'http.response.start', 'status': 200})
    await send({'type': 'http.response.body', 'body': b'hi', 'more_body': False})


def test_middleware_passes_response_through():
    sent = []

    async def send(message):
        sent.append(message)

    mw = perf_watch.SlowRequestTimingMiddleware(_ok_app)
    asyncio.run(mw(_http_scope(), None, send))

    assert [m['type'] for m in sent] == ['http.response.start', 'http.response.body']
    assert perf_watch.get_stats_snapshot()['requests'] == 1


def test_middleware_ignores_non_http_scopes():
    """WebSocket traffic must not be timed as an HTTP request."""
    seen = []

    async def ws_app(scope, receive, send):
        seen.append(scope['type'])

    mw = perf_watch.SlowRequestTimingMiddleware(ws_app)
    asyncio.run(mw({'type': 'websocket', 'path': '/_nicegui_ws/socket.io/'}, None, None))

    assert seen == ['websocket']
    assert perf_watch.get_stats_snapshot()['requests'] == 0


def test_middleware_disabled_does_not_count(monkeypatch):
    monkeypatch.setenv('GENIZAH_PERF_WATCH', '0')
    mw = perf_watch.SlowRequestTimingMiddleware(_ok_app)
    asyncio.run(mw(_http_scope(), None, lambda m: asyncio.sleep(0)))
    assert perf_watch.get_stats_snapshot()['requests'] == 0


def test_slow_request_is_logged(monkeypatch, caplog):
    monkeypatch.setenv('GENIZAH_SLOW_REQUEST_MS', '1')

    async def send(message):
        pass

    async def slow_app(scope, receive, send_):
        # Comfortably over the 1 ms threshold so the assertion is deterministic;
        # a trivially fast handler can finish inside it and legitimately not log.
        await asyncio.sleep(0.02)
        await _ok_app(scope, None, send_)

    mw = perf_watch.SlowRequestTimingMiddleware(slow_app)
    with caplog.at_level(logging.WARNING, logger='web.perf_watch'):
        asyncio.run(mw(_http_scope(path='/browse'), None, send))

    # getMessage() applies the lazy %-args; `record.message` is the raw template.
    assert any('slow request' in r.getMessage() and '/browse' in r.getMessage()
               for r in caplog.records), caplog.text
    assert perf_watch.get_stats_snapshot()['slow_requests'] == 1


def test_fast_request_is_silent(monkeypatch, caplog):
    monkeypatch.setenv('GENIZAH_SLOW_REQUEST_MS', '60000')

    async def send(message):
        pass

    mw = perf_watch.SlowRequestTimingMiddleware(_ok_app)
    with caplog.at_level(logging.WARNING, logger='web.perf_watch'):
        asyncio.run(mw(_http_scope(), None, send))

    assert caplog.records == []
    assert perf_watch.get_stats_snapshot()['slow_requests'] == 0


def test_exception_propagates_and_is_still_recorded():
    """Instrumentation must not swallow application errors."""
    async def boom_app(scope, receive, send):
        raise RuntimeError('handler exploded')

    mw = perf_watch.SlowRequestTimingMiddleware(boom_app)
    with pytest.raises(RuntimeError, match='handler exploded'):
        asyncio.run(mw(_http_scope(), None, lambda m: asyncio.sleep(0)))

    # Timing lives in a `finally`, so the request is still counted.
    assert perf_watch.get_stats_snapshot()['requests'] == 1


def test_max_request_path_tracked(monkeypatch):
    monkeypatch.setenv('GENIZAH_SLOW_REQUEST_MS', '60000')

    async def send(message):
        pass

    async def slow_app(scope, receive, send_):
        await asyncio.sleep(0.02)
        await _ok_app(scope, None, send_)

    asyncio.run(perf_watch.SlowRequestTimingMiddleware(_ok_app)(_http_scope('/fast'), None, send))
    asyncio.run(perf_watch.SlowRequestTimingMiddleware(slow_app)(_http_scope('/slow'), None, send))

    snapshot = perf_watch.get_stats_snapshot()
    assert snapshot['requests'] == 2
    assert snapshot['max_request_path'] == '/slow'


# ---------------------------------------------------------------------------
# Loop lag monitor
# ---------------------------------------------------------------------------

def test_monitor_not_started_when_disabled(monkeypatch):
    monkeypatch.setenv('GENIZAH_PERF_WATCH', '0')

    async def main():
        return perf_watch.start_event_loop_lag_monitor()

    assert asyncio.run(main()) is None


def test_monitor_detects_a_real_block(monkeypatch, caplog):
    """Block the loop with time.sleep and assert the monitor reports it."""
    import time as real_time

    monkeypatch.setenv('GENIZAH_LOOP_LAG_INTERVAL', '0.1')
    monkeypatch.setenv('GENIZAH_LOOP_LAG_MS', '100')

    async def main():
        task = perf_watch.start_event_loop_lag_monitor()
        assert task is not None
        await asyncio.sleep(0.15)
        real_time.sleep(0.4)      # synchronous stall -- exactly the prod failure mode
        await asyncio.sleep(0.25)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    with caplog.at_level(logging.WARNING, logger='web.perf_watch'):
        asyncio.run(main())

    assert any('event loop BLOCKED' in r.getMessage() for r in caplog.records), caplog.text
    assert perf_watch.get_stats_snapshot()['lag_breaches'] >= 1


def test_monitor_quiet_when_loop_is_free(monkeypatch, caplog):
    monkeypatch.setenv('GENIZAH_LOOP_LAG_INTERVAL', '0.1')
    monkeypatch.setenv('GENIZAH_LOOP_LAG_MS', '5000')

    async def main():
        task = perf_watch.start_event_loop_lag_monitor()
        await asyncio.sleep(0.35)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    with caplog.at_level(logging.WARNING, logger='web.perf_watch'):
        asyncio.run(main())

    assert not any('BLOCKED' in r.getMessage() for r in caplog.records)
    assert perf_watch.get_stats_snapshot()['lag_breaches'] == 0
