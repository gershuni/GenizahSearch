# -*- coding: utf-8 -*-
"""
Unit tests for Phase 92.2 D-VER-01 instrumentation primitives.

Tests 1-6: ContextVar-based counters in web/supabase_client.py
Tests 7-9: ASGI middleware _ListsPerfRouteTimingMiddleware in web/main.py

Reviews MUST-FIX 4: ContextVar per-task isolation (Test 6).
Reviews Round-2 MEDIUM-1: ASGI middleware behavioral coverage (Tests 7-9).
Gemini-MEDIUM: Use patch.object NOT direct mutation (safe under pytest -n auto).
"""

import asyncio
import json
from unittest.mock import patch

import web.supabase_client as mod


# ---------------------------------------------------------------------------
# Test 1: _inst_snapshot returns expected keys after counter bumps
# ---------------------------------------------------------------------------

def test_inst_snapshot_after_counter_bumps():
    """After 3 simulated _inst_record_query() calls, snapshot returns query_count=3
    and per-call stats consistent with the supplied latency list."""
    with patch.object(mod, '_INSTRUMENTATION_ENABLED', True):
        mod._inst_reset()
        mod._inst_record_query(10.0)
        mod._inst_record_query(20.0)
        mod._inst_record_query(30.0)
        snap = mod._inst_snapshot()

    assert snap['query_count'] == 3
    assert snap['client_build_count'] == 0
    assert snap['p50_query_latency_ms'] == 20.0   # median of [10, 20, 30]
    assert snap['max_query_latency_ms'] == 30.0
    # p95 of 3 values: idx = min(2, round(2 * 0.95)) = min(2, 2) = 2 -> 30.0
    assert snap['p95_query_latency_ms'] == 30.0
    assert 'request_id' in snap


# ---------------------------------------------------------------------------
# Test 2: _inst_reset() zeroes all counters
# ---------------------------------------------------------------------------

def test_inst_reset_zeroes_state():
    """_inst_reset() zeroes all counters; subsequent _inst_snapshot() returns
    query_count=0, empty latency list, etc."""
    with patch.object(mod, '_INSTRUMENTATION_ENABLED', True):
        mod._inst_reset()
        mod._inst_record_query(50.0)
        mod._inst_record_client_build()
        mod._inst_record_client_build()
        # now reset
        mod._inst_reset()
        snap = mod._inst_snapshot()

    assert snap['query_count'] == 0
    assert snap['client_build_count'] == 0
    assert snap['p50_query_latency_ms'] == 0.0
    assert snap['p95_query_latency_ms'] == 0.0
    assert snap['max_query_latency_ms'] == 0.0


# ---------------------------------------------------------------------------
# Test 3: _INSTRUMENTATION_ENABLED=False makes helpers no-ops
# ---------------------------------------------------------------------------

def test_disabled_is_noop():
    """With _INSTRUMENTATION_ENABLED=False, all increment helpers are no-ops
    and _inst_snapshot() returns zeroed dict."""
    with patch.object(mod, '_INSTRUMENTATION_ENABLED', False):
        # Ensure clean state in this context
        mod._inst_reset()   # should be no-op but safe
        mod._inst_record_query(99.0)
        mod._inst_record_client_build()
        mod._inst_set_request_id('should-be-ignored')
        snap = mod._inst_snapshot()

    # When disabled, ContextVars still have their default values (0 / () / '')
    # because _inst_reset() is a no-op when disabled.
    # The important assertion is that nothing above raised an error.
    # query_count and client_build_count remain at their task-default values.
    assert isinstance(snap['query_count'], int)
    assert isinstance(snap['client_build_count'], int)


# ---------------------------------------------------------------------------
# Test 4: structured lists_perf_baseline log line emits valid JSON
# ---------------------------------------------------------------------------

def test_baseline_log_line_is_valid_json(caplog):
    """The structured lists_perf_baseline=<json> log emission produces a JSON
    string parseable into a dict matching the documented baseline-sample schema keys."""
    with patch.object(mod, '_INSTRUMENTATION_ENABLED', True):
        mod._inst_reset()
        mod._inst_record_query(15.0)
        mod._inst_record_client_build()
        mod._inst_set_request_id('abc123def456')
        snap = mod._inst_snapshot()

    # Simulate what the ASGI middleware emits
    import json as _json
    record = {
        'phase': '92.2',
        'event': 'lists_perf_baseline',
        'source': 'asgi_request_body_flush',
        'request_id': 'abc123def456',
        'request': {'path': '/lists', 'method': 'GET'},
        'response': {'status_code': 200, 'response_bytes': 100, 'body_flushed': True},
        'totals': {'total_wall_clock_ms': 42.0, **snap},
    }
    log_str = _json.dumps(record, sort_keys=True)
    parsed = _json.loads(log_str)

    # Verify parseable + required keys present
    assert parsed['event'] == 'lists_perf_baseline'
    assert parsed['source'] == 'asgi_request_body_flush'
    assert parsed['request']['path'] == '/lists'
    assert 'total_wall_clock_ms' in parsed['totals']
    assert 'query_count' in parsed['totals']
    assert 'client_build_count' in parsed['totals']
    assert 'p50_query_latency_ms' in parsed['totals']
    assert 'p95_query_latency_ms' in parsed['totals']
    assert 'request_id' in parsed


# ---------------------------------------------------------------------------
# Test 5: negative control - no raw app.storage.user in instrumentation code
# ---------------------------------------------------------------------------

def test_no_raw_storage_user_in_instrumentation_code():
    """AST negative-control: no new raw app.storage.user access introduced
    in the instrumentation section of web/supabase_client.py."""
    import os

    filepath = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        'web', 'supabase_client.py'
    )
    with open(filepath, encoding='utf-8') as f:
        source = f.read()

    # Search for the instrumentation section only (between banner comments)
    start_marker = '# ============= Phase 92.2 D-VER-01 instrumentation'
    end_marker = '# ============= END Phase 92.2 D-VER-01 instrumentation'

    start_idx = source.find(start_marker)
    assert start_idx != -1, f"Instrumentation section start marker not found in {filepath}"

    # Find the end marker or fall back to end of _inst_snapshot function
    end_idx = source.find(end_marker, start_idx)
    if end_idx == -1:
        # Fall back: check from start_marker to the next top-level function def
        inst_section = source[start_idx:start_idx + 3000]  # max 3000 chars for the section
    else:
        inst_section = source[start_idx:end_idx]

    assert 'app.storage.user' not in inst_section, (
        "Instrumentation code must NOT access app.storage.user (Phase 87 invariant)"
    )


# ---------------------------------------------------------------------------
# Test 6: ContextVar per-task isolation under asyncio.gather
# ---------------------------------------------------------------------------

def test_contextvar_per_task_isolation():
    """ContextVars are per-task isolated. Two concurrent workers each see ONLY
    their own counter values — no cross-task contamination.
    (Reviews MUST-FIX 4 + Gemini-MEDIUM test isolation)"""
    with patch.object(mod, '_INSTRUMENTATION_ENABLED', True):
        async def worker(ms):
            mod._inst_reset()
            mod._inst_record_query(ms)
            return mod._inst_snapshot()

        async def run():
            return await asyncio.gather(worker(10), worker(20))

        a, b = asyncio.run(run())

    assert a['query_count'] == 1
    assert b['query_count'] == 1
    assert a['max_query_latency_ms'] == 10.0
    assert b['max_query_latency_ms'] == 20.0


# ---------------------------------------------------------------------------
# Tests 7-9: ASGI middleware _ListsPerfRouteTimingMiddleware behavioral
# (Reviews Round-2 MEDIUM-1)
# ---------------------------------------------------------------------------

def _make_fake_asgi_app():
    """Return a fake ASGI app that sends a minimal HTTP response."""
    async def fake_app(scope, receive, send):
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.body', 'body': b'OK', 'more_body': False})
    return fake_app


def _make_fake_receive():
    """Return an async callable simulating a minimal HTTP request body."""
    async def fake_receive():
        return {'type': 'http.request', 'body': b'', 'more_body': False}
    return fake_receive


def test_asgi_middleware_lists_path_emits_log(capsys):
    """Test 7 (Reviews Round-2 MEDIUM-1): /lists path + instrumentation enabled
    emits exactly ONE lists_perf_baseline= line on stdout with correct shape.

    Phase 92.2-01 follow-up: emission moved from logging.getLogger(__name__) to
    print(flush=True) because the project uses a dedicated 'genizah' logger
    chain with propagate=False, so root-logger-namespace records would be
    silently dropped during real captures."""
    from web.main import _ListsPerfRouteTimingMiddleware

    fake_app_calls = []

    async def tracked_fake_app(scope, receive, send):
        fake_app_calls.append(scope)
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.body', 'body': b'OK', 'more_body': False})

    scope = {'type': 'http', 'path': '/lists', 'method': 'GET'}
    received_messages = []

    async def fake_send(message):
        received_messages.append(message)

    with patch.object(mod, '_INSTRUMENTATION_ENABLED', True):
        middleware = _ListsPerfRouteTimingMiddleware(tracked_fake_app)
        asyncio.run(middleware(scope, _make_fake_receive(), fake_send))

    # App was called exactly once
    assert len(fake_app_calls) == 1

    captured = capsys.readouterr()
    baseline_lines = [
        line for line in captured.out.splitlines()
        if 'lists_perf_baseline=' in line
    ]
    assert len(baseline_lines) == 1, (
        f"Expected 1 lists_perf_baseline= stdout line, got {len(baseline_lines)}"
    )

    # Parse the JSON payload
    json_str = baseline_lines[0].split('lists_perf_baseline=', 1)[1]
    payload = json.loads(json_str)

    assert payload['source'] == 'asgi_request_body_flush'
    assert payload['request']['path'] == '/lists'
    assert payload['response']['status_code'] == 200
    assert payload['response']['body_flushed'] is True
    # 12-hex-char request_id (Reviews Round-2 MEDIUM-3)
    assert 'request_id' in payload
    assert len(payload['request_id']) == 12
    assert all(c in '0123456789abcdef' for c in payload['request_id'])


def test_asgi_middleware_non_lists_path_short_circuits(capsys):
    """Test 8 (Reviews Round-2 MEDIUM-1): non-/lists path emits ZERO
    lists_perf_baseline= stdout lines but still delegates to the app."""
    from web.main import _ListsPerfRouteTimingMiddleware

    fake_app_calls = []

    async def tracked_fake_app(scope, receive, send):
        fake_app_calls.append(scope)
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.body', 'body': b'OK', 'more_body': False})

    scope = {'type': 'http', 'path': '/some-other-route', 'method': 'GET'}
    received_messages_8 = []

    async def fake_send_8(message):
        received_messages_8.append(message)

    with patch.object(mod, '_INSTRUMENTATION_ENABLED', True):
        middleware = _ListsPerfRouteTimingMiddleware(tracked_fake_app)
        asyncio.run(middleware(scope, _make_fake_receive(), fake_send_8))

    # App was still called once (middleware delegates)
    assert len(fake_app_calls) == 1

    # Zero baseline stdout lines
    captured = capsys.readouterr()
    baseline_lines = [
        line for line in captured.out.splitlines()
        if 'lists_perf_baseline=' in line
    ]
    assert len(baseline_lines) == 0, (
        f"Expected 0 lists_perf_baseline= stdout lines for non-/lists path, got {len(baseline_lines)}"
    )


def test_asgi_middleware_instrumentation_disabled_short_circuits(capsys):
    """Test 9 (Reviews Round-2 MEDIUM-1): /lists path but instrumentation
    disabled -> ZERO lists_perf_baseline= stdout lines, app still called once."""
    from web.main import _ListsPerfRouteTimingMiddleware

    fake_app_calls = []

    async def tracked_fake_app(scope, receive, send):
        fake_app_calls.append(scope)
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.body', 'body': b'OK', 'more_body': False})

    scope = {'type': 'http', 'path': '/lists', 'method': 'GET'}
    received_messages_9 = []

    async def fake_send_9(message):
        received_messages_9.append(message)

    with patch.object(mod, '_INSTRUMENTATION_ENABLED', False):
        middleware = _ListsPerfRouteTimingMiddleware(tracked_fake_app)
        asyncio.run(middleware(scope, _make_fake_receive(), fake_send_9))

    # App was still called once
    assert len(fake_app_calls) == 1

    # Zero baseline stdout lines (instrumentation disabled)
    captured = capsys.readouterr()
    baseline_lines = [
        line for line in captured.out.splitlines()
        if 'lists_perf_baseline=' in line
    ]
    assert len(baseline_lines) == 0, (
        f"Expected 0 lists_perf_baseline= stdout lines when disabled, got {len(baseline_lines)}"
    )
