# -*- coding: utf-8 -*-
"""Phase 113 Plan 01 — CRASH-06 direct-send-bypasses-queue + lock-free assertions.

Covers send_crash_event_direct (D-06) and the lock-free snapshot globals (D-05 /
REVIEWS HIGH-1) added to shared/posthog_server.py.

No `qtbot` parameter is used anywhere in this file (repo is pytest-qt-FREE;
REVIEWS MEDIUM-6).
"""

import inspect
import queue

import pytest
import requests

import shared.posthog_server as ph


# ---------------------------------------------------------------------------
# Module-level autouse wrapper — opt-in to crash_telemetry_state fixture.
# Scoped to this file only (never project-wide).
# ---------------------------------------------------------------------------
@pytest.fixture(autouse=True)
def _use(crash_telemetry_state):
    yield


# ---------------------------------------------------------------------------
# Helper: a simple requests.post recorder
# ---------------------------------------------------------------------------
class _PostRecorder:
    def __init__(self):
        self.calls: list[dict] = []

    def __call__(self, url, *, json=None, timeout=None, **_kw):
        self.calls.append({'url': url, 'json': json, 'timeout': timeout})


# ---------------------------------------------------------------------------
# CRASH-06 — send_crash_event_direct bypasses the FIFO queue
# ---------------------------------------------------------------------------
def test_crash_send_bypasses_full_queue(monkeypatch):
    """CRASH-06 D-06: send_crash_event_direct POSTs even when _event_queue is full."""
    # Fill a tiny queue to capacity
    tiny_q: queue.Queue = queue.Queue(maxsize=5)
    for i in range(5):
        tiny_q.put_nowait({'event': f'dummy_{i}', 'distinct_id': 'x',
                            'properties': {}, 'timestamp': ''})
    monkeypatch.setattr(ph, '_event_queue', tiny_q)

    recorder = _PostRecorder()
    monkeypatch.setattr(requests, 'post', recorder)

    # Set key via setter (the production path that populates snapshot globals)
    ph.set_capture_api_key('test_key')
    ph.set_capture_host('https://example.posthog.com')

    ph.send_crash_event_direct('desktop_crash', {'exc_type': 'ValueError'}, 'did-123')

    assert len(recorder.calls) == 1, (
        'Exactly one POST must be made even with a full queue'
    )
    assert recorder.calls[0]['json']['event'] == 'desktop_crash'
    assert tiny_q.full(), 'Queue must be untouched (still full) after direct send'


def test_direct_send_does_not_touch_queue(monkeypatch):
    """CRASH-06: send_crash_event_direct never puts to / gets from _event_queue."""
    # Use a tiny_q and a size sentinel to verify queue length unchanged
    tiny_q: queue.Queue = queue.Queue(maxsize=3)
    for i in range(2):
        tiny_q.put_nowait({'event': f'e_{i}', 'distinct_id': 'x',
                            'properties': {}, 'timestamp': ''})
    initial_size = tiny_q.qsize()
    monkeypatch.setattr(ph, '_event_queue', tiny_q)

    monkeypatch.setattr(requests, 'post', lambda *a, **kw: None)
    ph.set_capture_api_key('test_key')
    ph.set_capture_host('https://example.posthog.com')

    ph.send_crash_event_direct('desktop_crash', {}, 'did-456')

    assert tiny_q.qsize() == initial_size, (
        '_event_queue size must be unchanged after send_crash_event_direct'
    )


# ---------------------------------------------------------------------------
# D-05 REVIEWS HIGH-1 — lock-free snapshot globals
# ---------------------------------------------------------------------------
def test_snapshot_globals_populated_by_setters():
    """D-05: set_capture_api_key/set_capture_host write _crash_*_snapshot globals."""
    ph.set_capture_api_key('snap_key')
    ph.set_capture_host('https://snap.host')

    assert ph._crash_api_key_snapshot == 'snap_key', (
        '_crash_api_key_snapshot must mirror key set via set_capture_api_key'
    )
    assert ph._crash_capture_url_snapshot == 'https://snap.host/capture', (
        '_crash_capture_url_snapshot must be host + /capture'
    )


def test_snapshot_globals_reset_to_empty_by_reset():
    """D-05: _reset_for_tests() clears _crash_*_snapshot globals back to ''."""
    ph.set_capture_api_key('snap_key')
    ph.set_capture_host('https://snap.host')
    ph._reset_for_tests()

    assert ph._crash_api_key_snapshot == '', (
        '_reset_for_tests must clear _crash_api_key_snapshot'
    )
    assert ph._crash_capture_url_snapshot == '', (
        '_reset_for_tests must clear _crash_capture_url_snapshot'
    )


def test_direct_send_no_key_no_post(monkeypatch):
    """D-05: send_crash_event_direct with no snapshot key makes zero POSTs."""
    recorder = _PostRecorder()
    monkeypatch.setattr(requests, 'post', recorder)

    # Do NOT set api key — snapshot stays ''
    ph.set_capture_host('https://example.posthog.com')

    ph.send_crash_event_direct('desktop_crash', {'exc_type': 'ValueError'}, 'did-789')

    assert len(recorder.calls) == 0, (
        'send_crash_event_direct must make zero POSTs when no api_key snapshot is set'
    )


def test_direct_send_payload_shape(monkeypatch):
    """CRASH-06: the POSTed JSON has event, distinct_id, properties, and ISO timestamp."""
    recorder = _PostRecorder()
    monkeypatch.setattr(requests, 'post', recorder)

    ph.set_capture_api_key('test_key')
    ph.set_capture_host('https://example.posthog.com')

    ph.send_crash_event_direct('desktop_crash', {'exc_type': 'RuntimeError'}, 'user-id-abc')

    assert len(recorder.calls) == 1
    payload = recorder.calls[0]['json']
    assert payload['event'] == 'desktop_crash'
    assert payload['distinct_id'] == 'user-id-abc'
    assert isinstance(payload['properties'], dict)
    assert payload['properties']['exc_type'] == 'RuntimeError'
    assert 'timestamp' in payload
    # Verify ISO timestamp format (basic check)
    ts = payload['timestamp']
    assert 'T' in ts and ('+' in ts or 'Z' in ts or ts.endswith('+00:00')), (
        f'timestamp {ts!r} does not look like an ISO 8601 UTC timestamp'
    )


def test_direct_send_never_raises_on_post_error(monkeypatch):
    """CRASH-06: send_crash_event_direct never raises even if requests.post raises."""
    def _raise(*a, **kw):
        raise OSError("network error")

    monkeypatch.setattr(requests, 'post', _raise)
    ph.set_capture_api_key('test_key')
    ph.set_capture_host('https://example.posthog.com')

    # Must NOT raise
    ph.send_crash_event_direct('desktop_crash', {'exc_type': 'ValueError'}, 'did-safe')


def test_direct_send_lock_free_static():
    """REVIEWS HIGH-1 static: send_crash_event_direct body contains no lock-taking symbols.

    Verifies that the CODE (non-docstring) of send_crash_event_direct does not
    call _resolve_api_key, _resolve_capture_url, reference _capture_config_lock,
    or touch _event_queue — all of which acquire locks or are the queue that must
    be bypassed (D-05 / REVIEWS HIGH-1).
    """
    import ast
    import textwrap

    full_src = inspect.getsource(ph.send_crash_event_direct)
    # Strip the docstring: parse the function, locate its body statements, skip
    # the first if it is an Expr(Constant) node (the docstring).
    tree = ast.parse(textwrap.dedent(full_src))
    func_def = tree.body[0]
    assert isinstance(func_def, ast.FunctionDef)
    body_stmts = func_def.body
    # Drop leading docstring node
    if body_stmts and isinstance(body_stmts[0], ast.Expr) and isinstance(
        getattr(body_stmts[0], 'value', None), ast.Constant
    ):
        body_stmts = body_stmts[1:]
    # Re-unparse only the body (code, no docstring)
    body_src = '\n'.join(ast.unparse(stmt) for stmt in body_stmts)

    assert '_resolve_api_key' not in body_src, (
        'send_crash_event_direct body must NOT call _resolve_api_key '
        '(acquires _capture_config_lock)'
    )
    assert '_resolve_capture_url' not in body_src, (
        'send_crash_event_direct body must NOT call _resolve_capture_url '
        '(acquires _capture_config_lock)'
    )
    assert '_capture_config_lock' not in body_src, (
        'send_crash_event_direct body must NOT reference _capture_config_lock'
    )
    assert '_event_queue' not in body_src, (
        'send_crash_event_direct body must NOT reference _event_queue'
    )
