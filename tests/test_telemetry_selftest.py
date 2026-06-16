# -*- coding: utf-8 -*-
"""Phase 116 Plan 02 — synchronous self-test send path + CLI wiring tests (SC#3).

Covers the new Phase 116 self-test surface:
  - shared.posthog_server.send_selftest_event_sync() — synchronous, return-valued
    SSL/delivery probe (REVIEWS HIGH #1): ONE requests.post, returns
    'SSL_OK' (HTTP 2xx) / 'SSL_FAIL ...' (any exception or non-2xx) / 'NO_KEY'
    (no phc_ key — returns WITHOUT any network call).
  - genizah_app.py __main__ `--telemetry-selftest` / `--telemetry-selftest-offline`
    headless block (static assertions): precedes QApplication, toggles consent
    IN-MEMORY only (never set_consent), and drives the SSL_OK/exit decision off the
    synchronous helper — NOT get_dropped_event_count (which counts queue.Full only).

Design contract:
- The helper NEVER raises, NEVER touches _event_queue / _dropped_events / the daemon.
- The drop counter is NOT a delivery signal; SSL_OK is a real HTTP-2xx confirmation.
- This file is part of the v8.1.0 milestone-exit regression gate (116-VERIFICATION.md).
"""

import queue

import pytest
import requests

import shared.posthog_server as ph


# ---------------------------------------------------------------------------
# Autouse fixture — mirrors tests/test_telemetry_posthog_server_ext.py
# ---------------------------------------------------------------------------
@pytest.fixture(autouse=True)
def _reset_posthog_server_state(monkeypatch):
    """Reset module state before EACH test (module-level state, Pitfall 6)."""
    ph._reset_for_tests()
    fresh_q: queue.Queue = queue.Queue(maxsize=10000)
    monkeypatch.setattr(ph, '_event_queue', fresh_q)
    ph.set_capture_api_key(None)
    ph.set_capture_host(None)
    # No ambient ingest key from the environment — these tests own key resolution.
    monkeypatch.delenv('POSTHOG_API_KEY', raising=False)
    yield
    ph._reset_for_tests()
    ph.set_capture_api_key(None)
    ph.set_capture_host(None)


# ---------------------------------------------------------------------------
# Helper: a requests.post recorder stub returning a configurable status code
# ---------------------------------------------------------------------------
class _PostRecorder:
    """Captures all requests.post calls and returns a stub response."""

    def __init__(self, status_code: int = 200):
        self.calls: list[tuple] = []  # (url, json_kwarg, timeout_kwarg)
        self._status_code = status_code

    def __call__(self, url, *, json=None, timeout=None, **_kwargs):
        self.calls.append((url, json, timeout))
        return _DummyResponse(self._status_code)


class _DummyResponse:
    def __init__(self, status_code: int = 200):
        self.status_code = status_code


def _explode(*_args, **_kwargs):
    raise AssertionError('requests.post must NOT be called in this path')


# ---------------------------------------------------------------------------
# send_selftest_event_sync — behavioral contract
# ---------------------------------------------------------------------------
class TestSendSelftestEventSync:
    def test_exported_and_documented(self):
        assert hasattr(ph, 'send_selftest_event_sync')
        assert 'send_selftest_event_sync' in ph.__all__
        assert ph.send_selftest_event_sync.__doc__

    def test_no_key_returns_no_key_without_network(self, monkeypatch):
        """No phc_ key configured -> 'NO_KEY', and NO requests.post is made."""
        monkeypatch.setattr('shared.posthog_server.requests.post', _explode)
        assert ph.send_selftest_event_sync() == 'NO_KEY'

    def test_2xx_returns_ssl_ok(self, monkeypatch):
        ph.set_capture_api_key('phc_x')
        recorder = _PostRecorder(status_code=200)
        monkeypatch.setattr('shared.posthog_server.requests.post', recorder)
        assert ph.send_selftest_event_sync() == 'SSL_OK'
        assert len(recorder.calls) == 1  # exactly ONE POST

    def test_sslerror_returns_ssl_fail(self, monkeypatch):
        ph.set_capture_api_key('phc_x')

        def _raise_ssl(*_a, **_k):
            raise requests.exceptions.SSLError('handshake failed')

        monkeypatch.setattr('shared.posthog_server.requests.post', _raise_ssl)
        result = ph.send_selftest_event_sync()
        assert result.startswith('SSL_FAIL')

    def test_non_2xx_returns_ssl_fail_with_code(self, monkeypatch):
        ph.set_capture_api_key('phc_x')
        recorder = _PostRecorder(status_code=400)
        monkeypatch.setattr('shared.posthog_server.requests.post', recorder)
        result = ph.send_selftest_event_sync()
        assert result.startswith('SSL_FAIL')
        assert '400' in result

    def test_connection_error_returns_ssl_fail_never_raises(self, monkeypatch):
        ph.set_capture_api_key('phc_x')

        def _raise_conn(*_a, **_k):
            raise requests.exceptions.ConnectionError('network down')

        monkeypatch.setattr('shared.posthog_server.requests.post', _raise_conn)
        # MUST NOT raise — returns the failure token instead.
        result = ph.send_selftest_event_sync()
        assert result.startswith('SSL_FAIL')

    def test_never_touches_queue_or_drop_counter(self, monkeypatch):
        ph.set_capture_api_key('phc_x')
        recorder = _PostRecorder(status_code=200)
        monkeypatch.setattr('shared.posthog_server.requests.post', recorder)
        before = ph.get_dropped_event_count()
        ph.send_selftest_event_sync()
        assert ph._event_queue.empty(), 'self-test must not enqueue any event'
        assert ph.get_dropped_event_count() == before, 'self-test must not touch the drop counter'

    def test_honors_set_capture_api_key(self, monkeypatch):
        """A key wired via set_capture_api_key (desktop _wire_transport_config) is used."""
        recorder = _PostRecorder(status_code=200)
        monkeypatch.setattr('shared.posthog_server.requests.post', recorder)
        # Without a key -> NO_KEY (no call).
        monkeypatch.setattr('shared.posthog_server.requests.post', _explode)
        assert ph.send_selftest_event_sync() == 'NO_KEY'
        # After wiring a key -> a real POST happens.
        ph.set_capture_api_key('phc_wired')
        monkeypatch.setattr('shared.posthog_server.requests.post', recorder)
        assert ph.send_selftest_event_sync() == 'SSL_OK'
        assert recorder.calls and recorder.calls[0][1]['api_key'] == 'phc_wired'


# ---------------------------------------------------------------------------
# genizah_app.py __main__ block — static wiring assertions (no Qt event loop)
# ---------------------------------------------------------------------------
class TestTelemetrySelftestCliBlock:
    @pytest.fixture(scope='class')
    def src(self):
        with open('genizah_app.py', encoding='utf-8') as fh:
            return fh.read()

    def test_block_present_with_all_four_tokens(self, src):
        assert '--telemetry-selftest' in src
        for token in ('SSL_OK', 'SSL_FAIL', 'NO_KEY', 'OFFLINE_OK'):
            assert token in src, token

    def test_block_precedes_qapplication(self, src):
        guard = src.index('if "--telemetry-selftest" in sys.argv')
        qapp = src.index('app = QApplication(sys.argv)')  # the __main__ construction
        assert guard < qapp, 'self-test block must run before QApplication construction'

    def test_block_uses_in_memory_consent_toggle_not_set_consent(self, src):
        guard = src.index('if "--telemetry-selftest" in sys.argv')
        block = src[guard:guard + 1600]
        assert '_enabled_lock' in block, 'must toggle consent in-memory under _enabled_lock'
        assert 'set_consent(' not in block, 'must NOT persist consent to config.pkl'

    def test_signal_driven_by_sync_helper_not_drop_counter(self, src):
        guard = src.index('if "--telemetry-selftest" in sys.argv')
        block = src[guard:guard + 1600]
        assert 'send_selftest_event_sync' in block
        assert 'get_dropped_event_count' not in block, (
            'SSL_OK / exit-0 must be driven by the synchronous helper, '
            'NOT the queue-saturation drop counter (REVIEWS HIGH #1)'
        )
