# -*- coding: utf-8 -*-
"""Phase 111 Plan 01 — shared/posthog_server.py neutral-additions behavioral tests.

Covers the 5 new functions added in Phase 111:
  set_default_distinct_id, register_scrub_hook, set_capture_api_key,
  set_capture_host, _flush_before_exit, _drain_and_discard

Design contract:
- These additions are NEUTRAL: existing web and NLI-circuit-breaker telemetry
  and the _event_queue monkeypatches in test_posthog_server.py remain unaffected.
- set_capture_api_key does NOT mutate os.environ (D-04 / REVIEWS HIGH-1).
- _flush_before_exit enforces a TRULY bounded wall-time deadline (REVIEWS MEDIUM).
- _reset_for_tests clears all four new globals (REVIEWS HIGH-2).

DO NOT put a global consent gate into shared/posthog_server.py (D-04).
"""

import queue
import time

import pytest

import shared.posthog_server as ph


# ---------------------------------------------------------------------------
# Autouse fixture — verbatim copy from tests/test_posthog_server.py lines 33-46
# plus belt-and-suspenders resets for the Phase 111 new globals.
# ---------------------------------------------------------------------------
@pytest.fixture(autouse=True)
def _reset_posthog_server_state(monkeypatch):
    """Reset module state before EACH test (Pitfall 6 — module-level state).

    Verbatim copy of the fixture from test_posthog_server.py, extended with
    explicit resets for the four new Phase 111 globals so test order does not
    matter even before _reset_for_tests is extended in Task 2.
    """
    ph._reset_for_tests()
    fresh_q: queue.Queue = queue.Queue(maxsize=10000)
    monkeypatch.setattr(ph, '_event_queue', fresh_q)
    # Belt-and-suspenders: also explicitly reset new globals (cleared by
    # _reset_for_tests after Task 2, but harmless to call explicitly here too)
    ph.set_default_distinct_id(None)
    ph.register_scrub_hook(None)
    ph.set_capture_api_key(None)
    ph.set_capture_host(None)
    yield
    ph._reset_for_tests()
    ph.set_default_distinct_id(None)
    ph.register_scrub_hook(None)
    ph.set_capture_api_key(None)
    ph.set_capture_host(None)


# ---------------------------------------------------------------------------
# Helper: a requests.post recorder stub
# ---------------------------------------------------------------------------
class _PostRecorder:
    """Captures all requests.post calls; optionally sleeps to simulate latency."""

    def __init__(self, sleep_secs: float = 0.0):
        self.calls: list[tuple] = []  # (url, json_kwarg, timeout_kwarg)
        self._sleep = sleep_secs

    def __call__(self, url, *, json=None, timeout=None, **_kwargs):
        if self._sleep:
            time.sleep(self._sleep)
        self.calls.append((url, json, timeout))
        return _DummyResponse()


class _DummyResponse:
    status_code = 200


# ---------------------------------------------------------------------------
# Test group 1: set_default_distinct_id behavior
# ---------------------------------------------------------------------------
class TestDefaultDistinctId:
    def test_existing_callers_unaffected(self):
        """Explicit distinct_id='explicit' is preserved even after a default is set."""
        ph.set_default_distinct_id('install-uuid')
        ph.enqueue_event('e', {}, distinct_id='explicit')
        payload = ph._event_queue.get(timeout=1.0)
        assert payload['distinct_id'] == 'explicit', (
            'Explicit distinct_id must not be overridden by the default'
        )

    def test_default_distinct_id_resolves_system_sentinel(self):
        """Default is injected when caller passes the 'system' sentinel."""
        ph.set_default_distinct_id('install-uuid')
        ph.enqueue_event('e', {})  # distinct_id defaults to 'system'
        payload = ph._event_queue.get(timeout=1.0)
        assert payload['distinct_id'] == 'install-uuid', (
            "'system' sentinel must resolve to the registered default distinct_id"
        )

    def test_default_distinct_id_none_keeps_system(self):
        """With no default set (None), distinct_id stays 'system'."""
        ph.set_default_distinct_id(None)
        ph.enqueue_event('e', {})
        payload = ph._event_queue.get(timeout=1.0)
        assert payload['distinct_id'] == 'system', (
            "When no default is registered, 'system' must remain unchanged"
        )


# ---------------------------------------------------------------------------
# Test group 2: _drain_and_discard behavior
# ---------------------------------------------------------------------------
class TestDrainAndDiscard:
    def test_drain_and_discard_no_post(self, monkeypatch):
        """_drain_and_discard empties the queue without making any POST request."""
        recorder = _PostRecorder()
        monkeypatch.setattr('shared.posthog_server.requests.post', recorder)

        ph.enqueue_event('e1', {})
        ph.enqueue_event('e2', {})
        ph.enqueue_event('e3', {})

        ph._drain_and_discard()

        with pytest.raises(queue.Empty):
            ph._event_queue.get_nowait()
        assert len(recorder.calls) == 0, (
            '_drain_and_discard must never call requests.post'
        )

    def test_drain_and_discard_preserves_dropped_counter(self, monkeypatch):
        """Unlike _reset_for_tests, _drain_and_discard does NOT zero _dropped_events."""
        # Bump the drop counter manually
        with ph._dropped_events_lock:
            ph._dropped_events = 7

        ph.enqueue_event('e', {})
        ph._drain_and_discard()

        assert ph.get_dropped_event_count() == 7, (
            '_drain_and_discard must not reset the _dropped_events counter'
        )


# ---------------------------------------------------------------------------
# Test group 3: register_scrub_hook behavior
# ---------------------------------------------------------------------------
class TestScrubHook:
    def test_scrub_hook_called_before_put(self):
        """Scrub hook runs inside enqueue_event BEFORE the queue put."""
        marker = {'was_called': False}

        def hook(payload: dict) -> dict:
            marker['was_called'] = True
            payload['_hook_marker'] = True
            return payload

        ph.register_scrub_hook(hook)
        ph.enqueue_event('e', {'a': 1})

        assert marker['was_called'], 'Scrub hook was never called'
        item = ph._event_queue.get(timeout=1.0)
        assert item.get('_hook_marker') is True, (
            'Hook must run BEFORE queue put — payload in queue must have marker'
        )

    def test_scrub_hook_returns_none_drops_event(self):
        """Hook returning None causes the event to be silently dropped."""
        ph.register_scrub_hook(lambda p: None)
        ph.enqueue_event('e', {})
        with pytest.raises(queue.Empty):
            ph._event_queue.get_nowait()

    def test_scrub_hook_exception_drops_event(self):
        """Hook that raises must NOT propagate; event is dropped (fail-closed)."""
        def bad_hook(payload):
            raise ValueError('hook failure')

        ph.register_scrub_hook(bad_hook)
        ph.enqueue_event('e', {})  # must NOT raise
        with pytest.raises(queue.Empty):
            ph._event_queue.get_nowait()

    def test_register_scrub_hook_none_is_noop(self):
        """Registering None as hook leaves enqueue_event behavior unchanged."""
        ph.register_scrub_hook(None)
        ph.enqueue_event('e', {'k': 'v'})
        payload = ph._event_queue.get(timeout=1.0)
        assert payload['event'] == 'e'
        assert payload['properties'] == {'k': 'v'}


# ---------------------------------------------------------------------------
# Test group 4: set_capture_api_key / set_capture_host transport behavior
# ---------------------------------------------------------------------------
class TestCaptureTransportConfig:
    def test_capture_api_key_override_used(self, monkeypatch):
        """Desktop key override reaches the transport even without env var."""
        monkeypatch.delenv('POSTHOG_API_KEY', raising=False)
        recorder = _PostRecorder()
        monkeypatch.setattr('shared.posthog_server.requests.post', recorder)

        ph.set_capture_api_key('phc_desktop')
        ph.enqueue_event('e', {})
        ph._flush_before_exit(timeout=0.5)

        assert len(recorder.calls) == 1, (
            'Exactly one POST should have been made for the queued event'
        )
        _url, json_kwarg, _timeout = recorder.calls[0]
        assert json_kwarg['api_key'] == 'phc_desktop', (
            'REVIEWS HIGH-1: desktop key override must reach the transport'
        )

    def test_web_path_uses_env_key_when_no_override(self, monkeypatch):
        """When no override is set, env POSTHOG_API_KEY is used (web behavior unchanged)."""
        monkeypatch.setenv('POSTHOG_API_KEY', 'phc_env')
        ph.set_capture_api_key(None)  # no override
        recorder = _PostRecorder()
        monkeypatch.setattr('shared.posthog_server.requests.post', recorder)

        ph.enqueue_event('e', {})
        ph._flush_before_exit(timeout=0.5)

        assert len(recorder.calls) == 1
        _url, json_kwarg, _timeout = recorder.calls[0]
        assert json_kwarg['api_key'] == 'phc_env', (
            'Web env key must be used when no override is set'
        )

    def test_capture_api_key_does_not_mutate_environ(self, monkeypatch):
        """set_capture_api_key must NEVER modify os.environ (D-04)."""
        import os
        # Ensure env is clean or has a different value
        monkeypatch.delenv('POSTHOG_API_KEY', raising=False)
        ph.set_capture_api_key('phc_desktop')
        assert os.environ.get('POSTHOG_API_KEY') is None, (
            'set_capture_api_key must NOT mutate os.environ'
        )

    def test_capture_host_override_used(self, monkeypatch):
        """Host override changes the POST URL target."""
        monkeypatch.setenv('POSTHOG_API_KEY', 'phc_any')
        recorder = _PostRecorder()
        monkeypatch.setattr('shared.posthog_server.requests.post', recorder)

        ph.set_capture_host('https://example.test')
        ph.enqueue_event('e', {})
        ph._flush_before_exit(timeout=0.5)

        assert len(recorder.calls) == 1
        url, _json, _timeout = recorder.calls[0]
        assert url.startswith('https://example.test'), (
            'Host override must change the POST URL'
        )

    def test_capture_host_fallback_to_posthog_host(self, monkeypatch):
        """When host override is None, the URL falls back to POSTHOG_HOST."""
        monkeypatch.setenv('POSTHOG_API_KEY', 'phc_any')
        recorder = _PostRecorder()
        monkeypatch.setattr('shared.posthog_server.requests.post', recorder)

        ph.set_capture_host(None)
        ph.enqueue_event('e', {})
        ph._flush_before_exit(timeout=0.5)

        assert len(recorder.calls) == 1
        url, _json, _timeout = recorder.calls[0]
        assert url.startswith(ph.POSTHOG_HOST), (
            'Without host override, URL must use POSTHOG_HOST'
        )


# ---------------------------------------------------------------------------
# Test group 5: _flush_before_exit behavior
# ---------------------------------------------------------------------------
class TestFlushBeforeExit:
    def test_flush_before_exit_drains(self, monkeypatch):
        """_flush_before_exit posts all queued events and leaves queue empty."""
        monkeypatch.setenv('POSTHOG_API_KEY', 'phc_test')
        recorder = _PostRecorder()
        monkeypatch.setattr('shared.posthog_server.requests.post', recorder)

        ph.enqueue_event('e1', {})
        ph.enqueue_event('e2', {})
        ph._flush_before_exit(timeout=0.5)

        with pytest.raises(queue.Empty):
            ph._event_queue.get_nowait()
        assert len(recorder.calls) == 2, (
            '_flush_before_exit must POST all queued events'
        )

    def test_flush_before_exit_no_key_no_post(self, monkeypatch):
        """Without api_key (env or override), flush drains but makes no POST."""
        monkeypatch.delenv('POSTHOG_API_KEY', raising=False)
        ph.set_capture_api_key(None)
        recorder = _PostRecorder()
        monkeypatch.setattr('shared.posthog_server.requests.post', recorder)

        ph.enqueue_event('e', {})
        ph._flush_before_exit(timeout=0.5)

        with pytest.raises(queue.Empty):
            ph._event_queue.get_nowait()
        assert len(recorder.calls) == 0, (
            'Without api_key, no POST should be made'
        )

    def test_flush_before_exit_total_wall_time_bounded(self, monkeypatch):
        """Total wall time is bounded near the deadline; per-POST timeout is deadline-aware.

        With 5 events and each POST sleeping 0.3s, the sum would be 1.5s without
        a deadline. With timeout=0.5s, total wall time must be well under 1.0s.
        Also asserts that the last attempted POST's timeout kwarg was <= remaining budget
        (REVIEWS MEDIUM — true deadline enforcement, not approximate).
        """
        monkeypatch.setenv('POSTHOG_API_KEY', 'phc_test')
        slow_recorder = _PostRecorder(sleep_secs=0.3)
        monkeypatch.setattr('shared.posthog_server.requests.post', slow_recorder)

        for _ in range(5):
            ph.enqueue_event('e', {})

        t0 = time.monotonic()
        ph._flush_before_exit(timeout=0.5)
        elapsed = time.monotonic() - t0

        # Total wall time must be bounded near the 0.5s deadline
        assert elapsed < 1.0, (
            f'_flush_before_exit wall time {elapsed:.3f}s exceeded 1.0s bound '
            f'(5×0.3s = 1.5s without real deadline enforcement)'
        )

        # At least one POST was attempted (we must have started before timeout)
        assert len(slow_recorder.calls) >= 1, (
            'At least one POST should have been attempted within 0.5s'
        )

        # The timeout kwarg on each attempted POST must be <= 0.5s (the deadline)
        # This verifies per-POST timeout = min(remaining, 2.0), not just 2.0
        for _url, _json, timeout_kwarg in slow_recorder.calls:
            assert timeout_kwarg is not None
            assert timeout_kwarg <= 0.5 + 0.05, (  # small float-math slack
                f'Per-POST timeout {timeout_kwarg} must be <= deadline 0.5s (REVIEWS MEDIUM)'
            )


# ---------------------------------------------------------------------------
# Test group 6: _reset_for_tests clears new globals (REVIEWS HIGH-2)
# ---------------------------------------------------------------------------
class TestResetForTestsClearsNewGlobals:
    def test_reset_for_tests_clears_new_globals(self):
        """_reset_for_tests must clear all four new Phase 111 module globals."""
        ph.set_default_distinct_id('x')
        ph.register_scrub_hook(lambda p: p)
        ph.set_capture_api_key('phc_test')
        ph.set_capture_host('https://h.example')

        ph._reset_for_tests()

        assert ph._default_distinct_id is None, (
            '_reset_for_tests must clear _default_distinct_id'
        )
        assert ph._scrub_hook is None, (
            '_reset_for_tests must clear _scrub_hook'
        )
        assert ph._api_key_override is None, (
            '_reset_for_tests must clear _api_key_override'
        )
        assert ph._host_override is None, (
            '_reset_for_tests must clear _host_override'
        )
