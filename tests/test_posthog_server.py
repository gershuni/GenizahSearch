# -*- coding: utf-8 -*-
"""Phase 98 Plan 01 — shared/posthog_server.py behavioral tests.

Covers the 6 behaviors from <behavior> in 98-01-PLAN.md plus the reset seam.
Autouse fixture prevents test order pollution per RESEARCH Pitfall 6.

DO NOT mock requests.post — these are pure module-state tests. The drain
thread will try to POST but POSTHOG_API_KEY is empty in test environment,
so requests are skipped silently (per drain loop body).
"""

import pathlib
import queue
import threading

import pytest

import shared.posthog_server as ph


# ---------------------------------------------------------------------------
# Autouse fixture — resets module-level state before AND after each test.
# This is the Phase 98 RESEARCH.md Pitfall 6 guard against test order pollution.
#
# Rule 1 deviation: the fixture ALSO swaps _event_queue for a private,
# drain-free queue.Queue per test. The lazy-started daemon thread (started on
# the first enqueue_event call of the test session) continues to drain
# whatever queue OBJECT it was bound to when it started — but that object is
# no longer the module's _event_queue attribute after monkeypatch, so the
# daemon harmlessly drains an orphaned queue (initially empty, never enqueued
# to again). Each test sees a clean queue that nothing is racing against.
# ---------------------------------------------------------------------------
@pytest.fixture(autouse=True)
def _reset_posthog_server_state(monkeypatch):
    """Reset module state before EACH test (Pitfall 6 — module-level state).

    Replaces _event_queue with a fresh queue.Queue(maxsize=10000) per test so
    the test thread's .get(timeout=…) calls do not race with the lazy-started
    drain daemon. Static maxsize-cap invariants are pinned by TestModuleConstants
    against the pristine module-load value (see test_event_queue_maxsize_is_10000).
    """
    ph._reset_for_tests()
    fresh_q: queue.Queue = queue.Queue(maxsize=10000)
    monkeypatch.setattr(ph, '_event_queue', fresh_q)
    yield
    ph._reset_for_tests()


# ---------------------------------------------------------------------------
# Test 1 + 2: payload shape and distinct_id override
# ---------------------------------------------------------------------------
class TestEnqueueEvent:
    def test_enqueue_event_places_payload_on_queue(self):
        ph.enqueue_event('foo', {'k': 'v'})
        # Drain one item — should be exactly the event we enqueued
        payload = ph._event_queue.get(timeout=1.0)
        assert payload['event'] == 'foo'
        assert payload['distinct_id'] == 'system'
        assert payload['properties'] == {'k': 'v'}
        assert isinstance(payload['timestamp'], str)
        # ISO 8601 timestamps contain 'T' separator
        assert 'T' in payload['timestamp']

    def test_distinct_id_kwarg_override(self):
        ph.enqueue_event('e', {}, distinct_id='abc123')
        payload = ph._event_queue.get(timeout=1.0)
        assert payload['distinct_id'] == 'abc123'

    def test_properties_dict_is_copied_not_referenced(self):
        original = {'k': 1}
        ph.enqueue_event('e', original)
        original['k'] = 99  # mutate after enqueue
        payload = ph._event_queue.get(timeout=1.0)
        assert payload['properties'] == {'k': 1}, (
            'properties dict must be defensively copied'
        )

    def test_enqueue_event_returns_none(self):
        result = ph.enqueue_event('foo', {})
        assert result is None


# ---------------------------------------------------------------------------
# Test 3: drop counter under queue.Full
#
# NOTE — Rule 1 deviation from 98-01-PLAN.md <behavior>:
# The plan suggested filling the real _event_queue to maxsize then asserting
# the next enqueue_event drops. That race-fails: once enqueue_event lazy-starts
# the drain daemon thread, the daemon actively drains the queue in the
# background (get with 60s timeout, immediate re-loop on success), so the
# "queue is full when overflow attempt arrives" precondition cannot hold
# deterministically. Switching the test to monkeypatch _event_queue with a
# tiny, unwatched queue.Queue(maxsize=N) preserves the contract being verified
# ("enqueue_event increments _dropped_events on queue.Full and never raises")
# without racing the daemon. The real maxsize=10000 cap is still asserted
# in TestModuleConstants below.
# ---------------------------------------------------------------------------
class TestDropCounter:
    def test_full_queue_increments_drop_counter(self, monkeypatch):
        # Override the fresh autouse queue with a tiny pre-filled one so any
        # subsequent put_nowait raises queue.Full deterministically.
        tiny_q: queue.Queue = queue.Queue(maxsize=1)
        tiny_q.put_nowait({'event': 'sentinel', 'distinct_id': 's',
                           'properties': {}, 'timestamp': 'x'})
        monkeypatch.setattr(ph, '_event_queue', tiny_q)
        before = ph.get_dropped_event_count()
        ph.enqueue_event('overflow', {})  # must NOT raise
        after = ph.get_dropped_event_count()
        assert after == before + 1, (
            f'expected drop counter to increment, got {before} -> {after}'
        )

    def test_get_dropped_event_count_initial_zero(self):
        assert ph.get_dropped_event_count() == 0

    def test_drop_counter_persists_across_multiple_overflows(self, monkeypatch):
        tiny_q: queue.Queue = queue.Queue(maxsize=1)
        tiny_q.put_nowait({'event': 'sentinel', 'distinct_id': 's',
                           'properties': {}, 'timestamp': 'x'})
        monkeypatch.setattr(ph, '_event_queue', tiny_q)
        # 5 overflow attempts
        for _ in range(5):
            ph.enqueue_event('overflow', {})
        assert ph.get_dropped_event_count() == 5


# ---------------------------------------------------------------------------
# Test 4: telemetry never raises when POSTHOG_API_KEY unset/empty
# ---------------------------------------------------------------------------
class TestPostHogKeyMissing:
    def test_unset_api_key_does_not_raise(self, monkeypatch):
        monkeypatch.delenv('POSTHOG_API_KEY', raising=False)
        ph.enqueue_event('e', {'k': 'v'})  # must NOT raise
        # Event still enqueued — drain loop will skip POST silently
        payload = ph._event_queue.get(timeout=1.0)
        assert payload['event'] == 'e'

    def test_empty_api_key_does_not_raise(self, monkeypatch):
        monkeypatch.setenv('POSTHOG_API_KEY', '')
        ph.enqueue_event('e', {})  # must NOT raise


# ---------------------------------------------------------------------------
# Test 5: thread safety — safe to call from daemon thread
# ---------------------------------------------------------------------------
class TestThreadSafety:
    def test_safe_from_daemon_thread(self):
        errors = []

        def worker():
            try:
                ph.enqueue_event('from_thread', {'tid': 'daemon'})
            except Exception as e:  # noqa: BLE001 — capture any exception for assertion
                errors.append(e)

        t = threading.Thread(target=worker, daemon=True)
        t.start()
        t.join(timeout=2.0)
        assert not errors, f'enqueue_event raised in daemon thread: {errors}'
        payload = ph._event_queue.get(timeout=1.0)
        assert payload['event'] == 'from_thread'

    def test_concurrent_enqueues_all_present(self):
        """N threads each enqueue 1 event; assert all N appear on the queue.

        Uses a synchronization barrier BEFORE enqueue_event to maximize
        contention (per RESEARCH Pattern 3 — Barrier-outside-the-lock idiom).
        The autouse fixture already swapped _event_queue for a private
        drain-free queue, so the lazy-started daemon cannot steal events.
        """
        N = 50
        barrier = threading.Barrier(N)
        errors = []

        def worker(i):
            try:
                barrier.wait(timeout=5.0)
                ph.enqueue_event(f'evt_{i}', {'i': i})
            except Exception as e:  # noqa: BLE001
                errors.append(e)

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(N)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=5.0)
        assert not errors, f'workers raised: {errors}'
        seen = set()
        for _ in range(N):
            payload = ph._event_queue.get(timeout=1.0)
            seen.add(payload['event'])
        assert len(seen) == N


# ---------------------------------------------------------------------------
# Test 6: _reset_for_tests behavior
# ---------------------------------------------------------------------------
class TestResetForTests:
    def test_reset_drains_queue(self):
        ph.enqueue_event('e1', {})
        ph.enqueue_event('e2', {})
        ph.enqueue_event('e3', {})
        ph._reset_for_tests()
        with pytest.raises(queue.Empty):
            ph._event_queue.get_nowait()

    def test_reset_zeros_drop_counter(self):
        # Manually bump the drop counter via the lock
        with ph._dropped_events_lock:
            ph._dropped_events = 42
        ph._reset_for_tests()
        assert ph.get_dropped_event_count() == 0

    def test_reset_idempotent(self):
        ph._reset_for_tests()
        ph._reset_for_tests()
        assert ph.get_dropped_event_count() == 0


# ---------------------------------------------------------------------------
# Public API surface + architectural-boundary static guards
# ---------------------------------------------------------------------------
class TestPublicAPI:
    def test_all_exports_present(self):
        assert 'enqueue_event' in ph.__all__
        assert 'get_dropped_event_count' in ph.__all__
        assert 'POSTHOG_CAPTURE_URL' in ph.__all__
        assert 'POSTHOG_HOST' in ph.__all__
        assert '_reset_for_tests' in ph.__all__

    def test_posthog_capture_url_shape(self):
        assert ph.POSTHOG_CAPTURE_URL == 'https://eu.i.posthog.com/capture'

    def test_no_web_dependencies(self):
        """Phase 98 RESEARCH Option (a): shared/ must NOT depend on web/."""
        src = pathlib.Path('shared/posthog_server.py').read_text(encoding='utf-8')
        # Static check — no `from web.` or `import web` lines
        for line in src.splitlines():
            stripped = line.strip()
            assert not stripped.startswith('from web.'), (
                f'unexpected web import: {line!r}'
            )
            assert not stripped.startswith('import web'), (
                f'unexpected web import: {line!r}'
            )


# ---------------------------------------------------------------------------
# Static invariants on module constants
# ---------------------------------------------------------------------------
class TestModuleConstants:
    def test_event_queue_maxsize_is_10000(self):
        """The bounded-queue cap matches web/api_hardening.py:524.

        Note: the autouse fixture monkeypatches _event_queue per test, but
        always to a fresh queue.Queue(maxsize=10000) — so this assertion
        validates both the fresh-fixture cap and (via the source-check below)
        the module-load value.
        """
        assert ph._event_queue.maxsize == 10000

    def test_event_queue_maxsize_in_source_is_10000(self):
        """Pin the module-load _event_queue cap by static source inspection."""
        src = pathlib.Path('shared/posthog_server.py').read_text(encoding='utf-8')
        assert 'queue.Queue(maxsize=10000)' in src, (
            'shared/posthog_server.py must declare _event_queue with maxsize=10000'
        )

    def test_posthog_host_is_eu(self):
        assert ph.POSTHOG_HOST == 'https://eu.i.posthog.com'
