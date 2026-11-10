"""Phase 98 Plan 02 — shared.nli_circuit_breaker behavioral tests.

Covers all 28 CONTEXT decisions D-01..D-28 via tests pinned to the
Per-Task Verification Map in 98-VALIDATION.md.

The Nyquist-critical invariant is
`TestNliCircuitBreakerConcurrency.test_20_threads_saturate_then_short_circuit`.
If this single test passes, the 2026-05-25 hang cannot recur.
"""

import ast
import pathlib
import threading
import time
from concurrent.futures import ThreadPoolExecutor, wait, ALL_COMPLETED
from typing import get_args
from unittest.mock import patch

import pytest

# Module under test
import shared.nli_circuit_breaker as br
import shared.posthog_server as ph


# ---------------------------------------------------------------------------
# Helpers — concurrency instrumentation borrowed from
# tests/test_nli_cache_persist_retry.py (the proven idiom).
# ---------------------------------------------------------------------------

class _ConcurrencyRecorder:
    """Track max concurrent entries into a critical region.

    enter() / exit() bracket the region; max_concurrent is the high-water mark.
    Used to PROVE a lock serializes concurrent threads.
    """
    def __init__(self):
        self._current = 0
        self.max_concurrent = 0
        self._lock = threading.Lock()

    def enter(self):
        with self._lock:
            self._current += 1
            if self._current > self.max_concurrent:
                self.max_concurrent = self._current

    def exit(self):
        with self._lock:
            self._current -= 1


# ---------------------------------------------------------------------------
# Test suites
# ---------------------------------------------------------------------------

class TestNliCircuitBreakerUnit:
    def test_fresh_module_is_closed(self):
        assert not br.is_open()
        snap = br._state_snapshot()
        assert snap['consecutive_failures'] == 0
        assert snap['open_until_monotonic'] == 0.0
        assert snap['is_open_now'] is False

    def test_three_consecutive_failures_trip_breaker(self):
        for _ in range(br.NLI_CIRCUIT_THRESHOLD):
            br.record_failure('timeout', 'test')
        assert br.is_open()
        snap = br._state_snapshot()
        assert snap['consecutive_failures'] >= br.NLI_CIRCUIT_THRESHOLD
        assert snap['open_until_monotonic'] > time.monotonic()

    def test_fewer_failures_than_threshold_do_not_trip(self):
        for _ in range(br.NLI_CIRCUIT_THRESHOLD - 1):
            br.record_failure('timeout', 'test')
        assert not br.is_open()

    def test_record_success_resets_counter(self):
        br.record_failure('timeout', 'a')
        br.record_failure('timeout', 'b')
        br.record_success('c')
        snap = br._state_snapshot()
        assert snap['consecutive_failures'] == 0
        assert snap['open_until_monotonic'] == 0.0

    def test_record_success_closes_open_breaker(self):
        for _ in range(br.NLI_CIRCUIT_THRESHOLD):
            br.record_failure('timeout', 'x')
        assert br.is_open()
        br.record_success('y')
        assert not br.is_open()

    def test_auto_recovery_when_window_elapses(self):
        """is_open() returns False after _open_until passes — D-04 + RESEARCH Open Q2."""
        for _ in range(br.NLI_CIRCUIT_THRESHOLD):
            br.record_failure('timeout', 'x')
        assert br.is_open()
        # Fast-forward monotonic clock past the window
        future = time.monotonic() + br.NLI_CIRCUIT_WINDOW + 1.0
        with patch('shared.nli_circuit_breaker.time.monotonic', return_value=future):
            assert not br.is_open()

    def test_auto_recovery_does_not_reset_counter(self):
        """RESEARCH Open Question 2 recommendation: counter persists across auto-recovery.

        Next failure after auto-recovery re-trips after just 1 increment.
        """
        for _ in range(br.NLI_CIRCUIT_THRESHOLD):
            br.record_failure('timeout', 'x')
        future = time.monotonic() + br.NLI_CIRCUIT_WINDOW + 1.0
        with patch('shared.nli_circuit_breaker.time.monotonic', return_value=future):
            assert not br.is_open()
        # Counter unchanged — RESEARCH Open Q2 recommendation
        snap = br._state_snapshot()
        assert snap['consecutive_failures'] == br.NLI_CIRCUIT_THRESHOLD


class TestFailureCounting:
    """D-06: timeout / connection_error / 5xx / 429 all trip the breaker."""

    @pytest.mark.parametrize('failure_type', ['timeout', 'connection_error', '5xx', '429'])
    def test_each_failure_type_increments(self, failure_type):
        for _ in range(br.NLI_CIRCUIT_THRESHOLD):
            br.record_failure(failure_type, 'test')
        assert br.is_open(), f'{failure_type} did not trip breaker'

    def test_mixed_failure_types_count_together(self):
        br.record_failure('timeout', 'a')
        br.record_failure('connection_error', 'b')
        br.record_failure('5xx', 'c')
        assert br.is_open(), 'mixed failures should accumulate into shared counter'

    def test_failure_type_literal_excludes_404(self):
        """D-07: 404 is NOT in the FailureType literal — callers MUST NOT pass it.

        Static type guard: the literal's __args__ contains only the 4 allowed values.
        """
        allowed = set(get_args(br.FailureType))
        assert allowed == {'timeout', 'connection_error', '5xx', '429'}
        assert '404' not in allowed
        assert 'empty' not in allowed


class TestNliBreakerTelemetry:
    """D-24, D-25, D-28: PostHog event emission on open / close."""

    def test_opened_event_emitted_on_threshold_cross(self, monkeypatch):
        events = []

        def fake_enqueue(event, properties, distinct_id='system'):
            events.append({
                'event': event,
                'properties': dict(properties),
                'distinct_id': distinct_id,
            })
        monkeypatch.setattr(ph, 'enqueue_event', fake_enqueue)
        # The breaker does `from shared.posthog_server import enqueue_event` INSIDE
        # the _safe_emit_* helpers, so the LOCAL alias resolves at call time → the
        # monkeypatch on ph.enqueue_event is the one used.
        for _ in range(br.NLI_CIRCUIT_THRESHOLD):
            br.record_failure('timeout', 'test_path')
        opened = [e for e in events if e['event'] == 'nli_breaker_opened']
        assert len(opened) == 1
        props = opened[0]['properties']
        assert props['consecutive_failures'] == br.NLI_CIRCUIT_THRESHOLD
        assert props['triggering_path'] == 'test_path'
        assert props['failure_type'] == 'timeout'
        assert props['threshold'] == br.NLI_CIRCUIT_THRESHOLD
        assert props['window_seconds'] == br.NLI_CIRCUIT_WINDOW

    def test_closed_event_emitted_on_success(self, monkeypatch):
        events = []

        def fake_enqueue(event, properties, distinct_id='system'):
            events.append({'event': event, 'properties': dict(properties)})
        monkeypatch.setattr(ph, 'enqueue_event', fake_enqueue)
        for _ in range(br.NLI_CIRCUIT_THRESHOLD):
            br.record_failure('timeout', 'a')
        br.record_success('recover_path')
        closed = [e for e in events if e['event'] == 'nli_breaker_closed']
        assert len(closed) == 1
        props = closed[0]['properties']
        assert props['closed_by_path'] == 'recover_path'
        assert isinstance(props['downtime_seconds'], (int, float))
        assert props['downtime_seconds'] >= 0.0

    def test_double_trip_does_not_double_emit(self, monkeypatch):
        events = []

        def fake_enqueue(event, properties, distinct_id='system'):
            events.append(event)
        monkeypatch.setattr(ph, 'enqueue_event', fake_enqueue)
        for _ in range(br.NLI_CIRCUIT_THRESHOLD + 5):  # over-trip
            br.record_failure('timeout', 'x')
        assert events.count('nli_breaker_opened') == 1, (
            f'one opened event per closed→open transition, got {events}'
        )

    def test_telemetry_never_raises(self, monkeypatch):
        """D-25 — if enqueue_event raises, breaker still works."""
        def raising(event, properties, distinct_id='system'):
            raise RuntimeError('simulated PostHog outage')
        monkeypatch.setattr(ph, 'enqueue_event', raising)
        # Trip the breaker — must NOT raise
        for _ in range(br.NLI_CIRCUIT_THRESHOLD):
            br.record_failure('timeout', 'test')
        assert br.is_open()
        # Close it — must also NOT raise
        br.record_success('recover')
        assert not br.is_open()

    def test_no_telemetry_when_breaker_not_crossing_threshold(self, monkeypatch):
        events = []
        monkeypatch.setattr(
            ph, 'enqueue_event',
            lambda event, properties, distinct_id='system': events.append(event),
        )
        # Sub-threshold failures emit no telemetry
        for _ in range(br.NLI_CIRCUIT_THRESHOLD - 1):
            br.record_failure('timeout', 'x')
        assert events == []

    def test_no_closed_event_when_breaker_already_closed(self, monkeypatch):
        events = []
        monkeypatch.setattr(
            ph, 'enqueue_event',
            lambda event, properties, distinct_id='system': events.append(event),
        )
        br.record_success('idempotent')
        assert events == [], 'record_success on closed breaker emits no telemetry'


class TestStaticGuards:
    """AST-level invariants — cheap belt-and-suspenders against regressions."""

    def test_monotonic_time_used_not_wall_clock(self):
        """D-04 invariant — time.time() must NOT appear in the breaker source.

        Existing genizah_core.py:3947 uses time.time() (the bug). Phase 98
        eliminates that pattern from shared/nli_circuit_breaker.py.
        """
        src = pathlib.Path('shared/nli_circuit_breaker.py').read_text(encoding='utf-8')
        tree = ast.parse(src)
        for node in ast.walk(tree):
            if isinstance(node, ast.Attribute) and node.attr == 'time':
                if isinstance(node.value, ast.Name) and node.value.id == 'time':
                    pytest.fail(
                        f'shared/nli_circuit_breaker.py uses time.time() at line '
                        f'{node.lineno} — D-04 mandates time.monotonic()'
                    )

    def test_module_does_not_import_web(self):
        """RESEARCH Option (a) — shared/ must not depend on web/."""
        src = pathlib.Path('shared/nli_circuit_breaker.py').read_text(encoding='utf-8')
        for i, line in enumerate(src.splitlines(), start=1):
            stripped = line.strip()
            assert not stripped.startswith('from web.'), (
                f'line {i}: unexpected web import: {line!r}'
            )
            assert not stripped.startswith('import web'), (
                f'line {i}: unexpected web import: {line!r}'
            )

    def test_public_api_surface(self):
        """The __all__ exports match the documented contract."""
        expected = {
            'is_open', 'record_failure', 'record_success',
            'NLI_CIRCUIT_THRESHOLD', 'NLI_CIRCUIT_WINDOW',
            'NLI_CONNECT_TIMEOUT', 'NLI_IIIF_READ_TIMEOUT',
            'NLI_MARC_READ_TIMEOUT', 'NLI_IMAGE_READ_TIMEOUT',
            '_state_snapshot', '_reset_for_tests',
        }
        assert set(br.__all__) == expected, (
            f'__all__ drift: missing={expected - set(br.__all__)}, '
            f'extra={set(br.__all__) - expected}'
        )


class TestNliCircuitBreakerConcurrency:
    """Nyquist-critical — proves the 2026-05-25 hang cannot recur (D-26, D-27)."""

    def test_20_threads_saturate_then_short_circuit(self):
        """D-26 Nyquist invariant — STRENGTHENED per Codex REVIEW Issue 2.

        Two-batch spawn proves the breaker actually short-circuits subsequent
        workers (not just that the test happens to finish quickly):

          Batch A: THRESHOLD (3) workers fail with a SLOW (2-3s) simulated
                   network call. Each failure increments the breaker counter.
          Gate:    Main thread WAITS via threading.Event until is_open() is
                   True (set by the breaker AFTER batch A's THRESHOLD-th
                   failure is recorded).
          Batch B: The remaining (N_WORKERS - THRESHOLD = 17) workers are
                   spawned AFTER the gate releases, so they all encounter
                   an already-open breaker.

        Tight assertions:
          - get_call_count <= THRESHOLD (3): only batch A made network calls;
            batch B short-circuited via is_open() before any session.get().
          - elapsed < 10.0s: even with 2-3s fake calls per worker in batch A,
            total wall time well under the threadpool-exhaustion budget.
          - is_open() is True at test end: post-trip behavior verified.
          - All N_WORKERS workers return without raising.

        This is the strict version of the Nyquist invariant: it proves that
        even with 20 workers READY TO FIRE, at most THRESHOLD calls hit the
        network — exactly the property that prevents the 2026-05-25 hang.
        """
        import requests as _requests

        N_WORKERS = 20
        THRESHOLD = br.NLI_CIRCUIT_THRESHOLD  # default 3
        SLOW_SLEEP = 2.0  # seconds per simulated failure — slow enough that the
        # breaker can record failures and trip while workers are arriving (vs.
        # the original 0.3s which let all 20 workers pass is_open() before any
        # record_failure).
        breaker_opened_event = threading.Event()

        get_call_count = {'n': 0}
        get_lock = threading.Lock()

        def fake_get():
            with get_lock:
                get_call_count['n'] += 1
            time.sleep(SLOW_SLEEP)
            raise _requests.exceptions.ReadTimeout('simulated NLI timeout')

        def call_site_wrapper():
            """The pattern that Wave 3 plans replicate at all 10 sites."""
            if br.is_open():
                return []  # short-circuit
            try:
                fake_get()
                br.record_success(path='test_wrapper')
                return ['ok']
            except _requests.exceptions.ReadTimeout:
                br.record_failure(failure_type='timeout', path='test_wrapper')
                # If this failure crossed the threshold, signal batch B to launch.
                if br.is_open():
                    breaker_opened_event.set()
                return []
            except _requests.exceptions.ConnectionError:
                br.record_failure(failure_type='connection_error', path='test_wrapper')
                if br.is_open():
                    breaker_opened_event.set()
                return []

        start = time.monotonic()
        with ThreadPoolExecutor(max_workers=N_WORKERS) as ex:
            # Batch A: spawn THRESHOLD slow-failing workers to trip the breaker.
            batch_a = [ex.submit(call_site_wrapper) for _ in range(THRESHOLD)]

            # Wait until the breaker has actually opened. Generous timeout
            # accommodates the SLOW_SLEEP per worker (THRESHOLD * SLOW_SLEEP
            # = 6s worst case if workers ran sequentially; in practice they
            # run concurrently and the event fires faster).
            opened = breaker_opened_event.wait(timeout=THRESHOLD * SLOW_SLEEP + 2.0)
            assert opened, (
                f'breaker did not open after {THRESHOLD} slow failures — '
                f'is_open()={br.is_open()}, snapshot={br._state_snapshot()}'
            )

            # Defensive: re-verify the breaker is OPEN before spawning batch B.
            # (The breaker_opened_event being set is necessary but not sufficient;
            # the auto-recovery window must still be in the future.)
            assert br.is_open(), (
                f'breaker_opened_event fired but is_open() is False — '
                f'snapshot={br._state_snapshot()}'
            )

            # Batch B: spawn the remaining workers. ALL of these should see
            # is_open() == True and short-circuit WITHOUT calling fake_get().
            batch_b = [ex.submit(call_site_wrapper) for _ in range(N_WORKERS - THRESHOLD)]

            # Wait for both batches to complete
            done_a, _ = wait(batch_a, timeout=15.0, return_when=ALL_COMPLETED)
            done_b, _ = wait(batch_b, timeout=15.0, return_when=ALL_COMPLETED)
            assert len(done_a) == THRESHOLD, (
                f'batch A hung: {len(done_a)}/{THRESHOLD} completed'
            )
            assert len(done_b) == N_WORKERS - THRESHOLD, (
                f'batch B hung: {len(done_b)}/{N_WORKERS - THRESHOLD} completed'
            )
            results = [f.result(timeout=1.0) for f in (batch_a + batch_b)]

        elapsed = time.monotonic() - start

        # NYQUIST INVARIANT — STRICT: at most THRESHOLD network calls total.
        # Batch A made THRESHOLD calls; batch B was spawned AFTER breaker_opened
        # so its workers must have short-circuited at is_open() check.
        assert get_call_count['n'] <= THRESHOLD, (
            f'STRICT Nyquist violation: {get_call_count["n"]} network calls '
            f'made but breaker should have stopped after {THRESHOLD}. '
            f'Batch B did NOT short-circuit even though is_open() was True '
            f'when they were spawned.'
        )

        # Wall time bounded (loose ceiling — exact bound depends on scheduling)
        assert elapsed < 10.0, (
            f'threadpool was hung: {elapsed:.2f}s for {N_WORKERS} workers — '
            f'breaker did NOT prevent saturation'
        )

        # Post-trip behavior: breaker is still open at test end (the auto-
        # recovery window is 60s, so 10s into the test it must still be open).
        assert br.is_open(), (
            f'breaker was opened mid-test but is now closed — auto-recovery '
            f'fired prematurely. Snapshot: {br._state_snapshot()}'
        )

        # All workers returned cleanly (no exceptions escaped)
        assert all(r == [] for r in results), f'unexpected non-empty results: {results}'
        assert len(results) == N_WORKERS, f'lost workers: {len(results)} != {N_WORKERS}'

    def test_20_threads_breaker_observability_post_trip(self):
        """Companion to D-26: after the strict Nyquist test, _state_snapshot
        reflects the actual call pattern (at least THRESHOLD failures recorded).
        This is a smaller, faster test (no slow sleeps) that verifies the
        breaker's introspection surface is consistent post-saturation.
        """
        for _ in range(br.NLI_CIRCUIT_THRESHOLD):
            br.record_failure('timeout', 'observability_test')
        snap = br._state_snapshot()
        assert snap['is_open_now'] is True
        assert snap['consecutive_failures'] >= br.NLI_CIRCUIT_THRESHOLD
        assert snap['open_until_monotonic'] > time.monotonic()

    def test_record_failure_under_n_threads(self):
        """D-27: N concurrent record_failure calls — no lost increments.

        Proves the threading.Lock is doing its job. Without the lock,
        _consecutive_failures += 1 is not atomic and we'd lose increments
        under high contention.
        """
        N = 50
        barrier = threading.Barrier(N)
        errors = []

        def worker():
            try:
                barrier.wait(timeout=5.0)  # BEFORE the breaker call (Pattern 3)
                br.record_failure('timeout', 'race_test')
            except Exception as exc:
                errors.append(exc)

        threads = [threading.Thread(target=worker) for _ in range(N)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10.0)

        assert not errors, f'workers raised: {errors}'
        snap = br._state_snapshot()
        assert snap['consecutive_failures'] == N, (
            f'lost increments under concurrent access: '
            f'expected {N}, got {snap["consecutive_failures"]}'
        )

    def test_lock_serializes_record_failure(self, monkeypatch):
        """Instrument the lock to prove max_concurrent == 1 inside critical region."""
        recorder = _ConcurrencyRecorder()
        original_lock = br._lock

        # Wrap the lock with instrumentation that records entry/exit
        class InstrumentedLock:
            def __enter__(self):
                original_lock.__enter__()
                recorder.enter()
                return self

            def __exit__(self, *args):
                recorder.exit()
                return original_lock.__exit__(*args)

            def acquire(self, *a, **k):
                return original_lock.acquire(*a, **k)

            def release(self, *a, **k):
                return original_lock.release(*a, **k)

        monkeypatch.setattr(br, '_lock', InstrumentedLock())

        N = 30
        barrier = threading.Barrier(N)

        def worker():
            barrier.wait(timeout=5.0)
            br.record_failure('timeout', 'race')

        threads = [threading.Thread(target=worker) for _ in range(N)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10.0)

        assert recorder.max_concurrent == 1, (
            f'lock did not serialize: max_concurrent={recorder.max_concurrent} '
            f'(two threads entered the critical region simultaneously)'
        )

    def test_concurrent_failures_and_successes_terminate(self):
        """Mixed failure/success workload terminates in bounded time with consistent state."""
        N = 30
        barrier = threading.Barrier(N)

        def worker(i):
            barrier.wait(timeout=5.0)
            if i % 3 == 0:
                br.record_success('mix')
            else:
                br.record_failure('timeout', 'mix')

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(N)]
        start = time.monotonic()
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=5.0)
        elapsed = time.monotonic() - start

        assert elapsed < 2.0, f'mixed workload took {elapsed:.2f}s — lock contention too high'
        # State is consistent (counter is some non-negative integer)
        snap = br._state_snapshot()
        assert snap['consecutive_failures'] >= 0
