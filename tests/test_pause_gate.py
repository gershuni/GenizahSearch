"""PauseGate unit tests — pure Python, no Qt, no project imports beyond the gate.

Timing assertions here are UPPER bounds only ("this must finish within N
seconds"), never lower bounds on latency. A test that asserts a worker is still
parked after sleeping is a flake generator; a test that asserts it *joined* is
not.
"""

import threading
import time

import pytest

from shared.pause_gate import PauseGate


class _Worker:
    """A thread that spins on wait_if_paused and counts its own progress."""

    def __init__(self, gate, should_abort=None):
        self.gate = gate
        self.should_abort = should_abort
        self.ticks = 0
        self.aborted = False
        self.thread = threading.Thread(target=self._run, daemon=True)

    def _run(self):
        while True:
            if self.gate.wait_if_paused(self.should_abort):
                self.aborted = True
                return
            self.ticks += 1
            time.sleep(0.001)

    def start(self):
        self.thread.start()
        return self

    def wait_until_ticking(self, timeout=2.0):
        """Block until the worker has made progress, so tests never race it."""
        start, seen = time.monotonic(), self.ticks
        while time.monotonic() - start < timeout:
            if self.ticks > seen:
                return True
            time.sleep(0.005)
        return False

    def join(self, timeout=2.0):
        self.thread.join(timeout)
        return not self.thread.is_alive()


def _wait_for(predicate, timeout=2.0):
    start = time.monotonic()
    while time.monotonic() - start < timeout:
        if predicate():
            return True
        time.sleep(0.005)
    return False


def test_gate_starts_running():
    gate = PauseGate()
    assert gate.state == PauseGate.RUNNING
    assert gate.wait_if_paused() is False
    assert gate.is_pause_pending() is False


def test_pause_parks_worker_and_acks():
    acks = []
    gate = PauseGate(on_ack=acks.append)
    w = _Worker(gate).start()
    assert w.wait_until_ticking()

    gate.pause()
    assert _wait_for(lambda: acks == [1]), "worker never acknowledged the pause"
    assert gate.state == PauseGate.PAUSED

    parked_at = w.ticks
    time.sleep(0.05)
    assert w.ticks == parked_at, "worker kept working while parked"

    gate.abort()
    assert w.join()


def test_resume_unparks():
    acks = []
    gate = PauseGate(on_ack=acks.append)
    w = _Worker(gate).start()
    assert w.wait_until_ticking()
    gate.pause()
    assert _wait_for(lambda: acks == [1])

    parked_at = w.ticks
    gate.resume()
    assert _wait_for(lambda: w.ticks > parked_at), "worker did not resume"
    assert gate.state == PauseGate.RUNNING

    gate.abort()
    assert w.join()


def test_abort_while_paused_unparks_immediately():
    gate = PauseGate()
    w = _Worker(gate).start()
    assert w.wait_until_ticking()
    gate.pause()
    assert _wait_for(lambda: gate.state == PauseGate.PAUSED)

    gate.abort()
    assert w.join(), "parked worker did not exit after abort()"
    assert w.aborted


def test_flag_only_cancel_unparks_parked_worker():
    """The load-bearing test: a cancel that never touches the gate still works.

    This is why the design polls instead of making cancel_flag a property whose
    setter unparks. closeEvent's composition branch calls requestInterruption()
    and never assigns cancel_flag, so a setter-based wake would miss it entirely
    and the worker would stay parked until QThread.terminate().

    Note abort() is deliberately NOT called here.
    """
    cancelled = {'v': False}
    gate = PauseGate()
    w = _Worker(gate, should_abort=lambda: cancelled['v']).start()
    assert w.wait_until_ticking()
    gate.pause()
    assert _wait_for(lambda: gate.state == PauseGate.PAUSED)

    cancelled['v'] = True           # the ONLY signal — gate untouched
    assert w.join(), "parked worker did not notice a flag-only cancel"
    assert w.aborted


def test_pause_after_abort_is_noop():
    acks = []
    gate = PauseGate(on_ack=acks.append)
    gate.abort()
    assert gate.pause() is False
    assert gate.is_pause_pending() is False
    assert gate.wait_if_paused() is True
    assert acks == []


def test_finish_releases_a_pending_pause():
    """A run that completes while a pause is pending must not strand the gate."""
    gate = PauseGate()
    gate.pause()
    assert gate.is_pause_pending() is True
    gate.finish()
    assert gate.is_pause_pending() is False
    assert gate.wait_if_paused() is True


def test_total_paused_s_accumulates_across_cycles():
    gate = PauseGate()
    w = _Worker(gate).start()
    assert w.wait_until_ticking()

    for _ in range(2):
        gate.pause()
        assert _wait_for(lambda: gate.state == PauseGate.PAUSED)
        time.sleep(0.05)
        gate.resume()
        assert w.wait_until_ticking()

    assert gate.total_paused_s > 0.0
    assert gate.total_paused_s < 10.0, "implausible parked time — clock bug?"

    gate.abort()
    assert w.join()


def test_park_hooks_run_on_the_worker_thread():
    """_prevent_sleep/_allow_sleep use SetThreadExecutionState, which is
    per-thread — running them on the UI thread would silently do nothing."""
    idents = {}
    gate = PauseGate()
    gate.on_park = lambda: idents.setdefault('park', threading.get_ident())
    gate.on_unpark = lambda: idents.setdefault('unpark', threading.get_ident())

    w = _Worker(gate).start()
    assert w.wait_until_ticking()
    gate.pause()
    assert _wait_for(lambda: 'park' in idents)
    gate.resume()
    assert _wait_for(lambda: 'unpark' in idents)

    worker_ident = w.thread.ident
    assert idents['park'] == worker_ident
    assert idents['unpark'] == worker_ident
    assert idents['park'] != threading.get_ident()

    gate.abort()
    assert w.join()


def test_hook_exceptions_do_not_break_the_gate():
    def boom(*_a):
        raise RuntimeError("listener is broken")

    gate = PauseGate(on_ack=boom)
    gate.on_park = boom
    gate.on_unpark = boom
    w = _Worker(gate).start()
    assert w.wait_until_ticking()

    gate.pause()
    assert _wait_for(lambda: gate.state == PauseGate.PAUSED)
    parked_at = w.ticks
    gate.resume()
    assert _wait_for(lambda: w.ticks > parked_at), "a broken hook stranded the worker"

    gate.abort()
    assert w.join()


def test_no_deadlock_under_rapid_pause_resume():
    gate = PauseGate()
    w = _Worker(gate).start()
    assert w.wait_until_ticking()

    for _ in range(200):
        gate.pause()
        gate.resume()

    assert w.wait_until_ticking(), "worker stopped advancing after churn"
    gate.abort()
    assert w.join()


def test_resume_between_check_and_ack_publishes_nothing():
    """Atomicity: the unlocked hot-path check and the ack are not one step.

    Simulated by resuming before the worker reaches the gate at all, which is
    the same interleaving from the gate's point of view.
    """
    acks, parks = [], []
    gate = PauseGate(on_ack=acks.append)
    gate.on_park = lambda: parks.append(1)

    gate.pause()
    gate.resume()                      # UI changed its mind before any park
    assert gate.wait_if_paused() is False
    assert acks == [], "published a paused ack for a worker that never parked"
    assert parks == [], "ran the park hook for a worker that never parked"


def test_bare_cancel_with_gate_paused_never_parks():
    """A pre-set cancel flag must short-circuit before the ack and the park."""
    acks, parks = [], []
    gate = PauseGate(on_ack=acks.append)
    gate.on_park = lambda: parks.append(1)
    gate.pause()

    t0 = time.monotonic()
    assert gate.wait_if_paused(lambda: True) is True
    assert time.monotonic() - t0 < 1.0, "parked despite an already-set cancel"
    assert acks == []
    assert parks == []


def test_epoch_increments_per_pause_request_only():
    gate = PauseGate()
    assert gate.epoch == 0
    gate.pause()
    assert gate.epoch == 1
    gate.pause()                       # already pausing — no-op
    assert gate.epoch == 1
    gate.resume()
    assert gate.epoch == 1
    gate.pause()
    assert gate.epoch == 2


@pytest.mark.parametrize('n', [1, 2, 3])
def test_ack_epoch_matches_the_pause_cycle(n):
    acks = []
    gate = PauseGate(on_ack=acks.append)
    w = _Worker(gate).start()
    assert w.wait_until_ticking()

    for cycle in range(1, n + 1):
        gate.pause()
        assert _wait_for(lambda c=cycle: len(acks) == c), (
            "cycle %d never acknowledged (got %r)" % (cycle, acks))
        assert gate.state == PauseGate.PAUSED
        gate.resume()
        assert w.wait_until_ticking()

    assert acks == list(range(1, n + 1)), "ack epochs did not track pause cycles"
    gate.abort()
    assert w.join()
