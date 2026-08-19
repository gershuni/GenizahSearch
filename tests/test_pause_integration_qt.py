"""Live-QThread integration for Pause/Resume. Runs in the `gui` CI lane.

The rest of this feature's suite is deliberately Qt-free, and that suite
structurally cannot catch what lives here: real thread affinity, queued-vs-direct
signal delivery, a stale acknowledgement crossing a run boundary, and actual
QThread.wait() timing. Those are exactly where a pause feature goes wrong.

Timing assertions are upper bounds ("finished within the existing budget"),
never lower bounds on latency.
"""

import os
import time

import pytest

os.environ.setdefault('QT_QPA_PLATFORM', 'offscreen')

from PyQt6.QtWidgets import QApplication  # noqa: E402

from genizah_app import _PauseCtx  # noqa: E402
from gui_threads import SearchThread  # noqa: E402

# The budget stop_search actually allows before it reaches terminate().
STOP_WAIT_BUDGET_MS = 5000

_app = QApplication.instance() or QApplication([])


class _FakeSearcher:
    """Calls progress_callback in a tight loop, like a real materialization loop."""

    def __init__(self, n=100000):
        self.n = n
        self.ticks = 0

    def execute_search(self, *a, **kw):
        cb = kw.get('progress_callback')
        for i in range(self.n):
            self.ticks += 1
            if cb:
                cb(i, self.n)
            time.sleep(0.001)
        return ['result'] * 3


def _spin(predicate, timeout=5.0):
    """Pump the event loop until predicate() or timeout. Returns success."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        _app.processEvents()
        if predicate():
            return True
        time.sleep(0.005)
    _app.processEvents()
    return predicate()


def _start(run_id=1):
    searcher = _FakeSearcher()
    thread = SearchThread(searcher, 'q', 'literal', 0, run_id=run_id)
    acks = []
    thread.pause_ack_signal.connect(lambda r, e: acks.append((r, e)))
    thread.start()
    assert _spin(lambda: searcher.ticks > 3), 'worker never started ticking'
    return searcher, thread, acks


def _stop(thread):
    if thread.isRunning():
        thread.request_cancel()
        thread.wait(STOP_WAIT_BUDGET_MS)


@pytest.mark.gui
def test_pause_parks_the_worker_and_the_ack_reaches_the_ui_thread():
    searcher, thread, acks = _start(run_id=3)
    try:
        thread.pause()
        assert _spin(lambda: acks), 'no pause acknowledgement was delivered'
        assert acks == [(3, 1)], acks

        parked = searcher.ticks
        time.sleep(0.15)
        _app.processEvents()
        assert searcher.ticks == parked, 'worker kept scanning while parked'
    finally:
        _stop(thread)


@pytest.mark.gui
def test_paused_cancellation_completes_before_the_existing_timeout():
    """NOTE the precise claim: a PARKED worker exits well inside the budget.

    This says nothing about a worker stuck between checkpoints — that one is not
    parked, so no wake mechanism reaches it and the pre-existing terminate()
    fallback stays reachable. See docs/OPEN_ISSUES.md.
    """
    searcher, thread, acks = _start()
    try:
        thread.pause()
        assert _spin(lambda: acks)

        started = time.monotonic()
        thread.request_cancel()
        finished = thread.wait(STOP_WAIT_BUDGET_MS)
        elapsed_ms = (time.monotonic() - started) * 1000.0

        assert finished, 'parked worker did not exit within the stop budget'
        assert elapsed_ms < STOP_WAIT_BUDGET_MS, elapsed_ms
    finally:
        _stop(thread)


@pytest.mark.gui
def test_resume_lets_the_worker_continue():
    searcher, thread, acks = _start()
    try:
        thread.pause()
        assert _spin(lambda: acks)
        parked = searcher.ticks

        thread.resume()
        assert _spin(lambda: searcher.ticks > parked), 'worker did not resume'
    finally:
        _stop(thread)


@pytest.mark.gui
def test_a_stale_ack_from_a_stopped_run_cannot_repaint_a_later_run():
    """The cross-run race the run token exists for.

    Run A is stopped while pausing; its acknowledgement may already be queued.
    Run B then starts and pauses, and B's own epoch numbering restarts at 1 — so
    the stale ack carries numbers that would match if only the epoch were checked.
    B must stay 'pausing' until its OWN worker acknowledges.
    """
    ctx = _PauseCtx()

    # --- run A: pause, then stop it before its ack is acted on --------------
    searcher_a, thread_a, acks_a = _start(run_id=1)
    ctx.reset_for_run(1, mono_start=time.monotonic())
    ctx.epoch = 1
    ctx.state = 'pausing'
    thread_a.pause()
    assert _spin(lambda: acks_a)
    stale_run_id, stale_epoch = acks_a[0]
    _stop(thread_a)

    # --- run B: a fresh worker, epoch numbering restarts ---------------------
    searcher_b, thread_b, acks_b = _start(run_id=2)
    try:
        ctx.reset_for_run(2, mono_start=time.monotonic())
        ctx.epoch = 1
        ctx.state = 'pausing'

        assert (stale_run_id, stale_epoch) == (1, 1)
        assert ctx.epoch == stale_epoch, 'epochs must collide for this test to mean anything'
        assert ctx.accepts_ack(stale_run_id, stale_epoch) is False, (
            'run A\'s stale acknowledgement was accepted by run B')
        assert ctx.accepts_ack(2, 1) is True
    finally:
        _stop(thread_b)


@pytest.mark.gui
def test_cancelled_run_emits_results_but_not_perf():
    searcher, thread, acks = _start()
    results, perfs, errors = [], [], []
    thread.results_signal.connect(results.append)
    thread.perf_signal.connect(lambda ms, rc: perfs.append((ms, rc)))
    thread.error_signal.connect(errors.append)
    try:
        thread.request_cancel()
        assert thread.wait(STOP_WAIT_BUDGET_MS)
        _spin(lambda: bool(results), timeout=2.0)

        assert results, 'no results_signal on the cancel path'
        assert perfs == [], 'perf telemetry emitted for a cancelled run'
        assert errors == [], 'a user cancel surfaced as an error'
    finally:
        _stop(thread)


@pytest.mark.gui
def test_two_workers_pause_independently():
    """The search and composition tabs are independently runnable."""
    s1, t1, acks1 = _start(run_id=10)
    s2, t2, acks2 = _start(run_id=11)
    try:
        t1.pause()
        assert _spin(lambda: acks1)
        assert acks1 == [(10, 1)]

        # the second worker must be unaffected
        moved = s2.ticks
        assert _spin(lambda: s2.ticks > moved), 'pausing one worker stalled the other'
        assert acks2 == [], 'the second worker acknowledged a pause it never got'

        t2.pause()
        assert _spin(lambda: acks2)
        assert acks2 == [(11, 1)]
    finally:
        _stop(t1)
        _stop(t2)


@pytest.mark.gui
def test_finish_releases_a_pause_requested_at_the_finish_line():
    """A run that completes while 'Pausing...' is pending must not strand the gate."""
    searcher = _FakeSearcher(n=5)
    thread = SearchThread(searcher, 'q', 'literal', 0, run_id=1)
    try:
        thread.start()
        assert thread.wait(STOP_WAIT_BUDGET_MS), 'short run did not finish'
        thread.pause()                       # too late — the gate is closed
        assert thread.pause_gate.is_pause_pending() is False
    finally:
        _stop(thread)
