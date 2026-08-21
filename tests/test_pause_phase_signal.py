"""The LOCAL-phase progress channel.

The LOCAL (My Library) pass reports hit counts unrelated to the Genizah loop's.
Sending them down the numeric channel rewinds the bar; pinning them at
(total, total) instead reads as 100% complete with a zero ETA while a long phase
is still running. So the phase gets its own signal and switches the bar to
indeterminate.

The subtle part is that the phase state must be DURABLE. A numeric tick emitted
by the Genizah loop can still be sitting in the queue when the phase arrives; if
it lands afterwards it would restore a determinate range and undo the switch.
"""

import types

import genizah_app as app
from genizah_app import _PauseCtx
from shared.search_engine import PHASE_LOCAL_SEARCH


class _FakeBar:
    def __init__(self):
        self.range = None
        self.fmt = None
        self.maximum = None
        self.value = None

    def setRange(self, a, b): self.range = (a, b)
    def setFormat(self, f): self.fmt = f
    def setMaximum(self, m): self.maximum = m
    def setValue(self, v): self.value = v


class _FakeLabel:
    def __init__(self): self.text = None
    def setText(self, t): self.text = t


def _stub():
    s = types.SimpleNamespace()
    s._pause_search = _PauseCtx()
    s._pause_search.reset_for_run(1, mono_start=0.0)
    s.search_progress = _FakeBar()
    s.status_label = _FakeLabel()
    for name in ('_on_search_phase', '_on_search_progress'):
        setattr(s, name, getattr(app.GenizahGUI, name).__get__(s, types.SimpleNamespace))
    return s


def test_phase_switches_the_bar_to_indeterminate():
    s = _stub()
    s._on_search_phase(PHASE_LOCAL_SEARCH)

    assert s.search_progress.range == (0, 0)
    assert s.search_progress.fmt == 'Searching My Library...'
    assert s.status_label.text == 'Searching My Library...'


def test_phase_sets_a_durable_flag():
    s = _stub()
    assert s._pause_search.local_phase_active is False
    s._on_search_phase(PHASE_LOCAL_SEARCH)
    assert s._pause_search.local_phase_active is True


def test_a_late_numeric_tick_cannot_undo_the_indeterminate_bar():
    """The regression this design exists for: a tick queued by the Genizah loop
    arriving after the phase switch must be dropped, not applied."""
    s = _stub()
    s._on_search_phase(PHASE_LOCAL_SEARCH)

    s._on_search_progress(17, 5000)          # stale numeric tick

    assert s.search_progress.range == (0, 0), 'bar went determinate again'
    assert s.search_progress.maximum is None
    assert s.search_progress.value is None


def test_numeric_progress_still_works_before_the_phase():
    s = _stub()
    s._on_search_progress(42, 100)
    assert s.search_progress.maximum == 100
    assert s.search_progress.value == 42


def test_unknown_phase_codes_are_ignored():
    s = _stub()
    s._on_search_phase('some_future_phase')
    assert s._pause_search.local_phase_active is False
    assert s.search_progress.range is None


def test_a_new_run_clears_the_phase_flag():
    """Otherwise the second search would open with a permanently indeterminate bar."""
    s = _stub()
    s._on_search_phase(PHASE_LOCAL_SEARCH)
    assert s._pause_search.local_phase_active is True

    s._pause_search.reset_for_run(2, mono_start=0.0)
    assert s._pause_search.local_phase_active is False

    s._on_search_progress(5, 50)
    assert s.search_progress.value == 5


def test_start_search_rearms_the_context_each_run():
    import inspect
    src = inspect.getsource(app.GenizahGUI.start_search)
    assert 'reset_for_run' in src, 'start_search does not re-arm the pause context'


def test_phase_signal_is_distinct_from_the_numeric_channel():
    """A phase is not a status string and not a progress pair. Keeping it its own
    signal is what stops the UI pattern-matching on prose."""
    import gui_threads
    sigs = gui_threads.SearchThread.phase_signal.signatures
    assert any('QString' in s or 'str' in s for s in sigs), sigs
    prog = gui_threads.SearchThread.progress_signal.signatures
    assert any('int,int' in s.replace(' ', '') for s in prog), prog
