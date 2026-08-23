"""Pause/Resume UI state machine, driven against a stub — no QApplication.

Every handler takes its _PauseCtx explicitly and reaches widgets only through
it, which is what lets the whole state machine be exercised on a
types.SimpleNamespace. Precedent: _make_wiring_stub in test_telemetry_phase114.

Kept out of the `gui` lane deliberately: none of this needs a real widget, and
the fragile lane should hold only what genuinely needs Qt.
"""

import itertools
import time
import types

import pytest

import genizah_app as app
from genizah_app import _PauseCtx

import genizah_core


@pytest.fixture(autouse=True)
def _pin_english_labels(monkeypatch):
    """tr() reads genizah_core.CURRENT_LANG, which load_language() fills from
    the DEVELOPER'S persisted app config at import -- so the label assertions
    below ('Pause', 'Paused', ...) would test the machine owner's language
    setting, not the code, and go red on any Hebrew-configured machine while
    CI stays green on its English default. Pin the language the assertions
    are written in (precedent: test_libfilter_desktop.py:1217)."""
    monkeypatch.setattr(genizah_core, 'CURRENT_LANG', 'en')



class _FakeButton:
    def __init__(self):
        self.text = None
        self.visible = None
        self.enabled = None
        self.style = ''
        self.tooltip = ''
        self.accessible_name = None

    def setText(self, t): self.text = t
    def setVisible(self, v): self.visible = v
    def setEnabled(self, e): self.enabled = e
    def setStyleSheet(self, s): self.style = s
    def setToolTip(self, t): self.tooltip = t
    def setAccessibleName(self, t): self.accessible_name = t


class _FakeWorker:
    def __init__(self, running=True):
        self.calls = []
        self._running = running

    def isRunning(self): return self._running
    def pause(self): self.calls.append('pause'); return True
    def resume(self): self.calls.append('resume'); return True


class _FakeBar:
    def __init__(self): self.fmt = None; self.range = None
    def setFormat(self, f): self.fmt = f
    def setRange(self, a, b): self.range = (a, b)


class _FakeLabel:
    def __init__(self): self.text = None
    def setText(self, t): self.text = t


class _FakeTimer:
    def __init__(self): self.running = True
    def start(self, _ms): self.running = True
    def stop(self): self.running = False


_METHODS = ('_pause_worker_for', '_apply_pause_state', '_pause_elapsed_str',
            '_paint_pause_status', '_on_pause_clicked', '_on_pause_ack',
            '_on_search_phase')


def _stub():
    s = types.SimpleNamespace()
    s._pause_search = _PauseCtx()
    s._pause_comp = _PauseCtx()
    s._pause_search.button = _FakeButton()
    s._pause_comp.button = _FakeButton()
    s.search_thread = _FakeWorker()
    s.comp_thread = _FakeWorker()
    s.search_progress = _FakeBar()
    s.comp_progress = _FakeBar()
    s.status_label = _FakeLabel()
    s._search_elapsed_timer = _FakeTimer()
    s.comp_chunks_processed = 3
    s.comp_chunks_total = 10
    for name in _METHODS:
        setattr(s, name, getattr(app.GenizahGUI, name).__get__(s, types.SimpleNamespace))
    return s


@pytest.fixture
def controlled_clock(monkeypatch):
    """A monotonic clock that provably advances, 0.5 s per read.

    Do not delete this as ceremony — CI on windows-latest caught the bug it
    exists for, and it cannot fail on Linux.

    On Windows, CPython 3.11's time.monotonic() is GetTickCount64-based with
    ~15.6 ms granularity. pause -> ack -> resume executed back-to-back all land
    inside one tick, so `monotonic() - pause_started` is exactly 0.0 and nothing
    is banked. That is CORRECT behaviour — nobody pauses and resumes inside
    16 ms, and any human-length pause spans many ticks — but it means a test
    that asserts on real elapsed time is testing the platform's clock
    resolution, not the arithmetic it means to check. Linux's nanosecond
    clock_gettime hid that for the whole of local development.

    Driving the clock ourselves makes the assertion about the code again, and
    is robust whatever a given platform's resolution turns out to be.
    """
    ticks = itertools.count(1000.0, 0.5)
    monkeypatch.setattr(time, 'monotonic', lambda: next(ticks))
    return ticks


@pytest.fixture
def running_search():
    s = _stub()
    s._pause_search.reset_for_run(1, mono_start=0.0)
    s._apply_pause_state(s._pause_search, 'pause')
    return s


# ------------------------------------------------------------ click -> pausing

def test_click_requests_a_pause_and_disables_the_button(running_search):
    s = running_search
    s._on_pause_clicked(s._pause_search)

    assert s.search_thread.calls == ['pause']
    assert s._pause_search.state == 'pausing'
    assert s._pause_search.epoch == 1
    assert s._pause_search.button.text == app.PAUSING_GLYPH
    assert s._pause_search.button.enabled is False


def test_the_pause_clock_does_not_start_at_the_click(running_search):
    """During "Pausing..." the worker is still doing real work toward the result,
    and with a coarse checkpoint that window is seconds. Excluding it would
    under-report elapsed and inflate the implied rate."""
    s = running_search
    s._on_pause_clicked(s._pause_search)
    assert s._pause_search.pause_started == 0.0


def test_the_elapsed_ticker_keeps_running_while_pausing(running_search):
    s = running_search
    s._on_pause_clicked(s._pause_search)
    assert s._search_elapsed_timer.running is True


def test_a_second_click_while_pausing_is_a_no_op(running_search):
    s = running_search
    s._on_pause_clicked(s._pause_search)
    s._on_pause_clicked(s._pause_search)
    assert s.search_thread.calls == ['pause'], 'pause requested twice'
    assert s._pause_search.epoch == 1


def test_click_is_ignored_when_no_worker_is_running():
    s = _stub()
    s._pause_search.reset_for_run(1, mono_start=0.0)
    s.search_thread = _FakeWorker(running=False)
    s._on_pause_clicked(s._pause_search)
    assert s._pause_search.state == 'running'


# ------------------------------------------------------------- ack -> paused

def test_valid_ack_flips_to_resume_and_stops_the_ticker(running_search):
    s = running_search
    s._on_pause_clicked(s._pause_search)
    s._on_pause_ack(s._pause_search, 1, 1)

    assert s._pause_search.state == 'paused'
    assert s._pause_search.button.text == app.RESUME_GLYPH
    assert s._pause_search.button.enabled is True
    assert s._pause_search.pause_started > 0.0
    assert s._search_elapsed_timer.running is False


@pytest.mark.parametrize('run_id,epoch', [(99, 1), (1, 99), (99, 99)])
def test_stale_acks_are_ignored(running_search, run_id, epoch):
    s = running_search
    s._on_pause_clicked(s._pause_search)
    s._on_pause_ack(s._pause_search, run_id, epoch)

    assert s._pause_search.state == 'pausing'
    assert s._pause_search.button.text == app.PAUSING_GLYPH


# ------------------------------------------------------------ resume -> running

def test_resume_banks_the_parked_time_and_restarts_the_ticker(running_search, controlled_clock):
    """Resuming must bank the time spent parked.

    Takes controlled_clock: with a real clock this asserts on the platform's
    timer resolution rather than on the banking arithmetic, and fails on Windows.
    """
    s = running_search
    s._on_pause_clicked(s._pause_search)
    s._on_pause_ack(s._pause_search, 1, 1)
    s._on_pause_clicked(s._pause_search)

    assert s.search_thread.calls == ['pause', 'resume']
    assert s._pause_search.state == 'running'
    assert s._pause_search.paused_total > 0.0
    assert s._pause_search.pause_started == 0.0
    assert s._pause_search.button.text == app.PAUSE_GLYPH
    assert s._search_elapsed_timer.running is True


def test_banking_is_correct_under_a_coarse_clock(monkeypatch):
    """The code — not the clock — under Windows-like timer granularity.

    Feeds an explicit reading sequence instead of a real clock, so this asserts
    the same thing on every platform. The handlers read time.monotonic() in this
    order (measured, not assumed): pause-click repaint, ack stamp, ack repaint,
    resume. Holding the first three inside one 15.6 ms tick and letting the
    fourth land on the next one is exactly what a real pause looks like on
    Windows, and the banked total must come out as the tick boundary.
    """
    readings = iter([1000.0, 1000.0, 1000.0, 1015.6])
    monkeypatch.setattr(time, 'monotonic', lambda: next(readings))

    s = _stub()
    s._pause_search.reset_for_run(1, mono_start=0.0)
    s._apply_pause_state(s._pause_search, 'pause')

    s._on_pause_clicked(s._pause_search)
    s._on_pause_ack(s._pause_search, 1, 1)       # pause_started = 1000.0
    s._on_pause_clicked(s._pause_search)         # resume reads 1015.6

    assert s._pause_search.paused_total == pytest.approx(15.6)


def test_a_sub_tick_pause_banks_nothing_and_that_is_correct(monkeypatch):
    """The behaviour that broke the original test, pinned as intended.

    A pause and resume inside a single clock tick bank exactly 0.0. No user can
    do that by hand, and treating it as a bug would mean inventing time that was
    never spent — so the contract is that it is a no-op, not that it is positive.
    """
    monkeypatch.setattr(time, 'monotonic', lambda: 1234.5)   # frozen: one tick

    s = _stub()
    s._pause_search.reset_for_run(1, mono_start=0.0)
    s._apply_pause_state(s._pause_search, 'pause')

    s._on_pause_clicked(s._pause_search)
    s._on_pause_ack(s._pause_search, 1, 1)
    s._on_pause_clicked(s._pause_search)

    assert s._pause_search.state == 'running'
    assert s._pause_search.paused_total == 0.0
    assert s._pause_search.pause_started == 0.0


def test_a_cycle_one_ack_arriving_during_cycle_two_is_ignored(running_search):
    s = running_search
    s._on_pause_clicked(s._pause_search)
    s._on_pause_ack(s._pause_search, 1, 1)
    s._on_pause_clicked(s._pause_search)          # resume
    s._on_pause_clicked(s._pause_search)          # pause again, epoch 2
    assert s._pause_search.epoch == 2

    s._on_pause_ack(s._pause_search, 1, 1)        # stale cycle-1 ack
    assert s._pause_search.state == 'pausing'


# ------------------------------------------------------------------- painting

def test_hidden_resets_the_label_so_it_never_reopens_on_resume(running_search):
    """Asserted against the glyph constants, not literals: the face is a symbol
    because the button only has 34 px, and a hardcoded word here would just
    re-encode the old design."""
    s = running_search
    s._on_pause_clicked(s._pause_search)
    s._on_pause_ack(s._pause_search, 1, 1)
    assert s._pause_search.button.text == app.RESUME_GLYPH

    s._apply_pause_state(s._pause_search, 'hidden')
    assert s._pause_search.button.visible is False
    assert s._pause_search.button.text == app.PAUSE_GLYPH


def test_a_glyph_face_still_carries_a_readable_label(running_search):
    """A symbol alone is not a label. Every state must set an accessible name,
    or the button is unreadable to a screen reader and to anyone who does not
    recognise the glyph."""
    s = running_search
    for state, expected in (('pause', 'Pause'),
                            ('pausing', 'Pausing...'),
                            ('resume', 'Resume'),
                            ('hidden', 'Pause')):
        s._apply_pause_state(s._pause_search, state)
        assert s._pause_search.button.accessible_name == expected, state
        assert s._pause_search.button.tooltip, state


def test_both_stylesheets_carry_a_disabled_rule(running_search):
    """Every button in genizah_app sets a literal background-color, which
    overrides the disabled palette. Without an explicit :disabled rule the
    "Pausing..." state renders as a full-strength, clickable-looking button."""
    s = running_search
    s._apply_pause_state(s._pause_search, 'pause')
    assert 'QPushButton:disabled' in s._pause_search.button.style
    s._apply_pause_state(s._pause_search, 'resume')
    assert 'QPushButton:disabled' in s._pause_search.button.style


def test_apply_pause_state_tolerates_a_missing_button():
    s = _stub()
    s._pause_search.button = None
    s._apply_pause_state(s._pause_search, 'pause')   # must not raise
    s._apply_pause_state(None, 'pause')


# ------------------------------------------------------------ tab independence

def test_pausing_a_composition_does_not_touch_a_paused_search():
    s = _stub()
    s._pause_search.reset_for_run(1, mono_start=0.0)
    s._pause_comp.reset_for_run(2, mono_start=0.0)

    s._on_pause_clicked(s._pause_search)
    s._on_pause_ack(s._pause_search, 1, 1)
    assert s._pause_search.state == 'paused'

    s._on_pause_clicked(s._pause_comp)
    s._on_pause_ack(s._pause_comp, 2, 1)

    assert s._pause_comp.state == 'paused'
    assert s._pause_search.state == 'paused', 'composition clobbered the search pause'
    assert s._pause_search.button is not s._pause_comp.button
    assert s.search_thread.calls == ['pause']
    assert s.comp_thread.calls == ['pause']


def test_composition_paints_its_progress_bar_not_the_search_label():
    s = _stub()
    s._pause_comp.reset_for_run(2, mono_start=0.0)
    s._on_pause_clicked(s._pause_comp)
    s._on_pause_ack(s._pause_comp, 2, 1)

    assert 'Paused' in (s.comp_progress.fmt or '')
    assert '3/10' in (s.comp_progress.fmt or '')
    assert s.status_label.text is None, 'composition wrote to the search status label'
