"""Pause/Resume UI state machine, driven against a stub — no QApplication.

Every handler takes its _PauseCtx explicitly and reaches widgets only through
it, which is what lets the whole state machine be exercised on a
types.SimpleNamespace. Precedent: _make_wiring_stub in test_telemetry_phase114.

Kept out of the `gui` lane deliberately: none of this needs a real widget, and
the fragile lane should hold only what genuinely needs Qt.
"""

import types

import pytest

import genizah_app as app
from genizah_app import _PauseCtx


class _FakeButton:
    def __init__(self):
        self.text = None
        self.visible = None
        self.enabled = None
        self.style = ''
        self.tooltip = ''

    def setText(self, t): self.text = t
    def setVisible(self, v): self.visible = v
    def setEnabled(self, e): self.enabled = e
    def setStyleSheet(self, s): self.style = s
    def setToolTip(self, t): self.tooltip = t


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
    assert s._pause_search.button.text == 'Pausing...'
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
    assert s._pause_search.button.text == 'Resume'
    assert s._pause_search.button.enabled is True
    assert s._pause_search.pause_started > 0.0
    assert s._search_elapsed_timer.running is False


@pytest.mark.parametrize('run_id,epoch', [(99, 1), (1, 99), (99, 99)])
def test_stale_acks_are_ignored(running_search, run_id, epoch):
    s = running_search
    s._on_pause_clicked(s._pause_search)
    s._on_pause_ack(s._pause_search, run_id, epoch)

    assert s._pause_search.state == 'pausing'
    assert s._pause_search.button.text == 'Pausing...'


# ------------------------------------------------------------ resume -> running

def test_resume_banks_the_parked_time_and_restarts_the_ticker(running_search):
    s = running_search
    s._on_pause_clicked(s._pause_search)
    s._on_pause_ack(s._pause_search, 1, 1)
    s._on_pause_clicked(s._pause_search)

    assert s.search_thread.calls == ['pause', 'resume']
    assert s._pause_search.state == 'running'
    assert s._pause_search.paused_total > 0.0
    assert s._pause_search.pause_started == 0.0
    assert s._pause_search.button.text == 'Pause'
    assert s._search_elapsed_timer.running is True


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
    s = running_search
    s._on_pause_clicked(s._pause_search)
    s._on_pause_ack(s._pause_search, 1, 1)
    assert s._pause_search.button.text == 'Resume'

    s._apply_pause_state(s._pause_search, 'hidden')
    assert s._pause_search.button.visible is False
    assert s._pause_search.button.text == 'Pause'


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
