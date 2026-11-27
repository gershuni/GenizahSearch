# -*- coding: utf-8 -*-
"""GS-14, PR 5: the Composition tab's scan and grouping are never waited for on the UI
thread and never terminated (desktop/search_run.py::comp_run_of; owner rulings D7, D8).

Stop during grouping drops the grouping and shows the ungrouped results at once, with
the grouping slot cleared before they render (Codex F1). New, a new run and Close
discard and hold. At most one stopped worker may still be finishing when a run or a
grouping starts, from every grouping caller (Codex F3). A held letter-level scan still
holds a close back, and every held worker holds the index install back.

The workers are stand-ins with real Qt signals whose wait() and terminate() fail the
test.
"""
import types

import pytest

pytestmark = pytest.mark.gui  # imports PyQt6: gui bucket only

from PyQt6.QtCore import QObject, pyqtSignal  # noqa: E402
from PyQt6.QtWidgets import QApplication  # noqa: E402

import genizah_app  # noqa: E402
from desktop.search_run import comp_run_of  # noqa: E402
from genizah_app import GenizahGUI  # noqa: E402

_APP = QApplication.instance() or QApplication([])


class _Grouping(QObject):
    """A GroupingThread: no request_cancel, it reads isInterruptionRequested()."""
    progress_signal = pyqtSignal(int, int)
    status_signal = pyqtSignal(str)
    finished_signal = pyqtSignal(list, dict, dict, list, dict, dict)
    error_signal = pyqtSignal(str)
    finished = pyqtSignal()

    def __init__(self):
        super().__init__()
        self.alive, self.interrupted = True, False

    def isRunning(self):
        return self.alive

    def requestInterruption(self):
        self.interrupted = True

    def wait(self, *a):
        raise AssertionError("the UI thread waited for the grouping")

    def terminate(self):
        raise AssertionError("the grouping was terminated")

    def start(self):
        pass

    def end(self):
        self.alive = False
        self.finished.emit()


class _Scan(_Grouping):
    """A Composition scan: it has request_cancel (the pausable mixin)."""

    def __init__(self):
        super().__init__()
        self.cancel_requested = False

    def request_cancel(self):
        self.cancel_requested = True


class _Bar:
    def __init__(self):
        self.format, self.visible = '', False

    def setFormat(self, text):
        self.format = text

    def setVisible(self, on):
        self.visible = on

    def __getattr__(self, name):
        return lambda *a, **k: None


class _Host:
    """Borrows the real methods under test."""
    toggle_composition = GenizahGUI.toggle_composition
    start_grouping = GenizahGUI.start_grouping
    _drain_previous_worker = GenizahGUI._drain_previous_worker
    _comp_still_stopping = GenizahGUI._comp_still_stopping
    _discardable = GenizahGUI._discardable
    _deliver_unless_discarded = GenizahGUI._deliver_unless_discarded
    _passage_workers_busy = GenizahGUI._passage_workers_busy
    _passage_scan_in_flight = GenizahGUI._passage_scan_in_flight
    _index_readers_busy = GenizahGUI._index_readers_busy
    _export_running = GenizahGUI._export_running

    def __init__(self):
        self.is_comp_running = False
        self._comp_grouping_active = False
        self._comp_last_result_method = 'chunk'
        self._comp_new_generation = 0
        self._pause_comp = types.SimpleNamespace(state='idle')
        self.comp_progress = _Bar()
        self.btn_comp_run = _Bar()
        self.spin_filter = types.SimpleNamespace(value=lambda: 5)
        self.searcher = object()
        self.comp_raw_items, self.comp_raw_filtered = ['row'], []
        self.shown = []

    def _apply_pause_state(self, *a):
        pass

    def _refresh_witness_panel(self):
        pass

    def _stop_auto_expand(self, msg):
        pass

    def on_comp_progress(self, curr, total):
        pass

    def _refuse_stop_during_passage_scan(self):
        return False

    def display_comp_results(self, *args):
        # What Codex F1 is about: the slot is already clear when the results render.
        self.shown.append((args, getattr(self, 'group_thread', 'unset')))


@pytest.fixture
def no_message_box(monkeypatch):
    boxes = []
    monkeypatch.setattr(genizah_app.QMessageBox, 'information',
                        staticmethod(lambda *a: boxes.append(a[2])))
    return boxes


def test_stop_during_grouping_never_waits_and_shows_the_ungrouped_results(no_message_box):
    host = _Host()
    host.is_comp_running = True
    host.group_thread = grouping = _Grouping()
    late = []
    grouping.finished_signal.connect(lambda *a: late.append(a))
    host.toggle_composition()                                   # Stop
    assert grouping.interrupted
    assert host.group_thread is None
    assert [slot for _args, slot in host.shown] == [None], 'rendered with the slot still set'
    assert host.shown[0][0][0] == ['row'], 'the ungrouped results were not shown'
    assert no_message_box == [genizah_app.tr("Grouping stopped. Showing ungrouped results.")]  # D8 (a)
    assert comp_run_of(host).running() == [grouping], 'not held while it ends'
    grouping.finished_signal.emit([], {}, {}, [], {}, {})       # its result, late
    assert late == [], 'the dropped grouping still delivered'
    grouping.end()
    assert comp_run_of(host).running() == []


def _hold(host, n):
    """n stopped workers still finishing."""
    workers = [_Scan() for _ in range(n)]
    for w in workers:
        comp_run_of(host).discard(w)
    return workers


@pytest.mark.parametrize('held, starts', [(0, True), (1, True), (2, False)])
def test_a_grouping_start_admits_through_the_holder(monkeypatch, held, starts):
    """Every caller of start_grouping (the scan's end, a display-mode change, a header
    click): with two stopped workers still finishing, a third is refused (Codex F3)."""
    made = []
    monkeypatch.setattr(genizah_app, 'GroupingThread',
                        lambda *a, **k: made.append(_Grouping()) or made[-1])
    host = _Host()
    _hold(host, held)
    host.start_grouping(['row'], [])
    if starts:
        assert len(made) == 1 and host.group_thread is made[0]
        assert host.shown == []
    else:
        assert made == [], 'a third worker started on the index'
        assert host.shown and host.shown[0][0][0] == ['row'], 'refused, and showed nothing'
        assert host.comp_progress.format == genizah_app.tr(
            "Still stopping the previous search — try again in a moment.")


def test_a_grouping_restarted_over_a_running_one_discards_and_holds_it(monkeypatch):
    made = []
    monkeypatch.setattr(genizah_app, 'GroupingThread',
                        lambda *a, **k: made.append(_Grouping()) or made[-1])
    host = _Host()
    host.group_thread = old = _Grouping()
    late = []
    old.finished_signal.connect(lambda *a: late.append(a))
    host.start_grouping(['row'], [])                            # a header click
    assert old.interrupted and host.group_thread is made[0]
    assert comp_run_of(host).running() == [old]
    old.finished_signal.emit([], {}, {}, [], {}, {})
    assert late == []
    old.end()


def test_a_grouping_progress_from_a_discarded_run_does_not_move_the_bar(monkeypatch):
    """Codex F2: the progress connection carries the New guard too."""
    made = []
    monkeypatch.setattr(genizah_app, 'GroupingThread',
                        lambda *a, **k: made.append(_Grouping()) or made[-1])
    host = _Host()
    moved = []
    host.on_comp_progress = lambda c, t: moved.append((c, t))
    host.start_grouping(['row'], [])
    host._comp_new_generation += 1                              # New
    made[0].progress_signal.emit(3, 10)
    _APP.processEvents()
    assert moved == []


@pytest.mark.parametrize('held', [0, 1])
def test_a_new_run_never_waits_for_the_previous_one(held):
    """The previous run still going is discarded and held; with one stopped worker
    already finishing, that would make two, so the run is refused instead."""
    host = _Host()
    _hold(host, held)
    host.comp_thread = prev = _Scan()
    before = host._comp_new_generation
    ok = host._drain_previous_worker('comp_thread', host._pause_comp)
    if held == 0:
        assert ok is True and prev.cancel_requested
        assert host._comp_new_generation == before + 1, 'what it queued is not dropped'
        assert prev in comp_run_of(host).running()
    else:
        assert ok is False and not prev.cancel_requested
        assert host.comp_progress.format == genizah_app.tr(
            "Still stopping the previous search — try again in a moment.")


def test_a_held_letter_level_scan_still_holds_a_close_back():
    host = _Host()
    scan = _Scan()
    comp_run_of(host).discard(scan, tag='passage')
    assert host._passage_workers_busy() == [genizah_app.tr("a letter-level search is running")]
    scan.end()
    assert host._passage_workers_busy() == []


def test_a_held_composition_worker_holds_the_index_install_back():
    host = _Host()
    scan = _Scan()
    comp_run_of(host).discard(scan)
    assert 'composition' in host._index_readers_busy()
    scan.end()
    assert 'composition' not in host._index_readers_busy()


def test_discarding_a_held_worker_again_stops_nothing_twice():
    host = _Host()
    scan = _Scan()
    run = comp_run_of(host)
    assert run.discard(scan) is True
    scan.cancel_requested = False
    assert run.discard(scan) is False and not scan.cancel_requested
    assert run.running() == [scan]
    scan.end()


@pytest.mark.parametrize('method', ['closeEvent', '_browse_exit_reading_desk',
                                    '_lists_cancel_preview_image_thread', 'display_comp_results'])
def test_no_wait_on_the_threads_these_paths_give_up(method):
    """The Browse image threads, the Reading Desk enrichment and the lists preview are
    held until they end (_keep_until_finished), not waited for (GS-14, PR 5)."""
    import inspect
    import re
    src = inspect.getsource(getattr(GenizahGUI, method))
    assert not re.search(r'\b(t|preview_thread|group_thread)\.wait\(', src), method


def test_close_drops_what_the_scan_still_delivers():
    """Codex on #156: the scan's result signal is guarded by the New generation, not cut
    by the holder, so Close moves the generation before it discards, as New does."""
    import inspect
    src = inspect.getsource(GenizahGUI.closeEvent)
    assert "_comp_new_generation = getattr(self, '_comp_new_generation', 0) + 1" in src
    assert src.index('_comp_new_generation = ') < src.index('comp_run.discard(')
