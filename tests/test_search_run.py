# -*- coding: utf-8 -*-
"""GS-14: Stop, New, a new search and Close never wait for the search worker, and
never terminate it (desktop/search_run.py; owner ruling D1, "detach").

Stop asks the worker to stop and lets it finish in the background: what it found
so far still arrives as partial results. New and a new search over a running one
also stop listening to it. At most one stopped worker may still be finishing when
a new run starts, and the index install waits for the stopped ones.

The workers here are stand-ins with real Qt signals whose wait() and terminate()
fail the test: the UI thread must call neither.
"""
from unittest.mock import MagicMock

import pytest

pytestmark = pytest.mark.gui  # imports PyQt6: gui bucket only

from PyQt6.QtCore import QObject, pyqtSignal  # noqa: E402
from PyQt6.QtWidgets import QApplication, QMainWindow  # noqa: E402

import genizah_app  # noqa: E402
from desktop.search_run import SearchRun, search_run_of  # noqa: E402
from genizah_app import GenizahGUI  # noqa: E402

_APP = QApplication.instance() or QApplication([])


class _Worker(QObject):
    """A search worker that keeps running until the test ends it."""
    results_signal = pyqtSignal(object)
    error_signal = pyqtSignal(str)
    progress_signal = pyqtSignal(int, int)
    finished = pyqtSignal()

    def __init__(self):
        super().__init__()
        self.alive = True
        self.cancel_requested = False

    def isRunning(self):
        return self.alive

    def request_cancel(self):
        self.cancel_requested = True

    def wait(self, *a):
        raise AssertionError("the UI thread waited for the search worker")

    def terminate(self):
        raise AssertionError("the search worker was terminated")

    def end(self):
        self.alive = False
        self.finished.emit()


# -- SearchRun ---------------------------------------------------------------

def test_stop_asks_the_worker_to_stop_and_holds_it_while_it_finishes():
    run, w = SearchRun(), _Worker()
    got = []
    w.results_signal.connect(got.append)
    assert run.stop(w) is True
    assert w.cancel_requested
    assert run.running() == [w], "the stopped worker is no longer tracked while it runs"
    w.results_signal.emit(['partial'])
    assert got == [['partial']], "Stop must still deliver what the worker found"
    w.end()
    assert run.running() == []


def test_discard_drops_what_the_worker_delivers_later():
    run, w = SearchRun(), _Worker()
    got = []
    w.results_signal.connect(lambda r: got.append(r))
    w.error_signal.connect(lambda e: got.append(e))
    assert run.discard(w) is True
    assert w.cancel_requested
    w.results_signal.emit(['late'])
    w.error_signal.emit('late error')
    assert got == [], "a discarded run still wrote the window"
    assert run.running() == [w]


def test_a_worker_that_is_not_running_is_left_alone():
    run, w = SearchRun(), _Worker()
    w.alive = False
    assert run.stop(w) is False and run.discard(w) is False and run.stop(None) is False
    assert not w.cancel_requested and run.running() == []


def test_at_most_one_stopped_worker_may_still_be_finishing_when_a_run_starts():
    run, first, second = SearchRun(), _Worker(), _Worker()
    assert run.can_start(None)
    assert run.can_start(first), "replacing one running worker is allowed"
    run.discard(first)
    assert run.can_start(None), "one stopped worker finishing: a new run may start"
    assert not run.can_start(second), "a second still running: the third run waits"
    first.end()
    assert run.can_start(second)


# -- the window's call sites ---------------------------------------------------

def _window(worker=None):
    w = GenizahGUI.__new__(GenizahGUI)
    QMainWindow.__init__(w)
    w.search_thread = worker
    w._pause_search = MagicMock()
    w._apply_pause_state = lambda ctx, state: None
    w.status_label = MagicMock()
    w.reset_ui = lambda: setattr(w, 'is_searching', False)
    w.is_searching = True
    w.telemetry = []
    w._emit_search_telemetry = lambda action, *a: w.telemetry.append(action)
    return w


def test_stop_returns_at_once_and_the_partial_results_still_arrive():
    worker = _Worker()
    w = _window(worker)
    got = []
    worker.results_signal.connect(got.append)
    w.stop_search()                        # _Worker.wait / terminate would fail here
    assert worker.cancel_requested and w._search_was_cancelled
    assert w.is_searching is False and w.telemetry == ['cancelled']
    worker.results_signal.emit(['partial'])
    assert got == [['partial']]
    assert search_run_of(w).running() == [worker]


def test_a_new_run_discards_the_running_one_without_waiting():
    old = _Worker()
    w = _window(old)
    got = []
    old.results_signal.connect(lambda r: got.append(r))
    assert w._free_search_slot() is True
    assert old.cancel_requested
    old.results_signal.emit(['stale'])
    assert got == [], "the old run's late results reached the new one"


@pytest.mark.parametrize('still_running', [True, False], ids=['running', 'already_ended'])
def test_what_the_old_run_already_queued_is_dropped_when_a_new_run_starts(still_running):
    """Codex on #147: a disconnect does not reach a delivery Qt has already queued, and a
    run that has just ended is not disconnected at all. Freeing the slot moves the run's
    generation on, so the queued results and error are dropped on arrival."""
    old = _Worker()
    old.alive = still_running
    w = _window(old)
    got = []
    queued_results = w._discardable('_search_new_generation', got.append)   # bound when the old run started
    queued_error = w._discardable('_search_new_generation', got.append)
    assert w._free_search_slot() is True
    queued_results(['stale rows'])      # Qt delivering what was queued before the new run
    queued_error('stale error')
    assert got == [], "the old run's queued delivery reached the new run"


def test_a_third_run_waits_while_two_are_still_finishing():
    stopped, current = _Worker(), _Worker()
    w = _window(current)
    search_run_of(w).stop(stopped)
    assert w._free_search_slot() is False
    assert not current.cancel_requested, "the running search was stopped although nothing could start"
    w.status_label.setText.assert_called_once()
    stopped.end()
    assert w._free_search_slot() is True


def test_the_index_install_waits_for_a_stopped_search():
    stopped = _Worker()
    w = _window(None)
    w.is_searching = False
    search_run_of(w).stop(stopped)
    assert 'search' in w._index_readers_busy()
    stopped.end()
    assert 'search' not in w._index_readers_busy()


# -- the Search press (Codex on #147) -------------------------------------------

VARIANTS = 1    # the mode combo's row


def _pressing(monkeypatch, var_mgr=None, settings=None):
    """The window as start_search reads it (a MagicMock with the real method bound;
    the pattern of tests/test_native_matching_desktop_gap.py), a Variants search typed."""
    from types import SimpleNamespace
    w = MagicMock()
    w.MODE_RESPONSA, w.MODE_PGP_TAGS = 2, 7
    w.query_input.text.return_value = "שלום"
    w._pre_search_filters_blocked.return_value = False
    w.searcher = SimpleNamespace(parse_query_syntax=lambda q, responsa_mode=False: (None, q), var_mgr=var_mgr)
    w.mode_combo.currentIndex.return_value = VARIANTS
    w.btn_lab_mode_toggle.isChecked.return_value = False
    w.gap_input.text.return_value = "0"
    w.exclude_input.text.return_value = ""
    w.corpus_scope_combo.currentData.return_value = "genizah"
    w._refine_mode, w.refinement_chain, w.var_mgr, w.meta_loader = False, [], var_mgr, None
    w._lab_settings.return_value = settings
    w._get_current_variant_pairs_count.return_value = 30
    w.export_buttons, w._run_seq, w.is_searching = [], 0, False
    w.pre_search_restrict_sys_ids = w.refinement_restrict_sys_ids = None
    w._current_search_run = {"mode": "the previous run"}
    w._free_search_slot.return_value = True
    search = MagicMock(name="SearchThread")
    monkeypatch.setattr(genizah_app, "SearchThread", search)
    return w, search


def test_a_refused_press_writes_none_of_its_runs_state(monkeypatch):
    """Two searches still finishing: the press is refused before it writes its run's
    state -- or a stopped search delivering later is shown and counted as this one."""
    w, search = _pressing(monkeypatch)
    w._free_search_slot.return_value = False
    w.last_search_query, w.refinement_chain = "the stopped one", ["its step"]
    w._refine_mode = False
    GenizahGUI.start_search(w)
    assert w._current_search_run == {"mode": "the previous run"}
    assert (w.last_search_query, w.refinement_chain) == ("the stopped one", ["its step"])
    assert search.call_count == 0


def test_each_search_expands_variants_with_settings_of_its_own(monkeypatch):
    """A stopped search may still expand words while the next one starts and sets the
    shared variant settings from the UI: each runs on a view of the engine with its own."""
    from types import SimpleNamespace

    from shared.variants import VariantManager
    settings = SimpleNamespace(variant_pairs_count=30, variant_max_changes=2)
    shared = VariantManager(settings=settings)
    w, search = _pressing(monkeypatch, var_mgr=shared, settings=settings)
    GenizahGUI.start_search(w)
    engine = search.call_args[0][0]
    assert engine is not w.searcher and engine.var_mgr is not shared
    shared.set_variant_level(150)          # the next search, from the UI
    settings.variant_max_changes = 3
    assert engine.var_mgr.get_variant_level() == 30, "the running search's level changed under it"
    assert engine.var_mgr._settings.variant_max_changes == 2


def test_the_module_never_waits_on_or_terminates_a_worker():
    """SearchRun itself calls no wait() or terminate() (an invariant, read from its code)."""
    import ast
    import inspect

    import desktop.search_run as module
    calls = {node.func.attr for node in ast.walk(ast.parse(inspect.getsource(module)))
             if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)}
    assert not calls & {'wait', 'terminate'}, calls & {'wait', 'terminate'}
    assert genizah_app.search_run_of is search_run_of
