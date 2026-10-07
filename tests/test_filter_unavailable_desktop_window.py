# -*- coding: utf-8 -*-
"""Desktop main window: filters that could not be applied never become a scope (#17).

Drives the real GenizahGUI methods on a window built with
GenizahGUI.__new__ (the pattern tests/test_exclusion_surfaces.py uses), so
__init__ never runs. Covers the catalog hand-offs, the failure slot for the
FilterCountWorker, the search and composition gates, stale workers, and
(K-14) a search pressed while the filter lookup is still running: it waits
for the lookup and runs on ITS answer, never on the old or None scope.

No test lets an exception escape a slot invoked by a signal (PyQt6 aborts
the process on that); the stubs past each gate are called directly.
"""
import os
import sys

import pytest

pytestmark = pytest.mark.gui

from PyQt6.QtCore import QObject, pyqtSignal  # noqa: E402
from PyQt6.QtWidgets import QApplication, QLineEdit, QMainWindow, QPlainTextEdit  # noqa: E402

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from fjms_filter_sidecar import FailingFinalExecute, build_sidecar  # noqa: E402

_app = QApplication.instance() or QApplication([])

import genizah_app  # noqa: E402
from genizah_app import GenizahGUI  # noqa: E402
from shared import fjms_service  # noqa: E402
from shared.fjms_service import FjmsService  # noqa: E402


class _Stop(Exception):
    """Raised by a stub placed just past the gate: reaching it means the run started."""


@pytest.fixture
def warned(monkeypatch):
    seen = []
    monkeypatch.setattr(genizah_app.QMessageBox, "warning",
                        staticmethod(lambda *a, **k: seen.append(a[2] if len(a) > 2 else a)))
    return seen


def _bare_window():
    w = GenizahGUI.__new__(GenizahGUI)
    QMainWindow.__init__(w)
    w.meta_mgr = None
    w.refinement_chain = []
    w._update_filter_chip_bar = lambda: None
    w._schedule_session_save = lambda: None
    w._set_active_tab = lambda i: None
    return w


# -- catalog hand-offs --

@pytest.fixture
def failing_singleton(monkeypatch, tmp_path):
    svc = FjmsService(db_path=build_sidecar(tmp_path / "s.db"))
    svc._conn = FailingFinalExecute(svc._conn)
    monkeypatch.setattr(fjms_service, "_default_service", svc)


@pytest.fixture
def absent_singleton(monkeypatch, tmp_path):
    monkeypatch.setattr(fjms_service, "_default_service",
                        FjmsService(db_path=str(tmp_path / "absent.db")))


def _catalog_window():
    w = _bare_window()
    w._catalog_current_domain = None
    w._catalog_current_author = None
    w._catalog_current_work = None
    w._catalog_date_from = 1000
    w._catalog_date_to = 1100
    w._catalog_library_filter = []
    w._catalog_library_mode = 'show_only'
    w.pre_search_filters = {'domains': ['Piyyut']}
    w.pre_search_restrict_sys_ids = {'990009'}
    return w


@pytest.mark.parametrize("method", ["_catalog_search_in_results", "_catalog_parallels_in_results"])
@pytest.mark.parametrize("sidecar", ["failing_singleton", "absent_singleton"])
def test_catalog_hand_off_that_cannot_apply_its_filters_changes_nothing(
        request, warned, method, sidecar):
    request.getfixturevalue(sidecar)
    w = _catalog_window()
    getattr(w, method)()
    assert w.pre_search_filters == {'domains': ['Piyyut']}, (
        "the browse filters replaced the search filters although they could not be applied")
    assert w.pre_search_restrict_sys_ids == {'990009'}, w.pre_search_restrict_sys_ids
    assert warned, "the user was not told"


def test_catalog_hand_off_that_works_still_scopes(monkeypatch, tmp_path, warned):
    monkeypatch.setattr(fjms_service, "_default_service",
                        FjmsService(db_path=build_sidecar(tmp_path / "ok.db")))
    w = _catalog_window()
    w._catalog_search_in_results()
    assert w.pre_search_filters == {'date_from': 1000, 'date_to': 1100}
    assert w.pre_search_restrict_sys_ids == {'990001'}
    assert not warned


# -- the failure slot --

def test_lookup_failure_cancels_both_deferred_reruns(warned):
    w = _bare_window()
    w.pre_search_filters = {'date_from': 1000}
    w.pre_search_restrict_sys_ids = {'990001'}
    w._rerun_search_after_filter = True
    w._rerun_comp_after_filter = True
    w._on_filter_lookup_failed('query_failed')
    assert w._pre_search_filter_error == 'query_failed'
    assert w.pre_search_restrict_sys_ids is None
    assert w._rerun_search_after_filter is False
    assert w._rerun_comp_after_filter is False, (
        "a composition history re-run stayed armed and fires on the next recompute")
    assert len(warned) == 1


class _FakeWorker(QObject):
    finished = pyqtSignal(object)
    failed = pyqtSignal(str)


def test_a_superseded_worker_cannot_block_or_rescope(warned):
    """Only the most recent pre-search recompute may set the scope or the error."""
    w = _bare_window()
    w.pre_search_filters = {'date_from': 1000}
    w.pre_search_restrict_sys_ids = None
    old, new = _FakeWorker(), _FakeWorker()
    w._connect_filter_worker(old, w._on_filter_recompute_finished)
    w._connect_filter_worker(new, w._on_filter_recompute_finished)
    new.finished.emit({'990001'})
    old.failed.emit('query_failed')
    old.finished.emit({'990777'})
    assert getattr(w, '_pre_search_filter_error', None) is None
    assert w.pre_search_restrict_sys_ids == {'990001'}


# -- the gates --

def _search_window(error):
    w = _bare_window()
    w.query_input = QLineEdit("word")
    w.pre_search_filters = {'date_from': 1000}
    w.pre_search_restrict_sys_ids = None
    if error is not None:
        w._pre_search_filter_error = error

    def _past_the_gate(*a, **k):
        raise _Stop()
    w._set_local_scope_strip_visible = _past_the_gate
    return w


def test_search_does_not_run_while_its_filters_could_not_be_applied(warned):
    w = _search_window('sidecar_unavailable')
    try:
        w.start_search()
    except _Stop:
        pytest.fail("start_search ran although its filters could not be applied")
    assert warned


def test_search_gate_tolerates_a_window_built_without_init(warned):
    """test_exclusion_surfaces builds its window without __init__, so the
    error flag may not exist; the gate must read it with a default."""
    w = _search_window(None)
    with pytest.raises(_Stop):
        w.start_search()
    assert not warned


def _comp_window():
    w = _bare_window()
    w.comp_text_area = QPlainTextEdit("some composition text")
    w.pre_search_filters = {'date_from': 1000}

    class _Title:
        def text(self):
            raise _Stop()
    w.comp_title_input = _Title()
    return w


def test_composition_does_not_run_while_its_filters_could_not_be_applied(warned):
    w = _comp_window()
    w._pre_search_filter_error = 'query_failed'
    try:
        w.run_composition()
    except _Stop:
        pytest.fail("run_composition ran although its filters could not be applied")
    assert warned


def test_saved_filters_without_a_catalog_block_the_search(warned, tmp_path, monkeypatch):
    """Owner ruling: saved filters that cannot be checked at all (no catalog)
    block the search with a message. Drives the REAL FilterCountWorker
    through the window's own connection."""
    from desktop.gui_threads import FilterCountWorker
    monkeypatch.setenv("GENIZAH_FJMS_DB_PATH", str(tmp_path / "absent.db"))
    w = _search_window(None)
    worker = FilterCountWorker(dict(w.pre_search_filters))
    w._connect_filter_worker(worker, w._on_restore_filter_finished)
    worker.run()   # synchronously: emits failed('sidecar_unavailable')
    assert w._pre_search_filter_error == 'sidecar_unavailable'
    try:
        w.start_search()
    except _Stop:
        pytest.fail("the search ran on filters that could not be checked")
    assert warned == [w._filter_unavailable_text('sidecar_unavailable')]


# -- K-14: a search pressed while the lookup is still running waits for it --

def test_search_waits_for_a_pending_filter_lookup_then_runs_on_its_answer(warned):
    w = _search_window(None)
    worker = _FakeWorker()
    w._connect_filter_worker(worker, w._on_restore_filter_finished)
    try:
        w.start_search()
    except _Stop:
        pytest.fail("start_search ran on the old scope while the filter lookup was running")
    assert w._rerun_search_after_filter is True
    assert not warned
    ran = []
    w.start_search = lambda: ran.append(w.pre_search_restrict_sys_ids)
    worker.finished.emit({'990001'})
    assert ran == [{'990001'}], "the waiting search did not run on the lookup's answer"
    assert w._filter_lookup_pending is False


def test_composition_waits_for_a_pending_filter_lookup_then_runs(warned):
    w = _comp_window()
    worker = _FakeWorker()
    w._connect_filter_worker(worker, w._on_restore_filter_finished)
    try:
        w.run_composition()
    except _Stop:
        pytest.fail("run_composition ran on the old scope while the filter lookup was running")
    assert w._rerun_comp_after_filter is True
    ran = []
    w.run_composition = lambda **k: ran.append((k, w.pre_search_restrict_sys_ids))
    worker.finished.emit({'990002'})
    assert ran == [({}, {'990002'})]


def test_a_waiting_search_is_cancelled_with_a_message_when_the_lookup_fails(warned):
    w = _search_window(None)
    worker = _FakeWorker()
    w._connect_filter_worker(worker, w._on_restore_filter_finished)
    try:
        w.start_search()
    except _Stop:
        pytest.fail("start_search ran while the filter lookup was running")
    worker.failed.emit('query_failed')
    assert w._rerun_search_after_filter is False
    assert w._pre_search_filter_error == 'query_failed'
    assert len(warned) == 1
    try:
        w.start_search()
    except _Stop:
        pytest.fail("start_search ran after its filter lookup failed")
    assert len(warned) == 2


# -- a restored history entry never shares a list with the live filters --

class _IdleWorker(_FakeWorker):
    """Stands in for FilterCountWorker: connected like the real one, never run."""

    def __init__(self, *a, **k):
        super().__init__()

    def start(self):
        pass


def _history_entry():
    return {'query': 'word', 'state': {'source_text': 'a b c d e f'},
            'pre_search_filters': {'include_mode': True, 'domains': ['Halakha', 'Piyyut'],
                                   'text_all': ['word']}}


@pytest.mark.parametrize("restore", ["_restore_regular_search_from_state",
                                     "_restore_comp_search_from_state"])
def test_removing_a_restored_chip_leaves_the_history_entry_alone(monkeypatch, tmp_path, restore):
    """Restore a history entry with domains=['Halakha', ...], then remove that
    chip: _remove_filter edits the live list IN PLACE, so a restore that
    shared the entry's lists rewrote the saved history (domains lost
    'Halakha'). Drives the real restore and the real chip removal."""
    monkeypatch.setattr(fjms_service, "_default_service",
                        FjmsService(db_path=str(tmp_path / "absent.db")))
    monkeypatch.setattr(genizah_app, "FilterCountWorker", _IdleWorker)
    w = _bare_window()
    w.query_input = QLineEdit()
    w._set_local_scope_strip_visible = lambda visible: None
    w.comp_text_area = QPlainTextEdit()
    w.comp_title_input = QLineEdit()
    w._witness_edits_are_locked = lambda: False
    w._restore_comp_passage_preferences = lambda params: None
    w.composition_tab = None
    entry = _history_entry()
    getattr(w, restore)(entry['state'], entry)
    assert w.pre_search_filters == entry['pre_search_filters'], "the restore lost a filter"
    w._remove_filter(('domains', 'Halakha'))
    w._remove_filter(('text_all', 'word'))
    assert w.pre_search_filters == {'include_mode': True, 'domains': ['Piyyut']}
    assert entry == _history_entry(), (
        f"removing a chip changed the saved history entry: {entry['pre_search_filters']}")


def test_dialog_ok_supersedes_a_pending_lookup(monkeypatch, warned):
    """A scope the dialog computed replaces any lookup still running; the
    late answer of the old one must not overwrite it."""
    w = _bare_window()
    w.pre_search_filters = {'date_from': 1000}
    old = _FakeWorker()
    w._connect_filter_worker(old, w._on_filter_recompute_finished)

    class _Dialog:
        def __init__(self, *a, **k):
            pass

        def exec(self):
            return genizah_app.QDialog.DialogCode.Accepted

        def get_filters(self):
            return {'include_mode': True, 'date_from': 1100}

        def get_restrict_sys_ids(self):
            return {'990002'}

    monkeypatch.setattr(genizah_app, 'PreSearchFilterDialog', _Dialog)
    w._open_pre_search_filter_dialog()
    assert w._filter_lookup_pending is False
    old.finished.emit({'990777'})
    assert w.pre_search_restrict_sys_ids == {'990002'}


# -- GitHub review (Codex on #387, 2026-10-07): replacing the filters cancels a waiting run --

class _AcceptedDialog:
    def __init__(self, *a, **k):
        pass

    def exec(self):
        return genizah_app.QDialog.DialogCode.Accepted

    def get_filters(self):
        return {'include_mode': True, 'date_from': 1100}

    def get_restrict_sys_ids(self):
        return {'990002'}


def test_replacing_the_filters_cancels_a_waiting_search(monkeypatch, warned):
    """Search pressed while a lookup runs waits for it. When the filters are then
    replaced (dialog OK), the wait is cancelled and the status bar says so: left
    armed, the search started when a later, unrelated lookup answered."""
    w = _search_window(None)
    w._connect_filter_worker(_FakeWorker(), w._on_filter_recompute_finished)
    try:
        w.start_search()
    except _Stop:
        pytest.fail("start_search ran on the old scope while the filter lookup was running")
    assert w._rerun_search_after_filter is True
    monkeypatch.setattr(genizah_app, 'PreSearchFilterDialog', _AcceptedDialog)
    w._open_pre_search_filter_dialog()
    assert w._rerun_search_after_filter is False
    assert w.statusBar().currentMessage() == 'Search cancelled'
    ran = []
    w.start_search = lambda: ran.append(w.pre_search_restrict_sys_ids)
    later = _FakeWorker()
    w._connect_filter_worker(later, w._on_filter_recompute_finished)
    later.finished.emit({'990003'})
    assert ran == [], 'a search cancelled with its filters started at a later lookup'
    assert not warned


def test_removing_the_last_filter_cancels_a_waiting_composition(warned):
    w = _comp_window()
    w._connect_filter_worker(_FakeWorker(), w._on_filter_recompute_finished)
    try:
        w.run_composition(custom_text='other text')
    except _Stop:
        pytest.fail("run_composition ran on the old scope while the filter lookup was running")
    assert w._rerun_comp_after_filter is True
    w._remove_filter('date_from')
    assert w.pre_search_filters == {}
    assert (w._rerun_comp_after_filter, w._rerun_comp_custom_text) == (False, None)
    ran = []
    w.run_composition = lambda **k: ran.append(k)
    w._run_deferred_after_filter()
    assert ran == []


def test_search_and_composition_both_waiting_both_run(warned):
    """GitHub review (Codex on #387, round 3): Search, then Composition, pressed
    while one lookup runs: both wait. When it answers, both run, and neither stays
    armed for a later, unrelated lookup."""
    w = _search_window(None)
    w.comp_text_area = QPlainTextEdit("some composition text")

    class _Title:
        def text(self):
            raise _Stop()
    w.comp_title_input = _Title()
    worker = _FakeWorker()
    w._connect_filter_worker(worker, w._on_filter_recompute_finished)
    for start in (w.start_search, w.run_composition):
        try:
            start()
        except _Stop:
            pytest.fail("a run started on the old scope while the filter lookup was running")
    assert (w._rerun_search_after_filter, w._rerun_comp_after_filter) == (True, True)
    ran = []
    w.start_search = lambda: ran.append('search')
    w.run_composition = lambda **k: ran.append('composition')
    worker.finished.emit({'990001'})
    assert ran == ['search', 'composition']
    assert (w._rerun_search_after_filter, w._rerun_comp_after_filter) == (False, False)
    later = _FakeWorker()
    w._connect_filter_worker(later, w._on_filter_recompute_finished)
    later.finished.emit({'990002'})
    assert ran == ['search', 'composition'], 'nothing starts at a later lookup'
