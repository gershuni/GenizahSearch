# -*- coding: utf-8 -*-
"""Letter-level search for My Library: the window's and the My Library tab's
glue -- the staleness notice after a scan, the build preflight off the UI
thread, and the My Library readiness the gate reads.

Qt-free, in the style of `tests/test_desktop_passage_gate.py`: the methods
under test are bound UNBOUND onto stub classes, so nothing here needs a
QApplication. The two QThread workers are run with `.run()` on this thread.
"""
from __future__ import annotations

import inspect
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import desktop.passage_lifecycle as pl        # noqa: E402
import desktop.passage_local as pll           # noqa: E402
import genizah_app                            # noqa: E402

APP = genizah_app.GenizahGUI


class _Label:
    def __init__(self):
        self.text = None

    def setText(self, t):
        self.text = t


class _Thread:
    def __init__(self, running):
        self._running = running

    def isRunning(self):
        return self._running


# ---------------------------------------------------------------------------
# The staleness notice after every scan (owner, 2026-10-16).
# ---------------------------------------------------------------------------

class _FreshWin:
    _on_my_library_changed = APP._on_my_library_changed
    _on_local_freshness = APP._on_local_freshness

    def __init__(self):
        self.status_label = _Label()
        self.checks = 0

    def _start_local_freshness_check(self):
        self.checks += 1


def test_a_library_change_rechecks_the_index():
    w = _FreshWin()
    w._on_my_library_changed()
    assert w.checks == 1


@pytest.mark.parametrize('attr', ['_close_pending',
                                  '_local_passage_build_in_flight'])
def test_no_recheck_while_closing_or_building(attr):
    """A build in flight is checked when it installs instead; it never
    rebuilds anything either way."""
    w = _FreshWin()
    setattr(w, attr, True)
    w._on_my_library_changed()
    assert w.checks == 0


def test_a_stale_answer_says_so():
    w = _FreshWin()
    w._on_local_freshness(pl.STALE)
    assert w.status_label.text and 'Settings' in w.status_label.text
    w2 = _FreshWin()
    w2._on_local_freshness(pl.FRESH)
    assert w2.status_label.text is None


def test_a_change_during_a_check_runs_it_again_before_answering():
    """The running check may have read the library before the change: its
    answer is not shown, and a fresh check runs instead."""
    class _W(_FreshWin):
        _start_local_freshness_check = APP._start_local_freshness_check

    w = _W()
    w.local_passage_freshness_thread = _Thread(running=True)
    w._start_local_freshness_check()
    assert w._local_freshness_recheck is True

    restarted = []
    w._start_local_freshness_check = lambda: restarted.append(1)
    w._on_local_freshness(pl.FRESH)
    assert restarted == [1] and w._local_freshness_recheck is False
    assert w.status_label.text is None


def test_the_tab_reports_every_change_and_never_raises():
    from desktop.my_library_tab import MyLibraryTab

    class _Parent:
        calls = 0

        def _on_my_library_changed(self):
            _Parent.calls += 1

    class _Tab:
        _notify_library_changed = MyLibraryTab._notify_library_changed

    t = _Tab()
    t._parent_window = _Parent()
    t._notify_library_changed()
    assert _Parent.calls == 1

    class _Broken:
        def _on_my_library_changed(self):
            raise RuntimeError('boom')
    t._parent_window = _Broken()
    t._notify_library_changed()                      # logged, not raised
    t._parent_window = None
    t._notify_library_changed()                      # no window: no-op
    # Both ways the library changes report it: the end of every scan, and a
    # folder removal (synchronous, so it never reaches the scan's end).
    for name in ('_on_worker_finished', '_on_remove_folder_clicked'):
        assert 'self._notify_library_changed()' in inspect.getsource(
            getattr(MyLibraryTab, name)), name


def test_a_finished_build_is_checked_too():
    src = inspect.getsource(APP._on_local_passage_build_finished)
    assert 'self._start_local_freshness_check()' in src


# ---------------------------------------------------------------------------
# The build preflight runs on a worker.
# ---------------------------------------------------------------------------

def test_the_preflight_worker_counts_and_sizes(monkeypatch):
    from desktop.passage_workers import LocalPassagePreflightThread
    import shared.passage_local_corpus as plc
    monkeypatch.setattr(os.path, 'exists', lambda p: True)
    monkeypatch.setattr(plc, 'count_local_pages', lambda p: 7)
    monkeypatch.setattr(pll, 'estimate_local_build_bytes',
                        lambda p: 3 * 2 ** 33)       # past 2**31 on purpose
    got = []
    t = LocalPassagePreflightThread('db')
    t.result_signal.connect(lambda n, b: got.append((n, b)))
    t.run()
    assert got == [(7, 3 * 2 ** 33)]


def test_a_failed_estimate_still_answers(monkeypatch):
    from desktop.passage_workers import LocalPassagePreflightThread
    import shared.passage_local_corpus as plc
    monkeypatch.setattr(os.path, 'exists', lambda p: True)
    monkeypatch.setattr(plc, 'count_local_pages', lambda p: 7)

    def _boom(p):
        raise OSError('locked')
    monkeypatch.setattr(pll, 'estimate_local_build_bytes', _boom)
    got = []
    t = LocalPassagePreflightThread('db')
    t.result_signal.connect(lambda n, b: got.append((n, b)))
    t.run()
    assert got == [(7, 0)]


def test_the_build_command_no_longer_reads_the_library_on_the_ui_thread():
    src = inspect.getsource(APP.run_local_passage_index_build)
    assert 'count_local_pages' not in src
    assert 'estimate_local_build_bytes' not in src
    assert 'LocalPassagePreflightThread' in src


class _PreWin:
    _on_local_passage_preflight = APP._on_local_passage_preflight

    def __init__(self):
        self.status_label = _Label()
        self.started = []

    def _revalidate_comp_method(self):
        pass

    def _start_local_passage_build_worker(self, db_path):
        self.started.append(db_path)


def test_an_empty_library_is_told_so_and_nothing_starts(monkeypatch):
    shown = []
    monkeypatch.setattr(genizah_app.QMessageBox, 'information',
                        lambda *a, **k: shown.append(a[1]))
    w = _PreWin()
    w._on_local_passage_preflight('db', 0, 0, True)
    assert shown and w.started == []


def test_a_counted_library_goes_on_to_the_build(monkeypatch, tmp_path):
    monkeypatch.setattr(pll, 'local_passage_root', lambda: str(tmp_path))
    w = _PreWin()
    w._on_local_passage_preflight('db', 5, 0, False)
    assert w.started == ['db']
    assert w._local_passage_build_in_flight is True


def test_an_answer_after_close_or_during_a_build_does_nothing():
    for attr in ('_close_pending', '_local_passage_build_in_flight'):
        w = _PreWin()
        setattr(w, attr, True)
        w._on_local_passage_preflight('db', 5, 0, False)
        assert w.started == []


# ---------------------------------------------------------------------------
# The gate: My Library readiness includes the tab's own "searchable".
# ---------------------------------------------------------------------------

class _Combo:
    def __init__(self, data):
        self._data = data

    def currentData(self):
        return self._data


class _GateWin:
    _passage_disabled_reason_now = APP._passage_disabled_reason_now

    def __init__(self, engine, scope='local'):
        self.searcher = engine
        self.comp_corpus_scope_combo = _Combo(scope)
        self._comp_corpus_scope = scope


class _Tab:
    def __init__(self, searchable):
        self.is_searchable = searchable


class _Engine:
    def __init__(self, tab=None):
        self.index = self.searcher = object()
        self.local_index = self.local_searcher = object()
        if tab is not None:
            self._my_library_tab_ref = lambda: tab


@pytest.mark.parametrize('tab, expected', [
    (None, None),
    (_Tab(True), None),
    (_Tab(False), pll.REASON_LOCAL_INDEX_MISSING),
])
def test_my_library_mid_rebuild_is_not_ready(monkeypatch, tab, expected):
    """While My Library rebuilds its own index after a schema change the
    tab says it is not searchable, and chunk search skips LOCAL; letter-level
    must refuse too, since every row reads its text from that index."""
    monkeypatch.setattr(pll, 'local_passage_available', lambda: True)
    w = _GateWin(_Engine(tab))
    assert APP._passage_disabled_reason_now(w) == expected
