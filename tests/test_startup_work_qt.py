# -*- coding: utf-8 -*-
"""The window's side of the startup queue (desktop/startup_work.py, 2026-11-23): the shelfmark
completer filled across event-loop turns with the same rows as at once, and attached only when
full; and a step that waits for the queue (the restored Browse page for the browse map) runs
once, whether the queue was there first or not.
"""
from __future__ import annotations

import re
import types

import pytest
from PyQt6.QtCore import QObject, Qt, pyqtSignal
from PyQt6.QtTest import QTest
from PyQt6.QtWidgets import QLineEdit, QWidget

import genizah_app

pytestmark = pytest.mark.gui

UR = Qt.ItemDataRole.UserRole


def _host():
    csv_bank = {
        '990000000000000001': {'shelfmark': 'T-S 12.123'},
        '990000000000000002': {'shelfmark': 'T-S 12.123'},
        '990000000000000003': {'shelfmark': 'MS. Heb. a.1'},
        '990000000000000004': {'shelfmark': 'Or. 5557B'},
        '990000000000000005': {'shelfmark': 'ENA 1052.1'},
        '990000000000000006': {'shelfmark': ''},
    }
    parts = [{'display': 'heb. d. 29 part 2', 'normalized': 'hebd29part2',
              'part_id': 'MS. Heb. d. 29/2'}]
    host = QWidget()
    host.meta_mgr = types.SimpleNamespace(csv_bank=csv_bank, get_part_autocomplete_list=lambda: parts)
    host.browse_shelf_input = QLineEdit(host)
    return host


def _rows(model):
    return [(model.item(i).text(), model.item(i).data(UR), model.item(i).data(UR + 1))
            for i in range(model.rowCount())]


def test_the_completer_filled_in_turns_has_the_rows_filled_at_once():
    whole = _host()
    assert genizah_app.GenizahGUI.setup_shelfmark_completer(whole) is True
    from desktop.startup_work import shelfmark_completer_rows
    host = _host()
    host._COMPLETER_CHUNK = 2
    genizah_app.GenizahGUI._fill_shelfmark_completer(host, shelfmark_completer_rows(host.meta_mgr))
    assert getattr(host, 'shelf_completer', None) is None        # never offered half-filled
    QTest.qWait(200)
    assert _rows(host.shelf_model) == _rows(whole.shelf_model)
    assert host.valid_shelf_keys == whole.valid_shelf_keys
    assert host.browse_shelf_input.completer() is host.shelf_completer
    assert host.shelf_completer.filterMode() == Qt.MatchFlag.MatchStartsWith


def test_parts_alone_make_no_completer():
    host = _host()
    rows = [('heb. d. 29 part 2', 'hebd29part2', 'MS. Heb. d. 29/2')]
    assert genizah_app.GenizahGUI.setup_shelfmark_completer(host, rows) is False
    genizah_app.GenizahGUI._fill_shelfmark_completer(host, rows)
    QTest.qWait(50)
    assert getattr(host, 'shelf_completer', None) is None


class _Work(QObject):
    browse_map_ready = pyqtSignal()

    def __init__(self, loaded):
        super().__init__()
        self.browse_map_loaded = loaded


def _waiter(work):
    host = types.SimpleNamespace(_startup_work=work, calls=[])
    step = genizah_app._after_startup_step
    return host, lambda: step(host, 'browse_map_loaded', 'browse_map_ready',
                              lambda: host.calls.append(1))


def test_a_step_waits_for_the_browse_map_and_runs_once():
    work = _Work(False)
    host, wait = _waiter(work)
    wait()
    assert host.calls == []
    work.browse_map_loaded = True
    work.browse_map_ready.emit()
    work.browse_map_ready.emit()
    assert host.calls == [1]


def test_a_step_runs_at_once_when_the_map_is_in_or_there_is_no_queue():
    host, wait = _waiter(_Work(True))
    wait()
    assert host.calls == [1]
    host, wait = _waiter(None)
    wait()
    assert host.calls == [1]


def test_the_restore_no_longer_rebuilds_the_history_menus():
    import inspect
    src = inspect.getsource(genizah_app.GenizahGUI._restore_session)
    assert '_refresh_search_history' not in src and '_refresh_comp_history' not in src
    # A saved sys_id waits for the browse map only; a shelfmark or FL id alone for Browse's
    # resolver index, with the gate opened (nothing is shown until it is resolved).
    assert "self, *wait_for, _restore_browse" in src
    assert "('browse_map_loaded', 'browse_map_ready')" in src
    assert re.search(r"_open_startup_gate\(self\)\s+wait_for = \('browse_resolver_built', "
                     r"'browse_resolver_ready'\)", src)


def test_the_fallback_builds_the_completer_once_the_loaders_end():
    host = _host()
    finished = []
    host.meta_mgr.background_loading_finished = lambda: bool(finished)
    host.setup_shelfmark_completer = lambda: genizah_app.GenizahGUI.setup_shelfmark_completer(host)
    genizah_app.GenizahGUI._completer_after_background_loading(host)
    QTest.qWait(700)
    assert getattr(host, 'shelf_completer', None) is None          # libraries.csv still loading
    finished.append(True)
    QTest.qWait(700)
    assert host.browse_shelf_input.completer() is host.shelf_completer
    assert not host._completer_fallback_timer.isActive()


def test_a_restored_desk_waits_for_the_queues_first_phase_only():
    # PR #105 x #111: the old loaders never start, so background_loading_finished() is True at
    # once; the desk waits for phase 1 (the catalogue read), not for phase 2's builds.
    busy = genizah_app._startup_loading_busy
    window = types.SimpleNamespace(_startup_work=types.SimpleNamespace(phase1_done=False),
                                   meta_mgr=types.SimpleNamespace(background_loading_finished=lambda: True),
                                   searcher=types.SimpleNamespace(_fl_id_index_building=True))
    assert busy(window) is True
    window._startup_work.phase1_done = True
    assert busy(window) is False                 # phase 2 (here the FL-id build) does not hold it
    window._startup_work = None                  # no queue: the loaders' own answer, as before
    assert busy(window) is True


def test_a_qthread_s_mark_goes_with_the_thread():
    from PyQt6.QtCore import QThread
    from desktop.gui_threads import mark_user_work_started
    from shared import background_work

    class _Worker(QThread):
        def run(self):
            mark_user_work_started()               # and never says it ended (terminated)
    worker = _Worker()
    worker.start()
    assert worker.wait(2000)
    try:
        assert background_work.user_work_running()
        background_work._drop_gone_workers()
        assert not background_work.user_work_running()
    finally:
        background_work._user_threads.clear()
