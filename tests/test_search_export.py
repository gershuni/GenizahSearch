# -*- coding: utf-8 -*-
"""GS-9 part 2: the Search tab's Export of the whole result list (desktop/search_export.py).

Part 1 made Export write every kept result, not the 500 loaded; 28,000 results then froze
the window for a minute. Above EXPORT_IN_BACKGROUND rows the file is now built on a worker
from a snapshot of the window, with progress and Cancel (owner ruling D5), the index install
waits for it, and the saved message says how many rows were written. While the table is
sorted by a column, the whole list is written in that order (owner ruling D4).
"""
from __future__ import annotations

import pytest
from PyQt6.QtCore import Qt
from PyQt6.QtTest import QTest

import desktop.search_export as se
import genizah_app
import genizah_core
from tests.test_exclusion_surfaces import (_personal_state_in_tmp, _search,  # noqa: F401
                                           slot_errors, window)
from tests.test_export_locked_file_sites import _search_export
from tests.test_result_view_window import _many, _sid

pytestmark = pytest.mark.gui  # imports genizah_app, which imports PyQt6


@pytest.fixture(autouse=True)
def _english(monkeypatch):
    monkeypatch.setattr(genizah_core, "CURRENT_LANG", "en")
    monkeypatch.setattr(genizah_app, "CURRENT_LANG", "en", raising=False)


def _ids_in(path):
    """The System IDs a CSV export wrote, in order."""
    text = path.read_bytes().decode("utf-8-sig")
    return [line.split(",")[0] for line in text.splitlines() if line.startswith("99")]


def _export(w, monkeypatch, tmp_path, n, fmt="csv"):
    w, out, shown = _search_export(w, monkeypatch, tmp_path, fmt)
    shown.clear()
    w._show_export_saved_dialog = lambda p, count=None: shown.append((p, count))
    _search(w, _many(n))
    return w, out, shown


def _wait(w, ms=20000):
    for _ in range(ms // 20):
        if not w._export_running():
            break
        QTest.qWait(20)
    QTest.qWait(50)                     # the queued done_signal


def test_a_large_export_is_built_on_a_worker_and_says_how_many(window, monkeypatch, tmp_path):  # noqa: F811 (the imported fixture)
    monkeypatch.setattr(genizah_app, "EXPORT_IN_BACKGROUND", 100)
    w, out, shown = _export(window, monkeypatch, tmp_path, 1200)
    started = []
    monkeypatch.setattr(se.ExportThread, "start", lambda self, *a: started.append(self) or se.QThread.start(self))
    w.export_results("csv")
    assert started, "a 1,200-row export was built on the window's thread"
    _wait(w)
    assert _ids_in(out) == [_sid(i) for i in range(1200)]
    assert shown == [(str(out), 1200)], "the saved message does not say how many rows were written"


def test_a_small_export_is_built_at_once(window, monkeypatch, tmp_path):  # noqa: F811 (the imported fixture)
    w, out, shown = _export(window, monkeypatch, tmp_path, 30)
    monkeypatch.setattr(se.ExportThread, "start", lambda self, *a: pytest.fail("a small export went to a worker"))
    w.export_results("csv")
    assert _ids_in(out) == [_sid(i) for i in range(30)] and shown == [(str(out), 30)]


def test_the_build_reads_a_snapshot_not_the_window(window, monkeypatch, tmp_path):  # noqa: F811 (the imported fixture)
    """Codex F2 on the plan: New or another search while the file is built changes nothing in it."""
    monkeypatch.setattr(genizah_app, "EXPORT_IN_BACKGROUND", 10)
    w, out, shown = _export(window, monkeypatch, tmp_path, 300)
    gate = _gated_build(monkeypatch)
    w.export_results("csv")
    assert w._export_running()
    assert "export" in w._index_readers_busy(), "the index install would close the index under the export"
    _search(w, _many(5))                                    # another search meanwhile
    w.last_search_query = "something else"
    gate.append(1)
    _wait(w)
    assert _ids_in(out) == [_sid(i) for i in range(300)]
    assert "word" in out.read_bytes().decode("utf-8-sig"), "the file took the later search's query"
    assert "export" not in w._index_readers_busy()


def test_cancel_writes_nothing(window, monkeypatch, tmp_path):  # noqa: F811 (the imported fixture)
    monkeypatch.setattr(genizah_app, "EXPORT_IN_BACKGROUND", 10)
    monkeypatch.setattr(se, "PROGRESS_EVERY", 1)
    w, out, shown = _export(window, monkeypatch, tmp_path, 300)
    gate = _gated_build(monkeypatch)
    w.export_results("csv")
    w._export_thread.request_cancel()                       # the progress dialog's Cancel
    gate.append(1)
    _wait(w)
    assert not out.exists() and shown == []
    assert w.status_label.text() == genizah_app.tr("Export cancelled")


def _gated_build(monkeypatch):
    """The builder waits until the test appends to the returned list."""
    import time
    gate, real = [], genizah_app._build_search_export

    def gated(job, progress=None, cancelled=None):
        while not gate:
            time.sleep(0.005)
        return real(job, progress, cancelled)
    monkeypatch.setattr(genizah_app, "_build_search_export", gated)
    return gate


def test_a_sorted_table_exports_the_whole_list_in_its_order(window, monkeypatch, tmp_path):  # noqa: F811 (the imported fixture)
    """D4: sorted by Shelfmark, the whole list is written in the table's sort, not the
    loaded rows sorted and the rest after them. The test metadata names result i
    "T-S <last three digits>", so 1,200 results sort by i % 1000."""
    w, out, shown = _export(window, monkeypatch, tmp_path, 1200)
    assert w.results_loaded < 1200
    w.results_table.sortItems(w.COL_SHELF, Qt.SortOrder.AscendingOrder)
    w.export_results("csv")
    wanted = [_sid(i) for i in sorted(range(1200), key=lambda i: i % 1000)]
    assert _ids_in(out) == wanted
    w.results_table.sortItems(w.COL_SHELF, Qt.SortOrder.DescendingOrder)
    w.export_results("csv")
    assert _ids_in(out) == [_sid(i) for i in sorted(range(1200), key=lambda i: i % 1000, reverse=True)]


def test_export_order_is_stable_in_both_directions():
    rows = [("b", 1), ("a", 2), ("b", 3), ("a", 4)]
    assert se.export_order(rows, lambda r: r[0], False) == [("a", 2), ("a", 4), ("b", 1), ("b", 3)]
    assert se.export_order(rows, lambda r: r[0], True) == [("b", 1), ("b", 3), ("a", 2), ("a", 4)]


def test_the_xlsx_steps_once_per_row_local_rows_too():
    """Codex on #151: LOCAL rows never reach meta_resolver, so a LOCAL-only xlsx
    reported no progress and ignored Cancel. The builder steps once per result."""
    rows = [{'display': {'source': 'LOCAL', 'id': f'97{i:08d}', 'shelfmark': f'doc {i}'},
             'uid': f'97{i:08d}_1', 'snippet': 'word'} for i in range(3)]
    rows.append({'display': {'id': '990000001', 'shelfmark': 'T-S 1'}, 'uid': '990000001_1', 'snippet': 'word'})
    steps = []
    genizah_app._build_search_results_xlsx_bytes(results=rows, meta_resolver=lambda sid: None, row_step=lambda: steps.append(1))
    assert len(steps) == len(rows)


def test_cancel_pressed_after_the_last_row_still_cancels():
    """Codex round 2 on #151: Cancel pressed while the workbook is saved, after the last
    row's step, must not save the file."""
    job = se.ExportJob(fmt='xlsx', results=[], selected=False, meta={}, lang='en')
    thread = se.ExportThread(lambda j, progress, cancelled: (thread.request_cancel(), b'file')[1], job)
    seen = []
    thread.cancelled_signal.connect(lambda: seen.append('cancelled'))
    thread.done_signal.connect(lambda payload: seen.append('done'))
    thread.run()
    assert seen == ['cancelled']


def test_the_sub_sheets_heed_cancel():
    """Codex round 3 on #151: after the last main-sheet row, the Manuscripts and
    Bibliography sheets look each manuscript up; Cancel must stop that too."""
    job = se.ExportJob(fmt='xlsx', results=[], selected=False, meta={}, lang='en')
    step = job.tracker(cancelled=lambda: True)
    with pytest.raises(se.ExportCancelled):
        step.check()
    rows = [{'display': {'id': '990000001', 'shelfmark': 'T-S 1'}, 'uid': '990000001_1', 'snippet': 'w'}]
    job = se.ExportJob(fmt='xlsx', results=rows, selected=False, meta={}, lang='en', local_filepaths={},
                       transcription_ids=set(), printed_ids=set(), result_domains={})
    seen = []
    with pytest.raises(se.ExportCancelled):
        genizah_app._build_search_export(job, cancelled=lambda: seen.append(1) or len(seen) > 1)
