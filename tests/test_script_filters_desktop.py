# -*- coding: utf-8 -*-
"""Desktop "Search only in...": the Script Style / Script Place lists (2026-11-01).

Labels follow the UI language, the saved values stay the catalogue's English
ones, a saved value the catalogue no longer offers stays chosen (dropping it
would widen the search), and the count worker -- which also computes the scope
the search runs with -- applies both lists.
"""
import os
import sys

import pytest

pytestmark = pytest.mark.gui  # imports PyQt6: gui bucket only

from PyQt6.QtCore import Qt  # noqa: E402
from PyQt6.QtWidgets import QApplication  # noqa: E402

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from fjms_filter_sidecar import build_script_sidecar  # noqa: E402

import desktop.dialogs_filter as dialogs_filter  # noqa: E402
from shared import fjms_service  # noqa: E402
from shared.fjms_service import FjmsService  # noqa: E402

_APP = QApplication.instance() or QApplication([])


@pytest.fixture
def sidecar(tmp_path, monkeypatch):
    path = build_script_sidecar(tmp_path / "script.db")
    service = FjmsService(db_path=path)
    monkeypatch.setattr(fjms_service, "_default_service", service)
    monkeypatch.setenv("GENIZAH_FJMS_DB_PATH", path)  # the count worker opens its own
    yield path
    service.close()


def _dialog(monkeypatch, current_filters, lang, counts=None):
    monkeypatch.setattr(dialogs_filter, 'CURRENT_LANG', lang)
    # The live count starts a worker thread; the worker is driven directly below.
    # Each recount the dialog asks for is recorded instead.
    monkeypatch.setattr(dialogs_filter.PreSearchFilterDialog, '_update_count',
                        lambda self: counts.append(self.get_filters()) if counts is not None else None)
    return dialogs_filter.PreSearchFilterDialog(None, current_filters)


def _items(list_widget):
    return [(list_widget.item(i).data(Qt.ItemDataRole.UserRole), list_widget.item(i).text(),
             list_widget.item(i).checkState() == Qt.CheckState.Checked)
            for i in range(list_widget.count())]


def test_hebrew_labels_english_values_and_saved_choices(sidecar, monkeypatch):
    dlg = _dialog(monkeypatch, {'script_places': ['Yemenite', 'Gone']}, 'he')
    try:
        assert _items(dlg._script_lists['script_places']) == [
            ('Oriental', 'מזרחי (2)', False),
            ('Spanish', 'ספרדי (1)', False),
            ('Yemenite', 'תימני (1)', True),
            ('Gone', 'Gone', True),  # not offered now, still chosen and shown
        ]
        assert dlg.get_filters()['script_places'] == ['Yemenite', 'Gone']
        assert 'script_styles' not in dlg.get_filters(), "nothing chosen, no key"
    finally:
        dlg.deleteLater()


def test_ticking_a_style_recounts_and_clear_all_empties_it(sidecar, monkeypatch):
    counts = []
    dlg = _dialog(monkeypatch, {}, 'en', counts)
    try:
        counts.clear()  # the dialog's own first count
        dlg._check_list_item(dlg._script_lists['script_styles'], 'Square', True)
        assert counts and counts[-1].get('script_styles') == ['Square'], (
            "ticking a script value must recount with it")
        dlg._clear_all()
        assert 'script_styles' not in counts[-1], "Clear All must recount without it"
        assert 'script_styles' not in dlg.get_filters()
    finally:
        dlg.deleteLater()


def test_a_script_chip_removes_its_value_and_recounts(sidecar, monkeypatch):
    from PyQt6.QtWidgets import QPushButton
    counts = []
    dlg = _dialog(monkeypatch, {'script_places': ['Spanish', 'Yemenite']}, 'he', counts)
    try:
        chips = [w for w in dlg.chip_bar_widget.findChildren(QPushButton) if 'ספרדי' in w.text()]
        assert len(chips) == 1, [w.text() for w in dlg.chip_bar_widget.findChildren(QPushButton)]
        counts.clear()
        chips[0].click()
        assert dlg.get_filters()['script_places'] == ['Yemenite']
        assert counts and counts[-1]['script_places'] == ['Yemenite']
    finally:
        dlg.deleteLater()


@pytest.mark.parametrize('include_mode,expected', [
    (True, '[include: Bible, Spanish]'),
    (False, '[exclude: Bible] [include: Spanish]'),
])
def test_desktop_history_summary_never_puts_a_script_under_exclude(monkeypatch, include_mode, expected):
    import genizah_app
    import genizah_core
    monkeypatch.setattr(genizah_core, 'CURRENT_LANG', 'en')
    monkeypatch.setattr(genizah_app, 'CURRENT_LANG', 'en')
    filters = {'include_mode': include_mode, 'domains': ['Bible'], 'script_places': ['Spanish']}
    assert genizah_app.GenizahGUI._build_filter_summary(filters, 80) == expected


def test_desktop_truncated_summary_keeps_its_brackets_balanced(monkeypatch):
    import genizah_app
    import genizah_core
    from fjms_filter_sidecar import LONG_EXCLUDE, LONG_EXCLUDE_50, brackets_balanced
    monkeypatch.setattr(genizah_core, 'CURRENT_LANG', 'en')
    monkeypatch.setattr(genizah_app, 'CURRENT_LANG', 'en')
    build = genizah_app.GenizahGUI._build_filter_summary
    assert build(LONG_EXCLUDE, 50) == LONG_EXCLUDE_50
    for max_len in range(20, 70):
        s = build(LONG_EXCLUDE, max_len)
        assert len(s) <= max_len and brackets_balanced(s), (max_len, s)


def test_the_count_worker_applies_both_lists(sidecar):
    from desktop.gui_threads import FilterCountWorker
    worker = FilterCountWorker({'include_mode': True, 'script_styles': ['Square'],
                                'script_places': ['Oriental']})
    finished, failed = [], []
    worker.finished.connect(finished.append)
    worker.failed.connect(failed.append)
    worker.run()  # synchronously, on this thread
    assert failed == [] and finished == [{'990003'}]
