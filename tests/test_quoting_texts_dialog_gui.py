# -*- coding: utf-8 -*-
"""desktop/quoting_texts_dialog.py::QuotingTextsDialog (owner decision
2026-10-15): the "Quoting texts..." dialog, built to look like the Quoted
texts dialog (desktop/filter_text_dialog.py::FilterTextDialog) and reusing
its Sefaria library browser (AllSourcesDialog) under "Load from Sefaria...".

GUI lane: run with `python scripts/run_gui_tests.py` (one file per process).
Labels are compared with tr(), never literal text, so the test holds in
either UI language. NO network: AllSourcesDialog, the passage prompt
(PassagePromptDialog) and SefariaFetchThread are all stubbed.
"""
from __future__ import annotations

import pytest
from PyQt6.QtCore import QObject, pyqtSignal
from PyQt6.QtGui import QCloseEvent
from PyQt6.QtWidgets import QApplication, QDialog, QDialogButtonBox

import desktop.quoting_texts_dialog as qtd
from genizah_core import tr
from shared import compound_query as cq

pytestmark = pytest.mark.gui

_APP = QApplication.instance() or QApplication([])

LONG = 'אבגדה' * 12          # 60 letters -- comfortably over the default 40-letter floor


# -- stubs: no network, no real Sefaria library, no real QThread -------------

class _StubAllSourcesDialog:
    """Stands in for desktop.filter_text_dialog.AllSourcesDialog: a test sets
    `selected_titles` (class attribute) before the dialog opens it."""
    selected_titles: list = []

    def __init__(self, parent):
        self.parent = parent
        self.selected_refs = list(type(self).selected_titles)

    def exec(self):
        return (QDialog.DialogCode.Accepted if self.selected_refs
               else QDialog.DialogCode.Rejected)

    def deleteLater(self):
        pass


class _StubAddCustomTextDialog:
    """Stands in for AddCustomTextDialog: a test sets the three `custom_*`
    class attributes before `_open_add_custom` is called."""
    custom_ref = 'custom:1:Stub'
    custom_text = LONG
    custom_name = 'Stub'

    def __init__(self, parent):
        pass

    def exec(self):
        return QDialog.DialogCode.Accepted

    def deleteLater(self):
        pass


class _StubFetchThread(QObject):
    """Stands in for desktop.filter_text_dialog.SefariaFetchThread: `start()`
    just marks it running -- a test completes the fetch itself by emitting
    `finished`/`error`, so timing is fully under the test's control."""
    progress = pyqtSignal(int, int, str)
    finished = pyqtSignal(dict)
    error = pyqtSignal(str)
    instances: list = []

    def __init__(self, refs, use_cache=True):
        super().__init__()
        self.refs = list(refs)
        self._running = False
        self.cancel_called = False
        self.wait_called = False
        type(self).instances.append(self)

    def isRunning(self):
        return self._running

    def start(self):
        self._running = True

    def cancel(self):
        self.cancel_called = True
        self._running = False

    def wait(self):
        self.wait_called = True


@pytest.fixture
def stub_thread(monkeypatch):
    _StubFetchThread.instances = []
    monkeypatch.setattr(qtd, 'SefariaFetchThread', _StubFetchThread)
    return _StubFetchThread


def _accept_with_suffix(suffix):
    """A PassagePromptDialog.exec replacement: types `suffix` after the
    pre-filled "<title> " and accepts."""
    def _exec(dlg):
        dlg.ref_input.setText(dlg.ref_input.text() + suffix)
        return QDialog.DialogCode.Accepted
    return _exec


def _warnings(monkeypatch):
    seen = []
    monkeypatch.setattr(qtd.QMessageBox, 'warning',
                        lambda *a, **k: seen.append(a[2]))
    return seen


# -- basics ---------------------------------------------------------------

def test_default_place_is_same_image():
    dlg = qtd.QuotingTextsDialog(None, floor=40)
    try:
        assert dlg.combo_where.currentData() == cq.WHERE_SAME_IMAGE
    finally:
        dlg.deleteLater()


def test_remove_selected_text():
    dlg = qtd.QuotingTextsDialog(None, floor=40)
    try:
        dlg._add_text('A', LONG)
        dlg._add_text('B', LONG)
        assert dlg.texts_list.count() == 2
        dlg.texts_list.setCurrentRow(0)
        dlg._remove_selected()
        assert [t['name'] for t in dlg._texts] == ['B']
        assert dlg.texts_list.count() == 1
    finally:
        dlg.deleteLater()


def test_ok_yields_a_must_not_condition_with_versions_and_names():
    dlg = qtd.QuotingTextsDialog(None, floor=40)
    try:
        dlg._add_text('A', LONG)
        dlg._add_text('B', LONG + LONG)
        c = dlg.condition()
        assert c.requirement == cq.REQ_MUST_NOT
        assert c.versions == (LONG, LONG + LONG)
        assert c.version_names == ('A', 'B')
        assert c.label == 'A, B'
        assert c.where == cq.WHERE_SAME_IMAGE
    finally:
        dlg.deleteLater()


def test_ok_disabled_with_no_texts_or_a_problem():
    dlg = qtd.QuotingTextsDialog(None, floor=40)
    ok = dlg.buttons.button(QDialogButtonBox.StandardButton.Ok)
    try:
        assert not ok.isEnabled()                   # nothing loaded yet
        dlg._add_text('A', LONG)
        assert ok.isEnabled()
        dlg._add_text('short', 'א' * 5)              # under the floor
        assert not ok.isEnabled()
    finally:
        dlg.deleteLater()


# -- editing an existing condition -----------------------------------------

def test_edit_round_trip_keeps_names():
    original = cq.Condition(cq.REQ_MUST_NOT, cq.WHERE_ADJACENT, (LONG, LONG + LONG),
                            'A, B', version_names=('A', 'B'))
    dlg = qtd.QuotingTextsDialog(None, condition=original, floor=40)
    try:
        assert [t['name'] for t in dlg._texts] == ['A', 'B']
        assert dlg.combo_where.currentData() == cq.WHERE_ADJACENT
        assert dlg.condition() == original
    finally:
        dlg.deleteLater()


def test_condition_without_names_shows_generic_text_labels():
    old = cq.Condition(cq.REQ_MUST_NOT, cq.WHERE_MANUSCRIPT, (LONG, LONG + LONG), 'old label')
    dlg = qtd.QuotingTextsDialog(None, condition=old, floor=40)
    try:
        names = [t['name'] for t in dlg._texts]
        assert names == [tr("Text {}").format(1), tr("Text {}").format(2)]
    finally:
        dlg.deleteLater()


# -- "Add Custom Text" / "Load from files..." --------------------------------

def test_custom_text_and_files_are_added(tmp_path, monkeypatch):
    monkeypatch.setattr(qtd, 'AddCustomTextDialog', _StubAddCustomTextDialog)
    path = tmp_path / 'Halakhot Gedolot.txt'
    path.write_text(LONG, encoding='utf-8')
    monkeypatch.setattr(qtd.QFileDialog, 'getOpenFileNames',
                        staticmethod(lambda *a, **k: ([str(path)], '')))
    dlg = qtd.QuotingTextsDialog(None, floor=40)
    try:
        dlg._open_add_custom()
        dlg._open_load_from_files()
        names = [t['name'] for t in dlg._texts]
        assert names == ['Stub', 'Halakhot Gedolot']
        assert dlg.texts_list.count() == 2
        assert dlg.texts_list.item(1).text() == tr("{} · {} letters").format(
            'Halakhot Gedolot', len(LONG))
    finally:
        dlg.deleteLater()


# -- "Load from Sefaria..." --------------------------------------------------

def test_sefaria_load_adds_the_text_under_its_ref(monkeypatch, stub_thread):
    _StubAllSourcesDialog.selected_titles = ['Rif Berakhot']
    monkeypatch.setattr(qtd, 'AllSourcesDialog', _StubAllSourcesDialog)
    monkeypatch.setattr(qtd.PassagePromptDialog, 'exec', _accept_with_suffix('1a-2b'))
    dlg = qtd.QuotingTextsDialog(None, floor=40)
    try:
        dlg._open_load_from_sefaria()
        [thread] = stub_thread.instances
        assert thread.refs == ['Rif Berakhot 1a-2b']
        thread.finished.emit({'Rif Berakhot 1a-2b': LONG})
        assert dlg._texts == [{'name': 'Rif Berakhot 1a-2b', 'text': LONG}]
        assert dlg.texts_list.item(0).text() == tr("{} · {} letters").format(
            'Rif Berakhot 1a-2b', len(LONG))
    finally:
        dlg.deleteLater()


def test_whole_book_or_empty_passage_is_refused(monkeypatch, stub_thread):
    _StubAllSourcesDialog.selected_titles = ['Halakhot Gedolot']
    monkeypatch.setattr(qtd, 'AllSourcesDialog', _StubAllSourcesDialog)
    warnings = _warnings(monkeypatch)
    dlg = qtd.QuotingTextsDialog(None, floor=40)
    try:
        # Whole book: the prompt is accepted unchanged ("<title> ").
        monkeypatch.setattr(qtd.PassagePromptDialog, 'exec',
                            lambda self: QDialog.DialogCode.Accepted)
        dlg._open_load_from_sefaria()
        assert stub_thread.instances == []
        assert warnings[-1] == tr("Choose a passage, not the whole book.")

        # Empty: the user clears the line edit entirely.
        monkeypatch.setattr(qtd.PassagePromptDialog, 'exec',
                            lambda self: (self.ref_input.setText(''),
                                         QDialog.DialogCode.Accepted)[1])
        dlg._open_load_from_sefaria()
        assert stub_thread.instances == []
        assert warnings[-1] == tr("Choose a passage, not the whole book.")
    finally:
        dlg.deleteLater()


def test_fetched_text_too_long_is_refused_with_the_message(monkeypatch, stub_thread):
    warnings = _warnings(monkeypatch)
    dlg = qtd.QuotingTextsDialog(None, floor=40)
    huge_letters = cq.MAX_VERSION_LETTERS + 1
    huge = 'א' * huge_letters
    try:
        dlg._start_fetch(['Halakhot Gedolot'])
        [thread] = stub_thread.instances
        thread.finished.emit({'Halakhot Gedolot': huge})
        assert dlg._texts == []
        assert warnings[-1] == tr(
            "{} is too long ({} letters; at most {}). Choose a shorter passage."
        ).format('Halakhot Gedolot', huge_letters, cq.MAX_VERSION_LETTERS)
    finally:
        dlg.deleteLater()


def test_not_found_on_sefaria_is_refused_with_the_message(monkeypatch, stub_thread):
    warnings = _warnings(monkeypatch)
    dlg = qtd.QuotingTextsDialog(None, floor=40)
    try:
        dlg._start_fetch(['No Such Ref'])
        [thread] = stub_thread.instances
        thread.finished.emit({})                      # the ref never came back
        assert dlg._texts == []
        assert warnings[-1] == tr("Not found on Sefaria: {}").format('No Such Ref')
    finally:
        dlg.deleteLater()


# -- the fetch thread never outlives the dialog ------------------------------

def test_reject_cancels_and_waits_for_the_fetch_thread(stub_thread):
    dlg = qtd.QuotingTextsDialog(None, floor=40)
    try:
        dlg._start_fetch(['Some Ref'])
        [thread] = stub_thread.instances
        assert thread.isRunning()
        dlg.reject()
        assert thread.cancel_called and thread.wait_called
    finally:
        dlg.deleteLater()


def test_accept_cancels_and_waits_for_the_fetch_thread(stub_thread):
    dlg = qtd.QuotingTextsDialog(None, floor=40)
    try:
        dlg._add_text('A', LONG)                      # so OK is reachable at all
        dlg._start_fetch(['Some Ref'])
        [thread] = stub_thread.instances
        dlg.accept()
        assert thread.cancel_called and thread.wait_called
    finally:
        dlg.deleteLater()


def test_close_event_cancels_and_waits_for_the_fetch_thread(stub_thread):
    dlg = qtd.QuotingTextsDialog(None, floor=40)
    try:
        dlg._start_fetch(['Some Ref'])
        [thread] = stub_thread.instances
        dlg.closeEvent(QCloseEvent())
        assert thread.cancel_called and thread.wait_called
    finally:
        dlg.deleteLater()
# -- review round 1 (2026-10-15) ---------------------------------------------

def test_a_replaced_fetch_cannot_report_into_the_next_one(monkeypatch, stub_thread):
    """A second load while the first fetch runs: the first fetch's late
    `finished` must not mark the second fetch's passage "not found"."""
    seen = _warnings(monkeypatch)
    dlg = qtd.QuotingTextsDialog()
    try:
        dlg._start_fetch(['First 1a'])
        first = stub_thread.instances[-1]
        dlg._start_fetch(['Second 2b'])
        second = stub_thread.instances[-1]
        first.finished.emit({})                      # late, from the replaced fetch
        assert seen == [] and dlg._texts == []
        second.finished.emit({'Second 2b': LONG})
        assert [t['name'] for t in dlg._texts] == ['Second 2b']
        assert seen == []
    finally:
        dlg.deleteLater()


def test_a_fetch_finishing_after_the_dialog_closed_changes_nothing(monkeypatch, stub_thread):
    seen = _warnings(monkeypatch)
    dlg = qtd.QuotingTextsDialog()
    try:
        dlg._start_fetch(['Missing Ref 1a'])
        thread = stub_thread.instances[-1]
        dlg.reject()
        thread.finished.emit({})
        thread.error.emit('late')
        assert seen == [] and dlg._texts == []
    finally:
        dlg.deleteLater()


def test_load_from_sefaria_is_disabled_while_a_fetch_runs(stub_thread):
    dlg = qtd.QuotingTextsDialog()
    try:
        dlg._start_fetch(['Rif Berakhot 1a'])
        assert not dlg.btn_sefaria.isEnabled()
        stub_thread.instances[-1].finished.emit({'Rif Berakhot 1a': LONG})
        assert dlg.btn_sefaria.isEnabled()
    finally:
        dlg.deleteLater()


def test_fetch_errors_are_shown(monkeypatch, stub_thread):
    seen = _warnings(monkeypatch)
    dlg = qtd.QuotingTextsDialog()
    try:
        dlg._start_fetch(['Rif Berakhot 1a'])
        thread = stub_thread.instances[-1]
        thread.error.emit('Error fetching Rif Berakhot 1a: timeout')
        thread.finished.emit({'Rif Berakhot 1a': LONG})
        assert seen and 'timeout' in seen[-1]
    finally:
        dlg.deleteLater()


def test_cancelling_a_passage_prompt_asks_no_more(monkeypatch, stub_thread):
    asked = []

    def _cancel(dlg):
        asked.append(dlg._title)
        return QDialog.DialogCode.Rejected
    _StubAllSourcesDialog.selected_titles = ['Rif Berakhot', 'Rif Shabbat', 'Rif Pesachim']
    monkeypatch.setattr(qtd, 'AllSourcesDialog', _StubAllSourcesDialog)
    monkeypatch.setattr(qtd.PassagePromptDialog, 'exec', _cancel)
    dlg = qtd.QuotingTextsDialog()
    try:
        dlg._open_load_from_sefaria()
        assert asked == ['Rif Berakhot']
        assert stub_thread.instances == []
    finally:
        dlg.deleteLater()


def test_an_over_limit_custom_text_keeps_ok_disabled():
    dlg = qtd.QuotingTextsDialog()
    try:
        dlg._add_text('huge', 'א' * (cq.MAX_VERSION_LETTERS + 1))
        assert not dlg.buttons.button(QDialogButtonBox.StandardButton.Ok).isEnabled()
    finally:
        dlg.deleteLater()


def test_more_than_the_version_limit_keeps_ok_disabled():
    dlg = qtd.QuotingTextsDialog()
    try:
        for i in range(cq.MAX_VERSIONS):
            dlg._add_text(f't{i}', LONG)
        assert dlg.buttons.button(QDialogButtonBox.StandardButton.Ok).isEnabled()
        dlg._add_text('one too many', LONG)
        assert not dlg.buttons.button(QDialogButtonBox.StandardButton.Ok).isEnabled()
    finally:
        dlg.deleteLater()


def test_an_older_one_text_condition_keeps_its_label_as_the_name():
    c = cq.Condition(cq.REQ_MUST_NOT, cq.WHERE_MANUSCRIPT, (LONG,), 'Halakhot Gedolot')
    dlg = qtd.QuotingTextsDialog(condition=c)
    try:
        assert [t['name'] for t in dlg._texts] == ['Halakhot Gedolot']
        assert dlg.condition().label == 'Halakhot Gedolot'
    finally:
        dlg.deleteLater()


def test_hebrew_ui_is_right_to_left(monkeypatch):
    from PyQt6.QtCore import Qt
    monkeypatch.setattr(qtd, 'CURRENT_LANG', 'he')
    dlg = qtd.QuotingTextsDialog()
    try:
        assert dlg.layoutDirection() == Qt.LayoutDirection.RightToLeft
    finally:
        dlg.deleteLater()


def test_a_queued_signal_from_an_old_fetch_is_ignored_even_if_still_connected(monkeypatch, stub_thread):
    """Disconnecting is one layer; a signal Qt had already queued before the
    disconnect is caught by the second: each slot checks it belongs to the
    current fetch."""
    seen = _warnings(monkeypatch)
    dlg = qtd.QuotingTextsDialog()
    try:
        dlg._start_fetch(['First 1a'])
        first = stub_thread.instances[-1]
        dlg._start_fetch(['Second 2b'])
        dlg._on_fetch_error('old error', thread=first)
        dlg._on_fetch_finished({}, thread=first)
        assert seen == [] and dlg._pending_refs == ['Second 2b']
        dlg.reject()
        dlg._on_fetch_finished({'Second 2b': LONG}, thread=stub_thread.instances[-1])
        assert dlg._texts == []
    finally:
        dlg.deleteLater()
