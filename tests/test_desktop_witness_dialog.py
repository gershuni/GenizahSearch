"""The Witnesses dialog -- the parts that only exist once Qt is real.

Everything about WHAT a witness is lives in `desktop/passage_witnesses.py`
and is tested Qt-free in `tests/test_passage_witnesses.py`. What is left here
is behaviour that depends on Qt's own semantics, and the first hand-test found
one: the dialog opened EMPTY, because the refresh that populates it checks
`isVisible()` and a dialog is not visible until `show()`.

Owner ruling 2026-08-27, from that hand-test: the witness list belongs in a
dialog, not inline. Seventeen witnesses on the main window put long Hebrew
labels beside a character count and a status in a space too narrow to read
any of them.
"""

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PyQt6.QtWidgets import QApplication, QHBoxLayout, QWidget  # noqa: E402

import pytest  # noqa: E402

from desktop import passage_witnesses as pw  # noqa: E402
from genizah_app import GenizahGUI  # noqa: E402

pytestmark = pytest.mark.gui  # imports PyQt6: gui bucket only

_APP = QApplication.instance() or QApplication([])

SEED = 'the seed text of this work'


def _window(results=False):
    """A real QWidget carrying the real methods, without app startup.

    `__new__` plus `QWidget.__init__` rather than `GenizahGUI()`: constructing
    the whole window pulls in the index, the sidecars and every tab, none of
    which this dialog touches.
    """
    w = GenizahGUI.__new__(GenizahGUI)
    QWidget.__init__(w)
    w._comp_witnesses = pw.WitnessSet()

    class _TextArea:
        def toPlainText(self):
            return SEED

    w.comp_text_area = _TextArea()
    w._has_comp_results = lambda: results
    w._schedule_session_save = lambda: None
    GenizahGUI._build_witness_button(w, QHBoxLayout())
    return w


def _add(w, *texts):
    return pw.add_texts(w._comp_witness_state(), list(texts), SEED, 'Pasted')


def test_the_dialog_is_populated_when_it_opens():
    """THE regression. `_refresh_witness_panel` fills the table only when the
    dialog is visible, and `isVisible()` is False until `show()` -- so
    refreshing before showing opened a dialog listing nothing at all, with
    the button beside it correctly saying there were two."""
    w = _window()
    _add(w, 'aleph bet gimel dalet', 'he vav zayin het')
    GenizahGUI._open_witness_dialog(w)
    try:
        assert w.comp_witness_table.rowCount() == 2
        assert w.comp_witness_table.item(0, 0).text() == 'aleph bet gimel dalet'
    finally:
        w._witness_dialog.close()


def test_the_table_shows_the_label_the_length_and_the_status():
    """Three columns, because the failure that prompted the dialog was
    unreadability: a long Hebrew label needs a column of its own."""
    w = _window()
    _add(w, 'aleph bet gimel dalet')
    GenizahGUI._open_witness_dialog(w)
    try:
        t = w.comp_witness_table
        assert t.columnCount() == 3
        assert t.item(0, 1).text() == str(len('aleph bet gimel dalet'))
        assert t.item(0, 2).text(), 'no status shown'
    finally:
        w._witness_dialog.close()


def test_the_button_carries_the_count_so_the_main_window_still_says_so():
    """Collapsing the panel is only safe if the main window still reports
    that witnesses exist."""
    w = _window()
    assert '(' not in w.btn_comp_witnesses.text()
    _add(w, 'aleph bet gimel dalet', 'he vav zayin het')
    GenizahGUI._refresh_witness_panel(w)
    assert '(2)' in w.btn_comp_witnesses.text()


def test_removing_the_selected_rows_removes_exactly_those():
    w = _window()
    _add(w, 'aleph bet gimel dalet', 'he vav zayin het', 'tet yod kaf lamed')
    GenizahGUI._open_witness_dialog(w)
    try:
        w.comp_witness_table.selectRow(1)
        assert GenizahGUI._selected_witness_ids(w) == ['w2']
        GenizahGUI._remove_selected_witnesses(w)
        remaining = [e.text for e in w._comp_witness_state().entries]
        assert remaining == ['aleph bet gimel dalet', 'tet yod kaf lamed']
        assert w.comp_witness_table.rowCount() == 2
    finally:
        w._witness_dialog.close()


def test_remove_all_asks_first_and_a_refusal_keeps_everything(monkeypatch):
    """A witness list can be seventeen hand-pasted texts that exist nowhere
    else, and there is no undo."""
    import genizah_app

    w = _window()
    _add(w, 'aleph bet gimel dalet', 'he vav zayin het')
    GenizahGUI._open_witness_dialog(w)
    try:
        asked = []
        monkeypatch.setattr(
            genizah_app.QMessageBox, 'question',
            staticmethod(lambda *a, **k: (
                asked.append(1),
                genizah_app.QMessageBox.StandardButton.No)[-1]))
        GenizahGUI._remove_all_witnesses(w)
        assert asked, 'removed everything without asking'
        assert len(w._comp_witness_state().entries) == 2
    finally:
        w._witness_dialog.close()


def test_remove_all_clears_the_list_when_confirmed(monkeypatch):
    import genizah_app

    w = _window()
    _add(w, 'aleph bet gimel dalet', 'he vav zayin het')
    GenizahGUI._open_witness_dialog(w)
    try:
        monkeypatch.setattr(
            genizah_app.QMessageBox, 'question',
            staticmethod(lambda *a, **k:
                         genizah_app.QMessageBox.StandardButton.Yes))
        GenizahGUI._remove_all_witnesses(w)
        assert w._comp_witness_state().entries == []
        assert w.comp_witness_table.rowCount() == 0
        assert '(' not in w.btn_comp_witnesses.text()
    finally:
        w._witness_dialog.close()


def test_retry_is_offered_only_when_something_failed():
    w = _window()
    _add(w, 'aleph bet gimel dalet')
    GenizahGUI._open_witness_dialog(w)
    try:
        assert not w.btn_comp_witness_retry.isVisible()
        w._comp_witness_state().entries[0].status = pw.STATUS_FAILED
        GenizahGUI._refresh_witness_panel(w)
        assert w.btn_comp_witness_retry.isVisible()
    finally:
        w._witness_dialog.close()


def test_a_notification_reaches_whichever_surface_is_visible():
    """The dialog is modeless, so it may be open or closed while a batch
    runs; writing to only one label loses the message half the time."""
    w = _window()
    GenizahGUI._witness_notify(w, 'closed-case')
    assert w.lbl_comp_witness_progress.text() == 'closed-case'
    GenizahGUI._open_witness_dialog(w)
    try:
        GenizahGUI._witness_notify(w, 'open-case')
        assert w.lbl_comp_witness_dialog_status.text() == 'open-case'
        assert w.lbl_comp_witness_progress.text() == 'open-case'
    finally:
        w._witness_dialog.close()


def test_reopening_does_not_reset_the_auto_expand_choices():
    """The spin boxes die with the dialog, so their values live on the
    window -- otherwise a chosen round count silently reverts to 3."""
    w = _window()
    GenizahGUI._open_witness_dialog(w)
    w.spin_comp_auto_rounds.setValue(5)
    w.spin_comp_auto_topk.setValue(9)
    w._witness_dialog.close()
    GenizahGUI._open_witness_dialog(w)
    try:
        assert w.spin_comp_auto_rounds.value() == 5
        assert w.spin_comp_auto_topk.value() == 9
    finally:
        w._witness_dialog.close()


def test_opening_twice_does_not_stack_two_dialogs():
    w = _window()
    GenizahGUI._open_witness_dialog(w)
    first = w._witness_dialog
    GenizahGUI._open_witness_dialog(w)
    try:
        assert w._witness_dialog is first
    finally:
        w._witness_dialog.close()


def test_the_button_hides_with_the_other_letter_level_controls():
    """It joins the same visibility contract as the policy selectors, so it
    cannot survive onto a chunk search."""
    w = _window()
    GenizahGUI._set_witness_panel_visible(w, False)
    assert not w.btn_comp_witnesses.isVisible()
