# -*- coding: utf-8 -*-
"""Keyboard in the desktop Search tab's search bar, driven with real key events.

Owner, v10.0.0 (Hebrew): Tab went query -> search type -> Search, skipping the Gap
field (and Focus Search) that sit between them, and Enter in the Gap field did
nothing. Qt's default Tab order is the order widgets got their parent, and both were
inserted into row 1 after its other widgets (desktop/widgets/tab_order.py).

The tab is built for real (tests/test_exclusion_surfaces.py's search_tab fixture),
in English (left to right) and in Hebrew (right to left).
"""
import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest  # noqa: E402
from PyQt6.QtCore import Qt  # noqa: E402
from PyQt6.QtTest import QTest  # noqa: E402
from PyQt6.QtWidgets import QApplication, QHBoxLayout, QLabel, QLineEdit, QPushButton, QVBoxLayout, QWidget  # noqa: E402

import genizah_app as app  # noqa: E402
import genizah_core  # noqa: E402
from desktop.widgets.tab_order import chain_layouts, widgets_in_layout_order  # noqa: E402
from tests.test_exclusion_surfaces import (  # noqa: E402,F401  (fixtures)
    _personal_state_in_tmp, search_tab, slot_errors, window,
)

pytestmark = pytest.mark.gui

_APP = QApplication.instance() or QApplication([])


@pytest.fixture(params=["en", "he"])
def lang(request, monkeypatch):
    monkeypatch.setattr(genizah_core, "CURRENT_LANG", request.param)
    monkeypatch.setattr(app, "CURRENT_LANG", request.param, raising=False)
    return request.param


@pytest.fixture
def bar(lang, search_tab):  # noqa: F811  (the imported fixture)
    w, panel = search_tab
    rtl = lang == "he"
    panel.setLayoutDirection(Qt.LayoutDirection.RightToLeft if rtl else Qt.LayoutDirection.LeftToRight)
    panel.resize(1400, 700)
    QApplication.processEvents()
    w.query_input.setText("שלום")
    w.btn_search.setEnabled(True)           # as once the index has loaded
    QApplication.processEvents()
    started = []
    w.searcher = object()
    w.is_searching = False
    w.start_search = lambda: started.append(True)
    return w, panel, rtl, started


def _tab_through(panel, start, presses):
    start.setFocus(Qt.FocusReason.OtherFocusReason)
    QApplication.processEvents()
    seen = [panel.focusWidget()]
    for _ in range(presses):
        QTest.keyClick(panel.focusWidget(), Qt.Key.Key_Tab)
        QApplication.processEvents()
        seen.append(panel.focusWidget())
    return seen


def _row1_stops(w):
    """Row 1's controls in the order the owner reads them."""
    return [w.query_input, w.mode_combo, w.gap_input, w.btn_pre_search_filters,
            w.btn_search, w.btn_search_pause, w.corpus_scope_combo, w.sources_combo, w.btn_reset_search]


def test_tab_walks_row_one_in_visual_order(bar):
    w, panel, rtl, _ = bar
    stops = [s for s in _row1_stops(w) if s.isVisible() and s.isEnabled()]
    assert w.gap_input in stops and w.btn_search in stops
    seen = _tab_through(panel, w.query_input, len(stops) - 1)
    names = [getattr(x, "objectName", lambda: "")() or type(x).__name__ for x in seen]
    assert seen == stops, f"Tab went {names}"
    # ...and that order is what the screen shows: right to left in Hebrew.
    xs = [s.mapTo(panel, s.rect().center()).x() for s in stops]
    assert xs == sorted(xs, reverse=rtl), f"row 1 is not laid out {'RTL' if rtl else 'LTR'}: {xs}"


def test_tab_continues_from_row_one_into_row_two(bar):
    w, panel, _, _ = bar
    seen = _tab_through(panel, w.btn_reset_search, 1)
    first_row2 = [x for x in widgets_in_layout_order(w.search_row2.flow) if x.isVisible() and x.isEnabled()][0]
    assert seen[1] is first_row2


def test_shift_tab_goes_back_from_gap_to_mode(bar):
    w, panel, _, _ = bar
    w.gap_input.setFocus(Qt.FocusReason.OtherFocusReason)
    QApplication.processEvents()
    QTest.keyClick(w.gap_input, Qt.Key.Key_Backtab, Qt.KeyboardModifier.ShiftModifier)
    QApplication.processEvents()
    assert panel.focusWidget() is w.mode_combo


@pytest.mark.parametrize("field", ["gap_input", "exclude_input", "query_input"])
@pytest.mark.parametrize("key", [Qt.Key.Key_Return, Qt.Key.Key_Enter])
def test_enter_in_a_search_bar_field_starts_the_search(bar, field, key):
    w, panel, _, started = bar
    w.query_input.setText("שלום")
    target = getattr(w, field)
    if field == "gap_input":
        target.setText("3")
    target.setFocus(Qt.FocusReason.OtherFocusReason)
    QApplication.processEvents()
    QTest.keyClick(target, key)
    QApplication.processEvents()
    assert started == [True], f"Enter in {field} did not start the search"


# --- the helper itself --------------------------------------------------------------

def test_chain_layouts_follows_layout_order_not_parenting_order():
    host = QWidget()
    row = QHBoxLayout(host)
    a, c = QLineEdit(), QPushButton("c")
    row.addWidget(a)
    row.addWidget(QLabel("label, no focus"))
    row.addWidget(c)
    b = QLineEdit()                       # parented last, placed between a and c
    row.insertWidget(1, b)
    box = QWidget()                       # a container: its children are the stops
    inner = QVBoxLayout(box)
    d = QPushButton("d")
    inner.addWidget(d)
    row.insertWidget(0, box)
    assert widgets_in_layout_order(row) == [d, a, b, c]
    assert chain_layouts(row) == [d, a, b, c]
    host.show()
    d.setFocus(Qt.FocusReason.OtherFocusReason)
    seen = [host.focusWidget()]
    for _ in range(3):
        QTest.keyClick(host.focusWidget(), Qt.Key.Key_Tab)
        seen.append(host.focusWidget())
    host.hide()
    assert seen == [d, a, b, c]


# --- Joins Lab ----------------------------------------------------------------------

def _tab_from(host, start, presses):
    host.show()
    start.setFocus(Qt.FocusReason.OtherFocusReason)
    QApplication.processEvents()
    seen = [host.focusWidget()]
    for _ in range(presses):
        QTest.keyClick(host.focusWidget(), Qt.Key.Key_Tab)
        QApplication.processEvents()
        seen.append(host.focusWidget())
    return seen


def _line_widgets(entry):
    row = entry["widget"].layout()
    info, add_or = row.itemAt(0).widget(), row.itemAt(2).widget()
    return info, add_or


@pytest.mark.parametrize("lang_code", ["en", "he"])
def test_joins_lab_line_tabs_into_its_word_box_first(lang_code, monkeypatch):
    """Rows are added after the controls under them, so Tab reached a line's word
    box last, after + Add Line, the options and the preview."""
    monkeypatch.setattr(genizah_core, "CURRENT_LANG", lang_code)
    from desktop.join_workbench import JoinQueryBuilder
    b = JoinQueryBuilder(on_search=lambda: None, first_hint="hint", allow_page_position=True)
    if lang_code == "he":
        b.setLayoutDirection(Qt.LayoutDirection.RightToLeft)
    entry = b.rows[0]
    info, add_or = _line_widgets(entry)
    box = entry["boxes"][0]["edit"]
    seen = _tab_from(b, info, 2)
    b.hide()
    assert seen == [info, box, add_or]


def test_joins_lab_or_box_and_new_line_follow_in_order():
    from desktop.join_workbench import JoinQueryBuilder
    b = JoinQueryBuilder(on_search=lambda: None, first_hint="hint", allow_page_position=True)
    first = b.rows[0]
    b.add_or_box(first)
    second = b.add_row()
    box1, or_box = first["boxes"][0]["edit"], first["boxes"][1]["edit"]
    seen = _tab_from(b, box1, 1)
    assert seen[1] is or_box, "Tab from a word box skipped its OR alternative"
    # The first line's last control (×) leads into the second line, not to the end.
    seen = _tab_from(b, first["rm"], 2)
    b.hide()
    info2, _ = _line_widgets(second)
    assert seen[1] is info2 and seen[2] is second["boxes"][0]["edit"]


@pytest.mark.parametrize("key", [Qt.Key.Key_Return, Qt.Key.Key_Enter])
def test_enter_in_joins_lab_shelfmark_box_opens_it(key, monkeypatch):
    from unittest.mock import MagicMock

    import desktop.join_workbench as jw
    opened = []
    monkeypatch.setattr(jw.JoinWorkbenchWindow, "_cold_start_open", lambda self: opened.append(True))
    win = jw.JoinWorkbenchWindow(parent=None, app=MagicMock())
    win.coldstart_input.setText("T-S 12.123")
    win.show()
    win.coldstart_input.setFocus()
    QTest.keyClick(win.coldstart_input, key)
    QApplication.processEvents()
    win.hide()
    assert opened == [True]
