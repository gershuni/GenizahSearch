"""Text size (owner, 2026-10-24: "option to change the size of the texts in search
etc windows").

Pinned here: the result lists take one size level (Settings > Text size in result
lists, Ctrl+= / Ctrl+- / Ctrl+0), their headers keep the window's font, table rows
grow when the text needs it, and snippet cells follow -- the ones already showing
and the ones still to come, also under the Composition tree's style sheet, which
keeps Qt from passing a font on; on the Browse tab the same keys size the
transcription, which shares one saved size with the Manuscript Viewer (A- / A+ in
either changes both).
"""

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PyQt6.QtGui import QFontInfo
from PyQt6.QtWidgets import (
    QApplication, QComboBox, QTableWidget, QTabWidget, QTextEdit, QToolButton,
    QTreeWidget, QTreeWidgetItem, QWidget,
)

import genizah_app
from desktop import result_dialog as rd
from desktop.ui_widgets import ContextSnippet, MatchCentredSnippet
from genizah_app import GenizahGUI

pytestmark = pytest.mark.gui

_APP = QApplication.instance() or QApplication([])


class _Window:
    """The parts of GenizahGUI the text size uses, with the real methods."""

    RESULTS_TEXT_KEY = GenizahGUI.RESULTS_TEXT_KEY
    RESULTS_TEXT_STEPS = GenizahGUI.RESULTS_TEXT_STEPS
    results_text_level = GenizahGUI.results_text_level
    _results_text_widgets = GenizahGUI._results_text_widgets
    _apply_results_text_level = GenizahGUI._apply_results_text_level
    _snippet_cells = GenizahGUI.__dict__["_snippet_cells"]
    set_results_text_level = GenizahGUI.set_results_text_level
    _step_text_size = GenizahGUI._step_text_size
    _browse_step_text_size = GenizahGUI._browse_step_text_size
    _browse_apply_text_size = GenizahGUI._browse_apply_text_size

    def __init__(self):
        self.tabs = QTabWidget()
        self.results_table = QTableWidget(3, 2)
        for r in range(3):
            self.results_table.setCellWidget(r, 1, MatchCentredSnippet("a <b>b</b> c"))
        self.comp_tree = QTreeWidget()
        self.comp_tree.setColumnCount(2)
        # The Composition tree's own kind of style sheet: it stops Qt passing a
        # font on to the cells.
        self.comp_tree.setStyleSheet("QTreeWidget::indicator { width: 16px; height: 16px; }")
        node = QTreeWidgetItem(self.comp_tree, ["12"])
        self.comp_tree.setItemWidget(node, 1, ContextSnippet("x *y* z"))
        child = QTreeWidgetItem(node, ["3"])
        self.comp_tree.setItemWidget(child, 1, ContextSnippet("p *q* r"))
        self.browse_tab = QWidget()
        self.browse_text = QTextEdit()
        self.btn_b_text_smaller = QToolButton()
        self.btn_b_text_larger = QToolButton()
        for page in (self.results_table, self.comp_tree, self.browse_tab):
            self.tabs.addTab(page, "")
        combo = QComboBox()
        combo.addItems(["Normal", "Large", "Larger", "Largest"])
        self.settings_dialog = type("Dlg", (), {"combo_results_text": combo})()
        self.tabs.resize(900, 500)
        self.tabs.show()
        QApplication.processEvents()


@pytest.fixture
def window(monkeypatch):
    cfg = {}
    monkeypatch.setattr(genizah_app, "save_app_config", cfg.update)
    monkeypatch.setattr(genizah_app, "load_app_config", lambda: dict(cfg))
    monkeypatch.setattr(rd, "save_app_config", cfg.update)
    monkeypatch.setattr(rd, "load_app_config", lambda: dict(cfg))
    w = _Window()
    w.cfg = cfg
    return w


def _pt(widget):
    return widget.font().pointSizeF()


def test_a_level_sizes_the_lists_but_not_their_headers(window):
    w = window
    base = QFontInfo(QApplication.font()).pointSizeF()
    w.set_results_text_level(3)
    for view in (w.results_table, w.comp_tree):
        assert _pt(view) == base + 6
    assert _pt(w.results_table.horizontalHeader()) == base
    assert _pt(w.comp_tree.header()) == base
    assert w.cfg == {"results_text_level": 3}
    assert w.settings_dialog.combo_results_text.currentIndex() == 3
    w.set_results_text_level(0)
    assert _pt(w.results_table) == base and _pt(w.comp_tree) == base


def test_snippet_cells_follow_including_under_a_style_sheet(window):
    w = window
    w.set_results_text_level(2)
    QApplication.processEvents()
    cells = list(w._snippet_cells(w.results_table)) + list(w._snippet_cells(w.comp_tree))
    assert len(cells) == 5
    want = QFontInfo(QApplication.font()).pointSizeF() + 4
    for cell in cells:
        assert _pt(cell) == want
        assert _pt(cell.label()) == want          # the label is what draws the text
    # A Composition cell is one line of its font tall, whatever the size.
    ctx = w.comp_tree.itemWidget(w.comp_tree.topLevelItem(0), 1)
    assert ctx.height() == ctx.fontMetrics().lineSpacing() + 4
    # Cells added afterwards (the next batch of results) start at the size too.
    node = QTreeWidgetItem(w.comp_tree, ["9"])
    late = ContextSnippet("m *n*")
    w.comp_tree.setItemWidget(node, 1, late)
    w.results_table.setRowCount(4)
    late_row = MatchCentredSnippet("d e")
    w.results_table.setCellWidget(3, 1, late_row)
    QApplication.processEvents()
    assert _pt(late.label()) == want and _pt(late_row.label()) == want


def test_table_rows_grow_with_their_text_and_never_shrink_below_the_style(window):
    w = window
    floor = w.results_table.verticalHeader().defaultSectionSize()
    w.set_results_text_level(3)
    from PyQt6.QtGui import QFontMetrics
    need = QFontMetrics(w.results_table.font()).height() + 10
    assert w.results_table.rowHeight(0) == max(floor, need)
    w.set_results_text_level(0)
    assert w.results_table.rowHeight(0) == floor


def test_keys_step_the_lists_and_clamp(window):
    w = window
    w.tabs.setCurrentWidget(w.results_table)
    for _ in range(6):
        w._step_text_size(1)                     # Ctrl+=
    assert w.results_text_level == len(w.RESULTS_TEXT_STEPS) - 1
    w._step_text_size(-1)                        # Ctrl+-
    assert w.results_text_level == len(w.RESULTS_TEXT_STEPS) - 2
    w._step_text_size(0)                         # Ctrl+0
    assert w.results_text_level == 0 and w.cfg["results_text_level"] == 0
    w._step_text_size(-1)
    assert w.results_text_level == 0


def test_on_the_browse_tab_the_keys_size_the_transcription_shared_with_the_viewer(window):
    w = window
    seen = []
    rd.reading_text_size().changed.connect(seen.append)   # an open Manuscript Viewer
    rd.reading_text_size().changed.connect(w._browse_apply_text_size)
    try:
        w._browse_apply_text_size(16)
        w.tabs.setCurrentWidget(w.browse_tab)
        w._step_text_size(1)
        assert w.browse_text.font().pointSize() == 16 + rd.TEXT_PT_STEP
        assert w.cfg == {rd.VIEWER_TEXT_PT_KEY: 16 + rd.TEXT_PT_STEP}   # the viewer's key
        assert seen == [16 + rd.TEXT_PT_STEP]
        assert w.results_text_level == 0          # the lists are untouched
        w._step_text_size(0)
        assert w.browse_text.font().pointSize() == rd.TEXT_PT_DEFAULT
        for _ in range(30):
            w._browse_step_text_size(rd.TEXT_PT_STEP)
        assert w.browse_text.font().pointSize() == rd.TEXT_PT_MAX
        assert not w.btn_b_text_larger.isEnabled() and w.btn_b_text_smaller.isEnabled()
    finally:
        rd.reading_text_size().changed.disconnect(seen.append)
        rd.reading_text_size().changed.disconnect(w._browse_apply_text_size)


def test_an_unreadable_saved_level_falls_back_to_normal(window):
    w = window
    w._apply_results_text_level("big")
    assert w.results_text_level == 0
    w._apply_results_text_level(99)
    assert w.results_text_level == len(w.RESULTS_TEXT_STEPS) - 1


def test_main_window_wiring():
    from pathlib import Path
    src = (Path(__file__).resolve().parent.parent / "genizah_app.py").read_text(encoding="utf-8")
    init_ui = src[src.index("    def init_ui(self):"):src.index("    def _update_corner_login_state(self):")]
    for keys in ('"Ctrl+="', '"Ctrl++"', '"Ctrl+-"', '"Ctrl+0"'):
        assert keys in init_ui
    assert "self._apply_results_text_level(load_app_config().get(self.RESULTS_TEXT_KEY, 0))" in init_ui
    assert 'QFont("SBL Hebrew", reading_text_size().value())' in src
    assert "reading_text_size().changed.connect(self._browse_apply_text_size)" in src
