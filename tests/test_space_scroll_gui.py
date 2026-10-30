"""Space in the desktop Search results -- GUI wiring test (gui slice).

Registered in conftest._GUI_TEST_FILES so it runs in the dedicated gui-tests job
(-m gui) and is excluded from the bulk slice (-m "not gui").

Phase 128 made Space page the results; since 2026-10-30 (owner) it ticks the
current result's box instead, in any column, and Page Down pages. This is the
eventFilter wiring proof for that branch: the REAL GenizahGUI.eventFilter and
key methods, driven on a bare QTableWidget. The rest of the result-list keys
(Enter, Ctrl+Enter, Ctrl+A, Ctrl+Space, Shift+Space) are pinned in
tests/test_quick_open.py.
"""
from __future__ import annotations

import pytest

pytestmark = pytest.mark.gui  # imports PyQt6: gui bucket only -- Qt in the mixed non-GUI run
# segfaults after thousands of NiceGUI/asyncio tests share the process (2026-08-21).

# NOTE: QApplication is created INSIDE the test (not at module scope) so that the
# bulk `-m "not gui"` slice — which still imports this file during collection before
# marker deselection — does not initialize Qt at import time (Codex CODE review LOW;
# matters given the project's PyQt6-headless-segfault sensitivity).


def test_desktop_eventfilter_space_ticks_the_current_result():
    from PyQt6.QtCore import QEvent, Qt
    from PyQt6.QtGui import QKeyEvent
    from PyQt6.QtWidgets import QApplication, QTableWidget, QTableWidgetItem

    _app = QApplication.instance() or QApplication([])  # noqa: F841 — Qt requires a live app for widgets

    import genizah_app as ga

    class _Proxy:
        _KEY_MODIFIERS = ga.GenizahGUI._KEY_MODIFIERS
        _enter_action = ga.GenizahGUI._enter_action
        _results_table_key = ga.GenizahGUI._results_table_key
        _tick_current_result = ga.GenizahGUI._tick_current_result
        eventFilter = ga.GenizahGUI.eventFilter
        COL_CHECKBOX = 0

    proxy = _Proxy()
    proxy.results_table = QTableWidget(10, 5)
    for row in range(10):
        box = QTableWidgetItem()
        box.setFlags(Qt.ItemFlag.ItemIsUserCheckable | Qt.ItemFlag.ItemIsEnabled)
        box.setCheckState(Qt.CheckState.Unchecked)
        proxy.results_table.setItem(row, 0, box)
    proxy.results_table.setCurrentCell(2, 3)  # a text column, not the box
    scrolled = proxy.results_table.verticalScrollBar().value()

    space = QKeyEvent(QEvent.Type.KeyPress, Qt.Key.Key_Space, Qt.KeyboardModifier.NoModifier)
    assert proxy.eventFilter(proxy.results_table, space) is True, "Space must be taken"
    assert proxy.results_table.item(2, 0).checkState() == Qt.CheckState.Checked
    assert proxy.results_table.verticalScrollBar().value() == scrolled, "Space no longer pages"
    assert proxy.eventFilter(proxy.results_table, space) is True
    assert proxy.results_table.item(2, 0).checkState() == Qt.CheckState.Unchecked
