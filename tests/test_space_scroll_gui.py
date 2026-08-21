"""Phase 128 Space-scroll — GUI wiring test (gui slice).

Registered in conftest._GUI_TEST_FILES so it runs in the dedicated gui-tests job
(-m gui) and is excluded from the bulk slice (-m "not gui").

Contains exactly ONE test: the eventFilter wiring proof for the desktop
results-table Space-scroll branch.

RED until 128-02 adds the Key_Space branch to GenizahGUI.eventFilter.
Do NOT skip or stub this test.
"""
from __future__ import annotations

import pytest

pytestmark = pytest.mark.gui  # imports PyQt6: gui bucket only -- Qt in the mixed non-GUI run
# segfaults after thousands of NiceGUI/asyncio tests share the process (2026-08-21).

# NOTE: QApplication is created INSIDE the test (not at module scope) so that the
# bulk `-m "not gui"` slice — which still imports this file during collection before
# marker deselection — does not initialize Qt at import time (Codex CODE review LOW;
# matters given the project's PyQt6-headless-segfault sensitivity).


def test_desktop_eventfilter_triggers_scroll():
    """Prove the production GenizahGUI.eventFilter Space branch wires to verticalScrollBar().triggerAction.

    Strategy (no full GenizahGUI construction):
      - Create a bare QTableWidget with 10 rows to act as a proxy results_table.
      - Bind GenizahGUI.eventFilter as an unbound method and call it with a minimal
        mock object that exposes the few attributes the Space branch reads
        (results_table, COL_CHECKBOX), plus mock the verticalScrollBar().triggerAction.
      - Synthesise a Key_Space QKeyEvent for a non-checkbox column.
      - Assert triggerAction was called with SliderPageStepAdd and that the
        eventFilter returned True (event consumed).

    RED until 128-02 adds the Key_Space branch + QAbstractSlider import to genizah_app.py.
    """
    import unittest.mock as mock

    from PyQt6.QtCore import QEvent, Qt
    from PyQt6.QtGui import QKeyEvent
    from PyQt6.QtWidgets import QAbstractSlider, QApplication, QTableWidget

    _app = QApplication.instance() or QApplication([])  # noqa: F841 — Qt requires a live app for widgets

    # Import the real production eventFilter method and the decision helper.
    # Both land in 128-02; ImportError here is the expected RED state.
    import genizah_app as ga

    assert hasattr(ga, "space_scroll_action"), (
        "genizah_app.space_scroll_action not found — 128-02 has not landed yet"
    )
    assert hasattr(ga.GenizahGUI, "eventFilter"), (
        "GenizahGUI.eventFilter not found — unexpected"
    )

    # Build a minimal proxy object that the eventFilter Space branch reads.
    COL_CHECKBOX = 0

    results_table = QTableWidget(10, 5)
    results_table.setCurrentCell(2, 3)  # non-checkbox column

    proxy = mock.MagicMock()
    proxy.results_table = results_table
    proxy.COL_CHECKBOX = COL_CHECKBOX

    # Mock verticalScrollBar().triggerAction so we can assert it was called.
    mock_scrollbar = mock.MagicMock()
    results_table.verticalScrollBar = mock.MagicMock(return_value=mock_scrollbar)

    # Synthesise a Key_Space KeyPress event (no modifiers → page_down).
    key_event = QKeyEvent(QEvent.Type.KeyPress, Qt.Key.Key_Space, Qt.KeyboardModifier.NoModifier)

    # Drive the real eventFilter method against our proxy (unbound call).
    result = ga.GenizahGUI.eventFilter(proxy, results_table, key_event)

    # The branch must return True (event consumed) and call triggerAction with SliderPageStepAdd.
    assert result is True, (
        "eventFilter must return True (consume the event) when routing Space to page-scroll"
    )
    mock_scrollbar.triggerAction.assert_called_once_with(
        QAbstractSlider.SliderAction.SliderPageStepAdd
    )
