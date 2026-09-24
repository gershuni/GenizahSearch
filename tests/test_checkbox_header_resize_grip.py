"""CheckBoxHeader: a press on a column's resize grip must reach Qt (2026-09-24).

The header returns early for the checkbox column and for non-sortable columns so
a click there does not sort. It also swallowed presses on those columns' own
resize grip, so the Img column (and Actions, where resizable) could not be
widened from its edge -- in LTR and in RTL, with or without a hidden column.
"""

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PyQt6.QtCore import QEvent, QPoint, QPointF, Qt
from PyQt6.QtGui import QMouseEvent
from PyQt6.QtWidgets import QApplication, QTableWidget

from desktop.ui_widgets import CheckBoxHeader

pytestmark = pytest.mark.gui

_APP = QApplication.instance() or QApplication([])


def _ev(kind, pos):
    buttons = (Qt.MouseButton.NoButton if kind == QEvent.Type.MouseButtonRelease
               else Qt.MouseButton.LeftButton)
    return QMouseEvent(kind, QPointF(pos), QPointF(pos), Qt.MouseButton.LeftButton,
                       buttons, Qt.KeyboardModifier.NoModifier)


def _table(rtl, hide):
    t = QTableWidget(3, 6)
    if rtl:
        t.setLayoutDirection(Qt.LayoutDirection.RightToLeft)
    h = CheckBoxHeader(t, non_sortable_cols=[0, 1, 4])
    t.setHorizontalHeader(h)
    for c in range(6):
        t.setColumnWidth(c, 100)
    if hide is not None:
        t.setColumnHidden(hide, True)
    t.resize(900, 300)
    t.show()
    QApplication.processEvents()
    return t, h


@pytest.mark.parametrize("rtl", [False, True])
@pytest.mark.parametrize("hide", [None, 2])
@pytest.mark.parametrize("col", [1, 3, 4])      # 1 and 4 are non-sortable
def test_dragging_a_columns_end_edge_widens_that_column(rtl, hide, col):
    t, h = _table(rtl, hide)
    start, width = h.sectionViewportPosition(col), h.sectionSize(col)
    # A section's end edge is on its right in LTR and on its left in RTL.
    edge = (start + 1) if rtl else (start + width - 2)
    y = h.height() // 2
    before = [h.sectionSize(c) for c in range(6)]
    drag = -30 if rtl else 30                    # outward
    h.mousePressEvent(_ev(QEvent.Type.MouseButtonPress, QPoint(edge, y)))
    h.mouseMoveEvent(_ev(QEvent.Type.MouseMove, QPoint(edge + drag, y)))
    h.mouseReleaseEvent(_ev(QEvent.Type.MouseButtonRelease, QPoint(edge + drag, y)))
    after = [h.sectionSize(c) for c in range(6)]
    assert {c: after[c] - before[c] for c in range(6) if after[c] != before[c]} == {col: 30}


def test_a_click_inside_a_non_sortable_column_still_does_not_sort():
    t, h = _table(False, None)
    t.setSortingEnabled(True)
    h.setSortIndicator(3, Qt.SortOrder.AscendingOrder)
    mid = QPoint(h.sectionViewportPosition(4) + 50, h.height() // 2)
    h.mousePressEvent(_ev(QEvent.Type.MouseButtonPress, mid))
    h.mouseReleaseEvent(_ev(QEvent.Type.MouseButtonRelease, mid))
    assert h.sortIndicatorSection() == 3
