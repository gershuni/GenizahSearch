# -*- coding: utf-8 -*-
"""Tests for desktop/widgets/flow_layout.py.

FlowLayout is a dependency-free, RTL-aware wrapping layout for desktop toolbars
and filter rows that must not clip or push the window wider on low-resolution
or high-DPI screens. These tests pin: single-row placement, wrapping at narrow
widths, heightForWidth agreeing with the real setGeometry() pass, hidden
widgets taking no space, RTL row order and right-edge anchoring, minimumSize /
sizeHint formulas, and FlowWidget's height-for-width growth inside a
QVBoxLayout.
"""

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PyQt6.QtCore import Qt
from PyQt6.QtWidgets import QApplication, QPushButton, QVBoxLayout, QWidget

from desktop.widgets.flow_layout import FlowLayout, FlowWidget

pytestmark = pytest.mark.gui

_APP = QApplication.instance() or QApplication([])


def _button(parent, width=60, height=24, text="x"):
    btn = QPushButton(text, parent)
    btn.setFixedSize(width, height)
    return btn


def _make_flow(parent, count=5, margin=0, h_spacing=5, v_spacing=5, width=60, height=24):
    flow = FlowLayout(parent, margin, h_spacing, v_spacing)
    buttons = []
    for i in range(count):
        btn = _button(parent, width, height, text=str(i))
        flow.addWidget(btn)
        buttons.append(btn)
    return flow, buttons


def test_wide_width_single_row_left_to_right():
    parent = QWidget()
    flow, buttons = _make_flow(parent, count=4, width=60, height=24, h_spacing=5, v_spacing=5)
    flow.setGeometry(flow.geometry().__class__(0, 0, 1000, 0))

    ys = [b.geometry().y() for b in buttons]
    xs = [b.geometry().x() for b in buttons]
    assert len(set(ys)) == 1  # all on the same row
    assert xs == sorted(xs)  # left-to-right in insertion order
    for i in range(1, len(buttons)):
        assert xs[i] == xs[i - 1] + 60 + 5


def test_narrow_width_wraps_and_height_for_width_matches_setgeometry():
    parent = QWidget()
    # Each button is 60 wide; a 140px row fits 2 buttons (60+5+60=125) but not 3.
    flow, buttons = _make_flow(parent, count=5, width=60, height=24, h_spacing=5, v_spacing=5)

    from PyQt6.QtCore import QRect

    width = 140
    computed_height = flow.heightForWidth(width)
    flow.setGeometry(QRect(0, 0, width, computed_height))

    rows_y = sorted(set(b.geometry().y() for b in buttons))
    assert len(rows_y) > 1  # wrapped onto multiple rows

    last_row_bottom = max(b.geometry().y() + b.geometry().height() for b in buttons)
    assert computed_height == last_row_bottom

    # Items within a row still go left-to-right in order.
    row0 = [b for b in buttons if b.geometry().y() == rows_y[0]]
    xs0 = [b.geometry().x() for b in row0]
    assert xs0 == sorted(xs0)


def test_hidden_widgets_take_no_space_and_unhiding_reflows():
    parent = QWidget()
    flow, buttons = _make_flow(parent, count=3, width=60, height=24, h_spacing=5, v_spacing=5)
    b0, b1, b2 = buttons

    from PyQt6.QtCore import QRect

    b1.hide()
    flow.setGeometry(QRect(0, 0, 1000, 0))
    # b2 should sit right after b0, as if b1 were never there.
    assert b2.geometry().x() == b0.geometry().x() + 60 + 5

    size_with_hidden = flow.sizeHint()

    b1.show()
    flow.setGeometry(QRect(0, 0, 1000, 0))
    size_with_shown = flow.sizeHint()
    assert size_with_shown.width() > size_with_hidden.width()
    # After showing, b1 takes its place between b0 and b2 again.
    assert b1.geometry().x() == b0.geometry().x() + 60 + 5
    assert b2.geometry().x() == b1.geometry().x() + 60 + 5


def test_hidden_widget_does_not_count_toward_height():
    parent = QWidget()
    flow = FlowLayout(parent, 0, 5, 5)
    tall = _button(parent, 60, 100, "tall")
    flow.addWidget(tall)
    tall.hide()

    from PyQt6.QtCore import QRect

    flow.setGeometry(QRect(0, 0, 1000, 0))
    assert flow.heightForWidth(1000) == 0


def test_rtl_single_row_right_to_left():
    parent = QWidget()
    parent.setLayoutDirection(Qt.LayoutDirection.RightToLeft)
    flow, buttons = _make_flow(parent, count=3, width=60, height=24, h_spacing=5, v_spacing=5)

    from PyQt6.QtCore import QRect

    total_width = 1000
    flow.setGeometry(QRect(0, 0, total_width, 0))

    # First item (insertion order) sits at the right edge.
    b0, b1, b2 = buttons
    assert b0.geometry().right() == total_width - 1
    # Logical order preserved: b1 is to the left of b0, b2 left of b1.
    assert b1.geometry().right() < b0.geometry().x()
    assert b2.geometry().right() < b1.geometry().x()


def test_rtl_wrapped_rows_start_from_the_right():
    parent = QWidget()
    parent.setLayoutDirection(Qt.LayoutDirection.RightToLeft)
    flow, buttons = _make_flow(parent, count=5, width=60, height=24, h_spacing=5, v_spacing=5)

    from PyQt6.QtCore import QRect

    width = 140
    computed_height = flow.heightForWidth(width)
    flow.setGeometry(QRect(0, 0, width, computed_height))

    rows_y = sorted(set(b.geometry().y() for b in buttons))
    assert len(rows_y) > 1

    for row_y in rows_y:
        row = [b for b in buttons if b.geometry().y() == row_y]
        rightmost = max(b.geometry().right() for b in row)
        assert rightmost == width - 1


def test_minimum_size_is_widest_single_item_plus_margins():
    parent = QWidget()
    flow = FlowLayout(parent, 4, 5, 5)
    _button(parent, 50, 20, "a")
    _button(parent, 90, 20, "b")
    _button(parent, 30, 20, "c")
    for w in parent.findChildren(QPushButton):
        flow.addWidget(w)

    min_size = flow.minimumSize()
    assert min_size.width() == 90 + 4 * 2
    assert min_size.height() == 20 + 4 * 2


def test_size_hint_is_one_row_size():
    parent = QWidget()
    flow, buttons = _make_flow(parent, count=3, width=60, height=24, margin=3, h_spacing=5, v_spacing=5)

    hint = flow.sizeHint()
    expected_width = 3 * 60 + 2 * 5 + 2 * 3
    expected_height = 24 + 2 * 3
    assert hint.width() == expected_width
    assert hint.height() == expected_height


def test_flow_widget_height_for_width_in_vbox_layout():
    host = QWidget()
    layout = QVBoxLayout(host)
    flow_widget = FlowWidget(margin=0, h_spacing=5, v_spacing=5)
    for i in range(6):
        flow_widget.add_widget(_button(None, 60, 24, str(i)))
    layout.addWidget(flow_widget)

    host.resize(1000, 200)
    host.show()
    QApplication.processEvents()
    wide_height = flow_widget.height()

    host.resize(140, 400)
    QApplication.processEvents()
    narrow_height = flow_widget.height()

    assert narrow_height > wide_height
    host.close()


def test_an_exactly_full_row_does_not_wrap_its_last_item():
    """Codex, #362: QRect.right() is inclusive, so the old comparison wrapped the
    last item of a row that fit exactly, and heightForWidth(sizeHint().width())
    reported two rows for a one-row layout."""
    from desktop.widgets.flow_layout import FlowLayout
    host = QWidget()
    lay = FlowLayout(host, margin=0, h_spacing=6, v_spacing=6)
    for _ in range(3):
        b = QPushButton("x")
        b.setFixedSize(100, 30)
        lay.addWidget(b)
    one_row = lay.sizeHint().width()
    assert lay.heightForWidth(one_row) == 30
    assert lay.heightForWidth(one_row - 1) > 30
