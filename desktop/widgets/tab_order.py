# -*- coding: utf-8 -*-
"""Tab order that follows the layout, i.e. what the user sees.

Qt's default Tab order is the order in which widgets got their parent, not where
they sit. A widget inserted into a row after the row's other widgets were added
(``insertWidget``), or a container filled before it was put in place, lands at the
end of that chain: in the Search tab Tab went query -> mode -> Search and skipped
the Gap field and Focus Search that had been inserted between them (owner, v10.0.0).

``chain_layouts`` walks the given layouts in order (a nested layout, or a container
widget's own layout, in place) and chains every widget that takes Tab focus. Layout
order is visual order in both directions: a QBoxLayout and FlowLayout mirror their
items in a right-to-left window, so Tab runs right to left in Hebrew, left to right
in English, with one chain. Hidden and disabled widgets stay in the chain; Qt skips
them while they are hidden or disabled, so widgets that show and hide themselves
need no re-chaining.
"""
from __future__ import annotations

from PyQt6.QtCore import Qt
from PyQt6.QtWidgets import QLayout, QWidget


def _takes_tab(widget: QWidget) -> bool:
    return bool(widget.focusPolicy() & Qt.FocusPolicy.TabFocus)


def widgets_in_layout_order(layout: QLayout) -> list[QWidget]:
    """Every widget in ``layout`` that takes Tab focus, in layout order.

    A widget that takes focus itself is one stop (an editable combo's line edit
    is its focus proxy, not a stop of its own); one that does not (a plain
    container) is replaced by the widgets of its own layout.
    """
    found: list[QWidget] = []
    for i in range(layout.count()):
        item = layout.itemAt(i)
        widget = item.widget()
        if widget is not None:
            if _takes_tab(widget):
                found.append(widget)
            elif widget.layout() is not None:
                found.extend(widgets_in_layout_order(widget.layout()))
        elif item.layout() is not None:
            found.extend(widgets_in_layout_order(item.layout()))
    return found


def chain_tab_order(widgets) -> list[QWidget]:
    """Make Tab go through ``widgets`` in the given order; returns the chain."""
    chain = [w for w in widgets if w is not None]
    for first, second in zip(chain, chain[1:]):
        QWidget.setTabOrder(first, second)
    return chain


def chain_layouts(*layouts: QLayout) -> list[QWidget]:
    """Chain the Tab-focusable widgets of ``layouts``, one after another."""
    widgets: list[QWidget] = []
    for layout in layouts:
        if layout is not None:
            widgets.extend(widgets_in_layout_order(layout))
    return chain_tab_order(widgets)
