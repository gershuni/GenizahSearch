# -*- coding: utf-8 -*-
"""A dependency-free, wrapping "flow" layout for the PyQt6 desktop app.

Qt ships no flow layout out of the box -- only the Qt documentation's own
"Flow Layout" C++ example, which every Python port copies verbatim, bugs and
all. We need one because several desktop toolbars and filter rows (library
chips, filter pills, Composition witness controls) hold a variable number of
small controls: on a low-resolution screen, or at 300% Windows display
scaling, a fixed QHBoxLayout either clips those controls or pushes the whole
window wider than the screen. A flow layout wraps them onto additional rows
instead, growing the container's height rather than its width.

This is a corrected reimplementation, not a straight port: it skips hidden
widgets (so toggling a control's visibility does not leave a gap or reserve
space for it), and it is RTL-aware, because large parts of this app's UI are
Hebrew and read right-to-left -- a flow layout that always fills left-to-right
would misorder rows for those screens.
"""

from __future__ import annotations

from PyQt6.QtCore import QPoint, QRect, QSize, Qt
from PyQt6.QtGui import QGuiApplication
from PyQt6.QtWidgets import QLayout, QSizePolicy, QStyle, QWidget


class FlowLayout(QLayout):
    """A QLayout that arranges its items left-to-right (or right-to-left),
    wrapping onto additional rows when it runs out of horizontal space.
    """

    def __init__(self, parent=None, margin=-1, h_spacing=-1, v_spacing=-1):
        super().__init__(parent)
        self._h_space = h_spacing
        self._v_space = v_spacing
        self._items = []
        if margin >= 0:
            self.setContentsMargins(margin, margin, margin, margin)

    # No __del__ draining the items (the C++ example has a destructor that does):
    # a Python wrapper can be collected while Qt still owns and uses the layout,
    # and emptying it then would strip the controls out of a live window.

    def addItem(self, item):
        self._items.append(item)

    def horizontalSpacing(self):
        if self._h_space >= 0:
            return self._h_space
        return self._smart_spacing(QStyle.PixelMetric.PM_LayoutHorizontalSpacing)

    def verticalSpacing(self):
        if self._v_space >= 0:
            return self._v_space
        return self._smart_spacing(QStyle.PixelMetric.PM_LayoutVerticalSpacing)

    def count(self):
        return len(self._items)

    def itemAt(self, index):
        if 0 <= index < len(self._items):
            return self._items[index]
        return None

    def takeAt(self, index):
        if 0 <= index < len(self._items):
            return self._items.pop(index)
        return None

    def expandingDirections(self):
        return Qt.Orientation(0)

    def hasHeightForWidth(self):
        return True

    def heightForWidth(self, width):
        return self._do_layout(QRect(0, 0, width, 0), True)

    def setGeometry(self, rect):
        super().setGeometry(rect)
        self._do_layout(rect, False)

    def sizeHint(self):
        return self._one_row_size()

    def minimumSize(self):
        size = QSize()
        for item in self._items:
            widget = item.widget()
            if widget is not None and widget.isHidden():
                continue
            size = size.expandedTo(item.minimumSize())
        margins = self.contentsMargins()
        size += QSize(
            margins.left() + margins.right(), margins.top() + margins.bottom()
        )
        return size

    # -- internals ---------------------------------------------------

    def _visible_items(self):
        for item in self._items:
            widget = item.widget()
            if widget is not None and widget.isHidden():
                continue
            yield item

    def _one_row_size(self):
        """The size of laying every visible item out on a single row."""
        margins = self.contentsMargins()
        h_space = self.horizontalSpacing()
        if h_space < 0:
            h_space = self._fallback_spacing()
        v_space = self.verticalSpacing()
        if v_space < 0:
            v_space = self._fallback_spacing()

        total_width = 0
        max_height = 0
        first = True
        for item in self._visible_items():
            item_size = item.sizeHint().expandedTo(item.minimumSize())
            if not first:
                total_width += h_space
            total_width += item_size.width()
            max_height = max(max_height, item_size.height())
            first = False

        size = QSize(total_width, max_height)
        size += QSize(
            margins.left() + margins.right(), margins.top() + margins.bottom()
        )
        return size

    def _is_rtl(self):
        parent = self.parentWidget()
        if parent is not None:
            return parent.layoutDirection() == Qt.LayoutDirection.RightToLeft
        return QGuiApplication.layoutDirection() == Qt.LayoutDirection.RightToLeft

    def _fallback_spacing(self):
        return 6

    def _smart_spacing(self, pm):
        parent = self.parent()
        if parent is None:
            return self._fallback_spacing()
        if isinstance(parent, QWidget):
            style = parent.style()
            spacing = style.pixelMetric(pm, None, parent)
            if spacing < 0:
                return self._fallback_spacing()
            return spacing
        # A parent QLayout: fall back to its own geometry-owning widget.
        spacing = parent.spacing()
        if spacing < 0:
            return self._fallback_spacing()
        return spacing

    def _do_layout(self, rect, test_only):
        margins = self.contentsMargins()
        effective_rect = rect.adjusted(
            margins.left(), margins.top(), -margins.right(), -margins.bottom()
        )
        rtl = self._is_rtl()

        x = effective_rect.x()
        y = effective_rect.y()
        line_height = 0
        h_space = self.horizontalSpacing()
        v_space = self.verticalSpacing()
        if h_space < 0:
            h_space = self._fallback_spacing()
        if v_space < 0:
            v_space = self._fallback_spacing()

        row_items = []

        def flush_row(row_y, row_height):
            """Place the buffered row items.

            Items are laid out left-to-right first (``left`` is that
            position). For RTL, each item's x is then mirrored around the
            layout's own rect -- not per-row -- so every row's leading edge
            (first inserted item) lands on the same right edge, exactly like
            QStyle.visualRect would, without calling it per item.
            """
            if not row_items or test_only:
                return
            for it, size, left in row_items:
                if rtl:
                    right_ltr = left + size.width() - 1
                    x0 = effective_rect.left() + effective_rect.right() - right_ltr
                else:
                    x0 = left
                it.setGeometry(
                    QRect(
                        QPoint(x0, row_y + (row_height - size.height()) // 2),
                        size,
                    )
                )

        for item in self._visible_items():
            item_size = item.sizeHint().expandedTo(item.minimumSize())
            next_x = x + item_size.width() + h_space

            # The item's right edge is next_x - h_space - 1 (QRect.right() is
            # inclusive): an exactly-full row must not wrap its last item (Codex, #362).
            if next_x - h_space - 1 > effective_rect.right() and line_height > 0:
                flush_row(y, line_height)
                row_items = []
                x = effective_rect.x()
                y = y + line_height + v_space
                next_x = x + item_size.width() + h_space
                line_height = 0

            row_items.append((item, item_size, x))
            x = next_x
            line_height = max(line_height, item_size.height())

        flush_row(y, line_height)

        return y + line_height - rect.y() + margins.bottom()


class FlowWidget(QWidget):
    """A QWidget that owns a FlowLayout and reports height-for-width to its
    own parent layout, so a QVBoxLayout containing it grows the widget's
    height instead of clipping wrapped rows.
    """

    def __init__(self, parent=None, margin=-1, h_spacing=-1, v_spacing=-1):
        super().__init__(parent)
        self.flow = FlowLayout(self, margin, h_spacing, v_spacing)
        self.setLayout(self.flow)
        policy = QSizePolicy(QSizePolicy.Policy.Preferred, QSizePolicy.Policy.Preferred)
        policy.setHeightForWidth(True)
        self.setSizePolicy(policy)

    def add_widget(self, widget):
        self.flow.addWidget(widget)

    def hasHeightForWidth(self):
        return True

    def heightForWidth(self, width):
        return self.flow.heightForWidth(width)
