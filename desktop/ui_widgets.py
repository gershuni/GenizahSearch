# -*- coding: utf-8 -*-
"""Reusable PyQt6 widget subclasses (extracted from genizah_app.py, Phase 126 D1).

Provides four top-level widget subclasses moved verbatim out of the
28K-line ``genizah_app.py`` god file:

  - ShelfmarkTableWidgetItem(QTableWidgetItem) — natural-sort shelfmark cells
  - CheckBoxHeader(QHeaderView) — header with a master checkbox + filter/star glyphs
  - HiddenScrollArea(QScrollArea) — single-line RTL snippet with marker highlighting
  - ListsTreeWidget(QTreeWidget) — drag-reorderable lists sidebar tree

ZERO behavior change vs. the originals. ``genizah_app.py`` re-exports these
via a plain re-export import (MOVE-and-shim, mirroring genizah_core 122-125; the
Phase-126 D1 ``# noqa: F401`` marker was retired in Phase 127 — the classes are
used internally so the import is no longer a bare re-export).

GUARD-01: NO module-level ``import genizah_app`` — shared symbols come from the
``genizah_core`` facade only.
"""
from __future__ import annotations

import re

from PyQt6.QtGui import QFontMetrics
from PyQt6.QtWidgets import (
    QTableWidgetItem,
    QHeaderView,
    QScrollArea,
    QTreeWidget,
    QFrame,
    QLabel,
    QStyle,
    QStyleOptionButton,
    QToolTip,
    QAbstractItemView,
)
from PyQt6.QtCore import Qt, QRect, QEvent, QTimer, pyqtSignal
from PyQt6.QtGui import QColor, QPalette, QPen, QBrush, QPainterPath

from genizah_core import natural_sort_key, tr


class ElidingLabel(QLabel):
    """A single-line QLabel that elides with "..." instead of clipping silently.

    A QLabel with word-wrap off does NOT elide: it paints what fits and stops,
    so a cut string is indistinguishable from a short one. That matters
    wherever the text is a citation -- a reader who cannot see that the DOI was
    cut off will paste a citation that ends mid-author-list.

    The full text is kept in `full_text` and re-elided on every resize, so the
    label shows as much as the current window allows. Anything that needs the
    whole string (a copy action, a tooltip) must read `full_text`, never
    `text()`, which is the elided form by design.
    """

    def __init__(self, text='', parent=None,
                 mode=Qt.TextElideMode.ElideRight):
        super().__init__(parent)
        self._mode = mode
        self.full_text = ''
        self.setText(text)

    def setText(self, text):                                # noqa: N802 (Qt)
        self.full_text = text or ''
        self._apply_elide()

    def resizeEvent(self, event):                           # noqa: N802 (Qt)
        super().resizeEvent(event)
        self._apply_elide()

    def showEvent(self, event):                             # noqa: N802 (Qt)
        # Qt does not deliver resize events to a HIDDEN widget -- it coalesces
        # them -- so a label resized before it is first shown would still be
        # carrying the elision it computed at whatever width it had when it was
        # built. Re-eliding on show closes that, and costs one call.
        super().showEvent(event)
        self._apply_elide()

    def _apply_elide(self):
        width = max(0, self.width())
        if not width:
            # Before the first layout pass there is no width to elide against;
            # showing the full string here is right -- resizeEvent re-elides as
            # soon as one exists, and a label that started empty would flash.
            super().setText(self.full_text)
            return
        metrics = QFontMetrics(self.font())
        super().setText(metrics.elidedText(self.full_text, self._mode, width))


class ShelfmarkTableWidgetItem(QTableWidgetItem):
    """Custom item for sorting shelfmarks by ignoring 'Ms.' prefix and case."""
    def __lt__(self, other):
        text1 = self.text()
        text2 = other.text()
        return natural_sort_key(text1) < natural_sort_key(text2)

class CheckBoxHeader(QHeaderView):
    """Custom HeaderView that draws a checkbox in the first section."""
    toggled = pyqtSignal(bool)

    def __init__(self, parent=None, non_sortable_cols=None, filter_columns=None, filter_callback=None, star_columns=None, star_callback=None, desc_first_cols=None):
        super().__init__(Qt.Orientation.Horizontal, parent)
        self.isChecked = False
        self.setSectionsClickable(True)
        self.non_sortable_cols = non_sortable_cols if non_sortable_cols else []
        self.filter_columns = set(filter_columns or [])
        self.filter_callback = filter_callback
        self.filter_states = {}
        self.star_columns = set(star_columns or [])
        self.star_callback = star_callback
        self.star_states = {}
        self.desc_first_cols = set(desc_first_cols or [])

    def get_checkbox_rect(self, rect):
        box_size = 20
        padding = 4
        y = rect.top() + (rect.height() - box_size) // 2

        if self.layoutDirection() == Qt.LayoutDirection.RightToLeft:
            x = rect.right() - box_size - padding
        else:
            x = rect.left() + padding

        return QRect(x, y, box_size, box_size)

    def paintSection(self, painter, rect, logicalIndex):
        painter.save()
        super().paintSection(painter, rect, logicalIndex)
        painter.restore()

        if logicalIndex in self.filter_columns:
            # Filter is usually right-most (index 0 from edge)
            icon_rect = self._get_icon_rect(rect, 0)
            self._draw_filter_icon(painter, icon_rect, self.filter_states.get(logicalIndex, False))

        if logicalIndex in self.star_columns:
            # Star is next to filter (index 1 if filter exists, else 0)
            offset = 1 if logicalIndex in self.filter_columns else 0
            icon_rect = self._get_icon_rect(rect, offset)
            self._draw_star_icon(painter, icon_rect, self.star_states.get(logicalIndex, False))

        if logicalIndex == 0:
            option = QStyleOptionButton()
            option.rect = self.get_checkbox_rect(rect)
            option.state = QStyle.StateFlag.State_Enabled | QStyle.StateFlag.State_Active
            if self.isChecked:
                option.state |= QStyle.StateFlag.State_On
            else:
                option.state |= QStyle.StateFlag.State_Off

            self.style().drawControl(QStyle.ControlElement.CE_CheckBox, option, painter)

    def mousePressEvent(self, event):
        idx = self.logicalIndexAt(event.pos())

        # Handle Filter/Star clicks
        if (idx in self.filter_columns and self.filter_callback) or (idx in self.star_columns and self.star_callback):
            sec_pos = self.sectionViewportPosition(idx)
            sec_width = self.sectionSize(idx)
            sec_rect = QRect(sec_pos, 0, sec_width, self.height())

            if idx in self.filter_columns and self.filter_callback:
                if self._get_icon_rect(sec_rect, 0).contains(event.pos()):
                    self.filter_callback(idx)
                    return

            if idx in self.star_columns and self.star_callback:
                offset = 1 if idx in self.filter_columns else 0
                if self._get_icon_rect(sec_rect, offset).contains(event.pos()):
                    self.star_callback(idx)
                    return

        if idx == 0:
            sec_pos = self.sectionViewportPosition(0)
            sec_width = self.sectionSize(0)
            sec_rect = QRect(sec_pos, 0, sec_width, self.height())

            chk_rect = self.get_checkbox_rect(sec_rect)

            if chk_rect.contains(event.pos()):
                self.isChecked = not self.isChecked
                self.viewport().update()
                self.toggled.emit(self.isChecked)
                return # Consume event (Checkbox toggle)

            # If we clicked the header area but NOT the checkbox, check if sort should be blocked
            if 0 in self.non_sortable_cols:
                return # Prevent sort on col 0

        elif idx in self.non_sortable_cols:
            return # Prevent sort

        # For desc-first columns: if not currently sorted on this column,
        # pre-set indicator to ascending so the toggle goes to descending
        if idx in self.desc_first_cols and self.sortIndicatorSection() != idx:
            self.setSortIndicator(idx, Qt.SortOrder.AscendingOrder)

        super().mousePressEvent(event)

    def setChecked(self, checked):
        if self.isChecked != checked:
            self.isChecked = checked
            self.viewport().update()

    def _get_icon_rect(self, rect, offset_index=0):
        icon_size = 12
        padding = 6
        spacing = 4

        total_offset = padding + (offset_index * (icon_size + spacing))

        y = rect.top() + (rect.height() - icon_size) // 2

        if self.layoutDirection() == Qt.LayoutDirection.RightToLeft:
            x = rect.left() + total_offset
        else:
            x = rect.right() - icon_size - total_offset

        return QRect(x, y, icon_size, icon_size)

    def _draw_filter_icon(self, painter, rect, active):
        painter.save()
        color = self.palette().color(QPalette.ColorRole.Highlight if active else QPalette.ColorRole.Mid)
        pen = QPen(color)
        pen.setWidth(1)
        painter.setPen(pen)
        painter.setBrush(QBrush(color if active else Qt.BrushStyle.NoBrush))

        x = rect.x()
        y = rect.y()
        w = rect.width()
        h = rect.height()
        top_h = int(h * 0.55)
        stem_w = max(2, int(w * 0.3))
        mid_x = x + w // 2
        stem_left = mid_x - stem_w // 2
        stem_right = stem_left + stem_w

        path = QPainterPath()
        path.moveTo(x, y)
        path.lineTo(x + w, y)
        path.lineTo(stem_right, y + top_h)
        path.lineTo(stem_right, y + h)
        path.lineTo(stem_left, y + h)
        path.lineTo(stem_left, y + top_h)
        path.closeSubpath()
        painter.drawPath(path)
        painter.restore()

    def _draw_star_icon(self, painter, rect, active):
        painter.save()
        # Star color: Gold if active, Gray if inactive
        if active:
            color = QColor("#f1c40f") # Gold
            brush = QBrush(color)
        else:
            color = self.palette().color(QPalette.ColorRole.Mid)
            brush = Qt.BrushStyle.NoBrush

        pen = QPen(color)
        pen.setWidth(1)
        painter.setPen(pen)
        painter.setBrush(brush)

        # Draw Star using a simple path
        center = rect.center()
        radius = rect.width() / 2.0

        path = QPainterPath()
        import math
        points = []
        for i in range(5):
            # Outer point
            angle_deg = -90 + i * 72 # Start from top (rotated -90)
            angle_rad = math.radians(angle_deg)
            ox = center.x() + radius * math.cos(angle_rad)
            oy = center.y() + radius * math.sin(angle_rad)
            points.append((ox, oy))

            # Inner point
            angle_deg = -90 + i * 72 + 36
            angle_rad = math.radians(angle_deg)
            ix = center.x() + (radius * 0.4) * math.cos(angle_rad)
            iy = center.y() + (radius * 0.4) * math.sin(angle_rad)
            points.append((ix, iy))

        path.moveTo(points[0][0], points[0][1])
        for x, y in points[1:]:
            path.lineTo(x, y)
        path.closeSubpath()

        painter.drawPath(path)
        painter.restore()

    def set_filter_active(self, column, active):
        if active:
            self.filter_states[column] = True
        else:
            self.filter_states.pop(column, None)
        self.viewport().update()

    def set_star_active(self, column, active):
        if active:
            self.star_states[column] = True
        else:
            self.star_states.pop(column, None)
        self.viewport().update()

    def event(self, event):
        if event.type() == QEvent.Type.ToolTip:
            pos = event.pos()
            idx = self.logicalIndexAt(pos)

            if idx in self.star_columns or idx in self.filter_columns:
                sec_pos = self.sectionViewportPosition(idx)
                sec_width = self.sectionSize(idx)
                sec_rect = QRect(sec_pos, 0, sec_width, self.height())

                # Check Star
                if idx in self.star_columns:
                    offset = 1 if idx in self.filter_columns else 0
                    if self._get_icon_rect(sec_rect, offset).contains(pos):
                        QToolTip.showText(event.globalPos(), tr("Show entries in selected lists") if self.star_states.get(idx) else tr("Filter by List (Click to enable)"))
                        return True

                # Check Filter
                if idx in self.filter_columns:
                    if self._get_icon_rect(sec_rect, 0).contains(pos):
                        QToolTip.showText(event.globalPos(), tr("Filter configuration"))
                        return True

        return super().event(event)

class HiddenScrollArea(QScrollArea):
    def __init__(self, text_with_markers="", anchor_text=None, parent=None):
        super().__init__(parent)
        self._raw_text = text_with_markers
        self._anchor_text = anchor_text

        # Hide scrollbars
        self.setWidgetResizable(True)
        self.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        self.setVerticalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        self.setFrameShape(QFrame.Shape.NoFrame)

        # Add horizontal margins to create a gap between columns (5px on each side = 10px total gap)
        # We also ensure the background is transparent to show the row selection color
        self.setStyleSheet("QScrollArea { background: transparent; margin-left: 5px; margin-right: 5px; }")

        # Keep height strictly slim
        self.setFixedHeight(self.fontMetrics().lineSpacing() + 4)

        self.label = QLabel()
        self.label.setTextFormat(Qt.TextFormat.RichText)
        self.label.setAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
        self.label.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)
        # Ensure label background is transparent
        self.label.setStyleSheet("background: transparent;")
        self.setWidget(self.label)

        self._update_content()

    def _update_content(self):
        if not self._raw_text:
            self.label.setText(""); return

        # Apply coloring to markers
        processed = re.sub(r'\*(.*?)\*', r"<b style='color:#c0392b;'>\1</b>", self._raw_text)
        processed = re.sub(r'\*([^*]+)$', r"<b style='color:#c0392b;'>\1</b>", processed)
        processed = re.sub(r'^([^*]+)\*', r"<b style='color:#c0392b;'>\1</b>", processed)
        final_html = processed.replace("*", "")

        # Enforce non-breaking text
        self.label.setText(f"<div dir='rtl' style='white-space:nowrap; padding: 0 5px;'>{final_html}</div>")
        self.setToolTip(self._raw_text.replace("*", ""))

        # Position highlight in view initially
        QTimer.singleShot(10, self._center_on_match)

    def _center_on_match(self):
        target_pos = -1
        if self._anchor_text:
            target_pos = self._raw_text.find(f"*{self._anchor_text}*")
        if target_pos == -1:
            target_pos = self._raw_text.find('*')

        if target_pos != -1:
            bar = self.horizontalScrollBar()
            max_val = bar.maximum()
            if max_val > 0:
                # Calculate center ratio for RTL scrollbar
                ratio = (len(self._raw_text) - target_pos) / len(self._raw_text)
                bar.setValue(int(max_val * ratio))

    def wheelEvent(self, event):
        # Convert vertical wheel movement to horizontal scroll
        if event.angleDelta().y() != 0:
            bar = self.horizontalScrollBar()
            # Sensitivity adjustment
            bar.setValue(bar.value() - event.angleDelta().y())
            event.accept()
        else:
            super().wheelEvent(event)

    def resizeEvent(self, event):
        super().resizeEvent(event)
        # Maintain highlight focus when column width changes
        QTimer.singleShot(10, self._center_on_match)


class ListsTreeWidget(QTreeWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.main_window = parent  # שומרים את ההפניה ל-GenizahGUI

        self.setDragEnabled(True)
        self.setAcceptDrops(True)
        self.setDropIndicatorShown(True)
        self.setDefaultDropAction(Qt.DropAction.MoveAction)
        self.setDragDropMode(QAbstractItemView.DragDropMode.InternalMove)

    def dropEvent(self, event):
        super().dropEvent(event)
        # קריאה לפונקציה בחלון הראשי לעדכון הצבעים והסדר
        if self.main_window and hasattr(self.main_window, 'lists_handle_tree_reorder'):
            self.main_window.lists_handle_tree_reorder()
