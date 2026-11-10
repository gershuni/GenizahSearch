# -*- coding: utf-8 -*-
"""Reusable PyQt6 widget subclasses (extracted from genizah_app.py, Phase 126 D1).

Provides four top-level widget subclasses moved verbatim out of the
28K-line ``genizah_app.py`` god file:

  - ShelfmarkTableWidgetItem(QTableWidgetItem) — natural-sort shelfmark cells
  - CheckBoxHeader(QHeaderView) — header with a master checkbox + filter/star glyphs
  - MatchCentredSnippet(QWidget) — a one-line RTL snippet cell, the *...* marks centred
  - ContextSnippet(MatchCentredSnippet) — the Composition tab's Context / MS Context cells
    (replaced HiddenScrollArea, a QScrollArea, on 2026-10-23)
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
from html import escape as html_escape

from PyQt6.QtGui import QFontMetrics
from PyQt6.QtWidgets import (
    QTableWidgetItem,
    QHeaderView,
    QTreeWidget,
    QLabel,
    QStyle,
    QStyleOptionButton,
    QToolTip,
    QAbstractItemView,
    QWidget,
)
from PyQt6.QtCore import Qt, QRect, QEvent, pyqtSignal
from PyQt6.QtGui import QColor, QPalette, QPen, QBrush, QPainterPath, QTextDocument

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

    def _on_resize_grip(self, pos):
        """True when ``pos`` is on a section boundary, where a drag resizes.

        The checks below return early for the checkbox and for non-sortable
        columns, and they used to swallow a press on those columns' own resize
        grip too, so the Actions and Img columns could not be resized from
        their edge in either direction (measured 2026-09-24).
        """
        idx = self.logicalIndexAt(pos)
        if idx < 0:
            return False
        grip = max(self.style().pixelMetric(QStyle.PixelMetric.PM_HeaderGripMargin), 2)
        start = self.sectionViewportPosition(idx)
        end = start + self.sectionSize(idx)
        x = pos.x()
        return abs(x - start) <= grip or abs(x - end) <= grip

    def mousePressEvent(self, event):
        idx = self.logicalIndexAt(event.pos())

        if self.sectionResizeMode(idx) != QHeaderView.ResizeMode.Fixed and self._on_resize_grip(event.pos()):
            super().mousePressEvent(event)
            return

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

class MatchCentredSnippet(QWidget):
    """A one-line snippet cell with the found words in the middle of the cell:
    the Search tab's Snippet column, and (ContextSnippet) the Composition
    tab's Context and MS Context columns.

    The Search cell was a QLabel holding the snippet as one centred RTL
    paragraph. A paragraph is centred only while it fits: a line wider than
    the label is laid out from the label's left edge, which for Hebrew is the
    END of the snippet, so the cell showed the last 600 px of the text and the
    match -- cut 60 characters from either side, so in the middle of the
    string -- only when the whole string was under twice the cell's width. In
    the app's font it seldom was: the match sat at the cell's right edge, cut,
    or past it (owner, 2026-10-23).

    The label is laid out at its natural width inside this clipping widget and
    slid so that the marked words (``*...*`` in the snippet, *mark_colour* in
    *html*) are centred in the cell's contents rect; several marks are centred
    as one span, from the first to the last, unless *anchor_text* names the
    one to centre on and it is marked. Never past either end of the text, as a
    scroll area would not: a snippet that fits is shown whole, centred as
    before, and a match near the start of a page is shown with the text's
    start at the cell's edge. The label is still a QLabel, so the text can be
    selected with the mouse as before. The contents margins are a gap the
    text is clipped to, between columns.
    """

    #: The colour ``SearchEngine.format_snippet(style='html_inline')`` gives a
    #: ``*...*`` mark; the marks are found in the laid-out text by it.
    MATCH_COLOUR = QColor('#ff0000')

    def __init__(self, html, parent=None, *, mark_colour=None, anchor_text=None):
        super().__init__(parent)
        self._html = html or ""
        self._mark_colour = QColor(mark_colour) if mark_colour is not None else self.MATCH_COLOUR
        self._anchor_text = anchor_text or None
        self._clip = QWidget(self)      # the contents rect; clips the label to it
        self._label = QLabel(self._html, self._clip)
        self._label.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)
        # (text width, x of the marks' centre) in the cell's font. Measured at
        # the first paint, not on insertion: a batch is 500 rows, and laying
        # out every row's text costs about as much as the old label's layout
        # again, which that label only paid for the rows that came on screen.
        self._metrics = None
        self._slid = 0      # px the reader slid the text by (ContextSnippet's wheel)

    def text(self):
        """The label's text, for the filter rule's fallback."""
        return self._label.text()

    def label(self):
        return self._label

    def slide(self, dx):
        """Move the text *dx* px to the right (a negative *dx*: to the left),
        no further than its ends; a text that fits does not move."""
        self._slid += dx
        if self._metrics is not None:
            self._place()

    def changeEvent(self, event):
        super().changeEvent(event)
        if event.type() == QEvent.Type.FontChange:
            # The cell takes the table's font on insertion; measure in it. The
            # label is given it outright: under a view with a style sheet (the
            # Composition tree's) Qt does not pass a font on to a cell's children.
            if self._label.font() != self.font():
                self._label.setFont(self.font())
            self._metrics = None
            self.update()

    def resizeEvent(self, event):
        super().resizeEvent(event)
        if self._metrics is not None:
            self._place()

    def paintEvent(self, event):
        if self._metrics is None:
            self._place()      # the label, a child, is painted after this, where put
        super().paintEvent(event)

    def _measure(self):
        """(width of the whole text, x of the centre of the marked span), in
        the label's own coordinates: the label is given exactly its natural
        width, so its one line starts at its left edge, as it does in a
        document laid out at that width."""
        width = self._label.sizeHint().width()
        # No mark to centre on: the start of the text, which for a right-to-left
        # paragraph is its right end (a scroll area's home position).
        centre = float(width)
        doc = QTextDocument()
        doc.setDefaultFont(self._label.font())
        doc.setDocumentMargin(0)
        doc.setHtml(self._html)
        doc.setTextWidth(width)
        doc.documentLayout().documentSize()     # the layout is lazy; lines exist after this
        block = doc.firstBlock()
        marks = []      # (start, end, text) of each run in the mark colour, in order
        it = block.begin()
        while not it.atEnd():
            frag = it.fragment()
            if frag.isValid() and frag.charFormat().foreground().color() == self._mark_colour:
                pos = frag.position() - block.position()
                marks.append((pos, pos + frag.length(), frag.text()))
            it += 1
        layout = block.layout()
        if marks and layout is not None and layout.lineCount() > 0:
            anchored = [m for m in marks if m[2] == self._anchor_text] if self._anchor_text else []
            start, end = (anchored[0][0], anchored[0][1]) if anchored else (marks[0][0], marks[-1][1])
            line = layout.lineAt(0)
            x0 = layout.position().x()
            centre = (line.cursorToX(start)[0] + line.cursorToX(end)[0]) / 2 + x0
        return width, centre

    def _place(self):
        if self._metrics is None:
            self._metrics = self._measure()
        text_w, centre = self._metrics
        area = self.contentsRect()
        self._clip.setGeometry(area)
        w = area.width()
        if text_w <= w:
            x = (w - text_w) // 2
            self._slid = 0
        else:
            centred = round(w / 2 - centre)
            x = max(w - text_w, min(0, centred + self._slid))
            # Keep the slide at what is shown: wheeling on past an end must
            # not bank turns the other way has to pay off first (Codex, PR #445).
            self._slid = x - centred
        self._label.setGeometry(x, 0, text_w, area.height())


class ContextSnippet(MatchCentredSnippet):
    """The Composition tab's Context and MS Context cells, built from the
    marked text: a mark is bold dark red (the tab's colour), an unbalanced
    marker at either end marks to that end, and HTML in the text is text. The
    whole text is the tooltip, a 5 px margin keeps the two columns' texts
    apart, the mouse wheel slides the text sideways, and the MS cell centres
    on the chunk the Context cell shows (*anchor_text*) when it is marked
    there.

    Until 2026-10-23 this was HiddenScrollArea, a QScrollArea whose scrollbar
    was set from the mark's share of the text's CHARACTERS, so the match stood
    near the middle only as the letters' widths allowed, and after a 10 ms
    timer per cell and per column resize. The mark is now centred by
    measurement, with no timer, at the cell's first paint.
    """

    MARK_COLOUR = QColor('#c0392b')
    MARGIN = 5

    def __init__(self, text_with_markers="", anchor_text=None, parent=None):
        text = text_with_markers or ""
        super().__init__(self.marked_html(text), parent,
                         mark_colour=self.MARK_COLOUR, anchor_text=anchor_text)
        self.setContentsMargins(self.MARGIN, 0, self.MARGIN, 0)
        self.setToolTip(text.replace("*", ""))
        # Keep the tree's rows slim: a row is as tall as its cells ask.
        self.setFixedHeight(self.fontMetrics().lineSpacing() + 4)

    def changeEvent(self, event):
        super().changeEvent(event)
        if event.type() == QEvent.Type.FontChange:
            # The tree's font (its own, or Settings > Text size): one line of it.
            self.setFixedHeight(self.fontMetrics().lineSpacing() + 4)

    @classmethod
    def marked_html(cls, text):
        """The cell's HTML: ``*...*`` as bold MARK_COLOUR runs. The old cell's
        rules for a marker left without a pair are kept: one followed by text
        marks from it to the end of the text (a mark cut off by the end of
        the context), one that ends the text marks from the start to it; any
        other stray marker is dropped."""
        mark = f"<b style='color:{cls.MARK_COLOUR.name()};'>\\1</b>"
        processed = html_escape(text or "")
        processed = re.sub(r'\*(.*?)\*', mark, processed)
        processed = re.sub(r'\*([^*]+)$', mark, processed)
        processed = re.sub(r'^([^*]+)\*', mark, processed)
        return f"<div dir='rtl'>{processed.replace('*', '')}</div>"

    def wheelEvent(self, event):
        # Vertical wheel movement slides the text sideways (as the scroll
        # area did); the tree does not scroll under it.
        dy = event.angleDelta().y()
        if dy:
            self.slide(dy)
            event.accept()
        else:
            super().wheelEvent(event)


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
