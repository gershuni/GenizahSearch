# -*- coding: utf-8 -*-
"""Line-number gutter for QTextEdit/QTextBrowser (desktop transcription panes).

Phase 999.4 Plan 02. Mirrors the web pillar's `_render_line_numbered_html`
helper from `web/pages/browse.py` (Plan 01) by exposing the same shared
contract: one number per logical `\\n` line including blanks (D-10), 1-based,
numbering restarts at 1 per `apply_line_numbered_text(...)` call (D-11).

Per Phase 999.4 D-04: the gutter is a SIBLING QWidget — not part of the
body widget's QTextDocument — so a user's mouse-drag-selection inside the
body cannot capture line-number digits. `widget.toPlainText()` after
apply_line_numbered_text() returns body-only text with zero gutter digits.

Persistence (D-09): toggle state stored in app config under key
`show_line_numbers` via genizah_core.load_app_config / save_app_config.
Default True (D-07). Shared with the Browse-tab toolbar toggle and the
ResultDialog find-row toggle — toggling in one surface takes effect on the
NEXT `apply_line_numbered_text(...)` call on the other surface.

Reference Qt pattern: the canonical "Code Editor Example" line-number-area
widget (https://doc.qt.io/qt-6/qtwidgets-widgets-codeeditor-example.html).
"""
from __future__ import annotations

from typing import Optional

from PyQt6.QtCore import QRect, QSize, Qt
from PyQt6.QtGui import QColor, QFont, QPainter
from PyQt6.QtWidgets import QAbstractScrollArea, QWidget

from genizah_core import load_app_config, save_app_config


_CONFIG_KEY = "show_line_numbers"
_GUTTER_PADDING_PX = 8
_GUTTER_FONT_SIZE = 10
_GUTTER_COLOR_HEX = "#9ca3af"
_GUTTER_BG_HEX = "#f3f4f6"


def is_line_numbers_enabled() -> bool:
    """Return current toggle state from app config; default True per D-07."""
    cfg = load_app_config() or {}
    val = cfg.get(_CONFIG_KEY, True)
    return bool(val)


def set_line_numbers_enabled(enabled: bool) -> None:
    """Persist toggle state to app config (D-09)."""
    save_app_config({_CONFIG_KEY: bool(enabled)})


class LineNumberArea(QWidget):
    """Sibling QWidget painted with one line number per source-text line.

    Selection-isolated from the body by design — Qt's text cursor cannot
    extend out of its QTextDocument into a sibling widget.
    """

    def __init__(self, body_widget):
        super().__init__(body_widget)
        self._body = body_widget
        self._line_count = 0
        self._font = QFont()
        self._font.setPointSize(_GUTTER_FONT_SIZE)
        self.setFont(self._font)
        # Repaint on body scroll
        if isinstance(body_widget, QAbstractScrollArea):
            body_widget.verticalScrollBar().valueChanged.connect(self.update)
        # Mouse events fall through to the body widget — the gutter is
        # display-only (D-12: no click handlers in this phase).
        self.setAttribute(Qt.WidgetAttribute.WA_TransparentForMouseEvents, True)

    def set_line_count(self, n: int) -> None:
        self._line_count = max(0, int(n))
        self.update()

    def gutter_width(self) -> int:
        """Width needed to fit the largest line number + padding."""
        digits = max(2, len(str(max(1, self._line_count))))
        metrics = self.fontMetrics()
        return metrics.horizontalAdvance("9") * digits + _GUTTER_PADDING_PX * 2

    def sizeHint(self) -> QSize:
        return QSize(self.gutter_width(), 0)

    def paintEvent(self, event):
        painter = QPainter(self)
        painter.fillRect(event.rect(), QColor(_GUTTER_BG_HEX))
        painter.setPen(QColor(_GUTTER_COLOR_HEX))
        painter.setFont(self._font)

        if self._line_count == 0:
            return

        body = self._body
        gutter_w = self.gutter_width()
        scroll_y = (
            body.verticalScrollBar().value()
            if isinstance(body, QAbstractScrollArea)
            else 0
        )
        viewport_height = (
            body.viewport().height() if hasattr(body, "viewport") else self.height()
        )

        # Walk QTextBlocks for accurate line-y alignment. Each `<br>` in the
        # rendered HTML (or each `\n` in plain text) maps 1:1 to a QTextBlock
        # for normal transcription content. Use blockBoundingRect for y.
        doc = body.document()
        block = doc.firstBlock()
        line_no = 1
        while block.isValid() and line_no <= self._line_count:
            block_rect = doc.documentLayout().blockBoundingRect(block)
            top = int(block_rect.top()) - scroll_y
            block_height = max(1, int(block_rect.height()))
            # Cull blocks outside the visible viewport
            if top + block_height > 0 and top < viewport_height:
                painter.drawText(
                    QRect(
                        0,
                        top,
                        gutter_w - _GUTTER_PADDING_PX,
                        block_height,
                    ),
                    int(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignTop),
                    str(line_no),
                )
            block = block.next()
            line_no += 1


def _ensure_gutter(widget) -> LineNumberArea:
    """Attach a LineNumberArea to widget on first call (idempotent)."""
    existing = getattr(widget, "_line_number_area", None)
    if existing is not None:
        return existing
    area = LineNumberArea(widget)
    widget._line_number_area = area
    _install_resize_hook(widget)
    return area


def _reposition_gutter(widget) -> None:
    """Place the gutter on the leading edge (visually RIGHT in RTL).

    Qt's `QAbstractScrollArea.setViewportMargins(left, top, right, bottom)`
    uses PHYSICAL coordinates and is NOT flipped by layoutDirection, so we
    pick the side ourselves based on the widget's current layout direction.
    """
    area = getattr(widget, "_line_number_area", None)
    if area is None:
        return
    gutter_w = area.gutter_width()
    visible = area.isVisible()
    margin = gutter_w if visible else 0
    is_rtl = widget.layoutDirection() == Qt.LayoutDirection.RightToLeft

    if hasattr(widget, "setViewportMargins"):
        if is_rtl:
            widget.setViewportMargins(0, 0, margin, 0)
        else:
            widget.setViewportMargins(margin, 0, 0, 0)

    cr = widget.contentsRect()
    if is_rtl:
        # Gutter sits at the right edge in RTL (the leading side)
        x = cr.right() - gutter_w + 1
    else:
        x = cr.left()
    area.setGeometry(QRect(x, cr.top(), gutter_w, cr.height()))


def _install_resize_hook(widget) -> None:
    """Reposition gutter on widget resize (idempotent)."""
    if getattr(widget, "_line_number_resize_hooked", False):
        return
    original_resize = widget.resizeEvent

    def resize_event(event, _orig=original_resize):
        _orig(event)
        if getattr(widget, "_line_number_area", None) is not None:
            _reposition_gutter(widget)

    widget.resizeEvent = resize_event
    widget._line_number_resize_hooked = True


def apply_line_numbered_text(
    widget,
    rendered_html_or_text: str,
    *,
    source_text: Optional[str] = None,
    is_html: bool = True,
) -> None:
    """Set widget text + render the line-number gutter.

    Args:
        widget: QTextEdit or QTextBrowser body widget.
        rendered_html_or_text: body content to display. HTML if ``is_html``
            is True, else plain text.
        source_text: raw pre-HTML text for line-counting (D-10). If None,
            ``rendered_html_or_text`` is used (correct for plain-text mode).
        is_html: if True call ``widget.setHtml(...)``, else ``setPlainText(...)``.

    Reads toggle state from app config; default True per D-07.
    """
    # Step 1: write the body
    if is_html:
        widget.setHtml(rendered_html_or_text)
    else:
        widget.setPlainText(rendered_html_or_text)

    # Step 2: compute line count from source (D-10: split('\n'), not splitlines())
    counting_source = source_text if source_text is not None else rendered_html_or_text
    if counting_source:
        line_count = len(counting_source.split("\n"))
    else:
        line_count = 0

    # Step 3: ensure the gutter exists and update count (D-11: per-call reset)
    area = _ensure_gutter(widget)
    area.set_line_count(line_count)

    # Step 4: apply visibility from config (D-07 default True)
    enabled = is_line_numbers_enabled()
    area.setVisible(enabled)
    _reposition_gutter(widget)


def refresh_visibility(widget) -> None:
    """Re-read config and update widget's gutter visibility.

    Call from a toolbar toggle's click handler after `set_line_numbers_enabled`.
    """
    area = getattr(widget, "_line_number_area", None)
    if area is None:
        return
    enabled = is_line_numbers_enabled()
    area.setVisible(enabled)
    _reposition_gutter(widget)
