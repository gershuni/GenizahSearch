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
from PyQt6.QtGui import QColor, QFont, QPainter, QTextCursor
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
        """Legacy single-page mode: continuous numbering 1..n across the whole document."""
        self._line_count = max(0, int(n))
        self._per_page_mode = False
        self.update()

    def set_per_page_mode(self, enabled: bool) -> None:
        """Per-page mode reads block userState (see `_mark_blocks_for_pages`)."""
        self._per_page_mode = bool(enabled)
        # In per-page mode `_line_count` is the total across pages — used only
        # to size the gutter width to fit the largest expected number.
        self.update()

    def gutter_width(self) -> int:
        """Width needed to fit the largest line number + padding."""
        digits = max(2, len(str(max(1, self._line_count))))
        metrics = self.fontMetrics()
        return metrics.horizontalAdvance("9") * digits + _GUTTER_PADDING_PX * 2

    def sizeHint(self) -> QSize:
        return QSize(self.gutter_width(), 0)

    def _compute_line_positions(self) -> list[tuple[int, int, int]]:
        """Return [(y_in_viewport, height, number), ...] for each numbered line.

        Uses `body.cursorRect(cursor)` to get the y-position of every visual
        line — this respects CSS line-height and font-size from the rendered
        HTML, which `QTextLayout.lineAt(i).y()` does not always do (the older
        approach drifted relative to the body text when CSS line-height was
        > 1). cursorRect returns viewport-relative coordinates, which align
        with the gutter widget's own coordinate system.

        Two modes:
        - Legacy (per_page_mode=False): numbering is continuous 1..N across
          the entire document; N == self._line_count.
        - Per-page (per_page_mode=True): walks QTextBlocks; blocks with
          `block.userState() >= 0` are page-content blocks (resets numbering
          to 1 when the userState value changes); blocks with userState == -1
          are separators and get no numbers.
        """
        body = self._body
        doc = body.document()
        positions: list[tuple[int, int, int]] = []

        # We walk SOURCE-LINE segments (block.text() split by U+2028, Qt's
        # representation of `<br>`), NOT visual lines. This is the key
        # invariant for the post-2026-05-18 smoke check: when the body
        # widget word-wraps a long source line to multiple visual lines,
        # the wrapped continuations must NOT get their own number — they
        # share the number of the first visual line of that segment.
        # cursorRect at the segment's start character gives the y of that
        # first visual line; subsequent visual lines (wraps) have no
        # cursor query, so they don't get numbered.
        if getattr(self, "_per_page_mode", False):
            current_page = -1
            line_in_page = 0
            block = doc.firstBlock()
            while block.isValid():
                state = block.userState()
                if state < 0:
                    block = block.next()
                    continue
                if state != current_page:
                    current_page = state
                    line_in_page = 0
                segments = block.text().split(" ")
                offset_in_block = 0
                for segment in segments:
                    cursor = QTextCursor(doc)
                    cursor.setPosition(block.position() + offset_in_block)
                    rect = body.cursorRect(cursor)
                    y = rect.y()
                    h = max(1, rect.height())
                    line_in_page += 1
                    positions.append((y, h, line_in_page))
                    offset_in_block += len(segment) + 1
                block = block.next()
            return positions

        # Legacy mode: continuous numbering, capped at self._line_count.
        block = doc.firstBlock()
        line_no = 0
        while block.isValid() and line_no < self._line_count:
            segments = block.text().split(" ")
            offset_in_block = 0
            for segment in segments:
                if line_no >= self._line_count:
                    break
                cursor = QTextCursor(doc)
                cursor.setPosition(block.position() + offset_in_block)
                rect = body.cursorRect(cursor)
                y = rect.y()
                h = max(1, rect.height())
                line_no += 1
                positions.append((y, h, line_no))
                offset_in_block += len(segment) + 1
            block = block.next()
        return positions

    def paintEvent(self, event):
        painter = QPainter(self)
        painter.fillRect(event.rect(), QColor(_GUTTER_BG_HEX))
        painter.setPen(QColor(_GUTTER_COLOR_HEX))
        painter.setFont(self._font)

        if self._line_count == 0 and not getattr(self, "_per_page_mode", False):
            return

        body = self._body
        gutter_w = self.gutter_width()
        viewport_height = (
            body.viewport().height() if hasattr(body, "viewport") else self.height()
        )

        for y, h, number in self._compute_line_positions():
            if y + h > 0 and y < viewport_height:
                # AlignVCenter — cursorRect's height includes the CSS line-
                # spacing leading, so centering vertically anchors the (small)
                # gutter glyph to roughly the same baseline-area as the body
                # text. AlignTop made numbers appear hugging the top of the
                # line space, visually offset from the body text glyphs.
                painter.drawText(
                    QRect(0, y, gutter_w - _GUTTER_PADDING_PX, h),
                    int(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter),
                    str(number),
                )


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


_BIDI_CONTROLS = (
    "‎‏"
    "‪‫‬‭‮"
    "⁦⁧⁨⁩"
)


_BIDI_CONTROLS = (
    "‎‏"
    "‪‫‬‭‮"
    "⁦⁧⁨⁩"
)


def _normalize_block_text(s: str) -> str:
    """Normalize block text for matching against page sources.

    Qt represents `<br>` soft-line breaks inside a single QTextBlock as
    U+2028 LINE SEPARATOR in `block.text()`. Source pages use `\n`. Map
    both to `\n`, strip Unicode directional control marks (Qt's HTML
    renderer inserts these around RTL `<div dir='rtl'>` content), and
    strip leading/trailing whitespace on each line so an extra space
    Qt added at a line edge does not break equality.
    """
    s = s.replace(" ", "\n")
    for ctl in _BIDI_CONTROLS:
        s = s.replace(ctl, "")
    lines = [line.strip() for line in s.split("\n")]
    return "\n".join(lines).strip()


def _mark_blocks_for_pages(doc, pages):
    """Tag QTextBlocks: setUserState(page_idx) for matched content blocks,
    setUserState(-1) for separators/titles.

    Matching strategy per block:
      1. Try EXACT equality against the next-expected unmatched page first
         (preserves source ordering for typical View All / Reading Desk
         content where pages render in order).
      2. If no exact match, scan all unmatched pages for exact equality
         (defensive against out-of-order block emission).
      3. If still none, try substring match (block contains page OR page
         contains block) -- handles Qt edge cases where rendered block
         text gains or loses small whitespace runs relative to the raw
         page source. Each transcription page is long enough that
         cross-page substring collisions are extremely unlikely.
      4. If still no match, the block is a separator (userState = -1).

    Returns the count of pages successfully matched.
    """
    norm_pages = [_normalize_block_text(p) for p in pages]
    unmatched = list(range(len(norm_pages)))
    block = doc.firstBlock()
    while block.isValid():
        matched_idx = None
        block_text = _normalize_block_text(block.text())
        if block_text and unmatched:
            first = unmatched[0]
            if block_text == norm_pages[first]:
                matched_idx = first
            if matched_idx is None:
                for i in unmatched:
                    if block_text == norm_pages[i]:
                        matched_idx = i
                        break
            if matched_idx is None:
                for i in unmatched:
                    p = norm_pages[i]
                    if p and (p in block_text or block_text in p):
                        matched_idx = i
                        break
        if matched_idx is not None:
            block.setUserState(matched_idx)
            unmatched.remove(matched_idx)
        else:
            block.setUserState(-1)
        block = block.next()
    return len(norm_pages) - len(unmatched)


def apply_line_numbered_text(
    widget,
    rendered_html_or_text: str,
    *,
    source_text: Optional[str] = None,
    pages: Optional[list] = None,
    is_html: bool = True,
) -> None:
    """Set widget text + render the line-number gutter.

    Args:
        widget: QTextEdit or QTextBrowser body widget.
        rendered_html_or_text: body content to display. HTML if ``is_html``
            is True, else plain text.
        source_text: raw pre-HTML text for line-counting (D-10). If None,
            ``rendered_html_or_text`` is used (correct for plain-text mode).
            Ignored when ``pages`` is provided.
        pages: optional list of per-page raw text. When provided, the
            painter enters per-page mode: numbering restarts at 1 inside
            each matched content block. Separator/title blocks (anything
            that doesn't match the next expected page source) get no
            numbers. Used by Full Manuscript View for web-parity per-page
            restart.
        is_html: if True call ``widget.setHtml(...)``, else ``setPlainText(...)``.

    Reads toggle state from app config; default True per D-07.
    """
    # Step 1: write the body
    if is_html:
        widget.setHtml(rendered_html_or_text)
    else:
        widget.setPlainText(rendered_html_or_text)

    # Step 2: configure mode + line counting
    area = _ensure_gutter(widget)
    if pages is not None:
        # Per-page mode. Mark each QTextBlock with userState; the painter
        # walks the marks to number per page with reset.
        matched = _mark_blocks_for_pages(widget.document(), pages)
        # Total = sum of per-page source line counts. Used only for sizing
        # the gutter width (so 3-digit numbers in long pages still fit).
        max_per_page = max(
            (len(p.split("\n")) for p in pages), default=0
        )
        area._line_count = max(0, int(max_per_page))
        area.set_per_page_mode(True)
    else:
        # Legacy single-page mode.
        counting_source = source_text if source_text is not None else rendered_html_or_text
        if counting_source:
            line_count = len(counting_source.split("\n"))
        else:
            line_count = 0
        area.set_line_count(line_count)

    # Step 3: apply visibility from config (D-07 default True)
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
