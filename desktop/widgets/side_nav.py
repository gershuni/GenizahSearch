# -*- coding: utf-8 -*-
"""The main window's side menu: the sections and tools down one side of the window.

Owner, 2026-10-24: "move the menu to one of the app sides, remove the V tick
boxes, and add also the puzzle, joins lab and settings to the menu", then "Keep
the tabs. Offer an option to change them to the side menu". So it is one of two
layouts, chosen in Settings: the tab bar with its corner buttons (the default),
or this column in their place. The tab layout's ☰ menu is built from this
column's entries too (``build_menu``), so the two always list the same things.

* It sits on the window's start side -- right in Hebrew, left in English; Qt
  mirrors the layout. Sections first, the current one highlighted (never
  ticked); then the tools that open their own windows; at the foot Settings, the
  account, the language switch, the website and the version.
* Expanded, an entry shows its icon and label; compact, only the icon, with the
  label as tooltip. The ☰ at the top switches between the two, and ``toggled``
  reports the choice so the caller can remember it.
* Labels need room: at 300% a 1920x1080 screen is 640 logical px wide. The
  column is compact whenever its expanded width would take more than
  ``EXPANDED_WIDTH_SHARE`` of the window (``fit_to``), and ☰ then opens the same
  entries as a menu, labels included and still without tick boxes.
* A short window scrolls the column instead of being made taller by it (a
  window that does not fit its screen was #362).
* ``NavButton.setText`` sets the LABEL and keeps the icon, so code written for a
  plain QPushButton works unchanged; ``text()`` returns the label.
* The login button and the language switch exist in both layouts. The tab-row
  one is a ``LinkedButton``: whatever relabels or disables it does the same to
  its side-menu twin.
"""
from __future__ import annotations

from PyQt6.QtCore import QEvent, QObject, QPoint, QRectF, QSize, Qt, pyqtSignal
from PyQt6.QtGui import QColor, QFont, QFontMetrics, QIcon, QPainter, QPalette, QPixmap
from PyQt6.QtWidgets import (
    QApplication, QButtonGroup, QFrame, QMenu, QPushButton, QScrollArea, QSizePolicy,
    QVBoxLayout, QWidget,
)

from desktop.widgets.overflow_row import explicitly_hidden
from genizah_core import tr

ICON_SIZE = 18
# Drawn at each scale so the icon stays sharp at 100-300% Windows zoom.
_ICON_SCALES = (1, 1.25, 1.5, 2, 3)
# An entry's padding, in px.
_PAD_X = 10
_PAD_Y = 7
# The accent bar on the current section's start edge.
_BAR = 3
# Measured to learn what an entry adds to its label (icon, padding, bar): wide,
# so no minimum button width hides in the difference.
_PROBE_TEXT = "MMMMMMMMMM"


def glyph_icon(glyph: str, color: QColor, size: int = ICON_SIZE) -> QIcon:
    """An icon drawn from a character: an emoji (in its own colours) or a symbol
    such as ☰ (in ``color``)."""
    icon = QIcon()
    for scale in _ICON_SCALES:
        px = round(size * scale)
        pm = QPixmap(px, px)
        pm.fill(Qt.GlobalColor.transparent)
        painter = QPainter(pm)
        painter.setRenderHint(QPainter.RenderHint.Antialiasing)
        painter.setRenderHint(QPainter.RenderHint.TextAntialiasing)
        font = QFont()
        font.setPixelSize(max(1, round(px * 0.8)))
        painter.setFont(font)
        painter.setPen(color)
        painter.drawText(QRectF(0, 0, px, px), Qt.AlignmentFlag.AlignCenter, glyph)
        painter.end()
        icon.addPixmap(pm)
    return icon


# Windows virtual-key codes of the keys LayoutProofShortcuts takes besides
# letters and digits (whose codes are their upper-case ASCII).
_WINDOWS_VK = {',': 0xBC, '-': 0xBD, '.': 0xBE, '=': 0xBB}


def windows_virtual_key(char: str):
    c = char.upper()
    if len(c) == 1 and ('A' <= c <= 'Z' or '0' <= c <= '9'):
        return ord(c)
    return _WINDOWS_VK.get(c)


class LayoutProofShortcuts(QObject):
    """Ctrl+letter shortcuts that also answer to the physical key.

    On Windows, a QShortcut for Ctrl+G can miss when the keyboard layout's G
    key types another script, the Hebrew \u05e2 for one (owner, 2026-10-25:
    "Ctrl-G does not work" while the menu entry did). Installed on the
    application, this sees a key press that no shortcut took and matches it
    by its virtual-key code, which is the same in every layout. A press a
    QShortcut did take never arrives as a key press, so nothing fires twice;
    only the window's own presses count, not a dialog's.
    """

    _MODIFIERS = (Qt.KeyboardModifier.ControlModifier | Qt.KeyboardModifier.ShiftModifier
                  | Qt.KeyboardModifier.AltModifier | Qt.KeyboardModifier.MetaModifier)

    def __init__(self, window, bindings):
        """``bindings``: {(key character, modifiers): slot}, e.g.
        {('G', Qt.KeyboardModifier.ControlModifier): window.open_go_to}."""
        super().__init__(window)
        self.window = window
        self.bindings = {(windows_virtual_key(k), m): slot for (k, m), slot in bindings.items()}

    def eventFilter(self, obj, event):  # noqa: N802 - Qt API
        if event.type() != QEvent.Type.KeyPress:
            return False
        if QApplication.activeModalWidget() is not None or QApplication.activeWindow() is not self.window:
            return False
        slot = self.bindings.get((event.nativeVirtualKey(), event.modifiers() & self._MODIFIERS))
        if slot is None:
            return False
        slot()
        return True


def ltr(text: str) -> str:
    """``text`` between left-to-right marks, for a key such as "Ctrl+," shown in
    the Hebrew UI: unmarked, its trailing comma moves to the front (",Ctrl+")."""
    return f"\u200e{text}\u200e" if text else text


def popup_point(anchor: QWidget, menu: QMenu, *, beside: bool = False) -> QPoint:
    """Where to open ``menu`` for ``anchor``: below it (a button in a top row),
    or ``beside`` it toward the window's content, growing up from its foot (an
    entry of the side menu). QMenu does not mirror an explicit position in
    right-to-left, so the menu's width is taken off here: unmirrored, a menu
    under the side menu's ☰ opened past the window's right edge."""
    size = menu.sizeHint()
    rtl = anchor.isRightToLeft()
    if beside:
        pos = anchor.mapToGlobal(QPoint(0, anchor.height() - size.height()))
        pos.setX(pos.x() - size.width() if rtl else pos.x() + anchor.width())
    else:
        pos = anchor.mapToGlobal(QPoint(0, anchor.height() + 1))
        if rtl:
            pos.setX(pos.x() + anchor.width() - size.width())
    return pos


class LinkedButton(QPushButton):
    """A QPushButton whose label and enabled state are copied to its twins: the
    same control in the other menu layout. The login button is relabelled and
    disabled from several places (signing out, the account name arriving); with
    this they never have to know which layout is showing."""

    def __init__(self, text: str = "", parent=None):
        super().__init__(text, parent)
        self._twins = []

    def link(self, twin: QPushButton):
        self._twins.append(twin)
        twin.setText(self.text())
        twin.setEnabled(self.isEnabled())

    def setText(self, text):  # noqa: N802 - Qt API
        super().setText(text)
        for twin in self._twins:
            twin.setText(text)

    def setEnabled(self, enabled):  # noqa: N802 - Qt API
        super().setEnabled(enabled)
        for twin in self._twins:
            twin.setEnabled(enabled)


def _text_color() -> QColor:
    return QApplication.palette().color(QPalette.ColorRole.WindowText)


class NavButton(QPushButton):
    """One entry: an icon and a label.

    ``setText`` / ``text`` / ``setToolTip`` act on the label and the hint, as on a
    QPushButton; what the button actually shows (the label, elided to the
    column's width, or nothing when compact) is derived from them.
    """

    label_changed = pyqtSignal()

    def __init__(self, glyph: str, label: str, parent=None, *, checkable: bool = False):
        super().__init__(parent)
        self._glyph = glyph
        self._label = label or ""
        self._hint = ""
        self._compact = False
        self._text_width = None
        # A keyboard shortcut to show beside the label in menus ("Ctrl+1").
        self.shortcut_text = ""
        self.setObjectName("navItem")
        self.setCheckable(checkable)
        self.setFlat(True)
        self.setCursor(Qt.CursorShape.PointingHandCursor)
        # Like a tab: reachable with Tab, but a click leaves the focus where it was.
        self.setFocusPolicy(Qt.FocusPolicy.TabFocus)
        self.setIconSize(QSize(ICON_SIZE, ICON_SIZE))
        self.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Fixed)
        self.repaint_icon()
        self._render()

    # -- the QPushButton API, as callers use it --------------------------------
    def setText(self, label):  # noqa: N802 - Qt API
        label = label or ""
        if label != self._label:
            self._label = label
            self._render()
            self.label_changed.emit()

    def text(self):
        return self._label

    def changeEvent(self, event):  # noqa: N802 - Qt API
        super().changeEvent(event)
        if event.type() == QEvent.Type.FontChange:
            # A font that arrives after the column was sized (a style sheet's,
            # the platform's): the column measures again, or the label is cut.
            self.label_changed.emit()

    def setToolTip(self, hint):  # noqa: N802 - Qt API
        self._hint = hint or ""
        self._render()

    def toolTip(self):  # noqa: N802 - Qt API
        return self._hint

    # -- the column's controls --------------------------------------------------
    def set_compact(self, compact: bool):
        self._compact = bool(compact)
        self._render()

    def set_text_width(self, px):
        self._text_width = px
        self._render()

    def label_width(self) -> int:
        """The width the label needs, measured bold for a section (the current
        one is drawn bold, and the column must not change width when it moves)."""
        font = QFont(self.font())
        if self.isCheckable():
            font.setBold(True)
        return QFontMetrics(font).horizontalAdvance(self._label)

    def repaint_icon(self):
        self.setIcon(glyph_icon(self._glyph, _text_color()))

    def _render(self):
        shown = ""
        if not self._compact:
            shown = self._label
            # Elided only when the label does not fit by the measure the column
            # is sized with (label_width): elidedText() can measure the same
            # text wider, and a label the column was made for came out cut
            # ("Composition Se..." on the Windows CI).
            if self._text_width and self.label_width() > self._text_width:
                font = QFont(self.font())
                if self.isCheckable():
                    font.setBold(True)
                shown = QFontMetrics(font).elidedText(
                    self._label, Qt.TextElideMode.ElideRight, self._text_width)
        super().setText(shown)
        tip = self._hint
        if shown != self._label:
            tip = f"{self._label}\n{self._hint}" if self._hint else self._label
        super().setToolTip(tip)
        self.setAccessibleName(self._label)


class SideNav(QFrame):
    """The column itself. Build it with ``add_section`` / ``add_button`` /
    ``add_separator`` / ``add_widget``, then call ``fit_to`` from the window's
    resizeEvent."""

    # The user's expanded/compact choice changed (from ☰): the caller saves it.
    toggled = pyqtSignal(bool)

    # The expanded column may take at most this share of the window's width.
    EXPANDED_WIDTH_SHARE = 0.2
    MIN_EXPANDED = 150
    MAX_EXPANDED = 240

    def __init__(self, parent=None, *, expanded: bool = True):
        super().__init__(parent)
        self.setObjectName("sideNav")
        self.setSizePolicy(QSizePolicy.Policy.Fixed, QSizePolicy.Policy.Preferred)
        self._prefer_expanded = bool(expanded)
        self._room = True
        # (label, callable) pairs added after the entries in the narrow-window
        # menu and the right-click menu -- the way back to the tab layout.
        self.menu_extras = []
        self.context_extras = []    # the right-click menu's only
        self._restyling = False
        self._entries = []          # (button, group) in display order, for the menu
        self._group = 0
        self.section_buttons = []
        self._sections = QButtonGroup(self)
        self._sections.setExclusive(True)
        # Never shown: measured to learn what an entry adds to its label.
        self._probe = NavButton("\u2630", _PROBE_TEXT, self)
        self._probe.hide()

        outer = QVBoxLayout(self)
        outer.setContentsMargins(6, 6, 6, 6)
        outer.setSpacing(2)
        self.toggle_button = NavButton("\u2630", "", self)
        self.toggle_button.clicked.connect(self._on_toggle)
        outer.addWidget(self.toggle_button)

        self._scroll = QScrollArea(self)
        self._scroll.setObjectName("sideNavScroll")
        self._scroll.setFrameShape(QFrame.Shape.NoFrame)
        self._scroll.setWidgetResizable(True)
        self._scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        self._scroll.verticalScrollBar().rangeChanged.connect(lambda *_: self._apply())
        body = QWidget()
        body.setObjectName("sideNavBody")
        body_layout = QVBoxLayout(body)
        body_layout.setContentsMargins(0, 0, 0, 0)
        body_layout.setSpacing(0)
        self._main = QVBoxLayout()
        self._main.setSpacing(2)
        self._foot = QVBoxLayout()
        self._foot.setSpacing(2)
        body_layout.addLayout(self._main)
        body_layout.addStretch(1)
        body_layout.addLayout(self._foot)
        self._scroll.setWidget(body)
        outer.addWidget(self._scroll, 1)

        self._restyle()
        self._apply()

    # -- building ---------------------------------------------------------------
    def add_section(self, glyph: str, label: str, shortcut: str = "") -> NavButton:
        """A section: exactly one is current (``set_current``). ``shortcut`` is
        shown as its hint and beside it in menus; the caller binds the key."""
        btn = self._add(NavButton(glyph, label, checkable=True), footer=False)
        btn.shortcut_text = shortcut
        btn.setToolTip(ltr(shortcut))
        self._sections.addButton(btn)
        self.section_buttons.append(btn)
        return btn

    def add_button(self, glyph: str, label: str, *, footer: bool = False,
                   shortcut: str = "") -> NavButton:
        """An action: a tool that opens its own window, Settings, the account...
        ``shortcut`` as for ``add_section``."""
        btn = self._add(NavButton(glyph, label), footer=footer)
        if shortcut:
            btn.shortcut_text = shortcut
            btn.setToolTip(ltr(shortcut))
        return btn

    def add_separator(self, *, footer: bool = False):
        line = QFrame()
        line.setObjectName("navSep")
        line.setFixedHeight(1)
        (self._foot if footer else self._main).addSpacing(4)
        (self._foot if footer else self._main).addWidget(line)
        (self._foot if footer else self._main).addSpacing(4)
        self._group += 1
        return line

    def add_widget(self, widget: QWidget, *, footer: bool = True):
        """Anything else (the version), centred; it is not offered in the menu."""
        (self._foot if footer else self._main).addWidget(
            widget, 0, Qt.AlignmentFlag.AlignHCenter)
        return widget

    def _add(self, btn: NavButton, *, footer: bool) -> NavButton:
        (self._foot if footer else self._main).addWidget(btn)
        self._entries.append((btn, self._group))
        btn.label_changed.connect(self._apply)
        self._apply()
        return btn

    # -- state ------------------------------------------------------------------
    def set_current(self, index: int):
        if 0 <= index < len(self.section_buttons):
            self.section_buttons[index].setChecked(True)

    def is_expanded(self) -> bool:
        """Whether the labels are showing (the choice AND the room for it)."""
        return self._prefer_expanded and self._room

    def set_expanded(self, expanded: bool):
        """Set the user's choice (as ☰ does), without reporting it."""
        self._prefer_expanded = bool(expanded)
        self._apply()

    def fit_to(self, window_width: int):
        """Compact unless the expanded column fits in its share of the window."""
        room = self._expanded_width() <= window_width * self.EXPANDED_WIDTH_SHARE
        if room != self._room:
            self._room = room
            self._apply()

    def _buttons(self):
        return [self.toggle_button] + [b for b, _g in self._entries]

    def _margins(self) -> int:
        m = self.layout().contentsMargins()
        return m.left() + m.right()

    def _extra(self) -> int:
        """What an entry adds to its label: the icon, padding and accent bar, as
        the style draws them (measured, not assumed)."""
        self._probe.ensurePolished()
        return (self._probe.sizeHint().width()
                - QFontMetrics(self._probe.font()).horizontalAdvance(_PROBE_TEXT))

    def _expanded_width(self) -> int:
        labels = [b.label_width() for b, _g in self._entries] or [0]
        # +2: elidedText() and horizontalAdvance() may round a pixel apart.
        want = self._margins() + self._extra() + max(labels) + 2
        return max(self.MIN_EXPANDED, min(self.MAX_EXPANDED, want))

    def _compact_width(self) -> int:
        self.toggle_button.ensurePolished()
        return self._margins() + self.toggle_button.sizeHint().width()

    def _apply(self):
        expanded = self.is_expanded()
        width = self._expanded_width() if expanded else self._compact_width()
        text_width = width - self._margins() - self._extra()
        for b in self._buttons():
            b.set_compact(not expanded or b is self.toggle_button)
            b.set_text_width(text_width)
        bar = self._scroll.verticalScrollBar()
        if bar.maximum() > 0:
            width += bar.sizeHint().width()
        self.setFixedWidth(width)
        if not self._room:
            tip = tr("Menu")
        elif expanded:
            tip = tr("Collapse menu")
        else:
            tip = tr("Expand menu")
        self.toggle_button.setToolTip(tip)

    def _on_toggle(self):
        if not self._room:
            self._show_menu()
            return
        self._prefer_expanded = not self._prefer_expanded
        self._apply()
        self.toggled.emit(self._prefer_expanded)

    def build_menu(self, menu: QMenu, *, omit=(), extras=None) -> QMenu:
        """Every entry (but those in ``omit``) as a menu item: icon, label and
        shortcut, the current section in bold. No item is checkable, so no tick
        boxes. Then ``extras`` (default ``menu_extras``) after a separator.
        Clicking an item clicks the entry, even while the column is hidden."""
        menu.clear()
        last_group = None
        for btn, group in self._entries:
            if btn in omit or explicitly_hidden(btn):
                continue
            if last_group is not None and group != last_group:
                menu.addSeparator()
            last_group = group
            # A tab marks QMenu's shortcut column; nothing is bound by it.
            text = f"{btn.text()}\t{ltr(btn.shortcut_text)}" if btn.shortcut_text else btn.text()
            act = menu.addAction(btn.icon(), text)
            if btn.isChecked():
                font = act.font()
                font.setBold(True)
                act.setFont(font)
            act.setEnabled(btn.isEnabled())
            act.triggered.connect(lambda _c=False, b=btn: b.click())
        self._add_extras(menu, self.menu_extras if extras is None else extras)
        return menu

    @staticmethod
    def _add_extras(menu: QMenu, extras):
        if extras:
            menu.addSeparator()
            for text, slot in extras:
                menu.addAction(text).triggered.connect(lambda _c=False, f=slot: f())

    def menu_position(self, menu: QMenu) -> QPoint:
        """Below ☰, opening toward the window's content."""
        return popup_point(self.toggle_button, menu)

    def _show_menu(self):
        menu = self.build_menu(QMenu(self))
        menu.exec(self.menu_position(menu))

    def contextMenuEvent(self, event):  # noqa: N802 - Qt API
        """Right-click: labels on or off (when there is room for them), the
        ``menu_extras`` and the ``context_extras`` -- so the way back to tabs
        is on the menu itself, not only in Settings."""
        menu = QMenu(self)
        if self._room:
            menu.addAction(self.toggle_button.toolTip()).triggered.connect(self._on_toggle)
        self._add_extras(menu, list(self.menu_extras) + list(self.context_extras))
        if menu.actions():
            menu.exec(event.globalPos())

    # -- look -------------------------------------------------------------------
    def changeEvent(self, event):  # noqa: N802 - Qt API
        super().changeEvent(event)
        if event.type() in (QEvent.Type.PaletteChange, QEvent.Type.LayoutDirectionChange):
            self._restyle()
        if event.type() in (QEvent.Type.FontChange, QEvent.Type.StyleChange):
            # Labels measured in the old font no longer fit (or over-fit).
            self._apply()

    def _restyle(self):
        """Colours from the application palette, so the column follows a dark or
        light Windows theme; the accent bar on the start edge, whichever side
        that is."""
        if self._restyling:
            return
        self._restyling = True
        try:
            pal = QApplication.palette()
            window = pal.color(QPalette.ColorRole.Window)
            is_dark = window.lightness() < 128
            bg = window.darker(125 if is_dark else 104).name()
            edge = pal.color(QPalette.ColorRole.Mid).name()
            hi = pal.color(QPalette.ColorRole.Highlight)
            current_bg = f"rgba({hi.red()},{hi.green()},{hi.blue()},{0.30 if is_dark else 0.16})"
            hover_bg = "rgba(255,255,255,0.07)" if is_dark else "rgba(0,0,0,0.06)"
            text = pal.color(QPalette.ColorRole.WindowText).name()
            # Box sides are physical in a style sheet, so the start edge is
            # chosen here; text-align is mirrored by Qt itself, so it stays "left".
            start, end = ("right", "left") if self.isRightToLeft() else ("left", "right")
            sheet = f"""
                #sideNav {{ background: {bg}; border: none; border-{end}: 1px solid {edge}; }}
                #sideNavScroll, #sideNavBody {{ background: transparent; }}
                #sideNavScroll QScrollBar:vertical {{ width: 6px; background: transparent; }}
                #sideNavScroll QScrollBar::handle:vertical {{ background: {edge}; border-radius: 3px; min-height: 20px; }}
                #sideNavScroll QScrollBar::add-line, #sideNavScroll QScrollBar::sub-line {{ height: 0; }}
                #navSep {{ background: {edge}; margin: 0 6px; }}
                QPushButton#navItem {{
                    text-align: left; color: {text}; background: transparent;
                    border: none; border-{start}: {_BAR}px solid transparent; border-radius: 6px;
                    padding: {_PAD_Y}px {_PAD_X}px;
                }}
                QPushButton#navItem:hover {{ background: {hover_bg}; }}
                QPushButton#navItem:checked {{
                    background: {current_bg}; font-weight: bold;
                    border-{start}: {_BAR}px solid {hi.name()};
                }}
                QPushButton#navItem:disabled {{ color: {pal.color(QPalette.ColorRole.PlaceholderText).name()}; }}
            """
            # An unchanged sheet is not re-applied: re-polishing can itself post
            # a PaletteChange, which would bring us back here.
            if sheet != self.styleSheet():
                self.setStyleSheet(sheet)
            for b in self._buttons() + [self._probe]:
                b.repaint_icon()
        finally:
            self._restyling = False
