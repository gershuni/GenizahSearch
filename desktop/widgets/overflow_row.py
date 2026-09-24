# -*- coding: utf-8 -*-
"""A one-line row of controls that moves what does not fit into a "More" menu.

Why it exists: at 300% Windows zoom, or on a 1366x768 laptop at 125%, the
Manuscript Viewer's action row asked for 1650-2500 logical px, and because a
QHBoxLayout's minimum width is the sum of its buttons, the viewer opened wider
than the screen with both edges cut off (owner screenshots, 2026-09-24).

How it works, and why not the obvious alternatives:

* Buttons in this app show and hide THEMSELVES as metadata arrives (a catalogue
  count, a bibliography, a LOCAL file). So overflow must never call setVisible()
  on them -- that would overwrite their own state. A widget the row cannot fit is
  moved outside the row's rectangle instead (clipped, out of the tab order),
  and ``isHidden()`` keeps meaning "not applicable for this manuscript". Only applicable widgets that did not fit are
  offered in the More menu, with their current text, enabled and checked state.
* QToolBar's own extension menu does not work for addWidget() outside a
  QMainWindow, which this dialog is not.
* Each widget has a priority. ``PINNED`` widgets (labels, combos -- anything a
  menu cannot represent) always stay in the row; among the rest the highest
  number leaves first, ties from the end of the row. The minimum width is the
  pinned widgets plus the More button, so the row never forces a window wider.
* Icons before menus (owner, 2026-09-24): when the row is tight, buttons whose
  label has an icon ("\U0001f4d6 Browse", "Next Result \u25b6") first shrink to
  that icon, with the label as tooltip; only then do the least important
  widgets move into More. Anything still too wide is squeezed toward its own
  minimum before it is cut.
* A droppable widget that is not a button (a label) simply leaves the row when
  there is no room; it has no menu entry, so put what it says somewhere else too.
* Right-to-left UIs are mirrored, like a QHBoxLayout.
"""
from __future__ import annotations

from PyQt6.QtCore import QRect, QSize, Qt
from PyQt6.QtGui import QGuiApplication
from PyQt6.QtWidgets import (
    QAbstractButton, QLayout, QMenu, QSizePolicy, QToolButton, QWidget,
)

PINNED = -1

# ``short_text`` sentinel: derive the icon from the label ("\U0001f4d6 Browse" -> the book).
AUTO = object()


def icon_part(text: str):
    """The icon a label starts or ends with, if it has one.

    "\U0001f4d6 Browse" -> "\U0001f4d6", "Next Result \u25b6" -> "\u25b6"; a label
    with no symbol-only first or last word gives None (it has no icon to keep).
    """
    parts = (text or "").split()
    if len(parts) < 2:
        return None
    for token in (parts[0], parts[-1]):
        if token and not any(ch.isalnum() for ch in token):
            return token
    return None


def explicitly_hidden(w: QWidget) -> bool:
    """True only when code hid the widget on purpose (setVisible(False)/hide()).

    A widget that was never shown also reports isHidden(), and Qt hides a widget
    whenever its parent changes; neither means "not applicable".
    """
    return (w.testAttribute(Qt.WidgetAttribute.WA_WState_ExplicitShowHide)
            and w.isHidden())


def reparent_keeping_state(w: QWidget, parent: QWidget):
    """setParent() without losing whether the widget was meant to be shown."""
    hidden = explicitly_hidden(w)
    w.setParent(parent)
    if not hidden:
        w.setVisible(True)

_OVERFLOW_PROP = "_overflow_hidden"
_SAVED_FOCUS_PROP = "_overflow_saved_focus"


class _Entry:
    __slots__ = ("item", "priority", "stretch", "short_text", "full_text", "applied",
                 "set_tip", "group")

    def __init__(self, item, priority, stretch, short_text=None, group=None):
        self.item = item
        self.group = group             # overflows into this group's menu, not More
        self.priority = priority
        self.stretch = stretch
        self.short_text = short_text   # str, AUTO (the label's icon) or None (never shorten)
        self.full_text = None          # the label to restore when there is room again
        self.applied = None            # the short label we set, to notice the code changing it
        self.set_tip = False           # we supplied the tooltip while shortened


class OverflowLayout(QLayout):
    """The layout behind OverflowRow. Use OverflowRow unless you need the layout."""

    def __init__(self, more_button: QToolButton, parent=None, spacing: int = 6):
        super().__init__(parent)
        self._entries: list[_Entry] = []
        self._more = more_button
        self._spacing = spacing
        self._overflowed: list[QWidget] = []
        self._over_entries: list = []
        self._groups: dict = {}          # group name -> its menu button
        self.setContentsMargins(0, 0, 0, 0)

    def set_group_button(self, group, button):
        self._groups[group] = button

    # --- QLayout plumbing ------------------------------------------------------
    def add(self, widget: QWidget, priority: int = 0, stretch: bool = False,
            short_text=AUTO, group=None):
        self.addChildWidget(widget)
        from PyQt6.QtWidgets import QWidgetItem
        self._entries.append(_Entry(QWidgetItem(widget), priority, stretch, short_text, group))
        self.invalidate()

    def addItem(self, item):  # noqa: N802 - Qt API
        self._entries.append(_Entry(item, 0, False))

    def count(self):
        return len(self._entries)

    def itemAt(self, index):  # noqa: N802
        if 0 <= index < len(self._entries):
            return self._entries[index].item
        return None

    def takeAt(self, index):  # noqa: N802
        if 0 <= index < len(self._entries):
            return self._entries.pop(index).item
        return None

    def expandingDirections(self):  # noqa: N802
        return Qt.Orientation(0)

    def spacing(self):
        return self._spacing

    # --- sizes -----------------------------------------------------------------
    @staticmethod
    def _width_of(w: QWidget) -> int:
        want = max(w.sizeHint().width(), w.minimumSizeHint().width())
        return max(w.minimumWidth(), min(want, w.maximumWidth()))

    @staticmethod
    def _min_of(w: QWidget) -> int:
        """The narrowest a widget may be squeezed to: an explicit minimum width if
        its code set one (the version combo), else Qt's minimum size hint."""
        m = w.minimumWidth() if w.minimumWidth() > 0 else w.minimumSizeHint().width()
        return min(m, w.maximumWidth())

    @staticmethod
    def _height_of(w: QWidget) -> int:
        want = max(w.sizeHint().height(), w.minimumSizeHint().height())
        return max(w.minimumHeight(), min(want, w.maximumHeight()))

    def _text_width(self, e: _Entry, text: str) -> int:
        """Width a button needs to show the short ``text`` (an icon) with padding.

        Not derived from sizeHint(): the Fusion style gives every text button at
        least 80 px, so an icon-only button would save nothing. The layout may
        place a widget narrower than its size hint; only an explicit
        minimumWidth() binds.
        """
        w = e.item.widget()
        fm = w.fontMetrics()
        pad = max(fm.height(), 12)
        return min(max(w.minimumWidth(), fm.horizontalAdvance(text) + pad), w.maximumWidth())

    def _sync_full_text(self, e: _Entry):
        """If the widget's own code relabelled it while shortened (Trans ON ->
        OFF), that new label is the one to keep and restore."""
        if e.full_text is not None and e.item.widget().text() != e.applied:
            e.full_text = e.item.widget().text()
            e.applied = None

    def _full_text(self, e: _Entry) -> str:
        self._sync_full_text(e)
        return e.full_text if e.full_text is not None else e.item.widget().text()

    def _short_of(self, e: _Entry):
        if e.short_text is None or not isinstance(e.item.widget(), QAbstractButton):
            return None
        if e.short_text is AUTO:
            return icon_part(self._full_text(e))
        return e.short_text

    def _entry_width(self, e: _Entry, compact: bool) -> int:
        short = self._short_of(e)
        if short is None:
            return self._width_of(e.item.widget())
        if compact:
            return self._text_width(e, short)
        w = e.item.widget()
        if e.full_text is None:
            return self._width_of(w)           # showing its label now: measure it
        # Shortened now: its size hint describes the icon, so estimate the label.
        fm = w.fontMetrics()
        extra = fm.horizontalAdvance(e.full_text) - fm.horizontalAdvance(w.text())
        return max(self._width_of(w) + extra, self._text_width(e, short))

    def _entry_min(self, e: _Entry) -> int:
        short = self._short_of(e)
        if short is not None:
            return self._text_width(e, short)
        return self._min_of(e.item.widget())

    def _applicable(self):
        return [e for e in self._entries
                if e.item.widget() is not None and not e.item.widget().isHidden()]

    def _margins_w(self):
        m = self.contentsMargins()
        return m.left() + m.right(), m.top() + m.bottom()

    def _extras(self, dropped):
        """The menu buttons ``dropped`` entries need: one per group they belong
        to, plus More for the ungrouped ones."""
        out, seen = [], set()
        for e in dropped:
            g = e.group
            if g is not None and g in self._groups and g not in seen:
                seen.add(g)
                out.append(self._groups[g])
        if any(e.group is None or e.group not in self._groups for e in dropped):
            out.append(self._more)
        return out

    def _row_width(self, entries, compact=(), dropped=()):
        """``compact``: True for every entry, or a set of entry ids shown as icons;
        ``dropped``: entries that went into menus (their menu buttons count)."""
        widths = [self._entry_width(e, compact is True or id(e) in compact) for e in entries]
        widths += [self._width_of(b) for b in self._extras(dropped)]
        if not widths:
            return 0
        return sum(widths) + self._spacing * (len(widths) - 1)

    def _height(self, entries):
        return max([self._height_of(e.item.widget()) for e in entries] + [self._height_of(self._more)])

    def sizeHint(self):  # noqa: N802
        entries = self._applicable()
        mw, mh = self._margins_w()
        return QSize(self._row_width(entries) + mw, self._height(entries) + mh)

    def minimumSize(self):  # noqa: N802
        entries = self._applicable()
        pinned = [e for e in entries if e.priority == PINNED]
        mw, mh = self._margins_w()
        widths = [self._entry_min(e) for e in pinned]
        widths += [self._width_of(b) for b in
                   self._extras([e for e in entries if e.priority != PINNED])]
        w = sum(widths) + self._spacing * max(len(widths) - 1, 0)
        return QSize(w + mw, self._height(entries) + mh)

    # --- placement -------------------------------------------------------------
    def _choose(self, avail: int):
        """(placed, overflowed, compact ids) -- the lists in row order.

        Icons before menus (owner, 2026-09-24): first every label as it is;
        then buttons with an icon shrink to it ONE AT A TIME, least important
        first, stopping as soon as the row fits; only when all of them are
        icons do the least important widgets move into More.
        """
        entries = self._applicable()
        if self._row_width(entries) <= avail:
            return entries, [], set()

        def rank(e):   # pinned widgets shrink last
            return (e.priority if e.priority != PINNED else -2, entries.index(e))

        compact = set()
        for e in sorted((e for e in entries if self._short_of(e) is not None),
                        key=rank, reverse=True):
            compact.add(id(e))
            if self._row_width(entries, compact=compact) <= avail:
                return entries, [], compact
        droppable = [e for e in entries if e.priority != PINNED]
        # Leave first: highest priority number, then the one further along the row.
        order = sorted(droppable, key=lambda e: (e.priority, entries.index(e)), reverse=True)
        dropped = set()
        for e in order:
            dropped.add(id(e))
            kept = [x for x in entries if id(x) not in dropped]
            gone = [x for x in entries if id(x) in dropped]
            if self._row_width(kept, compact=True, dropped=gone) <= avail:
                break
        placed = [e for e in entries if id(e) not in dropped]
        over = [e for e in entries if id(e) in dropped]
        return placed, over, compact

    def _is_rtl(self):
        pw = self.parentWidget()
        d = pw.layoutDirection() if pw is not None else QGuiApplication.layoutDirection()
        return d == Qt.LayoutDirection.RightToLeft

    def _apply_short_texts(self, compact, over=()):
        """Show the icon of each button in ``compact`` (a set of entry ids), the
        label of every other one.

        An overflowed button gets its full label back, because the More menu is
        built from the button's own text.
        """
        overflowed = {id(e) for e in over}
        for e in self._entries:
            short = self._short_of(e)
            if short is None and e.full_text is None:
                continue
            w = e.item.widget()
            self._sync_full_text(e)
            want_short = id(e) in compact and short is not None and id(e) not in overflowed
            if want_short:
                if e.full_text is None:
                    e.full_text = w.text()
                if w.text() != short:
                    w.setText(short)
                e.applied = short
                if not w.toolTip():
                    w.setToolTip(e.full_text)
                    e.set_tip = True
            elif e.full_text is not None:
                full = e.full_text
                e.full_text = None
                e.applied = None
                if w.text() != full:
                    w.setText(full)
                if e.set_tip:
                    w.setToolTip("")
                    e.set_tip = False

    def setGeometry(self, rect: QRect):  # noqa: N802
        super().setGeometry(rect)
        m = self.contentsMargins()
        inner = rect.adjusted(m.left(), m.top(), -m.right(), -m.bottom())
        placed, over, compact = self._choose(inner.width())
        self._apply_short_texts(compact, over)

        # Row order; a group's menu button takes the place of its first member
        # that went into it, and More goes last.
        placed_ids = {id(e) for e in placed}
        extras = self._extras(over)
        slots, widths, mins, stretch_flags, emitted = [], [], [], [], set()
        for e in self._applicable():
            if id(e) in placed_ids:
                slots.append(e.item.widget())
                widths.append(self._entry_width(e, id(e) in compact))
                mins.append(self._entry_min(e))
                stretch_flags.append(e.stretch)
            elif e.group is not None and e.group in self._groups and e.group not in emitted:
                emitted.add(e.group)
                btn = self._groups[e.group]
                slots.append(btn)
                widths.append(self._width_of(btn))
                mins.append(self._width_of(btn))
                stretch_flags.append(False)
        if self._more in extras:
            slots.append(self._more)
            widths.append(self._width_of(self._more))
            mins.append(self._width_of(self._more))
            stretch_flags.append(False)
        used = sum(widths) + self._spacing * max(len(slots) - 1, 0)
        if used > inner.width():
            # Still too wide: squeeze each widget toward its own minimum, in
            # proportion to how much it can give.
            deficit = used - inner.width()
            give = [max(wd - mn, 0) for wd, mn in zip(widths, mins)]
            total = sum(give)
            if total:
                cut = [min(g, deficit * g // total) for g in give]
                left = deficit - sum(cut)
                for i, g in enumerate(give):
                    if left <= 0:
                        break
                    extra_cut = min(g - cut[i], left)
                    cut[i] += extra_cut
                    left -= extra_cut
                widths = [wd - c for wd, c in zip(widths, cut)]
        used = sum(widths) + self._spacing * max(len(slots) - 1, 0)
        extra = max(inner.width() - used, 0)
        stretchers = [i for i, f in enumerate(stretch_flags) if f]
        if stretchers and extra:
            share, rest = divmod(extra, len(stretchers))
            for n, i in enumerate(stretchers):
                widths[i] += share + (1 if n < rest else 0)

        rtl = self._is_rtl()
        x = inner.left()
        for w, width in zip(slots, widths):
            h = min(self._height_of(w), inner.height())
            y = inner.top() + (inner.height() - h) // 2
            left = (inner.left() + inner.right() + 1 - x - width) if rtl else x
            w.setGeometry(QRect(left, y, width, h))
            self._set_overflowed(w, False)
            x += width + self._spacing

        for e in over:
            self._set_overflowed(e.item.widget(), True)
        # Hidden (not applicable) widgets need no geometry; Qt does not show them.
        for btn in [self._more] + list(self._groups.values()):
            if btn not in slots:
                self._set_overflowed(btn, True)
        self._overflowed = [e.item.widget() for e in over]
        self._over_entries = list(over)

    @staticmethod
    def _park(w: QWidget):
        # Outside the row's rectangle, so it is clipped away. Not a zero size:
        # fixed-size widgets (the Cite and Joins tool buttons) refuse to shrink.
        w.move(-10000 - w.width(), -10000 - w.height())

    @classmethod
    def _set_overflowed(cls, w: QWidget, on: bool):
        """Park a widget outside the row (and out of the tab order) without hiding it."""
        was = bool(w.property(_OVERFLOW_PROP))
        if on == was:
            if on:
                cls._park(w)
            return
        w.setProperty(_OVERFLOW_PROP, on)
        if on:
            w.setProperty(_SAVED_FOCUS_PROP, int(w.focusPolicy().value))
            w.setFocusPolicy(Qt.FocusPolicy.NoFocus)
            cls._park(w)
        else:
            saved = w.property(_SAVED_FOCUS_PROP)
            if saved is not None:
                w.setFocusPolicy(Qt.FocusPolicy(int(saved)))

    def overflowed(self) -> list:
        """Applicable widgets that did not fit at the last layout pass, in row order."""
        return [w for w in self._overflowed if not w.isHidden()]

    def overflowed_in(self, group) -> list:
        """The overflowed widgets that belong in ``group``'s menu (None: More)."""
        out = []
        for e in self._over_entries:
            w = e.item.widget()
            if w.isHidden():
                continue
            g = e.group if e.group in self._groups else None
            if g == group:
                out.append(w)
        return out


def populate_menu_from_buttons(menu: QMenu, widgets) -> int:
    """Fill ``menu`` with one entry per applicable button, mirroring its state.

    Text, icon, tooltip, enabled and checked state are copied at the moment the
    menu opens; choosing an entry clicks the real button, so every handler and
    toggle stays in one place. A tool button with its own menu becomes a submenu.
    Returns the number of entries added.
    """
    menu.clear()
    n = 0
    for w in widgets:
        if w is None or explicitly_hidden(w) or not isinstance(w, QAbstractButton):
            continue
        text = w.text() or w.toolTip() or w.accessibleName()
        if w.toolTip() and len(w.text()) <= 2:
            # Symbol-only tool buttons ("“”", "🔗"): the tooltip is the name.
            text = f"{w.text()}  {w.toolTip()}".strip()
        sub = w.menu() if isinstance(w, QToolButton) else None
        # A split button (MenuButtonPopup: its own action plus a dropdown) keeps
        # both. The menu is attached even when it is empty now: many are filled
        # by their aboutToShow handler (the viewer's Joins menu), which runs
        # when the submenu opens (Codex, #362).
        split = (sub is not None and w.popupMode()
                 == QToolButton.ToolButtonPopupMode.MenuButtonPopup)
        if sub is None or split:
            act = menu.addAction(w.icon(), text)
            act.setToolTip(w.toolTip())
            if w.isCheckable():
                act.setCheckable(True)
                act.setChecked(w.isChecked())
            act.triggered.connect(lambda _checked=False, b=w: b.click())
            act.setEnabled(w.isEnabled())
            n += 1
        if sub is not None:
            sub_act = menu.addMenu(sub)
            sub_act.setText(f"{text} \u25b8" if split else text)
            sub_act.setEnabled(w.isEnabled())
            n += 1
    return n


class OverflowRow(QWidget):
    """A row of controls with a More menu for the ones that do not fit.

    ``add(widget, priority=0, stretch=False)`` in display order. ``PINNED`` keeps a
    widget in the row at any width (use it for anything a menu cannot hold).
    """

    def __init__(self, more_text: str = "More", parent=None, spacing: int = 6):
        super().__init__(parent)
        self.more_button = QToolButton(self)
        self.more_button.setText(f"{more_text} ▾")
        self.more_button.setToolTip(more_text)
        self.more_button.setPopupMode(QToolButton.ToolButtonPopupMode.InstantPopup)
        self._menu = QMenu(self.more_button)
        self._menu.aboutToShow.connect(self._fill_menu)
        self.more_button.setMenu(self._menu)
        self._group_buttons = {}
        self.row = OverflowLayout(self.more_button, self, spacing=spacing)
        self.setSizePolicy(QSizePolicy.Policy.Preferred, QSizePolicy.Policy.Fixed)

    def add(self, widget: QWidget, priority: int = 0, stretch: bool = False,
            short_text=AUTO, group=None):
        """Append ``widget``. When the row is tight a button shows ``short_text``
        instead of its label -- by default (AUTO) the icon its label starts or
        ends with; None never shortens it. The full label is the tooltip
        meanwhile. A button that opens a group menu keeps its label.

        ``group``: a name ("Sources"). A member that does not fit goes into that
        group's own menu button, shown where the group sits, instead of More --
        so on a wide screen every member is its own button (owner, 2026-09-24).
        """
        if isinstance(widget, MenuGroupButton):
            short_text = None
        if group is not None and group not in self._group_buttons:
            btn = QToolButton(self)
            btn.setText(f"{group} \u25be")
            btn.setToolTip(group)
            btn.setPopupMode(QToolButton.ToolButtonPopupMode.InstantPopup)
            menu = QMenu(btn)
            menu.aboutToShow.connect(lambda g=group, m=menu: populate_menu_from_buttons(
                m, self.row.overflowed_in(g)))
            btn.setMenu(menu)
            self._group_buttons[group] = btn
            self.row.set_group_button(group, btn)
        reparent_keeping_state(widget, self)
        self.row.add(widget, priority, stretch, short_text, group)
        return widget

    def group_button(self, group):
        return self._group_buttons.get(group)

    def overflowed(self):
        return self.row.overflowed()

    _width_cap = None

    def set_width_cap(self, cap):
        """Cap the width this row ASKS for (its sizeHint), never below its minimum.

        For containers that size a child by its sizeHint alone and never squeeze
        it -- a QTabWidget corner widget is one -- so the row never overflows on
        its own. ``None`` removes the cap.
        """
        self._width_cap = cap
        self.updateGeometry()

    def sizeHint(self):  # noqa: N802 - Qt API
        sh = super().sizeHint()
        if self._width_cap is not None:
            sh.setWidth(min(sh.width(), max(int(self._width_cap), self.minimumSizeHint().width())))
        return sh

    def _fill_menu(self):
        populate_menu_from_buttons(self._menu, self.row.overflowed_in(None))


class MenuGroupButton(QToolButton):
    """One button standing for a group of buttons, listed in its menu.

    The grouped buttons live, unshown, inside this button's holder widget, so
    their own code keeps calling setVisible/setText/setEnabled on them exactly
    as before. This button hides itself when none of them is applicable, and its
    menu is rebuilt from their state each time it opens.
    """

    def __init__(self, text: str, buttons, parent=None):
        super().__init__(parent)
        self._label = text
        self.setText(f"{text} ▾")
        self.setToolTip(text)
        self.setPopupMode(QToolButton.ToolButtonPopupMode.InstantPopup)
        self._menu = QMenu(self)
        self._menu.aboutToShow.connect(self._fill)
        self.setMenu(self._menu)
        self._holder = QWidget(self)
        self._holder.hide()
        self.buttons = list(buttons)
        for b in self.buttons:
            reparent_keeping_state(b, self._holder)
            b.installEventFilter(self)
        self.refresh()

    def eventFilter(self, obj, event):  # noqa: N802
        from PyQt6.QtCore import QEvent
        if event.type() in (QEvent.Type.ShowToParent, QEvent.Type.HideToParent,
                            QEvent.Type.EnabledChange):
            self.refresh()
        return False

    def applicable(self):
        return [b for b in self.buttons if not explicitly_hidden(b)]

    def refresh(self):
        has_any = bool(self.applicable())
        if explicitly_hidden(self) == has_any or not self.testAttribute(
                Qt.WidgetAttribute.WA_WState_ExplicitShowHide):
            self.setVisible(has_any)

    def _fill(self):
        populate_menu_from_buttons(self._menu, self.buttons)
