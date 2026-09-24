# -*- coding: utf-8 -*-
"""Right-click a results header to choose which columns show; keep them inside the view.

Why: at 300% zoom or on a small laptop the Search results spent a quarter of the
row on System ID and the star column before the snippet began (owner
screenshots, 2026-09-24). Hiding administrative columns gives the text room.

The choice is saved in the app config under ``config_key`` as a list of logical
column indices, so it survives restarts and never changes what a column holds.
Columns whose visibility the app decides from the data (the Src column, the
composition Witnesses column) are simply not offered: two owners of one
``setColumnHidden`` would undo each other.
"""
from __future__ import annotations

from typing import Callable, Iterable, Optional

from PyQt6.QtCore import QEvent, QObject, Qt, QTimer
from PyQt6.QtWidgets import QHeaderView, QMenu

from genizah_core import load_app_config, save_app_config, tr


class ColumnChooser:
    """Owns the user's shown/hidden choice for a fixed set of columns.

    ``view`` is a QTableView/QTreeView (anything with setColumnHidden);
    ``columns`` the logical indices the user may hide; ``label(col)`` the header
    text. ``default_hidden`` applies only when nothing was ever saved.
    """

    def __init__(self, view, header, columns: Iterable[int], config_key: str,
                 label: Callable[[int], str], default_hidden: Optional[Iterable[int]] = None):
        self.view = view
        self.header = header
        self.columns = [int(c) for c in columns]
        self.config_key = config_key
        self.label = label
        saved = load_app_config().get(config_key)
        if isinstance(saved, (list, tuple, set)):
            self.hidden = {int(c) for c in saved if int(c) in self.columns}
        else:
            self.hidden = {int(c) for c in (default_hidden or ()) if int(c) in self.columns}
        header.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        header.customContextMenuRequested.connect(self._show_menu)
        self.apply()

    # Set by the host when a ColumnFitter keeps this view inside its width.
    fitter = None

    def apply(self):
        for col in self.columns:
            self.view.setColumnHidden(col, col in self.hidden)
        if self.fitter is not None:
            self.fitter.schedule()

    def set_hidden(self, col: int, hidden: bool):
        if hidden:
            self.hidden.add(col)
        else:
            self.hidden.discard(col)
        save_app_config({self.config_key: sorted(self.hidden)})
        self.apply()

    def show_all(self):
        self.hidden.clear()
        save_app_config({self.config_key: []})
        self.apply()

    def build_menu(self, parent=None) -> QMenu:
        menu = QMenu(parent)
        menu.setTitle(tr("Columns"))
        for col in self.columns:
            act = menu.addAction(self.label(col) or str(col))
            act.setCheckable(True)
            act.setChecked(col not in self.hidden)
            act.toggled.connect(lambda on, c=col: self.set_hidden(c, not on))
        menu.addSeparator()
        menu.addAction(tr("Show all columns")).triggered.connect(self.show_all)
        if self.fitter is not None:
            menu.addAction(tr("Fit columns to window")).triggered.connect(self.fitter.refit)
        return menu

    def _show_menu(self, pos):
        menu = self.build_menu(self.header)
        menu.exec(self.header.mapToGlobal(pos))


class ColumnFitter(QObject):
    """Keep a view's columns inside its width, so no sideways scrolling is needed.

    Owner, 2026-09-24: every column should be on screen without scrolling. The
    reading columns (``flex``: the snippet, the composition contexts) take what
    the other columns leave; when even their minimum does not fit, the other
    columns give way toward ``min_other``, in proportion to their width.

    Why not QHeaderView's Stretch mode: a Stretch section cannot be dragged, and
    dragging its neighbour resizes the stretch section instead -- the "I moved
    it right and the other side moved" complaint (MS Context was Stretch until
    2026-09-03). Here every column stays Interactive.

    Widening any other column by hand makes it the new wish for that column; a
    reading column resized by hand switches fitting off until ``refit()`` (the
    header menu's "Fit columns to window").
    """

    def __init__(self, view, header, flex, min_flex=160, min_other=48):
        super().__init__(view)
        self.view = view
        self.header = header
        self.flex = [int(c) for c in flex]
        self.min_flex = min_flex
        self.min_other = min_other
        self.auto = True
        self._busy = False
        self.base = {c: header.sectionSize(c) for c in range(header.count())}
        self._timer = QTimer(self)
        self._timer.setSingleShot(True)
        self._timer.setInterval(0)
        self._timer.timeout.connect(self.fit)
        header.sectionResized.connect(self._on_resized)
        view.viewport().installEventFilter(self)

    def eventFilter(self, obj, event):  # noqa: N802 - Qt API
        if event.type() == QEvent.Type.Resize:
            self.schedule()
        return False

    def _on_resized(self, col, old, new):
        if self._busy:
            return
        if old == 0 or new == 0:          # a column shown or hidden
            self.schedule()
            return
        if col in self.flex:
            self.auto = False             # the reader sized the reading column
            return
        self.base[col] = new
        self.schedule()

    def schedule(self):
        if self.auto:
            self._timer.start()

    def refit(self):
        self.auto = True
        self.fit()

    def fit(self):
        if not self.auto:
            return
        h = self.header
        vw = self.view.viewport().width()
        if vw <= 0:
            return
        visible = [c for c in range(h.count()) if not h.isSectionHidden(c)]
        flex = [c for c in self.flex if c in visible]
        if not flex:
            return
        others = [c for c in visible if c not in flex]
        want = {c: self.base.get(c) or h.sectionSize(c) for c in others}
        need = self.min_flex * len(flex)
        avail = vw - sum(want.values())
        if avail < need:
            floor = max(self.min_other, h.minimumSectionSize())
            give = {c: max(want[c] - floor, 0) for c in others
                    if h.sectionResizeMode(c) != QHeaderView.ResizeMode.Fixed}
            total = sum(give.values())
            deficit = need - avail
            if total:
                cut = {c: min(g, deficit * g // total) for c, g in give.items()}
                left = deficit - sum(cut.values())
                for c, g in give.items():          # the pixels rounding left over
                    if left <= 0:
                        break
                    more = min(g - cut[c], left)
                    cut[c] += more
                    left -= more
                for c, n in cut.items():
                    want[c] -= n
            avail = vw - sum(want.values())
        avail = max(avail, need) - 1      # one pixel short: never a scrollbar from rounding
        weights = {c: max(self.base.get(c) or 1, 1) for c in flex}
        total_w = sum(weights.values())
        widths, used = {}, 0
        for c in flex[:-1]:
            widths[c] = max(self.min_flex, avail * weights[c] // total_w)
            used += widths[c]
        widths[flex[-1]] = max(self.min_flex, avail - used)
        self._busy = True
        try:
            for c, w in want.items():
                if h.sectionSize(c) != w:
                    h.resizeSection(c, w)
            for c, w in widths.items():
                if h.sectionSize(c) != w:
                    h.resizeSection(c, w)
        finally:
            self._busy = False
