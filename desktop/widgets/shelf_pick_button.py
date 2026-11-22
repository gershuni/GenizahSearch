# -*- coding: utf-8 -*-
"""The "Pick from the shelf" button each desktop host puts beside its typed shelfmark box (shelf
tree picker, the desktop hosts of PRs 5-7: the Reading Desk, the Puzzle, the Join Workbench, the
Joins dialog, Share a Discovery). Browse and the Go to box have their own (PR 3).

One button, one rule for all of them: shown only while the shelf tree is offered
(``shelfmark_tree_picker.shelf_tree_service()``: ``SHELFMARK_TREE_ENABLED`` AND the file opened and
checked), so a host never shows a button that opens nothing; a click opens the picker
(``pick_from_shelf``, modal, single choice) at ``start()``; a pick that is not a corpus manuscript is
dropped (the tree never holds a LOCAL ``97`` id, plan §10.4), any other goes to the host's
``on_pick(selection)``, which hands it to the method the host's typed path ends in.

A button built before the tree was opened (the Reading Desk's toolbar is built with the window)
shows itself when ``tree_state().changed`` fires: ``GenizahGUI._on_shelf_tree_opened`` emits it.
"""
from __future__ import annotations

import logging
from typing import Callable, Optional

from PyQt6.QtCore import QObject, pyqtSignal
from PyQt6.QtWidgets import QPushButton

from genizah_core import tr
from shared.sys_id_patterns import CORPUS_SYS_ID_RE

logger = logging.getLogger(__name__)

ICON = "\U0001F4DA"


class _TreeState(QObject):
    """Says that the shelf tree has been opened (or refused) since the buttons were built."""

    changed = pyqtSignal()


_STATE: Optional[_TreeState] = None


def tree_state() -> _TreeState:
    """The process-wide notifier (made on first use, on the UI thread)."""
    global _STATE
    if _STATE is None:
        _STATE = _TreeState()
    return _STATE


def _service():
    # The module, not the name: tests replace shelf_tree_service and pick_from_shelf on it.
    import desktop.widgets.shelfmark_tree_picker as shelf_picker
    return shelf_picker.shelf_tree_service()


def corpus_pick(selection) -> Optional[dict]:
    """The pick when it names a corpus manuscript, else None."""
    sid = str((selection or {}).get('sys_id') or '')
    return selection if CORPUS_SYS_ID_RE.fullmatch(sid) else None


class ShelfPickButton(QPushButton):
    """See the module docstring. ``on_several(service)``, when given, is what a click does instead:
    the host opens the picker in its multiple-choice mode (``pick_several_from_shelf``: the Reading
    Desk, the Puzzle, a list), with the same rule for when the button is shown."""

    def __init__(self, parent, on_pick: Optional[Callable[[dict], object]] = None,
                 start: Callable[[], Optional[str]] = lambda: None, text: Optional[str] = None,
                 on_several: Optional[Callable[[object], object]] = None, act_label: Optional[str] = None):
        super().__init__(text or ICON, parent)
        self._on_pick = on_pick
        self._act_label = act_label                # the picker's button that takes the shelfmark (DX21)
        self._on_several = on_several
        self._start = start
        self.setAutoDefault(False)
        self.setToolTip(tr("Pick from the shelf"))
        self.setAccessibleName(tr("Pick from the shelf"))
        if not text:
            self.setFixedWidth(30)
        self.clicked.connect(self.pick)
        tree_state().changed.connect(self.refresh)
        self.refresh()

    def refresh(self) -> None:
        """Shown only while the shelf tree is offered."""
        self.setVisible(_service() is not None)

    def pick(self) -> Optional[dict]:
        service = _service()
        if service is None:
            self.refresh()
            return None
        if self._on_several is not None:
            self._on_several(service)
            return None
        import desktop.widgets.shelfmark_tree_picker as shelf_picker
        try:
            start = self._start()
        except Exception as exc:  # noqa: BLE001 -- the picker opens at the libraries instead
            logger.warning("Shelf picker start not read: %s", exc)
            start = None
        selection = corpus_pick(shelf_picker.pick_from_shelf(self.window(), service, start=start,
                                                             act_label=self._act_label))
        if selection is not None:
            self._on_pick(selection)
        return selection
