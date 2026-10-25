# -*- coding: utf-8 -*-
"""The Go to box (Ctrl+G, owner 2026-10-24): type a shelfmark or a system
number to open the manuscript in Browse from anywhere in the window -- or
Hebrew or Arabic words to search them. What the text is, is decided by
``shared/quick_open.py``, the same rule as the website's homepage box; the
window acts on it (``GenizahGUI.go_to``). An FL number still works, but the
box no longer offers it (owner, 2026-10-25: nobody types one).

The shelfmark field offers the same completions as Browse's, from a completer
of its own on the window's ``shelf_completer`` model (once the catalogue has
loaded). Under it, the
manuscripts opened last (owner, 2026-10-25), so going back to one is
Ctrl+G, Down, Enter; typing narrows that list, and what is highlighted in it
is what opens.
"""
from __future__ import annotations

from PyQt6.QtCore import QEvent, Qt
from PyQt6.QtWidgets import (
    QAbstractItemView, QCompleter, QDialog, QDialogButtonBox, QLabel, QLineEdit, QListWidget,
    QListWidgetItem, QVBoxLayout,
)

from genizah_core import tr

RECENT_SHOWN = 10


class GoToDialog(QDialog):
    """Asks for one line, or one of the recent manuscripts.

    ``choice()`` is ``('recent', payload)`` when a recent manuscript was
    highlighted on accepting, else ``('text', text)``; ``text()`` is what was
    typed.
    """

    def __init__(self, parent=None, completer=None, recent=()):
        super().__init__(parent)
        self.setWindowTitle(tr("Go to"))
        self.setMinimumWidth(420)
        layout = QVBoxLayout(self)
        hint = QLabel(tr("A shelfmark or system number opens the manuscript. "
                         "Hebrew or Arabic words are searched."))
        hint.setWordWrap(True)
        layout.addWidget(hint)
        self.input = QLineEdit()
        self.input.setPlaceholderText(tr("For example T-S 12.123"))
        self.input.setClearButtonEnabled(True)
        if completer is not None:
            self.input.setCompleter(self._own_completer(completer))
        layout.addWidget(self.input)

        # (label, payload) pairs, most recent first.
        self._recent = list(recent)[:RECENT_SHOWN]
        self.recent_label = QLabel(tr("Recent manuscripts"))
        self.recent_list = QListWidget()
        self.recent_list.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)
        self.recent_list.itemActivated.connect(lambda _item: self.accept())
        for label, payload in self._recent:
            item = QListWidgetItem(label)
            item.setData(Qt.ItemDataRole.UserRole, payload)
            item.setToolTip(label)
            self.recent_list.addItem(item)
        rows = max(1, min(len(self._recent), RECENT_SHOWN))
        self.recent_list.setFixedHeight(
            self.recent_list.sizeHintForRow(0) * rows + 2 * self.recent_list.frameWidth() + 4)
        self.recent_label.setVisible(bool(self._recent))
        self.recent_list.setVisible(bool(self._recent))
        layout.addWidget(self.recent_label)
        layout.addWidget(self.recent_list)
        self.input.textChanged.connect(self._filter_recent)
        self.input.installEventFilter(self)

        buttons = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel)
        buttons.button(QDialogButtonBox.StandardButton.Ok).setText(tr("Go"))
        buttons.button(QDialogButtonBox.StandardButton.Cancel).setText(tr("Cancel"))
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)
        self.input.setFocus(Qt.FocusReason.OtherFocusReason)

    def text(self) -> str:
        return self.input.text().strip()

    def choice(self):
        item = self._highlighted()
        if item is not None:
            return ('recent', item.data(Qt.ItemDataRole.UserRole))
        return ('text', self.text())

    def _highlighted(self):
        for item in self.recent_list.selectedItems():
            if not item.isHidden():
                return item
        return None

    def _filter_recent(self, text):
        """Typing narrows the list (letters and digits only, so "TS NS 13"
        finds "T-S NS 13.15") and clears the highlight: what was typed is
        what opens, until a row is chosen again."""
        needle = _letters(text)
        for row in range(self.recent_list.count()):
            item = self.recent_list.item(row)
            item.setHidden(bool(needle) and needle not in _letters(item.text()))
        self.recent_list.clearSelection()

    def eventFilter(self, obj, event):
        # Down/Up from the box move through the recent list; the box keeps
        # the focus, so typing goes on narrowing it. Not while the shelfmark
        # completer's own popup is open: those keys are its.
        if obj is self.input and event.type() == QEvent.Type.KeyPress \
                and event.key() in (Qt.Key.Key_Down, Qt.Key.Key_Up):
            completer = self.input.completer()
            if completer is not None and completer.popup() is not None \
                    and completer.popup().isVisible():
                return False
            visible = [self.recent_list.item(r) for r in range(self.recent_list.count())
                       if not self.recent_list.item(r).isHidden()]
            if not visible:
                return False
            current = self._highlighted()
            if current is None:
                target = visible[0] if event.key() == Qt.Key.Key_Down else None
            else:
                at = visible.index(current)
                at += 1 if event.key() == Qt.Key.Key_Down else -1
                target = visible[at] if 0 <= at < len(visible) else None
            self.recent_list.clearSelection()
            if target is not None:
                target.setSelected(True)
                self.recent_list.scrollToItem(target)
            return True
        return super().eventFilter(obj, event)

    def _own_completer(self, shared):
        """A completer of its own on the shared one's model and settings. A
        QCompleter serves one widget at a time: borrowing Browse's left it
        attached to nothing once this box closed, and typing in Browse's
        shelfmark field then crashed (Codex review of PR #448, 2026-10-25)."""
        from desktop.widgets import ShelfmarkCompleter
        if isinstance(shared, ShelfmarkCompleter):
            own = ShelfmarkCompleter(shared.model(), self, valid_keys=shared.valid_keys)
        else:
            own = QCompleter(shared.model(), self)
        own.setCompletionRole(shared.completionRole())
        own.setCaseSensitivity(shared.caseSensitivity())
        own.setCompletionMode(shared.completionMode())
        own.setFilterMode(shared.filterMode())
        return own


def _letters(text: str) -> str:
    return "".join(ch for ch in (text or "").casefold() if ch.isalnum())
