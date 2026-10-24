# -*- coding: utf-8 -*-
"""The Go to box (Ctrl+G, owner 2026-10-24): type a shelfmark, a system number
or an FL number to open the manuscript in Browse from anywhere in the window --
or Hebrew or Arabic words to search them. What the text is, is decided by
``shared/quick_open.py``, the same rule as the website's homepage box; the
window acts on it (``GenizahGUI.go_to``).

The shelfmark field offers the same completions as Browse's (the window's
``shelf_completer``, once the catalogue has loaded).
"""
from __future__ import annotations

from PyQt6.QtCore import Qt
from PyQt6.QtWidgets import (
    QDialog, QDialogButtonBox, QLabel, QLineEdit, QVBoxLayout,
)

from genizah_core import tr


class GoToDialog(QDialog):
    """Asks for one line; ``text()`` is what was typed when it was accepted."""

    def __init__(self, parent=None, completer=None):
        super().__init__(parent)
        self.setWindowTitle(tr("Go to"))
        self.setMinimumWidth(420)
        layout = QVBoxLayout(self)
        hint = QLabel(tr("A shelfmark, system number or FL number opens the manuscript. "
                         "Hebrew or Arabic words are searched."))
        hint.setWordWrap(True)
        layout.addWidget(hint)
        self.input = QLineEdit()
        self.input.setPlaceholderText(tr("For example T-S 12.123"))
        self.input.setClearButtonEnabled(True)
        if completer is not None:
            self.input.setCompleter(completer)
        layout.addWidget(self.input)
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

    def detach_completer(self):
        """Give the shared completer back before the dialog goes: a QLineEdit
        that is destroyed with a completer set would leave it pointing at a
        deleted widget, and Browse's shelfmark field shares it."""
        self.input.setCompleter(None)
