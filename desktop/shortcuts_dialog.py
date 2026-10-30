# -*- coding: utf-8 -*-
"""The keyboard shortcuts card (owner, 2026-10-30: a key you do not remember
is a key you do not have): every key of the main window on one page, from
Ctrl+/ and "Keyboard shortcuts" in the ☰ menu and the side menu.

``SHORTCUTS`` is the list. ``tests/test_quick_open.py`` checks it against the
keys ``GenizahGUI.init_ui`` binds, both ways, so the card names no key that
does nothing and leaves none out; the keys a single field or table takes
(Down, Space, Esc) are checked where they are bound. Ctrl++ is left out: it
is Ctrl+= by another name, the same key on most keyboards.
"""
from __future__ import annotations

from PyQt6.QtWidgets import QDialog, QDialogButtonBox, QFormLayout, QLabel, QVBoxLayout

from desktop.widgets.side_nav import ltr
from genizah_core import tr

# (keys as shown, what they do). " / " separates keys that do the same thing,
# " … " spans a run of keys.
SHORTCUTS = (
    ("Ctrl+1 … Ctrl+7", "Open a section, with the cursor in its search box where it has one"),
    ("Ctrl+F / Ctrl+E", "Put the cursor in the search box of the section in front"),
    ("Down", "In the search box: the search history (Enter runs a search, Delete removes it)"),
    ("Space / Shift+Space", "In the search results: a page down, a page up"),
    ("Esc", "Composition Search: stop the search"),
    ("Ctrl+G / Ctrl+O", "Go to: open a manuscript by its shelfmark or system number"),
    ("Ctrl+8", "Fragment Puzzle"),
    ("Ctrl+9", "Joins Lab"),
    ("Ctrl+= / Ctrl+- / Ctrl+0", "Larger, smaller or normal text"),
    ("F1", "Help, at the section in front"),
    ("Ctrl+,", "Settings"),
    ("Ctrl+/", "This list"),
)


class ShortcutsDialog(QDialog):
    """The keys on the left (on the right in Hebrew: QFormLayout mirrors), what
    they do beside them."""

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle(tr("Keyboard shortcuts"))
        layout = QVBoxLayout(self)
        form = QFormLayout()
        form.setHorizontalSpacing(18)
        form.setVerticalSpacing(8)
        for keys, what in SHORTCUTS:
            key_label = QLabel(ltr(keys))
            font = key_label.font()
            font.setBold(True)
            key_label.setFont(font)
            # One line each: a wrapped QLabel in a form row clipped its last
            # letter in Hebrew ("מספר מדף א").
            form.addRow(key_label, QLabel(tr(what)))
        layout.addLayout(form)
        buttons = QDialogButtonBox(QDialogButtonBox.StandardButton.Close)
        buttons.button(QDialogButtonBox.StandardButton.Close).setText(tr("Close"))
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)
