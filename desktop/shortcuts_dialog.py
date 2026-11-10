# -*- coding: utf-8 -*-
"""The keyboard shortcuts card (owner, 2026-10-30: a key you do not remember
is a key you do not have): every key of the program on one page, from Ctrl+/
and "Keyboard shortcuts" in the ☰ menu and the side menu.

``shortcut_sections(hebrew)`` is the list, in sections by where a key works.
The next / previous arrows are mirrored in Hebrew (owner, 2026-10-30): there
Ctrl+Left goes forward. ``key_label`` shows a key in the interface's language. ``tests/test_quick_open.py`` checks the list against
the keys the program binds, both ways, so the card names no key that does
nothing and leaves none out. Ctrl++ is left out: it is Ctrl+= by another
name, the same key on most keyboards.
"""
from __future__ import annotations

import re

from PyQt6.QtGui import QFont
from PyQt6.QtWidgets import (
    QDialog, QDialogButtonBox, QFormLayout, QLabel, QScrollArea, QVBoxLayout, QWidget,
)

from desktop.widgets.side_nav import ltr
from genizah_core import tr


def arrow_keys(hebrew: bool) -> tuple[str, str]:
    """The (next, previous) arrow key names: Right and Left, mirrored in Hebrew."""
    return ("Left", "Right") if hebrew else ("Right", "Left")


# Keys with a name in words get their Hebrew name in the Hebrew interface (owner,
# 2026-10-30); Ctrl, Shift, Enter, Esc, PgUp and PgDown stay as printed on the keys.
HEBREW_KEY_NAMES = {"Left": "חץ שמאלה", "Right": "חץ ימינה", "Up": "חץ למעלה",
                    "Down": "חץ למטה", "Space": "רווח"}
_LRM, _RLM = "‎", "‏"


def key_label(keys: str, hebrew: bool) -> str:
    """``keys`` as shown ("Ctrl+Shift+Left / Ctrl+Shift+Right"). In English, left
    to right as typed. In Hebrew, with the Hebrew key names and read right to
    left like the text beside it -- "Ctrl" first (rightmost), and the first key
    of a pair on the right, as "הבא / הקודם" is -- each key closed by a
    left-to-right mark so a trailing "," "/" or "=" stays with it."""
    if not hebrew:
        return ltr(keys)
    shown = []
    for part in re.split(r"( / | … )", keys):
        if part in (" / ", " … "):
            shown.append(f"{_RLM}{part}{_RLM}")
        else:
            names = part.split("+")
            if part.endswith("++"):                       # Ctrl++: the + key itself
                names = names[:-2] + ["+"]
            shown.append("+".join(HEBREW_KEY_NAMES.get(n, n) for n in names) + _LRM)
    return _RLM + "".join(shown)


# Under the list: in a text box Qt keeps Ctrl+arrow (and Ctrl+Shift+arrow) for
# moving and selecting by word, so there those keys do not page (Codex review).
TEXT_BOX_NOTE = "In a text box, such as Find, Ctrl with the arrows moves the cursor word by word, as usual."


def shortcut_sections(hebrew: bool):
    """[(section title, [(keys as shown, what they do)])]. " / " separates keys
    that do the two halves of a row (or the same thing), " … " spans a run."""
    nxt, prev = arrow_keys(hebrew)
    return [
        ("In the whole window", [
            ("Ctrl+1 … Ctrl+7",
             "Open a section, with the cursor in its results (Search, Composition Search) or its search box"),
            ("Ctrl+F / Ctrl+E", "Put the cursor in the search box of the section in front"),
            ("Ctrl+R", "Put the cursor in the results list of the section in front"),
            ("Down", "In the search box: the search history (Enter runs a search, Delete removes it)"),
            ("Esc", "Stop a running search (Search, Composition Search)"),
            ("Ctrl+G / Ctrl+O", "Go to: open a manuscript by its shelfmark or system number"),
            ("Ctrl+8", "Fragment Puzzle"),
            ("Ctrl+9", "Joins Lab"),
            ("Ctrl+= / Ctrl+- / Ctrl+0", "Larger, smaller or normal text"),
            ("F1", "Help, at the section in front"),
            ("Ctrl+,", "Settings"),
            ("Ctrl+/", "This list"),
        ]),
        ("In a list of results", [
            ("Space", "Tick or untick the result (Search)"),
            ("Ctrl+A", "Tick every result, or untick them all (Search)"),
            ("Enter", "Open the result (Search, Composition Search, Personal Lists)"),
            ("Ctrl+Enter / Ctrl+B", "Open it in Browse by Shelfmark"),
        ]),
        ("Composition Search", [
            ("Ctrl+Enter", "In the source text: run the search"),
        ]),
        ("Browse by Shelfmark", [
            ("Ctrl+PgDown / Ctrl+PgUp", "Next / previous image"),
            (f"Ctrl+Shift+{nxt} / Ctrl+Shift+{prev}", "Next / previous manuscript"),
        ]),
        ("Manuscript Viewer", [
            (f"Ctrl+{nxt} / Ctrl+{prev}", "Next / previous result"),
            (f"Ctrl+Shift+{nxt} / Ctrl+Shift+{prev}", "Next / previous manuscript"),
            ("Ctrl+PgDown / Ctrl+PgUp", "Next / previous image"),
            ("Ctrl+B", "Open it in Browse by Shelfmark"),
            ("Ctrl+F", "Find in the text"),
            ("Ctrl+= / Ctrl+- / Ctrl+0", "Larger, smaller or normal text"),
            ("Esc", "Close the window"),
        ]),
    ]


class ShortcutsDialog(QDialog):
    """The keys on the left (on the right in Hebrew: QFormLayout mirrors), what
    they do beside them, under a heading for each place they work."""

    def __init__(self, parent=None, hebrew=None):
        super().__init__(parent)
        if hebrew is None:
            import genizah_core
            hebrew = genizah_core.CURRENT_LANG == 'he'
        self.setWindowTitle(tr("Keyboard shortcuts"))
        layout = QVBoxLayout(self)
        body = QWidget()
        form = QFormLayout(body)
        form.setHorizontalSpacing(18)
        form.setVerticalSpacing(6)
        for title, rows in shortcut_sections(hebrew):
            heading = QLabel(tr(title))
            font = heading.font()
            font.setBold(True)
            font.setPointSizeF(font.pointSizeF() * 1.1)
            heading.setFont(font)
            form.addRow(heading)
            for keys, what in rows:
                keys_shown = QLabel(key_label(keys, hebrew))
                key_font = QFont(keys_shown.font())
                key_font.setBold(True)
                keys_shown.setFont(key_font)
                # One line each: a wrapped QLabel in a form row clipped its last
                # letter in Hebrew ("מספר מדף א").
                form.addRow(keys_shown, QLabel(tr(what)))
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QScrollArea.Shape.NoFrame)
        scroll.setWidget(body)
        layout.addWidget(scroll)
        note = QLabel(tr(TEXT_BOX_NOTE))
        layout.addWidget(note)
        buttons = QDialogButtonBox(QDialogButtonBox.StandardButton.Close)
        buttons.button(QDialogButtonBox.StandardButton.Close).setText(tr("Close"))
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)
        # As tall as the list, within the screen.
        hint = body.sizeHint()
        screen = self.screen().availableGeometry() if self.screen() else None
        height = hint.height() + note.sizeHint().height() + 130   # margins, Close row: no scroll bar
        if screen is not None:
            height = min(height, int(screen.height() * 0.85))
        self.resize(hint.width() + 60, height)
