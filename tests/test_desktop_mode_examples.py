# -*- coding: utf-8 -*-
"""The desktop's query box follows the mode: an example inside, the lead atop its tooltip.

desktop/catalog_search.py::show_mode_examples on a real QComboBox and QLineEdit, as
GenizahGUI.create_search_tab connects them (owner, 2026-11-18); the table itself is
tests/test_search_mode_examples.py."""
from __future__ import annotations

import inspect
import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest  # noqa: E402

pytest.importorskip("PyQt6")

from PyQt6.QtWidgets import QApplication, QComboBox, QLineEdit  # noqa: E402

import desktop.catalog_search as cs  # noqa: E402
from shared.genizah_translations import TRANSLATIONS  # noqa: E402
from shared.search_mode_examples import LRI, MODE_EXAMPLES, PDI, RLI, mode_lead  # noqa: E402

pytestmark = pytest.mark.gui

_APP = QApplication.instance() or QApplication([])
SHORTCUTS = "Search Shortcuts:\n= = Exact match"


def _he(text):
    return TRANSLATIONS.get(text, text)


def _en(text):
    return text


def _widgets(index=0):
    combo, box = QComboBox(), QLineEdit()
    combo.addItems([str(i) for i in range(9)])   # the combo's rows, the catalogue's included
    combo.setCurrentIndex(index)
    box.setToolTip(SHORTCUTS)
    return combo, box


def _example_of(box):
    text = box.placeholderText()
    for mark in (LRI, RLI, PDI):
        text = text.replace(mark, '')
    return text.split(': ', 1)[1]


def _assert_shows(box, mode, translate):
    assert _example_of(box) in MODE_EXAMPLES['exact' if mode == 'literal' else mode][1], mode
    assert box.toolTip() == mode_lead(mode, translate) + "\n\n" + SHORTCUTS, mode


def test_the_box_follows_the_combo():
    combo, box = _widgets()
    cs.show_mode_examples(combo, box, _he)
    _assert_shows(box, 'exact', _he)                       # before any change
    assert box.placeholderText().startswith('לדוגמה: ')
    for index, mode in [(2, 'responsa'), (4, 'Regex'), (5, 'Title'), (6, 'Shelfmark'),
                        (cs.MODE_INDEX_CATALOG, 'Catalog'), (3, 'fuzzy'), (1, 'variants'),
                        (cs.MODE_INDEX_PGP_TAGS, 'exact'), (0, 'exact')]:
        combo.setCurrentIndex(index)
        _assert_shows(box, mode, _he)


def test_it_starts_from_the_row_shown_and_keeps_the_shortcuts_once():
    combo, box = _widgets(6)
    cs.show_mode_examples(combo, box, _en)
    _assert_shows(box, 'Shelfmark', _en)
    combo.setCurrentIndex(5)
    combo.setCurrentIndex(6)
    assert box.toolTip().count(SHORTCUTS) == 1


class _ReorderedCombo(QComboBox):
    """A combo that shows Catalog (8) right under Title (5) and still answers in mode indices,
    its change signal carrying the row shown -- as a mode list in another order does."""

    _SHOWN = [0, 1, 2, 3, 4, 5, 8, 6, 7]          # row -> mode index

    def currentIndex(self):
        return self._SHOWN[super().currentIndex()]

    def setCurrentIndex(self, index):
        super().setCurrentIndex(self._SHOWN.index(index))


def test_the_mode_is_the_combos_index_not_the_row_its_signal_carries():
    combo, box = _ReorderedCombo(), QLineEdit()
    combo.addItems([str(i) for i in range(9)])
    box.setToolTip(SHORTCUTS)
    cs.show_mode_examples(combo, box, _he)
    for index, mode in [(cs.MODE_INDEX_CATALOG, 'Catalog'), (6, 'Shelfmark'), (7, 'exact'), (5, 'Title')]:
        combo.setCurrentIndex(index)          # rows 6, 7, 8, 5: the signal carries those
        _assert_shows(box, mode, _he)


def test_the_search_tab_wires_its_box_and_combo():
    import genizah_app
    src = inspect.getsource(genizah_app.GenizahGUI.create_search_tab)
    assert 'show_mode_examples(self.mode_combo, self.query_input, tr)' in src
    # After the combo has every row (the catalogue's is appended when offered) and after the
    # box's shortcuts tooltip is set (the lead is put above it).
    assert src.index('show_mode_examples(') > src.index('self.mode_combo.addItem(tr("Catalog")')
    assert src.index('show_mode_examples(') > src.index('self.query_input.setToolTip(')
    assert 'Search terms, title or shelfmark' not in src
