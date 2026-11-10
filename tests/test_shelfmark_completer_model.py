"""Pin what the desktop shelfmark autocomplete shows (catalogue-shelfmark plan, gate 1).

The catalogue-form lookup (shared/catalog_shelfmark.py) must not change the
completer. This file was written and green on unchanged code BEFORE that work
started, so any later change to the rows, their keys, the part rows or the
completer's matching settings fails here.

It calls the real ``GenizahGUI.setup_shelfmark_completer`` on a ``QWidget``
carrying a stub ``meta_mgr`` (a plain object cannot parent a ``QCompleter``).
Expected values are literal, not ``ShelfmarkCompleter.normalize(text)``: the
Oxford part rows carry keys from ``shared/codicological.py``'s own normalizer.
"""
import types

import pytest

pytestmark = pytest.mark.gui

from PyQt6.QtCore import Qt  # noqa: E402
from PyQt6.QtWidgets import QCompleter, QLineEdit, QWidget  # noqa: E402

import genizah_app  # noqa: E402

UR = Qt.ItemDataRole.UserRole


def _host():
    csv_bank = {
        '990000000000000001': {'shelfmark': 'T-S 12.123'},
        '990000000000000002': {'shelfmark': 'T-S 12.123'},   # duplicate shelfmark -> one row
        '990000000000000003': {'shelfmark': 'MS. Heb. a.1'},
        '990000000000000004': {'shelfmark': 'Or. 5557B'},
        '990000000000000005': {'shelfmark': ''},             # empty -> no row
    }
    parts = [{'display': 'heb. d. 29 part 2', 'normalized': 'hebd29part2',
              'part_id': 'MS. Heb. d. 29/2'}]
    meta = types.SimpleNamespace(csv_bank=csv_bank, get_part_autocomplete_list=lambda: parts)
    host = QWidget()
    host.meta_mgr = meta
    host.browse_shelf_input = QLineEdit(host)
    return host


def test_completer_rows_keys_and_parts_are_pinned():
    host = _host()
    assert genizah_app.GenizahGUI.setup_shelfmark_completer(host) is True
    model = host.shelf_model
    rows = [(model.item(i).text(), model.item(i).data(UR), model.item(i).data(UR + 1))
            for i in range(model.rowCount())]
    assert rows == [
        ('MS. Heb. a.1', 'heb.a.1', None),
        ('Or. 5557B', 'or.5557b', None),
        ('T-S 12.123', 'ts12.123', None),
        ('heb. d. 29 part 2', 'hebd29part2', 'MS. Heb. d. 29/2'),
    ]
    assert host.valid_shelf_keys == {'heb.a.1', 'or.5557b', 'ts12.123', 'hebd29part2'}


def test_completer_matching_settings_are_pinned():
    host = _host()
    genizah_app.GenizahGUI.setup_shelfmark_completer(host)
    c = host.shelf_completer
    assert c.completionRole() == UR
    assert c.caseSensitivity() == Qt.CaseSensitivity.CaseInsensitive
    assert c.completionMode() == QCompleter.CompletionMode.PopupCompletion
    assert c.filterMode() == Qt.MatchFlag.MatchStartsWith
    assert host.browse_shelf_input.completer() is c
    assert c.valid_keys is host.valid_shelf_keys


def test_completer_typing_and_selection_give_the_display_text():
    host = _host()
    genizah_app.GenizahGUI.setup_shelfmark_completer(host)
    c = host.shelf_completer
    # What the user types is normalized before matching.
    assert c.splitPath('T-S 12') == ['ts12']
    assert c.splitPath('MS. Heb. a') == ['heb.a']
    c.setCompletionPrefix(c.splitPath('t-s 12.1')[0])
    assert c.completionCount() == 1
    assert c.currentCompletion() == 'T-S 12.123'   # pathFromIndex -> display text
    # Choosing a row puts the pretty display text, not the key, in the box.
    model = host.shelf_model
    shown = [c.pathFromIndex(model.index(i, 0)) for i in range(model.rowCount())]
    assert shown == ['MS. Heb. a.1', 'Or. 5557B', 'T-S 12.123', 'heb. d. 29 part 2']
