# -*- coding: utf-8 -*-
"""The desktop discoveries list never names the author of an anonymous discovery.

The client fills ``author_username`` from ``profiles`` whether or not a discovery
is anonymous, and a new account's username is its email address. The list's cache
kept the username and dropped ``is_anonymous``, so the list printed "by <name>"
for an anonymous discovery (the detail dialogs already hid it).

Runs the real GenizahGUI methods on a stub window.
"""
from __future__ import annotations

from types import SimpleNamespace
from unittest import mock

from PyQt6.QtWidgets import QApplication

import genizah_app
from desktop.supabase_corrections_client import Discovery

_APP = QApplication.instance() or QApplication([])   # kept for the process


def _discovery(**kw):
    base = dict(id=7, title='A find', content='', author_username='someone@example.org')
    base.update(kw)
    return Discovery(**base)


def _texts(list_widget):
    return [list_widget.addItem.call_args_list[i].args[0].text()
            for i in range(list_widget.addItem.call_count)]


def _refresh(discoveries):
    """_refresh_discoveries_panel on a stub; returns what it cached."""
    cached = {}
    client = mock.Mock()
    client.get_cached_data.return_value = None
    client.is_logged_in.return_value = False
    client.get_discoveries.return_value = (discoveries, len(discoveries))
    client.set_cached_data.side_effect = lambda key, data: cached.__setitem__(key, data)
    window = SimpleNamespace(corrections_client=client, discoveries_list=mock.Mock(),
                             _filter_discoveries=lambda: None)
    genizah_app.GenizahGUI._refresh_discoveries_panel(window, use_cache_first=False)
    return cached['discoveries']


def test_an_anonymous_discoverys_username_is_not_cached():
    rows = _refresh([_discovery(is_anonymous=True),
                     _discovery(id=8, author_username='named', is_anonymous=False)])
    anon, named = rows
    assert anon['is_anonymous'] is True
    assert anon['author_username'] is None
    assert named['is_anonymous'] is False
    assert named['author_username'] == 'named'


def test_the_list_says_anonymous_for_an_anonymous_discovery():
    window = SimpleNamespace(discoveries_list=mock.Mock())
    rows = [
        {'id': 1, 'title': 'Hidden name', 'author_username': None, 'is_anonymous': True},
        {'id': 2, 'title': 'Named', 'author_username': 'named', 'is_anonymous': False},
    ]
    genizah_app.GenizahGUI._populate_discoveries_list(window, rows)
    texts = _texts(window.discoveries_list)
    assert texts[0].endswith('Anonymous'), texts[0]
    assert texts[1].endswith('named'), texts[1]


def test_a_row_flagged_anonymous_hides_a_username_it_still_carries():
    """The flag alone decides, whatever else the row holds."""
    window = SimpleNamespace(discoveries_list=mock.Mock())
    rows = [{'id': 1, 'title': 'T', 'author_username': 'someone@example.org', 'is_anonymous': True}]
    genizah_app.GenizahGUI._populate_discoveries_list(window, rows)
    [text] = _texts(window.discoveries_list)
    assert 'someone@example.org' not in text
    assert text.endswith('Anonymous'), text
