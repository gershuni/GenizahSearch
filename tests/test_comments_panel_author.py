# -*- coding: utf-8 -*-
"""The desktop Community "All Comments" list must not call a named comment anonymous.

The desktop reads comments with select('*') and no profiles join, so author_username
is always None there -- and the list printed "by Anonymous" for every comment, while
genizahsearch.com shows the author's name on the same comment. An unknown author is
now left out; a known one is shown; the "My Comments" list shows no author at all.

Drives the real GenizahGUI._populate_comments_list into a real QListWidget.
"""
from types import SimpleNamespace

import pytest

pytestmark = pytest.mark.gui  # imports PyQt6: gui bucket only

from PyQt6.QtWidgets import QApplication, QListWidget  # noqa: E402

import genizah_core  # noqa: E402
import genizah_app  # noqa: E402

_APP = QApplication.instance() or QApplication([])


def _row(author):
    return {'id': 1, 'document_id': '990000000000000001', 'content': 'hi',
            'author_username': author, 'page_number': 3}


def _lines(author, show_author):
    target = QListWidget()
    genizah_app.GenizahGUI._populate_comments_list(
        SimpleNamespace(meta_mgr=None), [_row(author)], target, show_author=show_author)
    assert target.count() == 1
    return [ln.strip() for ln in target.item(0).text().splitlines()]


@pytest.mark.parametrize("lang, anon, by_alice", [
    # lang, the word the list used to print, the author line for "alice"
    ('en', 'Anonymous', 'by alice'),
    ('he', 'אנונימי', 'מאת alice'),
])
def test_comments_list_names_only_a_known_author(monkeypatch, lang, anon, by_alice):
    monkeypatch.setattr(genizah_core, 'CURRENT_LANG', lang)
    by_word = by_alice.split()[0]

    # All Comments, author not loaded: no author line, and never "Anonymous".
    unknown = _lines(None, show_author=True)
    assert not any(anon in ln for ln in unknown), unknown
    assert not any(ln.split()[:1] == [by_word] for ln in unknown), (
        f"an author line with no author: {unknown}")
    assert unknown[-1] == 'hi'

    # All Comments, author known: named.
    assert by_alice in _lines('alice', show_author=True)

    # My Comments: no author line even when the name is known.
    mine = _lines('alice', show_author=False)
    assert not any('alice' in ln for ln in mine), mine
    assert mine[-1] == 'hi'
