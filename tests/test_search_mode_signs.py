# -*- coding: utf-8 -*-
"""The search-mode signs, one table for every box (2026-11-18).

``shared/search_mode_signs.py`` holds the signs (``$ שלום`` is a Title search). The engine's
own prefix parsing (``SearchEngine.parse_query_syntax``), the /search box's live switch
(``web/pages/search.py``) and the go-to boxes (``web/home_query.py``) read it; the boxes'
wiring is pinned in ``tests/render_smoke/test_go_to_box_mode_signs_render_smoke.py``.
"""
from __future__ import annotations

import pytest

from shared.search_mode_signs import CATALOG_MODE, MODE_SIGNS, split_mode_sign

WORD = 'שלום'

# The signs the /search box has offered since before the table moved here, and their modes.
EXPECTED = {
    '???': 'variants_maximum', '??': 'variants_extended', '?': 'variants', '=': 'exact',
    '~': 'fuzzy', '/': 'Regex', '$': 'Title', '#': 'Shelfmark', '%': 'Catalog', 'R': 'responsa',
}


def test_the_table_holds_every_sign_once():
    assert dict(MODE_SIGNS) == EXPECTED
    assert len(MODE_SIGNS) == len(EXPECTED)
    assert CATALOG_MODE == 'Catalog'


def test_a_sign_that_begins_another_comes_after_it():
    """``??`` must be tried before ``?``: the first sign that matches wins."""
    signs = [sign for sign, _ in MODE_SIGNS]
    for i, sign in enumerate(signs):
        for later in signs[i + 1:]:
            assert not later.startswith(sign), (sign, later)


@pytest.mark.parametrize('sign, mode', sorted(EXPECTED.items()))
def test_a_sign_and_a_space_choose_the_mode(sign, mode):
    assert split_mode_sign(f'{sign} {WORD}', catalog_served=True) == (mode, WORD)


def test_the_query_keeps_its_own_words_and_loses_only_the_leading_spaces():
    assert split_mode_sign('$   בית  חיים ', catalog_served=False) == ('Title', 'בית  חיים ')
    assert split_mode_sign('# T-S 12.123', catalog_served=False) == ('Shelfmark', 'T-S 12.123')


@pytest.mark.parametrize('text', ['$ ', '?? ', 'R '])
def test_the_sign_alone_chooses_the_mode_with_an_empty_query(text):
    mode, query = split_mode_sign(text, catalog_served=False)
    assert mode == EXPECTED[text.strip()] and query == ''


@pytest.mark.parametrize('text', [f'${WORD}', f'R{WORD}', 'Rashi', '$', '', None, WORD, ' $ x', 'r x'])
def test_without_a_sign_and_a_space_nothing_is_split(text):
    assert split_mode_sign(text, catalog_served=True) == (None, text or '')


def test_percent_is_a_sign_only_where_the_catalogue_search_is_served():
    assert split_mode_sign(f'% {WORD}', catalog_served=True) == ('Catalog', WORD)
    assert split_mode_sign(f'% {WORD}', catalog_served=False) == (None, f'% {WORD}')


def _engine():
    from shared.search_engine import SearchEngine
    return SearchEngine.__new__(SearchEngine)


@pytest.mark.parametrize('sign, mode', sorted(EXPECTED.items()))
def test_the_engine_parses_every_sign_in_the_table(sign, mode):
    """The engine reads every sign with a space after it, and a symbol sign without one.
    The letter sign R without a space is not pinned here: ``Rashi`` is a word, and
    PR #53 decides that case in the engine and tests it there."""
    engine = _engine()
    if not sign.isalpha():
        assert engine.parse_query_syntax(f'{sign}{WORD}') == (mode, WORD)
    assert engine.parse_query_syntax(f'{sign} {WORD}') == (mode, WORD)


def test_the_engine_reads_this_table_not_a_copy(monkeypatch):
    """A sign added to the table is one the engine knows: there is no second list."""
    import shared.search_engine as search_engine
    monkeypatch.setattr(search_engine, 'MODE_SIGNS', (('!', 'Title'),) + MODE_SIGNS)
    assert _engine().parse_query_syntax(f'!{WORD}') == ('Title', WORD)


def test_in_responsa_mode_the_engine_reads_no_sign():
    assert _engine().parse_query_syntax(f'# {WORD}', responsa_mode=True) == (None, f'# {WORD}')
