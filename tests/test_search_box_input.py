# -*- coding: utf-8 -*-
"""What the /search box was given (web/search_box_input.py).

The shelfmarks are real zero-result text searches from PostHog ``search_executed``
(2026-11-03). The page's wiring -- Shelfmark mode, the notice, the keyboard-layout
suggestion -- is tests/render_smoke/test_search_box_input_render_smoke.py.
"""
from __future__ import annotations

import pytest

from web.home_query import home_query_target
from web.search_box_input import TEXT_MODES, split_placeholder, typed_as_shelfmark
from web.search_bootstrap import VALID_SEARCH_MODES

SHELFMARKS = ['ts 20.57', 't-s ns 307.61b', 'add 3356', 'ts k2.27', 't-s 10g4.5', 'tad c2.1', '5559a.10']


@pytest.mark.parametrize('typed', SHELFMARKS + [
    'T-S 12.123',
    'MS Heb c 57',
    'Moss. IV,12.2',
    'ENA 2156.1',
    '  ts 20.57  ',
    '990053835020205171',      # a corpus sys_id (libraries.csv row 1)
    '12345',                   # a number: the Shelfmark lookup decides what it matches
])
def test_a_shelfmark_or_a_system_number_is_recognised(typed):
    assert typed_as_shelfmark(typed)


@pytest.mark.parametrize('typed', [
    'שבת',
    'ויאמר משה',
    'T-S 12.123 שבת',          # a Hebrew letter anywhere means text, as on the homepage
    'قال',
    'dcuk',                    # גבול on the English layout: a word, not a shelfmark
    'fufch anh',
    'marriage',                # an English word
    'ts',                      # a library alone, no number
    '٩٩٠٠٥٣٨٣٥٠٢٠٢٠٥١٧١',       # Arabic-Indic digits: not ASCII
    'ts 20.57?',               # a mark no shelfmark is written with
    '',
    '   ',
    None,
])
def test_words_and_hebrew_are_left_as_text(typed):
    assert not typed_as_shelfmark(typed)


@pytest.mark.parametrize('typed', SHELFMARKS)
def test_the_homepage_box_sends_the_same_strings_to_browse_as_shelfmarks(typed):
    """The same recogniser: what the homepage opens as a shelfmark, the search box
    runs in Shelfmark mode."""
    assert home_query_target(typed).startswith('/browse?shelfmark=')


def test_text_modes_are_the_page_modes_that_search_for_words():
    assert TEXT_MODES <= VALID_SEARCH_MODES
    assert 'exact' in TEXT_MODES and 'fuzzy' in TEXT_MODES
    for mode in ('Regex', 'responsa', 'Title', 'Shelfmark', 'pgp_tags'):
        assert mode not in TEXT_MODES, mode


def test_split_placeholder_gives_the_two_halves_around_the_query():
    assert split_placeholder('Did you mean {query}?') == ('Did you mean ', '?')
    assert split_placeholder('האם התכוונתם ל{query}?') == ('האם התכוונתם ל', '?')
    assert split_placeholder('{query}?') == ('', '?')
    assert split_placeholder('No placeholder here') == ('No placeholder here', '')
