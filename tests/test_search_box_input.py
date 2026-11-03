# -*- coding: utf-8 -*-
"""What the /search box was given (web/search_box_input.py).

The shelfmarks are real zero-result text searches from PostHog ``search_executed``
(2026-11-03). The page's wiring -- Browse for a system number, Shelfmark mode and
the notice for a shelfmark, the keyboard-layout suggestion -- is
tests/render_smoke/test_search_box_input_render_smoke.py.
"""
from __future__ import annotations

import pytest

from web.home_query import home_query_target
from web.search_box_input import (
    COLLECTION_PREFIXES, SHELFMARK_WORDS, TEXT_MODES, search_box_input_kind, split_placeholder, typed_as_shelfmark,
)
from web.search_bootstrap import VALID_SEARCH_MODES

SHELFMARKS = ['ts 20.57', 't-s ns 307.61b', 'add 3356', 'ts k2.27', 't-s 10g4.5', 'tad c2.1', '5559a.10']


@pytest.mark.parametrize('typed', SHELFMARKS + [
    'T-S 12.123',
    'MS Heb c 57',             # a letter-digit run is not needed: "Heb" is a collection prefix
    'Moss. IV,12.2',
    'ENA 2156.1',
    'Or 5557B',
    'Halper 331',
    'Gaster 1358',
    'Add. 3356',
    '  ts 20.57  ',
    'Cambridge T-S 12.123',    # a library name first (_strip_library_prefix)
    'JTS ENA 2156',
    'British Library Or 5557',
    'Cambridge University Library, Ms. T-S K 08.026',   # the NLI catalogue's form
    '20.57',                   # a dotted number alone
    'k2.27',
    '12b',
    'Mosseri VIII.402',        # a Roman numeral is no word
    'T-S Misc. 36.12',
    'T-S Ar. 18(1).6',
    'Westminster College Arabica I.12',
    'Evr. II A 12',
    'Gen 1.1',                 # three letters: a collection prefix or a short code, never a word
])
def test_a_shelfmark_is_recognised(typed):
    assert search_box_input_kind(typed) == 'shelfmark', typed
    assert typed_as_shelfmark(typed)


@pytest.mark.parametrize('sys_id', [
    '990053835020205171',      # libraries.csv row 1
    '  990053835020205171  ',
    '990000012345000000',      # Phase-85 synthetic
])
def test_a_corpus_system_number_is_a_sys_id_not_a_shelfmark(sys_id):
    assert search_box_input_kind(sys_id) == 'sys_id'
    assert not typed_as_shelfmark(sys_id)


@pytest.mark.parametrize('typed', [
    'שבת',
    'ויאמר משה',
    'T-S 12.123 שבת',          # a Hebrew letter anywhere means text, as on the homepage
    'قال',
    'dcuk',                    # גבול on the English layout: a word, not a shelfmark
    'fufch anh',
    'marriage',                # an English word
    'Psalm 23',                # a word and a number: no shelfmark shape (Codex on #469)
    'chapter 2',
    'version 1',
    'version 1.2',             # a word of four letters or more is a word, whatever follows it (round 2)
    'Psalm 23.1',
    'COVID19',
    'test123',
    'Cambridge marriage 1100',  # a library first does not make the words after it a shelfmark
    'ts',                      # a collection prefix alone, no number
    '12',                      # a bare number: 15,533 catalogue rows, not what anyone meant
    '3356',
    '970993169503583183',      # the LOCAL namespace is no corpus sys_id; nor a shelfmark shape
    '٩٩٠٠٥٣٨٣٥٠٢٠٢٠٥١٧١',       # Arabic-Indic digits: not ASCII
    'ts 20.57?',               # a mark no shelfmark is written with
    '',
    '   ',
    None,
])
def test_words_hebrew_and_bare_numbers_are_left_as_text(typed):
    assert search_box_input_kind(typed) == 'text', typed
    assert not typed_as_shelfmark(typed)


@pytest.mark.parametrize('typed', SHELFMARKS)
def test_the_homepage_box_sends_the_same_strings_to_browse_as_shelfmarks(typed):
    """The same recogniser: what the homepage opens as a shelfmark, the search box
    runs in Shelfmark mode."""
    assert home_query_target(typed).startswith('/browse?shelfmark=')


def test_the_homepage_box_opens_the_same_system_numbers():
    assert home_query_target('990053835020205171') == '/browse?sys_id=990053835020205171'


def test_collection_prefixes_are_lower_case_without_dots():
    for prefix in COLLECTION_PREFIXES | SHELFMARK_WORDS:
        assert prefix == prefix.lower() and '.' not in prefix and ' ' not in prefix, prefix
    assert {'t-s', 'ts', 'ena', 'add', 'or', 'moss', 'heb', 'evr', 'gaster', 'adler'} <= COLLECTION_PREFIXES
    assert COLLECTION_PREFIXES <= SHELFMARK_WORDS


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
