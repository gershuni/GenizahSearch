# -*- coding: utf-8 -*-
"""Hebrew typed on the English keyboard layout (shared/keyboard_layout.py).

The examples are real zero-result queries from PostHog ``search_executed``
(2026-11-03): readers typing Hebrew with the keyboard still on English.
"""
from __future__ import annotations

import pytest

from shared.keyboard_layout import (
    ENGLISH_TO_HEBREW, FINAL_LETTERS, HEBREW_LETTERS, is_latin_query, layout_suggestion, map_keys_to_hebrew,
)


@pytest.mark.parametrize('typed, hebrew', [
    ('dcuk', 'גבול'),
    ('fufch anh', 'כוכבי שמי'),
    ('bghkv', 'נעילה'),
    (',jkhe ku kaui', 'תחליק לו לשון'),
    ('t, gprv', 'את עפרה'),
])
def test_the_posthog_examples_read_as_the_hebrew_the_reader_meant(typed, hebrew):
    assert layout_suggestion(typed) == hebrew


def test_every_hebrew_letter_is_on_exactly_one_key():
    letters = [v for v in ENGLISH_TO_HEBREW.values() if v in HEBREW_LETTERS]
    assert sorted(letters) == sorted(HEBREW_LETTERS), 'the layout must give every letter once, finals included'
    assert len(set(letters)) == len(letters)
    assert FINAL_LETTERS <= HEBREW_LETTERS


def test_the_three_punctuation_keys_map_but_make_no_suggestion():
    assert map_keys_to_hebrew('q') == '/'
    assert map_keys_to_hebrew('w') == "'"
    assert map_keys_to_hebrew('/') == '.'
    # A word with one of them is not Hebrew letters alone: no suggestion.
    assert layout_suggestion('dcwk') is None
    assert layout_suggestion('abc/') is None


@pytest.mark.parametrize('typed', [
    'Dcuk',            # a phone capitalises the first letter
    'DCUK',
])
def test_case_is_ignored(typed):
    assert layout_suggestion(typed) == 'גבול'


@pytest.mark.parametrize('typed', [
    'שבת',             # already Hebrew
    'ts 20.57',        # a shelfmark: digits
    'dcuk2',           # a digit anywhere
    'abc-def',         # a hyphen has no key on the layout
    'dc?uk',           # a shifted mark
    'a',               # one letter is not a word anyone searched for
    '',
    '   ',
    None,
])
def test_what_is_not_a_wrong_layout_query_gets_no_suggestion(typed):
    assert layout_suggestion(typed) is None


def test_a_final_letter_inside_a_word_is_not_hebrew_anyone_typed():
    # l is ך: fine at the end of a word, never inside one.
    assert layout_suggestion('dcul') == 'גבוך'
    assert layout_suggestion('dluk') is None
    # i is ן, o is ם: "fio" is ignored, "fi" is accepted.
    assert layout_suggestion('fio') is None
    assert layout_suggestion('fi') == 'כן'


def test_spacing_is_normalised_and_the_words_kept_apart():
    assert layout_suggestion('  fufch   anh  ') == 'כוכבי שמי'
    assert layout_suggestion('fufch\tanh') == 'כוכבי שמי'


def test_is_latin_query_needs_a_latin_letter():
    assert is_latin_query('dcuk')
    assert is_latin_query('t, gprv')
    assert not is_latin_query(',.,')
    assert not is_latin_query('שבת')
    assert not is_latin_query('')
