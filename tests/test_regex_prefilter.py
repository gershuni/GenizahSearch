# -*- coding: utf-8 -*-
"""Tripwires for shared/regex_prefilter.py's assumptions about the index.

The prefilter's proof that Regex mode's candidate query cannot lose a match rests
on how the ``content`` field is tokenized (hebword). These tests fail when that
changes, so the prefilter is reviewed with it. The behaviour itself is tested
through execute_search in tests/test_regex_mode_lossless.py.
"""
import re

from shared import regex_prefilter
from shared.search_tokenizer import HEBWORD_TOKENIZER_PATTERN


def test_hebword_pattern_is_the_one_the_prefilter_was_proven_on():
    # Changing this pattern needs a re-index AND a review of shared/regex_prefilter.py.
    assert HEBWORD_TOKENIZER_PATTERN == "[\\w\\u0590-\\u05FF\\u0300-\\u036F'\"\\[\\]]+"


def test_hebrew_block_and_brackets_are_token_characters():
    chars = [chr(c) for c in range(0x0590, 0x0600)] + ['[', ']']
    assert all(regex_prefilter.is_token_char(c) for c in chars)


def test_a_hebrew_letter_matches_only_itself_ignoring_case():
    for code in range(0x05D0, 0x05EB):   # א..ת
        letter = chr(code)
        assert regex_prefilter.matched_chars(re.escape(letter)) == frozenset({letter})


def test_separators_are_not_token_characters():
    assert not any(regex_prefilter.is_token_char(c) for c in ' \t\n,.;:|')
