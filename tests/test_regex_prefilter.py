# -*- coding: utf-8 -*-
"""Tripwires for shared/regex_prefilter.py's assumptions about the index.

The prefilter's proof that Regex mode's candidate query cannot lose a match rests
on how the ``content`` field is tokenized (hebword). These tests fail when that
changes, so the prefilter is reviewed with it. The behaviour itself is tested
through execute_search in tests/test_regex_mode_lossless.py (and, for patterns
the two regex engines read differently, tests/test_regex_engine_divergence.py);
the last section here pins which of those patterns get no prefilter.
"""
import re

import pytest

from shared import regex_prefilter
from shared.indexer import build_main_schema
from shared.search_regex import compile as compile_search_regex
from shared.search_tokenizer import HEBWORD_TOKENIZER_PATTERN

from test_regex_mode_lossless import _index


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


# ---------------------------------------------------------------- engine divergence
# The matcher is the regex module; the formula comes from stdlib re's parse. regex
# reads '[:alpha:]' anywhere inside a class as a POSIX class, re reads its '[' as a
# literal and ends the class at the POSIX class's ']' -- without the nested-set
# warning, which re gives only for '[[' at a class's start. Such a pattern must get
# no prefilter.
POSIX_IN_A_SET = ["ש[a[:alpha:]]ום", "ש[^[:digit:]]ום"]


@pytest.fixture(scope="module")
def index():
    return _index({"shalom": "ברכה שלום עליכם", "other": "טקסט אחר לגמרי"},
                  build_main_schema())


@pytest.mark.parametrize("pattern", POSIX_IN_A_SET)
@pytest.mark.parametrize("stripped", [True, False])
def test_a_posix_class_inside_a_set_gets_no_prefilter(index, pattern, stripped):
    # The matcher finds שלום with it ...
    assert compile_search_regex(pattern, re.IGNORECASE).search("שלום")
    # ... so every document must be a candidate.
    assert regex_prefilter.extract_formula(pattern, stripped=stripped) is regex_prefilter.TRUE
    query, formula = regex_prefilter.build_candidate_query(pattern, index, stripped=stripped)
    assert query is None and formula is regex_prefilter.TRUE


def test_an_ordinary_class_still_gets_a_prefilter(index):
    query, formula = regex_prefilter.build_candidate_query("ש[לב]ום", index, stripped=False)
    assert formula is not regex_prefilter.TRUE and query is not None
    assert index.searcher().search(query, 10).count == 1


@pytest.mark.parametrize("pattern, diverges", [
    ("ש[a[:alpha:]]ום", True),
    ("[[:alpha:]]", True),
    ("x[a[b]y", True),            # the engines agree here; conservative is fine
    ("ש[a--b]ום", True), ("[a&&b]", True), ("[a~~b]", True), ("[a||b]", True),
    ("ש[לב]ום", False),
    (r"[\[\]]ש", False),          # escaped brackets are literals in both engines
    (r"\[ש[לב]", False),          # an escaped '[' opens no class
    ("[]a[]", True),              # ']' first is a literal: the class goes on to '['
    ("[]a]ש[ל]", False),
    ("[^]a]ש", False),
    ("[a-]ש|ש[-a]", False),
    ("ש(א|ב)ום", False),
])
def test_set_syntax_divergence_is_read_from_the_source(pattern, diverges):
    assert regex_prefilter._set_syntax_diverges(pattern) is diverges
