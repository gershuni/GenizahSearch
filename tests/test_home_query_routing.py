# -*- coding: utf-8 -*-
"""Routing of the homepage search box (owner, 2026-10-19).

``web/home_query.py::home_query_target`` decides where ``/`` sends what the
visitor typed: a corpus sys_id opens that manuscript, Hebrew or Arabic text is
searched, and anything else is a shelfmark for ``/browse`` to look up. The
wiring on the page itself is ``tests/render_smoke/test_home_search_routing_render_smoke.py``.
"""
from __future__ import annotations

from urllib.parse import parse_qs, urlsplit

import pytest

from web.home_query import has_hebrew_or_arabic_letter, home_query_target


def _route(text):
    """(path, the single query value) for where ``text`` is sent."""
    target = home_query_target(text)
    parts = urlsplit(target)
    params = parse_qs(parts.query)
    assert len(params) == 1, target
    (key, values), = params.items()
    return parts.path, key, values[0]


@pytest.mark.parametrize('text', [None, '', '   ', '\t\n'])
def test_a_blank_box_goes_nowhere(text):
    assert home_query_target(text) is None


@pytest.mark.parametrize('sys_id', [
    '990053835020205171',            # NLI Alma id, libraries.csv row 1
    '  990053835020205171  ',        # pasted with spaces
    '990000012345000000',            # Phase-85 synthetic (99 + InventoryId + 000000)
])
def test_a_corpus_sys_id_opens_that_manuscript(sys_id):
    assert _route(sys_id) == ('/browse', 'sys_id', sys_id.strip())


@pytest.mark.parametrize('text', [
    '970993169503583183',   # LOCAL "My Library" namespace -- never a corpus record
    '123456789099',         # ends with 99 but is not in the 99 namespace
    '99',                   # too short to be a sys_id
    '٩٩٠٠٥٣٨٣٥٠٢٠٢٠٥١٧١',   # Arabic-Indic digits: `\d` matches them, a sys_id never has them
])
def test_other_numbers_are_looked_up_as_shelfmarks(text):
    assert _route(text) == ('/browse', 'shelfmark', text)


@pytest.mark.parametrize('text', [
    'שבת',
    'ויאמר משה',
    'בְּרֵאשִׁית',                 # pointed
    'قال',                      # Arabic script
    'T-S 12.123 שבת',           # a Hebrew letter anywhere wins
    'ש*',
])
def test_hebrew_or_arabic_text_is_searched(text):
    assert _route(text) == ('/search', 'q', text)


@pytest.mark.parametrize('shelfmark', [
    'T-S 8J6.1',
    'MS Heb c 57',
    'ENA 2591.1',
    'Moss. III,27O',
    'T-S AS 74.1+2',            # '+' would read as a space if not encoded
    'T-S 10J9 #5',              # '#' would start a URL fragment if not encoded
    'A&B 1',                    # '&' would split the query string if not encoded
])
def test_anything_else_is_a_shelfmark_and_survives_the_url(shelfmark):
    assert _route(shelfmark) == ('/browse', 'shelfmark', shelfmark)


def test_a_search_query_survives_the_url():
    assert _route('משה + אהרן & #') == ('/search', 'q', 'משה + אהרן & #')


@pytest.mark.parametrize('text, expected', [
    ('א', True),
    ('ﬠ', True),                # Hebrew presentation form (FB20)
    ('ب', True),
    ('ﺏ', True),                # Arabic presentation form (FE8F)
    ('ָּ', False),    # niqqud alone is not a letter
    ('١٢٣', False),             # Arabic-Indic digits are not letters
    ('T-S 8J6.1', False),
    ('Ελληνικά', False),
])
def test_only_hebrew_and_arabic_letters_count(text, expected):
    assert has_hebrew_or_arabic_letter(text) is expected
