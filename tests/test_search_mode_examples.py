# -*- coding: utf-8 -*-
"""What the search box says per mode: a lead and an example drawn from the mode's (owner, 2026-11-18).

shared/search_mode_examples.py holds the table; the website's /search box is pinned on the real
page in tests/render_smoke/test_search_mode_placeholder_render_smoke.py. Here: the table and its
Hebrew; the desktop's wiring is tests/test_desktop_mode_examples.py."""
from __future__ import annotations

import random
import re

import pytest

from shared.genizah_translations import TRANSLATIONS
from shared.search_mode_examples import (EXAMPLE_TEMPLATE, LRI, MODE_EXAMPLES, PDI, RLI, mode_lead,
                                         mode_placeholder)


def _en(text):
    return text


def _he(text):
    return TRANSLATIONS.get(text, text)


def _first(seq):
    return seq[0]


EXPECTED_LEAD_HE = {
    'exact': 'הקלידו מונח לחיפוש',
    'variants': 'הקלידו מונח לחיפוש',
    'variants_extended': 'הקלידו מונח לחיפוש',
    'variants_maximum': 'הקלידו מונח לחיפוש',
    'fuzzy': 'הקלידו מונח לחיפוש',
    'responsa': 'הקלידו מילים עם אופרטורים לחיפוש מתקדם',
    'Regex': 'הקלידו מילים עם אופרטורים לחיפוש מתקדם',
    'Title': 'הקלידו מונח לחיפוש בכותרות',
    'Catalog': 'הקלידו מונח לחיפוש במידע הקטלוגי',
    'Shelfmark': 'הקלידו מחרוזת לחיפוש במספרי המדף',
}


def _visible(text):
    """The text without its direction marks."""
    return text.replace(LRI, '').replace(RLI, '').replace(PDI, '')


def test_every_mode_of_both_apps_has_an_entry():
    assert set(MODE_EXAMPLES) == set(EXPECTED_LEAD_HE)


@pytest.mark.parametrize('mode', sorted(EXPECTED_LEAD_HE))
def test_each_mode_has_the_owners_hebrew_lead(mode):
    assert mode_lead(mode, _he) == EXPECTED_LEAD_HE[mode]


@pytest.mark.parametrize('mode', sorted(EXPECTED_LEAD_HE))
def test_the_placeholder_is_one_of_the_modes_examples_kept_right_to_left(mode):
    examples = MODE_EXAMPLES[mode][1]
    assert examples and all(e.strip() == e and e for e in examples)
    shown = mode_placeholder(mode, _he, choose=_first)
    assert shown == 'לדוגמה: ' + RLI + examples[0] + PDI    # Hebrew needs no isolate of its own
    drawn = {_visible(mode_placeholder(mode, _he)) for _ in range(40 * len(examples))}
    assert drawn == {'לדוגמה: ' + e for e in examples}      # each one is drawn, none else


def test_every_lead_and_the_example_line_are_translated():
    for lead, _examples in MODE_EXAMPLES.values():
        assert lead in TRANSLATIONS, lead
    assert TRANSLATIONS[EXAMPLE_TEMPLATE].count('{query}') == 1 == EXAMPLE_TEMPLATE.count('{query}')


def test_english_is_isolated_left_to_right_and_the_example_right_to_left():
    example = MODE_EXAMPLES['responsa'][1][0]
    assert mode_placeholder('responsa', _en, choose=_first) == (
        LRI + 'For example: ' + RLI + example + PDI + PDI)
    assert mode_lead('Shelfmark', _en) == 'Type a string to search the shelfmarks'


@pytest.mark.parametrize('mode', ['literal', 'pgp_tags', 'no-such-mode'])
def test_the_desktops_exact_and_a_mode_without_a_box_get_the_text_modes(mode):
    assert mode_lead(mode, _he) == mode_lead('exact', _he)
    assert mode_placeholder(mode, _he, choose=_first) == mode_placeholder('exact', _he, choose=_first)


def test_the_examples_are_queries_their_modes_take():
    from shared.responsa import parse_responsa_query
    for example in MODE_EXAMPLES['responsa'][1]:
        assert parse_responsa_query(example), example
    for example in MODE_EXAMPLES['Regex'][1]:
        re.compile(example)
    for example in MODE_EXAMPLES['Shelfmark'][1]:
        assert example.isascii(), example


def test_the_default_draw_is_random():
    random.seed(0)
    first = [mode_placeholder('responsa', _he) for _ in range(20)]
    assert len(MODE_EXAMPLES['responsa'][1]) == 1 or len(set(first)) > 1
