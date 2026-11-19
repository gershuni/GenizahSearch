# -*- coding: utf-8 -*-
"""The desktop query box's shortcuts tooltip lists every sign of the one table of signs.

desktop/catalog_search.py::signs_tooltip reads shared/search_mode_signs.py::MODE_SIGNS, so a sign
added there is listed here too. The tooltip it replaced was written out by hand and fell behind:
it had no % (Catalog), no ?? or ??? (the Variants levels), and gave R as a sign with no space
after it, which since the engine needs one (search_engine.parse_query_syntax) searches a word."""
from __future__ import annotations

import os

import pytest

pytest.importorskip("PyQt6")

import desktop.catalog_search as cs  # noqa: E402
from shared.genizah_translations import TRANSLATIONS  # noqa: E402
from shared.search_mode_examples import EXAMPLE_TEMPLATE  # noqa: E402
from shared.search_mode_signs import CATALOG_MODE, MODE_SIGNS  # noqa: E402

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _he(text):
    return TRANSLATIONS.get(text, text)


def _en(text):
    return text


def _signs_listed(tooltip, translate):
    """The sign of each mode line ('Title: $' -> '$'; 'Responsa: R and a space' -> 'R')."""
    letter_tail = translate(cs.LETTER_SIGN).format(sign='')
    signs = []
    for line in tooltip.split("\n\n")[0].split("\n")[1:]:
        name, shown = line.rsplit(': ', 1)
        signs.append(shown[:-len(letter_tail)] if shown.endswith(letter_tail) else shown)
    return signs


@pytest.mark.parametrize("translate", [_en, _he])
def test_every_sign_is_listed_once_where_the_catalogue_is_offered(translate):
    listed = _signs_listed(cs.signs_tooltip(translate, True), translate)
    assert sorted(listed) == sorted(sign for sign, _mode in MODE_SIGNS)


def test_a_second_sign_for_a_mode_is_listed_too(monkeypatch):
    """The tooltip reads the table, not a copy, and keeps every sign a mode has."""
    import shared.search_mode_signs as signs
    monkeypatch.setattr(signs, 'MODE_SIGNS', (('!', 'Title'),) + MODE_SIGNS)
    tooltip = cs.signs_tooltip(_en, True)
    assert sorted(_signs_listed(tooltip, _en)) == sorted(['!'] + [sign for sign, _mode in MODE_SIGNS])
    assert "Title: !" in tooltip.split("\n") and "Title: $" in tooltip.split("\n")


def test_the_example_shows_variants_and_responsa_signs():
    sign_of = {mode: sign for sign, mode in MODE_SIGNS}
    tooltip = cs.signs_tooltip(_en, True)
    assert tooltip.endswith(f"For example: {sign_of['variants']}שלום, {sign_of['responsa']} שלום")


def test_the_example_survives_a_table_without_variants_or_responsa(monkeypatch):
    """The search tab builds this unconditionally: a mode leaving the table changes the example,
    never raises (Codex review of #58)."""
    import shared.search_mode_signs as signs
    monkeypatch.setattr(signs, 'MODE_SIGNS', tuple(
        (sign, mode) for sign, mode in MODE_SIGNS if mode not in ('variants', 'responsa')))
    tooltip = cs.signs_tooltip(_en, False)
    first_symbol = _signs_listed(tooltip, _en)[0]
    assert tooltip.endswith(f"For example: {first_symbol}שלום"), tooltip
    assert " and a space" not in tooltip
    monkeypatch.setattr(signs, 'MODE_SIGNS', ())
    assert cs.signs_tooltip(_en, True) == cs.SIGNS_HEADER


@pytest.mark.parametrize("translate", [_en, _he])
def test_the_catalogue_sign_is_listed_only_where_it_is_offered(translate):
    listed = _signs_listed(cs.signs_tooltip(translate, False), translate)
    assert sorted(listed) == sorted(sign for sign, mode in MODE_SIGNS if mode != CATALOG_MODE)


def test_each_line_names_its_mode_and_a_letter_sign_asks_for_a_space():
    lines = cs.signs_tooltip(_en, True).split("\n")
    sign_of = {mode: sign for sign, mode in MODE_SIGNS}
    assert f"Variants Extended: {sign_of['variants_extended']}" in lines
    assert f"Catalog: {sign_of[CATALOG_MODE]}" in lines
    assert f"Responsa: {sign_of['responsa']} and a space" in lines
    for sign, _mode in MODE_SIGNS:              # a symbol sign needs no space, a letter sign does
        assert (f"{sign} and a space" in cs.signs_tooltip(_en, True)) == sign.isalpha()


def test_catalog_is_listed_between_title_and_shelfmark_as_the_mode_combo_shows_it():
    lines = cs.signs_tooltip(_he, True).split("\n")
    names = [line.rsplit(': ', 1)[0] for line in lines if ': ' in line]
    at = names.index(_he('Title'))
    assert names[at:at + 3] == [_he('Title'), _he('Catalog'), _he('Shelfmark')]
    assert _he('Catalog') not in cs.signs_tooltip(_he, False)


def test_every_mode_with_a_sign_has_a_name_with_its_hebrew():
    assert set(cs.SIGN_NAMES) == {mode for _sign, mode in MODE_SIGNS}
    for text in list(cs.SIGN_NAMES.values()) + [cs.SIGNS_HEADER, cs.LETTER_SIGN, EXAMPLE_TEMPLATE]:
        assert TRANSLATIONS.get(text), text
    hebrew = cs.signs_tooltip(_he, True)
    assert not any(name in hebrew for name in cs.SIGN_NAMES.values()), hebrew


def test_the_search_tab_sets_it_once_it_knows_whether_the_catalogue_is_offered():
    with open(os.path.join(REPO_ROOT, "genizah_app.py"), encoding="utf-8") as f:
        src = f.read()
    call = "self.query_input.setToolTip(signs_tooltip(tr, self._catalog_offered))"
    assert src.count(call) == 1
    assert src.index("self._catalog_offered = catalog_offered()") < src.index(call)
    assert src.index(call) < src.index("show_mode_examples(self.mode_combo, self.query_input, tr)")
    assert "Search Shortcuts:\\n" not in src     # the hand-written list it replaced
