"""The public API's variant levels equal the website's for a visitor who chose nothing.

web/variant_preferences.py::default_search is what the API runs; for_search is what
the two website pages run (web/pages/search.py and web/pages/parallels.py call it as
below). With empty visitor storage the two must agree for every mode on both pages
(docs/plans/MCP_CHUNK_AND_VARIANT_LEVELS_PLAN.md, section 3).
"""
from __future__ import annotations

import pytest

from web import variant_preferences as prefs

MODES = ('exact', 'variants', 'variants_extended', 'variants_maximum', 'fuzzy')


@pytest.fixture(autouse=True)
def empty_visitor_storage(monkeypatch):
    import web.safe_storage
    monkeypatch.setattr(web.safe_storage, 'safe_user_get', lambda key, default=None: default)


def search_page(mode):
    """execute_search: a variant level is its own engine mode at its preset's pairs;
    fuzzy sends the current preset (Basic until the visitor picks another)."""
    if mode in prefs.PRESET_PAIRS:
        return mode, prefs.for_search(mode, prefs.PRESET_PAIRS[mode])
    if mode == 'fuzzy':
        return mode, prefs.for_search(mode, prefs.BASIC_PAIRS)
    return mode, prefs.for_search(mode)


def parallels_page(mode):
    """The parallels page: mode 'variants' with the level's pairs and Basic's x1-x3."""
    if mode in prefs.PRESET_PAIRS:
        return 'variants', prefs.for_search('variants', prefs.PRESET_PAIRS[mode], prefs.max_changes('basic'))
    return mode, prefs.for_search(mode)


@pytest.mark.parametrize('mode', MODES)
def test_an_api_search_runs_as_the_search_page_does(mode):
    assert prefs.default_search(mode) == search_page(mode)


@pytest.mark.parametrize('mode', MODES)
def test_an_api_chunk_search_runs_as_the_parallels_page_does(mode):
    assert prefs.default_search(mode, composition=True) == parallels_page(mode)


@pytest.mark.parametrize('mode,pairs,changes', [
    ('variants', 30, 1), ('variants_extended', 70, 2), ('variants_maximum', 150, 2)])
def test_the_search_page_levels_are_the_presets_at_their_own_changes(mode, pairs, changes):
    engine_mode, settings = prefs.default_search(mode)
    assert (engine_mode, settings['variant_pairs_count'], settings['variant_max_changes']) == (mode, pairs, changes)


@pytest.mark.parametrize('mode,pairs', [('variants', 30), ('variants_extended', 70), ('variants_maximum', 150)])
def test_composition_levels_change_the_pairs_only(mode, pairs):
    engine_mode, settings = prefs.default_search(mode, composition=True)
    assert (engine_mode, settings['variant_pairs_count'], settings['variant_max_changes']) == ('variants', pairs, 1)


def test_every_other_setting_is_the_website_default():
    for mode in MODES:
        for composition in (False, True):
            _, settings = prefs.default_search(mode, composition=composition)
            for name in ('variant_min_word_len', 'variant_aggressive', 'custom_variants', 'comp_min_score'):
                assert settings[name] == prefs.WEBSITE_DEFAULTS[name], (mode, composition, name)
