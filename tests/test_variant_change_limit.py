# -*- coding: utf-8 -*-
"""x1-x3 ("Num Changes") is the real per-word limit of single-letter changes.

Owner ruling 2026-09-28 (option A): in every variant level x1, x2 and x3 each
mean what they say, kept per level (defaults x1 Basic, x2 Extended and Maximum);
the single value saved before levels had their own seeds Extended and Maximum
only. Raising the limit never pushes out a spelling a smaller one finds (the
two-letters-for-one ones especially), and a list cut at its budget says so.

Uses the real VariantManager and the real pair table (shared/unified_variants.py).
"""
from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from shared.config import Config
from shared.variants import (
    DEFAULT_MAX_CHANGES_BY_PRESET, VariantManager, max_changes_by_preset, variant_preset_of,
)

LONG = 'והמשפטים'          # 8 letters, most of them confusable
COMMON = 'ישראל'


def _settings(pairs=30, changes=2, **extra):
    return SimpleNamespace(variant_pairs_count=pairs, variant_max_changes=changes,
                           variant_min_word_len=2, variant_aggressive=False,
                           custom_variants={}, **extra)


def _same_length_distance(term, variants):
    return max(sum(a != b for a, b in zip(term, v)) for v in variants if len(v) == len(term))


@pytest.mark.parametrize('tier', ['variants', 'variants_extended', 'variants_maximum'])
@pytest.mark.parametrize('changes', [1, 2, 3])
def test_the_limit_is_the_setting_in_every_tier(tier, changes):
    """x3 did nothing anywhere and x2 nothing in Basic: the tier capped the setting."""
    variants = VariantManager(_settings(changes=changes)).get_variants(
        LONG, tier, limit=Config.REGEX_VARIANTS_LIMIT)
    assert _same_length_distance(LONG, variants) == changes


def test_short_words_keep_one_change():
    variants = VariantManager(_settings(changes=3)).get_variants('אב', 'variants_maximum', limit=8000)
    assert _same_length_distance('אב', variants) == 1


@pytest.mark.parametrize('word', [COMMON, LONG])
@pytest.mark.parametrize('pairs', [70, 150])
def test_raising_the_limit_keeps_every_spelling_the_lower_one_finds(word, pairs):
    """Measured before: ישראל at 150 pairs kept 1 of 2,717 two-letters-for-one
    spellings at x3 -- the third change filled the 8,000 budget first."""
    two = set(VariantManager(_settings(pairs=pairs, changes=2)).get_variants(word, 'variants', limit=8000))
    three = set(VariantManager(_settings(pairs=pairs, changes=3)).get_variants(word, 'variants', limit=8000))
    assert two <= three
    assert len(three) > len(two) or len(two) >= 8000


def test_a_cut_list_says_so():
    mgr = VariantManager(_settings(pairs=150, changes=3))
    assert mgr.variants_overflowed(LONG, 'variants', limit=50) is True
    assert VariantManager(_settings(pairs=30, changes=1)).variants_overflowed(COMMON, 'variants') is False


def test_the_cache_follows_the_change_settings():
    """The desktop changes the limit between searches without clearing the cache."""
    settings = _settings(changes=1)
    mgr = VariantManager(settings)
    one = mgr.get_variants(LONG, 'variants', limit=8000)
    settings.variant_max_changes = 3
    three = mgr.get_variants(LONG, 'variants', limit=8000)
    assert _same_length_distance(LONG, one) == 1 and _same_length_distance(LONG, three) == 3


def test_the_engine_says_plus_when_a_word_overflowed():
    from shared.search_engine import SearchEngine, consume_last_search_cutoff
    engine = SearchEngine.__new__(SearchEngine)
    engine.var_mgr = SimpleNamespace(get_variants=lambda *a, **k: ['x'],
                                     variants_overflowed=lambda term, mode, limit=None: term == 'big')
    consume_last_search_cutoff()
    engine._get_or_compute_variants(['small'], 'variants')
    assert consume_last_search_cutoff()['capped'] is False
    engine._get_or_compute_variants(['small', 'big'], 'variants')
    assert consume_last_search_cutoff()['capped'] is True


@pytest.mark.parametrize('pairs, level', [(10, 'basic'), (30, 'basic'), (69, 'basic'), (70, 'extended'),
                                          (149, 'extended'), (150, 'maximum'), (300, 'maximum')])
def test_levels(pairs, level):
    assert variant_preset_of(pairs) == level


def test_defaults_and_the_old_single_value():
    assert DEFAULT_MAX_CHANGES_BY_PRESET == {'basic': 1, 'extended': 2, 'maximum': 2}
    assert max_changes_by_preset() == {'basic': 1, 'extended': 2, 'maximum': 2}
    # The old global value seeds Extended and Maximum only: Basic stays x1.
    assert max_changes_by_preset(legacy=3) == {'basic': 1, 'extended': 3, 'maximum': 3}
    assert max_changes_by_preset({'basic': 9, 'extended': 'x'}) == {'basic': 3, 'extended': 2, 'maximum': 2}


def test_an_upgraded_desktop_profile_keeps_todays_basic(tmp_path, monkeypatch):
    """A lab_config.json from before (one value, 2 for everyone) gives Basic x1 --
    what Basic always ran -- and Extended/Maximum x2."""
    from shared import lab_settings
    path = tmp_path / 'lab_config.json'
    path.write_text(json.dumps({'variant_max_changes': 2}), encoding='utf-8')
    monkeypatch.setattr(lab_settings.Config, 'LAB_CONFIG_FILE', str(path))
    monkeypatch.setattr(lab_settings.Config, 'LAB_DIR', str(tmp_path))
    settings = lab_settings.LabSettings()
    assert settings.variant_max_changes_by_preset == {'basic': 1, 'extended': 2, 'maximum': 2}
    assert (settings.max_changes_for(30), settings.max_changes_for(70), settings.max_changes_for(150)) == (1, 2, 2)
    settings.variant_max_changes_by_preset = {'basic': 2, 'extended': 3, 'maximum': 1}
    settings.save()
    assert lab_settings.LabSettings().variant_max_changes_by_preset == {'basic': 2, 'extended': 3, 'maximum': 1}
