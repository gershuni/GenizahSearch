# -*- coding: utf-8 -*-
"""Desktop: the ?, ?? and ??? prefixes choose Basic / Extended / Maximum, each with its x1-x3.

Runs the real GenizahGUI.start_search on a stub window (no QApplication), with
the real VariantManager and the real preset/level methods bound to the stub.
The run stops at _drain_previous_worker, which is after the variant level has
been applied and before any worker starts.
"""
from __future__ import annotations

from types import SimpleNamespace

import pytest

import genizah_app  # noqa: E402
from shared.search_engine import SearchEngine  # noqa: E402
from shared.variants import VariantManager  # noqa: E402

APP = genizah_app.GenizahGUI
WORD = 'אבגד'


class _Text:
    def __init__(self, text=''):
        self._text = text

    def text(self):
        return self._text

    def setText(self, t):
        self._text = t

    def blockSignals(self, _b):
        pass


class _Combo:
    def __init__(self, idx=0):
        self.idx = idx

    def currentIndex(self):
        return self.idx

    def setCurrentIndex(self, i):
        self.idx = i


class _Check:
    def __init__(self, on=False):
        self.on = on

    def isChecked(self):
        return self.on

    def setChecked(self, v):
        self.on = v


class _Slider:
    def __init__(self, v):
        self.v = v

    def value(self):
        return self.v

    def setValue(self, v):
        self.v = v

    def blockSignals(self, _b):
        pass


class _Spin:
    def __init__(self, v):
        self.v = v

    def value(self):
        return self.v

    def setValue(self, v):
        self.v = v

    def blockSignals(self, _b):
        pass


class _Host:
    MODE_RESPONSA = 2  # set per instance in GenizahGUI.__init__ (genizah_app.py:5627)
    start_search = APP.start_search
    _on_query_text_changed = APP._on_query_text_changed
    _SHORTCUT_PREFIXES = APP._SHORTCUT_PREFIXES
    _set_variant_preset = APP._set_variant_preset
    _get_current_variant_pairs_count = APP._get_current_variant_pairs_count

    def __init__(self, query, preset, use_slider=False, mode_idx=0):
        settings = SimpleNamespace(variant_pairs_count=preset, variant_max_changes=2,
                                   variant_min_word_len=2, variant_aggressive=False,
                                   custom_variants={}, variant_use_slider=use_slider,
                                   variant_max_changes_by_preset={'basic': 1, 'extended': 2, 'maximum': 3})
        self.lab_engine = SimpleNamespace(settings=settings)
        self.var_mgr = VariantManager(settings)
        self.searcher = SimpleNamespace(
            parse_query_syntax=lambda q, responsa_mode=False:
                SearchEngine.parse_query_syntax(None, q, responsa_mode=responsa_mode))
        self.query_input = _Text(query)
        self.mode_combo = _Combo(mode_idx)
        self.btn_lab_mode_toggle = None
        self.btn_variant_basic = _Check(preset == 30)
        self.btn_variant_extended = _Check(preset == 70)
        self.btn_variant_maximum = _Check(preset == 150)
        self.variant_slider = _Slider(preset)
        self.variant_slider_label = _Text(str(preset))
        self.spin_max_changes = _Spin(2)
        self.gap_input = _Text('0')
        self.exclude_input = _Text('')
        self._current_variant_preset = preset
        self._refine_mode = False
        self.refinement_chain = []
        self.reached_worker_start = False
        self._pause_search = None

    def __getattr__(self, name):
        # Any other GenizahGUI method (for example a helper a fix introduces)
        # runs for real on this stub; plain data attributes still fail loudly.
        attr = getattr(APP, name)
        if callable(attr):
            return attr.__get__(self, _Host)
        if isinstance(attr, (dict, list, tuple)):  # class-level tables
            return attr
        raise AttributeError(name)

    def _set_local_scope_strip_visible(self, _v):
        pass

    def _update_variant_count_preview(self):
        pass

    def _drain_previous_worker(self, *_a):
        self.reached_worker_start = True
        return False  # stop here: the level is applied, nothing is launched


@pytest.mark.parametrize('prefix,start_preset,expected', [
    ('???', 70, 150),
    ('??', 30, 70),
    ('?', 150, 30),
])
@pytest.mark.parametrize('use_slider', [False, True])
def test_prefix_selects_its_preset(prefix, start_preset, expected, use_slider):
    host = _Host(f'{prefix} {WORD}', start_preset, use_slider=use_slider)
    host.start_search()
    assert host.reached_worker_start, 'start_search returned before applying the level'
    assert host.mode_combo.currentIndex() == 1  # Variants
    assert host.last_search_query == WORD
    assert host.var_mgr.get_variant_level() == expected
    assert host._get_current_variant_pairs_count() == expected


def test_no_prefix_keeps_the_chosen_preset():
    host = _Host(WORD, 150, mode_idx=1)
    host.start_search()
    assert host.var_mgr.get_variant_level() == 150


# Typing "??? " (prefix then a space) is handled while typing, before any
# search starts: the prefix is removed from the box at once, so start_search
# never sees it. This is the path a user who types the prefix actually takes.
@pytest.mark.parametrize('prefix,start_preset,expected', [
    ('???', 70, 150),
    ('??', 30, 70),
    ('?', 150, 30),
])
@pytest.mark.parametrize('use_slider', [False, True])
def test_typed_prefix_selects_its_preset_then_search_keeps_it(prefix, start_preset, expected, use_slider):
    host = _Host(prefix + ' ', start_preset, use_slider=use_slider)
    host._on_query_text_changed()
    assert host.query_input.text() == ''
    assert host.mode_combo.currentIndex() == 1  # Variants
    assert host.var_mgr.get_variant_level() == expected
    # The user now types the word and presses Enter.
    host.query_input.setText(WORD)
    host.start_search()
    assert host.reached_worker_start
    assert host.var_mgr.get_variant_level() == expected
    assert host._get_current_variant_pairs_count() == expected


@pytest.mark.parametrize('prefix,changes', [('?', 1), ('??', 2), ('???', 3)])
def test_each_level_runs_with_its_own_changes(prefix, changes):
    """x1-x3 is kept per level: the search runs with the chosen level's limit, and
    the spin box shows it."""
    host = _Host(f'{prefix} {WORD}', 70)
    host.start_search()
    assert host.lab_engine.settings.variant_max_changes == changes
    assert host.spin_max_changes.value() == changes


def test_responsa_runs_with_basics_changes():
    host = _Host(WORD, 150, mode_idx=2)
    host.start_search()
    assert host.lab_engine.settings.variant_max_changes == 1
