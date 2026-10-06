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


# --- The spin box shows the x1-x3 the next search runs with (2026-10-06) ------------
# It changes with the level. Three ways of changing the level, or the table, left it
# showing another level's value: the Composition slider (it moves the search slider
# with signals blocked), saving Search Settings, and startup (the box is built with
# the defaults before the saved settings are read).

def test_the_composition_slider_moves_the_spin_box_with_the_search_slider():
    host = _Host(WORD, 30, use_slider=True, mode_idx=1)
    host._show_level_max_changes()
    assert host.spin_max_changes.value() == 1                  # Basic
    host._sync_variant_sliders(150, 'comp')                    # the Composition slider moved
    assert host.variant_slider.value() == 150
    assert host.spin_max_changes.value() == 3, 'the spin box still shows Basic x1'
    host.start_search()
    assert host.lab_engine.settings.variant_max_changes == host.spin_max_changes.value()


def test_saving_search_settings_shows_the_new_value(monkeypatch):
    class _Dialog:          # Search Settings: Extended changed to x3, Save & Close
        def __init__(self, parent, settings):
            self.settings = settings

        def exec(self):
            self.settings.variant_max_changes_by_preset = {'basic': 1, 'extended': 3, 'maximum': 3}
            return True

    monkeypatch.setattr(genizah_app, 'SearchSettingsDialog', _Dialog)
    host = _Host(WORD, 70, mode_idx=1)
    host._show_level_max_changes()
    assert host.spin_max_changes.value() == 2
    host.open_search_settings()
    assert host.spin_max_changes.value() == 3, 'the spin box still shows the old x2'
    host.start_search()
    assert host.lab_engine.settings.variant_max_changes == 3


def test_startup_shows_the_saved_value_of_the_shown_level(monkeypatch, tmp_path):
    from unittest.mock import MagicMock
    saved = SimpleNamespace(variant_use_slider=False, variant_pairs_count=70,
                            variant_max_changes_by_preset={'basic': 1, 'extended': 3, 'maximum': 3})
    monkeypatch.setattr(genizah_app, 'LabEngine', lambda meta, var_mgr: SimpleNamespace(settings=saved))
    monkeypatch.setattr(genizah_app, 'ListsManager', lambda meta: MagicMock())
    monkeypatch.setattr(genizah_app, 'JoinsManager', lambda client: MagicMock())
    monkeypatch.setattr(genizah_app, 'QMessageBox', MagicMock())
    monkeypatch.setattr(genizah_app.Config, 'REPORTS_DIR', str(tmp_path / 'reports'))
    monkeypatch.setattr(genizah_app.Config, 'INDEX_DIR', str(tmp_path))
    host = MagicMock()                        # the window init_ui built
    host.spin_max_changes = _Spin(2)          # built before the settings were read: the defaults
    host._current_variant_preset = 70         # Extended
    for name in ('_show_level_max_changes', '_level_max_changes', '_lab_settings',
                 '_get_current_variant_pairs_count'):
        setattr(host, name, getattr(APP, name).__get__(host))
    APP.on_startup_finished(host, MagicMock(), MagicMock(), MagicMock(), MagicMock())
    assert host.lab_engine.settings is saved
    assert host.spin_max_changes.value() == 3, 'the spin box shows the default x2'


# --- A damaged saved table falls back to the single value saved before (2026-10-06) --
# As on the website (web/variant_preferences.py::max_changes_table): a table that is
# not a dict is no table, so the old single value seeds Extended and Maximum.

@pytest.mark.parametrize('damaged', [[1, 2, 3], 'x', 2, True])
def test_a_damaged_table_falls_back_to_the_single_value(tmp_path, monkeypatch, damaged):
    import json
    from shared import lab_settings
    from shared.variants import max_changes_by_preset
    path = tmp_path / 'lab_config.json'
    path.write_text(json.dumps({'variant_max_changes': 3, 'variant_max_changes_by_preset': damaged}),
                    encoding='utf-8')
    monkeypatch.setattr(lab_settings.Config, 'LAB_CONFIG_FILE', str(path))
    monkeypatch.setattr(lab_settings.Config, 'LAB_DIR', str(tmp_path))
    table = lab_settings.LabSettings().variant_max_changes_by_preset
    assert table == {'basic': 1, 'extended': 3, 'maximum': 3}
    assert table == max_changes_by_preset(legacy=3)       # the website's fallback


# --- desktop/variant_run_settings.py ---------------------------------------------------

def _shared(pairs=150, changes=3):
    settings = SimpleNamespace(variant_pairs_count=pairs, variant_max_changes=changes,
                               variant_min_word_len=2, variant_aggressive=False, custom_variants={})
    return settings, VariantManager(settings)


class _Recorder:
    def __init__(self, settings, var_mgr):
        self.settings, self.var_mgr, self.ran = settings, var_mgr, []

    def execute_search(self, *a, **kw):
        self.ran.append((self.settings.variant_pairs_count, self.settings.variant_max_changes,
                         self.var_mgr.get_variant_level()))
        if kw.get('fail'):
            raise RuntimeError('engine failed')
        return ['row']

    def get_browse_page(self, sid):
        return {'sid': sid}


def test_a_bound_searcher_runs_with_its_values_and_puts_the_shared_ones_back():
    from desktop.variant_run_settings import recorded_settings_searcher
    settings, var_mgr = _shared()
    engine = _Recorder(settings, var_mgr)
    bound = recorded_settings_searcher(engine, settings, var_mgr,
                                       {'variant_pairs_count': 30, 'variant_max_changes': 1})
    assert bound.execute_search('q', 'variants', 0) == ['row']
    assert engine.ran == [(30, 1, 30)]
    assert (settings.variant_pairs_count, settings.variant_max_changes) == (150, 3)
    assert var_mgr.get_variant_level() == 150
    assert bound.get_browse_page('s') == {'sid': 's'}, 'everything else is the engine'
    with pytest.raises(RuntimeError):
        bound.execute_search('q', 'variants', 0, fail=True)
    assert (settings.variant_pairs_count, settings.variant_max_changes) == (150, 3), 'put back on failure'


def test_a_value_another_search_set_meanwhile_is_kept():
    """Joins Lab can search beside the main window: a value changed during the search
    is that other search's, and is not overwritten with the one from before."""
    from desktop.variant_run_settings import variant_settings_applied
    settings, var_mgr = _shared(70, 2)
    with variant_settings_applied(settings, var_mgr, {'variant_max_changes': 1}):
        assert settings.variant_max_changes == 1
        settings.variant_max_changes = 3                # a main-window Maximum search started
    assert settings.variant_max_changes == 3


@pytest.mark.parametrize('values', [None, {}, {'variant_max_changes': 'x', 'variant_pairs_count': 0},
                                    ['not', 'a', 'dict']])
def test_unusable_values_leave_the_shared_settings_alone(values):
    from desktop.variant_run_settings import variant_settings_applied
    settings, var_mgr = _shared(70, 2)
    with variant_settings_applied(settings, var_mgr, values):
        assert (settings.variant_pairs_count, settings.variant_max_changes) == (70, 2)
    assert (settings.variant_pairs_count, settings.variant_max_changes) == (70, 2)


def test_the_recorded_settings_are_what_the_search_runs_with():
    from desktop.variant_run_settings import variant_settings_now
    settings, var_mgr = _shared(70, 2)
    var_mgr.set_variant_level(150)
    settings.variant_max_changes = 3
    assert variant_settings_now(settings, var_mgr) == {'variant_pairs_count': 150, 'variant_max_changes': 3}
    assert variant_settings_now(None, var_mgr) is None
