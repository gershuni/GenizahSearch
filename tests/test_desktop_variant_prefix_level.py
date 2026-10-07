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
# A bound search (a refinement replay, a Joins search) runs on a view of the engine of
# its own: nothing shared is written. It used to set the shared settings and put them
# back after, "only while still the one set here": a replay at Basic x1 put Maximum x3
# back under a main-window search that had started meanwhile at the same Basic x1, and
# that search's ישראל went from 17 spellings to 8,000.

COMMON = 'ישראל'          # Basic x1: 17 spellings; Maximum x3: 8,000 (the budget)


def _shared(pairs=150, changes=3):
    settings = SimpleNamespace(variant_pairs_count=pairs, variant_max_changes=changes,
                               variant_min_word_len=2, variant_aggressive=False, custom_variants={})
    return settings, VariantManager(settings)


def _spellings(var_mgr):
    return len(var_mgr.get_variants(COMMON, 'variants', limit=8000))


class _Recorder:
    """A stand-in engine. At each search it records how its variant manager expands
    (level, x1-x3, the spellings of COMMON) and what the shared settings and the shared
    variant manager hold at that moment. *during*, when given, runs first: something
    else happening while this search runs."""

    def __init__(self, settings, var_mgr, during=None):
        self.settings, self.var_mgr, self.during, self.ran = settings, var_mgr, during, []
        self.shared_var_mgr = var_mgr

    def execute_search(self, *a, **kw):
        if self.during:
            self.during()
        self.ran.append({
            'expands': (self.var_mgr.get_variant_level(), self.var_mgr._max_changes_setting(2),
                        _spellings(self.var_mgr)),
            'shared': (self.settings.variant_pairs_count, self.settings.variant_max_changes,
                       self.shared_var_mgr.get_variant_level()),
        })
        if kw.get('fail'):
            raise RuntimeError('engine failed')
        return ['row']

    def get_browse_page(self, sid):
        return {'sid': sid}


BASIC_X1 = {'variant_pairs_count': 30, 'variant_max_changes': 1}


def test_a_bound_search_expands_with_its_own_values_and_writes_nothing_shared():
    from desktop.variant_run_settings import recorded_settings_searcher
    settings, var_mgr = _shared(150, 3)
    engine = _Recorder(settings, var_mgr)
    bound = recorded_settings_searcher(engine, settings, BASIC_X1)
    assert bound.execute_search('q', 'variants', 0) == ['row']
    assert engine.ran == [{'expands': (30, 1, 17), 'shared': (150, 3, 150)}], (
        'the shared settings are not touched, not even while the search runs')
    assert engine.var_mgr is var_mgr, 'the shared engine keeps its variant manager'
    assert (settings.variant_pairs_count, settings.variant_max_changes) == (150, 3)
    assert _spellings(var_mgr) == 8000
    assert bound.get_browse_page('s') == {'sid': 's'}, 'everything else is the engine'
    with pytest.raises(RuntimeError):
        bound.execute_search('q', 'variants', 0, fail=True)
    assert (settings.variant_pairs_count, settings.variant_max_changes) == (150, 3)
    assert var_mgr.get_variant_level() == 150


def test_a_main_window_search_started_during_a_bound_search_keeps_its_settings():
    """The reviewer's probe: a replay binds Basic x1 over shared Maximum x3; meanwhile
    the main window starts a search at the same Basic x1. When the replay ends, the
    main window's search still has Basic x1 -- nothing puts Maximum x3 back under it."""
    from desktop.variant_run_settings import recorded_settings_searcher
    settings, var_mgr = _shared(150, 3)

    def main_window_search():       # what start_search sets for a ? search
        settings.variant_max_changes = 1
        var_mgr.set_variant_level(30)

    engine = _Recorder(settings, var_mgr, during=main_window_search)
    recorded_settings_searcher(engine, settings, BASIC_X1).execute_search('q', 'variants', 0)
    assert engine.ran[0]['expands'] == (30, 1, 17)
    assert (settings.variant_pairs_count, settings.variant_max_changes) == (30, 1)
    assert var_mgr.get_variant_level() == 30
    assert _spellings(var_mgr) == 17, "the main window's search expands at its own Basic x1"


def test_a_bound_search_keeps_its_values_when_the_shared_ones_change_during_it():
    """And the other way round: a main-window Maximum x3 search started while a Basic
    x1 replay runs does not change what the replay expands with."""
    from desktop.variant_run_settings import recorded_settings_searcher
    settings, var_mgr = _shared(70, 2)

    def main_window_search():       # what start_search sets for a ??? search
        settings.variant_max_changes = 3
        var_mgr.set_variant_level(150)

    engine = _Recorder(settings, var_mgr, during=main_window_search)
    recorded_settings_searcher(engine, settings, BASIC_X1).execute_search('q', 'variants', 0)
    assert engine.ran == [{'expands': (30, 1, 17), 'shared': (150, 3, 150)}]
    assert (settings.variant_pairs_count, settings.variant_max_changes) == (150, 3)


def test_joins_search_binds_basics_changes_only_and_reads_the_table_at_each_search():
    """Joins Lab: Basic's x1-x3, read at each search; the level is the current one."""
    from desktop.variant_run_settings import basic_changes_searcher
    settings, var_mgr = _shared(150, 3)
    settings.variant_max_changes_by_preset = {'basic': 1, 'extended': 2, 'maximum': 3}
    engine = _Recorder(settings, var_mgr)
    bound = basic_changes_searcher(engine, lambda: settings)
    bound.execute_search('q', 'variants', 0)
    settings.variant_max_changes_by_preset = {'basic': 2, 'extended': 2, 'maximum': 3}
    bound.execute_search('q', 'variants', 0)
    assert [r['expands'][:2] for r in engine.ran] == [(150, 1), (150, 2)]
    assert [r['shared'] for r in engine.ran] == [(150, 3, 150), (150, 3, 150)]
    assert (settings.variant_pairs_count, settings.variant_max_changes) == (150, 3)


@pytest.mark.parametrize('values', [None, {}, {'variant_max_changes': 'x', 'variant_pairs_count': 0},
                                    ['not', 'a', 'dict']])
def test_unusable_values_run_on_the_engine_as_it_is(values):
    from desktop.variant_run_settings import engine_with_variant_settings, recorded_settings_searcher
    settings, var_mgr = _shared(70, 2)
    engine = _Recorder(settings, var_mgr)
    assert engine_with_variant_settings(engine, settings, values) is engine
    recorded_settings_searcher(engine, settings, values).execute_search('q', 'variants', 0)
    assert engine.ran[0]['expands'][:2] == (70, 2)
    assert (settings.variant_pairs_count, settings.variant_max_changes) == (70, 2)


def test_a_partly_usable_record_keeps_the_current_value_for_the_rest():
    from desktop.variant_run_settings import engine_with_variant_settings
    settings, var_mgr = _shared(70, 2)
    view = engine_with_variant_settings(_Recorder(settings, var_mgr), settings,
                                        {'variant_pairs_count': 'x', 'variant_max_changes': 7})
    assert (view.var_mgr.get_variant_level(), view.var_mgr._max_changes_setting(2)) == (70, 3)
    assert settings.variant_max_changes == 2


def test_a_view_shares_the_engines_index_and_searchers():
    """A shallow copy of the real SearchEngine: the index, the searchers and the
    metadata are the engine's own objects, only the variant manager is new."""
    from desktop.variant_run_settings import engine_with_variant_settings
    settings, var_mgr = _shared(150, 3)
    engine = SearchEngine.__new__(SearchEngine)
    engine.var_mgr, engine.index, engine.searcher, engine.meta_mgr = var_mgr, object(), object(), object()
    view = engine_with_variant_settings(engine, settings, BASIC_X1)
    assert isinstance(view, SearchEngine) and view is not engine
    assert (view.index, view.searcher, view.meta_mgr) == (engine.index, engine.searcher, engine.meta_mgr)
    assert view.var_mgr is not var_mgr and engine.var_mgr is var_mgr
    assert _spellings(view.var_mgr) == 17 and _spellings(var_mgr) == 8000


def test_the_recorded_settings_are_what_the_search_runs_with():
    from desktop.variant_run_settings import variant_settings_now
    settings, var_mgr = _shared(70, 2)
    var_mgr.set_variant_level(150)
    settings.variant_max_changes = 3
    assert variant_settings_now(settings, var_mgr) == {'variant_pairs_count': 150, 'variant_max_changes': 3}
    assert variant_settings_now(None, var_mgr) is None


# --- Composition: spellings cut say "Partial results" ------------------------------------
# search_composition_logic returns partial=True when a word's spellings were cut
# ('capped') as well as when Stop ended it ('cancelled'). The Composition tab shows its
# "Partial results" summary for both; a cut run searched every chunk, and its telemetry
# says 'cancelled' only for a Stop.

class _ShownSummary(Exception):
    """Raised by the telemetry stand-in, just after the summary is shown."""


class _Bar:
    def __init__(self):
        self.text = None

    def setVisible(self, _v):
        pass

    def setRange(self, _a, _b):
        pass

    def setValue(self, _v):
        pass

    def setFormat(self, text):
        self.text = text


def _comp_summary(monkeypatch, result):
    """(the summary the Composition tab shows for *result*, the telemetry action)."""
    import genizah_core
    monkeypatch.setattr(genizah_core, 'CURRENT_LANG', 'en')
    monkeypatch.setattr(genizah_app, 'CURRENT_LANG', 'en', raising=False)
    told = []

    def telemetry(action, _count=None):
        told.append(action)
        raise _ShownSummary()

    host = SimpleNamespace(
        is_comp_running=True, reset_comp_ui=lambda: None,
        _refresh_comp_method_enabled=lambda: None,
        comp_sort_mode='score', comp_sort_reverse=True,
        _pause_comp=SimpleNamespace(elapsed=lambda _now: 65.0),
        _prime_comp_local_filepath_cache=lambda _rows: None,
        comp_chunks_processed=1, comp_chunks_total=3, comp_progress=_Bar(),   # progress last said chunk 1
        _notify_search_complete=lambda *a, **k: None,
        _emit_comp_search_telemetry=telemetry)
    with pytest.raises(_ShownSummary):
        APP.on_comp_scan_finished(host, result)
    return host.comp_progress.text, told[0]


def _comp_result(**flags):
    return {'main': [], 'filtered': [], 'boundary_stats': None, **flags}


def test_a_composition_whose_spellings_were_cut_shows_partial_results(monkeypatch):
    shown, action = _comp_summary(monkeypatch, _comp_result(partial=True, capped=True, cancelled=False))
    assert shown.startswith('Partial results'), shown
    assert '3/3 chunks' in shown, shown
    assert action == 'completed', 'nobody stopped it'


def test_a_stopped_composition_is_still_cancelled(monkeypatch):
    shown, action = _comp_summary(monkeypatch, _comp_result(partial=True, capped=True, cancelled=True))
    assert shown.startswith('Partial results') and '1/3 chunks' in shown and action == 'cancelled'
    # A result that does not say which (Lab Mode, letter-level): partial is a Stop, as before.
    shown, action = _comp_summary(monkeypatch, _comp_result(partial=True))
    assert shown.startswith('Partial results') and action == 'cancelled'


def test_a_complete_composition_says_completed(monkeypatch):
    shown, action = _comp_summary(monkeypatch, _comp_result(partial=False, capped=False, cancelled=False))
    assert shown.startswith('Completed in') and action == 'completed'


# --- GitHub review (Codex on #386, 2026-10-07): composition writes nothing shared ---

def _composition_window(monkeypatch, mode_idx):
    """The real run_composition on a mock window, with the real settings, variant
    manager and level helpers; CompositionThread records how it was built."""
    from types import MethodType
    from unittest.mock import MagicMock
    settings, shared = _shared(150, 3)      # a main-window Maximum x3 search runs on these
    settings.variant_max_changes_by_preset = {'basic': 1, 'extended': 2, 'maximum': 3}
    settings.boundary_boost, settings.min_boundary_matches, settings.min_delimiter_distance = 1.5, 0, 3
    w = MagicMock()
    w.lab_engine = SimpleNamespace(settings=settings)
    w.var_mgr = shared
    w.searcher = SimpleNamespace(var_mgr=shared)
    w._run_seq = 0
    for name in ('_lab_settings', '_level_max_changes', '_use_variant_changes'):
        setattr(w, name, MethodType(getattr(APP, name), w))
    w.comp_text_area.toPlainText.return_value = f'{WORD} {WORD} {WORD}'
    w.comp_title_input.text.return_value = 'title'
    w.comp_mode_combo.currentIndex.return_value = mode_idx
    w.comp_variant_slider.value.return_value = 50
    w.boundary_mode_combo.currentData.return_value = 'full'
    w.comp_corpus_scope_combo.currentData.return_value = 'genizah'
    w.btn_lab_mode_toggle_comp.isChecked.return_value = False
    w._comp_method.return_value = 'chunk'
    thread = MagicMock()
    monkeypatch.setattr(genizah_app, 'CompositionThread', thread)
    return w, settings, shared, thread


@pytest.mark.parametrize('mode_idx,level,changes', [(1, 50, 1), (2, None, 2)], ids=['variants', 'fuzzy'])
def test_a_composition_runs_on_its_own_settings_and_writes_nothing_shared(monkeypatch, mode_idx, level,
                                                                          changes):
    """A composition started while a main-window Maximum x3 search runs changed the
    shared x to Basic's, and the shared level to its slider's, under that search.
    It now runs on a view of the engine with its own level and x."""
    w, settings, shared, thread = _composition_window(monkeypatch, mode_idx)
    APP.run_composition(w)
    assert thread.call_count == 1
    searcher = thread.call_args.args[0]
    assert (settings.variant_pairs_count, settings.variant_max_changes) == (150, 3)
    assert shared.get_variant_level() == 150 and _spellings(shared) == 8000
    assert searcher.var_mgr is not shared
    assert searcher.var_mgr._settings.variant_max_changes == changes
    if level is not None:
        assert searcher.var_mgr.get_variant_level() == level


def test_the_count_preview_counts_on_its_own_and_writes_nothing_shared():
    """GitHub review (Codex on #386, round 3): the variant-count preview, run as the
    user types or changes the level, wrote the shown level's x into the shared
    settings and reset the shared VariantManager -- under a search still running
    on them. It now counts on a VariantManager of its own."""
    import copy
    from types import MethodType
    from unittest.mock import MagicMock
    settings, shared = _shared(150, 3)      # a main-window Maximum x3 search runs on these
    settings.variant_max_changes_by_preset = {'basic': 1, 'extended': 2, 'maximum': 3}
    w = MagicMock()
    w.lab_engine = SimpleNamespace(settings=settings)
    w.var_mgr = shared
    w._variant_preview = None
    for name in ('_update_variant_count_preview', '_lab_settings', '_level_max_changes',
                 '_use_variant_changes'):
        setattr(w, name, MethodType(getattr(APP, name), w))
    w._get_current_variant_pairs_count = lambda: 30   # Basic shown, x1
    w.query_input.text.return_value = f'{COMMON} {WORD}'
    w._update_variant_count_preview()
    assert (settings.variant_pairs_count, settings.variant_max_changes) == (150, 3)
    assert shared.get_variant_level() == 150 and _spellings(shared) == 8000
    own = copy.copy(settings)
    own.variant_pairs_count, own.variant_max_changes = 30, 1
    expected = sum(len(VariantManager(own).get_variants(t, 'variants', limit=500)) for t in (COMMON, WORD))
    assert w.variant_count_label.setText.call_args.args[0] == f'≈{expected}'
    first = w._variant_preview._mgr
    w._update_variant_count_preview()
    assert w._variant_preview._mgr is first, 'typing reuses the preview manager and its cache'
