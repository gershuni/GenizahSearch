# -*- coding: utf-8 -*-
"""Search tab: a zero-result search in LOCAL scope offers the Genizah corpus.

Owner request (2026-09-17): users pick "Local" in the corpus-scope combo by
accident, search, find nothing, and conclude the manuscript is not there.
After a LOCAL-scope run with no results a strip above the empty results
table says the search looked only at their local files and offers the same
query against the Genizah corpus in one click.

Rules pinned here:
  - shown only when the run that just finished was scope 'local' AND found
    nothing; 'all' includes the Genizah corpus, a LOCAL run with results is
    what was asked for;
  - the scope is the one RECORDED for the run, not the combo's live value;
  - hidden again on every new search and on any manual scope change;
  - the button routes through the combo (so the choice persists like a
    manual change) and re-runs start_search();
  - both strings have Hebrew translations.

Pattern: unbound GenizahGUI methods bound to a stub -- no QApplication
(same approach as tests/test_desktop_passage_gate.py).
"""
from __future__ import annotations

import inspect
import os
import re
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

import genizah_app                       # noqa: E402
import genizah_translations as gt        # noqa: E402

APP = genizah_app.GenizahGUI

LABEL = "No results in My Library. This search looked only at your local files."
BUTTON = "Search the Genizah corpus instead"


class _Strip:
    def __init__(self):
        self.visible = None
        self.stylesheet = None

    def setVisible(self, v):
        self.visible = v

    def setStyleSheet(self, css):
        self.stylesheet = css


class _Combo:
    ITEMS = ['genizah', 'local', 'all']

    def __init__(self, data):
        self._idx = self.ITEMS.index(data)
        self.set_calls = []

    def findData(self, d):
        return self.ITEMS.index(d) if d in self.ITEMS else -1

    def currentIndex(self):
        return self._idx

    def currentData(self):
        return self.ITEMS[self._idx]

    def setCurrentIndex(self, i):
        self._idx = i
        self.set_calls.append(i)


class _Host:
    _search_run_corpus = APP._search_run_corpus
    _set_local_scope_strip_visible = APP._set_local_scope_strip_visible
    _update_local_scope_strip = APP._update_local_scope_strip
    _search_genizah_instead = APP._search_genizah_instead
    _style_local_scope_strip = APP._style_local_scope_strip
    _local_scope_strip_colors = staticmethod(APP._local_scope_strip_colors)

    def __init__(self, run_corpus='local', combo_corpus=None, dark=False):
        self.local_scope_strip = _Strip()
        self.local_scope_strip_label = _Strip()
        self.corpus_scope_combo = _Combo(combo_corpus or run_corpus)
        self._current_search_run = {'mode': 'literal', 'corpus': run_corpus,
                                    'emitted': False}
        self.started = 0
        self._dark = dark

    def start_search(self):
        self.started += 1

    def palette(self):
        lightness = 40 if self._dark else 240

        class _P:
            def color(self, _role):
                class _C:
                    def lightness(self_inner):
                        return lightness
                return _C()
        return _P()


# ---------------------------------------------------------------- the rule

def test_zero_results_in_local_scope_shows_the_hint():
    h = _Host('local')
    assert h._update_local_scope_strip(0) is True
    assert h.local_scope_strip.visible is True


def test_zero_results_in_genizah_or_all_scope_shows_nothing():
    for scope in ('genizah', 'all'):
        h = _Host(scope)
        assert h._update_local_scope_strip(0) is False, scope
        assert h.local_scope_strip.visible is False, scope


def test_a_cancelled_local_search_shows_nothing():
    """Codex (PR #343): a cancelled run is incomplete -- 'Partial results' --
    so 'nothing in your local files' would be a guess."""
    h = _Host('local')
    assert h._update_local_scope_strip(0, cancelled=True) is False
    assert h.local_scope_strip.visible is False
    assert h._update_local_scope_strip(0, cancelled=False) is True


def test_local_scope_with_results_shows_nothing():
    h = _Host('local')
    assert h._update_local_scope_strip(3) is False
    assert h.local_scope_strip.visible is False


def test_the_recorded_run_scope_wins_over_the_live_combo():
    # The user moved the combo to Genizah while the LOCAL run was still going.
    h = _Host(run_corpus='local', combo_corpus='genizah')
    assert h._update_local_scope_strip(0) is True
    # ...and the other way round: the run was Genizah, the combo now says Local.
    h = _Host(run_corpus='genizah', combo_corpus='local')
    assert h._update_local_scope_strip(0) is False


def test_falls_back_to_the_combo_when_no_run_was_recorded():
    h = _Host('local')
    h._current_search_run = None
    assert h._search_run_corpus() == 'local'
    del h.corpus_scope_combo
    assert h._search_run_corpus() == 'genizah'


def test_missing_strip_widget_is_tolerated():
    h = _Host('local')
    del h.local_scope_strip
    assert h._update_local_scope_strip(0) is True   # rule still evaluated
    h._set_local_scope_strip_visible(False)          # no AttributeError


# ---------------------------------------------------------------- the button

def test_button_switches_scope_to_genizah_hides_the_strip_and_reruns():
    h = _Host('local')
    h.local_scope_strip.visible = True
    h._search_genizah_instead()
    assert h.corpus_scope_combo.currentData() == 'genizah'
    assert h.corpus_scope_combo.set_calls == [_Combo.ITEMS.index('genizah')]
    assert h.local_scope_strip.visible is False
    assert h.started == 1


def test_button_does_not_touch_a_combo_already_on_genizah():
    h = _Host(run_corpus='local', combo_corpus='genizah')
    h._search_genizah_instead()
    assert h.corpus_scope_combo.set_calls == []
    assert h.started == 1


# ---------------------------------------------------------------- the wiring

def test_start_search_hides_the_hint_before_running():
    src = inspect.getsource(APP.start_search)
    assert 'self._set_local_scope_strip_visible(False)' in src
    # ...and does so before any worker is bound.
    assert (src.index('_set_local_scope_strip_visible(False)')
            < src.index('_drain_previous_worker'))


def test_zero_result_branch_of_on_search_finished_evaluates_the_hint():
    src = inspect.getsource(APP.on_search_finished)
    m = re.search(r"if not results:\s*\n\s*self\.reset_ui\(\)\s*\n\s*"
                  r"self\._update_local_scope_strip\(0, cancelled=was_cancelled\)", src)
    assert m, "the hint must be decided right where the empty result set is handled"
    # ...and the cancelled flag it passes is the one computed just above.
    assert src.index("was_cancelled = getattr(self, '_search_was_cancelled'") < m.start()


def test_reset_ui_hides_the_hint_so_the_new_button_clears_it():
    """Codex (PR #343): New runs _reset_search, which never re-searches or
    changes scope, so the strip from the previous run stayed. reset_ui is
    the funnel every exit path reaches, _reset_search included."""
    assert 'self._set_local_scope_strip_visible(False)' in inspect.getsource(APP.reset_ui)
    assert 'self.reset_ui()' in inspect.getsource(APP._reset_search)
    # and the zero-result branch decides AFTER reset_ui has hidden it
    src = inspect.getsource(APP.on_search_finished)
    assert src.index('self.reset_ui()') < src.index('_update_local_scope_strip(0')


def test_tag_search_results_hide_the_hint():
    """A tag search bypasses start_search and is never LOCAL."""
    src = inspect.getsource(APP._on_tag_search_results)
    assert 'self._set_local_scope_strip_visible(False)' in src


def test_tag_search_launch_hides_the_hint():
    """Codex CLI (PR #343): hiding only when tag RESULTS arrive left the old
    strip actionable for the whole tag search."""
    src = inspect.getsource(APP._execute_tag_search)
    assert 'self._set_local_scope_strip_visible(False)' in src
    assert (src.index('_set_local_scope_strip_visible(False)')
            < src.index('_pgp_tag_search_worker'))


def test_history_restore_hides_the_hint_on_entry():
    """History restores query and scope with signals blocked, and its re-run
    may be deferred or skipped."""
    src = inspect.getsource(APP._restore_regular_search_from_state)
    assert 'self._set_local_scope_strip_visible(False)' in src


def test_new_blocks_a_late_zero_result_completion_from_reshowing_the_hint():
    """A LOCAL completion already queued when New was clicked arrives AFTER
    the reset and would otherwise re-show the strip on the fresh screen."""
    h = _Host('local')
    h._local_scope_hint_blocked = True         # what _reset_search sets
    assert h._update_local_scope_strip(0) is False
    h._local_scope_hint_blocked = False        # what start_search sets
    assert h._update_local_scope_strip(0) is True
    reset_src = inspect.getsource(APP._reset_search)
    assert 'self._local_scope_hint_blocked = True' in reset_src
    start_src = inspect.getsource(APP.start_search)
    assert 'self._local_scope_hint_blocked = False' in start_src


def _top_level_statements(fn):
    """The direct statements of a method body (nothing nested in if/try/for),"""
    import ast
    import textwrap
    tree = ast.parse(textwrap.dedent(inspect.getsource(fn)))
    body = tree.body[0].body
    return [ast.unparse(stmt) for stmt in body]


def _has_unconditional(fn, fragment):
    """True only if a SIMPLE statement (assignment or bare call) at the top level
    of the method body contains the fragment. ast.unparse() of an `if` or `try`
    includes its nested body, so matching every top-level statement would let
    a hide moved under `if not tag:` still count (Codex, PR #343 round 3)."""
    import ast
    import textwrap
    tree = ast.parse(textwrap.dedent(inspect.getsource(fn)))
    return any(isinstance(stmt, (ast.Assign, ast.Expr))
               and fragment in ast.unparse(stmt)
               for stmt in tree.body[0].body)


def test_the_hide_and_block_statements_are_unconditional():
    """Codex CLI (PR #343, round 2): each of these survived as a 'guarded by the
    wrong condition' mutation because the tests only checked source membership.
    They must be direct statements of the method body, not nested in an if."""
    assert _has_unconditional(APP._reset_search, '_local_scope_hint_blocked = True')
    assert _has_unconditional(APP.start_search, '_local_scope_hint_blocked = False')
    assert _has_unconditional(APP._execute_tag_search, '_set_local_scope_strip_visible(False)')
    assert _has_unconditional(APP._restore_regular_search_from_state,
                              '_set_local_scope_strip_visible(False)')
    assert _has_unconditional(APP.reset_ui, '_set_local_scope_strip_visible(False)')
    assert _has_unconditional(APP._on_tag_search_results, '_set_local_scope_strip_visible(False)')


def test_block_is_set_before_the_running_check_and_cleared_after_the_query_guard():
    """New must block even when the worker already finished and its completion
    is merely queued; start_search must unblock for every real run."""
    reset = _top_level_statements(APP._reset_search)
    i_block = next(i for i, s in enumerate(reset) if '_local_scope_hint_blocked = True' in s)
    i_running = next(i for i, s in enumerate(reset) if 'search_thread.isRunning()' in s)
    assert i_block < i_running
    start = _top_level_statements(APP.start_search)
    i_unblock = next(i for i, s in enumerate(start) if '_local_scope_hint_blocked = False' in s)
    i_drain = next(i for i, s in enumerate(start) if '_drain_previous_worker' in s)
    assert i_unblock < i_drain


def test_manual_scope_change_hides_the_hint():
    src = inspect.getsource(APP._on_corpus_scope_changed)
    assert 'self._set_local_scope_strip_visible(False)' in src


def test_strip_is_built_above_the_results_table_and_wired_to_the_button():
    src = inspect.getsource(genizah_app)
    assert 'table_layout.addWidget(self.local_scope_strip)' in src
    assert ('self.btn_local_scope_search_genizah.clicked.connect(\n'
            '            self._search_genizah_instead)') in src
    # built before the table is added, so it sits above it
    assert (src.index('table_layout.addWidget(self.local_scope_strip)')
            < src.index('table_layout.addWidget(self.results_table)'))
    assert f'tr(\n            "{LABEL}")' in src or f'tr("{LABEL}")' in src
    assert f'tr("{BUTTON}")' in src


# ---------------------------------------------------------------- the colours

def _hue(hex_color):
    r, g, b = (int(hex_color[i:i + 2], 16) / 255 for i in (1, 3, 5))
    mx, mn = max(r, g, b), min(r, g, b)
    if mx == mn:
        return None
    if mx == r:
        h = (g - b) / (mx - mn) % 6
    elif mx == g:
        h = (b - r) / (mx - mn) + 2
    else:
        h = (r - g) / (mx - mn) + 4
    return h * 60


def _lightness(hex_color):
    r, g, b = (int(hex_color[i:i + 2], 16) for i in (1, 3, 5))
    return (max(r, g, b) + min(r, g, b)) / 2


def test_both_themes_use_an_orange_background_with_readable_text():
    """Owner (2026-09-17): brighter, light orange, in light AND dark mode."""
    for dark in (False, True):
        c = APP._local_scope_strip_colors(dark)
        assert 20 <= _hue(c['bg']) <= 45, (dark, c['bg'])          # orange
        assert 20 <= _hue(c['border']) <= 45, (dark, c['border'])
        # contrast direction follows the theme
        if dark:
            assert _lightness(c['text']) - _lightness(c['bg']) > 120
        else:
            assert _lightness(c['bg']) - _lightness(c['text']) > 120
    light, dark = (APP._local_scope_strip_colors(False),
                   APP._local_scope_strip_colors(True))
    assert _lightness(light['bg']) > 200, "light mode must read as LIGHT orange"
    assert light['bg'] != dark['bg']


def test_styling_follows_the_palette_lightness_probe():
    for dark in (False, True):
        h = _Host('local', dark=dark)
        h._style_local_scope_strip()
        expected = APP._local_scope_strip_colors(dark)
        assert expected['bg'] in h.local_scope_strip.stylesheet
        assert expected['border'] in h.local_scope_strip.stylesheet
        assert expected['text'] in h.local_scope_strip_label.stylesheet
        assert 'QFrame#localScopeStrip' in h.local_scope_strip.stylesheet


def test_showing_the_strip_restyles_it_for_the_current_theme():
    h = _Host('local', dark=True)
    h._set_local_scope_strip_visible(True)
    assert APP._local_scope_strip_colors(True)['bg'] in h.local_scope_strip.stylesheet
    h._dark = False
    h._set_local_scope_strip_visible(True)
    assert APP._local_scope_strip_colors(False)['bg'] in h.local_scope_strip.stylesheet


def test_strip_is_built_with_an_object_name_the_stylesheet_targets():
    src = inspect.getsource(genizah_app)
    assert 'self.local_scope_strip.setObjectName("localScopeStrip")' in src
    assert 'self._style_local_scope_strip()' in src


# ---------------------------------------------------------------- the strings

def test_both_strings_have_hebrew_translations():
    hebrew = re.compile(r'[֐-׿]')
    for key in (LABEL, BUTTON):
        assert key in gt.TRANSLATIONS, key
        assert hebrew.search(gt.TRANSLATIONS[key]), key
