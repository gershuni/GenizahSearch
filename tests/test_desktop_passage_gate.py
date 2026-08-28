# -*- coding: utf-8 -*-
"""Phase 146 Task 11 (non-gui lane): the desktop letter-level gate and the
persistence validators.

Deliberately Qt-FREE. Every function under test is a method on GenizahGUI
that touches only attributes and widget-shaped objects, so it is exercised by
binding the UNBOUND method to a stub -- no QApplication, no event loop, and
therefore none of the segfault risk that puts a test in the gui lane. The gui
lane is for tests that genuinely need a window; these do not.
"""
from __future__ import annotations

import ast
import io
import os
import re
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import desktop.passage_lifecycle as pl        # noqa: E402
import genizah_app                            # noqa: E402

APP = genizah_app.GenizahGUI
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


# ---------------------------------------------------------------------------
# Widget-shaped stubs. Only the surface the code under test actually uses.
# ---------------------------------------------------------------------------

class _Combo:
    def __init__(self, pairs, index=0):
        self._pairs = list(pairs)          # [(data, label), ...]
        self._index = index
        self.enabled = True
        self.visible = True
        self.tooltip = ''
        self.style = ''

    def count(self):
        return len(self._pairs)

    def itemData(self, i):
        return self._pairs[i][0]

    def itemText(self, i):
        return self._pairs[i][1]

    def currentData(self):
        if 0 <= self._index < len(self._pairs):
            return self._pairs[self._index][0]
        return None

    def currentIndex(self):
        return self._index

    def findData(self, value):
        for i, (data, _) in enumerate(self._pairs):
            if data == value:
                return i
        return -1

    def setCurrentIndex(self, i):
        self._index = i

    def blockSignals(self, _b):
        return False

    def setEnabled(self, b):
        self.enabled = bool(b)

    def setVisible(self, b):
        self.visible = bool(b)

    def setToolTip(self, s):
        self.tooltip = s

    def setStyleSheet(self, s):
        self.style = s


class _Label:
    def __init__(self):
        self.text = ''
        self.visible = False

    def setText(self, s):
        self.text = s

    def setVisible(self, b):
        self.visible = bool(b)

    def setStyleSheet(self, _s):
        pass

    def setWordWrap(self, _b):
        pass


class _Toggle:
    def __init__(self, checked=False):
        self._checked = checked

    def isChecked(self):
        return self._checked


class _Engine:
    """Stands in for SearchEngine. `index`/`searcher` are the attributes the
    gate must read -- `close_index` nulls them ON the engine while the window
    keeps holding the engine object."""

    def __init__(self, ready=True):
        self.index = object() if ready else None
        self.searcher = object() if ready else None


class _Win:
    """Borrows the real methods rather than re-implementing them. The
    functions under test call back into `self`, so a bare stub object would
    only prove that an AttributeError is raised."""
    _passage_disabled_reason_now = APP._passage_disabled_reason_now
    _restored_provenance_is_valid = APP._restored_provenance_is_valid
    _passage_reason_text = APP._passage_reason_text
    _comp_passage_axis = APP._comp_passage_axis
    _comp_axis_label = APP._comp_axis_label
    _comp_export_settings_lines = APP._comp_export_settings_lines
    _comp_chunk_preference = APP._comp_chunk_preference
    _PASSAGE_FORCED_CONTROLS = APP._PASSAGE_FORCED_CONTROLS
    _passage_scan_in_flight = APP._passage_scan_in_flight
    _passage_batch_in_flight = APP._passage_batch_in_flight
    _witness_notify = APP._witness_notify
    _refresh_witness_panel = APP._refresh_witness_panel
    _comp_witness_state = APP._comp_witness_state
    _witness_dialog_is_open = APP._witness_dialog_is_open
    _reset_composition = APP._reset_composition
    _retry_pending_reset = APP._retry_pending_reset
    _refuse_stop_during_passage_scan = APP._refuse_stop_during_passage_scan
    # Borrowed, not stubbed: Reset clears the auto-expand state before it
    # defers, and a stand-in would let that call vanish silently.
    _stop_auto_expand = APP._stop_auto_expand
    _on_pause_clicked = APP._on_pause_clicked
    _on_passage_build_finished = APP._on_passage_build_finished
    _passage_snapshot_must_wait = APP._passage_snapshot_must_wait
    _display_restored_comp_snapshot = APP._display_restored_comp_snapshot
    _comp_method = APP._comp_method
    _restore_comp_passage_preferences = APP._restore_comp_passage_preferences
    _method_help_text = APP._method_help_text
    _update_comp_method_help = APP._update_comp_method_help
    _update_comp_method_affordance = APP._update_comp_method_affordance
    _METHOD_COMBO_ACCENT = APP._METHOD_COMBO_ACCENT
    _update_witness_affordance = APP._update_witness_affordance
    _witness_button_accent = APP._witness_button_accent
    _accent_amber_pair = APP._accent_amber_pair
    _text_position_accent = APP._text_position_accent
    _composition_tab_is_current = APP._composition_tab_is_current
    _honour_deferred_comp_method = APP._honour_deferred_comp_method
    _announcement_allowed = APP._announcement_allowed
    _mark_announcement_seen = APP._mark_announcement_seen
    _retire_announcement = APP._retire_announcement
    _ANNOUNCE_METHOD = APP._ANNOUNCE_METHOD
    _ANNOUNCE_WITNESSES = APP._ANNOUNCE_WITNESSES
    # Borrowed, not stubbed: the letter-level default is applied from two
    # handlers this file already drives, and a stand-in would let either call
    # vanish without a test noticing.
    _apply_default_comp_method = APP._apply_default_comp_method


def _unannounced(w):
    """Cut a stub window off from the real `config.pkl`.

    The one-shot announcements live in the owner's own app config, so a test
    that let them read it would pass or fail by whether the owner had ever
    seen the badge -- green here, red on a fresh machine, and neither result
    about the code. Both caches are pre-seeded: allowed, and already
    written, so nothing reaches the disk in either direction.
    """
    w._announcements_allowed = {APP._ANNOUNCE_METHOD: True,
                                APP._ANNOUNCE_WITNESSES: True}
    w._announcements_written = {APP._ANNOUNCE_METHOD, APP._ANNOUNCE_WITNESSES}
    return w


def _window(scope='genizah', lab=False, engine_ready=True, building=False):
    w = _unannounced(_Win())
    w.searcher = _Engine(engine_ready)
    w.comp_corpus_scope_combo = _Combo(
        [('genizah', 'G'), ('local', 'L'), ('all', 'A')],
        index={'genizah': 0, 'local': 1, 'all': 2}[scope])
    w.btn_lab_mode_toggle_comp = _Toggle(lab)
    w._passage_build_in_flight = building
    w._comp_corpus_scope = scope
    return w


# ---------------------------------------------------------------------------
# The readiness gate.
# ---------------------------------------------------------------------------

@pytest.mark.parametrize('ready,scope,lab,building,engine_ok,expected', [
    (True,  'genizah', False, False, True,  None),
    (True,  'local',   False, False, True,  pl.REASON_SCOPE),
    (True,  'all',     False, False, True,  pl.REASON_SCOPE),
    (False, 'genizah', False, False, True,  pl.REASON_NOT_BUILT),
    (True,  'genizah', True,  False, True,  pl.REASON_LAB_ACTIVE),
    (True,  'genizah', False, True,  True,  pl.REASON_BUILD_IN_FLIGHT),
    (True,  'genizah', False, False, False, pl.REASON_MAIN_INDEX_MISSING),
    # Scope outranks everything: a My Library search is the wrong surface for
    # a Genizah-only artifact no matter what else is true.
    (False, 'local',   True,  True,  False, pl.REASON_SCOPE),
])
def test_the_gate_truth_table(monkeypatch, ready, scope, lab, building,
                              engine_ok, expected):
    monkeypatch.setattr(pl, 'passage_available', lambda: ready)
    w = _window(scope=scope, lab=lab, engine_ready=engine_ok, building=building)
    assert APP._passage_disabled_reason_now(w) == expected


def test_the_gate_reads_the_engines_attributes_not_the_engine_object(
        monkeypatch):
    """`SearchEngine.close_index` sets `index = None; searcher = None` ON the
    engine; the window's own reference stays non-None for the whole re-index.
    A gate written as `self.searcher is not None` reads READY across that
    entire window, and every passage row's text fetch then returns None."""
    monkeypatch.setattr(pl, 'passage_available', lambda: True)
    w = _window()
    assert APP._passage_disabled_reason_now(w) is None
    w.searcher.index = None        # exactly what close_index does
    w.searcher.searcher = None
    assert w.searcher is not None, 'the engine OBJECT is still held'
    assert APP._passage_disabled_reason_now(w) == pl.REASON_MAIN_INDEX_MISSING


def test_every_reason_key_has_translated_text():
    """A reason with no text shows an empty label, which reads as a silent
    refusal -- the selection snaps back and nothing says why."""
    w = _Win()
    for key in (pl.REASON_SCOPE, pl.REASON_NOT_BUILT, pl.REASON_LAB_ACTIVE,
                pl.REASON_BUILD_IN_FLIGHT, pl.REASON_MAIN_INDEX_MISSING):
        text = APP._passage_reason_text(w, key)
        assert text and text.strip(), ('no text for reason key', key)


# ---------------------------------------------------------------------------
# Provenance: which saved snapshots may still be shown.
# ---------------------------------------------------------------------------

def test_a_pre_v9_snapshot_without_provenance_is_never_discarded(monkeypatch):
    """MIGRATION. Sessions written before this phase carry no provenance at
    all; they are chunk by definition. Discarding them would delete a user's
    restored results for lacking fields that did not exist when they were
    written."""
    monkeypatch.setattr(pl, 'passage_available', lambda: False)
    w = _window(scope='local')          # the least favourable live state
    assert APP._restored_provenance_is_valid(w, {'results': [1, 2, 3]}) is True


def test_a_chunk_snapshot_survives_a_state_that_forbids_passage(monkeypatch):
    monkeypatch.setattr(pl, 'passage_available', lambda: False)
    w = _window(scope='local', lab=True)
    assert APP._restored_provenance_is_valid(
        w, {'last_result_method': 'chunk'}) is True


def test_a_passage_snapshot_stamped_local_is_discarded(monkeypatch):
    """Not merely stale -- impossible. Re-displaying it would attribute rows
    to a search the app would refuse to run."""
    monkeypatch.setattr(pl, 'passage_available', lambda: True)
    w = _window(scope='genizah')        # the LIVE scope is fine...
    assert APP._restored_provenance_is_valid(
        w, {'last_result_method': 'passage',
            'last_result_scope': 'local'}) is False, (
        'the stamped scope is what describes these rows, not the live combo')


def test_a_passage_snapshot_is_kept_when_it_could_still_run(monkeypatch):
    monkeypatch.setattr(pl, 'passage_available', lambda: True)
    w = _window(scope='local')          # live scope has since changed...
    assert APP._restored_provenance_is_valid(
        w, {'last_result_method': 'passage',
            'last_result_scope': 'genizah'}) is True, (
        'a genizah-stamped snapshot must survive the user changing scope')


def test_a_passage_snapshot_is_discarded_when_the_index_is_gone(monkeypatch):
    monkeypatch.setattr(pl, 'passage_available', lambda: False)
    w = _window(scope='genizah')
    assert APP._restored_provenance_is_valid(
        w, {'last_result_method': 'passage',
            'last_result_scope': 'genizah'}) is False


def test_a_garbage_method_stamp_is_refused(monkeypatch):
    monkeypatch.setattr(pl, 'passage_available', lambda: True)
    w = _window()
    assert APP._restored_provenance_is_valid(
        w, {'last_result_method': 'sideways'}) is False


# ---------------------------------------------------------------------------
# Policy axes: each validated on its own.
# ---------------------------------------------------------------------------

def _axis_window():
    w = _unannounced(_Win())
    w.comp_passage_width_combo = _Combo(
        [('standard-40', ''), ('wide-40', ''), ('wider-40', ''),
         ('widest-40', ''), ('max-40', '')], index=3)
    w.comp_passage_length_combo = _Combo([('normal', ''), ('short', '')])
    w.comp_passage_depth_combo = _Combo(
        [('normal', ''), ('deep', ''), ('deepest', '')])
    return w


def test_the_axis_defaults_match_the_web_surface():
    w = _axis_window()
    assert APP._comp_passage_axis(w, 'width') == 'widest-40'
    assert APP._comp_passage_axis(w, 'length') == 'normal'
    assert APP._comp_passage_axis(w, 'depth') == 'normal'


def test_an_unaddressable_axis_degrades_to_its_own_default():
    """`compose` RAISES on an unknown preset, so an axis that cannot name a
    value must be caught here -- and it must cost only its OWN axis.

    Note what this does NOT test: putting a bogus string into the combo's
    own item list makes that string a valid option by construction, so the
    membership check could never reject it. The reachable failure is an
    index that addresses no item, which is what a partially-built or
    cleared combo looks like."""
    w = _axis_window()
    w.comp_passage_width_combo._index = 99
    assert APP._comp_passage_axis(w, 'width') == 'widest-40'
    assert APP._comp_passage_axis(w, 'length') == 'normal', (
        'a bad width axis took the length axis down with it')


def test_a_persisted_axis_value_that_is_not_an_option_is_ignored():
    """Where the real independent validation lives: a stale or hand-edited
    persisted value has no matching option, so the axis keeps its current
    value instead of being set to something `compose` would raise on."""
    w = _axis_window()
    w.comp_method_combo = _Combo([('chunk', ''), ('passage', '')])
    w._apply_passage_mode_ui = lambda _on: None
    w._show_passage_reason = lambda _k: None
    w._refresh_comp_method_enabled = lambda: None
    APP._restore_comp_passage_preferences(w, {
        'comp_method': 'chunk',
        'comp_passage_width': 'nonsense-99',
        'comp_passage_depth': 'deepest',
    })
    assert APP._comp_passage_axis(w, 'width') == 'widest-40', (
        'an unknown persisted width was written into the widget anyway')
    assert APP._comp_passage_axis(w, 'depth') == 'deepest', (
        'a good axis was discarded along with the bad one')


def test_a_missing_axis_widget_still_yields_its_default():
    w = _Win()
    for axis in ('width', 'length', 'depth'):
        assert APP._comp_passage_axis(w, axis) in (
            'widest-40', 'normal')


# ---------------------------------------------------------------------------
# Source-anchored: the choke point is actually used at every site the plan
# names. These are the checks that catch a caller quietly growing its own
# copy of the predicate.
# ---------------------------------------------------------------------------

def _app_source():
    return io.open(os.path.join(REPO_ROOT, 'genizah_app.py'),
                   encoding='utf-8').read()


def _function_source(name):
    src = _app_source()
    tree = ast.parse(src)
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == name:
            return ast.get_source_segment(src, node) or ''
    raise AssertionError('%s not found in genizah_app.py' % name)


def test_dispatch_revalidates_the_method_through_the_wrapper():
    seg = _function_source('run_composition')
    assert '_passage_disabled_reason_now(' in seg, (
        'run_composition must re-check the gate: a restore path, a scope '
        'flip or a re-index can leave passage selected when it is no longer '
        'allowed, and dispatch is the last place that can still fall back '
        'to chunk instead of raising inside a worker thread')


def test_the_restore_path_revalidates_too():
    seg = _function_source('_restore_comp_passage_preferences')
    assert '_passage_disabled_reason_now(' in seg


def test_recursive_search_branches_by_engine_rather_than_refusing():
    """"Recursive" means something different per engine, and both meanings
    are correct for their own engine.

    Chunk decomposes a query into independent per-chunk lookups with no
    shared budget, so concatenating result texts is exactly equivalent to a
    union -- measured, 392 manuscripts both ways with an empty difference in
    both directions. The passage engine spends a per-query POSTING budget, so
    the same concatenation starves: 48.2% against 74.1% fused, and below the
    56.7% of the best SINGLE witness.

    This replaces a guard that asserted the function REFUSED in passage mode.
    That assertion still passes against the branching version -- both
    `_comp_method()` and `'passage'` appear either way -- so it had stopped
    testing anything it claimed to.
    """
    seg = _function_source('run_recursive_composition')
    assert "_run_auto_expand()" in seg, (
        'the passage branch no longer runs rank-fused expansion')
    assert "combined_text" in seg, (
        'the chunk branch no longer concatenates -- that is correct for it')
    # The passage branch must RETURN before reaching the concatenation.
    passage_at = seg.index("'passage'")
    concat_at = seg.index("combined_text")
    assert passage_at < concat_at, (
        'a passage run falls through into the concatenating path')


def test_the_retired_refusal_string_is_gone_from_the_code():
    """The button is no longer disabled in letter-level mode, so the message
    explaining why it was has no caller. A string left behind outlives the
    behaviour it described and reappears in the next translation sweep."""
    assert 'Recursive search uses chunk search for now' not in _app_source()
    import genizah_translations
    assert not [k for k in genizah_translations.TRANSLATIONS
                if 'Recursive search uses chunk search' in k], (
        'the retired message is still in the translation table')


def test_the_close_check_runs_before_any_shutdown_state_is_set():
    """Deferring AFTER `_app_shutting_down = True` leaves a running app whose
    telemetry and session-save paths are already disarmed."""
    seg = _function_source('closeEvent')
    defer = seg.index('self._defer_close_for_passage')
    # Match the STATEMENT, not the string: the comment above it quotes
    # `_app_shutting_down = True` verbatim, and a bare `.index()` finds the
    # comment first -- which made this gate fail against correct code.
    match = re.search(r'^\s*self\._app_shutting_down = True$', seg,
                      re.MULTILINE)
    assert match is not None, 'the shutdown flag statement moved or changed'
    flag = match.start()
    assert defer < flag, (
        'the busy-worker check must run before the shutdown flag is set')


def test_this_feature_never_terminates_a_thread():
    """Terminating a passage scan kills a thread holding live memory
    mappings; terminating a build strands a multi-GB staging directory."""
    for name in ('_defer_close_for_passage', '_retry_pending_close',
                 '_passage_workers_busy', '_finish_passage_build',
                 '_start_passage_build_worker'):
        seg = _function_source(name)
        assert 'terminate()' not in seg, (name, 'calls terminate()')


def test_the_stop_paths_all_route_through_the_one_guard():
    for name in ('toggle_composition', 'cancel_composition',
                 '_reset_composition'):
        seg = _function_source(name)
        assert '_refuse_stop_during_passage_scan()' in seg, (
            name, 'does not consult the passage stop guard')


def test_the_release_seam_forwards_the_generation():
    """The late-close guard is only as good as the seam's cooperation: a seam
    that accepts `expect_generation` and then calls `close_passage_state()`
    with no argument compiles, runs, and silently reinstates the bug."""
    seg = _function_source('_on_passage_release_requested')
    assert 'close_passage_state(' in seg
    assert 'expect_generation' in seg, (
        'the seam drops the generation it was handed')


def test_the_telemetry_method_prop_is_allowlisted_and_emitted():
    """End-to-end or not at all: the chokepoint silently drops any property
    not on the allowlist, so emitting without listing ships a scrubbed
    field that looks wired."""
    from desktop import telemetry
    assert 'method' in telemetry._ALLOWED_PROPS
    seg = _function_source('_emit_comp_search_telemetry')
    assert "'method'" in seg


# ---------------------------------------------------------------------------
# Shared vocabulary. Owner directive 2026-08-26: the web already has strings
# for this feature -- use them, and if we change one, change both. Both
# surfaces read the SAME genizah_translations.TRANSLATIONS, so identical
# English is what makes that true mechanically rather than by discipline.
# ---------------------------------------------------------------------------

def _tr_strings(rel_path):
    src = io.open(os.path.join(REPO_ROOT, rel_path), encoding='utf-8').read()
    out = set()
    for node in ast.walk(ast.parse(src)):
        if (isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
                and node.func.id == 'tr' and node.args):
            a = node.args[0]
            if isinstance(a, ast.Constant) and isinstance(a.value, str):
                out.add(a.value)
    return out


# The strings BOTH surfaces show for the same thing. Not a third copy to
# drift: the test asserts each one is present in both files, so editing
# either surface alone fails here and names the other.
_SHARED_WITH_WEB = (
    # Method selector items
    'Chunk search (slower)',
    'New! Letter-level search',
    # Per-method help lines
    'Faster, with fewer irrelevant results.',
    'The older method. Slower, but offers Exact / Variants / Fuzzy modes '
    'and cross-paragraph filtering.',
    # Policy axis labels and items
    'Match width', 'Passage length', 'Search depth',
    'Narrow (near-exact)', 'Medium width', 'Wide width',
    'Very wide (default)', 'Maximal (may add noise)',
    'Normal passages (default)', 'Also short passages',
    'Normal (fast, default)', 'Deep (slower, more witnesses)',
    'Deepest (slowest, most)',
    # The width/length/depth tooltips, ported verbatim
    'How far a manuscript may drift from your text and still match. Wider '
    'finds more noisy witnesses; the strongest matches always rank first.',
    'How much of the corpus the search may examine. Deeper searches take '
    'seconds longer and return more manuscripts \u2014 including badly '
    'damaged and reworked copies a fast pass misses. Long texts benefit '
    'the most.',
)


# The six that predate this work and are tracked separately. Listed rather
# than pattern-matched so adding a seventh is a deliberate act.
_UNTRANSLATED_BEFORE_THIS_WORK = {
    'Filter Local', 'JSON', 'No Local', 'Only Local', 'PGP',
    'Library Hide filter not applied to search/composition',
}


@pytest.mark.parametrize('rel_path', ['genizah_app.py',
                                      os.path.join('web', 'pages',
                                                   'parallels.py')])
def test_every_tr_string_has_a_hebrew_entry(rel_path):
    """`tr()` returns its ARGUMENT when the table has no entry, so a missing
    translation is invisible on an English machine and shows up as a lone
    English control on a Hebrew one. Nothing fails; it just reads wrong.

    That is how the web shipped "Witness text is too long (max {cap}
    characters)" untranslated across three call sites -- found only when the
    desktop needed the same string.

    Both files, because these two surfaces share one table: an English string
    added to either without an entry is the same defect.
    """
    import genizah_translations

    used = _tr_strings(rel_path)
    missing = sorted(
        s for s in used
        if s not in genizah_translations.TRANSLATIONS
        and s.isascii()                       # Hebrew literals are their own text
        and s not in _UNTRANSLATED_BEFORE_THIS_WORK
    )
    assert missing == [], (
        'these tr() strings have no Hebrew and will render in English on a '
        'Hebrew machine: %r' % (missing,))


@pytest.mark.parametrize('shared', _SHARED_WITH_WEB)
def test_the_desktop_and_web_say_the_same_thing(shared):
    """Identical English on both surfaces, so one TRANSLATIONS entry serves
    both and a reworded string cannot land on one surface only.

    If this fails, the fix is almost never to edit this list -- it is to make
    the two surfaces agree again."""
    web = _tr_strings(os.path.join('web', 'pages', 'parallels.py'))
    app = _tr_strings('genizah_app.py')
    assert shared in web, (
        'the web no longer uses this string; if it was reworded there, the '
        'desktop needs the same rewording')
    assert shared in app, (
        'the desktop no longer uses this string; the two surfaces have '
        'drifted into separate vocabularies for one feature')


def test_the_desktop_help_line_is_per_method_not_one_tooltip():
    """A single tooltip on a two-option control describes BOTH options --
    which is how the web ended up explaining letter-level search to someone
    hovering "Chunk search" (owner-reported 2026-08-25). The desktop combo
    has the same flaw, so it uses the same fix: a live line that shows the
    selected method's own text."""
    seg = _function_source('_method_help_text')
    assert "'passage'" in seg, 'the help text does not branch on the method'
    app_src = _app_source()
    assert 'comp_method_combo.setToolTip' not in app_src, (
        'a combined tooltip on the method combo describes both methods; use '
        'the per-method help line instead')


def test_the_depth_tooltip_keeps_the_shared_sentence_separate():
    """The desktop adds one sentence the web does not have. Concatenating it
    into the shared string would fork that sentence into a second version
    that stops changing when the web's does."""
    app_src = _app_source()
    assert 'A deep search cannot be interrupted once it has ' in app_src, (
        'the desktop-only non-interruptibility warning is missing')
    web = _tr_strings(os.path.join('web', 'pages', 'parallels.py'))
    combined = [s for s in _tr_strings('genizah_app.py')
                if 'How much of the corpus' in s]
    assert combined, 'the shared depth tooltip is gone'
    for s in combined:
        assert s in web, (
            'the desktop grew its own variant of the depth tooltip instead '
            'of appending a separate string: %r' % (s[:80],))


# ---------------------------------------------------------------------------
# Task 10: the Hebrew gate. Both directions, because both fail silently --
# an untranslated string shows English inside a Hebrew UI, and a dead entry
# is a string someone renamed on one side only.
# ---------------------------------------------------------------------------

_PHASE146_BLOCK_MARKER = '# --- Phase 146: desktop letter-level (passage) search'
_HEBREW = re.compile(r'[\u0590-\u05FF]')


def _phase146_translation_keys():
    """The keys of the Phase 146 block in genizah_translations.py, read from
    the source rather than by diffing against a base commit -- this has to
    keep working long after the branch is merged."""
    src = io.open(os.path.join(REPO_ROOT, 'genizah_translations.py'),
                  encoding='utf-8').read()
    start = src.index(_PHASE146_BLOCK_MARKER)
    block = src[start:]
    tree = ast.parse(block[block.index('TRANSLATIONS.update('):])
    call = tree.body[0].value
    return [(k.value, v.value) for k, v in zip(call.args[0].keys,
                                               call.args[0].values)]


def test_every_phase146_string_has_real_hebrew():
    pairs = _phase146_translation_keys()
    assert len(pairs) >= 30, ('the Phase 146 block shrank unexpectedly: %d'
                              % len(pairs))
    for en, he in pairs:
        assert _HEBREW.search(he), (
            'no Hebrew characters in the translation of %r -- an untranslated '
            'entry shows English inside a Hebrew UI, which is worse than a '
            'missing key because nothing reports it' % en[:60])
        assert he != en, ('translation is identical to the English', en[:60])


def test_no_phase146_translation_is_dead():
    """A key nothing calls is a string that was reworded on one side only."""
    used = _tr_strings('genizah_app.py') | _tr_strings(
        os.path.join('desktop', 'settings_dialogs.py'))
    for en, _he in _phase146_translation_keys():
        assert en in used, (
            'nothing calls tr() with %r any more; if it was reworded, the '
            'translation entry needs the same rewording' % en[:60])


def test_the_placeholder_counts_match():
    """`{}` counts must agree, or `.format()` raises at runtime in Hebrew
    only -- a crash no English-locale test run would ever see."""
    for en, he in _phase146_translation_keys():
        assert en.count('{}') == he.count('{}'), (
            'placeholder count differs for %r: %d vs %d'
            % (en[:50], en.count('{}'), he.count('{}')))


def test_the_disk_check_runs_before_the_confirmation():
    """Owner directive 2026-08-26: check the drive first. The worker's own
    preflight is authoritative but runs AFTER the user has approved an 11 GB
    build, so being refused there means being asked to consent to something
    that could never start."""
    seg = _function_source('run_passage_index_build')
    check = seg.index('disk_usage')
    promise = seg.index('QMessageBox.question')
    assert check < promise, (
        'the free-space check must run before the confirmation dialog')
    assert 'STAGING_DIRNAME' in seg, (
        'a crashed staging tree holds space the build reclaims; not counting '
        'it refuses every retry after a failure, forever')


# ---------------------------------------------------------------------------
# Startup wiring. Everything else was wired -- dispatch, the gate, the build --
# and none of it was reachable across a restart, because nothing opened the
# index at launch. `_state` was None on every start, so the gate said "not
# built" and offered to rebuild a 3.5 GB artifact already sitting on disk.
# ---------------------------------------------------------------------------

def test_startup_actually_opens_the_index():
    """The single call that makes the feature reachable at all."""
    seg = _function_source('on_startup_finished')
    assert '_start_passage_load()' in seg, (
        'startup never loads the letter-level index, so it can only ever be '
        'used in the session that built it')


def test_the_load_worker_recovers_and_opens():
    """`recover_at_startup` is both halves: it walks the candidates, promotes
    whichever actually opens, and returns that opened index. Calling
    `load_passage_state` instead would open the live directory without ever
    repairing a run that died mid-swap."""
    src = io.open(os.path.join(REPO_ROOT, 'desktop', 'passage_workers.py'),
                  encoding='utf-8').read()
    assert 'recover_at_startup' in src, (
        'the startup worker does not run recovery')
    tree = ast.parse(src)
    names = [n.name for n in ast.walk(tree)
             if isinstance(n, ast.ClassDef)]
    assert 'PassageLoadThread' in names


def test_the_loaded_index_is_installed(monkeypatch):
    installed = []
    monkeypatch.setattr(pl, 'install_passage_state',
                        lambda st: installed.append(st) or True)
    w = _Win()
    w._revalidate_comp_method = lambda: None
    w._maybe_offer_passage_build = lambda: None
    idx = object()
    result = type('R', (), {'index': idx, 'live_dir': 'd',
                            'status': 'live_ok'})()
    APP._on_passage_loaded(w, result)
    assert installed and installed[0].index is idx, (
        'the index the startup worker opened was never installed, so '
        'passage_available() stays False and the feature stays hidden')


def test_nothing_is_installed_when_there_is_no_index(monkeypatch):
    installed = []
    monkeypatch.setattr(pl, 'install_passage_state',
                        lambda st: installed.append(st) or True)
    w = _Win()
    w._revalidate_comp_method = lambda: None
    w._maybe_offer_passage_build = lambda: None
    result = type('R', (), {'index': None, 'live_dir': '',
                            'status': 'no_prior_state'})()
    APP._on_passage_loaded(w, result)
    assert installed == []


def test_a_restored_passage_preference_survives_the_load_race(monkeypatch):
    """The restore runs ~200ms after launch; the index finishes opening later.
    So a session saved with letter-level search selected is demoted to chunk
    before the index has had any chance to load, and the user's method is
    silently lost on EVERY launch unless the intent is remembered."""
    monkeypatch.setattr(pl, 'passage_available', lambda: True)
    monkeypatch.setattr(pl, 'install_passage_state', lambda st: True)
    w = _window(scope='genizah')
    w.comp_method_combo = _Combo([('chunk', ''), ('passage', '')], index=0)
    w._comp_method_deferred = 'passage'
    w._revalidate_comp_method = lambda: None
    w._maybe_offer_passage_build = lambda: None
    w._apply_passage_mode_ui = lambda _on: None
    w._show_passage_reason = lambda _k: None
    w._update_comp_method_help = lambda: None

    APP._on_passage_loaded(w, type('R', (), {
        'index': object(), 'live_dir': 'd', 'status': 'live_ok'})())

    assert w.comp_method_combo.currentData() == 'passage', (
        "the user's restored method was dropped because the index had not "
        'finished loading when the restore ran')
    assert w._comp_method_deferred is None, 'the deferral must be one-shot'


def test_only_a_not_built_demotion_is_deferred():
    """A demotion for scope or Lab Mode reflects real current state and must
    stand -- re-applying it later would override the user."""
    seg = _function_source('_restore_comp_passage_preferences')
    assert 'REASON_NOT_BUILT' in seg, (
        'the deferral is not restricted to the transient reason')


def test_the_close_check_tracks_the_load_worker():
    """It holds an open index and may be mid-rename inside recovery; exiting
    under it leaves exactly the half-swapped state recovery exists to fix."""
    seg = _function_source('_passage_workers_busy')
    assert 'passage_load_thread' in seg


# ---------------------------------------------------------------------------
# Use-before-create in the tab builders.
#
# This file is deliberately Qt-free, which means nothing here ever RUNS
# `create_composition_tab` -- and a widget added to a layout ~70 lines before
# the line that creates it crashed the app on launch while all 60 tests
# stayed green. A full GUI construction test would catch it, at the cost of a
# QApplication and real segfault risk in this lane. Reading the build order
# statically costs neither and catches the same class of defect across every
# tab builder in the file, not just the one that broke.
# ---------------------------------------------------------------------------

def _use_before_create(fn):
    """First read of `self.X` earlier in the function than its first
    assignment, for attributes the function itself assigns.

    Bodies of nested defs and lambdas are skipped: those run later (they are
    signal handlers), so a read there is not a build-order error."""
    nested = set()
    for n in ast.walk(fn):
        if n is not fn and isinstance(
                n, (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda)):
            for c in ast.walk(n):
                nested.add(id(c))
    stores, loads = {}, {}
    for n in ast.walk(fn):
        if id(n) in nested:
            continue
        if (isinstance(n, ast.Attribute) and isinstance(n.value, ast.Name)
                and n.value.id == 'self'):
            pos = (n.lineno, n.col_offset)
            if isinstance(n.ctx, ast.Store):
                if n.attr not in stores or pos < stores[n.attr]:
                    stores[n.attr] = pos
            elif isinstance(n.ctx, ast.Load):
                if n.attr not in loads or pos < loads[n.attr]:
                    loads[n.attr] = pos
    return [(a, loads[a][0], p[0]) for a, p in stores.items()
            if loads.get(a) and loads[a] < p]


def _tab_builders():
    tree = ast.parse(_app_source())
    return [n for n in ast.walk(tree)
            if isinstance(n, ast.FunctionDef)
            and n.name.startswith('create_') and n.name.endswith('_tab')]


def test_the_tab_builders_create_every_widget_before_using_it():
    builders = _tab_builders()
    assert len(builders) >= 5, 'the tab builders moved or were renamed'
    problems = []
    for fn in builders:
        for attr, read_line, made_line in _use_before_create(fn):
            problems.append(
                '%s: self.%s read at line %d but created at line %d'
                % (fn.name, attr, read_line, made_line))
    assert not problems, (
        'a widget is used before it exists, which is an AttributeError on '
        'launch and not something a Qt-free test can otherwise see:\n  '
        + '\n  '.join(problems))


# ---------------------------------------------------------------------------
# Attributes read but never assigned.
#
# `getattr(self, 'X', default)` turns a name that exists nowhere into a silent
# default instead of an AttributeError. Two real defects shipped this way:
# `startup_complete` (never set, so every build was refused with "still
# starting up") and `_comp_grouping_active` (read by the Stop guard and the
# combo-freeze logic, set by nobody, so the combo un-froze early and Stop was
# refused during grouping).
# ---------------------------------------------------------------------------

# Pre-existing and NOT introduced by this phase. Listed so the gate can go in
# without dragging an unrelated fix along; it is a real latent bug in the
# responsa path and deserves its own change.
_KNOWN_UNASSIGNED = {'_last_responsa_options'}


def _gui_class():
    tree = ast.parse(_app_source())
    return next(n for n in ast.walk(tree)
                if isinstance(n, ast.ClassDef) and n.name == 'GenizahGUI')


def test_no_attribute_is_read_via_getattr_but_never_assigned():
    cls = _gui_class()
    assigned = set()
    for n in ast.walk(cls):
        if (isinstance(n, ast.Attribute) and isinstance(n.value, ast.Name)
                and n.value.id == 'self' and isinstance(n.ctx, ast.Store)):
            assigned.add(n.attr)
    for n in cls.body:                       # class-level: signals, constants
        if isinstance(n, ast.Assign):
            for tg in n.targets:
                if isinstance(tg, ast.Name):
                    assigned.add(tg.id)
        if isinstance(n, ast.AnnAssign) and isinstance(n.target, ast.Name):
            assigned.add(n.target.id)

    unassigned = {}
    for n in ast.walk(cls):
        if (isinstance(n, ast.Call) and isinstance(n.func, ast.Name)
                and n.func.id == 'getattr' and len(n.args) >= 2
                and isinstance(n.args[0], ast.Name) and n.args[0].id == 'self'
                and isinstance(n.args[1], ast.Constant)
                and isinstance(n.args[1].value, str)):
            name = n.args[1].value
            if name not in assigned and name not in _KNOWN_UNASSIGNED:
                unassigned.setdefault(name, n.lineno)

    assert not unassigned, (
        'read via getattr with a default but never assigned anywhere in the '
        'class, so the default is the ONLY value it can ever have: '
        + ', '.join('self.%s (line %d)' % (k, v)
                    for k, v in sorted(unassigned.items())))


def test_the_grouping_flag_is_set_and_cleared():
    """It gates two different behaviours, so a stuck value breaks both: the
    method combo stays frozen through grouping, and the Stop guard is scoped
    to the SCAN so grouping keeps today's Stop behaviour."""
    src = _app_source()
    assert 'self._comp_grouping_active = True' in src, 'never set'
    assert src.count('self._comp_grouping_active = False') >= 2, (
        'cleared on fewer paths than it is set on, so it can stick on')
    for fn in ('on_comp_finished', 'on_grouping_error', 'reset_comp_ui'):
        assert '_comp_grouping_active = False' in _function_source(fn), (
            '%s leaves the grouping flag set' % fn)


# ---------------------------------------------------------------------------
# The startup offer.
# ---------------------------------------------------------------------------

class _NoDialog:
    def __init__(self, *a, **k):
        raise AssertionError(
            'a dialog was shown in a case that must be silent')


def _offer_window(monkeypatch, **kw):
    w = _window(**{k: v for k, v in kw.items()
                   if k in ('scope', 'lab', 'engine_ready', 'building')})
    w._maybe_offer_passage_build = APP._maybe_offer_passage_build
    monkeypatch.setattr(genizah_app, 'QMessageBox', _NoDialog)
    return w


@pytest.mark.parametrize('case', [
    'already_available', 'build_in_flight', 'main_index_missing',
    'already_shown', 'never_again',
])
def test_the_offer_stays_silent_when_it_would_be_useless(monkeypatch, case):
    """An offer the user cannot act on, or has already answered, is worse
    than no offer. The main-index case matters most: `on_startup_finished`
    already asks about that, and stacking a second modal on someone with no
    index at all is noise."""
    monkeypatch.setattr(pl, 'passage_available',
                        lambda: case == 'already_available')
    monkeypatch.setattr(genizah_app, 'load_app_config',
                        lambda: {'passage_build_prompt':
                                 'never' if case == 'never_again' else ''})
    w = _offer_window(monkeypatch,
                      engine_ready=(case != 'main_index_missing'),
                      building=(case == 'build_in_flight'))
    w._passage_offer_shown = (case == 'already_shown')
    APP._maybe_offer_passage_build(w)      # must not construct a dialog


def test_the_offer_is_silent_with_no_corpus_to_build_from(monkeypatch, tmp_path):
    monkeypatch.setattr(pl, 'passage_available', lambda: False)
    monkeypatch.setattr(genizah_app, 'load_app_config', lambda: {})
    monkeypatch.setattr(genizah_app.Config, 'FILE_V8',
                        str(tmp_path / 'absent.txt'))
    w = _offer_window(monkeypatch)
    w._passage_offer_shown = False
    APP._maybe_offer_passage_build(w)


def test_the_offer_only_fires_once_the_load_has_reported():
    """Before the load worker answers, "no index" means "not loaded yet", and
    offering there would prompt on every single launch."""
    seg = _function_source('_on_passage_loaded')
    assert '_maybe_offer_passage_build()' in seg


def test_build_now_does_not_ask_the_same_question_twice():
    """The offer already stated the figures and got an explicit yes; a second
    dialog with identical numbers reads as a bug.

    Checking that `if not confirm:` merely EXISTS is not enough -- emptying
    its body leaves that text in place and the branch falls through to the
    dialog anyway. The branch has to start the build and return."""
    assert 'def run_passage_index_build(self, confirm=True)' in _app_source()
    assert 'confirm=False' in _function_source('_maybe_offer_passage_build')

    src = _app_source()
    fn = next(n for n in ast.walk(ast.parse(src))
              if isinstance(n, ast.FunctionDef)
              and n.name == 'run_passage_index_build')
    branch = None
    for node in ast.walk(fn):
        if (isinstance(node, ast.If) and isinstance(node.test, ast.UnaryOp)
                and isinstance(node.test.op, ast.Not)
                and isinstance(node.test.operand, ast.Name)
                and node.test.operand.id == 'confirm'):
            branch = node
            break
    assert branch is not None, 'no `if not confirm:` branch'
    body = ast.dump(ast.Module(body=branch.body, type_ignores=[]))
    assert '_start_passage_build_worker' in body, (
        'the skip-confirmation branch does not actually start the build')
    assert any(isinstance(n, ast.Return) for n in ast.walk(branch)), (
        'the branch falls through to the confirmation dialog it exists to '
        'skip')


# ---------------------------------------------------------------------------
# Discoverability. "New!" lives inside the item text and chunk is the default,
# so the CLOSED control never showed it -- the feature was invisible unless a
# user happened to open the list.
# ---------------------------------------------------------------------------

def _affordance_window(monkeypatch, available, method='chunk', scope='genizah',
                       corpus_exists=True, tmp_path=None):
    monkeypatch.setattr(pl, 'passage_available', lambda: available)
    monkeypatch.setattr(
        genizah_app.Config, 'FILE_V8',
        __file__ if corpus_exists else str(tmp_path / 'absent.txt'))
    w = _window(scope=scope)
    w.comp_method_combo = _Combo([('chunk', ''), ('passage', '')],
                                 index=0 if method == 'chunk' else 1)
    w.lbl_comp_method_new = _Label()
    return w


def test_the_new_badge_shows_when_the_option_is_ready_and_unused(monkeypatch):
    w = _affordance_window(monkeypatch, available=True, method='chunk')
    APP._update_comp_method_affordance(w)
    assert w.lbl_comp_method_new.visible, (
        'nothing points at letter-level search while chunk is selected, so '
        'the closed dropdown never reveals it exists')
    assert 'border' in w.comp_method_combo.style, (
        'the combo carries no accent, so the badge is the only cue')


def test_the_badge_retires_itself_once_the_option_is_in_use(monkeypatch):
    w = _affordance_window(monkeypatch, available=True, method='passage')
    APP._update_comp_method_affordance(w)
    assert not w.lbl_comp_method_new.visible, (
        'the badge keeps advertising a method the user already switched to')
    assert 'border' not in w.comp_method_combo.style, (
        'the accent border outlives the reason for it')


def test_the_badge_shows_when_the_index_is_merely_unbuilt(monkeypatch):
    """One build away still counts: clicking now opens the build offer, which
    is exactly the path we want people to find."""
    w = _affordance_window(monkeypatch, available=False, method='chunk')
    APP._update_comp_method_affordance(w)
    assert w.lbl_comp_method_new.visible


def test_no_badge_when_switching_would_lead_nowhere(monkeypatch, tmp_path):
    """A My Library scope refuses letter-level search outright, so pointing
    at it would only produce a refusal."""
    w = _affordance_window(monkeypatch, available=True, method='chunk',
                           scope='local')
    APP._update_comp_method_affordance(w)
    assert not w.lbl_comp_method_new.visible

    w2 = _affordance_window(monkeypatch, available=False, method='chunk',
                            corpus_exists=False, tmp_path=tmp_path)
    APP._update_comp_method_affordance(w2)
    assert not w2.lbl_comp_method_new.visible, (
        'no corpus means the build offer cannot help either')


def test_clicking_the_unbuilt_option_offers_to_build_it():
    """A label pointing at Settings is a worse answer than the dialog that
    can just build it."""
    seg = _function_source('_on_comp_method_changed')
    assert 'REASON_NOT_BUILT' in seg and 'explicit=True' in seg, (
        'clicking letter-level search while unbuilt only refuses; it must '
        'offer the build')


def test_an_explicit_click_overrides_dont_ask_again():
    """Those guards exist to stop US nagging. The user just asked on purpose,
    so suppressing the dialog would make the control look broken."""
    seg = _function_source('_maybe_offer_passage_build')
    assert 'not explicit and getattr(self' in seg, (
        'the once-per-launch guard still suppresses an explicit click')
    assert "if not explicit:" in seg, (
        "'don't ask again' still suppresses an explicit click")


def test_the_offer_says_what_the_owner_asked_it_to_say():
    seg = _function_source('_maybe_offer_passage_build')
    for phrase in ('a new way to find parallels', 'Composition Search tab',
                   'much faster', 'far fewer unrelated results'):
        assert phrase in seg, ('the offer no longer says %r' % phrase)


# ---------------------------------------------------------------------------
# The affordance has to be REACHED, not merely correct.
#
# Every test above calls `_update_comp_method_affordance` directly, and it
# passed while the badge never appeared in the running app: the only caller
# that matters, `_revalidate_comp_method`, returned early when the method was
# not passage -- and chunk is the default, which is the exact case the badge
# exists for. Testing a unit is not testing its wiring.
# ---------------------------------------------------------------------------

def test_revalidate_updates_the_affordance_on_the_chunk_path(monkeypatch):
    monkeypatch.setattr(pl, 'passage_available', lambda: True)
    monkeypatch.setattr(genizah_app.Config, 'FILE_V8', __file__)
    w = _window(scope='genizah')
    w.comp_method_combo = _Combo([('chunk', ''), ('passage', '')], index=0)
    w.lbl_comp_method_new = _Label()
    w._refresh_comp_method_enabled = lambda: None

    APP._revalidate_comp_method(w)

    assert w.lbl_comp_method_new.visible, (
        'the badge is only updated on the passage path, so with chunk '
        'selected -- the default, and the whole point of the badge -- it '
        'never appears')


def test_the_method_row_leads_the_panel():
    """It decides what every control below it means. It was previously buried
    in the action row beside scope, Lab Mode, Deep Scan, Pause and Analyze,
    with its own help line under the PARAGRAPH controls two rows away."""
    seg = _function_source('create_composition_tab')
    method = seg.index('in_l.addLayout(method_row)')
    boundary = seg.index('in_l.addLayout(boundary_row)')
    passage = seg.index('in_l.addLayout(passage_row)')
    assert method < boundary, (
        'the paragraph controls come before the method that selects them')
    assert method < passage < boundary, (
        'the letter-level options are separated from the method they belong '
        'to')
    help_line = seg.index('in_l.addWidget(self.lbl_comp_method_help)')
    assert method < help_line < passage, (
        'the help line does not sit directly under the control it describes')


def test_the_method_controls_left_the_action_row():
    """That row already holds the corpus scope, Lab Mode, Deep Scan, Pause,
    Analyze, Full Recursive Search and New."""
    seg = _function_source('create_composition_tab')
    for widget in ('self.comp_method_combo', 'self.lbl_comp_method_caption',
                   'self.lbl_comp_method_new'):
        assert 'cr.addWidget(%s)' % widget not in seg, (
            '%s is back in the crowded action row' % widget)


# ---------------------------------------------------------------------------
# Build progress. The builder reports
# `progress('pass1', n_records_seen, n_records_indexed, elapsed)` -- so the
# second value is the INDEXED count, not a total, and pass 1 has no record
# total at all: nothing knows how many records a 1.47 GB file holds until it
# has been read. The file SIZE is the real denominator.
# ---------------------------------------------------------------------------

from desktop.passage_workers import PassageBuildThread   # noqa: E402
from shared.passage_corpus import iter_records           # noqa: E402


class _FakeThread:
    """Borrows the generator wrapper without a QThread or a QApplication."""
    _records_with_progress = PassageBuildThread._records_with_progress

    def __init__(self):
        self.emitted = []

    def _emit(self, phase, done, total, records=0):
        self.emitted.append((phase, done, total, records))


def _corpus(tmp_path, n_records=250, body_lines=40):
    p = tmp_path / 'corpus.txt'
    with io.open(str(p), 'w', encoding='utf-8', newline='\n') as fh:
        for i in range(n_records):
            fh.write('==> rec%04d <==\n' % i)
            for j in range(body_lines):
                fh.write('אבגדהוזחטי' * 6 + ' %d\n' % j)
    return str(p)


def test_the_progress_wrapper_yields_every_record_unchanged(tmp_path):
    """The wrapper sits between the corpus and the builder. A record dropped
    or altered here would silently produce a PARTIAL index that passes every
    structural check the loader makes -- the worst failure available in this
    subsystem, and invisible until a search quietly misses manuscripts."""
    path = _corpus(tmp_path)
    plain = list(iter_records(path))
    wrapped = list(_FakeThread()._records_with_progress(path))
    assert wrapped == plain, (
        'the progress wrapper changed what the builder receives')
    assert len(plain) == 250


def test_the_read_progress_is_a_real_fraction(tmp_path):
    path = _corpus(tmp_path)
    th = _FakeThread()
    list(th._records_with_progress(path))
    reads = [e for e in th.emitted if e[0] == 'read']
    assert reads, 'no read progress was reported at all'
    pcts = [e[1] for e in reads]
    assert pcts == sorted(pcts), 'progress went backwards: %r' % (pcts[:20],)
    assert max(pcts) <= 100, 'progress exceeded 100%%: %d' % max(pcts)
    assert reads[-1][1] == 100, 'the bar never reaches the end'
    assert reads[-1][2] == 100, 'the denominator is not a percentage scale'
    counts = [e[3] for e in reads]
    assert counts == sorted(counts) and counts[-1] == 250, (
        'the record count is not the number of records actually yielded')


def test_the_read_progress_is_throttled(tmp_path):
    """One signal per record would queue hundreds of thousands of events on
    the UI thread and make the dialog slower than the build it reports."""
    path = _corpus(tmp_path, n_records=250)
    th = _FakeThread()
    list(th._records_with_progress(path))
    reads = [e for e in th.emitted if e[0] == 'read']
    assert len(reads) <= 101 + 1, (
        'emitted %d progress events for 250 records -- not throttled to '
        'whole percents' % len(reads))


def test_pass1_from_the_builder_is_not_used_as_a_total():
    """Its second argument is n_records_indexed. Feeding that to a progress
    bar as a maximum is what produced a number with no unit."""
    src = io.open(os.path.join(REPO_ROOT, 'desktop', 'passage_workers.py'),
                  encoding='utf-8').read()
    assert "if phase == 'pass1':" in src and 'return' in src, (
        "the builder's pass1 callback is still being forwarded as a total")


def test_the_build_dialog_shows_a_determinate_bar():
    seg = _function_source('_on_passage_build_progress')
    assert "phase == 'read'" in seg
    assert 'setRange(0, 100)' in seg, (
        'the read phase still uses an indeterminate barber-pole bar')
    assert 'records so far' in seg, (
        'the record count has no unit beside it')


def test_the_not_built_line_is_gone_now_that_a_dialog_says_it():
    """The dialog appears at startup and on any click of the letter-level
    option, and it can actually start the build. A line under the control
    that can only point at Settings is noise beside it."""
    seg = _function_source('_show_passage_reason')
    assert 'REASON_NOT_BUILT' in seg and 'key = None' in seg, (
        'the not-built reason still renders an inline label')
    assert 'Settings \u2192' not in _app_source(), (
        'the "(Settings -> Build...)" suffix is still being appended')


# ---------------------------------------------------------------------------
# The paragraph row is HIDDEN in letter-level mode, not greyed.
# Letter-level search has no paragraph boundaries at all, so these controls
# describe a concept the method does not have.
# ---------------------------------------------------------------------------

class _FakeLayoutItem:
    def __init__(self, w):
        self._w = w

    def widget(self):
        return self._w


class _FakeRow:
    def __init__(self, widgets):
        self._items = [_FakeLayoutItem(w) for w in widgets]

    def count(self):
        return len(self._items)

    def itemAt(self, i):
        return self._items[i]


def _boundary_window():
    w = _Win()
    w.boundary_caption = _Label()
    w.boundary_mode_combo = _Label()
    w.separator_caption = _Label()
    w.boundary_delimiter_combo = _Label()
    w.btn_boundary_advanced = _Label()
    w.boundary_stats_label = _Label()
    for lbl in (w.boundary_caption, w.boundary_mode_combo,
                w.separator_caption, w.boundary_delimiter_combo,
                w.boundary_stats_label):
        lbl.visible = True
    w._comp_boundary_row = _FakeRow([
        w.boundary_caption, w.boundary_mode_combo, w.separator_caption,
        w.boundary_delimiter_combo, w.btn_boundary_advanced,
        w.boundary_stats_label])
    return w


def test_the_paragraph_row_hides_in_letter_level_mode():
    w = _boundary_window()
    APP._set_boundary_row_visible(w, False)
    for name in ('boundary_caption', 'boundary_mode_combo',
                 'separator_caption', 'boundary_delimiter_combo',
                 'boundary_stats_label', 'btn_boundary_advanced'):
        assert not getattr(w, name).visible, (
            '%s is still on screen in letter-level mode' % name)


def test_the_paragraph_row_comes_back_for_chunk_search():
    w = _boundary_window()
    APP._set_boundary_row_visible(w, False)
    APP._set_boundary_row_visible(w, True)
    for name in ('boundary_caption', 'boundary_mode_combo',
                 'separator_caption', 'boundary_delimiter_combo',
                 'boundary_stats_label'):
        assert getattr(w, name).visible, (
            '%s did not come back when the method returned to chunk' % name)


def test_showing_the_row_does_not_override_the_advanced_button():
    """It owns its own visibility -- non-full modes only, per
    `_on_boundary_mode_changed`. Forcing it visible here would reveal a
    button for a paragraph mode that is not selected."""
    w = _boundary_window()
    w.btn_boundary_advanced.visible = False      # full mode: correctly hidden
    APP._set_boundary_row_visible(w, False)
    APP._set_boundary_row_visible(w, True)
    assert not w.btn_boundary_advanced.visible, (
        'the Advanced button was forced back on for a mode that does not '
        'use it')


def test_the_mode_switch_hides_the_row():
    seg = _function_source('_apply_passage_mode_ui')
    assert '_set_boundary_row_visible(not on)' in seg, (
        'letter-level mode only greys the paragraph controls instead of '
        'removing them')


# ---------------------------------------------------------------------------
# The exported "Search Settings" block. A report that names settings which
# took no part in producing its rows is not merely untidy -- it is a false
# statement about provenance, and it outlives the session that made it.
# ---------------------------------------------------------------------------

class _Spin:
    def __init__(self, value):
        self._v = value

    def value(self):
        return self._v


# ASCII sentinels, not the real labels: the subject of these tests is which
# item the stamped key resolves to, and asserting on wording would only pin
# the current translation table.
_WIDTHS = [('standard-40', 'W-STD'), ('wide-40', 'W-WIDE'),
           ('wider-40', 'W-WIDER'), ('widest-40', 'W-WIDEST'),
           ('max-40', 'W-MAX')]
_LENGTHS = [('normal', 'L-NORMAL'), ('short', 'L-SHORT')]
_DEPTHS = [('normal', 'D-NORMAL'), ('deep', 'D-DEEP'),
           ('deepest', 'D-DEEPEST')]
_MODES = [('exact', 'M-EXACT'), ('variants', 'M-VARIANTS'),
          ('fuzzy', 'M-FUZZY')]


def _export_window(method='chunk', chunk_stamp=None, width=None, length=None,
                   depth=None, live_axes=(3, 0, 0), live_mode=0, lab=False,
                   deep=False):
    """A window whose LIVE controls are deliberately set to values other than
    the stamp's, so any test that passes could only have read the stamp."""
    w = _Win()
    w.comp_passage_width_combo = _Combo(_WIDTHS, index=live_axes[0])
    w.comp_passage_length_combo = _Combo(_LENGTHS, index=live_axes[1])
    w.comp_passage_depth_combo = _Combo(_DEPTHS, index=live_axes[2])
    w.comp_mode_combo = _Combo(_MODES, index=live_mode)
    w.spin_chunk = _Spin(7)
    w.spin_freq = _Spin(11)
    w.spin_filter = _Spin(9)
    w.btn_lab_mode_toggle_comp = _Toggle(lab)
    w.chk_lab_deep_comp = _Toggle(deep)
    w._comp_last_result_method = method
    w._comp_last_result_width = width
    w._comp_last_result_length = length
    w._comp_last_result_depth = depth
    w._comp_last_result_chunk = chunk_stamp
    return w


def _tr(s):
    return genizah_app.tr(s)


def test_a_letter_level_report_names_no_chunk_setting():
    """The owner's report: a letter-level export carried "Chunk: 5 / Max
    Freq: 10 / Search Mode: Exact / Filter > 5 / Lab Mode: Off". None of
    those five numbers took part in the search."""
    lines = _export_window(method='passage', width='widest-40',
                           length='normal', depth='normal'
                           )._comp_export_settings_lines()
    blob = ' | '.join(lines)
    for gone in ('Chunk: ', 'Max Freq: ', 'Search Mode', 'Filter > ',
                 'Lab Mode', 'Deep Scan'):
        assert _tr(gone) not in blob, (
            '%r is chunk-search vocabulary and has no meaning for a '
            'letter-level run: %r' % (gone, blob))


def test_a_letter_level_report_names_the_method_and_all_three_axes():
    lines = _export_window(method='passage', width='widest-40',
                           length='normal', depth='normal'
                           )._comp_export_settings_lines()
    blob = ' | '.join(lines)
    for present in ('Search method', 'Match width', 'Passage length',
                    'Search depth'):
        assert _tr(present) in blob, (
            '%r is part of what a letter-level run WAS and is missing from '
            'the report: %r' % (present, blob))
    assert _tr('Letter-level search') in blob
    # The combo's own item says "New!", which is an affordance for a control,
    # not the name of a search in a document the user keeps.
    assert 'New!' not in blob and _tr('New! Letter-level search') not in blob


def test_the_axis_labels_come_from_the_stamp_not_the_live_controls():
    """The controls are left on their defaults and the stamp says something
    else; a report describes the run, and the user may well have changed a
    control since."""
    w = _export_window(method='passage', width='standard-40', length='short',
                       depth='deepest', live_axes=(3, 0, 0))
    blob = ' | '.join(w._comp_export_settings_lines())
    for sentinel in ('W-STD', 'L-SHORT', 'D-DEEPEST'):
        assert sentinel in blob, (sentinel, blob)
    for live in ('W-WIDEST', 'L-NORMAL', 'D-NORMAL'):
        assert live not in blob, (
            'the report read the live control instead of the stamp: %r' % blob)


def test_a_pre_axis_stamp_falls_back_to_the_control():
    """A passage stamp with no axes cannot happen from a dispatch, but a
    hand-edited or future-shaped session could carry one. It must still name
    something true rather than print None."""
    blob = ' | '.join(_export_window(method='passage')
                      ._comp_export_settings_lines())
    assert 'W-WIDEST' in blob and 'None' not in blob, blob


def test_an_unknown_axis_value_is_reported_verbatim():
    """Better a raw key -- which is at least what ran -- than a blank line or
    a made-up label."""
    blob = ' | '.join(_export_window(method='passage', width='wider-99')
                      ._comp_export_settings_lines())
    assert 'wider-99' in blob, blob


def test_a_chunk_report_still_says_everything_it_used_to():
    stamp = {'chunk': 5, 'freq': 10, 'mode_index': 0, 'appendix': 5,
             'lab': False, 'deep': False}
    lines = _export_window(method='chunk',
                           chunk_stamp=stamp)._comp_export_settings_lines()
    blob = ' | '.join(lines)
    for kept in ('Chunk: ', 'Max Freq: ', 'Search Mode', 'Filter > ',
                 'Lab Mode'):
        assert _tr(kept) in blob, (kept, blob)
    assert _tr('Chunk search') in blob, (
        'once there are two methods, a report that does not name its own is '
        'ambiguous')
    assert 'M-EXACT' in blob and '5' in blob and '10' in blob


def test_a_chunk_report_is_not_rewritten_by_a_later_method_switch():
    """THE second defect this fix closes. Letter-level mode FORCES the chunk
    knobs, so a settings block read from live widgets would change the
    reported settings of a chunk search that had already finished, merely
    because the user then looked at the other method."""
    stamp = {'chunk': 5, 'freq': 10, 'mode_index': 0, 'appendix': 5,
             'lab': False, 'deep': False}
    w = _export_window(method='chunk', chunk_stamp=stamp, live_mode=2)
    w.spin_chunk = _Spin(5000)          # what forcing would leave behind
    w.spin_freq = _Spin(50)
    blob = ' | '.join(w._comp_export_settings_lines())
    assert '5000' not in blob and '50' not in blob, (
        'the report was rewritten from the widgets as they stand now: %r'
        % blob)
    assert 'M-EXACT' in blob and 'M-FUZZY' not in blob, blob


def test_with_no_stamp_the_chunk_half_reads_the_restored_widgets():
    """A session restored from disk has no run behind it. Falling back to the
    widgets is right there and not a degradation: the restore populated them
    from the same saved file as the rows."""
    blob = ' | '.join(_export_window(method='chunk', chunk_stamp=None,
                                     live_mode=1)
                      ._comp_export_settings_lines())
    assert '7' in blob and '11' in blob and '9' in blob, blob
    assert 'M-VARIANTS' in blob, blob


def test_deep_scan_is_reported_only_for_a_lab_run():
    off = ' | '.join(_export_window(
        method='chunk', chunk_stamp={'lab': False, 'deep': True})
        ._comp_export_settings_lines())
    assert _tr('Deep Scan') not in off, off
    on = ' | '.join(_export_window(
        method='chunk', chunk_stamp={'lab': True, 'deep': True})
        ._comp_export_settings_lines())
    assert _tr('Deep Scan') in on, on


def test_the_export_reads_the_run_record_and_no_widget():
    """Source-anchored: the whole point is that the settings block is not
    assembled from live controls. A future edit that reaches for one again
    reddens here."""
    seg = _function_source('export_comp_report')
    assert '_comp_export_settings_lines()' in seg, (
        'the export stopped going through the run record')
    for widget in ('spin_chunk', 'spin_freq', 'spin_filter',
                   'comp_mode_combo', 'chk_lab_deep_comp'):
        assert widget not in seg, (
            'the export reads %s directly again -- that is what made a '
            'letter-level report describe a chunk search' % widget)


def test_a_restored_snapshot_carries_its_own_provenance():
    """Without this the export of restored letter-level rows describes a
    chunk search: `_comp_last_result_method` defaults to 'chunk', and in a
    report a default is indistinguishable from a fact."""
    tail = _function_source('_display_restored_comp_snapshot')
    for field in ('last_result_method', 'last_result_width',
                  'last_result_length', 'last_result_depth'):
        assert "comp.get('%s')" % field in tail, (
            'the restore re-displays rows without re-establishing %s'
            % field)
    assert '_comp_last_result_chunk = None' in tail, (
        'a stale chunk stamp from an earlier run would outrank the widgets '
        'the restore just populated')


# ---------------------------------------------------------------------------
# Codex review round 1 (PR #331). Three P2 findings, all real.
# ---------------------------------------------------------------------------

class _Worker:
    def __init__(self, running=True):
        self._running = running
        self.paused = False
        self.resumed = False

    def isRunning(self):
        return self._running

    def pause(self):
        self.paused = True

    def resume(self):
        self.resumed = True


class _Ctx:
    def __init__(self, state='running'):
        self.state = state
        self.epoch = 0
        self.pause_started = 0.0
        self.paused_total = 0.0


class _Result:
    def __init__(self, status):
        self.status = status
        self.index = None
        self.live_dir = ''


# --- 1. A timed-out release leaves nothing behind, and must say so ---------

def _finish_window(monkeypatch, status):
    """`_on_passage_build_finished` with no index to install, which is what
    every failed swap looks like."""
    w = _Win()
    w._close_pending = False
    w.status_label = _Label()
    w.calls = []
    w._finish_passage_build = lambda: w.calls.append('finish')
    w._revalidate_comp_method = lambda: w.calls.append('revalidate')
    w._start_passage_load = lambda: w.calls.append('reload')
    APP._on_passage_build_finished(w, _Result(status))
    return w


def test_a_timed_out_release_reloads_the_index(monkeypatch):
    """The generation guard stops an abandoned seam closing a NEWER index.
    It does nothing when nothing newer was installed -- exactly the timeout
    case -- so the late close lands on a matching generation, succeeds, and
    clears the live state with nothing left to reinstall it."""
    w = _finish_window(monkeypatch, 'release_timed_out')
    assert 'reload' in w.calls, (
        'a timed-out release left the state to be closed by a seam that was '
        'abandoned, not cancelled, and nothing re-established it')


@pytest.mark.parametrize('status', ['readers_active', 'rename_blocked',
                                    'cancelled', 'reload_failed_rolled_back'])
def test_every_other_failed_swap_keeps_its_index_and_does_not_reload(status):
    """The reload is for the ONE status whose index may still be closed
    behind us. Reloading after the others would be a pointless ~109 MB scan
    of an index that is already installed and working."""
    w = _finish_window(None, status)
    assert 'reload' not in w.calls, (
        '%s left a working index behind and does not need re-establishing'
        % status)


def test_the_timeout_branch_does_not_claim_the_swap_guarantee():
    seg = _function_source('_on_passage_build_finished')
    assert "'release_timed_out'" in seg, (
        'the one status that does not leave a working index behind is back '
        'in the branch that says one is still in use')
    head, _, tail = seg.partition("elif status == 'release_timed_out':")
    assert '_start_passage_load()' in tail


# --- 2. Pause is the fourth way to ask a scan to stop ----------------------

def _pause_window(passage_running):
    w = _Win()
    w._pause_comp = _Ctx()
    w._pause_search = _Ctx()
    w.worker = _Worker()
    w._pause_worker_for = lambda ctx: w.worker
    w.lbl_comp_status = _Label()
    w.is_comp_running = passage_running
    w._comp_last_result_method = 'passage' if passage_running else 'chunk'
    w._comp_grouping_active = False
    w._apply_pause_state = lambda ctx, s: None
    w._paint_pause_status = lambda ctx, s: None
    return w


def test_pause_is_refused_during_a_passage_scan():
    """`PassageSearcher` never calls the progress callback, and the worker's
    only `_checkpoint()` lives inside it -- so pause() would park a disabled
    button at "Pausing..." waiting for an acknowledgement that cannot come."""
    w = _pause_window(True)
    APP._on_pause_clicked(w, w._pause_comp)
    assert not w.worker.paused, (
        'the worker was asked to pause by a path that can never acknowledge')
    assert w._pause_comp.state == 'running', (
        'the UI went to "pausing" for a scan that will not pause')
    assert w.lbl_comp_status.text, 'the refusal said nothing to the user'


def test_pause_still_works_for_a_chunk_scan():
    w = _pause_window(False)
    APP._on_pause_clicked(w, w._pause_comp)
    assert w.worker.paused and w._pause_comp.state == 'pausing'


def test_a_passage_scan_does_not_freeze_the_search_tabs_pause():
    """The guard is scoped to the composition context. A letter-level scan
    says nothing about whether a search-tab run may be paused, and the two
    can be running at once."""
    w = _pause_window(True)
    APP._on_pause_clicked(w, w._pause_search)
    assert w.worker.paused and w._pause_search.state == 'pausing', (
        'a passage scan on the composition tab refused an unrelated pause')


# --- 3. Persist the user's settings, not the ones we forced on them --------

def _pref_window(cached=None):
    w = _Win()
    w.comp_mode_combo = _Combo(_MODES, index=0)
    w.spin_chunk = _Spin(5)
    w.spin_freq = _Spin(50)
    w.spin_min_chunks = _Spin(1)
    w.boundary_mode_combo = _Combo([('full', 'F'), ('boundary', 'B')], 0)
    if cached is not None:
        w._passage_cached_chunk_state = cached
    return w


def test_with_no_cache_the_preference_is_the_live_widget():
    """Every path outside letter-level mode, which must behave exactly as
    it did before."""
    w = _pref_window()
    assert w._comp_chunk_preference('spin_chunk') == 5
    assert w._comp_chunk_preference('spin_freq') == 50
    assert w._comp_chunk_preference('comp_mode_combo') == 0


def test_the_cached_value_outranks_the_forced_widget():
    """The widgets here read exactly what letter-level mode forces them to
    (chunk=5, freq=50, mode=Exact); the cache holds what the user chose."""
    w = _pref_window({'spin_chunk': 9, 'spin_freq': 12,
                      'comp_mode_combo': 2})
    assert w._comp_chunk_preference('spin_chunk') == 9
    assert w._comp_chunk_preference('spin_freq') == 12
    assert w._comp_chunk_preference('comp_mode_combo') == 2


def test_an_unknown_control_has_no_preference():
    assert _pref_window()._comp_chunk_preference('spin_filter') is None


@pytest.mark.parametrize('fn', ['_save_session', '_add_comp_search_to_history'])
def test_neither_persistence_surface_stores_a_forced_value(fn):
    """Both write chunk PREFERENCES. Codex named the session; the history
    entry does the same thing two hundred lines away, and the in-memory
    cache that would have restored either dies with the process."""
    seg = _function_source(fn)
    for widget, reader in (('spin_chunk', '.value()'),
                           ('spin_freq', '.value()'),
                           ('comp_mode_combo', '.currentIndex()')):
        assert ('self.%s%s' % (widget, reader)) not in seg, (
            '%s persists the LIVE %s, which letter-level mode forces'
            % (fn, widget))
    assert '_comp_chunk_preference(' in seg


# ---------------------------------------------------------------------------
# Codex review round 2 (PR #331).
# ---------------------------------------------------------------------------

_SNAPSHOT = {'results': [{'x': 1}], 'filtered_results': [],
             'last_result_method': 'passage', 'last_result_scope': 'genizah',
             'last_result_width': 'wider-40', 'last_result_length': 'short',
             'last_result_depth': 'deep'}


def _snapshot_window(monkeypatch, available=True, in_flight=False,
                     scope='genizah'):
    monkeypatch.setattr(pl, 'passage_available', lambda: available)
    w = _window(scope=scope)
    w._passage_load_in_flight = in_flight
    w.displayed = []
    w.display_comp_results = lambda *a: w.displayed.append(a)
    return w


@pytest.mark.parametrize('method,in_flight,rows,expected', [
    # THE regression: a letter-level snapshot while the index is opening.
    ('passage', True,  True,  True),
    ('passage', False, True,  False),   # the answer is known: judge it
    ('chunk',   True,  True,  False),   # a chunk snapshot never waits
    ('passage', True,  False, False),   # nothing to hold back
])
def test_only_an_unanswerable_letter_level_snapshot_waits(
        monkeypatch, method, in_flight, rows, expected):
    w = _snapshot_window(monkeypatch, available=False, in_flight=in_flight)
    comp = dict(_SNAPSHOT, last_result_method=method)
    if not rows:
        comp['results'] = []
        comp['filtered_results'] = []
    assert w._passage_snapshot_must_wait(comp) is expected


def test_the_deferred_snapshot_is_displayed_once_the_index_is_there(
        monkeypatch):
    """The whole point of waiting: the rows survive."""
    w = _snapshot_window(monkeypatch, available=True)
    assert w._display_restored_comp_snapshot(dict(_SNAPSHOT)) is True
    assert w.displayed, 'the snapshot was held back and then never shown'
    assert w.comp_raw_items == [{'x': 1}]


def test_a_displayed_snapshot_carries_its_provenance(monkeypatch):
    w = _snapshot_window(monkeypatch, available=True)
    w._display_restored_comp_snapshot(dict(_SNAPSHOT))
    assert w._comp_last_result_method == 'passage'
    assert w._comp_last_result_width == 'wider-40'
    assert w._comp_last_result_depth == 'deep'
    assert w._comp_last_result_chunk is None


def test_a_snapshot_that_really_cannot_run_is_still_refused(monkeypatch):
    """Deferring must not become never judging. Once the index has loaded
    and letter-level is genuinely impossible for the stamped scope, the rows
    go -- they describe a search the app would refuse to perform."""
    w = _snapshot_window(monkeypatch, available=True, scope='genizah')
    stranded = dict(_SNAPSHOT, last_result_scope='local')
    assert w._display_restored_comp_snapshot(stranded) is False
    assert not w.displayed


def test_the_restore_defers_instead_of_guessing():
    seg = _function_source('_restore_session')
    assert '_passage_snapshot_must_wait(comp)' in seg, (
        'the session restore judges a letter-level snapshot while the index '
        'is still opening -- which discards it on every launch')
    assert '_deferred_comp_snapshot' in seg


def test_the_load_slot_judges_what_was_deferred():
    seg = _function_source('_on_passage_loaded')
    assert "_deferred_comp_snapshot" in seg and (
        '_display_restored_comp_snapshot(deferred)' in seg), (
        'a snapshot held back by the startup race is never looked at again')


def test_the_flag_is_set_before_the_thread_starts():
    """Set after start() it would be a race against the very thread it
    describes; the restore timer reads it moments later."""
    seg = _function_source('_start_passage_load')
    assert seg.index('_passage_load_in_flight = True') < seg.index('.start()')


# --- history records the method it ran ------------------------------------

def test_history_records_the_method_and_policy_from_the_stamp():
    seg = _function_source('_add_comp_search_to_history')
    for key in ("'comp_method'", "'comp_passage_width'",
                "'comp_passage_length'", "'comp_passage_depth'"):
        assert key in seg, (
            'a history entry without %s re-runs whichever method happens to '
            'be selected later, under the name of the one recorded' % key)
    assert '_comp_last_result_method' in seg, (
        'the method must come from the dispatch stamp, not a live widget')


def test_history_applies_the_recorded_method_before_re_running():
    seg = _function_source('_restore_comp_search_from_state')
    assert '_restore_comp_passage_preferences(' in seg, (
        'the recorded method is never applied on the way back')
    assert (seg.index('_restore_comp_passage_preferences(')
            < seg.index('self.run_composition()')), (
        'the method is applied AFTER the re-run has already been dispatched')


# ---------------------------------------------------------------------------
# Codex review round 3 (PR #331).
# ---------------------------------------------------------------------------

class _Signal:
    def __init__(self):
        self.emitted = []

    def emit(self, *args):
        self.emitted.append(args)


class _BuildResult:
    def __init__(self, status, error=None):
        self.status = status
        self.error = error
        self.index = None
        self.live_dir = ''


class _BuildStub:
    """Borrows the worker's `run` -- no QThread, no event loop."""
    run = PassageBuildThread.run

    def __init__(self):
        self._root = 'root'
        self._corpus_path = 'corpus'
        self.finished_signal = _Signal()
        self.cancelled_signal = _Signal()
        self.error_signal = _Signal()

    def _records_with_progress(self, path):
        return iter(())

    def _on_progress(self, *a, **k):
        pass

    def _cancel_check(self):
        return False

    def _release_live_state(self, generation):
        return True


def _run_worker(monkeypatch, result):
    from desktop import passage_workers
    monkeypatch.setattr(passage_workers.passage_lifecycle,
                        'run_build_and_swap',
                        lambda *a, **k: result)
    w = _BuildStub()
    w.run()
    return w


def test_a_returned_build_error_reaches_the_error_handler(monkeypatch):
    """`run_build_and_swap` RETURNS a failed build instead of raising it, so
    the worker's `except` never sees it. Emitted as a normal completion it
    dropped the diagnostic and told the user the previous index was still in
    use -- on a first build, an index that never existed."""
    w = _run_worker(monkeypatch, _BuildResult('error', error='disk on fire'))
    assert w.error_signal.emitted == [('disk on fire',)], (
        'a returned failure was reported as a completed build')
    assert not w.finished_signal.emitted


def test_a_cancelled_build_still_reports_cancellation(monkeypatch):
    w = _run_worker(monkeypatch, _BuildResult('cancelled'))
    assert w.cancelled_signal.emitted and not w.error_signal.emitted


def test_a_good_build_still_finishes_normally(monkeypatch):
    w = _run_worker(monkeypatch, _BuildResult('installed'))
    assert w.finished_signal.emitted and not w.error_signal.emitted


def test_a_swap_failure_is_not_an_error_signal(monkeypatch):
    """`readers_active` and friends leave a working index behind and are
    reported through the normal completion path, which says so."""
    w = _run_worker(monkeypatch, _BuildResult('readers_active'))
    assert w.finished_signal.emitted and not w.error_signal.emitted


def test_history_takes_the_scope_from_the_same_stamp_as_the_method():
    """Nothing disables the scope selector during a run, so a live read here
    could pair a letter-level method with a My Library scope -- impossible,
    demoted to chunk on reopen, and a different search re-run under the
    recorded one's name."""
    seg = _function_source('_add_comp_search_to_history')
    idx = seg.index("'comp_corpus_scope'")
    tail = seg[idx:idx + 400]
    assert '_comp_last_result_scope' in tail, (
        'the history entry records the scope as it stands now, not the one '
        'the run was dispatched under')


# ---------------------------------------------------------------------------
# Stop, at the witness boundary (owner ruling 2026-08-27).
#
# A multi-witness batch CAN be stopped -- its loop checkpoints between
# witnesses. It still cannot be KILLED: the witness in flight may be ~19s from
# done at Deepest, and terminating a thread that holds live memory mappings is
# the failure the single-search guard exists to prevent. Stoppable is not
# killable, and these pin the difference.
# ---------------------------------------------------------------------------

class _Tree:
    def __init__(self):
        self.cleared = 0

    def clear(self):
        self.cleared += 1


class _FakeBatchThread:
    """Stands in for MultiWitnessCompositionThread. Not a subclass by
    accident: `_passage_batch_in_flight` uses isinstance, so the tests
    monkeypatch that symbol rather than fake an inheritance the real code
    would then be free to stop relying on."""

    def __init__(self, running=True):
        self.cancelled = 0
        self.terminated = 0
        self.waited = []
        self._running = running

    def request_cancel(self):
        self.cancelled += 1

    def isRunning(self):
        return self._running

    def wait(self, ms=None):
        self.waited.append(ms)
        return True

    def terminate(self):
        self.terminated += 1


def _batch_window(monkeypatch, running=True):
    """A window mid-way through a multi-witness passage batch."""
    w = _Win()
    w.is_comp_running = True
    w._comp_last_result_method = 'passage'
    w._comp_grouping_active = False
    w.comp_thread = _FakeBatchThread(running=running)
    w.lbl_comp_status = _Label()
    monkeypatch.setattr(genizah_app, 'MultiWitnessCompositionThread',
                        _FakeBatchThread)
    return w


def _single_window():
    """A window mid-way through an ORDINARY single-witness passage search."""
    w = _Win()
    w.is_comp_running = True
    w._comp_last_result_method = 'passage'
    w._comp_grouping_active = False
    w.comp_thread = object()          # not a batch
    w.lbl_comp_status = _Label()
    return w


def test_a_single_witness_passage_scan_still_refuses_stop():
    """Unchanged from v9.0.0: one search has no checkpoint at all, so a flag
    would leave "Cancelling..." on screen for up to ~19s and then complete
    anyway."""
    w = _single_window()
    assert APP._refuse_stop_during_passage_scan(w) is True


def test_a_multi_witness_batch_allows_stop(monkeypatch):
    w = _batch_window(monkeypatch)
    assert APP._refuse_stop_during_passage_scan(w) is False


def test_stopping_a_batch_says_the_current_witness_finishes_first(monkeypatch):
    """Promising an instant stop and then taking 19 seconds is the same lie
    as refusing one that would work."""
    w = _batch_window(monkeypatch)
    APP._refuse_stop_during_passage_scan(w)
    # Compared against tr(), not an English substring. The earlier version
    # asserted `'witness' in ...`, which passed only because the string had
    # no Hebrew entry yet -- it was pinned to the translation GAP, and adding
    # the approved Hebrew broke it.
    from genizah_core import tr as _tr
    assert w.lbl_comp_status.text == _tr(
        "Stopping after the current witness finishes. Results found so far "
        "are kept.")


def test_a_chunk_search_is_untouched_by_any_of_this():
    w = _Win()
    w.is_comp_running = True
    w._comp_last_result_method = 'chunk'
    w._comp_grouping_active = False
    assert APP._refuse_stop_during_passage_scan(w) is False
    assert APP._passage_batch_in_flight(w) is False


def test_reset_never_terminates_a_witness_batch(monkeypatch):
    """THE guard. Reset's own path is request_cancel + wait(3000) +
    terminate(), and terminate on a thread holding live memory mappings is an
    access violation, not a catchable error. Being stoppable does not make a
    batch killable."""
    w = _batch_window(monkeypatch)
    w._auto_expand_left = 3
    scheduled = []
    monkeypatch.setattr(genizah_app.QTimer, 'singleShot',
                        staticmethod(lambda ms, fn: scheduled.append((ms, fn))))
    APP._reset_composition(w)
    # Cleared BEFORE the deferral, or the cancelled batch's partial
    # completion renders, arms the expansion and schedules the next round at
    # zero delay -- which beats the 400 ms retry, so New spends a whole
    # witness-search cancelling a round it just spawned, once per round.
    assert w._auto_expand_left == 0, (
        'Reset deferred without clearing the expansion it is racing'
    )
    assert w.comp_thread.terminated == 0, 'Reset terminated a passage batch'
    assert w.comp_thread.waited == [], 'Reset blocked the UI thread'
    assert w.comp_thread.cancelled == 1, 'Reset did not ask it to stop'
    assert scheduled, 'Reset neither completed nor scheduled a retry'


def test_reset_of_a_batch_defers_rather_than_clearing_the_results(monkeypatch):
    """Clearing while the worker is still writing rows is how a half-reset
    tab ends up showing the next search's rows under the old title."""
    w = _batch_window(monkeypatch)
    w.comp_tree = _Tree()
    monkeypatch.setattr(genizah_app.QTimer, 'singleShot',
                        staticmethod(lambda ms, fn: None))
    APP._reset_composition(w)
    assert w.comp_tree.cleared == 0, 'results were cleared mid-batch'
    assert w._reset_pending is True


def test_the_deferred_reset_retries_while_the_batch_is_still_running(monkeypatch):
    """It must RESCHEDULE and do nothing else.

    Asserting only that a 400 ms timer was armed is not enough, and a
    mutation proved it: with the busy check removed the retry falls straight
    through into `_reset_composition`, which arms a 400 ms timer of its OWN.
    The observable the test was watching looked identical while the reset had
    in fact run against a live batch. So the assertion is that the reset was
    NOT performed.
    """
    w = _batch_window(monkeypatch, running=True)
    scheduled, reset_calls = [], []
    monkeypatch.setattr(genizah_app.QTimer, 'singleShot',
                        staticmethod(lambda ms, fn: scheduled.append(ms)))
    w._reset_composition = lambda: reset_calls.append(1)
    w._reset_pending = True
    APP._retry_pending_reset(w)
    assert reset_calls == [], 'the reset ran while the batch was still going'
    assert w._reset_pending is True, 'it forgot it was still waiting'
    assert scheduled == [400], 'the retry gave up while the batch ran'


def test_the_deferred_reset_completes_once_the_batch_is_done(monkeypatch):
    """And the poll is a timer, never a blocking wait(): the UI thread is
    where the batch's own signals are delivered, so blocking here would stop
    the thread it is waiting for from ever reporting done."""
    w = _batch_window(monkeypatch)
    w.is_comp_running = False          # the batch has finished
    done = []
    # The INSTANCE, not APP: `_Win` copied the unbound method onto its own
    # class, so patching APP would not be seen through the stub.
    w._reset_composition = lambda: done.append(1)
    monkeypatch.setattr(genizah_app.QTimer, 'singleShot',
                        staticmethod(lambda ms, fn: done.append('rescheduled')))
    w._reset_pending = True
    APP._retry_pending_reset(w)
    assert done == [1], done
    assert w._reset_pending is False


def test_neither_deferred_reset_helper_ever_terminates():
    """Source guard beside the existing one, which named only the close and
    build helpers. Matched by statement, not substring: an earlier guard of
    this kind matched its own explanatory COMMENT and passed against code
    that was already wrong."""
    import re
    for name in ('_retry_pending_reset',):
        seg = _function_source(name)
        assert not re.search(r'^\s*\S*\.terminate\(\)', seg, re.M), (
            name, 'calls terminate()')


def test_reset_refuses_a_single_witness_scan_before_reaching_terminate():
    """The ordering that matters: the refusal is the FIRST statement, so a
    single-witness passage scan never reaches the wait/terminate block."""
    w = _single_window()
    w.comp_thread = _FakeBatchThread()
    APP._reset_composition(w)
    assert w.comp_thread.terminated == 0
    assert w.comp_thread.cancelled == 0, 'it got past the refusal'


# ---------------------------------------------------------------------------
# The announcements are ONE-SHOT (owner, 2026-08-28).
#
# An orange accent that returns every launch stops reading as "this is new"
# and starts reading as "this control is in a warning state". Stored in
# `config.pkl`, not the session file: "you have already been told" is a fact
# about the person, and a user who declines session restore must not be
# re-announced to on every launch.
# ---------------------------------------------------------------------------

class _CurrentTab:
    """A QTabWidget stand-in that answers only the question the announcement
    asks: is this the tab in front of the user."""

    def __init__(self, current):
        self._current = current

    def currentWidget(self):
        return self._current


def _announce_window(monkeypatch, seen=False, raises=False, method='chunk'):
    """A window whose announcement store is a dict in this test, not the
    owner's real config."""
    reads, writes = [], []
    # STATEFUL, because that is the only shape in which a re-read can be
    # caught: a store that forgets every write makes a badge that re-reads
    # look exactly like one that does not.
    store = {APP._ANNOUNCE_METHOD: seen, APP._ANNOUNCE_WITNESSES: seen}

    def _load():
        reads.append(1)
        if raises:
            raise OSError('unreadable config')
        return dict(store)

    def _save(data):
        writes.append(data)
        store.update(data)

    monkeypatch.setattr(genizah_app, 'load_app_config', _load)
    monkeypatch.setattr(genizah_app, 'save_app_config', _save)
    monkeypatch.setattr(pl, 'passage_available', lambda: True)
    monkeypatch.setattr(genizah_app.Config, 'FILE_V8', __file__)
    w = _window(scope='genizah')
    # Undo `_unannounced`: these tests are ABOUT the store, so they supply
    # their own rather than skipping it.
    del w._announcements_allowed
    del w._announcements_written
    w.comp_method_combo = _Combo([('chunk', ''), ('passage', '')],
                                 index=0 if method == 'chunk' else 1)
    w.lbl_comp_method_new = _Label()
    # ON the tab: every test here is about what happens once the badge can
    # be SEEN. The construction case has its own test below.
    w.composition_tab = object()
    w.tabs = _CurrentTab(w.composition_tab)
    w.reads, w.writes = reads, writes
    return w


def test_the_method_badge_is_silent_once_it_has_been_seen(monkeypatch):
    w = _announce_window(monkeypatch, seen=True)
    APP._update_comp_method_affordance(w)
    assert not w.lbl_comp_method_new.visible, (
        'the badge is announced again on a launch that already announced it')
    assert 'border' not in (w.comp_method_combo.style or ''), (
        'the accent border comes back every launch, which reads as a '
        'control stuck in a warning state')


def test_showing_the_method_badge_spends_it(monkeypatch):
    w = _announce_window(monkeypatch, seen=False)
    APP._update_comp_method_affordance(w)
    assert w.lbl_comp_method_new.visible, 'the first launch says nothing'
    assert w.writes == [{APP._ANNOUNCE_METHOD: True}], (
        'nothing was recorded, so the badge is announced again next launch')


def test_the_badge_does_not_vanish_from_under_the_reader(monkeypatch):
    """The write happens on the first paint. Re-reading the store on the next
    refresh -- a scope change, a finished search, any repaint -- would take
    the badge away mid-launch, which is not "once", it is a flicker."""
    w = _announce_window(monkeypatch, seen=False)
    APP._update_comp_method_affordance(w)
    w.writes[:] = []
    APP._update_comp_method_affordance(w)
    assert w.lbl_comp_method_new.visible, (
        'the badge disappeared during the very launch that showed it')
    assert w.writes == [], 'the config is rewritten on every refresh'


def test_choosing_letter_level_takes_the_badge_down_now(monkeypatch):
    """Not next launch: the user has plainly found the feature."""
    w = _announce_window(monkeypatch, seen=False)
    APP._update_comp_method_affordance(w)
    assert w.lbl_comp_method_new.visible
    APP._retire_announcement(w, APP._ANNOUNCE_METHOD)
    w.comp_method_combo.setCurrentIndex(0)      # back to chunk in the same run
    APP._update_comp_method_affordance(w)
    assert not w.lbl_comp_method_new.visible, (
        'switching back to chunk re-announces a feature the user has used')


def test_an_unreadable_config_stays_quiet(monkeypatch):
    """We do not know whether the user has been told, and silence is the
    cheaper of the two mistakes."""
    w = _announce_window(monkeypatch, raises=True)
    APP._update_comp_method_affordance(w)
    assert not w.lbl_comp_method_new.visible


def test_the_store_is_read_once_per_launch(monkeypatch):
    """It is a pickle load. Three refreshes must not be three reads."""
    w = _announce_window(monkeypatch, seen=False)
    for _ in range(3):
        APP._update_comp_method_affordance(w)
    assert len(w.reads) == 1, 'the config is re-read on every refresh'


# ---------------------------------------------------------------------------
# Letter-level search is the DEFAULT once an index exists (owner,
# 2026-08-28) -- but a stored method is a decision and outranks it, `chunk`
# included. The correction that shapes this: never override the last search
# state.
# ---------------------------------------------------------------------------

def _default_window(monkeypatch, available=True, scope='genizah',
                    method='chunk'):
    monkeypatch.setattr(pl, 'passage_available', lambda: available)
    w = _window(scope=scope)
    w.comp_method_combo = _Combo([('chunk', ''), ('passage', '')],
                                 index=0 if method == 'chunk' else 1)
    w.applied = []
    w._apply_passage_mode_ui = lambda on: w.applied.append(on)
    w._show_passage_reason = lambda _k: None
    w._update_comp_method_help = lambda: None
    return w


def test_letter_level_is_the_default_once_an_index_exists(monkeypatch):
    w = _default_window(monkeypatch)
    APP._apply_default_comp_method(w)
    assert w.comp_method_combo.currentData() == 'passage', (
        'a faster method with far fewer unrelated results is still behind a '
        'dropdown nobody opens')
    assert w.applied == [True], (
        'the method moved but the letter-level controls did not follow it -- '
        'a programmatic setCurrentIndex fires no handler')


def test_the_default_does_not_fire_without_an_index(monkeypatch):
    w = _default_window(monkeypatch, available=False)
    APP._apply_default_comp_method(w)
    assert w.comp_method_combo.currentData() == 'chunk'


def test_the_default_does_not_fire_where_letter_level_would_refuse(
        monkeypatch):
    """A My Library scope refuses letter-level search outright; defaulting to
    it would open on a method that can only say no."""
    w = _default_window(monkeypatch, scope='local')
    APP._apply_default_comp_method(w)
    assert w.comp_method_combo.currentData() == 'chunk'


def test_a_stored_chunk_choice_outranks_the_new_default(monkeypatch):
    """The owner's correction, and the whole reason this is not simply a new
    initial index: do not override the last search state."""
    w = _default_window(monkeypatch)
    w._refresh_comp_method_enabled = lambda: None
    w._refresh_witness_panel = lambda: None
    APP._restore_comp_passage_preferences(w, {'comp_method': 'chunk'})
    APP._apply_default_comp_method(w)
    assert w.comp_method_combo.currentData() == 'chunk', (
        "the user's own last method was overwritten by the default")


def test_a_session_with_no_stored_method_leaves_the_default_standing(
        monkeypatch):
    """A pre-Phase-146 session stores no method at all. Restoring it must not
    write `chunk` over a default the load race already applied."""
    w = _default_window(monkeypatch, method='passage')
    w._refresh_comp_method_enabled = lambda: None
    w._refresh_witness_panel = lambda: None
    APP._restore_comp_passage_preferences(w, {}, absent_method=None)
    assert w.comp_method_combo.currentData() == 'passage', (
        'a session that predates the method silently demoted the default')


def test_a_history_entry_with_no_method_is_still_chunk_by_definition(
        monkeypatch):
    """The SAME absent key means the opposite thing here. A pre-v9 history
    entry recorded a search that ran before the method existed; re-running it
    as letter-level would run a different search under the same name -- and
    now that letter-level is the default, that is what "leave it alone" would
    do."""
    w = _default_window(monkeypatch, method='passage')
    w._refresh_comp_method_enabled = lambda: None
    w._refresh_witness_panel = lambda: None
    APP._restore_comp_passage_preferences(w, {})
    assert w.comp_method_combo.currentData() == 'chunk', (
        'a pre-v9 history entry re-runs as a letter-level search')


def test_the_two_callers_disagree_on_purpose():
    """Reached, not merely correct: the distinction is worth nothing if both
    call sites pass the same thing."""
    hist = _function_source('_restore_comp_search_from_state')
    sess = _function_source('_restore_session')
    assert '_restore_comp_passage_preferences(params or {})' in hist, (
        'the history restore no longer takes the pre-v9 chunk fallback')
    assert 'absent_method=None' in sess, (
        'the session restore writes chunk over the letter-level default')


def test_a_click_on_chunk_is_a_choice_the_default_must_not_undo(monkeypatch):
    """Every programmatic move blocks signals, so reaching the handler means
    a person moved the control -- and choosing chunk is as much a decision as
    choosing letter-level."""
    w = _default_window(monkeypatch)
    w._update_comp_method_help = lambda: None
    w._update_comp_method_affordance = lambda: None
    w._save_session = lambda: None
    APP._on_comp_method_changed(w, 0)
    APP._apply_default_comp_method(w)
    assert w.comp_method_combo.currentData() == 'chunk', (
        'the default overrode a method the user had just picked by hand')


def test_the_default_is_applied_where_the_index_first_answers(monkeypatch):
    """Reached, not merely correct: at construction the combo cannot know
    whether an index exists, so the only two moments with a true answer are
    the startup load and the end of a build."""
    monkeypatch.setattr(pl, 'passage_available', lambda: True)
    monkeypatch.setattr(pl, 'install_passage_state', lambda st: True)
    w = _default_window(monkeypatch)
    w._revalidate_comp_method = lambda: None
    w._maybe_offer_passage_build = lambda: None
    APP._on_passage_loaded(w, type('R', (), {
        'index': object(), 'live_dir': 'd', 'status': 'live_ok'})())
    assert w.comp_method_combo.currentData() == 'passage'

    w2 = _default_window(monkeypatch)
    w2._close_pending = False
    w2.status_label = _Label()
    w2._finish_passage_build = lambda: None
    w2._revalidate_comp_method = lambda: None
    w2._start_passage_load = lambda: None
    APP._on_passage_build_finished(w2, type('R', (), {
        'index': object(), 'live_dir': 'd', 'status': 'installed'})())
    assert w2.comp_method_combo.currentData() == 'passage', (
        'a user who just spent ten minutes building the index is left on '
        'the method they built it to replace')


def test_the_default_waits_for_the_session_restore(monkeypatch):
    """`_apply_passage_mode_ui` caches the chunk knobs as "the user's own"
    the FIRST time it runs and only then. Applying the default while the
    restore is still pending caches the construction values (5 / 10 / Exact);
    the restore then writes the real ones into widgets that are already
    forced and disabled, and the next save reads the cache -- silently
    resetting the user's chunk settings."""
    w = _default_window(monkeypatch)
    w._restoring_session = True
    APP._apply_default_comp_method(w)
    assert w.comp_method_combo.currentData() == 'chunk', (
        'the default ran during the restore window and cached the '
        "construction chunk values as the user's own")
    assert w.applied == [], 'the mode UI was applied mid-restore'

    w._restoring_session = False
    APP._apply_default_comp_method(w)
    assert w.comp_method_combo.currentData() == 'passage', (
        'held back and never applied is not a deferral, it is a drop')


def test_the_restore_re_enters_the_default_on_every_path():
    """It sits in the `finally`, so a no-data start, a declined restore,
    `restore_mode='never'` and an exception mid-restore all reach it -- and
    those are the starts most likely to have no stored method at all."""
    seg = _function_source('_restore_session')
    _, _, after = seg.partition('self._restoring_session = False')
    assert '_apply_default_comp_method' in after.split('def ')[0], (
        'the default is never re-applied after the restore window closes, '
        'so a load that beat the restore drops it silently')


# ---------------------------------------------------------------------------
# Amber accents and the dark theme.
#
# Both accents shipped naming a light `background` and no `color`, so the
# text kept the palette's own near-white and the control read as pale text on
# cream. The Witnesses button was reported with a screenshot (owner,
# 2026-08-28); the Text Position selector had been doing it silently since
# Phase 45, and only when set to a non-default position -- which is exactly
# when the highlight exists to be read.
# ---------------------------------------------------------------------------

class _Colour:
    def __init__(self, lightness):
        self._l = lightness

    def lightness(self):
        return self._l


class _Palette:
    def __init__(self, lightness):
        self._c = _Colour(lightness)

    def color(self, _role):
        return self._c


def _themed_window(lightness=240):
    w = _unannounced(_Win())
    w.palette = lambda: _Palette(lightness)
    return w


def _calls_inside(name):
    """The attribute calls inside `name`, as bare names.

    A substring check against the source cannot tell a call from the comment
    above it -- which has bitten this branch four times -- and
    `_highlight_text_position` is a CLOSURE, so `ast.walk` over the module is
    also the only way to reach it by name at all.
    """
    tree = ast.parse(io.open(
        os.path.join(REPO_ROOT, 'genizah_app.py'), encoding='utf-8').read())
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == name:
            return {c.func.attr for c in ast.walk(node)
                    if isinstance(c, ast.Call)
                    and isinstance(c.func, ast.Attribute)}
    raise AssertionError('no function named %r' % name)


def test_the_text_position_accent_names_both_halves_of_its_pair():
    for lightness in (20, 240):
        style = APP._text_position_accent(_themed_window(lightness))
        assert 'background' in style and 'color' in style, (
            'the accent leaves half the contrast pair to the theme, so the '
            'combo is unreadable on one of them (lightness=%d)' % lightness)


def test_the_text_position_accent_follows_the_theme():
    dark = APP._text_position_accent(_themed_window(20))
    light = APP._text_position_accent(_themed_window(240))
    assert dark != light, (
        'one hard-coded background for both themes is the reported bug')
    assert '#3d3522' in dark and '#fffacd' in light


def test_the_text_position_accent_keeps_its_own_border_colour():
    """It is not the Witnesses button. Sharing the PAIR is the point; sharing
    the whole rule would repaint a control this change is not about, and
    adding padding or a radius would move a toolbar row that already needs
    1423 px on a 1440 px screen."""
    style = APP._text_position_accent(_themed_window())
    assert '#f39c12' in style, 'the Text Position border changed colour'
    assert 'padding' not in style and 'border-radius' not in style, (
        'the accent now changes the combo metrics, not just its colours')


def test_a_raising_palette_still_yields_an_accent():
    """These are recomputed during construction and teardown too."""
    w = _unannounced(_Win())

    def _boom():
        raise RuntimeError('no palette yet')

    w.palette = _boom
    assert 'background' in APP._text_position_accent(w)


def test_both_accents_read_one_pair():
    """A second hand-rolled copy is a second chance to make the same
    mistake -- which is how there came to be two of them."""
    for name in ('_witness_button_accent', '_text_position_accent'):
        assert '_accent_amber_pair' in _calls_inside(name), (
            '%s computes its own colours again' % name)


def test_the_highlight_closure_reaches_the_helper():
    """Reached, not merely correct: the rule lived inline in a closure, where
    no test could see it and the palette was never consulted."""
    calls = _calls_inside('_highlight_text_position')
    assert '_text_position_accent' in calls, (
        'the closure still hard-codes its own stylesheet')


def test_the_default_position_clears_the_accent_rather_than_repainting_it():
    """`Anywhere` is not a filter, so the control must go back to the
    theme's own look -- an accent recoloured to "normal" is still a control
    the theme no longer owns."""
    seg = _function_source('_highlight_text_position')
    head, _, _ = seg.partition('else:')
    assert 'setStyleSheet("")' in head, (
        'the default position no longer clears the stylesheet')


# ---------------------------------------------------------------------------
# Codex review round 12. Three findings, all real, all introduced by the
# letter-level default and its one-shot markers.
# ---------------------------------------------------------------------------

def test_an_explicit_click_survives_the_build_it_asked_for():
    """The sharpest of the three. With no index, clicking letter-level
    records a CHOICE (correctly -- a person moved the control), reverts to
    chunk and offers the build. Ten minutes later the build succeeds and the
    default cannot help, because the default's whole rule is that it never
    overrules a choice. The user is handed back the method they had just
    asked to leave."""
    seg = _function_source('_on_comp_method_changed')
    head, _, _ = seg.partition('_revert_comp_method_to_chunk')
    assert "_comp_method_deferred = 'passage'" in head, (
        'the intent is not recorded before the demotion, so nothing can '
        'bring it back')


def test_the_build_honours_the_deferral_before_the_default():
    """Order matters: the deferral carries an explicit choice, the default
    only fills a vacuum. Reached, not merely correct -- the deferral used to
    live inline in the load handler, which a build never runs."""
    calls = _calls_inside('_on_passage_build_finished')
    assert '_honour_deferred_comp_method' in calls, (
        'a finished build never re-applies the method that asked for it')
    seg = _function_source('_on_passage_build_finished')
    assert (seg.index('_honour_deferred_comp_method')
            < seg.index('_apply_default_comp_method')), (
        'the default runs first and returns, so the deferral is moot')


def test_the_deferral_is_one_helper_for_both_moments():
    calls = _calls_inside('_on_passage_loaded')
    assert '_honour_deferred_comp_method' in calls, (
        'the load handler grew its own second copy of the deferral')


def test_a_deferred_passage_choice_is_applied_once_the_index_exists(
        monkeypatch):
    w = _default_window(monkeypatch)
    w._comp_method_deferred = 'passage'
    APP._honour_deferred_comp_method(w)
    assert w.comp_method_combo.currentData() == 'passage'
    assert w.applied == [True], 'the mode UI did not follow the method'
    assert w._comp_method_deferred is None, 'the deferral must be one-shot'


def test_a_deferral_is_refused_while_letter_level_still_cannot_run(
        monkeypatch):
    """A demotion for scope or Lab Mode reflects real current state."""
    w = _default_window(monkeypatch, scope='local')
    w._comp_method_deferred = 'passage'
    APP._honour_deferred_comp_method(w)
    assert w.comp_method_combo.currentData() == 'chunk'


def test_a_stored_method_is_read_before_the_restore_can_bail_out():
    """`restore_mode='never'`, a declined prompt and a no-data start all
    return before `_restore_comp_passage_preferences` runs -- so nothing
    marked a stored `chunk` as chosen, and the default in the `finally`
    then overrode a preference the user had actually expressed."""
    seg = _function_source('_restore_session')
    head, _, _ = seg.partition("if restore_mode == 'never':")
    assert '_apply_persistent_session_preferences' in head, (
        'the persistent-preference path no longer precedes the early exits')
    pref = _function_source('_apply_persistent_session_preferences')
    assert '_comp_method_user_choice_seen' in pref, (
        'a stored method is never marked as chosen on the path that '
        'survives every early return')


def _persist_window(stored):
    w = _unannounced(_Win())
    w._local_file_optouts = set()
    w.corpus_scope_combo = _Combo([('genizah', ''), ('local', ''),
                                   ('all', '')], index=0)
    w.comp_corpus_scope_combo = _Combo([('genizah', ''), ('local', ''),
                                        ('all', '')], index=0)
    state = {'composition_search': {}}
    if stored is not None:
        state['composition_search']['comp_method'] = stored
    APP._apply_persistent_session_preferences(w, state)
    return w


def test_a_stored_chunk_survives_a_session_restore_that_never_ran():
    w = _persist_window('chunk')
    assert w._comp_method_user_choice_seen is True, (
        "the user's stored chunk is treated as no preference, so the "
        'letter-level default overrides it')


def test_a_stored_passage_survives_it_too():
    """Same footing as the corpus scope (Phase 110): a preference has to
    outlive `restore_mode='never'`. The index has not loaded yet, so it goes
    through the same deferral the full restore uses."""
    w = _persist_window('passage')
    assert w._comp_method_user_choice_seen is True
    assert w._comp_method_deferred == 'passage'


def test_no_stored_method_still_means_no_preference():
    """The case the default exists for. It must stay free to apply."""
    w = _persist_window(None)
    assert getattr(w, '_comp_method_user_choice_seen', False) is False
    assert getattr(w, '_comp_method_deferred', None) is None


def test_a_marker_is_not_spent_while_its_tab_is_out_of_sight(monkeypatch):
    """`create_composition_tab` runs the updaters before the tab has been
    added to `self.tabs` at all. On a machine with a corpus and no
    letter-level index that spent the method marker during CONSTRUCTION, so
    a user who never opened Composition Search that launch lost it
    permanently without ever seeing it."""
    w = _announce_window(monkeypatch, seen=False)
    w.tabs = _CurrentTab(object())          # some other tab is in front
    APP._update_comp_method_affordance(w)
    assert w.writes == [], (
        'the marker was spent on a tab the user is not looking at')


def test_a_marker_with_no_tabs_at_all_is_not_spent(monkeypatch):
    """Construction proper: `self.tabs` does not exist yet."""
    w = _announce_window(monkeypatch, seen=False)
    del w.tabs
    APP._update_comp_method_affordance(w)
    assert w.writes == []


def test_arriving_on_the_tab_spends_what_is_showing():
    """Reached, not merely correct: without this the marker would be shown
    forever, because the only place that spends it is a visibility test that
    was false when the badge was computed."""
    calls = _calls_inside('_on_tab_changed')
    for name in ('_update_comp_method_affordance',
                 '_update_witness_affordance'):
        assert name in calls, (
            'entering the composition tab does not recompute %s, so a '
            'marker computed off-tab is never spent' % name)
