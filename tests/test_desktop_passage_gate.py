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

    def count(self):
        return len(self._pairs)

    def itemData(self, i):
        return self._pairs[i][0]

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
    _comp_method = APP._comp_method
    _restore_comp_passage_preferences = APP._restore_comp_passage_preferences
    _method_help_text = APP._method_help_text
    _update_comp_method_help = APP._update_comp_method_help


def _window(scope='genizah', lab=False, engine_ready=True, building=False):
    w = _Win()
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
    w = _Win()
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


def test_recursive_search_refuses_a_programmatic_passage_call():
    """The button is disabled in passage mode, but a restored session or a
    programmatic call still reaches the function. Concatenating result texts
    starves the passage engine's posting budget -- measured 48.2% against
    74.1% for the same witnesses fused."""
    seg = _function_source('run_recursive_composition')
    assert "_comp_method()" in seg and "'passage'" in seg, (
        'run_recursive_composition has no passage guard of its own')


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
    'Faster, with fewer irrelevant results. Tolerates spelling and '
    'transcription differences.',
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
