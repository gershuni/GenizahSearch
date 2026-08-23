# -*- coding: utf-8 -*-
"""Codex review finding #13(c): while passage matching is selected, the GUI
must disable chunk_size/mode_select/freq_threshold (in addition to the
pre-existing boundary_mode disable) -- web/search_api.py rejects a
non-default value of any of them with 400 'passage_option_unsupported' when
method='passage', so the UI must never be able to send one.

Source-text assertion, not a live NiceGUI render test: this repo has no
render-smoke harness for web/pages/parallels.py (create_parallels_page is
never imported by any existing test -- confirmed by grep before writing
this file), and building one from scratch is out of scope for this fix.
This mirrors the project's existing "source_text_assertions_pin_
misspellings" pattern (e.g. tests/test_discovery_flag.py's substring checks
against web/main.py's source) -- a lighter-weight but real guard against a
regression in the handler's own code, extracted and inspected at the AST
level rather than by import (create_parallels_page has heavy NiceGUI/page
side effects unsuited to a unit import).
"""
from __future__ import annotations

import ast

import pytest
import os
import re

PARALLELS_PAGE_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    'web', 'pages', 'parallels.py',
)


def _read_source() -> str:
    with open(PARALLELS_PAGE_PATH, encoding='utf-8') as fh:
        return fh.read()


def _on_passage_mode_change_source() -> str:
    """Extract on_passage_mode_change's own source text via the AST (not a
    regex over lines), so nested defs/indentation changes elsewhere in the
    file cannot silently widen or narrow what this test inspects."""
    tree = ast.parse(_read_source())
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == 'on_passage_mode_change':
            return ast.get_source_segment(_read_source(), node) or ''
    raise AssertionError('on_passage_mode_change not found in web/pages/parallels.py')


def test_on_passage_mode_change_exists_exactly_once():
    tree = ast.parse(_read_source())
    matches = [
        node for node in ast.walk(tree)
        if isinstance(node, ast.FunctionDef) and node.name == 'on_passage_mode_change'
    ]
    assert len(matches) == 1, 'expected exactly one on_passage_mode_change definition'


def test_passage_mode_forces_and_disables_chunk_size():
    src = _on_passage_mode_change_source()
    assert re.search(r"chunk_size\.value\s*=\s*5\b", src), (
        "on_passage_mode_change must force chunk_size to the API default (5) "
        "-- web/search_api.py rejects any other value for method='passage'")
    assert 'chunk_size.disable()' in src, (
        'chunk_size must be disabled while passage mode is selected')
    assert 'chunk_size.enable()' in src, (
        'chunk_size must be re-enabled when passage mode is deselected')


def test_passage_mode_forces_and_disables_mode_select():
    src = _on_passage_mode_change_source()
    assert re.search(r"mode_select\.value\s*=\s*'exact'", src), (
        "on_passage_mode_change must force mode_select to the API default "
        "('exact') -- web/search_api.py rejects any other value for "
        "method='passage'")
    assert 'mode_select.disable()' in src, (
        'mode_select must be disabled while passage mode is selected')
    assert 'mode_select.enable()' in src, (
        'mode_select must be re-enabled when passage mode is deselected')
    # Forcing mode_select back to 'exact' must also hide the variant-only
    # controls if 'variants' was previously selected -- on_mode_change is
    # the SAME handler mode_select's own change event calls (a programmatic
    # .value assignment does not fire NiceGUI's update:model-value event,
    # so it must be invoked explicitly; the same lesson boundary_mode /
    # passage_mode's own mutual-exclusivity handling already applies).
    assert 'on_mode_change()' in src, (
        'forcing mode_select to exact must also call on_mode_change() to '
        'hide variant_controls_col if it was showing')


def test_passage_mode_forces_and_disables_freq_threshold():
    src = _on_passage_mode_change_source()
    assert re.search(r"freq_threshold\.value\s*=\s*50\b", src), (
        "on_passage_mode_change must force freq_threshold to the page's "
        "default (50) -- passage has no per-chunk frequency signal at all")
    assert 'freq_threshold.disable()' in src, (
        'freq_threshold must be disabled while passage mode is selected')
    assert 'freq_threshold.enable()' in src, (
        'freq_threshold must be re-enabled when passage mode is deselected')


def test_passage_mode_forces_and_disables_min_chunks():
    """Owner ruling 2026-08-23 (round 3): 'Min. chunk matches' counts
    CHUNKS, which letter-level search does not have -- and the value DOES
    reach the passage searcher (as an n_spans floor, where one long
    continuous match is a single span), so any min>1 would silently drop
    exactly the strongest witnesses. Force the no-op value and disable."""
    src = _on_passage_mode_change_source()
    assert re.search(r"min_chunks_input\.value\s*=\s*1\b", src), (
        "on_passage_mode_change must force min_chunks to 1 -- the passage "
        "searcher applies it as an n_spans floor, and one long continuous "
        "match is a SINGLE span")
    assert 'min_chunks_input.disable()' in src, (
        'min_chunks_input must be disabled while letter-level is selected')
    assert 'min_chunks_input.enable()' in src, (
        'min_chunks_input must be re-enabled when chunk search is selected')


def test_passage_mode_still_forces_and_disables_boundary_mode():
    """Regression guard for the PRE-EXISTING fix (adversarial review finding
    #2) that finding #13(c) sits alongside -- must not have been lost in the
    same edit."""
    src = _on_passage_mode_change_source()
    assert re.search(r"boundary_mode\.value\s*=\s*'full'", src)
    assert 'boundary_mode.disable()' in src
    assert 'boundary_mode.enable()' in src


def test_disable_calls_are_inside_the_passage_mode_true_branch():
    """The disable() calls must be gated on `if passage_mode.value:`, not
    unconditional -- an unconditional disable would lock these controls
    even when passage mode is off."""
    src = _on_passage_mode_change_source()
    lines = src.splitlines()
    if_true_idx = next(
        i for i, ln in enumerate(lines) if re.search(r'if _letter_level_selected\(\)\s*:', ln)
    )
    else_idx = next(i for i, ln in enumerate(lines) if ln.strip() == 'else:')
    assert if_true_idx < else_idx, 'expected an if _letter_level_selected(): ... else: shape'
    true_branch = '\n'.join(lines[if_true_idx:else_idx])
    false_branch = '\n'.join(lines[else_idx:])
    for widget in ('chunk_size', 'mode_select', 'freq_threshold',
                   'boundary_mode', 'min_chunks_input'):
        assert f'{widget}.disable()' in true_branch, (
            f'{widget}.disable() must be inside the passage_mode.value branch')
        assert f'{widget}.enable()' in false_branch, (
            f'{widget}.enable() must be inside the else (not-selected) branch')


# ---------------------------------------------------------------------------
# Codex review finding #15: the page must route passage searches through
# the shared execution budget (run_passage_search), never NiceGUI's
# generic, unbounded run.io_bound.
# ---------------------------------------------------------------------------

def test_page_imports_run_passage_search_from_search_api():
    src = _read_source()
    assert 'from web.search_api import run_passage_search' in src, (
        'web/pages/parallels.py must import the SAME run_passage_search '
        'web/search_api.py exposes -- not a separately reimplemented copy')


def test_passage_branch_calls_run_passage_search_not_io_bound():
    """The passage dispatch must go through run_passage_search (which
    itself routes through the shared semaphore + dedicated executor +
    timeout), never NiceGUI's generic run.io_bound -- the actual bug this
    finding fixes."""
    tree = ast.parse(_read_source())
    execute_parallels = next(
        node for node in ast.walk(tree)
        if isinstance(node, ast.AsyncFunctionDef) and node.name == 'execute_parallels'
    )
    src = ast.get_source_segment(_read_source(), execute_parallels) or ''

    # Split on the captured_passage_mode dispatch branch.
    branch_idx = src.index('if captured_passage_mode:')
    passage_branch_and_after = src[branch_idx:]
    else_idx = passage_branch_and_after.index('\n        else:')
    passage_branch = passage_branch_and_after[:else_idx]
    chunk_lab_branch = passage_branch_and_after[else_idx:]

    assert 'run_passage_search(' in passage_branch, (
        'the passage_mode branch must call run_passage_search')
    # 'run.io_bound' alone would also match this branch's own explanatory
    # comment (which names it to say why it is NOT used) -- check for an
    # actual CALL (the opening paren) instead of the bare substring.
    assert 'run.io_bound(' not in passage_branch, (
        'the passage_mode branch must NOT dispatch through run.io_bound -- '
        'that bypasses the shared semaphore/executor/timeout entirely')
    # The lab/chunk branch is UNCHANGED by this finding -- still run.io_bound.
    assert 'run.io_bound(run_search)' in chunk_lab_branch


def test_passage_branch_handles_busy_and_timeout_with_translated_messages():
    """Codex review finding #15 explicitly requires 'a translated busy/
    timeout message in the UI' -- not a bare exception, and not English-only
    (tr() is this codebase's i18n mechanism, used throughout this page)."""
    tree = ast.parse(_read_source())
    execute_parallels = next(
        node for node in ast.walk(tree)
        if isinstance(node, ast.AsyncFunctionDef) and node.name == 'execute_parallels'
    )
    src = ast.get_source_segment(_read_source(), execute_parallels) or ''
    branch_idx = src.index('if captured_passage_mode:')
    else_idx = src.index('\n        else:', branch_idx)
    passage_branch = src[branch_idx:else_idx]

    assert 'passage_search_busy' in passage_branch
    assert 'core_timeout' in passage_branch
    # Every ui.notify string literal in this branch must be wrapped in tr().
    notify_calls = re.findall(r"ui\.notify\(\s*(tr\(|[\"'])", passage_branch)
    assert notify_calls, 'expected at least one ui.notify call in the passage branch'
    assert all(call == 'tr(' for call in notify_calls), (
        f'every ui.notify message in the passage branch must go through '
        f'tr() -- found raw-string call(s): {notify_calls}')


# ---------------------------------------------------------------------------
# PR #324 round 4: the page must surface passage truncation, and every
# passage-UI string must actually be translated.
# ---------------------------------------------------------------------------

def test_page_notifies_on_a_truncated_passage_search():
    """The API path warns (`passage_results_truncated`); this direct page
    path was the one product caller still discarding `query_report`, so a
    GUI user could mistake capped results for exhaustive ones. Source-text
    assertion, same style as the run_passage_search gate above."""
    src = _read_source()
    assert "result_data.get('query_report')" in src, (
        'the page never reads query_report -- truncation is invisible to '
        'GUI users while the API path warns'
    )
    idx = src.index("result_data.get('query_report')")
    # 1,600 chars: the block carries a long WHY comment (the Dror
    # Yikra false-alarm measurement) between the read and the notify.
    after = src[idx:idx + 1600]
    assert "candidates_truncated" in after and "verify_truncated" in after
    assert 'ui.notify(' in after, 'reading the report without telling anyone'


def test_every_passage_ui_string_has_a_hebrew_translation():
    """Phase 145 shipped its passage strings untranslated -- Hebrew users got
    English notifies (found in PR #324 round 4). Pin every passage tr()
    string on the page to a real Hebrew entry so the leak class cannot
    recur silently."""
    import re as _re

    from genizah_translations import TRANSLATIONS

    src = _read_source()
    # Every tr('...') literal mentioning passage, with implicit-concat parts
    # joined the way Python joins them.
    calls = _re.findall(
        r"tr\(\s*((?:'[^']*'\s*)+)\)", src)
    passage_strings = set()
    for parts in calls:
        joined = ''.join(_re.findall(r"'([^']*)'", parts))
        if 'assage' in joined or 'etter-level' in joined:  # passage / Letter-level
            passage_strings.add(joined)
    assert passage_strings, 'the extractor matched nothing -- vacuous gate'
    missing = sorted(s for s in passage_strings if s not in TRANSLATIONS)
    assert not missing, (
        'passage UI strings with no Hebrew translation (Hebrew users get '
        'English): ' + repr(missing)
    )


def test_the_truncation_string_used_matches_its_translation_key():
    """The notify builds its string by implicit concatenation; a one-space
    drift between the source and the translations key silently reverts
    Hebrew users to English. Pin the exact joined string."""
    from genizah_translations import TRANSLATIONS

    key = ('Letter-level search checked the {n} best-evidenced '
           'candidates of {m}.')
    assert key in TRANSLATIONS, 'the exact notify string must be a key'
    # And the Hebrew side must keep both placeholders, or .format() on the
    # translated string drops the numbers for Hebrew users only.
    assert '{n}' in TRANSLATIONS[key] and '{m}' in TRANSLATIONS[key]


# ---------------------------------------------------------------------------
# Owner ruling 2026-08-23: letter-level search is the DEFAULT method.
# ---------------------------------------------------------------------------

def test_letter_level_is_the_default_method():
    """The radio must default to 'passage' (letter-level), with chunk as the
    explicit alternative -- and fall back to 'chunk' only when the index is
    unavailable. Source-text pins, same style as the rest of this file."""
    src = _read_source()
    assert 'method_radio = ui.radio(' in src
    idx = src.index('method_radio = ui.radio(')
    creation = src[idx:idx + 400]
    assert "value='passage'" in creation, 'letter-level must be pre-selected'
    assert "method_radio.value = 'chunk'" in src, (
        'the unavailable-index fallback must pin the value to chunk'
    )


def test_the_default_selection_state_is_applied_on_load():
    """Letter-level is pre-selected, so the chunk controls must START
    disabled -- the handler has to run once at build time, not wait for the
    first user toggle.

    And the call must sit AFTER every widget the handler closes over: the
    first version invoked it right after the handler definition, and the
    whole page died with NameError at build time (owner-reported,
    2026-08-23) because mode_select/chunk_size/freq_threshold are created
    BELOW the selector block. Source-order is exactly what this pins.
    """
    src = _read_source()
    init_at = src.rindex('on_passage_mode_change()')
    for widget in ("mode_select = ui.select",
                   "chunk_size = ui.slider",
                   "freq_threshold = ui.slider",
                   "min_chunks_input = ui.number"):
        assert widget in src, f'anchor {widget!r} vanished -- rewrite this pin'
        assert src.index(widget) < init_at, (
            f'the build-time on_passage_mode_change() call precedes '
            f'{widget!r}; the handler will NameError during page build'
        )


# ---------------------------------------------------------------------------
# Owner rulings 2026-08-23 (round 3): width-ladder terminology and tooltip
# scope. The 'assage'/'etter-level' i18n gate above cannot see the width
# labels (none contain either substring), so they get their own pins.
# ---------------------------------------------------------------------------

def _passage_width_creation_slice() -> str:
    src = _read_source()
    idx = src.index('passage_width = ui.select(')
    end = src.index('if not passage_available():', idx)
    return src[idx:end]


def test_width_ladder_has_no_rung_above_widest():
    """Owner ruling: 'widest and then maximal means this is not the
    widest'. The 1.8 preset's label must not claim to be the extreme while
    the 2.0 option sits above it."""
    slice_ = _passage_width_creation_slice()
    assert "tr('Very wide (default)')" in slice_, (
        'the widest-40 preset label must be Very wide (default)')
    assert 'Widest' not in slice_, (
        "a 'Widest' label with a 'Maximal' rung above it is a contradiction")
    assert "tr('Maximal (may add noise)')" in slice_


def test_every_width_control_string_has_a_hebrew_translation():
    from genizah_translations import TRANSLATIONS

    slice_ = _passage_width_creation_slice()
    calls = re.findall(r"tr\(\s*((?:'[^']*'\s*)+)\)", slice_)
    strings = []
    for parts in calls:
        strings.append(''.join(re.findall(r"'([^']*)'", parts)))
    # 5 ladder labels + the select's own label + the width tooltip.
    assert len(strings) >= 7, f'extractor matched too little: {strings}'
    missing = sorted(s for s in strings if s not in TRANSLATIONS)
    assert not missing, (
        'width-control strings with no Hebrew translation: ' + repr(missing))


def test_the_tooltip_does_not_scope_to_the_genizah_corpus():
    """Owner ruling: the website is Genizah-only everywhere, so the scope
    note is noise. (Desktop, which really has local/all corpus scopes, gets
    its own scoped wording with the 146B method selector.)"""
    assert 'Genizah corpus only' not in _read_source()


# ---------------------------------------------------------------------------
# PR #325 round 3 (Codex P2): the search fingerprint IS the identity the
# export preserve/recover logic trusts across reloads and tabs. Every input
# that changes the returned buckets must be hashed, canonicalized -- one
# omitted input means two tabs differing only in it share a fingerprint,
# and a reload can restore the other tab's rows.
# ---------------------------------------------------------------------------

def _fingerprint_call_slice() -> str:
    """The fresh-search call to the identity helper, arguments included."""
    src = _read_source()
    idx = src.index('_search_fingerprint = compute_parallels_search_fingerprint(')
    end = src.index('\n                    )', idx)
    return src[idx:end]


def test_fingerprint_call_passes_every_result_affecting_input():
    """The page must hand the helper every input that changes the returned
    buckets. (What the helper DOES with them is proven by execution in
    tests/test_parallels_fingerprint.py -- this pin only guards the wiring.)"""
    slice_ = _fingerprint_call_slice()
    required = [
        'text', 'engine', 'width', 'chunk_size', 'mode', 'max_freq',
        'filter_text', 'deep_scan',
        'boundary_mode', 'boundary_delimiter', 'boundary_boost',
        'min_boundary_matches', 'min_delimiter_distance',
        'variant_level', 'variant_max_changes',
        'library_mode', 'library_filter', 'restrict', 'excluded',
        'filters',
    ]
    missing = [k for k in required if f'{k}=' not in slice_]
    assert not missing, (
        'the fingerprint call omits result-affecting input(s) -- two tabs '
        'differing only in these would swap rows across a reload: '
        + repr(missing))


def test_fingerprint_call_reads_no_live_widget():
    """Round 5 (Codex P2) generalized: only the Run button is disabled during
    the await, so ANY `.value` read at this call site describes post-edit
    state the engine never used. Everything must be a dispatch capture."""
    slice_ = _fingerprint_call_slice()
    offenders = [ln.strip() for ln in slice_.splitlines() if '.value' in ln]
    assert not offenders, (
        'live widget read(s) at the fingerprint call site: ' + repr(offenders))


def test_fingerprint_call_reads_no_live_page_state():
    """Workflow review: library_mode / library_filter / excluded_manuscript_ids
    were read live from p_state AFTER the await. p_state is mutated by chip
    clicks, so those reads could describe a scope the search never used --
    and the post-search 'hide' pass must filter by the same captures it is
    fingerprinted with."""
    slice_ = _fingerprint_call_slice()
    offenders = [ln.strip() for ln in slice_.splitlines() if 'p_state.' in ln]
    assert not offenders, (
        'live p_state read(s) at the fingerprint call site: ' + repr(offenders))


def test_the_library_hide_pass_uses_the_same_captures_it_is_hashed_with():
    """If the filter reads live p_state while the identity hashes captures,
    the fingerprint describes rows the filter did not produce."""
    src = _read_source()
    assert ("if captured_library_mode == 'hide' and captured_library_filter:"
            in src), (
        'the post-search library filter must use the dispatch captures')


def test_fingerprint_and_meta_use_the_dispatched_text():
    """Round 4 (Codex P2): the textarea stays editable during the await, so
    reading text_input.value after it fingerprints text that was never
    searched -- colliding with a tab that really searched the edited text."""
    slice_ = _fingerprint_call_slice()
    assert 'text=text,' in slice_, (
        'the fingerprint must hash the dispatched text')

    # Two sites build _parallels_search_meta; the history-restore one
    # correctly echoes its stored snapshot. Pin the FRESH-SEARCH one.
    src = _read_source()
    fp_at = src.index('_search_fingerprint = compute_parallels_search_fingerprint(')
    meta_at = src.index('_parallels_search_meta = {', fp_at)
    meta_slice = src[meta_at:meta_at + 200]
    assert "'source_text': text," in meta_slice, (
        'export meta must echo the dispatched text, not the live textarea')


def test_history_restore_stamps_its_own_identity():
    """Workflow review (P1): the composition-history restore wrote the export
    payload but never set p_state.search_fingerprint, so the next snapshot
    persist stamped the restored rows with the PREVIOUS search's identity --
    and a later reload could recover that unrelated search's payload."""
    src = _read_source()
    handler_at = src.index("'warnings': ['restored-from-history'],")
    after = src[handler_at:handler_at + 1800]
    assert 'compute_parallels_search_fingerprint(' in after, (
        'history restore must compute an identity through the shared helper')
    assert 'p_state.search_fingerprint = ' in after, (
        'history restore must stamp p_state, or the stale fingerprint from '
        'the previous search survives into the snapshot')


def test_the_legacy_bootstrap_preserves_the_richer_payload():
    """Workflow review (P1): the no-snapshot branch (a second browser tab is
    enough to reach it) called set_parallels_export directly, overwriting the
    up-to-5,000-row payload with the 250-row user fallback. Harmless while
    results were capped near 200; this PR's uncapped fetch made it lossy."""
    src = _read_source()
    boot_at = src.index("_bootstrap_meta = {'source_text': _legacy_source_text}")
    after = src[boot_at:boot_at + 2600]  # window covers the round-6 identity-carry block too
    assert 'preserve_or_set_parallels_export(' in after, (
        'the bootstrap branch must not clobber a richer same-search payload')
    assert 'recover_richer_parallels_rows(' in after, (
        'the bootstrap branch should recover the payload tail, like the '
        'snapshot branch above it')
    # Scan CODE only: this block's own comment names the old writer to
    # explain why it is gone, and a substring check over comments is
    # how a gate goes vacuous (or, here, falsely red).
    code = [ln for ln in after.splitlines()
            if not ln.lstrip().startswith('#')]
    bare_writer = [ln for ln in code
                   if 'set_parallels_export(' in ln
                   and 'preserve_or_set_parallels_export(' not in ln]
    assert not bare_writer, (
        'the unconditional writer is still reachable in this branch: '
        + repr(bare_writer))


def test_programmatic_boundary_write_calls_its_handler():
    """NiceGUI fires no event for a programmatic .value write, so the
    boundary help/stats/Advanced-button state stayed as the user last left
    it while letter-level forced 'full'."""
    src = _on_passage_mode_change_source()
    idx = src.index("boundary_mode.value = 'full'")
    after = src[idx:idx + 700]
    assert 'update_boundary_ui()' in after, (
        'forcing boundary_mode without calling its handler leaves the help '
        'text, stats line and Advanced button describing the old mode')


# The page must actually BUILD. Source-text pins cannot execute closures; the
# NameError that took the whole /parallels page down (owner-reported,
# 2026-08-23: on_passage_mode_change() invoked at build time before
# chunk_size existed) sailed through every source-level gate in this file.
# This is the render-executed test that catches that class.
# ---------------------------------------------------------------------------

_SIM_READY = False


def _ensure_sim():
    global _SIM_READY
    if not _SIM_READY:
        from nicegui.testing.general import prepare_simulation

        prepare_simulation()
        _SIM_READY = True


@pytest.mark.parametrize('available', [True, False])
def test_the_page_builds_without_raising(available, monkeypatch):
    """Build /parallels with the passage index present and absent. Any
    NameError/AttributeError in a build-time code path -- including handler
    invocations like the load-time on_passage_mode_change() -- fails here
    the way it failed for the owner, instead of in production."""
    import asyncio

    import web.pages.parallels as pp
    from web.translations import set_language

    _ensure_sim()
    from nicegui import core, ui
    from nicegui.client import Client

    monkeypatch.setattr(pp, 'passage_available', lambda: available)
    set_language('he')
    try:
        async def _run():
            core.loop = asyncio.get_running_loop()
            with Client(ui.page('/_parallels_probe')) as client:
                with client:
                    pp.create_parallels_page()
            return client

        client = asyncio.run(_run())
    finally:
        set_language('he')
    # The method radio must exist exactly when the index is available.
    radios = [el for el in client.elements.values()
              if type(el).__name__ == 'Radio']
    # `>= 0` would have been vacuous for the unavailable arm (workflow
    # review): assert what each arm actually claims.
    if available:
        assert len(radios) >= 1, 'the method radio must exist'
        assert any(getattr(r, 'value', None) == 'passage' for r in radios), (
            'letter-level must be the pre-selected method'
        )
    else:
        assert all(getattr(r, 'value', None) != 'passage' for r in radios), (
            'with no passage index, no radio may sit on the letter-level '
            'value -- the page would send a method the backend rejects'
        )


# =========================================================================
# Round 6 (Codex P2): the legacy fallback carries its identity, so the
# mixed-pair rule in _same_parallels_search can verify instead of trusting
# source_text.
# =========================================================================

def test_the_fallback_write_stamps_its_identity_beside_it():
    src = _read_source()
    flat = ' '.join(src.split())
    assert ("safe_user_set('parallels_results_fingerprint', "
            '_search_fingerprint)') in flat, (
        'the fresh-search fallback write must stamp '
        "parallels_results_fingerprint with the same _search_fingerprint "
        'it just used for the export payload'
    )


def test_the_bootstrap_folds_the_stamp_into_its_meta():
    src = _read_source()
    flat = ' '.join(src.split())
    assert ("_legacy_fingerprint = _safe_get( "
            "'parallels_results_fingerprint', '') or ''") in flat
    assert ("_bootstrap_meta['search_fingerprint'] = _legacy_fingerprint"
            ) in flat


def test_the_clear_path_clears_the_stamp_with_the_rows():
    """A cleared results list with a surviving stamp would label the NEXT
    legacy bootstrap's empty rows with a dead search's identity."""
    src = _read_source()
    flat = ' '.join(src.split())
    assert ("safe_user_set('parallels_results', []) "
            "safe_user_set('parallels_results_fingerprint', '')") in flat


# =========================================================================
# Round 7 (Codex P2): after a reload the controls sit at build-time
# defaults, so the restore notice must not promise that a bare re-run
# recovers the full list -- it states the original-settings condition.
# =========================================================================

def test_the_restore_notice_states_its_condition():
    src = _read_source()
    assert 'run the search again for the full list' not in src, (
        'the unconditional restore promise is back -- a reload does not'
        ' restore the controls, so a bare re-run is a DIFFERENT search')
    assert 'with its original settings' in src
