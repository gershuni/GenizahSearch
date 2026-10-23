# -*- coding: utf-8 -*-
"""Desktop search-within: a step searched within a step that completing cannot fix (a
Responsa star search that left crossings out by its own rule, plan §7 Phase 3 point 8) is
"+" and cannot be completed either, unless its own candidate limit cut it (Codex round 2 on
PR #442). Pattern: the unbound GenizahGUI method bound to a stub, no QApplication.
"""
from __future__ import annotations

import os
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

import genizah_app  # noqa: E402
from shared.refinement import RefinementStep  # noqa: E402

APP = genizah_app.GenizahGUI
CLEAN = {'capped': False, 'interrupted': False}
PARTIAL = {'capped': True, 'interrupted': False, 'partial': True, 'limited': False}
# Stopped after it had left crossings out by rule: the stop left candidates out too.
STOPPED_PARTIAL = {'capped': False, 'interrupted': True, 'partial': True, 'limited': False}


class _Host:
    _new_step_marks = APP._new_step_marks
    _run_left_matches_out = APP._run_left_matches_out
    _run_only_partial = APP._run_only_partial

    def __init__(self, cutoff, chain):
        self._search_cutoff = cutoff
        self.refinement_chain = chain


def _step(capped, partial):
    return RefinementStep(query='q', mode='literal', result_count_capped=capped, result_partial=partial)


@pytest.mark.parametrize('parent, cutoff, marks', [
    (_step(True, True), CLEAN, (True, True)),                         # within a partial step
    (_step(True, True), {'capped': True, 'interrupted': False}, (True, False)),   # its own limit: completable
    (_step(True, True), {'capped': False, 'interrupted': True}, (True, False)),   # stopped: not partial
    (_step(True, False), CLEAN, (True, False)),                       # within a completable step
    (_step(False, False), PARTIAL, (True, True)),                     # its own crossings left out
    (_step(False, False), STOPPED_PARTIAL, (True, False)),            # ...and then stopped: completable
    (_step(False, False), CLEAN, (False, False)),
])
def test_a_new_step_inherits_what_its_parent_cannot_complete(parent, cutoff, marks):
    assert _Host(cutoff, [parent])._new_step_marks() == marks


def test_a_first_step_has_only_its_own_marks():
    assert _Host(PARTIAL, [])._new_step_marks() == (True, True)
    assert _Host(CLEAN, [])._new_step_marks() == (False, False)


@pytest.mark.parametrize('cutoff, partial', [(PARTIAL, True), (STOPPED_PARTIAL, False), (CLEAN, False)])
def test_a_stopped_star_search_can_be_completed(cutoff, partial):
    # Codex round 3 on PR #443: a star search that left crossings out and was then
    # stopped kept result_partial, so Search within did not run it again, though the
    # stop left out candidates completing reads. The chain's first step
    # (_run_only_partial) and a step within it (_new_step_marks) both say so.
    assert _Host(cutoff, [])._run_only_partial() is partial
    assert _Host(cutoff, [])._new_step_marks()[1] is partial


class _Session:
    _forget_responsa_notes = APP._forget_responsa_notes


@pytest.mark.parametrize('cutoff, rows, saved', [
    (PARTIAL, 2, {'capped': True, 'interrupted': False, 'partial': True, 'limited': False}),
    # The session keeps the first rows only: completing reads the rest (limited).
    (PARTIAL, 6, {'capped': True, 'interrupted': False, 'partial': True, 'limited': True}),
    (CLEAN, 2, {'capped': False, 'interrupted': False}),
    (CLEAN, 6, {'capped': True, 'interrupted': False}),
])
def test_the_session_keeps_whether_completing_can_fix_the_list(cutoff, rows, saved):
    # Codex round 3 on PR #442: a restored star search kept only "+", so a search within
    # it ran it again with every candidate.
    from shared.refinement import saved_cutoff
    assert saved_cutoff(cutoff, rows, 5) == saved


def test_the_pattern_count_goes_with_the_other_responsa_notes():
    # Codex round 3 on PR #442: a tag search's line after an all-star search spoke of the
    # star search's patterns.
    host = _Session()
    host._responsa_warning, host._responsa_expanded_count, host._responsa_pattern_count = 'w', 3, 2
    host._forget_responsa_notes()
    assert (host._responsa_warning, host._responsa_expanded_count, host._responsa_pattern_count) == (None, 0, 0)


def test_the_saved_session_carries_the_partial_state(monkeypatch):
    import types
    import shared.session_persistence as session_persistence
    written = []
    monkeypatch.setattr(session_persistence, "save_session_state", lambda state: written.append(state) or True)
    monkeypatch.setattr(genizah_app, "load_app_config", lambda: {"restore_mode": "ask"})
    fake = types.SimpleNamespace(_restoring_session=False, _local_file_optouts=set(),
                                 _search_corpus_scope="genizah", _is_browsing_local=lambda: False,
                                 _search_cutoff=dict(PARTIAL), last_results=[{}])
    APP._save_session(fake)
    assert written and written[0]['regular_search']['search_cutoff'] == {
        'capped': True, 'interrupted': False, 'partial': True, 'limited': False}
