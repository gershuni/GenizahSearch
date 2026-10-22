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


class _Host:
    _new_step_marks = APP._new_step_marks
    _run_left_matches_out = APP._run_left_matches_out

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
    (_step(False, False), CLEAN, (False, False)),
])
def test_a_new_step_inherits_what_its_parent_cannot_complete(parent, cutoff, marks):
    assert _Host(cutoff, [parent])._new_step_marks() == marks


def test_a_first_step_has_only_its_own_marks():
    assert _Host(PARTIAL, [])._new_step_marks() == (True, True)
    assert _Host(CLEAN, [])._new_step_marks() == (False, False)


class _Session:
    _saved_search_cutoff = APP._saved_search_cutoff
    _forget_responsa_notes = APP._forget_responsa_notes

    def __init__(self, cutoff, rows):
        self._search_cutoff = cutoff
        self.last_results = [{}] * rows


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
    assert _Session(cutoff, rows)._saved_search_cutoff(5) == saved


def test_the_pattern_count_goes_with_the_other_responsa_notes():
    # Codex round 3 on PR #442: a tag search's line after an all-star search spoke of the
    # star search's patterns.
    host = _Session(CLEAN, 0)
    host._responsa_warning, host._responsa_expanded_count, host._responsa_pattern_count = 'w', 3, 2
    host._forget_responsa_notes()
    assert (host._responsa_warning, host._responsa_expanded_count, host._responsa_pattern_count) == (None, 0, 0)
