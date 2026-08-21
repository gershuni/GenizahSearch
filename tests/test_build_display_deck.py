# -*- coding: utf-8 -*-
"""The display-policy deck's view construction and sampling arithmetic.

The interleave carries a real risk: an earlier version spun forever when one
side ran dry on its turn (the loop's guard checked only whether EITHER side
had candidates left, not whether the side whose turn it was could be served).
These tests pin the termination and the budget semantics.
"""
from __future__ import annotations

import importlib.util
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, 'scripts'))

_spec = importlib.util.spec_from_file_location(
    'build_display_deck',
    os.path.join(ROOT, 'scripts', 'build_display_deck.py'))
bdd = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(bdd)


def test_interleave_alternates_and_dedupes_across_sides():
    # 'dup' sits at a[1] and b[0]. B's first turn comes before A reaches
    # index 1, so B takes it; A's pointer then skips it and lands on 'a2'.
    # Pinning the exact sequence pins alternation, cross-side dedup, and
    # which side each slot came from -- a count-based assertion cannot,
    # because a shared id has no side in its name.
    a = ['a1', 'dup', 'a2', 'a3', 'a4', 'a5', 'a6']
    b = ['dup', 'b1', 'b2', 'b3', 'b4', 'b5']
    out = bdd.interleave(a, b, half=3)
    assert out == ['a1', 'dup', 'a2', 'b1', 'a3', 'b2'], out
    assert len(out) == len(set(out)), 'a manuscript appears twice'


def test_interleave_terminates_when_one_side_is_exhausted():
    # The old infinite-loop shape: A dry on its turn, B still has candidates.
    out = bdd.interleave([], ['b1', 'b2', 'b3'], half=5)
    assert out == ['b1', 'b2', 'b3']
    out2 = bdd.interleave(['a1'], ['b1', 'b2', 'b3', 'b4'], half=5)
    assert out2[0] == 'a1' and len(out2) == 5, out2


def test_interleave_respects_the_per_side_cap():
    # A has plenty; it still may not exceed `half`, and the view stays short
    # rather than letting one side take the whole budget.
    out = bdd.interleave([f'a{i}' for i in range(20)], ['b1'], half=3)
    assert out.count('b1') == 1
    assert sum(1 for x in out if x.startswith('a')) == 3
    assert len(out) == 4


def test_interleave_is_empty_when_both_sides_are():
    assert bdd.interleave([], [], half=5) == []


def test_build_views_truncates_to_depth_and_uses_the_right_configs():
    cand = {'q1': {'STD': [f's{i}' for i in range(15)],
                   'WIDE': [f'w{i}' for i in range(15)],
                   'CH': ['c0', 'c1']}}
    v = bdd.build_views(cand, 'STD', 'WIDE', 'CH', depth=10, half=5)
    assert v['S']['q1'] == [f's{i}' for i in range(10)]
    assert v['W']['q1'] == [f'w{i}' for i in range(10)]
    # C5: 5 from wide, 2 available from chunk
    assert sum(1 for x in v['C5']['q1'] if x.startswith('w')) == 5
    assert sum(1 for x in v['C5']['q1'] if x.startswith('c')) == 2


def test_stable_rand_is_deterministic_and_order_independent_of_dict():
    a = bdd.stable_rand('salt', 'S|1-3|<60', 'q1', 'sys1')
    b = bdd.stable_rand('salt', 'S|1-3|<60', 'q1', 'sys1')
    c = bdd.stable_rand('salt', 'S|1-3|<60', 'q1', 'sys2')
    assert a == b and 0.0 <= a < 1.0
    assert a != c


def test_span_band_threshold_is_the_measured_sixty_letters():
    # The band edge is load-bearing: it comes from the graded finding that
    # short shared spans are the formulas. If someone moves it, the strata
    # stop matching the evidence that justified them.
    assert bdd.SHORT_SPAN == 60
