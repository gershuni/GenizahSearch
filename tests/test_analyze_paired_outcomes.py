# -*- coding: utf-8 -*-
"""The paired-analysis script, pinned on hand-computable synthetic data.

This is the analysis code the holdout pre-registration freezes
(docs/specs/parallels-holdout-prereg.md), so its behavior is pinned BEFORE any
holdout data exists: the discordant table, the clustering unit, determinism,
and the refuse-to-guess paths for corrupt dumps.
"""
from __future__ import annotations

import importlib.util
import json
import os
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

_spec = importlib.util.spec_from_file_location(
    'analyze_paired_outcomes',
    os.path.join(ROOT, 'scripts', 'analyze_paired_outcomes.py'))
apo = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_spec and apo)


def _o(qid, rank, **strata):
    return {'query_id': qid, 'rank': rank, 'n_returned': 1, 'seconds': 0.0,
            'strata': strata}


def test_paired_table_is_exact():
    # 6 queries: A hits 4, B hits 3; both=2, onlyA=2, onlyB=1, neither=1.
    a = {f'q{i}': _o(f'q{i}', r)
         for i, r in enumerate([0, 0, 0, 0, None, None])}
    b = {f'q{i}': _o(f'q{i}', r)
         for i, r in enumerate([0, 0, None, None, 0, None])}
    t = apo.paired_table(a, b, sorted(a), k=50)
    assert t == {'both': 2, 'only_a': 2, 'only_b': 1, 'neither': 1}


def test_rank_at_k_boundary_is_exclusive():
    a = {'q0': _o('q0', 49), 'q1': _o('q1', 50)}
    assert apo.hit(a['q0'], 50) is True
    assert apo.hit(a['q1'], 50) is False


def test_cluster_unit_is_the_group_not_the_query():
    # Two groups of two siblings each; group w1 all-hit for A only, group w2
    # all-hit for B only. The bootstrap must resample at group grain: with
    # only 2 groups the resampled diff can only ever be -1, 0, or +1 --
    # query-grain resampling would produce fractional diffs like +-0.5 from
    # splitting a group.
    a = {'wit:w1#0': _o('wit:w1#0', 0), 'wit:w1#1': _o('wit:w1#1', 0),
         'wit:w2#0': _o('wit:w2#0', None), 'wit:w2#1': _o('wit:w2#1', None)}
    b = {'wit:w1#0': _o('wit:w1#0', None), 'wit:w1#1': _o('wit:w1#1', None),
         'wit:w2#0': _o('wit:w2#0', 0), 'wit:w2#1': _o('wit:w2#1', 0)}
    boot = apo.cluster_bootstrap_diff(a, b, sorted(a), k=50,
                                      resamples=500, seed=7)
    assert boot['n_groups'] == 2
    assert boot['diff'] == 0.0
    # every resampled statistic must be one of the three group-grain values
    lo, hi = boot['ci95_two_sided']
    assert lo in (-1.0, 0.0, 1.0) and hi in (-1.0, 0.0, 1.0)


def test_determinism_same_seed_same_bytes():
    a = {f'q{i}': _o(f'q{i}', 0 if i % 3 else None) for i in range(30)}
    b = {f'q{i}': _o(f'q{i}', 0 if i % 2 else None) for i in range(30)}
    r1 = apo.analyze(a, b, 50, 0.03, 300, 20260821, min_stratum_n=100)
    r2 = apo.analyze(a, b, 50, 0.03, 300, 20260821, min_stratum_n=100)
    assert json.dumps(r1, sort_keys=True) == json.dumps(r2, sort_keys=True)


def test_small_stratum_reports_insufficient_never_pools():
    a = {f'q{i}': _o(f'q{i}', 0, language='ja' if i < 3 else 'he')
         for i in range(30)}
    b = {f'q{i}': _o(f'q{i}', 0, language='ja' if i < 3 else 'he')
         for i in range(30)}
    r = apo.analyze(a, b, 50, 0.03, 200, 1, min_stratum_n=10)
    assert r['strata']['language']['ja'] == {'n': 3,
                                             'verdict': 'INSUFFICIENT'}
    assert r['strata']['language']['he']['n'] == 27


def test_duplicate_rows_in_a_dump_refuse_to_guess(tmp_path):
    p = tmp_path / 'dump.jsonl'
    row = {'config_id': 'a', 'query_id': 'q1', 'rank': 0,
           'n_returned': 1, 'seconds': 0.0, 'strata': {}}
    p.write_text(json.dumps(row) + '\n' + json.dumps(row) + '\n',
                 encoding='utf-8')
    with pytest.raises(SystemExit):
        apo.load(str(p))
