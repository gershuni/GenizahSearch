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


# ---------------------------------------------------------------------------
# --strict holdout mode (Codex C3: "not encoded as a fail-closed decision")
# ---------------------------------------------------------------------------


def test_strict_query_id_mismatch_is_fatal_no_intersection_fallback():
    a = {f'q{i}': _o(f'q{i}', 0) for i in range(20)}
    b = {f'q{i}': _o(f'q{i}', 0) for i in range(19)}  # missing q19
    # Non-strict: proceeds on the intersection (unchanged behaviour).
    r = apo.analyze(a, b, 50, 0.03, 50, 1, min_stratum_n=1, strict=False)
    assert r['n'] == 19
    # Strict: the same mismatch is fatal, no fallback.
    with pytest.raises(SystemExit):
        apo.analyze(a, b, 50, 0.03, 50, 1, min_stratum_n=1, strict=True)


def test_strict_strata_mismatch_is_fatal():
    a = {f'q{i}': _o(f'q{i}', 0, language='he') for i in range(20)}
    b = {f'q{i}': _o(f'q{i}', 0, language='he') for i in range(20)}
    b['q5']['strata'] = {'language': 'ja'}  # disagrees with A on this query
    with pytest.raises(SystemExit):
        apo.analyze(a, b, 50, 0.03, 50, 1, min_stratum_n=1, strict=True)
    # identical strata (the fixed-up case) must NOT raise.
    b['q5']['strata'] = {'language': 'he'}
    r = apo.analyze(a, b, 50, 0.03, 50, 1, min_stratum_n=1, strict=True)
    assert r['strict'] is True


def test_expect_cells_missing_cell_blocks_all_cells_pass():
    a = {f'q{i}': _o(f'q{i}', 0, language='he') for i in range(120)}
    b = {f'q{i}': _o(f'q{i}', 0, language='he') for i in range(120)}
    # Declared cell 'ja' never appears in the data at all -> missing.
    cells = [{'stratum': 'language', 'value': 'he', 'min_n': 100},
             {'stratum': 'language', 'value': 'ja', 'min_n': 100}]
    r = apo.analyze(a, b, 50, 0.03, 50, 1, min_stratum_n=1,
                    expect_cells=cells)
    by_val = {c['value']: c for c in r['expect_cells']}
    assert by_val['he']['sufficient'] is True
    assert by_val['ja'] == {'stratum': 'language', 'value': 'ja',
                            'min_n': 100, 'n': 0, 'sufficient': False}
    assert r['all_cells_pass'] is False
    assert r['overall_verdict'] == 'INSUFFICIENT-BLOCKED'


def test_expect_cells_undersized_cell_also_blocks():
    a = {f'q{i}': _o(f'q{i}', 0, language='he') for i in range(120)}
    b = {f'q{i}': _o(f'q{i}', 0, language='he') for i in range(120)}
    # 3 queries relabelled 'ja' -- present, but n=3 < min_n=100.
    for i in range(3):
        a[f'q{i}']['strata'] = {'language': 'ja'}
        b[f'q{i}']['strata'] = {'language': 'ja'}
    cells = [{'stratum': 'language', 'value': 'he', 'min_n': 100},
             {'stratum': 'language', 'value': 'ja', 'min_n': 100}]
    r = apo.analyze(a, b, 50, 0.03, 50, 1, min_stratum_n=1,
                    expect_cells=cells)
    by_val = {c['value']: c for c in r['expect_cells']}
    assert by_val['ja']['n'] == 3
    assert by_val['ja']['sufficient'] is False
    assert r['all_cells_pass'] is False
    assert r['overall_verdict'] == 'INSUFFICIENT-BLOCKED'


def test_expect_cells_all_sufficient_and_passing_verdict_non_inferior():
    a = {f'q{i}': _o(f'q{i}', 0, language='he') for i in range(120)}
    b = {f'q{i}': _o(f'q{i}', 0, language='he') for i in range(120)}
    cells = [{'stratum': 'language', 'value': 'he', 'min_n': 100}]
    r = apo.analyze(a, b, 50, 0.03, 50, 1, min_stratum_n=1,
                    expect_cells=cells)
    assert r['all_cells_pass'] is True
    assert r['overall_verdict'] == 'NON-INFERIOR'


def test_strict_requires_expect_cells_at_cli(tmp_path):
    # The dump is otherwise fully valid (matching query ids and strata for
    # both configs) so that, WITHOUT the --expect-cells gate, main() would
    # run analyze() to completion and exit 0. This isolates the gate: any
    # other SystemExit further down (mismatched configs, empty dump, etc.)
    # would also raise and could mask a mutation that disables the gate.
    dump = tmp_path / 'dump.jsonl'
    rows = []
    for cfg in ('x', 'y'):
        for i in range(5):
            rows.append({'config_id': cfg, 'query_id': f'q{i}', 'rank': 0,
                         'n_returned': 1, 'seconds': 0.0, 'strata': {}})
    dump.write_text('\n'.join(json.dumps(r) for r in rows) + '\n',
                    encoding='utf-8')

    # Sanity: the identical dump WITHOUT --strict runs to completion (exit 0
    # via SystemExit(0) from main()'s return-code wrapping is not how main()
    # itself exits -- main() returns an int; only error paths raise).
    argv_ok = ['prog', '--dump', str(dump), '--a', 'x', '--b', 'y']
    old_argv = sys.argv
    sys.argv = argv_ok
    try:
        assert apo.main() == 0
    finally:
        sys.argv = old_argv

    argv = ['prog', '--dump', str(dump), '--a', 'x', '--b', 'y', '--strict']
    sys.argv = argv
    try:
        with pytest.raises(SystemExit):
            apo.main()
    finally:
        sys.argv = old_argv


def test_unrounded_lower_bound_flips_a_verdict_the_rounded_one_would_pass():
    """A single-group synthetic case with an EXACT rational bootstrap LB.

    All 25000 queries share one group id ('grp'), so the cluster bootstrap
    (which resamples GROUPS) has only one group to draw from: every resample
    draws that same group, so every one of the `resamples` statistics equals
    the exact population value -- no randomness, no seed sensitivity.

    751 queries are onlyB (diff=-1); the remaining 24249 are both-hit
    (diff=0). raw LB = -751/25000 = -0.03004 exactly (bar float noise).
    round(-0.03004, 4) == -0.03, which PASSES a margin of 0.03 (equal, so
    >=). The true unrounded value, -0.03004, is strictly below -0.03 and
    must FAIL. If the comparison used the rounded value (the old bug), this
    verdict would be True; the fix requires it to be False.
    """
    n_only_b, n_total = 751, 25000
    a, b = {}, {}
    for i in range(n_total):
        qid = f'grp#{i:05d}'
        if i < n_only_b:
            a[qid] = _o(qid, None)   # A misses
            b[qid] = _o(qid, 0)     # B hits -> diff = 0 - 1 = -1
        else:
            a[qid] = _o(qid, 0)
            b[qid] = _o(qid, 0)     # both hit -> diff = 0
    assert apo.group_of(next(iter(a))) == 'grp'  # sanity: single group

    r = apo.analyze(a, b, 50, 0.03, 20, 1, min_stratum_n=1)
    boot = r['bootstrap']
    assert boot['n_groups'] == 1
    # The rounded display value sits exactly on the margin boundary.
    assert boot['lb95_one_sided'] == -0.03
    # The raw value is strictly more negative than -0.03.
    assert boot['lb95_one_sided_raw'] < -0.03
    assert abs(boot['lb95_one_sided_raw'] - (-0.03004)) < 1e-9
    # A rounded-value comparison would have passed (-0.03 >= -0.03 is True);
    # the fixed, unrounded comparison must fail.
    assert r['non_inferior_a_vs_b'] is False
