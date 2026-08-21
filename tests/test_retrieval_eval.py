# -*- coding: utf-8 -*-
"""The evaluation core's guarantees, especially the ones that are structural.

A metrics function is easy to get right and easy to misuse. What is tested
here is mostly the misuse-prevention: that the tune/holdout split cannot
drift, and that a second holdout look cannot happen quietly.
"""
from __future__ import annotations

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from shared.retrieval_eval import (  # noqa: E402
    SPLIT_HOLDOUT, SPLIT_TUNE, EvalLedger, EvalQuery, HoldoutReuse,
    QueryOutcome, evaluate, non_inferior, split_queries, summarize,
    summarize_by_stratum,
)


def _q(i: int, positives=('rec1',), **strata) -> EvalQuery:
    return EvalQuery(query_id=f'q{i}', text=f'text {i}',
                     positives=frozenset(positives), strata=strata)


# --- split -----------------------------------------------------------------

def test_split_is_stable_and_id_derived():
    q = _q(7)
    assert q.split() == q.split() == EvalQuery(
        query_id='q7', text='different text entirely',
        positives=frozenset({'x'})).split()


def test_split_does_not_drift_when_the_query_set_grows():
    small = [_q(i) for i in range(50)]
    big = [_q(i) for i in range(500)]
    a = {q.query_id: q.split() for q in small}
    b = {q.query_id: q.split() for q in big}
    assert all(a[k] == b[k] for k in a), 'assignment changed with set size'


def test_split_is_roughly_balanced_and_covers_both():
    parts = split_queries([_q(i) for i in range(600)])
    assert set(parts) == {SPLIT_TUNE, SPLIT_HOLDOUT}
    n_t, n_h = len(parts[SPLIT_TUNE]), len(parts[SPLIT_HOLDOUT])
    assert n_t + n_h == 600
    assert 0.4 < n_t / 600 < 0.6, (n_t, n_h)


def test_a_different_salt_gives_a_different_partition():
    qs = [_q(i) for i in range(300)]
    a = {q.query_id: q.split('v1') for q in qs}
    b = {q.query_id: q.split('v2') for q in qs}
    assert a != b


# --- metrics ---------------------------------------------------------------

def test_evaluate_finds_the_rank_of_the_first_positive():
    q = _q(1, positives=('good',))
    out = evaluate([q], lambda _t: ['bad', 'worse', 'good', 'good'])
    assert out[0].rank == 2 and out[0].n_returned == 4


def test_missing_positive_is_none_not_zero():
    out = evaluate([_q(1, positives=('good',))], lambda _t: ['bad'])
    assert out[0].rank is None
    s = summarize(out)
    assert s['recall@1'] == 0.0 and s['found_any'] == 0 and s['mrr'] == 0.0


def test_recall_at_k_and_mrr():
    outs = [QueryOutcome('a', 0, 5, 0.01), QueryOutcome('b', 9, 5, 0.02),
            QueryOutcome('c', None, 5, 0.03), QueryOutcome('d', 1, 5, 0.04)]
    s = summarize(outs, k_values=(1, 10))
    assert s['recall@1'] == 0.25          # only rank 0
    assert s['recall@10'] == 0.75         # ranks 0, 9, 1
    assert s['mrr'] == round((1 + 0.1 + 0 + 0.5) / 4, 4)
    assert s['n'] == 4


def test_wilson_interval_brackets_the_estimate_and_is_not_symmetric_at_edges():
    s = summarize([QueryOutcome(str(i), 0, 1, 0.0) for i in range(10)],
                  k_values=(1,))
    lo, hi = s['recall@1_ci']
    assert s['recall@1'] == 1.0
    assert lo < 1.0 and hi <= 1.0, 'a perfect score must still carry doubt'


def test_strata_summaries_split_the_population():
    qs = [_q(i, length_band='short' if i % 2 else 'long') for i in range(20)]
    outs = evaluate(qs, lambda _t: ['rec1'])
    by = summarize_by_stratum(outs, 'length_band')
    assert set(by) == {'short', 'long'}
    assert by['short']['n'] + by['long']['n'] == 20


# --- the ledger ------------------------------------------------------------

def test_holdout_is_write_once_per_method_and_policy(tmp_path):
    led = EvalLedger(str(tmp_path / 'l.jsonl'))
    summary = {'recall@50': 0.9, 'recall@50_ci': [0.85, 0.95], 'n': 100}
    led.record(method='passage', policy_id='pp1-a', split=SPLIT_HOLDOUT,
               query_set='fgp', summary=summary)
    with pytest.raises(HoldoutReuse):
        led.record(method='passage', policy_id='pp1-a', split=SPLIT_HOLDOUT,
                   query_set='fgp', summary=summary)
    # a different policy is a different estimand -- allowed
    led.record(method='passage', policy_id='pp1-b', split=SPLIT_HOLDOUT,
               query_set='fgp', summary=summary)


def test_tuning_split_may_be_scored_freely(tmp_path):
    led = EvalLedger(str(tmp_path / 'l.jsonl'))
    for _ in range(5):
        led.record(method='passage', policy_id='pp1-a', split=SPLIT_TUNE,
                   query_set='fgp', summary={'n': 1})
    assert len(led.entries) == 5


def test_forced_holdout_reuse_is_recorded_as_forced(tmp_path):
    p = str(tmp_path / 'l.jsonl')
    led = EvalLedger(p)
    s = {'n': 1}
    led.record(method='m', policy_id='p', split=SPLIT_HOLDOUT,
               query_set='q', summary=s)
    e = led.record(method='m', policy_id='p', split=SPLIT_HOLDOUT,
                   query_set='q', summary=s, force=True)
    assert e['forced'] is True
    assert sum(1 for x in EvalLedger(p).entries if x['forced']) == 1


def test_ledger_survives_a_new_process(tmp_path):
    p = str(tmp_path / 'l.jsonl')
    EvalLedger(p).record(method='m', policy_id='p', split=SPLIT_HOLDOUT,
                         query_set='q', summary={'n': 1})
    with pytest.raises(HoldoutReuse):
        EvalLedger(p).record(method='m', policy_id='p', split=SPLIT_HOLDOUT,
                             query_set='q', summary={'n': 1})


# --- non-inferiority -------------------------------------------------------

def test_non_inferiority_uses_the_lower_bound_not_the_point_estimate():
    cand = {'recall@50': 0.90, 'recall@50_ci': [0.86, 0.94], 'n': 200}
    inc = {'recall@50': 0.91, 'n': 200}
    r = non_inferior(cand, inc, 'recall@50', margin=0.03)
    assert r['candidate_ci_low'] == 0.86
    assert r['delta_lower_bound'] == -0.05
    assert r['pass'] is False, 'a point-estimate comparison would have passed'


def test_non_inferiority_passes_within_the_margin():
    cand = {'recall@50': 0.90, 'recall@50_ci': [0.89, 0.93], 'n': 900}
    inc = {'recall@50': 0.91, 'n': 900}
    assert non_inferior(cand, inc, 'recall@50', margin=0.03)['pass'] is True


def test_a_tiny_sample_cannot_pass_by_luck():
    """Wide intervals from small n must block, not flatter."""
    outs = [QueryOutcome(str(i), 0, 1, 0.0) for i in range(4)]
    cand = summarize(outs, k_values=(50,))
    inc = {'recall@50': 0.95, 'n': 900}
    assert non_inferior(cand, inc, 'recall@50')['pass'] is False
