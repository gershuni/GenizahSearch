# -*- coding: utf-8 -*-
"""The tune/holdout split's group contract, and the holdout's write-once lock.

Why these tests exist. An adversarial re-examination (2026-08-21) found that
hashing the bare query_id put sibling queries of one work -- identical positive
sets -- on both sides of the tune/holdout boundary for 510 of the witness
instrument's 573 works. The fix groups by the query_id prefix before '#'.
These tests pin the two properties that made the fix safe to apply after
results had already been recorded:

  1. siblings of one group NEVER straddle the boundary, and
  2. ids without '#' (the FGP set) get byte-identical assignments to the old
     per-id rule, so every FGP result recorded before the change stays valid.

Property 2 is asserted against a reimplementation of the OLD rule, not against
remembered outputs, so it holds for any id, not for a lucky sample.
"""
from __future__ import annotations

import hashlib
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from shared.retrieval_eval import (  # noqa: E402
    SPLIT_HOLDOUT, SPLIT_TUNE, EvalLedger, EvalQuery, HoldoutReuse,
    split_queries,
)


def q(qid: str) -> EvalQuery:
    return EvalQuery(query_id=qid, text='irrelevant', positives=frozenset())


def old_rule(qid: str, salt: str = 'v1') -> str:
    """The pre-fix assignment: hash of the bare query_id."""
    h = hashlib.sha256(f'{salt}|{qid}'.encode()).hexdigest()
    return SPLIT_TUNE if int(h[:8], 16) % 100 < 50 else SPLIT_HOLDOUT


def test_siblings_of_one_work_never_straddle_the_boundary():
    # 200 synthetic works x 4 slices, the witness instrument's shape.
    for w in range(200):
        splits = {q(f'wit:M:Ytext{w:06d}#{k}').split() for k in range(4)}
        assert len(splits) == 1, f'work {w} straddles: {splits}'


def test_both_splits_are_actually_populated_at_group_level():
    works = {q(f'wit:M:Ytext{w:06d}#0').split() for w in range(200)}
    assert works == {SPLIT_TUNE, SPLIT_HOLDOUT}


def test_hash_free_ids_keep_their_pre_fix_assignment():
    # The FGP set has no '#', so the group IS the id and nothing may move --
    # otherwise every FGP result recorded before the fix would be invalidated.
    for i in range(500):
        qid = f'fgp:{i}'
        assert q(qid).split() == old_rule(qid), qid


def test_split_queries_partitions_without_loss():
    queries = [q(f'wit:M:Ytext{w:06d}#{k}')
               for w in range(50) for k in range(4)]
    out = split_queries(queries)
    assert len(out[SPLIT_TUNE]) + len(out[SPLIT_HOLDOUT]) == len(queries)
    ids = {x.query_id for x in out[SPLIT_TUNE]} | \
          {x.query_id for x in out[SPLIT_HOLDOUT]}
    assert len(ids) == len(queries)


def test_holdout_is_write_once_per_config(tmp_path):
    path = str(tmp_path / 'ledger.jsonl')
    led = EvalLedger(path)
    led.record(method='passage', policy_id='p1', split=SPLIT_HOLDOUT,
               query_set='qs.jsonl', summary={'n': 1}, strata={})
    # Same (config, split, query set) again must raise, and force must not be
    # the default path.
    led2 = EvalLedger(path)
    with pytest.raises(HoldoutReuse):
        led2.record(method='passage', policy_id='p1', split=SPLIT_HOLDOUT,
                    query_set='qs.jsonl', summary={'n': 1}, strata={})
    # Tune stays freely re-recordable.
    led2.record(method='passage', policy_id='p1', split=SPLIT_TUNE,
                query_set='qs.jsonl', summary={'n': 1}, strata={})
