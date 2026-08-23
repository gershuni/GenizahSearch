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


def test_holdout_requires_reservation_then_is_write_once(tmp_path):
    path = str(tmp_path / 'ledger.jsonl')
    led = EvalLedger(path)
    # Recording without a reservation is itself a violation (Codex #1: the
    # runner must reserve BEFORE any query is issued).
    with pytest.raises(HoldoutReuse):
        led.record(method='passage', policy_id='p1', split=SPLIT_HOLDOUT,
                   query_set='qs.jsonl', summary={'n': 1}, strata={})
    led.reserve(method='passage', policy_id='p1', split=SPLIT_HOLDOUT,
                query_set='qs.jsonl')
    led.record(method='passage', policy_id='p1', split=SPLIT_HOLDOUT,
               query_set='qs.jsonl', summary={'n': 1}, strata={})
    # Persistence: a fresh ledger instance still refuses a second look --
    # both a second reservation and a second record.
    led2 = EvalLedger(path)
    with pytest.raises(HoldoutReuse):
        led2.reserve(method='passage', policy_id='p1', split=SPLIT_HOLDOUT,
                     query_set='qs.jsonl')
    with pytest.raises(HoldoutReuse):
        led2.record(method='passage', policy_id='p1', split=SPLIT_HOLDOUT,
                    query_set='qs.jsonl', summary={'n': 1}, strata={})
    # Tune stays freely re-recordable, no reservation needed.
    led2.record(method='passage', policy_id='p1', split=SPLIT_TUNE,
                query_set='qs.jsonl', summary={'n': 1}, strata={})


def test_holdout_key_includes_query_set(tmp_path):
    # Codex #2: after the FGP holdout, the SAME configs must still be able to
    # run the witness holdout -- one shared ledger, two instruments.
    path = str(tmp_path / 'ledger.jsonl')
    led = EvalLedger(path)
    led.reserve(method='passage', policy_id='p1', split=SPLIT_HOLDOUT,
                query_set='fgp.jsonl')
    led.record(method='passage', policy_id='p1', split=SPLIT_HOLDOUT,
               query_set='fgp.jsonl', summary={'n': 1}, strata={})
    # Same config, DIFFERENT query set: allowed.
    led.reserve(method='passage', policy_id='p1', split=SPLIT_HOLDOUT,
                query_set='witness.jsonl')
    led.record(method='passage', policy_id='p1', split=SPLIT_HOLDOUT,
               query_set='witness.jsonl', summary={'n': 1}, strata={})
    # Same config, SAME query set again: refused at reservation time.
    with pytest.raises(HoldoutReuse):
        led.reserve(method='passage', policy_id='p1', split=SPLIT_HOLDOUT,
                    query_set='fgp.jsonl')


# ---------------------------------------------------------------------------
# PR #324 review, P1: a partially-written holdout reservation batch.
# ---------------------------------------------------------------------------

def test_a_refused_batch_reserves_NOTHING(tmp_path):
    """The write-once holdout must not be spent by an error message.

    Looping `reserve()` over N configs wrote each as it went, so a duplicate
    at position N left 1..N-1 on disk as consumed -- before a single query
    ran. The operator's natural response (drop the duplicate, re-run) was
    then refused for the earlier configs too.
    """
    from shared.retrieval_eval import EvalLedger, HoldoutReuse, SPLIT_HOLDOUT

    path = str(tmp_path / 'ledger.jsonl')
    led = EvalLedger(path)
    led.reserve(method='passage', policy_id='passage-wide',
                split=SPLIT_HOLDOUT, query_set='qs.jsonl')

    fresh = EvalLedger(path)
    with pytest.raises(HoldoutReuse):
        fresh.reserve_all(
            configs=[('chunk', 'chunk-5'),            # new
                     ('passage', 'passage-wide')],    # already reserved
            split=SPLIT_HOLDOUT, query_set='qs.jsonl')

    after = EvalLedger(path)
    assert after.reserve(method='chunk', policy_id='chunk-5',
                         split=SPLIT_HOLDOUT, query_set='qs.jsonl'), (
        'chunk-5 was consumed by a batch that was refused -- the holdout is '
        'write-once, so a rejected batch must leave it untouched'
    )


def test_a_batch_that_repeats_a_key_within_itself_is_refused(tmp_path):
    """The per-call form could not see this at all: the first write landed
    and made the second look like a pre-existing reservation."""
    from shared.retrieval_eval import EvalLedger, HoldoutReuse, SPLIT_HOLDOUT

    path = str(tmp_path / 'dup.jsonl')
    led = EvalLedger(path)
    with pytest.raises(HoldoutReuse):
        led.reserve_all(configs=[('passage', 'p-1'), ('passage', 'p-1')],
                        split=SPLIT_HOLDOUT, query_set='qs.jsonl')

    assert EvalLedger(path).reserve(
        method='passage', policy_id='p-1', split=SPLIT_HOLDOUT,
        query_set='qs.jsonl'), 'the refused batch still wrote an entry'


def test_reserve_all_is_a_no_op_off_the_holdout_split(tmp_path):
    from shared.retrieval_eval import EvalLedger, SPLIT_TUNE

    path = str(tmp_path / 'tune.jsonl')
    led = EvalLedger(path)
    assert led.reserve_all(configs=[('passage', 'p-1'), ('passage', 'p-1')],
                           split=SPLIT_TUNE, query_set='qs.jsonl') == []
    assert not os.path.exists(path) or os.path.getsize(path) == 0
