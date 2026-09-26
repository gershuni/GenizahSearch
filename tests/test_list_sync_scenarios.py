# -*- coding: utf-8 -*-
"""The list sync's scenario gate: seeded random histories of two desktops and the website.

tests/list_sync_scenarios.py holds the fake Supabase, the actors, the operations
and the invariants; this file runs them. A failure prints the seed's shrunk op
list as a literal: paste it into REGRESSION_CASES below, with one line saying
what it shows, so it replays on every run.

Before a pull request that touches shared/lists_sync.py, also run the long
versions locally (see the harness's docstring):
    python tests/list_sync_scenarios.py --seeds 0-19999 --steps 60 --jobs 8
    python tests/list_sync_scenarios.py --seeds 0-1999 --steps 200 --jobs 8
"""
import os
import sys
import time

import pytest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import list_sync_scenarios as S  # noqa: E402

CI_SEEDS = range(0, 500)
CI_STEPS = 60

# (what it shows, seed, config, ops) -- each must replay without a violation.
REGRESSION_CASES = [
    ('an entry the desktop matched by sys_id alone had its row moved into another list (X2)',
     2, {'has_page': True, 'max_rows': None, 'p_inject': 0.0, 'past_end_raises': True, 'page_lag': False,
         'upgrade': 0},
     [('web', (9486, 54333, 50057, 35852, 34435)), ('sync', 'B', 'up', None),
      ('web', (49384, 56461, 63044, 48608, 26266))]),
    ('a second page of one manuscript took the first page\'s row, so two items held one cloud id (X3)',
     10, {'has_page': True, 'max_rows': 3, 'p_inject': 0.0, 'past_end_raises': True, 'page_lag': False,
          'upgrade': 0},
     [('desk', 'A', (22394, 53204, 16662, 1612, 40272)), ('sync', 'A', 'up', None),
      ('desk', 'A', (4658, 19529, 62103, 10191, 4605))]),
    ('a Download dropped a page from its list after the rows had been mixed up (X3 aftermath)',
     10, {'has_page': True, 'max_rows': 3, 'p_inject': 0.0, 'past_end_raises': True, 'page_lag': False,
          'upgrade': 0},
     [('desk', 'A', (22394, 53204, 16662, 1612, 40272)), ('sync', 'A', 'up', None),
      ('desk', 'A', (4658, 19529, 62103, 10191, 4605)), ('sync', 'A', 'up', None),
      ('sync', 'A', 'merge', None)]),
    ('an upload wrote the empty local note over a note written on the website (tracker line 144)',
     6, {'has_page': True, 'max_rows': None, 'p_inject': 0.1, 'past_end_raises': False, 'page_lag': False,
         'upgrade': 0},
     [('sync', 'B', 'up', None), ('web', (33561, 56577, 39119, 58486, 57659)),
      ('desk', 'B', (11389, 55333, 63701, 41847, 63164))]),
    ('an upload put an older note over the website\'s edit and a Download then replaced the newer local '
     'note with it, losing it everywhere (X4)',
     55, {'has_page': True, 'max_rows': 3, 'p_inject': 0.0, 'past_end_raises': True, 'page_lag': False,
          'upgrade': 11},
     [('desk', 'B', (30481, 4785, 25793, 25019, 34981)), ('sync', 'B', 'merge', None),
      ('web', (4034, 60271, 36893, 23592, 1753)), ('sync', 'A', 'merge', None)]),
    ('two folios of one manuscript on two computers overwrote each other\'s row and note',
     57, {'has_page': True, 'max_rows': 3, 'p_inject': 0.0, 'past_end_raises': False, 'page_lag': False,
          'upgrade': 0},
     [('desk', 'B', (44905, 19465, 34225, 53870, 53427)), ('desk', 'A', (31658, 9107, 55291, 22971, 52725))]),
    ('a My Library entry in a list was uploaded with its note and tags',
     116, {'has_page': True, 'max_rows': None, 'p_inject': 0.1, 'past_end_raises': False, 'page_lag': False,
           'upgrade': 0},
     [('desk', 'B', (59856, 49842, 25921, 7647, 62905))]),
    ('a My Library row already in the cloud was downloaded into a list',
     0, {'has_page': True, 'max_rows': None, 'p_inject': 0.1, 'past_end_raises': False, 'page_lag': True,
         'upgrade': 0},
     []),
]


def test_the_seed_block_holds_every_invariant():
    t0 = time.perf_counter()
    found = S.run_many(list(CI_SEEDS), CI_STEPS, 'current')
    took = time.perf_counter() - t0
    print(f'{len(CI_SEEDS)} seeds x {CI_STEPS} steps in {took:.1f}s')
    if found:
        seed = found[0][0]
        cfg = S.cfg_for(seed)
        ops = S.gen_ops(seed, CI_STEPS, cfg)
        small, v, w = S.shrink(seed, cfg, ops, 'current')
        pytest.fail(f'{len(found)} of {len(CI_SEEDS)} seeds break an invariant, first seeds '
                    f'{[f[0] for f in found[:10]]}\n{S.report(seed, cfg, small, v, w)}', pytrace=False)


@pytest.mark.parametrize('case', REGRESSION_CASES, ids=[c[0][:60] for c in REGRESSION_CASES])
def test_a_regression_case_replays_cleanly(case):
    _, seed, cfg, ops = case
    v, w = S.run_ops(seed, cfg, ops, 'current')
    assert v is None, S.report(seed, cfg, ops, v, w)


@pytest.mark.parametrize('inv', [1, 2, 4, 6])
def test_the_harness_catches_the_pre_2b_defects(inv):
    """The gate's own gate: on the engine before per-membership records, each invariant fires."""
    hits = [seed for seed in range(50)
            if (lambda v: v is not None and v.inv == inv)(S.run_seed(seed, 60, 'fixture',
                                                                     frozenset({inv, 'crash'}))[0])]
    assert hits, f'invariant {inv} never fired on the pre-2b engine in seeds 0-49'


# ---- the fake behaves like PostgREST where the engine depends on it

def _db(**kw):
    import random
    args = dict(has_page=True, max_rows=None, past_end_raises=False, page_lag=False)
    args.update(kw)
    db = S.FakeDB(random.Random(1), **args)
    web = S.FakeClient(db, 'web', 'u1')
    lst = web.table('user_lists').insert({'user_id': 'u1', 'name': 'L'}).execute().data[0]
    return db, web, lst


def test_the_fake_counts_before_the_range_and_caps_every_page():
    db, web, lst = _db(max_rows=2)
    web.table('list_items').insert([{'list_id': lst['id'], 'sys_id': str(n)} for n in range(5)]).execute()
    resp = (web.table('list_items').select('id', count='exact').eq('list_id', lst['id']).order('id')
            .range(0, 999).execute())
    assert len(resp.data) == 2 and resp.count == 5
    resp = web.table('list_items').select('id').in_('id', [r['id'] for r in db.tables['list_items']]).execute()
    assert len(resp.data) == 2 and resp.count is None


def test_the_fake_reads_tag_filters_as_jsonb():
    db, web, lst = _db()
    web.table('list_items').insert({'list_id': lst['id'], 'sys_id': '1', 'tags': ['a', 'ב"{,']}).execute()
    q = web.table('list_items').select('id')
    with pytest.raises(S.APIError) as e:
        q.contains('tags', ['a']).execute()
    assert e.value.code == '22P02'
    lit = '["ב\\"{,","a"]'
    assert len(web.table('list_items').select('id').contains('tags', lit).contained_by('tags', lit)
               .execute().data) == 1
    assert web.table('list_items').select('id').contained_by('tags', '["a"]').execute().data == []


def test_the_fake_hides_everything_without_a_session():
    db, web, lst = _db()
    web.table('list_items').insert({'list_id': lst['id'], 'sys_id': '1'}).execute()
    anon = S.FakeClient(db, 'A', None)
    resp = anon.table('list_items').select('id', count='exact').execute()
    assert resp.data == [] and resp.count == 0
    with pytest.raises(S.APIError) as e:
        anon.table('list_items').insert({'list_id': lst['id'], 'sys_id': '2'}).execute()
    assert e.value.code == '42501'
    assert anon.table('list_items').update({'note': 'x'}).eq('id', 101).execute().data == []


def test_the_fake_answers_a_range_past_the_end_either_way():
    for raises in (False, True):
        db, web, lst = _db(past_end_raises=raises)
        web.table('list_items').insert({'list_id': lst['id'], 'sys_id': '1'}).execute()
        q = web.table('list_items').select('id', count='exact').eq('list_id', lst['id']).order('id').range(5, 9)
        if raises:
            with pytest.raises(S.APIError) as e:
                q.execute()
            assert e.value.code == 'PGRST103'
        else:
            assert q.execute().data == []


def test_the_fake_knows_whether_the_page_column_exists():
    db, web, lst = _db(has_page=False)
    with pytest.raises(S.APIError) as e:
        web.table('list_items').select('id, page').execute()
    assert e.value.code == '42703'
    with pytest.raises(S.APIError) as e:
        web.table('list_items').insert({'list_id': lst['id'], 'sys_id': '1', 'page': '2'}).execute()
    assert e.value.code == 'PGRST204'
    db.migrate()
    web.table('list_items').insert({'list_id': lst['id'], 'sys_id': '1', 'page': '2'}).execute()


def test_the_fake_refuses_a_query_string_over_the_gateway_limit():
    db, web, lst = _db()
    row = web.table('list_items').insert({'list_id': lst['id'], 'sys_id': '1', 'note': 'x' * 9000}).execute().data[0]
    with pytest.raises(S.APIError) as e:
        web.table('list_items').update({'note': 'y'}).eq('id', row['id']).eq('note', 'x' * 9000).execute()
    assert e.value.code == 414
    assert db.tables['list_items'][0]['note'] == 'x' * 9000
