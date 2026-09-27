# -*- coding: utf-8 -*-
"""Directed pins of what a desktop removal, a move and an upload on a copy do in the cloud.

The scenario gate (tests/test_list_sync_scenarios.py) is the main check; these pin
the rulings and the cases it does not reach: explicit removals delete their row by
id and list at the next upload (and only rows this computer remembered), a Download
never brings a removed entry back, a moved row carries its destination, an edit made
while an upload runs counts as made after it, a close keeps what the upload reported,
and putting an entry back before its removal was sent keeps the website's row. They
drive a real ListsManager (lists.pkl under tmp_path) and a real ListsCloudSync against
the scenario gate's PostgREST stand-in, the way desktop/lists_sync_runner.py does
(begin_upload, sync_to_cloud(data=copy), finish_upload); nothing reaches Supabase.
"""
import ast
import copy
import os
import pathlib
import pickle
import random
import sys
import time
import types

import pytest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import list_sync_scenarios as S  # noqa: E402
from shared import lists_manager as lm  # noqa: E402
from shared import lists_sync  # noqa: E402

REPO = pathlib.Path(__file__).resolve().parents[1]
KEY = '990001::fl::FLa'


# --------------------------------------------------------------------------- the stand-ins

class Recorder:
    """Every request the clients make, and a hook that may act on one before it runs."""

    def __init__(self):
        self.reqs = []
        self.hook = None

    def before(self, client, req):
        self.reqs.append(types.SimpleNamespace(actor=client.actor, table=req.t, op=req.op, filters=list(req.filters),
                                               payload=copy.deepcopy(req.payload)))
        return self.hook(client, req) if self.hook else None

    def after(self, client, req):
        return None

    def mark(self):
        n = len(self.reqs)
        return lambda actor='A', table='list_items', op=None: [
            r for r in self.reqs[n:] if r.actor == actor and r.table == table and (op is None or r.op == op)]


class Cloud:
    def __init__(self, has_page=True, max_rows=None):
        self.db = S.FakeDB(random.Random(7), has_page=has_page, max_rows=max_rows, past_end_raises=False, page_lag=False)
        self.rec = Recorder()
        self.db.world = self.rec
        self.web = S.FakeClient(self.db, 'web', 'u1')

    def new_list(self, name, user='u1', deleted_at=None):
        client = self.web if user == 'u1' else S.FakeClient(self.db, 'web', user)
        return client.table('user_lists').insert({'user_id': user, 'name': name, 'name_en': name,
                                                  'deleted_at': deleted_at}).execute().data[0]['id']

    def add(self, list_id, sys_id, fl_id=None, note='', tags=None):
        return self.web.table('list_items').insert({'list_id': list_id, 'sys_id': sys_id,
                                                    'shelfmark': S.web_shelfmark(sys_id), 'title': S.web_title(sys_id),
                                                    'fl_id': fl_id, 'note': note,
                                                    'tags': tags or []}).execute().data[0]['id']

    def row(self, rid):
        return next((r for r in self.db.tables['list_items'] if r['id'] == rid), None)

    def rows(self, list_id=None, sys_id=None):
        return sorted((r for r in self.db.tables['list_items']
                       if (list_id is None or r['list_id'] == list_id) and (sys_id is None or r['sys_id'] == sys_id)),
                      key=lambda r: r['id'])

    def set(self, rid, **values):
        self.web.table('list_items').update(values).eq('id', rid).execute()

    def delete_row(self, rid):
        self.web.table('list_items').delete().eq('id', rid).execute()

    def cloud_list(self, cid):
        return self.db.list_by_id(cid)


def _manager_class(tmp_path, name):
    class Manager(lm.ListsManager):
        LISTS_FILE = str(tmp_path / f'{name}-lists.pkl')
    return Manager


def make_desk(tmp_path, cloud, name='A', user='u1'):
    mgr = _manager_class(tmp_path, name)(None)
    client = S.FakeClient(cloud.db, name, user)
    sync = lists_sync.ListsCloudSync(mgr)
    sync.set_client(client)
    sync.set_user(user)
    d = types.SimpleNamespace(mgr=mgr, sync=sync, client=client, name=name, reports=[], tmp=tmp_path, cloud=cloud)
    d.up = lambda **kw: upload(d, **kw)
    d.down = lambda: download(d)
    return d


def reopen(d):
    """The same computer after a restart: lists.pkl read again, a new sync object."""
    e = make_desk(d.tmp, d.cloud, d.name, d.sync._user_id)
    e.client.session_user = d.client.session_user
    return e


def sign_in(d, user):
    d.sync.set_user(user)
    d.client.session_user = user


def upload(d, **kw):
    """An upload as the desktop's runner makes one: on a copy, installed when it ends."""
    cp, base = d.mgr.begin_upload()
    d.reports = []
    d.copy, d.base = cp, base
    res = d.sync.sync_to_cloud(data=cp, withdrawn=d.mgr.withdrawn_now, on_recorded=d.reports.append, **kw)
    d.mgr.finish_upload(cp, base, res)
    return res


def download(d):
    """A download as the runner makes one: fetched, then applied."""
    state = d.sync.fetch_cloud_state(d.mgr.remembered_row_ids(d.sync._user_id))
    if not state.get('success'):
        return state
    return d.sync.apply_cloud_state(state)


def merge(d):
    down = d.down()
    return down, (d.up() if down.get('success') else None)


def item(d, key=KEY):
    return d.mgr.data['items'].get(key)


def rec(d, list_id, key=KEY):
    return ((item(d, key) or {}).get('cloud_rows') or {}).get(list_id)


def cloud_id(d, list_id):
    return d.mgr.data['lists'][list_id]['cloud_id']


def row_in(c, d, list_id, key=KEY):
    it = item(d, key)
    return next(r['id'] for r in c.rows(list_id=cloud_id(d, list_id), sys_id=it['sys_id'])
                if r.get('fl_id') == it.get('fl_id'))


def pending(d):
    return d.mgr.data.get('cloud_deletes') or {}


def deleted_ids(reqs):
    return sorted(next(v for k, col, v in r.filters if k == 'eq' and col == 'id') for r in reqs)


def explicit(reqs):
    """Every DELETE was filtered by one id and one list, nothing else."""
    return all(sorted((k, col) for k, col, _ in r.filters) == [('eq', 'id'), ('eq', 'list_id')] for r in reqs)


def lists_named(d):
    return {ld.get('name'): lid for lid, ld in d.mgr.data['lists'].items()}


@pytest.fixture(autouse=True)
def _configured(monkeypatch):
    monkeypatch.setattr(lists_sync, 'SUPABASE_AVAILABLE', True)
    monkeypatch.setattr(lists_sync, 'SUPABASE_ANON_KEY', 'test-key')
    import genizah_core
    monkeypatch.setattr(genizah_core, 'CURRENT_LANG', 'en')


@pytest.fixture
def cloud():
    return Cloud()


# --------------------------------------------------------------------------- R1-R6: explicit removals

R1_CELLS = ['remove-one-of-two', 'remove-last', 'delete-list-permanently', 'empty-trash', 'move-then-remove',
            'prompt-remove', 'prompt-remove-after-a-web-re-add']


@pytest.mark.parametrize('cell', R1_CELLS)
def test_a_desktop_removal_deletes_its_cloud_row(tmp_path, cloud, cell):
    a = make_desk(tmp_path, cloud)
    k, l_, m = a.mgr.create_list('K'), a.mgr.create_list('L'), a.mgr.create_list('M')
    a.mgr.add_item('990001', k, note='n', fl_id='FLa')
    if cell != 'remove-last' and cell not in ('delete-list-permanently', 'empty-trash'):
        a.mgr.add_item('990001', l_, fl_id='FLa')
    assert a.up()['success']
    rk, k_cloud = row_in(cloud, a, k), cloud_id(a, k)
    gone = {rk: k_cloud}
    if cell in ('remove-one-of-two', 'remove-last'):
        a.mgr.remove_item_from_list(KEY, k)
    elif cell == 'delete-list-permanently':
        a.mgr.delete_list(k, permanent=True)
    elif cell == 'empty-trash':
        a.mgr.delete_list(k)
        assert a.mgr.empty_trash() == 1
    elif cell == 'move-then-remove':
        a.mgr.move_items_to_list([KEY], k, m)
        a.mgr.remove_item_from_list(KEY, m)          # the entry stays in L
    else:
        cloud.delete_row(rk)                          # removed on the website
        assert a.up()['web_removed'] == [(KEY, k)]
        assert a.mgr.pending_web_removals() == [(KEY, k)]
        gone = {}
        if cell == 'prompt-remove-after-a-web-re-add':
            rn = cloud.add(k_cloud, '990001', fl_id='FLa', note='web re-add')
            assert a.down()['success'] and rec(a, k)['id'] == rn and not rec(a, k).get('gone')
            gone = {rn: k_cloud}
        # the prompt was shown while the removal was pending; its answer now
        assert a.mgr.resolve_web_removals({(KEY, k): 'remove'}) == (1, 0)
    mark = cloud.rec.mark()
    result = a.up()
    assert result['success'] and result['removals_failed'] == 0 and result['rows_deleted'] == len(gone)
    deletes = mark(op='delete')
    assert deleted_ids(deletes) == sorted(gone) and explicit(deletes)
    assert all(('eq', 'list_id', gone[rid]) in r.filters for rid, r in zip(deleted_ids(deletes), deletes))
    assert pending(a) == {}
    assert all(cloud.row(rid) is None for rid in gone)
    assert a.down()['success']
    it = item(a)
    assert it is None or k not in it['lists']        # not re-added where it was removed
    if cell != 'remove-last' and cell not in ('delete-list-permanently', 'empty-trash'):
        assert it is not None and l_ in it['lists'] and rec(a, l_) is not None


@pytest.mark.parametrize('how', ['download', 'merge'])
def test_a_download_never_re_adds_an_entry_pending_deletion(tmp_path, cloud, how):
    a = make_desk(tmp_path, cloud)
    k = a.mgr.create_list('K')
    a.mgr.add_item('990001', k, note='n', fl_id='FLa')
    a.mgr.add_item('990002', k, note='other', fl_id='FLb')
    assert a.up()['success']
    rk = row_in(cloud, a, k)
    other_note = item(a, '990002::fl::FLb')['note']
    a.mgr.remove_item_from_list(KEY, k)               # its only list: the entry is gone
    down = a.down()
    assert down['success'] and down['items_added'] == 0
    assert item(a) is None and not [iid for iid, it in a.mgr.data['items'].items() if it['sys_id'] == '990001']
    assert item(a, '990002::fl::FLb')['note'] == other_note
    assert cloud.row(rk) is not None and str(rk) in pending(a)
    if how == 'merge':
        mark = cloud.rec.mark()
        assert a.up()['success']
        assert deleted_ids(mark(op='delete')) == [rk] and cloud.row(rk) is None and pending(a) == {}


def test_a_failed_or_offline_delete_is_retried_and_survives_a_restart(tmp_path, cloud):
    a = make_desk(tmp_path, cloud)
    k = a.mgr.create_list('K')
    a.mgr.add_item('990001', k, note='n', fl_id='FLa')
    a.mgr.add_item('990002', k, note='m', fl_id='FLb')
    assert a.up()['success']
    rk, r2 = row_in(cloud, a, k), row_in(cloud, a, k, '990002::fl::FLb')
    a.mgr.remove_item_from_list(KEY, k)
    cloud.rec.hook = lambda client, req: 'raise_before' if client.actor == 'A' and req.op == 'delete' else None
    result = a.up()
    cloud.rec.hook = None
    assert result['success'] is False and result['complete'] is False
    assert result['items_failed'] == 1 and result['removals_failed'] == 1
    assert str(rk) in pending(a) and cloud.row(rk) is not None
    b = reopen(a)                                      # a restart: the removal was saved with lists.pkl
    assert str(rk) in pending(b)
    assert b.up()['success'] and cloud.row(rk) is None and pending(b) == {}
    # A DELETE that matches nothing while the session is gone (auth not proven) settles nothing.
    b.mgr.remove_item_from_list('990002::fl::FLb', k)

    def lose_the_session(client, req):
        if client.actor == 'A' and req.op == 'delete':
            client.session_user = None
        return None
    cloud.rec.hook = lose_the_session
    result = b.up()
    cloud.rec.hook = None
    b.client.session_user = 'u1'
    assert result['success'] is False and result['removals_failed'] == 1
    assert str(r2) in pending(b) and cloud.row(r2) is not None
    assert b.up()['success'] and cloud.row(r2) is None


def test_pending_deletions_wait_for_their_account(tmp_path, cloud):
    a = make_desk(tmp_path, cloud)
    k = a.mgr.create_list('K')
    a.mgr.add_item('990001', k, note='n', fl_id='FLa')
    assert a.up()['success']
    rk = row_in(cloud, a, k)
    a.mgr.remove_item_from_list(KEY, k)
    assert pending(a)[str(rk)]['account'] == 'u1'
    sign_in(a, 'u2')
    mark = cloud.rec.mark()
    assert a.up()['success']
    assert mark(op='delete') == [] and cloud.row(rk) is not None
    assert pending(a)[str(rk)]['account'] == 'u1' and a.mgr.data['cloud_account'] == 'u2'
    sign_in(a, 'u1')
    mark = cloud.rec.mark()
    assert a.up()['success']
    assert deleted_ids(mark(op='delete')) == [rk] and cloud.row(rk) is None and pending(a) == {}
    # an entry known only by a legacy scalar id queues nothing
    a.mgr.data['items']['990009'] = {'sys_id': '990009', 'lists': [k], 'note': '', 'tags': [], 'cloud_id': 424242}
    a.mgr.remove_item_from_list('990009', k)
    assert pending(a) == {}


def test_a_delete_never_follows_a_row_another_computer_moved(tmp_path, cloud):
    a, b = make_desk(tmp_path, cloud, 'A'), make_desk(tmp_path, cloud, 'B')
    k, x = a.mgr.create_list('K'), a.mgr.create_list('X')
    a.mgr.add_item('990001', k, note='n', fl_id='FLa')
    assert a.up()['success']
    rk = row_in(cloud, a, k)
    assert b.down()['success']
    bk, bx = lists_named(b)['K'], lists_named(b)['X']
    a.mgr.remove_item_from_list(KEY, k)               # removed here ...
    b.mgr.move_items_to_list([KEY], bk, bx)           # ... and moved on the other computer, first
    assert b.up()['success'] and cloud.row(rk)['list_id'] == cloud_id(a, x)
    mark = cloud.rec.mark()
    assert a.up()['success']
    assert mark(op='delete') == []                    # its move wins
    assert cloud.row(rk)['list_id'] == cloud_id(a, x) and pending(a) == {}
    assert a.down()['success'] and x in item(a)['lists'] and rec(a, x)['id'] == rk


def test_moving_a_list_to_the_trash_deletes_no_rows_and_restore_finds_them(tmp_path, cloud):
    a = make_desk(tmp_path, cloud)
    k = a.mgr.create_list('K')
    a.mgr.add_item('990001', k, note='n', fl_id='FLa')
    assert a.up()['success']
    rk = row_in(cloud, a, k)
    a.mgr.delete_list(k)                              # to the Trash
    mark = cloud.rec.mark()
    assert a.up()['success']
    assert mark(op='delete') == [] and cloud.row(rk) is not None and pending(a) == {}
    assert cloud.cloud_list(cloud_id(a, k))['deleted_at']
    a.mgr.restore_list(k)
    mark = cloud.rec.mark()
    assert a.up()['success']
    assert mark(op='insert') == [] and mark(op='delete') == [] and rec(a, k)['id'] == rk


# --------------------------------------------------------------------------- R7-R8: moves keep their destination

R7_DEST = ['active-without-row', 'active-with-row', 'trashed', 'no-cloud-id']
R7_FOLLOW = ['nothing', 'remove-destination-another-survives', 'remove-last', 'restore-destination',
             'delete-destination-permanently']
R7_CELLS = [f'{d}-{f}' for d in R7_DEST for f in R7_FOLLOW] + [
    'chained', 'moved-back', 'duplicate-merge', 'auto-duplicate-merge', 'trashed-another-membership-without-row',
    'chained-remove-last', 'chained-remove-destination-another-survives']


def _no_rows_for_list_insert(name):
    """The insert of that cloud list is written, but its answer shows no row (the list gets no cloud id)."""
    def hook(client, req):
        if client.actor == 'A' and req.t == 'user_lists' and req.op == 'insert' and \
                (req.payload or {}).get('name') == name:
            return 'no_rows'
        return None
    return hook


def _moves_of(reqs, rid):
    return [r for r in reqs if ('eq', 'id', rid) in r.filters and 'list_id' in (r.payload or {})]


@pytest.mark.parametrize('cell', R7_CELLS)
def test_moves_keep_their_destination_with_the_row(tmp_path, cloud, cell):
    a = make_desk(tmp_path, cloud)
    if cell.startswith('chained-remove-'):
        _chained_moves_then_removed(a, cloud, cell)
        return
    if cell in ('duplicate-merge', 'auto-duplicate-merge'):
        keep = a.mgr.create_list('Dup')                # the older one: the automatic merge keeps it
        dup = a.mgr.create_list('Dup')
        a.mgr.add_item('990001', dup, note='n', fl_id='FLa')
        assert a.up()['success']
        rd, keep_cloud, dup_cloud = row_in(cloud, a, dup), cloud_id(a, keep), cloud_id(a, dup)
        if cell == 'duplicate-merge':
            a.mgr.merge_duplicate_group(keep, [dup])
        else:
            (group,) = [g for g in a.mgr.find_duplicate_lists() if g['name'] == 'Dup']
            assert a.mgr.auto_merge_duplicate_group(group)['keep_id'] == keep
        mark = cloud.rec.mark()
        assert a.up()['success']
        (move,) = mark(op='update')
        assert move.payload == {'list_id': keep_cloud} and ('eq', 'list_id', dup_cloud) in move.filters
        assert mark(op='insert') == [] and mark(op='delete') == [] and cloud.row(rd)['list_id'] == keep_cloud
        return
    k, m = a.mgr.create_list('K'), a.mgr.create_list('M')
    dest, follow = next(((d, cell[len(d) + 1:]) for d in R7_DEST if cell.startswith(d + '-')), (None, None))
    l_ = None if dest == 'no-cloud-id' else a.mgr.create_list('L')
    n = a.mgr.create_list('N') if cell == 'chained' else None
    a.mgr.add_item('990001', k, note='n', fl_id='FLa')
    if dest == 'active-with-row':
        a.mgr.add_item('990001', l_, fl_id='FLa')
    if follow == 'remove-destination-another-survives':
        a.mgr.add_item('990001', m, fl_id='FLa')
    assert a.up()['success']
    rk, k_cloud = row_in(cloud, a, k), cloud_id(a, k)
    rl = row_in(cloud, a, l_) if dest == 'active-with-row' else None
    rm = row_in(cloud, a, m) if follow == 'remove-destination-another-survives' else None
    if dest == 'trashed' or cell == 'trashed-another-membership-without-row':
        a.mgr.delete_list(l_)
        assert a.up()['success']
    if cell == 'trashed-another-membership-without-row':
        a.mgr.add_item('990001', m, fl_id='FLa')       # M has no row yet
    if dest == 'no-cloud-id':
        l_ = a.mgr.create_list('L')
    a.mgr.move_items_to_list([KEY], k, l_)
    if cell == 'chained':
        a.mgr.move_items_to_list([KEY], l_, n)
    elif cell == 'moved-back':
        a.mgr.move_items_to_list([KEY], l_, k)
    if dest == 'no-cloud-id':
        cloud.rec.hook = _no_rows_for_list_insert('L')
        mark = cloud.rec.mark()
        result = a.up()
        cloud.rec.hook = None
        assert a.mgr.data['lists'][l_].get('cloud_id') is None
        assert result['deferred'] == 1 and mark(op='delete') == [] and not _moves_of(mark(op='update'), rk)
    if follow in ('remove-destination-another-survives', 'remove-last'):
        a.mgr.remove_item_from_list(KEY, l_)
    elif follow == 'restore-destination':
        a.mgr.restore_list(l_)
    elif follow == 'delete-destination-permanently':
        a.mgr.delete_list(l_, permanent=True)
    mark = cloud.rec.mark()
    result = a.up()
    assert result['success']
    deletes, moves = mark(op='delete'), _moves_of(mark(op='update'), rk)
    removed = follow in ('remove-destination-another-survives', 'remove-last', 'delete-destination-permanently')
    if cell == 'chained':
        (move,) = moves
        assert move.payload == {'list_id': cloud_id(a, n)} and ('eq', 'list_id', k_cloud) in move.filters
        expect_rows = {rk: cloud_id(a, n)}
    elif cell == 'moved-back':
        assert moves == [] and deletes == [] and mark(op='insert') == [] and rec(a, k)['id'] == rk
        expect_rows = {rk: k_cloud}
    elif cell == 'trashed-another-membership-without-row':
        assert moves == [] and deletes == [] and result['deferred'] == 1
        (ins,) = mark(op='insert')
        assert ins.payload['list_id'] == cloud_id(a, m) and cloud.row(rk)['list_id'] == k_cloud
        a.mgr.restore_list(l_)
        mark = cloud.rec.mark()
        assert a.up()['success']
        (move,) = _moves_of(mark(op='update'), rk)
        assert move.payload == {'list_id': cloud_id(a, l_)}
        expect_rows = {rk: cloud_id(a, l_), row_in(cloud, a, m): cloud_id(a, m)}
    elif removed:
        expect = sorted([rk] + ([rl] if rl is not None else []))
        assert deleted_ids(deletes) == expect and explicit(deletes) and moves == []
        assert all(('eq', 'list_id', k_cloud) in r.filters for r in deletes if ('eq', 'id', rk) in r.filters)
        expect_rows = {rm: cloud_id(a, m)} if rm is not None else {}
    elif dest == 'active-with-row':
        (conditional,) = deletes
        assert ('eq', 'id', rk) in conditional.filters and not explicit(deletes) and moves == []
        assert {col for _, col, _ in conditional.filters} >= {'note', 'tags'}
        expect_rows = {rl: cloud_id(a, l_)}
    elif dest == 'trashed' and follow != 'restore-destination':
        assert deletes == [] and moves == [] and result['deferred'] == 1
        expect_rows = {rk: k_cloud}
    else:
        (move,) = moves
        assert move.payload == {'list_id': cloud_id(a, l_)} and ('eq', 'list_id', k_cloud) in move.filters
        assert deletes == []
        expect_rows = {rk: cloud_id(a, l_)}
    have = {r['id']: r['list_id'] for r in cloud.rows(sys_id='990001')}
    assert have == expect_rows
    down, up = merge(a)                                # and nothing comes back, nothing moves again
    assert down['success'] and up['success']
    assert {r['id']: r['list_id'] for r in cloud.rows(sys_id='990001')} == expect_rows
    if removed:
        it = item(a)
        assert it is None or (l_ not in it['lists'] and k not in it['lists'])


def _chained_moves_then_removed(a, cloud, cell):
    """Moved K -> L -> N, then removed from N: the row, still in K's cloud list, is deleted there.

    In '...-another-survives' the entry is also in M, which has no row of its own yet: the
    moved row is not M's to take (it was bound for N), so M gets an insert.
    """
    k, l_, m, n = (a.mgr.create_list(x) for x in ('K', 'L', 'M', 'N'))
    a.mgr.add_item('990001', k, note='n', fl_id='FLa')
    assert a.up()['success']
    rk, k_cloud = row_in(cloud, a, k), cloud_id(a, k)
    survives = cell == 'chained-remove-destination-another-survives'
    if survives:
        a.mgr.add_item('990001', m, fl_id='FLa')
    a.mgr.move_items_to_list([KEY], k, l_)
    a.mgr.move_items_to_list([KEY], l_, n)
    a.mgr.remove_item_from_list(KEY, n)
    assert pending(a)[str(rk)]['list'] == k_cloud
    mark = cloud.rec.mark()
    assert a.up()['success']
    deletes = mark(op='delete')
    assert deleted_ids(deletes) == [rk] and explicit(deletes) and ('eq', 'list_id', k_cloud) in deletes[0].filters
    assert _moves_of(mark(op='update'), rk) == [] and pending(a) == {}
    if survives:
        (ins,) = mark(op='insert')
        assert ins.payload['list_id'] == cloud_id(a, m)
        expect_rows = {row_in(cloud, a, m): cloud_id(a, m)}
    else:
        assert mark(op='insert') == []
        expect_rows = {}
    assert {r['id']: r['list_id'] for r in cloud.rows(sys_id='990001')} == expect_rows
    assert a.down()['success']
    assert (item(a) or {}).get('lists', []) == ([m] if survives else [])


def test_a_moved_row_the_website_deleted_is_forgotten(tmp_path, cloud):
    """A row waiting for its list (in the Trash) and then deleted on the website: nothing is left
    waiting for it, and the list gets a row of its own once it is back."""
    a = make_desk(tmp_path, cloud)
    k, l_ = a.mgr.create_list('K'), a.mgr.create_list('L')
    a.mgr.add_item('990001', k, note='n', fl_id='FLa')
    assert a.up()['success']
    rk = row_in(cloud, a, k)
    a.mgr.delete_list(l_)
    assert a.up()['success']
    a.mgr.move_items_to_list([KEY], k, l_)
    assert a.up()['deferred'] == 1
    cloud.delete_row(rk)
    mark = cloud.rec.mark()
    result = a.up()
    assert result['success'] and result['deferred'] == 0 and rec(a, '~%s' % rk) is None
    assert mark(op='delete') == [] and mark(op='update') == [] and a.mgr.pending_web_removals() == []
    a.mgr.restore_list(l_)
    mark = cloud.rec.mark()
    assert a.up()['success']
    (ins,) = mark(op='insert')
    assert ins.payload['list_id'] == cloud_id(a, l_) and rec(a, l_)['id'] != rk


def test_an_entry_put_back_in_the_list_it_left_keeps_its_row_there(tmp_path, cloud):
    a = make_desk(tmp_path, cloud)
    k, l_ = a.mgr.create_list('K'), a.mgr.create_list('L')
    a.mgr.add_item('990001', k, note='n', fl_id='FLa')
    assert a.up()['success']
    rk = row_in(cloud, a, k)
    a.mgr.move_items_to_list([KEY], k, l_)
    a.mgr.add_item('990001', k, fl_id='FLa')          # and added to K again: now in both
    assert rec(a, k)['id'] == rk and rec(a, '~%s' % rk) is None
    mark = cloud.rec.mark()
    assert a.up()['success']
    assert _moves_of(mark(op='update'), rk) == [] and mark(op='delete') == []
    (ins,) = mark(op='insert')
    assert ins.payload['list_id'] == cloud_id(a, l_) and cloud.row(rk)['list_id'] == cloud_id(a, k)


def test_a_website_row_of_a_moved_entry_in_the_list_it_left_does_not_bring_it_back(tmp_path, cloud):
    a = make_desk(tmp_path, cloud)
    k, l_ = a.mgr.create_list('K'), a.mgr.create_list('L')
    a.mgr.add_item('990001', k, note='n', fl_id='FLa')
    assert a.up()['success']
    a.mgr.move_items_to_list([KEY], k, l_)
    cloud.add(cloud_id(a, k), '990001', fl_id='FLa', note='another row of it')
    assert a.down()['success']
    assert item(a)['lists'] == [l_]                   # the moved entry stays where it was moved
    (other,) = [iid for iid, it in a.mgr.data['items'].items() if iid != KEY and it['sys_id'] == '990001']
    assert a.mgr.data['items'][other]['lists'] == [k]  # that row is an entry of its own


def test_removing_a_destination_that_already_held_the_entry_deletes_the_moved_row(tmp_path, cloud):
    a = make_desk(tmp_path, cloud)
    k, l_, m = a.mgr.create_list('K'), a.mgr.create_list('L'), a.mgr.create_list('M')
    for lid in (k, l_, m):
        a.mgr.add_item('990001', lid, note='n', fl_id='FLa')
    assert a.up()['success']
    rk, rl, rm = row_in(cloud, a, k), row_in(cloud, a, l_), row_in(cloud, a, m)
    k_cloud, l_cloud = cloud_id(a, k), cloud_id(a, l_)
    cloud.set(rk, note='n\nedited on the website')
    a.mgr.move_items_to_list([KEY], k, l_)
    a.mgr.remove_item_from_list(KEY, l_)              # M survives
    mark = cloud.rec.mark()
    assert a.up()['success']
    deletes = mark(op='delete')
    assert deleted_ids(deletes) == sorted([rk, rl]) and explicit(deletes)
    by_id = {next(v for _, c, v in r.filters if c == 'id'): r for r in deletes}
    assert ('eq', 'list_id', k_cloud) in by_id[rk].filters and ('eq', 'list_id', l_cloud) in by_id[rl].filters
    assert [r['id'] for r in cloud.rows(sys_id='990001')] == [rm]
    assert a.down()['success'] and item(a)['lists'] == [m]


# --------------------------------------------------------------------------- R9: an edit made during an upload

def edit_during(cloud, fn, after=None, nth=1):
    """Run fn (a user edit on the live store) once while A's upload runs on its copy.

    after=None: before A's first request. after='insert' / 'update': before the request that
    follows A's nth request of that kind (so its answer was already given to the engine).
    """
    seen = [0]

    def hook(client, req):
        if client.actor != 'A':
            return None
        if after is None or seen[0] >= nth:
            cloud.rec.hook = None
            fn()
            return None
        if req.t == 'list_items' and req.op == after and (after != 'update' or 'list_id' in (req.payload or {})):
            seen[0] += 1
        return None
    cloud.rec.hook = hook


R9_CELLS = ['keep-then-move', 'move-during-move', 'remove-during-insert', 'delete-item-during-insert',
            'remove-and-re-add', 'account-switch-remove', 'prompt-remove-during-upload', 'tombstone-then-remove',
            'row-reassigned-to-another-membership', 'stale-repair-then-remove', 'remove-during-move']


@pytest.mark.parametrize('cell', R9_CELLS)
def test_edits_made_during_an_upload_count_as_made_after_it(tmp_path, cloud, cell):
    a = make_desk(tmp_path, cloud)
    if cell == 'stale-repair-then-remove':
        _stale_repair_then_remove(a, cloud)
        return
    k, l_, m, n = (a.mgr.create_list(x) for x in ('K', 'L', 'M', 'N'))
    z = a.mgr.create_list('Z')                         # written last: a request after the others' writes
    if cell == 'keep-then-move':
        a.mgr.add_item('990001', l_, note='n', fl_id='FLa')
        assert a.up()['success']
        cloud.delete_row(row_in(cloud, a, l_))
        assert a.up()['web_removed'] == [(KEY, l_)]
        r201 = cloud.add(cloud_id(a, l_), '990001', fl_id='FLa', note='n\nweb re-add')
        edit_during(cloud, lambda: (a.mgr.resolve_web_removals({(KEY, l_): 'keep'}),
                                    a.mgr.move_items_to_list([KEY], l_, n)))
        assert a.up()['success']
        assert rec(a, '~%s' % r201) == dict(rec(a, '~%s' % r201), id=r201, to=n) and pending(a) == {}
        mark = cloud.rec.mark()
        assert a.up()['success']
        (move,) = _moves_of(mark(op='update'), r201)
        assert move.payload.get('list_id') == cloud_id(a, n) and mark(op='delete') == []
        assert cloud.row(r201)['note'] == 'n\nweb re-add'
        return
    if cell == 'row-reassigned-to-another-membership':
        b = make_desk(tmp_path, cloud, 'B')
        a.mgr.add_item('990001', m, note='n', fl_id='FLa')
        assert a.up()['success']
        r102 = row_in(cloud, a, m)
        assert b.down()['success']
        b.mgr.move_items_to_list([KEY], lists_named(b)['M'], lists_named(b)['L'])
        assert b.up()['success'] and cloud.row(r102)['list_id'] == cloud_id(a, l_)
        a.mgr.add_item('990001', l_, fl_id='FLa')      # E in L too, with no row of its own
        edit_during(cloud, lambda: a.mgr.remove_item_from_list(KEY, m))
        mark = cloud.rec.mark()
        assert a.up()['success']
        assert rec(a, l_)['id'] == r102 and str(r102) not in pending(a)
        assert mark(op='delete') == [] and mark(op='insert') == []
        mark = cloud.rec.mark()
        assert a.up()['success'] and mark(op='delete') == [] and cloud.row(r102)['list_id'] == cloud_id(a, l_)
        return
    if cell == 'prompt-remove-during-upload':
        a.mgr.add_item('990001', l_, note='n', fl_id='FLa')
        assert a.up()['success']
        cloud.delete_row(row_in(cloud, a, l_))
        assert a.up()['web_removed'] == [(KEY, l_)]
        edit_during(cloud, lambda: a.mgr.resolve_web_removals({(KEY, l_): 'remove'}))
        assert a.up()['success']
        assert item(a) is None and pending(a) == {}
        mark = cloud.rec.mark()
        assert a.up()['success'] and mark(op='delete') == [] and mark(op='insert') == []
        return
    if cell == 'account-switch-remove':
        a.mgr.add_item('990001', l_, note='n', fl_id='FLa')
        a.mgr.add_item('990001', m, fl_id='FLa')
        assert a.up()['success']
        r101, r102 = row_in(cloud, a, l_), row_in(cloud, a, m)
        sign_in(a, 'u2')                                # another account's first upload over this store
        edit_during(cloud, lambda: a.mgr.remove_item_from_list(KEY, l_), after='insert')
        assert a.up()['success']
        cd = pending(a)
        r201 = next(int(rid) for rid, e in cd.items() if e['account'] == 'u2')
        assert cd[str(r101)]['account'] == 'u1' and set(cd) == {str(r101), str(r201)}
        r202 = rec(a, m)['id']
        assert r202 not in (r101, r102) and cloud.row(r202) is not None
        mark = cloud.rec.mark()
        assert a.up()['success']
        assert deleted_ids(mark(op='delete')) == [r201] and cloud.row(r101) is not None
        sign_in(a, 'u1')
        mark = cloud.rec.mark()
        assert a.up()['success']
        assert deleted_ids(mark(op='delete')) == [r101] and cloud.row(r202) is not None and pending(a) == {}
        return
    if cell == 'tombstone-then-remove':
        a.mgr.add_item('990001', l_, note='n', fl_id='FLa')
        a.mgr.add_item('990001', m, fl_id='FLa')
        assert a.up()['success']
        r101 = row_in(cloud, a, l_)
        cloud.delete_row(r101)                          # the upload will find it gone
        edit_during(cloud, lambda: a.mgr.remove_item_from_list(KEY, l_))
        result = a.up()
        assert result['success'] and result['web_removed'] == [(KEY, l_)]
        assert set(pending(a)) == {str(r101)}           # the one difference from a serial order ...
        mark = cloud.rec.mark()
        assert a.up()['success']
        assert mark(op='delete') == [] and pending(a) == {}   # ... settled with no request
        return
    if cell in ('move-during-move', 'remove-during-move'):
        a.mgr.add_item('990001', k, note='n', fl_id='FLa')
        a.mgr.add_item('990001', m, fl_id='FLa')
        assert a.up()['success']
        r101, r103 = row_in(cloud, a, k), row_in(cloud, a, m)
        a.mgr.move_items_to_list([KEY], k, l_)
        a.mgr.add_item('990005', z, fl_id='FLz')        # an insert after L's write
        if cell == 'move-during-move':
            edit_during(cloud, lambda: a.mgr.move_items_to_list([KEY], l_, n))
            assert a.up()['success']
            assert cloud.row(r101)['list_id'] == cloud_id(a, l_)          # the upload moved it into L
            moved = rec(a, '~%s' % r101)
            assert moved['to'] == n and moved['list'] == cloud_id(a, l_)
            a.mgr.remove_item_from_list(KEY, n)                           # M survives
            mark = cloud.rec.mark()
            assert a.up()['success']
            deletes = mark(op='delete')
            assert deleted_ids(deletes) == [r101] and explicit(deletes)
            assert ('eq', 'list_id', cloud_id(a, l_)) in deletes[0].filters and cloud.row(r103) is not None
        else:
            edit_during(cloud, lambda: a.mgr.remove_item_from_list(KEY, l_), after='update')
            assert a.up()['success']
            assert pending(a)[str(r101)]['list'] == cloud_id(a, l_)
            mark = cloud.rec.mark()
            assert a.up()['success']
            assert deleted_ids(mark(op='delete')) == [r101] and cloud.row(r101) is None
            assert a.down()['success'] and item(a)['lists'] == [m]
        return
    # the first insert of an entry's row, and the entry removed (or deleted, or put back) meanwhile
    a.mgr.add_item('990001', m, note='n', fl_id='FLa')
    assert a.up()['success']
    if cell == 'delete-item-during-insert':
        a.mgr.remove_item_from_list(KEY, m)
        assert a.up()['success']
        a.mgr.add_item('990001', l_, note='n', fl_id='FLa')
    else:
        a.mgr.add_item('990001', l_, fl_id='FLa')       # E in L too, with no row yet
    a.mgr.add_item('990005', z, fl_id='FLz')            # an insert after L's
    if cell == 'remove-and-re-add':
        edit = (lambda: (a.mgr.remove_item_from_list(KEY, l_), a.mgr.add_item('990001', l_, fl_id='FLa')))
    else:
        edit = (lambda: a.mgr.remove_item_from_list(KEY, l_))
    edit_during(cloud, edit, after='insert')
    assert a.up()['success']
    r201 = next(r['id'] for r in cloud.rows(list_id=cloud_id(a, l_), sys_id='990001'))
    mark = cloud.rec.mark()
    if cell == 'remove-and-re-add':                    # back before its removal went: it keeps that row
        assert pending(a) == {} and rec(a, l_)['id'] == r201
        assert a.up()['success'] and mark(op='delete') == [] and mark(op='insert') == []
        return
    assert set(pending(a)) == {str(r201)} and pending(a)[str(r201)]['list'] == cloud_id(a, l_)
    assert a.up()['success']
    assert deleted_ids(mark(op='delete')) == [r201] and cloud.row(r201) is None
    assert a.down()['success']
    assert item(a) is None if cell == 'delete-item-during-insert' else item(a)['lists'] == [m]


@pytest.mark.parametrize('cell', ['renamed-before-and-during', 'renamed-only-during'])
def test_a_list_renamed_while_an_upload_runs_keeps_its_new_name(tmp_path, cloud, cell):
    """The upload sends the name its copy had; the name given meanwhile is still to be sent, and a
    Download before that keeps it rather than taking the website's back."""
    a = make_desk(tmp_path, cloud)
    k = a.mgr.create_list('K')
    a.mgr.add_item('990001', k, note='n', fl_id='FLa')
    assert a.up()['success']
    c_k = cloud_id(a, k)
    sent = 'K'
    if cell == 'renamed-before-and-during':
        a.mgr.update_list(k, name='K first')
        sent = 'K first'
    edit_during(cloud, lambda: a.mgr.update_list(k, name='K second'))
    assert a.up()['success']
    lst = a.mgr.data['lists'][k]
    assert cloud.cloud_list(c_k)['name'] == sent
    assert lst['name'] == 'K second' and lst.get(lists_sync.LIST_NAME_UNSENT)
    assert saved(a)['lists'][k].get(lists_sync.LIST_NAME_UNSENT)
    assert a.down()['success'] and a.mgr.data['lists'][k]['name'] == 'K second'
    assert a.up()['success'] and cloud.cloud_list(c_k)['name'] == 'K second'
    assert not a.mgr.data['lists'][k].get(lists_sync.LIST_NAME_UNSENT)
    assert a.down()['success'] and a.mgr.data['lists'][k]['name'] == 'K second'


def _stale_repair_then_remove(a, cloud):
    """Two local lists held one cloud list: the upload gives L its own, and E is removed from L meanwhile."""
    c101 = cloud.new_list('K')
    r102 = cloud.add(c101, '990001', fl_id='FLa', note='n')
    r105 = cloud.add(c101, '990002', fl_id='FLb', note='f')
    k, l_, z = a.mgr.create_list('K'), a.mgr.create_list('L'), a.mgr.create_list('Z')
    for lid in (k, l_):
        a.mgr.data['lists'][lid]['cloud_id'] = c101
    a.mgr.data['cloud_account'] = 'u1'
    a.mgr.data['items'][KEY] = {'sys_id': '990001', 'fl_id': 'FLa', 'lists': [l_], 'note': 'n', 'tags': [],
                                'cloud_rows': {l_: {'id': r102, 'list': c101, 'note': 'n', 'tags': []}}}
    a.mgr.data['items']['990002::fl::FLb'] = {'sys_id': '990002', 'fl_id': 'FLb', 'lists': [k], 'note': 'f',
                                              'tags': [], 'cloud_rows': {k: {'id': r105, 'list': c101,
                                                                             'note': 'f', 'tags': []}}}
    a.mgr.add_item('990005', z, fl_id='FLz')            # an insert after L's
    edit_during(cloud, lambda: a.mgr.remove_item_from_list(KEY, l_), after='insert')
    assert a.up()['success']
    c103 = cloud_id(a, l_)
    assert c103 != c101 and cloud_id(a, k) == c101
    cd = pending(a)
    r104 = next(int(rid) for rid, e in cd.items() if e['list'] == c103)
    assert {rid: e['list'] for rid, e in cd.items()} == {str(r102): c101, str(r104): c103}
    mark = cloud.rec.mark()
    assert a.up()['success']
    assert deleted_ids(mark(op='delete')) == sorted([r102, r104])
    assert cloud.row(r105) is not None and cloud.row(r105)['list_id'] == c101
    down = a.down()
    assert down['success'] and down['items_added'] == 0 and item(a) is None


# --------------------------------------------------------------------------- R10: a close keeps what the upload reported

def close_during(d, after, nth=1, edit=None, user=None):
    """The app closes during an upload: before the request that follows A's nth request of kind `after`
    ('insert', 'move', 'select', 'user_lists'), edit() runs on the live store, then the program goes away
    (the request is not sent). Then what the runner's shutdown() does: abandon_upload with every report."""
    cp, base = d.mgr.begin_upload()
    d.reports = []
    seen = [0]
    cloud = d.cloud

    def kind_of(req):
        if req.t == 'user_lists':
            return 'user_lists'
        if req.t != 'list_items':
            return None
        if req.op == 'update' and 'list_id' in (req.payload or {}):
            return 'move'
        return req.op

    def hook(client, req):
        if client.actor != d.name:
            return None
        if seen[0] >= nth:
            cloud.rec.hook = None
            if edit is not None:
                edit()
            return 'gone_before'
        if kind_of(req) == after:
            seen[0] += 1
        return None
    cloud.rec.hook = hook
    try:
        d.sync.sync_to_cloud(data=cp, withdrawn=d.mgr.withdrawn_now, on_recorded=d.reports.append)
    except S._ProcessGone:
        pass
    else:
        raise AssertionError('the upload ended before the close')
    finally:
        cloud.rec.hook = None
    d.mgr.abandon_upload(base, list(d.reports), user or d.sync._user_id)
    return base


def saved(d):
    with open(d.mgr.LISTS_FILE, 'rb') as fh:
        return pickle.load(fh)


R10_CELLS = ['first-insert-then-move', 'first-insert-then-remove', 'another-accounts-first-upload',
             'completed-move-then-remove', 'chained-move-then-remove', 'new-list-then-close',
             'replaced-cloud-id-then-close', 'shared-list-repair-then-close', 'list-state-sent-then-close',
             'first-sync-match-then-remove', 'nothing-reported', 'large-store']


@pytest.mark.parametrize('cell', R10_CELLS)
def test_an_interrupted_upload_keeps_what_its_reports_say(tmp_path, cloud, cell):
    a = make_desk(tmp_path, cloud)
    if cell == 'large-store':
        _large_store_abandon(a)
        return
    k, l_, m, n, z = (a.mgr.create_list(x) for x in ('K', 'L', 'M', 'N', 'Z'))
    if cell in ('first-insert-then-move', 'first-insert-then-remove'):
        a.mgr.add_item('990001', k, note='n', fl_id='FLa')       # a store never synced: no account yet
        a.mgr.add_item('990005', z, fl_id='FLz')
        if cell == 'first-insert-then-remove':
            a.mgr.add_item('990001', m, fl_id='FLa')
        edit = ((lambda: a.mgr.move_items_to_list([KEY], k, l_)) if cell == 'first-insert-then-move'
                else (lambda: a.mgr.remove_item_from_list(KEY, k)))
        assert a.mgr.data.get('cloud_account') is None
        close_during(a, 'insert', edit=edit)
        r201 = cloud.rows(list_id=cloud_id(a, k))[0]['id']
        on_disk = saved(a)
        assert on_disk['cloud_account'] == 'u1'
        if cell == 'first-insert-then-move':
            moved = on_disk['items'][KEY]['cloud_rows']['~%s' % r201]
            assert moved['to'] == l_ and moved['list'] == cloud_id(a, k)
            b = reopen(a)                                  # after a restart, the removal of its new list
            b.mgr.remove_item_from_list(KEY, l_)
        else:
            b = reopen(a)
        assert pending(b)[str(r201)]['account'] == 'u1'
        mark = cloud.rec.mark()
        assert b.up()['success'] and deleted_ids(mark(op='delete')) == [r201] and cloud.row(r201) is None
        return
    if cell == 'another-accounts-first-upload':
        a.mgr.add_item('990001', m, note='n', fl_id='FLa')
        assert a.up()['success']
        r102 = row_in(cloud, a, m)
        a.mgr.add_item('990001', k, fl_id='FLa')
        sign_in(a, 'u2')
        close_during(a, 'insert', edit=lambda: a.mgr.remove_item_from_list(KEY, m))
        on_disk = saved(a)
        assert on_disk['cloud_account'] == 'u2'
        recs = on_disk['items'][KEY]['cloud_rows']
        assert list(recs) == [k] and cloud.row(recs[k]['id']) is not None       # the u2 row, recorded
        assert on_disk['cloud_deletes'] == {str(r102): dict(on_disk['cloud_deletes'][str(r102)], account='u1')}
        return
    if cell in ('completed-move-then-remove', 'chained-move-then-remove'):
        a.mgr.add_item('990001', k, note='n', fl_id='FLa')
        a.mgr.add_item('990001', m, fl_id='FLa')
        assert a.up()['success']
        r101 = row_in(cloud, a, k)
        a.mgr.move_items_to_list([KEY], k, l_)
        a.mgr.add_item('990005', z, fl_id='FLz')
        if cell == 'completed-move-then-remove':
            edit = lambda: a.mgr.remove_item_from_list(KEY, l_)           # noqa: E731 - M survives
        else:
            edit = lambda: (a.mgr.move_items_to_list([KEY], l_, n), a.mgr.remove_item_from_list(KEY, n))  # noqa: E731
        close_during(a, 'move', edit=edit)
        assert cloud.row(r101)['list_id'] == cloud_id(a, l_)
        assert saved(a)['cloud_deletes'][str(r101)]['list'] == cloud_id(a, l_)
        b = reopen(a)
        mark = cloud.rec.mark()
        assert b.up()['success'] and deleted_ids(mark(op='delete')) == [r101] and cloud.row(r101) is None
        assert b.down()['success'] and item(b)['lists'] == [m]
        return
    if cell == 'first-sync-match-then-remove':
        c_l = cloud.new_list('L')
        r201 = cloud.add(c_l, '990001', fl_id='FLa', note='n')
        a.mgr.add_item('990001', l_, note='n', fl_id='FLa')             # no record: the upload pairs 201 by content
        a.mgr.add_item('990001', m, fl_id='FLa')
        a.mgr.add_item('990005', z, fl_id='FLz')
        close_during(a, 'insert', edit=lambda: a.mgr.remove_item_from_list(KEY, l_))
        assert [rep for rep in a.reports if rep[0] == 'row' and rep[3] == r201]
        assert saved(a)['cloud_deletes'][str(r201)]['list'] == c_l
        b = reopen(a)
        mark = cloud.rec.mark()
        assert b.up()['success'] and deleted_ids(mark(op='delete')) == [r201]
        return
    if cell == 'nothing-reported':
        a.mgr.add_item('990001', k, note='n', fl_id='FLa')
        assert a.up()['success']
        a.mgr.add_item('990002', l_, fl_id='FLb')
        before = copy.deepcopy(a.mgr.data)
        base = close_during(a, 'select', nth=1, edit=lambda: a.mgr.add_item('990003', m, fl_id='FLc'))
        assert a.reports == []
        after = copy.deepcopy(a.mgr.data)
        assert set(after['items']) == set(before['items']) | {'990003::fl::FLc'}
        assert {k_: v for k_, v in after['items'].items() if k_ != '990003::fl::FLc'} == before['items']
        assert after.get('cloud_account') == base.get('cloud_account')
        return
    # the list and project writes of an upload
    if cell == 'new-list-then-close':
        p = a.mgr.create_list('P')
        a.mgr.add_item('990001', p, note='n', fl_id='FLa')
        a.mgr.add_item('990005', a.mgr.create_list('Y'), fl_id='FLz')   # an insert after P's
        close_during(a, 'insert')
        c = saved(a)['lists'][p].get('cloud_id')
        assert c is not None and saved(a)['items'][KEY]['cloud_rows'][p]['list'] == c
    elif cell == 'replaced-cloud-id-then-close':
        p = a.mgr.create_list('P')
        proj = a.mgr.create_project('Q')
        a.mgr.update_list_project(p, proj)
        a.mgr.data['lists'][p]['cloud_id'] = 77
        a.mgr.data['projects'][proj]['cloud_id'] = 78
        a.mgr.add_item('990001', p, note='n', fl_id='FLa')
        close_during(a, 'select')                       # after the lists were written, before any row read
        assert saved(a)['lists'][p]['cloud_id'] not in (None, 77)
        assert saved(a)['projects'][proj]['cloud_id'] not in (None, 78)
    elif cell == 'shared-list-repair-then-close':
        c101 = cloud.new_list('K')
        a.mgr.data['lists'][k]['cloud_id'] = c101
        a.mgr.data['lists'][l_]['cloud_id'] = c101
        p = l_
        close_during(a, 'select')
        assert saved(a)['lists'][k]['cloud_id'] == c101
        assert saved(a)['lists'][l_]['cloud_id'] not in (None, c101)
    else:   # 'list-state-sent-then-close'
        c_l = cloud.new_list('L')
        cloud.web.table('user_lists').update({'color': '#FF0000'}).eq('id', c_l).execute()
        a.mgr.data['lists'][l_].update({'cloud_id': c_l, 'color': '#00FF00', lists_sync.LIST_STATE_UNSENT: True})
        a.mgr.data['cloud_account'] = 'u1'
        close_during(a, 'select')
        assert lists_sync.LIST_STATE_UNSENT not in saved(a)['lists'][l_]
        assert cloud.cloud_list(c_l)['color'] == '#00FF00'
        cloud.web.table('user_lists').update({'color': '#0000FF', 'deleted_at': '2026-09-27T10:00:00+00:00'}).eq(
            'id', c_l).execute()
        b = reopen(a)
        assert b.down()['success']
        assert b.mgr.data['lists'][l_]['color'] == '#0000FF' and b.mgr.data['lists'][l_].get('deleted_at')
        assert b.up()['success']
        assert cloud.cloud_list(c_l)['color'] == '#0000FF' and cloud.cloud_list(c_l)['deleted_at']
        return
    b = reopen(a)
    b.mgr.update_list(p, name='P renamed')            # a rename before the next upload
    count = len([cl for cl in cloud.db.tables['user_lists'] if cl['user_id'] == 'u1'])
    assert b.up()['success']
    assert len([cl for cl in cloud.db.tables['user_lists'] if cl['user_id'] == 'u1']) == count   # no second list
    if cell == 'replaced-cloud-id-then-close':
        assert len(cloud.db.tables['projects']) == 1


def _large_store_abandon(a):
    """20,000 memberships, one report each (all but 50 refreshing a record the store holds)."""
    lid = a.mgr.create_list('L')
    a.mgr.data['cloud_account'] = 'u1'
    a.mgr.data['lists'][lid]['cloud_id'] = 5000
    items = a.mgr.data['items']
    for n in range(20000):
        key = f'99{n:05d}'
        items[key] = {'sys_id': key, 'lists': [lid], 'note': '', 'tags': [],
                      'cloud_rows': {lid: {'id': 100000 + n, 'list': 5000, 'note': '', 'tags': []}}}
    for n in range(50):                               # these are new
        items[f'98{n:05d}'] = {'sys_id': f'98{n:05d}', 'lists': [lid], 'note': '', 'tags': []}
    a.mgr.save()
    base = pickle.loads(pickle.dumps(a.mgr.data))
    a.mgr.begin_upload()
    reports = [('row', f'99{n:05d}', lid, 100000 + n, 5000, '', [], False) for n in range(19950)]
    reports += [('row', f'98{n:05d}', lid, 200000 + n, 5000, '', [], False) for n in range(50)]
    t0 = time.perf_counter()
    a.mgr.abandon_upload(base, reports, 'u1')
    took = time.perf_counter() - t0
    # about 0.2 s here on an idle machine; a scan of the store per report takes minutes
    assert took < 3.0, f'abandon_upload took {took:.2f}s'
    assert a.mgr.data['items']['9800007']['cloud_rows'][lid]['id'] == 200007


# --------------------------------------------------------------------------- R11-R12: the copy and what reaches it

@pytest.mark.parametrize('cell', ['insert', 'orphan-move'])
def test_an_upload_skips_an_entry_removed_before_its_request(tmp_path, cloud, cell):
    a = make_desk(tmp_path, cloud)
    k, l_, m = a.mgr.create_list('K'), a.mgr.create_list('L'), a.mgr.create_list('M')
    a.mgr.add_item('990001', m, note='n', fl_id='FLa')
    if cell == 'orphan-move':
        a.mgr.add_item('990001', k, fl_id='FLa')
    assert a.up()['success']
    if cell == 'insert':
        a.mgr.add_item('990001', l_, fl_id='FLa')      # no row yet
    else:
        r101 = row_in(cloud, a, k)
        a.mgr.move_items_to_list([KEY], k, l_)

    def remove_at_the_first_list_read(client, req):
        if client.actor == 'A' and req.t == 'list_items' and req.op == 'select':
            cloud.rec.hook = None
            a.mgr.remove_item_from_list(KEY, l_)
        return None
    cloud.rec.hook = remove_at_the_first_list_read
    mark = cloud.rec.mark()
    result = a.up()
    assert result['success']
    assert mark(op='insert') == [] and [r for r in mark(op='update') if 'list_id' in (r.payload or {})] == []
    assert [r['list_id'] for r in cloud.rows(sys_id='990001') if r['list_id'] == cloud_id(a, l_)] == []
    # every record the upload wrote was reported
    written = {(iid, key_, r['id']) for iid, it in a.copy['items'].items() for key_, r in (it.get('cloud_rows') or {}).items()
               if not r.get('gone') and not str(key_).startswith('~')}
    told = {(rep[1], rep[2], rep[3]) for rep in a.reports if rep[0] == 'row'}
    assert written and written <= told
    if cell == 'orphan-move':
        assert pending(a)[str(r101)]['list'] == cloud_id(a, k)
        assert a.up()['success'] and cloud.row(r101) is None


@pytest.mark.parametrize('cell', ['insert-one-by-one', 'orphan-move-after-a-re-read', 'orphan-move-after-a-too-long-note'])
def test_an_upload_skips_an_entry_removed_before_a_retried_request(tmp_path, cloud, cell):
    """A request the upload sends again (one by one after a refused batch; the move alone after its
    note could not be sent) asks again which memberships were removed since."""
    a = make_desk(tmp_path, cloud)
    k, l_, m = a.mgr.create_list('K'), a.mgr.create_list('L'), a.mgr.create_list('M')
    if cell == 'insert-one-by-one':
        a.mgr.add_item('990001', m, note='n', fl_id='FLa')
        assert a.up()['success']
        a.mgr.add_item('990001', l_, fl_id='FLa')
        a.mgr.add_item('990002', l_, note='o', fl_id='FLb')     # two new rows for L: one batch

        def hook(client, req):
            if client.actor == 'A' and req.t == 'list_items' and req.op == 'insert' and isinstance(req.payload, list):
                cloud.rec.hook = None
                a.mgr.remove_item_from_list(KEY, l_)           # M survives
                return 'api_23502'                              # the batch is refused whole: then one by one
            return None
    else:
        long = cell == 'orphan-move-after-a-too-long-note'
        a.mgr.add_item('990001', k, note=S.LONG_FILLER if long else 'n', fl_id='FLa')
        a.mgr.add_item('990001', m, fl_id='FLa')
        assert a.up()['success']
        rk, k_cloud = row_in(cloud, a, k), cloud_id(a, k)
        a.mgr.update_item(KEY, note='changed here')             # the move carries the note, on the condition read
        a.mgr.move_items_to_list([KEY], k, l_)

        def hook(client, req):
            if client.actor == 'A' and req.t == 'list_items' and req.op == 'update' and 'list_id' in (req.payload or {}):
                cloud.rec.hook = None
                if not long:
                    cloud.set(rk, note='changed on the website')   # the condition no longer holds
                a.mgr.remove_item_from_list(KEY, l_)           # M survives
            return None
    cloud.rec.hook = hook
    mark = cloud.rec.mark()
    result = a.up()
    cloud.rec.hook = None
    assert result['success'], result
    if cell == 'insert-one-by-one':
        singles = [r for r in mark(op='insert') if isinstance(r.payload, dict)]
        assert [r.payload['sys_id'] for r in singles] == ['990002']
        assert [r for r in cloud.rows(list_id=cloud_id(a, l_)) if r['sys_id'] == '990001'] == []
        return
    assert len(_moves_of(mark(op='update'), rk)) == 1 and cloud.row(rk)['list_id'] == k_cloud
    assert pending(a)[str(rk)]['list'] == k_cloud
    mark = cloud.rec.mark()
    assert a.up()['success'] and deleted_ids(mark(op='delete')) == [rk] and cloud.row(rk) is None


def _identity_of(store):
    out = {'store': {k: store.get(k) for k in lists_sync.IDENTITY_FIELDS['store']}}
    for part in ('projects', 'lists', 'items'):
        out[part] = {i: {k: v.get(k) for k in lists_sync.IDENTITY_FIELDS[part]} for i, v in store.get(part, {}).items()}
    return out


def _without(store):
    out = copy.deepcopy(store)
    for k in lists_sync.IDENTITY_FIELDS['store']:
        out.pop(k, None)
    for part in ('projects', 'lists', 'items'):
        for v in out.get(part, {}).values():
            for k in lists_sync.IDENTITY_FIELDS[part]:
                v.pop(k, None)
    return out


def test_an_upload_on_a_copy_changes_only_identity_fields(tmp_path, cloud):
    a = make_desk(tmp_path, cloud)
    k, l_, m = a.mgr.create_list('K'), a.mgr.create_list('L'), a.mgr.create_list('M')
    a.mgr.add_item('990001', k, note='n', fl_id='FLa')
    a.mgr.add_item('990002', l_, note='gone', fl_id='FLb')
    a.mgr.add_item('990003', k, note='moving', fl_id='FLc')
    assert a.up()['success']
    cloud.delete_row(row_in(cloud, a, l_, '990002::fl::FLb'))
    assert a.up()['web_removed']                       # a 'gone' record
    a.mgr.move_items_to_list(['990003::fl::FLc'], k, m)   # a '~' orphan
    a.mgr.data['cloud_deletes'] = {'7001': {'id': 7001, 'list': 1, 'account': 'u1'},
                                   '7002': {'id': 7002, 'list': 2, 'account': 'u9'}}
    # two items sharing a row (a store from before per-membership records)
    shared = row_in(cloud, a, k)
    a.mgr.data['items']['990001'] = {'sys_id': '990001', 'fl_id': 'FLa', 'lists': [k], 'note': 'twin', 'tags': [],
                                     'cloud_rows': {k: {'id': shared, 'list': cloud_id(a, k)}}}
    c_new = cloud.new_list('S')
    s_ = a.mgr.create_list('S')
    a.mgr.data['lists'][s_].update({'cloud_id': c_new, 'color': '#123456', lists_sync.LIST_STATE_UNSENT: True})
    live_bytes = pickle.dumps(a.mgr.data)
    snap, lists_pkl = pathlib.Path(a.mgr.LISTS_FILE + '.pre-upload'), pathlib.Path(a.mgr.LISTS_FILE)
    cp, base = a.mgr.begin_upload()
    assert snap.read_bytes() == live_bytes             # the snapshot is begin_upload's, on the thread that owns the store
    snap.unlink()
    on_disk = lists_pkl.read_bytes()
    result = a.sync.sync_to_cloud(data=cp, withdrawn=a.mgr.withdrawn_now, on_recorded=lambda rep: None)
    assert result['success'] is not None
    assert not snap.exists() and lists_pkl.read_bytes() == on_disk   # the worker writes no file
    assert pickle.dumps(a.mgr.data) == live_bytes      # the engine never touched the live store
    assert _without(cp) == _without(base)              # and changed only identity on its copy
    live_before = copy.deepcopy(a.mgr.data)
    a.mgr.finish_upload(cp, base, result)
    assert _without(a.mgr.data) == _without(live_before)
    assert _identity_of(a.mgr.data) == _identity_of(cp)     # every identity field installed
    assert lists_sync.LIST_STATE_UNSENT not in a.mgr.data['lists'][s_]


# --------------------------------------------------------------------------- R13-R15: the pass's options, the download's halves

def test_page_backfill_is_skipped_on_sign_out(tmp_path):
    c = Cloud(has_page=False)
    a = make_desk(tmp_path, c)
    lid = a.mgr.create_list('L')
    for n in range(5):
        a.mgr.add_item(f'99000{n}', lid, img='1', fl_id='FLa')
    assert a.up()['success']                           # written without the page column
    c.db.migrate()                                     # the column appears: every row wants its page

    def page_only(reqs):
        return [r for r in reqs if set(r.payload or {}) == {'page'}]
    mark = c.rec.mark()
    assert a.up(backfill_pages=False)['success']       # the sign-out's upload
    assert page_only(mark(op='update')) == []
    mark = c.rec.mark()
    assert a.up()['success']
    assert len(page_only(mark(op='update'))) == 5


def test_a_stopped_pass_says_sync_stopped_in_either_language(tmp_path, cloud, monkeypatch):
    a = make_desk(tmp_path, cloud)
    l1, l2 = a.mgr.create_list('L1'), a.mgr.create_list('L2')
    a.mgr.add_item('990001', l1, note='n', fl_id='FLa')
    a.mgr.add_item('990002', l2, note='m', fl_id='FLb')
    inserts = []
    cloud.rec.hook = lambda client, req: inserts.append(req) if req.op == 'insert' and req.t == 'list_items' else None
    result = a.up(should_stop=lambda: bool(inserts))   # stopped before the request after the first insert
    cloud.rec.hook = None
    assert result['stopped'] is True and result['success'] is False and result['error'] == 'Sync stopped'
    assert len(inserts) == 1 and rec(a, l1) is not None      # what it recorded before the stop is kept
    assert item(a, '990002::fl::FLb').get('cloud_rows') is None
    # what a sign-out notice names: the list being written when it stopped, and those after it
    assert 'L2' in result['lists_not_uploaded'] and 'L1' not in result['lists_not_uploaded']
    import genizah_core
    from genizah_app import GenizahGUI
    monkeypatch.setattr(genizah_core, 'CURRENT_LANG', 'he')
    assert GenizahGUI._sync_error_text(result) == 'הסנכרון נעצר'
    monkeypatch.setattr(genizah_core, 'CURRENT_LANG', 'en')
    assert GenizahGUI._sync_error_text(result) == 'Sync stopped'


def test_no_removal_is_sent_once_the_pass_is_stopped(tmp_path, cloud):
    a = make_desk(tmp_path, cloud)
    k = a.mgr.create_list('K')
    a.mgr.add_item('990001', k, note='n', fl_id='FLa')
    a.mgr.add_item('990002', k, note='m', fl_id='FLb')
    assert a.up()['success']
    rk = row_in(cloud, a, k)
    a.mgr.remove_item_from_list(KEY, k)
    stop = []

    def progress(done, total, what='lists'):
        if what == 'deletes':                          # every list written; the deletes' step begins
            stop.append(done)
    mark = cloud.rec.mark()
    result = a.up(should_stop=lambda: bool(stop), progress=progress)
    assert stop and result['stopped'] is True and result['success'] is False
    assert mark(op='delete') == [] and str(rk) in pending(a) and cloud.row(rk) is not None
    assert a.up()['success'] and cloud.row(rk) is None and pending(a) == {}


@pytest.mark.parametrize('removal', [True, False], ids=['with-a-removal', 'no-removal'])
def test_an_uploads_progress_counts_its_lists_and_tells_the_deletes_apart(tmp_path, cloud, removal):
    """From 0 of the lists whose entries it writes (a list in the Trash sends only its own state),
    one step per list, and the removals as a step of their own -- never an extra list."""
    a = make_desk(tmp_path, cloud)
    k, l_, t = a.mgr.create_list('K'), a.mgr.create_list('L'), a.mgr.create_list('T')
    a.mgr.add_item('990001', k, note='n', fl_id='FLa')
    a.mgr.add_item('990002', l_, fl_id='FLb')
    a.mgr.add_item('990003', t, fl_id='FLc')
    assert a.up()['success']
    a.mgr.delete_list(t)                               # to the Trash
    if removal:
        a.mgr.remove_item_from_list('990002::fl::FLb', l_)
    told = []
    result = a.up(progress=lambda *args: told.append(args))
    assert result['success']
    lists = len([lid for lid, ld in a.mgr.data['lists'].items()
                 if lid != 'recent' and not ld.get('is_system') and not ld.get('deleted_at')])
    assert lists == 3                                  # General, K and L
    assert told[:lists + 1] == [(n, lists) for n in range(lists + 1)]
    assert told[lists + 1:] == ([(0, 0, 'deletes')] if removal else [])


def test_a_sign_out_drops_the_signed_out_accounts_client(tmp_path, cloud, monkeypatch):
    a = make_desk(tmp_path, cloud)
    monkeypatch.setattr(lists_sync, '_sync_instance', a.sync)     # the manager's own sync is this desk's
    assert a.sync._get_client() is a.client
    a.mgr.disable_cloud_sync()
    assert a.sync._user_id is None and a.sync._external_client is None


@pytest.mark.parametrize('cell', ['stale-fetch', 'cancelled-fetch', 'failed-snapshot'])
def test_the_download_snapshot_is_written_by_the_apply(tmp_path, cloud, cell, monkeypatch):
    a = make_desk(tmp_path, cloud)
    lid = a.mgr.create_list('L')
    a.mgr.add_item('990001', lid, note='n', fl_id='FLa')
    assert a.up()['success']
    cloud.add(cloud_id(a, lid), '990002', fl_id='FLb', note='web')
    snap = pathlib.Path(a.mgr.LISTS_FILE + '.pre-download')
    ids = a.mgr.remembered_row_ids('u1')
    if cell == 'cancelled-fetch':
        state = a.sync.fetch_cloud_state(ids, should_stop=lambda: True)
        assert state['success'] is False and state['stopped'] is True and not snap.exists()
        return
    state = a.sync.fetch_cloud_state(ids)
    assert state['success'] and not snap.exists()      # the fetch reads the cloud only
    if cell == 'stale-fetch':
        return                                         # the runner drops a stale state: nothing is written
    monkeypatch.setattr(a.mgr, 'write_snapshot', lambda label, payload=None: False)
    before = copy.deepcopy(a.mgr.data)
    on_disk = pathlib.Path(a.mgr.LISTS_FILE).read_bytes()
    mark = cloud.rec.mark()
    result = a.sync.apply_cloud_state(state)
    assert result == {'success': False, 'error': lists_sync.DOWNLOAD_BACKUP_FAILED}
    assert a.mgr.data == before and pathlib.Path(a.mgr.LISTS_FILE).read_bytes() == on_disk
    assert mark(op='select') == [] and mark(op='update') == []
    monkeypatch.undo()
    _configured_again(monkeypatch)
    assert a.sync.apply_cloud_state(state)['success'] and snap.exists()


def _configured_again(monkeypatch):
    monkeypatch.setattr(lists_sync, 'SUPABASE_AVAILABLE', True)
    monkeypatch.setattr(lists_sync, 'SUPABASE_ANON_KEY', 'test-key')


# --------------------------------------------------------------------------- R16: every way an entry leaves a list

HELPERS = {'_queue_cloud_delete', '_forget_item', '_mark_pending_move', '_keep_web_removal'}


def _leaves_a_list(fn):
    """The method takes an id out of an item's lists, or deletes an item."""
    for node in ast.walk(fn):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr == 'remove':
            owner = node.func.value
            if isinstance(owner, ast.Subscript) and isinstance(owner.slice, ast.Constant) and owner.slice.value == 'lists':
                return True
        if isinstance(node, ast.Delete):
            for t in node.targets:
                if isinstance(t, ast.Subscript) and isinstance(t.value, ast.Subscript) and \
                        isinstance(t.value.slice, ast.Constant) and t.value.slice.value == 'items':
                    return True
                if isinstance(t, ast.Subscript) and isinstance(t.value, ast.Name) and t.value.id == 'items':
                    return True
    return False


def _books(fn):
    return {node.args[0].value for node in ast.walk(fn)
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr == '_book'
            and node.args and isinstance(node.args[0], ast.Constant)}


def test_every_list_mutation_that_leaves_a_list_does_its_bookkeeping(tmp_path, cloud):
    tree = ast.parse((REPO / 'shared' / 'lists_manager.py').read_text(encoding='utf-8'))
    cls = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == 'ListsManager')
    leaving = {fn.name: _books(fn) for fn in cls.body if isinstance(fn, ast.FunctionDef) and _leaves_a_list(fn)}
    assert set(leaving) >= {'delete_list', 'merge_duplicate_group', 'remove_item_from_list', 'move_items_to_list',
                            'resolve_web_removals'}, sorted(leaving)
    for name, booked in leaving.items():
        assert booked & HELPERS, f'ListsManager.{name} takes an entry out of a list without its bookkeeping'
    # and each path of the table does what it says
    a = make_desk(tmp_path, cloud)
    k, l_, m, t_ = (a.mgr.create_list(x) for x in ('K', 'L', 'M', 'T'))
    keys = {}
    for n, lid in enumerate((k, l_, m, t_)):
        keys[lid] = f'99010{n}::fl::F{n}'
        a.mgr.add_item(f'99010{n}', lid, note=f'n{n}', fl_id=f'F{n}')
        a.mgr.add_item(f'99010{n}', m if lid != m else k, fl_id=f'F{n}')
    assert a.up()['success']

    def row(key, lid):
        return rec(a, lid, key)['id']
    r = row(keys[k], k)
    a.mgr.remove_item_from_list(keys[k], k)                       # remove from one of two
    assert str(r) in pending(a) and rec(a, k, keys[k]) is None
    r = row(keys[l_], l_)
    a.mgr.move_items_to_list([keys[l_]], l_, k)                   # move
    assert rec(a, '~%s' % r, keys[l_])['to'] == k
    r = row(keys[t_], t_)
    a.mgr.delete_list(t_)                                         # to the Trash: nothing
    assert str(r) not in pending(a) and rec(a, t_, keys[t_])['id'] == r
    a.mgr.restore_list(t_)
    a.mgr.delete_list(t_, permanent=True)                         # delete permanently
    assert str(r) in pending(a)
    r1, r2 = row(keys[m], m), row(keys[m], k)
    a.mgr.remove_item_from_list(keys[m], m)
    a.mgr.remove_item_from_list(keys[m], k)                       # its last list: the entry is deleted
    assert {str(r1), str(r2)} <= set(pending(a)) and item(a, keys[m]) is None
    assert a.up()['success']
    d1, d2 = a.mgr.create_list('Dup'), a.mgr.create_list('Dup')
    a.mgr.add_item('990200', d1, fl_id='G1')
    assert a.up()['success']
    r = row('990200::fl::G1', d1)
    a.mgr.merge_duplicate_group(d2, [d1])                         # Clean up duplicate lists
    assert rec(a, '~%s' % r, '990200::fl::G1')['to'] == d2
    e = a.mgr.create_list('E')
    a.mgr.add_item('990300', e, fl_id='H1')
    assert a.up()['success']
    a.mgr.delete_list(e)
    assert a.up()['success']
    r = row('990300::fl::H1', e)
    assert a.mgr.empty_trash() == 1                               # empty the Trash
    assert str(r) in pending(a)
    # the prompt: Remove forgets a tombstone (nothing to delete), Keep lets the entry go up again
    w = a.mgr.create_list('W')
    a.mgr.add_item('990400', w, fl_id='J1')
    a.mgr.add_item('990401', w, fl_id='J2')
    assert a.up()['success']
    for key in ('990400::fl::J1', '990401::fl::J2'):
        cloud.delete_row(row(key, w))
    assert len(a.up()['web_removed']) == 2
    before = set(pending(a))
    assert a.mgr.resolve_web_removals({('990400::fl::J1', w): 'remove', ('990401::fl::J2', w): 'keep'}) == (1, 1)
    assert set(pending(a)) == before and item(a, '990400::fl::J1') is None
    assert rec(a, w, '990401::fl::J2') is None and w in item(a, '990401::fl::J2')['lists']
    on_disk = saved(a)                                            # the answers are saved with the lists
    assert '990400::fl::J1' not in on_disk['items']
    assert (on_disk['items']['990401::fl::J2'].get('cloud_rows') or {}).get(w) is None


# --------------------------------------------------------------------------- R17: a moved row whose destination has one

@pytest.mark.parametrize('cell', ['held', 'website-edited', 'over-gateway-limit', 'delete-raises'])
def test_a_redundant_row_is_deleted_only_when_its_text_is_held(tmp_path, cloud, cell):
    a = make_desk(tmp_path, cloud)
    k, l_ = a.mgr.create_list('K'), a.mgr.create_list('L')
    note = S.LONG_FILLER if cell == 'over-gateway-limit' else 'n'
    a.mgr.add_item('990001', k, note=note, fl_id='FLa')
    a.mgr.add_item('990001', l_, fl_id='FLa')
    assert a.up()['success']
    rk, rl = row_in(cloud, a, k), row_in(cloud, a, l_)
    if cell == 'website-edited':
        cloud.set(rk, note='n\nedited on the website')
    a.mgr.move_items_to_list([KEY], k, l_)             # L already has its own row
    if cell == 'delete-raises':
        cloud.rec.hook = lambda client, req: 'raise_before' if client.actor == 'A' and req.op == 'delete' else None
    mark = cloud.rec.mark()
    result = a.up()
    cloud.rec.hook = None
    if cell == 'delete-raises':                        # tried again at the next upload, and this one is not a success
        assert result['success'] is False and result['complete'] is False and result['items_failed'] == 1
        assert cloud.row(rk) is not None and rec(a, '~%s' % rk)['to'] == l_
        assert a.up()['success'] and cloud.row(rk) is None and rec(a, '~%s' % rk) is None
        return
    assert result['success']
    deletes = mark(op='delete')
    if cell == 'held':
        (d,) = deletes
        assert ('eq', 'id', rk) in d.filters and not explicit(deletes)
        assert cloud.row(rk) is None and cloud.row(rl) is not None and rec(a, '~%s' % rk) is None
        return
    if cell == 'over-gateway-limit':
        assert cloud.row(rk) is not None and result['notes_too_long'] == 1
        return
    assert deletes == [] and cloud.row(rk) is not None
    assert result['notes_kept'] == 1 and rec(a, '~%s' % rk)['differs'] is True
    assert not rec(a, l_).get('differs')               # L's own row agrees
    assert result['notes_differing'] == 1 and a.mgr.differing_notes_count() == 1
    assert a.down()['success']                         # folds the website's line into the entry
    assert 'edited on the website' in item(a)['note'] and a.mgr.differing_notes_count() == 0
    mark = cloud.rec.mark()
    assert a.up()['success']
    assert deleted_ids(mark(op='delete')) == [rk] and cloud.row(rk) is None


# --------------------------------------------------------------------------- R18: who may start a sync

SYNC_ENTRY_POINTS = {'sync_to_cloud', 'sync_from_cloud', 'fetch_cloud_state', 'apply_cloud_state',
                     'get_cloud_lists_preview', 'sync_list_to_cloud', 'sync_item_to_cloud', 'delete_list_from_cloud',
                     'delete_item_from_cloud'}


def test_only_the_runner_starts_a_list_sync():
    offenders = []
    files = [REPO / 'genizah_app.py'] + sorted((REPO / 'desktop').rglob('*.py'))
    for path in files:
        if path.name == 'lists_sync_runner.py':
            continue
        tree = ast.parse(path.read_text(encoding='utf-8'))
        for node in ast.walk(tree):
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and \
                    node.func.attr in SYNC_ENTRY_POINTS:
                offenders.append(f'{path.relative_to(REPO)}:{node.lineno} {node.func.attr}')
    assert offenders == []


# --------------------------------------------------------------------------- R21: an entry put back before its removal went

R21_CELLS = ['readd-before-upload', 'readd-during-upload', 'readd-after-a-failed-delete', 'readd-as-another-folio',
             'readd-while-its-delete-is-sent', 'pending-row-moved-elsewhere', 'readd-after-a-move',
             'readd-under-another-account',
             # the other ways an entry goes into a list (add_item is readd-before-upload's)
             'readd-by-bulk-add', 'readd-by-bulk-add-to-a-listed-entry', 'readd-by-merge-lists',
             'readd-by-duplicate-merge']
# the entry is also in X from the start, so the removal from K leaves it in a list
STILL_LISTED = ('readd-by-bulk-add-to-a-listed-entry', 'readd-by-merge-lists')


@pytest.mark.parametrize('cell', R21_CELLS)
def test_an_entry_put_back_before_its_removal_went_keeps_its_website_row(tmp_path, cloud, cell):
    a = make_desk(tmp_path, cloud)
    if cell == 'readd-after-a-move':
        _put_back_after_a_move(a, cloud)
        return
    if cell == 'readd-under-another-account':
        _put_back_under_another_account(a, cloud)
        return
    k, x, z = a.mgr.create_list('K'), a.mgr.create_list('X'), a.mgr.create_list('Z')
    key = '990001::img::1' if cell == 'readd-as-another-folio' else KEY
    a.mgr.add_item('990001', k, note='n', fl_id='FLa', img='1' if cell == 'readd-as-another-folio' else None)
    if cell in STILL_LISTED:
        a.mgr.add_item('990001', x, fl_id='FLa')
    assert a.up()['success']
    r101 = row_in(cloud, a, k, key)
    if cell == 'readd-by-duplicate-merge':
        a.mgr.add_item('990001', x, fl_id='FLa')       # X, merged into K below, holds no row of it
    if cell == 'pending-row-moved-elsewhere':
        b = make_desk(tmp_path, cloud, 'B')
        assert b.down()['success']
        a.mgr.add_item('990001', x, fl_id='FLa')       # also in X here, with no row there yet
        a.mgr.remove_item_from_list(KEY, k)
        b.mgr.move_items_to_list([KEY], lists_named(b)['K'], lists_named(b)['X'])
        assert b.up()['success'] and cloud.row(r101)['list_id'] == cloud_id(a, x)
        mark = cloud.rec.mark()
        assert a.up()['success']
        assert mark(op='delete') == [] and mark(op='insert') == []
        assert rec(a, x)['id'] == r101 and pending(a) == {}
        return
    cloud.set(r101, note='n\nadded on the website')   # written there after the removal was made here
    if cell == 'readd-during-upload':                  # removed and put back while an upload runs
        a.mgr.add_item('990005', z, fl_id='FLz')
        edit_during(cloud, lambda: (a.mgr.remove_item_from_list(KEY, k), a.mgr.add_item('990001', k, fl_id='FLa')))
        assert a.up()['success']
    else:
        a.mgr.remove_item_from_list(key, k)            # unless it is in X too, the entry is deleted
        assert str(r101) in pending(a)
    if cell == 'readd-after-a-failed-delete':
        cloud.rec.hook = lambda client, req: 'raise_before' if client.actor == 'A' and req.op == 'delete' else None
        assert a.up()['removals_failed'] == 1
        cloud.rec.hook = None
    if cell == 'readd-while-its-delete-is-sent':
        # put back while the upload that sends its delete runs: as if put back right after it
        edit_during(cloud, lambda: a.mgr.add_item('990001', k, fl_id='FLa'))
        assert a.up()['success'] and cloud.row(r101) is None and rec(a, k) is None
        mark = cloud.rec.mark()
        assert a.up()['success'] and len(mark(op='insert')) == 1 and mark(op='delete') == []
        return
    if cell == 'readd-during-upload':
        pass
    elif cell == 'readd-as-another-folio':
        a.mgr.add_item('990001', k, fl_id='FLb', img='1')      # the same key, another folio: not that row's entry
    elif cell in ('readd-by-bulk-add', 'readd-by-bulk-add-to-a-listed-entry'):
        assert a.mgr.add_items_bulk([{'sys_id': '990001', 'fl_id': 'FLa'}], k) == 1
    elif cell == 'readd-by-merge-lists':
        assert a.mgr.merge_lists(x, k, delete_source=False)
    elif cell == 'readd-by-duplicate-merge':
        assert a.mgr.merge_duplicate_group(k, [x])['merged_items'] == 1
    else:
        a.mgr.add_item('990001', k, fl_id='FLa')
    mark = cloud.rec.mark()
    assert a.up()['success']
    if cell == 'readd-as-another-folio':
        assert deleted_ids(mark(op='delete')) == [r101] and len(mark(op='insert')) == 1
        return
    assert mark(op='delete') == [] and mark(op='insert') == [] and pending(a) == {}
    assert cloud.row(r101)['note'] == 'n\nadded on the website' and rec(a, k)['id'] == r101
    assert a.down()['success'] and 'added on the website' in item(a)['note']
    mark = cloud.rec.mark()
    assert a.up()['success'] and mark(op='delete') == [] and mark(op='insert') == []
    assert a.mgr.pending_web_removals() == []


def _put_back_after_a_move(a, cloud):
    """Moved K -> L, removed from L (M survives), put back in L before the upload: the move stands.

    The row is still in K's cloud list, on its way to L: it is moved there, as if the
    removal never happened -- not recorded as L's own row, which would give L a second
    row and bring the entry back to K at the next Download.
    """
    k, l_, m = (a.mgr.create_list(x) for x in ('K', 'L', 'M'))
    a.mgr.add_item('990001', k, note='n', fl_id='FLa')
    a.mgr.add_item('990001', m, fl_id='FLa')
    assert a.up()['success']
    rk, rm, k_cloud = row_in(cloud, a, k), row_in(cloud, a, m), cloud_id(a, k)
    a.mgr.move_items_to_list([KEY], k, l_)
    a.mgr.remove_item_from_list(KEY, l_)
    assert str(rk) in pending(a)
    a.mgr.add_item('990001', l_, fl_id='FLa')
    assert pending(a) == {} and rec(a, '~%s' % rk)['to'] == l_ and rec(a, l_) is None
    mark = cloud.rec.mark()
    assert a.up()['success']
    (move,) = _moves_of(mark(op='update'), rk)
    assert move.payload == {'list_id': cloud_id(a, l_)} and ('eq', 'list_id', k_cloud) in move.filters
    assert mark(op='insert') == [] and mark(op='delete') == []
    assert {r['id']: r['list_id'] for r in cloud.rows(sys_id='990001')} == {rk: cloud_id(a, l_), rm: cloud_id(a, m)}
    assert a.down()['success'] and sorted(item(a)['lists']) == sorted([l_, m])


def _put_back_under_another_account(a, cloud):
    """Removed as u1, put back after u2 signed in: u2 does not take u1's row, and u1's removal still goes."""
    k = a.mgr.create_list('K')
    a.mgr.add_item('990001', k, note='n', fl_id='FLa')
    assert a.up()['success']
    rk = row_in(cloud, a, k)
    a.mgr.remove_item_from_list(KEY, k)               # as u1: the entry is deleted
    sign_in(a, 'u2')
    assert a.up()['success']                          # u2's first upload over this store
    a.mgr.add_item('990001', k, note='n', fl_id='FLa')
    assert pending(a)[str(rk)]['account'] == 'u1' and rec(a, k) is None
    mark = cloud.rec.mark()
    result = a.up()
    assert result['success'] and result['web_removed'] == [] and a.mgr.pending_web_removals() == []
    (ins,) = mark(op='insert')
    assert ins.payload['list_id'] == cloud_id(a, k) and mark(op='delete') == []
    assert rec(a, k)['id'] != rk and cloud.row(rk) is not None and pending(a)[str(rk)]['account'] == 'u1'
    sign_in(a, 'u1')
    mark = cloud.rec.mark()
    assert a.up()['success']
    assert deleted_ids(mark(op='delete')) == [rk] and cloud.row(rk) is None and pending(a) == {}


def test_an_entry_made_again_with_another_note_does_not_overwrite_its_old_row(tmp_path, cloud):
    a = make_desk(tmp_path, cloud)
    k = a.mgr.create_list('K')
    a.mgr.add_item('990001', k, note='kept on the website', fl_id='FLa')
    assert a.up()['success']
    rk = row_in(cloud, a, k)
    a.mgr.remove_item_from_list(KEY, k)               # the entry is deleted ...
    a.mgr.add_item('990001', k, note='', fl_id='FLa')  # ... and made again, with no note
    mark = cloud.rec.mark()
    result = a.up()
    assert result['success'] and mark(op='delete') == [] and mark(op='insert') == []
    assert cloud.row(rk)['note'] == 'kept on the website'   # the new entry does not hold it: kept, not replaced
    assert result['notes_kept'] == 1 and rec(a, k)['id'] == rk
    assert a.down()['success'] and item(a)['note'] == 'kept on the website'


# --------------------------------------------------------------------------- R22-R24

@pytest.mark.parametrize('cell', ['cloud-only-changed', 'both-changed'])
def test_the_public_fetch_and_apply_keep_the_confirmation_only_rows(tmp_path, cell):
    c = Cloud(max_rows=50)
    a = make_desk(tmp_path, c)
    lid = a.mgr.create_list('L')
    for n in range(80):
        a.mgr.add_item(f'99{n:04d}', lid, note=f'n{n}', fl_id='FLa')
    assert a.up()['success']
    rows = c.rows(list_id=cloud_id(a, lid))
    target, victim = rows[50], rows[5]
    key = next(k for k, it in a.mgr.data['items'].items() if it['sys_id'] == target['sys_id'])
    c.set(target['id'], note='website')
    if cell == 'both-changed':
        a.mgr.update_item(key, note='mine')

    def short_read(client, req):
        # At the list's second page: the website removes a row the first page returned, and the
        # server answers that page without its keyset filter (the first rows again). The read ends
        # there, short, and the rows past its first page are located by the confirmation alone.
        if client.actor == 'A' and S.is_later_page(req):
            c.rec.hook = None
            c.delete_row(victim['id'])
            req.filters[:] = [f for f in req.filters if f[0] != 'gt']
    c.rec.hook = short_read
    items_before = set(a.mgr.data['items'])
    state = a.sync.fetch_cloud_state(a.mgr.remembered_row_ids('u1'))
    assert state['success'] and state['pass'].confirmed_only
    result = a.sync.apply_cloud_state(state)
    assert result['success'] and result['web_removed'] == []
    mark = "\n\n--- from the cloud ---\n"
    assert item(a, key)['note'] == ('website' if cell == 'cloud-only-changed' else 'mine' + mark + 'website')
    assert set(a.mgr.data['items']) == items_before
    victim_key = next(k for k, it in a.mgr.data['items'].items() if (rec(a, lid, k) or {}).get('id') == victim['id'])
    assert not rec(a, lid, victim_key).get('gone') and result['unchecked'] == 0


@pytest.mark.parametrize('cell', ['delete-raises', 'matches-nothing-reread-unknown'])
def test_an_unsent_removal_makes_the_upload_incomplete(tmp_path, cloud, cell, monkeypatch):
    from desktop.lists_sync_runner import ListsSyncRunner
    a = make_desk(tmp_path, cloud)
    monkeypatch.setattr(lists_sync, '_sync_instance', a.sync)     # the manager's own sync is this desk's
    k = a.mgr.create_list('K')
    a.mgr.add_item('990001', k, note='n', fl_id='FLa')
    assert a.up()['success']
    rk = row_in(cloud, a, k)
    a.mgr.remove_item_from_list(KEY, k)

    def fail(client, req):
        if client.actor != 'A' or req.t != 'list_items':
            return None
        if req.op == 'delete':
            return 'raise_before' if cell == 'delete-raises' else 'anon'
        if cell != 'delete-raises' and req.op == 'select' and ('eq', 'id', rk) in req.filters:
            return 'raise_before'                      # and the row cannot be read again
        return None
    cloud.rec.hook = fail
    runner = ListsSyncRunner(a.mgr, inline=True)
    outcomes = []
    runner.run('upload', on_done=outcomes.append)
    cloud.rec.hook = None
    (outcome,) = outcomes
    up = outcome['upload']
    assert up['items_failed'] == 1 and up['removals_failed'] == 1
    assert up['success'] is False and up['complete'] is False
    assert runner.unsent is True and str(rk) in pending(a) and cloud.row(rk) is not None


def _writes_reached_from_upload():
    """The functions of shared/lists_sync.py an upload runs (by name, through self./module calls)."""
    tree = ast.parse((REPO / 'shared' / 'lists_sync.py').read_text(encoding='utf-8'))
    funcs = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef):
            funcs.setdefault(node.name, node)
    reached, todo = set(), ['_upload']
    while todo:
        name = todo.pop()
        if name in reached or name not in funcs:
            continue
        reached.add(name)
        for node in ast.walk(funcs[name]):
            if isinstance(node, ast.Call):
                f = node.func
                todo.append(f.attr if isinstance(f, ast.Attribute) else getattr(f, 'id', ''))
    return reached, funcs


def test_a_close_rebuilds_what_the_upload_wrote(tmp_path, cloud):
    # (i) inside an upload, only _set_field writes a list's or project's cloud id or unsent marks,
    # and only record_row makes a record
    reached, funcs = _writes_reached_from_upload()
    assert {'_push_projects_and_lists', '_push_list_items', '_write_list', '_move_orphan', 'record_row',
            '_set_field', '_send_deletes'} <= reached
    marks = {'cloud_id', 'LIST_STATE_UNSENT', 'LIST_NAME_UNSENT', 'list_state_unsent', 'list_name_unsent'}
    for name in reached - {'_set_field', 'record_row', 'apply_account_guard', '_guard', '_repair_shared_cloud_rows'}:
        for node in ast.walk(funcs[name]):
            if isinstance(node, ast.Assign):
                for t in node.targets:
                    if isinstance(t, ast.Subscript):
                        key = t.slice.value if isinstance(t.slice, ast.Constant) else getattr(t.slice, 'id', None)
                        assert key not in marks, f'{name}:{node.lineno} writes {key} outside _set_field'
                        assert key != 'cloud_rows', f'{name}:{node.lineno} makes a record outside record_row'
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr == 'pop' \
                    and node.args:
                arg = node.args[0]
                key = arg.value if isinstance(arg, ast.Constant) else getattr(arg, 'id', None)
                owner = node.func.value
                on_item = isinstance(owner, ast.Name) and owner.id in ('it', 'item', 'items')
                assert key not in marks or on_item, f'{name}:{node.lineno} pops {key} outside _set_field'
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and \
                    node.func.attr == 'setdefault' and node.args and isinstance(node.args[0], ast.Constant):
                assert node.args[0].value != 'cloud_rows', f'{name}:{node.lineno} makes a record outside record_row'
    # (ii) every kind of list and project write, then a close: the reports rebuild the copy exactly
    a = make_desk(tmp_path, cloud)
    cloud.web.table('projects').insert({'user_id': 'u1', 'name': 'Taken'}).execute()
    stale_p, taken_p, new_p = a.mgr.create_project('Stale'), a.mgr.create_project('Taken'), a.mgr.create_project('New')
    a.mgr.data['projects'][stale_p]['cloud_id'] = 91
    c_same = cloud.new_list('Same')
    c_shared = cloud.new_list('Shared')
    c_state = cloud.new_list('State')
    lists = {n: a.mgr.create_list(n) for n in ('StaleL', 'Same', 'NewL', 'Shared', 'Shared2', 'State', 'Moves')}
    a.mgr.data['lists'][lists['StaleL']]['cloud_id'] = 92
    a.mgr.data['lists'][lists['Shared']]['cloud_id'] = c_shared
    a.mgr.data['lists'][lists['Shared2']]['cloud_id'] = c_shared
    a.mgr.data['lists'][lists['State']].update({'cloud_id': c_state, lists_sync.LIST_STATE_UNSENT: True})
    a.mgr.update_list_project(lists['NewL'], new_p)
    a.mgr.update_list_project(lists['Same'], taken_p)
    a.mgr.update_list_project(lists['StaleL'], stale_p)
    c_moves = cloud.new_list('Moves')
    a.mgr.data['lists'][lists['Moves']]['cloud_id'] = c_moves
    a.mgr.data['cloud_account'] = 'u1'
    r_match = cloud.add(c_same, '990010', fl_id='M1', note='match')          # a content match
    a.mgr.add_item('990010', lists['Same'], note='match', fl_id='M1')
    a.mgr.add_item('990011', lists['NewL'], note='insert', fl_id='I1')       # an insert
    r_move = cloud.add(c_moves, '990012', fl_id='O1', note='move')           # an orphan move
    a.mgr.data['items']['990012::fl::O1'] = {'sys_id': '990012', 'fl_id': 'O1', 'lists': [lists['State']], 'note': 'move',
                                             'tags': [], 'cloud_rows': {'~%s' % r_move: {
                                                 'id': r_move, 'list': c_moves, 'note': 'move', 'tags': [],
                                                 'to': lists['State']}}}
    r_adopt = cloud.add(c_state, '990013', fl_id='A1', note='adopt')         # an adoption
    a.mgr.data['items']['990013::fl::A1'] = {'sys_id': '990013', 'fl_id': 'A1', 'lists': [lists['State']],
                                             'note': 'adopt', 'tags': [], 'cloud_rows': {'~%s' % r_adopt: {
                                                 'id': r_adopt, 'list': c_state, 'note': 'adopt', 'tags': [],
                                                 'to': lists['State']}}}
    cp, base = a.mgr.begin_upload()
    reports = []
    result = a.sync.sync_to_cloud(data=cp, withdrawn=a.mgr.withdrawn_now, on_recorded=reports.append)
    assert result['success'], result
    kinds = {(r[0], r[1] if r[0] == 'field' else None, r[3] if r[0] == 'field' else None) for r in reports}
    assert ('field', 'projects', 'cloud_id') in kinds and ('field', 'lists', 'cloud_id') in kinds
    assert ('field', 'lists', lists_sync.LIST_STATE_UNSENT) in kinds
    assert {r[3] for r in reports if r[0] == 'row'} >= {r_match, r_move, r_adopt}
    a.mgr.abandon_upload(base, reports, 'u1')          # the close: nothing but the reports
    live = a.mgr.data
    for pid, pd in cp['projects'].items():
        assert live['projects'][pid].get('cloud_id') == pd.get('cloud_id'), pid
    for lid, ld in cp['lists'].items():
        for key_ in ('cloud_id', lists_sync.LIST_STATE_UNSENT):
            assert live['lists'][lid].get(key_) == ld.get(key_), (lid, key_)
    for iid, it in cp['items'].items():
        assert live['items'][iid].get('cloud_rows') == it.get('cloud_rows'), iid
    assert cloud_id(a, 'default') is not None and lists_sync.LIST_STATE_UNSENT not in live['lists'][lists['State']]
