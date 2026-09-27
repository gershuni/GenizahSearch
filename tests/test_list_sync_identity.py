# -*- coding: utf-8 -*-
"""Directed pins of the list sync's per-membership rules (shared/lists_sync.py).

The seeded scenario gate (tests/test_list_sync_scenarios.py) is the main check of
the sync; these tests pin specific rulings and cases it does not generate: the
exact keep-both format, the page column's degraded mode, website removals,
accounts, the helpers, retries, caps, and moves made on the desktop. They drive a
real ListsManager (lists.pkl under tmp_path) and a real ListsCloudSync against the
scenario gate's stateful PostgREST stand-in; nothing reaches Supabase.
"""
import copy
import json
import os
import pickle
import random
import sys
import threading
import types

import pytest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import list_sync_scenarios as S  # noqa: E402
from shared import lists_manager as lm  # noqa: E402
from shared import lists_sync  # noqa: E402

MARK = "\n\n--- from the cloud ---\n"
BOTH = pytest.mark.parametrize('has_page', [True, False], ids=['page-column', 'no-page-column'])


# --------------------------------------------------------------------------- the stand-ins

class Recorder:
    """Every request the clients make, and a hook that may act on one before it runs."""

    def __init__(self):
        self.reqs = []
        self.hook = None

    def before(self, client, req):
        self.reqs.append(types.SimpleNamespace(actor=client.actor, table=req.t, op=req.op, filters=list(req.filters),
                                               payload=copy.deepcopy(req.payload), cols=req.selected_cols()))
        return self.hook(client, req) if self.hook else None

    def after(self, client, req):
        return None

    def of(self, actor='A', table='list_items', op=None):
        return [r for r in self.reqs if r.actor == actor and r.table == table and (op is None or r.op == op)]

    def mark(self):
        n = len(self.reqs)
        return lambda actor='A', table='list_items', op=None: [
            r for r in self.reqs[n:] if r.actor == actor and r.table == table and (op is None or r.op == op)]


class Cloud:
    def __init__(self, has_page=True, max_rows=None, past_end_raises=False, page_lag=False):
        self.db = S.FakeDB(random.Random(7), has_page=has_page, max_rows=max_rows, past_end_raises=past_end_raises,
                           page_lag=page_lag)
        self.rec = Recorder()
        self.db.world = self.rec
        self.web = S.FakeClient(self.db, 'web', 'u1')

    def new_list(self, name, user='u1', deleted_at=None):
        client = self.web if user == 'u1' else S.FakeClient(self.db, 'web', user)
        row = client.table('user_lists').insert({'user_id': user, 'name': name, 'name_en': name,
                                                 'deleted_at': deleted_at}).execute().data[0]
        return row['id']

    def add(self, list_id, sys_id, fl_id=None, note='', tags=None, page=None):
        """A row as the website adds it (and, with page=, as a desktop wrote it)."""
        row = self.web.table('list_items').insert({'list_id': list_id, 'sys_id': sys_id, 'shelfmark': None,
                                                   'title': None, 'fl_id': fl_id, 'note': note,
                                                   'tags': tags or []}).execute().data[0]
        if page is not None:
            self.row(row['id'])['page'] = page
        return row['id']

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


def make_desk(tmp_path, cloud, name='A', user='u1'):
    class Manager(lm.ListsManager):
        LISTS_FILE = str(tmp_path / f'{name}-lists.pkl')

    mgr = Manager(None)
    client = S.FakeClient(cloud.db, name, user)
    sync = lists_sync.ListsCloudSync(mgr)
    sync.set_client(client)
    sync.set_user(user)
    return types.SimpleNamespace(mgr=mgr, sync=sync, client=client, up=sync.sync_to_cloud, down=sync.sync_from_cloud,
                                 data=lambda: mgr.data, name=name)


def merge(d):
    down = d.down()
    return down, (d.up() if down.get('success') else None)


def item(d, key):
    return d.mgr.data['items'][key]


def rec(d, key, list_id):
    return (item(d, key).get('cloud_rows') or {}).get(list_id)


def cloud_id(d, list_id):
    return d.mgr.data['lists'][list_id]['cloud_id']


def saved(d):
    with open(d.mgr.LISTS_FILE, 'rb') as fh:
        return pickle.load(fh)


@pytest.fixture(autouse=True)
def _configured(monkeypatch):
    monkeypatch.setattr(lists_sync, 'SUPABASE_AVAILABLE', True)
    monkeypatch.setattr(lists_sync, 'SUPABASE_ANON_KEY', 'test-key')
    import genizah_core
    monkeypatch.setattr(genizah_core, 'CURRENT_LANG', 'en')


@pytest.fixture
def cloud():
    return Cloud()


# --------------------------------------------------------------------------- identity

@BOTH
def test_a_second_computer_gets_both_pages_back(tmp_path, has_page):
    c = Cloud(has_page=has_page)
    a, b = make_desk(tmp_path, c, 'A'), make_desk(tmp_path, c, 'B')
    lid = a.mgr.create_list('Pages')
    a.mgr.add_item('990001', lid, note='p1', img='1')
    a.mgr.add_item('990001', lid, note='p2', img='2')
    assert a.up()['success']
    assert len(c.rows(sys_id='990001')) == 2

    assert b.down()['success']
    got = {k: it['note'] for k, it in b.mgr.data['items'].items() if it['sys_id'] == '990001'}
    if has_page:
        assert got == {'990001::img::1': 'p1', '990001::img::2': 'p2'}
    else:
        # without the column nothing says which page a row is: two whole-manuscript entries
        assert sorted(got.values()) == ['p1', 'p2'] and len(got) == 2
        assert all(b.mgr.data['items'][k].get('img') is None for k in got)


def test_pages_are_written_once_the_column_exists(tmp_path):
    c = Cloud(has_page=False)
    a = make_desk(tmp_path, c)
    lid = a.mgr.create_list('Folios')
    a.mgr.add_item('990001', lid, note='n', fl_id='FLa', img='2')
    assert a.up()['success']
    (row,) = c.rows(sys_id='990001')
    assert 'page' not in row
    c.db.migrate()
    assert a.up()['success']
    assert c.row(row['id'])['page'] == '2'
    a.mgr.add_item('990002', lid, img='5')
    assert a.up()['success']
    assert c.rows(sys_id='990002')[0]['page'] == '5'


# --------------------------------------------------------------------------- notes and tags

@BOTH
@pytest.mark.parametrize('how', ['merge', 'download'])
def test_merge_and_download_keep_both_differing_notes_marked_and_combine_tags(tmp_path, has_page, how):
    c = Cloud(has_page=has_page)
    a = make_desk(tmp_path, c)
    lid = a.mgr.create_list('Mine')
    a.mgr.add_item('990001', lid, note='mine', tags=['a'], fl_id='FLa')
    cl = c.new_list('Mine')
    rid = c.add(cl, '990001', fl_id='FLa', note='theirs', tags=['b'])
    for _ in range(2):
        merge(a) if how == 'merge' else a.down()
        it = item(a, '990001::fl::FLa')
        assert it['note'] == 'mine' + MARK + 'theirs'
        assert it['tags'] == ['a', 'b']
    row = c.row(rid)
    if how == 'merge':
        assert row['note'] == 'mine' + MARK + 'theirs' and sorted(row['tags']) == ['a', 'b']
    else:
        assert row['note'] == 'theirs' and row['tags'] == ['b']


@BOTH
def test_after_a_backup_recovery_the_upload_keeps_the_newer_cloud_note(tmp_path, has_page):
    c = Cloud(has_page=has_page)
    a = make_desk(tmp_path, c)
    lid = a.mgr.create_list('L')
    a.mgr.add_item('990001', lid, note='v1', fl_id='FLa')
    assert a.up()['success']
    backup = copy.deepcopy(a.mgr.data)
    a.mgr.update_item('990001::fl::FLa', note='v2')
    assert a.up()['success']
    (row,) = c.rows(sys_id='990001')
    assert row['note'] == 'v2'

    a.mgr.data = backup                   # lists.pkl restored from an older copy
    result = a.up()
    assert result['success'] and result['notes_kept'] == 1
    assert c.row(row['id'])['note'] == 'v2', "the upload pushed the older note over the newer cloud note"
    result = a.down()
    assert result['success'] and result['notes_merged'] == 0
    assert item(a, '990001::fl::FLa')['note'] == 'v2'   # only the cloud had changed since: taken, unmarked


@BOTH
def test_the_first_upload_after_the_update_keeps_a_differing_cloud_note(tmp_path, has_page):
    c = Cloud(has_page=has_page)
    a = make_desk(tmp_path, c)
    lid = a.mgr.create_list('L')
    a.mgr.add_item('990001', lid, note='x', fl_id='FLa')
    a.mgr.add_item('990002', lid, note='ok, and more', fl_id='FLb')
    cl = c.new_list('L')
    r1 = c.add(cl, '990001', fl_id='FLa', note='')
    r2 = c.add(cl, '990002', fl_id='FLb', note='ok')
    result = a.up()
    assert result['success']
    assert result['notes_kept'] == 2 and result['notes_differing'] == 2
    assert c.row(r1)['note'] == '' and c.row(r2)['note'] == 'ok'
    assert len(c.rows(list_id=cl)) == 2


def test_short_local_note_is_not_swallowed_by_a_longer_cloud_note(tmp_path, cloud):
    a = make_desk(tmp_path, cloud)
    lid = a.mgr.create_list('L')
    a.mgr.add_item('990001', lid, note='ok', fl_id='FLa')
    cl = cloud.new_list('L')
    cloud.add(cl, '990001', fl_id='FLa', note='Look again at the verso')
    assert a.down()['success']
    assert item(a, '990001::fl::FLa')['note'] == 'ok' + MARK + 'Look again at the verso'


def test_first_upload_does_not_replace_a_short_cloud_note_it_merely_contains(tmp_path, cloud):
    a = make_desk(tmp_path, cloud)
    lid = a.mgr.create_list('L')
    a.mgr.add_item('990001', lid, note='Look again at the verso', fl_id='FLa')
    cl = cloud.new_list('L')
    rid = cloud.add(cl, '990001', fl_id='FLa', note='ok')
    assert a.up()['success']
    assert cloud.row(rid)['note'] == 'ok'


# --------------------------------------------------------------------------- helpers and counting

@BOTH
def test_the_single_item_helpers_touch_only_their_own_folio(tmp_path, has_page):
    c = Cloud(has_page=has_page)
    a = make_desk(tmp_path, c)
    lid = a.mgr.create_list('L')
    a.mgr.add_item('990001', lid, note='recto', fl_id='FLr')
    a.mgr.add_item('990001', lid, note='verso', fl_id='FLv')
    assert a.up()['success']
    recto = rec(a, '990001::fl::FLr', lid)['id']
    verso = rec(a, '990001::fl::FLv', lid)['id']
    a.mgr.update_item('990001::fl::FLv', note='verso, again')
    assert a.sync.sync_item_to_cloud('990001::fl::FLv', lid)
    assert c.row(verso)['note'] == 'verso, again' and c.row(recto)['note'] == 'recto'
    assert a.sync.delete_item_from_cloud('990001::fl::FLv', lid)
    assert c.row(verso) is None and c.row(recto) is not None
    assert not a.sync.delete_item_from_cloud('990001::fl::FLv', lid)   # nothing remembered: nothing deleted
    assert c.row(recto) is not None


@BOTH
def test_an_update_that_matches_no_row_is_not_counted(tmp_path, has_page):
    c = Cloud(has_page=has_page)
    a = make_desk(tmp_path, c)
    lid = a.mgr.create_list('L')
    a.mgr.add_item('990001', lid, note='hi', fl_id='FLa')
    assert a.up()['success']
    (row,) = c.rows(sys_id='990001')
    rid = row['id']
    a.mgr.update_item('990001::fl::FLa', note='hi again')

    def gone_before_the_write(client, req):
        if client.actor == 'A' and req.t == 'list_items' and req.op == 'update':
            c.rec.hook = None
            c.delete_row(rid)
    c.rec.hook = gone_before_the_write
    result = a.up()
    assert result['success'] is False
    assert result['items_pushed'] == 0 and result['items_failed'] == 1


# --------------------------------------------------------------------------- guards

def test_moving_an_item_to_another_list_moves_it_in_the_cloud(tmp_path, cloud):
    a = make_desk(tmp_path, cloud)
    k, l_ = a.mgr.create_list('K'), a.mgr.create_list('L')
    a.mgr.add_item('990001', k, fl_id='FLa')
    assert a.up()['success']
    (row,) = cloud.rows(sys_id='990001')
    rid = row['id']
    a.mgr.move_items_to_list(['990001::fl::FLa'], k, l_)
    assert a.up()['success']
    assert len(cloud.rows(list_id=cloud_id(a, k))) == 0
    assert [r['id'] for r in cloud.rows(list_id=cloud_id(a, l_))] == [rid]
    assert a.down()['success']
    assert item(a, '990001::fl::FLa')['lists'] == [l_]


def test_merging_a_duplicate_list_leaves_no_rows_behind(tmp_path, cloud):
    a = make_desk(tmp_path, cloud)
    keep = a.mgr.create_list('Keep')
    dup = a.mgr.create_list('Dup')
    a.mgr.add_item('990001', dup, fl_id='FLa')
    assert a.up()['success']          # the kept list has its own cloud list, with no row
    dup_cloud = cloud_id(a, dup)
    a.mgr.merge_duplicate_group(keep, [dup])
    assert a.up()['success']
    assert cloud.rows(list_id=dup_cloud) == []
    assert len(cloud.rows(list_id=cloud_id(a, keep))) == 1


def test_a_list_over_one_page_is_read_in_full(tmp_path, cloud):
    a = make_desk(tmp_path, cloud)
    cl = cloud.new_list('Big')
    cloud.web.table('list_items').insert([{'list_id': cl, 'sys_id': '990001', 'fl_id': f'FL{n}', 'note': '',
                                          'tags': []} for n in range(1500)]).execute()
    assert a.down()['success']
    assert sum(1 for it in a.mgr.data['items'].values() if it['sys_id'] == '990001') == 1500
    mark = cloud.rec.mark()
    assert a.up()['success']
    assert mark(op='insert') == []


# --------------------------------------------------------------------------- website removals, recorded only

def _recorded(tmp_path, c, note='n', sys_id='990001', fl='FLa'):
    a = make_desk(tmp_path, c)
    lid = a.mgr.create_list('L')
    a.mgr.add_item(sys_id, lid, note=note, fl_id=fl)
    assert a.up()['success']
    key = f'{sys_id}::fl::{fl}'
    return a, lid, key, rec(a, key, lid)['id']


@pytest.mark.parametrize('direction', ['upload', 'download'])
def test_a_row_removed_on_the_website_is_recorded_not_uploaded_and_nothing_is_deleted(tmp_path, cloud, direction):
    a, lid, key, rid = _recorded(tmp_path, cloud)
    cloud.delete_row(rid)
    sync = a.up if direction == 'upload' else a.down
    mark = cloud.rec.mark()
    result = sync()
    assert result['success'] and result['web_removed'] == [(key, lid)]
    assert mark(op='insert') == [] and mark(op='delete') == []
    assert lid in item(a, key)['lists'] and item(a, key)['note'] == 'n'
    assert saved(a)['items'][key]['cloud_rows'][lid]['gone'] is True
    mark = cloud.rec.mark()
    result = sync()
    assert result['web_removed'] == [] and mark(op='insert') == []
    a.mgr.update_item(key, note='n, edited here')
    mark = cloud.rec.mark()
    assert a.up()['success']
    assert mark(op='insert') == [] and mark(op='update') == [] and mark(op='delete') == []


def _no_count(client, req):
    if client.actor == 'A' and req.t == 'list_items' and req.op == 'select':
        req.count = None


def _raise_on(pred, action='raise_before'):
    def hook(client, req):
        return action if client.actor == 'A' and pred(req) else None
    return hook


def _is_confirmation(req):
    return req.t == 'list_items' and req.op == 'select' and any(k == 'in' for k, _, _ in req.filters)


def _is_list_read(req):
    return req.t == 'list_items' and req.op == 'select' and any(k == 'eq' and col == 'list_id'
                                                                for k, col, _ in req.filters)


def _payloads(reqs):
    return [p for r in reqs for p in (r.payload if isinstance(r.payload, list) else [r.payload])]


CELLS = ['count-none', 'count-changes-between-pages', 'read-error', 'confirmation-error', 'confirmation-capped',
         'found-in-another-list', 'anonymous-read', 'session-lost-mid-pass', 'range-past-end']


@pytest.mark.parametrize('direction', ['upload', 'download'])
@pytest.mark.parametrize('cell', CELLS)
def test_removal_is_never_concluded_from_an_incomplete_read(tmp_path, cell, direction):
    cap = {'count-changes-between-pages': 1, 'range-past-end': 1, 'confirmation-capped': 50}.get(cell)
    c = Cloud(max_rows=cap, past_end_raises=cell == 'range-past-end')
    a = make_desk(tmp_path, c)
    lid = a.mgr.create_list('L')
    n = 80 if cell == 'confirmation-capped' else 3
    for i in range(n):
        a.mgr.add_item(f'99{i:04d}', lid, note=f'note {i}', fl_id='FLa')
    assert a.up()['success']
    keys = sorted(k for k, it in a.mgr.data['items'].items() if lid in it['lists'])
    first = keys[0]
    rid = rec(a, first, lid)['id']
    old_cloud = cloud_id(a, lid)
    if cell in ('confirmation-capped', 'found-in-another-list'):
        moved_to = c.new_list('Elsewhere')
        for k in (keys if cell == 'confirmation-capped' else [first]):
            c.row(rec(a, k, lid)['id'])['list_id'] = moved_to      # another computer moved the rows
        if cell == 'confirmation-capped':
            c.web.table('user_lists').delete().eq('id', old_cloud).execute()
    else:
        c.delete_row(rid)
    if cell in ('count-changes-between-pages', 'range-past-end'):
        a.mgr.add_item('990009', lid, fl_id='FLz')    # a membership with no record: no insert this pass
    sync = a.up if direction == 'upload' else a.down
    pages = []

    def churn(client, req):
        if client.actor == 'A' and _is_list_read(req) and any(v == old_cloud for _, _, v in req.filters):
            pages.append(req.rng)
            if req.rng and req.rng[0] > 0 and len(pages) == 2:
                if cell == 'count-changes-between-pages':
                    c.add(old_cloud, '990008', fl_id='FLq')
                else:
                    for r in c.rows(list_id=old_cloud)[:1]:
                        c.delete_row(r['id'])
        return None

    def lose(client, req):
        if client.actor == 'A' and _is_confirmation(req):
            client.session_user = None

    hooks = {'count-none': _no_count, 'count-changes-between-pages': churn, 'range-past-end': churn,
             'read-error': _raise_on(_is_list_read), 'confirmation-error': _raise_on(_is_confirmation),
             'session-lost-mid-pass': lose}
    c.rec.hook = hooks.get(cell)
    if cell == 'anonymous-read':
        a.client.session_user = None
    mark = c.rec.mark()
    result = sync()
    c.rec.hook = None
    a.client.session_user = 'u1'
    gone = [(k, key) for k, it in a.mgr.data['items'].items() for key, r in (it.get('cloud_rows') or {}).items()
            if r.get('gone')]
    assert gone == [] and result.get('web_removed', []) == []
    if cell == 'read-error' or (cell == 'anonymous-read' and direction == 'upload'):
        # a failed read fails the pass; so does the list step's insert without a session
        assert result['success'] is False and mark(op='insert') == []
        return
    if cell in ('confirmation-capped', 'found-in-another-list'):
        assert result['success'] and result['unchecked'] == 0
        moved = keys if cell == 'confirmation-capped' else [first]
        if direction == 'upload':   # a move made elsewhere arrives as a copy
            assert sorted(p['sys_id'] for p in _payloads(mark(op='insert'))) == \
                sorted(item(a, k)['sys_id'] for k in moved)
        else:
            assert mark(op='insert') == []
        return
    assert result['unchecked'] >= 1
    assert not [p for p in _payloads(mark(op='insert')) if p.get('sys_id') == item(a, first)['sys_id']]
    if cell in ('count-changes-between-pages', 'range-past-end'):
        assert result['success']
        assert not [p for p in _payloads(mark(op='insert')) if p.get('sys_id') == '990009']


@pytest.mark.parametrize('direction', ['upload', 'download', 'whole-cloud-empty'])
def test_a_list_deleted_on_the_website_marks_its_entries(tmp_path, cloud, direction):
    a, lid, key, rid = _recorded(tmp_path, cloud)
    if direction != 'whole-cloud-empty':
        cloud.new_list('Other')
    cloud.web.table('user_lists').delete().eq('id', cloud_id(a, lid)).execute()
    lists_before = copy.deepcopy(a.mgr.data['lists'])
    result = a.up() if direction == 'upload' else a.down()
    assert result['success'] and result['web_removed'] == [(key, lid)]
    assert rec(a, key, lid)['gone'] is True and lid in item(a, key)['lists']
    if direction == 'whole-cloud-empty':
        assert a.mgr.data['lists'] == lists_before


def test_a_web_re_add_clears_the_removal_record(tmp_path, cloud):
    a, lid, key, rid = _recorded(tmp_path, cloud)
    cloud.delete_row(rid)
    assert a.down()['web_removed'] == [(key, lid)]
    new = cloud.add(cloud_id(a, lid), '990001', fl_id='FLa', note='again')
    result = a.down()
    assert result['success'] and result['web_removed'] == []
    assert rec(a, key, lid)['id'] == new and not rec(a, key, lid).get('gone')
    assert item(a, key)['note'] == 'n' + MARK + 'again'


def test_legacy_ids_never_produce_a_removal(tmp_path, cloud):
    a = make_desk(tmp_path, cloud)
    lid = a.mgr.create_list('L')
    a.mgr.add_item('990001', lid, note='n', fl_id='FLa')
    item(a, '990001::fl::FLa')['cloud_id'] = 999999     # left by an older version; that row exists nowhere
    result = a.up()
    assert result['success'] and result['web_removed'] == []
    (row,) = cloud.rows(sys_id='990001')
    assert rec(a, '990001::fl::FLa', lid)['id'] == row['id'] and 'cloud_id' not in item(a, '990001::fl::FLa')


def test_records_of_another_account_are_ignored(tmp_path, cloud):
    a = make_desk(tmp_path, cloud)
    lid = a.mgr.create_list('L')
    a.mgr.add_item('990001', lid, note='n1', fl_id='FLa')
    a.mgr.add_item('990002', lid, note='n2', fl_id='FLb')
    assert a.up()['success'] and a.mgr.data['cloud_account'] == 'u1'
    u1_rows = {r['id'] for r in cloud.rows()}
    a.sync.set_user('u2')
    a.client.session_user = 'u2'
    result = a.up()
    assert result['success'] and result['web_removed'] == []
    assert a.mgr.data['cloud_account'] == 'u2'
    u2_rows = {r['id']: r for r in cloud.rows() if r['id'] not in u1_rows}
    assert sorted(r['sys_id'] for r in u2_rows.values()) == ['990001', '990002']
    assert all(r.get('id') in u2_rows for it in a.mgr.data['items'].values()
               for r in (it.get('cloud_rows') or {}).values())
    a.sync.set_user('u1')
    a.client.session_user = 'u1'
    mark = cloud.rec.mark()
    assert a.up()['success']
    assert mark(op='insert') == []          # the account's rows are found again, by content
    assert a.mgr.data['cloud_account'] == 'u1'


def test_local_items_are_never_uploaded_or_downloaded(tmp_path, cloud, monkeypatch):
    a = make_desk(tmp_path, cloud)
    monkeypatch.setattr(lists_sync, '_sync_instance', a.sync)
    local = '970000000000000123'
    lid = a.mgr.create_list('L')
    a.mgr.add_item(local, lid, note='private', tags=['mine'])
    a.mgr.add_item('990001', lid, fl_id='FLa')
    assert a.mgr.sync_to_cloud()['success']
    sent = json.dumps([(r.payload, r.filters) for r in cloud.rec.reqs if r.actor == 'A'], default=str)
    assert '97000000000' not in sent and cloud.rows(sys_id='990001')
    cl = cloud.new_list('Theirs')
    cloud.add(cl, '970000000000000456', note='from an older desktop')
    assert a.mgr.sync_from_cloud()['success']
    assert not [k for k, it in a.mgr.data['items'].items() if it['sys_id'] == '970000000000000456']


# --------------------------------------------------------------------------- the page column

def _no_page_none(reqs):
    for r in reqs:
        for p in _payloads([r]) if r.payload else []:
            assert 'page' not in p or p['page'] not in (None, ''), r


def _carries_page(r):
    return any('page' in p for p in (_payloads([r]) if r.payload else []))


@pytest.mark.parametrize('cell', ['select', 'batch-insert', 'single-insert', 'update', 'orphan-move',
                                  'page-only-update-found-before', 'page-only-update-found-by-the-write'])
def test_writes_retry_without_page_when_the_schema_cache_lacks_it(tmp_path, cell):
    c = Cloud(has_page=cell not in ('select', 'page-only-update-found-before'))
    a = make_desk(tmp_path, c)
    lid = a.mgr.create_list('L')
    other = a.mgr.create_list('M')
    if cell in ('update', 'orphan-move', 'page-only-update-found-by-the-write', 'page-only-update-found-before'):
        cl = c.new_list('L')
        c.new_list('M')
        r1 = c.add(cl, '990001', fl_id='FLa', note='old')     # rows without a page: an upload fills it
        c.add(cl, '990002', fl_id='FLb')
        a.mgr.add_item('990001', lid, fl_id='FLa', note='old', img='1')
        a.mgr.add_item('990002', lid, fl_id='FLb', img='2')
        c.db.page_lag_writes = 0
        if cell in ('update', 'orphan-move'):
            real = c.db.has_page
            c.db.has_page = False
            assert a.up()['success']          # recorded while the column was missing: rows keep no page
            c.db.has_page = real
            a.mgr.update_item('990001::img::1', note='new')
        if cell == 'orphan-move':
            a.mgr.move_items_to_list(['990001::img::1'], lid, other)
    if cell == 'select':
        a.mgr.add_item('990001', lid, img='1')
    if cell == 'batch-insert':
        a.mgr.add_item('990001', lid, img='1')
        a.mgr.add_item('990002', lid, img='2')
    if cell == 'single-insert':
        a.mgr.add_item('990001', lid, img='1')
    if cell not in ('select', 'page-only-update-found-before'):
        c.db.page_lag_writes = 1              # the column exists, the API's schema cache does not know it yet
    mark = c.rec.mark()
    result = a.up()
    assert result['success'], result
    writes = mark(op='insert') + mark(op='update')
    _no_page_none(writes)
    if cell == 'page-only-update-found-before':
        assert mark(op='update') == []             # the select found it missing: no request for the page
        return
    if cell == 'page-only-update-found-by-the-write':
        page_only = [r for r in mark(op='update') if set(r.payload) == {'page'}]
        assert len(page_only) == 1 and result['items_failed'] == 0   # one failed PATCH, no retry, no failure
        return
    if cell == 'select':
        assert c.rows(sys_id='990001') and all('page' not in r for r in c.rows())
        assert not [r for r in mark(op='select')[1:] if 'page' in r.cols]
        return
    carrying = [n for n, r in enumerate(writes) if _carries_page(r)]
    assert len(carrying) == 1, writes              # the write that met it; retried once without page
    assert not [r for r in writes[carrying[0] + 1:] if _carries_page(r)]
    if cell in ('batch-insert', 'single-insert'):
        assert sorted(r['sys_id'] for r in c.rows()) == (['990001', '990002'] if cell == 'batch-insert'
                                                          else ['990001'])
    if cell == 'update':
        assert c.row(r1)['note'] == 'new'
    if cell == 'orphan-move':
        assert c.row(r1)['list_id'] == cloud_id(a, other)


def test_page_backfill_is_capped_per_pass(tmp_path, cloud):
    a = make_desk(tmp_path, cloud)
    lid = a.mgr.create_list('L')
    cl = cloud.new_list('L')
    cloud.web.table('list_items').insert([{'list_id': cl, 'sys_id': f'99{n:05d}', 'fl_id': 'FLa', 'note': '',
                                          'tags': []} for n in range(300)]).execute()
    for n in range(300):
        a.mgr.data['items'][f'99{n:05d}::img::1'] = {'sys_id': f'99{n:05d}', 'lists': [lid], 'tags': [], 'note': '',
                                                    'fl_id': 'FLa', 'img': '1', 'added': 1, 'modified': 1}
    counts = []
    for _ in range(3):
        mark = cloud.rec.mark()
        assert a.up()['success']
        counts.append(len([r for r in mark(op='update') if set(r.payload) == {'page'}]))
    assert counts == [200, 100, 0]


# --------------------------------------------------------------------------- merges and counts

@pytest.mark.parametrize('lang', ['en', 'he'])
def test_repeated_merges_add_nothing(tmp_path, cloud, monkeypatch, lang):
    import genizah_core
    from shared import genizah_translations
    marker = 'מהענן'
    monkeypatch.setattr(genizah_core, 'CURRENT_LANG', lang)
    monkeypatch.setitem(genizah_translations.TRANSLATIONS, 'from the cloud', marker)
    label = 'from the cloud' if lang == 'en' else marker
    a = make_desk(tmp_path, cloud)
    lid = a.mgr.create_list('L')
    a.mgr.add_item('990001', lid, note='mine', fl_id='FLa')
    rid = cloud.add(cloud.new_list('L'), '990001', fl_id='FLa', note='theirs')
    expected = f'mine\n\n--- {label} ---\ntheirs'
    for _ in range(3):
        merge(a)
        assert item(a, '990001::fl::FLa')['note'] == expected
        assert cloud.row(rid)['note'] == expected


def test_notes_kept_and_merged_are_counted(tmp_path, cloud):
    a = make_desk(tmp_path, cloud)
    k, l_ = a.mgr.create_list('K'), a.mgr.create_list('L')
    a.mgr.add_item('990001', k, note='local', fl_id='FLa')
    a.mgr.add_item('990001', l_, fl_id='FLa')
    ck, cl = cloud.new_list('K'), cloud.new_list('L')
    cloud.add(ck, '990001', fl_id='FLa', note='k text')
    cloud.add(cl, '990001', fl_id='FLa', note='l text')
    result = a.up()
    assert result['notes_kept'] == 2 and result['notes_differing'] == 2      # per membership
    assert a.mgr.differing_notes_count() == 2
    result = a.down()
    assert result['notes_merged'] == 1                                        # per entry
    assert item(a, '990001::fl::FLa')['note'] == 'local' + MARK + 'k text' + MARK + 'l text'
    assert result['notes_differing'] == 0 and a.mgr.differing_notes_count() == 0


# --------------------------------------------------------------------------- moves and removals here

@pytest.mark.parametrize('how', ['move', 'duplicate-merge'])
def test_moving_into_a_list_that_already_holds_the_entry_leaves_the_old_row_and_never_re_adds_it(tmp_path, cloud,
                                                                                                    how):
    a = make_desk(tmp_path, cloud)
    names = ('K', 'L') if how == 'move' else ('Dup', 'Dup')
    k, l_ = a.mgr.create_list(names[0]), a.mgr.create_list(names[1])
    key = '990001::fl::FLa'
    a.mgr.add_item('990001', k, note='base', fl_id='FLa')
    a.mgr.add_item('990001', l_, fl_id='FLa')
    assert a.up()['success']
    rk, rl = rec(a, key, k)['id'], rec(a, key, l_)['id']
    a.mgr.update_item(key, note='local')
    cloud.set(rk, note='k web')
    cloud.set(rl, note='l web')
    if how == 'move':
        a.mgr.move_items_to_list([key], k, l_)
    else:
        a.mgr.merge_duplicate_group(l_, [k])
    mark = cloud.rec.mark()
    assert a.up()['success']
    assert mark(op='delete') == []
    assert not [r for r in mark(op='update') if any(col == 'id' and v == rk for _, col, v in r.filters)]
    assert a.down()['success']
    assert item(a, key)['lists'] == [l_]
    assert item(a, key)['note'] == 'local' + MARK + 'k web' + MARK + 'l web'
    assert cloud.row(rk) is not None


@pytest.mark.parametrize('order', ['kept-id-lower', 'kept-id-higher'])
def test_a_merge_into_a_trashed_list_moves_the_row_after_restore(tmp_path, cloud, order):
    a = make_desk(tmp_path, cloud)
    key = '990001::fl::FLa'

    def make_k():
        k = a.mgr.create_list('Foo')
        a.mgr.add_item('990001', k, note='n', fl_id='FLa')
        assert a.up()['success']
        return k

    def make_l():
        lst = a.mgr.create_list('Foo')
        assert a.up()['success']
        a.mgr.delete_list(lst)
        assert a.up()['success']
        return lst
    if order == 'kept-id-lower':
        l_ = make_l()
        k = make_k()
    else:
        k = make_k()
        l_ = make_l()
    assert (cloud_id(a, l_) < cloud_id(a, k)) == (order == 'kept-id-lower')
    l_cloud, k_cloud, rid = cloud_id(a, l_), cloud_id(a, k), rec(a, key, k)['id']
    a.mgr.merge_duplicate_group(l_, [k])      # keeps the trashed list, as Fix Duplicates may
    mark = cloud.rec.mark()
    assert a.up()['success']
    assert mark(op='update') == [] and mark(op='insert') == [] and mark(op='delete') == []
    result = a.down()
    assert result['success'] and result['items_added'] == 0
    assert item(a, key)['lists'] == [l_]
    assert a.mgr.data['lists'][l_].get('deleted_at') and cloud_id(a, l_) == l_cloud
    a.mgr.restore_list(l_)
    mark = cloud.rec.mark()
    assert a.up()['success']
    moves = mark(op='update')
    assert len(moves) == 1 and moves[0].payload == {'list_id': l_cloud}
    assert ('eq', 'id', rid) in moves[0].filters and ('eq', 'list_id', k_cloud) in moves[0].filters
    assert mark(op='insert') == [] and mark(op='delete') == []


@pytest.mark.parametrize('cell', ['remove-one-of-two', 'remove-last', 'delete-list-permanently', 'empty-trash',
                                  'move-then-remove'])
def test_2b1_sends_no_delete_for_a_desktop_removal(tmp_path, cloud, cell):
    a = make_desk(tmp_path, cloud)
    k, l_, m = a.mgr.create_list('K'), a.mgr.create_list('L'), a.mgr.create_list('M')
    key = '990001::fl::FLa'
    a.mgr.add_item('990001', k, note='n', fl_id='FLa')
    if cell in ('remove-one-of-two', 'move-then-remove'):
        a.mgr.add_item('990001', l_, fl_id='FLa')
    assert a.up()['success']
    rows_before = {r['id'] for r in cloud.rows()}
    assert len(rows_before) == len(item(a, key)['lists'])     # a row in each of its lists
    if cell in ('remove-one-of-two', 'remove-last'):
        a.mgr.remove_item_from_list(key, k)
    elif cell == 'delete-list-permanently':
        a.mgr.delete_list(k, permanent=True)
    elif cell == 'empty-trash':
        a.mgr.delete_list(k)
        a.mgr.empty_trash()
    else:
        a.mgr.move_items_to_list([key], k, m)
        a.mgr.remove_item_from_list(key, m)
    mark = cloud.rec.mark()
    assert a.up()['success']
    assert mark(op='delete') == [] and {r['id'] for r in cloud.rows()} >= rows_before
    assert a.down()['success']
    lists_named = {ld['name']: lid for lid, ld in a.mgr.data['lists'].items()}
    if cell in ('remove-one-of-two', 'move-then-remove'):
        assert item(a, key)['lists'] == [l_]          # not re-added where it was removed
    else:
        assert lists_named['K'] in item(a, key)['lists']   # its row brings it back, as before


@pytest.mark.parametrize('cell', ['chained-before-upload', 'moved-again-during-the-move'])
def test_moves_end_with_one_row_in_the_final_list(tmp_path, cloud, cell):
    a = make_desk(tmp_path, cloud)
    k, l_, n = a.mgr.create_list('K'), a.mgr.create_list('L'), a.mgr.create_list('N')
    key = '990001::fl::FLa'
    a.mgr.add_item('990001', k, fl_id='FLa')
    assert a.up()['success']
    k_cloud, rid = cloud_id(a, k), rec(a, key, k)['id']
    mark = cloud.rec.mark()
    if cell == 'chained-before-upload':
        a.mgr.move_items_to_list([key], k, l_)
        a.mgr.move_items_to_list([key], l_, n)
        assert a.up()['success']
        moves = mark(op='update')
        assert len(moves) == 1 and moves[0].payload == {'list_id': cloud_id(a, n)}
        assert ('eq', 'list_id', k_cloud) in moves[0].filters
    else:
        a.mgr.move_items_to_list([key], k, l_)

        def move_again(client, req):
            if client.actor == 'A' and req.t == 'list_items' and req.op == 'update':
                cloud.rec.hook = None
                a.mgr.move_items_to_list([key], l_, n)
        cloud.rec.hook = move_again
        a.up()
        assert a.up()['success']
    assert mark(op='insert') == []
    assert [(r['id'], r['list_id']) for r in cloud.rows(sys_id='990001')] == [(rid, cloud_id(a, n))]


# --------------------------------------------------------------------------- writes that must not overwrite or duplicate

@pytest.mark.parametrize('cell', ['long-note', 'long-tags', 'over-gateway-limit'])
def test_a_website_edit_between_read_and_write_is_not_overwritten(tmp_path, cloud, cell):
    a = make_desk(tmp_path, cloud)
    lid = a.mgr.create_list('L')
    key = '990001::fl::FLa'
    note = {'long-note': 'x' * 7000, 'over-gateway-limit': 'y' * 9000}.get(cell, 'short')
    tags = [str(n) for n in range(300)] if cell == 'long-tags' else []
    a.mgr.add_item('990001', lid, note=note, tags=tags, fl_id='FLa')
    assert a.up()['success']
    rid = rec(a, key, lid)['id']
    record_before = copy.deepcopy(rec(a, key, lid))
    if cell == 'long-tags':
        a.mgr.update_item(key, tags=tags + ['new'])
    else:
        a.mgr.update_item(key, note=note + '\nmore')

    def website_edits_first(client, req):
        if client.actor == 'A' and req.t == 'list_items' and req.op == 'update':
            cloud.rec.hook = None
            cloud.set(rid, **({'tags': ['website']} if cell == 'long-tags' else {'note': 'website'}))
    if cell != 'over-gateway-limit':
        cloud.rec.hook = website_edits_first
    mark = cloud.rec.mark()
    result = a.up()
    field = 'tags' if cell == 'long-tags' else 'note'
    for r in mark(op='update'):
        if field in (r.payload or {}):
            if field == 'note':
                assert ('eq', 'note', note) in r.filters             # the full value, whatever its length
            else:
                assert ('cs', 'tags', json.dumps(tags, separators=(',', ':'))) in r.filters
    if cell == 'over-gateway-limit':
        assert result['notes_too_long'] == 1 and result['items_failed'] == 0
        assert cloud.row(rid)['note'] == note and rec(a, key, lid) == record_before
        return
    assert result['notes_kept'] == 1 and rec(a, key, lid).get('differs') is True
    assert cloud.row(rid)[field] == (['website'] if cell == 'long-tags' else 'website')
    assert rec(a, key, lid).get(field) == record_before.get(field)


@pytest.mark.parametrize('cell', ['sqlstate-23502', 'pgrst204'])
def test_an_ambiguous_batch_failure_makes_no_duplicate_rows(tmp_path, cell):
    c = Cloud()
    a = make_desk(tmp_path, c)
    lid = a.mgr.create_list('L')
    for sys_id in ('990001', '990002', '990003'):
        a.mgr.add_item(sys_id, lid, img='1')
    if cell == 'sqlstate-23502':
        def reject(client, req):
            if client.actor != 'A' or req.t != 'list_items' or req.op != 'insert':
                return None
            if isinstance(req.payload, list) or req.payload.get('sys_id') == '990002':
                return 'api_23502'          # refused by the database: nothing was written
            return None
        c.rec.hook = reject
    else:
        c.db.page_lag_writes = 1
    result = a.up()
    c.rec.hook = None
    got = sorted(r['sys_id'] for r in c.rows())
    if cell == 'sqlstate-23502':
        assert got == ['990001', '990003'] and result['items_failed'] == 1   # the bad row isolated
    else:
        assert got == ['990001', '990002', '990003'] and result['success']   # retried once without page
    assert a.up()['success'] or cell == 'sqlstate-23502'
    assert len(c.rows()) == len(set(r['sys_id'] for r in c.rows()))


# --------------------------------------------------------------------------- accounts, a row only the confirmation found, differing notes, tags

@pytest.mark.parametrize('cell', ['write-fails-after-records', 'exception-after-records', 'session-user-differs'])
def test_the_account_is_recorded_before_the_first_record(tmp_path, cloud, cell, monkeypatch):
    a = make_desk(tmp_path, cloud)
    l1, l2 = a.mgr.create_list('L1'), a.mgr.create_list('L2')
    a.mgr.add_item('990001', l1, fl_id='FLa')
    a.mgr.add_item('990002', l2, fl_id='FLb')
    a.mgr.add_item('990003', l2, fl_id='FLc')
    if cell == 'session-user-differs':
        a.client.session_user = 'u2'
        mark = cloud.rec.mark()
        result = a.up()
        assert result == {'success': False, 'error': 'Sync not available'}
        assert [r for r in cloud.rec.reqs[len(cloud.rec.reqs) - 0:]] == [] and mark() == [] \
            and mark(table='user_lists') == [] and mark(table='projects') == []
        assert not any(it.get('cloud_rows') for it in a.mgr.data['items'].values())
        return
    if cell == 'write-fails-after-records':
        def fail_l2(client, req):
            if client.actor == 'A' and req.t == 'list_items' and req.op == 'insert' and isinstance(req.payload, list):
                return 'raise_before'
            return None
        cloud.rec.hook = fail_l2
    else:
        real = lists_sync.ListsCloudSync._write_list
        calls = []

        def second_call_raises(self, *args, **kw):
            calls.append(1)
            if len(calls) == 2:
                raise RuntimeError('the pass stopped here')
            return real(self, *args, **kw)
        monkeypatch.setattr(lists_sync.ListsCloudSync, '_write_list', second_call_raises)
    result = a.up()
    cloud.rec.hook = None
    monkeypatch.undo()
    monkeypatch.setattr(lists_sync, 'SUPABASE_AVAILABLE', True)
    monkeypatch.setattr(lists_sync, 'SUPABASE_ANON_KEY', 'test-key')
    assert result['success'] is False
    assert a.mgr.data['cloud_account'] == 'u1'
    assert rec(a, '990001::fl::FLa', l1) is not None             # L1's rows were recorded
    a.mgr.save()
    a.sync.set_user('u2')
    a.client.session_user = 'u2'
    result = a.up()
    assert result['success'] and result['web_removed'] == []
    u2_rows = [r for r in cloud.rows() if cloud.db.owner('list_items', r) == 'u2']
    assert sorted(r['sys_id'] for r in u2_rows) == ['990001', '990002', '990003']


@pytest.mark.parametrize('cell', ['cloud-only-changed', 'both-changed'])
def test_a_download_applies_a_remembered_row_that_only_the_confirmation_found(tmp_path, cell):
    c = Cloud(max_rows=50)
    a = make_desk(tmp_path, c)
    lid = a.mgr.create_list('L')
    for n in range(80):
        a.mgr.add_item(f'99{n:04d}', lid, note=f'n{n}', fl_id='FLa')
    assert a.up()['success']
    rows = c.rows(list_id=cloud_id(a, lid))
    target, victim = rows[50], rows[5]
    key = next(k for k, it in a.mgr.data['items'].items() if rec(a, k, lid)['id'] == target['id'])
    c.set(target['id'], note='website')
    if cell == 'both-changed':
        a.mgr.update_item(key, note='mine')
    pages = []

    def delete_between_pages(client, req):
        if client.actor == 'A' and _is_list_read(req):
            pages.append(req.rng)
            if req.rng and req.rng[0] > 0:
                c.rec.hook = None
                c.delete_row(victim['id'])
    c.rec.hook = delete_between_pages
    items_before = set(a.mgr.data['items'])
    result = a.down()
    assert result['success'] and result['web_removed'] == []
    assert item(a, key)['note'] == ('website' if cell == 'cloud-only-changed' else 'mine' + MARK + 'website')
    assert set(a.mgr.data['items']) == items_before
    # position 5's row was read on page 1, before the website deleted it: paired, neither removed nor unchecked
    victim_key = next(k for k, it in a.mgr.data['items'].items() if (rec(a, k, lid) or {}).get('id') == victim['id'])
    assert not rec(a, victim_key, lid).get('gone') and result['unchecked'] == 0


def test_a_differing_note_stays_known_until_a_pass_reaches_it(tmp_path):
    c = Cloud(max_rows=1)
    a = make_desk(tmp_path, c)
    lid = a.mgr.create_list('L')
    cl = c.new_list('L')
    other = c.add(cl, '990009', fl_id='FLz')              # a website-only row read before the entry's
    rid = c.add(cl, '990001', fl_id='FLa', note='cloud')
    a.mgr.add_item('990001', lid, note='mine', fl_id='FLa')
    result = a.up()
    assert result['notes_kept'] == 1 and result['notes_differing'] == 1 and result['complete'] is True
    backup = copy.deepcopy(a.mgr.data)

    def skip_it(client, req):
        if client.actor == 'A' and _is_list_read(req) and req.rng and req.rng[0] > 0:
            c.delete_row(other)                           # the entry's row slips past the paging
        if client.actor == 'A' and _is_confirmation(req):
            c.rec.hook = None
            return 'raise_before'
        return None
    c.rec.hook = skip_it
    result = a.up()
    assert result['success'] is True and result['notes_kept'] == 0 and result['unchecked'] == 1
    assert result['complete'] is False
    assert result['notes_differing'] == 1 and a.mgr.differing_notes_count() == 1
    c.set(rid, note='mine')
    result = a.up()
    assert result['notes_differing'] == 0 and a.mgr.differing_notes_count() == 0
    a.mgr.data = backup                                   # the other branch: a Download that keeps both
    c.set(rid, note='cloud again')
    result = a.down()
    assert result['notes_differing'] == 0 and a.mgr.differing_notes_count() == 0


def test_tag_filters_are_json_for_the_jsonb_tags_column(tmp_path, cloud):
    a = make_desk(tmp_path, cloud)
    k, l_ = a.mgr.create_list('K'), a.mgr.create_list('L')
    odd = ['a,b', 'say "x"', '{brace}', 'two words', 'עברית']
    a.mgr.add_item('990001', k, tags=list(odd), fl_id='FLa')
    a.mgr.add_item('990002', k, tags=['a', 'b'], fl_id='FLb')
    assert a.up()['success']
    a.mgr.update_item('990001::fl::FLa', tags=odd + ['new'])
    a.mgr.update_item('990002::fl::FLb', tags=['a', 'b', 'c'])
    a.mgr.move_items_to_list(['990002::fl::FLb'], k, l_)
    mark = cloud.rec.mark()
    assert a.up()['success']
    writes = {r.payload.get('list_id', 'update'): r for r in mark(op='update') if 'tags' in r.payload}
    plain = next(r for r in mark(op='update') if r.payload.get('tags') == odd + ['new'])
    lit = json.dumps(odd, ensure_ascii=False, separators=(',', ':'))
    assert ('cs', 'tags', lit) in plain.filters and ('cd', 'tags', lit) in plain.filters
    move = next(r for r in mark(op='update') if 'list_id' in r.payload)
    assert ('cs', 'tags', '["a","b"]') in move.filters and ('cd', 'tags', '["a","b"]') in move.filters
    assert writes
    assert cloud.rows(sys_id='990001')[0]['tags'] == odd + ['new']
    assert cloud.rows(sys_id='990002')[0]['tags'] == ['a', 'b', 'c']


# --------------------------------------------------------------------------- one pass at a time

def test_a_second_pass_while_one_runs_sends_nothing(tmp_path, cloud):
    a = make_desk(tmp_path, cloud)
    a.mgr.add_item('990001', 'default', fl_id='FLa')
    entered, release = threading.Event(), threading.Event()
    snapshots = []
    real = a.mgr.write_snapshot

    def held_snapshot(label):
        snapshots.append(label)
        if len(snapshots) == 1:
            entered.set()
            release.wait(10)
        return real(label)
    a.mgr.write_snapshot = held_snapshot
    first = threading.Thread(target=a.up)
    first.start()
    assert entered.wait(10)
    before = len(cloud.rec.reqs)
    assert a.up() == {'success': False, 'error': 'Sync already in progress'}
    assert a.down() == {'success': False, 'error': 'Sync already in progress'}
    assert snapshots == ['pre-upload'] and len(cloud.rec.reqs) == before
    release.set()
    first.join(10)
    assert cloud.rows(sys_id='990001')


# --------------------------------------------------------------------------- lost responses, identity, Trash state, remapped lists, reconcile

@pytest.mark.parametrize('cell', ['cloud-only-changed', 'both-changed'])
def test_a_download_after_a_lost_move_response_applies_the_website_edit(tmp_path, cloud, cell):
    a = make_desk(tmp_path, cloud)
    k, l_ = a.mgr.create_list('K'), a.mgr.create_list('L')
    key = '990001::fl::FLa'
    a.mgr.add_item('990001', k, note='a', fl_id='FLa')
    assert a.up()['success']
    rid = rec(a, key, k)['id']
    a.mgr.move_items_to_list([key], k, l_)

    def lose_the_response(client, req):
        if client.actor == 'A' and req.t == 'list_items' and req.op == 'update':
            cloud.rec.hook = None
            return 'raise_after'
        return None
    cloud.rec.hook = lose_the_response
    assert a.up()['success'] is False
    assert cloud.row(rid)['list_id'] == cloud_id(a, l_) and rec(a, key, k)['id'] == rid
    cloud.set(rid, note='b')
    if cell == 'both-changed':
        a.mgr.update_item(key, note='c')
    items_before = set(a.mgr.data['items'])
    assert a.down()['success']
    expected = 'b' if cell == 'cloud-only-changed' else 'c' + MARK + 'b'
    assert item(a, key)['note'] == expected and item(a, key)['lists'] == [l_]
    assert set(a.mgr.data['items']) == items_before and rec(a, key, k)['note'] == 'b'
    mark = cloud.rec.mark()
    assert a.up()['success']
    assert rec(a, key, l_)['id'] == rid and rec(a, key, l_)['list'] == cloud_id(a, l_)
    assert mark(op='insert') == [] and not [r for r in mark(op='update') if 'list_id' in r.payload]
    if cell == 'cloud-only-changed':
        assert mark(op='update') == [] and rec(a, key, l_)['note'] == 'b'
    else:
        (patch,) = mark(op='update')
        assert patch.payload == {'note': expected} and ('eq', 'note', 'b') in patch.filters
        assert rec(a, key, l_)['note'] == expected


@pytest.mark.parametrize('cell', ['whole-row-beside-page', 'same-page-other-folio', 'same-page-one-folio-unknown',
                                  'recorded-row-other-folio'])
def test_identity_is_never_bent_to_pair_a_row(tmp_path, cloud, cell):
    a = make_desk(tmp_path, cloud)
    if cell == 'whole-row-beside-page':
        a.mgr.add_item('990003', 'default', note='n1', img='2')
        general = cloud.new_list('General')
        web_row = cloud.add(general, '990003', note='n3')
        mark = cloud.rec.mark()
        assert a.up()['success']
        inserted = _payloads(mark(op='insert'))
        assert [(p['sys_id'], p.get('page')) for p in inserted] == [('990003', '2')]
        assert mark(op='update') == [] and cloud.row(web_row).get('page') is None
        assert a.down()['success']
        assert item(a, '990003')['note'] == 'n3' and item(a, '990003::img::2')['note'] == 'n1'
    elif cell == 'same-page-other-folio':
        lid = a.mgr.create_list('A')
        a.mgr.add_item('990001', lid, note='n1', fl_id='FL1', img='1')
        assert a.up()['success']
        cloud.add(cloud_id(a, lid), '990001', fl_id='FL2', note='n2', page='1')
        assert a.down()['success']
        assert item(a, '990001::fl::FL2')['note'] == 'n2' and item(a, '990001::img::1')['note'] == 'n1'
    elif cell == 'same-page-one-folio-unknown':
        lid = a.mgr.create_list('A')
        a.mgr.add_item('990001', lid, fl_id='FL1', img='1')
        row = cloud.add(cloud.new_list('A'), '990001', page='1')
        mark = cloud.rec.mark()
        assert a.up()['success']
        assert mark(op='insert') == [] and cloud.row(row)['fl_id'] == 'FL1'
    else:
        lid = a.mgr.create_list('A')
        a.mgr.add_item('990001', lid, fl_id='FLa', img='1')
        assert a.up()['success']
        rid = rec(a, '990001::img::1', lid)['id']
        a.mgr.add_item('990001', 'default', fl_id='FLb', img='1')     # add_item overwrites fl_id on an existing key
        mark = cloud.rec.mark()
        assert a.up()['success']
        assert not [r for r in mark(op='update') if 'fl_id' in r.payload] and cloud.row(rid)['fl_id'] == 'FLa'


@pytest.mark.parametrize('method', ['merge_duplicate_group', 'auto_merge_duplicate_group'])
@pytest.mark.parametrize('state', ['trashed', 'live'])
def test_a_kept_list_that_had_no_cloud_id_keeps_its_trash_state(tmp_path, state, method):
    ends = {}
    for order in ('download-first', 'upload-first'):
        c = Cloud()
        (tmp_path / order).mkdir()
        a = make_desk(tmp_path / order, c)
        trashed_at = '2026-09-27T00:00:00+00:00' if state == 'live' else None
        five = c.new_list('Foo', deleted_at=trashed_at)
        c.new_list('Foo', deleted_at=trashed_at)
        k = a.mgr.create_list('Foo')
        a.mgr.data['lists'][k]['cloud_id'] = five
        l_ = a.mgr.create_list('Foo')
        if state == 'trashed':
            a.mgr.delete_list(l_)
        was_trashed = bool(a.mgr.data['lists'][l_].get('deleted_at'))
        if method == 'merge_duplicate_group':
            a.mgr.merge_duplicate_group(l_, [k])
        else:
            group = next(g for g in a.mgr.find_duplicate_lists() if g['name'] == 'Foo')
            assert a.mgr.auto_merge_duplicate_group(group)['keep_id'] == l_   # it prefers the list without an id
        passes = [a.down, a.up] if order == 'download-first' else [a.up, a.down]
        for sync in passes + passes:
            assert sync()['success']
            ld = a.mgr.data['lists'][l_]
            assert bool(ld.get('deleted_at')) == was_trashed and ld['cloud_id'] == five
            if sync == a.up:
                assert bool(c.db.list_by_id(five).get('deleted_at')) == was_trashed
        ends[order] = ({key: v for key, v in a.mgr.data['lists'][l_].items() if key not in ('created', 'deleted_at')},
                       [(r['id'], r['name'], bool(r.get('deleted_at'))) for r in c.db.tables['user_lists']],
                       c.rows())
    assert ends['download-first'] == ends['upload-first']


@pytest.mark.parametrize('cell', ['remapped-list-upload', 'remapped-list-download', 'two-local-lists-one-name'])
def test_a_record_for_a_list_that_is_no_longer_its_own_is_dropped(tmp_path, cloud, cell):
    a = make_desk(tmp_path, cloud)
    if cell.startswith('remapped-list'):
        lid = a.mgr.create_list('L')
        key = '990001::fl::FLa'
        a.mgr.add_item('990001', lid, fl_id='FLa')
        assert a.up()['success']
        five, rid = cloud_id(a, lid), rec(a, key, lid)['id']
        cloud.web.table('user_lists').update({'name': 'Renamed'}).eq('id', five).execute()
        cloud.delete_row(rid)
        nine = cloud.new_list('L')
        if cell == 'remapped-list-upload':
            a.mgr.data['lists'][lid]['cloud_id'] = nine     # L took 9 as its own in an earlier Download
            mark = cloud.rec.mark()
            result = a.up()
            assert result['success'] and result['unchecked'] == 0 and result['web_removed'] == []
            assert [p['list_id'] for p in _payloads(mark(op='insert'))] == [nine]
            mark = cloud.rec.mark()
            result = a.up()
            assert result['unchecked'] == 0 and mark(op='insert') == [] and mark(op='update') == []
        else:
            result = a.down()
            assert result['success'] and result['unchecked'] == 0 and result['web_removed'] == []
            assert cloud_id(a, lid) == nine and rec(a, key, lid) is None
        return
    la, lb = a.mgr.create_list('Foo'), a.mgr.create_list('Foo')
    five = cloud.new_list('Foo')
    a.mgr.data['lists'][la]['cloud_id'] = five            # both held one cloud list after a pre-2b upload
    a.mgr.data['lists'][lb]['cloud_id'] = five
    a.mgr.add_item('990001', la, fl_id='FLx')
    a.mgr.add_item('990002', lb, fl_id='FLy')
    cloud.add(five, '990001', fl_id='FLx')
    cloud.add(five, '990002', fl_id='FLy')
    mark = cloud.rec.mark()
    assert a.up()['success']
    assert cloud_id(a, la) == five and cloud_id(a, lb) != five
    assert [(p['sys_id'], p['list_id']) for p in _payloads(mark(op='insert'))] == [('990002', cloud_id(a, lb))]
    assert mark(op='delete') == []
    result = a.down()
    assert result['success'] and result['unchecked'] == 0
    assert set(item(a, '990002::fl::FLy')['lists']) == {la, lb} and la in item(a, '990001::fl::FLx')['lists']
    assert rec(a, '990002::fl::FLy', lb)['list'] == cloud_id(a, lb)
    state = copy.deepcopy(a.mgr.data['items'])
    mark = cloud.rec.mark()
    assert a.up()['success'] and a.down()['success']
    assert mark(op='insert') == [] and mark(op='update') == [] and a.mgr.data['items'] == state


@pytest.mark.parametrize('shape', ['two-memberships', 'orphan-and-destination'])
@pytest.mark.parametrize('field', ['note', 'tags'])
@pytest.mark.parametrize('first', ['K-first', 'L-first'])
def test_a_download_reconciles_every_source_of_an_entry_at_once(tmp_path, cloud, first, field, shape):
    def val(x):
        return x if field == 'note' else [x]
    a = make_desk(tmp_path, cloud)
    k, l_ = a.mgr.create_list('K'), a.mgr.create_list('L')
    key = '990001::fl::FLa'
    a.mgr.add_item('990001', k, fl_id='FLa')
    a.mgr.add_item('990001', l_, fl_id='FLa')
    a.mgr.update_item(key, **{field: val('b')})
    assert a.up()['success']
    rk, rl = rec(a, key, k)['id'], rec(a, key, l_)['id']
    a.mgr.update_item(key, **{field: val('a')})

    def fail_l(client, req):
        if client.actor == 'A' and req.t == 'list_items' and req.op == 'update' and ('eq', 'id', rl) in req.filters:
            return 'raise_before'
        return None
    cloud.rec.hook = fail_l
    assert a.up()['success'] is False
    cloud.rec.hook = None
    assert rec(a, key, k)[field] == val('a') and rec(a, key, l_)[field] == val('b')
    cloud.set(rk, **{field: val('b')})          # pasted back onto K's row
    cloud.set(rl, **{field: val('c')})
    if shape == 'orphan-and-destination':
        a.mgr.move_items_to_list([key], k, l_)
    a.mgr.data['lists_order'] = [k, l_] if first == 'K-first' else [l_, k]
    result = a.down()
    expected = 'b' + MARK + 'c' if field == 'note' else ['b', 'c']
    assert item(a, key)[field] == expected and result['notes_merged'] == 1
    assert (rec(a, key, k)[field], rec(a, key, l_)[field]) == (val('b'), val('c'))
    mark = cloud.rec.mark()
    assert a.up()['success']
    patched = {next(v for _, col, v in r.filters if col == 'id'): r for r in mark(op='update')}
    if shape == 'two-memberships':
        assert set(patched) == {rk, rl}
    else:
        assert set(patched) == {rl} and cloud.row(rk)[field] == val('b')
    assert cloud.row(rl)[field] == expected
    mark = cloud.rec.mark()
    merge(a)
    assert mark(op='update') == [] and item(a, key)[field] == expected


def test_the_harness_and_the_engine_share_one_identity_predicate():
    values = (None, 'FLa', 'FLb')
    pages = (None, '1', '2')
    for has_page in (True, False):
        for fa in values:
            for pa in pages:
                for fb in values:
                    for pb in pages:
                        a, b = ('990001', fa, pa), ('990001', fb, pb)
                        assert S.same_entry(a, b, has_page) == lists_sync._same_entry(a, b, has_page), (a, b, has_page)
    assert not lists_sync._same_entry(('990001', None, None), ('990002', None, None), True)
