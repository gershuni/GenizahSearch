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
import time
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
        """A row as the website adds it, with the catalogue's shelfmark and title (with page=, as a desktop wrote it)."""
        row = self.web.table('list_items').insert({'list_id': list_id, 'sys_id': sys_id,
                                                   'shelfmark': S.web_shelfmark(sys_id), 'title': S.web_title(sys_id),
                                                   'fl_id': fl_id, 'note': note,
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


def row_in(c, d, key, list_id):
    """The id of the cloud row of an entry in a list, found in the cloud (not through a record)."""
    it = item(d, key)
    fl = it.get('fl_id')
    return next(r['id'] for r in c.rows(list_id=cloud_id(d, list_id), sys_id=it['sys_id'])
                if r.get('fl_id') == fl)


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


def test_a_list_over_one_page_is_read_in_full(tmp_path):
    cloud = Cloud(max_rows=1000)      # the server answers at most 1000 rows a request
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
    return a, lid, key, row_in(c, a, key, lid)


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


def _raise_on(pred, action='raise_before'):
    def hook(client, req):
        return action if client.actor == 'A' and pred(req) else None
    return hook


def _is_confirmation(req):
    return req.t == 'list_items' and req.op == 'select' and any(k == 'in' for k, _, _ in req.filters)


def _is_list_read(req):
    return req.t == 'list_items' and req.op == 'select' and any(k == 'eq' and col == 'list_id'
                                                                for k, col, _ in req.filters)


def _is_later_page(req, table='list_items'):
    """A select continuing a paged read: past offset 0, or after a row id (keyset)."""
    return req.t == table and req.op == 'select' and (
        bool(req.rng and req.rng[0] > 0) or any(k == 'gt' and col == 'id' for k, col, _ in req.filters))


def _as_the_first_page(req):
    """The server ignores what makes this request a later page and answers the first page again."""
    req.filters[:] = [f for f in req.filters if not (f[0] == 'gt' and f[1] == 'id')]
    req.rng = None


def _payloads(reqs):
    return [p for r in reqs for p in (r.payload if isinstance(r.payload, list) else [r.payload])]


# Reads are paged by row id (keyset), so a read is incomplete only when a page does not rise from the last
# id -- 'filter-ignored' -- or when it fails. (Paged by offset they were also taken as incomplete when a
# page carried no count or a different one, or a range ran past the end after rows went; a row the website
# adds between pages now leaves the read complete: see the test after this one.)
CELLS = ['filter-ignored', 'read-error', 'later-page-error', 'confirmation-error', 'confirmation-capped',
         'found-in-another-list', 'anonymous-read', 'session-lost-mid-pass']


@pytest.mark.parametrize('direction', ['upload', 'download'])
@pytest.mark.parametrize('cell', CELLS)
def test_removal_is_never_concluded_from_an_incomplete_read(tmp_path, cell, direction):
    cap = {'filter-ignored': 1, 'later-page-error': 1, 'confirmation-capped': 50}.get(cell)
    c = Cloud(max_rows=cap)
    a = make_desk(tmp_path, c)
    lid = a.mgr.create_list('L')
    n = 80 if cell == 'confirmation-capped' else 3
    for i in range(n):
        a.mgr.add_item(f'99{i:04d}', lid, note=f'note {i}', fl_id='FLa')
    assert a.up()['success']
    keys = sorted(k for k, it in a.mgr.data['items'].items() if lid in it['lists'])
    first = keys[0]
    rid = row_in(c, a, first, lid)
    old_cloud = cloud_id(a, lid)
    if cell in ('confirmation-capped', 'found-in-another-list'):
        moved_to = c.new_list('Elsewhere')
        for k in (keys if cell == 'confirmation-capped' else [first]):
            c.row(row_in(c, a, k, lid))['list_id'] = moved_to      # another computer moved the rows
        if cell == 'confirmation-capped':
            c.web.table('user_lists').delete().eq('id', old_cloud).execute()
    else:
        c.delete_row(rid)
    if cell in ('filter-ignored', 'later-page-error'):
        a.mgr.add_item('990009', lid, fl_id='FLz')    # a membership with no record: no insert this pass
    sync = a.up if direction == 'upload' else a.down

    def later_page(client, req):
        if client.actor == 'A' and _is_list_read(req) and any(v == old_cloud for _, _, v in req.filters) \
                and _is_later_page(req):
            if cell == 'later-page-error':
                return 'api_pgrst103'   # an error on a later page fails the read; it does not end it short
            _as_the_first_page(req)     # the server answers the first page again: the ids do not rise
        return None

    def lose(client, req):
        if client.actor == 'A' and _is_confirmation(req):
            client.session_user = None

    hooks = {'filter-ignored': later_page, 'later-page-error': later_page,
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
    if cell in ('read-error', 'later-page-error') or (cell == 'anonymous-read' and direction == 'upload'):
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
    if cell == 'filter-ignored':
        assert result['success']
        assert not [p for p in _payloads(mark(op='insert')) if p.get('sys_id') == '990009']


@pytest.mark.parametrize('direction', ['upload', 'download'])
def test_a_row_the_website_adds_between_pages_is_read_and_the_read_is_complete(tmp_path, direction):
    """A row added between two pages has an id above every row returned, so a later page returns it and the
    read is complete. (Paged by offset, the count changed and the read was not complete.) A row removed on
    the website before the read is recorded as removed, and an upload inserts a membership with no record
    in the same pass."""
    c = Cloud(max_rows=1)
    a = make_desk(tmp_path, c)
    lid = a.mgr.create_list('L')
    for i in range(3):
        a.mgr.add_item(f'99{i:04d}', lid, note=f'note {i}', fl_id='FLa')
    assert a.up()['success']
    first = sorted(k for k, it in a.mgr.data['items'].items() if lid in it['lists'])[0]
    cl = cloud_id(a, lid)
    c.delete_row(row_in(c, a, first, lid))
    a.mgr.add_item('990009', lid, fl_id='FLz')           # a membership with no record

    def add_between_pages(client, req):
        if client.actor == 'A' and _is_list_read(req) and _is_later_page(req):
            c.rec.hook = None
            c.add(cl, '990008', fl_id='FLq', note='website')
    c.rec.hook = add_between_pages
    mark = c.rec.mark()
    result = a.up() if direction == 'upload' else a.down()
    assert result['success'] and result['web_removed'] == [(first, lid)]
    if direction == 'upload':
        assert [p['sys_id'] for p in _payloads(mark(op='insert'))] == ['990009']
        assert result['unchecked'] == 0
    else:
        assert mark(op='insert') == []
        assert [k for k, it in a.mgr.data['items'].items() if it['sys_id'] == '990008' and lid in it['lists']]


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
    assert result['notes_kept'] == 2                                          # per membership
    assert result['notes_differing'] == 1 and a.mgr.differing_notes_count() == 1   # per entry
    result = a.down()
    assert result['notes_merged'] == 1 and result['tags_merged'] == 0         # per entry
    assert item(a, '990001::fl::FLa')['note'] == 'local' + MARK + 'k text' + MARK + 'l text'
    assert result['notes_differing'] == 0 and a.mgr.differing_notes_count() == 0


def test_a_note_too_long_to_update_counts_once_for_an_entry_in_two_lists(tmp_path, cloud):
    a = make_desk(tmp_path, cloud)
    k, l_ = a.mgr.create_list('K'), a.mgr.create_list('L')
    key = '990001::fl::FLa'
    long_note = 'y' * 9000
    a.mgr.add_item('990001', k, note=long_note, fl_id='FLa')
    a.mgr.add_item('990001', l_, fl_id='FLa')
    assert a.up()['success']
    a.mgr.update_item(key, note=long_note + '\nmore')
    mark = cloud.rec.mark()
    result = a.up()
    assert len([r for r in mark(op='update') if 'note' in (r.payload or {})]) == 2   # both rows were tried
    assert result['notes_too_long'] == 1 and result['items_failed'] == 0
    assert [r['note'] for r in cloud.rows(sys_id='990001')] == [long_note, long_note]


@pytest.mark.parametrize('what', ['tags-only', 'note-and-tags'])
def test_a_download_counts_combined_tags_apart_from_notes_kept_under_a_marker(tmp_path, cloud, what):
    # The dialog's note line promises a marker line in the note: a Download that only
    # combined tags wrote none, so it counts in tags_merged alone.
    a = make_desk(tmp_path, cloud)
    lid = a.mgr.create_list('Mine')
    a.mgr.add_item('990001', lid, note='same note', tags=['a'], fl_id='FLa')
    cl = cloud.new_list('Mine')
    cloud.add(cl, '990001', fl_id='FLa', note='same note' if what == 'tags-only' else 'theirs', tags=['b'])
    result = a.down()
    it = item(a, '990001::fl::FLa')
    assert it['tags'] == ['a', 'b']
    if what == 'tags-only':
        assert it['note'] == 'same note'
        assert (result['notes_merged'], result['tags_merged']) == (0, 1)
    else:
        assert it['note'] == 'same note' + MARK + 'theirs'
        assert (result['notes_merged'], result['tags_merged']) == (1, 1)
    again = a.down()
    assert (again['notes_merged'], again['tags_merged']) == (0, 0)


def test_a_differing_note_in_a_list_in_the_trash_is_not_counted_until_it_is_restored(tmp_path, cloud):
    # No pass reaches the entries of a list in the Trash, so the count the dialog shows
    # (and 'Merge Both' cannot clear) leaves them out; the difference is still known.
    a = make_desk(tmp_path, cloud)
    lid = a.mgr.create_list('L')
    key = '990001::fl::FLa'
    a.mgr.add_item('990001', lid, note='desk', fl_id='FLa')
    assert a.up()['success']
    cloud.set(row_in(cloud, a, key, lid), note='web')
    a.mgr.update_item(key, note='desk 2')
    assert a.up()['notes_differing'] == 1 and a.mgr.differing_notes_count() == 1
    assert a.mgr.delete_list(lid) and a.mgr.data['lists'][lid].get('deleted_at')
    assert a.mgr.differing_notes_count() == 0
    assert a.up()['notes_differing'] == 0
    down, up = merge(a)
    assert down['notes_differing'] == 0 and up['notes_differing'] == 0
    assert rec(a, key, lid).get('differs') is True
    assert a.mgr.restore_list(lid)
    assert a.mgr.differing_notes_count() == 1
    assert a.up()['notes_differing'] == 1
    down, up = merge(a)
    assert down['notes_merged'] == 1 and up['notes_differing'] == 0
    assert item(a, key)['note'] == 'desk 2' + MARK + 'web'


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
    rk, rl = row_in(cloud, a, key, k), row_in(cloud, a, key, l_)
    a.mgr.update_item(key, note='local')
    cloud.set(rk, note='k web')
    cloud.set(rl, note='l web')
    if how == 'move':
        a.mgr.move_items_to_list([key], k, l_)
    else:
        a.mgr.merge_duplicate_group(l_, [k])
    mark = cloud.rec.mark()
    assert a.up()['success']
    assert mark(op='delete') == []             # K's row carries a website edit the entry lacks: it stays
    assert not [r for r in mark(op='update') if any(col == 'id' and v == rk for _, col, v in r.filters)]
    assert a.down()['success']
    assert item(a, key)['lists'] == [l_]
    assert item(a, key)['note'] == 'local' + MARK + 'k web' + MARK + 'l web'
    assert cloud.row(rk) is not None
    mark = cloud.rec.mark()
    assert a.up()['success']                   # the entry holds its text now: the redundant row goes
    (gone,) = mark(op='delete')
    assert ('eq', 'id', rk) in gone.filters and cloud.row(rk) is None and cloud.row(rl) is not None


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
    l_cloud, k_cloud, rid = cloud_id(a, l_), cloud_id(a, k), row_in(cloud, a, key, k)
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


@pytest.mark.parametrize('cell', ['chained-before-upload', 'moved-again-during-the-move'])
def test_moves_end_with_one_row_in_the_final_list(tmp_path, cloud, cell):
    a = make_desk(tmp_path, cloud)
    k, l_, n = a.mgr.create_list('K'), a.mgr.create_list('L'), a.mgr.create_list('N')
    key = '990001::fl::FLa'
    a.mgr.add_item('990001', k, fl_id='FLa')
    assert a.up()['success']
    k_cloud, rid = cloud_id(a, k), row_in(cloud, a, key, k)
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
    rid = row_in(cloud, a, key, lid)
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
        assert mark() == [] and mark(table='user_lists') == [] and mark(table='projects') == []
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
    key = next(k for k, it in a.mgr.data['items'].items() if it['sys_id'] == target['sys_id'])
    c.set(target['id'], note='website')
    if cell == 'both-changed':
        a.mgr.update_item(key, note='mine')

    def delete_between_pages(client, req):
        if client.actor == 'A' and _is_list_read(req) and _is_later_page(req):
            c.rec.hook = None
            c.delete_row(victim['id'])
            # and the server answers page 2 with the first page again: the read ends not complete, and
            # position 50's row, which it never returned, is found by the confirmation
            _as_the_first_page(req)
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
        if client.actor == 'A' and _is_list_read(req) and _is_later_page(req):
            _as_the_first_page(req)     # page 2 comes back as page 1: the read never reaches the entry's row
        if client.actor == 'A' and _is_confirmation(req):   # ... and the confirmation fails
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
    rid = row_in(cloud, a, key, k)
    a.mgr.move_items_to_list([key], k, l_)

    def lose_the_response(client, req):
        if client.actor == 'A' and req.t == 'list_items' and req.op == 'update':
            cloud.rec.hook = None
            return 'raise_after'
        return None
    cloud.rec.hook = lose_the_response
    assert a.up()['success'] is False
    moved = '~%s' % rid                        # a moved row's record, on its way to L
    assert cloud.row(rid)['list_id'] == cloud_id(a, l_) and rec(a, key, moved)['id'] == rid
    cloud.set(rid, note='b')
    if cell == 'both-changed':
        a.mgr.update_item(key, note='c')
    items_before = set(a.mgr.data['items'])
    assert a.down()['success']
    expected = 'b' if cell == 'cloud-only-changed' else 'c' + MARK + 'b'
    assert item(a, key)['note'] == expected and item(a, key)['lists'] == [l_]
    assert set(a.mgr.data['items']) == items_before and rec(a, key, moved)['note'] == 'b'
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


# The cells about a page row need the page column; the two about folios hold with it and without it.
T55_CELLS = [('whole-row-beside-page', True), ('same-page-other-folio', True), ('same-page-other-folio', False),
             ('same-page-one-folio-unknown', True), ('recorded-row-other-folio', True),
             ('recorded-row-other-folio', False)]


@pytest.mark.parametrize('cell,has_page', T55_CELLS,
                         ids=[f"{c}-{'page-column' if p else 'no-page-column'}" for c, p in T55_CELLS])
def test_identity_is_never_bent_to_pair_a_row(tmp_path, cell, has_page):
    cloud = Cloud(has_page=has_page)
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
        cloud.add(cloud_id(a, lid), '990001', fl_id='FL2', note='n2', page='1' if has_page else None)
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
        rid = row_in(cloud, a, '990001::img::1', lid)
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


# ---- a list renamed on the website or on this computer

def _rename_on_web(c, cloud_list_id, name):
    """update_list on the website: name and name_en (web/user_lists.py)."""
    c.web.table('user_lists').update({'name': name, 'name_en': name}).eq('id', cloud_list_id).execute()


def _name_writes(mark, actor='A'):
    return [r for r in mark(actor=actor, table='user_lists', op='update') if 'name' in r.payload]


@pytest.mark.parametrize('first', ['download-first', 'upload-first'])
def test_a_list_renamed_on_the_website_takes_that_name_and_keeps_its_entries(tmp_path, cloud, first):
    # Matched by the cloud id it holds, not by name: the Download gives the list the website's
    # name and keeps its records; an upload -- before that Download or after it -- keeps the name.
    a = make_desk(tmp_path, cloud)
    lid = a.mgr.create_list('L')
    a.mgr.add_item('990001', lid, fl_id='FLa')
    assert a.up()['success']
    five = cloud_id(a, lid)
    records = copy.deepcopy(item(a, '990001::fl::FLa')['cloud_rows'])
    _rename_on_web(cloud, five, 'Renamed')
    if first == 'upload-first':
        mark = cloud.rec.mark()
        assert a.up()['success']
        assert _name_writes(mark) == [] and cloud.db.list_by_id(five)['name'] == 'Renamed'
    result = a.down()
    assert result['success'] and result['unchecked'] == 0 and result['web_removed'] == []
    assert a.mgr.data['lists'][lid]['name'] == 'Renamed'
    assert [k for k, ld in a.mgr.data['lists'].items() if ld.get('cloud_id') == five] == [lid]
    assert item(a, '990001::fl::FLa')['cloud_rows'] == records       # no record dropped for a rename
    state = copy.deepcopy(a.mgr.data)
    assert a.down()['success']
    assert a.mgr.data == state                     # a second Download changes nothing
    mark = cloud.rec.mark()
    assert a.up()['success']
    assert mark(op='insert') == [] and _name_writes(mark) == [] and len(cloud.rows(list_id=five)) == 1
    assert cloud.db.list_by_id(five)['name'] == 'Renamed' and a.mgr.data['lists'][lid]['name'] == 'Renamed'


@pytest.mark.parametrize('first', ['download-first', 'upload-first'])
def test_a_list_renamed_here_since_the_last_upload_keeps_its_name_over_the_website(tmp_path, cloud, first):
    # Renamed on this computer and on the website before this computer uploaded: this
    # computer's name stands, the next upload sends it, and later website renames are taken again.
    a = make_desk(tmp_path, cloud)
    lid = a.mgr.create_list('L')
    a.mgr.add_item('990001', lid, fl_id='FLa')
    assert a.up()['success']
    five = cloud_id(a, lid)
    records = copy.deepcopy(item(a, '990001::fl::FLa')['cloud_rows'])
    assert a.mgr.update_list(lid, name='Mine')
    _rename_on_web(cloud, five, 'Theirs')
    if first == 'download-first':
        result = a.down()
        assert result['success'] and result['unchecked'] == 0 and result['web_removed'] == []
        assert a.mgr.data['lists'][lid]['name'] == 'Mine'
        assert item(a, '990001::fl::FLa')['cloud_rows'] == records
    mark = cloud.rec.mark()
    assert a.up()['success']
    assert [r.payload['name'] for r in _name_writes(mark)] == ['Mine']
    assert cloud.db.list_by_id(five)['name'] == 'Mine'
    mark = cloud.rec.mark()
    assert a.down()['success'] and a.up()['success']
    assert a.mgr.data['lists'][lid]['name'] == 'Mine' and _name_writes(mark) == []
    _rename_on_web(cloud, five, 'Later')
    assert a.down()['success']
    assert a.mgr.data['lists'][lid]['name'] == 'Later'
    assert item(a, '990001::fl::FLa')['cloud_rows'] == records


def test_a_rename_whose_write_reached_no_row_goes_up_with_the_next_upload(tmp_path, cloud):
    # The list's update answered by an anonymous session writes nothing: the rename made here
    # is still unsent, so a Download keeps it and the next upload sends it.
    a = make_desk(tmp_path, cloud)
    lid = a.mgr.create_list('L')
    assert a.up()['success']
    five = cloud_id(a, lid)
    assert a.mgr.update_list(lid, name='Mine')
    cloud.rec.hook = lambda client, req: 'anon' if (client.actor, req.t, req.op) == ('A', 'user_lists', 'update') \
        else None
    a.up()
    cloud.rec.hook = None
    assert cloud.db.list_by_id(five)['name'] == 'L'
    assert a.down()['success'] and a.mgr.data['lists'][lid]['name'] == 'Mine'
    assert a.up()['success'] and cloud.db.list_by_id(five)['name'] == 'Mine'


def test_two_computers_take_one_website_rename_and_never_rename_it_back(tmp_path, cloud):
    a, b = make_desk(tmp_path, cloud, 'A'), make_desk(tmp_path, cloud, 'B')
    lid = a.mgr.create_list('L')
    a.mgr.add_item('990001', lid, fl_id='FLa')
    assert a.up()['success'] and b.down()['success']
    five = cloud_id(a, lid)
    (blid,) = [k for k, ld in b.mgr.data['lists'].items() if ld.get('cloud_id') == five]
    _rename_on_web(cloud, five, 'Renamed')
    b.mgr.add_item('990002', blid, fl_id='FLb')     # B uploads this before it downloads anything
    mark = cloud.rec.mark()
    seen = []
    for d, sync in [(b, 'up'), (a, 'down'), (a, 'up'), (b, 'down')] * 3:
        assert getattr(d, sync)()['success']
        seen.append((cloud.db.list_by_id(five)['name'], a.mgr.data['lists'][lid]['name'],
                     b.mgr.data['lists'][blid]['name']))
    # the cloud keeps the website's name throughout; each computer takes it at its first
    # Download and never changes it again
    assert [s[0] for s in seen] == ['Renamed'] * len(seen)
    for n in (1, 2):
        names = [s[n] for s in seen]
        first = names.index('Renamed')
        assert set(names[first:]) == {'Renamed'}
    assert seen[-1] == ('Renamed', 'Renamed', 'Renamed')
    assert _name_writes(mark, 'A') == [] and _name_writes(mark, 'B') == []
    for d, key in ((a, lid), (b, blid)):
        assert sorted(k for k, it in d.mgr.data['items'].items() if key in it['lists']) == [
            '990001::fl::FLa', '990002::fl::FLb']
        assert [k for k, ld in d.mgr.data['lists'].items() if ld.get('cloud_id') == five] == [key]
    assert len(cloud.rows(list_id=five)) == 2


@pytest.mark.parametrize('other', ['holds-its-own-cloud-list', 'not-uploaded-yet'])
def test_a_website_rename_to_another_lists_name_leaves_two_lists_of_that_name(tmp_path, cloud, other):
    # The name is taken anyway: two local lists may share a name (create_list and
    # update_list allow it), each keeps its own cloud list and its own entries, and
    # nothing is merged -- Clean up duplicate lists offers that to the user.
    a = make_desk(tmp_path, cloud)
    l_ = a.mgr.create_list('L')
    a.mgr.add_item('990001', l_, fl_id='FLa')
    if other == 'holds-its-own-cloud-list':
        m = a.mgr.create_list('M')
        a.mgr.add_item('990002', m, fl_id='FLb')
    assert a.up()['success']
    if other == 'not-uploaded-yet':
        m = a.mgr.create_list('M')
        a.mgr.add_item('990002', m, fl_id='FLb')
    five = cloud_id(a, l_)
    _rename_on_web(cloud, five, 'M')
    result = a.down()
    assert result['success'] and result['unchecked'] == 0 and result['web_removed'] == []
    lists = a.mgr.data['lists']
    assert lists[l_]['name'] == 'M' and lists[m]['name'] == 'M'
    assert lists[l_]['cloud_id'] == five and lists[m].get('cloud_id') != five
    assert item(a, '990001::fl::FLa')['lists'] == [l_] and item(a, '990002::fl::FLb')['lists'] == [m]
    assert [sorted(x['id'] for x in g['lists']) for g in a.mgr.find_duplicate_lists()] == [sorted([l_, m])]
    mark = cloud.rec.mark()
    assert a.up()['success']
    assert _name_writes(mark) == [] and cloud.db.list_by_id(five)['name'] == 'M'
    nine = cloud_id(a, m)
    assert nine != five and cloud.db.list_by_id(nine)['name'] == 'M'
    assert [r['sys_id'] for r in cloud.rows(list_id=five)] == ['990001']
    assert [r['sys_id'] for r in cloud.rows(list_id=nine)] == ['990002']
    state = copy.deepcopy(a.mgr.data)
    mark = cloud.rec.mark()
    assert a.down()['success'] and a.up()['success']
    assert a.mgr.data == state and mark(op='insert') == [] and _name_writes(mark) == []


@pytest.mark.parametrize('lang', ['en', 'he'])
@pytest.mark.parametrize('which', ['default-list', 'downloaded-list'])
def test_a_list_renamed_here_shows_its_new_name_in_either_interface(tmp_path, cloud, monkeypatch, which, lang):
    # The English interface shows a list's name_en when it has one: the default list and
    # every list a Download created have one. A rename made here must reach it, and the
    # upload then sends the new name as both names (as the website's rename writes them).
    import genizah_app
    monkeypatch.setattr(genizah_app, 'CURRENT_LANG', lang)
    a = make_desk(tmp_path, cloud)

    def shown(lid):
        return genizah_app.GenizahGUI._get_list_display_name(None, a.mgr.data['lists'][lid])

    if which == 'default-list':
        lid = 'default'
        assert a.up()['success']
    else:
        five = cloud.new_list('Theirs')
        assert a.down()['success']
        (lid,) = [k for k, ld in a.mgr.data['lists'].items() if ld.get('cloud_id') == five]
        assert shown(lid) == 'Theirs'
    assert 'name_en' in a.mgr.data['lists'][lid]
    assert a.mgr.update_list(lid, name='Mine')
    assert shown(lid) == 'Mine'
    mark = cloud.rec.mark()
    assert a.up()['success']
    assert [(r.payload['name'], r.payload['name_en']) for r in _name_writes(mark)] == [('Mine', 'Mine')]
    assert a.down()['success'] and shown(lid) == 'Mine'


@pytest.mark.parametrize('marked', [False, True], ids=['renamed-on-the-website', 'renamed-here'])
def test_the_single_list_upload_sends_a_name_only_for_a_list_renamed_here(tmp_path, cloud, marked):
    # sync_list_to_cloud's update path follows the upload's rule: a list with a cloud id
    # sends its name only while it is marked as renamed here, and the write that returns
    # the row clears that mark, so a later website rename is taken by the next Download.
    a = make_desk(tmp_path, cloud)
    lid = a.mgr.create_list('L')
    assert a.up()['success']
    five = cloud_id(a, lid)
    _rename_on_web(cloud, five, 'Theirs')
    if marked:
        assert a.mgr.update_list(lid, name='Mine')
        assert a.mgr.data['lists'][lid].get(lists_sync.LIST_NAME_UNSENT)
    mark = cloud.rec.mark()
    assert a.sync.sync_list_to_cloud(lid)
    (write,) = mark(table='user_lists', op='update')
    if marked:
        assert (write.payload['name'], write.payload['name_en']) == ('Mine', 'Mine')
        assert cloud.db.list_by_id(five)['name'] == 'Mine'
        assert not a.mgr.data['lists'][lid].get(lists_sync.LIST_NAME_UNSENT)
        assert not saved(a)['lists'][lid].get(lists_sync.LIST_NAME_UNSENT)
        _rename_on_web(cloud, five, 'Later')
        assert a.down()['success'] and a.mgr.data['lists'][lid]['name'] == 'Later'
    else:
        assert 'name' not in write.payload and 'name_en' not in write.payload
        assert cloud.db.list_by_id(five)['name'] == 'Theirs'
        assert a.down()['success'] and a.mgr.data['lists'][lid]['name'] == 'Theirs'


def _web_list_change(c, cloud_list_id, change):
    """A change the website makes to a list other than its name: the values it writes."""
    if change == 'colour':
        values = {'color': '#123456'}
    elif change == 'trash':
        values = {'deleted_at': '2026-09-27T10:00:00+00:00'}
    else:
        proj = c.web.table('projects').insert({'user_id': 'u1', 'name': 'P', 'color': '#111111'}).execute().data[0]
        values = {'project_id': proj['id']}
    c.web.table('user_lists').update(values).eq('id', cloud_list_id).execute()
    return values


@pytest.mark.parametrize('first', ['download-first', 'upload-first'])
def test_a_first_download_that_kept_the_colour_here_still_takes_a_website_rename(tmp_path, cloud, first):
    # The first Download pairs a list that held no cloud id with the website's list of its
    # name, whose colour differs: the colour chosen here stands until an upload sends it.
    # That is no rename here, so a later website rename is taken by the next Download, and
    # no upload -- before that Download or after it -- writes the old name back.
    a = make_desk(tmp_path, cloud)
    lid = a.mgr.create_list('Foo')
    colour = a.mgr.data['lists'][lid]['color']
    five = cloud.new_list('Foo')
    _web_list_change(cloud, five, 'colour')
    assert colour != '#123456'
    assert a.down()['success']
    assert cloud_id(a, lid) == five and a.mgr.data['lists'][lid]['color'] == colour
    _rename_on_web(cloud, five, 'Bar')
    if first == 'upload-first':
        mark = cloud.rec.mark()
        assert a.up()['success']
        assert _name_writes(mark) == [] and cloud.db.list_by_id(five)['name'] == 'Bar'
    assert a.down()['success']
    assert a.mgr.data['lists'][lid]['name'] == 'Bar'
    mark = cloud.rec.mark()
    assert a.up()['success']
    assert _name_writes(mark) == []
    row = cloud.db.list_by_id(five)
    assert (row['name'], row['color']) == ('Bar', colour)
    assert a.down()['success'] and a.mgr.data['lists'][lid]['name'] == 'Bar'


@pytest.mark.parametrize('change', ['colour', 'trash', 'project'])
def test_a_rename_here_does_not_hold_back_a_website_change_to_the_list(tmp_path, cloud, change):
    # Renamed here, and on the website given another colour (or put in the Trash, or into a
    # project) before this computer uploaded: the Download takes the website's change and
    # keeps this computer's name; the upload sends that name and leaves the change as it is.
    a = make_desk(tmp_path, cloud)
    lid = a.mgr.create_list('Foo')
    assert a.up()['success']
    five = cloud_id(a, lid)
    assert a.mgr.update_list(lid, name='Mine')
    values = _web_list_change(cloud, five, change)
    assert a.down()['success']
    ld = a.mgr.data['lists'][lid]
    assert ld['name'] == 'Mine'
    if change == 'colour':
        assert ld['color'] == '#123456'
    elif change == 'trash':
        assert ld.get('deleted_at')
    else:
        assert a.mgr.data['projects'][ld['project_id']]['cloud_id'] == values['project_id']
    mark = cloud.rec.mark()
    assert a.up()['success']
    assert [r.payload['name'] for r in _name_writes(mark)] == ['Mine']
    row = cloud.db.list_by_id(five)
    assert row['name'] == 'Mine'
    assert {k: row[k] for k in values} == values


@pytest.mark.parametrize('cell', ['remapped-list-upload', 'remapped-list-download', 'two-local-lists-one-name'])
def test_a_record_for_a_list_that_is_no_longer_its_own_is_dropped(tmp_path, cloud, cell):
    a = make_desk(tmp_path, cloud)
    if cell.startswith('remapped-list'):
        lid = a.mgr.create_list('L')
        key = '990001::fl::FLa'
        a.mgr.add_item('990001', lid, fl_id='FLa')
        assert a.up()['success']
        five, rid = cloud_id(a, lid), row_in(cloud, a, key, lid)
        cloud.web.table('user_lists').update({'name': 'Renamed'}).eq('id', five).execute()
        cloud.delete_row(rid)
        nine = cloud.new_list('L')
        # L took 9 as its own in an earlier pass (a list holding an id owns that cloud list
        # whatever it is called, so a Download no longer moves L from 5 to 9 by name)
        a.mgr.data['lists'][lid]['cloud_id'] = nine
        if cell == 'remapped-list-upload':
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
    rk, rl = row_in(cloud, a, key, k), row_in(cloud, a, key, l_)
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
    k_key = k
    if shape == 'orphan-and-destination':
        a.mgr.move_items_to_list([key], k, l_)
        k_key = '~%s' % rk                     # K's row, on its way to L
    a.mgr.data['lists_order'] = [k, l_] if first == 'K-first' else [l_, k]
    result = a.down()
    expected = 'b' + MARK + 'c' if field == 'note' else ['b', 'c']
    assert item(a, key)[field] == expected
    # a combined note is kept under a marker line; combined tags are counted apart
    assert (result['notes_merged'], result['tags_merged']) == ((1, 0) if field == 'note' else (0, 1))
    assert (rec(a, key, k_key)[field], rec(a, key, l_)[field]) == (val('b'), val('c'))
    mark = cloud.rec.mark()
    assert a.up()['success']
    patched = {next(v for _, col, v in r.filters if col == 'id'): r for r in mark(op='update')}
    if shape == 'two-memberships':
        assert set(patched) == {rk, rl}
    else:
        # L has its own row, and the entry holds K's text now: K's row goes, only as it was read
        (gone,) = mark(op='delete')
        assert set(patched) == {rl} and cloud.row(rk) is None and ('eq', 'id', rk) in gone.filters
        assert {col for _, col, _ in gone.filters} >= {'note', 'tags'}
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


# --------------------------------------------------------------------------- an upload while the user edits the lists

_ENGINE_CODE = {}


def _is_engine(code):
    known = _ENGINE_CODE.get(code)
    if known is None:
        known = _ENGINE_CODE[code] = (os.path.normcase(os.path.abspath(code.co_filename))
                                      == os.path.normcase(os.path.abspath(lists_sync.__file__)))
    return known


def _edit_inside_every_engine_loop(edit):
    """A trace function that calls edit() inside each loop of the sync engine, between two of its steps.

    The auto-upload runs on the live store while the user edits it on another thread,
    which may run at any step of a loop. Here every loop of the engine gets an edit the
    first time it goes round, before its next step. Returns (tracer, the loops edited in).
    """
    last, done = {}, set()

    def local(frame, event, arg):
        if event == 'line':
            prev = last.get(frame)
            last[frame] = frame.f_lineno
            where = (frame.f_code, frame.f_lineno)
            if prev is not None and frame.f_lineno <= prev and where not in done:
                done.add(where)
                edit()
        return local

    def tracer(frame, event, arg):
        return local if _is_engine(frame.f_code) else None
    return tracer, done


def _one_row_per_membership(c, d):
    """Each membership of a list an upload writes has exactly one row in that list's cloud list."""
    for key, it in d.mgr.data['items'].items():
        for lid in it.get('lists', []):
            ld = d.mgr.data['lists'][lid]
            if lid == 'recent' or ld.get('is_system') or ld.get('deleted_at'):
                continue
            rows = [r for r in c.rows(list_id=ld.get('cloud_id'), sys_id=it['sys_id'])
                    if r.get('fl_id') == it.get('fl_id')]
            assert len(rows) == 1, (key, lid, rows)


def test_an_upload_goes_through_when_the_user_edits_between_its_steps(tmp_path, cloud):
    a = make_desk(tmp_path, cloud)
    lids = [a.mgr.create_list(f'L{i}') for i in range(3)]
    for n in range(12):
        a.mgr.add_item(f'9900{n:02d}', lids[n % 3], note=f'n{n}', fl_id=f'FL{n}')
    assert a.up()['success']
    for n in range(12, 18):
        a.mgr.add_item(f'9900{n:02d}', lids[n % 3], fl_id=f'FL{n}')        # entries to insert
    a.mgr.move_items_to_list(['990000::fl::FL0'], lids[0], lids[1])        # a row to move
    a.mgr.update_item('990001::fl::FL1', note='n1, edited')                # a note to write
    made = []

    def user_edit():                  # creating a list starts an auto-upload; the user goes on creating and adding
        made.append(len(made))
        lids.append(a.mgr.create_list(f'New {len(made)}'))
        a.mgr.add_item(f'9950{len(made):02d}', lids[len(made) % len(lids)], note=f'added {len(made)}')
    tracer, loops = _edit_inside_every_engine_loop(user_edit)
    before = sys.gettrace()
    sys.settrace(tracer)
    try:
        result = a.up()
    finally:
        sys.settrace(before)
    assert len(loops) >= 10 and made           # the edits did come inside the engine's loops
    assert result['success'], result.get('error')
    assert result['items_failed'] == 0 and result['lists_not_uploaded'] == []
    assert a.up()['success']                   # what the user added meanwhile goes up with the next upload
    _one_row_per_membership(cloud, a)


def test_an_auto_upload_racing_the_user_on_another_thread_succeeds(tmp_path, cloud, monkeypatch):
    a = make_desk(tmp_path, cloud)
    lids = [a.mgr.create_list(f'L{i}') for i in range(8)]
    for n in range(3000):                      # entries in no list, which the upload still walks past
        key = f'99{n:05d}'
        a.mgr.data['items'][key] = {'sys_id': key, 'lists': [], 'note': '', 'tags': [], 'added': n, 'modified': n}
    for n in range(80):
        a.mgr.add_item(f'9910{n:02d}', lids[n % 8], note=f'n{n}', fl_id=f'FL{n}')
    assert a.up()['success']
    monkeypatch.setattr(a.mgr, 'save', lambda: True)       # the edits are about the store, not the file
    cloud.rec.hook = lambda client, req: time.sleep(0.001) if client.actor == 'A' else None   # a request takes a moment
    stop, added, results = threading.Event(), [], []

    def user():
        while not stop.is_set() and len(added) < 2000:
            n = len(added)
            a.mgr.add_item(f'9920{n:04d}', lids[n % 8], fl_id=f'U{n}')
            added.append(n)
            time.sleep(0.0003)
    interval = sys.getswitchinterval()
    other = threading.Thread(target=user, daemon=True)
    sys.setswitchinterval(1e-5)
    try:
        other.start()
        for _ in range(4):
            results.append(a.up())
    finally:
        stop.set()
        other.join(10)
        sys.setswitchinterval(interval)
        cloud.rec.hook = None
    assert added and [r.get('error') for r in results] == [None] * 4
    assert all(r['success'] for r in results)
    assert a.up()['success']
    _one_row_per_membership(cloud, a)


# --------------------------------------------------------------------------- a batch that commits and then fails

@pytest.mark.parametrize('cell', ['read-timeout', 'gateway-504', 'pgrst111-after-commit'])
def test_a_batch_that_committed_before_its_error_is_not_inserted_again(tmp_path, cell):
    c = Cloud()
    a = make_desk(tmp_path, c)
    lid = a.mgr.create_list('L')
    for sys_id in ('990001', '990002', '990003'):
        a.mgr.add_item(sys_id, lid, img='1')
    error = {'read-timeout': 'raise_after', 'gateway-504': 'api_504', 'pgrst111-after-commit': 'api_pgrst111'}[cell]

    def commit_then_fail(client, req):
        if client.actor == 'A' and req.t == 'list_items' and req.op == 'insert' and isinstance(req.payload, list):
            c.rec.hook = None
            return error                   # the rows are written; the answer is an error
        return None
    c.rec.hook = commit_then_fail
    mark = c.rec.mark()
    result = a.up()
    assert len(mark(op='insert')) == 1, 'rows of a batch that may have been written were inserted again'
    assert result['success'] is False and result['items_failed'] == 3 and result['items_pushed'] == 0
    assert result['lists_not_uploaded'] == ['L'] and result['complete'] is False
    assert sorted(r['sys_id'] for r in c.rows()) == ['990001', '990002', '990003']
    mark = c.rec.mark()
    result = a.up()
    assert result['success'] and result['complete'] and mark(op='insert') == []
    assert sorted(r['sys_id'] for r in c.rows()) == ['990001', '990002', '990003']   # one row per entry
    assert {rec(a, f'{s}::img::1', lid)['id'] for s in ('990001', '990002', '990003')} == \
        {r['id'] for r in c.rows()}


# --------------------------------------------------------------------------- a remembered row while the read changes

@pytest.mark.parametrize('cell', ['website-removes-a-read-row', 'read-ends-short'])
def test_a_recorded_row_is_found_and_written_when_the_read_changes_under_it(tmp_path, cell):
    """website-removes-a-read-row: after page 1 the website removes the row it returned. (Paged by offset,
    page 2 then started past this entry's row, which the confirmation had to find; paged by row id, page 2
    starts after the id page 1 returned and reads it.) read-ends-short: the server answers page 2 with page
    1 again, so the read never returns this entry's row and the confirmation finds it where it was
    recorded. Either way the write goes to that row, conditional on the note read."""
    c = Cloud(max_rows=1)                      # the server answers one row a request
    a = make_desk(tmp_path, c)
    lid = a.mgr.create_list('L')
    for n in range(3):
        a.mgr.add_item(f'99000{n}', lid, note=f'n{n}', fl_id='FLa')
    key = '990001::fl::FLa'
    a.mgr.update_item(key, shelfmark_override='My shelfmark')
    assert a.up()['success']
    first, skipped = row_in(c, a, '990000::fl::FLa', lid), row_in(c, a, key, lid)
    assert first < skipped
    a.mgr.update_item(key, note='n1, edited here')

    def after_page_one(client, req):
        if client.actor == 'A' and _is_list_read(req) and _is_later_page(req):
            c.rec.hook = None
            if cell == 'website-removes-a-read-row':
                c.delete_row(first)
            else:
                _as_the_first_page(req)
    c.rec.hook = after_page_one
    mark = c.rec.mark()
    result = a.up()
    assert mark(op='insert') == []
    (patch,) = mark(op='update')
    assert patch.payload == {'note': 'n1, edited here'}
    assert ('eq', 'id', skipped) in patch.filters and ('eq', 'note', 'n1') in patch.filters
    assert c.row(skipped)['note'] == 'n1, edited here'
    assert result['success'] and result['web_removed'] == []
    # (website-removes-a-read-row) the removed row was read on page 1 before it went: its membership was
    # paired, not unchecked; (read-ends-short) the rows the read never returned were found where recorded
    assert result['unchecked'] == 0
    assert rec(a, key, lid)['id'] == skipped and rec(a, key, lid)['note'] == 'n1, edited here'


# --------------------------------------------------------------------------- a read the website changes as it goes

def test_a_row_a_same_count_change_would_skip_is_paired_not_inserted_again(tmp_path):
    """Between two pages the website removes a row the first page returned and adds another: the count
    holds. Paged by offset, the next page then starts one row late, the read collects as many ids as the
    count and calls itself complete, and the upload inserts a second row for the entry this computer
    added with no record of the website's row."""
    c = Cloud(max_rows=1)                               # the server answers one row a request
    cl = c.new_list('L')
    first = c.add(cl, '990001', fl_id='FLa', note='n1')
    a = make_desk(tmp_path, c)
    assert a.down()['success']
    lid = next(k for k, ld in a.mgr.data['lists'].items() if ld.get('cloud_id') == cl)
    held = c.add(cl, '990002', fl_id='FLa', note='website')   # added on the website ...
    a.mgr.add_item('990002', lid, note='mine', fl_id='FLa')    # ... and here, before either synced it
    key = '990002::fl::FLa'
    assert rec(a, key, lid) is None

    def churn(client, req):
        if client.actor == 'A' and _is_list_read(req) and _is_later_page(req):
            c.rec.hook = None
            c.delete_row(first)                         # a row page 1 returned goes ...
            c.add(cl, '990003', fl_id='FLb')            # ... and a new one comes: the count holds
    c.rec.hook = churn
    mark = c.rec.mark()
    result = a.up()
    assert _payloads(mark(op='insert')) == [], 'the entry held here was inserted a second time'
    assert [r['id'] for r in c.rows(list_id=cl, sys_id='990002')] == [held]
    assert result['success'] and rec(a, key, lid)['id'] == held
    assert 'website' in c.row(held)['note']


def test_a_list_a_same_count_change_would_skip_keeps_its_cloud_list(tmp_path):
    """The same change to the lists themselves: the website deletes a list the first page returned and
    creates another. Paged by offset, the read skips the next list, the upload takes that list's id for
    stale, and creates a second cloud list of its name holding a second row for each of its entries."""
    c = Cloud(max_rows=1)
    spare = c.new_list('Spare')
    cl = c.new_list('B')
    row = c.add(cl, '990001', fl_id='FLa', note='n1')
    a = make_desk(tmp_path, c)
    assert a.down()['success'] and a.up()['success']     # (the upload also sends the local default list)
    lid = next(k for k, ld in a.mgr.data['lists'].items() if ld.get('cloud_id') == cl)

    def churn(client, req):
        if client.actor == 'A' and _is_later_page(req, 'user_lists'):
            c.rec.hook = None
            c.web.table('user_lists').delete().eq('id', spare).execute()   # a list page 1 returned goes ...
            c.new_list('Other')                                           # ... and a new one comes
    c.rec.hook = churn
    mark = c.rec.mark()
    result = a.up()
    assert mark(table='user_lists', op='insert') == [], 'a second cloud list of the same name was created'
    assert mark(op='insert') == []
    assert cloud_id(a, lid) == cl and [r['id'] for r in c.rows(sys_id='990001')] == [row]
    assert result['success']


def _lists_read_ends_short(client, req):
    """The server answers a later page of the list of lists with the first page again: the read is not complete."""
    if client.actor == 'A' and _is_later_page(req, 'user_lists'):
        _as_the_first_page(req)


def test_a_list_whose_cloud_list_an_incomplete_read_missed_keeps_its_id(tmp_path):
    """The list of lists is read short, and a list's cloud list lies past what was read. The upload keeps the
    list's cloud id and leaves the list for the next pass; it creates no cloud list. A pass that reads every
    list then writes to the one it holds."""
    c = Cloud(max_rows=1)
    c.new_list('Spare')                                  # the lowest id: the only list the short read returns
    cl = c.new_list('B')
    row = c.add(cl, '990001', fl_id='FLa', note='n1')
    a = make_desk(tmp_path, c)
    assert a.down()['success'] and a.up()['success']     # (the upload also sends the local default list)
    lid = next(k for k, ld in a.mgr.data['lists'].items() if ld.get('cloud_id') == cl)
    a.mgr.add_item('990002', lid, note='new here', fl_id='FLa')
    c.rec.hook = _lists_read_ends_short
    mark = c.rec.mark()
    result = a.up()
    c.rec.hook = None
    assert mark(table='user_lists', op='insert') == [], 'a second cloud list was created'
    assert cloud_id(a, lid) == cl
    assert mark(op='insert') == [] and mark(op='update') == []
    assert 'B' in result['lists_not_uploaded'] and result['complete'] is False
    mark = c.rec.mark()
    result = a.up()
    assert result['success'] and result['lists_not_uploaded'] == []
    assert mark(table='user_lists', op='insert') == [] and cloud_id(a, lid) == cl
    assert [p['sys_id'] for p in _payloads(mark(op='insert'))] == ['990002']
    assert [r['id'] for r in c.rows(sys_id='990001')] == [row]
    assert sorted(lst['name'] for lst in c.db.tables['user_lists']) == ['B', 'General', 'Spare']


def test_a_new_list_whose_same_name_cloud_list_an_incomplete_read_missed_is_not_created(tmp_path):
    """A list made here, never uploaded, has a same-name cloud list past what a short read of the list of
    lists returned. The upload does not create a cloud list for it (nor for the default list, whose cloud
    list it cannot tell is missing); a pass that reads every list pairs it with the cloud list of its name."""
    c = Cloud(max_rows=1)
    c.new_list('Spare')
    cl = c.new_list('C')
    row = c.add(cl, '990001', fl_id='FLa', note='n1')
    a = make_desk(tmp_path, c)
    lid = a.mgr.create_list('C')
    a.mgr.add_item('990002', lid, note='mine', fl_id='FLa')
    c.rec.hook = _lists_read_ends_short
    mark = c.rec.mark()
    result = a.up()
    c.rec.hook = None
    assert mark(table='user_lists', op='insert') == [], 'a cloud list was created on a short read'
    assert a.mgr.data['lists'][lid].get('cloud_id') is None and mark(op='insert') == []
    assert 'C' in result['lists_not_uploaded'] and result['complete'] is False
    mark = c.rec.mark()
    result = a.up()
    assert result['success'] and result['lists_not_uploaded'] == []
    assert cloud_id(a, lid) == cl
    assert [r.payload['name'] for r in mark(table='user_lists', op='insert')] == ['General']   # none existed
    assert sorted(lst['name'] for lst in c.db.tables['user_lists']) == ['C', 'General', 'Spare']
    assert sorted(r['sys_id'] for r in c.rows(list_id=cl)) == ['990001', '990002'] and c.row(row)['note'] == 'n1'


def test_a_download_keeps_a_list_whose_cloud_list_an_incomplete_read_missed(tmp_path):
    """A download reads the list of lists short: the list's own cloud list lies past page 1, and page 1 holds
    a same-name cloud list no local list holds (the decoy). The list keeps its cloud id, takes nothing from
    the decoy, and keeps the record of its entry; nothing is concluded about it. A download that reads every
    list then applies its own cloud list to it."""
    c = Cloud(max_rows=1)
    decoy = c.new_list('Z')                               # the lowest id: the only list the short read returns
    c.add(decoy, '990005', fl_id='FLd', note='decoy')
    a = make_desk(tmp_path, c)
    lid = a.mgr.create_list('B')
    a.mgr.add_item('990001', lid, note='n1', fl_id='FLa')
    assert a.up()['success']                              # cloud list B made for it, above the decoy's id
    own = cloud_id(a, lid)
    key = '990001::fl::FLa'
    rid = rec(a, key, lid)['id']
    assert own > decoy
    _rename_on_web(c, decoy, 'B')                         # the decoy now has the list's name
    c.set(rid, note='n1, edited on the website')
    c.rec.hook = _lists_read_ends_short
    result = a.down()
    c.rec.hook = None
    assert result['success']
    assert cloud_id(a, lid) == own, 'the list was moved onto the same-name decoy'
    assert not [k for k, it in a.mgr.data['items'].items() if it['sys_id'] == '990005'], 'a decoy row was applied'
    assert rec(a, key, lid) is not None and rec(a, key, lid)['id'] == rid, 'the entry lost its record'
    assert item(a, key)['note'] == 'n1' and result['web_removed'] == []
    result = a.down()
    assert result['success'] and cloud_id(a, lid) == own
    assert rec(a, key, lid)['id'] == rid and item(a, key)['note'] == 'n1, edited on the website'
    # the decoy is a cloud list of this list's name that no local list holds: read for it, as by design
    assert [lid in it['lists'] for it in a.mgr.data['items'].values() if it['sys_id'] == '990005'] == [True]
    assert len([k for k, it in a.mgr.data['items'].items() if it['sys_id'] == '990001']) == 1


# --------------------------------------------------------------------------- what the website stores on a row

def test_an_upload_leaves_the_website_shelfmark_and_title_of_a_row_alone(tmp_path, cloud):
    a = make_desk(tmp_path, cloud)
    lid = a.mgr.create_list('L')
    rid = cloud.add(cloud.new_list('L'), '990001', fl_id='FLa', note='web')   # with the catalogue's shelfmark
    assert a.down()['success']
    (key,) = [k for k, it in a.mgr.data['items'].items() if it['sys_id'] == '990001']
    assert not item(a, key).get('shelfmark_override') and lid in item(a, key)['lists']
    a.mgr.update_item(key, note='web\nand mine')
    mark = cloud.rec.mark()
    assert a.up()['success']
    (patch,) = mark(op='update')
    assert patch.payload == {'note': 'web\nand mine'}
    row = cloud.row(rid)
    assert row['shelfmark'] == S.web_shelfmark('990001') and row['title'] == S.web_title('990001')


# --------------------------------------------------------------------------- moves, duplicates and sessions

def test_a_move_whose_answer_was_lost_moves_on_from_where_the_row_is(tmp_path, cloud):
    a = make_desk(tmp_path, cloud)
    k, l_, n = a.mgr.create_list('K'), a.mgr.create_list('L'), a.mgr.create_list('N')
    key = '990001::fl::FLa'
    a.mgr.add_item('990001', k, fl_id='FLa')
    assert a.up()['success']
    rid = row_in(cloud, a, key, k)
    a.mgr.move_items_to_list([key], k, l_)

    def lose_the_answer(client, req):
        if client.actor == 'A' and req.t == 'list_items' and req.op == 'update':
            cloud.rec.hook = None
            return 'raise_after'
        return None
    cloud.rec.hook = lose_the_answer
    assert a.up()['success'] is False
    assert cloud.row(rid)['list_id'] == cloud_id(a, l_)         # the move was made
    a.mgr.move_items_to_list([key], l_, n)
    mark = cloud.rec.mark()
    result = a.up()
    assert result['success'] and result['items_failed'] == 0
    (move,) = mark(op='update')
    assert move.payload == {'list_id': cloud_id(a, n)}
    assert ('eq', 'id', rid) in move.filters and ('eq', 'list_id', cloud_id(a, l_)) in move.filters
    assert mark(op='insert') == []
    assert [(r['id'], r['list_id']) for r in cloud.rows(sys_id='990001')] == [(rid, cloud_id(a, n))]


def test_two_items_of_one_entry_sharing_an_old_cloud_id_wait_for_a_download(tmp_path, cloud):
    # A store from before per-membership records: the same folio under two keys, both
    # holding the one cloud id of its row. Only a Download may fold them into one item.
    a = make_desk(tmp_path, cloud)
    lid = a.mgr.create_list('L')
    cl = cloud.new_list('L')
    rid = cloud.add(cl, '990001', fl_id='FLa', note='one')
    a.mgr.data['lists'][lid]['cloud_id'] = cl
    for key, note, img, when in (('990001::img::1', 'one', '1', 1), ('990001::fl::FLa', 'two', None, 2)):
        a.mgr.data['items'][key] = {'sys_id': '990001', 'lists': [lid], 'note': note, 'tags': [], 'source': '',
                                    'fl_id': 'FLa', 'img': img, 'shelfmark_override': None, 'cloud_id': rid,
                                    'added': when, 'modified': when}
    notes = {k: it['note'] for k, it in a.mgr.data['items'].items()}
    mark = cloud.rec.mark()
    result = a.up()
    assert {k: it['note'] for k, it in a.mgr.data['items'].items()} == notes    # the upload folds nothing
    assert cloud.row(rid)['note'] == 'one'     # and neither item's note replaced the row's
    assert result.get('waiting') == 1 and mark(op='insert') == []
    assert result['success'] and result['complete'] is False
    assert a.down()['success']
    (kept,) = [it for it in a.mgr.data['items'].values() if it['sys_id'] == '990001']
    assert kept['note'] == 'two' + MARK + 'one' and kept['lists'] == [lid]


def test_a_download_refuses_a_session_of_another_user(tmp_path, cloud):
    a = make_desk(tmp_path, cloud)
    lid = a.mgr.create_list('L')
    a.mgr.add_item('990001', lid, note='n', fl_id='FLa')
    cloud.add(cloud.new_list('L'), '990002', fl_id='FLb', note='web')
    a.client.session_user = 'u2'               # lists sync was set up for u1
    state = copy.deepcopy(a.mgr.data)
    mark = cloud.rec.mark()
    result = a.down()
    assert result == {'success': False, 'error': 'Sync not available'}
    assert mark(op='select') == [] and mark(table='user_lists') == [] and mark(table='projects') == []
    assert a.mgr.data == state


def test_a_same_name_list_in_the_website_trash_adds_no_entries(tmp_path, cloud):
    a = make_desk(tmp_path, cloud)
    lid = a.mgr.create_list('Foo')
    a.mgr.add_item('990001', lid, fl_id='FLa')
    assert a.up()['success']
    own = cloud_id(a, lid)
    trashed = cloud.new_list('Foo', deleted_at='2026-09-27T00:00:00+00:00')
    cloud.add(trashed, '990002', fl_id='FLb', note='in the Trash')
    result = a.down()
    assert result['success'] and result['items_added'] == 0
    assert not [k for k, it in a.mgr.data['items'].items() if it['sys_id'] == '990002']
    assert cloud_id(a, lid) == own and not a.mgr.data['lists'][lid].get('deleted_at')


# --------------------------------------------------------------------------- what the store keeps about a membership

def test_a_differing_note_of_a_list_the_entry_left_is_not_counted(tmp_path, cloud):
    a = make_desk(tmp_path, cloud)
    k, m = a.mgr.create_list('K'), a.mgr.create_list('M')
    key = '990001::fl::FLa'
    a.mgr.add_item('990001', k, note='mine', fl_id='FLa')
    a.mgr.add_item('990001', m, fl_id='FLa')
    cloud.add(cloud.new_list('K'), '990001', fl_id='FLa', note='theirs in K')
    cloud.add(cloud.new_list('M'), '990001', fl_id='FLa', note='mine')
    result = a.up()
    assert result['notes_differing'] == 1 and a.mgr.differing_notes_count() == 1
    a.mgr.remove_item_from_list(key, k)        # the entry stays in M
    assert a.mgr.differing_notes_count() == 0
    assert a.up()['notes_differing'] == 0


def test_an_upload_forgets_a_removal_once_the_entry_left_that_list(tmp_path, cloud):
    a = make_desk(tmp_path, cloud)
    lid, m = a.mgr.create_list('L'), a.mgr.create_list('M')
    key = '990001::fl::FLa'
    a.mgr.add_item('990001', lid, note='n', fl_id='FLa')
    a.mgr.add_item('990001', m, fl_id='FLa')
    assert a.up()['success']
    cloud.delete_row(row_in(cloud, a, key, lid))
    result = a.up()
    assert result['web_removed'] == [(key, lid)] and rec(a, key, lid)['gone'] is True
    a.mgr.remove_item_from_list(key, lid)
    assert a.up()['success']
    assert rec(a, key, lid) is None and rec(a, key, m)['id'] == row_in(cloud, a, key, m)


def test_a_move_whose_row_cannot_be_read_again_is_not_taken_as_gone(tmp_path, cloud):
    a = make_desk(tmp_path, cloud)
    k, l_ = a.mgr.create_list('K'), a.mgr.create_list('L')
    key = '990001::fl::FLa'
    a.mgr.add_item('990001', k, note='n', fl_id='FLa')
    assert a.up()['success']
    rid = row_in(cloud, a, key, k)
    a.mgr.move_items_to_list([key], k, l_)
    a.mgr.update_item(key, note='n, and more')
    moved = []

    def website_edits_then_the_read_fails(client, req):
        if client.actor != 'A' or req.t != 'list_items':
            return None
        if req.op == 'update' and 'list_id' in (req.payload or {}) and not moved:
            moved.append(req)
            cloud.set(rid, note='website')     # the conditional move matches no row
            return None
        if moved and req.op == 'select' and ('eq', 'id', rid) in req.filters:
            cloud.rec.hook = None
            return 'raise_before'              # and reading the row again fails
        return None
    cloud.rec.hook = website_edits_then_the_read_fails
    mark = cloud.rec.mark()
    result = a.up()
    cloud.rec.hook = None
    assert moved and mark(op='insert') == []
    assert result['success'] is False and result['items_failed'] == 1
    assert cloud.row(rid)['list_id'] == cloud_id(a, k) and cloud.row(rid)['note'] == 'website'
    assert rec(a, key, '~%s' % rid)['id'] == rid   # the row is still the one to move
    mark = cloud.rec.mark()
    result = a.up()
    assert result['success'] and mark(op='insert') == []
    assert cloud.row(rid)['list_id'] == cloud_id(a, l_) and cloud.row(rid)['note'] == 'website'


# --------------------------------------------------------------------------- the pass's check before every request

class _Stopped(Exception):
    pass


def _checks_and_requests(monkeypatch, cloud, stop_at=None):
    """The pass's checks and A's requests, in order; the check raises at its stop_at-th call."""
    events = []

    def check(self):
        events.append('check')
        if stop_at is not None and events.count('check') >= stop_at:
            raise _Stopped('stopped')
    monkeypatch.setattr(lists_sync._Pass, 'check', check)
    cloud.rec.hook = lambda client, req: events.append((req.t, req.op)) if client.actor == 'A' else None
    return events


def _desk_with_a_project(tmp_path, cloud):
    a = make_desk(tmp_path, cloud)
    lid = a.mgr.create_list('L')
    assert a.mgr.update_list_project(lid, a.mgr.create_project('P'))
    a.mgr.add_item('990001', lid, note='n', fl_id='FLa')
    cloud.web.table('projects').insert({'user_id': 'u1', 'name': 'Q'}).execute()
    cloud.add(cloud.new_list('M'), '990002', fl_id='FLb')
    return a


@pytest.mark.parametrize('direction', ['upload', 'download'])
def test_every_request_of_a_pass_comes_right_after_its_check(tmp_path, cloud, monkeypatch, direction):
    # 2b-2 makes _Pass.check() raise when the user presses Stop, so every request a pass
    # makes -- the projects and lists ones too -- must come right after a check.
    a = _desk_with_a_project(tmp_path, cloud)
    events = _checks_and_requests(monkeypatch, cloud)
    result = a.up() if direction == 'upload' else a.down()
    cloud.rec.hook = None
    assert result['success']
    requests = [n for n, e in enumerate(events) if e != 'check']
    assert all(n > 0 and events[n - 1] == 'check' for n in requests), events
    assert {'projects', 'user_lists', 'list_items'} <= {events[n][0] for n in requests}


@pytest.mark.parametrize('direction', ['upload', 'download'])
def test_a_check_that_stops_the_pass_before_its_first_request_sends_nothing(tmp_path, cloud, monkeypatch,
                                                                             direction):
    a = _desk_with_a_project(tmp_path, cloud)
    before = copy.deepcopy(a.mgr.data)
    events = _checks_and_requests(monkeypatch, cloud, stop_at=1)
    result = a.up() if direction == 'upload' else a.down()
    cloud.rec.hook = None
    assert result['success'] is False
    assert events == ['check']
    if direction == 'download':
        assert a.mgr.data == before
