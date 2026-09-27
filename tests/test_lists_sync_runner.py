# -*- coding: utf-8 -*-
"""The desktop's list-sync runner (desktop/lists_sync_runner.py), without a window.

Its contract with ListsManager: one job at a time; an upload runs on a copy and is
installed (finish_upload) whatever its end, or, at a close, rebuilt from every report it
made (abandon_upload); a download is fetched, then applied, unless the sign-in it was
started under is gone. These drive the real runner over a real ListsManager and
ListsCloudSync against the scenario gate's PostgREST stand-in. Most run the runner inline
(every stage on the calling thread); the ones about a worker still inside a request run it
on its worker threads and drain its queue by hand, as its 50 ms timer would. No Qt event
loop, no network.
"""
import itertools
import os
import sys
import threading
import time

import pytest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import test_list_sync_removals as R  # noqa: E402  (its Cloud, desks and helpers)
from desktop import lists_sync_runner as runner_mod  # noqa: E402
from desktop.lists_sync_runner import ListsSyncRunner, sync_dialog_allowed  # noqa: E402
from shared import lists_sync  # noqa: E402

KEY = R.KEY


@pytest.fixture(autouse=True)
def _configured(monkeypatch):
    monkeypatch.setattr(lists_sync, 'SUPABASE_AVAILABLE', True)
    monkeypatch.setattr(lists_sync, 'SUPABASE_ANON_KEY', 'test-key')
    import genizah_core
    monkeypatch.setattr(genizah_core, 'CURRENT_LANG', 'en')


@pytest.fixture
def cloud():
    return R.Cloud()


@pytest.fixture
def desk(tmp_path, cloud, monkeypatch):
    d = R.make_desk(tmp_path, cloud)
    monkeypatch.setattr(lists_sync, '_sync_instance', d.sync)   # the manager's sync is this desk's
    return d


def threaded(mgr, **kw):
    """A runner whose stages run on worker threads, drained by the test instead of a timer."""
    r = ListsSyncRunner(mgr, inline=True, **kw)
    r._inline = False
    return r


def drain_until(r, done, timeout=10):
    """Drain the runner's queue (the timer's job) until done() or the time runs out."""
    ev = threading.Event()
    for _ in range(int(timeout / 0.01)):
        r._poll()
        if done():
            return True
        ev.wait(0.01)
    return done()


def worker_ended(r, timeout=10):
    ev = threading.Event()
    for _ in range(int(timeout / 0.01)):
        if any(m[0] == 'done' for m in list(r._results.queue)):
            return True
        ev.wait(0.01)
    return False


class Gate:
    """Holds A's first request of a kind until released; says when it was reached."""

    def __init__(self, cloud, pred):
        self.reached, self.release = threading.Event(), threading.Event()
        self.pred = pred
        cloud.rec.hook = self.hook
        self.cloud = cloud

    def hook(self, client, req):
        if client.actor == 'A' and self.pred(req):
            self.cloud.rec.hook = None
            self.reached.set()
            self.release.wait(10)
        return None


# --------------------------------------------------------------------------- the dialog gate

def test_a_sync_question_opens_only_on_a_free_shown_window():
    for visible, restoring, modal, logout, closing in itertools.product((False, True), repeat=5):
        assert sync_dialog_allowed(visible, restoring, modal, logout, closing) == (
            visible and not restoring and not modal and not logout and not closing)


def test_the_gate_needs_no_qt():
    src = open(runner_mod.__file__, encoding='utf-8').read()
    top = [ln for ln in src.splitlines() if ln.startswith(('import ', 'from '))]
    assert not [ln for ln in top if 'PyQt' in ln]


# --------------------------------------------------------------------------- one job, its end

def test_an_upload_job_installs_its_copy_and_completes_once(desk, cloud, monkeypatch):
    k = desk.mgr.create_list('K')
    desk.mgr.add_item('990001', k, note='n', fl_id='FLa')
    r = ListsSyncRunner(desk.mgr, inline=True)
    seen = {}
    real = desk.sync.sync_to_cloud

    def spy(**kw):
        seen.update(kw)
        return real(**kw)
    monkeypatch.setattr(desk.sync, 'sync_to_cloud', spy)
    done = []
    job = r.run('upload', on_done=done.append)
    assert len(done) == 1 and done[0]['upload']['success'] and job.done
    # on a copy, told of the removals made meanwhile, reporting what it records
    assert seen['data'] is not None and seen['data'] is not desk.mgr.data
    assert seen['withdrawn'] == desk.mgr.withdrawn_now and callable(seen['on_recorded'])
    assert R.rec(desk, k)['id'] == R.row_in(cloud, desk, k)       # the copy's record is the store's now
    assert r.unsent is False and r.busy is False
    r.cancel(job)                                                 # a finished job is left alone
    assert len(done) == 1


def test_a_failed_upload_leaves_its_changes_unsent_and_keeps_what_it_did(desk, cloud):
    k, l_ = desk.mgr.create_list('K'), desk.mgr.create_list('L')
    desk.mgr.add_item('990001', k, note='n', fl_id='FLa')
    desk.mgr.add_item('990002', l_, note='m', fl_id='FLb')

    def fail_the_second_lists_insert(client, req):
        if client.actor == 'A' and req.t == 'list_items' and req.op == 'insert':
            rows = req.payload if isinstance(req.payload, list) else [req.payload]
            if any(r.get('sys_id') == '990002' for r in rows):
                return 'raise_before'
        return None
    cloud.rec.hook = fail_the_second_lists_insert
    r = ListsSyncRunner(desk.mgr, inline=True)
    done = []
    r.run('upload', on_done=done.append)
    cloud.rec.hook = None
    assert done[0]['upload']['success'] is False and r.unsent is True
    assert R.rec(desk, k) is not None                             # the first list's row was kept
    assert R.item(desk, '990002::fl::FLb').get('cloud_rows') is None


@pytest.mark.parametrize('cell', ['ok', 'download-fails', 'download-snapshot-fails'])
def test_a_merge_uploads_only_after_its_download(desk, cloud, cell, monkeypatch):
    k = desk.mgr.create_list('K')
    desk.mgr.add_item('990001', k, note='n', fl_id='FLa')
    if cell == 'download-fails':                          # the fetch fails
        cloud.rec.hook = lambda client, req: 'raise_before' if client.actor == 'A' and req.t == 'projects' else None
    elif cell == 'download-snapshot-fails':               # the fetch succeeds; its apply stops at the snapshot
        real = desk.mgr.write_snapshot
        monkeypatch.setattr(desk.mgr, 'write_snapshot',
                            lambda label, payload=None: label != 'pre-download' and real(label, payload))
    r = ListsSyncRunner(desk.mgr, inline=True)
    done = []
    mark = cloud.rec.mark()
    r.run('merge', on_done=done.append)
    cloud.rec.hook = None
    (outcome,) = done
    if cell == 'ok':
        assert outcome['download']['success'] and outcome['upload']['success'] and mark(op='insert')
    else:
        assert outcome['download']['success'] is False and 'upload' not in outcome and mark(op='insert') == []
    if cell == 'download-snapshot-fails':
        assert outcome['download']['error'] == lists_sync.DOWNLOAD_BACKUP_FAILED


def test_a_stage_that_returns_no_result_ends_with_an_error_the_window_translates(desk, monkeypatch):
    from shared.genizah_translations import TRANSLATIONS
    monkeypatch.setattr(desk.mgr, 'get_cloud_lists_preview', lambda **kw: None)
    r = ListsSyncRunner(desk.mgr, inline=True)
    done = []
    r.run('preview', on_done=done.append)
    assert done[0]['preview'] == {'success': False, 'error': 'Unknown error'}
    assert 'Unknown error' in TRANSLATIONS


# --------------------------------------------------------------------------- what counts as not yet uploaded

def test_an_upload_under_way_counts_its_changes_as_unsent_until_it_succeeds(desk, cloud):
    k = desk.mgr.create_list('K')
    desk.mgr.add_item('990001', k, note='n', fl_id='FLa')
    r = threaded(desk.mgr)
    gate = Gate(cloud, lambda req: req.t == 'list_items' and req.op == 'insert')
    r.request_auto()                                      # a change: its upload takes it
    assert gate.reached.wait(10)
    assert r._dirty is False and r.unsent is True         # taken by the copy, not in the account yet
    gate.release.set()
    assert drain_until(r, lambda: not r.busy)
    assert r.unsent is False
    gate = Gate(cloud, lambda req: req.t == 'user_lists' and req.op == 'update')
    r2 = threaded(desk.mgr)
    r2.run('upload')                                      # a manual upload that took no change
    assert gate.reached.wait(10)
    assert r2.busy and r2.unsent is False
    gate.release.set()
    assert drain_until(r2, lambda: not r2.busy)


def test_an_upload_that_leaves_lists_for_the_next_one_leaves_them_unsent(desk, monkeypatch):
    desk.mgr.add_item('990001', desk.mgr.create_list('K'), note='n', fl_id='FLa')
    monkeypatch.setattr(desk.sync, 'sync_to_cloud', lambda **kw: {
        'success': True, 'lists_pushed': 1, 'items_pushed': 0, 'lists_not_uploaded': ['K'], 'complete': False})
    r = ListsSyncRunner(desk.mgr, inline=True)
    r.request_auto()
    assert r.unsent is True and r._logout_needs_upload()


@pytest.mark.parametrize('cell', ['changes-unsent', 'last-upload-failed', 'nothing-unsent', 'download-fails',
                                  'cancelled', 'during-a-sign-out'])
def test_a_download_is_followed_by_one_upload_while_changes_are_unsent(desk, cloud, cell, monkeypatch):
    """A Download sends nothing: with changes this computer holds that are not in the account, one
    automatic upload follows it (what the log-out texts promise after the next log-in)."""
    k = desk.mgr.create_list('K')
    desk.mgr.add_item('990001', k, note='n', fl_id='FLa')
    assert desk.up()['success']
    if cell != 'nothing-unsent':
        desk.mgr.add_item('990002', k, fl_id='FLb')       # made while list sync was off
    autos = []
    r = ListsSyncRunner(desk.mgr, inline=True, on_auto_done=autos.append)
    if cell in ('changes-unsent', 'download-fails', 'cancelled', 'during-a-sign-out'):
        r.mark_dirty()
    elif cell == 'last-upload-failed':
        r._last_upload_ok = False
    if cell == 'download-fails':
        cloud.rec.hook = lambda client, req: 'raise_before' if client.actor == 'A' and req.t == 'projects' else None
    if cell == 'during-a-sign-out':
        r.begin_logout(lambda outcome: None, budget_s=10)  # its own upload runs; no automatic one after
        r.mark_dirty()
    begins = []
    real = desk.mgr.begin_upload
    monkeypatch.setattr(desk.mgr, 'begin_upload', lambda: begins.append(1) or real())
    done = []
    if cell == 'cancelled':
        r = threaded(desk.mgr, on_auto_done=autos.append)
        r.mark_dirty()
        job = r.run('download', on_done=done.append)
        r.cancel(job)                                     # Cancel in the progress dialog
        assert drain_until(r, lambda: not r.busy)
        assert done[0]['cancelled'] is True and begins == [] and autos == []
        return
    r.run('download', on_done=done.append)
    cloud.rec.hook = None
    uploaded = cell in ('changes-unsent', 'last-upload-failed', 'download-fails')
    assert len(begins) == (1 if uploaded else 0)
    assert len(autos) == (1 if uploaded else 0)
    if cell in ('changes-unsent', 'last-upload-failed'):
        assert done[0]['download']['success'] and R.rec(desk, k, '990002::fl::FLb') is not None
        assert r.unsent is False
    if cell == 'nothing-unsent':
        assert r.unsent is False
    if cell == 'during-a-sign-out':
        r.allow_auto()                                    # the next log-in: its preview comes first
        r.run('preview')
        assert begins == [], 'an upload started before the log-in offered its sync choice'


# --------------------------------------------------------------------------- what lists.pkl holds after a restart

PENDING = ['removal', 'note-edit', 'tags-edit', 'new-entry', 'list-renamed', 'list-recoloured', 'move', 'new-list',
           'move-into-a-list-that-has-it']
NOTHING_TO_SEND = ['nothing', 'kept-note', 'website-removal', 'my-library', 'move-into-the-trash']
LOCAL_ID = '970012345601234567'                           # My Library: never sent


def _restart_with(desk, cloud, monkeypatch, cell):
    """Synced lists, then `cell` changed with no runner (list sync off, or the program closed before
    it uploaded), then a restart: lists.pkl read again by a new manager and sync. Returns the new
    desk and a check that the change is in the account."""
    mgr = desk.mgr
    k, m = mgr.create_list('K'), mgr.create_list('M')
    t = mgr.create_list('T')
    mgr.add_item('990001', k, note='n', tags=['t'], fl_id='FLa')
    mgr.add_item('990002', m, fl_id='FLb')
    if cell == 'move-into-a-list-that-has-it':
        mgr.add_item('990001', m, fl_id='FLa')
    assert desk.up()['success']
    r1, r2 = R.row_in(cloud, desk, k), R.row_in(cloud, desk, m, '990002::fl::FLb')
    if cell == 'kept-note':                               # both sides changed it: kept for Merge Both
        cloud.set(r1, note='n\nfrom the website')
        mgr.update_item(R.KEY, note='n\nfrom here')
        assert desk.up()['notes_kept'] == 1
    elif cell == 'website-removal':                       # waits for the user's answer
        cloud.delete_row(r2)
        assert desk.up()['web_removed']
    elif cell == 'move-into-the-trash':
        mgr.delete_list(t)
        assert desk.up()['success']
    reached = None
    if cell == 'removal':
        mgr.remove_item_from_list('990002::fl::FLb', m)
        reached = lambda: cloud.row(r2) is None                                        # noqa: E731
    elif cell == 'note-edit':
        mgr.update_item(R.KEY, note='edited offline')
        reached = lambda: cloud.row(r1)['note'] == 'edited offline'                    # noqa: E731
    elif cell == 'tags-edit':
        mgr.update_item(R.KEY, tags=['t', 'u'])
        reached = lambda: 'u' in (cloud.row(r1)['tags'] or [])                         # noqa: E731
    elif cell == 'new-entry':
        mgr.add_item('990003', k, fl_id='FLc')
        reached = lambda: bool(cloud.rows(list_id=R.cloud_id(desk, k), sys_id='990003'))  # noqa: E731
    elif cell == 'list-renamed':
        mgr.update_list(k, name='K2')
        reached = lambda: cloud.cloud_list(R.cloud_id(desk, k))['name'] == 'K2'        # noqa: E731
    elif cell == 'list-recoloured':
        mgr.update_list(k, color='#0000FF')
        reached = lambda: cloud.cloud_list(R.cloud_id(desk, k))['color'] == '#0000FF'  # noqa: E731
    elif cell == 'move':
        mgr.move_items_to_list([R.KEY], k, m)
        reached = lambda: cloud.row(r1)['list_id'] == R.cloud_id(desk, m)              # noqa: E731
    elif cell == 'move-into-a-list-that-has-it':          # only the moved row is left to go: deleted
        mgr.move_items_to_list([R.KEY], k, m)
        reached = lambda: cloud.row(r1) is None                                        # noqa: E731
    elif cell == 'new-list':
        mgr.create_list('N')
        reached = lambda: any(cl['name'] == 'N' for cl in cloud.db.tables['user_lists'])  # noqa: E731
    elif cell == 'my-library':
        mgr.add_item(LOCAL_ID, k)
    elif cell == 'move-into-the-trash':
        mgr.move_items_to_list([R.KEY], k, t)
    e = R.reopen(desk)
    monkeypatch.setattr(lists_sync, '_sync_instance', e.sync)
    return e, reached


@pytest.mark.parametrize('cell', PENDING + NOTHING_TO_SEND)
def test_changes_saved_before_a_restart_count_as_unsent(desk, cloud, cell, monkeypatch):
    """A runner made after a restart counts what lists.pkl holds for the account as unsent: a Download
    is followed by one upload that sends it, and a log-out a moment after a sync still uploads.
    What an upload would not send -- a note kept for Merge Both, a row the website removed, My
    Library, a move into a list in the Trash -- does not count."""
    e, reached = _restart_with(desk, cloud, monkeypatch, cell)
    pending = cell in PENDING
    assert e.mgr.has_unsent_changes('u1') is pending
    begins = []
    real = e.mgr.begin_upload
    monkeypatch.setattr(e.mgr, 'begin_upload', lambda: begins.append(1) or real())
    r = ListsSyncRunner(e.mgr, inline=True)
    assert r.unsent is pending
    e.sync._last_sync = time.time()                       # synced a moment ago
    assert r._logout_needs_upload() is pending
    done = []
    r.run('download', on_done=done.append)
    assert done[0]['download']['success']
    assert len(begins) == (1 if pending else 0)
    if pending:
        assert reached(), 'the change saved before the restart did not reach the account'
        assert r.unsent is False and not e.mgr.has_unsent_changes('u1')


@pytest.mark.parametrize('cell', ['removal', 'note-edit'])
def test_a_log_out_right_after_a_restart_and_a_download_still_uploads(desk, cloud, cell, monkeypatch):
    """The Download that refreshes the last-sync time is followed by an upload that fails (offline
    for a moment); a log-out within the minute still uploads what lists.pkl held."""
    e, reached = _restart_with(desk, cloud, monkeypatch, cell)
    r = ListsSyncRunner(e.mgr, inline=True)
    real_apply = e.mgr.apply_cloud_state

    def apply_then_go_offline(state):
        applied = real_apply(state)
        cloud.rec.hook = lambda client, req: 'raise_before' if client.actor == 'A' else None
        return applied
    monkeypatch.setattr(e.mgr, 'apply_cloud_state', apply_then_go_offline)
    done = []
    r.run('download', on_done=done.append)
    cloud.rec.hook = None
    assert done[0]['download']['success'] and not reached()
    logout = []
    r.begin_logout(logout.append, budget_s=10)
    assert logout[0]['upload']['success'] and logout[0].get('skipped') is None
    assert reached()


def test_seeding_again_counts_a_change_made_since_the_runner_was_made(desk, cloud):
    k = desk.mgr.create_list('K')
    desk.mgr.add_item('990001', k, note='n', fl_id='FLa')
    assert desk.up()['success']
    r = ListsSyncRunner(desk.mgr, inline=True)
    assert r.unsent is False
    desk.mgr.remove_item_from_list(R.KEY, k)              # not through the runner (list sync was off)
    assert r.unsent is False
    r.seed_unsent()                                       # what the window asks at a log-in
    assert r.unsent is True


def test_lists_that_cannot_be_read_for_it_count_as_unsent(desk, monkeypatch):
    def unreadable(user_id=None):
        raise RuntimeError('the lists could not be read')
    monkeypatch.setattr(desk.mgr, 'has_unsent_changes', unreadable)
    assert ListsSyncRunner(desk.mgr, inline=True).unsent is True


def test_only_this_accounts_pending_state_counts(desk, cloud):
    k = desk.mgr.create_list('K')
    desk.mgr.add_item('990001', k, note='n', fl_id='FLa')
    assert desk.up()['success']
    assert desk.mgr.has_unsent_changes('u1') is False
    assert desk.mgr.has_unsent_changes() is False         # the account the store was last synced with
    assert desk.mgr.has_unsent_changes('u2') is True      # another account has none of these rows
    desk.mgr.remove_item_from_list(R.KEY, k)
    assert desk.mgr.has_unsent_changes('u1') is True
    for entry in desk.mgr.data['cloud_deletes'].values():
        entry['account'] = 'u9'                           # another account's removal: it waits for that one
    desk.mgr.data['items'].clear()
    assert desk.mgr.has_unsent_changes('u1') is False


# --------------------------------------------------------------------------- what an upload that succeeded left

@pytest.mark.parametrize('left, unsent', [
    ('unchecked', True), ('waiting', True), ('lists_not_uploaded', True),
    ('notes_too_long', False), ('notes_differing', False), ('notes_kept', False), ('deferred', False)])
def test_what_a_successful_upload_left_decides_whether_its_changes_stay_unsent(desk, monkeypatch, left, unsent):
    """Left for the next upload (unchecked memberships, entries waiting for a Download, lists not
    reached): still unsent, so a log-out a moment later uploads again. Left as it is on purpose (a
    note too long for the account, notes kept for Merge Both, a move into the Trash): sent."""
    desk.mgr.add_item('990001', desk.mgr.create_list('K'), note='n', fl_id='FLa')
    value = ['K'] if left == 'lists_not_uploaded' else 1
    monkeypatch.setattr(desk.sync, 'sync_to_cloud', lambda **kw: {
        'success': True, 'lists_pushed': 1, 'items_pushed': 1, left: value, 'complete': not unsent})
    r = ListsSyncRunner(desk.mgr, inline=True)
    r.request_auto()
    desk.sync._last_sync = time.time()                    # synced a moment ago
    assert r.unsent is unsent and r._logout_needs_upload() is unsent


@pytest.mark.parametrize('cell', ['a-failed-confirmation', 'a-note-too-long'])
def test_a_real_upload_that_succeeded_is_judged_by_what_it_left(desk, cloud, cell):
    k = desk.mgr.create_list('K')
    note = R.S.LONG_FILLER if cell == 'a-note-too-long' else 'n'
    desk.mgr.add_item('990001', k, note=note, fl_id='FLa')
    assert desk.up()['success']
    r = ListsSyncRunner(desk.mgr, inline=True)
    if cell == 'a-failed-confirmation':
        w = cloud.new_list('W')
        cloud.set(R.row_in(cloud, desk, k), list_id=w)     # moved on the website: this upload must confirm it
        cloud.rec.hook = lambda client, req: ('raise_before' if client.actor == 'A' and req.t == 'list_items'
                                              and any(f[0] == 'in' for f in req.filters) else None)
    else:
        desk.mgr.update_item(R.KEY, note=note + ' and more')   # its PATCH filter is past the gateway's limit
    done = []
    r.run('upload', on_done=done.append)
    cloud.rec.hook = None
    up = done[0]['upload']
    assert up['success'] is True
    if cell == 'a-failed-confirmation':
        assert up['unchecked'] and up['complete'] is False and r.unsent is True
    else:
        assert up['notes_too_long'] == 1 and up['complete'] is True and r.unsent is False


def test_a_download_queued_behind_an_upload_keeps_the_list_state_changed_during_it(desk, cloud):
    """The runner's order: a Download queued while an upload runs goes before the follow-up upload
    that carries the edits made meanwhile. It keeps them (colour, Trash), and that upload sends them."""
    k, t = desk.mgr.create_list('K', color='#FF0000'), desk.mgr.create_list('T')
    desk.mgr.add_item('990001', k, note='n', fl_id='FLa')
    assert desk.up()['success']
    desk.mgr.add_item('990002', k, fl_id='FLb')
    gate = Gate(cloud, lambda req: req.t == 'list_items' and req.op == 'insert')
    r = threaded(desk.mgr)
    started = []
    real_start = r._start
    r._start = lambda job: started.append(job.kind) or real_start(job)
    r.request_auto()
    assert gate.reached.wait(10)
    desk.mgr.update_list(k, color='#0000FF')              # edits during the upload
    desk.mgr.delete_list(t)
    r.request_auto()                                      # folded into a follow-up upload
    done = []
    r.run('download', on_done=done.append)
    gate.release.set()
    assert drain_until(r, lambda: not r.busy and not r._auto_wanted)
    assert started == ['auto', 'download', 'auto'] and done[0]['download']['success']
    lists = desk.mgr.data['lists']
    assert lists[k]['color'] == '#0000FF' and lists[t].get('deleted_at')
    assert cloud.cloud_list(R.cloud_id(desk, k))['color'] == '#0000FF'
    assert cloud.cloud_list(R.cloud_id(desk, t)).get('deleted_at')


# --------------------------------------------------------------------------- what the progress dialog is told

def test_a_queued_merge_is_told_its_turn_and_each_half(desk, cloud):
    k, l_ = desk.mgr.create_list('K'), desk.mgr.create_list('L')
    desk.mgr.add_item('990001', k, note='n', fl_id='FLa')
    desk.mgr.add_item('990002', l_, fl_id='FLb')
    assert desk.up()['success']
    gate = Gate(cloud, lambda req: req.t == 'user_lists' and req.op == 'update')
    r = threaded(desk.mgr)
    r.run('upload')
    assert gate.reached.wait(10)
    desk.mgr.remove_item_from_list('990002::fl::FLb', l_)  # after that upload's copy: the Merge sends it
    told = []
    r.run('merge', on_progress=lambda *a: told.append(a))
    assert told == [('waiting', 0, 0)]
    gate.release.set()
    assert drain_until(r, lambda: not r.busy)
    stages = [t[0] for t in told]
    assert stages[:2] == ['waiting', 'start']
    first_up = stages.index('upload')
    assert set(stages[2:first_up]) == {'download'} and 'download' not in stages[first_up:]
    lists = 3                                             # General, K and L
    assert told[first_up] == ('upload', 0, lists), 'the upload half did not say so as it began'
    assert [t for t in told if t[0] == 'upload'] == [('upload', n, lists) for n in range(lists + 1)]
    assert told[-1] == ('deletes', 0, 0)


def test_a_preview_job_reads_the_cloud_lists(desk, cloud):
    cloud.add(cloud.new_list('Web'), '990001', fl_id='FLa')
    r = ListsSyncRunner(desk.mgr, inline=True)
    done = []
    r.run('preview', on_done=done.append)
    assert done[0]['preview']['success'] and [x['name'] for x in done[0]['preview']['lists']] == ['Web']


def test_a_preview_from_before_a_sign_out_is_not_delivered(desk, cloud):
    cloud.add(cloud.new_list('Web'), '990001', fl_id='FLa')
    r = threaded(desk.mgr)
    done = []
    r.run('preview', on_done=done.append)
    assert worker_ended(r)                                # read; its answer waits in the queue
    r.invalidate_auth()
    assert drain_until(r, lambda: bool(done))
    assert done == [{'kind': 'preview', 'cancelled': True, 'stale': True}]


@pytest.mark.parametrize('step', ['start', 'apply', 'install'])
def test_a_step_that_raises_completes_its_job_and_the_next_one_runs(desk, step):
    desk.mgr.add_item('990001', desk.mgr.create_list('K'), note='n', fl_id='FLa')
    r = ListsSyncRunner(desk.mgr, inline=True)
    name = {'start': 'remembered_row_ids', 'apply': 'apply_cloud_state', 'install': 'finish_upload'}[step]

    def boom(*_):
        raise RuntimeError('the step failed')
    setattr(desk.mgr, name, boom)
    done = []
    r.run('upload' if step == 'install' else 'download', on_done=done.append)
    assert done[0]['error'] == 'the step failed' and r.busy is False
    delattr(desk.mgr, name)
    r.run('upload', on_done=done.append)
    assert len(done) == 2 and done[1]['upload']['success']


# --------------------------------------------------------------------------- one at a time, on workers

def test_jobs_run_one_after_another_and_automatic_uploads_fold_into_one(desk, cloud, monkeypatch):
    k = desk.mgr.create_list('K')
    desk.mgr.add_item('990001', k, note='n', fl_id='FLa')
    gate = Gate(cloud, lambda req: req.t == 'list_items' and req.op == 'insert')
    r = threaded(desk.mgr)
    uploads = []
    real_begin = desk.mgr.begin_upload
    monkeypatch.setattr(desk.mgr, 'begin_upload', lambda: uploads.append(1) or real_begin())
    done = []
    r.run('upload', on_done=lambda o: done.append(('first', o)))
    assert gate.reached.wait(10)
    waiting = []
    second = r.run('download', on_done=lambda o: done.append(('second', o)), on_progress=lambda *a: waiting.append(a))
    assert waiting[0][0] == 'waiting' and not second.started
    for _ in range(3):
        r.request_auto()                                  # three edits while it runs
    third = r.run('upload', on_done=lambda o: done.append(('third', o)))
    r.cancel(third)                                       # a queued job ends at once
    assert done == [('third', {'kind': 'upload', 'cancelled': True})]
    gate.release.set()
    assert drain_until(r, lambda: not r.busy)
    order = [name for name, _ in done]
    assert order == ['third', 'first', 'second'] and done[1][1]['upload']['success']
    assert len(uploads) == 2 and r._auto_wanted is False  # the three requests became one follow-up upload


@pytest.mark.parametrize('cell', ['signed-out-during-the-fetch', 'signed-out-after-the-fetch',
                                  'another-account-after-the-fetch'])
def test_a_download_whose_sign_in_ended_meanwhile_changes_nothing(desk, cloud, cell):
    k = desk.mgr.create_list('K')
    desk.mgr.add_item('990001', k, note='n', fl_id='FLa')
    assert desk.up()['success']
    cloud.add(R.cloud_id(desk, k), '990002', fl_id='FLb', note='web')
    r = threaded(desk.mgr)
    done = []
    if cell == 'signed-out-during-the-fetch':
        gate = Gate(cloud, lambda req: req.t == 'list_items' and req.op == 'select')
        r.run('download', on_done=done.append)
        assert gate.reached.wait(10)
        r.invalidate_auth()                               # the worker stops before its next request
        gate.release.set()
    else:
        r.run('download', on_done=done.append)
        assert worker_ended(r)                            # fetched; its state waits in the queue
        if cell == 'signed-out-after-the-fetch':
            r.invalidate_auth()
        else:
            R.sign_in(desk, 'u2')                         # the same sign-in epoch, another account
    before = set(desk.mgr.data['items'])
    assert drain_until(r, lambda: bool(done))
    assert done[0]['cancelled'] is True and 'download' not in done[0]
    if cell != 'signed-out-during-the-fetch':
        assert done[0]['stale'] is True
    assert set(desk.mgr.data['items']) == before and '990002::fl::FLb' not in before
    assert not os.path.exists(desk.mgr.LISTS_FILE + '.pre-download')


def test_a_download_cancelled_after_its_fetch_changes_nothing(desk, cloud):
    k = desk.mgr.create_list('K')
    desk.mgr.add_item('990001', k, note='n', fl_id='FLa')
    assert desk.up()['success']
    cloud.add(R.cloud_id(desk, k), '990002', fl_id='FLb', note='web')
    r = threaded(desk.mgr)
    done = []
    job = r.run('download', on_done=done.append)
    assert worker_ended(r)                                # its last request answered; the state waits in the queue
    r.cancel(job)                                         # Cancel in the progress dialog, just then
    before = set(desk.mgr.data['items'])
    assert drain_until(r, lambda: bool(done))
    assert done[0]['cancelled'] is True and 'download' not in done[0] and not done[0].get('stale')
    assert set(desk.mgr.data['items']) == before and '990002::fl::FLb' not in before
    assert not os.path.exists(desk.mgr.LISTS_FILE + '.pre-download')


def test_a_stale_upload_is_still_installed(desk, cloud):
    k = desk.mgr.create_list('K')
    desk.mgr.add_item('990001', k, note='n', fl_id='FLa')
    desk.mgr.add_item('990002', desk.mgr.create_list('Z'), fl_id='FLz')
    gate = Gate(cloud, lambda req: req.t == 'list_items' and req.op == 'insert')
    r = threaded(desk.mgr)
    done = []
    r.run('upload', on_done=done.append)
    assert gate.reached.wait(10)
    r.invalidate_auth()                                   # the worker stops before its next request
    gate.release.set()
    assert drain_until(r, lambda: bool(done))
    assert done[0]['upload']['stopped'] is True
    assert R.rec(desk, k) is not None                     # what it did before it stopped is kept
    assert r.unsent is True


@pytest.mark.parametrize('how', ['cancel', 'sign-out-deadline'])
def test_a_running_upload_stops_at_cancel_or_the_sign_out_deadline_and_is_installed(desk, cloud, how):
    k = desk.mgr.create_list('K')
    desk.mgr.add_item('990001', k, note='n', fl_id='FLa')
    desk.mgr.add_item('990002', desk.mgr.create_list('Z'), fl_id='FLz')
    gate = Gate(cloud, lambda req: req.t == 'list_items' and req.op == 'insert')
    r = threaded(desk.mgr)
    done, logout = [], []
    job = r.run('upload', on_done=done.append)
    assert gate.reached.wait(10)
    if how == 'cancel':
        r.cancel(job)
    else:
        r.begin_logout(logout.append, budget_s=0.01)      # the running job is held to the same deadline
        assert drain_until(r, job.past_deadline, timeout=2)
    gate.release.set()                                    # K's insert goes; the stop comes before Z's
    assert drain_until(r, lambda: bool(done) and (how == 'cancel' or bool(logout)))
    assert done[0]['upload']['stopped'] is True and done[0]['cancelled'] is (how == 'cancel')
    if how != 'cancel':
        assert done[0]['deadline'] is True and logout[0]['deadline'] is True
    assert R.rec(desk, k) is not None and R.item(desk, '990002::fl::FLz').get('cloud_rows') is None
    assert r.unsent is True


def test_an_automatic_upload_from_before_a_sign_out_reports_nothing(desk, cloud):
    k = desk.mgr.create_list('K')
    desk.mgr.add_item('990001', k, note='n', fl_id='FLa')
    autos = []
    r = threaded(desk.mgr, on_auto_done=autos.append)
    r.request_auto()
    assert drain_until(r, lambda: not r.busy) and len(autos) == 1   # an ordinary one is reported
    desk.mgr.add_item('990002', k, fl_id='FLb')
    desk.mgr.add_item('990003', desk.mgr.create_list('Z'), fl_id='FLz')
    gate = Gate(cloud, lambda req: req.t == 'list_items' and req.op == 'insert')
    r.request_auto()
    assert gate.reached.wait(10)
    r.invalidate_auth()
    gate.release.set()
    assert drain_until(r, lambda: not r.busy)
    assert len(autos) == 1 and R.rec(desk, k, '990002::fl::FLb') is not None   # installed, not reported


def test_a_sign_out_uploads_once_without_page_backfill_or_skips_when_nothing_is_left(desk, cloud, monkeypatch):
    r = ListsSyncRunner(desk.mgr, inline=True)
    desk.mgr.add_item('990001', desk.mgr.create_list('K'), note='n', fl_id='FLa')
    r.request_auto()                                      # uploaded now (inline), nothing left after it
    assert r.unsent is False
    done = []
    job = r.begin_logout(done.append, budget_s=10)
    assert done == [{'kind': 'logout', 'cancelled': False, 'skipped': True}] and job.done
    r.request_auto()                                      # sign-out pending: no automatic upload, but marked
    assert r.unsent is True
    r2 = ListsSyncRunner(desk.mgr, inline=True)
    r2.mark_dirty()
    seen = {}
    real = desk.sync.sync_to_cloud

    def spy(**kw):
        seen.update(kw)
        return real(**kw)
    monkeypatch.setattr(desk.sync, 'sync_to_cloud', spy)
    done = []
    r2.begin_logout(done.append, budget_s=10)
    assert done[0]['upload']['success'] and seen['backfill_pages'] is False and seen['data'] is not None


def test_a_sign_out_during_an_upload_that_fails_uploads_again(desk, cloud):
    k = desk.mgr.create_list('K')
    desk.mgr.add_item('990001', k, note='n', fl_id='FLa')
    r = ListsSyncRunner(desk.mgr, inline=True)
    r.request_auto()
    assert r.unsent is False                              # synced a moment ago: a sign-out now needs no upload
    desk.mgr.add_item('990002', k, fl_id='FLb')
    gate = Gate(cloud, lambda req: req.t == 'list_items' and req.op == 'insert')
    r._inline = False

    def held_then_failed(client, req):
        if client.actor == 'A' and req.t == 'list_items' and req.op == 'insert':
            gate.hook(client, req)                        # (clears this hook)
            return 'raise_before'
        return None
    cloud.rec.hook = held_then_failed
    r.request_auto()                                      # an automatic upload, held in its insert
    assert gate.reached.wait(10)
    done = []
    job = r.begin_logout(done.append, budget_s=10)
    assert not job.done                                   # it waits for the upload under way
    gate.release.set()
    assert drain_until(r, lambda: bool(done))
    assert done[0]['upload']['success'] and done[0].get('skipped') is None
    assert R.rec(desk, k, '990002::fl::FLb') is not None


# --------------------------------------------------------------------------- the close

def test_a_close_completes_the_queued_jobs_too(desk, cloud):
    """shutdown(): the running job and every queued one hear of the close, once each; none starts."""
    desk.mgr.add_item('990001', desk.mgr.create_list('K'), note='n', fl_id='FLa')
    gate = Gate(cloud, lambda req: req.t == 'list_items' and req.op == 'insert')
    r = threaded(desk.mgr)
    done = []
    r.run('upload', on_done=lambda o: done.append(('running', o)))
    assert gate.reached.wait(10)
    r.run('download', on_done=lambda o: done.append(('queued-download', o)))
    r.run('merge', on_done=lambda o: done.append(('queued-merge', o)))
    r.shutdown()
    gate.release.set()
    closed = {'cancelled': True, 'shutdown': True}
    assert [name for name, _ in done] == ['running', 'queued-download', 'queued-merge']
    assert all({k: o[k] for k in closed} == closed for _, o in done)
    assert r.busy is False and r.run('upload') is None


@pytest.mark.parametrize('cell', ['download-past-the-sign-out-deadline', 'merge-upload-half-another-account',
                                  'merge-upload-half-new-sign-in'])
def test_a_late_result_from_before_a_sign_out_changes_nothing(desk, cloud, cell, monkeypatch):
    """A download fetched in time but used past the sign-out's deadline is not applied; a Merge whose
    sign-in ended while its download was applied never starts its upload."""
    k = desk.mgr.create_list('K')
    desk.mgr.add_item('990001', k, note='n', fl_id='FLa')
    assert desk.up()['success']
    cloud.add(R.cloud_id(desk, k), '990002', fl_id='FLb', note='web')
    begins = []
    real_begin = desk.mgr.begin_upload
    monkeypatch.setattr(desk.mgr, 'begin_upload', lambda: begins.append(1) or real_begin())
    r = threaded(desk.mgr)
    done = []
    if cell == 'download-past-the-sign-out-deadline':
        r.run('download', on_done=done.append)
        assert worker_ended(r)                            # fetched in time; its state waits in the queue
        r.begin_logout(lambda outcome: None, budget_s=0.01)
        threading.Event().wait(0.05)                      # the deadline passes before the UI thread reads it
        before = set(desk.mgr.data['items'])
        assert drain_until(r, lambda: bool(done))
        assert done[0]['cancelled'] is True and done[0]['stale'] is True and 'download' not in done[0]
        assert set(desk.mgr.data['items']) == before and '990002::fl::FLb' not in before
        assert not os.path.exists(desk.mgr.LISTS_FILE + '.pre-download')
        return
    real_apply = desk.mgr.apply_cloud_state

    def apply_then_the_sign_in_ends(state):
        applied = real_apply(state)
        if cell == 'merge-upload-half-another-account':
            R.sign_in(desk, 'u2')                         # the same sign-in epoch, another account
        else:
            r.invalidate_auth()
        return applied
    monkeypatch.setattr(desk.mgr, 'apply_cloud_state', apply_then_the_sign_in_ends)
    mark = cloud.rec.mark()
    r.run('merge', on_done=done.append)
    assert drain_until(r, lambda: bool(done))
    assert done[0]['download']['success'] and done[0]['stale'] is True and 'upload' not in done[0]
    assert begins == [] and mark(op='insert') == [] and mark(op='update') == []


def test_a_close_installs_a_finished_upload_whose_result_was_not_drained_yet(desk, monkeypatch):
    k = desk.mgr.create_list('K')
    desk.mgr.add_item('990001', k, note='n', fl_id='FLa')
    r = threaded(desk.mgr)
    calls = []
    monkeypatch.setattr(desk.mgr, 'abandon_upload', lambda *a: calls.append('abandon'))
    done = []
    r.run('upload', on_done=done.append)
    assert worker_ended(r)                                # the worker ended; its result waits in the queue
    r.shutdown()
    assert calls == [] and R.rec(desk, k) is not None
    assert R.saved(desk)['items'][KEY]['cloud_rows'][k] == R.rec(desk, k)
    assert done == [{'kind': 'upload', 'cancelled': True, 'shutdown': True}]
    assert r.run('upload') is None and r.begin_logout(done.append, 10) is None


@pytest.mark.parametrize('drained', [True, False], ids=['drained-by-a-timer-tick', 'left-in-the-queue'])
def test_a_close_keeps_what_a_stuck_upload_reported_drained_or_not(desk, cloud, drained):
    """The report of a completed move, drained by an earlier timer tick or still in the queue; the worker
    then stuck in a request; the entry removed from its new list on the UI thread; the close: the removal
    names the list the row is in."""
    k, l_, m, z = (desk.mgr.create_list(x) for x in ('K', 'L', 'M', 'Z'))
    desk.mgr.add_item('990001', k, note='n', fl_id='FLa')
    desk.mgr.add_item('990001', m, fl_id='FLa')
    assert desk.up()['success']
    r101 = R.row_in(cloud, desk, k)
    desk.mgr.move_items_to_list([KEY], k, l_)
    desk.mgr.add_item('990005', z, fl_id='FLz')           # an insert after the move: where the worker sticks
    gate = Gate(cloud, lambda req: req.t == 'list_items' and req.op == 'insert')
    r = threaded(desk.mgr)
    done = []
    job = r.run('upload', on_done=done.append)
    assert gate.reached.wait(10)
    if drained:
        r._poll()                                         # a timer tick drains the move's report
        assert [rep for rep in job.recorded if rep[0] == 'row' and rep[3] == r101]
    else:
        assert job.recorded == [] and [m_ for m_ in list(r._results.queue) if m_[0] == 'recorded']
    desk.mgr.remove_item_from_list(KEY, l_)               # M survives
    r.shutdown()                                          # never waits for the worker
    assert done[0].get('shutdown') is True
    gate.release.set()
    assert worker_ended(r)                                # (unread: the runner is closed)
    on_disk = R.saved(desk)
    assert on_disk['cloud_deletes'][str(r101)]['list'] == R.cloud_id(desk, l_)
    assert cloud.row(r101)['list_id'] == R.cloud_id(desk, l_)
