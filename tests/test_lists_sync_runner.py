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
