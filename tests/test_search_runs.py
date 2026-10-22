# -*- coding: utf-8 -*-
"""web/search_runs.py: what a search page left, kept per browser and run code for Back."""

from __future__ import annotations

import asyncio

import pytest

from web import search_runs as sr


@pytest.fixture(autouse=True)
def _empty():
    sr.clear()
    yield
    sr.clear()


class _Job:
    """A job as SearchRun reads it: done(), and how it ended."""
    def __init__(self, done=False, error=None, cancelled=False):
        self._done, self._error, self._cancelled = done, error, cancelled

    def done(self):
        return self._done

    def cancelled(self):
        return self._cancelled

    def exception(self):
        return self._error


def test_a_run_is_kept_for_its_browser_only():
    run = sr.start_run('browser-a', 'code1', {'last_run': {'query': 'q'}})
    assert sr.get_run('browser-a', 'code1') is run
    assert sr.get_run('browser-b', 'code1') is None   # another visitor's address
    assert sr.get_run('browser-a', 'code2') is None
    assert sr.get_run(None, 'code1') is None and sr.get_run('browser-a', None) is None


def test_running_attachable_done_and_failed():
    run = sr.start_run('b', 'c', {})
    # Preparing (no job yet): a Back follows it; the first page to reach the job
    # starts it (Codex, PR #436, round 9).
    assert not run.running and run.attachable and not run.failed
    run.job = _Job(done=False)
    assert run.running and run.attachable and not run.failed
    # Ended with a result no page has saved yet: a Back takes it instead of
    # searching again (Codex, PR #436, round 7).
    run.job = _Job(done=True)
    assert not run.running and run.attachable and not run.failed
    run.job = _Job(done=True, error=RuntimeError('worker died'))
    assert not run.attachable and run.failed
    run.job = _Job(done=True, cancelled=True)
    assert not run.attachable and run.failed
    run.job = _Job(done=True)
    run.cancelled = True                                # Stop: its rows are not the list
    assert not run.attachable and run.failed
    sr.finish_run('b', 'c', {'results': [1]})
    assert run.done and not run.running and not run.attachable and not run.failed
    assert sr.get_run('b', 'c').snapshot == {'results': [1]}


def test_finishing_saves_the_snapshot_and_later_changes_merge_in():
    sr.start_run('b', 'c', {'last_run': {'query': 'q'}})
    assert sr.update_run('b', 'c', pgp_filter='only_pgp') is False   # not run yet: nothing to change
    sr.finish_run('b', 'c', {'results': [1], 'pgp_filter': 'all'})
    assert sr.update_run('b', 'c', pgp_filter='only_pgp') is True
    assert sr.get_run('b', 'c').snapshot == {'results': [1], 'pgp_filter': 'only_pgp'}
    assert sr.update_run('other', 'c', pgp_filter='x') is False


def test_finishing_a_run_that_was_not_kept_keeps_it():
    """A restart forgot the run, or it was pruned: saving it again brings it back."""
    run = sr.finish_run('b', 'c', {'results': []})
    assert run.done and sr.get_run('b', 'c') is run


def test_snapshots_are_copies():
    snapshot = {'results': [1]}
    run = sr.start_run('b', 'c', snapshot)
    snapshot['results'] = [2]
    assert run.snapshot == {'results': [1]}


def test_each_browser_keeps_its_newest_runs(monkeypatch):
    monkeypatch.setattr(sr, 'PER_BROWSER', 2)
    for i in range(4):
        sr.finish_run('b', f'c{i}', {'n': i})
    sr.finish_run('other', 'x', {})
    assert [sr.get_run('b', f'c{i}') is not None for i in range(4)] == [False, False, True, True]
    assert sr.get_run('other', 'x') is not None


def test_a_running_search_outlives_finished_ones(monkeypatch):
    monkeypatch.setattr(sr, 'PER_BROWSER', 1)
    running = sr.start_run('b', 'live', {})
    running.job = _Job(done=False)
    sr.finish_run('b', 'later', {})
    assert sr.get_run('b', 'live') is running        # the finished one went first
    assert sr.get_run('b', 'later') is None


def test_the_total_is_bounded(monkeypatch):
    monkeypatch.setattr(sr, 'MAX_RUNS', 3)
    for i in range(5):
        sr.finish_run(f'b{i}', 'c', {})
    kept = [sr.get_run(f'b{i}', 'c') is not None for i in range(5)]
    assert kept == [False, False, True, True, True]


def test_finished_runs_expire_but_running_ones_do_not(monkeypatch):
    sr.finish_run('b', 'old', {})
    running = sr.start_run('b', 'live', {})
    running.job = _Job(done=False)
    later = sr._runs[('b', 'old')].touched + sr.KEEP_SECONDS + 1
    sr._prune(now=later)
    assert ('b', 'old') not in sr._runs
    assert ('b', 'live') in sr._runs


def test_codes_are_random_and_url_safe():
    codes = {sr.new_stamp() for _ in range(200)}
    assert len(codes) == 200
    assert all(c and all(ch.isalnum() or ch in '-_' for ch in c) for c in codes)


def test_real_futures_drive_running_and_attachable():
    async def scenario():
        loop = asyncio.get_running_loop()
        run = sr.start_run('b', 'c', {})
        run.job = loop.create_future()
        assert run.running and run.attachable
        run.job.set_result([])
        assert not run.running and run.attachable     # done, no page saved it yet

        failed = sr.start_run('b', 'd', {})
        failed.job = loop.create_future()
        failed.job.set_exception(RuntimeError('boom'))
        assert failed.failed and not failed.attachable
        failed.job.exception()                        # retrieved: no "never retrieved" warning

        gone = sr.start_run('b', 'e', {})
        gone.job = loop.create_future()
        gone.job.cancel()
        assert gone.failed and not gone.attachable
    asyncio.run(scenario())


def test_a_saved_run_lets_go_of_its_job_and_page():
    """Codex round 8 (P1): the finished job holds the full rows (a Task keeps its
    result) and the owner its page; only the compact snapshot stays for 30 minutes."""
    run = sr.start_run('b', 'c', {})
    run.job, run.owner, run.watchers = _Job(done=True), object(), [object()]
    run.cutoff = {'capped': True}
    run.preview_rows = [{'display': {'id': 'early'}}]
    sr.finish_run('b', 'c', {'results': [1]})
    assert run.job is None and run.owner is None and run.watchers == []
    assert run.preview_rows is None
    assert run.cutoff == {'capped': True}       # a page still finishing it reads these
    assert run.done and not run.attachable and not run.failed


def test_a_scoped_run_is_never_followed():
    """Codex round 10: a search within, or a visual-similarity set, is a scope the
    address does not carry; a page reached by Back could not rebuild it."""
    run = sr.start_run('b', 'c', {}, scoped=True)
    assert not run.attachable
    run.job = _Job(done=False)
    assert run.running and not run.attachable


def test_a_discarded_run_is_gone():
    sr.start_run('b', 'c', {})
    sr.discard_run('b', 'c')
    assert sr.get_run('b', 'c') is None
    sr.discard_run('b', 'missing')        # nothing to forget: no error
    sr.discard_run(None, None)
