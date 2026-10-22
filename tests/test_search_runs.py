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
    def __init__(self, done=False):
        self._done = done

    def done(self):
        return self._done


def test_a_run_is_kept_for_its_browser_only():
    run = sr.start_run('browser-a', 'code1', {'last_run': {'query': 'q'}})
    assert sr.get_run('browser-a', 'code1') is run
    assert sr.get_run('browser-b', 'code1') is None   # another visitor's address
    assert sr.get_run('browser-a', 'code2') is None
    assert sr.get_run(None, 'code1') is None and sr.get_run('browser-a', None) is None


def test_running_done_and_failed():
    run = sr.start_run('b', 'c', {})
    assert not run.running and run.failed            # no job yet: nothing to wait for
    run.job = _Job(done=False)
    assert run.running and not run.failed
    run.job = _Job(done=True)                        # ended without a snapshot
    assert not run.running and run.failed
    sr.finish_run('b', 'c', {'results': [1]})
    assert run.done and not run.running and not run.failed
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


def test_a_real_future_drives_running():
    async def scenario():
        loop = asyncio.get_running_loop()
        run = sr.start_run('b', 'c', {})
        run.job = loop.create_future()
        assert run.running
        run.job.set_result([])
        assert not run.running and run.failed     # done, but no page saved it
    asyncio.run(scenario())
