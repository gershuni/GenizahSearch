# -*- coding: utf-8 -*-
"""Tests for web/gc_freeze.py -- the one-time gc.freeze() after the startup loaders.

Startup leaves 0.6-0.84 M tracked objects in the web process and every full
collection walks them on the event loop (0.25-0.53 s stalls, measured
2026-10-20). The behaviours pinned here:

  * on by default; GENIZAH_GC_FREEZE=0/false/no/off disables and touches nothing;
  * the freeze WAITS for the MetadataManager threads, the FL-ID build (through
    the IsolatedEngine wrapper) and the extra task, then collects and freezes
    exactly once, in that order;
  * the wait is bounded: on timeout it freezes anyway and names the loader;
  * it is a singleton and never raises;
  * initialize_engine schedules it only when _init_sync succeeded (a
    run.io_bound that returns None is a failure, tests/test_web_io_bound_none.py);
  * /_internal/objgraph reports the freeze count and can unfreeze/refreeze,
    because frozen objects are invisible to gc.get_objects() AND
    gc.get_referrers() (the real-gc sanity test at the end shows the premise).

The freeze and collect are injected recorders (tests/test_malloc_trim.py shape);
only the sanity test and the objgraph actions touch the real gc.
"""
from __future__ import annotations

import asyncio
import gc
import json
import os
import threading
from types import SimpleNamespace

import pytest

os.environ.setdefault('GENIZAH_STORAGE_SECRET', 'gc-freeze-test-secret-0123456789abcdef')

from web import gc_freeze  # noqa: E402


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    for var in (
        'GENIZAH_GC_FREEZE',
        'GENIZAH_GC_FREEZE_MAX_WAIT_SECONDS',
        'GENIZAH_GC_FREEZE_POLL_SECONDS',
    ):
        monkeypatch.delenv(var, raising=False)
    # A tiny poll keeps the real-sleep tests fast (floor is 10 ms).
    monkeypatch.setenv('GENIZAH_GC_FREEZE_POLL_SECONDS', '0.01')
    gc_freeze.reset_stats()
    yield
    gc_freeze.reset_stats()


@pytest.fixture
def registered(monkeypatch):
    seen = {}
    monkeypatch.setattr(
        'web.api.register_runtime_cache_stats',
        lambda name, provider: seen.update({name: provider}),
    )
    return seen


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------

class FakeMeta:
    """MetadataManager stand-in: finished once ``is_finished()`` says so."""

    def __init__(self, is_finished):
        self._is_finished = is_finished

    def background_loading_finished(self):
        return self._is_finished()


class FakeEngine:
    def __init__(self, building=False):
        self._fl_id_index_building = building


def _finished_state():
    return SimpleNamespace(meta_mgr=FakeMeta(lambda: True),
                           searcher=SimpleNamespace(_engine=FakeEngine(False)))


def _never_finishing_state():
    return SimpleNamespace(meta_mgr=FakeMeta(lambda: False),
                           searcher=SimpleNamespace(_engine=FakeEngine(False)))


def _recorders(state):
    """collect/freeze recorders that note whether the loaders were done when called."""
    events = []

    def collect():
        events.append(('collect', gc_freeze.loaders_finished(state)))
        return 0

    def freeze():
        events.append(('freeze', gc_freeze.loaders_finished(state)))

    return events, collect, freeze


def _run(coro):
    return asyncio.run(coro)


# ---------------------------------------------------------------------------
# Env handling
# ---------------------------------------------------------------------------

def test_enabled_by_default():
    assert gc_freeze.gc_freeze_enabled() is True
    assert gc_freeze.max_wait_seconds() == 180.0


def test_poll_default_is_one_second(monkeypatch):
    monkeypatch.delenv('GENIZAH_GC_FREEZE_POLL_SECONDS', raising=False)
    assert gc_freeze.poll_seconds() == 1.0


@pytest.mark.parametrize('raw', ['0', 'false', 'no', 'off', 'FALSE', ' Off '])
def test_flag_off_spellings(monkeypatch, raw):
    monkeypatch.setenv('GENIZAH_GC_FREEZE', raw)
    assert gc_freeze.gc_freeze_enabled() is False


@pytest.mark.parametrize('raw', ['1', 'true', 'yes', 'on', 'TRUE', ''])
def test_flag_on_spellings(monkeypatch, raw):
    monkeypatch.setenv('GENIZAH_GC_FREEZE', raw)
    assert gc_freeze.gc_freeze_enabled() is True


def test_bad_numbers_fall_back_and_negatives_clamp(monkeypatch):
    monkeypatch.setenv('GENIZAH_GC_FREEZE_MAX_WAIT_SECONDS', 'soon')
    assert gc_freeze.max_wait_seconds() == 180.0
    monkeypatch.setenv('GENIZAH_GC_FREEZE_MAX_WAIT_SECONDS', '-5')
    assert gc_freeze.max_wait_seconds() == 0.0
    monkeypatch.setenv('GENIZAH_GC_FREEZE_POLL_SECONDS', '0')
    assert gc_freeze.poll_seconds() == 0.01
    monkeypatch.setenv('GENIZAH_GC_FREEZE_MAX_WAIT_SECONDS', '12.5')
    assert gc_freeze.max_wait_seconds() == 12.5


# ---------------------------------------------------------------------------
# Which loaders are pending
# ---------------------------------------------------------------------------

def test_pending_loaders_names_each_loader():
    assert gc_freeze.pending_loaders(SimpleNamespace(meta_mgr=None, searcher=None)) == ['MetadataManager']
    bare = SimpleNamespace(meta_mgr=FakeMeta(lambda: False), searcher=FakeEngine(True))
    assert gc_freeze.pending_loaders(bare) == [
        'MetadataManager.start_background_loading', 'SearchEngine FL-ID index build',
    ]
    # The engine may be wrapped as IsolatedEngine: the flag is read through ._engine.
    wrapped = SimpleNamespace(meta_mgr=FakeMeta(lambda: True),
                              searcher=SimpleNamespace(_engine=FakeEngine(True)))
    assert gc_freeze.pending_loaders(wrapped) == ['SearchEngine FL-ID index build']
    assert gc_freeze.loaders_finished(wrapped) is False
    assert gc_freeze.loaders_finished(_finished_state()) is True
    # A MetadataManager without the method (an older object) does not block.
    assert gc_freeze.loaders_finished(SimpleNamespace(meta_mgr=object(), searcher=None)) is True


# ---------------------------------------------------------------------------
# The freeze itself
# ---------------------------------------------------------------------------

def test_freeze_waits_for_the_loaders(capsys):
    polls = {'n': 0}
    engine = FakeEngine(building=True)
    state = SimpleNamespace(
        meta_mgr=FakeMeta(lambda: polls['n'] >= 3),      # done after 3 polls
        searcher=SimpleNamespace(_engine=engine),         # wrapped, done one poll later
    )

    async def sleep(seconds):
        polls['n'] += 1
        engine._fl_id_index_building = polls['n'] < 4

    events, collect, freeze = _recorders(state)
    assert _run(gc_freeze.freeze_after_loaders(state, collect=collect, freeze=freeze, sleep=sleep)) is True
    assert events == [('collect', True), ('freeze', True)]
    assert polls['n'] == 4
    stats = gc_freeze.stats()
    assert stats['enabled'] is True
    assert stats['loaders_finished'] is True
    assert stats['pending_loaders'] == []
    assert isinstance(stats['frozen_objects'], int)
    assert stats['collect_ms'] is not None and stats['frozen_at_utc']
    out = capsys.readouterr().out
    assert '[init] gc.freeze:' in out and 'after the loaders finished' in out
    assert 'STILL RUNNING' not in out


def test_freeze_waits_for_the_extra_task_and_survives_its_failure():
    order = []

    async def main():
        state = _finished_state()
        gate = asyncio.Event()

        async def prewarm():
            await gate.wait()
            order.append('prewarm')

        async def failing():
            raise RuntimeError('prewarm failed')

        tasks = (asyncio.create_task(prewarm()), asyncio.create_task(failing()))

        async def release():
            await asyncio.sleep(0.02)
            gate.set()

        asyncio.create_task(release())
        return await gc_freeze.freeze_after_loaders(
            state, extra_tasks=tasks,
            collect=lambda: order.append('collect'), freeze=lambda: order.append('freeze'),
        )

    assert _run(main()) is True
    assert order == ['prewarm', 'collect', 'freeze']


def test_timeout_freezes_anyway_and_names_the_loader(monkeypatch, capsys):
    monkeypatch.setenv('GENIZAH_GC_FREEZE_MAX_WAIT_SECONDS', '0.05')
    state = _never_finishing_state()
    events, collect, freeze = _recorders(state)
    assert _run(gc_freeze.freeze_after_loaders(state, collect=collect, freeze=freeze)) is True
    assert events == [('collect', False), ('freeze', False)]
    stats = gc_freeze.stats()
    assert stats['loaders_finished'] is False
    assert stats['pending_loaders'] == ['MetadataManager.start_background_loading']
    assert stats['waited_seconds'] >= 0.05
    out = capsys.readouterr().out
    assert 'STILL RUNNING' in out and 'MetadataManager.start_background_loading' in out


def test_a_wedged_extra_task_is_bounded_by_the_same_budget(monkeypatch, capsys):
    """A prewarm whose io_bound never returns must not defer the freeze forever:
    the loaders and the extra tasks share GENIZAH_GC_FREEZE_MAX_WAIT_SECONDS."""
    monkeypatch.setenv('GENIZAH_GC_FREEZE_MAX_WAIT_SECONDS', '0.05')
    order = []

    async def main():
        never = asyncio.Event()
        wedged = asyncio.create_task(never.wait(), name='fjms-prewarm')
        try:
            return await asyncio.wait_for(gc_freeze.freeze_after_loaders(
                _finished_state(), extra_tasks=(wedged,),
                collect=lambda: order.append('collect'), freeze=lambda: order.append('freeze'),
            ), timeout=5.0)
        finally:
            wedged.cancel()

    assert _run(main()) is True
    assert order == ['collect', 'freeze']
    assert gc_freeze.stats()['pending_loaders'] == ['startup task fjms-prewarm']
    assert 'STILL RUNNING' in capsys.readouterr().out


def test_disabled_touches_nothing(monkeypatch, capsys):
    monkeypatch.setenv('GENIZAH_GC_FREEZE', 'off')
    state = _never_finishing_state()
    events, collect, freeze = _recorders(state)
    sleeps = []

    async def sleep(seconds):
        sleeps.append(seconds)

    assert _run(gc_freeze.freeze_after_loaders(state, collect=collect, freeze=freeze, sleep=sleep)) is False
    assert events == [] and sleeps == []
    assert gc_freeze.stats()['enabled'] is False
    assert gc_freeze.stats()['frozen_objects'] is None
    assert 'gc.freeze disabled' in capsys.readouterr().out


def test_singleton_runs_once():
    state = _finished_state()
    events, collect, freeze = _recorders(state)
    assert _run(gc_freeze.freeze_after_loaders(state, collect=collect, freeze=freeze)) is True
    assert _run(gc_freeze.freeze_after_loaders(state, collect=collect, freeze=freeze)) is False
    assert events == [('collect', True), ('freeze', True)]


def test_never_raises(capsys):
    state = _finished_state()

    def collect():
        raise RuntimeError('boom')

    assert _run(gc_freeze.freeze_after_loaders(state, collect=collect, freeze=lambda: None)) is False
    assert 'gc.freeze failed (non-fatal): boom' in capsys.readouterr().out


def test_stats_registered_as_gc_freeze_for_memstat(registered):
    state = _finished_state()
    events, collect, freeze = _recorders(state)
    _run(gc_freeze.freeze_after_loaders(state, collect=collect, freeze=freeze))
    assert 'gc_freeze' in registered
    stats = registered['gc_freeze']()
    for key in ('enabled', 'frozen_objects', 'waited_seconds', 'loaders_finished',
                'collect_ms', 'frozen_at_utc', 'current_freeze_count'):
        assert key in stats
    assert stats['enabled'] is True


# ---------------------------------------------------------------------------
# MetadataManager.background_loading_finished
# ---------------------------------------------------------------------------

def test_background_loading_finished_with_finished_blocked_and_no_threads():
    from shared.metadata_manager import MetadataManager

    mgr = MetadataManager.__new__(MetadataManager)  # no __init__: no files, no caches
    assert mgr.background_loading_finished() is True  # never started

    done = threading.Thread(target=lambda: None)
    done.start()
    done.join()
    gate = threading.Event()
    blocked = threading.Thread(target=gate.wait, daemon=True)
    blocked.start()
    try:
        mgr._bg_threads = [done]
        assert mgr.background_loading_finished() is True
        mgr._bg_threads = [done, blocked]
        assert mgr.background_loading_finished() is False
    finally:
        gate.set()
        blocked.join(timeout=5.0)
    assert mgr.background_loading_finished() is True

    # Created but not started yet (the window inside start_background_loading):
    # is_alive() is False before start(), but nothing has loaded.
    mgr._bg_threads = [done, threading.Thread(target=lambda: None)]
    assert mgr.background_loading_finished() is False


def test_start_background_loading_keeps_both_thread_handles():
    from shared.metadata_manager import MetadataManager

    mgr = MetadataManager.__new__(MetadataManager)
    gate = threading.Event()
    mgr._load_heavy_caches_bg = gate.wait
    mgr._build_file_map_background = gate.wait
    mgr.start_background_loading()
    try:
        assert len(mgr._bg_threads) == 2
        assert all(t.daemon for t in mgr._bg_threads)
        assert mgr.background_loading_finished() is False
    finally:
        gate.set()
        for t in mgr._bg_threads:
            t.join(timeout=5.0)
    assert mgr.background_loading_finished() is True


# ---------------------------------------------------------------------------
# initialize_engine schedules the task only when _init_sync succeeded
# ---------------------------------------------------------------------------

@pytest.mark.parametrize('init_ok, expect', [(True, True), (False, False), (None, False)])
def test_initialize_engine_schedules_the_freeze_only_on_success(monkeypatch, init_ok, expect):
    import web.main as web_main

    async def fake_io_bound(fn, *args, **kwargs):
        return init_ok  # _init_sync / _prewarm_sync never run

    monkeypatch.setattr('nicegui.run.io_bound', fake_io_bound)
    calls = []

    async def fake_freeze(state, *, extra_tasks=()):
        calls.append((state, extra_tasks))

    monkeypatch.setattr(gc_freeze, 'freeze_after_loaders', fake_freeze)

    async def main():
        await web_main.initialize_engine()
        named = [t for t in asyncio.all_tasks() if t.get_name() == 'gc-freeze-after-loaders']
        others = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
        await asyncio.gather(*others)
        return named

    named = _run(main())
    assert bool(named) is expect
    assert len(calls) == (1 if expect else 0)
    if expect:
        state, extra_tasks = calls[0]
        assert state is web_main.state
        assert len(extra_tasks) == 1 and isinstance(extra_tasks[0], asyncio.Task)


# ---------------------------------------------------------------------------
# /_internal/objgraph sees the freeze
# ---------------------------------------------------------------------------

class _FreezeProbe:
    pass


def test_objgraph_reports_the_freeze_count_and_unfreezes_and_refreezes(monkeypatch):
    import web.main as web_main

    monkeypatch.setattr(web_main, '_MEMSTAT_SECRET', 'secret')

    def call(**kw):
        return json.loads(web_main._objgraph_endpoint(x_memstat_secret='secret', **kw).body)

    probes = [_FreezeProbe(), _FreezeProbe()]
    try:
        body = call(action='stats')
        assert body['gc_freeze_count'] == gc.get_freeze_count()
        assert 'gc.get_objects()' in body['note'] and 'action=unfreeze' in body['note']

        body = call(action='backrefs', target_class='_FreezeProbe', sample=1, max_depth=1)
        assert body['total_count'] == 2 and 'gc_freeze_count' in body
        assert 'gc.get_referrers()' in body['note'] and 'frozen hop' in body['note']

        body = call(action='backrefs', target_class='NoSuchClassAnywhere')
        assert body['total_count'] == 0 and 'gc_freeze_count' in body

        refrozen = call(action='refreeze')
        assert refrozen['action'] == 'refreeze'
        assert refrozen['objects_moved'] == refrozen['gc_freeze_count'] == gc.get_freeze_count() > 0
        assert gc_freeze.stats()['unfrozen_for_diagnostics'] is False

        unfrozen = call(action='unfreeze')
        assert unfrozen['action'] == 'unfreeze'
        assert unfrozen['objects_moved'] == refrozen['gc_freeze_count']
        assert unfrozen['gc_freeze_count'] == 0 == gc.get_freeze_count()
        assert gc_freeze.stats()['unfrozen_for_diagnostics'] is True
    finally:
        gc.unfreeze()
    assert len(probes) == 2


# ---------------------------------------------------------------------------
# The premise, on the real gc
# ---------------------------------------------------------------------------

def test_real_gc_freeze_hides_frozen_objects_from_get_objects_and_get_referrers():
    probe = {'probe': []}
    holder = [probe]                                       # a tracked referrer of probe
    bank = [{'i': i, 'cell': []} for i in range(100_000)]  # the cell keeps each dict tracked
    gc.collect()
    before = len(gc.get_objects())
    assert before > 100_000
    assert any(r is holder for r in gc.get_referrers(probe))
    try:
        gc.collect()
        gc.freeze()
        assert gc.get_freeze_count() > 100_000
        after = len(gc.get_objects())
        assert after < before // 10, (before, after)
        # The frozen holder is no longer reported: a backref chain truncates here.
        assert not any(r is holder for r in gc.get_referrers(probe))
    finally:
        gc.unfreeze()
    assert gc.get_freeze_count() == 0
    assert any(r is holder for r in gc.get_referrers(probe))
    assert len(bank) == 100_000
