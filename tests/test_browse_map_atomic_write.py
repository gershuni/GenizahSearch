# -*- coding: utf-8 -*-
"""Phase 146 Task 2c: the FL-ID background thread's browse_map.pkl rewrite
must be atomic.

`_load_browse_map` runs off `SearchEngine._build_fl_id_index_thread`, a
daemon thread no app-close drain waits on. A process killed mid-write must
leave the live browse_map.pkl exactly as it was, or exactly as the new
version -- never a truncated pickle a later reader chokes on.
"""
from __future__ import annotations

import glob
import os
import pickle
import threading
import time

import pytest

import shared.browse_map_utils as browse_map_utils
import shared.search_engine as search_engine_mod
from shared.config import Config
from shared.search_engine import SearchEngine


def _fresh_engine(monkeypatch, browse_map_path, on_disk):
    # An engine built by an earlier test in the process may still be loading the
    # real browse map on its FL-ID thread (tests/test_recovery_gate.py and other
    # files that build real engines do). Its load holds the class lock and ends by
    # filling the class cache: a test that cleared the cache first would wait on
    # that lock and get the real map. Wait for it here, then clear.
    #
    # That load also calls dedupe_browse_map and then writes to Config.BROWSE_MAP,
    # both looked up after its pickle.load. A dedupe faked before this wait is the
    # one it calls, and it writes the fixture onto the REAL map (B17): call this
    # before faking dedupe.
    assert search_engine_mod.dedupe_browse_map is browse_map_utils.dedupe_browse_map, (
        'redirect Config.BROWSE_MAP before faking dedupe')
    with SearchEngine._browse_map_lock:
        pass
    monkeypatch.setattr(Config, 'BROWSE_MAP', str(browse_map_path))
    monkeypatch.setattr(SearchEngine, '_shared_browse_map', None)
    with open(browse_map_path, 'wb') as f:
        pickle.dump(on_disk, f)
    return SearchEngine.__new__(SearchEngine)  # bypass __init__


def _leftover_tmp_siblings(browse_map_path):
    # Phase 146 D4: the tmp sibling carries a per-writer uuid, not a fixed
    # `.tmp` suffix, so a leftover check has to glob for any sibling rather
    # than the one literal name.
    return glob.glob(str(browse_map_path) + '.*.tmp')


def _assert_dedup_write_is_atomic(monkeypatch, browse_map_path):
    cleaned = {'sid': ['one', 'two']}
    engine = _fresh_engine(monkeypatch, browse_map_path, {'sid': ['raw']})
    monkeypatch.setattr(search_engine_mod, 'dedupe_browse_map',
                        lambda raw: (cleaned, True))

    got = engine._load_browse_map()

    assert got == cleaned
    with open(browse_map_path, 'rb') as f:
        assert pickle.load(f) == cleaned
    assert not _leftover_tmp_siblings(browse_map_path)


def test_dedup_write_is_atomic_and_leaves_no_tmp_file(tmp_path, monkeypatch):
    _assert_dedup_write_is_atomic(monkeypatch, tmp_path / 'browse_map.pkl')


class _SlowPage(dict):
    """A browse-map page that takes a while to unpickle, standing in for the
    real ~133 MB map: it holds a background load inside the class lock long
    enough for a test to start while that load is still running."""

    def __setstate__(self, state):
        time.sleep(1.0)
        self.__dict__.update(state)


def test_a_faked_dedupe_never_reaches_a_map_another_thread_is_loading(tmp_path, monkeypatch):
    """B17: an earlier test's FL-ID thread, still loading the REAL browse map
    under the class lock, called the dedupe a later test had already faked
    and os.replace()d that test's fixture onto the developer's real
    browse_map.pkl (it formats the write path from Config.BROWSE_MAP after
    dedupe returns, while Config still named the real file). Here a 'live'
    map under tmp_path plays the real one, and the atomic-write test runs
    while a load of it holds the lock; the live map must come out untouched."""
    live = tmp_path / 'live' / 'browse_map.pkl'
    live.parent.mkdir()
    page = _SlowPage(p_num=1, uid='IE1_P000001_FL1')
    page.slow = True  # pickle only calls __setstate__ when there is instance state
    with open(live, 'wb') as f:
        pickle.dump({'990000000000000001': [page]}, f)
    live_bytes = live.read_bytes()

    with SearchEngine._browse_map_lock:
        pass
    monkeypatch.setattr(Config, 'BROWSE_MAP', str(live))
    monkeypatch.setattr(SearchEngine, '_shared_browse_map', None)
    loader = threading.Thread(
        target=SearchEngine.__new__(SearchEngine)._load_browse_map, daemon=True)
    loader.start()
    deadline = time.monotonic() + 10
    while not SearchEngine._browse_map_lock.locked():
        assert time.monotonic() < deadline, 'the live load never took the lock'
        time.sleep(0.005)

    test_dir = tmp_path / 'test'
    test_dir.mkdir()
    with monkeypatch.context() as mp:
        _assert_dedup_write_is_atomic(mp, test_dir / 'browse_map.pkl')
    loader.join(timeout=30)

    assert not loader.is_alive()
    assert live.read_bytes() == live_bytes, (
        'a faked dedupe was written onto the live map: %r'
        % pickle.loads(live.read_bytes()))


def test_fresh_engine_refuses_a_dedupe_faked_before_the_redirect(tmp_path, monkeypatch):
    """The guard that keeps B17's ordering from coming back must be able to
    fire. Config is redirected first and the fake never asks for a write, so
    this check cannot reach the real map itself."""
    with SearchEngine._browse_map_lock:
        pass
    monkeypatch.setattr(Config, 'BROWSE_MAP', str(tmp_path / 'browse_map.pkl'))
    monkeypatch.setattr(search_engine_mod, 'dedupe_browse_map', lambda raw: (raw, False))

    with pytest.raises(AssertionError, match='redirect Config.BROWSE_MAP before faking dedupe'):
        _fresh_engine(monkeypatch, tmp_path / 'browse_map.pkl', {})


def test_unchanged_map_never_writes(tmp_path, monkeypatch):
    """changed=False is the common case (an already-clean map); no .tmp file
    and no rewrite of browse_map.pkl should happen at all."""
    browse_map_path = tmp_path / 'browse_map.pkl'
    original = {'sid': ['one']}
    engine = _fresh_engine(monkeypatch, browse_map_path, original)
    monkeypatch.setattr(search_engine_mod, 'dedupe_browse_map',
                        lambda raw: (raw, False))
    mtime_before = os.path.getmtime(browse_map_path)

    engine._load_browse_map()

    assert os.path.getmtime(browse_map_path) == mtime_before
    assert not _leftover_tmp_siblings(browse_map_path)


def test_a_write_failure_leaves_the_original_file_untouched(tmp_path, monkeypatch):
    """The write goes to a .tmp sibling first; if it never completes, the
    live browse_map.pkl -- the only file any reader ever opens -- must be
    exactly what it was before, not a half-written pickle. This is what
    os.replace buys over the old in-place `open(BROWSE_MAP, 'wb')`."""
    browse_map_path = tmp_path / 'browse_map.pkl'
    original = {'sid': ['raw', 'raw']}
    engine = _fresh_engine(monkeypatch, browse_map_path, original)
    monkeypatch.setattr(search_engine_mod, 'dedupe_browse_map',
                        lambda raw: ({'sid': ['raw']}, True))

    def _boom(*_a, **_k):
        raise OSError('disk full')

    monkeypatch.setattr(search_engine_mod.pickle, 'dump', _boom)

    got = engine._load_browse_map()  # logs a warning, does not raise

    assert got == {'sid': ['raw']}  # in-memory result is still the cleaned map
    with open(browse_map_path, 'rb') as f:
        on_disk = pickle.load(f)
    assert on_disk == original, 'a failed write corrupted the live file'
    assert not _leftover_tmp_siblings(browse_map_path)


def test_two_concurrent_writers_do_not_clobber_each_others_tmp_file(tmp_path, monkeypatch):
    """Phase 146 D4: the class-level `_browse_map_lock` is process-local, so
    two DESKTOP PROCESSES racing this write are not serialized by it at all
    -- a fresh interpreter gets its own unlocked Lock() and its own None
    `_shared_browse_map`, exactly what this test hands writer B mid-flight
    to stand in for a second process. A shared, fixed `.tmp` sibling name
    would let one process's in-flight write get truncated, replaced, or
    deleted by the other; each writer needs its OWN temp file so the two
    rewrites can interleave without corrupting either one's result or
    leaving the live file partial.

    Findings from inside the os.replace hook are stashed in `observations`
    and asserted on only AFTER the top-level call returns: the production
    code wraps this same call in a broad `except Exception`, so a bare
    `assert` raised from inside the hook would be swallowed there and never
    reach pytest.
    """
    browse_map_path = tmp_path / 'browse_map.pkl'
    original = {'sid': ['raw', 'raw']}
    with open(browse_map_path, 'wb') as f:
        pickle.dump(original, f)
    # Let an earlier test's load of the real map finish on the REAL lock before
    # swapping it out, as _fresh_engine does: once the lock is swapped nothing
    # waits for that load, and it would reach the fakes patched below.
    with SearchEngine._browse_map_lock:
        pass
    monkeypatch.setattr(Config, 'BROWSE_MAP', str(browse_map_path))
    monkeypatch.setattr(SearchEngine, '_shared_browse_map', None)
    monkeypatch.setattr(SearchEngine, '_browse_map_lock', threading.Lock())

    engine_a = SearchEngine.__new__(SearchEngine)
    engine_b = SearchEngine.__new__(SearchEngine)

    real_replace = os.replace
    tmp_paths_seen = []
    cleaned_a = {'sid': ['from_a']}
    cleaned_b = {'sid': ['from_b']}
    observations = {}

    def _dedupe(raw):
        # The first call in program order is writer A's; the nested one
        # triggered from inside A's os.replace is writer B's.
        return (cleaned_b, True) if tmp_paths_seen else (cleaned_a, True)

    def _tracking_replace(src, dst):
        # Writer A pauses here, mid-rewrite, with its OWN tmp file already
        # written but not yet swapped in -- this is the window a shared tmp
        # name would let writer B collide into.
        tmp_paths_seen.append(src)
        if len(tmp_paths_seen) == 1:
            observations['a_tmp_before_b'] = os.path.exists(src)
            # Stand in for B being a separate OS process: its own unlocked
            # lock and its own empty class-level cache, since neither of
            # those actually crosses a process boundary in reality.
            monkeypatch.setattr(SearchEngine, '_browse_map_lock', threading.Lock())
            monkeypatch.setattr(SearchEngine, '_shared_browse_map', None)
            observations['b_result'] = engine_b._load_browse_map()
            # Writer A's own tmp file must still be exactly what A wrote --
            # untouched by B's independent write.
            observations['a_tmp_after_b'] = os.path.exists(src)
        real_replace(src, dst)

    monkeypatch.setattr(search_engine_mod.os, 'replace', _tracking_replace)
    monkeypatch.setattr(search_engine_mod, 'dedupe_browse_map', _dedupe)

    got_a = engine_a._load_browse_map()

    assert got_a == cleaned_a
    assert observations.get('b_result') == cleaned_b
    assert observations.get('a_tmp_before_b') is True, 'writer A tmp file must exist once written'
    assert observations.get('a_tmp_after_b') is True, (
        "writer B must not delete or overwrite writer A's tmp file")
    assert len(tmp_paths_seen) == 2, 'both writers must reach os.replace'
    assert tmp_paths_seen[0] != tmp_paths_seen[1], (
        'concurrent writers must not share a tmp filename')
    # Writer A's real os.replace runs last (after B's nested call returns),
    # so the live file must end up as A's complete result -- never a
    # partial mix of both writers' content.
    with open(browse_map_path, 'rb') as f:
        final = pickle.load(f)
    assert final == cleaned_a
    assert not _leftover_tmp_siblings(browse_map_path)
