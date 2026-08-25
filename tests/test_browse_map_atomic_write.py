# -*- coding: utf-8 -*-
"""Phase 146 Task 2c: the FL-ID background thread's browse_map.pkl rewrite
must be atomic.

`_load_browse_map` runs off `SearchEngine._build_fl_id_index_thread`, a
daemon thread no app-close drain waits on. A process killed mid-write must
leave the live browse_map.pkl exactly as it was, or exactly as the new
version -- never a truncated pickle a later reader chokes on.
"""
from __future__ import annotations

import os
import pickle

import shared.search_engine as search_engine_mod
from shared.config import Config
from shared.search_engine import SearchEngine


def _fresh_engine(monkeypatch, browse_map_path, on_disk):
    monkeypatch.setattr(Config, 'BROWSE_MAP', str(browse_map_path))
    monkeypatch.setattr(SearchEngine, '_shared_browse_map', None)
    with open(browse_map_path, 'wb') as f:
        pickle.dump(on_disk, f)
    return SearchEngine.__new__(SearchEngine)  # bypass __init__


def test_dedup_write_is_atomic_and_leaves_no_tmp_file(tmp_path, monkeypatch):
    browse_map_path = tmp_path / 'browse_map.pkl'
    cleaned = {'sid': ['one', 'two']}
    monkeypatch.setattr(search_engine_mod, 'dedupe_browse_map',
                        lambda raw: (cleaned, True))
    engine = _fresh_engine(monkeypatch, browse_map_path, {'sid': ['raw']})

    got = engine._load_browse_map()

    assert got == cleaned
    with open(browse_map_path, 'rb') as f:
        assert pickle.load(f) == cleaned
    assert not os.path.exists(str(browse_map_path) + '.tmp')


def test_unchanged_map_never_writes(tmp_path, monkeypatch):
    """changed=False is the common case (an already-clean map); no .tmp file
    and no rewrite of browse_map.pkl should happen at all."""
    browse_map_path = tmp_path / 'browse_map.pkl'
    original = {'sid': ['one']}
    monkeypatch.setattr(search_engine_mod, 'dedupe_browse_map',
                        lambda raw: (raw, False))
    engine = _fresh_engine(monkeypatch, browse_map_path, original)
    mtime_before = os.path.getmtime(browse_map_path)

    engine._load_browse_map()

    assert os.path.getmtime(browse_map_path) == mtime_before
    assert not os.path.exists(str(browse_map_path) + '.tmp')


def test_a_write_failure_leaves_the_original_file_untouched(tmp_path, monkeypatch):
    """The write goes to a .tmp sibling first; if it never completes, the
    live browse_map.pkl -- the only file any reader ever opens -- must be
    exactly what it was before, not a half-written pickle. This is what
    os.replace buys over the old in-place `open(BROWSE_MAP, 'wb')`."""
    browse_map_path = tmp_path / 'browse_map.pkl'
    original = {'sid': ['raw', 'raw']}
    monkeypatch.setattr(search_engine_mod, 'dedupe_browse_map',
                        lambda raw: ({'sid': ['raw']}, True))
    engine = _fresh_engine(monkeypatch, browse_map_path, original)

    def _boom(*_a, **_k):
        raise OSError('disk full')

    monkeypatch.setattr(search_engine_mod.pickle, 'dump', _boom)

    got = engine._load_browse_map()  # logs a warning, does not raise

    assert got == {'sid': ['raw']}  # in-memory result is still the cleaned map
    with open(browse_map_path, 'rb') as f:
        on_disk = pickle.load(f)
    assert on_disk == original, 'a failed write corrupted the live file'
    assert not os.path.exists(str(browse_map_path) + '.tmp')
