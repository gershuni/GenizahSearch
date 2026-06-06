# -*- coding: utf-8 -*-
"""Tests for the bounded, thread-safe LRU backing MetadataManager.nli_cache.

Origin: 2026-06-06 production heap re-attribution found nli_cache was an
unbounded dict (50K+ entries and climbing on a 3-day-old web process). These
tests pin the eviction bound, the dict-API surface the rest of the codebase
relies on, pickle-as-plain-dict on-disk compatibility, and concurrency safety.
"""
import pickle
import threading

import pytest

from genizah_core import _BoundedLRUCache


def test_bounds_to_maxsize():
    c = _BoundedLRUCache(maxsize=10)
    for i in range(100):
        c[f'k{i}'] = i
    assert len(c) == 10


def test_lru_eviction_order_evicts_oldest():
    c = _BoundedLRUCache(maxsize=3)
    c['a'] = 1
    c['b'] = 2
    c['c'] = 3
    # Touch 'a' so it becomes most-recently-used; 'b' is now the oldest.
    assert c['a'] == 1
    c['d'] = 4  # should evict 'b'
    assert 'b' not in c
    assert 'a' in c and 'c' in c and 'd' in c
    assert len(c) == 3


def test_get_recency_and_default():
    c = _BoundedLRUCache(maxsize=2)
    c['a'] = 1
    c['b'] = 2
    assert c.get('a') == 1          # touch 'a' -> 'b' is oldest
    assert c.get('missing') is None
    assert c.get('missing', {}) == {}
    c['z'] = 26                     # evicts 'b', not 'a'
    assert 'a' in c and 'z' in c and 'b' not in c


def test_api_surface_matches_dict_usage():
    c = _BoundedLRUCache(maxsize=100)
    c['x'] = {'shelfmark': 'T-S 1.1'}
    assert 'x' in c
    assert c['x']['shelfmark'] == 'T-S 1.1'
    assert c.get('x') == {'shelfmark': 'T-S 1.1'}
    assert list(c.items()) == [('x', {'shelfmark': 'T-S 1.1'})]
    assert list(c.keys()) == ['x']
    assert list(c.values()) == [{'shelfmark': 'T-S 1.1'}]
    assert list(iter(c)) == ['x']
    assert len(c) == 1


def test_getitem_missing_raises_keyerror():
    c = _BoundedLRUCache(maxsize=5)
    with pytest.raises(KeyError):
        _ = c['nope']


def test_items_returns_snapshot_safe_under_mutation():
    # Iterating the snapshot must not raise even if the cache is mutated,
    # mirroring the unsnapshotted .items() loops at genizah_core.py:~5062/~5097.
    c = _BoundedLRUCache(maxsize=1000)
    for i in range(50):
        c[f'k{i}'] = i
    snapshot = c.items()
    c['new'] = 999  # mutate after taking the snapshot
    # No RuntimeError; snapshot is a stable list.
    assert sum(1 for _ in snapshot) == 50


def test_pickles_as_plain_dict():
    c = _BoundedLRUCache(maxsize=10)
    c['a'] = 1
    c['b'] = 2
    blob = pickle.dumps(c)
    restored = pickle.loads(blob)
    # On-disk/unpickled form is a plain dict (desktop + older-build compat).
    assert type(restored) is dict
    assert restored == {'a': 1, 'b': 2}


def test_construct_from_data_evicts_to_maxsize():
    big = {f'k{i}': i for i in range(100)}
    c = _BoundedLRUCache(maxsize=10, data=big)
    assert len(c) == 10
    # Keeps the most-recently-inserted keys (insertion order of dict).
    assert 'k99' in c
    assert 'k0' not in c


def test_maxsize_zero_is_unbounded():
    c = _BoundedLRUCache(maxsize=0)
    for i in range(5000):
        c[f'k{i}'] = i
    assert len(c) == 5000


def test_concurrent_access_is_safe_and_bounded():
    c = _BoundedLRUCache(maxsize=500)
    errors = []

    def worker(base):
        try:
            for i in range(2000):
                key = f'k{(base + i) % 1000}'
                c[key] = i
                _ = c.get(key)
                if i % 100 == 0:
                    _ = c.items()  # snapshot iteration under concurrent writes
                    _ = len(c)
        except Exception as e:  # pragma: no cover - failure path
            errors.append(e)

    threads = [threading.Thread(target=worker, args=(t * 1000,)) for t in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors, f"concurrent access raised: {errors[:3]}"
    assert len(c) <= 500
