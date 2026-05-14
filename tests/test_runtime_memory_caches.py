from collections import OrderedDict


def test_ttl_memory_cache_evicts_by_entry_count():
    from web.api import _TTLMemoryCache

    cache = _TTLMemoryCache('test', ttl_seconds=60, max_entries=2, max_bytes=1024)
    cache.set('a', b'a' * 10)
    cache.set('b', b'b' * 10)
    cache.set('c', b'c' * 10)

    assert cache.get('a') is None
    assert cache.get('b') == b'b' * 10
    assert cache.get('c') == b'c' * 10
    assert cache.stats()['entries'] == 2


def test_ttl_memory_cache_evicts_by_byte_budget():
    from web.api import _TTLMemoryCache

    cache = _TTLMemoryCache('test', ttl_seconds=60, max_entries=10, max_bytes=25)
    cache.set('a', b'a' * 10)
    cache.set('b', b'b' * 10)
    cache.set('c', b'c' * 10)

    assert cache.get('a') is None
    assert cache.get('b') == b'b' * 10
    assert cache.get('c') == b'c' * 10
    assert cache.stats()['bytes_estimate'] <= 25


def test_metadata_manifest_cache_evicts_oldest_entry():
    from genizah_core import MetadataManager

    mgr = MetadataManager.__new__(MetadataManager)
    mgr.nli_cache = {}
    mgr._iiif_manifest_cache = OrderedDict()
    mgr._IIIF_MANIFEST_CACHE_MAX = 2

    mgr._bounded_cache_set(mgr._iiif_manifest_cache, ('1', 1), {'canvas_map': {'1': 'a'}}, 2)
    mgr._bounded_cache_set(mgr._iiif_manifest_cache, ('2', 1), {'canvas_map': {'2': 'b'}}, 2)
    mgr._bounded_cache_set(mgr._iiif_manifest_cache, ('3', 1), {'canvas_map': {'3': 'c'}}, 2)

    assert ('1', 1) not in mgr._iiif_manifest_cache
    assert ('2', 1) in mgr._iiif_manifest_cache
    assert ('3', 1) in mgr._iiif_manifest_cache
