# -*- coding: utf-8 -*-
"""The NLI image proxy's on-disk stores (web/nli_image_store.py).

2026-10-21 (owner report): in the Visual Similarity grid some thumbnails came up
blank, and different ones after each size switch. Every card whose manuscript the
server had not resolved needed an NLI manifest; only 8 manifests may be fetched at
once with a 1-second wait (NLI_MAX_CONCURRENT_FETCHES / NLI_SEMAPHORE_TIMEOUT), so
the rest answered 404. A failure is no-store, so each redraw asked again for
exactly the failed cards. The FL-id cache that should have spared most of those
manifests was meant to last 30 days but held the last 5 minutes: every save
rewrote the file from the memory cache, which keeps an entry 5 minutes.

tests/conftest.py gives every test its own (empty) store and thumbnail folder.
"""
from __future__ import annotations

import hashlib
import json
import os
import threading
import time
from unittest.mock import patch

import pytest
from fastapi import FastAPI

from web import nli_image_store as store

JPEG = b'\xff\xd8\xff\xe0' + b'\x00' * 5000
PNG = b'\x89PNG\r\n\x1a\n' + b'\x00' * 2500
SID = '990051310150205171'   # T-S Ar.43.124, blank in the owner's Small screenshot
DAY = 24 * 60 * 60


# ─── FL-id store ──────────────────────────────────────────────────────────────

class TestFlIdStore:
    def test_round_trip_keeps_only_positive_lists(self):
        now = time.time()
        cache = {'a': ['FL123', '00456'], 'b': 'not-a-list', 'c': []}
        assert store.fl_id_store_put(cache, dict.fromkeys(cache, now), 30 * DAY)
        assert store.fl_id_store_get('a', 30 * DAY) == ['123', '00456']
        assert store.fl_id_store_get('b', 30 * DAY) is None
        assert store.fl_id_store_get('c', 30 * DAY) is None

    def test_an_entry_lasts_its_ttl_and_no_longer(self):
        now = time.time()
        store.fl_id_store_put({'k': ['1']}, {'k': now - 10 * DAY}, 30 * DAY, now=now)
        assert store.fl_id_store_get('k', 30 * DAY, now=now) == ['1']
        assert store.fl_id_store_get('k', 30 * DAY, now=now + 21 * DAY) is None

    def test_old_json_cache_is_imported_once_without_overwriting_newer_rows(self):
        now = time.time()
        legacy = os.path.join(os.path.dirname(store.fl_id_store_path()), 'nli_fl_ids_cache.json')
        os.makedirs(os.path.dirname(legacy), exist_ok=True)
        with open(legacy, 'w', encoding='utf-8') as handle:
            json.dump({'version': 1, 'entries': {
                'old': {'fl_ids': ['111'], 'cached_at': now - DAY},
                'both': {'fl_ids': ['222'], 'cached_at': now - DAY},
                'expired': {'fl_ids': ['333'], 'cached_at': now - 40 * DAY},
            }}, handle)
        store.fl_id_store_put({'both': ['999']}, {'both': now}, 30 * DAY)

        assert store.fl_id_store_prepare(30 * DAY, now=now) == 3
        assert store.fl_id_store_get('old', 30 * DAY) == ['111']
        assert store.fl_id_store_get('both', 30 * DAY) == ['999']
        assert store.fl_id_store_get('expired', 30 * DAY) is None
        assert not os.path.exists(legacy) and os.path.exists(legacy + '.imported')
        assert store.fl_id_store_prepare(30 * DAY, now=now) == 0

    def test_prepare_drops_expired_rows(self):
        now = time.time()
        store.fl_id_store_put({'k': ['1']}, {'k': now - 10}, 30 * DAY, now=now)
        store.fl_id_store_prepare(5, now=now)
        assert store.fl_id_store_get('k', 30 * DAY, now=now) is None

    def test_an_empty_setting_turns_it_off(self, monkeypatch):
        monkeypatch.setenv('NLI_FL_ID_STORE_FILE', '')
        assert store.fl_id_store_path() is None
        assert store.fl_id_store_put({'k': ['1']}, {'k': time.time()}, 30 * DAY)
        assert store.fl_id_store_get('k', 30 * DAY) is None


# ─── Thumbnail cache ──────────────────────────────────────────────────────────

class TestThumbnailCache:
    @pytest.mark.parametrize('sys_id, page, width, suffix', [
        ('../../etc/passwd', 0, 300, 1),          # never a path from the request
        ('99/../../x', 0, 300, 1),
        ('970993169503583183', 0, 300, 1),        # LOCAL "My Library" namespace
        ('990001234560000000', 0, 300, 1),        # synthetic: no NLI image exists
        (SID, 0, 600, 1),                         # not a grid width (NLI_THUMB_CACHE_WIDTHS)
        (SID, 0, 301, 1),                         # nor one width per request: one copy per image
        (SID, -1, 300, 1),
        (SID, 0, 300, 0),
    ])
    def test_keys_it_does_not_keep(self, sys_id, page, width, suffix):
        assert store.thumb_path(sys_id, page, width, suffix) is None
        assert store.thumb_put(sys_id, page, width, suffix, JPEG) is False

    def test_round_trip_and_layout(self):
        assert store.thumb_put(SID, 2, 300, 1, JPEG)
        shard = hashlib.md5(SID.encode('ascii')).hexdigest()[:2]
        expected = os.path.join(store.thumb_cache_dir(), 'w300', shard, f'{SID}_s1_p2.jpg')
        assert store.thumb_path(SID, 2, 300, 1) == expected and os.path.isfile(expected)
        assert store.thumb_get(SID, 2, 300, 1) == JPEG
        assert store.thumb_get(SID, 3, 300, 1) is None
        assert not [n for n in os.listdir(os.path.dirname(expected)) if '.tmp.' in n]

    def test_only_jpeg_is_kept(self):
        assert store.thumb_put(SID, 0, 300, 1, PNG) is False
        assert store.thumb_get(SID, 0, 300, 1) is None

    def test_no_write_past_the_size_cap(self, monkeypatch):
        monkeypatch.setattr(store, 'NLI_THUMB_CACHE_MAX_MB', 8 / 1024)      # 8 KB
        assert store.thumb_put(SID, 0, 300, 1, JPEG)                      # 5 KB
        before = store.stats()['thumb_skipped_full']
        assert store.thumb_put(SID, 1, 300, 1, JPEG) is False
        assert store.stats()['thumb_skipped_full'] == before + 1
        assert store.thumb_get(SID, 1, 300, 1) is None

    def test_files_already_on_disk_count_toward_the_cap(self, monkeypatch):
        assert store.thumb_put(SID, 0, 300, 1, JPEG)
        root = store.thumb_cache_dir()
        store._size.update(root=None, bytes=None)                         # as after a restart
        monkeypatch.setattr(store, 'NLI_THUMB_CACHE_MAX_MB', 8 / 1024)
        store._reserve(root, 0)                                          # starts the count
        deadline = time.time() + 5
        while store.stats()['thumb_bytes'] is None and time.time() < deadline:
            time.sleep(0.01)
        assert store.stats()['thumb_bytes'] == len(JPEG)
        assert store.thumb_put(SID, 1, 300, 1, JPEG) is False

    def test_two_requests_for_one_thumbnail_count_it_once(self, monkeypatch):
        # Codex on #427 (P2): both writers counted the bytes, one file remained, and the
        # cache then refused new thumbnails it had room for.
        monkeypatch.setattr(store, 'NLI_THUMB_CACHE_MAX_MB', (2 * len(JPEG) + 100) / (1024 * 1024))
        entered, release = threading.Event(), threading.Event()
        real_disk_usage = store.shutil.disk_usage

        def first_writer_waits(path):
            if not entered.is_set():
                entered.set()
                release.wait(5)
            return real_disk_usage(path)

        monkeypatch.setattr(store.shutil, 'disk_usage', first_writer_waits)
        first = threading.Thread(target=store.thumb_put, args=(SID, 0, 300, 1, JPEG))
        first.start()
        assert entered.wait(5)
        store.thumb_put(SID, 0, 300, 1, JPEG)             # the same thumbnail, meanwhile
        release.set()
        first.join(5)
        assert store.thumb_get(SID, 0, 300, 1) == JPEG
        assert store.stats()['thumb_bytes'] == len(JPEG)
        assert store.thumb_put(SID, 1, 300, 1, JPEG)      # room for one more

    def test_no_write_while_the_folder_is_still_being_counted(self, monkeypatch):
        # Codex on #427 (P2): after a restart every write was accepted while the folder
        # was counted, even when it was already full.
        assert store.thumb_put(SID, 0, 300, 1, JPEG)
        root = store.thumb_cache_dir()
        store._size.update(root=None, bytes=None)                         # as after a restart
        monkeypatch.setattr(store, 'NLI_THUMB_CACHE_MAX_MB', (len(JPEG) + 100) / (1024 * 1024))
        counting, finish = threading.Event(), threading.Event()
        real_walk = os.walk

        def slow_walk(top, *a, **kw):
            if top == root:
                counting.set()
                finish.wait(5)
            return real_walk(top, *a, **kw)

        monkeypatch.setattr(store.os, 'walk', slow_walk)
        store._reserve(root, 0)                                           # starts the count
        assert counting.wait(5)
        assert store.thumb_put(SID, 1, 300, 1, JPEG) is False            # full, not yet known
        finish.set()
        deadline = time.time() + 5
        while store.stats()['thumb_bytes'] is None and time.time() < deadline:
            time.sleep(0.01)
        assert store.stats()['thumb_bytes'] == len(JPEG)
        assert store.thumb_get(SID, 1, 300, 1) is None

    def test_no_write_when_the_disk_is_nearly_full(self, monkeypatch):
        monkeypatch.setattr(store, 'NLI_THUMB_CACHE_MIN_FREE_MB', 10 ** 12)
        before = store.stats()['thumb_skipped_low_disk']
        assert store.thumb_put(SID, 0, 300, 1, JPEG) is False
        assert store.stats()['thumb_skipped_low_disk'] == before + 1
        assert store.thumb_get(SID, 0, 300, 1) is None


# ─── Wired into the proxy (web/api.py) ────────────────────────────────────────

class _Resp:
    def __init__(self, status, content_type, content=b'', json_data=None, text=''):
        self.status_code = status
        self.headers = {'Content-Type': content_type}
        self.content = content
        self.text = text
        self._json = json_data

    def json(self):
        return self._json


def _manifest(fl_ids):
    return _Resp(200, 'application/json', json_data={'sequences': [{'canvases': [
        {'images': [{'resource': {'service': {'@id': f'https://iiif.nli.org.il/IIIFv21/FL{fl}'}}}]}
        for fl in fl_ids
    ]}]})


class _FakeNli:
    """NLI as a router: manifests, IIIF images and Rosetta thumbnails by FL id."""

    def __init__(self, manifests, iiif_ok=(), rosetta_ok=(), manifest_delay=0.0,
                 marc=None, manifest_status=200):
        self.manifests = manifests          # sys_id -> [fl, ...]
        self.iiif_ok = set(iiif_ok)
        self.rosetta_ok = set(rosetta_ok)
        self.manifest_delay = manifest_delay
        self.marc = marc or {}              # sys_id -> [fl, ...] in the MARC record
        self.manifest_status = manifest_status
        self.manifest_calls = 0
        self._lock = threading.Lock()

    def get(self, url, *a, **kw):
        if url.endswith('/manifest'):
            with self._lock:
                self.manifest_calls += 1
            time.sleep(self.manifest_delay)
            if self.manifest_status != 200:
                return _Resp(self.manifest_status, 'text/plain')
            sys_id = url.split('PNX_MANUSCRIPTS', 1)[1].rsplit('-', 1)[0]
            if sys_id in self.manifests:
                return _manifest(self.manifests[sys_id])
            return _Resp(404, 'text/plain')
        if '/marc/bib/' in url:
            fls = self.marc.get(url.rsplit('/', 1)[1])
            if fls:
                return _Resp(200, 'application/xml', text=''.join(f'<sf>FL{fl}</sf>' for fl in fls))
            return _Resp(404, 'text/plain')
        fl = url.split('/FL', 1)[1].split('/', 1)[0]
        if fl in self.iiif_ok:
            return _Resp(200, 'image/jpeg', JPEG)
        return _Resp(500, 'text/xml', b'<error/>')

    def rosetta(self, url, *a, **kw):
        fl = url.rsplit('FL', 1)[1]
        if fl in self.rosetta_ok:
            return _Resp(200, 'image/png', PNG)
        return _Resp(404, 'text/html', b'nope')

    def patches(self):
        return (
            patch('shared.puzzle_image_service.get_with_checked_redirects', side_effect=self.get),
            patch('web.api.nli_image_get', side_effect=self.rosetta),
        )


@pytest.fixture(scope='module')
def seam():
    from web.api import _api_test_seam, init_api_routes
    init_api_routes(app_override=FastAPI())
    return _api_test_seam


@pytest.fixture(autouse=True)
def _empty_memory_caches(request):
    """Clear the proxy's FL-id and image memory caches, so a disk answer is a disk answer."""
    if 'seam' not in request.fixturenames:
        yield
        return
    seam = request.getfixturevalue('seam')

    def clear():
        for fn in (seam['fetch_fl_ids_from_nli'], seam['_fetch_nli_image_bytes']):
            for name, cell in zip(fn.__code__.co_freevars, fn.__closure__ or ()):
                value = cell.cell_contents
                if name in ('_nli_cache', '_nli_cache_time', '_nli_partial_keys'):
                    value.clear()
                elif name == '_image_cache':
                    with value._lock:
                        value._items.clear()
                        value._total_bytes = 0

    clear()
    request.node.clear_memory = clear
    yield
    clear()


class TestProxyUsesTheStores:
    def test_resolved_fl_ids_outlive_the_five_minute_memory_cache(self, seam, request):
        # The bug: resolving B six minutes after A rewrote the file from memory,
        # which no longer held A.
        nli = _FakeNli({'990000000000105171': ['11'], '990000000000205171': ['22']})
        p1, p2 = nli.patches()
        clock = [time.time()]
        with p1, p2, patch('time.time', lambda: clock[0]):
            assert seam['fetch_fl_ids_from_nli']('990000000000105171') == ['11']
            clock[0] += 6 * 60
            assert seam['fetch_fl_ids_from_nli']('990000000000205171') == ['22']
            request.node.clear_memory()
            nli.manifests.clear()           # NLI would now answer nothing
            assert seam['fetch_fl_ids_from_nli']('990000000000105171') == ['11']
        assert nli.manifest_calls == 2

    def test_a_grid_of_stored_manuscripts_needs_no_manifest_and_no_slot(self, seam):
        # The owner's screen: 20 cards at once. With their FL ids stored, no card
        # waits for one of the 8 manifest slots -- all 8 are held here throughout.
        import web.api as api
        sys_ids = [f'9900000{i:04d}0205171' for i in range(20)]
        now = time.time()
        store.fl_id_store_put({s: [f'7{i}'] for i, s in enumerate(sys_ids)},
                              dict.fromkeys(sys_ids, now), api.NLI_DISK_CACHE_TTL)
        nli = _FakeNli({}, iiif_ok=[f'7{i}' for i in range(20)], manifest_delay=1.5)
        held = [api._nli_semaphore.acquire(timeout=1) for _ in range(api.NLI_MAX_CONCURRENT_FETCHES)]
        results = {}
        p1, p2 = nli.patches()
        try:
            with p1, p2:
                threads = [threading.Thread(
                    target=lambda s=s: results.__setitem__(s, seam['_fetch_nli_image_bytes'](s, 0, width=300)))
                    for s in sys_ids]
                for t in threads:
                    t.start()
                for t in threads:
                    t.join(timeout=30)
        finally:
            for ok in held:
                if ok:
                    api._nli_semaphore.release()
        assert all(held)
        assert [s for s in sys_ids if results.get(s) is None] == []
        assert nli.manifest_calls == 0

    def test_a_thumbnail_is_kept_and_served_from_disk_during_an_nli_outage(self, seam, request):
        import shared.nli_circuit_breaker as br
        nli = _FakeNli({SID: ['501', '502']}, iiif_ok=['501', '502'])
        p1, p2 = nli.patches()
        with p1, p2:
            content, ct, fl_id = seam['_fetch_nli_image_bytes'](SID, 1, width=300)
        assert (content, fl_id) == (JPEG, '502')
        assert store.thumb_get(SID, 1, 300, 1) == JPEG

        request.node.clear_memory()
        for _ in range(br.NLI_CIRCUIT_THRESHOLD):
            br.record_failure(failure_type='timeout', path='test')
        assert br.is_open()
        with patch('shared.puzzle_image_service.get_with_checked_redirects',
                   side_effect=AssertionError('no NLI call for a kept thumbnail')):
            assert seam['_fetch_nli_image_bytes'](SID, 1, width=300) == (JPEG, 'image/jpeg', '')

    def test_a_marc_fallback_list_is_kept_neither_30_days_nor_as_a_thumbnail(self, seam, request):
        # Codex on #427 (P2): with the manifest failing, MARC names only the first image.
        # Stored for 30 days, it answered every later page with that image after NLI
        # recovered; the old 5-minute memory cache recovered on its own.
        sid = '990000000000305171'
        nli = _FakeNli({sid: ['31', '32', '33']}, iiif_ok=['31', '32', '33'],
                       marc={sid: ['31']}, manifest_status=500)
        p1, p2 = nli.patches()
        with p1, p2:
            assert seam['fetch_fl_ids_from_nli'](sid) == ['31']
            assert seam['_fetch_nli_image_bytes'](sid, 0, width=300)[0] == JPEG
        assert store.fl_id_store_get(sid, 30 * DAY) is None
        assert store.thumb_get(sid, 0, 300, 1) is None

        request.node.clear_memory()           # five minutes later, NLI is back
        nli.manifest_status = 200
        p1, p2 = nli.patches()
        with p1, p2:
            assert seam['fetch_fl_ids_from_nli'](sid) == ['31', '32', '33']
            assert seam['_fetch_nli_image_bytes'](sid, 2, width=2000)[2] == '33'
        assert store.fl_id_store_get(sid, 30 * DAY) == ['31', '32', '33']

    def test_a_rosetta_stand_in_is_served_but_not_kept(self, seam):
        nli = _FakeNli({SID: ['601']}, rosetta_ok=['601'])
        p1, p2 = nli.patches()
        with p1, p2:
            content, ct, _fl = seam['_fetch_nli_image_bytes'](SID, 0, width=300)
        assert (content, ct) == (PNG, 'image/png')
        assert store.thumb_get(SID, 0, 300, 1) is None

    def test_another_pages_image_from_the_fallback_loop_is_not_kept(self, seam):
        # Page 1's FL fails everywhere; the loop serves page 0's image for it.
        nli = _FakeNli({SID: ['701', '702']}, iiif_ok=['701'])
        p1, p2 = nli.patches()
        with p1, p2:
            content, _ct, fl_id = seam['_fetch_nli_image_bytes'](SID, 1, width=300)
        assert (content, fl_id) == (JPEG, '701')
        assert store.thumb_get(SID, 1, 300, 1) is None

    def test_full_size_images_stay_off_disk(self, seam):
        nli = _FakeNli({SID: ['801']}, iiif_ok=['801'])
        p1, p2 = nli.patches()
        with p1, p2:
            assert seam['_fetch_nli_image_bytes'](SID, 0, width=2000)[0] == JPEG
        assert not os.path.exists(store.thumb_cache_dir())
