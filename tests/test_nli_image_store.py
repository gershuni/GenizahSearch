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

    def test_the_old_json_cache_is_not_trusted_after_the_upgrade(self, seam):
        # Codex on #427 (P2): nli_fl_ids_cache.json mixes manifest lists with one-image
        # MARC fallback lists and cannot tell them apart. Imported into the 30-day
        # store, such a list answered page 2 with page 1 after NLI recovered.
        sid = '990000000000405171'
        legacy = os.path.join(os.path.dirname(store.fl_id_store_path()), 'nli_fl_ids_cache.json')
        os.makedirs(os.path.dirname(legacy), exist_ok=True)
        with open(legacy, 'w', encoding='utf-8') as handle:
            json.dump({'version': 1, 'entries': {sid: {'fl_ids': ['41'], 'cached_at': time.time()}}}, handle)
        store.fl_id_store_prepare(30 * DAY)                                # the first start
        nli = _FakeNli({sid: ['41', '42']}, iiif_ok=['41', '42'])
        p1, p2 = nli.patches()
        with p1, p2:
            assert seam['_fetch_nli_image_bytes'](sid, 1, width=2000)[2] == '42'
        assert nli.manifest_calls == 1
        assert store.fl_id_store_get(sid, 30 * DAY) == ['41', '42']

    def test_a_rosetta_stand_in_is_served_but_not_kept(self, seam):
        nli = _FakeNli({SID: ['601']}, rosetta_ok=['601'])
        p1, p2 = nli.patches()
        with p1, p2:
            content, ct, _fl = seam['_fetch_nli_image_bytes'](SID, 0, width=300)
        assert (content, ct) == (PNG, 'image/png')
        assert store.thumb_get(SID, 0, 300, 1) is None

    def test_a_page_with_its_own_image_is_never_answered_with_another_page(self, seam):
        # T-S 12.782 (2026-11-11): page 0's FL failed during an NLI hiccup and the loop
        # answered it with page 1's image -- the verso shown as 1r, kept a week by the
        # browser and Cloudflare. Now: no answer, and nothing kept anywhere.
        nli = _FakeNli({SID: ['701', '702']}, iiif_ok=['702'])
        p1, p2 = nli.patches()
        with p1, p2:
            assert seam['_fetch_nli_image_bytes'](SID, 0, width=2000) is None
            assert seam['_fetch_nli_image_bytes'](SID, 0, width=300) is None
            nli.iiif_ok.add('701')            # NLI answers again: page 0's own image
            assert seam['_fetch_nli_image_bytes'](SID, 0, width=2000)[2] == '701'
        assert store.thumb_get(SID, 0, 300, 1) is None

    def test_a_page_with_no_fl_id_of_its_own_gets_a_stand_in_that_is_not_kept(self, seam):
        # A MARC fallback list (the manifest failing) names only the first image: page 2
        # is shown that one rather than nothing, but no browser or CDN may keep it.
        sid = '990000000000505171'
        nli = _FakeNli({sid: ['71', '72', '73']}, iiif_ok=['71', '72', '73'],
                       marc={sid: ['71']}, manifest_status=500)
        p1, p2 = nli.patches()
        with p1, p2:
            content, _ct, fl_id, kept = seam['_fetch_nli_image_bytes'](sid, 2, width=300, with_kept=True)
            assert (content, fl_id, kept) == (JPEG, '71', False)
            # Page 0 is its own image, but from a MARC list: right today, not kept either.
            assert seam['_fetch_nli_image_bytes'](sid, 0, width=300, with_kept=True)[2:] == ('71', False)
        assert store.thumb_get(sid, 2, 300, 1) is None

    def test_a_marc_list_replaced_by_a_manifest_list_meanwhile_is_still_not_kept(self, seam):
        # Codex round 2: request A gets the MARC list; request B stores the manifest's list
        # before A reads whether its list is complete. A's image came from the MARC list:
        # not kept. B lands in that window here: right after A stored its MARC list
        # (the breaker's success hook is the next thing A runs).
        import shared.nli_circuit_breaker as br
        sid = '990000000000605171'
        nli = _FakeNli({sid: ['81', '82']}, iiif_ok=['81', '82'], marc={sid: ['81']}, manifest_status=500)
        fn = seam['_fetch_nli_image_bytes']
        cells = dict(zip(fn.__code__.co_freevars, fn.__closure__))
        cache, partial, lock = (cells[n].cell_contents for n in ('_nli_cache', '_nli_partial_keys', '_nli_cache_lock'))

        def success(path='', **kw):
            if path == 'fetch_fl_ids_from_nli' and sid in partial:
                with lock:                             # B: the manifest answered meanwhile
                    cache[sid] = ['81', '82']
                    partial.discard(sid)
            return br.record_success(path=path, **kw)

        p1, p2 = nli.patches()
        with p1, p2, patch('web.api._nli_record_success', side_effect=success):
            assert fn(sid, 0, width=300, with_kept=True)[2:] == ('81', False)
        assert store.thumb_get(sid, 0, 300, 1) is None

    def test_only_a_pages_own_iiif_image_is_marked_kept(self, seam, request):
        nli = _FakeNli({SID: ['901', '902']}, iiif_ok=['901'], rosetta_ok=['902'])
        p1, p2 = nli.patches()
        with p1, p2:
            assert seam['_fetch_nli_image_bytes'](SID, 0, width=2000, with_kept=True)[2:] == ('901', True)
            # Page 1 from Rosetta: its own page, but a stand-in for the IIIF image.
            assert seam['_fetch_nli_image_bytes'](SID, 1, width=2000, with_kept=True)[2:] == ('902', False)
            # From the memory cache, the same answers.
            assert seam['_fetch_nli_image_bytes'](SID, 0, width=2000, with_kept=True)[3] is True
            assert seam['_fetch_nli_image_bytes'](SID, 1, width=2000, with_kept=True)[3] is False
            # The three-item answer the other callers read is unchanged.
            assert len(seam['_fetch_nli_image_bytes'](SID, 0, width=2000)) == 3
            # A disk thumbnail is a page's own IIIF image.
            seam['_fetch_nli_image_bytes'](SID, 0, width=300)
        request.node.clear_memory()
        assert seam['_fetch_nli_image_bytes'](SID, 0, width=300, with_kept=True)[2:] == ('', True)

    def test_full_size_images_stay_off_disk(self, seam):
        nli = _FakeNli({SID: ['801']}, iiif_ok=['801'])
        p1, p2 = nli.patches()
        with p1, p2:
            assert seam['_fetch_nli_image_bytes'](SID, 0, width=2000)[0] == JPEG
        assert not os.path.exists(store.thumb_cache_dir())


class TestProxyRouteCacheHeaders:
    """What /api/nli_image_by_sysid lets a browser and Cloudflare keep."""

    @pytest.fixture()
    def client(self, seam):
        from fastapi.testclient import TestClient
        import web.api as api
        app = FastAPI()
        api.init_api_routes(app_override=app)
        return TestClient(app), api._api_test_seam

    def test_a_pages_own_image_is_kept_a_stand_in_and_a_miss_are_not(self, client):
        import web.api as api
        http, routes_seam = client
        nli = _FakeNli({SID: ['951', '952', '953']}, iiif_ok=['951'], rosetta_ok=['952'])
        p1, p2 = nli.patches()
        with p1, p2:
            own = http.get(f'/api/nli_image_by_sysid/{SID}?page=0')
            stand_in = http.get(f'/api/nli_image_by_sysid/{SID}?page=1')
            missing = http.get(f'/api/nli_image_by_sysid/{SID}?page=2')
        assert own.status_code == 200
        assert own.headers['cache-control'] == f'public, max-age={api.IMAGE_HTTP_MAX_AGE}'
        assert stand_in.status_code == 200 and stand_in.headers['cache-control'] == 'no-store'
        # Page 2 has its own FL id and NLI did not deliver it: not page 0's image.
        assert missing.status_code == 404 and missing.headers['cache-control'] == 'no-store'

    def test_the_middle_image_is_worked_out_from_the_list_and_kept(self, client):
        import web.api as api
        http, _seam = client
        nli = _FakeNli({SID: ['961', '962', '963', '964', '965']}, iiif_ok=['963'])
        p1, p2 = nli.patches()
        with p1, p2:
            middle = http.get(f'/api/nli_image_by_sysid/{SID}?page=0&width=300&middle=1')
            first = http.get(f'/api/nli_image_by_sysid/{SID}?page=0&width=300')
        assert middle.status_code == 200 and middle.content == JPEG       # 963 alone is given: the middle
        assert middle.headers['cache-control'] == f'public, max-age={api.IMAGE_HTTP_MAX_AGE}'
        assert first.status_code == 404 and nli.manifest_calls == 1       # the list is read once

    def test_the_middle_of_a_list_that_is_not_the_whole_one_is_not_kept(self, client):
        http, _seam = client
        nli = _FakeNli({}, iiif_ok=['971'], marc={SID: ['971']})
        p1, p2 = nli.patches()
        with p1, p2:
            got = http.get(f'/api/nli_image_by_sysid/{SID}?middle=1&width=300')
        assert got.status_code == 200 and got.headers['cache-control'] == 'no-store'

    def test_a_refusal_is_not_an_nli_outage(self, client):
        import shared.nli_circuit_breaker as breaker
        _http, routes_seam = client
        nli = _FakeNli({SID: ['981', '982', '983', '984', '985']})
        refusal = b'<error>\n    <parameter>unauthorized</parameter>\n</error>'

        def get(url, *a, **kw):
            return nli.get(url) if url.endswith('/manifest') else _Resp(500, 'text/xml', body[0])
        for body, opened in (([refusal], False), ([b'<error/>'], True)):
            with patch('shared.puzzle_image_service.get_with_checked_redirects', side_effect=get), \
                    patch('web.api.nli_image_get', side_effect=nli.rosetta):
                for page in range(4):
                    assert routes_seam['_fetch_nli_image_bytes'](SID, page) is None
            assert breaker.is_open() is opened

    def test_rosetta_s_refusal_is_not_an_nli_outage_either(self, client):
        import shared.nli_circuit_breaker as breaker
        http, routes_seam = client
        nli = _FakeNli({SID: ['991', '992', '993', '994', '995']})
        refused = _Resp(500, 'text/xml', b'<error><parameter>unauthorized</parameter></error>')

        def get(url, *a, **kw):
            return nli.get(url) if url.endswith('/manifest') else refused
        with patch('shared.puzzle_image_service.get_with_checked_redirects', side_effect=get), \
                patch('web.api.nli_image_get', side_effect=lambda *a, **kw: refused):
            for page in range(4):
                assert routes_seam['_fetch_nli_image_bytes'](SID, page) is None
                assert http.get(f'/api/nli_image/99{page}').status_code == 404
                http.get('/api/proxy_image', params={
                    'url': f'https://iiif.nli.org.il/IIIFv21/FL99{page}/full/400,/0/default.jpg'})
        assert breaker._state_snapshot()['consecutive_failures'] == 0
