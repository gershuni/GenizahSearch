# -*- coding: utf-8 -*-
"""The desktop image loader's cache (2026-11-22, faster Browse images).

- One disk file per size (GS-66: a 400-px preview and the 2000-px page shared one file).
- A request for an image another request is downloading waits for it (a preload, a
  neighbour prefetch) instead of downloading it again.
- A completed download is kept even when its view moved on; files are written atomically
  (GS-76 d).
- Decoded images are kept in memory within a budget.
- A preview-sized image in an old page file is not shown as the page.
- Library connections come from a pool of sessions, one download per session at a time.

The loader's run() is called on the test's thread (no QThread started), with the network
replaced; the cache folder is a temporary one.
"""
from __future__ import annotations

import os
import threading
import time

import pytest
from PyQt6.QtCore import QBuffer, QByteArray, QIODevice
from PyQt6.QtGui import QColor, QImage

from desktop import image_loader
from desktop.image_loader import ImageLoaderThread, cache_path_for

pytestmark = pytest.mark.gui

NLI = "https://iiif.nli.org.il/IIIFv21/FL4242"
CUDL = "https://images.lib.cam.ac.uk/iiif/MS-TS-00013-J-00019-00011-000-00002.jp2"


def _jpeg(width, height=None):
    img = QImage(width, height or width, QImage.Format.Format_RGB32)
    img.fill(QColor(120, 90, 60))
    data = QByteArray()
    buf = QBuffer(data)
    buf.open(QIODevice.OpenModeFlag.WriteOnly)
    img.save(buf, "JPEG", 85)
    return bytes(data)


class _Resp:
    def __init__(self, content, status=200):
        self.status_code = status
        self.content = content
        self.headers = {'Content-Type': 'image/jpeg'}

    def close(self):
        pass


@pytest.fixture(autouse=True)
def cache_dir(tmp_path, monkeypatch):
    monkeypatch.setattr(image_loader.Config, "IMAGE_CACHE_DIR", str(tmp_path))
    image_loader.clear_memory()
    yield tmp_path
    image_loader.clear_memory()


def _load(url):
    """run() on this thread; the image it emitted, or 'failed'."""
    got = []
    loader = ImageLoaderThread(url)
    loader.image_loaded.connect(lambda img: got.append(img))
    loader.load_failed.connect(lambda: got.append('failed'))
    loader.run()
    return got[0] if got else None


def test_each_size_has_its_own_file(cache_dir):
    assert cache_path_for(f"{NLI}/full/2000,/0/default.jpg").endswith("FL4242_v2.jpg")  # the old name
    assert cache_path_for(f"{NLI}/full/400,/0/default.jpg").endswith("FL4242_w400.jpg")
    assert cache_path_for(f"{CUDL}/full/400,/0/default.jpg") != cache_path_for(f"{CUDL}/full/2000,/0/default.jpg")
    assert cache_path_for(None) is None


def test_a_preview_and_its_page_do_not_share_a_file(cache_dir, monkeypatch):
    sizes = {'400': _jpeg(400, 560), '2000': _jpeg(2000, 2800)}
    monkeypatch.setattr(image_loader, "nli_image_get",
                        lambda url, **kw: _Resp(sizes['400' if '/400,' in url else '2000']))
    assert _load(f"{NLI}/full/400,/0/default.jpg").width() == 400
    image_loader.clear_memory()
    assert _load(f"{NLI}/full/2000,/0/default.jpg").width() == 2000, 'the page came back at preview size'
    assert sorted(os.listdir(cache_dir)) == ["FL4242_v2.jpg", "FL4242_w400.jpg"]


def test_an_old_preview_sized_page_file_is_not_the_page(cache_dir, monkeypatch):
    with open(cache_dir / "FL4242_v2.jpg", "wb") as f:   # what the old cache could hold (GS-66)
        f.write(_jpeg(400, 560))
    monkeypatch.setattr(image_loader, "nli_image_get", lambda url, **kw: _Resp(_jpeg(2000, 2800)))
    assert _load(f"{NLI}/full/2000,/0/default.jpg").width() == 2000
    image_loader.clear_memory()
    assert _load(f"{NLI}/full/2000,/0/default.jpg").width() == 2000   # now from the rewritten file


def test_two_requests_for_one_image_download_it_once(cache_dir, monkeypatch):
    calls = []
    entered, release = threading.Event(), threading.Event()

    def slow_get(url, **kw):
        calls.append(url)
        entered.set()
        release.wait(5)
        return _Resp(_jpeg(1000, 1400))

    monkeypatch.setattr(image_loader, "nli_image_get", slow_get)
    url = f"{CUDL}/full/2000,/0/default.jpg"
    results = {}
    first = threading.Thread(target=lambda: results.setdefault('a', _load(url)))
    first.start()
    assert entered.wait(5)
    second = threading.Thread(target=lambda: results.setdefault('b', _load(url)))
    second.start()
    time.sleep(0.3)                     # the second is waiting, not downloading
    release.set()
    first.join(10)
    second.join(10)
    assert len(calls) == 1, f'downloaded {len(calls)} times'
    assert results['a'].width() == results['b'].width() == 1000
    assert image_loader._inflight == {}


def test_a_failed_download_lets_the_waiting_request_try_itself(cache_dir, monkeypatch):
    calls = []
    entered, release = threading.Event(), threading.Event()

    def get(url, **kw):
        calls.append(url)
        if len(calls) == 1:
            entered.set()
            release.wait(5)
            return _Resp(b'', status=503)
        return _Resp(_jpeg(800, 1000))

    monkeypatch.setattr(image_loader, "nli_image_get", get)
    url = f"{CUDL}/full/2000,/0/default.jpg"
    results = {}
    first = threading.Thread(target=lambda: results.setdefault('a', _load(url)))
    first.start()
    assert entered.wait(5)
    second = threading.Thread(target=lambda: results.setdefault('b', _load(url)))
    second.start()
    time.sleep(0.2)
    release.set()
    first.join(10)
    second.join(10)
    assert results['a'] == 'failed'
    assert results['b'].width() == 800
    assert len(calls) == 2


def test_a_download_whose_view_moved_on_is_kept(cache_dir, monkeypatch):
    url = f"{CUDL}/full/2000,/0/default.jpg"
    loader = ImageLoaderThread(url)
    emitted = []
    loader.image_loaded.connect(lambda img: emitted.append(img))

    body = _jpeg(900, 1200)

    class _Streaming(_Resp):
        @property
        def content(self):
            loader.cancel()             # the reader moved on while the bytes came
            return body

        @content.setter
        def content(self, _value):
            pass

    monkeypatch.setattr(image_loader, "nli_image_get", lambda u, **kw: _Streaming(b''))
    loader.run()
    assert os.path.exists(cache_path_for(url)), 'the whole download was thrown away'
    assert not emitted, 'a cancelled load still reported its image'
    assert not [n for n in os.listdir(cache_dir) if n.endswith('.tmp')]


def test_decoded_images_are_kept_in_memory_within_a_budget(cache_dir, monkeypatch):
    monkeypatch.setattr(image_loader, "nli_image_get", lambda url, **kw: _Resp(_jpeg(500, 500)))
    first = f"{CUDL}/full/2000,/0/default.jpg"
    assert image_loader.memory_image(first) is None
    _load(first)
    assert image_loader.memory_image(first) is not None
    one = image_loader.memory_image(first).sizeInBytes()
    monkeypatch.setattr(image_loader, "MEMORY_BUDGET_BYTES", one * 2)
    for n in range(3):
        _load(f"https://images.lib.cam.ac.uk/iiif/other-{n}.jp2/full/2000,/0/default.jpg")
    assert image_loader.memory_image(first) is None, 'the oldest image outlived the budget'
    assert image_loader.memory_image("https://images.lib.cam.ac.uk/iiif/other-2.jp2/full/2000,/0/default.jpg")


def test_downloads_use_a_leased_session_and_give_it_back(cache_dir, monkeypatch):
    seen = []
    monkeypatch.setattr(image_loader, "nli_image_get",
                        lambda url, **kw: seen.append(kw.get('session')) or _Resp(_jpeg(300, 300)))
    while not image_loader._sessions.empty():
        image_loader._sessions.get_nowait()
    _load(f"{CUDL}/full/400,/0/default.jpg")
    _load(f"{CUDL}/full/2000,/0/default.jpg")
    assert seen[0] is not None and seen[0] is seen[1], 'the second download opened a new session'


def test_a_claim_left_by_a_terminated_loader_does_not_hold_later_requests(cache_dir, monkeypatch):
    """Codex on #98: a loader terminated at shutdown never releases its claim. A claim
    older than the wait is taken over at once; the image is downloaded and the claim gone."""
    url = f"{CUDL}/full/2000,/0/default.jpg"
    path = cache_path_for(url)
    image_loader._inflight[path] = (threading.Event(), time.monotonic() - image_loader.INFLIGHT_WAIT_SECONDS - 1)
    monkeypatch.setattr(image_loader, "nli_image_get", lambda u, **kw: _Resp(_jpeg(700, 900)))
    start = time.monotonic()
    assert _load(url).width() == 700
    assert time.monotonic() - start < 2, 'waited on a dead claim'
    assert image_loader._inflight == {}


def test_a_request_that_waited_out_a_silent_claim_takes_it_over(cache_dir, monkeypatch):
    url = f"{CUDL}/full/2000,/0/default.jpg"
    path = cache_path_for(url)
    monkeypatch.setattr(image_loader, "INFLIGHT_WAIT_SECONDS", 0.3)
    # live by its age (claimed 'later' than now), but never ends: only the take-over frees it
    image_loader._inflight[path] = (threading.Event(), time.monotonic() + 60)
    monkeypatch.setattr(image_loader, "nli_image_get", lambda u, **kw: _Resp(_jpeg(640, 900)))
    assert _load(url).width() == 640
    assert image_loader._inflight == {}, 'the claim it took over was not released'
    image_loader.clear_memory()
    start = time.monotonic()
    assert _load(url).width() == 640                 # from disk, no wait
    assert time.monotonic() - start < 0.3


def test_a_rosetta_thumbnail_answered_for_a_page_is_shown_but_not_kept_as_the_page(cache_dir, monkeypatch):
    """Codex on #98: the 2000-px IIIF image and the Rosetta stream fail, the Rosetta thumbnail
    answers. It is shown for that load, but memory (like the disk) never serves it as the page,
    so the next request tries the page again."""
    calls = []

    def get(url, **kw):
        calls.append(url)
        if 'dps_func=thumbnail' in url:
            return _Resp(_jpeg(600, 800))
        return _Resp(b'', status=503)

    monkeypatch.setattr(image_loader, "nli_image_get", get)
    monkeypatch.setattr(image_loader.MetadataManager, "get_rosetta_fallback_url",
                        staticmethod(lambda fl: f"https://rosetta.nli.org.il/stream?FL{fl}"))
    url = f"{NLI}/full/2000,/0/default.jpg"
    assert _load(url).width() == 600                       # shown: better than nothing
    assert image_loader.memory_image(url) is None, 'the thumbnail is kept as the page'
    first = len(calls)
    _load(url)
    assert any('iiif.nli.org.il' in u for u in calls[first:]), 'the page was not tried again'


def test_each_memory_layer_refuses_a_preview_under_a_page_name(cache_dir):
    page = f"{NLI}/full/2000,/0/default.jpg"
    path = cache_path_for(page)
    small = QImage.fromData(_jpeg(600, 800))
    image_loader._remember(path, small)
    assert path not in image_loader._memory, 'kept under the page name'
    with image_loader._memory_lock:
        image_loader._memory[path] = small                 # however it got there
    assert image_loader.memory_image(page) is None, 'served as the page'
    preview = f"{NLI}/full/400,/0/default.jpg"
    image_loader._remember(cache_path_for(preview), small)
    assert image_loader.memory_image(preview) is not None, 'a preview under its own name is fine'
