# -*- coding: utf-8 -*-
"""Browse's thumbnail loader is let go of once it is handed over to be deleted (2026-11-22).

``cancel_browse_image_thread`` keeps a running loader in ``_browse_inflight``, which deletes
it when it finishes. The window kept pointing at it, and ``fetch_browse_thumbnail`` returns
early (no thumbnail URL, a Bodleian refresh) without starting another, so the next call -- the
next page, the window closing -- raised "wrapped C/C++ object of type ImageLoaderThread has been
deleted" and the page stopped drawing. Browse's instant draws (PR #98) took that path on almost
every move. The real GenizahGUI methods run on a stand-in window with a real QThread loader.
"""
from __future__ import annotations

import os
import threading
from types import SimpleNamespace

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PyQt6 import sip
from PyQt6.QtCore import QCoreApplication, QEvent, QThread, pyqtSignal
from PyQt6.QtTest import QTest
from PyQt6.QtWidgets import QLabel

pytestmark = pytest.mark.gui

SID = "990051247450205171"
LOADERS = []


class _Loader(QThread):
    """Runs until cancelled or released, like a download."""
    image_loaded = pyqtSignal(object)
    load_failed = pyqtSignal()

    def __init__(self, url):
        super().__init__()
        self.url = url
        self.stop = threading.Event()
        LOADERS.append(self)

    def cancel(self):
        self.stop.set()

    def run(self):
        self.stop.wait(5)


@pytest.fixture(autouse=True)
def _clean(monkeypatch):
    import shared.codicological as codicological
    monkeypatch.setattr(codicological, "bodleian_cache_needs_refresh", lambda *a: False)
    LOADERS.clear()
    yield
    for t in LOADERS:
        if not sip.isdeleted(t):
            t.cancel()
            t.wait(2000)
    LOADERS.clear()


def _window(monkeypatch, cache):
    import genizah_app
    G = genizah_app.GenizahGUI
    monkeypatch.setattr(genizah_app, "ImageLoaderThread", _Loader)
    w = SimpleNamespace(current_browse_sid=SID, browse_img_thread=None, browse_thumb=QLabel(),
                        meta_mgr=SimpleNamespace(nli_cache=cache))
    for name in ("cancel_browse_image_thread", "_cleanup_browse_inflight", "fetch_browse_thumbnail",
                 "start_browse_download", "on_browse_img_loaded", "on_browse_img_failed"):
        # plain functions: PyQt refuses a slot bound to a non-QObject stand-in
        setattr(w, name, (lambda f: lambda *a: f(w, *a))(getattr(G, name)))
    return w


def _until_deleted(thread, ms=3000):
    for _ in range(ms // 10):
        if sip.isdeleted(thread):
            return True
        QTest.qWait(10)
        QCoreApplication.sendPostedEvents(None, QEvent.Type.DeferredDelete)
    return sip.isdeleted(thread)


def test_a_page_without_a_thumbnail_after_a_cancelled_loader_raises_nothing(monkeypatch):
    with_thumb = {'thumb_url': 'https://iiif.nli.org.il/IIIFv21/FL1/full/400,/0/default.jpg'}
    w = _window(monkeypatch, {SID: with_thumb})
    w.fetch_browse_thumbnail(SID)
    [first] = LOADERS
    assert first.isRunning()
    w.fetch_browse_thumbnail(SID, {'sys_id': SID})     # no thumbnail: returns early
    assert w.browse_thumb.text() == 'No Image'
    assert _until_deleted(first), 'the cancelled loader was never deleted'
    w.fetch_browse_thumbnail(SID, {'sys_id': SID})     # the next move: raised here
    w.cancel_browse_image_thread()                         # what the window's close calls
    w.fetch_browse_thumbnail(SID)                           # and a thumbnail loads again
    assert len(LOADERS) == 2 and LOADERS[1].isRunning()
    assert w.browse_img_thread is LOADERS[1]


def test_the_cancelled_loader_is_kept_until_it_finishes_and_no_longer_held_by_the_window(monkeypatch):
    w = _window(monkeypatch, {})
    w.start_browse_download(SID, 'https://example.org/thumb.jpg')
    [loader] = LOADERS
    w.cancel_browse_image_thread()
    assert w.browse_img_thread is None, 'the window still points at a loader that will be deleted'
    assert w._browse_inflight == [loader], 'a running loader was let go'
    assert loader.stop.is_set()
    w.cancel_browse_image_thread()                         # a second cancel does not keep it twice
    assert w._browse_inflight == [loader]
    assert _until_deleted(loader)
    assert w._browse_inflight == []


class _StopsInsideCancel(_Loader):
    """Has finished before ``cancel`` returns: the order the Linux CI ran the test above in, where
    the loader stopped between the cancel and the connection of its ``finished`` signal."""

    def cancel(self):
        super().cancel()
        self.wait(2000)


def test_a_loader_that_finishes_inside_the_cancel_is_still_deleted(monkeypatch):
    import genizah_app
    w = _window(monkeypatch, {})
    monkeypatch.setattr(genizah_app, "ImageLoaderThread", _StopsInsideCancel)
    w.start_browse_download(SID, 'https://example.org/thumb.jpg')
    [loader] = LOADERS
    w.cancel_browse_image_thread()
    assert w.browse_img_thread is None
    assert _until_deleted(loader), 'a loader that finished before the cleanup was connected is kept'
    assert w._browse_inflight == []


class _FinishedAfterTheCheck(_Loader):
    """Seen running, then finished before ``finished`` is connected: its signal is gone."""

    def isRunning(self):
        return True


def test_a_loader_that_finished_before_the_connection_is_deleted_on_the_next_turn(monkeypatch):
    import genizah_app
    w = _window(monkeypatch, {})
    monkeypatch.setattr(genizah_app, "ImageLoaderThread", _FinishedAfterTheCheck)
    w.start_browse_download(SID, 'https://example.org/thumb.jpg')
    [loader] = LOADERS
    loader.stop.set()
    assert loader.wait(2000)
    QTest.qWait(50)                                        # its finished signal is delivered to no one
    w.cancel_browse_image_thread()
    assert w._browse_inflight == [loader], 'cleaned up inside the cancel, not on the next turn'
    assert _until_deleted(loader), 'a loader that finished before the connection is kept'
    assert w._browse_inflight == []


def test_a_loader_deleted_while_still_held_is_dropped_quietly(monkeypatch):
    w = _window(monkeypatch, {})
    loader = _Loader('https://example.org/thumb.jpg')
    loader.deleteLater()
    assert _until_deleted(loader)
    w.browse_img_thread = loader
    w.cancel_browse_image_thread()
    assert w.browse_img_thread is None
