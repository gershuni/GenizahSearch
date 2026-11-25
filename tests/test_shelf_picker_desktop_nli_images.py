# -*- coding: utf-8 -*-
"""The images NLI's server gives only to its own site (the National Library of Russia's: blank in the desktop
picker, 2026-11-25). NLI answered them HTTP 500 "unauthorized" for want of its ``Referer``, the picker counted
each as an outage, three closed NLI off for a minute, and the tiles asked meanwhile stayed at "none".

* Every request to an NLI image host carries the ``Referer``; no other host gets it, also past a redirect.
* A refusal (``nli_refused``) is not counted toward NLI's circuit breaker; a real 5xx is.
* A picture that did not come is asked for again later, a settled "none" is not.
* A microfilm's tile is its middle image, the one its details open on.
"""
from __future__ import annotations

import os
import time

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PyQt6.QtCore import QBuffer, QByteArray, QIODevice, Qt
from PyQt6.QtGui import QColor, QImage, QPixmap
from PyQt6.QtWidgets import QApplication

import genizah_core
import desktop.widgets.shelfmark_tree_picker as picker
import shared.nli_circuit_breaker as breaker
import shared.nli_fetch as nli_fetch
from shared.shelfmark_tree_rows import STORAGE_KEY
from shared.shelfmark_tree_service import ShelfmarkTreeService
from tests.shelfmark_tree_fixture import build_fixture

pytestmark = pytest.mark.gui

_APP = QApplication.instance() or QApplication([])
_ROW = Qt.ItemDataRole.UserRole

NLI = 'https://iiif.nli.org.il/IIIFv21/FL{fl}/full/400,/0/default.jpg'
REFUSAL = (b'<?xml version="1.0" encoding="UTF-8" standalone="yes"?>\n<error>\n'
           b'    <parameter>unauthorized</parameter>\n    <text>Error, no permissions</text>\n</error>')


class _Resp:
    def __init__(self, status, content=b'', next_url=None):
        self.status_code, self.content, self.headers = status, content, {}
        self.is_redirect = next_url is not None
        self.next = type('_Next', (), {'url': next_url})() if next_url else None

    def close(self):
        pass


def _image():
    image = QImage(60, 40, QImage.Format.Format_RGB32)
    image.fill(QColor('#806040'))
    return image


def _png():
    data = QByteArray()
    buf = QBuffer(data)
    buf.open(QIODevice.OpenModeFlag.WriteOnly)
    _image().save(buf, 'PNG')
    return bytes(data)


@pytest.fixture(autouse=True)
def _isolated(tmp_path, monkeypatch):
    for var in ("USERPROFILE", "HOME", "LOCALAPPDATA", "APPDATA"):
        folder = tmp_path / "profile" / var
        folder.mkdir(parents=True)
        monkeypatch.setenv(var, str(folder))
    config = {STORAGE_KEY: {'view': 'gallery', 'size': 'l'}}
    monkeypatch.setattr(genizah_core, "load_app_config", lambda: dict(config))
    monkeypatch.setattr(genizah_core, "save_app_config", lambda new: config.update(new))
    monkeypatch.setattr(genizah_core, "CURRENT_LANG", "en")
    monkeypatch.delenv("SHELFMARK_TREE_ENABLED", raising=False)
    for hook in ("title_of", "catalogue", "page_count_of", "library_code_of", "page_texts_of", "community", "signs"):
        monkeypatch.setattr(picker, hook, None)
    monkeypatch.setattr(picker, '_thumb_cache', picker.OrderedDict())
    monkeypatch.setattr(picker, '_table_widths', {})
    breaker._reset_for_tests()
    yield
    breaker._reset_for_tests()
    _APP.setLayoutDirection(Qt.LayoutDirection.LeftToRight)


@pytest.fixture
def svc(tmp_path):
    db, _libraries, _meta = build_fixture(tmp_path)
    service = ShelfmarkTreeService(str(db), thread_safe=True)
    yield service
    service.close()


def _wait(pred, timeout=8.0):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        _APP.processEvents()
        if pred():
            return True
        time.sleep(0.01)
    _APP.processEvents()
    return pred()


def _open(svc):
    dialog = picker.ShelfPickerDialog(svc, start="vol:100")
    dialog.resize(1240, 520)
    dialog.show()
    assert _wait(lambda: dialog._view is not None and not dialog._loads.busy()), "the picker never drew"
    return dialog


def _close(dialog):
    dialog.reject()
    dialog.deleteLater()
    _APP.processEvents()


def _first(dialog):
    item = dialog.leaf_panel.gallery.item(0)
    return item, item.data(_ROW).sys_id


# -- the Referer --------------------------------------------------------------------------------------

def test_the_picker_s_request_carries_nli_s_referer(monkeypatch):
    sent = []

    def get(url, headers=None, **_kw):
        sent.append(headers)
        return _Resp(200, _png())
    monkeypatch.setattr(nli_fetch.requests, 'get', get)
    assert picker._image_at(NLI.format(fl=1)) is not None
    assert sent[0]['Referer'] == nli_fetch.NLI_REFERER
    assert sent[0]['User-Agent'] == genizah_core.Config.HTTP_HEADERS['User-Agent']
    assert 'Referer' not in genizah_core.Config.HTTP_HEADERS       # every other host is asked as before


def test_only_nli_s_hosts_get_it_also_past_a_redirect(monkeypatch):
    sent = []

    def get(url, headers=None, **_kw):
        sent.append((url, dict(headers or {})))
        return _Resp(302, next_url='https://example.org/x.jpg') if 'nli.org.il' in url else _Resp(200, b'x')
    monkeypatch.setattr(nli_fetch.requests, 'get', get)
    nli_fetch.nli_image_get(NLI.format(fl=1), headers={'User-Agent': 'a'})
    assert sent[0][1]['Referer'] == nli_fetch.NLI_REFERER
    assert sent[1] == ('https://example.org/x.jpg', {'User-Agent': 'a'})
    sent.clear()
    nli_fetch.nli_image_get(NLI.format(fl=1), headers={'referer': 'https://own/'}, allow_redirects=False)
    assert sent[0][1] == {'referer': 'https://own/'}                # the caller's own stays


def test_the_puzzle_s_fetches_give_it_hop_by_hop(monkeypatch):
    import shared.puzzle_image_service as pis
    sent = []

    class _Hop:
        def __init__(self, status, location=''):
            self.status_code, self.content = status, b'x' * 200
            self.headers = {'Location': location} if location else {}
            self.is_redirect = bool(location)

        def close(self):
            pass
    target = 'https://images.lib.cam.ac.uk/iiif/x.jp2/full/400,/0/default.jpg'

    def get(url, headers=None, **_kw):
        sent.append((url, dict(headers or {})))
        return _Hop(302, target) if 'nli.org.il' in url else _Hop(200)
    monkeypatch.setattr(pis.requests, 'get', get)
    for fetch in (pis.get_with_checked_redirects, pis._image_get):      # the website's, the desktop's
        sent.clear()
        assert fetch(NLI.format(fl=1), headers={'User-Agent': 'a'}).status_code == 200
        assert sent[0][1]['Referer'] == nli_fetch.NLI_REFERER
        assert sent[1] == (target, {'User-Agent': 'a'})
    sent.clear()
    pis._image_get(target, headers={'User-Agent': 'a'})
    assert sent == [(target, {'User-Agent': 'a'})]


# -- the breaker --------------------------------------------------------------------------------------

def test_refusals_never_close_nli_off_and_an_outage_does(monkeypatch):
    monkeypatch.setattr(nli_fetch, 'nli_image_get', lambda url, **_kw: _Resp(500, REFUSAL))
    for fl in range(5):
        assert picker._load_image(NLI.format(fl=fl)) == (None, True)
    assert not breaker.is_open()
    monkeypatch.setattr(nli_fetch, 'nli_image_get', lambda url, **_kw: _Resp(500, b'<html>busy</html>'))
    for fl in range(3):
        assert picker._load_image(NLI.format(fl=fl)) == (None, False)
    assert breaker.is_open()
    monkeypatch.setattr(nli_fetch, 'nli_image_get', lambda url, **_kw: pytest.fail('asked while NLI is closed off'))
    assert picker._load_image(NLI.format(fl=9)) == (None, False)


def test_what_a_refusal_is():
    assert nli_fetch.nli_refused(_Resp(500, REFUSAL)) and nli_fetch.nli_refused(_Resp(503, REFUSAL))
    assert not nli_fetch.nli_refused(_Resp(500, b'')) and not nli_fetch.nli_refused(_Resp(404, REFUSAL))
    assert not nli_fetch.nli_refused(_Resp(200, REFUSAL)) and not nli_fetch.nli_refused(object())


def test_an_image_not_there_is_settled_and_a_busy_server_is_not(monkeypatch):
    for status, settled in ((404, True), (403, True), (429, False), (503, False)):
        monkeypatch.setattr(nli_fetch, 'nli_image_get', lambda url, s=status, **_kw: _Resp(s))
        assert picker._load_image(NLI.format(fl=1)) == (None, settled), status
        breaker._reset_for_tests()

    def down(url, **_kw):
        raise OSError('no network')
    monkeypatch.setattr(nli_fetch, 'nli_image_get', down)
    assert picker._load_image(NLI.format(fl=1)) == (None, False)
    assert picker._load_image('') == (None, False)
    monkeypatch.setattr(picker, 'thumbnail_url', lambda sid, meta, width=400: NLI.format(fl=1))
    assert picker._thumbnail_image('1', object()) is picker.NOT_COME     # the tile's answer says so


# -- a tile that did not come -------------------------------------------------------------------------

def test_a_tile_that_did_not_come_is_asked_for_again_and_drawn(svc, monkeypatch):
    asked, down = [], [True]

    def thumb(sid, _meta, *_w):
        asked.append(sid)
        return picker.NOT_COME if down[0] else _image()
    monkeypatch.setattr(picker, '_thumbnail_image', thumb)
    dialog = _open(svc)
    try:
        item, sid = _first(dialog)
        assert _wait(lambda: sid in dialog._thumb_tries and not dialog._thumb_pool.running())
        assert dialog.known_thumb(sid) == (False, None)
        assert _wait(lambda: item.data(picker._THUMB_ROLE) is False)    # the dash, until it comes
        dialog.leaf_panel.queue_visible()                           # scrolled past again: not asked at once
        assert _wait(lambda: not dialog._thumb_pool.running())
        assert asked.count(sid) == 1 and dialog._retry_timer.isActive()
        down[0] = False
        monkeypatch.setattr(picker, 'THUMB_RETRY_S', 0.0)           # the wait is over
        dialog._retry_timer.stop()
        dialog._retry_thumbs()
        assert _wait(lambda: isinstance(item.data(picker._THUMB_ROLE), QPixmap))
        assert asked.count(sid) == 2 and sid not in dialog._thumb_tries
    finally:
        _close(dialog)


def test_it_counts_as_none_after_its_tries_and_a_settled_none_is_not_asked_again(svc, monkeypatch):
    asked = []
    answer = [picker.NOT_COME]
    monkeypatch.setattr(picker, '_thumbnail_image', lambda sid, _m, *_w: asked.append(sid) or answer[0])
    monkeypatch.setattr(picker, 'THUMB_RETRY_S', 0.0)
    dialog = _open(svc)
    try:
        item, sid = _first(dialog)

        def tried(n):
            dialog._retry_thumbs()
            return _wait(lambda: asked.count(sid) >= n and not dialog._thumb_pool.running())
        assert _wait(lambda: asked.count(sid) >= 1)
        for n in range(2, picker.THUMB_TRIES + 1):
            assert tried(n)
        assert _wait(lambda: dialog.known_thumb(sid) == (True, None))
        assert item.data(picker._THUMB_ROLE) is False
        asked.clear()
        dialog._retry_thumbs()
        _wait(lambda: False, 0.4)
        assert sid not in asked
    finally:
        _close(dialog)
    asked.clear()
    answer[0] = None                                                # settled: none, at the first answer
    dialog = _open(svc)
    try:
        _item, sid = _first(dialog)
        assert _wait(lambda: dialog.known_thumb(sid) == (True, None))
        dialog._retry_thumbs()
        _wait(lambda: False, 0.4)
        assert asked.count(sid) == 1 and not dialog._thumb_tries
    finally:
        _close(dialog)


def test_the_details_picture_is_asked_for_again_too(svc, monkeypatch):
    monkeypatch.setattr(picker, 'catalogue', object())
    monkeypatch.setattr(picker, '_image_list', lambda sid, meta: ([], '', 0))
    monkeypatch.setattr(genizah_core, "load_app_config", lambda: {STORAGE_KEY: {'view': 'list', 'size': 'm'}})
    down = [True]
    monkeypatch.setattr(picker, '_thumbnail_image', lambda sid, _m, *_w: picker.NOT_COME if down[0] else _image())
    dialog = _open(svc)
    try:
        leaf = dialog.leaf_panel.leaves['rows'][0]['leaf']
        dialog.details.show_leaf(leaf)
        assert _wait(lambda: leaf.sys_id in dialog._thumb_tries and not dialog._thumb_pool.running())
        down[0] = False
        monkeypatch.setattr(picker, 'THUMB_RETRY_S', 0.0)
        dialog._retry_thumbs()
        assert _wait(lambda: not dialog.details.image.pixmap().isNull())
    finally:
        _close(dialog)


# -- a microfilm's tile -------------------------------------------------------------------------------

def test_a_microfilm_s_tile_is_its_middle_image(monkeypatch):
    pages = [NLI.format(fl=n) for n in range(1, 8)]
    monkeypatch.setattr(picker, 'page_image_urls', lambda sid, meta, width=400: list(pages))
    monkeypatch.setattr(picker, 'thumbnail_url', lambda sid, meta, width=400: NLI.format(fl=1))
    asked = []
    monkeypatch.setattr(picker, '_load_image', lambda url: (asked.append(url), (_image(), True))[1])
    monkeypatch.setattr(picker, 'library_code_of', lambda sid: 'RNL')
    assert isinstance(picker._thumbnail_image('1', object()), QImage)
    assert asked == [pages[3]]
    listed, tile, start = picker._image_list('1', object())
    assert (listed, tile, start) == (pages, pages[3], 3)            # the details' own: not fetched twice
    asked.clear()
    monkeypatch.setattr(picker, 'page_image_urls', lambda sid, meta, width=400: [])
    picker._thumbnail_image('1', object())
    assert asked == [NLI.format(fl=1)]                              # no list: its catalogue thumbnail
    asked.clear()
    monkeypatch.setattr(picker, 'library_code_of', lambda sid: 'CUL')
    monkeypatch.setattr(picker, 'page_image_urls', lambda *_a, **_kw: pytest.fail('a list for a tile'))
    picker._thumbnail_image('1', object())
    assert asked == [NLI.format(fl=1)]


def test_a_microfilm_s_details_fetch_no_second_picture(svc, monkeypatch):
    pages = [NLI.format(fl=n) for n in range(1, 8)]
    monkeypatch.setattr(picker, 'catalogue', object())
    monkeypatch.setattr(picker, '_image_list', lambda sid, meta: (pages, pages[3], 3))
    monkeypatch.setattr(picker, '_thumbnail_image', lambda sid, _m, *_w: _image())
    asked = []
    monkeypatch.setattr(picker, '_image_at', lambda url: asked.append(url) or _image())
    dialog = _open(svc)
    try:
        dialog.details.show_leaf(dialog.leaf_panel.leaves['rows'][0]['leaf'])
        assert _wait(lambda: len(dialog.details.pages) == 7 and not dialog.details.image.pixmap().isNull())
        assert dialog.details.page == 3
        assert _wait(lambda: pages[4] in asked)                     # the one after it, ahead
        assert pages[3] not in asked
    finally:
        _close(dialog)
