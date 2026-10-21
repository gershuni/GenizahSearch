# -*- coding: utf-8 -*-
"""Fragment Puzzle page: exports, thumbnails and publishing use this browser's images.

Drives the real /puzzle page with a NiceGUI User: one fragment is restored
from the tab's saved canvas, then the Export PNG, Save or Publish button is
clicked and what reaches the browser (or the shared code) is captured.

* The export arrives as the PNG bytes themselves (no file on disk, no URL).
* The export draws this browser's own uploaded image when the shared cache
  has none.
* Saving a join draws its thumbnail with this browser's images, and so does
  publishing it.
"""
from __future__ import annotations

import asyncio
import io
import os
import re
from contextlib import asynccontextmanager
from unittest.mock import patch

import httpx
import pytest
from PIL import Image

import web.main as web_main  # noqa: F401  (registers /puzzle)
import web.pages.puzzle  # noqa: F401
from nicegui import app, core, events, ui
from nicegui.context import context as nicegui_context
from nicegui.testing.general import prepare_simulation
from nicegui.testing.user import User
from nicegui.testing.user_download import UserDownload
from nicegui.ui_run import set_storage_secret
from tests.render_smoke._bounded_run import run_user_driver

FL = '45678901'
KEY = '990000000000001,1r'


def _png(color=(40, 90, 160, 255)) -> bytes:
    buf = io.BytesIO()
    Image.new('RGBA', (80, 60), color=color).save(buf, format='PNG')
    return buf.getvalue()


@pytest.fixture
def image_service(tmp_path, monkeypatch):
    import shared.puzzle_image_service as pis
    pis.reset_puzzle_image_service()
    service = pis.get_puzzle_image_service(cache_dir=tmp_path / 'puzzle')
    monkeypatch.setattr(service, '_fetch_iiif_image', lambda fl_id, size: None)
    monkeypatch.setattr(service, '_fetch_direct_url', lambda url, size: None)
    yield service
    pis.reset_puzzle_image_service()


@pytest.fixture
def downloads(monkeypatch):
    captured = []

    def _record(self, src, filename=None, media_type=''):
        captured.append((src, filename, media_type))

    monkeypatch.setattr(UserDownload, '__call__', _record)
    return captured


@asynccontextmanager
async def _puzzle_user():
    saved_handlers = list(core.app._startup_handlers)
    core.app._startup_handlers.clear()
    try:
        prepare_simulation()
        set_storage_secret('puzzle-export-render-smoke-secret', {})
        with patch('web.main._resolve_ui_language', return_value='en'):
            os.environ['NICEGUI_USER_SIMULATION'] = 'true'
            try:
                async with core.app.router.lifespan_context(core.app):
                    async with httpx.AsyncClient(
                        transport=httpx.ASGITransport(core.app),
                        base_url='http://test',
                    ) as client:
                        yield User(client)
            finally:
                os.environ.pop('NICEGUI_USER_SIMULATION', None)
    finally:
        core.app._startup_handlers.clear()
        core.app._startup_handlers.extend(saved_handlers)


def _run(driver):
    saved_slots = list(nicegui_context.slot_stack)

    try:
        run_user_driver(_puzzle_user, driver)
    finally:
        nicegui_context.slot_stack.clear()
        nicegui_context.slot_stack.extend(saved_slots)


async def _open_with_one_fragment(user):
    user.javascript_rules[re.compile(r'.*puzzleCanvas\.get(Crop)?State\(\)')] = lambda _m: '{}'
    await user.open('/puzzle')
    with user._client:
        # Restored by the page's init_canvas (scheduled 0.5 s after load),
        # which restores a tab's canvas only for the visitor it was left by.
        from web.saved_joins import owner_key
        app.storage.tab['puzzle_owner'] = owner_key()
        app.storage.tab['puzzle_fragments'] = {KEY: {
            'sys_id': '990000000000001', 'shelfmark': 'ENA 1.1', 'folio_label': '1r',
            'fl_id': FL, 'threshold': 30, 'processed': True, 'size': 800,
            'image_url': '', 'external_provider': '', 'page_index': -1,
        }}
        session_id = app.storage.browser['id']
    await asyncio.sleep(1.0)
    return session_id


def _click_export(user):
    with user._client:
        buttons = [el for el in user._client.elements.values()
                   if isinstance(el, ui.button) and (el._props or {}).get('icon') == 'image']
        assert len(buttons) == 1
        for listener in buttons[0]._event_listeners.values():
            if listener.type == 'click':
                events.handle_event(listener.handler, events.GenericEventArguments(
                    sender=buttons[0], client=user._client, args=None))


async def _wait_for(captured, timeout=15.0):
    deadline = asyncio.get_running_loop().time() + timeout
    while not captured:
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError('no download reached the browser')
        await asyncio.sleep(0.1)
    return captured[-1]


def test_png_export_is_sent_as_bytes(image_service, downloads):
    path = image_service.get_cache_path(FL, 800, 30.0, True, False)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(_png())

    async def driver(user):
        await _open_with_one_fragment(user)
        _click_export(user)
        src, filename, media_type = await _wait_for(downloads)
        assert isinstance(src, bytes)
        assert src[:4] == b'\x89PNG'
        assert filename.endswith('.png')
        assert media_type == 'image/png'

    _run(driver)


def test_png_export_uses_this_browsers_own_image(image_service, downloads):
    async def driver(user):
        session_id = await _open_with_one_fragment(user)
        own = image_service.get_browser_cache_path('b:' + session_id, FL, 800, 30.0, True, False)
        own.parent.mkdir(parents=True, exist_ok=True)
        own.write_bytes(_png((160, 40, 40, 255)))
        _click_export(user)
        src, _filename, _media_type = await _wait_for(downloads)
        assert isinstance(src, bytes)
        assert src[:4] == b'\x89PNG'

    _run(driver)


def _buttons(user, predicate):
    return [el for el in user._client.elements.values()
            if isinstance(el, ui.button) and predicate(el)]


def _click_button(user, predicate):
    with user._client:
        buttons = _buttons(user, predicate)
        assert len(buttons) == 1, len(buttons)
        for listener in buttons[0]._event_listeners.values():
            if listener.type == 'click':
                events.handle_event(listener.handler, events.GenericEventArguments(
                    sender=buttons[0], client=user._client, args=None))


def _icon(name):
    return lambda el: (el._props or {}).get('icon') == name


def _text(label):
    return lambda el: getattr(el, 'text', None) == label


async def _wait_until(check, timeout=15.0):
    deadline = asyncio.get_running_loop().time() + timeout
    while not check():
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError('timed out')
        await asyncio.sleep(0.1)


def _tab_value(user, key):
    with user._client:
        return app.storage.tab.get(key)


@pytest.fixture
def joins_service(tmp_path, monkeypatch):
    import shared.puzzle_service as ps
    service = ps.PuzzleService(db_path=str(tmp_path / 'joins.db'), thread_safe=True)
    monkeypatch.setattr(ps, 'get_puzzle_service', lambda thread_safe=False: service)
    return service


@pytest.fixture
def thumbnail_services(monkeypatch):
    import shared.puzzle_export as pe
    captured = []

    def _capture(fragments, image_service, thumb_size=150):
        captured.append(image_service)
        return ''

    monkeypatch.setattr(pe, 'generate_thumbnail', _capture)
    return captured


def _put_own_image(image_service, session_id, color):
    own = image_service.get_browser_cache_path('b:' + session_id, FL, 800, 30.0, True, False)
    own.parent.mkdir(parents=True, exist_ok=True)
    data = _png(color)
    own.write_bytes(data)
    return data


async def _click_dialog_button(user, opener, label):
    """Click ``opener``, then ``label`` in the dialog it opens. The dialog is
    built only after the page's handler has been to a worker thread, which a
    slow runner can take well over any fixed sleep to come back from."""
    _click_button(user, opener)
    await _wait_until(lambda: _buttons(user, _text(label)))
    _click_button(user, _text(label))


async def _save_join(user):
    await _click_dialog_button(user, _icon('save'), 'Save')


def test_saving_a_join_draws_its_thumbnail_with_this_browsers_images(
        image_service, joins_service, thumbnail_services):
    async def driver(user):
        session_id = await _open_with_one_fragment(user)
        own = _put_own_image(image_service, session_id, (30, 160, 60, 255))
        await _save_join(user)
        await _wait_until(lambda: thumbnail_services)
        drawn = thumbnail_services[-1].resolve_fragment_image(FL, 800, 30.0, True, False)
        assert drawn == own

    _run(driver)


def test_publishing_a_join_draws_it_with_this_browsers_images(
        image_service, joins_service, thumbnail_services, monkeypatch):
    import shared.puzzle_publish_service as pps
    import web.supabase_client as supabase_client
    from web.auth_state import GlobalAuthState
    published = []

    def _publish(client, user_id, doc, img_svc):
        published.append(img_svc)
        return 'published-join-1'

    monkeypatch.setattr(pps, 'publish_join', _publish)
    monkeypatch.setattr(supabase_client, 'get_user_client', lambda: object())

    async def driver(user):
        # Signed in before the page opens (signing in reloads the page): a
        # saved join belongs to the account that saved it, and only its owner
        # can publish it.
        monkeypatch.setattr(GlobalAuthState, 'is_logged_in', classmethod(lambda cls: True))
        monkeypatch.setattr(GlobalAuthState, 'get_user_id', classmethod(lambda cls: 'account-x'))
        session_id = await _open_with_one_fragment(user)
        own = _put_own_image(image_service, session_id, (90, 30, 160, 255))
        await _save_join(user)
        await _wait_until(lambda: thumbnail_services)
        # Publishing needs the save to have finished: the page records the
        # saved document's id in the tab once its worker thread is back.
        await _wait_until(lambda: _tab_value(user, 'puzzle_doc_id'))
        assert joins_service.list_documents(owner_key='u:account-x')
        await _click_dialog_button(user, _icon('publish'), 'Publish')
        await _wait_until(lambda: published)
        drawn = published[-1].resolve_fragment_image(FL, 800, 30.0, True, False)
        assert drawn == own

    _run(driver)


def test_canvas_positions_are_kept_in_the_tab(image_service, monkeypatch):
    """The periodic save of the canvas positions ran in a task with no page, so
    it never wrote and a reload put every fragment back at the default spot.
    A fragment on the canvas but not drawn keeps the position it had."""
    import json

    import web.pages.puzzle as puzzle_page

    # After the restore (0.5 s after load), as the real 30 s interval always is.
    monkeypatch.setattr(puzzle_page, 'STATE_SAVE_INTERVAL_S', 1.5)
    undrawn = '990000000000002,2v'
    drawn = {KEY: {'x': 321, 'y': 123, 'rotation': 15, 'scaleX': 0.5, 'scaleY': 0.5}}

    async def driver(user):
        # What the browser's canvas reports: only the fragment it could draw.
        user.javascript_rules[re.compile(r'.*puzzleCanvas\.getState\(\)')] = lambda _m: json.dumps(drawn)
        user.javascript_rules[re.compile(r'.*puzzleCanvas\.getCropState\(\)')] = lambda _m: '{}'
        await user.open('/puzzle')
        with user._client:
            # Restored by the page's init_canvas (0.5 s after load).
            from web.saved_joins import owner_key
            app.storage.tab['puzzle_owner'] = owner_key()
            app.storage.tab['puzzle_fragments'] = {
                KEY: {'sys_id': '990000000000001', 'shelfmark': 'ENA 1.1', 'folio_label': '1r',
                      'fl_id': FL, 'threshold': 30, 'processed': True, 'size': 800,
                      'image_url': '', 'external_provider': '', 'page_index': -1},
                undrawn: {'sys_id': '990000000000002', 'shelfmark': 'ENA 1.2', 'folio_label': '2v',
                          'fl_id': '45678902', 'threshold': 30, 'processed': True, 'size': 800,
                          'image_url': '', 'external_provider': '', 'page_index': -1},
            }
            app.storage.tab['puzzle_state'] = json.dumps({undrawn: {'x': 7, 'y': 8, 'rotation': 90}})
        saved = None
        for _ in range(80):
            await asyncio.sleep(0.1)
            with user._client:
                saved = app.storage.tab.get('puzzle_state')
            if saved and KEY in json.loads(saved):
                break
        assert saved and KEY in json.loads(saved), 'the canvas positions were never kept in the tab'
        state = json.loads(saved)
        assert state[KEY]['x'] == 321 and state[KEY]['rotation'] == 15
        assert state.get(undrawn, {}).get('rotation') == 90, state

    _run(driver)
