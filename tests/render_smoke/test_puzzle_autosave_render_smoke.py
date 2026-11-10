# -*- coding: utf-8 -*-
"""Fragment Puzzle: the open draft auto-saves after an edit, and only then.

Drives the real /puzzle page with a NiceGUI User. The Fabric.js canvas cannot
run here, so its replies (getState / getCropState) are scripted and its events
(puzzle-add-result, puzzle-object-modified, puzzle-delete) are fired by hand;
saved joins go to a temporary SQLite file. The same behaviour is checked in a
real browser by scripts/e2e_puzzle_autosave.py.

* An edit of the open draft is saved; opening or restoring it writes nothing.
* A fragment whose image never drew keeps its saved place.
* An empty canvas never empties a saved draft.
* Opening a draft by link replaces the tab's older canvas, also for a reload.
* A crop is kept with the tab's canvas and put back on a reload.
"""
from __future__ import annotations

import asyncio
import json
import re

import pytest

from nicegui import app
from shared.puzzle_model import PuzzleDocument, PuzzleFragment
from tests.render_smoke.test_puzzle_export_render_smoke import _run

K1, K2, OLD = '990000000000001,1r', '990000000000002,1r', '990000000000009,1r'


@pytest.fixture(autouse=True)
def offline(tmp_path, monkeypatch):
    import shared.puzzle_export as pe
    import shared.puzzle_image_service as pis
    import shared.puzzle_service as ps
    pis.reset_puzzle_image_service()
    pis.get_puzzle_image_service(cache_dir=tmp_path / 'puzzle')
    service = ps.PuzzleService(db_path=str(tmp_path / 'joins.db'), thread_safe=True)
    monkeypatch.setattr(ps, 'get_puzzle_service', lambda thread_safe=False: service)
    monkeypatch.setattr(pe, 'generate_thumbnail', lambda fragments, image_service, thumb_size=150: '')
    yield service
    pis.reset_puzzle_image_service()


def _fragment(sys_id, x, y, rotation=0.0, **extra):
    return PuzzleFragment(sys_id=sys_id, folio_label='1r', fl_id='FL' + sys_id[-4:],
                          shelfmark='ENA ' + sys_id[-1], x=x, y=y, rotation=rotation, **extra)


def _meta(key):
    sys_id = key.split(',')[0]
    return {'sys_id': sys_id, 'shelfmark': 'ENA ' + sys_id[-1], 'folio_label': '1r',
            'fl_id': 'FL' + sys_id[-4:], 'threshold': 30, 'processed': True, 'size': 800,
            'image_url': '', 'external_provider': '', 'page_index': -1}


class Canvas:
    """What the browser's canvas reports, and every script the page sent it."""

    def __init__(self, user):
        self.state = {}
        self.crops = {}
        self.scripts = []
        user.javascript_rules[re.compile(r'.*puzzleCanvas\.getState\(\)', re.S)] = \
            lambda _m: json.dumps(self.state)
        user.javascript_rules[re.compile(r'.*puzzleCanvas\.getCropState\(\)', re.S)] = \
            lambda _m: json.dumps(self.crops)
        # Awaited calls with nothing to report (the harness times out any it has no rule for).
        user.javascript_rules[re.compile(r'.*(clearAll|fitAll)\(\)|.*var obj = window\.puzzleCanvas', re.S)] = \
            lambda _m: None

    @property
    def added(self):
        """Keys the page asked the canvas to draw, in order."""
        return [m.group(1) for js in self.scripts for m in re.finditer(r'addFragment\("([^"]+)"', js)]


@pytest.fixture
def canvas_scripts(monkeypatch):
    """Every script sent to the browser (the harness answers only the awaited ones)."""
    from nicegui import ui
    sent = []
    original = ui.run_javascript

    def _record(code, *args, **kwargs):
        sent.append(code)
        return original(code, *args, **kwargs)

    monkeypatch.setattr(ui, 'run_javascript', _record)
    return sent


def _fire(user, name, detail):
    """Deliver a canvas CustomEvent to the page, as the browser would."""
    from nicegui import helpers
    wanted = helpers.event_type_to_camel_case(name)
    with user._client:
        for element in list(user._client.elements.values()):
            for listener in list(element._event_listeners.values()):
                if listener.type == wanted:
                    element._handle_event({'listener_id': listener.id,
                                           'args': {'detail': json.dumps(detail)}})
                    return
    raise AssertionError(f'no listener for {name}')


async def _until(check, timeout=15.0):
    deadline = asyncio.get_running_loop().time() + timeout
    while not check():
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError('timed out')
        await asyncio.sleep(0.1)


async def _open_with_draft(user, service, fragments):
    """Open /puzzle with ``fragments`` saved as the visitor's draft, open in the tab."""
    await user.open('/puzzle')
    with user._client:
        from web.saved_joins import owner_key
        owner = owner_key()
        doc = PuzzleDocument(id='draft-1', title='My draft', notes='', fragments=fragments)
        service.save_document(doc, owner_key=owner)
        app.storage.tab['puzzle_owner'] = owner
        app.storage.tab['puzzle_doc_id'] = doc.id
        app.storage.tab['puzzle_fragments'] = {}
    return owner


def _stored(service, owner):
    doc = service.load_document('draft-1', owner_key=owner)
    return doc, {f"{f.sys_id},{f.folio_label}": f for f in doc.fragments}


def _drawn(key, x, y):
    return {key: {'x': x, 'y': y, 'centerX': x + 400, 'centerY': y + 300, 'rotation': 0,
                  'scaleX': 1, 'scaleY': 1, 'unscaledWidth': 800, 'unscaledHeight': 600}}


def test_an_edit_auto_saves_the_open_draft_and_loading_does_not(offline, canvas_scripts):
    async def driver(user):
        canvas = Canvas(user)
        canvas.scripts = canvas_scripts
        owner = await _open_with_draft(user, offline, [_fragment(K1[:15], 10, 20)])
        stamp = _stored(offline, owner)[0].updated_at
        await _until(lambda: K1 in canvas.added)            # the draft was reopened
        _fire(user, 'puzzle-add-result', {'key': K1, 'success': True})
        canvas.state = _drawn(K1, 10, 20)
        await asyncio.sleep(2.5)
        assert _stored(offline, owner)[0].updated_at == stamp, 'reopening the draft rewrote it'

        canvas.state = _drawn(K1, 250, 20)
        _fire(user, 'puzzle-object-modified', {'key': K1})
        await _until(lambda: _stored(offline, owner)[1][K1].x == 250)

    _run(driver)


def test_a_fragment_that_never_drew_keeps_its_saved_place(offline, canvas_scripts):
    async def driver(user):
        canvas = Canvas(user)
        canvas.scripts = canvas_scripts
        owner = await _open_with_draft(user, offline, [
            _fragment(K1[:15], 10, 20), _fragment(K2[:15], 500, 600, rotation=45.0, scale=0.5)])
        await _until(lambda: K1 in canvas.added and K2 in canvas.added)
        _fire(user, 'puzzle-add-result', {'key': K1, 'success': True})
        _fire(user, 'puzzle-add-result', {'key': K2, 'success': False, 'error': 'all-failed'})
        canvas.state = _drawn(K1, 250, 20)                   # only K1 is on the canvas
        _fire(user, 'puzzle-object-modified', {'key': K1})
        await _until(lambda: _stored(offline, owner)[1][K1].x == 250)
        kept = _stored(offline, owner)[1][K2]
        assert (kept.x, kept.y, kept.rotation, kept.scale) == (500, 600, 45.0, 0.5)

    _run(driver)


def test_an_empty_canvas_never_empties_a_saved_draft(offline, canvas_scripts):
    async def driver(user):
        canvas = Canvas(user)
        canvas.scripts = canvas_scripts
        owner = await _open_with_draft(user, offline, [_fragment(K1[:15], 10, 20)])
        await _until(lambda: K1 in canvas.added)
        _fire(user, 'puzzle-add-result', {'key': K1, 'success': True})
        await asyncio.sleep(0.3)
        canvas.state = {}
        _fire(user, 'puzzle-delete', [K1])                   # the canvas is now empty
        await asyncio.sleep(2.5)
        _, stored = _stored(offline, owner)
        assert K1 in stored, 'an empty canvas emptied the saved draft'

    _run(driver)


def test_opening_a_draft_by_link_replaces_the_tabs_older_canvas(offline, canvas_scripts):
    async def driver(user):
        canvas = Canvas(user)
        canvas.scripts = canvas_scripts
        await user.open('/puzzle')
        with user._client:
            from web.saved_joins import owner_key
            owner = owner_key()
            offline.save_document(PuzzleDocument(id='draft-1', title='Linked', notes='',
                                                 fragments=[_fragment(K1[:15], 10, 20)]),
                                  owner_key=owner)
        await asyncio.sleep(1.0)
        with user._client:                                    # an older canvas in this tab
            app.storage.tab['puzzle_owner'] = owner
            app.storage.tab['puzzle_fragments'] = {OLD: _meta(OLD)}
            app.storage.tab['puzzle_doc_id'] = None

        canvas.scripts.clear()
        await user.open('/puzzle?doc=draft-1')
        await _until(lambda: K1 in canvas.added)
        assert OLD not in canvas.added, 'the older canvas was redrawn under the linked draft'
        with user._client:
            assert set(app.storage.tab['puzzle_fragments']) == {K1}
            assert app.storage.tab['puzzle_doc_id'] == 'draft-1'

        canvas.scripts.clear()
        await user.open('/puzzle')                            # a reload without the link
        await _until(lambda: K1 in canvas.added)
        assert OLD not in canvas.added

    _run(driver)


def test_a_crop_is_kept_with_the_tabs_canvas(offline, canvas_scripts, monkeypatch):
    import web.pages.puzzle as puzzle_page
    monkeypatch.setattr(puzzle_page, 'STATE_SAVE_INTERVAL_S', 1.0)
    crop = {'cropX': 60, 'cropY': 0, 'width': 340, 'height': 300,
            'originalWidth': 400, 'originalHeight': 300}

    async def driver(user):
        canvas = Canvas(user)
        canvas.scripts = canvas_scripts
        canvas.state = {K1: {'x': 1, 'y': 2, 'centerX': 321, 'centerY': 123, 'rotation': 0,
                             'scaleX': 1, 'scaleY': 1}}
        canvas.crops = {K1: crop}
        await user.open('/puzzle')
        with user._client:
            from web.saved_joins import owner_key
            app.storage.tab['puzzle_owner'] = owner_key()
            app.storage.tab['puzzle_fragments'] = {K1: _meta(K1)}

        def kept():
            with user._client:
                saved = app.storage.tab.get('puzzle_state')
            return saved and json.loads(saved).get(K1, {}).get('crop') == crop
        await _until(kept)

        canvas.scripts.clear()
        await user.open('/puzzle')                            # reload
        await _until(lambda: K1 in canvas.added)
        _fire(user, 'puzzle-add-result', {'key': K1, 'success': True})
        await _until(lambda: any('var cropX = 60;' in js for js in canvas.scripts))
        applied = next(js for js in canvas.scripts if 'var cropX = 60;' in js)
        assert 'var cropRight = 0;' in applied and 'var centerX = 321;' in applied, applied

    _run(driver)
