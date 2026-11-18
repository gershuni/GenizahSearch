# -*- coding: utf-8 -*-
"""Render-smoke: the search-mode signs in the go-to boxes, on the real pages (2026-11-18).

The /search box takes a sign and a space before a query to choose its mode (``$ שלום`` is
a Title search). The homepage box and the header box now do too: Enter opens
``/search?q=שלום&mode=Title``, the sign removed (``web/home_query.py::home_query_target``,
the signs' one table ``shared/search_mode_signs.py``). ``%`` (the catalogue text search) is
a sign only where that search is served: each page asks ``catalog_available()`` when the
box is used, patched here at each call site.

Pinned here: each box's wiring (``web/pages/home.py``, ``web/main.py``), the /search box's
own live switch reading the same table (``web/pages/search.py``), and that the address a
box opens runs its search in that mode -- with the slider, ``??`` at its level. The rules
themselves: ``tests/test_home_query_routing.py``, ``tests/test_search_mode_signs.py``.
"""
from __future__ import annotations

import asyncio
from unittest.mock import patch
from urllib.parse import quote

from nicegui import events, ui

from tests.render_smoke.test_home_search_routing_render_smoke import _navigated, _press_enter
from tests.render_smoke.test_home_teaser_render_smoke import _marked_element, _run_home_smoke
from tests.render_smoke.test_search_box_input_render_smoke import _Engine, _settle, _wait_for_calls
from tests.render_smoke.test_search_url_open_dispatch import _app, _connect, _elements, _get_page, _run

WORD = 'שלום'
Q = quote(WORD, safe='')

# What is typed, and where Enter goes, with the catalogue text search not served.
SIGNED = [
    (f'$ {WORD}', f'/search?q={Q}&mode=Title'),
    (f'# {WORD}', f'/search?q={Q}&mode=Shelfmark'),
    (f'? {WORD}', f'/search?q={Q}&mode=variants'),
    (f'?? {WORD}', f'/search?q={Q}&mode=variants_extended'),
    (f'??? {WORD}', f'/search?q={Q}&mode=variants_maximum'),
    (f'~ {WORD}', f'/search?q={Q}&mode=fuzzy'),
    (f'= {WORD}', f'/search?q={Q}&mode=exact'),
    (f'/ {WORD}', f'/search?q={Q}&mode=Regex'),
    (f'R {WORD}', f'/search?q={Q}&mode=responsa'),
    ('R 99 [3] 100', '/search?q=99%20%5B3%5D%20100&mode=responsa'),   # digits only: Responsa still
    ('# T-S 12.123', '/search?q=T-S%2012.123&mode=Shelfmark'),
    ('$ ', '/search?mode=Title'),
    # Not a sign where the catalogue search is not served: searched whole, as before.
    (f'% {WORD}', '/search?q=' + quote(f'% {WORD}', safe='')),
    # No space after the sign: as before (the /search page's own parsing reads it there).
    (f'${WORD}', '/search?q=' + quote(f'${WORD}', safe='')),
]
CATALOG = (f'% {WORD}', f'/search?q={Q}&mode=Catalog')


class _Catalogue:
    """A catalogue holding the Mosseri shelfmark ``R 15``, once its index is ready."""

    def __init__(self):
        self.ready = True

    def has_shelfmark(self, query):
        return self.ready and query == 'R 15'


async def _enter_each(user, box, cases):
    for typed, expected in cases:
        box.value = typed
        with patch('nicegui.ui.navigate.to') as nav_mock:
            _press_enter(user, box)
            calls = await _navigated(nav_mock)
        assert calls == [(expected,)], typed


def _drive_box(page, marker, served_at, monkeypatch):
    """Every sign in the box marked *marker* on *page*; ``%`` with ``catalog_available``
    patched where that page's box reads it (*served_at*)."""
    from web.state import state
    catalogue = _Catalogue()
    monkeypatch.setattr(state, 'meta_mgr', catalogue)

    async def driver(user):
        await user.open(page)
        box = _marked_element(user, marker)
        assert box is not None, f'no element marked {marker!r} on {page}'
        with patch(served_at, return_value=False):
            await _enter_each(user, box, SIGNED)
        with patch(served_at, return_value=True):
            await _enter_each(user, box, [CATALOG])
        # R is a sign only before Hebrew or Arabic: R 15 (a shelfmark) is routed as before,
        # ready index or not (Codex review of PR #52).
        await _enter_each(user, box, [('R 15', '/browse?shelfmark=R%2015')])
        catalogue.ready = False
        await _enter_each(user, box, [('R 15', '/search?q=R%2015')])

    _run_home_smoke(driver, lang='en')


def test_the_header_box_opens_search_in_the_signs_mode(monkeypatch):
    _drive_box('/about', 'header-search', 'web.main.catalog_available', monkeypatch)


def test_the_homepage_box_opens_search_in_the_signs_mode(monkeypatch):
    _drive_box('/', 'home-hero-search', 'web.pages.home.catalog_available', monkeypatch)


# --- /search: the box's live switch, and the address a go-to box opens ------------------

def _search_box(user):
    return next(e for e in _elements(user, ui.input) if 'data-gs-primary-search' in e.props)


def _mode_select(user):
    return next(e for e in _elements(user, ui.select) if e.props.get('label') == 'Mode')


def _type_space(user, element):
    """Fire the box's keyup.space listener(s): the moment the /search box reads a sign."""
    with user.client:
        for listener in element._event_listeners.values():
            if listener.element_id == element.id and listener.type == 'keyup.space':
                events.handle_event(listener.handler, events.GenericEventArguments(
                    sender=element, client=user.client, args=None))


def test_the_search_box_reads_the_same_signs():
    engine = _Engine()
    seen = {}

    async def go():
        async with _app(engine) as user:
            await _get_page(user, '/search')
            await _connect(user)
            await asyncio.sleep(0.3)   # the page's deferred loaders
            box, mode = _search_box(user), _mode_select(user)
            for typed in (f'$ {WORD}', f'?? {WORD}', f'# {WORD}', f'% {WORD}'):
                with user.client:
                    mode.value = 'exact'
                    box.value = typed
                _type_space(user, box)
                await asyncio.sleep(0.05)
                seen[typed] = (mode.value, box.value)

    _run(go)
    assert seen == {
        f'$ {WORD}': ('Title', WORD),
        f'?? {WORD}': ('variants_extended', WORD),
        f'# {WORD}': ('Shelfmark', WORD),
        f'% {WORD}': ('exact', f'% {WORD}'),   # the catalogue search is not served here
    }


def test_the_address_a_box_opens_runs_in_that_mode():
    engine = _Engine()
    seen = []

    async def go():
        async with _app(engine) as user:
            for i, mode in enumerate(('Title', 'Shelfmark', 'fuzzy', 'variants_extended', 'Regex', 'exact')):
                await _get_page(user, f'/search?q={Q}&mode={mode}')
                await _wait_for_calls(engine, i + 1)
                await _settle()
                seen.append(engine.calls[-1])

    _run(go)
    assert seen == [{'query': WORD, 'mode': m} for m in
                    ('Title', 'Shelfmark', 'fuzzy', 'variants_extended', 'Regex', 'literal')]


def test_with_the_slider_a_level_sign_opens_spelling_variants_at_that_level():
    """``?? שלום`` from a go-to box: with the slider, the page's one variants mode at the
    level ``??`` names (70 pairs), as typing it into the /search box does."""
    from web import variant_preferences
    from web.pages import search as search_page

    from web import safe_storage

    engine = _Engine()
    settings = []
    saved = []
    real_get = variant_preferences.get
    real_set = safe_storage.safe_user_set

    def _set(key, value, *args, **kwargs):
        saved.append(key)
        return real_set(key, value, *args, **kwargs)

    def _get(name, *args, **kwargs):
        return True if name == 'variant_use_slider' else real_get(name, *args, **kwargs)

    def _recording(engine_, **values):
        settings.append(values)
        return engine_

    async def go():
        with patch.object(variant_preferences, 'get', _get), \
                patch.object(safe_storage, 'safe_user_set', _set), \
                patch.object(search_page, 'with_request_settings', _recording):
            async with _app(engine) as user:
                await _get_page(user, f'/search?q={Q}&mode=variants_extended')
                await _wait_for_calls(engine, 1)
                await _settle()

    _run(go)
    assert engine.calls[0] == {'query': WORD, 'mode': 'variants'}
    assert settings and settings[-1]['variant_pairs_count'] == 70, settings
    # The page's own: the visitor's saved level is not changed (Codex review of PR #52).
    assert 'search_preset' not in saved, saved
