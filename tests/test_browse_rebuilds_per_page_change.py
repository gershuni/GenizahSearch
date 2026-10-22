# -*- coding: utf-8 -*-
"""A /browse page change rebuilds the content area once per phase, not two or three times.

``update_content()`` cleared ``content_container`` BEFORE looking at
``state.is_loading``, so showing the spinner was a full clear of the page plus
a 3-element build, and Phase A was then another full rebuild: opening the page
cleared the area 4 times and one Next click 3 times (measured on the stub page,
2026-10-21; NiceGUI 3.18 builds elements ~55-60% slower than 3.8, so each
rebuild costs more than it did). Then Phase B rebuilt the page with
``enrichment_loaded`` set -- which fills the five placeholders inline -- and
``update_enrichment_sections`` cleared and refilled the same five again, so the
version selector (and ``choose_default_source`` under it) ran twice per page.

Now the spinner is a row of its own above the content, toggled in place; while
a page loads the old content stays, dimmed and inert (``.browse-loading``), and
the keyboard listener ignores keys so arrow keys cannot start a second load.
Phase B fills its placeholders once. Harness: the stubbed page of
tests/test_browse_recents_off_loop.py.
"""

from __future__ import annotations

import asyncio
import itertools
import threading
from pathlib import Path
from unittest.mock import AsyncMock

import pytest
from nicegui import core, events, ui
from nicegui.client import Client
from nicegui.element import Element
from nicegui.testing.general import prepare_simulation

_paths = itertools.count()
_REPO = Path(__file__).resolve().parents[1]


@pytest.fixture
def page(monkeypatch):
    """A /browse page on a fake service, with every clear of its content area recorded.

    ``clears`` holds one entry per ``content_container.clear()`` with the page
    state at that moment; ``hold`` (a threading.Event) makes the next page fetch
    wait for the test; ``selectors`` counts the version selector builds.
    """
    from web.services import BrowsePage
    import shared.supabase_provider as provider
    import web.auth_state as auth_state
    import web.components as components
    import web.pages.browse as browse
    import web.pages.browse_enrichment as enrichment
    import web.supabase_client as supabase_client

    # Any Supabase client the page builds points at a refused local port: no network.
    for module in (supabase_client, provider):
        monkeypatch.setattr(module, 'SUPABASE_URL', 'http://127.0.0.1:9', raising=False)
        monkeypatch.setattr(module, '_client', None, raising=False)
    monkeypatch.setattr(supabase_client, '_public_read_client', None)

    def make(p):
        return BrowsePage(uid=f'u{p}', p_num=p, text='שלום', full_header='hdr',
                          total_pages=3, current_idx=p, sys_id='990000000000017',
                          shelfmark='T-S 1.1', fl_id=f'FL{p}')

    h = {'clears': [], 'hold': None, 'selectors': 0, 'refs': None, 'state': None}

    class _Service:
        is_ready = True

        def get_browse_page(self, sys_id, p_num=1, direction=0, volume_ie=None, **_kwargs):
            held = h['hold']
            if held is not None:
                held.wait(20)
            return make(max(1, min(3, (p_num or 1) + (direction or 0))))

        def __getattr__(self, _name):
            return lambda *a, **k: None

    class _Refs(browse.BrowsePageRefs):
        def __init__(self, *a, **k):
            super().__init__(*a, **k)
            h['refs'] = self

    class _State(browse.BrowseState):
        def __init__(self, *a, **k):
            super().__init__(*a, **k)
            h['state'] = self

    real_clear = Element.clear

    def counting_clear(self):
        refs = h['refs']
        if refs is not None and self is refs.content_container:
            h['clears'].append({
                'is_loading': h['state'].is_loading,
                'enrichment_loaded': h['state'].enrichment_loaded,
                'spinner_visible': getattr(getattr(refs, 'loading_row', None), 'visible', None),
            })
        return real_clear(self)

    def counting_selector(*_a, **_k):
        h['selectors'] += 1

    monkeypatch.setattr(browse, 'get_service', lambda: _Service())
    monkeypatch.setattr(browse, 'BrowsePageRefs', _Refs)
    monkeypatch.setattr(browse, 'BrowseState', _State)
    monkeypatch.setattr(browse, 'load_connected_fragments_into', AsyncMock())
    monkeypatch.setattr(Element, 'clear', counting_clear)
    monkeypatch.setattr(components, 'create_version_selector', counting_selector)
    # One PGP edition, so Phase B has a source and the version selector is built.
    monkeypatch.setattr(enrichment, 'get_all_sources_for_fragment',
                        lambda _sys_id: [{'doc_relation': 'Digital Edition', 'content': 'שלום עולם'}])
    monkeypatch.setattr(auth_state.GlobalAuthState, 'is_logged_in', staticmethod(lambda: False))
    monkeypatch.setattr(auth_state.GlobalAuthState, 'get_user_id', staticmethod(lambda: None))
    monkeypatch.setenv('GENIZAH_STORAGE_SECRET', 'x' * 40)
    prepare_simulation()

    def open_page():
        with Client(ui.page(f'/_rebuilds_probe_{next(_paths)}')) as client:
            browse.create_browse_page(initial_sys_id='990000000000017')
        return client

    h['open_page'] = open_page
    yield h
    if h['hold'] is not None:
        h['hold'].set()  # never leave a worker thread waiting


async def _wait(pred, timeout=20.0):
    deadline = asyncio.get_running_loop().time() + timeout
    while not pred():
        if asyncio.get_running_loop().time() > deadline:
            return False
        await asyncio.sleep(0.02)
    return True


def _next_button(client):
    buttons = [e for e in client.elements.values() if isinstance(e, ui.button)
               and str(e._props.get('data-action', '')).strip('"') == 'next']
    assert buttons, 'the page rendered no Next button'
    return buttons[-1]


def _click(client, element):
    """Fire the button's click listeners the way NiceGUI does (a no-argument
    handler is called without the event)."""
    with client:
        for listener in list(element._event_listeners.values()):
            if listener.element_id == element.id and listener.type == 'click':
                events.handle_event(listener.handler,
                                    events.ClickEventArguments(sender=element, client=client))


def _settled(h):
    """Phase B of the current load has landed."""
    return h['state'].enrichment_loaded


def test_opening_the_page_clears_the_content_area_once_per_phase(page):
    async def _run():
        core.loop = asyncio.get_running_loop()
        page['open_page']()
        assert await _wait(lambda: _settled(page)), 'Phase B never landed'
        await asyncio.sleep(0.3)  # room for a further rebuild, were there one

    asyncio.run(_run())
    clears = page['clears']
    assert len(clears) == 2, f'opening the page cleared the content area {len(clears)} times: {clears}'
    assert [c['enrichment_loaded'] for c in clears] == [False, True], clears  # Phase A, then Phase B
    assert not any(c['is_loading'] for c in clears), f'cleared for the spinner: {clears}'
    refs = page['refs']
    assert refs.loading_row.visible is False
    assert 'browse-loading' not in refs.content_container.classes
    assert page['selectors'] == 1, f'the version selector was built {page["selectors"]} times for one Phase B'
    # The line-number toggle ("always visible when text is present") survives
    # Phase B: the deleted second fill cleared its container and dropped it.
    toggles = [e for e in refs.content_container.descendants() if isinstance(e, ui.button)
               and str(e._props.get('icon', '')).strip('"') == 'format_list_numbered']
    assert len(toggles) == 1, f'{len(toggles)} line-number toggles after Phase B'


def test_the_spinner_row_takes_no_room_from_the_content():
    """The spinner row floats (zero height, sticky) instead of pushing the
    dimmed content down by its own height for the whole load."""
    css = (_REPO / 'web' / 'static' / 'common.css').read_text(encoding='utf-8')
    rule = css[css.index('.browse-loading-row {'):]
    rule = rule[:rule.index('}')]
    assert 'height: 0' in rule and 'position: sticky' in rule, rule
    import web.pages.browse as browse
    src = Path(browse.__file__).read_text(encoding='utf-8')
    assert "ui.row().classes('w-full justify-center browse-loading-row')" in src


def test_a_page_turn_keeps_the_old_content_under_the_spinner(page):
    async def _run():
        core.loop = asyncio.get_running_loop()
        client = page['open_page']()
        assert await _wait(lambda: _settled(page)), 'Phase B never landed'
        await asyncio.sleep(0.3)
        refs, state = page['refs'], page['state']
        n_open = len(page['clears'])
        n_content = len(list(refs.content_container.descendants()))
        assert n_content > 0
        page['hold'] = threading.Event()  # the next page's fetch waits for the test
        _click(client, _next_button(client))
        assert await _wait(lambda: refs.loading_row.visible), 'no spinner during the load'
        # While the page loads: no clear, the old content stays, dimmed and inert.
        assert state.is_loading
        assert len(page['clears']) == n_open, f'cleared for the spinner: {page["clears"][n_open:]}'
        assert 'browse-loading' in refs.content_container.classes
        # `inert`, not only pointer-events: no focus, no Enter/Space on its buttons.
        assert 'inert' in refs.content_container.props
        assert len(list(refs.content_container.descendants())) == n_content
        page['hold'].set()
        assert await _wait(lambda: state.current_page.p_num == 2 and _settled(page)), 'page 2 never landed'
        await asyncio.sleep(0.3)
        return n_open

    n_open = asyncio.run(_run())
    turn = page['clears'][n_open:]
    assert len(turn) == 2, f'one page turn cleared the content area {len(turn)} times: {turn}'
    assert [c['enrichment_loaded'] for c in turn] == [False, True], turn
    assert not any(c['is_loading'] for c in turn), turn
    refs = page['refs']
    assert refs.loading_row.visible is False
    assert 'browse-loading' not in refs.content_container.classes
    assert 'inert' not in refs.content_container.props
    assert page['selectors'] == 2, f'{page["selectors"]} version selectors for two Phase Bs'


def test_a_joins_count_landing_after_a_page_turn_leaves_the_dead_button_alone(monkeypatch):
    """``create_joins_button`` fetches its count 0.1 s later, off the loop. The
    Phase B button now lives until the NEXT page's Phase A replaces it (there is
    no spinner rebuild in between), so a count can land on a deleted button;
    it must not touch it (NiceGUI: "An element has been deleted but is still
    being used")."""
    import web.components.joins_panel as joins_panel

    release = threading.Event()

    def slow_fetch(**_kwargs):
        release.wait(20)
        return {'total_fragments': 1, 'fragments': []}

    monkeypatch.setattr(joins_panel, 'fetch_connected_fragments', slow_fetch)
    monkeypatch.setenv('GENIZAH_STORAGE_SECRET', 'x' * 40)
    prepare_simulation()

    async def _run():
        core.loop = asyncio.get_running_loop()
        with Client(ui.page(f'/_joins_probe_{next(_paths)}')) as client:
            with ui.column() as holder:
                btn = joins_panel.create_joins_button(
                    shelfmark='T-S 1.1', document_id='990000000000017',
                    find_joins_url='/joins-lab?sys_id=990000000000017')
        await asyncio.sleep(0.3)  # the count is being fetched (call_later 0.1 s)
        with client:
            holder.clear()  # the page turned
        assert btn.is_deleted
        release.set()
        await asyncio.sleep(0.3)  # room for the count to land
        return btn

    try:
        btn = asyncio.run(_run())
    finally:
        release.set()
    assert 'text-neutral-500' not in btn.classes, 'the joins count was applied to a deleted button'


def test_the_keyboard_listener_ignores_keys_while_a_page_loads():
    """The listener clicks ``[data-action="next"]`` programmatically, which
    ``pointer-events: none`` does not stop; so it stands down itself."""
    import web.pages.browse as browse

    src = Path(browse.__file__).read_text(encoding='utf-8')
    script = src[src.index("document.addEventListener('keydown'"):]
    script = script[:script.index('switch(e.key)')]
    assert "if (document.querySelector('.browse-loading')) return;" in script
    css = (_REPO / 'web' / 'static' / 'common.css').read_text(encoding='utf-8')
    assert '.browse-loading { opacity: .5; pointer-events: none; }' in css
