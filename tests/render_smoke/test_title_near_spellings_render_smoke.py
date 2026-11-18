# -*- coding: utf-8 -*-
"""Near spellings in Title mode on the real /search page (catalogue plan §4.7, PR 2b).

An address with ``mode=Title&near=1`` runs the Title search with the option, the switch
shows in Title mode without the Automatic translations switch, its tooltip adds that the
words are found in any order, and the address the page writes back keeps ``&near=1``; the
switch unticked, the next search runs without it and the address drops it; in another mode
the switch is hidden. The engine is a stub; the harness is the one
tests/render_smoke/test_search_url_open_dispatch.py built.
"""
from __future__ import annotations

import asyncio
import time
from unittest.mock import patch
from urllib.parse import quote

from nicegui import ui
from nicegui.client import Client
from nicegui.testing.user_interaction import UserInteraction

from shared.catalog_text_rows import CATALOG_NEAR_TOOLTIP
from shared.near_spellings import TITLE_NEAR_SPELLINGS_TOOLTIP
from tests.render_smoke.test_search_url_open_dispatch import (
    _app, _connect, _elements, _get_page, _run,
)

QUERY = 'תפילין'


class _Engine:
    """Records every search's query, mode and catalogue options; finds nothing."""

    def __init__(self):
        self.calls = []

    def parse_query_syntax(self, query, responsa_mode=False):
        return None, query

    def execute_search(self, query_str, mode, gap, **kwargs):
        self.calls.append({'query': query_str, 'mode': mode,
                           'catalog_options': kwargs.get('catalog_options')})
        return []

    def words_occur(self, query):
        return False


async def _wait_for_calls(engine, n, timeout=10.0):
    deadline = time.perf_counter() + timeout
    while len(engine.calls) < n and time.perf_counter() < deadline:
        await asyncio.sleep(0.01)
    assert len(engine.calls) >= n, f'the engine was called {len(engine.calls)} times, not {n}'


def _shown(element):
    """The element and every element around it are visible."""
    while element is not None:
        if not element.visible:
            return False
        slot = element.parent_slot
        element = slot.parent if slot is not None else None
    return True


def _checkbox(user, text):
    return next(e for e in _elements(user, ui.checkbox) if e.text == text)


def _mode_select(user):
    return next(e for e in _elements(user, ui.select)
                if isinstance(e.options, dict) and 'Title' in e.options)


async def _search(user, engine):
    before = len(engine.calls)
    button = next(b for b in _elements(user, ui.button)
                  if b.props.get('label') == 'Search' and 'px-8' in b.classes)
    UserInteraction(user, {button}, None).click()
    await _wait_for_calls(engine, before + 1)
    await asyncio.sleep(0.4)
    return engine.calls[-1]


def test_title_mode_carries_near_spellings_from_the_address_to_the_engine_and_back():
    engine = _Engine()
    urls = []
    seen = {}
    real = Client.run_javascript

    def recording(self, code, *args, **kwargs):
        if code.startswith('history.replaceState'):
            urls.append(code)
        return real(self, code, *args, **kwargs)

    async def go():
        async with _app(engine) as user:
            with patch.object(Client, 'run_javascript', recording):
                await _get_page(user, f'/search?q={quote(QUERY)}&mode=Title&near=1')
                await _wait_for_calls(engine, 1)
                await _connect(user)
                await asyncio.sleep(0.7)
                seen['first'] = dict(engine.calls[0])
                seen['first_url'] = urls[-1] if urls else ''
                near = _checkbox(user, 'Fuzzy search')
                seen['near_shown'] = _shown(near)
                seen['near_value'] = near.value
                seen['translations_shown'] = _shown(_checkbox(user, 'Automatic translations'))
                seen['tooltips'] = [t.text for t in _elements(user, ui.tooltip)]
                # Unticked: the next search runs without it, and the address drops it.
                UserInteraction(user, {near}, None).click()
                seen['second'] = dict(await _search(user, engine))
                seen['second_url'] = urls[-1] if urls else ''
                # Another mode hides the switch.
                select = _mode_select(user)
                with user.client:
                    select.value = 'literal'
                # NiceGUI keeps the page's .on('update:model-value') listener camelCased.
                UserInteraction(user, {select}, None).trigger('update:modelValue')
                await asyncio.sleep(0.2)
                seen['near_shown_in_exact'] = _shown(near)

    _run(go)
    assert seen['first'] == {'query': QUERY, 'mode': 'Title',
                             'catalog_options': {'near_spellings': True}}, seen
    assert 'mode=Title' in seen['first_url'] and '&near=1' in seen['first_url'], seen
    assert seen['near_shown'] and seen['near_value'] is True, seen
    assert seen['translations_shown'] is False, seen
    assert CATALOG_NEAR_TOOLTIP + '. ' + TITLE_NEAR_SPELLINGS_TOOLTIP in seen['tooltips'], seen
    assert seen['second'] == {'query': QUERY, 'mode': 'Title',
                              'catalog_options': {'near_spellings': False}}, seen
    assert 'mode=Title' in seen['second_url'] and 'near=' not in seen['second_url'], seen
    assert seen['near_shown_in_exact'] is False, seen
