# -*- coding: utf-8 -*-
"""Website: Num Changes (x1-x3) is kept per variant level, and the API pins it.

Owner ruling 2026-09-28: Basic x1, Extended and Maximum x2 by default; the one
value saved before levels had their own seeds Extended and Maximum only;
Responsa (and composition, Joins) run with Basic's; the API runs variants and
Responsa at x1 and fuzzy at x2. Drives the real /search and /api/search with
the recording job queue of tests/test_web_variant_settings_per_visitor.py.
"""
from __future__ import annotations

import httpx

from tests.test_web_variant_settings_per_visitor import (  # noqa: F401  (server is a fixture)
    WORD, WORD2, _element, _fire, run, server, store, submit, wait_for_payloads,
)


def _sent(queue):
    return {queue.query_of(p): p['settings']['variant_max_changes'] for p in queue.payloads}


def test_an_old_saved_value_seeds_extended_and_maximum_only(server):  # noqa: F811 (the imported fixture)
    async def driver(a, b):
        await a.open('/search')
        store(a, search_max_changes=2)          # saved before levels had their own
        await a.open('/search')
        submit(a, f'? {WORD}')
        await wait_for_payloads(server.queue, 1)
        submit(a, f'?? {WORD2}')
        await wait_for_payloads(server.queue, 2)

    run(driver)
    assert _sent(server.queue) == {WORD: 1, WORD2: 2}


def test_each_level_runs_with_its_own_value(server):  # noqa: F811 (the imported fixture)
    async def driver(a, b):
        await a.open('/search')
        store(a, search_max_changes_by_level={'basic': 2, 'extended': 3, 'maximum': 1})
        await a.open('/search')
        submit(a, f'?? {WORD}')
        await wait_for_payloads(server.queue, 1)
        submit(a, f'??? {WORD2}')
        await wait_for_payloads(server.queue, 2)

    run(driver)
    assert _sent(server.queue) == {WORD: 3, WORD2: 1}


def test_responsa_runs_with_basics_value(server):  # noqa: F811 (the imported fixture)
    async def driver(a, b):
        await a.open('/search')
        store(a, search_max_changes_by_level={'basic': 2, 'extended': 3, 'maximum': 3},
              search_mode='responsa')
        await a.open('/search')
        submit(a, WORD)
        await wait_for_payloads(server.queue, 1)

    run(driver)
    assert _sent(server.queue) == {WORD: 2}


def test_the_api_runs_variants_at_x1_and_fuzzy_at_x2(server):  # noqa: F811 (the imported fixture)
    from nicegui import core

    async def driver(a, b):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(core.app),
                                     base_url='http://test') as api:
            for mode, word in (('variants', WORD), ('fuzzy', WORD2)):
                response = await api.post('/api/search', json={'query': word, 'search_mode': mode})
                assert response.status_code == 200, response.text
        await wait_for_payloads(server.queue, 2)

    run(driver)
    assert _sent(server.queue) == {WORD: 1, WORD2: 2}


def test_composition_runs_at_basics_value_at_every_level(server):  # noqa: F811 (the imported fixture)
    """/parallels searches at the chosen level's pairs but Basic's x1-x3 (owner
    ruling K-20): Basic x1 / Extended x3 sends composition at Extended with x1."""
    from nicegui import events, ui
    from shared.search_engine import SearchEngine
    from tests.test_web_variant_settings_per_visitor import FakeSearchEngine
    from web.state import state
    # The worker queue records the job; only the real signature is needed here.
    server.monkeypatch.setattr(FakeSearchEngine, 'search_composition_logic',
                               SearchEngine.search_composition_logic, raising=False)
    server.monkeypatch.setattr(state, 'is_ready', lambda: True)

    async def driver(a, b):
        await a.open('/parallels')
        store(a, search_max_changes_by_level={'basic': 1, 'extended': 3, 'maximum': 3})
        await a.open('/parallels')
        mode = _element(a, ui.select, lambda e: isinstance(e.options, dict) and 'fuzzy' in e.options
                        and 'variants' in e.options)
        _fire(a, mode, 'update:modelValue', 'variants')
        level = _element(a, ui.select, lambda e: isinstance(e.options, dict) and 70 in e.options
                         and 150 in e.options)
        _fire(a, level, 'update:modelValue', 70)
        text = _element(a, ui.textarea, lambda e: e.props.get('placeholder') == 'Paste your Hebrew text here...')
        with a._client:
            text.value = 'אבגד הוזח טיכל מנסע פצקר שתאב'
        button = _element(a, ui.button, lambda e: e.text == 'Find Parallels')
        with a._client:
            for listener in button._event_listeners.values():
                if listener.type == 'click':
                    events.handle_event(listener.handler, events.GenericEventArguments(
                        sender=button, client=a._client, args={}))
        await wait_for_payloads(server.queue, 1)

    run(driver)
    sent = server.queue.payloads[0]['settings']
    assert (sent['variant_pairs_count'], sent['variant_max_changes']) == (70, 1), sent
