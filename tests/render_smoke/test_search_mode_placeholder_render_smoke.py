# -*- coding: utf-8 -*-
"""The /search box follows the mode, on the real page (owner, 2026-11-18).

The box's label says what to type in the mode chosen and its placeholder holds one of the mode's
examples (shared/search_mode_examples.py, whose table is pinned in
tests/test_search_mode_examples.py): on opening, on a choice in the Mode list, on a mode set in
code, and on a sign typed in the box (``$ `` -> Title). The NiceGUI User harness is
tests/render_smoke/test_search_url_open_dispatch.py's (the app lifespan with the engine
bootstrap replaced), so this file is in tests/render_smoke/ for the same reason.
"""
from __future__ import annotations

import asyncio
from unittest.mock import patch

from nicegui import events, ui

from shared.genizah_translations import TRANSLATIONS
from shared.search_mode_examples import LRI, MODE_EXAMPLES, PDI, RLI, mode_lead
from tests.render_smoke.test_search_url_open_dispatch import _app, _connect, _elements, _get_page, _run


class _Engine:
    def parse_query_syntax(self, query, responsa_mode=False):
        return None, query

    def execute_search(self, *args, **kwargs):
        return []


def _en(text):
    return text


def _he(text):
    return TRANSLATIONS.get(text, text)


def _box(user):
    return next(e for e in _elements(user, ui.input) if 'data-gs-primary-search' in e.props)


def _mode_list(user):
    return next(e for e in _elements(user, ui.select) if 'responsa' in (e.options or {}))


def _fire(user, element, event_type, value):
    """The browser's event *event_type* with *value*, through the page's own listeners."""
    with user.client:
        element.value = value
        for listener in list(element._event_listeners.values()):
            if listener.type == event_type:
                events.handle_event(listener.handler, events.GenericEventArguments(
                    sender=element, client=user.client, args=value))


def _shown(box):
    """(label, the example in the placeholder, the placeholder's lead) of the box."""
    text = box.props['placeholder']
    for mark in (LRI, RLI, PDI):
        text = text.replace(mark, '')
    lead, example = text.split(': ', 1)
    return box.props['label'], example, lead


def _assert_mode(shown, mode, translate, lead_word):
    label, example, lead = shown
    assert label == mode_lead(mode, translate), mode
    assert example in MODE_EXAMPLES[mode][1], (mode, example)
    assert lead == lead_word, mode


async def _open(user):
    await _get_page(user, '/search')
    await _connect(user)
    await asyncio.sleep(0.3)   # the page's deferred loaders


def test_the_box_says_what_to_type_in_the_mode_chosen():
    seen = {}

    async def go():
        async with _app(_Engine()) as user:
            await _open(user)
            box, modes = _box(user), _mode_list(user)
            seen['opened'] = _shown(box)
            seen['stacked'] = 'stack-label' in box.props
            # A choice in the list, as the browser sends it, then modes set in code.
            _fire(user, modes, 'update:modelValue', 'responsa')
            seen['responsa'] = _shown(box)
            for mode in ('Regex', 'Shelfmark', 'Title', 'fuzzy', 'variants', 'exact'):
                with user.client:
                    modes.value = mode
                seen[mode] = _shown(box)
            # A sign typed in the box switches the mode, and the box's words with it.
            _fire(user, box, 'update:modelValue', '$ ')
            await asyncio.sleep(0.05)
            seen['typed'] = (modes.value, box.value, _shown(box))

    _run(go)
    assert seen['stacked']
    _assert_mode(seen['opened'], 'exact', _en, 'For example')
    for mode in ('responsa', 'Regex', 'Shelfmark', 'Title', 'fuzzy', 'variants', 'exact'):
        _assert_mode(seen[mode], mode, _en, 'For example')
    assert seen['typed'][:2] == ('Title', '')
    _assert_mode(seen['typed'][2], 'Title', _en, 'For example')


def test_the_hebrew_page_has_the_owners_wording():
    seen = {}

    async def go():
        async with _app(_Engine()) as user:
            with patch('web.main._resolve_ui_language', return_value='he'):
                await _open(user)
                box, modes = _box(user), _mode_list(user)
                seen['opened'] = _shown(box)
                with user.client:
                    modes.value = 'Shelfmark'
                seen['Shelfmark'] = _shown(box)

    _run(go)
    assert seen['opened'][0] == 'הקלידו מונח לחיפוש'
    _assert_mode(seen['opened'], 'exact', _he, 'לדוגמה')
    _assert_mode(seen['Shelfmark'], 'Shelfmark', _he, 'לדוגמה')
