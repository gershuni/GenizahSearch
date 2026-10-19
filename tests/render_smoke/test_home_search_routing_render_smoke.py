# -*- coding: utf-8 -*-
"""Render-smoke: the homepage search box routes what is typed (owner, 2026-10-19).

The rules themselves are unit-tested in ``tests/test_home_query_routing.py``;
this proves the live ``/`` page wires them in: Enter in the hero box navigates
to ``home_query_target``'s URL -- Hebrew text to ``/search``, a sys_id and a
shelfmark to ``/browse`` -- the placeholder names all three inputs in the
Hebrew UI, and ``/browse?shelfmark=`` shows the shelfmark in its own box so a
miss can be corrected there. Harness: the homepage User context of
``test_home_teaser_render_smoke.py``.
"""
from __future__ import annotations

from unittest.mock import patch

from nicegui import events, ui

from tests.render_smoke.test_home_teaser_render_smoke import (
    _all_elements,
    _marked_element,
    _run_home_smoke,
)

_HERO_SEARCH_MARKER = 'home-hero-search'


def _press_enter(user, element) -> None:
    """Fire the box's keydown.enter listener(s)."""
    with user._client:
        for listener in element._event_listeners.values():
            if listener.element_id != element.id or listener.type != 'keydown.enter':
                continue
            ea = events.GenericEventArguments(sender=element, client=user._client, args=None)
            events.handle_event(listener.handler, ea)


def test_enter_in_the_hero_box_goes_where_the_input_belongs():
    cases = [
        ('שבת', '/search?q=%D7%A9%D7%91%D7%AA'),
        ('990053835020205171', '/browse?sys_id=990053835020205171'),
        ('T-S 8J6.1', '/browse?shelfmark=T-S%208J6.1'),
    ]

    async def driver(user):
        await user.open('/')
        box = _marked_element(user, _HERO_SEARCH_MARKER)
        assert box is not None, "no element marked 'home-hero-search' on '/'"
        for typed, expected in cases:
            box.value = typed
            with patch('nicegui.ui.navigate.to') as nav_mock:
                _press_enter(user, box)
            assert [c.args for c in nav_mock.call_args_list] == [(expected,)], typed

        # A blank box navigates nowhere.
        box.value = '   '
        with patch('nicegui.ui.navigate.to') as nav_mock:
            _press_enter(user, box)
        assert not nav_mock.called

    _run_home_smoke(driver, lang='en')


def test_the_placeholder_names_all_three_inputs_in_hebrew():
    async def driver(user):
        await user.open('/')
        box = _marked_element(user, _HERO_SEARCH_MARKER)
        assert box is not None
        placeholder = box.props.get('placeholder', '')
        assert placeholder == 'טקסט, מספר מדף או מספר מערכת', placeholder

    _run_home_smoke(driver, lang='he')


def test_browse_shows_the_shelfmark_it_was_sent_in_its_own_box():
    seen = {}

    async def driver(user):
        await user.open('/browse?shelfmark=T-S%20AS%2074.1%2B2')
        seen['values'] = [
            e.value for e in _all_elements(user)
            if isinstance(e, ui.input) and e.props.get('label') == 'Enter shelfmark or NLI system ID'
        ]

    _run_home_smoke(driver, lang='en')
    assert seen['values'] == ['T-S AS 74.1+2'], seen
