# -*- coding: utf-8 -*-
"""Render-smoke: the on-screen Hebrew keyboard in every search box (owner, 2026-11-16).

The /search box had it first (pilot); the homepage box and the header's go-to box
followed. Each box carries its field as the script's target name, and its panel the
same field, so the header's box (on /search too) never takes the main box's keys
(the pairing itself runs in Chromium in tests/test_hebrew_keyboard.py). The ⌨
button in each box opens its own panel and tells PostHog which field.
"""
from __future__ import annotations

from unittest.mock import patch

from nicegui import events, ui

from tests.render_smoke.test_home_teaser_render_smoke import (
    _all_elements, _marked_element, _run_home_smoke,
)
from web import hebrew_keyboard as hk


def _panel(user, field):
    for el in _all_elements(user):
        if el.props.get('data-hk-field') == field and hk.PANEL_ATTR in el.props:
            return el
    return None


def _toggle(user, box):
    with user._client:
        buttons = [c for c in box.slots['append'].children if isinstance(c, ui.button)]
    assert len(buttons) == 1, buttons
    return buttons[0]


def _click(user, element):
    with user._client:
        for listener in element._event_listeners.values():
            if listener.type == 'click':
                events.handle_event(listener.handler, events.GenericEventArguments(
                    sender=element, client=user._client, args={}))


def _pair(user, marker, field):
    box = _marked_element(user, marker)
    assert box is not None, marker
    panel = _panel(user, field)
    assert panel is not None, field
    return box, panel


def test_the_homepage_and_header_boxes_open_their_own_keys():
    seen = {}

    async def driver(user):
        await user.open('/')
        home_box, home_panel = _pair(user, 'home-hero-search', hk.HOME_FIELD)
        header_box, header_panel = _pair(user, 'header-search', hk.HEADER_FIELD)
        seen['targets'] = (home_box.props.get(hk.TARGET_ATTR), header_box.props.get(hk.TARGET_ATTR))
        seen['hidden'] = (home_panel.visible, header_panel.visible)
        seen['floating'] = ('gs-hk-floating' in header_panel.classes,
                            'gs-hk-floating' in home_panel.classes)
        _click(user, _toggle(user, home_box))
        seen['home open'] = (home_panel.visible, header_panel.visible,
                             _toggle(user, home_box).props.get('aria-expanded'))
        _click(user, _toggle(user, header_box))
        seen['both open'] = (home_panel.visible, header_panel.visible)
        _click(user, _toggle(user, home_box))
        seen['home closed'] = (home_panel.visible, header_panel.visible)

    with patch('web.analytics.posthog_capture') as capture:
        _run_home_smoke(driver, lang='en')
    assert seen['targets'] == (hk.HOME_FIELD, hk.HEADER_FIELD), seen
    assert seen['hidden'] == (False, False), seen
    assert seen['floating'] == (True, False), seen
    assert seen['home open'] == (True, False, 'true'), seen
    assert seen['both open'] == (True, True), seen
    assert seen['home closed'] == (False, True), seen
    opened = [c.args for c in capture.call_args_list if c.args and c.args[0] == hk.POSTHOG_OPENED]
    assert opened == [(hk.POSTHOG_OPENED, {'field': hk.HOME_FIELD, 'layout': hk.LAYOUT}),
                      (hk.POSTHOG_OPENED, {'field': hk.HEADER_FIELD, 'layout': hk.LAYOUT})], opened


def test_the_search_page_has_the_main_and_the_header_pairs():
    seen = {}

    async def driver(user):
        await user.open('/search')
        header_box, _ = _pair(user, 'header-search', hk.HEADER_FIELD)
        main_panel = _panel(user, hk.FIELD)
        mains = [el for el in _all_elements(user)
                 if el.props.get(hk.TARGET_ATTR) == hk.FIELD]
        seen['pairs'] = (header_box.props.get(hk.TARGET_ATTR), main_panel is not None, len(mains))

    _run_home_smoke(driver, lang='en')
    assert seen['pairs'] == (hk.HEADER_FIELD, True, 1), seen
