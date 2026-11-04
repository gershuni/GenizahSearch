# -*- coding: utf-8 -*-
"""Render-smoke: the go-to box in the site header (2026-11-04).

``web/main.py::create_layout`` puts a box in the header on every page but
``/`` (the hero box) and ``/search`` (its own box). Enter in it navigates to
``web/home_query.py::home_query_target``'s URL -- the homepage box's routing --
so a shelfmark opens Browse and words, including Hebrew typed on the English
layout, are searched. Below 1024px a search button opens it (common.css,
client-side; pinned in tests/test_phone_layout_rules.py). Harness: the
homepage User context of ``test_home_teaser_render_smoke.py``; the box's
neighbours there are routed by the same pages.
"""
from __future__ import annotations

import asyncio
from unittest.mock import patch

from tests.render_smoke.test_home_search_routing_render_smoke import _navigated, _press_enter
from tests.render_smoke.test_home_teaser_render_smoke import _marked_element, _run_home_smoke

_BOX = 'header-search'
_TOGGLE = 'header-search-toggle'


def test_enter_in_the_header_box_goes_where_the_input_belongs():
    cases = [
        ('dcuk', '/search?q=dcuk'),
        ('Psalm 23', '/search?q=Psalm%2023'),
        ('שבת', '/search?q=%D7%A9%D7%91%D7%AA'),
        ('ts 20.57', '/browse?shelfmark=ts%2020.57'),
        ('990053835020205171', '/browse?sys_id=990053835020205171'),
    ]

    async def driver(user):
        await user.open('/about')
        box = _marked_element(user, _BOX)
        assert box is not None, "no element marked 'header-search' on /about"
        assert _marked_element(user, _TOGGLE) is not None
        assert box.props.get('placeholder') == 'Text, shelfmark or system ID'
        assert box.props.get('aria-label') == 'Search'
        for typed, expected in cases:
            box.value = typed
            with patch('nicegui.ui.navigate.to') as nav_mock:
                _press_enter(user, box)
                calls = await _navigated(nav_mock)
            assert calls == [(expected,)], typed

        box.value = '  '
        with patch('nicegui.ui.navigate.to') as nav_mock:
            _press_enter(user, box)
            await asyncio.sleep(0.3)
        assert not nav_mock.called

    _run_home_smoke(driver, lang='en')


def test_the_header_box_speaks_hebrew():
    async def driver(user):
        await user.open('/about')
        box = _marked_element(user, _BOX)
        assert box is not None
        assert box.props.get('placeholder') == 'טקסט, מספר מדף או מספר מערכת'
        assert box.props.get('aria-label') == 'חפש'

    _run_home_smoke(driver, lang='he')


def test_no_header_box_on_the_homepage():
    """The hero box is the homepage's box; a second one beside it is noise."""
    async def driver(user):
        await user.open('/')
        assert _marked_element(user, 'home-hero-search') is not None
        assert _marked_element(user, _BOX) is None
        assert _marked_element(user, _TOGGLE) is None

    _run_home_smoke(driver, lang='en')


def test_the_pages_without_it_are_the_homepage_and_search():
    from web.main import _HEADER_SEARCH_HIDDEN_ON
    assert _HEADER_SEARCH_HIDDEN_ON == {'/', '/search'}


class _Catalogue:
    """A loaded catalogue that holds one shelfmark the recogniser does not know."""

    def __init__(self):
        self.asked = []

    def has_shelfmark(self, query):
        self.asked.append(query)
        return query == 'Mingana 9'


def test_a_shelfmark_only_the_catalogue_knows_opens_browse(monkeypatch):
    """Codex on #476: ``Mingana 9`` is a libraries.csv shelfmark the rules call
    text; the box asks the catalogue before it searches, and asks nothing of
    words or Hebrew."""
    from web.state import state

    catalogue = _Catalogue()
    monkeypatch.setattr(state, 'meta_mgr', catalogue)

    async def driver(user):
        await user.open('/about')
        for box_marker in (_BOX,):
            box = _marked_element(user, box_marker)
            for typed, expected in (('Mingana 9', '/browse?shelfmark=Mingana%209'),
                                    ('Psalm 23', '/search?q=Psalm%2023'),
                                    ('dcuk', '/search?q=dcuk')):
                box.value = typed
                with patch('nicegui.ui.navigate.to') as nav_mock:
                    _press_enter(user, box)
                    calls = await _navigated(nav_mock)
                assert calls == [(expected,)], typed

    _run_home_smoke(driver, lang='en')
    assert catalogue.asked == ['Mingana 9', 'Psalm 23'], catalogue.asked


def test_the_homepage_box_asks_the_catalogue_too(monkeypatch):
    from web.state import state

    monkeypatch.setattr(state, 'meta_mgr', _Catalogue())

    async def driver(user):
        await user.open('/')
        box = _marked_element(user, 'home-hero-search')
        box.value = 'Mingana 9'
        with patch('nicegui.ui.navigate.to') as nav_mock:
            _press_enter(user, box)
            calls = await _navigated(nav_mock)
        assert calls == [('/browse?shelfmark=Mingana%209',)]

    _run_home_smoke(driver, lang='en')
