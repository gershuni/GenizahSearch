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

from unittest.mock import patch

from tests.render_smoke.test_home_search_routing_render_smoke import _press_enter
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
            assert [c.args for c in nav_mock.call_args_list] == [(expected,)], typed

        box.value = '  '
        with patch('nicegui.ui.navigate.to') as nav_mock:
            _press_enter(user, box)
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
