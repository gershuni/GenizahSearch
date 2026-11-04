# -*- coding: utf-8 -*-
"""The /search box's two input fixes, on the real page (2026-11-03).

1. A shelfmark typed into the box runs in Shelfmark mode, as the ``#`` prefix
   would, with "Searched as a shelfmark." under the count and a "Search as text
   instead" link that runs the same query as text; a corpus system number opens
   that manuscript in Browse, as the homepage box does. A Hebrew query, a word
   with a number ("Psalm 23") and a bare number run as before, in the page's
   text mode.
2. A Latin-script query that spells Hebrew on the Hebrew keyboard layout
   (``dcuk`` = גבול) and finds nothing gets "Did you mean <Hebrew>?" under
   "0 Results" -- only when the engine says the words occur in the corpus
   (``words_occur``) -- as a link to the same search in the same mode; the
   click runs it on this page, and both the showing and the taking are
   counted (PostHog ``search_suggestion``, kind=layout).

The engine is a stub (web/search_box_input.py and shared/keyboard_layout.py
are pure and tested on their own); what is pinned here is the page's wiring:
which mode reaches the engine for which input, and what the page then shows.
The NiceGUI User harness is the one tests/render_smoke/test_search_url_open_dispatch.py
built (the app lifespan with the engine bootstrap replaced), so this file is in
tests/render_smoke/ for the same reason.
"""
from __future__ import annotations

import asyncio
import time
from unittest.mock import patch
from urllib.parse import parse_qs, urlsplit

from nicegui import ui
from nicegui.testing.user_interaction import UserInteraction

from tests.render_smoke.test_search_url_open_dispatch import (
    _app, _connect, _elements, _get_page, _run,
)

SHELFMARKS = ['ts 20.57', 't-s ns 307.61b', 'add 3356', 'ts k2.27', 't-s 10g4.5', 'tad c2.1', '5559a.10']
HEBREW = 'שבת קודש'


class _Engine:
    """Records every search's query and mode; finds nothing."""

    def __init__(self, occurring=()):
        self.calls = []
        self.asked = []
        self.occurring = set(occurring)

    def parse_query_syntax(self, query, responsa_mode=False):
        return None, query

    def execute_search(self, query_str, mode, gap, **kwargs):
        self.calls.append({'query': query_str, 'mode': mode})
        return []

    def words_occur(self, query):
        self.asked.append(query)
        return query in self.occurring


async def _wait_for_calls(engine, n, timeout=10.0):
    deadline = time.perf_counter() + timeout
    while len(engine.calls) < n and time.perf_counter() < deadline:
        await asyncio.sleep(0.01)
    assert len(engine.calls) >= n, f'the engine was called {len(engine.calls)} times, not {n}'


async def _settle():
    """The page's work after the engine answered (the render, the suggestion check)."""
    await asyncio.sleep(0.4)


def _texts(user, kind):
    return [e.text for e in _elements(user, kind)]


async def _search_typed(user, engine, query):
    """Type *query* into the box and click Search; the engine's call for it."""
    before = len(engine.calls)
    box = next(e for e in _elements(user, ui.input) if e.props.get('placeholder') == 'Enter Hebrew text to search')
    with user.client:
        box.value = query
    button = next(b for b in _elements(user, ui.button) if b.props.get('label') == 'Search' and 'px-8' in b.classes)
    UserInteraction(user, {button}, None).click()
    await _wait_for_calls(engine, before + 1)
    await _settle()
    return engine.calls[-1]


def test_a_shelfmark_typed_into_the_box_runs_in_shelfmark_mode_and_a_hebrew_query_as_text():
    engine = _Engine()
    seen = {}

    async def go():
        async with _app(engine) as user:
            await _get_page(user, '/search?q=ts%2020.57')
            await _wait_for_calls(engine, 1)
            seen['url_open'] = dict(engine.calls[0])
            await _connect(user)
            await asyncio.sleep(0.3)   # the page's deferred loaders
            await _settle()
            seen['notice'] = [t for t in _texts(user, ui.label)
                              if t in ('Searched as a shelfmark.', 'Search as text instead')]
            # "Search as text instead": the same query, as text, in the page's mode.
            user.find('Search as text instead').click()
            await _wait_for_calls(engine, 2)
            await _settle()
            seen['as_text'] = dict(engine.calls[1])
            seen['notice_after'] = [t for t in _texts(user, ui.label) if t == 'Searched as a shelfmark.']
            # Every example from PostHog, typed; then a Hebrew query, unchanged.
            seen['typed'] = {q: await _search_typed(user, engine, q) for q in SHELFMARKS}
            seen['hebrew'] = await _search_typed(user, engine, HEBREW)
            seen['hebrew_notice'] = [t for t in _texts(user, ui.label) if t == 'Searched as a shelfmark.']
            # A word with a number, and a bare number, are text (Codex on #469).
            seen['as_text_shapes'] = {q: await _search_typed(user, engine, q)
                                      for q in ('Psalm 23', '12', 'version 1.2', 'COVID19')}
            # New Search clears the notice with everything else (Codex round 2).
            await _search_typed(user, engine, 'ts 20.57')
            seen['notice_again'] = [t for t in _texts(user, ui.label) if t == 'Searched as a shelfmark.']
            new_search = next(b for b in _elements(user, ui.button) if b.props.get('aria-label') == 'New Search')
            UserInteraction(user, {new_search}, None).click()
            await _settle()
            seen['notice_after_new'] = [t for t in _texts(user, ui.label) if t == 'Searched as a shelfmark.']
            seen['suggestions_asked'] = list(engine.asked)
            # A corpus system number opens that manuscript in Browse; no search runs.
            calls_before = len(engine.calls)
            with patch('nicegui.ui.navigate.to') as nav_mock:
                box = next(e for e in _elements(user, ui.input)
                           if e.props.get('placeholder') == 'Enter Hebrew text to search')
                with user.client:
                    box.value = '990053835020205171'
                button = next(b for b in _elements(user, ui.button)
                              if b.props.get('label') == 'Search' and 'px-8' in b.classes)
                UserInteraction(user, {button}, None).click()
                await _settle()
                seen['sys_id_nav'] = [c.args[0] for c in nav_mock.call_args_list]
            seen['sys_id_calls'] = len(engine.calls) - calls_before

    _run(go)
    assert seen['url_open'] == {'query': 'ts 20.57', 'mode': 'Shelfmark'}, seen
    assert seen['notice'] == ['Searched as a shelfmark.', 'Search as text instead'], seen
    assert seen['as_text'] == {'query': 'ts 20.57', 'mode': 'literal'}, seen
    assert seen['notice_after'] == [], 'the notice must go once the query runs as text'
    for query, call in seen['typed'].items():
        assert call == {'query': query, 'mode': 'Shelfmark'}, (query, call)
    assert seen['hebrew'] == {'query': HEBREW, 'mode': 'literal'}, seen
    assert seen['hebrew_notice'] == [], 'a Hebrew query is not a shelfmark'
    for query, call in seen['as_text_shapes'].items():
        assert call == {'query': query, 'mode': 'literal'}, (query, call)
    assert seen['notice_again'] == ['Searched as a shelfmark.'], seen
    assert seen['notice_after_new'] == [], 'New Search must clear the notice'
    assert seen['suggestions_asked'] == [], 'no keyboard-layout suggestion for a shelfmark, Hebrew or digits'
    assert seen['sys_id_nav'] == ['/browse?sys_id=990053835020205171'], seen
    assert seen['sys_id_calls'] == 0, 'a system number is opened, not searched'


def test_a_query_typed_on_the_english_layout_is_offered_as_the_hebrew_it_spells():
    engine = _Engine(occurring={'גבול'})
    captured = []
    seen = {}

    def _capture(event, properties=None, **kwargs):
        # The page's own search_executed goes through the same function.
        if event == 'search_suggestion':
            captured.append((event, dict(properties or {})))

    async def go():
        async with _app(engine) as user:
            with patch('web.analytics.posthog_capture', _capture):
                await _get_page(user, '/search?q=dcuk')
                await _wait_for_calls(engine, 1)
                await _connect(user)
                await asyncio.sleep(0.3)
                await _settle()
                seen['first'] = dict(engine.calls[0])
                seen['asked'] = list(engine.asked)
                links = [e for e in _elements(user, ui.link) if e.text == 'גבול']
                seen['link_count'] = len(links)
                seen['href'] = links[0].props.get('href') if links else None
                seen['around'] = [t for t in _texts(user, ui.label) if t in ('Did you mean ', '?')]
                seen['shown'] = list(captured)
                # The click runs the suggestion here, in the same mode (no navigation).
                user.find('גבול').trigger('click.prevent')
                await _wait_for_calls(engine, 2)
                await _settle()
                seen['taken'] = dict(engine.calls[1])
                seen['captured'] = list(captured)
                seen['link_after'] = [e.text for e in _elements(user, ui.link) if e.text == 'גבול']
                # Hebrew the corpus does not hold (the engine says so): nothing offered.
                await _search_typed(user, engine, 'fufch anh')
                seen['asked_missing'] = list(engine.asked)
                seen['link_missing'] = [e.text for e in _elements(user, ui.link) if e.text == 'כוכבי שמי']
                seen['captured_end'] = list(captured)

    _run(go)
    assert seen['first'] == {'query': 'dcuk', 'mode': 'literal'}, seen
    assert seen['asked'] == ['גבול'], 'the corpus is asked once, for the mapped Hebrew'
    assert seen['link_count'] == 1, seen
    parts = urlsplit(seen['href'])
    assert parts.path == '/search' and parse_qs(parts.query) == {'q': ['גבול'], 'mode': ['exact']}, seen['href']
    assert seen['around'] == ['Did you mean ', '?'], seen
    assert seen['shown'] == [('search_suggestion', {'kind': 'layout', 'taken': False})], seen
    assert seen['taken'] == {'query': 'גבול', 'mode': 'literal'}, seen
    assert seen['captured'][1] == ('search_suggestion', {'kind': 'layout', 'taken': True}), seen
    assert seen['link_after'] == [], 'the suggestion goes once it is taken'
    # The taken search is Hebrew: nothing to map, the corpus not asked again.
    assert seen['asked_missing'] == ['גבול', 'כוכבי שמי'], seen
    assert seen['link_missing'] == [], 'words the corpus lacks are not suggested'
    assert len(seen['captured_end']) == 2, 'nothing is counted for a suggestion that was not shown'


def test_the_header_box_is_on_the_search_page_too():
    """Owner, 2026-11-04: the header's go-to box stays on /search, beside the
    page's own box, so a reader can jump to a manuscript from there."""
    seen = {}

    async def go():
        async with _app(_Engine()) as user:
            await _get_page(user, '/search')
            await _connect(user)
            seen['boxes'] = [e for e in _elements(user, ui.input) if 'header-search' in e._markers]
            seen['page_box'] = [e for e in _elements(user, ui.input)
                                if e.props.get('placeholder') == 'Enter Hebrew text to search']

    _run(go)
    assert len(seen['boxes']) == 1, seen
    assert len(seen['page_box']) == 1, seen
