"""Server-render smoke coverage for the "What's New" toast.

The toast lives in `create_layout`, so it renders on nearly every page — which is
exactly why it needs its own guard.

REWRITTEN 2026-08-26. The toast used to advertise three surfaces: the guided
introduction, the atlas, and computed identifications. All three shipped on
2026-08-16, all three keep their own nav entry, and the owner replaced the
toast's contents with the two new parallels features. These tests pinned the old
contract exactly as they should have, and failed the moment it changed.

What is pinned now:

1. It advertises the two letter-level features, each gated on its OWN
   availability predicate — the flag ANDed with an index that actually loaded.
2. It can be EMPTY, and that is correct. The old list opened with `/start`,
   which is registered unconditionally, so the toast always had something to
   say; `web/main.py`'s own comment called the empty-list guard "not currently
   reachable". Both entries are now gated, so a box whose index did not open
   shows no toast at all. That is also the state CI runs in — there is no index
   there — which is how the previous version of this suite would have gone on
   passing vacuously once every assertion became `banner is None`.
3. Its links land on a PREFILLED search rather than an empty form, built from
   `/start`'s own prepared demo so the two cannot drift apart.
4. It suppresses itself on `/parallels` and `/help` — the pages that already
   present this — and NOT on `/`, because the toast exists for the reader
   arriving at the site and that arrival is normally the homepage.
5. Its strings are translated. Hebrew is the DEFAULT language here, so an
   unregistered string leaks English to most of the site's readers. This is the
   failure that actually shipped once: the wording was revised, the `tr()` calls
   kept asking for the old keys, and `tr()` answers a miss by returning its
   argument, so nothing raised anywhere.

Absence is asserted against the container's own `whats-new-banner` mark rather
than against body text. An earlier revision checked for a lead-in sentence; when
that sentence was removed from the implementation those assertions kept passing
while testing nothing.
"""

from __future__ import annotations

import asyncio
import os
from contextlib import AsyncExitStack, asynccontextmanager
from unittest.mock import patch

import httpx
from nicegui import core
from nicegui.context import context as nicegui_context
from nicegui.testing.general import prepare_simulation
from nicegui.testing.user import User
from nicegui.ui_run import set_storage_secret


@asynccontextmanager
async def _user(lang: str, *, passage_on: bool, witnesses_on: bool):
    saved_handlers = list(core.app._startup_handlers)
    core.app._startup_handlers.clear()
    try:
        prepare_simulation()
        set_storage_secret('whats-new-render-smoke-secret', {})
        async with AsyncExitStack() as stack:
            # `web.main` binds both predicates at import (`from ... import f`),
            # so the toast reads THESE names, not the source module's.
            stack.enter_context(
                patch('web.main.passage_available', return_value=passage_on))
            stack.enter_context(
                patch('web.main.passage_multi_witness_available',
                      return_value=witnesses_on))
            stack.enter_context(
                patch('web.main._resolve_ui_language', return_value=lang))
            os.environ['NICEGUI_USER_SIMULATION'] = 'true'
            try:
                await stack.enter_async_context(
                    core.app.router.lifespan_context(core.app))
                client = await stack.enter_async_context(httpx.AsyncClient(
                    transport=httpx.ASGITransport(core.app),
                    base_url='http://test',
                ))
                yield User(client)
            finally:
                os.environ.pop('NICEGUI_USER_SIMULATION', None)
    finally:
        core.app._startup_handlers.clear()
        core.app._startup_handlers.extend(saved_handlers)


def _run(driver, *, lang='en', passage_on=True, witnesses_on=True):
    saved_slots = list(nicegui_context.slot_stack)

    async def invoke():
        async with _user(lang, passage_on=passage_on,
                         witnesses_on=witnesses_on) as user:
            await driver(user)

    try:
        asyncio.run(invoke())
    finally:
        nicegui_context.slot_stack.clear()
        nicegui_context.slot_stack.extend(saved_slots)


def _elements(user):
    with user._client:
        return list(user._client.elements.values())


def _banner(elements):
    """The toast container, or None. Keyed to its mark, never to body text."""
    for element in elements:
        if 'whats-new-banner' in (getattr(element, '_markers', None) or []):
            return element
    return None


def _hrefs(elements):
    return {
        (getattr(element, '_props', None) or {}).get('href')
        for element in elements
    } - {None}


def _parallels_hrefs(elements):
    return [h for h in _hrefs(elements) if str(h).startswith('/parallels')]


def _texts(elements):
    return ' '.join(
        str((getattr(element, '_props', None) or {}).get('label') or '')
        + ' ' + str(getattr(element, 'text', '') or '')
        for element in elements
    )


def test_toast_advertises_both_letter_level_features():
    async def driver(user):
        await user.open('/search')
        elements = _elements(user)
        assert _banner(elements) is not None
        assert _parallels_hrefs(elements), 'the toast advertised nothing'
        # Both entries lead to the same page, so their hrefs collapse to one
        # value in a set. What distinguishes them is the NAME, so that is what
        # this asserts.
        text = _texts(elements)
        assert 'Letter-level parallels search' in text
        assert 'textual witnesses' in text

    _run(driver)


def test_the_links_land_on_a_prefilled_search_not_an_empty_form():
    """A reader who has just been told a new search method exists, and who then
    arrives at a blank textarea, has to go and find something to paste in before
    the announcement means anything. The text is `/start`'s prepared demo."""
    async def driver(user):
        await user.open('/search')
        targets = _parallels_hrefs(_elements(user))
        assert targets, 'no parallels link in the toast'
        assert all(t.startswith('/parallels?text=') for t in targets), (
            'the toast links to a bare form: %r' % targets)

    _run(driver)


def test_toast_shows_on_the_homepage_because_that_is_where_readers_arrive():
    async def driver(user):
        await user.open('/')
        assert _banner(_elements(user)) is not None

    _run(driver)


def test_each_entry_is_gated_on_its_own_predicate():
    """Advertising a clean-hidden feature would point a reader at a control the
    page does not draw."""
    async def witnesses_down(user):
        await user.open('/search')
        text = _texts(_elements(user))
        assert 'Letter-level parallels search' in text
        assert 'textual witnesses' not in text

    _run(witnesses_down, passage_on=True, witnesses_on=False)


def test_no_index_means_no_toast_at_all():
    """The empty-list guard is REACHABLE now, and load-bearing.

    While the list opened with the unconditionally-registered `/start`, the
    toast always had something to say and that guard was a formality. Both
    entries are gated now, so a box whose index did not open advertises
    nothing — which is right, because on that box there is nothing new to
    announce.
    """
    async def driver(user):
        await user.open('/search')
        assert _banner(_elements(user)) is None

    _run(driver, passage_on=False, witnesses_on=False)


def test_toast_suppresses_itself_where_the_page_already_presents_this():
    """Suppression follows the CONTENT. It named `/start` while `/start` was
    what the toast advertised; it now names the page the toast points at.

    The third case is a control, and it is the point of the test. With the
    feature off the banner is absent everywhere, so the two suppression
    assertions would both pass while proving nothing — which is precisely the
    trap the old suite fell into once its subject changed.
    """
    async def on_parallels(user):
        await user.open('/parallels')
        assert _banner(_elements(user)) is None

    _run(on_parallels)

    async def on_help(user):
        await user.open('/help')
        assert _banner(_elements(user)) is None

    _run(on_help)

    async def on_search(user):
        await user.open('/search')
        assert _banner(_elements(user)) is not None, (
            'the control failed: if the toast is absent HERE too, the two '
            'assertions above prove nothing about suppression')

    _run(on_search)


def test_toast_is_translated_rather_than_leaking_english_into_the_hebrew_ui():
    """Hebrew is the DEFAULT language, so an unregistered string reaches most
    readers. This has shipped once: the wording was revised and the `tr()` calls
    were left asking for the old keys, which `tr()` answers by handing back the
    key itself."""
    async def driver(user):
        await user.open('/search')
        text = _texts(_elements(user))
        assert 'תכונות חדשות!' in text
        assert 'חיפוש מקבילות ברמת האות' in text
        assert 'עדי נוסח' in text
        assert 'Letter-level parallels search' not in text
        assert 'New Features!' not in text

    _run(driver, lang='he')
