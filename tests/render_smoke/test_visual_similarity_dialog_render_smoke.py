"""Server-render coverage for the browse page's Visual Similarity window.

Pinned here by driving the real dialog through the client's own events:

1. Choosing a domain narrows the suggestions. The Sort / Library / Domain
   selects used raw 'update:model-value' listeners whose handlers read
   ``e.value``; GenericEventArguments has none, so every change raised and the
   list was never re-filtered (found 2026-10-18 from a researcher's report).
2. The Thumbnails view shows one low-resolution image per filtered suggestion,
   from the same proxy URL the Joins Lab grid uses.
3. The List / Thumbnails choice is remembered for the visitor's next window.
"""
from __future__ import annotations

import asyncio
import os
from contextlib import AsyncExitStack, asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import httpx
from nicegui import core, ui
from nicegui.context import context as nicegui_context
from nicegui.testing.general import prepare_simulation
from nicegui.testing.user import User
from nicegui.testing.user_interaction import UserInteraction
from nicegui.ui_run import set_storage_secret

from web.components import visual_similarity_dialog as vsd
from web.state import state as web_state

PAGE = '/_render_smoke/visual-similarity'
ORIGINAL_SID, ORIGINAL_SHELFMARK = '990000000000100', 'T-S Original'

# alma_id -> (shelfmark, library_code, domain)
SUGGESTIONS = {
    '990000000000101': ('T-S Alpha', 'CUL', 'Bible'),
    '990000000000102': ('ENA Beta', 'JTS', 'Liturgy'),
    '990000000000103': ('T-S Gamma', 'CUL', 'Bible'),
    '990000000000104': ('Mosseri Delta', 'Mosseri', 'Liturgy'),
}


class _StubVisualSimilarity:
    def get_suggestions(self, sys_id, limit):
        return [
            {'alma_id': sid, 'svm_score': 1.0 - rank / 10, 'rank': rank}
            for rank, sid in enumerate(SUGGESTIONS, start=1)
        ]


class _StubFjms:
    def is_available(self):
        return True

    def get_domains_for_sys_ids(self, sys_ids):
        return {
            sid: [{
                'domain': SUGGESTIONS[sid][2], 'domain_heb': '',
                'parent_domain': '', 'parent_domain_heb': '',
            }]
            for sid in sys_ids if sid in SUGGESTIONS
        }


@ui.page(PAGE)
async def _visual_similarity_page():
    await vsd.show_visual_similarity_dialog(
        ORIGINAL_SID, ORIGINAL_SHELFMARK, vs_service=_StubVisualSimilarity(),
    )


def _thumbnail(sid):
    shelfmark, library_code, _ = SUGGESTIONS[sid]
    return vsd._suggestion_thumbnail_url(
        {'alma_id': sid, 'shelfmark': shelfmark, 'library_code': library_code}
    )


@asynccontextmanager
async def _user():
    saved_handlers = list(core.app._startup_handlers)
    core.app._startup_handlers.clear()
    saved_meta_mgr = web_state.meta_mgr
    web_state.meta_mgr = SimpleNamespace(csv_bank={
        sid: {'shelfmark': shelfmark, 'library_code': library_code}
        for sid, (shelfmark, library_code, _) in SUGGESTIONS.items()
    })
    try:
        prepare_simulation()
        set_storage_secret('visual-similarity-render-smoke-secret', {})
        async with AsyncExitStack() as stack:
            stack.enter_context(patch('shared.fjms_service.get_fjms_service', return_value=_StubFjms()))
            # The original's image and text are fetched from services this test has no data for.
            stack.enter_context(patch.object(vsd, '_fetch_original_info', AsyncMock(return_value={})))
            os.environ['NICEGUI_USER_SIMULATION'] = 'true'
            try:
                await stack.enter_async_context(core.app.router.lifespan_context(core.app))
                client = await stack.enter_async_context(httpx.AsyncClient(
                    transport=httpx.ASGITransport(core.app), base_url='http://test'))
                yield User(client)
            finally:
                os.environ.pop('NICEGUI_USER_SIMULATION', None)
    finally:
        web_state.meta_mgr = saved_meta_mgr
        core.app._startup_handlers.clear()
        core.app._startup_handlers.extend(saved_handlers)


def _run(driver):
    saved_slots = list(nicegui_context.slot_stack)

    async def invoke():
        async with _user() as user:
            return await driver(user)

    try:
        return asyncio.run(invoke())
    finally:
        nicegui_context.slot_stack.clear()
        nicegui_context.slot_stack.extend(saved_slots)


def _domain_select(user):
    """The Domain select: the only one whose options are the domain names."""
    [select] = [s for s in user.find(ui.select).elements if s.options == ['Bible', 'Liturgy']]
    return select


def _choose_domain(user, domain):
    """Pick a domain the way the browser does: Quasar emits the chosen options."""
    select = _domain_select(user)
    index = select.options.index(domain)
    UserInteraction(user, {select}, None).trigger(
        'update:modelValue', [{'value': index, 'label': domain}])


def _choose_view(user, view):
    """Pick List or Thumbnails the way the browser does: by option index."""
    [toggle] = user.find(ui.toggle).elements
    UserInteraction(user, {toggle}, None).trigger(
        'update:modelValue', list(toggle.options).index(view))


def _thumbnail_sources(user):
    # Not user.find(ui.image): that raises when there is none, which the list view relies on.
    return {
        element.props.get('src')
        for element in user.client.elements.values() if isinstance(element, ui.image)
    }


def test_choosing_a_domain_narrows_the_suggestions():
    async def driver(user):
        await user.open(PAGE)
        for shelfmark in ('T-S Alpha', 'ENA Beta', 'T-S Gamma', 'Mosseri Delta'):
            await user.should_see(shelfmark)

        _choose_domain(user, 'Bible')

        await user.should_see('T-S Alpha')
        await user.should_see('T-S Gamma')
        await user.should_not_see('ENA Beta')
        await user.should_not_see('Mosseri Delta')

    _run(driver)


def test_thumbnails_show_one_image_per_filtered_suggestion():
    async def driver(user):
        await user.open(PAGE)
        # The list shows no suggestion images until a row is expanded.
        assert _thumbnail_sources(user) == set()

        _choose_domain(user, 'Bible')
        _choose_view(user, 'grid')

        assert _thumbnail_sources(user) == {
            _thumbnail('990000000000101'), _thumbnail('990000000000103'),
        }
        await user.should_see('T-S Alpha')
        await user.should_not_see('ENA Beta')

        _choose_view(user, 'list')
        assert _thumbnail_sources(user) == set()
        await user.should_see('T-S Gamma')

    _run(driver)


def test_the_view_choice_is_remembered_for_the_next_window():
    async def driver(user):
        await user.open(PAGE)
        _choose_view(user, 'grid')

        await user.open(PAGE)
        assert _thumbnail_sources(user) == {_thumbnail(sid) for sid in SUGGESTIONS}

    _run(driver)
