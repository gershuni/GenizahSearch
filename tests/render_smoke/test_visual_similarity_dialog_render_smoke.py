"""Server-render coverage for the browse page's Visual Similarity window.

Pinned here by driving the real dialog through the client's own events:

1. Choosing a domain narrows the suggestions. The Sort / Library / Domain
   selects used raw 'update:model-value' listeners whose handlers read
   ``e.value``; GenericEventArguments has none, so every change raised and the
   list was never re-filtered (found 2026-10-18 from a researcher's report).
2. The Thumbnails view shows one low-resolution image per filtered suggestion,
   from the same proxy URL the Joins Lab grid uses.
3. The List / Thumbnails choice is remembered for the visitor's next window.
4. Each domain and library option says how many suggestions carry it, and
   the Hebrew UI shows the Hebrew domain names (they were English in both UIs).
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
    '990000000000104': ('Mosseri Delta', 'Mosseri', 'Poetry'),
}
# Poetry has no Hebrew name here, so the Hebrew UI falls back to the English one.
DOMAIN_HEB = {'Bible': 'מקרא', 'Liturgy': 'תפילה'}


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
                'domain': SUGGESTIONS[sid][2],
                'domain_heb': DOMAIN_HEB.get(SUGGESTIONS[sid][2], ''),
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


def _run(driver, lang='en'):
    saved_slots = list(nicegui_context.slot_stack)

    async def invoke():
        # The UI language is a process-wide default ('he'), so pin it per test.
        with patch.object(vsd, 'get_language', return_value=lang):
            async with _user() as user:
                return await driver(user)

    try:
        return asyncio.run(invoke())
    finally:
        nicegui_context.slot_stack.clear()
        nicegui_context.slot_stack.extend(saved_slots)


def _domain_select(user):
    """The Domain select: the only one whose option values are the domain names."""
    [select] = [
        s for s in user.find(ui.select).elements
        if set(s.options) == {'Bible', 'Liturgy', 'Poetry'}
    ]
    return select


def _library_select(user):
    """The Library select: the only one whose option values are the library codes."""
    [select] = [
        s for s in user.find(ui.select).elements
        if set(s.options) == {'CUL', 'JTS', 'Mosseri'}
    ]
    return select


def _choose(user, select, value):
    """Pick an option the way the browser does: Quasar emits the chosen options."""
    index = list(select.options).index(value)
    UserInteraction(user, {select}, None).trigger(
        'update:modelValue', [{'value': index, 'label': select.options[value]}])


def _choose_domain(user, domain):
    _choose(user, _domain_select(user), domain)


def _toggle_with(user, options):
    """The toggle whose option values are *options* (the window has more than one)."""
    [toggle] = [t for t in user.find(ui.toggle).elements if set(t.options) == set(options)]
    return toggle


def _choose_view(user, view):
    """Pick List or Thumbnails the way the browser does: by option index."""
    toggle = _toggle_with(user, {'list', 'grid'})
    UserInteraction(user, {toggle}, None).trigger(
        'update:modelValue', list(toggle.options).index(view))


def _choose_thumb_size(user, size):
    toggle = _toggle_with(user, {'s', 'm', 'l'})
    UserInteraction(user, {toggle}, None).trigger(
        'update:modelValue', list(toggle.options).index(size))


def _image_heights(user):
    return {
        element._style.get('height')
        for element in user.client.elements.values() if isinstance(element, ui.image)
    }


def _marked(user, marker):
    return [e for e in user.client.elements.values() if marker in e._markers]


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


def test_each_domain_option_counts_its_suggestions():
    async def driver(user):
        await user.open(PAGE)
        assert list(_domain_select(user).options.items()) == [
            ('Bible', 'Bible (2)'), ('Liturgy', 'Liturgy (1)'), ('Poetry', 'Poetry (1)'),
        ]

    _run(driver)


def test_each_library_option_counts_its_suggestions_and_still_filters():
    async def driver(user):
        await user.open(PAGE)
        assert list(_library_select(user).options.items()) == [
            ('CUL', 'CUL (2)'), ('JTS', 'JTS (1)'), ('Mosseri', 'Mosseri (1)'),
        ]

        _choose(user, _library_select(user), 'CUL')

        await user.should_see('T-S Alpha')
        await user.should_see('T-S Gamma')
        await user.should_not_see('ENA Beta')
        await user.should_not_see('Mosseri Delta')

    _run(driver)


def test_the_hebrew_ui_shows_hebrew_domain_names():
    async def driver(user):
        await user.open(PAGE)
        # In label order; a domain with no Hebrew name keeps its English one.
        assert list(_domain_select(user).options.items()) == [
            ('Poetry', 'Poetry (1)'), ('Bible', 'מקרא (2)'), ('Liturgy', 'תפילה (1)'),
        ]
        await user.should_see('תפילה')
        await user.should_not_see('Liturgy')

        _choose_view(user, 'grid')
        await user.should_see('תפילה')
        await user.should_not_see('Liturgy')

        # The filter still works by the domain itself, not by its label.
        _choose_domain(user, 'Bible')
        await user.should_see('T-S Alpha')
        await user.should_not_see('ENA Beta')

    _run(driver, lang='he')


def test_the_thumbnail_size_resizes_the_grid_and_is_remembered():
    """Small / Medium / Large set the grid's column width and the image height
    (owner, 2026-10-19), only in the grid view, and stay for the next window."""
    async def driver(user):
        await user.open(PAGE)
        [size_toggle] = _marked(user, 'vs-thumb-size')
        assert not size_toggle.visible, "the list view has no thumbnails to size"

        _choose_view(user, 'grid')
        assert size_toggle.visible
        assert _image_heights(user) == {'170px'}

        _choose_thumb_size(user, 'l')
        assert _image_heights(user) == {'300px'}
        [grid] = _marked(user, 'vs-thumb-grid')
        assert 'minmax(min(300px, 100%), 1fr)' in grid._style['grid-template-columns']

        await user.open(PAGE)
        assert _image_heights(user) == {'300px'}

    _run(driver)


def test_the_original_can_be_enlarged_and_it_is_remembered():
    async def driver(user):
        await user.open(PAGE)
        [pane] = _marked(user, 'vs-original-pane')
        [button] = _marked(user, 'vs-original-size')
        assert pane._style['width'] == '260px'

        UserInteraction(user, {button}, None).trigger('click')
        assert pane._style['width'] == 'min(450px, 45vw)'
        assert button.props['icon'] == 'zoom_in_map'

        await user.open(PAGE)
        [pane] = _marked(user, 'vs-original-pane')
        assert pane._style['width'] == 'min(450px, 45vw)'

        [button] = _marked(user, 'vs-original-size')
        UserInteraction(user, {button}, None).trigger('click')
        assert pane._style['width'] == '260px'

    _run(driver)
