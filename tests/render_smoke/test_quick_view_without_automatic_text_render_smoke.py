# -*- coding: utf-8 -*-
"""Render-smoke: Quick View on a page without an automatic transcription.

Browse was fixed on 2026-10-27 (84e42e93, docs/plans/EDITIONS_WITHOUT_AUTOMATIC_TEXT_PLAN.md): such a page takes
its automatic text as empty, so the version chooser is built and an FGP or PGP edition of the page shows by the
usual default rule. Quick View (``open_advanced_dialog`` in web/pages/search_results.py) built its chooser, and its
Edit, Comment and Joins buttons, only when the page had an automatic text. A reader reaches such a page there by
paging from a hit on another page of the manuscript, or from a shelfmark/title search for a manuscript with no
automatic text at all (the result opens on page 1). On such a page with no edition either, Quick View showed the
hit page's snippet as that page's text; it now says "No text available", as Browse does.

Drives the real dialog on a page of its own, over a stubbed browse service and FGP source list. T-S NS 243.146
page 1 has an FGP edition (row 22952) and no automatic text; page 2 is given an automatic text here.
"""
from __future__ import annotations

import asyncio
import os
import threading
import time
from contextlib import AsyncExitStack, asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import httpx
from nicegui import core, events, ui
from nicegui.context import context as nicegui_context
from nicegui.testing.general import prepare_simulation
from nicegui.testing.user import User
from nicegui.ui_run import set_storage_secret

import web.main as _web_main  # noqa: F401  (registers the app's routes, as the rest of this package does)
from tests.render_smoke._bounded_run import run_bounded
from web.pages import search_results
from web.services import BrowsePage
from web.translations import using_language

PAGE = '/_render_smoke/quick-view'
SID = '990051614260205171'                # T-S NS 243.146
SYNTHETIC_SID = '990001234560000000'      # a catalogue-only record (shared/synthetic_sys_id.py)
NO_EDITION_SID = '990051614270205171'     # T-S NS 243.147 here: page 1 has no automatic text and no edition
# Manuscripts the index holds no pages of (no automatic text at all): the engine's page has total_pages 0.
NO_TEXT_SID = '990051614250205171'        # T-S NS 243.145: 4 NLI images
UNCOUNTED_SID = '990051614280205171'      # its images cannot be counted (NLI unreadable, no crossref folios)
IMAGE_COUNTS = {NO_TEXT_SID: 4}
SLOW_SID = '990051614290205171'           # 4 images too, but counting them takes the NLI manifest (slow)
SLOW_FETCH_SID = '990051614300205171'     # 2 images; its page takes longer to fetch than SLOW_SID's count
IMAGE_COUNTS[SLOW_FETCH_SID] = 2
SLOW_COUNT_DONE = threading.Event()       # SLOW_SID's count has come
MEDIUM_SID = '990051614320205171'         # 4 images, counted in 1.5 s: inside a wait widened past it
MEDIUM_COUNT_DONE = threading.Event()
SLOW_FETCH_DONE = threading.Event()       # SLOW_FETCH_SID's page has come
SHELFMARK = 'T-S NS 243.146'
FGP_TEXT = 'אֵל מְשוך בְטלה / לְנִפְקְדו בַטְלאִ ים ' * 20
HTR_TEXT = 'שורה ראשונה של הדף השני\nשורה שנייה של הדף השני'

# One chooser-shaped FGP edition (shared/fgp_service.py::_row_to_fgp_source) on folio 1r, i.e. page 1 only.
EDITION = {
    'source': 'fgp', 'is_fgp': True, 'id': 22952, 'uid': 'fgp:22952', 'doc_relation': 'Digital Edition',
    'language': None, 'content': FGP_TEXT, 'sections': None, 'page_info': 'recto', 'source_scholar': 'FGP',
    'attribution': 'Friedberg Genizah Project', 'source_credit': None, 'source_credit_he': None,
    'source_credit_en': 'FGP team', 'folio_num': 1, 'image_side': '1r', 'folio_label': '1r',
    'fgp_c_number': None, 'sequence_order': 0,
}


def _page(sys_id, p_num, text, total_pages=2):
    return BrowsePage(
        uid=f'u{p_num}', p_num=p_num, text=text, full_header=f'{sys_id}_IE1_P{p_num:06d}_FL{p_num}',
        total_pages=total_pages, current_idx=p_num, sys_id=sys_id, shelfmark=SHELFMARK, library_code='CUL',
    )


PAGES = {
    SID: [_page(SID, 1, ''), _page(SID, 2, HTR_TEXT)],
    SYNTHETIC_SID: [_page(SYNTHETIC_SID, 1, '', total_pages=1)],
    NO_EDITION_SID: [_page(NO_EDITION_SID, 1, ''), _page(NO_EDITION_SID, 2, HTR_TEXT)],
}


class _Service:
    def get_browse_page(self, sys_id, p_num=None, direction=0, volume_ie=None, **_kw):
        if sys_id == SLOW_FETCH_SID:
            time.sleep(4.5)
            SLOW_FETCH_DONE.set()
        if sys_id in (NO_TEXT_SID, UNCOUNTED_SID, SLOW_SID, SLOW_FETCH_SID, MEDIUM_SID):
            # SearchEngine._get_metadata_only_browse_page: the requested page, moved by direction, no count
            return _page(sys_id, max(1, (p_num or 1) + direction), '', total_pages=0)
        pages = PAGES.get(sys_id) or []
        idx = (p_num or 1) - 1 + direction
        return pages[idx] if 0 <= idx < len(pages) else None

    def get_full_manuscript(self, sys_id):
        return PAGES.get(sys_id) or []


def _result(sys_id, img, snippet='', full_text=''):
    return {
        'display': {'id': sys_id, 'shelfmark': SHELFMARK, 'title': '', 'img': img, 'source': 'V0.8',
                    'library_code': 'CUL'},
        'snippet': snippet, 'full_text': full_text,
    }


# A hit on page 2; a shelfmark-search row for a manuscript with no automatic text (img '', no snippet).
RESULTS = {
    'hit': _result(SID, '2', snippet='שורה *שנייה* של הדף השני', full_text=HTR_TEXT),
    'shelfmark': _result(NO_TEXT_SID, ''),
    'uncounted': _result(UNCOUNTED_SID, ''),
    'slow': _result(SLOW_SID, ''),
    'slow-fetch': _result(SLOW_FETCH_SID, ''),
    'medium': _result(MEDIUM_SID, ''),
    'catalogue': _result(SYNTHETIC_SID, ''),
    'no-edition': _result(NO_EDITION_SID, '2', snippet='שורה *שנייה* של הדף השני', full_text=HTR_TEXT),
}


@ui.page(PAGE)
def _quick_view_page(case: str = 'hit'):
    # 'a,b': two results, opened on the first with Next/Previous result enabled
    results = [RESULTS[c] for c in case.split(',')]
    search_state = SimpleNamespace(displayed_results=results, title_translations={}, printed_ids=set(),
                                   translation_data={})
    with using_language('en'):
        search_results.open_advanced_dialog(search_state, SimpleNamespace(), 0 if len(results) > 1 else None,
                                            results[0])


def _image_count(sys_id, folio_images=None, suffix=1):
    if sys_id == SLOW_SID:
        time.sleep(3.0)
        SLOW_COUNT_DONE.set()
        return 4
    if sys_id == MEDIUM_SID:
        time.sleep(1.5)
        MEDIUM_COUNT_DONE.set()
        return 4
    return IMAGE_COUNTS.get(sys_id, 0)


class _Unavailable:
    def is_available(self):
        return False


@asynccontextmanager
async def _user():
    saved_handlers = list(core.app._startup_handlers)
    core.app._startup_handlers.clear()
    try:
        prepare_simulation()
        set_storage_secret('quick-view-render-smoke-secret', {})
        async with AsyncExitStack() as stack:
            for target, value in [
                ('get_service', lambda: _Service()),
                ('web_fgp_enabled', lambda: True),
                ('get_all_sources_for_fragment', lambda sys_id: []),
                ('get_fgp_sources_for_fragment',
                 lambda sys_id: [dict(EDITION)] if sys_id in (SID, NO_TEXT_SID) else []),
                ('get_document_for_fragment', lambda sys_id, p_num=None: None),
                # The displayed folio of each page; the edition is on 1r.
                ('displayed_folio_label', lambda sys_id, p, total=0: {1: '1r', 2: '1v'}.get(p, '')),
                ('displayed_fgp_image_number', lambda sys_id, p, total=0: ''),
            ]:
                stack.enter_context(patch.object(search_results, target, value))
            stack.enter_context(patch('web.page_count.manuscript_image_count', _image_count))
            stack.enter_context(patch('web.page_count.PAGE_COUNT_WAIT_S', 0.05))
            stack.enter_context(patch('shared.fjms_service.get_fjms_service', return_value=_Unavailable()))
            stack.enter_context(patch('shared.nli_crossref_service.get_nli_crossref_service',
                                      return_value=_Unavailable()))
            # Supabase reads: no approved versions, no joins.
            stack.enter_context(patch('web.components.version_selector.fetch_page_versions_async',
                                      AsyncMock(return_value={'all_versions': [], 'current_default': None,
                                                              'total': 0})))
            stack.enter_context(patch('web.components.joins_panel.fetch_connected_fragments',
                                      lambda **_kw: {'total_fragments': 1}))
            os.environ['NICEGUI_USER_SIMULATION'] = 'true'
            try:
                await stack.enter_async_context(core.app.router.lifespan_context(core.app))
                client = await stack.enter_async_context(httpx.AsyncClient(
                    transport=httpx.ASGITransport(core.app), base_url='http://test'))
                yield User(client)
            finally:
                os.environ.pop('NICEGUI_USER_SIMULATION', None)
    finally:
        core.app._startup_handlers.clear()
        core.app._startup_handlers.extend(saved_handlers)


def _run(driver):
    saved_slots = list(nicegui_context.slot_stack)

    async def invoke():
        async with _user() as user:
            return await driver(user)

    try:
        return run_bounded(invoke())
    finally:
        nicegui_context.slot_stack.clear()
        nicegui_context.slot_stack.extend(saved_slots)


def _elements(user):
    with user._client:
        return list(user._client.elements.values())


def _labels(user):
    return {e.text for e in _elements(user) if isinstance(e, ui.label)}


def _html(user):
    return ' '.join(e.content for e in _elements(user) if isinstance(e, ui.html))


def _button(user, aria_label):
    return [b for b in _elements(user) if isinstance(b, ui.button) and b.props.get('aria-label') == aria_label]


def _history_buttons(user):
    return [b for b in _elements(user) if isinstance(b, ui.button) and b.props.get('icon') == 'history']


async def _wait_until(ready, timeout=10.0):
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while not ready() and loop.time() < deadline:
        await asyncio.sleep(0.05)


def _click(user, element):
    """Fire the button's click listeners (as tests/render_smoke/test_browse_button_icons_render_smoke.py)."""
    with user._client:
        for listener in list(element._event_listeners.values()):
            if listener.element_id == element.id and listener.type == 'click':
                events.handle_event(listener.handler, events.ClickEventArguments(sender=element, client=user._client))


def _version_label(user):
    """The chooser's own label (version_selector.py): the first child of the row that holds its history button.
    Not any 'V0.8' label: the info bar's source chip also reads 'V0.8'."""
    buttons = _history_buttons(user)
    if len(buttons) != 1:
        return None
    return buttons[0].parent_slot.parent.default_slot.children[0].text


def _edition_shown(user):
    return 'מְשוך' in _html(user) and _version_label(user) == 'FGP'


def _assert_reading_view(user):
    assert _history_buttons(user), 'no version chooser'
    assert _button(user, 'Edit'), 'no Edit button'
    assert _button(user, 'Send Comment'), 'no Comment button'


def test_paging_to_a_page_without_automatic_text_shows_its_edition_and_the_chooser():
    async def driver(user):
        await user.open(f'{PAGE}?case=hit')
        await _wait_until(lambda: _version_label(user) == 'V0.8' and 'שנייה' in _html(user))
        # The hit's own page: its automatic text, as before.
        assert _version_label(user) == 'V0.8' and 'שנייה' in _html(user) and 'מְשוך' not in _html(user)
        _assert_reading_view(user)

        [previous] = _button(user, 'Previous Page')
        _click(user, previous)
        await _wait_until(lambda: _edition_shown(user))
        assert _edition_shown(user), (sorted(_labels(user)), _html(user)[:300])
        _assert_reading_view(user)

    _run(driver)


def test_a_shelfmark_result_without_any_automatic_text_opens_on_its_edition():
    async def driver(user):
        await user.open(f'{PAGE}?case=shelfmark')
        await _wait_until(lambda: _edition_shown(user))
        assert _edition_shown(user), (sorted(_labels(user)), _html(user)[:300])
        _assert_reading_view(user)
        # The index has no pages of it (total_pages 0): its 4 images are its pages, not "Page 1/0".
        assert 'Page 1/4' in _labels(user), sorted(_labels(user))

        [following] = _button(user, 'Next Page')
        _click(user, following)
        await _wait_until(lambda: 'Page 2/4' in _labels(user))
        assert 'Page 2/4' in _labels(user)
        assert 'מְשוך' not in _html(user)  # the edition is of folio 1r

    _run(driver)


def test_a_page_count_that_cannot_be_known_is_not_shown_as_zero():
    async def driver(user):
        await user.open(f'{PAGE}?case=uncounted')
        await _wait_until(lambda: 'Page 1' in _labels(user))
        await asyncio.sleep(0.3)
        labels = _labels(user)
        assert 'Page 1' in labels and not [label for label in labels if label.endswith('/0')], sorted(labels)

    _run(driver)


def test_a_catalogue_only_record_still_gets_no_chooser():
    async def driver(user):
        await user.open(f'{PAGE}?case=catalogue')
        await _wait_until(lambda: 'Page 1/1' in _labels(user))
        assert 'Page 1/1' in _labels(user)  # the dialog has rendered the page
        await asyncio.sleep(0.3)  # past a chooser's deferred default
        assert not _history_buttons(user)
        assert not _button(user, 'Edit') and not _button(user, 'Send Comment')

    _run(driver)


def test_a_page_with_no_text_and_no_edition_says_so_not_the_hits_snippet():
    # The result's snippet is the hit page's text; on another page it is not that page's (Browse: "No text available").
    async def driver(user):
        await user.open(f'{PAGE}?case=no-edition')
        await _wait_until(lambda: _version_label(user) == 'V0.8' and 'שנייה' in _html(user))
        [previous] = _button(user, 'Previous Page')
        _click(user, previous)
        await _wait_until(lambda: 'No text available' in _labels(user))
        await asyncio.sleep(0.3)  # past the chooser's deferred default
        assert 'No text available' in _labels(user)
        assert 'שנייה' not in _html(user)
        assert _version_label(user) == 'V0.8'
        _assert_reading_view(user)

    _run(driver)


def test_a_slow_count_does_not_hold_the_dialog_up():
    # Codex on #454: a count that needs the NLI manifest must not leave the dialog blank until it comes.
    async def driver(user):
        await user.open(f'{PAGE}?case=slow')
        await _wait_until(lambda: 'Page 1' in _labels(user), timeout=2.5)
        assert 'Page 1' in _labels(user), 'the page did not render before its count came'
        assert 'Page 1/4' not in _labels(user)
        await _wait_until(lambda: 'Page 1/4' in _labels(user))
        assert 'Page 1/4' in _labels(user), sorted(_labels(user))
        assert _button(user, 'Next Page')

    _run(driver)


def test_a_late_count_does_not_render_over_the_next_result():
    # Codex on #454: Next result selects it before its page arrives; the first result's late count must not render
    # the second result over the first one's page.
    SLOW_COUNT_DONE.clear()
    SLOW_FETCH_DONE.clear()

    async def driver(user):
        await user.open(f'{PAGE}?case=slow,slow-fetch')
        await _wait_until(lambda: 'Page 1' in _labels(user), timeout=2.5)
        [next_result] = _button(user, 'Next')
        _click(user, next_result)
        await _wait_until(lambda: 'Result 2 / 2' in _labels(user))
        await _wait_until(SLOW_COUNT_DONE.is_set, timeout=10)
        await asyncio.sleep(0.3)  # past the late count's callback
        assert SLOW_COUNT_DONE.is_set() and not SLOW_FETCH_DONE.is_set(), 'the window was not reached'
        assert 'Page 1/4' not in _labels(user), sorted(_labels(user))
        await _wait_until(lambda: 'Page 1/2' in _labels(user))
        assert 'Page 1/2' in _labels(user), sorted(_labels(user))

    _run(driver)


def test_a_load_overtaken_by_the_next_result_does_not_render_over_it():
    # Codex on #454: result 1 waits (within PAGE_COUNT_WAIT_S) for its count; the reader moves to result 2, which
    # renders first. Result 1's load must then stop, not render result 1 under result 2's header.
    MEDIUM_COUNT_DONE.clear()

    async def driver(user):
        with patch('web.page_count.PAGE_COUNT_WAIT_S', 3.0):
            await user.open(f'{PAGE}?case=medium,hit')
            await _wait_until(lambda: _button(user, 'Next'))
            [next_result] = _button(user, 'Next')
            _click(user, next_result)
            await _wait_until(lambda: _version_label(user) == 'V0.8' and 'שנייה' in _html(user))
            assert not MEDIUM_COUNT_DONE.is_set(), 'result 2 did not render first'
            await _wait_until(MEDIUM_COUNT_DONE.is_set, timeout=10)
            await asyncio.sleep(0.3)  # past result 1's load resuming
            labels = _labels(user)
            assert 'Result 2 / 2' in labels and 'Page 1/4' not in labels, sorted(labels)
            assert 'שנייה' in _html(user)

    _run(driver)
