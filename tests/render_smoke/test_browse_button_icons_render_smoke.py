# -*- coding: utf-8 -*-
"""Render-smoke: the Browse page's text buttons carry an icon (owner, 2026-10-19).

The owner's rules: website only; an icon goes next to the text, and may stand
in for it only where the text would widen a row on a phone. Every control here
keeps its text in the render tree. Where the phone CSS hides that text
(``browse-icon-collapse``), the button keeps its name through ``aria-label``,
so this adds no nameless icon-only buttons; the tracker already lists about
158 of those.

Drives the real ``/browse`` against a stubbed browse service (harness: the
homepage User context of ``test_home_teaser_render_smoke.py``), plus the
enrichment chips through ``populate_bib_catalog_buttons`` on a page of its own.
"""
from __future__ import annotations

import asyncio
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

from nicegui import events, ui

from tests.render_smoke.test_home_teaser_render_smoke import _all_elements, _run_home_smoke
from web.pages import browse
from web.pages.browse_enrichment import populate_bib_catalog_buttons
from web.pages.browse_state import BrowseState
from web.services import BrowsePage
from web.translations import using_language

SID = '990000000000000001'
CUDL_URL = 'https://cudl.lib.cam.ac.uk/view/MS-TS-00012-00001'

# A Cambridge fragment with NLI images too, but no Cambridge images of its own:
# each source chip then opens that library's viewer in a new tab.
OPEN_PAGE = BrowsePage(
    uid='u1', p_num=1, text='שלום', full_header='V0.8', total_pages=3, current_idx=1,
    sys_id=SID, shelfmark='T-S 12.001', library_code='CUL',
    is_cambridge=True, external_url=CUDL_URL,
    image_source_info={'nli_fgp': True, 'cambridge': True, 'image_count': 3},
)
# The same fragment WITH Cambridge images: the chips switch the images shown here.
SWITCH_PAGE = replace(OPEN_PAGE, cambridge_images=[{'url': 'https://example.org/iiif/1/full/max/0/default.jpg'}])


def _service(page):
    svc = MagicMock()
    svc.is_ready.return_value = True
    svc.get_browse_page.return_value = page
    svc.get_browse_page_by_fl.return_value = page
    return svc


def _buttons(user):
    return [e for e in _all_elements(user) if isinstance(e, ui.button)]


def _button(user, text):
    found = [b for b in _buttons(user) if b.text == text]
    assert found, f'no {text!r} button; buttons: {sorted({b.text for b in _buttons(user) if b.text})}'
    return found


async def _wait_until(ready, timeout=10.0):
    """Poll until ``ready()`` holds, at most ``timeout`` s; the assertion after
    it says what is missing. Fixed sleeps were too short on Windows CI: the
    page had not drawn its chips 0.5 s after opening (PR #419), while the same
    open takes ~0.25 s on Linux."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while not ready() and loop.time() < deadline:
        await asyncio.sleep(0.05)


def _texts(user):
    return [b.text for b in _buttons(user)]


def _labels(user):
    return {e.text for e in _all_elements(user) if isinstance(e, ui.label)}


async def _open_browse(user, ready):
    await user.open(f'/browse?sys_id={SID}')
    await _wait_until(lambda: ready(_texts(user)))


def _button_with_tooltip(user, text):
    """``.tooltip()`` adds a sibling ui.tooltip that targets the button's html id."""
    targets = {t.props.get('target') for t in _all_elements(user)
               if isinstance(t, ui.tooltip) and t.text == text}
    [button] = [b for b in _buttons(user) if f'#{b.html_id}' in targets]
    return button


def _click(user, element):
    """Fire the button's click listeners. Not UserInteraction.click(): these
    handlers rebuild the page, which changes the listener dict it iterates."""
    with user._client:
        for listener in list(element._event_listeners.values()):
            if listener.element_id == element.id and listener.type == 'click':
                args = events.ClickEventArguments(sender=element, client=user._client)
                events.handle_event(listener.handler, args)


def _run_browse(page, driver, lang='en'):
    # The metadata panel's related-fragments loader goes to the network; stub it.
    # The browse enrichment overwrites the stubbed page's image_source_info from
    # nli_crossref.db when that sidecar is present (it is locally, not on CI), so
    # make it absent: the chips must come from the page above, not local data.
    crossref = MagicMock()
    crossref.is_available.return_value = False
    with patch.object(browse, 'get_service', return_value=_service(page)), \
            patch('shared.nli_crossref_service.get_nli_crossref_service', return_value=crossref), \
            patch.object(browse, 'load_connected_fragments_into', AsyncMock()), \
            patch.object(browse.GlobalAuthState, 'is_logged_in', return_value=True), \
            patch.object(browse.GlobalAuthState, 'get_user_id', return_value=None):
        _run_home_smoke(driver, lang=lang)


def _assert_collapsing(button, icon, text):
    """Icon and text, and a name that survives the phone CSS hiding the text."""
    assert button.props.get('icon') == icon, (text, button.props)
    assert button.text == text
    assert 'browse-icon-collapse' in button.classes
    assert button.props.get('aria-label') == text, button.props


def test_go_buttons_carry_an_icon_and_keep_their_text():
    async def driver(user):
        await _open_browse(user, lambda texts: texts.count('Go') >= 2)
        go_buttons = _button(user, 'Go')
        icons = sorted(b.props.get('icon') for b in go_buttons)
        assert icons == ['arrow_forward', 'search'], icons
        for b in go_buttons:
            _assert_collapsing(b, b.props.get('icon'), 'Go')

    _run_browse(OPEN_PAGE, driver)


def test_the_page_go_arrow_points_the_hebrew_way():
    async def driver(user):
        await _open_browse(user, lambda texts: texts.count('עבור') >= 2)
        icons = sorted(b.props.get('icon') for b in _button(user, 'עבור'))
        assert icons == ['arrow_back', 'search'], icons

    _run_browse(OPEN_PAGE, driver, lang='he')


def test_chips_that_open_a_library_viewer_carry_open_in_new_and_replace_the_icon_button():
    async def driver(user):
        await _open_browse(user, lambda texts: {'NLI', 'Cambridge'} <= set(texts))
        for name in ('NLI', 'Cambridge'):
            [chip] = _button(user, name)
            assert chip.props.get('icon-right') == 'open_in_new', chip.props
        # The chip opens the viewer itself, so the separate icon-only button is gone.
        assert not [b for b in _buttons(user) if not b.text and b.props.get('icon') == 'open_in_new']

    _run_browse(OPEN_PAGE, driver)


def test_chips_that_switch_images_carry_an_image_icon_and_keep_a_named_external_button():
    async def driver(user):
        await _open_browse(user, lambda texts: {'NLI', 'Cambridge'} <= set(texts))
        for name in ('NLI', 'Cambridge'):
            [chip] = _button(user, name)
            assert chip.props.get('icon') == 'image', chip.props
            assert 'browse-icon-wide' in chip.classes
        external = [b for b in _buttons(user) if not b.text and b.props.get('icon') == 'open_in_new']
        assert sorted(b.props.get('aria-label') for b in external) == [
            'Open in Cambridge Digital Library', 'Open in NLI KTIV',
        ]

    _run_browse(SWITCH_PAGE, driver)


def test_external_links_carry_open_in_new_after_their_text():
    async def driver(user):
        await _open_browse(user, lambda texts: 'Show Metadata' in texts)
        [show] = _button(user, 'Show Metadata')
        _click(user, show)
        await _wait_until(lambda: {'NLI Ktiv', 'Cambridge CUDL'} <= _labels(user))
        links = {}
        for link in (e for e in _all_elements(user) if isinstance(e, ui.link)):
            children = link.default_slot.children
            labels = [c.text for c in children if isinstance(c, ui.label)]
            icons = [c.props.get('name') for c in children if isinstance(c, ui.icon)]
            if labels:
                links[labels[0]] = (icons, link.props.get('href'), link.classes)
        for text in ('NLI Ktiv', 'Cambridge CUDL'):
            assert text in links, sorted(links)
            icons, href, classes = links[text]
            assert icons == ['open_in_new'], (text, icons)
            assert 'browse-icon-wide' in classes
        assert links['Cambridge CUDL'][1] == CUDL_URL

    _run_browse(OPEN_PAGE, driver)


def test_both_submit_buttons_carry_send_and_keep_their_text():
    async def driver(user):
        await _open_browse(user, lambda texts: 'Edit' in texts)
        [edit] = _button(user, 'Edit')
        _click(user, edit)
        await _wait_until(lambda: 'Submit' in _texts(user))
        [submit] = _button(user, 'Submit')
        _assert_collapsing(submit, 'send', 'Submit')
        assert 'browse-icon-flip-rtl' in submit.classes

        # The fullscreen editor has its own Submit; the inline one stays underneath.
        _click(user, _button_with_tooltip(user, 'Fullscreen Edit'))
        await _wait_until(lambda: _texts(user).count('Submit') >= 2)
        submits = _button(user, 'Submit')
        assert len(submits) == 2
        for submit in submits:
            _assert_collapsing(submit, 'send', 'Submit')

    _run_browse(OPEN_PAGE, driver)


ENRICHMENT_PAGE = '/_render_smoke/browse-enrichment-chips'


@ui.page(ENRICHMENT_PAGE)
def _enrichment_chips_page():
    state = BrowseState()
    state.fjms_data = {
        'bibliography': [{'title': 'A study'}, {'title': 'Another study'}],
        'source_names': ['Catalogue A'],
        'has_measurements': True,
        'has_visual_suggestions': True,
        'visual_suggestion_count': 7,
    }
    container = ui.element('span')
    with using_language('en'):
        populate_bib_catalog_buttons(container, state, SimpleNamespace(sys_id=SID, shelfmark='T-S 12.001', p_num=1))


def test_enrichment_chips_carry_an_icon_and_keep_their_text():
    expected = {
        'Bib. FJMS (2)': 'menu_book',
        'Bib. Ktiv (1)': 'menu_book',
        'Catalog rec. (1)': 'info_outline',
        'Measurements': 'straighten',
        'Visual Similarity (7)': 'compare',
    }

    async def driver(user):
        await user.open(ENRICHMENT_PAGE)
        chips = {b.text: b for b in _buttons(user) if b.text in expected}
        assert sorted(chips) == sorted(expected), sorted(b.text for b in _buttons(user))
        for text, icon in expected.items():
            assert chips[text].props.get('icon') == icon, (text, chips[text].props)
            assert 'browse-icon-wide' in chips[text].classes

    from web.state import state as app_state
    nli_cache = {SID: {'marc': {'bibliography': ['Ref. 1']}}}
    with patch.object(app_state, 'meta_mgr', SimpleNamespace(nli_cache=nli_cache)):
        _run_home_smoke(driver, lang='en')


def test_the_phone_rules_ship_with_the_page():
    """The classes above only keep rows from widening on a phone with these rules."""
    styles = browse.VIEWER_STYLES
    assert '@media (max-width: 599px)' in styles
    assert '.browse-icon-collapse .q-btn__content > .block { font-size: 0; width: 0; overflow: hidden; }' in styles
    assert '.browse-icon-wide .q-icon { display: none; }' in styles
    assert '[dir="rtl"] .browse-icon-flip-rtl .q-icon { transform: scaleX(-1); }' in styles
