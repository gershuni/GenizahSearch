"""Browse opens Penn catalogue items and keeps the same scan when switching sources."""

from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import patch

import pytest
from nicegui import ui

from shared.nli_crossref_service import katz_canvases, katz_component_links, katz_reference_revision, katz_viewer_url
from tests.render_smoke.test_browse_button_icons_render_smoke import (
    OPEN_PAGE, _all_elements, _button, _button_with_tooltip, _click, _run_browse,
    _viewer_image_html, _wait_until,
)
from web.state import state


@pytest.mark.parametrize('metadata_only,row_has_fl', [(False, False), (True, False), (True, True)])
def test_halper_77_opens_penn_and_switches_the_same_verso(metadata_only, row_has_fl):
    sid = '990001664240205171'
    verso = '262908052'
    nli = [{'label': 'Frag. 001v', 'fl_id': verso}, {'label': 'Frag. 001r', 'fl_id': '262908051'}]
    entry = {'external_provider': 'katz', 'images_ext': katz_canvases(sid, nli),
             'attribution': 'Provided by the University of Pennsylvania Libraries.',
             'attribution_nli': 'National Library of Israel', 'katz_checked': katz_reference_revision(sid)}
    meta = SimpleNamespace(nli_cache={sid: entry}, get_library_for_id=lambda _: 'Katz')
    page = replace(OPEN_PAGE, sys_id=sid, library_code='Katz', shelfmark='Halper 77',
                   is_cambridge=False, external_url=None, fl_id=verso, total_pages=2,
                   image_source_info={'nli_fgp': True})
    crossref = None
    if metadata_only:
        rows = [{'folio_label': '1r'}, {'folio_label': '1v'}]
        if row_has_fl:
            rows[0]['fl_id'], rows[1]['fl_id'] = '262908051', verso
        crossref = SimpleNamespace(
            is_available=lambda: True, get_folio_images=lambda _: rows, get_crossref_metadata=lambda _: {},
            get_image_sources=lambda *a, **kw: {'nli_fgp': True},
        )
        page = replace(page, uid='', text='', full_header='', fl_id=None, metadata_only=True,
                       p_num=2, current_idx=2, total_pages=0)

    async def driver(user):
        await user.open(f'/browse?sys_id={sid}&page=2')
        await _wait_until(lambda: '/api/katz_image/' in _viewer_image_html(user))
        image = _viewer_image_html(user)
        assert f'/api/katz_image/{sid}?page=0&fl={verso}' in image
        assert f'data-nli-fl="{verso}"' in image
        if metadata_only:
            assert (page.p_num, page.current_idx, page.total_pages, page.folio_label) == (2, 2, 2, '1v')
            assert page.fl_id == verso
        # The Penn button follows Browse's background lookup of the library link, which can
        # land after the image: wait for it as for the image.
        await _wait_until(lambda: any(isinstance(t, ui.tooltip) and t.text == 'Open in Penn Libraries'
                                      for t in _all_elements(user)))
        penn = _button_with_tooltip(user, 'Open in Penn Libraries')
        assert penn.props['href'] == katz_viewer_url(sid)['url']
        [switch] = _button(user, 'NLI')
        _click(user, switch)
        await _wait_until(lambda: f'/api/nli_image/{verso}' in _viewer_image_html(user))
        assert f'/api/nli_image/{verso}' in _viewer_image_html(user)
        assert any(e.props.get('data-role') == 'image-credit' and e.text == 'National Library of Israel'
                   for e in _all_elements(user) if isinstance(e, ui.label))

    with patch.object(state, 'meta_mgr', meta):
        _run_browse(page, driver, crossref=crossref)


def test_composite_record_offers_each_halper_item_by_name():
    sid = '990001935630205171'
    page = replace(OPEN_PAGE, sys_id=sid, library_code='Katz', shelfmark='Halper 138, 143',
                   is_cambridge=False, external_url=None, image_source_info={})
    meta = SimpleNamespace(nli_cache={}, get_library_for_id=lambda _: 'Katz', enrich_metadata=lambda _: {})

    async def driver(user):
        await user.open(f'/browse?sys_id={sid}')
        expected = {item['url'] for item in katz_component_links(sid)}
        assert len(expected) == 2

        def destinations():
            return {e.props.get('href') for e in _all_elements(user) if isinstance(e, ui.link)}

        await _wait_until(lambda: expected <= destinations())
        assert expected <= destinations()
        assert '/api/katz_image/' not in _viewer_image_html(user)

    with patch.object(state, 'meta_mgr', meta):
        _run_browse(page, driver)
