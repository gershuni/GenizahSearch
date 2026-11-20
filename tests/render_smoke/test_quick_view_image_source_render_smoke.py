# -*- coding: utf-8 -*-
"""Render-smoke: Quick View opens on the image Browse opens on (owner, 2026-11-11).

T-S 12.782 opened on Cambridge's image in Browse and on NLI's in Quick View, which built
the NLI URL itself. ``open_advanced_dialog`` now fetches the manuscript's library images
beside the page (``quick_view_external_images``) and draws ``quick_view_image``'s choice;
when they come after ``QUICK_VIEW_EXTERNAL_WAIT_S`` the dialog shows NLI first and is drawn
again once they change the image.

Harness: tests/render_smoke/test_quick_view_without_automatic_text_render_smoke.py.
"""
from __future__ import annotations

import time
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import patch

from nicegui import ui

from tests.render_smoke.test_quick_view_without_automatic_text_render_smoke import (
    PAGE, SID, _elements, _html, _run, _version_label, _wait_until,
)
from web.pages import search_results
from shared.codicological import CodicologicalManager

CUDL = [{'url': 'https://cudl.lib.cam.ac.uk/iiif/MS-TS-NS-00243-00146/canvas/1'},
        {'url': 'https://cudl.lib.cam.ac.uk/iiif/MS-TS-NS-00243-00146/canvas/2'}]


def _external(sys_id):
    return {'sys_id': sys_id, 'cambridge_images': CUDL, 'external_provider': '',
            'cambridge_alignment': {'verdict': 'aligned'}, 'attribution': 'Cambridge University Library'}


def _slow_external(sys_id):
    time.sleep(1.0)
    return _external(sys_id)


def _shows(user, prefix):
    return f'src="{prefix}' in _html(user)


def _image_element(user):
    found = [e for e in _elements(user) if isinstance(e, ui.html) and 'adv-zoomable-image' in (e.content or '')]
    return found[0] if found else None


def test_quick_view_opens_the_current_bodleian_side_and_the_whole_volume():
    from tests.render_smoke.test_quick_view_without_automatic_text_render_smoke import PAGES

    shelfmark = 'MS heb. g.2/27'
    codico = CodicologicalManager()
    codico.load({SID: {'shelfmark': shelfmark, 'oxford_part_id': 'MS. Heb. g. 2'}})
    pages = [replace(p, library_code='Oxford', shelfmark=shelfmark, is_oxford=True) for p in PAGES[SID]]

    async def driver(user):
        # The search hit still has its old catalogue label; the opened page identifies the volume.
        with patch.dict(PAGES, {SID: pages}), \
                patch.object(search_results.state, 'meta_mgr', SimpleNamespace(codico_mgr=codico)), \
                patch.object(search_results, 'quick_view_external_images', lambda sid: {'sys_id': sid}):
            await user.open(f'{PAGE}?case=hit')
            await _wait_until(lambda: 'MS_HEB_g_2_27b.jpg' in _html(user))
            links = {e.props.get('data-role'): e.props.get('href') for e in _elements(user) if isinstance(e, ui.link)}
            assert links['bodleian-recto'].endswith('/MS_HEB_g_2_27a.jpg')
            assert links['bodleian-verso'].endswith('/MS_HEB_g_2_27b.jpg')
            assert links['bodleian-volume'].endswith('/catalog/volume_215')
            assert 'data-oxford-direct="https://hebrew.bodleian.ox.ac.uk/fragments/full/MS_HEB_g_2_27b.jpg"' in _html(user)

    _run(driver)


def _credit_label(user):
    found = [e for e in _elements(user) if isinstance(e, ui.label) and e.props.get('data-role') == 'image-credit']
    return found[0] if found else None


def test_quick_view_opens_on_the_cambridge_image_browse_shows():
    async def driver(user):
        with patch.object(search_results, 'quick_view_external_images', _external), \
                patch('shared.nli_circuit_breaker.is_open', return_value=False):
            await user.open(f'{PAGE}?case=hit')   # the hit is on page 2
            await _wait_until(lambda: 'adv-zoomable-image' in _html(user))
            assert _shows(user, f'/api/cambridge_image/{SID}?page=1'), _html(user)[:500]
            assert f'/api/nli_image_by_sysid/{SID}' not in _html(user)
            assert 'Cambridge University Library' in {getattr(e, 'text', '') for e in user._client.elements.values()}
            # Codex review: if the browser falls back to NLI's image, the credit turns to NLI's.
            assert 'data-credit-fallback="nli"' in _image_element(user).content
            credit = _credit_label(user)
            assert credit is not None and credit.props['data-credit-oxford'] == 'Cambridge University Library'
            assert credit.props['data-credit-nli'] == search_results.QUICK_VIEW_NLI_CREDIT

    _run(driver)


def test_library_images_that_come_late_redraw_the_image():
    async def driver(user):
        with patch.object(search_results, 'quick_view_external_images', _slow_external), \
                patch.object(search_results, 'QUICK_VIEW_EXTERNAL_WAIT_S', 0.2), \
                patch('shared.nli_circuit_breaker.is_open', return_value=False):
            await user.open(f'{PAGE}?case=hit')
            await _wait_until(lambda: 'adv-zoomable-image' in _html(user) and _version_label(user) == 'V0.8')
            assert _shows(user, f'/api/nli_image_by_sysid/{SID}?page=1'), _html(user)[:500]
            image, chooser = _image_element(user), _version_label(user)
            await _wait_until(lambda: _shows(user, f'/api/cambridge_image/{SID}'))
            assert _shows(user, f'/api/cambridge_image/{SID}?page=1'), _html(user)[:500]
            # Only the image was swapped (Codex review): the dialog was not drawn again, so
            # the edition the reader may have chosen meanwhile stays.
            assert _image_element(user) is image
            assert _version_label(user) == chooser
            assert _credit_label(user).text == 'Cambridge University Library'

    _run(driver)


class _SlowCrossref:
    def is_available(self):
        return True

    def get_crossref_metadata(self, sys_id):
        time.sleep(1.2)
        return None


def _external_after(seconds):
    def lookup(sys_id):
        time.sleep(seconds)
        return _external(sys_id)
    return lookup


def test_the_wait_for_library_images_counts_from_the_lookups_start():
    """Codex on #6: the wait began only after the page and enrichment reads, so their time
    came on top of it. Here the cross-reference read (1.2 s) outlasts the whole wait (1.0 s):
    the dialog is drawn at once, on NLI, and swapped when the images come (1.8 s). Timed
    from the end of the reads, the wait would have lasted until the images came and the
    dialog would have opened on Cambridge."""
    async def driver(user):
        with patch.object(search_results, 'quick_view_external_images', _external_after(1.8)), \
                patch.object(search_results, 'QUICK_VIEW_EXTERNAL_WAIT_S', 1.0), \
                patch('shared.nli_crossref_service.get_nli_crossref_service', return_value=_SlowCrossref()), \
                patch('shared.nli_circuit_breaker.is_open', return_value=False):
            await user.open(f'{PAGE}?case=hit')
            await _wait_until(lambda: 'adv-zoomable-image' in _html(user))
            assert _shows(user, f'/api/nli_image_by_sysid/{SID}?page=1'), _html(user)[:500]
            await _wait_until(lambda: _shows(user, f'/api/cambridge_image/{SID}'))

    _run(driver)


def test_without_library_images_it_stays_on_nli():
    async def driver(user):
        with patch.object(search_results, 'quick_view_external_images',
                          lambda sys_id: {'sys_id': sys_id, 'cambridge_images': []}):
            await user.open(f'{PAGE}?case=hit')
            await _wait_until(lambda: 'adv-zoomable-image' in _html(user))
            assert _shows(user, f'/api/nli_image_by_sysid/{SID}?page=1'), _html(user)[:500]

    _run(driver)
