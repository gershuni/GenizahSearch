# -*- coding: utf-8 -*-
"""Quick View shows the image Browse shows first (web/pages/search_results.py::quick_view_image).

Owner report, 2026-11-11: T-S 12.782 (990051340280205171) opened on Cambridge's image in
Browse and on NLI's in Quick View, which built the NLI URL itself. Quick View now asks
``resolve_image_url``, as Browse does, with the library images of
``quick_view_external_images``.
"""
from __future__ import annotations

from unittest.mock import patch

from web.pages.search_results import quick_view_external_images, quick_view_image
from web.services import BrowsePage

SID = '990051340280205171'  # T-S 12.782
OTHER = '990000000000105171'
CUDL = [{'url': 'https://cudl.lib.cam.ac.uk/iiif/MS-TS-00012-00782/canvas/1'},
        {'url': 'https://cudl.lib.cam.ac.uk/iiif/MS-TS-00012-00782/canvas/2'}]
ALIGNED = {'verdict': 'aligned'}


def _page(**kw):
    base = dict(uid='u', p_num=1, text='', full_header='', total_pages=2, current_idx=1, sys_id=SID)
    base.update(kw)
    return BrowsePage(**base)


def _external(sys_id=SID, images=CUDL, alignment=ALIGNED):
    return {'sys_id': sys_id, 'cambridge_images': images, 'external_provider': '',
            'cambridge_alignment': alignment, 'attribution': 'Cambridge University Library'}


def _url(**kw):
    with patch('shared.nli_circuit_breaker.is_open', return_value=False):
        return quick_view_image(SID, kw.pop('page_idx', 0), **kw)


def test_an_aligned_cambridge_manuscript_opens_on_cambridge_as_in_browse():
    got = _url(page=_page(), shelfmark='T-S 12.782', external=_external())
    assert got['active_source'] == 'cambridge'
    assert got['img_url'].startswith(f'/api/cambridge_image/{SID}?page=0')


def test_the_second_side_is_the_second_page():
    got = _url(page_idx=1, page=_page(p_num=2), shelfmark='T-S 12.782', external=_external())
    assert got['img_url'].startswith(f'/api/cambridge_image/{SID}?page=1')


def test_without_library_images_it_is_nli():
    got = _url(page=_page(), shelfmark='T-S 12.782', external=None)
    assert got['active_source'] == 'nli'
    assert got['img_url'].startswith(f'/api/nli_image_by_sysid/{SID}?page=0')


def test_another_manuscripts_library_images_are_not_used():
    # Next/Previous result: the images fetched for the last result never pick this one's.
    got = _url(page=_page(), external=_external(sys_id=OTHER))
    assert got['active_source'] == 'nli'


def test_a_misaligned_cambridge_record_keeps_nli_as_browse_does():
    got = _url(page=_page(), external=_external(alignment={'verdict': 'misaligned'}))
    assert got['active_source'] == 'nli'


def test_a_second_volume_keeps_its_suffix_on_nli():
    got = _url(page=_page(volume_suffix=2), volume_suffix=2, external=None)
    assert '&suffix=2' in got['img_url']


def test_oxford_still_opens_on_the_bodleian_image():
    got = _url(page=_page(shelfmark='MS Heb. e.93/58'), is_oxford=True,
               shelfmark='MS Heb. e.93/58', external=None)
    assert got['active_source'] == 'oxford'
    assert 'nli_image_by_sysid' not in got['img_url']


def test_the_external_images_carry_their_sys_id_and_the_manifest_credit():
    class _Mgr:
        nli_cache = {SID: {'attribution': 'Cambridge University Library, "Ktiv" Project'}}

    with patch('web.components.image_resolution.resolve_external_images',
               return_value={'cambridge_images': CUDL, 'external_provider': '',
                             'cambridge_alignment': ALIGNED}), \
            patch('web.pages.search_results.state') as st:
        st.meta_mgr = _Mgr()
        got = quick_view_external_images(SID)
    assert got['sys_id'] == SID
    assert got['cambridge_images'] == CUDL
    assert got['attribution'].startswith('Cambridge University Library')


def test_a_catalogue_only_record_keeps_quick_views_metadata_only_view():
    # Codex review: Quick View could show its first library image but not page through them.
    synthetic = '990001234560000000'
    with patch('shared.nli_circuit_breaker.is_open', return_value=False):
        got = quick_view_image(synthetic, 0, page=_page(sys_id=synthetic), external=_external(sys_id=synthetic))
    assert got['img_url'] == '' and not got['has_image']


def test_the_img_escapes_its_url_and_marks_a_library_image_for_the_credit_fallback():
    from web.pages.search_results import quick_view_img_html
    tag = quick_view_img_html(f'/api/cambridge_image/{SID}?page=0&_cb=1', SID, 0,
                              is_oxford=False, credit_falls_back_to_nli=True)
    assert f'src="/api/cambridge_image/{SID}?page=0&amp;_cb=1&amp;width=400"' in tag
    assert f'data-full-src="/api/cambridge_image/{SID}?page=0&amp;_cb=1"' in tag
    assert 'data-credit-fallback="nli"' in tag
    assert 'data-credit-fallback' not in quick_view_img_html('/api/nli_image_by_sysid/1?page=0', '1', 0,
                                                             is_oxford=False)


def test_the_viewer_switches_the_credit_for_a_marked_image_on_both_nli_fallbacks():
    from pathlib import Path
    js = (Path(__file__).resolve().parent.parent / 'web' / 'static' / 'manuscript_viewer.js').read_text(encoding='utf-8')
    assert js.count("if (isOxford || img.dataset.creditFallback === 'nli') switchImageCredit('nli');") == 2


def test_the_image_says_unavailable_in_the_readers_language():
    """Owner, 2026-11-23: Quick View said "Image not available" and "Open in Bodleian Libraries" in English in the
    Hebrew interface; handleImageError reads both from the image."""
    from web.pages.search_results import quick_view_img_html
    from web.translations import using_language
    with using_language('he'):
        tag = quick_view_img_html('/api/nli_image_by_sysid/1?page=0', '1', 0, is_oxford=False)
    assert 'data-unavailable-label="תמונה לא זמינה"' in tag
    assert 'data-oxford-direct-label="פתח בספריות הבודליאנה"' in tag
