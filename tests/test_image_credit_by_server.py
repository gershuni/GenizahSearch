# -*- coding: utf-8 -*-
"""An image's credit follows the server it came from, in both apps (owner, 2026-11-23).

* Cambridge's and JTS's own images (CUDL; JTS's through Princeton) name their library alone: Ktiv had no
  part in them. The cache's `attribution` is the catalogue's credit whenever there is one, and it names
  Ktiv ("Cambridge University Library, "Ktiv" Project, The National Library of Israel."): it was the
  credit under Cambridge's own image, and NLI's image got NLI's manifest credit or a bare "NLI".
* NLI's image of the same leaf is Ktiv's photograph: the catalogue's credit, else NLI's manifest's.

The rule is shared/metadata_manager.py's; Browse (web/components/image_resolution.py::image_credits) and
the desktop viewer (desktop/viewers.py::load_images) both take it.
"""
from __future__ import annotations

import pytest

from shared.browse_map_utils import get_library_display
from shared.metadata_manager import nli_image_credit, own_server_image_credit

KTIV = 'Cambridge University Library, "Ktiv" Project, The National Library of Israel.'
JTS_KTIV = ('Courtesy of the Library of the Jewish Theological Seminary, The National Library of Israel. '
            '"Ktiv" Project, The National Library of Israel.')
CANVAS = [{'label': '1r', 'url': 'https://images.lib.cam.ac.uk/iiif/MS-TS-00006-H-00002-00002-000-00001.jp2'}]


def _cambridge(**extra):
    return {'external_provider': 'cambridge', 'images_ext': CANVAS, 'attribution': KTIV,
            'attribution_nli': 'NLI manifest credit', 'marc': {'attribution': KTIV}, **extra}


@pytest.mark.parametrize('lang', ['en', 'he'])
def test_a_librarys_own_image_names_the_library_alone(lang):
    assert own_server_image_credit(_cambridge(), lang=lang) == get_library_display('CUL', short=False, lang=lang)
    jts = {'external_provider': 'jts', 'images_ext': CANVAS, 'attribution': JTS_KTIV}
    assert own_server_image_credit(jts, lang=lang) == get_library_display('JTS', short=False, lang=lang)
    # Cambridge's images whose provider was not written: the library says whose they are
    unwritten = dict(_cambridge(), external_provider='')
    assert own_server_image_credit(unwritten, library_code='CUL', lang=lang) == get_library_display(
        'CUL', short=False, lang=lang)
    assert own_server_image_credit(unwritten, lang=lang) == ''


def test_no_rule_without_the_librarys_images_or_for_libraries_with_credits_of_their_own():
    assert own_server_image_credit(dict(_cambridge(), images_ext=[])) == ''
    assert own_server_image_credit({}) == ''
    for provider in ('manchester', 'katz', 'oxford'):
        assert own_server_image_credit({'external_provider': provider, 'images_ext': CANVAS}) == ''


def test_nlis_image_takes_the_catalogue_credit_else_nlis_manifest():
    assert nli_image_credit(_cambridge()) == KTIV
    assert nli_image_credit(dict(_cambridge(), marc={})) == 'NLI manifest credit'
    assert nli_image_credit({'marc': {'attribution': ''}, 'attribution_nli': None}) == ''
    assert nli_image_credit(None) == ''


def test_browse_takes_the_rule():
    from web.components import image_resolution as ir
    credit, nli = ir.image_credits(_cambridge(), 'CUL', False)
    assert credit in {get_library_display('CUL', short=False, lang=lang) for lang in ('en', 'he')}
    assert nli == KTIV
    # Phase B's first read, before its enrichment says whether there are Cambridge images: the cache's credit
    assert ir.image_credits(_cambridge(), 'CUL', False, own_images=False) == (KTIV, KTIV)


def test_the_first_draws_credit_waits_for_phase_b():
    """Owner, 2026-11-23: NLI's credit showed, then the library's. The first draw's (NLI's default) is out of
    sight, its room kept, until Phase B draws the right one; seen after 8s if Phase B never does."""
    from web.pages import browse
    css = browse.VIEWER_STYLES
    assert '.browse-image-credit.is-pending { visibility: hidden; animation: gs-credit-late 0s linear 8s forwards; }' in css
    assert '@keyframes gs-credit-late { to { visibility: visible; } }' in css


@pytest.mark.gui
def test_the_desktop_viewer_credits_the_image_on_screen(monkeypatch, tmp_path):
    import genizah_core
    from desktop import image_loader, viewers

    monkeypatch.setattr(genizah_core, 'CURRENT_LANG', 'en')
    monkeypatch.setattr(image_loader.Config, 'IMAGE_CACHE_DIR', str(tmp_path))

    class _Loader:
        def __init__(self, url, *a, **k):
            self.url = url

        def __getattr__(self, _name):
            return lambda *a, **k: None

        def isRunning(self):
            return False

    monkeypatch.setattr(viewers, 'ImageLoaderThread', _Loader)
    image_loader.clear_memory()
    w = viewers.ManuscriptViewerWidget()
    try:
        meta = dict(_cambridge(), images_nli=[{'label': '1r', 'url': 'https://iiif.nli.org.il/IIIFv21/FL777',
                                               'fl_id': '777'}])
        w.load_images(meta)
        assert w.current_source != 'nli'
        assert w.lbl_attribution.text() == 'Cambridge University Library'
        w.combo_source.setCurrentIndex(w.combo_source.findData('nli'))
        assert w.current_source == 'nli'
        assert w.lbl_attribution.text() == KTIV
        # no image, no credit (owner, 2026-11-23): a failed image takes it away, the next image brings it back
        w._on_image_load_failed(w._load_generation)
        assert w.lbl_attribution.isHidden()
        from PyQt6.QtGui import QColor, QImage
        image = QImage(20, 20, QImage.Format.Format_RGB32)
        image.fill(QColor(10, 20, 30))
        w.display_image(image)
        assert not w.lbl_attribution.isHidden() and w.lbl_attribution.text() == KTIV
        w.load_images({})                                   # a manuscript with no image at all
        assert w.lbl_attribution.isHidden()
    finally:
        w._nav_debounce_timer.stop()
        w.close()
        image_loader.clear_memory()


@pytest.mark.parametrize('catalogue, shown', [
    (JTS_KTIV, 'Courtesy of the Library of the Jewish Theological Seminary, "Ktiv" Project, '
               'The National Library of Israel.'),
    ('From the collections of The National Library of Russia, The National Library of Israel. "Ktiv" Project, '
     'The National Library of Israel.',
     'From the collections of The National Library of Russia, "Ktiv" Project, The National Library of Israel.'),
    ('כתבי יד מאוסף יד בן צבי, The National Library of Israel. "Ktiv" Project, The National Library of Israel.',
     'כתבי יד מאוסף יד בן צבי, "Ktiv" Project, The National Library of Israel.'),
    (KTIV, KTIV),                                                   # NLI named once already
    ('The National Library of Israel. "Ktiv" Project', 'The National Library of Israel. "Ktiv" Project'),
])
def test_the_catalogue_credit_names_nli_once(catalogue, shown):
    """Owner, 2026-11-23: NLI's catalogue names NLI twice in most of its credits; shown once, as Cambridge's."""
    assert nli_image_credit({'marc': {'attribution': catalogue}}) == shown


def test_quick_views_credit_goes_with_a_failed_image():
    import pathlib
    css = (pathlib.Path(__file__).resolve().parents[1] / 'web/static/common.css').read_text(encoding='utf-8')
    assert '.browse-image-credit[hidden] { display: none !important; }' in css


def test_quick_view_takes_the_rule():
    """Owner, 2026-11-23: Quick View showed the shelfmark as a JTS image's credit (a JTS manifest has no credit:
    its label stood there). It takes the shared rule: the library alone; NLI's image the catalogue's, NLI once."""
    from unittest.mock import patch
    from web.pages import search_results as sr

    class _Mgr:
        nli_cache = {'990053322030205171': {'images_ext': CANVAS, 'external_provider': 'jts', 'attribution': 'ENA 4011.62',
                                            'marc': {'attribution': JTS_KTIV}, 'attribution_nli': ''}}

    with patch('web.components.image_resolution.resolve_external_images',
               return_value={'cambridge_images': CANVAS, 'external_provider': 'jts', 'cambridge_alignment': None}), \
            patch.object(sr, 'state') as st:
        st.meta_mgr = _Mgr()
        ext = sr.quick_view_external_images('990053322030205171')
    own, nli = sr.quick_view_credits(ext, 'JTS')
    assert own in {get_library_display('JTS', short=False, lang=lang) for lang in ('en', 'he')}, own
    assert nli == ('Courtesy of the Library of the Jewish Theological Seminary, "Ktiv" Project, '
                   'The National Library of Israel.')
    # an image set without the new fields (as the render tests stub it): its credit as it came
    assert sr.quick_view_credits({'attribution': 'Cambridge University Library'}, 'CUL') == (
        'Cambridge University Library', '')
