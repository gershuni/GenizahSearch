# -*- coding: utf-8 -*-
"""The shared parts of the shelf picker's round 5: the extra large size, the details pane's kept
choice, and a manuscript's images at a width (both apps)."""
from __future__ import annotations

from shared.genizah_translations import TRANSLATIONS
from shared.shelfmark_tree_details import image_index, page_image_urls, sized_image_url, thumbnail_url
from shared.shelfmark_tree_rows import DETAILS_KEY, SIZE_LABELS, SIZES, STORAGE_KEY, details_shown, normal_size


def test_extra_large_is_a_size_with_its_words():
    assert SIZES == ('s', 'm', 'l', 'xl')
    assert normal_size('xl') == 'xl' and SIZE_LABELS['xl'] == 'Extra large'
    assert TRANSLATIONS['Extra large']


def test_the_pane_is_shown_unless_hidden_was_kept():
    assert DETAILS_KEY != STORAGE_KEY
    assert details_shown(None) and details_shown(True) and details_shown('x')
    assert not details_shown(False)


def test_an_nli_thumbnail_asks_for_the_width_wanted():
    url = 'https://iiif.nli.org.il/IIIFv21/FL123/full/400,/0/default.jpg'
    assert sized_image_url(url, 800) == 'https://iiif.nli.org.il/IIIFv21/FL123/full/800,/0/default.jpg'
    assert sized_image_url(url, 400) == url
    assert sized_image_url('https://other/x.jpg', 800) == 'https://other/x.jpg'
    assert sized_image_url('', 800) == ''

    class Meta:
        def get_thumbnail(self, sid, size=320):
            return url
    assert thumbnail_url('99', Meta(), width=600).endswith('/full/600,/0/default.jpg')
    assert thumbnail_url('99', Meta()) == url


def test_a_manuscript_s_images_in_order_at_a_width():
    class Meta:
        def fetch_volume_manifest(self, sid, suffix):
            assert suffix == 1
            return {'images_nli': [{'url': 'https://iiif/FL1'}, {'url': 'https://iiif/FL2'}, {'label': 'none'}]}

    class Broken:
        def fetch_volume_manifest(self, sid, suffix):
            raise RuntimeError('NLI down')
    assert page_image_urls('99', Meta(), width=500) == ['https://iiif/FL1/full/500,/0/default.jpg',
                                                        'https://iiif/FL2/full/500,/0/default.jpg']
    assert page_image_urls('99', Broken()) == [] and page_image_urls('99', None) == []
    assert page_image_urls('', Meta()) == []


def test_an_image_is_found_in_the_list_by_its_nli_id_whatever_its_width():
    urls = ['https://iiif/FL11/full/400,/0/default.jpg', 'https://iiif/FL12/full/400,/0/default.jpg']
    assert image_index(urls, 'https://iiif/FL12/full/800,/0/default.jpg') == 1
    assert image_index(urls, 'https://iiif/FL1/full/400,/0/default.jpg') is None     # not a prefix match
    assert image_index(urls, '') is None and image_index([], 'https://iiif/FL11') is None


def test_a_microfilm_opens_on_its_middle_image_past_the_start_target_and_the_card():
    # owner, 2026-11-25: the National Library of Russia's images begin with a START frame and a card
    from shared.shelfmark_tree_details import opening_image
    assert opening_image('RNL', 29) == 14 and opening_image('rnl ', 29) == 14
    assert opening_image('RNL', 3) == 2 and opening_image('RNL', 4) == 2 and opening_image('RNL', 5) == 2
    assert opening_image('RNL', 2) == 0 and opening_image('RNL', 1) == 0 and opening_image('RNL', 0) == 0
    assert opening_image('CUL', 29) == 0 and opening_image('', 29) == 0
