"""Holding-library links from the desktop viewer and its compact toolbar."""

import re
from types import SimpleNamespace

import pytest
from PyQt6.QtCore import Qt
from PyQt6.QtGui import QDesktopServices
from PyQt6.QtWidgets import QMenu

from desktop import result_dialog
from desktop.viewers import ManuscriptViewerWidget
from desktop.result_dialog import ResultDialog
from desktop.widgets import LibraryLinkButton
from desktop.widgets.overflow_row import populate_menu_from_buttons
from genizah_core import tr

pytestmark = pytest.mark.gui


def test_desktop_does_not_reuse_an_obsolete_oxford_part_or_start_its_thumbnail():
    from PyQt6.QtWidgets import QLabel
    from shared.codicological import CodicologicalManager

    sid = '990053460440205171'
    old = {'marc': {}, 'oxford_part_id': 'MS. Heb. f. 30/4',
           'thumb_url': 'https://hebrew.bodleian.ox.ac.uk/fragments/thumbs/MS_HEB_f_30_19a.jpg'}
    bank = {sid: {'library_code': 'Oxford', 'shelfmark': 'MS heb. f.3/19'}}
    catalogue = CodicologicalManager()
    assert catalogue.load(csv_bank=bank)
    manager = SimpleNamespace(nli_cache={sid: old}, csv_bank=bank, codico_mgr=catalogue)
    assert result_dialog._cached_meta_needs_enrichment(manager, sid, old)
    downloads = []
    dialog = SimpleNamespace(meta_mgr=manager, lbl_thumb=QLabel(), cancel_image_thread=lambda: None,
                             start_download=lambda s, url: downloads.append(url))
    ResultDialog.fetch_image(dialog, sid)
    assert downloads == []


def test_bodleian_links_stay_together_above_the_image_and_follow_the_leaf(monkeypatch):
    site = 'https://hebrew.bodleian.ox.ac.uk/'
    volume = site + 'catalog/volume_75'
    labels = ['19a', '19b', '20a', '20b']
    images = [{'label': label, 'full_url': site + f'fragments/full/MS_HEB_d_50_{label}.jpg'}
              for label in labels]
    part = {'images': images, 'volume_url': volume, 'direct_link': volume + '#part_3'}
    widget = ManuscriptViewerWidget()
    widget.load_images({
        'shelfmark': 'MS heb. d.50/19', 'oxford_part_metadata': part,
        'images_ext': [{'label': img['label'], 'url': img['full_url']} for img in images],
    }, initial_idx=1)
    try:
        links = widget.bodleian_links
        assert links.alignment() == Qt.AlignmentFlag.AlignCenter
        assert links.openExternalLinks()
        assert not links.isHidden()
        assert widget.layout().indexOf(links) + 1 == widget.layout().indexOf(widget.scroll_area)
        assert re.findall(r'<a href="([^"]+)">([^<]+)</a>', links.text()) == [
            (images[0]['full_url'], tr('Recto')), (images[1]['full_url'], tr('Verso')), (volume, tr('Whole volume'))]
        widget.set_page(3)
        assert re.findall(r'href="([^"]+)"', links.text()) == [images[2]['full_url'], images[3]['full_url'], volume]
        assert widget.btn_external.isHidden()

        general = LibraryLinkButton()
        general.set_record({'oxford_part_metadata': part}, shelfmark='MS heb. d.50/19',
                           position=lambda: {'folio_num': 20})
        opened = []
        monkeypatch.setattr(QDesktopServices, 'openUrl', lambda url: opened.append(url.toString()))
        overflow = QMenu()
        assert populate_menu_from_buttons(overflow, [general]) == 1
        menu = overflow.actions()[0].menu()
        menu.aboutToShow.emit()
        for action in menu.actions():
            action.trigger()
        assert opened == [images[2]['full_url'], images[3]['full_url'], volume, part['direct_link']]

        widget.load_images({'oxford_part_metadata': {'volume_url': volume}})
        assert re.findall(r'href="([^"]+)"', links.text()) == [volume]
        widget.load_images({})
        assert links.isHidden() and links.text() == ''
    finally:
        widget._nav_debounce_timer.stop()
        widget.close()


def test_link_only_katz_record_opens_its_item_and_clears_old_menu(monkeypatch):
    url = 'https://digitalcollections.library.upenn.edu/catalog/penn-item'
    button = LibraryLinkButton()
    button.set_record({'oxford_part_metadata': {'volume_url': 'https://hebrew.bodleian.ox.ac.uk/catalog/volume_75'}})
    button.set_record({'library_viewer_url': {'url': url, 'label': 'Katz Center'}})
    opened = []
    monkeypatch.setattr(QDesktopServices, 'openUrl', lambda u: opened.append(u.toString()))
    assert button.text() == tr('Katz Center')
    assert button.menu() is None
    button.click()
    button.set_record({})
    assert button.isHidden()
    button.click()
    assert opened == [url]


def test_composite_katz_record_offers_each_item_by_shelfmark(monkeypatch):
    items = [{'label': f'Halper {number}', 'url': f'https://digitalcollections.library.upenn.edu/items/{number}'}
             for number in (138, 143)]
    button = LibraryLinkButton()
    button.set_record({'katz_component_links': items})
    assert button.text() == tr('Katz Center')
    opened = []
    monkeypatch.setattr(QDesktopServices, 'openUrl', lambda u: opened.append(u.toString()))
    for action, item in zip(button.menu().actions(), items, strict=True):
        assert action.text() == item['label']
        action.trigger()
    assert opened == [item['url'] for item in items]


def test_sparse_katz_transcription_uses_scan_id_on_load_navigation_and_fallback():
    # NLI's first scan is verso. Transcription page 2 belongs to the fourth scan.
    scans = [('1v', '10'), ('1r', '20'), ('2r', '30'), ('2v', '40')]
    nli = [{'label': side, 'fl_id': fl, 'url': f'https://iiif.nli.org.il/FL{fl}'} for side, fl in scans]
    penn = [{**img, 'url': 'https://iiif-images.library.upenn.edu/' + img['fl_id']} for img in nli]
    meta = {'external_provider': 'katz', 'images_ext': penn, 'images_nli': nli}
    widget = ManuscriptViewerWidget()
    dialog = SimpleNamespace(current_sys_id='S', current_fl_id='FL40', current_p_num=2, current_metadata_only=False,
                             meta_mgr=SimpleNamespace(nli_cache={'S': meta}), ms_viewer=widget)
    try:
        ResultDialog.sync_external_view(dialog)
        assert widget.active_list[widget.current_idx] == penn[3]
        widget.set_katz_page(meta, '10', 1)
        widget._on_image_load_failed(widget._load_generation)
        assert widget.active_list[widget.current_idx] == nli[0]
        widget.set_katz_page(meta, '40', 2)
        assert widget.active_list[widget.current_idx] == nli[3]
        widget.set_katz_page(meta, '50', 3)
        assert not widget.images_ext and widget.current_source == 'nli'
        assert widget.active_list[widget.current_idx]['url'].endswith('/FL50')
        widget.set_katz_page(meta, '20', 4)
        assert widget.active_list[widget.current_idx] == penn[1]
        meta['folio_images'] = [{'folio_label': label} for label in ('1r', '1v', '2r', '2v')]
        widget.set_katz_page(meta, None, 2, metadata_only=True)
        assert widget.active_list[widget.current_idx] == penn[0]  # 1v is first in the manifest.
        widget.set_katz_page(meta, None, 2)
        assert widget.current_source == 'nli'  # An unidentified transcription cannot borrow 1v.
    finally:
        widget._nav_debounce_timer.stop()
        widget.close()


def test_katz_image_failure_switches_to_nli_at_the_same_side():
    canvases = [{'label': side, 'url': 'https://iiif-images.library.upenn.edu/' + side}
                for side in ('1r', '1v')]
    nli = [{'label': side, 'url': 'https://iiif.nli.org.il/' + side} for side in ('1r', '1v')]
    widget = ManuscriptViewerWidget()
    widget.load_images({'external_provider': 'katz', 'images_ext': canvases, 'images_nli': nli},
                       initial_idx=1)
    try:
        assert tr('Katz Center') in widget.combo_source.currentText()
        widget._on_image_load_failed(widget._load_generation)
        assert widget.current_source == 'nli'
        assert widget.current_idx == 1
    finally:
        widget._nav_debounce_timer.stop()
        widget.close()
