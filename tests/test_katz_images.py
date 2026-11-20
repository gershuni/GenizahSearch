"""A Penn item may be linkable even when its scans cannot be aligned to NLI."""

import csv
import sqlite3

import pytest

from scripts.import_katz_images import build_rows
from shared import metadata_manager, nli_crossref_service
from shared.cudl_identity import fresh_external_entry
from shared.nli_crossref_service import (
    NliCrossrefService, katz_cache_needs_refresh, katz_canvases, katz_component_links, katz_reference_revision,
    katz_catalogue_page_fl, katz_viewer_url,
)


HALPER_77 = '990001664240205171'
HALPER_384 = '990053958400205171'
PENN_ITEM = 'https://digitalcollections.library.upenn.edu/items/cc12404a-b4b2-4c70-9a2b-310aacb92d46'
VERSO = 'https://iiif-images.library.upenn.edu/iiif/3/829bba2f-cc46-48c5-837d-8ec9e93a6321'
RECTO = 'https://iiif-images.library.upenn.edu/iiif/3/733395f6-6ebe-4f1d-b32b-60180b41a1c9'


def test_link_without_an_installed_nli_sidecar(tmp_path):
    service = NliCrossrefService(str(tmp_path / 'absent.db'))
    assert service.get_library_viewer_url(HALPER_77)['url'] == PENN_ITEM
    assert katz_viewer_url('997008595470105171') is None  # Penn Ms. 275 is not Halper 275.
    assert katz_viewer_url('990001932560205171') is None  # File h001 is on Penn's Halper 12a.
    assert katz_viewer_url('990001932430205171') is None  # Halper 12a and 12b are different leaves.
    assert katz_viewer_url('990053959180205171') is None  # RAR MS GF 60 is not Halper 60.
    assert katz_cache_needs_refresh(HALPER_77, {})
    assert not katz_cache_needs_refresh(HALPER_77, {'katz_checked': katz_reference_revision(HALPER_77)})
    assert katz_cache_needs_refresh('990001932560205171', {'katz_checked': 'withdrawn'})
    assert not katz_cache_needs_refresh('other', {})


def pages(*labels):
    return [{'fl_id': str(i + 1), 'label': label} for i, label in enumerate(labels)]


def test_images_follow_unique_nli_folio_labels_even_if_out_of_order():
    images = katz_canvases(HALPER_77, pages('Frag. 001v', 'Frag. 001r'))
    assert [(i['label'], i['url'], i['fl_id']) for i in images] == [('1v', VERSO, '1'), ('1r', RECTO, '2')]


def test_catalogue_only_pages_require_consistent_side_labels():
    scans = katz_canvases(HALPER_77, pages('1v', '1r'))
    def fl(labels, p_num):
        return katz_catalogue_page_fl(scans, [{'folio_label': label} for label in labels], p_num)

    assert fl(['1r', '1v'], 1) == '2'
    assert fl(['1r', '1v'], 2) == '1'
    assert fl([], 1) == '1'  # Without local labels the manifest supplies the page order.
    assert fl(['1r', '1r'], 1) is None
    assert fl(['1r'], 1) is None
    assert fl(['2r', '1v'], 1) is None
    assert fl([], 0) is None
    assert fl([], 3) is None


@pytest.mark.parametrize('labels', [(), ('1r',), ('1r', '1r'), ('1r', '2v'), ('p. 1', 'p. 2'),
                                    ('1r', '1v', 'Cover'), ('1r', None)])
def test_absent_or_ambiguous_sides_keep_only_the_library_link(labels):
    assert katz_canvases(HALPER_77, pages(*labels)) == []
    assert katz_viewer_url(HALPER_77)['url'] == PENN_ITEM


def test_multiple_nli_editions_keep_the_link():
    assert katz_canvases(HALPER_384, pages('1r', '1v')) == []
    assert katz_viewer_url(HALPER_384)['url'].endswith('/c5b4c161-1e9c-4915-9678-4871c7b63623')


@pytest.mark.parametrize(('sys_id', 'labels'), [
    ('990001935630205171', ['Halper 138', 'Halper 143']),
])
def test_composite_records_offer_separate_items(sys_id, labels):
    links = katz_component_links(sys_id)
    assert [link['label'] for link in links] == labels
    assert len({link['url'] for link in links}) == 2
    assert katz_viewer_url(sys_id) is None
    assert katz_cache_needs_refresh(sys_id, {})
    assert katz_canvases(sys_id, pages('1r', '1v')) == []


def test_extra_crossref_shelfmark_cannot_override_the_displayed_catalogue():
    sid = '990053958370205171'  # NLI crossref also has Halper 380; catalogue only has 381.
    assert katz_viewer_url(sid)['url'].endswith('/38e3ce1b-6dae-4c33-aa4a-4adeecb022bf')
    assert katz_component_links(sid) == []
    assert katz_canvases(sid, pages('1r', '1v')) == []


def test_upgrade_cached_metadata_and_drop_images_if_alignment_changes(monkeypatch):
    manager = metadata_manager.MetadataManager.__new__(metadata_manager.MetadataManager)
    manager.nli_cache = {HALPER_77: {'images_ext': [{'url': 'old'}]}}
    manager.csv_bank = {HALPER_77: {'shelfmark': 'Halper 77', 'library_code': 'Katz'}}
    monkeypatch.setattr(manager, 'resolve_cambridge', lambda *a, **k: {'status': 'none'})
    monkeypatch.setattr(manager, 'get_part_for_folio', lambda *a: None)
    monkeypatch.setattr(metadata_manager, '_get_fjms_service', lambda: None)
    monkeypatch.setattr(metadata_manager, '_get_crossref_service', lambda: None)
    monkeypatch.setattr(manager, 'fetch_marc_data', lambda s: {'attribution': 'MARC'})
    manifest = {'canvas_map': {'2': 'Frag. 001v', '1': 'Frag. 001r'}, 'attribution': 'NLI'}
    monkeypatch.setattr(manager, 'fetch_iiif_manifest', lambda *a: manifest)
    meta = fresh_external_entry(manager, HALPER_77)
    assert meta['external_provider'] == 'katz'
    assert meta['external_url'] == meta['library_viewer_url']['url'] == PENN_ITEM
    assert [i['url'] for i in meta['images_ext']] == [RECTO, VERSO]
    assert meta['attribution'] == 'Provided by the University of Pennsylvania Libraries.'
    assert meta['attribution_nli'] == 'NLI'
    manifest['canvas_map'] = {'1': 'frag', '2': 'frag'}
    refreshed = manager.enrich_metadata(HALPER_77)
    assert refreshed['images_ext'] == []
    assert refreshed['images'] == refreshed['images_nli']
    assert 'external_provider' not in refreshed
    assert refreshed['external_url'] == PENN_ITEM
    assert refreshed['attribution'] == 'MARC'
    monkeypatch.setattr(nli_crossref_service, '_katz_references', lambda: ({}, {}, None))
    assert katz_cache_needs_refresh(HALPER_77, refreshed)
    withdrawn = manager.enrich_metadata(HALPER_77)
    assert not {'katz_checked', 'external_url', 'library_viewer_url'} & withdrawn.keys()


def test_import_collisions_compound_records_and_missing_filenames(tmp_path):
    db = tmp_path / 'crossref.db'
    with sqlite3.connect(db) as conn:
        conn.execute('CREATE TABLE nli_images (NLI_AlmaId, Shelfmark, ImageSourceName, ImageName, LibraryAbbrev)')
        conn.executemany("INSERT INTO nli_images VALUES (?, ?, ?, ?, 'Penn CAJS')", [
            ('one', 'Halper 1', 'original', 'H__L1F0B0S1'),
            ('multi', 'Halper 1', 'original', 'H__L1F0B0S1'),
            ('multi', 'Halper 2', 'other', 'H__L1F0B0S2'),
            ('unknown-part', 'Halper 1', 'original', 'H__L1F0B0S1'),
            ('unknown-part', 'other fragment', '', ''),
            ('holding', 'Halper 2', 'original', 'H__L1F0B0S2'),  # File belongs to item A.
            ('wrong-holding', 'Halper 12', 'original', 'H__L1F0B0S2'),
            ('duplicates', 'Halper 2', 'other', 'H__L1F0B0S2'),
            ('duplicates', 'Halper 2', 'extra', 'H__L1F0B0S2'),
        ])
    records = [
        {'id': 'A', 'images': [('original', 'a')], 'locations': ['Library at the Katz Center, Halper 1']},
        {'id': 'B', 'images': [('other', 'b'), ('extra', 'c')], 'locations': ['Library at the Katz Center, Halper 2']},
    ]
    libs = tmp_path / 'libs.csv'
    with libs.open('w', newline='') as out:
        writer = csv.writer(out)
        writer.writerow(('system_number', 'library_code', 'call_numbers'))
        writer.writerows([
            ('one', 'Katz', 'Ms. Genizah 1'), ('multi', 'Katz', 'Halper 1 | Halper 2'),
            ('unknown-part', 'Katz', 'Halper 1 | Halper 3'), ('holding', 'Katz', 'Halper 2'),
            ('duplicates', 'Katz', 'Halper 2'), ('wrong-holding', 'Katz', 'Ms. Genizah 12'),
            ('catalogue-only', 'Katz', 'Ms. Genizah 2'), ('other-library', 'UPenn', 'Ms. Genizah 1'),
        ])
    assert build_rows(records, db, libs) == [
        ('catalogue-only', 'B', '', '', ''), ('duplicates', 'B', '', '', ''), ('holding', 'B', '', '', ''),
        ('multi', 'A', '', '', 'Halper 1'), ('multi', 'B', '', '', 'Halper 2'),
        ('one', 'A', '1r', 'a', '')]
    records.append({'id': 'C', 'images': [('original', 'd')], 'locations': ['Library at the Katz Center, Halper 1']})
    assert {row[0] for row in build_rows(records, db, libs)} == {'catalogue-only', 'duplicates', 'holding'}
