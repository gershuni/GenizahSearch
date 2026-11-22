# -*- coding: utf-8 -*-
"""The Reading Desk's links beside each image (web/pages/browse_desk_links.py).

``fragment_links`` over fake catalogues: a Bodleian leaf's own links on its image and the
fragment's after them (also with no pages known, and under PGP's name for the shelfmark);
Cambridge's CUDL item once; the Katz Center's components and viewer, with no NLI sidecar; each
side's PGP document when the recto and verso are in two; a failing source; a catalogue-only
record. ``_resolve``: one lookup per fragment, none kept when a source failed. The menu in the
desk itself: tests/render_smoke/test_reading_desk_document_render_smoke.py.
"""
from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from web.pages import browse_desk_links as dl
from web.services import DocumentPage

OX = '990000000010205171'
CUL = '990000000020205171'
KATZ = '990000000030205171'
PGP = {'pgp_url': 'https://geniza.princeton.edu/documents/1/'}
PGP_LINK = (('Princeton Geniza Project',), PGP['pgp_url'])


def _entry(sys_id, shelfmark, pages=2, pgp=None):
    return {'sys_id': sys_id, 'shelfmark': shelfmark, 'pgp_doc': pgp or {},
            'pages': [DocumentPage(uid=f'{sys_id}-{n}', p_num=n, text='', full_header='', sys_id=sys_id)
                      for n in range(1, pages + 1)]}


def _meta(library, part=None, shelfmark=''):
    meta = MagicMock()
    meta.get_library_for_id.return_value = library
    meta.get_meta_for_id.return_value = (shelfmark, '')
    meta.get_part_for_folio.return_value = 'part-1' if part else None
    meta.get_part_metadata.return_value = part
    return meta


def _crossref(viewer=None, folio_images=()):
    svc = MagicMock()
    svc.is_available.return_value = True
    svc.get_folio_images.return_value = list(folio_images)
    svc.get_library_viewer_url.return_value = viewer
    return svc


def _no_pgp(sys_id, page_num=None):
    return None


LEAF = {'recto': 'https://digital.bodleian/f1a.jpg', 'verso': 'https://digital.bodleian/f1b.jpg',
        'volume': 'https://digital.bodleian/catalog/volume_9'}


def _bodleian(seen):
    def links_for(page, codico, folio_images, part_meta):
        seen.append((page.p_num, page.total_pages, page.shelfmark, folio_images, part_meta))
        return dict(LEAF)
    return links_for


def test_a_bodleian_leaf_has_its_own_links_on_its_image_and_the_fragments_after():
    seen = []
    part = {'direct_link': 'https://digital.bodleian/part_3'}
    with patch('web.services.bodleian_links_for_page', side_effect=_bodleian(seen)), \
            patch('shared.document_service.get_document_for_fragment', side_effect=_no_pgp):
        found = dl.fragment_links(_entry(OX, 'MS. Heb. c. 13/1', pgp=PGP), _meta('Oxford', part, 'MS. Heb. c. 13/1'),
                                  _crossref(folio_images=[{'folio_label': '1r'}]))
    # Each page asked with the leaf's own number, the page count and the catalogue's images.
    assert [s[:3] for s in seen] == [(1, 2, 'MS. Heb. c. 13/1'), (2, 2, 'MS. Heb. c. 13/1')]
    assert seen[0][3] == [{'folio_label': '1r'}] and seen[0][4] is part
    assert found['pages'][0] == [(('Bodleian', 'Recto'), LEAF['recto']), (('Bodleian', 'Verso'), LEAF['verso'])]
    assert found['pages'][1] == found['pages'][0]
    assert found['fragment'] == [
        (('NLI Ktiv',), dl.KTIV_URL.format(OX)),
        (('Bodleian part',), part['direct_link']),
        (('Bodleian', 'Whole volume'), LEAF['volume']),
        PGP_LINK,
    ]
    assert found['complete'] is True
    # One image's menu: its leaf first, then the fragment's.
    assert dl.image_links(found, 0)[:2] == found['pages'][0] and dl.image_links(found, 5) == found['fragment']


def test_a_bodleian_fragment_with_no_pages_known_or_named_as_pgp_names_it_keeps_its_links():
    # No pages known: the desk shows the two sides of one leaf, and so do the links (Codex CLI, #85).
    seen = []
    with patch('web.services.bodleian_links_for_page', side_effect=_bodleian(seen)), \
            patch('shared.document_service.get_document_for_fragment', side_effect=_no_pgp):
        found = dl.fragment_links(_entry(OX, 'Bodl. MS heb. c 13/1', pages=0), _meta('Oxford', None, 'MS. Heb. c. 13/1'),
                                  _crossref())
    assert [s[:3] for s in seen] == [(1, 2, 'MS. Heb. c. 13/1'), (2, 2, 'MS. Heb. c. 13/1')]  # the catalogue's name
    assert set(found['pages']) == {0, 1} and (('Bodleian', 'Whole volume'), LEAF['volume']) in found['fragment']
    # No catalogue name to be had: PGP's prefix is taken off, as the desk's images take it off.
    seen.clear()
    with patch('web.services.bodleian_links_for_page', side_effect=_bodleian(seen)), \
            patch('shared.document_service.get_document_for_fragment', side_effect=_no_pgp):
        dl.fragment_links(_entry(OX, 'Bodl. MS heb. c 13/1'), _meta('Oxford'), _crossref())
    assert [s[2] for s in seen] == ['MS heb. c 13/1', 'MS heb. c 13/1']


def test_cambridge_names_its_cudl_item_once_and_katz_its_components_without_the_sidecar():
    cudl = 'https://cudl.lib.cam.ac.uk/view/MS-TS-00012-00001'
    crossref = _crossref(viewer={'url': cudl, 'label': 'Cambridge Digital Library'})
    with patch('shared.document_service.get_document_for_fragment', side_effect=_no_pgp):
        found = dl.fragment_links(_entry(CUL, 'T-S 12.1'), _meta('CUL'), crossref, cambridge_url=cudl)
    assert found['fragment'] == [(('NLI Ktiv',), dl.KTIV_URL.format(CUL)), (('Cambridge CUDL',), cudl)]
    crossref.get_library_viewer_url.assert_called_once_with(CUL, cambridge_viewer_url=cudl)
    assert found['pages'] == {}

    # The Katz Center's links come from the bundled references: no NLI sidecar needed (Codex CLI, #85).
    components = [{'label': 'Part 1', 'url': 'https://katz/1'}, {'label': 'Part 2', 'url': 'https://katz/2'}]
    with patch('shared.nli_crossref_service.katz_component_links', return_value=components), \
            patch('shared.nli_crossref_service.katz_viewer_url', return_value={'url': 'https://katz/item', 'label': 'Katz Center'}), \
            patch('shared.document_service.get_document_for_fragment', side_effect=_no_pgp):
        found = dl.fragment_links(_entry(KATZ, 'Katz 5'), _meta('Katz'), None)
    assert found['fragment'] == [(('NLI Ktiv',), dl.KTIV_URL.format(KATZ)),
                                 (('Katz Center', 'Part 1'), 'https://katz/1'),
                                 (('Katz Center', 'Part 2'), 'https://katz/2'),
                                 (('Katz Center',), 'https://katz/item')]


def test_each_side_has_its_own_pgp_document_when_recto_and_verso_are_in_two():
    docs = {1: {'pgp_url': 'https://geniza.princeton.edu/documents/10/'},
            2: {'pgp_url': 'https://geniza.princeton.edu/documents/20/'}}
    with patch('shared.document_service.get_document_for_fragment', side_effect=lambda sid, page_num=None: docs[page_num]):
        found = dl.fragment_links(_entry(CUL, 'T-S 12.1', pgp={'pgp_url': docs[1]['pgp_url']}), _meta('CUL'), None)
    assert found['pages'] == {0: [(('Princeton Geniza Project',), docs[1]['pgp_url'])],
                              1: [(('Princeton Geniza Project',), docs[2]['pgp_url'])]}
    assert found['fragment'] == [(('NLI Ktiv',), dl.KTIV_URL.format(CUL))]  # not the fragment's, on both
    # One document for both sides: the fragment's link, once.
    with patch('shared.document_service.get_document_for_fragment', side_effect=lambda sid, page_num=None: docs[1]):
        found = dl.fragment_links(_entry(CUL, 'T-S 12.1'), _meta('CUL'), None)
    assert found['pages'] == {} and found['fragment'][-1] == (('Princeton Geniza Project',), docs[1]['pgp_url'])


def test_one_source_failing_leaves_the_others_and_a_catalogue_only_record_has_pgp_alone():
    crossref = _crossref()
    crossref.get_library_viewer_url.side_effect = RuntimeError('sidecar locked')
    with patch('shared.document_service.get_document_for_fragment', side_effect=_no_pgp):
        found = dl.fragment_links(_entry(CUL, 'T-S 12.1', pgp=PGP), _meta('CUL'), crossref)
    assert found['fragment'] == [(('NLI Ktiv',), dl.KTIV_URL.format(CUL)), PGP_LINK]
    assert found['complete'] is False  # so it is not kept
    with patch.object(dl, 'is_synthetic_sys_id', return_value=True):
        found = dl.fragment_links({'sys_id': 'S1', 'shelfmark': 'X', 'pgp_doc': PGP}, _meta('CUL'), _crossref())
        assert found == {'fragment': [PGP_LINK], 'pages': {}, 'complete': True}
        # Nothing at NLI, but its Cambridge item when the record's decision names one (Codex CLI, #85).
        cudl = 'https://cudl.lib.cam.ac.uk/view/MS-TS-K-00025-00240'
        found = dl.fragment_links({'sys_id': 'S1', 'shelfmark': 'X'}, _meta('CUL'), _crossref(), cambridge_url=cudl)
        assert found['fragment'] == [(('Cambridge CUDL',), cudl)]


def test_the_links_are_looked_up_once_per_fragment_and_again_when_a_source_failed():
    dl._CACHE.clear()
    calls = []
    complete = {'value': True}

    def links(entry, *args):
        calls.append(entry['sys_id'])
        return {'fragment': [], 'pages': {}, 'complete': complete['value']}

    from shared.cudl_identity import RESOLVER_REVISION
    decision = {'status': 'no_match', 'revision': RESOLVER_REVISION}
    with patch.object(dl, 'fragment_links', side_effect=links), \
            patch('web.state.state', SimpleNamespace(meta_mgr=MagicMock())), \
            patch('web.components.image_resolution.resolve_external_images',
                  side_effect=lambda sys_id, meta_mgr=None: {'cambridge_resolution': dict(decision)}):
        dl._resolve({'sys_id': CUL})
        dl._resolve({'sys_id': CUL})
        assert calls == [CUL]  # kept
        # ... for a while: a service that failed without saying so is asked again (GitHub Codex, #85).
        with patch.object(dl.time, 'monotonic', return_value=dl.time.monotonic() + dl._CACHE_TTL + 1):
            dl._resolve({'sys_id': CUL})
        assert calls == [CUL, CUL]
        calls.remove(CUL)
        complete['value'] = False
        dl._resolve({'sys_id': KATZ})
        dl._resolve({'sys_id': KATZ})
        assert calls == [CUL, KATZ, KATZ]  # a failed source: asked again (Codex CLI, #85)
        complete['value'] = True
        decision.update(status='unavailable', revision=None)  # NLI could not be asked yet: not final
        dl._resolve({'sys_id': OX})
        assert dl._resolve({'sys_id': OX})['complete'] is False  # the menu then asks again too
        assert calls == [CUL, KATZ, KATZ, OX, OX]
        # No decision at all: the record's enrichment failed (GitHub Codex, #85). Not final either.
        decision.clear()
        with patch('web.components.image_resolution.resolve_external_images',
                   side_effect=lambda sys_id, meta_mgr=None: {'cambridge_resolution': None}):
            dl._resolve({'sys_id': '990000000040205171'})
            dl._resolve({'sys_id': '990000000040205171'})
        assert calls[-2:] == ['990000000040205171', '990000000040205171']
        # A match made without the crossref (no revision) is shown but not kept (Codex CLI, #85).
        provisional = {'status': 'match', 'viewer_url': 'https://cudl/x', 'revision': None}
        with patch('web.components.image_resolution.resolve_external_images',
                   side_effect=lambda sys_id, meta_mgr=None: {'cambridge_resolution': dict(provisional)}):
            dl._resolve({'sys_id': '990000000050205171'})
            dl._resolve({'sys_id': '990000000050205171'})
        assert calls[-2:] == ['990000000050205171', '990000000050205171']
        # A catalogue-only record's decision is asked too.
        with patch.object(dl, 'is_synthetic_sys_id', return_value=True), \
                patch('web.components.image_resolution.resolve_external_images',
                      side_effect=lambda sys_id, meta_mgr=None: {'cambridge_resolution': {
                          'status': 'match', 'viewer_url': 'https://cudl/s', 'revision': RESOLVER_REVISION}}) as asked:
            dl._resolve({'sys_id': 'S9'})
        assert asked.call_count == 1
    dl._CACHE.clear()
