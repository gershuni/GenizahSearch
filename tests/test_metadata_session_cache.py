# -*- coding: utf-8 -*-
"""What MetadataManager keeps for Browse's speed (2026-11-22).

- A library's image list (CUDL, Figgy) is kept in memory: enrich_metadata downloaded it on
  every visit, most of the desktop's 0.8 s return to a manuscript seen a minute before. A
  failed fetch is not kept; answers are copies, so a caller's edit does not reach the cache.
- enriched_recently: a whole answer (its catalogue record came) from this process, within
  the time limit; Browse then shows the entry at once instead of enriching again.
"""
from __future__ import annotations

from unittest.mock import patch

import pytest

from shared import metadata_manager as mm_mod
from shared.metadata_manager import MetadataManager
from tests.test_cudl_identity import SID_318_41, _enrich, _manager, svc  # noqa: F401  (svc: a fixture)

VIEW = "https://cudl.lib.cam.ac.uk/view/MS-TS-00013-J-00019-00011"


def _bare():
    return MetadataManager.__new__(MetadataManager)


def test_a_library_image_list_is_downloaded_once():
    mm = _bare()
    calls = []

    def fetch(self, url):
        calls.append(url)
        return {'canvases': [{'label': '1r', 'url': 'img:1'}], 'metadata': {}, 'attribution': 'CUL'}

    with patch.object(MetadataManager, "_fetch_external_iiif_data", fetch):
        first = mm.fetch_external_iiif_data(VIEW)
        first['canvases'].append({'label': 'mine'})          # a caller's edit
        second = mm.fetch_external_iiif_data(VIEW)
        second['canvases'].append({'label': 'mine too'})     # an edit of an answer from the cache
        third = mm.fetch_external_iiif_data(VIEW)
    assert calls == [VIEW]
    assert second['canvases'][0] == {'label': '1r', 'url': 'img:1'}, "a caller's edit reached the cache"
    assert third['canvases'] == [{'label': '1r', 'url': 'img:1'}], "a caller's edit reached the cache"


def test_a_failed_fetch_is_tried_again():
    mm = _bare()
    calls = []

    def fetch(self, url):
        calls.append(url)
        return {'canvases': [], 'metadata': {}, 'attribution': 'External Library'}

    with patch.object(MetadataManager, "_fetch_external_iiif_data", fetch):
        mm.fetch_external_iiif_data(VIEW)
        mm.fetch_external_iiif_data(VIEW)
    assert len(calls) == 2


def test_the_list_expires_and_the_cache_is_bounded(monkeypatch):
    mm = _bare()
    calls = []
    now = [1000.0]
    monkeypatch.setattr(mm_mod._time, "monotonic", lambda: now[0])

    def fetch(self, url):
        calls.append(url)
        return {'canvases': [{'url': url}], 'metadata': {}, 'attribution': ''}

    with patch.object(MetadataManager, "_fetch_external_iiif_data", fetch):
        mm.fetch_external_iiif_data(VIEW)
        now[0] += mm_mod.EXTERNAL_IIIF_CACHE_TTL + 1
        mm.fetch_external_iiif_data(VIEW)
        assert calls == [VIEW, VIEW], 'an expired list was served'
        monkeypatch.setattr(mm_mod, "EXTERNAL_IIIF_CACHE_MAX", 2)
        for n in range(3):
            mm.fetch_external_iiif_data(f"{VIEW}-{n}")
        assert len(mm._external_iiif_state()[0]) == 2


def test_a_whole_enrichment_counts_as_fresh(svc):  # noqa: F811 -- the fixture imported above
    sid = SID_318_41
    mm = _manager({sid: {"shelfmark": "T-S NS 318.41"}},
                  {sid: {"shelfmark": "T-S NS 318.41", "library_code": "CUL", "title": "",
                         "call_numbers_raw": ["T-S NS 318.41"]}})
    assert not mm.enriched_recently(sid)
    _enrich(mm, sid, svc, marc={'title': 'x'})
    assert mm.enriched_recently(sid)
    assert not mm.enriched_recently(sid, max_age=-1)


def test_an_enrichment_without_its_catalogue_record_is_not_fresh(svc):  # noqa: F811 -- the fixture imported above
    sid = SID_318_41
    mm = _manager({sid: {"shelfmark": "T-S NS 318.41"}},
                  {sid: {"shelfmark": "T-S NS 318.41", "library_code": "CUL", "title": "",
                         "call_numbers_raw": ["T-S NS 318.41"]}})
    _enrich(mm, sid, svc, marc={})                          # the MARC fetch failed
    assert not mm.enriched_recently(sid)


def test_an_enrichment_without_images_is_not_fresh(svc):  # noqa: F811 -- the fixture imported above
    """Codex on #98: NLI's manifest can fail while its MARC record comes; the entry then has
    no images, and must be enriched again on the next visit, not shown empty for hours."""
    sid = SID_318_41
    mm = _manager({sid: {"shelfmark": "T-S NS 318.41"}},
                  {sid: {"shelfmark": "T-S NS 318.41", "library_code": "CUL", "title": "",
                         "call_numbers_raw": ["T-S NS 318.41"]}})
    mm._enriched_times()[sid] = mm_mod._time.monotonic()      # fresh from an earlier visit
    no_images = {"canvases": [], "metadata": {}, "attribution": ""}
    with patch.object(MetadataManager, "fetch_external_iiif_data", lambda self, url: dict(no_images)),          patch.object(MetadataManager, "fetch_marc_data", lambda self, s: {'title': 'x'}),          patch.object(MetadataManager, "fetch_iiif_manifest", lambda self, s, suffix=1: {}),          patch.object(MetadataManager, "get_part_for_folio", lambda self, s: None),          patch.object(mm_mod, "_get_crossref_service", lambda: svc),          patch.object(mm_mod, "_get_fjms_service", lambda: None):
        meta = mm.enrich_metadata(sid)
    assert not (meta.get('images_nli') or meta.get('images_ext'))
    assert not mm.enriched_recently(sid), 'an answer without images counted as fresh'


_FRESH_MARC = {'title': 'x'}
_FRESH_MANIFEST = {'canvas_map': {'1': '1r'}}


@pytest.mark.parametrize("marc, manifest", [
    ({**_FRESH_MARC, 'stale': True, 'fetched_at': 1000.0}, _FRESH_MANIFEST),       # catalogue record old
    (_FRESH_MARC, {**_FRESH_MANIFEST, 'stale': True, 'fetched_at': 900.0}),        # image list old
    ({**_FRESH_MARC, 'stale': True}, _FRESH_MANIFEST),                             # old, its time unknown
], ids=["marc-stale", "manifest-stale", "stale-without-time"])
def test_an_enrichment_served_from_old_copies_is_not_fresh(svc, marc, manifest):  # noqa: F811 -- the fixture imported above
    """Codex on #98: while NLI is down, enrich_metadata serves the stored copies (marked
    stale). Counted as fresh, Browse would keep showing them for hours after NLI is back."""
    sid = SID_318_41
    mm = _manager({sid: {"shelfmark": "T-S NS 318.41"}},
                  {sid: {"shelfmark": "T-S NS 318.41", "library_code": "CUL", "title": "",
                         "call_numbers_raw": ["T-S NS 318.41"]}})
    mm._enriched_times()[sid] = mm_mod._time.monotonic()      # fresh from an earlier visit
    images = {"canvases": [{"label": "1r", "url": "img:1"}], "metadata": {}, "attribution": "CUL"}
    with patch.object(MetadataManager, "fetch_external_iiif_data", lambda self, url: dict(images)), \
         patch.object(MetadataManager, "fetch_marc_data", lambda self, s: dict(marc)), \
         patch.object(MetadataManager, "fetch_iiif_manifest", lambda self, s, suffix=1: dict(manifest)), \
         patch.object(MetadataManager, "get_part_for_folio", lambda self, s: None), \
         patch.object(mm_mod, "_get_crossref_service", lambda: svc), \
         patch.object(mm_mod, "_get_fjms_service", lambda: None):
        meta = mm.enrich_metadata(sid)
    assert meta.get('images_nli') or meta.get('images_ext')
    assert not mm.enriched_recently(sid), 'an answer from old copies counted as fresh'
    with patch.object(MetadataManager, "fetch_external_iiif_data", lambda self, url: dict(images)), \
         patch.object(MetadataManager, "fetch_marc_data", lambda self, s: dict(_FRESH_MARC)), \
         patch.object(MetadataManager, "fetch_iiif_manifest", lambda self, s, suffix=1: dict(_FRESH_MANIFEST)), \
         patch.object(MetadataManager, "get_part_for_folio", lambda self, s: None), \
         patch.object(mm_mod, "_get_crossref_service", lambda: svc), \
         patch.object(mm_mod, "_get_fjms_service", lambda: None):
        mm.enrich_metadata(sid)                              # NLI answers again
    assert mm.enriched_recently(sid)
