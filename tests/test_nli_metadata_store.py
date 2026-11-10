# -*- coding: utf-8 -*-
"""The parsed MARC / IIIF-manifest answers kept on disk (shared/nli_metadata_store.py).

PostHog, 2026-11-03: 4,338 NLI circuit-breaker opens in 30 days, and /browse -- the
most-used page -- lost its catalogue details, image labels and (with no other image
source) its images for the whole of each breaker window, because ``fetch_marc_data``
and ``fetch_iiif_manifest`` answered empty whenever NLI could not answer. Now:

* a stored answer younger than the TTL is served with no network call;
* an older one is refreshed, and served marked ``stale`` when NLI fails or the
  breaker is open;
* an error body is never stored; the cap prunes oldest first;
* a synthetic sys_id (Phase 85 D-14) touches neither the store nor the network.

tests/conftest.py gives every test its own (empty) store file.
"""
from __future__ import annotations

import json
import time
from unittest.mock import patch

import pytest
import requests

import shared.nli_circuit_breaker as br
import shared.nli_metadata_store as store
from shared.metadata_manager import MetadataManager

DAY = 24 * 60 * 60
SYS = '990012345670205171'
SYNTHETIC = '990001234560000000'  # the 99...000000 shape shared/synthetic_sys_id.py recognises

MARC_BODY = (
    '<marc:record xmlns:marc="http://www.loc.gov/MARC21/slim">'
    '<marc:datafield tag="581"><marc:subfield code="a">Goitein, Med. Soc. II, 12</marc:subfield></marc:datafield>'
    '<marc:datafield tag="300"><marc:subfield code="a">2 leaves</marc:subfield>'
    '<marc:subfield code="c">18 x 12 cm</marc:subfield></marc:datafield>'
    '<marc:datafield tag="942"><marc:subfield code="z">T-S 1.1</marc:subfield></marc:datafield>'
    '</marc:record>'
)
MANIFEST_BODY = {
    'attribution': 'National Library of Israel',
    'sequences': [{'canvases': [
        {'label': '1r', 'images': [{'resource': {'service': {'@id': 'https://iiif.nli.org.il/IIIFv21/FL1234567'}}}]},
        {'label': '1v', 'images': [{'resource': {'service': {'@id': 'https://iiif.nli.org.il/IIIFv21/FL1234568'}}}]},
    ]}],
}
MARC_PARSED = {
    'bibliography': ['Goitein, Med. Soc. II, 12'], 'notes': [], 'english_title': '',
    'dimensions': '2 leaves | 18 x 12 cm', 'people': [], 'current_owner': '', 'shelfmark_alt': 'T-S 1.1',
    'date': '', 'subjects': [], 'physical_medium': '', 'attribution': '',
    'online_link': None, 'external_iiif_link': None,
}
MANIFEST_PARSED = {
    'physical_desc': 'National Library of Israel', 'attribution': 'National Library of Israel',
    'canvas_map': {'1234567': '1r', '1234568': '1v'},
}


class _Resp:
    def __init__(self, status=200, text='', json_body=None):
        self.status_code = status
        self.text = text
        self.content = text.encode('utf-8')
        self._json = json_body

    def json(self):
        if self._json is None:
            raise ValueError('not JSON')
        return self._json


class _Session:
    """``_make_session`` stand-in: answers ``routes`` by URL substring, counts every call."""

    def __init__(self, answer=None, raises=None):
        self.answer = answer
        self.raises = raises
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append(url)
        if self.raises is not None:
            raise self.raises
        return self.answer


@pytest.fixture
def mm(monkeypatch):
    manager = MetadataManager.__new__(MetadataManager)
    for cache in (MetadataManager._iiif_manifest_cache, MetadataManager._iiif_manifest_fail_cache,
                  MetadataManager._marc_fail_cache):
        cache.clear()
    store._reset_for_tests()
    return manager


def _serve(mm, monkeypatch, session):
    monkeypatch.setattr(mm, '_make_session', lambda: session)
    return session


# ─── the store itself ─────────────────────────────────────────────────────────

class TestStore:
    def test_round_trip_and_ttl(self):
        now = time.time()
        assert store.metadata_store_put('marc', SYS, 1, MARC_PARSED, now=now - 10 * DAY)
        got = store.metadata_store_get('marc', SYS, 1, ttl_seconds=30 * DAY, now=now)
        assert got.payload == MARC_PARSED and got.fresh and got.fetched_at == pytest.approx(now - 10 * DAY)
        old = store.metadata_store_get('marc', SYS, 1, ttl_seconds=30 * DAY, now=now + 21 * DAY)
        assert old.payload == MARC_PARSED and not old.fresh  # kept, just not fresh
        assert store.metadata_store_get('manifest', SYS, 1) is None
        assert store.metadata_store_get('marc', SYS, 2) is None

    def test_an_empty_answer_is_never_stored(self):
        empty_marc = {key: ([] if isinstance(v, list) else ('' if isinstance(v, str) else None))
                      for key, v in MARC_PARSED.items()}
        assert store.metadata_store_put('marc', SYS, 1, empty_marc) is False
        assert store.metadata_store_put('manifest', SYS, 1, {'physical_desc': '', 'canvas_map': {}, 'attribution': ''}) is False
        assert store.metadata_store_put('marc', SYS, 1, 'not a dict') is False
        assert store.metadata_store_get('marc', SYS, 1) is None

    def test_serving_marks_are_not_written_back(self):
        served = store.served_stale(store.StoredAnswer(MARC_PARSED, 1.0, False))
        assert served['stale'] is True and served['fetched_at'] == 1.0
        assert store.metadata_store_put('marc', SYS, 1, served)
        assert store.metadata_store_get('marc', SYS, 1).payload == MARC_PARSED

    def test_the_cap_prunes_oldest_fetched_first(self):
        now = time.time()
        for i in range(5):
            assert store.metadata_store_put('marc', f'99001234567020517{i}', 1, MARC_PARSED,
                                            now=now - (5 - i) * DAY, max_rows=3)
        kept = [i for i in range(5) if store.metadata_store_get('marc', f'99001234567020517{i}', 1) is not None]
        assert kept == [2, 3, 4]
        assert store.stats()['pruned'] == 2 and store.stats()['rows'] == 3

    def test_prepare_brings_an_old_file_under_the_cap(self, monkeypatch):
        now = time.time()
        for i in range(4):
            store.metadata_store_put('manifest', f'99001234567020517{i}', 1, MANIFEST_PARSED, now=now - i, max_rows=0)
        monkeypatch.setattr(store, 'NLI_METADATA_STORE_MAX_ROWS', 2)
        store.metadata_store_prepare()
        assert store.stats()['rows'] == 2
        assert store.metadata_store_get('manifest', '990012345670205170', 1) is not None  # the newest

    def test_synthetic_and_bad_keys_are_refused(self):
        assert store.metadata_store_put('marc', SYNTHETIC, 1, MARC_PARSED) is False
        assert store.metadata_store_get('marc', SYNTHETIC, 1) is None
        assert store.metadata_store_put('other', SYS, 1, MARC_PARSED) is False
        assert store.metadata_store_put('marc', '', 1, MARC_PARSED) is False

    def test_an_empty_setting_turns_it_off(self, monkeypatch):
        monkeypatch.setenv('NLI_METADATA_STORE_FILE', '')
        assert store.metadata_store_path() is None
        assert store.metadata_store_put('marc', SYS, 1, MARC_PARSED)  # nothing to do, not a failure
        assert store.metadata_store_get('marc', SYS, 1) is None

    def test_a_corrupt_row_is_a_miss_not_an_error(self):
        import sqlite3
        store.metadata_store_put('marc', SYS, 1, MARC_PARSED)
        conn = sqlite3.connect(store.metadata_store_path())
        with conn:
            conn.execute('UPDATE nli_metadata SET payload = ?', ('{not json',))
        conn.close()
        assert store.metadata_store_get('marc', SYS, 1) is None
        assert store.stats()['errors'] == 1

    def test_stale_from_is_the_oldest_stale_answer(self):
        fresh = dict(MARC_PARSED)
        a = store.served_stale(store.StoredAnswer(MARC_PARSED, 200.0, False))
        b = store.served_stale(store.StoredAnswer(MANIFEST_PARSED, 100.0, False))
        assert store.stale_from(fresh, a, b) == 100.0
        assert store.stale_from(fresh, {}) is None


# ─── fetch_marc_data / fetch_iiif_manifest over the store ─────────────────────

class TestFetchMarcData:
    def test_a_fresh_row_needs_no_network(self, mm, monkeypatch):
        store.metadata_store_put('marc', SYS, 1, MARC_PARSED)
        session = _serve(mm, monkeypatch, _Session(raises=AssertionError('no network call expected')))
        result = mm.fetch_marc_data(SYS)
        assert result == MARC_PARSED  # no serving marks on a fresh answer
        assert session.calls == []

    def test_a_successful_parse_is_stored_and_answers_the_next_call(self, mm, monkeypatch):
        session = _serve(mm, monkeypatch, _Session(answer=_Resp(200, MARC_BODY)))
        first = mm.fetch_marc_data(SYS)
        assert first['bibliography'] == ['Goitein, Med. Soc. II, 12']
        assert store.metadata_store_get('marc', SYS, 1).payload == first
        second = mm.fetch_marc_data(SYS)
        assert second == first and len(session.calls) == 1

    def test_a_stale_row_is_served_when_the_fetch_times_out(self, mm, monkeypatch):
        store.metadata_store_put('marc', SYS, 1, MARC_PARSED, now=time.time() - 40 * DAY)
        session = _serve(mm, monkeypatch, _Session(raises=requests.exceptions.Timeout('slow')))
        result = mm.fetch_marc_data(SYS)
        assert len(session.calls) == 1  # the refresh was attempted
        assert result['stale'] is True
        assert result['fetched_at'] == pytest.approx(time.time() - 40 * DAY, abs=5)
        assert result['bibliography'] == MARC_PARSED['bibliography']
        assert br._state_snapshot()['consecutive_failures'] == 1  # the failure still counts

    def test_a_stale_row_is_served_when_the_breaker_is_open(self, mm, monkeypatch):
        store.metadata_store_put('marc', SYS, 1, MARC_PARSED, now=time.time() - 40 * DAY)
        for _ in range(br.NLI_CIRCUIT_THRESHOLD):
            br.record_failure('timeout', 'preload')
        assert br.is_open()
        session = _serve(mm, monkeypatch, _Session(raises=AssertionError('breaker open: no network call')))
        result = mm.fetch_marc_data(SYS)
        assert session.calls == []
        assert result['stale'] is True and result['dimensions'] == MARC_PARSED['dimensions']

    def test_a_stale_row_is_served_on_a_5xx(self, mm, monkeypatch):
        store.metadata_store_put('marc', SYS, 1, MARC_PARSED, now=time.time() - 40 * DAY)
        _serve(mm, monkeypatch, _Session(answer=_Resp(503, '<html>Service Unavailable</html>')))
        result = mm.fetch_marc_data(SYS)
        assert result['stale'] is True and result['shelfmark_alt'] == 'T-S 1.1'
        # ...and the 503 body did not replace the stored record
        assert store.metadata_store_get('marc', SYS, 1).payload == MARC_PARSED

    def test_an_error_body_with_status_200_is_not_stored(self, mm, monkeypatch):
        _serve(mm, monkeypatch, _Session(answer=_Resp(200, '<html><body>Error 500</body></html>')))
        result = mm.fetch_marc_data(SYS)
        assert not result.get('stale') and result['bibliography'] == []
        assert store.metadata_store_get('marc', SYS, 1) is None
        # a record that parses but holds nothing is not kept either
        _serve(mm, monkeypatch, _Session(answer=_Resp(200, '<record><leader>x</leader></record>')))
        MetadataManager._marc_fail_cache.clear()
        mm.fetch_marc_data(SYS)
        assert store.metadata_store_get('marc', SYS, 1) is None

    def test_a_synthetic_sys_id_touches_neither_store_nor_network(self, mm, monkeypatch):
        def _no_store(*a, **k):
            raise AssertionError('the store must not be read or written for a synthetic sys_id')
        monkeypatch.setattr(store, 'metadata_store_get', _no_store)
        monkeypatch.setattr(store, 'metadata_store_put', _no_store)
        session = _serve(mm, monkeypatch, _Session(raises=AssertionError('no network for synthetic')))
        result = mm.fetch_marc_data(SYNTHETIC)
        assert session.calls == [] and result['bibliography'] == []
        assert store.stats()['misses'] == 0 and store.stats()['writes'] == 0


class TestFetchIiifManifest:
    def test_a_fresh_row_needs_no_network_and_warms_the_memory_cache(self, mm, monkeypatch):
        store.metadata_store_put('manifest', SYS, 2, MANIFEST_PARSED)
        session = _serve(mm, monkeypatch, _Session(raises=AssertionError('no network call expected')))
        assert mm.fetch_iiif_manifest(SYS, suffix=2) == MANIFEST_PARSED
        assert session.calls == []
        assert MetadataManager._iiif_manifest_cache[(SYS, 2)][0] == MANIFEST_PARSED

    def test_a_successful_parse_is_stored(self, mm, monkeypatch):
        _serve(mm, monkeypatch, _Session(answer=_Resp(200, json.dumps(MANIFEST_BODY), json_body=MANIFEST_BODY)))
        result = mm.fetch_iiif_manifest(SYS)
        assert result['canvas_map'] == {'1234567': '1r', '1234568': '1v'}
        assert store.metadata_store_get('manifest', SYS, 1).payload == result

    def test_a_stale_row_is_served_when_the_breaker_is_open(self, mm, monkeypatch):
        store.metadata_store_put('manifest', SYS, 1, MANIFEST_PARSED, now=time.time() - 40 * DAY)
        for _ in range(br.NLI_CIRCUIT_THRESHOLD):
            br.record_failure('5xx', 'preload')
        session = _serve(mm, monkeypatch, _Session(raises=AssertionError('breaker open: no network call')))
        result = mm.fetch_iiif_manifest(SYS)
        assert session.calls == []
        assert result['stale'] is True and result['canvas_map'] == MANIFEST_PARSED['canvas_map']
        # a stale answer does not sit in the memory cache: the next call asks again once NLI is back
        assert (SYS, 1) not in MetadataManager._iiif_manifest_cache

    def test_a_stale_row_is_served_when_the_fetch_fails(self, mm, monkeypatch):
        store.metadata_store_put('manifest', SYS, 1, MANIFEST_PARSED, now=time.time() - 40 * DAY)
        session = _serve(mm, monkeypatch, _Session(raises=requests.exceptions.ConnectionError('down')))
        result = mm.fetch_iiif_manifest(SYS)
        assert len(session.calls) == 1
        assert result['stale'] is True and result['attribution'] == 'National Library of Israel'

    def test_an_error_body_is_not_stored(self, mm, monkeypatch):
        _serve(mm, monkeypatch, _Session(answer=_Resp(200, '<html>Error</html>')))  # .json() raises
        assert mm.fetch_iiif_manifest(SYS)['canvas_map'] == {}
        assert store.metadata_store_get('manifest', SYS, 1) is None
        MetadataManager._iiif_manifest_fail_cache.clear()
        MetadataManager._iiif_manifest_cache.clear()
        _serve(mm, monkeypatch, _Session(answer=_Resp(200, '{}', json_body={'error': 'not found'})))
        assert mm.fetch_iiif_manifest(SYS)['canvas_map'] == {}
        assert store.metadata_store_get('manifest', SYS, 1) is None

    def test_a_synthetic_sys_id_touches_neither_store_nor_network(self, mm, monkeypatch):
        def _no_store(*a, **k):
            raise AssertionError('the store must not be read or written for a synthetic sys_id')
        monkeypatch.setattr(store, 'metadata_store_get', _no_store)
        monkeypatch.setattr(store, 'metadata_store_put', _no_store)
        session = _serve(mm, monkeypatch, _Session(raises=AssertionError('no network for synthetic')))
        assert mm.fetch_iiif_manifest(SYNTHETIC)['canvas_map'] == {}
        assert session.calls == []


# ─── enrich_metadata tells the page what it served ────────────────────────────

def _enrich(mm, marc, manifest):
    import shared.metadata_manager as mm_mod
    mm.nli_cache = {SYS: {'shelfmark': 'T-S 1.1', 'title': '', 'fl_ids': []}}
    mm.csv_bank = {SYS: {'shelfmark': 'T-S 1.1', 'title': '', 'library_code': 'CUL', 'call_numbers_raw': ['T-S 1.1']}}
    mm._csv_bank_loaded = True
    with patch.object(MetadataManager, 'fetch_marc_data', lambda self, s: dict(marc)), \
         patch.object(MetadataManager, 'fetch_iiif_manifest', lambda self, s, suffix=1: dict(manifest)), \
         patch.object(MetadataManager, 'get_part_for_folio', lambda self, s: None), \
         patch.object(MetadataManager, 'resolve_cambridge',
                      lambda self, s, **k: {'status': 'none', 'label': None, 'viewer_url': None}), \
         patch.object(mm_mod, '_get_crossref_service', lambda: None), \
         patch.object(mm_mod, '_get_fjms_service', lambda: None):
        return mm.enrich_metadata(SYS)


def test_enrich_metadata_records_the_oldest_stale_answer_and_clears_it_when_fresh(mm):
    old_marc = store.served_stale(store.StoredAnswer(MARC_PARSED, 1_700_000_000.0, False))
    meta = _enrich(mm, old_marc, MANIFEST_PARSED)
    assert meta['nli_stale_from'] == 1_700_000_000.0
    assert meta['images_nli'][0]['fl_id'] == '1234567'  # the stale manifest still fills the image list
    older_manifest = store.served_stale(store.StoredAnswer(MANIFEST_PARSED, 1_600_000_000.0, False))
    meta = _enrich(mm, old_marc, older_manifest)
    assert meta['nli_stale_from'] == 1_600_000_000.0
    meta = _enrich(mm, MARC_PARSED, MANIFEST_PARSED)
    assert 'nli_stale_from' not in meta


# ─── the web shows what it served, and counts it ──────────────────────────────

def test_browse_shows_the_stale_note_and_the_web_counts_the_store():
    """Source pins: the enrichment pane carries ``nli_stale_from`` onto the page, /browse
    renders it under the image, the string has a Hebrew translation, and web/api.py
    registers the store's counters and prunes it at startup."""
    import pathlib
    from shared.genizah_translations import TRANSLATIONS
    repo = pathlib.Path(__file__).resolve().parent.parent
    enrichment = (repo / 'web' / 'pages' / 'browse_enrichment.py').read_text(encoding='utf-8')
    browse = (repo / 'web' / 'pages' / 'browse.py').read_text(encoding='utf-8')
    api = (repo / 'web' / 'api.py').read_text(encoding='utf-8')
    assert "result['nli_stale_from'] = _cached_attr['nli_stale_from']" in enrichment
    assert "pg.nli_stale_from = browse_enrich.get('nli_stale_from')" in enrichment
    assert 'if page.nli_stale_from:' in browse
    assert 'data-role="nli-stale-note"' in browse
    assert "tr('Catalogue details from {date}')" in browse
    assert 'Catalogue details from {date}' in TRANSLATIONS
    assert '{date}' in TRANSLATIONS['Catalogue details from {date}']
    assert "_register_runtime_cache_stats('nli_metadata_store', nli_metadata_store.stats)" in api
    assert 'nli_metadata_store.metadata_store_prepare()' in api
    from web.services import BrowsePage
    assert BrowsePage.__dataclass_fields__['nli_stale_from'].default is None


# ─── Codex round 1 on #470 ────────────────────────────────────────────────────

class TestAnEmptyAnswerWithStatus200:
    """P1: a well-formed body with nothing in it must not replace the stored copy, be
    recorded as a success, or sit in the memory cache."""

    def test_marc_stale_row_survives_an_empty_record(self, mm, monkeypatch):
        store.metadata_store_put('marc', SYS, 1, MARC_PARSED, now=time.time() - 40 * DAY)
        br.record_failure('timeout', 'preload')  # one failure on the counter...
        _serve(mm, monkeypatch, _Session(answer=_Resp(200, '<record><leader>x</leader></record>')))
        result = mm.fetch_marc_data(SYS)
        assert result['stale'] is True and result['bibliography'] == MARC_PARSED['bibliography']
        assert store.metadata_store_get('marc', SYS, 1).payload == MARC_PARSED
        assert br._state_snapshot()['consecutive_failures'] == 1  # ...not reset: it was no answer
        assert SYS in MetadataManager._marc_fail_cache

    def test_manifest_stale_row_survives_a_json_error_object(self, mm, monkeypatch):
        store.metadata_store_put('manifest', SYS, 1, MANIFEST_PARSED, now=time.time() - 40 * DAY)
        _serve(mm, monkeypatch, _Session(answer=_Resp(200, '{}', json_body={'error': 'not available'})))
        result = mm.fetch_iiif_manifest(SYS)
        assert result['stale'] is True and result['canvas_map'] == MANIFEST_PARSED['canvas_map']
        assert store.metadata_store_get('manifest', SYS, 1).payload == MANIFEST_PARSED
        assert (SYS, 1) not in MetadataManager._iiif_manifest_cache

    def test_an_empty_manifest_is_not_memory_cached_for_the_life_of_the_process(self, mm, monkeypatch):
        session = _serve(mm, monkeypatch, _Session(answer=_Resp(200, '{}', json_body={'sequences': []})))
        assert mm.fetch_iiif_manifest(SYS)['canvas_map'] == {}
        assert (SYS, 1) not in MetadataManager._iiif_manifest_cache
        assert (SYS, 1) in MetadataManager._iiif_manifest_fail_cache  # held off for _NLI_FAIL_TTL only
        MetadataManager._iiif_manifest_fail_cache.clear()
        mm.fetch_iiif_manifest(SYS)
        assert len(session.calls) == 2


class TestTheMemoryCacheAgesWithTheDiskTtl:
    """P2: the manifest memory cache had no TTL, so a long-running server served one
    manifest for the life of the process and NLI_METADATA_STORE_TTL=0 refreshed nothing."""

    def test_ttl_zero_asks_nli_on_every_call(self, mm, monkeypatch):
        monkeypatch.setattr(store, 'NLI_METADATA_STORE_TTL', 0)
        session = _serve(mm, monkeypatch, _Session(answer=_Resp(200, json.dumps(MANIFEST_BODY), json_body=MANIFEST_BODY)))
        mm.fetch_iiif_manifest(SYS)
        mm.fetch_iiif_manifest(SYS)
        assert len(session.calls) == 2
        # ...and the stored row still serves an outage
        _serve(mm, monkeypatch, _Session(raises=requests.exceptions.Timeout('slow')))
        MetadataManager._iiif_manifest_fail_cache.clear()
        assert mm.fetch_iiif_manifest(SYS)['stale'] is True

    def test_a_memory_entry_expires_with_the_disk_row(self, mm, monkeypatch):
        session = _serve(mm, monkeypatch, _Session(answer=_Resp(200, json.dumps(MANIFEST_BODY), json_body=MANIFEST_BODY)))
        fetched = time.time()
        mm.fetch_iiif_manifest(SYS)
        mm.fetch_iiif_manifest(SYS)
        assert len(session.calls) == 1  # memory
        monkeypatch.setattr(time, 'time', lambda: fetched + 40 * DAY)
        mm.fetch_iiif_manifest(SYS)
        assert len(session.calls) == 2  # past the TTL: asked again

    def test_a_disk_row_brought_into_memory_keeps_its_own_age(self, mm, monkeypatch):
        now = time.time()
        store.metadata_store_put('manifest', SYS, 1, MANIFEST_PARSED, now=now - 25 * DAY)
        session = _serve(mm, monkeypatch, _Session(answer=_Resp(200, json.dumps(MANIFEST_BODY), json_body=MANIFEST_BODY)))
        assert mm.fetch_iiif_manifest(SYS) == MANIFEST_PARSED and session.calls == []
        assert MetadataManager._iiif_manifest_cache[(SYS, 1)][1] == pytest.approx(now - 25 * DAY)
        monkeypatch.setattr(time, 'time', lambda: now + 10 * DAY)  # the row is now 35 days old
        mm.fetch_iiif_manifest(SYS)
        assert len(session.calls) == 1
