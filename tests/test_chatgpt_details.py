"""Stable source pagination, public provenance and bounded background work."""
import asyncio
import json
from pathlib import Path
import threading
import sys
from types import SimpleNamespace

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from fastapi.testclient import TestClient
from httpx import ASGITransport, AsyncClient
import pytest

from shared.api_errors import APIError
from shared.manuscript_details import source_credit_rows
from web import chatgpt_details as details


def app_for(loader, owner=lambda request: 'test', limiter=None):
    app = FastAPI()
    details.register_manuscript_details(app, owner, loader,
                                       limiter or SimpleNamespace(check=lambda owner: None))
    return app


BASE = '/chatgpt/manuscript-details?sys_id=99001&section=fjms_bibliography'


def test_complete_pages_preserve_citations_and_query_once():
    # Other CI lanes collect this module before marker deselection, but only
    # the main test lane installs the schema-validation dependency.
    from jsonschema import Draft202012Validator

    calls = []
    rows = [{'citation': f'Author {i}, title; pages 12–19', 'mention_type': 'Discussion'} for i in range(23)]
    def loader(*args):
        calls.append(args)
        return rows, 'local_records'
    spec = json.loads(Path('integrations/chatgpt/openapi.json').read_text(encoding='utf-8'))
    validator = Draft202012Validator({'$ref': '#/components/schemas/ManuscriptDetails', 'components': spec['components']})
    with TestClient(app_for(loader)) as client:
        body = client.get(BASE).json()
        found = []
        while True:
            validator.validate(body)
            found.extend(body['records'])
            if body['next_offset'] is None:
                break
            body = client.get(BASE + f"&offset={body['next_offset']}&snapshot={body['snapshot']}").json()
    assert found == rows
    assert len(calls) == 1


def test_byte_bound_reduces_page_size_without_cutting_records():
    rows = [{'citation': 'א' * 20000}, {'citation': 'ב' * 20000}]
    with TestClient(app_for(lambda *args: (rows, 'local_records'))) as client:
        first = client.get(BASE)
        assert len(first.content) < 80000
        body = first.json()
        assert body['records'] == rows[:1]
        second = client.get(BASE + f"&offset=1&snapshot={body['snapshot']}").json()
        assert second['records'] == rows[1:] and second['next_offset'] is None


def test_expiry_and_eviction_do_not_silently_mix_pages(monkeypatch):
    monkeypatch.setattr(details, 'MAX_ENTRIES', 1)
    with TestClient(app_for(lambda *args: ([{}] * 11, 'local_records'))) as client:
        first = client.get(BASE).json()
        client.get(BASE.replace('99001', '99002'))
        assert client.get(BASE + f"&offset=10&snapshot={first['snapshot']}").status_code == 409
        assert client.get(BASE + '&offset=10').status_code == 409
        new = client.get(BASE).json()
        monkeypatch.setattr(details, 'TTL', 0)
        assert client.get(BASE + f"&snapshot={new['snapshot']}").status_code == 409


@pytest.mark.parametrize('state', ['not_cached', 'unavailable'])
def test_missing_source_is_explicit(state):
    with TestClient(app_for(lambda *args: ([], state))) as client:
        body = client.get(BASE).json()
    assert body['availability'] == state
    assert state in [w['code'] for w in body['warnings']]


def test_invalid_requests_gate_and_rate_limit_never_load():
    def loader(*args):
        raise AssertionError('Must not load')
    with TestClient(app_for(loader)) as client:
        for suffix in ('&offset=-1', '&unknown=x', '&offset=abc'):
            assert client.get(BASE + suffix).status_code == 400
        assert client.get(BASE.replace('fjms_bibliography', 'invented')).status_code == 400
    with TestClient(app_for(loader, owner=lambda request: JSONResponse({}, status_code=403))) as client:
        assert client.get(BASE).status_code == 403
    def reject(owner):
        raise APIError('rate_limited', 'Slow down', http_status=429, headers={'Retry-After': '12'})
    with TestClient(app_for(loader, limiter=SimpleNamespace(check=reject))) as client:
        response = client.get(BASE)
        assert response.status_code == 429 and response.headers['Retry-After'] == '12'


def test_failure_and_oversize_are_errors(monkeypatch):
    with TestClient(app_for(lambda *args: ([{'citation': 'x' * 71000}], 'local_records'))) as client:
        assert client.get(BASE).json()['error']['code'] == 'details_too_large'
    monkeypatch.setattr(details, 'MAX_SECTION_BYTES', 10)
    with TestClient(app_for(lambda *args: ([{'citation': 'long citation'}], 'local_records'))) as client:
        assert client.get(BASE).status_code == 503
    def failed(*args):
        raise RuntimeError('private diagnostic')
    with TestClient(app_for(failed)) as client:
        response = client.get(BASE)
        assert response.status_code == 503 and 'private diagnostic' not in response.text


def test_timeout_keeps_work_slot_and_coalesces_retries(monkeypatch):
    monkeypatch.setattr(details, 'WAIT_SECONDS', .01)
    monkeypatch.setattr(details, 'MAX_ACTIVE', 1)
    release = threading.Event()
    calls = []
    def loader(*args):
        calls.append(args)
        release.wait(3)
        return [], 'local_records'
    async def scenario():
        async with AsyncClient(transport=ASGITransport(app=app_for(loader)), base_url='http://test') as client:
            try:
                assert (await client.get(BASE)).json()['error']['code'] == 'details_pending'
                assert (await client.get(BASE)).json()['error']['code'] == 'details_pending'
                assert (await client.get(BASE.replace('99001', '99002'))).json()['error']['code'] == 'details_busy'
                assert len(calls) == 1
            finally:
                release.set()
            await asyncio.sleep(.05)
            assert (await client.get(BASE)).status_code == 200
    asyncio.run(scenario())


def test_credits_keep_scholars_and_roles_without_edition_content():
    row = {'source_scholar': 'Scholar, Published edition', 'pgpid': 12,
           'doc_relation': 'Digital Translation', 'source_credit_he': 'קרדיט',
           'content': 'long full edition', 'sections': ['private layout'], 'created_at': 'internal'}
    result = source_credit_rows([row])[0]
    assert result['source_scholar'] == row['source_scholar']
    assert result['doc_relation'] == row['doc_relation']
    assert not {'content', 'sections', 'created_at'} & result.keys()


def test_nli_cache_only_preserves_raw_citation(monkeypatch):
    from shared.manuscript_details import load_section
    marc = {'bibliography': ['Author, title $$g 12–19'], 'notes': ['Catalog note']}
    fake_state = SimpleNamespace(meta_mgr=SimpleNamespace(nli_cache={'99001': {'marc': marc}}))
    monkeypatch.setitem(sys.modules, 'web.state', SimpleNamespace(state=fake_state))
    assert details.load_web_section('99001', 'nli_bibliography') == ([{'citation': marc['bibliography'][0]}], 'local_cache')
    assert details.load_web_section('99001', 'nli_catalog')[0] == [{'field': 'notes', 'value': ['Catalog note']}]
    assert details.load_web_section('missing', 'nli_catalog') == ([], 'not_cached')
    assert load_section('99001', 'nli_catalog') == ([], 'not_cached')
    assert load_section('missing', 'nli_catalog') == ([], 'not_cached')


def test_catalog_records_keep_category_and_record_associations(monkeypatch):
    from shared.manuscript_details import load_section
    service = SimpleNamespace(is_available=lambda: True, get_catalog_detail=lambda sid: {
        'records': [{'title': 'Work', 'source_name': 'Cataloguer', 'unit_catalog_rec_id': 42}],
        'fields': {42: {'Material': [{'value': 'paper'}]}},
    })
    monkeypatch.setitem(sys.modules, 'shared.fjms_service', SimpleNamespace(get_fjms_service=lambda **kw: service))
    rows, state = load_section('99001', 'fjms_catalog')
    assert state == 'local_records'
    assert rows[0]['data']['source_name'] == 'Cataloguer'
    assert rows[1] == {'category': 'fields', 'unit_catalog_rec_id': 42, 'data': {'Material': [{'value': 'paper'}]}}


def test_selected_pgp_text_credit_reaches_browse_metadata(monkeypatch):
    from shared.browse_service import _pgp_sync
    from shared.search_serializer import _build_pgp_subset
    doc = {'pgpid': 123, 'transcription_source': 'Scholar, edition, pages 5–8',
           'doc_relation': 'Digital Edition'}
    monkeypatch.setitem(sys.modules, 'shared.document_service', SimpleNamespace(
        get_document_for_fragment=lambda *args: doc, get_section_for_page=lambda *args, **kwargs: None))
    metadata = _build_pgp_subset(_pgp_sync('99001', 1))
    assert metadata['transcription_source'] == doc['transcription_source']
    assert metadata['doc_relation'] == doc['doc_relation']
    assert metadata['pgpid'] == 123


def test_disabled_fgp_does_not_load_sources(monkeypatch):
    from shared.manuscript_details import load_section
    def forbidden(**kwargs):
        raise AssertionError('Disabled provider must not be loaded')
    monkeypatch.setitem(sys.modules, 'shared.fgp_service', SimpleNamespace(
        _fgp_enabled=lambda: False, get_fgp_service=forbidden))
    assert load_section('99001', 'fgp_sources') == ([], 'unavailable')
