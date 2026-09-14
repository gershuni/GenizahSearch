"""Saved-result pagination must be bounded, complete, and computation-free on reads."""
import json

import pytest
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.testclient import TestClient

from web.chatgpt_api import register_chatgpt_api, _validate_pilot_body
from web.chatgpt_pagination import build_pages, PAGE_BYTES


def payload(n=191):
    return {'schema_version': 1, 'source': 'search', 'generated_at': '2026-09-14T00:00:00Z',
            'request': {'search_mode': 'fuzzy', 'limit': 500},
            'count': n, 'total': n + 1, 'warnings': [{'code': 'engine_cap'}],
            'results': [{'uid': str(i), 'locator': {'sys_id': str(i), 'p_num': 1},
                         'shelfmark': str(i), 'snippet': 'שלום'} for i in range(n)],
            'filtered': [{'uid': 'filtered', 'shelfmark': 'filtered', 'snippet': 'א'}]}


def test_all_191_results_in_order_and_filtered_is_separate():
    original = payload()
    before = json.dumps(original)
    pages, size = build_pages(original, 'private')
    rows = []
    for index, raw in enumerate(pages['results']):
        page = json.loads(raw)
        rows += page['results']
        assert page['pagination']['page'] == index
        assert page['pagination']['next_page'] == (index + 1 if index < 19 else None)
        assert page['pagination']['available'] == 191
        assert page['request'] == original['request'] and page['total'] == 192
        assert page['warnings'][0] == original['warnings'][0]
        assert len(raw) <= PAGE_BYTES
    assert [r['uid'] for r in rows] == [str(i) for i in range(191)]
    assert len(rows) == 191 and len(pages['results']) == 20
    filtered = json.loads(pages['filtered'][0])
    assert filtered['count'] == 0 and filtered['pagination']['returned'] == 1
    assert filtered['filtered'][0]['uid'] == 'filtered'
    assert size == sum(len(raw) for collection in pages.values() for raw in collection)
    assert json.dumps(original) == before


def test_large_rows_shrink_pages_without_dropping_candidates_or_spans():
    original = payload(11)
    for row in original['results']:
        row['snippet'] = 'א' * 10000
        row['matches'] = [{'manuscript_snippet': 'complete', 'score': 1}] * 6
    pages, _ = build_pages(original, 'private')
    rows = []
    for raw in pages['results']:
        page = json.loads(raw)
        assert len(raw) <= PAGE_BYTES
        assert page['pagination']['offset'] == len(rows)
        rows.extend(page['results'])
    assert [r['uid'] for r in rows] == [str(i) for i in range(11)]
    assert all(r['snippet'] == 'א' * 10000 and r['matches_available'] == 6 for r in rows)


def test_oversize_row_and_storage_overflow_fail_explicitly(monkeypatch):
    original = payload(1)
    original['results'][0]['snippet'] = 'x' * 100000
    with pytest.raises(ValueError, match='page allowance'):
        build_pages(original, 'private')
    monkeypatch.setattr('web.chatgpt_pagination.MAX_STORED_BYTES', 1)
    with pytest.raises(ValueError, match='storage allowance'):
        build_pages(payload(), 'private')


def app_for(monkeypatch):
    calls = []
    monkeypatch.setattr('web.chatgpt_api._job_owner', lambda req: req.client.host)
    app = FastAPI()
    async def handler(request: Request):
        calls.append(await request.json())
        return JSONResponse(payload())
    for name in ('search', 'parallels', 'browse', 'capabilities'):
        app.add_api_route('/' + name, handler, methods=['POST', 'GET'])
    register_chatgpt_api(app)
    return app, calls


def test_show_more_never_runs_search_again_and_reads_are_throttled(monkeypatch):
    app, calls = app_for(monkeypatch)
    with TestClient(app) as client:
        token = client.post('/chatgpt/search/jobs', json={'query': 'a', 'search_mode': 'fuzzy'}).json()['job_id']
        rows = []
        index = 0
        while index is not None:
            response = client.get(f'/chatgpt/jobs/{token}', params={'page': index})
            assert response.status_code == 200
            data = response.json()
            rows.extend(data['results'])
            index = data['pagination']['next_page']
        assert len(rows) == 191
        assert calls == [{'query': 'a', 'search_mode': 'fuzzy', 'limit': 500}]
        assert client.get(f'/chatgpt/jobs/{token}?page=2').json()['results'][0]['uid'] == '20'
        assert len(calls) == 1
        assert client.get(f'/chatgpt/jobs/{token}?page=999').status_code == 400
        assert client.get(f'/chatgpt/jobs/{token}?page=-1').status_code == 400
        assert client.get(f'/chatgpt/jobs/{token}?collection=unknown').status_code == 400
        monkeypatch.setattr('web.chatgpt_jobs.RETRIEVALS_PER_MINUTE', 1)
        limited = client.get(f'/chatgpt/jobs/{token}')
        assert limited.status_code == 429 and limited.headers['Retry-After'] == '60'
        assert len(calls) == 1


def test_global_storage_limit_returns_error_not_incomplete_pages(monkeypatch):
    app, calls = app_for(monkeypatch)
    with TestClient(app) as client:
        token = client.post('/chatgpt/search/jobs', json={'query': 'a', 'search_mode': 'exact'}).json()['job_id']
        assert client.get(f'/chatgpt/jobs/{token}').status_code == 200
        monkeypatch.setattr('web.chatgpt_jobs.MAX_CACHE_BYTES', app.state.chatgpt_jobs[token].stored_bytes)
        rejected = client.post('/chatgpt/search/jobs', json={'query': 'b', 'search_mode': 'exact'})
        assert rejected.status_code == 503
        assert rejected.json()['error']['code'] == 'chatgpt_storage_limit'
        assert client.get(f'/chatgpt/jobs/{token}?page=1').status_code == 200
        assert len(calls) == 1


def test_background_candidate_limits_do_not_expand_old_sync_contract():
    for mode, expected in [('exact', 100), ('variants', 100), ('fuzzy', 500)]:
        request = {'query': 'a', 'search_mode': mode}
        assert _validate_pilot_body('search', request, background=True) is None
        assert request['limit'] == expected
        assert _validate_pilot_body('search', dict(request, limit=expected+1), background=True)
    assert _validate_pilot_body('search', {'query': 'a', 'limit': 11})


def test_pagination_matches_published_contract():
    from pathlib import Path
    from jsonschema import Draft202012Validator
    spec = json.loads((Path(__file__).parents[1] / 'integrations/chatgpt/openapi.json').read_text(encoding='utf8'))
    validator = Draft202012Validator({'$ref': '#/components/schemas/Results', 'components': spec['components']})
    pages, _ = build_pages(payload(), 'private')
    for raw in pages['results']:
        validator.validate(json.loads(raw))
