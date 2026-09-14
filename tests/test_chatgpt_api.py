"""GPT transport limits must not silently change research evidence."""
import asyncio
from copy import deepcopy
import json

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response
from fastapi.testclient import TestClient

from web.chatgpt_api import MAX_RESPONSE_BYTES, compact_payload, register_chatgpt_api


def payload(n=20):
    return {'schema_version': 1, 'count': n, 'total': 123, 'source': 'parallels',
            'request': {'method': 'passage', 'passage_policy': {'verified': 3000}},
            'warnings': [{'code': 'passage_results_truncated'}],
            'results': [{'shelfmark': str(i), 'locator': {'sys_id': str(i), 'p_num': 1},
                         'snippet': 'אבג' * 300, 'matches': [{'score': j, 'manuscript_snippet': 'שלם'} for j in range(7)]}
                        for i in range(n)], 'filtered': []}


def client_for(handler):
    app = FastAPI()
    for name in ('search', 'parallels', 'browse', 'capabilities'):
        app.add_api_route('/' + name, handler, methods=['POST', 'GET'])
    register_chatgpt_api(app)
    parent = FastAPI()
    parent.mount('/api', app)
    return TestClient(parent)


def test_compaction_retains_provenance_and_discloses_missing_evidence():
    original = payload()
    before = deepcopy(original)
    result = compact_payload(original)
    assert original == before
    assert result['total'] == 123 and result['count'] == 5
    assert result['request'] == before['request']
    assert result['warnings'][0] == before['warnings'][0]
    assert result['warnings'][-1]['upstream_count'] == 20
    assert result['results'][0]['matches_available'] == 7
    assert result['results'][0]['matches'] == before['results'][0]['matches'][:3]
    assert len(JSONResponse(result).body) <= MAX_RESPONSE_BYTES


def test_large_rows_are_removed_whole_and_counted():
    original = payload(5)
    for row in original['results']:
        row['snippet'] = 'א' * 10000
    result = compact_payload(original)
    assert 0 < result['count'] < 5
    assert len(JSONResponse(result).body) <= MAX_RESPONSE_BYTES
    assert result['results'][0]['snippet'] == original['results'][0]['snippet']


def test_oversized_first_result_never_becomes_false_empty_success():
    original = payload(1)
    original['results'][0]['snippet'] = 'x' * 100000
    assert compact_payload(original) is None


def test_browse_preserves_source_and_complete_text():
    original = {'text': 'א' * 10000, 'text_source': 'snippet', 'text_truncated': True,
                'locator': {'sys_id': '1'}, 'metadata': {'pgp': None}, 'warnings': []}
    assert compact_payload(original) == original
    assert compact_payload({'text': 'x' * 100000}) is None


def test_wrapper_preserves_error_status_headers_and_body():
    async def handler(request):
        return Response('upstream unavailable', status_code=503, headers={'Retry-After': '12'})
    with client_for(handler) as client:
        response = client.post('/api/chatgpt/search', json={'query': 'a', 'search_mode': 'exact'})
        assert response.status_code == 503
        assert response.headers['Retry-After'] == '12'
        assert response.text == 'upstream unavailable'


def test_wrapper_forwards_caller_and_enforces_bounded_inputs():
    calls = []

    async def handler(request):
        calls.append((request.client.host, request.headers['x-test'], await request.json()))
        return JSONResponse(payload())

    with client_for(handler) as client:
        response = client.post('/api/chatgpt/search', json={'query': 'a', 'search_mode': 'exact'}, headers={'x-test': 'kept'})
        assert response.status_code == 200
        assert response.json()['count'] == 5
        assert calls == [('testclient', 'kept', {'query': 'a', 'search_mode': 'exact', 'limit': 5})]
        for body in ({'limit': 500}, {'limit': True}):
            assert client.post('/api/chatgpt/search', json=body).status_code == 400
        for body in ({'method': 'chunk', 'text': 'abc'},
                     {'method': 'passage', 'text': 'x' * 5001},
                     {'method': 'passage', 'witnesses': [{'text': 'a'}] * 4}):
            assert client.post('/api/chatgpt/parallels', json=body).status_code == 400
        assert client.post('/api/chatgpt/search', content='x' * 65537).status_code == 413
        assert len(calls) == 1


def test_oversized_error_retains_code_status_and_retry_delay():
    async def handler(request):
        return JSONResponse({'error': {'code': 'rate_limited', 'message': 'x' * 100000}},
                            status_code=429, headers={'Retry-After': '17'})
    with client_for(handler) as client:
        response = client.get('/api/chatgpt/browse')
    assert response.status_code == 429 and response.headers['Retry-After'] == '17'
    assert response.json()['error']['code'] == 'rate_limited'
    assert response.json()['error']['upstream_body_truncated'] is True
    assert len(response.content) < MAX_RESPONSE_BYTES


def test_invalid_upstream_success_is_reported_as_failure():
    async def handler(request):
        return JSONResponse(['not', 'an', 'envelope'])
    with client_for(handler) as client:
        response = client.get('/api/chatgpt/browse')
    assert response.status_code == 502
    assert response.json()['error']['code'] == 'invalid_upstream_response'


def test_timeout_is_an_error_and_cancels_handler(monkeypatch):
    monkeypatch.setattr('web.chatgpt_api.ACTION_TIMEOUT', 0.01)
    cancelled = []

    async def handler(request):
        try:
            await asyncio.sleep(10)
        finally:
            cancelled.append(True)

    with client_for(handler) as client:
        response = client.get('/api/chatgpt/browse')
    assert response.status_code == 504
    assert response.json()['error']['code'] == 'chatgpt_timeout'
    assert cancelled == [True]


def test_original_routes_unchanged_and_import_schema_served():
    async def handler(request: Request):
        return JSONResponse(payload())
    with client_for(handler) as client:
        assert client.post('/api/parallels').json()['count'] == 20
        schema = client.get('/api/chatgpt/openapi.json').json()
        assert schema['servers'][0]['url'] == 'https://genizahsearch.com/api/chatgpt'
        assert set(schema['paths']) == {'/search', '/browse', '/parallels', '/capabilities'}
        assert 'request' not in schema['components']['schemas']['Browse']['required']
        json.dumps(schema)
