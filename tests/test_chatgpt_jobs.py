"""Regression: a search survives many short HTTP polls and changing egress IPs."""
import asyncio
import json
import time

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.testclient import TestClient
import httpx

from web.chatgpt_api import register_chatgpt_api


def make_app(monkeypatch, handler):
    monkeypatch.setattr('web.chatgpt_api._job_owner', lambda request: request.client.host)
    monkeypatch.setattr('web.chatgpt_jobs.POLL_SECONDS', 0.01)
    child = FastAPI()
    for kind in ('search', 'parallels', 'browse', 'capabilities'):
        child.add_api_route('/' + kind, handler, methods=['POST', 'GET'])
    register_chatgpt_api(child)
    parent = FastAPI()
    parent.mount('/api', child)
    return parent, child


BODY = {'query': 'משה בן אלעזר', 'search_mode': 'variants', 'limit': 5}


def test_search_survives_short_polls_and_ip_change(monkeypatch):
    async def exercise():
        finish = asyncio.Event()
        calls = []

        async def handler(request: Request):
            job = request.scope['research_job']
            calls.append(await request.json())
            job.update('Starting search worker', (0, 0))
            await finish.wait()
            assert not job.cancel.is_set()
            return JSONResponse({'schema_version': 1, 'count': 1, 'total': 1,
                                 'request': BODY, 'results': [{'shelfmark': 'A'}],
                                 'warnings': [{'code': 'upstream_warning'}]})

        parent, child = make_app(monkeypatch, handler)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(parent, client=('192.0.2.1', 1)), base_url='http://test') as submitter:
            response = await submitter.post('/api/chatgpt/search/jobs', json=BODY)
            assert response.status_code == 202
            token = response.json()['job_id']
            assert len(token) >= 40
        async with httpx.AsyncClient(transport=httpx.ASGITransport(parent, client=('192.0.2.2', 1)), base_url='http://test') as poller:
            for _ in range(3):
                response = await poller.get('/api/chatgpt/jobs/' + token)
                assert response.status_code == 202
                assert response.json()['job_id'] == token
                assert response.json()['state'] == 'running'
            assert len(calls) == 1  # Pending polling does not submit more workers.
            finish.set()
            for _ in range(50):
                result = await poller.get('/api/chatgpt/jobs/' + token)
                if result.status_code != 202:
                    break
            assert result.status_code == 200
            assert result.json()['request'] == BODY
            assert result.json()['warnings'][0]['code'] == 'upstream_warning'
            assert result.headers['Cache-Control'] == 'no-store'
            assert (await poller.get('/api/chatgpt/jobs/wrong-id')).status_code == 404
            assert len(calls) == 1
            child.state.chatgpt_jobs[token].finished = time.monotonic() - 601
            assert (await poller.get('/api/chatgpt/jobs/' + token)).status_code == 404
    asyncio.run(exercise())


def test_job_error_preserves_status_and_retry_after(monkeypatch):
    async def handler(request: Request):
        return JSONResponse({'error': {'code': 'rate_limited', 'message': 'Wait'}},
                            status_code=429, headers={'Retry-After': '18'})
    app, _ = make_app(monkeypatch, handler)
    with TestClient(app) as client:
        token = client.post('/api/chatgpt/search/jobs', json=BODY).json()['job_id']
        response = client.get('/api/chatgpt/jobs/' + token)
        assert response.status_code == 429
        assert response.headers['Retry-After'] == '18'
        assert response.json()['error']['code'] == 'rate_limited'


def test_job_deadline_signals_worker_cancellation(monkeypatch):
    monkeypatch.setattr('web.chatgpt_jobs.MAX_JOB_SECONDS', 0.02)
    cancelled = []

    async def handler(request: Request):
        cancelled.append(request.scope['research_job'].cancel)
        await asyncio.sleep(10)

    app, _ = make_app(monkeypatch, handler)
    with TestClient(app) as client:
        token = client.post('/api/chatgpt/search/jobs', json=BODY).json()['job_id']
        for _ in range(10):
            response = client.get('/api/chatgpt/jobs/' + token)
            if response.status_code != 202:
                break
        assert response.status_code == 504
        assert response.json()['error']['code'] == 'research_job_timeout'
        assert cancelled[0].is_set()


def test_admission_input_bounds_and_shutdown(monkeypatch):
    async def handler(request: Request):
        await asyncio.sleep(10)
    app, child = make_app(monkeypatch, handler)
    with TestClient(app) as client:
        for _ in range(2):
            assert client.post('/api/chatgpt/search/jobs', json=BODY).status_code == 202
        assert client.post('/api/chatgpt/search/jobs', json=BODY).status_code == 429
        assert client.post('/api/chatgpt/parallels/jobs', json={'method': 'chunk'}).status_code == 400
        assert client.post('/api/chatgpt/search/jobs', content='x' * 65537).status_code == 413
        # Mounted sub-app lifecycle is explicitly forwarded by production main.
        records = list(child.state.chatgpt_jobs.values())
        client.portal.call(child.state.shutdown_chatgpt_jobs)
        assert all(job.cancel.is_set() and job.task.done() for job in records)
        assert not child.state.chatgpt_jobs


def test_mode_gate_still_blocks_polling(monkeypatch):
    async def handler(request: Request):
        return JSONResponse({})
    app, _ = make_app(monkeypatch, handler)
    # register_chatgpt_jobs keeps the owner callable; change its behavior via request.
    app2 = FastAPI()
    from web.chatgpt_jobs import register_chatgpt_jobs
    async def prepare(kind, request):
        return request
    register_chatgpt_jobs(app2, {'/search': handler, '/parallels': handler}, prepare,
                         lambda response: response,
                         lambda request: JSONResponse({'error': {'code': 'disabled'}}, status_code=503))
    with TestClient(app2) as client:
        assert client.get('/chatgpt/jobs/any-id').status_code == 503
        assert client.post('/chatgpt/search/jobs', json=BODY).status_code == 503


def test_pending_response_matches_published_schema(monkeypatch):
    from pathlib import Path
    from jsonschema import Draft202012Validator
    async def handler(request: Request):
        return JSONResponse({'error': {'code': 'invalid_request', 'message': 'test'}}, status_code=400)
    app, _ = make_app(monkeypatch, handler)
    with TestClient(app) as client:
        pending = client.post('/api/chatgpt/search/jobs', json=BODY).json()
        spec = json.loads((Path(__file__).parents[1] / 'integrations/chatgpt/openapi.json').read_text(encoding='utf8'))
        Draft202012Validator({'$ref': '#/components/schemas/PendingJob', 'components': spec['components']}).validate(pending)
