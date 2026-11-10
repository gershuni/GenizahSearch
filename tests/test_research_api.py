"""Background requests finish independently of a single HTTP response."""
import asyncio
import time

from fastapi import FastAPI
from fastapi.responses import JSONResponse
import httpx

from web.research_api import register_research_api


def test_mounted_api_returns_external_job_urls(monkeypatch):
    monkeypatch.setenv('SEARCH_API_MODE', 'open')

    async def exercise():
        async def handler(request):
            return JSONResponse({'results': []})

        parent, child = FastAPI(), FastAPI()
        register_research_api(child, '', handler, handler)
        parent.mount('/api', child)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(parent), base_url='http://test') as client:
            response = await client.post('/api/search/jobs', json={'query': 'q', 'search_mode': 'exact'})
            url = response.json()['status_url']
            assert url.startswith('/api/research/jobs/')
            assert (await client.get(url)).status_code == 200
            await client.delete(url)
        child.state.research_api_directory.cleanup()
    asyncio.run(exercise())


def test_background_job_lifecycle_privacy_and_cancellation(monkeypatch):
    monkeypatch.setenv('SEARCH_API_MODE', 'open')

    async def exercise():
        finish = asyncio.Event()

        async def handler(request):
            job = request.scope['research_job']
            job.update('Searching', (2, 10))
            await finish.wait()
            return JSONResponse({'results': ['complete'], 'warnings': []})

        app = FastAPI()
        register_research_api(app, '/api', handler, handler)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url='http://test') as client:
            created = await client.post('/api/search/jobs', json={'query': 'שלום', 'search_mode': 'exact'})
            assert created.status_code == 202
            status_url = created.json()['status_url']
            result_url = created.json()['result_url']
            await asyncio.sleep(0)
            assert (await client.get(status_url)).json()['state'] == 'running'
            assert (await client.get(result_url)).status_code == 409
            other_transport = httpx.ASGITransport(app, client=('192.0.2.4', 1234))
            async with httpx.AsyncClient(transport=other_transport, base_url='http://test') as other:
                assert (await other.get(status_url)).status_code == 404
                assert (await other.delete(status_url)).status_code == 404
            finish.set()
            for _ in range(100):
                if (await client.get(status_url)).json()['state'] == 'completed':
                    break
                await asyncio.sleep(0.01)
            assert (await client.get(result_url)).json()['results'] == ['complete']

            finish.clear()
            queued = await client.post('/api/search/jobs', json={'query': 'another', 'search_mode': 'exact'})
            cancel_url = queued.json()['status_url']
            assert (await client.delete(cancel_url)).json()['state'] == 'cancelled'
            assert (await client.get(cancel_url)).json()['state'] == 'cancelled'

            # A completed record expires; its private result file is removed.
            job = app.state.research_api_jobs[created.json()['job_id']]
            job.finished = time.monotonic() - 601
            assert (await client.get(status_url)).status_code == 404
        app.state.research_api_directory.cleanup()
    asyncio.run(exercise())


def test_admission_and_schema_limits(monkeypatch):
    monkeypatch.setenv('SEARCH_API_MODE', 'open')

    async def exercise():
        async def handler(request):
            await asyncio.Event().wait()

        app = FastAPI()
        register_research_api(app, '/api', handler, handler)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url='http://test') as client:
            assert (await client.post('/api/search/jobs', json={'nonsense': True})).status_code == 400
            jobs = [await client.post('/api/search/jobs', json={'query': 'q', 'search_mode': 'exact'}) for _ in range(2)]
            assert all(job.status_code == 202 for job in jobs)
            assert (await client.post('/api/search/jobs', json={'query': 'q', 'search_mode': 'exact'})).status_code == 429
            for job in jobs:
                await client.delete(job.json()['status_url'])
            assert (await client.post('/api/search/jobs', content=b'x' * 65537)).status_code == 413
        app.state.research_api_directory.cleanup()
    asyncio.run(exercise())


def test_existing_search_handler_background_path_has_no_http_deadline(monkeypatch):
    from web.search_api import init_search_api, _rate_limiter
    from web.state import state
    monkeypatch.setenv('SEARCH_API_MODE', 'open')
    monkeypatch.setenv('SEARCH_API_CORE_TIMEOUT', '0.01')
    monkeypatch.setenv('SEARCH_API_POSTHOG_SAMPLE_N', '999999')
    _rate_limiter.reset_for_tests()

    class Searcher:
        def execute_search(self, **kwargs):
            time.sleep(0.1)
            return []

    monkeypatch.setattr(state, 'searcher', Searcher())

    async def exercise():
        app = FastAPI()
        init_search_api(app)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url='http://test') as client:
            response = await client.post('/api/search/jobs', json={'query': 'שלום', 'search_mode': 'exact'})
            assert response.status_code == 202
            urls = response.json()
            for _ in range(100):
                if (await client.get(urls['status_url'])).json()['state'] in ('completed', 'failed'):
                    break
                await asyncio.sleep(0.01)
            result = await client.get(urls['result_url'])
            assert result.status_code == 200, result.text
            assert result.json()['results'] == []
        app.state.research_api_directory.cleanup()
    asyncio.run(exercise())
