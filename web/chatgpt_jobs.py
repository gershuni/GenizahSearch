"""Bounded research jobs whose opaque IDs survive changes in ChatGPT egress IP.

These are separate from the existing IP-bound research jobs. IDs are private
bearer capabilities, not manuscript links. No job listing endpoint is exposed.
"""
import asyncio
from contextlib import asynccontextmanager
import time
import json

from fastapi import Request
from fastapi.responses import JSONResponse, Response

from web.research_api import APIJob
from web.chatgpt_pagination import build_pages, MAX_STORED_BYTES

MAX_JOBS = 32
MAX_ACTIVE_PER_OWNER = 2
MAX_JOB_SECONDS = 600
RETENTION_SECONDS = 600
POLL_SECONDS = 10
MAX_CACHE_BYTES = 64 * 1024 * 1024
MAX_RAW_BYTES = 16 * 1024 * 1024
RETRIEVALS_PER_MINUTE = 60


def register_chatgpt_jobs(app, handlers, prepare, finish, owner):
    jobs = {}
    app.state.chatgpt_jobs = jobs

    def error(code, message, status):
        return JSONResponse({'error': {'code': code, 'message': message}}, status_code=status,
                            headers={'Cache-Control': 'no-store'})

    def prune():
        now = time.monotonic()
        for token, job in list(jobs.items()):
            if job.finished is not None and now - job.finished >= RETENTION_SECONDS:
                del jobs[token]

    def pending(job):
        return JSONResponse({'job_id': job.token, 'state': job.state,
                             'status': job.status, 'progress': list(job.progress),
                             'next_action': 'getResearchJob', 'poll_after_seconds': 2,
                             'retention_seconds': RETENTION_SECONDS}, status_code=202,
                            headers={'Cache-Control': 'no-store', 'Retry-After': '2'})

    async def run(job, handler, request):
        try:
            response = await asyncio.wait_for(handler(request), MAX_JOB_SECONDS)
            if not isinstance(response, Response):
                response = JSONResponse(response)
            if response.status_code < 400:
                if len(response.body) > MAX_RAW_BYTES:
                    raise ValueError('Search output exceeds the storage allowance; narrow the query.')
                def paginate():
                    return build_pages(json.loads(response.body), job.token)
                # Serialization is performed once off the event loop.
                pages, size = await asyncio.to_thread(paginate)
                prune()
                if sum(getattr(j, 'stored_bytes', 0) for j in jobs.values()) + size > MAX_CACHE_BYTES:
                    raise ValueError('Saved result capacity is full; try later or narrow the query.')
                job.pages, job.stored_bytes = pages, size
                job.response = Response(status_code=200)
            else:
                job.response = finish(response)
        except ValueError as exc:
            job.response = error('chatgpt_storage_limit', str(exc), 503)
        except TimeoutError:
            job.response = error('research_job_timeout', 'Search exceeded the background job allowance. No complete result is available.', 504)
        except asyncio.CancelledError:
            job.response = error('job_cancelled', 'The search job was cancelled.', 409)
            raise
        except Exception:
            job.response = error('research_job_failed', 'The search could not complete. No complete result is available.', 500)
        finally:
            job.cancel.set()
            job.finished = time.monotonic()
            job.state = 'completed' if job.response.status_code < 400 else 'failed'

    def submit(kind):
        async def endpoint(request: Request):
            client = owner(request)  # Same mode gate/proxy rules as original API.
            if isinstance(client, JSONResponse):
                return client
            prepared = await prepare(kind, request)
            if isinstance(prepared, JSONResponse):
                return prepared
            prune()
            # No await between admission and insertion.
            if len(jobs) >= MAX_JOBS:
                return error('research_queue_full', 'Background capacity is full. Try later.', 503)
            reserved = sum(MAX_STORED_BYTES if j.finished is None else getattr(j, 'stored_bytes', 0)
                           for j in jobs.values())
            if reserved + MAX_STORED_BYTES > MAX_CACHE_BYTES:
                return error('chatgpt_storage_limit', 'Saved-result capacity is full. Try later; no new search was started.', 503)
            if sum(j.owner == client and j.finished is None for j in jobs.values()) >= MAX_ACTIVE_PER_OWNER:
                return error('research_jobs_busy', 'Two searches are already active. Retrieve their results before submitting another.', 429)
            job = APIJob(client)
            job.response = None
            job.pages, job.stored_bytes = {}, 0
            job.read_times = []
            job.polling = False
            jobs[job.token] = job
            prepared.scope = dict(prepared.scope, research_job=job)
            job.task = asyncio.create_task(run(job, handlers['/' + kind], prepared))
            return pending(job)
        return endpoint

    async def poll(request: Request, job_id: str):
        guard = owner(request)  # Gate still applies; possession of the ID authorizes retrieval.
        if isinstance(guard, JSONResponse):
            return guard
        prune()
        job = jobs.get(job_id)
        if job is None:
            return error('job_not_found', 'Job ID is unknown, expired, or lost after a server restart. This is not an empty search result.', 404)
        try:
            page = int(request.query_params.get('page', '0'))
            collection = request.query_params.get('collection', 'results')
            if page < 0 or collection not in ('results', 'filtered'):
                raise ValueError()
        except ValueError:
            return error('invalid_page', 'Use a nonnegative page number and results or filtered collection.', 400)
        now = time.monotonic()
        job.read_times = [stamp for stamp in job.read_times if now - stamp < 60]
        if len(job.read_times) >= RETRIEVALS_PER_MINUTE or job.polling:
            response = error('rate_limited', 'Retrieve pages sequentially; retry after the indicated delay.', 429)
            response.headers['Retry-After'] = '2' if job.polling else '60'
            return response
        job.read_times.append(now)
        if job.finished is None:
            # A cancelled/disconnected poll must never cancel the running search.
            job.polling = True
            try:
                await asyncio.wait({job.task}, timeout=POLL_SECONDS)
            finally:
                job.polling = False
        if job.finished is None:
            return pending(job)
        response = job.response
        if job.pages:
            pages = job.pages[collection]
            if page >= len(pages):
                return error('invalid_page', 'Page is beyond the saved results. Follow pagination.next_page.', 400)
            return Response(pages[page], media_type='application/json', headers={'Cache-Control': 'no-store'})
        response.headers['Cache-Control'] = 'no-store'
        return response

    async def shutdown():
        tasks = []
        for job in jobs.values():
            job.cancel.set()
            if job.task is not None and not job.task.done():
                job.task.cancel()
                tasks.append(job.task)
        await asyncio.gather(*tasks, return_exceptions=True)
        jobs.clear()

    app.state.shutdown_chatgpt_jobs = shutdown
    previous_lifespan = app.router.lifespan_context

    @asynccontextmanager
    async def lifespan(application):
        async with previous_lifespan(application) as state:
            try:
                yield state
            finally:
                await shutdown()

    app.router.lifespan_context = lifespan
    app.add_api_route('/chatgpt/search/jobs', submit('search'), methods=['POST'], include_in_schema=False)
    app.add_api_route('/chatgpt/parallels/jobs', submit('parallels'), methods=['POST'], include_in_schema=False)
    app.add_api_route('/chatgpt/jobs/{job_id}', poll, methods=['GET'], include_in_schema=False)
