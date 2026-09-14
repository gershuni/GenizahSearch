"""Bounded research jobs whose opaque IDs survive changes in ChatGPT egress IP.

These are separate from the existing IP-bound research jobs. IDs are private
bearer capabilities, not manuscript links. No job listing endpoint is exposed.
"""
import asyncio
from contextlib import asynccontextmanager
import time

from fastapi import Request
from fastapi.responses import JSONResponse

from web.research_api import APIJob

MAX_JOBS = 32
MAX_ACTIVE_PER_OWNER = 2
MAX_JOB_SECONDS = 600
RETENTION_SECONDS = 600
POLL_SECONDS = 10


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
            job.response = finish(response)
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
            if sum(j.owner == client and j.finished is None for j in jobs.values()) >= MAX_ACTIVE_PER_OWNER:
                return error('research_jobs_busy', 'Two searches are already active. Retrieve their results before submitting another.', 429)
            job = APIJob(client)
            job.response = None
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
        if job.finished is None:
            # A cancelled/disconnected poll must never cancel the running search.
            await asyncio.wait({job.task}, timeout=POLL_SECONDS)
        if job.finished is None:
            return pending(job)
        response = job.response
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
