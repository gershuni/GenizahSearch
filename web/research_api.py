"""Bounded background API jobs; existing endpoint responses are preserved."""
from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
import json
from pathlib import Path
import secrets
import tempfile
import threading
import time

from fastapi import Request
from fastapi.responses import JSONResponse, Response
from pydantic import ValidationError


@dataclass
class APIJob:
    owner: str
    token: str = field(default_factory=lambda: secrets.token_urlsafe(32))
    cancel: threading.Event = field(default_factory=threading.Event)
    state: str = 'queued'
    status: str = 'Queued'
    progress: tuple = (0, 0)
    task: asyncio.Task | None = None
    finished: float | None = None
    http_status: int = 200
    media_type: str = 'application/json'

    def update(self, status, progress):
        self.status = status
        self.progress = progress
        self.state = 'queued' if status.startswith(('Queued', 'Waiting')) else 'running'


def register_research_api(app, prefix, search_endpoint, parallels_endpoint):
    from shared.api_errors import APIError
    from web.search_api import SearchRequest, ParallelsRequest, enforce_mode_gate, _resolve_rate_limit_key

    directory = tempfile.TemporaryDirectory(prefix='genizah-api-jobs-')
    root = Path(directory.name)
    records = {}
    # Kept by app state so its lifetime matches the serving app, not a request.
    app.state.research_api_jobs = records
    app.state.research_api_directory = directory
    ttl = 600

    async def shutdown():
        tasks = []
        for job in records.values():
            job.cancel.set()
            if job.task is not None and not job.task.done():
                job.task.cancel()
                tasks.append(job.task)
        await asyncio.gather(*tasks, return_exceptions=True)
        directory.cleanup()

    if hasattr(app, 'on_shutdown'):
        app.on_shutdown(shutdown)
    else:
        app.add_event_handler('shutdown', shutdown)

    def error(code, message, status):
        return JSONResponse({'error': {'code': code, 'message': message}}, status_code=status)

    def prune():
        for token, job in list(records.items()):
            if job.finished is not None and time.monotonic() - job.finished >= ttl:
                records.pop(token)
                (root / token).unlink(missing_ok=True)

    def owner(request):
        enforce_mode_gate(request)
        return _resolve_rate_limit_key(request)

    async def execute(job, handler, scope, body):
        async def receive():
            return {'type': 'http.request', 'body': body, 'more_body': False}
        try:
            response = await handler(Request(scope, receive))
            if not isinstance(response, Response):
                response = JSONResponse(response)
            if len(response.body) > 16 * 1024**2:
                response = error('research_result_too_large',
                                 'Result exceeded the background transfer allowance.', 503)
            job.http_status = response.status_code
            job.media_type = response.media_type or 'application/json'
            await asyncio.to_thread((root / job.token).write_bytes, response.body)
            job.state = 'completed' if response.status_code < 400 else 'failed'
            job.status = job.state.capitalize()
        except (asyncio.CancelledError, InterruptedError):
            job.state, job.status = 'cancelled', 'Cancelled'
        except Exception:
            job.state, job.status = 'failed', 'Search could not complete'
            job.http_status = 500
        finally:
            job.cancel.set()
            job.finished = time.monotonic()

    def submit_endpoint(handler, model, route):
        async def submit(request: Request):
            try:
                client = owner(request)
            except APIError as exc:
                return error(exc.code, str(exc), exc.http_status)
            prune()
            if len(records) >= 32:
                return error('research_queue_full', 'Background job capacity is full. Retry shortly.', 503)
            if sum(j.owner == client and j.finished is None for j in records.values()) >= 2:
                return error('research_jobs_busy', 'Two background jobs are already active for this client.', 429)
            body = bytearray()
            async for chunk in request.stream():
                body.extend(chunk)
                if len(body) > 65536:
                    return error('invalid_request', 'Job request exceeds 64 KiB.', 413)
            try:
                parsed = json.loads(body)
                model.model_validate(parsed)
            except (ValueError, ValidationError):
                return error('invalid_request', 'Request must match the corresponding search API schema.', 400)
            # Reading the body yields; repeat admission atomically with insertion.
            if len(records) >= 32:
                return error('research_queue_full', 'Background job capacity is full. Retry shortly.', 503)
            if sum(j.owner == client and j.finished is None for j in records.values()) >= 2:
                return error('research_jobs_busy', 'Two background jobs are already active for this client.', 429)
            job = APIJob(client)
            records[job.token] = job
            scope = dict(request.scope, research_job=job, path=f'{prefix}/{route}',
                         raw_path=f'{prefix}/{route}'.encode())
            job.task = asyncio.create_task(execute(job, handler, scope, bytes(body)))
            external_prefix = request.scope.get('root_path', '').rstrip('/') + prefix
            return JSONResponse({
                'job_id': job.token, 'state': 'queued', 'retention_seconds': ttl,
                'status_url': f'{external_prefix}/research/jobs/{job.token}',
                'result_url': f'{external_prefix}/research/jobs/{job.token}/result',
            }, status_code=202, headers={'Cache-Control': 'no-store'})
        return submit

    def lookup(request, token):
        client = owner(request)
        prune()
        job = records.get(token)
        return job if job is not None and job.owner == client else None

    async def status(request: Request, job_id: str):
        try:
            job = lookup(request, job_id)
        except APIError as exc:
            return error(exc.code, str(exc), exc.http_status)
        if job is None:
            return error('job_not_found', 'Job does not exist or has expired.', 404)
        return JSONResponse({'job_id': job.token, 'state': job.state, 'status': job.status,
                             'progress': list(job.progress), 'result_status': job.http_status if job.finished else None},
                            headers={'Cache-Control': 'no-store'})

    async def result(request: Request, job_id: str):
        try:
            job = lookup(request, job_id)
        except APIError as exc:
            return error(exc.code, str(exc), exc.http_status)
        if job is None:
            return error('job_not_found', 'Job does not exist or has expired.', 404)
        if job.finished is None:
            return error('job_pending', 'Search is still queued or running.', 409)
        if job.state == 'cancelled':
            return error('job_cancelled', 'Search was cancelled.', 409)
        path = root / job.token
        if not path.exists():
            return error('job_cancelled' if job.state == 'cancelled' else 'job_failed', job.status, 409)
        return Response(await asyncio.to_thread(path.read_bytes), status_code=job.http_status,
                        media_type=job.media_type, headers={'Cache-Control': 'no-store'})

    async def cancel(request: Request, job_id: str):
        try:
            job = lookup(request, job_id)
        except APIError as exc:
            return error(exc.code, str(exc), exc.http_status)
        if job is None:
            return error('job_not_found', 'Job does not exist or has expired.', 404)
        job.cancel.set()
        if job.finished is None:
            job.task.cancel()
            await asyncio.gather(job.task, return_exceptions=True)
            job.state, job.status = 'cancelled', 'Cancelled'
            job.finished = time.monotonic()
        return JSONResponse({'job_id': job.token, 'state': job.state}, headers={'Cache-Control': 'no-store'})

    app.add_api_route(f'{prefix}/search/jobs', submit_endpoint(search_endpoint, SearchRequest, 'search'), methods=['POST'])
    app.add_api_route(f'{prefix}/parallels/jobs', submit_endpoint(parallels_endpoint, ParallelsRequest, 'parallels'), methods=['POST'])
    app.add_api_route(f'{prefix}/research/jobs/{{job_id}}', status, methods=['GET'])
    app.add_api_route(f'{prefix}/research/jobs/{{job_id}}', cancel, methods=['DELETE'])
    app.add_api_route(f'{prefix}/research/jobs/{{job_id}}/result', result, methods=['GET'])
