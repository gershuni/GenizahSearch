"""Small, additive GPT Actions facade over the existing hardened API handlers.

Original routes retain their contracts. All requests still pass through their
mode gate, rate limiter, validation and search concurrency controls.
"""
from __future__ import annotations

import asyncio
from copy import deepcopy
import json
from pathlib import Path

from fastapi import Request
from fastapi.responses import JSONResponse, Response

MAX_RESPONSE_BYTES = 80000  # Conservative: below the 100,000-character Actions cap.
ACTION_TIMEOUT = 40


def _error(code, message, status=400):
    return JSONResponse({'error': {'code': code, 'message': message}}, status_code=status)


def compact_payload(payload):
    """Keep complete text spans and provenance; disclose every removed row/match.

    Returns None if the fixed envelope or a browse payload cannot safely fit.
    Never cut a quotation or change the upstream effective request/total.
    """
    result = deepcopy(payload)
    if not isinstance(result, dict):
        raise ValueError('Expected a JSON object')
    if 'results' not in result:
        return result if len(JSONResponse(result).body) <= MAX_RESPONSE_BYTES else None
    original_count = len(result['results'])
    original_filtered = len(result.get('filtered', []))
    result['results'] = result['results'][:5]
    if 'filtered' in result:
        result['filtered'] = result['filtered'][:3]
    omitted_matches = 0
    for row in result['results'] + result.get('filtered', []):
        if len(row.get('matches', [])) > 3:
            row['matches_available'] = len(row['matches'])
            omitted_matches += len(row['matches']) - 3
            row['matches'] = row['matches'][:3]
    note = {'code': 'chatgpt_output_limited',
            'message': 'Bounded preview, not a census. Full API results may contain additional evidence.',
            'upstream_count': original_count, 'upstream_filtered_count': original_filtered,
            'matches_omitted_from_initial_preview': omitted_matches}
    result.setdefault('warnings', []).append(note)
    while True:
        result['count'] = len(result['results'])
        note['results_returned'] = result['count']
        note['filtered_returned'] = len(result.get('filtered', []))
        if len(JSONResponse(result).body) <= MAX_RESPONSE_BYTES:
            if original_count and not result['results']:
                return None  # An oversized first row must not become "no matches".
            return result
        if result.get('filtered'):
            result['filtered'].pop()
        elif result['results']:
            result['results'].pop()
        else:
            return None


def _validate_pilot_body(kind, body):
    if not isinstance(body, dict):
        return 'Expected a JSON object.'
    if kind == 'search':
        limit = body.get('limit', 5)
        if type(limit) is not int or not 1 <= limit <= 10:
            return 'ChatGPT search limit must be an integer from 1 to 10.'
        body['limit'] = limit
    if kind == 'parallels':
        if body.get('method') != 'passage':
            return 'This action requires method=passage; chunk search is not supported here.'
        texts = [body.get('text', '')]
        witnesses = body.get('witnesses')
        if witnesses is not None:
            if not isinstance(witnesses, list) or not 1 <= len(witnesses) <= 3:
                return 'This pilot supports one to three separate witnesses per request.'
            for witness in witnesses:
                if not isinstance(witness, dict):
                    return 'Each witness must be a JSON object.'
                texts.append(witness.get('text', ''))
        if any(text is not None and (not isinstance(text, str) or len(text) > 5000) for text in texts):
            return 'Each passage must contain at most 5000 characters; search labeled sections separately.'
    return None


async def _prepare_request(kind, request):
    raw = bytearray()
    async for chunk in request.stream():
        raw.extend(chunk)
        if len(raw) > 65536:
            return _error('invalid_request', 'ChatGPT request exceeds 64 KiB.', 413)
    try:
        body = json.loads(raw)
    except (ValueError, UnicodeError):
        return _error('invalid_request', 'Expected a JSON request body.')
    problem = _validate_pilot_body(kind, body)
    if problem:
        return _error('invalid_request', problem)
    encoded = json.dumps(body, ensure_ascii=False).encode('utf-8')

    async def receive():
        return {'type': 'http.request', 'body': encoded, 'more_body': False}

    # Keep client, headers and scope so the original guards see the caller.
    request = Request(request.scope, receive)
    return request


def _finish_response(response):
    if not isinstance(response, Response):
        response = JSONResponse(response)
    if response.status_code >= 400:
        if len(response.body) <= MAX_RESPONSE_BYTES:
            return response  # Includes the original status and Retry-After.
        code, message = 'upstream_error', 'Upstream error body exceeded the ChatGPT transfer limit.'
        try:
            detail = json.loads(response.body)['error']
            code = str(detail['code'])[:200]
            message = str(detail['message'])[:2000]
        except (ValueError, TypeError, KeyError):
            pass
        headers = {'Retry-After': response.headers['Retry-After']} if 'Retry-After' in response.headers else {}
        return JSONResponse({'error': {'code': code, 'message': message,
                                       'upstream_body_truncated': True}},
                            status_code=response.status_code, headers=headers)
    try:
        payload = compact_payload(json.loads(response.body))
    except (ValueError, TypeError):
        return _error('invalid_upstream_response', 'Search service did not return a JSON object.', 502)
    if payload is None:
        return _error('chatgpt_response_too_large', 'This result cannot fit in ChatGPT. Use the website or request a narrower search.', 503)
    headers = {k: v for k, v in response.headers.items()
               if k.lower() not in ('content-length', 'content-type', 'content-encoding')}
    return JSONResponse(payload, status_code=response.status_code, headers=headers)


def _job_owner(request):
    from shared.api_errors import APIError
    from web.search_api import enforce_mode_gate, _resolve_rate_limit_key
    try:
        enforce_mode_gate(request)
        return _resolve_rate_limit_key(request)
    except APIError as exc:
        return _error(exc.code, str(exc), exc.http_status)


def register_chatgpt_api(app):
    """Call after init_search_api on its /api sub-app; no extra corpus imports."""
    handlers = {route.path: route.endpoint for route in app.routes if hasattr(route, 'endpoint')}
    from web.chatgpt_jobs import register_chatgpt_jobs
    register_chatgpt_jobs(app, handlers, _prepare_request, _finish_response, _job_owner)

    def wrapped(kind):
        handler = handlers['/' + kind]

        async def endpoint(request: Request):
            if request.method == 'POST':
                request = await _prepare_request(kind, request)
                if isinstance(request, Response):
                    return request
            try:
                response = await asyncio.wait_for(handler(request), timeout=ACTION_TIMEOUT)
            except TimeoutError:
                return _error('chatgpt_timeout', 'Search exceeded this ChatGPT action deadline. No complete results returned; narrow the query or use the website.', 504)
            return _finish_response(response)

        return endpoint

    for kind, method in [('search', 'POST'), ('parallels', 'POST'), ('browse', 'GET'), ('capabilities', 'GET')]:
        app.add_api_route('/chatgpt/' + kind, wrapped(kind), methods=[method], include_in_schema=False)

    async def schema():
        path = Path(__file__).resolve().parents[1] / 'integrations' / 'chatgpt' / 'openapi.json'
        return JSONResponse(json.loads(path.read_text(encoding='utf-8')))

    app.add_api_route('/chatgpt/openapi.json', schema, methods=['GET'], include_in_schema=False)
