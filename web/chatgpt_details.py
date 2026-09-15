"""Bounded, cached, local-only manuscript details for GPT Actions."""
import asyncio
from collections import OrderedDict
from datetime import datetime, timezone
import json
import re
import secrets
import time

from fastapi import Request
from fastapi.responses import JSONResponse, Response

from shared.manuscript_details import SECTIONS, load_section

MAX_SECTION_BYTES = 1024 * 1024
MAX_CACHE_BYTES = 8 * 1024 * 1024
MAX_ENTRIES = 32
MAX_ACTIVE = 2
TTL = 600
WAIT_SECONDS = 10
PAGE_BYTES = 70000
PAGE_ROWS = 10


def encode(value):
    return json.dumps(value, ensure_ascii=False, separators=(',', ':'), allow_nan=False).encode('utf-8')


def prepare_section(sys_id, section, loader):
    rows, availability = loader(sys_id, section)
    encoded = []
    size = 0
    for row in rows:
        raw = encode(row)
        size += len(raw)
        if size > MAX_SECTION_BYTES or len(raw) > PAGE_BYTES:
            raise ValueError('Section exceeds the transfer allowance; consult the website.')
        encoded.append(raw)
    return {'rows': encoded, 'bytes': size, 'availability': availability,
            'snapshot': secrets.token_urlsafe(18), 'created': time.monotonic(),
            'generated_at': datetime.now(timezone.utc).isoformat()}


def load_web_section(sys_id, section):
    """Supply the web application's local cache to the shared record reader."""
    if section.startswith('nli_'):
        from web.state import state
        return load_section(sys_id, section, nli_cache=getattr(state.meta_mgr, 'nli_cache', None))
    return load_section(sys_id, section)


def register_manuscript_details(app, owner, loader=load_web_section, limiter=None):
    cache = OrderedDict()
    active = {}

    def error(code, message, status, headers=None):
        return JSONResponse({'error': {'code': code, 'message': message}}, status_code=status,
                            headers={'Cache-Control': 'no-store', **(headers or {})})

    async def load(key):
        try:
            record = await asyncio.to_thread(prepare_section, *key, loader)
            while cache and (len(cache) >= MAX_ENTRIES or
                             sum(item['bytes'] for item in cache.values()) + record['bytes'] > MAX_CACHE_BYTES):
                cache.popitem(last=False)
            cache[key] = record
            return record
        finally:
            active.pop(key, None)

    async def details(request: Request):
        nonlocal limiter
        caller = owner(request)
        if isinstance(caller, Response):
            return caller
        from shared.api_errors import APIError
        if limiter is None:
            from web.api_hardening import RateLimiter
            limiter = RateLimiter(default_limit=30)
        try:
            limiter.check(caller)
        except APIError as exc:
            return error(exc.code, str(exc), exc.http_status, getattr(exc, 'headers', {}))
        query = request.query_params
        sys_id = query.get('sys_id', '')
        section = query.get('section', '')
        try:
            offset = int(query.get('offset', '0'))
            if (set(query) - {'sys_id', 'section', 'offset', 'snapshot'} or
                    not re.fullmatch(r'[A-Za-z0-9_-]{1,100}', sys_id) or
                    section not in SECTIONS or offset < 0):
                raise ValueError()
        except ValueError:
            return error('invalid_request', 'Use a returned sys_id, a supported section and nonnegative offset.', 400)
        key = (sys_id, section)
        for old_key, item in list(cache.items()):
            if time.monotonic() - item['created'] >= TTL:
                del cache[old_key]
        record = cache.get(key)
        snapshot = query.get('snapshot')
        if (offset and not snapshot) or (snapshot and (record is None or record['snapshot'] != snapshot)):
            return error('details_expired', 'This snapshot expired or was evicted. Restart at offset 0; do not combine snapshots.', 409)
        if record is None:
            if key not in active:
                if len(active) >= MAX_ACTIVE:
                    return error('details_busy', 'Details retrieval is busy; retry later.', 503, {'Retry-After': '5'})
                active[key] = asyncio.create_task(load(key))
                # Retrieve failures even if the caller disconnects or times out.
                active[key].add_done_callback(lambda task: None if task.cancelled() else task.exception())
            try:
                record = await asyncio.wait_for(asyncio.shield(active[key]), timeout=WAIT_SECONDS)
            except TimeoutError:
                return error('details_pending', 'Local details retrieval is still running; retry the same request later.', 503, {'Retry-After': '5'})
            except ValueError as exc:
                return error('details_too_large', str(exc), 503)
            except Exception:
                return error('details_unavailable', 'The local source lookup failed; no conclusion about records can be drawn.', 503)
        rows = record['rows']
        if offset > len(rows) or (offset == len(rows) and offset != 0):
            return error('invalid_offset', 'Follow next_offset from the preceding page.', 400)
        selected = []
        size = 0
        for raw in rows[offset:offset + PAGE_ROWS]:
            if size + len(raw) > PAGE_BYTES:
                break
            selected.append(json.loads(raw))
            size += len(raw)
        next_offset = offset + len(selected)
        warnings = [{'code': 'local_source_coverage', 'message':
                     'Local records only, not an exhaustive bibliography. Empty results do not establish absence; legacy source lookups may also return no rows on failure.'}]
        if record['availability'] in ('unavailable', 'not_cached'):
            warnings.append({'code': record['availability'], 'message': 'This source is unavailable locally; no external request was made.'})
        return JSONResponse({
            'sys_id': sys_id, 'section': section, 'provider': section.split('_')[0].upper(),
            'availability': record['availability'], 'generated_at': record['generated_at'],
            'snapshot': record['snapshot'], 'offset': offset, 'returned': len(selected),
            'available': len(rows), 'next_offset': next_offset if next_offset < len(rows) else None,
            'records': selected, 'warnings': warnings,
        }, headers={'Cache-Control': 'no-store'})

    app.add_api_route('/chatgpt/manuscript-details', details, methods=['GET'], include_in_schema=False)
