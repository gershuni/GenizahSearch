"""Build small immutable result pages once, never during subsequent retrieval."""
import json
from copy import deepcopy

from fastapi.responses import JSONResponse

PAGE_SIZE = 10
PAGE_BYTES = 80000
MAX_STORED_BYTES = 8 * 1024 * 1024

# MCP clients (docs/plans/MCP_SERVER_PLAN.md D9). A page reaches an MCP client as
# the text of a JSON-RPC result, and the 2026-07-28 SDK serializer writes it with
# json.dumps' default ensure_ascii=True: every Hebrew character becomes a 6-byte
# \uXXXX and every quote is escaped again. An 80,000-byte Hebrew page measured
# 239,113 bytes on the wire. So MCP pages are sized by that escaped form, never by
# raw bytes, for both protocol generations (a stored page may be polled under either).
MCP_RESPONSE_BYTES = 100_000
MCP_ENVELOPE_RESERVE = 2_000
MCP_PAGE_BYTES = MCP_RESPONSE_BYTES - MCP_ENVELOPE_RESERVE


def mcp_escaped_size(raw: bytes) -> int:
    """Bytes ``raw`` (UTF-8 JSON) occupies once embedded as an ASCII-escaped JSON string."""
    return len(json.dumps(raw.decode('utf-8')))


def page_budget(channel):
    """``(page_bytes, measure)`` for build_pages: MCP's escaped budget, else the GPT's."""
    if channel == 'mcp':
        return MCP_PAGE_BYTES, mcp_escaped_size
    return PAGE_BYTES, len


def build_pages(payload, job_id, *, page_bytes=PAGE_BYTES, measure=len):
    """Return collection -> response bytes. Preserve order and complete spans.

    Limits fail explicitly rather than silently discarding later candidates.
    Only a single candidate page is copied at a time. ``measure`` sizes a page
    against ``page_bytes``; storage is always counted in actual stored bytes.
    """
    envelope = {k: v for k, v in payload.items() if k not in ('results', 'filtered', 'count')}
    collections = {'results': payload['results'], 'filtered': payload.get('filtered', [])}
    pages = {}
    stored = 0
    for collection, rows in collections.items():
        result = []
        offset = 0
        while offset < len(rows) or not result:
            candidates = deepcopy(rows[offset:offset + PAGE_SIZE])
            for row in candidates:
                if len(row.get('matches', [])) > 3:
                    row['matches_available'] = len(row['matches'])
                    row['matches'] = row['matches'][:3]
            while True:
                end = offset + len(candidates)
                data = {**envelope, 'count': len(candidates),
                        'results': candidates if collection == 'results' else [],
                        'filtered': candidates if collection == 'filtered' else [],
                        'pagination': {'job_id': job_id, 'collection': collection,
                                       'page': len(result), 'offset': offset,
                                       'returned': len(candidates), 'available': len(rows),
                                       'results_available': len(collections['results']),
                                       'filtered_available': len(collections['filtered']),
                                       'next_page': len(result) + 1 if end < len(rows) else None},
                        'warnings': list(envelope.get('warnings', [])) + [{
                            'code': 'chatgpt_paginated',
                            'message': 'More candidates are available when next_page is set. Match spans are limited to three per candidate; matches_available reports additional spans.'}]}
                # count consistently counts main results, even on filtered pages.
                data['count'] = len(data['results'])
                raw = JSONResponse(data).body
                if measure(raw) <= page_bytes:
                    break
                if len(candidates) <= 1:
                    raise ValueError('A result or its fixed metadata exceeds the page allowance. Narrow the query or use the website.')
                candidates.pop()
            stored += len(raw)
            if stored > MAX_STORED_BYTES:
                raise ValueError('Saved result pages exceed the storage allowance. Narrow the query or use the website.')
            result.append(raw)
            offset = end
            if offset >= len(rows):
                break
        pages[collection] = result
    return pages, stored
