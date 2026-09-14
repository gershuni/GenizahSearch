"""Build small immutable result pages once, never during subsequent retrieval."""
from copy import deepcopy

from fastapi.responses import JSONResponse

PAGE_SIZE = 10
PAGE_BYTES = 80000
MAX_STORED_BYTES = 8 * 1024 * 1024


def build_pages(payload, job_id):
    """Return collection -> response bytes. Preserve order and complete spans.

    Limits fail explicitly rather than silently discarding later candidates.
    Only a single candidate page is copied at a time.
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
                if len(raw) <= PAGE_BYTES:
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
