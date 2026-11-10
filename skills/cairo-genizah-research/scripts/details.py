"""Fetch one page of manuscript catalog, bibliography or source-credit records."""
from __future__ import annotations

import argparse
import json
import re
import sys

import requests

try:
    from . import _config, throttle
except ImportError:
    import os
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    import _config, throttle


SECTIONS = ('fjms_catalog', 'fjms_bibliography', 'fjms_catalog_refs',
            'nli_catalog', 'nli_bibliography', 'pgp_sources', 'fgp_sources')


def call_details(*, sys_id: str, section: str, offset: int = 0,
                 snapshot: str | None = None, base_url: str | None = None,
                 timeout: float = 30.0) -> dict:
    """Single request; preserve errors and continuation fields without retrying."""
    if (not isinstance(sys_id, str) or not re.fullmatch(r'[A-Za-z0-9_-]{1,100}', sys_id)
            or section not in SECTIONS or type(offset) is not int or offset < 0
            or (offset > 0 and not snapshot)):
        return {'error': {'code': 'invalid_request', 'message':
                         'Use a returned sys_id, supported section and nonnegative offset; later pages need snapshot.'}}
    params = {'sys_id': sys_id, 'section': section, 'offset': offset}
    if snapshot:
        params['snapshot'] = snapshot
    # Separate from browse/search pacing: below the default 30 rpm details limit.
    throttle.acquire('details', rpm=min(24, _config.get_rpm()), burst=1)
    try:
        response = requests.get(_config.resolve_base_url(base_url) + '/api/chatgpt/manuscript-details',
                                params=params, timeout=timeout)
    except requests.exceptions.Timeout:
        return {'error': {'code': 'transport_timeout', 'message':
                         'Details request timed out; no complete response received.'}}
    except requests.exceptions.RequestException as exc:
        return {'error': {'code': 'connection_error', 'message': str(exc)}}
    try:
        data = response.json()
    except ValueError:
        data = None
    if not isinstance(data, dict):
        data = {'error': {'code': 'invalid_response', 'message': 'Expected a JSON object from details endpoint.'}}
    if response.status_code >= 400 and not isinstance(data.get('error'), dict):
        data = {'error': {'code': 'http_error', 'message': 'Details endpoint rejected the request.'}}
    if isinstance(data.get('error'), dict):
        data['error']['http_status'] = response.status_code
        if 'Retry-After' in response.headers:
            data['error']['retry_after'] = response.headers['Retry-After']
    return data


def _main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--sys-id', required=True)
    parser.add_argument('--section', required=True, choices=SECTIONS)
    parser.add_argument('--offset', type=int, default=0)
    parser.add_argument('--snapshot')
    parser.add_argument('--base-url')
    args = parser.parse_args(argv)
    result = call_details(**vars(args))
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 1 if 'error' in result else 0


if __name__ == '__main__':
    sys.exit(_main())
