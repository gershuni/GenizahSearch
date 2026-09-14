"""Exercise the public endpoints used by the ChatGPT pilot (standard library only).

Run explicitly; this sends a few public example queries to GenizahSearch.
Responses are saved locally for contract validation, never printed in full.
"""
import argparse
import json
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, default=Path('scratch/chatgpt-smoke'))
    parser.add_argument('--api-prefix', choices=['/api', '/api/chatgpt'], default='/api/chatgpt',
                        help='Use /api only to diagnose the original, unbounded endpoints.')
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    checks = []

    def call(name, path, body=None, expected=200):
        data = json.dumps(body, ensure_ascii=False).encode('utf-8') if body is not None else None
        request = urllib.request.Request(
            'https://genizahsearch.com' + args.api_prefix + path.removeprefix('/api'), data=data,
            headers={'User-Agent': 'GenizahSearch-ChatGPT-Pilot/0.1',
                     'Accept': 'application/json', 'Content-Type': 'application/json'},
        )
        started = time.monotonic()
        status, retry_after, raw = None, None, ''
        try:
            try:
                response = urllib.request.urlopen(request, timeout=44)
            except urllib.error.HTTPError as exc:
                response = exc
            with response:
                status = response.status
                retry_after = response.headers.get('Retry-After')
                raw = response.read().decode('utf-8')
            result = json.loads(raw)
            failure = None
        except (OSError, ValueError) as exc:
            result = {}
            failure = str(exc)
        elapsed = round(time.monotonic() - started, 2)
        check = {
            'name': name, 'http_status': status, 'expected_status': expected,
            'seconds': elapsed, 'response_characters': len(raw),
            'within_actions_limits': elapsed < 45 and len(raw) < 100000,
            'count': result.get('count'), 'total': result.get('total'),
            'warnings': result.get('warnings', []), 'error': result.get('error', failure),
            'retry_after': retry_after,
            'passed': status == expected and failure is None and elapsed < 45 and len(raw) < 100000,
        }
        checks.append(check)
        print(json.dumps(check, ensure_ascii=True), flush=True)
        (args.output / (name + '.json')).write_text(
            json.dumps(result, ensure_ascii=False, indent=2), encoding='utf-8')
        return result

    call('capabilities', '/api/capabilities')
    search = call('phrase-search', '/api/search',
                  {'query': 'אתה זכור מעשה עולם', 'search_mode': 'exact', 'limit': 5})
    shelfmark = call('shelfmark-search', '/api/search',
                    {'query': 'ENA 1628.38', 'search_mode': 'shelfmark', 'limit': 5})
    rows = search.get('results', []) or shelfmark.get('results', [])
    if rows:
        locator = rows[0].get('locator', {})
        params = {k: locator[k] for k in ('sys_id', 'p_num', 'volume_ie') if locator.get(k)}
        if params.get('sys_id') and params.get('p_num'):
            call('browse', '/api/browse?' + urllib.parse.urlencode(dict(params, text_cap=4000)))
    if shelfmark.get('results'):
        passage = shelfmark['results'][0]['excerpt']
        call('passage', '/api/parallels', {'method': 'passage', 'text': passage})
        call('multi-witness', '/api/parallels', {'method': 'passage', 'witnesses': [
            {'label': 'Whole page', 'text': passage},
            {'label': 'Excerpt of same page (transport test, not an independent witness)', 'text': passage[:200]},
        ], 'sort': 'witness_count'})
    call('invalid-search', '/api/search',
         {'query': '', 'search_mode': 'exact', 'limit': 5}, expected=400)
    report = {'checked_at': datetime.now(timezone.utc).isoformat(),
              'base_url': 'https://genizahsearch.com' + args.api_prefix, 'checks': checks,
              'chatgpt_editor_tested': False,
              'note': 'Direct HTTP checks only; these do not prove ChatGPT can connect.'}
    (args.output / 'report.json').write_text(json.dumps(report, indent=2), encoding='utf-8')
    required = {'capabilities', 'phrase-search', 'shelfmark-search', 'browse', 'passage', 'multi-witness', 'invalid-search'}
    return 0 if all(check['passed'] for check in checks) and required <= {
        check['name'] for check in checks} else 1


if __name__ == '__main__':
    raise SystemExit(main())
