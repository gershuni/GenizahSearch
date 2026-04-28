#!/usr/bin/env python3
"""Phase 78 D-22 form 2: live soak test against /api/search.

Hits a live deployment at a sustained rate, observes 429 + Retry-After end-to-end
(through nginx, exercising the X-Forwarded-For loopback resolution). Run manually
as part of phase-gate verification; not part of CI.

Usage:
    python scripts/soak_search_api.py --url https://genizahsearch.com/api/search \\
                                       --rate 60 --duration 120

Exits 0 if at least one 429 was observed AND every 429 carried a parseable
Retry-After header AND the rate_limited error code. Exits 1 otherwise.
"""

import argparse
import sys
import time
import json
import requests


def main() -> int:
    p = argparse.ArgumentParser(description='Live soak test for Phase 78 /api/search rate limiter.')
    p.add_argument('--url', default='https://genizahsearch.com/api/search',
                   help='Full URL of /api/search to soak. Default: production.')
    p.add_argument('--rate', type=int, default=60,
                   help='Target requests per minute. Default: 60.')
    p.add_argument('--duration', type=int, default=120,
                   help='Seconds to run. Default: 120.')
    p.add_argument('--query', default='soak',
                   help='Query string to send. Default: "soak".')
    p.add_argument('--mode', default='text',
                   help='Search mode. Default: text.')
    p.add_argument('--limit', type=int, default=1,
                   help='Result limit per request. Default: 1.')
    p.add_argument('--verbose', '-v', action='store_true',
                   help='Print every response status.')
    args = p.parse_args()

    body = {'query': args.query, 'mode': args.mode, 'limit': args.limit}
    interval = 60.0 / max(1, args.rate)
    deadline = time.monotonic() + args.duration

    counts = {'2xx': 0, '4xx_429': 0, '4xx_other': 0, '5xx': 0, 'network_error': 0}
    first_429_retry_after = None
    retry_after_observations: list[int] = []
    error_codes: dict[str, int] = {}

    print(f"Soaking {args.url} at {args.rate} req/min for {args.duration}s "
          f"(interval={interval:.3f}s/request)...", file=sys.stderr)

    request_count = 0
    while time.monotonic() < deadline:
        t0 = time.monotonic()
        request_count += 1
        try:
            r = requests.post(args.url, json=body, timeout=10)
        except requests.RequestException as exc:
            counts['network_error'] += 1
            if args.verbose:
                print(f"[{request_count}] NET ERROR {type(exc).__name__}: {exc}",
                      file=sys.stderr)
            time.sleep(interval)
            continue

        if 200 <= r.status_code < 300:
            counts['2xx'] += 1
            if args.verbose:
                print(f"[{request_count}] {r.status_code}", file=sys.stderr)
        elif r.status_code == 429:
            counts['4xx_429'] += 1
            ra = r.headers.get('Retry-After')
            if first_429_retry_after is None:
                first_429_retry_after = ra
            try:
                retry_after_observations.append(int(ra))
            except (TypeError, ValueError):
                pass
            try:
                ec = r.json().get('error', {}).get('code', '<no_code>')
            except (json.JSONDecodeError, ValueError):
                ec = '<not_json>'
            error_codes[ec] = error_codes.get(ec, 0) + 1
            if args.verbose:
                print(f"[{request_count}] 429 Retry-After={ra} code={ec}",
                      file=sys.stderr)
        elif 400 <= r.status_code < 500:
            counts['4xx_other'] += 1
            if args.verbose:
                print(f"[{request_count}] {r.status_code}", file=sys.stderr)
        else:
            counts['5xx'] += 1
            if args.verbose:
                print(f"[{request_count}] {r.status_code}", file=sys.stderr)

        elapsed = time.monotonic() - t0
        sleep_for = interval - elapsed
        if sleep_for > 0:
            time.sleep(sleep_for)

    print()
    print("=== Soak Summary ===")
    print(f"URL: {args.url}")
    print(f"Target rate: {args.rate}/min, duration: {args.duration}s, sent: {request_count}")
    print(f"Counts: {counts}")
    print(f"First 429 Retry-After: {first_429_retry_after!r}")
    if retry_after_observations:
        print(f"Retry-After observations (n={len(retry_after_observations)}): "
              f"min={min(retry_after_observations)}, max={max(retry_after_observations)}, "
              f"first 5={retry_after_observations[:5]}")
    print(f"Error codes seen: {error_codes}")

    if counts['4xx_429'] == 0:
        print("FAIL: no 429 observed -- either rate too low for SEARCH_API_RATE_LIMIT, "
              "or the rate limiter is not active.")
        return 1
    bad = [v for v in retry_after_observations if v < 1]
    if bad or len(retry_after_observations) != counts['4xx_429']:
        print(f"FAIL: Retry-After missing or invalid on some 429s "
              f"(observations={len(retry_after_observations)}, expected={counts['4xx_429']}, "
              f"invalid={bad}).")
        return 1
    if 'rate_limited' not in error_codes:
        print(f"FAIL: 429 envelopes missing the 'rate_limited' code; got {error_codes}.")
        return 1
    print("PASS: 429 + honest Retry-After + rate_limited envelope all observed.")
    return 0


if __name__ == '__main__':
    sys.exit(main())
