# -*- coding: utf-8 -*-
"""Query-latency acceptance table for the passage index (Phase 142, part 2).

Measures what the plan owes: warm and COLD cache p50/p95/p99 at each query
length, per policy, plus postings/candidates/verifications so a slow tail is
attributable rather than mysterious.

Queries are drawn FROM THE INDEX ITSELF, deterministically: record streams at
fixed strides, cut to the target length. That makes every query a guaranteed
true positive (its own record must come back), so the same run doubles as a
self-retrieval recall check -- rank of the true record is reported alongside
latency. Self-retrieval is a mechanism check, not the user-task measurement;
the method comparison instruments are separate.

Cold-cache mode: on Windows there is no portable page-cache drop, so cold is
approximated by touching a large decoy file between queries (--cold-touch-gb)
and reported as such -- honestly labelled approximation beats a fake number.

Usage:
  python scripts/bench_passage_query.py --index C:/GenizahSearch/passage_index/full_v1
  python scripts/bench_passage_query.py --index ... --policies standard-40,flat-25
"""
from __future__ import annotations

import argparse
import json
import os
import statistics
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from shared.passage_index import open_index  # noqa: E402
from shared.passage_policy import PRESETS, get_preset  # noqa: E402
from shared.passage_search import search_passage  # noqa: E402

QUERY_LETTERS = (50, 100, 200, 400, 1000, 2000, 8000)
N_QUERIES_PER_LEN = 12


def _pct(xs, q):
    xs = sorted(xs)
    if not xs:
        return 0.0
    k = min(len(xs) - 1, max(0, int(round(q / 100 * (len(xs) - 1)))))
    return xs[k]


def draw_queries(idx, length: int, n: int):
    """Deterministic: records at fixed strides, one cut each, skipping
    records too short to yield the target length plus flank."""
    out = []
    stride = max(1, idx.n_records // (n * 7))
    ri = 0
    while len(out) < n and ri < idx.n_records:
        s = idx.stream(ri)
        if len(s) >= length + 40:
            start = (len(s) - length) // 3
            out.append((ri, s[start:start + length]))
        ri += stride
    return out


def run_policy(idx, policy, cold_touch_gb: float, decoy_path: str):
    rows = []
    for length in QUERY_LETTERS:
        queries = draw_queries(idx, length, N_QUERIES_PER_LEN)
        if not queries:
            continue
        lat, ranks, reports = [], [], []
        for ri, q in queries:
            if cold_touch_gb > 0:
                _touch(decoy_path, cold_touch_gb)
            t0 = time.perf_counter()
            hits, rep = search_passage(idx, q, policy)
            lat.append((time.perf_counter() - t0) * 1000)
            rank = next((k for k, h in enumerate(hits) if h.record == ri),
                        None)
            ranks.append(rank)
            reports.append(rep)
        found = [r for r in ranks if r is not None]
        rows.append({
            'query_letters': length,
            'n': len(queries),
            'p50_ms': round(_pct(lat, 50), 1),
            'p95_ms': round(_pct(lat, 95), 1),
            'p99_ms': round(_pct(lat, 99), 1),
            'max_ms': round(max(lat), 1),
            'self_found': len(found),
            'self_rank0': sum(1 for r in ranks if r == 0),
            'mean_postings': int(statistics.mean(
                r.postings_admitted for r in reports)),
            'mean_candidates': int(statistics.mean(
                r.candidates for r in reports)),
            'mean_verified': int(statistics.mean(
                r.verified for r in reports)),
            'any_truncated': any(r.candidates_truncated or r.verify_truncated
                                 for r in reports),
        })
    return rows


def _touch(path: str, gb: float):
    """Read a decoy file to evict the index from the page cache. An
    approximation of cold cache, and labelled as one."""
    if not os.path.exists(path):
        with open(path, 'wb') as fh:
            fh.truncate(int(gb * 1e9))
    with open(path, 'rb') as fh:
        while fh.read(1 << 24):
            pass


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--index', required=True)
    ap.add_argument('--policies', default='standard-40,flat-25')
    ap.add_argument('--cold-touch-gb', type=float, default=0.0,
                    help='>0: read a decoy file this big between queries '
                         '(approximate cold cache; labelled as such)')
    ap.add_argument('--json', default=None)
    args = ap.parse_args()

    idx = open_index(args.index)
    if idx is None:
        print(f'index will not open: {args.index}', file=sys.stderr)
        return 2
    print(f'index: {idx.n_records:,} records, {idx.n_postings:,} postings')
    mode = (f'COLD (approx: {args.cold_touch_gb} GB decoy read between '
            f'queries)' if args.cold_touch_gb > 0 else 'WARM')
    print(f'mode: {mode}\n')

    decoy = os.path.join(os.environ.get('TEMP', '.'), 'passage_cold_decoy.bin')
    all_rows = {}
    for name in args.policies.split(','):
        policy = get_preset(name.strip()) if name.strip() in PRESETS \
            else get_preset(name.strip())
        rows = run_policy(idx, policy, args.cold_touch_gb, decoy)
        all_rows[policy.name] = rows
        print(f'=== {policy.name}  ({policy.policy_id}) ===')
        print(f'{"len":>6} {"n":>3} {"p50":>8} {"p95":>8} {"p99":>8} '
              f'{"max":>8} {"self@0":>7} {"postings":>10} {"cand":>9} '
              f'{"verif":>8} {"trunc":>6}')
        for r in rows:
            print(f'{r["query_letters"]:>6} {r["n"]:>3} '
                  f'{r["p50_ms"]:>7}ms {r["p95_ms"]:>7}ms '
                  f'{r["p99_ms"]:>7}ms {r["max_ms"]:>7}ms '
                  f'{r["self_rank0"]:>3}/{r["self_found"]:<3} '
                  f'{r["mean_postings"]:>10,} {r["mean_candidates"]:>9,} '
                  f'{r["mean_verified"]:>8,} '
                  f'{"YES" if r["any_truncated"] else "-":>6}')
        print()

    if args.json:
        with open(args.json, 'a', encoding='utf-8') as fh:
            fh.write(json.dumps({'index': args.index, 'mode': mode,
                                 'results': all_rows}) + '\n')
        print(f'appended to {args.json}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
