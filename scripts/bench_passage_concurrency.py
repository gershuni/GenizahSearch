# -*- coding: utf-8 -*-
"""Concurrency and cold-start latency for the passage index -- the web SLO input.

Two measurements the single-query benchmarks cannot give:

CONCURRENCY. Production is one uvicorn worker dispatching reads to a thread
pool, so contention shows up as threads inside ONE process -- which is exactly
what this measures (thread pool, shared mmap). Reported per level: p50/p95 of
individual query latency, plus aggregate throughput. If p95 at concurrency 8
is many times the concurrency-1 p95, the discovery-budget lesson applies here
too and the Phase 145 executor needs its own bounded budget sized accordingly.

COLD-ISH START. True cold cache needs a reboot; what a deploy or a quiet-night
eviction actually produces is "index pages evicted by other I/O". Simulated
honestly: read N GB of UNRELATED large files (the Tantivy index) to pressure
the page cache, then time, in a FRESH subprocess, open_index + the first
queries individually. Labelled cold-ish everywhere, because that is what it
is. The real SLO must be re-measured on the production box before Phase 145
ships; this bounds the dev-box shape of the problem.

Usage:
  python scripts/bench_passage_concurrency.py --index IDX --queries Q.jsonl \
      [--n 60] [--levels 1,4,8] [--pressure-gb 24] [--cold-runs 3] \
      [--pressure-dir C:/Users/gersh/Genizah_Tantivy_Index] [--out out.json]
  (internal) --cold-worker: run one cold trial and print JSON; used by the
  parent via subprocess so each trial starts with nothing mapped.
"""
from __future__ import annotations

import argparse
import concurrent.futures
import json
import os
import statistics
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def load_query_texts(path: str, n: int) -> list:
    """n evenly spaced tune-split query texts -- same draw rule as the eval."""
    from shared.retrieval_eval import SPLIT_TUNE, EvalQuery, split_queries
    qs = []
    with open(path, encoding='utf-8') as fh:
        for line in fh:
            d = json.loads(line)
            qs.append(EvalQuery(query_id=d['query_id'], text=d['text'],
                                positives=frozenset()))
    tune = split_queries(qs)[SPLIT_TUNE]
    step = max(1.0, len(tune) / n)
    return [tune[int(i * step)].text for i in range(min(n, len(tune)))]


def pct(xs: list, p: float) -> float:
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(p * len(xs)))]


def run_concurrency(index_dir: str, texts: list, levels: list) -> dict:
    from shared.passage_index import open_index
    from shared.passage_policy import get_preset
    from shared.passage_search import search_passage
    idx = open_index(index_dir)
    if idx is None:
        raise SystemExit(f'index will not open: {index_dir}')
    policy = get_preset('standard-40')

    def one(text: str) -> float:
        t0 = time.perf_counter()
        search_passage(idx, text, policy)
        return (time.perf_counter() - t0) * 1000

    # Warm the mmap once so concurrency levels measure contention, not paging.
    for t in texts[:8]:
        one(t)

    out = {}
    for level in levels:
        lat = []
        t0 = time.perf_counter()
        with concurrent.futures.ThreadPoolExecutor(max_workers=level) as ex:
            for ms in ex.map(one, texts):
                lat.append(ms)
        wall = time.perf_counter() - t0
        out[str(level)] = {
            'n': len(lat),
            'p50_ms': round(statistics.median(lat), 1),
            'p95_ms': round(pct(lat, 0.95), 1),
            'max_ms': round(max(lat), 1),
            'throughput_qps': round(len(lat) / wall, 2),
        }
        print(f'concurrency {level}: p50={out[str(level)]["p50_ms"]}ms '
              f'p95={out[str(level)]["p95_ms"]}ms '
              f'qps={out[str(level)]["throughput_qps"]}', flush=True)
    return out


def pressure_cache(pressure_dir: str, gb: float) -> float:
    """Sequentially read unrelated large files to evict index pages."""
    target = int(gb * 2**30)
    done = 0
    files = []
    for root, _dirs, names in os.walk(pressure_dir):
        for nm in names:
            p = os.path.join(root, nm)
            try:
                sz = os.path.getsize(p)
            except OSError:
                continue
            if sz > 64 * 2**20:
                files.append((p, sz))
    files.sort(key=lambda x: -x[1])
    t0 = time.perf_counter()
    buf = bytearray(16 * 2**20)
    for p, _sz in files:
        with open(p, 'rb', buffering=0) as fh:
            while done < target:
                k = fh.readinto(buf)
                if not k:
                    break
                done += k
        if done >= target:
            break
    print(f'cache pressure: read {done / 2**30:.1f} GB '
          f'in {time.perf_counter() - t0:.0f}s', flush=True)
    return done / 2**30


def pressure_alloc(gb: float) -> None:
    """Hold and touch ~gb of anonymous memory, then free it.

    File reads alone cannot evict a mapped index from a 63 GB standby list;
    only allocation pressure forces Windows to reclaim standby pages. Chunked
    4 GB at a time so a commit-limit failure degrades to partial pressure
    instead of an exception.
    """
    chunks = []
    held = 0.0
    try:
        while held < gb:
            take = min(4.0, gb - held)
            try:
                b = bytearray(int(take * 2**30))
            except MemoryError:
                print(f'  alloc pressure capped at {held:.0f} GB '
                      '(commit limit)', flush=True)
                break
            # touch one byte per page so the pages are really committed
            for off in range(0, len(b), 4096):
                b[off] = 1
            chunks.append(b)
            held += take
        print(f'  alloc pressure held {held:.0f} GB', flush=True)
    finally:
        chunks.clear()


def cold_worker(index_dir: str, texts: list) -> None:
    """One fresh-process trial: time open + each of the first queries."""
    from shared.passage_index import open_index
    from shared.passage_policy import get_preset
    from shared.passage_search import search_passage
    t0 = time.perf_counter()
    idx = open_index(index_dir)
    open_ms = (time.perf_counter() - t0) * 1000
    if idx is None:
        print(json.dumps({'error': 'index will not open'}))
        return
    policy = get_preset('standard-40')
    lat = []
    for text in texts:
        t1 = time.perf_counter()
        search_passage(idx, text, policy)
        lat.append(round((time.perf_counter() - t1) * 1000, 1))
    print(json.dumps({'open_ms': round(open_ms, 1), 'query_ms': lat}))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--index', required=True)
    ap.add_argument('--queries', required=True)
    ap.add_argument('--n', type=int, default=60)
    ap.add_argument('--levels', default='1,4,8')
    ap.add_argument('--pressure-gb', type=float, default=24.0)
    ap.add_argument('--pressure-alloc-gb', type=float, default=0.0)
    ap.add_argument('--pressure-dir',
                    default=os.path.expanduser('~/Genizah_Tantivy_Index'))
    ap.add_argument('--cold-runs', type=int, default=3)
    ap.add_argument('--cold-queries', type=int, default=5)
    ap.add_argument('--out', default=None)
    ap.add_argument('--cold-worker', action='store_true')
    args = ap.parse_args()

    texts = load_query_texts(args.queries, args.n)
    if args.cold_worker:
        cold_worker(args.index, texts[:args.cold_queries])
        return 0

    try:
        import psutil
        ram_gb = round(psutil.virtual_memory().total / 2**30, 1)
    except ImportError:
        ram_gb = None
    print(f'machine RAM: {ram_gb} GB   queries: {len(texts)}', flush=True)

    report = {'ram_gb': ram_gb, 'n_queries': len(texts)}
    levels = [int(x) for x in args.levels.split(',')]
    report['warm_concurrency'] = run_concurrency(args.index, texts, levels)

    colds = []
    for i in range(args.cold_runs):
        if not os.path.isdir(args.pressure_dir):
            print(f'pressure dir missing: {args.pressure_dir} -- '
                  'cold-ish trials skipped', flush=True)
            break
        pressure_cache(args.pressure_dir, args.pressure_gb)
        if args.pressure_alloc_gb > 0:
            pressure_alloc(args.pressure_alloc_gb)
        r = subprocess.run(
            [sys.executable, '-X', 'utf8', os.path.abspath(__file__),
             '--index', args.index, '--queries', args.queries,
             '--n', str(args.n), '--cold-queries', str(args.cold_queries),
             '--cold-worker'],
            capture_output=True, text=True, encoding='utf-8')
        line = (r.stdout or '').strip().splitlines()[-1:] or ['{}']
        try:
            d = json.loads(line[0])
        except json.JSONDecodeError:
            d = {'error': r.stdout[-300:] + r.stderr[-300:]}
        colds.append(d)
        print(f'cold-ish trial {i + 1}: {d}', flush=True)
    report['cold_ish'] = colds

    if args.out:
        with open(args.out, 'w', encoding='utf-8') as fh:
            json.dump(report, fh)
        print(f'wrote {args.out}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
