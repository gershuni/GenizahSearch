# -*- coding: utf-8 -*-
"""Is the witness instrument cross-witness retrieval, or self-retrieval?

The concern. Most works in the oracle are attested through the edition's own
source-manuscript statement, so if a query passage of work W retrieves exactly
the one manuscript that edition was transcribed from, the instrument is a
clean-query/noisy-target recognition task -- realistic (paste a printed
edition, find the manuscript) but NOT evidence about finding an unrelated
witness. The two readings support different claims, so the split is measured
rather than assumed.

The discriminator: how many DISTINCT attested manuscripts appear in the top k.
Two or more cannot all be one edition's single source, so those queries are
genuinely cross-witness. One is ambiguous and is reported as such.

Latency printed here is not a benchmark -- this is run alongside other jobs.

Usage:
  python scripts/measure_witness_coverage.py --queries Q.jsonl --index IDX \
      --limit 300 [--k 50]
"""
from __future__ import annotations

import argparse
import collections
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from shared.passage_index import open_index          # noqa: E402
from shared.passage_policy import get_preset         # noqa: E402
from shared.retrieval_adapters import PassageRetriever  # noqa: E402
from shared.retrieval_eval import EvalQuery, split_queries  # noqa: E402


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--queries', required=True)
    ap.add_argument('--index', required=True)
    ap.add_argument('--preset', default='standard-40')
    ap.add_argument('--split', default='tune')
    ap.add_argument('--limit', type=int, default=300)
    ap.add_argument('--k', type=int, default=50)
    ap.add_argument('--out', default=None)
    args = ap.parse_args()

    queries = []
    with open(args.queries, encoding='utf-8') as fh:
        for line in fh:
            d = json.loads(line)
            queries.append(EvalQuery(
                query_id=d['query_id'], text=d['text'],
                positives=frozenset(d['positives']),
                strata=d.get('strata') or {}))
    chosen = split_queries(queries)[args.split]
    if args.limit and len(chosen) > args.limit:
        step = len(chosen) / args.limit
        chosen = [chosen[int(i * step)] for i in range(args.limit)]

    idx = open_index(args.index)
    if idx is None:
        print(f'index will not open: {args.index}', file=sys.stderr)
        return 2
    r = PassageRetriever(index=idx, policy=get_preset(args.preset))

    dist = collections.Counter()
    per_query = []
    for q in chosen:
        ranked = r.retrieve(q.text)[:args.k]
        # record ids are <sys_id>_<IE>_<P...>_<FL...>
        found = {rid.split('_', 1)[0] for rid in ranked if rid in q.positives}
        dist[min(len(found), 5)] += 1
        per_query.append({'query_id': q.query_id,
                          'n_attested_sys_found': len(found),
                          'n_returned': len(ranked)})

    n = len(chosen)
    hit = sum(v for k, v in dist.items() if k >= 1)
    multi = sum(v for k, v in dist.items() if k >= 2)
    print(f'n={n}  k={args.k}  preset={args.preset}')
    print(f'  queries finding >=1 attested manuscript : {hit:4d} '
          f'({hit / n:.1%})')
    print(f'  queries finding >=2 DISTINCT attested MS: {multi:4d} '
          f'({multi / n:.1%})   <- cannot be one edition source')
    print('  distinct attested manuscripts in top k:')
    for key in sorted(dist):
        label = f'{key}' if key < 5 else '5+'
        print(f'    {label:>2} : {dist[key]:4d}')
    if hit:
        print(f'  among the {hit} hitting queries, '
              f'{multi / hit:.1%} are multi-witness')

    if args.out:
        with open(args.out, 'w', encoding='utf-8') as fh:
            json.dump({'n': n, 'k': args.k, 'preset': args.preset,
                       'hit': hit, 'multi': multi,
                       'distribution': {str(k): v for k, v in dist.items()},
                       'per_query': per_query}, fh, ensure_ascii=False)
        print(f'wrote {args.out}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
