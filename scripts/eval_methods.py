# -*- coding: utf-8 -*-
"""Run the method comparison: incumbent chunk search vs passage matching.

Both methods answer identical queries through identical code
(shared/retrieval_eval.py), page-scoped, in record-id units. Both are swept --
tuning one method against the other's defaults would be a rigged comparison,
and the incumbent's own default max_freq=10 is aggressive enough to return
nothing on a verbatim query.

The tuning split is for sweeping. The holdout split is write-once per
(method, config) and the ledger enforces it, so a swept winner cannot be
quietly re-reported as a pre-registered result.

Usage:
  python scripts/eval_methods.py --queries Q.jsonl --index IDX --limit 150
  python scripts/eval_methods.py ... --split holdout --configs passage:standard-40
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from shared.passage_index import open_index  # noqa: E402
from shared.passage_policy import get_preset  # noqa: E402
from shared.retrieval_adapters import (  # noqa: E402
    ChunkRetriever, PassageRetriever, eligible_record_ids,
)
from shared.retrieval_eval import (  # noqa: E402
    SPLIT_HOLDOUT, SPLIT_TUNE, EvalLedger, EvalQuery, evaluate, non_inferior,
    split_queries, summarize, summarize_by_stratum,
)

K_VALUES = (1, 10, 50, 200)
STRATA = ('length_band', 'pages_in_sys')


def load_queries(path: str) -> list:
    out = []
    with open(path, encoding='utf-8') as fh:
        for line in fh:
            d = json.loads(line)
            out.append(EvalQuery(query_id=d['query_id'], text=d['text'],
                                 positives=frozenset(d['positives']),
                                 strata=d.get('strata') or {}))
    return out


def build_retrievers(spec: str, index_dir: str,
                     equal_eligibility: bool = True) -> list:
    """spec: comma-separated 'passage:<preset>' / 'chunk:<size>:<mode>:<freq>'.

    equal_eligibility restricts the incumbent to the records the passage index
    actually holds. Without it the two methods search different corpora --
    948,549 records against 702,466 -- and the comparison measures document
    sets rather than methods.
    """
    out = []
    engine = None
    idx = None
    eligible = None
    for item in spec.split(','):
        item = item.strip()
        if not item:
            continue
        kind, _, rest = item.partition(':')
        if kind == 'passage':
            if idx is None:
                idx = open_index(index_dir)
                if idx is None:
                    raise SystemExit(f'index will not open: {index_dir}')
            out.append(PassageRetriever(index=idx, policy=get_preset(rest)))
        elif kind == 'chunk':
            if engine is None:
                from shared.metadata_manager import MetadataManager
                from shared.search_engine import SearchEngine
                from shared.variants import VariantManager
                engine = SearchEngine(MetadataManager(), VariantManager())
                if engine.searcher is None:
                    raise SystemExit(
                        'Tantivy index failed to open -- composition search '
                        'would return empty results SILENTLY')
            if equal_eligibility and eligible is None:
                if idx is None:
                    idx = open_index(index_dir)
                    if idx is None:
                        raise SystemExit(f'index will not open: {index_dir}')
                eligible = eligible_record_ids(idx)
                print(f'equal-eligibility set: {len(eligible):,} records',
                      flush=True)
            size, mode, freq = (rest.split(':') + ['exact', '100'])[:3]
            out.append(ChunkRetriever(engine=engine, chunk_size=int(size),
                                      mode=mode, max_freq=int(freq),
                                      eligible=eligible))
        else:
            raise SystemExit(f'unknown retriever kind: {kind!r}')
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--queries', required=True)
    ap.add_argument('--index', required=True)
    ap.add_argument('--configs',
                    default='passage:standard-40,passage:flat-25,'
                            'chunk:5:exact:100,chunk:3:exact:100')
    ap.add_argument('--split', default=SPLIT_TUNE,
                    choices=[SPLIT_TUNE, SPLIT_HOLDOUT])
    ap.add_argument('--limit', type=int, default=150)
    ap.add_argument('--ledger', default=None)
    ap.add_argument('--baseline', default=None,
                    help='config_id to treat as incumbent for non-inferiority')
    ap.add_argument('--force', action='store_true')
    ap.add_argument('--all-docs', action='store_true',
                    help='do NOT restrict the incumbent to the '
                         'shared document set (compares products, '
                         'not methods)')
    args = ap.parse_args()

    queries = load_queries(args.queries)
    chosen = split_queries(queries)[args.split]
    # Deterministic subsample: evenly spaced, so a --limit run is a spread of
    # the population rather than its head.
    if args.limit and len(chosen) > args.limit:
        step = len(chosen) / args.limit
        chosen = [chosen[int(i * step)] for i in range(args.limit)]
    print(f'{len(queries):,} queries loaded; split={args.split}; '
          f'running {len(chosen):,}')

    ledger = EvalLedger(args.ledger) if args.ledger else None
    results = {}
    for r in build_retrievers(args.configs, args.index,
                              equal_eligibility=not args.all_docs):
        cid = r.config_id
        t0 = time.time()
        outs = evaluate(chosen, r.retrieve, k_values=K_VALUES)
        s = summarize(outs, k_values=K_VALUES)
        strata = {name: summarize_by_stratum(outs, name, k_values=K_VALUES)
                  for name in STRATA}
        results[cid] = s
        print(f'\n=== {cid} ===  ({time.time() - t0:.0f}s)', flush=True)
        print(f'  n={s["n"]}  found={s["found_any"]}  mrr={s["mrr"]}', flush=True)
        for k in K_VALUES:
            lo, hi = s[f'recall@{k}_ci']
            print(f'  recall@{k:<3} = {s[f"recall@{k}"]:.3f}  [{lo:.3f}, {hi:.3f}]', flush=True)
        print(f'  p50={s["p50_ms"]}ms  p95={s["p95_ms"]}ms', flush=True)
        for name in STRATA:
            cells = '  '.join(
                f'{key}:{v["recall@50"]:.2f}(n={v["n"]})'
                for key, v in sorted(strata[name].items()))
            print(f'  by {name}: {cells}', flush=True)
        if ledger:
            ledger.record(method=cid.split('-')[0], policy_id=cid,
                          split=args.split, query_set=os.path.basename(
                              args.queries), summary=s, strata=strata,
                          force=args.force)

    if args.baseline and args.baseline in results:
        base = results[args.baseline]
        print(f'\n--- non-inferiority vs {args.baseline} (margin 3 points) ---', flush=True)
        for cid, s in results.items():
            if cid == args.baseline:
                continue
            for metric in ('recall@10', 'recall@50'):
                r = non_inferior(s, base, metric)
                print(f'  {cid:<44} {metric:<10} '
                      f'lo={r["candidate_ci_low"]:.3f} vs {r["incumbent"]:.3f} '
                      f'-> {"PASS" if r["pass"] else "FAIL"}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
