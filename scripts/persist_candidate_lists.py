# -*- coding: utf-8 -*-
"""Persist each method's RANKED CANDIDATE LIST for a query panel.

Why this exists. The outcome dumps from `scripts/eval_methods.py` keep only
`rank` and `n_returned` -- enough for recall, useless for building a display
view, because they do not say WHICH records came back. The display-policy deck
(external review, Codex 2026-08-21, rec 2) needs the actual ranked lists, and
it needs them manuscript-deduplicated, because the product shows manuscripts
and not pages: a record-grain top-10 can be ten pages of two manuscripts.

Output, one JSON line per (config, query):
  {"config_id", "query_id", "records": [...], "sys_ids": [...],
   "sys_first_record": {sys_id: record_id}, "n_records", "n_sys"}
`sys_ids` is in first-appearance order -- the manuscript ranking the product
would display -- and `sys_first_record` names the best-ranked record for each,
which is the one a card must render.

The query panel is reproduced EXACTLY as eval_methods.py draws it (tune split,
evenly spaced to --limit), so these lists line up row-for-row with the
existing outcome dumps and no re-run of the recall numbers is needed.

Chunk is slow (~13-20 s/query); passage is ~0.3 s. Written as one line per
result and flushed, so a killed run leaves usable partial output and can be
resumed with --skip-done.

Usage:
  python scripts/persist_candidate_lists.py --queries Q.jsonl --index IDX \
      --configs "passage:standard-40,passage:wide-40,chunk:3:exact:100" \
      --limit 300 --out candidates.jsonl [--skip-done]
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from shared.retrieval_eval import (  # noqa: E402
    SPLIT_HOLDOUT, SPLIT_TUNE, EvalQuery, split_queries,
)


def load_panel(path: str, split: str, limit: int) -> list:
    qs = []
    with open(path, encoding='utf-8') as fh:
        for line in fh:
            d = json.loads(line)
            qs.append(EvalQuery(query_id=d['query_id'], text=d['text'],
                                positives=frozenset(d['positives']),
                                strata=d.get('strata') or {}))
    chosen = split_queries(qs)[split]
    if limit and len(chosen) > limit:
        step = len(chosen) / limit
        chosen = [chosen[int(i * step)] for i in range(limit)]
    return chosen


def sys_of(record_id: str) -> str:
    return record_id.split('_', 1)[0]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--queries', required=True)
    ap.add_argument('--index', required=True)
    ap.add_argument('--configs', required=True)
    ap.add_argument('--split', default=SPLIT_TUNE,
                    choices=[SPLIT_TUNE, SPLIT_HOLDOUT])
    ap.add_argument('--limit', type=int, default=300)
    ap.add_argument('--cap', type=int, default=200,
                    help='keep at most this many records per list (the deck '
                         'never looks past rank 10; 200 leaves room for the '
                         'depth probe and costs nothing to store)')
    ap.add_argument('--out', required=True)
    ap.add_argument('--skip-done', action='store_true',
                    help='append, skipping (config, query) pairs already in '
                         '--out; for resuming a killed chunk run')
    args = ap.parse_args()

    panel = load_panel(args.queries, args.split, args.limit)
    print(f'panel: {len(panel)} queries from {args.split}', flush=True)

    done = set()
    if args.skip_done and os.path.exists(args.out):
        with open(args.out, encoding='utf-8') as fh:
            for line in fh:
                d = json.loads(line)
                done.add((d['config_id'], d['query_id']))
        print(f'resuming: {len(done)} (config, query) pairs already present',
              flush=True)

    # build_retrievers lives in the runner; importing it keeps ONE definition
    # of what 'chunk:3:exact:100' means (equal eligibility included).
    sys.path.insert(0, os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        'scripts'))
    from eval_methods import build_retrievers
    retrievers = build_retrievers(args.configs, args.index,
                                  equal_eligibility=True)

    mode = 'a' if (args.skip_done and os.path.exists(args.out)) else 'w'
    with open(args.out, mode, encoding='utf-8') as out:
        for r in retrievers:
            cid = r.config_id
            t0 = time.time()
            n_skipped = 0
            for i, q in enumerate(panel):
                if (cid, q.query_id) in done:
                    n_skipped += 1
                    continue
                records = list(r.retrieve(q.text))[:args.cap]
                first: dict = {}
                for rid in records:
                    first.setdefault(sys_of(rid), rid)
                out.write(json.dumps({
                    'config_id': cid, 'query_id': q.query_id,
                    'records': records, 'sys_ids': list(first),
                    'sys_first_record': first,
                    'n_records': len(records), 'n_sys': len(first),
                }, ensure_ascii=False) + '\n')
                out.flush()
                if (i + 1) % 25 == 0:
                    print(f'  {cid} {i + 1}/{len(panel)} '
                          f'({time.time() - t0:.0f}s)', flush=True)
            print(f'{cid}: done in {time.time() - t0:.0f}s '
                  f'({n_skipped} skipped)', flush=True)
    print(f'wrote {args.out}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
