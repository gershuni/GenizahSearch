# -*- coding: utf-8 -*-
"""Map the passage engine's recall/precision tradeoff across its policy knobs.

Purpose (owner request, 2026-08-21): an INTERNAL map -- "how much better is
the new method, and can parameters trade recall against precision in either
direction" -- not a certified claim. Tune split only; the shelved holdout
stays untouched.

The two real knobs (measured earlier: verify_cap/posting_budget sweeps changed
results not at all, only latency):

  min_span        floor on accepted span length, in normalized letters.
                  Raising it trades away short quotations for precision --
                  the graded deck showed short shared spans ARE the formulas
                  (strict 0.16 below 60 letters vs 0.90 at 60+, measured
                  within one method so it is content, not method bias).
  density_scale   multiplies the edit-density acceptance boundary.
                  >1 admits noisier matches (recall up), <1 tightens.

Plus: regime (one-sided clean-query boundary vs the looser two-sided one) and
min_anchors (2 -> 3 demands a third distinct seed).

Three measurements per configuration, per instrument:

1. RECALL, automatic: recall@k against the instrument's positives, on the
   exact 300-query tune samples the method comparison used.
2. PRECISION ON LABELED PAIRS, free: the graded deck's 100 verdicts are
   labels on (query, record) pairs -- including the 58 pairs only the
   INCUMBENT returned, so a loosened config that starts returning them is
   scored on real labels in both directions. For each config we replay the
   deck's 60 queries and score top-3-per-query against every labeled pair:
   strict share on labeled, plus the count of NEW unlabeled returns (the
   unmeasurable frontier -- reported, never guessed at).
3. BURDEN: result-set size and the short-span share (spans < 60 letters,
   the validated formula proxy).

Latency is recorded but NOT trustworthy here -- sweeps run alongside other
work; benchmark latency separately.

Usage:
  python scripts/sweep_passage_tradeoff.py --index IDX \
      --fgp fgp_queries_lang.jsonl --witness witness_queries_v2_lang.jsonl \
      --deck-key deck_v5/deck_key.json --verdicts <export.json> \
      --out sweep.json [--limit 300]
"""
from __future__ import annotations

import argparse
import collections
import json
import os
import statistics
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from shared.passage_index import open_index                      # noqa: E402
from shared.passage_policy import PassagePolicy                  # noqa: E402
from shared.passage_search import search_passage                 # noqa: E402
from shared.retrieval_eval import SPLIT_TUNE, EvalQuery, split_queries  # noqa: E402

STRICT = {'same_text', 'paraphrase'}
SHORT_SPAN = 60          # letters; the deck-validated formula proxy
DECK_TOP_K = 3           # the deck's per-method pooling depth


def build_grid(which: str = 'broad') -> list:
    """The swept configurations. Names are stable identifiers.

    'broad' crosses both axes; 'focused' is the follow-up along the one axis
    the broad sweep showed matters (density_scale + regime; min_span and
    min_anchors measured inert on these instruments).
    """
    if which == 'focused':
        grid = []
        for ds in (1.0, 1.15, 1.3, 1.45):
            grid.append(PassagePolicy(name=f'ms40-ds{ds:g}',
                                      density_scale=ds))
        for ds in (1.0, 1.15, 1.3):
            grid.append(PassagePolicy(name=f'ms40-ds{ds:g}-2s',
                                      density_scale=ds, regime='two_sided'))
        return grid
    grid = []
    for ms in (25, 40, 60, 80):
        for ds in (0.85, 1.0, 1.15, 1.3):
            grid.append(PassagePolicy(name=f'ms{ms}-ds{ds:g}',
                                      min_span=ms, density_scale=ds))
    # the looser two-sided boundary, at the standard span floor
    for ds in (1.0, 1.15):
        grid.append(PassagePolicy(name=f'ms40-ds{ds:g}-2s', min_span=40,
                                  density_scale=ds, regime='two_sided'))
    # a third required anchor, at the standard point
    grid.append(PassagePolicy(name='ms40-ds1-a3', min_anchors=3))
    return grid


def load_queries(path: str, limit: int) -> list:
    qs = []
    with open(path, encoding='utf-8') as fh:
        for line in fh:
            d = json.loads(line)
            qs.append(EvalQuery(query_id=d['query_id'], text=d['text'],
                                positives=frozenset(d['positives']),
                                strata=d.get('strata') or {}))
    tune = split_queries(qs)[SPLIT_TUNE]
    if limit and len(tune) > limit:
        step = len(tune) / limit
        tune = [tune[int(i * step)] for i in range(limit)]
    return tune


def load_deck(deck_key_path: str, verdicts_path: str, fgp_path: str):
    """-> (query_id -> text, labeled {(query_id, record_id): grade})."""
    with open(verdicts_path, encoding='utf-8') as fh:
        verdicts = {v['id']: v['grade']
                    for v in json.load(fh)['verdicts']}
    with open(deck_key_path, encoding='utf-8') as fh:
        key = json.load(fh)
    labeled = {}
    deck_qids = set()
    for card in key:
        deck_qids.add(card['query_id'])
        if card['id'] in verdicts:
            labeled[(card['query_id'], card['record_id'])] = \
                verdicts[card['id']]
    texts = {}
    with open(fgp_path, encoding='utf-8') as fh:
        for line in fh:
            d = json.loads(line)
            if d['query_id'] in deck_qids:
                texts[d['query_id']] = d['text']
    missing = deck_qids - set(texts)
    if missing:
        raise SystemExit(f'{len(missing)} deck queries missing from the '
                         f'query file -- wrong inputs')
    return texts, labeled


def recall_metrics(idx, policy, queries: list) -> dict:
    ranks = []
    sizes = []
    short_shares = []
    lat = []
    for q in queries:
        t0 = time.perf_counter()
        hits, _rep = search_passage(idx, q.text, policy)
        lat.append((time.perf_counter() - t0) * 1000)
        sizes.append(len(hits))
        if hits:
            short = sum(1 for h in hits if h.matched_letters < SHORT_SPAN)
            short_shares.append(short / len(hits))
        rank = None
        for pos, h in enumerate(hits):
            if h.record_id in q.positives:
                rank = pos
                break
        ranks.append(rank)
    n = len(queries)
    out = {'n': n}
    for k in (1, 10, 50):
        out[f'recall@{k}'] = round(
            sum(1 for r in ranks if r is not None and r < k) / n, 4)
    out['mrr'] = round(sum(1 / (r + 1) for r in ranks if r is not None) / n, 4)
    out['result_size_median'] = statistics.median(sizes)
    out['result_size_p90'] = sorted(sizes)[int(0.9 * n)]
    out['short_span_share_mean'] = round(
        statistics.mean(short_shares), 4) if short_shares else 0.0
    out['p50_ms_untrusted'] = round(statistics.median(lat), 1)
    return out


def deck_replay(idx, policy, texts: dict, labeled: dict) -> dict:
    """Score this config's top-3-per-query against the graded labels."""
    counts = collections.Counter()
    returned_labeled = []
    for qid, text in sorted(texts.items()):
        hits, _rep = search_passage(idx, text, policy)
        for h in hits[:DECK_TOP_K]:
            grade = labeled.get((qid, h.record_id))
            if grade is None:
                counts['unlabeled'] += 1
            else:
                counts[grade] += 1
                returned_labeled.append(grade)
    n_lab = len(returned_labeled)
    strict = sum(1 for g in returned_labeled if g in STRICT)
    return {
        'labeled_returned': n_lab,
        'strict_on_labeled': round(strict / n_lab, 4) if n_lab else None,
        'unlabeled_returned': counts['unlabeled'],
        'grade_counts': {g: c for g, c in sorted(counts.items())
                         if g != 'unlabeled'},
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--index', required=True)
    ap.add_argument('--fgp', required=True)
    ap.add_argument('--witness', required=True)
    ap.add_argument('--deck-key', required=True)
    ap.add_argument('--verdicts', required=True)
    ap.add_argument('--limit', type=int, default=300)
    ap.add_argument('--grid', default='broad', choices=['broad', 'focused'])
    ap.add_argument('--out', required=True)
    args = ap.parse_args()

    idx = open_index(args.index)
    if idx is None:
        raise SystemExit(f'index will not open: {args.index}')
    fgp = load_queries(args.fgp, args.limit)
    wit = load_queries(args.witness, args.limit)
    texts, labeled = load_deck(args.deck_key, args.verdicts, args.fgp)
    print(f'fgp tune n={len(fgp)}  witness tune n={len(wit)}  '
          f'deck queries={len(texts)}  labeled pairs={len(labeled)}',
          flush=True)

    grid = build_grid(args.grid)
    print(f'{len(grid)} configurations', flush=True)
    results = []
    for i, policy in enumerate(grid):
        t0 = time.time()
        row = {'name': policy.name, 'policy_id': policy.policy_id,
               'min_span': policy.min_span,
               'density_scale': policy.density_scale,
               'regime': policy.regime, 'min_anchors': policy.min_anchors}
        row['fgp'] = recall_metrics(idx, policy, fgp)
        row['witness'] = recall_metrics(idx, policy, wit)
        row['deck'] = deck_replay(idx, policy, texts, labeled)
        results.append(row)
        print(f'[{i + 1:2d}/{len(grid)}] {policy.name:16s} '
              f"fgp@50={row['fgp']['recall@50']:.3f} "
              f"wit@50={row['witness']['recall@50']:.3f} "
              f"strict={row['deck']['strict_on_labeled']} "
              f"(lab {row['deck']['labeled_returned']}, "
              f"new {row['deck']['unlabeled_returned']}) "
              f"size~{row['fgp']['result_size_median']:.0f} "
              f"({time.time() - t0:.0f}s)", flush=True)
        # checkpoint after every config -- long jobs must be resumable reads
        with open(args.out, 'w', encoding='utf-8') as fh:
            json.dump({'limit': args.limit, 'results': results}, fh,
                      ensure_ascii=False, indent=1)
    print(f'wrote {args.out}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
