# -*- coding: utf-8 -*-
"""Build a stratified, blinded DISPLAY-POLICY grading deck.

Why this and not `scripts/build_grading_deck.py`. That builder samples by
METHOD -- it answers "what does this retriever return". The decision on the
table is about a DISPLAY POLICY: what a reader is actually shown by a
candidate UI. External review (Codex, 2026-08-21) rejected a per-method tail
deck for this purpose on two grounds, both verified here:

  1. Ranks 4-10 are a materially SELECTED population. Measured on this panel:
     242 of 300 standard-40 queries (81%) and 203 of 300 wide-40 queries (68%)
     return fewer than 10 distinct manuscripts, medians 1 and 3. A deck drawn
     from ranks 4-10 estimates precision conditional on a deep list existing,
     which is the minority case.
  2. A method-attributed card cannot express combined-view precision at all,
     because a combined view is a policy over two rankings, not a method.

So the sampling frame here is the DISPLAYED CARD: one (query, manuscript) cell
that a given view actually puts in front of a reader. Three fixed views:

  S   standard-40, top 10 distinct manuscripts
  W   wide-40,     top 10 distinct manuscripts
  C5  wide-40 + the incumbent chunk search, 5 distinct manuscripts each,
      alternating, backfilling from the other side after cross-method
      duplicates so the slot budget is always spent

Manuscript grain throughout, because the product shows manuscripts: a
record-grain top 10 can be ten pages of two manuscripts.

Stratification, per view: visible rank band (1-3 / 4-10) x neutral aligned
span band (<60 / >=60 letters). The span split is there because short shared
spans ARE the formulas on this corpus (measured: strict 0.16 below 60 letters
against 0.90 at 60+, within one method), so without it a proportional draw
under-represents exactly the cards that decide the burden question. The span
band is used for ALLOCATION ONLY -- grades are never imputed from it.

Sampling: census any cell with at most --cell-census cards; otherwise draw
that many, then spread --extra additional draws proportionally across the
4-10 cells. Every selection records N_h, n_h and pi_h = n_h / N_h so the
scorer can weight by inverse probability (`scripts/score_display_deck.py`);
an unweighted card average over these strata would be badly biased.

A card selected by several views carries one `selections` entry per (view,
stratum) -- it is graded ONCE and its verdict is reused for every view that
showed it, which is both cheaper and consistent.

The deck HTML never contains the view, the rank, the method, or is_source.
Order is a deterministic shuffle by card id.

Usage:
  python scripts/build_display_deck.py \
      --candidates fgp_candidates_300.jsonl --queries fgp_queries_lang.jsonl \
      --index IDX --out-dir deck_display_v1 \
      --standard <config_id> --wide <config_id> --chunk <config_id> \
      [--depth 10] [--half 5] [--cell-census 8] [--extra 24] [--salt ...]
"""
from __future__ import annotations

import argparse
import collections
import hashlib
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from build_grading_deck import (  # noqa: E402
    GRADES, TEMPLATE_PATH, compute_is_source, fingerprint_inputs,
    neutral_span, sha,
)
from shared.passage_index import open_index  # noqa: E402

SHORT_SPAN = 60
RANK_BANDS = ('1-3', '4-10')
SPAN_BANDS = ('<60', '>=60')


def stable_rand(*parts) -> float:
    """Deterministic [0,1) from the parts -- no global RNG, no seed drift."""
    h = hashlib.blake2b('|'.join(str(p) for p in parts).encode(),
                        digest_size=8).digest()
    return int.from_bytes(h, 'big') / float(1 << 64)


def interleave(a: list, b: list, half: int) -> list:
    """Alternate a/b, skipping cross-side duplicates, `half` NEW each at most.

    "Backfill" means WITHIN a side: a candidate already taken by the other
    side does not waste that side's slot -- the pointer advances to its next
    unseen candidate. A side that runs dry early leaves the view SHORT rather
    than handing its slots to the other side; that cross-side variant is a
    different policy and is deliberately not modelled here, because "five
    from each" has to keep meaning that to be interpretable.

    A turn that cannot be served falls through to the other side, and the
    loop stops when neither side can contribute -- an earlier version spun
    forever in exactly that state.
    """
    taken: list = []
    seen: set = set()
    idx_a = idx_b = 0
    from_a = from_b = 0
    prefer_a = True
    while len(taken) < 2 * half:
        progressed = False
        for want_a in ((True, False) if prefer_a else (False, True)):
            if want_a:
                if from_a >= half:
                    continue
                while idx_a < len(a) and a[idx_a] in seen:
                    idx_a += 1
                if idx_a < len(a):
                    seen.add(a[idx_a])
                    taken.append(a[idx_a])
                    idx_a += 1
                    from_a += 1
                    progressed = True
                    break
            else:
                if from_b >= half:
                    continue
                while idx_b < len(b) and b[idx_b] in seen:
                    idx_b += 1
                if idx_b < len(b):
                    seen.add(b[idx_b])
                    taken.append(b[idx_b])
                    idx_b += 1
                    from_b += 1
                    progressed = True
                    break
        if not progressed:
            break
        prefer_a = not prefer_a
    return taken


def build_views(cand: dict, standard: str, wide: str, chunk: str,
                depth: int, half: int) -> dict:
    """-> {view: {query_id: [sys_id, ...]}} in visible order."""
    views: dict = {'S': {}, 'W': {}, 'C5': {}}
    for qid, per_cfg in cand.items():
        views['S'][qid] = per_cfg.get(standard, [])[:depth]
        views['W'][qid] = per_cfg.get(wide, [])[:depth]
        views['C5'][qid] = interleave(list(per_cfg.get(wide, [])),
                                      list(per_cfg.get(chunk, [])), half)
    return views


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--candidates', required=True)
    ap.add_argument('--queries', required=True)
    ap.add_argument('--index', required=True)
    ap.add_argument('--out-dir', required=True)
    ap.add_argument('--standard', required=True, help='config_id')
    ap.add_argument('--wide', required=True)
    ap.add_argument('--chunk', required=True)
    ap.add_argument('--depth', type=int, default=10)
    ap.add_argument('--half', type=int, default=5)
    ap.add_argument('--cell-census', type=int, default=8)
    ap.add_argument('--extra', type=int, default=24)
    ap.add_argument('--salt', default='display-v1')
    ap.add_argument('--transcriptions',
                    default=r'C:\GenizahSearch\Transcriptions.txt')
    args = ap.parse_args()

    os.makedirs(args.out_dir, exist_ok=True)

    # ---- inputs ----------------------------------------------------------
    cand: dict = collections.defaultdict(dict)
    configs = set()
    with open(args.candidates, encoding='utf-8') as fh:
        for line in fh:
            d = json.loads(line)
            cand[d['query_id']][d['config_id']] = d['sys_ids']
            configs.add(d['config_id'])
    for name, cfg in (('standard', args.standard), ('wide', args.wide),
                      ('chunk', args.chunk)):
        if cfg not in configs:
            raise SystemExit(f'--{name} {cfg!r} not in the candidate file; '
                             f'present: {sorted(configs)}')
    qrow = {}
    with open(args.queries, encoding='utf-8') as fh:
        for line in fh:
            d = json.loads(line)
            if d['query_id'] in cand:
                qrow[d['query_id']] = d
    missing = set(cand) - set(qrow)
    if missing:
        raise SystemExit(f'{len(missing)} candidate queries absent from '
                         f'--queries -- wrong inputs')
    print(f'panel: {len(cand)} queries', flush=True)

    # first-record map per (query, sys) so a card renders the best page
    first_rec: dict = {}
    with open(args.candidates, encoding='utf-8') as fh:
        for line in fh:
            d = json.loads(line)
            for s, rid in d['sys_first_record'].items():
                first_rec.setdefault((d['query_id'], s), rid)

    views = build_views(cand, args.standard, args.wide, args.chunk,
                        args.depth, args.half)
    for v, per_q in views.items():
        tot = sum(len(x) for x in per_q.values())
        nonempty = sum(1 for x in per_q.values() if x)
        print(f'  view {v:3s}: {tot:5d} displayed cards over '
              f'{nonempty} non-empty queries', flush=True)

    # ---- text for every DISPLAYED record (span bands need it) -------------
    wanted = {first_rec[(q, s)] for per_q in views.values()
              for q, ss in per_q.items() for s in ss
              if (q, s) in first_rec}
    print(f'fetching original text for {len(wanted):,} displayed records...',
          flush=True)
    original: dict = {}
    from shared.passage_corpus import iter_records as _iter
    for rid, txt in _iter(args.transcriptions):
        if rid in wanted:
            original[rid] = txt
            if len(original) == len(wanted):
                break
    print(f'  found {len(original):,}, missing {len(wanted) - len(original)}',
          flush=True)

    idx = open_index(args.index)
    if idx is None:
        raise SystemExit(f'index will not open: {args.index}')
    rid_index = {idx.record_id(i): i for i in range(idx.n_records)}

    # ---- one span per (query, record), computed once ----------------------
    span_cache: dict = {}

    def span_of(qid: str, rid: str) -> dict:
        k = (qid, rid)
        if k not in span_cache:
            ri = rid_index.get(rid)
            text = original.get(rid) or (idx.stream(ri) if ri is not None
                                         else '')
            span_cache[k] = neutral_span(qrow[qid]['text'], text)
        return span_cache[k]

    # ---- population: every displayed cell, with its stratum --------------
    population: dict = collections.defaultdict(list)   # stratum -> [cell]
    for view, per_q in views.items():
        for qid, syss in per_q.items():
            for pos, s in enumerate(syss, start=1):
                rid = first_rec.get((qid, s))
                if rid is None:
                    continue
                sp = span_of(qid, rid)
                rank_band = '1-3' if pos <= 3 else '4-10'
                span_band = '<60' if sp['letters'] < SHORT_SPAN else '>=60'
                stratum = f'{view}|{rank_band}|{span_band}'
                population[stratum].append(
                    {'view': view, 'query_id': qid, 'sys_id': s,
                     'record_id': rid, 'rank': pos, 'rank_band': rank_band,
                     'span_band': span_band, 'stratum': stratum})
    print('\npopulation by stratum:')
    for st in sorted(population):
        print(f'  {st:18s} N={len(population[st]):5d}')

    # ---- allocation ------------------------------------------------------
    alloc = {}
    for st, cells in population.items():
        alloc[st] = min(len(cells), args.cell_census)
    # spread the extra draws over the deep bands, proportional to their size
    deep = {st: len(c) for st, c in population.items()
            if st.split('|')[1] == '4-10' and len(c) > alloc[st]}
    total_deep = sum(deep.values())
    if total_deep:
        for st, n in sorted(deep.items()):
            add = int(round(args.extra * n / total_deep))
            alloc[st] = min(len(population[st]), alloc[st] + add)

    # ---- draw ------------------------------------------------------------
    selections: dict = collections.defaultdict(list)   # (qid, rid) -> [sel]
    for st in sorted(population):
        cells = population[st]
        n_h, N_h = alloc[st], len(cells)
        ordered = sorted(cells, key=lambda c: stable_rand(
            args.salt, st, c['query_id'], c['sys_id']))
        for c in ordered[:n_h]:
            sel = dict(c)
            sel.update({'N_h': N_h, 'n_h': n_h, 'pi_h': n_h / N_h})
            selections[(c['query_id'], c['record_id'])].append(sel)
    print(f'\nselected {sum(len(v) for v in selections.values())} view-cards '
          f'-> {len(selections)} unique cards to grade')

    # ---- cards + key -----------------------------------------------------
    cards, key = [], []
    for (qid, rid), sels in sorted(selections.items()):
        sp = span_of(qid, rid)
        card_id = sha({'q': qid, 'r': rid, 'salt': args.salt})[:16]
        cards.append({
            'id': card_id,
            'q_before': sp['q'][0], 'q_match': sp['q'][1],
            'q_after': sp['q'][2],
            'r_before': sp['r'][0], 'r_match': sp['r'][1],
            'r_after': sp['r'][2],
            'letters': sp['letters'], 'aligned': sp['aligned'],
            'r_sys': rid.split('_', 1)[0],
        })
        # is_source must be a real boolean. compute_is_source returns None
        # when a query row has no meta.sys_id, and a None would land in the
        # "non-source" bucket -- the population the externally-valid figure is
        # computed over -- silently mixing unknowns into it. Measured: 0 of
        # 19,090 FGP queries lack meta.sys_id, so this cannot fire on this
        # panel; it fails loudly rather than contaminating a future one.
        is_source = compute_is_source(rid, qrow[qid])
        if is_source is None:
            raise SystemExit(
                f'query {qid!r} has no meta.sys_id, so is_source is unknown '
                f'for record {rid!r}; refusing to file it as non-source')
        key.append({
            'id': card_id, 'query_id': qid, 'record_id': rid,
            'sys_id': rid.split('_', 1)[0],
            'is_source': is_source,
            'selections': [
                {k: v for k, v in s.items()
                 if k in ('view', 'rank', 'rank_band', 'span_band',
                          'stratum', 'N_h', 'n_h', 'pi_h')}
                for s in sorted(sels, key=lambda x: (x['view'], x['rank']))],
        })
    cards.sort(key=lambda c: c['id'])       # blind order

    view_meta = {}
    for view in ('S', 'W', 'C5'):
        strata = {st: {'N_h': len(population[st]), 'n_h': alloc[st]}
                  for st in population if st.startswith(view + '|')}
        # Per-query display counts. Needed because a cluster bootstrap that
        # resamples QUERIES while holding N_display fixed produces impossible
        # intervals -- the integration smoke returned a precision CI upper
        # bound of 1.486. Precision here is a RATIO estimator, so a resample
        # must recompute its denominator from the same resampled queries.
        by_q = collections.Counter()
        for qid, syss in views[view].items():
            n = sum(1 for s in syss if (qid, s) in first_rec)
            if n:
                by_q[qid] = n
        view_meta[view] = {
            'N_display': sum(v['N_h'] for v in strata.values()),
            'display_counts_by_query': dict(sorted(by_q.items())),
            'strata': strata,
        }
        assert sum(by_q.values()) == view_meta[view]['N_display'], (
            f'{view}: per-query display counts {sum(by_q.values())} do not '
            f'sum to N_display {view_meta[view]["N_display"]}')
    prereg = {
        'prereg_id': sha({'salt': args.salt, 'panel': sorted(cand)})[:16],
        'purpose': 'stratified display-policy precision, IPW-weighted',
        'exploratory': True,
        'views': {'S': args.standard, 'W': args.wide,
                  'C5': f'{args.wide} + {args.chunk} ({args.half}+'
                        f'{args.half}, alternating, backfilled)'},
        'depth': args.depth, 'grain': 'sys',
        'cell_census': args.cell_census, 'extra_deep_draws': args.extra,
        'span_band_letters': SHORT_SPAN,
        'span_band_use': 'ALLOCATION ONLY -- grades are never imputed from it',
        'panel_n_queries': len(cand),
        'salt': args.salt,
        'vocabulary': [g for g, _ in GRADES],
        'query_ids': sorted(cand),
        'inputs': fingerprint_inputs(args.queries, args.index,
                                     args.transcriptions),
    }
    manifest = {
        'prereg_id': prereg['prereg_id'], 'grain': 'sys',
        'n_cards': len(cards), 'n_queries': len(cand),
        'panel_n_queries': len(cand),
        'views': view_meta,
        'cards_hash': sha(cards), 'key_hash': sha(key),
    }
    for name, obj in (('deck_key.json', key), ('prereg.json', prereg),
                      ('deck_manifest.json', manifest)):
        with open(os.path.join(args.out_dir, name), 'w',
                  encoding='utf-8') as fh:
            json.dump(obj, fh, ensure_ascii=False, indent=1, sort_keys=True)

    with open(TEMPLATE_PATH, encoding='utf-8') as fh:
        html = fh.read()
    html = html.replace('__DATA__', json.dumps(cards, ensure_ascii=False))
    html = html.replace('__GRADES__', json.dumps(GRADES, ensure_ascii=False))
    html = html.replace('__DECKID__', manifest['cards_hash'][:16])
    with open(os.path.join(args.out_dir, 'grading_deck.html'), 'w',
              encoding='utf-8') as fh:
        fh.write(html)

    print(f'\ncards {len(cards)}  unaligned '
          f'{sum(1 for c in cards if not c["aligned"])}')
    for v, m in sorted(view_meta.items()):
        print(f'  view {v:3s} N_display={m["N_display"]:5d}  '
              f'sampled={sum(x["n_h"] for x in m["strata"].values()):4d}')
    print(f'deck  {args.out_dir}\\grading_deck.html')
    print(f'key   deck_key.json (hash {manifest["key_hash"][:16]}) '
          f'-- do NOT open before grading')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
