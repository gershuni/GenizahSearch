# -*- coding: utf-8 -*-
"""Score exported relation verdicts into per-method precision.

Unblinding happens HERE and only here: the deck HTML never sees deck_key.json,
so the grader cannot have been influenced by which method produced a card.

Precision is relation-aware, not binary, because binary would be wrong for
this feature. The grader's vocabulary distinguishes:

  same_text        the two passages are the same text          -> REAL
  paraphrase       a different wording of the same content     -> REAL
  canonical        a shared scripture/rabbinic/liturgy quote   -> REAL BUT
                                                                  not a find
  shared_formula   documentary or liturgical boilerplate       -> WEAK
  duplicate_photo  one physical page photographed twice        -> ARTEFACT
  topical          related subject only                        -> WRONG
  unrelated        no relation                                 -> WRONG
  junk             the page is unusable                        -> WRONG

Three precision figures are reported rather than one, because they answer
different questions and a single number would have to pick silently:

  strict     same_text + paraphrase
             "did it find the same text?"
  useful     strict + canonical + shared_formula
             "was the match real, whatever kind?"
  not_wrong  1 - (topical + unrelated + junk)
             "how often was it not simply mistaken?"

duplicate_photo is excluded from all three denominators: it is a corpus
artefact, not a retrieval verdict, and charging it to either method would
measure the photography.

Usage:
  python scripts/score_grading_deck.py --deck-dir DIR --verdicts V.json
"""
from __future__ import annotations

import argparse
import collections
import json
import math
import os
import sys

REAL_STRICT = {'same_text', 'paraphrase'}
REAL_USEFUL = REAL_STRICT | {'canonical', 'shared_formula'}
WRONG = {'topical', 'unrelated', 'junk'}
ARTEFACT = {'duplicate_photo'}
ALL_GRADES = REAL_USEFUL | WRONG | ARTEFACT


def wilson(k: int, n: int, z: float = 1.96) -> tuple:
    if n == 0:
        return (0.0, 0.0, 0.0)
    p = k / n
    d = 1 + z * z / n
    c = (p + z * z / (2 * n)) / d
    h = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / d
    return (round(p, 4), round(max(0.0, c - h), 4), round(min(1.0, c + h), 4))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--deck-dir', required=True)
    ap.add_argument('--verdicts', required=True)
    ap.add_argument('--out', default=None)
    args = ap.parse_args()

    key = {k['id']: k for k in json.load(
        open(os.path.join(args.deck_dir, 'deck_key.json'), encoding='utf-8'))}
    manifest = json.load(open(os.path.join(args.deck_dir,
                                           'deck_manifest.json'),
                              encoding='utf-8'))
    payload = json.load(open(args.verdicts, encoding='utf-8'))
    verdicts = payload.get('verdicts', payload)

    if payload.get('deck') and payload['deck'] != manifest['cards_hash'][:16]:
        print(f'REFUSING: verdicts are for deck {payload["deck"]}, this is '
              f'{manifest["cards_hash"][:16]}', file=sys.stderr)
        return 2

    seen, unknown, orphan = set(), collections.Counter(), 0
    by_method: dict = collections.defaultdict(collections.Counter)
    for v in verdicts:
        cid, g = v.get('id'), v.get('grade')
        if cid not in key:
            orphan += 1
            continue
        if g not in ALL_GRADES:
            unknown[g] += 1
            continue
        seen.add(cid)
        for m in key[cid]['methods']:
            by_method[m][g] += 1
        if len(key[cid]['methods']) > 1:
            by_method['BOTH (agreement)'][g] += 1

    print(f'deck {manifest["cards_hash"][:16]}  cards {manifest["n_cards"]}  '
          f'graded {len(seen)}  ({100 * len(seen) / manifest["n_cards"]:.0f}%)')
    if orphan:
        print(f'  {orphan} verdicts for unknown card ids (ignored)')
    if unknown:
        print(f'  unknown grades ignored: {dict(unknown)}')
    print()

    rows = []
    for method, c in sorted(by_method.items()):
        n_all = sum(c.values())
        n = n_all - sum(c[g] for g in ARTEFACT)
        if not n:
            continue
        strict = sum(c[g] for g in REAL_STRICT)
        useful = sum(c[g] for g in REAL_USEFUL)
        notwrong = n - sum(c[g] for g in WRONG)
        rows.append((method, n_all, n, wilson(strict, n), wilson(useful, n),
                     wilson(notwrong, n), c))

    w = max(len(r[0]) for r in rows) if rows else 10
    print(f'{"method":<{w}} {"n":>4} {"strict":>22} {"useful":>22} '
          f'{"not-wrong":>22}')
    print('-' * (w + 76))
    for method, n_all, n, st, us, nw, _c in rows:
        def f(t):
            return f'{t[0]:.3f} [{t[1]:.3f},{t[2]:.3f}]'
        print(f'{method:<{w}} {n:>4} {f(st):>22} {f(us):>22} {f(nw):>22}')
    print()
    for method, n_all, n, _s, _u, _w2, c in rows:
        dist = '  '.join(f'{g}:{c[g]}' for g in sorted(c) if c[g])
        print(f'{method}  (artefacts excluded: {n_all - n})')
        print(f'    {dist}')

    if args.out:
        with open(args.out, 'w', encoding='utf-8') as fh:
            json.dump({'deck': manifest['cards_hash'][:16],
                       'graded': len(seen),
                       'methods': {m: {'n': n, 'strict': st, 'useful': us,
                                       'not_wrong': nw, 'grades': dict(c)}
                                   for m, _a, n, st, us, nw, c in rows}},
                      fh, ensure_ascii=False, indent=1, sort_keys=True)
        print(f'\nwrote {args.out}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
