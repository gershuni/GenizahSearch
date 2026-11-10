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

Codex review ("neither tamper-evident nor cluster-aware"): this script used
to trust its inputs. It now refuses (fatal, never a silent skip) on:

  - a verdicts export whose declared deck id is absent or does not match the
    deck manifest's cards_hash prefix
  - a deck_key.json whose recomputed hash does not match the manifest's
    key_hash (the key was edited or regenerated without re-baking the
    manifest)
  - any duplicate verdict id, any verdict id absent from the key, or any
    grade outside the prereg vocabulary -- every defect is counted across
    the WHOLE export before refusing, so the report names how many of each
    kind, rather than failing on the first one found
  - --min-graded set and fewer cards graded than that

Every reported precision is also a query-CLUSTER bootstrap CI, alongside the
existing Wilson interval. Wilson assumes independent trials; the 240-card /
60-query deck's cards are NOT independent -- several cards share a query, and
grading errors or genuinely hard queries correlate within a query. The
cluster bootstrap resamples query ids (not cards) with replacement, so it is
labelled "query-clustered" against Wilson's "iid assumption".

Usage:
  python scripts/score_grading_deck.py --deck-dir DIR --verdicts V.json
"""
from __future__ import annotations

import argparse
import collections
import hashlib
import json
import math
import os
import random

REAL_STRICT = {'same_text', 'paraphrase'}
REAL_USEFUL = REAL_STRICT | {'canonical', 'shared_formula'}
WRONG = {'topical', 'unrelated', 'junk'}
ARTEFACT = {'duplicate_photo'}
ALL_GRADES = REAL_USEFUL | WRONG | ARTEFACT

CLUSTER_SEED_DEFAULT = 20260821
CLUSTER_RESAMPLES_DEFAULT = 10_000


def sha(obj) -> str:
    """Byte-identical to scripts/build_grading_deck.py::sha().

    Duplicated rather than imported: build_grading_deck.py imports
    shared.passage_index / shared.passage_normalize at module scope, and
    pulling that whole chain in just to reuse a 4-line hash helper would
    couple the scorer to the passage engine for no reason. Both copies hash
    the same way (json.dumps sort_keys, no whitespace, utf-8, sha256), which
    is what tests/test_score_grading_deck.py pins directly against a real
    build_grading_deck.sha() call.
    """
    blob = json.dumps(obj, sort_keys=True, ensure_ascii=False,
                      separators=(',', ':')).encode('utf-8')
    return hashlib.sha256(blob).hexdigest()


def wilson(k: int, n: int, z: float = 1.96) -> tuple:
    if n == 0:
        return (0.0, 0.0, 0.0)
    p = k / n
    d = 1 + z * z / n
    c = (p + z * z / (2 * n)) / d
    h = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / d
    return (round(p, 4), round(max(0.0, c - h), 4), round(min(1.0, c + h), 4))


def cluster_bootstrap_ci(indicator_by_query: dict, resamples: int,
                          seed: int) -> tuple:
    """Two-sided 95% CI for a proportion, resampling GROUPS (query ids).

    `indicator_by_query` is {query_id: [0/1, 0/1, ...]}, one 0/1 per card
    that counts toward this precision's denominator (artefacts already
    excluded by the caller). Each resample draws len(keys) query ids WITH
    replacement and pools every card belonging to the drawn ids -- this is
    the same cluster-bootstrap construction as
    scripts/analyze_paired_outcomes.py::cluster_bootstrap_diff, applied to a
    single proportion instead of a paired difference.

    Returns (point, lo95, hi95), the same shape `wilson()` returns, so the
    two can be printed side by side.
    """
    keys = sorted(indicator_by_query)
    total_n = sum(len(v) for v in indicator_by_query.values())
    total_k = sum(sum(v) for v in indicator_by_query.values())
    point = (total_k / total_n) if total_n else 0.0
    if not keys or not total_n:
        return (round(point, 4), 0.0, 0.0)

    rng = random.Random(seed)
    stats = []
    for _ in range(resamples):
        k = n = 0
        for _ in range(len(keys)):
            g = keys[rng.randrange(len(keys))]
            vals = indicator_by_query[g]
            k += sum(vals)
            n += len(vals)
        stats.append(k / n if n else 0.0)
    stats.sort()

    def q_(p: float) -> float:
        return stats[min(len(stats) - 1, int(p * len(stats)))]

    return (round(point, 4), round(q_(0.025), 4), round(q_(0.975), 4))


def is_source_split(cids: list, key: dict) -> dict:
    """Strict precision split by is_source, when the key carries the field.

    `cids` is a list of (card_id, grade) tuples already attributed to one
    method. Returns None -- never a fabricated split -- if no entry among
    these cards carries an 'is_source' field at all: older decks (e.g. the
    pre-Task-C deck_v5) do not carry it, and the caller must skip cleanly
    rather than report a false True/False on absent data.
    """
    have_field = any(key[cid].get('is_source') is not None
                     for cid, _g in cids)
    if not have_field:
        return None
    out = {}
    for label, flag in (('true', True), ('false', False)):
        sub = [(cid, g) for cid, g in cids if key[cid].get('is_source') == flag]
        n_all = len(sub)
        n = n_all - sum(1 for _cid, g in sub if g in ARTEFACT)
        if not n:
            out[label] = None
            continue
        strict_n = sum(1 for _cid, g in sub if g in REAL_STRICT)
        out[label] = wilson(strict_n, n)
    return out


def score(deck_dir: str, verdicts_path: str, *, min_graded: int = 0,
          cluster_seed: int = CLUSTER_SEED_DEFAULT,
          cluster_resamples: int = CLUSTER_RESAMPLES_DEFAULT) -> dict:
    """Validate a verdicts export against its deck, then score it.

    Raises SystemExit (fatal, never a silent skip) on any tamper-evidence
    or format defect described in the module docstring. On success, returns
    a dict of the same top-level and per-method shape scripts/score_grading_
    deck.py has always written to --out, plus additive keys (this function
    never removes a key a caller of the old shape might depend on).
    """
    key_path = os.path.join(deck_dir, 'deck_key.json')
    manifest_path = os.path.join(deck_dir, 'deck_manifest.json')
    with open(key_path, encoding='utf-8') as fh:
        key_list = json.load(fh)
    with open(manifest_path, encoding='utf-8') as fh:
        manifest = json.load(fh)
    with open(verdicts_path, encoding='utf-8') as fh:
        payload = json.load(fh)
    verdicts = payload.get('verdicts', payload)

    deck_id = manifest['cards_hash'][:16]
    declared = payload.get('deck')
    if not declared:
        raise SystemExit(
            f'REFUSING to score {verdicts_path}: no deck id declared in '
            f'the verdicts export -- cannot verify it belongs to deck '
            f'{deck_id} (tamper-evidence requires a declared id, not just '
            f'a matching one)')
    if declared != deck_id:
        raise SystemExit(
            f'REFUSING: verdicts are for deck {declared}, this deck is '
            f'{deck_id}')

    recomputed_key_hash = sha(key_list)
    if recomputed_key_hash != manifest.get('key_hash'):
        raise SystemExit(
            f'REFUSING: deck_key.json does not match deck_manifest.json -- '
            f'manifest key_hash={manifest.get("key_hash")!r}, recomputed '
            f'from the key file on disk={recomputed_key_hash!r}. The key '
            f'was edited or regenerated without re-baking the manifest.')

    key = {k['id']: k for k in key_list}

    dup_count = 0
    orphan_count = 0
    unknown_grades: collections.Counter = collections.Counter()
    seen: set = set()
    cards_by_method: dict = collections.defaultdict(list)

    for v in verdicts:
        cid, g = v.get('id'), v.get('grade')
        if cid not in key:
            orphan_count += 1
            continue
        if g not in ALL_GRADES:
            unknown_grades[g] += 1
            continue
        if cid in seen:
            dup_count += 1
            continue
        seen.add(cid)
        for m in key[cid]['methods']:
            cards_by_method[m].append((cid, g))
        if len(key[cid]['methods']) > 1:
            cards_by_method['BOTH (agreement)'].append((cid, g))

    if dup_count or orphan_count or unknown_grades:
        lines = [f'REFUSING to score {verdicts_path}: '
                f'{dup_count + orphan_count + sum(unknown_grades.values())} '
                f'defective verdict row(s) found']
        if dup_count:
            lines.append(f'  {dup_count} duplicate verdict id(s)')
        if orphan_count:
            lines.append(f'  {orphan_count} verdict id(s) absent from the '
                         f'deck key')
        if unknown_grades:
            lines.append(f'  grade(s) outside the prereg vocabulary: '
                         f'{dict(unknown_grades)}')
        raise SystemExit('\n'.join(lines))

    if min_graded and len(seen) < min_graded:
        raise SystemExit(
            f'REFUSING: only {len(seen)} of {manifest.get("n_cards")} '
            f'cards graded, --min-graded requires {min_graded}')

    methods_out = {}
    for method, cids in sorted(cards_by_method.items()):
        c = collections.Counter(g for _cid, g in cids)
        n_all = len(cids)
        n = n_all - sum(c[g] for g in ARTEFACT)
        if not n:
            continue
        strict_n = sum(c[g] for g in REAL_STRICT)
        useful_n = sum(c[g] for g in REAL_USEFUL)
        notwrong_n = n - sum(c[g] for g in WRONG)

        def indicator(pred, cids=cids):
            by_q: dict = collections.defaultdict(list)
            for cid, g in cids:
                if g in ARTEFACT:
                    continue
                by_q[key[cid]['query_id']].append(1 if pred(g) else 0)
            return by_q

        entry = {
            'n': n,
            'n_all': n_all,
            'strict': wilson(strict_n, n),
            'useful': wilson(useful_n, n),
            'not_wrong': wilson(notwrong_n, n),
            'strict_cluster': cluster_bootstrap_ci(
                indicator(lambda g: g in REAL_STRICT),
                cluster_resamples, cluster_seed),
            'useful_cluster': cluster_bootstrap_ci(
                indicator(lambda g: g in REAL_USEFUL),
                cluster_resamples, cluster_seed),
            'not_wrong_cluster': cluster_bootstrap_ci(
                indicator(lambda g: g not in WRONG),
                cluster_resamples, cluster_seed),
            'grades': dict(c),
        }
        by_src = is_source_split(cids, key)
        if by_src is not None:
            entry['strict_by_is_source'] = by_src
        methods_out[method] = entry

    return {
        'deck': deck_id,
        'n_cards': manifest.get('n_cards'),
        'graded': len(seen),
        'min_graded': min_graded,
        'cluster_seed': cluster_seed,
        'cluster_resamples': cluster_resamples,
        'methods': methods_out,
    }


def render_report(res: dict) -> list:
    """Human-readable report lines for `res` (the score() return value)."""
    lines = []
    n_cards = res.get('n_cards') or 0
    pct = f'{100 * res["graded"] / n_cards:.0f}%' if n_cards else 'n/a'
    lines.append(f'deck {res["deck"]}  cards {n_cards}  '
                f'graded {res["graded"]}  ({pct})')
    if res.get('min_graded'):
        lines.append(f'  --min-graded {res["min_graded"]} satisfied')
    lines.append('')

    rows = sorted(res['methods'].items())
    if not rows:
        lines.append('(no method has any graded, non-artefact card)')
        return lines

    def f(t):
        return f'{t[0]:.3f} [{t[1]:.3f},{t[2]:.3f}]'

    w = max(len(m) for m, _ in rows)
    lines.append(f'{"method":<{w}} {"n":>4} {"strict":>22} {"useful":>22} '
                f'{"not-wrong":>22}   (iid assumption -- Wilson)')
    lines.append('-' * (w + 76))
    for m, e in rows:
        lines.append(f'{m:<{w}} {e["n"]:>4} {f(e["strict"]):>22} '
                     f'{f(e["useful"]):>22} {f(e["not_wrong"]):>22}')
    lines.append('')
    lines.append(f'{"method":<{w}} {"n":>4} {"strict":>22} {"useful":>22} '
                f'{"not-wrong":>22}   (query-clustered bootstrap, '
                f'seed {res["cluster_seed"]}, {res["cluster_resamples"]} '
                f'resamples)')
    lines.append('-' * (w + 76))
    for m, e in rows:
        lines.append(f'{m:<{w}} {e["n"]:>4} {f(e["strict_cluster"]):>22} '
                     f'{f(e["useful_cluster"]):>22} '
                     f'{f(e["not_wrong_cluster"]):>22}')
    lines.append('')
    for m, e in rows:
        dist = '  '.join(f'{g}:{n}' for g, n in sorted(e['grades'].items())
                         if n)
        lines.append(f'{m}  (artefacts excluded: {e["n_all"] - e["n"]})')
        lines.append(f'    {dist}')
        if 'strict_by_is_source' in e:
            src = e['strict_by_is_source']

            def fmt_bucket(t):
                return f(t) if t is not None else 'n/a'
            lines.append(f'    strict by is_source: '
                         f'source_ms={fmt_bucket(src["true"])}  '
                         f'not_source_ms={fmt_bucket(src["false"])}')
    return lines


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--deck-dir', required=True)
    ap.add_argument('--verdicts', required=True)
    ap.add_argument('--min-graded', type=int, default=0,
                    help='fatal if fewer graded cards than this (0 = off; '
                         'the holdout prereg sets 150)')
    ap.add_argument('--cluster-seed', type=int,
                    default=CLUSTER_SEED_DEFAULT)
    ap.add_argument('--cluster-resamples', type=int,
                    default=CLUSTER_RESAMPLES_DEFAULT)
    ap.add_argument('--out', default=None)
    args = ap.parse_args()

    res = score(args.deck_dir, args.verdicts, min_graded=args.min_graded,
               cluster_seed=args.cluster_seed,
               cluster_resamples=args.cluster_resamples)

    for line in render_report(res):
        print(line)

    if args.out:
        with open(args.out, 'w', encoding='utf-8') as fh:
            json.dump(res, fh, ensure_ascii=False, indent=1, sort_keys=True)
        print(f'\nwrote {args.out}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
