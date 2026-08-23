# -*- coding: utf-8 -*-
"""Paired comparison of two retrieval methods from per-query outcome dumps.

Why paired. The eval harness reports independent Wilson intervals per method,
which throws away the pairing: both methods answered the SAME queries, so the
decisive quantity is the per-query difference, not two marginal rates. The
non-inferiority endpoint in docs/specs/parallels-holdout-prereg.md is defined
on the paired difference, and this file is the analysis code that document
freezes -- written and tested BEFORE any holdout data exists, so the analysis
cannot be shaped around the answer.

Method. For each k: recall@k per config; the discordant-pair table (McNemar
counts: only-A-hit, only-B-hit); the paired difference with a SEEDED cluster
bootstrap (resamples GROUPS, where group = query_id up to the first '#' --
sibling queries of one work share identical positive sets and are not
independent). One-sided lower bound at the pre-registered level, compared to
the margin. Per-stratum breakdown with the same machinery; a stratum below
--min-stratum-n is reported as INSUFFICIENT, never silently pooled.

Determinism: --seed defaults to the pre-registered 20260821; identical inputs
give identical output bytes.

Usage:
  python scripts/analyze_paired_outcomes.py --dump OUT.jsonl \
      --a passage-standard-40-pp1-... --b chunk-3-exact-100-elig \
      [--k 50] [--margin 0.03] [--resamples 10000] [--out report.json]
"""
from __future__ import annotations

import argparse
import collections
import json
import random
import sys

K_DEFAULT = 50
SEED_DEFAULT = 20260821
RESAMPLES_DEFAULT = 10_000
MIN_STRATUM_N = 100


def load(path: str) -> dict:
    """-> {config_id: {query_id: outcome-dict}} with duplicate protection."""
    out = collections.defaultdict(dict)
    with open(path, encoding='utf-8') as fh:
        for line in fh:
            d = json.loads(line)
            cfg, qid = d['config_id'], d['query_id']
            if qid in out[cfg]:
                raise SystemExit(
                    f'duplicate outcome for ({cfg}, {qid}) -- the dump is '
                    f'corrupt or two runs were appended; refusing to guess')
            out[cfg][qid] = d
    return dict(out)


def hit(outcome: dict, k: int) -> bool:
    r = outcome.get('rank')
    return r is not None and r < k


def group_of(qid: str) -> str:
    return qid.split('#', 1)[0]


def paired_table(a: dict, b: dict, qids: list, k: int) -> dict:
    both = only_a = only_b = neither = 0
    for q in qids:
        ha, hb = hit(a[q], k), hit(b[q], k)
        both += ha and hb
        only_a += ha and not hb
        only_b += hb and not ha
        neither += not ha and not hb
    return {'both': both, 'only_a': only_a, 'only_b': only_b,
            'neither': neither}


def cluster_bootstrap_diff(a: dict, b: dict, qids: list, k: int,
                           resamples: int, seed: int) -> dict:
    """Paired recall@k difference (A-B) with a cluster bootstrap.

    Resamples GROUPS with replacement; each draw carries all of a group's
    queries, preserving the within-work correlation the plain bootstrap
    would destroy.
    """
    groups = collections.defaultdict(list)
    for q in qids:
        groups[group_of(q)].append(q)
    keys = sorted(groups)
    diffs = [(1 if hit(a[q], k) else 0) - (1 if hit(b[q], k) else 0)
             for q in qids]
    point = sum(diffs) / len(qids)

    per_group = {g: [(1 if hit(a[q], k) else 0) - (1 if hit(b[q], k) else 0)
                     for q in qs] for g, qs in groups.items()}
    rng = random.Random(seed)
    stats = []
    for _ in range(resamples):
        num = den = 0
        for _ in range(len(keys)):
            g = keys[rng.randrange(len(keys))]
            num += sum(per_group[g])
            den += len(per_group[g])
        stats.append(num / den if den else 0.0)
    stats.sort()

    def q_(p: float) -> float:
        return stats[min(len(stats) - 1, int(p * len(stats)))]

    lb_raw = q_(0.05)
    return {
        'diff': round(point, 4),
        'ci95_two_sided': [round(q_(0.025), 4), round(q_(0.975), 4)],
        'lb95_one_sided': round(lb_raw, 4),
        # UNROUNDED lower bound, kept alongside the rounded one above. Any
        # pass/fail comparison against --margin MUST use this value: rounding
        # to 4 decimals before comparing can flip a verdict (e.g. a true LB of
        # -0.03004 rounds to -0.0300, which reads as passing a 0.03 margin
        # when the real bootstrap quantile does not). Round only for display.
        'lb95_one_sided_raw': lb_raw,
        'n_groups': len(keys),
    }


def analyze(a: dict, b: dict, k: int, margin: float, resamples: int,
            seed: int, min_stratum_n: int, strict: bool = False,
            expect_cells: list = None) -> dict:
    """strict: the holdout mode (Codex C3 fail-closed fix).

    Non-strict (tune) behaviour is UNCHANGED: a mismatched query_id set
    triggers a WARNING and the analysis proceeds on the intersection.

    Under strict, an intersection fallback would silently let the two
    configs' dumps disagree about what was even run and still emit a
    verdict, so it is fatal instead: (a) the query_id sets must be IDENTICAL,
    no fallback; (b) every shared query's strata dict must match between A
    and B (a stratum that differs between the two dumps means at least one
    of them is not describing this query correctly).

    expect_cells (independent of strict, so it is unit-testable on its own):
    a list of {'stratum', 'value', 'min_n'} dicts, each checked for EXISTENCE
    (n > 0) and SUFFICIENCY (n >= min_n) over the shared-query set -- always
    computed for exactly these declared cells, never substituted with
    whatever strata happen to be dynamically discovered in the data. Any
    missing/undersized cell fails the whole set closed: 'all_cells_pass' is
    False and 'overall_verdict' is INSUFFICIENT-BLOCKED, never a silent
    partial pass.
    """
    qids = sorted(set(a) & set(b))
    missing = sorted(set(a) ^ set(b))
    if strict and missing:
        raise SystemExit(
            f'STRICT: {len(missing)} query_id(s) present in only one config '
            f'-- refusing the intersection fallback under --strict '
            f'(first few: {missing[:5]})')
    if missing:
        print(f'WARNING: {len(missing)} queries present in only one config '
              f'-- analysed on the {len(qids)}-query intersection',
              file=sys.stderr)
    if not qids:
        raise SystemExit('no shared queries between the two configs')

    if strict:
        for q in qids:
            sa = a[q].get('strata') or {}
            sb = b[q].get('strata') or {}
            if sa != sb:
                raise SystemExit(
                    f'STRICT: strata mismatch for query {q!r} -- A={sa} '
                    f'B={sb} (the two dumps disagree about this query)')

    def recall(cfg: dict) -> float:
        return sum(1 for q in qids if hit(cfg[q], k)) / len(qids)

    boot = cluster_bootstrap_diff(a, b, qids, k, resamples, seed)
    res = {
        'n': len(qids),
        'k': k,
        'margin': margin,
        'seed': seed,
        'strict': strict,
        'recall_a': round(recall(a), 4),
        'recall_b': round(recall(b), 4),
        'paired': paired_table(a, b, qids, k),
        'bootstrap': boot,
        'non_inferior_a_vs_b': boot['lb95_one_sided_raw'] >= -margin,
    }

    strata_keys = sorted({sk for q in qids
                          for sk in (a[q].get('strata') or {})})
    res['strata'] = {}
    for sk in strata_keys:
        res['strata'][sk] = {}
        values = sorted({(a[q].get('strata') or {}).get(sk) for q in qids
                         if (a[q].get('strata') or {}).get(sk) is not None})
        for val in values:
            sub = [q for q in qids
                   if (a[q].get('strata') or {}).get(sk) == val]
            if len(sub) < min_stratum_n:
                res['strata'][sk][val] = {'n': len(sub),
                                          'verdict': 'INSUFFICIENT'}
                continue
            sboot = cluster_bootstrap_diff(a, b, sub, k, resamples, seed)
            res['strata'][sk][val] = {
                'n': len(sub),
                'recall_a': round(sum(1 for q in sub if hit(a[q], k))
                                  / len(sub), 4),
                'recall_b': round(sum(1 for q in sub if hit(b[q], k))
                                  / len(sub), 4),
                'bootstrap': sboot,
                'non_inferior': sboot['lb95_one_sided_raw'] >= -margin,
            }

    if expect_cells is not None:
        cells_out = []
        for cell in expect_cells:
            st, val, min_n = cell['stratum'], cell['value'], cell['min_n']
            sub = [q for q in qids
                   if (a[q].get('strata') or {}).get(st) == val]
            n = len(sub)
            sufficient = n > 0 and n >= min_n
            entry = {'stratum': st, 'value': val, 'min_n': min_n, 'n': n,
                     'sufficient': sufficient}
            if sufficient:
                cboot = cluster_bootstrap_diff(a, b, sub, k, resamples, seed)
                entry['bootstrap'] = cboot
                entry['non_inferior'] = (
                    cboot['lb95_one_sided_raw'] >= -margin)
            cells_out.append(entry)
        res['expect_cells'] = cells_out
        # Computed over EXACTLY the declared cells above -- never over
        # res['strata'], which reflects whatever strata the data happens to
        # carry and could silently omit a pre-registered class this run.
        res['all_cells_pass'] = all(c['sufficient'] for c in cells_out)
        if not res['all_cells_pass']:
            res['overall_verdict'] = 'INSUFFICIENT-BLOCKED'
        else:
            res['overall_verdict'] = (
                'NON-INFERIOR' if all(c['non_inferior'] for c in cells_out)
                else 'NOT SHOWN')
    return res


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--dump', required=True, action='append',
                    help='outcome jsonl (repeatable; rows are merged)')
    ap.add_argument('--a', required=True, help='candidate config_id')
    ap.add_argument('--b', required=True, help='incumbent config_id')
    ap.add_argument('--k', type=int, default=K_DEFAULT)
    ap.add_argument('--margin', type=float, default=0.03)
    ap.add_argument('--resamples', type=int, default=RESAMPLES_DEFAULT)
    ap.add_argument('--seed', type=int, default=SEED_DEFAULT)
    ap.add_argument('--min-stratum-n', type=int, default=MIN_STRATUM_N)
    ap.add_argument('--strict', action='store_true',
                    help='holdout mode: no intersection fallback on a '
                         'query_id mismatch, strata must match between A '
                         'and B, and --expect-cells becomes required')
    ap.add_argument('--expect-cells', default=None,
                    help='JSON file: a list of {"stratum","value","min_n"} '
                         'cells that must all exist and be sufficiently '
                         'sized, else the run is INSUFFICIENT-BLOCKED. '
                         'Required when --strict is set.')
    ap.add_argument('--out', default=None)
    args = ap.parse_args()

    if args.strict and not args.expect_cells:
        raise SystemExit('--strict requires --expect-cells FILE '
                         '(the holdout mode has no undeclared cells)')

    expect_cells = None
    if args.expect_cells:
        with open(args.expect_cells, encoding='utf-8') as fh:
            expect_cells = json.load(fh)
        if not isinstance(expect_cells, list):
            raise SystemExit('--expect-cells must be a JSON list of '
                             '{"stratum","value","min_n"} objects')

    merged = collections.defaultdict(dict)
    for path in args.dump:
        for cfg, rows in load(path).items():
            overlap = set(rows) & set(merged[cfg])
            if overlap:
                raise SystemExit(
                    f'{len(overlap)} duplicate (config, query) rows across '
                    f'dumps for {cfg} -- refusing to merge silently')
            merged[cfg].update(rows)
    if args.a not in merged or args.b not in merged:
        raise SystemExit(f'configs in dumps: {sorted(merged)} -- '
                         f'need both {args.a!r} and {args.b!r}')

    res = analyze(merged[args.a], merged[args.b], args.k, args.margin,
                  args.resamples, args.seed, args.min_stratum_n,
                  strict=args.strict, expect_cells=expect_cells)

    p = res['paired']
    print(f"n={res['n']}  k={res['k']}  A={args.a}  B={args.b}")
    print(f"  recall  A={res['recall_a']:.3f}  B={res['recall_b']:.3f}  "
          f"diff={res['bootstrap']['diff']:+.3f}")
    print(f"  paired: both={p['both']}  onlyA={p['only_a']}  "
          f"onlyB={p['only_b']}  neither={p['neither']}")
    print(f"  cluster bootstrap ({res['bootstrap']['n_groups']} groups, "
          f"seed {res['seed']}): "
          f"CI95={res['bootstrap']['ci95_two_sided']}  "
          f"one-sided LB={res['bootstrap']['lb95_one_sided']:+.3f}  "
          f"margin=-{res['margin']:.2f}  "
          f"-> {'NON-INFERIOR' if res['non_inferior_a_vs_b'] else 'NOT SHOWN'}")
    for sk, vals in res['strata'].items():
        for val, s in vals.items():
            if s.get('verdict') == 'INSUFFICIENT':
                print(f"  [{sk}={val}] n={s['n']} INSUFFICIENT")
            else:
                print(f"  [{sk}={val}] n={s['n']} "
                      f"A={s['recall_a']:.3f} B={s['recall_b']:.3f} "
                      f"LB={s['bootstrap']['lb95_one_sided']:+.3f} "
                      f"-> {'NON-INFERIOR' if s['non_inferior'] else 'NOT SHOWN'}")

    if 'expect_cells' in res:
        print()
        for c in res['expect_cells']:
            if not c['sufficient']:
                print(f"  [expect {c['stratum']}={c['value']}] n={c['n']} "
                      f"min_n={c['min_n']}  MISSING/UNDERSIZED")
            else:
                print(f"  [expect {c['stratum']}={c['value']}] n={c['n']} "
                      f"LB={c['bootstrap']['lb95_one_sided']:+.3f} "
                      f"-> {'NON-INFERIOR' if c['non_inferior'] else 'NOT SHOWN'}")
        print(f"  all_cells_pass={res['all_cells_pass']}  "
              f"overall_verdict={res['overall_verdict']}")

    if args.out:
        with open(args.out, 'w', encoding='utf-8') as fh:
            json.dump(res, fh, ensure_ascii=False, indent=1, sort_keys=True)
        print(f'wrote {args.out}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
