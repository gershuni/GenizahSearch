# -*- coding: utf-8 -*-
"""CAL-1 margin refit -- add the candidate-margin feature (FRAG-2, post-pilot).

The pilot + noise arms showed (len, density) saturates exactly where Hillel's
real density_fail cards live: 10/10 correct at density 0.41-0.55, predicted
P <= 0.06 by every arm. Diagnosis: those cards are all SINGLETONS (exactly one
work verified) while the synthetic pool at those densities is dominated by
crops with MANY chance candidates scattered across works. The margin/singleton
structure separates them, and it is known at deployment time.

This refit needs NO re-querying: cal1_rows_*.json already carries every
candidate per crop (crop_id column), so per-row margin is computable post-hoc:
    margin(row) = (min density of any OTHER work's candidate on the same crop)
                  - row.dens
    singleton   = no other work verified on the crop at the wide cutoff.
Bands: singleton / m>=0.10 / 0.03<=m<0.10 / 0<m<0.03 / m<=0 (not best).
Fit: per (length, band), per-work-weighted PAVA isotonic on density (reusing
cal1_calibration functions). Work-granular holdout as before. Stress test:
Hillel's density_fail cards are all singletons -> predict with the singleton
band; success criterion = they now get materially higher P.

Usage: python -X utf8 -u cal1_margin_refit.py [--arm pilot|pilot-n10|pilot-n20]
Out:   ../data/p_calibration_margin_<arm>.json
       ../results/p_calibration_margin.md   (all arms, one report)
"""
import argparse
import json
import random
import time
from collections import Counter, defaultdict

from cal1_calibration import (LENGTHS, HOLDOUT_FRAC, RNG_SEED,
                              pava_decreasing, work_weights, p_lookup,
                              FRAG1_CARDS, FRAG1_GRADES)
from frag1_truncation import log

PROBE = r"C:\Genizahsearch\same_work_spike\probe"
ROWS = PROBE + r"\data\cal1_rows_{arm}.json"
OUT_MODEL = PROBE + r"\data\p_calibration_margin_{arm}.json"
OUT_MD = PROBE + r"\results\p_calibration_margin.md"

ARMS = ['pilot', 'pilot-n10', 'pilot-n20']
BANDS = ['singleton', 'm>=0.10', '0.03<=m<0.10', '0<m<0.03', 'm<=0']


def margin_band(m):
    if m is None:
        return 'singleton'
    if m >= 0.10:
        return 'm>=0.10'
    if m >= 0.03:
        return '0.03<=m<0.10'
    if m > 0:
        return '0<m<0.03'
    return 'm<=0'


def add_margins(rows):
    """Attach 'band' to every row (margin vs best OTHER work on same crop)."""
    by_crop = defaultdict(list)
    for r in rows:
        by_crop[r['crop_id']].append(r)
    for lst in by_crop.values():
        for r in lst:
            # rows are already best-per-work per crop, so any other row on
            # the crop IS a different work.
            others = [x['dens'] for x in lst if x is not r]
            r['band'] = margin_band(min(others) - r['dens']) if others \
                else 'singleton'
    return rows


def fit_arm(rows):
    works = sorted({r['true_work'] for r in rows})
    rnd = random.Random(RNG_SEED + 7)          # same split as the base fit
    rnd.shuffle(works)
    hold_works = set(works[:max(1, int(len(works) * HOLDOUT_FRAC))])
    train = [r for r in rows if r['true_work'] not in hold_works]
    hold = [r for r in rows if r['true_work'] in hold_works]
    tw = work_weights(train)
    model = {}                                  # (band)(len) -> knots
    for band in BANDS:
        bl = {}
        for L in LENGTHS:
            pts = [(r['dens'], r['correct'], tw[(L, r['true_work'])])
                   for r in train if r['len'] == L and r['band'] == band]
            if len(pts) >= 20:
                bl[L] = pava_decreasing(pts)
        if bl:
            model[band] = bl
    # holdout reliability, per-work weighted
    hw = work_weights(hold)
    rel = defaultdict(lambda: [0.0, 0.0, 0])
    n_scored = 0
    for r in hold:
        bl = model.get(r['band'])
        if not bl or r['len'] not in bl:
            continue
        p = p_lookup(bl, r['len'], r['dens'])
        b = min(9, int(p * 10))
        w = hw[(r['len'], r['true_work'])]
        rel[b][0] += r['correct'] * w
        rel[b][1] += w
        rel[b][2] += 1
        n_scored += 1
    reliability = {f"{b/10:.1f}-{(b+1)/10:.1f}":
                   {'n': n, 'empirical': round(kw / ww, 3)}
                   for b, (kw, ww, n) in sorted(rel.items()) if n > 0}
    return model, reliability, len(train), len(hold), n_scored


def band_stats(rows):
    """Correctness by band (raw, unweighted) at a few density slices --
    the direct check that the singleton band re-prices high densities."""
    out = {}
    for band in BANDS:
        rs = [r for r in rows if r['band'] == band]
        slices = {}
        for lo, hi in [(0.0, 0.3), (0.3, 0.4), (0.4, 0.5), (0.5, 0.6)]:
            sl = [r for r in rs if lo <= r['dens'] < hi]
            if sl:
                slices[f"{lo}-{hi}"] = (
                    round(sum(r['correct'] for r in sl) / len(sl), 3), len(sl))
        out[band] = {'n': len(rs), 'slices': slices}
    return out


def stress_density_fail(models):
    """Hillel's 10/10-correct density_fail cards (ALL singletons) -- predicted
    P per arm using the singleton band."""
    cards = {c['id']: c for c in
             json.load(open(FRAG1_CARDS, encoding='utf-8'))}
    rows = []
    for g in json.load(open(FRAG1_GRADES, encoding='utf-8')):
        c = cards.get(g['id'])
        if not c or g.get('type') != 'density_fail':
            continue
        dens, length = c.get('cand_density'), c.get('cand_aligned_len')
        if dens is None or length is None:
            continue
        row = {'len': int(length), 'dens': float(dens), 'grade': g['grade'],
               'n_cands': len(c.get('ref_candidates') or [])}
        for arm, model in models.items():
            bl = model.get('singleton')
            row[arm] = round(p_lookup(bl, row['len'], row['dens']), 3) \
                if bl else None
        rows.append(row)
    return rows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--arms', nargs='*', default=ARMS)
    args = ap.parse_args()

    t0 = time.time()
    models, reliabs, stats, meta = {}, {}, {}, {}
    for arm in args.arms:
        rows = json.load(open(ROWS.format(arm=arm), encoding='utf-8'))
        rows = add_margins(rows)
        model, rel, n_tr, n_ho, n_sc = fit_arm(rows)
        models[arm] = model
        reliabs[arm] = rel
        stats[arm] = band_stats(rows)
        meta[arm] = {'n_rows': len(rows), 'train': n_tr, 'hold': n_ho,
                     'hold_scored': n_sc,
                     'bands': dict(Counter(r['band'] for r in rows))}
        json.dump({'meta': meta[arm],
                   'model': {b: {str(L): model[b][L] for L in model[b]}
                             for b in model}},
                  open(OUT_MODEL.format(arm=arm), 'w', encoding='utf-8'))
        log(f"{arm}: {len(rows):,} rows, bands {meta[arm]['bands']}")

    stress = stress_density_fail(models)

    # ---- report ----
    L_ = []
    A = L_.append
    A("# CAL-1 margin refit — (length, density, margin-band) model")
    A("")
    A(f"Generated {time.strftime('%Y-%m-%d %H:%M')} "
      f"({time.time()-t0:.0f}s, no re-querying — margins computed post-hoc "
      "from the pilot candidate rows). margin = best-OTHER-work density − "
      "this candidate's density on the same crop; 'singleton' = no other "
      "work verified at the 0.75 wide cutoff.")
    A("")
    A("## Why: the singleton signal (raw correctness by band × density slice)")
    A("")
    for arm in args.arms:
        A(f"**{arm}** (rows per band: {meta[arm]['bands']})")
        A("")
        A("| band | n | 0.0-0.3 | 0.3-0.4 | 0.4-0.5 | 0.5-0.6 |")
        A("|---|---|---|---|---|---|")
        for band in BANDS:
            st = stats[arm][band]
            cells = []
            for k in ['0.0-0.3', '0.3-0.4', '0.4-0.5', '0.5-0.6']:
                v = st['slices'].get(k)
                cells.append(f"{v[0]} (n={v[1]})" if v else "—")
            A(f"| {band} | {st['n']:,} | " + " | ".join(cells) + " |")
        A("")
    A("## Holdout reliability (work-granular split, per-work weighted)")
    A("")
    for arm in args.arms:
        A(f"**{arm}** (holdout rows scored: {meta[arm]['hold_scored']:,})")
        A("")
        A("| pred bucket | n | empirical |")
        A("|---|---|---|")
        for k, v in reliabs[arm].items():
            A(f"| {k} | {v['n']} | {v['empirical']} |")
        A("")
    A("## Stress test — Hillel's density_fail cards (10/10 graded correct; "
      "ALL singletons), singleton-band predicted P per arm")
    A("")
    A("| len | density | " + " | ".join(args.arms) +
      " | (len,dens)-only best arm |")
    A("|---|---|" + "---|" * (len(args.arms) + 1))
    OLD = {(125, 0.472): 0.891, (298, 0.406): 0.570, (241, 0.456): 0.321,
           (222, 0.477): 0.261, (257, 0.463): 0.062, (276, 0.460): 0.062,
           (145, 0.552): 0.026, (176, 0.545): 0.010, (272, 0.526): 0.002,
           (152, 0.500): None}
    for r in sorted(stress, key=lambda x: -x['dens']):
        old = OLD.get((r['len'], round(r['dens'], 3)))
        A(f"| {r['len']} | {r['dens']:.3f} | "
          + " | ".join(f"{r[a]:.3f}" if r.get(a) is not None else "—"
                       for a in args.arms)
          + f" | {old if old is not None else '—'} |")
    A("")
    A("Success criterion: singleton-band P materially above the "
      "(len,dens)-only predictions for these human-verified-correct cards. "
      "If it holds, the FINAL CAL-1 model ships as "
      "P(correct | length, density, margin-band) with per-provenance arms.")
    A("")
    open(OUT_MD, 'w', encoding='utf-8').write('\n'.join(L_))
    log(f"wrote {OUT_MD}")


if __name__ == '__main__':
    main()
