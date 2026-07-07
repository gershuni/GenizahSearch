# -*- coding: utf-8 -*-
"""Fit the sloped length x density acceptance boundary (PROBE-RESULTS step #2).

Method: per length band, set the density threshold at the q-th percentile of
Tier-1 pair densities in that band (keep-q fit), per family profile:
- 'literary'  fit on tier1_titles (+ tier1_joins)
- 'liturgy'   fit on tier1_bh
- 'combined'  fit on all tier1

Evaluate each profile: Tier-1 recall (overall + per family), cross-class
yield (potential FPs, dupes excluded), related_new bonus, BH witness
connectivity. Plot boundary over the scatter.

Inputs: results/verified_pairs_d50.json, results/tier1.json,
        data/bh_witnesses.json
Outputs: results/roc_boundary.md + .png + .json
"""
import json
import sys
from collections import defaultdict

sys.path.insert(0, r"C:\Genizahsearch\same_work_spike\probe\scripts")

ROOT = r"C:\Genizahsearch"
PAIRS = ROOT + r"\same_work_spike\probe\results\verified_pairs_d50.json"
BH = ROOT + r"\same_work_spike\probe\data\bh_witnesses.json"
OUT_MD = ROOT + r"\same_work_spike\probe\results\roc_boundary.md"
OUT_PNG = ROOT + r"\same_work_spike\probe\results\roc_boundary.png"
OUT_JSON = ROOT + r"\same_work_spike\probe\results\roc_boundary.json"

BANDS = [(25, 60), (60, 100), (100, 200), (200, 400), (400, 10 ** 9)]
CEIL = 0.50  # verification ran at 0.50; thresholds cannot exceed it


def band_of(ln):
    for i, (lo, hi) in enumerate(BANDS):
        if lo <= ln < hi:
            return i
    return len(BANDS) - 1


pairs = json.load(open(PAIRS, encoding='utf-8'))

# ---- fit profiles ----
def fit(positive_classes, q):
    """per-band density threshold keeping fraction q of the positives."""
    dens_by_band = defaultdict(list)
    for p in pairs:
        if p['cls'] in positive_classes:
            dens_by_band[band_of(p['len'])].append(p['density'])
    thresholds = []
    prev = 0.30
    for i in range(len(BANDS)):
        ds = sorted(dens_by_band.get(i, []))
        if len(ds) >= 10:
            t = ds[min(len(ds) - 1, int(q * len(ds)))]
            t = min(CEIL, round(t + 0.005, 3))
        else:
            t = prev  # sparse band: inherit neighbor
        # enforce monotone non-decreasing with length (longer spans may be
        # noisier per char in liturgy; monotonicity keeps the rule simple)
        t = max(t, prev)
        thresholds.append(t)
        prev = t
    return thresholds


PROFILES = {
    'literary_q95': fit({'tier1_titles', 'tier1_joins'}, 0.95),
    'combined_q95': fit({'tier1_titles', 'tier1_joins', 'tier1_bh'}, 0.95),
    'liturgy_q95': fit({'tier1_bh'}, 0.95),
}

# ---- BH witness map for connectivity metric ----
bh = json.load(open(BH, encoding='utf-8'))
sys_to_sigla = defaultdict(set)
for sig, w in bh['witnesses'].items():
    for sm in w['shelfmarks']:
        for s in sm.get('sys_ids', []):
            sys_to_sigla[s].add(sig)


def accepts(profile, p):
    return p['density'] <= profile[band_of(p['len'])]


def evaluate(profile):
    res = defaultdict(int)
    tier1_tot = defaultdict(int)
    tier1_acc = defaultdict(int)
    bh_sigla = set()
    for p in pairs:
        cls = p['cls']
        if cls.startswith('tier1_'):
            tier1_tot[cls] += 1
        if not accepts(profile, p):
            continue
        res[cls] += 1
        if cls.startswith('tier1_'):
            tier1_acc[cls] += 1
        sa = sys_to_sigla.get(p['a'].split('_')[0], set())
        sb = sys_to_sigla.get(p['b'].split('_')[0], set())
        if sa and sb and (sa != sb or len(sa | sb) > 1):
            bh_sigla |= sa | sb
    out = {'accepted_by_class': dict(res),
           'bh_witnesses_connected': len(bh_sigla)}
    for cls, tot in tier1_tot.items():
        out[f'recall_{cls}'] = round(tier1_acc.get(cls, 0) / tot, 4)
    return out


evals = {name: evaluate(prof) for name, prof in PROFILES.items()}

# ---- plot ----
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
fig, ax = plt.subplots(figsize=(11, 6.5))
COLORS = {'cross': ('#999999', 0.3, 7), 'duplicate': ('#4444cc', 0.5, 7),
          'related_new': ('#e8a33d', 0.5, 9),
          'tier1_joins': ('#7a3fbf', 0.7, 12),
          'tier1_titles': ('#2e7d32', 0.55, 10),
          'tier1_bh': ('#c62828', 0.65, 12)}
by_cls = defaultdict(list)
for p in pairs:
    by_cls[p['cls']].append((p['len'], p['density']))
for cls, (c, a, s) in COLORS.items():
    pts = by_cls.get(cls, [])
    if pts:
        xs, ys = zip(*pts)
        ax.scatter(xs, ys, c=c, alpha=a, s=s, label=f"{cls} (n={len(pts)})")
STYLES = {'literary_q95': ('#2e7d32', '--'), 'combined_q95': ('#000000', '-'),
          'liturgy_q95': ('#c62828', ':')}
for name, prof in PROFILES.items():
    xs, ys = [], []
    for (lo, hi), t in zip(BANDS, prof):
        xs += [max(lo, 25), min(hi, 3000)]
        ys += [t, t]
    c, ls = STYLES[name]
    ax.plot(xs, ys, ls, color=c, lw=2, label=f"boundary {name}")
ax.set_xscale('log')
ax.set_xlabel('aligned span length (normalized letters, log)')
ax.set_ylabel('edit density')
ax.set_title('Acceptance boundary calibration (diag candidates, verify@0.50)')
ax.legend(fontsize=8, loc='lower left')
ax.grid(alpha=0.3)
fig.tight_layout()
fig.savefig(OUT_PNG, dpi=130)

# ---- report ----
lines = ["# Acceptance boundary calibration (2026-07-06)", "",
         f"Length bands: {BANDS}", ""]
for name, prof in PROFILES.items():
    lines.append(f"## {name}")
    lines.append("thresholds per band: " +
                 ", ".join(f"[{lo}-{hi if hi < 10**9 else 'inf'}): {t}"
                           for (lo, hi), t in zip(BANDS, prof)))
    for k, v in evals[name].items():
        lines.append(f"- {k}: {v}")
    lines.append("")
lines.append("Notes: 'cross' accepted pairs are POTENTIAL FPs but include "
             "canonical shares and genuine discoveries (the ona'ah find was "
             "cross) — the 200-pair graded sampling turns this into real "
             "precision. 'duplicate' (density<=0.02) is removed by stage-0. "
             "BH connectivity counts witnesses touched by any accepted pair "
             "between two BH-witness sys_ids.")
open(OUT_MD, 'w', encoding='utf-8').write("\n".join(lines))
json.dump({'bands': BANDS, 'profiles': PROFILES, 'evals': evals},
          open(OUT_JSON, 'w'), indent=1)
print("\n".join(lines))
