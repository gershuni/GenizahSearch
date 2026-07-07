# -*- coding: utf-8 -*-
"""Birkat Hamazon experiment: witness-to-witness detection over the
(almost) exhaustive human witness index.

Q1: witness-level recall of candidate mode vs Tier-1 oracle (BH subset)
Q2: DF-cap sensitivity sweep - how much liturgical recall does DF-banding cost?
Q3: identification framing - top partner of each BH page: is it another
    known BH witness? (uses full-pilot verified pairs from separability.py)

Output: results/bh_report.txt + results/bh_experiment.json
"""
import json
import sqlite3
import sys
import time
from collections import defaultdict

sys.path.insert(0, r"C:\Genizahsearch\same_work_spike\probe\scripts")
from normalize import norm_stream  # noqa: E402
import engine  # noqa: E402

ROOT = r"C:\Genizahsearch"
PROBE_DB = ROOT + r"\same_work_spike\probe\data\probe.db"
BH = ROOT + r"\same_work_spike\probe\data\bh_witnesses.json"
TIER1 = ROOT + r"\same_work_spike\probe\results\tier1.json"
VPAIRS = ROOT + r"\same_work_spike\probe\results\verified_pairs.json"
OUT_JSON = ROOT + r"\same_work_spike\probe\results\bh_experiment.json"
OUT_TXT = ROOT + r"\same_work_spike\probe\results\bh_report.txt"

bh = json.load(open(BH, encoding='utf-8'))
sys_to_sigla = defaultdict(set)
for sig, w in bh['witnesses'].items():
    for sm in w['shelfmarks']:
        for s in sm.get('sys_ids', []):
            sys_to_sigla[s].add(sig)

con = sqlite3.connect(PROBE_DB)
ids, streams, keys = [], [], []
for pid, sid, text in con.execute(
        "SELECT page_id, sys_id, text FROM pages WHERE buckets LIKE '%bh%'"):
    s, _ = norm_stream(text)
    if len(s) >= 60:
        ids.append(pid)
        streams.append(s)
        keys.append(sid)
print(f"BH pages: {len(ids)}")


def witness_pairs(page_pairs):
    """page-pair list -> set of cross-witness (sigA, sigB) pairs + coverage."""
    wp = set()
    for a, b in page_pairs:
        sa, sb = a.split('_')[0], b.split('_')[0]
        for siga in sys_to_sigla.get(sa, ()):
            for sigb in sys_to_sigla.get(sb, ()):
                if siga != sigb:
                    wp.add(tuple(sorted((siga, sigb))))
    return wp


# ---- Tier-1 oracle (already computed by ground_truth.py) ----
tier1 = json.load(open(TIER1, encoding='utf-8'))
gt_pages = {tuple(sorted((p['a'], p['b']))) for p in tier1['bh']}
gt_wit = witness_pairs(gt_pages)
gt_sigla = {s for p in gt_wit for s in p}
lines = [f"Tier-1 oracle: {len(gt_pages)} page pairs, {len(gt_wit)} witness pairs, "
         f"{len(gt_sigla)} witnesses connected"]

# ---- Q2a: DF sweep in candidate mode over the BH subset (density 0.30) ----
sweep = {}
for df in (None, 200, 100, 50, 30):
    t0 = time.time()
    verified, stats = engine.run(
        streams, ids, k=5, df_drop=df, posting_cap=2000, min_anchors=2,
        band=20, margin=30, min_span=25, max_density=0.30,
        exclude_same_key=keys)
    vp = {tuple(sorted((v['a'], v['b']))) for v in verified}
    wv = witness_pairs(vp)
    sigla_cov = {s for p in wv for s in p}
    sweep[str(df)] = {
        'verified_page_pairs': len(vp),
        'page_recall_vs_tier1': round(len(vp & gt_pages) / max(1, len(gt_pages)), 4),
        'witness_pairs': len(wv),
        'witness_recall_vs_tier1': round(len(wv & gt_wit) / max(1, len(gt_wit)), 4),
        'witnesses_connected': len(sigla_cov),
        'grams_dropped_df': stats['grams_dropped_df'],
        'time_s': round(time.time() - t0),
    }
    lines.append(f"df_drop={df}: {sweep[str(df)]}")

# ---- Q2b: DENSITY sweep (df=100) - Tier-1's 0.30 sits at the two-sided
# noise floor (CER ~15%/side => ~26-30% combined mismatch on identical text),
# and BH nusach variance adds real textual distance on top.
density_sweep = {}
for dens in (0.30, 0.35, 0.40, 0.45):
    t0 = time.time()
    verified, _ = engine.run(
        streams, ids, k=5, df_drop=100, posting_cap=2000, min_anchors=2,
        band=20, margin=30, min_span=25, max_density=dens,
        exclude_same_key=keys)
    vp = {tuple(sorted((v['a'], v['b']))) for v in verified}
    wv = witness_pairs(vp)
    sigla_cov = {s for p in wv for s in p}
    density_sweep[str(dens)] = {
        'verified_page_pairs': len(vp),
        'witness_pairs': len(wv),
        'witnesses_connected': f"{len(sigla_cov)}/428",
        'time_s': round(time.time() - t0),
    }
    lines.append(f"max_density={dens}: {density_sweep[str(dens)]}")

# ---- Q3: identification framing (full-pilot context, DF=100) ----
try:
    vpairs = json.load(open(VPAIRS, encoding='utf-8'))
    bh_pages_set = set(ids)
    best_partner = {}  # bh page -> (aligned_len, partner)
    for v in vpairs:
        for me, other in ((v['a'], v['b']), (v['b'], v['a'])):
            if me in bh_pages_set:
                cur = best_partner.get(me)
                if cur is None or v['aligned_len'] > cur[0]:
                    best_partner[me] = (v['aligned_len'], other)
    n_with_partner = len(best_partner)
    n_partner_is_bh = sum(
        1 for ln, other in best_partner.values()
        if sys_to_sigla.get(other.split('_')[0]))
    lines.append(
        f"identification: {n_with_partner}/{len(ids)} BH pages have a partner; "
        f"top partner is a known BH witness for {n_partner_is_bh} "
        f"({100 * n_partner_is_bh / max(1, n_with_partner):.0f}%)")
    ident = {'pages': len(ids), 'with_partner': n_with_partner,
             'partner_is_bh': n_partner_is_bh}
except FileNotFoundError:
    lines.append("identification: verified_pairs.json missing "
                 "(run separability.py first)")
    ident = None

json.dump({'tier1': {'page_pairs': len(gt_pages), 'witness_pairs': len(gt_wit),
                     'witnesses_connected': len(gt_sigla)},
           'df_sweep': sweep, 'identification': ident},
          open(OUT_JSON, 'w', encoding='utf-8'), indent=1)
open(OUT_TXT, 'w', encoding='utf-8').write("\n".join(lines))
print("\n".join(lines))
