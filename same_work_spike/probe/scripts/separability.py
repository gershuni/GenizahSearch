# -*- coding: utf-8 -*-
"""The separability probe: candidate-mode engine over the FULL pilot corpus
(target pages + 10K background), measured against Tier-1 ground truth.

Outputs:
- results/separability.json   (all numbers)
- results/separability.png    (scatter: aligned_len x density by class)
- results/discoveries.txt     (top cross-group verified pairs, Hebrew spans,
                               for manual precision inspection)
"""
import json
import sqlite3
import sys
import time
import unicodedata
from collections import defaultdict

sys.path.insert(0, r"C:\Genizahsearch\same_work_spike\probe\scripts")
from normalize import norm_stream, project_span  # noqa: E402
import engine  # noqa: E402

ROOT = r"C:\Genizahsearch"
PROBE_DB = ROOT + r"\same_work_spike\probe\data\probe.db"
BUCKETS = ROOT + r"\same_work_spike\probe\data\buckets.json"
BH = ROOT + r"\same_work_spike\probe\data\bh_witnesses.json"
TIER1 = ROOT + r"\same_work_spike\probe\results\tier1.json"
OUT_JSON = ROOT + r"\same_work_spike\probe\results\separability.json"
OUT_PNG = ROOT + r"\same_work_spike\probe\results\separability.png"
OUT_DISC = ROOT + r"\same_work_spike\probe\results\discoveries.txt"
OUT_TXT = ROOT + r"\same_work_spike\probe\results\separability_report.txt"

CAND = dict(k=5, df_drop=100, posting_cap=2000, min_anchors=2,
            band=20, margin=30, min_span=25, max_density=0.30)

t0 = time.time()
con = sqlite3.connect(PROBE_DB)
ids, streams, keys, bucket_of, text_of, offs_of = [], [], [], {}, {}, {}
for pid, sid, bks, text in con.execute(
        "SELECT page_id, sys_id, buckets, text FROM pages"):
    s, offs = norm_stream(text)
    if len(s) < 60:
        continue
    ids.append(pid)
    streams.append(s)
    keys.append(sid)
    bucket_of[pid] = bks
    text_of[pid] = unicodedata.normalize('NFC', text)
    offs_of[pid] = offs
print(f"pilot corpus: {len(ids)} pages ({time.time()-t0:.0f}s to normalize)")

# group membership for classification
b = json.load(open(BUCKETS, encoding='utf-8'))
bh = json.load(open(BH, encoding='utf-8'))
group_of_sys = defaultdict(set)  # sys_id -> set of group labels
for gid, members in b['joins'].items():
    for s in members:
        group_of_sys[s].add(f"join:{gid}")
for tid, t in b['titles'].items():
    for s in t['sys_ids']:
        group_of_sys[s].add(f"title:{tid}")
for sig, w in bh['witnesses'].items():
    for sm in w['shelfmarks']:
        for s in sm.get('sys_ids', []):
            group_of_sys[s].add("bh")


def same_group(pid_a, pid_b):
    sa, sb = pid_a.split('_')[0], pid_b.split('_')[0]
    return bool(group_of_sys.get(sa, set()) & group_of_sys.get(sb, set()))


# ---- run candidate mode ----
t0 = time.time()
pair_anchors, stats = engine.build_anchor_pairs(
    streams, k=CAND['k'], df_drop=CAND['df_drop'],
    posting_cap=CAND['posting_cap'], exclude_same_key=keys,
    min_anchors=CAND['min_anchors'])
t_cand = time.time() - t0
print(f"candidates: {len(pair_anchors)} pairs [{t_cand:.0f}s] stats={stats}")

t0 = time.time()
verified = []
idx_of = {pid: i for i, pid in enumerate(ids)}
for (pa, pb), anchors in pair_anchors.items():
    v = engine.verify_pair(streams[pa], streams[pb], anchors,
                           k=CAND['k'], band=CAND['band'], margin=CAND['margin'],
                           min_span=CAND['min_span'],
                           max_density=CAND['max_density'],
                           min_anchors=CAND['min_anchors'])
    if v:
        v['a'], v['b'] = ids[pa], ids[pb]
        verified.append(v)
t_ver = time.time() - t0
print(f"verified: {len(verified)} pairs [{t_ver:.0f}s]")

# ---- recall vs Tier-1 ----
tier1 = json.load(open(TIER1, encoding='utf-8'))
cand_set = {tuple(sorted((ids[pa], ids[pb]))) for pa, pb in pair_anchors}
ver_set = {tuple(sorted((v['a'], v['b']))) for v in verified}
recall = {}
for fam, pairs in tier1.items():
    gt = {tuple(sorted((p['a'], p['b']))) for p in pairs}
    if not gt:
        continue
    in_cand = len(gt & cand_set)
    in_ver = len(gt & ver_set)
    recall[fam] = {
        'gt_pairs': len(gt),
        'candidate_recall': round(in_cand / len(gt), 4),
        'verified_recall': round(in_ver / len(gt), 4),
    }
    # recall weighted toward substantial overlaps (aligned_len >= 60)
    gt_big = {tuple(sorted((p['a'], p['b']))) for p in pairs
              if p['aligned_len'] >= 60}
    if gt_big:
        recall[fam]['candidate_recall_len60'] = round(
            len(gt_big & cand_set) / len(gt_big), 4)
        recall[fam]['verified_recall_len60'] = round(
            len(gt_big & ver_set) / len(gt_big), 4)

# ---- classify verified pairs & volume ----
classes = defaultdict(int)
per_page_cand = defaultdict(int)
for pa, pb in pair_anchors:
    per_page_cand[ids[pa]] += 1
    per_page_cand[ids[pb]] += 1
bg_involved = sum(1 for v in verified
                  if bucket_of[v['a']] == 'background' or
                  bucket_of[v['b']] == 'background')
scatter = {'tier1': [], 'related_new': [], 'cross': []}
tier1_all = set()
for fam, pairs in tier1.items():
    tier1_all |= {tuple(sorted((p['a'], p['b']))) for p in pairs}
cross_pairs = []
for v in verified:
    key = tuple(sorted((v['a'], v['b'])))
    if key in tier1_all:
        cls = 'tier1'
    elif same_group(v['a'], v['b']):
        cls = 'related_new'
    else:
        cls = 'cross'
        cross_pairs.append(v)
    classes[cls] += 1
    scatter[cls].append((v['aligned_len'], v['density']))

vol = sorted(per_page_cand.values())
volume = {
    'pages_with_candidates': len(per_page_cand),
    'median_cand_per_page': vol[len(vol) // 2] if vol else 0,
    'p90_cand_per_page': vol[int(0.9 * len(vol))] if vol else 0,
    'max_cand_per_page': vol[-1] if vol else 0,
    'total_candidates': len(pair_anchors),
    'total_verified': len(verified),
    'verified_with_background': bg_involved,
}

# ---- scatter plot ----
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
fig, ax = plt.subplots(figsize=(10, 6))
for cls, color, alpha, size in (
        ('cross', '#999999', 0.35, 8),
        ('related_new', '#e8a33d', 0.5, 10),
        ('tier1', '#2e7d32', 0.6, 12)):
    pts = scatter[cls]
    if pts:
        xs, ys = zip(*pts)
        ax.scatter(xs, ys, c=color, alpha=alpha, s=size,
                   label=f"{cls} (n={len(pts)})")
ax.set_xscale('log')
ax.set_xlabel('aligned span length (normalized letters, log)')
ax.set_ylabel('edit density')
ax.set_title('Separability: verified candidate pairs (pilot corpus, k=5 DF=100 two-hit)')
ax.legend()
ax.grid(alpha=0.3)
fig.tight_layout()
fig.savefig(OUT_PNG, dpi=130)

# ---- discoveries dump (top cross pairs by aligned_len, Hebrew spans) ----
cross_pairs.sort(key=lambda v: -v['aligned_len'])
disc = []
for v in cross_pairs[:25]:
    ta, tb = text_of[v['a']], text_of[v['b']]
    oa, ob = offs_of[v['a']], offs_of[v['b']]
    disc.append(
        f"--- {v['a']} [{bucket_of[v['a']]}] <-> {v['b']} [{bucket_of[v['b']]}] "
        f"len={v['aligned_len']} density={v['density']} anchors={v['n_anchors']}\n"
        f"A: {project_span(oa, v['a0'], v['a1'], ta, pad=5)}\n"
        f"B: {project_span(ob, v['b0'], v['b1'], tb, pad=5)}\n")
open(OUT_DISC, 'w', encoding='utf-8').write("\n".join(disc))

json.dump(verified, open(
    ROOT + r"\same_work_spike\probe\results\verified_pairs.json",
    'w', encoding='utf-8'))

report = {
    'corpus_pages': len(ids),
    'engine_params': CAND,
    'engine_stats': stats,
    'timing_s': {'candidates': round(t_cand), 'verify': round(t_ver)},
    'recall_vs_tier1': recall,
    'volume': volume,
    'verified_classes': dict(classes),
}
json.dump(report, open(OUT_JSON, 'w', encoding='utf-8'), indent=1)
txt = json.dumps(report, indent=1)
open(OUT_TXT, 'w', encoding='utf-8').write(txt)
print(txt)
