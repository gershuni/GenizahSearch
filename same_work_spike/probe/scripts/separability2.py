# -*- coding: utf-8 -*-
"""Separability v2: diagonal-keyed candidate stage (engine.build_diag_pairs)
over the full pilot, verified at max_density=0.50 to expose the boundary tail.

Outputs:
- results/separability2_report.txt / .json
- results/verified_pairs_d50.json  (with class + family labels, for ROC fit)
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
import os  # noqa: E402
CAP = int(os.environ.get('PROBE_GRAM_PAIR_CAP', '8'))
SUFFIX = '' if CAP == 8 else f'_cap{CAP}'
PROBE_DB = ROOT + r"\same_work_spike\probe\data\probe.db"
BUCKETS = ROOT + r"\same_work_spike\probe\data\buckets.json"
BH = ROOT + r"\same_work_spike\probe\data\bh_witnesses.json"
TIER1 = ROOT + r"\same_work_spike\probe\results\tier1.json"
OUT_JSON = (ROOT + r"\same_work_spike\probe\results\separability2_report{}.json"
            ).format(SUFFIX)
OUT_TXT = (ROOT + r"\same_work_spike\probe\results\separability2_report{}.txt"
           ).format(SUFFIX)
OUT_PAIRS = (ROOT + r"\same_work_spike\probe\results\verified_pairs_d50{}.json"
             ).format(SUFFIX)

CAND = dict(k=5, df_drop=100, posting_cap=2000, min_anchors=2, band=20,
            per_gram_pair_cap=CAP)
VER = dict(k=5, margin=30, min_span=25, max_density=0.50)

t0 = time.time()
con = sqlite3.connect(PROBE_DB)
ids, streams, keys, bucket_of = [], [], [], {}
for pid, sid, bks in con.execute("SELECT page_id, sys_id, buckets FROM pages"):
    row = con.execute("SELECT text FROM pages WHERE page_id=?", (pid,)).fetchone()
    s, _ = norm_stream(row[0])
    if len(s) < 60:
        continue
    ids.append(pid)
    streams.append(s)
    keys.append(sid)
    bucket_of[pid] = bks
print(f"pilot corpus: {len(ids)} pages ({time.time()-t0:.0f}s)")

b = json.load(open(BUCKETS, encoding='utf-8'))
bh = json.load(open(BH, encoding='utf-8'))
group_of_sys = defaultdict(set)
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


t0 = time.time()
candidates, stats = engine.build_diag_pairs(
    streams, exclude_same_key=keys, **CAND)
t_cand = time.time() - t0
print(f"diag candidates: {len(candidates)} [{t_cand:.0f}s] stats={stats}")

t0 = time.time()
verified = []
for (pa, pb), ext in candidates.items():
    v = engine.verify_span(streams[pa], streams[pb], ext, **VER)
    if v:
        v['a'], v['b'] = ids[pa], ids[pb]
        verified.append(v)
t_ver = time.time() - t0
print(f"verified@0.50: {len(verified)} [{t_ver:.0f}s]")

# recall vs tier1 at candidate stage and at density levels
tier1 = json.load(open(TIER1, encoding='utf-8'))
cand_set = {tuple(sorted((ids[pa], ids[pb]))) for pa, pb in candidates}
ver_by_pair = {tuple(sorted((v['a'], v['b']))): v for v in verified}
recall = {}
for fam, pairs in tier1.items():
    gt = {tuple(sorted((p['a'], p['b']))) for p in pairs}
    if not gt:
        continue
    recall[fam] = {
        'gt_pairs': len(gt),
        'candidate_recall': round(len(gt & cand_set) / len(gt), 4),
    }
    for dens in (0.30, 0.35, 0.40, 0.45, 0.50):
        n = sum(1 for p in gt
                if p in ver_by_pair and ver_by_pair[p]['density'] <= dens)
        recall[fam][f'verified_recall@{dens}'] = round(n / len(gt), 4)

# label pairs for the ROC fit
tier1_all = {}
for fam, pairs in tier1.items():
    for p in pairs:
        tier1_all[tuple(sorted((p['a'], p['b'])))] = fam
out_pairs = []
classes = defaultdict(int)
for v in verified:
    kp = tuple(sorted((v['a'], v['b'])))
    fam = tier1_all.get(kp)
    if fam:
        cls = f'tier1_{fam}'
    elif same_group(v['a'], v['b']):
        cls = 'related_new'
    elif v['density'] <= 0.02:
        cls = 'duplicate'
    else:
        cls = 'cross'
    classes[cls] += 1
    out_pairs.append({'a': v['a'], 'b': v['b'], 'len': v['aligned_len'],
                      'density': v['density'], 'cls': cls,
                      'bg': int(bucket_of[v['a']] == 'background' or
                                bucket_of[v['b']] == 'background')})
json.dump(out_pairs, open(OUT_PAIRS, 'w', encoding='utf-8'))

report = {
    'corpus_pages': len(ids),
    'cand_params': CAND, 'verify_params': VER,
    'engine_stats': stats,
    'timing_s': {'candidates': round(t_cand), 'verify': round(t_ver)},
    'candidate_pairs': len(candidates),
    'candidate_pairs_v1_was': 31744866,
    'recall_vs_tier1': recall,
    'verified_at_050': len(verified),
    'classes': dict(classes),
}
json.dump(report, open(OUT_JSON, 'w'), indent=1)
txt = json.dumps(report, indent=1)
open(OUT_TXT, 'w', encoding='utf-8').write(txt)
print(txt)
