# -*- coding: utf-8 -*-
"""Tier-1 ground truth: near-exhaustive verified shared-wording pairs WITHIN
known-related groups (join groups / title groups / the BH witness set).

Permissive engine mode: k=4, no DF drop (posting cap only), min_anchors=1.
Output: results/tier1.json + results/tier1_report.txt
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
BUCKETS = ROOT + r"\same_work_spike\probe\data\buckets.json"
BH = ROOT + r"\same_work_spike\probe\data\bh_witnesses.json"
OUT = ROOT + r"\same_work_spike\probe\results\tier1.json"
REPORT = ROOT + r"\same_work_spike\probe\results\tier1_report.txt"

# min_anchors=2 (not 1): at k=4 / CER 20% two-sided, a true >=25-char shared
# span yields ~4 expected clean anchors -> P(>=2) is high; and 2+ anchors is
# what makes the memory-guarded accumulation tractable on liturgical text.
GT = dict(k=4, df_drop=None, posting_cap=800, min_anchors=2,
          band=20, margin=30, min_span=25, max_density=0.30)

con = sqlite3.connect(PROBE_DB)
pages_by_sys = defaultdict(list)  # sys_id -> [(page_id, stream)]
for pid, sid, text in con.execute(
        "SELECT page_id, sys_id, text FROM pages WHERE buckets != 'background'"):
    s, _ = norm_stream(text)
    if len(s) >= 60:
        pages_by_sys[sid].append((pid, s))

b = json.load(open(BUCKETS, encoding='utf-8'))
bh = json.load(open(BH, encoding='utf-8'))

lines = []
results = {}


def run_group_family(name, groups):
    """groups: dict group_id -> list[sys_id]. Verified pairs across DIFFERENT
    sys_ids within each group."""
    t0 = time.time()
    fam_pairs = []
    n_groups_with_pages = 0
    n_groups_connected = 0
    for gid, members in groups.items():
        ids, streams, keys = [], [], []
        for sid in members:
            for pid, s in pages_by_sys.get(sid, []):
                ids.append(pid)
                streams.append(s)
                keys.append(sid)
        if len(set(keys)) < 2:
            continue
        n_groups_with_pages += 1
        verified, _ = engine.run(streams, ids, exclude_same_key=keys, **GT)
        for v in verified:
            v['group'] = gid
        fam_pairs.extend(verified)
        if verified:
            n_groups_connected += 1
    dt = time.time() - t0
    lines.append(
        f"{name}: {len(groups)} groups, {n_groups_with_pages} with pages from "
        f">=2 MSS, {n_groups_connected} with >=1 verified cross-MS pair "
        f"({100 * n_groups_connected / max(1, n_groups_with_pages):.0f}%), "
        f"{len(fam_pairs)} verified page pairs [{dt:.0f}s]")
    results[name] = fam_pairs
    return fam_pairs


# ---- join groups ----
run_group_family('joins', b['joins'])

# ---- title groups ----
title_groups = {tid: t['sys_ids'] for tid, t in b['titles'].items()}
run_group_family('titles', title_groups)
# per-title connectivity detail
for tid, t in b['titles'].items():
    pairs = [p for p in results['titles'] if p['group'] == tid]
    mss_here = {s for s in t['sys_ids'] if s in pages_by_sys}
    connected_mss = set()
    for p in pairs:
        connected_mss.add(p['a'].split('_')[0])
        connected_mss.add(p['b'].split('_')[0])
    lines.append(f"  title {tid} ({t['name'][:35]}): {len(mss_here)} MSS with pages, "
                 f"{len(connected_mss)} in verified pairs, {len(pairs)} page pairs")

# ---- BH witness set (one big group; cross-sys pairs) ----
sys_to_sigla = defaultdict(set)
for sig, w in bh['witnesses'].items():
    for sm in w['shelfmarks']:
        for s in sm.get('sys_ids', []):
            sys_to_sigla[s].add(sig)

t0 = time.time()
ids, streams, keys = [], [], []
for sid in sorted(sys_to_sigla):
    for pid, s in pages_by_sys.get(sid, []):
        ids.append(pid)
        streams.append(s)
        keys.append(sid)
print(f"BH: {len(ids)} pages from {len(set(keys))} sys_ids; running GT engine...")
bh_verified, bh_stats = engine.run(streams, ids, exclude_same_key=keys, **GT)
dt = time.time() - t0
results['bh'] = bh_verified

# connectivity: witnesses (sigla) connected by >=1 verified pair
sig_pairs = set()
connected_sigla = set()
for v in bh_verified:
    sa, sb = v['a'].split('_')[0], v['b'].split('_')[0]
    for siga in sys_to_sigla[sa]:
        for sigb in sys_to_sigla[sb]:
            if siga != sigb:
                sig_pairs.add(tuple(sorted((siga, sigb))))
                connected_sigla.add(siga)
                connected_sigla.add(sigb)
sigla_with_pages = {sig for s, sigs in sys_to_sigla.items()
                    if s in pages_by_sys for sig in sigs}
lines.append(
    f"bh: {len(ids)} pages / {len(set(keys))} sys_ids / {len(sigla_with_pages)} "
    f"witnesses with pages; verified page pairs: {len(bh_verified)}; "
    f"witness-level pairs: {len(sig_pairs)}; witnesses connected: "
    f"{len(connected_sigla)}/{len(sigla_with_pages)} "
    f"({100 * len(connected_sigla) / max(1, len(sigla_with_pages)):.0f}%) [{dt:.0f}s]")
lines.append(f"bh engine stats: {bh_stats}")

json.dump(results, open(OUT, 'w', encoding='utf-8'), ensure_ascii=False)
open(REPORT, 'w', encoding='utf-8').write("\n".join(lines))
print("\n".join(lines))
