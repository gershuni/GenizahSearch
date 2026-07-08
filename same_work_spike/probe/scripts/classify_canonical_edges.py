# -*- coding: utf-8 -*-
"""Edge-level canonical classification — the mask-strictness alternative.

Character-masking (maskcanon) guts ~27K mid-band pages that quote canon
without BEING canon (mask_severity_full.md). This tests the gentler
regime on the UNMASKED run: classify each accepted span by its overlap
with Track-1 canonical intervals on BOTH pages —
  canonical : both sides >= 70% inside canonical spans (shared quotation)
  clean     : both sides <= 30%
  mixed     : everything else (verse-interwoven liturgy etc.)
— then rebuild the map metrics EXCLUDING only 'canonical' edges, and
compare against the char-masked (canonmask) and raw (unmasked) regimes:
does edge-exclusion keep BH/liturgy connectivity while still collapsing
the CUL-RNL citation core?

Usage: python classify_canonical_edges.py [db] [tag] [pairs_table]
Out: results/edge_class_<tag>.md; adds edge_canon_class column-like aux
     table <pairs_table>_canonclass (rowid, class, fa, fb)
"""
import json
import sqlite3
import sys
import time
from collections import Counter, defaultdict

import numpy as np
from scipy.sparse import coo_matrix
from scipy.sparse.csgraph import connected_components

ROOT = r"C:\Genizahsearch"
DB = sys.argv[1] if len(sys.argv) > 1 else \
    ROOT + r"\same_work_spike\probe\data\fullcorpus.db"
TAG = sys.argv[2] if len(sys.argv) > 2 else "fullnomask"
TABLE = sys.argv[3] if len(sys.argv) > 3 else "accepted_pairs"
OUT = ROOT + rf"\same_work_spike\probe\results\edge_class_{TAG}.md"
CANON_CATS = ('Bible', 'Mishnah', 'Tosefta', 'Bavli', 'Yerushalmi')
T_CANON, T_CLEAN = 0.70, 0.30


def load_lib(sys_ids):
    import csv
    lib = {}
    with open(ROOT + r"\libraries.csv", encoding='utf-8-sig', newline='') as f:
        r = csv.reader(f)
        next(r, None)
        for row in r:
            if len(row) >= 4 and row[0] and row[0] in sys_ids:
                lib[row[0]] = row[3].strip() or '?'
    return lib


def overlap_frac(s, e, intervals):
    if not intervals:
        return 0.0
    ov = 0
    for q0, q1 in intervals:
        ov += max(0, min(e, q1) - max(s, q0))
    return ov / max(1, e - s)


def merge_intervals(iv):
    iv.sort()
    out = []
    for a, b in iv:
        if out and a <= out[-1][1]:
            out[-1][1] = max(out[-1][1], b)
        else:
            out.append([a, b])
    return out


def components_stats(edge_set, name):
    nodes = sorted({s for e in edge_set for s in e})
    idx = {s: i for i, s in enumerate(nodes)}
    if not edge_set:
        return f"- {name}: 0 edges"
    ea = np.array([idx[a] for a, b in edge_set])
    eb = np.array([idx[b] for a, b in edge_set])
    m = coo_matrix((np.ones(len(ea)), (ea, eb)),
                   shape=(len(nodes), len(nodes)))
    _, labels = connected_components(m, directed=False)
    sizes = sorted(Counter(labels).values(), reverse=True)
    return (f"- {name}: {len(edge_set):,} MS-pair edges, "
            f"{len(nodes):,} MSS, largest components {sizes[:8]}")


def main():
    t0 = time.time()
    con = sqlite3.connect(DB)
    # canonical intervals per page
    canon = defaultdict(list)
    for pid, spans_json in con.execute(
            "SELECT page_id, spans_json FROM track1_matches WHERE cat IN ("
            + ",".join(f"'{c}'" for c in CANON_CATS) + ")"):
        canon[pid].extend((int(s[0]), int(s[1]))
                          for s in json.loads(spans_json))
    for pid in canon:
        canon[pid] = merge_intervals(canon[pid])
    print(f"canonical pages: {len(canon):,} ({time.time() - t0:.0f}s)",
          flush=True)

    rows = con.execute(f"""
        SELECT rowid, page_a, page_b, sys_a, sys_b, a0, a1, b0, b1,
               bucket_a, bucket_b, dup_shelf, dup_lines, flank_dist
        FROM {TABLE}""").fetchall()
    print(f"pairs: {len(rows):,}", flush=True)

    aux = []
    cls_count = Counter()
    ms_edges = defaultdict(Counter)   # (sysa,sysb) -> class counts
    ms_fclass = defaultdict(Counter)  # continuation evidence per pair
    bh_pairs_all = set()
    bh_pairs_kept = set()
    for (rid, pa, pb, sa, sb, a0, a1, b0, b1, bka, bkb,
         dsh, dln, fd) in rows:
        fa = overlap_frac(a0, a1, canon.get(pa, ()))
        fb = overlap_frac(b0, b1, canon.get(pb, ()))
        c = ('canonical' if fa >= T_CANON and fb >= T_CANON else
             'clean' if fa <= T_CLEAN and fb <= T_CLEAN else 'mixed')
        aux.append((rid, c, round(fa, 3), round(fb, 3)))
        cls_count[c] += 1
        if dsh or dln >= 0.6:
            continue
        key = (sa, sb) if sa < sb else (sb, sa)
        ms_edges[key][c] += 1
        fclass = ('edge' if fd < 0 else
                  'continuation' if fd <= 0.52 else
                  'ambig' if fd <= 0.58 else 'island')
        if c != 'canonical':
            ms_fclass[key][fclass] += 1
        if 'bh' in bka and 'bh' in bkb:
            bh_pairs_all.add(key)
            if c != 'canonical':
                bh_pairs_kept.add(key)
    print(f"classified ({time.time() - t0:.0f}s): {dict(cls_count)}",
          flush=True)

    con.execute(f"DROP TABLE IF EXISTS {TABLE}_canonclass")
    con.execute(f"CREATE TABLE {TABLE}_canonclass "
                f"(rowid INT, class TEXT, fa REAL, fb REAL)")
    con.executemany(f"INSERT INTO {TABLE}_canonclass VALUES (?,?,?,?)", aux)
    con.commit()

    # ---- metrics under three edge sets ----
    all_edges = set(ms_edges)
    kept_edges = {k for k, cc in ms_edges.items()
                  if cc['clean'] + cc['mixed'] > 0}
    cont_edges = {k for k, cc in ms_fclass.items()
                  if cc['continuation'] + cc['edge']
                  >= max(1, cc['island'])}
    lib = load_lib({s for k in all_edges for s in k})

    def culrnl(edges):
        return sum(1 for (a, b) in edges
                   if {lib.get(a, '?'), lib.get(b, '?')} == {'CUL', 'RNL'})

    bh_ms_all = {s for k in bh_pairs_all for s in k}
    bh_ms_kept = {s for k in bh_pairs_kept for s in k}
    lines = [
        f"# Canonical edge classification — '{TAG}' over {TABLE}", "",
        f"- accepted page pairs: {len(rows):,}; classes: "
        f"{dict(cls_count)} (thresholds: canonical both>= {T_CANON}, "
        f"clean both<= {T_CLEAN})", "",
        "## MS-pair edge sets",
        components_stats(all_edges, "ALL edges (raw unmasked)"),
        components_stats(kept_edges,
                         "canonical-EXCLUDED (clean+mixed edges only)"),
        components_stats(cont_edges,
                         "continuation layer of canonical-excluded"),
        "",
        "## Citation-core collapse (CUL–RNL cross links)",
        f"- raw unmasked: {culrnl(all_edges):,}",
        f"- canonical-excluded: {culrnl(kept_edges):,}",
        f"- (char-masked canonmask reference: 4,894 at 100K-scale ratio; "
        f"see rehearsal_full_map.md for full)",
        "",
        "## BH witness connectivity (direct bh-page pairs)",
        f"- raw unmasked: {len(bh_ms_all)} sys connected",
        f"- canonical-excluded: {len(bh_ms_kept)} sys connected",
        f"- (char-masked full canonmask reference: 166/512; "
        f"unmasked 100K reference: 326/512)",
    ]
    open(OUT, 'w', encoding='utf-8').write('\n'.join(lines))
    print('\n'.join(lines))
    con.close()


if __name__ == '__main__':
    main()
