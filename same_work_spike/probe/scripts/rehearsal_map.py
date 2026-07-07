# -*- coding: utf-8 -*-
"""Text-reuse map over rehearsal accepted_pairs: clusters + stats.

Usage: python rehearsal_map.py [db_path] [tag]
Outputs: results/rehearsal_<tag>_map.md (+ cluster CSV)
Graph layers (METHOD.md §"three maps"):
- ALL clean edges (dup-filtered)
- continuation-only  -> same-unit graph (works census)
- island-only        -> citation/formula graph
"""
import csv
import json
import sqlite3
import sys
from collections import Counter, defaultdict

import numpy as np
from scipy.sparse import coo_matrix
from scipy.sparse.csgraph import connected_components

ROOT = r"C:\Genizahsearch"
DB = sys.argv[1] if len(sys.argv) > 1 else \
    ROOT + r"\same_work_spike\probe\data\rehearsal.db"
TAG = sys.argv[2] if len(sys.argv) > 2 else "100k"
OUT = ROOT + rf"\same_work_spike\probe\results\rehearsal_{TAG}_map.md"
CSV_OUT = ROOT + rf"\same_work_spike\probe\results\rehearsal_{TAG}_clusters.csv"


def load_lib_meta():
    """sys_id -> (library_code, title)."""
    meta = {}
    with open(ROOT + r"\libraries.csv", encoding='utf-8-sig', newline='') as f:
        r = csv.reader(f)
        next(r, None)
        for row in r:
            if len(row) >= 4 and row[0]:
                title = row[7].strip() if len(row) >= 8 else ''
                meta[row[0]] = (row[3].strip() or '?', title)
    return meta


def components(ms_index, edges):
    n = len(ms_index)
    if not edges:
        return np.zeros(0, int), 0
    ea = np.array([e[0] for e in edges])
    eb = np.array([e[1] for e in edges])
    m = coo_matrix((np.ones(len(ea)), (ea, eb)), shape=(n, n))
    ncomp, labels = connected_components(m, directed=False)
    return labels, ncomp


def main():
    con = sqlite3.connect(DB)
    rows = con.execute("""
        SELECT sys_a, sys_b, bucket_a, bucket_b, aligned_len, density,
               dup_shelf, dup_lines, flank_dist
        FROM accepted_pairs""").fetchall()
    con.close()
    meta = load_lib_meta()

    # flank classes re-derived from raw distance (0.45 in-run cutoff was too
    # strict: two-sided-noise same-work flanks land 0.45-0.55, random ~0.6)
    def fclass(fd):
        if fd < 0:
            return 'edge'
        if fd <= 0.52:
            return 'continuation'
        return 'ambig' if fd <= 0.58 else 'island'

    # ---- page-pair -> manuscript-pair aggregation (clean only) ----
    ms_pairs = defaultdict(lambda: {'n': 0, 'best_len': 0, 'best_d': 1.0,
                                    'fc': Counter()})
    n_dup = 0
    fd_hist = Counter()
    for sa, sb, bka, bkb, alen, dens, dsh, dln, fd in rows:
        if dsh or dln >= 0.6:
            n_dup += 1
            continue
        if fd >= 0:
            fd_hist[round(fd * 20) / 20] += 1
        key = (sa, sb) if sa < sb else (sb, sa)
        r = ms_pairs[key]
        r['n'] += 1
        r['fc'][fclass(fd)] += 1
        if alen > r['best_len']:
            r['best_len'] = alen
        if dens < r['best_d']:
            r['best_d'] = dens

    ms_ids = sorted({s for k in ms_pairs for s in k})
    ms_index = {s: i for i, s in enumerate(ms_ids)}

    def edge_list(pred):
        return [(ms_index[a], ms_index[b]) for (a, b), r in ms_pairs.items()
                if pred(r)]

    layers = {
        'all': edge_list(lambda r: True),
        'continuation': edge_list(
            lambda r: r['fc']['continuation'] + r['fc']['edge']
            >= max(1, r['fc']['island'])),
        'island': edge_list(lambda r: r['fc']['island'] > 0),
    }
    lines_fd = [f"flank-dist histogram (clean page pairs, 0.05 bins): "
                f"{dict(sorted(fd_hist.items()))}"]

    lines = [f"# Text-reuse map — rehearsal '{TAG}'", ""]
    lines += [f"- accepted page pairs: {len(rows):,} "
              f"(duplicate-photography/shelfmark filtered: {n_dup:,})",
              f"- clean manuscript pairs: {len(ms_pairs):,} over "
              f"{len(ms_ids):,} manuscripts", ""]

    labels_by_layer = {}
    for name, edges in layers.items():
        labels, ncomp = components(ms_index, edges)
        labels_by_layer[name] = labels
        sizes = Counter(labels[ms_index[s]] for s in ms_ids
                        if len(labels)) if len(labels) else Counter()
        # count only components touched by an edge (size >= 2)
        comp_sizes = sorted((c for c in sizes.values() if c >= 2),
                            reverse=True)
        lines += [f"## Layer: {name}",
                  f"- edges (MS pairs): {len(edges):,}",
                  f"- components (>=2 MSS): {len(comp_sizes):,}",
                  f"- largest components: {comp_sizes[:15]}", ""]

    # ---- top clusters of the same-unit layer, with catalog metadata ----
    labels = labels_by_layer['continuation']
    if len(labels):
        comp_members = defaultdict(list)
        edge_ms = {s for a, b in layers['continuation']
                   for s in (ms_ids[a], ms_ids[b])}
        for s in ms_ids:
            if s in edge_ms:
                comp_members[labels[ms_index[s]]].append(s)
        top = sorted(comp_members.values(), key=len, reverse=True)[:20]
        lines.append("## Top same-unit clusters (continuation layer)")
        for ci, members in enumerate(top):
            libs = Counter(meta.get(s, ('?', ''))[0] for s in members)
            titles = Counter(t for s in members
                             for t in [meta.get(s, ('?', ''))[1]] if t)
            tt = '; '.join(f"{t} ({c})" for t, c in titles.most_common(3))
            lines.append(f"- **C{ci + 1}** — {len(members)} MSS; libraries "
                         f"{dict(libs.most_common(5))}; titles: {tt or '—'}")
        lines.append("")

    # ---- library x library matrix (clean MS pairs, cross-library) ----
    libmat = Counter()
    for (a, b), r in ms_pairs.items():
        la = meta.get(a, ('?', ''))[0]
        lb = meta.get(b, ('?', ''))[0]
        libmat[tuple(sorted((la, lb)))] += 1
    lines.append("## Library x library (clean MS pairs, top 20)")
    for (la, lb), c in libmat.most_common(20):
        lines.append(f"- {la} — {lb}: {c:,}")
    lines.append("")

    # ---- flank-class distribution over page pairs ----
    fc = Counter(fclass(r[8]) for r in rows if not (r[6] or r[7] >= 0.6))
    lines.append(f"## Flank classes (clean page pairs): {dict(fc)}")
    lines += lines_fd

    # ---- cluster CSV (same-unit layer) ----
    with open(CSV_OUT, 'w', encoding='utf-8-sig', newline='') as f:
        w = csv.writer(f)
        w.writerow(['cluster', 'sys_id', 'library', 'title'])
        if len(labels):
            for comp, members in sorted(comp_members.items(),
                                        key=lambda kv: -len(kv[1])):
                if len(members) < 2:
                    continue
                for s in sorted(members):
                    la, ti = meta.get(s, ('?', ''))
                    w.writerow([comp, s, la, ti])

    open(OUT, 'w', encoding='utf-8').write("\n".join(lines))
    print("\n".join(lines[:60]))
    print(f"\nwrote {OUT}\n      {CSV_OUT}")


if __name__ == '__main__':
    main()
