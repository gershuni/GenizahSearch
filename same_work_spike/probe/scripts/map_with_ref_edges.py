# -*- coding: utf-8 -*-
"""Reference-edge layer: fold Track-1 witness webs into the works census.

For reference-covered works, Track-1 already connects all witnesses —
but the map only ever drew Track-2 (pairwise alignment) edges, so
DF-cap-starved short works (df_damage_full.md) looked disconnected.
This merges the two edge sources at the manuscript level:
- Track-2: clean continuation-qualified MS pairs (canonmask map)
- Track-1: same-work links (edited works, >= 200 matched letters,
  >= 2 witness MSS; canonical works stay in the testimony census)

Usage: python map_with_ref_edges.py [db]
Out: results/map_ref_edges_full.md
"""
import json
import sqlite3
import sys
from collections import Counter, defaultdict

ROOT = r"C:\Genizahsearch"
DB = sys.argv[1] if len(sys.argv) > 1 else \
    ROOT + r"\same_work_spike\probe\data\fullcorpus.db"
OUT = ROOT + r"\same_work_spike\probe\results\map_ref_edges_full.md"
TABLE = "accepted_pairs_canonmask"
CANON_CATS = {'Bible', 'Mishnah', 'Tosefta', 'Bavli', 'Yerushalmi'}
MIN_LETTERS = 200
# witness-count plausibility flag (df_damage finding): works with wildly
# implausible witness counts are Track-1 review items, not census edges
SUSPECT_MIN_MS = 150


class DSU:
    def __init__(self):
        self.p = {}

    def find(self, x):
        p = self.p
        p.setdefault(x, x)
        while p[x] != x:
            p[x] = p[p[x]]
            x = p[x]
        return x

    def union(self, a, b):
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self.p[rb] = ra


def comp_stats(dsu, nodes):
    sizes = Counter(dsu.find(s) for s in nodes)
    top = sorted(sizes.values(), reverse=True)
    return len([c for c in top if c >= 2]), top[:8]


def main():
    con = sqlite3.connect(DB)

    # ---- Track-2 continuation-qualified MS edges ----
    ms_fc = defaultdict(Counter)
    for sa, sb, fd, dsh, dln in con.execute(f"""
            SELECT sys_a, sys_b, flank_dist, dup_shelf, dup_lines
            FROM {TABLE}"""):
        if dsh or dln >= 0.6:
            continue
        key = (sa, sb) if sa < sb else (sb, sa)
        fc = ('edge' if fd < 0 else 'continuation' if fd <= 0.52
              else 'ambig' if fd <= 0.58 else 'island')
        ms_fc[key][fc] += 1
    t2_edges = [k for k, cc in ms_fc.items()
                if cc['continuation'] + cc['edge'] >= max(1, cc['island'])]
    print(f"Track-2 continuation edges: {len(t2_edges):,}")

    # ---- Track-1 witness webs (edited works) ----
    work_ms = defaultdict(set)
    work_name = {}
    t1cols = [r[1] for r in con.execute("PRAGMA table_info(track1_matches)")]
    live = (" WHERE shadowed_by IS NULL" if 'shadowed_by' in t1cols else "")
    for sid, wid, cat, author, title, letters in con.execute(
            "SELECT sys_id, work_id, cat, author, title, matched_letters "
            f"FROM track1_matches{live}"):
        if letters < MIN_LETTERS or cat in CANON_CATS:
            continue
        work_ms[wid].add(sid)
        work_name[wid] = f"{author} — {title}" if author else title
    webs = {w: ms for w, ms in work_ms.items()
            if 2 <= len(ms) < SUSPECT_MIN_MS}
    suspects = {w: len(ms) for w, ms in work_ms.items()
                if len(ms) >= SUSPECT_MIN_MS}
    print(f"Track-1 witness webs: {len(webs):,} works "
          f"({sum(len(m) for m in webs.values()):,} memberships); "
          f"suspect works excluded: {len(suspects)}")

    # ---- BH ground truth ----
    bh = json.load(open(
        ROOT + r"\same_work_spike\probe\data\bh_witnesses.json",
        encoding='utf-8'))
    bh_sys = set()
    for sig, w in bh['witnesses'].items():
        for sm in w['shelfmarks']:
            bh_sys.update(sm.get('sys_ids', []))
    present = {r[0] for r in con.execute(
        "SELECT DISTINCT sys_id FROM pages")}
    bh_present = bh_sys & present

    def bh_connected(dsu):
        roots = Counter()
        for s in bh_present:
            if s in dsu.p:
                roots[dsu.find(s)] += 1
        return sum(c for c in roots.values() if c >= 2)

    # ---- merge ----
    d2 = DSU()
    for a, b in t2_edges:
        d2.union(a, b)
    nodes2 = set(d2.p)
    n2, top2 = comp_stats(d2, nodes2)
    bh2 = bh_connected(d2)

    dm = DSU()
    for a, b in t2_edges:
        dm.union(a, b)
    for wid, ms in webs.items():
        it = iter(ms)
        first = next(it)
        for s in it:
            dm.union(first, s)
    nodesm = set(dm.p)
    nm, topm = comp_stats(dm, nodesm)
    bhm = bh_connected(dm)

    lines = [
        "# Works census with reference edges (Track-2 ∪ Track-1 webs)", "",
        f"| | Track-2 only | + reference edges |",
        f"|---|---|---|",
        f"| connected MSS | {len(nodes2):,} | {len(nodesm):,} |",
        f"| components (>=2) | {n2:,} | {nm:,} |",
        f"| largest components | {top2} | {topm} |",
        f"| BH witnesses in >=2-witness components | {bh2} | {bhm} |",
        "",
        f"- reference webs added: {len(webs):,} works "
        f"(2 <= witnesses < {SUSPECT_MIN_MS})",
        f"- suspect works EXCLUDED (>= {SUSPECT_MIN_MS} witness MSS — "
        f"Track-1 review queue):",
    ]
    for w, n in sorted(suspects.items(), key=lambda kv: -kv[1]):
        lines.append(f"  - {work_name[w][:70]}: {n} MSS")
    open(OUT, 'w', encoding='utf-8').write('\n'.join(lines))
    print('\n'.join(lines[:20]))
    con.close()


if __name__ == '__main__':
    main()
