# -*- coding: utf-8 -*-
"""Quick triage of top motif-query growth: what ARE the big gainers?

For each top-growth motif: representative text, Track-1 identification
of its pilot member pages (live rows only), density profile of the new
hits. Separates 'known work found its census' from 'unidentified text
grew' (the fragmentary-prize class).
"""
import json
import sqlite3
from collections import Counter, defaultdict

ROOT = r"C:\Genizahsearch"
DB = ROOT + r"\same_work_spike\probe\data\fullcorpus.db"
TOP = 30

con = sqlite3.connect(DB)

# growth per motif
members = defaultdict(set)
for m, sid in con.execute(
        "SELECT motif, sys_id FROM motif_members_pilot"):
    members[m].add(sid)
new_ms = defaultdict(set)
dens = defaultdict(list)
for m, sid, d in con.execute(
        "SELECT motif, sys_id, best_density FROM motif_query_hits"):
    if sid not in members[m]:
        new_ms[m].add(sid)
    dens[m].append(d)
growth = sorted(((len(v), m) for m, v in new_ms.items()), reverse=True)

# Track-1 identification of each motif's pilot pages (live rows)
pages_of = defaultdict(list)   # motif -> [(page_id, start, end)]
for m, pid, s, e in con.execute(
        "SELECT motif, page_id, start, end FROM motif_members_pilot"):
    pages_of[m].append((pid, s, e))

t1 = {}
for pid, author, title in con.execute(
        "SELECT page_id, author, title FROM track1_matches "
        "WHERE shadowed_by IS NULL AND matched_letters >= 150"):
    t1.setdefault(pid, []).append(f"{author or ''}|{title or ''}")

print(f"motifs grown: {len(new_ms):,}; new memberships: "
      f"{sum(len(v) for v in new_ms.values()):,}")

# identified vs not, over ALL grown motifs
n_ident = 0
for m in new_ms:
    if any(pid in t1 for pid, _, _ in pages_of[m]):
        n_ident += 1
print(f"grown motifs whose pilot pages carry a live Track-1 id: "
      f"{n_ident:,} / {len(new_ms):,}")
print()

for n_new, m in growth[:TOP]:
    # rep text
    best = max(pages_of[m], key=lambda x: x[2] - x[1])
    pid, s, e = best
    tx = con.execute("SELECT text FROM pages WHERE page_id=?",
                     (pid,)).fetchone()[0]
    from normalize import norm_stream
    stream, _ = norm_stream(tx)
    rep = stream[s:e][:120]
    ids = Counter()
    for p, _, _ in pages_of[m]:
        for lab in t1.get(p, []):
            ids[lab] += 1
    label = ids.most_common(1)[0][0] if ids else '(unidentified)'
    ds = sorted(dens[m])
    med_d = ds[len(ds) // 2]
    print(f"### motif {m}: {len(members[m])} -> "
          f"{len(members[m]) + n_new} MSS (+{n_new})  "
          f"med_dens={med_d:.2f}  len={e - s}")
    print(f"  id: {label}")
    print(f"  text: {rep}")
    print()
