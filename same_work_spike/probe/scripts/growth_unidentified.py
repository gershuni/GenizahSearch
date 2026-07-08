# -*- coding: utf-8 -*-
"""Top UNIDENTIFIED motif-query gainers — the fragmentary-prize pool.

Grown motifs whose pilot pages carry NO live Track-1 identification:
texts that exist only in Genizah witnesses (or evade the reference
corpus) and just gained witnesses through motif-as-query.
"""
import sqlite3
from collections import defaultdict

from normalize import norm_stream

ROOT = r"C:\Genizahsearch"
DB = ROOT + r"\same_work_spike\probe\data\fullcorpus.db"
TOP = 25

con = sqlite3.connect(DB)

members = defaultdict(set)
for m, sid in con.execute("SELECT motif, sys_id FROM motif_members_pilot"):
    members[m].add(sid)
new_ms = defaultdict(set)
for m, sid in con.execute("SELECT motif, sys_id FROM motif_query_hits"):
    if sid not in members[m]:
        new_ms[m].add(sid)

pages_of = defaultdict(list)
for m, pid, s, e in con.execute(
        "SELECT motif, page_id, start, end FROM motif_members_pilot"):
    pages_of[m].append((pid, s, e))

t1_pages = {r[0] for r in con.execute(
    "SELECT DISTINCT page_id FROM track1_matches "
    "WHERE shadowed_by IS NULL AND matched_letters >= 150")}

unident = []
for m, new in new_ms.items():
    if not any(pid in t1_pages for pid, _, _ in pages_of[m]):
        unident.append((len(new), m))
unident.sort(reverse=True)
print(f"unidentified grown motifs: {len(unident):,} "
      f"(+{sum(n for n, _ in unident):,} memberships)")
buckets = defaultdict(int)
for n, _ in unident:
    buckets[1 if n < 3 else (3 if n < 10 else (10 if n < 50 else 50))] += 1
print(f"growth buckets: +1-2: {buckets[1]}, +3-9: {buckets[3]}, "
      f"+10-49: {buckets[10]}, +50+: {buckets[50]}")
print()

for n_new, m in unident[:TOP]:
    best = max(pages_of[m], key=lambda x: x[2] - x[1])
    pid, s, e = best
    tx = con.execute("SELECT text FROM pages WHERE page_id=?",
                     (pid,)).fetchone()[0]
    stream, _ = norm_stream(tx)
    old = len(members[m])
    print(f"### motif {m}: {old} -> {old + n_new} MSS (+{n_new})  "
          f"len={e - s}  rep={pid}")
    print(f"  {stream[s:e][:150]}")
    print()
