# -*- coding: utf-8 -*-
"""Recompute flank_dist on accepted_pairs with EQUAL-LENGTH flanks.

The run-time version compared flanks of unequal length (60..150 letters vs
150), and normalized_distance = dist/max(len) then floors at the length
ratio — a 60-vs-150 comparison scores >=0.60 regardless of content,
inflating the island class. Fix: clip both flanks to the same L =
min(150, avail_a, avail_b), require L >= 60.
"""
import sqlite3
import sys
import time

from rapidfuzz.distance import Levenshtein

from normalize import norm_stream

DB = sys.argv[1] if len(sys.argv) > 1 else \
    r"C:\Genizahsearch\same_work_spike\probe\data\rehearsal.db"
FLANK = 150

t0 = time.time()
con = sqlite3.connect(DB)
streams = {pid: norm_stream(tx)[0] for pid, tx in
           con.execute("SELECT page_id, text FROM pages")}
print(f"streams: {len(streams):,} ({time.time() - t0:.0f}s)", flush=True)

rows = con.execute(
    "SELECT rowid, page_a, page_b, a0, a1, b0, b1 FROM accepted_pairs"
).fetchall()
updates = []
for rid, pa, pb, a0, a1, b0, b1 in rows:
    sa, sb = streams[pa], streams[pb]
    best = None
    # left flank: L letters immediately before the span, both sides
    L = min(FLANK, a0, b0)
    if L >= 60:
        d = Levenshtein.normalized_distance(sa[a0 - L:a0], sb[b0 - L:b0])
        best = d if best is None else min(best, d)
    # right flank
    L = min(FLANK, len(sa) - a1, len(sb) - b1)
    if L >= 60:
        d = Levenshtein.normalized_distance(sa[a1:a1 + L], sb[b1:b1 + L])
        best = d if best is None else min(best, d)
    fd = -1.0 if best is None else round(best, 4)
    fclass = ('edge' if fd < 0 else
              'continuation' if fd <= 0.52 else
              'ambig' if fd <= 0.58 else 'island')
    updates.append((fd, fclass, rid))
con.executemany(
    "UPDATE accepted_pairs SET flank_dist=?, flank_class=? WHERE rowid=?",
    updates)
con.commit()
con.close()
print(f"updated {len(updates):,} rows in {time.time() - t0:.0f}s")
