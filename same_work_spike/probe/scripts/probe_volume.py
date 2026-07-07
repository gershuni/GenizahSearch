# -*- coding: utf-8 -*-
"""Cheap pre-flight: raw pair-hit volume of the rehearsal corpus at DF=100.

Builds only the position table + gram group sizes (no pair emission).
Decides whether the in-RAM path suffices or disk-partitioning is needed.
"""
import sqlite3
import sys
import time

import numpy as np

import engine_np
from normalize import norm_stream

DB = sys.argv[1] if len(sys.argv) > 1 else \
    r"C:\Genizahsearch\same_work_spike\probe\data\rehearsal.db"
DF = int(sys.argv[2]) if len(sys.argv) > 2 else 100

t0 = time.time()
con = sqlite3.connect(DB)
texts = [r[0] for r in con.execute("SELECT text FROM pages ORDER BY rowid")]
con.close()
streams = [norm_stream(t)[0] for t in texts]
print(f"pages={len(streams):,} letters={sum(map(len, streams)):,} "
      f"({time.time() - t0:.0f}s)", flush=True)

parts = []
for pi, s in enumerate(streams):
    g = engine_np._gram_codes(s)
    if len(g):
        key = ((g << np.uint64(40)) | (np.uint64(pi) << np.uint64(22))
               | np.arange(len(g), dtype=np.uint64))
        parts.append(key)
keys = np.concatenate(parts)
del parts
keys.sort()
gp = keys >> np.uint64(22)
first = np.empty(len(keys), dtype=bool)
first[0] = True
np.not_equal(gp[1:], gp[:-1], out=first[1:])
G = (keys[first] >> np.uint64(40))
del keys, gp, first
starts = np.flatnonzero(np.r_[True, G[1:] != G[:-1]])
sizes = np.diff(np.r_[starts, len(G)]).astype(np.int64)
for df in (50, 100, 200, 400):
    keep = (sizes >= 2) & (sizes <= df)
    hits = int((sizes[keep] * (sizes[keep] - 1) // 2).sum())
    print(f"DF<={df}: grams={int(keep.sum()):,} raw pair-hits={hits:,} "
          f"(~{hits * 16 / 1e9:.1f} GB emission)", flush=True)
print(f"total {time.time() - t0:.0f}s")
