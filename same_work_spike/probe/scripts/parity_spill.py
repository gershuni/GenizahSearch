# -*- coding: utf-8 -*-
"""Deep parity: in-RAM vs two-pass-spill paths of engine_np.build_candidates.

Runs on the 100K rehearsal corpus (unmasked), where the recorded in-RAM
run produced 40,549,024 candidate segments / 38,232,433 unique pairs.
Each path runs in its own process (peak-RSS isolation); outputs are
canonical-sorted and saved to npz, then byte-compared.

Usage:
  python parity_spill.py ram      -> data/parity_ram.npz
  python parity_spill.py spill    -> data/parity_spill.npz  (spill forced)
  python parity_spill.py compare  -> verdict
"""
import sqlite3
import sys
import time

import numpy as np

import engine_np
from normalize import norm_stream

ROOT = r"C:\Genizahsearch"
DB = ROOT + r"\same_work_spike\probe\data\rehearsal.db"
DATA = ROOT + r"\same_work_spike\probe\data"
NAMES = ('pa', 'pb', 'cnt', 'mina', 'maxa', 'minb', 'maxb')


def canon(res):
    pa, pb, cnt, mina, maxa, minb, maxb, stats = res
    key = (pa.astype(np.uint64) << np.uint64(engine_np._P_BITS)) | pb
    order = np.lexsort((maxb, minb, maxa, mina, key))
    return {n: a[order] for n, a in
            zip(NAMES, (pa, pb, cnt, mina, maxa, minb, maxb))}, stats


def run(mode):
    con = sqlite3.connect(DB)
    rows = con.execute(
        "SELECT sys_id, text FROM pages ORDER BY rowid").fetchall()
    con.close()
    sys_ids = [r[0] for r in rows]
    streams = [norm_stream(r[1])[0] for r in rows]
    del rows
    sys_uniq = {s: i for i, s in enumerate(dict.fromkeys(sys_ids))}
    sys_codes = np.array([sys_uniq[s] for s in sys_ids], dtype=np.int32)
    t0 = time.time()
    if mode == 'ram':
        res = engine_np.build_candidates(streams, sys_codes)
    else:
        res = engine_np.build_candidates(
            streams, sys_codes,
            spill_dir=DATA + r"\spill", spill_min_hits=0)
    arrs, stats = canon(res)
    np.savez(DATA + rf"\parity_{mode}.npz", **arrs)
    print(f"[{mode}] candidates={stats['candidate_pairs']:,} "
          f"acc_entries={stats['acc_entries']:,} "
          f"saturated={stats.get('cnt_saturated', 0)} "
          f"({time.time() - t0:.0f}s)")


def compare():
    a = np.load(DATA + r"\parity_ram.npz")
    b = np.load(DATA + r"\parity_spill.npz")
    n_a, n_b = len(a['pa']), len(b['pa'])
    print(f"rows: ram={n_a:,} spill={n_b:,}")
    assert n_a == n_b, "candidate counts differ"
    for n in NAMES:
        eq = (a[n] == b[n]).all()
        print(f"  {n}: {'OK' if eq else 'MISMATCH'}")
        assert eq, n
    upairs = len(np.unique(
        (a['pa'].astype(np.uint64) << np.uint64(engine_np._P_BITS))
        | a['pb']))
    print(f"PARITY OK — {n_a:,} segments, {upairs:,} unique pairs")


if __name__ == '__main__':
    m = sys.argv[1]
    compare() if m == 'compare' else run(m)
