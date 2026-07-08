# -*- coding: utf-8 -*-
"""Motif-as-query completion — the fragmentary-prize engine.

Every motif from the continuum decomposition becomes a retrieval QUERY
against the full corpus, using the Track-1 asymmetric machinery with
the motif's representative text as the 'reference'. Per-query DF
economics are immune to the global DF<=100 cap — this is how a
2-witness fragment of an unknown short text becomes a full witness
census. Unlike Track-1 (clean reference), the query text is itself
noisy HTR, so acceptance uses the TWO-SIDED boundary.

Usage: python motif_query.py [db] [min_ms] [min_len]
Out: <db>::motif_query_hits, results/motif_query_growth.md
"""
import json
import re
import sqlite3
import sys
import time
from collections import defaultdict

import numpy as np
from rapidfuzz.distance import Levenshtein

from engine_np import _gram_codes
from normalize import norm_stream
from track1_match import build_ref_index

ROOT = r"C:\Genizahsearch"
DB = sys.argv[1] if len(sys.argv) > 1 else \
    ROOT + r"\same_work_spike\probe\data\fullcorpus.db"
MIN_MS = int(sys.argv[2]) if len(sys.argv) > 2 else 3
MIN_LEN = int(sys.argv[3]) if len(sys.argv) > 3 else 100
OUT = ROOT + r"\same_work_spike\probe\results\motif_query_growth.md"

K = 5
BAND, MIN_ANCHORS, B_OFF = 20, 2, 256
MARGIN, MIN_SPAN, PAGE_BATCH = 30, 40, 8000
MAX_MOTIFS = 55000          # ref-index 16-bit segment budget guard


def accept_density(length):
    # two-sided HTR noise (METHOD.md liturgy_q95 boundary)
    return 0.30 if length < 100 else (0.386 if length < 200 else 0.418)


P_RE = re.compile(r'_P(\d+)_')


def main():
    t0 = time.time()
    con = sqlite3.connect(DB)

    # ---- motif representatives ----
    motif_meta = {r[0]: (r[1], r[2]) for r in con.execute(
        "SELECT motif, n_ms, med_len FROM motifs_pilot "
        f"WHERE n_ms >= {MIN_MS} AND med_len >= {MIN_LEN} "
        f"ORDER BY n_ms DESC LIMIT {MAX_MOTIFS}")}
    reps = {}      # motif -> (page_id, start, end)
    members = defaultdict(set)
    for m, pid, sid, s, e in con.execute(
            "SELECT motif, page_id, sys_id, start, end "
            "FROM motif_members_pilot"):
        if m not in motif_meta:
            continue
        members[m].add(sid)
        cur = reps.get(m)
        if cur is None or e - s > cur[2] - cur[1]:
            reps[m] = (pid, s, e)
    print(f"query motifs: {len(reps):,} (>= {MIN_MS} MSS, med >= "
          f"{MIN_LEN})", flush=True)

    works = []
    need_pages = {r[0] for r in reps.values()}
    texts = {}
    ids = list(need_pages)
    for i in range(0, len(ids), 500):
        batch = ids[i:i + 500]
        ph = ','.join('?' * len(batch))
        for pid, tx in con.execute(
                f"SELECT page_id, text FROM pages WHERE page_id IN ({ph})",
                batch):
            texts[pid] = norm_stream(tx)[0]
    for m, (pid, s, e) in reps.items():
        stream = texts.get(pid, '')[s:e]
        if len(stream) >= MIN_LEN:
            works.append({'id': m, 'stream': stream})
    del texts
    print(f"query streams: {len(works):,} "
          f"({sum(len(w['stream']) for w in works):,} letters, "
          f"{time.time() - t0:.0f}s)", flush=True)

    (seg_streams, seg_work, seg_off, codes_f, seg_f, pos_f,
     df_dropped) = build_ref_index(works)
    print(f"index: {len(seg_streams):,} segments, {len(codes_f):,} "
          f"postings ({time.time() - t0:.0f}s)", flush=True)

    rows = con.execute(
        "SELECT page_id, sys_id, text FROM pages ORDER BY rowid").fetchall()
    ids = [r[0] for r in rows]
    sys_ids = [r[1] for r in rows]
    streams = [norm_stream(r[2])[0] for r in rows]
    del rows
    print(f"pages: {len(ids):,} ({time.time() - t0:.0f}s)", flush=True)

    hits = defaultdict(list)   # (page_idx, motif) accepted spans
    # resume from checkpoint (2026-07-08 PC hard-crash at batch 65/84
    # lost 2h of RAM-only hits — persist progress every 8 batches)
    start_b0 = 0
    if con.execute("SELECT name FROM sqlite_master WHERE "
                   "name='motif_query_ckpt'").fetchone():
        row = con.execute(
            "SELECT next_b0, hits_json FROM motif_query_ckpt").fetchone()
        if row:
            pi_of = {p: i for i, p in enumerate(ids)}
            wi_of = {w['id']: i for i, w in enumerate(works)}
            for pid, m, spans in json.loads(row[1]):
                if pid in pi_of and m in wi_of:
                    hits[(pi_of[pid], wi_of[m])] = \
                        [tuple(s) for s in spans]
            start_b0 = row[0]
            print(f"resume: batch {start_b0 // PAGE_BATCH + 1}, "
                  f"{len(hits):,} hits restored", flush=True)
    t1 = time.time()
    n_cand = 0
    for b0 in range(start_b0, len(ids), PAGE_BATCH):
        bpages = range(b0, min(b0 + PAGE_BATCH, len(ids)))
        parts_c, parts_p, parts_pos = [], [], []
        for pi in bpages:
            g = _gram_codes(streams[pi])
            if not len(g):
                continue
            parts_c.append(g.astype(np.uint32))
            parts_p.append(np.full(len(g), pi, np.uint32))
            parts_pos.append(np.arange(len(g), dtype=np.uint32))
        if not parts_c:
            continue
        pg_c = np.concatenate(parts_c)
        pg_p = np.concatenate(parts_p)
        pg_pos = np.concatenate(parts_pos)
        del parts_c, parts_p, parts_pos
        lo = np.searchsorted(codes_f, pg_c, 'left')
        hi = np.searchsorted(codes_f, pg_c, 'right')
        cnt = hi - lo
        sel = cnt > 0
        counts = cnt[sel]
        total = int(counts.sum())
        if not total:
            continue
        cum0 = np.cumsum(counts) - counts
        ridx = (np.repeat(lo[sel], counts)
                + (np.arange(total, dtype=np.int64)
                   - np.repeat(cum0, counts)))
        page_r = np.repeat(pg_p[sel], counts).astype(np.uint64)
        ppos_r = np.repeat(pg_pos[sel], counts).astype(np.int64)
        seg_h = seg_f[ridx].astype(np.uint64)
        rpos_h = pos_f[ridx].astype(np.int64)
        del ridx
        bucket = ((ppos_r - rpos_h) // BAND + B_OFF).astype(np.uint64)
        key = (page_r << np.uint64(34)) | (seg_h << np.uint64(18)) | bucket
        order = np.argsort(key, kind='stable')
        key = key[order]
        ppos_r, rpos_h = ppos_r[order], rpos_h[order]
        del order, page_r, seg_h, bucket
        s2 = np.flatnonzero(np.r_[True, key[1:] != key[:-1]])
        cnt2 = np.diff(np.r_[s2, len(key)])
        minp = np.minimum.reduceat(ppos_r, s2)
        maxp = np.maximum.reduceat(ppos_r, s2)
        minr = np.minimum.reduceat(rpos_h, s2)
        maxr = np.maximum.reduceat(rpos_h, s2)
        k2 = key[s2]
        pair = k2 >> np.uint64(18)
        buck = (k2 & np.uint64((1 << 18) - 1)).astype(np.int64)
        new_seg = np.r_[True, (pair[1:] != pair[:-1])
                        | (buck[1:] - buck[:-1] > 1)]
        s3 = np.flatnonzero(new_seg)
        seg_cnt = np.add.reduceat(cnt2, s3)
        hit = seg_cnt >= MIN_ANCHORS
        c_pair = pair[s3][hit]
        c_minp = np.minimum.reduceat(minp, s3)[hit]
        c_maxp = np.maximum.reduceat(maxp, s3)[hit]
        c_minr = np.minimum.reduceat(minr, s3)[hit]
        c_maxr = np.maximum.reduceat(maxr, s3)[hit]
        n_cand += len(c_pair)
        for i in range(len(c_pair)):
            pi = int(c_pair[i] >> np.uint64(16))
            si = int(c_pair[i] & np.uint64(0xFFFF))
            sp, sr = streams[pi], seg_streams[si]
            p0 = max(0, int(c_minp[i]) - MARGIN)
            p1 = min(len(sp), int(c_maxp[i]) + K + MARGIN)
            r0 = max(0, int(c_minr[i]) - MARGIN)
            r1 = min(len(sr), int(c_maxr[i]) + K + MARGIN)
            if min(p1 - p0, r1 - r0) < MIN_SPAN:
                continue
            alen = max(p1 - p0, r1 - r0)
            cutoff = int(0.45 * alen) + 1
            dist = Levenshtein.distance(sp[p0:p1], sr[r0:r1],
                                        score_cutoff=cutoff)
            dens = dist / alen
            if dens > accept_density(alen):
                continue
            hits[(pi, int(seg_work[si]))].append(
                (p0, p1, round(dens, 4)))
        if (b0 // PAGE_BATCH) % 8 == 0:
            print(f"  batch {b0 // PAGE_BATCH + 1}/"
                  f"{(len(ids) - 1) // PAGE_BATCH + 1}: cand={n_cand:,} "
                  f"hits={len(hits):,} ({time.time() - t1:.0f}s)",
                  flush=True)
            con.execute("CREATE TABLE IF NOT EXISTS motif_query_ckpt "
                        "(next_b0 INT, hits_json TEXT)")
            con.execute("DELETE FROM motif_query_ckpt")
            con.execute(
                "INSERT INTO motif_query_ckpt VALUES (?,?)",
                (b0 + PAGE_BATCH, json.dumps(
                    [[ids[pi], works[wi]['id'], sp]
                     for (pi, wi), sp in hits.items()])))
            con.commit()

    # ---- persist + growth report ----
    out_rows = []
    motif_new = defaultdict(set)
    for (pi, wi), spans in hits.items():
        motif = works[wi]['id']
        spans.sort()
        letters = sum(s[1] - s[0] for s in spans)
        out_rows.append((motif, ids[pi], sys_ids[pi], letters,
                         min(s[2] for s in spans),
                         json.dumps([[s[0], s[1], s[2]] for s in spans])))
        if sys_ids[pi] not in members[motif]:
            motif_new[motif].add(sys_ids[pi])
    con.execute("DROP TABLE IF EXISTS motif_query_hits")
    con.execute("""CREATE TABLE motif_query_hits (
        motif INT, page_id TEXT, sys_id TEXT, matched_letters INT,
        best_density REAL, spans_json TEXT)""")
    con.executemany("INSERT INTO motif_query_hits VALUES (?,?,?,?,?,?)",
                    out_rows)
    con.execute("CREATE INDEX idx_mqh ON motif_query_hits(motif)")
    con.execute("DROP TABLE IF EXISTS motif_query_ckpt")
    con.commit()

    growth = sorted(((len(new), m) for m, new in motif_new.items()),
                    reverse=True)
    tot_new = sum(len(v) for v in motif_new.values())
    lines = [
        "# Motif-query completion — growth report", "",
        f"- query motifs: {len(works):,}; hit rows: {len(out_rows):,}",
        f"- motifs that GREW: {len(motif_new):,}; new (motif, MS) "
        f"memberships: {tot_new:,}", "",
        "## Top growth (new witness MSS beyond the pilot membership)",
    ]
    for n_new, m in growth[:40]:
        old = len(members[m])
        lines.append(f"- motif {m}: {old} -> {old + n_new} MSS "
                     f"(+{n_new})")
    open(OUT, 'w', encoding='utf-8').write('\n'.join(lines))
    print('\n'.join(lines[:12]))
    print(f"wrote {OUT} ({time.time() - t0:.0f}s total)")
    con.close()


if __name__ == '__main__':
    main()
