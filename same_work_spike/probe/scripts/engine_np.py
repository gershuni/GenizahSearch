# -*- coding: utf-8 -*-
"""Numpy sort-merge candidate generator — the scale representation.

Semantics identical to engine.build_diag_pairs(per_gram_pair_cap=1):
- char-k-gram inverted index over normalized letter streams
- postings deduped to FIRST position per (gram, page)  == pair_cap=1
- DF band: keep grams on 2 <= DF <= df_drop distinct pages
- per shared gram, per page pair: one hit at diagonal bucket (pos_a-pos_b)//band
- candidate = >= min_anchors hits within an adjacent-bucket chain
  (chain-merge of buckets with gap <= 1; superset of the dict engine's
  best-bucket±1 cluster — the verifier arbitrates)
- same-sys_id pairs excluded at generation time

Representation: everything is packed uint64 keys + np.sort/reduceat.
No Python dicts anywhere on the hot path.

Bit budgets (asserted): gram code < 27^5 < 2^24; page index < 2^20
(repacked 18->20 for the 667K-page full corpus); position < 2^20
(and < 655,360 for the 16-bit diagonal bucket). 24+20+20 = 64 exactly.

Two execution paths, same outputs:
- in-RAM (default): per-chunk partial reduce with min/max positions,
  one global final merge. Holds ~28 B/accumulator-entry — fine to ~1B
  entries on the 63 GB box.
- two-pass disk spill (spill_dir= set AND raw hits >= spill_min_hits):
  the full corpus emits ~4-5B raw hits; position records would need
  ~126 GB, over the box's free disk. Pass 1 spills only (pair-key u8,
  count u2) records (10 B/entry) hash-partitioned by page pair, then
  reduces each partition independently -> candidate segments
  (pair, bucket_lo, bucket_hi, count). Pass 2 re-emits the same hits
  (deterministic: same gram tables, same chunk bounds) and recovers
  min/max positions for candidate segments only via searchsorted.
"""
import os
import time

import numpy as np

K = 5
BASE = np.uint64(27)
HEB_MIN = 0x05D0

# packed (G,P,POS) sort key layout — 24+20+20 = 64 bits exactly
_POS_BITS = 20
_P_BITS = 20
# packed (pa,pb,bucket) pair key layout — 20+20+16 = 56 bits
_B_BITS = 16
_B_OFF = 1 << (_B_BITS - 1)

# pass-1 spill record: packed pair key + saturating per-chunk hit count
_SPILL_DT = np.dtype([('k', '<u8'), ('c', '<u2')])


def _gram_codes(stream: str) -> np.ndarray:
    """uint64 gram codes for all overlapping k-grams of a normalized stream."""
    a = (np.frombuffer(stream.encode('utf-16-le'), dtype=np.uint16)
         .astype(np.uint64) - np.uint64(HEB_MIN))
    n = len(a) - K + 1
    if n <= 0:
        return np.empty(0, dtype=np.uint64)
    c = np.zeros(n, dtype=np.uint64)
    for j in range(K):
        c = c * BASE + a[j:j + n]
    return c


def _reduceat_minmax(vals, starts):
    return (np.minimum.reduceat(vals, starts),
            np.maximum.reduceat(vals, starts))


def build_candidates(streams, sys_codes, df_drop=100, band=20, min_anchors=2,
                     chunk_pairs=80_000_000, log=print, masks=None,
                     spill_dir=None, spill_min_hits=800_000_000,
                     n_partitions=64):
    # spill_min_hits history: 1.5B OOM'd the 63 GB box on the maskcanon FULL
    # run (2026-07-08: canonical masking cut raw hits to ~1.48B — just under
    # the gate — and the in-RAM final merge needs ~28 B/entry). 0.8B leaves
    # the in-RAM path only for runs the box demonstrably handles (~650M).
    """Returns (pa, pb, count, min_a, max_a, min_b, max_b) arrays + stats.

    masks: optional dict page_idx -> list of (start, end) stream intervals
    (e.g. Track-1 canonical spans); grams whose k-window overlaps a masked
    interval are not indexed — 'Track 2 never sees canonical characters'.

    spill_dir: scratch directory for the two-pass disk-partitioned merge;
    engaged only when raw pair-hits >= spill_min_hits (pass spill_min_hits=0
    to force it, e.g. for parity testing). Spill files are deleted on return.
    """
    t0 = time.time()
    n_pages = len(streams)
    assert n_pages < (1 << _P_BITS)
    sys_codes = np.asarray(sys_codes, dtype=np.int32)

    # ---- position table: packed (G << 40 | P << 20 | POS), one np.sort ----
    parts = []
    max_len = 0
    n_masked_grams = 0
    for pi, s in enumerate(streams):
        g = _gram_codes(s)
        if not len(g):
            continue
        max_len = max(max_len, len(g))   # position budget: ORIGINAL indices
        pos = np.arange(len(g), dtype=np.uint64)
        if masks:
            iv = masks.get(pi)
            if iv:
                keep = np.ones(len(g), dtype=bool)
                for m0, m1 in iv:
                    keep[max(0, m0 - K + 1):m1] = False
                n_masked_grams += int(len(g) - keep.sum())
                g, pos = g[keep], pos[keep]
                if not len(g):
                    continue
        key = ((g << np.uint64(_POS_BITS + _P_BITS))
               | (np.uint64(pi) << np.uint64(_POS_BITS))
               | pos)
        parts.append(key)
    assert max_len < (1 << _POS_BITS) and max_len < 655_360
    keys = np.concatenate(parts)
    del parts
    n_positions = len(keys)
    keys.sort()
    t_index = time.time() - t0
    log(f"[engine_np] positions={n_positions:,} sorted in {t_index:.0f}s")

    # ---- dedupe to first POS per (gram, page) ----
    gp = keys >> np.uint64(_POS_BITS)
    first = np.empty(len(keys), dtype=bool)
    first[0] = True
    np.not_equal(gp[1:], gp[:-1], out=first[1:])
    keys = keys[first]
    del gp, first
    G = (keys >> np.uint64(_POS_BITS + _P_BITS)).astype(np.uint32)
    P = ((keys >> np.uint64(_POS_BITS))
         & np.uint64((1 << _P_BITS) - 1)).astype(np.uint32)
    POS = (keys & np.uint64((1 << _POS_BITS) - 1)).astype(np.uint32)
    del keys

    # ---- gram groups + DF band ----
    starts = np.flatnonzero(np.r_[True, G[1:] != G[:-1]])
    sizes = np.diff(np.r_[starts, len(G)])
    stats = {
        'n_pages': n_pages, 'n_positions': n_positions,
        'grams_masked': n_masked_grams,
        'grams_total': len(starts),
        'grams_singleton': int((sizes < 2).sum()),
        'grams_dropped_df': int((sizes > df_drop).sum()),
    }
    keep = (sizes >= 2) & (sizes <= df_drop)
    g_starts, g_sizes = starts[keep], sizes[keep]
    stats['grams_kept'] = len(g_starts)
    total_hits = int((g_sizes.astype(np.int64) * (g_sizes - 1) // 2).sum())
    stats['pair_hits_total'] = total_hits
    log(f"[engine_np] grams kept={len(g_starts):,} "
        f"(singleton={stats['grams_singleton']:,}, "
        f"df-dropped={stats['grams_dropped_df']:,}); "
        f"raw pair-hits={total_hits:,}")

    # ---- chunk plan (shared by both paths; pass 2 re-runs it verbatim) ----
    cum_hits = np.cumsum(g_sizes.astype(np.int64) * (g_sizes - 1) // 2)
    chunk_bounds = [0]
    while chunk_bounds[-1] < len(g_starts):
        nxt = int(np.searchsorted(
            cum_hits, (cum_hits[chunk_bounds[-1] - 1] if chunk_bounds[-1]
                       else 0) + chunk_pairs))
        chunk_bounds.append(min(max(nxt, chunk_bounds[-1] + 1), len(g_starts)))
    n_chunks = len(chunk_bounds) - 1

    def _emit_chunk(ci):
        """Raw sys-filtered hits of chunk ci: (pa, pb, posa, posb, n_dropped).
        Deterministic — pass 2 re-emits identical hits."""
        lo, hi = chunk_bounds[ci], chunk_bounds[ci + 1]
        st, sz = g_starts[lo:hi], g_sizes[lo:hi]
        a_parts, b_parts = [], []
        for d in np.unique(sz):
            off = st[sz == d]
            i_loc, j_loc = np.triu_indices(int(d), 1)
            a_parts.append((off[:, None] + i_loc[None, :]).ravel())
            b_parts.append((off[:, None] + j_loc[None, :]).ravel())
        A = np.concatenate(a_parts)
        B = np.concatenate(b_parts)
        del a_parts, b_parts
        pa, pb = P[A], P[B]                      # pa < pb within a gram group
        posa, posb = POS[A].astype(np.int64), POS[B].astype(np.int64)
        del A, B
        ok = sys_codes[pa] != sys_codes[pb]
        n_dropped = int(len(ok) - ok.sum())
        return pa[ok], pb[ok], posa[ok], posb[ok], n_dropped

    use_spill = spill_dir is not None and total_hits >= spill_min_hits
    if use_spill:
        return _spill_path(_emit_chunk, n_chunks, band, min_anchors,
                           spill_dir, n_partitions, stats, t0, log)

    # ================= in-RAM path =================
    t1 = time.time()
    partials = []
    same_sys_dropped = 0
    for ci in range(n_chunks):
        pa, pb, posa, posb, nd = _emit_chunk(ci)
        same_sys_dropped += nd
        bucket = ((posa - posb) // band + _B_OFF).astype(np.uint64)
        pkey = ((pa.astype(np.uint64) << np.uint64(_P_BITS + _B_BITS))
                | (pb.astype(np.uint64) << np.uint64(_B_BITS)) | bucket)
        order = np.argsort(pkey, kind='stable')
        pkey, posa, posb = pkey[order], posa[order], posb[order]
        del order, pa, pb, bucket
        s2 = np.flatnonzero(np.r_[True, pkey[1:] != pkey[:-1]])
        cnt = np.diff(np.r_[s2, len(pkey)]).astype(np.uint32)
        mina, maxa = _reduceat_minmax(posa, s2)
        minb, maxb = _reduceat_minmax(posb, s2)
        partials.append((pkey[s2], cnt,
                         mina.astype(np.uint32), maxa.astype(np.uint32),
                         minb.astype(np.uint32), maxb.astype(np.uint32)))
        log(f"[engine_np] chunk {ci + 1}/{n_chunks}: "
            f"partial keys={len(s2):,} ({time.time() - t1:.0f}s)")
    stats['same_sys_dropped'] = same_sys_dropped

    # ---- final merge of partials ----
    pkey = np.concatenate([p[0] for p in partials])
    cnt = np.concatenate([p[1] for p in partials])
    mina = np.concatenate([p[2] for p in partials])
    maxa = np.concatenate([p[3] for p in partials])
    minb = np.concatenate([p[4] for p in partials])
    maxb = np.concatenate([p[5] for p in partials])
    del partials
    order = np.argsort(pkey, kind='stable')
    pkey, cnt = pkey[order], cnt[order]
    mina, maxa, minb, maxb = mina[order], maxa[order], minb[order], maxb[order]
    del order
    s3 = np.flatnonzero(np.r_[True, pkey[1:] != pkey[:-1]])
    pkey2 = pkey[s3]
    cnt2 = np.add.reduceat(cnt.astype(np.int32), s3)
    mina2, _ = _reduceat_minmax(mina, s3)
    _, maxa2 = _reduceat_minmax(maxa, s3)
    minb2, _ = _reduceat_minmax(minb, s3)
    _, maxb2 = _reduceat_minmax(maxb, s3)
    stats['acc_entries'] = len(pkey2)
    # free the pre-merge arrays (~17 GB at 600M entries) BEFORE chain-merge —
    # keeping them alive OOM'd the 63 GB box (maskcanon run, 2026-07-07)
    del pkey, cnt, mina, maxa, minb, maxb, s3

    # ---- chain-merge adjacent buckets, two-hit ----
    pair = pkey2 >> np.uint64(_B_BITS)
    bucket = (pkey2 & np.uint64((1 << _B_BITS) - 1)).astype(np.int32)
    del pkey2
    new_seg = np.r_[True, (pair[1:] != pair[:-1])
                    | (bucket[1:] - bucket[:-1] > 1)]
    s4 = np.flatnonzero(new_seg)
    del bucket
    seg_cnt = np.add.reduceat(cnt2, s4)
    seg_pair = pair[s4]
    seg_mina, _ = _reduceat_minmax(mina2, s4)
    _, seg_maxa = _reduceat_minmax(maxa2, s4)
    seg_minb, _ = _reduceat_minmax(minb2, s4)
    _, seg_maxb = _reduceat_minmax(maxb2, s4)
    hit = seg_cnt >= min_anchors
    pa = (seg_pair[hit] >> np.uint64(_P_BITS)).astype(np.uint32)
    pb = (seg_pair[hit] & np.uint64((1 << _P_BITS) - 1)).astype(np.uint32)
    stats['candidate_pairs'] = len(pa)
    stats['t_total_s'] = round(time.time() - t0, 1)
    log(f"[engine_np] acc entries={stats['acc_entries']:,} -> "
        f"candidates={len(pa):,} in {stats['t_total_s']}s")
    return (pa, pb, seg_cnt[hit].astype(np.uint32),
            seg_mina[hit], seg_maxa[hit], seg_minb[hit], seg_maxb[hit],
            stats)


def _spill_path(_emit_chunk, n_chunks, band, min_anchors,
                spill_dir, n_partitions, stats, t0, log):
    """Two-pass disk-partitioned merge (see module docstring)."""
    os.makedirs(spill_dir, exist_ok=True)
    paths = [os.path.join(spill_dir, f"part{p:03d}.bin")
             for p in range(n_partitions)]

    # ---- pass 1: per-chunk (pair-key, count) reduce -> partitioned spill ----
    t1 = time.time()
    fhs = [open(p, 'wb') for p in paths]
    spilled = 0
    same_sys_dropped = 0
    n_saturated = 0
    try:
        for ci in range(n_chunks):
            pa, pb, posa, posb, nd = _emit_chunk(ci)
            same_sys_dropped += nd
            bucket = ((posa - posb) // band + _B_OFF).astype(np.uint64)
            pkey = ((pa.astype(np.uint64) << np.uint64(_P_BITS + _B_BITS))
                    | (pb.astype(np.uint64) << np.uint64(_B_BITS)) | bucket)
            del pa, pb, posa, posb, bucket
            pkey.sort()
            s2 = np.flatnonzero(np.r_[True, pkey[1:] != pkey[:-1]])
            cnt = np.diff(np.r_[s2, len(pkey)])
            k2 = pkey[s2]
            del pkey, s2
            n_saturated += int((cnt > 65535).sum())
            rec = np.empty(len(k2), dtype=_SPILL_DT)
            rec['k'] = k2
            rec['c'] = np.minimum(cnt, 65535).astype(np.uint16)
            part = ((k2 >> np.uint64(_B_BITS))
                    % np.uint64(n_partitions)).astype(np.uint16)
            del k2, cnt
            order = np.argsort(part, kind='stable')
            rec, part = rec[order], part[order]
            del order
            bounds = np.r_[np.flatnonzero(np.r_[True, part[1:] != part[:-1]]),
                           len(part)]
            for bi in range(len(bounds) - 1):
                fhs[int(part[bounds[bi]])].write(
                    rec[bounds[bi]:bounds[bi + 1]].tobytes())
            spilled += len(rec)
            del rec, part
            log(f"[engine_np] pass1 chunk {ci + 1}/{n_chunks}: "
                f"spilled={spilled:,} entries "
                f"({spilled * _SPILL_DT.itemsize / 2**30:.1f} GB, "
                f"{time.time() - t1:.0f}s)")
    finally:
        for fh in fhs:
            fh.close()
    stats['same_sys_dropped'] = same_sys_dropped
    stats['acc_entries'] = spilled
    stats['cnt_saturated'] = n_saturated

    # ---- per-partition merge -> candidate segments ----
    t2 = time.time()
    seg_pairs, seg_blos, seg_bhis, seg_cnts = [], [], [], []
    for p in range(n_partitions):
        rec = np.fromfile(paths[p], dtype=_SPILL_DT)
        if not len(rec):
            continue
        k = rec['k'].copy()
        c = rec['c'].astype(np.int32)
        del rec
        order = np.argsort(k, kind='stable')
        k, c = k[order], c[order]
        del order
        s = np.flatnonzero(np.r_[True, k[1:] != k[:-1]])
        k2 = k[s]
        c2 = np.add.reduceat(c, s)
        del k, c
        pair = k2 >> np.uint64(_B_BITS)
        bucket = (k2 & np.uint64((1 << _B_BITS) - 1)).astype(np.int32)
        del k2
        new_seg = np.r_[True, (pair[1:] != pair[:-1])
                        | (bucket[1:] - bucket[:-1] > 1)]
        s4 = np.flatnonzero(new_seg)
        seg_cnt = np.add.reduceat(c2, s4)
        hit = seg_cnt >= min_anchors
        ends = np.r_[s4[1:], len(bucket)] - 1
        seg_pairs.append(pair[s4][hit])
        seg_blos.append(bucket[s4][hit])
        seg_bhis.append(bucket[ends][hit])
        seg_cnts.append(seg_cnt[hit].astype(np.uint32))
        del pair, bucket, c2, s4, seg_cnt, hit, ends
    for p in paths:
        os.remove(p)
    seg_pair = np.concatenate(seg_pairs)
    seg_blo = np.concatenate(seg_blos)
    seg_bhi = np.concatenate(seg_bhis)
    seg_cnt = np.concatenate(seg_cnts)
    del seg_pairs, seg_blos, seg_bhis, seg_cnts
    # global order by (pair, bucket_lo) for pass-2 searchsorted mapping;
    # partitions are pair-disjoint so this equals a global pair-key sort
    skey = (seg_pair << np.uint64(_B_BITS)) | seg_blo.astype(np.uint64)
    order = np.argsort(skey, kind='stable')
    skey = skey[order]
    seg_pair, seg_blo = seg_pair[order], seg_blo[order]
    seg_bhi, seg_cnt = seg_bhi[order], seg_cnt[order]
    del order
    n_seg = len(seg_pair)
    stats['candidate_pairs'] = n_seg
    log(f"[engine_np] partition merge: {stats['acc_entries']:,} entries -> "
        f"candidates={n_seg:,} ({time.time() - t2:.0f}s)")

    # ---- pass 2: re-emit hits, recover min/max positions per segment ----
    t3 = time.time()
    run_mina = np.full(n_seg, 0xFFFFFFFF, np.uint32)
    run_maxa = np.zeros(n_seg, np.uint32)
    run_minb = np.full(n_seg, 0xFFFFFFFF, np.uint32)
    run_maxb = np.zeros(n_seg, np.uint32)
    for ci in range(n_chunks):
        pa, pb, posa, posb, _ = _emit_chunk(ci)
        pairh = (pa.astype(np.uint64) << np.uint64(_P_BITS)) | pb
        del pa, pb
        bucket = ((posa - posb) // band + _B_OFF).astype(np.uint64)
        hkey = (pairh << np.uint64(_B_BITS)) | bucket
        idx = np.searchsorted(skey, hkey, side='right') - 1
        del hkey
        idxc = np.maximum(idx, 0)
        ok = ((idx >= 0) & (seg_pair[idxc] == pairh)
              & (bucket.astype(np.int64) <= seg_bhi[idxc]))
        del idx, pairh, bucket
        si = idxc[ok]
        A = posa[ok].astype(np.uint32)
        Bv = posb[ok].astype(np.uint32)
        del idxc, ok, posa, posb
        if not len(si):
            continue
        order = np.argsort(si, kind='stable')
        si, A, Bv = si[order], A[order], Bv[order]
        del order
        st = np.flatnonzero(np.r_[True, si[1:] != si[:-1]])
        u = si[st]
        run_mina[u] = np.minimum(run_mina[u], np.minimum.reduceat(A, st))
        run_maxa[u] = np.maximum(run_maxa[u], np.maximum.reduceat(A, st))
        run_minb[u] = np.minimum(run_minb[u], np.minimum.reduceat(Bv, st))
        run_maxb[u] = np.maximum(run_maxb[u], np.maximum.reduceat(Bv, st))
        del si, A, Bv, st, u
        log(f"[engine_np] pass2 chunk {ci + 1}/{n_chunks} "
            f"({time.time() - t3:.0f}s)")

    pa = (seg_pair >> np.uint64(_P_BITS)).astype(np.uint32)
    pb = (seg_pair & np.uint64((1 << _P_BITS) - 1)).astype(np.uint32)
    stats['t_total_s'] = round(time.time() - t0, 1)
    log(f"[engine_np] spill path done: candidates={n_seg:,} "
        f"in {stats['t_total_s']}s")
    return (pa, pb, seg_cnt, run_mina, run_maxa, run_minb, run_maxb, stats)
