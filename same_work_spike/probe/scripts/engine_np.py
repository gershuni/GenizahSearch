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

Bit budgets (asserted): gram code < 27^5 < 2^24; page index < 2^18;
position < 2^22 (and < 655,360 for the 16-bit diagonal bucket).
"""
import time

import numpy as np

K = 5
BASE = np.uint64(27)
HEB_MIN = 0x05D0

# packed (G,P,POS) sort key layout
_POS_BITS = 22
_P_BITS = 18
# packed (pa,pb,bucket) pair key layout
_B_BITS = 16
_B_OFF = 1 << (_B_BITS - 1)


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
                     chunk_pairs=80_000_000, log=print):
    """Returns (pa, pb, count, min_a, max_a, min_b, max_b) arrays + stats."""
    t0 = time.time()
    n_pages = len(streams)
    assert n_pages < (1 << _P_BITS)
    sys_codes = np.asarray(sys_codes, dtype=np.int32)

    # ---- position table: packed (G << 40 | P << 22 | POS), one np.sort ----
    parts = []
    max_len = 0
    for pi, s in enumerate(streams):
        g = _gram_codes(s)
        if not len(g):
            continue
        max_len = max(max_len, len(g))
        key = ((g << np.uint64(_POS_BITS + _P_BITS))
               | (np.uint64(pi) << np.uint64(_POS_BITS))
               | np.arange(len(g), dtype=np.uint64))
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

    # ---- chunked pair emission + partial reduce ----
    t1 = time.time()
    cum_hits = np.cumsum(g_sizes.astype(np.int64) * (g_sizes - 1) // 2)
    chunk_bounds = [0]
    while chunk_bounds[-1] < len(g_starts):
        nxt = int(np.searchsorted(
            cum_hits, (cum_hits[chunk_bounds[-1] - 1] if chunk_bounds[-1]
                       else 0) + chunk_pairs))
        chunk_bounds.append(min(max(nxt, chunk_bounds[-1] + 1), len(g_starts)))
    partials = []
    same_sys_dropped = 0
    for ci in range(len(chunk_bounds) - 1):
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
        same_sys_dropped += int(len(ok) - ok.sum())
        pa, pb, posa, posb = pa[ok], pb[ok], posa[ok], posb[ok]
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
        log(f"[engine_np] chunk {ci + 1}/{len(chunk_bounds) - 1}: "
            f"grams={hi - lo:,} -> partial keys={len(s2):,} "
            f"({time.time() - t1:.0f}s)")
    del G, P, POS
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
    cnt2 = np.add.reduceat(cnt.astype(np.int64), s3)
    mina2, _ = _reduceat_minmax(mina, s3)
    _, maxa2 = _reduceat_minmax(maxa, s3)
    minb2, _ = _reduceat_minmax(minb, s3)
    _, maxb2 = _reduceat_minmax(maxb, s3)
    stats['acc_entries'] = len(pkey2)

    # ---- chain-merge adjacent buckets, two-hit ----
    pair = pkey2 >> np.uint64(_B_BITS)
    bucket = (pkey2 & np.uint64((1 << _B_BITS) - 1)).astype(np.int64)
    new_seg = np.r_[True, (pair[1:] != pair[:-1])
                    | (bucket[1:] - bucket[:-1] > 1)]
    s4 = np.flatnonzero(new_seg)
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
