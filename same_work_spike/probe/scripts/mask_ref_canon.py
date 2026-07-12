# -*- coding: utf-8 -*-
"""Reference-side canonical masking — fix the מגילת המקדש class.

Reference works that heavily EMBED canonical text (Temple Scroll =
Deuteronomy paraphrase; verse-quilted midrashim) produce lexically-true
but bibliographically-false Track-1 identifications: the page matches
the work's embedded Bible, not the work. Fix at the root: find canonical
spans INSIDE each edited reference work (canonical corpora as the index,
edited works as clean 'queries', tight clean-vs-clean boundary) and mask
them when building the Track-1 reference index — a work can then only be
identified through its OWN formulations.

Usage: python mask_ref_canon.py [test:<title-substring>]
Out: data/ref_canon_masks.json (work_id -> [[m0, m1], ...], stream coords)
"""
import json
import os
import pickle
import sys
import time
from collections import defaultdict

import numpy as np
from rapidfuzz.distance import Levenshtein

from engine_np import _gram_codes
from track1_match import build_ref_index

ROOT = r"C:\Genizahsearch"
REF = ROOT + r"\same_work_spike\probe\data\ref_corpus.pkl"
OUT = ROOT + r"\same_work_spike\probe\data\ref_canon_masks.json"
REPORT = ROOT + r"\same_work_spike\probe\results\ref_canon_masks.md"

CANON_CATS = {'Bible', 'Mishnah', 'Tosefta', 'Bavli', 'Yerushalmi'}
K = 5
CHUNK, OVERLAP = 5000, 200
BAND, MIN_ANCHORS, B_OFF = 20, 2, 512
MARGIN, MIN_SPAN, GAP = 20, 25, 30
# v1 (0.15/0.20, cutoff 0.25) caught only 104 spans in 5,271 works:
# canonical text inside reference works carries orthographic + light
# paraphrase divergence (11QT's Deuteronomy tracks Deut 17 at ~0.15-0.28
# clean-vs-clean). Loosened after evidence probe 2026-07-08.
CUT = 0.45


def accept_density(length):
    return 0.28 if length < 100 else 0.32


def mask_one_work(s, seg_streams, codes_f, seg_f, pos_f, stats):
    """Find canonical spans inside one edited work's stream `s` against a
    prebuilt canonical index (build_ref_index output). Returns merged
    [[a, b], ...] intervals in stream coords, or None if no span accepted.
    Pure extraction of the mask_ref_canon per-work loop body — algorithm
    and constants unchanged."""
    step = CHUNK - OVERLAP
    chunk_offs = list(range(0, max(1, len(s) - OVERLAP), step))
    parts_c, parts_o, parts_pos = [], [], []
    for co in chunk_offs:
        seg = s[co:co + CHUNK]
        g = _gram_codes(seg)
        if not len(g):
            continue
        parts_c.append(g.astype(np.uint32))
        parts_o.append(np.full(len(g), co, np.int64))
        parts_pos.append(np.arange(len(g), dtype=np.int64))
    if not parts_c:
        return None
    pg_c = np.concatenate(parts_c)
    pg_o = np.concatenate(parts_o)
    pg_pos = np.concatenate(parts_pos)
    lo = np.searchsorted(codes_f, pg_c, 'left')
    hi = np.searchsorted(codes_f, pg_c, 'right')
    cnt = hi - lo
    sel = cnt > 0
    counts = cnt[sel]
    total = int(counts.sum())
    stats['hits'] += total
    if not total:
        return None
    cum0 = np.cumsum(counts) - counts
    ridx = (np.repeat(lo[sel], counts)
            + (np.arange(total, dtype=np.int64)
               - np.repeat(cum0, counts)))
    wpos = (pg_o[sel].repeat(counts)
            + pg_pos[sel].repeat(counts))          # work coords
    seg_h = seg_f[ridx].astype(np.uint64)
    rpos = pos_f[ridx].astype(np.int64)
    bucket = ((wpos - rpos) // BAND + B_OFF).astype(np.uint64)
    key = (seg_h << np.uint64(24)) | bucket
    order = np.argsort(key, kind='stable')
    key, wpos_s, rpos_s = key[order], wpos[order], rpos[order]
    s2 = np.flatnonzero(np.r_[True, key[1:] != key[:-1]])
    cnt2 = np.diff(np.r_[s2, len(key)])
    minw = np.minimum.reduceat(wpos_s, s2)
    maxw = np.maximum.reduceat(wpos_s, s2)
    minr = np.minimum.reduceat(rpos_s, s2)
    maxr = np.maximum.reduceat(rpos_s, s2)
    k2 = key[s2]
    segid = k2 >> np.uint64(24)
    buck = (k2 & np.uint64((1 << 24) - 1)).astype(np.int64)
    new_seg = np.r_[True, (segid[1:] != segid[:-1])
                    | (buck[1:] - buck[:-1] > 1)]
    s3 = np.flatnonzero(new_seg)
    seg_cnt = np.add.reduceat(cnt2, s3)
    hit = seg_cnt >= MIN_ANCHORS
    c_seg = segid[s3][hit]
    c_minw = np.minimum.reduceat(minw, s3)[hit]
    c_maxw = np.maximum.reduceat(maxw, s3)[hit]
    c_minr = np.minimum.reduceat(minr, s3)[hit]
    c_maxr = np.maximum.reduceat(maxr, s3)[hit]
    stats['cand'] += len(c_seg)

    def verify_split(w0, w1, r0, r1, sr, depth=0):
        """Recursive hull refinement: diagonal drift (plene
        insertions) + interleaved non-canonical passages inflate a
        long hull's density even when it contains tight sub-spans
        (the 11QT/Deut-17 case) — reject big hulls by bisecting."""
        if min(w1 - w0, r1 - r0) < MIN_SPAN:
            return
        alen = max(w1 - w0, r1 - r0)
        cutoff = int(CUT * alen) + 1
        dist = Levenshtein.distance(s[w0:w1], sr[r0:r1],
                                    score_cutoff=cutoff)
        if dist / alen <= accept_density(alen):
            spans.append((w0, w1))
            stats['accepted'] += 1
            return
        if alen <= 300 or depth >= 4:
            return
        wm = (w0 + w1) // 2
        rm = (r0 + r1) // 2
        pad_w = min(50, (w1 - w0) // 4)
        pad_r = min(50, (r1 - r0) // 4)
        verify_split(w0, wm + pad_w, r0, rm + pad_r, sr, depth + 1)
        verify_split(wm - pad_w, w1, rm - pad_r, r1, sr, depth + 1)

    spans = []
    for i in range(len(c_seg)):
        si = int(c_seg[i])
        sr = seg_streams[si]
        w0 = max(0, int(c_minw[i]) - MARGIN)
        w1 = min(len(s), int(c_maxw[i]) + K + MARGIN)
        r0 = max(0, int(c_minr[i]) - MARGIN)
        r1 = min(len(sr), int(c_maxr[i]) + K + MARGIN)
        verify_split(w0, w1, r0, r1, sr)
    if not spans:
        return None
    spans.sort()
    merged = []
    for a, b in spans:
        if merged and a <= merged[-1][1] + GAP:
            merged[-1][1] = max(merged[-1][1], b)
        else:
            merged.append([a, b])
    return merged


def mask_edited_works(edited, seg_streams, seg_work, seg_off, codes_f,
                      seg_f, pos_f, stats, progress_cb=None):
    """Mask a list of edited works against a prebuilt canonical index.
    Returns {work_id: merged_intervals}. seg_work/seg_off are accepted
    for build_ref_index signature symmetry (unused: masks live in work
    stream coords). progress_cb(wi, work, masks) fires after each work."""
    masks = {}
    for wi, w in enumerate(edited):
        merged = mask_one_work(w['stream'], seg_streams, codes_f,
                               seg_f, pos_f, stats)
        if merged:
            masks[w['id']] = merged
        if progress_cb is not None:
            progress_cb(wi, w, masks)
    return masks


def main():
    t0 = time.time()
    works = pickle.load(open(REF, 'rb'))
    canon = [w for w in works if w['cat'] in CANON_CATS]
    edited = [w for w in works if w['cat'] not in CANON_CATS]
    test_filter = None
    if len(sys.argv) > 1 and sys.argv[1].startswith('test:'):
        test_filter = sys.argv[1][5:]
        edited = [w for w in edited if test_filter in w['title']]
        print(f"TEST MODE: {len(edited)} works matching "
              f"'{test_filter}'", flush=True)
    print(f"canonical index: {len(canon)} works "
          f"({sum(len(w['stream']) for w in canon):,} letters); "
          f"edited queries: {len(edited)} works "
          f"({sum(len(w['stream']) for w in edited):,} letters)",
          flush=True)
    (seg_streams, seg_work, seg_off, codes_f, seg_f, pos_f,
     df_dropped) = build_ref_index(canon)
    print(f"index: {len(seg_streams):,} segments, {len(codes_f):,} "
          f"postings ({time.time() - t0:.0f}s)", flush=True)

    masks = defaultdict(list)
    stats = {'hits': 0, 'cand': 0, 'accepted': 0}
    # resume from checkpoint (2026-07-08 PC hard-crash lost a run in
    # progress — persist every 500 works, atomic tmp+replace)
    ck_path = OUT + '.ckpt'
    start_wi = 0
    if test_filter is None and os.path.exists(ck_path):
        ck = json.load(open(ck_path, encoding='utf-8'))
        start_wi = ck['done']
        masks.update(ck['masks'])
        stats.update(ck['stats'])
        print(f"resume: {start_wi}/{len(edited)} works done, "
              f"{len(masks):,} masked", flush=True)
    for wi, w in enumerate(edited):
        if wi < start_wi:
            continue
        merged = mask_one_work(w['stream'], seg_streams, codes_f,
                               seg_f, pos_f, stats)
        if merged:
            masks[w['id']] = merged
        if (wi + 1) % 500 == 0:
            print(f"  {wi + 1}/{len(edited)} works, masked "
                  f"{len(masks):,} ({time.time() - t0:.0f}s)", flush=True)
            if test_filter is None:
                tmp = ck_path + '.tmp'
                with open(tmp, 'w', encoding='utf-8') as f:
                    json.dump({'done': wi + 1, 'stats': stats,
                               'masks': masks}, f)
                os.replace(tmp, ck_path)

    out_path = OUT + '.test' if test_filter else OUT
    rep_path = REPORT + '.test' if test_filter else REPORT
    json.dump(masks, open(out_path, 'w', encoding='utf-8'))
    if test_filter is None and os.path.exists(ck_path):
        os.remove(ck_path)

    # report: most-masked works
    rows = []
    wlen = {w['id']: len(w['stream']) for w in edited}
    wname = {w['id']: (f"{w['author']} — {w['title']}" if w['author']
                       else w['title']) for w in edited}
    for wid, iv in masks.items():
        m = sum(b - a for a, b in iv)
        rows.append((m / max(1, wlen[wid]), m, wid))
    rows.sort(reverse=True)
    lines = [
        "# Reference-side canonical masks", "",
        f"- edited works with canonical spans: {len(masks):,} / "
        f"{len(edited):,}",
        f"- letters masked: {sum(r[1] for r in rows):,} "
        f"({stats['accepted']:,} spans; candidates {stats['cand']:,})",
        "", "## Most canonical-embedded works (masked fraction)",
    ]
    for frac, m, wid in rows[:35]:
        lines.append(f"- {100 * frac:.0f}% ({m:,} let of {wlen[wid]:,}) "
                     f"— {wname[wid][:70]}")
    open(rep_path, 'w', encoding='utf-8').write('\n'.join(lines))
    print('\n'.join(lines[:14]))
    print(f"wrote {out_path} ({time.time() - t0:.0f}s)")


if __name__ == '__main__':
    main()
