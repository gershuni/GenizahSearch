# -*- coding: utf-8 -*-
"""100K scale rehearsal: rehearsal.db -> candidates -> verified pairs -> map data.

Usage: python rehearsal_run.py [db_path] [tag] [mask]
- 3rd arg 'mask': load Track-1 spans from <db>::track1_matches and mask them
  out of the gram index (Track 2 never sees identified known-work text);
  results go to accepted_pairs_masked instead of accepted_pairs.
Writes: <db>::accepted_pairs[_masked] table, results/rehearsal_<tag>_stats.json
"""
import json
import sqlite3
import sys
import time

import numpy as np
from rapidfuzz.distance import Levenshtein

import engine_np
from normalize import norm_stream
from stage0 import line_agreement, load_shelf_variants, same_shelf

ROOT = r"C:\Genizahsearch"
DB = sys.argv[1] if len(sys.argv) > 1 else \
    ROOT + r"\same_work_spike\probe\data\rehearsal.db"
TAG = sys.argv[2] if len(sys.argv) > 2 else "100k"
USE_MASKS = len(sys.argv) > 3 and sys.argv[3] == 'mask'
PAIRS_TABLE = 'accepted_pairs_masked' if USE_MASKS else 'accepted_pairs'
STATS_OUT = ROOT + rf"\same_work_spike\probe\results\rehearsal_{TAG}_stats.json"

K, BAND, DF_DROP, MIN_ANCHORS = 5, 20, 100, 2
MARGIN, MIN_SPAN, SPAN_CAP = 30, 25, 6000
FLANK = 150

# sloped acceptance boundary (METHOD.md §6.2, liturgy_q95)
def accept_density(length):
    return 0.30 if length < 100 else (0.386 if length < 200 else 0.418)


def flank_dist(sa, sb, a0, a1, b0, b1):
    """Best (lowest) EQUAL-LENGTH flank normalized distance; None if none.

    Equal-length clipping is load-bearing: normalized_distance floors at
    the length ratio, so unequal flanks inflate the island class
    (fix_flanks.py post-mortem)."""
    best = None
    L = min(FLANK, a0, b0)
    if L >= 60:
        d = Levenshtein.normalized_distance(sa[a0 - L:a0], sb[b0 - L:b0])
        best = d
    L = min(FLANK, len(sa) - a1, len(sb) - b1)
    if L >= 60:
        d = Levenshtein.normalized_distance(sa[a1:a1 + L], sb[b1:b1 + L])
        best = d if best is None else min(best, d)
    return best


def main():
    t0 = time.time()
    con = sqlite3.connect(DB)
    rows = con.execute(
        "SELECT page_id, sys_id, buckets, text FROM pages ORDER BY rowid"
    ).fetchall()
    ids = [r[0] for r in rows]
    sys_ids = [r[1] for r in rows]
    buckets = [r[2] for r in rows]
    texts = [r[3] for r in rows]
    print(f"pages: {len(rows)}")

    streams = [norm_stream(t)[0] for t in texts]
    sys_uniq = {s: i for i, s in enumerate(dict.fromkeys(sys_ids))}
    sys_codes = np.array([sys_uniq[s] for s in sys_ids], dtype=np.int32)
    print(f"streams normalized in {time.time() - t0:.0f}s; "
          f"letters={sum(map(len, streams)):,}")

    masks = None
    if USE_MASKS:
        id_to_idx = {p: i for i, p in enumerate(ids)}
        masks = {}
        for pid, spans_json in con.execute(
                "SELECT page_id, spans_json FROM track1_matches"):
            pi = id_to_idx.get(pid)
            if pi is None:
                continue
            iv = masks.setdefault(pi, [])
            iv.extend((int(s[0]), int(s[1]))
                      for s in json.loads(spans_json))
        n_iv = sum(len(v) for v in masks.values())
        print(f"masks: {len(masks):,} pages, {n_iv:,} Track-1 intervals")

    pa, pb, cnt, mina, maxa, minb, maxb, stats = engine_np.build_candidates(
        streams, sys_codes, df_drop=DF_DROP, band=BAND,
        min_anchors=MIN_ANCHORS, masks=masks)
    stats['candidate_unique_pairs'] = int(len(
        np.unique((pa.astype(np.uint64) << np.uint64(18)) | pb)))

    # ---- verification (sloped boundary; best segment per pair) ----
    t1 = time.time()
    best = {}   # (pa,pb) -> dict
    n_rej_short = n_rej_dense = n_clipped = 0
    for i in range(len(pa)):
        sa, sb = streams[pa[i]], streams[pb[i]]
        a0 = max(0, int(mina[i]) - MARGIN)
        a1 = min(len(sa), int(maxa[i]) + K + MARGIN)
        b0 = max(0, int(minb[i]) - MARGIN)
        b1 = min(len(sb), int(maxb[i]) + K + MARGIN)
        if a1 - a0 > SPAN_CAP or b1 - b0 > SPAN_CAP:
            n_clipped += 1
            a1, b1 = min(a1, a0 + SPAN_CAP), min(b1, b0 + SPAN_CAP)
        la, lb = a1 - a0, b1 - b0
        if min(la, lb) < MIN_SPAN:
            n_rej_short += 1
            continue
        alen = max(la, lb)
        cutoff = int(0.45 * alen) + 1
        dist = Levenshtein.distance(sa[a0:a1], sb[b0:b1],
                                    score_cutoff=cutoff)
        density = dist / alen
        if density > accept_density(alen):
            n_rej_dense += 1
            continue
        key = (int(pa[i]), int(pb[i]))
        rec = {'a0': a0, 'a1': a1, 'b0': b0, 'b1': b1,
               'n_anchors': int(cnt[i]), 'aligned_len': alen,
               'density': round(density, 4)}
        old = best.get(key)
        if old is None or (rec['aligned_len'], -rec['density']) > \
                (old['aligned_len'], -old['density']):
            best[key] = rec
        if i and i % 20_000_000 == 0:
            print(f"  verify {i:,}/{len(pa):,} accepted={len(best):,} "
                  f"({time.time() - t1:.0f}s)")
    stats.update(
        verify_s=round(time.time() - t1, 1), rej_short=n_rej_short,
        rej_density=n_rej_dense, span_clipped=n_clipped,
        accepted_pairs=len(best))
    print(f"verified: accepted={len(best):,} in {stats['verify_s']}s "
          f"(rej short={n_rej_short:,} dense={n_rej_dense:,})")

    # ---- post-verify: dup detectors + flank contrast ----
    t2 = time.time()
    shelf_variants = load_shelf_variants()
    out_rows = []
    for (ia, ib), r in best.items():
        sa, sb = streams[ia], streams[ib]
        dup_shelf = int(same_shelf(sys_ids[ia], sys_ids[ib], shelf_variants))
        dup_lines = line_agreement(texts[ia], texts[ib])
        fd = flank_dist(sa, sb, r['a0'], r['a1'], r['b0'], r['b1'])
        if fd is None:
            fclass = 'edge'
        elif fd <= 0.52:
            fclass = 'continuation'
        else:
            fclass = 'ambig' if fd <= 0.58 else 'island'
        out_rows.append((
            ids[ia], ids[ib], sys_ids[ia], sys_ids[ib],
            buckets[ia], buckets[ib],
            r['a0'], r['a1'], r['b0'], r['b1'], r['n_anchors'],
            r['aligned_len'], r['density'], dup_shelf, dup_lines,
            -1.0 if fd is None else round(fd, 4), fclass))
    stats['post_s'] = round(time.time() - t2, 1)

    con.execute(f"DROP TABLE IF EXISTS {PAIRS_TABLE}")
    con.execute(f"""
        CREATE TABLE {PAIRS_TABLE} (
            page_a TEXT, page_b TEXT, sys_a TEXT, sys_b TEXT,
            bucket_a TEXT, bucket_b TEXT,
            a0 INT, a1 INT, b0 INT, b1 INT, n_anchors INT,
            aligned_len INT, density REAL,
            dup_shelf INT, dup_lines REAL, flank_dist REAL, flank_class TEXT
        )""")
    con.executemany(
        f"INSERT INTO {PAIRS_TABLE} VALUES "
        "(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", out_rows)
    con.execute(f"CREATE INDEX idx_{PAIRS_TABLE}_sys "
                f"ON {PAIRS_TABLE}(sys_a, sys_b)")
    con.commit()

    # ---- tracer recall ----
    tier1 = json.load(open(
        ROOT + r"\same_work_spike\probe\results\tier1.json", encoding='utf-8'))
    in_db = set(ids)
    acc_keys = {frozenset((ra[0], ra[1])) for ra in out_rows}
    t1_pairs = [p for p in tier1['titles']
                if p['a'] in in_db and p['b'] in in_db]
    t1_hit = sum(1 for p in t1_pairs
                 if frozenset((p['a'], p['b'])) in acc_keys)
    stats['tier1_titles_in_db'] = len(t1_pairs)
    stats['tier1_titles_recall'] = round(t1_hit / max(1, len(t1_pairs)), 4)

    bh_sys_present = {s for s, bk in zip(sys_ids, buckets) if 'bh' in bk}
    bh_connected = set()
    for ra in out_rows:
        if 'bh' in ra[4] and 'bh' in ra[5] and ra[13] == 0 and ra[14] < 0.6:
            bh_connected.update((ra[2], ra[3]))
    stats['bh_sys_present'] = len(bh_sys_present)
    stats['bh_sys_connected'] = len(bh_connected & bh_sys_present)

    # ---- summary breakdowns ----
    from collections import Counter
    stats['flank_classes'] = dict(Counter(r[16] for r in out_rows))
    stats['dup_flagged'] = sum(
        1 for r in out_rows if r[13] or r[14] >= 0.6)
    stats['clean_pairs'] = len(out_rows) - stats['dup_flagged']
    stats['total_s'] = round(time.time() - t0, 1)
    json.dump(stats, open(STATS_OUT, 'w', encoding='utf-8'),
              indent=1, ensure_ascii=False)
    print(json.dumps(stats, indent=1))
    con.close()


if __name__ == '__main__':
    main()
