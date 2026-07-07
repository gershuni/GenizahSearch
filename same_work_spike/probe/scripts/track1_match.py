# -*- coding: utf-8 -*-
"""Track 1: match Genizah HTR pages against the Maagarim+JA reference corpus.

Asymmetric seed-and-extend (numpy): reference gram index -> page-gram lookup
-> diagonal two-hit per (page, ref-segment) -> one-sided verify.
One-sided noise (noisy HTR vs clean edition) => tighter boundary than the
two-sided Track-2 rule: density <= 0.28 (<100 letters) / 0.35 (>=100).

Usage: python track1_match.py [db_path] [tag]
Writes: <db>::track1_matches, results/track1_<tag>_report.md + stats json
"""
import json
import pickle
import sqlite3
import sys
import time
from collections import Counter, defaultdict

import numpy as np
from rapidfuzz.distance import Levenshtein

from engine_np import _gram_codes
from normalize import norm_stream

ROOT = r"C:\Genizahsearch"
DB = sys.argv[1] if len(sys.argv) > 1 else \
    ROOT + r"\same_work_spike\probe\data\rehearsal.db"
TAG = sys.argv[2] if len(sys.argv) > 2 else "100k"
REF = ROOT + r"\same_work_spike\probe\data\ref_corpus.pkl"
REPORT = ROOT + rf"\same_work_spike\probe\results\track1_{TAG}_report.md"
STATS = ROOT + rf"\same_work_spike\probe\results\track1_{TAG}_stats.json"

K = 5
SEG_LEN, SEG_OVERLAP = 3800, 200
BAND, MIN_ANCHORS = 20, 2
REF_DF_CAP = 128          # max reference entries per gram code
MARGIN, MIN_SPAN = 30, 30
PAGE_BATCH = 8000
B_OFF = 256               # bucket offset (ref pos < 4096 -> bucket >= -190)


def accept_density(length):
    return 0.28 if length < 100 else 0.35


def build_ref_index(works):
    seg_streams, seg_work, seg_off = [], [], []
    for wi, w in enumerate(works):
        s = w['stream']
        step = SEG_LEN - SEG_OVERLAP
        for off in range(0, max(1, len(s) - SEG_OVERLAP), step):
            seg = s[off:off + SEG_LEN]
            if len(seg) >= K:
                seg_streams.append(seg)
                seg_work.append(wi)
                seg_off.append(off)
    assert len(seg_streams) < (1 << 16), len(seg_streams)
    parts = []
    for si, seg in enumerate(seg_streams):
        g = _gram_codes(seg)
        key = ((g << np.uint64(28)) | (np.uint64(si) << np.uint64(12))
               | np.arange(len(g), dtype=np.uint64))
        parts.append(key)
    keys = np.concatenate(parts)
    del parts
    keys.sort()
    codes = (keys >> np.uint64(28)).astype(np.uint32)
    # DF cap per code
    starts = np.flatnonzero(np.r_[True, codes[1:] != codes[:-1]])
    sizes = np.diff(np.r_[starts, len(codes)])
    keep_grp = sizes <= REF_DF_CAP
    keep = np.repeat(keep_grp, sizes)
    dropped = int((~keep_grp).sum())
    keys = keys[keep]
    codes_f = (keys >> np.uint64(28)).astype(np.uint32)
    seg_f = ((keys >> np.uint64(12))
             & np.uint64(0xFFFF)).astype(np.uint32)
    pos_f = (keys & np.uint64(0xFFF)).astype(np.uint16)
    return (seg_streams, np.array(seg_work, np.int32),
            np.array(seg_off, np.int64), codes_f, seg_f, pos_f, dropped)


def main():
    t0 = time.time()
    works = pickle.load(open(REF, 'rb'))
    (seg_streams, seg_work, seg_off, codes_f, seg_f, pos_f,
     df_dropped) = build_ref_index(works)
    print(f"ref: {len(works)} works, {len(seg_streams):,} segments, "
          f"{len(codes_f):,} postings (df-dropped {df_dropped:,} codes) "
          f"({time.time() - t0:.0f}s)", flush=True)

    con = sqlite3.connect(DB)
    rows = con.execute(
        "SELECT page_id, sys_id, text FROM pages ORDER BY rowid").fetchall()
    ids = [r[0] for r in rows]
    sys_ids = [r[1] for r in rows]
    streams = [norm_stream(r[2])[0] for r in rows]
    del rows
    print(f"pages: {len(ids):,} ({time.time() - t0:.0f}s)", flush=True)

    page_hits = defaultdict(list)   # page_idx -> [(work, p0, p1, dens, seg)]
    stats = Counter()
    t1 = time.time()
    for b0 in range(0, len(ids), PAGE_BATCH):
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
        stats['hits'] += total
        if not total:
            continue
        cum0 = np.cumsum(counts) - counts
        ref_idx = (np.repeat(lo[sel], counts)
                   + (np.arange(total, dtype=np.int64)
                      - np.repeat(cum0, counts)))
        page_r = np.repeat(pg_p[sel], counts).astype(np.uint64)
        ppos_r = np.repeat(pg_pos[sel], counts).astype(np.int64)
        seg_h = seg_f[ref_idx].astype(np.uint64)
        rpos_h = pos_f[ref_idx].astype(np.int64)
        del ref_idx
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
        stats['candidates'] += len(c_pair)
        # verify
        for i in range(len(c_pair)):
            pi = int(c_pair[i] >> np.uint64(16))
            si = int(c_pair[i] & np.uint64(0xFFFF))
            sp, sr = streams[pi], seg_streams[si]
            p0 = max(0, int(c_minp[i]) - MARGIN)
            p1 = min(len(sp), int(c_maxp[i]) + K + MARGIN)
            r0 = max(0, int(c_minr[i]) - MARGIN)
            r1 = min(len(sr), int(c_maxr[i]) + K + MARGIN)
            if min(p1 - p0, r1 - r0) < MIN_SPAN:
                stats['rej_short'] += 1
                continue
            alen = max(p1 - p0, r1 - r0)
            cutoff = int(0.40 * alen) + 1
            dist = Levenshtein.distance(sp[p0:p1], sr[r0:r1],
                                        score_cutoff=cutoff)
            dens = dist / alen
            if dens > accept_density(alen):
                stats['rej_density'] += 1
                continue
            page_hits[pi].append(
                (int(seg_work[si]), p0, p1, round(dens, 4), si))
        print(f"  batch {b0 // PAGE_BATCH + 1}/"
              f"{(len(ids) - 1) // PAGE_BATCH + 1}: hits={total:,} "
              f"cand={stats['candidates']:,} "
              f"accepted-pages={len(page_hits):,} "
              f"({time.time() - t1:.0f}s)", flush=True)

    # ---- aggregate per (page, work): merge page intervals ----
    out_rows = []
    for pi, hits in page_hits.items():
        by_work = defaultdict(list)
        for wi, p0, p1, dens, si in hits:
            by_work[wi].append((p0, p1, dens))
        for wi, spans in by_work.items():
            spans.sort()
            merged = []
            for p0, p1, dens in spans:
                if merged and p0 <= merged[-1][1] + 30:
                    merged[-1][1] = max(merged[-1][1], p1)
                    merged[-1][2] = min(merged[-1][2], dens)
                else:
                    merged.append([p0, p1, dens])
            w = works[wi]
            out_rows.append((
                ids[pi], sys_ids[pi], w['id'], w['cat'], w['genre'],
                w['author'], w['title'], w['mesirah'],
                sum(m[1] - m[0] for m in merged),
                min(m[2] for m in merged), len(merged),
                json.dumps([[m[0], m[1], m[2]] for m in merged]),
            ))
    con.execute("DROP TABLE IF EXISTS track1_matches")
    con.execute("""
        CREATE TABLE track1_matches (
            page_id TEXT, sys_id TEXT, work_id TEXT, cat TEXT, genre TEXT,
            author TEXT, title TEXT, mesirah TEXT,
            matched_letters INT, best_density REAL, n_spans INT,
            spans_json TEXT)""")
    con.executemany("INSERT INTO track1_matches VALUES "
                    "(?,?,?,?,?,?,?,?,?,?,?,?)", out_rows)
    con.execute("CREATE INDEX idx_t1_page ON track1_matches(page_id)")
    con.commit()

    # ---- report ----
    from build_reuse_graph import load_domains, _group_of, DOMAIN_GROUPS  # noqa
    domains = load_domains()
    pages_matched = {r[0] for r in out_rows}
    strong = {r[0] for r in out_rows if r[8] >= 100}
    lines = [f"# Track 1 — Maagarim/JA identification over rehearsal "
             f"'{TAG}' ({len(ids):,} pages)", ""]
    lines += [
        f"- reference: {len(works):,} works "
        f"({sum(len(w['stream']) for w in works):,} letters)",
        f"- pages with >=1 identification: **{len(pages_matched):,} "
        f"({100 * len(pages_matched) / len(ids):.1f}%)**",
        f"- pages with >=100 matched letters: **{len(strong):,} "
        f"({100 * len(strong) / len(ids):.1f}%)**",
        f"- (page, work) identification rows: {len(out_rows):,}",
        f"- candidates {stats['candidates']:,}, accepted spans kept, "
        f"rej_density {stats['rej_density']:,}, "
        f"rej_short {stats['rej_short']:,}", "",
    ]
    # per page-domain-group match rate
    grp_tot = Counter()
    grp_hit = Counter()
    other = len(DOMAIN_GROUPS) - 1
    for pi, s in enumerate(sys_ids):
        rec = domains.get(s)
        g = other
        if rec and rec[0]:
            top = rec[0].most_common(2)
            g = (top[1][0] if top[0][0] == other and len(top) > 1
                 else top[0][0])
        grp_tot[g] += 1
        if ids[pi] in pages_matched:
            grp_hit[g] += 1
    lines.append("## Match rate by page domain group "
                 "(catalog domain of the page's MS)")
    for g, tot in grp_tot.most_common():
        lines.append(f"- {DOMAIN_GROUPS[g][0]}: {grp_hit[g]:,}/{tot:,} "
                     f"= {100 * grp_hit[g] / tot:.1f}%")
    lines.append("")
    # per reference category
    cat_pages = Counter()
    for r in out_rows:
        cat_pages[r[3]] += 1
    lines.append(f"## Identification rows by reference category: "
                 f"{dict(cat_pages.most_common())}")
    lines.append("")
    # top works
    work_pages = Counter()
    work_letters = Counter()
    for r in out_rows:
        wkey = f"{r[5]} — {r[6]}" if r[5] else r[6]
        work_pages[(r[3], wkey)] += 1
        work_letters[(r[3], wkey)] += r[8]
    lines.append("## Top identified works (by distinct pages)")
    for (cat, wkey), c in work_pages.most_common(40):
        lines.append(f"- [{cat}] {wkey}: {c:,} pages, "
                     f"{work_letters[(cat, wkey)]:,} letters")
    lines.append("")
    # mesirah cross-check sample
    mes = [r for r in out_rows if r[7] and r[8] >= 150]
    lines.append(f"## Mesirah cross-check (ref works that ARE Genizah "
                 f"editions; {len(mes):,} strong rows) — sample 20")
    for r in sorted(mes, key=lambda x: -x[8])[:20]:
        lines.append(f"- page `{r[0]}` ({r[8]} letters, d={r[9]:.2f}) "
                     f"= {r[6][:40]} | mesirah: {r[7][:60]}")
    stats_out = {
        'pages': len(ids), 'pages_matched': len(pages_matched),
        'pages_strong': len(strong), 'rows': len(out_rows),
        'hits': int(stats['hits']), 'candidates': int(stats['candidates']),
        'rej_density': int(stats['rej_density']),
        'cat_rows': dict(cat_pages),
        'total_s': round(time.time() - t0, 1),
    }
    json.dump(stats_out, open(STATS, 'w'), indent=1)
    open(REPORT, 'w', encoding='utf-8').write('\n'.join(lines))
    print('\n'.join(lines[:40]))
    print(f"\nwrote {REPORT} ({time.time() - t0:.0f}s total)")
    con.close()


if __name__ == '__main__':
    main()
