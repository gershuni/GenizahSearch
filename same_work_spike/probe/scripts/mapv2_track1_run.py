# -*- coding: utf-8 -*-
"""MAPV2 Track-1 runner — tier A/B identification over the v2 corpus state.

FRAG2-PLAN.md step 3.1. Same seed-and-extend engine as track1_match.py
(identical K/band/min_anchors/DF-cap/hull margins, build_ref_index imported)
but with THREE structural changes:

1. WIDE verification: every diagonal-cluster hull is Levenshtein-verified out
   to WIDE_CUTOFF=0.55 edit-density (production boundary is 0.28/<100, 0.35).
   Tier-A acceptance is untouched — a hull with true density <= the production
   boundary gets the SAME distance value under the wider score_cutoff, so the
   tier-A row set is exactly what track1_match.py would produce (verified
   mathematically: accept boundary 0.35 < old cutoff 0.40 < WIDE 0.55).

2. Tier assignment FIRST, physically separate tables (Codex BLOCKER 4 / HIGH):
   - tier A  = (page, work) rows with >=1 hull at the production boundary ->
     `track1_matches` in the V2 db, built from A-accepted hulls ONLY (byte-
     compatible schema + semantics with track1_match.py; shadowing +
     testimonies run on it unchanged).
   - tier B  = (page, work) rows whose hulls verify only in the wide band ->
     `track1_candidates`, stamped with calibrated P(same-work) from the FINAL
     CAL-1 model (margin-band isotonic + decoy-anchored singleton null).
     Census consumers NEVER read this table.
   Tier-B rows can never shadow or displace a tier-A row (shadowing runs
   downstream on track1_matches only).

3. Two-page-merge flag (FRAG2-PLAN step-2 stage-0 addition): a page whose
   tier-A labels contain TWO works on disjoint halves of the stream, on a
   manuscript where FGP disagrees with our page count (stage0_sys_flags,
   fgp_disagree=1), is flagged as a suspected two-page merge: its tier-A rows
   are EXCLUDED from track1_matches and parked in track1_candidates
   (flag='merge_page', P stamped) — kept, flagged, in tier B; census never
   sees them. Disjoint-two-work pages WITHOUT FGP corroboration are recorded
   in mapv2_page_flags (weak signal) but stay in tier A.

Checkpointing (PC-crash lesson 2026-07-08): pages are processed in fixed
rowid-ordered batches of PAGE_BATCH; each batch's rows are committed in ONE
transaction together with the batch cursor (mapv2_meta), so a crash resumes
at the next batch with no partial writes.

Usage: python -X utf8 -u mapv2_track1_run.py [db] [tag] [p_model]
       db      default data\fullcorpus_v2.db (pages.text IS the search text)
       tag     default 'v2'
       p_model default data\p_calibration_final.json (smoke runs may pass
               the pilot model against a throwaway db copy)
Reads: data\ref_corpus_v2.pkl, data\ref_canon_masks_v2.json,
       data\p_calibration_final.json, <db>::pages + stage0_sys_flags
Writes: <db>::track1_matches, <db>::track1_candidates, <db>::mapv2_page_flags,
        <db>::mapv2_meta, results/track1_<tag>_report.md + stats json
"""
import json
import os
import pickle
import sqlite3
import sys
import time
from collections import Counter, defaultdict

import numpy as np
from rapidfuzz.distance import Levenshtein

from engine_np import _gram_codes
from normalize import norm_stream
from track1_match import accept_density, build_ref_index

ROOT = r"C:\Genizahsearch"
PROBE = ROOT + r"\same_work_spike\probe"
DB = sys.argv[1] if len(sys.argv) > 1 else PROBE + r"\data\fullcorpus_v2.db"
TAG = sys.argv[2] if len(sys.argv) > 2 else "v2"
REF = PROBE + r"\data\ref_corpus_v2.pkl"
MASKS = PROBE + r"\data\ref_canon_masks_v2.json"
P_MODEL = sys.argv[3] if len(sys.argv) > 3 else \
    PROBE + r"\data\p_calibration_final.json"
REPORT = PROBE + rf"\results\track1_{TAG}_report.md"
STATS = PROBE + rf"\results\track1_{TAG}_stats.json"

K = 5
BAND, MIN_ANCHORS = 20, 2
MARGIN, MIN_SPAN = 30, 30
PAGE_BATCH = 8000
B_OFF = 256

WIDE_CUTOFF = 0.55        # record hulls out to here (pilot: chance singletons
                          # dominate past ~0.45-0.50 -> nothing usable beyond)
P_MIN_STORE = 0.05        # tier-B storage floor (recall over precision, but
                          # not-ballast: dropped rows are COUNTED in the
                          # report — no silent caps)
OVERLAP_FRAC = 0.5        # competitor test: hull overlap >= this of shorter
MERGE_OVERLAP_MAX = 0.10  # merge-flag: two works' spans overlap < this
GAP_MERGE = 30            # span merging gap (same as track1_match.py)


# =====================================================================
# P model (FINAL CAL-1 artifact)
# =====================================================================

MARGIN_BANDS = ['singleton', 'm_ge_010', 'm_003_010', 'm_0_003', 'not_best']


def margin_band(m, n_competitors):
    if n_competitors == 0:
        return 'singleton'
    if m <= 0:
        return 'not_best'
    if m >= 0.10:
        return 'm_ge_010'
    if m >= 0.03:
        return 'm_003_010'
    return 'm_0_003'


class PModel:
    """P(same-work | alen, density, margin-band) from p_calibration_final.

    - margin-band isotonic knots per (band, length-bin) where fitted;
    - singleton band: decoy-anchored null P = 1 - DecoyBestCDF(len, dens)
      (FRAG2-PLAN singleton finding: the band is unfittable from synthetic
      crops; the chunk-shuffle decoys' best-candidate-density distribution IS
      the chance-singleton null);
    - fallback: pooled per-length isotonic.
    Length feature: nearest calibrated length bin by the row's ALIGNED length
    (calibration crops are fully covered, alen ~= crop len; documented
    approximation for partially-covered pages).
    """

    def __init__(self, path):
        d = json.load(open(path, encoding='utf-8'))
        self.meta = d.get('meta', {})
        self.pooled = {int(L): v for L, v in d.get('model', {}).items()}
        self.margin = {b: {int(L): v for L, v in bins.items()}
                       for b, bins in d.get('margin_model', {}).items()}
        self.singleton_null = {int(L): v for L, v in
                               d.get('singleton_null', {}).items()}
        if not self.pooled:
            raise SystemExit(f"P model {path} has no pooled curves")

    @staticmethod
    def _knot_lookup(knots, dens):
        for max_d, p, _n in knots:
            if dens <= max_d:
                return p
        return knots[-1][1] if knots else 0.0

    def _nearest(self, bins, alen):
        return min(bins, key=lambda L: abs(L - alen))

    def p(self, alen, dens, band):
        if band == 'singleton' and self.singleton_null:
            L = self._nearest(self.singleton_null, alen)
            cdf = self.singleton_null[L]      # [[dens, cum_frac] ...] asc
            frac = 0.0
            for d0, f in cdf:
                if dens <= d0:
                    frac = f
                    break
            else:
                frac = cdf[-1][1] if cdf else 1.0
            return max(0.0, 1.0 - frac)
        bins = self.margin.get(band)
        if bins:
            L = self._nearest(bins, alen)
            return self._knot_lookup(bins[L], dens)
        L = self._nearest(self.pooled, alen)
        return self._knot_lookup(self.pooled[L], dens)


# =====================================================================
# per-page aggregation: hulls -> tier A / tier B rows
# =====================================================================

def merge_spans(spans):
    """[(p0,p1,dens)] -> merged [[p0,p1,dens]] (gap<=GAP_MERGE, min dens) —
    identical to track1_match.py's aggregation."""
    spans = sorted(spans)
    merged = []
    for p0, p1, dens in spans:
        if merged and p0 <= merged[-1][1] + GAP_MERGE:
            merged[-1][1] = max(merged[-1][1], p1)
            merged[-1][2] = min(merged[-1][2], dens)
        else:
            merged.append([p0, p1, dens])
    return merged


def assign_page(pid, sid, plen, hulls_by_work, works, fgp_disagree, pmodel,
                stats):
    """hulls_by_work: work_idx -> [(p0, p1, alen, dens)] all wide-verified.

    Returns (a_rows, b_rows, flag_row) where a_rows go to track1_matches,
    b_rows to track1_candidates, flag_row to mapv2_page_flags (or None)."""
    per_work = {}
    for wi, hulls in hulls_by_work.items():
        a_hulls = [(p0, p1, dens) for p0, p1, alen, dens in hulls
                   if dens <= accept_density(alen)]
        # best evidence hull: lowest density, tie -> largest aligned length
        bp0, bp1, balen, bdens = min(hulls, key=lambda h: (h[3], -h[2]))
        per_work[wi] = {'a_hulls': a_hulls, 'all': hulls,
                        'best': (bp0, bp1, balen, bdens)}

    # ---- margin features: competitors = other works whose BEST hull
    # overlaps this work's best hull by >= OVERLAP_FRAC of the shorter ----
    for wi, d in per_work.items():
        b0, b1, _, bd = d['best']
        comp = []
        for wj, e in per_work.items():
            if wj == wi:
                continue
            c0, c1, _, cd = e['best']
            ov = min(b1, c1) - max(b0, c0)
            if ov > 0 and ov >= OVERLAP_FRAC * min(b1 - b0, c1 - c0):
                comp.append(cd)
        d['n_comp'] = len(comp)
        d['margin'] = (min(comp) - bd) if comp else None

    a_work = [wi for wi, d in per_work.items() if d['a_hulls']]

    # ---- two-page-merge test on the tier-A labels ----
    merge_flag = 0
    weak_flag = 0
    if len(a_work) >= 2 and plen > 0:
        best_a = []
        for wi in a_work:
            ah = max(per_work[wi]['a_hulls'], key=lambda h: h[1] - h[0])
            best_a.append((wi, ah[0], ah[1]))
        best_a.sort(key=lambda t: t[1])
        for i in range(len(best_a) - 1):
            _, a0, a1 = best_a[i]
            _, c0, c1 = best_a[i + 1]
            ov = min(a1, c1) - max(a0, c0)
            disjoint = ov < MERGE_OVERLAP_MAX * min(a1 - a0, c1 - c0)
            halves = ((a0 + a1) / 2 < plen / 2) != ((c0 + c1) / 2 < plen / 2)
            if disjoint and halves:
                weak_flag = 1
                if fgp_disagree:
                    merge_flag = 1
                break

    a_rows, b_rows = [], []
    for wi, d in per_work.items():
        w = works[wi]
        bp0, bp1, balen, bdens = d['best']
        band = ('not_best' if (d['margin'] is not None and d['margin'] <= 0)
                else margin_band(d['margin'] if d['margin'] is not None
                                 else 1.0, d['n_comp']))
        if d['a_hulls'] and not merge_flag:
            merged = merge_spans(d['a_hulls'])
            a_rows.append((
                pid, sid, w['id'], w['cat'], w['genre'], w['author'],
                w['title'], w['mesirah'],
                sum(m[1] - m[0] for m in merged),
                min(m[2] for m in merged), len(merged),
                json.dumps(merged)))
            stats['tierA_rows'] += 1
            continue
        # tier B (incl. merge-demoted tier-A evidence)
        p = pmodel.p(balen, bdens, band)
        flag = 'merge_page' if (d['a_hulls'] and merge_flag) else ''
        if p < P_MIN_STORE and not flag:
            stats[f'tierB_dropped_{band}'] += 1
            continue
        merged = merge_spans([(p0, p1, dens)
                              for p0, p1, _a, dens in d['all']])
        b_rows.append((
            pid, sid, w['id'], w['cat'], w['genre'], w['author'],
            w['title'], w['mesirah'],
            sum(m[1] - m[0] for m in merged),
            balen, round(bdens, 4),
            (round(d['margin'], 4) if d['margin'] is not None else None),
            d['n_comp'], band, round(p, 4), flag,
            len(merged), json.dumps(merged)))
        stats['tierB_rows'] += 1
    flag_row = ((pid, sid, merge_flag, weak_flag)
                if (merge_flag or weak_flag) else None)
    if merge_flag:
        stats['pages_merge_flagged'] += 1
    elif weak_flag:
        stats['pages_weak_flagged'] += 1
    return a_rows, b_rows, flag_row


# =====================================================================
# main
# =====================================================================

def init_tables(con, resume):
    if not resume:
        con.execute("DROP TABLE IF EXISTS track1_matches")
        con.execute("DROP TABLE IF EXISTS track1_candidates")
        con.execute("DROP TABLE IF EXISTS mapv2_page_flags")
        con.execute("DROP TABLE IF EXISTS mapv2_meta")
    con.execute("""
        CREATE TABLE IF NOT EXISTS track1_matches (
            page_id TEXT, sys_id TEXT, work_id TEXT, cat TEXT, genre TEXT,
            author TEXT, title TEXT, mesirah TEXT,
            matched_letters INT, best_density REAL, n_spans INT,
            spans_json TEXT)""")
    con.execute("""
        CREATE TABLE IF NOT EXISTS track1_candidates (
            page_id TEXT, sys_id TEXT, work_id TEXT, cat TEXT, genre TEXT,
            author TEXT, title TEXT, mesirah TEXT,
            matched_letters INT, best_alen INT, best_density REAL,
            margin REAL, n_competitors INT, margin_band TEXT,
            p_same_work REAL, flag TEXT, n_spans INT, spans_json TEXT)""")
    con.execute("""
        CREATE TABLE IF NOT EXISTS mapv2_page_flags (
            page_id TEXT PRIMARY KEY, sys_id TEXT,
            merge_flag INT, weak_two_work_flag INT)""")
    con.execute("""
        CREATE TABLE IF NOT EXISTS mapv2_meta (
            key TEXT PRIMARY KEY, value TEXT)""")


def main():
    t0 = time.time()
    pmodel = PModel(P_MODEL)
    print(f"P model: {P_MODEL} meta={pmodel.meta.get('stage')} "
          f"pooled bins={sorted(pmodel.pooled)} "
          f"margin bands={sorted(pmodel.margin)} "
          f"singleton bins={sorted(pmodel.singleton_null)}", flush=True)

    works = pickle.load(open(REF, 'rb'))
    canon_masks = None
    if os.path.exists(MASKS):
        canon_masks = json.load(open(MASKS, encoding='utf-8'))
        print(f"ref-side canonical masks: {len(canon_masks):,} works "
              f"({MASKS})", flush=True)
    else:
        raise SystemExit(f"missing canonical masks: {MASKS}")
    (seg_streams, seg_work, seg_off, codes_f, seg_f, pos_f,
     df_dropped) = build_ref_index(works, canon_masks)
    print(f"ref: {len(works)} works, {len(seg_streams):,} segments, "
          f"{len(codes_f):,} postings (df-dropped {df_dropped:,} codes) "
          f"({time.time() - t0:.0f}s)", flush=True)

    con = sqlite3.connect(DB)
    con.execute("PRAGMA busy_timeout=120000")
    have_meta = con.execute(
        "SELECT name FROM sqlite_master WHERE name='mapv2_meta'").fetchone()
    done_batch = -1
    saved_stats = None
    if have_meta:
        row = con.execute(
            "SELECT value FROM mapv2_meta WHERE key='done_batch'").fetchone()
        if row is not None:
            done_batch = int(row[0])
        srow = con.execute(
            "SELECT value FROM mapv2_meta WHERE key='stats'").fetchone()
        if srow is not None:
            saved_stats = json.loads(srow[0])
    resume = done_batch >= 0
    init_tables(con, resume)
    if resume:
        print(f"RESUME: batches 0..{done_batch} already committed",
              flush=True)

    fgp_disagree_sys = set()
    if con.execute("SELECT name FROM sqlite_master WHERE "
                   "name='stage0_sys_flags'").fetchone():
        fgp_disagree_sys = {r[0] for r in con.execute(
            "SELECT sys_id FROM stage0_sys_flags WHERE fgp_disagree=1")}
    print(f"fgp_disagree sys_ids: {len(fgp_disagree_sys):,}", flush=True)

    rows = con.execute(
        "SELECT page_id, sys_id, text FROM pages ORDER BY rowid").fetchall()
    ids = [r[0] for r in rows]
    sys_ids = [r[1] for r in rows]
    streams = [norm_stream(r[2])[0] for r in rows]
    del rows
    print(f"pages: {len(ids):,} ({time.time() - t0:.0f}s)", flush=True)

    # stats are persisted per batch (same transaction as the rows) so a
    # crash/resume cannot silently lose drop counts (Codex code-gate HIGH 3
    # — the "no silent caps" invariant must survive restarts).
    stats = Counter(saved_stats or {})
    n_batches = (len(ids) - 1) // PAGE_BATCH + 1
    t1 = time.time()
    for bi in range(n_batches):
        if bi <= done_batch:
            continue
        b0 = bi * PAGE_BATCH
        bpages = range(b0, min(b0 + PAGE_BATCH, len(ids)))
        parts_c, parts_p, parts_pos = [], [], []
        for pi in bpages:
            g = _gram_codes(streams[pi])
            if not len(g):
                continue
            parts_c.append(g.astype(np.uint32))
            parts_p.append(np.full(len(g), pi, np.uint32))
            parts_pos.append(np.arange(len(g), dtype=np.uint32))
        page_hulls = defaultdict(lambda: defaultdict(list))
        if parts_c:
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
            if total:
                cum0 = np.cumsum(counts) - counts
                ref_idx = (np.repeat(lo[sel], counts)
                           + (np.arange(total, dtype=np.int64)
                              - np.repeat(cum0, counts)))
                page_r = np.repeat(pg_p[sel], counts).astype(np.uint64)
                ppos_r = np.repeat(pg_pos[sel], counts).astype(np.int64)
                seg_h = seg_f[ref_idx].astype(np.uint64)
                rpos_h = pos_f[ref_idx].astype(np.int64)
                del ref_idx
                bucket = ((ppos_r - rpos_h) // BAND
                          + B_OFF).astype(np.uint64)
                key = ((page_r << np.uint64(34)) | (seg_h << np.uint64(18))
                       | bucket)
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
                    cutoff = int(WIDE_CUTOFF * alen) + 1
                    dist = Levenshtein.distance(sp[p0:p1], sr[r0:r1],
                                                score_cutoff=cutoff)
                    dens = dist / alen
                    if dens > WIDE_CUTOFF:
                        stats['rej_wide'] += 1
                        continue
                    page_hulls[pi][int(seg_work[si])].append(
                        (p0, p1, alen, round(dens, 4)))

        a_out, b_out, flags_out = [], [], []
        for pi, by_work in page_hulls.items():
            a_rows, b_rows, flag_row = assign_page(
                ids[pi], sys_ids[pi], len(streams[pi]), by_work, works,
                sys_ids[pi] in fgp_disagree_sys, pmodel, stats)
            a_out.extend(a_rows)
            b_out.extend(b_rows)
            if flag_row:
                flags_out.append(flag_row)

        # ---- one transaction: rows + cursor ----
        with con:
            con.executemany(
                "INSERT INTO track1_matches VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                a_out)
            con.executemany(
                "INSERT INTO track1_candidates VALUES "
                "(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", b_out)
            con.executemany(
                "INSERT OR REPLACE INTO mapv2_page_flags VALUES (?,?,?,?)",
                flags_out)
            con.execute("INSERT OR REPLACE INTO mapv2_meta VALUES "
                        "('done_batch', ?)", (str(bi),))
            con.execute("INSERT OR REPLACE INTO mapv2_meta VALUES "
                        "('stats', ?)", (json.dumps(dict(stats)),))
        print(f"  batch {bi + 1}/{n_batches}: "
              f"A+={len(a_out):,} B+={len(b_out):,} "
              f"totA={stats['tierA_rows']:,} totB={stats['tierB_rows']:,} "
              f"cand={stats['candidates']:,} ({time.time() - t1:.0f}s)",
              flush=True)

    con.execute("CREATE INDEX IF NOT EXISTS idx_t1_page "
                "ON track1_matches(page_id)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_t1c_page "
                "ON track1_candidates(page_id)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_t1c_work "
                "ON track1_candidates(work_id)")
    con.commit()

    # ---- report ----
    nA = con.execute("SELECT COUNT(*) FROM track1_matches").fetchone()[0]
    nB = con.execute("SELECT COUNT(*) FROM track1_candidates").fetchone()[0]
    pagesA = con.execute(
        "SELECT COUNT(DISTINCT page_id) FROM track1_matches").fetchone()[0]
    pagesB = con.execute("SELECT COUNT(DISTINCT page_id) FROM "
                         "track1_candidates").fetchone()[0]
    band_hist = dict(con.execute(
        "SELECT margin_band, COUNT(*) FROM track1_candidates "
        "GROUP BY margin_band"))
    p_hist = dict(con.execute(
        "SELECT ROUND(p_same_work, 1), COUNT(*) FROM track1_candidates "
        "GROUP BY 1 ORDER BY 1"))
    dropped = {k: v for k, v in stats.items()
               if k.startswith('tierB_dropped')}
    lines = [
        f"# Track-1 v2 — tier A/B identification ('{TAG}', "
        f"{len(ids):,} pages)", "",
        f"- reference: {len(works):,} works (v2, header-fix + REF-2), "
        f"canonical masks: {len(canon_masks):,} works",
        f"- WIDE verification cutoff: {WIDE_CUTOFF}; production boundary "
        f"unchanged (0.28/<100, 0.35)",
        f"- **tier A rows: {nA:,}** on {pagesA:,} pages "
        f"(track1_matches — census path, shadowing downstream)",
        f"- **tier B rows: {nB:,}** on {pagesB:,} pages "
        f"(track1_candidates — P-stamped, census never reads)",
        f"- tier-B storage floor P >= {P_MIN_STORE}; dropped-below-floor "
        f"counts (NO silent caps): {dropped or 'none'}",
        f"- margin-band histogram (stored tier B): {band_hist}",
        f"- P histogram (stored tier B): {p_hist}",
        f"- pages merge-flagged (excluded from tier A): "
        f"{stats['pages_merge_flagged']:,}; weak two-work flags (tier A "
        f"kept): {stats['pages_weak_flagged']:,}",
        f"- engine: hits {stats['hits']:,}, hull candidates "
        f"{stats['candidates']:,}, rej_short {stats['rej_short']:,}, "
        f"rej_wide {stats['rej_wide']:,}",
        f"- total {((time.time() - t0) / 60):.0f} min",
    ]
    stats_out = {
        'pages': len(ids), 'tierA_rows': nA, 'tierB_rows': nB,
        'pagesA': pagesA, 'pagesB': pagesB,
        'band_hist': band_hist,
        'p_hist': {str(k): v for k, v in p_hist.items()},
        'dropped_below_floor': dropped,
        'merge_flagged': stats['pages_merge_flagged'],
        'weak_flagged': stats['pages_weak_flagged'],
        'hits': int(stats['hits']), 'candidates': int(stats['candidates']),
        'wide_cutoff': WIDE_CUTOFF, 'p_min_store': P_MIN_STORE,
        'total_s': round(time.time() - t0, 1),
    }
    json.dump(stats_out, open(STATS, 'w'), indent=1)
    open(REPORT, 'w', encoding='utf-8').write('\n'.join(lines))
    print('\n'.join(lines))
    print(f"\nwrote {REPORT}")
    con.close()


if __name__ == '__main__':
    main()
