# -*- coding: utf-8 -*-
"""Work-query second pass -- DF-policy v2, work-keyed (SEED-029 spike A2).

Each work in the DF-cap damage cohort becomes a retrieval QUERY against the
corpus, reusing motif_query.py's mechanics almost verbatim: a private
per-query reference index (`build_ref_index`, imported UNMODIFIED from
track1_match.py) gives each work's grams immunity from the engine_np /
rehearsal_run.py corpus-side DF<=100 cap, and Levenshtein verification uses
the SAME two-sided sloped acceptance boundary as motif_query.py. The one
deliberate deviation: unlike motif_query (query = a noisy page excerpt
standing in for a motif), each work's query text here is its CLEAN
Maagarim/JA reference edition (ref_corpus.pkl) -- the exact text
track1_match.py already used to identify the cohort's known witnesses -- so
ref-side canonical masking (ref_canon_masks.json, same as track1_match.py's
own main()) is applied. Several cohort works (the Rambam Mishnah-commentary
orders, ראב"ש מלכים א, סעדיה בן דוד מדרש הבאור) DO have canon masks because
they quote Bible/Mishnah/Talmud verbatim; skipping the mask would reintroduce
exactly the false-ID-via-shared-canonical-text class track1_shadow.py /
mask_ref_canon.py exists to fix.

Query-set resolution (`resolve_cohort`) ALWAYS re-derives df_damage.py's own
cohort definitions against fullcorpus.db -- the census that DEFINES the
cohort -- independent of which db is actually being scanned. df_damage_full.md
prints only the worst 15/30 rows of each list, so the full ~40-90 work query
set must be recomputed here, not scraped from markdown. Pairing-rate
before/after is then measured on the TARGET db's own
track1_matches/accepted_pairs_canonmask (liturgy.db for the dry run is a
139,694-page subcorpus, not the 667,411-page fullcorpus.db).

Checkpointing mirrors motif_query.py's pattern (commit 7bc6b6b0) exactly:
a `work_query_ckpt(next_b0, hits_json)` table written every CKPT_EVERY
batches, read back on the next invocation to resume mid-scan.

Usage: python -X utf8 -u work_query.py [db] [tag] [--limit N] [--only ID,ID,...]
Out: <db>::work_query_hits, results/a2_work_query_<tag>_report.md
"""
import argparse
import hashlib
import json
import os
import pickle
import re
import sqlite3
import time
from collections import defaultdict

import numpy as np
from rapidfuzz.distance import Levenshtein

from engine_np import _gram_codes
from normalize import norm_stream
from stage0 import line_agreement, load_shelf_variants, same_shelf
from track1_match import SEG_LEN, SEG_OVERLAP

ROOT = r"C:\Genizahsearch"
PROBE = ROOT + r"\same_work_spike\probe"
FULLCORPUS = PROBE + r"\data\fullcorpus.db"
REF = PROBE + r"\data\ref_corpus.pkl"
CANON_MASKS = PROBE + r"\data\ref_canon_masks.json"

K = 5
BAND, MIN_ANCHORS, B_OFF = 20, 2, 256
MARGIN, MIN_SPAN, PAGE_BATCH = 30, 40, 8000
CKPT_EVERY = 8   # batches between checkpoint commits (motif_query.py convention)
# ---- reference-side generic-gram filter: DISTINCT-WORK DF cap ----
# (Codex pre-launch review BLOCKER-1/2). track1_match.build_ref_index caps on
# RAW POSTING COUNT per gram-code across the single shared index and DROPS
# ALL postings of any code over the cap. That is catastrophic for work_query:
# our whole point is per-query DF immunity so a HIGHLY-COPIED work's grams
# survive the corpus DF<=100 cap -- but a raw-posting cap then deletes exactly
# those grams (a formula repeated many times across a big work's segments, or
# a formula shared across several cohort works, blows the raw count) and
# silently re-suppresses the very texts this pass exists to recover.
# The right discriminator is DISTINCT QUERY WORKS per gram: a gram in few
# works is discriminative (keep ALL its postings, however often it repeats --
# so a formula repeated 100x WITHIN one work has distinct-work-count 1 and
# always survives: that is the BLOCKER fix); a gram spread across MANY
# unrelated cohort works is generic connective tissue (drop it -- it is what
# scatters spurious anchors AND explodes candidate volume).
# The cap is a SMALL ABSOLUTE count, NOT a fraction of the cohort: distinct-
# work frequency is meaningful in absolute terms (a gram in 4+ of these
# heterogeneous works -- piyyut + Rambam-JA + Saadia + grammar -- is generic
# regardless of whether the cohort has 5 works or 500). An earlier frac=0.5
# (cap=24/49) let the semi-generic 4-24-work band through and produced 34.5M
# candidates in a single 8k-page batch (~15 min/batch, unusable); cap=3 keeps
# the discriminative 1-3-work band (dw-hist: 458k+302k codes) and drops the
# 302k generic 4+-work codes. Per-work retention + dw-histogram are printed
# so the choice stays auditable (--index-audit); overridable via --work-df-cap.
WORK_DF_CAP_DEFAULT = 3
# debug-only knob for kill/resume testing -- unset in production (0 = off)
_DEBUG_SLEEP = float(os.environ.get('WORK_QUERY_DEBUG_SLEEP', '0') or 0)

MIN_LETTERS = 200                       # df_damage.py substantive-presence gate
SHORT_LEN_MAX, SHORT_MIN_MS, SHORT_RATE_MAX = 2000, 10, 0.60
BIG_MIN_MS, BIG_RATE_MAX = 20, 0.30
EDITED_CATS = {'Maagarim', 'JA'}


def accept_density(length):
    # two-sided HTR-noise boundary, reused verbatim from motif_query.py
    return 0.30 if length < 100 else (0.386 if length < 200 else 0.418)


def build_work_query_index(works, canon_masks, work_df_cap):
    """Mirror of track1_match.build_ref_index BUT the DF cap counts DISTINCT
    QUERY WORKS per gram-code (not raw postings) and keeps ALL postings of
    surviving codes (Codex BLOCKER-1/2 fix; see WORK_DF_CAP_FRAC note).
    Same key packing as build_ref_index: (code<<28)|(seg<<12)|pos, so pos<4096
    (SEG_LEN=3800) and seg<65536. Returns build_ref_index's 7-tuple + a
    per-work retention dict {work_id: (kept, total)} + the distinct-work
    count array (for the histogram)."""
    seg_streams, seg_work_l, seg_off_l = [], [], []
    for wi, w in enumerate(works):
        s = w['stream']
        step = SEG_LEN - SEG_OVERLAP
        for off in range(0, max(1, len(s) - SEG_OVERLAP), step):
            seg = s[off:off + SEG_LEN]
            if len(seg) >= K:
                seg_streams.append(seg)
                seg_work_l.append(wi)
                seg_off_l.append(off)
    assert len(seg_streams) < (1 << 16), len(seg_streams)
    seg_work = np.array(seg_work_l, np.int32)
    seg_off = np.array(seg_off_l, np.int64)

    n_masked = 0
    parts = []
    for si, seg in enumerate(seg_streams):
        g = _gram_codes(seg)
        pos = np.arange(len(g), dtype=np.uint64)
        if canon_masks:
            iv = canon_masks.get(works[int(seg_work[si])]['id'])
            if iv:
                off = int(seg_off[si])
                keep = np.ones(len(g), dtype=bool)
                for m0, m1 in iv:
                    l0, l1 = m0 - off, m1 - off
                    if l1 <= 0 or l0 >= len(g):
                        continue
                    keep[max(0, int(l0) - K + 1):max(0, int(l1))] = False
                n_masked += int(len(g) - keep.sum())
                g, pos = g[keep], pos[keep]
                if not len(g):
                    continue
        key = ((g << np.uint64(28)) | (np.uint64(si) << np.uint64(12)) | pos)
        parts.append(key)
    if n_masked:
        print(f"ref index: {n_masked:,} grams canonical-masked", flush=True)
    keys = np.concatenate(parts)
    del parts
    keys.sort()

    codes = (keys >> np.uint64(28)).astype(np.int64)
    segs = ((keys >> np.uint64(12)) & np.uint64(0xFFFF)).astype(np.int64)
    wof = seg_work[segs].astype(np.int64)           # work index per posting
    n_w = len(works)
    # distinct-work count per code, via unique (code, work) pairs
    pair = codes * n_w + wof                        # code < 2^36, n_w small
    ucode, dw_count = np.unique(np.unique(pair) // n_w, return_counts=True)
    dw_per_posting = dw_count[np.searchsorted(ucode, codes)]
    keep = dw_per_posting <= work_df_cap
    dropped_codes = int((dw_count > work_df_cap).sum())

    kept_by_w = np.bincount(wof[keep], minlength=n_w)
    tot_by_w = np.bincount(wof, minlength=n_w)
    retention = {works[wi]['id']: (int(kept_by_w[wi]), int(tot_by_w[wi]))
                 for wi in range(n_w)}

    keys = keys[keep]
    codes_f = (keys >> np.uint64(28)).astype(np.uint32)
    seg_f = ((keys >> np.uint64(12)) & np.uint64(0xFFFF)).astype(np.uint32)
    pos_f = (keys & np.uint64(0xFFF)).astype(np.uint16)
    return (seg_streams, seg_work, seg_off, codes_f, seg_f, pos_f,
            dropped_codes, retention, dw_count)


def reference_mediated_pairing(con, hits_tbl, min_letters):
    """df_damage-COMPATIBLE pairing from work-query hits (Codex HIGH-5).
    Two pages pair for a work iff BOTH carry it via work-query hits with
    >= min_letters aggregated matched letters, DIFFERENT sys_id, and pass the
    same_shelf / line_agreement<0.6 duplicate-photography filters (the exact
    filters df_damage.py applies to accepted_pairs_canonmask). Returns
    {work_id: set(page_ids in >=1 valid pair)} + {work_id: [(pid,sid)] qpages}."""
    agg = defaultdict(int)
    page_sys = {}
    for wid, pid, sid, ml in con.execute(
            f"SELECT work_id, page_id, sys_id, matched_letters FROM {hits_tbl}"):
        agg[(wid, pid)] += ml
        page_sys[pid] = sid
    qpages = defaultdict(list)
    for (wid, pid), ml in agg.items():
        if ml >= min_letters:
            qpages[wid].append((pid, page_sys[pid]))

    all_pids = {pid for lst in qpages.values() for pid, _ in lst}
    texts = {}
    pid_list = list(all_pids)
    for i in range(0, len(pid_list), 900):
        chunk = pid_list[i:i + 900]
        ph = ','.join('?' * len(chunk))
        for pid, txt in con.execute(
                f"SELECT page_id, text FROM pages WHERE page_id IN ({ph})",
                chunk):
            texts[pid] = txt
    shelf = load_shelf_variants()

    paired = {}
    for wid, members in qpages.items():
        pp = set()
        for i in range(len(members)):
            pi, si = members[i]
            for j in range(i + 1, len(members)):
                pj, sj = members[j]
                if si == sj or same_shelf(si, sj, shelf):
                    continue
                if line_agreement(texts.get(pi, ''),
                                  texts.get(pj, '')) >= 0.6:
                    continue
                pp.add(pi)
                pp.add(pj)
        paired[wid] = pp
    return paired, qpages


def _live_cond(con):
    cols = [r[1] for r in con.execute("PRAGMA table_info(track1_matches)")]
    return "shadowed_by IS NULL" if 'shadowed_by' in cols else None


def resolve_cohort():
    """Re-derive df_damage.py's own two cohort definitions against the
    canonical full-corpus census (ALWAYS fullcorpus.db, regardless of the
    scan target) -- (a) short-work cohort (<2,000 letters, >=10 MSS,
    pairing rate <60%), (b) >=20-MSS edited works with pairing rate <30%.
    Returns a list of dicts sorted worst-rate-first."""
    con = sqlite3.connect(FULLCORPUS)
    page_works = defaultdict(set)
    work_pages = defaultdict(set)
    work_ms = defaultdict(set)
    work_info = {}
    cond = _live_cond(con)
    q = ("SELECT page_id, sys_id, work_id, cat, author, title, "
         "matched_letters FROM track1_matches")
    if cond:
        q += f" WHERE {cond}"
    for pid, sid, wid, cat, author, title, letters in con.execute(q):
        if letters < MIN_LETTERS:
            continue
        page_works[pid].add(wid)
        work_pages[wid].add(pid)
        work_ms[wid].add(sid)
        work_info[wid] = (cat, f"{author} — {title}" if author else title)
    # single-witness works cannot pair (df_damage.py convention)
    for wid in [w for w, ms in work_ms.items() if len(ms) < 2]:
        for pid in work_pages[wid]:
            page_works[pid].discard(wid)
        del work_pages[wid], work_ms[wid]

    paired_e = defaultdict(set)
    for pa, pb in con.execute(
            "SELECT page_a, page_b FROM accepted_pairs_canonmask "
            "WHERE dup_shelf = 0 AND dup_lines < 0.6"):
        wa = page_works.get(pa)
        if not wa:
            continue
        wb = page_works.get(pb)
        if not wb:
            continue
        for w in wa & wb:
            if work_info[w][0] in EDITED_CATS:
                paired_e[w].add(pa)
                paired_e[w].add(pb)
    con.close()

    wlen = {w['id']: len(w['stream'])
            for w in pickle.load(open(REF, 'rb'))}

    cohort = []
    for wid, pages in work_pages.items():
        cat, name = work_info[wid]
        if cat not in EDITED_CATS or len(pages) < 2:
            continue
        n_ms = len(work_ms[wid])
        rate = len(paired_e.get(wid, ())) / len(pages)
        is_short = (n_ms >= SHORT_MIN_MS
                    and wlen.get(wid, 10 ** 9) < SHORT_LEN_MAX)
        is_big = n_ms >= BIG_MIN_MS
        reasons = []
        if is_short and rate < SHORT_RATE_MAX:
            reasons.append('short')
        if is_big and rate < BIG_RATE_MAX:
            reasons.append('big')
        if reasons:
            cohort.append({
                'work_id': wid, 'cat': cat, 'name': name, 'n_ms': n_ms,
                'pages': len(pages), 'paired': len(paired_e.get(wid, ())),
                'rate': rate, 'reasons': reasons, 'letters': wlen.get(wid),
            })
    cohort.sort(key=lambda c: c['rate'])
    return cohort


def measure_pairing(con, work_ids):
    """Pairing stats on whichever db `con` has open, scoped to work_ids --
    df_damage.py's own method, but restricted to the query set so it can be
    rerun cheaply against a subcorpus (liturgy.db) or the full corpus."""
    if not work_ids:
        return {}
    cond = _live_cond(con)
    ph = ','.join('?' * len(work_ids))
    where = [f"work_id IN ({ph})"]
    if cond:
        where.append(cond)
    q = ("SELECT page_id, sys_id, work_id, matched_letters FROM "
         f"track1_matches WHERE {' AND '.join(where)}")
    page_works = defaultdict(set)
    work_pages = defaultdict(set)
    work_ms_all = defaultdict(set)   # ANY matched_letters (live census)
    for pid, sid, wid, letters in con.execute(q, work_ids):
        work_ms_all[wid].add(sid)
        if letters < MIN_LETTERS:
            continue
        page_works[pid].add(wid)
        work_pages[wid].add(pid)

    paired = defaultdict(set)
    for pa, pb in con.execute(
            "SELECT page_a, page_b FROM accepted_pairs_canonmask "
            "WHERE dup_shelf = 0 AND dup_lines < 0.6"):
        wa = page_works.get(pa)
        if not wa:
            continue
        wb = page_works.get(pb)
        if not wb:
            continue
        for w in wa & wb:
            paired[w].add(pa)
            paired[w].add(pb)

    out = {}
    for wid in work_ids:
        pages = work_pages.get(wid, set())
        out[wid] = {
            'pages': pages,
            'paired': paired.get(wid, set()) & pages,
            'ms_all': work_ms_all.get(wid, set()),
        }
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('db', nargs='?', default=FULLCORPUS)
    ap.add_argument('tag', nargs='?', default='full')
    ap.add_argument('--limit', type=int, default=None,
                     help='process only the N worst (lowest-rate) cohort works')
    ap.add_argument('--only', default=None,
                     help='comma-separated work_id allowlist (overrides '
                          '--limit; for dry runs / smoke tests)')
    ap.add_argument('--work-df-cap', type=int, default=None,
                     help=f'distinct-work DF cap (default {WORK_DF_CAP_DEFAULT}); '
                          'grams appearing in more query works than this are '
                          'dropped as generic')
    ap.add_argument('--index-audit', action='store_true',
                     help='build the full-cohort index, print the dw-histogram '
                          '+ per-work retention, then exit (no corpus scan)')
    args = ap.parse_args()
    DB, TAG = args.db, args.tag
    if not re.match(r'^\w+$', TAG):
        raise SystemExit(f"tag must be alphanumeric/underscore: {TAG!r}")
    CKPT_TBL = f"work_query_ckpt_{TAG}"
    HITS_TBL = f"work_query_hits_{TAG}"
    OUT = PROBE + rf"\results\a2_work_query_{TAG}_report.md"

    t0 = time.time()
    cohort = resolve_cohort()
    print(f"resolved cohort (fullcorpus.db census, MIN_LETTERS="
          f"{MIN_LETTERS}): {len(cohort):,} works", flush=True)

    if args.only:
        only = args.only.split(',')
        by_id = {c['work_id']: c for c in cohort}
        cohort = [by_id[wid] for wid in only if wid in by_id]
        missing = [wid for wid in only if wid not in by_id]
        if missing:
            print(f"  WARNING: --only ids not in cohort: {missing}",
                  flush=True)
    elif args.limit:
        cohort = cohort[:args.limit]

    ref_by_id = {w['id']: w for w in pickle.load(open(REF, 'rb'))}
    works, cohort2 = [], []
    for c in cohort:
        w = ref_by_id.get(c['work_id'])
        if w is None:
            print(f"  WARNING: {c['work_id']} not in ref_corpus.pkl -- "
                  f"skipped", flush=True)
            continue
        works.append(w)
        cohort2.append(c)
    cohort = cohort2
    print(f"query set: {len(works):,} works "
          f"({sum(len(w['stream']) for w in works):,} reference letters)",
          flush=True)
    for c in cohort:
        print(f"  [{','.join(c['reasons']):>9}] {100 * c['rate']:5.1f}% "
              f"ms={c['n_ms']:3d} pages={c['pages']:3d} "
              f"letters={c['letters']:>7} {c['work_id']} {c['name'][:55]}",
              flush=True)

    canon_masks, n_apply = None, 0
    if os.path.exists(CANON_MASKS):
        canon_masks = json.load(open(CANON_MASKS, encoding='utf-8'))
        n_apply = sum(1 for w in works if w['id'] in canon_masks)
    print(f"ref-side canonical masks: {n_apply}/{len(works)} query works "
          f"masked ({time.time() - t0:.0f}s)", flush=True)

    work_df_cap = args.work_df_cap or WORK_DF_CAP_DEFAULT
    (seg_streams, seg_work, seg_off, codes_f, seg_f, pos_f,
     df_dropped, retention, dw_count) = build_work_query_index(
        works, canon_masks, work_df_cap)
    hist = defaultdict(int)
    for dwc in dw_count.tolist():
        b = ('1' if dwc == 1 else '2-3' if dwc <= 3 else '4-9' if dwc <= 9
             else '10-24' if dwc <= 24 else '25+')
        hist[b] += 1
    print(f"index: {len(seg_streams):,} segments, {len(codes_f):,} postings; "
          f"distinct-work cap={work_df_cap} -> dropped {df_dropped:,} generic "
          f"gram-codes; dw-hist {dict(hist)} ({time.time() - t0:.0f}s)",
          flush=True)
    low_ret = sorted(((wid, k, t) for wid, (k, t) in retention.items()
                      if t and k / t < 0.5), key=lambda x: x[1] / max(1, x[2]))
    for wid, k, t in low_ret[:12]:
        print(f"  RETENTION WARN {wid}: kept {k:,}/{t:,} postings "
              f"({100 * k / t:.0f}%) -- generic-heavy work", flush=True)

    if args.index_audit:
        name_of = {c['work_id']: c['name'] for c in cohort}
        print("\n-- per-work retention (kept/total postings, "
              "worst first) --", flush=True)
        for wid, (k, t) in sorted(retention.items(),
                                  key=lambda kv: kv[1][0] / max(1, kv[1][1])):
            print(f"  {100 * k / max(1, t):5.1f}%  {k:>6,}/{t:<6,}  {wid}  "
                  f"{name_of.get(wid, '')[:50]}", flush=True)
        print("\nindex-audit only; exiting before corpus scan.", flush=True)
        return

    con = sqlite3.connect(DB)
    work_ids = [w['id'] for w in works]
    works_sig = hashlib.sha1(
        '|'.join(sorted(work_ids)).encode()).hexdigest()[:16]
    run_meta = {'db': os.path.basename(DB), 'tag': TAG,
                'work_df_cap': work_df_cap, 'limit': args.limit,
                'only': args.only, 'works_sig': works_sig}
    before = measure_pairing(con, work_ids)
    print(f"'before' pairing measured on target db "
          f"({time.time() - t0:.0f}s)", flush=True)

    rows = con.execute(
        "SELECT page_id, sys_id, text FROM pages ORDER BY rowid").fetchall()
    ids = [r[0] for r in rows]
    sys_ids = [r[1] for r in rows]
    streams = [norm_stream(r[2])[0] for r in rows]
    del rows
    print(f"pages: {len(ids):,} ({time.time() - t0:.0f}s)", flush=True)

    hits = defaultdict(list)   # (page_idx, work_idx) -> [(q0,q1,p0,p1,dens)]
    start_b0 = 0
    if con.execute("SELECT name FROM sqlite_master WHERE "
                   f"name='{CKPT_TBL}'").fetchone():
        row = con.execute(
            f"SELECT next_b0, hits_json, meta_json FROM {CKPT_TBL}").fetchone()
        if row:
            ck_meta = json.loads(row[2]) if row[2] else {}
            if ck_meta != run_meta:
                raise SystemExit(
                    f"REFUSING to resume: checkpoint {CKPT_TBL} was written by "
                    f"a different run.\n  checkpoint meta: {ck_meta}\n  "
                    f"current meta:    {run_meta}\nDrop the table to start "
                    f"fresh (DROP TABLE {CKPT_TBL};) or use a new tag.")
            pi_of = {p: i for i, p in enumerate(ids)}
            wi_of = {w['id']: i for i, w in enumerate(works)}
            for pid, wid, spans in json.loads(row[1]):
                if pid in pi_of and wid in wi_of:
                    hits[(pi_of[pid], wi_of[wid])] = [tuple(s) for s in spans]
            start_b0 = row[0]
            print(f"resume: batch {start_b0 // PAGE_BATCH + 1}, "
                  f"{len(hits):,} hits restored", flush=True)

    t1 = time.time()
    n_cand = 0
    for b0 in range(start_b0, len(ids), PAGE_BATCH):
        if _DEBUG_SLEEP:
            time.sleep(_DEBUG_SLEEP)
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
            q0 = r0 + int(seg_off[si])
            q1 = r1 + int(seg_off[si])
            hits[(pi, int(seg_work[si]))].append(
                (q0, q1, p0, p1, round(dens, 4)))
        if (b0 // PAGE_BATCH) % CKPT_EVERY == 0:
            print(f"  batch {b0 // PAGE_BATCH + 1}/"
                  f"{(len(ids) - 1) // PAGE_BATCH + 1}: cand={n_cand:,} "
                  f"hits={len(hits):,} ({time.time() - t1:.0f}s)",
                  flush=True)
            con.execute(f"CREATE TABLE IF NOT EXISTS {CKPT_TBL} "
                        "(next_b0 INT, hits_json TEXT, meta_json TEXT)")
            con.execute(f"DELETE FROM {CKPT_TBL}")
            con.execute(
                f"INSERT INTO {CKPT_TBL} VALUES (?,?,?)",
                (b0 + PAGE_BATCH, json.dumps(
                    [[ids[pi], works[wi]['id'], sp]
                     for (pi, wi), sp in hits.items()]),
                 json.dumps(run_meta)))
            con.commit()

    # ---- persist hits (one row per accepted span) ----
    out_rows = []
    for (pi, wi), spans in hits.items():
        wid, pid, sid = works[wi]['id'], ids[pi], sys_ids[pi]
        for q0, q1, p0, p1, dens in spans:
            out_rows.append((wid, pid, sid, q0, q1, p0, p1, p1 - p0, dens))
    con.execute(f"DROP TABLE IF EXISTS {HITS_TBL}")
    con.execute(f"""CREATE TABLE {HITS_TBL} (
        work_id TEXT, page_id TEXT, sys_id TEXT, q0 INT, q1 INT,
        p0 INT, p1 INT, matched_letters INT, density REAL)""")
    con.executemany(
        f"INSERT INTO {HITS_TBL} VALUES (?,?,?,?,?,?,?,?,?)", out_rows)
    con.execute(f"CREATE INDEX idx_wqh_{TAG}_work ON {HITS_TBL}(work_id)")
    con.execute(f"CREATE INDEX idx_wqh_{TAG}_page ON {HITS_TBL}(page_id)")
    con.execute(f"DROP TABLE IF EXISTS {CKPT_TBL}")
    con.commit()
    elapsed = time.time() - t0
    print(f"hit rows: {len(out_rows):,} ({elapsed:.0f}s total)", flush=True)

    # ---- metric (a): raw reference-hit coverage (NOT a pairing rate) ----
    hit_pages = defaultdict(set)
    hit_sys = defaultdict(set)
    for wid, pid, sid in con.execute(
            f"SELECT DISTINCT work_id, page_id, sys_id FROM {HITS_TBL}"):
        hit_pages[wid].add(pid)
        hit_sys[wid].add(sid)
    # ---- metric (b): df_damage-COMPATIBLE reference-mediated pairing ----
    ref_paired, _qpages = reference_mediated_pairing(con, HITS_TBL, MIN_LETTERS)
    print(f"reference-mediated pairing computed ({time.time() - t0:.0f}s)",
          flush=True)
    con.close()

    report_rows = []
    for c in cohort:
        wid = c['work_id']
        b = before.get(wid, {'pages': set(), 'paired': set(),
                              'ms_all': set()})
        pages_b = b['pages']                       # df_damage denominator
        hp = hit_pages.get(wid, set())
        # metric (a) coverage: known census pages the work-query re-hit
        coverage = (pages_b & hp)
        # metric (b) HONEST: census pages now in a valid reference-mediated
        # pair (>=200 aggregated letters both sides, distinct sys, dup-clean)
        paired_after = ref_paired.get(wid, set()) & pages_b
        rate_before_local = (len(b['paired']) / len(pages_b)
                              if pages_b else None)
        rate_after = (len(paired_after) / len(pages_b)
                      if pages_b else None)
        cov_rate = len(coverage) / len(pages_b) if pages_b else None
        added_ms = hit_sys.get(wid, set()) - b['ms_all']
        report_rows.append({
            'work_id': wid, 'name': c['name'], 'cat': c['cat'],
            'reasons': c['reasons'], 'pages': len(pages_b),
            'rate_before_full': c['rate'],
            'rate_before_local': rate_before_local,
            'rate_after': rate_after, 'cov_rate': cov_rate,
            'hit_pages': len(hp), 'added_ms': len(added_ms),
        })

    def pct(x):
        return f"{100 * x:.0f}%" if x is not None else "n/a"

    tot_pages = sum(r['pages'] for r in report_rows)
    tot_before = sum(len(before.get(r['work_id'], {}).get('paired', ()))
                      for r in report_rows)
    tot_after = sum(len(ref_paired.get(r['work_id'], set())
                        & before.get(r['work_id'], {}).get('pages', set()))
                    for r in report_rows)
    tot_added = sum(r['added_ms'] for r in report_rows)
    n_grew = sum(1 for r in report_rows if r['added_ms'] > 0)
    lines = [
        f"# A2 work-query second pass -- tag '{TAG}' ({os.path.basename(DB)})",
        "",
        f"- query works: {len(works):,} (fullcorpus.db df_damage cohort: "
        f"short-work <2,000L/>=10MS rate<{int(100*SHORT_RATE_MAX)}% UNION "
        f">={BIG_MIN_MS}-MS edited-cat rate<{int(100*BIG_RATE_MAX)}%)",
        f"- reference letters indexed: "
        f"{sum(len(w['stream']) for w in works):,}; ref-canon-masks applied "
        f"to {n_apply}/{len(works)} works; segments {len(seg_streams):,}, "
        f"postings {len(codes_f):,}; distinct-work DF cap={work_df_cap} "
        f"(dropped {df_dropped:,} generic gram-codes)",
        f"- runtime: {elapsed:.0f}s total, {elapsed / max(1, len(works)):.1f}"
        f"s/work avg ({len(ids):,} target-db pages scanned)",
        f"- hit rows: {len(out_rows):,}; works with >=1 hit: "
        f"{len(hit_pages):,}/{len(works):,}",
        f"- cohort pages in target-db scope: {tot_pages:,}",
        f"- **metric (b) df_damage-compatible pairing** -- before (Track-2 "
        f"same-work accepted pairs): {tot_before:,} "
        f"({100 * tot_before / max(1, tot_pages):.0f}%); after "
        f"(reference-mediated, >=200L both sides, distinct sys, dup-clean): "
        f"{tot_after:,} ({100 * tot_after / max(1, tot_pages):.0f}%)",
        f"- added (MS,work) memberships beyond the live track1_matches "
        f"census: {tot_added:,} (across {n_grew}/{len(works)} works)",
        "",
        "Metric (a) = raw reference-hit coverage (share of census pages the "
        "work-query re-hit at all); metric (b) = the honest pairing rate "
        "directly comparable to df_damage.py's before-column.",
        "",
        "## Per-work before/after (sorted by rate-after (b), worst first)",
        "| work | reasons | pages(target-db) | rate-before(full-census) | "
        "rate-before(local) | rate-after(b) | coverage(a) | hit-pages | "
        "added-MS |",
        "|---|---|---|---|---|---|---|---|---|",
    ]
    for r in sorted(report_rows,
                     key=lambda r: (r['rate_after'] if r['rate_after']
                                    is not None else -1)):
        lines.append(
            f"| [{r['cat']}] {r['name'][:45]} | {','.join(r['reasons'])} | "
            f"{r['pages']} | {pct(r['rate_before_full'])} | "
            f"{pct(r['rate_before_local'])} | {pct(r['rate_after'])} | "
            f"{pct(r['cov_rate'])} | {r['hit_pages']} | {r['added_ms']} |")
    open(OUT, 'w', encoding='utf-8').write('\n'.join(lines))
    print('\n'.join(lines[:18]))
    print(f"wrote {OUT} ({elapsed:.0f}s total)")


if __name__ == '__main__':
    main()
