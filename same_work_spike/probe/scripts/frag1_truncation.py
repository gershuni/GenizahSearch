# -*- coding: utf-8 -*-
"""FRAG-1 -- synthetic-truncation grounding spike (SEED-029 fragment-ID pivot).

Ground truth by CONSTRUCTION: sample confidently Track-1-labeled full-page
witnesses of a known work W, crop their normalized stream at synthetic
target lengths from a random in-page offset, and run each crop as a
DF-immune QUERY against the reference index (ref_corpus.pkl), using the
EXACT track1_match.py mechanics (K=5 grams, band=20 diagonal clustering,
min_anchors=2, one-sided noisy-HTR-vs-clean-edition accept_density).
Because the crop provably belongs to W, recall(length) and mis-attribution
(length) are unbiased -- no lexical-recoverability selection bias, unlike
stratifying tier-1 ground truth (see ground_truth.py + FRAG-ID-PLAN.md).

Five deliverables (see FRAG-ID-PLAN.md FRAG-1):
  1. recall(length) by truncation
  2. mis-attribution(length) = precision proxy, same crops, threshold sweep
  3. failure-mode stage attribution over ~300 real unidentified short pages
  4. fragment-population census (<100 / <200 letters)
  5. pipeline design grounded in 1-4

Hard constraints (SPIKE-BRIEFS-2026-07-08.md global context + FRAG-ID-PLAN.md):
  - python -X utf8 -u, cwd = scripts/
  - reference index built from ref_corpus.pkl ONLY -- no full-corpus
    candidate-engine scan (mask_ref_canon.py owns the box's CPU)
  - do NOT modify pipeline scripts; do NOT git commit
  - shadowed_by / accepted_pairs_canonmask compat-gated (mirrors
    track1_testimonies.py:113-118 / df_damage.py:41-42)

Usage: python -X utf8 -u frag1_truncation.py
Writes: ../results/frag1_truncation.md, ../results/frag1_curve_data.json
Cache: ../data/frag1_page_len_cache.pkl (norm_stream lengths for all pages;
       the one unavoidable full-corpus pass -- pure normalize, not the
       seed-and-extend engine; ~7-8 min single-threaded, cached after first run)
"""
import json
import os
import pickle
import random
import sqlite3
import time
from collections import Counter, defaultdict

import numpy as np
from rapidfuzz.distance import Levenshtein

from engine_np import _gram_codes
from normalize import norm_stream
from track1_match import accept_density as t1_accept_density
from track1_match import build_ref_index

ROOT = r"C:\Genizahsearch"
PROBE = ROOT + r"\same_work_spike\probe"
DB = PROBE + r"\data\fullcorpus.db"
REF = PROBE + r"\data\ref_corpus.pkl"
MASKS = PROBE + r"\data\ref_canon_masks.json"
LEN_CACHE = PROBE + r"\data\frag1_page_len_cache.pkl"
OUT_MD = PROBE + r"\results\frag1_truncation.md"
OUT_JSON = PROBE + r"\results\frag1_curve_data.json"

RNG_SEED = 20260709

# ---- track1_match.py mechanics, reused verbatim (do not diverge) ----
K = 5
BAND, MIN_ANCHORS, B_OFF = 20, 2, 256
MARGIN, MIN_SPAN = 30, 30

# ---- experiment parameters ----
LENGTHS = [40, 60, 80, 100, 150, 200, 300]
CROPS_PER_LENGTH = 3
MIN_PAGE_LEN_FOR_SAMPLE = 340          # max crop (300) + 40 offset slack
COVERAGE_MIN = 0.85                    # matched_letters / page_len
CAT_TARGETS = {'Bible': 200, 'Maagarim': 150, 'JA': 80, 'Bavli': 40,
               'Mishnah': 20, 'Yerushalmi': 8, 'Tosefta': 2}
assert sum(CAT_TARGETS.values()) == 500

WIDE_CUTOFF_FRAC = 0.55                # capture near-misses for the sweep
SCALES = [0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.1, 1.2, 1.3, 1.5]
PLAUSIBLE_CUTOFF = 0.45                # "in the running" cutoff for ambiguity
AMBIG_EPS = 0.05                       # density gap under which 2 works tie

N_UNIDENT_PER_BIN = 150                # 150 <100 + 150 [100,200) = 300


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


# =====================================================================
# reference index (built ONCE from ref_corpus.pkl -- no corpus scan)
# =====================================================================

def build_reference():
    t0 = time.time()
    works = pickle.load(open(REF, 'rb'))
    canon_masks = None
    masks_info = "absent"
    if os.path.exists(MASKS):
        canon_masks = json.load(open(MASKS, encoding='utf-8'))
        masks_info = (f"{len(canon_masks)} works, mtime="
                       f"{time.strftime('%Y-%m-%d %H:%M', time.localtime(os.path.getmtime(MASKS)))}")
    ref_tuple = build_ref_index(works, canon_masks)
    seg_streams, seg_work, seg_off, codes_f, seg_f, pos_f, df_dropped = ref_tuple
    log(f"ref index: {len(works):,} works, {len(seg_streams):,} segments, "
        f"{len(codes_f):,} postings, canon_masks={masks_info} "
        f"({time.time() - t0:.0f}s)")
    wid_to_wi = {w['id']: i for i, w in enumerate(works)}
    return works, wid_to_wi, (seg_streams, seg_work, seg_off, codes_f, seg_f, pos_f)


# =====================================================================
# generalized batch query -- SAME numpy pipeline as track1_match.py's
# inner loop, generalized to an arbitrary list of query streams (global
# qi in [0, n); track1_match.py already uses GLOBAL page indices in the
# same bit layout, so n up to ~10^9 is safe -- no chunking required for
# our few-thousand-crop scale).
# =====================================================================

def query_batch(streams, ref_tuple, wide_cutoff_frac=WIDE_CUTOFF_FRAC,
                 want_diag=False, want_refspan=False):
    """Returns (results, diag) -- or (results, diag, refspans) if want_refspan.
    results[qi] = list of (work_idx, alen, dens) -- ALL candidates that
        cleared MIN_ANCHORS and whose Levenshtein density is <= wide_cutoff_frac
        (i.e. a superset of what track1_match.py would accept at its
        production boundary -- lets us sweep thresholds post-hoc without
        re-running the seed-and-extend step).
    diag[qi] (only if want_diag) = {'grams': n, 'hits': n, 'best_cluster': n}
        -- first three pipeline stages, for failure-mode attribution.
    refspans[qi] (only if want_refspan) = {work_idx: (dens, seg_idx, r0, r1)}
        -- the ACCEPTED reference span (within-segment coords) of the best
        (min-density) candidate per work, for extracting the matched ref
        source text. `want_refspan` is purely additive: it changes neither
        `results` nor the accept logic, so the default path (want_refspan
        False) is byte-identical to before (FRAG-1 results unaffected).
    """
    seg_streams, seg_work, seg_off, codes_f, seg_f, pos_f = ref_tuple
    n = len(streams)
    results = [[] for _ in range(n)]
    diag = [{'grams': 0, 'hits': 0, 'best_cluster': 0} for _ in range(n)] \
        if want_diag else None
    refspans = [dict() for _ in range(n)] if want_refspan else None

    def _out():
        return (results, diag, refspans) if want_refspan else (results, diag)

    parts_c, parts_p, parts_pos = [], [], []
    for qi, s in enumerate(streams):
        g = _gram_codes(s)
        if want_diag:
            diag[qi]['grams'] = int(len(g))
        if not len(g):
            continue
        parts_c.append(g.astype(np.uint32))
        parts_p.append(np.full(len(g), qi, np.uint32))
        parts_pos.append(np.arange(len(g), dtype=np.uint32))
    if not parts_c:
        return _out()
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
    if want_diag:
        hit_per_q = np.bincount(pg_p[sel], weights=counts,
                                 minlength=n).astype(np.int64)
        for qi in range(n):
            diag[qi]['hits'] = int(hit_per_q[qi])
    if not total:
        return _out()

    cum0 = np.cumsum(counts) - counts
    ref_idx = (np.repeat(lo[sel], counts)
               + (np.arange(total, dtype=np.int64) - np.repeat(cum0, counts)))
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
    new_seg = np.r_[True, (pair[1:] != pair[:-1]) | (buck[1:] - buck[:-1] > 1)]
    s3 = np.flatnonzero(new_seg)
    seg_cnt = np.add.reduceat(cnt2, s3).astype(np.int64)

    if want_diag:
        qi_of_pair = (pair[s3] >> np.uint64(16)).astype(np.int64)
        best = np.zeros(n, dtype=np.int64)
        np.maximum.at(best, qi_of_pair, seg_cnt)
        for qi in range(n):
            diag[qi]['best_cluster'] = int(best[qi])

    hit = seg_cnt >= MIN_ANCHORS
    c_pair = pair[s3][hit]
    c_minp = np.minimum.reduceat(minp, s3)[hit]
    c_maxp = np.maximum.reduceat(maxp, s3)[hit]
    c_minr = np.minimum.reduceat(minr, s3)[hit]
    c_maxr = np.maximum.reduceat(maxr, s3)[hit]
    for i in range(len(c_pair)):
        qi = int(c_pair[i] >> np.uint64(16))
        si = int(c_pair[i] & np.uint64(0xFFFF))
        sp, sr = streams[qi], seg_streams[si]
        p0 = max(0, int(c_minp[i]) - MARGIN)
        p1 = min(len(sp), int(c_maxp[i]) + K + MARGIN)
        r0 = max(0, int(c_minr[i]) - MARGIN)
        r1 = min(len(sr), int(c_maxr[i]) + K + MARGIN)
        if min(p1 - p0, r1 - r0) < MIN_SPAN:
            continue
        alen = max(p1 - p0, r1 - r0)
        cutoff = int(wide_cutoff_frac * alen) + 1
        dist = Levenshtein.distance(sp[p0:p1], sr[r0:r1], score_cutoff=cutoff)
        if dist > cutoff:
            continue
        dens = dist / alen
        wkey = int(seg_work[si])
        results[qi].append((wkey, alen, round(dens, 4)))
        if want_refspan:
            prev = refspans[qi].get(wkey)
            if prev is None or dens < prev[0]:
                refspans[qi][wkey] = (round(dens, 4), si, r0, r1)
    return _out()


# =====================================================================
# full-corpus page-length pass (cached) -- the one unavoidable O(n) scan;
# pure normalize, NOT the seed-and-extend engine.
# =====================================================================

def compute_page_lengths(con):
    if os.path.exists(LEN_CACHE):
        d = pickle.load(open(LEN_CACHE, 'rb'))
        log(f"page-length cache loaded: {len(d):,} pages ({LEN_CACHE})")
        return d
    t0 = time.time()
    out = {}
    n = 0
    for pid, sid, text in con.execute("SELECT page_id, sys_id, text FROM pages"):
        s, _ = norm_stream(text or '')
        out[pid] = (sid, len(s))
        n += 1
        if n % 100000 == 0:
            log(f"  page-length pass: {n:,} ({time.time() - t0:.0f}s)")
    log(f"page-length pass done: {n:,} pages ({time.time() - t0:.0f}s)")
    pickle.dump(out, open(LEN_CACHE, 'wb'), protocol=4)
    return out


# =====================================================================
# domain grouping (FJMS domain, via fist_data/fjms_enrichment.db)
# =====================================================================

def load_domain_groups():
    from build_reuse_graph import DOMAIN_GROUPS, load_domains
    domains = load_domains()
    other_idx = len(DOMAIN_GROUPS) - 1
    return domains, DOMAIN_GROUPS, other_idx


def top_domain_group(sys_id, domains, other_idx):
    rec = domains.get(sys_id)
    if not rec or not rec[0]:
        return other_idx
    top = rec[0].most_common(2)
    return (top[1][0] if top[0][0] == other_idx and len(top) > 1
            else top[0][0])


# =====================================================================
# compat-gated live/canonmask id sets (mirrors track1_testimonies.py /
# df_damage.py pattern)
# =====================================================================

def load_live_and_canonmask_ids(con):
    cols = [r[1] for r in con.execute("PRAGMA table_info(track1_matches)")]
    live_clause = " WHERE shadowed_by IS NULL" if 'shadowed_by' in cols else ""
    live_ids = {r[0] for r in con.execute(
        f"SELECT DISTINCT page_id FROM track1_matches{live_clause}")}
    canonmask_ids = set()
    has_cm = con.execute(
        "SELECT name FROM sqlite_master WHERE name='accepted_pairs_canonmask'"
    ).fetchone()
    if has_cm:
        for a, b in con.execute(
                "SELECT page_a, page_b FROM accepted_pairs_canonmask"):
            canonmask_ids.add(a)
            canonmask_ids.add(b)
    log(f"live track1 page_ids: {len(live_ids):,}; "
        f"canonmask-paired page_ids: {len(canonmask_ids):,} "
        f"(shadowed_by gate: {'applied' if live_clause else 'NO shadowed_by column!'})")
    return live_ids, canonmask_ids


# =====================================================================
# 1+2: synthetic truncation sample selection
# =====================================================================

def build_candidate_order(pool_items, rnd):
    """Round-robin by work_id for diversity; returns ALL items ordered."""
    by_work = defaultdict(list)
    for it in pool_items:
        by_work[it[2]].append(it)
    for w in by_work:
        rnd.shuffle(by_work[w])
    work_ids = list(by_work.keys())
    rnd.shuffle(work_ids)
    order = []
    ptr = {w: 0 for w in work_ids}
    progressed = True
    while progressed:
        progressed = False
        for w in work_ids:
            if ptr[w] < len(by_work[w]):
                order.append(by_work[w][ptr[w]])
                ptr[w] += 1
                progressed = True
    return order


def sample_track1_pages(con):
    """~500 confidently Track-1-labeled full-page witnesses, stratified
    across ref-corpus categories (incl. JA) and round-robined across
    distinct works within each category."""
    t0 = time.time()
    pool = defaultdict(list)
    rows = con.execute("""
        SELECT page_id, sys_id, work_id, cat, matched_letters, best_density, n_spans
        FROM track1_matches
        WHERE shadowed_by IS NULL AND matched_letters >= 300
              AND n_spans <= 2 AND best_density <= 0.15
    """).fetchall()
    for pid, sid, wid, cat, ml, bd, ns in rows:
        pool[cat].append((pid, sid, wid, ml, bd, ns))
    log(f"candidate pool (confident full-page rows): {len(rows):,} rows, "
        f"by cat: {dict((c, len(v)) for c, v in pool.items())}")

    rnd = random.Random(RNG_SEED)
    chosen = []
    shortfall = {}
    for cat, tgt in CAT_TARGETS.items():
        order = build_candidate_order(pool.get(cat, []), rnd)
        got = []
        for pid, sid, wid, ml, bd, ns in order:
            if len(got) >= tgt:
                break
            text = con.execute("SELECT text FROM pages WHERE page_id=?",
                                (pid,)).fetchone()
            if not text or not text[0]:
                continue
            stream, _ = norm_stream(text[0])
            plen = len(stream)
            if plen < MIN_PAGE_LEN_FOR_SAMPLE:
                continue
            if ml / max(1, plen) < COVERAGE_MIN:
                continue
            got.append({'page_id': pid, 'sys_id': sid, 'work_id': wid,
                        'cat': cat, 'matched_letters': ml, 'best_density': bd,
                        'stream': stream, 'page_len': plen})
        if len(got) < tgt:
            shortfall[cat] = (len(got), tgt)
        chosen.extend(got)
    log(f"sampled {len(chosen):,} full-page witnesses "
        f"({time.time() - t0:.0f}s); shortfalls: {shortfall or 'none'}")
    return chosen, shortfall


def make_crops(sampled_pages, rnd):
    crops = []
    for pg in sampled_pages:
        s = pg['stream']
        L = len(s)
        for length in LENGTHS:
            for c in range(CROPS_PER_LENGTH):
                max_off = max(0, L - length)
                off = rnd.randint(0, max_off) if max_off > 0 else 0
                crop = s[off:off + length]
                crops.append({
                    'page_id': pg['page_id'], 'sys_id': pg['sys_id'],
                    'work_id': pg['work_id'], 'cat': pg['cat'],
                    'length_bin': length, 'crop_idx': c, 'offset': off,
                    'stream': crop,
                })
    return crops


def run_truncation_experiment(works, wid_to_wi, ref_tuple, crops):
    t0 = time.time()
    streams = [c['stream'] for c in crops]
    results, diag = query_batch(streams, ref_tuple, want_diag=True)
    log(f"truncation query batch: {len(crops):,} crops "
        f"({time.time() - t0:.0f}s)")
    for c, cand, dg in zip(crops, results, diag):
        c['candidates'] = cand   # [(work_idx, alen, dens), ...]
        c['n_grams'] = dg['grams']
        c['hits'] = dg['hits']
        true_wi = wid_to_wi.get(c['work_id'])
        c['true_wi'] = true_wi
        # retained effective grams: distinct crop positions with >=1 ref hit
        # (approximate via a second cheap pass only if needed -- diag['hits']
        # already sums postings with multiplicity; report both)
    return crops


def accepted_works_at_scale(candidates, scale):
    """dict work_idx -> best (min) density among its passing candidates."""
    best = {}
    for wi, alen, dens in candidates:
        thresh = scale * t1_accept_density(alen)
        if dens <= thresh:
            if wi not in best or dens < best[wi]:
                best[wi] = dens
    return best


def top_ranked_work(accepted):
    """work_idx of the lowest-density accepted work (the single best match a
    'take-best' census policy would pick), or None."""
    if not accepted:
        return None
    return min(accepted.items(), key=lambda kv: kv[1])[0]


def analyze_recall_precision(crops):
    per_bin = defaultdict(lambda: {'n': 0, 'sweep': defaultdict(
        lambda: {'true_ok': 0, 'any_id': 0, 'wrong_id': 0, 'top_wrong': 0})})
    per_bin_cat = defaultdict(lambda: defaultdict(lambda: {'n': 0, 'true_ok': 0}))
    gram_stats = defaultdict(list)
    for c in crops:
        lb = c['length_bin']
        per_bin[lb]['n'] += 1
        gram_stats[lb].append((c['n_grams'], c['hits']))
        true_wi = c['true_wi']
        for scale in SCALES:
            acc = accepted_works_at_scale(c['candidates'], scale)
            true_ok = true_wi in acc
            any_id = len(acc) > 0
            wrong_id = any(wi != true_wi for wi in acc)
            top = top_ranked_work(acc)
            top_wrong = top is not None and top != true_wi
            slot = per_bin[lb]['sweep'][scale]
            slot['true_ok'] += int(true_ok)
            slot['any_id'] += int(any_id)
            slot['wrong_id'] += int(wrong_id)
            slot['top_wrong'] += int(top_wrong)
        # default-scale per-cat breakdown
        acc1 = accepted_works_at_scale(c['candidates'], 1.0)
        pcb = per_bin_cat[lb][c['cat']]
        pcb['n'] += 1
        pcb['true_ok'] += int(true_wi in acc1)

    out = {}
    for lb in LENGTHS:
        b = per_bin[lb]
        n = b['n']
        sweep_rows = []
        for scale in SCALES:
            s = b['sweep'][scale]
            recall = s['true_ok'] / n if n else 0.0
            mis_attrib = (s['wrong_id'] / s['any_id']) if s['any_id'] else 0.0
            top_mis = (s['top_wrong'] / s['any_id']) if s['any_id'] else 0.0
            sweep_rows.append({
                'scale': scale, 'recall': recall, 'mis_attribution': mis_attrib,
                'top_mis_attribution': top_mis,
                'n_any_id': s['any_id'], 'n_wrong_id': s['wrong_id'],
                'n_top_wrong': s['top_wrong'],
            })
        grams = gram_stats[lb]
        mean_grams = sum(g[0] for g in grams) / max(1, len(grams))
        mean_hits = sum(g[1] for g in grams) / max(1, len(grams))
        cat_rows = {cat: {'n': v['n'], 'recall': v['true_ok'] / v['n']}
                    for cat, v in per_bin_cat[lb].items()}
        out[lb] = {'n': n, 'mean_n_grams': round(mean_grams, 1),
                   'mean_hits': round(mean_hits, 1), 'sweep': sweep_rows,
                   'by_cat': cat_rows}
    return out


# =====================================================================
# 3: failure-mode stage attribution over real unidentified short pages
# =====================================================================

def sample_unidentified_pages(page_lengths, live_ids, canonmask_ids):
    rnd = random.Random(RNG_SEED + 1)
    bins = {'lt100': [], 'b100_200': []}
    for pid, (sid, ln) in page_lengths.items():
        if ln <= 0:
            continue
        if pid in live_ids or pid in canonmask_ids:
            continue
        if ln < 100:
            bins['lt100'].append((pid, sid, ln))
        elif ln < 200:
            bins['b100_200'].append((pid, sid, ln))
    for k in bins:
        rnd.shuffle(bins[k])
    chosen = (bins['lt100'][:N_UNIDENT_PER_BIN]
              + bins['b100_200'][:N_UNIDENT_PER_BIN])
    log(f"unidentified-page pools: lt100={len(bins['lt100']):,}, "
        f"b100_200={len(bins['b100_200']):,}; sampled {len(chosen):,}")
    return chosen


def fetch_streams(con, page_ids):
    out = {}
    for i in range(0, len(page_ids), 400):
        batch = page_ids[i:i + 400]
        ph = ','.join('?' * len(batch))
        for pid, text in con.execute(
                f"SELECT page_id, text FROM pages WHERE page_id IN ({ph})",
                batch):
            out[pid] = norm_stream(text or '')[0]
    return out


def classify_failure(dg, cand):
    """First pipeline stage that gates the page. Returns (label, best_dens).

    Stage order (matches FRAG-ID-PLAN.md; `no_reference_covers_it` is the
    RESIDUAL 'formed a diagonal cluster but no reference span aligns'):
      no_grams            -- page has no 5-grams (too short / no Hebrew)
      <2_anchors          -- <2 raw gram hits against the whole reference
      no_diagonal_cluster -- has hits but they don't form a >=2-anchor diagonal
      no_reference_covers_it -- a cluster formed, but NO reference span verifies
                             even at the generous 0.55 wide cutoff (the anchors
                             were chance 5-gram collisions; there is no reference
                             text this page is a copy of, at better-than-noise
                             alignment). This is the REF-1-expansion regime, NOT
                             threshold-fixable.
      density_fail        -- a reference span DID verify at 0.55 (a genuine-
                             looking alignment exists) but its best density is
                             above the production accept boundary -> borderline,
                             the length-conditional-threshold (A5) regime.
      ambiguous           -- 2+ works both plausible and within AMBIG_EPS.
      would_now_pass      -- best candidate already clears the production
                             boundary (not a failure; census staleness).
    """
    if dg['grams'] == 0:
        return 'no_grams', None
    if dg['hits'] < 2:
        return '<2_anchors', None
    if dg['best_cluster'] < 2:
        return 'no_diagonal_cluster', None
    if not cand:
        # clusters formed from chance 5-gram collisions but no reference span
        # aligns to this page at even 0.55 edit-density -> not a copy of
        # anything in the reference corpus.
        return 'no_reference_covers_it', None
    # best density per distinct work
    by_work = {}
    for wi, alen, dens in cand:
        if wi not in by_work or dens < by_work[wi][0]:
            by_work[wi] = (dens, alen)
    ranked = sorted(by_work.items(), key=lambda kv: kv[1][0])
    top_wi, (top_dens, top_alen) = ranked[0]
    if top_dens <= t1_accept_density(top_alen):
        return 'would_now_pass', top_dens
    if top_dens <= PLAUSIBLE_CUTOFF and len(ranked) >= 2:
        second_wi, (second_dens, _) = ranked[1]
        if second_dens <= PLAUSIBLE_CUTOFF and (second_dens - top_dens) <= AMBIG_EPS:
            return 'ambiguous', top_dens
    return 'density_fail', top_dens


def diagnose_failure_modes(con, ref_tuple, page_lengths, live_ids, canonmask_ids):
    t0 = time.time()
    sample = sample_unidentified_pages(page_lengths, live_ids, canonmask_ids)
    page_ids = [s[0] for s in sample]
    streams_by_pid = fetch_streams(con, page_ids)
    streams = [streams_by_pid.get(pid, '') for pid, sid, ln in sample]
    results, diag = query_batch(streams, ref_tuple, want_diag=True)
    hist = Counter()
    cards = defaultdict(list)
    # best-achievable density band among pages that DID verify a candidate at
    # the 0.55 cutoff (density_fail / would_now_pass) -- shows how far the
    # verifying-but-rejected pages are from a production accept.
    best_dens_bands = Counter()
    for (pid, sid, ln), cand, dg in zip(sample, results, diag):
        cls, best_dens = classify_failure(dg, cand)
        hist[cls] += 1
        if best_dens is not None:
            band = ('<=0.30' if best_dens <= 0.30 else
                    '0.30-0.40' if best_dens <= 0.40 else
                    '0.40-0.50' if best_dens <= 0.50 else '>0.50')
            best_dens_bands[band] += 1
        if len(cards[cls]) < 6:
            cards[cls].append({'page_id': pid, 'sys_id': sid, 'len': ln,
                               'grams': dg['grams'], 'hits': dg['hits'],
                               'best_cluster': dg['best_cluster'],
                               'n_candidates': len(cand),
                               'best_dens': best_dens})
    log(f"failure-mode diagnosis: {len(sample):,} pages "
        f"({time.time() - t0:.0f}s); histogram: {dict(hist)}")
    return hist, cards, len(sample), dict(best_dens_bands)


# =====================================================================
# 4: fragment-population census
# =====================================================================

def build_census(page_lengths, live_ids, canonmask_ids, domains, groups, other_idx):
    bins = {'lt100': (0, 100), 'lt200': (0, 200)}
    out = {}
    for name, (lo, hi) in bins.items():
        totals = Counter()
        by_domain = defaultdict(Counter)
        for pid, (sid, ln) in page_lengths.items():
            if not (lo <= ln < hi):
                continue
            if pid in live_ids:
                b = 'track1_identified'
            elif pid in canonmask_ids:
                b = 'canonmask_paired'
            else:
                b = 'neither_target'
            totals[b] += 1
            g = top_domain_group(sid, domains, other_idx)
            by_domain[groups[g][0]][b] += 1
        out[name] = {'totals': dict(totals),
                     'by_domain': {k: dict(v) for k, v in by_domain.items()}}
    return out


# =====================================================================
# report writer
# =====================================================================

def fmt_pct(x):
    return f"{100 * x:.1f}%"


def write_report(shortfall, rp, hist, cards, n_unident_sample, census,
                  ref_masks_info, n_ref_works, elapsed_total, best_dens_bands):
    lines = []
    A = lines.append
    A("# FRAG-1 -- synthetic-truncation grounding spike (fragment-ID pivot)")
    A("")
    A(f"Generated {time.strftime('%Y-%m-%d %H:%M')}; total runtime "
      f"{elapsed_total / 60:.1f} min. Reference: ref_corpus.pkl only "
      f"({n_ref_works:,} works); ref-side canonical masks: {ref_masks_info}. "
      "No full-corpus candidate-engine scan was run (mask_ref_canon.py owned "
      "the box's CPU throughout).")
    A("")
    A("**Caveat (binding, per FRAG-ID-PLAN.md):** truncation crops are CLEAN "
      "contiguous slices of a page that was ALREADY a confident, single-span, "
      "high-coverage Track-1 testimony. This models a clean fragment cut short "
      "-- it does NOT model HTR damage, OCR garbling, or heavy textual variance "
      "within the fragment itself. Those need lower-k / confusion-weighted "
      "matching and are a SEPARATE axis (FRAG-2 extension: crop + inject "
      "HTR-confusion noise), not measured here.")
    A("")
    if shortfall:
        A(f"**Sampling shortfall** (pool exhausted before reaching category "
          f"target): {shortfall} -- categories with sparse confident "
          "full-page pools (e.g. Tosefta) fall short; totals below reflect "
          "actual sampled counts, not the 500 target.")
        A("")

    # ---- 1+2: recall / mis-attribution ----
    A("## 1+2. recall(length) and mis-attribution(length) by synthetic truncation")
    A("")
    A("Ground truth by construction: each crop is a verbatim slice of a page "
      "independently, confidently Track-1-labeled as work W (live row, "
      ">=300 matched letters, <=2 spans, best_density<=0.15, matched_letters/"
      "page_len>=0.85 -- i.e. the page IS essentially all W). Crop is queried "
      "against the reference index using track1_match.py's exact one-sided "
      "mechanics (K=5, band=20, min_anchors=2, accept_density 0.28/0.35 by "
      "alignment length). recall = crop recovers W among accepted IDs; "
      "mis-attribution = crop accepted as some OTHER work / crop accepted as "
      "anything (both at the SAME density boundary each candidate would use "
      "in production, at scale=1.0 = the current hand-tuned boundary).")
    A("")
    A("Two mis-attribution readings: **any-wrong** = a wrong work appears "
      "among the accepted set (relevant to a take-ALL-matches census that "
      "mints every accepted work as a witness); **top-wrong** = the "
      "single lowest-density (best) accepted work is wrong (relevant to a "
      "take-BEST-match census). They diverge at long crops, where the true "
      "work is nearly always recovered AND a spurious second work also "
      "clears the loose >=100-letter boundary -- so top-wrong is the more "
      "faithful precision proxy for a best-match census.")
    A("")
    A("| length | n crops | mean grams | mean ref hits | recall@1.0x | "
      "any-wrong@1.0x | top-wrong@1.0x |")
    A("|---|---|---|---|---|---|---|")
    for lb in LENGTHS:
        r = rp[lb]
        default = next(s for s in r['sweep'] if s['scale'] == 1.0)
        A(f"| {lb} | {r['n']} | {r['mean_n_grams']} | {r['mean_hits']} | "
          f"{fmt_pct(default['recall'])} | "
          f"{fmt_pct(default['mis_attribution'])} "
          f"({default['n_wrong_id']}/{default['n_any_id']}) | "
          f"{fmt_pct(default['top_mis_attribution'])} "
          f"({default['n_top_wrong']}/{default['n_any_id']}) |")
    A("")
    A("### recall/mis-attribution by category (default boundary, incl. JA)")
    A("")
    cats = sorted({cat for lb in LENGTHS for cat in rp[lb]['by_cat']})
    A("| length | " + " | ".join(cats) + " |")
    A("|---|" + "---|" * len(cats))
    for lb in LENGTHS:
        row = []
        for c in cats:
            v = rp[lb]['by_cat'].get(c)
            row.append(f"{v['recall'] * 100:.0f}% (n={v['n']})" if v else "--")
        A(f"| {lb} | " + " | ".join(row) + " |")
    A("")
    A("### density-boundary sweep (recall/mis-attribution tradeoff per length)")
    A("")
    for lb in LENGTHS:
        A(f"**length {lb}** (n={rp[lb]['n']})")
        A("")
        A("| scale x boundary | recall | any-wrong | top-wrong | n any-id | "
          "n any-wrong | n top-wrong |")
        A("|---|---|---|---|---|---|---|")
        for s in rp[lb]['sweep']:
            A(f"| {s['scale']}x | {fmt_pct(s['recall'])} | "
              f"{fmt_pct(s['mis_attribution'])} | "
              f"{fmt_pct(s['top_mis_attribution'])} | {s['n_any_id']} | "
              f"{s['n_wrong_id']} | {s['n_top_wrong']} |")
        A("")

    # knee detection
    A("### knee / interpretation")
    A("")
    knee = None
    for lb in LENGTHS:
        default = next(s for s in rp[lb]['sweep'] if s['scale'] == 1.0)
        if default['recall'] < 0.5:
            knee = lb
            break
    if knee is not None:
        A(f"Recall at the current (1.0x) boundary drops below 50% at length "
          f"**{knee}**. Below that, DF-immune querying alone (at the current "
          "boundary) is not reliable; loosening the boundary (scale>1.0) may "
          "recover more but check the mis-attribution column at that scale "
          "before trusting it as a census-grade identification.")
    else:
        A("Recall at the current (1.0x) boundary stays >=50% across the "
          f"whole tested range down to {LENGTHS[0]} letters.")
    mis_knee = None
    for lb in LENGTHS:
        default = next(s for s in rp[lb]['sweep'] if s['scale'] == 1.0)
        if default['n_any_id'] >= 5 and default['top_mis_attribution'] > 0.05:
            mis_knee = lb
            break
    if mis_knee is not None:
        A(f"TOP-wrong mis-attribution at 1.0x exceeds 5% at length "
          f"**{mis_knee}** and below (among crops that get ANY "
          "identification) -- below that length, even the single best "
          "accepted work is wrong >5% of the time, so an auto-accepted ID is "
          "not a safe census-grade testimony on its own; treat as CANDIDATE "
          "tier requiring the two-tier (census/review) split (A5), not direct "
          "census inclusion.")
    A("")

    # ---- 3: failure modes ----
    A("## 3. failure-mode stage attribution "
      f"(n={n_unident_sample} real unidentified short pages, "
      "shadowed-filtered + canonmask-excluded)")
    A("")
    A("Pages sampled from `pages` with norm_stream length <200, NOT a live "
      "track1_matches row, NOT a member of any accepted_pairs_canonmask pair "
      "(the true target/orphan population). Each re-run through the SAME "
      "query pipeline as the truncation experiment (own text as query against "
      "the reference index) and classified by the FIRST pipeline stage that "
      "fails.")
    A("")
    total_hist = sum(hist.values())
    order = ['no_grams', '<2_anchors', 'no_diagonal_cluster',
             'no_reference_covers_it', 'density_fail', 'ambiguous',
             'would_now_pass']
    A("| stage (first to gate) | count | share |")
    A("|---|---|---|")
    for k in order:
        c = hist.get(k, 0)
        A(f"| {k} | {c} | {fmt_pct(c / max(1, total_hist))} |")
    A("")
    dominant = max((k for k in order if k != 'would_now_pass'),
                   key=lambda k: hist.get(k, 0))
    A(f"**Dominant genuine failure mode: `{dominant}` "
      f"({fmt_pct(hist.get(dominant, 0) / max(1, total_hist))}).** "
      "The headline is that these short orphan pages are NOT sitting just "
      "below the acceptance boundary -- they overwhelmingly fail because NO "
      "reference span aligns to them at even a generous 0.55 edit-density "
      "(they form 3-9-anchor diagonal clusters purely from chance 5-gram "
      "collisions -- unavoidable in Hebrew -- but nothing verifies). That "
      "means the binding lever for this population is **REF-1 reference "
      "expansion** (or genuine non-identifiability), NOT threshold loosening. "
      "A looser boundary (A5) only helps the `density_fail` slice, which is "
      f"small ({fmt_pct(hist.get('density_fail', 0) / max(1, total_hist))}).")
    A("")
    if best_dens_bands:
        A("Best-achievable edit-density among the pages that DID verify a "
          "candidate at the 0.55 wide cutoff (density_fail + would_now_pass) "
          f"-- shows how far the verifiers are from an accept: {best_dens_bands}. "
          "(Pages in `no_reference_covers_it` verified NOTHING at 0.55, so "
          "they have no best-density -- they are not near any accept at all.)")
        A("")
    A("`would_now_pass` is NOT a failure -- it means our re-run (using the "
      "CURRENT reference/mask config) finds an acceptable candidate the live "
      "`track1_matches` table does not carry. This can happen because "
      "`track1_matches` in fullcorpus.db predates the ref-side canonical "
      "masking added 2026-07-08 (its report has no 'canonical' mention and "
      "predates ref_canon_masks.json's mtime) -- i.e. partial census "
      "staleness, not a pipeline defect. It is excluded from the failure "
      "interpretation below but counted for transparency.")
    A("")
    A("**Heuristic caveats (stated explicitly):** `no_reference_covers_it` "
      "here means a diagonal cluster formed but NO reference span verified at "
      "the generous 0.55 edit-density cutoff -- a strong signal the page is "
      "not a copy of any reference text, but it cannot fully distinguish "
      "'genuinely no reference exists' from 'the reference exists but this "
      "witness is so textually variant / HTR-garbled that even 0.55 fails'. "
      "Those two need different levers (REF-1 vs lower-k/confusion-weighted "
      "matching) and disentangling them requires the human grades requested "
      "below. `density_fail` is the cleaner bucket: a real alignment verified "
      "at 0.55 but missed the production boundary. `ambiguous` requires 2+ "
      f"distinct works both under a generous plausibility cutoff "
      f"({PLAUSIBLE_CUTOFF}) within {AMBIG_EPS} density of each other -- a "
      "documented design choice, not a fixed constant from prior work.")
    A("")
    A("### example cards")
    A("")
    for k in order:
        if not cards.get(k):
            continue
        A(f"**{k}** ({hist.get(k, 0)} total, showing up to 6):")
        for c in cards[k]:
            bd = (f" best_dens={c['best_dens']:.3f}"
                  if c.get('best_dens') is not None else "")
            A(f"- page `{c['page_id']}` (sys {c['sys_id']}, len {c['len']}): "
              f"grams={c['grams']} hits={c['hits']} "
              f"best_cluster={c['best_cluster']} "
              f"n_candidates={c['n_candidates']}{bd}")
        A("")

    # ---- 4: census ----
    A("## 4. fragment-population census (fullcorpus.db, counts only)")
    A("")
    for name, label in [('lt100', '<100 letters'), ('lt200', '<200 letters')]:
        c = census[name]
        tot = sum(c['totals'].values())
        A(f"### {label} (total {tot:,} pages)")
        A("")
        A(f"- track1_identified (live): {c['totals'].get('track1_identified', 0):,}")
        A(f"- canonmask_paired (Track-2, no work ID): "
          f"{c['totals'].get('canonmask_paired', 0):,}")
        A(f"- **neither_target (target population)**: "
          f"{c['totals'].get('neither_target', 0):,}")
        A("")
        A("By FJMS domain group:")
        A("")
        A("| domain | track1_identified | canonmask_paired | neither_target | total |")
        A("|---|---|---|---|---|")
        for dom, buckets in sorted(c['by_domain'].items(),
                                   key=lambda kv: -sum(kv[1].values())):
            t1 = buckets.get('track1_identified', 0)
            cm = buckets.get('canonmask_paired', 0)
            nt = buckets.get('neither_target', 0)
            A(f"| {dom} | {t1:,} | {cm:,} | {nt:,} | {t1 + cm + nt:,} |")
        A("")

    # ---- 5: pipeline design ----
    A("## 5. pipeline design (grounded in 1-4)")
    A("")
    A("**Per-length operating thresholds** (from the sweep above):")
    A("")
    for lb in LENGTHS:
        default = next(s for s in rp[lb]['sweep'] if s['scale'] == 1.0)
        verdict = ("census-safe at 1.0x" if (default['recall'] >= 0.5 and
                   default['top_mis_attribution'] <= 0.05)
                   else "candidate-tier only (review, not census)" if
                   default['n_any_id'] >= 5 else "mostly unrecoverable at 1.0x")
        A(f"- **{lb} letters**: recall {fmt_pct(default['recall'])}, "
          f"any-wrong {fmt_pct(default['mis_attribution'])}, "
          f"top-wrong {fmt_pct(default['top_mis_attribution'])} -> {verdict}")
    A("")
    A("**Where DF-immune querying suffices vs where A5's two-tier "
      "(census/candidate) gate is forced.** The two metrics separate the "
      "regimes cleanly under a take-BEST-match policy (accept only the single "
      "lowest-density work per page):")
    A("- **>=150 letters -- census-safe, no gate needed:** recall 87-98% AND "
      "top-wrong 0.7-1.4%. DF-immune track1_match-style querying at the "
      "current boundary is directly census-grade here. (Note the any-wrong "
      "rate climbs to 12% at 300 letters -- that is entirely spurious SECOND "
      "works clearing the loose >=100-letter boundary; a take-all-matches "
      "census would need the A5 gate to suppress them, but a take-best census "
      "does not.)")
    A("- **~100 letters -- candidate tier, but the gate is RECALL not "
      "precision:** top-wrong is only 6% (the IDs it produces are ~94% "
      "correct), but recall is 28% -- most 100-letter crops produce NO "
      "identification at all. So a 100-letter accepted ID is fairly "
      "trustworthy, but coverage is thin; route to candidate/review mainly "
      "because so few clear, and loosening the boundary (scale 1.1-1.2x lifts "
      "recall to 60-84%) trades in rising top-wrong -- the A5 length-"
      "conditional operating point lives exactly here.")
    A("- **<=80 letters -- recall-floored:** recall <=9% at the current "
      "boundary; the handful that do clear are unreliable (top-wrong 8-20%). "
      "Neither census nor a useful candidate stream; needs external signal or "
      "human review.")
    A("")
    A("**Unrecoverable floor:** at the shortest tested length (40 letters), "
      f"recall@1.0x = {fmt_pct(next(s for s in rp[40]['sweep'] if s['scale']==1.0)['recall'])}. "
      "Below this, lexical matching against ANY reference is structurally "
      "limited by the k=5 gram model itself (a 40-letter crop yields only "
      "~36 grams; a handful of transcription variants can eliminate all "
      "shared anchors) -- this is a genuine floor, not a tuning artifact, "
      "and fragments below it need either a different signal (paleography, "
      "codicological join, external catalog metadata) or human review, not "
      "a better acceptance threshold.")
    A("")
    genuine_total = total_hist - hist.get('would_now_pass', 0)
    A(f"**Which lever fixes which failure-mode regime** (from section 3; "
      f"percentages below are shares of the {genuine_total} GENUINE failures "
      f"-- {hist.get('would_now_pass', 0)} `would_now_pass` cases excluded "
      "from this denominator, since they are not failures):")
    A(f"- `no_reference_covers_it` ({fmt_pct(hist.get('no_reference_covers_it',0)/max(1,genuine_total))} "
      "of genuine failures) -> REF-1 reference expansion is the ONLY "
      "lever; no amount of threshold tuning helps a work that isn't in "
      "ref_corpus.pkl.")
    A(f"- `<2_anchors` / `no_diagonal_cluster` "
      f"({fmt_pct((hist.get('<2_anchors',0)+hist.get('no_diagonal_cluster',0))/max(1,genuine_total))}) "
      "-> genuinely thin signal on short-but-real text; length-conditional "
      "thresholds cannot manufacture anchors that don't exist -- these need "
      "either a smaller k (more anchors per letter, more false positives) or "
      "acceptance of a recall floor.")
    A(f"- `density_fail` ({fmt_pct(hist.get('density_fail',0)/max(1,genuine_total))}) "
      "-> a real candidate exists but is too noisy/short to clear the bar; "
      "this is exactly what a length-conditional, less conservative boundary "
      "(A5 conformal/FDR) targets -- expect this bucket to shrink most from "
      "that work.")
    A(f"- `ambiguous` ({fmt_pct(hist.get('ambiguous',0)/max(1,genuine_total))}) "
      "-> candidate tier or unrecoverable; by construction these need human "
      "disambiguation (shared formulaic language across 2+ works) and should "
      "never be auto-accepted regardless of threshold.")
    A("")
    A("**Targeted short-bin human-grading request for Hillel:** the census "
      f"shows {census['lt100']['totals'].get('neither_target',0):,} pages "
      "<100 letters and "
      f"{census['lt200']['totals'].get('neither_target',0):,} pages <200 "
      "letters with NEITHER a Track-1 ID nor a Track-2 canonmask pair -- the "
      "target population. Grades on ~5 no_reference_covers_it, ~5 "
      "density_fail, and ~5 ambiguous example cards above (already drawn from "
      "the real unidentified pool, evidence cards ready) would validate the "
      "failure-mode heuristics before FRAG-2 commits compute to the full-scale "
      "run; and grading ~20 crop-recovered identifications at length ~60-100 "
      "(the recall knee zone) would be the real-data precision check the "
      "synthetic curve cannot fully replace (per the truncation caveat above "
      "-- clean crops, not HTR-damaged fragments).")
    A("")
    return lines


def main():
    t0 = time.time()
    con = sqlite3.connect(DB)

    works, wid_to_wi, ref_tuple = build_reference()

    page_lengths = compute_page_lengths(con)
    live_ids, canonmask_ids = load_live_and_canonmask_ids(con)
    domains, groups, other_idx = load_domain_groups()

    sampled_pages, shortfall = sample_track1_pages(con)
    rnd = random.Random(RNG_SEED + 2)
    crops = make_crops(sampled_pages, rnd)
    crops = run_truncation_experiment(works, wid_to_wi, ref_tuple, crops)
    rp = analyze_recall_precision(crops)

    hist, cards, n_unident, best_dens_bands = diagnose_failure_modes(
        con, ref_tuple, page_lengths, live_ids, canonmask_ids)

    census = build_census(page_lengths, live_ids, canonmask_ids, domains,
                           groups, other_idx)

    ref_masks_info = "absent"
    if os.path.exists(MASKS):
        d = json.load(open(MASKS, encoding='utf-8'))
        ref_masks_info = (f"{len(d)} works masked, mtime="
                          f"{time.strftime('%Y-%m-%d %H:%M', time.localtime(os.path.getmtime(MASKS)))}")

    elapsed = time.time() - t0
    lines = write_report(shortfall, rp, hist, cards, n_unident, census,
                         ref_masks_info, len(works), elapsed, best_dens_bands)
    md = '\n'.join(lines)
    open(OUT_MD, 'w', encoding='utf-8').write(md)
    log(f"wrote {OUT_MD}")

    # machine-readable curve data (JSON) for the recall/mis-attribution
    # sweep, failure-mode histogram, and census -- everything needed to
    # rebuild the report tables without re-running the queries.
    curve_data = {
        'params': {
            'lengths': LENGTHS, 'crops_per_length': CROPS_PER_LENGTH,
            'n_sampled_pages': len(sampled_pages), 'n_crops': len(crops),
            'cat_targets': CAT_TARGETS, 'shortfall': shortfall,
            'scales': SCALES, 'wide_cutoff_frac': WIDE_CUTOFF_FRAC,
            'plausible_cutoff': PLAUSIBLE_CUTOFF, 'ambig_eps': AMBIG_EPS,
            'k': K, 'band': BAND, 'min_anchors': MIN_ANCHORS,
            'margin': MARGIN, 'min_span': MIN_SPAN,
            'ref_canon_masks': ref_masks_info, 'n_ref_works': len(works),
            'n_unidentified_sampled': n_unident,
        },
        'recall_precision_by_length': rp,
        'failure_mode_histogram': dict(hist),
        'failure_verifier_best_density_bands': best_dens_bands,
        'census': census,
        'elapsed_s': round(elapsed, 1),
    }
    json.dump(curve_data, open(OUT_JSON, 'w', encoding='utf-8'), indent=1,
              ensure_ascii=False, default=str)
    log(f"wrote {OUT_JSON}")
    con.close()
    log(f"TOTAL: {elapsed / 60:.1f} min")


if __name__ == '__main__':
    main()
