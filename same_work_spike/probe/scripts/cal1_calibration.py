# -*- coding: utf-8 -*-
"""CAL-1 -- P(same-work | density, length) calibration spike (FRAG-2 step 1).

Hillel's directive (2026-07-09): small texts, RECALL over precision, "maybe
with probability grade -- to find new items". This spike builds the grade:
a calibrated probability that a candidate identification is the right work,
as a function of (fragment length, edit-density), so the Map-v2 discovery
tier (tier B) can accept far below the census boundary and rank by P instead
of dropping near-misses.

Design (FRAG2-PLAN.md step 1; reuses frag1_truncation.py infra verbatim):
  1. LABELED candidates by construction: sample pages with EXACTLY ONE live
     Track-1 work label (shadowed_by IS NULL, high coverage, low density) and
     crop *within the labeled matched span* (spans_json) -- so the crop
     provably belongs to work W (fixes FRAG-1's residual-15%% contamination
     channel where a crop could land on an embedded quotation). Query each
     crop against the reference index at a WIDE verification cutoff (0.75)
     and record every distinct-work candidate as a row:
         (crop_len, alen, density, is_correct = matched work == W).
  2. NULL arm: chunk-shuffle decoys (CHUNK=25 primary / CHUNK=10 optimistic,
     A5 protocol) through the IDENTICAL query path -> chance-candidate rate
     and null density distribution per length.
  3. FIT: per crop-length bin, isotonic (PAVA, non-increasing in density)
     regression of is_correct on density; WORK-GRANULAR train/holdout split
     (split by the crop's true work -- page/crop-level splits leak).
  4. CONTAMINATION curve: per length, threshold sweep -> accept rate, wrong-
     work share among accepted, decoy (chance) accept rate.
  5. EXTERNAL validation vs Hillel's graded cards (frag1 33 + discovery 34):
     empirical precision of real graded verdicts vs predicted P.

Read-only: fullcorpus.db opened with busy_timeout; no map tables touched;
no git commit from this script.

Usage: python -X utf8 -u cal1_calibration.py
Out:   ../data/p_calibration.json      (binned isotonic model + meta)
       ../data/cal1_rows.json          (raw labeled candidate rows)
       ../results/p_calibration.md     (report)
"""
import json
import random
import sqlite3
import time
from collections import Counter, defaultdict

from frag1_truncation import (build_reference, query_batch, log)
from normalize import norm_stream

ROOT = r"C:\Genizahsearch"
PROBE = ROOT + r"\same_work_spike\probe"
DB = PROBE + r"\data\fullcorpus.db"
# FINAL (frozen Map-v2) state: v2 reference + v2 masks; page labels still
# come from the v1 track1_matches (the only labels existing pre-rebuild),
# but crop TEXT is read from the v2 db restricted to provenance='htr'
# (identical text; FGP-substituted pages are excluded because their v1 span
# offsets no longer apply).
DB_V2 = PROBE + r"\data\fullcorpus_v2.db"
REF_V2 = PROBE + r"\data\ref_corpus_v2.pkl"
MASKS_V2 = PROBE + r"\data\ref_canon_masks_v2.json"
# stage-tagged outputs: the PILOT (pre-REF-2) model must never be mistaken
# for the FINAL (frozen Map-v2 state) model -- rerun with --tag final there.
STAGE_DEFAULT = 'pilot'
OUT_MODEL = PROBE + r"\data\p_calibration_{tag}.json"
OUT_ROWS = PROBE + r"\data\cal1_rows_{tag}.json"
OUT_MD = PROBE + r"\results\p_calibration_{tag}.md"
FRAG1_CARDS = PROBE + r"\review\frag1_cards.json"
FRAG1_GRADES = PROBE + r"\review\grades_frag1_2026-07-09.json"
DISC_CARDS = PROBE + r"\review\discovery_cards.json"
DISC_GRADES = PROBE + r"\review\grades_discovery_2026-07-09.json"

RNG_SEED = 20260710

# experiment parameters
LENGTHS = [40, 60, 80, 100, 150, 200, 300]
CROPS_PER_LENGTH = 2
MIN_SPAN_LEN = 340                 # crop inside the labeled span only
COVERAGE_MIN = 0.85
DENSITY_MAX = 0.15
CAT_TARGETS = {'Bible': 380, 'Maagarim': 300, 'JA': 160, 'Bavli': 80,
               'Mishnah': 40, 'Yerushalmi': 16, 'Tosefta': 4}   # ~980 pages
WIDE_CUTOFF = 0.75                 # record candidates out to here (A5 q=0.01
                                   # envelope reached ~0.66 at short lengths)
# NO top-K censoring: ALL distinct-work candidates are recorded (Codex HIGH —
# a rank cap hides lower-ranked false candidates and biases contamination
# downward; any deployment rank policy is applied downstream, consistently).
CHUNK_PRIMARY, CHUNK_OPT = 25, 10  # decoy shuffle protocols (A5)
HOLDOUT_FRAC = 0.2                 # of true works, for reliability check
P_TARGETS = [0.9, 0.7, 0.5, 0.3, 0.2]   # tier-B operating-point candidates


# =====================================================================
# sampling: single-work pages, crop inside the labeled span
# =====================================================================

def sample_single_work_pages(con, text_con=None, require_htr=False):
    """Pages with exactly ONE distinct live work label, confident + high
    coverage; returns per page the largest labeled span's stream slice.

    FINAL mode: labels/spans from `con` (v1 fullcorpus.db — the only labels
    existing pre-rebuild); text from `text_con` (fullcorpus_v2.db) restricted
    to provenance='htr' (identical text to v1, so v1 span offsets stay
    valid; FGP-substituted pages are excluded and counted)."""
    t0 = time.time()
    tcon = text_con or con
    n_fgp_skipped = 0
    rows = con.execute("""
        SELECT page_id, MIN(work_id), MIN(cat), MIN(spans_json),
               MIN(best_density), COUNT(DISTINCT work_id)
        FROM track1_matches
        WHERE shadowed_by IS NULL
        GROUP BY page_id
        HAVING COUNT(DISTINCT work_id) = 1 AND MIN(best_density) <= ?
    """, (DENSITY_MAX,)).fetchall()
    pool = defaultdict(list)
    for pid, wid, cat, spans_json, bd, _ in rows:
        pool[cat].append((pid, wid, cat, spans_json))
    log(f"single-work page pool: {sum(len(v) for v in pool.values()):,} pages, "
        f"by cat: {dict((c, len(v)) for c, v in pool.items())} "
        f"({time.time() - t0:.0f}s)")

    rnd = random.Random(RNG_SEED)
    chosen, shortfall = [], {}
    for cat, tgt in CAT_TARGETS.items():
        cand = pool.get(cat, [])
        # round-robin by work for diversity
        by_work = defaultdict(list)
        for it in cand:
            by_work[it[1]].append(it)
        for w in by_work:
            rnd.shuffle(by_work[w])
        wids = list(by_work)
        rnd.shuffle(wids)
        order, ptr, prog = [], {w: 0 for w in wids}, True
        while prog:
            prog = False
            for w in wids:
                if ptr[w] < len(by_work[w]):
                    order.append(by_work[w][ptr[w]])
                    ptr[w] += 1
                    prog = True
        got = []
        for pid, wid, _cat, spans_json in order:
            if len(got) >= tgt:
                break
            if require_htr:
                trow = tcon.execute(
                    "SELECT text, provenance FROM pages WHERE page_id=?",
                    (pid,)).fetchone()
                if trow and trow[1] != 'htr':
                    n_fgp_skipped += 1
                    continue
            else:
                trow = tcon.execute(
                    "SELECT text FROM pages WHERE page_id=?",
                    (pid,)).fetchone()
            if not trow or not trow[0]:
                continue
            stream, _ = norm_stream(trow[0])
            plen = len(stream)
            if plen <= 0:
                continue
            spans = json.loads(spans_json) if spans_json else []
            if not spans:
                continue
            s, e = max(spans, key=lambda sp: sp[1] - sp[0])[:2]
            s, e = max(0, int(s)), min(plen, int(e))
            if e - s < MIN_SPAN_LEN:
                continue
            # coverage: labeled letters / page letters
            cov = sum(min(plen, int(b)) - max(0, int(a))
                      for a, b, *_ in spans) / plen
            if cov < COVERAGE_MIN:
                continue
            got.append({'page_id': pid, 'work_id': wid, 'cat': cat,
                        'span_stream': stream[s:e]})
        if len(got) < tgt:
            shortfall[cat] = (len(got), tgt)
        chosen.extend(got)
    log(f"sampled {len(chosen):,} single-work pages "
        f"({time.time() - t0:.0f}s); shortfalls: {shortfall or 'none'}"
        + (f"; fgp-provenance pages skipped: {n_fgp_skipped}"
           if require_htr else ""))
    return chosen, shortfall


def make_crops(pages, rnd):
    crops = []
    for pg in pages:
        s = pg['span_stream']
        for length in LENGTHS:
            if len(s) < length:
                continue
            for ci in range(CROPS_PER_LENGTH):
                off = rnd.randint(0, len(s) - length)
                crops.append({'page_id': pg['page_id'],
                              'work_id': pg['work_id'], 'cat': pg['cat'],
                              'length': length, 'crop_idx': ci,
                              'stream': s[off:off + length]})
    return crops


def chunk_shuffle(stream, chunk, rnd):
    parts = [stream[i:i + chunk] for i in range(0, len(stream), chunk)]
    rnd.shuffle(parts)
    return ''.join(parts)


# =====================================================================
# noise-injection arm (FRAG2-PLAN mitigation (b) -- TRIGGERED by the pilot
# stress test: Hillel's real density_fail cards are 10/10 correct at density
# 0.41-0.55 but clean-crop calibration predicts P 0.002-0.24 there. HTR noise
# pushes CORRECT matches into densities where clean crops are mostly wrong,
# so the clean model is PESSIMISTIC for the real orphan population. Injecting
# the MEASURED HTR error profile (results/confusion_matrix.json, 209 FGP-
# aligned pages) recalibrates for noisy-but-correct matches.)
# =====================================================================

CONF_MATRIX = PROBE + r"\results\confusion_matrix.json"


def load_noise_model():
    """(sub_dist, ins_dist, op_weights) from the empirical profile.
    sub_dist: ref_char -> ([hyp_chars], [cum_weights]); ins_dist likewise
    global. Op mix approximated from available counts (full substitution
    list; top-10 ins/del lists) -- a stress-arm approximation, documented."""
    d = json.load(open(CONF_MATRIX, encoding='utf-8'))
    subs = d['substitutions']                      # [ref, hyp, count]
    by_ref = defaultdict(list)
    for ref, hyp, cnt in subs:
        by_ref[ref].append((hyp, cnt))
    sub_dist = {ref: ([h for h, _ in lst], [c for _, c in lst])
                for ref, lst in by_ref.items()}
    ins_top = d['report']['top_insertions']
    ins_dist = ([e['hyp'] for e in ins_top], [e['count'] for e in ins_top])
    n_sub = sum(c for _, _, c in subs)
    n_del = sum(e['count'] for e in d['report']['top_deletions'])
    n_ins = sum(e['count'] for e in ins_top)
    return sub_dist, ins_dist, (n_sub, n_del, n_ins)


def perturb(stream, rate, noise_model, rnd):
    """Inject sub/del/ins errors at ~`rate` per letter, sampled from the
    empirical HTR profile."""
    sub_dist, ins_dist, (w_sub, w_del, w_ins) = noise_model
    w_tot = w_sub + w_del + w_ins
    out = []
    for ch in stream:
        r = rnd.random()
        if r >= rate:
            out.append(ch)
            continue
        op = rnd.random() * w_tot
        if op < w_sub:
            if ch in sub_dist:
                hyps, wts = sub_dist[ch]
                out.append(rnd.choices(hyps, weights=wts)[0])
            else:
                out.append(rnd.choices(*ins_dist)[0])
        elif op < w_sub + w_del:
            pass                                   # deletion
        else:
            out.append(rnd.choices(*ins_dist)[0])  # insertion + keep char
            out.append(ch)
    return ''.join(out)


# =====================================================================
# candidate rows
# =====================================================================

def best_per_work(cands):
    """[(work_idx, alen, dens)] -> {work_idx: (dens, alen)} best per work."""
    best = {}
    for wi, alen, dens in cands:
        if wi not in best or dens < best[wi][0]:
            best[wi] = (dens, alen)
    return best


def collect_rows(crops, results, wid_to_wi, works=None, use_vgroups=False):
    """One row per (crop, distinct candidate work) — ALL works, no rank cap.
    Also returns per-crop best WRONG-work density (the leave-work-out arm:
    dropping the true work's candidates post-hoc is identical, on the accept
    surface, to removing that work from the index — measures how often a
    wrong work would be accepted when the right one is ABSENT).

    use_vgroups (FINAL only): truth relation = version-group identity — a
    candidate in the SAME vgroup as the labeled work counts correct (multi-
    rite liturgy twins etc. are the same-work answer, not contamination).
    Adds per-row 'margin' (best-competing density minus this row's density,
    None for singletons) + 'n_comp' for the margin-band model."""
    def token_of(wi):
        if use_vgroups and works is not None:
            vg = works[wi].get('vgroup')
            return ('vg', vg) if vg is not None else works[wi]['id']
        return wi

    rows = []
    recall_wide = Counter()
    n_by_len = Counter()
    lwo_wrong = defaultdict(list)          # length -> per-crop min wrong dens
    for ci, (c, cands) in enumerate(zip(crops, results)):
        n_by_len[c['length']] += 1
        true_wi = wid_to_wi.get(c['work_id'])
        true_tok = (token_of(true_wi) if true_wi is not None
                    else c['work_id'])
        bp = best_per_work(cands)
        ranked = sorted(bp.items(), key=lambda kv: kv[1][0])
        if any(token_of(wi) == true_tok for wi in bp):
            recall_wide[c['length']] += 1
        wrong_ds = [dens for wi, (dens, _a) in ranked
                    if token_of(wi) != true_tok]
        if wrong_ds:
            lwo_wrong[c['length']].append(min(wrong_ds))
        best_d = ranked[0][1][0] if ranked else None
        second_d = ranked[1][1][0] if len(ranked) > 1 else None
        for rank, (wi, (dens, alen)) in enumerate(ranked):
            if len(ranked) == 1:
                m = None                        # singleton
            elif rank == 0:
                m = second_d - dens
            else:
                m = best_d - dens               # <= 0: not-best
            rows.append({'crop_id': ci, 'len': c['length'], 'alen': alen,
                         'dens': dens, 'rank': rank,
                         'margin': (round(m, 4) if m is not None else None),
                         'n_comp': len(ranked) - 1,
                         'correct': int(token_of(wi) == true_tok),
                         'true_work': c['work_id'], 'cat': c['cat'],
                         'page_id': c['page_id']})
    return rows, recall_wide, n_by_len, lwo_wrong


# =====================================================================
# PAVA isotonic fit (non-increasing P in density)
# =====================================================================

def pava_decreasing(triples):
    """triples = [(dens, y, w)] -> step function as [(max_dens, p, n)] blocks,
    p non-increasing with density; weighted pool-adjacent-violators. `n` is
    the RAW row count in the block (for the report); the fit uses weights."""
    triples = sorted(triples)
    blocks = []                       # each: [sum_wy, sum_w, n_rows, max_dens]
    for dens, y, w in triples:
        cur = [float(y) * w, w, 1, dens]
        # non-increasing violated when previous block mean < current mean
        while blocks and blocks[-1][0] / blocks[-1][1] < cur[0] / cur[1]:
            prev = blocks.pop()
            cur = [prev[0] + cur[0], prev[1] + cur[1],
                   prev[2] + cur[2], cur[3]]
        blocks.append(cur)
    return [(b[3], b[0] / b[1], b[2]) for b in blocks]


def work_weights(rows, by_alen=False):
    """Per-(length, true_work) equal-total-weight: each source work
    contributes weight 1 per length bin regardless of how many candidate rows
    its crops generated (Codex HIGH: raw rows overweight candidate-rich
    works). Returns {(binL, true_work) -> w}. by_alen (final2) keys the bin by
    the row's nearest ALEN bin instead of crop len — the deploy feature."""
    if by_alen:
        n_per = Counter((nearest_bin(r.get('alen') or r['len']), r['true_work'])
                        for r in rows)
    else:
        n_per = Counter((r['len'], r['true_work']) for r in rows)
    return {k: 1.0 / n for k, n in n_per.items()}


def nearest_bin(x):
    """Nearest calibrated length bin: min |L - x| over LENGTHS. Identical
    selection rule to p_deploy / PModel._nearest / p_lookup. final2 buckets the
    FIT by alen through this; the runner already DEPLOYS by alen through the
    same rule (so fit / validate / deploy all key on one feature)."""
    return min(LENGTHS, key=lambda L: abs(L - x))


def p_lookup(model_bins, length, dens):
    """Step-function lookup with clamping; nearest length bin."""
    lb = min(model_bins, key=lambda L: abs(L - length))
    knots = model_bins[lb]            # [(max_dens, p, n)] density-ascending
    for max_d, p, _n in knots:
        if dens <= max_d:
            return p
    return knots[-1][1] if knots else 0.0


def fit_calibration(rows, by_alen=False):
    """Work-granular split -> per-length isotonic model + holdout reliability.
    Returns the actual train/hold row lists so the margin-band model can be
    fit on the SAME split. by_alen (final2): the pooled fallback model is fit
    (and its reliability bucketed) by nearest ALEN bin, matching how p_deploy
    keys the pooled fallback at deployment."""
    works = sorted({r['true_work'] for r in rows})
    rnd = random.Random(RNG_SEED + 7)
    rnd.shuffle(works)
    n_hold = max(1, int(len(works) * HOLDOUT_FRAC))
    hold_works = set(works[:n_hold])
    train = [r for r in rows if r['true_work'] not in hold_works]
    hold = [r for r in rows if r['true_work'] in hold_works]

    def binof(r):
        return nearest_bin(r.get('alen') or r['len']) if by_alen else r['len']

    tw = work_weights(train, by_alen)
    model = {}
    for L in LENGTHS:
        pts = [(r['dens'], r['correct'], tw[(L, r['true_work'])])
               for r in train if binof(r) == L]
        if len(pts) >= 20:
            model[L] = pava_decreasing(pts)
    # reliability on holdout: bucket by predicted P, same per-work weighting
    hw = work_weights(hold, by_alen)
    rel = defaultdict(lambda: [0.0, 0.0, 0])
    for r in hold:
        Lr = binof(r)
        if Lr not in model:
            continue
        p = p_lookup(model, Lr, r['dens'])
        b = min(9, int(p * 10))
        w = hw[(Lr, r['true_work'])]
        rel[b][0] += r['correct'] * w
        rel[b][1] += w
        rel[b][2] += 1
    reliability = {f"{b/10:.1f}-{(b+1)/10:.1f}":
                   {'n': n, 'pred_mid': (b + 0.5) / 10,
                    'empirical': round(kw / ww, 3)}
                   for b, (kw, ww, n) in sorted(rel.items()) if n > 0}
    return model, reliability, train, hold, len(hold_works)


# =====================================================================
# margin-band model (FINAL; cal1_margin_refit.py finding folded in) +
# decoy-anchored singleton null (FRAG2-PLAN singleton fix)
# =====================================================================

MARGIN_BANDS = ['m_ge_010', 'm_003_010', 'm_0_003', 'not_best']
MARGIN_FIT_FLOOR = 20
# ---- final2 (alen-bin refit) knobs — GATED on FIT_BY_ALEN. No effect on the
# 'pilot' / plain-'final' tags, which keep the crop-len fit byte-for-byte.
# Audit (results/agent_final_cal_audit.md §3/§7): alen collapses to ~66 for
# every crop len, so the isotonic must be FIT by alen (as it is deployed), and
# the two small-margin bands have <10 effective works per cell → pool them.
POOL_BANDS = {'m_003_010', 'm_0_003'}   # too few effective works to stratify
EFFECTIVE_WORKS_FLOOR = 8               # pool alen bins below this many works


def margin_band_of(row):
    """Same band edges the Map-v2 runner deploys (mapv2_track1_run.py)."""
    if row['n_comp'] == 0:
        return 'singleton'
    m = row['margin']
    if m is None or m <= 0:
        return 'not_best'
    if m >= 0.10:
        return 'm_ge_010'
    if m >= 0.03:
        return 'm_003_010'
    return 'm_0_003'


def fit_margin_model(train, by_alen=False):
    """{band: {L: knots}} isotonic per (margin-band, length) where >= floor
    rows exist. The SINGLETON band is deliberately absent: only 12/921K pilot
    rows were synthetic singletons (random crops of well-referenced works
    always chance-match other works) — it is scored by the decoy null.

    by_alen (final2): bucket by the row's nearest ALEN bin (not crop len), so
    fit / validate / deploy all key on the SAME feature (audit §3). Within the
    two small-margin POOL_BANDS, a cell with < EFFECTIVE_WORKS_FLOOR distinct
    works borrows the band-pooled curve (fit on the union of that band's rows
    across all alen bins, one weight-unit per distinct work) instead of an
    overfit per-cell isotonic (audit §7)."""
    tw = work_weights(train, by_alen)

    def binof(r):
        return nearest_bin(r.get('alen') or r['len']) if by_alen else r['len']

    out = {}
    n_per = Counter()
    eff_per = {}
    for band in MARGIN_BANDS:
        cells = {L: [] for L in LENGTHS}
        for r in train:
            if margin_band_of(r) == band:
                cells[binof(r)].append(r)
        for L in LENGTHS:
            n_per[(band, L)] = len(cells[L])
            eff_per[(band, L)] = len({r['true_work'] for r in cells[L]})

        do_pool = by_alen and band in POOL_BANDS
        pooled_curve = None
        if do_pool:
            band_rows = [r for L in LENGTHS for r in cells[L]]
            if band_rows:
                n_pw = Counter(r['true_work'] for r in band_rows)
                pooled_curve = pava_decreasing(
                    [(r['dens'], r['correct'], 1.0 / n_pw[r['true_work']])
                     for r in band_rows])

        bins = {}
        for L in LENGTHS:
            cell = cells[L]
            if do_pool:
                # small-margin band: self-fit only when the cell clears the
                # EFFECTIVE-works floor; otherwise borrow the band-pooled curve.
                if eff_per[(band, L)] >= EFFECTIVE_WORKS_FLOOR:
                    bins[L] = pava_decreasing(
                        [(r['dens'], r['correct'], tw[(L, r['true_work'])])
                         for r in cell])
                elif pooled_curve is not None:
                    bins[L] = pooled_curve
            elif len(cell) >= MARGIN_FIT_FLOOR:
                bins[L] = pava_decreasing(
                    [(r['dens'], r['correct'], tw[(L, r['true_work'])])
                     for r in cell])
        if bins:
            out[band] = bins
    counts = {f"{b}/{L}": n for (b, L), n in sorted(n_per.items())}
    if by_alen:
        counts = {k: {'rows': counts[k],
                      'eff_works': eff_per[(k.split('/')[0], int(k.split('/')[1]))]}
                  for k in counts}
    return out, counts


def build_singleton_null(decoy_best, n_decoy_by_len, max_knots=60):
    """Chance null per length: CDF of the decoys' BEST-candidate density,
    denominated over ALL decoy queries of that length (a decoy with no
    candidate contributes only to the denominator). Deployment scores a
    singleton at (L, d) as P ~= 1 - CDF_L(d): the probability that pure
    chance would NOT produce a candidate this good. High-density singletons
    (d > ~0.45) get low P automatically — chance singletons are common
    there (pilot n20 decoy_accept ~0.40 at len150/d0.50)."""
    null = {}
    for L, n_total in n_decoy_by_len.items():
        ds = sorted(decoy_best.get(L, []))
        if not n_total:
            continue
        knots = []
        for i, d in enumerate(ds):
            knots.append([round(d, 4), round((i + 1) / n_total, 5)])
        if len(knots) > max_knots:
            step = len(knots) / max_knots
            knots = [knots[int(i * step)] for i in range(max_knots - 1)]
            knots.append([round(ds[-1], 4), round(len(ds) / n_total, 5)])
        null[L] = knots
    return null


def p_deploy(row, model, margin_model, singleton_null):
    """Deployment-composed lookup: margin band -> band curve; singleton ->
    decoy null; fallback pooled. MUST mirror mapv2_track1_run.PModel.p —
    including the LENGTH FEATURE: the runner buckets by the row's ALIGNED
    length, so this lookup does too (Codex code-gate BLOCKER 2: bucketing
    the holdout by crop len while deploying by alen would validate one bin
    and deploy another)."""
    alen = row.get('alen') or row['len']
    band = margin_band_of(row)
    if band == 'singleton' and singleton_null:
        L = min(singleton_null, key=lambda x: abs(x - alen))
        frac = 0.0
        for d0, f in singleton_null[L]:
            if row['dens'] <= d0:
                frac = f
                break
        else:
            frac = singleton_null[L][-1][1] if singleton_null[L] else 1.0
        return max(0.0, 1.0 - frac)
    bins = margin_model.get(band)
    if bins:
        L = min(bins, key=lambda x: abs(x - alen))
        knots = bins[L]
        for max_d, p, _n in knots:
            if row['dens'] <= max_d:
                return p
        return knots[-1][1] if knots else 0.0
    return p_lookup(model, alen, row['dens'])


def margin_reliability(hold, model, margin_model, singleton_null,
                       by_alen=False):
    """Holdout reliability of the DEPLOYMENT-composed lookup."""
    hw = work_weights(hold, by_alen)
    rel = defaultdict(lambda: [0.0, 0.0, 0])
    for r in hold:
        p = p_deploy(r, model, margin_model, singleton_null)
        b = min(9, int(p * 10))
        Lr = nearest_bin(r.get('alen') or r['len']) if by_alen else r['len']
        w = hw[(Lr, r['true_work'])]
        rel[b][0] += r['correct'] * w
        rel[b][1] += w
        rel[b][2] += 1
    return {f"{b/10:.1f}-{(b+1)/10:.1f}":
            {'n': n, 'pred_mid': (b + 0.5) / 10,
             'empirical': round(kw / ww, 3)}
            for b, (kw, ww, n) in sorted(rel.items()) if n > 0}


# =====================================================================
# final2 self-validation: (margin-band x alen-bin) reliability grid through
# the DEPLOY path (p_deploy), with a comparison vs the crop-len-fit `final`
# model. Reproduces the audit's §2 reliability grid so a re-fit can be judged
# against the exact table the audit flagged.
# =====================================================================

def load_deploy_model(path):
    """Load a p_calibration_*.json into (model, margin_model, singleton_null)
    typed exactly as PModel does (int length keys), so p_deploy can score
    against it. Returns None if the file is absent/unreadable."""
    try:
        d = json.load(open(path, encoding='utf-8'))
    except Exception as e:                                    # noqa: BLE001
        log(f"comparison model {path} not loaded: {e!r}")
        return None
    model = {int(L): v for L, v in d.get('model', {}).items()}
    margin_model = {b: {int(L): v for L, v in bins.items()}
                    for b, bins in d.get('margin_model', {}).items()}
    singleton_null = {int(L): v for L, v in d.get('singleton_null', {}).items()}
    return model, margin_model, singleton_null


def reliability_grid(hold, model, margin_model, singleton_null):
    """Per (margin-band, alen-bin): raw n, work-weighted mean predicted P
    (through p_deploy — the deploy path), work-weighted empirical same-work
    rate, and gap = pred - empirical. Work weight = 1 per (alen-bin, work)."""
    hw = work_weights(hold, by_alen=True)
    g = defaultdict(lambda: [0.0, 0.0, 0.0, 0])   # sum_w_pred, sum_w_corr, w, n
    for r in hold:
        band = margin_band_of(r)
        L = nearest_bin(r.get('alen') or r['len'])
        p = p_deploy(r, model, margin_model, singleton_null)
        w = hw[(L, r['true_work'])]
        cell = g[(band, L)]
        cell[0] += p * w
        cell[1] += r['correct'] * w
        cell[2] += w
        cell[3] += 1
    out = {}
    for (band, L), (sp, sc, sw, n) in g.items():
        if sw > 0:
            pred, emp = sp / sw, sc / sw
            out[(band, L)] = {'n': n, 'pred': round(pred, 4),
                              'emp': round(emp, 4), 'gap': round(pred - emp, 4)}
    return out


def grid_section(hold, new_deploy, final_path, worse_thresh=0.05):
    """Build the (band x alen-bin) self-validation md section for final2,
    comparing the new deploy composition against the crop-len-fit `final`
    model at final_path (if present). Returns (md_lines, worsened, worst5)."""
    new_grid = reliability_grid(hold, *new_deploy)
    old = load_deploy_model(final_path)
    old_grid = reliability_grid(hold, *old) if old else {}

    band_order = {b: i for i, b in enumerate(
        ['singleton', 'm_ge_010', 'm_003_010', 'm_0_003', 'not_best'])}
    keys = sorted(new_grid, key=lambda k: (band_order.get(k[0], 9), k[1]))

    lines = [
        "### Self-validation — holdout reliability grid (margin band × ALEN "
        "bin) through the DEPLOY path",
        "",
        "Work-weighted (1 unit per (alen-bin, work)); `gap = pred − empirical`. "
        "`final gap` re-scores the SAME holdout rows through the crop-len-fit "
        "`final` model for a like-for-like comparison. **`Δ|gap|`** = "
        "`|final2 gap| − |final gap|` (negative = final2 improved); a bucket "
        f"flagged **WORSE** regressed by > {worse_thresh}.",
        "",
        "| band | alen | n | final2 pred | empirical | final2 gap | final pred "
        "| final gap | Δ\\|gap\\| | flag |",
        "|---|---|---|---|---|---|---|---|---|---|",
    ]
    worsened, deltas = [], []
    for k in keys:
        band, L = k
        v = new_grid[k]
        ov = old_grid.get(k)
        if ov:
            dabs = round(abs(v['gap']) - abs(ov['gap']), 4)
            deltas.append((k, dabs, v, ov))
            worse = dabs > worse_thresh
            if worse:
                worsened.append((k, dabs, v, ov))
            lines.append(
                f"| {band} | {L} | {v['n']} | {v['pred']:.3f} | {v['emp']:.3f} "
                f"| {v['gap']:+.3f} | {ov['pred']:.3f} | {ov['gap']:+.3f} "
                f"| {dabs:+.3f} | {'**WORSE**' if worse else ''} |")
        else:
            lines.append(
                f"| {band} | {L} | {v['n']} | {v['pred']:.3f} | {v['emp']:.3f} "
                f"| {v['gap']:+.3f} | — | — | — | |")
    # summary
    lines.append("")
    if old_grid:
        n_improved = sum(1 for _, d, _, _ in deltas if d < -1e-9)
        n_regressed = len(worsened)
        mean_abs_new = (sum(abs(new_grid[k]['gap']) for k in keys) / len(keys)
                        if keys else 0.0)
        common = [k for k in keys if k in old_grid]
        mean_abs_new_c = (sum(abs(new_grid[k]['gap']) for k in common)
                          / len(common) if common else 0.0)
        mean_abs_old_c = (sum(abs(old_grid[k]['gap']) for k in common)
                          / len(common) if common else 0.0)
        lines.append(
            f"**Mean |gap| over {len(common)} shared buckets: "
            f"final2 {mean_abs_new_c:.3f} vs final {mean_abs_old_c:.3f}.** "
            f"Improved: {n_improved}; regressed by >{worse_thresh}: "
            f"{n_regressed}.")
        if worsened:
            lines.append("")
            lines.append("**Buckets that regressed:**")
            for (band, L), d, v, ov in sorted(worsened, key=lambda x: -x[1]):
                lines.append(
                    f"- `{band}`/alen-{L}: |gap| {abs(ov['gap']):.3f} → "
                    f"{abs(v['gap']):.3f} (Δ {d:+.3f}); pred {ov['pred']:.3f}"
                    f"→{v['pred']:.3f}, empirical {v['emp']:.3f} (n={v['n']})")
    else:
        lines.append(f"(no `final` model at {final_path} — no comparison)")
    lines.append("")
    return lines, worsened, new_grid, old_grid


# =====================================================================
# contamination / threshold sweep + decoys
# =====================================================================

def sweep(rows, decoy_best, lwo_wrong, n_by_len, n_decoy_by_len):
    """Per length, per density threshold: recall of the true work, wrong-work
    share among accepted candidates, leave-work-out (absent-work) accept rate,
    decoy chance-accept rate."""
    TH = [0.20, 0.25, 0.30, 0.35, 0.40, 0.45, 0.50, 0.55, 0.60, 0.65, 0.70]
    out = {}
    by_len = defaultdict(list)
    for r in rows:
        by_len[r['len']].append(r)
    for L in LENGTHS:
        rs = by_len.get(L, [])
        n_crops = n_by_len.get(L, 0)
        # recall counted per CROP (true work accepted for that crop)
        true_by_crop = defaultdict(list)
        for r in rs:
            if r['correct']:
                true_by_crop[r['crop_id']].append(r['dens'])
        table = []
        for t in TH:
            acc = [r for r in rs if r['dens'] <= t]
            n_true_crops = sum(1 for ds in true_by_crop.values()
                               if min(ds) <= t)
            n_true = sum(1 for r in acc if r['correct'])
            n_wrong = len(acc) - n_true
            lwo = lwo_wrong.get(L, [])
            absent = (sum(1 for d in lwo if d <= t) / n_crops) \
                if n_crops else 0.0
            dec = decoy_best.get(L, [])
            chance = (sum(1 for d in dec if d <= t) / len(dec)) if dec else 0.0
            table.append({'thresh': t,
                          'recall': round(n_true_crops / max(1, n_crops), 3),
                          'n_accepted': len(acc), 'n_wrong': n_wrong,
                          'wrong_share': round(n_wrong / max(1, len(acc)), 3),
                          'absent_work_accept': round(absent, 4),
                          'decoy_accept': round(chance, 4)})
        out[L] = table
    return out


# =====================================================================
# external validation vs Hillel's graded cards
# =====================================================================

def load_graded_external(model):
    """Best-effort: map graded cards to (length, density) -> predicted P vs
    verdict. Defensive to schema drift; skips cards lacking density data."""
    out = []
    try:
        cards = {c['id']: c for c in
                 json.load(open(DISC_CARDS, encoding='utf-8'))}
        for g in json.load(open(DISC_GRADES, encoding='utf-8')):
            c = cards.get(g['id'])
            if not c or c.get('type') != 'new_sample':
                continue
            dens = c.get('best_density')
            ml = c.get('matched_letters')
            if dens is None or not ml:
                continue
            correct = int(g['grade'] in
                          ('confirmed-new-witness', 'correct-work-likely-known'))
            out.append({'set': 'discovery_new', 'len': int(ml),
                        'dens': float(dens),
                        'pred': round(p_lookup(model, int(ml), float(dens)), 3),
                        'human_correct': correct, 'grade': g['grade']})
    except Exception as e:                                    # noqa: BLE001
        log(f"external validation (discovery) skipped: {e!r}")
    try:
        cards = {c['id']: c for c in
                 json.load(open(FRAG1_CARDS, encoding='utf-8'))}
        for g in json.load(open(FRAG1_GRADES, encoding='utf-8')):
            c = cards.get(g['id'])
            if not c or g.get('type') not in ('density_fail', 'crop_recovered'):
                continue
            # real schema (verified): density_fail carries cand_density +
            # cand_aligned_len; crop_recovered carries density + crop_len.
            if g['type'] == 'density_fail':
                dens, length = c.get('cand_density'), c.get('cand_aligned_len')
            else:
                dens, length = c.get('density'), c.get('crop_len')
            if dens is None or length is None:
                continue
            correct = int(g['grade'] in ('candidate-correct', 'correct-id'))
            out.append({'set': f"frag1_{g['type']}", 'len': int(length),
                        'dens': float(dens),
                        'pred': round(p_lookup(model, int(length), float(dens)), 3),
                        'human_correct': correct, 'grade': g['grade']})
    except Exception as e:                                    # noqa: BLE001
        log(f"external validation (frag1) skipped: {e!r}")
    return out


# =====================================================================
# report
# =====================================================================

def write_report(shortfall, n_pages, n_crops, recall_wide, n_by_len, rows,
                 model, reliability, n_train, n_hold, n_hold_works,
                 sweeps, decoy_meta, external, elapsed):
    L_ = []
    A = L_.append
    A("# CAL-1 **PILOT** — P(same-work | density, length) calibration "
      "(FRAG-2 step 1)")
    A("")
    A("**PILOT run on the CURRENT (pre-REF-2) reference state.** Per the "
      "Codex plan-gate, the DEPLOYED model is re-fit by re-running this exact "
      "script on the frozen Map-v2 corpus/reference state (ref_corpus v2 + "
      "version groups + search_text). Use this report for machinery "
      "validation, curve SHAPES, and operating-point brackets — not final "
      "thresholds.")
    A("")
    A(f"Generated {time.strftime('%Y-%m-%d %H:%M')}; runtime "
      f"{elapsed/60:.1f} min. {n_pages:,} single-work pages → {n_crops:,} "
      f"crops (crop taken INSIDE the labeled span — label purity by "
      f"construction); wide verification cutoff {WIDE_CUTOFF}; ALL "
      f"distinct-work candidates recorded (no rank cap); decoys: "
      f"chunk-shuffle {decoy_meta}. Sampling shortfalls: "
      f"{shortfall or 'none'}.")
    A("")
    A("**Bias statement (binding):** crops are clean slices of pages whose "
      "HTR was good enough for a confident full-page ID. Real orphan "
      "fragments are noisier at equal length, so P here is an OPTIMISTIC "
      "bound; the external-validation section (real graded cards) bounds "
      "the gap. Do not use tier-B P values as census-grade truth.")
    A("")
    A("## Wide-cutoff recall (any candidate for the true work at ≤ "
      f"{WIDE_CUTOFF})")
    A("")
    A("| crop len | crops | true-work candidate exists | share |")
    A("|---|---|---|---|")
    for L in LENGTHS:
        n = n_by_len.get(L, 0)
        r = recall_wide.get(L, 0)
        A(f"| {L} | {n} | {r} | {r / max(1, n) * 100:.1f}% |")
    A("")
    A("This is the CEILING any acceptance rule can reach — crops whose true "
      "work never even forms a wide-cutoff candidate are structurally lost "
      "to the k=5 gram model at that length.")
    A("")
    A("## Calibrated P(same-work) — isotonic per length "
      f"(train {n_train:,} rows / holdout {n_hold:,} rows, "
      f"{n_hold_works} held-out works)")
    A("")
    for L in LENGTHS:
        if L not in model:
            A(f"**len {L}: too few rows to fit.**")
            continue
        A(f"**len {L}** — density → P (step knots, n per block):")
        A("")
        A("| density ≤ | P(same-work) | n |")
        A("|---|---|---|")
        for max_d, p, n in model[L]:
            A(f"| {max_d:.3f} | {p:.3f} | {n} |")
        A("")
    A("### Holdout reliability (predicted-P bucket vs empirical precision)")
    A("")
    A("| pred bucket | n | empirical |")
    A("|---|---|---|")
    for k, v in reliability.items():
        A(f"| {k} | {v['n']} | {v['empirical']} |")
    A("")
    A("## Contamination / operating-point sweep")
    A("")
    A("Per length: acceptance threshold → recall of the true work (per crop), "
      "share of accepted candidate rows that are WRONG-WORK, the "
      "**leave-work-out accept rate** (how often a wrong work would be "
      "accepted when the RIGHT one is ABSENT from the references — the "
      "failure mode the 82%-no-reference orphan population actually faces), "
      "and the decoy (pure-chance) accept rate. The three error columns are "
      "DIFFERENT populations — do not sum them. Pick tier B where all three "
      "are tolerable for a human-reviewed discovery list.")
    A("")
    for L in LENGTHS:
        A(f"**len {L}**")
        A("")
        A("| dens ≤ | recall | accepted | wrong | wrong-share | "
          "absent-work-accept | decoy-accept |")
        A("|---|---|---|---|---|---|---|")
        for row in sweeps.get(L, []):
            A(f"| {row['thresh']:.2f} | {row['recall']:.3f} | "
              f"{row['n_accepted']} | {row['n_wrong']} | "
              f"{row['wrong_share']:.3f} | {row['absent_work_accept']:.4f} | "
              f"{row['decoy_accept']:.4f} |")
        A("")
    A("### Proposed tier-B operating points")
    A("")
    A("| len | max density for P ≥ " +
      " | max density for P ≥ ".join(str(p) for p in P_TARGETS) + " |")
    A("|---|" + "---|" * len(P_TARGETS))
    for L in LENGTHS:
        if L not in model:
            continue
        cells = []
        for pt in P_TARGETS:
            d_ok = None
            for max_d, p, _n in model[L]:
                if p >= pt:
                    d_ok = max_d
            cells.append(f"{d_ok:.3f}" if d_ok is not None else "—")
        A(f"| {L} | " + " | ".join(cells) + " |")
    A("")
    A("## Stress test — Hillel's graded real cards vs predicted P")
    A("")
    A("**NOT a reliability sample** (Codex plan-gate: these cards were "
      "selection-biased and partially plan-adaptive). Used only as a stress "
      "test: cards Hillel judged correct must not receive absurdly low P, and "
      "vice versa. The true reliability instrument is the holdout split above "
      "plus a FRESH blinded deck drawn from tier-B probability buckets after "
      "Map v2.")
    A("")
    if external:
        A("| set | len | density | predicted P | human verdict | correct? |")
        A("|---|---|---|---|---|---|")
        for e in sorted(external, key=lambda x: (x['set'], -x['pred'])):
            A(f"| {e['set']} | {e['len']} | {e['dens']:.3f} | {e['pred']:.3f} "
              f"| {e['grade']} | {'✓' if e['human_correct'] else '✗'} |")
        n_ok = sum(e['human_correct'] for e in external)
        mean_p = sum(e['pred'] for e in external) / len(external)
        A("")
        A(f"Graded cards: {len(external)}; human-correct {n_ok} "
          f"({n_ok/len(external)*100:.0f}%); mean predicted P {mean_p:.3f}. "
          "If empirical correctness sits well ABOVE predicted P, the "
          "calibration is conservative on real data (safe); well BELOW → "
          "optimism bias is material → run the noise-injection arm before "
          "trusting tier-B P values.")
    else:
        A("(no graded cards could be mapped — see run log)")
    A("")
    return L_


def refit_from_rows(rows_path, tag, final, fit_by_alen, out_model, out_md,
                    noise_rate):
    """CPU-light path: fit the model straight from an existing cal1_rows_*.json
    dump (no corpus/reference rebuild). The labeled candidate rows carry
    everything the fit needs; the decoy arm (singleton null) is reused verbatim
    from the crop-len `final` model because the alen re-fit does not touch it
    (task point d). Writes out_model (PModel-loadable) + out_md (report +
    self-validation grid vs the `final` model)."""
    t0 = time.time()
    log(f"REFIT-FROM-ROWS: loading {rows_path} (skipping corpus/reference)")
    dump = json.load(open(rows_path, encoding='utf-8'))
    rows = dump['rows']
    decoy_rows = dump.get('decoy_rows', [])
    log(f"loaded {len(rows):,} labeled rows, {len(decoy_rows):,} decoy rows "
        f"({time.time() - t0:.0f}s)")

    # decoy summaries recoverable from the dump (for the report only)
    decoy_best = defaultdict(list)
    n_dec_singletons = Counter()
    for dr in decoy_rows:
        decoy_best[dr['len']].append(dr['best_dens'])
        if dr['n_works'] == 1:
            n_dec_singletons[dr['len']] += 1

    # Singleton null + its per-length denominator (which INCLUDES zero-candidate
    # decoys the dump does not carry) are reused verbatim from the crop-len
    # `final` model: the decoy arm is unchanged by the alen re-fit, so this
    # keeps the singleton band byte-identical to the deployed model.
    final_path = OUT_MODEL.format(tag='final')
    reused_meta = {}
    try:
        fd = json.load(open(final_path, encoding='utf-8'))
        singleton_null = {int(L): v
                          for L, v in fd.get('singleton_null', {}).items()}
        decoy_singleton_rate = fd.get('decoy_singleton_rate', {})
        reused_meta = fd.get('meta', {})
        log(f"reused singleton_null ({len(singleton_null)} bins) + "
            f"decoy_singleton_rate from {final_path}")
    except Exception as e:                                    # noqa: BLE001
        log(f"could not reuse final singleton null ({e!r}); rebuilding from "
            "decoy_rows with an approximate denominator (candidate-bearing "
            "decoys only — slightly conservative)")
        n_decoy_by_len = Counter({L: len(v) for L, v in decoy_best.items()})
        singleton_null = build_singleton_null(decoy_best, n_decoy_by_len)
        decoy_singleton_rate = {
            str(L): round(n_dec_singletons.get(L, 0)
                          / max(1, n_decoy_by_len[L]), 4)
            for L in n_decoy_by_len}

    model, reliability, train, hold, n_hold_works = fit_calibration(
        rows, by_alen=fit_by_alen)
    n_train, n_hold = len(train), len(hold)
    margin_model, margin_counts = fit_margin_model(train, by_alen=fit_by_alen)
    reliability_margin = margin_reliability(hold, model, margin_model,
                                            singleton_null,
                                            by_alen=fit_by_alen)
    log(f"fit done ({time.time() - t0:.0f}s); margin bands: "
        f"{ {b: sorted(bins) for b, bins in margin_model.items()} }")

    meta = {'stage': tag, 'noise_rate': noise_rate, 'lengths': LENGTHS,
            'wide_cutoff': WIDE_CUTOFF,
            'n_pages': reused_meta.get('n_pages'),
            'n_crops': reused_meta.get('n_crops'),
            'n_rows': len(rows), 'seed': RNG_SEED,
            'chunk_primary': CHUNK_PRIMARY,
            'truth_relation': ('version-group' if final else 'work-id'),
            'fit_by': ('alen' if fit_by_alen else 'crop-len'),
            'refit_from_rows': rows_path,
            'generated': time.strftime('%Y-%m-%d %H:%M')}
    json.dump({'meta': meta,
               'model': {str(L): model[L] for L in model},
               'margin_model': {b: {str(L): v for L, v in bins.items()}
                                for b, bins in margin_model.items()},
               'margin_fit_counts': margin_counts,
               'singleton_null': {str(L): v
                                  for L, v in singleton_null.items()},
               'decoy_singleton_rate': decoy_singleton_rate,
               'reliability': reliability,
               'reliability_margin': reliability_margin},
              open(out_model, 'w', encoding='utf-8'), indent=1)
    log(f"wrote {out_model}")

    # ---- focused report ----
    L_ = []
    A = L_.append
    label = 'FINAL2 (alen-bin refit)' if fit_by_alen else tag
    A(f"# CAL-1 **{label}** — P(same-work | density, length) [refit-from-rows]")
    A("")
    A(f"Generated {time.strftime('%Y-%m-%d %H:%M')}; **refit from "
      f"`{rows_path}`** (no corpus/reference rebuild) in "
      f"{(time.time() - t0) / 60:.1f} min. train {n_train:,} / holdout "
      f"{n_hold:,} rows, {n_hold_works} held-out works. "
      f"Truth relation = {meta['truth_relation']}.")
    A("")
    if fit_by_alen:
        A("**Isotonic re-fit by ALEN bin** — audit §3 root-cause remedy. Every "
          "margin-band curve and the pooled fallback are fit by the row's "
          "nearest ALEN bin (edges [40,60,80,100,150,200,300]) — the SAME "
          "feature `p_deploy` / `PModel` deploy on (fit / validate / deploy "
          "now share one feature). The two small-margin bands "
          f"({', '.join(sorted(POOL_BANDS))}) pool alen bins whose cell has "
          f"< {EFFECTIVE_WORKS_FLOOR} distinct (effective) works, borrowing a "
          "band-pooled curve fit on the union. Singleton null + decoy arm "
          "reused verbatim from the crop-len `final` model (unchanged by the "
          "re-fit). Byte-compatible with `mapv2_track1_run.PModel`.")
        A("")
    # margin-band fit-count table
    A("## Margin-band fit inventory (rows / effective works per alen bin)")
    A("")
    A("| band | alen | rows | eff works | fitted |")
    A("|---|---|---|---|---|")
    for band in MARGIN_BANDS:
        fitted_bins = set(margin_model.get(band, {}))
        for L in LENGTHS:
            key = f"{band}/{L}"
            c = margin_counts.get(key)
            if isinstance(c, dict):
                nr, ew = c['rows'], c['eff_works']
            else:
                nr, ew = c, '—'
            A(f"| {band} | {L} | {nr} | {ew} | "
              f"{'yes' if L in fitted_bins else '—'} |")
    A("")
    A("### Holdout reliability of the deployment-composed lookup "
      "(predicted-P bucket)")
    A("")
    A("| pred bucket | n | empirical |")
    A("|---|---|---|")
    for k, v in reliability_margin.items():
        A(f"| {k} | {v['n']} | {v['empirical']} |")
    A("")
    grid_lines, worsened, new_grid, old_grid = grid_section(
        hold, (model, margin_model, singleton_null), final_path)
    L_ += grid_lines
    open(out_md, 'w', encoding='utf-8').write('\n'.join(L_))
    log(f"wrote {out_md}")
    log(f"refit TOTAL {((time.time() - t0) / 60):.1f} min; "
        f"regressed buckets (>0.05): {len(worsened)}")


def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument('--tag', default=STAGE_DEFAULT,
                    help="output stage tag: 'pilot' (pre-REF-2, default), "
                         "'final' (frozen Map-v2 state, crop-len fit), or "
                         "'final2' (frozen state, ALEN-bin fit — audit §3)")
    ap.add_argument('--noise-rate', type=float, default=0.0,
                    help="inject empirical HTR noise into crops at this "
                         "per-letter rate (e.g. 0.15); tag gets -n<pct>")
    ap.add_argument('--fit-by-alen', dest='fit_by_alen', action='store_true',
                    help="fit the per-band + pooled curves by nearest ALEN "
                         "bin instead of crop len (audit §3). DEFAULT: on for "
                         "--tag final2, off for every other tag.")
    ap.add_argument('--no-fit-by-alen', dest='fit_by_alen',
                    action='store_false',
                    help="force crop-len fit even under --tag final2.")
    ap.set_defaults(fit_by_alen=None)
    ap.add_argument('--refit-from-rows', default=None, metavar='PATH',
                    help="path to an existing cal1_rows_*.json dump; SKIP all "
                         "corpus/reference work and fit straight from the "
                         "dumped rows (CPU-light — no 6-min rebuild). Reuses "
                         "the singleton null + decoy arm from the crop-len "
                         "`final` model (unchanged by the alen re-fit).")
    args = ap.parse_args()
    final = args.tag.startswith('final')
    # FIT_BY_ALEN gating: on for 'final2' by default, else off; --fit-by-alen /
    # --no-fit-by-alen override. Pilot / plain-final stay crop-len (byte-ident).
    fit_by_alen = (args.fit_by_alen if args.fit_by_alen is not None
                   else args.tag == 'final2')
    if args.noise_rate > 0:
        args.tag = f"{args.tag}-n{int(round(args.noise_rate * 100))}"
    out_model = OUT_MODEL.format(tag=args.tag)
    out_rows = OUT_ROWS.format(tag=args.tag)
    out_md = OUT_MD.format(tag=args.tag)

    if args.refit_from_rows:
        refit_from_rows(args.refit_from_rows, args.tag, final, fit_by_alen,
                        out_model, out_md, args.noise_rate)
        return

    t0 = time.time()
    con = sqlite3.connect(DB)
    con.execute("PRAGMA busy_timeout=120000")

    if final:
        log(f"FINAL stage: reference={REF_V2}, masks={MASKS_V2}, "
            f"text={DB_V2} (provenance='htr' only), truth=version-groups")
        works, wid_to_wi, ref_tuple = build_reference(REF_V2, MASKS_V2)
        tcon = sqlite3.connect(DB_V2)
        tcon.execute("PRAGMA busy_timeout=120000")
        pages, shortfall = sample_single_work_pages(con, tcon,
                                                    require_htr=True)
        tcon.close()
    else:
        works, wid_to_wi, ref_tuple = build_reference()
        pages, shortfall = sample_single_work_pages(con)
    con.close()
    rnd = random.Random(RNG_SEED + 1)
    crops = make_crops(pages, rnd)
    log(f"{len(crops):,} crops")
    if args.noise_rate > 0:
        nm = load_noise_model()
        nrnd = random.Random(RNG_SEED + 5)
        for c in crops:
            c['stream'] = perturb(c['stream'], args.noise_rate, nm, nrnd)
        log(f"noise injected at rate {args.noise_rate} "
            f"(empirical HTR profile, {CONF_MATRIX})")

    # labeled arm
    results, _ = query_batch([c['stream'] for c in crops], ref_tuple,
                             wide_cutoff_frac=WIDE_CUTOFF)
    rows, recall_wide, n_by_len, lwo_wrong = collect_rows(
        crops, results, wid_to_wi, works=works, use_vgroups=final)
    log(f"labeled candidate rows: {len(rows):,}"
        + (" (version-group truth relation)" if final else ""))

    # decoy arm (primary protocol; optimistic bracket on a half-sample).
    # FULL candidate sets recorded (FRAG2-PLAN singleton fix): decoy_best
    # feeds the singleton null CDF; decoy_rows measure the chance-singleton
    # rate directly.
    decoy_best = defaultdict(list)
    decoy_rows = []
    n_decoy_by_len = Counter()
    drnd = random.Random(RNG_SEED + 2)
    dstreams = [chunk_shuffle(c['stream'], CHUNK_PRIMARY, drnd) for c in crops]
    dresults, _ = query_batch(dstreams, ref_tuple,
                              wide_cutoff_frac=WIDE_CUTOFF)
    n_dec_singletons = Counter()
    for c, cands in zip(crops, dresults):
        n_decoy_by_len[c['length']] += 1
        bp = best_per_work(cands)
        if bp:
            decoy_best[c['length']].append(min(d for d, _a in bp.values()))
            # FULL best-per-work candidate set persisted (Codex code-gate
            # MEDIUM 5): the singleton-null audit/refit path needs the
            # per-candidate (density, alen) distribution, not just the best.
            decoy_rows.append({'len': c['length'], 'n_works': len(bp),
                               'best_dens': round(min(
                                   d for d, _a in bp.values()), 4),
                               'cands': sorted(
                                   [round(d, 4), int(a)]
                                   for d, a in bp.values())})
            if len(bp) == 1:
                n_dec_singletons[c['length']] += 1
    n_dec_hits = sum(len(v) for v in decoy_best.values())
    log(f"decoys (CHUNK={CHUNK_PRIMARY}): {len(dstreams):,} queries, "
        f"{n_dec_hits:,} with any candidate; chance singletons by len: "
        f"{dict(n_dec_singletons)}")

    model, reliability, train, hold, n_hold_works = fit_calibration(
        rows, by_alen=fit_by_alen)
    n_train, n_hold = len(train), len(hold)
    margin_model, margin_counts = fit_margin_model(train, by_alen=fit_by_alen)
    singleton_null = build_singleton_null(decoy_best, n_decoy_by_len)
    reliability_margin = margin_reliability(hold, model, margin_model,
                                            singleton_null,
                                            by_alen=fit_by_alen)
    sweeps = sweep(rows, decoy_best, lwo_wrong, n_by_len, n_decoy_by_len)
    external = load_graded_external(model)

    json.dump({'meta': {'stage': args.tag, 'noise_rate': args.noise_rate,
                        'lengths': LENGTHS,
                        'wide_cutoff': WIDE_CUTOFF,
                        'n_pages': len(pages), 'n_crops': len(crops),
                        'n_rows': len(rows), 'seed': RNG_SEED,
                        'chunk_primary': CHUNK_PRIMARY,
                        'truth_relation': ('version-group' if final
                                           else 'work-id'),
                        'generated': time.strftime('%Y-%m-%d %H:%M')},
               'model': {str(L): model[L] for L in model},
               'margin_model': {b: {str(L): v for L, v in bins.items()}
                                for b, bins in margin_model.items()},
               'margin_fit_counts': margin_counts,
               'singleton_null': {str(L): v
                                  for L, v in singleton_null.items()},
               'decoy_singleton_rate': {
                   str(L): round(n_dec_singletons.get(L, 0)
                                 / max(1, n_decoy_by_len[L]), 4)
                   for L in n_decoy_by_len},
               'reliability': reliability,
               'reliability_margin': reliability_margin},
              open(out_model, 'w', encoding='utf-8'), indent=1)
    json.dump({'rows': rows, 'decoy_rows': decoy_rows},
              open(out_rows, 'w', encoding='utf-8'))
    lines = write_report(shortfall, len(pages), len(crops), recall_wide,
                         n_by_len, rows, model, reliability, n_train, n_hold,
                         n_hold_works, sweeps,
                         f"CHUNK={CHUNK_PRIMARY} (n={len(dstreams):,})",
                         external, time.time() - t0)
    if final:
        lines[0] = lines[0].replace('**PILOT**', '**FINAL**')
        lines[2] = ("**FINAL run on the frozen Map-v2 state** (ref_corpus_v2 "
                    "+ ref_canon_masks_v2 + search_text db, provenance='htr' "
                    "pages only; truth relation = version-group identity). "
                    "This is the model tier B deploys.")
    if fit_by_alen:
        lines[0] = lines[0].replace('**FINAL**', '**FINAL2 (alen-bin refit)**')
        lines[2] = ("**FINAL2 — isotonic re-fit by ALEN bin** (audit §3 "
                    "root-cause remedy) on the frozen Map-v2 state. Every "
                    "margin-band curve and the pooled fallback are fit by the "
                    "row's nearest ALEN bin — the SAME feature p_deploy / "
                    "PModel deploy on. The two small-margin bands "
                    f"({', '.join(sorted(POOL_BANDS))}) pool alen bins whose "
                    f"cell has < {EFFECTIVE_WORKS_FLOOR} effective works. "
                    "Singleton null + decoy arm unchanged.")
    lines += [
        "", "## Margin-band model (deployment-composed) + singleton null",
        "",
        f"Margin bands fitted (>= {MARGIN_FIT_FLOOR} rows): "
        f"{ {b: sorted(bins) for b, bins in margin_model.items()} }",
        f"Chance-singleton rate per length (decoy arm): "
        f"{ {L: round(n_dec_singletons.get(L, 0) / max(1, n_decoy_by_len[L]), 4) for L in sorted(n_decoy_by_len)} }",
        "",
        "Singletons are scored by the decoy-anchored null "
        "P = 1 - DecoyBestCDF(len, dens) — NOT by synthetic isotonic "
        "(unfittable band; see FRAG2-PLAN finding 2026-07-10).",
        "",
        "### Holdout reliability of the deployment-composed lookup "
        "(margin bands + singleton null + pooled fallback)", "",
        "| pred bucket | n | empirical |", "|---|---|---|",
    ] + [f"| {k} | {v['n']} | {v['empirical']} |"
         for k, v in reliability_margin.items()]
    if fit_by_alen:
        grid_lines, _worse, _ng, _og = grid_section(
            hold, (model, margin_model, singleton_null),
            OUT_MODEL.format(tag='final'))
        lines += ["", ""] + grid_lines
    open(out_md, 'w', encoding='utf-8').write('\n'.join(lines))
    log(f"wrote {out_model}")
    log(f"wrote {out_md}")
    log(f"TOTAL {((time.time() - t0) / 60):.1f} min")


if __name__ == '__main__':
    main()
