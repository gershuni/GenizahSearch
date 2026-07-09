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

def sample_single_work_pages(con):
    """Pages with exactly ONE distinct live work label, confident + high
    coverage; returns per page the largest labeled span's stream slice."""
    t0 = time.time()
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
            trow = con.execute("SELECT text FROM pages WHERE page_id=?",
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
        f"({time.time() - t0:.0f}s); shortfalls: {shortfall or 'none'}")
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
# candidate rows
# =====================================================================

def best_per_work(cands):
    """[(work_idx, alen, dens)] -> {work_idx: (dens, alen)} best per work."""
    best = {}
    for wi, alen, dens in cands:
        if wi not in best or dens < best[wi][0]:
            best[wi] = (dens, alen)
    return best


def collect_rows(crops, results, wid_to_wi):
    """One row per (crop, distinct candidate work) — ALL works, no rank cap.
    Also returns per-crop best WRONG-work density (the leave-work-out arm:
    dropping the true work's candidates post-hoc is identical, on the accept
    surface, to removing that work from the index — measures how often a
    wrong work would be accepted when the right one is ABSENT)."""
    rows = []
    recall_wide = Counter()
    n_by_len = Counter()
    lwo_wrong = defaultdict(list)          # length -> per-crop min wrong dens
    for ci, (c, cands) in enumerate(zip(crops, results)):
        n_by_len[c['length']] += 1
        true_wi = wid_to_wi.get(c['work_id'])
        bp = best_per_work(cands)
        ranked = sorted(bp.items(), key=lambda kv: kv[1][0])
        if true_wi in bp:
            recall_wide[c['length']] += 1
        wrong_ds = [dens for wi, (dens, _a) in ranked if wi != true_wi]
        if wrong_ds:
            lwo_wrong[c['length']].append(min(wrong_ds))
        for rank, (wi, (dens, alen)) in enumerate(ranked):
            rows.append({'crop_id': ci, 'len': c['length'], 'alen': alen,
                         'dens': dens, 'rank': rank,
                         'correct': int(wi == true_wi),
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


def work_weights(rows):
    """Per-(length, true_work) equal-total-weight: each source work
    contributes weight 1 per length bin regardless of how many candidate rows
    its crops generated (Codex HIGH: raw rows overweight candidate-rich
    works). Returns {id(row-key)->w} keyed by (len, true_work)."""
    n_per = Counter((r['len'], r['true_work']) for r in rows)
    return {k: 1.0 / n for k, n in n_per.items()}


def p_lookup(model_bins, length, dens):
    """Step-function lookup with clamping; nearest length bin."""
    lb = min(model_bins, key=lambda L: abs(L - length))
    knots = model_bins[lb]            # [(max_dens, p, n)] density-ascending
    for max_d, p, _n in knots:
        if dens <= max_d:
            return p
    return knots[-1][1] if knots else 0.0


def fit_calibration(rows):
    """Work-granular split -> per-length isotonic model + holdout reliability."""
    works = sorted({r['true_work'] for r in rows})
    rnd = random.Random(RNG_SEED + 7)
    rnd.shuffle(works)
    n_hold = max(1, int(len(works) * HOLDOUT_FRAC))
    hold_works = set(works[:n_hold])
    train = [r for r in rows if r['true_work'] not in hold_works]
    hold = [r for r in rows if r['true_work'] in hold_works]

    tw = work_weights(train)
    model = {}
    for L in LENGTHS:
        pts = [(r['dens'], r['correct'], tw[(L, r['true_work'])])
               for r in train if r['len'] == L]
        if len(pts) >= 20:
            model[L] = pava_decreasing(pts)
    # reliability on holdout: bucket by predicted P, same per-work weighting
    hw = work_weights(hold)
    rel = defaultdict(lambda: [0.0, 0.0, 0])
    for r in hold:
        if r['len'] not in model:
            continue
        p = p_lookup(model, r['len'], r['dens'])
        b = min(9, int(p * 10))
        w = hw[(r['len'], r['true_work'])]
        rel[b][0] += r['correct'] * w
        rel[b][1] += w
        rel[b][2] += 1
    reliability = {f"{b/10:.1f}-{(b+1)/10:.1f}":
                   {'n': n, 'pred_mid': (b + 0.5) / 10,
                    'empirical': round(kw / ww, 3)}
                   for b, (kw, ww, n) in sorted(rel.items()) if n > 0}
    return model, reliability, len(train), len(hold), len(hold_works)


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


def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument('--tag', default=STAGE_DEFAULT,
                    help="output stage tag: 'pilot' (pre-REF-2, default) or "
                         "'final' (frozen Map-v2 state)")
    args = ap.parse_args()
    out_model = OUT_MODEL.format(tag=args.tag)
    out_rows = OUT_ROWS.format(tag=args.tag)
    out_md = OUT_MD.format(tag=args.tag)

    t0 = time.time()
    con = sqlite3.connect(DB)
    con.execute("PRAGMA busy_timeout=120000")

    works, wid_to_wi, ref_tuple = build_reference()

    pages, shortfall = sample_single_work_pages(con)
    con.close()
    rnd = random.Random(RNG_SEED + 1)
    crops = make_crops(pages, rnd)
    log(f"{len(crops):,} crops")

    # labeled arm
    results, _ = query_batch([c['stream'] for c in crops], ref_tuple,
                             wide_cutoff_frac=WIDE_CUTOFF)
    rows, recall_wide, n_by_len, lwo_wrong = collect_rows(
        crops, results, wid_to_wi)
    log(f"labeled candidate rows: {len(rows):,}")

    # decoy arm (primary protocol; optimistic bracket on a half-sample)
    decoy_best = defaultdict(list)
    n_decoy_by_len = Counter()
    drnd = random.Random(RNG_SEED + 2)
    dstreams = [chunk_shuffle(c['stream'], CHUNK_PRIMARY, drnd) for c in crops]
    dresults, _ = query_batch(dstreams, ref_tuple,
                              wide_cutoff_frac=WIDE_CUTOFF)
    for c, cands in zip(crops, dresults):
        n_decoy_by_len[c['length']] += 1
        bp = best_per_work(cands)
        if bp:
            decoy_best[c['length']].append(min(d for d, _a in bp.values()))
    n_dec_hits = sum(len(v) for v in decoy_best.values())
    log(f"decoys (CHUNK={CHUNK_PRIMARY}): {len(dstreams):,} queries, "
        f"{n_dec_hits:,} with any candidate")

    model, reliability, n_train, n_hold, n_hold_works = fit_calibration(rows)
    sweeps = sweep(rows, decoy_best, lwo_wrong, n_by_len, n_decoy_by_len)
    external = load_graded_external(model)

    json.dump({'meta': {'stage': args.tag, 'lengths': LENGTHS,
                        'wide_cutoff': WIDE_CUTOFF,
                        'n_pages': len(pages), 'n_crops': len(crops),
                        'n_rows': len(rows), 'seed': RNG_SEED,
                        'chunk_primary': CHUNK_PRIMARY,
                        'generated': time.strftime('%Y-%m-%d %H:%M')},
               'model': {str(L): model[L] for L in model},
               'reliability': reliability},
              open(out_model, 'w', encoding='utf-8'), indent=1)
    json.dump(rows, open(out_rows, 'w', encoding='utf-8'))
    lines = write_report(shortfall, len(pages), len(crops), recall_wide,
                         n_by_len, rows, model, reliability, n_train, n_hold,
                         n_hold_works, sweeps,
                         f"CHUNK={CHUNK_PRIMARY} (n={len(dstreams):,})",
                         external, time.time() - t0)
    open(out_md, 'w', encoding='utf-8').write('\n'.join(lines))
    log(f"wrote {out_model}")
    log(f"wrote {out_md}")
    log(f"TOTAL {((time.time() - t0) / 60):.1f} min")


if __name__ == '__main__':
    main()
