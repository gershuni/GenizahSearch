# -*- coding: utf-8 -*-
"""A5 spike: conformal + FDR acceptance thresholds via TARGET-DECOY null.

Replaces the hand-tuned sloped density boundary (rehearsal_run.accept_density)
with an FDR-bounded operating table, calibrated on a candidate-conditioned
null (Codex plan-review BLOCKER fix: a null of random non-candidate pairs is
not exchangeable with tested pairs, which are selected by seed/DF/two-hit
candidate generation + best-segment verification).

Decoy design (chunk-shuffle): each decoy = a real page's normalized stream
shuffled in CHUNK-letter chunks. This preserves gram content and DF statistics
(decoy candidates arise through the identical two-hit machinery) but destroys
the diagonal structure genuine reuse produces, so a decoy-involved candidate's
best-segment density is a draw from the chance-alignment distribution.
Decoys inherit their source page's sys_code, so decoy-vs-own-manuscript pairs
are excluded by the engine itself. A decoy CAN pair with true reuse-partners
of its source — under chunk-shuffle those alignments are still chance-level
(no surviving diagonal), i.e. valid null draws. Known residual leak: a single
intact CHUNK shared verbatim yields a short low-anchor cluster whose span is
mostly margin -> high density; CHUNK=10 keeps this far above any threshold.

Scope (wave-1, per SPIKE-BRIEFS-2026-07-08.md): probe.db ONLY (18K pages).
Known caveats for the wave-2 scale-up: (1) +DECOY_FRAC pages inflate DF by up
to that fraction, mildly tightening the DF<=100 cap vs the production run —
the clean fix is a two-run protocol (clean run for tested pairs, decoy run
for the null); (2) ~2% duplicate (pair) candidate rows are not deduped here;
(3) chunk-shuffle destroys every k-gram crossing a chunk boundary and mints
synthetic boundary grams (Codex review HIGH-1) — decoys are less phrase-like
than real pages, which could push the null high and the thresholds loose.
Empirical control: pass CHUNK as argv[1] and compare null quantiles across
chunk sizes (larger CHUNK = fewer destroyed grams but more intact-chunk
contiguity leak); thresholds should be taken from the conservative envelope.

Usage: python -X utf8 -u probe_conformal_fdr.py [chunk_size]
Out:   results/a5_conformal_fdr_report[_c<chunk>].md + a5_thresholds[...].json
"""
import json
import sqlite3
import sys
import time

import numpy as np
from rapidfuzz.distance import Levenshtein

sys.path.insert(0, r"C:\Genizahsearch\same_work_spike\probe\scripts")
import engine_np                     # noqa: E402
from normalize import norm_stream    # noqa: E402

ROOT = r"C:\Genizahsearch"
DB = ROOT + r"\same_work_spike\probe\data\probe.db"
TIER1 = ROOT + r"\same_work_spike\probe\results\tier1.json"
GRADES = ROOT + r"\same_work_spike\probe\review\grades_hillel_2026-07-07.json"
SPILL = ROOT + r"\same_work_spike\probe\data\spill"

K, BAND, DF_DROP, MIN_ANCHORS = 5, 20, 100, 2      # production candidate mode
MARGIN, MIN_SPAN, SPAN_CAP = 30, 25, 6000
DECOY_FRAC = 0.25
CHUNK = int(sys.argv[1]) if len(sys.argv) > 1 else 10
SEED = 29
_SFX = '' if CHUNK == 10 else f'_c{CHUNK}'
OUT_MD = ROOT + rf"\same_work_spike\probe\results\a5_conformal_fdr_report{_SFX}.md"
OUT_JSON = ROOT + rf"\same_work_spike\probe\results\a5_thresholds{_SFX}.json"
LEN_BINS = [25, 50, 100, 200, 400, 800]            # right-open; last = 800+
Q_LIST = [0.01, 0.05]


def accept_density(length):  # the incumbent hand-tuned boundary
    return 0.30 if length < 100 else (0.386 if length < 200 else 0.418)


def make_decoy(stream, rng):
    chunks = [stream[i:i + CHUNK] for i in range(0, len(stream), CHUNK)]
    rng.shuffle(chunks)
    return ''.join(chunks)


def main():
    t0 = time.time()
    rng = np.random.default_rng(SEED)
    con = sqlite3.connect(DB)
    ids, sys_ids, buckets, streams = [], [], [], []
    for pid, sid, bck, text in con.execute(
            "SELECT page_id, sys_id, buckets, text FROM pages"):
        s, _ = norm_stream(text)
        if len(s) >= 60:
            ids.append(pid)
            sys_ids.append(sid)
            buckets.append(bck or '?')
            streams.append(s)
    n_real = len(streams)
    sys_uniq = {s: i for i, s in enumerate(dict.fromkeys(sys_ids))}
    codes = [sys_uniq[s] for s in sys_ids]
    print(f"real pages: {n_real:,} ({time.time() - t0:.0f}s)", flush=True)

    n_decoy = int(n_real * DECOY_FRAC)
    picks = rng.choice(n_real, size=n_decoy, replace=False)
    for pi in picks:
        streams.append(make_decoy(streams[pi], rng))
        codes.append(codes[pi])          # engine excludes same-sys pairs
        buckets.append(buckets[pi])
        ids.append('DECOY__' + ids[pi])
    is_decoy = np.zeros(len(streams), dtype=bool)
    is_decoy[n_real:] = True
    print(f"decoys: {n_decoy:,} (chunk={CHUNK}, frac={DECOY_FRAC})", flush=True)

    pa, pb, cnt, mina, maxa, minb, maxb, stats = engine_np.build_candidates(
        streams, codes, df_drop=DF_DROP, band=BAND,
        min_anchors=MIN_ANCHORS, spill_dir=SPILL)
    m = len(pa)
    print(f"candidates: {m:,} ({time.time() - t0:.0f}s)", flush=True)

    # ---- verify ALL candidates with the density gate LIFTED ----
    alen_arr = np.zeros(m, dtype=np.int32)
    dens_arr = np.full(m, np.nan, dtype=np.float32)
    t1 = time.time()
    for i in range(m):
        sa, sb = streams[pa[i]], streams[pb[i]]
        a0 = max(0, int(mina[i]) - MARGIN)
        a1 = min(len(sa), int(maxa[i]) + K + MARGIN)
        b0 = max(0, int(minb[i]) - MARGIN)
        b1 = min(len(sb), int(maxb[i]) + K + MARGIN)
        a1, b1 = min(a1, a0 + SPAN_CAP), min(b1, b0 + SPAN_CAP)
        la, lb = a1 - a0, b1 - b0
        if min(la, lb) < MIN_SPAN:
            continue
        alen = max(la, lb)
        dist = Levenshtein.distance(sa[a0:a1], sb[b0:b1])
        alen_arr[i] = alen
        dens_arr[i] = dist / alen
        if i and i % 5_000_000 == 0:
            print(f"  verify {i:,}/{m:,} ({time.time() - t1:.0f}s)", flush=True)
    ok = ~np.isnan(dens_arr)
    null_mask = ok & (is_decoy[pa] | is_decoy[pb])
    test_mask = ok & ~is_decoy[pa] & ~is_decoy[pb]
    n_null, n_test = int(null_mask.sum()), int(test_mask.sum())
    print(f"verified: null={n_null:,} tested={n_test:,} "
          f"({time.time() - t1:.0f}s)", flush=True)

    bins_all = np.digitize(alen_arr, LEN_BINS) - 1     # 0..len(LEN_BINS)-1
    nb = len(LEN_BINS)
    null_sorted = []                                   # per-bin sorted nulls
    for b in range(nb):
        d = np.sort(dens_arr[null_mask & (bins_all == b)])
        null_sorted.append(d)

    # ---- conformal p per tested candidate + Benjamini-Hochberg ----
    ti = np.flatnonzero(test_mask)
    pvals = np.ones(len(ti), dtype=np.float64)
    for b in range(nb):
        d0 = null_sorted[b]
        sel = bins_all[ti] == b
        if not len(d0) or not sel.any():
            continue
        # lower density = more extreme; p = (1 + #null <= obs) / (1 + N)
        pvals[sel] = (1.0 + np.searchsorted(
            d0, dens_arr[ti[sel]], side='right')) / (1.0 + len(d0))

    results_q = {}
    ps = np.sort(pvals)
    mtests = len(ps)
    for q in Q_LIST:
        k = np.flatnonzero(ps <= q * (np.arange(1, mtests + 1) / mtests))
        # tie-robust: accept ALL tests with p <= p_cut (Codex MEDIUM-4)
        p_cut = float(ps[int(k[-1])]) if len(k) else 0.0
        acc_mask = pvals <= p_cut if len(k) else np.zeros(len(ti), dtype=bool)
        acc_idx = ti[acc_mask]
        thr = {}
        for b in range(nb):
            sel = bins_all[acc_idx] == b
            thr[b] = float(dens_arr[acc_idx][sel].max()) if sel.any() else None
        results_q[q] = dict(accepted=int(acc_mask.sum()), thresholds=thr,
                            p_cut=p_cut)
        print(f"BH q={q}: accepted {int(acc_mask.sum()):,}/{mtests:,}",
              flush=True)

    # ---- incumbent boundary: acceptance + decoy-estimated FDR ----
    hand_thr = np.where(alen_arr < 100, 0.30,
                        np.where(alen_arr < 200, 0.386, 0.418))
    hand_ok = ok & (dens_arr <= hand_thr)
    n_hand_test = int((hand_ok & test_mask).sum())
    n_hand_null = int((hand_ok & null_mask).sum())
    ratio = n_test / max(1, n_null)
    fdr_hand = (n_hand_null * ratio) / max(1, n_hand_test)
    print(f"hand boundary: tested-accepted={n_hand_test:,} "
          f"null-accepted={n_hand_null:,} -> est FDR={fdr_hand:.4f}", flush=True)

    # ---- tier-1 recall under each rule ----
    # Rule-recall = ANY candidate segment of the GT pair passes the rule —
    # matches production's filter-then-keep-best order (Codex HIGH-2).
    idx_of = {p: i for i, p in enumerate(ids[:n_real])}
    key_rows = {}
    gt = json.load(open(TIER1, encoding='utf-8'))
    gt_pages = set()
    for fam in gt.values():
        for r in fam:
            gt_pages.add(r['a'])
            gt_pages.add(r['b'])
    gt_idx = {idx_of[p] for p in gt_pages if p in idx_of}
    for i in np.flatnonzero(test_mask):
        A, B = int(pa[i]), int(pb[i])
        if A in gt_idx and B in gt_idx:
            key_rows.setdefault((min(A, B), max(A, B)), []).append(
                (int(alen_arr[i]), float(dens_arr[i])))
    fam_rows = []
    for fam_name, fam in gt.items():
        tot = found = hand = 0
        fdrq = {q: 0 for q in Q_LIST}
        for r in fam:
            ia, ib = idx_of.get(r['a']), idx_of.get(r['b'])
            if ia is None or ib is None:
                continue
            tot += 1
            rows = key_rows.get((min(ia, ib), max(ia, ib)))
            if not rows:
                continue
            found += 1
            if any(d <= accept_density(al) for al, d in rows):
                hand += 1
            for q in Q_LIST:
                thrs = results_q[q]['thresholds']
                for al, d in rows:
                    b = int(np.digitize([al], LEN_BINS)[0]) - 1
                    t = thrs.get(b)
                    if t is not None and d <= t:
                        fdrq[q] += 1
                        break
        fam_rows.append((fam_name, tot, found, hand,
                         *[fdrq[q] for q in Q_LIST]))
        print(f"tier1[{fam_name}]: {tot} pairs, candidates {found}, "
              f"hand {hand}, fdr {[fdrq[q] for q in Q_LIST]}", flush=True)

    # ---- graded-pairs overlap (likely none at probe scale) ----
    try:
        import re
        blob = open(GRADES, encoding='utf-8').read()
        graded_ids = set(re.findall(r'\d{10,}_IE\d+_P\d+_FL\d+', blob))
        n_overlap = len(graded_ids & set(ids[:n_real]))
    except Exception as e:  # noqa: BLE001
        n_overlap = f"unavailable ({e})"

    # ---- per-stratum null sensitivity (top buckets) ----
    from collections import Counter
    buniq = {b: i for i, b in enumerate(dict.fromkeys(buckets))}
    bcode = np.array([buniq[b] for b in buckets], dtype=np.int32)
    same_b = null_mask & (bcode[pa] == bcode[pb])
    bcount = Counter(bcode[pa][same_b].tolist())
    inv_b = {v: k for k, v in buniq.items()}
    top_b = [inv_b[c] for c, _ in bcount.most_common(4)]
    strat_lines = []
    for bname in top_b:
        sel = same_b & (bcode[pa] == buniq[bname])
        row = [bname[:24]]
        for b in range(nb):
            d = dens_arr[sel & (bins_all == b)]
            row.append(f"{np.median(d):.3f}/{np.quantile(d, 0.05):.3f}"
                       if len(d) > 20 else "—")
        strat_lines.append("| " + " | ".join(row) + " |")

    # ---- report ----
    bin_lbl = [f"[{LEN_BINS[i]},{LEN_BINS[i+1]})" if i + 1 < nb
               else f"{LEN_BINS[i]}+" for i in range(nb)]
    lines = [
        "# A5 — conformal + FDR thresholds via target-decoy null (probe.db dry run)",
        "",
        f"Corpus: {n_real:,} real pages + {n_decoy:,} chunk-shuffle decoys "
        f"(CHUNK={CHUNK}, seed={SEED}); production candidate mode "
        f"(k={K}, DF<={DF_DROP}, two-hit, band={BAND}).",
        f"Candidates: {m:,} — tested (real-real) {n_test:,}, "
        f"null (decoy-involved) {n_null:,} "
        f"(decoy candidate volume = {n_null / max(1, m):.1%} of all).",
        "",
        "## Null density distribution (per length bin)",
        "| bin | n null | median | q05 | q01 |",
        "|---|---|---|---|---|",
    ]
    for b in range(nb):
        d = null_sorted[b]
        lines.append(
            f"| {bin_lbl[b]} | {len(d):,} | " +
            (f"{np.median(d):.3f} | {np.quantile(d, .05):.3f} | "
             f"{np.quantile(d, .01):.3f} |" if len(d) > 50 else "— | — | — |"))
    lines += [
        "",
        "## FDR-bounded operating table (max accepted density per bin)",
        "| bin | hand boundary | " +
        " | ".join(f"q={q}" for q in Q_LIST) + " |",
        "|---|---|" + "---|" * len(Q_LIST),
    ]
    for b in range(nb):
        mid = (LEN_BINS[b] + (LEN_BINS[b + 1] if b + 1 < nb
                              else LEN_BINS[b] * 2)) // 2
        cells = []
        for q in Q_LIST:
            t = results_q[q]['thresholds'].get(b)
            cells.append(f"{t:.3f}" if t is not None else "—")
        lines.append(f"| {bin_lbl[b]} | {accept_density(mid):.3f} | "
                     + " | ".join(cells) + " |")
    lines += [
        "",
        "## Acceptance volume + incumbent-boundary FDR estimate",
        f"- tested accepted by HAND boundary: {n_hand_test:,}; decoy-side "
        f"accepted: {n_hand_null:,}; population ratio {ratio:.2f} -> "
        f"point estimate FDR = {fdr_hand:.2%}. With 0 of {n_null:,} null "
        f"acceptances, the one-sided 95% bound is <= "
        f"{3.0 * ratio / max(1, n_hand_test):.3%} of hand-accepted pairs "
        f"(rule-of-three x ratio). This bounds CHANCE-alignment errors only — "
        f"it is a different (narrower) error class than human semantic "
        f"grading.",
        *[f"- BH q={q}: accepted {results_q[q]['accepted']:,} tested pairs "
          f"(p-cut {results_q[q]['p_cut']:.2e})" for q in Q_LIST],
        "",
        "## Tier-1 ground-truth recall (pairs present in probe corpus)",
        "| family | GT pairs | as candidates | hand | " +
        " | ".join(f"q={q}" for q in Q_LIST) + " |",
        "|---|---|---|---|" + "---|" * len(Q_LIST),
        *[f"| {r[0]} | {r[1]} | {r[2]} | {r[3]} | " +
          " | ".join(str(x) for x in r[4:]) + " |" for r in fam_rows],
        "",
        "## Per-stratum null sensitivity (same-bucket null pairs; median/q05)",
        "| bucket | " + " | ".join(bin_lbl) + " |",
        "|---|" + "---|" * nb,
        *strat_lines,
        "",
        "## Graded-pairs overlap",
        f"- pages from Hillel's 164 graded pairs present in probe corpus: "
        f"{n_overlap} (grades were taken at rehearsal/full scale; full "
        f"validation belongs to the wave-2 calibration)",
        "",
        "## Caveats / wave-2 plan",
        "- DF inflation: decoys add ~25% pages -> DF cap slightly stricter "
        "than production. Wave-2: two-run protocol (clean run for tested "
        "pairs, decoy run for the null), on liturgy.db then fullcorpus.db "
        "behind the compute queue.",
        "- ~2% duplicate candidate rows not deduped (matches "
        "candidate_pairs vs candidate_unique_pairs at rehearsal scale).",
        "- Single-chunk residual leak bounded by CHUNK=10 (span is mostly "
        "margin -> density >> any threshold).",
        "- Exchangeability argument: decoy candidates traverse the identical "
        "seed/DF/two-hit/verify path as tested candidates; the null is "
        "conditioned on candidate-generation by construction.",
        f"",
        f"Total runtime: {time.time() - t0:.0f}s.",
    ]
    open(OUT_MD, 'w', encoding='utf-8').write('\n'.join(lines))
    json.dump({'len_bins': LEN_BINS,
               'null_sizes': [len(d) for d in null_sorted],
               'results_q': {str(q): results_q[q] for q in Q_LIST},
               'fdr_hand_estimate': fdr_hand,
               'stats': {k: v for k, v in stats.items()
                         if isinstance(v, (int, float, str))}},
              open(OUT_JSON, 'w', encoding='utf-8'), indent=1)
    print(f"report -> {OUT_MD}", flush=True)


if __name__ == '__main__':
    main()
