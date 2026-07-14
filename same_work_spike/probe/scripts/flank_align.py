# -*- coding: utf-8 -*-
"""MAPV2-15m flank primitives — ref-side span RELOCATION + edge-gap-tolerant
flank RECONVERGENCE (Codex-gated "Way 2", pre-flight-approved).

The one genuinely missing primitive per the recon: everything else (norm_stream,
RefText, rapidfuzz) is reused. Two public fns:

  gram_index(stream, k=5)          -> {gram: [positions]} (build ONCE per work)
  relocate(span, work, gpos)       -> (r0, r1, edit, n_anchors) | None
       locate a page's matched-span stream slice inside a work stream: sample
       k-grams from the span, diagonal-vote (bin 25) their work positions to a
       candidate region, then rapidfuzz partial-align the span into a small
       window (NOT the whole work — Codex perf note for long works like Mishneh
       Torah). Credible iff aligned norm-edit <= 0.38 (<=0.42 for <80) & >=2
       anchor votes.
  flank_dist(p_flank, w_flank)     -> best_norm_edit | None(=short/edge)
       the maximal-span-boundary fix: seed-and-extend stops AT the span edge so
       the immediate flank always diverges; we search a leading EDGE-GAP shift
       delta in [-G, G] (a lacuna/HTR gap of up to G letters may precede the
       continuation), take equal-length windows (clip — unequal Levenshtein
       floors at the length ratio, the documented fix_flanks bug), and return
       the BEST (min) normalized edit over all shifts. Classification into
       continuation/ambiguous/island is deferred to the caller so the calibrator
       can vary thresholds over stored distances without recompute.

Distances are raw; the calibrator (calibrate_flank.py) owns the cutoffs.
"""
from collections import Counter

from rapidfuzz.distance import Levenshtein
from rapidfuzz.fuzz import partial_ratio_alignment

K = 5
GAP = 60          # max leading edge-gap (lacuna/HTR) before reconvergence
WINDOW = 120      # flank comparison window (Codex preferred block 80-120)
MIN_LEN = 60      # min usable flank letters to judge (else short/edge)
GRAM_CAP = 40     # ignore in-work grams occurring > this many times (noise)
# default classification cutoffs (calibrator overrides via flank_thresholds.json)
CONT_THR = 0.42
ISLAND_THR = 0.58


def gram_index(stream, k=K, cap=GRAM_CAP):
    """{gram -> [sorted positions]} over a work stream; very frequent grams
    (repetitive formulae within one work) dropped as non-discriminative."""
    idx = {}
    for i in range(len(stream) - k + 1):
        idx.setdefault(stream[i:i + k], []).append(i)
    return {g: p for g, p in idx.items() if len(p) <= cap}


def relocate(span, work, gpos, k=K):
    """Locate `span` (a page's matched-span letter slice) inside `work` stream.
    Returns (r0, r1, norm_edit, n_anchors) of the best relocation, or None."""
    if len(span) < k or not work:
        return None
    n = len(span)
    step = max(1, n // 12)
    votes = Counter()
    for i in range(0, n - k + 1, step):
        for p in gpos.get(span[i:i + k], ()):
            votes[(p - i) // 25] += 1          # implied work-start, bin 25
    if not votes:
        return None
    bin_, nv = votes.most_common(1)[0]
    if nv < 2:                                 # need >=2 anchor votes
        return None
    r_start = max(0, bin_ * 25 - 30)
    r_end = min(len(work), bin_ * 25 + n + 30)
    win = work[r_start:r_end]
    if len(win) < k:
        return None
    al = partial_ratio_alignment(span, win, score_cutoff=0)
    if al is None:
        return None
    r0, r1 = r_start + al.dest_start, r_start + al.dest_end
    if r1 <= r0:
        return None
    d = Levenshtein.normalized_distance(span, work[r0:r1])
    thr = 0.42 if n < 80 else 0.38
    if d > thr:
        return None
    return (r0, r1, round(d, 4), nv)


def flank_dist(p_flank, w_flank, gap=GAP, window=WINDOW, min_len=MIN_LEN):
    """Best equal-length normalized edit over leading edge-gap shifts. None if
    neither side offers a >= min_len window at any shift (short/edge)."""
    if len(p_flank) < min_len or len(w_flank) < min_len:
        return None
    best = None
    for delta in range(-gap, gap + 1):
        if delta >= 0:
            a, b = p_flank[delta:], w_flank
        else:
            a, b = p_flank, w_flank[-delta:]
        L = min(len(a), len(b), window)
        if L < min_len:
            continue
        d = Levenshtein.normalized_distance(a[:L], b[:L])
        if best is None or d < best:
            best = d
    return None if best is None else round(best, 4)


def classify(dist, cont=CONT_THR, island=ISLAND_THR):
    if dist is None:
        return 'short'
    if dist <= cont:
        return 'continuation'
    if dist <= island:
        return 'ambiguous'
    return 'island'


# --------------------------------------------------------------- self-test
def _selftest():
    import random
    random.seed(0)  # module-level seed is fine here (test only, not workflow)
    alpha = 'אבגדהוזחטיכלמנסעפצקרשת'

    def rnd(n):
        return ''.join(random.choice(alpha) for _ in range(n))

    def noise(s, rate=0.18):
        out = []
        for c in s:
            r = random.random()
            if r < rate * 0.5:
                continue                       # deletion
            elif r < rate:
                out.append(random.choice(alpha))  # substitution
            else:
                out.append(c)
        return ''.join(out)

    span = rnd(120)
    cont = rnd(200)          # the shared continuation text
    other = rnd(200)         # unrelated (island) text
    work = span + cont
    gpos = gram_index(work)

    # relocation: a noised copy of the span should relocate into work
    r = relocate(noise(span), work, gpos)
    assert r and r[0] <= 5 and abs(r[1] - 120) <= 20, f"relocate span: {r}"
    print(f"[ok] relocate span -> r0={r[0]} r1={r[1]} edit={r[2]} anchors={r[3]}")

    # CONTINUATION: page right flank = noised cont; ref right flank = cont
    d_cont = flank_dist(noise(cont), cont)
    print(f"[ok] continuation flank_dist={d_cont} -> {classify(d_cont)}")
    assert classify(d_cont) == 'continuation', d_cont

    # ISLAND: page right flank = unrelated text
    d_isl = flank_dist(other, cont)
    print(f"[ok] island flank_dist={d_isl} -> {classify(d_isl)}")
    assert classify(d_isl) == 'island', d_isl

    # EDGE-GAP: page has 40 junk letters before the continuation resumes
    d_gap = flank_dist(rnd(40) + noise(cont), cont)
    print(f"[ok] edge-gap(40) flank_dist={d_gap} -> {classify(d_gap)}")
    assert classify(d_gap) in ('continuation', 'ambiguous'), d_gap

    # SHORT: <60 letters -> abstain
    assert flank_dist(rnd(30), cont) is None
    assert classify(None) == 'short'
    print("[ok] short flank -> None -> 'short' (abstain)")
    print("ALL FLANK-ALIGN SELF-TESTS PASSED")


if __name__ == '__main__':
    _selftest()
