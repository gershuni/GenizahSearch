# -*- coding: utf-8 -*-
"""Seed-and-extend engine for the shared-passage probe.

One implementation, two modes:
- ground-truth mode: k=4, no DF drop (posting cap only, logged), min_anchors=1
  -> near-exhaustive oracle over known-related pages
- candidate mode:    k=5, DF-banded (drop high-DF + singletons), min_anchors=2
  -> the production-shaped candidate generator being tested

Pipeline per pair: shared k-gram anchors -> diagonal binning (band) ->
best cluster -> span + margin -> Levenshtein edit density -> accept/reject.
"""
from collections import defaultdict

from rapidfuzz.distance import Levenshtein


def build_anchor_pairs(streams, k=5, df_drop=None, posting_cap=None,
                       exclude_same_key=None, min_anchors=2,
                       max_anchors_per_pair=64):
    """Find candidate pairs sharing >= min_anchors k-gram seeds.

    streams: list[str] normalized letter streams
    df_drop: drop grams appearing in more than df_drop *distinct pages* (None = keep)
    posting_cap: hard cap on posting-list length for pair generation (None = 3000)
    exclude_same_key: list same length as streams; pairs with equal key are skipped
                      (e.g. sys_id -> never pair a manuscript with itself)
    Returns: dict[(i, j)] -> list[(pos_i, pos_j)], stats dict
    """
    posting_cap = posting_cap or 3000
    index = defaultdict(list)  # gram -> [(page_idx, pos)]
    for pi, s in enumerate(streams):
        if not s:
            continue
        for pos in range(len(s) - k + 1):
            index[s[pos:pos + k]].append((pi, pos))

    stats = {
        'grams_total': len(index),
        'grams_dropped_df': 0,
        'grams_dropped_cap': 0,
        'grams_dropped_singleton': 0,
    }

    pair_anchors = defaultdict(list)
    for gram, postings in index.items():
        pages_here = {pi for pi, _ in postings}
        if len(pages_here) < 2:
            stats['grams_dropped_singleton'] += 1
            continue
        if df_drop is not None and len(pages_here) > df_drop:
            stats['grams_dropped_df'] += 1
            continue
        if len(postings) > posting_cap:
            stats['grams_dropped_cap'] += 1
            continue
        # group postings per page to avoid quadratic same-page work
        by_page = defaultdict(list)
        for pi, pos in postings:
            by_page[pi].append(pos)
        pages = sorted(by_page)
        for a_i in range(len(pages)):
            for b_i in range(a_i + 1, len(pages)):
                pa, pb = pages[a_i], pages[b_i]
                if exclude_same_key is not None and \
                        exclude_same_key[pa] == exclude_same_key[pb]:
                    continue
                # limit per-gram anchor fanout AND total anchors per pair
                # (memory guard: liturgical corpora otherwise accumulate
                # hundreds of anchors per pair -> tens of GB)
                lst = pair_anchors[(pa, pb)]
                if len(lst) >= max_anchors_per_pair:
                    continue
                for pos_a in by_page[pa][:8]:
                    for pos_b in by_page[pb][:8]:
                        lst.append((pos_a, pos_b))
                        if len(lst) >= max_anchors_per_pair:
                            break
                    else:
                        continue
                    break

    if min_anchors > 1:
        pair_anchors = {p: a for p, a in pair_anchors.items()
                        if len(a) >= min_anchors}
    stats['candidate_pairs'] = len(pair_anchors)
    return pair_anchors, stats


def verify_pair(sa, sb, anchors, k=5, band=20, margin=30,
                min_span=25, max_density=0.30, min_anchors=2):
    """Diagonal-bin the anchors, take the best cluster, verify by edit density.

    Returns None or dict(span_a, span_b, n_anchors, aligned_len, density,
    coverage_shorter)."""
    bins = defaultdict(list)
    for i, j in anchors:
        bins[(i - j) // band].append((i, j))
    # best bin = bin + neighbors with most anchors
    best_key, best = None, []
    for key in bins:
        cluster = bins.get(key - 1, []) + bins[key] + bins.get(key + 1, [])
        if len(cluster) > len(best):
            best_key, best = key, cluster
    if len(best) < min_anchors:
        return None
    ia = [i for i, _ in best]
    jb = [j for _, j in best]
    a0, a1 = max(0, min(ia) - margin), min(len(sa), max(ia) + k + margin)
    b0, b1 = max(0, min(jb) - margin), min(len(sb), max(jb) + k + margin)
    span_a, span_b = sa[a0:a1], sb[b0:b1]
    if min(len(span_a), len(span_b)) < min_span:
        return None
    dist = Levenshtein.distance(span_a, span_b)
    density = dist / max(len(span_a), len(span_b))
    if density > max_density:
        return None
    aligned_len = max(len(span_a), len(span_b))
    return {
        'a0': a0, 'a1': a1, 'b0': b0, 'b1': b1,
        'n_anchors': len(best),
        'aligned_len': aligned_len,
        'density': round(density, 4),
        'coverage_shorter': round(
            min(len(span_a), len(span_b)) / max(1, min(len(sa), len(sb))), 4),
    }


def run(streams, ids, k=5, df_drop=None, posting_cap=None, min_anchors=2,
        band=20, margin=30, min_span=25, max_density=0.30,
        exclude_same_key=None):
    """Full pass: candidates -> verified pairs. Returns (verified, stats)."""
    pair_anchors, stats = build_anchor_pairs(
        streams, k=k, df_drop=df_drop, posting_cap=posting_cap,
        exclude_same_key=exclude_same_key, min_anchors=min_anchors)
    verified = []
    for (pa, pb), anchors in pair_anchors.items():
        v = verify_pair(streams[pa], streams[pb], anchors, k=k, band=band,
                        margin=margin, min_span=min_span,
                        max_density=max_density, min_anchors=min_anchors)
        if v:
            v['a'] = ids[pa]
            v['b'] = ids[pb]
            verified.append(v)
    stats['verified_pairs'] = len(verified)
    return verified, stats
