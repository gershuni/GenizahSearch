# -*- coding: utf-8 -*-
"""Arrangement C: stream a query through the corpus-resident passage index.

Contract: docs/specs/passage-matching-algorithm.md sections 5, 6, 7, 10.2.

Pipeline, per query:

  norm_stream(query)                       letters + offsets for display
  gram codes, DEDUPLICATED                 first query position kept per code
  posting budget (policy.budget_policy)    deterministic, reported
  searchsorted into the CSR index          admitted postings only
  diagonal two-hit, DISTINCT gram codes    spec section 6.1: keep positions
                                           AND count distinct codes per
                                           cluster -- neither research
                                           shortcut
  extend +-MARGIN, Levenshtein             against policy's boundary
  merge accepted spans per record          30-letter gap, like track1_match
  -> PassageHit rows + a QueryReport that names every budget that fired

Determinism. Every stage sorts on a total order and every cap cuts on that
order, so the same (index, policy, query) triple always returns the same
result. The budget order is (band, df, gram_code, first_query_position) as the
spec fixes it; candidates order by (record, bucket); verification order is the
candidate order. No randomness, no dict iteration on the hot path.
"""
from __future__ import annotations

import time
from dataclasses import dataclass

import numpy as np
from rapidfuzz.distance import Levenshtein

from shared.passage_index import PassageIndex
from shared.passage_normalize import K, gram_codes, norm_stream
from shared.passage_policy import (
    BUDGET_NO_CAP, BUDGET_RAREST_FIRST, DEFAULT_POLICY,
    PassagePolicy,
)

BAND = 20          # diagonal bucket width, letters (spec section 6)
MARGIN = 30        # verification extension, letters (spec section 7)
MERGE_GAP = 30     # per-record span merge gap (track1_match convention)


@dataclass
class PassageHit:
    record: int                  # index into the artifact's records table
    record_id: str               # '{sys}_{IE}_{P}_{FL}'
    matched_letters: int         # sum of merged span lengths (query side)
    best_density: float
    n_spans: int
    spans: list                  # [(q0, q1, r0, r1, density), ...] merged
    score: float                 # = matched_letters (comparable to chunk path)


@dataclass
class QueryReport:
    """Everything that happened to the query. Ships in the result envelope --
    a truncated search that does not say so is a correctness defect."""
    policy_id: str = ''
    policy_name: str = ''
    query_letters: int = 0
    query_grams_distinct: int = 0
    grams_admitted: int = 0
    grams_excluded: int = 0
    postings_admitted: int = 0
    postings_excluded: int = 0
    candidates: int = 0
    candidates_truncated: bool = False
    candidates_restricted: int = 0
    verified: int = 0
    verify_truncated: bool = False
    accepted_spans: int = 0
    rejected_short: int = 0
    rejected_density: int = 0
    below_min_span_query: bool = False
    seconds: float = 0.0

    def as_dict(self) -> dict:
        from dataclasses import asdict
        return asdict(self)


def _admit_grams(idx: PassageIndex, codes: np.ndarray, first_pos: np.ndarray,
                 policy: PassagePolicy, report: QueryReport) -> np.ndarray:
    """Select which query grams to expand, under the posting budget.

    Returns indices into `codes` in a deterministic order. Budget unit is
    POSTINGS (spec 10.2): a gram is admitted only if its whole posting list
    fits the remaining budget -- partial lists would make results depend on
    posting order within a code in a way no one can reason about.

    BUDGET_BAND is real reservation, not just ordering: the budget is split
    equally across the artifact's frozen log-DF bands, each band spends its
    share on its own grams in (df, code, first_pos) order, and whatever a band
    cannot use is pooled and re-spent across the leftovers in the same global
    order (deterministic borrowing). Without reservation, band-then-df
    ordering degenerates into rarest-first, which the spec rejects: the rarest
    grams of a noisy query are disproportionately the corrupted ones.
    """
    dfs = idx.dfs(codes).astype(np.int64)
    held = dfs > 0
    report.grams_excluded += int((~held).sum())
    codes, first_pos, dfs = codes[held], first_pos[held], dfs[held]
    if not len(codes):
        return np.empty(0, dtype=np.int64)
    base = np.arange(len(codes), dtype=np.int64)

    if policy.budget_policy == BUDGET_NO_CAP:
        report.grams_admitted = len(codes)
        report.postings_admitted = int(dfs.sum())
        order = np.lexsort((first_pos, codes, dfs))
        # Map back through `held`, exactly as the capped branches do below.
        # Returning `base[order]` here was a real defect (PR #324 review):
        # `base` is built AFTER `codes = codes[held]`, so its indices address
        # the FILTERED array, while `_candidates` applies whatever we return
        # to the caller's ORIGINAL `codes`/`qpos`. Whenever any query gram was
        # absent from the index -- routine on noisy HTR text -- no-cap
        # expanded the wrong grams and stamped them with the wrong query
        # positions. Silently: every index was in range, so nothing raised.
        return np.flatnonzero(held)[order]

    edges = np.asarray(idx.manifest.get('query', {}).get('df_band_edges')
                       or [], dtype=np.int64)
    if policy.budget_policy == BUDGET_RAREST_FIRST or edges.size == 0:
        order = np.lexsort((first_pos, codes, dfs))
        take, spent = [], 0
        for i in order:
            if spent + dfs[i] > policy.posting_budget:
                continue
            take.append(i)
            spent += int(dfs[i])
        chosen = np.array(take, dtype=np.int64)
    else:
        band = np.searchsorted(edges, dfs, side='left')
        n_bands = int(edges.size) + 1
        share = policy.posting_budget // n_bands
        order = np.lexsort((first_pos, codes, dfs, band))
        chosen_mask = np.zeros(len(codes), dtype=bool)
        spent_band = np.zeros(n_bands, dtype=np.int64)
        # Round 1: each band spends its own reserved share.
        for i in order:
            b = int(band[i])
            if spent_band[b] + dfs[i] <= share:
                chosen_mask[i] = True
                spent_band[b] += int(dfs[i])
        # Round 2: deterministic borrowing -- the pooled leftover is spent
        # across ALL not-yet-admitted grams in the same global order.
        leftover = policy.posting_budget - int(spent_band.sum())
        for i in order:
            if chosen_mask[i]:
                continue
            if dfs[i] <= leftover:
                chosen_mask[i] = True
                leftover -= int(dfs[i])
        chosen = base[order][chosen_mask[order]]

    report.grams_admitted = int(len(chosen))
    report.grams_excluded += int(len(codes) - len(chosen))
    report.postings_admitted = int(dfs[chosen].sum())
    report.postings_excluded = int(dfs.sum()) - report.postings_admitted
    # Map back through the `held` filter to the caller's index space.
    return np.flatnonzero(held)[chosen]


def _candidates(idx: PassageIndex, codes: np.ndarray, qpos: np.ndarray,
                admitted: np.ndarray, policy: PassagePolicy,
                report: QueryReport, record_allowed=None):
    """Diagonal two-hit with DISTINCT gram codes (spec section 6.1).

    Returns (rec, min_q, max_q, min_r, max_r) arrays for clusters that carry
    >= policy.min_anchors distinct codes, ordered by (record, bucket) --
    which is also the deterministic truncation order for candidate_cap.
    """
    parts_rec, parts_qp, parts_rp, parts_code = [], [], [], []
    for i in admitted.tolist():
        pages, positions = idx.postings_for(int(codes[i]))
        if not pages.size:
            continue
        parts_rec.append(pages.astype(np.int64))
        parts_rp.append(positions.astype(np.int64))
        parts_qp.append(np.full(pages.size, int(qpos[i]), dtype=np.int64))
        parts_code.append(np.full(pages.size, int(codes[i]), dtype=np.int64))
    if not parts_rec:
        return (np.empty(0, np.int64),) * 5
    rec = np.concatenate(parts_rec)
    rp = np.concatenate(parts_rp)
    qp = np.concatenate(parts_qp)
    code = np.concatenate(parts_code)

    bucket = (qp - rp) // BAND
    order = np.lexsort((code, bucket, rec))
    rec, rp, qp, code, bucket = (a[order] for a in (rec, rp, qp, code, bucket))

    # Group hits into clusters: same record, adjacent-or-equal bucket chain
    # (gap <= 1 merges bucket +-1 exactly like the research engines).
    new = np.empty(len(rec), dtype=bool)
    new[0] = True
    np.logical_or(rec[1:] != rec[:-1],
                  (bucket[1:] - bucket[:-1]) > 1, out=new[1:])
    grp = np.cumsum(new) - 1
    starts = np.flatnonzero(new)

    # Distinct codes per cluster: a code counts once per cluster. `code` is
    # sorted within one (rec, bucket) run but not across a merged bucket
    # chain, so re-sort within each group by code before the first-occurrence
    # mask -- otherwise a code straddling two buckets counts twice.
    order2 = np.lexsort((code, grp))
    grp2, code2 = grp[order2], code[order2]
    fc = np.empty(len(rec), dtype=bool)
    fc[0] = True
    np.logical_or(grp2[1:] != grp2[:-1], code2[1:] != code2[:-1], out=fc[1:])
    distinct = np.bincount(grp2[fc], minlength=starts.size)

    keep = distinct >= policy.min_anchors

    # Record restriction BEFORE either cap is spent (PR #324 round 5). The
    # searcher used to filter restrict_sys_ids on the HITS, after
    # candidate_cap and verify_cap had already been consumed globally -- so
    # a library/work/date-filtered search on a common text could lose its
    # in-set witnesses to stronger candidates from manuscripts the caller
    # had explicitly excluded: false negatives that look like absence of
    # evidence. Applied here, the caps are spent only on records the caller
    # can actually receive. The predicate sees the record-id STRING; decode
    # once per unique candidate record (tens of thousands at most), never
    # per posting.
    if record_allowed is not None and keep.any():
        g_rec_all = rec[starts]
        cand_recs = np.unique(g_rec_all[keep])
        allowed_set = {int(r) for r in cand_recs.tolist()
                       if record_allowed(idx.record_id(int(r)))}
        allowed_mask = np.fromiter((int(r) in allowed_set
                                    for r in g_rec_all.tolist()),
                                   dtype=bool, count=g_rec_all.size)
        before = int(keep.sum())
        keep &= allowed_mask
        report.candidates_restricted = before - int(keep.sum())

    report.candidates = int(keep.sum())
    kept_groups = np.flatnonzero(keep)
    # Order candidates by EVIDENCE STRENGTH (distinct anchors, descending),
    # tie-broken by (record, bucket) for determinism. The first version
    # ordered by (record, bucket) alone, which under a firing verify cap
    # meant low record indices always won and later records were never
    # verified at all -- measured on the full corpus as self-retrieval
    # collapsing to 5/12 at 1,000-letter queries purely by catalog position.
    # Strength ordering makes the caps keep the best-evidenced candidates,
    # which is what a cap is for.
    strength_order = np.lexsort((kept_groups, -distinct[kept_groups]))
    kept_groups = kept_groups[strength_order]
    if kept_groups.size > policy.candidate_cap:
        report.candidates_truncated = True
        kept_groups = kept_groups[:policy.candidate_cap]

    min_q = np.minimum.reduceat(qp, starts)
    max_q = np.maximum.reduceat(qp, starts)
    min_r = np.minimum.reduceat(rp, starts)
    max_r = np.maximum.reduceat(rp, starts)
    g_rec = rec[starts]
    return (g_rec[kept_groups], min_q[kept_groups], max_q[kept_groups],
            min_r[kept_groups], max_r[kept_groups])


def _verify_and_merge(idx: PassageIndex, qstream: str, cand, policy:
                      PassagePolicy, report: QueryReport) -> dict:
    """Extend, align, accept, and merge per record.

    Returns record -> [(q0, q1, r0, r1, density), ...] merged spans. The
    verification order is the candidate order -- anchor strength descending,
    then (record, bucket) -- so verify_cap keeps the best-evidenced
    candidates and cuts deterministically; when it fires the envelope says
    so.
    """
    g_rec, min_q, max_q, min_r, max_r = cand
    by_record: dict = {}
    n_verified = 0
    for i in range(len(g_rec)):
        if n_verified >= policy.verify_cap:
            report.verify_truncated = True
            break
        n_verified += 1
        ri = int(g_rec[i])
        rstream = idx.stream(ri)
        q0 = max(0, int(min_q[i]) - MARGIN)
        q1 = min(len(qstream), int(max_q[i]) + K + MARGIN)
        # MIRRORED extension -- an arrangement-C necessity the research code
        # never needed. Its streamed side was always a full page, so +-MARGIN
        # on both sides extended into real flanking text symmetrically. Here
        # the query IS the passage: its window clamps at the query bounds,
        # and if the record window keeps the full margin anyway it comes out
        # up to 2*MARGIN letters longer. Levenshtein must pay one edit per
        # surplus letter, so density inflates by ~2*MARGIN/aligned_len --
        # 0.43 for an 80-letter query, an automatic rejection of a VERBATIM
        # match. The record therefore extends exactly as far as the query
        # actually managed to, per side.
        left = int(min_q[i]) - q0
        right = q1 - (int(max_q[i]) + K)
        r0 = max(0, int(min_r[i]) - left)
        r1 = min(len(rstream), int(max_r[i]) + K + right)
        shorter = min(q1 - q0, r1 - r0)
        if shorter < policy.min_span:
            report.rejected_short += 1
            continue
        alen = max(q1 - q0, r1 - r0)
        limit = policy.max_density(alen)
        cutoff = int(limit * alen) + 1
        dist = Levenshtein.distance(qstream[q0:q1], rstream[r0:r1],
                                    score_cutoff=cutoff)
        density = dist / alen
        if not policy.accepts(shorter, alen, density):
            report.rejected_density += 1
            continue
        by_record.setdefault(ri, []).append(
            (q0, q1, r0, r1, round(density, 4)))
    report.verified = n_verified

    merged: dict = {}
    for ri, spans in by_record.items():
        spans.sort()
        out = []
        for q0, q1, r0, r1, d in spans:
            if out and q0 <= out[-1][1] + MERGE_GAP:
                prev = out[-1]
                out[-1] = (prev[0], max(prev[1], q1), min(prev[2], r0),
                           max(prev[3], r1), min(prev[4], d))
            else:
                out.append((q0, q1, r0, r1, d))
        merged[ri] = out
        report.accepted_spans += len(out)
    return merged


def search_passage(idx: PassageIndex, query_text: str,
                   policy: PassagePolicy = DEFAULT_POLICY,
                   record_allowed=None,
                   ) -> tuple[list, QueryReport]:
    """The full arrangement-C query. Returns (hits, report).

    Hits are sorted by (-score, record) -- deterministic, and score is
    matched letters, directly comparable to the chunk path's merged-span
    character score.
    """
    t0 = time.time()
    report = QueryReport(policy_id=policy.policy_id, policy_name=policy.name)
    qstream, _offsets = norm_stream(query_text)
    report.query_letters = len(qstream)
    if len(qstream) < policy.min_span:
        report.below_min_span_query = True
        report.seconds = round(time.time() - t0, 4)
        return [], report

    all_codes = gram_codes(qstream)
    # Deduplicate, keeping the FIRST query position per code (spec 10.2).
    codes, first_idx = np.unique(all_codes, return_index=True)
    qpos = first_idx.astype(np.int64)
    report.query_grams_distinct = int(codes.size)

    admitted = _admit_grams(idx, codes, qpos, policy, report)
    if not admitted.size:
        report.seconds = round(time.time() - t0, 4)
        return [], report

    cand = _candidates(idx, codes, qpos, admitted, policy, report,
                       record_allowed=record_allowed)
    merged = _verify_and_merge(idx, qstream, cand, policy, report)

    hits = []
    for ri, spans in merged.items():
        matched = sum(q1 - q0 for q0, q1, _r0, _r1, _d in spans)
        hits.append(PassageHit(
            record=ri, record_id=idx.record_id(ri),
            matched_letters=matched,
            best_density=min(d for *_x, d in spans),
            n_spans=len(spans), spans=spans, score=float(matched)))
    hits.sort(key=lambda h: (-h.score, h.record))
    report.seconds = round(time.time() - t0, 4)
    return hits, report
