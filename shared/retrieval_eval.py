# -*- coding: utf-8 -*-
"""Method-agnostic evaluation core for retrieval comparisons.

Contract: the plan's Deliverable 3, and the tuning/held-out discipline the
external review insisted be STRUCTURAL rather than remembered.

Nothing here knows what a passage index or a Tantivy chunk search is. A method
is any callable `retrieve(query_text) -> ranked list of record ids`, so the
incumbent and the new engine are measured by identical code on identical
queries -- which is the only way the comparison means anything.

Three things this enforces that a plain metrics function would not:

1. SPLIT BY CONSTRUCTION. Every query is assigned to `tune` or `holdout` by a
   hash of its own id, so the split is stable across runs, machines and
   sessions, and cannot drift as the query set grows. You cannot accidentally
   tune on the deciding data by forgetting which list you were holding.

2. HOLDOUT IS WRITE-ONCE. `EvalLedger` refuses a second holdout scoring of the
   same (method, policy_id) unless explicitly overridden, and records every
   scoring that happened. Sweeping N policies and reporting the best is how a
   3-point non-inferiority margin gets silently eaten; the ledger makes the
   attempt visible instead of invisible.

3. STRATA ARE DECLARED, NOT DISCOVERED. Per-stratum results are computed for
   strata named up front, because a headline that passes while a subgroup
   fails is a failure mode this project has already shipped once (CERT-01:
   0.9382 weighted, 0.471 in one stratum, one work causing 45% of error).
"""
from __future__ import annotations

import hashlib
import json
import math
import os
import time
from dataclasses import dataclass, field
from typing import Callable, Iterable, Optional

SPLIT_TUNE = 'tune'
SPLIT_HOLDOUT = 'holdout'


@dataclass(frozen=True)
class EvalQuery:
    """One query with its known-correct answer.

    `positives` is the set of record ids that count as correct. For FGP
    self-retrieval that is a single page; for a witness-index oracle it is the
    whole witness set.
    """
    query_id: str
    text: str
    positives: frozenset
    strata: dict = field(default_factory=dict)

    def split(self, salt: str = 'v1') -> str:
        """Stable tune/holdout assignment from the query id alone."""
        h = hashlib.sha256(f'{salt}|{self.query_id}'.encode()).hexdigest()
        return SPLIT_TUNE if int(h[:8], 16) % 100 < 50 else SPLIT_HOLDOUT


@dataclass
class QueryOutcome:
    query_id: str
    rank: Optional[int]          # 0-based rank of the first positive
    n_returned: int
    seconds: float
    strata: dict = field(default_factory=dict)


def evaluate(queries: Iterable[EvalQuery],
             retrieve: Callable[[str], list],
             *, k_values=(1, 10, 50, 200),
             progress: Optional[Callable] = None) -> list:
    """Run one method over one query set. Returns per-query outcomes."""
    out = []
    for i, q in enumerate(queries):
        t0 = time.perf_counter()
        ranked = retrieve(q.text)
        dt = time.perf_counter() - t0
        rank = None
        for pos, rid in enumerate(ranked):
            if rid in q.positives:
                rank = pos
                break
        out.append(QueryOutcome(query_id=q.query_id, rank=rank,
                                n_returned=len(ranked), seconds=dt,
                                strata=dict(q.strata)))
        if progress and (i + 1) % 25 == 0:
            progress(i + 1, dt)
    return out


def _wilson(successes: int, n: int, z: float = 1.96) -> tuple:
    """Wilson score interval -- correct near 0 and 1, unlike normal-approx."""
    if n == 0:
        return (0.0, 0.0, 0.0)
    p = successes / n
    d = 1 + z * z / n
    centre = (p + z * z / (2 * n)) / d
    half = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / d
    return (p, max(0.0, centre - half), min(1.0, centre + half))


def summarize(outcomes: list, *, k_values=(1, 10, 50, 200)) -> dict:
    """Recall@k with Wilson intervals, MRR, and latency percentiles."""
    n = len(outcomes)
    if not n:
        return {'n': 0}
    res = {'n': n}
    for k in k_values:
        hits = sum(1 for o in outcomes if o.rank is not None and o.rank < k)
        p, lo, hi = _wilson(hits, n)
        res[f'recall@{k}'] = round(p, 4)
        res[f'recall@{k}_ci'] = [round(lo, 4), round(hi, 4)]
    rr = [1.0 / (o.rank + 1) for o in outcomes if o.rank is not None]
    res['mrr'] = round(sum(rr) / n, 4)
    res['found_any'] = sum(1 for o in outcomes if o.rank is not None)
    lat = sorted(o.seconds * 1000 for o in outcomes)
    res['p50_ms'] = round(lat[len(lat) // 2], 1)
    res['p95_ms'] = round(lat[min(len(lat) - 1, int(0.95 * len(lat)))], 1)
    return res


def summarize_by_stratum(outcomes: list, stratum: str, **kw) -> dict:
    """Per-stratum summaries. Strata must be declared on the queries."""
    groups: dict = {}
    for o in outcomes:
        groups.setdefault(o.strata.get(stratum, 'unknown'), []).append(o)
    return {str(key): summarize(vals, **kw)
            for key, vals in sorted(groups.items(), key=lambda kv: str(kv[0]))}


def split_queries(queries: Iterable[EvalQuery], salt: str = 'v1') -> dict:
    """Partition into {'tune': [...], 'holdout': [...]} by query-id hash."""
    out = {SPLIT_TUNE: [], SPLIT_HOLDOUT: []}
    for q in queries:
        out[q.split(salt)].append(q)
    return out


class HoldoutReuse(Exception):
    """Raised when a (method, policy) is scored on holdout a second time."""


class EvalLedger:
    """Append-only record of every scoring, with a write-once holdout rule.

    The rule exists because sweeping N policies and reporting the best one
    silently inflates any non-inferiority claim: with enough attempts, one
    clears the margin by luck. Tuning-split scorings are unrestricted; a
    holdout scoring of the same (method, policy_id) twice raises, and forcing
    it records `forced: true` so the report cannot pretend it was a first look.
    """

    def __init__(self, path: str):
        self.path = path
        self.entries: list = []
        if os.path.exists(path):
            with open(path, encoding='utf-8') as fh:
                for line in fh:
                    line = line.strip()
                    if line:
                        self.entries.append(json.loads(line))

    def _holdout_keys(self) -> set:
        return {(e['method'], e.get('policy_id', ''))
                for e in self.entries if e['split'] == SPLIT_HOLDOUT}

    def record(self, *, method: str, policy_id: str, split: str,
               query_set: str, summary: dict, strata: Optional[dict] = None,
               force: bool = False) -> dict:
        if split == SPLIT_HOLDOUT and not force:
            if (method, policy_id) in self._holdout_keys():
                raise HoldoutReuse(
                    f'{method} / {policy_id} has already been scored on the '
                    f'holdout split. A second look invalidates the '
                    f'pre-registered margin; pass force=True to record it '
                    f'anyway, and it will be flagged in the ledger.')
        entry = {
            'method': method, 'policy_id': policy_id, 'split': split,
            'query_set': query_set, 'summary': summary,
            'strata': strata or {}, 'forced': bool(force),
        }
        self.entries.append(entry)
        with open(self.path, 'a', encoding='utf-8') as fh:
            fh.write(json.dumps(entry, ensure_ascii=False,
                                sort_keys=True) + '\n')
        return entry


def non_inferior(candidate: dict, incumbent: dict, metric: str,
                 margin: float = 0.03) -> dict:
    """One-sided non-inferiority on a proportion metric.

    Passes when the candidate's LOWER confidence bound is no more than
    `margin` below the incumbent's point estimate -- the pre-declared rule
    from the plan (3 percentage points). Reported per stratum, never as a
    single headline, because that is the failure mode this project has
    already shipped once.
    """
    c_lo = candidate.get(f'{metric}_ci', [0, 0])[0]
    i_p = incumbent.get(metric, 0.0)
    delta = c_lo - i_p
    return {
        'metric': metric,
        'candidate': candidate.get(metric),
        'candidate_ci_low': c_lo,
        'incumbent': i_p,
        'delta_lower_bound': round(delta, 4),
        'margin': margin,
        'pass': bool(delta >= -margin),
        'n_candidate': candidate.get('n', 0),
        'n_incumbent': incumbent.get('n', 0),
    }
