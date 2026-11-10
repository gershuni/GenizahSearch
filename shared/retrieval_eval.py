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
        """Stable tune/holdout assignment, GROUP-aware.

        Hashing the bare query_id put sibling queries of one work on both
        sides of the boundary: the witness instrument draws 4 slices per work
        (`wit:<work>#0..3`) with IDENTICAL positive sets, and 510 of its 573
        works straddled tune/holdout -- tuning would have seen the very
        works/positives the holdout is meant to decide on. The group key is
        the query_id up to the first '#', which keeps a work's slices
        together. Ids without '#' (the FGP set) are their own group, so their
        assignment is byte-identical to the old rule and every FGP result
        recorded before this change remains valid. Found and fixed before any
        holdout was ever scored.
        """
        group = self.query_id.split('#', 1)[0]
        h = hashlib.sha256(f'{salt}|{group}'.encode()).hexdigest()
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
    # RESULT-SET SIZE. Not precision -- precision is not computable from a
    # self-retrieval instrument, because a returned manuscript that is not the
    # query's own may be a GENUINE parallel, which is the whole point of the
    # feature. What this does bound is the reader's burden: two methods at
    # equal recall are not equivalent if one hands back 5 candidates and the
    # other 500. Real precision needs the pooled blinded deck.
    sizes = sorted(o.n_returned for o in outcomes)
    res['returned_p50'] = sizes[len(sizes) // 2]
    res['returned_p95'] = sizes[min(len(sizes) - 1, int(0.95 * len(sizes)))]
    res['returned_max'] = sizes[-1]
    res['returned_mean'] = round(sum(sizes) / len(sizes), 1)
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

    def _holdout_keys(self, entry_type: str) -> set:
        """Keys of holdout entries of one type ('reservation' or 'record').

        The key includes query_set (external review, Codex #2): keyed on
        (method, policy_id) alone, scoring the FGP holdout would have locked
        the SAME configs out of the witness holdout -- one shared ledger
        could not execute the pre-registered two-instrument plan.
        """
        return {(e['method'], e.get('policy_id', ''), e.get('query_set', ''))
                for e in self.entries
                if e['split'] == SPLIT_HOLDOUT
                and e.get('type', 'record') == entry_type}

    def reserve(self, *, method: str, policy_id: str, split: str,
                query_set: str) -> Optional[dict]:
        """Durable pre-run reservation (external review, Codex #1).

        The defect this closes: the runner used to evaluate and write the
        outcome dump BEFORE the ledger got a chance to refuse, so a duplicate
        holdout run consumed the holdout and only then learned it should not
        have. Now the runner must reserve BEFORE any query is issued; a
        second reservation of the same (method, policy_id, query_set) raises
        while the holdout is still untouched. Tune reservations are a no-op.
        """
        if split != SPLIT_HOLDOUT:
            return None
        key = (method, policy_id, query_set)
        if key in self._holdout_keys('reservation') \
                or key in self._holdout_keys('record'):
            raise HoldoutReuse(
                f'{method} / {policy_id} / {query_set} is already reserved '
                f'or scored on the holdout split -- refusing before any '
                f'query runs.')
        entry = {'type': 'reservation', 'method': method,
                 'policy_id': policy_id, 'split': split,
                 'query_set': query_set}
        self.entries.append(entry)
        with open(self.path, 'a', encoding='utf-8') as fh:
            fh.write(json.dumps(entry, ensure_ascii=False,
                                sort_keys=True) + '\n')
        return entry

    def reserve_all(self, *, configs, split: str, query_set: str,
                    force: bool = False) -> list:
        """Reserve several configs ALL-OR-NOTHING (PR #324 review).

        `configs` is an iterable of (method, policy_id).

        The defect this closes is one level down from the one `reserve`
        closes. A caller looping `reserve()` over N configs writes each
        reservation as it goes, so if config 3 is a duplicate the ledger
        raises *after* configs 1 and 2 are already on disk. No query has run,
        yet those two are now permanently marked as having consumed the
        write-once holdout, and the obvious operator response -- fix the
        duplicate, re-run -- is refused. The holdout is spent by an error
        message.

        This also rejects a batch that repeats a key WITHIN itself, which the
        per-call form could not see at all: the first write would land and
        make the second look like a pre-existing reservation.

        Validate everything, then write once.
        """
        if split != SPLIT_HOLDOUT:
            return []

        pairs = [(str(m), str(p)) for m, p in configs]
        taken = self._holdout_keys('reservation') | self._holdout_keys('record')

        seen = set()
        for method, policy_id in pairs:
            key = (method, policy_id, query_set)
            if key in seen:
                raise HoldoutReuse(
                    f'{method} / {policy_id} / {query_set} appears twice in '
                    f'one reservation batch -- refusing before any query '
                    f'runs, and before anything is written.')
            seen.add(key)
            if key in taken and not force:
                # `force` mirrors `record(force=True)`: the operator is
                # explicitly re-opening a spent holdout and the ledger will
                # mark the scoring `forced`. Refusing here made --force
                # unreachable through the runner (PR #324 review) -- the
                # reservation rejected the key before `record` ever saw the
                # flag. The WITHIN-batch duplicate check above still applies:
                # asking for the same config twice in one command is a
                # mistake under any flag.
                raise HoldoutReuse(
                    f'{method} / {policy_id} / {query_set} is already '
                    f'reserved or scored on the holdout split -- refusing '
                    f'before any query runs, and before any config in this '
                    f'batch is written.')

        entries = [{'type': 'reservation', 'method': m, 'policy_id': p,
                    'split': split, 'query_set': query_set}
                   for m, p in pairs]
        # One write, after every key has passed. Not a transaction, but it
        # removes the window where a refusal leaves earlier configs consumed.
        with open(self.path, 'a', encoding='utf-8') as fh:
            fh.write(''.join(
                json.dumps(e, ensure_ascii=False, sort_keys=True) + '\n'
                for e in entries))
        self.entries.extend(entries)
        return entries

    def record(self, *, method: str, policy_id: str, split: str,
               query_set: str, summary: dict, strata: Optional[dict] = None,
               force: bool = False) -> dict:
        if split == SPLIT_HOLDOUT and not force:
            key = (method, policy_id, query_set)
            if key in self._holdout_keys('record'):
                raise HoldoutReuse(
                    f'{method} / {policy_id} / {query_set} has already been '
                    f'scored on the holdout split. A second look invalidates '
                    f'the pre-registered margin; pass force=True to record '
                    f'it anyway, and it will be flagged in the ledger.')
            if key not in self._holdout_keys('reservation'):
                raise HoldoutReuse(
                    f'{method} / {policy_id} / {query_set}: no reservation. '
                    f'Holdout scorings must call reserve() BEFORE running -- '
                    f'recording without one means the run consumed the '
                    f'holdout outside the ledger discipline.')
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
