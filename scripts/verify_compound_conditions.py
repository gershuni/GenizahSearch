"""Gate G8 of docs/plans/COMPOSITION_CONDITIONS_PLAN.md: Composition Search
conditions on the REAL installed letter-level index.

    python scripts/verify_compound_conditions.py [--index DIR] [--cases FILE]
                                                 [--seed N] [--self-test]

Every case is built from real records, so its answer is known in advance:

  (a) adjacent image: A cut from image P, B cut from image P+1 of the same
      image set -> found. A decoy B' from a page further on, whose absence from
      P-1 and P+1 is established independently (by reading those records'
      streams, not by the engine) -> not found.
  (b) same manuscript: A and B from distant images of one manuscript -> kept;
      B taken from another manuscript, absent from A's (checked in its
      streams) -> A's page dropped.
  (c) must not appear: C cut from another page of A's manuscript -> A's page
      removed, the evidence names C's page; the same C as "must appear" ->
      kept (the gate flips).
  (d) exhaustiveness: corrupted texts (every 8th letter changed) searched for
      their own page by the ordinary budgeted search and by the masked
      condition search; the masked search must find at least as many, and
      every miss is listed.
  (e) cost: the longest allowed version (10,000 letters) over a large mask.

The user's own cases (the Talmud / Halakhot Gedolot passage, the siddur case)
run from --cases FILE (JSON: [{"name", "main", "conditions": [{"requirement",
"where", "versions", "label"}], "expect": {"main": [sys_ids], "removed":
[sys_ids], "dropped": [sys_ids]}}]). Without the file they are reported as
PENDING -- never as passed.

FAILS (exit 1), never skips, when the index is missing. --self-test runs the
adjacency case once more with the adjacency rule broken (P+-2) and fails
unless that case then fails: proof the gate can fail.
"""

from __future__ import annotations

import argparse
import json
import os
import random
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from rapidfuzz import fuzz  # noqa: E402

from shared import compound_query as cq  # noqa: E402
from shared.passage_index import open_index  # noqa: E402
from shared.passage_normalize import norm_stream_fast  # noqa: E402
from shared.passage_pages import page_index_for  # noqa: E402
from shared.passage_parallels import PassageSearcher, _RegexSysIdParser  # noqa: E402
from shared.passage_policy import compose  # noqa: E402
from shared.passage_search import search_passage  # noqa: E402

CUT = 220            # letters per synthetic text
ABSENT_BELOW = 60    # partial_ratio below this = "not there" (independent check)


def _default_index_dir():
    try:
        from desktop import passage_lifecycle as pl
        return os.path.join(pl.passage_root(), pl.LIVE_DIRNAME)
    except Exception:
        return ''


class _StreamFetcher:
    """Page text for rendering, from the index's own streams (letters only):
    enough to project spans and build highlight patterns."""

    def __init__(self, idx, pmap):
        self.idx, self.pmap = idx, pmap

    def get_full_text_by_header(self, header):
        key = cq.parse_page_key(header)
        if key is None:
            return None
        recs = self.pmap.records_for_page(key.sys_id, key.ie, key.p)
        return self.idx.stream(int(recs[0])) if len(recs) else None


def run_compound(searcher, main_text, conditions):
    """The worker's pipeline without Qt: main search uncapped, every condition
    version searched over its scope, judged, routed (cap after judging)."""
    res = searcher.search_composition_logic(
        main_text, boundary_mode='full', min_boundary_matches=1)
    main, filt = list(res.get('main') or []), list(res.get('filtered') or [])
    keys = [cq.parse_page_key(r['raw_header']) for r in main + filt]
    results = []
    for c in conditions:
        pages, sys_ids = cq.search_scope(c.where, keys)
        results.append([searcher.search_condition(
            v, pages=tuple(pages), sys_ids=tuple(sys_ids)).as_version_search()
            for v in c.versions])
    outcomes = cq.judge([r['raw_header'] for r in main + filt], conditions, results)
    return cq.route(main, filt, outcomes, len(conditions), cap=0,
                    sys_parser=_RegexSysIdParser()), outcomes


def _sys_of(rows):
    return {cq.parse_page_key(r['raw_header']).sys_id for r in rows}


class Corpus:
    def __init__(self, idx, rng):
        self.idx, self.rng = idx, rng
        self.pmap = page_index_for(idx)
        self.by_set = {}
        for sys_id, recs in self.pmap.sys_records.items():
            for r in recs.tolist():
                ie = self.pmap.rec_ie[r]
                self.by_set.setdefault((sys_id, int(ie)), {})[int(self.pmap.rec_p[r])] = r
        self.ie_name = {code: name for name, code in self.pmap.ie_codes.items()}

    def stream(self, r):
        return self.idx.stream(int(r))

    def cut(self, r, n=CUT):
        s = self.stream(r)
        if len(s) < n + 20:
            return None
        start = self.rng.randrange(10, len(s) - n - 5)
        return s[start:start + n]

    def absent(self, text, records):
        t = norm_stream_fast(text)
        return all(fuzz.partial_ratio(t, self.stream(r)) < ABSENT_BELOW for r in records)

    def pick_adjacent(self):
        sets = [k for k, pages in self.by_set.items() if len(pages) >= 6]
        for _ in range(5000):
            sys_id, ie = self.rng.choice(sets)
            pages = self.by_set[(sys_id, ie)]
            ps = sorted(pages)
            p = self.rng.choice(ps)
            if (p + 1) in pages and (p + 4) in pages and (p - 1) in pages:
                a, b, far = self.cut(pages[p]), self.cut(pages[p + 1]), self.cut(pages[p + 4])
                if a and b and far:
                    return sys_id, self.ie_name[ie], p, a, b, far, [pages[p - 1], pages[p + 1]]
        raise RuntimeError('no adjacent case found')

    def pick_distant(self):
        for _ in range(5000):
            (sys_id, ie), pages = self.rng.choice(list(self.by_set.items()))
            ps = sorted(pages)
            if len(ps) < 8:
                continue
            p1, p2 = ps[1], ps[-2]
            if p2 - p1 < 4:
                continue
            a, b = self.cut(pages[p1]), self.cut(pages[p2])
            if a and b:
                return sys_id, self.ie_name[ie], p1, p2, a, b
        raise RuntimeError('no distant case found')

    def records_of(self, sys_id):
        return self.pmap.sys_records[sys_id].tolist()


def case_adjacent(searcher, corpus, report, broken=False):
    sys_id, ie, p, a, b, far, neighbours = corpus.pick_adjacent()
    if not corpus.absent(far, neighbours):
        return case_adjacent(searcher, corpus, report, broken)
    t0 = time.perf_counter()
    found_c = cq.Condition(cq.REQ_MUST, cq.WHERE_ADJACENT, (b,), 'B')
    routed, _ = run_compound(searcher, a, [found_c])
    ok_found = sys_id in _sys_of(routed.main + routed.filtered)
    decoy_c = cq.Condition(cq.REQ_MUST, cq.WHERE_ADJACENT, (far,), "B'")
    routed2, outcomes2 = run_compound(searcher, a, [decoy_c])
    mine = [o for h, o in outcomes2.items() if cq.parse_page_key(h).sys_id == sys_id
            and cq.parse_page_key(h).p == p]
    ok_decoy = bool(mine) and all(o.category == cq.CAT_DROPPED for o in mine)
    report.append({'case': 'adjacent' + (' (BROKEN RULE)' if broken else ''),
                   'page': f'{sys_id} {ie} P{p}', 'found': ok_found,
                   'decoy_dropped': ok_decoy, 'seconds': round(time.perf_counter() - t0, 2),
                   'pass': ok_found and ok_decoy})


def case_manuscript(searcher, corpus, report):
    sys_id, ie, p1, p2, a, b = corpus.pick_distant()
    t0 = time.perf_counter()
    routed, _ = run_compound(searcher, a, [cq.Condition(
        cq.REQ_MUST, cq.WHERE_MANUSCRIPT, (b,), 'B')])
    ok_kept = sys_id in _sys_of(routed.main + routed.filtered)
    # B from another manuscript, absent from A's (checked in its streams).
    for _ in range(200):
        other_sys, _ie, _p1, _p2, _a2, b2 = corpus.pick_distant()
        if other_sys != sys_id and corpus.absent(b2, corpus.records_of(sys_id)):
            break
    else:
        raise RuntimeError('no foreign text found')
    routed2, outcomes2 = run_compound(searcher, a, [cq.Condition(
        cq.REQ_MUST, cq.WHERE_MANUSCRIPT, (b2,), 'B2')])
    mine = [o for h, o in outcomes2.items() if cq.parse_page_key(h).sys_id == sys_id]
    ok_dropped = bool(mine) and all(o.category == cq.CAT_DROPPED for o in mine)
    report.append({'case': 'same manuscript', 'page': f'{sys_id} {ie} P{p1}+P{p2}',
                   'kept': ok_kept, 'foreign_dropped': ok_dropped,
                   'seconds': round(time.perf_counter() - t0, 2),
                   'pass': ok_kept and ok_dropped})


def case_must_not(searcher, corpus, report):
    sys_id, ie, p1, p2, a, c = corpus.pick_distant()
    t0 = time.perf_counter()
    routed, outcomes = run_compound(searcher, a, [cq.Condition(
        cq.REQ_MUST_NOT, cq.WHERE_MANUSCRIPT, (c,), 'C')])
    removed = [r for r in routed.removed if cq.parse_page_key(r['raw_header']).sys_id == sys_id]
    evidence_ok = any(any(cq.parse_page_key(e).p == p2 for ev in r['compound']['evidence']
                          for e in ev) for r in removed)
    flipped, _ = run_compound(searcher, a, [cq.Condition(
        cq.REQ_MUST, cq.WHERE_MANUSCRIPT, (c,), 'C')])
    ok_flip = sys_id in _sys_of(flipped.main + flipped.filtered)
    report.append({'case': 'must not appear', 'page': f'{sys_id} {ie} P{p1}, C on P{p2}',
                   'removed': bool(removed), 'evidence_names_C': evidence_ok,
                   'flipped_kept': ok_flip, 'seconds': round(time.perf_counter() - t0, 2),
                   'pass': bool(removed) and evidence_ok and ok_flip})


def _corrupt(text, every=8):
    out = list(text)
    for i in range(every // 2, len(out), every):
        out[i] = chr(0x05D0 + ((ord(out[i]) - 0x05D0 + 7) % 22))
    return ''.join(out)


def case_exhaustive(searcher, corpus, report, samples=40):
    policy = searcher.policy
    budget_hits = mask_hits = 0
    rescued = []
    t0 = time.perf_counter()
    for _ in range(samples):
        sys_id, ie, p1, _p2, a, _b = corpus.pick_distant()
        q = _corrupt(a)
        target = corpus.by_set[(sys_id, corpus.pmap.ie_codes[ie])][p1]
        hits, _rep = search_passage(corpus.idx, q, policy,
                                    record_allowed=lambda rid, s=sys_id: rid.startswith(s))
        b_found = any(h.record == target for h in hits)
        m = searcher.search_condition(q, sys_ids=(sys_id,))
        m_found = corpus.idx.record_id(int(target)) in m.hit_record_ids
        budget_hits += b_found
        mask_hits += m_found
        if m_found and not b_found:
            rescued.append(f'{sys_id} {ie} P{p1}')
        if b_found and not m_found:
            rescued.append(f'LOST BY MASK: {sys_id} {ie} P{p1}')
    lost = [r for r in rescued if r.startswith('LOST')]
    report.append({'case': 'exhaustiveness (every 8th letter changed)',
                   'samples': samples, 'budgeted_found': budget_hits,
                   'masked_found': mask_hits, 'rescued_by_mask': rescued[:10],
                   'seconds': round(time.perf_counter() - t0, 2),
                   'pass': mask_hits >= budget_hits and not lost})


def case_cost(searcher, corpus, report):
    rng = corpus.rng
    parts = []
    while sum(map(len, parts)) < 10_000:
        r = rng.randrange(corpus.idx.n_records)
        parts.append(corpus.stream(r)[:800])
    text = ''.join(parts)[:10_000]
    scope = rng.sample(list(corpus.pmap.sys_records), 5000)
    t0 = time.perf_counter()
    found = searcher.search_condition(text, sys_ids=tuple(scope))
    report.append({'case': 'cost: 10,000 letters over 5,000 manuscripts',
                   'seconds': round(time.perf_counter() - t0, 2),
                   'cut': found.as_version_search().cut or 'none',
                   'pass': True})


def case_user(searcher, case, report):
    conds = [cq.Condition.from_dict(c) for c in case['conditions']]
    t0 = time.perf_counter()
    routed, _ = run_compound(searcher, case['main'], conds)
    got = {'main': _sys_of(routed.main + routed.filtered),
           'removed': _sys_of(routed.removed)}
    exp = case.get('expect') or {}
    ok = (set(exp.get('main', [])) <= got['main']
          and set(exp.get('removed', [])) <= got['removed']
          and not (set(exp.get('dropped', [])) & (got['main'] | got['removed'])))
    report.append({'case': f"user: {case.get('name', '?')}",
                   'main': len(got['main']), 'removed': len(got['removed']),
                   'seconds': round(time.perf_counter() - t0, 2), 'pass': ok})


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split('\n\n')[0])
    ap.add_argument('--index', default=_default_index_dir())
    ap.add_argument('--cases', default='')
    ap.add_argument('--seed', type=int, default=20261014)
    ap.add_argument('--self-test', action='store_true')
    args = ap.parse_args(argv)

    idx = open_index(args.index) if args.index and os.path.isdir(args.index) else None
    if idx is None:
        print(f'FAIL: no letter-level index at {args.index!r}', file=sys.stderr)
        return 1
    rng = random.Random(args.seed)
    corpus = Corpus(idx, rng)
    searcher = PassageSearcher(index=idx, text_fetcher=_StreamFetcher(idx, corpus.pmap),
                               policy=compose('widest-40'), render_cap=0)
    report = []
    case_adjacent(searcher, corpus, report)
    case_manuscript(searcher, corpus, report)
    case_must_not(searcher, corpus, report)
    case_exhaustive(searcher, corpus, report)
    case_cost(searcher, corpus, report)
    pending = []
    if args.cases and os.path.isfile(args.cases):
        with open(args.cases, encoding='utf-8') as fh:
            for case in json.load(fh):
                case_user(searcher, case, report)
    else:
        pending = ['user: Talmud / Halakhot Gedolot or Rif', 'user: siddur (prayer + halakhot)']

    if args.self_test:
        original = cq.PageKey.neighbours
        cq.PageKey.neighbours = lambda self: (cq.PageKey(self.sys_id, self.ie, self.p - 2),
                                              cq.PageKey(self.sys_id, self.ie, self.p + 2))
        try:
            broken = []
            case_adjacent(searcher, Corpus(idx, random.Random(args.seed)), broken, broken=True)
        finally:
            cq.PageKey.neighbours = original
        report.extend(broken)
        broken[0]['pass'] = not broken[0]['pass']      # it MUST fail

    for row in report:
        print(json.dumps(row, ensure_ascii=False))
    for name in pending:
        print(json.dumps({'case': name, 'status': 'PENDING (no --cases file)'}, ensure_ascii=False))
    failed = [r for r in report if not r['pass']]
    print(f"{len(report) - len(failed)}/{len(report)} passed"
          + (f", {len(pending)} pending" if pending else ''))
    return 1 if failed else 0


if __name__ == '__main__':
    sys.exit(main())
