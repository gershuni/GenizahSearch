# -*- coding: utf-8 -*-
"""Gate for the editions search on the real indexes (editions search plan, §3.8, PR 2 checks).

    python scripts/verify_editions_search.py --gen <generation dir> [--records N] [--no-timing]
    python scripts/verify_editions_search.py --gen <generation dir> --self-test

The checks run ``SearchEngine.execute_search`` on the main index with the generation attached
(``attach_editions``), in-process:

* **findability** -- deterministic records (``verify_editions_index.content_records``: the first
  ones by id of each source and text source), a 3-word span of a kept line of the expected search
  text, searched with editions only in Exact, Variants and Responsa: a result must hold a member
  with that record's edition uid. With ``--with-main N``, the first N records also with the
  automatic transcriptions and editions together (the main leg running first).
* **line-break** -- for each record, its first pair of consecutive kept lines that both hold a
  plain word: ``<last word of the line> | <first word of the next>`` in Responsa finds that uid.
  Zero pairs fails.
* **timing** (reported) -- per mode, a fixed list of queries run as the coordinator runs a
  search with editions, its parts timed apart (each the fastest of three runs, after a warm-up):
  what the editions leg and the grouping add to the main leg, at the median and at the 90th
  percentile of the mode's queries. In this process the legs run one after the other, as on the
  desktop: this is the desktop's cost, which the owner accepted on 2026-11-15, and it is reported,
  not judged. The 20% limit applies to the website's wall-clock time, with the editions leg in a
  research worker of its own beside the main one (PR 3 checks it). ``--timing-limit 0.2`` makes
  this check fail above a limit all the same.

Every check reports how many cases it ran and fails on zero. It also reports the located share
of the edition hits it saw. ``--self-test`` reruns findability and line-break on a few records
with a planted fault each (the leaf keeping only V0.8/V0.7 rows; the leaf flattening line
breaks) and requires them to fail: proof that the gate can fail.
"""
from __future__ import annotations

import argparse
import os
import statistics
import sys
import time
from collections import Counter
from contextlib import contextmanager
from typing import List, Optional, Tuple
from unittest.mock import patch

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
for p in (ROOT, os.path.join(ROOT, "scripts")):
    if p not in sys.path:
        sys.path.insert(0, p)

from verify_editions_index import _HEB_WORD, Result, _span, content_records, expected_fgp, expected_pgp  # noqa: E402

R = {"responsa_mode": True}
FIND_MODES = (("Exact", "literal", None), ("Variants", "variants", None), ("Responsa", "literal", R))
#: Timing queries per mode: common and rare words and phrases (owner-visible numbers are not
#: claims; the threshold is the plan's).
TIMING = {
    "literal": ["שלום", "ברוך אתה", "בית דין", "אמר רבי יוחנן", "משה רבנו", "דינרים", "ירושלים", "כתב ידי"],
    "variants": ["שלום", "בית דין", "משה רבנו", "ירושלים", "דינרים"],
    "fuzzy": ["ירושלים", "מצרים", "שמואל", "דינרים"],
    "Regex": ["ש.לום", "בית ד[יו]ן", "ירוש.?לים"],
    "responsa": ["של*", "בית דין", "משה רב*", "ירושלים"],
}


class Located:
    def __init__(self):
        self.counts = Counter()

    def see(self, rows) -> None:
        for r in rows or []:
            for m in r.get("members") or []:
                e = m.get("edition")
                if e:
                    self.counts["located" if e.get("located") else "unlocated"] += 1

    def line(self) -> str:
        n = sum(self.counts.values())
        if not n:
            return "located share of edition hits: no edition hits seen"
        return (f"located share of edition hits: {self.counts['located']:,} of {n:,} "
                f"({100.0 * self.counts['located'] / n:.1f}%)")


def make_engine(gen_dir: str):
    from shared.metadata_manager import MetadataManager
    from shared.search_engine import SearchEngine
    from shared.variants import VariantManager
    meta = MetadataManager()
    loader = getattr(meta, "_load_heavy_caches_bg", None)
    if loader is not None:
        loader()
    engine = SearchEngine(meta, VariantManager(), worker_mode=True, open_local=False)
    if engine.searcher is None:
        raise SystemExit("the main index did not open")
    started = time.perf_counter()
    if not engine.attach_editions(gen_dir):
        raise SystemExit(f"the editions generation at {gen_dir} is not ready")
    return engine, time.perf_counter() - started


def _member_uids(rows) -> set:
    return {m.get("uid") for r in rows or [] for m in (r.get("members") or [])}


def check_findability(engine, records, located: Located, with_main: int = 0) -> Result:
    from shared.index_seams import hebword_analyzer
    res = Result("findability")
    analyzer = hebword_analyzer()
    for i, rec in enumerate(records):
        trio = _span(rec["text"], analyzer)
        if not trio:
            continue
        query = " ".join(trio)
        runs = [("editions", label, mode, opts) for label, mode, opts in FIND_MODES]
        if i < with_main:
            runs.append(("all", "Exact", "literal", None))
        for sources, label, mode, opts in runs:
            res.cases += 1
            rows = engine.execute_search(query, mode, 0, sources=sources, corpus_scope="genizah",
                                         responsa_options=opts)
            located.see(rows)
            if rec["uid"] not in _member_uids(rows):
                res.fail(f"{rec['uid']} [{label}, {sources}]: {query!r} not found ({len(rows)} results)")
    return res


def line_pair(text: str, analyzer) -> Optional[Tuple[str, str]]:
    """The first two consecutive kept lines that both hold a plain Hebrew word: the last such
    word of the first and the first of the second."""
    lines = [ln for ln in text.split("\n") if ln.strip()]
    for a, b in zip(lines, lines[1:]):
        wa = [w for w in analyzer.analyze(a) if _HEB_WORD.match(w)]
        wb = [w for w in analyzer.analyze(b) if _HEB_WORD.match(w)]
        if wa and wb:
            return wa[-1], wb[0]
    return None


def check_line_break(engine, records, located: Located) -> Result:
    from shared.index_seams import hebword_analyzer
    res = Result("line-break")
    analyzer = hebword_analyzer()
    for rec in records:
        pair = line_pair(rec["text"], analyzer)
        if not pair:
            continue
        res.cases += 1
        query = f"{pair[0]} | {pair[1]}"
        rows = engine.execute_search(query, "literal", 0, sources="editions", corpus_scope="genizah",
                                     responsa_options=R)
        located.see(rows)
        if rec["uid"] not in _member_uids(rows):
            res.fail(f"{rec['uid']}: {query!r} not found ({len(rows)} results)")
    return res


def _clock(fn):
    started = time.perf_counter()
    value = fn()
    return time.perf_counter() - started, value


def _legs(engine, query, mode, opts):
    """(main leg, editions leg, grouping) seconds of one search with editions, run as the
    coordinator runs it (execute_search(sources='all')), each part timed on its own, and the
    grouped rows."""
    from shared.search_engine import consume_last_search_cutoff
    editions = engine._editions_for_request()
    main_leg, editions_leg = {}, {}
    engine._clear_search_state()
    kw = dict(corpus_scope="genizah", responsa_options=opts)
    t_main, main_rows = _clock(lambda: engine._execute_search_leg(query, mode, 0, _leg=main_leg, **kw))
    t_ed, ed_rows = _clock(lambda: editions._execute_search_leg(query, mode, 0, _leg=editions_leg, **kw))
    t_group, rows = _clock(lambda: engine._grouped_rows(main_rows, ed_rows, main_leg, editions_leg, False))
    consume_last_search_cutoff()
    return t_main, t_ed, t_group, rows


def _p90(values: List[float]) -> float:
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int(round(0.9 * (len(ordered) - 1))))]


def check_timing(engine, located: Located, threshold: Optional[float], rounds: int = 3) -> Result:
    """Per mode, what the editions add to a search -- the editions leg and the grouping -- against
    the main leg, each the fastest of *rounds* runs after a warm-up (the parts are timed apart, so
    the machine's other load does not enter as the difference of two totals)."""
    res = Result("timing")
    for name, queries in TIMING.items():
        mode, opts = ("literal", R) if name == "responsa" else (name, None)
        main_t, added_t = [], []
        for q in queries:
            _legs(engine, q, mode, opts)                                  # warm-up
            runs = [_legs(engine, q, mode, opts) for _ in range(rounds)]
            m = min(r[0] for r in runs)
            added = min(r[1] for r in runs) + min(r[2] for r in runs)
            located.see(runs[0][3])
            main_t.append(m)
            added_t.append(added)
            res.cases += 1
            print(f"         {name:9} {q:16} main {m * 1000:7.0f} ms, editions leg + grouping "
                  f"{added * 1000:7.0f} ms ({100 * added / m if m else 0:5.1f}%)")
        for stat, f in (("median", statistics.median), ("p90", _p90)):
            base, added = f(main_t), f(added_t)
            share = added / base if base > 0 else 0.0
            print(f"       {name:9} {stat:6} main leg {base * 1000:8.0f} ms, editions add "
                  f"{added * 1000:8.0f} ms: +{100 * share:5.1f}%")
            if threshold is not None and share > threshold:
                res.fail(f"{name} {stat}: the editions add {100 * share:.1f}% (> {100 * threshold:.0f}%)")
    return res


@contextmanager
def planted(fault: str):
    """A fault in the leaf, for the self-test."""
    from shared.search_engine import SearchEngine
    if fault == "main source policy":
        original = SearchEngine._deduplicate

        def main_policy(self, results, first_wins=False, pages_win=False):
            leaf = self._edition_leaf
            self._edition_leaf = False
            try:
                return original(self, results, first_wins, pages_win)
            finally:
                self._edition_leaf = leaf
        with patch.object(SearchEngine, "_deduplicate", main_policy):
            yield
    elif fault == "flattened lines":
        original = SearchEngine._edition_members

        def flat(self, doc, content, *a, **k):
            return original(self, doc, content.replace("\n", " "), *a, **k)
        with patch.object(SearchEngine, "_edition_members", flat):
            yield
    else:
        raise ValueError(fault)


def main(argv=None) -> int:
    from build_editions_index import default_inputs
    d = default_inputs()
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--gen", required=True, help="the generation directory")
    p.add_argument("--fgp-db", default=d["fgp_db"])
    p.add_argument("--pgp-db", default=d["pgp_db"])
    p.add_argument("--records", type=int, default=40, help="records per source and text source")
    p.add_argument("--with-main", type=int, default=20, help="records also searched with the main leg")
    p.add_argument("--timing-limit", type=float, default=None,
                   help="fail the timing above this share (default: report only, as the desktop's cost)")
    p.add_argument("--no-timing", action="store_true")
    p.add_argument("--timing-only", action="store_true")
    p.add_argument("--self-test", action="store_true", help="prove the checks can fail")
    args = p.parse_args(argv)

    expected = dict(expected_fgp(args.fgp_db))
    expected.update(expected_pgp(args.pgp_db))
    engine, open_s = make_engine(args.gen)
    print(f"editions index opened in {open_s * 1000:.0f} ms")
    located = Located()
    if args.self_test:
        records = content_records(expected, per_group=3)
        ok = True
        for fault, check in (("main source policy", check_findability), ("flattened lines", check_line_break)):
            with planted(fault):
                r = check(engine, records, Located())
            caught = not r.ok
            ok = ok and caught
            print(f"[{'CAUGHT' if caught else 'MISSED'}] {fault}: {r.name} {'failed' if caught else 'passed'}")
        clean = [check_findability(engine, records, Located()), check_line_break(engine, records, Located())]
        for r in clean:
            print("clean:", r.line())
        ok = ok and all(r.ok for r in clean)
        print("SELF-TEST PASS" if ok else "SELF-TEST FAIL")
        return 0 if ok else 1

    records = content_records(expected, per_group=args.records)
    results = []
    if not args.timing_only:
        results = [check_findability(engine, records, located, with_main=args.with_main),
                   check_line_break(engine, records, located)]
    for r in results:
        print(r.line())
    if not args.no_timing:
        timing = check_timing(engine, located, args.timing_limit)
        if args.timing_limit is None:
            print(f"[REPORT] timing: {timing.cases:,} cases (the desktop's sequential cost; not judged)")
            if not timing.cases:
                results.append(timing)          # no case ran: that still fails
        else:
            results.append(timing)
            print(timing.line())
    print(located.line())
    ok = all(r.ok for r in results)
    print("GATE PASS" if ok else "GATE FAIL")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
