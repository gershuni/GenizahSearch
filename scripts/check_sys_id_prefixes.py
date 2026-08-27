#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Re-measure which sys_id prefixes actually occur in this corpus.

``shared/sys_id_patterns.py`` restricts corpus extraction to the ``99``
namespace on the strength of a measurement. The corpus grows, so that
measurement has a shelf life -- this script re-takes it.

Exits non-zero if any corpus source yields a sys_id outside ``99``, which would
mean the corpus/LOCAL namespace split in shared/sys_id_patterns.py needs
revisiting before anything else is changed.

Sources, each skipped with a note when absent (the sidecars and the index are
gitignored / machine-local):
  * libraries.csv          -- master metadata, repo root
  * nli_crossref.db        -- NLI images/metadata sidecar
  * the Tantivy index      -- pass --index PATH

Usage:
    python scripts/check_sys_id_prefixes.py
    python scripts/check_sys_id_prefixes.py --index ~/Genizah_Tantivy_Index/tantivy_db
"""
from __future__ import annotations

import argparse
import collections
import csv
import os
import re
import sqlite3
import sys

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# Running this file directly puts `scripts/` on sys.path, not the repo root, so
# `import shared...` below would fail with ModuleNotFoundError. Insert the root
# here, at module level, so it is in place before ANY shared import -- including
# the deferred one inside scan_tantivy, which is the only caller that needs it.
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)
CORPUS_PREFIX = "99"
#: Documents per offset page when walking a Tantivy index.
TANTIVY_PAGE = 10000

#: Prefix-AGNOSTIC sys_id shape, and deliberately NOT one of the shared
#: constants. This script exists to discover whether a prefix OUTSIDE the known
#: namespaces has appeared; a pattern that only matches 99 and 97 cannot see a
#: 98, so using the constants under test as the instrument of the test would
#: make the check blind to its own subject. (An earlier revision did exactly
#: that and reported RESULT: OK on an index of nothing but 98-prefixed records.)
#: Any digit run of sys_id length at a digit boundary: the IE/P/FL components of
#: a header are all shorter than 10 digits, so they cannot be mistaken for one.
_ANY_PREFIX_SYS_ID_RE = re.compile(r'(?<!\d)(\d{10,})')
LOCAL_PREFIX = "97"


def _report(source, counter, samples):
    total = sum(counter.values())
    print(f"\n{source}: {total:,} sys_ids")
    for prefix, n in counter.most_common():
        flag = "" if prefix == CORPUS_PREFIX else "   <-- NOT the corpus prefix"
        print(f"    {prefix!r:6} {n:>10,}{flag}")
    off = {p: n for p, n in counter.items() if p != CORPUS_PREFIX}
    if off:
        for p in off:
            print(f"    sample {p!r}: {samples.get(p, [])[:3]}")
    return off


def scan_libraries_csv(path):
    if not os.path.exists(path):
        print(f"\nlibraries.csv: ABSENT at {path} -- skipped")
        return {}
    counter = collections.Counter()
    samples = collections.defaultdict(list)
    lengths = collections.Counter()
    with open(path, encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f)
        next(reader, None)
        for row in reader:
            if not row:
                continue
            sid = (row[0] or "").strip()
            if not sid.isdigit():
                continue  # '# BEGIN SYNTHETIC' markers etc.
            counter[sid[:2]] += 1
            lengths[len(sid)] += 1
            if len(samples[sid[:2]]) < 3:
                samples[sid[:2]].append(sid)
    off = _report("libraries.csv", counter, samples)
    print(f"    digit lengths: {dict(lengths)}")
    return off


def scan_nli_crossref(path):
    if not os.path.exists(path):
        print(f"\nnli_crossref.db: ABSENT at {path} -- skipped")
        return {}
    counter = collections.Counter()
    samples = collections.defaultdict(list)
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        tables = [r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'")]
        for table in tables:
            cols = [r[1] for r in conn.execute(f'PRAGMA table_info("{table}")')]
            for col in cols:
                if "sys_id" not in col.lower():
                    continue
                for (val,) in conn.execute(f'SELECT DISTINCT "{col}" FROM "{table}"'):
                    sid = str(val or "").strip()
                    if not sid.isdigit():
                        continue
                    counter[sid[:2]] += 1
                    if len(samples[sid[:2]]) < 3:
                        samples[sid[:2]].append(sid)
    finally:
        conn.close()
    if not counter:
        print("\nnli_crossref.db: no sys_id-bearing columns found -- skipped")
        return {}
    return _report("nli_crossref.db", counter, samples)


def scan_tantivy(index_dir):
    if not index_dir:
        print("\nTantivy index: not requested (pass --index PATH) -- skipped")
        return {}
    if not os.path.isdir(index_dir):
        print(f"\nTantivy index: ABSENT at {index_dir} -- skipped")
        return {}
    try:
        import tantivy  # noqa: F401
    except ImportError:
        print("\nTantivy index: `tantivy` not installed -- skipped")
        return {}
    # Imported to CROSS-CHECK the constant against real data, never to do the
    # prefix detection itself (see _ANY_PREFIX_SYS_ID_RE).
    from shared.sys_id_patterns import extract_corpus_sys_id

    index = tantivy.Index.open(index_dir)
    searcher = index.searcher()
    counter = collections.Counter()
    samples = collections.defaultdict(list)

    # Enumerate through the Python binding's supported surface. There is no
    # `Searcher.segment_readers()`; the binding exposes `num_docs`, `search`
    # and `doc`, so walk a match-all query in offset pages. One searcher is
    # held for the whole walk, so paging is over a fixed index snapshot.
    total = searcher.num_docs
    query = tantivy.Query.all_query()

    # Every document lands in EXACTLY ONE of these three, and they must sum to
    # `total`. A document that could not be read, or whose header carried no
    # sys_id-shaped run, is NOT an inspected document -- counting it as one is
    # how a walk that examined nothing still reported clean.
    classified = 0
    unreadable = 0
    unparsed = 0
    unreadable_samples = []
    unparsed_samples = []
    missed = []  # ids the shared CORPUS pattern failed to pull from the header

    offset = 0
    while offset < total:
        hits = searcher.search(
            query, limit=TANTIVY_PAGE, offset=offset, count=False).hits
        if not hits:
            break
        for _score, address in hits:
            try:
                header = str(searcher.doc(address).get_first("full_header") or "")
            except Exception as exc:
                unreadable += 1
                if len(unreadable_samples) < 3:
                    unreadable_samples.append(f"{type(exc).__name__}: {exc}")
                continue
            match = _ANY_PREFIX_SYS_ID_RE.search(header)
            if not match:
                unparsed += 1
                if len(unparsed_samples) < 3:
                    unparsed_samples.append(header[:80] or "<empty>")
                continue
            sid = match.group(1)
            classified += 1
            counter[sid[:2]] += 1
            if len(samples[sid[:2]]) < 3:
                samples[sid[:2]].append(sid)
            # The script's own subject: does the shared corpus constant actually
            # pull this id out of this real header?
            if sid.startswith(CORPUS_PREFIX) and extract_corpus_sys_id(header) != sid:
                if len(missed) < 3:
                    missed.append((header[:80], sid))
        offset += len(hits)

    print(f"\n(of {total:,} index documents: {classified:,} classified, "
          f"{unreadable:,} unreadable, {unparsed:,} with no sys_id)")

    problems = {}
    seen = classified + unreadable + unparsed
    if seen != total:
        print(f"    INCOMPLETE WALK: accounted for {seen:,} of {total:,} documents.")
        problems["incomplete walk"] = seen
    if unreadable:
        print(f"    UNREADABLE: {unreadable:,} document(s) could not be read; "
              f"e.g. {unreadable_samples}")
        problems["unreadable documents"] = unreadable
    if unparsed:
        print(f"    NO SYS_ID: {unparsed:,} header(s) carried no sys_id-shaped "
              f"run; e.g. {unparsed_samples}")
        problems["headers with no sys_id"] = unparsed
    if total and not classified:
        print("    NOTHING CLASSIFIED: not one header yielded a sys_id.")
        problems["nothing classified"] = total
    if missed:
        print(f"    CORPUS PATTERN MISSED its own id in {len(missed)} header(s); "
              f"e.g. {missed}")
        problems["corpus pattern missed"] = len(missed)

    off = _report("Tantivy index", counter, samples)
    if problems:
        # A walk with holes proves nothing either way, so it must not be able to
        # come back clean -- whatever the prefixes it did manage to see.
        return {"__walk_problems__": problems}
    return off


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--libraries", default=os.path.join(REPO_ROOT, "libraries.csv"))
    ap.add_argument("--nli-crossref", default=os.path.join(REPO_ROOT, "nli_crossref.db"))
    ap.add_argument("--index", default=None,
                    help="path to the Tantivy index dir (e.g. ~/Genizah_Tantivy_Index/tantivy_db)")
    args = ap.parse_args()

    print("Measuring sys_id prefixes actually present in this corpus.")
    print(f"Corpus namespace = {CORPUS_PREFIX!r};  LOCAL 'My Library' namespace = "
          f"{LOCAL_PREFIX!r} (never a corpus record).")

    off = {}
    off.update(scan_libraries_csv(args.libraries))
    off.update(scan_nli_crossref(args.nli_crossref))
    off.update(scan_tantivy(os.path.expanduser(args.index) if args.index else None))

    print()
    walk_problems = off.pop("__walk_problems__", None)
    if walk_problems:
        print("RESULT: FAIL -- the index walk had holes, so this run proves")
        print("nothing either way:")
        for kind, count in walk_problems.items():
            print(f"    - {kind}: {count:,}")
        print("Fix the walk before trusting the measurement.")
        return 1
    if off:
        print("RESULT: FAIL -- a non-99 prefix is present in corpus data.")
        print("Revisit the namespace split in shared/sys_id_patterns.py BEFORE")
        print("changing any call site; the corpus-only narrowing no longer holds.")
        return 1
    print("RESULT: OK -- every corpus sys_id seen begins '99'.")
    print("The corpus-only narrowing in shared/sys_id_patterns.py still holds.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
