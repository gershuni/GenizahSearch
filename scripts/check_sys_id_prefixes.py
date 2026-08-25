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
import sqlite3
import sys

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CORPUS_PREFIX = "99"
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
    from shared.sys_id_patterns import ANY_SYS_ID_RE
    index = tantivy.Index.open(index_dir)
    searcher = index.searcher()
    counter = collections.Counter()
    samples = collections.defaultdict(list)
    scanned = 0
    for seg in searcher.segment_readers():
        for doc_id in range(seg.num_docs()):
            try:
                doc = searcher.doc(tantivy.DocAddress(seg.segment_id(), doc_id))
            except Exception:
                continue
            header = (doc.get_first("full_header") or "")
            m = ANY_SYS_ID_RE.search(str(header))
            scanned += 1
            if not m:
                continue
            sid = m.group(1)
            counter[sid[:2]] += 1
            if len(samples[sid[:2]]) < 3:
                samples[sid[:2]].append(sid)
    print(f"\n(scanned {scanned:,} index documents)")
    return _report("Tantivy index", counter, samples)


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
