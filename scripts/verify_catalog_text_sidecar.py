#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Gate ``catalog_data/catalog_text.db`` before it is served or shipped.

    python scripts/verify_catalog_text_sidecar.py --db catalog_data/catalog_text.db --expect-rows <N>

Plan: docs/plans/CATALOGUE_TEXT_SEARCH_PLAN.md §4.1. It FAILS (exit 1) rather than skips: a
missing or empty file, or any check below that does not hold, is a failure with its reason.

1. Schema: the tables, the ``records`` columns, the sys_id index, the FTS5 columns and tokenizer,
   all as ``scripts/build_catalog_text_sidecar.py`` writes them.
2. ``meta``: the schema version this repository's reader serves, the normalizer version this
   repository runs (a file keyed by another normalizer would make every search miss).
3. Counts: ``records`` holds exactly ``--expect-rows`` rows, ``meta.rows_total`` says the same,
   the FTS5 table indexes one document per record, and every ``meta.rows_<source>_<kind>`` matches
   the rows of that source and kind.
4. Every record: a corpus sys_id, a known (source, field), language ``he``/``en`` matching its
   text, ``automatic`` 0 or 1, non-empty text with at least one word.
5. Keys: for a seeded sample of records (``--sample``, all of them with ``--sample 0``), the
   record's own words, recomputed by today's normalizer, find that very record in the FTS5 index
   (a stale normalizer or misaligned rowids fail here).
6. Canary searches through the reader (``shared.catalog_text_service``): each finds at least its
   floor of manuscripts (``CANARIES``, floors at about half of what the first build found).
7. ``PRAGMA quick_check``.
"""
from __future__ import annotations

import argparse
import random
import sqlite3
import sys
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.build_catalog_text_sidecar import (  # noqa: E402  -- the one schema definition
    COLUMN_OF,
    FTS_COLUMNS,
    SCHEMA_VERSION,
    SOURCES,
    TOKENIZER,
)
from shared.catalog_text_normalize import NORMALIZER_VERSION, is_hebrew_text, key_words  # noqa: E402
from shared.catalog_text_service import CatalogTextService  # noqa: E402
from shared.sys_id_patterns import CORPUS_SYS_ID_RE  # noqa: E402

RECORD_COLUMNS = ("id", "sys_id", "source", "field", "lang", "automatic", "text")
#: (query, minimum manuscripts). The first full build (2026-11-05) found the counts in the
#: comments; the floors are about half, so a data refresh passes and a broken key does not.
CANARIES = (
    ("כתובה", 1000),             # 2,212+ exact
    ("גבינה", 15),               # 37+
    ("marriage contract", 400),  # 966+
    ("ketubba", 450),            # 1,043+ (mostly PGP)
    ('רמב"ם', 800),              # 1,883+ exact
    ("Maimonides", 2500),        # 5,086+
    ("cheese", 50),              # 123+
    ("תפילין", 150),             # 368+
)


def schema_problems(con: sqlite3.Connection) -> list[str]:
    problems: list[str] = []
    tables = {r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    for table in ("meta", "records", "catalog_fts"):
        if table not in tables:
            problems.append(f"missing table {table}")
    if problems:
        return problems
    columns = tuple(r[1] for r in con.execute("PRAGMA table_info(records)"))
    if columns != RECORD_COLUMNS:
        problems.append(f"records columns {columns} != {RECORD_COLUMNS}")
    indexes = {r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type='index'")}
    if "idx_records_sys_id" not in indexes:
        problems.append("missing index idx_records_sys_id")
    fts_sql = con.execute("SELECT sql FROM sqlite_master WHERE name='catalog_fts'").fetchone()[0] or ""
    fts_columns = tuple(r[1] for r in con.execute("PRAGMA table_info(catalog_fts)"))
    if fts_columns != FTS_COLUMNS:
        problems.append(f"catalog_fts columns {fts_columns} != {FTS_COLUMNS}")
    if f"tokenize='{TOKENIZER}'" not in fts_sql or "content=''" not in fts_sql:
        problems.append(f"catalog_fts is not contentless with tokenizer {TOKENIZER!r}: {fts_sql}")
    return problems


def verify(db: str, expect_rows: int, sample: int = 2000, seed: int = 20261105,
           canaries=CANARIES, quick_check: bool = True) -> list[str]:
    path = Path(db)
    if not path.is_file() or path.stat().st_size == 0:
        return [f"no sidecar (or an empty file) at {db}"]
    con = sqlite3.connect(f"file:{path.resolve().as_posix()}?mode=ro", uri=True)
    try:
        problems = schema_problems(con)
        if problems:
            return problems
        meta = dict(con.execute("SELECT key, value FROM meta").fetchall())
        if meta.get("version") != SCHEMA_VERSION:
            problems.append(f"meta.version {meta.get('version')!r} != {SCHEMA_VERSION!r}")
        if meta.get("normalizer_version") != str(NORMALIZER_VERSION):
            problems.append(f"meta.normalizer_version {meta.get('normalizer_version')!r} != {NORMALIZER_VERSION}")

        rows = con.execute("SELECT COUNT(*) FROM records").fetchone()[0]
        if rows != expect_rows:
            problems.append(f"records {rows:,} != expected {expect_rows:,}")
        if meta.get("rows_total") != str(rows):
            problems.append(f"meta.rows_total {meta.get('rows_total')!r} != records {rows:,}")
        indexed = con.execute("SELECT COUNT(*) FROM catalog_fts_docsize").fetchone()[0]
        if indexed != rows:
            problems.append(f"catalog_fts indexes {indexed:,} documents, records holds {rows:,}")
        grouped = Counter({f"{s}_{'automatic' if a else 'original'}": n for s, a, n in
                           con.execute("SELECT source, automatic, COUNT(*) FROM records GROUP BY 1, 2")})
        for source in SOURCES:
            for kind in ("original", "automatic"):
                want = meta.get(f"rows_{source}_{kind}", "")
                have = grouped.get(f"{source}_{kind}", 0)
                if want != str(have):
                    problems.append(f"meta.rows_{source}_{kind} {want!r} != {have:,} records")

        bad = Counter()
        first_bad: dict[str, tuple] = {}
        for rec in con.execute("SELECT id, sys_id, source, field, lang, automatic, text FROM records"):
            rid, sid, source, field, lang, automatic, text = rec
            reason = None
            if not CORPUS_SYS_ID_RE.fullmatch(sid or ""):
                reason = "not a corpus sys_id"
            elif (source, field) not in COLUMN_OF:
                reason = "unknown (source, field)"
            elif automatic not in (0, 1):
                reason = "automatic not 0/1"
            elif not text or not key_words(text):
                reason = "no words"
            elif lang != ("he" if is_hebrew_text(text) else "en"):
                reason = "language label disagrees with the text"
            if reason:
                bad[reason] += 1
                first_bad.setdefault(reason, (rid, sid, source, field))
        for reason, count in bad.items():
            problems.append(f"{count:,} record(s): {reason} (first: {first_bad[reason]})")

        ids = [r[0] for r in con.execute("SELECT id FROM records ORDER BY id")]
        chosen = ids if sample == 0 or sample >= len(ids) else random.Random(seed).sample(ids, sample)
        missing = []
        for rid in chosen:
            text = con.execute("SELECT text FROM records WHERE id = ?", (rid,)).fetchone()[0]
            words = key_words(text)[:3]
            if not words:
                continue
            match = '"' + " ".join(words) + '"'
            found = con.execute("SELECT 1 FROM catalog_fts WHERE catalog_fts MATCH ? AND rowid = ?",
                                (match, rid)).fetchone()
            if found is None:
                missing.append(rid)
        if missing:
            problems.append(f"{len(missing):,} of {len(chosen):,} sampled records are not found by their own "
                            f"words (first ids: {missing[:5]})")

        if quick_check:
            check = con.execute("PRAGMA quick_check").fetchone()
            if check is None or check[0] != "ok":
                problems.append(f"quick_check: {check[0] if check else 'no answer'}")
    finally:
        con.close()

    if not problems and canaries:
        service = CatalogTextService(db_path=str(path))
        if not service.is_ready():
            problems.append(f"the reader refuses the file: {service.unavailable_reason}")
        else:
            for query, floor in canaries:
                total = service.search(query, limit=0).total
                if total < floor:
                    problems.append(f"canary {query!r} finds {total:,} manuscripts, floor {floor:,}")
            service.close()
    return problems


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--db", required=True)
    ap.add_argument("--expect-rows", type=int, required=True)
    ap.add_argument("--sample", type=int, default=2000, help="records whose keys are re-checked (0 = all)")
    args = ap.parse_args(argv)
    problems = verify(args.db, args.expect_rows, sample=args.sample, canaries=CANARIES)
    if problems:
        for problem in problems:
            print(f"FAIL: {problem}")
        print(f"RESULT: FAIL ({len(problems)} problem(s))")
        return 1
    print(f"RESULT: OK ({args.db}, {args.expect_rows:,} records)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
