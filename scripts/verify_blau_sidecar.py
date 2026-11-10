#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Gate for ``blau_data/blau.db`` before it is served: integrity, schema, counts, lookup keys, credit.

    python scripts/verify_blau_sidecar.py --db blau_data/blau.db --expect-roots 3047 --expect-forms 12753
    python scripts/verify_blau_sidecar.py --db blau_data/blau.db --expect-roots 3047 --expect-forms 12753 \\
        --expect-tokens <count> --expect-source-sha256 <hex of semantic_blau.json.gz>

FAILS (exit 1) rather than skips on a missing file. Every check is a sentence on its own line, so a
mutated sidecar says what changed: a dropped row fails the count checks, an edited lookup key fails
the recomputation (every ``forms_norm.norm`` is recomputed with the ``shared.blau_normalize`` that is
running, so a sidecar built under other rules is refused too), an edited ``meta`` count fails the
meta-versus-table check, a dangling source id fails the citation check, an edited or dropped token row
fails the recomputation of the expression search's ``tokens`` from the names and citations with the
``shared.ja_expression`` that is running (schema 1.1.0). Opens the file read-only.
"""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys

_SCRIPTS_DIR = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.dirname(_SCRIPTS_DIR)
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from scripts.build_blau_sidecar import DDL, SCHEMA_VERSION  # noqa: E402  -- the one schema definition
from shared.blau_service import name_alternatives  # noqa: E402
from shared.blau_normalize import NORMALIZER_VERSION, normalize_ja  # noqa: E402
from shared.ja_expression import KEYING_VERSION, scope_keys  # noqa: E402

EXPECTED_TABLES = ("meta", "sources", "roots", "sub_entries", "stems", "forms", "citations", "forms_norm", "tokens")
COUNTED_TABLES = ("roots", "sub_entries", "stems", "forms", "citations", "forms_norm", "sources", "tokens")


def _schema(con: sqlite3.Connection) -> dict:
    """{table: [(column, declared type, notnull, pk)]} and {'<indexes>': [names]} of a connection."""
    out = {}
    for (name,) in con.execute("SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'"):
        # The table-valued pragma takes the name as a bound parameter, so a stray table whose
        # name needs quoting ("stray-table") is reported, not a traceback (Codex round 4).
        out[name] = [(r[1], (r[2] or "").upper(), r[3], r[5])
                     for r in con.execute("SELECT * FROM pragma_table_info(?)", (name,))]
    out["<indexes>"] = sorted(r[0] for r in con.execute(
        "SELECT name FROM sqlite_master WHERE type = 'index' AND name NOT LIKE 'sqlite_%'"))
    return out


def expected_schema() -> dict:
    """The schema the builder writes, read back from an in-memory database built with its DDL."""
    con = sqlite3.connect(":memory:")
    try:
        con.executescript(DDL)
        return _schema(con)
    finally:
        con.close()


def schema_problems(con: sqlite3.Connection) -> list[str]:
    """Every table, column (name, type, NOT NULL, primary key) and index, compared with the DDL."""
    want, have = expected_schema(), _schema(con)
    problems = []
    for table, columns in want.items():
        if table == "<indexes>":
            continue
        if table not in have:
            problems.append(f"table {table} is missing")
        elif have[table] != columns:
            missing = [c[0] for c in columns if c[0] not in {h[0] for h in have[table]}]
            extra = [c[0] for c in have[table] if c[0] not in {w[0] for w in columns}]
            changed = [c[0] for c in columns if c[0] not in missing
                       and next(h for h in have[table] if h[0] == c[0]) != c]
            problems.append(f"table {table} does not match the schema: missing {missing}, extra {extra}, "
                            f"changed {changed}")
    for table in have:
        if table != "<indexes>" and table not in want:
            problems.append(f"table {table} is not part of the schema")
    if have["<indexes>"] != want["<indexes>"]:
        problems.append(f"indexes {have['<indexes>']} != expected {want['<indexes>']}")
    return problems
REQUIRED_META = ("version", "source_release", "source_sha256", "source_json_sha256", "built_at",
                 "normalizer_version", "keying_version", "credit_he", "credit_en")
NORM_KINDS = ("root", "sub_entry", "headword", "variant", "phrase")


def expected_tokens(con: sqlite3.Connection) -> list[tuple]:
    """The ``tokens`` rows the builder writes for the names and citations the file holds, recomputed
    with the keying that is running: one scope per name alternative and per quoted citation."""
    rows: list[tuple] = []

    def add(scope, scope_id, n, root_id, sub_entry_id, text):
        for pos, key, peeled in scope_keys(text):
            rows.append((scope, scope_id, n, root_id, sub_entry_id, pos, key, peeled))

    for root_id, names_json in con.execute("SELECT id, names_json FROM roots ORDER BY ordinal"):
        for n, name in enumerate(json.loads(names_json or "[]")):
            add("root", root_id, n, root_id, None, name)
    for sub_id, root_id, names_json in con.execute(
            "SELECT id, root_id, names_json FROM sub_entries ORDER BY root_id, ordinal"):
        for n, name in enumerate(json.loads(names_json or "[]")):
            add("sub_entry", sub_id, n, root_id, sub_id, name)
    for form_id, root_id, sub_entry_id, ja_text in con.execute(
            "SELECT id, root_id, sub_entry_id, ja_text FROM forms ORDER BY id"):
        for n, name in enumerate(name_alternatives(ja_text)):
            add("form", form_id, n, root_id, sub_entry_id, name)
    for cit_id, root_id, sub_entry_id, ja_text in con.execute(
            "SELECT c.id, f.root_id, f.sub_entry_id, c.ja_text FROM citations c JOIN forms f ON f.id = c.form_id "
            "WHERE c.ja_text IS NOT NULL AND c.ja_text <> '' ORDER BY c.id"):
        add("citation", cit_id, 0, root_id, sub_entry_id, ja_text)
    return rows


def verify(db: str, expect_roots: int, expect_forms: int, expect_source_sha256: str | None = None,
           expect_version: str | None = None, expect_tokens: int | None = None) -> list[str]:
    """Every problem found, as one sentence each; an empty list is a pass."""
    problems: list[str] = []
    if not os.path.isfile(db):
        return [f"{db} does not exist"]
    if os.path.getsize(db) == 0:
        return [f"{db} is empty"]
    try:
        con = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    except sqlite3.Error as exc:
        return [f"{db} cannot be opened: {exc}"]
    try:
        try:
            integrity = con.execute("PRAGMA integrity_check").fetchone()[0]
        except sqlite3.Error as exc:
            return [f"{db} is not a SQLite database: {exc}"]
        if integrity != "ok":
            problems.append(f"integrity_check: {integrity}")
        tables = {r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}
        missing = [t for t in EXPECTED_TABLES if t not in tables]
        if missing:
            problems.append(f"tables missing: {missing}")
            return problems
        # Columns, types, constraints and indexes against the builder's DDL (Codex round 2: the
        # table names alone let a file with a dropped column pass).
        schema = schema_problems(con)
        if schema:
            # A structurally different file cannot be queried by the checks below (a missing
            # column would raise); say what differs and stop (Codex round 3).
            problems.extend(schema)
            problems.append("schema differs from the builder's DDL; no further checks were run")
            return problems
        fk = con.execute("PRAGMA foreign_key_check").fetchall()
        if fk:
            problems.append(f"{len(fk)} foreign-key rows do not resolve (first: {fk[0]})")

        meta = dict(con.execute("SELECT key, value FROM meta").fetchall())
        for key in REQUIRED_META:
            if not meta.get(key):
                problems.append(f"meta.{key} is missing or empty")
        if meta.get("version") != SCHEMA_VERSION:
            problems.append(f"meta.version {meta.get('version')!r} != the schema this code reads, {SCHEMA_VERSION!r}")
        if expect_version and meta.get("version") != expect_version:
            problems.append(f"meta.version {meta.get('version')!r} != expected {expect_version!r}")
        if meta.get("normalizer_version") != str(NORMALIZER_VERSION):
            problems.append(f"meta.normalizer_version {meta.get('normalizer_version')!r} != the running "
                            f"normalizer {NORMALIZER_VERSION}; rebuild the sidecar")
        if meta.get("keying_version") != str(KEYING_VERSION):
            problems.append(f"meta.keying_version {meta.get('keying_version')!r} != the running expression keying "
                            f"{KEYING_VERSION}; rebuild the sidecar")
        if expect_source_sha256 and (meta.get("source_sha256") or "").lower() != expect_source_sha256.lower():
            problems.append(f"meta.source_sha256 {meta.get('source_sha256')} != expected {expect_source_sha256}")

        counts = {t: con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0] for t in COUNTED_TABLES}
        for t, n in counts.items():
            recorded = meta.get(f"rows_{t}")
            if recorded != str(n):
                problems.append(f"meta.rows_{t} = {recorded!r} but the table holds {n:,} rows")
        if counts["roots"] != expect_roots:
            problems.append(f"roots {counts['roots']:,} != expected {expect_roots:,}")
        if counts["forms"] != expect_forms:
            problems.append(f"forms {counts['forms']:,} != expected {expect_forms:,}")
        if expect_tokens is not None and counts["tokens"] != expect_tokens:
            problems.append(f"tokens {counts['tokens']:,} != expected {expect_tokens:,}")
        for t in ("stems", "forms_norm", "sources", "citations", "tokens"):
            if counts[t] == 0:
                problems.append(f"{t} is empty")

        # Roots and sub-entries: names and reachable targets.
        n = con.execute("SELECT COUNT(*) FROM roots WHERE name_ja IS NULL OR name_ja = ''").fetchone()[0]
        if str(n) != meta.get("roots_unnamed", "0"):
            problems.append(f"{n} roots have no name_ja but meta.roots_unnamed says {meta.get('roots_unnamed')!r}")
        if n > max(5, counts["roots"] // 100):
            problems.append(f"{n} roots have no name_ja (more than 1% of the roots)")
        n = con.execute("SELECT COUNT(*) FROM sub_entries WHERE name_ja IS NULL OR name_ja = ''").fetchone()[0]
        if str(n) != meta.get("sub_entries_unnamed", "0"):
            problems.append(f"{n} sub-entries have no name_ja but meta.sub_entries_unnamed says "
                            f"{meta.get('sub_entries_unnamed')!r}")
        if n > max(5, counts["sub_entries"] // 20):
            problems.append(f"{n} sub-entries have no name_ja (more than 5% of the sub-entries)")
        n = con.execute(
            "SELECT COUNT(*) FROM roots r WHERE r.is_cross_reference = 1 AND r.target_id IS NOT NULL "
            "AND NOT EXISTS (SELECT 1 FROM roots t WHERE t.id = r.target_id)").fetchone()[0]
        if n:
            problems.append(f"{n} cross-reference roots point at a root that is not in the table")
        # A cross-reference with no target is one the site itself left unresolved (14 in release
        # 1.1); the build pins their number, so a NULLed target is a mutation, not a source gap.
        n = con.execute("SELECT COUNT(*) FROM roots WHERE is_cross_reference = 1 AND target_id IS NULL").fetchone()[0]
        xrefs = con.execute("SELECT COUNT(*) FROM roots WHERE is_cross_reference = 1").fetchone()[0]
        if str(xrefs) != meta.get("cross_references", "0"):
            problems.append(f"{xrefs} roots are cross-references but meta.cross_references says "
                            f"{meta.get('cross_references')!r}")
        if str(n) != meta.get("cross_references_unresolved", "0"):
            problems.append(f"{n} cross-reference roots have no target but meta.cross_references_unresolved "
                            f"says {meta.get('cross_references_unresolved')!r}")
        if n > max(2, xrefs // 10):
            problems.append(f"{n} cross-reference roots have no target (more than 10% of {xrefs})")
        n = con.execute("SELECT COUNT(*) FROM roots WHERE is_cross_reference = 0 AND num_forms = 0").fetchone()[0]
        if n:
            problems.append(f"{n} ordinary roots have no form")
        for column, table in (("num_forms", "forms"), ("num_stems", "stems"), ("num_sub_entries", "sub_entries")):
            n = con.execute(
                f"SELECT COUNT(*) FROM roots r WHERE r.{column} != "
                f"(SELECT COUNT(*) FROM {table} t WHERE t.root_id = r.id)").fetchone()[0]
            if n:
                problems.append(f"{n} roots record a {column} that differs from their {table}")

        # Forms: every form renders from its segments.
        n = con.execute("SELECT COUNT(*) FROM forms WHERE segments_json IS NULL OR segments_json = ''").fetchone()[0]
        if n:
            problems.append(f"{n} forms have no segments_json")
        n = con.execute("SELECT COUNT(*) FROM forms WHERE gloss_he IS NULL AND gloss_en IS NULL AND notes IS NULL "
                        "AND classical_note IS NULL AND ja_text IS NULL AND arabic_spelling IS NULL "
                        "AND cross_reference IS NULL").fetchone()[0]
        if n > max(5, counts["forms"] // 100):
            problems.append(f"{n} forms carry no gloss, note, text, spelling or cross-reference at all "
                            f"(more than 1% of the forms)")

        # Citations: kinds and sources.
        n = con.execute("SELECT COUNT(*) FROM citations WHERE kind NOT IN ('reference', 'dictionary')").fetchone()[0]
        if n:
            problems.append(f"{n} citations have an unknown kind")
        n = con.execute("SELECT COUNT(*) FROM citations c WHERE c.source_id IS NOT NULL "
                        "AND NOT EXISTS (SELECT 1 FROM sources s WHERE s.id = c.source_id)").fetchone()[0]
        if str(n) != meta.get("citations_dangling_source", "0"):
            problems.append(f"{n} citations name a source_id that is not in the sources table, but "
                            f"meta.citations_dangling_source says {meta.get('citations_dangling_source')!r}")
        if n > max(5, counts["citations"] // 1000):
            problems.append(f"{n} citations name a source_id that is not in the sources table (more than 0.1%)")

        # Lookup keys: recomputed with the normalizer that is running.
        n = con.execute("SELECT COUNT(*) FROM forms_norm WHERE norm IS NULL OR norm = ''").fetchone()[0]
        if n:
            problems.append(f"{n} forms_norm rows have an empty key")
        n = con.execute(f"SELECT COUNT(*) FROM forms_norm WHERE kind NOT IN "
                        f"({', '.join('?' * len(NORM_KINDS))})", NORM_KINDS).fetchone()[0]
        if n:
            problems.append(f"{n} forms_norm rows have an unknown kind")
        stale = 0
        first_stale = None
        for norm, raw, word_count in con.execute("SELECT norm, raw, word_count FROM forms_norm"):
            expected = normalize_ja(raw)
            if norm != expected or word_count != len(expected.split(" ")):
                stale += 1
                if first_stale is None:
                    first_stale = (raw, norm, expected)
        if stale:
            problems.append(f"{stale} forms_norm keys differ from normalize_ja(raw) under normalizer "
                            f"{NORMALIZER_VERSION} (first: raw {first_stale[0]!r} stored {first_stale[1]!r} "
                            f"recomputed {first_stale[2]!r})")
        n = con.execute("SELECT COUNT(*) FROM roots r WHERE r.is_cross_reference = 0 AND NOT EXISTS "
                        "(SELECT 1 FROM forms_norm n WHERE n.root_id = r.id AND n.kind = 'root')").fetchone()[0]
        if n > max(5, counts["roots"] // 100):
            problems.append(f"{n} ordinary roots have no lookup key of kind 'root' (more than 1% of the roots)")
        idx = {r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type = 'index'")}
        if "idx_forms_norm_norm" not in idx:
            problems.append("index idx_forms_norm_norm is missing")
        if "idx_tokens_key" not in idx:
            problems.append("index idx_tokens_key is missing")

        # The expression search's tokens: every row recomputed from the names and the citations with
        # the keying that is running (a dropped, edited or extra row, or a text edited under its rows).
        stored = con.execute("SELECT scope, scope_id, n, root_id, sub_entry_id, pos, key, peeled FROM tokens "
                             "ORDER BY scope, scope_id, n, pos, rowid").fetchall()
        wanted = expected_tokens(con)
        if sorted(stored) != sorted(wanted):
            have, want = set(stored), set(wanted)
            extra, missing = sorted(have - want), sorted(want - have)
            problems.append(f"tokens differ from the names and citations under keying {KEYING_VERSION}: "
                            f"{len(missing)} missing, {len(extra)} extra, {len(stored) - len(wanted):+d} rows "
                            f"(first missing {missing[0] if missing else None}, first extra "
                            f"{extra[0] if extra else None})")
    finally:
        con.close()
    return problems


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--db", required=True)
    ap.add_argument("--expect-roots", type=int, required=True)
    ap.add_argument("--expect-forms", type=int, required=True)
    ap.add_argument("--expect-source-sha256", help="sha256 of the semantic_blau.json.gz the file was built from")
    ap.add_argument("--expect-version", help="the meta.version the file must carry")
    ap.add_argument("--expect-tokens", type=int, help="the rows the expression search's tokens table must hold")
    args = ap.parse_args(argv)
    problems = verify(args.db, args.expect_roots, args.expect_forms, args.expect_source_sha256, args.expect_version,
                      args.expect_tokens)
    if os.path.isfile(args.db):
        try:
            con = sqlite3.connect(f"file:{args.db}?mode=ro", uri=True)
            try:
                meta = dict(con.execute("SELECT key, value FROM meta").fetchall())
            finally:
                con.close()
            print(f"file     {args.db} ({os.path.getsize(args.db):,} bytes); version {meta.get('version')}; "
                  f"source release {meta.get('source_release')} sha256 {str(meta.get('source_sha256'))[:16]}...; "
                  f"built {meta.get('built_at')}; rows: " +
                  "; ".join(f"{t} {meta.get('rows_' + t)}" for t in COUNTED_TABLES))
        except sqlite3.Error:
            pass
    for p in problems:
        print(f"FAIL: {p}")
    print("RESULT: " + ("FAIL" if problems else "PASS"))
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
