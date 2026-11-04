#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Gate for ``arabic_data/arabic.db`` before it is served: integrity, schema, counts, keys, credit.

    python scripts/verify_arabic_sidecar.py --db arabic_data/arabic.db --expect-entries 47890

FAILS (exit 1) rather than skips on a missing file. Every check is a sentence on its own line, so a
mutated sidecar says what changed: a dropped row fails the count checks, an edited key fails the
recomputation (every root key, headword skeleton, frequency key and closed-class key is recomputed
with the ``shared.arabic_normalize`` that is running, so a sidecar built under other rules is refused
too), an edited ``meta`` count fails the meta-versus-table check, a changed credit line fails the
credit check. Opens the file read-only. Plan: ``docs/plans/ARABIC_DICTIONARY_LAYER_PLAN.md`` §7.
"""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
from pathlib import Path
from typing import Optional

_SCRIPTS_DIR = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.dirname(_SCRIPTS_DIR)
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from scripts.build_arabic_sidecar import (  # noqa: E402  -- the one schema definition
    COUNTED_TABLES,
    DDL,
    FREQ_MIN_STORED,
    MAX_UNAVAILABLE_SHARE,
    PERSEUS_AVAILABILITY,
    PERSEUS_CREDIT,
    SCHEMA_VERSION,
)
from shared.arabic_normalize import NORMALIZER_VERSION, root_key, skeleton, strip_harakat  # noqa: E402

REQUIRED_META = ("version", "normalizer_version", "built_at", "builder_revision", "lane_source",
                 "lane_source_built_at", "lane_manifest_sha256", "lane_pins_sha256", "lane_credit",
                 "lane_availability", "freq_source", "freq_doi", "freq_credit", "freq_texts", "freq_tokens",
                 "freq_manifest_sha256", "freq_min_stored", "closed_class_sha256", "lane_entries_in_source",
                 "lane_entries_without_root", "freq_types", "freq_texts_selected", "freq_texts_unavailable",
                 "freq_metadata_sha256")
#: Floors a real build clears by far (47,890 entries, about 4,700 roots, over 100 million tokens);
#: a truncated source or a half-written count trips them.
MIN_ROOTS = 3000
MIN_FORMS = 15000
MIN_FREQ_TOKENS = 50_000_000
MIN_CLOSED_CLASS = 100
#: The source leaves the root unset on a few entries (35 of 47,890 on 2026-11-01).
MAX_ROOTLESS_SHARE = 0.002


def _schema(con: sqlite3.Connection) -> dict:
    out = {}
    for (name,) in con.execute("SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'"):
        out[name] = [(r[1], (r[2] or "").upper(), r[3], r[5])
                     for r in con.execute("SELECT * FROM pragma_table_info(?)", (name,))]
    out["<indexes>"] = sorted(r[0] for r in con.execute(
        "SELECT name FROM sqlite_master WHERE type = 'index' AND name NOT LIKE 'sqlite_%'"))
    return out


def schema_problems(con: sqlite3.Connection) -> list[str]:
    """Every table, column (name, type, NOT NULL, primary key) and index, compared with the DDL."""
    mem = sqlite3.connect(":memory:")
    try:
        mem.executescript(DDL)
        want = _schema(mem)
    finally:
        mem.close()
    have = _schema(con)
    problems = []
    for table, columns in want.items():
        if table == "<indexes>":
            continue
        if table not in have:
            problems.append(f"table {table} is missing")
        elif have[table] != columns:
            problems.append(f"table {table} does not match the schema: {have[table]} != {columns}")
    problems += [f"table {t} is not part of the schema" for t in have if t != "<indexes>" and t not in want]
    if have["<indexes>"] != want["<indexes>"]:
        problems.append(f"indexes {have['<indexes>']} != expected {want['<indexes>']}")
    return problems


def _int(value: Optional[str]) -> int:
    try:
        return int(value or "")
    except ValueError:
        return -1


def verify(db: str, expect_entries: int, expect_version: Optional[str] = None) -> list[str]:
    """Every problem found, as one sentence each; an empty list is a pass."""
    if not os.path.isfile(db):
        return [f"{db} does not exist"]
    if os.path.getsize(db) == 0:
        return [f"{db} is empty"]
    try:
        con = sqlite3.connect(f"file:{Path(db).resolve().as_posix()}?mode=ro", uri=True)
    except sqlite3.Error as exc:
        return [f"{db} cannot be opened: {exc}"]
    problems: list[str] = []
    try:
        try:
            integrity = con.execute("PRAGMA integrity_check").fetchone()[0]
        except sqlite3.Error as exc:
            return [f"{db} is not a SQLite database: {exc}"]
        if integrity != "ok":
            problems.append(f"integrity_check: {integrity}")
        schema = schema_problems(con)
        if schema:
            return problems + schema + ["schema differs from the builder's DDL; no further checks were run"]
        fk = con.execute("PRAGMA foreign_key_check").fetchall()
        if fk:
            problems.append(f"{len(fk)} foreign-key rows do not resolve (first: {fk[0]})")

        meta = dict(con.execute("SELECT key, value FROM meta").fetchall())
        problems += [f"meta.{k} is missing or empty" for k in REQUIRED_META if not meta.get(k)]
        if meta.get("version") != SCHEMA_VERSION:
            problems.append(f"meta.version = {meta.get('version')!r}, this code reads {SCHEMA_VERSION!r}")
        if expect_version and meta.get("version") != expect_version:
            problems.append(f"meta.version = {meta.get('version')!r}, expected {expect_version!r}")
        if meta.get("normalizer_version") != str(NORMALIZER_VERSION):
            problems.append(f"meta.normalizer_version = {meta.get('normalizer_version')!r}, the running "
                            f"shared/arabic_normalize.py is {NORMALIZER_VERSION}")
        if meta.get("lane_credit") != PERSEUS_CREDIT:
            problems.append("meta.lane_credit is not the Perseus credit line")
        if meta.get("lane_availability") != PERSEUS_AVAILABILITY:
            problems.append("meta.lane_availability is not the Perseus availability statement, intact")

        counts = {t: con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0] for t in COUNTED_TABLES}
        for table, n in counts.items():
            if n == 0:
                problems.append(f"table {table} is empty")
            if meta.get(f"rows_{table}") != str(n):
                problems.append(f"meta.rows_{table} = {meta.get(f'rows_{table}')!r} but the table holds {n:,} rows")
        # The source's count, pinned; the entries it gives no root are left out, counted and capped.
        rootless = _int(meta.get("lane_entries_without_root"))
        if _int(meta.get("lane_entries_in_source")) != expect_entries:
            problems.append(f"meta.lane_entries_in_source {meta.get('lane_entries_in_source')!r} != expected "
                            f"{expect_entries:,}")
        if counts["lane_entries"] + rootless != expect_entries:
            problems.append(f"lane_entries {counts['lane_entries']:,} + {rootless} without a root != expected "
                            f"{expect_entries:,}")
        if not 0 <= rootless <= MAX_ROOTLESS_SHARE * expect_entries:
            problems.append(f"meta.lane_entries_without_root {rootless} is outside 0..{MAX_ROOTLESS_SHARE:.1%}")
        if counts["lane_roots"] < MIN_ROOTS:
            problems.append(f"lane_roots {counts['lane_roots']:,} < {MIN_ROOTS:,}")
        if counts["lane_forms"] < MIN_FORMS:
            problems.append(f"lane_forms {counts['lane_forms']:,} < {MIN_FORMS:,}")
        if counts["closed_class"] < MIN_CLOSED_CLASS:
            problems.append(f"closed_class {counts['closed_class']} < {MIN_CLOSED_CLASS}")
        if _int(meta.get("freq_tokens")) < MIN_FREQ_TOKENS:
            problems.append(f"meta.freq_tokens {meta.get('freq_tokens')!r} < {MIN_FREQ_TOKENS:,}")
        # The counts add up: a stored count edited, or a row dropped, moves the sum off the token
        # count, since every type left out was counted fewer than FREQ_MIN_STORED times.
        if meta.get("freq_min_stored") != str(FREQ_MIN_STORED):
            problems.append(f"meta.freq_min_stored = {meta.get('freq_min_stored')!r}, this code stores "
                            f"{FREQ_MIN_STORED} or more")
        stored = con.execute("SELECT COALESCE(SUM(count), 0) FROM freq").fetchone()[0]
        left_out = _int(meta.get("freq_types")) - counts["freq"]
        tokens = _int(meta.get("freq_tokens"))
        if left_out < 0 or not stored + left_out <= tokens <= stored + left_out * (FREQ_MIN_STORED - 1):
            problems.append(f"freq counts sum to {stored:,} with {left_out:,} types left out, which does not "
                            f"add up to meta.freq_tokens {tokens:,}")
        # Every text D7 selects was counted or is listed as unavailable, and few are unavailable.
        selected, unavailable = _int(meta.get("freq_texts_selected")), _int(meta.get("freq_texts_unavailable"))
        if _int(meta.get("freq_texts")) + unavailable != selected:
            problems.append(f"meta.freq_texts {meta.get('freq_texts')!r} + {unavailable} unavailable != "
                            f"{selected} selected")
        if not 0 <= unavailable <= MAX_UNAVAILABLE_SHARE * selected:
            problems.append(f"meta.freq_texts_unavailable {unavailable} is outside 0..{MAX_UNAVAILABLE_SHARE:.1%} "
                            f"of the selection")

        # Every key recomputed with the running normalizer.
        names = {}
        for key, name, names_json, n_entries in con.execute(
                "SELECT root_key, name, names_json, num_entries FROM lane_roots"):
            names[key] = n_entries
            spellings = json.loads(names_json or "[]")
            if name not in spellings or any(root_key(s) != key for s in spellings):
                problems.append(f"lane_roots {key!r}: a spelling does not key to it")
                break
        per_root = dict(con.execute("SELECT root_key, COUNT(*) FROM lane_entries GROUP BY root_key").fetchall())
        if per_root != names:
            bad = sorted(k for k in set(per_root) | set(names) if per_root.get(k) != names.get(k))
            problems.append(f"{len(bad)} lane_roots whose num_entries differs from their entries (first {bad[0]!r})")
        wrong = con.execute("SELECT id, root_key, root FROM lane_entries").fetchall()
        wrong = [r for r in wrong if root_key(strip_harakat(r[2])) != r[1]]
        if wrong:
            problems.append(f"{len(wrong)} lane_entries whose root_key differs from root_key(root) (first {wrong[0][0]})")
        bad_forms = 0
        for form, rkey, headword, eroot in con.execute(
                "SELECT f.skeleton, f.root_key, e.headword, e.root_key FROM lane_forms f "
                "JOIN lane_entries e ON e.id = f.entry_id"):
            if skeleton(strip_harakat(headword).strip()) != form or rkey != eroot:
                bad_forms += 1
        if bad_forms:
            problems.append(f"{bad_forms} lane_forms keys differ from skeleton(headword) or the entry's root")
        bad_freq = sum(1 for key, n, surface in con.execute("SELECT skeleton, count, surface FROM freq")
                       if skeleton(surface) != key or n < FREQ_MIN_STORED)
        if bad_freq:
            problems.append(f"{bad_freq} freq rows whose key differs from skeleton(surface) or count < "
                            f"{FREQ_MIN_STORED}")
        bad_closed = sum(1 for key, form in con.execute("SELECT skeleton, form FROM closed_class")
                         if skeleton(strip_harakat(form)) != key)
        if bad_closed:
            problems.append(f"{bad_closed} closed_class keys differ from skeleton(form)")
        stray = con.execute("SELECT COUNT(*) FROM closed_class c WHERE c.entry_id IS NOT NULL AND NOT EXISTS "
                            "(SELECT 1 FROM lane_entries e WHERE e.id = c.entry_id AND e.root_key = c.root_key)"
                            ).fetchone()[0]
        if stray:
            problems.append(f"{stray} closed_class rows point at an entry outside their root")
        empty_text = con.execute("SELECT COUNT(*) FROM lane_entries WHERE trim(text) = ''").fetchone()[0]
        if empty_text:
            problems.append(f"{empty_text} lane_entries have no text")
        for index in ("idx_lane_forms_skeleton", "idx_lane_entries_root"):
            plan = " ".join(str(r) for r in con.execute(
                "EXPLAIN QUERY PLAN SELECT * FROM lane_forms WHERE skeleton = 'x'" if "forms" in index else
                "EXPLAIN QUERY PLAN SELECT * FROM lane_entries WHERE root_key = 'x' ORDER BY ordinal"))
            if index not in plan:
                problems.append(f"the lookup does not use {index}")
    finally:
        con.close()
    return problems


def main(argv: Optional[list[str]] = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--db", required=True)
    ap.add_argument("--expect-entries", type=int, required=True, help="Lane entries the source holds (47,890)")
    ap.add_argument("--expect-version", default=None)
    args = ap.parse_args(argv)
    problems = verify(args.db, args.expect_entries, args.expect_version)
    for p in problems:
        print(f"FAIL: {p}")
    print("RESULT: " + ("OK" if not problems else f"FAIL ({len(problems)} problems)"))
    return 0 if not problems else 1


if __name__ == "__main__":
    sys.exit(main())
