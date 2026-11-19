#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Gate ``shelfmark_tree_data/shelfmark_tree.db`` before it is served or shipped.

    python scripts/verify_shelfmark_tree.py --db shelfmark_tree_data/shelfmark_tree.db \
        --expect-leaves <N> [--libraries libraries.csv]

Plan: docs/plans/SHELFMARK_TREE_PICKER_PLAN.md (private repository), §3.1 and §10.3. It FAILS
(exit 1) rather than skips: a missing file, or any check below that does not hold.

1. Schema: the tables and their columns as ``scripts/build_shelfmark_tree.py`` writes them.
2. ``meta``: the schema version this repository's reader serves; ``leaves_total`` equals
   ``--expect-leaves`` and the leaf rows; ``nodes_total`` and ``manuscripts_total`` equal the
   tables; the ``placed_*`` counts add up to the manuscripts.
3. Coverage against ``libraries.csv``: every leaf is a manuscript of it, every manuscript of it
   has at least one leaf.
4. Structure: positions run 0..n-1; every node's parent exists one level up and has an allowed
   kind; a node owns the positions ``lo..hi`` of its leaves and of its children, which follow one
   another in ``sort_key`` order; the stored counts are the counted ones; no node is empty; no
   node holds more than ``RANGE_MAX`` leaves directly, no range more than ``RANGE_SIZE``; a leaf's
   ``library_id`` is its node's library; a FGP leaf carries its inventory row, a matched one none;
   "Other shelfmarks" holds exactly the ``other`` leaves.
5. Canaries (the real data; ``CANARIES``): the CUL collections in FGP's order, the first
   shelfmarks of ``T-S AS 39``, and a record a collector's code must not place (O5).
6. ``PRAGMA quick_check``.
"""
from __future__ import annotations

import argparse
import csv
import sqlite3
import sys
from collections import Counter, defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.build_shelfmark_tree import (  # noqa: E402  -- the one schema definition
    DEFAULT_LIBRARIES,
    HOW,
    RANGE_MAX,
    RANGE_SIZE,
    SCHEMA_VERSION,
    file_sha256,
)
from shared.shelfmark_tree_service import ShelfmarkTreeService  # noqa: E402

NODE_COLUMNS = ("id", "parent_id", "kind", "level", "name_en", "name_he", "short", "private", "sort_key",
                "lo", "hi", "manuscripts", "direct_leaves", "child_nodes")
LEAF_COLUMNS = ("id", "node_id", "library_id", "sys_id", "shelfmark", "shelf_label", "part", "inventory_id",
                "how", "pos")
META_COLUMNS = ("key", "value")
LIBRARY_CODE_COLUMNS = ("code", "node_id")
NODE_ALIAS_COLUMNS = ("node_id", "alias")
CHILD_KINDS = {
    None: {"library"},
    "library": {"collection", "other"},
    "collection": {"volume", "range"},
    "volume": {"range"},
    "other": {"range"},
    "range": set(),
}
#: What the real tree must show (FGP's screen, 2026-11-19; plan §2.1, §10.10).
CUL_COLLECTIONS = ("T-S A - K", "T-S Bound", "T-S Ar.", "T-S Misc.", "T-S 8 - 32", "T-S NS", "T-S AS",
                   "Oriental", "Add.")
CANARIES = {
    "collections": (("CUL", CUL_COLLECTIONS),),
    "first_leaves": (("T-S AS 39", ("T-S AS 39.1", "T-S AS 39.2", "T-S AS 39.3")),),
    "other": ("990001465150205171",),   # coded Harkavy, names the NLI: never placed by the code
}


def _csv_sys_ids(path) -> set[str]:
    out = set()
    with open(path, encoding="utf-8", newline="") as f:
        for row in csv.reader(f):
            if row and row[0].strip().isdigit():
                out.add(row[0].strip())
    return out


def _columns(con, table) -> tuple:
    return tuple(r[1] for r in con.execute(f"PRAGMA table_info({table})"))


def schema_problems(con) -> list[str]:
    tables = {r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}
    problems = [f"missing table {t}" for t in ("meta", "node", "leaf", "library_code", "node_alias")
                if t not in tables]
    if problems:
        return problems
    for table, want in (("meta", META_COLUMNS), ("node", NODE_COLUMNS), ("leaf", LEAF_COLUMNS),
                        ("library_code", LIBRARY_CODE_COLUMNS), ("node_alias", NODE_ALIAS_COLUMNS)):
        if _columns(con, table) != want:
            problems.append(f"{table} columns {_columns(con, table)} != {want}")
    indexes = {r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type = 'index'")}
    for index in ("idx_node_parent", "idx_leaf_node", "idx_leaf_sys"):
        if index not in indexes:
            problems.append(f"missing index {index}")
    return problems


def structure_problems(con) -> list[str]:
    problems: list[str] = []
    nodes = {r[0]: dict(zip(NODE_COLUMNS, r)) for r in con.execute(f"SELECT {', '.join(NODE_COLUMNS)} FROM node")}
    leaves = [dict(zip(LEAF_COLUMNS, r)) for r in
              con.execute(f"SELECT {', '.join(LEAF_COLUMNS)} FROM leaf ORDER BY pos")]
    if [lf["pos"] for lf in leaves] != list(range(len(leaves))):
        problems.append("leaf positions do not run 0..n-1")
        return problems

    children = defaultdict(list)
    for n in nodes.values():
        parent = nodes.get(n["parent_id"]) if n["parent_id"] is not None else None
        if n["parent_id"] is not None and parent is None:
            problems.append(f"node {n['id']}: parent {n['parent_id']} missing")
            continue
        if n["kind"] not in CHILD_KINDS[parent["kind"] if parent else None]:
            problems.append(f"node {n['id']}: a {n['kind']} under {parent['kind'] if parent else 'the root'}")
        if n["level"] != (parent["level"] + 1 if parent else 0):
            problems.append(f"node {n['id']}: level {n['level']}")
        children[n["parent_id"]].append(n)

    # Ancestry must end at a library: a missing parent or a cycle stops here, before any walk up.
    for nid in nodes:
        seen = set()
        current = nid
        while current is not None and current in nodes and current not in seen:
            seen.add(current)
            current = nodes[current]["parent_id"]
        if current is not None:
            problems.append(f"node {nid}: its ancestry {'loops' if current in seen else 'is broken'} "
                            f"at {current}")
            return problems
    if problems:
        return problems

    # The libraries themselves: sort_key 0..k-1 and their positions one after another from 0.
    roots = sorted(children.get(None, []), key=lambda k: k["sort_key"])
    if [k["sort_key"] for k in roots] != list(range(len(roots))):
        problems.append("the libraries' sort_key is not 0..k-1")
    expect = 0
    for k in roots:
        if k["lo"] != expect:
            problems.append(f"library {k['id']} does not start where the previous library ended")
            break
        expect = k["hi"] + 1
    if roots and expect != len(leaves):
        problems.append("the libraries do not end at the last position")

    direct = defaultdict(list)
    for lf in leaves:
        if lf["node_id"] not in nodes:
            problems.append(f"leaf {lf['id']}: node {lf['node_id']} missing")
            continue
        direct[lf["node_id"]].append(lf)

    def library_of(nid):
        while nodes[nid]["parent_id"] is not None:
            nid = nodes[nid]["parent_id"]
        return nid

    bad = Counter()
    for nid, n in nodes.items():
        kids = sorted(children.get(nid, []), key=lambda k: k["sort_key"])
        own = direct.get(nid, [])
        if [k["sort_key"] for k in kids] != list(range(len(kids))):
            bad["children's sort_key is not 0..k-1"] += 1
        # children first, in sort_key order, then the node's own leaves: contiguous lo..hi
        expect = n["lo"]
        for k in kids:
            if k["lo"] != expect:
                bad["a child does not start where its sibling ended"] += 1
            expect = k["hi"] + 1
        if own and (own[0]["pos"] != expect or own[-1]["pos"] != n["hi"] or len(own) != n["hi"] - expect + 1):
            bad["direct leaves are not the node's last positions"] += 1
        elif not own and expect != n["hi"] + 1:
            bad["children do not end where the node ends"] += 1
        under = {leaves[p]["sys_id"] for p in range(n["lo"], n["hi"] + 1)} if n["hi"] >= n["lo"] else set()
        if not under:
            bad["empty node"] += 1
        if n["manuscripts"] != len(under):
            bad["manuscripts is not the count of distinct manuscripts under it"] += 1
        if n["direct_leaves"] != len(own) or n["child_nodes"] != len(kids):
            bad["direct_leaves or child_nodes is wrong"] += 1
        if len(own) > RANGE_MAX:
            bad[f"more than {RANGE_MAX} leaves directly in a node"] += 1
        if n["kind"] == "range" and len(own) > RANGE_SIZE:
            bad[f"a range of more than {RANGE_SIZE}"] += 1
    for reason, count in bad.items():
        problems.append(f"{count:,} node(s): {reason}")

    lbad = Counter()
    for lf in leaves:
        if lf["node_id"] not in nodes:
            continue
        if lf["library_id"] != library_of(lf["node_id"]):
            lbad["library_id is not its node's library"] += 1
        if lf["how"] not in HOW:
            lbad["unknown how"] += 1
        elif lf["how"] == "fist" and (lf["inventory_id"] is None or not lf["shelf_label"]):
            lbad["a FGP leaf without its inventory row"] += 1
        elif lf["how"] != "fist" and lf["inventory_id"] is not None:
            lbad["a matched leaf with an inventory row"] += 1
        holder = nodes[lf["node_id"]]
        while holder["kind"] == "range":          # a range belongs to the node above it
            holder = nodes[holder["parent_id"]]
        if (lf["how"] == "other") != (holder["kind"] == "other"):
            lbad["Other shelfmarks does not hold exactly the other leaves"] += 1
    for reason, count in lbad.items():
        problems.append(f"{count:,} leaf(s): {reason}")

    stray = [nid for nid, _a in con.execute("SELECT node_id, alias FROM node_alias")
             if nid not in nodes or nodes[nid]["kind"] != "collection"]
    if stray:
        problems.append(f"{len(stray):,} alias(es) not on a collection (first: {stray[:3]})")
    loose = [nid for _code, nid in con.execute("SELECT code, node_id FROM library_code")
             if nid not in nodes or nodes[nid]["kind"] != "library"]
    if loose:
        problems.append(f"{len(loose):,} library_code row(s) not on a library (first: {loose[:3]})")
    return problems


def canary_problems(db: str, canaries) -> list[str]:
    service = ShelfmarkTreeService(db_path=db)
    if not service.is_ready():
        return [f"the reader refuses the file: {service.unavailable_reason}"]
    problems = []
    try:
        libraries = service.children()
        for short, names in canaries.get("collections", ()):
            lib = next((n for n in libraries if n.short == short), None)
            got = tuple(c.name_en for c in service.children(lib.id) if c.kind == "collection") if lib else ()
            if got != tuple(names):
                problems.append(f"canary: {short} collections {got} != {tuple(names)}")
        for prefix, first in canaries.get("first_leaves", ()):
            found = service.locate(prefix)
            if len(found.nodes) != 1 or found.nodes[0].name_en != prefix:
                problems.append(f"canary: {prefix!r} does not locate one node ({[n.id for n in found.nodes]})")
                continue
            got = tuple(lf.shelf_label for lf in service.leaves(found.nodes[0].id, 0, len(first))[0])
            if got != tuple(first):
                problems.append(f"canary: {prefix} starts {got}, not {tuple(first)}")
        for sys_id in canaries.get("other", ()):
            hows = [p.leaf.how for p in service.path_of(sys_id)]
            if hows != ["other"]:
                problems.append(f"canary: {sys_id} is placed {hows}, not only in Other shelfmarks")
    finally:
        service.close()
    return problems


def verify(db: str, expect_leaves: int, libraries=DEFAULT_LIBRARIES, canaries=CANARIES,
           quick_check: bool = True, fist=None) -> list[str]:
    """``fist``: the FGP dump the tree must have been built from. The dump is never on the server
    or in the installer, so the gate checks it only where it is given (locally, before a deploy)."""
    path = Path(db)
    if not path.is_file() or path.stat().st_size == 0:
        return [f"no sidecar (or an empty file) at {db}"]
    con = sqlite3.connect(f"file:{path.resolve().as_posix()}?mode=ro", uri=True)
    try:
        problems = schema_problems(con)
        if problems:
            return problems
        meta = dict(con.execute("SELECT key, value FROM meta").fetchall())
        if fist is not None:
            if not Path(fist).is_file():
                problems.append(f"no FGP dump at {fist}")
            else:
                digest = file_sha256(fist)
                if meta.get("input_fist_sha256") != digest:
                    problems.append(f"built from another FGP dump (meta.input_fist_sha256 "
                                    f"{meta.get('input_fist_sha256')!r}, {fist} is {digest}): rebuild the tree")
        if meta.get("version") != SCHEMA_VERSION:
            problems.append(f"meta.version {meta.get('version')!r} != {SCHEMA_VERSION!r}")
        leaves = con.execute("SELECT COUNT(*) FROM leaf").fetchone()[0]
        nodes = con.execute("SELECT COUNT(*) FROM node").fetchone()[0]
        manuscripts = con.execute("SELECT COUNT(DISTINCT sys_id) FROM leaf").fetchone()[0]
        if leaves != expect_leaves:
            problems.append(f"leaf holds {leaves:,} rows, expected {expect_leaves:,}")
        for key_, have in (("leaves_total", leaves), ("nodes_total", nodes), ("manuscripts_total", manuscripts)):
            if meta.get(key_) != str(have):
                problems.append(f"meta.{key_} {meta.get(key_)!r} != {have:,}")
        placed = sum(int(meta.get(f"placed_{h}", "0") or 0) for h in HOW)
        if placed != manuscripts:
            problems.append(f"meta.placed_* add up to {placed:,}, the file holds {manuscripts:,} manuscripts")

        digest = file_sha256(libraries)
        if meta.get("input_libraries_sha256") != digest:
            problems.append(f"built from another libraries.csv (meta.input_libraries_sha256 "
                            f"{meta.get('input_libraries_sha256')!r}, {libraries} is {digest}): rebuild the tree")
        wanted = _csv_sys_ids(libraries)
        have = {r[0] for r in con.execute("SELECT DISTINCT sys_id FROM leaf")}
        if have - wanted:
            problems.append(f"{len(have - wanted):,} leaf manuscript(s) not in {libraries} "
                            f"(first: {sorted(have - wanted)[:3]})")
        if wanted - have:
            problems.append(f"{len(wanted - have):,} manuscript(s) of {libraries} have no place "
                            f"(first: {sorted(wanted - have)[:3]})")

        problems += structure_problems(con)
        if quick_check:
            check = con.execute("PRAGMA quick_check").fetchone()
            if check is None or check[0] != "ok":
                problems.append(f"quick_check: {check[0] if check else 'no answer'}")
    finally:
        con.close()
    if not problems and canaries:
        problems += canary_problems(str(path), canaries)
    return problems


def _meta_leaves(db: str):
    try:
        con = sqlite3.connect(f"file:{Path(db).resolve().as_posix()}?mode=ro", uri=True)
        try:
            row = con.execute("SELECT value FROM meta WHERE key = 'leaves_total'").fetchone()
        finally:
            con.close()
        return int(row[0]) if row and str(row[0]).isdigit() else None
    except (sqlite3.Error, OSError, ValueError):
        return None


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--db", required=True)
    ap.add_argument("--libraries", default=str(DEFAULT_LIBRARIES))
    ap.add_argument("--fist", default=None,
                    help="the FGP dump; when given, the tree must have been built from this very file "
                         "(run it locally before a deploy: the server has no dump)")
    count = ap.add_mutually_exclusive_group(required=True)
    count.add_argument("--expect-leaves", type=int)
    # The desktop build: the count the file's own meta records stands in for one given by hand;
    # every other check still runs. A deploy names the number it expects.
    count.add_argument("--leaves-from-meta", action="store_true")
    args = ap.parse_args(argv)
    if args.leaves_from_meta:
        args.expect_leaves = _meta_leaves(args.db)
        if args.expect_leaves is None:
            print(f"FAIL: {args.db} has no readable meta.leaves_total")
            print("RESULT: FAIL (1 problem(s))")
            return 1
    problems = verify(args.db, args.expect_leaves, libraries=args.libraries, fist=args.fist)
    if problems:
        for problem in problems:
            print(f"FAIL: {problem}")
        print(f"RESULT: FAIL ({len(problems)} problem(s))")
        return 1
    print(f"RESULT: OK ({args.db}, {args.expect_leaves:,} leaves)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
