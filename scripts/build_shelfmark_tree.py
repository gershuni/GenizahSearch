#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Build ``shelfmark_tree_data/shelfmark_tree.db``: every manuscript on its library's shelf.

    python scripts/build_shelfmark_tree.py [--fist fist_data/FIST.db] [--libraries libraries.csv]
                                           [--out shelfmark_tree_data/shelfmark_tree.db] [--force]

Plan: docs/plans/SHELFMARK_TREE_PICKER_PLAN.md (private repository), §3.1 as amended by §10.
``scripts/verify_shelfmark_tree.py`` gates the file; ``shared/shelfmark_tree_service.py`` reads it.

The tree is library > collection > volume > shelfmark, as the FGP "find a fragment" screen has it,
taken from the FGP dump (``FIST.db``, read-only). Only the manuscripts of ``libraries.csv`` are
leaves; a node with none is left out.

Placement (one leaf per placement; a manuscript may sit at two places):

* **FIST.** Every active inventory row linked to the manuscript through ``dbo_InventoryAlma``
  (``dbo_inventoryVsAlma`` is not used, §10.2) in an active collection of an active library; in its
  volume when the volume is active, else directly in the collection.
* **The rest** (no active link), by the owner's rules O1-O5 (§10.11), each inside the FIST
  libraries the record's ``library_code`` is tied to:

  0. a record whose call numbers name another institution in their long form (``<institution>,
     Ms. ...``; ``OWN_HEADS`` lists what counts as the code's own) goes to "Other shelfmarks";
  1. **exact**: a call number equal, token by token (numbers kept apart, so ``T-S AS 84.1`` is not
     ``T-S AS 8.41``), to an inventory shelfmark of those libraries. Matches in two collections are
     a collision ("Other"). A match is used only where, on the placed manuscripts, the exact step
     finds the manuscript's own collection at least ``PURITY`` of the time over ``MIN_CASES`` (the
     trust is per code and collection); then the leaf sits beside that row, or, when the matches
     are two rows of one collection, in the collection outside any volume;
  2. **collection head**: the call number up to its first digit names one collection: at least
     ``MIN_CASES`` placed manuscripts of the code carry that head and ``PURITY`` of their
     placements are in one collection of the code's libraries. Outside any volume (O4). Two
     collections: "Other";
  3. **single collection**: a code that names a holding institution (not ``COLLECTOR_CODES``, O5),
     tied to one library that has one active collection, with at least ``MIN_CASES`` placed
     manuscripts, takes its leftovers;
  4. otherwise "Other shelfmarks" under the code's library (its own fallback node when FIST has
     none).

Order: libraries by size (``TOP_LIBRARY_MIN`` manuscripts or more, largest first), then the rest
alphabetically (D9). An Oxford leaf carries its Neubauer Part (``libraries.csv`` column 1) as
information, not as a level (owner, 2026-11-19). Collections by ``DisplayOrder``; volumes by ``OrderBy1Num``, ``OrderBy2Num``,
``OrderBy3Text``; shelfmarks by the ``OB*`` columns, then the shelfmark's natural order. A node
holding more than ``RANGE_MAX`` shelfmarks directly gets range nodes of ``RANGE_SIZE`` (D5). Every
leaf has a position in one shelf order across the whole tree, so previous / next cross ranges,
volumes and collections (§10.3), and every node owns the contiguous positions ``lo..hi``.

Read-only on its inputs; writes a temporary file and swaps it in. Refuses to write a file with
fewer manuscripts than the one it replaces unless ``--force``.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import os
import re
import sqlite3
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.browse_map_utils import LIBRARY_CODES, natural_sort_key  # noqa: E402
from shared.catalog_shelfmark import head_codes  # noqa: E402

#: The sidecar schema; shared/shelfmark_tree_service.py serves only this version.
SCHEMA_VERSION = "1.0.0"
DEFAULT_OUT = ROOT / "shelfmark_tree_data" / "shelfmark_tree.db"
DEFAULT_FIST = ROOT / "fist_data" / "FIST.db"
DEFAULT_LIBRARIES = ROOT / "libraries.csv"

MIN_CASES = 5
PURITY = 0.98
#: A code is also tied to a second FIST library holding this share (and MIN_CASES) of its placed
#: manuscripts.
INSTITUTION_SHARE = 0.02
#: Libraries with at least this many manuscripts come first, largest first (D9).
TOP_LIBRARY_MIN = 1000
RANGE_MAX = 1000
RANGE_SIZE = 500
#: FIST's "Undefined Shelfmarks" collection is not a place.
UNDEFINED_COLLECTION = 99

#: Codes that name a collector, a former owner or a catalogue, not the institution that holds the
#: manuscripts: step 3 never places by them (O5; owner, 2026-11-19).
COLLECTOR_CODES = frozenset({
    "Harkavy", "Mosseri", "Lutzki", "Sofer", "Sassoon", "Schoeyen", "Lehmann", "Bisno", "Solomon",
    "Combs", "Lehnardt", "Reinach", "Wallach", "Boesky", "Benayahu", "Salmon", "Adler", "Shapira",
    "Karp", "Weiss", "Nahum", "Mehlman", "Corwin", "Allony", "AllonyLoew", "AllonyKupf",
})

#: Long-form heads (``<head>, Ms. ...``) that count as the code's own institution besides its name
#: in ``LIBRARY_CODES`` and what ``shared.catalog_shelfmark.head_codes`` already gives it (owner,
#: 2026-11-19). Any other head, a former owner's name included, is another institution (O2).
OWN_HEADS = {
    "BL": ("lon bl", "gaster, moses collection"),
    "Manchester": ("un of manchester library", "gaster, moses collection", "gaster heb."),
    "JTS": ("ny jts", "adler, elkan nathan"),
    "RNL": ("nl of russia", "constantin von tischendorf collection"),
    "Oxford": ("the bodleian lib un of ox",),
    "AIU": ("library of the alliance israélite universelle", "library of the alliance israélite un"),
    "Katz": ("library at the herbert d. katz center for advanced judaic studies, university of pennsylvania",
             "library at the herbert d. katz center for advanced judaic studies, university of pennsylvania cajs rar",
             "rar", "phil up"),
    "Leeds": ("roth, cecil",),
    "Geneva": ("library of geneva.",),
    "Sofer": ("sofer david london",),
    "Allony": ("the national library of israel",),
    "AllonyLoew": ("state library of berlin",),
    "AllonyKupf": ("the royal danish library",),
    "Senckenberg": ("university library johann christian senckenberg",),
    "Heidelberg": ("university library heidelberg",),
    "IOM": ("institute of oriental manuscripts, the russian academy of sciences",),
    "TCD": ("the library of trinity college dublin",),
    "Turin": ("oriental studies library of the department of humanities of the university of turin",),
    "UPenn": ("museum of archaeology and anthropology, university of pennsylvania",),
    "Chetham": ("chetham's library",),
}

OB_COLUMNS = ("OBCollectionCodeOrder", "OBBoxNumber", "OBBox", "OBVolume", "OBVolumeText", "OBFolio",
              "OBFolioText", "OBFolioMtf1", "OBFolioMtf1Text", "OBFolioMtf2", "OBFolioMtf2Text")
OTHER_NAME_EN = "Other shelfmarks"
OTHER_NAME_HE = "מספרי מדף אחרים"
NO_CODE_NAME = "No library code"
HOW = ("fist", "exact", "collection", "single", "other")

_TOKEN = re.compile(r"[^\W\d_]+|\d+")
_HEAD_RE = re.compile(r"^(?P<head>.+?)[,\s]+(?:Ms\.?|MS\.?)\s+\S")


def key(text: str) -> tuple:
    """Letters and numbers as separate tokens, numbers as ints: punctuation and case drop out,
    number boundaries stay."""
    return tuple(int(t) if t.isdigit() else t for t in _TOKEN.findall((text or "").lower()))


def head(text: str) -> tuple:
    """The tokens of a call number before its first digit."""
    m = re.match(r"\D*", text or "")
    return tuple(_TOKEN.findall(m.group(0).lower()))


def institution_head(variant: str) -> Optional[str]:
    """The institution head of a long form ``<head>, Ms. <rest>``, lowercased, or None."""
    m = _HEAD_RE.match(variant or "")
    return " ".join(m.group("head").lower().split()) if m else None


def display_shelfmark(variants: list[str]) -> str:
    """The shelfmark the apps show: the shortest call number, the first on a tie
    (``MetadataManager``'s CSV loader)."""
    shelf = variants[0] if variants else ""
    for v in variants:
        if v and len(v) < len(shelf):
            shelf = v
    return shelf


def own_heads(code: str) -> frozenset:
    names = {" ".join(str(LIBRARY_CODES.get(code, "")).lower().split())} - {""}
    return frozenset(names | set(OWN_HEADS.get(code, ())))


def foreign_heads(code: str, variants: list[str]) -> list[str]:
    """Long-form heads of a record that are not its code's own institution (O2, O5)."""
    own = own_heads(code)
    out = []
    for v in variants:
        h = institution_head(v)
        if h is None or h in own or (code and code in head_codes(h)):
            continue
        out.append(h)
    return sorted(set(out))


def _ob_part(value):
    if value is None or value == "":
        return (1,)
    if isinstance(value, str):
        return (0, value.lower())
    return (0, float(value))


def ob_key(row: tuple, shelfmark: str) -> tuple:
    """Shelf order inside a node: the ``OB*`` columns (empty last), then the natural order."""
    return tuple(_ob_part(v) for v in row) + (tuple(natural_sort_key(shelfmark)),)


def _nat(text) -> tuple:
    return tuple((0, p) if isinstance(p, int) else (1, p) for p in natural_sort_key(text or ""))


@dataclass
class Node:
    id: str
    kind: str                 # library | collection | volume | range | other
    parent: Optional[str]
    name_en: str
    name_he: str = ""
    short: str = ""
    private: int = 0
    order: tuple = ()
    children: list = field(default_factory=list)
    leaves: list = field(default_factory=list)   # [(sort key, Leaf)]
    aliases: list = field(default_factory=list)  # shelfmark heads a collection is also found by


@dataclass
class Leaf:
    id: str
    sys_id: str
    shelfmark: str
    shelf_label: str          # FIST's shelfmark for a FIST leaf, else ''
    inventory_id: Optional[int]
    how: str
    part: str = ""            # the Oxford (Neubauer) Part, ``libraries.csv`` column 1: information only


class BuildError(RuntimeError):
    pass


def read_libraries(path) -> list[tuple[str, str, list[str]]]:
    rows = []
    with open(path, encoding="utf-8", newline="") as f:
        for row in csv.reader(f):
            if not row or not row[0].strip().isdigit():
                continue
            variants = [v.strip() for v in (row[2] if len(row) > 2 else "").split("|") if v.strip()]
            rows.append((row[0].strip(), (row[3].strip() if len(row) > 3 else ""), variants,
                         (row[1].strip() if len(row) > 1 else "")))
    return rows


def build_tree(fist_path, libraries_path, log=print):
    """Read the inputs and return (nodes by id, library ids in order, stats, the FIST libraries
    each code is tied to). Raises BuildError."""
    con = sqlite3.connect(f"file:{Path(fist_path).resolve().as_posix()}?mode=ro", uri=True)
    q = lambda sql, *a: con.execute(sql, a).fetchall()  # noqa: E731
    try:
        moved = q("SELECT COUNT(*) FROM dbo_Inventory WHERE MoveToInventoryId IS NOT NULL "
                  "AND MoveToInventoryId <> 0 AND MoveToInventoryId <> ''")[0][0]
        if moved:
            raise BuildError(f"{moved} inventory rows have MoveToInventoryId; no rule is written for "
                             "moved rows yet (plan §10.2)")
        libraries = {lid: (ac or eng or str(lid), eng or ac or str(lid), heb or "", int(priv or 0))
                     for lid, ac, eng, heb, priv in
                     q("SELECT LibraryId, LibraryNameAc, LibraryNameEng, LibraryNameHeb, IsPrivate "
                       "FROM CODE_Library WHERE COALESCE(IsCanceledCode, 0) = 0")}
        collections = {cid: (lid, name or str(cid), order) for cid, lid, name, order in
                       q("SELECT CollectionId, LibraryId, CollectionName, DisplayOrder FROM CODE_Collection "
                         "WHERE COALESCE(IsCanceledCode, 0) = 0 AND CollectionId <> ?", UNDEFINED_COLLECTION)
                       if lid in libraries}
        volumes = {gid: (cid, desc or str(gid), (o1, o2, o3)) for gid, cid, desc, o1, o2, o3 in
                   q("SELECT InventoryGroupId, CollectionId, InventoryGroupDesc, OrderBy1Num, OrderBy2Num, "
                     "OrderBy3Text FROM dbo_InventoryGroup WHERE RecordStatus = 0")}
        inventory = {}
        for row in q(f"SELECT InventoryId, CollectionId, InventoryGroupId, Shelfmark, {', '.join(OB_COLUMNS)} "
                     "FROM dbo_Inventory WHERE RecordStatus = 0"):
            iid, cid, gid, shelf = row[:4]
            if cid not in collections:
                continue
            vol = gid if gid in volumes and volumes[gid][0] == cid else None
            inventory[iid] = (cid, vol, shelf or "", ob_key(row[4:], shelf or ""))
        links = defaultdict(list)
        for alma, iid in q("SELECT AlmaId, InventoryId FROM dbo_InventoryAlma"):
            if iid in inventory:
                links[str(alma)].append(iid)
    finally:
        con.close()

    records = read_libraries(libraries_path)
    by_sys = {}
    parts = {}
    for sid, code, variants, part in records:
        by_sys.setdefault(sid, (code, variants))
        parts.setdefault(sid, part)
    lib_of_collection = {cid: v[0] for cid, v in collections.items()}
    collections_of_library = Counter(v[0] for v in collections.values())

    # -- where each code's placed manuscripts sit -----------------------------------------------
    placed = {sid: sorted(set(links[sid])) for sid in by_sys if links.get(sid)}
    code_libs: dict[str, Counter] = defaultdict(Counter)
    for sid, iids in placed.items():
        for lid in {lib_of_collection[inventory[i][0]] for i in iids}:
            code_libs[by_sys[sid][0]][lid] += 1
    tied: dict[str, list[int]] = {}
    for code, counter in code_libs.items():
        total = sum(counter.values())
        top = counter.most_common(1)[0][0]
        tied[code] = [lid for lid, n in counter.most_common()
                      if lid == top or (n / total >= INSTITUTION_SHARE and n >= MIN_CASES)]

    fist_key = defaultdict(set)   # (library, key) -> {inventory id}
    for iid, (cid, _vol, shelf, _ob) in inventory.items():
        fist_key[(lib_of_collection[cid], key(shelf))].add(iid)

    def exact(code, variants):
        found = set()
        for v in variants:
            for lid in tied.get(code, ()):
                found |= fist_key.get((lid, key(v)), set())
        return found

    heads = defaultdict(Counter)   # (code, head) -> collections of every placement
    for sid, iids in placed.items():
        code, variants = by_sys[sid]
        cs = {inventory[i][0] for i in iids}
        for h in {head(v) for v in variants}:
            if h:
                for c in cs:
                    heads[(code, h)][c] += 1
    trust = defaultdict(Counter)   # (code, collection) -> right / wrong of the exact step
    for sid, iids in placed.items():
        code, variants = by_sys[sid]
        own = {inventory[i][0] for i in iids}
        for c in {inventory[i][0] for i in exact(code, variants)}:
            trust[(code, c)]["right" if c in own else "wrong"] += 1

    def trusted(code, cid):
        r = trust.get((code, cid))
        n = (r["right"] + r["wrong"]) if r else 0
        return n >= MIN_CASES and r["right"] / n >= PURITY

    def by_head(code, variants):
        allowed = set(tied.get(code, ()))
        out = set()
        for h in {head(v) for v in variants}:
            counter = heads.get((code, h))
            if not counter:
                continue
            n = sum(counter.values())
            top, k = counter.most_common(1)[0]
            if n >= MIN_CASES and k / n >= PURITY and lib_of_collection[top] in allowed:
                out.add(top)
        return out

    def single_collection(code):
        libs = tied.get(code, [])
        if code in COLLECTOR_CODES or len(libs) != 1 or sum(code_libs[code].values()) < MIN_CASES:
            return None
        lid = libs[0]
        if collections_of_library[lid] != 1:
            return None
        return next(c for c, v in collections.items() if v[0] == lid)

    # -- nodes -------------------------------------------------------------------------------------
    nodes: dict[str, Node] = {}

    def library_node(lid):
        nid = f"lib:{lid}"
        if nid not in nodes:
            short, eng, heb, priv = libraries[lid]
            nodes[nid] = Node(nid, "library", None, eng, heb, short, priv)
        return nodes[nid]

    def code_node(code):
        nid = f"code:{code or '-'}"
        if nid not in nodes:
            name = str(LIBRARY_CODES.get(code, code)) if code else NO_CODE_NAME
            nodes[nid] = Node(nid, "library", None, name, "", code or "-")
        return nodes[nid]

    def collection_node(cid):
        nid = f"col:{cid}"
        if nid not in nodes:
            lid, name, order = collections[cid]
            parent = library_node(lid)
            nodes[nid] = Node(nid, "collection", parent.id, name,
                              order=((0, order) if order is not None else (1,), _nat(name)))
            parent.children.append(nid)
        return nodes[nid]

    def volume_node(gid):
        nid = f"vol:{gid}"
        if nid not in nodes:
            cid, desc, (o1, o2, o3) = volumes[gid]
            parent = collection_node(cid)
            nodes[nid] = Node(nid, "volume", parent.id, desc,
                              order=(_ob_part(o1), _ob_part(o2), _ob_part(o3), _nat(desc)))
            parent.children.append(nid)
        return nodes[nid]

    def other_node(code):
        libs = tied.get(code)
        parent = library_node(libs[0]) if libs else code_node(code)
        nid = f"other:{parent.id}"
        if nid not in nodes:
            nodes[nid] = Node(nid, "other", parent.id, OTHER_NAME_EN, OTHER_NAME_HE, order=((9,),))
            parent.children.append(nid)
        return nodes[nid]

    stats = Counter()
    foreign_seen: Counter = Counter()
    for sid, (code, variants) in by_sys.items():
        shown = display_shelfmark(variants)
        nat = tuple(natural_sort_key(shown))
        if sid in placed:
            for iid in placed[sid]:
                cid, vol, shelf, ob = inventory[iid]
                target = volume_node(vol) if vol is not None else collection_node(cid)
                target.leaves.append((ob + ((0,), nat), Leaf(f"inv:{iid}:{sid}", sid, shown, shelf, iid, "fist",
                                                                    parts[sid])))
            stats["fist"] += 1
            stats["fist_placements"] += len(placed[sid])
            continue
        leaf = Leaf(f"sys:{sid}", sid, shown, "", None, "other", parts[sid])
        foreign = foreign_heads(code, variants)
        target, sort = None, ((9,), nat)
        if foreign:
            stats["other_foreign"] += 1
            foreign_seen.update((code, h) for h in foreign)
        else:
            found = exact(code, variants)
            cs = {inventory[i][0] for i in found}
            if len(cs) > 1:
                stats["other_collision"] += 1
            else:
                if cs and trusted(code, next(iter(cs))):
                    if len(found) == 1:
                        cid, vol, _shelf, ob = inventory[next(iter(found))]
                        target = volume_node(vol) if vol is not None else collection_node(cid)
                        sort = ob + ((1,), nat)
                    else:
                        target = collection_node(next(iter(cs)))
                    leaf.how = "exact"
                elif cs:
                    stats["exact_not_trusted"] += 1
                if target is None:
                    hits = by_head(code, variants)
                    if len(hits) == 1:
                        target, leaf.how = collection_node(next(iter(hits))), "collection"
                    elif len(hits) > 1:
                        stats["other_two_collections"] += 1
                    else:
                        single = single_collection(code)
                        if single is not None:
                            target, leaf.how = collection_node(single), "single"
        if target is None:
            target = other_node(code)
        stats[leaf.how] += 1
        target.leaves.append((sort, leaf))

    # -- ranges, order, positions ------------------------------------------------------------------
    for node in list(nodes.values()):
        node.leaves.sort(key=lambda kl: (kl[0], kl[1].id))
        if len(node.leaves) > RANGE_MAX:
            chunks = [node.leaves[i:i + RANGE_SIZE] for i in range(0, len(node.leaves), RANGE_SIZE)]
            node.leaves = []
            for k, chunk in enumerate(chunks, 1):
                first, last = chunk[0][1], chunk[-1][1]
                name = f"{first.shelf_label or first.shelfmark} – {last.shelf_label or last.shelfmark}"
                rid = f"rng:{node.id}:{k}"
                nodes[rid] = Node(rid, "range", node.id, name, order=((8,), k), leaves=chunk)
                node.children.append(rid)

    def manuscripts_of(nid):
        out = {leaf.sys_id for _k, leaf in nodes[nid].leaves}
        for child in nodes[nid].children:
            out |= manuscripts_of(child)
        return out

    sizes = {nid: len(manuscripts_of(nid)) for nid, n in nodes.items() if n.kind == "library"}
    top = sorted((n for n in nodes.values() if n.kind == "library" and sizes[n.id] >= TOP_LIBRARY_MIN),
                 key=lambda n: (-sizes[n.id], n.short.casefold()))
    rest = sorted((n for n in nodes.values() if n.kind == "library" and sizes[n.id] < TOP_LIBRARY_MIN),
                  key=lambda n: (n.short.casefold(), n.id))
    order = [n.id for n in top + rest]
    for node in nodes.values():
        node.children.sort(key=lambda c: (nodes[c].order, nodes[c].id))
    collection_aliases(nodes)
    stats["top_libraries"] = len(top)
    stats["manuscripts"] = len(by_sys)
    log(f"libraries first by size (>= {TOP_LIBRARY_MIN:,}): " +
        ", ".join(f"{n.short} {sizes[n.id]:,}" for n in top))
    if foreign_seen:
        log("records held back because a long form names another institution (code, head, records):")
        for (code, h), n in foreign_seen.most_common():
            log(f"  {code or '(none)'}\t{h}\t{n}")
    return nodes, order, stats, tied


def collection_aliases(nodes: dict) -> None:
    """Give each collection the shelfmark heads its own leaves are written with, so a typed
    shelfmark finds a collection whose display name differs (CUL's "Oriental" holds ``Or. ...``).
    A head counts when at least ``MIN_CASES`` of a collection's leaves carry it (FGP's shelfmark or
    ours) and ``PURITY`` of the library's leaves with that head are in that collection. The
    collection's own name is not repeated."""
    def leaves_under(nid):
        out = [leaf for _k, leaf in nodes[nid].leaves]
        for child in nodes[nid].children:
            out += leaves_under(child)
        return out

    def library_of(nid):
        while nodes[nid].parent is not None:
            nid = nodes[nid].parent
        return nid

    per_library: dict[str, Counter] = defaultdict(Counter)   # library -> head -> leaves
    per_collection: dict[str, Counter] = {}
    for nid, node in nodes.items():
        if node.kind != "collection":
            continue
        counter = Counter()
        for leaf in leaves_under(nid):
            for h in {head(leaf.shelf_label), head(leaf.shelfmark)} - {()}:
                counter[h] += 1
        per_collection[nid] = counter
        per_library[library_of(nid)].update(counter)
    for nid, counter in per_collection.items():
        library = per_library[library_of(nid)]
        own = head(nodes[nid].name_en) or tuple(_TOKEN.findall(nodes[nid].name_en.lower()))
        nodes[nid].aliases = sorted(" ".join(map(str, h)) for h, n in counter.items()
                                    if n >= MIN_CASES and n / library[h] >= PURITY and h != own)


SCHEMA = """
CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE node (
    id TEXT PRIMARY KEY, parent_id TEXT, kind TEXT NOT NULL, level INTEGER NOT NULL,
    name_en TEXT NOT NULL, name_he TEXT NOT NULL, short TEXT NOT NULL, private INTEGER NOT NULL,
    sort_key INTEGER NOT NULL, lo INTEGER NOT NULL, hi INTEGER NOT NULL,
    manuscripts INTEGER NOT NULL, direct_leaves INTEGER NOT NULL, child_nodes INTEGER NOT NULL);
CREATE TABLE leaf (
    id TEXT PRIMARY KEY, node_id TEXT NOT NULL, library_id TEXT NOT NULL, sys_id TEXT NOT NULL,
    shelfmark TEXT NOT NULL, shelf_label TEXT NOT NULL, part TEXT NOT NULL, inventory_id INTEGER,
    how TEXT NOT NULL,
    pos INTEGER NOT NULL UNIQUE);
CREATE TABLE library_code (code TEXT NOT NULL, node_id TEXT NOT NULL, PRIMARY KEY (code, node_id));
CREATE TABLE node_alias (node_id TEXT NOT NULL, alias TEXT NOT NULL, PRIMARY KEY (node_id, alias));
CREATE INDEX idx_node_parent ON node(parent_id, sort_key);
CREATE INDEX idx_leaf_node ON leaf(node_id, pos);
CREATE INDEX idx_leaf_sys ON leaf(sys_id);
"""


def write(nodes, order, stats, out_path, inputs: dict, codes: dict):
    rows_node, rows_leaf = [], []
    pos = 0

    def walk(nid, level, sort_key, library_id):
        nonlocal pos
        node = nodes[nid]
        lo = pos
        ms = set()
        for i, child in enumerate(node.children):
            ms |= walk(child, level + 1, i, library_id)
        for _k, leaf in node.leaves:
            rows_leaf.append((leaf.id, nid, library_id, leaf.sys_id, leaf.shelfmark, leaf.shelf_label,
                              leaf.part, leaf.inventory_id, leaf.how, pos))
            ms.add(leaf.sys_id)
            pos += 1
        rows_node.append((nid, node.parent, node.kind, level, node.name_en, node.name_he, node.short,
                          node.private, sort_key, lo, pos - 1, len(ms), len(node.leaves), len(node.children)))
        return ms

    for i, lib in enumerate(order):
        walk(lib, 0, i, lib)
    manuscripts = len({r[3] for r in rows_leaf})
    meta = {
        "version": SCHEMA_VERSION,
        "leaves_total": str(len(rows_leaf)),
        "nodes_total": str(len(rows_node)),
        "manuscripts_total": str(manuscripts),
        "top_library_min": str(TOP_LIBRARY_MIN),
        **{f"placed_{h}": str(stats.get(h, 0)) for h in HOW},
        **{k: str(v) for k, v in inputs.items()},
    }
    tmp = Path(str(out_path) + ".tmp")
    if tmp.exists():
        tmp.unlink()
    con = sqlite3.connect(str(tmp))
    try:
        con.executescript(SCHEMA)
        con.executemany("INSERT INTO meta VALUES (?, ?)", sorted(meta.items()))
        con.executemany("INSERT INTO node VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)", rows_node)
        con.executemany("INSERT INTO leaf VALUES (?,?,?,?,?,?,?,?,?,?)", rows_leaf)
        con.executemany("INSERT INTO library_code VALUES (?, ?)",
                        sorted((c, n) for c, ns in codes.items() for n in ns if n in nodes))
        con.executemany("INSERT INTO node_alias VALUES (?, ?)",
                        sorted((nid, a) for nid, n in nodes.items() for a in n.aliases))
        con.commit()
        con.execute("VACUUM")
    finally:
        con.close()
    return tmp, meta


def file_sha256(path) -> str:
    """The file's sha256: binds the tree to the ``libraries.csv`` it was built from (a change of a
    shelfmark or a code keeps the row count but must fail the gate until the tree is rebuilt)."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


def _existing_manuscripts(path) -> Optional[int]:
    try:
        con = sqlite3.connect(f"file:{Path(path).resolve().as_posix()}?mode=ro", uri=True)
        try:
            row = con.execute("SELECT value FROM meta WHERE key = 'manuscripts_total'").fetchone()
        finally:
            con.close()
        return int(row[0]) if row else None
    except (sqlite3.Error, OSError, ValueError):
        return None


def build(fist, libraries, out, force=False, log=print) -> dict:
    # The inputs are fingerprinted before they are read and again before the file is swapped in:
    # an input replaced meanwhile would otherwise leave old placements under the new fingerprint.
    fist_sha, libraries_sha = file_sha256(fist), file_sha256(libraries)
    nodes, order, stats, tied = build_tree(fist, libraries, log=log)
    codes = defaultdict(set)
    for node in nodes.values():
        if node.kind == "library" and node.id.startswith("code:"):
            codes[node.short if node.short != "-" else ""].add(node.id)
    for code, lids in tied.items():
        codes[code] |= {f"lib:{lid}" for lid in lids}
    out = Path(out)
    out.parent.mkdir(parents=True, exist_ok=True)
    tmp, meta = write(nodes, order, stats, out, {
        "input_fist_bytes": os.path.getsize(fist),
        "input_fist_sha256": fist_sha,
        "input_libraries_rows": stats["manuscripts"],
        "input_libraries_sha256": libraries_sha,
    }, codes)
    changed = [name for name, path, sha in (("FIST.db", fist, fist_sha), ("libraries.csv", libraries, libraries_sha))
               if file_sha256(path) != sha]
    if changed:
        tmp.unlink()
        raise BuildError(f"{' and '.join(changed)} changed while the tree was built; build again")
    before = _existing_manuscripts(out) if out.exists() else None
    if before is not None and int(meta["manuscripts_total"]) < before and not force:
        tmp.unlink()
        raise BuildError(f"the new file holds {meta['manuscripts_total']} manuscripts, the old one {before}; "
                         "pass --force to replace it")
    os.replace(tmp, out)
    return meta


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--fist", default=str(DEFAULT_FIST))
    ap.add_argument("--libraries", default=str(DEFAULT_LIBRARIES))
    ap.add_argument("--out", default=str(DEFAULT_OUT))
    ap.add_argument("--force", action="store_true", help="replace a file holding more manuscripts")
    args = ap.parse_args(argv)
    try:
        meta = build(args.fist, args.libraries, args.out, force=args.force)
    except BuildError as exc:
        print(f"FAIL: {exc}")
        return 1
    print("built", args.out)
    for k in sorted(meta):
        print(f"  {k} = {meta[k]}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
