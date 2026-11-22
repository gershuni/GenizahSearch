# -*- coding: utf-8 -*-
"""Read-only access to the shelf tree (``shelfmark_tree_data/shelfmark_tree.db``).

Plan: docs/plans/SHELFMARK_TREE_PICKER_PLAN.md (private repository), §3.2 as amended by §10.
``scripts/build_shelfmark_tree.py`` builds the file and ``scripts/verify_shelfmark_tree_sidecar.py`` gates
it; this module only reads it. No Qt, no NiceGUI.

The tree is library > collection > volume > shelfmark, with range nodes over large flat nodes and
an "Other shelfmarks" node per library for the manuscripts FGP does not place. A *leaf* is one
placement of one manuscript (``placement_id`` = the leaf id): a manuscript can sit at two places,
and ``path_of`` answers every one. Every leaf has a position in one shelf order across a library,
so ``neighbours`` crosses ranges, volumes and collections and stops only at the library's ends.

Fail-closed like the other sidecar readers: READY only when the file is where a sidecar lives,
opens read-only, carries the schema version this reader serves, holds every table it reads and as
many leaves as its ``meta`` says. Otherwise ``is_ready()`` is False with the reason in
``unavailable_reason``, and every query answers empty. Nothing here raises into a page.
"""
from __future__ import annotations

import logging
import os
import re
import sqlite3
import threading
from collections import OrderedDict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

from shared.thread_local_db import ThreadLocalConnection

logger = logging.getLogger(__name__)

#: The sidecar schema this reader serves; tests pin it to scripts/build_shelfmark_tree.py's.
SCHEMA_VERSION = "1.1.0"      # 1.1.0: leaf.aliases
SIDECAR_DIR = "shelfmark_tree_data"
SIDECAR_FILENAME = "shelfmark_tree.db"
REQUIRED_TABLES = ("meta", "node", "leaf", "library_code", "node_alias", "shortcut")
#: The most leaves one ``leaves`` call returns.
MAX_PAGE = 1000
#: Nodes whose manuscripts ``members_under`` keeps (a picker recounts its chosen nodes on every change).
_MEMBERS_KEPT = 32

_NODE_COLUMNS = ("id, parent_id, kind, level, name_en, name_he, short, private, sort_key, lo, hi, "
                 "manuscripts, direct_leaves, child_nodes")
_LEAF_COLUMNS = ("id, node_id, library_id, sys_id, shelfmark, shelf_label, label, part, inventory_id, how, pos, "
                 "aliases")
_TOKEN = re.compile(r"[^\W\d_]+|\d+")
#: ``leaf.how`` of a manuscript's second place, under its entry in a catalogue FGP lists a collection by.
CATALOGUE = "catalogue"


def default_sidecar_path() -> str:
    """The user's updated copy first (as for the other sidecars), then the repository's."""
    local = os.environ.get("LOCALAPPDATA", "")
    if local:
        user_path = os.path.join(local, "GenizahSearchPro", "data", SIDECAR_DIR, SIDECAR_FILENAME)
        if os.path.isfile(user_path):
            return user_path
    return str(Path(__file__).resolve().parents[1] / SIDECAR_DIR / SIDECAR_FILENAME)


@dataclass(frozen=True)
class ShelfNode:
    id: str
    parent_id: Optional[str]
    kind: str            # library | collection | volume | range | other
    level: int
    name_en: str
    name_he: str
    short: str           # a library's short name ('CUL'); '' below libraries
    private: bool
    sort_key: int        # position among its siblings
    lo: int              # the shelf positions it owns, inclusive
    hi: int
    manuscripts: int     # distinct manuscripts under it
    direct_leaves: int   # leaves directly in it (not in a child node)
    child_nodes: int


@dataclass(frozen=True)
class ShelfLeaf:
    id: str              # the placement id
    node_id: str
    library_id: str
    sys_id: str
    shelfmark: str       # the shelfmark the apps show for the manuscript
    shelf_label: str     # FGP's shelfmark of the inventory row ('' when not placed by FGP)
    label: str           # what a picker shows: FGP's shelfmark, else ours without institution or file names
    part: str            # the Oxford (Neubauer) Part, information only ('' elsewhere)
    inventory_id: Optional[int]
    how: str             # fist | exact | collection | single | name | other; catalogue (a second place)
    pos: int
    aliases: str = ""    # the record's other call numbers, " | " (a filter finds them; 1.1.0)

    @property
    def placement_id(self) -> str:
        return self.id

    def as_selection(self) -> dict:
        """What a picker hands its host (plan §10.4)."""
        return {"sys_id": self.sys_id, "shelfmark": self.shelfmark, "placement_id": self.id}


@dataclass(frozen=True)
class Placement:
    leaf: ShelfLeaf
    path: tuple[ShelfNode, ...]   # library first, the leaf's own node last


@dataclass(frozen=True)
class Located:
    nodes: tuple[ShelfNode, ...]  # the deepest nodes whose names the text starts with
    ambiguous: bool               # more than one


def _key(text: str) -> tuple:
    return tuple(int(t) if t.isdigit() else t for t in _TOKEN.findall((text or "").lower()))


def _like_escape(text: str) -> str:
    return text.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


class ShelfmarkTreeService:
    """Read-only access to the shelf tree. See the module docstring."""

    def __init__(self, db_path: Optional[str] = None, thread_safe: bool = False):
        self._conn: Any = None
        self._meta: dict[str, str] = {}
        self._names: Optional[list[tuple[tuple, ShelfNode]]] = None
        self._names_lock = threading.Lock()
        self._members: OrderedDict = OrderedDict()
        self._members_lock = threading.Lock()
        self.unavailable_reason = "not opened"
        self._db_path = db_path or default_sidecar_path()
        if not os.path.isfile(self._db_path):
            self.unavailable_reason = f"no sidecar at {self._db_path}"
            logger.info("ShelfmarkTreeService: %s", self.unavailable_reason)
            return
        conn = None
        try:
            uri = f"file:{Path(self._db_path).resolve().as_posix()}?mode=ro"
            if thread_safe:
                conn = ThreadLocalConnection(uri, timeout=10.0)
            else:
                conn = sqlite3.connect(uri, uri=True, check_same_thread=False, timeout=10.0)
            problem, meta = _validate(conn)
        except Exception as exc:  # not a database, locked, ...
            problem, meta = f"cannot open: {exc}", {}
        if problem:
            self.unavailable_reason = problem
            logger.warning("ShelfmarkTreeService: %s is not served: %s", self._db_path, problem)
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass
            return
        self._conn = conn
        self._meta = meta
        self.unavailable_reason = ""
        logger.info("ShelfmarkTreeService: serving %s (schema %s, %s leaves, %s manuscripts)",
                    self._db_path, meta.get("version"), meta.get("leaves_total"), meta.get("manuscripts_total"))

    # -- state ---------------------------------------------------------------

    @property
    def db_path(self) -> str:
        return self._db_path

    def is_ready(self) -> bool:
        return self._conn is not None

    def meta(self, key: str) -> str:
        return self._meta.get(key, "")

    def close(self) -> None:
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:
                pass
        self._conn = None
        with self._members_lock:
            self._members.clear()
        self.unavailable_reason = "closed"

    def _rows(self, sql: str, args: tuple = ()) -> list:
        if self._conn is None:
            return []
        try:
            return self._conn.execute(sql, args).fetchall()
        except Exception as exc:
            logger.warning("ShelfmarkTreeService: query failed: %s", exc)
            return []

    # -- the tree ------------------------------------------------------------

    def node(self, node_id: str) -> Optional[ShelfNode]:
        rows = self._rows(f"SELECT {_NODE_COLUMNS} FROM node WHERE id = ?", (node_id,))
        return _node(rows[0]) if rows else None

    def children(self, node_id: Optional[str] = None) -> list[ShelfNode]:
        """The next level in shelf order; the libraries when ``node_id`` is None."""
        if node_id is None:
            rows = self._rows(f"SELECT {_NODE_COLUMNS} FROM node WHERE parent_id IS NULL ORDER BY sort_key")
        else:
            rows = self._rows(f"SELECT {_NODE_COLUMNS} FROM node WHERE parent_id = ? ORDER BY sort_key",
                              (node_id,))
        return [_node(r) for r in rows]

    def leaves(self, node_id: str, offset: int = 0, limit: int = 200,
               filter: str = "") -> tuple[list[ShelfLeaf], int]:
        """The shelfmarks directly in a node, in shelf order, paged; with ``filter``, only those whose
        shelfmark, FGP shelfmark or Oxford Part contains it (case-insensitive for Latin letters).
        Answers the page and the total that match."""
        limit = max(0, min(int(limit), MAX_PAGE))
        offset = max(0, int(offset))
        where, args = "node_id = ?", [node_id]
        text = (filter or "").strip()
        if text:
            pattern = f"%{_like_escape(text)}%"
            where += (" AND (shelfmark LIKE ? ESCAPE '\\' OR label LIKE ? ESCAPE '\\'"
                      " OR part LIKE ? ESCAPE '\\' OR aliases LIKE ? ESCAPE '\\')")
            args += [pattern, pattern, pattern, pattern]
        total = self._rows(f"SELECT COUNT(*) FROM leaf WHERE {where}", tuple(args))
        rows = self._rows(f"SELECT {_LEAF_COLUMNS} FROM leaf WHERE {where} ORDER BY pos LIMIT ? OFFSET ?",
                          tuple(args) + (limit, offset))
        return [_leaf(r) for r in rows], (total[0][0] if total else 0)

    def search_leaves(self, node_id: str, text: str, limit: int = 200,
                      offset: int = 0) -> tuple[list[ShelfLeaf], int]:
        """Shelfmarks anywhere under a node (inside its ranges, groups and volumes) whose
        shelfmark, label or Oxford Part contains ``text``: for a reader who knows the shelfmark.
        Answers the page and the total that match. A node that holds the shelf and a catalogue's
        list of the same manuscripts answers their places on the shelf, once each; a search inside
        the list answers the list's."""
        node = self.node(node_id)
        text = (text or "").strip()
        if node is None or not text:
            return [], 0
        pattern = f"%{_like_escape(text)}%"
        where = ("pos BETWEEN ? AND ? AND (shelfmark LIKE ? ESCAPE '\\' OR label LIKE ? ESCAPE '\\'"
                 " OR part LIKE ? ESCAPE '\\' OR aliases LIKE ? ESCAPE '\\')" + self._shelf_only(node))
        args = (node.lo, node.hi, pattern, pattern, pattern, pattern)
        total = self._rows(f"SELECT COUNT(*) FROM leaf WHERE {where}", args)
        rows = self._rows(f"SELECT {_LEAF_COLUMNS} FROM leaf WHERE {where} ORDER BY pos LIMIT ? OFFSET ?",
                          args + (max(0, min(int(limit), MAX_PAGE)), max(0, int(offset))))
        return [_leaf(r) for r in rows], (total[0][0] if total else 0)

    def _node_search(self, node_id: str, text: str):
        node = self.node(node_id)
        text = (text or "").strip()
        if node is None or not text:
            return None
        pattern = f"%{_like_escape(text)}%"
        # every kind a column shows, so a filter reaches what the unfiltered column offers
        return ("lo >= ? AND hi <= ? AND id <> ? AND kind IN ('collection', 'group', 'volume', 'range', 'other') "
                "AND (name_en LIKE ? ESCAPE '\\' OR name_he LIKE ? ESCAPE '\\')",
                (node.lo, node.hi, node.id, pattern, pattern))

    def search_nodes(self, node_id: str, text: str, limit: int = 200, offset: int = 0) -> list[ShelfNode]:
        """Collections, groups and volumes anywhere under a node whose name contains ``text``."""
        found = self._node_search(node_id, text)
        if found is None:
            return []
        where, args = found
        rows = self._rows(f"SELECT {_NODE_COLUMNS} FROM node WHERE {where} ORDER BY lo, level LIMIT ? OFFSET ?",
                          args + (max(0, min(int(limit), MAX_PAGE)), max(0, int(offset))))
        return [_node(r) for r in rows]

    def count_nodes(self, node_id: str, text: str) -> int:
        """How many nodes ``search_nodes`` would answer without a limit."""
        found = self._node_search(node_id, text)
        if found is None:
            return 0
        where, args = found
        rows = self._rows(f"SELECT COUNT(*) FROM node WHERE {where}", args)
        return rows[0][0] if rows else 0

    def shortcuts(self) -> list:
        """The libraries shown inside another: (id, name_en, name_he, short, target node)."""
        out = []
        for sid, en, he, short, target in self._rows("SELECT id, name_en, name_he, short, target_id FROM shortcut"):
            node = self.node(target)
            if node is not None:
                out.append((sid, en, he, short, node))
        return out

    def leaf(self, placement_id: str) -> Optional[ShelfLeaf]:
        rows = self._rows(f"SELECT {_LEAF_COLUMNS} FROM leaf WHERE id = ?", (placement_id,))
        return _leaf(rows[0]) if rows else None

    def ancestors(self, node_id: str) -> tuple[ShelfNode, ...]:
        """The node and the nodes above it, library first."""
        out = []
        seen = set()
        current = self.node(node_id)
        while current is not None and current.id not in seen:
            seen.add(current.id)
            out.append(current)
            current = self.node(current.parent_id) if current.parent_id else None
        return tuple(reversed(out))

    def path_of(self, sys_id: str) -> list[Placement]:
        """Every place of a manuscript (empty when it has none): its places on the shelf, in shelf
        order, then those under a catalogue's entries. The shelf comes first whatever the positions
        say: a collection's own shelfmarks are written after its children, a catalogue's list among
        them."""
        rows = self._rows(f"SELECT {_LEAF_COLUMNS} FROM leaf WHERE sys_id = ? "
                          "ORDER BY (how = 'catalogue'), pos", (str(sys_id),))
        return [Placement(leaf, self.ancestors(leaf.node_id)) for leaf in map(_leaf, rows)]

    def neighbours(self, placement_id: str) -> tuple[Optional[ShelfLeaf], Optional[ShelfLeaf]]:
        """The previous and next manuscript on the shelf from this placement, across ranges,
        volumes and collections; None at the library's ends (or for an unknown placement).
        Another placement of the same manuscript is passed over."""
        here = self.leaf(placement_id)
        if here is None:
            return None, None
        # A catalogue's list (Oxford by Neubauer-Cowley number) is a second way through manuscripts
        # that are on the shelf: stepping stays in the list it began in, and the shelf's own order
        # does not run on into a catalogue's.
        same, args = "how <> 'catalogue'", (here.library_id, here.pos, here.sys_id)
        if here.how == CATALOGUE:
            # ... and in that list alone: a library may list two of its collections by a catalogue
            lo, hi = self._list_bounds(here)
            same, args = "how = 'catalogue' AND pos BETWEEN ? AND ?", args + (lo, hi)
        prev = self._rows(f"SELECT {_LEAF_COLUMNS} FROM leaf WHERE library_id = ? AND pos < ? AND sys_id <> ? "
                          f"AND {same} ORDER BY pos DESC LIMIT 1", args)
        nxt = self._rows(f"SELECT {_LEAF_COLUMNS} FROM leaf WHERE library_id = ? AND pos > ? AND sys_id <> ? "
                         f"AND {same} ORDER BY pos LIMIT 1", args)
        return (_leaf(prev[0]) if prev else None), (_leaf(nxt[0]) if nxt else None)

    def _list_bounds(self, leaf: ShelfLeaf) -> tuple[int, int]:
        """The positions of the catalogue list a catalogue placement stands in: the topmost node
        above it that holds no place on the shelf."""
        for node in self.ancestors(leaf.node_id):
            if not self._rows("SELECT 1 FROM leaf WHERE pos BETWEEN ? AND ? AND how <> 'catalogue' LIMIT 1",
                              (node.lo, node.hi)):
                return node.lo, node.hi
        return leaf.pos, leaf.pos

    def place_among(self, leaf: ShelfLeaf, lo: int, hi: int) -> tuple[int, int]:
        """(the 1-based index of a place, how many there are) among the places ``lo..hi`` of its own
        kind: those on the shelf, or those of a catalogue's list. A collection holds both, and a
        shelfmark's "7 of 279" does not count the list's copies."""
        same = "how = 'catalogue'" if leaf.how == CATALOGUE else "how <> 'catalogue'"
        rows = self._rows(f"SELECT COALESCE(SUM(pos <= ?), 0), COUNT(*) FROM leaf "
                          f"WHERE pos BETWEEN ? AND ? AND {same}", (leaf.pos, lo, hi))
        return (int(rows[0][0]), int(rows[0][1])) if rows else (0, 0)

    def _shelf_only(self, node: ShelfNode) -> str:
        """The condition that keeps a node's places on the shelf when it holds any: a collection
        holds its shelfmarks and a catalogue's list of the same manuscripts, written before them.
        Inside the list itself (no place on the shelf at all) nothing is left out."""
        on_shelf = self._rows("SELECT 1 FROM leaf WHERE pos BETWEEN ? AND ? AND how <> 'catalogue' LIMIT 1",
                              (node.lo, node.hi))
        return " AND how <> 'catalogue'" if on_shelf else ""

    def sys_ids_under(self, node_id: str) -> list[str]:
        """Every manuscript under a node, each once, in shelf order ("the whole volume"); in a
        catalogue's list, in the list's order."""
        node = self.node(node_id)
        if node is None:
            return []
        rows = self._rows(f"SELECT sys_id FROM leaf WHERE pos BETWEEN ? AND ?{self._shelf_only(node)} ORDER BY pos",
                          (node.lo, node.hi))
        return list(dict.fromkeys(r[0] for r in rows))

    def leaves_under(self, node_id: str, direct: bool = False) -> list[ShelfLeaf]:
        """Each manuscript under a node once, by its first place there in shelf order, with
        ``sys_ids_under``'s order and its policy for a catalogue's list: a node holding places on the
        shelf leaves the list's copies out, a node inside the list keeps them. ``direct``: only the
        node's own shelfmarks (a collection that also holds volumes). What a whole-node choice of the
        picker resolves to (plan §10.13); never a manuscript's first place elsewhere (``path_of``)."""
        node = self.node(node_id)
        if node is None:
            return []
        if direct:
            rows = self._rows(f"SELECT {_LEAF_COLUMNS} FROM leaf WHERE node_id = ? ORDER BY pos", (node.id,))
        else:
            rows = self._rows(f"SELECT {_LEAF_COLUMNS} FROM leaf WHERE pos BETWEEN ? AND ?{self._shelf_only(node)} "
                              "ORDER BY pos", (node.lo, node.hi))
        seen: set[str] = set()
        out = []
        for leaf in map(_leaf, rows):
            if leaf.sys_id not in seen:
                seen.add(leaf.sys_id)
                out.append(leaf)
        return out

    def members_under(self, node_id: str, direct: bool = False, keep: bool = True) -> frozenset:
        """The manuscripts ``leaves_under`` answers, as a set; kept for the last few nodes asked
        (a picker counts its chosen nodes again on every change). ``keep=False``: read, and not
        kept unless it already was (a picker's drawn rows, read once each)."""
        key = (node_id, bool(direct))
        with self._members_lock:
            hit = self._members.get(key)
            if hit is not None:
                self._members.move_to_end(key)
                return hit
        node = self.node(node_id)
        if node is None:
            return frozenset()
        if direct:
            rows = self._rows("SELECT sys_id FROM leaf WHERE node_id = ?", (node.id,))
        else:
            rows = self._rows(f"SELECT sys_id FROM leaf WHERE pos BETWEEN ? AND ?{self._shelf_only(node)}",
                              (node.lo, node.hi))
        found = frozenset(r[0] for r in rows)
        if keep and self._conn is not None:
            with self._members_lock:
                self._members[key] = found
                while len(self._members) > _MEMBERS_KEPT:
                    self._members.popitem(last=False)
        return found

    def libraries_of_code(self, code: str) -> list[str]:
        """The library node ids a ``library_code`` is tied to."""
        return [r[0] for r in self._rows("SELECT node_id FROM library_code WHERE code = ? ORDER BY node_id",
                                         (code,))]

    def codes_of_library(self, node_id: str) -> list[str]:
        """The ``library_code`` values tied to a library node ("CUL", "Mosseri", "Westminster"), A-Z;
        the chips name a library by one of them (``shared/shelfmark_tree_limits.py::library_code``)."""
        return [r[0] for r in self._rows("SELECT code FROM library_code WHERE node_id = ? ORDER BY code",
                                         (node_id,)) if r[0]]

    def library_codes(self) -> dict[str, tuple[str, ...]]:
        """Every library node's ``library_code`` values, A-Z, in one query: the library column's
        filter finds a library by them ("RNL", "CUL", "BL")."""
        out: dict[str, list[str]] = {}
        for node_id, code in self._rows("SELECT node_id, code FROM library_code ORDER BY node_id, code"):
            if code:
                out.setdefault(node_id, []).append(code)
        return {k: tuple(v) for k, v in out.items()}

    def locate(self, text: str, library: Optional[str] = None) -> Located:
        """The collection or volume whose name covers most of the typed text, token by token
        (``T-S AS 39.7`` -> the volume ``T-S AS 39``), within one library node or code when given.
        Libraries compete only on how many tokens their best name covers: a deeper node in one
        library does not hide an equally long name in another (``Add.`` is CUL's collection and a
        Westminster volume), so both are answered and reported as ambiguous. Within a library the
        deepest node wins."""
        want = _key(text)
        if not want or self._conn is None:
            return Located((), False)
        scope: Optional[set[str]] = None
        if library:
            scope = {library} if library.startswith(("lib:", "code:")) else set(self.libraries_of_code(library))
        matches: list[tuple[int, ShelfNode, str]] = []
        for name_key, node in self._name_index():
            if len(name_key) > len(want) or want[:len(name_key)] != name_key:
                continue
            library_id = self._library_of(node)
            if scope is not None and library_id not in scope:
                continue
            matches.append((len(name_key), node, library_id))
        if not matches:
            return Located((), False)
        longest = max(length for length, _n, _l in matches)
        per_library: dict[str, list[ShelfNode]] = {}
        for length, node, library_id in matches:
            if length == longest:
                per_library.setdefault(library_id, []).append(node)
        found = []
        for nodes in per_library.values():
            deepest = max(n.level for n in nodes)
            found += list({n.id: n for n in nodes if n.level == deepest}.values())
        found.sort(key=lambda n: n.lo)
        return Located(tuple(found), len(found) > 1)

    def _library_of(self, node: ShelfNode) -> str:
        return self.ancestors(node.id)[0].id

    def _name_index(self) -> list[tuple[tuple, ShelfNode]]:
        with self._names_lock:
            if self._names is None:
                # and a group that is a shelf of its own, holding shelfmarks (or their ranges) under a
                # collection: Lehmann's series "Ms. FR", Toronto's "Friedberg MSS 9-004". Not a group
                # of volumes, nor a series of "Other shelfmarks" (a typed "Ms. 9160" is JTS's "MS")
                columns = ", ".join(f"n.{c}" for c in _NODE_COLUMNS.split(", "))
                rows = self._rows(
                    f"SELECT {columns} FROM node n WHERE n.kind IN ('collection', 'volume') OR (n.kind = 'group'"
                    " AND (SELECT kind FROM node p WHERE p.id = n.parent_id) = 'collection'"
                    " AND (n.direct_leaves > 0 OR n.child_nodes > 0)"
                    " AND NOT EXISTS (SELECT 1 FROM node c WHERE c.parent_id = n.id AND c.kind <> 'range'))")
                names = [(_key(r[4]), _node(r)) for r in rows if _key(r[4])]
                # a collection is also found by the shelfmark heads its leaves are written with
                # (the builder's ``collection_aliases``: CUL "Oriental" by ``Or.``)
                by_id = {n.id: n for _k, n in names}
                for node_id, alias in self._rows("SELECT node_id, alias FROM node_alias"):
                    if node_id in by_id and _key(alias):
                        names.append((_key(alias), by_id[node_id]))
                self._names = names
            return self._names


def _node(row) -> ShelfNode:
    return ShelfNode(row[0], row[1], row[2], int(row[3]), row[4], row[5], row[6], bool(row[7]), int(row[8]),
                     int(row[9]), int(row[10]), int(row[11]), int(row[12]), int(row[13]))


def _leaf(row) -> ShelfLeaf:
    return ShelfLeaf(row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], int(row[10]),
                     row[11] or "")


def _validate(conn: Any) -> tuple[str, dict[str, str]]:
    """'' and the meta when the open file is one this reader serves; else the reason."""
    tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}
    missing = [t for t in REQUIRED_TABLES if t not in tables]
    if missing:
        return f"missing table(s): {', '.join(missing)}", {}
    meta = {str(k): ("" if v is None else str(v)) for k, v in conn.execute("SELECT key, value FROM meta")}
    if meta.get("version") != SCHEMA_VERSION:
        return f"schema version {meta.get('version')!r}; this reader serves {SCHEMA_VERSION}", {}
    expected = meta.get("leaves_total", "")
    actual = conn.execute("SELECT COUNT(*) FROM leaf").fetchone()[0]
    if not expected.isdigit() or int(expected) != int(actual) or int(actual) == 0:
        return f"meta.leaves_total = {expected!r} but leaf holds {actual} rows", {}
    return "", meta


# ---------------------------------------------------------------------------
# The process-wide instance
# ---------------------------------------------------------------------------

_default_service: Optional[ShelfmarkTreeService] = None
_lock = threading.Lock()


def shelfmark_tree_enabled() -> bool:
    """The ``SHELFMARK_TREE_ENABLED`` switch, read on every call; on unless set off. Necessary, not
    sufficient: a surface also needs the file open and checked (``is_ready``)."""
    value = os.environ.get("SHELFMARK_TREE_ENABLED")
    if value is None:
        return True
    return value.strip().lower() in {"1", "true", "yes", "on"}


def peek_shelfmark_tree_service() -> Optional[ShelfmarkTreeService]:
    """The default service if it was opened already, else None (never opens the file)."""
    return _default_service


def get_shelfmark_tree_service(thread_safe: bool = True) -> ShelfmarkTreeService:
    """The default service, opened on first use (per-thread connections by default: safe under
    NiceGUI's ``run.io_bound`` pool)."""
    global _default_service
    with _lock:
        if _default_service is None:
            _default_service = ShelfmarkTreeService(thread_safe=thread_safe)
        return _default_service


def reset_shelfmark_tree_service() -> None:
    """Forget the default service (after the sidecar file is replaced)."""
    global _default_service
    with _lock:
        if _default_service is not None:
            _default_service.close()
            _default_service = None
