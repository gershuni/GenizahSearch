# -*- coding: utf-8 -*-
"""What both apps' shelf pickers and Browse show from the shelf tree: names, starting points, a
manuscript's place and its position. Pure: it reads a ``ShelfmarkTreeService`` and returns
values; no Qt, no NiceGUI.

Plan: docs/plans/SHELFMARK_TREE_PICKER_PLAN.md (private repository), §3.3-§3.4, §10.4-§10.5.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from shared.shelfmark_tree_service import Placement, ShelfLeaf, ShelfmarkTreeService, ShelfNode

#: Leaves a picker loads at a time.
PAGE_SIZE = 300

#: Between the names of a place on the shelf on the web (Browse, the picker's breadcrumb). The same
#: character in both languages: it is a mirrored character, so the browser draws it as its mirror image
#: in right-to-left text and it points the way the names are read; writing the other one for Hebrew
#: draws it backwards. Not the desktop's: a QLabel holding only the arrow takes no direction from it and
#: draws it as written, so the desktop writes each language's own arrow.
CRUMB_SEP = "›"


def display_name(node: ShelfNode, lang: str = "en") -> str:
    """A node's name in the visitor's language: the Hebrew name where the tree has one."""
    if lang == "he" and node.name_he:
        return node.name_he
    return node.name_en


def is_top_library(node: ShelfNode, service: ShelfmarkTreeService) -> bool:
    """One of the libraries listed first by size (D9); the rest follow alphabetically."""
    try:
        floor = int(service.meta("top_library_min") or 0)
    except ValueError:
        floor = 0
    return node.kind == "library" and floor > 0 and node.manuscripts >= floor


def shown_list(service: ShelfmarkTreeService, leaf: ShelfLeaf) -> Optional[tuple[int, int]]:
    """The positions ``lo..hi`` of the list a leaf is shown in: its node's own shelfmarks, which
    come after the node's volumes and groups (a collection holding both shows them apart), or all
    the ranges a long list is divided into. A group holding shelfmarks is their own shelf, as
    ``crumbs`` shows it (Toronto's "Friedberg MSS 9-004", Lehmann's "Ms. FR")."""
    node = service.node(leaf.node_id)
    if node is None:
        return None
    if node.kind == "range" and node.parent_id:
        ranges = [c for c in service.children(node.parent_id) if c.kind == "range"]
        if ranges:
            return ranges[0].lo, ranges[-1].hi
    return node.hi - node.direct_leaves + 1, node.hi


def crumbs(placement: Placement) -> tuple[ShelfNode, ...]:
    """The place on the shelf to show: library, collection, group, volume. Ranges are left out, and
    so is a library's only collection (Oxford's "Library Shelmarks"): it is no choice."""
    path = placement.path
    return tuple(n for i, n in enumerate(path) if n.kind != "range"
                 and not (i and path[i - 1].child_nodes == 1 and path[i - 1].direct_leaves == 0))


def place_of(service: ShelfmarkTreeService, sys_id: str,
             placement_id: Optional[str] = None) -> Optional[Placement]:
    """The place of a manuscript: the given placement when it is one of its places (the one the
    reader arrived by), else its first in shelf order; None when it has none."""
    places = service.path_of(sys_id) if sys_id else []
    if not places:
        return None
    if placement_id:
        for p in places:
            if p.leaf.id == placement_id:
                return p
    return places[0]


def placement_request_is_spent(want, sys_id: str) -> bool:
    """A placement asked for from the shelf (``(sys_id, placement id)``: a pick, previous / next)
    is for the manuscript opened right after it. Once the reader opens another manuscript it is
    spent: left waiting, it would come back on a later, ordinary arrival at its own manuscript."""
    return bool(want) and bool(sys_id) and want[0] != sys_id


def step_from(service: ShelfmarkTreeService, sys_id: str, want=None):
    """(previous, next) on the shelf from a manuscript whose place has not been read yet, by the
    placement it was opened with (``want``: ``(sys_id, placement id)``) when that is its own, else
    by its first place; None when it has no place. For a second step taken while the first is
    still opening its manuscript: the shelf's order goes on, not the transcription file's."""
    place = place_of(service, sys_id, want[1] if want and want[0] == sys_id else None)
    return None if place is None else service.neighbours(place.leaf.id)


@dataclass(frozen=True)
class Position:
    index: int    # 1-based
    total: int    # shelfmarks in the list it is shown in (a collection's, group's or volume's own)


def position(service: ShelfmarkTreeService, leaf: ShelfLeaf) -> Optional[Position]:
    """Where a leaf stands in the list it is shown in ("7 of 279"), not counting the volumes before
    it in the same collection."""
    bounds = shown_list(service, leaf)
    if bounds is None or bounds[1] < bounds[0]:
        return None
    index, total = service.place_among(leaf, *bounds)
    return Position(index, total) if total else None


def direct_index(service: ShelfmarkTreeService, leaf: ShelfLeaf) -> int:
    """The leaf's index among its own node's direct leaves (0-based), for paging to it."""
    node = service.node(leaf.node_id)
    if node is None:
        return 0
    return max(0, leaf.pos - (node.hi - node.direct_leaves + 1))


@dataclass(frozen=True)
class Start:
    path: tuple[str, ...]           # node ids, library first
    leaf: Optional[ShelfLeaf] = None


def start_at(service: ShelfmarkTreeService, where: Optional[str]) -> Start:
    """Where a picker opens: a manuscript (``sys_id``), a placement id, a node id, or a typed
    prefix (``T-S AS 39``; only when it names one node). Anything else: the libraries."""
    text = (where or "").strip()
    if not text or not service.is_ready():
        return Start(())
    if ":" in text:                                # a placement: on the shelf, or under a catalogue's entry
        leaf = service.leaf(text)
        if leaf is not None:
            return Start(tuple(n.id for n in service.ancestors(leaf.node_id)), leaf)
    if text.isdigit():
        place = place_of(service, text)
        if place is not None:
            return Start(tuple(n.id for n in place.path), place.leaf)
        return Start(())
    if service.node(text) is not None:
        return Start(_down_through(service, tuple(n.id for n in service.ancestors(text))))
    found = service.locate(text)
    if len(found.nodes) == 1:
        return Start(_down_through(service, tuple(n.id for n in service.ancestors(found.nodes[0].id))))
    return Start(())


def _down_through(service: ShelfmarkTreeService, path: tuple) -> tuple:
    """A node of one child and no shelfmarks of its own is no choice: open at what is below it, as
    a click on it does (a library's sole collection, a collection's sole volume)."""
    while path:
        node = service.node(path[-1])
        if node is None or node.direct_leaves or node.child_nodes != 1:
            break
        path = path + (service.children(node.id)[0].id,)
    return path
