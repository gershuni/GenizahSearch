# -*- coding: utf-8 -*-
"""What a reader has chosen on the shelf in the picker's multiple-choice mode, for both apps.

Pure: no Qt, no NiceGUI. The web dialog (``web/components/shelfmark_tree_picker.py``) and, later,
the desktop widget keep one ``Selection`` per purpose (Exclude, the Reading Desk, a list ...) and
hand their host what it resolves to.

A selection is a list of *chips*, in the order chosen (plan §10.13, D11):

- a **shelfmark**: one placement of one manuscript;
- a **whole node**: a volume, a range, a collection ... ``scope`` ``all`` is everything under it,
  ``direct`` only the node's own shelfmarks (a collection that also holds volumes). A whole
  volume is ONE chip, however many manuscripts it holds (DX3); unticking a shelfmark inside it
  records the manuscript in the chip's ``removed`` and lowers its count; removing the chip removes
  all of it.

A manuscript is chosen once however it is reached: choosing a node folds in the chips it covers,
choosing what a chosen node already covers ticks it again there, and every count is of distinct
manuscripts (a manuscript at two places in two chosen volumes counts once).

What a node holds is ``service.members_under`` / ``service.leaves_under``: in ``sys_ids_under``'s
order and with its policy for a catalogue's list (a node holding places on the shelf leaves the
list's copies out; a node inside the list keeps them). A selection resolves to the picker's contract
(§10.4) ``[{sys_id, shelfmark, placement_id}]`` or to the ids alone, never to a LOCAL ``97…`` id.

It is kept as JSON (D12, versioned): ``to_dict`` / ``from_dict`` never raise on a stale or broken
value, and ``prune`` drops what a rebuilt tree no longer has. ``consume`` takes out what a host
applied, so a partial application (the Reading Desk's limit per add, DX5) keeps the rest.

Plan: docs/plans/SHELFMARK_TREE_PICKER_PLAN.md (private repository), §3.3, §10.4, §10.8, §10.13;
handoff DX3-DX5.
"""
from __future__ import annotations

import copy
from dataclasses import dataclass, field
from typing import Any, Callable, Iterable, Optional

from shared.sys_id_patterns import CORPUS_SYS_ID_RE

FORMAT = "shelf-selection"
VERSION = 1
LEAF = "leaf"
NODE = "node"
ALL = "all"
DIRECT = "direct"
#: Node kinds a reader may choose whole. A library is not (the search has its own library
#: filter, and a whole library is tens of thousands of manuscripts).
CHOOSABLE_KINDS = frozenset({"collection", "group", "volume", "range", "other"})


def is_corpus(sys_id: Any) -> bool:
    """A corpus ``99…`` id; the tree never holds another, and a stored set must not bring one."""
    return isinstance(sys_id, str) and CORPUS_SYS_ID_RE.fullmatch(sys_id) is not None


@dataclass
class Chip:
    kind: str                     # LEAF | NODE
    ref: str                      # the placement id (LEAF) or the node id (NODE)
    label: str = ""               # the name when chosen; a host shows a node's name in the reader's language
    sys_id: str = ""              # LEAF
    shelfmark: str = ""           # LEAF
    scope: str = ALL              # NODE: ALL | DIRECT
    removed: set = field(default_factory=set)   # NODE: manuscripts unticked inside it

    @property
    def key(self) -> str:
        return f"node:{self.scope}:{self.ref}" if self.kind == NODE else f"leaf:{self.ref}"


MembersOf = Callable[[Chip], frozenset]


def members_function(service) -> MembersOf:
    """What a chip holds, from the tree: a node's manuscripts (cached by the service), a
    shelfmark's one."""
    def members_of(chip: Chip) -> frozenset:
        if chip.kind == NODE:
            return service.members_under(chip.ref, chip.scope == DIRECT)
        return frozenset((chip.sys_id,)) if chip.sys_id else frozenset()
    return members_of


def scope_for(node) -> str:
    """The scope of "Select all" over a node's shelfmarks: everything under it when it has no child
    nodes (a volume, a range, "Other shelfmarks"), else its own shelfmarks only."""
    return DIRECT if node is not None and node.child_nodes and node.direct_leaves else ALL


class Selection:
    """See the module docstring. Changes take ``members_of`` (``members_function(service)``);
    resolving takes the service."""

    def __init__(self, chips: Optional[Iterable[Chip]] = None):
        self.chips: list[Chip] = list(chips or [])

    def __len__(self) -> int:
        return len(self.chips)

    def copy(self) -> "Selection":
        """A copy a worker thread may read while the page changes the selection."""
        return Selection(copy.deepcopy(self.chips))

    # -- what is chosen -------------------------------------------------------

    def _effective(self, chip: Chip, members_of: MembersOf) -> frozenset:
        if chip.kind == LEAF:
            return frozenset((chip.sys_id,)) if is_corpus(chip.sys_id) else frozenset()
        return frozenset(s for s in members_of(chip) if s not in chip.removed and is_corpus(s))

    def chip_count(self, chip: Chip, members_of: MembersOf) -> int:
        return len(self._effective(chip, members_of))

    def chosen_set(self, members_of: MembersOf) -> set[str]:
        out: set[str] = set()
        for chip in self.chips:
            out |= self._effective(chip, members_of)
        return out

    def count(self, members_of: MembersOf) -> int:
        """Distinct manuscripts chosen."""
        return len(self.chosen_set(members_of))

    def chosen_among(self, sys_ids: Iterable[str], members_of: MembersOf) -> set[str]:
        chosen = self.chosen_set(members_of)
        return {s for s in sys_ids if s in chosen}

    def has_node(self, node_id: str, scope: str = ALL) -> bool:
        return any(c.kind == NODE and c.ref == node_id and c.scope == scope for c in self.chips)

    def node_state(self, node, scope: str, members_of: MembersOf) -> str:
        """``all``, ``some`` or ``none`` of a node's manuscripts chosen (its box: ticked, partly,
        empty)."""
        members = members_of(Chip(NODE, node.id, scope=scope))
        if not members:
            return "none"
        chosen = self.chosen_set(members_of) & members
        if not chosen:
            return "none"
        return "all" if chosen == members else "some"

    # -- changes --------------------------------------------------------------

    def add_leaf(self, leaf, members_of: MembersOf) -> bool:
        """Tick one shelfmark. A manuscript a chosen node holds but had unticked is ticked again
        there; one already chosen is left as it is. Answers whether anything changed."""
        sid = leaf.sys_id
        if not is_corpus(sid):
            return False
        covered = changed = False
        for chip in self.chips:
            if chip.kind == LEAF:
                covered = covered or chip.sys_id == sid
            elif sid in members_of(chip):
                covered = True
                if sid in chip.removed:
                    chip.removed.discard(sid)
                    changed = True
        if covered:
            return changed
        self.chips.append(Chip(LEAF, leaf.id, leaf.label or leaf.shelfmark or sid, sid, leaf.shelfmark))
        return True

    def add_node(self, node, scope: str, members_of: MembersOf) -> bool:
        """Tick a whole node: what is chosen becomes what was chosen and all the node's manuscripts
        (any unticked inside it are ticked again). One chip: chips whose every chosen manuscript the
        node holds are folded into it; a node a chosen node already holds whole is ticked again
        inside that chip instead of making a second one. Coverage is by manuscripts, never by shelf
        positions (a collection's ``direct`` chip does not hold its volumes)."""
        if node is None or node.kind not in CHOOSABLE_KINDS:
            return False
        new = Chip(NODE, node.id, node.name_en, scope=DIRECT if scope == DIRECT else ALL)
        members = frozenset(s for s in members_of(new) if is_corpus(s))
        if not members:
            return False
        for chip in self.chips:
            if chip.kind == NODE and members <= members_of(chip):
                again = chip.removed & members
                chip.removed -= members
                return bool(again)
        self.chips = [c for c in self.chips if not (self._effective(c, members_of) <= members)]
        self.chips.append(new)
        return True

    def drop_leaf(self, sys_id: str, members_of: MembersOf) -> bool:
        """Untick one manuscript wherever it is chosen. A node chip left holding nothing goes."""
        return self._drop({sys_id}, members_of)

    def drop_node(self, node, scope: str, members_of: MembersOf) -> bool:
        """Untick a whole node: its chip goes, and so do its manuscripts wherever else they are
        chosen (a node ticked through a chosen collection is unticked inside it)."""
        members = members_of(Chip(NODE, node.id, scope=DIRECT if scope == DIRECT else ALL))
        before = len(self.chips)
        self.chips = [c for c in self.chips if not (c.kind == NODE and c.ref == node.id and c.scope == scope)]
        changed = self._drop(set(members), members_of)
        return changed or len(self.chips) != before

    def _drop(self, sys_ids: set, members_of: MembersOf) -> bool:
        changed = False
        keep = []
        for chip in self.chips:
            if chip.kind == LEAF:
                if chip.sys_id in sys_ids:
                    changed = True
                    continue
            else:
                members = members_of(chip)
                gone = (members & sys_ids) - chip.removed
                if gone:
                    chip.removed |= gone
                    changed = True
                    if not (members - chip.removed):
                        continue
            keep.append(chip)
        self.chips = keep
        return changed

    def remove_chip(self, key: str) -> bool:
        before = len(self.chips)
        self.chips = [c for c in self.chips if c.key != key]
        return len(self.chips) != before

    def clear(self) -> None:
        self.chips = []

    def consume(self, applied_ids: Iterable[str], members_of: MembersOf) -> None:
        """Take out what a host applied; what it did not take stays chosen."""
        self._drop(set(applied_ids), members_of)

    # -- what it resolves to (blocking: call off the UI thread) -----------------

    def resolve_entries(self, service) -> list[dict]:
        """The picker's contract, one entry per manuscript in the order chosen (a node's in shelf
        order): ``{sys_id, shelfmark, placement_id}``."""
        out: list[dict] = []
        seen: set[str] = set()
        for chip in self.chips:
            if chip.kind == LEAF:
                entries = [{"sys_id": chip.sys_id, "shelfmark": chip.shelfmark, "placement_id": chip.ref}]
            else:
                entries = [lf.as_selection() for lf in service.leaves_under(chip.ref, chip.scope == DIRECT)
                           if lf.sys_id not in chip.removed]
            for entry in entries:
                sid = entry["sys_id"]
                if is_corpus(sid) and sid not in seen:
                    seen.add(sid)
                    out.append(entry)
        return out

    def sys_ids(self, service) -> list[str]:
        return [e["sys_id"] for e in self.resolve_entries(service)]

    def summary(self, name_of: Optional[Callable[[Chip], str]] = None, shown: int = 3) -> str:
        """The chips' names for a host's label ("T-S AS 39, Or. 5 +3")."""
        names = [(name_of(c) if name_of else c.label) or c.label for c in self.chips]
        head = ", ".join(names[:shown])
        return head + (f" +{len(names) - shown}" if len(names) > shown else "")

    # -- kept as JSON (D12) -------------------------------------------------------

    def to_dict(self) -> dict:
        chips = []
        for c in self.chips:
            item = {"kind": c.kind, "ref": c.ref, "label": c.label}
            if c.kind == LEAF:
                item.update(sys_id=c.sys_id, shelfmark=c.shelfmark)
            else:
                item.update(scope=c.scope, removed=sorted(c.removed))
            chips.append(item)
        return {"format": FORMAT, "version": VERSION, "chips": chips}

    @classmethod
    def from_dict(cls, data: Any) -> "Selection":
        """A kept selection; anything not of this format and version, and any broken chip, is left
        out (a stale value never breaks the picker)."""
        if not isinstance(data, dict) or data.get("format") != FORMAT or data.get("version") != VERSION:
            return cls()
        items = data.get("chips")
        if not isinstance(items, list):            # a number, a string, a mapping ... (Codex, PR #70)
            return cls()
        chips = []
        seen = set()
        for item in items:
            if not isinstance(item, dict):
                continue
            kind, ref = item.get("kind"), item.get("ref")
            label = item.get("label") if isinstance(item.get("label"), str) else ""
            if not isinstance(ref, str) or not ref:
                continue
            if kind == LEAF:
                sid = item.get("sys_id")
                shelfmark = item.get("shelfmark") if isinstance(item.get("shelfmark"), str) else ""
                if not is_corpus(sid):
                    continue
                chip = Chip(LEAF, ref, label, sid, shelfmark)
            elif kind == NODE:
                scope = item.get("scope") if item.get("scope") in (ALL, DIRECT) else ALL
                removed = item.get("removed") if isinstance(item.get("removed"), list) else []
                chip = Chip(NODE, ref, label, scope=scope, removed={s for s in removed if isinstance(s, str)})
            else:
                continue
            if chip.key not in seen:
                seen.add(chip.key)
                chips.append(chip)
        return cls(chips)

    def prune(self, service) -> Optional[int]:
        """Bring a kept selection up to the tree as it is now; answers how many chips were dropped,
        or None when the tree could not be read (nothing was looked at, nothing changed).
        A placement that is gone, or now holds another manuscript, goes; a shelfmark's writing is
        read again; a node that is gone, or is no longer one a reader may choose, goes; a node's
        unticked manuscripts are kept only where it still holds them, and a node left holding
        nothing goes. With the tree not served or not answering, nothing is dropped: a missing or
        unreadable file must not cost the reader a kept selection."""
        if service is None or not service.is_ready() or not service.children(None):
            return None
        keep = []
        for chip in self.chips:
            if chip.kind == LEAF:
                leaf = service.leaf(chip.ref)
                if leaf is None or leaf.sys_id != chip.sys_id:
                    continue
                chip.shelfmark = leaf.shelfmark
                chip.label = leaf.label or leaf.shelfmark or chip.sys_id
            else:
                node = service.node(chip.ref)
                if node is None or node.kind not in CHOOSABLE_KINDS:
                    continue
                chip.label = node.name_en
                members = service.members_under(node.id, chip.scope == DIRECT)
                chip.removed &= set(members)
                if not (members - chip.removed):
                    continue
            keep.append(chip)
        dropped = len(self.chips) - len(keep)
        self.chips = keep
        return dropped
