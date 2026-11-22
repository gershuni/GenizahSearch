# -*- coding: utf-8 -*-
"""The picker's multiple-choice state (shared/shelfmark_tree_selection.py) and the service calls it
stands on (``members_under``, ``leaves_under``), on the fixture tree of tests/shelfmark_tree_fixture.py.

Plan §10.13 (D11, D12), handoff DX3-DX5: a whole volume is one chip; unticking inside it lowers its
count; a manuscript is chosen and counted once however it is reached; a selection resolves to the
picker's contract in the order chosen and never to a LOCAL id; it is kept as versioned JSON and
survives a broken or stale value; a partial application keeps the rest.
"""
from __future__ import annotations

import json

import pytest

from shared.shelfmark_tree_selection import (
    ALL,
    DIRECT,
    FORMAT,
    VERSION,
    Chip,
    Selection,
    members_function,
    scope_for,
)
from shared.shelfmark_tree_service import ShelfmarkTreeService
from tests.shelfmark_tree_fixture import SYS, build_fixture


@pytest.fixture
def svc(tmp_path):
    db, _libraries, _meta = build_fixture(tmp_path)
    s = ShelfmarkTreeService(str(db))
    yield s
    s.close()


def _leaf(s, sys_id, node_id=None):
    places = s.path_of(sys_id)
    for p in places:
        if node_id is None or p.leaf.node_id == node_id:
            return p.leaf
    raise AssertionError(f"{sys_id} has no place in {node_id}")


# -- the service ---------------------------------------------------------------------------------

def test_leaves_under_a_volume_are_its_manuscripts_once_in_shelf_order(svc):
    leaves = svc.leaves_under("vol:100")
    assert [lf.sys_id for lf in leaves] == svc.sys_ids_under("vol:100")
    assert all(lf.node_id == "vol:100" or svc.node(lf.node_id).parent_id == "vol:100" for lf in leaves)
    assert svc.members_under("vol:100") == frozenset(svc.sys_ids_under("vol:100"))
    # a manuscript at two places answers the place inside the node asked about, not its first place
    first = svc.path_of(SYS["two_places"])[0].leaf
    other = "vol:100" if first.node_id != "vol:100" else "vol:110"
    two = next(lf for lf in svc.leaves_under(other) if lf.sys_id == SYS["two_places"])
    assert two.node_id == other and two.id != first.id
    assert svc.leaves_under("no such node") == [] and svc.members_under("no such node") == frozenset()


def test_direct_takes_only_a_nodes_own_shelfmarks(svc):
    # T-S AS (col:10) holds volumes and shelfmarks outside any volume
    node = svc.node("col:10")
    assert node.child_nodes and node.direct_leaves
    direct = svc.members_under("col:10", direct=True)
    assert direct == {lf.sys_id for lf in svc.leaves("col:10", 0, 1000)[0]}
    assert direct < svc.members_under("col:10")
    assert scope_for(node) == DIRECT and scope_for(svc.node("vol:100")) == ALL


def test_a_catalogue_list_keeps_its_places_and_a_mixed_node_leaves_them_out(tmp_path):
    from tests.test_shelfmark_tree import _with_catalogue
    db, _libraries, _meta = _with_catalogue(tmp_path)
    s = ShelfmarkTreeService(str(db))
    try:
        listed = next(c for c in s.children("col:11") if c.kind == "group")
        in_list = s.leaves_under(listed.id)
        assert in_list and all(lf.how == "catalogue" for lf in in_list)      # never emptied
        assert [lf.sys_id for lf in in_list] == s.sys_ids_under(listed.id)    # the list's own order
        mixed = s.leaves_under("col:11")
        assert mixed and all(lf.how != "catalogue" for lf in mixed)
        # choosing the list resolves to its placements, not to the manuscripts' places on the shelf
        sel = Selection()
        assert sel.add_node(listed, ALL, members_function(s))
        assert {e["placement_id"] for e in sel.resolve_entries(s)} == {lf.id for lf in in_list}
    finally:
        s.close()


# -- choosing ------------------------------------------------------------------------------------

def test_a_whole_volume_is_one_chip_and_unticking_inside_lowers_it(svc):
    m = members_function(svc)
    sel = Selection()
    vol = svc.node("vol:100")
    assert sel.add_node(vol, ALL, m)
    assert len(sel) == 1 and sel.chips[0].kind == "node"
    total = len(svc.members_under("vol:100"))
    assert sel.count(m) == sel.chip_count(sel.chips[0], m) == total
    assert sel.node_state(vol, ALL, m) == "all"
    assert sel.drop_leaf(SYS["as2"][0], m)
    assert len(sel) == 1 and sel.count(m) == total - 1
    assert sel.node_state(vol, ALL, m) == "some"
    # ticked again: back inside the same chip, no second chip
    assert sel.add_leaf(_leaf(svc, SYS["as2"][0]), m)
    assert len(sel) == 1 and sel.count(m) == total
    # the chip's x removes all of it
    assert sel.remove_chip(sel.chips[0].key) and sel.count(m) == 0


def test_a_node_folds_in_what_it_covers_and_a_node_inside_a_chosen_one_makes_no_chip(svc):
    m = members_function(svc)
    sel = Selection()
    assert sel.add_leaf(_leaf(svc, SYS["as2"][1]), m)
    assert sel.add_leaf(_leaf(svc, SYS["ns5"][0]), m)
    assert sel.add_node(svc.node("vol:100"), ALL, m)
    kinds = [(c.kind, c.ref) for c in sel.chips]
    assert ("node", "vol:100") in kinds and len(sel) == 2              # the T-S AS 2.2 chip folded in
    assert sel.add_node(svc.node("col:10"), ALL, m)                     # T-S AS folds its volume in
    assert [c.ref for c in sel.chips if c.kind == "node"] == ["col:10"]
    sel.drop_leaf(SYS["as2"][2], m)
    # the volume again, inside the chosen collection: its manuscripts are ticked there again
    assert sel.add_node(svc.node("vol:100"), ALL, m)
    assert [c.ref for c in sel.chips if c.kind == "node"] == ["col:10"]
    assert SYS["as2"][2] in sel.chosen_set(m)
    assert sel.add_leaf(_leaf(svc, SYS["as2"][3]), m) is False         # already chosen: nothing changes


def test_a_manuscript_in_two_chosen_volumes_is_counted_and_resolved_once(svc):
    m = members_function(svc)
    sel = Selection()
    sel.add_node(svc.node("vol:100"), ALL, m)      # T-S AS 2 holds two_places (T-S AS 2.3) ...
    sel.add_node(svc.node("vol:110"), ALL, m)      # ... and so does T-S NS 5 (T-S NS 5.3)
    union = svc.members_under("vol:100") | svc.members_under("vol:110")
    assert SYS["two_places"] in svc.members_under("vol:100") & svc.members_under("vol:110")
    assert sel.count(m) == len(union)
    entries = sel.resolve_entries(svc)
    assert len(entries) == len({e["sys_id"] for e in entries}) == len(union)
    # in the order chosen: the first volume's in shelf order, then the second's
    assert [e["sys_id"] for e in entries][:len(svc.members_under("vol:100"))] == svc.sys_ids_under("vol:100")
    # unticked once, it is unticked in both
    sel.drop_leaf(SYS["two_places"], m)
    assert SYS["two_places"] not in sel.chosen_set(m)
    assert SYS["two_places"] not in sel.sys_ids(svc)


def test_unticking_a_node_takes_it_out_of_a_chosen_collection(svc):
    m = members_function(svc)
    sel = Selection()
    sel.add_node(svc.node("col:10"), ALL, m)
    vol = svc.node("vol:100")
    assert sel.node_state(vol, ALL, m) == "all"
    assert sel.drop_node(vol, ALL, m)
    assert sel.node_state(vol, ALL, m) == "none"
    assert sel.count(m) == len(svc.members_under("col:10") - svc.members_under("vol:100"))


def test_a_chip_left_with_nothing_goes(svc):
    m = members_function(svc)
    sel = Selection()
    sel.add_node(svc.node("vol:101"), ALL, m)
    for sid in SYS["as10"]:
        sel.drop_leaf(sid, m)
    assert len(sel) == 0


def test_libraries_are_not_chosen_whole(svc):
    sel = Selection()
    assert not sel.add_node(svc.node("lib:1"), ALL, members_function(svc))
    assert len(sel) == 0


def test_consume_keeps_what_the_host_did_not_take(svc):
    m = members_function(svc)
    sel = Selection()
    sel.add_node(svc.node("vol:100"), ALL, m)
    sel.add_leaf(_leaf(svc, SYS["stras"][0]), m)
    ids = sel.sys_ids(svc)
    sel.consume(ids[:3], m)
    assert sel.sys_ids(svc) == ids[3:]
    sel.consume(ids[3:], m)
    assert len(sel) == 0


def test_never_a_local_id(svc):
    m = members_function(svc)
    sel = Selection()
    fake = _leaf(svc, SYS["as2"][0])
    from dataclasses import replace
    assert not sel.add_leaf(replace(fake, sys_id="970000000010205171"), m)
    data = {"format": FORMAT, "version": VERSION, "chips": [
        {"kind": "leaf", "ref": fake.id, "sys_id": "970000000010205171", "shelfmark": "x"}]}
    assert len(Selection.from_dict(data)) == 0
    assert all(s.startswith("99") for s in Selection(
        [Chip("leaf", fake.id, "x", "970000000010205171", "x")]).sys_ids(svc)) is True
    assert Selection([Chip("leaf", fake.id, "x", "970000000010205171", "x")]).sys_ids(svc) == []


# -- kept as JSON ----------------------------------------------------------------------------------

def test_a_selection_round_trips_through_json(svc):
    m = members_function(svc)
    sel = Selection()
    sel.add_node(svc.node("vol:100"), ALL, m)
    sel.drop_leaf(SYS["as2"][4], m)
    sel.add_leaf(_leaf(svc, SYS["stras"][1]), m)
    sel.add_node(svc.node("col:10"), DIRECT, m)
    back = Selection.from_dict(json.loads(json.dumps(sel.to_dict())))
    assert [c.key for c in back.chips] == [c.key for c in sel.chips]
    assert back.resolve_entries(svc) == sel.resolve_entries(svc)
    assert back.count(m) == sel.count(m)


@pytest.mark.parametrize("bad", [None, [], "x", {}, {"format": FORMAT, "version": VERSION + 1, "chips": []},
                                 {"format": "other", "version": VERSION},
                                 # chips that are not a list (Codex, PR #70): a TypeError until then
                                 {"format": FORMAT, "version": VERSION, "chips": 5},
                                 {"format": FORMAT, "version": VERSION, "chips": True},
                                 {"format": FORMAT, "version": VERSION, "chips": "node"},
                                 {"format": FORMAT, "version": VERSION, "chips": {"kind": "node", "ref": "vol:1"}},
                                 {"format": FORMAT, "version": VERSION, "chips": [1, None, {"kind": "node"},
                                                                                  {"kind": "x", "ref": "y"}]}])
def test_a_broken_or_stale_value_is_an_empty_selection(bad):
    assert len(Selection.from_dict(bad)) == 0


def test_prune_drops_what_the_tree_no_longer_has_and_nothing_when_it_is_not_served(svc, tmp_path):
    m = members_function(svc)
    sel = Selection()
    sel.add_node(svc.node("vol:100"), ALL, m)
    sel.chips.append(Chip("node", "vol:gone", "Gone"))
    sel.chips.append(Chip("leaf", "inv:0:none", "Gone", SYS["as2"][0], "T-S AS 2.1"))
    unserved = ShelfmarkTreeService(str(tmp_path / "missing.db"))
    assert sel.prune(unserved) is None and len(sel) == 3
    assert sel.prune(svc) == 2 and [c.ref for c in sel.chips] == ["vol:100"]


def test_summary_names_the_first_chips():
    sel = Selection([Chip("node", f"vol:{i}", f"V {i}") for i in range(5)])
    assert sel.summary() == "V 0, V 1, V 2 +2"
    assert sel.summary(lambda c: c.label.lower(), shown=5) == "v 0, v 1, v 2, v 3, v 4"
