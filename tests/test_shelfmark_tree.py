# -*- coding: utf-8 -*-
"""The shelf tree: builder (matching rules O1-O5), verifier (must be able to fail) and service.

Plan: docs/plans/SHELFMARK_TREE_PICKER_PLAN.md (private repository), §3.1-§3.2, §10.
Runs on the small fixture of tests/shelfmark_tree_fixture.py, never on the real FGP dump.
"""
from __future__ import annotations

import shutil
import sqlite3

import pytest

from scripts import build_shelfmark_tree as builder
from scripts import verify_shelfmark_tree_sidecar as verifier
from shared import shelfmark_tree_service as service_module
from shared.shelfmark_tree_service import ShelfmarkTreeService
from tests.shelfmark_tree_fixture import CANARIES, SYS, build_fixture, make_fist, make_libraries


@pytest.fixture
def built(tmp_path):
    db, libraries, meta = build_fixture(tmp_path)
    return db, libraries, meta


@pytest.fixture
def svc(built):
    s = ShelfmarkTreeService(str(built[0]))
    yield s
    s.close()


def _places(svc, key):
    return [(p.leaf.how, [n.name_en for n in p.path]) for p in svc.path_of(SYS[key])]


# -- builder ---------------------------------------------------------------------------------------

def test_schema_versions_agree():
    assert service_module.SCHEMA_VERSION == builder.SCHEMA_VERSION


def test_meta_counts(built):
    _db, _libraries, meta = built
    assert meta["manuscripts_total"] == "49"
    assert meta["leaves_total"] == "50"          # the manuscript at two places
    assert {k: meta[f"placed_{k}"] for k in builder.HOW} == {
        "fist": "39", "exact": "1", "collection": "3", "single": "1", "name": "0", "other": "5"}


def test_fgp_placements(svc):
    assert _places(svc, "inactive_volume") == [("fist", ["Cambridge University Library", "T-S AS"])]
    assert sorted(_places(svc, "two_places")) == [
        ("fist", ["Cambridge University Library", "T-S AS", "T-S AS 2"]),
        ("fist", ["Cambridge University Library", "T-S NS", "T-S NS 5"]),
    ]
    shared = svc.leaves(svc.locate("T-S AS 2").nodes[0].id, 0, 50)[0]
    assert sum(1 for leaf in shared if leaf.inventory_id == 1004) == 2   # one row, two manuscripts


def test_exact_step_sits_beside_its_row(svc):
    assert _places(svc, "exact") == [("exact", ["Cambridge University Library", "T-S AS", "T-S AS 2"])]
    labels = [leaf.shelfmark for leaf in svc.leaves(svc.locate("T-S AS 2").nodes[0].id, 0, 50)[0]]
    # after FGP's last row 2.8; only "T-S AS 2.99", placed into the volume by its name, comes after
    assert labels[-2:] == ["T-S AS 2.9", "T-S AS 2.99"]


def test_number_boundaries_are_kept(svc):
    # T-S AS 29 is not T-S AS 2.9: the collection head places it, outside any volume (O4)
    assert _places(svc, "boundary") == [("collection", ["Cambridge University Library", "T-S AS"])]


def test_collection_head(svc):
    assert _places(svc, "by_head") == [("collection", ["Cambridge University Library", "Or."])]
    # placed at its collection, then in the volume its name leads with (plan §10.15)
    assert _places(svc, "inactive_row") == [("collection", ["Cambridge University Library", "T-S AS", "T-S AS 2"])]


def test_another_institution_goes_to_other(svc):
    assert _places(svc, "foreign") == [("other", ["Cambridge University Library", "Other shelfmarks"])]


def test_collision_goes_to_other(svc):
    assert _places(svc, "collision") == [("other", ["Cambridge University Library", "Other shelfmarks"])]


def test_single_collection_but_not_for_a_collector(svc):
    assert _places(svc, "single") == [("single", ["National and University Library of Strasbourg", "Strasbourg"])]
    assert _places(svc, "collector") == [("other", ["Vernadsky National Library", "Other shelfmarks"])]


def test_every_record_is_reachable(svc):
    assert _places(svc, "no_code") == [("other", [builder.NO_CODE_NAME, "Other shelfmarks"])]
    assert _places(svc, "no_fist")[0][0] == "other"
    assert _places(svc, "no_fist")[0][1][1] == "Other shelfmarks"


def test_oxford_part_is_information_not_a_level(svc):
    (place,) = svc.path_of(SYS["oxford_part"])
    assert place.leaf.part == "MS. Heb. d. 29/2"
    assert [n.kind for n in place.path] == ["library", "collection"]
    leaves, total = svc.leaves(place.leaf.node_id, 0, 10, filter="d. 29/2")
    assert total == 1 and leaves[0].sys_id == SYS["oxford_part"]


def test_order(svc):
    cul = next(n for n in svc.children() if n.short == "CUL")
    assert [c.name_en for c in svc.children(cul.id)] == ["T-S NS", "T-S AS", "Or.", "Other shelfmarks"]
    assert [c.name_en for c in svc.children("col:10")] == ["T-S AS 2", "T-S AS 10"]


def test_cancelled_and_undefined_are_not_places(svc):
    assert all(n.short != "Gone" for n in svc.children())
    assert svc.node("col:99") is None and svc.node("col:13") is None


def test_a_null_cancellation_flag_is_active(tmp_path):
    # PR review: FIST exports treat IsCanceledCode NULL as active (scripts/export_fist_enrichment.py)
    fist = make_fist(tmp_path / "FIST.db")
    con = sqlite3.connect(str(fist))
    con.execute("UPDATE CODE_Library SET IsCanceledCode = NULL WHERE LibraryId = 2")
    con.execute("UPDATE CODE_Collection SET IsCanceledCode = NULL WHERE CollectionId = 20")
    con.commit()
    con.close()
    libraries = make_libraries(tmp_path / "libraries.csv")
    out = tmp_path / "out.db"
    builder.build(fist, libraries, out, log=lambda *_: None)
    s = ShelfmarkTreeService(str(out))
    try:
        assert [p.leaf.how for p in s.path_of(SYS["stras"][0])] == ["fist"]
        assert [p.leaf.node_id for p in s.path_of(SYS["single"])] == ["col:20"]
    finally:
        s.close()


def test_moved_rows_stop_the_build(tmp_path):
    fist = make_fist(tmp_path / "FIST.db", moved=True)
    libraries = make_libraries(tmp_path / "libraries.csv")
    with pytest.raises(builder.BuildError, match="MoveToInventoryId"):
        builder.build(fist, libraries, tmp_path / "out.db", log=lambda *_: None)


def test_an_input_changed_during_the_build_stops_it(tmp_path, monkeypatch):
    # PR review: an input edited between its read and the fingerprint must not be published
    fist = make_fist(tmp_path / "FIST.db")
    libraries = make_libraries(tmp_path / "libraries.csv")
    real = builder.build_tree

    def edit_during(*args, **kwargs):
        result = real(*args, **kwargs)
        libraries.write_text(libraries.read_text(encoding="utf-8").replace("Or. 77", "Or. 777"),
                             encoding="utf-8", newline="\n")
        return result

    monkeypatch.setattr(builder, "build_tree", edit_during)
    out = tmp_path / "out.db"
    with pytest.raises(builder.BuildError, match="libraries.csv changed"):
        builder.build(fist, libraries, out, log=lambda *_: None)
    assert not out.exists() and not (tmp_path / "out.db.tmp").exists()


def test_refuses_to_shrink_without_force(tmp_path, built):
    db, libraries, _meta = built
    con = sqlite3.connect(str(db))
    con.execute("UPDATE meta SET value = '9999' WHERE key = 'manuscripts_total'")
    con.commit()
    con.close()
    with pytest.raises(builder.BuildError, match="--force"):
        builder.build(tmp_path / "FIST.db", libraries, db, log=lambda *_: None)
    builder.build(tmp_path / "FIST.db", libraries, db, force=True, log=lambda *_: None)


def test_ranges(tmp_path, monkeypatch):
    monkeypatch.setattr(builder, "RANGE_MAX", 4)
    monkeypatch.setattr(builder, "RANGE_SIZE", 3)
    monkeypatch.setattr(builder, "RANGE_TARGET", 3)
    monkeypatch.setattr(builder, "RANGE_WIDTHS", (2, 4))
    monkeypatch.setattr(verifier, "RANGE_MAX", 4)
    monkeypatch.setattr(verifier, "RANGE_SIZE", 3)
    db, libraries, meta = build_fixture(tmp_path)
    s = ShelfmarkTreeService(str(db))
    try:
        vol = s.locate("T-S AS 2").nodes[0]
        ranges = s.children(vol.id)
        assert {r.kind for r in ranges} == {"range"} and vol.direct_leaves == 0
        assert all(r.direct_leaves <= 3 for r in ranges) and sum(r.direct_leaves for r in ranges) == 12
        # round item numbers; the two rows 2.3 and 2.4 hold four shelfmarks: divided again, by count,
        # and named by their first and last shelfmark, the last without what both begin with
        assert [r.name_en for r in ranges] == [
            "T-S AS 2.1 - 2", "T-S AS 2.3 – 4", "T-S AS 2.4 [2]", "T-S AS 2.5 - 6",
            "T-S AS 2.7 - 8", "T-S AS 2.9 - 12", "T-S AS 2.97 - 100"]
        last = s.leaves(ranges[0].id, 1, 1)[0][0]
        nxt = s.neighbours(last.id)[1]
        assert nxt.node_id == ranges[1].id                     # previous / next cross ranges
        # a position counts the whole list the ranges divide, not the range alone
        from shared.shelfmark_tree_view import Position, position
        assert position(s, nxt) == Position(3, 12)
    finally:
        s.close()
    assert verifier.verify(str(db), int(meta["leaves_total"]), libraries, canaries=None) == []


def test_libraries_by_size(tmp_path, monkeypatch):
    monkeypatch.setattr(builder, "TOP_LIBRARY_MIN", 7)
    db, _libraries, _meta = build_fixture(tmp_path)
    s = ShelfmarkTreeService(str(db))
    try:
        libs = s.children()
    finally:
        s.close()
    assert [n.short for n in libs[:3]] == ["CUL", "Kiev", "Strasbourg"]   # 7 or more, largest first (a tie A-Z)
    names = [n.name_en for n in libs[3:]]
    assert names == sorted(names, key=str.casefold)                     # then A-Z by the name shown


# -- verifier --------------------------------------------------------------------------------------

def test_verifier_passes_the_fixture(built):
    db, libraries, meta = built
    assert verifier.verify(str(db), int(meta["leaves_total"]), libraries, canaries=CANARIES) == []


def _mutated(built, tmp_path, sql):
    db, libraries, meta = built
    copy = tmp_path / "mutated.db"
    shutil.copy(db, copy)
    con = sqlite3.connect(str(copy))
    con.executescript(sql)
    con.commit()
    con.close()
    return verifier.verify(str(copy), int(meta["leaves_total"]), libraries, canaries=CANARIES)


@pytest.mark.parametrize("name, sql, expect", [
    ("wrong count", "", "expected"),
    ("a manuscript dropped",
     f"DELETE FROM leaf WHERE sys_id = '{SYS['by_head']}'; "
     "UPDATE meta SET value = CAST(value AS INTEGER) - 1 WHERE key IN ('leaves_total', 'manuscripts_total')",
     "have no place"),
    ("collections reversed", "UPDATE node SET sort_key = 2 - sort_key WHERE id IN ('col:11', 'col:12')",
     "node(s)"),
    ("positions broken", "UPDATE leaf SET pos = -pos - 1 WHERE node_id = 'vol:100'", "positions"),
    ("a collection dropped", "DELETE FROM node WHERE id = 'col:12'", "missing"),
    ("wrong schema version", "UPDATE meta SET value = '0.9' WHERE key = 'version'", "meta.version"),
    ("a placed leaf in Other", f"UPDATE leaf SET how = 'collection' WHERE sys_id = '{SYS['foreign']}'",
     "Other shelfmarks"),
    # Codex review of PR 1: a range under Other, a parent cycle, the libraries' own order
    ("a placed leaf in a range under Other",
     "INSERT INTO node SELECT 'rng:other:lib:1:1', id, 'range', level + 1, 'r', '', '', 0, 0, lo, hi, "
     "manuscripts, direct_leaves, 0 FROM node WHERE id = 'other:lib:1'; "
     "UPDATE node SET direct_leaves = 0, child_nodes = 1 WHERE id = 'other:lib:1'; "
     "UPDATE leaf SET node_id = 'rng:other:lib:1:1' WHERE node_id = 'other:lib:1'; "
     f"UPDATE leaf SET how = 'collection' WHERE sys_id = '{SYS['foreign']}'",
     "Other shelfmarks"),
    ("a parent cycle", "UPDATE node SET parent_id = 'col:10' WHERE id = 'lib:1'", "ancestry loops"),
    ("libraries reordered", "UPDATE node SET sort_key = 0 WHERE parent_id IS NULL", "libraries"),
    ("an alias on a volume", "INSERT INTO node_alias VALUES ('vol:100', 'x')", "alias"),
    # PR review: the auxiliary tables' layouts are checked too
    ("library_code with one column",
     "DROP TABLE library_code; CREATE TABLE library_code (code TEXT)", "library_code columns"),
    ("node_alias with other columns",
     "DROP TABLE node_alias; CREATE TABLE node_alias (alias TEXT, node_id TEXT)", "node_alias columns"),
    ("a code tied to a collection", "INSERT INTO library_code VALUES ('X', 'col:10')", "library_code row"),
])
def test_verifier_can_fail(built, tmp_path, name, sql, expect):
    db, libraries, meta = built
    if name == "wrong count":
        problems = verifier.verify(str(db), int(meta["leaves_total"]) + 1, libraries, canaries=CANARIES)
    else:
        problems = _mutated(built, tmp_path, sql)
    assert problems, name
    assert any(expect in p for p in problems), problems


def test_verifier_canary_catches_a_reordered_volume(built, tmp_path):
    db, libraries, meta = built
    copy = tmp_path / "swapped.db"
    shutil.copy(db, copy)
    con = sqlite3.connect(str(copy))
    first, second = con.execute("SELECT id, pos FROM leaf WHERE node_id = 'vol:100' ORDER BY pos LIMIT 2").fetchall()
    con.execute("UPDATE leaf SET pos = -1 WHERE id = ?", (first[0],))
    con.execute("UPDATE leaf SET pos = ? WHERE id = ?", (first[1], second[0]))
    con.execute("UPDATE leaf SET pos = ? WHERE id = ?", (second[1], first[0]))
    con.commit()
    con.close()
    problems = verifier.verify(str(copy), int(meta["leaves_total"]), libraries, canaries=CANARIES)
    assert any("canary: T-S AS 2 starts" in p for p in problems), problems


def test_verifier_fails_after_libraries_csv_changes_a_shelfmark(built):
    # PR review: same rows, one shelfmark changed -> the tree is stale and must not pass
    db, libraries, meta = built
    text = libraries.read_text(encoding="utf-8").replace("Or. 77", "Or. 777")
    libraries.write_text(text, encoding="utf-8", newline="\n")
    problems = verifier.verify(str(db), int(meta["leaves_total"]), libraries, canaries=CANARIES)
    assert any("another libraries.csv" in p for p in problems), problems


def test_verifier_checks_the_fgp_dump_when_given(built, tmp_path):
    # PR review: a refreshed dump with the old tree must fail where the dump is at hand
    db, libraries, meta = built
    fist = tmp_path / "FIST.db"
    assert verifier.verify(str(db), int(meta["leaves_total"]), libraries, canaries=CANARIES, fist=fist) == []
    con = sqlite3.connect(str(fist))
    con.execute("UPDATE dbo_Inventory SET Shelfmark = 'T-S AS 2.10' WHERE InventoryId = 1009")
    con.commit()
    con.close()
    problems = verifier.verify(str(db), int(meta["leaves_total"]), libraries, canaries=CANARIES, fist=fist)
    assert any("another FGP dump" in p for p in problems), problems
    missing = verifier.verify(str(db), int(meta["leaves_total"]), libraries, canaries=CANARIES,
                              fist=tmp_path / "none.db")
    assert any("no FGP dump" in p for p in missing), missing


def test_verifier_cli_fails_on_a_missing_file(tmp_path, capsys):
    assert verifier.main(["--db", str(tmp_path / "none.db"), "--expect-leaves", "1"]) == 1
    assert "RESULT: FAIL" in capsys.readouterr().out


# -- service ---------------------------------------------------------------------------------------

def test_service_is_closed_on_a_missing_or_bad_file(tmp_path):
    assert not ShelfmarkTreeService(str(tmp_path / "none.db")).is_ready()
    bad = tmp_path / "bad.db"
    bad.write_bytes(b"not a database at all")
    s = ShelfmarkTreeService(str(bad))
    assert not s.is_ready() and s.unavailable_reason
    assert s.children() == [] and s.path_of(SYS["exact"]) == [] and s.neighbours("x") == (None, None)


def test_service_refuses_another_schema_or_count(built, tmp_path):
    for sql in ("UPDATE meta SET value = '2.0.0' WHERE key = 'version'",
                "UPDATE meta SET value = '1' WHERE key = 'leaves_total'"):
        copy = tmp_path / "copy.db"
        shutil.copy(built[0], copy)
        con = sqlite3.connect(str(copy))
        con.execute(sql)
        con.commit()
        con.close()
        s = ShelfmarkTreeService(str(copy))
        assert not s.is_ready()
        s.close()


def test_leaves_paging_and_filter(svc):
    vol = svc.locate("T-S AS 2").nodes[0]
    page, total = svc.leaves(vol.id, 2, 3)
    # the manuscript at two places shows its own shelfmark; FGP's is the shelf label
    assert total == vol.direct_leaves
    assert [leaf.shelf_label for leaf in page] == ["T-S AS 2.3", "T-S AS 2.3", "T-S AS 2.4"]
    assert page[1].shelfmark == "T-S NS 5.3"
    assert svc.leaves(vol.id, 0, 10, filter="2.9")[1] == 2           # 2.9 and 2.99
    assert svc.leaves(vol.id, 0, 10, filter="%")[1] == 0          # LIKE wildcards are literal


def test_neighbours_cross_volumes_and_stop_at_the_library(svc):
    first = svc.leaves(svc.locate("T-S AS 10").nodes[0].id, 0, 1)[0][0]
    prev, nxt = svc.neighbours(first.id)
    assert prev.shelfmark == "T-S AS 2.99" and nxt.shelfmark == "T-S AS 10.2"
    stras = next(n for n in svc.children() if n.short == "Strasbourg")
    first_stras = svc.leaves(svc.children(stras.id)[0].id, 0, 1)[0][0]
    assert svc.neighbours(first_stras.id)[0] is None


def test_neighbours_pass_over_the_same_manuscript(svc):
    rows = svc.leaves(svc.locate("T-S AS 2").nodes[0].id, 0, 50)[0]
    shared = [leaf for leaf in rows if leaf.inventory_id == 1004]
    prev, nxt = svc.neighbours(shared[0].id)
    assert prev.shelf_label == "T-S AS 2.3" and nxt.sys_id != shared[0].sys_id
    two = [leaf for leaf in rows if leaf.sys_id == SYS["two_places"]]
    before, after = svc.neighbours(two[0].id)
    # row 2.3 carries two manuscripts: the other one comes first (by placement id)
    assert (before.shelf_label, before.sys_id) == ("T-S AS 2.3", SYS["as2"][2])
    assert after.shelf_label == "T-S AS 2.4"


def test_sys_ids_under_counts_manuscripts_once(svc):
    cul = next(n for n in svc.children() if n.short == "CUL")
    under = svc.sys_ids_under(cul.id)
    assert len(under) == len(set(under)) == cul.manuscripts
    assert SYS["two_places"] in under


def test_locate(svc):
    found = svc.locate("T-S AS 2.7")
    assert [n.name_en for n in found.nodes] == ["T-S AS 2"] and not found.ambiguous
    assert [n.name_en for n in svc.locate("T-S AS 29").nodes] == ["T-S AS"]   # not the volume T-S AS 2
    assert svc.locate("T-S AS 2", library="Strasbourg").nodes == ()
    assert [n.name_en for n in svc.locate("T-S AS 2", library="CUL").nodes] == ["T-S AS 2"]
    assert svc.locate("").nodes == ()


def test_locate_keeps_other_libraries_with_an_equally_long_name(tmp_path):
    # Codex review of PR 1: a deeper node in one library must not hide another library's
    # collection of the same name (CUL's collection Add. and a Westminster volume Add.).
    db, _libraries, _meta = build_fixture(tmp_path)
    con = sqlite3.connect(str(db))
    con.execute("UPDATE node SET name_en = 'Or.' WHERE id = 'vol:110'")    # a volume named like CUL's Or.
    con.commit()
    con.close()
    s = ShelfmarkTreeService(str(db))
    try:
        # same library: the deeper wins, nothing ambiguous
        found = s.locate("Or. 5")
        assert {n.id for n in found.nodes} == {"vol:110"} and not found.ambiguous
    finally:
        s.close()
    con = sqlite3.connect(str(db))
    con.execute("UPDATE node SET name_en = 'Ms.' WHERE id = 'col:12'")       # CUL's collection
    con.execute("UPDATE node SET name_en = 'Ms.' WHERE id = 'col:20'")       # Strasbourg's, the same name
    con.execute("UPDATE node SET name_en = 'Ms. 2' WHERE id = 'vol:110'")
    con.commit()
    con.close()
    s = ShelfmarkTreeService(str(db))
    try:
        found = s.locate("Ms. 2.7")
        assert [n.id for n in found.nodes] == ["vol:110"]                    # longer name wins across libraries
        found = s.locate("Ms. 9")
        assert {n.id for n in found.nodes} == {"col:12", "col:20"} and found.ambiguous
        assert [n.id for n in s.locate("Ms. 9", library="Strasbourg").nodes] == ["col:20"]
    finally:
        s.close()


def test_locate_by_a_collections_shelfmark_heads(tmp_path):
    # PR review: CUL's collection "Oriental" holds "Or. ..." shelfmarks; locate must find it.
    fist = make_fist(tmp_path / "FIST.db")
    con = sqlite3.connect(str(fist))
    con.execute("UPDATE CODE_Collection SET CollectionName = 'Oriental' WHERE CollectionId = 12")
    con.commit()
    con.close()
    libraries = make_libraries(tmp_path / "libraries.csv")
    out = tmp_path / "out.db"
    builder.build(fist, libraries, out, log=lambda *_: None)
    s = ShelfmarkTreeService(str(out))
    try:
        assert [n.id for n in s.locate("Or. 1324", library="CUL").nodes] == ["col:12"]
        assert [n.id for n in s.locate("Or. 1324").nodes] == ["col:12"]
        assert [n.id for n in s.locate("T-S AS 2.7").nodes] == ["vol:100"]   # a volume still wins
    finally:
        s.close()
    con = sqlite3.connect(str(out))
    aliases = con.execute("SELECT node_id, alias FROM node_alias").fetchall()
    con.close()
    assert ("col:12", "or") in aliases
    assert not any(nid == "col:10" and a == "t s as" for nid, a in aliases)   # its own name is not repeated


def test_path_of_is_in_shelf_order(svc):
    places = svc.path_of(SYS["two_places"])
    assert [p.leaf.pos for p in places] == sorted(p.leaf.pos for p in places)


def test_selection_contract(svc):
    (place,) = svc.path_of(SYS["exact"])
    assert place.leaf.as_selection() == {"sys_id": SYS["exact"], "shelfmark": "T-S AS 2.9",
                                         "placement_id": f"sys:{SYS['exact']}"}


# -- the arrangement (owner, 2026-11-19): names, merges, labels, groups -----------------------------

def test_clean_label_prefers_a_plain_call_number():
    assert builder.clean_label(["The National Library of Russia Box D.7", "Box_k_f_186r_color.tif"]) == "Box D.7"
    assert builder.clean_label(["Gaster, Moses Collection Ms. 391", "Ms. 391", "Or. 10112D",
                                "The British Library Or. 10112D"]) == "Or. 10112D"
    assert builder.clean_label(["Cambridge University Library, Ms. T-S AS 39.7", "T-S AS 39.7"]) == "T-S AS 39.7"
    assert builder.clean_label(["more", "Ms. 63"]) == "Ms. 63"


def test_a_library_shown_inside_another_keeps_a_shortcut(tmp_path, monkeypatch):
    monkeypatch.setattr(builder, "MERGE_INTO", {2: (1, "Strasbourg (inside)", "שטרסבורג בפנים")})
    db, _libraries, meta = build_fixture(tmp_path)
    s = ShelfmarkTreeService(str(db))
    try:
        assert s.node("lib:2") is None
        col = s.node("col:20")
        assert col.parent_id == "lib:1" and col.name_en == "Strasbourg (inside)"
        assert [c.name_en for c in s.children("lib:1")][-2:] == ["Strasbourg (inside)", "Other shelfmarks"]
        ((sid, _en, _he, short, target),) = s.shortcuts()
        assert (sid, short, target.id) == ("link:2", "Strasbourg", "col:20")
        # the matching rules still run on FGP's own libraries: the single-collection step holds
        assert [p.leaf.how for p in s.path_of(SYS["single"])] == ["single"]
    finally:
        s.close()
    assert verifier.verify(str(db), int(meta["leaves_total"]), _libraries, canaries=None) == []


def test_volumes_grouped_by_fgp_ranges_prefix_and_number(tmp_path, monkeypatch):
    monkeypatch.setattr(builder, "PREFIX_GROUP_MIN", 1)
    monkeypatch.setattr(builder, "FGP_RANGES", {10: (("T-S AS 1-5", 1, 5),)})
    db, libraries, meta = build_fixture(tmp_path)
    s = ShelfmarkTreeService(str(db))
    try:
        groups = [(c.kind, c.name_en) for c in s.children("col:10")]
        assert ("group", "T-S AS 1-5") in groups and ("group", "T-S AS (other volumes)") in groups
        vol = s.locate("T-S AS 2").nodes[0]
        assert s.node(vol.parent_id).name_en == "T-S AS 1-5"
        # a reader who knows the shelfmark: found below the groups
        assert [n.name_en for n in s.search_nodes("col:10", "AS 10")] == ["T-S AS 10"]
        # a filter reaches every kind a column shows: "Other shelfmarks" too (PR review)
        assert "Other shelfmarks" in [n.name_en for n in s.search_nodes("lib:1", "Other")]
        leaves, total = s.search_leaves("col:10", "2.4")
        # two manuscripts on one row: the second is marked
        assert total == 2 and {lf.label for lf in leaves} == {"T-S AS 2.4", "T-S AS 2.4 [2]"}
    finally:
        s.close()
    assert verifier.verify(str(db), int(meta["leaves_total"]), libraries, canaries=None) == []


def test_a_filtered_column_offers_the_matches_past_its_first_page(tmp_path, monkeypatch):
    from web.components import shelfmark_tree_picker as picker
    monkeypatch.setattr(picker, "NODE_ROWS", 1)
    monkeypatch.setattr(picker, "MAX_PAGE", 1)             # pages are asked for one by one
    db, _libraries, _meta = build_fixture(tmp_path)
    s = ShelfmarkTreeService(str(db))
    try:
        path = (s.node("col:10").parent_id, "col:10")
        labels = {"library": "Library", "others": "Others", "in": "in {place}"}
        nodes = s.count_nodes("col:10", "T-S AS")
        leaves = s.search_leaves("col:10", "T-S AS 2", limit=0)[1]
        assert nodes > 1 and s.search_nodes("col:10", "T-S AS", limit=1, offset=1)[0].name_en

        def column(text, shown):
            view = picker._load_view(s, path, {"n:col:10": text}, shown, "en", labels)
            return next(c for c in view["columns"] if c["parent"] == "col:10")

        first = column("T-S AS", {})
        assert len(first["rows"]) == 1 and first["more"] == nodes - 1
        # a match opens at its own place: the path holds each level once
        row = first["rows"][0]
        assert row["go"] == tuple(a.id for a in s.ancestors(row["id"])) + picker._passed_through(s, row["id"])[1:]
        assert len(column("T-S AS", {"n:col:10": nodes})["rows"]) == nodes
        with_number = column("T-S AS 2", {})
        assert with_number["more"] == (s.count_nodes("col:10", "T-S AS 2") - 1) + (leaves - 1)
        everything = column("T-S AS 2", {"n:col:10": max(nodes, leaves)})
        assert everything["more"] == 0 and sum("leaf" in r for r in everything["rows"]) == leaves
    finally:
        s.close()


def test_other_shelfmarks_divided_by_prefix(tmp_path, monkeypatch):
    monkeypatch.setattr(builder, "GROUP_MIN", 2)
    db, libraries, meta = build_fixture(tmp_path)
    s = ShelfmarkTreeService(str(db))
    try:
        kids = s.children("other:lib:1")
        assert {k.kind for k in kids} == {"group"} and len(kids) == 2
        assert sorted(k.name_en for k in kids) == ["Or.", "T-S X"]
        # a series of "Other shelfmarks" is no place a typed shelfmark opens: those are what nothing placed
        assert all(n.kind != "group" for n in s.locate("T-S X 1").nodes + s.locate("Or. 77").nodes)
    finally:
        s.close()
    assert verifier.verify(str(db), int(meta["leaves_total"]), libraries, canaries=None) == []


def test_volume_names_keep_their_shelfmarks_punctuation():
    # Owner, 2026-11-19: FGP writes AIU "IA", "IVB"; the shelfmarks are "I.A.1", "IV.B.3".
    assert builder.volume_display_name("IA", ["I.A.1", "I.A.2"]) == "I.A"
    assert builder.volume_display_name("IVB", ["IV.B.1", "IV.B.17"]) == "IV.B"
    assert builder.volume_display_name("Glass", ["L-G Glass.2", "L-G Glass.3"]) == "Glass"   # differs: kept
    assert builder.volume_display_name("T-S AS 39", ["T-S AS 39.1", "T-S AS 39.2"]) == "T-S AS 39"


def test_a_record_naming_another_holding_library_is_shown_there(tmp_path, monkeypatch):
    # The Vernadsky records coded CUL are Kyiv's: shown in Kyiv's one collection, not CUL's; a
    # record its long form gives to the library is written as that library writes it (2026-11-20:
    # a Harkavy record that is the NLI's "Ms. Heb. 1151.3=4")
    monkeypatch.setattr(builder, "INSTITUTION_LIBRARY", {"vernadsky national library of ukraine": 3})
    fist = make_fist(tmp_path / "FIST.db")
    libraries = make_libraries(tmp_path / "libraries.csv")
    renamed = "990000990010205171"
    with open(libraries, "a", encoding="utf-8", newline="\n") as f:
        f.write(f'{renamed},,"Harkavy Collection, Ms. P 15|Ms. P 15|'
                f'Vernadsky National Library of Ukraine, Ms. Or. 99",Harkavy,,,,\n')
    out = tmp_path / "shelfmark_tree.db"
    meta = builder.build(fist, libraries, out, log=lambda *_: None)
    s = ShelfmarkTreeService(str(out))
    try:
        (place,) = s.path_of(SYS["foreign"])
        assert (place.leaf.how, place.leaf.label, [n.id for n in place.path]) == ("single", "Or. 78", ["lib:3", "col:30"])
        (place,) = s.path_of(renamed)
        assert (place.leaf.label, [n.id for n in place.path]) == ("Ms. Or. 99", ["lib:3", "col:30"])
    finally:
        s.close()
    assert verifier.verify(str(out), int(meta["leaves_total"]), libraries, canaries=None) == []
    # the gate's canary for such a record: in that library, and only there
    assert verifier.canary_problems(str(out), {"in_library": ((SYS["foreign"], "Kiev"),)}) == []
    assert verifier.canary_problems(str(out), {"in_library": ((SYS["foreign"], "CUL"),)}) != []


def test_a_merged_librarys_codes_answer_for_the_library_it_joins(tmp_path, monkeypatch):
    # PR review: library_code rows of a merged library must point at the library it joins.
    monkeypatch.setattr(builder, "MERGE_INTO", {2: (1, "Strasbourg (inside)", "שטרסבורג בפנים")})
    db, _libraries, _meta = build_fixture(tmp_path)
    s = ShelfmarkTreeService(str(db))
    try:
        assert s.libraries_of_code("Strasbourg") == ["lib:1"]
        # scoped to the code, a typed shelfmark finds the collection now shown inside CUL (by its prefix)
        assert [n.id for n in s.locate("Ms. 3", library="Strasbourg").nodes] == ["col:20"]
        assert [n.id for n in s.locate("Ms. 3", library="Harkavy").nodes] == []
    finally:
        s.close()


# -- coherence rules (owner's review, 2026-11-20; plan §10.15) ------------------------------------

def test_roman_numerals_sort_as_numbers_and_letters_as_letters():
    mosseri = ["Moss. IV", "Moss. I", "Moss. IX", "Moss. Ia", "Moss. IXa", "Moss. II", "Moss. X", "Moss. V"]
    keys = builder.sibling_order_keys(mosseri)
    assert sorted(mosseri, key=keys.get) == [
        "Moss. I", "Moss. Ia", "Moss. II", "Moss. IV", "Moss. V", "Moss. IX", "Moss. IXa", "Moss. X"]
    series = ["2nd Series: V", "2nd Series: A", "2nd Series: T", "2nd Series: C"]
    keys = builder.sibling_order_keys(series)
    assert sorted(series, key=keys.get) == ["2nd Series: A", "2nd Series: C", "2nd Series: T", "2nd Series: V"]
    # a letter glued to a number is a suffix: Or. 5557H before Or. 5557I
    assert builder.roman_natural_key("Or. 5557H") < builder.roman_natural_key("Or. 5557I")


def test_a_shelfmark_matches_the_name_it_begins_with():
    def m(name, label):
        return builder.name_matches(builder._match_tokens(name), builder._match_tokens(label))
    assert m("Talmudica II", "Ms. Talm. II 15") == 2       # an abbreviation of three letters or more
    assert m("I.A", "Ms. I A 1") == 2                      # "Ms." and punctuation are not compared
    assert m("Moss. IV", "Moss. IV,366.2") == 2
    assert m("Moss. I", "Moss. Ia,1.3") == 0               # Ia is not I
    assert m("Or.", "Ms. O 5") == 0                        # too short to be an abbreviation


def test_forms_stems_and_spellings():
    assert builder.form_of("Yevr. III B 1003") == "Yevr. III B"
    assert builder.stem_of("Box D.7") == "Box D" and builder.stem_of("MS heb. c.9/1") == "MS heb. c.9"
    assert builder.stem_of("Or. 10126") == ""               # a space after the dot: one part
    assert builder._written_as("OR 10578R", "Or. 10578R.1") == "Or. 10578R"
    assert builder._written_as("OR 1057", "Or. 10578") is None
    assert builder._written_as("T-S F2(1)", "T-S F2(1).1") == "T-S F2(1)"     # its ending is kept
    assert builder._written_as("4077/", "4077/1") == "4077/"


def _leaf(sid, label, how="other", shelf="", shelfmark=None):
    return builder.Leaf(f"x:{sid}", sid, shelfmark or label, shelf, None, how, "", label)


def test_cohere_places_rewrites_and_holds_back(monkeypatch):
    monkeypatch.setattr(builder, "SIGLA", {("lon", "bl"): 2})
    N = builder.Node
    nodes = {
        "lib:1": N("lib:1", "library", None, "L", children=["col:1", "col:2", "other:lib:1"]),
        "lib:2": N("lib:2", "library", None, "BL", children=["col:3"]),
        "col:1": N("col:1", "collection", "lib:1", "Mosseri", children=["vol:1", "vol:2"]),
        "vol:1": N("vol:1", "volume", "col:1", "Moss. IV", order=((),)),
        "vol:2": N("vol:2", "volume", "col:1", "Unspecified volume", order=((),)),
        "col:2": N("col:2", "collection", "lib:1", "Antonin", children=["vol:3"]),
        "vol:3": N("vol:3", "volume", "col:2", "Yevr. III B", order=((),)),
        "col:3": N("col:3", "collection", "lib:2", "Or."),
        "other:lib:1": N("other:lib:1", "other", "lib:1", "Other shelfmarks"),
    }
    fist = [_leaf(f"f{i}", f"Yevr. III B {i}", "fist", f"Yevr. III B {i}", f"Ms. Evr. Antonin B {i}")
            for i in range(1, 7)]
    nodes["vol:3"].leaves = [(((0, i),), lf) for i, lf in enumerate(fist)]
    nodes["vol:3"].leaves.append((((1,),), _leaf("a", "Ms. Evr. Antonin B 1092", "fist")))
    nodes["col:1"].leaves = [(((9,),), _leaf("m", "Moss. IV,366.2", "collection"))]
    nodes["vol:2"].leaves = [(((9,),), _leaf(f"e{i}", f"ENA L{i}", "fist")) for i in (34, 35)]
    held = _leaf("h", "Moss. IV,1")
    held.held = True
    nodes["other:lib:1"].leaves = [(((9,),), held), (((9,),), _leaf("bl", "LON BL Or. 10578K f.10"))]
    stats = builder.cohere(nodes)
    labels = lambda nid: [lf.label for _k, lf in nodes[nid].leaves]  # noqa: E731
    assert "Yevr. III B 1092" in labels("vol:3")                        # the collection's own form
    assert "Moss. IV,366.2" in labels("vol:1")                          # beside its volume -> into it
    assert ["ENA L34", "ENA L35"] == labels("vol:col:1:form:enal")      # FGP's unspecified: by form
    assert "vol:2" not in nodes
    assert labels("other:lib:1") == ["Moss. IV,1"]                     # held back stays held back
    assert labels("col:3") == ["Or. 10578K f.10"]                      # its siglum named the BL
    assert stats["cohere_rewritten"] == 1


def test_a_filtered_match_of_one_child_opens_below_it(tmp_path, monkeypatch):
    # PR review: a match with one child and no shelfmarks of its own is no choice; like a click on it,
    # it opens below (its child column is not drawn, so the path must not end at it).
    from web.components import shelfmark_tree_picker as picker
    monkeypatch.setattr(builder, "PREFIX_GROUP_MIN", 1)
    monkeypatch.setattr(builder, "FGP_RANGES", {10: (("T-S AS 1-5", 1, 5),)})
    db, _libraries, _meta = build_fixture(tmp_path)
    s = ShelfmarkTreeService(str(db))
    try:
        con = sqlite3.connect(f"file:{db.as_posix()}?mode=ro", uri=True)
        row = con.execute(
            "SELECT n.id, n.parent_id FROM node n JOIN node p ON p.id = n.parent_id WHERE n.child_nodes = 1 "
            "AND n.direct_leaves = 0 AND (p.child_nodes > 1 OR p.direct_leaves > 0) "
            "AND p.parent_id IS NOT NULL").fetchone()
        con.close()
        assert row, "the fixture has a group of one volume"
        lone, parent = row
        labels = {"library": "Library", "others": "Others", "in": "in {place}"}
        at = tuple(a.id for a in s.ancestors(parent))
        view = picker._load_view(s, at, {f"n:{parent}": s.node(lone).name_en}, {}, "en", labels)
        hit = next(r for c in view["columns"] if c["parent"] == parent for r in c["rows"] if r.get("id") == lone)
        assert hit["go"][-2:] == (lone, s.children(lone)[0].id)
    finally:
        s.close()


def test_a_collections_rewrite_reaches_the_librarys_other_shelfmarks():
    # owner, 2026-11-20: "Brumer is Rab": FGP writes JTS "Catalogue Brumer Rab. 18" as "MS R18"
    N = builder.Node
    nodes = {
        "lib:1": N("lib:1", "library", None, "JTS", children=["col:1", "other:lib:1"]),
        "col:1": N("col:1", "collection", "lib:1", "MS Rabbinica", children=["vol:18"]),
        "vol:18": N("vol:18", "volume", "col:1", "MS R18", order=((),)),
        "other:lib:1": N("other:lib:1", "other", "lib:1", "Other shelfmarks"),
    }
    nodes["vol:18"].leaves = [(((0, i),), _leaf(f"f{i}", f"MS R18, fol. {i}", "fist", f"MS R18, fol. {i}",
                                                f"Catalogue Brumer Rab. 18, fol. {i}")) for i in range(1, 7)]
    nodes["other:lib:1"].leaves = [(((9,),), _leaf("b", "Catalogue Brumer Rab. 18"))]
    builder.cohere(nodes)
    assert "MS R18" in [lf.label for _k, lf in nodes["vol:18"].leaves]
    assert "other:lib:1" not in nodes                                   # emptied, so gone


def test_aliases_place_a_shelfmark_and_are_searched(tmp_path):
    # owner, 2026-11-20: libraries.csv's other call numbers place a shelfmark its label cannot
    # ("Ms. II 137" is "Ms. 5440" too: MS 5440), and a filter finds a shelfmark by any of them
    N = builder.Node
    nodes = {
        "lib:1": N("lib:1", "library", None, "JTS", children=["col:1", "other:lib:1"]),
        "col:1": N("col:1", "collection", "lib:1", "MS", children=["vol:5440", "vol:1"]),
        "vol:5440": N("vol:5440", "volume", "col:1", "MS 5440", order=((),)),
        "vol:1": N("vol:1", "volume", "col:1", "MS 1", order=((),)),
        "other:lib:1": N("other:lib:1", "other", "lib:1", "Other shelfmarks"),
    }
    nodes["vol:5440"].leaves = [(((0,),), _leaf("f", "MS 5440", "fist", "MS 5440"))]
    nodes["vol:1"].leaves = [(((0,),), _leaf("g", "MS 1", "fist", "MS 1"))]
    nodes["other:lib:1"].leaves = [(((9,),), _leaf("a", "Ms. II 137"))]
    builder.cohere(nodes, {"a": ["Adler, Elkan Nathan Ms. II 137", "Ms. 5440", "Ms. II 137"]})
    placed = [lf for _k, lf in nodes["vol:5440"].leaves if lf.sys_id == "a"]
    assert placed and placed[0].label == "MS 5440"                     # by its alias, and so written
    assert "Ms. II 137" in placed[0].aliases
    db, _libraries, _meta = build_fixture(tmp_path)
    s = ShelfmarkTreeService(str(db))
    try:
        leaf = s.leaves(s.locate("T-S AS 2").nodes[0].id, 0, 1)[0][0]
        con = sqlite3.connect(str(db))
        con.execute("UPDATE leaf SET aliases = 'Ms. Unique 777' WHERE id = ?", (leaf.id,))
        con.commit()
        con.close()
        assert [lf.id for lf in s.search_leaves("lib:1", "Unique 777")[0]] == [leaf.id]
        assert s.leaves(leaf.node_id, 0, 10, filter="Unique 777")[1] == 1
    finally:
        s.close()


def test_a_long_list_of_unnumbered_volumes_groups_by_stem_without_failing():
    # PR review: a collection of more than VOLUME_RANGE_MIN volumes, none numbered, must not abort
    # the build; the stem fallback groups them ("Box A ..." -> "Box A")
    N = builder.Node
    nodes = {"col:1": N("col:1", "collection", None, "Boxes")}
    for i in range(builder.VOLUME_RANGE_MIN + 1):          # one prefix, no numbers
        vid = f"vol:{i}"
        nodes[vid] = N(vid, "volume", "col:1", "Box", order=((i,),))
        nodes["col:1"].children.append(vid)
    builder.group_volumes(nodes, collections_by_node={"col:1": 1})       # does not raise
    assert len(nodes["col:1"].children) == builder.VOLUME_RANGE_MIN + 1  # nothing to divide by: left as is


def test_every_volume_is_named_as_its_shelfmarks_are_written():
    # PR review: the renaming ran on one collection only
    N = builder.Node
    nodes = {"lib:1": N("lib:1", "library", None, "L", children=["col:1", "col:2"])}
    for c in (1, 2):
        nodes[f"col:{c}"] = N(f"col:{c}", "collection", "lib:1", f"C{c}", children=[f"vol:{c}"])
        nodes[f"vol:{c}"] = N(f"vol:{c}", "volume", f"col:{c}", f"OR {c}0", order=((),))
        nodes[f"vol:{c}"].leaves = [(((0, i),), _leaf(f"{c}{i}", f"Or. {c}0.{i}", "fist", f"Or. {c}0.{i}"))
                                    for i in range(3)]
    builder.cohere(nodes)
    assert [nodes["vol:1"].name_en, nodes["vol:2"].name_en] == ["Or. 10", "Or. 20"]


def test_the_csv_places_a_code_librarys_shelfmarks_and_an_alias_in_a_collections_form():
    # owner, 2026-11-20 ("use the csv"): records coded "Lutzki" are JTS MS Lutzki's, by the rewrite its
    # own records show; "Ms. II 573" is also "Ms. 10617", so JTS MS
    N = builder.Node
    nodes = {
        "lib:1": N("lib:1", "library", None, "JTS", children=["col:1", "col:2", "other:lib:1"]),
        "col:1": N("col:1", "collection", "lib:1", "MS Lutzki", children=["vol:1"]),
        "vol:1": N("vol:1", "volume", "col:1", "MS L1", order=((),)),
        "col:2": N("col:2", "collection", "lib:1", "MS", children=["vol:2"]),
        "vol:2": N("vol:2", "volume", "col:2", "MS 2", order=((),)),
        "other:lib:1": N("other:lib:1", "other", "lib:1", "Other shelfmarks"),
        "code:Lutzki": N("code:Lutzki", "library", None, "Lutzki Collection", children=["other:code:Lutzki"]),
        "other:code:Lutzki": N("other:code:Lutzki", "other", "code:Lutzki", "Other shelfmarks"),
    }
    nodes["vol:1"].leaves = [(((0, i),), _leaf(f"f{i}", f"MS L1, fol. {i}", "fist", f"MS L1, fol. {i}",
                                               f"Catalogue Lutzki L 1, fol. {i}")) for i in range(1, 7)]
    nodes["vol:2"].leaves = [(((0,),), _leaf("g", "MS 2", "fist", "MS 2"))]   # no majority needed
    nodes["other:code:Lutzki"].leaves = [(((9,),), _leaf("l", "Catalogue Lutzki L 588"))]
    nodes["other:lib:1"].leaves = [(((9,),), _leaf("a", "Ms. II 573"))]
    builder.cohere(nodes, {"a": ["Adler, Elkan Nathan Ms. II 573", "Ms. 10617"]})
    assert "MS L588" in [lf.label for _k, lf in nodes["col:1"].leaves]
    assert "code:Lutzki" not in nodes                                   # emptied: gone
    assert "MS 10617" in [lf.label for _k, lf in nodes["col:2"].leaves]


def test_a_path_never_ends_at_a_level_that_is_no_choice(svc):
    # PR review: Strasbourg (lib:2) holds one collection and no shelfmarks of its own
    from web.components.shelfmark_tree_picker import _settle
    assert _settle(svc, ("lib:2",), ("lib:2", "col:20")) == ()            # back: past it
    assert _settle(svc, ("lib:2",), ()) == ("lib:2", "col:20")            # forward: below it
    assert _settle(svc, ("lib:1",), ("lib:1", "col:10")) == ("lib:1",)    # a real level stays


# -- FGP's arrangement and the audit's rules (owner's notes and the tree audit, 2026-11-20) ---------

def _volume(nodes, vid, col, name, fgp=None, leaves=()):
    part = builder._ob_part
    nodes[vid] = builder.Node(vid, "volume", col, name, order=((),),
                              fgp=tuple(part(x) for x in fgp) if fgp else ())
    nodes[vid].leaves = [(((0, i),), lf) for i, lf in enumerate(leaves)]
    nodes[col].children.append(vid)


def test_volumes_stand_in_fgps_order_with_numerals_in_their_own():
    # FGP: Moss. IV, IX, I, Ia, II ..., then the letters, then the 2nd Series; its own order reads the
    # Roman numerals as letters. A volume FGP does not have stands where its name puts it.
    nodes = {"col:1": builder.Node("col:1", "collection", None, "Mosseri Collection")}
    for i, (name, fgp) in enumerate([
            ("2nd Series: A", (2, 2, "A")), ("Moss. IV", (1, -4, "0")), ("Moss. IX", (1, 1, "0")),
            ("Moss. I", (1, 1, "0")), ("Moss. Ia", (1, 1, "a")), ("Moss. II", (1, 2, "0")), ("A", (2, 1, "A")),
            ("C", (2, 1, "C")), ("Moss. III", None)]):
        _volume(nodes, f"vol:{i}", "col:1", name, fgp)
    builder.order_volumes(nodes)
    ordered = sorted(nodes["col:1"].children, key=lambda v: nodes[v].order)
    assert [nodes[v].name_en for v in ordered] == [
        "Moss. I", "Moss. Ia", "Moss. II", "Moss. III", "Moss. IV", "Moss. IX", "A", "C", "2nd Series: A"]
    # "T-S Loan" after "T-S Misc.36", as FGP has it, not before "T-S Misc.1" by its letters
    nodes = {"col:2": builder.Node("col:2", "collection", None, "T-S Misc.")}
    for i, (name, fgp) in enumerate([("T-S Loan", (150, 37, None)), ("T-S Misc.10", (150, 10, None)),
                                     ("T-S Misc.2", (150, 2, None))]):
        _volume(nodes, f"vol:{i}", "col:2", name, fgp)
    builder.order_volumes(nodes)
    ordered = sorted(nodes["col:2"].children, key=lambda v: nodes[v].order)
    assert [nodes[v].name_en for v in ordered] == ["T-S Misc.2", "T-S Misc.10", "T-S Loan"]


def test_fgps_own_groups_stand_where_their_first_volume_does(tmp_path, monkeypatch):
    import re
    monkeypatch.setattr(builder, "FGP_GROUPS", {10: (("Tens", re.compile(r"AS 10$")), ("Other", None, "אחר"))})
    db, libraries, meta = build_fixture(tmp_path)
    s = ShelfmarkTreeService(str(db))
    try:
        groups = [(c.kind, c.name_en) for c in s.children("col:10")]
        assert groups == [("group", "Other"), ("group", "Tens")]         # T-S AS 2 comes before T-S AS 10
        assert [c.name_he for c in s.children("col:10")] == ["אחר", ""]   # FGP's group, named in Hebrew too
        assert [c.name_en for c in s.children(s.children("col:10")[1].id)] == ["T-S AS 10"]
    finally:
        s.close()
    assert verifier.verify(str(db), int(meta["leaves_total"]), libraries, canaries=None) == []


def test_ranges_are_round_item_numbers_as_fgps():
    N = builder.Node
    moss = N("vol:1", "volume", None, "Moss. IV")
    moss.leaves = [(((0,),), _leaf(f"m{i}{s}", f"Moss. IV,{i}{s}", "fist")) for i in range(1, 401) for s in ("", ".1")]
    assert [name for name, _chunk in builder.split_ranges(moss)] == [
        "Moss. IV,1 - 100", "Moss. IV,101 - 200", "Moss. IV,201 - 300", "Moss. IV,301 - 400"]
    wide = N("vol:2", "volume", None, "2nd Series: P")               # one shelfmark a number: 200 wide
    wide.leaves = [(((0,),), _leaf(f"p{i}", f"2nd Series: P {i}", "fist")) for i in range(1, 801)]
    assert [name for name, _chunk in builder.split_ranges(wide)] == [
        "2nd Series: P 1 - 200", "2nd Series: P 201 - 400", "2nd Series: P 401 - 600", "2nd Series: P 601 - 800"]
    # numbers that do not rise with the order: the node mixes forms, and no range holds two forms' numbers
    mixed = N("col:3", "collection", None, "Academy")
    mixed.leaves = [(((0,),), _leaf(f"a{i}", f"M {i}")) for i in range(1, 401)] + [
        (((0,),), _leaf(f"b{i}", f"T {i}")) for i in range(1, 401)]
    names = [name for name, _chunk in builder.split_ranges(mixed)]
    assert names == ["M 1 – 400", "T 1 – 400"]
    # tree audit (Budapest): each form by itself, ranged again where it is large; a bare Roman
    # numeral is a form of its own; the stray ones (fewer than three) together at the end
    labels = ([str(i) for i in range(1, 701)] + [f"M {i}" for i in range(1, 31)]
              + ["I", "II", "III", "G 1", "G 2"])
    academy = N("col:5", "collection", None, "Academy")
    academy.leaves = [(((0,),), _leaf(f"y{i}", lb)) for i, lb in enumerate(
        sorted(labels, key=builder.roman_natural_key))]
    chunks = builder.split_ranges(academy)
    assert [name for name, _chunk in chunks] == [
        "1 - 200", "201 - 400", "401 - 600", "601 - 800", "I – III", "M 1 – 30", "G 1 – 2"]
    assert [lf.label for _k, lf in chunks[-1][1]] == ["G 1", "G 2"]
    # a number holding more than a range may: divided again, by count where no narrower width helps
    dense = N("vol:4", "volume", None, "X 1")
    dense.leaves = [(((0,),), _leaf(f"d{i}", f"X 1.7.{i}", "fist")) for i in range(1, 1202)]
    chunks = builder.split_ranges(dense)
    assert all(len(chunk) <= builder.RANGE_SIZE for _name, chunk in chunks) and sum(
        len(chunk) for _name, chunk in chunks) == 1201


def test_a_call_number_after_a_colon_is_the_shelfmark():
    assert builder.after_colon("T-S Boxes: T-S K25.240") == "T-S K25.240"
    assert builder.after_colon("Oriental: Or.1080.46") == "Or.1080.46"
    assert builder.after_colon("2nd Series: A 12") == "2nd Series: A 12"      # FGP's own name of the volume
    assert builder.after_colon("T-S AS 2.1") == "T-S AS 2.1"


def test_an_alias_places_what_its_label_only_names_the_collection_of():
    # owner, 2026-11-20: FGP's "ENA 2b" (its "Unspecified volume") is also "Ms. ENA 2557.1"
    N = builder.Node
    nodes = {"lib:1": N("lib:1", "library", None, "JTS", children=["col:1"]),
             "col:1": N("col:1", "collection", "lib:1", "ENA")}
    _volume(nodes, "vol:1", "col:1", "ENA 2557", leaves=[_leaf("a", "ENA 2557.1", "fist", "ENA 2557.1")])
    _volume(nodes, "vol:2", "col:1", "Unspecified volume", leaves=[_leaf("b", "ENA 2b", "fist", "ENA 2b")])
    builder.cohere(nodes, {"b": ["ENA 2b", "Ms. 2b", "Ms. ENA 2557.1"]})
    assert sorted(lf.label for _k, lf in nodes["vol:1"].leaves) == ["ENA 2557.1", "ENA 2b"]
    assert "vol:2" not in nodes and not nodes["col:1"].leaves


def test_a_shelfmark_goes_to_the_volume_its_label_names():
    # owner, 2026-11-20: FGP's "F" of "RNL Boxes" holds "F 1" and "D 37"; our records are "Box D.7" ...
    N = builder.Node
    nodes = {"lib:1": N("lib:1", "library", None, "RNL", children=["col:1", "col:2"]),
             "col:1": N("col:1", "collection", "lib:1", "RNL Boxes"),
             "col:2": N("col:2", "collection", "lib:1", "Mosseri Collection")}
    _volume(nodes, "vol:f", "col:1", "F", (1, 1, None),
            [_leaf("f1", "F 1", "fist", "F 1"), _leaf("d37", "D 37", "fist", "D 37")])
    nodes["col:1"].leaves = [(((9,),), _leaf(n, n, "collection")) for n in
                             ("Box D.7", "Box D.8", "Box F.13", "Box F.21")]
    # a shelfmark its own volume's name ends with stays ("C 7" in FGP's "2nd Series: C", beside "C")
    _volume(nodes, "vol:c", "col:2", "C", (2, 1, "C"), [_leaf("c1", "C 1", "fist", "C 1")])
    _volume(nodes, "vol:2c", "col:2", "2nd Series: C", (2, 2, "C"),
            [_leaf("s2", "2nd Series: C 2", "fist", "2nd Series: C 2"), _leaf("c7", "C 7", "fist", "C 7")])
    # ... and one that names another volume of the collection goes to it ("Moss. Ia,9" in "Moss. I")
    _volume(nodes, "vol:i", "col:2", "Moss. I", (1, 1, "0"),
            [_leaf("i1", "Moss. I,1", "fist", "Moss. I,1"), _leaf("ia9", "Moss. Ia,9", "fist", "Moss. Ia,9")])
    _volume(nodes, "vol:ia", "col:2", "Moss. Ia", (1, 1, "a"), [_leaf("ia1", "Moss. Ia,1", "fist", "Moss. Ia,1")])
    builder.cohere(nodes)
    labels = lambda nid: [lf.label for _k, lf in nodes[nid].leaves]  # noqa: E731
    assert labels("vol:col:1:stem:boxd") == ["Box D.7", "Box D.8", "Box D.37"]
    assert labels("vol:col:1:stem:boxf") == ["Box F.1", "Box F.13", "Box F.21"]
    assert "vol:f" not in nodes                                         # FGP's "F" is our "Box F"
    assert labels("vol:2c") == ["2nd Series: C 2", "C 7"] and labels("vol:c") == ["C 1"]
    assert labels("vol:ia") == ["Moss. Ia,1", "Moss. Ia,9"] and labels("vol:i") == ["Moss. I,1"]


def test_a_volume_keeps_fgps_name_on_a_tie_and_its_shelfmarks_write_it_so():
    N = builder.Node
    nodes = {"lib:1": N("lib:1", "library", None, "CUL", children=["col:1"]),
             "col:1": N("col:1", "collection", "lib:1", "T-S Bound")}
    _volume(nodes, "vol:1", "col:1", "T-S 18K1", leaves=[
        _leaf(str(i), f"T-S 18{'K' if i < 3 else 'k'}1.{i}", "fist", f"T-S 18{'K' if i < 3 else 'k'}1.{i}")
        for i in range(1, 5)])
    # never away from a Roman numeral, whatever its shelfmarks write
    _volume(nodes, "vol:2", "col:1", "Moss. VIa", leaves=[
        _leaf(f"v{i}", f"Moss. Via,{i}", "fist", f"Moss. Via,{i}") for i in (1, 2)])
    builder.cohere(nodes)
    assert nodes["vol:1"].name_en == "T-S 18K1" and nodes["vol:2"].name_en == "Moss. VIa"
    assert [lf.label for _k, lf in nodes["vol:1"].leaves] == [f"T-S 18K1.{i}" for i in range(1, 5)]
    assert [lf.label for _k, lf in nodes["vol:2"].leaves] == ["Moss. VIa,1", "Moss. VIa,2"]


def test_volumes_fgp_keeps_apart_are_not_twins():
    # tree audit: "T-S F11" was folded into "T-S F1(1)", "MS heb. f.7*" into "MS heb. f.7"
    assert builder.twin_key("T-S F11") != builder.twin_key("T-S F1(1)")
    assert builder.twin_key("MS heb. f.7*") != builder.twin_key("MS heb. f.7")
    assert builder.twin_key("OR") == builder.twin_key("Or.") and builder.twin_key("Com") == builder.twin_key("com.")
    N = builder.Node
    nodes = {"lib:1": N("lib:1", "library", None, "CUL", children=["col:1"]),
             "col:1": N("col:1", "collection", "lib:1", "T-S A - K")}
    _volume(nodes, "vol:1", "col:1", "T-S F1(1)", leaves=[_leaf("a", "T-S F1(1).1", "fist", "T-S F1(1).1")])
    _volume(nodes, "vol:2", "col:1", "T-S F11", leaves=[_leaf("b", "T-S F11.1", "fist", "T-S F11.1")])
    builder.cohere(nodes)
    assert [lf.label for _k, lf in nodes["vol:2"].leaves] == ["T-S F11.1"]
    assert [lf.label for _k, lf in nodes["vol:1"].leaves] == ["T-S F1(1).1"]


def test_shelfmarks_of_one_label_show_what_their_call_number_adds():
    N = builder.Node
    nodes = {"vol:1": N("vol:1", "volume", None, "OR 10578R"), "vol:2": N("vol:2", "volume", None, "T-S K25")}
    nodes["vol:1"].leaves = [(((0,),), _leaf(s, "OR 10578R", "collection")) for s in ("a", "b", "c")] + [
        (((0,),), _leaf("d", "Yevr.-Arab. II 974", "fist", "Yevr.-Arab. II 974")),
        (((0,),), _leaf("e", "Yevr.-Arab. II 974", "fist", "Yevr.-Arab. II 974"))]
    nodes["vol:2"].leaves = [(((0,),), _leaf("x", "T-S K25.236", "fist")), (((0,),), _leaf("y", "T-S K25.236", "fist")),
                             (((0,),), _leaf("z", "T-S K25.9", "fist"))]
    told = builder.tell_apart(nodes, {
        "a": ["LON BL Or. 10578R f.1", "Ms. 1775"], "b": ["The British Library Or. 10578R FF. 35- 36"],
        "c": ["Or. 10578S f. 9"],                              # names another volume: says nothing here
        "d": ["Ms. EVR ARAB II 974.1", "The National Library of Russia Ms. EVR ARAB II 974.1"],
        "e": ["Ms. EVR ARAB II 974.2"],
        "x": ["Ms. T-S K 25.236"], "y": ["Ms. T-S K 25.236"], "z": ["Ms. T-S K 25.9a"]})   # alone: untouched
    assert [lf.label for _k, lf in nodes["vol:1"].leaves] == [
        "OR 10578R f. 1", "OR 10578R f. 35-36", "OR 10578R", "Yevr.-Arab. II 974.1", "Yevr.-Arab. II 974.2"]
    assert [lf.label for _k, lf in nodes["vol:2"].leaves] == ["T-S K25.236", "T-S K25.236", "T-S K25.9"]
    assert told == 4


def test_the_fuller_call_number_decides_and_group_titles_are_written_as_their_volumes():
    # tree audit: a bare "Ms. 9160" was written "ENA 9160" by ENA's rule; its full form is JTS's own
    N = builder.Node
    nodes = {"lib:1": N("lib:1", "library", None, "JTS", children=["col:1", "col:2", "other:lib:1"]),
             "col:1": N("col:1", "collection", "lib:1", "ENA"), "col:2": N("col:2", "collection", "lib:1", "MS"),
             "other:lib:1": N("other:lib:1", "other", "lib:1", "Other shelfmarks")}
    _volume(nodes, "vol:1", "col:1", "ENA 5", leaves=[
        _leaf(f"e{i}", f"ENA 5.{i}", "fist", f"ENA 5.{i}") for i in range(1, 7)])
    _volume(nodes, "vol:2", "col:2", "MS 7", leaves=[
        _leaf(f"m{i}", f"MS 7.{i}", "fist", f"MS 7.{i}") for i in range(1, 7)])
    nodes["other:lib:1"].leaves = [(((9,),), _leaf("j", "Ms. 9160")), (((9,),), _leaf("a", "Ms. 88"))]
    variants = {f"e{i}": [f"Ms. 5.{i}", f"Adler, Elkan Nathan Ms. 5.{i}"] for i in range(1, 7)}
    variants.update({f"m{i}": [f"Ms. 7.{i}", f"The Seminary Ms. 7.{i}"] for i in range(1, 7)})
    variants.update({"j": ["Ms. 9160", "The Seminary Ms. 9160"], "a": ["Ms. 88", "Adler, Elkan Nathan Ms. 88"]})
    builder.cohere(nodes, variants)
    assert [lf.label for _k, lf in nodes["col:2"].leaves] == ["MS 9160"]
    assert [lf.label for _k, lf in nodes["col:1"].leaves] == ["ENA 88"]
    # "MS L33-489" over "MS L33" ..., not "MS L 33-489"
    nodes = {"col:1": N("col:1", "collection", None, "MS Lutzki")}
    for i in range(builder.VOLUME_RANGE_MIN + 2):
        _volume(nodes, f"vol:{i}", "col:1", f"MS L{i + 1}")
        nodes[f"vol:{i}"].order = ((0, i),)
    builder.group_volumes(nodes, collections_by_node={"col:1": 1})
    titles = [nodes[g].name_en for g in nodes["col:1"].children]
    assert titles[0].startswith("MS L1-") and all(t.startswith("MS L") and " " not in t[4:] for t in titles)


def _with_catalogue(tmp_path):
    fist = make_fist(tmp_path / "FIST.db")
    con = sqlite3.connect(str(fist))
    con.executescript("""
        CREATE TABLE CODE_Catalog (CatalogId INTEGER, CatAcronym TEXT, IsCanceledCode INTEGER);
        CREATE TABLE dbo_InventoryCatalogEntry (InventoryId INTEGER, CatalogId INTEGER, CatalogEntry TEXT,
                                                RecordStatus INTEGER, OrderBy INTEGER);
        INSERT INTO CODE_Catalog VALUES (7, 'N - C', 0);
        INSERT INTO dbo_InventoryCatalogEntry VALUES (1301, 7, '2603.1', 0, 1), (1302, 7, '2603.1', 0, 2),
            (1303, 7, '2603.2', 0, 3), (1304, 7, '2603.10', 0, 4), (1305, 7, '2603.11', 1, 5),
            (1401, 7, '2700', 0, 6);
    """)
    con.commit()
    con.close()
    libraries = make_libraries(tmp_path / "libraries.csv")
    out = tmp_path / "shelfmark_tree.db"
    meta = builder.build(fist, libraries, out, log=lambda *_: None)
    return out, libraries, meta


def test_a_catalogue_is_a_second_way_into_a_collection(tmp_path):
    # owner, 2026-11-20: Oxford in FGP is also listed 'By "Neubauer - Cowley" catalog'
    db, libraries, meta = _with_catalogue(tmp_path)
    s = ShelfmarkTreeService(str(db))
    try:
        kids = s.children("col:11")
        assert [(c.kind, c.name_en) for c in kids] == [("volume", "T-S NS 5"), ("group", 'By "N - C" catalogue')]
        entries = s.children(kids[1].id)
        assert [e.name_en for e in entries] == ["2603.1", "2603.2", "2603.10"]      # numbers, not letters
        assert [lf.label for lf in s.leaves(entries[0].id)[0]] == ["T-S NS 5.1", "T-S NS 5.2"]
        # the place on the shelf stays a manuscript's first; the catalogue's is one more
        assert [p.leaf.how for p in s.path_of(SYS["ns5"][0])] == ["fist", "catalogue"]
        assert int(meta["catalogue_placements"]) == 6 and int(meta["placed_fist"]) == int(meta["manuscripts_total"]) - sum(
            int(meta[f"placed_{h}"]) for h in builder.HOW if h != "fist")
        # stepping along the shelf never runs on into the catalogue's list, and the list stays a list
        last_on_shelf = s.leaves("vol:110", 0, 50)[0][-1]
        assert s.neighbours(last_on_shelf.id)[1].how != "catalogue"
        first_entry = s.leaves(entries[0].id)[0][0]
        prev, nxt = s.neighbours(first_entry.id)
        assert prev is None and nxt.how == "catalogue" and nxt.label == "T-S NS 5.2"
        # PR review: "Or. 1" sits in its collection outside any volume, so the collection's list is
        # written before it; its place on the shelf is still its first, and a filter over the
        # collection still finds it once
        listed = next(c for c in s.children("col:12") if c.kind == "group")
        assert s.leaves(listed.id, 0, 1)[0] == [] and s.node(listed.id).lo < s.leaves("col:12", 0, 1)[0][0].pos
        assert [p.leaf.how for p in s.path_of(SYS["or"][0])] == ["fist", "catalogue"]
        assert [lf.how for lf in s.search_leaves("col:12", "Or. 1")[0]] == ["fist"]
        assert [lf.how for lf in s.search_leaves(listed.id, "Or. 1")[0]] == ["catalogue"]
        # PR review: a catalogue place is found by the record's other call numbers, as its shelf place is
        on_shelf, in_list = (p.leaf for p in s.path_of(SYS["or"][0]))
        assert in_list.aliases == on_shelf.aliases != ""
        other_number = on_shelf.aliases.split(" | ")[0]
        assert [lf.id for lf in s.search_leaves(listed.id, other_number)[0]] == [in_list.id]
        # PR review: a library with two lists: stepping stays in the list it began in
        last_listed = s.leaves(entries[-1].id, 0, 50)[0][-1]
        assert s.neighbours(last_listed.id)[1] is None
        assert s.neighbours(s.path_of(SYS["or"][0])[1].leaf.id) == (None, None)
        # ... and its "1 of N" counts the shelf's places, not the list's copies
        from shared.shelfmark_tree_view import Position, position
        on_shelf = s.path_of(SYS["or"][0])[0].leaf
        assert position(s, on_shelf) == Position(1, len(s.leaves("col:12", 0, 50)[0]))
        assert position(s, s.path_of(SYS["or"][0])[1].leaf) == Position(1, 1)
        # PR review: the picker reopens at a catalogue placement as it does at one on the shelf
        from shared.shelfmark_tree_view import start_at
        again = start_at(s, first_entry.id)
        assert again.leaf.id == first_entry.id and again.path[-1] == entries[0].id
        # a filter over the collection finds a shelfmark once, on the shelf; inside the list, the list's
        assert [lf.how for lf in s.search_leaves("col:11", "NS 5.1")[0]] == ["fist"]
        assert [lf.how for lf in s.search_leaves(kids[1].id, "NS 5.1")[0]] == ["catalogue"]
    finally:
        s.close()
    assert verifier.verify(str(db), int(meta["leaves_total"]), libraries, canaries=None) == []


def test_a_record_left_out_has_no_place_and_the_gate_does_not_ask_for_one(tmp_path, monkeypatch):
    # owner, 2026-11-20: a record with nothing in it at any library is not shown
    monkeypatch.setattr(builder, "LEFT_OUT", {SYS["no_code"]: "nothing in it"})
    db, libraries, meta = build_fixture(tmp_path)
    s = ShelfmarkTreeService(str(db))
    try:
        assert s.path_of(SYS["no_code"]) == []
    finally:
        s.close()
    problems = verifier.verify(str(db), int(meta["leaves_total"]), libraries, canaries=None)
    assert any("have no place" in p for p in problems), problems           # the gate can fail on it
    monkeypatch.setattr(verifier, "LEFT_OUT", {SYS["no_code"]: "nothing in it"})
    assert verifier.verify(str(db), int(meta["leaves_total"]), libraries, canaries=None) == []


def test_one_place_per_node_manuscript_and_label(tmp_path):
    # tree audit: FGP's "alternate" rows repeat a shelfmark for the same record (Budapest "10" x3)
    fist = make_fist(tmp_path / "FIST.db")
    con = sqlite3.connect(str(fist))
    con.execute("INSERT INTO dbo_Inventory (InventoryId, CollectionId, InventoryGroupId, Shelfmark, RecordStatus) "
                "VALUES (9001, 11, 110, 'T-S NS 5.1', 0)")
    con.execute("INSERT INTO dbo_InventoryAlma (InventoryId, AlmaId) VALUES (9001, ?)", (int(SYS["ns5"][0]),))
    con.commit()
    con.close()
    libraries = make_libraries(tmp_path / "libraries.csv")
    out = tmp_path / "shelfmark_tree.db"
    meta = builder.build(fist, libraries, out, log=lambda *_: None)
    s = ShelfmarkTreeService(str(out))
    try:
        assert len(s.path_of(SYS["ns5"][0])) == 1
    finally:
        s.close()
    assert verifier.verify(str(out), int(meta["leaves_total"]), libraries, canaries=None) == []


def test_a_collection_shown_as_fgps_groups_alone(monkeypatch):
    # owner, 2026-11-20: instead of "T-S Bound", its two groups "T-S 6F - 8K" and "T-S 10C - 18K"
    monkeypatch.setattr(builder, "FGP_IN_PLACE", frozenset({102, 103}))
    N = builder.Node
    nodes = {"lib:1": N("lib:1", "library", None, "CUL", children=["col:101", "col:102", "col:103"]),
             "col:101": N("col:101", "collection", "lib:1", "T-S A - K", order=((0, 1), ())),
             "col:102": N("col:102", "collection", "lib:1", "T-S Bound", order=((0, 2), ()),
                          children=["grp:col:102:1", "grp:col:102:2"]),
             "grp:col:102:1": N("grp:col:102:1", "group", "col:102", "T-S 6F - 8K", children=["vol:1"]),
             "grp:col:102:2": N("grp:col:102:2", "group", "col:102", "T-S 10C - 18K", children=["vol:2"]),
             "vol:1": N("vol:1", "volume", "grp:col:102:1", "T-S 6F1"),
             "vol:2": N("vol:2", "volume", "grp:col:102:2", "T-S 10C1"),
             # one that holds shelfmarks of its own stays as it is
             "col:103": N("col:103", "collection", "lib:1", "T-S Ar.", order=((0, 3), ()), children=["grp:x"]),
             "grp:x": N("grp:x", "group", "col:103", "T-S Ar. 1-10")}
    nodes["col:103"].leaves = [(((0,),), _leaf("a", "T-S Ar.1"))]
    builder.groups_in_place(nodes, {"col:101": 101, "col:102": 102, "col:103": 103})
    assert nodes["lib:1"].children == ["col:101", "col:102:1", "col:102:2", "col:103"]
    assert "col:102" not in nodes and "grp:col:102:1" not in nodes
    first, second = nodes["col:102:1"], nodes["col:102:2"]
    assert (first.kind, first.name_en, first.parent) == ("collection", "T-S 6F - 8K", "lib:1")
    assert nodes["vol:1"].parent == "col:102:1" and nodes["vol:2"].parent == "col:102:2"
    assert first.order < second.order and nodes["col:101"].order < first.order   # 6F before 10C, in its place
    assert nodes["col:103"].kind == "collection" and nodes["grp:x"].kind == "group"


def test_fgps_slips_in_a_volumes_number_are_read_from_its_shelfmarks():
    # tree audit: FGP's volume "ENA II 177" holds "ENA II 117.1" ... (its own order column says 117);
    # "ENA NS I" stands among "ENA NS 2" ..., and its records' call numbers write "ENA NS 1"
    N, part = builder.Node, builder._ob_part
    nodes = {"lib:1": N("lib:1", "library", None, "JTS", children=["col:1", "col:2", "col:3"]),
             "col:1": N("col:1", "collection", "lib:1", "ENA II"), "col:2": N("col:2", "collection", "lib:1", "ENA NS"),
             "col:3": N("col:3", "collection", "lib:1", "Lewis-Gibson")}
    _volume(nodes, "vol:177", "col:1", "ENA II 177")
    nodes["vol:177"].leaves = [((part(None), part(None), part(None), part(117.0)),
                                _leaf(f"a{i}", f"ENA II 117.{i}", "fist", f"ENA II 117.{i}")) for i in (1, 2, 3)]
    _volume(nodes, "vol:137", "col:1", "ENA II 137", leaves=[_leaf("b", "ENA II 137.1", "fist", "ENA II 137.1")])
    _volume(nodes, "vol:n1", "col:2", "ENA NS I", leaves=[
        _leaf(f"n{i}", f"ENA NS I.{i}", "fist", f"ENA NS I.{i}") for i in (1, 2, 3)])
    for k in range(2, 8):
        _volume(nodes, f"vol:n{k}", "col:2", f"ENA NS {k}", leaves=[
            _leaf(f"k{k}{i}", f"ENA NS {k}.{i}", "fist", f"ENA NS {k}.{i}") for i in (1, 2)])
    # Roman numerals among Roman numerals are a series' own writing: untouched
    for k, name in enumerate(("Lit I", "Lit II", "Lit III", "Bib.I", "Bib.II", "Bib.III"), 1):
        _volume(nodes, f"vol:l{k}", "col:3", name, leaves=[
            _leaf(f"l{k}{i}", f"{name} {i}", "fist", f"{name} {i}") for i in (1, 2)])
    builder.cohere(nodes, {"n1": ["Ms. ENA NS 1.1"], "n2": ["Ms. ENA NS 1.2"], "l11": ["Ms. Lit 1 1"]})
    assert nodes["vol:177"].name_en == "ENA II 117"
    assert nodes["vol:n1"].name_en == "ENA NS 1"
    leaves = [lf for _k, lf in nodes["vol:n1"].leaves]
    assert [lf.label for lf in leaves] == ["ENA NS 1.1", "ENA NS 1.2", "ENA NS 1.3"]
    assert "ENA NS I.1" in leaves[0].aliases.split(" | ")          # FGP's own writing is still found
    assert nodes["vol:l1"].name_en == "Lit I" and nodes["vol:137"].name_en == "ENA II 137"


# -- the owner's answers of 2026-11-20 (second round) and the small fixes ---------------------------

def test_two_records_of_one_shelfmark_stay_two_rows_the_second_marked():
    N = builder.Node
    nodes = {"vol:1": N("vol:1", "volume", None, "MS L45")}
    ours, fgp, third = _leaf("b", "MS L45", "collection"), _leaf("a", "MS L45", "fist", "MS L45"), _leaf("c", "MS L45")
    alone = _leaf("d", "MS L46", "fist", "MS L46")
    nodes["vol:1"].leaves = [(((0,),), lf) for lf in (ours, fgp, third, alone)]
    assert builder.mark_same_labels(nodes) == 2
    assert (fgp.label, ours.label, third.label, alone.label) == ("MS L45", "MS L45 [2]", "MS L45 [3]", "MS L46")


def test_a_shelfmark_written_another_way_than_fgps_is_written_fgps_way():
    # Columbia: FGP's "X893.1 BC T17" and the library's "X893.1 BC T 17", "v.21" and "vol. 21"
    N = builder.Node
    nodes = {"lib:1": N("lib:1", "library", None, "Columbia", children=["col:1"]),
             "col:1": N("col:1", "collection", "lib:1", "Columbia")}
    fgp = [_leaf(f"f{i}", lb, "fist", lb) for i, lb in enumerate(("X893.1 BC T17", "X893.15 Z6 v.21", "R II 5"))]
    ours = [_leaf(f"o{i}", lb, "collection") for i, lb in enumerate(
        ("X893.1 BC T 17", "X893.15 Z 6 vol. 21", "Ms. R II 5", "X893.1 BC T 18"))]
    nodes["col:1"].leaves = [(((0, i),), lf) for i, lf in enumerate(fgp + ours)]
    builder.cohere(nodes)
    assert [lf.label for lf in ours] == ["X893.1 BC T17", "X893.15 Z6 v.21", "R II 5", "X893.1 BC T 18"]


def test_a_code_that_names_an_fgp_library_is_that_librarys(tmp_path, monkeypatch):
    # Basel: the code's own name is FGP's library; its record goes to that library's one collection
    monkeypatch.setitem(builder.LIBRARY_CODES, "Vern", "Vernadsky National Library")
    fist = make_fist(tmp_path / "FIST.db")
    libraries = make_libraries(tmp_path / "libraries.csv")
    with open(libraries, "a", encoding="utf-8", newline="\n") as f:
        f.write("990000801010205171,,Ms. 77,Vern,,,,\n")
    out = tmp_path / "shelfmark_tree.db"
    meta = builder.build(fist, libraries, out, log=lambda *_: None)
    s = ShelfmarkTreeService(str(out))
    try:
        places = [(p.leaf.how, [n.name_en for n in p.path]) for p in s.path_of("990000801010205171")]
        assert places == [("single", ["Vernadsky National Library", "Kiev"])]
        assert s.libraries_of_code("Vern") == ["lib:3"]
    finally:
        s.close()
    assert verifier.verify(str(out), int(meta["leaves_total"]), libraries, canaries=None) == []


def test_a_volumes_number_and_letter_and_a_number_after_a_word():
    N = builder.Node
    nodes = {"lib:1": N("lib:1", "library", None, "L", children=["col:1", "col:2"]),
             "col:1": N("col:1", "collection", "lib:1", "Gaster Hebrew ms. series"),
             "col:2": N("col:2", "collection", "lib:1", "NLI")}
    # Manchester: "Ms. Gaster 2110 A.1" is of FGP's "Gaster heb. ms 2110a", set off as its shelfmarks are
    _volume(nodes, "vol:a", "col:1", "Gaster heb. ms 2110a", leaves=[
        _leaf(f"a{i}", f"Gaster heb. ms 2110a/{i}", "fist", f"Gaster heb. ms 2110a/{i}") for i in (1, 2)])
    _volume(nodes, "vol:b", "col:1", "Gaster heb. ms 2110b", leaves=[
        _leaf("b1", "Gaster heb. ms 2110b/1", "fist", "Gaster heb. ms 2110b/1")])
    nodes["col:1"].leaves = [(((9,),), _leaf(n, n, "collection")) for n in
                             ("Ms. Gaster 2110 A.3", "Ms. Gaster 2110 A.4")]
    # NLI: volumes named by numbers; "Ms. Heb. 577.2.26=4" is "577.2/26"; a size mark is no part of a stem
    _volume(nodes, "vol:n", "col:2", "577.2", leaves=[
        _leaf(f"n{i}", f"577.2/{i}", "fist", f"577.2/{i}") for i in (1, 2)])
    _volume(nodes, "vol:m", "col:2", "577.10", leaves=[_leaf("m1", "577.10/1", "fist", "577.10/1")])
    nodes["col:2"].leaves = [(((9,),), _leaf(n, n, "collection")) for n in
                             ("Ms. Heb. 577.2.26=4", "Ms. Heb. 9463.1=4", "Ms. Heb. 9463.2=4")]
    builder.cohere(nodes)
    labels = lambda nid: [lf.label for _k, lf in nodes[nid].leaves]  # noqa: E731
    assert labels("vol:a") == ["Gaster heb. ms 2110a/1", "Gaster heb. ms 2110a/2",
                               "Gaster heb. ms 2110a/3", "Gaster heb. ms 2110a/4"]
    assert labels("vol:b") == ["Gaster heb. ms 2110b/1"] and not nodes["col:1"].leaves
    assert labels("vol:n") == ["577.2/1", "577.2/2", "577.2/26"]
    assert labels("vol:col:2:stem:msheb9463") == ["Ms. Heb. 9463.1=4", "Ms. Heb. 9463.2=4"]
    assert builder.stem_of("Ms. Heb. 9463.1=4") == "Ms. Heb. 9463"


def test_the_oxford_part_is_called_a_part_in_the_tooltip():
    from types import SimpleNamespace

    from web.components.shelfmark_tree_picker import _hint, _part
    labels = {"part": "Part: {part}"}
    leaf = SimpleNamespace(aliases="Ms. heb. a. 1.5 | The Bodleian Libraries, University of Oxford Ms. heb. a. 1.5",
                           label="MS heb. a.1/5", shelfmark="MS heb. a.1/5", part="MS. Heb. a. 1/2")
    assert _part(leaf, labels) == "Part: MS. Heb. a. 1/2"
    assert _hint(leaf, labels) == "Part: MS. Heb. a. 1/2 · Ms. heb. a. 1.5"
    leaf.part = ""
    assert _part(leaf, labels) == "" and _hint(leaf, labels) == "Ms. heb. a. 1.5"


def test_a_record_without_call_number_or_code_stands_in_the_library_named_for_it(tmp_path, monkeypatch):
    # owner, 2026-11-20: 11 records libraries.csv gives no code or call number are placed by the
    # library the catalogue's notes name; they are shown by their system number, not their title
    fist = make_fist(tmp_path / "FIST.db")
    libraries = make_libraries(tmp_path / "libraries.csv")
    with open(libraries, "a", encoding="utf-8", newline="\n") as f:
        f.write("990000811010205171,,,,,,,ספר המצוות.\n")          # a library of one collection
        f.write("990000812010205171,,,,,,,קטעי גניזה.\n")          # a library of several
        f.write("990000813010205171,,,,,,,\n")                      # a library FGP does not have; no title
    monkeypatch.setattr(builder, "RECORD_LIBRARY", {
        "990000811010205171": "Strasbourg", "990000812010205171": "CUL", "990000813010205171": "Carpi"})
    out = tmp_path / "shelfmark_tree.db"
    meta = builder.build(fist, libraries, out, log=lambda *_: None)
    s = ShelfmarkTreeService(str(out))
    try:
        def place(sid):
            (p,) = s.path_of(sid)
            return p.leaf.how, p.leaf.label, [n.name_en for n in p.path]
        assert place("990000811010205171") == (
            "single", "990000811010205171", ["National and University Library of Strasbourg", "Strasbourg"])
        assert place("990000812010205171") == (
            "other", "990000812010205171", ["Cambridge University Library", "Other shelfmarks"])
        how, label, path = place("990000813010205171")
        assert (how, label, path[0]) == ("other", "990000813010205171", "Jerusalem, D. Carpi Collection")
        carpi = next(n for n in s.children() if n.short == "Carpi")
        assert carpi.name_he == builder.EXTRA_LIBRARIES["Carpi"][1]
        # the record with no code that no library is named for stays where a reader can reach it
        (unknown,) = s.path_of(SYS["no_code"])
        assert unknown.path[0].name_en == builder.NO_CODE_NAME and unknown.path[0].name_he == builder.NO_CODE_NAME_HE
    finally:
        s.close()
    assert verifier.verify(str(out), int(meta["leaves_total"]), libraries, canaries=None) == []


def test_the_hebrew_library_names_are_written_one_way():
    # owner, 2026-11-20: one geresh character (U+0027, the one a keyboard types, so a filter finds
    # the name), single spaces, and "City, Library" for a library FGP has
    given = {lid: he for lid, (_en, he) in builder.LIBRARY_NAMES.items() if he}
    names = list(given.values()) + list(builder.CODE_NAMES_HE.values()) + [
        he for _en, he in builder.EXTRA_LIBRARIES.values()] + [builder.NO_CODE_NAME_HE]
    for name in names:
        assert "’" not in name and "׳" not in name and "  " not in name, name
    # a name with its city in English has it in Hebrew too ("Boesky Collection" has none in either)
    assert all("," in he for lid, he in given.items() if "," in builder.LIBRARY_NAMES[lid][0])
    assert given[411] == "מנצ'סטר, ספריית צ'תם" and given[300].startswith("לוס אנג'לס, ")
    assert given[130] == "פרנקפורט, ספריית האוניברסיטה"


def test_a_library_known_by_its_code_has_its_hebrew_name(tmp_path, monkeypatch):
    monkeypatch.setitem(builder.CODE_NAMES_HE, "Lutzki", "אוסף לוצקי")
    db, _libraries, _meta = build_fixture(tmp_path)
    s = ShelfmarkTreeService(str(db))
    try:
        assert s.node("code:Lutzki").name_he == "אוסף לוצקי"
    finally:
        s.close()


def _add_library(tmp_path, lid, short, name, cid, collection, rows, code, unplaced=()):
    """The fixture with one more library of one collection: ``rows`` are (inventory id, shelfmark,
    call numbers of the record FGP links to it); ``unplaced`` are call numbers of the library's
    records FGP places nowhere. Answers (FIST, libraries.csv, {call numbers: sys_id})."""
    fist = make_fist(tmp_path / "FIST.db")
    libraries = make_libraries(tmp_path / "libraries.csv")
    sids = {}
    con = sqlite3.connect(str(fist))
    con.execute("INSERT INTO CODE_Library VALUES (?,?,?,?,?,?)", (lid, short, name, "", 0, 0))
    con.execute("INSERT INTO CODE_Collection VALUES (?,?,?,?,?)", (cid, lid, collection, 1, 0))
    for n, (iid, shelf, calls) in enumerate(rows, 1):
        sid = sids[calls] = f"99{lid:04d}{n:03d}0205171"
        con.execute("INSERT INTO dbo_Inventory VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (iid, cid, None, shelf) + (None,) * 11 + (0, None))
        con.execute("INSERT INTO dbo_InventoryAlma (InventoryId, AlmaId) VALUES (?, ?)", (iid, int(sid)))
    con.commit()
    con.close()
    with open(libraries, "a", encoding="utf-8", newline="\n") as f:
        for n, calls in enumerate([r[2] for r in rows] + list(unplaced), 1):
            sid = sids.setdefault(calls, f"99{lid:04d}{n:03d}0205171")
            f.write(f'{sid},,"{calls}",{code},,,,\n')
    return fist, libraries, sids


def test_series_are_one_however_written():
    assert builder._series("Ms. FR") == builder._series("Fr.") == builder._series("MS FR") == "fr"
    assert builder._series("Ms.") == "ms" and builder._series("") == ""
    assert builder._series("Ms. GNB") != builder._series("Ms. FR")
    assert builder._series("Harkavi R") == builder._series("Ms. R")      # Kyiv: FGP's and the library's


def test_a_library_of_one_collection_is_shown_by_its_series(tmp_path, monkeypatch):
    # owner, 2026-11-20: Lehmann's FGP collection "Lehman" holds four of the library's shelfmarks,
    # of its "Ms. FR"; the library's records, coded by a collector's code, stood in "Other
    # shelfmarks". They go together, by series, one level up.
    from shared.shelfmark_tree_view import Position, crumbs, position
    fist, libraries, sids = _add_library(
        tmp_path, 5, "Lehman", "New York, M. Lehmann Collection", 50, "Lehman",
        [(5001, "Fr. 10/5", "Manfred and Anne Lehmann Foundation Ms. FR 10|Ms. FR 10"),
         (5002, "Fr. 27/26", "Fr. 27/26")],
        "Lehmann", unplaced=("Ms. FR 12", "Ms. GNB 13", "Manfred and Anne Lehmann Foundation Ms. GNB 15|Ms. GNB 15"))
    assert "Lehmann" in builder.COLLECTOR_CODES

    def build(series):
        monkeypatch.setattr(builder, "LIBRARY_SERIES", frozenset(series))
        out = tmp_path / f"tree{len(series)}.db"
        meta = builder.build(fist, libraries, out, log=lambda *_: None)
        assert verifier.verify(str(out), int(meta["leaves_total"]), libraries, canaries=None) == []
        return ShelfmarkTreeService(str(out))

    s = build(())
    try:
        assert s.path_of(sids["Ms. FR 12"])[0].leaf.how == "other"      # a collector's code: not placed
        assert [n.name_en for n in s.children("lib:5")] == ["Lehman", "Other shelfmarks"]
    finally:
        s.close()
    s = build({50})
    try:
        assert [n.id for n in s.children("lib:5")] == ["col:50"]
        series = s.children("col:50")
        assert [n.name_en for n in series] == ["Ms. FR", "Ms. GNB"]     # the library's writing, not "Fr."
        # FGP's "Fr. 27/26" among the library's "Ms. FR", in the order of the numbers
        assert [lf.label for lf in s.leaves(series[0].id, 0, 10)[0]] == ["Fr. 10/5", "Ms. FR 12", "Fr. 27/26"]
        assert [lf.label for lf in s.leaves(series[1].id, 0, 10)[0]] == ["Ms. GNB 13", "Ms. GNB 15"]
        # the library's one collection is no level of its own: its series stand under the library
        (place,) = s.path_of(sids["Fr. 27/26"])
        assert [n.name_en for n in crumbs(place)] == ["New York, M. Lehmann Collection", "Ms. FR"]
        assert position(s, place.leaf) == Position(3, 3)                  # in "Ms. FR", as shown
        # a typed shelfmark opens its series (PR review)
        from shared.shelfmark_tree_view import start_at
        assert [n.id for n in s.locate("Ms. FR 12").nodes] == [series[0].id]
        assert start_at(s, "Ms. FR 12").path[-1] == series[0].id
        assert place.leaf.how == "fist" and s.path_of(sids["Ms. FR 12"])[0].leaf.how == "single"
    finally:
        s.close()


def test_a_collection_divided_by_volume_number(tmp_path, monkeypatch):
    # owner, 2026-11-20: Toronto's "Friedberg MSS 9-004/1" .. "9-004/32" are in "9-004", beside
    # "9-001", "9-002", "9-003"
    from shared.shelfmark_tree_view import Position, crumbs, position
    assert builder._volume_of("Friedberg MSS 9-004/30b") == "Friedberg MSS 9-004"
    assert builder._volume_of("Friedberg MSS 9-002.1") == "Friedberg MSS 9-002"
    assert builder._volume_of("Friedberg MSS 9-001") == "Friedberg MSS 9-001"
    # Sofer's folios and "bis", Berlin Community's Roman parts; Guenzburg's own name does not count
    assert [builder._volume_of(x) for x in ("108 f. 16/1", "108 f. 9", "111/21bis", "A1 II", "A1.4")] == [
        "108", "108", "111", "A1", "A1"]
    own = builder._own_words("Guenzburg")
    assert builder._volume_key("Ms. Guenzburg 920", own) == builder._volume_key("Ms. 920", own) == "920"
    shelves = ["Friedberg MSS 9-004/10", "Friedberg MSS 9-001", "Friedberg MSS 9-004/2", "Friedberg MSS 9-002.1",
               "Friedberg MSS 9-004/1", "Friedberg MSS 9-002.2", "Friedberg MSS 9-004/30b"]
    fist, libraries, sids = _add_library(
        tmp_path, 6, "Friedberg", "Toronto, Friedberg Collection", 60, "Toronto",
        [(6001 + n, shelf, shelf.replace("Friedberg MSS", "Ms. FR")) for n, shelf in enumerate(shelves)], "Friedberg")

    def build(by_volume):
        monkeypatch.setattr(builder, "BY_VOLUME_NUMBER", frozenset(by_volume))
        out = tmp_path / f"tree{len(by_volume)}.db"
        meta = builder.build(fist, libraries, out, log=lambda *_: None)
        assert verifier.verify(str(out), int(meta["leaves_total"]), libraries, canaries=None) == []
        return ShelfmarkTreeService(str(out))

    s = build(())
    try:
        assert s.children("col:60") == [] and len(s.leaves("col:60", 0, 20)[0]) == len(shelves)
    finally:
        s.close()
    s = build({60})
    try:
        volumes = s.children("col:60")
        assert [n.name_en for n in volumes] == ["Friedberg MSS 9-001", "Friedberg MSS 9-002", "Friedberg MSS 9-004"]
        assert [lf.label for lf in s.leaves(volumes[2].id, 0, 10)[0]] == [
            "Friedberg MSS 9-004/1", "Friedberg MSS 9-004/2", "Friedberg MSS 9-004/10", "Friedberg MSS 9-004/30b"]
        (place,) = s.path_of(sids["Ms. FR 9-004/2"])
        assert [n.name_en for n in crumbs(place)] == ["Toronto, Friedberg Collection", "Friedberg MSS 9-004"]
        # its position counts the volume it is shown in, not the volumes before it (PR review)
        assert position(s, place.leaf) == Position(2, 4)
        # a typed shelfmark opens its volume (PR review)
        assert [n.id for n in s.locate("Friedberg MSS 9-004/2").nodes] == [volumes[2].id]
    finally:
        s.close()


def test_a_record_held_back_stays_out_of_a_library_shown_by_series(tmp_path, monkeypatch):
    # a record whose long form names another institution is not the library's (Kyiv kept the
    # Harkavy record that is the NLI's out of its series)
    fist, libraries, sids = _add_library(
        tmp_path, 5, "Lehman", "New York, M. Lehmann Collection", 50, "Lehman",
        [(5001, "Fr. 10/5", "Ms. FR 10"), (5002, "Fr. 27/26", "Fr. 27/26")],
        "Lehmann", unplaced=("Ms. FR 12", "Columbia University Library Ms. General 7|Ms. GNB 7"))
    monkeypatch.setattr(builder, "LIBRARY_SERIES", frozenset({50}))
    out = tmp_path / "tree.db"
    meta = builder.build(fist, libraries, out, log=lambda *_: None)
    s = ShelfmarkTreeService(str(out))
    try:
        assert s.path_of(sids["Ms. FR 12"])[0].leaf.how == "single"
        held = s.path_of(sids["Columbia University Library Ms. General 7|Ms. GNB 7"])[0]
        assert held.leaf.how == "other" and held.path[1].id == "other:lib:5"
    finally:
        s.close()
    assert verifier.verify(str(out), int(meta["leaves_total"]), libraries, canaries=None) == []


def test_a_catalogue_keeps_its_own_order(tmp_path):
    # PR review: FGP's OrderBy is the catalogue's order; Oxford's "2723.A" ... "2723.I" stand so,
    # where natural order reads the "I" as a Roman one and puts it first. An entry without one is last.
    fist = make_fist(tmp_path / "FIST.db")
    con = sqlite3.connect(str(fist))
    con.executescript("""
        CREATE TABLE CODE_Catalog (CatalogId INTEGER, CatAcronym TEXT, IsCanceledCode INTEGER);
        CREATE TABLE dbo_InventoryCatalogEntry (InventoryId INTEGER, CatalogId INTEGER, CatalogEntry TEXT,
                                                RecordStatus INTEGER, OrderBy INTEGER);
        INSERT INTO CODE_Catalog VALUES (7, 'N - C', 0);
        INSERT INTO dbo_InventoryCatalogEntry VALUES (1301, 7, '2723.I', 0, 9), (1302, 7, '2723.A', 0, 1),
            (1303, 7, '2723.B', 0, 2), (1304, 7, '2724', 0, NULL), (1305, 7, '2723.A', 0, 3);
    """)
    con.commit()
    con.close()
    libraries = make_libraries(tmp_path / "libraries.csv")
    out = tmp_path / "shelfmark_tree.db"
    meta = builder.build(fist, libraries, out, log=lambda *_: None)
    s = ShelfmarkTreeService(str(out))
    try:
        listed = next(c for c in s.children("col:11") if c.kind == "group")
        assert [e.name_en for e in s.children(listed.id)] == ["2723.A", "2723.B", "2723.I", "2724"]
        assert builder.roman_natural_key("2723.I") < builder.roman_natural_key("2723.A")   # what it overrides
    finally:
        s.close()
    assert verifier.verify(str(out), int(meta["leaves_total"]), libraries, canaries=None) == []


def test_a_whole_collection_is_taken_in_shelf_order_not_its_lists(tmp_path):
    # PR review: "Or." holds its shelfmarks and a catalogue's list of them, written before them; the
    # whole collection's manuscripts are in shelf order, the list's in the list's
    fist = make_fist(tmp_path / "FIST.db")
    con = sqlite3.connect(str(fist))
    con.executescript("""
        CREATE TABLE CODE_Catalog (CatalogId INTEGER, CatAcronym TEXT, IsCanceledCode INTEGER);
        CREATE TABLE dbo_InventoryCatalogEntry (InventoryId INTEGER, CatalogId INTEGER, CatalogEntry TEXT,
                                                RecordStatus INTEGER, OrderBy INTEGER);
        INSERT INTO CODE_Catalog VALUES (7, 'N - C', 0);
        INSERT INTO dbo_InventoryCatalogEntry VALUES (1403, 7, '1', 0, 1), (1401, 7, '2', 0, 2);
    """)
    con.commit()
    con.close()
    libraries = make_libraries(tmp_path / "libraries.csv")
    out = tmp_path / "shelfmark_tree.db"
    builder.build(fist, libraries, out, log=lambda *_: None)
    s = ShelfmarkTreeService(str(out))
    try:
        listed = next(c for c in s.children("col:12") if c.kind == "group")
        assert s.node(listed.id).lo < s.leaves("col:12", 0, 1)[0][0].pos           # the list comes first
        on_shelf = list(dict.fromkeys(lf.sys_id for lf in s.leaves("col:12", 0, 50)[0]))
        assert s.sys_ids_under("col:12") == on_shelf and on_shelf[:3] == SYS["or"][:3]
        assert s.sys_ids_under(listed.id) == [SYS["or"][2], SYS["or"][0]]       # Or. 3, then Or. 1
    finally:
        s.close()


def test_a_shelfmark_without_its_series_letter_stands_in_the_series_its_number_falls_in(monkeypatch):
    # owner, 2026-11-20: FGP's "Yevr. III 1155" belongs in "Yevr. III B 1001 - 1200", not in a range
    # of its own beside the series "A" and "B": B ends at 1154, A at 13
    monkeypatch.setattr(builder, "FAMILY_MIN", 3)
    monkeypatch.setattr(builder, "RANGE_SIZE", 6)

    def kl(label):
        return ((0,), builder.Leaf(label, label, label, label, 1, "fist", "", label))

    labels = ["Yevr. III 1155"] + [f"Yevr. III A {n}" for n in (1, 2, 13)] + [
        f"Yevr. III B {n}" for n in (1, 2, 1150, 1153, 1154)]
    chunks = builder._ranges_of([kl(x) for x in labels], "Yevr. III (Antonin)", families=True)
    assert [[k[1].label for k in chunk] for _name, chunk in chunks] == [
        ["Yevr. III A 1", "Yevr. III A 2", "Yevr. III A 13"],
        ["Yevr. III B 1", "Yevr. III B 2", "Yevr. III B 1150", "Yevr. III B 1153", "Yevr. III B 1154",
         "Yevr. III 1155"]]
    # a number no series of its form spans stays apart, as before
    chunks = builder._ranges_of([kl(x) for x in ["Yevr. III 5000"] + labels[1:]], "Yevr. III (Antonin)", True)
    assert chunks[-1][1][0][1].label == "Yevr. III 5000"


def test_the_empty_records_left_out():
    # owner, 2026-11-20: RNL Box D.37 and D.38 each had an empty record beside the right one; so do
    # 109 more ("Remove all empty records"). Each names the record of its shelfmark that stays.
    import csv

    from shared.sys_id_patterns import CORPUS_SYS_ID_RE
    with open(builder.LEFT_OUT_FILE, encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f))
    assert {"990053864070205171", "990053864090205171"} <= set(builder.LEFT_OUT) == {r["sys_id"] for r in rows}
    assert len(rows) == len(builder.LEFT_OUT) == 111
    for r in rows:
        assert CORPUS_SYS_ID_RE.fullmatch(r["sys_id"]) and CORPUS_SYS_ID_RE.fullmatch(r["kept"]), r
        assert r["kept"] not in builder.LEFT_OUT and r["shelfmark"].strip(), r


def test_a_catalogue_against_its_numbers_is_divided_as_it_stands(tmp_path, monkeypatch):
    # PR review: a catalogue whose order runs against its numbers ("3", "2", "1") is not re-sorted
    # by the ranges; Oxford's never does, and keeps its number ranges
    monkeypatch.setattr(builder, "VOLUME_RANGE_SIZE", 2)
    fist = make_fist(tmp_path / "FIST.db")
    con = sqlite3.connect(str(fist))
    con.executescript("""
        CREATE TABLE CODE_Catalog (CatalogId INTEGER, CatAcronym TEXT, IsCanceledCode INTEGER);
        CREATE TABLE dbo_InventoryCatalogEntry (InventoryId INTEGER, CatalogId INTEGER, CatalogEntry TEXT,
                                                RecordStatus INTEGER, OrderBy INTEGER);
        INSERT INTO CODE_Catalog VALUES (7, 'N - C', 0);
        INSERT INTO dbo_InventoryCatalogEntry VALUES (1301, 7, '3', 0, 1), (1302, 7, '2', 0, 2),
            (1303, 7, '1', 0, 3);
    """)
    con.commit()
    con.close()
    libraries = make_libraries(tmp_path / "libraries.csv")
    out = tmp_path / "shelfmark_tree.db"
    meta = builder.build(fist, libraries, out, log=lambda *_: None)
    s = ShelfmarkTreeService(str(out))
    try:
        listed = next(c for c in s.children("col:11") if c.kind == "group")
        parts = s.children(listed.id)
        assert [p.name_en for p in parts] == ["3 – 2", "1"]
        assert [e.name_en for p in parts for e in s.children(p.id)] == ["3", "2", "1"]
    finally:
        s.close()
    assert verifier.verify(str(out), int(meta["leaves_total"]), libraries, canaries=None) == []


def test_a_catalogue_keeps_an_entry_without_a_number_in_its_place(tmp_path, monkeypatch):
    # PR review: "1, Appendix A, 101, 201": the entry without a number stays where the catalogue has it
    monkeypatch.setattr(builder, "VOLUME_RANGE_SIZE", 2)
    fist = make_fist(tmp_path / "FIST.db")
    con = sqlite3.connect(str(fist))
    con.executescript("""
        CREATE TABLE CODE_Catalog (CatalogId INTEGER, CatAcronym TEXT, IsCanceledCode INTEGER);
        CREATE TABLE dbo_InventoryCatalogEntry (InventoryId INTEGER, CatalogId INTEGER, CatalogEntry TEXT,
                                                RecordStatus INTEGER, OrderBy INTEGER);
        INSERT INTO CODE_Catalog VALUES (7, 'N - C', 0);
        INSERT INTO dbo_InventoryCatalogEntry VALUES (1301, 7, '1', 0, 1), (1302, 7, 'Appendix A', 0, 2),
            (1303, 7, '101', 0, 3), (1304, 7, '201', 0, 4);
    """)
    con.commit()
    con.close()
    libraries = make_libraries(tmp_path / "libraries.csv")
    out = tmp_path / "shelfmark_tree.db"
    meta = builder.build(fist, libraries, out, log=lambda *_: None)
    s = ShelfmarkTreeService(str(out))
    try:
        listed = next(c for c in s.children("col:11") if c.kind == "group")
        entries = [e.name_en for p in s.children(listed.id) for e in (s.children(p.id) or [p])]
        assert entries == ["1", "Appendix A", "101", "201"]
    finally:
        s.close()
    assert verifier.verify(str(out), int(meta["leaves_total"]), libraries, canaries=None) == []


def test_a_kept_record_takes_the_fgp_row_of_the_empty_one(tmp_path, monkeypatch):
    # AIU "I.D.1": FGP linked its row to the empty record only; the record kept in its place, written
    # "Ms. I D 1", stands on that row in the volume, which would otherwise not be there at all
    kept = [f"9900009{n:02d}0205171" for n in range(1, 4)]
    left = dict(zip(SYS["as10"], kept))
    reasons = {sid: "empty" for sid in left}
    monkeypatch.setattr(builder, "LEFT_OUT", reasons)
    monkeypatch.setattr(builder, "REPLACED_BY", left)
    monkeypatch.setattr(verifier, "LEFT_OUT", reasons)
    fist = make_fist(tmp_path / "FIST.db")
    libraries = make_libraries(tmp_path / "libraries.csv")
    with open(libraries, "a", encoding="utf-8", newline="\n") as f:
        for n, sid in enumerate(kept, 1):
            f.write(f"{sid},,Ms. AS ten/{n},CUL,,,,\n")
    out = tmp_path / "shelfmark_tree.db"
    meta = builder.build(fist, libraries, out, log=lambda *_: None)
    s = ShelfmarkTreeService(str(out))
    try:
        assert all(s.path_of(sid) == [] for sid in left)
        places = [s.path_of(sid)[0] for sid in kept]
        assert [(p.leaf.how, p.leaf.label, p.path[-1].name_en) for p in places] == [
            ("fist", f"T-S AS 10.{n}", "T-S AS 10") for n in (1, 2, 3)]
    finally:
        s.close()
    assert verifier.verify(str(out), int(meta["leaves_total"]), libraries, canaries=None) == []


def _catalogue_build(tmp_path, rows):
    fist = make_fist(tmp_path / "FIST.db")
    con = sqlite3.connect(str(fist))
    con.executescript("""
        CREATE TABLE CODE_Catalog (CatalogId INTEGER, CatAcronym TEXT, IsCanceledCode INTEGER);
        CREATE TABLE dbo_InventoryCatalogEntry (InventoryId INTEGER, CatalogId INTEGER, CatalogEntry TEXT,
                                                RecordStatus INTEGER, OrderBy INTEGER);
        INSERT INTO CODE_Catalog VALUES (7, 'N - C', 0);
    """)
    con.executemany("INSERT INTO dbo_InventoryCatalogEntry VALUES (?, 7, ?, 0, ?)", rows)
    con.commit()
    con.close()
    libraries = make_libraries(tmp_path / "libraries.csv")
    out = tmp_path / "shelfmark_tree.db"
    meta = builder.build(fist, libraries, out, log=lambda *_: None)
    return out, libraries, meta


def test_a_catalogue_entry_without_a_number_at_its_end_stays_at_the_end(tmp_path, monkeypatch):
    # PR review: "1, 101, 201, Appendix A": the entry without a number came before the ranges
    monkeypatch.setattr(builder, "VOLUME_RANGE_SIZE", 2)
    out, libraries, meta = _catalogue_build(tmp_path, [(1301, "1", 1), (1302, "101", 2), (1303, "201", 3),
                                                       (1304, "Appendix A", 4)])
    s = ShelfmarkTreeService(str(out))
    try:
        listed = next(c for c in s.children("col:11") if c.kind == "group")
        kids = s.children(listed.id)
        assert [k.kind for k in kids] == ["group", "group", "volume"]                # two ranges, then it
        assert [e.name_en for k in kids for e in (s.children(k.id) or [k])] == ["1", "101", "201", "Appendix A"]
    finally:
        s.close()
    assert verifier.verify(str(out), int(meta["leaves_total"]), libraries, canaries=None) == []


def test_two_catalogue_entries_written_alike_keep_a_place_each(tmp_path):
    # PR review: one row under "2603.1" and "2603-1" (alike but for punctuation) made one id twice and
    # the build failed
    out, libraries, meta = _catalogue_build(tmp_path, [(1301, "2603.1", 1), (1301, "2603-1", 2)])
    s = ShelfmarkTreeService(str(out))
    try:
        assert [p.leaf.how for p in s.path_of(SYS["ns5"][0])] == ["fist", "catalogue", "catalogue"]
    finally:
        s.close()
    assert verifier.verify(str(out), int(meta["leaves_total"]), libraries, canaries=None) == []
