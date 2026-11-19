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
from scripts import verify_shelfmark_tree as verifier
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
        "fist": "39", "exact": "1", "collection": "3", "single": "1", "other": "5"}


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
    assert labels.index("T-S AS 2.9") == len(labels) - 1


def test_number_boundaries_are_kept(svc):
    # T-S AS 29 is not T-S AS 2.9: the collection head places it, outside any volume (O4)
    assert _places(svc, "boundary") == [("collection", ["Cambridge University Library", "T-S AS"])]


def test_collection_head(svc):
    assert _places(svc, "by_head") == [("collection", ["Cambridge University Library", "Or."])]
    assert _places(svc, "inactive_row") == [("collection", ["Cambridge University Library", "T-S AS"])]


def test_another_institution_goes_to_other(svc):
    assert _places(svc, "foreign") == [("other", ["Cambridge University Library", "Other shelfmarks"])]


def test_collision_goes_to_other(svc):
    assert _places(svc, "collision") == [("other", ["Cambridge University Library", "Other shelfmarks"])]


def test_single_collection_but_not_for_a_collector(svc):
    assert _places(svc, "single") == [("single", ["National and University Library of Strasbourg", "Strasbourg"])]
    assert _places(svc, "collector") == [("other", ["Vernadsky National Library", "Other shelfmarks"])]


def test_every_record_is_reachable(svc):
    assert _places(svc, "no_code") == [("other", ["No library code", "Other shelfmarks"])]
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
    monkeypatch.setattr(verifier, "RANGE_MAX", 4)
    monkeypatch.setattr(verifier, "RANGE_SIZE", 3)
    db, libraries, meta = build_fixture(tmp_path)
    s = ShelfmarkTreeService(str(db))
    try:
        vol = s.locate("T-S AS 2").nodes[0]
        ranges = s.children(vol.id)
        assert [r.kind for r in ranges] == ["range"] * 4 and vol.direct_leaves == 0
        assert ranges[0].name_en == "T-S AS 2.1 – T-S AS 2.3"
        last = s.leaves(ranges[0].id, 2, 1)[0][0]
        nxt = s.neighbours(last.id)[1]
        assert nxt.node_id == ranges[1].id                     # previous / next cross ranges
    finally:
        s.close()
    assert verifier.verify(str(db), int(meta["leaves_total"]), libraries, canaries=None) == []


def test_libraries_by_size(tmp_path, monkeypatch):
    monkeypatch.setattr(builder, "TOP_LIBRARY_MIN", 7)
    db, _libraries, _meta = build_fixture(tmp_path)
    s = ShelfmarkTreeService(str(db))
    try:
        shorts = [n.short for n in s.children()]
    finally:
        s.close()
    assert shorts[:3] == ["CUL", "Kiev", "Strasbourg"]          # 7 or more, largest first (a tie A-Z)
    assert shorts[3:] == sorted(shorts[3:], key=str.casefold)   # then A-Z


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
    assert svc.leaves(vol.id, 0, 10, filter="2.9")[1] == 1
    assert svc.leaves(vol.id, 0, 10, filter="%")[1] == 0          # LIKE wildcards are literal


def test_neighbours_cross_volumes_and_stop_at_the_library(svc):
    first = svc.leaves(svc.locate("T-S AS 10").nodes[0].id, 0, 1)[0][0]
    prev, nxt = svc.neighbours(first.id)
    assert prev.shelfmark == "T-S AS 2.9" and nxt.shelfmark == "T-S AS 10.2"
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
