# -*- coding: utf-8 -*-
"""Gate tests for scripts/build_work_registry.py.

Every gate is proven able to FAIL (mutation fixtures), and the green fixture
proves the happy path builds the expected registry shape.
"""
import hashlib
import json
import os
import sqlite3
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__))), "scripts"))
from build_work_registry import (  # noqa: E402
    GateError, build, check_membership_gates, kw_id_of)


def alias_sha(links):
    ser = json.dumps(sorted((r, b, k) for r, b, k in links),
                     ensure_ascii=False).encode("utf-8")
    return hashlib.sha256(ser).hexdigest()


def write_inputs(d, *, links, families=None, groups=None, questions=None,
                 shares=None, scope=None, merges=None, non_mat=None,
                 author_rulings=None):
    docs = {
        "author_authority.json": {
            "version": 1, "variants": {}, "fills": {},
            "owner_author_rulings": author_rulings or {},
            "owner_title_rulings": {}},
        "census_members.json": {
            "version": 1, "merges": merges or [], "dropped_by_135": [],
            "non_materialized_members": non_mat or []},
        "container_families.json": {
            "version": 1, "families": families or [], "work_groups": groups or {},
            "owner_questions": questions or [], "shares_material_edges": shares or [],
            "alias_fact_sha256": alias_sha(links), "alias_fact_count": len(links)},
        "scope_map.json": {"version": 1, "containers": scope or {}},
    }
    pins = {"version": 1, "files": {}}
    for name, doc in docs.items():
        data = json.dumps(doc, ensure_ascii=False, sort_keys=True).encode("utf-8")
        with open(os.path.join(d, name), "wb") as f:
            f.write(data)
        pins["files"][name] = hashlib.sha256(data).hexdigest()
    with open(os.path.join(d, "PINS.json"), "w", encoding="utf-8") as f:
        json.dump(pins, f)


def make_db(path, rows, links):
    con = sqlite3.connect(path)
    con.execute("CREATE TABLE review_row(work_id TEXT, work_title TEXT, "
                "work_author TEXT, source_corpus TEXT, locus_label TEXT)")
    con.execute("CREATE TABLE work_alias_fact(rs_work TEXT, base_work TEXT, "
                "kind TEXT, source TEXT, shared_pages INTEGER)")
    con.execute("CREATE TABLE meta(key TEXT PRIMARY KEY, value TEXT)")
    con.executemany("INSERT INTO review_row VALUES (?,?,?,?,?)", rows)
    con.executemany("INSERT INTO work_alias_fact VALUES (?,?,?,'gate',1)", links)
    con.commit()
    con.close()


BASE_ROWS = [
    # anthology container rsA: two divisions
    ("rsA", "אסופה", None, "rsource", "אסופה, חלק ראשון, פרק א"),
    ("rsA", "אסופה", None, "rsource", "אסופה, חלק שני, פרק א"),
    # container-is-work rsB + its part wP1
    ("rsB", "חיבור גדול", "מחבר אחד", "rsource", "חיבור גדול, שער א"),
    ("wP1", "חיבור גדול, שער א", "מחבר אחד", "msource", None),
    # census pair
    ("wC1", "מדרש פלוני", None, "msource", None),
    ("wC2", "מדרש פלוני", None, "sefaria", None),
    # singleton, also the target of rsA's first division
    ("wS", "חלק ראשון", None, "msource", None),
    # plain same_work pair
    ("rsW", "ספר מוכר", "פלוני בן אלמוני", "rsource", "ספר מוכר, א"),
    ("wW", "ספר מוכר", "פלוני בן אלמוני", "sefaria", None),
]
BASE_LINKS = [("rsB", "wP1", "same_work_contains"), ("rsW", "wW", "same_work")]
BASE_FAMILIES = [
    {"container": "rsB", "family": "חיבור גדול", "class": "container_is_work",
     "contains_parts": ["wP1"], "same_work_partners": []},
    {"container": "rsA", "family": "אסופה", "class": "anthology",
     "contains_parts": [], "same_work_partners": []},
]
BASE_SCOPE = {"rsA": [
    {"prefix": "אסופה, חלק ראשון", "target": {"work": "wS"},
     "basis": "division_title", "target_title": "חלק ראשון", "rows_matched": 1},
    {"prefix": "אסופה, חלק שני", "target": {"mint": "חלק שני"},
     "basis": "division_title", "rows_matched": 1},
]}
BASE_MERGES = [{"members": ["wC1", "wC2"], "canonical": "wC2", "not_in_review_db": []}]


def base_inputs(inputs, links=BASE_LINKS, **kw):
    kw.setdefault("families", BASE_FAMILIES)
    kw.setdefault("scope", BASE_SCOPE)
    kw.setdefault("merges", BASE_MERGES)
    write_inputs(inputs, links=links, **kw)


@pytest.fixture
def world(tmp_path):
    db = str(tmp_path / "t.db")
    inputs = str(tmp_path / "inputs")
    os.makedirs(inputs)
    make_db(db, BASE_ROWS, BASE_LINKS)
    base_inputs(inputs)
    return db, inputs


def repin(inputs, name, mutate):
    """Apply `mutate` to one pinned doc and RE-PIN it, so what refuses the
    build is the input contract and never the hash gate."""
    path = os.path.join(inputs, name)
    doc = json.loads(open(path, encoding="utf-8").read())
    mutate(doc)
    data = json.dumps(doc, ensure_ascii=False, sort_keys=True).encode("utf-8")
    with open(path, "wb") as f:
        f.write(data)
    pins_path = os.path.join(inputs, "PINS.json")
    pins = json.loads(open(pins_path, encoding="utf-8").read())
    pins["files"][name] = hashlib.sha256(data).hexdigest()
    with open(pins_path, "w", encoding="utf-8") as f:
        json.dump(pins, f)


def add_db(db, rows=(), links=()):
    con = sqlite3.connect(db)
    con.executemany("INSERT INTO review_row VALUES (?,?,?,?,?)", rows)
    con.executemany("INSERT INTO work_alias_fact VALUES (?,?,?,'gate',1)", links)
    con.commit()
    con.close()


def test_green_build_shape(world):
    db, inputs = world
    build(db, inputs, say=lambda *a: None)
    con = sqlite3.connect(db)
    kw = {t: (k, a, mw, ms) for k, t, a, mw, ms in con.execute(
        "SELECT kw_id, title, author, main_witness_work, main_witness_scope "
        "FROM known_work")}
    fam_id, fam_author, fam_main, fam_scope = kw["חיבור גדול"]
    assert fam_author == "מחבר אחד" and (fam_main, fam_scope) == ("rsB", "whole")
    mems = dict(con.execute(
        "SELECT work_id, scope FROM known_work_member WHERE kw_id=?", (fam_id,)))
    assert mems == {"rsB": "whole", "wP1": "חיבור גדול, שער א"}
    assert kw["מדרש פלוני"][0] == kw_id_of("work:wC2")
    ws_mems = sorted(con.execute(
        "SELECT work_id, scope FROM known_work_member WHERE kw_id=?",
        (kw["חלק ראשון"][0],)))
    assert ("rsA", "אסופה, חלק ראשון") in ws_mems and ("wS", "whole") in ws_mems
    assert "חלק שני" in kw
    assert not con.execute("SELECT 1 FROM known_work_member WHERE work_id='rsA' "
                           "AND scope='whole'").fetchall()
    con.close()


def test_pin_tamper_fails(world):
    db, inputs = world
    with open(os.path.join(inputs, "container_families.json"), "ab") as f:
        f.write(b" ")
    with pytest.raises(GateError, match="PIN MISMATCH"):
        build(db, inputs, say=lambda *a: None)


def test_pins_trusted_root_fails(world):
    db, inputs = world
    with pytest.raises(GateError, match="trusted root"):
        build(db, inputs, say=lambda *a: None, pins_sha256="0" * 64)


def test_unruled_question_refused(world):
    db, inputs = world
    base_inputs(inputs, questions=[
        {"key": "q", "status": "pending", "ruling": "-", "affected": []}])
    with pytest.raises(GateError, match="not ruled"):
        build(db, inputs, say=lambda *a: None)


def test_alias_fact_drift_fails(world):
    db, inputs = world
    add_db(db, links=[("rsW", "wS", "same_work")])  # db changed AFTER pinning
    with pytest.raises(GateError, match="drifted"):
        build(db, inputs, say=lambda *a: None)


def test_same_work_edge_on_anthology_fails(world):
    db, inputs = world
    links = BASE_LINKS + [("rsA", "wS", "same_work")]
    add_db(db, links=links[-1:])
    base_inputs(inputs, links=links)
    with pytest.raises(GateError, match="anthology"):
        build(db, inputs, say=lambda *a: None)


def test_part_group_mix_fails(world):
    """HIGH-1a: a cluster that is both a family part and a work-group member
    must fail, not silently follow the part branch."""
    db, inputs = world
    add_db(db, rows=[("wX", "חיבור גדול, שער א", "מחבר אחד", "sefaria", None)])
    base_inputs(inputs, groups={
        "קבוצה": {"members": {"wP1": "א", "wX": "ב"},
                  "overrides_census": False, "ruling": "owner"}})
    with pytest.raises(GateError, match="mixes identity categories"):
        build(db, inputs, say=lambda *a: None)


def test_external_canonical_in_override_group_fails(world):
    """HIGH-1b: overrides_census exempts ONLY the group's named members; an
    external census canonical merged in must still fail."""
    db, inputs = world
    add_db(db, rows=[("wC3", "מדרש אחר", None, "sefaria", None)])
    links = BASE_LINKS + [("wC2", "wC3", "same_work")]
    add_db(db, links=links[-1:])
    merges = BASE_MERGES + [{"members": ["wC3"], "canonical": "wC3",
                             "not_in_review_db": []}]
    base_inputs(inputs, links=links, merges=merges, groups={
        "מדרש פלוני": {"members": {"wC1": "whole", "wC2": "whole"},
                       "overrides_census": True, "ruling": "owner"}})
    with pytest.raises(GateError, match="census canonicals"):
        build(db, inputs, say=lambda *a: None)


def test_census_reconciliation_fails(world):
    """MED-4: a db-absent census member NOT pinned as non-materialized fails."""
    db, inputs = world
    merges = [{"members": ["wC1", "wC2", "wGhost"], "canonical": "wC2",
               "not_in_review_db": ["wGhost"]}]
    base_inputs(inputs, merges=merges, non_mat=[])
    with pytest.raises(GateError, match="reconciliation"):
        build(db, inputs, say=lambda *a: None)


def test_target_title_assertion_fails(world):
    """MED-6: a mis-keyed target id behind a right-looking prefix fails."""
    db, inputs = world
    scope = {"rsA": [
        {"prefix": "אסופה, חלק ראשון", "target": {"work": "wS"},
         "basis": "division_title", "target_title": "כותרת אחרת",
         "rows_matched": 1},
        BASE_SCOPE["rsA"][1],
    ]}
    base_inputs(inputs, scope=scope)
    with pytest.raises(GateError, match="pinned title"):
        build(db, inputs, say=lambda *a: None)


def test_verified_link_claim_without_edge_fails(world):
    db, inputs = world
    scope = {"rsA": [
        {"prefix": "אסופה, חלק ראשון", "target": {"work": "wS"},
         "basis": "verified_link", "target_title": "חלק ראשון",
         "rows_matched": 1},
        BASE_SCOPE["rsA"][1],
    ]}
    base_inputs(inputs, scope=scope)
    with pytest.raises(GateError, match="verified_link"):
        build(db, inputs, say=lambda *a: None)


def test_unrouted_container_row_fails(world):
    db, inputs = world
    add_db(db, rows=[("rsA", "אסופה", None, "rsource", "אסופה, חלק שלישי, פרק א")])
    with pytest.raises(GateError, match="unrouted"):
        build(db, inputs, say=lambda *a: None)


def test_author_conflict_fails(world):
    db, inputs = world
    con = sqlite3.connect(db)
    con.execute("UPDATE review_row SET work_author='אחר לגמרי' WHERE work_id='wW'")
    con.commit()
    con.close()
    with pytest.raises(GateError, match="conflicting authors"):
        build(db, inputs, say=lambda *a: None)


def test_intra_work_ragged_author_fails(world):
    """MED-2: a work carrying two different author strings must fail, not be
    hidden by MIN()."""
    db, inputs = world
    add_db(db, rows=[("wW", "ספר מוכר", "מישהו אחר", "sefaria", None)])
    with pytest.raises(GateError, match="mixed title/author"):
        build(db, inputs, say=lambda *a: None)


def test_owner_author_ruling_wins(world):
    """MED-5: the pinned authority is the source of the known-work author."""
    db, inputs = world
    base_inputs(inputs, author_rulings={"rsW": None, "wW": None})
    build(db, inputs, say=lambda *a: None)
    con = sqlite3.connect(db)
    a, basis = con.execute("SELECT author, author_basis FROM known_work "
                           "WHERE title='ספר מוכר'").fetchone()
    assert a is None and basis == "owner_ruling"
    con.close()


def test_non_materialized_member_asserted_not_minted(world):
    db, inputs = world
    merges = [{"members": ["wC1", "wC2", "wGhost"], "canonical": "wC2",
               "not_in_review_db": ["wGhost"]}]
    base_inputs(inputs, merges=merges, non_mat=["wGhost"])
    build(db, inputs, say=lambda *a: None)
    con = sqlite3.connect(db)
    kw_id, = con.execute("SELECT kw_id FROM known_work_assertion "
                         "WHERE work_id='wGhost'").fetchone()
    assert kw_id == kw_id_of("work:wC2")
    assert not con.execute("SELECT 1 FROM known_work_member "
                           "WHERE work_id='wGhost'").fetchall()
    con.close()


def test_overlapping_groups_fail(world):
    """Round-2 HIGH: a work in two work-groups must fail, not silently take
    the last group's identity."""
    db, inputs = world
    base_inputs(inputs, groups={
        "קבוצה א": {"members": {"wC1": "whole"}, "overrides_census": False,
                    "ruling": "owner"},
        "קבוצה ב": {"members": {"wC1": "whole"}, "overrides_census": False,
                    "ruling": "owner"}})
    with pytest.raises(GateError, match="two work-groups"):
        build(db, inputs, say=lambda *a: None)


def test_census_canonical_not_member_fails(world):
    db, inputs = world
    merges = [{"members": ["wC1"], "canonical": "wC2", "not_in_review_db": []}]
    base_inputs(inputs, merges=merges)
    with pytest.raises(GateError, match="not a member of its own merge group"):
        build(db, inputs, say=lambda *a: None)


def test_unknown_scope_container_fails(world):
    """A scoped container with no rows at all is a fake witness (declared as an
    anthology family so the anthology-set equality gate is not what fires)."""
    db, inputs = world
    scope = dict(BASE_SCOPE, rsZ=[
        {"prefix": "כלום", "target": {"mint": "כלום"}, "basis": "division_title",
         "rows_matched": 1}])
    fams = BASE_FAMILIES + [
        {"container": "rsZ", "family": "כלום", "class": "anthology",
         "contains_parts": [], "same_work_partners": []}]
    base_inputs(inputs, scope=scope, families=fams)
    with pytest.raises(GateError, match="does not exist in the db"):
        build(db, inputs, say=lambda *a: None)


def test_bad_relation_endpoint_fails(world):
    db, inputs = world
    base_inputs(inputs, shares=[{"a": "wNope", "b": "wS", "note": "x"}])
    with pytest.raises(GateError, match="endpoint"):
        build(db, inputs, say=lambda *a: None)


def test_pending_owner_provenance_preserved(world):
    """Round-2 MED-4: a pending_owner mint stays visibly provisional."""
    db, inputs = world
    scope = {"rsA": [
        BASE_SCOPE["rsA"][0],
        {"prefix": "אסופה, חלק שני", "target": {"mint": "חלק שני"},
         "basis": "pending_owner", "rows_matched": 1},
    ]}
    base_inputs(inputs, scope=scope)
    build(db, inputs, say=lambda *a: None)
    con = sqlite3.connect(db)
    prov, = con.execute("SELECT provisional FROM known_work "
                        "WHERE title='חלק שני'").fetchone()
    rb, = con.execute("SELECT route_basis FROM known_work_member "
                      "WHERE work_id='rsA' AND scope='אסופה, חלק שני'").fetchone()
    assert prov == 1 and rb == "pending_owner"
    con.close()


def test_dead_prefix_fails(world):
    """Round-3 HIGH-1: a prefix routing zero rows must not mint a phantom."""
    db, inputs = world
    scope = {"rsA": BASE_SCOPE["rsA"] + [
        {"prefix": "אסופה, חלק רביעי", "target": {"mint": "רפאים"},
         "basis": "division_title", "rows_matched": 0}]}
    base_inputs(inputs, scope=scope)
    with pytest.raises(GateError, match="rows_matched"):
        build(db, inputs, say=lambda *a: None)


def test_rows_matched_drift_fails(world):
    """Round-3 HIGH-1: recomputed routing must equal the pinned counts."""
    db, inputs = world
    scope = {"rsA": [dict(BASE_SCOPE["rsA"][0], rows_matched=2),
                     BASE_SCOPE["rsA"][1]]}
    base_inputs(inputs, scope=scope)
    with pytest.raises(GateError, match="rows_matched"):
        build(db, inputs, say=lambda *a: None)


def test_contains_closure_fails(world):
    """Round-3 HIGH-2: pinned contains_parts must equal the live facts."""
    db, inputs = world
    fams = [dict(BASE_FAMILIES[0], contains_parts=[]), BASE_FAMILIES[1]]
    base_inputs(inputs, families=fams)
    with pytest.raises(GateError, match="live containment facts"):
        build(db, inputs, say=lambda *a: None)


def test_undeclared_container_fails(world):
    """Round-3 HIGH-2: a containment fact without a family declaration fails."""
    db, inputs = world
    base_inputs(inputs, families=[BASE_FAMILIES[1]])
    with pytest.raises(GateError, match="no family declaration"):
        build(db, inputs, say=lambda *a: None)


def test_fabricated_family_container_fails(world):
    """Round-3 HIGH-2: an unknown container cannot claim a real part."""
    db, inputs = world
    fams = BASE_FAMILIES + [{"container": "rsZ", "family": "בדיה",
                             "class": "container_is_work",
                             "contains_parts": ["wS"], "same_work_partners": []}]
    base_inputs(inputs, families=fams)
    with pytest.raises(GateError, match="does not exist in the db"):
        build(db, inputs, say=lambda *a: None)


def test_ghost_group_member_fails(world):
    db, inputs = world
    base_inputs(inputs, groups={
        "רפאים": {"members": {"wGhost": "whole"}, "overrides_census": False,
                  "ruling": "owner"}})
    with pytest.raises(GateError, match="does not exist in the db"):
        build(db, inputs, say=lambda *a: None)


def test_ghost_alias_endpoint_fails(world):
    db, inputs = world
    links = BASE_LINKS + [("rsW", "wGhost", "same_work")]
    add_db(db, links=links[-1:])
    base_inputs(inputs, links=links)
    with pytest.raises(GateError, match="no evidence rows"):
        build(db, inputs, say=lambda *a: None)


def test_unknown_family_class_fails(world):
    """Round-4 HIGH-1: a typo'd class must not drop the family silently."""
    db, inputs = world
    fams = [dict(BASE_FAMILIES[0], **{"class": "containr_is_work"}),
            BASE_FAMILIES[1]]
    base_inputs(inputs, families=fams)
    with pytest.raises(GateError, match="class must be one of"):
        build(db, inputs, say=lambda *a: None)


def test_duplicate_container_declaration_fails(world):
    db, inputs = world
    base_inputs(inputs, families=BASE_FAMILIES + [BASE_FAMILIES[0]])
    with pytest.raises(GateError, match="duplicate family declaration"):
        build(db, inputs, say=lambda *a: None)


def test_part_in_two_families_fails(world):
    """Round-4 HIGH-2: same family key may share parts, distinct keys may not."""
    db, inputs = world
    add_db(db, rows=[("rsC", "חיבור אחר", None, "rsource", "חיבור אחר, שער א")],
           links=[("rsC", "wP1", "same_work_contains")])
    links = BASE_LINKS + [("rsC", "wP1", "same_work_contains")]
    fams = BASE_FAMILIES + [{"container": "rsC", "family": "חיבור אחר",
                             "class": "container_is_work",
                             "contains_parts": ["wP1"], "same_work_partners": []}]
    base_inputs(inputs, links=links, families=fams)
    with pytest.raises(GateError, match="claimed by two families"):
        build(db, inputs, say=lambda *a: None)


def test_absent_member_two_canonicals_fails(world):
    """Round-4 HIGH-3: an absent member asserting to two known works fails."""
    db, inputs = world
    merges = [{"members": ["wC1", "wGhost"], "canonical": "wC1",
               "not_in_review_db": ["wGhost"]},
              {"members": ["wC2", "wGhost"], "canonical": "wC2",
               "not_in_review_db": ["wGhost"]}]
    base_inputs(inputs, merges=merges, non_mat=["wGhost"])
    with pytest.raises(GateError, match="asserts to 2 known works"):
        build(db, inputs, say=lambda *a: None)


def test_anthology_in_census_merge_fails(world):
    """Round-4 HIGH-4: anthology containers have no whole-work identity."""
    db, inputs = world
    merges = BASE_MERGES + [{"members": ["rsA", "wS"], "canonical": "wS",
                             "not_in_review_db": []}]
    base_inputs(inputs, merges=merges)
    with pytest.raises(GateError, match="anthology container"):
        build(db, inputs, say=lambda *a: None)


def test_anthology_in_work_group_fails(world):
    db, inputs = world
    base_inputs(inputs, groups={
        "קבוצה": {"members": {"rsA": "whole", "wS": "whole"},
                  "overrides_census": False, "ruling": "owner"}})
    with pytest.raises(GateError, match="anthology container"):
        build(db, inputs, say=lambda *a: None)


def test_absent_canonical_fails(world):
    """MED-4: an assertion may never target a non-materialized known work."""
    db, inputs = world
    merges = BASE_MERGES + [{"members": ["wGhost"], "canonical": "wGhost",
                             "not_in_review_db": ["wGhost"]}]
    base_inputs(inputs, merges=merges, non_mat=["wGhost"])
    with pytest.raises(GateError, match="no materialized known work"):
        build(db, inputs, say=lambda *a: None)


def test_ambiguous_scope_target_fails(world):
    """Round-5 HIGH-1: two target keys silently took the 'work' branch."""
    db, inputs = world
    scope = {"rsA": [BASE_SCOPE["rsA"][0],
                     dict(BASE_SCOPE["rsA"][1],
                          target={"work": "wS", "mint": "חלק שני"})]}
    base_inputs(inputs, scope=scope)
    with pytest.raises(GateError, match="exactly one of work/group/mint"):
        build(db, inputs, say=lambda *a: None)


def test_empty_mint_target_fails(world):
    """Round-5 HIGH-1: an empty mint minted a titleless known work."""
    db, inputs = world
    scope = {"rsA": [BASE_SCOPE["rsA"][0],
                     dict(BASE_SCOPE["rsA"][1], target={"mint": ""})]}
    base_inputs(inputs, scope=scope)
    with pytest.raises(GateError, match="target value must be a non-blank string"):
        build(db, inputs, say=lambda *a: None)


def test_unknown_scope_target_key_fails(world):
    """Round-5 HIGH-1: an unknown target key must not fall through to mint."""
    db, inputs = world
    scope = {"rsA": [BASE_SCOPE["rsA"][0],
                     dict(BASE_SCOPE["rsA"][1], target={"kw": "חלק שני"})]}
    base_inputs(inputs, scope=scope)
    with pytest.raises(GateError, match="target keys wrong"):
        build(db, inputs, say=lambda *a: None)


def test_empty_scope_prefix_fails(world):
    """Round-5 HIGH-2: an empty prefix matched every label."""
    db, inputs = world
    scope = {"rsA": [BASE_SCOPE["rsA"][0],
                     dict(BASE_SCOPE["rsA"][1], prefix="", rows_matched=1)]}
    base_inputs(inputs, scope=scope)
    with pytest.raises(GateError, match="prefix must be a non-blank string"):
        build(db, inputs, say=lambda *a: None)


def test_whole_scope_prefix_fails(world):
    """Round-5 HIGH-2: a routed part may not claim the whole-work scope."""
    db, inputs = world
    scope = {"rsA": [BASE_SCOPE["rsA"][0],
                     dict(BASE_SCOPE["rsA"][1], prefix="whole")]}
    base_inputs(inputs, scope=scope)
    with pytest.raises(GateError, match="may never claim the whole-work scope"):
        build(db, inputs, say=lambda *a: None)


def test_duplicate_scope_prefix_fails(world):
    """Round-5 HIGH-2: a repeated prefix double-counts the same rows."""
    db, inputs = world
    scope = {"rsA": BASE_SCOPE["rsA"] + [
        dict(BASE_SCOPE["rsA"][1], target={"mint": "כפול"})]}
    base_inputs(inputs, scope=scope)
    with pytest.raises(GateError, match="duplicate prefix"):
        build(db, inputs, say=lambda *a: None)


def test_null_locus_label_in_container_fails(world):
    """Round-5 HIGH-2: a NULL label is unroutable, never silently ''-matched."""
    db, inputs = world
    add_db(db, rows=[("rsA", "אסופה", None, "rsource", None)])
    with pytest.raises(GateError, match="no locus_label"):
        build(db, inputs, say=lambda *a: None)


def test_scope_entry_for_non_anthology_work_fails(world):
    """Round-6 HIGH-1: a scoped non-anthology work is counted twice."""
    db, inputs = world
    add_db(db, rows=[("wP1", "חיבור גדול, שער א", "מחבר אחד", "msource",
                      "חיבור גדול, שער א, סימן ב")])
    scope = dict(BASE_SCOPE, wP1=[
        {"prefix": "חיבור גדול, שער א, סימן ב", "target": {"mint": "סימן ב"},
         "basis": "division_title", "rows_matched": 1}])
    base_inputs(inputs, scope=scope)
    with pytest.raises(GateError, match="exactly the anthology set"):
        build(db, inputs, say=lambda *a: None)


def test_unscoped_anthology_container_fails(world):
    """Round-6 HIGH-1 (other direction): an anthology must declare its routing."""
    db, inputs = world
    base_inputs(inputs, scope={})
    with pytest.raises(GateError, match="anthology-not-scoped"):
        build(db, inputs, say=lambda *a: None)


def test_physical_coverage_gate_can_fail():
    """Round-6 HIGH-1: rows must be covered EXACTLY once per work.

    Exercised directly: once the anthology-set equality gate holds, no pinned
    input can hand a work two memberships, so this gate is defense-in-depth --
    it is proven able to fail by handing it a duplicated membership.
    """
    meta = {"wX": {"rows": 3}}
    ok = [("work:wX", "wX", "whole", None, "singleton", None, 3)]
    check_membership_gates(ok, meta)  # green
    dup = ok + [("work:wX", "wX", "פרק א", "פרק א", "scope_map",
                 "division_title", 3)]
    with pytest.raises(GateError, match="cover 6 rows, db has 3"):
        check_membership_gates(dup, meta)
    with pytest.raises(GateError, match="blank/invalid scope"):
        check_membership_gates(
            [("work:wX", "wX", "  ", None, "work_group", None, 3)], meta)


def test_unsupported_alias_kind_fails(world):
    """Round-6 HIGH-3: an unknown kind must not pose as a same_work partner."""
    db, inputs = world
    links = BASE_LINKS + [("rsW", "wS", "unsupported_kind")]
    add_db(db, links=[("rsW", "wS", "unsupported_kind")])
    base_inputs(inputs, links=links)
    with pytest.raises(GateError, match="unsupported kind"):
        build(db, inputs, say=lambda *a: None)


def test_blank_group_scope_fails(world):
    """Round-6 HIGH-2: a group member's scope may not be blank."""
    db, inputs = world
    groups = {"מדרש פלוני": {"members": {"wC1": "", "wC2": "whole"},
                             "overrides_census": True}}
    base_inputs(inputs, groups=groups)
    with pytest.raises(GateError,
                       match=r"members\[.wC1.\] must be a non-blank string"):
        build(db, inputs, say=lambda *a: None)


def test_verified_link_on_mint_target_fails(world):
    """Round-6 HIGH-4: a mint can carry no containment edge."""
    db, inputs = world
    scope = {"rsA": [BASE_SCOPE["rsA"][0],
                     dict(BASE_SCOPE["rsA"][1], basis="verified_link")]}
    base_inputs(inputs, scope=scope)
    with pytest.raises(GateError, match="verified_link cannot target a mint"):
        build(db, inputs, say=lambda *a: None)


def test_pending_owner_on_work_target_fails(world):
    """Round-6 HIGH-4: pending_owner must stay on a provisional mint."""
    db, inputs = world
    scope = {"rsA": [dict(BASE_SCOPE["rsA"][0], basis="pending_owner"),
                     BASE_SCOPE["rsA"][1]]}
    base_inputs(inputs, scope=scope)
    with pytest.raises(GateError, match="pending_owner must target a mint"):
        build(db, inputs, say=lambda *a: None)


def test_unknown_scope_basis_fails(world):
    """Round-6 HIGH-4: an unknown basis must not reach known_work_member."""
    db, inputs = world
    scope = {"rsA": [BASE_SCOPE["rsA"][0],
                     dict(BASE_SCOPE["rsA"][1], basis="looks_right")]}
    base_inputs(inputs, scope=scope)
    with pytest.raises(GateError, match="basis must be one of"):
        build(db, inputs, say=lambda *a: None)


def test_refused_build_closes_the_db(world):
    """Round-5 MED-3: a refused build must not keep the file handle open."""
    db, inputs = world
    scope = {"rsA": [BASE_SCOPE["rsA"][0],
                     dict(BASE_SCOPE["rsA"][1], prefix="whole")]}
    base_inputs(inputs, scope=scope)
    with pytest.raises(GateError):
        build(db, inputs, say=lambda *a: None)
    # on Windows a retained handle blocks the rename; an exclusive lock proves
    # the connection is gone
    con = sqlite3.connect(db)
    try:
        con.execute("PRAGMA locking_mode=EXCLUSIVE")
        con.execute("BEGIN EXCLUSIVE")
        con.execute("ROLLBACK")
    finally:
        con.close()
    os.replace(db, db + ".moved")
    os.replace(db + ".moved", db)


# --------------------------------------------------------------------------
# the input contract: one gate class instead of one gate per malformed state
#
# Seven Codex review rounds each produced a fresh hand-authored input shape
# that built green (a truthy 'false', a blank name, two target keys, a typo'd
# key, an unsupported kind). validate_inputs() closes the class; this matrix is
# its proof, and every row is a state that MUST refuse.
# --------------------------------------------------------------------------
FAM = "container_families.json"
CEN = "census_members.json"
SCP = "scope_map.json"
AUT = "author_authority.json"


def _fam0(doc):
    return doc["families"][0]


def _entry(doc, i=1):
    return doc["containers"]["rsA"][i]


CONTRACT_MUTATIONS = [
    # (doc, mutation, expected message fragment)
    (FAM, lambda d: _fam0(d).__setitem__("family", ""),
     "families[0].family must be a non-blank string"),
    (FAM, lambda d: _fam0(d).__setitem__("family", "   "),
     "families[0].family must be a non-blank string"),
    (FAM, lambda d: _fam0(d).__setitem__("container", None),
     "families[0].container must be a non-blank string"),
    (FAM, lambda d: _fam0(d).__setitem__("class", "Anthology"),
     "families[0].class must be one of"),
    (FAM, lambda d: _fam0(d).pop("same_work_partners"),
     "missing ['same_work_partners']"),
    (FAM, lambda d: _fam0(d).__setitem__("contains_part", ["wP1"]),
     "unknown ['contains_part']"),
    (FAM, lambda d: _fam0(d).__setitem__("contains_parts", "wP1"),
     "contains_parts must be a list"),
    (FAM, lambda d: _fam0(d).__setitem__("contains_parts", [""]),
     "contains_parts[0] must be a non-blank string"),
    (FAM, lambda d: d.__setitem__("work_groups", {"": {
        "members": {"wC1": "whole"}, "overrides_census": True}}),
     "work_groups key ''"),
    (FAM, lambda d: d.__setitem__("work_groups", {"g": {
        "members": {"wC1": "whole"}, "overrides_census": "false"}}),
     "overrides_census must be a boolean"),
    (FAM, lambda d: d.__setitem__("work_groups", {"g": {
        "members": {"wC1": "whole"}, "overrides_census": 1}}),
     "overrides_census must be a boolean"),
    (FAM, lambda d: d.__setitem__("work_groups", {"g": {
        "members": {}, "overrides_census": True}}),
     "members is empty"),
    (FAM, lambda d: d.__setitem__("work_groups", {"g": {
        "members": {"wC1": "whole"}, "overrides_cenus": True}}),
     "unknown ['overrides_cenus']"),
    (FAM, lambda d: d.__setitem__("alias_fact_sha256", "abc"),
     "must be a sha256 hex digest"),
    (FAM, lambda d: d.__setitem__("alias_fact_count", "228"),
     "alias_fact_count must be an integer"),
    (FAM, lambda d: d.__setitem__("shares_material_edges", [
        {"a": "wS", "b": "wS", "note": "self"}]),
     "relates wS to itself"),
    (FAM, lambda d: d.__setitem__("shares_material_edges", [
        {"a": "wS", "b": "wW"}]), "missing ['note']"),
    (CEN, lambda d: d["merges"][0].__setitem__("canonical", ""),
     "merges[0].canonical must be a non-blank string"),
    (CEN, lambda d: d["merges"][0].__setitem__("members", ["wC1", "wC1"]),
     "members repeats an id"),
    (CEN, lambda d: d["merges"][0].pop("not_in_review_db"),
     "missing ['not_in_review_db']"),
    (CEN, lambda d: d.__setitem__("non_materialized_members", [None]),
     "non_materialized_members[0] must be a non-blank string"),
    (SCP, lambda d: _entry(d).__setitem__("rows_matched", 0),
     "rows_matched must be >= 1"),
    (SCP, lambda d: _entry(d).__setitem__("rows_matched", True),
     "rows_matched must be an integer"),
    (SCP, lambda d: _entry(d).__setitem__("rows_matched", "1"),
     "rows_matched must be an integer"),
    (SCP, lambda d: _entry(d).__setitem__("target", {}),
     "must name exactly one of work/group/mint"),
    (SCP, lambda d: _entry(d).__setitem__(
        "target", {"work": "wS", "mint": "x"}),
     "must name exactly one of work/group/mint"),
    (SCP, lambda d: _entry(d).__setitem__("target", "wS"),
     "target must be an object"),
    (SCP, lambda d: _entry(d).__setitem__("target_title", ""),
     "target_title must be a non-blank string"),
    (SCP, lambda d: _entry(d).__setitem__("prefx", "x"),
     "unknown ['prefx']"),
    (SCP, lambda d: d["containers"].__setitem__("rsA", []),
     "declares no scope entries"),
    (SCP, lambda d: d["containers"].__setitem__("rsA", {}),
     "must be a list"),
    (AUT, lambda d: d.__setitem__("variants", {"א": ""}),
     "variants['א'] must be a non-blank string"),
    (AUT, lambda d: d.__setitem__("owner_title_rulings", {"wS": None}),
     "owner_title_rulings['wS'] must be a non-blank string"),
    (AUT, lambda d: d.__setitem__("fills", {"wS": {"author": "פ"}}),
     "missing ['provenance']"),
    (AUT, lambda d: d.pop("variants"), "missing ['variants']"),
]


@pytest.mark.parametrize("doc,mutate,want", CONTRACT_MUTATIONS,
                         ids=[f"{d}:{w[:38]}" for d, _, w in CONTRACT_MUTATIONS])
def test_input_contract_refuses(world, doc, mutate, want):
    db, inputs = world
    repin(inputs, doc, mutate)
    with pytest.raises(GateError) as ei:
        build(db, inputs, say=lambda *a: None)
    assert want in str(ei.value), f"expected {want!r}, got {ei.value}"


def test_contract_runs_before_semantics(world):
    """The contract must fire on the FIRST malformed value, before any gate
    reads it -- otherwise a semantic gate may 'pass' on a mistyped input."""
    db, inputs = world
    repin(inputs, FAM, lambda d: d.__setitem__("work_groups", {"g": {
        "members": {"wGhost": "whole"}, "overrides_census": "true"}}))
    with pytest.raises(GateError) as ei:
        build(db, inputs, say=lambda *a: None)
    # the boolean violation, NOT the (also true) ghost-member violation
    assert "overrides_census must be a boolean" in str(ei.value)


def test_blank_db_title_refused(world):
    """Round-7 HIGH-1 at its reachable source: the contract cannot type-check
    the DB, so a whitespace work_title must still refuse at ensure_kw()."""
    db, inputs = world
    add_db(db, rows=[("wBlank", "   ", None, "msource", None)])
    with pytest.raises(GateError, match="blank title"):
        build(db, inputs, say=lambda *a: None)


def test_string_override_cannot_merge_canonicals(world):
    """Round-7 HIGH-2 end-to-end: even if a truthy string reached the cluster
    loop, `is True` would refuse the census override."""
    db, inputs = world
    add_db(db, rows=[("wC3", "מדרש פלוני", None, "sefaria", None)],
           links=[("wC2", "wC3", "same_work")])
    links = BASE_LINKS + [("wC2", "wC3", "same_work")]
    merges = BASE_MERGES + [{"members": ["wC3"], "canonical": "wC3",
                             "not_in_review_db": []}]
    groups = {"קבוצת בעלים": {"members": {"wC1": "whole", "wC2": "whole",
                                          "wC3": "whole"},
                              "overrides_census": True}}
    base_inputs(inputs, links=links, merges=merges, groups=groups)
    build(db, inputs, say=lambda *a: None)  # explicit true: allowed
    repin(inputs, FAM, lambda d: d["work_groups"]["קבוצת בעלים"].__setitem__(
        "overrides_census", "true"))
    with pytest.raises(GateError, match="overrides_census must be a boolean"):
        build(db, inputs, say=lambda *a: None)


# --------------------------------------------------------------------------
# owner-added whole-work merges (round-8 HIGH: three Rabenu Hananel pairs the
# upstream census leaves split). A separate mechanism from `merges`, so the
# pinned upstream contract is never edited -- and never double-claims a work.
# --------------------------------------------------------------------------
OWNER_MERGE = [{"members": ["wS", "wP2"], "canonical": "wS",
                "ruling": "owner: one work, two witnesses"}]


def _with_owner_merge(inputs, merges=OWNER_MERGE):
    repin(inputs, CEN, lambda d: d.__setitem__("owner_merges", merges))


def test_owner_merge_builds_one_known_work(world):
    db, inputs = world
    add_db(db, rows=[("wP2", "חלק ראשון", None, "sefaria", None)])
    _with_owner_merge(inputs)
    build(db, inputs, say=lambda *a: None)
    con = sqlite3.connect(db)
    kws = dict(con.execute(
        "SELECT work_id, kw_id FROM known_work_member WHERE work_id IN "
        "('wS','wP2') AND scope='whole'"))
    assert kws["wS"] == kws["wP2"] == kw_id_of("work:wS")
    basis, title_basis = con.execute(
        "SELECT (SELECT basis FROM known_work_member WHERE work_id='wP2'), "
        "(SELECT title_basis FROM known_work WHERE kw_id=?)",
        (kws["wS"],)).fetchone()
    con.close()
    # provenance is visible, not folded into 'census'
    assert (basis, title_basis) == ("owner_merge", "owner_merge")


def test_owner_merge_cannot_reclaim_a_census_member(world):
    """One id, one mechanism: upstream contract OR owner ruling, never both."""
    db, inputs = world
    _with_owner_merge(inputs, [{"members": ["wC1", "wS"], "canonical": "wS",
                                "ruling": "owner"}])
    with pytest.raises(GateError,
                       match="already claimed by an upstream census merge"):
        build(db, inputs, say=lambda *a: None)


def test_owner_merge_ghost_member_fails(world):
    db, inputs = world
    _with_owner_merge(inputs, [{"members": ["wS", "wGhost"], "canonical": "wS",
                                "ruling": "owner"}])
    with pytest.raises(GateError, match="has no evidence rows"):
        build(db, inputs, say=lambda *a: None)


def test_owner_merge_anthology_member_fails(world):
    db, inputs = world
    _with_owner_merge(inputs, [{"members": ["wS", "rsA"], "canonical": "wS",
                                "ruling": "owner"}])
    with pytest.raises(GateError, match="contains anthology container"):
        build(db, inputs, say=lambda *a: None)


def test_two_owner_merges_cannot_share_a_member(world):
    db, inputs = world
    add_db(db, rows=[("wP2", "חלק ראשון", None, "sefaria", None),
                     ("wP3", "חלק ראשון", None, "msource", None)])
    _with_owner_merge(inputs, [
        {"members": ["wS", "wP2"], "canonical": "wS", "ruling": "owner"},
        {"members": ["wS", "wP3"], "canonical": "wS", "ruling": "owner"}])
    with pytest.raises(GateError, match="claimed by two owner merges"):
        build(db, inputs, say=lambda *a: None)


def test_owner_merge_absorbed_by_a_group_fails(world):
    """An owner assertion must not silently become a work-group membership."""
    db, inputs = world
    add_db(db, rows=[("wP2", "חלק ראשון", None, "sefaria", None)])
    base_inputs(inputs, groups={"קבוצה": {"members": {"wS": "א", "wP2": "ב"},
                                          "overrides_census": False,
                                          "ruling": "owner"}})
    _with_owner_merge(inputs)
    with pytest.raises(GateError, match="pulled into a family/group cluster"):
        build(db, inputs, say=lambda *a: None)


def test_owner_merge_united_with_census_canonical_fails(world):
    """A same_work link dragging an owner merge onto a census canonical is a
    NEW identity assertion, not a silent absorption."""
    db, inputs = world
    add_db(db, rows=[("wP2", "חלק ראשון", None, "sefaria", None)],
           links=[("wP2", "wC2", "same_work")])
    links = BASE_LINKS + [("wP2", "wC2", "same_work")]
    base_inputs(inputs, links=links)
    _with_owner_merge(inputs)
    with pytest.raises(GateError, match="united with census canonical"):
        build(db, inputs, say=lambda *a: None)


OWNER_MERGE_CONTRACT = [
    (lambda d: d.__setitem__("owner_merges", [
        {"members": ["wS"], "canonical": "wS", "ruling": "owner"}]),
     "merges fewer than two works"),
    (lambda d: d.__setitem__("owner_merges", [
        {"members": ["wS", "wP2"], "canonical": "wX", "ruling": "owner"}]),
     "is not one of its members"),
    (lambda d: d.__setitem__("owner_merges", [
        {"members": ["wS", "wP2"], "canonical": "wS", "ruling": ""}]),
     "ruling must be a non-blank string"),
    (lambda d: d.__setitem__("owner_merges", [
        {"members": ["wS", "wS"], "canonical": "wS", "ruling": "owner"}]),
     "members repeats an id"),
    (lambda d: d.__setitem__("owner_merges", [
        {"members": ["wS", "wP2"], "canonical": "wS"}]), "missing ['ruling']"),
    (lambda d: d.__setitem__("owner_merges", [
        {"members": ["wS", "wP2"], "canonical": "wS", "ruling": "owner",
         "not_in_review_db": []}]), "unknown ['not_in_review_db']"),
]


@pytest.mark.parametrize("mutate,want", OWNER_MERGE_CONTRACT,
                         ids=[w[:34] for _, w in OWNER_MERGE_CONTRACT])
def test_owner_merge_contract_refuses(world, mutate, want):
    db, inputs = world
    repin(inputs, CEN, mutate)
    with pytest.raises(GateError) as ei:
        build(db, inputs, say=lambda *a: None)
    assert want in str(ei.value), f"expected {want!r}, got {ei.value}"


def test_registry_rebuild_drops_the_card_grain(world):
    """The card grain projects these identities and references known_work: a
    registry rebuild must invalidate it, not fail against its foreign keys and
    not leave cards built from the previous identities behind."""
    db, inputs = world
    build(db, inputs, say=lambda *a: None)
    con = sqlite3.connect(db)
    con.execute("CREATE TABLE card(card_id TEXT PRIMARY KEY, kw_id TEXT "
                "NOT NULL REFERENCES known_work(kw_id))")
    con.execute("CREATE TABLE card_member(evidence_id TEXT PRIMARY KEY, "
                "card_id TEXT NOT NULL REFERENCES card(card_id))")
    kw = con.execute("SELECT kw_id FROM known_work LIMIT 1").fetchone()[0]
    con.execute("INSERT INTO card VALUES ('c1', ?)", (kw,))
    con.execute("INSERT INTO card_member VALUES ('e1', 'c1')")
    con.execute("INSERT INTO meta VALUES ('card_grain.cards', '1')")
    con.commit()
    con.close()
    build(db, inputs, say=lambda *a: None)      # must not raise on the FKs
    con = sqlite3.connect(db)
    left = [r[0] for r in con.execute(
        "SELECT name FROM sqlite_master WHERE name IN ('card','card_member')")]
    meta = con.execute("SELECT COUNT(*) FROM meta WHERE key LIKE "
                       "'card_grain.%'").fetchone()[0]
    con.close()
    assert left == [] and meta == 0
