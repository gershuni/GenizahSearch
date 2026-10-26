# -*- coding: utf-8 -*-
"""Gate 2 of the FGP source-folders plan (``scripts/fgp_gate2_rules.py``).

The extended synthetic world of ``tests/fgp_fixture_world.py`` holds one sidecar row for
every rule: a kept row, a translation, a sidecar-only file, a file FIST does not know, an
exact duplicate, a second manuscript of one file, a visibility hold, a sys_id the FIST
chain contradicts, an unreachable row, an unconfirmed association, a double registration,
catalogue and book credits, a book without a title, and XML text from another record's
folder. Gate 1 runs first; gate 2 must give every row one outcome and fail on a unit whose
kind disagrees with the row's.
"""
from __future__ import annotations

import json
import os
import sqlite3

import pytest

from scripts import fgp_association_inventory as gate1
from scripts import fgp_gate2_rules as gate2
from tests.fgp_fixture_world import ALMA_A, ALMA_C, build_world, rows as _rows


@pytest.fixture
def world(tmp_path):
    w = build_world(str(tmp_path), extended=True)
    rc = gate1.main(["--nopos", w["nopos"], "--withpos", w["withpos"], "--fist", w["fist"], "--fjms", w["fjms"],
                     "--nli", w["nli"], "--fgp-db", w["fgp"], "--out-dir", w["out"]])
    assert rc == 0
    return w


def _args(w, *extra):
    return ["--inventory", os.path.join(w["out"], "inventory.db"), "--fgp-db", w["fgp"], "--fist", w["fist"],
            "--out-dir", w["out"], *extra]


def test_every_row_gets_one_outcome_and_the_gate_passes(world, capsys):
    rc = gate2.main(_args(world, "--expect", "rows=20"))
    out = capsys.readouterr().out
    assert rc == 0, out
    db = os.path.join(world["out"], "gate2.db")
    o = {r["id"]: r for r in _rows(db, "SELECT * FROM row_outcomes")}
    counts = {r["key"]: r["value"] for r in _rows(db, "SELECT * FROM counts")}
    assert len(o) == 20

    # identity: the FIST unit reached through the row's own sys_id, and a durable key
    assert o[1]["association_basis"] == "fist_chain" and o[1]["unit_id"] == 1
    assert o[1]["durable_key"] == "dbo_UnitTranscription:1|990001|C100000"
    assert o[1]["outcome"] == "kept" and o[1]["text_status"] == "ok" and o[1]["xml_owner_relation"] == "own_folder"
    assert o[2]["unit_table"] == "dbo_UnitTranslation" and o[2]["outcome"] == "kept" and o[2]["kind"] == "translation"

    # the sidecar-only file is tied through FIST directly; the file FIST never saw has no record
    assert o[3]["in_export"] == 0 and o[3]["association_basis"] == "fist_chain" and o[3]["unit_id"] == 8
    assert o[3]["outcome"] == "re-sourced" and o[3]["text_status"] == "file_absent_in_delivery"
    assert json.loads(o[3]["hold_reasons"]) == ["file_absent_in_delivery"]
    # source 850 with exactly one site-user part in the manuscript's DataSource: that person is the credit
    assert o[3]["credit_category"] == "datasource_part" and o[3]["credit_status"] == "resolved"
    assert o[3]["credit_he"] == "משתמש באתר - פלוני אלמוני" and o[3]["credit_changed"] == 1
    assert o[17]["association_basis"] == "none" and o[17]["outcome"] == "kept"
    assert o[17]["outcome_reason"] == "no_fist_unit_of_this_kind" and o[17]["text_status"] == "no_record"
    assert o[17]["publishable_strict"] == 0 and o[17]["publishable_generic"] == 0
    assert o[4]["outcome"] == "retired-with-alias" and o[4]["outcome_reason"] == "no_sys_id_unreachable"

    # exact duplicates: the lowest id stays, the other retires with it as successor; the second
    # manuscript of the same file is its own row
    assert o[5]["outcome"] == "re-sourced" and o[6]["outcome"] == "retired-with-alias"
    assert o[6]["successor_id"] == 5 and o[6]["outcome_reason"] == "exact_duplicate_row"
    assert o[7]["outcome"] == "re-sourced" and o[7]["sys_id"] != o[5]["sys_id"]
    assert o[5]["multi_class"] == "inventory_multi_alma"
    # ... and their credit now comes from FIST (team 132), not the 200 in the file name
    assert o[5]["credit_category"] == "team" and o[5]["credit_changed"] == 1
    assert o[5]["outcome_reason"] == "credit_from_fist" and o[5]["publishable_strict"] == 1
    assert "גויטין" in o[5]["credit_he"] and "Goitein" in o[5]["credit_en"]

    # visibility hold
    assert o[8]["visibility_hold"] == 1 and o[8]["publishable_strict"] == 0 and o[8]["outcome"] == "kept"
    assert json.loads(o[8]["hold_reasons"]) == ["tmp_DoubleNotToDisplay"]

    # a sys_id the FIST chain contradicts retires without a successor; so does an unreachable row
    assert o[9]["association_basis"] == "conflict" and o[9]["outcome"] == "retired-with-alias"
    assert o[9]["successor_id"] is None and o[9]["outcome_reason"] == "sys_id_not_in_fist_chain"
    assert o[10]["outcome"] == "retired-with-alias" and o[10]["outcome_reason"] == "no_sys_id_unreachable"
    assert o[10]["successor_id"] is None  # rows 10 and 11 share a file name; 10 is not a duplicate of 11
    assert o[11]["association_basis"] == "unconfirmed" and o[11]["outcome"] == "kept"
    assert json.loads(o[11]["hold_reasons"]) == ["association_unconfirmed"]

    # registered twice for one manuscript: the lowest unit id is canonical, the row is publishable
    assert o[12]["association_basis"] == "fist_chain_multi_registration" and o[12]["unit_id"] == 10
    assert o[12]["publishable_strict"] == 1 and o[12]["credit_category"] == "team"

    # catalogue and book credits resolve from FIST SubId; a book without a title stays unresolved
    assert o[13]["credit_category"] == "catalog" and o[13]["credit_en"] == "CUDL Catalog, Cambridge, 2010"
    assert o[13]["outcome"] == "re-sourced" and o[13]["publishable_strict"] == 1
    assert o[14]["credit_category"] == "book" and o[14]["credit_he"] == "ספר הבהיר"
    # FIST's unit comment says the transcription was never checked against the source -> held, credit kept
    assert o[14]["fist_comment"] == "לא נבדק מול המקור. מצב כתיבה-הגהה א'" and o[14]["publishable"] == 0
    assert json.loads(o[14]["hold_reasons"]) == ["fist_comment_unchecked"] and o[14]["outcome"] == "re-sourced"
    assert o[15]["credit_category"] == "unresolved" and o[15]["credit_changed"] == 0
    assert o[15]["outcome"] == "kept" and o[15]["publishable_strict"] == 0 and o[15]["publishable_generic"] == 1
    assert o[15]["publishable"] == 1 and o[15]["credit_status"] == "generic" and o[15]["credit_he"] == "פרויקט הגניזה של פרידברג"
    rulings = {r["source"]: r for r in _rows(db, "SELECT * FROM credit_rulings")}
    assert rulings["source 600 sub 78"]["rows"] == 1
    assert "source 850" not in rulings  # row 3's contributor was found in the DataSource part

    # XML text that came from another record's folder holds the row until gate 3 verifies it
    assert o[16]["text_status"] == "xml_unverified"
    assert o[16]["xml_owner_relation"] == "owner_same_source_same_manuscript"
    assert o[16]["outcome"] == "re-sourced" and o[16]["publishable_strict"] == 0

    # the copyright text rides beside the credit for the unit that has one (none of today's rows here)
    assert counts["credit.copyright_rows"] == 0

    # a bibliographic reference (source 601) is composed from FIST's reference row, as FJMS shows it
    assert o[18]["credit_category"] == "bibliographic_reference" and o[18]["publishable"] == 1
    assert o[18]["credit_he"] == 'הפניה ביבליוגרפית – מילר, יעקב יצחק, קובץ חצי גבורים אלול תשע"ד, עמוד כו'
    assert o[18]["credit_en"].startswith("Bibliographical reference – מילר")

    # a site user known by id and a personal list known by id, both read off the live site
    assert o[19]["credit_category"] == "site_user" and o[19]["credit_en"] == "Site user - Prof. Robert Brody"
    assert o[20]["credit_category"] == "personal_list" and o[20]["credit_he"] == "חיים מיליקובסקי, רשימות אישיות"
    assert o[19]["publishable"] == 1 and o[20]["publishable"] == 1

    # totals
    assert counts["outcome.kept"] == 6 and counts["outcome.re-sourced"] == 10 and counts["outcome.retired-with-alias"] == 4
    assert counts["publishable.strict"] == 9 and counts["publishable.generic_credit_allowed"] == 10
    assert counts["publishable.under_policy"] == 10 and counts["credit.generic_rows"] == 2  # rows 15, 17
    assert counts["hold.fist_comment_unchecked"] == 1
    assert counts["durable_keys_distinct"] == 17  # row 6 shares row 5's key; rows 4 and 17 have no unit
    aliases = {r["old_id"]: r for r in _rows(db, "SELECT * FROM aliases")}
    assert set(aliases) == {4, 6, 9, 10} and aliases[6]["same_meaning"] == 1 and aliases[9]["same_meaning"] == 0
    checks = {r["name"]: r for r in _rows(db, "SELECT * FROM checks")}
    assert all(c["passed"] == 1 for c in checks.values()), [n for n, c in checks.items() if not c["passed"]]
    report = open(os.path.join(world["out"], "GATE2_REPORT.md"), encoding="utf-8").read()
    assert "**Result: PASS**" in report and "source 600 sub 78" in report
    assert ALMA_A in o[1]["sys_id"] and ALMA_C in o[5]["sys_id"]


def test_gate_fails_when_a_unit_kind_disagrees_with_the_row(world, capsys):
    # register the translation's file as a transcription in FIST: the row's kind no longer matches its unit
    con = sqlite3.connect(world["fist"])
    con.execute("DELETE FROM dbo_UnitTranslation")
    con.execute("INSERT INTO dbo_UnitTranscription (UnitTranscriptionId, SignatureId, FileName, Lang, FolderPath, "
                "tmp_DoubleNotToDisplay, HasFiles, ImageTranscriptionFolder, HebCopyRight, EngCopyRight) "
                "VALUES (99, 109, '20000000000011_CUL_T-S 1.1_Tl', 2, '2000000\\', NULL, 2, NULL, NULL, NULL)")
    con.commit()
    con.close()
    assert gate1.main(["--nopos", world["nopos"], "--withpos", world["withpos"], "--fist", world["fist"],
                       "--fjms", world["fjms"], "--nli", world["nli"], "--fgp-db", world["fgp"], "--out-dir", world["out"]]) == 0
    rc = gate2.main(_args(world))
    out = capsys.readouterr().out
    assert rc == 1
    assert "[FAIL] G02_meaning_preserved" in out
    db = os.path.join(world["out"], "gate2.db")
    assert _rows(db, "SELECT unit_kind, kind FROM row_outcomes WHERE id = 2")[0] == {"unit_kind": "transcription", "kind": "translation"}


def test_gate_fails_on_a_missed_expectation_and_refuses_a_foreign_db(world, capsys):
    assert gate2.main(_args(world, "--expect", "outcome.kept=1")) == 1
    assert "[FAIL] E_outcome.kept: expected 1, got 6" in capsys.readouterr().out
    foreign = os.path.join(world["out"], "gate2.db")
    os.remove(foreign)
    con = sqlite3.connect(foreign)
    con.execute("CREATE TABLE not_ours (x)")
    con.commit()
    con.close()
    with pytest.raises(SystemExit, match="refusing to overwrite"):
        gate2.main(_args(world))
    assert gate2.main(_args(world, "--expect-file", os.path.join(world["root"], "missing.json"))) == 2


def test_strict_policy_holds_the_generic_credit_rows(world, capsys):
    assert gate2.main(_args(world, "--credit-policy", "strict", "--expect", "publishable.under_policy=9")) == 0
    db = os.path.join(world["out"], "gate2.db")
    o = {r["id"]: r for r in _rows(db, "SELECT * FROM row_outcomes")}
    assert o[15]["publishable"] == 0 and o[15]["credit_status"] == "unresolved" and o[15]["credit_he"] is None
    assert json.loads(o[15]["hold_reasons"]) == ["credit_unresolved"]
    assert json.loads(o[3]["hold_reasons"]) == ["file_absent_in_delivery"]  # its credit is a resolved DataSource part
    assert "Credit policy: **strict**" in open(os.path.join(world["out"], "GATE2_REPORT.md"), encoding="utf-8").read()
