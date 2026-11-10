# -*- coding: utf-8 -*-
"""Gate 1 of the FGP source-folders plan (``scripts/fgp_association_inventory.py``).

The synthetic world of ``tests/fgp_fixture_world.py`` is built in ``tmp_path``; the inventory
must tie every file to its record, classify every multi-association, give every unmatched /
absent / sidecar-only file a reason, and fail when a basename is registered under two kinds
or an expectation is not reproduced.
"""
from __future__ import annotations

import json
import os
import sqlite3

import pytest

from scripts import fgp_association_inventory as inv
from tests.fgp_fixture_world import ALMA_A, ALMA_B, ALMA_C, ALMA_D, build_fist, build_world, rows as _rows


@pytest.fixture
def world(tmp_path):
    return build_world(str(tmp_path))


def _args(w, *extra):
    return ["--nopos", w["nopos"], "--withpos", w["withpos"], "--fist", w["fist"], "--fjms", w["fjms"],
            "--nli", w["nli"], "--fgp-db", w["fgp"], "--out-dir", w["out"], *extra]


def test_inventory_passes_and_ties_every_file(world, capsys):
    rc = inv.main(_args(world, "--expect", "fist.matched=8"))
    out = capsys.readouterr().out
    assert rc == 0, out
    db = os.path.join(world["out"], "inventory.db")
    files = {r["basename"]: r for r in _rows(db, "SELECT * FROM files")}
    counts = {r["key"]: r["value"] for r in _rows(db, "SELECT * FROM counts")}

    # physical files: lock, junk and the stray .txt are excluded, the scan twin folds in
    assert counts["nopos.lock_files"] == 1 and counts["nopos.junk"] == 1 and counts["nopos.other_ext"] == 1
    assert counts["nopos.basenames"] == 13 and counts["nopos.basenames_docx_only"] == 1
    assert files["20000000000012_CUL_T-S 1.2"]["has_scan_pdf"] == 1
    assert files["20000000000012_CUL_T-S 1.2"]["pdf_files"] == 2

    # kinds come from FIST, never from the name
    assert files["20000000000013"]["kind"] == "description"
    assert files["20000000000013"]["status"] == "description"
    assert files["20000000000011_CUL_T-S 1.1_Tl"]["kind"] == "translation"
    assert counts["kind.transcription"] == 6 and counts["kind.translation"] == 1 and counts["kind.description"] == 1

    # statuses
    served = files["20000000000011_CUL_T-S 1.1_Tc"]
    assert served["status"] == "served" and served["alma_ids"] == json.dumps([ALMA_A])
    assert served["b_dir"] == "100000" and served["b_dir_exists"] == 1 and served["b_format"] == "B1"
    assert served["today_row_ids"] == "[1]"
    cand = files["20000000000012_CUL_T-S 1.2"]
    assert cand["status"] == "candidate" and cand["has_copyright"] == 1 and cand["b_format"] == "B2"
    assert files["20000000000016_CUL_T-S 1.6_Tc"]["status"] == "hold_visibility"
    assert files["20000000000018_CUL_T-S 1.8_Tc"]["status"] == "unresolved_alma"
    assert files["20000000000018_CUL_T-S 1.8_Tc"]["unresolved_reason"] == "set_without_inventory"

    # multi-associations each carry a class and a declared rule
    multi = files["20000000000015_CUL_T-S 1.5_Tc"]
    assert multi["status"] == "candidate_multi_alma"
    assert multi["multi_class"] == "inventory_multi_alma"
    assert multi["resolution_rule"] == inv.RESOLUTION_RULES["inventory_multi_alma"]
    assert multi["alma_count"] == 2 and multi["b_dir"] == "9999_G1" and multi["b_dir_exists"] == 0
    assert multi["credit_prefix_agrees"] == 0  # 200 in the name, 132 in FIST
    dup = files["20000000000017"]
    assert dup["multi_class"] == "dup_unit_rows_same_signature" and dup["unit_rows"] == 2
    assert dup["unit_ids"] == json.dumps([["dbo_UnitTranscription", 5], ["dbo_UnitTranscription", 6]])
    assert counts["alma.multi_alma_files"] == 1 and counts.get("multi.unclassified", 0) == 0

    # unmatched files each have a reason
    reasons = {b: r["unmatched_reason"] for b, r in files.items() if r["status"] == "unmatched"}
    assert reasons == {
        "20000000000014_CUL_T-S 1.4_Tc": "no_fist_trace",
        "20000000000015_CUL_T-S 1.5 renamed": "fist_match_by_id_prefix_other_name",
        "20000000000011_CUL_T-S  1.1_Tc": "fist_match_after_normalizing",
        "C252953": "c_number_named_no_fist",
        "20000000000020_docx_only": "no_fist_trace",
    }
    assert counts["fist.unmatched"] == 5

    # the FIST side: files absent from the export, with a reason each
    absent = {r["filename"]: r["reason"] for r in _rows(db, "SELECT * FROM fist_absent")}
    assert absent == {
        "20000000000099_CUL_T-S 9.9_Tc": "dir_present_file_missing",
        "85000070000451_JTS_ENA NS 2.23_Tc": "dir_absent_from_export",
    }

    # today's sidecar: every row placed, sidecar-only rows reasoned
    today = {r["id"]: r for r in _rows(db, "SELECT * FROM today_rows")}
    assert today[1]["in_nopos"] == 1 and today[3]["db_only_reason"] == "fist_dir_absent_from_export"
    assert today[4]["db_only_reason"] == "not_in_fist"
    assert counts["today.db_only_basenames"] == 2

    # folder B: formats, numbers, regions, joins, bridges
    bd = {r["dir"]: r for r in _rows(db, "SELECT * FROM b_dirs")}
    assert bd["100000"]["format"] == "B1" and bd["100000"]["has_xml"] == 1 and bd["100000"]["has_js"] == 1
    assert bd["100000"]["png_count"] == 1 and bd["100000"]["fist_transcription_refs"] == 1
    assert bd["C200"]["format"] == "B2" and bd["C200"]["has_data_js"] == 1
    assert bd["300 301 99944"]["is_multi_image"] == 1 and bd["300 301 99944"]["number_count"] == 3
    assert bd["400_G1"]["region"] == "G1" and bd["400_G1"]["numbers"] == '["400"]'
    assert bd["C200.G2"]["region"] == "G2" and bd["V"]["format"] == "other"
    assert counts["withpos.dirs"] == 7 and counts["withpos.stray_files"] == 1
    assert counts["withpos.region_dirs"] == 2 and counts["withpos.multi_image_dirs"] == 1
    bn = {r["number"]: r for r in _rows(db, "SELECT * FROM b_numbers")}
    assert set(bn) == {"100000", "200", "300", "301", "99944", "400"}
    assert bn["100000"]["dirs"] == json.dumps(["100000", "100000 dup"])
    assert bn["200"]["fjms_alma_ids"] == json.dumps([ALMA_B]) and bn["200"]["rotation_deg"] == 182.0
    assert bn["200"]["nli_alma_ids"] == json.dumps([ALMA_B, ALMA_C])
    assert bn["300"]["today_rows"] == 1 and bn["400"]["fjms_alma_ids"] is None
    assert counts["withpos.numbers_fjms_resolved"] == 5 and counts["withpos.numbers_fjms_unresolved"] == 1
    assert counts["withpos.numbers_nli_multi_alma"] == 1
    assert counts["withpos.manuscripts"] == 3 and counts["withpos.manuscripts_in_today_db"] == 1
    assert counts["withpos.itf_distinct"] == 3 and counts["withpos.itf_exists"] == 2 and counts["withpos.itf_missing"] == 1
    assert counts["withpos.dirs_unreferenced_by_fist"] == 5
    assert counts["withpos.rotation.~180"] == 1 and counts["withpos.rotation.~90/270"] == 1

    # manuscripts
    assert counts["alma.transcription_manuscripts"] == 4
    assert counts["alma.transcription_manuscripts_not_in_today"] == 2  # ALMA_B, ALMA_C (A served, D sidecar-only)

    # association paths
    paths = _rows(db, "SELECT * FROM file_almas WHERE basename = '20000000000015_CUL_T-S 1.5_Tc' ORDER BY alma_id")
    assert [p["alma_id"] for p in paths] == [ALMA_C, ALMA_D]
    assert all(p["inventory_alma_count"] == 2 for p in paths)

    # every check passed, the expectation included
    checks = {r["name"]: r for r in _rows(db, "SELECT * FROM checks")}
    assert all(c["passed"] == 1 for c in checks.values()), [n for n, c in checks.items() if not c["passed"]]
    assert checks["E_fist.matched"]["actual"] == "8"
    summary = json.load(open(os.path.join(world["out"], "summary.json"), encoding="utf-8"))
    assert summary["counts"]["fist.matched"] == 8
    report = open(os.path.join(world["out"], "REPORT.md"), encoding="utf-8").read()
    assert "**Result: PASS**" in report and "inventory_multi_alma" in report


def test_gate_fails_on_a_basename_of_two_kinds(world, capsys):
    os.remove(world["fist"])
    build_fist(world["fist"], mixed_kind=True)
    rc = inv.main(_args(world))
    out = capsys.readouterr().out
    assert rc == 1
    assert "[FAIL] C03_kinds_partition" in out
    db = os.path.join(world["out"], "inventory.db")
    row = _rows(db, "SELECT kind, status FROM files WHERE basename = '20000000000011_CUL_T-S 1.1_Tc'")[0]
    assert row["kind"] == "mixed:description+transcription"
    assert row["status"] == "unresolved_alma"  # never publishable while the kind is open
    assert "**Result: FAIL**" in open(os.path.join(world["out"], "REPORT.md"), encoding="utf-8").read()


def test_gate_fails_on_an_expectation_the_run_does_not_reproduce(world, capsys):
    rc = inv.main(_args(world, "--expect", "fist.matched=999", "--expect", "withpos.numbers=6"))
    out = capsys.readouterr().out
    assert rc == 1
    assert "[FAIL] E_fist.matched: expected 999, got 8" in out
    assert "[ok  ] E_withpos.numbers" in out


def test_expect_file_is_read_and_enforced(world, capsys):
    path = os.path.join(world["root"], "expect.json")
    with open(path, "w", encoding="utf-8") as fh:
        json.dump({"_comment": "ignored", "fist.matched": 8, "withpos.numbers": 6}, fh)
    assert inv.main(_args(world, "--expect-file", path)) == 0
    with open(path, "w", encoding="utf-8") as fh:
        json.dump({"fist.matched": 7}, fh)
    assert inv.main(_args(world, "--expect-file", path)) == 1
    assert "[FAIL] E_fist.matched: expected 7, got 8" in capsys.readouterr().out


def test_bad_expectation_is_a_usage_error(world):
    assert inv.main(_args(world, "--expect", "fist.matched")) == 2
    assert inv.main(_args(world, "--expect", "fist.matched=many")) == 2
    bad = os.path.join(world["root"], "bad.json")
    with open(bad, "w", encoding="utf-8") as fh:
        json.dump({"fist.matched": "8"}, fh)
    assert inv.main(_args(world, "--expect-file", bad)) == 2
    assert inv.main(_args(world, "--expect-file", os.path.join(world["root"], "missing.json"))) == 2


def test_refuses_to_overwrite_a_foreign_inventory_db(world):
    os.makedirs(world["out"])
    foreign = os.path.join(world["out"], "inventory.db")
    con = sqlite3.connect(foreign)
    con.execute("CREATE TABLE somebody_elses (x)")
    con.commit()
    con.close()
    with pytest.raises(SystemExit, match="refusing to overwrite"):
        inv.main(_args(world))
    assert _rows(foreign, "SELECT name FROM sqlite_master")[0]["name"] == "somebody_elses"


def test_rerun_replaces_its_own_inventory(world):
    assert inv.main(_args(world)) == 0
    assert inv.main(_args(world)) == 0
    db = os.path.join(world["out"], "inventory.db")
    assert _rows(db, "SELECT COUNT(*) AS n FROM files")[0]["n"] == 13
    assert not os.path.exists(db + ".tmp")


@pytest.mark.parametrize("name,expected", [
    ("100000", ("B1", ["100000"], None)),
    ("C100181", ("B2", ["100181"], None)),
    ("100022 100024 100038 120582", ("B1", ["100022", "100024", "100038", "120582"], None)),
    ("100446_G2", ("B1", ["100446"], "G2")),
    ("101331 110280 115028.G1", ("B1", ["101331", "110280", "115028"], "G1")),
    ("C203310", ("B2", ["203310"], None)),
    ("V", ("other", [], None)),
])
def test_parse_b_dirname(name, expected):
    assert inv.parse_b_dirname(name) == expected


@pytest.mark.parametrize("base,expected", [
    ("20000070000181_CUL_T-S NS 329.533_Tc", ("suffixed", "20000070000181", "Tc")),
    ("10800010000031_RNL_Yevr_ III B 71", ("no_suffix", "10800010000031", None)),
    ("50002300001701", ("id_only", "50002300001701", None)),
    ("C252953", ("c_named", "C252953", None)),
    ("notes", ("no_leading_id", None, None)),
])
def test_name_shape(base, expected):
    assert inv.name_shape(base) == expected


def test_today_basename_strips_only_the_export_suffix():
    assert inv.today_basename("20000070000181_CUL_T-S NS 329.533_Tc_trans.pdf") == "20000070000181_CUL_T-S NS 329.533_Tc"
    assert inv.today_kind("x_translate_heb.pdf") == "translation"
    assert inv.today_kind("x_trans.pdf") == "transcription"
    assert inv.today_kind("x.pdf") is None


def test_a_gate4_candidate_is_refused_as_todays_sidecar(tmp_path):
    # 2026-10-28: after the local swap, gates 3-4 read the candidate as "today" and held 6,481 rows on C1
    today, cand = tmp_path / "today.db", tmp_path / "candidate.db"
    for path, extra in ((today, ""), (cand, ", text_provenance TEXT")):
        c = sqlite3.connect(path)
        c.execute(f"CREATE TABLE fgp_transcriptions (id INTEGER, content TEXT, rel_path TEXT{extra})")
        c.commit()
        c.close()
    inv.refuse_candidate_as_today(inv.ro(str(today)), str(today))
    with pytest.raises(SystemExit, match="gate-4 candidate"):
        inv.refuse_candidate_as_today(inv.ro(str(cand)), str(cand))
