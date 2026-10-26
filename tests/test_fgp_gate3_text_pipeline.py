# -*- coding: utf-8 -*-
"""Gate 3 of the FGP source-folders plan (``scripts/fgp_gate3_text_pipeline.py``).

The text world of ``tests/fgp_fixture_world.py`` (``build_world(text=True)``) puts real content
behind the rows: a B1 XML folder, two B2 ``data.js`` folders, DOCX files with auto-numbering, a
footnote, a header, a hidden run, a table, uncertainty marks and one reading difference, and
today's texts with one PDF artefact per line. Gates 1 and 2 run first; gate 3 must read every
row from the source its own FIST record links to, classify every changed block, replace only
accepted texts, verify XML rows without replacing them, and fail on a lost line, a missing
source link, a changed digest and a missed expectation.
"""
from __future__ import annotations

import json
import os
import shutil
import sqlite3

import pytest

from scripts import fgp_association_inventory as gate1
from scripts import fgp_gate2_rules as gate2
from scripts import fgp_gate3_text_pipeline as g3
from tests.fgp_fixture_world import (
    ALMA_B, B1_TEXT, B2_C400_TEXT, DOCX_UNIT3_TEXT, build_world, read_docx_path, rows as _rows,
)


@pytest.fixture
def world(tmp_path):
    w = build_world(str(tmp_path), text=True)
    assert gate1.main(["--nopos", w["nopos"], "--withpos", w["withpos"], "--fist", w["fist"], "--fjms", w["fjms"],
                       "--nli", w["nli"], "--fgp-db", w["fgp"], "--out-dir", w["out"]]) == 0
    assert gate2.main(["--inventory", os.path.join(w["out"], "inventory.db"), "--fgp-db", w["fgp"], "--fist", w["fist"],
                       "--out-dir", w["out"]]) == 0
    return w


def _args(w, *extra):
    return ["--inventory", os.path.join(w["out"], "inventory.db"), "--gate2", os.path.join(w["out"], "gate2.db"),
            "--fgp-db", w["fgp"], "--nopos", w["nopos"], "--withpos", w["withpos"], "--today-files", w["today_files"],
            "--approvals", os.path.join(w["root"], "no-approvals.json"), "--out-dir", w["out"], *extra]


def _load(w):
    db = os.path.join(w["out"], "gate3.db")
    rows = {r["row_key"]: r for r in _rows(db, "SELECT * FROM text_rows")}
    counts = {r["key"]: r["value"] for r in _rows(db, "SELECT * FROM counts")}
    texts = {(r["unit_table"], r["unit_id"]): r for r in _rows(db, "SELECT * FROM unit_texts")}
    diffs = _rows(db, "SELECT * FROM line_diffs ORDER BY rowid")
    checks = {r["name"]: r for r in _rows(db, "SELECT * FROM checks")}
    return rows, counts, texts, diffs, checks


def test_every_row_is_read_from_its_own_record_and_classified(world, capsys):
    rc = g3.main(_args(world, "--allow-single-run", "--expect", "rows.today=21"))
    out = capsys.readouterr().out
    assert rc == 0, out
    rows, counts, texts, diffs, checks = _load(world)
    assert all(c["passed"] == 1 for c in checks.values()), [n for n, c in checks.items() if not c["passed"]]
    t = {r["today_id"]: r for r in rows.values() if r["today_id"]}

    # B1: the unit's own position folder through today's parser -> identical, publishable
    assert t[1]["reader"] == "b1_xml" and t[1]["status"] == "identical" and t[1]["source_link"] == "fist_itf"
    assert t[1]["publishable_text"] == 1 and t[1]["text_replaced"] == 0
    assert texts[("dbo_UnitTranscription", 1)]["text"] == B1_TEXT

    # PDF route: the delivered PDF is hashed against today's copy
    assert t[2]["reader"] == "pdf_today" and t[2]["source_link"] == "pdf_hash_identical" and t[2]["status"] == "identical"
    assert t[12]["source_link"] == "pdf_hash_differs"
    assert t[3]["status"] == "no_source" and t[3]["hold_reason"] == "file_absent_in_delivery"
    assert t[17]["status"] == "no_source" and t[17]["reader"] == "none"
    assert all(t[i]["status"] == "retired" for i in (4, 6, 9, 10))

    # DOCX route: one artefact per line, every block classified, the text replaced
    r5 = t[5]
    assert r5["reader"] == "docx" and r5["status"] == "accepted" and r5["text_replaced"] == 1
    assert r5["source_link"] == "fist_filename_docx" and r5["publishable_text"] == 1
    classes = json.loads(r5["classes"])
    for c in ("numbering", "restored_prefix", "bracket", "control_chars", "final_letters", "regrouped", "catchword",
              "footnote_apart", "header_apart", "bidi_reorder", "escape"):
        assert c in classes, (c, classes)
    assert "unclassified" not in classes and r5["held_blocks"] == 0 and r5["lost_today_lines"] == 0
    assert texts[("dbo_UnitTranscription", 3)]["text"] == DOCX_UNIT3_TEXT
    structure = json.loads(r5["structure"])
    assert structure["numbered_paragraphs"] == 4 and structure["footnotes"] == 1 and structure["hidden_runs"] == 1
    assert structure["header_parts"] == 1 and structure["footnote_refs"] == 1
    assert t[7]["new_hash"] == r5["new_hash"] and t[7]["status"] == "accepted"  # the same unit's second manuscript
    assert all(d["disposition"] == "accepted" for d in diffs if d["row_key"] == "today:5")

    # a reading difference holds the row; its block is listed for review
    assert t[8]["status"] == "held" and t[8]["hold_reason"] == "unclassified_diff" and t[8]["publishable_text"] == 0
    held = [d for d in diffs if d["row_key"] == "today:8" and d["disposition"] == "held"]
    assert held and "unclassified" in json.loads(held[0]["classes"]) and held[0]["today_text"] == "אבד דהו"

    # an XML row is verified by its own record's DOCX, never replaced by it; the gate-2 hold is lifted
    assert t[16]["reader"] == "docx" and t[16]["status"] == "verified" and t[16]["text_replaced"] == 0
    assert t[16]["xml_verified"] == 1 and t[16]["gate2_publishable"] == 0 and t[16]["publishable_text"] == 1
    assert "sigla_rendering" in json.loads(t[16]["classes"])
    assert [d["disposition"] for d in diffs if d["row_key"] == "today:16"] == ["verify_only"]

    # B2 route: the folder's line text replaces the PDF text
    assert t[21]["reader"] == "b2_rows" and t[21]["status"] == "accepted" and t[21]["text_replaced"] == 1
    for c in ("bidi_reorder", "escape", "catchword"):
        assert c in json.loads(t[21]["classes"])
    assert texts[("dbo_UnitTranscription", 19)]["text"] == B2_C400_TEXT

    # new rows: no comparator, nothing accepted under the default policy, structure counted
    new = {k: r for k, r in rows.items() if k.startswith("new:")}
    assert set(new) == {f"new:dbo_UnitTranscription:2|{ALMA_B}", "new:dbo_UnitTranscription:20|990002",
                        "new:dbo_UnitTranscription:21|990003"}
    for r in new.values():
        assert r["status"] == "no_comparator" and r["text_accepted"] == 0 and r["hold_reason"] == "needs_acceptance"
    assert new[f"new:dbo_UnitTranscription:2|{ALMA_B}"]["reader"] == "b2_rows"
    assert json.loads(new["new:dbo_UnitTranscription:20|990002"]["structure"])["tables"] == 1
    assert texts[("dbo_UnitTranscription", 20)]["text"] == "כותרת\n1 תא ראשון\n2 תא שני"

    # totals, and nothing from a description
    assert counts["rows.today"] == 21 and counts["rows.new"] == 3 and counts["replaced_rows"] == 3
    assert counts["status.verified"] == 1 and counts["status.held"] == 1 and counts["xml.holds_lifted"] == 1
    assert not any(r["unit_table"] == "dbo_UnitFreeDescription" for r in rows.values())
    report = open(os.path.join(world["out"], "GATE3_REPORT.md"), encoding="utf-8").read()
    assert "**Result: PASS**" in report and "`restored_prefix`" in report and "today:8" in report


def test_plain_policy_accepts_folder_b_and_plain_docx_new_rows_only(world):
    assert g3.main(_args(world, "--allow-single-run", "--new-rows-policy", "plain")) == 0
    rows, counts, _texts, _diffs, checks = _load(world)
    assert rows[f"new:dbo_UnitTranscription:2|{ALMA_B}"]["acceptance_basis"] == "rule:folder_b_text"
    assert rows["new:dbo_UnitTranscription:21|990003"]["acceptance_basis"] == "rule:plain_docx"
    held = rows["new:dbo_UnitTranscription:20|990002"]
    assert held["text_accepted"] == 0 and held["hold_reason"] == "docx_structure"
    assert counts["new.accepted"] == 2 and checks["T05_new_rows_need_acceptance"]["passed"] == 1


def test_an_owner_approval_accepts_a_held_row(world):
    assert g3.main(_args(world, "--allow-single-run")) == 0
    rows, *_ = _load(world)
    r8 = rows["today:8"]
    approvals = os.path.join(world["root"], "approvals.json")
    with open(approvals, "w", encoding="utf-8") as fh:
        json.dump({"approvals": [{"durable_key": r8["durable_key"], "today_hash": r8["today_hash"],
                                  "new_hash": r8["new_hash"], "by": "owner", "note": "דהו read as דהו"}]}, fh, ensure_ascii=False)
    args = [a for a in _args(world, "--allow-single-run")]
    args[args.index("--approvals") + 1] = approvals
    assert g3.main(args) == 0
    rows, _counts, _texts, diffs, checks = _load(world)
    r8 = rows["today:8"]
    assert r8["status"] == "approved" and r8["text_replaced"] == 1 and r8["acceptance_basis"] == "approval:owner"
    assert r8["publishable_text"] == 0  # the unit's visibility hold still stands
    assert [d["disposition"] for d in diffs if d["row_key"] == "today:8"] == ["approved"]
    assert checks["T04_replaced_blocks_in_accepted_classes"]["passed"] == 1


def test_two_runs_must_agree(world, capsys):
    assert g3.main(_args(world, "--allow-single-run")) == 0
    first = os.path.join(world["root"], "first_summary.json")
    shutil.copy(os.path.join(world["out"], "gate3_summary.json"), first)
    assert g3.main(_args(world, "--previous-summary", first)) == 0
    assert "[ok  ] T01_two_runs_identical" in capsys.readouterr().out
    with open(first, encoding="utf-8") as fh:
        s = json.load(fh)
    s["meta"]["digest"] = "0" * 64
    with open(first, "w", encoding="utf-8") as fh:
        json.dump(s, fh)
    assert g3.main(_args(world, "--previous-summary", first)) == 1
    assert "[FAIL] T01_two_runs_identical" in capsys.readouterr().out
    assert g3.main(_args(world)) == 1  # neither a previous run nor --allow-single-run
    assert g3.main(_args(world, "--previous-summary", os.path.join(world["root"], "missing.json"))) == 2


def test_gate_fails_when_a_lossy_diff_is_accepted(world, monkeypatch, capsys):
    # the B1 XML of unit 1 loses its second line; today's XML row would lose it too
    with open(os.path.join(world["withpos"], "100000", "100000.xml"), "w", encoding="utf-8") as fh:
        fh.write('<root><col n="1"><row u="1"><t><w u="2">שורה ראשונה</w></t></row></col></root>')
    assert g3.main(_args(world, "--allow-single-run")) == 0
    rows, *_ = _load(world)
    assert rows["today:1"]["status"] == "held" and rows["today:1"]["text_replaced"] == 0  # the deletion is unclassified
    monkeypatch.setattr(g3, "classify_block", lambda T, N: ({"regrouped"}, True))
    assert g3.main(_args(world, "--allow-single-run")) == 1
    out = capsys.readouterr().out
    assert "[FAIL] T06_no_xml_regression" in out
    rows, *_ = _load(world)
    assert rows["today:1"]["text_replaced"] == 1 and rows["today:1"]["lost_today_lines"] == 1


def test_gate_fails_when_a_replaced_row_has_no_source_link(world, monkeypatch, capsys):
    real = g3.read_unit

    def unlinked(unit, inp, nopos, withpos):
        ut = real(unit, inp, nopos, withpos)
        ut.source_link = None
        return ut

    monkeypatch.setattr(g3, "read_unit", unlinked)
    assert g3.main(_args(world, "--allow-single-run")) == 1
    assert "[FAIL] T02_replaced_rows_have_source_link" in capsys.readouterr().out


def test_missed_expectation_and_foreign_db(world, capsys):
    assert g3.main(_args(world, "--allow-single-run", "--expect", "replaced_rows=1")) == 1
    assert "[FAIL] E_replaced_rows: expected 1, got 3" in capsys.readouterr().out
    foreign = os.path.join(world["out"], "gate3.db")
    os.remove(foreign)
    con = sqlite3.connect(foreign)
    con.execute("CREATE TABLE not_ours (x)")
    con.commit()
    con.close()
    with pytest.raises(SystemExit, match="refusing to overwrite"):
        g3.main(_args(world, "--allow-single-run"))
    assert g3.main(_args(world, "--allow-single-run", "--expect-file", os.path.join(world["root"], "missing.json"))) == 2


def test_docx_reader_renders_numbering_footnotes_and_drops_hidden_text(tmp_path):
    w = build_world(str(tmp_path), text=True)
    d = g3.read_docx(read_docx_path(w, "2000000", "20000000000015_CUL_T-S 1.5_Tc"))
    assert g3.docx_text(d) == DOCX_UNIT3_TEXT
    assert d.footnotes == [(1, "זו הערת שוליים")] and d.headers and d.headers[0][0] == g3.canon("T-S 1.5 Cambridge")
    assert "0001" not in g3.docx_text(d) and d.structure["hidden_runs"] == 1
    table = g3.read_docx(read_docx_path(w, "2000000", "20000000000031_CUL_T-S 3.1_Tc"))
    assert table.structure["tables"] == 1 and table.structure["table_rows"] == 2
    assert g3.docx_text(table).split("\n") == ["כותרת", "1 תא ראשון", "2 תא שני"]


@pytest.mark.parametrize("today, new, expected, ok", [
    (["ג שנים"], ['ועד י"ג שנים'], {"restored_prefix"}, True),
    ([".1 אב גד"], ["1. אב גד"], {"numbering"}, True),
    (["-1 אב"], ["1- אב"], {"numbering"}, True),
    (["שלו\x0e ולכ\x04"], ["שלום ולכן"], {"control_chars", "spacing"}, True),
    (['ואיל"'], ["ואילך"], {"final_letters"}, True),
    (["דנו ברחמי"], ["דנום ברחמים"], {"final_letters", "spacing"}, True),
    (["]אב[ ג"], ["[אב] ג"], {"bracket"}, True),
    (["{ה\\\\/\\\\/ב}"], ["{ה\\/\\/ב}"], {"escape"}, True),
    (["12.528: TS ראה"], ["ראה TS 12.528:"], {"bidi_reorder", "spacing"}, True),
    (["@]......["], ["[......]@"], {"bidi_reorder", "bracket"}, True),
    (["אב גד", "הו"], ["אב גד הו"], {"regrouped"}, True),
    (["אבגד", "הו"], ["אב גדהו"], {"regrouped", "spacing"}, True),
    ([], ["%ואמר%"], {"catchword"}, True),
    (["<<הן>>"], ["%(הן)%"], {"catchword"}, True),
    (["العقل ناقص"], ["العقلا ناقص"], {"arabic_alef", "spacing"}, True),
    (["א רֶֶֿץ"], ["אֶֿרֶץ"], {"diacritics", "spacing"}, True),
    (["בּוֹ"], ["בּוֹ"], {"diacritics"}, True),            # precomposed בּ וֹ vs letter + mark
    (["מֶלֶך |ְ \x12[דִּיר"], ["מֶלֶךְ | אַ[דִּיר"], {"diacritics", "control_chars", "spacing"}, True),
    (["ש0ב ואוזאנה"], ["שֵב ואוזאנה"], {"vowel_glyphs", "diacritics", "spacing"}, True),   # an ASCII glyph for a vowel point
    (["י<צ \x0fק"], ["יְצֹק"], {"vowel_glyphs", "diacritics", "spacing"}, True),
    (['!"'], ["تاييده"], {"arabic_glyphs"}, True),                           # Arabic reduced to junk
    (["ادام ا\x19"], ["ادام الله"], {"control_chars", "spacing"}, True),
    (["ש0ב"], ["שב"], {"unclassified"}, False),                              # a stray digit with no point behind it
    (["סוֹף מֵ<ה"], ["סוֹף מֵאָה"], {"vowel_glyphs", "diacritics", "spacing"}, True),   # a vocalised letter as a symbol
    (["כּ חְִים"], ["כְּאַחִים"], {"vowel_glyphs", "diacritics", "spacing"}, True),      # a vocalised letter dropped
    (["מה"], ["מאה"], {"unclassified"}, False),                              # the same letter without a point: a reading
    (["1"], [], {"numbering"}, True),                                        # a footnote marker on its own line
    (["שורה"], ["?ש?ורה"], {"sigla_rendering"}, True),
    ([], ["1.(שורה 8) יש הערת גליון"], {"extra_line"}, True),
    (["אבג דהו"], ["אבד דהו"], {"unclassified"}, False),
    (["אב"], [], {"unclassified"}, False),
])
def test_classes_of_the_pdf_artefacts(today, new, expected, ok):
    cls, got_ok = g3.classify_block(today, new)
    assert got_ok is ok and expected <= cls, cls
    accepted = cls <= g3.ACCEPTED_CLASSES
    assert accepted == (ok and not (cls & g3.VERIFY_ONLY_CLASSES))
