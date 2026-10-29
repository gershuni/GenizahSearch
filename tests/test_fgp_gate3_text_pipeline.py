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
    GATE3_REREAD_ARGS,
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
            "--approvals", os.path.join(w["root"], "no-approvals.json"), "--out-dir", w["out"], *GATE3_REREAD_ARGS,
            *extra]


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
              "footnote_apart", "header_apart", "bidi_reorder", "escape", "footnote_marker"):
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
    assert rows["new:dbo_UnitTranscription:21|990003"]["acceptance_basis"] == "rule:docx_no_pdf_text"
    held = rows["new:dbo_UnitTranscription:20|990002"]
    assert held["text_accepted"] == 0 and held["hold_reason"] == "docx_multicolumn_table"
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


def test_footnote_marker_is_a_superscript_and_one_marker_to_the_comparison():
    """The owner's ruling of 2026-10-26: the reference is a superscript digit on its word (the search
    tokenizer drops it, so the word stays searchable); against the PDF's plain digit it is one marker."""
    assert g3.classify_pair("הערה1 בטקסט", "הערה¹ בטקסט") == ({"footnote_marker"}, True)
    assert g3.line_key("הערה1 בטקסט", True) == g3.line_key("הערה¹ בטקסט", False)
    assert g3.canon("כתואר¹") == g3.canon("כתואר")
    # the XML editions' ² siglum (the DOCX types a backslash) is not a marker: letters-and-digits equality still verifies
    cls, ok = g3.classify_pair("טבילה לבעלי קריין² אלא אמ' רבא", "טבילה לבעלי קריין\\ אלא אמ' רבא")
    assert ok and "footnote_marker" not in cls and "unclassified" not in cls


def test_new_docx_rows_are_checked_against_their_pdf(tmp_path, monkeypatch):
    """The owner's ruling of 2026-10-26: a new DOCX row with FGP's PDF beside it is diffed against the
    PDF's text through the classes -- clean: ``checked`` and accepted under ``plain``; a difference
    outside the classes: ``pdf_held``; no PDF text: accepted by the no-yardstick rule, counted apart."""
    w = build_world(str(tmp_path), text=True)
    pdf = os.path.join(w["nopos"], "2000000", "pdf", "20000000000032_CUL_T-S 3.2_Tc.pdf")
    open(pdf, "wb").close()  # FGP's PDF beside unit 21's plain DOCX; its text comes from the patched extractor
    assert gate1.main(["--nopos", w["nopos"], "--withpos", w["withpos"], "--fist", w["fist"], "--fjms", w["fjms"],
                       "--nli", w["nli"], "--fgp-db", w["fgp"], "--out-dir", w["out"]]) == 0
    assert gate2.main(["--inventory", os.path.join(w["out"], "inventory.db"), "--fgp-db", w["fgp"], "--fist", w["fist"],
                       "--out-dir", w["out"]]) == 0
    texts = {}
    monkeypatch.setattr(g3, "pdf_text_of", lambda path, cache=None: texts.get(os.path.basename(path)))
    rendering = g3.docx_text(g3.read_docx(os.path.join(w["nopos"], "2000000", "word", "20000000000032_CUL_T-S 3.2_Tc.docx")))
    key = "new:dbo_UnitTranscription:21|990003"

    texts[os.path.basename(pdf)] = rendering  # the PDF prints what the DOCX says
    assert g3.main(_args(w, "--allow-single-run", "--new-rows-policy", "plain")) == 0
    rows, counts, _t, _d, checks = _load(w)
    r = rows[key]
    assert r["status"] == "checked" and r["yardstick"] == "pdf" and r["text_accepted"] == 1
    assert r["acceptance_basis"] == "rule:docx_checked_against_pdf" and counts["new.yardstick_pdf"] == 1
    assert counts["new.accepted_by.rule:docx_checked_against_pdf"] == 1
    assert checks["T09_accepted_new_docx_rows_clean_against_pdf"]["passed"] == 1
    held = rows["new:dbo_UnitTranscription:20|990002"]
    assert held["hold_reason"] == "docx_multicolumn_table" and held["status"] == "no_comparator"

    texts[os.path.basename(pdf)] = "שורה שאין לה שום קשר למה שבקובץ"  # a reading the classes cannot explain
    assert g3.main(_args(w, "--allow-single-run", "--new-rows-policy", "plain")) == 0
    rows, counts, _t, diffs, checks = _load(w)
    r = rows[key]
    assert r["status"] == "pdf_held" and r["hold_reason"] == "pdf_held" and r["text_accepted"] == 0
    assert r["held_blocks"] >= 1 and "unclassified" in json.loads(r["classes"])
    assert counts["new.held.pdf_held"] == 1 and counts["new.accepted"] == 1  # the folder-B row still enters
    assert any(d["row_key"] == key and d["disposition"] == "held" for d in diffs)

    texts.clear()  # no PDF text at all (a scan): accepted by the no-yardstick rule, counted as such
    assert g3.main(_args(w, "--allow-single-run", "--new-rows-policy", "plain")) == 0
    rows, counts, _t, _d, _c = _load(w)
    assert rows[key]["status"] == "no_comparator" and rows[key]["acceptance_basis"] == "rule:docx_no_pdf_text"
    assert counts["new.accepted_by.rule:docx_no_pdf_text"] == 1 and "new.yardstick_pdf" not in counts


# --- §9: the glyph re-read's checks T11-T14 (each must be able to fail) ------------------------------------------

def _reread_counts(**over):
    c = {"glyph_reread.rows.accepted": 10, "glyph_reread.c3.rows": 5, "glyph_reread.c3.weight": 1000,
         "glyph_reread.c3.judged": 950, "glyph_reread.c3.agree": 940, "glyph_reread.c3.decode_error_weight": 0,
         "glyph_reread.c5.denominator": 100, "glyph_reread.c5.recovered": 80, "glyph_reread.c5.unresolved": 5,
         "glyph_reread.map_entries": 3, "glyph_reread.map.certified": 2, "glyph_reread.map.certified by shape": 1}
    c.update(over)
    import collections
    return collections.Counter(c)


def _t(C):
    return {c["name"]: c["passed"] for c in g3.run_checks([], [], C, {}, "d" * 16, None, True)
            if c["name"].startswith("T1") and c["name"] not in ("T10_final_letters_accurate", "T15_team_markings_letters")}


def test_reread_checks_pass_a_sound_run():
    assert _t(_reread_counts()) == {"T11_glyph_reread_faithful": 1, "T12_glyph_reread_c3": 1,
                                    "T13_glyph_reread_c5": 1, "T14_glyph_reread_map_certified": 1}


@pytest.mark.parametrize("over, failing", [
    ({"glyph_reread.errors": 1}, "T11_glyph_reread_faithful"),
    ({"glyph_reread.certify_errors": 1}, "T11_glyph_reread_faithful"),
    ({"glyph_reread.unfaithful_bench": 1}, "T11_glyph_reread_faithful"),
    ({"glyph_reread.hold.C1: the extractor no longer reproduces the stored text": 1}, "T11_glyph_reread_faithful"),
    ({"glyph_reread.c3.judged": 800}, "T12_glyph_reread_c3"),
    ({"glyph_reread.c3.decode_error_weight": 10}, "T12_glyph_reread_c3"),
    ({"glyph_reread.c5.recovered": 0}, "T13_glyph_reread_c5"),
    ({"glyph_reread.c5.unresolved": 30}, "T13_glyph_reread_c5"),
    ({"glyph_reread.map_entries": 4}, "T14_glyph_reread_map_certified"),
])
def test_each_reread_check_fails_on_its_own_fault(over, failing):
    res = _t(_reread_counts(**over))
    assert res[failing] == 0 and all(v == 1 for k, v in res.items() if k != failing)


def test_t11_fails_a_run_whose_only_pdf_failed():
    # no measured row, no benchmark weight, no letters: the recorded error alone must fail T11 (Codex, round 2)
    import collections
    for key in ("glyph_reread.errors", "glyph_reread.certify_errors"):
        assert _t(collections.Counter({key: 1}))["T11_glyph_reread_faithful"] == 0
    assert _t(collections.Counter())["T11_glyph_reread_faithful"] == 1


# --------------------------------------------------------------------------- marking team (owner, 2026-10-28)

def _marked_docx(path, paragraphs, styles=""):
    """A DOCX of runs: each paragraph a list of (text, props); props: font, outline, strike, style, br."""
    import zipfile
    from xml.sax.saxutils import escape
    ns = 'xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main"'

    def run(text, props):
        rpr = ""
        if props.get("style"):
            rpr += f'<w:rStyle w:val="{props["style"]}"/>'
        if props.get("font"):
            rpr += f'<w:rFonts w:ascii="{props["font"]}" w:cs="{props["font"]}"/>'
        if props.get("outline"):
            rpr += "<w:outline/>"
        if props.get("strike"):
            rpr += "<w:strike/>"
        if props.get("superscript"):
            rpr += '<w:vertAlign w:val="superscript"/>'
        if props.get("rtl"):
            rpr += "<w:rtl/>"
        br ="<w:br/>" if props.get("br") else ""
        return f'<w:r><w:rPr>{rpr}</w:rPr>{br}<w:t xml:space="preserve">{escape(text)}</w:t></w:r>'

    body = "".join("<w:p>" + "".join(run(t, p) for t, p in para) + "</w:p>" for para in paragraphs)
    with zipfile.ZipFile(path, "w") as z:
        z.writestr("word/document.xml", f'<?xml version="1.0" encoding="UTF-8"?><w:document {ns}><w:body>{body}</w:body></w:document>')
        if styles:
            z.writestr("word/styles.xml", f'<?xml version="1.0" encoding="UTF-8"?><w:styles {ns}>{styles}</w:styles>')


YAD = {"font": "Guttman Yad"}
OUTLINE = {"outline": True}


def _marked(tmp_path, paragraphs, styles="", markings=True):
    p = str(tmp_path / "t.docx")
    _marked_docx(p, paragraphs, styles)
    return g3.docx_text(g3.read_docx(p, markings=markings))


def test_completions_are_bracketed_inside_words_and_across_runs(tmp_path):
    text = _marked(tmp_path, [
        [("לא", YAD), (" כל שכן", {})],                                 # at a line start
        [("טעו", {}), ("א", YAD), (" שאר", {})],                          # inside a word
        [("מה", YAD), (" ", {}), ("את", YAD), (" עבד", {})],              # one span over runs, spaces outside
    ])
    assert text.split("\n") == ["[לא] כל שכן", "טעו[א] שאר", "[מה את] עבד"]


def test_a_lacuna_marker_or_an_already_bracketed_span_is_left_alone(tmp_path):
    text = _marked(tmp_path, [[("ביו", {}), ("<..>", YAD)], [("[", {}), ("אב", YAD), ("]ג", {})]])
    assert text.split("\n") == ["ביו<..>", "[אב]ג"]


def test_doubtful_letters_are_marked_after_their_points(tmp_path):
    mark = g3.DOUBTFUL_MARK
    text = _marked(tmp_path, [
        [("עב", {}), ("ד", OUTLINE), (" להון", {})],
        [("ש", {}), ("בָ", OUTLINE), ("ת", {})],                          # a point stays on its letter
        [("לא", {}), ("ד", {"font": "Guttman Yad", "outline": True})],     # a doubtful completion
    ])
    assert text.split("\n") == [f"עבד{mark} להון", f"שבָ{mark}ת", f"לא[ד{mark}]"]


def test_deleted_letters_get_double_brackets(tmp_path):
    text = _marked(tmp_path, [[("למ", {}), ("ל", {"strike": True}), ("חצה", {})]])
    assert text == f"למ{g3.DELETED_OPEN}ל{g3.DELETED_CLOSE}חצה"


def test_interlinear_additions_get_leiden_slashes(tmp_path):
    sup = {"superscript": True}
    text = _marked(tmp_path, [
        [("והשבים ", {}), ("אומרין", sup), (" מה", {})],
        [("מ", {}), ("ת", sup), ("נחמהה", {})],
        [("אינו חייב", {}), ("0", sup), (" עסקיו", {}), (".", sup)],     # a sign without a letter stays as written
    ])
    o, c = g3.INTERLINEAR_OPEN, g3.INTERLINEAR_CLOSE
    assert text.split("\n") == [f"והשבים {o}אומרין{c} מה", f"מ{o}ת{c}נחמהה", "אינו חייב0 עסקיו."]


def test_the_rashi_font_is_set_apart(tmp_path):
    rashi = {"font": "Rashi"}
    text = _marked(tmp_path, [[("בכי", {}), ("כר", rashi), (" ר' אבון", {})], [("מ<.. ...", {}), (">", rashi)]])
    assert text.split("\n") == [f"בכי{g3.SET_APART_OPEN}כר{g3.SET_APART_CLOSE} ר' אבון", "מ<.. ...>"]


@pytest.mark.parametrize("props, expect", [
    ({"font": "Guttman Yad", "strike": True}, "{do}[א]{dc}"),
    ({"superscript": True, "strike": True}, "{do}{io}א{ic}{dc}"),
    ({"font": "Guttman Yad", "superscript": True}, "{io}[א]{ic}"),
    ({"font": "Rashi", "superscript": True}, "{io}{so}א{sc}{ic}"),
])
def test_marks_on_one_span_nest_in_one_order(tmp_path, props, expect):
    text = _marked(tmp_path, [[("ב", {}), ("א", props), ("ג", {})]])
    assert text == "ב" + expect.format(do=g3.DELETED_OPEN, dc=g3.DELETED_CLOSE, io=g3.INTERLINEAR_OPEN,
                                       ic=g3.INTERLINEAR_CLOSE, so=g3.SET_APART_OPEN, sc=g3.SET_APART_CLOSE) + "ג"


def test_a_latin_shelfmark_typed_right_to_left_keeps_words_direction(tmp_path):
    """Word shows B.M. Or. 6581,2 [1154,2] + TS 12.183 because the digits, spaces and dots sit in right-to-left runs;
    the plain characters alone read .Or .M.B. An RLM at each edge of a stretch of such runs keeps Word's display."""
    rtl = {"rtl": True}
    text = _marked(tmp_path, [
        [("12.183 ", rtl), ("TS", {}), (" + [1154,2] 6581,2 .", rtl), ("Or", {}), (" .", rtl), ("M", {}), (".", rtl),
         ("B", {}), (" ", rtl)],
        [("מהדורת", rtl), (" האקדמיה: 52", rtl), ("7", rtl), (", 10 ואילך", rtl)],    # Hebrew only: nothing added
    ])
    m = g3.RLM
    first, second = text.split("\n")
    assert first == f"{m}12.183 {m}TS{m} + [1154,2] 6581,2 .{m}Or{m} .{m}M{m}.{m}B{m}", ascii(first)
    assert first.replace(m, "") == "12.183 TS + [1154,2] 6581,2 .Or .M.B"   # the trailing space still goes
    assert second == "מהדורת האקדמיה: 527, 10 ואילך"
    plain = _marked(tmp_path, [[("12.183 ", rtl), ("TS", {})]], markings=False)
    assert plain == "12.183 TS"                                                      # other teams: unchanged


def test_marks_come_from_a_run_style_and_never_cross_a_line(tmp_path):
    styles = ('<w:style w:type="character" w:styleId="Yad"><w:rPr><w:rFonts w:cs="Guttman Yad"/></w:rPr></w:style>')
    text = _marked(tmp_path, [[("אב", {"style": "Yad"}), ("גד", {"style": "Yad", "br": True})]], styles=styles)
    assert text.split("\n") == ["[אב]", "[גד]"]
    assert not g3._SENTINEL_RE.search(text)


def test_without_the_marking_profile_the_text_is_plain(tmp_path):
    text = _marked(tmp_path, [[("לא", YAD), (" עב", {}), ("ד", OUTLINE), (" אומרין", {"superscript": True}),
                               (" בכי", {}), ("כר", {"font": "Rashi"})]], markings=False)
    assert text == "לא עבד אומרין בכיכר"


@pytest.mark.parametrize("pdf, word, passed", [
    ("לא ל שכן\x0eר", "[לא] כ֯ל שכן ר", True),                         # the PDF lost the doubtful כ; a box for ר
    ("שלום עליכם ושלום לכם", "שלום עליכם", False),                     # a line missing from the Word text
    ("שלום עליכם", "ברוך הבא לכולם", False),                           # another text
    ("אב", "אבגדהוזחטיכלמנסעפצקרשת", False),                            # far more letters than the PDF shows
    ("דדדדף ב שלום", "דף ב שלום", True),                                # fake bold: four times one letter
    ("עב להון", "עבד֯ להון", True),                                      # a doubtful letter the PDF drew as strokes
])
def test_team_letter_check(pdf, word, passed):
    assert g3.team_letters(pdf, word)["passed"] is passed


def _team_row(**kw):
    row = {"row_key": "today:1", "today_id": 1, "unit_table": "u", "unit_id": 1, "basename": "13500010000011",
           "kind": "transcription", "reader": "docx", "source_path": r"1350001\word\13500010000011.docx",
           "source_link": "fist_filename_docx", "today_text_source": "pdf_rtl", "status": "approved", "text_replaced": 1,
           "text_accepted": 1, "acceptance_basis": g3.TEAM_RULE_BASIS, "lost_today_lines": 0, "yardstick": None,
           "team_letters": json.dumps({"found": 0.995, "excess": 0.05, "pdf_letters": 900, "word_letters": 1000,
                                       "passed": True})}
    row.update(kw)
    return row


def _t15(rows):
    import collections
    return next(c["passed"] for c in g3.run_checks(rows, [], collections.Counter(), {}, "d" * 16, None, True)
                if c["name"] == "T15_team_markings_letters")


def test_t15_passes_a_team_row_accepted_on_its_letters():
    assert _t15([_team_row()]) == 1


@pytest.mark.parametrize("row", [
    _team_row(team_letters=json.dumps({"found": 0.97, "excess": 0.05, "pdf_letters": 900, "word_letters": 1000,
                                       "passed": False})),                       # too few of the PDF's letters
    _team_row(team_letters=json.dumps({"found": 0.995, "excess": 0.2, "pdf_letters": 900, "word_letters": 1000,
                                       "passed": False})),                       # far more letters than the PDF
    _team_row(team_letters=None),                                                # accepted without a check
    _team_row(source_path=r"2000000\word\20000000000011.docx", acceptance_basis=None),  # marks read in another team
])
def test_t15_fails(row):
    assert _t15([row]) == 0
