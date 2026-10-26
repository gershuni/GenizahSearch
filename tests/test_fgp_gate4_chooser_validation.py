# -*- coding: utf-8 -*-
"""Gate 4 of the FGP source-folders plan (``scripts/fgp_gate4_chooser_validation.py``).

Gates 1-3 run on the fixture text world; gate 4 must build the candidate sidecar by the declared
rules (ids kept, gate-3 texts and gate-2 credits applied, gate-2 holds and retirements excluded,
new rows under the plain policy with the image and shadowing rules), list every carried edition
through the real ``FgpService``, keep every page key, disposition every default change on the
reference set, and fail on a lost edition, an undispositioned default change, an unapproved page
key change, a breached budget, a changed digest and a missed expectation.
"""
from __future__ import annotations

import json
import os
import sqlite3

import pytest

from scripts import check_fgp_sidecar as check_sidecar
from scripts import fgp_association_inventory as gate1
from scripts import fgp_gate2_rules as gate2
from scripts import fgp_gate3_text_pipeline as g3
from scripts import fgp_gate4_chooser_validation as g4
from shared import fgp_service as fs
from tests.fgp_fixture_world import ALMA_A, ALMA_B, ALMA_C, B2_C400_TEXT, DOCX_UNIT3_TEXT, build_world, rows as _rows

HTR = {
    ALMA_A: [{"p_num": 1, "text": "שורה ראשונה שורה שנייה ועוד מילים רבות כאן בדף הזה"},
             {"p_num": 2, "text": "דף שני עם טקסט אחר לגמרי שאינו דומה"}],
    ALMA_B: [{"p_num": 1, "text": "שורה אחת שורה שתיים ועוד כמה מילים נוספות"}],
    ALMA_C: [{"p_num": 1, "text": "טקסט פשוט שורה שנייה ועוד מילים"}],
}


@pytest.fixture
def world(tmp_path):
    w = build_world(str(tmp_path), text=True)
    inv = os.path.join(w["out"], "inventory.db")
    assert gate1.main(["--nopos", w["nopos"], "--withpos", w["withpos"], "--fist", w["fist"], "--fjms", w["fjms"],
                       "--nli", w["nli"], "--fgp-db", w["fgp"], "--out-dir", w["out"]]) == 0
    assert gate2.main(["--inventory", inv, "--fgp-db", w["fgp"], "--fist", w["fist"], "--out-dir", w["out"]]) == 0
    assert g3.main(["--inventory", inv, "--gate2", os.path.join(w["out"], "gate2.db"), "--fgp-db", w["fgp"],
                    "--nopos", w["nopos"], "--withpos", w["withpos"], "--today-files", w["today_files"],
                    "--approvals", os.path.join(w["root"], "none.json"), "--out-dir", w["out"], "--allow-single-run"]) == 0
    w["htr"] = os.path.join(w["root"], "htr.json")
    with open(w["htr"], "w", encoding="utf-8") as fh:
        json.dump(HTR, fh, ensure_ascii=False)
    w["candidate"] = os.path.join(w["out"], g4.CANDIDATE_NAME)
    return w


def _args(w, *extra):
    return ["--fgp-db", w["fgp"], "--gate2", os.path.join(w["out"], "gate2.db"), "--gate3", os.path.join(w["out"], "gate3.db"),
            "--inventory", os.path.join(w["out"], "inventory.db"), "--fist", w["fist"], "--nli", w["nli"],
            "--libraries-csv", os.path.join(w["root"], "none.csv"), "--approvals", os.path.join(w["root"], "none.json"),
            "--exclusions", os.path.join(w["root"], "none.json"), "--out-dir", w["out"], "--htr-json", w["htr"], *extra]


def _load(w):
    db = os.path.join(w["out"], "gate4.db")
    cand = {r["id"]: r for r in _rows(db, "SELECT * FROM candidate_rows")}
    disp = {r["key"]: r for r in _rows(db, "SELECT * FROM row_dispositions")}
    sel = _rows(db, "SELECT * FROM selectability ORDER BY sys_id, uid")
    dec = _rows(db, "SELECT * FROM default_decisions ORDER BY sys_id, p_num")
    counts = {r["key"]: r["value"] for r in _rows(db, "SELECT * FROM counts")}
    checks = {r["name"]: r for r in _rows(db, "SELECT * FROM checks")}
    return cand, disp, sel, dec, counts, checks


def test_candidate_follows_the_rules_and_the_chooser_lists_every_edition(world, capsys):
    rc = g4.main(_args(world, "--allow-single-run", "--expect", "rows.candidate=13"))
    out = capsys.readouterr().out
    assert rc == 0, out
    cand, disp, sel, dec, counts, checks = _load(world)
    assert all(c["passed"] == 1 for c in checks.values()), [n for n, c in checks.items() if not c["passed"]]

    # R1/R2: today's ids kept; retired and gate-2-held rows excluded, each with its reason
    assert sorted(i for i in cand if cand[i]["origin"] == "today") == [1, 2, 5, 7, 12, 13, 15, 16, 18, 19, 20, 21]
    assert disp["6"]["disposition"] == "retired_alias" and json.loads(disp["6"]["detail"])["successor_id"] == 5
    assert disp["14"]["disposition"] == "gate2_hold" and "fist_comment_no_publish" in disp["14"]["detail"]
    assert disp["3"]["disposition"] == "gate2_hold" and "file_absent_in_delivery" in disp["3"]["detail"]
    assert disp["8"]["disposition"] == "gate2_hold" and "tmp_DoubleNotToDisplay" in disp["8"]["detail"]
    assert disp["11"]["disposition"] == "gate2_hold" and "association_unconfirmed" in disp["11"]["detail"]

    # R3: gate 3's text where it replaced, today's elsewhere (the XML row is verified, never replaced)
    con = sqlite3.connect(f"file:{world['candidate']}?mode=ro", uri=True)
    text = {r[0]: r[1:] for r in con.execute("SELECT id, content, text_source, text_provenance, source_credit_he, origin FROM fgp_transcriptions")}
    meta = dict(con.execute("SELECT key, value FROM fgp_meta").fetchall())
    con.close()
    assert text[5][0] == DOCX_UNIT3_TEXT and text[5][1] == "gate3:docx" and text[5][2] == "gate3:docx"
    assert text[21][0] == B2_C400_TEXT and text[21][2] == "gate3:b2_rows"
    assert text[1][2] == "today" and text[16][2] == "today"
    # R4: gate 2's credits, the generic attribution where gate 2 ruled generic
    assert text[13][3] == "CUDL Catalog, Cambridge, 2010"
    assert text[15][3] == gate2.GENERIC_CREDIT[0]
    # R5: the plain DOCX of unit 21 enters as a whole-document row of its AlmaId with the team credit;
    # the table DOCX is held; the B2 row of unit 2 would shadow today's whole-document row 12 on the same manuscript
    assert cand[22]["origin"] == "new" and cand[22]["row_key"] == f"new:dbo_UnitTranscription:21|{ALMA_C}"
    assert text[22][4] == "new" and text[22][2] == "gate3:docx" and "גנוזות" in text[22][3]
    assert disp[f"new:dbo_UnitTranscription:20|{ALMA_B}"]["disposition"] == "docx_multicolumn_table"
    assert disp[f"new:dbo_UnitTranscription:2|{ALMA_B}"]["disposition"] == "shadows_whole_doc_row"
    # R6
    assert meta["version"] == g4.CANDIDATE_VERSION and meta["candidate"] and meta["rows"] == "13"

    # C2: the real service lists every carried edition; the lost ones carry their dispositions
    svc = fs.FgpService(db_path=world["candidate"])
    assert [s["uid"] for s in svc.get_fgp_sources_for_fragment(ALMA_C)] == ["fgp:5", "fgp:22"]
    assert svc.get_version() == g4.CANDIDATE_VERSION
    svc.close()
    lost = {(r["sys_id"], r["uid"]): r for r in sel if r["today_listed"] and not r["candidate_listed"]}
    assert lost[(ALMA_C, "fgp:6")]["disposition"] == "retired_alias" and lost[(ALMA_C, "fgp:6")]["detail"] == "fgp:5"
    assert lost[(ALMA_C, "fgp:17")]["disposition"] == "gate2_hold"
    assert counts["selectability.new_unlisted"] == 0 and counts["selectability.new_listed"] == 1

    # C4 / C5: page keys unchanged; the two default changes are the new edition and the excluded row
    assert counts["page_assignment.unchanged"] == 12
    changed = {(d["sys_id"], d["p_num"]): d for d in dec if d["changed"]}
    assert changed[(ALMA_C, 1)]["disposition"] == "new_edition" and changed[(ALMA_C, 1)]["candidate_uid"] == "fgp:22"
    assert changed[("990004", 1)]["disposition"] == "row_excluded"
    assert counts["defaults.changed.unexplained"] == 0 if "defaults.changed.unexplained" in counts else True
    assert counts["reference.manuscripts"] == 4 and counts["defaults.pages"] == 5


def test_hold_policy_adds_no_new_row(world, capsys):
    assert g4.main(_args(world, "--allow-single-run", "--new-rows-policy", "hold")) == 0, capsys.readouterr().out
    cand, disp, sel, dec, counts, checks = _load(world)
    assert counts["rows.new"] == 0 and counts["new_held.policy_hold"] == 3 and counts["rows.candidate"] == 12


def test_two_runs_must_agree(world, capsys):
    assert g4.main(_args(world, "--allow-single-run")) == 0
    summary = os.path.join(world["out"], "gate4_summary.json")
    first = os.path.join(world["root"], "first.json")
    os.replace(summary, first)
    assert g4.main(_args(world, "--previous-summary", first)) == 0, capsys.readouterr().out
    with open(first, encoding="utf-8") as fh:
        data = json.load(fh)
    data["meta"]["digest"] = "0" * 64
    with open(first, "w", encoding="utf-8") as fh:
        json.dump(data, fh)
    assert g4.main(_args(world, "--previous-summary", first)) == 1
    assert _load(world)[5]["C1_two_runs_identical"]["passed"] == 0
    assert g4.main(_args(world)) == 1  # neither a previous summary nor --allow-single-run


def test_release_check_requires_the_file_gate_4_validated(world, capsys):
    # Codex 2026-10-26: built_at alone let an edited candidate pass; the summary now carries the file's checksum
    assert g4.main(_args(world, "--allow-single-run")) == 0, capsys.readouterr().out
    summary = os.path.join(world["out"], "gate4_summary.json")
    with open(summary, encoding="utf-8") as fh:
        data = json.load(fh)
    assert data["meta"]["candidate_sha256"] == check_sidecar.sha256_of(world["candidate"])
    check = ["--db", world["candidate"], "--version", g4.CANDIDATE_VERSION, "--rows", "13", "--gate4-summary", summary]
    capsys.readouterr()
    assert check_sidecar.main(check) == 0, capsys.readouterr().out

    no_sha = os.path.join(world["root"], "no_sha.json")
    data["meta"].pop("candidate_sha256")
    with open(no_sha, "w", encoding="utf-8") as fh:
        json.dump(data, fh)
    assert check_sidecar.main(check[:-1] + [no_sha]) == 1
    assert "records no candidate_sha256" in capsys.readouterr().out

    con = sqlite3.connect(world["candidate"])
    con.execute("UPDATE fgp_transcriptions SET content = content || 'א' WHERE id = 1")
    con.commit()
    con.close()
    assert check_sidecar.main(check) == 1
    out = capsys.readouterr().out
    assert "changed after the run" in out and "RESULT: FAIL" in out


def test_gate_fails_when_a_listed_edition_disappears(world, monkeypatch, capsys):
    real = g4.source_rows

    def hide_row_5(svc, sid):
        out = real(svc, sid)
        if svc._db_path == world["candidate"]:
            return [s for s in out if s["uid"] != "fgp:5"]
        return out

    monkeypatch.setattr(g4, "source_rows", hide_row_5)
    assert g4.main(_args(world, "--allow-single-run")) == 1
    cand, disp, sel, dec, counts, checks = _load(world)
    assert checks["C2_selectability_dispositioned"]["passed"] == 0
    assert counts["selectability.lost.unexplained"] == 1


def test_gate_fails_when_an_exclusion_has_no_declared_reason(world, monkeypatch):
    real = g4.plan_candidate

    def drop_a_kept_row(*a, **kw):
        plan = real(*a, **kw)
        k = next(k for k in plan["kept"] if k["id"] == 5)
        plan["kept"].remove(k)
        plan["excluded"].append((5, "oops", ""))
        return plan

    monkeypatch.setattr(g4, "plan_candidate", drop_a_kept_row)
    assert g4.main(_args(world, "--allow-single-run")) == 1
    checks = _load(world)[5]
    assert checks["C2_selectability_dispositioned"]["passed"] == 0


def test_listed_exclusions_apply_and_a_stale_key_fails(world, capsys):
    """R2d: a listed durable key excludes its today row with the listed reason (a declared selectability
    disposition); a listed key that names no row fails C9, so the list cannot go stale unnoticed."""
    assert g4.main(_args(world, "--allow-single-run")) == 0, capsys.readouterr().out
    con = sqlite3.connect(f"file:{world['candidate']}?mode=ro", uri=True)
    key16 = con.execute("SELECT durable_key FROM fgp_transcriptions WHERE id = 16").fetchone()[0]
    con.close()
    assert key16
    listed = os.path.join(world["root"], "exclusions.json")

    def write(entries):
        with open(listed, "w", encoding="utf-8") as fh:
            json.dump({"exclusions": entries}, fh, ensure_ascii=False)

    write([{"durable_key": key16, "reason": "keyboard_dummy"},
           {"durable_key": "dbo_UnitTranscription:999999|990000000000000000|", "reason": "non_manuscript"}])
    args = _args(world, "--allow-single-run")
    args[args.index("--exclusions") + 1] = listed
    assert g4.main(args) == 1
    cand, disp, sel, dec, counts, checks = _load(world)
    assert checks["C9_exclusions_matched"]["passed"] == 0 and counts["exclusions.unmatched"] == 1
    assert disp["16"]["disposition"] == "keyboard_dummy" and 16 not in cand
    write([{"durable_key": key16, "reason": "keyboard_dummy"}])
    assert g4.main(args) == 0, capsys.readouterr().out
    cand, disp, sel, dec, counts, checks = _load(world)
    assert all(c["passed"] == 1 for c in checks.values()), [n for n, c in checks.items() if not c["passed"]]
    assert counts["excluded.keyboard_dummy"] == 1 and counts.get("exclusions.unmatched", 0) == 0
    write([{"durable_key": key16, "reason": "looks_odd"}])
    with pytest.raises(ValueError):
        g4.load_exclusions(listed)


def test_gate_fails_on_an_undispositioned_default_change(world, monkeypatch):
    real = fs.choose_default_source
    calls = {"n": 0}

    def flip_every_second_call(*a, **kw):
        d = real(*a, **kw)
        calls["n"] += 1
        if calls["n"] % 2 == 0:  # the candidate's call of each page
            d = dict(d, reason="tampered")
        return d

    monkeypatch.setattr(fs, "choose_default_source", flip_every_second_call)
    assert g4.main(_args(world, "--allow-single-run")) == 1
    cand, disp, sel, dec, counts, checks = _load(world)
    assert checks["C5_defaults_dispositioned"]["passed"] == 0 and counts["defaults.changed.unexplained"] >= 1


def test_page_key_change_needs_an_approval(world, monkeypatch, tmp_path):
    real = g4.write_candidate

    def move_row_21(path, *a, **kw):
        real(path, *a, **kw)
        con = sqlite3.connect(path)
        con.execute("UPDATE fgp_transcriptions SET c_number='C401' WHERE id=21")
        con.commit()
        con.close()

    monkeypatch.setattr(g4, "write_candidate", move_row_21)
    assert g4.main(_args(world, "--allow-single-run")) == 1
    cand, disp, sel, dec, counts, checks = _load(world)
    assert checks["C4_page_assignment"]["passed"] == 0 and counts["page_assignment.unapproved"] == 1
    approvals = os.path.join(str(tmp_path), "approvals.json")
    with open(approvals, "w", encoding="utf-8") as fh:
        json.dump({"page_assignment": [{"id": 21, "field": "c_number", "candidate": "C401", "by": "owner"}]}, fh)
    args = _args(world, "--allow-single-run")
    args[args.index("--approvals") + 1] = approvals
    assert g4.main(args) == 0
    assert _load(world)[4]["page_assignment.approved"] == 1


def test_breached_budgets_fail(world, monkeypatch):
    monkeypatch.setitem(g4.BUDGETS, "latency.page.p95_ms", 0.0)
    monkeypatch.setitem(g4.BUDGETS, "memory.app_open_mb", -1.0)
    assert g4.main(_args(world, "--allow-single-run")) == 1
    checks = _load(world)[5]
    assert checks["C6_latency.page.cand"]["passed"] == 0 and checks["C7_memory.app_open_mb"]["passed"] == 0
    assert checks["C6_latency.load.cand"]["passed"] == 1


def test_missed_expectation_and_foreign_candidate(world, capsys):
    assert g4.main(_args(world, "--allow-single-run", "--expect", "rows.candidate=99")) == 1
    assert _load(world)[5]["E_rows.candidate"]["passed"] == 0
    con = sqlite3.connect(world["candidate"])
    con.execute("DROP TABLE fgp_meta")
    con.commit()
    con.close()
    with pytest.raises(SystemExit):
        g4.main(_args(world, "--allow-single-run"))


def test_new_row_image_rules():
    """R5's image rules on synthetic inputs: gallery mismatch, unknown image, an image another unit serves,
    a whole-document row shadowed by per-image rows, and the row that passes."""
    texts = sqlite3.connect(":memory:")
    texts.execute("CREATE TABLE unit_texts (unit_table, unit_id, text)")
    texts.executemany("INSERT INTO unit_texts VALUES (?,?,?)", [("dbo_UnitTranscription", i, "טקסט %d" % i) for i in range(1, 7)])
    today = {1: {"id": 1, "sys_id": "A", "c_number": "C10", "doc_relation": "Digital Edition", "language": None, "content": "אבג"},
             2: {"id": 2, "sys_id": "B", "c_number": None, "doc_relation": "Digital Edition", "language": None, "content": "דהו"},
             3: {"id": 3, "sys_id": "D", "c_number": None, "doc_relation": "Digital Edition", "language": None, "content": " "},
             4: {"id": 4, "sys_id": "D", "c_number": None, "doc_relation": "Digital Edition", "language": None, "content": "טסט טסט"},
             5: {"id": 5, "sys_id": "D", "c_number": None, "doc_relation": "Digital Translation", "language": "English",
                 "content": "phrastic"},
             # the same image of one manuscript served by three units: a full text, a stub that is a part of it,
             # and a short DIFFERENT text (another layer or reading); then two readings of about one length
             6: {"id": 6, "sys_id": "F", "c_number": "C20", "doc_relation": "Digital Edition", "language": None,
                 "content": "שורה ראשונה של העתקה מלאה ושורה שנייה ושלישית ורביעית וחמישית וכן הלאה עד סוף הדף"},
             7: {"id": 7, "sys_id": "F", "c_number": "C20", "doc_relation": "Digital Edition", "language": None,
                 "content": "...שורה שנייה ושלי[שית]"},
             8: {"id": 8, "sys_id": "F", "c_number": "C21", "doc_relation": "Digital Edition", "language": None,
                 "content": "עליהן אין מתענין ולא מספידין בשבתות ובימים טובים"},
             9: {"id": 9, "sys_id": "F", "c_number": "C21", "doc_relation": "Digital Edition", "language": None,
                 "content": "ולא והרי החובל בחבירו דאיכא ממון ומלקות"},
             10: {"id": 10, "sys_id": "F", "c_number": "C20", "doc_relation": "Digital Edition", "language": None,
                  "content": "ואמר ליה רב"},
             # R2e: a contact list filed as a transcription; R2d: a row listed as keyboard noise
             11: {"id": 11, "sys_id": "J", "c_number": None, "doc_relation": "Digital Edition", "language": None,
                  "content": "פלוני -02-5550101\nאלמוני (שוכר)- 0505550102\nשכן: 054-5550103\nכתובת: a.b@example.co.il"},
             12: {"id": 12, "sys_id": "J", "c_number": "C30", "doc_relation": "Digital Edition", "language": None,
                  "content": "יעכיכ עיכי"}}
    g2 = {i: {"outcome": "kept", "publishable": 1, "hold_reasons": None, "credit_he": "x", "credit_en": "x", "unit_id": 100 * i,
              "unit_table": "dbo_UnitTranscription", "durable_key": f"k{i}"} for i in range(1, 13)}

    def new(uid, sid, reader, path, structure=None, **extra):
        d = {"row_key": f"new:dbo_UnitTranscription:{uid}|{sid}", "unit_table": "dbo_UnitTranscription", "unit_id": uid,
             "sys_id": sid, "kind": "transcription", "language": None, "reader": reader, "source_path": path,
             "structure": json.dumps(structure) if structure else None, "status": "no_comparator", "basename": f"u{uid}",
             "durable_key": f"n{uid}"}
        d.update(extra)
        return d

    texts.executemany("INSERT INTO unit_texts VALUES (?,?,?)", [
        ("dbo_UnitTranscription", 7, "test"), ("dbo_UnitTranscription", 8, "נסיון\nנא להתעלם!!!"),
        ("dbo_UnitTranscription", 9, "ggg"), ("dbo_UnitTranscription", 10, "טקסט של ממש"), ("dbo_UnitTranscription", 11, "5555"),
        ("dbo_UnitTranscription", 12, LEGACY_FONT_TEXT), ("dbo_UnitTranscription", 13, VOCALISED_TEXT),
        ("dbo_UnitTranscription", 14, "טקסט שהקריאה שלו שונה מן הקובץ של פרידברג"),
        ("dbo_UnitTranscription", 15, "מנהג אשכנז מנהג איטליה סדר רב עמרם"),
        ("dbo_UnitTranscription", 16, "1. שורה ראשונה¹ של כתב היד\n2. שורה שנייה\n\n1 הערת שוליים"),
        ("dbo_UnitTranscription", 17, "מהדורה של כל כתב היד בקובץ וורד"), ("dbo_UnitTranscription", 18, "מהדורה של תמונה אחת"),
        ("dbo_UnitTranscription", 19, "פלונית: 0525550104\nאלמונית 054-5550105\nמהלשכה: 02-5550106"),
        ("dbo_UnitTranscription", 20, "בצק לילדים\nהמצרכים:\n4 ביצים\n1 כוס שמן"),
        ("dbo_UnitTranscription", 21, "גרסה ראשונה של העתקה שהוחלפה בגרסה שנייה"),
        ("dbo_UnitTranscription", 22, "תרגום של מכתב שאינו מן הקטע הזה"),
        ("dbo_UnitTranscription", 23, "העתקה שפרויקט פרידברג מציג באתרו")])
    g3_new = [new(1, "A", "b2_rows", "C10\\data.js"),          # image 10 already served by unit 100 on A
              new(2, "A", "b2_rows", "C11\\data.js"),          # gallery lists image 11 under Z, not A
              new(3, "A", "b1_xml", "99\\99.xml"),             # unknown image
              new(4, "B", "b2_rows", "C12\\data.js"),          # B has a whole-document row -> would shadow it
              new(5, "A", "docx", "x\\word\\u5.docx"),         # whole-document on A, which has per-image rows
              new(6, "C", "b2_rows", "C13\\data.js"),          # passes: image 13 confirmed for C
              new(7, "C", "docx", "x\\word\\u7.docx"),         # test files and dummies
              new(8, "C", "docx", "x\\word\\u8.docx"),
              new(9, "C", "docx", "x\\word\\u9.docx"),
              new(10, "C", "docx", "x\\word\\u10.docx"),       # held by its FIST comment
              new(11, "C", "docx", "x\\word\\u11.docx"),
              new(12, "C", "docx", "x\\word\\u12.docx"),       # a legacy-font DOCX: vowel points where letters should be
              new(13, "E", "docx", "x\\word\\u13.docx"),       # a vocalised piyyut: every mark follows a letter -> passes
              # the Word-file rulings of 2026-10-26: a difference gate 3 could not classify against the PDF holds,
              # a real table holds, and hidden runs, numbering, footnotes and a one-column table render and enter
              new(14, "G", "docx", "x\\word\\u14.docx", status="pdf_held", held_blocks=2, classes=json.dumps({"unclassified": 2}),
                  yardstick="pdf"),
              new(15, "G", "docx", "x\\word\\u15.docx", {"tables": 1, "table_rows": 15, "table_max_columns": 5}),
              new(16, "G", "docx", "x\\word\\u16.docx", {"hidden_runs": 40, "numbered_paragraphs": 2, "footnotes": 1,
                                                          "tables": 1, "table_rows": 2, "table_max_columns": 1},
                  status="checked", yardstick="pdf"),
              # both new on manuscript H, the whole-document DOCX listed first: the per-image row enters and the
              # whole-document row is held, since the chooser would drop it beside a per-image row
              new(17, "H", "docx", "x\\word\\u17.docx", status="checked", yardstick="pdf"),
              new(18, "H", "b2_rows", "C14\\data.js"),
              new(19, "K", "docx", "x\\word\\u19.docx"),       # R2e: personal phone numbers -> held
              new(20, "K", "docx", "x\\word\\u20.docx"),       # R2d: listed as not a manuscript -> held
              new(21, "L", "docx", "x\\word\\u21.docx"),       # R2f: an older version FIST superseded -> held
              new(22, "M", "docx", "x\\word\\u22.docx", kind="translation"),  # R2g: status-1 translation -> held
              new(23, "N", "docx", "x\\word\\u23.docx")]       # R2g: a status-1 transcription enters
    b_numbers = {"10": {"A"}, "11": {"Z"}, "12": {"B"}, "13": {"C"}, "14": {"H"}}
    units = {("dbo_UnitTranscription", i): (200, 1, f"u{i}") for i in range(1, 24)}
    ctx = {"parts": {}, "catalog": {}, "book": {}, "bibref": {}}
    comments = {("dbo_UnitTranscription", 10): "אין לפרסם עד לקריאה מול מקור"}
    exclusions = {"k12": "keyboard_dummy", "n20": "non_manuscript", "k99": "keyboard_dummy"}
    superseded = {("dbo_UnitTranscription", 21): 777}
    status_one = {("dbo_UnitTranscription", 22), ("dbo_UnitTranscription", 23)}
    plan = g4.plan_candidate(today, g2, {}, {}, g3_new, texts, b_numbers, units, {}, ctx, "plain", comments=comments,
                             exclusions=exclusions, superseded=superseded, status_one=status_one)
    # R2: a carried row must have a text that is not a test file; an English translation is not a test file.
    # R2d/R2e: a listed row and a contact list are excluded with their reasons.
    # R2c: only the stub CONTAINED in the full text of the same image is retired; the short different text and the
    # two readings of about one length stay.
    assert [k["id"] for k in plan["kept"]] == [1, 2, 5, 6, 8, 9, 10]
    assert plan["excluded"][:2] == [(3, "empty_text", "gate3:"), (4, "test_or_dummy", "טסט טסט")]
    assert [e[:2] for e in plan["excluded"][2:]] == [(11, "personal_data"), (12, "keyboard_dummy"), (7, "stub_retired")]
    assert json.loads(plan["excluded"][2][2]) == {"emails": 1, "phones": 3}
    stub = json.loads(plan["excluded"][4][2])
    assert stub["longer"] == "fgp:6" and stub["contained"] == 1.0
    assert plan["exclusions_unmatched"] == ["k99"]
    held = {rk.split(":")[2].split("|")[0]: d for rk, d, _ in plan["held"]}
    assert held == {"1": "image_already_served", "2": "image_alma_mismatch", "3": "image_unknown",
                    "4": "shadows_whole_doc_row", "5": "whole_doc_shadowed", "7": "test_or_dummy", "8": "test_or_dummy",
                    "9": "test_or_dummy", "10": "fist_comment_no_publish", "11": "test_or_dummy", "12": "legacy_font",
                    "14": "pdf_held", "15": "docx_multicolumn_table", "17": "whole_doc_shadowed", "19": "personal_data",
                    "20": "non_manuscript", "21": "superseded_version", "22": "status1_translation"}
    assert json.loads(dict((rk, d) for rk, _, d in plan["held"])["new:dbo_UnitTranscription:21|L"]) == {"newer_signature": 777}
    assert json.loads(dict((rk, d) for rk, _, d in plan["held"])["new:dbo_UnitTranscription:15|G"])["columns"] == 5
    assert [(r["row_key"], r["c_number"], r["sys_id"]) for r in plan["new_rows"]] == [
        ("new:dbo_UnitTranscription:18|H", "C14", "H"), ("new:dbo_UnitTranscription:6|C", "C13", "C"),
        ("new:dbo_UnitTranscription:13|E", None, "E"), ("new:dbo_UnitTranscription:16|G", None, "G"),
        ("new:dbo_UnitTranscription:23|N", None, "N")]
    assert plan["yardstick"] == {"new:dbo_UnitTranscription:6|C": "none", "new:dbo_UnitTranscription:18|H": "none",
                                 "new:dbo_UnitTranscription:13|E": "none", "new:dbo_UnitTranscription:16|G": "pdf",
                                 "new:dbo_UnitTranscription:23|N": "none"}
    assert plan["new_rows"][0]["id"] == 13 and "גנוזות" in plan["new_rows"][0]["source_credit_he"]
    assert g4.plan_candidate(today, g2, {}, {}, g3_new, texts, b_numbers, units, {}, ctx, "hold")["new_rows"] == []


@pytest.mark.parametrize("text, relation, expected", [
    ("test", "Digital Edition", True), ("ggg", "Digital Edition", True), ("241241", "Digital Edition", True),
    ("בדיקה", "Digital Edition", True), ("נסיון", "Digital Translation", True), ("טסט מפולים", "Digital Edition", True),
    ("נסיון\nנא להתעלם!!!", "Digital Edition", True), ("phrastic", "Digital Translation", False),
    ("בדיקה של חמץ", "Digital Edition", False), ("בגמר דין שמא", "Digital Edition", False),
    ("test " * 20, "Digital Edition", False), ("", "Digital Edition", False),
])
def test_test_or_dummy_rule(text, relation, expected):
    assert g4.test_or_dummy(text, relation) is expected


@pytest.mark.parametrize("text, expected", [
    ("פלוני -02-5550101\nאלמוני - 0505550102\nשכן: 054-5550103", True),  # three phone numbers (synthetic)
    ("כתובת: someone@example.co.il", True),                              # an email address
    ("פלונית: 0525550104\nשכן: 0545550105", False),                     # two are not enough
    (".....כך צריך להניח שליד למעלה כדי שיפ@גע@ בה", False),             # the transcribers' @ notation
    ("FGP 89745 JTS ENA 2941/5r\nשנת 4851 ליצירה 1090 למספרם", False),   # catalogue and year numbers
    ("C112408 C112409 C112410 20000040006041", False),                    # image and file numbers
])
def test_personal_data_rule(text, expected):
    assert g4.personal_data(text) is expected


def test_load_superseded_reads_fist_versions():
    """R2f: a unit whose FIST signature has RecordStatus 5 is an older version; the newer status-0 version of the
    same unit (SourceId, SubId, UnitNumInSource) is recorded when FIST has one. Status 1 is not read as superseded."""
    fist = sqlite3.connect(":memory:")
    fist.execute("CREATE TABLE dbo_Signature (SignatureId, SourceId, SubId, UnitNumInSource, Version, RecordStatus)")
    fist.executemany("INSERT INTO dbo_Signature VALUES (?,?,?,?,?,?)", [
        (1, 200, 1, 5, 1, 5), (2, 200, 1, 5, 2, 0),   # version 1 superseded by version 2
        (3, 200, 1, 6, 1, 5),                          # superseded, no newer version in the export
        (4, 107, 9, 7, 1, 1),                          # status 1: meaning unknown, left alone
        (5, 107, 9, 8, 1, 0)])
    fist.execute("CREATE TABLE dbo_UnitTranscription (UnitTranscriptionId, SignatureId)")
    fist.execute("CREATE TABLE dbo_UnitTranslation (UnitTranslationId, SignatureId)")
    fist.executemany("INSERT INTO dbo_UnitTranscription VALUES (?,?)", [(10, 1), (11, 2), (12, 3), (13, 4), (14, 5)])
    fist.execute("INSERT INTO dbo_UnitTranslation VALUES (20, 1)")
    assert g4.load_superseded(fist) == {("dbo_UnitTranscription", 10): 2, ("dbo_UnitTranscription", 12): None,
                                        ("dbo_UnitTranslation", 20): 2}
    assert g4.load_status_one(fist) == {("dbo_UnitTranscription", 13)}
    old = sqlite3.connect(":memory:")
    old.execute("CREATE TABLE dbo_Signature (SignatureId, SourceId, SubId, UnitNumInSource)")
    assert g4.load_superseded(old) == {} and g4.load_status_one(old) == set()


def test_text_ratio_reads_the_whole_text():
    """Two texts with one 4,000-letter beginning and different endings are not the same text."""
    base = ("אבגדהוזחטיכלמנסעפצקרשת" * 200)[:4000]
    a, b = base + "שלום עולם " * 120, base + "ותפארת ישראל " * 90
    assert g4.text_ratio(a, b) < g4.SAME_TEXT_RATIO
    assert g4.text_ratio(a, a) == 1.0


@pytest.mark.parametrize("short, expected", [
    ("שורה שנייה ושלישית", 1.0),                          # a part of the longer text
    ("...שו[רה] שנייה ושלי?שית?", 1.0),                    # with the transcribers' marks
    ("ואמר ליה רב", 0.0),                                  # another text
    ("שורה ראשונה ואמר ליה רב", 10 / 19),                 # one ten-letter run of the longer text, of 19 letters
])
def test_contained_share(short, expected):
    long = "שורה ראשונה של העתקה מלאה ושורה שנייה ושלישית ורביעית וחמישית וכן הלאה עד סוף הדף"
    assert g4.contained_share(short, long) == pytest.approx(expected, abs=0.05)


# A DOCX typed in a pre-Unicode Hebrew font: the letters arrive as vowel points after spaces (RNL Arab.-Yevr. 72).
LEGACY_FONT_TEXT = "העתקות: 1א: " + " ".join(["ַָָ ם ַהו זַֽֿ בַ ַֻהם בו"] * 4)
# A vocalised piyyut: every mark follows its letter.
VOCALISED_TEXT = "אֵל פְּנֵה לַנֱּחֳלִים מִנְּדוּדְךָ יִדְאָבוּ\nבְּחֹדֶשׁ סִיוָן נִתְּנָה דָת לְנוֹחֲלֶיהָ\n" * 6


@pytest.mark.parametrize("text, expected", [
    (LEGACY_FONT_TEXT, True), (VOCALISED_TEXT, False), ("טקסט פשוט בלי ניקוד " * 10, False),
    ("ַא " * 9, False),                           # fewer than 10 orphan marks: left alone
    ("ַא " * 10 + "א" * 200, False),              # 10 orphans but under a tenth of the letters
    ("ַא " * 10 + "א" * 50, True),
    ("] ָ ְ ָ (שם) " * 10, False),           # marks standing alone: a placeholder for an illegible letter
    ("Muḥammad and Buṭrus " * 10, False),       # Latin letters with decomposed diacritics
])
def test_legacy_font_rule(text, expected):
    assert g4.legacy_font(text) is expected
