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
            "--out-dir", w["out"], "--htr-json", w["htr"], *extra]


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
    rc = g4.main(_args(world, "--allow-single-run", "--expect", "rows.candidate=14"))
    out = capsys.readouterr().out
    assert rc == 0, out
    cand, disp, sel, dec, counts, checks = _load(world)
    assert all(c["passed"] == 1 for c in checks.values()), [n for n, c in checks.items() if not c["passed"]]

    # R1/R2: today's ids kept; retired and gate-2-held rows excluded, each with its reason
    assert sorted(i for i in cand if cand[i]["origin"] == "today") == [1, 2, 5, 7, 12, 13, 14, 15, 16, 18, 19, 20, 21]
    assert disp["6"]["disposition"] == "retired_alias" and json.loads(disp["6"]["detail"])["successor_id"] == 5
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
    assert disp[f"new:dbo_UnitTranscription:20|{ALMA_B}"]["disposition"] == "docx_structure"
    assert disp[f"new:dbo_UnitTranscription:2|{ALMA_B}"]["disposition"] == "shadows_whole_doc_row"
    # R6
    assert meta["version"] == g4.CANDIDATE_VERSION and meta["candidate"] and meta["rows"] == "14"

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
    assert counts["page_assignment.unchanged"] == 13
    changed = {(d["sys_id"], d["p_num"]): d for d in dec if d["changed"]}
    assert changed[(ALMA_C, 1)]["disposition"] == "new_edition" and changed[(ALMA_C, 1)]["candidate_uid"] == "fgp:22"
    assert changed[("990004", 1)]["disposition"] == "row_excluded"
    assert counts["defaults.changed.unexplained"] == 0 if "defaults.changed.unexplained" in counts else True
    assert counts["reference.manuscripts"] == 4 and counts["defaults.pages"] == 5


def test_hold_policy_adds_no_new_row(world, capsys):
    assert g4.main(_args(world, "--allow-single-run", "--new-rows-policy", "hold")) == 0, capsys.readouterr().out
    cand, disp, sel, dec, counts, checks = _load(world)
    assert counts["rows.new"] == 0 and counts["new_held.policy_hold"] == 3 and counts["rows.candidate"] == 13


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
             3: {"id": 3, "sys_id": "D", "c_number": None, "doc_relation": "Digital Edition", "language": None, "content": " "}}
    g2 = {i: {"outcome": "kept", "publishable": 1, "hold_reasons": None, "credit_he": "x", "credit_en": "x", "unit_id": 100 * i,
              "unit_table": "dbo_UnitTranscription", "durable_key": f"k{i}"} for i in (1, 2, 3)}

    def new(uid, sid, reader, path, structure=None):
        return {"row_key": f"new:dbo_UnitTranscription:{uid}|{sid}", "unit_table": "dbo_UnitTranscription", "unit_id": uid,
                "sys_id": sid, "kind": "transcription", "language": None, "reader": reader, "source_path": path,
                "structure": json.dumps(structure) if structure else None, "status": "no_comparator", "basename": f"u{uid}",
                "durable_key": f"n{uid}"}

    g3_new = [new(1, "A", "b2_rows", "C10\\data.js"),          # image 10 already served by unit 100 on A
              new(2, "A", "b2_rows", "C11\\data.js"),          # gallery lists image 11 under Z, not A
              new(3, "A", "b1_xml", "99\\99.xml"),             # unknown image
              new(4, "B", "b2_rows", "C12\\data.js"),          # B has a whole-document row -> would shadow it
              new(5, "A", "docx", "x\\word\\u5.docx"),         # whole-document on A, which has per-image rows
              new(6, "C", "b2_rows", "C13\\data.js")]          # passes: image 13 confirmed for C
    b_numbers = {"10": {"A"}, "11": {"Z"}, "12": {"B"}, "13": {"C"}}
    units = {("dbo_UnitTranscription", i): (200, 1, f"u{i}") for i in range(1, 7)}
    ctx = {"parts": {}, "catalog": {}, "book": {}, "bibref": {}}
    plan = g4.plan_candidate(today, g2, {}, {}, g3_new, texts, b_numbers, units, {}, ctx, "plain")
    # R2: a carried row must have a text
    assert [k["id"] for k in plan["kept"]] == [1, 2] and plan["excluded"] == [(3, "empty_text", "gate3:")]
    held = {rk.split(":")[2].split("|")[0]: d for rk, d, _ in plan["held"]}
    assert held == {"1": "image_already_served", "2": "image_alma_mismatch", "3": "image_unknown",
                    "4": "shadows_whole_doc_row", "5": "whole_doc_shadowed"}
    assert [(r["row_key"], r["c_number"], r["sys_id"]) for r in plan["new_rows"]] == [("new:dbo_UnitTranscription:6|C", "C13", "C")]
    assert plan["new_rows"][0]["id"] == 4 and "גנוזות" in plan["new_rows"][0]["source_credit_he"]
    assert g4.plan_candidate(today, g2, {}, {}, g3_new, texts, b_numbers, units, {}, ctx, "hold")["new_rows"] == []
