# -*- coding: utf-8 -*-
"""Gate 5 of the FGP source-folders plan (``scripts/fgp_gate5_positions.py``).

The position world of ``tests/fgp_fixture_world.py`` gives the B1 folder 100000 and the B2 folder C400 word
boxes, synthetic photographs whose ink sits in those boxes, and the renditions NLI would serve (one of them
rotated by 180 degrees), so the geometry check runs offline. Gate 5 must find the transform, land every landmark
inside the declared tolerance, build both stores tied to the candidate rows' text hashes with spans that
reproduce the content, choose a schema within the declared limits, and fail on a displayed image that is another
photograph, on a payload no candidate row carries, on a changed digest and on a missed expectation.
"""
from __future__ import annotations

import hashlib
import json
import os
import shutil
import sqlite3

import pytest

from scripts import fgp_association_inventory as gate1
from scripts import fgp_gate2_rules as gate2
from scripts import fgp_gate3_text_pipeline as g3
from scripts import fgp_gate4_chooser_validation as g4
from scripts import fgp_gate5_positions as g5
from scripts.fgp_rebuild_text_and_credit import parse_fgp_xml
from tests.fgp_fixture_world import (
    ALMA_A, ALMA_B, ALMA_C, B1_POS_POSITIONED, B1_POS_TEXT,
    B2_C400_TEXT, B2_POS_DISPLAY_ROT, B2_POS_POSITIONED, add_position_gallery, build_world, rows as _rows,
    write_other_displayed_image, write_position_content,
)

HTR = {
    ALMA_A: [{"p_num": 1, "text": "בראשית ברא אלהים את השמים ואת הארץ"}, {"p_num": 2, "text": "דף שני עם טקסט אחר לגמרי"}],
    ALMA_B: [{"p_num": 1, "text": "שורה אחת שורה שתיים ועוד כמה מילים נוספות"}],
    ALMA_C: [{"p_num": 1, "text": "טקסט פשוט שורה שנייה ועוד מילים"}],
}


@pytest.fixture(scope="module")
def world(tmp_path_factory):
    root = str(tmp_path_factory.mktemp("gate5"))
    w = build_world(root, text=True)
    w["cache"] = os.path.join(root, "cache")
    write_position_content(root, w["cache"])
    add_position_gallery(w)
    inv = os.path.join(w["out"], "inventory.db")
    assert gate1.main(["--nopos", w["nopos"], "--withpos", w["withpos"], "--fist", w["fist"], "--fjms", w["fjms"],
                       "--nli", w["nli"], "--fgp-db", w["fgp"], "--out-dir", w["out"]]) == 0
    assert gate2.main(["--inventory", inv, "--fgp-db", w["fgp"], "--fist", w["fist"], "--out-dir", w["out"]]) == 0
    assert g3.main(["--inventory", inv, "--gate2", os.path.join(w["out"], "gate2.db"), "--fgp-db", w["fgp"],
                    "--nopos", w["nopos"], "--withpos", w["withpos"], "--today-files", w["today_files"],
                    "--approvals", os.path.join(root, "none.json"), "--out-dir", w["out"], "--allow-single-run"]) == 0
    htr = os.path.join(root, "htr.json")
    with open(htr, "w", encoding="utf-8") as fh:
        json.dump(HTR, fh, ensure_ascii=False)
    assert g4.main(["--fgp-db", w["fgp"], "--gate2", os.path.join(w["out"], "gate2.db"), "--gate3", os.path.join(w["out"], "gate3.db"),
                    "--inventory", inv, "--fist", w["fist"], "--nli", w["nli"], "--libraries-csv", os.path.join(root, "none.csv"),
                    "--approvals", os.path.join(root, "none.json"), "--out-dir", w["out"], "--htr-json", htr,
                    "--allow-single-run"]) == 0
    w["candidate"] = os.path.join(w["out"], g4.CANDIDATE_NAME)
    return w


def _args(w, out_dir, *extra):
    return ["--withpos", w["withpos"], "--inventory", os.path.join(w["out"], "inventory.db"),
            "--gate3", os.path.join(w["out"], "gate3.db"), "--gate4", os.path.join(w["out"], "gate4.db"),
            "--candidate", w["candidate"], "--nli", w["nli"], "--libraries-csv", os.path.join(w["root"], "none.csv"),
            "--out-dir", out_dir, "--cache-dir", w.get("cache_override") or w["cache"], "--offline", "--latency-others", "5", *extra]


def _load(out_dir):
    db = os.path.join(out_dir, "gate5.db")
    sample = {r["number"]: r for r in _rows(db, "SELECT * FROM sample")}
    marks = _rows(db, "SELECT * FROM landmarks ORDER BY rowid")
    dirs = {r["dir"]: r for r in _rows(db, "SELECT * FROM dirs")}
    payloads = _rows(db, "SELECT * FROM payloads ORDER BY image, col, durable_key")
    counts = {r["key"]: r["value"] for r in _rows(db, "SELECT * FROM counts")}
    checks = {r["name"]: r for r in _rows(db, "SELECT * FROM checks")}
    return sample, marks, dirs, payloads, counts, checks


def _candidate(w, rid):
    con = sqlite3.connect(f"file:{w['candidate']}?mode=ro", uri=True)
    try:
        dk, content = con.execute("SELECT durable_key, content FROM fgp_transcriptions WHERE id=?", (rid,)).fetchone()
    finally:
        con.close()
    return dk, content, hashlib.sha256(content.encode("utf-8")).hexdigest()


def test_geometry_readers_reproduce_the_text_with_offsets(world):
    b1 = g5.B1Geometry(os.path.join(world["withpos"], "100000"))
    assert b1.error is None and not b1.lenient
    assert b1.text == parse_fgp_xml(os.path.join(world["withpos"], "100000", "100000.xml")) == B1_POS_TEXT
    col = b1.columns[0]
    assert col["image"] == "100000" and col["frame"] == [1200.0, 1500.0] and len(col["rows"]) == 6
    assert len(col["words"]) == B1_POS_POSITIONED            # every word with a box, dict or list form
    texts = {w["text"] for w in col["words"]}
    assert "תהו" not in texts and "פני" in texts             # uid 13 has no box; uid 19 is the list form
    assert all(b1.text[w["start"]:w["end"]] == w["text"] for w in col["words"])
    assert b1.photo()[1] == [40.0, 30.0, 1160.0, 1470.0]
    b2 = g5.B2Geometry(os.path.join(world["withpos"], "C400"), (1500, 2000))
    assert b2.error is None and b2.text == g3.read_b2(os.path.join(world["withpos"], "C400"))[0] == B2_C400_TEXT
    col = b2.columns[0]
    assert len(col["words"]) == B2_POS_POSITIONED and b2.spans_dropped == 1   # "זר" is in no line; the _test box is dropped
    assert all(b2.text[w["start"]:w["end"]] == w["text"] for w in col["words"])
    assert col["words"][0]["box"][0] >= 300 and col["rows"][0]["baseline"][0][1] >= 400   # the column offset is added
    assert b2.photo()[1] == [300.0, 400.0, 1300.0, 1800.0]
    assert g5.B2Geometry(os.path.join(world["withpos"], "C400"), None).error == "no_frame"


def test_landmarks_land_and_the_store_is_tied_to_the_candidate_revision(world, tmp_path):
    out = str(tmp_path / "out")
    rc = g5.main(_args(world, out, "--images", "100000,400", "--allow-single-run", "--expect", "payloads=2"))
    sample, marks, dirs, payloads, counts, checks = _load(out)
    assert rc == 0, [n for n, c in checks.items() if not c["passed"]]
    # G1: both images pass; the B2 rendition is served rotated and the transform says so
    assert sample["100000"]["passed"] == 1 and sample["400"]["passed"] == 1
    t1, t2 = json.loads(sample["100000"]["transform"]), json.loads(sample["400"]["transform"])
    assert t1["rot"] == 0 and not t1["mirror"] and abs(t1["dx"]) <= 6 and abs(t1["dy"]) <= 6   # one coarse pixel
    assert t1["angle"] == 0 and abs(t1["sx"] - 1.0) < 0.012
    assert t2["rot"] == 180 and not t2["mirror"] and abs(abs(t2["angle"]) - (B2_POS_DISPLAY_ROT - 180)) <= 0.5
    for s in sample.values():
        assert s["found"] >= g5.TOLERANCE["min_found"] and s["cells"] >= g5.TOLERANCE["min_cells"]
        assert s["max_r"] <= g5.TOLERANCE["max_r_over_h"] * s["pitch_disp"]
        assert s["rms_r"] <= g5.TOLERANCE["rms_r_over_h"] * s["pitch_disp"]
    assert sample["100000"]["frame_is_master"] == 0 and sample["400"]["frame_is_master"] == 1
    assert sample["100000"]["canvas_label_agrees"] == 1 and sample["400"]["gallery_manifest_agree"] == 1
    assert all(m["found"] for m in marks if m["number"] == "100000")
    # the tie: one payload per (row, image, column), keyed by the candidate row's durable key and content hash
    dk1, content1, h1 = _candidate(world, 1)
    dk21, content21, h21 = _candidate(world, 21)
    assert [(p["durable_key"], p["text_hash"], p["image"], p["col"]) for p in payloads] == [(dk1, h1, "100000", "1"), (dk21, h21, "400", "1")]
    assert all(p["overlay_allowed"] == 1 and p["transform_status"] == "verified" for p in payloads)
    assert dirs["100000"]["status"] == "resolved" and dirs["100000"]["rows_tied"] == 1
    assert dirs["C400"]["status"] == "resolved" and dirs["C400"]["spans_dropped"] == 1
    assert counts["payloads.words"] == B1_POS_POSITIONED + B2_POS_POSITIONED and counts["spans.dropped"] == 1
    # both schemas hold the same spans and every span reproduces the content
    p = g5.read_payload_p(os.path.join(out, g5.PAYLOAD_DB), (dk1, h1, "100000", "1"))
    r = g5.read_payload_r(os.path.join(out, g5.RELATIONAL_DB), (dk1, h1, "100000", "1"))
    assert [w[:3] for w in p["words"]] == [w[:3] for w in r["words"]] and len(p["rows"]) == len(r["rows"]) == 6
    assert all(content1[w[0]:w[1]].strip() for w in p["words"]) and p["transform"]["rot"] == 0
    q = g5.read_payload_p(os.path.join(out, g5.PAYLOAD_DB), (dk21, h21, "400", "1"))
    assert [content21[w[0]:w[1]] for w in q["words"]] == ["@[.........]", "יום", "הכפורים", "אסור", "באכילה", "אוסרי{ם\\/\\/ן}", "ב'", "ליענש"]
    assert q["frame"] == [1500.0, 2000.0] and q["transform"]["rot"] == 180
    assert checks["G5_size"]["passed"] and checks["G7_cold_read_latency"]["passed"] and checks["G9_schemas_agree"]["passed"]
    with open(os.path.join(out, "gate5_summary.json"), encoding="utf-8") as fh:
        summary = json.load(fh)
    assert summary["passed"] and summary["chosen_schema"] in ("P", "R") and len(summary["digest"]) == 64


def test_sample_is_seeded_and_stratified(world, tmp_path):
    out = str(tmp_path / "out")
    rc = g5.main(_args(world, out, "--allow-single-run"))
    sample, _, _, _, counts, _ = _load(out)
    assert rc == 0
    # the frame holds two images: 100000 (B1, small rotation) and 400 (B2, 180 degrees); every other stratum is short
    assert sorted(sample) == ["100000", "400"]
    assert sample["100000"]["stratum"] == "A" and sample["400"]["stratum"] == "D"
    assert counts["sample.images"] == 2 and counts["sample.shortfall"] == g5.SAMPLE_SIZE - 2
    assert counts["frame.eligible_images"] == 2 and counts["frame.B2.180"] == 1
    # the draw is a function of the seed alone
    F = {"1": {"number": "1", "format": "B1", "rclass": "small", "library": "CUL", "two_col": False},
         "2": {"number": "2", "format": "B1", "rclass": "small", "library": "JTS", "two_col": False},
         "3": {"number": "3", "format": "B1", "rclass": "small", "library": "CUL", "two_col": False},
         "4": {"number": "4", "format": "B2", "rclass": "90/270", "library": "CUL", "two_col": True}}
    a, short_a = g5.sample_images(F)
    b, _ = g5.sample_images(F)
    assert a == b and len(a) == 4 and short_a == {"A": 4, "B": 3, "C": 6, "D": 3}
    assert {n for n, s in a if s == "A"} == {"1", "2", "3"} and [n for n, s in a if s == "D"] == ["4"]


def test_fails_when_the_displayed_image_is_another_photograph(world, tmp_path):
    cache = str(tmp_path / "cache")
    shutil.copytree(world["cache"], cache)
    write_other_displayed_image(cache)
    out = str(tmp_path / "out")
    w = dict(world, cache_override=cache)
    rc = g5.main(_args(w, out, "--images", "100000,400", "--allow-single-run"))
    sample, _, _, payloads, _, checks = _load(out)
    assert rc == 1 and checks["G1_geometry"]["passed"] == 0
    assert sample["100000"]["passed"] == 0 and sample["100000"]["reason"] in ("too_few_landmarks", "max_residual", "rms_residual",
                                                                               "landmarks_not_distributed")
    assert sample["400"]["passed"] == 1
    # the unverified image shows no overlay; the verified one does
    by_image = {p["image"]: p for p in payloads}
    assert by_image["100000"]["overlay_allowed"] == 0 and by_image["100000"]["transform_status"] == "unchecked"
    assert by_image["400"]["overlay_allowed"] == 1
    assert checks["G2_no_overlay_without_tie"]["passed"] == 1


def test_fails_on_an_untied_payload_a_changed_digest_and_a_missed_expectation(world, tmp_path):
    out = str(tmp_path / "out")
    assert g5.main(_args(world, out, "--images", "100000,400", "--allow-single-run")) == 0
    p_path = os.path.join(out, g5.PAYLOAD_DB)
    dk1, _, h1 = _candidate(world, 1)
    hashes = {dk1: h1}
    # a payload whose text hash no candidate row carries, and an overlay allowed on an image the check did not pass
    con = sqlite3.connect(p_path)
    con.execute("INSERT INTO positions VALUES (?,?,?,?,?,?,?,?,?)", ("dbo_UnitTranscription:999|990009|", "0" * 64, "9", "1",
                                                                      1.0, 1.0, "unchecked", 0, b"x"))
    con.commit()
    con.close()
    g2 = g5.verify_stores(p_path, hashes, {"100000"}, set(), {})
    assert g2["untied"] >= 1
    g2b = g5.verify_stores(p_path, hashes, set(), set(), {})
    assert g2b["unallowed"] >= 1
    # a changed digest fails G4
    summary = os.path.join(out, "gate5_summary.json")
    with open(summary, encoding="utf-8") as fh:
        s = json.load(fh)
    s["digest"] = "0" * 64
    doctored = str(tmp_path / "doctored.json")
    with open(doctored, "w", encoding="utf-8") as fh:
        json.dump(s, fh)
    out2 = str(tmp_path / "out2")
    rc = g5.main(_args(world, out2, "--images", "100000,400", "--previous-summary", doctored))
    _, _, _, _, _, checks = _load(out2)
    assert rc == 1 and checks["G4_two_runs_identical"]["passed"] == 0
    # the same run with the true digest passes G4, and a missed expectation fails
    out3 = str(tmp_path / "out3")
    rc = g5.main(_args(world, out3, "--images", "100000,400", "--previous-summary", summary, "--expect", "payloads=99"))
    _, _, _, _, _, checks = _load(out3)
    assert rc == 1 and checks["G4_two_runs_identical"]["passed"] == 1 and checks["E_payloads"]["passed"] == 0
    assert checks["E_payloads"]["actual"] == "2"


def test_without_a_previous_summary_the_single_run_must_be_allowed(world, tmp_path):
    out = str(tmp_path / "out")
    rc = g5.main(_args(world, out, "--images", "100000,400"))
    _, _, _, _, _, checks = _load(out)
    assert rc == 1 and checks["G4_two_runs_identical"]["passed"] == 0
    assert all(c["passed"] for n, c in checks.items() if n != "G4_two_runs_identical")
