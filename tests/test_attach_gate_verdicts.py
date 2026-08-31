# -*- coding: utf-8 -*-
"""gate_verdict_fact attach: dedup, transport-failure drop, masking fail-close."""
import json
import os
import sqlite3
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import scripts.attach_gate_verdicts as agv  # noqa: E402


def _write_run(path, records):
    with open(path, "w", encoding="utf-8") as fh:
        for r in records:
            fh.write(json.dumps(r, ensure_ascii=False) + "\n")


def _mini_db(tmp_path):
    db = tmp_path / "mini.db"
    con = sqlite3.connect(db)
    con.execute("CREATE TABLE meta(key TEXT PRIMARY KEY, value TEXT)")
    con.execute("CREATE TABLE facet_row(evidence_id TEXT)")
    con.commit()
    con.close()
    return str(db)


def _patterns_env(tmp_path, monkeypatch, patterns=("SECRETCORPUSNAME",)):
    p = tmp_path / "patterns.txt"
    p.write_text("\n".join(patterns), encoding="utf-8")
    monkeypatch.setenv("MASKING_SCAN_PATTERNS_FILE", str(p))


def test_attach_end_to_end(tmp_path, monkeypatch):
    _patterns_env(tmp_path, monkeypatch)
    db = _mini_db(tmp_path)
    div = tmp_path / "div.jsonl"
    nf = tmp_path / "nf.jsonl"
    _write_run(div, [
        {"sys_id": "s1", "work_id": "w1", "page_id": "p1", "model": "m",
         "prompt_sha": "x", "verdict": "catalogue_right_match_is_quotation",
         "reason": "quotes the Talmud", "doubt": None},
        # a transport failure is dropped, never stored
        {"sys_id": "s2", "work_id": "w1", "page_id": "p2", "model": "m",
         "prompt_sha": "x", "verdict": "transport_failed"},
        # a resumed run appends: the LAST verdict for a pair wins
        {"sys_id": "s1", "work_id": "w1", "page_id": "p1", "model": "m",
         "prompt_sha": "x", "verdict": "both_right_multiple_works",
         "reason": "Targum interleaved", "doubt": None},
    ])
    _write_run(nf, [
        {"sys_id": "s3", "work_id": "w2", "page_id": "p3", "model": "m",
         "prompt_sha": "y", "verdict": "credible_new_identification",
         "reason": "continuous text", "doubt": "check folio 2"},
    ])
    counts = agv.attach(db, runs=(("divergence", str(div)), ("new_finds", str(nf))),
                        say=lambda *a: None)
    assert counts == {"divergence": {"both_right_multiple_works": 1},
                      "new_finds": {"credible_new_identification": 1}}
    con = sqlite3.connect(db)
    got = con.execute("SELECT sys_id, task, verdict FROM gate_verdict_fact "
                      "ORDER BY sys_id").fetchall()
    assert got == [("s1", "divergence", "both_right_multiple_works"),
                   ("s3", "new_finds", "credible_new_identification")]
    # facet_row dropped so the viewer rebuilds with the two new columns
    assert con.execute("SELECT COUNT(*) FROM sqlite_master "
                       "WHERE name='facet_row'").fetchone()[0] == 0
    meta = dict(con.execute("SELECT key, value FROM meta"))
    assert "doc.gate_divergence" in meta and "doc.gate_new_finds" in meta
    con.close()


def test_masked_name_in_reason_fails_closed(tmp_path, monkeypatch):
    _patterns_env(tmp_path, monkeypatch)
    db = _mini_db(tmp_path)
    div = tmp_path / "div.jsonl"
    _write_run(div, [
        {"sys_id": "s1", "work_id": "w1", "page_id": "p1", "model": "m",
         "prompt_sha": "x", "verdict": "catalogue_too_general",
         "reason": "matches SECRETCORPUSNAME edition", "doubt": None},
    ])
    with pytest.raises(SystemExit, match="MASKING"):
        agv.attach(db, runs=(("divergence", str(div)),), say=lambda *a: None)
    con = sqlite3.connect(db)
    # nothing written, facet_row untouched
    assert con.execute("SELECT COUNT(*) FROM sqlite_master "
                       "WHERE name='gate_verdict_fact'").fetchone()[0] == 0
    assert con.execute("SELECT COUNT(*) FROM sqlite_master "
                       "WHERE name='facet_row'").fetchone()[0] == 1
    con.close()


def test_out_of_vocabulary_verdict_is_rejected(tmp_path, monkeypatch):
    _patterns_env(tmp_path, monkeypatch)
    db = _mini_db(tmp_path)
    div = tmp_path / "div.jsonl"
    _write_run(div, [
        {"sys_id": "s1", "work_id": "w1", "page_id": "p1", "model": "m",
         "prompt_sha": "x", "verdict": "made_up_value", "reason": "", "doubt": None},
    ])
    with pytest.raises(sqlite3.IntegrityError):
        agv.attach(db, runs=(("divergence", str(div)),), say=lambda *a: None)
