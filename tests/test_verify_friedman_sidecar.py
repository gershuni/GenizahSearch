# -*- coding: utf-8 -*-
"""scripts/verify_friedman_sidecar.py, the deploy gate, on a synthetic sidecar: it passes the file
the builder wrote and FAILS on a wrong count, a tampered token row, a version the reader does
not serve, and a missing file. The reader's pinned versions equal the builders'.

No dictionary text in the repository.
"""
from __future__ import annotations

import sqlite3

import pytest

from scripts import build_friedman_sidecar, friedman_index, friedman_units
from scripts import verify_friedman_sidecar as gate
from scripts.build_friedman_sidecar import write_sidecar
from shared import friedman_service as fs


def unit(uid, kind, head, keys, examples=(), parent=None):
    return {"id": uid, "kind": kind, "letter": "א", "parent": parent, "binyan": None,
            "head": {"text": head, "plain": head, "root": head, "root_source": "single_word", "root_candidate": None,
                     "root_dotted": [head], "lemma": head, "alternatives": [head], "frames": [], "notes": [],
                     "keys": list(keys)},
            "gloss": {"definition": f"def {uid}", "references": "", "foreign": "", "xrefs": [], "other": "", "locator": ""},
            "examples": [{"quote": q, "translation": "", "locator": "(ח1)", "foreign": "", "xrefs": [], "other": ""}
                         for q in examples],
            "segments": [], "text": f"text {uid}", "flags": [],
            "locations": [{"source_id": "1", "pdf_page": 1, "printed_page": "1", "paragraph": 0, "bbox": [0, 0, 1, 1], "chars": 5}]}


@pytest.fixture
def sidecar(tmp_path):
    units = {"recovery_version": friedman_units.RECOVERY_VERSION, "units": [
        unit("f000001", "entry", "אבד", ["אבד"], examples=["לא אבד מן", "ואבד מנה"]),
        unit("f000002", "subentry", "לא אבד (=לא בד)", ["לא אבד", "לא בד"], parent="f000001"),
    ]}
    index = {"index_version": friedman_index.INDEX_VERSION, "rows": [
        {"form": "אבאד", "form_key": "אבאד", "line": "אבאד", "pdf_page": 1, "printed_page": "969", "url": "u",
         "roots": [{"text": "אבד", "dotted": "אבד", "key": "אבד", "alternatives": 1}]}]}
    path = tmp_path / "friedman.db"
    meta = write_sidecar(units, index, {"units_sha256": "u", "index_sha256": "i", "manifest_sha256": "m"}, path)
    return path, int(meta["units"]), int(meta["index_rows"]), int(meta["tokens"])


def test_the_pinned_versions_are_the_builders():
    assert fs.EXPECTED_RECOVERY_VERSION == str(friedman_units.RECOVERY_VERSION)
    assert fs.EXPECTED_INDEX_VERSION == str(friedman_index.INDEX_VERSION)
    assert fs.EXPECTED_SIDECAR_VERSION == str(build_friedman_sidecar.SIDECAR_VERSION)


def test_the_gate_passes_the_builders_file_and_reports_it(sidecar, capsys):
    path, units, rows, tokens = sidecar
    summary = gate.verify(path, units, rows, tokens)
    assert summary["verified"] and summary["units"] == 2 and summary["quotations"] == 2 and summary["tokens"] == tokens
    assert summary["recovery_version"] == fs.EXPECTED_RECOVERY_VERSION and len(summary["sha256"]) == 64
    assert gate.main(["--db", str(path), "--expect-units", str(units), "--expect-index-rows", str(rows),
                      "--expect-tokens", str(tokens)]) == 0
    assert '"verified": true' in capsys.readouterr().out


def test_the_gate_fails_on_counts_tampering_versions_and_a_missing_file(sidecar, tmp_path, capsys):
    path, units, rows, tokens = sidecar
    with pytest.raises(ValueError, match="units: 2 rows, expected 3"):
        gate.verify(path, units + 1, rows, tokens)
    with pytest.raises(ValueError, match="tokens: .* expected"):
        gate.verify(path, units, rows, tokens - 1)
    conn = sqlite3.connect(path)
    conn.execute("DELETE FROM tokens WHERE rowid = (SELECT MIN(rowid) FROM tokens)")
    conn.execute("UPDATE meta SET value = ? WHERE key = 'tokens'", (str(tokens - 1),))
    conn.commit()
    conn.close()
    with pytest.raises(ValueError, match="tokens differ from the texts: 0 row.* 1 missing"):
        gate.verify(path, units, rows, tokens - 1), "the counts agree but a row is gone"
    conn = sqlite3.connect(path)
    conn.execute("UPDATE meta SET value = '1' WHERE key = 'recovery_version'")
    conn.commit()
    conn.close()
    with pytest.raises(ValueError, match="recovery_version '1'; this reader serves"):
        gate.verify(path, units, rows, tokens - 1)
    assert gate.main(["--db", str(tmp_path / "absent.db"), "--expect-units", "1", "--expect-index-rows", "1",
                      "--expect-tokens", "1"]) == 1
    assert "REFUSED" in capsys.readouterr().err, "a missing file fails, never skips"
