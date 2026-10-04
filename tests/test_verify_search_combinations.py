# -*- coding: utf-8 -*-
"""scripts/verify_search_combinations.py fails, never skips, when its data is absent.

The check itself needs the real index (minutes; run nightly by
scripts/schedule_nightly_search_gate.ps1). What CI can pin is the contract that
makes a nightly run trustworthy: a machine without the index gets exit 2 and a
logged "missing-data" verdict naming each absent file, not a green run.
"""
import importlib.util
import json
import os

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_spec = importlib.util.spec_from_file_location(
    "verify_search_combinations", os.path.join(ROOT, "scripts", "verify_search_combinations.py"))
gate = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(gate)


def test_missing_data_is_a_failure_and_is_logged(tmp_path):
    log = tmp_path / "log" / "gate.jsonl"
    code = gate.main(["--index-dir", str(tmp_path / "nowhere"),
                      "--libraries-csv", str(tmp_path / "libraries.csv"), "--json-out", str(log)])
    assert code == gate.EXIT_MISSING_DATA == 2
    (record,) = [json.loads(line) for line in log.read_text(encoding="utf-8").splitlines()]
    assert record["verdict"] == "missing-data" and record["exit_code"] == 2
    assert [m.split(":")[0] for m in record["missing"]] == ["search index", "browse map", "libraries.csv"]


def test_each_missing_file_is_named(tmp_path):
    (tmp_path / "tantivy_db").mkdir()
    (tmp_path / "tantivy_db" / "meta.json").write_text("{}", encoding="utf-8")
    csv = tmp_path / "libraries.csv"
    csv.write_text("", encoding="utf-8")
    assert [m.split(":")[0] for m in gate.missing_data(str(tmp_path), str(csv))] == ["browse map"]
    (tmp_path / "browse_map.pkl").write_bytes(b"")
    assert gate.missing_data(str(tmp_path), str(csv)) == []
