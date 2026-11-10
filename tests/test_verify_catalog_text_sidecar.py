# -*- coding: utf-8 -*-
"""scripts/verify_catalog_text_sidecar.py passes a good sidecar and FAILS on every kind of damage
(a gate that cannot fail proves nothing)."""
from __future__ import annotations

import shutil
import sqlite3
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

from catalog_text_fixture import build_sidecar  # noqa: E402
from scripts import verify_catalog_text_sidecar as gate  # noqa: E402

FIXTURE_CANARIES = (("כתובה", 3), ("marriage contract", 3), ("cheese", 2))


@pytest.fixture(scope="module")
def sidecar(tmp_path_factory):
    return build_sidecar(tmp_path_factory.mktemp("catalog_gate"))


def _rows(path: Path) -> int:
    con = sqlite3.connect(f"file:{path.resolve().as_posix()}?mode=ro", uri=True)
    try:
        return con.execute("SELECT COUNT(*) FROM records").fetchone()[0]
    finally:
        con.close()


def _damaged(sidecar: Path, tmp_path: Path, *sql: str) -> Path:
    copy = tmp_path / "damaged.db"
    shutil.copyfile(sidecar, copy)
    con = sqlite3.connect(copy)
    for statement in sql:
        con.execute(statement)
    con.commit()
    con.close()
    return copy


def test_a_good_sidecar_passes(sidecar):
    assert gate.verify(str(sidecar), _rows(sidecar), sample=0, canaries=FIXTURE_CANARIES) == []


def test_main_passes_and_fails_by_exit_code(sidecar, monkeypatch):
    monkeypatch.setattr(gate, "CANARIES", FIXTURE_CANARIES)
    assert gate.main(["--db", str(sidecar), "--expect-rows", str(_rows(sidecar))]) == 0
    assert gate.main(["--db", str(sidecar), "--expect-rows", str(_rows(sidecar) + 1)]) == 1


def test_a_missing_or_empty_file_fails(tmp_path):
    assert gate.verify(str(tmp_path / "absent.db"), 1)
    empty = tmp_path / "empty.db"
    empty.write_bytes(b"")
    assert gate.verify(str(empty), 1)


@pytest.mark.parametrize("sql, expected", [
    (("UPDATE meta SET value = '999' WHERE key = 'normalizer_version'",), "normalizer_version"),
    (("UPDATE meta SET value = '0.1.0' WHERE key = 'version'",), "meta.version"),
    (("UPDATE meta SET value = '1' WHERE key = 'rows_total'",), "meta.rows_total"),
    (("UPDATE meta SET value = '0' WHERE key = 'rows_pgp_original'",), "rows_pgp_original"),
    (("UPDATE records SET sys_id = '12345' WHERE id = 1",), "not a corpus sys_id"),
    (("UPDATE records SET field = 'Bogus' WHERE id = 1",), "unknown (source, field)"),
    (("UPDATE records SET lang = 'en' WHERE lang = 'he'",), "language label"),
    (("UPDATE records SET text = 'zzzz qqqq' WHERE id = 1",), "not found by their own words"),
    (("DELETE FROM catalog_fts_docsize WHERE id = 1",), "catalog_fts indexes"),
    (("DROP INDEX idx_records_sys_id",), "missing index"),
])
def test_every_kind_of_damage_fails(sidecar, tmp_path, sql, expected):
    damaged = _damaged(sidecar, tmp_path, *sql)
    problems = gate.verify(str(damaged), _rows(sidecar), sample=0, canaries=FIXTURE_CANARIES)
    assert any(expected in p for p in problems), problems


def test_a_row_count_other_than_expected_fails(sidecar):
    problems = gate.verify(str(sidecar), _rows(sidecar) - 1, sample=0, canaries=())
    assert any("!= expected" in p for p in problems)


def test_a_canary_below_its_floor_fails(sidecar):
    problems = gate.verify(str(sidecar), _rows(sidecar), sample=0, canaries=(("כתובה", 99),))
    assert problems == ["canary 'כתובה' finds 3 manuscripts, floor 99"]
