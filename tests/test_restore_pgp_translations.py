# -*- coding: utf-8 -*-
"""The withheld PGP translations must be restorable, exactly, and not by accident.

On 2026-09-21 the regenerated ``pgp_translations`` table was lifted out of ``pgp_data/pgp.db``
because 27.8% of its rows are materially wrong and the desktop ``.spec`` bundles that file into
every installer. The 35,111 rows were kept rather than deleted, as the baseline a remediation has
to beat -- which is only worth anything if putting them back is a verified operation rather than a
hopeful one.

So these tests pin the two properties that matter: a restore reproduces the source row for row,
and the script will not quietly overwrite a table that already holds something else.
"""
from __future__ import annotations

import importlib.util
import pathlib
import sqlite3

import pytest

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
COLUMNS = ("pgpid", "description_he", "document_type_he", "translated_at", "model_version")


@pytest.fixture(scope="module")
def script():
    """Import by path -- scripts/ is a flat namespace package with no __init__."""
    path = REPO_ROOT / "scripts" / "restore_pgp_translations.py"
    spec = importlib.util.spec_from_file_location("_restore_pgp_translations", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _make_source(path, rows, note=None):
    conn = sqlite3.connect(str(path))
    try:
        conn.execute(
            "CREATE TABLE pgp_translations ("
            " pgpid INTEGER PRIMARY KEY, description_he TEXT, document_type_he TEXT,"
            " translated_at TEXT, model_version TEXT DEFAULT 'dictalm2.0')"
        )
        conn.executemany(
            "INSERT INTO pgp_translations (%s) VALUES (?,?,?,?,?)" % ", ".join(COLUMNS), rows
        )
        if note is not None:
            conn.execute("CREATE TABLE withheld_note (note TEXT)")
            conn.execute("INSERT INTO withheld_note VALUES (?)", (note,))
        conn.commit()
    finally:
        conn.close()
    return str(path)


def _make_target(path, with_table_rows=None):
    """A stand-in pgp.db: the other tables, and pgp_translations only if asked."""
    conn = sqlite3.connect(str(path))
    try:
        conn.execute("CREATE TABLE documents (pgpid INTEGER PRIMARY KEY, description TEXT)")
        conn.execute("INSERT INTO documents VALUES (1, 'a description')")
        if with_table_rows is not None:
            conn.execute(
                "CREATE TABLE pgp_translations ("
                " pgpid INTEGER PRIMARY KEY, description_he TEXT, document_type_he TEXT,"
                " translated_at TEXT, model_version TEXT)"
            )
            conn.executemany(
                "INSERT INTO pgp_translations (%s) VALUES (?,?,?,?,?)" % ", ".join(COLUMNS),
                with_table_rows,
            )
        conn.commit()
    finally:
        conn.close()
    return str(path)


ROWS = [
    (1, "תרגום אחד", "מכתב",
     "2026-09-21T00:00:00Z", "dictalm2.0"),
    (2, "תרגום שני", None,
     "2026-09-21T00:00:01Z", "dictalm2.0"),
    (7, None, "מסמך", "2026-09-21T00:00:02Z", "dictalm2.0"),
]


def _rows_of(path):
    conn = sqlite3.connect("file:%s?mode=ro" % str(path).replace("\\", "/"), uri=True)
    try:
        return conn.execute(
            "SELECT %s FROM pgp_translations ORDER BY pgpid" % ", ".join(COLUMNS)
        ).fetchall()
    finally:
        conn.close()


def test_restore_reproduces_the_source_exactly(script, tmp_path):
    src = _make_source(tmp_path / "withheld.db", ROWS, note="withheld for quality")
    tgt = _make_target(tmp_path / "pgp.db")
    code = script.main(["--source", src, "--pgp-db", tgt])
    assert code == 0
    assert _rows_of(tgt) == ROWS, "NULLs and Hebrew must survive the round trip"


def test_dry_run_changes_nothing(script, tmp_path):
    src = _make_source(tmp_path / "withheld.db", ROWS)
    tgt = _make_target(tmp_path / "pgp.db")
    before = pathlib.Path(tgt).read_bytes()
    assert script.main(["--source", src, "--pgp-db", tgt, "--dry-run"]) == 0
    assert pathlib.Path(tgt).read_bytes() == before


def test_it_refuses_to_overwrite_different_content(script, tmp_path):
    """The whole point of keeping the corpus is that it can be compared with a better one.
    Silently clobbering whatever is already there would destroy the comparison."""
    src = _make_source(tmp_path / "withheld.db", ROWS)
    other = [(1, "a different translation", None, "2026-10-01T00:00:00Z", "something-better")]
    tgt = _make_target(tmp_path / "pgp.db", with_table_rows=other)
    assert script.main(["--source", src, "--pgp-db", tgt]) == 1
    assert _rows_of(tgt) == other, "the refusal must leave the target untouched"


def test_force_replaces_it(script, tmp_path):
    src = _make_source(tmp_path / "withheld.db", ROWS)
    other = [(1, "a different translation", None, "2026-10-01T00:00:00Z", "something-better")]
    tgt = _make_target(tmp_path / "pgp.db", with_table_rows=other)
    assert script.main(["--source", src, "--pgp-db", tgt, "--force"]) == 0
    assert _rows_of(tgt) == ROWS


def test_restoring_the_same_data_twice_is_a_no_op(script, tmp_path):
    src = _make_source(tmp_path / "withheld.db", ROWS)
    tgt = _make_target(tmp_path / "pgp.db")
    assert script.main(["--source", src, "--pgp-db", tgt]) == 0
    assert script.main(["--source", src, "--pgp-db", tgt]) == 0, (
        "an identical restore should report success without needing --force"
    )
    assert _rows_of(tgt) == ROWS


def test_a_missing_file_is_an_input_error_not_a_crash(script, tmp_path):
    tgt = _make_target(tmp_path / "pgp.db")
    assert script.main(["--source", str(tmp_path / "nope.db"), "--pgp-db", tgt]) == 2
    src = _make_source(tmp_path / "withheld.db", ROWS)
    assert script.main(["--source", src, "--pgp-db", str(tmp_path / "nope.db")]) == 2


def test_a_source_without_the_table_is_rejected(script, tmp_path):
    empty = tmp_path / "empty.db"
    sqlite3.connect(str(empty)).close()
    tgt = _make_target(tmp_path / "pgp.db")
    assert script.main(["--source", str(empty), "--pgp-db", tgt]) == 2
