# -*- coding: utf-8 -*-
"""The PGP sidecar export must not lose data it was never asked to lose.

Three separate silent losses were found on 2026-09-22, all in one script:

* ``documents.doc_relation`` was fetched from Supabase and then simply never named in the
  INSERT, because the column lists are hand-written while the query says ``SELECT *``.
  Nothing was red. Both web call sites read the missing field, hit their
  ``or not doc_relation`` fallback, and treated every document as an edition -- so 891
  documents that Princeton flags ``Digital Translation`` *and* that carry transcription
  text were presented as "PGP Transcription".
* ``pgp_translations`` is generated locally and has no upstream, yet the export deleted
  ``pgp.db`` and rebuilt four tables from Supabase. The 2026-04-22 rebuild destroyed
  34,954 rows and it took five months for anyone to notice.
* The live sidecar was deleted *before* the build, so a failed or interrupted export left
  the machine with no ``pgp.db`` at all.

These tests pin the fixes, and -- more usefully -- pin the *generic* guard, so the next
column Princeton adds cannot disappear the same way.
"""
from __future__ import annotations

import importlib.util
import pathlib
import re
import sqlite3

import pytest

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
SCRIPT_PATH = REPO_ROOT / "scripts" / "export_pgp_sidecar.py"


@pytest.fixture(scope="module")
def exporter():
    """Import by path -- scripts/ is a flat namespace package with no __init__."""
    spec = importlib.util.spec_from_file_location("_export_pgp_sidecar", SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


# ── the column that was lost ──────────────────────────────────────────────────


def test_doc_relation_is_carried(exporter):
    """The whole point of scripts/update_doc_relation.py is this column reaching the apps."""
    assert "doc_relation" in exporter.DOCUMENT_COLUMNS


def test_create_table_and_column_list_agree(exporter):
    """The INSERT is generated from DOCUMENT_COLUMNS; the CREATE TABLE is hand-written.

    If they drift, every value shifts one column left and the sidecar is quietly wrong
    rather than broken. The exporter asserts this at runtime; this asserts it in CI,
    against the real CREATE TABLE in the real script.
    """
    source = SCRIPT_PATH.read_text(encoding="utf-8")
    match = re.search(
        r'cursor\.execute\("""\s*(CREATE TABLE documents\s*\(.*?\))\s*"""\)',
        source,
        re.DOTALL,
    )
    assert match, "could not find the documents CREATE TABLE in the script"

    conn = sqlite3.connect(":memory:")
    try:
        conn.execute(match.group(1))
        columns = tuple(r[1] for r in conn.execute("PRAGMA table_info(documents)"))
    finally:
        conn.close()

    assert columns == exporter.DOCUMENT_COLUMNS


# ── the generic guard: no column may be dropped silently ──────────────────────


def _cursor_with(columns):
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE documents (%s)" % ", ".join("%s TEXT" % c for c in columns))
    return conn.cursor()


def test_a_supabase_column_the_sidecar_lacks_is_fatal(exporter):
    """This is the check that would have caught doc_relation on day one."""
    cursor = _cursor_with(["pgpid", "description"])
    rows = [{"pgpid": 1, "description": "x", "doc_relation": "Digital Translation"}]

    with pytest.raises(RuntimeError) as excinfo:
        exporter.assert_no_dropped_columns("documents", rows, cursor)
    assert "doc_relation" in str(excinfo.value)


def test_matching_columns_pass(exporter):
    cursor = _cursor_with(["pgpid", "description", "doc_relation"])
    rows = [{"pgpid": 1, "description": "x", "doc_relation": None}]
    exporter.assert_no_dropped_columns("documents", rows, cursor)  # must not raise


def test_a_deliberate_omission_can_be_declared(exporter, monkeypatch):
    """A column may be left out -- but only on purpose, with a reason, in one place."""
    cursor = _cursor_with(["pgpid"])
    rows = [{"pgpid": 1, "internal_note": "not ours to ship"}]

    with pytest.raises(RuntimeError):
        exporter.assert_no_dropped_columns("documents", rows, cursor)

    monkeypatch.setattr(
        exporter, "KNOWN_UNEXPORTED", {"documents.internal_note": "Princeton-internal"}
    )
    exporter.assert_no_dropped_columns("documents", rows, cursor)


def test_no_rows_is_not_an_error(exporter):
    """An empty table tells us nothing about columns; it must not be read as 'all fine'
    OR as a failure."""
    exporter.assert_no_dropped_columns("documents", [], _cursor_with(["pgpid"]))


# ── carrying the locally-generated tables across a rebuild ────────────────────

TRANSLATION_ROWS = [
    (1, "תרגום אחד", "מכתב",
     "2026-09-21T00:00:00Z", "dictalm2.0"),
    (2, "תרגום שני", None,
     "2026-09-21T00:00:01Z", "dictalm2.0"),
    (7, None, "מסמך", "2026-09-21T00:00:02Z", "dictalm2.0"),
]


def _make_sidecar(path, with_translations=True):
    conn = sqlite3.connect(str(path))
    try:
        conn.execute("CREATE TABLE documents (pgpid INTEGER PRIMARY KEY, description TEXT)")
        conn.execute("INSERT INTO documents VALUES (1, 'a description')")
        if with_translations:
            conn.execute(
                "CREATE TABLE pgp_translations ("
                " pgpid INTEGER PRIMARY KEY, description_he TEXT, document_type_he TEXT,"
                " translated_at TEXT, model_version TEXT DEFAULT 'dictalm2.0')"
            )
            conn.execute(
                "CREATE INDEX idx_translations_model ON pgp_translations(model_version)"
            )
            conn.executemany(
                "INSERT INTO pgp_translations VALUES (?,?,?,?,?)", TRANSLATION_ROWS
            )
        conn.commit()
    finally:
        conn.close()
    return str(path)


def test_translations_survive_a_rebuild(exporter, tmp_path):
    """The 2026-04-22 loss, in one test."""
    old = _make_sidecar(tmp_path / "pgp.db")
    carried = exporter.read_carryover_tables(old)
    assert [t["name"] for t in carried] == ["pgp_translations"]
    assert len(carried[0]["rows"]) == len(TRANSLATION_ROWS)

    rebuilt = sqlite3.connect(str(tmp_path / "pgp.db.new"))
    try:
        exporter.write_carryover_tables(rebuilt.cursor(), carried)
        got = rebuilt.execute(
            "SELECT pgpid, description_he, document_type_he, translated_at, model_version"
            " FROM pgp_translations ORDER BY pgpid"
        ).fetchall()
        indexes = {
            r[0] for r in rebuilt.execute(
                "SELECT name FROM sqlite_master WHERE type='index' AND sql IS NOT NULL"
            )
        }
    finally:
        rebuilt.close()

    assert got == TRANSLATION_ROWS, "NULLs and Hebrew must survive the carry-forward"
    assert "idx_translations_model" in indexes, "indexes must be carried too"


def test_a_sidecar_without_translations_carries_nothing(exporter, tmp_path):
    """The current, withheld state. It is normal, not an error."""
    old = _make_sidecar(tmp_path / "pgp.db", with_translations=False)
    assert exporter.read_carryover_tables(old) == []


def test_a_missing_sidecar_does_not_create_a_stub(exporter, tmp_path):
    """A bare sqlite3.connect() on a missing path creates a 0-byte file that later reads
    as 'no such table' -- a trap this repo has been bitten by before."""
    missing = tmp_path / "does-not-exist.db"
    assert exporter.read_carryover_tables(str(missing)) == []
    assert not missing.exists()


def test_a_short_carry_is_caught(exporter, tmp_path):
    """If the carry-forward silently wrote nothing it would look exactly like the data
    loss it exists to prevent, so validation counts the rows back."""
    old = _make_sidecar(tmp_path / "pgp.db")
    carried = exporter.read_carryover_tables(old)

    rebuilt = sqlite3.connect(str(tmp_path / "pgp.db.new"))
    try:
        rebuilt.execute(carried[0]["create_sql"])
        rebuilt.execute("INSERT INTO pgp_translations VALUES (1,'x',NULL,NULL,NULL)")
        rebuilt.commit()
        errors = exporter.validate_carryover(rebuilt, carried)
    finally:
        rebuilt.close()

    assert errors and "pgp_translations" in errors[0]


def test_a_missing_carried_table_is_caught(exporter, tmp_path):
    old = _make_sidecar(tmp_path / "pgp.db")
    carried = exporter.read_carryover_tables(old)

    rebuilt = sqlite3.connect(str(tmp_path / "pgp.db.new"))
    try:
        errors = exporter.validate_carryover(rebuilt, carried)
    finally:
        rebuilt.close()

    assert errors and "pgp_translations" in errors[0]


# ── provenance ────────────────────────────────────────────────────────────────


def test_provenance_is_optional_and_never_raises(exporter, tmp_path):
    """`meta.created` dates the BUILD; provenance dates the DATA. Fetching the CSVs by
    hand is allowed -- it just means the sidecar records no upstream commit."""
    assert exporter.read_provenance(tmp_path) == {}

    (tmp_path / exporter.PROVENANCE_FILENAME).write_text(
        '{"upstream_commit": "abc123", "upstream_repo": "princetongenizalab/pgp-metadata"}',
        encoding="utf-8",
    )
    assert exporter.read_provenance(tmp_path)["upstream_commit"] == "abc123"

    (tmp_path / exporter.PROVENANCE_FILENAME).write_text("{ not json", encoding="utf-8")
    assert exporter.read_provenance(tmp_path) == {}
