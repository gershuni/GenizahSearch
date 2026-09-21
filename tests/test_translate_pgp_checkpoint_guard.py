# -*- coding: utf-8 -*-
"""``stale_checkpoint_ids`` must catch a checkpoint the database cannot confirm.

The checkpoint file records only pgpids, never translated text, and the batch loop skips every
id it names. ``scripts/export_pgp_sidecar.py`` deletes pgp.db and recreates it *without*
``pgp_translations``, so after a rebuild every id in an old checkpoint points at a row that no
longer exists. Resuming then skips those documents forever and prints a successful summary --
which is exactly how the table stayed missing from 2026-04-22 to 2026-09-21.

This guard turns that into a stop condition, so it is worth a test that can fail: the first run
against the real 2026-09 checkpoint found 221 such ids.
"""
from __future__ import annotations

import importlib.util
import pathlib
import sqlite3

import pytest

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent


def _load_script():
    """Import the script by path -- ``scripts/`` is a flat namespace package with no __init__."""
    path = REPO_ROOT / "scripts" / "translate_pgp_descriptions.py"
    spec = importlib.util.spec_from_file_location("_translate_pgp_descriptions", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def script():
    return _load_script()


def _make_db(path: pathlib.Path, *, with_table: bool, pgpids=()) -> str:
    conn = sqlite3.connect(str(path))
    try:
        if with_table:
            conn.execute(
                "CREATE TABLE pgp_translations ("
                " pgpid INTEGER PRIMARY KEY, description_he TEXT, document_type_he TEXT)"
            )
            conn.executemany(
                "INSERT INTO pgp_translations (pgpid, description_he) VALUES (?, ?)",
                [(p, "x") for p in pgpids],
            )
        else:
            conn.execute("CREATE TABLE documents (pgpid INTEGER PRIMARY KEY)")
        conn.commit()
    finally:
        conn.close()
    return str(path)


def test_empty_checkpoint_is_never_stale(script, tmp_path):
    db = _make_db(tmp_path / "pgp.db", with_table=True, pgpids=[1, 2])
    assert script.stale_checkpoint_ids(db, set()) == set()


def test_every_id_is_stale_when_the_table_is_gone(script, tmp_path):
    """The rebuild case: pgp.db exists, pgp_translations does not."""
    db = _make_db(tmp_path / "pgp.db", with_table=False)
    assert script.stale_checkpoint_ids(db, {1, 2, 3}) == {1, 2, 3}


def test_only_the_missing_ids_are_reported(script, tmp_path):
    db = _make_db(tmp_path / "pgp.db", with_table=True, pgpids=[1, 3])
    assert script.stale_checkpoint_ids(db, {1, 2, 3, 4}) == {2, 4}


def test_a_fully_confirmed_checkpoint_is_clean(script, tmp_path):
    db = _make_db(tmp_path / "pgp.db", with_table=True, pgpids=[1, 2, 3])
    assert script.stale_checkpoint_ids(db, {1, 2, 3}) == set()


def test_more_ids_than_one_sql_batch(script, tmp_path):
    """The lookup chunks at 400 ids to stay under SQLITE_MAX_VARIABLE_NUMBER."""
    present = list(range(1, 1001))
    db = _make_db(tmp_path / "pgp.db", with_table=True, pgpids=present)
    checkpoint = set(range(1, 1201))
    assert script.stale_checkpoint_ids(db, checkpoint) == set(range(1001, 1201))


def test_the_probe_does_not_write_to_the_database(script, tmp_path):
    """Opened read-only on purpose: a bare sqlite3.connect() against a missing sidecar
    creates a 0-byte stub that later reads as 'no such table' (see the nli_crossref trap)."""
    path = tmp_path / "pgp.db"
    db = _make_db(path, with_table=True, pgpids=[1])
    before = path.stat().st_mtime_ns, path.stat().st_size
    script.stale_checkpoint_ids(db, {1, 2})
    assert (path.stat().st_mtime_ns, path.stat().st_size) == before


def test_a_missing_database_is_not_silently_treated_as_clean(script, tmp_path):
    """mode=ro refuses to create the file, so a wrong --pgp-db raises instead of
    reporting an empty stale set (which would read as 'checkpoint confirmed')."""
    with pytest.raises(sqlite3.OperationalError):
        script.stale_checkpoint_ids(str(tmp_path / "does_not_exist.db"), {1})
