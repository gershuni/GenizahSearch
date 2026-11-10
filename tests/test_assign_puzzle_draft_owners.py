# -*- coding: utf-8 -*-
"""scripts/assign_puzzle_draft_owners.py: give saved joins without an owner
an owner, on a copy of joins.db, printing counts only."""
from __future__ import annotations

import importlib.util
import json
import pathlib
import sqlite3

import pytest

REPO = pathlib.Path(__file__).resolve().parents[1]
SCRIPT = REPO / 'scripts' / 'assign_puzzle_draft_owners.py'


def _load():
    spec = importlib.util.spec_from_file_location('assign_puzzle_draft_owners', SCRIPT)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _make_db(path, with_owner_column=True):
    conn = sqlite3.connect(str(path))
    cols = ', owner_key TEXT' if with_owner_column else ''
    conn.execute(
        "CREATE TABLE join_documents (id TEXT PRIMARY KEY, title TEXT, notes TEXT, "
        f"created_at TEXT, updated_at TEXT{cols})")
    rows = [('doc-1', 'Title one', 'Notes one'), ('doc-2', 'Title two', 'Notes two'),
            ('doc-3', 'Title three', 'Notes three')]
    for r in rows:
        conn.execute("INSERT INTO join_documents (id, title, notes, created_at, updated_at) "
                     "VALUES (?, ?, ?, '2026-03-17', '2026-03-17')", r)
    if with_owner_column:
        conn.execute("UPDATE join_documents SET owner_key = 'u:already' WHERE id = 'doc-3'")
    conn.commit()
    conn.close()


def _owners(path):
    conn = sqlite3.connect(str(path))
    try:
        return dict(conn.execute('SELECT id, owner_key FROM join_documents'))
    finally:
        conn.close()


def test_dry_run_writes_nothing_and_prints_counts_only(tmp_path, capsys):
    db = tmp_path / 'joins-copy.db'
    _make_db(db)
    mapping = tmp_path / 'owners.json'
    mapping.write_text(json.dumps({'doc-1': 'u:user-1', 'doc-2': 'b:' + 'a' * 32}), encoding='utf-8')
    before = db.read_bytes()

    assert _load().main([str(db), '--mapping', str(mapping)]) == 0

    assert db.read_bytes() == before
    out = capsys.readouterr().out
    assert 'to_assign: 2' in out
    for secret in ('Title', 'Notes', 'doc-1', 'user-1', 'a' * 32):
        assert secret not in out


def test_apply_assigns_only_rows_without_owner(tmp_path, capsys):
    db = tmp_path / 'joins-copy.db'
    _make_db(db)
    mapping = tmp_path / 'owners.csv'
    mapping.write_text('doc_id,owner_key\ndoc-1,u:user-1\ndoc-3,u:already\nmissing,u:user-9\n',
                       encoding='utf-8')

    assert _load().main([str(db), '--mapping', str(mapping), '--apply']) == 0

    assert _owners(db) == {'doc-1': 'u:user-1', 'doc-2': None, 'doc-3': 'u:already'}
    out = capsys.readouterr().out
    assert 'Written: 1' in out and 'not_found: 1' in out and 'already_this_owner: 1' in out


def test_apply_never_replaces_an_existing_owner(tmp_path):
    db = tmp_path / 'joins-copy.db'
    _make_db(db)
    mapping = tmp_path / 'owners.json'
    mapping.write_text(json.dumps({'doc-1': 'u:user-1', 'doc-3': 'u:someone-else'}), encoding='utf-8')

    assert _load().main([str(db), '--mapping', str(mapping), '--apply']) == 1
    # All or nothing: doc-1 was not written either.
    assert _owners(db) == {'doc-1': None, 'doc-2': None, 'doc-3': 'u:already'}


@pytest.mark.parametrize('bad', ['', 'user-1', 'u:', 'x:abc', 'u: spaced', None, 7])
def test_apply_refuses_a_malformed_owner(tmp_path, bad):
    db = tmp_path / 'joins-copy.db'
    _make_db(db)
    mapping = tmp_path / 'owners.json'
    mapping.write_text(json.dumps([{'doc_id': 'doc-1', 'owner_key': 'u:ok'},
                                   {'doc_id': 'doc-2', 'owner_key': bad}]), encoding='utf-8')
    assert _load().main([str(db), '--mapping', str(mapping), '--apply']) == 1
    assert _owners(db)['doc-1'] is None


def test_file_from_before_owners_gets_the_column_on_apply(tmp_path):
    db = tmp_path / 'joins-copy.db'
    _make_db(db, with_owner_column=False)
    mapping = tmp_path / 'owners.json'
    mapping.write_text(json.dumps({'doc-2': 'u:user-2'}), encoding='utf-8')
    before = db.read_bytes()

    mod = _load()
    assert mod.main([str(db), '--mapping', str(mapping)]) == 0
    assert db.read_bytes() == before            # the dry run added nothing
    assert mod.main([str(db), '--mapping', str(mapping), '--apply']) == 0
    assert _owners(db) == {'doc-1': None, 'doc-2': 'u:user-2', 'doc-3': None}


def test_missing_database_is_never_created(tmp_path):
    db = tmp_path / 'nope.db'
    mapping = tmp_path / 'owners.json'
    mapping.write_text('{}', encoding='utf-8')
    assert _load().main([str(db), '--mapping', str(mapping), '--apply']) == 2
    assert not db.exists()


def test_apply_never_creates_a_database_moved_after_the_check(tmp_path, monkeypatch):
    """The file exists when it is checked but is gone (moved, renamed) by the
    time it is opened: nothing is created in its place."""
    mod = _load()
    db = tmp_path / 'moved.db'
    mapping = tmp_path / 'owners.json'
    mapping.write_text(json.dumps({'doc-1': 'u:user-1'}), encoding='utf-8')
    real_isfile = mod.os.path.isfile
    monkeypatch.setattr(mod.os.path, 'isfile',
                        lambda p: True if pathlib.Path(p) == db else real_isfile(p))

    assert mod.main([str(db), '--mapping', str(mapping), '--apply']) == 2
    assert not db.exists()
