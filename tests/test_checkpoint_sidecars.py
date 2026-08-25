# -*- coding: utf-8 -*-
"""Phase 146 Task 2d: checkpoint_sidecars.py must exit non-zero when a
sidecar with a NON-EMPTY WAL fails to checkpoint.

The old script caught every sqlite3.Error, printed SKIPPED and always
exited 0 -- a release build could package stale .db files while looking
green. PRAGMA wal_checkpoint(TRUNCATE) can also report `busy` in its own
result row WITHOUT raising, so failure has to be judged from that row (or
the post-checkpoint WAL size), not from exceptions alone.
"""
from __future__ import annotations

import os
import sqlite3

import scripts.checkpoint_sidecars as ckpt


def _make_wal_db(path, rows=1):
    """Leaves the WAL non-empty ON DISK: closing the writer -- even with no
    other connection open -- triggers sqlite's own auto-checkpoint-on-close
    and empties the WAL before the test ever gets to call checkpoint_one, so
    the writer connection is returned OPEN and the caller must close it."""
    conn = sqlite3.connect(str(path))
    conn.execute('PRAGMA journal_mode=WAL')
    conn.execute('CREATE TABLE t (x)')
    for i in range(rows):
        conn.execute('INSERT INTO t VALUES (?)', (i,))
    conn.commit()
    return conn


def _wal_path(db_path):
    return str(db_path) + '-wal'


def test_no_wal_file_is_ok(tmp_path):
    p = tmp_path / 'plain.db'
    conn = sqlite3.connect(str(p))
    conn.execute('CREATE TABLE t (x)')
    conn.commit()
    conn.close()
    assert not os.path.exists(_wal_path(p))
    assert ckpt.checkpoint_one(str(p)) is True


def test_a_clean_wal_checkpoints_and_reports_ok(tmp_path):
    p = tmp_path / 'clean.db'
    writer = _make_wal_db(p)
    try:
        assert os.path.getsize(_wal_path(p)) > 0, 'fixture must start with a WAL'
        assert ckpt.checkpoint_one(str(p)) is True
        assert os.path.getsize(_wal_path(p)) == 0
    finally:
        writer.close()


def test_absent_wal_is_safe_even_if_the_file_is_not_a_real_database(tmp_path):
    """A file with no WAL has nothing at risk, so a connect/pragma error on
    it must not be fatal -- exactly the 'absent WAL may still skip safely'
    case, proven with a file sqlite3 cannot even open."""
    p = tmp_path / 'bogus.db'
    p.write_bytes(b'not a real sqlite database')
    assert not os.path.exists(_wal_path(p))
    assert ckpt.checkpoint_one(str(p)) is True


def test_busy_non_empty_wal_reports_failure_without_raising(tmp_path):
    """The defect this pins: PRAGMA wal_checkpoint(TRUNCATE) can return a
    `busy` result row instead of raising. A reader holding an open snapshot
    blocks TRUNCATE from draining a WAL a concurrent writer has since
    extended -- the exact shape of 'a running GenizahSearch app has this DB
    open' at build time."""
    p = tmp_path / 'busy.db'
    setup_writer = _make_wal_db(p)
    setup_writer.close()  # commit is durable; -wal is refilled by `writer` below

    reader = sqlite3.connect(str(p), isolation_level=None)
    reader.execute('BEGIN')
    reader.execute('SELECT * FROM t').fetchall()
    writer = sqlite3.connect(str(p), isolation_level=None)
    writer.execute('INSERT INTO t VALUES (99)')
    try:
        wal_before = os.path.getsize(_wal_path(p))
        assert wal_before > 0, 'fixture must start with a non-empty WAL'

        assert ckpt.checkpoint_one(str(p)) is False

        assert os.path.getsize(_wal_path(p)) > 0, (
            'the WAL must still be non-empty -- this is what makes the '
            'failure real, not cosmetic')
    finally:
        reader.close()
        writer.close()


def test_main_exits_zero_when_every_sidecar_is_clean(tmp_path):
    a = tmp_path / 'a.db'
    b = tmp_path / 'b.db'
    writer_a = _make_wal_db(a)
    conn = sqlite3.connect(str(b))
    conn.execute('CREATE TABLE t (x)')
    conn.commit()
    conn.close()
    try:
        assert ckpt.main([str(a), str(b)]) == 0
    finally:
        writer_a.close()


def test_main_skips_a_missing_sidecar_without_failing(tmp_path):
    missing = tmp_path / 'does_not_exist.db'
    assert not os.path.exists(missing)
    assert ckpt.main([str(missing)]) == 0


def test_main_exits_nonzero_when_one_sidecar_has_a_busy_non_empty_wal(tmp_path):
    ok_db = tmp_path / 'ok.db'
    busy_db = tmp_path / 'busy.db'
    writer_ok = _make_wal_db(ok_db)
    setup_writer = _make_wal_db(busy_db)
    setup_writer.close()

    reader = sqlite3.connect(str(busy_db), isolation_level=None)
    reader.execute('BEGIN')
    reader.execute('SELECT * FROM t').fetchall()
    writer = sqlite3.connect(str(busy_db), isolation_level=None)
    writer.execute('INSERT INTO t VALUES (99)')
    try:
        assert ckpt.main([str(ok_db), str(busy_db)]) != 0
    finally:
        writer_ok.close()
        reader.close()
        writer.close()
