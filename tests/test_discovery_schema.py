# -*- coding: utf-8 -*-
"""Column-allowlist tests for scripts/verify_discovery_sidecar.py (Phase 134,
plan 134-03, Task 2).

Masking discipline: every mutation below adds an obviously-fabricated,
test-only column/value (e.g. a bare "cat" column, a "M:"-prefixed string) --
never a real restricted corpus name or reference text.
"""
import shutil
import sqlite3
from pathlib import Path

from scripts import verify_discovery_sidecar as verify_mod

FIXTURE_DB = (
    Path(__file__).resolve().parent / "fixtures" / "discovery" / "discovery-v1-fixture.db"
)


def _copy_fixture(tmp_path, name="corrupt.db"):
    dest = tmp_path / name
    shutil.copyfile(FIXTURE_DB, dest)
    return dest


def _connect_rw(db_path):
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA foreign_keys = OFF")
    return conn


# ---------------------------------------------------------------------------
# test_no_reference_columns
# ---------------------------------------------------------------------------

def test_no_reference_columns_clean_fixture_passes():
    conn = sqlite3.connect(f"file:{FIXTURE_DB}?mode=ro", uri=True)
    try:
        violations = verify_mod.check_column_allowlist(conn)
    finally:
        conn.close()
    assert violations == []


def test_no_reference_columns_fails_on_forbidden_bare_column(tmp_path):
    db_path = _copy_fixture(tmp_path)
    conn = _connect_rw(db_path)
    conn.execute("ALTER TABLE discovery_evidence ADD COLUMN cat TEXT")
    conn.commit()
    conn.close()

    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        violations = verify_mod.check_column_allowlist(conn)
    finally:
        conn.close()
    assert any("cat" in v for v in violations)


def test_no_reference_columns_fails_on_title_outside_works(tmp_path):
    db_path = _copy_fixture(tmp_path)
    conn = _connect_rw(db_path)
    conn.execute("ALTER TABLE discovery_claim ADD COLUMN title TEXT")
    conn.commit()
    conn.close()

    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        violations = verify_mod.check_column_allowlist(conn)
    finally:
        conn.close()
    assert any("title" in v for v in violations)


def test_no_reference_columns_allows_title_author_genre_on_works():
    # works.author / works.genre are the REVIEWED columns (D-07) -- must never
    # trip the allowlist (asserted directly against the clean, unmodified fixture).
    conn = sqlite3.connect(f"file:{FIXTURE_DB}?mode=ro", uri=True)
    try:
        violations = verify_mod.check_column_allowlist(conn)
    finally:
        conn.close()
    assert not any("works.author" in v or "works.genre" in v for v in violations)


def test_no_reference_columns_fails_on_raw_work_id_shaped_value(tmp_path):
    db_path = _copy_fixture(tmp_path)
    conn = _connect_rw(db_path)
    # Fabricated, obviously-fake raw-shaped work_id token -- NEVER a real
    # restricted corpus identifier (masking-scan convention).
    conn.execute(
        "UPDATE works SET work_id = 'M:ZZZ_FAKE_RAW_WORK_ID_ZZZ' WHERE work_id = 'w000008'"
    )
    conn.commit()
    conn.close()

    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        violations = verify_mod.check_column_allowlist(conn)
    finally:
        conn.close()
    assert any("raw-shaped work_id" in v for v in violations)
