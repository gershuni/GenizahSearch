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

import pytest

from scripts import build_discovery_sidecar as sidecar_build
from scripts import discovery_ids as ids
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


# ---------------------------------------------------------------------------
# D-02a (136-06, docs/specs/discovery-sidecar-schema-v1.md SS1.6 amendment
# 2026-08-02): the tier_a CERT-01 authorization at the RAW SQL/DDL layer --
# a check independent of the Python-level _validate_precision_spec /
# check_measurement_status_ci_consistency tests in tests/test_discovery_build.py.
# ---------------------------------------------------------------------------

def _fresh_schema_db(tmp_path, name="fresh-schema.db"):
    db_path = tmp_path / name
    conn = sqlite3.connect(str(db_path))
    sidecar_build.create_schema(conn)
    return conn


def test_band_precision_check_constraint_accepts_tier_a_authorization(tmp_path):
    """The band_precision.measurement_status CHECK constraint (create_schema)
    must accept the frozen D-02a tier_a authorization value 'measured_pass'
    with precision NULL -- proven by inserting the EXACT frozen row and
    asserting no sqlite3.IntegrityError."""
    conn = _fresh_schema_db(tmp_path)
    tier_a = next(
        r for r in sidecar_build._frozen_real_band_precision_rows()
        if r["scope"] == "band" and r["evidence_source"] == ids.EVIDENCE_SOURCE_TRACK1_DIRECT
        and r["confidence_band"] == ids.CONFIDENCE_BAND_TIER_A
    )
    conn.execute(
        "INSERT INTO band_precision (scope, collection_id, evidence_source, confidence_band, "
        "numerator, denominator, precision, ci_low, ci_high, method, sampling_frame, ins_policy, "
        "weighting, notes, measurement_status) VALUES "
        "(:scope, :collection_id, :evidence_source, :confidence_band, :numerator, :denominator, "
        ":precision, :ci_low, :ci_high, :method, :sampling_frame, :ins_policy, :weighting, :notes, "
        ":measurement_status)",
        tier_a,
    )
    conn.commit()
    (stored,) = conn.execute(
        "SELECT measurement_status FROM band_precision WHERE confidence_band=? AND "
        "evidence_source=?", (ids.CONFIDENCE_BAND_TIER_A, ids.EVIDENCE_SOURCE_TRACK1_DIRECT)
    ).fetchone()
    assert stored == "measured_pass"
    conn.close()


def test_band_precision_check_constraint_rejects_out_of_vocab_measurement_status(tmp_path):
    """The SAME CHECK constraint must reject a measurement_status outside the
    closed vocabulary -- proven at the raw SQL layer, independent of the
    Python-level closed-vocabulary cross-check in _validate_precision_spec."""
    conn = _fresh_schema_db(tmp_path)
    try:
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                "INSERT INTO band_precision (scope, collection_id, evidence_source, "
                "confidence_band, measurement_status) VALUES "
                "('band', 'e1_certification_registry_v1', ?, ?, 'bogus_status_outside_vocab')",
                (ids.EVIDENCE_SOURCE_TRACK1_DIRECT, ids.CONFIDENCE_BAND_TIER_A),
            )
    finally:
        conn.close()
