# -*- coding: utf-8 -*-
"""Release-contract corruption tests for scripts/verify_discovery_sidecar.py
(Phase 134, plan 134-03, Task 2).

Drives `verify()` over the committed fixture (clean -> PASS) and over a
per-test corrupted COPY (one invariant broken at a time -> FAIL). Two
corruption types (missing a_page_id / R5, duplicate evidence key) require
recreating the target table WITHOUT its NOT NULL/PK constraints first
(`_recreate_table_without_constraints`) -- the frozen DDL's own constraints
would otherwise reject the mutation outright, which is a GOOD thing (defense
in depth) but means the verifier's OWN redundant Python-level check has to be
exercised via a table recreated without those constraints, simulating a
non-conforming producer.

Masking discipline: every fixture value is synthetic (see
scripts/build_discovery_sidecar.py); nothing here touches real research data.
"""
import json
import shutil
import sqlite3
from pathlib import Path

from scripts import verify_discovery_sidecar as verify_mod

FIXTURE_DIR = Path(__file__).resolve().parent / "fixtures" / "discovery"
FIXTURE_DB = FIXTURE_DIR / "discovery-v1-fixture.db"
FIXTURE_EXPECTED = FIXTURE_DIR / "discovery-v1-fixture-expected.json"


def _load_expected():
    return json.loads(FIXTURE_EXPECTED.read_text(encoding="utf-8"))


def _copy_fixture(tmp_path, name="corrupt.db"):
    dest = tmp_path / name
    shutil.copyfile(FIXTURE_DB, dest)
    return dest


def _connect_rw(db_path):
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA foreign_keys = OFF")
    return conn


def _recreate_table_without_constraints(conn, table):
    """Drop NOT NULL/PK/CHECK/UNIQUE constraints on `table` via CREATE TABLE
    ... AS SELECT (copies data, not constraints). Lets a corruption test
    reach a DB state the frozen DDL itself would otherwise reject outright,
    so the verifier's OWN defensive check gets genuinely exercised."""
    cur = conn.cursor()
    cur.execute(f'ALTER TABLE "{table}" RENAME TO "{table}__bak"')
    cur.execute(f'CREATE TABLE "{table}" AS SELECT * FROM "{table}__bak"')
    cur.execute(f'DROP TABLE "{table}__bak"')


EXPECTED = _load_expected()
EXPECTED_FRAME_HASH = EXPECTED["frame_content_hash"]


# ---------------------------------------------------------------------------
# Clean fixture -- baseline PASS
# ---------------------------------------------------------------------------

def test_verify_clean_fixture_passes():
    assert verify_mod.verify(str(FIXTURE_DB), EXPECTED_FRAME_HASH) == 0


def test_verify_clean_fixture_passes_without_expected_hash_arg():
    # --expected-frame-hash is optional for ad-hoc local verification.
    assert verify_mod.verify(str(FIXTURE_DB), None) == 0


# ---------------------------------------------------------------------------
# Positive cases -- these must PASS, not fail
# ---------------------------------------------------------------------------

def test_positive_nullable_b_side_offsets_on_shared_text_passes():
    conn = sqlite3.connect(f"file:{FIXTURE_DB}?mode=ro", uri=True)
    try:
        row = conn.execute(
            "SELECT b_start, b_end FROM discovery_evidence WHERE evidence_kind='shared_text' LIMIT 1"
        ).fetchone()
    finally:
        conn.close()
    assert row is not None
    assert row[0] is None and row[1] is None
    assert verify_mod.verify(str(FIXTURE_DB), EXPECTED_FRAME_HASH) == 0


def test_positive_witness_shared_text_collision_claim_passes():
    conn = sqlite3.connect(f"file:{FIXTURE_DB}?mode=ro", uri=True)
    try:
        rows = conn.execute(
            """
            SELECT dc.claim_id, dc.claim_type, GROUP_CONCAT(de.evidence_kind)
            FROM discovery_claim dc JOIN discovery_evidence de ON de.claim_id = dc.claim_id
            GROUP BY dc.claim_id
            HAVING COUNT(DISTINCT de.evidence_kind) > 1
            """
        ).fetchall()
    finally:
        conn.close()
    assert len(rows) >= 1, "fixture must carry >=1 witness+shared_text collision claim (F7)"
    claim_id, claim_type, kinds = rows[0]
    assert claim_type in ("direct_witness", "quotes_this_work")
    assert "witness" in kinds and "shared_text" in kinds
    assert verify_mod.verify(str(FIXTURE_DB), EXPECTED_FRAME_HASH) == 0


# ---------------------------------------------------------------------------
# Negative cases -- each breaks exactly ONE invariant
# ---------------------------------------------------------------------------

def test_f4_source_corpus_mismatch_fails(tmp_path):
    db_path = _copy_fixture(tmp_path)
    conn = _connect_rw(db_path)
    conn.execute("UPDATE discovery_claim SET source_corpus = 'ja' WHERE page_id = 'p001'")
    conn.commit()
    conn.close()
    assert verify_mod.verify(str(db_path), EXPECTED_FRAME_HASH) != 0


def test_n1_nulled_a_side_snapshot_hash_fails(tmp_path):
    db_path = _copy_fixture(tmp_path)
    conn = _connect_rw(db_path)
    conn.execute(
        "UPDATE discovery_evidence SET snapshot_hash = NULL "
        "WHERE evidence_id = (SELECT evidence_id FROM discovery_evidence LIMIT 1)"
    )
    conn.commit()
    conn.close()
    assert verify_mod.verify(str(db_path), EXPECTED_FRAME_HASH) != 0


def test_r5_missing_a_page_id_fails(tmp_path):
    db_path = _copy_fixture(tmp_path)
    conn = _connect_rw(db_path)
    _recreate_table_without_constraints(conn, "discovery_evidence")
    conn.execute(
        "UPDATE discovery_evidence SET a_page_id = NULL "
        "WHERE evidence_id = (SELECT evidence_id FROM discovery_evidence LIMIT 1)"
    )
    conn.commit()
    conn.close()
    assert verify_mod.verify(str(db_path), EXPECTED_FRAME_HASH) != 0


def test_f12_cross_claim_display_pointer_fails(tmp_path):
    db_path = _copy_fixture(tmp_path)
    conn = _connect_rw(db_path)
    rows = conn.execute("SELECT claim_id, display_evidence_id FROM discovery_claim").fetchall()
    claim_a, claim_b = rows[0][0], rows[1][0]
    other_evidence_id = rows[1][1]
    conn.execute(
        "UPDATE discovery_claim SET display_evidence_id = ? WHERE claim_id = ?",
        (other_evidence_id, claim_a),
    )
    conn.commit()
    conn.close()
    assert verify_mod.verify(str(db_path), EXPECTED_FRAME_HASH) != 0


def test_r2_invalid_evidence_combination_fails(tmp_path):
    db_path = _copy_fixture(tmp_path)
    conn = _connect_rw(db_path)
    conn.execute(
        "UPDATE discovery_evidence SET confidence_band = 'not_evaluated' "
        "WHERE evidence_kind = 'witness' "
        "AND evidence_id = (SELECT evidence_id FROM discovery_evidence WHERE evidence_kind='witness' LIMIT 1)"
    )
    conn.commit()
    conn.close()
    assert verify_mod.verify(str(db_path), EXPECTED_FRAME_HASH) != 0


def test_duplicate_evidence_key_fails(tmp_path):
    db_path = _copy_fixture(tmp_path)
    conn = _connect_rw(db_path)
    _recreate_table_without_constraints(conn, "discovery_evidence")
    row = conn.execute("SELECT * FROM discovery_evidence LIMIT 1").fetchone()
    cols = [d[0] for d in conn.execute("SELECT * FROM discovery_evidence LIMIT 1").description]
    placeholders = ",".join("?" for _ in cols)
    conn.execute(f"INSERT INTO discovery_evidence ({','.join(cols)}) VALUES ({placeholders})", row)
    conn.commit()
    conn.close()
    assert verify_mod.verify(str(db_path), EXPECTED_FRAME_HASH) != 0


def test_integrity_fk_violation_fails(tmp_path):
    db_path = _copy_fixture(tmp_path)
    conn = _connect_rw(db_path)  # foreign_keys=OFF on the WRITER connection only
    conn.execute(
        "UPDATE discovery_evidence SET claim_id = 'ZZZ_FAKE_DANGLING_CLAIM_ID_ZZZ' "
        "WHERE evidence_id = (SELECT evidence_id FROM discovery_evidence LIMIT 1)"
    )
    conn.commit()
    conn.close()
    assert verify_mod.verify(str(db_path), EXPECTED_FRAME_HASH) != 0


def test_wrong_frame_hash_fails(tmp_path):
    db_path = _copy_fixture(tmp_path)
    conn = _connect_rw(db_path)
    conn.execute(
        "UPDATE meta SET value = 'deadbeef_wrong_hash' WHERE key = 'frame_content_hash'"
    )
    conn.commit()
    conn.close()
    assert verify_mod.verify(str(db_path), EXPECTED_FRAME_HASH) != 0


def test_wrong_expected_frame_hash_argument_fails():
    # meta + recomputed both agree, but the CALLER passed the wrong expected value.
    assert verify_mod.verify(str(FIXTURE_DB), "0" * 64) != 0


def test_g9_shared_text_parent_with_witness_evidence_fails(tmp_path):
    db_path = _copy_fixture(tmp_path)
    conn = _connect_rw(db_path)
    # p010/w000008 is a pure shared_text claim (the family-router row) -- flip
    # its claim_type to a witness type without adding any witness evidence.
    conn.execute(
        "UPDATE discovery_claim SET claim_type = 'direct_witness' "
        "WHERE page_id = 'p010' AND work_id = 'w000008'"
    )
    conn.commit()
    conn.close()
    assert verify_mod.verify(str(db_path), EXPECTED_FRAME_HASH) != 0


def test_g9_witness_parent_with_zero_witness_evidence_fails(tmp_path):
    db_path = _copy_fixture(tmp_path)
    conn = _connect_rw(db_path)
    # p006/w000002 is a pure witness (expert_verified, human_confirmed) claim
    # -- flip its claim_type to shared_text while it still carries the
    # witness evidence row (the reverse G9 direction).
    conn.execute(
        "UPDATE discovery_claim SET claim_type = 'shared_text' "
        "WHERE page_id = 'p006' AND work_id = 'w000002'"
    )
    conn.commit()
    conn.close()
    assert verify_mod.verify(str(db_path), EXPECTED_FRAME_HASH) != 0


def test_g8_collection_precision_on_band_row_fails(tmp_path):
    db_path = _copy_fixture(tmp_path)
    conn = _connect_rw(db_path)
    conn.execute(
        "UPDATE band_precision SET precision = 0.926 "
        "WHERE scope = 'band' AND evidence_source = 'track1_direct' AND confidence_band = 'tier_a'"
    )
    conn.commit()
    conn.close()
    assert verify_mod.verify(str(db_path), EXPECTED_FRAME_HASH) != 0


def test_g8_propagated_band_carrying_a_number_fails(tmp_path):
    db_path = _copy_fixture(tmp_path)
    conn = _connect_rw(db_path)
    conn.execute(
        "UPDATE band_precision SET precision = 0.75, ci_low = 0.6, ci_high = 0.9 "
        "WHERE scope = 'band' AND evidence_source = 'propagated' AND confidence_band = 'corroborated'"
    )
    conn.commit()
    conn.close()
    assert verify_mod.verify(str(db_path), EXPECTED_FRAME_HASH) != 0


def test_g8_missing_collection_row_fails(tmp_path):
    db_path = _copy_fixture(tmp_path)
    conn = _connect_rw(db_path)
    conn.execute("DELETE FROM band_precision WHERE scope = 'collection'")
    conn.commit()
    conn.close()
    assert verify_mod.verify(str(db_path), EXPECTED_FRAME_HASH) != 0


def test_g8_duplicate_collection_row_fails(tmp_path):
    db_path = _copy_fixture(tmp_path)
    conn = _connect_rw(db_path)
    row = conn.execute(
        "SELECT scope, collection_id, evidence_source, confidence_band, numerator, denominator, "
        "precision, ci_low, ci_high, method, sampling_frame, ins_policy, weighting, notes "
        "FROM band_precision WHERE scope = 'collection'"
    ).fetchone()
    conn.execute(
        "INSERT INTO band_precision (scope, collection_id, evidence_source, confidence_band, "
        "numerator, denominator, precision, ci_low, ci_high, method, sampling_frame, ins_policy, "
        "weighting, notes) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        row,
    )
    conn.commit()
    conn.close()
    assert verify_mod.verify(str(db_path), EXPECTED_FRAME_HASH) != 0


# ---------------------------------------------------------------------------
# Release-contract row-count mismatch
# ---------------------------------------------------------------------------

def test_release_contract_row_count_mismatch_fails(tmp_path):
    db_path = _copy_fixture(tmp_path)
    conn = _connect_rw(db_path)
    conn.execute(
        "UPDATE meta SET value = CAST(CAST(value AS INTEGER) + 1 AS TEXT) "
        "WHERE key = 'expected_rows_claims'"
    )
    conn.commit()
    conn.close()
    assert verify_mod.verify(str(db_path), EXPECTED_FRAME_HASH) != 0
