# -*- coding: utf-8 -*-
"""Membership frame_content_hash tests (Phase 134, plan 134-03, Task 3).

Proves the frame hash is a MEMBERSHIP hash (claim/evidence/band/display
composition), not a raw-byte hash: mutating a band or dropping a claim
changes it.
"""
import json
import shutil
import sqlite3
from pathlib import Path

from scripts import build_discovery_sidecar as sidecar_build

FIXTURE_DIR = Path(__file__).resolve().parent / "fixtures" / "discovery"
FIXTURE_DB = FIXTURE_DIR / "discovery-v1-fixture.db"
FIXTURE_EXPECTED = FIXTURE_DIR / "discovery-v1-fixture-expected.json"


def _load_expected():
    return json.loads(FIXTURE_EXPECTED.read_text(encoding="utf-8"))


def _copy_fixture(tmp_path, name="mutate.db"):
    dest = tmp_path / name
    shutil.copyfile(FIXTURE_DB, dest)
    return dest


def _connect_rw(db_path):
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA foreign_keys = OFF")
    return conn


def test_frame_hash_golden_matches_expected_and_meta():
    expected = _load_expected()
    conn = sqlite3.connect(f"file:{FIXTURE_DB}?mode=ro", uri=True)
    try:
        recomputed = sidecar_build.compute_frame_content_hash(conn)
        (meta_hash,) = conn.execute(
            "SELECT value FROM meta WHERE key = 'frame_content_hash'"
        ).fetchone()
    finally:
        conn.close()
    assert recomputed == expected["frame_content_hash"]
    assert recomputed == meta_hash


def test_frame_hash_changes_on_band_mutation(tmp_path):
    baseline_conn = sqlite3.connect(f"file:{FIXTURE_DB}?mode=ro", uri=True)
    baseline = sidecar_build.compute_frame_content_hash(baseline_conn)
    baseline_conn.close()

    db_path = _copy_fixture(tmp_path, "band_mutate.db")
    conn = _connect_rw(db_path)
    conn.execute(
        "UPDATE discovery_evidence SET confidence_band = 'weak' "
        "WHERE evidence_id = (SELECT evidence_id FROM discovery_evidence "
        "WHERE confidence_band = 'corroborated' LIMIT 1)"
    )
    conn.commit()
    mutated = sidecar_build.compute_frame_content_hash(conn)
    conn.close()

    assert mutated != baseline, "flipping one evidence row's band must change the frame hash"


def test_frame_hash_changes_on_claim_drop(tmp_path):
    baseline_conn = sqlite3.connect(f"file:{FIXTURE_DB}?mode=ro", uri=True)
    baseline = sidecar_build.compute_frame_content_hash(baseline_conn)
    baseline_conn.close()

    db_path = _copy_fixture(tmp_path, "claim_drop.db")
    conn = _connect_rw(db_path)
    (claim_id,) = conn.execute(
        "SELECT claim_id FROM discovery_claim WHERE page_id = 'p008' AND work_id = 'w000006'"
    ).fetchone()
    # Drop BOTH the claim and its evidence rows (a full claim removal) --
    # FKs are off on this writer connection so ordering doesn't matter.
    conn.execute("DELETE FROM discovery_evidence WHERE claim_id = ?", (claim_id,))
    conn.execute("DELETE FROM discovery_claim WHERE claim_id = ?", (claim_id,))
    conn.commit()
    mutated = sidecar_build.compute_frame_content_hash(conn)
    conn.close()

    assert mutated != baseline, "dropping a claim must change the frame hash"


def test_frame_hash_stable_across_repeated_recompute():
    conn = sqlite3.connect(f"file:{FIXTURE_DB}?mode=ro", uri=True)
    try:
        h1 = sidecar_build.compute_frame_content_hash(conn)
        h2 = sidecar_build.compute_frame_content_hash(conn)
    finally:
        conn.close()
    assert h1 == h2
