# -*- coding: utf-8 -*-
"""Evidence-combination + display-pointer-ownership tests over the fixture
(Phase 134, plan 134-03, Task 3).
"""
import shutil
import sqlite3
from pathlib import Path

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
# test_valid_evidence_combinations
# ---------------------------------------------------------------------------

def test_valid_evidence_combinations_over_fixture():
    conn = sqlite3.connect(f"file:{FIXTURE_DB}?mode=ro", uri=True)
    try:
        rows = conn.execute(
            "SELECT evidence_kind, evidence_source, confidence_band FROM discovery_evidence"
        ).fetchall()
    finally:
        conn.close()
    assert rows, "fixture must carry evidence rows"
    for kind, source, band in rows:
        assert (kind, source, band) in verify_mod.VALID_EVIDENCE_COMBOS, (
            f"invalid combination ({kind}, {source}, {band}) -- R2"
        )
    # Sanity: witness+shared_text collision combination itself is valid.
    assert (ids.EVIDENCE_KIND_SHARED_TEXT, ids.EVIDENCE_SOURCE_PROPAGATED,
            ids.CONFIDENCE_BAND_NOT_EVALUATED) in verify_mod.VALID_EVIDENCE_COMBOS


def test_multi_work_per_manuscript_keeps_separate_claims():
    # Landmine 10: p012/sys012 witnesses TWO distinct works -- must be TWO
    # separate discovery_claim rows, never merged/collapsed.
    conn = sqlite3.connect(f"file:{FIXTURE_DB}?mode=ro", uri=True)
    try:
        rows = conn.execute(
            "SELECT DISTINCT de.sys_id, dc.work_id "
            "FROM discovery_evidence de JOIN discovery_claim dc ON dc.claim_id = de.claim_id"
        ).fetchall()
    finally:
        conn.close()
    by_sys = {}
    for sys_id, work_id in rows:
        by_sys.setdefault(sys_id, set()).add(work_id)
    multi_work_sys = [s for s, works in by_sys.items() if len(works) > 1]
    assert multi_work_sys, "fixture must carry >=1 manuscript witnessing multiple distinct works"

    conn = sqlite3.connect(f"file:{FIXTURE_DB}?mode=ro", uri=True)
    try:
        for sys_id in multi_work_sys:
            claim_rows = conn.execute(
                "SELECT DISTINCT dc.page_id, dc.work_id, dc.claim_id "
                "FROM discovery_claim dc JOIN discovery_evidence de ON de.claim_id = dc.claim_id "
                "WHERE de.sys_id = ?",
                (sys_id,),
            ).fetchall()
            claim_ids = {r[2] for r in claim_rows}
            works_for_sys = {r[1] for r in claim_rows}
            assert len(claim_ids) == len(works_for_sys), (
                "each distinct work on a shared manuscript must have its OWN claim_id "
                "(no claim-key collapse across works)"
            )
    finally:
        conn.close()


def test_at_least_one_claim_carries_multiple_bands():
    # The dropped "one band per claim key" invariant: a claim MAY legitimately
    # carry >1 evidence rows with DIFFERENT bands.
    conn = sqlite3.connect(f"file:{FIXTURE_DB}?mode=ro", uri=True)
    try:
        rows = conn.execute(
            """
            SELECT dc.claim_id, GROUP_CONCAT(DISTINCT de.confidence_band)
            FROM discovery_claim dc JOIN discovery_evidence de ON de.claim_id = dc.claim_id
            GROUP BY dc.claim_id
            HAVING COUNT(DISTINCT de.confidence_band) > 1
            """
        ).fetchall()
    finally:
        conn.close()
    assert len(rows) >= 1, "fixture must carry >=1 claim spanning multiple confidence_band values"


# ---------------------------------------------------------------------------
# test_display_pointer_ownership
# ---------------------------------------------------------------------------

def test_display_pointer_ownership_clean_fixture_passes():
    conn = sqlite3.connect(f"file:{FIXTURE_DB}?mode=ro", uri=True)
    try:
        violations = verify_mod.check_display_pointer_ownership(conn)
    finally:
        conn.close()
    assert violations == []


def test_display_pointer_ownership_cross_claim_pointer_fails(tmp_path):
    db_path = _copy_fixture(tmp_path)
    conn = _connect_rw(db_path)
    rows = conn.execute("SELECT claim_id, display_evidence_id FROM discovery_claim").fetchall()
    claim_a = rows[0][0]
    other_evidence_id = rows[1][1]
    # 136-12: the D-10a UNIQUE index on `discovery_claim(display_evidence_id)`
    # now rejects this mutation at the DDL layer -- a stronger guarantee than the
    # F12 check, and one the previous golden fixture did not carry. The index is
    # dropped here so the F12 VERIFIER check itself is still exercised: defence
    # in depth means both layers must work, and only one of them is testable
    # while the other is holding the door shut.
    conn.execute("DROP INDEX IF EXISTS ux_discovery_claim_display_evidence_id")
    conn.execute(
        "UPDATE discovery_claim SET display_evidence_id = ? WHERE claim_id = ?",
        (other_evidence_id, claim_a),
    )
    conn.commit()

    violations = verify_mod.check_display_pointer_ownership(conn)
    conn.close()
    assert violations, "a cross-claim display_evidence_id must fail F12"
