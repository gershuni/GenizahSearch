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

from scripts import build_discovery_sidecar as sidecar_build
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


# ---------------------------------------------------------------------------
# M4: strict release-mode-only band_precision validation -- gated so it
# NEVER fires against the synthetic fixture (sidecar_version stays
# "discovery-v1-synthetic-fixture"), only against a REAL_SIDECAR_VERSION db.
# ---------------------------------------------------------------------------

def _make_band_precision_only_db(tmp_path, band_precision_rows, *, sidecar_version):
    """A minimal db carrying ONLY a `meta.sidecar_version` + `band_precision`
    rows -- enough to exercise `check_band_precision` in isolation without
    needing a full claims/evidence/works graph.

    D-02a (136-06): the INSERT now also carries `measurement_status`, using
    the SAME `{"measurement_status": None, **r}` dict-literal override the
    real builder uses (`build_discovery_sidecar.py`'s own band_precision
    INSERT) -- a row's own `measurement_status` key (the frozen `tier_a`
    row's `measured_pass`) wins over this default, so a caller passing
    `_frozen_real_band_precision_rows()` gets the SAME shape a real build
    would write."""
    db_path = tmp_path / "band-precision-only.db"
    conn = sqlite3.connect(str(db_path))
    try:
        sidecar_build.create_schema(conn)
        conn.executemany(
            """
            INSERT INTO band_precision (
                scope, collection_id, evidence_source, confidence_band, numerator, denominator,
                precision, ci_low, ci_high, method, sampling_frame, ins_policy, weighting, notes,
                measurement_status
            ) VALUES (:scope, :collection_id, :evidence_source, :confidence_band, :numerator,
                       :denominator, :precision, :ci_low, :ci_high, :method, :sampling_frame,
                       :ins_policy, :weighting, :notes, :measurement_status)
            """,
            [{"measurement_status": None, **r} for r in band_precision_rows],
        )
        conn.execute("INSERT INTO meta (key, value) VALUES (?, ?)", ("sidecar_version", sidecar_version))
        conn.commit()
    finally:
        conn.close()
    return db_path


def test_m4_release_mode_strict_check_passes_on_frozen_rows(tmp_path):
    db_path = _make_band_precision_only_db(
        tmp_path, sidecar_build._frozen_real_band_precision_rows(),
        sidecar_version=sidecar_build.REAL_SIDECAR_VERSION,
    )
    conn = sqlite3.connect(str(db_path))
    try:
        meta = dict(conn.execute("SELECT key, value FROM meta").fetchall())
        violations = verify_mod.check_band_precision(conn, meta)
    finally:
        conn.close()
    assert violations == []


def test_m4_release_mode_strict_check_rejects_fabricated_tier_a(tmp_path):
    """M4: in release mode, a non-null tier_a precision (the OLD fabricated
    0.90 synthetic-placeholder shape) must be REJECTED -- the lax
    fixture-only checks above wouldn't have caught this on their own."""
    db_path = _make_band_precision_only_db(
        tmp_path, sidecar_build._band_precision_rows(),  # carries tier_a=0.90
        sidecar_version=sidecar_build.REAL_SIDECAR_VERSION,
    )
    conn = sqlite3.connect(str(db_path))
    try:
        meta = dict(conn.execute("SELECT key, value FROM meta").fetchall())
        violations = verify_mod.check_band_precision(conn, meta)
    finally:
        conn.close()
    assert any("tier_a" in v for v in violations)


def test_m4_release_mode_strict_check_rejects_missing_measured_band(tmp_path):
    rows = [r for r in sidecar_build._frozen_real_band_precision_rows()
            if not (r["scope"] == "band" and r["confidence_band"] == "screening_canon")]
    db_path = _make_band_precision_only_db(
        tmp_path, rows, sidecar_version=sidecar_build.REAL_SIDECAR_VERSION,
    )
    conn = sqlite3.connect(str(db_path))
    try:
        meta = dict(conn.execute("SELECT key, value FROM meta").fetchall())
        violations = verify_mod.check_band_precision(conn, meta)
    finally:
        conn.close()
    assert any("screening_canon" in v for v in violations)


def test_m4_strict_check_never_fires_on_synthetic_fixture_sidecar_version(tmp_path):
    """M4 gating: the SAME fabricated-tier_a rows that fail strict release
    validation above must NOT be flagged when sidecar_version is the
    synthetic-fixture constant -- the strict gate must never fire against
    the pinned 134-03 golden fixture."""
    db_path = _make_band_precision_only_db(
        tmp_path, sidecar_build._band_precision_rows(),  # carries tier_a=0.90
        sidecar_version=sidecar_build.SIDECAR_VERSION,  # synthetic-fixture constant
    )
    conn = sqlite3.connect(str(db_path))
    try:
        meta = dict(conn.execute("SELECT key, value FROM meta").fetchall())
        violations = verify_mod.check_band_precision(conn, meta)
    finally:
        conn.close()
    assert not any("(M4)" in v for v in violations)


def test_m4_release_mode_strict_check_rejects_duplicate_measured_band_row(tmp_path):
    """Codex R2 MED (dict-collapse fix): TWO rows sharing the SAME
    (collection_id, evidence_source, confidence_band) key for an
    already-satisfied expected band must be REJECTED -- even when the
    SECOND (later, higher rowid) row carries the frozen-correct value,
    which a naive dict keyed only on (source, band) (last-value-wins on
    plain assignment) would have let slip through silently."""
    rows = list(sidecar_build._frozen_real_band_precision_rows())
    expert_verified_row = next(
        r for r in rows if r["scope"] == "band" and r["confidence_band"] == "expert_verified"
    )
    # A wrong/extra duplicate inserted BEFORE the valid row -- a
    # last-value-wins dict collapse would keep only the (valid) second row
    # and never notice the extra, wrong one sharing its key.
    bogus_extra = {**expert_verified_row, "precision": 0.111}
    idx = rows.index(expert_verified_row)
    rows.insert(idx, bogus_extra)

    db_path = _make_band_precision_only_db(
        tmp_path, rows, sidecar_version=sidecar_build.REAL_SIDECAR_VERSION,
    )
    conn = sqlite3.connect(str(db_path))
    try:
        meta = dict(conn.execute("SELECT key, value FROM meta").fetchall())
        violations = verify_mod.check_band_precision(conn, meta)
    finally:
        conn.close()
    assert any("expert_verified" in v and "exactly 1" in v for v in violations)


def test_m4_release_mode_strict_check_rejects_extra_unexpected_band_row(tmp_path):
    """Codex R2 MED: an entirely extra band row (a confidence_band outside
    the frozen 6-key release row-set) must be rejected regardless of its
    own precision value -- even when that value is NULL, which the OLD
    "only reject non-null-and-unexpected" check would have missed
    entirely."""
    rows = list(sidecar_build._frozen_real_band_precision_rows())
    rows.append({
        "scope": "band", "collection_id": "e1_certification_registry_v1",
        "evidence_source": "track1_direct", "confidence_band": "bogus_extra_band",
        "numerator": None, "denominator": None, "precision": None,
        "ci_low": None, "ci_high": None, "method": None,
        "sampling_frame": None, "ins_policy": None, "weighting": None, "notes": None,
    })

    db_path = _make_band_precision_only_db(
        tmp_path, rows, sidecar_version=sidecar_build.REAL_SIDECAR_VERSION,
    )
    conn = sqlite3.connect(str(db_path))
    try:
        meta = dict(conn.execute("SELECT key, value FROM meta").fetchall())
        violations = verify_mod.check_band_precision(conn, meta)
    finally:
        conn.close()
    assert any("bogus_extra_band" in v for v in violations)


def test_m4_committed_synthetic_fixture_never_triggers_strict_checks():
    """The committed 134-03 golden fixture keeps its synthetic sidecar_version
    -- confirms the full verify() pipeline never runs the M4 strict checks
    against it (byte-identical fixture requirement)."""
    conn = sqlite3.connect(f"file:{FIXTURE_DB}?mode=ro", uri=True)
    try:
        meta = dict(conn.execute("SELECT key, value FROM meta").fetchall())
        violations = verify_mod.check_band_precision(conn, meta)
    finally:
        conn.close()
    assert not any("(M4)" in v for v in violations)


# ---------------------------------------------------------------------------
# Gate-bearing tables (Codex code review 2026-08-03, finding 4)
# ---------------------------------------------------------------------------
#
# Five registered checks read `discovery_routing_audit` behind
# `if not _has_table(...): return []`. That compat gate exists so a pre-v2 asset
# still verifies -- but nothing insisted the table be present on a CURRENT one,
# so deleting it turned all five green at once. `discovery_identification` and
# `manuscript_display` carried the same hazard.
#
# These are whole-verifier controls: each drops one table from a copy of the
# clean fixture and asserts `verify()` reports the specific inventory violation.
# Asserting on the message, not merely on a nonzero return, matters here --
# dropping a referenced table also trips the FK check, so a bare "it went red"
# assertion would pass even with the inventory check deleted.

import pytest  # noqa: E402


@pytest.mark.parametrize("table", [
    "discovery_routing_audit",
    "discovery_identification",
    "manuscript_display",
])
def test_dropping_a_gate_bearing_table_is_a_violation(tmp_path, capsys, table):
    db = _copy_fixture(tmp_path, f"no_{table}.db")
    conn = _connect_rw(db)
    conn.execute(f'DROP TABLE "{table}"')
    conn.commit()
    conn.close()

    rc = verify_mod.verify(str(db), EXPECTED_FRAME_HASH)
    captured = capsys.readouterr()
    out = captured.err + captured.out  # violations go to stderr

    assert rc != 0, f"dropping {table} left the verifier green"
    assert "gate-bearing table(s) absent" in out, (
        f"dropping {table} did not produce the inventory violation; the run failed "
        f"for some other reason, which means the inventory check is not what caught "
        f"it. Output:\n{out}"
    )
    assert table in out, f"the violation did not name {table}. Output:\n{out}"


def test_gate_bearing_control_is_not_vacuous():
    """The clean fixture carries all three tables, so the parametrized controls
    above are testing a state the fixture can actually leave -- not asserting on
    something already absent."""
    conn = sqlite3.connect(f"file:{FIXTURE_DB}?mode=ro", uri=True)
    try:
        present = {
            r[0] for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'")
        }
    finally:
        conn.close()
    assert verify_mod._GATE_BEARING_TABLES <= present, (
        "the clean fixture is missing a gate-bearing table, so the drop controls "
        "prove nothing"
    )


# ---------------------------------------------------------------------------
# NULL-as-absent genre (Codex code review 2026-08-03, finding 3)
# ---------------------------------------------------------------------------
#
# `check_works_genre_vocabulary` promised "never silently NULL-as-absent" while
# every query in it filtered NULLs out. The rule is scoped to REACHABLE works:
# measured on both real artifacts, ZERO of the 58 public / 181 private
# NULL-genre rows are reachable through discovery_identification, shipped
# evidence, or human-confirmed evidence. A blanket rule would fail the deployed
# artifact over rows no reader can meet.
#
# The control therefore has to make the work reachable, which is also what makes
# it a real test rather than a restatement of the query.

def _reachable_work_id(conn):
    """A work_id that IS reachable from a public surface in the fixture."""
    row = conn.execute(
        "SELECT w.work_id FROM works w "
        "JOIN discovery_identification di ON di.canonical_work_id = w.canonical_work_id "
        "LIMIT 1").fetchone()
    return row[0] if row else None


def test_null_genre_on_a_reachable_work_is_a_violation(tmp_path, capsys):
    db = _copy_fixture(tmp_path, "null_genre_reachable.db")
    conn = _connect_rw(db)
    work_id = _reachable_work_id(conn)
    assert work_id is not None, "fixture has no reachable work -- control is vacuous"
    conn.execute("UPDATE works SET genre = NULL WHERE work_id = ?", (work_id,))
    conn.commit()
    conn.close()

    rc = verify_mod.verify(str(db), EXPECTED_FRAME_HASH)
    captured = capsys.readouterr()
    out = captured.err + captured.out

    assert rc != 0, "a NULL genre on a reachable work left the verifier green"
    assert "reachable from a public surface have a NULL/empty genre" in out, (
        f"the run failed for some other reason, so this control proves nothing "
        f"about the genre check. Output:\n{out}"
    )


def test_null_genre_reachable_only_through_the_review_opt_in_is_a_violation(tmp_path, capsys):
    """The surface the first version of this check missed entirely (Codex code
    review 2A, finding 3).

    That version scoped reachability to the DEFAULT population -- an
    identification, or shipped, or human-confirmed evidence. But
    `get_claims_for_page(include_review=True)` sets `routing_clause = ""`,
    dropping the routing predicate and returning review-only and unreviewed
    claims with `works.genre` among their columns. Re-measured against that
    surface, 58 of 58 public and 181 of 181 private NULL-genre works are
    reachable -- every one, not a handful.

    The sibling control above cannot express this: `_reachable_work_id` joins
    through `discovery_identification`, so its work is reachable by the OLD rule
    too and passes either way.

    Here the work has NO identification row and NO shipped or human-confirmed
    evidence -- only a review-only claim. Under the old scoping it is invisible;
    under the correct one it is a violation.
    """
    db = _copy_fixture(tmp_path, "null_genre_review_only.db")
    conn = _connect_rw(db)
    conn.execute("PRAGMA foreign_keys = OFF")
    row = conn.execute(
        """SELECT w.work_id FROM works w
            JOIN discovery_claim dc ON dc.work_id = w.work_id
           WHERE NOT EXISTS (SELECT 1 FROM discovery_identification di
                              WHERE di.canonical_work_id = w.canonical_work_id)
           LIMIT 1"""
    ).fetchone()
    if row is None:
        # Make one: strip the fixture work's identification rows so it is
        # reachable ONLY through the opt-in read.
        work_id = _reachable_work_id(conn)
        assert work_id is not None, "fixture has no claim-bearing work at all"
        canonical = conn.execute(
            "SELECT canonical_work_id FROM works WHERE work_id = ?", (work_id,)
        ).fetchone()[0]
        conn.execute(
            "DELETE FROM discovery_identification WHERE canonical_work_id = ?", (canonical,))
    else:
        work_id = row[0]

    # Neither eligibility limb, so the OLD rule cannot see this work.
    conn.execute(
        """UPDATE discovery_evidence SET routing_status = 'review_only',
                                         adjudication_status = 'unreviewed'
            WHERE claim_id IN (SELECT claim_id FROM discovery_claim WHERE work_id = ?)""",
        (work_id,),
    )
    conn.execute("UPDATE works SET genre = NULL WHERE work_id = ?", (work_id,))
    conn.commit()

    # The control is only meaningful if the row really is invisible to the old
    # rule -- otherwise it would pass for the wrong reason.
    still_default_reachable = conn.execute(
        """SELECT COUNT(*) FROM works w
            WHERE w.work_id = ?
              AND (EXISTS (SELECT 1 FROM discovery_identification di
                            WHERE di.canonical_work_id = w.canonical_work_id)
                OR EXISTS (SELECT 1 FROM discovery_claim dc
                             JOIN discovery_evidence e ON e.claim_id = dc.claim_id
                            WHERE dc.work_id = w.work_id
                              AND (e.routing_status = 'shipped'
                                   OR e.adjudication_status = 'human_confirmed')))""",
        (work_id,),
    ).fetchone()[0]
    conn.close()
    assert still_default_reachable == 0, (
        "the seeded work is STILL reachable by the old shipped-or-confirmed rule, "
        "so this control would pass without the widened scoping and proves nothing"
    )

    rc = verify_mod.verify(str(db), EXPECTED_FRAME_HASH)
    captured = capsys.readouterr()
    out = captured.err + captured.out

    assert rc != 0, (
        "a NULL genre on a work reachable ONLY through the review opt-in left the "
        "verifier green -- the scoping is back to the default population"
    )
    assert "reachable from a public surface have a NULL/empty genre" in out, (
        f"the run failed for another reason, so this proves nothing about the "
        f"genre check. Output:\n{out}"
    )


def test_entirely_unpopulated_genre_column_is_the_pre_rebuild_state_not_a_violation(tmp_path):
    """The state the fixture used to represent with a single stray NULL, now
    exercised where it belongs: blank the WHOLE column on a copy.

    A fully unpopulated genre column is the legitimate pre-rebuild state and must
    pass. One NULL among populated rows is partial population, which is a
    different thing entirely -- and on a reachable work it is the NULL-as-absent
    the contract forbids (see the test above)."""
    db = _copy_fixture(tmp_path, "genre_unpopulated.db")
    conn = _connect_rw(db)
    conn.execute("UPDATE works SET genre = NULL")
    conn.commit()
    conn.close()

    assert verify_mod.verify(str(db), EXPECTED_FRAME_HASH) == 0, (
        "an entirely unpopulated genre column must remain the pre-rebuild state, "
        "not a release violation"
    )


def test_partially_populated_genre_column_is_not_confused_with_unpopulated(tmp_path, capsys):
    """Guards the distinction directly: the clean fixture is fully populated, so
    blanking exactly one reachable row must fail while blanking all of them
    passes. If someone widens the early-return to `any NULL present`, this fails."""
    db = _copy_fixture(tmp_path, "genre_partial.db")
    conn = _connect_rw(db)
    work_id = _reachable_work_id(conn)
    conn.execute("UPDATE works SET genre = NULL WHERE work_id = ?", (work_id,))
    conn.commit()
    conn.close()

    rc = verify_mod.verify(str(db), EXPECTED_FRAME_HASH)
    captured = capsys.readouterr()
    assert rc != 0, (
        "partial population was treated as the pre-rebuild state -- a single "
        "NULL now hides behind the unpopulated-column early return. Output:\n"
        + captured.err + captured.out
    )
