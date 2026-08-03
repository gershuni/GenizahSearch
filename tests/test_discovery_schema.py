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


# ===========================================================================
# 136-12 Task 3: the curated artifacts (loaded by content hash) and the TWELVE
# new release-verifier checks -- one per field the Phase-136 rebuild adds
# (docs/specs/discovery-sidecar-schema-v1.md SS Amendment 2026-08-02).
# ===========================================================================

import hashlib
import inspect
import io
import json

from shared import discovery_novelty as novelty_mod
from shared import discovery_visibility as vis_mod

# The twelve checks this plan registers, in `verify()`'s own accumulation order.
_NEW_CHECKS = (
    "check_coverage_persistence",
    "check_band_rank_materialized",
    "check_novelty_status_vocabulary",
    "check_novelty_source_label_masked",
    "check_divergence_correctness_applicability",
    "check_visibility_axes",
    "check_identification_grain",
    "check_manuscript_display_carries_no_reference_content",
    "check_works_genre_vocabulary",
    "check_meta_audience",
    "check_authorized_index_set",
    "check_kept_tie_names_its_pair",
)


def _fixture_conn():
    return sqlite3.connect(f"file:{FIXTURE_DB}?mode=ro", uri=True)


def _mutable_fixture(tmp_path, name="mutated.db"):
    return _copy_fixture(tmp_path, name=name)


def _rebuild_without_constraints(conn, table):
    """Rebuild ``table`` preserving its rows but WITHOUT its CHECK/NOT NULL
    constraints.

    Every defect the twelve new checks exist to catch is ALSO blocked by a DDL
    constraint -- which is the point of defence in depth, and exactly what makes
    the verifier untestable while the constraint holds the door shut. Seeding
    the defect therefore requires an asset whose constraints were not enforced
    at write time, which is precisely the asset a verifier is FOR (a
    hand-edited, externally-produced, or partially-migrated file).

    Implemented as ``CREATE TABLE ... AS SELECT`` rather than an
    ``ALTER TABLE ... DROP CONSTRAINT`` (SQLite has none) or a
    ``writable_schema`` edit (which needs a schema-cookie bump and silently
    no-ops when the DDL text does not match byte for byte -- a false green)."""
    cols = [r[1] for r in conn.execute('PRAGMA table_info("' + table + '")')]
    col_list = ", ".join('"' + c + '"' for c in cols)
    conn.execute('CREATE TABLE "' + table + '__free" AS SELECT ' + col_list
                 + ' FROM "' + table + '"')
    conn.execute('DROP TABLE "' + table + '"')
    conn.execute('ALTER TABLE "' + table + '__free" RENAME TO "' + table + '"')
    conn.commit()


# ---------------------------------------------------------------------------
# Registration + independence
# ---------------------------------------------------------------------------

def test_twelve_new_checks_exist_and_are_registered_in_verify():
    """A check that exists but is never called is not a gate."""
    assert len(_NEW_CHECKS) == 12
    source = inspect.getsource(verify_mod.verify)
    for name in _NEW_CHECKS:
        assert hasattr(verify_mod, name), f"{name} is not defined"
        assert f"{name}(conn" in source, f"{name} is not registered in verify()"


def test_new_checks_follow_the_amendment_ordering_convention():
    """Registered in the amendment's own subsection order -- (A) evidence/works
    additions, (B) the new tables, (C)/(C1), (D), (F) -- so a reader of
    `verify()` can walk the contract top to bottom."""
    source = inspect.getsource(verify_mod.verify)
    positions = [source.index(f"{name}(conn") for name in _NEW_CHECKS]
    assert positions == sorted(positions)


def test_the_new_checks_do_not_import_the_builders_constants():
    """Each new check must be independent of the builder -- otherwise a builder
    bug is invisible to the verifier that exists to catch it. The vocabularies
    are MIRRORED as local literals (the verifier's own standing convention) and
    guarded against drift by the tests below."""
    for name in _NEW_CHECKS:
        body = inspect.getsource(getattr(verify_mod, name))
        assert "sidecar_build" not in body, f"{name} reaches into the builder"
    # The verifier's only two builder references BOTH predate this plan and are
    # deliberately shared: the canonical frame-hash recipe and the real-mode
    # sidecar_version literal. Pinned so a third cannot appear unnoticed.
    src = io.open(verify_mod.__file__, encoding="utf-8").read()
    assert src.count("sidecar_build.") == 2
    assert "sidecar_build.compute_frame_content_hash" in src
    assert "sidecar_build.REAL_SIDECAR_VERSION" in src


def test_verifier_vocabulary_mirrors_do_not_drift_from_their_contract_modules():
    """The mirrors are only safe because this test makes a drift a RED SUITE."""
    assert verify_mod._NOVELTY_STATUSES == novelty_mod.NOVELTY_STATUSES
    assert verify_mod._DIVERGENCE_SHADES == novelty_mod.DIVERGENCE_SHADES
    assert verify_mod._DIVERGENCE_CORRECTNESS_VALUES == novelty_mod.DIVERGENCE_CORRECTNESS_VALUES
    assert verify_mod._MASKED_PROVENANCE_LABELS == novelty_mod.MASKED_PROVENANCE_LABELS
    assert verify_mod._VISIBILITY_VALUES == vis_mod.VISIBILITY_VALUES
    assert verify_mod._COVERAGE_STATUSES == sidecar_build.COVERAGE_STATUSES
    assert verify_mod._AUDIENCE_VALUES == sidecar_build.ASSET_AUDIENCES
    assert verify_mod._PRIVATE_AUDIENCE == sidecar_build.ASSET_AUDIENCE_PRIVATE
    assert verify_mod._GENRE_UNASSIGNED == sidecar_build.GENRE_UNASSIGNED
    assert verify_mod._GENRE_PATH_SEPARATOR == sidecar_build.GENRE_PATH_SEPARATOR
    from shared.discovery_main_pool import MAIN_POOL_REASONS
    assert verify_mod._MAIN_POOL_REASONS == set(MAIN_POOL_REASONS)


def test_the_refreshed_golden_fixture_passes_every_invariant():
    """The whole battery, over the committed fixture -- the same code 136-13
    runs over the real distilled asset."""
    assert verify_mod.verify(str(FIXTURE_DB)) == 0


def test_no_new_violation_message_interpolates_a_cell_value(tmp_path):
    """Sentinel test: seed an obviously-fabricated value into every column the
    new checks read, and assert no violation message ECHOES it. On the
    novelty/provenance/visibility columns that is not cosmetic -- echoing the
    value is the exact leak D-25/NOVEL-02 exists to prevent."""
    sentinel = "ZZZ_FAKE_SENTINEL_VALUE_ZZZ"
    db_path = _mutable_fixture(tmp_path)
    conn = _connect_rw(db_path)
    _rebuild_without_constraints(conn, "discovery_evidence")
    conn.execute("UPDATE discovery_evidence SET novelty_source_label = ?", (sentinel,))
    conn.execute("UPDATE discovery_evidence SET coverage_status = ?", (sentinel,))
    conn.execute("UPDATE works SET genre = ?", (sentinel,))
    conn.execute("UPDATE meta SET value = ? WHERE key = 'audience'", (sentinel,))
    conn.commit()
    conn.close()

    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        meta = dict(conn.execute("SELECT key, value FROM meta").fetchall())
        messages = []
        messages += verify_mod.check_coverage_persistence(conn)
        messages += verify_mod.check_novelty_source_label_masked(conn)
        messages += verify_mod.check_works_genre_vocabulary(conn, meta)
        messages += verify_mod.check_meta_audience(conn, meta)
    finally:
        conn.close()

    assert messages, "the seeded values must actually trip the checks"
    for message in messages:
        assert sentinel not in message, f"violation message echoed a cell value: {message}"


# ---------------------------------------------------------------------------
# novelty_status -- the TEN-value vocabulary, fail-closed
# ---------------------------------------------------------------------------

def test_novelty_status_check_fails_closed_on_every_retired_vocabulary(tmp_path):
    """Fails on ANY value outside the TEN -- including every earlier draft's
    vocabulary: the three-value tri-state, the E' eight-value set with its
    unsplit `diverges`, and the pre-ruling-H nine-value set."""
    retired = [
        "not_in_finding_aids", "already_recorded",   # the D-23a tri-state
        "indeterminate",                              # its alternative naming
        "diverges",                                   # retired by ruling F
        "witness",                                    # rejected name (ruling H)
        "ZZZ_FAKE_UNKNOWN_SHADE_ZZZ",
    ]
    for value in retired:
        db_path = _mutable_fixture(tmp_path, name=f"novelty-{abs(hash(value))}.db")
        conn = _connect_rw(db_path)
        # The DDL CHECK rejects this outright -- defence in depth. Drop it so
        # the VERIFIER layer is exercised too, on an asset whose constraints
        # were not enforced at write time.
        _rebuild_without_constraints(conn, "discovery_evidence")
        conn.execute("UPDATE discovery_evidence SET novelty_status = ?", (value,))
        conn.commit()
        conn.close()

        conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
        try:
            violations = verify_mod.check_novelty_status_vocabulary(conn)
        finally:
            conn.close()
        assert violations, f"retired/unknown novelty value {value!r} must fail the check"
        assert any("TEN-value" in v for v in violations)


def test_novelty_status_check_accepts_every_one_of_the_ten(tmp_path):
    """...and accepts each of the ten, so the check is a closed vocabulary and
    not merely a denylist of the tokens someone remembered."""
    for value in sorted(novelty_mod.NOVELTY_STATUSES):
        db_path = _mutable_fixture(tmp_path, name=f"ok-{value}.db")
        conn = _connect_rw(db_path)
        conn.execute("UPDATE discovery_evidence SET novelty_status = ?", (value,))
        conn.execute("UPDATE discovery_identification SET novelty_status = ?", (value,))
        conn.commit()
        conn.close()
        conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
        try:
            violations = verify_mod.check_novelty_status_vocabulary(conn)
        finally:
            conn.close()
        # `fills_gap`/`not_checked` are ineligible for a source label, and the
        # fixture carries none, so no other check is implicated either.
        assert violations == [], f"{value!r} is one of the ten and must pass"


def test_novelty_status_check_catches_a_claim_whose_rows_disagree(tmp_path):
    """D-23a, enforced independently of the builder's own assertion."""
    db_path = _mutable_fixture(tmp_path)
    conn = _connect_rw(db_path)
    claim_id = conn.execute(
        "SELECT claim_id FROM discovery_evidence GROUP BY claim_id HAVING COUNT(*) > 1"
    ).fetchone()[0]
    victim = conn.execute(
        "SELECT evidence_id FROM discovery_evidence WHERE claim_id = ? LIMIT 1",
        (claim_id,)).fetchone()[0]
    conn.execute(
        "UPDATE discovery_evidence SET novelty_status = 'fills_gap' WHERE evidence_id = ?",
        (victim,))
    conn.commit()
    conn.close()

    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        violations = verify_mod.check_novelty_status_vocabulary(conn)
    finally:
        conn.close()
    assert any("disagree" in v for v in violations)


def test_novelty_source_label_check_rejects_an_unmasked_value(tmp_path):
    db_path = _mutable_fixture(tmp_path)
    conn = _connect_rw(db_path)
    conn.execute(
        "UPDATE discovery_evidence SET novelty_status='confirms', "
        "novelty_source_label='ZZZ_FAKE_UNMASKED_ZZZ'")
    conn.commit()
    conn.close()
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        violations = verify_mod.check_novelty_source_label_masked(conn)
    finally:
        conn.close()
    assert any("masked label set" in v for v in violations)


# ---------------------------------------------------------------------------
# divergence_correctness -- BOTH directions, as ruling L leaves them
# ---------------------------------------------------------------------------

def test_divergence_correctness_check_rejects_a_value_on_a_non_divergence_shade(tmp_path):
    db_path = _mutable_fixture(tmp_path)
    conn = _connect_rw(db_path)
    _rebuild_without_constraints(conn, "discovery_evidence")
    conn.execute(
        "UPDATE discovery_evidence SET novelty_status='confirms', "
        "divergence_correctness='catalogue_correct'")
    conn.commit()
    conn.close()

    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        violations = verify_mod.check_divergence_correctness_applicability(conn)
    finally:
        conn.close()
    assert any("non-divergence shade" in v for v in violations)


def test_divergence_correctness_check_rejects_an_out_of_vocabulary_value(tmp_path):
    db_path = _mutable_fixture(tmp_path)
    conn = _connect_rw(db_path)
    _rebuild_without_constraints(conn, "discovery_evidence")
    conn.execute(
        "UPDATE discovery_evidence SET novelty_status='diverges_work', "
        "divergence_correctness='ZZZ_FAKE_CORRECTNESS_ZZZ'")
    conn.commit()
    conn.close()

    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        violations = verify_mod.check_divergence_correctness_applicability(conn)
    finally:
        conn.close()
    assert any("three-value correctness vocabulary" in v for v in violations)


def test_divergence_shade_with_null_correctness_PASSES_the_check_per_ruling_l(tmp_path):
    """136-12-PLAN.md asks this direction to FAIL. Owner ruling L (2026-08-03,
    `136-GATE1-DECISIONS.md` SS L) makes it mandatory to PASS, and the plan text
    predates the ruling.

    The model no longer produces `divergence_correctness` at all
    (`resolve_model_output` always returns `None` for it) and no human/owner
    annotation pathway exists yet, so NULL is the ONLY value a build can write
    on a divergence row. A verifier that required non-NULL here would make
    every `diverges_work`/`diverges_part` row unshippable -- silently deleting
    ruling F's opt-in divergence category from the asset."""
    db_path = _mutable_fixture(tmp_path)
    conn = _connect_rw(db_path)
    conn.execute(
        "UPDATE discovery_evidence SET novelty_status='diverges_work', "
        "divergence_correctness=NULL")
    conn.execute(
        "UPDATE discovery_identification SET novelty_status='diverges_part', "
        "divergence_correctness=NULL")
    conn.commit()
    conn.close()

    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        violations = verify_mod.check_divergence_correctness_applicability(conn)
    finally:
        conn.close()
    assert violations == []


# ---------------------------------------------------------------------------
# The visibility axes, meta.audience and the index set
# ---------------------------------------------------------------------------

def test_visibility_axes_check_rejects_a_null_and_an_out_of_enum_value(tmp_path):
    for column, table in (("assertion_visibility", "discovery_evidence"),
                          ("identity_visibility", "works")):
        db_path = _mutable_fixture(tmp_path, name=f"vis-null-{column}.db")
        conn = _connect_rw(db_path)
        _rebuild_without_constraints(conn, table)
        conn.execute(f"UPDATE {table} SET {column} = NULL")
        conn.commit()
        conn.close()
        conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
        try:
            violations = verify_mod.check_visibility_axes(conn)
        finally:
            conn.close()
        assert any(f"{table}.{column}" in v and "NULL" in v for v in violations)


def test_meta_audience_check_requires_exactly_private(tmp_path):
    """A fixture build carries `private`; a MISSING value and an out-of-enum
    value both fail; and `public` fails too -- only the public projection may
    write that, which is what makes the boundary structural."""
    conn = _fixture_conn()
    try:
        meta = dict(conn.execute("SELECT key, value FROM meta").fetchall())
        assert verify_mod.check_meta_audience(conn, meta) == []
    finally:
        conn.close()
    assert meta["audience"] == "private"

    conn = _fixture_conn()
    try:
        assert verify_mod.check_meta_audience(conn, {}) == ["meta.audience: absent -- "
                                                            "the artifact must declare "
                                                            "its own audience"]
        assert verify_mod.check_meta_audience(conn, {"audience": "ZZZ_FAKE_AUDIENCE_ZZZ"})
        # `public` still fails the DEFAULT (private) profile -- that boundary is
        # what makes the exclusion structural, and it is unchanged.
        assert verify_mod.check_meta_audience(conn, {"audience": "public"})
        # 2026-08-03 (136-13): the check gained an explicit audience so the same
        # verifier can gate the PUBLIC projection, which plan 136-13 requires. The
        # public artifact passes only when it is verified AS public, and a private
        # artifact verified as public still fails -- neither direction is a hole.
        assert verify_mod.check_meta_audience(conn, {"audience": "public"}, "public") == []
        assert verify_mod.check_meta_audience(conn, {"audience": "private"}, "public")
    finally:
        conn.close()


def test_authorized_index_set_check_catches_a_dropped_index(tmp_path):
    db_path = _mutable_fixture(tmp_path)
    conn = _connect_rw(db_path)
    conn.execute("DROP INDEX ix_discovery_evidence_novelty_status")
    conn.commit()
    conn.close()
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        violations = verify_mod.check_authorized_index_set(conn)
    finally:
        conn.close()
    assert any("ix_discovery_evidence_novelty_status" in v for v in violations)


def test_kept_tie_check_catches_a_null_demoted_work_id(tmp_path):
    db_path = _mutable_fixture(tmp_path)
    conn = _connect_rw(db_path)
    conn.execute(
        "INSERT INTO discovery_routing_audit "
        "(page_id, kept_work_id, demoted_work_id, kept_year, demoted_year, delta_years, "
        " decision, routing_reason) VALUES ('p001','w000001',NULL,1000,1010,10,'kept_tie',NULL)")
    conn.commit()
    conn.close()
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        violations = verify_mod.check_kept_tie_names_its_pair(conn)
    finally:
        conn.close()
    assert any("kept_tie" in v and "demoted_work_id" in v for v in violations)


def test_coverage_and_band_rank_checks_catch_their_own_defects(tmp_path):
    db_path = _mutable_fixture(tmp_path)
    conn = _connect_rw(db_path)
    conn.execute(
        "UPDATE discovery_evidence SET coverage_ppm = 500000 "
        "WHERE evidence_source = 'propagated'")
    conn.execute("UPDATE discovery_evidence SET band_rank = 99")
    conn.commit()
    conn.close()
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        coverage = verify_mod.check_coverage_persistence(conn)
        band = verify_mod.check_band_rank_materialized(conn)
    finally:
        conn.close()
    assert any("DIRECT FAMILY ONLY" in v for v in coverage)
    assert any("lattice range" in v for v in band)


def test_manuscript_display_check_is_schema_level_not_row_level(tmp_path):
    """Checked on the SCHEMA, so an EMPTY table cannot pass vacuously -- the
    synthetic fixture's `manuscript_display` has no rows at all."""
    conn = _fixture_conn()
    try:
        (n_rows,) = conn.execute("SELECT COUNT(*) FROM manuscript_display").fetchone()
        assert verify_mod.check_manuscript_display_carries_no_reference_content(conn) == []
    finally:
        conn.close()
    assert n_rows == 0

    db_path = _mutable_fixture(tmp_path)
    conn = _connect_rw(db_path)
    conn.execute("ALTER TABLE manuscript_display ADD COLUMN work_title TEXT")
    conn.commit()
    conn.close()
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        violations = verify_mod.check_manuscript_display_carries_no_reference_content(conn)
    finally:
        conn.close()
    assert any("work_title" in v for v in violations)


def test_identification_grain_check_catches_a_tampered_row_count(tmp_path):
    db_path = _mutable_fixture(tmp_path)
    conn = _connect_rw(db_path)
    conn.execute("DELETE FROM discovery_identification WHERE rowid = "
                 "(SELECT rowid FROM discovery_identification LIMIT 1)")
    conn.commit()
    conn.close()
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        violations = verify_mod.check_identification_grain(conn)
    finally:
        conn.close()
    assert any("pair count" in v for v in violations)


# ---------------------------------------------------------------------------
# works.genre + the curated artifacts
# ---------------------------------------------------------------------------

def test_works_genre_check_requires_a_path_or_the_unassigned_sentinel(tmp_path):
    db_path = _mutable_fixture(tmp_path)
    conn = _connect_rw(db_path)
    conn.execute("UPDATE works SET genre = 'BareLeafWithNoParent'")
    conn.commit()
    conn.close()
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        meta = dict(conn.execute("SELECT key, value FROM meta").fetchall())
        violations = verify_mod.check_works_genre_vocabulary(conn, meta)
    finally:
        conn.close()
    assert any("well-formed" in v for v in violations)


def test_unassigned_survives_into_the_asset_as_a_real_queryable_value():
    """`Unassigned` is a REAL value with its own bucket, not missing data -- a
    work the vocabulary cannot place stays VISIBLE in the corpus view rather
    than disappearing from the facet."""
    conn = _fixture_conn()
    try:
        rows = conn.execute(
            "SELECT work_id FROM works WHERE genre = ?",
            (sidecar_build.GENRE_UNASSIGNED,)).fetchall()
        meta = dict(conn.execute("SELECT key, value FROM meta").fetchall())
        violations = verify_mod.check_works_genre_vocabulary(conn, meta)
    finally:
        conn.close()
    assert rows, "the fixture must carry at least one Unassigned work"
    assert violations == []


def test_populated_genre_must_name_its_pinned_artifact(tmp_path):
    """A populated genre column with no provenance pin is a column nobody can
    trace back to a reviewed artifact."""
    db_path = _mutable_fixture(tmp_path)
    conn = _connect_rw(db_path)
    conn.execute("DELETE FROM meta WHERE key = 'work_domains_content_hash'")
    conn.commit()
    conn.close()
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        meta = dict(conn.execute("SELECT key, value FROM meta").fetchall())
        violations = verify_mod.check_works_genre_vocabulary(conn, meta)
    finally:
        conn.close()
    assert any("work_domains_content_hash" in v for v in violations)


def test_curated_content_hash_recipe_matches_the_curation_scripts_own(tmp_path):
    """The builder reproduces `curate_work_domains.compute_content_hash` rather
    than importing it (that module pulls in `shared.fjms_service` and the 1.59 GB
    FJMS sidecar). This test is what keeps the duplication honest."""
    payload = [
        {"canonical_work_id": "w000001", "domain_parent": "P", "domain_leaf": "L",
         "confidence": "high", "provenance": "rule:synthetic"},
        {"canonical_work_id": "w000002", "domain_parent": "Unassigned",
         "domain_leaf": "Unassigned", "confidence": "medium", "provenance": "manual:synthetic"},
    ]
    expected = "sha256:" + hashlib.sha256(
        json.dumps(payload, sort_keys=True, ensure_ascii=False).encode("utf-8")).hexdigest()
    assert sidecar_build.curated_content_hash(payload) == expected


def test_work_genre_value_maps_the_unassigned_bucket_to_the_bare_sentinel():
    assert sidecar_build.work_genre_value(
        {"domain_parent": "Midrash", "domain_leaf": "Aggadic Midrashim"}
    ) == "Midrash / Aggadic Midrashim"
    assert sidecar_build.work_genre_value(
        {"domain_parent": "Unassigned", "domain_leaf": "Unassigned"}) == "Unassigned"


def _write_domain_artifact(tmp_path, assignments, name="work_domains.json"):
    payload = {
        "artifact": "work_domains",
        "artifact_version": "v1",
        "content_hash": sidecar_build.curated_content_hash(assignments),
        "assignments": assignments,
    }
    path = tmp_path / name
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    return str(path), payload["content_hash"]


def test_curated_artifact_refuses_a_content_hash_mismatch(tmp_path):
    rows = [{"canonical_work_id": "w000001", "domain_parent": "P", "domain_leaf": "L",
             "confidence": "high", "provenance": "rule:synthetic"}]
    path, real = _write_domain_artifact(tmp_path, rows)
    with pytest.raises(sidecar_build.CuratedArtifactError) as exc:
        sidecar_build.load_work_domains(path, content_hash="sha256:" + "0" * 64)
    assert "pin mismatch" in str(exc.value)
    genres, stats = sidecar_build.load_work_domains(path, content_hash=real)
    assert genres == {"w000001": "P / L"}
    assert stats["work_domains_assigned"] == 1


def test_curated_artifact_refuses_an_unpinned_load(tmp_path):
    rows = [{"canonical_work_id": "w000001", "domain_parent": "P", "domain_leaf": "L",
             "confidence": "high", "provenance": "rule:synthetic"}]
    path, _real = _write_domain_artifact(tmp_path, rows)
    with pytest.raises(sidecar_build.CuratedArtifactError) as exc:
        sidecar_build.load_work_domains(path, content_hash=None)
    assert "without a content-hash pin" in str(exc.value)


def test_curated_artifact_refuses_a_payload_edited_after_it_declared_its_hash(tmp_path):
    """An artifact edited after pinning (T-136-12-04): the SELF hash no longer
    matches its own payload, which is caught before the caller's pin is even
    consulted."""
    rows = [{"canonical_work_id": "w000001", "domain_parent": "P", "domain_leaf": "L",
             "confidence": "high", "provenance": "rule:synthetic"}]
    path, real = _write_domain_artifact(tmp_path, rows)
    doc = json.loads(io.open(path, encoding="utf-8").read())
    doc["assignments"][0]["domain_leaf"] = "TAMPERED"
    io.open(path, "w", encoding="utf-8").write(json.dumps(doc, ensure_ascii=False))
    with pytest.raises(sidecar_build.CuratedArtifactError) as exc:
        sidecar_build.load_work_domains(path, content_hash=real)
    assert "SELF content_hash mismatch" in str(exc.value)


def test_curated_artifact_release_gate_holds_an_unruled_needs_ruling_row(tmp_path):
    """The 'ship as Unassigned' default was explicitly DECLINED by the owner, so
    a held row REFUSES the build rather than defaulting."""
    rows = [
        {"canonical_work_id": "w000001", "domain_parent": None, "domain_leaf": None,
         "confidence": "needs-ruling", "provenance": "needs-ruling:synthetic",
         "candidate_leaves": [{"domain_parent": "P", "domain_leaf": "L"}]},
    ]
    path, real = _write_domain_artifact(tmp_path, rows)
    with pytest.raises(sidecar_build.CuratedArtifactError) as exc:
        sidecar_build.load_work_domains(path, content_hash=real)
    assert "RELEASE GATE" in str(exc.value)


def test_release_gate_keys_on_owner_ruling_not_on_confidence(tmp_path):
    """The 29 owner-ruled rows deliberately RETAIN `confidence: needs-ruling`;
    the `owner_ruling` citation is what marks them settled (owner rulings P and
    Q). Keying on `confidence` here would refuse a build the owner has already
    authorized."""
    rows = [
        {"canonical_work_id": "w000001", "domain_parent": "Rabbinic Literature",
         "domain_leaf": "Other", "confidence": "needs-ruling",
         "provenance": "owner-ruling:136-GATE1-DECISIONS.md",
         "owner_ruling": "136-GATE1-DECISIONS.md SS Ruling P"},
    ]
    path, real = _write_domain_artifact(tmp_path, rows)
    genres, stats = sidecar_build.load_work_domains(path, content_hash=real)
    assert genres == {"w000001": "Rabbinic Literature / Other"}
    assert stats["work_domains_assigned"] == 1


def test_apply_work_genres_writes_at_the_canonical_grain():
    """Assignment is keyed on `canonical_work_id`, so a D-13a duplicate is never
    assigned twice and two `works` rows sharing a canonical id always agree."""
    conn = sqlite3.connect(":memory:")
    sidecar_build.create_schema(conn)
    try:
        conn.executemany(
            "INSERT INTO works (work_id, canonical_work_id, neutral_title, author, genre, "
            "source_corpus, identity_visibility) VALUES (?, ?, 'T', NULL, NULL, 'sefaria', "
            "'public')",
            [("w000001", "w000001"), ("w000002", "w000001"), ("w000003", "w000003")],
        )
        stats = sidecar_build.apply_work_genres(
            conn, {"w000001": "P / L", "w000003": sidecar_build.GENRE_UNASSIGNED})
        rows = dict(conn.execute("SELECT work_id, genre FROM works").fetchall())
    finally:
        conn.close()
    assert rows == {"w000001": "P / L", "w000002": "P / L", "w000003": "Unassigned"}
    assert stats["works_genre_written"] == 3


def test_author_key_coverage_is_enforced_against_the_asset():
    """The curated author key writes no column (none is authorized), so it is
    bound to the asset by an enforced coverage check instead."""
    conn = sqlite3.connect(":memory:")
    sidecar_build.create_schema(conn)
    try:
        conn.execute(
            "INSERT INTO works (work_id, canonical_work_id, neutral_title, author, genre, "
            "source_corpus, identity_visibility) VALUES "
            "('w000001','w000001','T','Synthetic Author A',NULL,'sefaria','public')")
        # 2026-08-03 (136-13): the coverage check is scoped to works carrying a
        # SHIPPED claim -- the population the curated artifact is actually built
        # from. A work with no claims at all is outside that scope and is
        # correctly invisible to the check, so the fixture must now give this
        # work a shipped row or it asserts nothing.
        conn.execute(
            "INSERT INTO discovery_claim (page_id, work_id, claim_id, claim_type, "
            "display_evidence_id, source_corpus, sidecar_version) VALUES "
            "('p1','w000001','c1','direct_witness','e1','sefaria','fixture-v1')")
        conn.execute(
            "INSERT INTO discovery_evidence (evidence_id, claim_id, evidence_kind, "
            "evidence_source, confidence_band, adjudication_status, audit_status, "
            "routing_status, routing_reason, is_new, a_page_id, sys_id, span_start, "
            "span_end, novelty_status, assertion_visibility) VALUES "
            "('e1','c1','witness','track1_direct','tier_a','unreviewed','n/a',"
            "'shipped','none',0,'p1','s1',0,10,'not_checked','public')")
        stats = sidecar_build.assert_author_key_coverage(
            conn, {"Synthetic Author A": {"author": "Synthetic Author A"}})
        assert stats["works_author_strings_covered"] == 1
        with pytest.raises(sidecar_build.CuratedArtifactError) as exc:
            sidecar_build.assert_author_key_coverage(conn, {})
        # The message names the COUNT, never the author strings.
        assert "Synthetic Author A" not in str(exc.value)
        assert "1 distinct works.author" in str(exc.value)
    finally:
        conn.close()
