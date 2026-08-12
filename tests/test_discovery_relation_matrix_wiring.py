# -*- coding: utf-8 -*-
"""C-track wiring: the matrix as it reaches an ASSET — stored by the builder,
re-stored by the projector over the pruned population, and checked row-for-row
by the release verifier.

`tests/test_discovery_relation_matrix.py` owns the semantics; this file owns the
three claims that only an asset can settle:

1. every matrix input is recoverable from stored columns (so the recompute is a
   function of the artifact, not of build-time scratch state);
2. the verifier's equality gate is PROVEN able to fail — a single mis-stored row
   must fail the build;
3. the public asset recomputes rather than copies, and does so with Contract 1's
   input tables already in place.

Honest limitation, recorded here because it is easy to mistake green for
coverage: the SYNTHETIC asset has `max_coverage_ppm` NULL on all 18
identifications, so a stock synthetic build reaches step 5 on every row and
exercises no other branch. Every test below that needs another branch drives the
stored columns explicitly."""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
for _p in (str(REPO_ROOT), str(REPO_ROOT / "scripts")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import build_discovery_sidecar as sidecar_build
import discovery_ids as ids
import verify_discovery_sidecar as verify_mod
from shared import discovery_relation_matrix as matrix


def _build_synthetic(path: Path) -> None:
    conn = sqlite3.connect(str(path))
    conn.execute("PRAGMA foreign_keys = ON")
    try:
        sidecar_build.create_schema(conn)
        sidecar_build.populate_synthetic(conn, source_db_hash="matrix-wiring-test")
        conn.commit()
    finally:
        conn.close()


def _meta(conn: sqlite3.Connection) -> dict:
    return {k: v for k, v in conn.execute("SELECT key, value FROM meta")}


def _identification_ids(conn: sqlite3.Connection):
    return [r[0] for r in conn.execute(
        "SELECT identification_id FROM discovery_identification ORDER BY identification_id")]


@pytest.fixture()
def asset(tmp_path):
    """A synthetic asset, open, with every identification given known coverage
    so rows reach step 6 instead of all short-circuiting at step 5."""
    db = tmp_path / "asset.db"
    _build_synthetic(db)
    conn = sqlite3.connect(str(db))
    conn.execute("UPDATE discovery_identification SET max_coverage_ppm = 500000")
    matrix.recompute_and_store(conn, matrix.DEPLOY_1_PARAMETERIZATION)
    conn.commit()
    try:
        yield conn
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# 1. The stored asset carries every input the matrix needs.
# ---------------------------------------------------------------------------

def test_the_synthetic_asset_reaches_only_step_5_as_documented(tmp_path):
    """Pins the limitation stated in this module's docstring, so a future change
    that gives the synthetic dataset coverage has to come here and update the
    claim rather than silently invalidate it."""
    db = tmp_path / "stock.db"
    _build_synthetic(db)
    conn = sqlite3.connect(str(db))
    try:
        stored = {r[0] for r in conn.execute(
            "SELECT DISTINCT rendered_relation FROM discovery_identification")}
        assert stored == {ids.RENDERED_RELATION_UNCERTAIN}
        (n_cov,) = conn.execute(
            "SELECT COUNT(*) FROM discovery_identification "
            "WHERE max_coverage_ppm IS NOT NULL").fetchone()
        assert n_cov == 0, "synthetic coverage appeared -- re-read this module's docstring"
    finally:
        conn.close()


def test_with_coverage_present_rows_render_their_stored_relation(asset):
    """Step 6, end to end through the real materializer path: the census follows
    `relation_kind`, which is what "the stored relation stands" means."""
    census = dict(asset.execute(
        "SELECT rendered_relation, COUNT(*) FROM discovery_identification "
        "GROUP BY rendered_relation"))
    kinds = dict(asset.execute(
        "SELECT relation_kind, COUNT(*) FROM discovery_identification "
        "GROUP BY relation_kind"))
    assert census == kinds
    assert census.get(ids.RENDERED_RELATION_DIRECT_WITNESS, 0) > 0


def test_step_1_is_readable_from_eligibility_basis(asset):
    """`eligibility_basis='human_confirmed'` IS "no shipped evidence at all" --
    the encoding the recompute depends on. If this ever stops being true, the
    recompute silently mis-renders step 1 rather than failing."""
    target = _identification_ids(asset)[0]
    asset.execute(
        "UPDATE discovery_identification SET eligibility_basis = ? "
        "WHERE identification_id = ?",
        (ids.ADJUDICATION_STATUS_HUMAN_CONFIRMED, target),
    )
    matrix.recompute_and_store(asset, matrix.DEPLOY_1_PARAMETERIZATION)
    (stored,) = asset.execute(
        "SELECT rendered_relation FROM discovery_identification "
        "WHERE identification_id = ?", (target,)).fetchone()
    assert stored == ids.RENDERED_RELATION_UNCERTAIN


def test_step_2_router_reason_demotes_a_stored_direct_row(asset):
    target = _identification_ids(asset)[0]
    asset.execute(
        "UPDATE discovery_identification "
        "SET routing_reason = ?, relation_kind = ? WHERE identification_id = ?",
        (ids.ROUTING_REASON_CO_CITATION, ids.CLAIM_TYPE_DIRECT_WITNESS, target),
    )
    matrix.recompute_and_store(asset, matrix.DEPLOY_1_PARAMETERIZATION)
    (stored,) = asset.execute(
        "SELECT rendered_relation FROM discovery_identification "
        "WHERE identification_id = ?", (target,)).fetchone()
    assert stored == ids.RENDERED_RELATION_SHARED_TEXT


def test_step_4b_curated_list_moves_the_rows_of_that_work(asset):
    """The owner's 2026-08-12 ruling, exercised on an asset: putting a canonical
    work on the curated list re-renders ITS rows and no others."""
    (canonical, n_of_work) = asset.execute(
        "SELECT canonical_work_id, COUNT(*) FROM discovery_identification "
        "WHERE relation_kind = ? GROUP BY canonical_work_id ORDER BY 2 DESC LIMIT 1",
        (ids.CLAIM_TYPE_DIRECT_WITNESS,),
    ).fetchone()
    asset.execute(
        "INSERT INTO discovery_curated_quoter "
        "(list_version, canonical_work_id, ruled_date, note) "
        "VALUES ('quoter-test', ?, '2026-08-12', 'wiring test')",
        (canonical,),
    )
    matrix.recompute_and_store(asset, matrix.DEPLOY_1_PARAMETERIZATION)
    (n_quoted,) = asset.execute(
        "SELECT COUNT(*) FROM discovery_identification "
        "WHERE rendered_relation = ? AND canonical_work_id = ?",
        (ids.RENDERED_RELATION_QUOTES_THIS_WORK, canonical),
    ).fetchone()
    assert n_quoted == n_of_work
    (n_elsewhere,) = asset.execute(
        "SELECT COUNT(*) FROM discovery_identification "
        "WHERE rendered_relation = ? AND canonical_work_id != ? AND relation_kind = ?",
        (ids.RENDERED_RELATION_QUOTES_THIS_WORK, canonical,
         ids.CLAIM_TYPE_DIRECT_WITNESS),
    ).fetchone()
    assert n_elsewhere == 0


def test_the_census_returned_by_the_recompute_matches_the_table(asset):
    census = matrix.recompute_and_store(asset, matrix.DEPLOY_1_PARAMETERIZATION)
    stored = dict(asset.execute(
        "SELECT rendered_relation, COUNT(*) FROM discovery_identification "
        "GROUP BY rendered_relation"))
    assert {k: v for k, v in census.items() if v} == stored
    assert set(census) == ids.RENDERED_RELATIONS   # every state keyed, zero or not


# ---------------------------------------------------------------------------
# 2. The verifier gate — clean, and PROVEN able to fail.
# ---------------------------------------------------------------------------

def test_the_gate_is_silent_on_a_freshly_built_asset(tmp_path):
    db = tmp_path / "clean.db"
    _build_synthetic(db)
    conn = sqlite3.connect(str(db))
    try:
        assert verify_mod.check_relation_matrix_recompute(conn, _meta(conn)) == []
    finally:
        conn.close()


@pytest.mark.parametrize("wrong", [
    ids.RENDERED_RELATION_DIRECT_WITNESS,
    ids.RENDERED_RELATION_SHARED_TEXT,
    ids.RENDERED_RELATION_QUOTES_THIS_WORK,
])
def test_the_gate_fails_on_one_mis_stored_row(asset, wrong):
    """The mutation control. One row out of eighteen, set to a value the matrix
    would not produce, must be caught — a gate that only notices wholesale
    corruption would miss precisely the case that matters (a single work
    over-claiming)."""
    target = _identification_ids(asset)[0]
    (correct,) = asset.execute(
        "SELECT rendered_relation FROM discovery_identification "
        "WHERE identification_id = ?", (target,)).fetchone()
    if correct == wrong:
        pytest.skip("this row already renders that state")
    asset.execute(
        "UPDATE discovery_identification SET rendered_relation = ? "
        "WHERE identification_id = ?", (wrong, target))
    violations = verify_mod.check_relation_matrix_recompute(asset, _meta(asset))
    assert len(violations) == 1
    assert "Contract 1" in violations[0]
    assert target in violations[0]
    assert repr(wrong) in violations[0]


def test_the_gate_catches_a_curated_list_the_stored_values_predate(asset):
    """The realistic failure this gate exists for: the owner's ruling is ingested
    but the grain was materialized BEFORE it (or the projector copied the column
    instead of recomputing). Every row of that work then over-claims, and no
    per-row sanity rule can see it — the values are individually legal."""
    (canonical,) = asset.execute(
        "SELECT canonical_work_id FROM discovery_identification LIMIT 1").fetchone()
    asset.execute(
        "INSERT INTO discovery_curated_quoter "
        "(list_version, canonical_work_id, ruled_date, note) "
        "VALUES ('quoter-test', ?, '2026-08-12', 'ingested after the grain')",
        (canonical,),
    )
    violations = verify_mod.check_relation_matrix_recompute(asset, _meta(asset))
    assert violations, "a curated ruling the stored relations predate must fail the build"
    assert "Contract 1" in violations[0]


def test_the_gate_refuses_an_asset_missing_its_parameterization(asset):
    """Post-batch marker present, parameterization keys gone: the stored values
    cannot be re-derived, so nothing vouches for them."""
    for key in matrix.PARAMETERIZATION_META_KEYS:
        asset.execute("DELETE FROM meta WHERE key = ?", (key,))
    violations = verify_mod.check_relation_matrix_recompute(asset, _meta(asset))
    assert len(violations) == 1
    assert "cannot be re-derived" in violations[0]


def test_the_gate_stays_quiet_on_a_pre_batch_asset(asset):
    """Rollback safety: an asset built before the batch has neither the marker
    nor the parameterization, and asserts nothing about either."""
    for key in tuple(matrix.PARAMETERIZATION_META_KEYS) + ("locus_schema_version",):
        asset.execute("DELETE FROM meta WHERE key = ?", (key,))
    assert verify_mod.check_relation_matrix_recompute(asset, _meta(asset)) == []


def test_the_gate_refuses_a_foreign_matrix_version(asset):
    asset.execute(
        "UPDATE meta SET value = 'matrix-v9' WHERE key = 'relation_matrix_version'")
    violations = verify_mod.check_relation_matrix_recompute(asset, _meta(asset))
    assert len(violations) == 1
    assert "unusable matrix parameterization" in violations[0]


def test_the_gate_refuses_region_active_while_no_footprint_recipe_exists(asset):
    """Fail-closed, not region-blind. If step 3 were switched on before the
    D-track footprint lands, recomputing with an absent region input would
    silently bless every demotion the builder made (or failed to make)."""
    asset.execute(
        "UPDATE meta SET value = '1' WHERE key = 'relation_matrix_region_active'")
    violations = verify_mod.check_relation_matrix_recompute(asset, _meta(asset))
    assert len(violations) == 1
    assert "cannot recompute rendered_relation" in violations[0]
    assert "region" in violations[0]


def test_a_systematic_failure_is_summarized_not_dumped(asset):
    """55,377 identifiers in one violation string is a message nobody reads."""
    asset.execute("UPDATE discovery_identification SET rendered_relation = ?",
                  (ids.RENDERED_RELATION_QUOTES_THIS_WORK,))
    violations = verify_mod.check_relation_matrix_recompute(asset, _meta(asset))
    assert len(violations) == 1
    assert "more)" in violations[0]
    assert violations[0].count("stored ") <= verify_mod._RELATION_MISMATCH_REPORT_LIMIT


# ---------------------------------------------------------------------------
# 3. The public asset recomputes, with Contract 1's inputs already in place.
# ---------------------------------------------------------------------------

def test_the_grain_is_rematerialized_after_every_base_table_is_populated():
    """What actually sequences the projector is the explicit post-pass in
    `project()` — the tables are projected SORTED, and the order-sensitive steps
    run afterwards. So the property to pin is that the identification
    rematerialization happens after the generic insert loop, not that
    `PROJECTION_RULES` is typed in some order (it is iterated `sorted`, so its
    key order controls nothing — a test asserting that order would pass forever
    while proving nothing).

    Measured, not read: a curated ruling reaches the public asset even when the
    rules dict is reordered to put the grain first."""
    import project_discovery_public as proj

    src = Path(proj.__file__).read_text(encoding="utf-8")
    loop = src.index("for table in sorted(")
    grain = src.index("_materialize_public_identification(out_conn)", loop)
    membership = src.index("_materialize_public_stratum_membership(", grain)
    assert loop < grain < membership, (
        "the identification rematerialization must follow the generic insert "
        "loop (so Contract 1's input tables are populated) and precede the "
        "membership copy (which is keyed by identification)"
    )
    # And the rule itself must not materialize during the loop.
    assert proj._project_discovery_identification(
        proj.ProjectionContext.__new__(proj.ProjectionContext)) == []


def test_the_public_asset_honours_a_curated_ruling(tmp_path):
    """End to end through the projector: a curated work in the PRIVATE asset
    renders `quotes_this_work` in the PUBLIC one, and the public asset passes its
    own recompute gate."""
    import project_discovery_public as proj

    private = tmp_path / "private.db"
    _build_synthetic(private)
    conn = sqlite3.connect(str(private))
    try:
        conn.execute("UPDATE discovery_identification SET max_coverage_ppm = 500000")
        (canonical,) = conn.execute(
            "SELECT canonical_work_id FROM discovery_identification "
            "WHERE relation_kind = ? LIMIT 1", (ids.CLAIM_TYPE_DIRECT_WITNESS,)
        ).fetchone()
        conn.execute(
            "INSERT INTO discovery_curated_quoter "
            "(list_version, canonical_work_id, ruled_date, note) "
            "VALUES ('quoter-test', ?, '2026-08-12', 'x')",
            (canonical,),
        )
        matrix.recompute_and_store(conn, matrix.DEPLOY_1_PARAMETERIZATION)
        conn.commit()
    finally:
        conn.close()

    public = tmp_path / "public.db"
    proj.project(str(private), str(public),
                 masking_patterns=["ZZ-MATRIX-WIRING-DISPOSABLE-MARKER-ZZ"])

    out = sqlite3.connect(str(public))
    try:
        (n_curated,) = out.execute(
            "SELECT COUNT(*) FROM discovery_curated_quoter").fetchone()
        if not n_curated:
            pytest.skip("the curated work did not survive public projection")
        (n_quoted,) = out.execute(
            "SELECT COUNT(*) FROM discovery_identification "
            "WHERE canonical_work_id = ? AND rendered_relation = ?",
            (canonical, ids.RENDERED_RELATION_QUOTES_THIS_WORK),
        ).fetchone()
        (n_rows,) = out.execute(
            "SELECT COUNT(*) FROM discovery_identification "
            "WHERE canonical_work_id = ?", (canonical,)).fetchone()
        assert n_rows and n_quoted == n_rows
        assert verify_mod.check_relation_matrix_recompute(out, _meta(out)) == []
    finally:
        out.close()


def test_the_projected_asset_passes_its_own_recompute_gate(tmp_path):
    """The plain case, which is the one that would break if the projector ever
    went back to COPYING the column: the public asset's step-4a denominators are
    its own."""
    import project_discovery_public as proj

    private = tmp_path / "private.db"
    _build_synthetic(private)
    public = tmp_path / "public.db"
    proj.project(str(private), str(public),
                 masking_patterns=["ZZ-MATRIX-WIRING-DISPOSABLE-MARKER-ZZ"])
    out = sqlite3.connect(str(public))
    try:
        assert verify_mod.check_relation_matrix_recompute(out, _meta(out)) == []
        assert _meta(out).get("relation_matrix_version") == matrix.MATRIX_VERSION
    finally:
        out.close()
