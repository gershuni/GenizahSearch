# -*- coding: utf-8 -*-
"""CD schema batch (schema Amendment 2026-08-12) — the lockstep proof set.

Covers, in the amendment's own order:

  (N)/(Q)/(R) the seven new tables exist in a fresh build, empty, counted;
  (O) `rendered_relation` stores the fail-closed state on every row;
  (P) `routing_reason` equals a from-scratch recompute of the best-row rule;
  (T) the Contract-0 basis gate + the CERT-01 frame-regression gate, each
      PROVEN able to fail (the standing mutation-control convention: a gate
      that has never failed in a test is a gate nobody has seen work);
  (U) the loader's marker-conditional requirements, both directions.

Plus the three-way mirror drift guards: the builder's table list, the
verifier's deliberately-not-imported local literal, and the fixture module's
column maps must all agree, because the verifier's independence convention
forbids importing the builder's constants.
"""
from __future__ import annotations

import shutil
import sqlite3
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
for _p in (str(REPO_ROOT), str(REPO_ROOT / "scripts")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import build_discovery_sidecar as sidecar_build
import check_frame_regression as frame_regression
import discovery_ids as ids
import verify_discovery_sidecar as verify_mod

from tests.fixtures import discovery_v2_fixture as fixture_mod

import web.discovery_assets as da


# ---------------------------------------------------------------------------
# Shared synthetic asset (the golden recipe, in tmp)
# ---------------------------------------------------------------------------

def _build_synthetic(path: Path) -> None:
    conn = sqlite3.connect(str(path))
    conn.execute("PRAGMA foreign_keys = ON")
    try:
        sidecar_build.create_schema(conn)
        sidecar_build.populate_synthetic(conn, source_db_hash="cd-batch-test")
        conn.commit()
    finally:
        conn.close()


def _meta(conn: sqlite3.Connection) -> dict:
    return {k: v for k, v in conn.execute("SELECT key, value FROM meta")}


# ---------------------------------------------------------------------------
# Mirror drift guards
# ---------------------------------------------------------------------------

def test_builder_verifier_fixture_table_lists_cannot_drift():
    builder_tables = tuple(sidecar_build.AMENDMENT_2026_08_12_COUNT_TABLES)
    verifier_tables = tuple(verify_mod._AMENDMENT_2026_08_12_COUNT_TABLES)
    fixture_tables = tuple(fixture_mod.CD_BATCH_TABLE_COLUMNS)
    loader_tables = tuple(sorted(da._AMENDMENT_2026_08_12_TABLES))
    assert builder_tables == verifier_tables
    assert sorted(builder_tables) == sorted(fixture_tables)
    assert sorted(builder_tables) == sorted(loader_tables)


def test_identification_column_mirrors_cannot_drift():
    verifier_cols = set(verify_mod._AMENDMENT_2026_08_12_IDENTIFICATION_COLUMNS)
    fixture_cols = {
        name for name, _decl in fixture_mod.CD_BATCH_ADDED_COLUMNS["discovery_identification"]
    }
    loader_cols = set(da._AMENDMENT_2026_08_12_COLUMNS["discovery_identification"])
    assert verifier_cols == fixture_cols == loader_cols


def test_rendered_relation_vocabulary_is_in_the_ddl_check_literal():
    # A SQLite CHECK cannot import ids.RENDERED_RELATIONS; the DDL literal is
    # the one unavoidable restatement, so pin it against the frozen enum.
    for state in ids.RENDERED_RELATIONS:
        assert f"'{state}'" in sidecar_build._DDL


# ---------------------------------------------------------------------------
# The fresh-build facts: (N)/(O)/(P)/(U)
# ---------------------------------------------------------------------------

def test_fresh_build_carries_the_batch_shape(tmp_path):
    db = tmp_path / "cd.db"
    _build_synthetic(db)
    conn = sqlite3.connect(str(db))
    try:
        tables = {
            r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
        for table in sidecar_build.AMENDMENT_2026_08_12_COUNT_TABLES:
            assert table in tables, table
            (n,) = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
            assert n == 0, f"{table} must ship EMPTY from the batch"
        meta = _meta(conn)
        assert meta.get("locus_schema_version") == sidecar_build.LOCUS_SCHEMA_VERSION
        for table in sidecar_build.AMENDMENT_2026_08_12_COUNT_TABLES:
            assert meta.get(f"expected_rows_{table}") == "0"
        # (O): the fail-closed posture, every row, until C-track.
        (n_bad,) = conn.execute(
            "SELECT COUNT(*) FROM discovery_identification WHERE rendered_relation != ?",
            (ids.RENDERED_RELATION_FAIL_CLOSED,),
        ).fetchone()
        assert n_bad == 0
    finally:
        conn.close()


def test_routing_reason_equals_best_row_recompute(tmp_path):
    """(P), Codex finding 2's rule pinned by from-scratch recompute: the
    identification's routing_reason is the reason of the SAME row selected as
    `best` (lowest band_rank, then lexicographic evidence_id) over the
    eligible (shipped OR human_confirmed) evidence set."""
    db = tmp_path / "cd.db"
    _build_synthetic(db)
    conn = sqlite3.connect(str(db))
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            """
            SELECT de.sys_id, w.canonical_work_id, de.band_rank, de.evidence_id,
                   de.routing_reason
            FROM discovery_evidence de
            JOIN discovery_claim dc ON dc.claim_id = de.claim_id
            JOIN works w            ON w.work_id  = dc.work_id
            WHERE de.routing_status = 'shipped'
               OR de.adjudication_status = 'human_confirmed'
            """
        ).fetchall()
        groups = {}
        for r in rows:
            groups.setdefault((r["sys_id"], r["canonical_work_id"]), []).append(r)
        expected = {
            key: min(
                group,
                key=lambda r: (
                    r["band_rank"] if r["band_rank"] is not None
                    else sidecar_build.BAND_RANK_LATTICE_SIZE,
                    r["evidence_id"],
                ),
            )["routing_reason"]
            for key, group in groups.items()
        }
        stored = {
            (r["sys_id"], r["canonical_work_id"]): r["routing_reason"]
            for r in conn.execute(
                "SELECT sys_id, canonical_work_id, routing_reason "
                "FROM discovery_identification")
        }
        assert stored == expected
        assert stored, "the synthetic asset must actually exercise the rule"
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# (T) Contract 0 — the basis gate, proven able to fail
# ---------------------------------------------------------------------------

def _add_locus_rows(db: Path) -> None:
    conn = sqlite3.connect(str(db))
    try:
        (work_id,) = conn.execute("SELECT work_id FROM works LIMIT 1").fetchone()
        conn.execute(
            "INSERT INTO locus_work (work_id, family, grain, stream_len, unit_count) "
            "VALUES (?, 'sefaria', 'chapter', 1000, 1)",
            (work_id,),
        )
        conn.execute(
            "INSERT INTO locus_unit (work_id, unit_ord, start_offset, part_key, "
            "label_he, citation_pos) VALUES (?, 0, 0, 'ch:0.1', 'פרק א', 1)",
            (work_id,),
        )
        conn.commit()
    finally:
        conn.close()


def test_contract0_gate_full_matrix(tmp_path):
    db = tmp_path / "cd.db"
    _build_synthetic(db)
    conn = sqlite3.connect(str(db))
    try:
        # Empty locus + no pins: nothing asserted yet -- clean.
        assert verify_mod.check_locus_reference_basis(conn, {}) == []
        # Contradictory pins are a violation even with an empty locus table.
        both_unequal = {
            "reference_corpus_sha256": "a" * 64,
            "locus_reference_corpus_sha256": "b" * 64,
        }
        assert len(verify_mod.check_locus_reference_basis(conn, both_unequal)) == 1
    finally:
        conn.close()

    _add_locus_rows(db)
    conn = sqlite3.connect(str(db))
    try:
        # Populated locus with NO pins: both required -- two violations.
        violations = verify_mod.check_locus_reference_basis(conn, {})
        assert len(violations) == 2
        # Equal pins: clean.
        both_equal = {
            "reference_corpus_sha256": "a" * 64,
            "locus_reference_corpus_sha256": "a" * 64,
        }
        assert verify_mod.check_locus_reference_basis(conn, both_equal) == []
        # THE mutation control: flip one hash, the gate must fail.
        flipped = dict(both_equal, locus_reference_corpus_sha256="b" * 64)
        assert verify_mod.check_locus_reference_basis(conn, flipped), (
            "flipping one basis hash MUST fail the Contract-0 gate"
        )
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# The amendment-contract check, proven able to fail
# ---------------------------------------------------------------------------

def test_amendment_contract_check_full_matrix(tmp_path):
    db = tmp_path / "cd.db"
    _build_synthetic(db)
    conn = sqlite3.connect(str(db))
    try:
        meta = _meta(conn)
        assert verify_mod.check_amendment_2026_08_12_contract(conn, meta) == []
        # Marker absent -> violation.
        no_marker = {k: v for k, v in meta.items() if k != "locus_schema_version"}
        assert any(
            "locus_schema_version" in v
            for v in verify_mod.check_amendment_2026_08_12_contract(conn, no_marker)
        )
        # A count that stopped matching -> violation.
        bad_count = dict(meta, expected_rows_locus_work="7")
        assert any(
            "expected_rows_locus_work" in v
            for v in verify_mod.check_amendment_2026_08_12_contract(conn, bad_count)
        )
    finally:
        conn.close()


def test_amendment_contract_check_has_vocabulary_teeth():
    # The DDL CHECK blocks a bad INSERT on a real asset, so hand-craft a
    # CHECK-free artifact -- the verifier must catch it INDEPENDENTLY of the
    # constraint (a verifier that relies on the writer's own CHECK is not an
    # independent verifier).
    conn = sqlite3.connect(":memory:")
    try:
        conn.execute(
            "CREATE TABLE discovery_identification "
            "(identification_id TEXT, routing_reason TEXT, rendered_relation TEXT)"
        )
        conn.execute(
            "INSERT INTO discovery_identification VALUES ('i1', 'none', 'definitely')"
        )
        violations = verify_mod.check_amendment_2026_08_12_contract(
            conn, {"locus_schema_version": "locus-v1"}
        )
        assert any("rendered_relation" in v and "definitely" in v for v in violations)
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Excerpt sidecar (_tmp/PLAN-textvtext-excerpts.md, Track C) -- the
# verifier's `check_excerpt_contract` / `check_excerpt_vocabulary`, proven
# able to fail. Genuinely marker-conditional (unlike the CD batch's own
# `check_amendment_2026_08_12_contract` above, whose marker-absence is now
# itself a violation): the excerpt table ships on its own bake schedule, so
# an asset without the marker must verify clean regardless of shape.
# ---------------------------------------------------------------------------

_EXCERPT_COLUMNS = tuple(verify_mod._EXCERPT_COLUMNS)
_GOOD_EXCERPT_ROW = (
    "i1", "ev1", "p1", "before", "span text", "after", 0,
    "wbefore", "wspan", "wafter", 0, "direct", 0.9, "attribution text", 1, "htr",
    "[[0, 4]]", "[[0, 5]]", None,
)


def _excerpt_conn_with_table(omit_columns=()):
    conn = sqlite3.connect(":memory:")
    cols = [c for c in _EXCERPT_COLUMNS if c not in omit_columns]
    conn.execute(f"CREATE TABLE discovery_excerpt ({', '.join(cols)})")
    return conn


def _valid_excerpt_conn():
    conn = _excerpt_conn_with_table()
    conn.execute(f"INSERT INTO discovery_excerpt VALUES ({', '.join('?' * len(_EXCERPT_COLUMNS))})",
                 _GOOD_EXCERPT_ROW)
    return conn


def _excerpt_meta(**overrides):
    meta = {
        verify_mod._EXCERPT_SCHEMA_MARKER_KEY: verify_mod._EXPECTED_EXCERPT_SCHEMA_VERSION,
        "expected_rows_discovery_excerpt": "1",
        "excerpt_ctx": "90",
        "excerpt_span_cap": "600",
        "excerpt_refs_manifest_sha256": "a" * 64,
    }
    meta.update(overrides)
    return meta


def test_excerpt_marker_and_column_literals_cannot_drift_between_loader_and_verifier():
    """The verifier deliberately does NOT import the loader's constants (its
    standing independence convention), so the two literal sets are pinned
    against each other here instead."""
    assert verify_mod._EXCERPT_SCHEMA_MARKER_KEY == da._EXCERPT_SCHEMA_MARKER_KEY
    assert verify_mod._EXPECTED_EXCERPT_SCHEMA_VERSION == da._EXPECTED_EXCERPT_SCHEMA_VERSION
    assert set(verify_mod._EXCERPT_COLUMNS) == da._AMENDMENT_EXCERPT_COLUMNS["discovery_excerpt"]
    assert set(verify_mod._EXCERPT_META_KEYS) == (
        da._AMENDMENT_EXCERPT_META_KEYS
        | {meta_key for meta_key, _table in da._AMENDMENT_EXCERPT_COUNTS}
    )


def test_excerpt_contract_check_marker_absent_is_a_noop_even_on_a_malformed_shape():
    """Backward compat at the CHECK-FUNCTION level: with no marker in meta,
    a table missing a required column and carrying a nonsense row count must
    still verify clean -- proof the gate is genuinely marker-conditional
    rather than just happening to pass on a well-formed table."""
    conn = _excerpt_conn_with_table(omit_columns=["frag_span"])
    try:
        assert verify_mod.check_excerpt_contract(conn, {}) == []
        assert verify_mod.check_excerpt_contract(
            conn, {"expected_rows_discovery_excerpt": "999"}
        ) == []
    finally:
        conn.close()


def test_excerpt_contract_check_full_matrix():
    conn = _valid_excerpt_conn()
    try:
        meta = _excerpt_meta()
        assert verify_mod.check_excerpt_contract(conn, meta) == []

        # Marker present but the wrong value -> reject-incompatible.
        bad_value = dict(meta, **{verify_mod._EXCERPT_SCHEMA_MARKER_KEY: "excerpt-v0-wrong"})
        assert any(
            "excerpt_schema_version" in v
            for v in verify_mod.check_excerpt_contract(conn, bad_value)
        )

        # A count that stopped matching -> violation.
        bad_count = dict(meta, expected_rows_discovery_excerpt="999")
        assert any(
            "expected_rows_discovery_excerpt" in v
            for v in verify_mod.check_excerpt_contract(conn, bad_count)
        )

        # Each of the four excerpt meta keys is independently required once
        # the marker is present.
        for key in (
            "expected_rows_discovery_excerpt", "excerpt_ctx",
            "excerpt_span_cap", "excerpt_refs_manifest_sha256",
        ):
            missing = {k: v for k, v in meta.items() if k != key}
            violations = verify_mod.check_excerpt_contract(conn, missing)
            assert any(key in v for v in violations), (key, violations)
    finally:
        conn.close()


def test_excerpt_contract_check_table_missing_when_marker_present():
    conn = sqlite3.connect(":memory:")
    try:
        violations = verify_mod.check_excerpt_contract(
            conn, _excerpt_meta(expected_rows_discovery_excerpt="0")
        )
        assert any("table absent" in v for v in violations)
    finally:
        conn.close()


def test_excerpt_contract_check_column_missing_when_marker_present():
    conn = _excerpt_conn_with_table(omit_columns=["frag_span"])
    try:
        violations = verify_mod.check_excerpt_contract(
            conn, _excerpt_meta(expected_rows_discovery_excerpt="0")
        )
        assert any("frag_span" in v and "column absent" in v for v in violations)
    finally:
        conn.close()


def test_excerpt_vocabulary_check_is_a_noop_when_table_absent():
    conn = sqlite3.connect(":memory:")
    try:
        assert verify_mod.check_excerpt_vocabulary(conn) == []
    finally:
        conn.close()


def test_excerpt_vocabulary_check_catches_bad_work_source_and_blank_span():
    conn = _valid_excerpt_conn()
    try:
        assert verify_mod.check_excerpt_vocabulary(conn) == []

        conn.execute("UPDATE discovery_excerpt SET work_source = 'bogus_source'")
        assert any(
            "work_source" in v for v in verify_mod.check_excerpt_vocabulary(conn)
        )

        conn.execute("UPDATE discovery_excerpt SET work_source = 'direct', frag_span = ''")
        assert any(
            "frag_span" in v for v in verify_mod.check_excerpt_vocabulary(conn)
        )

        # NULL work_source is the legitimate "no work pane" case and must
        # NEVER be flagged -- a positive control against an over-strict gate.
        conn.execute("UPDATE discovery_excerpt SET work_source = NULL, frag_span = 'ok'")
        assert verify_mod.check_excerpt_vocabulary(conn) == []
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# (T) The CERT-01 frame-regression gate
# ---------------------------------------------------------------------------

def test_frame_regression_clean_on_identical_assets(tmp_path):
    before = tmp_path / "before.db"
    _build_synthetic(before)
    after = tmp_path / "after.db"
    shutil.copyfile(before, after)
    assert frame_regression.check_frame_regression(str(before), str(after)) == []


def test_frame_moves_on_nothing_the_amendment_adds(tmp_path):
    """THE stated invariant: locus rows, region rows, curated rows,
    withholding scopes, stratum membership, and the two new identification
    columns are all OUTSIDE the frame's tuple set -- populating every one of
    them must leave the frame IDENTICAL. This is what makes 'withholding never
    mutates CERT-01 membership' a checked fact rather than a sentence."""
    before = tmp_path / "before.db"
    _build_synthetic(before)
    after = tmp_path / "after.db"
    shutil.copyfile(before, after)

    _add_locus_rows(after)
    conn = sqlite3.connect(str(after))
    try:
        (work_id,) = conn.execute("SELECT work_id FROM locus_work LIMIT 1").fetchone()
        (canonical,) = conn.execute(
            "SELECT canonical_work_id FROM works WHERE work_id = ?", (work_id,)
        ).fetchone()
        (ident_id,) = conn.execute(
            "SELECT identification_id FROM discovery_identification LIMIT 1"
        ).fetchone()
        conn.execute(
            "INSERT INTO discovery_region_map VALUES ('r1', ?, 0, 0, 'ruling', 'test')",
            (work_id,),
        )
        conn.execute(
            "INSERT INTO discovery_curated_quoter VALUES ('q1', ?, '2026-08-12', NULL)",
            (canonical,),
        )
        # Amendment 2026-08-13 (V): the band table joins the same invariant. A
        # region band changes what a surface CLAIMS about a row, never which
        # rows exist, so it must be outside the frame's tuple set exactly as the
        # region map is.
        conn.execute(
            "INSERT INTO discovery_region_band VALUES ('b1', ?, 0, 100, 0, 'ruling', 'test')",
            (work_id,),
        )
        conn.execute(
            "INSERT INTO discovery_withholding VALUES "
            "('w1', 's1', '{\"main_pool\": 1}', NULL, NULL, 'test', '2026-08-12')"
        )
        conn.execute(
            "INSERT INTO discovery_stratum_membership VALUES ('f1', 's1', ?)",
            (ident_id,),
        )
        conn.execute(
            "UPDATE discovery_identification SET rendered_relation = 'shared_text' "
            "WHERE identification_id = ?",
            (ident_id,),
        )
        conn.commit()
    finally:
        conn.close()

    assert frame_regression.check_frame_regression(str(before), str(after)) == []


def test_frame_regression_gate_proven_able_to_fail(tmp_path):
    before = tmp_path / "before.db"
    _build_synthetic(before)
    after = tmp_path / "after.db"
    shutil.copyfile(before, after)
    conn = sqlite3.connect(str(after))
    try:
        conn.execute(
            "UPDATE discovery_evidence SET confidence_band = 'weak' "
            "WHERE evidence_id = (SELECT evidence_id FROM discovery_evidence "
            "WHERE evidence_source = 'propagated' LIMIT 1)"
        )
        conn.commit()
    finally:
        conn.close()
    violations = frame_regression.check_frame_regression(str(before), str(after))
    assert violations, "a flipped confidence band MUST fail the frame gate"
    assert any("frame_content_hash moved" in v for v in violations)


# ---------------------------------------------------------------------------
# (U) The loader's marker-conditional contract
# ---------------------------------------------------------------------------

def _load_from(tmp_path, monkeypatch) -> bool:
    monkeypatch.setattr(da, "DISCOVERY_DATA_DIR", str(tmp_path))
    return da.load_discovery_state()


def _restore_loader_state():
    # The monkeypatched dir is undone by pytest; re-load so module state never
    # leaks a tmp asset into later tests (mirrors test_discovery_loader.py).
    da.load_discovery_state()


def test_loader_accepts_the_full_cd_batch_shape(tmp_path, monkeypatch):
    try:
        fixture_mod.materialize_cd_batch_sidecar(tmp_path)
        assert _load_from(tmp_path, monkeypatch) is True
    finally:
        monkeypatch.undo()
        _restore_loader_state()


def test_loader_refuses_marker_present_but_table_missing(tmp_path, monkeypatch):
    try:
        fixture_mod.materialize_cd_batch_sidecar(tmp_path, omit_tables=["locus_unit"])
        assert _load_from(tmp_path, monkeypatch) is False
    finally:
        monkeypatch.undo()
        _restore_loader_state()


#: Omitting `rendered_relation` now takes BOTH knobs. Since C-track step 3c the
#: column is created at the POST-REBUILD step (the loader requires it
#: unconditionally, so the shape the fixture calls "what the loader accepts"
#: carries it), and it is added again at the CD-batch step. Asking only the
#: second knob leaves the column present and turns a refusal test green for the
#: wrong reason.
_OMIT_RENDERED_RELATION = {
    "omit_columns": [("discovery_identification", "rendered_relation")],
    "post_rebuild_kwargs": {
        "omit_columns": [("discovery_identification", "rendered_relation")]},
}


def test_loader_refuses_marker_present_but_column_missing(tmp_path, monkeypatch):
    try:
        fixture_mod.materialize_cd_batch_sidecar(tmp_path, **_OMIT_RENDERED_RELATION)
        assert _load_from(tmp_path, monkeypatch) is False
    finally:
        monkeypatch.undo()
        _restore_loader_state()


def test_the_batch_stays_marker_conditional_except_for_the_column_surfaces_read(
        tmp_path, monkeypatch):
    """(U)'s rollback half, AMENDED 2026-08-12 by C-track step 3c.

    As written, this asserted that a pre-batch asset keeps loading -- no marker,
    none of the batch required -- so the deployed asset would survive new code.
    That property stopped being reachable the moment three read paths began
    SELECTing `discovery_identification.rendered_relation`: such an asset loads,
    reports itself available, and then answers every claims / manuscript-pane /
    findings read with `unavailable / query_failed`. Measured on the served
    pre-batch asset, not inferred.

    So the column is now unconditionally required and a pre-batch asset is
    REFUSED -- discovery hides cleanly instead of erroring on first query, which
    is the loader's stated job. Everything else in the batch stays
    marker-conditional, which is the half of (U) that still holds: the second
    case below omits the marker, every batch TABLE and the count meta keys, and
    still loads.
    """
    try:
        fixture_mod.materialize_cd_batch_sidecar(tmp_path, omit_marker=True,
                                                 omit_tables=list(
                                                     fixture_mod.CD_BATCH_TABLE_COLUMNS),
                                                 omit_columns=[
                                                     ("discovery_identification", "routing_reason"),
                                                     ("discovery_identification", "rendered_relation"),
                                                 ],
                                                 post_rebuild_kwargs=_OMIT_RENDERED_RELATION[
                                                     "post_rebuild_kwargs"],
                                                 omit_meta_keys=list(
                                                     fixture_mod.CD_BATCH_COUNT_META_KEY_BY_TABLE.values()))
        assert _load_from(tmp_path, monkeypatch) is False, (
            "an asset without `rendered_relation` must not reach a read path "
            "that selects it")
    finally:
        monkeypatch.undo()
        _restore_loader_state()

    try:
        fixture_mod.materialize_cd_batch_sidecar(tmp_path, omit_marker=True,
                                                 omit_tables=list(
                                                     fixture_mod.CD_BATCH_TABLE_COLUMNS),
                                                 omit_columns=[
                                                     ("discovery_identification", "routing_reason"),
                                                 ],
                                                 omit_meta_keys=list(
                                                     fixture_mod.CD_BATCH_COUNT_META_KEY_BY_TABLE.values()))
        assert _load_from(tmp_path, monkeypatch) is True, (
            "the locus tables, the count meta keys and `routing_reason` are "
            "still marker-conditional -- no read path selects any of them")
    finally:
        monkeypatch.undo()
        _restore_loader_state()


# ---------------------------------------------------------------------------
# Excerpt sidecar (_tmp/PLAN-textvtext-excerpts.md, Track C) -- the loader's
# OWN marker-conditional contract, independent of `locus_schema_version`
# above. Layered on top of an already-materialized CD-batch sidecar (the
# realistic base: the currently-deployed candidate already carries the CD
# batch), same defect-knob / build-UP convention as
# `fixture_mod.upgrade_db_to_cd_batch`.
# ---------------------------------------------------------------------------

_EXCERPT_MARKER_KEY = da._EXCERPT_SCHEMA_MARKER_KEY
_EXCERPT_MARKER_VALUE = da._EXPECTED_EXCERPT_SCHEMA_VERSION
_EXCERPT_ROW_VALUES = {
    "identification_id": "synthetic-ident-1",
    "evidence_id": "synthetic-ev-1",
    "a_page_id": "synthetic-page-1",
    "frag_before": "before",
    "frag_span": "span text",
    "frag_after": "after",
    "frag_clipped": 0,
    "work_before": "wbefore",
    "work_span": "wspan",
    "work_after": "wafter",
    "work_clipped": 0,
    "work_source": "direct",
    "align_score": None,
    "attribution": "attribution text",
    "n_spans": 1,
    "text_layer": "htr",
    "frag_hl": "[[0, 4]]",
    "work_hl": "[[0, 5]]",
    "work_markup": None,
}


def _add_excerpt_layer(
    db,
    *,
    omit_marker=False,
    marker_value=_EXCERPT_MARKER_VALUE,
    omit_table=False,
    omit_columns=(),
    meta_overrides=None,
    omit_meta_keys=(),
):
    """Layer the excerpt marker/table/meta on top of an already-materialized
    CD-batch sidecar, IN PLACE. Every defect knob BUILDS UP from a valid
    shape -- never tears one down -- mirroring
    ``fixture_mod.upgrade_db_to_cd_batch``'s own convention."""
    conn = sqlite3.connect(str(db))
    try:
        conn.execute("PRAGMA foreign_keys = OFF")
        if not omit_table:
            present_cols = [c for c in _EXCERPT_COLUMNS if c not in omit_columns]
            conn.execute(f"CREATE TABLE discovery_excerpt ({', '.join(present_cols)})")
            values = [_EXCERPT_ROW_VALUES[c] for c in present_cols]
            placeholders = ", ".join("?" for _ in present_cols)
            conn.execute(
                f"INSERT INTO discovery_excerpt ({', '.join(present_cols)}) "
                f"VALUES ({placeholders})",
                values,
            )

        meta = {}
        if not omit_marker:
            meta[_EXCERPT_MARKER_KEY] = marker_value
        if not omit_table:
            (count,) = conn.execute("SELECT COUNT(*) FROM discovery_excerpt").fetchone()
            meta["expected_rows_discovery_excerpt"] = str(count)
        else:
            # Same convention as upgrade_db_to_cd_batch: still write the
            # count key so a missing-TABLE test fails on the table check,
            # never vacuously on the missing meta key.
            meta["expected_rows_discovery_excerpt"] = "0"
        meta["excerpt_ctx"] = "90"
        meta["excerpt_span_cap"] = "600"
        meta["excerpt_refs_manifest_sha256"] = "a" * 64
        if meta_overrides:
            meta.update({k: str(v) for k, v in meta_overrides.items()})
        for key in omit_meta_keys:
            meta.pop(key, None)

        for key, value in meta.items():
            conn.execute(
                "INSERT INTO meta (key, value) VALUES (?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                (key, value),
            )
        for key in omit_meta_keys:
            conn.execute("DELETE FROM meta WHERE key = ?", (key,))
        conn.commit()
    finally:
        conn.close()


def _load_excerpt_case(tmp_path, monkeypatch, **excerpt_kwargs):
    db = fixture_mod.materialize_cd_batch_sidecar(tmp_path)
    _add_excerpt_layer(db, **excerpt_kwargs)
    # Manifest content_hash must reflect the bytes AFTER the excerpt layer,
    # or the loader's hash check refuses the asset before the excerpt
    # contract ever runs -- masking the defect under test.
    fixture_mod.write_manifest(tmp_path, db)
    return _load_from(tmp_path, monkeypatch)


def test_loader_ignores_excerpt_amendment_when_marker_and_table_are_both_absent(
        tmp_path, monkeypatch):
    """Backward compat: an asset that predates the excerpt bake (or the
    currently-deployed candidate, verified before Track B runs) keeps
    loading -- the excerpt table is not required when its marker is absent."""
    try:
        fixture_mod.materialize_cd_batch_sidecar(tmp_path)
        assert _load_from(tmp_path, monkeypatch) is True
    finally:
        monkeypatch.undo()
        _restore_loader_state()


def test_loader_accepts_the_full_excerpt_shape(tmp_path, monkeypatch):
    try:
        assert _load_excerpt_case(tmp_path, monkeypatch) is True
    finally:
        monkeypatch.undo()
        _restore_loader_state()


def test_loader_refuses_excerpt_marker_with_wrong_value(tmp_path, monkeypatch):
    try:
        assert _load_excerpt_case(
            tmp_path, monkeypatch, marker_value="excerpt-v0-wrong"
        ) is False
    finally:
        monkeypatch.undo()
        _restore_loader_state()


def test_loader_refuses_excerpt_marker_present_but_table_missing(tmp_path, monkeypatch):
    try:
        assert _load_excerpt_case(tmp_path, monkeypatch, omit_table=True) is False
    finally:
        monkeypatch.undo()
        _restore_loader_state()


def test_loader_refuses_excerpt_marker_present_but_count_mismatch(tmp_path, monkeypatch):
    try:
        assert _load_excerpt_case(
            tmp_path, monkeypatch,
            meta_overrides={"expected_rows_discovery_excerpt": "999"},
        ) is False
    finally:
        monkeypatch.undo()
        _restore_loader_state()


def test_loader_refuses_excerpt_marker_present_but_column_missing(tmp_path, monkeypatch):
    try:
        assert _load_excerpt_case(
            tmp_path, monkeypatch, omit_columns=["frag_span"]
        ) is False
    finally:
        monkeypatch.undo()
        _restore_loader_state()


def test_loader_refuses_excerpt_marker_present_but_expected_rows_key_missing(
        tmp_path, monkeypatch):
    try:
        assert _load_excerpt_case(
            tmp_path, monkeypatch, omit_meta_keys=["expected_rows_discovery_excerpt"]
        ) is False
    finally:
        monkeypatch.undo()
        _restore_loader_state()


def test_loader_refuses_excerpt_marker_present_but_ctx_key_missing(tmp_path, monkeypatch):
    try:
        assert _load_excerpt_case(
            tmp_path, monkeypatch, omit_meta_keys=["excerpt_ctx"]
        ) is False
    finally:
        monkeypatch.undo()
        _restore_loader_state()


def test_loader_refuses_excerpt_marker_present_but_span_cap_key_missing(tmp_path, monkeypatch):
    try:
        assert _load_excerpt_case(
            tmp_path, monkeypatch, omit_meta_keys=["excerpt_span_cap"]
        ) is False
    finally:
        monkeypatch.undo()
        _restore_loader_state()


def test_loader_refuses_excerpt_marker_present_but_manifest_sha_key_missing(
        tmp_path, monkeypatch):
    try:
        assert _load_excerpt_case(
            tmp_path, monkeypatch, omit_meta_keys=["excerpt_refs_manifest_sha256"]
        ) is False
    finally:
        monkeypatch.undo()
        _restore_loader_state()


# ---------------------------------------------------------------------------
# (R) The Contract-1 input ingests — curated quoter + region map
# ---------------------------------------------------------------------------

TRACKED_CURATED = REPO_ROOT / "docs" / "specs" / "discovery-curated-quoter-v1.json"


def test_tracked_curated_list_is_well_formed():
    import json as _json
    curated = _json.loads(TRACKED_CURATED.read_text(encoding="utf-8"))
    assert curated["list_version"] == "quoter-v1"
    assert curated["ruled_date"] == "2026-08-12"
    ids_in_list = [e["canonical_work_id"] for e in curated["entries"]]
    assert ids_in_list == ["w001383", "w001384"]  # both Yalkut works, owner ruling


def _write_json(path, payload):
    import json as _json
    path.write_text(_json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def test_curated_ingest_inserts_and_fails_closed_on_unknown_id(tmp_path):
    db = tmp_path / "cd.db"
    _build_synthetic(db)
    conn = sqlite3.connect(str(db))
    try:
        (canonical,) = conn.execute(
            "SELECT canonical_work_id FROM works LIMIT 1").fetchone()
        good = tmp_path / "curated.json"
        _write_json(good, {
            "list_version": "quoter-test", "ruled_date": "2026-08-12",
            "entries": [{"canonical_work_id": canonical, "note": "t"}],
        })
        meta_rows = sidecar_build.ingest_curated_quoter(conn, str(good))
        assert meta_rows == [("curated_quoter_version", "quoter-test")]
        row = conn.execute(
            "SELECT list_version, canonical_work_id, ruled_date, note "
            "FROM discovery_curated_quoter").fetchone()
        assert row == ("quoter-test", canonical, "2026-08-12", "t")

        bad = tmp_path / "bad.json"
        _write_json(bad, {
            "list_version": "quoter-test2", "ruled_date": "2026-08-12",
            "entries": [{"canonical_work_id": "w_nope"}],
        })
        try:
            sidecar_build.ingest_curated_quoter(conn, str(bad))
        except ValueError as exc:
            assert "w_nope" in str(exc)
        else:
            raise AssertionError("unknown canonical id MUST fail the build")
    finally:
        conn.close()


def test_region_ingest_requires_locus_and_resolves_every_row(tmp_path):
    db = tmp_path / "cd.db"
    _build_synthetic(db)
    region = tmp_path / "region.json"
    _write_json(region, {"frame": "region-test", "rows": [
        {"locus_ref_id": "REF:x", "unit_ord": 0, "discriminative": False,
         "source": "derived", "basis": "b"},
        {"locus_ref_id": "REF:x", "unit_ord": 1, "discriminative": None,
         "source": "open"},
    ]})

    conn = sqlite3.connect(str(db))
    try:
        # Precondition: empty locus_unit fails closed with the D-track message.
        try:
            sidecar_build.ingest_region_map(conn, str(region), {"REF:x": "w1"})
        except ValueError as exc:
            assert "locus_unit is EMPTY" in str(exc)
        else:
            raise AssertionError("region ingest before the locus import MUST fail")
    finally:
        conn.close()

    _add_locus_rows(db)  # one locus_work + unit_ord 0 for a real work
    conn = sqlite3.connect(str(db))
    try:
        (work_id,) = conn.execute("SELECT work_id FROM locus_work").fetchone()
        conn.execute(
            "INSERT INTO locus_unit (work_id, unit_ord, start_offset, part_key, "
            "label_he, citation_pos) VALUES (?, 1, 500, 'ch:0.2', 'פרק ב', 2)",
            (work_id,),
        )
        crosswalk = {"REF:x": work_id}
        meta_rows = sidecar_build.ingest_region_map(conn, str(region), crosswalk)
        assert meta_rows == [("region_map_version", "region-test")]
        stored = conn.execute(
            "SELECT region_version, work_id, unit_ord, discriminative, source, basis "
            "FROM discovery_region_map ORDER BY unit_ord").fetchall()
        # The tri-state survives: False -> 0, open/None -> NULL (fails closed).
        assert stored == [
            ("region-test", work_id, 0, 0, "derived", "b"),
            ("region-test", work_id, 1, None, "open", None),
        ]

        # An unanchored ruling (unit nobody has) fails closed.
        bad = tmp_path / "bad_region.json"
        _write_json(bad, {"frame": "region-test2", "rows": [
            {"locus_ref_id": "REF:x", "unit_ord": 99, "discriminative": True,
             "source": "ruling"},
        ]})
        try:
            sidecar_build.ingest_region_map(conn, str(bad), crosswalk)
        except ValueError as exc:
            assert "unanchored" in str(exc)
        else:
            raise AssertionError("a ruling on a nonexistent unit MUST fail")

        # An unresolvable ref id fails closed, and the message withholds it.
        orphan = tmp_path / "orphan_region.json"
        _write_json(orphan, {"frame": "region-test3", "rows": [
            {"locus_ref_id": "REF:unknown", "unit_ord": 0,
             "discriminative": True, "source": "ruling"},
        ]})
        try:
            sidecar_build.ingest_region_map(conn, str(orphan), crosswalk)
        except ValueError as exc:
            assert "no crosswalk entry" in str(exc)
            assert "REF:unknown" not in str(exc)  # raw ref ids never reach messages
        else:
            raise AssertionError("an unresolvable ref id MUST fail")
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Projection carries the batch through
# ---------------------------------------------------------------------------

def test_projection_registers_and_counts_the_new_tables(tmp_path):
    import project_discovery_public as proj

    private = tmp_path / "private.db"
    _build_synthetic(private)
    public = tmp_path / "public.db"
    proj.project(str(private), str(public),
                 masking_patterns=["ZZ-CD-BATCH-DISPOSABLE-MARKER-ZZ"])
    conn = sqlite3.connect(str(public))
    try:
        tables = {
            r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
        meta = _meta(conn)
        for table in sidecar_build.AMENDMENT_2026_08_12_COUNT_TABLES:
            assert table in tables
            (actual,) = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
            assert meta.get(f"expected_rows_{table}") == str(actual)
        assert meta.get("locus_schema_version") == sidecar_build.LOCUS_SCHEMA_VERSION
        # The public rematerialization writes the batch columns too.
        (n_null,) = conn.execute(
            "SELECT COUNT(*) FROM discovery_identification "
            "WHERE rendered_relation IS NULL OR routing_reason IS NULL"
        ).fetchone()
        assert n_null == 0
    finally:
        conn.close()
