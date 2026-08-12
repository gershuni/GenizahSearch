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
