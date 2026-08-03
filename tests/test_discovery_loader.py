# -*- coding: utf-8 -*-
"""Fail-closed matrix for ``web/discovery_assets.py::load_discovery_state()``
(Phase 134, plan 134-05, Task 2).

Mirrors ``tests/test_atlas_flag_gating.py``'s one-fixture-per-defect-mode
shape: a ``ready_sidecar``-style test proves the ready path against the
committed 134-03 golden fixture (``discovery-v1-fixture.db`` + ``manifest.json``),
then one test per defect mode independently builds/mutates a small DB +
manifest in ``tmp_path`` and proves ``ready=False`` with NO exception
escaping. Every test that mutates module state ends with a bare
``da.load_discovery_state()`` restore call (via the ``_restore_state``
autouse fixture below) so tests never leak state into each other -- copying
the atlas test file's own idiom.

Masking discipline: every value here is fabricated/synthetic (mirrors
``scripts/build_discovery_sidecar.py``'s own synthetic dataset convention);
nothing here touches real research data.
"""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

import web.discovery_assets as da
from scripts import build_discovery_sidecar as sidecar_build
from tests.fixtures.discovery_v2_fixture import (
    materialize_sidecar,
    upgrade_db_to_post_rebuild,
)

FIXTURE_DIR = Path(__file__).resolve().parent / "fixtures" / "discovery"
FIXTURE_DB = FIXTURE_DIR / "discovery-v1-fixture.db"
FIXTURE_MANIFEST = FIXTURE_DIR / "manifest.json"


@pytest.fixture(autouse=True)
def _restore_state():
    """Every test in this module ends with a bare load_discovery_state()
    restore call so module state never leaks across tests (atlas test idiom)."""
    yield
    da.load_discovery_state()


# ---------------------------------------------------------------------------
# Helpers -- build small, fully-controllable synthetic discovery.db files
# (never derived from real research data; mirrors
# scripts/build_discovery_sidecar.py's own synthetic-dataset convention).
# ---------------------------------------------------------------------------

def _write_manifest(dir_path: Path, asset_basename: str, db_path: Path,
                     schema_version: str = "discovery-v1",
                     content_hash: str | None = None) -> None:
    manifest = {
        "schema_version": schema_version,
        "asset_basename": asset_basename,
        "content_hash": content_hash if content_hash is not None else da._sha256_file(str(db_path)),
        "frame_content_hash": "irrelevant-for-loader-tests",
    }
    (dir_path / da.MANIFEST_FILENAME).write_text(json.dumps(manifest), encoding="utf-8")


def _build_minimal_db(db_path: Path, *, meta_overrides: dict | None = None,
                       omit_meta_keys=(), confidence_band_override: str | None = None) -> None:
    """A tiny, fully-valid discovery.db (1 work / 1 claim / 1 evidence row +
    all 7 required tables + a complete meta release contract), reusing the
    FROZEN DDL from scripts/build_discovery_sidecar.py::create_schema so this
    test file never duplicates the schema. Fabricated values only."""
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA foreign_keys = OFF")
    try:
        sidecar_build.create_schema(conn)
        conn.execute(
            "INSERT INTO works (work_id, canonical_work_id, neutral_title, author, genre, source_corpus) "
            "VALUES ('w000001', 'w000001', 'Synthetic Test Title', NULL, NULL, 'sefaria')"
        )
        conn.execute(
            "INSERT INTO discovery_claim "
            "(page_id, work_id, claim_id, claim_type, display_evidence_id, source_corpus, sidecar_version) "
            "VALUES ('p001', 'w000001', 'claim1', 'direct_witness', 'ev1', 'sefaria', 'discovery-v1-test')"
        )
        confidence_band = confidence_band_override or "tier_a"
        conn.execute(
            "INSERT INTO discovery_evidence "
            "(evidence_id, claim_id, evidence_kind, evidence_source, confidence_band, "
            " adjudication_status, audit_status, routing_status, routing_reason, is_new, "
            " a_page_id, sys_id, span_start, span_end, text_layer, snapshot_hash) "
            "VALUES ('ev1', 'claim1', 'witness', 'track1_direct', ?, "
            " 'unreviewed', 'n/a', 'shipped', 'none', 0, "
            " 'p001', 's001', 0, 10, 'htr', 'hash1')",
            (confidence_band,),
        )
        conn.execute(
            "INSERT INTO band_precision "
            "(scope, collection_id, evidence_source, confidence_band, numerator, denominator, "
            " precision, ci_low, ci_high, method, sampling_frame, ins_policy, weighting, notes) "
            "VALUES ('collection', 'propagated_witness_collection_v1', NULL, NULL, "
            " 176, 190, 0.926, 0.875, 0.968, 'work-cluster bootstrap', 'frame', "
            " 'locked-rule evaluation', 'unweighted', 'note')"
        )
        (n_claims,) = conn.execute("SELECT COUNT(*) FROM discovery_claim").fetchone()
        (n_evidence,) = conn.execute("SELECT COUNT(*) FROM discovery_evidence").fetchone()
        (n_works,) = conn.execute("SELECT COUNT(*) FROM works").fetchone()
        (n_units,) = conn.execute("SELECT COUNT(*) FROM witness_units").fetchone()

        meta = {
            "schema_version": "discovery-v1",
            "sidecar_version": "discovery-v1-test",
            "source_db_sha256": "test",
            "build_date": "2026-07-22T00:00:00Z",
            "data_as_of": "2026-07-21",
            "htr_snapshot_hash": "test-hash",
            "expected_rows_claims": str(n_claims),
            "expected_rows_evidence": str(n_evidence),
            "expected_rows_works": str(n_works),
            "expected_rows_units": str(n_units),
            "frame_content_hash": "test-frame-hash",
        }
        if meta_overrides:
            meta.update(meta_overrides)
        for k in omit_meta_keys:
            meta.pop(k, None)
        conn.executemany("INSERT INTO meta (key, value) VALUES (?, ?)", list(meta.items()))
        conn.commit()
    finally:
        conn.close()

    # Phase 136, plan 136-20: the startup readiness contract now also requires
    # meta.audience, the two Amendment-2026-08-02 tables, their release-contract
    # counts and the amendment's new columns. Bring the minimal DB up to that
    # shape LAST so each defect test below still fails for the reason it names
    # rather than passing vacuously on a missing audience key.
    upgrade_db_to_post_rebuild(db_path)


def _drop_table(db_path: Path, table: str) -> None:
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA foreign_keys = OFF")
    conn.execute(f"DROP TABLE {table}")
    conn.commit()
    conn.close()


# ---------------------------------------------------------------------------
# Ready case -- the committed 134-03 golden fixture
# ---------------------------------------------------------------------------

def test_ready_sidecar_loads_and_is_available_with_flag_on(tmp_path, monkeypatch):
    # The golden fixture is a PRE-REBUILD asset; materialize_sidecar() upgrades
    # a copy of it to the Amendment 2026-08-02 shape the post-136-20 readiness
    # contract requires (plan 136-20). The pre-rebuild shape's own refusal is
    # asserted in tests/test_discovery_assets_audience.py.
    materialize_sidecar(tmp_path)
    monkeypatch.setattr(da, "DISCOVERY_DATA_DIR", str(tmp_path))

    assert da.load_discovery_state() is True

    monkeypatch.setattr(da, "DISCOVERY_ENABLED", True)
    assert da.discovery_available() is True
    assert da.discovery_db_path() is not None
    assert Path(da.discovery_db_path()).name == "discovery-v1-fixture.db"
    assert da.discovery_sidecar_version() == "discovery-v1-synthetic-fixture"
    assert da.discovery_meta("schema_version") == "discovery-v1"


def test_ready_sidecar_flag_off_hides_even_when_loaded(tmp_path, monkeypatch):
    materialize_sidecar(tmp_path)
    monkeypatch.setattr(da, "DISCOVERY_DATA_DIR", str(tmp_path))

    assert da.load_discovery_state() is True
    monkeypatch.setattr(da, "DISCOVERY_ENABLED", False)
    assert da.discovery_available() is False


# ---------------------------------------------------------------------------
# Defect matrix -- each independently proves ready=False, no exception
# ---------------------------------------------------------------------------

def test_named_file_absent(tmp_path, monkeypatch):
    manifest = {
        "schema_version": "discovery-v1",
        "asset_basename": "discovery-v1-nonexistent",
        "content_hash": "0" * 64,
        "frame_content_hash": "irrelevant",
    }
    (tmp_path / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    monkeypatch.setattr(da, "DISCOVERY_DATA_DIR", str(tmp_path))

    assert da.load_discovery_state() is False


def test_sibling_db_ignored_when_not_manifest_basename(tmp_path, monkeypatch):
    """A stale sibling *.db present under a DIFFERENT name than the manifest's
    asset_basename must never be picked up (rollback-safe, T-134-rollback)."""
    sibling_path = tmp_path / "stale-rollback-sibling.db"
    _build_minimal_db(sibling_path)
    manifest = {
        "schema_version": "discovery-v1",
        "asset_basename": "discovery-v1-current",  # does NOT match the sibling's stem
        "content_hash": "0" * 64,
        "frame_content_hash": "irrelevant",
    }
    (tmp_path / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    monkeypatch.setattr(da, "DISCOVERY_DATA_DIR", str(tmp_path))

    assert da.load_discovery_state() is False
    # Prove the sibling truly was never opened/considered: only the
    # manifest-named path is ever touched, so no partial ready state exists.
    assert da._state.ready is False
    assert da._state.path is None


def test_content_hash_mismatch(tmp_path, monkeypatch):
    db_path = tmp_path / "discovery-v1-current.db"
    _build_minimal_db(db_path)
    _write_manifest(tmp_path, "discovery-v1-current", db_path, content_hash="f" * 64)
    monkeypatch.setattr(da, "DISCOVERY_DATA_DIR", str(tmp_path))

    assert da.load_discovery_state() is False


def test_corrupt_integrity_fails_closed(tmp_path, monkeypatch):
    db_path = tmp_path / "discovery-v1-current.db"
    db_path.write_bytes(b"this is not a sqlite database at all -- corrupt bytes")
    _write_manifest(tmp_path, "discovery-v1-current", db_path)  # hash matches the corrupt bytes
    monkeypatch.setattr(da, "DISCOVERY_DATA_DIR", str(tmp_path))

    assert da.load_discovery_state() is False


def test_incompatible_schema_version_fails_closed(tmp_path, monkeypatch):
    db_path = tmp_path / "discovery-v1-current.db"
    _build_minimal_db(db_path, meta_overrides={"schema_version": "discovery-v0-old"})
    _write_manifest(tmp_path, "discovery-v1-current", db_path, schema_version="discovery-v0-old")
    monkeypatch.setattr(da, "DISCOVERY_DATA_DIR", str(tmp_path))

    assert da.load_discovery_state() is False


def test_missing_meta_key_fails_closed(tmp_path, monkeypatch):
    db_path = tmp_path / "discovery-v1-current.db"
    _build_minimal_db(db_path, omit_meta_keys=["htr_snapshot_hash"])
    _write_manifest(tmp_path, "discovery-v1-current", db_path)
    monkeypatch.setattr(da, "DISCOVERY_DATA_DIR", str(tmp_path))

    assert da.load_discovery_state() is False


def test_missing_table_fails_closed(tmp_path, monkeypatch):
    db_path = tmp_path / "discovery-v1-current.db"
    _build_minimal_db(db_path)
    _drop_table(db_path, "witness_unit_members")
    _write_manifest(tmp_path, "discovery-v1-current", db_path)  # hashed AFTER the drop
    monkeypatch.setattr(da, "DISCOVERY_DATA_DIR", str(tmp_path))

    assert da.load_discovery_state() is False


def test_invalid_confidence_band_vocab_fails_closed(tmp_path, monkeypatch):
    """confidence_band carries no DB-level CHECK constraint (it is validated
    against evidence_source's enum at the application level per the frozen
    schema doc), so this exercises the loader's OWN vocab spot-check without
    needing a constraint-bypass trick."""
    db_path = tmp_path / "discovery-v1-current.db"
    _build_minimal_db(db_path, confidence_band_override="not_a_real_band")
    _write_manifest(tmp_path, "discovery-v1-current", db_path)
    monkeypatch.setattr(da, "DISCOVERY_DATA_DIR", str(tmp_path))

    assert da.load_discovery_state() is False


def test_row_count_mismatch_fails_closed(tmp_path, monkeypatch):
    """Release-contract expected_rows_* meta must match actual table counts."""
    db_path = tmp_path / "discovery-v1-current.db"
    _build_minimal_db(db_path, meta_overrides={"expected_rows_claims": "999"})
    _write_manifest(tmp_path, "discovery-v1-current", db_path)
    monkeypatch.setattr(da, "DISCOVERY_DATA_DIR", str(tmp_path))

    assert da.load_discovery_state() is False


# ---------------------------------------------------------------------------
# App stays up -- no exception ever escapes load_discovery_state()
# ---------------------------------------------------------------------------

def test_every_defect_mode_never_raises(tmp_path, monkeypatch):
    """Belt-and-braces: even a totally empty/garbage directory never raises."""
    monkeypatch.setattr(da, "DISCOVERY_DATA_DIR", str(tmp_path))
    # No manifest.json at all.
    result = da.load_discovery_state()
    assert result is False
