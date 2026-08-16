"""Tests for the V4.2 plan C1 (run identity) and C3 (contract v2 emitter)
conditions implemented in ``scripts/discovery_v4_match.py``.

All fixtures are synthetic SQLite databases and hand-written JSON registries /
source maps built under ``tmp_path``. No network access, no real research DB,
and no dependency on the external ``rsource`` pilot/calibration files (those
are pinned by fixed SHA-256 constants that only exist in the operator's real
build environment, so the CLI-level ``run``/``preflight`` entry points are
intentionally not exercised here -- every gate under test is a standalone,
directly-callable function extracted for exactly this reason).
"""

from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path

import pytest

import scripts.discovery_v4_match as v4match
from scripts.discovery_track1_contract import (
    CONTRACT_V2_SCHEMA_VERSION,
    load_cohort_registry,
    validate_contract_v2,
)


# --------------------------------------------------------------------------
# Shared fixtures
# --------------------------------------------------------------------------


def _report(**overrides) -> dict:
    report = {
        "reference_sha256": "a" * 64,
        "masks_sha256": "b" * 64,
        "source_db_seed_sha256": "c" * 64,
        "pilot_sha256": "d" * 64,
        "calibration_sha256": "e" * 64,
        "page_count": 667_411,
    }
    report.update(overrides)
    return report


def _status(**overrides) -> dict:
    status = {
        "run_id": "0" * 64,
        "reference_sha256": "a" * 64,
        "masks_sha256": "b" * 64,
        "source_db_seed_sha256": "c" * 64,
        "pilot_sha256": "d" * 64,
        "calibration_sha256": "e" * 64,
        "fingerprint": "f" * 40,
        "page_count": 667_411,
        "page_batch": 2_000,
        "expected_batches": 334,
        "missing_ref_offsets": 0,
        "duplicate_pairs": 0,
    }
    status.update(overrides)
    return status


def _empty_db(tmp_path: Path, name: str = "research.sqlite3") -> Path:
    db_path = tmp_path / name
    sqlite3.connect(db_path).close()
    return db_path


def _make_registry(tmp_path: Path, ref6_sources: list[dict]) -> Path:
    """A synthetic 4-cohort registry (REF2 legacy, REF4/REF5 private_sibling,
    REF6 per_entry) with real-but-empty REF4/REF5 stub source maps and a
    REF6 source map carrying the given ``sources`` list.
    """
    (tmp_path / "ref4_map.json").write_text(
        json.dumps({"schema_version": "discovery-v4-sources-v1", "sources": []}),
        encoding="utf-8",
    )
    (tmp_path / "ref5_map.json").write_text(
        json.dumps({"schema_version": "discovery-v4-sources-v1", "sources": []}),
        encoding="utf-8",
    )
    (tmp_path / "ref6_map.json").write_text(
        json.dumps(
            {"schema_version": "discovery-v4-sources-v1", "sources": ref6_sources}
        ),
        encoding="utf-8",
    )
    registry_path = tmp_path / "registry.json"
    registry_path.write_text(
        json.dumps(
            {
                "schema_version": "discovery-routing-cohorts-v1",
                "cohorts": [
                    {"namespace": "REF2", "cohort": "legacy"},
                    {
                        "namespace": "REF4",
                        "cohort": "extrapolated",
                        "identity_mode": "private_sibling",
                        "source_map": "ref4_map.json",
                    },
                    {
                        "namespace": "REF5",
                        "cohort": "extrapolated",
                        "identity_mode": "private_sibling",
                        "source_map": "ref5_map.json",
                    },
                    {
                        "namespace": "REF6",
                        "cohort": "extrapolated",
                        "identity_mode": "per_entry",
                        "source_map": "ref6_map.json",
                    },
                ],
            }
        ),
        encoding="utf-8",
    )
    return registry_path


_REF6_SOURCES = [
    {"key": "mt_intro", "provider": "sefaria", "mappings": [{"target_work_id": "w000174"}]},
    {"key": "wikisource_text", "identity_mode": "public_first"},
]


def _make_track1_matches_db(tmp_path: Path, rows: list[tuple[str, str | None]]) -> Path:
    db_path = tmp_path / "matches.sqlite3"
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "CREATE TABLE track1_matches ("
            "page_id TEXT, sys_id TEXT, work_id TEXT, cat TEXT, genre TEXT, "
            "author TEXT, title TEXT, matched_letters INTEGER, best_density REAL, "
            "n_spans INTEGER, spans_json TEXT, generation TEXT, ref_spans_json TEXT, "
            "shadowed_by TEXT)"
        )
        for i, (work_id, shadowed_by) in enumerate(rows):
            conn.execute(
                "INSERT INTO track1_matches VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    f"p{i}", f"s{i}", work_id, "cat", "genre", "author", "title",
                    10, 0.5, 1, "[[0,1,0.5]]", "live", "[[0,1]]", shadowed_by,
                ),
            )
        conn.commit()
    return db_path


_BASE_ROWS = [
    ("REF4:a", None),
    ("REF4:b", None),
    ("REF6:mt_intro", None),
    ("REF6:mt_intro", "REF6:mt_intro"),
    ("REF6:wikisource_text", None),
]


# --------------------------------------------------------------------------
# Gate 1: run-id write-once (C1)
# --------------------------------------------------------------------------


def test_run_id_write_once_second_run_with_changed_fact_errors(tmp_path):
    db = _empty_db(tmp_path)
    run_id1 = v4match.compute_run_id(_report(), page_batch=2_000, tag="t1")
    v4match.pin_run_identity(str(db), "t1", run_id1)
    # Resuming with the identical inputs is a no-op, not a second write.
    v4match.pin_run_identity(str(db), "t1", run_id1)

    changed_report = _report(masks_sha256="f" * 64)  # ANY changed input fact
    run_id2 = v4match.compute_run_id(changed_report, page_batch=2_000, tag="t1")
    assert run_id2 != run_id1

    with pytest.raises(ValueError, match="run-identity mismatch") as excinfo:
        v4match.pin_run_identity(str(db), "t1", run_id2)
    message = str(excinfo.value)
    assert run_id1 in message
    assert run_id2 in message


def test_run_id_write_once_also_trips_on_page_batch_or_tag_change(tmp_path):
    db = _empty_db(tmp_path)
    run_id1 = v4match.compute_run_id(_report(), page_batch=2_000, tag="t1b")
    v4match.pin_run_identity(str(db), "t1b", run_id1)
    run_id_diff_batch = v4match.compute_run_id(_report(), page_batch=1_000, tag="t1b")
    assert run_id_diff_batch != run_id1
    with pytest.raises(ValueError, match="run-identity mismatch"):
        v4match.pin_run_identity(str(db), "t1b", run_id_diff_batch)


# --------------------------------------------------------------------------
# Gate 2: status/promote verification against the stored id (C1)
# --------------------------------------------------------------------------


def test_verify_run_identity_requires_a_recorded_id_and_then_matches(tmp_path):
    db = _empty_db(tmp_path)
    run_id = v4match.compute_run_id(_report(), page_batch=2_000, tag="t2")
    # Nothing recorded yet -- a hard error, never a silent pass.
    with pytest.raises(ValueError, match="not recorded"):
        v4match.verify_run_identity(str(db), "t2", run_id)

    v4match.pin_run_identity(str(db), "t2", run_id)
    v4match.verify_run_identity(str(db), "t2", run_id)  # matches: no raise

    other_run_id = v4match.compute_run_id(
        _report(reference_sha256="9" * 64), page_batch=2_000, tag="t2"
    )
    with pytest.raises(ValueError, match="run-identity mismatch") as excinfo:
        v4match.verify_run_identity(str(db), "t2", other_run_id)
    message = str(excinfo.value)
    assert run_id in message
    assert other_run_id in message


# --------------------------------------------------------------------------
# Gate 3: batch ledger gap detection (C1)
# --------------------------------------------------------------------------


def test_batch_ledger_gap_detection_lists_missing_indices(tmp_path):
    db = _empty_db(tmp_path)
    v4match.record_batch_ledger(str(db), "t3", 1)  # fills batches 0, 1
    # Simulate an observed batch 3 with no batch 2 ever recorded (e.g. a
    # resumed run whose upstream high-water mark advanced past a gap).
    with sqlite3.connect(db) as conn:
        v4match._ensure_batch_ledger_table(conn)
        conn.execute(
            f"INSERT INTO {v4match.BATCH_LEDGER_TABLE} VALUES (?, ?, ?)",
            ("t3", v4match.GENERATION, 3),
        )
        conn.commit()

    assert v4match.missing_ledger_batches(str(db), "t3", expected_batches=4) == [2]
    # A fully covered range reports no gaps.
    v4match.record_batch_ledger(str(db), "t3", 3)
    assert v4match.missing_ledger_batches(str(db), "t3", expected_batches=4) == []


def test_batch_ledger_absent_table_reports_every_batch_missing(tmp_path):
    db = _empty_db(tmp_path)
    assert v4match.missing_ledger_batches(str(db), "unstarted", expected_batches=5) == [
        0, 1, 2, 3, 4,
    ]


def test_promote_refuses_when_ledger_has_a_gap(tmp_path, monkeypatch):
    db = _empty_db(tmp_path)
    with sqlite3.connect(db) as conn:
        v4match._ensure_batch_ledger_table(conn)
        conn.executemany(
            f"INSERT INTO {v4match.BATCH_LEDGER_TABLE} VALUES (?, ?, ?)",
            [("t4", v4match.GENERATION, i) for i in (0, 1, 3)],
        )
        conn.commit()

    canned_status = {
        "db": str(db),
        "table": "irrelevant_staged_table",
        "complete": True,
        "missing_ref_offsets": 0,
        "duplicate_pairs": 0,
        "expected_batches": 4,
        "row_count": 0,
    }
    monkeypatch.setattr(v4match, "inspect_stage", lambda args: canned_status)
    args = argparse.Namespace(
        tag="t4", cohort_registry="unused-because-we-fail-first", report=None, contract=None
    )
    with pytest.raises(RuntimeError, match=r"\[2\]"):
        v4match.promote(args)


# --------------------------------------------------------------------------
# Gate 4: generalized at-least-one-extrapolated-work gate (C4 amendment)
# --------------------------------------------------------------------------


def test_assert_has_extrapolated_reference_accepts_any_extrapolated_namespace():
    extrapolated = {"REF4", "REF5", "REF6"}
    v4match.assert_has_extrapolated_reference({"REF4": 1}, extrapolated)
    # REF6-only (no REF4 at all) must ALSO pass -- the whole point of the
    # generalization away from a REF4-specific check.
    v4match.assert_has_extrapolated_reference({"REF6": 5, "REF2": 100}, extrapolated)


def test_assert_has_extrapolated_reference_rejects_none_present():
    extrapolated = {"REF4", "REF5", "REF6"}
    with pytest.raises(ValueError, match="no works from any extrapolated namespace"):
        v4match.assert_has_extrapolated_reference({"REF2": 50}, extrapolated)
    with pytest.raises(ValueError, match="no works from any extrapolated namespace"):
        v4match.assert_has_extrapolated_reference({}, extrapolated)


# --------------------------------------------------------------------------
# Gate 5: contract v2 emission shape (C3)
# --------------------------------------------------------------------------


def test_release_contract_v2_shape_zero_namespace_and_ref6_split(tmp_path):
    registry_path = _make_registry(tmp_path, _REF6_SOURCES)
    registry = load_cohort_registry(registry_path)
    db_path = _make_track1_matches_db(tmp_path, _BASE_ROWS)

    with sqlite3.connect(db_path) as conn:
        contract = v4match.build_release_contract_v2(
            _status(),
            conn,
            total_rows=5,
            live_rows=4,
            snapshot_rows=381_341,
            registry=registry,
            registry_path=registry_path,
        )

    assert contract["schema_version"] == CONTRACT_V2_SCHEMA_VERSION
    assert contract["shadow_algorithm"] == "track1-shadow-v1/input-order:rowid"
    assert contract["namespaces"]["REF4"] == {"total_rows": 2, "live_rows": 2}
    # Explicit zero for a namespace with no promoted rows at all.
    assert contract["namespaces"]["REF5"] == {"total_rows": 0, "live_rows": 0}
    assert contract["namespaces"]["REF6"]["total_rows"] == 3
    assert contract["namespaces"]["REF6"]["live_rows"] == 2
    assert contract["namespaces"]["REF6"]["by_identity_mode"] == {
        "private_sibling": {"total_rows": 2, "live_rows": 1},
        "public_first": {"total_rows": 1, "live_rows": 1},
    }
    # Independently re-validated (the emitter already did this internally).
    validate_contract_v2(contract, expected_namespaces={"REF4", "REF5", "REF6"})


def test_release_contract_v2_self_validates_before_returning(tmp_path, monkeypatch):
    registry_path = _make_registry(tmp_path, _REF6_SOURCES)
    registry = load_cohort_registry(registry_path)
    db_path = _make_track1_matches_db(tmp_path, _BASE_ROWS)

    # Simulate an emitter bug that drops a registered extrapolated namespace
    # from its own namespaces object -- ``validate_contract_v2`` must be the
    # thing that catches this, not a silent, malformed v2 document on disk.
    monkeypatch.setattr(
        v4match,
        "namespace_counts_for_contract",
        lambda conn, registry, registry_path: {"REF4": {"total_rows": 1, "live_rows": 1}},
    )
    with sqlite3.connect(db_path) as conn:
        with pytest.raises(ValueError, match="missing="):
            v4match.build_release_contract_v2(
                _status(),
                conn,
                total_rows=1,
                live_rows=1,
                snapshot_rows=381_341,
                registry=registry,
                registry_path=registry_path,
            )


def test_ref6_raw_id_with_source_key_absent_from_map_is_a_hard_error(tmp_path):
    registry_path = _make_registry(tmp_path, _REF6_SOURCES)
    registry = load_cohort_registry(registry_path)
    rows = [*_BASE_ROWS, ("REF6:not_in_the_map", None)]
    db_path = _make_track1_matches_db(tmp_path, rows)

    with sqlite3.connect(db_path) as conn:
        with pytest.raises(ValueError, match="absent from the source map"):
            v4match.build_release_contract_v2(
                _status(),
                conn,
                total_rows=len(rows),
                live_rows=5,
                snapshot_rows=381_341,
                registry=registry,
                registry_path=registry_path,
            )


# --------------------------------------------------------------------------
# Gate 6: unknown-namespace propagation (C4/C12)
# --------------------------------------------------------------------------


def test_classify_reference_ids_propagates_unknown_ref_prefix(tmp_path):
    registry_path = _make_registry(tmp_path, _REF6_SOURCES)
    registry = load_cohort_registry(registry_path)
    with pytest.raises(ValueError, match="REF7"):
        v4match.classify_reference_ids(["REF7:mystery_work"], registry)
    # Known namespaces still classify normally alongside the failure case
    # being isolated to only the unregistered one.
    assert v4match.classify_reference_ids(["REF4:known"], registry) == {"REF4": 1}


def test_release_contract_v2_propagates_unknown_namespace_in_matches(tmp_path):
    registry_path = _make_registry(tmp_path, _REF6_SOURCES)
    registry = load_cohort_registry(registry_path)
    rows = [*_BASE_ROWS, ("REF9:mystery", None)]
    db_path = _make_track1_matches_db(tmp_path, rows)

    with sqlite3.connect(db_path) as conn:
        with pytest.raises(ValueError, match="REF9"):
            v4match.build_release_contract_v2(
                _status(),
                conn,
                total_rows=len(rows),
                live_rows=5,
                snapshot_rows=381_341,
                registry=registry,
                registry_path=registry_path,
            )
