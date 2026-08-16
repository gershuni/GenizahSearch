# -*- coding: utf-8 -*-
"""Tests for the V4.2 plan's C3 (release-contract v2, consumer side) and C4
(routing cohort registry) in scripts/build_discovery_sidecar.py.

C3: `load_track1_release_contract` accepts EITHER the frozen v1 document
(byte-identical behavior) OR a v2 document validated via the shared,
committed `discovery_track1_contract.validate_contract_v2`; a v2 contract's
recompute-and-compare (`assert_track1_release_contract_v2`) re-derives
PER-NAMESPACE counts from the research DB via `classify_work_id`.

C4: `finalize_build`'s tier-A routing classifies every row's work_id via the
reviewed cohort registry instead of the REF4-only special-casing it
replaces. This is the fix for an open P2 defect: before this change, only
`REF4:`-prefixed rows were excluded from the legacy split-grain regrain
population, so a REF5 (or REF6) row would reach the fitted gen-2 router's
regrain path -- which never scored it -- instead of the new-reference
extrapolation path built for exactly this population.

Every fixture here is synthetic (masking discipline, matching the rest of
this file's test suite): no real corpus data, no real work ids beyond the
tiny synthetic ones constructed below.
"""
from __future__ import annotations

import csv as _csv
import json
import sqlite3
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "scripts"))

import build_discovery_sidecar as bds  # noqa: E402
import discovery_ids as dids  # noqa: E402
import discovery_track1_contract as track1_contract  # noqa: E402

REAL_REGISTRY = bds._load_cohort_registry_for_build(None)


# ---------------------------------------------------------------------------
# Shared fixture helpers
# ---------------------------------------------------------------------------

def _write_json(path: Path, doc) -> Path:
    path.write_text(json.dumps(doc), encoding="utf-8")
    return path


def _v1_contract_doc(**overrides):
    doc = {
        "schema_version": "discovery-v4-track1-release-contract-v1",
        "reference_corpus_sha256": "a" * 64,
        "canonical_masks_sha256": "b" * 64,
        "source_db_seed_sha256": "d" * 64,
        "matcher_fingerprint": "c" * 40,
        "page_count": 667411,
        "total_rows": 400000,
        "live_rows": 290000,
        "ref4_total_rows": 20000,
        "ref4_live_rows": 15000,
        "v2_snapshot_rows": 381341,
        "missing_ref_offsets": 0,
        "duplicate_pairs": 0,
        "shadow_algorithm": "track1-shadow-v1",
        "promoted_columns": [
            "page_id", "sys_id", "work_id", "cat", "genre", "author", "title",
            "matched_letters", "best_density", "n_spans", "spans_json",
            "generation", "ref_spans_json", "shadowed_by",
        ],
    }
    doc.update(overrides)
    return doc


def _v2_contract_doc(*, namespaces, total_rows, live_rows, **overrides):
    doc = {
        "schema_version": track1_contract.CONTRACT_V2_SCHEMA_VERSION,
        "run_id": "1" * 64,
        "reference_corpus_sha256": "a" * 64,
        "canonical_masks_sha256": "b" * 64,
        "source_db_seed_sha256": "d" * 64,
        "pilot_sha256": "e" * 64,
        "calibration_sha256": "f" * 64,
        "matcher_fingerprint": "c" * 40,
        "page_count": 2,
        "page_batch": 1,
        "expected_batches": 2,
        "total_rows": total_rows,
        "live_rows": live_rows,
        "v2_snapshot_rows": 0,
        "missing_ref_offsets": 0,
        "duplicate_pairs": 0,
        "shadow_algorithm": "track1-shadow-v1",
        "promoted_columns": bds._TRACK1_V4_PROMOTED_COLUMNS,
        "namespaces": namespaces,
    }
    doc.update(overrides)
    return doc


def _ref6_split(private_total, private_live, public_total, public_live):
    return {
        "total_rows": private_total + public_total,
        "live_rows": private_live + public_live,
        "by_identity_mode": {
            "private_sibling": {"total_rows": private_total, "live_rows": private_live},
            "public_first": {"total_rows": public_total, "live_rows": public_live},
        },
    }


def _research_db_v2_shape(path, rows, *, page_count=2):
    """A research DB whose `track1_matches` table carries the EXACT v2
    promoted schema (incl. `generation`), for the `assert_track1_release_
    contract_v2` recompute tests -- these check the table's column list
    against `contract["promoted_columns"]`.

    `rows`: [(page_id, work_id, shadowed_by)].
    """
    conn = sqlite3.connect(str(path))
    cols_ddl = ", ".join(f'"{c}" TEXT' for c in bds._TRACK1_V4_PROMOTED_COLUMNS)
    conn.execute(f"CREATE TABLE track1_matches ({cols_ddl})")
    conn.execute(
        "CREATE TABLE pages (page_id TEXT PRIMARY KEY, n_chars INTEGER, "
        "text TEXT, provenance TEXT)"
    )
    conn.execute("CREATE TABLE track1_matches_v2_snapshot (page_id TEXT, work_id TEXT)")
    placeholders = ", ".join(["?"] * len(bds._TRACK1_V4_PROMOTED_COLUMNS))
    col_index = {c: i for i, c in enumerate(bds._TRACK1_V4_PROMOTED_COLUMNS)}
    for page_id, work_id, shadowed_by in rows:
        values = [None] * len(bds._TRACK1_V4_PROMOTED_COLUMNS)
        values[col_index["page_id"]] = page_id
        values[col_index["work_id"]] = work_id
        values[col_index["shadowed_by"]] = shadowed_by
        values[col_index["ref_spans_json"]] = '[{"p0":0,"p1":1,"rg0":0,"rg1":1}]'
        conn.execute(
            f"INSERT INTO track1_matches VALUES ({placeholders})", values
        )
    for i in range(page_count):
        conn.execute(
            "INSERT INTO pages (page_id, n_chars, provenance, text) VALUES (?,?,?,?)",
            (f"pg{i}", 100, "htr", "x" * 100),
        )
    conn.commit()
    conn.close()


def _research_db_simple(path, track1_rows, page_rows):
    """A research DB shaped like the pre-existing `_split_grain_fixture`-
    style tests in tests/test_v3_routing_ingest.py: no `generation` column,
    no v2-snapshot table -- fine for tests that never pass a
    `track1_release_contract_path` into `finalize_build`.

    `track1_rows`: [(page_id, sys_id, work_id, cat, matched_letters,
    best_density, spans_json, ref_spans_json)].
    `page_rows`: [(page_id, n_chars, text)].
    """
    conn = sqlite3.connect(str(path))
    conn.executescript(
        """
        CREATE TABLE track1_matches (
          page_id TEXT, sys_id TEXT, work_id TEXT, cat TEXT, genre TEXT, author TEXT,
          title TEXT, matched_letters INT, best_density REAL, n_spans INT,
          spans_json TEXT, shadowed_by TEXT, ref_spans_json TEXT
        );
        CREATE TABLE pages (
          page_id TEXT PRIMARY KEY, n_chars INTEGER, text TEXT, provenance TEXT
        );
        """
    )
    conn.executemany(
        "INSERT INTO track1_matches VALUES (?,?,?,?,NULL,NULL,NULL,?,?,1,?,NULL,?)",
        track1_rows,
    )
    conn.executemany(
        "INSERT INTO pages (page_id, n_chars, provenance, text) VALUES (?,?,?,?)",
        [(pid, n, "htr", text) for pid, n, text in page_rows],
    )
    conn.commit()
    conn.close()


def _router_db_exact(path, exact_rows, *, threshold=0.30):
    """`exact_rows`: [(page_id, work_id, surface)] -- the router scored these
    EXACT (page_id, raw_work_id) keys directly, so `regrain_router_to_split`'s
    step-1 exact-match branch fires for them: no re-grain math runs, and they
    never become "undecided"."""
    conn = sqlite3.connect(str(path))
    conn.execute(
        "CREATE TABLE coverage_route (page_id TEXT, canonical_work_id TEXT, run_id TEXT, "
        "page_coverage REAL, matched_letters INT, page_chars INT, shipped INT, surface TEXT)"
    )
    conn.execute("CREATE TABLE coverage_route_meta (run_id TEXT, threshold REAL)")
    conn.execute("INSERT INTO coverage_route_meta VALUES ('g', ?)", (threshold,))
    conn.execute(
        "CREATE TABLE discovery_claim (claim_id TEXT, page_id TEXT, work_id TEXT, "
        "canonical_work_id TEXT)"
    )
    conn.execute(
        "CREATE TABLE discovery_evidence (evidence_id TEXT, claim_id TEXT, "
        "ref_work TEXT, matched_letters_legacy INT, shadowed_by TEXT)"
    )
    for i, (page_id, work_id, surface) in enumerate(exact_rows):
        shipped = 0 if surface == "not_shipped" else 1
        conn.execute(
            "INSERT INTO coverage_route VALUES (?,?,?,?,?,?,?,?)",
            (page_id, work_id, "g", 0.9, 100, 100, shipped, surface),
        )
        claim_id = f"cl{i}"
        conn.execute(
            "INSERT INTO discovery_claim VALUES (?,?,?,?)",
            (claim_id, page_id, work_id, work_id),
        )
        conn.execute(
            "INSERT INTO discovery_evidence VALUES (?,?,?,?,NULL)",
            (f"ev{i}", claim_id, work_id, 100),
        )
    conn.commit()
    conn.close()


def _write_approved_and_crosswalk(tmp_path, entries):
    """`entries`: [(raw_work_id, opaque_work_id, title)]."""
    crosswalk_path = tmp_path / "crosswalk.json"
    _write_json(crosswalk_path, {raw: opaque for raw, opaque, _ in entries})

    approved_path = tmp_path / "approved.csv"
    with open(approved_path, "w", encoding="utf-8-sig", newline="") as fh:
        writer = _csv.DictWriter(fh, fieldnames=bds.APPROVED_HEADER)
        writer.writeheader()
        for _raw, opaque, title in entries:
            row = {h: "" for h in bds.APPROVED_HEADER}
            row["work_id"] = opaque
            row["owner_verdict"] = "approve"
            row["candidate_title"] = title
            row["source_label"] = dids.SOURCE_CORPUS_SEFARIA
            writer.writerow(row)
    return crosswalk_path, approved_path


def _routing_meta(db_path):
    conn = sqlite3.connect(str(db_path))
    try:
        meta = dict(conn.execute("SELECT key, value FROM meta").fetchall())
        rows = conn.execute(
            "SELECT c.work_id, e.routing_status, e.routing_reason "
            "FROM discovery_evidence e JOIN discovery_claim c ON c.claim_id = e.claim_id "
            "WHERE e.evidence_source = 'track1_direct'"
        ).fetchall()
    finally:
        conn.close()
    return meta, {w: (s, r) for w, s, r in rows}


def _finalize(tmp_path, source_db, crosswalk, approved, *, gen2_router_evidence_db, **kw):
    return bds.finalize_build(
        source_db_path=str(source_db),
        from_approved_path=str(approved),
        crosswalk_path=str(crosswalk),
        out_db_path=str(Path(tmp_path) / "out" / "discovery-test.db"),
        gen2_router_evidence_db=str(gen2_router_evidence_db),
        masking_patterns=["TOTALLY-UNMATCHED-MARKER-XYZ-999"],
        **kw,
    )


# ---------------------------------------------------------------------------
# C3: v1 documents still load, byte-identical
# ---------------------------------------------------------------------------

def test_v1_document_still_loads_with_v1_behavior(tmp_path):
    path = _write_json(tmp_path / "v1.json", _v1_contract_doc())
    loaded = bds.load_track1_release_contract(path)
    assert loaded["live_rows"] == 290000
    assert loaded["ref4_live_rows"] == 15000
    assert "namespaces" not in loaded
    assert len(loaded["sha256"]) == 64


def test_v1_document_missing_a_key_still_raises_the_v1_message(tmp_path):
    doc = _v1_contract_doc()
    del doc["missing_ref_offsets"]
    path = _write_json(tmp_path / "v1-bad.json", doc)
    with pytest.raises(
        bds.ReleaseInputsIncompleteError, match="frozen V4 field set"
    ):
        bds.load_track1_release_contract(path)


def test_v1_document_schema_drift_still_raises_the_v1_message(tmp_path):
    doc = _v1_contract_doc()
    doc["promoted_columns"] = doc["promoted_columns"][:-1]
    path = _write_json(tmp_path / "v1-schema-bad.json", doc)
    with pytest.raises(bds.ReleaseInputsIncompleteError, match="schema drift"):
        bds.load_track1_release_contract(path)


# ---------------------------------------------------------------------------
# C3: v2 documents validate against the cohort registry
# ---------------------------------------------------------------------------

def _valid_v2_doc():
    return _v2_contract_doc(
        namespaces={
            "REF4": {"total_rows": 5, "live_rows": 3},
            "REF5": {"total_rows": 2, "live_rows": 2},
            "REF6": _ref6_split(1, 1, 1, 0),
        },
        total_rows=100, live_rows=80,
    )


def test_v2_document_validates_and_loads(tmp_path):
    path = _write_json(tmp_path / "v2.json", _valid_v2_doc())
    loaded = bds.load_track1_release_contract(path)
    assert loaded["schema_version"] == track1_contract.CONTRACT_V2_SCHEMA_VERSION
    assert loaded["namespaces"]["REF5"]["live_rows"] == 2
    assert loaded["namespaces"]["REF6"]["by_identity_mode"]["public_first"]["total_rows"] == 1
    assert len(loaded["sha256"]) == 64


def test_v2_document_missing_a_registry_namespace_is_rejected(tmp_path):
    doc = _valid_v2_doc()
    del doc["namespaces"]["REF5"]
    path = _write_json(tmp_path / "v2-missing-ns.json", doc)
    with pytest.raises(bds.ReleaseInputsIncompleteError, match="cohort registry"):
        bds.load_track1_release_contract(path)


def test_v2_document_unknown_namespace_is_rejected(tmp_path):
    doc = _valid_v2_doc()
    doc["namespaces"]["REF9"] = {"total_rows": 0, "live_rows": 0}
    path = _write_json(tmp_path / "v2-extra-ns.json", doc)
    with pytest.raises(bds.ReleaseInputsIncompleteError, match="cohort registry"):
        bds.load_track1_release_contract(path)


def test_v2_document_ref6_identity_mode_sum_mismatch_is_rejected(tmp_path):
    doc = _valid_v2_doc()
    doc["namespaces"]["REF6"]["by_identity_mode"]["public_first"]["total_rows"] = 99
    path = _write_json(tmp_path / "v2-bad-split.json", doc)
    with pytest.raises(bds.ReleaseInputsIncompleteError):
        bds.load_track1_release_contract(path)


def test_cohort_registry_path_override_is_honored_by_the_loader(tmp_path):
    """A registry that does not know REF6 at all must reject a v2 document
    the REAL registry accepts -- proving the parameter is actually threaded
    through, not merely declared (the exact bug class this file's own
    comments warn about elsewhere)."""
    minimal_registry = tmp_path / "mini-registry.json"
    _write_json(minimal_registry, {
        "schema_version": track1_contract.COHORT_REGISTRY_SCHEMA_VERSION,
        "cohorts": [
            {"namespace": "REF2", "cohort": "legacy"},
            {"namespace": "REF4", "cohort": "extrapolated",
             "identity_mode": "private_sibling", "source_map": "mini-src.json"},
        ],
    })
    _write_json(tmp_path / "mini-src.json", {})

    path = _write_json(tmp_path / "v2.json", _valid_v2_doc())
    with pytest.raises(bds.ReleaseInputsIncompleteError, match="cohort registry"):
        bds.load_track1_release_contract(path, cohort_registry_path=str(minimal_registry))


# ---------------------------------------------------------------------------
# C3: v2 recompute-and-compare (assert_track1_release_contract_v2)
# ---------------------------------------------------------------------------

def _v2_recompute_fixture(tmp_path):
    """One private row (live), one shadowed REF4 row, one live REF4 row, one
    live REF5 row, one live REF6 row -- so total_rows=5, live_rows=4,
    REF4={2,1}, REF5={1,1}, REF6={1,1}."""
    db_path = tmp_path / "research.db"
    _research_db_v2_shape(db_path, [
        ("pg0", "private:w1", None),
        ("pg1", "REF4:one", "REF4:one"),   # shadowed
        ("pg2", "REF4:two", None),
        ("pg3", "REF5:extra", None),
        ("pg4", "REF6:extra", None),
    ], page_count=5)
    contract = _v2_contract_doc(
        namespaces={
            "REF4": {"total_rows": 2, "live_rows": 1},
            "REF5": {"total_rows": 1, "live_rows": 1},
            "REF6": _ref6_split(1, 1, 0, 0),
        },
        total_rows=5, live_rows=4, page_count=5,
    )
    return db_path, contract


def test_v2_recompute_passes_on_a_matching_research_db(tmp_path):
    db_path, contract = _v2_recompute_fixture(tmp_path)
    conn = sqlite3.connect(str(db_path))
    try:
        bds.assert_track1_release_contract_v2(conn, contract, REAL_REGISTRY)
    finally:
        conn.close()


def test_v2_recompute_catches_a_namespace_count_drift(tmp_path):
    """The namespace-count drift test (deliverable 4): a v2 contract
    claiming a REF5 live_rows count the research DB does not have must be
    rejected, exactly like a v1 count mismatch."""
    db_path, contract = _v2_recompute_fixture(tmp_path)
    contract["namespaces"]["REF5"]["live_rows"] = 0  # DB actually has 1
    conn = sqlite3.connect(str(db_path))
    try:
        with pytest.raises(bds.ReleaseInputsIncompleteError, match="REF5"):
            bds.assert_track1_release_contract_v2(conn, contract, REAL_REGISTRY)
    finally:
        conn.close()


def test_v2_recompute_hard_errors_on_an_unregistered_namespace_in_the_db(tmp_path):
    db_path, contract = _v2_recompute_fixture(tmp_path)
    conn = sqlite3.connect(str(db_path))
    conn.execute(
        "UPDATE track1_matches SET work_id = 'REF9:mystery' WHERE page_id = 'pg3'"
    )
    conn.commit()
    try:
        with pytest.raises(ValueError, match="REF9"):
            bds.assert_track1_release_contract_v2(conn, contract, REAL_REGISTRY)
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# C4: cohort routing inside finalize_build
# ---------------------------------------------------------------------------

def test_ref5_rows_are_excluded_from_legacy_regrain_and_routed_via_extrapolation(tmp_path):
    """THE P2 bug this work fixes. Before the fix, a REF5 row was NOT
    excluded from the legacy split-grain regrain population (only `REF4:`
    was), so it reached `regrain_router_to_split`, which never scored it --
    `undecided`, and (with no release contract supplied) the build HALTS
    with `RoutingConflictError`. After the fix, the REF5 row is classified
    via the cohort registry, excluded from the legacy population, and
    routed through its own `REF5:` extrapolation call -- the build succeeds
    and the asset's meta reports it under its own namespace."""
    work_dir = tmp_path / "a"
    work_dir.mkdir()
    router_db = work_dir / "route.db"
    _router_db_exact(router_db, [("pg1", "raw:legacy1", "same_work")])

    research_db = work_dir / "research.db"
    _research_db_simple(
        research_db,
        track1_rows=[
            ("pg1", "s1", "raw:legacy1", "Sefaria", 270, 0.9,
             "[[0,270,0.9]]", '[{"p0":0,"p1":270,"rg0":10,"rg1":280}]'),
            ("pg2", "s1", "REF5:extra", "Sefaria", 90, 0.9,
             "[[0,90,0.9]]", '[{"p0":0,"p1":90,"rg0":5,"rg1":95}]'),
        ],
        page_rows=[("pg1", 300, "x" * 300), ("pg2", 100, "x" * 100)],
    )
    crosswalk, approved = _write_approved_and_crosswalk(work_dir, [
        ("raw:legacy1", "w000001", "Legacy Work"),
        ("REF5:extra", "w000002", "REF5 Work"),
    ])

    stats = _finalize(
        work_dir, research_db, crosswalk, approved,
        gen2_router_evidence_db=router_db,
    )

    conn = sqlite3.connect(stats["db_path"])
    try:
        meta = dict(conn.execute("SELECT key, value FROM meta").fetchall())
    finally:
        conn.close()

    assert meta["coverage_ref5_reference_routing"] == "threshold_extrapolated_new_works"
    assert meta["coverage_ref5_reference_prefix"] == "REF5:"
    assert meta["coverage_ref5_reference_considered"] == "1"
    assert meta["coverage_ref5_reference_routed"] == "1"
    assert meta["coverage_ref5_reference_same_work"] == "1"
    assert meta["coverage_ref5_reference_parallel"] == "0"
    # It must NOT have been misrouted through the fullscan-legacy path, and
    # REF4's key names must not be reused for a REF5 population.
    assert "coverage_fullscan_legacy_routing" not in meta
    assert "coverage_v4_reference_routing" not in meta


def test_ref4_only_build_keeps_the_exact_pre_existing_meta_key_names(tmp_path):
    """Byte-compatibility check: a REF4-only build must still emit
    `coverage_v4_reference_*` (not a generic `coverage_ref4_reference_*`)."""
    work_dir = tmp_path / "a"
    work_dir.mkdir()
    router_db = work_dir / "route.db"
    _router_db_exact(router_db, [("pg1", "raw:legacy1", "same_work")])

    research_db = work_dir / "research.db"
    _research_db_simple(
        research_db,
        track1_rows=[
            ("pg1", "s1", "raw:legacy1", "Sefaria", 270, 0.9,
             "[[0,270,0.9]]", '[{"p0":0,"p1":270,"rg0":10,"rg1":280}]'),
            ("pg2", "s1", "REF4:extra", "Sefaria", 90, 0.9,
             "[[0,90,0.9]]", '[{"p0":0,"p1":90,"rg0":5,"rg1":95}]'),
        ],
        page_rows=[("pg1", 300, "x" * 300), ("pg2", 100, "x" * 100)],
    )
    crosswalk, approved = _write_approved_and_crosswalk(work_dir, [
        ("raw:legacy1", "w000001", "Legacy Work"),
        ("REF4:extra", "w000002", "REF4 Work"),
    ])

    stats = _finalize(
        work_dir, research_db, crosswalk, approved,
        gen2_router_evidence_db=router_db,
    )
    meta, _ = _routing_meta(stats["db_path"])
    assert meta["coverage_v4_reference_routing"] == "threshold_extrapolated_new_works"
    assert meta["coverage_v4_reference_prefix"] == "REF4:"
    assert meta["coverage_v4_reference_considered"] == "1"
    assert "coverage_ref4_reference_routing" not in meta
    assert meta["coverage_routing"] == "gen2_router_split_regrained_v4_extrapolated"


def test_ref2_rows_stay_on_the_legacy_path(tmp_path):
    """REF2 is a registered `legacy` cohort: a REF2 row must be decided by
    the ordinary split-grain/exact-match path, never routed through the
    per-namespace v4-extrapolation call."""
    work_dir = tmp_path / "a"
    work_dir.mkdir()
    router_db = work_dir / "route.db"
    _router_db_exact(router_db, [("pg1", "REF2:legacy1", "same_work")])

    research_db = work_dir / "research.db"
    _research_db_simple(
        research_db,
        track1_rows=[
            ("pg1", "s1", "REF2:legacy1", "Sefaria", 270, 0.9,
             "[[0,270,0.9]]", '[{"p0":0,"p1":270,"rg0":10,"rg1":280}]'),
        ],
        page_rows=[("pg1", 300, "x" * 300)],
    )
    crosswalk, approved = _write_approved_and_crosswalk(work_dir, [
        ("REF2:legacy1", "w000001", "Legacy REF2 Work"),
    ])

    stats = _finalize(
        work_dir, research_db, crosswalk, approved,
        gen2_router_evidence_db=router_db,
    )
    meta, _ = _routing_meta(stats["db_path"])
    assert meta["coverage_routing"] == "gen2_router_split_regrained"
    assert meta["coverage_regrain_kept_exact"] == "1"
    assert "coverage_v4_reference_routing" not in meta
    assert "coverage_ref2_reference_routing" not in meta
    assert "coverage_fullscan_legacy_routing" not in meta


def test_unknown_ref_prefix_hard_errors_during_routing(tmp_path):
    """An unregistered REF* prefix (REF7, not in the committed registry)
    must propagate `classify_work_id`'s own ValueError -- never a silent
    legacy fallback."""
    work_dir = tmp_path / "a"
    work_dir.mkdir()
    router_db = work_dir / "route.db"
    _router_db_exact(router_db, [("pg1", "raw:legacy1", "same_work")])

    research_db = work_dir / "research.db"
    _research_db_simple(
        research_db,
        track1_rows=[
            ("pg1", "s1", "raw:legacy1", "Sefaria", 270, 0.9,
             "[[0,270,0.9]]", '[{"p0":0,"p1":270,"rg0":10,"rg1":280}]'),
            ("pg2", "s1", "REF7:mystery", "Sefaria", 90, 0.9,
             "[[0,90,0.9]]", '[{"p0":0,"p1":90,"rg0":5,"rg1":95}]'),
        ],
        page_rows=[("pg1", 300, "x" * 300), ("pg2", 100, "x" * 100)],
    )
    crosswalk, approved = _write_approved_and_crosswalk(work_dir, [
        ("raw:legacy1", "w000001", "Legacy Work"),
        ("REF7:mystery", "w000002", "Mystery Work"),
    ])

    with pytest.raises(ValueError, match="REF7"):
        _finalize(
            work_dir, research_db, crosswalk, approved,
            gen2_router_evidence_db=router_db,
        )


def test_cohort_registry_path_override_changes_routing_behavior(tmp_path):
    """A custom registry that does not know REF5 must hard-error on a REF5
    row the REAL (default) registry would happily route -- proving
    `cohort_registry_path` is actually threaded into `finalize_build`'s
    routing, not merely accepted and ignored."""
    work_dir = tmp_path / "a"
    work_dir.mkdir()
    router_db = work_dir / "route.db"
    _router_db_exact(router_db, [("pg1", "raw:legacy1", "same_work")])

    research_db = work_dir / "research.db"
    _research_db_simple(
        research_db,
        track1_rows=[
            ("pg1", "s1", "raw:legacy1", "Sefaria", 270, 0.9,
             "[[0,270,0.9]]", '[{"p0":0,"p1":270,"rg0":10,"rg1":280}]'),
            ("pg2", "s1", "REF5:extra", "Sefaria", 90, 0.9,
             "[[0,90,0.9]]", '[{"p0":0,"p1":90,"rg0":5,"rg1":95}]'),
        ],
        page_rows=[("pg1", 300, "x" * 300), ("pg2", 100, "x" * 100)],
    )
    crosswalk, approved = _write_approved_and_crosswalk(work_dir, [
        ("raw:legacy1", "w000001", "Legacy Work"),
        ("REF5:extra", "w000002", "REF5 Work"),
    ])

    minimal_registry = work_dir / "mini-registry.json"
    _write_json(minimal_registry, {
        "schema_version": track1_contract.COHORT_REGISTRY_SCHEMA_VERSION,
        "cohorts": [{"namespace": "REF2", "cohort": "legacy"}],
    })

    with pytest.raises(ValueError, match="REF5"):
        _finalize(
            work_dir, research_db, crosswalk, approved,
            gen2_router_evidence_db=router_db,
            cohort_registry_path=str(minimal_registry),
        )

    # The SAME fixture, with no override, must succeed via the real registry.
    stats = _finalize(
        work_dir, research_db, crosswalk, approved,
        gen2_router_evidence_db=router_db,
    )
    meta, _ = _routing_meta(stats["db_path"])
    assert meta["coverage_ref5_reference_routing"] == "threshold_extrapolated_new_works"


def test_ref5_and_ref6_route_separately_in_the_same_build(tmp_path):
    """Two extrapolated namespaces present at once must each get their own
    per-namespace report and meta facts -- proving the per-namespace loop,
    not just a single hardcoded extra case."""
    work_dir = tmp_path / "a"
    work_dir.mkdir()
    router_db = work_dir / "route.db"
    _router_db_exact(router_db, [("pg1", "raw:legacy1", "same_work")])

    research_db = work_dir / "research.db"
    _research_db_simple(
        research_db,
        track1_rows=[
            ("pg1", "s1", "raw:legacy1", "Sefaria", 270, 0.9,
             "[[0,270,0.9]]", '[{"p0":0,"p1":270,"rg0":10,"rg1":280}]'),
            ("pg2", "s1", "REF5:extra", "Sefaria", 90, 0.9,
             "[[0,90,0.9]]", '[{"p0":0,"p1":90,"rg0":5,"rg1":95}]'),
            ("pg3", "s1", "REF6:extra", "Sefaria", 10, 0.9,
             "[[0,10,0.9]]", '[{"p0":0,"p1":10,"rg0":1,"rg1":11}]'),
        ],
        page_rows=[
            ("pg1", 300, "x" * 300), ("pg2", 100, "x" * 100), ("pg3", 100, "x" * 100),
        ],
    )
    crosswalk, approved = _write_approved_and_crosswalk(work_dir, [
        ("raw:legacy1", "w000001", "Legacy Work"),
        ("REF5:extra", "w000002", "REF5 Work"),
        ("REF6:extra", "w000003", "REF6 Work"),
    ])

    stats = _finalize(
        work_dir, research_db, crosswalk, approved,
        gen2_router_evidence_db=router_db,
    )
    meta, _ = _routing_meta(stats["db_path"])
    assert meta["coverage_ref5_reference_same_work"] == "1"   # 90/100 >= 0.30
    assert meta["coverage_ref6_reference_parallel"] == "1"    # 10/100 < 0.30
    assert meta["coverage_routing"] == "gen2_router_split_regrained_v4_extrapolated"
    assert stats["coverage_v4_references_by_namespace"].keys() == {"REF5", "REF6"}
