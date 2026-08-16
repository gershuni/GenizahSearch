# -*- coding: utf-8 -*-
"""Tests for the ordered `--sources-bundle` mechanism (V4.2 plan C2, C12).

`scripts/bake_discovery_excerpts.py::load_v4_public_sources` used to
dereference a reference manifest's OWN recorded `acquisition_manifest` path
string -- a depot move left that recorded absolute path pointing at a
deleted `_tmp\\` directory, which is the P2 defect this file's tests pin
(`docs/OPEN_ISSUES.md`, "Excerpt bake dereferences the dead `_tmp\\`
acquisition-manifest path..."). The fix replaces the single reference-
manifest + normalized-dir CLI wiring with an ORDERED `--sources-bundle` JSON
(schema `discovery-excerpt-sources-bundle-v1`) naming every reference-
manifest-chain stage (REF4 -> REF5 -> REF6 -> ...) as an explicit hash-
pinned input, and generalizes the REF4-only pkl/crosswalk gates to any
registered namespace.

Synthetic fixtures ONLY: every reference manifest, acquisition manifest, and
normalized-source file below is built fresh in `tmp_path` with throwaway
Hebrew filler text. Nothing here reads or references `same_work_spike/` or
any other real corpus/depot data, and no network access occurs.
"""
from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

import pytest  # noqa: E402

from bake_discovery_excerpts import (  # noqa: E402
    EXCERPT_SOURCES_BUNDLE_SCHEMA_VERSION,
    check_bundle_covers_pkl_namespaces,
    check_crosswalk_namespace_coverage,
    check_pkl_source_set_equality,
    load_excerpt_sources_bundle,
    load_public_sources_from_bundle,
    load_v4_public_sources,
    pkl_namespace_ids,
    sha256_file,
)

_BASE_HASH = "0" * 64  # synthetic pinned base V2 reference-corpus hash


def _write_json(path: Path, obj) -> None:
    path.write_text(json.dumps(obj, ensure_ascii=False), encoding="utf-8")


def _bundle_doc(stages: list) -> dict:
    return {
        "schema_version": EXCERPT_SOURCES_BUNDLE_SCHEMA_VERSION,
        "stages": stages,
    }


def _build_stage(
    tmp_path: Path,
    namespace: str,
    *,
    base_reference_sha256: str,
    key: str = "src",
    unit_text: str = "מלה אחת בלבד",
    ordinal: int = 1,
    attribution: str = "Synthetic public-domain attribution",
    reference_corpus_sha256: str | None = None,
    dead_recorded_acquisition_path: bool = True,
):
    """Build one bundle stage's on-disk fixtures.

    Returns ``(stage_dict, reference_corpus_sha256, raw_reference_id)``. By
    default the reference manifest's OWN recorded ``acquisition_manifest``
    field points at a path that does not exist on disk -- mirroring the real
    depot-move P2 defect -- so any test using the default builder only
    passes if the acquisition manifest path actually used comes from the
    bundle's EXPLICIT stage input, never from the manifest's recorded
    string.
    """
    stage_dir = tmp_path / f"stage_{namespace.lower()}_{key}"
    stage_dir.mkdir()
    normalized_dir = stage_dir / "normalized"
    normalized_dir.mkdir()
    normalized = {
        "attribution": attribution,
        "units": [{"ordinal": ordinal, "text": unit_text}],
    }
    normalized_path = normalized_dir / f"{key}.json"
    _write_json(normalized_path, normalized)

    acquisition = {
        "entries": [
            {
                "key": key,
                "status": "acquired",
                "normalized_file": f"{key}.json",
                "normalized_sha256": sha256_file(normalized_path),
            }
        ]
    }
    acquisition_path = stage_dir / "acquisition.json"
    _write_json(acquisition_path, acquisition)

    raw_id = f"{namespace}:{key}"
    corpus_hash = reference_corpus_sha256 or hashlib.sha256(
        f"{namespace}-{key}-corpus".encode("utf-8")
    ).hexdigest()
    recorded_acquisition_path = (
        str(tmp_path / "_tmp" / "discovery_v4" / "sources" / "dead_manifest.json")
        if dead_recorded_acquisition_path
        else str(acquisition_path)
    )
    reference = {
        "schema_version": "discovery-v4-reference-manifest-v1",
        "base_reference_sha256": base_reference_sha256,
        "reference_corpus_sha256": corpus_hash,
        "acquisition_manifest": recorded_acquisition_path,
        "acquisition_manifest_sha256": sha256_file(acquisition_path),
        "entries": [
            {
                "raw_reference_id": raw_id,
                "source_key": key,
                "unit_offsets": [{"source_ordinal": ordinal}],
            }
        ],
    }
    if namespace != "REF4":
        reference["reference_namespace"] = namespace
    reference_path = stage_dir / "reference.json"
    _write_json(reference_path, reference)

    stage = {
        "namespace": namespace,
        "reference_manifest": str(reference_path),
        "reference_manifest_sha256": sha256_file(reference_path),
        "acquisition_manifest": str(acquisition_path),
        "normalized_dir": str(normalized_dir),
    }
    return stage, corpus_hash, raw_id


# ---------------------------------------------------------------------------
# P2 regression: recorded-path independence
# ---------------------------------------------------------------------------


def test_p2_regression_bundle_explicit_path_loads_despite_dead_recorded_path(tmp_path):
    """The reference manifest's OWN `acquisition_manifest` field is a dead
    absolute path (mirroring the real depot-move defect). The bundle's
    EXPLICIT acquisition-manifest path is the one actually dereferenced, so
    the load succeeds. Calling `load_v4_public_sources` the PRE-FIX way
    (omitting the explicit path -- the exact call shape the old, buggy
    `main()` used) fails loudly on this very fixture, which is what makes
    this the P2 regression test."""
    stage, _corpus_hash, raw_id = _build_stage(
        tmp_path, "REF4", base_reference_sha256=_BASE_HASH
    )

    texts, attributions, acquisition_sha = load_v4_public_sources(
        Path(stage["reference_manifest"]),
        Path(stage["normalized_dir"]),
        acquisition_manifest_path=Path(stage["acquisition_manifest"]),
        expected_namespace="REF4",
    )
    assert texts == {raw_id: "מלה אחת בלבד"}
    assert attributions == {raw_id: "Synthetic public-domain attribution"}
    assert acquisition_sha == sha256_file(Path(stage["acquisition_manifest"]))

    with pytest.raises(OSError):
        load_v4_public_sources(
            Path(stage["reference_manifest"]), Path(stage["normalized_dir"])
        )


def test_single_ref4_stage_bundle_reproduces_todays_v4_only_path(tmp_path):
    """Deliverable 4: a bundle with only the REF4 stage must behave like the
    pre-bundle single-manifest V4 path for the shipped V4 inputs."""
    stage, corpus_hash, raw_id = _build_stage(
        tmp_path, "REF4", base_reference_sha256=_BASE_HASH
    )
    bundle_path = tmp_path / "bundle.json"
    _write_json(bundle_path, _bundle_doc([stage]))

    bundle = load_excerpt_sources_bundle(bundle_path)
    texts, attributions, stage_reports = load_public_sources_from_bundle(
        bundle, _BASE_HASH
    )

    assert texts == {raw_id: "מלה אחת בלבד"}
    assert attributions == {raw_id: "Synthetic public-domain attribution"}
    assert set(stage_reports) == {"REF4"}
    assert stage_reports["REF4"]["raw_ids"] == {raw_id}
    assert stage_reports["REF4"]["reference_corpus_sha256"] == corpus_hash


# ---------------------------------------------------------------------------
# Hash pins
# ---------------------------------------------------------------------------


def test_acquisition_manifest_hash_binding_rejects_a_tampered_file(tmp_path):
    stage, _corpus_hash, _raw_id = _build_stage(
        tmp_path, "REF4", base_reference_sha256=_BASE_HASH
    )
    acq_path = Path(stage["acquisition_manifest"])
    tampered = json.loads(acq_path.read_text(encoding="utf-8"))
    tampered["entries"][0]["normalized_sha256"] = "f" * 64
    _write_json(acq_path, tampered)

    with pytest.raises(ValueError, match="acquisition manifest hash differs"):
        load_v4_public_sources(
            Path(stage["reference_manifest"]),
            Path(stage["normalized_dir"]),
            acquisition_manifest_path=acq_path,
            expected_namespace="REF4",
        )


def test_bundle_reference_manifest_hash_pin_rejects_a_tampered_manifest(tmp_path):
    stage, _corpus_hash, _raw_id = _build_stage(
        tmp_path, "REF4", base_reference_sha256=_BASE_HASH
    )
    stage["reference_manifest_sha256"] = "0" * 64  # wrong pin
    bundle_path = tmp_path / "bundle.json"
    _write_json(bundle_path, _bundle_doc([stage]))
    bundle = load_excerpt_sources_bundle(bundle_path)

    with pytest.raises(ValueError, match="differs from the sources bundle's pin"):
        load_public_sources_from_bundle(bundle, _BASE_HASH)


# ---------------------------------------------------------------------------
# Chain continuity (C2)
# ---------------------------------------------------------------------------


def test_chain_discontinuity_names_both_stages(tmp_path):
    ref4_stage, _ref4_hash, _ref4_id = _build_stage(
        tmp_path, "REF4", base_reference_sha256=_BASE_HASH, key="s4"
    )
    wrong_base = "1" * 64  # should have been ref4's reference_corpus_sha256
    ref5_stage, _ref5_hash, _ref5_id = _build_stage(
        tmp_path, "REF5", base_reference_sha256=wrong_base, key="s5"
    )
    bundle_path = tmp_path / "bundle.json"
    _write_json(bundle_path, _bundle_doc([ref4_stage, ref5_stage]))
    bundle = load_excerpt_sources_bundle(bundle_path)

    with pytest.raises(ValueError) as exc_info:
        load_public_sources_from_bundle(bundle, _BASE_HASH)
    msg = str(exc_info.value)
    assert "manifest-chain discontinuity" in msg
    assert "REF5" in msg
    assert "REF4" in msg


def test_first_stage_chain_continuity_checks_against_the_pinned_base(tmp_path):
    stage, _corpus_hash, _raw_id = _build_stage(
        tmp_path, "REF4", base_reference_sha256=_BASE_HASH
    )
    bundle_path = tmp_path / "bundle.json"
    _write_json(bundle_path, _bundle_doc([stage]))
    bundle = load_excerpt_sources_bundle(bundle_path)

    wrong_pinned_base = "2" * 64
    with pytest.raises(ValueError, match="manifest-chain discontinuity"):
        load_public_sources_from_bundle(bundle, wrong_pinned_base)


# ---------------------------------------------------------------------------
# Namespace checks
# ---------------------------------------------------------------------------


def test_bundle_namespace_mismatch_against_the_manifests_own_reference_namespace(
    tmp_path,
):
    """Deliverable 5: bundle says REF5, manifest entries say REF4 (the
    manifest omits `reference_namespace`, which means REF4) -> hard error."""
    stage, _corpus_hash, _raw_id = _build_stage(
        tmp_path, "REF4", base_reference_sha256=_BASE_HASH
    )
    stage["namespace"] = "REF5"  # bundle declares REF5 for a REF4 manifest
    bundle_path = tmp_path / "bundle.json"
    _write_json(bundle_path, _bundle_doc([stage]))
    bundle = load_excerpt_sources_bundle(bundle_path)

    with pytest.raises(
        ValueError, match="does not match the sources bundle's declared namespace"
    ):
        load_public_sources_from_bundle(bundle, _BASE_HASH)


def test_per_entry_raw_id_must_carry_the_stages_namespace_prefix(tmp_path):
    stage, _corpus_hash, _raw_id = _build_stage(
        tmp_path, "REF4", base_reference_sha256=_BASE_HASH
    )
    reference_path = Path(stage["reference_manifest"])
    doc = json.loads(reference_path.read_text(encoding="utf-8"))
    doc["entries"][0]["raw_reference_id"] = "REF5:src"  # wrong prefix
    _write_json(reference_path, doc)
    stage["reference_manifest_sha256"] = sha256_file(reference_path)

    bundle_path = tmp_path / "bundle.json"
    _write_json(bundle_path, _bundle_doc([stage]))
    bundle = load_excerpt_sources_bundle(bundle_path)

    with pytest.raises(ValueError, match="does not carry the"):
        load_public_sources_from_bundle(bundle, _BASE_HASH)


# ---------------------------------------------------------------------------
# Multi-stage merge
# ---------------------------------------------------------------------------


def test_multi_stage_bundle_merges_texts_keyed_by_raw_id(tmp_path):
    ref4_stage, ref4_hash, ref4_id = _build_stage(
        tmp_path, "REF4", base_reference_sha256=_BASE_HASH, key="a4",
        unit_text="טקסט ארבע",
    )
    ref5_stage, ref5_hash, ref5_id = _build_stage(
        tmp_path, "REF5", base_reference_sha256=ref4_hash, key="a5",
        unit_text="טקסט חמש",
    )
    ref6_stage, ref6_hash, ref6_id = _build_stage(
        tmp_path, "REF6", base_reference_sha256=ref5_hash, key="a6",
        unit_text="טקסט שש",
    )
    bundle_path = tmp_path / "bundle.json"
    _write_json(bundle_path, _bundle_doc([ref4_stage, ref5_stage, ref6_stage]))
    bundle = load_excerpt_sources_bundle(bundle_path)

    texts, attributions, stage_reports = load_public_sources_from_bundle(
        bundle, _BASE_HASH
    )

    assert texts == {
        ref4_id: "טקסט ארבע",
        ref5_id: "טקסט חמש",
        ref6_id: "טקסט שש",
    }
    assert set(attributions) == {ref4_id, ref5_id, ref6_id}
    assert set(stage_reports) == {"REF4", "REF5", "REF6"}
    assert stage_reports["REF4"]["raw_ids"] == {ref4_id}
    assert stage_reports["REF5"]["raw_ids"] == {ref5_id}
    assert stage_reports["REF6"]["raw_ids"] == {ref6_id}
    assert stage_reports["REF6"]["reference_corpus_sha256"] == ref6_hash


# ---------------------------------------------------------------------------
# Generalized pkl / crosswalk gates (main()'s per-namespace logic)
# ---------------------------------------------------------------------------


def test_pickle_namespace_without_a_bundle_stage_is_a_hard_error():
    """Deliverable 5: a namespace present in the pickle but absent from the
    bundle is a hard error -- the generalized "REF4 references require
    their pinned V4 public-source inputs" gate."""
    pkl_stream = {
        "REF4:a": "stream-a",
        "REF6:b": "stream-b",  # no REF6 bundle stage registered
        "REF2:c": "stream-c",  # REF2 is governed elsewhere, never bundle-gated
        "w000123": "stream-private",  # non-namespaced private id, ignored
    }
    grouped = pkl_namespace_ids(pkl_stream)
    assert grouped == {"REF4": {"REF4:a"}, "REF6": {"REF6:b"}}

    with pytest.raises(ValueError, match="REF6.*sources bundle"):
        check_bundle_covers_pkl_namespaces(grouped, bundle_namespaces={"REF4"})

    # A namespace registered in the bundle raises nothing.
    check_bundle_covers_pkl_namespaces(grouped, bundle_namespaces={"REF4", "REF6"})


def test_pkl_source_set_equality_gate_rejects_a_mismatched_namespace():
    pkl_namespaces = {"REF4": {"REF4:a", "REF4:b"}}
    stage_reports = {"REF4": {"raw_ids": {"REF4:a"}}}  # REF4:b missing

    with pytest.raises(
        ValueError, match="REF4 public-source set does not equal the REF4 pickle set"
    ):
        check_pkl_source_set_equality(pkl_namespaces, stage_reports)

    # An exact match raises nothing.
    check_pkl_source_set_equality(
        {"REF4": {"REF4:a"}}, {"REF4": {"raw_ids": {"REF4:a"}}}
    )


def test_crosswalk_namespace_coverage_gate_rejects_an_unknown_id():
    crosswalk = {"REF4:a": "w1", "REF4:ghost": "w2"}
    stage_reports = {"REF4": {"raw_ids": {"REF4:a"}}}

    with pytest.raises(ValueError, match="crosswalk contains REF4 ids"):
        check_crosswalk_namespace_coverage(crosswalk, stage_reports)

    # A fully-covered crosswalk raises nothing.
    check_crosswalk_namespace_coverage(
        {"REF4:a": "w1"}, {"REF4": {"raw_ids": {"REF4:a"}}}
    )


# ---------------------------------------------------------------------------
# Bundle shape validation
# ---------------------------------------------------------------------------


def test_bundle_schema_version_is_checked(tmp_path):
    bundle_path = tmp_path / "bundle.json"
    _write_json(bundle_path, {"schema_version": "wrong-version", "stages": []})

    with pytest.raises(
        ValueError, match="unsupported excerpt sources bundle schema_version"
    ):
        load_excerpt_sources_bundle(bundle_path)


def test_bundle_schema_rejects_an_invalid_namespace_string(tmp_path):
    bundle_path = tmp_path / "bundle.json"
    _write_json(bundle_path, _bundle_doc([{
        "namespace": "REFX",
        "reference_manifest": "x",
        "reference_manifest_sha256": "a" * 64,
        "acquisition_manifest": "y",
        "normalized_dir": "z",
    }]))

    with pytest.raises(ValueError, match="invalid namespace"):
        load_excerpt_sources_bundle(bundle_path)


def test_bundle_schema_rejects_a_duplicate_namespace(tmp_path):
    stage_a = {
        "namespace": "REF4",
        "reference_manifest": "x",
        "reference_manifest_sha256": "a" * 64,
        "acquisition_manifest": "y",
        "normalized_dir": "z",
    }
    bundle_path = tmp_path / "bundle.json"
    _write_json(bundle_path, _bundle_doc([stage_a, dict(stage_a)]))

    with pytest.raises(ValueError, match="duplicate namespace"):
        load_excerpt_sources_bundle(bundle_path)


def test_bundle_schema_rejects_a_stage_missing_a_required_key(tmp_path):
    bundle_path = tmp_path / "bundle.json"
    _write_json(bundle_path, _bundle_doc([{
        "namespace": "REF4",
        "reference_manifest": "x",
        "reference_manifest_sha256": "a" * 64,
        "acquisition_manifest": "y",
        # normalized_dir intentionally missing
    }]))

    with pytest.raises(ValueError, match="missing keys"):
        load_excerpt_sources_bundle(bundle_path)
