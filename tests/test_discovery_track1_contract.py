"""The shared v2 release-contract schema, run identity, and cohort registry."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts.discovery_track1_contract import (
    CONTRACT_V2_KEYS,
    CONTRACT_V2_SCHEMA_VERSION,
    classify_work_id,
    derive_run_id,
    extrapolated_namespaces,
    load_cohort_registry,
    validate_contract_v2,
)

SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
REGISTRY = SCRIPTS / "discovery_routing_cohorts.json"


def _facts(**overrides) -> dict:
    facts = {
        "reference_corpus_sha256": "a" * 64,
        "canonical_masks_sha256": "b" * 64,
        "source_db_seed_sha256": "c" * 64,
        "pilot_sha256": "d" * 64,
        "calibration_sha256": "e" * 64,
        "page_count": 667_411,
        "page_batch": 2_000,
        "generation": "live",
        "tag": "v42_combined",
    }
    facts.update(overrides)
    return facts


def _counts(total: int, live: int) -> dict:
    return {"total_rows": total, "live_rows": live}


def _contract(**overrides) -> dict:
    ref6_modes = {
        "private_sibling": _counts(60, 40),
        "public_first": _counts(40, 30),
    }
    doc = {
        "schema_version": CONTRACT_V2_SCHEMA_VERSION,
        "run_id": derive_run_id(_facts()),
        "reference_corpus_sha256": "a" * 64,
        "canonical_masks_sha256": "b" * 64,
        "source_db_seed_sha256": "c" * 64,
        "pilot_sha256": "d" * 64,
        "calibration_sha256": "e" * 64,
        "matcher_fingerprint": "f" * 40,
        "page_count": 667_411,
        "page_batch": 2_000,
        "expected_batches": 334,
        "total_rows": 400_000,
        "live_rows": 280_000,
        "v2_snapshot_rows": 381_341,
        "missing_ref_offsets": 0,
        "duplicate_pairs": 0,
        "shadow_algorithm": "track1-shadow-v1",
        "promoted_columns": ["page_id", "work_id"],
        "namespaces": {
            "REF4": _counts(9_000, 5_000),
            "REF5": _counts(1_000, 700),
            "REF6": {**_counts(100, 70), "by_identity_mode": ref6_modes},
        },
    }
    doc.update(overrides)
    return doc


EXPECTED = {"REF4", "REF5", "REF6"}


def test_run_id_is_deterministic_and_input_sensitive():
    base = derive_run_id(_facts())
    assert base == derive_run_id(_facts())
    for key, value in (
        ("reference_corpus_sha256", "9" * 64),
        ("page_batch", 2_001),
        ("tag", "other"),
    ):
        assert derive_run_id(_facts(**{key: value})) != base


def test_run_id_rejects_missing_extra_and_malformed_facts():
    facts = _facts()
    del facts["pilot_sha256"]
    with pytest.raises(ValueError, match="missing"):
        derive_run_id(facts)
    with pytest.raises(ValueError, match="unexpected"):
        derive_run_id(_facts(extra="x"))
    with pytest.raises(ValueError, match="SHA-256"):
        derive_run_id(_facts(canonical_masks_sha256="not-hex"))
    with pytest.raises(ValueError, match="positive integer"):
        derive_run_id(_facts(page_count=0))


def test_contract_v2_accepts_the_reference_shape():
    validate_contract_v2(_contract(), expected_namespaces=EXPECTED)


def test_contract_v2_rejects_key_drift_both_directions():
    doc = _contract()
    del doc["pilot_sha256"]
    with pytest.raises(ValueError, match="missing=\\['pilot_sha256'\\]"):
        validate_contract_v2(doc, expected_namespaces=EXPECTED)
    with pytest.raises(ValueError, match="unexpected=\\['ref4_total_rows'\\]"):
        validate_contract_v2(
            _contract(ref4_total_rows=9_000), expected_namespaces=EXPECTED
        )
    assert "ref4_total_rows" not in CONTRACT_V2_KEYS  # v1 keys stay v1's


def test_contract_v2_requires_every_registry_namespace_even_at_zero():
    doc = _contract()
    del doc["namespaces"]["REF5"]
    with pytest.raises(ValueError, match="missing=\\['REF5'\\]"):
        validate_contract_v2(doc, expected_namespaces=EXPECTED)
    doc = _contract()
    doc["namespaces"]["REF7"] = _counts(1, 1)
    with pytest.raises(ValueError, match="unexpected=\\['REF7'\\]"):
        validate_contract_v2(doc, expected_namespaces=EXPECTED)
    # explicit zero IS acceptable
    doc = _contract()
    doc["namespaces"]["REF5"] = _counts(0, 0)
    validate_contract_v2(doc, expected_namespaces=EXPECTED)


def test_contract_v2_rejects_inconsistent_counts():
    doc = _contract()
    doc["namespaces"]["REF4"] = _counts(5, 9)
    with pytest.raises(ValueError, match="live_rows > total_rows"):
        validate_contract_v2(doc, expected_namespaces=EXPECTED)
    doc = _contract()
    doc["namespaces"]["REF6"]["by_identity_mode"]["public_first"] = _counts(1, 1)
    with pytest.raises(ValueError, match="identity-mode sum"):
        validate_contract_v2(doc, expected_namespaces=EXPECTED)
    doc = _contract()
    del doc["namespaces"]["REF6"]["by_identity_mode"]["public_first"]
    with pytest.raises(ValueError, match="by_identity_mode"):
        validate_contract_v2(doc, expected_namespaces=EXPECTED)
    doc = _contract()
    doc["namespaces"]["REF4"]["by_identity_mode"] = {}
    with pytest.raises(ValueError, match="keys must be"):
        validate_contract_v2(doc, expected_namespaces=EXPECTED)


def test_committed_registry_loads_and_names_the_three_extrapolated_cohorts():
    registry = load_cohort_registry(REGISTRY)
    assert extrapolated_namespaces(registry) == EXPECTED
    legacy = [c["namespace"] for c in registry["cohorts"] if c["cohort"] == "legacy"]
    assert legacy == ["REF2"]


def test_registry_rejects_duplicates_bad_modes_and_missing_maps(tmp_path: Path):
    def write(cohorts) -> Path:
        path = tmp_path / "registry.json"
        path.write_text(
            json.dumps(
                {"schema_version": "discovery-routing-cohorts-v1", "cohorts": cohorts}
            ),
            encoding="utf-8",
        )
        return path

    ok = {"namespace": "REF4", "cohort": "extrapolated",
          "identity_mode": "private_sibling", "source_map": "map.json"}
    (tmp_path / "map.json").write_text("{}", encoding="utf-8")
    load_cohort_registry(write([ok]))

    with pytest.raises(ValueError, match="duplicate"):
        load_cohort_registry(write([ok, ok]))
    with pytest.raises(ValueError, match="legacy or extrapolated"):
        load_cohort_registry(write([{**ok, "cohort": "other"}]))
    with pytest.raises(ValueError, match="identity_mode"):
        load_cohort_registry(write([{**ok, "identity_mode": "minted"}]))
    with pytest.raises(ValueError, match="not found"):
        load_cohort_registry(write([{**ok, "source_map": "absent.json"}]))
    with pytest.raises(ValueError, match="must not set identity_mode"):
        load_cohort_registry(
            write([{"namespace": "REF2", "cohort": "legacy",
                    "identity_mode": "private_sibling"}])
        )


def test_classification_covers_legacy_extrapolated_private_and_unknown():
    registry = load_cohort_registry(REGISTRY)
    assert classify_work_id("REF2:ja2_rambam_moreh", registry) == ("REF2", "legacy")
    assert classify_work_id("REF4:sifra", registry) == ("REF4", "extrapolated")
    assert classify_work_id("REF5:machberet_menachem", registry) == ("REF5", "extrapolated")
    assert classify_work_id("REF6:mt_sefer_ahavah", registry) == ("REF6", "extrapolated")
    assert classify_work_id("w000926", registry) is None
    assert classify_work_id("REFORM:not-a-generation", registry) is None
    with pytest.raises(ValueError, match="REF7"):
        classify_work_id("REF7:new_thing", registry)
