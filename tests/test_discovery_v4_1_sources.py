"""V4.1 source map and the REF5 namespace it appends under.

The V4.1 plan (docs/specs/discovery-v4.1-public-source-and-r-shadow-plan.md)
adds ten publicly licensed reference streams without touching a single REF4 raw
id or either byte-stable prefix.  These tests pin the two properties that makes
possible: the namespace is an explicit input rather than a literal, and the
hash-pinned source maps are byte-identical in every working tree.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts.discovery_v4_build_reference import (
    _extend_locus,
    coverage_extension_key,
    raw_reference_id,
)
from scripts.discovery_v4_common import (
    load_source_config,
    raw_id_prefix,
    reference_namespace,
    resolve_namespace,
    sha256_file,
    source_target_ids,
)

SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
V4_MAP = SCRIPTS / "discovery_v4_sources.json"
V4_1_MAP = SCRIPTS / "discovery_v4_1_sources.json"

# The SHA-256 the V4 acquisition and reference manifests record for their source
# map.  It is checked by every consuming stage, so the file is frozen: an edit
# invalidates the whole V4 chain rather than updating it.
V4_SOURCE_MAP_SHA256 = (
    "6f21efcdeeefa22ba167bc52a5ade9beb997b4b10edc2f9ca9e4f3fa3a7669c6"
)


def _write_map(path: Path, **overrides) -> Path:
    document = {
        "schema_version": "discovery-v4-sources-v1",
        "sources": [
            {
                "key": "one",
                "provider": "sefaria",
                "source_ref": "One",
                "mappings": [{"target_work_id": "w000001"}],
            }
        ],
    }
    document.update(overrides)
    path.write_text(json.dumps(document), encoding="utf-8")
    return path


def test_v4_source_map_still_hashes_to_its_pinned_value():
    # core.autocrlf rewrote this file to CRLF once already, which silently broke
    # the pin; .gitattributes now holds it at LF.
    assert b"\r\n" not in V4_MAP.read_bytes()
    assert sha256_file(V4_MAP) == V4_SOURCE_MAP_SHA256


def test_v4_1_source_map_is_stored_without_carriage_returns():
    assert b"\r\n" not in V4_1_MAP.read_bytes()


def test_v4_1_map_declares_ref5_and_carries_exactly_ten_records():
    config = load_source_config(V4_1_MAP)
    assert reference_namespace(config) == "REF5"
    assert len(config["sources"]) == 10
    assert len(source_target_ids(config)) == 10
    assert {source["provider"] for source in config["sources"]} == {
        "sefaria",
        "hewikisource",
    }


def test_v4_1_targets_no_work_that_v4_already_maps():
    v4_targets = source_target_ids(load_source_config(V4_MAP))
    v4_1_targets = source_target_ids(load_source_config(V4_1_MAP))
    assert v4_targets & v4_1_targets == set()


def test_v4_1_inherits_v4s_license_allowlist_and_size_floor():
    v4 = load_source_config(V4_MAP)
    v4_1 = load_source_config(V4_1_MAP)
    assert v4_1["license_allowlist"] == v4["license_allowlist"]
    assert v4_1["minimum_hebrew_letters"] == v4["minimum_hebrew_letters"]


def test_v4_1_excluded_sources_are_recorded_rather_than_merely_absent():
    config = json.loads(V4_1_MAP.read_text(encoding="utf-8"))
    quarantined = {row["source_ref"] for row in config["quarantined_pending_license"]}
    rejected = {row["source_ref"] for row in config["rejected"]}
    queued = {row["source_ref"] for row in config["reconciliation_queue"]}
    assert quarantined == {"Bamidbar Rabbah", "Rabbeinu Chananel on Shabbat"}
    assert rejected == {"מדרש תדשא", "מדרש תנחומא", "עשרת הדיברות"}
    assert queued == {"אליהו זוטא", "אליהו רבה"}
    # The quarantined Sefaria edition and the accepted Wikisource one name the
    # same work; only one of them may be a source.
    assert "w000496" in source_target_ids(load_source_config(V4_1_MAP))


def test_reference_ids_take_the_namespace_and_v4_keeps_its_literal():
    mapping = {"target_work_id": "w000001", "chapter_range": [2, 3]}
    assert raw_reference_id("parent", mapping, 1) == "REF4:parent"
    assert raw_reference_id("parent", mapping, 2) == "REF4:parent:2_3"
    assert raw_reference_id("parent", mapping, 1, "REF5") == "REF5:parent"
    assert raw_reference_id("parent", mapping, 2, "REF5") == "REF5:parent:2_3"


def test_raw_id_prefix_rejects_a_namespace_that_is_not_a_ref_generation():
    assert raw_id_prefix("REF5") == "REF5:"
    for bad in ("", "REF", "ref5", "REF5:", "REF5x", "REF5' OR 1=1--"):
        with pytest.raises(ValueError, match="namespace"):
            raw_id_prefix(bad)


def test_namespace_defaults_to_ref4_and_a_disagreeing_flag_is_refused(tmp_path: Path):
    v4_style = load_source_config(_write_map(tmp_path / "v4.json"))
    assert resolve_namespace(v4_style, None) == "REF4"
    assert resolve_namespace(v4_style, "REF4") == "REF4"
    with pytest.raises(ValueError, match="disagrees"):
        resolve_namespace(v4_style, "REF5")

    v5_style = load_source_config(
        _write_map(tmp_path / "v5.json", reference_namespace="REF5")
    )
    assert resolve_namespace(v5_style, None) == "REF5"
    with pytest.raises(ValueError, match="disagrees"):
        resolve_namespace(v5_style, "REF4")


def test_source_config_rejects_a_malformed_namespace(tmp_path: Path):
    for bad in ("REF", "ref5", 5, None):
        path = _write_map(tmp_path / f"bad-{bad}.json", reference_namespace=bad)
        with pytest.raises(ValueError, match="reference_namespace"):
            load_source_config(path)


def test_coverage_extension_key_keeps_v4s_literal_and_scopes_later_namespaces():
    assert coverage_extension_key("REF4") == "v4_extension"
    assert coverage_extension_key("REF5") == "ref5_extension"


def test_extending_locus_twice_under_one_namespace_is_refused(tmp_path: Path):
    coverage = tmp_path / "coverage.json"
    coverage.write_text(
        json.dumps(
            {
                "invariant_problems": [],
                "works_with_units": 1,
                "units_total": 1,
                "v4_extension": {"added_works_with_units": 43},
            }
        ),
        encoding="utf-8",
    )
    kwargs = {
        "base_db": tmp_path / "absent.db",
        "base_coverage": coverage,
        "output_db": tmp_path / "out.db",
        "output_coverage": tmp_path / "out.json",
        "new_reference_hash": "0" * 64,
        "reference_entries": [],
    }
    # REF4 has already run against this coverage; re-running would overwrite the
    # record of what V4 added.
    with pytest.raises(ValueError, match="already records a REF4 extension"):
        _extend_locus(**kwargs, namespace="REF4")
    # REF5 has not, so the guard lets it through to the real work (which fails
    # here only because the fixture has no locus database to copy).
    with pytest.raises(FileNotFoundError):
        _extend_locus(**kwargs, namespace="REF5")
