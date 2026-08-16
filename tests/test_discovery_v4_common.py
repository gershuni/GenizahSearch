import argparse
import csv
import json
import sqlite3
from pathlib import Path

import pytest

from scripts.discovery_v4_common import (
    clean_hebrew,
    compact_stream,
    load_source_config,
    normalize_title,
    sha256_file,
)
from scripts.discovery_v4_fetch_sources import (
    _schema_leaf_refs,
    hebrew_numeral,
    select_chapter_links,
    visible_text,
)
from scripts.discovery_v4_build_reference import (
    _legacy_reference_metadata_key,
    raw_reference_id,
    select_units,
)
from scripts.discovery_v4_match import shadow_rows, staged_table
from scripts.discovery_v4_reconcile import (
    canonical_map,
    curated_content_hash,
    next_work_ids,
    run as reconcile_v4,
)


def test_normalize_title_folds_marks_punctuation_and_spacing():
    assert normalize_title('  רש״י_עַל--התורה ') == normalize_title("רש'י על התורה")


def test_clean_hebrew_preserves_final_letters_and_drops_marks():
    assert clean_hebrew("אָב־גָּד 12, end") == "אב גד"


def test_compact_stream_matches_established_final_letter_fold():
    assert compact_stream("מלך עולם מן") == "מלכעולממנ"


def test_source_config_rejects_duplicate_target(tmp_path: Path):
    path = tmp_path / "sources.json"
    path.write_text(
        json.dumps(
            {
                "schema_version": "discovery-v4-sources-v1",
                "sources": [
                    {
                        "key": "one",
                        "provider": "sefaria",
                        "source_ref": "One",
                        "mappings": [{"target_work_id": "w000001"}],
                    },
                    {
                        "key": "two",
                        "provider": "hewikisource",
                        "source_ref": "Two",
                        "mappings": [{"target_work_id": "w000001"}],
                    },
                ],
            }
        ),
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="appears more than once"):
        load_source_config(path)


def test_hebrew_numeral_and_chapter_link_selection_reject_nested_paragraphs():
    links = [
        {"ns": 0, "title": "אסתר רבה א"},
        {"ns": 0, "title": "אסתר רבה א א"},
        {"ns": 0, "title": "אסתר רבה י"},
    ]
    assert hebrew_numeral("טו") == 15
    assert select_chapter_links(links, "אסתר רבה ") == [
        (1, "אסתר רבה א"),
        (10, "אסתר רבה י"),
    ]


def test_visible_text_drops_edit_controls_and_scripts():
    value = (
        '<div>גוף <span class="mw-editsection">עריכה</span>'
        '<script>פסול</script><p>המשך</p></div>'
    )
    assert visible_text(value) == "גוף המשך"


def test_schema_leaf_refs_does_not_duplicate_root_title():
    schema = {
        "title": "Root",
        "nodes": [
            {
                "title": "Leaf",
                "heTitle": "עלה",
                "nodeType": "JaggedArrayNode",
            }
        ],
    }
    assert _schema_leaf_refs("Root", schema) == [("Root, Leaf", "Leaf", "עלה")]


def test_reference_id_and_range_slice_are_stable():
    mapping = {"target_work_id": "w000001", "chapter_range": [2, 3]}
    units = [{"ordinal": value} for value in range(1, 5)]
    assert raw_reference_id("parent", mapping, 2) == "REF4:parent:2_3"
    assert [row["ordinal"] for row in select_units(units, mapping)] == [2, 3]


def test_reference_metadata_key_is_derived_from_frozen_mapping_shape():
    corpus = [
        {
            "id": "base",
            "genre": "g",
            "legacy_private_field": "",
            "stream": "abc",
        }
    ]
    original_key = list(corpus[0])[2]
    derived_key = _legacy_reference_metadata_key(corpus)
    assert derived_key == "legacy_private_field"
    assert derived_key is not original_key

    with pytest.raises(ValueError, match="position drift"):
        _legacy_reference_metadata_key(
            [{"id": "base", "genre": "g", "stream": "abc"}]
        )


def test_v4_staged_table_rejects_sql_metacharacters():
    assert staged_table("v4_full") == "track1_matches_pilot_v4_full_live"
    with pytest.raises(ValueError, match="tag"):
        staged_table("v4;drop")


def test_shadow_rows_preserves_distinct_spans_and_shadows_worse_overlap():
    rows = [
        (1, "p1", "better", 0.10, json.dumps([[10, 110, 0.10]])),
        (2, "p1", "worse", 0.14, json.dumps([[20, 100, 0.14]])),
        (3, "p1", "distinct", 0.30, json.dumps([[200, 260, 0.30]])),
    ]
    assert shadow_rows(rows) == [("better", 2)]


def test_shadow_rows_uses_worse_span_as_overlap_denominator():
    rows = [
        (1, "p1", "better", 0.10, json.dumps([[0, 50, 0.10]])),
        (2, "p1", "worse", 0.14, json.dumps([[20, 70, 0.14]])),
    ]
    # 30/50 meets the frozen 0.6 threshold exactly.
    assert shadow_rows(rows) == [("better", 2)]


def test_v4_reconciliation_mints_after_the_persisted_namespace():
    assert next_work_ids({"raw:one": "w000001", "raw:two": "w000003"}, 2) == [
        "w000004",
        "w000005",
    ]


def test_v4_reconciliation_rejects_transitive_base_merges():
    document = {
        "merges": [
            {
                "members_w": ["w000001", "w000002"],
                "canonical_w": "w000002",
                "owner_verdict": "approve",
            },
            {
                "members_w": ["w000001", "w000003"],
                "canonical_w": "w000003",
                "owner_verdict": "approve",
            },
        ]
    }
    with pytest.raises(ValueError, match="disjoint"):
        canonical_map(document)


def test_curated_content_hash_depends_only_on_payload():
    rows = [{"canonical_work_id": "w000001", "domain_leaf": "One"}]
    assert curated_content_hash(rows) == curated_content_hash(list(rows))
    assert curated_content_hash(rows).startswith("sha256:")


def test_v4_reconciliation_mints_and_merges_only_a_live_public_reference(
    tmp_path: Path,
):
    manifest = tmp_path / "reference-manifest.json"
    manifest.write_text(
        json.dumps(
            {
                "schema_version": "discovery-v4-reference-manifest-v1",
                "acquisition_manifest_sha256": "a" * 64,
                "entries": [
                    {
                        "raw_reference_id": "REF4:one",
                        "target_private_work_id": "w000001",
                        "title": "Public One",
                    },
                    {
                        "raw_reference_id": "REF4:no_live_match",
                        "target_private_work_id": "w000002",
                        "title": "No Match",
                    },
                ],
            }
        ),
        encoding="utf-8",
    )
    crosswalk = tmp_path / "crosswalk.json"
    crosswalk.write_text(
        json.dumps({"PRIVATE:one": "w000001", "PRIVATE:two": "w000002"}),
        encoding="utf-8",
    )
    approved = tmp_path / "approved.csv"
    with approved.open("w", encoding="utf-8-sig", newline="") as stream:
        writer = csv.DictWriter(
            stream,
            fieldnames=(
                "work_id",
                "candidate_title",
                "author",
                "genre",
                "source_label",
                "confidence_basis",
                "tier_a_witnesses",
                "claim_count",
                "owner_title",
                "owner_verdict",
                "owner_note",
            ),
        )
        writer.writeheader()
        for work_id, title in (("w000001", "Private One"), ("w000002", "Private Two")):
            writer.writerow(
                {
                    "work_id": work_id,
                    "candidate_title": title,
                    "author": "Author",
                    "genre": "Genre",
                    "source_label": "msource",
                    "confidence_basis": "none-owner-supplies",
                    "tier_a_witnesses": "1",
                    "claim_count": "1",
                    "owner_title": "",
                    "owner_verdict": "approve",
                    "owner_note": "",
                }
            )
    merges = tmp_path / "merges.json"
    merges.write_text(json.dumps({"merges": []}), encoding="utf-8")
    domain_rows = [
        {
            "canonical_work_id": work_id,
            "domain_parent": "Parent",
            "domain_leaf": "Leaf",
            "confidence": "high",
            "provenance": "test",
        }
        for work_id in ("w000001", "w000002")
    ]
    domains = tmp_path / "domains.json"
    domains.write_text(
        json.dumps(
            {
                "artifact": "work_domains",
                "artifact_version": "v1",
                "assignments": domain_rows,
                "content_hash": curated_content_hash(domain_rows),
            }
        ),
        encoding="utf-8",
    )
    match_db = tmp_path / "matches.db"
    with sqlite3.connect(match_db) as conn:
        conn.execute(
            "CREATE TABLE track1_matches (work_id TEXT, title TEXT, author TEXT, "
            "genre TEXT, sys_id TEXT, page_id TEXT, shadowed_by TEXT, "
            "ref_spans_json TEXT)"
        )
        conn.execute(
            "INSERT INTO track1_matches VALUES "
            "('REF4:one', 'Public One', '', 'Genre', 's1', 'p1', NULL, '[[0,5]]')"
        )

    out_crosswalk = tmp_path / "out-crosswalk.json"
    out_approved = tmp_path / "out-approved.csv"
    out_merges = tmp_path / "out-merges.json"
    out_domains = tmp_path / "out-domains.json"
    report = reconcile_v4(
        argparse.Namespace(
            reference_manifest=str(manifest),
            reference_manifest_sha256=sha256_file(manifest),
            match_db=str(match_db),
            base_crosswalk=str(crosswalk),
            base_crosswalk_sha256=sha256_file(crosswalk),
            base_approved=str(approved),
            base_approved_sha256=sha256_file(approved),
            base_merges=str(merges),
            base_merges_sha256=sha256_file(merges),
            base_work_domains=str(domains),
            base_work_domains_sha256=sha256_file(domains),
            output_crosswalk=str(out_crosswalk),
            output_approved=str(out_approved),
            output_merges=str(out_merges),
            output_work_domains=str(out_domains),
            report=None,
        )
    )

    assert report["live_public_reference_count"] == 1
    assert report["quarantined_or_unmatched_reference_count"] == 1
    assert report["raw_to_opaque"] == {"REF4:one": "w000003"}
    assert json.loads(out_crosswalk.read_text(encoding="utf-8"))["REF4:one"] == "w000003"
    merged = json.loads(out_merges.read_text(encoding="utf-8"))
    assert merged["v4_public_reference_canonical_ids"] == ["w000003"]
    assert merged["merges"] == [
        {
            "canonical_w": "w000003",
            "members_w": ["w000001", "w000003"],
            "owner_verdict": "approve",
        }
    ]
    domain_doc = json.loads(out_domains.read_text(encoding="utf-8"))
    copied = next(
        row for row in domain_doc["assignments"]
        if row["canonical_work_id"] == "w000003"
    )
    assert copied["domain_parent"] == "Parent"
    assert copied["provenance"] == "v4-public-reference-inherits:w000001"
