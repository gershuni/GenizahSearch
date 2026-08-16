# -*- coding: utf-8 -*-
"""Tests for the C5 public-first identity artifact (discovery-v4.2 plan),
its hookup into scripts/discovery_v4_reconcile.py's identity_mode-aware
reconcile step, and the consumer-side merge-contract widening in
scripts/build_discovery_sidecar.py::load_canonical_merges.

EVERY fixture here is FABRICATED, masking-clean test data: synthetic
``pf-####`` identity keys, synthetic Hebrew titles (e.g. "חיבור בדוי א"),
neutral ``w000xxx`` opaque work ids, and obviously-synthetic raw ids
(``REF4:...`` / ``REF6:...``). Nothing from ``same_work_spike/`` is read or
referenced.
"""
from __future__ import annotations

import argparse
import csv
import json
import sqlite3
from pathlib import Path

import pytest

from scripts import build_discovery_sidecar as sidecar_build
from scripts.discovery_public_first_identity import (
    PublicFirstIdentityError,
    SCHEMA_VERSION,
    content_hash_for_entries,
    load_public_first_artifact,
)
from scripts.discovery_v4_common import sha256_file
from scripts.discovery_v4_reconcile import curated_content_hash, run as reconcile_v4

# ---------------------------------------------------------------------------
# Shared fixture helpers
# ---------------------------------------------------------------------------

APPROVED_FIELDS = (
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
)


def _write_json(path, obj) -> str:
    Path(path).write_text(json.dumps(obj, ensure_ascii=False), encoding="utf-8")
    return str(path)


def _write_approved_csv(path, rows) -> str:
    with Path(path).open("w", encoding="utf-8-sig", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=APPROVED_FIELDS)
        writer.writeheader()
        writer.writerows(rows)
    return str(path)


def _write_match_db(path, rows) -> None:
    with sqlite3.connect(path) as conn:
        conn.execute(
            "CREATE TABLE track1_matches (work_id TEXT, title TEXT, author TEXT, "
            "genre TEXT, sys_id TEXT, page_id TEXT, shadowed_by TEXT, "
            "ref_spans_json TEXT)"
        )
        for row in rows:
            conn.execute(
                "INSERT INTO track1_matches VALUES (?, ?, ?, ?, ?, ?, ?, ?)", row
            )


def _pf_entry(identity_key: str, verdict: str = "approve", **overrides) -> dict:
    base = {
        "identity_key": identity_key,
        "title_he": "חיבור בדוי א" if verdict == "approve" else "",
        "author": "",
        "genre": "הלכה" if verdict == "approve" else "",
        "domain_parent": "הלכה" if verdict == "approve" else "",
        "domain_leaf": "כללי" if verdict == "approve" else "",
        "provider": "hewikisource" if verdict == "approve" else "",
        "source_ref": "Fabricated Ref" if verdict == "approve" else "",
        "license": "Public Domain" if verdict == "approve" else "",
        "verdict": verdict,
        "note": "",
    }
    base.update(overrides)
    return base


def _pf_artifact_doc(entries, ruled_on="2026-08-16") -> dict:
    return {
        "schema_version": SCHEMA_VERSION,
        "ruled_on": ruled_on,
        "entries": entries,
        "content_hash": content_hash_for_entries(entries),
    }


def _base_crosswalk() -> dict:
    return {"PRIVATE:one": "w000001", "PRIVATE:two": "w000002"}


def _base_approved_rows() -> list:
    return [
        {
            "work_id": wid,
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
        for wid, title in (("w000001", "Private One"), ("w000002", "Private Two"))
    ]


def _base_domains() -> dict:
    rows = [
        {
            "canonical_work_id": wid,
            "domain_parent": "Parent",
            "domain_leaf": "Leaf",
            "confidence": "high",
            "provenance": "test",
        }
        for wid in ("w000001", "w000002")
    ]
    return {
        "artifact": "work_domains",
        "artifact_version": "v1",
        "assignments": rows,
        "content_hash": curated_content_hash(rows),
    }


def _reconcile_namespace(
    tmp_path,
    *,
    entries,
    match_rows,
    pf_entries=None,
    crosswalk=None,
    approved_rows=None,
    merges_doc=None,
    domains_doc=None,
    omit_pf_sha256=False,
    prefix="",
):
    manifest = {
        "schema_version": "discovery-v4-reference-manifest-v1",
        "acquisition_manifest_sha256": "a" * 64,
        "entries": entries,
    }
    manifest_path = _write_json(tmp_path / f"{prefix}manifest.json", manifest)
    crosswalk_path = _write_json(
        tmp_path / f"{prefix}crosswalk.json", crosswalk or _base_crosswalk()
    )
    approved_path = _write_approved_csv(
        tmp_path / f"{prefix}approved.csv", approved_rows or _base_approved_rows()
    )
    merges_path = _write_json(
        tmp_path / f"{prefix}merges.json", merges_doc or {"merges": []}
    )
    domains_path = _write_json(
        tmp_path / f"{prefix}domains.json", domains_doc or _base_domains()
    )
    match_db = tmp_path / f"{prefix}matches.db"
    _write_match_db(str(match_db), match_rows)

    kwargs = dict(
        reference_manifest=manifest_path,
        reference_manifest_sha256=sha256_file(manifest_path),
        match_db=str(match_db),
        base_crosswalk=crosswalk_path,
        base_crosswalk_sha256=sha256_file(crosswalk_path),
        base_approved=approved_path,
        base_approved_sha256=sha256_file(approved_path),
        base_merges=merges_path,
        base_merges_sha256=sha256_file(merges_path),
        base_work_domains=domains_path,
        base_work_domains_sha256=sha256_file(domains_path),
        output_crosswalk=str(tmp_path / f"{prefix}out-crosswalk.json"),
        output_approved=str(tmp_path / f"{prefix}out-approved.csv"),
        output_merges=str(tmp_path / f"{prefix}out-merges.json"),
        output_work_domains=str(tmp_path / f"{prefix}out-domains.json"),
        report=None,
    )
    if pf_entries is not None:
        pf_path = _write_json(
            tmp_path / f"{prefix}pf.json", _pf_artifact_doc(pf_entries)
        )
        kwargs["public_first_artifact"] = pf_path
        if not omit_pf_sha256:
            kwargs["public_first_artifact_sha256"] = sha256_file(pf_path)
    return argparse.Namespace(**kwargs)


# ===========================================================================
# discovery_public_first_identity.py -- artifact loader validation battery
# ===========================================================================


def test_public_first_loader_accepts_valid_artifact(tmp_path):
    entries = [
        _pf_entry("pf-0001"),
        _pf_entry("pf-0002", verdict="reject", note="not distinctive enough"),
        _pf_entry("pf-0003", verdict="defer", note="owner wants a second look"),
    ]
    path = _write_json(tmp_path / "pf.json", _pf_artifact_doc(entries))
    doc = load_public_first_artifact(path, sha256=sha256_file(path))
    assert set(doc["entries_by_key"]) == {"pf-0001", "pf-0002", "pf-0003"}
    assert doc["entries_by_key"]["pf-0001"]["verdict"] == "approve"
    assert doc["entries_by_key"]["pf-0002"]["verdict"] == "reject"
    assert doc["entries_by_key"]["pf-0003"]["verdict"] == "defer"


def test_public_first_loader_sha256_mismatch_rejected(tmp_path):
    path = _write_json(tmp_path / "pf.json", _pf_artifact_doc([_pf_entry("pf-0001")]))
    with pytest.raises(PublicFirstIdentityError, match="SHA-256"):
        load_public_first_artifact(path, sha256="deadbeef" * 8)


def test_public_first_loader_duplicate_identity_key_rejected(tmp_path):
    entries = [_pf_entry("pf-0001"), _pf_entry("pf-0001", verdict="reject")]
    path = _write_json(tmp_path / "pf.json", _pf_artifact_doc(entries))
    with pytest.raises(PublicFirstIdentityError, match="duplicate identity_key"):
        load_public_first_artifact(path)


def test_public_first_loader_bad_identity_key_syntax_rejected(tmp_path):
    path = _write_json(tmp_path / "pf.json", _pf_artifact_doc([_pf_entry("pf-1")]))
    with pytest.raises(PublicFirstIdentityError, match="identity_key"):
        load_public_first_artifact(path)


def test_public_first_loader_invalid_verdict_rejected(tmp_path):
    path = _write_json(
        tmp_path / "pf.json", _pf_artifact_doc([_pf_entry("pf-0001", verdict="maybe")])
    )
    with pytest.raises(PublicFirstIdentityError, match="verdict"):
        load_public_first_artifact(path)


def test_public_first_loader_invalid_provider_rejected(tmp_path):
    path = _write_json(
        tmp_path / "pf.json",
        _pf_artifact_doc([_pf_entry("pf-0001", provider="wikipedia")]),
    )
    with pytest.raises(PublicFirstIdentityError, match="provider"):
        load_public_first_artifact(path)


@pytest.mark.parametrize(
    "field",
    ["title_he", "genre", "domain_parent", "domain_leaf", "provider", "source_ref", "license"],
)
def test_public_first_loader_approved_entry_requires_nonempty_field(tmp_path, field):
    path = _write_json(
        tmp_path / "pf.json",
        _pf_artifact_doc([_pf_entry("pf-0001", **{field: ""})]),
    )
    with pytest.raises(PublicFirstIdentityError, match="empty required field"):
        load_public_first_artifact(path)


def test_public_first_loader_approved_entry_author_may_be_empty(tmp_path):
    path = _write_json(
        tmp_path / "pf.json", _pf_artifact_doc([_pf_entry("pf-0001", author="")])
    )
    load_public_first_artifact(path)  # must not raise


def test_public_first_loader_approved_title_must_contain_hebrew(tmp_path):
    path = _write_json(
        tmp_path / "pf.json",
        _pf_artifact_doc([_pf_entry("pf-0001", title_he="Latin Only Title")]),
    )
    with pytest.raises(PublicFirstIdentityError, match="Hebrew"):
        load_public_first_artifact(path)


def test_public_first_loader_masking_gate_rejects_hashlike_string(tmp_path):
    path = _write_json(
        tmp_path / "pf.json",
        _pf_artifact_doc([_pf_entry("pf-0001", note="see " + "a" * 40)]),
    )
    with pytest.raises(PublicFirstIdentityError, match="masking gate"):
        load_public_first_artifact(path)


def test_public_first_loader_masking_gate_rejects_restricted_token(tmp_path):
    path = _write_json(
        tmp_path / "pf.json",
        _pf_artifact_doc(
            [_pf_entry("pf-0001", note="cross-checked against same_work_spike review")]
        ),
    )
    with pytest.raises(PublicFirstIdentityError, match="masking gate"):
        load_public_first_artifact(path)


def test_public_first_loader_content_hash_mismatch_rejected(tmp_path):
    doc = _pf_artifact_doc([_pf_entry("pf-0001")])
    doc["content_hash"] = "sha256:" + "0" * 64
    path = _write_json(tmp_path / "pf.json", doc)
    with pytest.raises(PublicFirstIdentityError, match="content_hash"):
        load_public_first_artifact(path)


def test_public_first_loader_top_level_key_drift_rejected(tmp_path):
    doc = _pf_artifact_doc([_pf_entry("pf-0001")])
    doc["extra_key"] = 1
    path = _write_json(tmp_path / "pf.json", doc)
    with pytest.raises(PublicFirstIdentityError, match="key drift"):
        load_public_first_artifact(path)


def test_public_first_loader_entry_key_drift_rejected(tmp_path):
    entries = [_pf_entry("pf-0001")]
    entries[0]["extra_field"] = "x"
    path = _write_json(tmp_path / "pf.json", _pf_artifact_doc(entries))
    with pytest.raises(PublicFirstIdentityError, match="key drift"):
        load_public_first_artifact(path)


def test_public_first_loader_wrong_schema_version_rejected(tmp_path):
    doc = _pf_artifact_doc([_pf_entry("pf-0001")])
    doc["schema_version"] = "some-other-v1"
    path = _write_json(tmp_path / "pf.json", doc)
    with pytest.raises(PublicFirstIdentityError, match="schema_version"):
        load_public_first_artifact(path)


def test_public_first_loader_bad_ruled_on_rejected(tmp_path):
    doc = _pf_artifact_doc([_pf_entry("pf-0001")])
    doc["ruled_on"] = "16 Aug 2026"
    path = _write_json(tmp_path / "pf.json", doc)
    with pytest.raises(PublicFirstIdentityError, match="ruled_on"):
        load_public_first_artifact(path)


def test_public_first_loader_duplicate_json_key_rejected(tmp_path):
    raw = (
        '{"schema_version": "%s", "ruled_on": "2026-08-16", '
        '"entries": [], "entries": []}' % SCHEMA_VERSION
    )
    path = tmp_path / "pf.json"
    path.write_text(raw, encoding="utf-8")
    with pytest.raises(PublicFirstIdentityError, match="duplicate JSON key"):
        load_public_first_artifact(str(path))


def test_public_first_loader_empty_entries_rejected(tmp_path):
    path = _write_json(tmp_path / "pf.json", _pf_artifact_doc([]))
    with pytest.raises(PublicFirstIdentityError, match="non-empty"):
        load_public_first_artifact(path)


def test_public_first_loader_file_not_found_rejected(tmp_path):
    with pytest.raises(PublicFirstIdentityError, match="not found"):
        load_public_first_artifact(str(tmp_path / "nope.json"))


# ===========================================================================
# discovery_v4_reconcile.py -- identity_mode-aware reconcile (happy path)
# ===========================================================================


def test_reconcile_mixed_manifest_mints_standalone_public_first_and_sibling(tmp_path):
    entries = [
        {
            "raw_reference_id": "REF4:one",
            "identity_mode": "private_sibling",
            "target_private_work_id": "w000001",
            "title": "Public One",
        },
        {
            "raw_reference_id": "REF4:no_live_match",
            "identity_mode": "private_sibling",
            "target_private_work_id": "w000002",
            "title": "No Match",
        },
        {
            "raw_reference_id": "REF6:pf_live",
            "identity_mode": "public_first",
            "identity_key": "pf-0001",
        },
        {
            "raw_reference_id": "REF6:pf_unmatched",
            "identity_mode": "public_first",
            "identity_key": "pf-0002",
        },
    ]
    match_rows = [
        ("REF4:one", "Public One", "", "Genre", "s1", "p1", None, "[[0,5]]"),
        (
            "REF6:pf_live",
            "Provider Title",
            "Provider Author",
            "ProviderGenre",
            "s2",
            "p2",
            None,
            "[[0,5]]",
        ),
    ]
    pf_entries = [
        _pf_entry("pf-0001", title_he="חיבור בדוי א", provider="hewikisource"),
        # approved, referenced by the manifest, but NEVER live -> reported unmatched
        _pf_entry("pf-0002", title_he="חיבור בדוי ב", provider="hewikisource"),
        # approved, not referenced by the manifest AT ALL -> also reported unmatched
        _pf_entry("pf-0003", title_he="חיבור בדוי ג", provider="sefaria"),
    ]
    ns = _reconcile_namespace(
        tmp_path, entries=entries, match_rows=match_rows, pf_entries=pf_entries
    )
    report = reconcile_v4(ns)

    assert report["raw_to_opaque"] == {"REF4:one": "w000003", "REF6:pf_live": "w000004"}
    assert report["live_public_reference_count"] == 1
    assert report["quarantined_or_unmatched_reference_count"] == 2
    assert report["live_public_first_count"] == 1
    assert report["public_first_standalone_canonical_ids"] == ["w000004"]
    assert report["public_first_unmatched_approved"] == [
        {"identity_key": "pf-0002", "verdict": "approve"},
        {"identity_key": "pf-0003", "verdict": "approve"},
    ]

    merges_doc = json.loads(Path(ns.output_merges).read_text(encoding="utf-8"))
    assert merges_doc["merges"] == [
        {
            "canonical_w": "w000003",
            "members_w": ["w000001", "w000003"],
            "owner_verdict": "approve",
        }
    ]
    assert merges_doc["public_first_standalone_canonical_ids"] == ["w000004"]
    assert merges_doc["v4_public_reference_canonical_ids"] == ["w000003"]
    all_members = {m for group in merges_doc["merges"] for m in group["members_w"]}
    all_canon = {group["canonical_w"] for group in merges_doc["merges"]}
    assert "w000004" not in all_members
    assert "w000004" not in all_canon

    domains_doc = json.loads(Path(ns.output_work_domains).read_text(encoding="utf-8"))
    pf_domain_row = next(
        r for r in domains_doc["assignments"] if r["canonical_work_id"] == "w000004"
    )
    assert pf_domain_row["domain_parent"] == "הלכה"
    assert pf_domain_row["domain_leaf"] == "כללי"
    assert pf_domain_row["provenance"] == "public-first:pf-0001"
    assert pf_domain_row["confidence"] == "high"

    with Path(ns.output_approved).open(encoding="utf-8-sig", newline="") as stream:
        approved_rows = list(csv.DictReader(stream))
    pf_row = next(r for r in approved_rows if r["work_id"] == "w000004")
    assert pf_row["candidate_title"] == "חיבור בדוי א"
    assert pf_row["author"] == ""
    assert pf_row["genre"] == "הלכה"
    assert pf_row["source_label"] == "hewikisource"
    assert pf_row["owner_verdict"] == "approve"
    assert "pf-0001" in pf_row["owner_note"]
    sibling_row = next(r for r in approved_rows if r["work_id"] == "w000003")
    assert sibling_row["source_label"] == "sefaria"  # private_sibling KEEPS the hardcode


# ===========================================================================
# discovery_v4_reconcile.py -- hard-error paths
# ===========================================================================


def test_reconcile_rejects_rejected_identity_key(tmp_path):
    ns = _reconcile_namespace(
        tmp_path,
        entries=[
            {
                "raw_reference_id": "REF6:pf_x",
                "identity_mode": "public_first",
                "identity_key": "pf-0004",
            }
        ],
        match_rows=[("REF6:pf_x", "T", "A", "G", "s1", "p1", None, "[[0,5]]")],
        pf_entries=[_pf_entry("pf-0004", verdict="reject", note="not distinctive")],
    )
    with pytest.raises(ValueError, match="rejected, or deferred"):
        reconcile_v4(ns)


def test_reconcile_rejects_deferred_identity_key(tmp_path):
    ns = _reconcile_namespace(
        tmp_path,
        entries=[
            {
                "raw_reference_id": "REF6:pf_x",
                "identity_mode": "public_first",
                "identity_key": "pf-0005",
            }
        ],
        match_rows=[("REF6:pf_x", "T", "A", "G", "s1", "p1", None, "[[0,5]]")],
        pf_entries=[_pf_entry("pf-0005", verdict="defer", note="pending")],
    )
    with pytest.raises(ValueError, match="rejected, or deferred"):
        reconcile_v4(ns)


def test_reconcile_rejects_identity_key_absent_from_artifact(tmp_path):
    ns = _reconcile_namespace(
        tmp_path,
        entries=[
            {
                "raw_reference_id": "REF6:pf_x",
                "identity_mode": "public_first",
                "identity_key": "pf-9999",
            }
        ],
        match_rows=[("REF6:pf_x", "T", "A", "G", "s1", "p1", None, "[[0,5]]")],
        pf_entries=[_pf_entry("pf-0001")],
    )
    with pytest.raises(ValueError, match="absent, rejected, or deferred"):
        reconcile_v4(ns)


def test_reconcile_rejects_public_first_live_without_artifact(tmp_path):
    ns = _reconcile_namespace(
        tmp_path,
        entries=[
            {
                "raw_reference_id": "REF6:pf_x",
                "identity_mode": "public_first",
                "identity_key": "pf-0001",
            }
        ],
        match_rows=[("REF6:pf_x", "T", "A", "G", "s1", "p1", None, "[[0,5]]")],
        pf_entries=None,
    )
    with pytest.raises(ValueError, match="no --public-first-artifact"):
        reconcile_v4(ns)


def test_reconcile_rejects_artifact_without_sha256_pair(tmp_path):
    ns = _reconcile_namespace(
        tmp_path,
        entries=[
            {
                "raw_reference_id": "REF6:pf_x",
                "identity_mode": "public_first",
                "identity_key": "pf-0001",
            }
        ],
        match_rows=[("REF6:pf_x", "T", "A", "G", "s1", "p1", None, "[[0,5]]")],
        pf_entries=[_pf_entry("pf-0001")],
        omit_pf_sha256=True,
    )
    with pytest.raises(ValueError, match="supplied together"):
        reconcile_v4(ns)


def test_reconcile_rejects_invalid_identity_mode(tmp_path):
    ns = _reconcile_namespace(
        tmp_path,
        entries=[
            {
                "raw_reference_id": "REF4:x",
                "identity_mode": "bogus_mode",
                "target_private_work_id": "w000001",
                "title": "X",
            }
        ],
        match_rows=[("REF4:x", "T", "", "G", "s1", "p1", None, "[[0,5]]")],
    )
    with pytest.raises(ValueError, match="invalid identity_mode"):
        reconcile_v4(ns)


def test_reconcile_rejects_private_sibling_missing_target(tmp_path):
    ns = _reconcile_namespace(
        tmp_path,
        entries=[
            {
                "raw_reference_id": "REF4:x",
                "identity_mode": "private_sibling",
                "title": "X",
            }
        ],
        match_rows=[("REF4:x", "T", "", "G", "s1", "p1", None, "[[0,5]]")],
    )
    with pytest.raises(ValueError, match="missing target_private_work_id"):
        reconcile_v4(ns)


def test_reconcile_rejects_public_first_missing_identity_key(tmp_path):
    ns = _reconcile_namespace(
        tmp_path,
        entries=[{"raw_reference_id": "REF6:x", "identity_mode": "public_first"}],
        match_rows=[("REF6:x", "T", "A", "G", "s1", "p1", None, "[[0,5]]")],
        pf_entries=[_pf_entry("pf-0001")],
    )
    with pytest.raises(ValueError, match="missing identity_key"):
        reconcile_v4(ns)


# ===========================================================================
# discovery_v4_reconcile.py -- byte-compatibility of the artifact-absent path
# ===========================================================================


def test_reconcile_artifact_absent_path_is_byte_compatible(tmp_path):
    """Reruns (a copy of) the PINNED fixture from
    test_discovery_v4_common.py::test_v4_reconciliation_mints_and_merges_only_a_live_public_reference
    through the changed reconcile code and asserts the outputs are not just
    functionally equal but carry NO C5 keys at all -- omitted, not
    zero/empty -- proving the artifact-absent path is untouched."""
    manifest = {
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
    manifest_path = _write_json(tmp_path / "manifest.json", manifest)
    crosswalk_path = _write_json(tmp_path / "crosswalk.json", _base_crosswalk())
    approved_path = _write_approved_csv(tmp_path / "approved.csv", _base_approved_rows())
    merges_path = _write_json(tmp_path / "merges.json", {"merges": []})
    domains_path = _write_json(tmp_path / "domains.json", _base_domains())
    match_db = tmp_path / "matches.db"
    _write_match_db(
        str(match_db),
        [("REF4:one", "Public One", "", "Genre", "s1", "p1", None, "[[0,5]]")],
    )

    out_crosswalk = tmp_path / "out-crosswalk.json"
    out_approved = tmp_path / "out-approved.csv"
    out_merges = tmp_path / "out-merges.json"
    out_domains = tmp_path / "out-domains.json"

    # Namespace with NO public_first_artifact / public_first_artifact_sha256
    # attributes at all -- exactly the shape the PINNED test uses. Proves
    # the getattr-based fallback in run().
    report = reconcile_v4(
        argparse.Namespace(
            reference_manifest=str(manifest_path),
            reference_manifest_sha256=sha256_file(manifest_path),
            match_db=str(match_db),
            base_crosswalk=str(crosswalk_path),
            base_crosswalk_sha256=sha256_file(crosswalk_path),
            base_approved=str(approved_path),
            base_approved_sha256=sha256_file(approved_path),
            base_merges=str(merges_path),
            base_merges_sha256=sha256_file(merges_path),
            base_work_domains=str(domains_path),
            base_work_domains_sha256=sha256_file(domains_path),
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
    assert set(report) == {
        "schema_version",
        "reference_manifest_sha256",
        "source_manifest_sha256",
        "live_public_reference_count",
        "quarantined_or_unmatched_reference_count",
        "live_match_rows",
        "live_witnesses",
        "minted_first",
        "minted_last",
        "crosswalk_sha256",
        "approved_review_sha256",
        "canonical_merges_sha256",
        "canonical_merge_count",
        "work_domains_file_sha256",
        "work_domains_content_hash",
        "work_domain_assignments",
        "raw_to_opaque",
    }

    merged = json.loads(out_merges.read_text(encoding="utf-8"))
    assert merged["v4_public_reference_canonical_ids"] == ["w000003"]
    assert merged["merges"] == [
        {
            "canonical_w": "w000003",
            "members_w": ["w000001", "w000003"],
            "owner_verdict": "approve",
        }
    ]
    assert "public_first_standalone_canonical_ids" not in merged
    assert set(merged) == {
        "merges",
        "release_contract_version",
        "v4_public_reference_canonical_ids",
        "v4_source_manifest_sha256",
        "source",
    }

    with out_approved.open(encoding="utf-8-sig", newline="") as stream:
        approved_rows = list(csv.DictReader(stream))
    minted_row = next(r for r in approved_rows if r["work_id"] == "w000003")
    assert minted_row["source_label"] == "sefaria"


# ===========================================================================
# build_discovery_sidecar.py::load_canonical_merges -- public_first_standalone
# ===========================================================================


def _minimal_v4_doc(*, standalone=None, v4_members=("w030001", "w040001")):
    doc = {
        "release_contract_version": "discovery-v4-public-reference-merges-v1",
        "v4_public_reference_canonical_ids": [v4_members[-1]],
        "v4_source_manifest_sha256": "a" * 64,
        "merges": [
            {
                "members_w": list(v4_members),
                "canonical_w": v4_members[-1],
                "owner_verdict": "approve",
            }
        ],
        "dropped_by_135": [],
    }
    if standalone is not None:
        doc["public_first_standalone_canonical_ids"] = list(standalone)
    return doc


def _v4_release_merges_doc_local(*, v4_members=("w030001", "w040001")):
    """Local mirror of test_discovery_v2_bake.py::_v4_release_merges_doc (17
    approve merges total: 15 base pairs + the D-14 flip + the v4 pair) --
    reused here so the "absent standalone list stays byte-identical under the
    full release-semantics gate" test exercises the REAL production shape."""
    base = [
        {
            "members_w": [f"w01{i:04d}", f"w02{i:04d}"],
            "canonical_w": f"w02{i:04d}",
            "owner_verdict": "approve",
        }
        for i in range(1, 16)
    ]
    base.append(
        {
            "members_w": ["w000452", "w001239"],
            "canonical_w": "w000452",
            "owner_verdict": "approve",
        }
    )
    return {
        "release_contract_version": "discovery-v4-public-reference-merges-v1",
        "v4_public_reference_canonical_ids": [v4_members[-1]],
        "v4_source_manifest_sha256": "a" * 64,
        "merges": [
            *base,
            {
                "members_w": list(v4_members),
                "canonical_w": v4_members[-1],
                "owner_verdict": "approve",
            },
        ],
        "dropped_by_135": ["w001239"],
    }


def test_load_canonical_merges_accepts_disjoint_standalone_ids(tmp_path):
    path = _write_json(
        tmp_path / "m.json", _minimal_v4_doc(standalone=["w050001", "w050002"])
    )
    out = sidecar_build.load_canonical_merges(path)
    assert out["public_first_standalone_canonical_ids"] == {"w050001", "w050002"}
    assert "w050001" not in out["cross_corpus_map"]


def test_load_canonical_merges_absent_standalone_list_is_byte_identical(tmp_path):
    path = _write_json(tmp_path / "m.json", _v4_release_merges_doc_local())
    out = sidecar_build.load_canonical_merges(path, require_release_semantics=True)
    assert out["approve_count"] == 17
    assert out["public_first_standalone_canonical_ids"] == set()


def test_load_canonical_merges_rejects_standalone_id_inside_a_merge_group(tmp_path):
    # w030001 is already a MEMBER of the v4 pair merge -- forbidden overlap.
    path = _write_json(tmp_path / "m.json", _minimal_v4_doc(standalone=["w030001"]))
    with pytest.raises(sidecar_build.CanonicalMergesError, match="appear in a merge group"):
        sidecar_build.load_canonical_merges(path)


def test_load_canonical_merges_rejects_standalone_id_overlapping_sibling_canonical(tmp_path):
    # w040002 is declared a v4 sibling canonical id but (deliberately, to
    # isolate this check from the "appears in a merge group" one above)
    # never actually sits in any merge group -- only the sibling-overlap
    # check can catch it.
    doc = {
        "release_contract_version": "discovery-v4-public-reference-merges-v1",
        "v4_public_reference_canonical_ids": ["w040001", "w040002"],
        "v4_source_manifest_sha256": "a" * 64,
        "merges": [
            {
                "members_w": ["w030001", "w040001"],
                "canonical_w": "w040001",
                "owner_verdict": "approve",
            }
        ],
        "dropped_by_135": [],
        "public_first_standalone_canonical_ids": ["w040002"],
    }
    path = _write_json(tmp_path / "m.json", doc)
    with pytest.raises(
        sidecar_build.CanonicalMergesError,
        match="overlap v4_public_reference_canonical_ids",
    ):
        sidecar_build.load_canonical_merges(path)


def test_load_canonical_merges_rejects_duplicate_standalone_ids(tmp_path):
    path = _write_json(
        tmp_path / "m.json", _minimal_v4_doc(standalone=["w050001", "w050001"])
    )
    with pytest.raises(sidecar_build.CanonicalMergesError, match="DISTINCT"):
        sidecar_build.load_canonical_merges(path)


def test_load_canonical_merges_rejects_non_w_shaped_standalone_id(tmp_path):
    path = _write_json(tmp_path / "m.json", _minimal_v4_doc(standalone=["not-a-w-id"]))
    with pytest.raises(sidecar_build.CanonicalMergesError, match="w000xxx-shaped"):
        sidecar_build.load_canonical_merges(path)


def test_load_canonical_merges_rejects_standalone_ids_on_v3_contract(tmp_path):
    doc = {
        "merges": [
            {
                "members_w": ["w000190", "w001382"],
                "canonical_w": "w001382",
                "owner_verdict": "approve",
            }
        ],
        "dropped_by_135": [],
        "public_first_standalone_canonical_ids": ["w050001"],
    }
    path = _write_json(tmp_path / "m.json", doc)
    with pytest.raises(sidecar_build.CanonicalMergesError, match="V3 canonical-merge contract"):
        sidecar_build.load_canonical_merges(path)
