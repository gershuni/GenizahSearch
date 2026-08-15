#!/usr/bin/env python3
"""Independently verify the V4 reference corpus and locus extension."""

from __future__ import annotations

import argparse
import hashlib
import json
import pickle
import sqlite3
from collections import Counter
from pathlib import Path

try:
    from scripts.discovery_v4_common import sha256_file, stable_json_dump
except ModuleNotFoundError:  # direct invocation
    from discovery_v4_common import sha256_file, stable_json_dump


def stream_hash(stream: str) -> str:
    return hashlib.sha256(stream.encode("utf-8")).hexdigest()


def run(args: argparse.Namespace) -> dict:
    manifest_path = Path(args.manifest).resolve()
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest.get("schema_version") != "discovery-v4-reference-manifest-v1":
        raise ValueError("unsupported V4 reference manifest")
    base_path = Path(manifest["base_reference"])
    reference_path = Path(manifest["reference_corpus"])
    locus_path = Path(manifest["locus_divisions"])
    coverage_path = Path(manifest["locus_coverage"])
    pinned = (
        (base_path, manifest["base_reference_sha256"], "base reference"),
        (reference_path, manifest["reference_corpus_sha256"], "V4 reference"),
        (locus_path, manifest["locus_divisions_sha256"], "V4 locus DB"),
        (coverage_path, manifest["locus_coverage_sha256"], "V4 locus coverage"),
    )
    for path, expected, label in pinned:
        actual = sha256_file(path)
        if actual != expected:
            raise ValueError(f"{label} hash mismatch")
    with base_path.open("rb") as stream:
        base = pickle.load(stream)
    with reference_path.open("rb") as stream:
        reference = pickle.load(stream)
    if reference[: len(base)] != base:
        raise ValueError("V4 reference changed the base-corpus prefix")
    appended = reference[len(base) :]
    entries = manifest["entries"]
    entry_by_id = {entry["raw_reference_id"]: entry for entry in entries}
    if len(entry_by_id) != len(entries):
        raise ValueError("manifest contains duplicate V4 raw reference ids")
    appended_by_id = {work["id"]: work for work in appended}
    if set(appended_by_id) != set(entry_by_id):
        raise ValueError("manifest entries do not equal the appended reference set")
    for raw_id, entry in entry_by_id.items():
        work = appended_by_id[raw_id]
        if len(work["stream"]) != entry["stream_len"]:
            raise ValueError("manifest stream length drift")
        if stream_hash(work["stream"]) != entry["stream_sha256"]:
            raise ValueError("manifest stream hash drift")
        offsets = entry["unit_offsets"]
        starts = [row["start_offset"] for row in offsets]
        if not starts or starts[0] != 0 or starts != sorted(set(starts)):
            raise ValueError("manifest unit offsets are empty, duplicated, or unordered")
        if starts[-1] >= entry["stream_len"]:
            raise ValueError("manifest unit offset exceeds its reference stream")
    coverage = json.loads(coverage_path.read_text(encoding="utf-8"))
    if coverage.get("reference_corpus_sha256") != sha256_file(reference_path):
        raise ValueError("locus coverage names a different reference corpus")
    if coverage.get("invariant_problems") != []:
        raise ValueError("locus coverage reports invariant problems")
    with sqlite3.connect(locus_path) as conn:
        integrity = conn.execute("PRAGMA integrity_check").fetchone()[0]
        foreign_keys = conn.execute("PRAGMA foreign_key_check").fetchall()
        work_count = conn.execute("SELECT COUNT(*) FROM locus_work").fetchone()[0]
        unit_count = conn.execute("SELECT COUNT(*) FROM locus_unit").fetchone()[0]
        families = dict(
            conn.execute(
                "SELECT family, COUNT(*) FROM locus_work GROUP BY family ORDER BY family"
            )
        )
        grains = dict(
            conn.execute(
                "SELECT grain, COUNT(*) FROM locus_work GROUP BY grain ORDER BY grain"
            )
        )
        locus_ids = {
            row[0]
            for row in conn.execute(
                "SELECT locus_ref_id FROM locus_work WHERE locus_ref_id LIKE 'REF4:%'"
            )
        }
        expected_locus_ids = {
            raw_id
            for raw_id, entry in entry_by_id.items()
            if len(entry["unit_offsets"]) > 1
        }
        if locus_ids != expected_locus_ids:
            raise ValueError("V4 locus rows disagree with whole-work fallback decisions")
        for raw_id in sorted(locus_ids):
            entry = entry_by_id[raw_id]
            rows = conn.execute(
                "SELECT unit_ord, start_offset, label_he, citation_pos "
                "FROM locus_unit WHERE locus_ref_id=? ORDER BY unit_ord",
                (raw_id,),
            ).fetchall()
            expected = [
                (ordinal, row["start_offset"], row["label_he"], row["citation_pos"])
                for ordinal, row in enumerate(entry["unit_offsets"])
            ]
            if rows != expected:
                raise ValueError("V4 locus units drifted from the reference manifest")
    if integrity != "ok" or foreign_keys:
        raise ValueError("V4 locus database failed SQLite integrity checks")
    expected_vocab = {"sefaria", "ja", "msource_header", "msource_daf"}
    if set(families) != expected_vocab:
        raise ValueError("V4 locus family vocabulary drift")
    if work_count != coverage["works_with_units"]:
        raise ValueError("V4 locus work count drift")
    if unit_count != coverage["units_total"]:
        raise ValueError("V4 locus unit count drift")
    if families != coverage["by_family"] or grains != coverage["by_grain"]:
        raise ValueError("V4 locus coverage summaries drift from the database")
    report = {
        "schema_version": "discovery-v4-reference-verification-v1",
        "manifest_sha256": sha256_file(manifest_path),
        "base_reference_count": len(base),
        "reference_count": len(reference),
        "appended_reference_count": len(appended),
        "appended_letters": sum(len(work["stream"]) for work in appended),
        "providers": dict(Counter(entry["provider"] for entry in entries)),
        "locus_work_count": work_count,
        "locus_unit_count": unit_count,
        "v4_locus_work_count": len(locus_ids),
        "whole_work_fallback_count": len(entries) - len(locus_ids),
        "integrity_check": integrity,
        "foreign_key_problems": len(foreign_keys),
    }
    if args.report:
        stable_json_dump(report, args.report)
    print(json.dumps(report, indent=2))
    return report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--report")
    return parser.parse_args()


if __name__ == "__main__":
    run(parse_args())
