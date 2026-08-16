#!/usr/bin/env python3
"""Mint and reconcile the public identities that survive the V4 rematch."""

from __future__ import annotations

import argparse
import copy
import csv
import hashlib
import json
import re
import sqlite3
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

try:
    from scripts.discovery_v4_common import require_hash, sha256_file, stable_json_dump
except ModuleNotFoundError:  # direct invocation
    from discovery_v4_common import require_hash, sha256_file, stable_json_dump


APPROVED_HEADER = (
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
V4_MERGE_CONTRACT = "discovery-v4-public-reference-merges-v1"
OPAQUE_RE = re.compile(r"w[0-9]{6}")


def curated_content_hash(payload: list[dict]) -> str:
    encoded = json.dumps(payload, sort_keys=True, ensure_ascii=False)
    return "sha256:" + hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def canonical_map(merges: dict) -> dict[str, str]:
    result: dict[str, str] = {}
    for group in merges["merges"]:
        if group.get("owner_verdict") != "approve":
            continue
        for member in group["members_w"]:
            if member in result:
                raise ValueError("base canonical merges are not disjoint")
            result[member] = group["canonical_w"]
    return result


def next_work_ids(crosswalk: dict[str, str], count: int) -> list[str]:
    values = list(crosswalk.values())
    if any(not OPAQUE_RE.fullmatch(value) for value in values):
        raise ValueError("base crosswalk contains a malformed opaque work id")
    if len(values) != len(set(values)):
        raise ValueError("base crosswalk is not injective")
    next_number = max(int(value[1:]) for value in values) + 1
    if next_number + count - 1 > 999_999:
        raise ValueError("opaque work-id namespace exhausted")
    return [f"w{number:06d}" for number in range(next_number, next_number + count)]


def update_domain_counts(document: dict) -> None:
    rows = document["assignments"]
    confidence = Counter(row["confidence"] for row in rows)
    needs = [row for row in rows if row["confidence"] == "needs-ruling"]
    document["counts"] = {
        "total": len(rows),
        "by_confidence": dict(sorted(confidence.items())),
        "unassigned": sum(
            row["domain_parent"] == "Unassigned" and row["domain_leaf"] == "Unassigned"
            for row in rows
        ),
        "needs_ruling_held": sum(not row.get("owner_ruling") for row in needs),
        "needs_ruling_ruled": sum(bool(row.get("owner_ruling")) for row in needs),
    }


def run(args: argparse.Namespace) -> dict:
    reference_manifest_path = Path(args.reference_manifest).resolve()
    require_hash(
        reference_manifest_path,
        args.reference_manifest_sha256,
        "V4 reference manifest",
    )
    reference_manifest = json.loads(
        reference_manifest_path.read_text(encoding="utf-8")
    )
    if reference_manifest.get("schema_version") != "discovery-v4-reference-manifest-v1":
        raise ValueError("unsupported V4 reference manifest")
    entries = {
        entry["raw_reference_id"]: entry for entry in reference_manifest["entries"]
    }
    if len(entries) != len(reference_manifest["entries"]):
        raise ValueError("reference manifest contains duplicate raw ids")

    base_crosswalk_path = Path(args.base_crosswalk)
    base_approved_path = Path(args.base_approved)
    base_merges_path = Path(args.base_merges)
    base_domains_path = Path(args.base_work_domains)
    require_hash(base_crosswalk_path, args.base_crosswalk_sha256, "base crosswalk")
    require_hash(base_approved_path, args.base_approved_sha256, "base approved review")
    require_hash(base_merges_path, args.base_merges_sha256, "base canonical merges")
    require_hash(base_domains_path, args.base_work_domains_sha256, "base work domains")

    match_db = Path(args.match_db).resolve()
    with sqlite3.connect(f"file:{match_db.as_posix()}?mode=ro", uri=True) as conn:
        columns = {
            row[1] for row in conn.execute("PRAGMA table_info(track1_matches)")
        }
        if not {"shadowed_by", "ref_spans_json"}.issubset(columns):
            raise ValueError("V4 match DB has not been promoted with reference offsets")
        match_rows = conn.execute(
            """SELECT work_id, MIN(title), MIN(author), MIN(genre),
                      COUNT(*), COUNT(DISTINCT sys_id), COUNT(DISTINCT page_id)
                 FROM track1_matches
                WHERE shadowed_by IS NULL AND work_id LIKE 'REF4:%'
                GROUP BY work_id ORDER BY work_id"""
        ).fetchall()
    live_raw_ids = [row[0] for row in match_rows]
    if not live_raw_ids:
        raise ValueError("V4 rematch produced no live public-reference rows")
    unknown = set(live_raw_ids) - set(entries)
    if unknown:
        raise ValueError("V4 rematch contains raw ids absent from its reference manifest")

    crosswalk = json.loads(base_crosswalk_path.read_text(encoding="utf-8"))
    collisions = set(live_raw_ids) & set(crosswalk)
    if collisions:
        raise ValueError("V4 raw ids already exist in the base crosswalk")
    minted = dict(zip(live_raw_ids, next_work_ids(crosswalk, len(live_raw_ids))))
    crosswalk.update(minted)
    stable_json_dump(crosswalk, args.output_crosswalk)

    with base_approved_path.open(encoding="utf-8-sig", newline="") as stream:
        reader = csv.DictReader(stream)
        if tuple(reader.fieldnames or ()) != APPROVED_HEADER:
            raise ValueError("base approved-review header drift")
        approved_rows = list(reader)
    approved_by_id = {row["work_id"]: row for row in approved_rows}
    if len(approved_by_id) != len(approved_rows):
        raise ValueError("base approved-review file contains duplicate work ids")
    for raw_id, _title, _author, genre, row_count, witnesses, claims in match_rows:
        entry = entries[raw_id]
        target = entry["target_private_work_id"]
        target_review = approved_by_id.get(target)
        if target_review is None or target_review["owner_verdict"] not in {"approve", "edit"}:
            raise ValueError("V4 target private identity is not owner-approved")
        approved_rows.append(
            {
                "work_id": minted[raw_id],
                "candidate_title": entry["title"],
                "author": target_review["author"],
                "genre": genre or target_review["genre"],
                "source_label": "sefaria",
                "confidence_basis": "v4-public-reference-owner-authorized",
                "tier_a_witnesses": str(witnesses),
                "claim_count": str(claims),
                "owner_title": "",
                "owner_verdict": "approve",
                "owner_note": (
                    f"V4 public-source sibling of {target}; matcher rows={row_count}"
                ),
            }
        )
    output_approved = Path(args.output_approved)
    output_approved.parent.mkdir(parents=True, exist_ok=True)
    with output_approved.open("w", encoding="utf-8-sig", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=APPROVED_HEADER)
        writer.writeheader()
        writer.writerows(approved_rows)

    base_merges = json.loads(base_merges_path.read_text(encoding="utf-8"))
    old_canonical = canonical_map(base_merges)
    used_members = set(old_canonical)
    new_groups = []
    for raw_id in live_raw_ids:
        target = entries[raw_id]["target_private_work_id"]
        public = minted[raw_id]
        if target in used_members:
            raise ValueError("V4 target already participates in a canonical merge")
        used_members.update((target, public))
        new_groups.append(
            {
                "members_w": [target, public],
                "canonical_w": public,
                "owner_verdict": "approve",
            }
        )
    merges = copy.deepcopy(base_merges)
    merges["release_contract_version"] = V4_MERGE_CONTRACT
    merges["v4_public_reference_canonical_ids"] = [
        minted[raw_id] for raw_id in live_raw_ids
    ]
    merges["v4_source_manifest_sha256"] = reference_manifest[
        "acquisition_manifest_sha256"
    ]
    merges["source"] = "Discovery V4 public-reference expansion"
    merges["merges"] = [*base_merges["merges"], *new_groups]
    stable_json_dump(merges, args.output_merges)

    domains = json.loads(base_domains_path.read_text(encoding="utf-8"))
    if curated_content_hash(domains["assignments"]) != domains["content_hash"]:
        raise ValueError("base work-domain artifact has a stale content hash")
    domain_by_id = {
        row["canonical_work_id"]: row for row in domains["assignments"]
    }
    for raw_id in live_raw_ids:
        target = entries[raw_id]["target_private_work_id"]
        target_canonical = old_canonical.get(target, target)
        source_row = domain_by_id.get(target_canonical)
        if source_row is None:
            raise ValueError("V4 target has no curated work-domain assignment")
        new_row = copy.deepcopy(source_row)
        new_row["canonical_work_id"] = minted[raw_id]
        new_row["provenance"] = f"v4-public-reference-inherits:{target_canonical}"
        domains["assignments"].append(new_row)
    domains["assignments"].sort(key=lambda row: row["canonical_work_id"])
    domains["generated_by"] = "scripts/discovery_v4_reconcile.py"
    domains["generated_utc"] = datetime.now(timezone.utc).isoformat()
    update_domain_counts(domains)
    domains["content_hash"] = curated_content_hash(domains["assignments"])
    stable_json_dump(domains, args.output_work_domains)

    report = {
        "schema_version": "discovery-v4-reconciliation-v1",
        "reference_manifest_sha256": sha256_file(reference_manifest_path),
        "source_manifest_sha256": reference_manifest["acquisition_manifest_sha256"],
        "live_public_reference_count": len(live_raw_ids),
        "quarantined_or_unmatched_reference_count": len(entries) - len(live_raw_ids),
        "live_match_rows": sum(row[4] for row in match_rows),
        "live_witnesses": sum(row[5] for row in match_rows),
        "minted_first": min(minted.values()),
        "minted_last": max(minted.values()),
        "crosswalk_sha256": sha256_file(args.output_crosswalk),
        "approved_review_sha256": sha256_file(args.output_approved),
        "canonical_merges_sha256": sha256_file(args.output_merges),
        "canonical_merge_count": len(merges["merges"]),
        "work_domains_file_sha256": sha256_file(args.output_work_domains),
        "work_domains_content_hash": domains["content_hash"],
        "work_domain_assignments": len(domains["assignments"]),
        "raw_to_opaque": minted,
    }
    if args.report:
        stable_json_dump(report, args.report)
    print(json.dumps(report, indent=2))
    return report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--reference-manifest", required=True)
    parser.add_argument("--reference-manifest-sha256", required=True)
    parser.add_argument("--match-db", required=True)
    parser.add_argument("--base-crosswalk", required=True)
    parser.add_argument("--base-crosswalk-sha256", required=True)
    parser.add_argument("--base-approved", required=True)
    parser.add_argument("--base-approved-sha256", required=True)
    parser.add_argument("--base-merges", required=True)
    parser.add_argument("--base-merges-sha256", required=True)
    parser.add_argument("--base-work-domains", required=True)
    parser.add_argument("--base-work-domains-sha256", required=True)
    parser.add_argument("--output-crosswalk", required=True)
    parser.add_argument("--output-approved", required=True)
    parser.add_argument("--output-merges", required=True)
    parser.add_argument("--output-work-domains", required=True)
    parser.add_argument("--report")
    return parser.parse_args()


if __name__ == "__main__":
    run(parse_args())
