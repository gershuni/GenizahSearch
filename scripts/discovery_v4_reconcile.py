#!/usr/bin/env python3
"""Mint and reconcile the public identities that survive the V4 rematch.

Reference-manifest entry contract (V4.2 C5, ``identity_mode`` awareness)
--------------------------------------------------------------------------
Each entry in a V4/V4.2 reference manifest's ``entries`` list may carry an
``identity_mode`` field: ``"private_sibling"`` (the default when the field
is ABSENT, so every pre-C5 manifest -- with no such field at all -- keeps
its original meaning byte-for-byte) or ``"public_first"``.

* A ``private_sibling`` entry keeps the pre-existing ``target_private_work_id``
  field: an opaque ``w000xxx`` id of an existing, owner-approved private
  identity this public source is a sibling reference of. Reference metadata
  (author/genre) is inherited from that private identity's approved review
  row, exactly as before.
* A ``public_first`` entry instead carries an ``identity_key`` field (a
  ``pf-####``-shaped key into the C5 public-first identity artifact loaded
  from ``--public-first-artifact``) and has NO ``target_private_work_id`` --
  no private counterpart exists. Reference metadata (title/author/genre) and
  domain assignment come EXCLUSIVELY from that artifact's matching approved
  entry; the minted work is STANDALONE (no merge group of any kind).

When the reference manifest contains no ``public_first`` entries and
``--public-first-artifact`` is not supplied, this module's behavior is
byte-for-byte identical to the pre-C5 script (see
``tests/test_discovery_v4_common.py::test_v4_reconciliation_mints_and_merges_only_a_live_public_reference``,
which is pinned and must keep passing unmodified).
"""

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
    from scripts.discovery_ids import SOURCE_CORPUS_SEFARIA
    from scripts.discovery_v4_common import require_hash, sha256_file, stable_json_dump
    from scripts.discovery_track1_contract import IDENTITY_MODES
    from scripts.discovery_identification_eligibility import load_eligibility_artifact
    from scripts.discovery_public_first_identity import load_public_first_artifact
except ModuleNotFoundError:  # direct invocation
    from discovery_ids import SOURCE_CORPUS_SEFARIA
    from discovery_v4_common import require_hash, sha256_file, stable_json_dump
    from discovery_track1_contract import IDENTITY_MODES
    from discovery_identification_eligibility import load_eligibility_artifact
    from discovery_public_first_identity import load_public_first_artifact


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
_PRIVATE_SIBLING = "private_sibling"
_PUBLIC_FIRST = "public_first"

#: Acquisition PROVIDERS that are open corpora, and therefore map to the masked
#: `sefaria` (open-corpus) `source_corpus` code.
#:
#: This mapping exists because of a defect found on 2026-08-19: the public-first
#: branch below wrote `pf_entry["provider"]` straight into `source_label`, a
#: column contractually holding "the masked `source_corpus` code only". It
#: looked correct for two appends because 35 of REF6's 50 providers are
#: literally named `sefaria` -- and silently cost 14 owner-approved works,
#: 9,715 claims and about 30% of the REF6 append when 15 Hebrew Wikisource
#: sources arrived labelled `hewikisource`, a value outside the frozen
#: three-code vocabulary. `load_approved_works` swallowed the resulting
#: ValueError with a bare `continue`, so the build exited 0 and the release
#: verifier passed over an artifact quietly missing סמ"ג, all three Tur
#: sections and all three parts of the Zohar.
#:
#: An UNKNOWN provider halts rather than defaulting: guessing `sefaria` for
#: something that might be restricted is the one error this must never make.
_OPEN_PROVIDER_SOURCE_LABELS = {
    "sefaria": SOURCE_CORPUS_SEFARIA,
    "hewikisource": SOURCE_CORPUS_SEFARIA,
}


def public_first_source_label(provider: str) -> str:
    """The masked `source_corpus` code for a public-first acquisition provider.

    Never the provider name: `source_corpus` is a masked, provider-agnostic
    code, `discovery_claim.source_corpus` is derived independently from the
    matcher's own `cat`, and F4
    (`verify_discovery_sidecar.py::check_source_corpus_consistency`) requires
    the two to be equal -- so a provider name here fails the release verifier
    even when it survives the vocabulary check.
    """
    code = _OPEN_PROVIDER_SOURCE_LABELS.get((provider or "").strip())
    if code is None:
        raise ValueError(
            f"unknown public-first acquisition provider {provider!r}: cannot "
            "derive a masked source_corpus code. Add it to "
            "_OPEN_PROVIDER_SOURCE_LABELS if it is an OPEN corpus; a provider "
            "that is not open must not be minted as a public-first identity."
        )
    return code


assert {_PRIVATE_SIBLING, _PUBLIC_FIRST} == set(IDENTITY_MODES)
# Recognizes a public-reference raw id (any REF<digits>: prefix). This decides
# only whether a row is a REFERENCE row at all -- WHICH reference namespaces
# this run reconciles comes from the manifest bundle, never from the prefix
# shape. Selecting on the shape alone was a real defect: it swept in REF2
# (legacy, reconciled under the fitted gen-2 decisions) and REF4 (minted in
# the V4 bake), both of which a REF5/REF6 manifest cannot describe, so the
# first real multi-namespace match table failed the manifest-coverage check.
_RAW_ID_NAMESPACE_RE = re.compile(r"^REF[0-9]+:")


def _namespace_of(raw_id: str) -> str:
    return raw_id.split(":", 1)[0] if ":" in raw_id else ""


def _as_list(value) -> list:
    """One-or-many CLI input as a list; a bare scalar is a one-stage bundle.

    Callers that hand-build an ``argparse.Namespace`` (the pinned tests do)
    pass scalars, and the V4-era command line passed a single manifest. Both
    stay valid.
    """
    if value is None:
        return []
    return list(value) if isinstance(value, (list, tuple)) else [value]


def load_manifest_bundle(args: argparse.Namespace) -> tuple[list[dict], dict]:
    """The ORDERED reference-manifest bundle (V4.2 plan C2) and its entries.

    C2 requires consumers to take the complete ordered bundle rather than one
    manifest, because a consumer must never assume the manifest it was handed
    describes every reference row it can see. The V4.2 match table carries
    four namespaces (REF2, REF4, REF5, REF6) and no single producer manifest
    covers more than one.

    REF4 is deliberately NOT part of reconcile's bundle even though it is part
    of the chain: its identities were minted in the V4 bake and live in the
    base crosswalk, so passing it would (correctly) trip the crosswalk
    collision guard. Reconcile's bundle is the stages whose identities THIS
    run mints.
    """
    paths = _as_list(getattr(args, "reference_manifest", None))
    shas = _as_list(getattr(args, "reference_manifest_sha256", None))
    if not paths:
        raise ValueError("at least one --reference-manifest is required")
    if len(paths) != len(shas):
        raise ValueError(
            "each --reference-manifest needs exactly one "
            "--reference-manifest-sha256, given in the same order"
        )
    stages: list[dict] = []
    entries: dict[str, dict] = {}
    for path, expected_sha in zip(paths, shas):
        manifest_path = Path(path).resolve()
        require_hash(manifest_path, expected_sha, "V4 reference manifest")
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        if manifest.get("schema_version") != "discovery-v4-reference-manifest-v1":
            raise ValueError("unsupported V4 reference manifest")
        stage_entries = manifest["entries"]
        if not stage_entries:
            raise ValueError(f"reference manifest describes no references: {manifest_path}")
        for entry in stage_entries:
            raw_id = entry["raw_reference_id"]
            if raw_id in entries:
                raise ValueError(
                    f"reference manifest bundle describes {raw_id} more than once"
                )
            entries[raw_id] = entry
        stages.append(
            {
                "namespaces": sorted(
                    {_namespace_of(entry["raw_reference_id"]) for entry in stage_entries}
                ),
                "reference_manifest_sha256": sha256_file(manifest_path),
                "acquisition_manifest_sha256": manifest["acquisition_manifest_sha256"],
                "reference_count": len(stage_entries),
            }
        )
    return stages, entries


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
    stages, entries = load_manifest_bundle(args)
    bundle_namespaces = {
        namespace for stage in stages for namespace in stage["namespaces"]
    }
    for raw_id, entry in entries.items():
        mode = entry.get("identity_mode", _PRIVATE_SIBLING)
        if mode not in IDENTITY_MODES:
            raise ValueError(
                f"reference manifest entry {raw_id} has an invalid identity_mode: {mode!r}"
            )
        if mode == _PRIVATE_SIBLING:
            if not entry.get("target_private_work_id"):
                raise ValueError(
                    f"private_sibling entry {raw_id} is missing target_private_work_id"
                )
        elif not entry.get("identity_key"):
            raise ValueError(f"public_first entry {raw_id} is missing identity_key")

    base_crosswalk_path = Path(args.base_crosswalk)
    base_approved_path = Path(args.base_approved)
    base_merges_path = Path(args.base_merges)
    base_domains_path = Path(args.base_work_domains)
    require_hash(base_crosswalk_path, args.base_crosswalk_sha256, "base crosswalk")
    require_hash(base_approved_path, args.base_approved_sha256, "base approved review")
    require_hash(base_merges_path, args.base_merges_sha256, "base canonical merges")
    require_hash(base_domains_path, args.base_work_domains_sha256, "base work domains")

    # C5: the public-first identity artifact is OPTIONAL input. Absent, the
    # reconcile step behaves exactly as the pre-C5 (private_sibling-only)
    # script did -- this is what keeps the pinned byte-compatibility test
    # green. Both flags are getattr'd with a None default (not read via
    # args.public_first_artifact directly) because callers that build an
    # argparse.Namespace by hand -- as the pinned test does -- never set
    # these new attributes at all.
    public_first_artifact_path = getattr(args, "public_first_artifact", None)
    public_first_artifact_sha256 = getattr(args, "public_first_artifact_sha256", None)
    if bool(public_first_artifact_path) != bool(public_first_artifact_sha256):
        raise ValueError(
            "--public-first-artifact and --public-first-artifact-sha256 must be "
            "supplied together"
        )
    public_first_artifact = None
    if public_first_artifact_path:
        public_first_artifact = load_public_first_artifact(
            public_first_artifact_path, sha256=public_first_artifact_sha256,
        )

    # The eligibility artifact is OPTIONAL here and read for REPORTING only:
    # reconcile mints from live rows, and a withdrawn work has none, so this
    # changes no identity decision. It only lets the report say WHY nothing was
    # minted. Same both-or-neither discipline as the artifact above.
    eligibility_path = getattr(args, "eligibility", None)
    eligibility_sha256 = getattr(args, "eligibility_sha256", None)
    if bool(eligibility_path) != bool(eligibility_sha256):
        raise ValueError(
            "--eligibility and --eligibility-sha256 must be supplied together"
        )
    eligibility = None
    if eligibility_path:
        eligibility = load_eligibility_artifact(
            eligibility_path, sha256=eligibility_sha256
        )

    match_db = Path(args.match_db).resolve()
    with sqlite3.connect(f"file:{match_db.as_posix()}?mode=ro", uri=True) as conn:
        columns = {
            row[1] for row in conn.execute("PRAGMA table_info(track1_matches)")
        }
        if not {"shadowed_by", "ref_spans_json"}.issubset(columns):
            raise ValueError("V4 match DB has not been promoted with reference offsets")
        all_rows = conn.execute(
            """SELECT work_id, MIN(title), MIN(author), MIN(genre),
                      COUNT(*), COUNT(DISTINCT sys_id), COUNT(DISTINCT page_id)
                 FROM track1_matches
                WHERE shadowed_by IS NULL
                GROUP BY work_id ORDER BY work_id"""
        ).fetchall()
    # Scope: the namespaces THIS bundle describes. Reference rows from any
    # other namespace belong to an earlier bake (REF4) or to the legacy cohort
    # (REF2) -- out of scope here, and counted into the report rather than
    # dropped quietly.
    match_rows = [row for row in all_rows if _namespace_of(row[0]) in bundle_namespaces]
    out_of_scope: dict[str, dict] = {}
    for row in all_rows:
        namespace = _namespace_of(row[0])
        if namespace in bundle_namespaces or not _RAW_ID_NAMESPACE_RE.match(row[0]):
            continue
        bucket = out_of_scope.setdefault(namespace, {"references": 0, "live_rows": 0})
        bucket["references"] += 1
        bucket["live_rows"] += row[4]
    live_raw_ids = [row[0] for row in match_rows]
    if not live_raw_ids:
        raise ValueError("V4 rematch produced no live public-reference rows")
    unknown = set(live_raw_ids) - set(entries)
    if unknown:
        raise ValueError("V4 rematch contains raw ids absent from its reference manifest")
    in_scope: dict[str, dict] = {}
    for row in match_rows:
        bucket = in_scope.setdefault(
            _namespace_of(row[0]), {"references": 0, "live_rows": 0}
        )
        bucket["references"] += 1
        bucket["live_rows"] += row[4]

    if public_first_artifact is None:
        for raw_id in live_raw_ids:
            if entries[raw_id].get("identity_mode", _PRIVATE_SIBLING) == _PUBLIC_FIRST:
                raise ValueError(
                    f"live raw id {raw_id} has identity_mode public_first but no "
                    "--public-first-artifact was supplied"
                )

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

    private_sibling_raw_ids: list[str] = []
    public_first_raw_ids: list[str] = []
    public_first_matched_keys: set[str] = set()

    for raw_id, _title, _author, genre, row_count, witnesses, claims in match_rows:
        entry = entries[raw_id]
        mode = entry.get("identity_mode", _PRIVATE_SIBLING)
        if mode == _PRIVATE_SIBLING:
            target = entry["target_private_work_id"]
            target_review = approved_by_id.get(target)
            if target_review is None or target_review["owner_verdict"] not in {"approve", "edit"}:
                raise ValueError("V4 target private identity is not owner-approved")
            private_sibling_raw_ids.append(raw_id)
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
        else:
            # public_first (C5): reference metadata comes EXCLUSIVELY from
            # the approved artifact entry -- never the matcher's own
            # title/genre, and never inherited from any private identity
            # (there isn't one).
            identity_key = entry["identity_key"]
            pf_entry = public_first_artifact["entries_by_key"].get(identity_key)
            if pf_entry is None or pf_entry["verdict"] != "approve":
                raise ValueError(
                    f"public-first identity_key {identity_key!r} for raw id {raw_id} "
                    "is absent, rejected, or deferred in the public-first artifact"
                )
            public_first_raw_ids.append(raw_id)
            public_first_matched_keys.add(identity_key)
            approved_rows.append(
                {
                    "work_id": minted[raw_id],
                    "candidate_title": pf_entry["title_he"],
                    "author": pf_entry["author"],
                    "genre": pf_entry["genre"],
                    "source_label": public_first_source_label(pf_entry["provider"]),
                    "confidence_basis": "v4-public-first-owner-authorized",
                    "tier_a_witnesses": str(witnesses),
                    "claim_count": str(claims),
                    "owner_title": "",
                    "owner_verdict": "approve",
                    "owner_note": (
                        f"V4.2 public-first identity {identity_key}; "
                        f"matcher rows={row_count}"
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
    for raw_id in private_sibling_raw_ids:
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
    # C5: public_first mints are STANDALONE canonical works -- NO merge group
    # of any kind (no singleton, no synthetic two-member pairing). They are
    # recorded in their own list instead, validated by
    # build_discovery_sidecar.py::load_canonical_merges to be absent from
    # every merge group.
    public_first_standalone_canonical_ids = [
        minted[raw_id] for raw_id in public_first_raw_ids
    ]
    merges = copy.deepcopy(base_merges)
    merges["release_contract_version"] = V4_MERGE_CONTRACT
    # ACCUMULATE, exactly as ``merges["merges"]`` does below. The public
    # sidecar checks `approve_count == _EXPECTED_APPROVE_MERGES +
    # len(v4_public_reference_canonical_ids)`, so a second expansion that
    # OVERWROTE this list with only its own mints would leave the earlier
    # bake's approve groups counted on one side of that equation and not the
    # other -- a hard error at sidecar-build time, far from its cause.
    merges["v4_public_reference_canonical_ids"] = [
        *base_merges.get("v4_public_reference_canonical_ids", []),
        *(minted[raw_id] for raw_id in private_sibling_raw_ids),
    ]
    # REF4-SPECIFIC BY DOWNSTREAM CONTRACT: the excerpt bake compares this pin
    # (via the sidecar's `canonical_merges_v4_source_manifest_sha256`) against
    # REF4's acquisition manifest. A later expansion must PRESERVE it, not
    # replace it with its own stage's hash. It is established only when the
    # base carries none, which is what the V4 bake did.
    if not base_merges.get("v4_source_manifest_sha256"):
        merges["v4_source_manifest_sha256"] = stages[0]["acquisition_manifest_sha256"]
    merges["source"] = "Discovery V4 public-reference expansion"
    merges["merges"] = [*base_merges["merges"], *new_groups]
    if public_first_standalone_canonical_ids:
        merges["public_first_standalone_canonical_ids"] = (
            public_first_standalone_canonical_ids
        )
    stable_json_dump(merges, args.output_merges)

    domains = json.loads(base_domains_path.read_text(encoding="utf-8"))
    if curated_content_hash(domains["assignments"]) != domains["content_hash"]:
        raise ValueError("base work-domain artifact has a stale content hash")
    domain_by_id = {
        row["canonical_work_id"]: row for row in domains["assignments"]
    }
    for raw_id in private_sibling_raw_ids:
        target = entries[raw_id]["target_private_work_id"]
        target_canonical = old_canonical.get(target, target)
        source_row = domain_by_id.get(target_canonical)
        if source_row is None:
            raise ValueError("V4 target has no curated work-domain assignment")
        new_row = copy.deepcopy(source_row)
        new_row["canonical_work_id"] = minted[raw_id]
        new_row["provenance"] = f"v4-public-reference-inherits:{target_canonical}"
        domains["assignments"].append(new_row)
    for raw_id in public_first_raw_ids:
        identity_key = entries[raw_id]["identity_key"]
        pf_entry = public_first_artifact["entries_by_key"][identity_key]
        # C5: domains come from the artifact -- no inheritance from a
        # nonexistent private identity.
        domains["assignments"].append(
            {
                "canonical_work_id": minted[raw_id],
                "domain_parent": pf_entry["domain_parent"],
                "domain_leaf": pf_entry["domain_leaf"],
                "confidence": "high",
                "provenance": f"public-first:{identity_key}",
            }
        )
    domains["assignments"].sort(key=lambda row: row["canonical_work_id"])
    domains["generated_by"] = "scripts/discovery_v4_reconcile.py"
    domains["generated_utc"] = datetime.now(timezone.utc).isoformat()
    update_domain_counts(domains)
    domains["content_hash"] = curated_content_hash(domains["assignments"])
    stable_json_dump(domains, args.output_work_domains)

    report = {
        "schema_version": "discovery-v4-reconciliation-v1",
        # The two scalars name the NEWEST stage, so a one-manifest call reports
        # exactly what it always did; `reference_bundle` carries every stage in
        # order, which is the honest record when there is more than one.
        "reference_manifest_sha256": stages[-1]["reference_manifest_sha256"],
        "source_manifest_sha256": stages[-1]["acquisition_manifest_sha256"],
        "reference_bundle": stages,
        "live_by_namespace": in_scope,
        # Recorded, never silently dropped: reference rows this bundle does not
        # describe (REF2 legacy, REF4 minted in the V4 bake).
        "out_of_scope_namespaces": out_of_scope,
        "live_public_reference_count": len(private_sibling_raw_ids),
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
    # C5 reporting is additive-only: these keys are omitted entirely (not
    # emitted as empty/zero) when no public-first artifact was supplied, so
    # the artifact-absent report is byte-identical to the pre-C5 script's.
    if public_first_artifact is not None:
        # An approved identity can end up with nothing minted for two very
        # different reasons, and reporting them as one number would be
        # misleading: either no source route was ever found for it, or an
        # eligibility ruling withdrew the work AFTER approval. The artifact
        # still says "approve" in the second case, and correctly so -- the
        # approval happened; the ruling came later.
        withdrawn_keys = {
            entries[raw_id].get("identity_key")
            for raw_id, record in (eligibility or {}).get("by_work", {}).items()
            if record["scope"] == "work" and raw_id in entries
        }
        withdrawn_keys.discard(None)
        unmatched = []
        withdrawn = []
        for identity_key, pf_entry in public_first_artifact["entries_by_key"].items():
            if pf_entry["verdict"] != "approve" or identity_key in public_first_matched_keys:
                continue
            row = {"identity_key": identity_key, "verdict": pf_entry["verdict"]}
            if identity_key in withdrawn_keys:
                withdrawn.append(row)
            else:
                unmatched.append(row)
        public_first_unmatched_approved = sorted(
            unmatched, key=lambda row: row["identity_key"]
        )
        if withdrawn:
            report["public_first_withdrawn_by_ruling"] = sorted(
                withdrawn, key=lambda row: row["identity_key"]
            )
        report["live_public_first_count"] = len(public_first_raw_ids)
        report["public_first_standalone_canonical_ids"] = (
            public_first_standalone_canonical_ids
        )
        report["public_first_unmatched_approved"] = public_first_unmatched_approved
    if args.report:
        stable_json_dump(report, args.report)
    print(json.dumps(report, indent=2))
    return report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    # Repeatable and ORDERED (V4.2 plan C2): pass one --reference-manifest per
    # chain stage this run mints, each with its own --reference-manifest-sha256
    # in the same order.
    parser.add_argument("--reference-manifest", action="append", required=True)
    parser.add_argument("--reference-manifest-sha256", action="append", required=True)
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
    parser.add_argument(
        "--public-first-artifact",
        help="C5 public-first identity artifact (discovery-public-first-identities-v1)",
    )
    parser.add_argument(
        "--public-first-artifact-sha256",
        help="required alongside --public-first-artifact",
    )
    parser.add_argument(
        "--eligibility",
        help=(
            "identification-eligibility artifact; REPORTING only here -- it lets an "
            "approved identity that a ruling withdrew be reported as withdrawn "
            "rather than as unmatched"
        ),
    )
    parser.add_argument(
        "--eligibility-sha256", help="required alongside --eligibility"
    )
    parser.add_argument("--report")
    return parser.parse_args()


if __name__ == "__main__":
    run(parse_args())
