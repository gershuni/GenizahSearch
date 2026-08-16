"""Shared Track-1 release-contract v2 schema, run identity, and cohort registry.

This module pins the interfaces the combined V4.1+V4.2 bake shares across
three otherwise-independent scripts (V4.2 plan C1/C3/C4):

- ``discovery_v4_match.py`` derives the run id, maintains the batch ledger,
  and EMITS the v2 release contract at promote time.
- ``build_discovery_sidecar.py`` CONSUMES the v2 contract and routes
  namespaced reference rows by the cohort registry.
- ``bake_discovery_excerpts.py`` walks the ordered manifest chain whose
  namespace vocabulary the registry defines.

The frozen v1 contract (``discovery-v4-track1-release-contract-v1``) is not
touched: v1 documents keep validating against the v1 key set wherever they are
accepted today. v2 is a different schema version with its own exact key set.
"""

from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path

CONTRACT_V2_SCHEMA_VERSION = "discovery-track1-release-contract-v2"
COHORT_REGISTRY_SCHEMA_VERSION = "discovery-routing-cohorts-v1"

_NAMESPACE_RE = re.compile(r"REF[0-9]+")
_SHA256_RE = re.compile(r"[0-9a-f]{64}")

# The exact top-level key set of a v2 contract. Exact means exact: a consumer
# rejects both missing and unexpected keys, as the sidecar builder does for v1.
CONTRACT_V2_KEYS = frozenset(
    {
        "schema_version",
        "run_id",
        "reference_corpus_sha256",
        "canonical_masks_sha256",
        "source_db_seed_sha256",
        "pilot_sha256",
        "calibration_sha256",
        "matcher_fingerprint",
        "page_count",
        "page_batch",
        "expected_batches",
        "total_rows",
        "live_rows",
        "v2_snapshot_rows",
        "missing_ref_offsets",
        "duplicate_pairs",
        "shadow_algorithm",
        "promoted_columns",
        "namespaces",
    }
)

# Facts that determine a run's identity. Every one is required; the id is the
# SHA-256 of their canonical JSON. Changing any input (reference, masks, seed,
# pilot, calibration, page frame size, batch geometry, tag, generation) yields
# a different run id, which is the point: a staged table can only be resumed or
# promoted by the run that created it.
RUN_ID_FACT_KEYS = (
    "reference_corpus_sha256",
    "canonical_masks_sha256",
    "source_db_seed_sha256",
    "pilot_sha256",
    "calibration_sha256",
    "page_count",
    "page_batch",
    "generation",
    "tag",
)
_RUN_ID_HEX_FACTS = frozenset(
    {
        "reference_corpus_sha256",
        "canonical_masks_sha256",
        "source_db_seed_sha256",
        "pilot_sha256",
        "calibration_sha256",
    }
)

IDENTITY_MODES = ("private_sibling", "public_first")


def derive_run_id(facts: dict) -> str:
    """Return the run id binding a staged matcher run to ALL of its inputs."""
    missing = [key for key in RUN_ID_FACT_KEYS if key not in facts]
    if missing:
        raise ValueError(f"run-id facts missing: {missing}")
    unexpected = sorted(set(facts) - set(RUN_ID_FACT_KEYS))
    if unexpected:
        raise ValueError(f"run-id facts unexpected: {unexpected}")
    for key in _RUN_ID_HEX_FACTS:
        value = facts[key]
        if not isinstance(value, str) or not _SHA256_RE.fullmatch(value):
            raise ValueError(f"run-id fact {key} must be a lowercase SHA-256")
    for key in ("page_count", "page_batch"):
        if not isinstance(facts[key], int) or facts[key] <= 0:
            raise ValueError(f"run-id fact {key} must be a positive integer")
    for key in ("generation", "tag"):
        if not isinstance(facts[key], str) or not facts[key]:
            raise ValueError(f"run-id fact {key} must be a non-empty string")
    canonical = json.dumps(
        {key: facts[key] for key in RUN_ID_FACT_KEYS},
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _validate_namespace_counts(namespace: str, counts: dict) -> None:
    if not isinstance(counts, dict):
        raise ValueError(f"contract namespaces[{namespace}] must be an object")
    required = {"total_rows", "live_rows"}
    allowed = required | ({"by_identity_mode"} if namespace == "REF6" else set())
    keys = set(counts)
    if not required.issubset(keys) or not keys.issubset(allowed):
        raise ValueError(
            f"contract namespaces[{namespace}] keys must be {sorted(allowed)}"
        )
    for key in ("total_rows", "live_rows"):
        if not isinstance(counts[key], int) or counts[key] < 0:
            raise ValueError(
                f"contract namespaces[{namespace}].{key} must be a non-negative int"
            )
    if counts["live_rows"] > counts["total_rows"]:
        raise ValueError(
            f"contract namespaces[{namespace}] has live_rows > total_rows"
        )
    modes = counts.get("by_identity_mode")
    if namespace == "REF6":
        if not isinstance(modes, dict) or set(modes) != set(IDENTITY_MODES):
            raise ValueError(
                "contract namespaces[REF6].by_identity_mode must carry exactly "
                f"{list(IDENTITY_MODES)}"
            )
        for mode, mode_counts in modes.items():
            if (
                not isinstance(mode_counts, dict)
                or set(mode_counts) != {"total_rows", "live_rows"}
                or any(
                    not isinstance(mode_counts[k], int) or mode_counts[k] < 0
                    for k in ("total_rows", "live_rows")
                )
            ):
                raise ValueError(
                    f"contract namespaces[REF6].by_identity_mode[{mode}] is malformed"
                )
        for key in ("total_rows", "live_rows"):
            if sum(m[key] for m in modes.values()) != counts[key]:
                raise ValueError(
                    f"contract namespaces[REF6].{key} does not equal its identity-mode sum"
                )


def validate_contract_v2(doc: dict, *, expected_namespaces: set[str]) -> None:
    """Reject a malformed v2 contract. ``expected_namespaces`` comes from the
    cohort registry's extrapolated cohorts: every one must be present (an
    explicit zero beats an absent count), and no unknown namespace may appear.
    """
    if not isinstance(doc, dict):
        raise ValueError("contract must be an object")
    if doc.get("schema_version") != CONTRACT_V2_SCHEMA_VERSION:
        raise ValueError("contract schema_version is not v2")
    keys = set(doc)
    if keys != CONTRACT_V2_KEYS:
        missing = sorted(CONTRACT_V2_KEYS - keys)
        unexpected = sorted(keys - CONTRACT_V2_KEYS)
        raise ValueError(
            f"contract v2 key drift: missing={missing} unexpected={unexpected}"
        )
    if not isinstance(doc["run_id"], str) or not _SHA256_RE.fullmatch(doc["run_id"]):
        raise ValueError("contract run_id must be a lowercase SHA-256")
    namespaces = doc["namespaces"]
    if not isinstance(namespaces, dict):
        raise ValueError("contract namespaces must be an object")
    for namespace in namespaces:
        if not _NAMESPACE_RE.fullmatch(str(namespace)):
            raise ValueError(f"contract namespace has invalid syntax: {namespace!r}")
    present = set(namespaces)
    if present != expected_namespaces:
        raise ValueError(
            "contract namespaces disagree with the cohort registry: "
            f"missing={sorted(expected_namespaces - present)} "
            f"unexpected={sorted(present - expected_namespaces)}"
        )
    for namespace, counts in namespaces.items():
        _validate_namespace_counts(namespace, counts)


def load_cohort_registry(path: str | Path) -> dict:
    """Load and validate the routing cohort registry.

    Registry semantics: every ``REF*``-prefixed work id in a match table
    belongs to exactly one registered namespace. ``cohort`` is ``legacy``
    (fitted gen-2 decisions govern; today only REF2) or ``extrapolated``
    (classified separately with the frozen coverage estimand — REF4/5/6).
    An unregistered ``REF*`` prefix in match data is a hard error at the
    classification site, never a silent legacy fallback.
    """
    registry_path = Path(path)
    doc = json.loads(registry_path.read_text(encoding="utf-8"))
    if doc.get("schema_version") != COHORT_REGISTRY_SCHEMA_VERSION:
        raise ValueError("unsupported cohort-registry schema_version")
    cohorts = doc.get("cohorts")
    if not isinstance(cohorts, list) or not cohorts:
        raise ValueError("cohort registry must contain a non-empty cohorts list")
    seen: set[str] = set()
    for cohort in cohorts:
        namespace = cohort.get("namespace")
        if not isinstance(namespace, str) or not _NAMESPACE_RE.fullmatch(namespace):
            raise ValueError(f"cohort has invalid namespace: {namespace!r}")
        if namespace in seen:
            raise ValueError(f"duplicate cohort namespace: {namespace}")
        seen.add(namespace)
        if cohort.get("cohort") not in ("legacy", "extrapolated"):
            raise ValueError(f"cohort {namespace} must be legacy or extrapolated")
        identity_mode = cohort.get("identity_mode")
        if cohort["cohort"] == "legacy":
            if identity_mode is not None:
                raise ValueError(f"legacy cohort {namespace} must not set identity_mode")
            continue
        if identity_mode not in (*IDENTITY_MODES, "per_entry"):
            raise ValueError(
                f"cohort {namespace} identity_mode must be one of "
                f"{[*IDENTITY_MODES, 'per_entry']}"
            )
        source_map = cohort.get("source_map")
        if not isinstance(source_map, str) or not source_map:
            raise ValueError(f"extrapolated cohort {namespace} needs a source_map")
        map_path = registry_path.parent / source_map
        if not map_path.is_file():
            raise ValueError(f"cohort {namespace} source_map not found: {source_map}")
    return doc


def extrapolated_namespaces(registry: dict) -> set[str]:
    return {
        cohort["namespace"]
        for cohort in registry["cohorts"]
        if cohort["cohort"] == "extrapolated"
    }


def classify_work_id(work_id: str, registry: dict) -> tuple[str, str] | None:
    """Return ``(namespace, cohort)`` for a namespaced work id, or ``None``
    for a non-namespaced (private/legacy-corpus) id.

    A ``REF*`` prefix that is not in the registry raises: an unknown reference
    generation in match data means the registry (a reviewed artifact) and the
    reference corpus have drifted, and routing must not guess.
    """
    head, _, _ = str(work_id).partition(":")
    if not _NAMESPACE_RE.fullmatch(head):
        return None
    for cohort in registry["cohorts"]:
        if cohort["namespace"] == head:
            return head, cohort["cohort"]
    raise ValueError(f"work id namespace {head} is not in the cohort registry")
