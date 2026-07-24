#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Path-parameterized all-invariant release verifier for `discovery.db`.

Implements the release-contract checks frozen in
`docs/specs/discovery-sidecar-schema-v1.md` (Phase 134, plan 134-03, Task 2):
column allowlist, valid (evidence_kind x evidence_source x confidence_band)
combinations (R2), no-duplicate-evidence-keys, nonempty-claim evidence,
display-evidence-id ownership (F12), parent claim_type vs child
evidence_kind consistency (G9), F4 source_corpus cross-table consistency,
per-side drift (N1/OQ3), PRAGMA integrity_check + foreign_key_check,
release-contract row counts, band_precision scope discrimination (G8), and
the membership-based frame_content_hash (recomputed via
`scripts.build_discovery_sidecar.compute_frame_content_hash` -- the ONE
canonical recipe, never duplicated here).

Runs over ANY db path -- the SAME code this plan runs over the committed
synthetic fixture is the code 134-07 runs over the real distilled DB.

Usage:
    python scripts/verify_discovery_sidecar.py <DB_PATH> --expected-frame-hash <hex>
"""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple

_SCRIPTS_DIR = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.dirname(_SCRIPTS_DIR)
for _p in (_REPO_ROOT, _SCRIPTS_DIR):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import discovery_ids as ids  # scripts/discovery_ids.py -- FROZEN enum vocab
import build_discovery_sidecar as sidecar_build  # scripts/build_discovery_sidecar.py -- canonical frame-hash recipe


class VerificationError(Exception):
    """Raised by callers that want an exception instead of an exit code."""


# ---------------------------------------------------------------------------
# Frozen constants
# ---------------------------------------------------------------------------

FORBIDDEN_EXACT_COLUMNS = {"text", "cat", "provenance"}
RESTRICTED_TO_WORKS_COLUMNS = {"title", "author", "genre"}
_RAW_WORK_ID_PREFIXES = ("M:", "J:", "REF")

# The verifier's evidence-combination enum check DERIVES its valid
# track1_direct bands straight from `ids.CONFIDENCE_BANDS_BY_SOURCE`, so the
# 135-05 v2 rename lands here automatically and IN LOCKSTEP: it accepts BOTH
# the v1 stored key (`expert_verified`) AND the v2 key
# (`high_confidence_algorithmic`) through the transition (v1-read-compat,
# Codex #8) with no manual edit. The routing_reason enum (now including
# `later_shared_text`) is enforced at the discovery_evidence DDL CHECK layer
# mirroring `ids.ROUTING_REASONS` -- the verifier does not re-validate it.
# (The release-strict `_EXPECTED_BAND_KEYS` / `_EXPECTED_MEASURED_BAND_PRECISIONS`
# below still key on `expert_verified`: they gate the REAL v2 asset, whose
# band_precision rename lands together with the build's band-assignment flip in
# 135-06 -- NOT in 135-05.)
VALID_EVIDENCE_COMBOS = frozenset(
    {(ids.EVIDENCE_KIND_WITNESS, ids.EVIDENCE_SOURCE_TRACK1_DIRECT, b)
     for b in ids.CONFIDENCE_BANDS_BY_SOURCE[ids.EVIDENCE_SOURCE_TRACK1_DIRECT]}
    | {(ids.EVIDENCE_KIND_WITNESS, ids.EVIDENCE_SOURCE_PROPAGATED, b)
       for b in (ids.CONFIDENCE_BAND_CORROBORATED, ids.CONFIDENCE_BAND_WEAK)}
    | {(ids.EVIDENCE_KIND_SHARED_TEXT, ids.EVIDENCE_SOURCE_PROPAGATED, ids.CONFIDENCE_BAND_NOT_EVALUATED)}
)

# The FROZEN collection-level precision (C-7/R1, docs/specs SS1.6) -- a
# single, immutable, already-measured empirical number; a scope='band' row
# must NEVER carry this value (G8).
_COLLECTION_PRECISION_VALUE = 0.926
_PRECISION_TOLERANCE = 1e-6
_COLLECTION_ID = "propagated_witness_collection_v1"

# M4: the THREE measured track1_direct band precisions (docs/specs SS1.6/
# SS4.1) -- these are the ONLY (evidence_source, confidence_band) pairs
# permitted to carry a non-NULL precision in a real/release build; tier_a
# and every propagated band must stay NULL (H3/G8).
_EXPECTED_MEASURED_BAND_PRECISIONS = {
    (ids.EVIDENCE_SOURCE_TRACK1_DIRECT, ids.CONFIDENCE_BAND_EXPERT_VERIFIED): 0.889,
    (ids.EVIDENCE_SOURCE_TRACK1_DIRECT, ids.CONFIDENCE_BAND_SCREENING_RB): 0.859,
    (ids.EVIDENCE_SOURCE_TRACK1_DIRECT, ids.CONFIDENCE_BAND_SCREENING_CANON): 0.647,
}

# Mirrors (never imports/edits) the literal collection_id
# `scripts/build_discovery_sidecar.py::_frozen_real_band_precision_rows`
# uses for the E1-certification-registry `scope='band'` rows (Codex R2 MED
# dict-collapse fix) -- used ONLY to build the exact expected
# (collection_id, evidence_source, confidence_band) key-set below.
_E1_REGISTRY_COLLECTION_ID = "e1_certification_registry_v1"

# Codex R2 MED: the COMPLETE frozen release band_precision row-set, keyed
# on (collection_id, evidence_source, confidence_band) -- used to require
# EXACTLY ONE row per expected key (never a bare (source, band) dict
# collapse, which let a duplicate/extra row for an already-satisfied key
# silently overwrite a valid one and slip through undetected).
_EXPECTED_BAND_KEYS = {
    (_COLLECTION_ID, ids.EVIDENCE_SOURCE_PROPAGATED, ids.CONFIDENCE_BAND_CORROBORATED),
    (_COLLECTION_ID, ids.EVIDENCE_SOURCE_PROPAGATED, ids.CONFIDENCE_BAND_WEAK),
    (_E1_REGISTRY_COLLECTION_ID, ids.EVIDENCE_SOURCE_TRACK1_DIRECT, ids.CONFIDENCE_BAND_EXPERT_VERIFIED),
    (_E1_REGISTRY_COLLECTION_ID, ids.EVIDENCE_SOURCE_TRACK1_DIRECT, ids.CONFIDENCE_BAND_TIER_A),
    (_E1_REGISTRY_COLLECTION_ID, ids.EVIDENCE_SOURCE_TRACK1_DIRECT, ids.CONFIDENCE_BAND_SCREENING_RB),
    (_E1_REGISTRY_COLLECTION_ID, ids.EVIDENCE_SOURCE_TRACK1_DIRECT, ids.CONFIDENCE_BAND_SCREENING_CANON),
}

_RELEASE_CONTRACT_COUNTS = [
    ("expected_rows_claims", "discovery_claim"),
    ("expected_rows_evidence", "discovery_evidence"),
    ("expected_rows_works", "works"),
    ("expected_rows_units", "witness_units"),
]

# ---------------------------------------------------------------------------
# v2 re-distill verifier invariants (Phase 135, plan 135-06). Mirrors
# shared/discovery_band_labels.STRICT_FLOOR / MEASUREMENT_STATUSES (D-07 /
# 135-05 closed vocab) as local literals so the verifier stays lean and
# stdlib-only -- the SAME 0.85 floor + closed status vocab the build enforces.
# ---------------------------------------------------------------------------
_STRICT_FLOOR = 0.85
_MEASUREMENT_STATUSES = frozenset({
    "not_measured", "measured_pass", "measured_fail", "insufficient_evidence",
})
_V1_BAND_LITERAL = ids.CONFIDENCE_BAND_EXPERT_VERIFIED  # "expert_verified"
_V2_BAND_KEY = ids.CONFIDENCE_BAND_HIGH_CONFIDENCE_ALGORITHMIC  # "high_confidence_algorithmic"
_D17_DELTA_YEARS = 100
_REBAND_TARGET_META_KEY = "tier_a_reband_target"


def _has_table(conn: sqlite3.Connection, name: str) -> bool:
    return conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)
    ).fetchone() is not None


def _has_column(conn: sqlite3.Connection, table: str, column: str) -> bool:
    if not _has_table(conn, table):
        return False
    return any(r[1] == column for r in conn.execute(f'PRAGMA table_info("{table}")').fetchall())


def _connect_ro(db_path) -> sqlite3.Connection:
    uri = Path(db_path).resolve().as_uri() + "?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


# ---------------------------------------------------------------------------
# 1. Column allowlist
# ---------------------------------------------------------------------------

def check_column_allowlist(conn: sqlite3.Connection) -> List[str]:
    violations = []
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
    tables = [r[0] for r in cur.fetchall()]
    for tbl in tables:
        cur.execute(f'PRAGMA table_info("{tbl}")')
        for row in cur.fetchall():
            col = row[1]
            col_lower = col.lower()
            if col_lower in FORBIDDEN_EXACT_COLUMNS:
                violations.append(f"{tbl}.{col}: forbidden reference-content column name")
            if col_lower in RESTRICTED_TO_WORKS_COLUMNS and tbl != "works":
                violations.append(f"{tbl}.{col}: title/author/genre column only allowed on works")

    for tbl, col in (("works", "work_id"), ("works", "canonical_work_id"), ("discovery_claim", "work_id")):
        cur.execute(f'SELECT DISTINCT "{col}" FROM "{tbl}"')
        for (val,) in cur.fetchall():
            if val is not None and str(val).startswith(_RAW_WORK_ID_PREFIXES):
                violations.append(f"{tbl}.{col}={val!r}: raw-shaped work_id value leaked")
    return violations


# ---------------------------------------------------------------------------
# 2. Evidence-row-combination invariants (R2/R5, no-duplicate-keys, nonempty)
# ---------------------------------------------------------------------------

def check_evidence_combinations(conn: sqlite3.Connection) -> List[str]:
    violations = []
    cur = conn.cursor()
    cur.execute(
        "SELECT evidence_id, claim_id, evidence_kind, evidence_source, confidence_band, a_page_id "
        "FROM discovery_evidence"
    )
    rows = cur.fetchall()
    seen = {}
    for evidence_id, claim_id, kind, source, band, a_page_id in rows:
        if (kind, source, band) not in VALID_EVIDENCE_COMBOS:
            violations.append(
                f"evidence {evidence_id}: invalid (evidence_kind, evidence_source, confidence_band) "
                f"combination ({kind}, {source}, {band}) (R2)"
            )
        if a_page_id is None or str(a_page_id).strip() == "":
            violations.append(f"evidence {evidence_id}: missing a_page_id (R5)")
        key = (claim_id, evidence_id)
        seen[key] = seen.get(key, 0) + 1
    for key, count in seen.items():
        if count > 1:
            violations.append(f"duplicate (claim_id, evidence_id) key {key} appears {count} times")

    cur.execute(
        """
        SELECT dc.claim_id FROM discovery_claim dc
        LEFT JOIN discovery_evidence de ON de.claim_id = dc.claim_id
        GROUP BY dc.claim_id HAVING COUNT(de.evidence_id) = 0
        """
    )
    for (cid,) in cur.fetchall():
        violations.append(f"claim {cid}: zero evidence rows (nonempty invariant)")
    return violations


# ---------------------------------------------------------------------------
# 2b. F12 display-evidence-id ownership
# ---------------------------------------------------------------------------

def check_display_pointer_ownership(conn: sqlite3.Connection) -> List[str]:
    violations = []
    cur = conn.cursor()
    cur.execute(
        """
        SELECT dc.claim_id, dc.display_evidence_id, de.claim_id
        FROM discovery_claim dc
        LEFT JOIN discovery_evidence de ON de.evidence_id = dc.display_evidence_id
        """
    )
    for claim_id, display_evidence_id, evidence_claim_id in cur.fetchall():
        if evidence_claim_id is None:
            violations.append(
                f"claim {claim_id}: display_evidence_id {display_evidence_id} does not exist (F12)"
            )
        elif evidence_claim_id != claim_id:
            violations.append(
                f"claim {claim_id}: display_evidence_id {display_evidence_id} belongs to "
                f"claim {evidence_claim_id}, not this claim (F12 cross-claim pointer)"
            )
    return violations


# ---------------------------------------------------------------------------
# 2c. G9 parent claim_type <-> child evidence_kind consistency
# ---------------------------------------------------------------------------

def check_parent_resolver_consistency(conn: sqlite3.Connection) -> List[str]:
    violations = []
    cur = conn.cursor()
    cur.execute("SELECT claim_id, claim_type FROM discovery_claim")
    claim_types = dict(cur.fetchall())
    cur.execute("SELECT claim_id, evidence_kind FROM discovery_evidence")
    kinds_by_claim = {}
    for claim_id, kind in cur.fetchall():
        kinds_by_claim.setdefault(claim_id, set()).add(kind)

    for claim_id, claim_type in claim_types.items():
        kinds = kinds_by_claim.get(claim_id, set())
        has_witness = ids.EVIDENCE_KIND_WITNESS in kinds
        if claim_type == ids.CLAIM_TYPE_SHARED_TEXT and has_witness:
            violations.append(
                f"claim {claim_id}: claim_type=shared_text but carries witness evidence (G9)"
            )
        if claim_type in (ids.CLAIM_TYPE_DIRECT_WITNESS, ids.CLAIM_TYPE_QUOTES_THIS_WORK) and not has_witness:
            violations.append(
                f"claim {claim_id}: claim_type={claim_type} but carries ZERO witness evidence rows (G9)"
            )
    return violations


# ---------------------------------------------------------------------------
# 3. F4 -- source_corpus cross-table consistency
# ---------------------------------------------------------------------------

def check_source_corpus_consistency(conn: sqlite3.Connection) -> List[str]:
    violations = []
    cur = conn.cursor()
    cur.execute(
        """
        SELECT dc.claim_id, dc.source_corpus, w.source_corpus
        FROM discovery_claim dc JOIN works w ON w.work_id = dc.work_id
        WHERE dc.source_corpus != w.source_corpus
        """
    )
    for claim_id, claim_src, work_src in cur.fetchall():
        violations.append(
            f"claim {claim_id}: source_corpus {claim_src!r} != parent work source_corpus {work_src!r} (F4)"
        )
    return violations


# ---------------------------------------------------------------------------
# 4. Per-side drift (N1 / OQ3)
# ---------------------------------------------------------------------------

def check_per_side_drift(conn: sqlite3.Connection) -> List[str]:
    violations = []
    cur = conn.cursor()
    cur.execute(
        "SELECT evidence_id, evidence_kind, text_layer, snapshot_hash, other_page_id, snapshot_hash_b "
        "FROM discovery_evidence"
    )
    for evidence_id, kind, text_layer, snapshot_hash, other_page_id, snapshot_hash_b in cur.fetchall():
        if snapshot_hash is None:
            violations.append(f"evidence {evidence_id}: nulled a-side snapshot_hash (N1 drift)")
        if text_layer is None:
            violations.append(f"evidence {evidence_id}: nulled a-side text_layer (N1 drift)")
        if kind == ids.EVIDENCE_KIND_SHARED_TEXT:
            if other_page_id is None:
                violations.append(
                    f"evidence {evidence_id}: shared_text row missing other_page_id (N1 b-side)"
                )
            if snapshot_hash_b is None:
                violations.append(
                    f"evidence {evidence_id}: shared_text row missing snapshot_hash_b (N1 b-side)"
                )
        # b_start/b_end are NULLABLE on shared_text (never checked -- allowed).
    return violations


# ---------------------------------------------------------------------------
# 5. PRAGMA integrity_check / foreign_key_check
# ---------------------------------------------------------------------------

def check_integrity_and_fk(conn: sqlite3.Connection) -> List[str]:
    violations = []
    (integrity_result,) = conn.execute("PRAGMA integrity_check").fetchone()
    if integrity_result != "ok":
        violations.append(f"PRAGMA integrity_check failed: {integrity_result}")
    fk_rows = conn.execute("PRAGMA foreign_key_check").fetchall()
    if fk_rows:
        violations.append(f"PRAGMA foreign_key_check found {len(fk_rows)} violation(s): {fk_rows}")
    return violations


# ---------------------------------------------------------------------------
# 6. Release-contract row counts
# ---------------------------------------------------------------------------

def check_release_contract_counts(conn: sqlite3.Connection):
    violations = []
    meta = dict(conn.execute("SELECT key, value FROM meta").fetchall())
    for meta_key, table in _RELEASE_CONTRACT_COUNTS:
        expected = meta.get(meta_key)
        if expected is None:
            violations.append(f"meta missing release-contract key {meta_key!r}")
            continue
        (actual,) = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()  # noqa: S608 (fixed table names)
        if int(expected) != actual:
            violations.append(f"{meta_key}: meta expects {expected}, actual {table} count is {actual}")
    return violations, meta


# ---------------------------------------------------------------------------
# 7. band_precision scope discrimination (G8)
# ---------------------------------------------------------------------------

def check_band_precision(conn: sqlite3.Connection, meta: dict) -> List[str]:
    violations = []
    cur = conn.cursor()
    cur.execute(
        "SELECT scope, collection_id, evidence_source, confidence_band, numerator, "
        "denominator, precision, ci_low, ci_high, method FROM band_precision"
    )
    rows = cur.fetchall()
    collection_rows = [r for r in rows if r[0] == "collection"]

    if len(collection_rows) == 0:
        violations.append(
            "band_precision: missing scope='collection' propagated-witness precision row (G8)"
        )
    elif len(collection_rows) > 1:
        violations.append(
            f"band_precision: {len(collection_rows)} scope='collection' rows found, expected exactly 1 (G8)"
        )
    else:
        precision = collection_rows[0][6]
        if precision is None or abs(precision - _COLLECTION_PRECISION_VALUE) > _PRECISION_TOLERANCE:
            violations.append(
                f"band_precision: scope='collection' precision {precision} != frozen "
                f"{_COLLECTION_PRECISION_VALUE} (G8)"
            )

    for scope, _collection_id, source, band, _numerator, _denominator, precision, ci_low, ci_high, _method in rows:
        if scope != "band":
            continue
        if precision is not None and abs(precision - _COLLECTION_PRECISION_VALUE) <= _PRECISION_TOLERANCE:
            violations.append(
                f"band_precision: scope='band' row ({source}, {band}) carries the collection "
                f"0.926 interval (G8)"
            )
        if source == ids.EVIDENCE_SOURCE_PROPAGATED and band in (
            ids.CONFIDENCE_BAND_CORROBORATED, ids.CONFIDENCE_BAND_WEAK
        ):
            if precision is not None or ci_low is not None or ci_high is not None:
                violations.append(
                    f"band_precision: propagated scope='band' row ({band}) carries non-null "
                    f"precision/CI (G8) -- no valid band-specific measurement exists"
                )

    # M4: STRICT release-mode-only validation, gated so it never applies to
    # the pinned 134-03 synthetic golden fixture (which keeps its looser,
    # expected.json-driven checks above). Detected via the build's own
    # sidecar_version meta flag (REAL_SIDECAR_VERSION) rather than the mere
    # presence of collection_id (present in BOTH modes).
    if meta.get("sidecar_version") == sidecar_build.REAL_SIDECAR_VERSION:
        violations += _check_band_precision_release_strict(rows)
    return violations


def _check_band_precision_release_strict(rows) -> List[str]:
    """M4: strict release-mode-only band_precision validation -- exactly
    one scope='collection' row (id=`_COLLECTION_ID`, precision~=0.926,
    non-null numerator/denominator/ci/method); BOTH propagated bands
    (corroborated, weak) present with NULL precision/ci; the THREE measured
    track1_direct bands present at their frozen values; tier_a present with
    NULL precision; and no OTHER band carries a non-null precision.

    Codex R2 MED (dict-collapse fix): band rows are grouped by the FULL
    (collection_id, evidence_source, confidence_band) key into a LIST of
    matches per key (never a bare dict keyed on (source, band) alone, which
    let a duplicate/extra row silently overwrite a valid one via a plain
    dict assignment and slip through undetected). Every expected key must
    have EXACTLY ONE matching row -- a count of 0 (missing) or >1
    (duplicate) is rejected BEFORE any value is even inspected; any row
    keyed OUTSIDE the frozen expected set at all is rejected too, whether
    or not its own precision happens to be non-null."""
    violations: List[str] = []
    collection_rows = []
    band_rows_by_key: Dict[Tuple[Optional[str], Optional[str], Optional[str]], List[Tuple]] = {}
    for scope, collection_id, source, band, numerator, denominator, precision, ci_low, ci_high, method in rows:
        if scope == "collection":
            collection_rows.append((collection_id, precision, numerator, denominator, ci_low, ci_high, method))
        elif scope == "band":
            band_rows_by_key.setdefault((collection_id, source, band), []).append(
                (precision, ci_low, ci_high, numerator, denominator)
            )

    matching = [r for r in collection_rows if r[0] == _COLLECTION_ID]
    if len(matching) != 1:
        violations.append(
            f"release band_precision (M4): expected exactly 1 scope='collection' row with "
            f"collection_id={_COLLECTION_ID!r}, found {len(matching)}"
        )
    else:
        _cid, precision, numerator, denominator, ci_low, ci_high, method = matching[0]
        if None in (numerator, denominator, ci_low, ci_high, method):
            violations.append(
                "release band_precision (M4): collection row missing numerator/denominator/ci/method"
            )
        if precision is None or abs(precision - _COLLECTION_PRECISION_VALUE) > _PRECISION_TOLERANCE:
            violations.append(
                f"release band_precision (M4): collection precision {precision} != "
                f"{_COLLECTION_PRECISION_VALUE}"
            )

    # R2 MED: require EXACTLY ONE row for EVERY expected
    # (collection_id, evidence_source, confidence_band) key -- BEFORE any
    # value is checked, so a duplicate/extra row can never silently
    # overwrite a valid one via dict-assignment ordering.
    for key in sorted(_EXPECTED_BAND_KEYS):
        count = len(band_rows_by_key.get(key, []))
        if count != 1:
            violations.append(
                f"release band_precision (M4): expected exactly 1 row for "
                f"(collection_id, evidence_source, confidence_band)={key}, found {count}"
            )

    # Any band row keyed OUTSIDE the frozen expected set at all is rejected
    # -- regardless of its own precision value (a fully-NULL bogus/extra
    # band row would previously slip through the old "only reject
    # non-null-and-unexpected" check silently).
    extra_keys = sorted(set(band_rows_by_key) - _EXPECTED_BAND_KEYS)
    for key in extra_keys:
        violations.append(
            f"release band_precision (M4): unexpected band row for "
            f"(collection_id, evidence_source, confidence_band)={key} -- not part of the "
            "frozen release row-set"
        )

    def _single_row(key):
        matches = band_rows_by_key.get(key)
        return matches[0] if matches and len(matches) == 1 else None

    for band in (ids.CONFIDENCE_BAND_CORROBORATED, ids.CONFIDENCE_BAND_WEAK):
        row = _single_row((_COLLECTION_ID, ids.EVIDENCE_SOURCE_PROPAGATED, band))
        if row is None:
            continue  # already flagged above (missing/duplicate)
        precision, ci_low, ci_high, _numerator, _denominator = row
        if precision is not None or ci_low is not None or ci_high is not None:
            violations.append(
                f"release band_precision (M4): propagated/{band} carries non-null precision/CI"
            )

    for (source, band), expected_precision in _EXPECTED_MEASURED_BAND_PRECISIONS.items():
        row = _single_row((_E1_REGISTRY_COLLECTION_ID, source, band))
        if row is None:
            continue  # already flagged above (missing/duplicate)
        precision = row[0]
        if precision is None or abs(precision - expected_precision) > _PRECISION_TOLERANCE:
            violations.append(
                f"release band_precision (M4): ({source}, {band}) precision {precision} != "
                f"expected {expected_precision}"
            )

    tier_a_key = (_E1_REGISTRY_COLLECTION_ID, ids.EVIDENCE_SOURCE_TRACK1_DIRECT, ids.CONFIDENCE_BAND_TIER_A)
    tier_a_row = _single_row(tier_a_key)
    if tier_a_row is not None:
        precision = tier_a_row[0]
        if precision is not None:
            violations.append(
                f"release band_precision (M4): tier_a precision must be NULL, got {precision}"
            )

    return violations


# ---------------------------------------------------------------------------
# 9. v2 no-mixed-enum-state (asset/bake-level atomicity, band-labels §5 / bake
#    plan §7 gate 14, Codex #8)
# ---------------------------------------------------------------------------

def check_no_mixed_enum_state(conn: sqlite3.Connection) -> List[str]:
    """A v2 asset must not carry BOTH the retired v1 band literal
    `expert_verified` AND the v2 key `high_confidence_algorithmic` -- ANY
    mixed v1/v2 state in the shipped DB's band-bearing columns is a HARD FAIL.
    Scans `discovery_evidence.confidence_band` + `band_precision.confidence_band`
    (the ASSET bytes; the runtime dual-key read-compat from 135-05 is
    unaffected). A pure-v1 asset (only `expert_verified`) or a pure-v2 asset
    (only `high_confidence_algorithmic`) both PASS."""
    bands = set()
    for tbl in ("discovery_evidence", "band_precision"):
        for (b,) in conn.execute(f"SELECT DISTINCT confidence_band FROM {tbl}"):  # noqa: S608 (fixed names)
            if b is not None:
                bands.add(b)
    if _V1_BAND_LITERAL in bands and _V2_BAND_KEY in bands:
        return [
            "no-mixed-enum-state: the v2 asset carries BOTH the v1 literal "
            f"{_V1_BAND_LITERAL!r} AND the v2 key {_V2_BAND_KEY!r} (asset/bake-level "
            "atomicity, Codex #8)"
        ]
    return []


# ---------------------------------------------------------------------------
# 10. v2 never-orphan-shipped (bake plan §7 gate 8 + §4a shadow-orphan)
# ---------------------------------------------------------------------------

def check_never_orphan_shipped(conn: sqlite3.Connection) -> List[str]:
    """(A) A claim owning >=1 shipped evidence row must have its
    `display_evidence_id` point at one of ITS OWN shipped rows -- never a
    review_only sibling (gate 8, EXACT). (B) Shadow-orphan (§4a): a page that
    carries any WITNESS evidence must have >=1 shipped claim (a claim whose
    display evidence is shipped) -- a witness page left with 0 shipped claims
    is a HARD FAIL. Pages with only shared_text/co-citation review_only rows
    (never a shipped witness) are legitimately non-shipping and exempt."""
    violations: List[str] = []
    cur = conn.cursor()
    claims = cur.execute(
        "SELECT claim_id, page_id, display_evidence_id FROM discovery_claim").fetchall()
    ev_by_claim: Dict[str, List[Tuple]] = {}
    routing_by_evid: Dict[str, str] = {}
    for evid, claim_id, routing, kind in cur.execute(
        "SELECT evidence_id, claim_id, routing_status, evidence_kind FROM discovery_evidence"
    ).fetchall():
        ev_by_claim.setdefault(claim_id, []).append((evid, routing, kind))
        routing_by_evid[evid] = routing

    # (A) display-ownership under routing.
    for claim_id, _page_id, display_id in claims:
        rows = ev_by_claim.get(claim_id, [])
        if any(r == ids.ROUTING_STATUS_SHIPPED for (_e, r, _k) in rows):
            if routing_by_evid.get(display_id) != ids.ROUTING_STATUS_SHIPPED:
                violations.append(
                    f"claim {claim_id}: owns a shipped evidence row but its display_evidence_id "
                    "points at a review_only row (never-orphan-shipped, gate 8)"
                )

    # (B) shadow-orphan, scoped to witness-bearing pages.
    page_claims: Dict[str, List[Tuple[str, str]]] = {}
    for claim_id, page_id, display_id in claims:
        page_claims.setdefault(page_id, []).append((claim_id, display_id))
    for page_id, clist in page_claims.items():
        page_has_witness = any(
            kind == ids.EVIDENCE_KIND_WITNESS
            for (claim_id, _d) in clist
            for (_e, _r, kind) in ev_by_claim.get(claim_id, [])
        )
        if not page_has_witness:
            continue
        page_has_shipped_claim = any(
            routing_by_evid.get(display_id) == ids.ROUTING_STATUS_SHIPPED
            for (_c, display_id) in clist
        )
        if not page_has_shipped_claim:
            violations.append(
                f"page {page_id}: has witness claim(s) but 0 shipped claims (shadow-orphan HARD FAIL, §4a)"
            )
    return violations


# ---------------------------------------------------------------------------
# 11. v2 unknown-date-never-demoted (bake plan §4.3)
# ---------------------------------------------------------------------------

def check_unknown_date_never_demoted(conn: sqlite3.Connection) -> List[str]:
    """Every `routing_reason='later_shared_text'` evidence row MUST correspond
    to a `discovery_routing_audit` row on the same page with
    `decision='demoted'` and BOTH years non-NULL -- an unknown-date pair is
    fail-safe and can NEVER produce a demotion. A pre-v2 asset (no
    `discovery_routing_audit` table) degrades gracefully to [] (compat-gate)."""
    if not _has_table(conn, "discovery_routing_audit"):
        return []
    violations: List[str] = []
    cur = conn.cursor()
    demoted_pages = set()
    for page_id, ky, dy in cur.execute(
        "SELECT page_id, kept_year, demoted_year FROM discovery_routing_audit WHERE decision='demoted'"
    ).fetchall():
        if ky is not None and dy is not None:
            demoted_pages.add(page_id)
    rows = cur.execute(
        "SELECT de.evidence_id, de.a_page_id FROM discovery_evidence de "
        "WHERE de.routing_reason=?", (ids.ROUTING_REASON_LATER_SHARED_TEXT,)
    ).fetchall()
    for evid, page_id in rows:
        if page_id not in demoted_pages:
            violations.append(
                f"evidence {evid}: routing_reason='later_shared_text' on page {page_id} has NO "
                "discovery_routing_audit decision='demoted' row with both years non-NULL "
                "(unknown-date-never-demoted)"
            )
    return violations


# ---------------------------------------------------------------------------
# 12. v2 routing-audit replayability (bake plan §4.3/§7 gate 10, Codex #5)
# ---------------------------------------------------------------------------

def check_routing_audit_replayability(conn: sqlite3.Connection) -> List[str]:
    """Cross-check `discovery_routing_audit` internal consistency: every
    `decision='demoted'` row has BOTH years non-NULL, `demoted_year -
    kept_year >= DELTA` (100), `delta_years` recomputes exactly, and a
    non-NULL `demoted_work_id`; every `fail_safe_unknown_date`/`kept_tie` row
    has `demoted_work_id IS NULL`. So every demotion is replayable DB-only. A
    pre-v2 asset (no audit table) degrades gracefully to [] (compat-gate)."""
    if not _has_table(conn, "discovery_routing_audit"):
        return []
    violations: List[str] = []
    for (page_id, kept, demoted, ky, dy, delta, decision) in conn.execute(
        "SELECT page_id, kept_work_id, demoted_work_id, kept_year, demoted_year, "
        "delta_years, decision FROM discovery_routing_audit"
    ).fetchall():
        if decision == "demoted":
            if ky is None or dy is None:
                violations.append(
                    f"routing_audit page {page_id}: demoted row has a NULL year (not replayable)")
                continue
            if demoted is None:
                violations.append(
                    f"routing_audit page {page_id}: demoted row missing demoted_work_id")
            computed = abs(dy - ky)
            if delta != computed:
                violations.append(
                    f"routing_audit page {page_id}: delta_years {delta} != |{dy}-{ky}|={computed}")
            if computed < _D17_DELTA_YEARS:
                violations.append(
                    f"routing_audit page {page_id}: demoted delta {computed} < DELTA "
                    f"{_D17_DELTA_YEARS} (a within-DELTA pair must never be 'demoted')")
        elif decision in ("kept_tie", "fail_safe_unknown_date"):
            if demoted is not None:
                violations.append(
                    f"routing_audit page {page_id}: {decision} row must have NULL demoted_work_id")
        else:
            violations.append(
                f"routing_audit page {page_id}: unknown decision {decision!r}")
    return violations


# ---------------------------------------------------------------------------
# 13. v2 measurement_status <-> ci_low consistency (bake plan §7 gate 12, Codex #B3)
# ---------------------------------------------------------------------------

def check_measurement_status_ci_consistency(conn: sqlite3.Connection) -> List[str]:
    """EXHAUSTIVE over the closed measurement_status vocab: `measured_pass`
    requires all five precision/CI/num/denom fields non-NULL AND `ci_low >=
    STRICT_FLOOR`; `measured_fail` requires all five non-NULL AND `ci_low <
    STRICT_FLOOR`; `not_measured`/`insufficient_evidence` require ALL FIVE
    NULL; any other stored status is a HARD FAIL. A NULL measurement_status
    (legacy/v1-compat, the current real-build default) is exempt. A pre-v2
    asset (no `measurement_status` column) degrades gracefully to []."""
    if not _has_column(conn, "band_precision", "measurement_status"):
        return []
    violations: List[str] = []
    for (cb, status, precision, ci_low, ci_high, num, den) in conn.execute(
        "SELECT confidence_band, measurement_status, precision, ci_low, ci_high, "
        "numerator, denominator FROM band_precision"
    ).fetchall():
        if status is None:
            continue
        if status not in _MEASUREMENT_STATUSES:
            violations.append(f"band_precision ({cb}): unknown measurement_status {status!r}")
            continue
        five = (precision, ci_low, ci_high, num, den)
        all_present = all(v is not None for v in five)
        all_null = all(v is None for v in five)
        if status == "measured_pass":
            if not all_present or ci_low is None or ci_low < _STRICT_FLOOR:
                violations.append(
                    f"band_precision ({cb}): measured_pass requires all five fields non-NULL AND "
                    f"ci_low>={_STRICT_FLOOR} (Codex #B3)")
        elif status == "measured_fail":
            if not all_present or ci_low is None or ci_low >= _STRICT_FLOOR:
                violations.append(
                    f"band_precision ({cb}): measured_fail requires all five fields non-NULL AND "
                    f"ci_low<{_STRICT_FLOOR} (Codex #B3)")
        else:  # not_measured / insufficient_evidence
            if not all_null:
                violations.append(
                    f"band_precision ({cb}): {status} must have ALL five precision/CI/num/denom NULL")
    return violations


# ---------------------------------------------------------------------------
# 14. v2 reband-precision-invalidation (bake plan §7 gate 13, gate-13 iff, Codex #B2)
# ---------------------------------------------------------------------------

def check_reband_precision_invalidation(conn: sqlite3.Connection, meta: dict) -> List[str]:
    """IFF `meta.tier_a_reband_target='screening_rb'` is set (a
    population-changing reband occurred), BOTH the target `screening_rb` AND
    the source `tier_a` band_precision rows MUST be
    `measurement_status='not_measured'` with `precision IS NULL` -- a retained
    pre-reband number is a HARD FAIL. The check is a NO-OP when the marker is
    absent (gate-13 iff)."""
    target = meta.get(_REBAND_TARGET_META_KEY)
    if not target:
        return []
    if not _has_column(conn, "band_precision", "measurement_status"):
        return []
    violations: List[str] = []
    for band in (target, ids.CONFIDENCE_BAND_TIER_A):
        rows = conn.execute(
            "SELECT measurement_status, precision FROM band_precision "
            "WHERE confidence_band=? AND evidence_source=?",
            (band, ids.EVIDENCE_SOURCE_TRACK1_DIRECT),
        ).fetchall()
        for status, precision in rows:
            if status != "not_measured" or precision is not None:
                violations.append(
                    f"reband-precision-invalidation: meta.{_REBAND_TARGET_META_KEY}={target!r} is set "
                    f"but band_precision ({band}) retains measurement_status={status!r}/precision="
                    f"{precision!r} (a population-changing reband can never keep the prior number, "
                    "Codex #B2)")
    return violations


# ---------------------------------------------------------------------------
# 15. v2 evidence_id-content-consistency (bake plan §7 gate 15, Codex-R4 new-HIGH)
# ---------------------------------------------------------------------------

def check_evidence_id_content_consistency(conn: sqlite3.Connection) -> List[str]:
    """For EVERY discovery_evidence row, RECOMPUTE the frozen §2 evidence_id
    recipe over the row's stored fields and require it to EQUAL the stored id
    -- a mismatch catches a stale id left by a bare in-place `UPDATE
    confidence_band` (reband not fed as a rebuild input). AND for every claim,
    `select_display_evidence` over its CURRENT evidence rows must reproduce the
    stored `display_evidence_id` -- a stale display pointer (e.g. still at a
    now-review_only row after a reband) is a HARD FAIL."""
    violations: List[str] = []
    cur = conn.cursor()
    for (evid, work_id, a_page_id, sys_id, kind, source, band, span_start, span_end,
         other_page_id, seed_spans_json) in cur.execute(
        "SELECT de.evidence_id, dc.work_id, de.a_page_id, de.sys_id, de.evidence_kind, "
        "de.evidence_source, de.confidence_band, de.span_start, de.span_end, "
        "de.other_page_id, de.seed_spans "
        "FROM discovery_evidence de JOIN discovery_claim dc ON dc.claim_id = de.claim_id"
    ).fetchall():
        seed_spans = json.loads(seed_spans_json) if seed_spans_json else None
        recomputed = ids.evidence_id(
            work_id=work_id, a_page_id=a_page_id, sys_id=sys_id, evidence_kind=kind,
            evidence_source=source, confidence_band=band, span_start=span_start,
            span_end=span_end, other_page_id=other_page_id, seed_spans=seed_spans)
        if recomputed != evid:
            violations.append(
                f"evidence {evid}: stored evidence_id != recomputed frozen §2 recipe "
                "(stale id -- a reband applied as a bare in-place UPDATE, not a rebuild input)")

    ev_by_claim: Dict[str, List[Dict]] = {}
    for (claim_id, evid, source, band, adjudication, routing) in cur.execute(
        "SELECT claim_id, evidence_id, evidence_source, confidence_band, adjudication_status, "
        "routing_status FROM discovery_evidence"
    ).fetchall():
        ev_by_claim.setdefault(claim_id, []).append({
            "evidence_id": evid, "evidence_source": source, "confidence_band": band,
            "adjudication_status": adjudication, "routing_status": routing,
        })
    for (claim_id, display_id) in cur.execute(
        "SELECT claim_id, display_evidence_id FROM discovery_claim"
    ).fetchall():
        rows = ev_by_claim.get(claim_id)
        if not rows:
            continue  # zero-evidence claims already caught by check_evidence_combinations
        recomputed_display = ids.select_display_evidence(rows)
        if recomputed_display != display_id:
            violations.append(
                f"claim {claim_id}: stored display_evidence_id != select_display_evidence recompute "
                "(stale display pointer -- reband/demotion not fed as a rebuild input)")
    return violations


# ---------------------------------------------------------------------------
# 16. v2 coverage-gap report (bake plan §4a -- LOG+REPORT, non-fatal)
# ---------------------------------------------------------------------------

def check_coverage_gap_report(conn: sqlite3.Connection) -> List[str]:
    """Non-fatal (§4a): report the D-17 routing-audit demotion/tie/fail-safe
    split for operator visibility. NEVER a violation (returns [])."""
    if not _has_table(conn, "discovery_routing_audit"):
        return []
    counts = dict(conn.execute(
        "SELECT decision, COUNT(*) FROM discovery_routing_audit GROUP BY decision").fetchall())
    if counts:
        print(f"coverage-gap report (non-fatal): routing_audit decision counts = {counts}",
              file=sys.stderr)
    return []


# ---------------------------------------------------------------------------
# 8. Membership frame_content_hash
# ---------------------------------------------------------------------------

def check_frame_content_hash(conn: sqlite3.Connection, meta: dict, expected_frame_hash) -> List[str]:
    violations = []
    recomputed = sidecar_build.compute_frame_content_hash(conn)
    meta_hash = meta.get("frame_content_hash")
    if recomputed != meta_hash:
        violations.append(
            f"frame_content_hash mismatch: recomputed {recomputed} != meta.frame_content_hash {meta_hash}"
        )
    if expected_frame_hash is not None and recomputed != expected_frame_hash:
        violations.append(
            f"frame_content_hash mismatch: recomputed {recomputed} != "
            f"--expected-frame-hash {expected_frame_hash}"
        )
    return violations


# ---------------------------------------------------------------------------
# verify() -- the single all-invariant entry point
# ---------------------------------------------------------------------------

def verify(db_path, expected_frame_hash=None) -> int:
    """Run ALL release-contract invariants over `db_path`. Returns 0 on a
    clean DB, 1 (fail-closed) on ANY violation. Prints every violation found
    to stderr."""
    violations: List[str] = []
    conn = _connect_ro(db_path)
    try:
        violations += check_column_allowlist(conn)
        violations += check_evidence_combinations(conn)
        violations += check_display_pointer_ownership(conn)
        violations += check_parent_resolver_consistency(conn)
        violations += check_source_corpus_consistency(conn)
        violations += check_per_side_drift(conn)
        violations += check_integrity_and_fk(conn)
        count_violations, meta = check_release_contract_counts(conn)
        violations += count_violations
        violations += check_band_precision(conn, meta)
        violations += check_frame_content_hash(conn, meta, expected_frame_hash)
        # v2 re-distill invariants (135-06).
        violations += check_no_mixed_enum_state(conn)
        violations += check_never_orphan_shipped(conn)
        violations += check_unknown_date_never_demoted(conn)
        violations += check_routing_audit_replayability(conn)
        violations += check_measurement_status_ci_consistency(conn)
        violations += check_reband_precision_invalidation(conn, meta)
        violations += check_evidence_id_content_consistency(conn)
        violations += check_coverage_gap_report(conn)  # non-fatal report
    finally:
        conn.close()

    if violations:
        for v in violations:
            print(f"VIOLATION: {v}", file=sys.stderr)
        print(f"verify_discovery_sidecar: {len(violations)} violation(s) -- FAILED (fail-closed)",
              file=sys.stderr)
        return 1

    print("verify_discovery_sidecar: all invariants pass -- clean.")
    return 0


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__,
                                      formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("db_path", help="Path to the discovery.db sidecar to verify")
    parser.add_argument("--expected-frame-hash", metavar="HEX", default=None,
                        help="Expected membership frame_content_hash (hex); required for a full "
                             "release-gate run, optional for ad-hoc local verification")
    return parser


def main(argv=None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return verify(args.db_path, args.expected_frame_hash)


if __name__ == "__main__":
    sys.exit(main())
