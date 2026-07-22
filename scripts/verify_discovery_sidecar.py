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

_RELEASE_CONTRACT_COUNTS = [
    ("expected_rows_claims", "discovery_claim"),
    ("expected_rows_evidence", "discovery_evidence"),
    ("expected_rows_works", "works"),
    ("expected_rows_units", "witness_units"),
]


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
    NULL precision; and no OTHER band carries a non-null precision."""
    violations: List[str] = []
    collection_rows = []
    by_band_key: Dict[Tuple[Optional[str], Optional[str]], Tuple] = {}
    for scope, collection_id, source, band, numerator, denominator, precision, ci_low, ci_high, method in rows:
        if scope == "collection":
            collection_rows.append((collection_id, precision, numerator, denominator, ci_low, ci_high, method))
        elif scope == "band":
            by_band_key[(source, band)] = (precision, ci_low, ci_high, numerator, denominator)

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

    for band in (ids.CONFIDENCE_BAND_CORROBORATED, ids.CONFIDENCE_BAND_WEAK):
        key = (ids.EVIDENCE_SOURCE_PROPAGATED, band)
        if key not in by_band_key:
            violations.append(f"release band_precision (M4): missing propagated/{band} band row")
            continue
        precision, ci_low, ci_high, _numerator, _denominator = by_band_key[key]
        if precision is not None or ci_low is not None or ci_high is not None:
            violations.append(
                f"release band_precision (M4): propagated/{band} carries non-null precision/CI"
            )

    for (source, band), expected_precision in _EXPECTED_MEASURED_BAND_PRECISIONS.items():
        key = (source, band)
        if key not in by_band_key:
            violations.append(f"release band_precision (M4): missing measured band {key}")
            continue
        precision = by_band_key[key][0]
        if precision is None or abs(precision - expected_precision) > _PRECISION_TOLERANCE:
            violations.append(
                f"release band_precision (M4): {key} precision {precision} != "
                f"expected {expected_precision}"
            )

    tier_a_key = (ids.EVIDENCE_SOURCE_TRACK1_DIRECT, ids.CONFIDENCE_BAND_TIER_A)
    if tier_a_key not in by_band_key:
        violations.append("release band_precision (M4): missing tier_a band row")
    else:
        precision = by_band_key[tier_a_key][0]
        if precision is not None:
            violations.append(
                f"release band_precision (M4): tier_a precision must be NULL, got {precision}"
            )

    allowed_nonnull_keys = set(_EXPECTED_MEASURED_BAND_PRECISIONS.keys())
    for (source, band), (precision, *_rest) in by_band_key.items():
        if precision is not None and (source, band) not in allowed_nonnull_keys:
            violations.append(
                f"release band_precision (M4): unexpected non-null precision on band "
                f"({source}, {band})={precision}"
            )

    return violations


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
