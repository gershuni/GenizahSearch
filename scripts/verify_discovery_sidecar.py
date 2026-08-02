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
import re
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
# (The release-strict expected band_precision key-set is version-AWARE as of
# 135-07: `_expected_band_keys` / `_expected_measured_band_precisions` select
# the top-tier KEY from the DETECTED asset band-vocabulary version -- v1 =>
# `expert_verified`, v2 => `high_confidence_algorithmic` -- while every value/
# count check stays exactly as strict. `check_band_vocabulary` then
# cross-checks the meta marker against the ACTUAL bands present, so the asset
# can never choose its own contract.)
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

# Mirrors (never imports/edits) the literal collection_id
# `scripts/build_discovery_sidecar.py::_frozen_real_band_precision_rows`
# uses for the E1-certification-registry `scope='band'` rows (Codex R2 MED
# dict-collapse fix) -- used ONLY to build the exact expected
# (collection_id, evidence_source, confidence_band) key-set below.
_E1_REGISTRY_COLLECTION_ID = "e1_certification_registry_v1"

# D-02a (136-06, docs/specs/discovery-sidecar-schema-v1.md SS1.6 amendment
# 2026-08-02): the ONE authorized `tier_a` CERT-01 pair -- stores the
# authorization `is_default_eligible()` reads, NEVER a measured precision
# number. Mirrors (never imports) `scripts/build_discovery_sidecar.py`'s
# frozen `tier_a` row so this check stays independent of the builder.
_TIER_A_AUTHORIZED_MEASUREMENT_STATUS = "measured_pass"
_TIER_A_AUTHORIZED_CI_LOW = 0.9084


def _top_tier_band_key(v2: bool) -> str:
    """135-07: the ONE helper that selects the version-specific track1_direct
    top-tier band KEY -- v1 `expert_verified`, v2 `high_confidence_algorithmic`
    -- so the release-strict expected sets never drift out of a global."""
    return _V2_BAND_KEY if v2 else _V1_BAND_LITERAL


def _expected_measured_band_precisions(v2: bool) -> Dict[Tuple[str, str], float]:
    """M4: the THREE measured track1_direct band precisions (docs/specs
    SS1.6/SS4.1) -- the ONLY (evidence_source, confidence_band) pairs permitted
    to carry a non-NULL precision in a real/release build; tier_a and every
    propagated band must stay NULL (H3/G8). Only the top-tier KEY tracks the
    detected version -- the 0.889/0.859/0.647 VALUES are frozen either way."""
    return {
        (ids.EVIDENCE_SOURCE_TRACK1_DIRECT, _top_tier_band_key(v2)): 0.889,
        (ids.EVIDENCE_SOURCE_TRACK1_DIRECT, ids.CONFIDENCE_BAND_SCREENING_RB): 0.859,
        (ids.EVIDENCE_SOURCE_TRACK1_DIRECT, ids.CONFIDENCE_BAND_SCREENING_CANON): 0.647,
    }


def _expected_band_keys(v2: bool) -> set:
    """Codex R2 MED: the COMPLETE frozen release band_precision row-set, keyed
    on (collection_id, evidence_source, confidence_band) -- used to require
    EXACTLY ONE row per expected key (never a bare (source, band) dict
    collapse, which let a duplicate/extra row for an already-satisfied key
    silently overwrite a valid one and slip through undetected). The top-tier
    key tracks the detected band-vocabulary version (135-07)."""
    return {
        (_COLLECTION_ID, ids.EVIDENCE_SOURCE_PROPAGATED, ids.CONFIDENCE_BAND_CORROBORATED),
        (_COLLECTION_ID, ids.EVIDENCE_SOURCE_PROPAGATED, ids.CONFIDENCE_BAND_WEAK),
        (_E1_REGISTRY_COLLECTION_ID, ids.EVIDENCE_SOURCE_TRACK1_DIRECT, _top_tier_band_key(v2)),
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

# 135-07: the explicit operator-intent band-vocabulary marker + its allowed
# values. A REAL v2 build ALSO records the verified `canonical_merges_sha256`;
# the verifier validates that as a genuine 64-hex SHA (not mere key presence),
# so a hand-forged `band_vocab_version='v2'` meta row alone cannot fake v2.
_BAND_VOCAB_META_KEY = "band_vocab_version"
_BAND_VOCAB_V1 = "v1"
_BAND_VOCAB_V2 = "v2"
_CANONICAL_MERGES_SHA_META_KEY = "canonical_merges_sha256"
_SHA256_HEX_RE = re.compile(r"^[0-9a-f]{64}$")


def _detected_band_vocab(meta: dict) -> str:
    """The effective band-vocabulary version for an asset: the explicit meta
    marker when present + valid, else v1 (a marker-less asset -- the synthetic
    golden fixture or a legacy v1 build -- is v1 by contract). An UNKNOWN
    marker value is returned verbatim so `check_band_vocabulary` can hard-fail
    it rather than silently coercing to a valid version."""
    marker = meta.get(_BAND_VOCAB_META_KEY)
    if marker is None:
        return _BAND_VOCAB_V1
    return marker


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
    # D-02a (136-06): `measurement_status` is read here so the M4 strict
    # checks below (which now assert the tier_a CERT-01 authorization pair)
    # have it available -- but the pinned 134-03 golden fixture predates the
    # 135-05 column and must stay byte-identical, so this degrades to a
    # literal NULL select on a pre-135-05 asset rather than erroring.
    has_measurement_status = _has_column(conn, "band_precision", "measurement_status")
    measurement_status_expr = "measurement_status" if has_measurement_status else "NULL"
    cur.execute(
        "SELECT scope, collection_id, evidence_source, confidence_band, numerator, "
        f"denominator, precision, ci_low, ci_high, method, {measurement_status_expr} "
        "FROM band_precision"
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

    for scope, _collection_id, source, band, _numerator, _denominator, precision, ci_low, ci_high, _method, _measurement_status in rows:
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
        v2_asset = _detected_band_vocab(meta) == _BAND_VOCAB_V2
        violations += _check_band_precision_release_strict(rows, v2_bands=v2_asset)
    return violations


def _check_band_precision_release_strict(rows, *, v2_bands: bool = False) -> List[str]:
    """M4: strict release-mode-only band_precision validation -- exactly
    one scope='collection' row (id=`_COLLECTION_ID`, precision~=0.926,
    non-null numerator/denominator/ci/method); BOTH propagated bands
    (corroborated, weak) present with NULL precision/ci; the THREE measured
    track1_direct bands present at their frozen values; tier_a present with
    NULL precision AND the D-02a `measurement_status='measured_pass'`/
    `ci_low=0.9084` authorization (with `ci_high`/`numerator`/`denominator`
    NULL); no OTHER band carries a non-null precision; and no OTHER band
    carries `measurement_status='measured_pass'` (the D-02a smuggling
    check -- only `tier_a` may hold this slot, since it is exactly what
    `is_default_eligible()` reads).

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
    expected_band_keys = _expected_band_keys(v2_bands)
    expected_measured = _expected_measured_band_precisions(v2_bands)
    collection_rows = []
    band_rows_by_key: Dict[Tuple[Optional[str], Optional[str], Optional[str]], List[Tuple]] = {}
    for scope, collection_id, source, band, numerator, denominator, precision, ci_low, ci_high, method, measurement_status in rows:
        if scope == "collection":
            collection_rows.append((collection_id, precision, numerator, denominator, ci_low, ci_high, method))
        elif scope == "band":
            band_rows_by_key.setdefault((collection_id, source, band), []).append(
                (precision, ci_low, ci_high, numerator, denominator, measurement_status)
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
    for key in sorted(expected_band_keys):
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
    extra_keys = sorted(set(band_rows_by_key) - expected_band_keys)
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
        precision, ci_low, ci_high, _numerator, _denominator, _measurement_status = row
        if precision is not None or ci_low is not None or ci_high is not None:
            violations.append(
                f"release band_precision (M4): propagated/{band} carries non-null precision/CI"
            )

    for (source, band), expected_precision in expected_measured.items():
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
        precision, ci_low, ci_high, numerator, denominator, measurement_status = tier_a_row
        # Pre-existing check -- unrelaxed, unmodified wording.
        if precision is not None:
            violations.append(
                f"release band_precision (M4): tier_a precision must be NULL, got {precision}"
            )
        # D-02a (136-06, docs/specs/discovery-sidecar-schema-v1.md SS1.6
        # amendment 2026-08-02): tier_a additionally REQUIRES the CERT-01
        # AUTHORIZATION pair (`measurement_status='measured_pass'`,
        # `ci_low=0.9084`) that `is_default_eligible()` reads, with
        # `ci_high`/`numerator`/`denominator` all NULL -- one violation per
        # mismatched field, never the found value (masking discipline).
        if measurement_status != _TIER_A_AUTHORIZED_MEASUREMENT_STATUS:
            violations.append(
                "release band_precision (M4): tier_a measurement_status must be "
                f"{_TIER_A_AUTHORIZED_MEASUREMENT_STATUS!r}"
            )
        if ci_low is None or abs(ci_low - _TIER_A_AUTHORIZED_CI_LOW) > _PRECISION_TOLERANCE:
            violations.append(
                f"release band_precision (M4): tier_a ci_low must be {_TIER_A_AUTHORIZED_CI_LOW}"
            )
        if ci_high is not None:
            violations.append("release band_precision (M4): tier_a ci_high must be NULL")
        if numerator is not None:
            violations.append("release band_precision (M4): tier_a numerator must be NULL")
        if denominator is not None:
            violations.append("release band_precision (M4): tier_a denominator must be NULL")

    # D-02a inverse risk (136-06): no OTHER band row may carry
    # measurement_status='measured_pass' -- otherwise an arbitrary band could
    # be smuggled into default visibility through this exact slot
    # (is_default_eligible() reads it). Independent of the builder's own
    # frozen row-set -- `_TIER_A_AUTHORIZED_MEASUREMENT_STATUS`/`tier_a_key`
    # above are local literals, never imported from build_discovery_sidecar.
    for key, matches in band_rows_by_key.items():
        if key == tier_a_key:
            continue
        for match in matches:
            if match[5] == "measured_pass":
                violations.append(
                    "release band_precision (M4): unauthorized measurement_status="
                    f"'measured_pass' on band {key} -- only tier_a may carry this "
                    "authorization (D-02a)"
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
# 9b. v2 band-vocabulary intent gate (135-07): the asset must PROVE it matches
#     operator intent -- never choose its own contract (Codex #3).
# ---------------------------------------------------------------------------

def _scan_text_columns_for_literal(conn: sqlite3.Connection, literal: str) -> List[str]:
    """Defense-in-depth (Codex #3): scan EVERY TEXT/untyped column of every
    non-sqlite table for an EXACT-token match of `literal` (an external
    `--precision-spec` can persist free-form fields like `notes`/`method`
    beyond the two band columns). Uses a word-boundary match so a substring
    inside an unrelated word is not a false positive, but any standalone
    occurrence of the retired v1 literal in a v2 asset is a HARD FAIL."""
    violations: List[str] = []
    token = re.compile(r"\b" + re.escape(literal) + r"\b")
    tables = [
        r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        ).fetchall()
    ]
    for tbl in tables:
        cols = conn.execute(f'PRAGMA table_info("{tbl}")').fetchall()
        # affinity/type is col[2]; TEXT or empty (untyped) columns can hold strings.
        text_cols = [c[1] for c in cols if (c[2] or "").upper() in ("", "TEXT")]
        for col in text_cols:
            for (val,) in conn.execute(
                f'SELECT DISTINCT "{col}" FROM "{tbl}" WHERE "{col}" IS NOT NULL'  # noqa: S608
            ):
                if isinstance(val, str) and token.search(val):
                    violations.append(
                        f"band-vocabulary(v2): retired v1 literal {literal!r} persisted in "
                        f"{tbl}.{col} (defense-in-depth text-column scan)"
                    )
                    break  # one hit per column is enough to fail
    return violations


def check_band_vocabulary(
    conn: sqlite3.Connection, meta: dict, *, expected_band_vocabulary: Optional[str] = None
) -> List[str]:
    """135-07 (Codex #3): version-aware band-vocabulary gate that does NOT
    trust the asset's self-report alone. Detect the version from meta
    `band_vocab_version`, then CROSS-CHECK it against the ACTUAL band literals
    present across BOTH band-bearing tables:

      * marker='v2' => REQUIRE the v2 key present, FORBID the v1 literal,
        require `canonical_merges_sha256` to be a REAL 64-hex SHA (not mere key
        presence), and scan every TEXT column for the retired v1 literal;
      * marker='v1' (or absent -> v1 by contract) => inverse: FORBID the v2 key
        without a marker;
      * a marker present with NEITHER top-tier key => HARD FAIL;
      * an unknown marker value => HARD FAIL.

    `expected_band_vocabulary` (the external `--require-v2` / operator-intent
    gate) is the FALSE-GREEN closer: when set, the DETECTED version must equal
    it, so an intended-v2 bake that accidentally produced an internally-
    consistent v1 asset (no marker) FAILS instead of passing."""
    violations: List[str] = []
    marker = meta.get(_BAND_VOCAB_META_KEY)
    detected = _detected_band_vocab(meta)

    bands = set()
    for tbl in ("discovery_evidence", "band_precision"):
        for (b,) in conn.execute(f"SELECT DISTINCT confidence_band FROM {tbl}"):  # noqa: S608 (fixed names)
            if b is not None:
                bands.add(b)
    has_v1 = _V1_BAND_LITERAL in bands
    has_v2 = _V2_BAND_KEY in bands

    # External operator-intent gate (--require-v2 / --expected-band-vocabulary).
    if expected_band_vocabulary is not None and detected != expected_band_vocabulary:
        violations.append(
            f"band-vocabulary: operator declared expected version "
            f"{expected_band_vocabulary!r} but the asset's detected version is "
            f"{detected!r} (meta marker={marker!r}) -- the asset must prove it matches "
            "operator intent, not choose its own contract (--require-v2)"
        )

    if detected == _BAND_VOCAB_V2:
        if not has_v2:
            violations.append(
                f"band-vocabulary(v2): marker set but the v2 key {_V2_BAND_KEY!r} is absent "
                "from every band-bearing column"
            )
        if has_v1:
            violations.append(
                f"band-vocabulary(v2): marker set but the retired v1 literal "
                f"{_V1_BAND_LITERAL!r} is still present"
            )
        sha = meta.get(_CANONICAL_MERGES_SHA_META_KEY)
        if not (isinstance(sha, str) and _SHA256_HEX_RE.match(sha)):
            violations.append(
                f"band-vocabulary(v2): meta.{_CANONICAL_MERGES_SHA_META_KEY} is not a real "
                "64-hex SHA-256 -- a v2 marker without the verified census SHA cannot prove v2"
            )
        violations += _scan_text_columns_for_literal(conn, _V1_BAND_LITERAL)
    elif detected == _BAND_VOCAB_V1:
        # v1 contract (marker='v1' or absent): the v2 key must NOT appear
        # without an explicit v2 marker.
        if has_v2:
            violations.append(
                f"band-vocabulary(v1): the v2 key {_V2_BAND_KEY!r} is present but the asset is "
                f"not marked band_vocab_version='v2' (meta marker={marker!r})"
            )
        # A marker EXPLICITLY set to v1 must carry the v1 top-tier key (reject
        # neither-top-tier-key-present); a marker-less asset is exempt (a
        # minimal/legacy asset may legitimately carry neither top-tier band --
        # the release-strict band_precision keyset gates real assets separately).
        if marker == _BAND_VOCAB_V1 and not has_v1 and not has_v2:
            violations.append(
                f"band-vocabulary(v1): marker set but neither the v1 literal {_V1_BAND_LITERAL!r} "
                f"nor the v2 key {_V2_BAND_KEY!r} is present (no top-tier band at all)"
            )
    else:
        violations.append(
            f"band-vocabulary: unknown band_vocab_version marker {marker!r} "
            f"(expected {_BAND_VOCAB_V1!r} or {_BAND_VOCAB_V2!r})"
        )
    return violations


# ---------------------------------------------------------------------------
# 10. v2 never-orphan-shipped (bake plan §7 gate 8 + §4a shadow-orphan)
# ---------------------------------------------------------------------------

def check_never_orphan_shipped(conn: sqlite3.Connection) -> List[str]:
    """NON-DISPLACEMENT invariant (gate 8; narrowed in 135-07 per bake plan
    §4.4 + discovery-band-labels-v1.md §3.1): a claim owning >=1 SHIPPED
    evidence row must have its `display_evidence_id` point at one of ITS OWN
    shipped rows -- never a `review_only` sibling. A `review_only` row must
    never DISPLACE a shipped claim.

    Deliberately NARROW: it does NOT require a witness-bearing page to carry any
    shipped claim. Under the now-correct Lever-1 coverage routing (bake plan
    §4.4), a page whose EVERY claim is legitimately low-coverage may be entirely
    `review_only` (recoverable, queryable behind the toggle) -- that is a valid
    routing outcome, not a shadow-orphan. The earlier `check` had a second
    branch (§4a) that HARD-FAILED any witness page with 0 shipped claims; it was
    overbroad (it would reject every legitimately all-low page) and is REMOVED.
    The multi-register invariant (band-labels §4) holds because
    `display_evidence_id` is per-claim `(page_id, work_id)`, never per-page: this
    check catches the real bug -- a claim that OWNS a shipped row yet points its
    display at a review_only sibling -- without penalising all-low pages."""
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

    # Per-claim display-ownership under routing: a claim owning any shipped row
    # must display a shipped row (never a review_only sibling).
    for claim_id, _page_id, display_id in claims:
        rows = ev_by_claim.get(claim_id, [])
        if any(r == ids.ROUTING_STATUS_SHIPPED for (_e, r, _k) in rows):
            if routing_by_evid.get(display_id) != ids.ROUTING_STATUS_SHIPPED:
                violations.append(
                    f"claim {claim_id}: owns a shipped evidence row but its display_evidence_id "
                    "points at a review_only row (never-orphan-shipped displacement, gate 8)"
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
# 12b. v2 fail-closed cascade / kept_invalid_reference gate (bake plan §4.3,
# Codex R4/R5 span-orphaning fix)
# ---------------------------------------------------------------------------

def check_demotion_kept_reference_shipped(conn: sqlite3.Connection) -> List[str]:
    """Every `discovery_routing_audit` row with `decision='demoted'` MUST name a
    `kept_work_id` whose canonical group STILL ships >=1 display-evidence row --
    i.e. the demoter reference is NOT itself fully demoted/review_only.

    Closes the three-work cascade Codex R4/R5 flagged: the D-17 pairwise pass
    can let an already-demoted canonical group act as the 'kept' (demoter) of a
    third group, producing a 'demoted' audit row whose `kept_work_id` is itself
    review_only -- a non-replayable/misleading provenance row (the shared span
    would be left with NO shipped witness). This gate is FAIL-CLOSED: if a REAL
    bake trips it, the bake HALTs and the owner adjudicates the concrete cascade
    cases, rather than silently shipping misleading audit provenance. The
    demotion ROUTING is left AS-IS (routing-invariant) -- this is a verifier-only
    guard.

    Canonical grouping is derived DB-only from `works.canonical_work_id` (a raw
    member work_id -> its canonical); a group 'ships' iff >=1 of its claims has a
    `display_evidence_id` pointing at a `routing_status='shipped'` evidence row.
    Masking-safe: opaque ids + page ids only. A pre-v2 asset (no
    `discovery_routing_audit` table) degrades gracefully to [] (compat-gate)."""
    if not _has_table(conn, "discovery_routing_audit"):
        return []
    violations: List[str] = []
    cur = conn.cursor()
    shipped_canonicals = {
        row[0]
        for row in cur.execute(
            "SELECT DISTINCT w.canonical_work_id "
            "FROM discovery_claim dc "
            "JOIN works w ON w.work_id = dc.work_id "
            "JOIN discovery_evidence de ON de.evidence_id = dc.display_evidence_id "
            "WHERE de.routing_status = ?", (ids.ROUTING_STATUS_SHIPPED,)
        ).fetchall()
    }
    for (page_id, kept) in cur.execute(
        "SELECT page_id, kept_work_id FROM discovery_routing_audit WHERE decision='demoted'"
    ).fetchall():
        if kept is not None and kept not in shipped_canonicals:
            violations.append(
                f"routing_audit page {page_id}: demoted row's kept_work_id {kept} is "
                "itself fully review_only (its canonical group has no shipped display "
                "evidence) -- a three-work cascade demoting against an already-demoted "
                "reference (kept_invalid_reference, bake plan §4.3)"
            )
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
    asset (no `measurement_status` column) degrades gracefully to [].

    D-02a CARVE-OUT (136-06, docs/specs/discovery-sidecar-schema-v1.md SS1.6
    amendment 2026-08-02 -- discovered as a direct consequence of that
    amendment, since a stored `tier_a` `measured_pass` row now legitimately
    carries ONLY `ci_low` and no other of the five fields): for
    `evidence_source=track1_direct, confidence_band=tier_a` specifically,
    `measured_pass` means the CERT-01 AUTHORIZATION shape -- `ci_low`
    present and `>= STRICT_FLOOR`, with `precision`/`ci_high`/`numerator`/
    `denominator` ALL NULL (never a fabricated number, per the no-numbers
    posture) -- rather than the all-five-fields "genuinely measured" shape
    every other band's `measured_pass`/`measured_fail` still requires
    unchanged below. `tier_a`'s own `measured_fail`/`not_measured`/
    `insufficient_evidence` outcomes (e.g. a reband-invalidated row) are
    UNAFFECTED by this carve-out and keep the original all-five rule."""
    if not _has_column(conn, "band_precision", "measurement_status"):
        return []
    violations: List[str] = []
    for (source, cb, status, precision, ci_low, ci_high, num, den) in conn.execute(
        "SELECT evidence_source, confidence_band, measurement_status, precision, ci_low, "
        "ci_high, numerator, denominator FROM band_precision"
    ).fetchall():
        if status is None:
            continue
        if status not in _MEASUREMENT_STATUSES:
            violations.append(f"band_precision ({cb}): unknown measurement_status {status!r}")
            continue
        five = (precision, ci_low, ci_high, num, den)
        all_present = all(v is not None for v in five)
        all_null = all(v is None for v in five)
        is_tier_a_authorization = (
            status == "measured_pass"
            and source == ids.EVIDENCE_SOURCE_TRACK1_DIRECT
            and cb == ids.CONFIDENCE_BAND_TIER_A
        )
        if is_tier_a_authorization:
            if ci_low is None or ci_low < _STRICT_FLOOR:
                violations.append(
                    f"band_precision ({cb}): measured_pass authorization requires "
                    f"ci_low>={_STRICT_FLOOR} (D-02a, Codex #B3)")
            if precision is not None or ci_high is not None or num is not None or den is not None:
                violations.append(
                    f"band_precision ({cb}): measured_pass authorization must carry NULL "
                    "precision/ci_high/numerator/denominator (D-02a no-numbers posture)")
        elif status == "measured_pass":
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

def verify(db_path, expected_frame_hash=None, *, expected_band_vocabulary: Optional[str] = None) -> int:
    """Run ALL release-contract invariants over `db_path`. Returns 0 on a
    clean DB, 1 (fail-closed) on ANY violation. Prints every violation found
    to stderr.

    `expected_band_vocabulary` ('v1'/'v2', from `--require-v2` /
    `--expected-band-vocabulary`) is the external operator-intent gate: when
    set, the asset's DETECTED band-vocabulary version must equal it, closing
    the false-green where an intended-v2 bake accidentally emits a valid v1
    asset (135-07)."""
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
        violations += check_band_vocabulary(
            conn, meta, expected_band_vocabulary=expected_band_vocabulary)
        violations += check_never_orphan_shipped(conn)
        violations += check_unknown_date_never_demoted(conn)
        violations += check_routing_audit_replayability(conn)
        violations += check_demotion_kept_reference_shipped(conn)
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
    parser.add_argument("--expected-band-vocabulary", choices=["v1", "v2"], default=None,
                        help="External operator-intent gate (135-07): the asset's DETECTED "
                             "band-vocabulary version MUST equal this. Closes the false-green "
                             "where an intended-v2 bake accidentally emits a valid v1 asset.")
    parser.add_argument("--require-v2", action="store_true",
                        help="Shorthand for --expected-band-vocabulary v2 -- the production "
                             "135-07 v2-bake invocation, proving v2 INTENT not mere internal "
                             "consistency.")
    return parser


def main(argv=None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    expected_vocab = args.expected_band_vocabulary
    if args.require_v2:
        if expected_vocab not in (None, "v2"):
            parser.error("--require-v2 conflicts with --expected-band-vocabulary v1")
        expected_vocab = "v2"
    return verify(args.db_path, args.expected_frame_hash, expected_band_vocabulary=expected_vocab)


if __name__ == "__main__":
    sys.exit(main())
