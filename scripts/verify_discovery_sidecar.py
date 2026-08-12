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
# The fam-v1 family assignment -- the ONE canonical recipe (same
# never-duplicated posture as compute_frame_content_hash): the retention gate
# is only meaningful if it assigns families exactly as the lock emitter did.
import shared.discovery_family as discovery_family
import shared.discovery_relation_matrix as relation_matrix


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

    id_sweep = (
        ("works", "work_id"),
        ("works", "canonical_work_id"),
        ("discovery_claim", "work_id"),
        # CD batch / schema Amendment 2026-08-12: the locus + Contract-1 input
        # tables key on opaque ids too. The raw locus_ref_id ('M:', 'J:',
        # 'REF2:' shaped) is deliberately NOT stored in the asset -- the
        # D-track import re-keys via the crosswalk -- and this sweep is what
        # keeps that true rather than merely intended.
        ("locus_work", "work_id"),
        ("discovery_region_map", "work_id"),
        ("discovery_curated_quoter", "canonical_work_id"),
    )
    for tbl, col in id_sweep:
        if not _has_table(conn, tbl):
            continue
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
    to a `discovery_routing_audit` row on the same page **naming that evidence's
    own canonical work as the demoted one**, with `decision='demoted'` and BOTH
    years non-NULL -- an unknown-date pair is fail-safe and can NEVER produce a
    demotion.

    Keyed by (page, demoted canonical work), not by page alone. A page-only test
    lets an unrelated demotion on the same page satisfy the check for evidence
    whose own backing demotion is absent, so the gate would pass an artifact in
    exactly the state it exists to reject (Codex code review 2026-08-03, finding
    2 -- the projection carried the identical defect and is corrected in the same
    change). Audit rows carry canonical work ids, hence the join through
    `works.canonical_work_id`. A NULL `demoted_work_id` substantiates nothing and
    is excluded: fail closed.

    A pre-v2 asset (no `discovery_routing_audit` table) degrades gracefully to
    [] (compat-gate); `check_gate_bearing_tables_present` is what stops that
    compatibility from silently covering a current asset."""
    if not _has_table(conn, "discovery_routing_audit"):
        return []
    violations: List[str] = []
    cur = conn.cursor()
    demoted_page_works = {
        (page_id, demoted_work_id)
        for page_id, demoted_work_id, ky, dy in cur.execute(
            "SELECT page_id, demoted_work_id, kept_year, demoted_year "
            "FROM discovery_routing_audit WHERE decision='demoted'"
        ).fetchall()
        if ky is not None and dy is not None and demoted_work_id is not None
    }
    # `dc.page_id`, NOT `de.a_page_id`. The projection this check mirrors keys
    # its closure on the CLAIM's page (scripts/project_discovery_public.py --
    # `page_id = claim["page_id"]`), so reading the EVIDENCE's page here made the
    # two sides non-equivalent: the verifier could pass an artifact the
    # projection would prune, or vice versa, the moment those columns diverge
    # (Codex code review 2A, finding 2).
    #
    # They do not diverge today -- measured on the deployed public artifact,
    # zero of the joined rows have `de.a_page_id IS NOT dc.page_id`, and zero
    # among `later_shared_text` rows specifically. That is exactly why this was
    # latent rather than a live miss, and exactly why it is worth pinning: two
    # independently-written queries that agree only by coincidence will stop
    # agreeing without anything failing.
    rows = cur.execute(
        "SELECT de.evidence_id, dc.page_id, w.canonical_work_id "
        "FROM discovery_evidence de "
        "JOIN discovery_claim dc ON dc.claim_id = de.claim_id "
        "JOIN works w ON w.work_id = dc.work_id "
        "WHERE de.routing_reason=?", (ids.ROUTING_REASON_LATER_SHARED_TEXT,)
    ).fetchall()
    for evid, page_id, canonical_work_id in rows:
        if (page_id, canonical_work_id) not in demoted_page_works:
            violations.append(
                f"evidence {evid}: routing_reason='later_shared_text' on page {page_id} has NO "
                "discovery_routing_audit decision='demoted' row naming its own canonical work "
                "with both years non-NULL (unknown-date-never-demoted)"
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
        elif decision == "kept_tie":
            # Schema amendment (F), 2026-08-02: a kept_tie row MUST name the work
            # it beat, or the tie pair is unreconstructable from the audit table
            # alone. This branch previously required the opposite (NULL), which
            # directly contradicted `check_kept_tie_names_its_pair` in this same
            # script -- whichever way the builder wrote the column, one of the two
            # checks was guaranteed to fail. 136-12 fixed the builder and added
            # the new check but left this one on the pre-amendment rule.
            if demoted is None:
                violations.append(
                    f"routing_audit page {page_id}: kept_tie row missing demoted_work_id "
                    "(schema amendment (F): the tie pair must be reconstructable)")
        elif decision == "fail_safe_unknown_date":
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
    # Inert NULL-genre works: present in the artifact but reachable from no
    # public surface, so not a release violation (see
    # check_works_genre_vocabulary for the measurement that set that scope).
    # Reported so the curation gap does not stay invisible between rebuilds.
    #
    # The reachability predicate was MISSING here (found 2026-08-12 running this
    # verifier over the C-track bake). This counted EVERY NULL-genre work and
    # then described the total as "reachable from no public surface" -- so on
    # both real assets it printed a reassurance that was false about every work
    # it counted, immediately above the fatal violation counting the SAME rows:
    # "181 work(s) ... reachable from no public surface" one line above
    # "VIOLATION: 181 work(s) reachable from a public surface". Same 181, and
    # 58/58 on the public side.
    #
    # It is a leftover from the FIRST scoping, which `check_works_genre_vocabulary`
    # records as wrong and superseded ("58 of 58 public and 181 of 181 private are
    # reachable. Not a few more -- every single one."). That correction landed in
    # the check and never reached this report. So the message is not merely
    # imprecise: it tells an operator the gap is inert at exactly the moment the
    # release is failing because it is not.
    #
    # Subtracting the reachable population makes the sentence true again, and
    # makes it SILENT on today's assets -- which is correct, because when every
    # NULL-genre work is reachable there is no inert remainder to report and the
    # violation already says everything there is to say. The predicate is the
    # same one check_works_genre_vocabulary uses; keep them together.
    if not _has_table(conn, "discovery_identification"):
        return []
    (inert,) = conn.execute(
        """
        SELECT COUNT(DISTINCT w.work_id) FROM works w
         WHERE (w.genre IS NULL OR w.genre = '')
           AND NOT (
             EXISTS (SELECT 1 FROM discovery_identification di
                      WHERE di.canonical_work_id = w.canonical_work_id)
             OR EXISTS (SELECT 1 FROM discovery_claim dc
                         WHERE dc.work_id = w.work_id)
           )
        """
    ).fetchone()
    if inert:
        print(
            f"genre-curation report (non-fatal): {inert} work(s) carry no genre and "
            "are reachable from no public surface -- widen the curated domain "
            "population at the next rebuild",
            file=sys.stderr)
    return []


# ---------------------------------------------------------------------------
# 17-28. 136-12: one registered check per field the Phase-136 rebuild adds
# (docs/specs/discovery-sidecar-schema-v1.md SS Amendment 2026-08-02).
#
# INDEPENDENCE. Every constant below is MIRRORED as a local literal, never
# imported from `scripts/build_discovery_sidecar.py` -- the same standing
# convention `_MEASUREMENT_STATUSES` / `_STRICT_FLOOR` already follow, and the
# reason the verifier can catch a builder bug at all. The vocabularies are
# cross-checked against their contract modules by
# `tests/test_discovery_schema.py`, so a mirror that drifts is a red suite
# rather than a silent false green.
#
# MESSAGE DISCIPLINE. Every violation names the TABLE, the KEY and the FIELD --
# never the offending cell value. On the novelty/provenance/visibility columns
# that is not cosmetic: echoing the value is the exact leak the masking rules
# (D-25 / NOVEL-02) exist to prevent.
# ---------------------------------------------------------------------------

# The TEN-value novelty shade enum (owner rulings E/E'/F/G/H --
# 136-GATE1-DECISIONS.md SS E-H; canonical prose statement:
# docs/specs/discovery-novelty-v1.md SS2). Mirrors
# shared/discovery_novelty.py::NOVELTY_STATUSES.
_NOVELTY_STATUSES = frozenset({
    "confirms", "refines_granularity", "aid_more_specific", "diverges_work",
    "diverges_part", "container_predicts", "fills_gap", "extends", "alias_merge",
    "not_checked",
})
# The two shades, and ONLY these two, that may carry a divergence_correctness
# value (ruling F).
_DIVERGENCE_SHADES = frozenset({"diverges_work", "diverges_part"})
# Ruling F's own three-value correctness vocabulary. Mirrors
# shared/discovery_novelty.py::DIVERGENCE_CORRECTNESS_VALUES.
_DIVERGENCE_CORRECTNESS_VALUES = frozenset({"catalogue_correct", "claim_correct", "unclear"})
# The complete masked provenance label set (NOVEL-02 / D-25). Mirrors
# shared/discovery_novelty.py::MASKED_PROVENANCE_LABELS -- an ALLOWLIST, so a
# restricted corpus name can never be a member.
_MASKED_PROVENANCE_LABELS = frozenset({
    "recorded in the catalogue", "מתועד בקטלוג",
    "recorded in the bibliography", "מתועד בביבליוגרפיה",
    "recorded in the Princeton Geniza Project", "מתועד בפרויקט הגניזה של פרינסטון",
    "recorded in a scholarly transcription", "מתועד בתעתיק מדעי",
    "recorded in the NLI catalogue", "מתועד בקטלוג הספרייה הלאומית",
    "recorded in another reference source", "מתועד במקור עזר אחר",
})
# The closed {public, private} visibility enum (Amendment (A) / D-22). Mirrors
# shared/discovery_visibility.py::VISIBILITY_VALUES.
_VISIBILITY_VALUES = frozenset({"public", "private"})
# The closed meta.audience enum (Amendment (C1)). The PRIVATE build must write
# exactly `private`; `scripts/project_discovery_public.py` is the sole writer of
# `public`.
_AUDIENCE_VALUES = frozenset({"public", "private"})
_PRIVATE_AUDIENCE = "private"
_PUBLIC_AUDIENCE = "public"
# The closed coverage validity vocabulary (Amendment (A)).
_COVERAGE_STATUSES = frozenset({"measured", "no_denominator", "not_applicable"})
# The closed main_pool_reason vocabulary (Amendment (B)).
_MAIN_POOL_REASONS = frozenset({
    "shared_wording", "overlapping_tie", "low_coverage", "insufficient_length",
    "missing_signal", "main_multifolio", "main_full_coverage", "main_human_confirmed",
})
# The global band-rank lattice (schema SS6) is 0-INDEXED in the implementation
# it is materialized from (`shared.discovery_service._BAND_RANK_ORDER`, 8
# entries because BOTH the v1 and v2 top-tier band keys are present through the
# transition), and an unknown (evidence_source, confidence_band) pair ranks
# LAST at `len(order)`. So the stored key is bounded by [0, 8]. The sentinel 8
# is permitted here rather than rejected because an unknown pair is already
# rejected, more precisely, by `check_evidence_combinations` -- duplicating that
# rejection here would report one defect as two.
_BAND_RANK_MIN = 0
_BAND_RANK_MAX = 8
# `works.genre` is the FULL "{parent} / {leaf}" path, except for the explicit
# `Unassigned` bucket -- a REAL value, never missing data (Amendment (C)).
_GENRE_UNASSIGNED = "Unassigned"
_GENRE_PATH_SEPARATOR = " / "
# The authorized index set (Amendment (D)).
_AUTHORIZED_INDEXES = frozenset({
    "ix_discovery_evidence_coverage_ppm",
    "ix_discovery_evidence_band_rank",
    "ix_discovery_evidence_novelty_status",
    "ux_discovery_claim_display_evidence_id",
    "ix_discovery_identification_order",
    "ix_discovery_identification_canonical_work_id",
    "ix_discovery_identification_sys_id",
    "ix_manuscript_display_sort",
    # CD batch / schema Amendment 2026-08-12 (N)/(Q).
    "ix_locus_unit_part",
    "ix_stratum_membership_identification",
})
# `manuscript_display` is sourced ONLY from libraries.csv and carries NO work
# title, NO reference text and NO locus (T-136-11-03).
_MANUSCRIPT_DISPLAY_FORBIDDEN_SUBSTRINGS = ("title", "text", "locus", "span", "offset")
_SHA256_PREFIXED_RE = re.compile(r"^sha256:[0-9a-f]{64}$")


def check_coverage_persistence(conn: sqlite3.Connection) -> List[str]:
    """Amendment (A): `coverage_ppm` is DIRECT-FAMILY ONLY, and `coverage_status`
    is the separate validity axis. A propagated row carries no coverage value
    because it has no page-length denominator at all -- not because the number
    was withheld for display."""
    if not _has_column(conn, "discovery_evidence", "coverage_ppm"):
        return ["discovery_evidence.coverage_ppm: column absent (Amendment (A))"]
    violations = []
    (n_propagated_with_coverage,) = conn.execute(
        "SELECT COUNT(*) FROM discovery_evidence WHERE evidence_source = ? "
        "AND coverage_ppm IS NOT NULL", (ids.EVIDENCE_SOURCE_PROPAGATED,)
    ).fetchone()
    if n_propagated_with_coverage:
        violations.append(
            f"discovery_evidence.coverage_ppm: {n_propagated_with_coverage} propagated row(s) "
            "carry a coverage value (DIRECT FAMILY ONLY, D-08a)")
    (n_propagated_wrong_status,) = conn.execute(
        "SELECT COUNT(*) FROM discovery_evidence WHERE evidence_source = ? "
        "AND coverage_status IS NOT 'not_applicable'", (ids.EVIDENCE_SOURCE_PROPAGATED,)
    ).fetchone()
    if n_propagated_wrong_status:
        violations.append(
            f"discovery_evidence.coverage_status: {n_propagated_wrong_status} propagated row(s) "
            "are not 'not_applicable'")
    (n_direct_no_status,) = conn.execute(
        "SELECT COUNT(*) FROM discovery_evidence WHERE evidence_source = ? "
        "AND coverage_status IS NULL", (ids.EVIDENCE_SOURCE_TRACK1_DIRECT,)
    ).fetchone()
    if n_direct_no_status:
        violations.append(
            f"discovery_evidence.coverage_status: {n_direct_no_status} direct row(s) carry no "
            "validity status (an absent coverage_ppm must never be readable as zero coverage)")
    bad = [
        r[0] for r in conn.execute(
            "SELECT DISTINCT coverage_status FROM discovery_evidence "
            "WHERE coverage_status IS NOT NULL")
        if r[0] not in _COVERAGE_STATUSES
    ]
    if bad:
        violations.append(
            f"discovery_evidence.coverage_status: {len(bad)} value(s) outside the closed "
            "vocabulary")
    return violations


def check_band_rank_materialized(conn: sqlite3.Connection) -> List[str]:
    """Amendment (A) / D-10a: `band_rank` is materialized on every evidence row
    and lies inside the known lattice range."""
    if not _has_column(conn, "discovery_evidence", "band_rank"):
        return ["discovery_evidence.band_rank: column absent (Amendment (A))"]
    violations = []
    (n_null,) = conn.execute(
        "SELECT COUNT(*) FROM discovery_evidence WHERE band_rank IS NULL").fetchone()
    if n_null:
        violations.append(
            f"discovery_evidence.band_rank: {n_null} row(s) carry no materialized sort key")
    (n_out_of_range,) = conn.execute(
        "SELECT COUNT(*) FROM discovery_evidence WHERE band_rank IS NOT NULL "
        "AND (band_rank < ? OR band_rank > ?)", (_BAND_RANK_MIN, _BAND_RANK_MAX)
    ).fetchone()
    if n_out_of_range:
        violations.append(
            f"discovery_evidence.band_rank: {n_out_of_range} row(s) outside the known lattice "
            f"range [{_BAND_RANK_MIN}, {_BAND_RANK_MAX}]")
    return violations


def check_identification_grain(conn: sqlite3.Connection) -> List[str]:
    """Amendment (B): ONE row per `(sys_id, canonical_work_id)`, every
    `main_pool_reason` in the closed vocabulary, and the `works` identity join
    exactly 1:1 on `display_work_id` (SS(B1) -- a fan-out here silently inflates
    every downstream count)."""
    if not _has_table(conn, "discovery_identification"):
        return ["discovery_identification: table absent (Amendment (B))"]
    violations = []
    (n_rows,) = conn.execute("SELECT COUNT(*) FROM discovery_identification").fetchone()
    (n_pairs,) = conn.execute(
        """
        SELECT COUNT(*) FROM (
            SELECT DISTINCT de.sys_id, w.canonical_work_id
            FROM discovery_evidence de
            JOIN discovery_claim dc ON dc.claim_id = de.claim_id
            JOIN works w            ON w.work_id  = dc.work_id
            WHERE de.routing_status = ? OR de.adjudication_status = ?
        )
        """,
        (ids.ROUTING_STATUS_SHIPPED, ids.ADJUDICATION_STATUS_HUMAN_CONFIRMED),
    ).fetchone()
    if n_rows != n_pairs:
        violations.append(
            f"discovery_identification: row count {n_rows} != distinct "
            f"(sys_id, canonical_work_id) pair count {n_pairs}")
    (n_joined,) = conn.execute(
        "SELECT COUNT(*) FROM discovery_identification di "
        "JOIN works w ON w.work_id = di.display_work_id").fetchone()
    if n_joined != n_rows:
        violations.append(
            f"discovery_identification.display_work_id: the works identity join produced "
            f"{n_joined} rows for {n_rows} identifications (must be exactly 1:1, SS(B1))")
    bad = [
        r[0] for r in conn.execute(
            "SELECT DISTINCT main_pool_reason FROM discovery_identification")
        if r[0] not in _MAIN_POOL_REASONS
    ]
    if bad:
        violations.append(
            f"discovery_identification.main_pool_reason: {len(bad)} value(s) outside the closed "
            "vocabulary")
    return violations


def check_manuscript_display_carries_no_reference_content(conn: sqlite3.Connection) -> List[str]:
    """T-136-11-03: `manuscript_display` is sourced ONLY from libraries.csv and
    carries NO work title, NO reference text and NO locus. Checked on the
    SCHEMA, so an empty table cannot pass by having no rows to inspect."""
    if not _has_table(conn, "manuscript_display"):
        return ["manuscript_display: table absent (Amendment (B))"]
    violations = []
    for row in conn.execute("PRAGMA table_info(manuscript_display)"):
        col = row[1].lower()
        for forbidden in _MANUSCRIPT_DISPLAY_FORBIDDEN_SUBSTRINGS:
            if forbidden in col and "shelfmark" not in col:
                violations.append(
                    f"manuscript_display.{row[1]}: reference-content-shaped column "
                    f"(forbidden token {forbidden!r})")
    return violations


def check_novelty_status_vocabulary(conn: sqlite3.Connection) -> List[str]:
    """Amendment (A), owner rulings E/E'/F/G/H: `novelty_status` is a CLOSED
    TEN-VALUE vocabulary, present on BOTH evidence families, and every evidence
    row of one claim agrees (D-23a).

    Fails closed on ANY value outside the ten -- including every retired
    vocabulary (the three-value tri-state, the eight-value E' set with its
    unsplit `diverges`, and the nine-value pre-ruling-H set)."""
    if not _has_column(conn, "discovery_evidence", "novelty_status"):
        return ["discovery_evidence.novelty_status: column absent (Amendment (A))"]
    violations = []
    bad = [
        r[0] for r in conn.execute("SELECT DISTINCT novelty_status FROM discovery_evidence")
        if r[0] not in _NOVELTY_STATUSES
    ]
    if bad:
        violations.append(
            f"discovery_evidence.novelty_status: {len(bad)} value(s) outside the closed "
            "TEN-value shade vocabulary")
    (n_null,) = conn.execute(
        "SELECT COUNT(*) FROM discovery_evidence WHERE novelty_status IS NULL").fetchone()
    if n_null:
        violations.append(
            f"discovery_evidence.novelty_status: {n_null} row(s) carry NULL "
            "(the fail-closed default is 'not_checked', never absent)")
    # Computed for ALL families -- the coverage gap this rebuild closes.
    for family in (ids.EVIDENCE_SOURCE_TRACK1_DIRECT, ids.EVIDENCE_SOURCE_PROPAGATED):
        (n_family,) = conn.execute(
            "SELECT COUNT(*) FROM discovery_evidence WHERE evidence_source = ?",
            (family,)).fetchone()
        if not n_family:
            continue
        (n_bad_family,) = conn.execute(
            "SELECT COUNT(*) FROM discovery_evidence WHERE evidence_source = ? "
            "AND novelty_status IS NULL", (family,)).fetchone()
        if n_bad_family:
            violations.append(
                f"discovery_evidence.novelty_status: {n_bad_family} {family} row(s) carry NULL")
    # D-23a: one result per claim.
    (n_disagreeing,) = conn.execute(
        """
        SELECT COUNT(*) FROM (
            SELECT claim_id FROM discovery_evidence
            GROUP BY claim_id
            HAVING COUNT(DISTINCT novelty_status) > 1
                OR COUNT(DISTINCT COALESCE(novelty_source_label, '')) > 1
                OR COUNT(DISTINCT COALESCE(divergence_correctness, '')) > 1
        )
        """
    ).fetchone()
    if n_disagreeing:
        violations.append(
            f"discovery_evidence.novelty_status: {n_disagreeing} claim(s) carry evidence rows "
            "that disagree about their novelty result (D-23a)")
    if _has_column(conn, "discovery_identification", "novelty_status"):
        bad_ident = [
            r[0] for r in conn.execute(
                "SELECT DISTINCT novelty_status FROM discovery_identification")
            if r[0] not in _NOVELTY_STATUSES
        ]
        if bad_ident:
            violations.append(
                f"discovery_identification.novelty_status: {len(bad_ident)} value(s) outside "
                "the closed TEN-value shade vocabulary")
    return violations


def check_novelty_source_label_masked(conn: sqlite3.Connection) -> List[str]:
    """NOVEL-02 / D-25: every stored `novelty_source_label` is a member of the
    masked label set. The raw provenance value -- which finding aid, which
    restricted corpus -- is NEVER stored, and this check never echoes a value
    (doing so would perform the leak it exists to detect)."""
    if not _has_column(conn, "discovery_evidence", "novelty_source_label"):
        return ["discovery_evidence.novelty_source_label: column absent (Amendment (A))"]
    violations = []
    bad = [
        r[0] for r in conn.execute(
            "SELECT DISTINCT novelty_source_label FROM discovery_evidence "
            "WHERE novelty_source_label IS NOT NULL")
        if r[0] not in _MASKED_PROVENANCE_LABELS
    ]
    if bad:
        violations.append(
            f"discovery_evidence.novelty_source_label: {len(bad)} value(s) outside the masked "
            "label set (values withheld -- naming them would be the leak)")
    # NULL on the two ineligible shades: `fills_gap` has nothing to name and
    # `not_checked` checked nothing.
    (n_ineligible,) = conn.execute(
        "SELECT COUNT(*) FROM discovery_evidence WHERE novelty_source_label IS NOT NULL "
        "AND novelty_status IN ('fills_gap', 'not_checked')").fetchone()
    if n_ineligible:
        violations.append(
            f"discovery_evidence.novelty_source_label: {n_ineligible} row(s) name a source on a "
            "shade where no aid says anything (fills_gap / not_checked)")
    return violations


def check_divergence_correctness_applicability(conn: sqlite3.Connection) -> List[str]:
    """Owner ruling F: `divergence_correctness` is populated ONLY on
    `diverges_work`/`diverges_part` rows and is drawn from its own closed
    three-value vocabulary when non-NULL.

    ⟨AMENDED 2026-08-03, owner ruling L⟩ The converse -- "non-NULL on every
    divergence row" -- is deliberately NOT checked, and must not be added. The
    model no longer produces this field at all
    (`resolve_model_output` always returns `None` for it) and no human/owner
    annotation pathway exists yet, so NULL is the ONLY value a build can
    currently write on a divergence row. Requiring non-NULL here would make
    every `diverges_work`/`diverges_part` row unshippable."""
    if not _has_column(conn, "discovery_evidence", "divergence_correctness"):
        return ["discovery_evidence.divergence_correctness: column absent (Amendment (A))"]
    violations = []
    for table in ("discovery_evidence", "discovery_identification"):
        if not _has_column(conn, table, "divergence_correctness"):
            continue
        (n_misapplied,) = conn.execute(
            f"SELECT COUNT(*) FROM {table} WHERE divergence_correctness IS NOT NULL "  # noqa: S608 -- fixed table names
            "AND novelty_status NOT IN ('diverges_work', 'diverges_part')"
        ).fetchone()
        if n_misapplied:
            violations.append(
                f"{table}.divergence_correctness: {n_misapplied} row(s) carry a correctness call "
                "on a non-divergence shade (ruling F: valid ONLY on diverges_work/diverges_part)")
        bad = [
            r[0] for r in conn.execute(
                f"SELECT DISTINCT divergence_correctness FROM {table} "  # noqa: S608 -- fixed table names
                "WHERE divergence_correctness IS NOT NULL")
            if r[0] not in _DIVERGENCE_CORRECTNESS_VALUES
        ]
        if bad:
            violations.append(
                f"{table}.divergence_correctness: {len(bad)} value(s) outside the closed "
                "three-value correctness vocabulary")
    return violations


def check_visibility_axes(conn: sqlite3.Connection) -> List[str]:
    """Amendment (A) / D-22: BOTH axes are stored, non-NULL, and drawn from the
    closed `{public, private}` enum. `assertion_visibility` lives on
    `discovery_evidence`, `identity_visibility` on `works` -- neither axis is a
    proxy for the other, so neither may substitute for the other."""
    violations = []
    for table, column in (
        ("discovery_evidence", "assertion_visibility"),
        ("works", "identity_visibility"),
        ("discovery_identification", "assertion_visibility"),
        ("discovery_identification", "identity_visibility"),
    ):
        if not _has_column(conn, table, column):
            violations.append(f"{table}.{column}: column absent (Amendment (A)/(B))")
            continue
        (n_null,) = conn.execute(
            f"SELECT COUNT(*) FROM {table} WHERE {column} IS NULL").fetchone()  # noqa: S608 -- fixed names
        if n_null:
            violations.append(
                f"{table}.{column}: {n_null} row(s) carry NULL (public eligibility requires "
                "BOTH axes -- an underived row must read 'private', never nothing)")
        bad = [
            r[0] for r in conn.execute(
                f"SELECT DISTINCT {column} FROM {table} WHERE {column} IS NOT NULL")  # noqa: S608
            if r[0] not in _VISIBILITY_VALUES
        ]
        if bad:
            violations.append(
                f"{table}.{column}: {len(bad)} value(s) outside the closed "
                "{public, private} enum")
    return violations


def check_kept_tie_names_its_pair(conn: sqlite3.Connection) -> List[str]:
    """Amendment (F): every `discovery_routing_audit` row with
    `decision='kept_tie'` carries a non-NULL `demoted_work_id`. A NULL there
    makes the tie pair unreconstructable from the audit alone, and the
    main-pool rule's competition gate reads exactly those ties."""
    if not _has_table(conn, "discovery_routing_audit"):
        return []
    (n_bad,) = conn.execute(
        "SELECT COUNT(*) FROM discovery_routing_audit "
        "WHERE decision = 'kept_tie' AND demoted_work_id IS NULL").fetchone()
    if n_bad:
        return [
            f"discovery_routing_audit.demoted_work_id: {n_bad} kept_tie row(s) carry NULL "
            "(the tie pair is unreconstructable from the audit alone)"
        ]
    return []


def check_works_genre_vocabulary(conn: sqlite3.Connection, meta: dict) -> List[str]:
    """Amendment (C): `works.genre` is populated from the curated, hash-pinned
    artifact and constrained to the closed FJMS vocabulary or the explicit
    `Unassigned` value -- never silently NULL-as-absent.

    **What an INDEPENDENT verifier can enforce, stated honestly.** The 202-leaf
    FJMS vocabulary lives in a gitignored sidecar this verifier must not
    require, and the curated artifact is gitignored too. The full vocabulary is
    enforced at BUILD time against the hash-pinned artifact, which
    `curate_work_domains.py --validate` had already validated against the live
    tree. Independently checkable here, and checked: the `Unassigned` sentinel
    or a well-formed `"{parent} / {leaf}"` path; canonical-grain agreement (two
    `works` rows sharing a canonical id must agree, since assignment is at that
    grain); and the PROVENANCE PIN -- a populated genre column must name the
    pinned artifact that produced it."""
    violations = []
    values = [
        r[0] for r in conn.execute(
            "SELECT DISTINCT genre FROM works WHERE genre IS NOT NULL AND genre != ''")
    ]
    if not values:
        return violations  # an unpopulated column is the pre-rebuild state, not a violation

    # NULL-as-absent, on a column that IS populated (Codex code review
    # 2026-08-03, finding 3). The docstring above promises "never silently
    # NULL-as-absent", but every query in this check filtered NULLs out, so a
    # partially-populated column passed silently. The deployed public artifact
    # carries 58 such rows and the private source 181.
    #
    # Scoped to REACHABLE works. The first version of this scoping was WRONG and
    # is worth recording, because the error was in the measurement, not the idea
    # (Codex code review 2A, finding 3).
    #
    # It defined reachable as "has an identification, or shipped, or
    # human-confirmed evidence" -- the DEFAULT population -- measured zero of the
    # 58 public / 181 private NULL-genre works reachable, and concluded they were
    # inert. But `get_claims_for_page` takes `include_review=True`, which drops
    # the routing clause ENTIRELY (`shared/discovery_service.py`:
    # `routing_clause = "" if include_review else ...`) and returns review-only
    # and unreviewed claims, `works.genre` among their columns. Re-measured on
    # that surface: **58 of 58 public and 181 of 181 private are reachable.** Not
    # a few more -- every single one.
    #
    # So reachability here is "referenced by ANY surviving claim", which is the
    # population the opt-in panel can actually return. Do not narrow it back to
    # the shipped/confirmed set without re-checking `include_review`; that is the
    # exact mistake this comment exists to stop being repeated.
    #
    # NOTE FOR WHOEVER HITS THIS: on the artifacts as built today this check
    # FAILS, because the builder writes `works.genre` only for works matched in
    # the curated 136-09 artifact and leaves the rest NULL. That is a real gap
    # against the frozen contract ("an explicit Unassigned bucket, never
    # NULL-as-absent"), not a false alarm. Closing it is a decision, not a
    # one-liner: backfilling `Unassigned` is honest but `Unassigned` carries
    # display semantics (ruling Q declined it for one work precisely because it
    # would hide the row), so the choice between curating the remainder and
    # accepting the bucket belongs to the owner. Tracked in docs/OPEN_ISSUES.md.
    if not _has_table(conn, "discovery_identification"):
        # check_gate_bearing_tables_present makes this unreachable on a current
        # asset; kept so this check degrades rather than raising on a legacy one.
        return violations
    # ANY surviving claim -- not just shipped/human-confirmed. The review opt-in
    # read drops the routing predicate, so every work referenced by a claim is
    # selectable, and `works.genre` travels with it.
    unassigned_reachable = conn.execute(
        """
        SELECT COUNT(DISTINCT w.work_id) FROM works w
         WHERE (w.genre IS NULL OR w.genre = '')
           AND (
             EXISTS (SELECT 1 FROM discovery_identification di
                      WHERE di.canonical_work_id = w.canonical_work_id)
             OR EXISTS (SELECT 1 FROM discovery_claim dc
                         WHERE dc.work_id = w.work_id)
           )
        """
    ).fetchone()[0]
    if unassigned_reachable:
        violations.append(
            f"works.genre: {unassigned_reachable} work(s) reachable from a public "
            f"surface have a NULL/empty genre on a populated column -- the contract "
            f"is an explicit {_GENRE_UNASSIGNED!r} bucket, never NULL-as-absent. "
            f"Reachability here includes the REVIEW OPT-IN read (include_review=True "
            f"drops the routing predicate), which is the population the earlier "
            f"shipped-or-confirmed scoping missed entirely"
        )
    malformed = 0
    for value in values:
        if value == _GENRE_UNASSIGNED:
            continue
        parts = value.split(_GENRE_PATH_SEPARATOR)
        if len(parts) != 2 or not parts[0].strip() or not parts[1].strip():
            malformed += 1
    if malformed:
        violations.append(
            f"works.genre: {malformed} distinct value(s) are neither the explicit "
            f"{_GENRE_UNASSIGNED!r} bucket nor a well-formed '{{parent}} / {{leaf}}' path")
    (n_disagreeing,) = conn.execute(
        """
        SELECT COUNT(*) FROM (
            SELECT canonical_work_id FROM works
            GROUP BY canonical_work_id
            HAVING COUNT(DISTINCT COALESCE(genre, '')) > 1
        )
        """
    ).fetchone()
    if n_disagreeing:
        violations.append(
            f"works.genre: {n_disagreeing} canonical group(s) disagree about their genre "
            "(assignment is at the CANONICAL grain, so a duplicate is never assigned twice)")
    pin = meta.get("work_domains_content_hash")
    if not pin:
        violations.append(
            "meta.work_domains_content_hash: absent while works.genre is populated -- a "
            "populated genre column must name the pinned artifact that produced it")
    elif not _SHA256_PREFIXED_RE.match(str(pin)):
        violations.append(
            "meta.work_domains_content_hash: not a 'sha256:<64 hex>' content-hash pin")
    return violations


def check_meta_audience(
    conn: sqlite3.Connection, meta: dict, expected_audience: str = _PRIVATE_AUDIENCE
) -> List[str]:
    """Amendment (C1): an artifact declares its own audience, drawn from the
    closed enum, and it must be the audience it was verified AS.

    This is the field the runtime loader gates on so a public route can never
    resolve a private artifact by accident. Without it the exclusion is
    procedural (a code-review discipline) rather than structural (a fact the
    loader itself can check).

    2026-08-03 (136-13): `expected_audience` added. This verifier hard-coded
    `'private'`, so running it over the PUBLIC projection -- which plan 136-13
    requires as a gate -- reported a guaranteed false violation, and the only way
    to read the result was to eyeball which failures "did not apply". A gate
    whose output needs manual triage is not a gate."""
    audience = meta.get("audience")
    if audience is None:
        return ["meta.audience: absent -- the artifact must declare its own audience"]
    if audience not in _AUDIENCE_VALUES:
        return ["meta.audience: value outside the closed {public, private} enum"]
    if audience != expected_audience:
        return [
            f"meta.audience: this artifact declares {audience!r} but was verified as "
            f"{expected_audience!r} -- pass --audience to verify a public projection "
            "(only scripts/project_discovery_public.py may write 'public')"
        ]
    return []


# Tables whose ABSENCE silently converts a registered gate into a no-op.
#
# Each check that reads one of these is compat-gated (`if not _has_table(...):
# return []`) so a pre-v2 asset still verifies. That compatibility is only safe
# while SOMETHING insists the table exists on a current asset. It did not:
# removing `discovery_routing_audit` from a current artifact turned FIVE gates
# green simultaneously -- replayability, kept-tie pairing, demotion-reference
# shipping, unknown-date demotion, and the coverage report -- with no violation
# anywhere (Codex code review 2026-08-03, finding 4).
#
# `discovery_identification` and `manuscript_display` are compat-gated the same
# way and carry the same hazard.
_GATE_BEARING_TABLES = frozenset({
    "discovery_routing_audit",
    "discovery_identification",
    "manuscript_display",
    # CD batch / schema Amendment 2026-08-12: same hazard class. An absent
    # locus_unit silently no-ops the Contract-0 basis gate; an absent
    # region map / curated list makes matrix steps 3/4 read as "never fires"
    # with no violation anywhere; absent Contract-4 tables no-op the
    # withholding gates. The batch builder creates all seven unconditionally,
    # so absence on a current asset is a build defect, never a profile.
    "locus_work",
    "locus_unit",
    "locus_edition",
    "discovery_region_map",
    "discovery_curated_quoter",
    "discovery_stratum_membership",
    "discovery_withholding",
})


def check_gate_bearing_tables_present(conn: sqlite3.Connection) -> List[str]:
    """Every gate-bearing table MUST be present. Unconditionally.

    There is deliberately no legacy profile here. `meta.schema_version` cannot
    discriminate -- it reads `discovery-v1` on both pre-v2 and current assets,
    because the v2 changes shipped as amendments inside that version. The signal
    that DOES discriminate is `meta.audience`, which `check_meta_audience`
    already requires of every artifact without a compat gate. So any asset that
    reaches this point is a current one, and a "legacy profile" would be
    fiction: it would never be entered, while reading as though absence were
    sometimes tolerated.

    The per-check `_has_table` early returns are left in place. They are now
    unreachable-in-practice belt-and-braces rather than the thing standing
    between a missing table and a green run."""
    missing = sorted(t for t in _GATE_BEARING_TABLES if not _has_table(conn, t))
    if missing:
        return [
            f"sqlite_master: gate-bearing table(s) absent from a current-schema "
            f"artifact: {missing} -- each one silently no-ops at least one "
            f"registered check, so their absence must be a violation rather than "
            f"a quiet pass"
        ]
    return []


def check_authorized_index_set(conn: sqlite3.Connection) -> List[str]:
    """Amendment (D): the authorized index set is PRESENT. These are not
    performance hints -- `ux_discovery_claim_display_evidence_id` is a real
    uniqueness invariant, and the findings page's default sort depends on the
    composite ordering index."""
    present = {
        r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index' AND name IS NOT NULL")
    }
    missing = sorted(_AUTHORIZED_INDEXES - present)
    if missing:
        return [f"sqlite_master: {len(missing)} authorized index/indexes absent: {missing}"]
    return []


# ---------------------------------------------------------------------------
# 8. Membership frame_content_hash
# ---------------------------------------------------------------------------

def check_frame_content_hash(
    conn: sqlite3.Connection, meta: dict, expected_frame_hash,
    audience: str = _PRIVATE_AUDIENCE,
) -> List[str]:
    """On a PRIVATE artifact the frame hash recomputes to the value it stores.

    On a PUBLIC projection it CANNOT: the frame hash is membership-based, and
    the projection deliberately removes rows, so recomputing over the public
    membership necessarily yields a different value. What the public artifact's
    `meta.frame_content_hash` records is the frame it was PROJECTED FROM, and
    that is the invariant worth checking -- it pins the projection to a specific
    private bake. Recomputing and demanding equality would be checking that the
    projection did nothing (2026-08-03, 136-13)."""
    violations = []
    meta_hash = meta.get("frame_content_hash")
    if audience == _PUBLIC_AUDIENCE:
        if expected_frame_hash is not None and meta_hash != expected_frame_hash:
            violations.append(
                f"frame_content_hash: public artifact records source frame {meta_hash} "
                f"!= --expected-frame-hash {expected_frame_hash} -- it was projected "
                "from a different private bake than the one being verified"
            )
        return violations
    recomputed = sidecar_build.compute_frame_content_hash(conn)
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
# 9. CD batch (schema Amendment 2026-08-12) -- the amendment contract + the
#    Contract-0 coordinate-basis pin
# ---------------------------------------------------------------------------

_AMENDMENT_2026_08_12_IDENTIFICATION_COLUMNS = ("routing_reason", "rendered_relation")

# The post-batch marker key. Mirrored as a literal for the same independence
# reason as the count tables below.
_LOCUS_SCHEMA_MARKER_KEY = "locus_schema_version"

# How many mismatching rows a Contract-1 failure names before summarizing. A
# systematic matrix error mismatches EVERY row, and 55,377 ids in one violation
# string is a message nobody reads.
_RELATION_MISMATCH_REPORT_LIMIT = 5

# MIRRORED from the builder's AMENDMENT_2026_08_12_COUNT_TABLES, deliberately
# NOT imported (the verifier's standing independence convention -- a builder
# bug must be visible to the verifier that exists to catch it; drift between
# the two literals is guarded by test).
_AMENDMENT_2026_08_12_COUNT_TABLES = (
    "locus_work",
    "locus_unit",
    "locus_edition",
    "discovery_region_map",
    "discovery_curated_quoter",
    "discovery_stratum_membership",
    "discovery_withholding",
)


def check_amendment_2026_08_12_contract(conn: sqlite3.Connection, meta: dict) -> List[str]:
    """The CD batch's own presence-and-vocabulary contract: the unconditional
    `locus_schema_version` marker, the two `discovery_identification` columns
    with their closed vocabularies, and one release-contract count per new
    table (zero is a legitimate count -- the batch creates them empty; the
    count keys keep a half-imported asset from passing as complete).

    The row-for-row rendered_relation RECOMPUTE-equality gate is a separate
    check (`check_relation_matrix_recompute`, C-track); this one pins vocabulary
    and presence, which is what still has to hold on an asset whose recompute
    cannot run at all."""
    violations: List[str] = []
    if _LOCUS_SCHEMA_MARKER_KEY not in meta:
        violations.append(
            "meta.locus_schema_version absent -- a current asset must carry the "
            "Amendment 2026-08-12 marker (the builder writes it unconditionally)"
        )
    for column in _AMENDMENT_2026_08_12_IDENTIFICATION_COLUMNS:
        if not _has_column(conn, "discovery_identification", column):
            violations.append(
                f"discovery_identification.{column}: column absent (Amendment 2026-08-12)"
            )
    if not violations:
        stored_relations = {
            r[0] for r in conn.execute(
                "SELECT DISTINCT rendered_relation FROM discovery_identification")
        }
        bad_relations = stored_relations - ids.RENDERED_RELATIONS
        if bad_relations:
            violations.append(
                f"discovery_identification.rendered_relation: {sorted(bad_relations)} "
                "outside the frozen five-state vocabulary"
            )
        stored_reasons = {
            r[0] for r in conn.execute(
                "SELECT DISTINCT routing_reason FROM discovery_identification")
        }
        bad_reasons = stored_reasons - ids.ROUTING_REASONS
        if bad_reasons:
            violations.append(
                f"discovery_identification.routing_reason: {sorted(bad_reasons)} "
                "outside the frozen routing_reason vocabulary"
            )
    for table in _AMENDMENT_2026_08_12_COUNT_TABLES:
        key = f"expected_rows_{table}"
        if not _has_table(conn, table):
            continue  # check_gate_bearing_tables_present already reports absence
        (actual,) = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
        expected = meta.get(key)
        try:
            expected_int = int(expected)  # type: ignore[arg-type]
        except (TypeError, ValueError):
            violations.append(f"meta.{key} missing or non-integer (actual count {actual})")
            continue
        if expected_int != actual:
            violations.append(f"meta.{key}={expected_int} != actual {table} count {actual}")
    return violations


def check_locus_reference_basis(conn: sqlite3.Connection, meta: dict) -> List[str]:
    """Contract 0 (Amendment 2026-08-12 (T)): the bake's reference-corpus hash
    and the locus build's must be THE SAME STREAM, asserted -- not assumed.

    - Both pins present -> they must be EQUAL (even with an empty locus table:
      an asset carrying contradictory pins is wrong somewhere).
    - locus_unit populated -> BOTH pins are REQUIRED. A populated address
      table with an unpinned basis is exactly the silent-refresh hazard the
      preflight measured (214,132 offsets agreeing empirically, nothing
      contractually).
    - locus_unit empty and a pin missing -> nothing asserted yet, no
      violation (the pre-D-track batch state)."""
    violations: List[str] = []
    bake_pin = meta.get("reference_corpus_sha256")
    locus_pin = meta.get("locus_reference_corpus_sha256")
    if bake_pin is not None and locus_pin is not None and bake_pin != locus_pin:
        violations.append(
            "Contract 0: meta.reference_corpus_sha256 != meta.locus_reference_corpus_sha256 "
            "-- the evidence offsets and the locus units index DIFFERENT reference streams; "
            "every citation resolved across them is unsound"
        )
    if _has_table(conn, "locus_unit"):
        (n_units,) = conn.execute("SELECT COUNT(*) FROM locus_unit").fetchone()
        if n_units:
            if bake_pin is None:
                violations.append(
                    "Contract 0: locus_unit is populated but meta.reference_corpus_sha256 "
                    "is absent -- the bake never pinned the stream its w_start/w_end index"
                )
            if locus_pin is None:
                violations.append(
                    "Contract 0: locus_unit is populated but "
                    "meta.locus_reference_corpus_sha256 is absent -- the import never "
                    "carried the locus build's own stream pin"
                )
    return violations


def check_relation_matrix_recompute(conn: sqlite3.Connection, meta: dict) -> List[str]:
    """Contract 1's asset-relativity gate: every stored `rendered_relation` must
    EQUAL what the frozen matrix renders from that row's own inputs, recomputed
    here, row for row.

    Why a recompute and not a spot check: `rendered_relation` is the only column
    on the identification grain that is derived rather than observed, and step 4
    is a work-level aggregate, so a projector that copied the column instead of
    recomputing it would ship values that are *correct for a different row
    population* -- true of the private asset, false of the public one, and
    invisible to any per-row sanity rule.

    The parameterization is read from the ASSET'S OWN meta, never assumed: a
    gate that recomputed under deploy-1 defaults would silently pass an asset
    built with a threshold set. A missing parameterization is only tolerated on
    a pre-batch asset (no `locus_schema_version` marker); once the marker is
    there, absence of the keys is itself a violation.
    """
    violations: List[str] = []
    if not _has_table(conn, "discovery_identification"):
        # A missing grain table is reported by the gate-bearing-table check,
        # which exists for exactly that. Raising here instead would replace a
        # precise violation with a traceback.
        return violations
    marker = meta.get(_LOCUS_SCHEMA_MARKER_KEY)
    missing = [k for k in relation_matrix.PARAMETERIZATION_META_KEYS if k not in meta]
    if missing:
        if marker is not None:
            violations.append(
                "Contract 1: asset carries the locus-schema marker but is missing "
                "relation-matrix parameterization meta keys ("
                + ", ".join(sorted(missing))
                + ") -- the stored rendered_relation values cannot be re-derived, "
                "so nothing can vouch for them"
            )
        return violations

    try:
        parameterization = relation_matrix.parameterization_from_meta(meta)
    except relation_matrix.RelationMatrixError as exc:
        violations.append(f"Contract 1: unusable matrix parameterization in meta -- {exc}")
        return violations

    try:
        mismatches = relation_matrix.stored_relation_mismatches(
            conn, parameterization, limit=_RELATION_MISMATCH_REPORT_LIMIT + 1
        )
    except relation_matrix.RelationMatrixError as exc:
        # Includes RegionInputUnavailable: step 3 active with no footprint
        # recipe wired. Fails the build rather than recomputing region-blind.
        violations.append(f"Contract 1: cannot recompute rendered_relation -- {exc}")
        return violations

    if mismatches:
        shown = mismatches[:_RELATION_MISMATCH_REPORT_LIMIT]
        detail = "; ".join(
            f"{iid}: stored {stored!r} != matrix {recomputed!r}"
            for iid, stored, recomputed in shown
        )
        more = (
            f" (+ at least {len(mismatches) - len(shown)} more)"
            if len(mismatches) > len(shown) else ""
        )
        violations.append(
            f"Contract 1: {len(shown)}{'+' if more else ''} rows store a "
            f"rendered_relation the matrix does not produce -- {detail}{more}"
        )
    return violations


def check_population_lock_retention(conn: sqlite3.Connection, meta: dict) -> List[str]:
    """Contract 2's bounded-withholding rule, executable (Amendment 2026-08-12
    (S)): the CURRENT pre-withholding population, per fam-v1 family, must
    retain the lock's floors against the LOCKED constants carried in meta.
    A breach BLOCKS the build -- a materially failing population is a plan
    failure, never a denominator adjustment.

    Conditional on the lock keys being present (a pre-lock asset asserts
    nothing). On a PRIVATE asset the recomputed population is a superset of
    the public one, so the >= floors pass trivially there; the gate has its
    teeth on the public projection, which is the population the lock governs.

    Withholding refinement stated for honesty: rows withheld by Contract 4
    still COUNT here (withholding is display-layer and must never satisfy or
    evade a retention floor by deletion -- rows are never deleted); the
    per-cell withholding-vs-retention arithmetic lands with the frame gates."""
    if "population_lock_version" not in meta:
        return []
    violations: List[str] = []
    if meta.get("population_lock_family_version") != discovery_family.FAMILY_VERSION:
        return [
            "population lock: meta.population_lock_family_version does not match "
            f"this verifier's {discovery_family.FAMILY_VERSION!r} -- a retention "
            "recomputation under a different family rule is meaningless (value withheld)"
        ]
    try:
        daf_overrides = set(json.loads(meta.get("population_lock_daf_overrides", "[]")))
        floor_overall = float(meta["population_lock_retention_floor_overall"])
        floor_family = float(meta["population_lock_retention_floor_per_family"])
        locked_total = int(meta["population_lock_total"])
        locked = {
            family: int(meta[f"population_lock_family_{family}"])
            for family in discovery_family.FAMILIES
        }
    except (KeyError, TypeError, ValueError):
        return [
            "population lock: lock meta keys incomplete or non-numeric -- an asset "
            "carrying population_lock_version must carry the full constant set"
        ]

    works = {
        w: (g, c) for w, g, c in conn.execute(
            "SELECT work_id, genre, source_corpus FROM works")
    }
    current = {family: 0 for family in discovery_family.FAMILIES}
    for display_work_id, canonical_work_id in conn.execute(
        "SELECT display_work_id, canonical_work_id FROM discovery_identification "
        "WHERE main_pool = 1"
    ):
        genre, corpus = works.get(display_work_id, (None, None))
        current[discovery_family.assign_family(
            genre, corpus, canonical_work_id, daf_overrides)] += 1

    current_total = sum(current.values())
    if current_total < floor_overall * locked_total:
        violations.append(
            f"population lock: overall retention breached -- current main-pool total "
            f"{current_total} < {floor_overall:.0%} of locked {locked_total} "
            "(blocks deploy 2; remediate the population, never the denominator)"
        )
    for family in discovery_family.FAMILIES:
        if locked[family] and current[family] < floor_family * locked[family]:
            violations.append(
                f"population lock: family {family!r} retention breached -- current "
                f"{current[family]} < {floor_family:.0%} of locked {locked[family]}"
            )
    return violations


# ---------------------------------------------------------------------------
# verify() -- the single all-invariant entry point
# ---------------------------------------------------------------------------

def verify(db_path, expected_frame_hash=None, *, expected_band_vocabulary: Optional[str] = None,
           audience: str = _PRIVATE_AUDIENCE) -> int:
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
        violations += check_frame_content_hash(conn, meta, expected_frame_hash, audience)
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
        # 136-12: one registered check per field the Phase-136 rebuild adds
        # (docs/specs/discovery-sidecar-schema-v1.md SS Amendment 2026-08-02),
        # in the amendment's own subsection order: (A) evidence/works additions,
        # (B) the new tables, (C) works.genre, (C1) meta.audience, (D) the index
        # set, (F) the routing-audit fix.
        violations += check_coverage_persistence(conn)
        violations += check_band_rank_materialized(conn)
        violations += check_novelty_status_vocabulary(conn)
        violations += check_novelty_source_label_masked(conn)
        violations += check_divergence_correctness_applicability(conn)
        violations += check_visibility_axes(conn)
        violations += check_identification_grain(conn)
        violations += check_manuscript_display_carries_no_reference_content(conn)
        violations += check_works_genre_vocabulary(conn, meta)
        violations += check_meta_audience(conn, meta, audience)
        violations += check_gate_bearing_tables_present(conn)
        violations += check_authorized_index_set(conn)
        violations += check_kept_tie_names_its_pair(conn)
        # CD batch (schema Amendment 2026-08-12): the amendment's own
        # presence/vocabulary/count contract + the Contract-0 basis pin.
        violations += check_amendment_2026_08_12_contract(conn, meta)
        violations += check_locus_reference_basis(conn, meta)
        violations += check_relation_matrix_recompute(conn, meta)
        violations += check_population_lock_retention(conn, meta)
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
    parser.add_argument("--audience", choices=["private", "public"], default="private",
                        help="Which artifact is being verified. 'public' selects the "
                             "public-projection profile: meta.audience must be 'public', and "
                             "frame_content_hash is checked as the SOURCE frame it was projected "
                             "from rather than recomputed over the (deliberately smaller) public "
                             "membership. Default 'private'.")
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
    return verify(args.db_path, args.expected_frame_hash, expected_band_vocabulary=expected_vocab,
                  audience=args.audience)


if __name__ == "__main__":
    sys.exit(main())
