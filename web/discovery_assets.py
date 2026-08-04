# -*- coding: utf-8 -*-
"""Authoritative asset-state source for the Discovery Data Spine (Phase 134,
DATA-07/DATA-08). Modeled 1:1 on the Phase-133-hardened ``web/atlas_assets.py``
(its structure survived 4 Codex hardening rounds) -- mirror it, don't reinvent.

This module is the SINGLE source of truth for two questions the rest of the
web app asks about the discovery sidecar:

  1. "Is discovery available right now?"      -> ``discovery_available()``
  2. "What sidecar am I reading right now?"    -> ``discovery_db_path()`` /
     ``discovery_sidecar_version()`` / ``discovery_meta(key)``

Structural note (mirrors atlas HIGH-1): the sidecar lives in repo-root
``discovery_data/`` -- OUTSIDE ``web/static/`` -- so it can NEVER be served
through the public ``/static`` mount. This phase (134-05) ships NO discovery
route/UI at all; the sidecar is read ONLY through this module's own read-only
sqlite3 connection, never exposed as raw bytes to any HTTP surface.

The sidecar is loaded + validated ONCE at startup by ``load_discovery_state()``
(called from ``web/main.py``, mirroring the ``load_atlas_state()`` wiring
point). ``discovery_available()`` = ``DISCOVERY_ENABLED and <loaded
successfully>`` -- flag AND readiness, NEVER the flag alone -- so a
flag-ON/sidecar-missing (or corrupt/incompatible) window hides cleanly rather
than any future surface trusting a half-loaded state. Deploy posture (mirrors
atlas): the sidecar is scp'd asset-first, THEN the process restarts, so the
startup load is authoritative -- there is deliberately NO per-request
``os.path.exists``.

Fail-closed validation matrix (docs/specs/discovery-sidecar-schema-v1.md is
the frozen contract this validates against): the sidecar is resolved by the
EXACT ``asset_basename`` named in ``manifest.json`` -- a sibling ``*.db`` file
that is NOT that exact name is deliberately IGNORED (rollback-safe: a
rollback that leaves an old sibling behind is never picked up, T-134-rollback).
ANY of the following leaves the state ``ready=False`` with no traceback
escaping the loader (T-134-tamper / T-134-failopen):
  - the named file is absent (only a stale sibling present, or nothing at all)
  - the manifest's ``content_hash`` does not match the actual sidecar bytes
  - ``PRAGMA integrity_check`` does not return ``'ok'`` (corrupt/malformed DB)
  - ``meta.schema_version`` != the frozen ``_EXPECTED_SCHEMA_VERSION``
    (reject-incompatible)
  - ``meta.audience`` is anything other than the single value this PUBLIC
    loader is allowed to serve -- private, missing, empty or unrecognised all
    fail closed identically (Phase 136, plan 136-20; VIS-01). This is a second,
    INDEPENDENT gate on the CONTENT of whatever the manifest selected: the
    manifest says WHICH file, the audience says what that file is allowed to
    be. Without it the public/private exclusion would be procedural (a
    deploy-time discipline) rather than structural (a fact the loader checks).
  - any required ``meta`` release-contract key is missing
  - any required table (per the frozen schema doc) is missing
  - any required COLUMN is missing from a table that IS present -- a subset
    check per ``_REQUIRED_COLUMNS``, because a partially-built asset can carry
    every required table and still be missing a column a read path needs
    (Phase 136, plan 136-20)
  - a release-contract expected row count does not match the actual count
  - a ``confidence_band``/``claim_type`` value falls outside the frozen enum
    vocab for its family (a cheap startup spot-check; the FULL 8-invariant
    release verifier that gates the real offline build lives in
    ``scripts/verify_discovery_sidecar.py`` -- this loader does not duplicate
    that machinery, it re-checks only what matters for safe runtime reads)
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import sqlite3
import threading
from dataclasses import dataclass, field
from typing import Dict, Optional, Tuple

from web.feature_flags import DISCOVERY_ENABLED

logger = logging.getLogger(__name__)

# Repo-root discovery_data/ -- deliberately OUTSIDE web/static/ (mirrors atlas
# HIGH-1). Computed as the parent of this file's directory (web/) -> repo
# root -> discovery_data/.
DISCOVERY_DATA_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "discovery_data",
)
MANIFEST_FILENAME = "manifest.json"

# Frozen schema_version marker (docs/specs/discovery-sidecar-schema-v1.md;
# scripts/build_discovery_sidecar.py::SCHEMA_VERSION). NOTE: this is a
# versioned STRING marker (e.g. "discovery-v1"), not an integer -- the sidecar
# meta table stores it as text, matching the manifest's own "schema_version"
# field. A future incompatible schema bump mints a new string constant here
# (reject-incompatible, never a silent int comparison).
#
# RETAIN DECISION (Phase 136, plan 136-20): the Amendment 2026-08-02 rebuild
# does NOT bump this marker. Three reasons, recorded here so a later reader
# never mistakes the non-bump for an oversight:
#
#  1. The amendment is purely ADDITIVE -- new tables and new columns; no
#     existing field changes meaning. This marker's stated job is
#     reject-incompatible, and nothing in the amendment is incompatible.
#  2. The `_REQUIRED_TABLES`, `_REQUIRED_COLUMNS` and `_RELEASE_CONTRACT_COUNTS`
#     checks below catch the exact failure this gate exists to close -- a
#     pre-rebuild or partially-deployed sidecar passing readiness --
#     deterministically, by table name, by column name and by row count, which
#     is a sharper signal than a version string.
#  3. Rolling BACK to the pre-rebuild asset must leave the surfaces hidden
#     rather than crashing. Those checks give exactly that; a marker bump would
#     too, but with no added safety and a lockstep edit across the builder and
#     the frozen schema document.
#
# Reason 2 is only honest because COLUMNS are actually checked. Without
# `_REQUIRED_COLUMNS`, this marker would have been the only thing standing
# between a partially-built asset and a live nav entry that errors on its first
# query.
_EXPECTED_SCHEMA_VERSION = "discovery-v1"

# The VIS-01 audience boundary (Phase 136, plan 136-20;
# docs/specs/discovery-sidecar-schema-v1.md § Amendment 2026-08-02 (C1)).
# A CLOSED module-level enum, deliberately not an inline literal comparison:
# `meta.audience` is written `private` by the private build and `public` by the
# public-projection step, and THIS loader -- the one every publicly reachable
# route resolves through -- may serve exactly one of them.
#
# Belt and braces, deliberately. The deploy (plan 136-13) points production's
# manifest.json at the PUBLIC projection and keeps the private database off the
# web box entirely, so this check should never fire in a correct deployment. It
# exists because "never fires in a correct deployment" is exactly the property
# that stops being true under a rushed rollback or a mistyped path, and because
# VIS-01's exclusion is supposed to be STRUCTURAL rather than procedural. A
# control that only works when everyone is careful is a procedure, not a
# control.
_AUDIENCES = frozenset({"public", "private"})
_PUBLIC_LOADER_AUDIENCE = "public"

# Required tables (docs/specs/discovery-sidecar-schema-v1.md SS1) -- the full
# two-table claim model + its supporting tables, plus the two tables the
# Amendment 2026-08-02 (B) adds.
_REQUIRED_TABLES = frozenset({
    "works",
    "discovery_claim",
    "discovery_evidence",
    "witness_units",
    "witness_unit_members",
    "meta",
    "band_precision",
    # Amendment 2026-08-02 (B), Phase 136 plan 136-20.
    "discovery_identification",
    "manuscript_display",
})

# Required COLUMNS, per table (Amendment 2026-08-02). `_REQUIRED_TABLES` only
# proves a table is PRESENT; this phase also adds columns to tables that already
# existed, so an audience-public asset with both new tables and correct row
# counts but a missing `coverage_ppm` or `novelty_status` would otherwise pass
# readiness, expose the nav entry, and fail on the first query that touched it
# -- the same failure `_REQUIRED_TABLES` exists to prevent, one level down.
#
# Validated via `PRAGMA table_info` as a SUBSET check, so an unexpected EXTRA
# column is never a failure (a future additive build must not be gratuitously
# rejected).
#
# ADDING A COLUMN TO THE CONTRACT MEANS ADDING IT HERE. The source of truth is
# docs/specs/discovery-sidecar-schema-v1.md § Amendment 2026-08-02 -- sections
# (A) (evidence/works additions), (B) (the two new tables' DDL), (C)
# (`works.genre`, an EXISTING column the amendment populates and constrains) and
# (F) (`discovery_routing_audit.demoted_work_id`, made contractual by the
# kept_tie rule).
#
# NOTE on `discovery_routing_audit`: it is NOT in `_REQUIRED_TABLES` above, but
# naming it here makes it effectively required -- `PRAGMA table_info` on an
# absent table returns no rows, so its required column reads as missing. That is
# intended: the amendment makes `demoted_work_id` contractual, and the builder
# creates the table unconditionally.
_REQUIRED_COLUMNS: Dict[str, frozenset] = {
    "discovery_evidence": frozenset({
        "coverage_ppm",
        "coverage_status",
        "band_rank",
        "novelty_status",
        "novelty_source_label",
        "divergence_correctness",
        "assertion_visibility",
    }),
    "works": frozenset({"genre", "identity_visibility"}),
    "discovery_routing_audit": frozenset({"demoted_work_id"}),
    "discovery_identification": frozenset({
        "identification_id",
        "sys_id",
        "canonical_work_id",
        "display_work_id",
        "main_pool",
        "main_pool_reason",
        "best_band_rank",
        "page_count",
        "max_coverage_ppm",
        "relation_kind",
        "novelty_status",
        "divergence_correctness",
        "assertion_visibility",
        "identity_visibility",
    }),
    "manuscript_display": frozenset({
        "sys_id",
        "library_code",
        "library_sort_key",
        "shelfmark_display",
        "shelfmark_sort_key",
    }),
}

# Required release-contract meta keys (docs/specs/discovery-sidecar-schema-v1.md SS1.5).
_REQUIRED_META_KEYS = frozenset({
    "schema_version",
    "sidecar_version",
    "source_db_sha256",
    "build_date",
    "data_as_of",
    "htr_snapshot_hash",
    "expected_rows_claims",
    "expected_rows_evidence",
    "expected_rows_works",
    "expected_rows_units",
    "frame_content_hash",
    # Amendment 2026-08-02 (C1) -- the two new tables need the same
    # release-contract validation as claims/evidence/works/units.
    "expected_rows_discovery_identification",
    "expected_rows_manuscript_display",
})

# (meta release-contract key, table) pairs cross-checked against actual counts.
_RELEASE_CONTRACT_COUNTS = (
    ("expected_rows_claims", "discovery_claim"),
    ("expected_rows_evidence", "discovery_evidence"),
    ("expected_rows_works", "works"),
    ("expected_rows_units", "witness_units"),
    # Amendment 2026-08-02 (C1) -- fed through the SAME count loop below, no
    # new mechanism.
    ("expected_rows_discovery_identification", "discovery_identification"),
    ("expected_rows_manuscript_display", "manuscript_display"),
)

# Frozen enum vocab spot-check (docs/specs/discovery-sidecar-schema-v1.md
# "Frozen Enum Vocabularies" / scripts/discovery_ids.py). Deliberately
# inlined as plain string constants here rather than importing
# scripts/discovery_ids.py -- this module must stay a lightweight web/
# runtime dependency, not couple to the offline-build script tree. This is a
# defense-in-depth cell-value spot-check only; it is NOT a substitute for the
# full (evidence_kind x evidence_source x confidence_band) combination
# invariant enforced by scripts/verify_discovery_sidecar.py at build time.
_CLAIM_TYPES = frozenset({"direct_witness", "quotes_this_work", "shared_text"})
# v1-read-compat window (Codex #8): the track1_direct top band accepts BOTH the
# v1 stored key `expert_verified` (the live v1 asset + the v1 fixture) AND its
# v2 rename `high_confidence_algorithmic`. Do NOT drop `expert_verified` until
# the v2 manifest is live (135-08) -- a built v2 asset carries only the v2 key
# (the offline verifier's no-mixed-enum-state check enforces that), while this
# runtime spot-check accepts whichever key the currently-loaded asset uses.
_CONFIDENCE_BANDS_BY_SOURCE: Dict[str, frozenset] = {
    "track1_direct": frozenset(
        {"high_confidence_algorithmic", "expert_verified", "tier_a", "screening_rb", "screening_canon"}
    ),
    "propagated": frozenset({"corroborated", "weak", "not_evaluated"}),
}


@dataclass
class _DiscoveryState:
    """Immutable-ish snapshot of the loaded discovery sidecar. Replaced
    wholesale by ``load_discovery_state()`` under a lock so readers never see
    a half-updated state (mirrors ``web/atlas_assets.py::_AtlasState``)."""

    ready: bool = False
    path: Optional[str] = None
    version: Optional[str] = None  # meta['sidecar_version'] -- for LRU/cache versioning downstream
    # meta['audience'] of the artifact that is actually LIVE. Only ever set on
    # a ready state (a refused artifact leaves it None), so a later diagnostic
    # or admin surface can report which artifact is serving without reopening
    # the database -- reachable publicly through discovery_meta('audience').
    audience: Optional[str] = None
    meta: Dict[str, str] = field(default_factory=dict)


_state = _DiscoveryState()
_lock = threading.Lock()


def _sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _resolve_versioned_db() -> Tuple[str, dict]:
    """Resolve the EXACT ``asset_basename``-named sidecar path from
    ``manifest.json``. Sibling ``*.db`` files in ``DISCOVERY_DATA_DIR`` are
    deliberately IGNORED -- only the manifest-named file is ever considered
    (rollback-safe, T-134-rollback). Returns ``(db_path, manifest_dict)``;
    raises on any malformed/missing manifest (caught by the caller's
    fail-closed wrapper).
    """
    manifest_path = os.path.join(DISCOVERY_DATA_DIR, MANIFEST_FILENAME)
    with open(manifest_path, "rb") as fh:
        manifest_bytes = fh.read()
    manifest = json.loads(manifest_bytes.decode("utf-8"))

    asset_basename = manifest.get("asset_basename")
    if not asset_basename or not isinstance(asset_basename, str):
        raise ValueError("manifest missing a string asset_basename")
    # Defensive: the basename must be a bare filename stem -- never a path
    # (mirrors web/atlas_assets.py's identical guard) -- fails closed rather
    # than ever composing a traversal-shaped path.
    if (
        "/" in asset_basename
        or "\\" in asset_basename
        or os.path.isabs(asset_basename)
        or asset_basename in (".", "..")
    ):
        raise ValueError("asset_basename must be a bare filename stem")

    db_path = os.path.join(DISCOVERY_DATA_DIR, f"{asset_basename}.db")
    return db_path, manifest


def load_discovery_state() -> bool:
    """Load + fully validate the discovery.db sidecar ONCE at startup.

    Fail-closed: EVERY failure mode (missing/malformed manifest, absent named
    file, a sibling *.db that isn't the manifest's exact basename,
    content_hash mismatch, failed PRAGMA integrity_check, an incompatible
    schema_version, a missing required meta key or table, a release-contract
    row-count mismatch, or an out-of-vocab enum value) leaves the module
    state ``ready=False`` with no traceback ever escaping this function --
    the app stays fully up and ``discovery_available()`` then reads False, so
    every future discovery surface hides cleanly.

    Safe to call more than once (a rebuild + restart, or a test re-point of
    ``DISCOVERY_DATA_DIR``) -- it atomically replaces the module state under
    a lock, mirroring ``web/atlas_assets.py::load_atlas_state()``.
    """
    global _state
    new_state = _DiscoveryState(ready=False)
    conn: Optional[sqlite3.Connection] = None
    try:
        db_path, manifest = _resolve_versioned_db()

        content_hash = manifest.get("content_hash")
        if not content_hash or not isinstance(content_hash, str):
            raise ValueError("manifest missing a string content_hash")
        actual_hash = _sha256_file(db_path)  # raises FileNotFoundError if the named file is absent
        if actual_hash != content_hash:
            raise ValueError("content_hash mismatch (manifest vs actual sidecar bytes)")

        uri = f"file:{db_path}?mode=ro"
        conn = sqlite3.connect(uri, uri=True)
        conn.row_factory = sqlite3.Row

        (integrity_result,) = conn.execute("PRAGMA integrity_check").fetchone()
        if integrity_result != "ok":
            # The result string is SQLite's, but it is derived from an artifact
            # this loader has just decided not to trust. Report that it failed
            # and how much it said, never what it said (see the audience check
            # below for the same discipline, and the module note on why).
            raise ValueError(
                "PRAGMA integrity_check failed "
                f"(detail withheld, {len(str(integrity_result))} chars) -- "
                "run scripts/verify_discovery_sidecar.py against the artifact for detail"
            )

        meta_rows = conn.execute("SELECT key, value FROM meta").fetchall()
        meta = {row["key"]: row["value"] for row in meta_rows}

        missing_meta_keys = _REQUIRED_META_KEYS - meta.keys()
        if missing_meta_keys:
            raise ValueError(f"meta missing required key(s): {sorted(missing_meta_keys)}")

        schema_version = meta.get("schema_version")
        if schema_version != _EXPECTED_SCHEMA_VERSION:
            # Only OUR expected value is safe to name; the found value is
            # untrusted artifact content.
            raise ValueError(
                f"incompatible schema_version (expected {_EXPECTED_SCHEMA_VERSION!r}, "
                "found value withheld) -- reject-incompatible"
            )

        # The VIS-01 audience boundary (plan 136-20). Same reject-incompatible
        # idiom as the schema_version check directly above -- fail closed on
        # private, missing, empty or unrecognised alike, so the DEFAULT is
        # closed and an artifact that says nothing about its audience is never
        # assumed public.
        #
        # The raw value is deliberately NEVER interpolated into the message:
        # this loader may be looking at an artifact it has just decided not to
        # trust, and the reason is fully expressible without echoing any of its
        # content (T-136-20-05).
        audience = meta.get("audience")
        if audience != _PUBLIC_LOADER_AUDIENCE:
            if audience in _AUDIENCES:
                raise ValueError(
                    "refusing a private-audience artifact: this public loader may only "
                    f"serve meta.audience={_PUBLIC_LOADER_AUDIENCE!r} -- reject-incompatible"
                )
            raise ValueError(
                "meta.audience is missing, empty or outside the closed {public, private} "
                "enum -- fail-closed default, never treated as public"
            )

        table_rows = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        ).fetchall()
        actual_tables = {row["name"] for row in table_rows}
        missing_tables = _REQUIRED_TABLES - actual_tables
        if missing_tables:
            raise ValueError(f"missing required table(s): {sorted(missing_tables)}")

        # Required COLUMNS (Amendment 2026-08-02). A SUBSET check per table via
        # PRAGMA table_info -- an unexpected extra column is NOT a failure. The
        # table names come from the fixed `_REQUIRED_COLUMNS` allowlist above,
        # never from user input (PRAGMA cannot be parameterized).
        for table in sorted(_REQUIRED_COLUMNS):
            column_rows = conn.execute(f"PRAGMA table_info({table})").fetchall()  # noqa: S608 -- fixed allowlisted table names, never user input
            actual_columns = {row["name"] for row in column_rows}
            missing_columns = _REQUIRED_COLUMNS[table] - actual_columns
            if missing_columns:
                raise ValueError(
                    f"table {table!r} missing required column(s): {sorted(missing_columns)}"
                )

        for meta_key, table in _RELEASE_CONTRACT_COUNTS:
            expected = meta.get(meta_key)
            (actual,) = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()  # noqa: S608 -- table is one of the fixed allowlisted names above, never user input
            if expected is None or int(expected) != actual:
                raise ValueError(
                    f"release-contract row-count mismatch: meta.{meta_key}={expected!r}, "
                    f"actual {table} count={actual}"
                )

        # Frozen enum vocab spot-check (defense-in-depth). The full
        # (evidence_kind x evidence_source x confidence_band) combination
        # invariant is enforced by scripts/verify_discovery_sidecar.py at
        # build time; this is a cheap runtime sanity re-check only.
        claim_type_rows = conn.execute("SELECT DISTINCT claim_type FROM discovery_claim").fetchall()
        invalid_claim_types = {row[0] for row in claim_type_rows} - _CLAIM_TYPES
        if invalid_claim_types:
            # Count, never the values: an out-of-vocabulary claim_type is by
            # definition a string this loader did not put there.
            raise ValueError(
                f"discovery_claim.claim_type: {len(invalid_claim_types)} distinct "
                "value(s) outside the frozen vocabulary (values withheld) -- "
                "run scripts/verify_discovery_sidecar.py against the artifact for detail"
            )

        band_rows = conn.execute(
            "SELECT DISTINCT evidence_source, confidence_band FROM discovery_evidence"
        ).fetchall()
        invalid_band_pairs = 0
        for evidence_source, confidence_band in band_rows:
            valid_bands = _CONFIDENCE_BANDS_BY_SOURCE.get(evidence_source)
            if valid_bands is None or confidence_band not in valid_bands:
                invalid_band_pairs += 1
        if invalid_band_pairs:
            raise ValueError(
                f"discovery_evidence: {invalid_band_pairs} distinct "
                "(evidence_source, confidence_band) combination(s) outside the frozen "
                "vocabulary (values withheld) -- run scripts/verify_discovery_sidecar.py "
                "against the artifact for detail"
            )

        new_state = _DiscoveryState(
            ready=True,
            path=db_path,
            version=meta.get("sidecar_version"),
            audience=audience,
            meta=meta,
        )
    except Exception as exc:  # fail-closed: never raise out of startup load
        logger.info("Discovery sidecar not loaded (fail-closed): %s", exc)
        new_state = _DiscoveryState(ready=False)
    finally:
        if conn is not None:
            conn.close()

    with _lock:
        _state = new_state
    return _state.ready


def discovery_available() -> bool:
    """The ONE predicate any future discovery surface must gate on.

    True only when the dedicated flag is ON AND the sidecar loaded and
    validated successfully at startup. Reads the module globals live so a
    test can flip either the flag or the loaded state -- mirrors
    ``web/atlas_assets.py::atlas_preview_available()`` exactly.
    """
    return bool(DISCOVERY_ENABLED and _state.ready)


def discovery_db_path() -> Optional[str]:
    """LAZY path provider for the loaded sidecar. Read at CALL time (never
    captured at import) so a later ``load_discovery_state()`` re-point (a
    rebuild + restart, or a test re-load) is always reflected immediately --
    e.g. by ``shared/discovery_service.py``'s lazy connection provider."""
    return _state.path


def discovery_sidecar_version() -> Optional[str]:
    """LAZY ``sidecar_version`` provider -- read at call time. Intended for
    cache/LRU keys downstream (a version change must invalidate any cached
    read), not captured at import."""
    return _state.version


def discovery_meta(key: str) -> Optional[str]:
    """LAZY release-contract ``meta`` getter -- read at call time. Returns
    ``None`` for an unknown key or when the sidecar isn't loaded/ready."""
    return _state.meta.get(key)
