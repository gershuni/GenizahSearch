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
  - any required ``meta`` release-contract key is missing
  - any required table (per the frozen schema doc) is missing
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
_EXPECTED_SCHEMA_VERSION = "discovery-v1"

# Required tables (docs/specs/discovery-sidecar-schema-v1.md SS1) -- the full
# two-table claim model + its supporting tables.
_REQUIRED_TABLES = frozenset({
    "works",
    "discovery_claim",
    "discovery_evidence",
    "witness_units",
    "witness_unit_members",
    "meta",
    "band_precision",
})

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
})

# (meta release-contract key, table) pairs cross-checked against actual counts.
_RELEASE_CONTRACT_COUNTS = (
    ("expected_rows_claims", "discovery_claim"),
    ("expected_rows_evidence", "discovery_evidence"),
    ("expected_rows_works", "works"),
    ("expected_rows_units", "witness_units"),
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
_CONFIDENCE_BANDS_BY_SOURCE: Dict[str, frozenset] = {
    "track1_direct": frozenset({"expert_verified", "tier_a", "screening_rb", "screening_canon"}),
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
            raise ValueError(f"PRAGMA integrity_check failed: {integrity_result!r}")

        meta_rows = conn.execute("SELECT key, value FROM meta").fetchall()
        meta = {row["key"]: row["value"] for row in meta_rows}

        missing_meta_keys = _REQUIRED_META_KEYS - meta.keys()
        if missing_meta_keys:
            raise ValueError(f"meta missing required key(s): {sorted(missing_meta_keys)}")

        schema_version = meta.get("schema_version")
        if schema_version != _EXPECTED_SCHEMA_VERSION:
            raise ValueError(
                f"incompatible schema_version {schema_version!r} "
                f"(expected {_EXPECTED_SCHEMA_VERSION!r}) -- reject-incompatible"
            )

        table_rows = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        ).fetchall()
        actual_tables = {row["name"] for row in table_rows}
        missing_tables = _REQUIRED_TABLES - actual_tables
        if missing_tables:
            raise ValueError(f"missing required table(s): {sorted(missing_tables)}")

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
            raise ValueError(f"invalid claim_type value(s): {sorted(invalid_claim_types)}")

        band_rows = conn.execute(
            "SELECT DISTINCT evidence_source, confidence_band FROM discovery_evidence"
        ).fetchall()
        for evidence_source, confidence_band in band_rows:
            valid_bands = _CONFIDENCE_BANDS_BY_SOURCE.get(evidence_source)
            if valid_bands is None or confidence_band not in valid_bands:
                raise ValueError(
                    "invalid (evidence_source, confidence_band) combination: "
                    f"({evidence_source!r}, {confidence_band!r})"
                )

        new_state = _DiscoveryState(
            ready=True,
            path=db_path,
            version=meta.get("sidecar_version"),
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
