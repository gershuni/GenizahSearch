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

NOTE (Task 1 skeleton -- completed in Task 2): this revision resolves the
sidecar by its EXACT manifest ``asset_basename`` (siblings ignored,
rollback-safe), verifies the manifest ``content_hash`` against the actual
bytes, opens the file read-only, and checks ``PRAGMA integrity_check`` +
``meta.schema_version``. The full release-contract matrix (required meta
keys, required tables, release-contract row counts, frozen enum vocab
spot-check) is completed in Task 2 against
``docs/specs/discovery-sidecar-schema-v1.md``.
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
    """Load + validate the discovery.db sidecar ONCE at startup. Fail-closed:
    any error anywhere leaves the module state ``ready=False`` with no
    traceback escaping this function -- the app stays fully up and
    ``discovery_available()`` then reads False.

    Task 1 (this revision) validates: exact-basename manifest resolution
    (siblings ignored), the manifest ``content_hash`` against the actual
    sidecar bytes, ``PRAGMA integrity_check``, and ``meta.schema_version``.
    Task 2 completes the release-contract matrix (required meta keys,
    required tables, release-contract row counts, frozen enum vocab).

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

        schema_version = meta.get("schema_version")
        if schema_version != _EXPECTED_SCHEMA_VERSION:
            raise ValueError(
                f"incompatible schema_version {schema_version!r} "
                f"(expected {_EXPECTED_SCHEMA_VERSION!r}) -- reject-incompatible"
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
