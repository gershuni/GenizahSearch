# -*- coding: utf-8 -*-
"""Authoritative asset-state source for the Visual Atlas Preview (Phase 133, ATLAS-01).

This module is the SINGLE source of truth for two questions the rest of the web
app asks about the atlas preview:

  1. "Is the atlas preview available right now?"  -> ``atlas_preview_available()``
  2. "What bytes do I serve for the manifest / the content-hashed asset?"
     -> ``atlas_manifest_bytes()`` / ``atlas_plain_bytes()`` / ``atlas_br_bytes()`` ...

HIGH-1 (structural): the baked asset lives in repo-root ``atlas_data/`` — OUTSIDE
``web/static/`` — so it can NEVER be served through the public ``/static`` mount
and thereby bypass the ATLAS_PREVIEW_ENABLED flag. It is served ONLY through the
dedicated, flag+readiness-gated data routes in ``web/main.py``.

HIGH-2 / MEDIUM-6: the asset bytes are loaded ONCE at startup by
``load_atlas_state()`` (called from ``web/main.py``). ``atlas_preview_available()``
= ``ATLAS_PREVIEW_ENABLED and <loaded successfully>``, and that SAME predicate
gates the page route, the data routes, and the nav link — so those surfaces can
never disagree (a flag-ON/asset-missing window hides cleanly everywhere rather
than rendering broken chrome over a 404ing fetch). Deploy posture: the asset is
scp'd asset-first, THEN the process restarts (133-06), so the startup load is
authoritative — there is deliberately NO per-request ``os.path.exists``.

MEDIUM-3: readiness requires the manifest parsed AND the plain ``.bin`` loaded
(both REQUIRED). The ``.bin.br`` (Brotli) representation is OPTIONAL — absent
does NOT block readiness, it only makes the brotli branch of the data route's
Accept-Encoding negotiation unavailable, which is what makes the "no acceptable
representation -> 406" path genuinely reachable.

Fail-closed: ANY failure of the required parts (missing/unparseable manifest,
missing/empty plain bytes, a basename that is not a bare filename stem, a
content-hash mismatch) leaves the state ``ready=False`` with no traceback.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import threading
from dataclasses import dataclass
from typing import Optional

from web.feature_flags import ATLAS_PREVIEW_ENABLED

logger = logging.getLogger(__name__)

# Repo-root atlas_data/ — deliberately OUTSIDE web/static/ (HIGH-1). Computed as
# the parent of this file's directory (web/) -> repo root -> atlas_data/.
ATLAS_DATA_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "atlas_data",
)
MANIFEST_FILENAME = "manifest.json"


@dataclass
class _AtlasState:
    """Immutable-ish snapshot of the loaded atlas asset. Replaced wholesale by
    ``load_atlas_state()`` under a lock so readers never see a half-updated state."""

    ready: bool = False
    manifest_bytes: Optional[bytes] = None
    manifest: Optional[dict] = None
    bin_name: Optional[str] = None  # e.g. "atlas-v1-<content_hash>.bin"
    plain_bytes: Optional[bytes] = None
    br_bytes: Optional[bytes] = None  # None when the .bin.br is absent (optional)
    etag: Optional[str] = None  # quoted ETag for the manifest (mutable pointer)


_state = _AtlasState()
_lock = threading.Lock()


def load_atlas_state() -> bool:
    """Load the atlas asset state ONCE at startup. Fail-closed on any error.

    Returns ``state.ready``. Safe to call more than once (a rebake + restart, or
    a test re-point of ``ATLAS_DATA_DIR``) — it atomically replaces the module
    state under a lock.
    """
    global _state
    new_state = _AtlasState(ready=False)
    try:
        manifest_path = os.path.join(ATLAS_DATA_DIR, MANIFEST_FILENAME)
        with open(manifest_path, "rb") as fh:
            manifest_bytes = fh.read()
        manifest = json.loads(manifest_bytes.decode("utf-8"))

        asset_basename = manifest.get("asset_basename")
        if not asset_basename or not isinstance(asset_basename, str):
            raise ValueError("manifest missing a string asset_basename")
        # Defensive: the basename must be a bare filename stem — never a path.
        # (The data route also whitelist-compares the requested name to the
        # loaded bin_name, but rejecting a non-stem basename here fails closed
        # rather than ever composing a traversal-shaped path.)
        if (
            "/" in asset_basename
            or "\\" in asset_basename
            or os.path.isabs(asset_basename)
            or asset_basename in (".", "..")
        ):
            raise ValueError("asset_basename must be a bare filename stem")

        bin_name = f"{asset_basename}.bin"
        plain_path = os.path.join(ATLAS_DATA_DIR, bin_name)
        with open(plain_path, "rb") as fh:
            plain_bytes = fh.read()
        if not plain_bytes:
            raise ValueError("plain .bin payload is empty")

        # Consistency: if the manifest carries a content_hash it MUST match
        # sha256(plain)[:12] (the schema-v1 filename derivation). A mismatch
        # means the manifest and the bytes disagree -> fail closed.
        content_hash = manifest.get("content_hash")
        computed_hash = hashlib.sha256(plain_bytes).hexdigest()[:12]
        if content_hash and content_hash != computed_hash:
            raise ValueError("content_hash mismatch (manifest vs plain .bin bytes)")

        # OPTIONAL brotli representation. Absent -> br branch simply unavailable.
        br_bytes: Optional[bytes] = None
        br_path = os.path.join(ATLAS_DATA_DIR, f"{asset_basename}.bin.br")
        if os.path.exists(br_path):
            with open(br_path, "rb") as fh:
                br_bytes = fh.read() or None

        # ETag for the MUTABLE manifest pointer: prefer the content_hash (changes
        # on every rebake, since the asset filename is content-hashed), else a
        # sha256 of the manifest bytes. Quoted per RFC 7232.
        etag_core = content_hash or hashlib.sha256(manifest_bytes).hexdigest()[:16]

        new_state = _AtlasState(
            ready=True,
            manifest_bytes=manifest_bytes,
            manifest=manifest,
            bin_name=bin_name,
            plain_bytes=plain_bytes,
            br_bytes=br_bytes,
            etag=f'"{etag_core}"',
        )
    except Exception as exc:  # fail-closed: never raise out of startup load
        logger.info("Atlas preview asset not loaded (fail-closed): %s", exc)
        new_state = _AtlasState(ready=False)

    with _lock:
        _state = new_state
    return _state.ready


def atlas_preview_available() -> bool:
    """The ONE predicate gating the page route, the data routes, and the nav link.

    True only when the dedicated flag is ON AND the baked asset loaded
    successfully at startup. Reads the module globals live so a test can flip
    either the flag or the loaded state.
    """
    return bool(ATLAS_PREVIEW_ENABLED and _state.ready)


def atlas_bin_name() -> Optional[str]:
    """The content-hashed plain-asset filename (``atlas-v1-<hash>.bin``) the data
    route whitelist-compares an incoming asset name against."""
    return _state.bin_name


def atlas_plain_bytes() -> Optional[bytes]:
    """The uncompressed asset bytes (REQUIRED for readiness)."""
    return _state.plain_bytes


def atlas_br_bytes() -> Optional[bytes]:
    """The Brotli-precompressed asset bytes, or None when the .bin.br is absent."""
    return _state.br_bytes


def atlas_manifest_bytes() -> Optional[bytes]:
    """The raw manifest.json bytes served (no-cache + ETag) by the manifest route."""
    return _state.manifest_bytes


def atlas_manifest_etag() -> Optional[str]:
    """The quoted ETag for the mutable manifest pointer (content_hash-derived)."""
    return _state.etag
