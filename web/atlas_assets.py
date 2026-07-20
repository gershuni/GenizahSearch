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
representation -> 406" path genuinely reachable. A PRESENT ``.bin.br`` is not
trusted blindly: it is Brotli-decompressed and compared byte-for-byte against
the plain payload; a corrupt/partial/mismatched sidecar just drops the brotli
representation (readiness is unaffected) rather than ever being served.

Fail-closed: ANY failure of the required parts (missing/unparseable manifest,
missing/empty plain bytes, a basename that is not a bare filename stem, a
missing/mismatched content-hash, a non-content-hashed ``asset_basename``, or a
structurally invalid binary header/section-table per
``docs/specs/atlas-asset-schema-v1.md``) leaves the state ``ready=False`` with
no traceback.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import struct
import threading
from dataclasses import dataclass
from typing import Optional

from web.feature_flags import ATLAS_PREVIEW_ENABLED

try:
    import brotli
except ImportError:  # pragma: no cover - brotli is a genuine runtime dep (see
    # requirements.txt) but this import is defensively guarded so a
    # not-yet-installed environment degrades to "brotli representation
    # unavailable" (fail-closed) rather than crashing the whole web process.
    brotli = None

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Binary header / section-table structural validation (MEDIUM-1). Mirrors the
# FROZEN contract in docs/specs/atlas-asset-schema-v1.md sections 2-3 -- this
# is header + section-table BOUNDS validation only (not a full decode): magic,
# schema_version, and every section's dtype/elem_size/count/byte_offset/
# byte_length must be internally consistent and stay within the buffer.
# ---------------------------------------------------------------------------
_ATLAS_MAGIC = b"ATLAS001"
_ATLAS_SCHEMA_VERSION = 1
_HEADER_SIZE = 16  # 8-byte magic + uint32 schema_version + uint32 section_count
_SECTION_ENTRY_SIZE = 32  # schema §3
_DTYPE_ELEM_SIZE = {1: 4, 2: 1, 3: 2, 4: 4, 5: 8}  # dtype_code -> elem_size (schema §5)


def _validate_atlas_binary_header(data: bytes) -> None:
    """Validate the fixed header + section table of a candidate atlas binary.

    Raises ``ValueError`` (caught by ``load_atlas_state``'s fail-closed
    wrapper) on ANY structural violation:
      - buffer too short for the fixed header / the declared section table
      - bad magic bytes
      - unsupported schema_version
      - a section's dtype_code/elem_size disagree, its byte_length doesn't
        equal count*elem_size, its byte_offset isn't 8-byte aligned, or its
        [byte_offset, byte_offset+byte_length) range falls outside the buffer

    This is intentionally NOT a full decode (no string-heap/edge-delta
    interpretation) -- just enough to guarantee every section slice the data
    route or a downstream decoder takes is in-bounds.
    """
    if len(data) < _HEADER_SIZE:
        raise ValueError(f"atlas binary too short for fixed header: {len(data)} bytes")
    magic, schema_version, section_count = struct.unpack_from("<8sII", data, 0)
    if magic != _ATLAS_MAGIC:
        raise ValueError(f"bad atlas binary magic: {magic!r}")
    if schema_version != _ATLAS_SCHEMA_VERSION:
        raise ValueError(f"unsupported atlas schema_version: {schema_version}")

    table_end = _HEADER_SIZE + _SECTION_ENTRY_SIZE * section_count
    if table_end > len(data):
        raise ValueError(
            f"section table ({section_count} entries) overruns buffer "
            f"({table_end} > {len(data)} bytes)"
        )

    off = _HEADER_SIZE
    for _ in range(section_count):
        _sec_id, dtype_code, elem_size, count, byte_offset, byte_length = (
            struct.unpack_from("<IIIIQQ", data, off)
        )
        expected_elem_size = _DTYPE_ELEM_SIZE.get(dtype_code)
        if expected_elem_size is None or expected_elem_size != elem_size:
            raise ValueError(
                f"section {_sec_id}: dtype_code {dtype_code} / elem_size "
                f"{elem_size} mismatch"
            )
        if byte_length != count * elem_size:
            raise ValueError(
                f"section {_sec_id}: byte_length {byte_length} != "
                f"count*elem_size ({count}*{elem_size})"
            )
        if byte_offset % 8 != 0:
            raise ValueError(
                f"section {_sec_id}: byte_offset {byte_offset} not 8-byte aligned"
            )
        if byte_offset > len(data) or byte_offset + byte_length > len(data):
            raise ValueError(
                f"section {_sec_id}: bounds out of range (offset={byte_offset}, "
                f"length={byte_length}, buffer={len(data)})"
            )
        off += _SECTION_ENTRY_SIZE

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

        # MEDIUM-1: validate the binary header + section-table BOUNDS per
        # docs/specs/atlas-asset-schema-v1.md before trusting these bytes for
        # anything. Fail-closed on any structural violation (bad magic, an
        # unsupported schema_version, or a section whose dtype/count/offset/
        # length is internally inconsistent or falls outside the buffer).
        _validate_atlas_binary_header(plain_bytes)

        # MEDIUM-2: the manifest MUST carry a content_hash, it MUST match
        # sha256(plain)[:12] (the schema-v1 filename derivation), AND
        # asset_basename MUST be exactly the canonical content-hashed form
        # "atlas-v1-<content_hash>" (schema §9) -- otherwise the 1-year
        # immutable cache the data route applies to this basename would be
        # unsafe (a non-content-hashed name could get reused for different
        # bytes after a rebake, stranding a stale cached copy). Any mismatch
        # fails closed rather than serving under an unverified name.
        content_hash = manifest.get("content_hash")
        if not content_hash or not isinstance(content_hash, str):
            raise ValueError("manifest missing a string content_hash")
        computed_hash = hashlib.sha256(plain_bytes).hexdigest()[:12]
        if content_hash != computed_hash:
            raise ValueError("content_hash mismatch (manifest vs plain .bin bytes)")
        expected_basename = f"atlas-v1-{content_hash}"
        if asset_basename != expected_basename:
            raise ValueError(
                f"asset_basename {asset_basename!r} is not the content-hashed "
                f"form {expected_basename!r} -- refusing to apply an immutable "
                "cache to a non-content-hashed filename"
            )

        # OPTIONAL brotli representation. Absent -> br branch simply unavailable.
        # MEDIUM-3: a PRESENT .bin.br is never trusted blindly -- it must
        # Brotli-decompress AND decompress to byte-identical content as the
        # plain payload, or it is dropped (readiness is unaffected either way;
        # only the brotli branch of the negotiation becomes unreachable).
        br_bytes: Optional[bytes] = None
        br_path = os.path.join(ATLAS_DATA_DIR, f"{asset_basename}.bin.br")
        if os.path.exists(br_path):
            with open(br_path, "rb") as fh:
                candidate_br = fh.read() or None
            if candidate_br is not None:
                if brotli is None:
                    logger.warning(
                        "atlas .bin.br present but the brotli package is not "
                        "installed -- brotli representation unavailable (fail-closed)"
                    )
                else:
                    try:
                        decompressed = brotli.decompress(candidate_br)
                    except Exception as br_exc:
                        logger.warning(
                            "atlas .bin.br failed to decompress -- brotli "
                            "representation unavailable (fail-closed): %s", br_exc,
                        )
                    else:
                        if decompressed == plain_bytes:
                            br_bytes = candidate_br
                        else:
                            logger.warning(
                                "atlas .bin.br decompressed content does not match "
                                "the plain .bin payload -- brotli representation "
                                "unavailable (fail-closed)"
                            )

        # ETag for the MUTABLE manifest pointer: the content_hash (required,
        # above; changes on every rebake since the asset filename is
        # content-hashed). Quoted per RFC 7232.
        etag_core = content_hash

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
