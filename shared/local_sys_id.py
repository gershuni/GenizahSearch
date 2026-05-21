# -*- coding: utf-8 -*-
"""Phase 95 LOCAL sys_id helpers (LOCAL-NAMESPACE / REQ-2).

The 18-digit format ``97 + machine_id(8 decimal digits) + content_hash(8 decimal digits)``
is the only publishable contract. All call sites MUST consult these helpers; never
hand-roll string slicing or int() conversions.

Per CONTEXT D-19: machine_id and content_hash both use ``% 10**8`` to guarantee
exactly 8 decimal digits. Without the modulo, ``hex(...)[:8]`` can produce up to
10 decimal digits and overflow the 18-digit slot.

Per CONTEXT D-42: filepaths are normalized via ``_canonical_filepath`` BEFORE
hashing. Same physical file must produce same sys_id across rescans regardless
of case differences, separator differences, junctions, or 8.3 short names.

Per CONTEXT D-19 collision-retry: `_content_hash(path, slot=N)` walks deeper
into the SHA256 hex digest on collision (slot 0 = chars [0:8], slot 1 = chars
[8:16], slot 2 = chars [16:24], slot 3 = chars [24:32]). Indexer caps retries at 4.

The repo-grep lint test in ``tests/test_local_sys_id_namespace.py::TestNoIntCoercion``
enforces the string-in/string-out contract.

Public API:
  is_local_sys_id(s)                      -> bool
  generate_local_sys_id(filepath, slot=0) -> str
  _canonical_filepath(p)                  -> str   (private, but tested)
  _machine_id()                           -> str   (private, but tested)
  _content_hash(canonical, slot=0)        -> str   (private, but tested)
"""
from __future__ import annotations

import hashlib
import os
import socket
from pathlib import Path
from typing import Union

_LOCAL_PREFIX = "97"
_MACHINE_PAD = 8
_HASH_PAD = 8
_TOTAL_LENGTH = 2 + _MACHINE_PAD + _HASH_PAD  # 18


def is_local_sys_id(s: object) -> bool:
    """Return True iff ``s`` represents a Phase-95 LOCAL sys_id.

    The discriminator is ``97`` prefix + 18-digit total length. Unlike
    synthetic (Phase 85 — has a ``000000`` suffix as additional discriminator),
    LOCAL has no suffix; the prefix + length is sufficient.

    Examples:
        >>> is_local_sys_id("970012345601234567")
        True
        >>> is_local_sys_id("990001234560000000")  # 99-prefix synthetic
        False
        >>> is_local_sys_id("990025143260205171")  # real Alma
        False
        >>> is_local_sys_id("")
        False
        >>> is_local_sys_id(None)
        False
    """
    if not s:
        return False
    s = str(s)
    if not s.isdigit():
        return False
    if len(s) != _TOTAL_LENGTH:
        return False
    return s.startswith(_LOCAL_PREFIX)


def _canonical_filepath(p: Union[str, Path]) -> str:
    """Canonical form for sys_id generation and folder-overlap detection (D-42).

    Resolves symlinks/junctions (strict=False so missing files still normalize),
    normalizes case (Windows: lowercase drive letter + path), normalizes separators.

    Examples (Windows):
        >>> # All three return the same string:
        >>> # _canonical_filepath("C:/Users/x/Foo.PDF")
        >>> # _canonical_filepath("c:\\\\users\\\\X\\\\foo.pdf")
        >>> # _canonical_filepath("C:\\\\USERS\\\\x\\\\FOO.PDF")
    """
    resolved = Path(p).resolve(strict=False)
    return os.path.normcase(str(resolved))


def _machine_id() -> str:
    """Stable per-machine ID, exactly 8 decimal digits (D-19 % 10**8 contract)."""
    host = socket.gethostname()
    digest_hex = hashlib.sha256(host.encode("utf-8")).hexdigest()
    # CONTEXT D-19: hex[:8] can decode to up to 10 decimal digits.
    # Apply % 10**8 to guarantee exactly 8 decimal digits.
    return f"{int(digest_hex[:8], 16) % 10**8:08d}"


def _content_hash(canonical_filepath: str, slot: int = 0) -> str:
    """Per-file content hash, exactly 8 decimal digits (D-19 + collision retry).

    slot=0 uses hex chars [0:8]; slot=N uses hex chars [8N:8N+8]. Caller
    (indexer) bumps `slot` on UNIQUE constraint collision; max useful slot is 7
    (sha256 hex digest is 64 chars).
    """
    if slot < 0 or slot > 7:
        raise ValueError(f"slot out of range [0,7]: {slot}")
    digest_hex = hashlib.sha256(canonical_filepath.encode("utf-8")).hexdigest()
    start = slot * 8
    return f"{int(digest_hex[start:start + 8], 16) % 10**8:08d}"


def generate_local_sys_id(filepath: Union[str, Path], slot: int = 0) -> str:
    """Generate the 18-digit LOCAL sys_id for a given filepath.

    Per CONTEXT D-18 + D-19 + D-42: canonical filepath -> SHA256 -> modulo 10**8
    -> 8-digit zero-padded, concatenated after machine_id.

    Examples:
        >>> sid = generate_local_sys_id("/tmp/foo.pdf")  # doctest: +SKIP
        >>> # is_local_sys_id(sid) == True
    """
    canonical = _canonical_filepath(filepath)
    return f"{_LOCAL_PREFIX}{_machine_id()}{_content_hash(canonical, slot=slot)}"
