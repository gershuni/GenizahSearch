# -*- coding: utf-8 -*-
"""Phase 85 synthetic sys_id helpers (SYNTH-01).

The 18-digit format ``99 + InventoryId-zfill(10) + 000000`` is the only
publishable contract. All other code MUST consult these helpers; never
hand-roll string slicing or int() conversions.

Per D-01b: sys_ids are strings. Never int(). The 99 prefix preserves
numeric round-trip but we don't permit numeric semantics. The helper
internally calls ``str(s)`` for migration safety (existing call sites may
have already coerced to int), but the contract is "string in, string
out". The repo-grep lint test in tests/test_synthetic_sys_id.py is the
affirmative D-01b enforcement: any new ``int(sys_id)`` outside this file
fails CI.

Per D-13: detection is consistent regardless of whether input has been
digit-normalized by the codebase's
``"".join(ch for ch in str(s) if ch.isdigit())`` pattern (see
``genizah_core.py:3374``). All-digit input + length-18 + leading-99 +
trailing-000000 is sufficient and DETERMINISTIC.

Per D-01a: real Alma sys_ids using the NLI institution suffix ``205171``
(e.g. ``990025143260205171``) MUST NOT classify as synthetic. The
``000000`` suffix is the discriminator. Plan 02 adds an export-time
collision check using these helpers.

Format-only contract: this helper does NOT prove row existence. Plan 02
and Plan 03 collision scans are the affirmative existence guards.

Public API (consumed by Plans 02-05):
  is_synthetic_sys_id(s)              -> bool
  encode_inventory_sys_id(inv_id)     -> str
  decode_inventory_id(sys_id)         -> Optional[int]
"""
from __future__ import annotations

from typing import Optional

_SYNTHETIC_PREFIX = "99"
_SYNTHETIC_SUFFIX = "000000"
_INVENTORY_PAD = 10
_TOTAL_LENGTH = 2 + _INVENTORY_PAD + 6  # 18


def is_synthetic_sys_id(s: object) -> bool:
    """Return True iff ``s`` represents a Phase-85 synthetic sys_id.

    Stable under digit-normalization (D-13): input may already have been
    passed through ``"".join(ch for ch in str(s) if ch.isdigit())`` — this
    helper accepts the canonical all-digit form. Any input with non-digit
    characters returns False.

    The helper internally tolerates int via ``str(s)`` coercion for
    migration safety, but the documented contract is string-only (D-01b);
    new call sites MUST pass strings. The repo-grep lint test
    (``tests/test_synthetic_sys_id.py::TestNoIntCoercion``) enforces this
    affirmatively.

    Examples:
        >>> is_synthetic_sys_id("990001234560000000")  # InvId=123456
        True
        >>> is_synthetic_sys_id("990025143260205171")  # real Alma (NLI 205171 suffix)
        False
        >>> is_synthetic_sys_id("")
        False
        >>> is_synthetic_sys_id(None)
        False
    """
    if not s:
        return False
    s = str(s)
    if not s.isdigit():
        return False
    if len(s) != _TOTAL_LENGTH:
        return False
    return s.startswith(_SYNTHETIC_PREFIX) and s.endswith(_SYNTHETIC_SUFFIX)


def encode_inventory_sys_id(inventory_id: int) -> str:
    """Convert a FIST.db InventoryId into the 18-digit synthetic sys_id.

    Args:
        inventory_id: Positive InventoryId from ``dbo_Inventory``. Must fit in
            10 digits (0 < inventory_id < 10**10).

    Returns:
        18-character all-digit string. Never an int (D-01b).

    Raises:
        ValueError: when inventory_id is non-positive, non-int, or >10-digit overflow.

    Examples:
        >>> encode_inventory_sys_id(1)
        '990000000001000000'
        >>> encode_inventory_sys_id(123456)
        '990001234560000000'
        >>> encode_inventory_sys_id(9999999999)
        '999999999999000000'
    """
    # bool is a subclass of int in Python; reject it explicitly so True/False
    # don't slip through as 1/0.
    if not isinstance(inventory_id, int) or isinstance(inventory_id, bool):
        raise ValueError(
            f"inventory_id must be int; got {type(inventory_id).__name__}: {inventory_id!r}"
        )
    if inventory_id <= 0:
        raise ValueError(f"inventory_id must be positive; got {inventory_id!r}")
    if inventory_id >= 10 ** _INVENTORY_PAD:
        raise ValueError(
            f"inventory_id exceeds {_INVENTORY_PAD}-digit width: {inventory_id}"
        )
    return f"{_SYNTHETIC_PREFIX}{inventory_id:0{_INVENTORY_PAD}d}{_SYNTHETIC_SUFFIX}"


def decode_inventory_id(sys_id: object) -> Optional[int]:
    """Extract the InventoryId from a synthetic sys_id, or None.

    Returns None for any non-synthetic input (real Alma, empty, malformed)
    so callers can use::

        inv = decode_inventory_id(s)
        if inv is not None:
            ...

    without a prior is_synthetic_sys_id check.

    Examples:
        >>> decode_inventory_id("990001234560000000")
        123456
        >>> decode_inventory_id("990025143260205171") is None  # real Alma
        True
        >>> decode_inventory_id("") is None
        True
    """
    if not is_synthetic_sys_id(sys_id):
        return None
    # Documented slice — the only int() coercion allowed in this codebase per
    # D-01b. The TestNoIntCoercion repo-grep lint allowlists this file.
    return int(str(sys_id)[2 : 2 + _INVENTORY_PAD])
