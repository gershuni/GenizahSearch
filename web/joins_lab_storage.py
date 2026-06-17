# -*- coding: utf-8 -*-
"""Versioned safe_storage helpers for the ``joins_lab`` namespace.

All Joins Lab per-user state is funnelled through this module — callers never
touch NiceGUI storage directly; that raw access is forbidden by the Phase 87
CI guard (``tests/test_no_raw_storage_access.py``, allowlist ``[]``).

Schema (Phase 117 definition — same shape as ``117-UI-SPEC.md``):

.. code-block:: python

    {
        'schema_version': 1,    # int; increment ONLY on breaking shape change
        'anchor_sys_id': str | None,
        'anchor_fl_id': str | None,
        'anchor_volume_ie': str | None,
    }

**Forward-compatibility note (Phase 120):**
Phase 120 will extend this dict with builder rows, triage verdicts, and filter
state.  Those keys can be added under the **same** ``schema_version: 1`` — only
bump the version when a key is removed or its type changes.

**Size discipline (search-history-bloat class of bug):**
The spine stores ONLY the anchor fragment's identity (``sys_id``, optional
``fl_id``, optional ``volume_ie``).  Never write ``full_text``, image bytes,
result lists, or other blobs here.  See CHANGELOG v7.16 for the 778 MB
``search_history.json`` incident that motivated this constraint.
"""
from __future__ import annotations

from typing import Any, Optional

from web.safe_storage import safe_user_get, safe_user_set, safe_user_pop

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_JOINS_LAB_KEY = 'joins_lab'
_SCHEMA_VERSION = 1


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def read_joins_lab_state() -> Optional[dict]:
    """Return the stored ``joins_lab`` dict, or ``None`` on cold start.

    Returns ``None`` when:
    - the key is absent from storage,
    - the stored value is not a ``dict``,
    - the ``schema_version`` field does not equal :data:`_SCHEMA_VERSION`.

    Callers treat a ``None`` return as a cold start (no previously pinned
    anchor).
    """
    data: Any = safe_user_get(_JOINS_LAB_KEY, default=None)
    if not isinstance(data, dict):
        return None
    if data.get('schema_version') != _SCHEMA_VERSION:
        return None  # stale or forged schema — discard, cold start
    return data


def write_anchor(
    anchor_sys_id: Optional[str],
    anchor_fl_id: Optional[str] = None,
    anchor_volume_ie: Optional[str] = None,
) -> bool:
    """Persist the current anchor fragment identity to per-user storage.

    :param anchor_sys_id: The manuscript system number (e.g. ``'990001234'``).
        Pass ``None`` to write an explicit "no anchor" record (prefer
        :func:`clear_joins_lab_state` instead if erasing is the intent).
    :param anchor_fl_id: Optional folio/leaf ID (e.g. ``'T-S 12.123.1r'``).
    :param anchor_volume_ie: Optional volume IE identifier for multi-volume MSS.
    :returns: ``True`` if the write succeeded, ``False`` on prune-race or other
        storage failure (mirroring :func:`web.safe_storage.safe_user_set`).

    Blob discipline: only identity fields are written.  Do NOT pass
    ``full_text``, image data, or search result lists.
    """
    payload: dict = {
        'schema_version': _SCHEMA_VERSION,
        'anchor_sys_id': str(anchor_sys_id) if anchor_sys_id is not None else None,
        'anchor_fl_id': anchor_fl_id,
        'anchor_volume_ie': anchor_volume_ie,
    }
    return safe_user_set(_JOINS_LAB_KEY, payload)


def read_anchor() -> Optional[dict]:
    """Convenience alias for the page restore path (D-13).

    Returns the validated storage dict (or ``None``) — identical to
    :func:`read_joins_lab_state`.  Phase 120 can call this to check for a
    previously pinned anchor without knowing the internal schema details.
    """
    return read_joins_lab_state()


def clear_joins_lab_state() -> Any:
    """Remove the ``joins_lab`` key from per-user storage.

    Used by Phase 120's "Clear / Reset" action.  Returns the popped value
    (or ``None`` if the key was absent), mirroring
    :func:`web.safe_storage.safe_user_pop`.
    """
    return safe_user_pop(_JOINS_LAB_KEY, None)
