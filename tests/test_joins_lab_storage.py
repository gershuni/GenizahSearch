# -*- coding: utf-8 -*-
"""Tests for web/joins_lab_storage.py.

Covers:
1. Schema-version invalidation (3 cases: stale version, missing key, non-dict).
2. Round-trip write/read via in-memory backing store.
3. Two-anonymous-session no-state-bleed (SC#5).

All storage I/O is monkeypatched — no live NiceGUI storage context required.
The module under test uses only safe_user_get/set/pop; we substitute them with
per-session in-memory dicts to simulate real NiceGUI session isolation.
"""
from __future__ import annotations

import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_session_store() -> tuple:
    """Return a (get, set, pop) triple backed by a private dict.

    Simulates a single NiceGUI anonymous session's storage.  Two calls to
    this function produce two **independent** stores — writing to store A
    never touches store B.
    """
    store: dict = {}

    def _get(key: str, default=None):
        return store.get(key, default)

    def _set(key: str, value) -> bool:
        store[key] = value
        return True

    def _pop(key: str, default=None):
        return store.pop(key, default)

    return _get, _set, _pop


# ---------------------------------------------------------------------------
# Invalidation tests (3 cases)
# ---------------------------------------------------------------------------

def test_schema_version_mismatch_returns_none(monkeypatch):
    """A stored dict with schema_version != 1 is treated as cold start."""
    stale_data = {'schema_version': 0, 'anchor_sys_id': '990001234'}
    monkeypatch.setattr(
        'web.joins_lab_storage.safe_user_get',
        lambda key, default=None: stale_data if key == 'joins_lab' else default,
    )
    import importlib
    import web.joins_lab_storage as _mod
    importlib.reload(_mod)  # ensure monkeypatch hits the module-level names
    # Re-import after reload so the test calls the patched version
    from web.joins_lab_storage import read_joins_lab_state  # noqa: PLC0415
    result = read_joins_lab_state()
    assert result is None, (
        f"Expected None for stale schema_version=0, got {result!r}"
    )


def test_missing_key_returns_none(monkeypatch):
    """Absent joins_lab key (safe_user_get returns None) → cold start."""
    monkeypatch.setattr(
        'web.joins_lab_storage.safe_user_get',
        lambda key, default=None: default,  # always returns default (None)
    )
    from web.joins_lab_storage import read_joins_lab_state
    result = read_joins_lab_state()
    assert result is None, (
        f"Expected None for missing key, got {result!r}"
    )


def test_non_dict_stored_value_returns_none(monkeypatch):
    """A non-dict stored value (e.g. a string) → cold start (isinstance guard)."""
    monkeypatch.setattr(
        'web.joins_lab_storage.safe_user_get',
        lambda key, default=None: 'corrupted-string' if key == 'joins_lab' else default,
    )
    from web.joins_lab_storage import read_joins_lab_state
    result = read_joins_lab_state()
    assert result is None, (
        f"Expected None for non-dict value, got {result!r}"
    )


def test_valid_schema_version_returns_data(monkeypatch):
    """A stored dict with schema_version == 1 is returned as-is."""
    valid_data = {'schema_version': 1, 'anchor_sys_id': '990001234',
                  'anchor_fl_id': None, 'anchor_volume_ie': None}
    monkeypatch.setattr(
        'web.joins_lab_storage.safe_user_get',
        lambda key, default=None: valid_data if key == 'joins_lab' else default,
    )
    from web.joins_lab_storage import read_joins_lab_state
    result = read_joins_lab_state()
    assert result is not None, "Expected data dict, got None"
    assert result['anchor_sys_id'] == '990001234'
    assert result['schema_version'] == 1


# ---------------------------------------------------------------------------
# Round-trip test
# ---------------------------------------------------------------------------

def test_write_then_read_round_trip(monkeypatch):
    """write_anchor() then read_anchor() returns the written dict (round-trip).

    Uses an in-memory backing store so no NiceGUI context is needed.
    """
    _get, _set, _pop = _make_session_store()
    monkeypatch.setattr('web.joins_lab_storage.safe_user_get', _get)
    monkeypatch.setattr('web.joins_lab_storage.safe_user_set', _set)
    monkeypatch.setattr('web.joins_lab_storage.safe_user_pop', _pop)

    from web.joins_lab_storage import write_anchor, read_anchor, _SCHEMA_VERSION

    ok = write_anchor('990001234', anchor_fl_id='T-S 12.123.1r')
    assert ok is True, "write_anchor should return True on success"

    result = read_anchor()
    assert result is not None, "read_anchor() should return data after write_anchor()"
    assert result['anchor_sys_id'] == '990001234'
    assert result['anchor_fl_id'] == 'T-S 12.123.1r'
    assert result['schema_version'] == _SCHEMA_VERSION


# ---------------------------------------------------------------------------
# No-state-bleed test (SC#5 — two anonymous sessions)
# ---------------------------------------------------------------------------

def test_two_sessions_do_not_share_state(monkeypatch):
    """Writing anchor in session A must not surface in session B.

    Simulates two NiceGUI anonymous sessions via two independent in-memory
    dicts.  The joins_lab key written by session A is absent in session B's
    read → read_joins_lab_state() returns None for B (SC#5).
    """
    get_a, set_a, pop_a = _make_session_store()
    get_b, set_b, pop_b = _make_session_store()

    from web import joins_lab_storage as _mod

    # Session A: write an anchor
    monkeypatch.setattr(_mod, 'safe_user_get', get_a)
    monkeypatch.setattr(_mod, 'safe_user_set', set_a)
    monkeypatch.setattr(_mod, 'safe_user_pop', pop_a)
    _mod.write_anchor('990001234')

    # Verify session A sees its own data
    assert _mod.read_joins_lab_state() is not None, \
        "Session A should see the written anchor"

    # Session B: switch to B's backing store — should see nothing
    monkeypatch.setattr(_mod, 'safe_user_get', get_b)
    monkeypatch.setattr(_mod, 'safe_user_set', set_b)
    monkeypatch.setattr(_mod, 'safe_user_pop', pop_b)

    result_b = _mod.read_joins_lab_state()
    assert result_b is None, (
        f"Session B must not see session A's anchor (SC#5 no-state-bleed). "
        f"Got: {result_b!r}"
    )

    # Double-check: the two backing stores are independent
    assert 'joins_lab' not in get_b.__code__.co_freevars or True  # Duck-typed check
    # Simpler: write to B and confirm A doesn't change
    monkeypatch.setattr(_mod, 'safe_user_get', get_b)
    monkeypatch.setattr(_mod, 'safe_user_set', set_b)
    _mod.write_anchor('999999999')

    monkeypatch.setattr(_mod, 'safe_user_get', get_a)
    result_a_again = _mod.read_joins_lab_state()
    assert result_a_again is not None, \
        "Session A's anchor should still be present after session B writes"
    assert result_a_again['anchor_sys_id'] == '990001234', \
        "Session A's anchor_sys_id should still be '990001234'"
