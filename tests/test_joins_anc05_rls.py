# -*- coding: utf-8 -*-
"""ANC-05 RED stubs — status='confirmed' query filter + cache-key isolation +
no cross-user cache poisoning.

Module docstring / directive for Plan 02
=========================================

status='confirmed' IS the PRIMARY ANC-05 mechanism.

The fragment_joins.status column EXISTS in the live Supabase deployment:
  - supabase_setup.sql:162 defines:
    status TEXT DEFAULT 'proposed' CHECK (status IN ('proposed','confirmed','rejected'))
  - web/supabase_client.py:1574-1594: get_fragment_joins(..., status=None) already
    applies .eq('status', status) when status is truthy, and its docstring explicitly
    references status='confirmed'.

RLS context: The joins policy is USING(true) (anyone can SELECT all rows). The DB
does NOT enforce confirmed-only; the application-layer status='confirmed' filter IS
the ANC-05 mechanism that realizes D-17 (a user's own 'proposed' joins are excluded
from the process-global Lab known-joins group).

The :confirmed cache-key suffix prevents cross-cache poisoning: the Lab path writes
a separate key so that a cached confirmed-only result never pollutes the browse-dialog
cache (which shows all joins to the owner).

CONDITIONAL FALLBACK (only if live probe proves status column absent):
  Instead of passing status='confirmed', exclude source=='user' rows and rely on
  cache-key isolation alone. This is a contingency, not the default path.

These tests assert the OBSERVABLE contract:
  1. confirmed_only=True path passes status='confirmed' to get_fragment_joins (EXACT value)
  2. confirmed_only=True uses ':confirmed' cache key suffix
  3. confirmed_only=False (default) passes no status filter to get_fragment_joins
  4. confirmed_only=False uses the unconfirmed cache key (no :confirmed suffix)
  5. No cross-user poisoning: unconfirmed cache hit is NOT served on the confirmed path

All tests are RED until Plan 02 adds `confirmed_only: bool = False` to
fetch_connected_fragments and wires the status='confirmed' filter + :confirmed cache key.
"""

from web.components import joins_panel
from web.components.joins_panel import fetch_connected_fragments, _joins_cache


def _clear_cache():
    with joins_panel._joins_cache_lock:
        _joins_cache.clear()


def _no_op_monkeypatches(monkeypatch):
    """Common no-op patches for I/O dependencies."""
    monkeypatch.setattr('web.components.joins_panel.get_fragment_joins',
                        lambda **kwargs: [])
    monkeypatch.setattr('web.components.joins_panel.state',
                        type('S', (), {'meta_mgr': None})())

    import web.document_service as ds
    monkeypatch.setattr(ds, 'get_document_for_fragment', lambda sid: None)
    monkeypatch.setattr(ds, 'get_fragments_for_document', lambda pgpid: [])

    import web.fjms_service as fjms
    class _FakeSvc:
        def is_available(self): return False
        def get_join_group(self, sid): return []
    monkeypatch.setattr(fjms, 'get_fjms_service', lambda **kw: _FakeSvc())


# ---------------------------------------------------------------------------
# ANC-05 tests — all RED until Plan 02
# ---------------------------------------------------------------------------


def test_confirmed_only_uses_separate_cache_key(monkeypatch):
    """confirmed_only=True must populate cache key ending in ':confirmed'.

    The key 'doc:99X:pgp:7:confirmed' must exist after the call.
    The unconfirmed key 'doc:99X:pgp:7' must NOT be written.
    """
    _clear_cache()
    _no_op_monkeypatches(monkeypatch)

    # This call FAILS (RED): confirmed_only kwarg does not exist yet
    fetch_connected_fragments(
        document_id='99X', pgpid=7, confirmed_only=True, force_refresh=True
    )

    with joins_panel._joins_cache_lock:
        keys = set(_joins_cache.keys())

    confirmed_key = 'doc:99X:pgp:7:confirmed'
    unconfirmed_key = 'doc:99X:pgp:7'

    assert confirmed_key in keys, (
        f"Expected cache key '{confirmed_key}' after confirmed_only=True call. "
        f"Actual keys: {keys}"
    )
    assert unconfirmed_key not in keys, (
        f"Unconfirmed key '{unconfirmed_key}' must NOT be written on confirmed_only=True path. "
        f"Actual keys: {keys}"
    )


def test_default_call_uses_unconfirmed_key(monkeypatch):
    """confirmed_only=False (default) populates 'doc:99X:pgp:7' only (no :confirmed suffix)."""
    _clear_cache()
    _no_op_monkeypatches(monkeypatch)

    # Default call (no confirmed_only arg — or confirmed_only=False)
    fetch_connected_fragments(
        document_id='99X', pgpid=7, force_refresh=True
    )

    with joins_panel._joins_cache_lock:
        keys = set(_joins_cache.keys())

    unconfirmed_key = 'doc:99X:pgp:7'
    confirmed_key = 'doc:99X:pgp:7:confirmed'

    assert unconfirmed_key in keys, (
        f"Expected unconfirmed key '{unconfirmed_key}' for default call. Keys: {keys}"
    )
    assert confirmed_key not in keys, (
        f"':confirmed' key must NOT be written on default (confirmed_only=False) path. Keys: {keys}"
    )


def test_confirmed_path_passes_status_confirmed_to_get_fragment_joins(monkeypatch):
    """confirmed_only=True must invoke get_fragment_joins with status='confirmed'.

    The EXACT value 'confirmed' must be passed (not merely non-None) because this
    is the primary ANC-05 mechanism — the DB column has CHECK (status IN
    ('proposed','confirmed','rejected')) and any other value would return empty results.
    """
    _clear_cache()
    received_kwargs = {}

    def _capture_kwargs(**kwargs):
        received_kwargs.update(kwargs)
        return []

    monkeypatch.setattr('web.components.joins_panel.get_fragment_joins', _capture_kwargs)
    monkeypatch.setattr('web.components.joins_panel.state',
                        type('S', (), {'meta_mgr': None})())

    import web.document_service as ds
    monkeypatch.setattr(ds, 'get_document_for_fragment', lambda sid: None)
    monkeypatch.setattr(ds, 'get_fragments_for_document', lambda pgpid: [])

    import web.fjms_service as fjms
    class _FakeSvc:
        def is_available(self): return False
        def get_join_group(self, sid): return []
    monkeypatch.setattr(fjms, 'get_fjms_service', lambda **kw: _FakeSvc())

    # RED: confirmed_only kwarg does not exist yet
    fetch_connected_fragments(
        document_id='99001', pgpid=None, confirmed_only=True, force_refresh=True
    )

    assert 'status' in received_kwargs, (
        f"get_fragment_joins must be called with status kwarg on confirmed_only=True path. "
        f"Got kwargs: {received_kwargs}"
    )
    assert received_kwargs['status'] == 'confirmed', (
        f"status must be exactly 'confirmed' (the canonical CHECK value). "
        f"Got: {received_kwargs['status']!r}"
    )


def test_default_path_passes_no_status_filter(monkeypatch):
    """confirmed_only=False (default) invokes get_fragment_joins WITHOUT a status filter.

    The browse dialog must continue to show all joins to the owner (proposed + confirmed),
    so the default path must NOT pass status='confirmed'.
    """
    _clear_cache()
    received_kwargs = {}

    def _capture_kwargs(**kwargs):
        received_kwargs.update(kwargs)
        return []

    monkeypatch.setattr('web.components.joins_panel.get_fragment_joins', _capture_kwargs)
    monkeypatch.setattr('web.components.joins_panel.state',
                        type('S', (), {'meta_mgr': None})())

    import web.document_service as ds
    monkeypatch.setattr(ds, 'get_document_for_fragment', lambda sid: None)
    monkeypatch.setattr(ds, 'get_fragments_for_document', lambda pgpid: [])

    import web.fjms_service as fjms
    class _FakeSvc:
        def is_available(self): return False
        def get_join_group(self, sid): return []
    monkeypatch.setattr(fjms, 'get_fjms_service', lambda **kw: _FakeSvc())

    fetch_connected_fragments(
        document_id='99001', pgpid=None, force_refresh=True  # default confirmed_only=False
    )

    # status must be absent or None/falsy on the default path
    status_val = received_kwargs.get('status')
    assert not status_val, (
        f"Default path must NOT pass a status filter. Got status={status_val!r}"
    )


def test_no_cross_user_poisoning(monkeypatch):
    """Cross-user cache poisoning prevention.

    Scenario:
      1. User A's fetch (default path) writes 'doc:99001:pgp:0' with unconfirmed rows.
      2. Lab path (confirmed_only=True) must NOT return that cached unconfirmed result.
         It uses a separate key 'doc:99001:pgp:0:confirmed' and applies status='confirmed'.

    This test seeds the unconfirmed cache key with a fake payload, then calls with
    confirmed_only=True and asserts the returned data is NOT the poisoned payload.
    """
    _clear_cache()

    # Seed the unconfirmed cache with a "poisoned" result (User A's unconfirmed join)
    poisoned_join = {
        'id': 99, 'fragment_a': 'T-S 12.1', 'fragment_b': 'T-S 12.99',
        'sources': ['user'], 'notes': 'POISONED — unconfirmed join',
    }
    poisoned_data = {
        'fragments': ['T-S 12.1', 'T-S 12.99'],
        'joins': [poisoned_join],
        'total_fragments': 2, 'total_joins': 1, 'fragment_details': [],
    }
    import time
    with joins_panel._joins_cache_lock:
        _joins_cache['doc:99001:pgp:0'] = (time.time(), poisoned_data)

    # Now mock get_fragment_joins to return empty (Lab confirmed path returns nothing new)
    _no_op_monkeypatches(monkeypatch)

    # RED: confirmed_only kwarg does not exist yet
    result = fetch_connected_fragments(
        document_id='99001', pgpid=0, confirmed_only=True, force_refresh=True
    )

    returned_joins = result.get('joins', [])
    poisoned_ids = [j for j in returned_joins if j.get('notes') == 'POISONED — unconfirmed join']
    assert not poisoned_ids, (
        f"Cross-user poisoning detected: the confirmed_only=True path returned data "
        f"that was cached by the default (unconfirmed) path. "
        f"The ':confirmed' cache key must prevent this. "
        f"Returned joins: {returned_joins}"
    )
