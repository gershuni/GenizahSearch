# -*- coding: utf-8 -*-
"""ANC-04 — known-joins source attribution, dedup, and community merge tests.

These tests monkeypatch the I/O dependencies of fetch_connected_fragments so
they run headless (no live Supabase / SQLite). All calls use force_refresh=True
to bypass the module-level _joins_cache.

Source attribution rules (from joins_panel.py):
  - User joins (from fragment_joins table) → sources=['user']
  - PGP joins (from document_fragments via pgpid) → sources=['PGP']
  - FJMS joins (from fjms_service.get_join_group) → sources=['FJMS']
  - Community puzzle joins (published_join_fragments + published_joins) → sources=['community']
    (currently only in create_joins_dialog; Plan 02 adds it to the Lab/confirmed_only path)

Test layout:
  - GREEN NOW: test_user_join_has_user_source, test_fragment_details_populated,
    test_empty_returns_zero_joins, test_multi_source_dedup_merges_sources
    (exercise existing fetch_connected_fragments behavior)
  - RED (Plan 02): test_community_member_appears_in_lab_group
    (confirmed_only=True kwarg + community merge don't exist yet)
"""

import pytest
from web.components import joins_panel
from web.components.joins_panel import fetch_connected_fragments, _joins_cache


def _clear_cache():
    """Clear the joins panel module-level cache before each test."""
    with joins_panel._joins_cache_lock:
        _joins_cache.clear()


# ---------------------------------------------------------------------------
# Helper factories for fake join rows
# ---------------------------------------------------------------------------


def _user_join_row(frag_a='T-S 12.1', frag_b='T-S 12.2',
                   sys_id_a='99001', sys_id_b='99002'):
    """Minimal fragment_joins row as returned by get_fragment_joins."""
    return {
        'id': 1,
        'fragment_a_shelfmark': frag_a,
        'fragment_b_shelfmark': frag_b,
        'fragment_a_sys_id': sys_id_a,
        'fragment_b_sys_id': sys_id_b,
        'join_type': 'direct_join',
        'notes': '',
        'created_by_username': 'testuser',
        'created_at': '2026-01-01T00:00:00Z',
        'status': 'proposed',
    }


# ---------------------------------------------------------------------------
# GREEN NOW: source attribution + basic structure tests
# ---------------------------------------------------------------------------


def test_user_join_has_user_source(monkeypatch):
    """A single user join row → formatted join with sources==['user']."""
    _clear_cache()
    join_row = _user_join_row()

    monkeypatch.setattr('web.components.joins_panel.get_fragment_joins',
                        lambda **kwargs: [join_row])
    monkeypatch.setattr('web.components.joins_panel.state',
                        type('S', (), {'meta_mgr': None})())

    # Patch document_service to skip PGP merge
    import web.document_service as ds
    monkeypatch.setattr(ds, 'get_document_for_fragment', lambda sid: None)
    monkeypatch.setattr(ds, 'get_fragments_for_document', lambda pgpid: [])

    # Patch fjms_service to skip FJMS merge
    import web.fjms_service as fjms
    class _FakeSvc:
        def is_available(self): return False
        def get_join_group(self, sid): return []
    monkeypatch.setattr(fjms, 'get_fjms_service', lambda **kw: _FakeSvc())

    result = fetch_connected_fragments(
        document_id='99001', pgpid=None, force_refresh=True
    )
    joins = result.get('joins', [])
    assert len(joins) == 1
    assert joins[0]['sources'] == ['user'], f"Expected ['user'], got {joins[0]['sources']}"


def test_fragment_details_populated(monkeypatch):
    """Returned dict has fragment_details list with dict entries containing 'shelfmark' and 'document_id'."""
    _clear_cache()
    join_row = _user_join_row()

    monkeypatch.setattr('web.components.joins_panel.get_fragment_joins',
                        lambda **kwargs: [join_row])
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

    result = fetch_connected_fragments(
        document_id='99001', pgpid=None, force_refresh=True
    )
    details = result.get('fragment_details', [])
    assert isinstance(details, list), "fragment_details should be a list"
    assert len(details) > 0, "fragment_details should be non-empty when joins exist"
    for d in details:
        assert 'shelfmark' in d, f"Missing 'shelfmark' in fragment_detail: {d}"
        assert 'document_id' in d, f"Missing 'document_id' in fragment_detail: {d}"


def test_empty_returns_zero_joins(monkeypatch):
    """No joins from any source → total_joins==0 and joins==[]."""
    _clear_cache()
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

    result = fetch_connected_fragments(
        document_id='99001', pgpid=None, force_refresh=True
    )
    assert result['total_joins'] == 0
    assert result['joins'] == []


def test_multi_source_dedup_merges_sources(monkeypatch):
    """A fragment present in both user join and FJMS join ends with sources=['user', 'FJMS'].

    This exercises the dedup-merge branch at joins_panel.py:200-216 where an existing
    join from user joins is augmented with the FJMS source when the same fragment appears.
    """
    _clear_cache()
    # User join: T-S 12.1 ↔ T-S 12.2
    join_row = _user_join_row(frag_a='T-S 12.1', frag_b='T-S 12.2',
                              sys_id_a='99001', sys_id_b='99002')

    monkeypatch.setattr('web.components.joins_panel.get_fragment_joins',
                        lambda **kwargs: [join_row])

    # FJMS says 99003 (alma_id of T-S 12.2) is also in the group for 99001
    # We need FJMS to report T-S 12.2 as a member → triggers merge branch
    class _FakeMeta:
        def get_meta_for_id(self, alma_id):
            return ('T-S 12.2', '')  # same shelfmark as the user join

    monkeypatch.setattr('web.components.joins_panel.state',
                        type('S', (), {'meta_mgr': _FakeMeta()})())

    import web.document_service as ds
    monkeypatch.setattr(ds, 'get_document_for_fragment', lambda sid: None)
    monkeypatch.setattr(ds, 'get_fragments_for_document', lambda pgpid: [])

    import web.fjms_service as fjms
    class _FakeJoinSvc:
        def is_available(self): return True
        def get_join_group(self, sid):
            return [{'alma_id': '99003', 'scholar_names': [], 'join_types': ['direct_join'],
                     'join_group_ids': [42]}]
    monkeypatch.setattr(fjms, 'get_fjms_service', lambda **kw: _FakeJoinSvc())

    result = fetch_connected_fragments(
        shelfmark='T-S 12.1', document_id='99001', pgpid=None, force_refresh=True
    )
    joins = result.get('joins', [])
    assert joins, "Expected at least one join"
    # Find the join for T-S 12.2
    target = [j for j in joins if j.get('fragment_b', '').upper() == 'T-S 12.2']
    assert target, f"Expected join with fragment_b='T-S 12.2'; got: {joins}"
    sources = target[0].get('sources', [])
    assert 'user' in sources, f"Expected 'user' in sources: {sources}"
    assert 'FJMS' in sources, f"Expected 'FJMS' in sources after merge: {sources}"


# ---------------------------------------------------------------------------
# RED (Plan 02): community-source test
# ---------------------------------------------------------------------------


def test_community_member_appears_in_lab_group(monkeypatch):
    """ANC-04 community source — RED until Plan 02 wires the Lab community merge.

    Calling fetch_connected_fragments with confirmed_only=True (Lab path) and
    asserting the result includes a join with sources=['community'] from published
    puzzle joins. This kwarg does NOT exist yet → RED.

    Plan 02 adds:
      - confirmed_only: bool=False param to fetch_connected_fragments
      - When confirmed_only=True: merge published community puzzle joins as sources=['community']
      - Uses a separate ':confirmed' cache key
    """
    _clear_cache()
    # No user joins
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

    # Mock the Supabase client used by the community fetch path
    # published_join_fragments returns one row for document_id='99001'
    # published_joins returns one published join with a different member
    class _FakeTable:
        def __init__(self, table_name):
            self._table = table_name
            self._filters = {}
        def select(self, *a): return self
        def eq(self, col, val):
            self._filters[col] = val
            return self
        def in_(self, col, vals): return self
        def execute(self):
            if self._table == 'published_join_fragments':
                return type('R', (), {'data': [{'join_id': 1, 'shelfmark': 'T-S 10.1'}]})()
            if self._table == 'published_joins':
                return type('R', (), {'data': [{
                    'id': 1, 'user_id': 'u1', 'title': 'Test Join',
                    'shelfmarks': ['T-S 10.1', 'T-S 10.2'],
                    'thumbnail_path': None, 'created_at': '2026-01-01T00:00:00Z',
                    'is_published': True,
                }]})()
            return type('R', (), {'data': []})()

    class _FakeClient:
        def table(self, name):
            return _FakeTable(name)

    monkeypatch.setattr('web.components.joins_panel.get_client', lambda: _FakeClient())

    # This call FAILS (RED) because confirmed_only kwarg does not exist yet
    result = fetch_connected_fragments(
        document_id='99001', pgpid=None, confirmed_only=True, force_refresh=True
    )

    joins = result.get('joins', [])
    community_joins = [j for j in joins if 'community' in j.get('sources', [])]
    assert community_joins, (
        "Expected at least one join with sources=['community'] on the confirmed_only=True "
        "Lab path. Plan 02 wires the community merge into fetch_connected_fragments."
    )
