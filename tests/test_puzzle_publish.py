# -*- coding: utf-8 -*-
"""
Tests for shared/puzzle_publish_service.py

All Supabase interactions are mocked. Tests verify correct API call
sequences, data transformations, and error handling.
"""

import json
import io
import uuid
from unittest.mock import MagicMock, patch, call
import pytest

from shared.puzzle_model import PuzzleDocument, PuzzleFragment


# ── Fixtures ─────────────────────────────────────────────────────────

def _make_doc(n_frags=2, title='Test Join', doc_id=None):
    """Create a PuzzleDocument with n_frags fragments."""
    frags = []
    for i in range(n_frags):
        frags.append(PuzzleFragment(
            sys_id=f'sys_{i}',
            folio_label=f'{i+1}r',
            fl_id=f'FL_{i}',
            shelfmark=f'T-S {i+1}.1' if i < n_frags else '',
            x=float(i * 100),
            y=0.0,
        ))
    return PuzzleDocument(
        id=doc_id or str(uuid.uuid4()),
        title=title,
        notes='Test notes',
        join_type='physical',
        fragments=frags,
    )


def _mock_client():
    """Create a mock Supabase client with chainable query builder."""
    client = MagicMock()

    # Storage mock
    bucket = MagicMock()
    client.storage.from_.return_value = bucket
    bucket.upload.return_value = None
    bucket.remove.return_value = None
    bucket.get_public_url.side_effect = lambda path: f'https://storage.test/{path}'

    # Table mock with chainable API
    def make_table_mock(data=None):
        tbl = MagicMock()
        response = MagicMock()
        response.data = data or []
        # Make all chainable methods return the same mock
        for method in ['select', 'insert', 'upsert', 'update', 'delete',
                       'eq', 'in_', 'order', 'limit', 'range']:
            getattr(tbl, method).return_value = tbl
        tbl.execute.return_value = response
        return tbl

    # Default table mock
    client._table_mocks = {}

    def table_factory(name):
        if name not in client._table_mocks:
            client._table_mocks[name] = make_table_mock()
        return client._table_mocks[name]

    client.table.side_effect = table_factory
    return client


def _make_image_service():
    """Create a mock image service that returns a tiny valid PNG."""
    from PIL import Image
    img = Image.new('RGBA', (100, 100), (200, 200, 200, 255))
    buf = io.BytesIO()
    img.save(buf, format='PNG')
    png_bytes = buf.getvalue()

    svc = MagicMock()
    svc.resolve_fragment_image.return_value = png_bytes
    return svc


# ── Test 1: publish_join uploads storage + upserts table ─────────

def test_publish_join_uploads_and_upserts():
    """publish_join() calls storage.upload twice (PNG + thumbnail) and table.upsert once."""
    from shared.puzzle_publish_service import publish_join

    client = _mock_client()
    doc = _make_doc()
    user_id = 'user-123'
    image_svc = _make_image_service()

    result = publish_join(client, user_id, doc, image_svc)

    # Should return doc.id
    assert result == doc.id

    # Storage: two uploads (PNG + thumbnail)
    bucket = client.storage.from_.return_value
    assert bucket.upload.call_count == 2
    upload_calls = bucket.upload.call_args_list
    paths = [c[0][0] for c in upload_calls]
    assert f'{user_id}/{doc.id}.png' in paths
    assert f'{user_id}/{doc.id}_thumb.png' in paths

    # Table: upsert to published_joins
    published_joins_tbl = client._table_mocks.get('published_joins')
    assert published_joins_tbl is not None
    assert published_joins_tbl.upsert.call_count >= 1


# ── Test 2: publish_join extracts shelfmarks ─────────────────────

def test_publish_join_extracts_shelfmarks():
    """publish_join() extracts unique shelfmarks from fragments."""
    from shared.puzzle_publish_service import publish_join

    client = _mock_client()
    doc = _make_doc(n_frags=3)
    # Make two fragments share a shelfmark
    doc.fragments[0].shelfmark = 'T-S 1.1'
    doc.fragments[1].shelfmark = 'T-S 1.1'  # duplicate
    doc.fragments[2].shelfmark = 'T-S 2.2'

    result = publish_join(client, 'user-1', doc, _make_image_service())

    # Check the upsert data includes deduplicated shelfmarks
    published_joins_tbl = client._table_mocks['published_joins']
    upsert_call = published_joins_tbl.upsert.call_args
    upsert_data = upsert_call[0][0]
    assert upsert_data['shelfmarks'] == ['T-S 1.1', 'T-S 2.2']


# ── Test 3: publish_join populates fragment index ────────────────

def test_publish_join_populates_fragments():
    """publish_join() populates published_join_fragments with sys_id/shelfmark pairs."""
    from shared.puzzle_publish_service import publish_join

    client = _mock_client()
    doc = _make_doc(n_frags=2)

    publish_join(client, 'user-1', doc, _make_image_service())

    # Should delete old fragments then insert new ones
    frags_tbl = client._table_mocks['published_join_fragments']
    # Delete was called (eq with join_id, then delete, then execute)
    assert frags_tbl.delete.call_count >= 1
    # Insert was called with fragment data
    assert frags_tbl.insert.call_count >= 1
    insert_data = frags_tbl.insert.call_args[0][0]
    assert len(insert_data) == 2
    assert insert_data[0]['sys_id'] == 'sys_0'
    assert insert_data[1]['sys_id'] == 'sys_1'


# ── Test 4: unpublish_join ───────────────────────────────────────

def test_unpublish_join():
    """unpublish_join() sets is_published=False and removes storage files."""
    from shared.puzzle_publish_service import unpublish_join

    client = _mock_client()
    user_id = 'user-1'
    join_id = 'join-abc'

    unpublish_join(client, user_id, join_id)

    # Table update
    pj_tbl = client._table_mocks['published_joins']
    pj_tbl.update.assert_called_once()
    update_data = pj_tbl.update.call_args[0][0]
    assert update_data['is_published'] is False

    # Storage removal
    bucket = client.storage.from_.return_value
    bucket.remove.assert_called_once()
    removed_paths = bucket.remove.call_args[0][0]
    assert f'{user_id}/{join_id}.png' in removed_paths
    assert f'{user_id}/{join_id}_thumb.png' in removed_paths


# ── Test 5: list_published_joins ─────────────────────────────────

def test_list_published_joins():
    """list_published_joins() returns list with thumbnail URLs."""
    from shared.puzzle_publish_service import list_published_joins

    client = _mock_client()
    # Set up the published_joins table to return data
    pj_tbl = client._table_mocks['published_joins'] = MagicMock()
    response = MagicMock()
    response.data = [
        {
            'id': 'j1', 'title': 'Join 1', 'notes': '', 'shelfmarks': ['T-S 1.1'],
            'thumbnail_path': 'user-1/j1_thumb.png', 'user_id': 'user-1',
            'created_at': '2026-01-01T00:00:00Z',
        }
    ]
    for method in ['select', 'eq', 'order', 'limit', 'range']:
        getattr(pj_tbl, method).return_value = pj_tbl
    pj_tbl.execute.return_value = response

    # Mock profiles table for resolve_author_profiles
    profiles_tbl = MagicMock()
    profiles_response = MagicMock()
    profiles_response.data = [{'id': 'user-1', 'full_name': 'Dr. Test'}]
    for method in ['select', 'in_']:
        getattr(profiles_tbl, method).return_value = profiles_tbl
    profiles_tbl.execute.return_value = profiles_response
    client._table_mocks['profiles'] = profiles_tbl
    # Override table side_effect to use our mocks
    client.table.side_effect = lambda name: client._table_mocks.get(name, MagicMock())

    result = list_published_joins(client, limit=10, offset=0)

    assert len(result) == 1
    assert result[0]['id'] == 'j1'
    assert 'thumbnail_url' in result[0]
    assert result[0]['author_name'] == 'Dr. Test'


# ── Test 6: get_published_joins_for_fragment uses batch query ────

def test_get_published_joins_for_fragment_batch():
    """get_published_joins_for_fragment(sys_id) returns matching joins via batch query."""
    from shared.puzzle_publish_service import get_published_joins_for_fragment

    client = _mock_client()

    # Fragment index returns two join_ids
    frags_tbl = MagicMock()
    frags_response = MagicMock()
    frags_response.data = [
        {'join_id': 'j1', 'sys_id': 'sys_0', 'shelfmark': 'T-S 1.1'},
        {'join_id': 'j2', 'sys_id': 'sys_0', 'shelfmark': 'T-S 1.1'},
    ]
    for method in ['select', 'eq']:
        getattr(frags_tbl, method).return_value = frags_tbl
    frags_tbl.execute.return_value = frags_response
    client._table_mocks['published_join_fragments'] = frags_tbl

    # Published joins batch query
    pj_tbl = MagicMock()
    pj_response = MagicMock()
    pj_response.data = [
        {'id': 'j1', 'title': 'Join 1', 'shelfmarks': ['T-S 1.1'],
         'thumbnail_path': 'u/j1_thumb.png', 'user_id': 'u1', 'created_at': '2026-01-01'},
        {'id': 'j2', 'title': 'Join 2', 'shelfmarks': ['T-S 1.1'],
         'thumbnail_path': 'u/j2_thumb.png', 'user_id': 'u1', 'created_at': '2026-01-02'},
    ]
    for method in ['select', 'in_', 'eq']:
        getattr(pj_tbl, method).return_value = pj_tbl
    pj_tbl.execute.return_value = pj_response
    client._table_mocks['published_joins'] = pj_tbl

    # Profiles
    profiles_tbl = MagicMock()
    profiles_resp = MagicMock()
    profiles_resp.data = [{'id': 'u1', 'full_name': 'Author 1'}]
    for method in ['select', 'in_']:
        getattr(profiles_tbl, method).return_value = profiles_tbl
    profiles_tbl.execute.return_value = profiles_resp
    client._table_mocks['profiles'] = profiles_tbl
    client.table.side_effect = lambda name: client._table_mocks.get(name, MagicMock())

    result = get_published_joins_for_fragment(client, 'sys_0')

    assert len(result) == 2
    # Verify batch query was used: in_() should have been called with both join_ids
    pj_tbl.in_.assert_called_once()
    in_args = pj_tbl.in_.call_args[0]
    assert in_args[0] == 'id'
    assert set(in_args[1]) == {'j1', 'j2'}


# ── Test 7: get_published_join_detail ────────────────────────────

def test_get_published_join_detail():
    """get_published_join_detail(join_id) returns full arrangement JSON."""
    from shared.puzzle_publish_service import get_published_join_detail

    client = _mock_client()
    pj_tbl = MagicMock()
    pj_response = MagicMock()
    pj_response.data = [{
        'id': 'j1', 'title': 'Detail Join', 'notes': 'Notes here',
        'shelfmarks': ['T-S 1.1'], 'fragments_json': {'fragments': []},
        'image_path': 'u/j1.png', 'thumbnail_path': 'u/j1_thumb.png',
        'user_id': 'u1', 'created_at': '2026-01-01',
    }]
    for method in ['select', 'eq']:
        getattr(pj_tbl, method).return_value = pj_tbl
    pj_tbl.execute.return_value = pj_response
    client._table_mocks['published_joins'] = pj_tbl

    # Profiles
    profiles_tbl = MagicMock()
    profiles_resp = MagicMock()
    profiles_resp.data = [{'id': 'u1', 'full_name': 'Author Detail'}]
    for method in ['select', 'in_']:
        getattr(profiles_tbl, method).return_value = profiles_tbl
    profiles_tbl.execute.return_value = profiles_resp
    client._table_mocks['profiles'] = profiles_tbl
    client.table.side_effect = lambda name: client._table_mocks.get(name, MagicMock())

    result = get_published_join_detail(client, 'j1')

    assert result is not None
    assert result['title'] == 'Detail Join'
    assert result['author_name'] == 'Author Detail'
    assert 'image_url' in result
    assert 'fragments_json' in result


# ── Test 8: fork_published_join ──────────────────────────────────

def test_fork_published_join():
    """fork_published_join() creates local PuzzleDocument with 'Fork of:' title prefix."""
    from shared.puzzle_publish_service import fork_published_join

    client = _mock_client()
    # Set up detail data with fragments_json that includes the full doc
    doc = _make_doc(title='Original Join')
    fragments_data = json.loads(doc.to_json())

    pj_tbl = MagicMock()
    pj_response = MagicMock()
    pj_response.data = [{
        'id': 'j1', 'title': 'Original Join', 'notes': 'Some notes',
        'shelfmarks': ['T-S 1.1'], 'fragments_json': fragments_data,
        'image_path': 'u/j1.png', 'thumbnail_path': 'u/j1_thumb.png',
        'user_id': 'u1', 'created_at': '2026-01-01',
    }]
    for method in ['select', 'eq']:
        getattr(pj_tbl, method).return_value = pj_tbl
    pj_tbl.execute.return_value = pj_response
    client._table_mocks['published_joins'] = pj_tbl

    profiles_tbl = MagicMock()
    profiles_resp = MagicMock()
    profiles_resp.data = [{'id': 'u1', 'full_name': 'Auth'}]
    for method in ['select', 'in_']:
        getattr(profiles_tbl, method).return_value = profiles_tbl
    profiles_tbl.execute.return_value = profiles_resp
    client._table_mocks['profiles'] = profiles_tbl
    client.table.side_effect = lambda name: client._table_mocks.get(name, MagicMock())

    # Mock puzzle_service
    puzzle_service = MagicMock()
    puzzle_service.save_document.return_value = 'new-doc-id'

    result = fork_published_join(client, 'j1', puzzle_service)

    assert result is not None
    # save_document should have been called with a PuzzleDocument
    puzzle_service.save_document.assert_called_once()
    saved_doc = puzzle_service.save_document.call_args[0][0]
    assert saved_doc.title.startswith('Fork of:')
    assert 'Original Join' in saved_doc.title
    # The forked doc should have a NEW id (not the original)
    assert saved_doc.id != 'j1'


# ── Test 9: resolve_author_profiles ──────────────────────────────

def test_resolve_author_profiles():
    """resolve_author_profiles() batch-fetches display names from profiles table."""
    from shared.puzzle_publish_service import resolve_author_profiles

    client = _mock_client()
    profiles_tbl = MagicMock()
    profiles_resp = MagicMock()
    profiles_resp.data = [
        {'id': 'u1', 'full_name': 'Alice'},
        {'id': 'u2', 'full_name': 'Bob'},
    ]
    for method in ['select', 'in_']:
        getattr(profiles_tbl, method).return_value = profiles_tbl
    profiles_tbl.execute.return_value = profiles_resp
    client._table_mocks['profiles'] = profiles_tbl
    client.table.side_effect = lambda name: client._table_mocks.get(name, MagicMock())

    result = resolve_author_profiles(client, ['u1', 'u2', 'u3'])

    assert result['u1'] == 'Alice'
    assert result['u2'] == 'Bob'
    assert result['u3'] == 'Anonymous'  # not found = Anonymous
