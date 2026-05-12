"""Regression tests for SupabaseCorrectionsClient.

v7.11.0 hotfix coverage: the desktop CommentDialog has a "Type" dropdown with
values (general, question, scholarly_note, suggestion, issue). Before the fix
that string was passed straight through as the `comments.scope` column, but
that column is CHECK-constrained to ('page', 'manuscript', 'general'), so 4 of
5 dropdown choices raised a 23514 violation. This test asserts the client now
derives `scope` from `page_number` presence (matching the web client) instead
of from the dropdown value — i.e. dropdown choice never leaks into `scope`.

Persisting the category proper is a Phase 87 follow-up (add a `category` TEXT
CHECK column to the comments table).
"""
from __future__ import annotations

import os
import sys
from unittest.mock import MagicMock

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)


@pytest.fixture
def client_with_mocked_supabase():
    """Build a SupabaseCorrectionsClient with the Supabase client mocked so we
    can capture insert payloads without hitting the network."""
    from supabase_corrections_client import SupabaseCorrectionsClient

    cli = SupabaseCorrectionsClient.__new__(SupabaseCorrectionsClient)

    # Minimum state create_comment touches
    user = MagicMock()
    user._uuid = '00000000-0000-0000-0000-000000000001'
    cli.current_user = user

    # Replace _get_client + capture inserts
    captured = {}
    table_mock = MagicMock()
    insert_mock = MagicMock()
    execute_mock = MagicMock(return_value=MagicMock(data=[{
        'id': 1,
        'author_id': user._uuid,
        'sys_id': 'sid',
        'content': 'c',
        'scope': 'page',
        'page_number': 1,
        'is_public': True,
        'parent_id': None,
        'created_at': '2026-05-12T00:00:00Z',
        'profiles': {},
    }]))

    def _insert(payload):
        captured['payload'] = payload
        return MagicMock(execute=execute_mock)

    insert_mock.side_effect = _insert
    table_mock.insert = insert_mock

    client_mock = MagicMock()
    client_mock.table.return_value = table_mock
    cli._get_client = lambda: client_mock

    return cli, captured


_DROPDOWN_VALUES = ['general', 'question', 'scholarly_note', 'suggestion', 'issue']
_VALID_SCOPES = {'page', 'manuscript', 'general'}


@pytest.mark.parametrize("dropdown_value", _DROPDOWN_VALUES)
def test_create_comment_scope_is_valid_for_every_dropdown_choice(
    client_with_mocked_supabase, dropdown_value
):
    """Regression: every desktop CommentDialog dropdown value must produce a
    `scope` that passes the DB CHECK constraint."""
    cli, captured = client_with_mocked_supabase
    comment, msg = cli.create_comment(
        content='test',
        document_id='990000000000000001',
        comment_type=dropdown_value,
        page_number=3,
        is_public=True,
    )
    assert 'payload' in captured, "insert() was never called"
    scope = captured['payload'].get('scope')
    assert scope in _VALID_SCOPES, (
        f"dropdown={dropdown_value!r} produced scope={scope!r}, "
        f"which violates comments_scope_check (allowed: {_VALID_SCOPES})"
    )


@pytest.mark.parametrize("dropdown_value", _DROPDOWN_VALUES)
def test_create_comment_does_not_leak_category_into_scope(
    client_with_mocked_supabase, dropdown_value
):
    """Regression: the dropdown's category-flavored values (question /
    scholarly_note / suggestion / issue) must never be passed straight through
    to `scope`. They are categorically the wrong type of value for that column.
    """
    cli, captured = client_with_mocked_supabase
    cli.create_comment(
        content='test',
        document_id='990000000000000001',
        comment_type=dropdown_value,
        page_number=1,
    )
    scope = captured['payload'].get('scope')
    assert scope not in {'question', 'scholarly_note', 'suggestion', 'issue'}, (
        f"category value {scope!r} leaked into scope column for "
        f"dropdown={dropdown_value!r}"
    )


def test_create_comment_scope_is_page_when_page_number_present(
    client_with_mocked_supabase
):
    """With an explicit page_number, scope='page' (matches web behavior)."""
    cli, captured = client_with_mocked_supabase
    cli.create_comment(
        content='test',
        document_id='990000000000000001',
        comment_type='suggestion',  # would have failed pre-fix
        page_number=5,
    )
    assert captured['payload']['scope'] == 'page'


def test_create_comment_scope_is_manuscript_when_no_page_number(
    client_with_mocked_supabase
):
    """With no page_number, scope='manuscript' (matches web behavior)."""
    cli, captured = client_with_mocked_supabase
    cli.create_comment(
        content='test',
        document_id='990000000000000001',
        comment_type='question',
        page_number=None,
    )
    assert captured['payload']['scope'] == 'manuscript'
