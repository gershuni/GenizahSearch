# -*- coding: utf-8 -*-
"""
Tests for corrections service.

Tests cover the get_pending_corrections_for_page function using mocks
to avoid real Supabase calls:
- None client handling
- None user_id handling
- Successful query with results
- Correct filter parameters
- Exception handling
- Empty results handling
"""

import pytest
from unittest.mock import MagicMock

from shared.corrections_service import get_pending_corrections_for_page


class TestGetPendingCorrectionsForPage:
    """Tests for get_pending_corrections_for_page function."""

    def test_returns_empty_when_no_client(self):
        """Should return empty list when client is None."""
        result = get_pending_corrections_for_page(
            client=None, sys_id='003072766', page_number=1, user_id='user-uuid-1'
        )
        assert result == []

    def test_returns_empty_when_no_user_id(self):
        """Should return empty list when user_id is None, without making any Supabase call."""
        mock_client = MagicMock()

        result = get_pending_corrections_for_page(
            client=mock_client, sys_id='003072766', page_number=1, user_id=None
        )

        assert result == []
        # Should NOT have called client.table at all
        mock_client.table.assert_not_called()

    def test_returns_pending_corrections_for_user(self):
        """Should return corrections with correct fields when query succeeds."""
        mock_client = MagicMock()

        sample_corrections = [
            {
                'id': 42,
                'corrected_text': 'Fixed text here',
                'status': 'pending',
                'created_at': '2026-02-11T10:00:00Z',
                'notes': 'Corrected typo',
                'original_text': 'Original text here',
            },
            {
                'id': 37,
                'corrected_text': 'Another fix',
                'status': 'draft',
                'created_at': '2026-02-10T08:30:00Z',
                'notes': '',
                'original_text': 'Another original',
            },
        ]

        # Build mock chain
        mock_response = MagicMock()
        mock_response.data = sample_corrections
        mock_client.table.return_value.select.return_value.eq.return_value.eq.return_value.eq.return_value.in_.return_value.order.return_value.execute.return_value = mock_response

        result = get_pending_corrections_for_page(
            client=mock_client, sys_id='003072766', page_number=1, user_id='user-uuid-1'
        )

        assert len(result) == 2
        assert result[0]['id'] == 42
        assert result[0]['corrected_text'] == 'Fixed text here'
        assert result[0]['status'] == 'pending'
        assert result[0]['created_at'] == '2026-02-11T10:00:00Z'
        assert result[0]['notes'] == 'Corrected typo'
        assert result[0]['original_text'] == 'Original text here'
        assert result[1]['id'] == 37
        assert result[1]['status'] == 'draft'

    def test_filters_by_sys_id_page_and_user(self):
        """Should pass correct filter parameters to Supabase query chain."""
        mock_client = MagicMock()

        mock_response = MagicMock()
        mock_response.data = []

        # Build the chain so we can inspect individual calls
        mock_table = MagicMock()
        mock_client.table.return_value = mock_table

        mock_select = MagicMock()
        mock_table.select.return_value = mock_select

        mock_eq1 = MagicMock()
        mock_select.eq.return_value = mock_eq1

        mock_eq2 = MagicMock()
        mock_eq1.eq.return_value = mock_eq2

        mock_eq3 = MagicMock()
        mock_eq2.eq.return_value = mock_eq3

        mock_in = MagicMock()
        mock_eq3.in_.return_value = mock_in

        mock_order = MagicMock()
        mock_in.order.return_value = mock_order

        mock_order.execute.return_value = mock_response

        result = get_pending_corrections_for_page(
            client=mock_client, sys_id='ABC123', page_number=2, user_id='user-uuid-1'
        )

        # Verify filter chain
        mock_client.table.assert_called_once_with('corrections')
        mock_table.select.assert_called_once_with(
            'id, corrected_text, status, created_at, notes, original_text'
        )
        mock_select.eq.assert_called_once_with('sys_id', 'ABC123')
        mock_eq1.eq.assert_called_once_with('page_number', 2)
        mock_eq2.eq.assert_called_once_with('author_id', 'user-uuid-1')
        mock_eq3.in_.assert_called_once_with('status', ['draft', 'pending', 'under_review'])
        mock_in.order.assert_called_once_with('created_at', desc=True)

        assert result == []

    def test_returns_empty_on_exception(self):
        """Should return empty list on exception, not propagate."""
        mock_client = MagicMock()
        mock_client.table.return_value.select.side_effect = Exception("Database connection error")

        result = get_pending_corrections_for_page(
            client=mock_client, sys_id='003072766', page_number=1, user_id='user-uuid-1'
        )

        assert result == []

    def test_returns_empty_when_no_corrections(self):
        """Should return empty list when query returns no data."""
        mock_client = MagicMock()

        mock_response = MagicMock()
        mock_response.data = []
        mock_client.table.return_value.select.return_value.eq.return_value.eq.return_value.eq.return_value.in_.return_value.order.return_value.execute.return_value = mock_response

        result = get_pending_corrections_for_page(
            client=mock_client, sys_id='003072766', page_number=1, user_id='user-uuid-1'
        )

        assert result == []
