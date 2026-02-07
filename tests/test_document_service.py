# -*- coding: utf-8 -*-
"""
Tests for document service.

Tests cover all 4 service functions using mocks to avoid real Supabase calls:
- get_document_for_fragment
- get_fragments_for_document
- get_transcription_for_document
- get_document_metadata
"""

import pytest
from unittest.mock import patch, MagicMock


class TestGetDocumentForFragment:
    """Tests for get_document_for_fragment function."""

    @patch('shared.document_service.get_client')
    def test_get_document_for_fragment_found(self, mock_get_client):
        """Should return document when fragment is linked."""
        from web.document_service import get_document_for_fragment

        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        # Mock fragment query - returns document_id
        mock_fragment_chain = MagicMock()
        mock_fragment_chain.data = [{'document_id': 1234}]

        # Mock document query - returns full document
        mock_doc_chain = MagicMock()
        mock_doc_chain.data = {
            'pgpid': 1234,
            'shelfmark_combined': 'T-S 8J5.11',
            'document_type': 'Letter',
            'tags': ['letter', 'commercial'],
            'doc_date_original': '1050-1100 CE',
            'doc_date_standard': '1050/1100',
            'inferred_date_display': '11th century',
            'description': 'A commercial letter',
            'transcription': 'Test transcription',
            'transcription_source': 'PGP',
            'pgp_url': 'https://geniza.princeton.edu/documents/1234'
        }

        # Set up the mock chain for fragments
        mock_client.table.return_value.select.return_value.eq.return_value.limit.return_value.execute.return_value = mock_fragment_chain
        # Set up the mock chain for document (single)
        mock_client.table.return_value.select.return_value.eq.return_value.single.return_value.execute.return_value = mock_doc_chain

        result = get_document_for_fragment('003072766')

        assert result is not None
        assert result['pgpid'] == 1234
        assert result['shelfmark_combined'] == 'T-S 8J5.11'
        assert result['document_type'] == 'Letter'

    @patch('shared.document_service.get_client')
    def test_get_document_for_fragment_not_found(self, mock_get_client):
        """Should return None when fragment is not linked."""
        from web.document_service import get_document_for_fragment

        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        # Mock fragment query - returns empty
        mock_fragment_chain = MagicMock()
        mock_fragment_chain.data = []
        mock_client.table.return_value.select.return_value.eq.return_value.execute.return_value = mock_fragment_chain

        result = get_document_for_fragment('999999999')

        assert result is None

    def test_get_document_for_fragment_empty_id(self):
        """Should return None for empty sys_id."""
        from web.document_service import get_document_for_fragment

        assert get_document_for_fragment('') is None
        assert get_document_for_fragment(None) is None


class TestGetFragmentsForDocument:
    """Tests for get_fragments_for_document function."""

    @patch('shared.document_service.get_client')
    def test_get_fragments_for_document_found(self, mock_get_client):
        """Should return ordered list of fragments."""
        from web.document_service import get_fragments_for_document

        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        # Mock query returning 3 fragments in order
        mock_response = MagicMock()
        mock_response.data = [
            {'id': 1, 'document_id': 1234, 'sys_id': '003072766', 'shelfmark': 'T-S 8J5.11', 'sequence_order': 1, 'page_info': 'recto'},
            {'id': 2, 'document_id': 1234, 'sys_id': '003072767', 'shelfmark': 'T-S 8J5.12', 'sequence_order': 2, 'page_info': 'verso'},
            {'id': 3, 'document_id': 1234, 'sys_id': '003072768', 'shelfmark': 'T-S 8J5.13', 'sequence_order': 3, 'page_info': None},
        ]
        mock_client.table.return_value.select.return_value.eq.return_value.order.return_value.execute.return_value = mock_response

        result = get_fragments_for_document(1234)

        assert len(result) == 3
        assert result[0]['sequence_order'] == 1
        assert result[1]['sequence_order'] == 2
        assert result[2]['sequence_order'] == 3
        assert result[0]['shelfmark'] == 'T-S 8J5.11'

    @patch('shared.document_service.get_client')
    def test_get_fragments_for_document_empty(self, mock_get_client):
        """Should return empty list when no fragments found."""
        from web.document_service import get_fragments_for_document

        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        # Mock query returning empty
        mock_response = MagicMock()
        mock_response.data = []
        mock_client.table.return_value.select.return_value.eq.return_value.order.return_value.execute.return_value = mock_response

        result = get_fragments_for_document(9999)

        assert result == []  # Should be empty list, not None

    def test_get_fragments_for_document_empty_pgpid(self):
        """Should return empty list for empty pgpid."""
        from web.document_service import get_fragments_for_document

        assert get_fragments_for_document(0) == []
        assert get_fragments_for_document(None) == []


class TestGetTranscriptionForDocument:
    """Tests for get_transcription_for_document function."""

    @patch('shared.document_service.get_client')
    def test_get_transcription_found(self, mock_get_client):
        """Should return transcription string when found."""
        from web.document_service import get_transcription_for_document

        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        mock_response = MagicMock()
        mock_response.data = {'transcription': 'This is the transcription text.\nLine 2.'}
        mock_client.table.return_value.select.return_value.eq.return_value.single.return_value.execute.return_value = mock_response

        result = get_transcription_for_document(1234)

        assert result == 'This is the transcription text.\nLine 2.'

    @patch('shared.document_service.get_client')
    def test_get_transcription_not_found(self, mock_get_client):
        """Should return None when document not found."""
        from web.document_service import get_transcription_for_document

        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        mock_response = MagicMock()
        mock_response.data = None
        mock_client.table.return_value.select.return_value.eq.return_value.single.return_value.execute.return_value = mock_response

        result = get_transcription_for_document(9999)

        assert result is None

    @patch('shared.document_service.get_client')
    def test_get_transcription_empty_string(self, mock_get_client):
        """Should return None when transcription is empty string."""
        from web.document_service import get_transcription_for_document

        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        mock_response = MagicMock()
        mock_response.data = {'transcription': ''}
        mock_client.table.return_value.select.return_value.eq.return_value.single.return_value.execute.return_value = mock_response

        result = get_transcription_for_document(1234)

        assert result is None  # Empty string should return None

    def test_get_transcription_empty_pgpid(self):
        """Should return None for empty pgpid."""
        from web.document_service import get_transcription_for_document

        assert get_transcription_for_document(0) is None
        assert get_transcription_for_document(None) is None


class TestGetDocumentMetadata:
    """Tests for get_document_metadata function."""

    @patch('shared.document_service.get_client')
    def test_get_document_metadata_found(self, mock_get_client):
        """Should return metadata dict when document found."""
        from web.document_service import get_document_metadata

        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        mock_response = MagicMock()
        mock_response.data = {
            'document_type': 'Letter',
            'tags': ['letter', 'commercial', 'india trade'],
            'doc_date_original': '1050-1100 CE',
            'doc_date_standard': '1050/1100',
            'inferred_date_display': '11th century',
            'description': 'A commercial letter about the India trade',
            'pgp_url': 'https://geniza.princeton.edu/documents/1234',
            'shelfmark_combined': 'T-S 8J5.11 + T-S 8J5.12'
        }
        mock_client.table.return_value.select.return_value.eq.return_value.single.return_value.execute.return_value = mock_response

        result = get_document_metadata(1234)

        assert result is not None
        assert 'document_type' in result
        assert 'pgp_url' in result
        assert result['document_type'] == 'Letter'
        assert 'letter' in result['tags']

    @patch('shared.document_service.get_client')
    def test_get_document_metadata_not_found(self, mock_get_client):
        """Should return None when document not found."""
        from web.document_service import get_document_metadata

        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        mock_response = MagicMock()
        mock_response.data = None
        mock_client.table.return_value.select.return_value.eq.return_value.single.return_value.execute.return_value = mock_response

        result = get_document_metadata(9999)

        assert result is None

    def test_get_document_metadata_empty_pgpid(self):
        """Should return None for empty pgpid."""
        from web.document_service import get_document_metadata

        assert get_document_metadata(0) is None
        assert get_document_metadata(None) is None


class TestErrorHandling:
    """Tests for error handling across all functions."""

    @patch('shared.document_service.get_client')
    def test_get_document_for_fragment_error_handling(self, mock_get_client):
        """Should return None on exception, not propagate."""
        from web.document_service import get_document_for_fragment

        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        mock_client.table.return_value.select.side_effect = Exception("Database connection error")

        result = get_document_for_fragment('003072766')

        assert result is None  # Should not raise

    @patch('shared.document_service.get_client')
    def test_get_fragments_for_document_error_handling(self, mock_get_client):
        """Should return empty list on exception, not propagate."""
        from web.document_service import get_fragments_for_document

        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        mock_client.table.return_value.select.side_effect = Exception("Database connection error")

        result = get_fragments_for_document(1234)

        assert result == []  # Should not raise, return empty list

    @patch('shared.document_service.get_client')
    def test_get_transcription_error_handling(self, mock_get_client):
        """Should return None on exception, not propagate."""
        from web.document_service import get_transcription_for_document

        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        mock_client.table.return_value.select.side_effect = Exception("Database connection error")

        result = get_transcription_for_document(1234)

        assert result is None  # Should not raise

    @patch('shared.document_service.get_client')
    def test_get_document_metadata_error_handling(self, mock_get_client):
        """Should return None on exception, not propagate."""
        from web.document_service import get_document_metadata

        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        mock_client.table.return_value.select.side_effect = Exception("Database connection error")

        result = get_document_metadata(1234)

        assert result is None  # Should not raise
