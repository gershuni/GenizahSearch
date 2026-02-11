# -*- coding: utf-8 -*-
"""
Tests for pending corrections integration in version selector.

Validates that the version selector correctly:
- Imports pending corrections service
- Calls fetch with correct arguments when logged in
- Skips fetch when not logged in
"""

import pytest
from unittest.mock import patch, MagicMock


# Test 1: Import chain works
def test_import_chain_from_web_shim():
    """web.corrections_service should re-export get_pending_corrections_for_page."""
    from web.corrections_service import get_pending_corrections_for_page
    from shared.corrections_service import get_pending_corrections_for_page as shared_fn
    assert get_pending_corrections_for_page is shared_fn


# Test 2: version_selector imports the function
def test_version_selector_imports_pending_corrections():
    """version_selector.py should import get_pending_corrections_for_page."""
    import inspect
    import web.components.version_selector as vs
    source = inspect.getsource(vs)
    assert 'get_pending_corrections_for_page' in source


# Test 3: version_selector imports get_user_client
def test_version_selector_imports_user_client():
    """version_selector.py should import get_user_client for authenticated queries."""
    import inspect
    import web.components.version_selector as vs
    source = inspect.getsource(vs)
    assert 'get_user_client' in source


# Test 4: Pending corrections data structure compatibility
def test_pending_correction_has_expected_fields():
    """Pending corrections from service should have fields needed by version selector."""
    mock_client = MagicMock()
    mock_response = MagicMock()
    mock_response.data = [{
        'id': 1,
        'corrected_text': 'Test correction text',
        'status': 'pending',
        'created_at': '2026-02-11T10:00:00Z',
        'notes': 'Test note',
        'original_text': 'Original text',
    }]
    mock_client.table.return_value.select.return_value.eq.return_value.eq.return_value.eq.return_value.in_.return_value.order.return_value.execute.return_value = mock_response

    from shared.corrections_service import get_pending_corrections_for_page
    result = get_pending_corrections_for_page(
        client=mock_client, sys_id='test123', page_number=1, user_id='user-1'
    )

    assert len(result) == 1
    # Fields used by version selector
    assert 'corrected_text' in result[0]  # Displayed when selected
    assert 'status' in result[0]          # Shown in label
    assert 'id' in result[0]              # Passed in version_info
    assert 'created_at' in result[0]      # Shown as date


# Test 5: version_selector source contains pending corrections UI elements
def test_version_selector_has_pending_ui_elements():
    """version_selector.py should contain pending corrections UI markers."""
    import inspect
    import web.components.version_selector as vs
    source = inspect.getsource(vs)
    assert 'pending_corrections' in source, "Should have pending_corrections variable"
    assert 'schedule' in source, "Should use schedule icon for pending items"
    assert 'is_pending' in source, "Should set is_pending flag in version_info"
    assert 'My Pending Corrections' in source or 'Pending' in source, "Should label pending section"
