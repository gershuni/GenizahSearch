# -*- coding: utf-8 -*-
"""
Corrections Service for user correction data.

This module provides functions for accessing correction data from Supabase,
shared between the web app and desktop app:
- get_pending_corrections_for_page(client, sys_id, page_number, user_id) -> list[dict]

All functions handle errors gracefully, returning empty lists
rather than raising exceptions.
"""

import logging
from typing import Optional, List, Dict, Any

logger = logging.getLogger(__name__)


def get_pending_corrections_for_page(
    client,
    sys_id: str,
    page_number: int,
    user_id: Optional[str] = None
) -> List[Dict[str, Any]]:
    """
    Get a user's own pending (unapproved) corrections for a specific manuscript page.

    Returns corrections with statuses 'draft', 'pending', or 'under_review'
    for the given sys_id, page_number, and user. Server-side RLS policy
    ("Users can view own corrections") provides security; the user_id parameter
    is a client-side convenience filter matching the authenticated session.

    Args:
        client: A Supabase Client instance (authenticated as the user).
                Web uses get_user_client(), desktop uses _get_client().
        sys_id: The manuscript system ID
        page_number: The page number (1=recto, 2=verso, etc.)
        user_id: The user's UUID. If None, returns empty list immediately.

    Returns:
        List of correction dicts, each containing:
        - id (int): correction ID
        - corrected_text (str): the correction text content
        - status (str): one of 'draft', 'pending', 'under_review'
        - created_at (str): ISO timestamp
        - notes (str): any notes the user added
        - original_text (str): what was being corrected
        Returns empty list if client is None, user_id is None, or on error.
    """
    if client is None or user_id is None:
        return []

    try:
        response = client.table('corrections').select(
            'id, corrected_text, status, created_at, notes, original_text'
        ).eq('sys_id', sys_id).eq(
            'page_number', page_number
        ).eq('author_id', user_id).in_(
            'status', ['draft', 'pending', 'under_review']
        ).order('created_at', desc=True).execute()

        return response.data or []

    except Exception as e:
        logger.error(f"Error getting pending corrections for {sys_id} page {page_number}: {e}")
        return []
