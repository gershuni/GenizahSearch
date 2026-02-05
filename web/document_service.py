# -*- coding: utf-8 -*-
"""
Document Service for PGP document-fragment relationships.

This module provides functions for accessing PGP document data from Supabase:
- get_document_for_fragment(sys_id) -> dict | None
- get_fragments_for_document(pgpid) -> list[dict]
- get_transcription_for_document(pgpid) -> str | None
- get_document_metadata(pgpid) -> dict | None

All functions handle errors gracefully, returning None or empty lists
rather than raising exceptions.
"""

from typing import Optional, List, Dict, Any
from web.supabase_client import get_client


def get_document_for_fragment(sys_id: str) -> Optional[Dict[str, Any]]:
    """
    Get the PGP document associated with a fragment.

    Args:
        sys_id: The GenizahSearch system ID for the fragment

    Returns:
        Document dict with all fields (pgpid, shelfmark_combined, document_type,
        tags, doc_date_original, doc_date_standard, inferred_date_display,
        description, transcription, transcription_source, pgp_url),
        or None if not found or on error.
    """
    if not sys_id:
        return None

    try:
        client = get_client()

        # First, find the fragment link
        fragment_response = client.table('document_fragments').select(
            'document_id'
        ).eq('sys_id', sys_id).limit(1).execute()

        if not fragment_response.data:
            return None

        # Get the document_id (which is the pgpid)
        pgpid = fragment_response.data[0].get('document_id')
        if not pgpid:
            return None

        # Now get the full document
        doc_response = client.table('documents').select('*').eq(
            'pgpid', pgpid
        ).single().execute()

        return doc_response.data

    except Exception as e:
        print(f"Error getting document for fragment {sys_id}: {e}")
        return None


def get_fragments_for_document(pgpid: int) -> List[Dict[str, Any]]:
    """
    Get all fragments for a PGP document, ordered by sequence.

    Args:
        pgpid: The PGP document ID

    Returns:
        List of fragment dicts (id, document_id, sys_id, shelfmark,
        sequence_order, page_info), ordered by sequence_order ASC.
        Returns empty list if not found or on error.
    """
    if not pgpid:
        return []

    try:
        client = get_client()

        response = client.table('document_fragments').select('*').eq(
            'document_id', pgpid
        ).order('sequence_order', desc=False).execute()

        return response.data or []

    except Exception as e:
        print(f"Error getting fragments for document {pgpid}: {e}")
        return []


def get_transcription_for_document(pgpid: int) -> Optional[str]:
    """
    Get the transcription text for a PGP document.

    Args:
        pgpid: The PGP document ID

    Returns:
        Transcription string, or None if not found, empty, or on error.
    """
    if not pgpid:
        return None

    try:
        client = get_client()

        response = client.table('documents').select(
            'transcription'
        ).eq('pgpid', pgpid).single().execute()

        if response.data:
            transcription = response.data.get('transcription')
            # Return None for empty strings as well
            return transcription if transcription else None

        return None

    except Exception as e:
        print(f"Error getting transcription for document {pgpid}: {e}")
        return None


def get_document_metadata(pgpid: int) -> Optional[Dict[str, Any]]:
    """
    Get metadata for a PGP document.

    Args:
        pgpid: The PGP document ID

    Returns:
        Dict with document_type, tags, date fields, description, pgp_url,
        shelfmark_combined. Returns None if not found or on error.

    Note:
        Date columns are: doc_date_original, doc_date_standard, inferred_date_display
    """
    if not pgpid:
        return None

    try:
        client = get_client()

        response = client.table('documents').select(
            'document_type, tags, doc_date_original, doc_date_standard, '
            'inferred_date_display, description, pgp_url, shelfmark_combined'
        ).eq('pgpid', pgpid).single().execute()

        return response.data

    except Exception as e:
        print(f"Error getting metadata for document {pgpid}: {e}")
        return None
