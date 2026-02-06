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

import re
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
        description, transcription, transcription_source, pgp_url, doc_relation),
        or None if not found or on error.

    Note:
        The doc_relation field indicates the source type ('Digital Edition' for
        transcriptions, 'Digital Translation' for translations). The transcription
        field always contains the imported content regardless of doc_relation.
        Future: translations could be offered as a separate version option.
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

        if doc_response.data:
            return doc_response.data

        return None

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


def parse_transcription_sections(transcription: str) -> dict:
    """
    Parse PGP transcription text into sections by recto/verso markers.

    Args:
        transcription: Full transcription text with section markers

    Returns:
        Dict with 'recto' and 'verso' lists, each containing section text.
        Sections include variations like "Recto - right margin".
        If no markers found, returns {'recto': [transcription], 'verso': []}.
    """
    if not transcription:
        return {'recto': [], 'verso': []}

    # Pattern matches section headers at start of line
    # Handles: Recto, Verso, Recto - right margin, Verso, address, etc.
    # The marker itself is on its own line or followed by colon/newline
    section_pattern = re.compile(
        r'^(Recto|Verso)(?:\s*[-,]\s*[^\n]+)?[:\s]*\n',
        re.MULTILINE | re.IGNORECASE
    )

    # Find all section markers with their positions
    markers = list(section_pattern.finditer(transcription))

    if not markers:
        # No markers found - treat entire text as recto
        return {'recto': [transcription.strip()], 'verso': []}

    sections = {'recto': [], 'verso': []}

    for i, match in enumerate(markers):
        # Determine section type from marker text
        marker_text = match.group(0).lower()
        section_type = 'recto' if marker_text.startswith('recto') else 'verso'

        # Get text from after this marker to before next marker (or end)
        start = match.end()
        end = markers[i + 1].start() if i + 1 < len(markers) else len(transcription)
        section_text = transcription[start:end].strip()

        if section_text:
            sections[section_type].append(section_text)

    # Handle text before first marker (if any)
    if markers and markers[0].start() > 0:
        preamble = transcription[:markers[0].start()].strip()
        if preamble:
            # Preamble goes to recto by default
            sections['recto'].insert(0, preamble)

    return sections


def get_section_for_page(transcription: str, page_num: int) -> Optional[str]:
    """
    Get the appropriate transcription section for a page number.

    Args:
        transcription: Full transcription text
        page_num: Page number (1 = recto, 2 = verso for single-fragment docs)

    Returns:
        Section text for the page, None if no content exists for this page,
        or full transcription if no recto/verso markers found at all.
    """
    sections = parse_transcription_sections(transcription)

    # Check if the transcription has any recto/verso markers
    has_recto = bool(sections.get('recto'))
    has_verso = bool(sections.get('verso'))

    # Map page number to section type
    # For single-fragment: page 1 = recto, page 2 = verso
    if page_num == 1:
        section_list = sections.get('recto', [])
        # If document has verso-only content, return None for recto page
        if not section_list and has_verso:
            return None
    elif page_num == 2:
        section_list = sections.get('verso', [])
        # If document has recto-only content, return None for verso page
        if not section_list and has_recto:
            return None
    else:
        # For pages beyond 2, return full content (multi-fragment case)
        # TODO: Enhance for multi-fragment documents in future
        return transcription

    if section_list:
        return '\n\n'.join(section_list)

    # No markers found at all - return full transcription
    # (handles documents without recto/verso structure)
    return transcription


def get_sources_for_document(pgpid: int) -> List[Dict[str, Any]]:
    """
    Get all sources (editions and translations) for a PGP document.

    Args:
        pgpid: The PGP document ID

    Returns:
        List of source dicts ordered by: doc_relation (Edition first, then Translation),
        then sequence_order. Returns empty list if not found or on error.

    Note:
        Each source dict contains: id, pgpid, source_scholar, doc_relation,
        content, language, content_length, sequence_order, created_at.
    """
    if not pgpid:
        return []

    try:
        client = get_client()

        # Order by doc_relation (Editions first alphabetically before Translations)
        # then by sequence_order
        response = client.table('document_sources').select('*').eq(
            'pgpid', pgpid
        ).order('doc_relation', desc=False).order('sequence_order', desc=False).execute()

        return response.data or []

    except Exception as e:
        print(f"Error getting sources for document {pgpid}: {e}")
        return []


def get_editions_for_document(pgpid: int) -> List[Dict[str, Any]]:
    """
    Get Digital Editions for a PGP document.

    Args:
        pgpid: The PGP document ID

    Returns:
        List of edition source dicts (doc_relation contains 'Edition').
        Useful for transcription selector with multiple scholars.
    """
    if not pgpid:
        return []

    try:
        client = get_client()

        # Filter to doc_relation containing 'Edition' (Digital Edition, Edition)
        response = client.table('document_sources').select('*').eq(
            'pgpid', pgpid
        ).like('doc_relation', '%Edition%').order('sequence_order', desc=False).execute()

        return response.data or []

    except Exception as e:
        print(f"Error getting editions for document {pgpid}: {e}")
        return []


def get_translations_for_document(pgpid: int) -> List[Dict[str, Any]]:
    """
    Get Digital Translations for a PGP document.

    Args:
        pgpid: The PGP document ID

    Returns:
        List of translation source dicts (doc_relation contains 'Translation').
        Each includes a 'language' field (Hebrew or English).
    """
    if not pgpid:
        return []

    try:
        client = get_client()

        # Filter to doc_relation containing 'Translation' (Digital Translation)
        response = client.table('document_sources').select('*').eq(
            'pgpid', pgpid
        ).like('doc_relation', '%Translation%').order('sequence_order', desc=False).execute()

        return response.data or []

    except Exception as e:
        print(f"Error getting translations for document {pgpid}: {e}")
        return []
