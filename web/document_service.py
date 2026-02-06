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
import json
from typing import Optional, List, Dict, Any, Set
from web.supabase_client import get_client


def get_document_for_fragment(sys_id: str, page_num: int = None) -> Optional[Dict[str, Any]]:
    """
    Get the PGP document associated with a fragment.

    Args:
        sys_id: The GenizahSearch system ID for the fragment
        page_num: Optional page number (1=recto, 2=verso) to select the correct
                  document when multiple PGP documents exist for the same fragment

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

        # First, find all fragment links for this sys_id
        fragment_response = client.table('document_fragments').select(
            'document_id, page_info'
        ).eq('sys_id', sys_id).execute()

        if not fragment_response.data:
            return None

        # If page_num specified, try to find matching page_info
        # page_num 1 = recto, page_num 2 = verso
        pgpid = None
        if page_num and len(fragment_response.data) > 1:
            target_page = 'recto' if page_num == 1 else 'verso'
            for frag in fragment_response.data:
                if frag.get('page_info') == target_page:
                    pgpid = frag.get('document_id')
                    break

        # Fallback to first result if no page match or page_num not specified
        if not pgpid:
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


def get_all_sources_for_fragment(sys_id: str) -> List[Dict[str, Any]]:
    """
    Get all sources (editions and translations) for ALL PGP documents linked to a fragment.

    Some fragments have multiple PGP documents (e.g., one for recto, one for verso).
    This function retrieves sources from ALL linked documents.

    Args:
        sys_id: The GenizahSearch system ID for the fragment

    Returns:
        List of source dicts from all linked documents, ordered by: doc_relation
        (Edition first), then sequence_order. Each source includes 'page_info'
        from fragment link (recto/verso). Returns empty list if not found.
    """
    if not sys_id:
        return []

    try:
        client = get_client()

        # Get all fragment links for this sys_id
        fragment_response = client.table('document_fragments').select(
            'document_id, page_info'
        ).eq('sys_id', sys_id).execute()

        if not fragment_response.data:
            return []

        # Collect sources from all linked documents
        all_sources = []
        for frag in fragment_response.data:
            pgpid = frag.get('document_id')
            page_info = frag.get('page_info')  # 'recto' or 'verso'

            if pgpid:
                sources = get_sources_for_document(pgpid)
                # Add page_info to each source so we know which page it belongs to
                for source in sources:
                    source['page_info'] = page_info
                all_sources.extend(sources)

        # Sort: Editions first, then by sequence_order
        all_sources.sort(key=lambda x: (
            0 if 'Edition' in (x.get('doc_relation') or '') else 1,
            x.get('sequence_order', 0)
        ))

        return all_sources

    except Exception as e:
        print(f"Error getting all sources for fragment {sys_id}: {e}")
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


def get_sys_ids_with_transcriptions(sys_ids: List[str]) -> Set[str]:
    """
    Batch check which sys_ids have PGP transcriptions.

    Args:
        sys_ids: List of system IDs to check

    Returns:
        Set of sys_ids that have linked PGP documents with transcriptions
    """
    if not sys_ids:
        return set()

    try:
        client = get_client()
        # Query document_fragments for matching sys_ids
        # Any sys_id in document_fragments has a linked document
        response = client.table('document_fragments').select(
            'sys_id'
        ).in_('sys_id', sys_ids).execute()

        return {row['sys_id'] for row in (response.data or [])}
    except Exception as e:
        print(f"Error batch checking transcriptions: {e}")
        return set()


def get_fragments_by_tag(tag: str) -> List[Dict[str, Any]]:
    """
    Get all fragments linked to PGP documents with a specific tag.

    Uses GIN-indexed JSONB @> query for efficient tag matching.
    Returns fragment-level results (one per sys_id) with document metadata.

    Args:
        tag: Tag string to search for (e.g., "communal", "marriage")

    Returns:
        List of dicts with sys_id, shelfmark, document_type, description, pgpid.
        Returns empty list if not found or on error.
    """
    if not tag:
        return []

    try:
        client = get_client()

        # Step 1: Find documents with this tag (GIN-indexed JSONB @> query)
        doc_response = client.table('documents').select(
            'pgpid, shelfmark_combined, document_type, description'
        ).filter('tags', 'cs', json.dumps([tag])).execute()

        if not doc_response.data:
            return []

        # Step 2: Batch get all fragments for matching documents
        doc_ids = [d['pgpid'] for d in doc_response.data]
        frag_response = client.table('document_fragments').select(
            'sys_id, shelfmark, document_id'
        ).in_('document_id', doc_ids).execute()

        if not frag_response.data:
            return []

        # Step 3: Join fragment info with document metadata
        doc_map = {d['pgpid']: d for d in doc_response.data}
        results = []
        for frag in frag_response.data:
            doc = doc_map.get(frag['document_id'], {})
            results.append({
                'sys_id': frag['sys_id'],
                'shelfmark': frag['shelfmark'],
                'document_type': doc.get('document_type', ''),
                'description': doc.get('description', ''),
                'pgpid': frag['document_id'],
            })

        return results

    except Exception as e:
        print(f"Error searching by tag '{tag}': {e}")
        return []
