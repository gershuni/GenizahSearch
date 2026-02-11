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
from html.parser import HTMLParser
from html import unescape
from typing import Optional, List, Dict, Any, Set
from shared.supabase_provider import get_client


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
    # Handles all known marker variants:
    #   bare: Recto\n, Verso\n
    #   period: Verso.\n, Recto.\n
    #   period+qualifier: Verso. Address.\n
    #   parenthetical: Verso (address)\n, Verso (upside down)\n
    #   dash/comma: Recto - right margin\n
    #   space+word: Recto Margin\n
    # Uses explicit [Rr]ecto/[Vv]erso (not IGNORECASE) to avoid matching
    # content lines like "recto text..." which start with the keyword.
    # \b word boundary prevents matching "Rectory" etc.
    section_pattern = re.compile(
        r'^([Rr]ecto|[Vv]erso)\b'
        r'(?:'
        r'[.\s]*'                            # optional periods/spaces after keyword
        r'(?:'
        r'[-,]\s*[^\n]{0,40}'               # dash/comma modifier
        r'|'
        r'\([^\n)]{0,40}\)'                  # parenthetical modifier
        r'|'
        r'[A-Z][a-z]*(?:\.[^\n]{0,30})?'    # capitalized qualifier word(s)
        r')?'
        r')?'
        r'\s*\n',
        re.MULTILINE
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


def get_section_for_page(transcription: str, page_num: int, sections: list = None) -> Optional[str]:
    """
    Get the appropriate transcription section for a page number.

    When structured sections are available (from pgp-text HTML import),
    uses canvas_num matching for reliable page mapping.
    Falls back to regex-based parsing when sections are not available.

    Args:
        transcription: Full transcription text
        page_num: Page number (1 = recto, 2 = verso for single-fragment docs)
        sections: Optional structured sections from document_sources.sections JSONB.
                  List of dicts with canvas_url, canvas_num, label, text.
                  When provided, uses canvas_num matching instead of regex parsing.

    Returns:
        Section text for the page, None if no content exists for this page,
        or full transcription if no recto/verso markers found at all.
    """
    # Path 1: Structured sections (reliable canvas-based lookup)
    if sections:
        for section in sections:
            if section.get('canvas_num') == page_num:
                return section.get('text')
        # page_num beyond available canvases -- return full transcription
        return transcription

    # Path 2: Regex fallback (existing logic)
    parsed = parse_transcription_sections(transcription)

    # Check if the transcription has any recto/verso markers
    has_recto = bool(parsed.get('recto'))
    has_verso = bool(parsed.get('verso'))

    # Map page number to section type
    # For single-fragment: page 1 = recto, page 2 = verso
    if page_num == 1:
        section_list = parsed.get('recto', [])
        # If document has verso-only content, return None for recto page
        if not section_list and has_verso:
            return None
    elif page_num == 2:
        section_list = parsed.get('verso', [])
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
        result_set = set()
        # Chunk to avoid URL length limits with large result sets
        chunk_size = 200
        for i in range(0, len(sys_ids), chunk_size):
            chunk = sys_ids[i:i + chunk_size]
            response = client.table('document_fragments').select(
                'sys_id'
            ).in_('sys_id', chunk).execute()
            result_set.update(row['sys_id'] for row in (response.data or []))
        return result_set
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
        List of dicts with sys_id, shelfmark, document_type, description, pgpid, transcription.
        Returns empty list if not found or on error.
    """
    if not tag:
        return []

    try:
        client = get_client()

        # Step 1: Find documents with this tag (GIN-indexed JSONB @> query)
        doc_response = client.table('documents').select(
            'pgpid, shelfmark_combined, document_type, description, transcription'
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
                'transcription': doc.get('transcription', ''),
            })

        return results

    except Exception as e:
        print(f"Error searching by tag '{tag}': {e}")
        return []


def get_all_distinct_tags() -> List[str]:
    """Get all distinct PGP tags across all documents, sorted alphabetically."""
    try:
        client = get_client()
        response = client.table('documents').select('tags').not_.is_('tags', 'null').execute()
        all_tags = set()
        for row in (response.data or []):
            tags = row.get('tags', [])
            if tags:
                for tag in tags:
                    all_tags.add(tag)
        return sorted(all_tags)
    except Exception as e:
        print(f"Error getting distinct tags: {e}")
        return []


class PGPHTMLParser(HTMLParser):
    """Parse PGP HTML into structured per-canvas sections.

    Handles the pgp-text HTML structure where h3 elements are INSIDE
    data-canvas divs, with multiple h3+ol sub-sections possible per canvas.
    """

    def __init__(self):
        super().__init__()
        self.sections = []          # Final list of canvas sections
        self.language = None
        self.direction = None

        self._in_canvas_div = False
        self._current_canvas_url = None
        self._current_canvas_num = None
        self._current_subsections = []  # [{label, lines}]
        self._current_label = None
        self._current_lines = []

        self._in_h3 = False
        self._h3_text = ''
        self._in_li = False
        self._in_p_inside_canvas = False
        self._li_text = ''
        self._p_text = ''
        self._canvas_count = 0
        self._div_depth = 0         # Track nested div depth inside canvas

    def handle_starttag(self, tag, attrs):
        attrs_dict = dict(attrs)

        if tag == 'section':
            self.language = attrs_dict.get('lang')
            self.direction = attrs_dict.get('dir')

        elif tag == 'div' and 'data-canvas' in attrs_dict:
            self._canvas_count += 1
            self._in_canvas_div = True
            self._div_depth = 1
            self._current_canvas_url = attrs_dict['data-canvas']
            # Try to extract numeric canvas ID from URL
            m = re.search(r'/canvas/(\d+)/?$', self._current_canvas_url)
            self._current_canvas_num = int(m.group(1)) if m else self._canvas_count
            self._current_subsections = []
            self._current_label = None
            self._current_lines = []

        elif tag == 'div' and self._in_canvas_div:
            self._div_depth += 1

        elif tag == 'h3' and self._in_canvas_div:
            self._in_h3 = True
            self._h3_text = ''

        elif tag == 'li' and self._in_canvas_div:
            self._in_li = True
            self._li_text = ''

        elif tag == 'p' and self._in_canvas_div and not self._in_li:
            # Standalone <p> in canvas div (not <li><p>)
            self._in_p_inside_canvas = True
            self._p_text = ''

    def handle_data(self, data):
        if self._in_h3:
            self._h3_text += data
        elif self._in_li:
            self._li_text += data
        elif self._in_p_inside_canvas:
            self._p_text += data

    def handle_endtag(self, tag):
        if tag == 'h3' and self._in_h3:
            self._in_h3 = False
            # Save any accumulated lines under the previous label
            if self._current_lines:
                self._current_subsections.append({
                    'label': self._current_label,
                    'lines': self._current_lines[:]
                })
                self._current_lines = []
            self._current_label = unescape(self._h3_text.strip())

        elif tag == 'li' and self._in_li:
            self._in_li = False
            text = unescape(self._li_text.strip())
            if text:
                self._current_lines.append(text)

        elif tag == 'p' and self._in_p_inside_canvas:
            self._in_p_inside_canvas = False
            text = unescape(self._p_text.strip())
            if text and text != '...':
                self._current_lines.append(text)

        elif tag == 'div' and self._in_canvas_div:
            self._div_depth -= 1
            if self._div_depth <= 0:
                # Closing the canvas div -- finalize
                if self._current_lines:
                    self._current_subsections.append({
                        'label': self._current_label,
                        'lines': self._current_lines[:]
                    })

                # Build merged text from all sub-sections
                all_text_parts = []
                for sub in self._current_subsections:
                    if sub['label'] and len(self._current_subsections) > 1:
                        all_text_parts.append(f"[{sub['label']}]")
                    all_text_parts.extend(sub['lines'])

                self.sections.append({
                    'canvas_url': self._current_canvas_url,
                    'canvas_num': self._current_canvas_num,
                    'label': self._current_subsections[0]['label'] if self._current_subsections else None,
                    'text': '\n'.join(all_text_parts),
                    'subsections': self._current_subsections if len(self._current_subsections) > 1 else None,
                })

                self._in_canvas_div = False
                self._current_canvas_url = None
                self._current_canvas_num = None
                self._current_subsections = []
                self._current_label = None
                self._current_lines = []

    def handle_entityref(self, name):
        self.handle_data(unescape(f'&{name};'))

    def handle_charref(self, name):
        self.handle_data(unescape(f'&#{name};'))


def parse_html_sections(html_content: str) -> dict:
    """
    Parse PGP HTML into structured per-canvas sections.

    Parses pgp-text HTML files that use <div data-canvas="..."> elements
    to map transcription sections to IIIF canvas URLs. Handles h3 elements
    INSIDE data-canvas divs, multiple sub-sections per canvas, and both
    ol/li and standalone p text containers.

    Args:
        html_content: Raw HTML string from a pgp-text file

    Returns:
        Dict with:
        - sections: list of {canvas_url, canvas_num, label, text, subsections}
        - language: language code from section element (e.g. 'jrb', 'he', 'en')
        - direction: text direction from section element ('rtl' or 'ltr')
    """
    if not html_content:
        return {'sections': [], 'language': None, 'direction': None}

    parser = PGPHTMLParser()
    parser.feed(html_content)

    return {
        'sections': parser.sections,
        'language': parser.language,
        'direction': parser.direction,
    }
