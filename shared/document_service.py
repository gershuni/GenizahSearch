# -*- coding: utf-8 -*-
"""
Document Service for PGP document-fragment relationships.

This module provides PgpService class and module-level functions for accessing
PGP document data from the local pgp.db SQLite sidecar:
- get_document_for_fragment(sys_id) -> dict | None
- get_fragments_for_document(pgpid) -> list[dict]
- get_transcription_for_document(pgpid) -> str | None
- get_document_metadata(pgpid) -> dict | None
- get_sources_for_document(pgpid) -> list[dict]
- get_all_sources_for_fragment(sys_id) -> list[dict]
- get_editions_for_document(pgpid) -> list[dict]
- get_translations_for_document(pgpid) -> list[dict]
- get_sys_ids_with_transcriptions(sys_ids) -> set[str]
- get_fragments_by_tag(tag) -> list[dict]
- get_all_distinct_tags() -> list[str]
- parse_transcription_sections(transcription) -> dict
- get_section_for_page(transcription, page_num, sections) -> str | None
- parse_html_sections(html_content) -> dict

All functions handle errors gracefully, returning None or empty lists
rather than raising exceptions. When the sidecar database is missing,
the service degrades gracefully (is_available() returns False).

Thread-safe mode (check_same_thread=False) is available for the NiceGUI
web app which serves concurrent requests from multiple threads.
"""

import json
import logging
import re
import sqlite3
from html import unescape
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

logger = logging.getLogger(__name__)

# Default sidecar location
_SIDECAR_FILENAME = "pgp.db"
_SIDECAR_DIR = "pgp_data"


def _find_project_root() -> Optional[Path]:
    """Find the project root by looking for libraries.csv up from this file."""
    current = Path(__file__).resolve().parent
    for _ in range(5):  # Up to 5 levels
        if (current / "libraries.csv").exists():
            return current
        current = current.parent
    return None


def _row_to_dict(row: sqlite3.Row, json_columns: tuple = ()) -> dict:
    """Convert sqlite3.Row to dict with JSON deserialization.

    Args:
        row: A sqlite3.Row object.
        json_columns: Column names whose TEXT values should be parsed
            via json.loads() back to Python objects (lists/dicts).

    Returns:
        Plain dict with JSON columns deserialized.
    """
    d = dict(row)
    for col in json_columns:
        if col in d and d[col] is not None:
            try:
                d[col] = json.loads(d[col])
            except (json.JSONDecodeError, TypeError):
                pass  # Leave as-is if parsing fails
    return d


class PgpService:
    """Service for accessing PGP document data from the SQLite sidecar."""

    def __init__(self, db_path: str = None, thread_safe: bool = False):
        """
        Initialize PgpService.

        Args:
            db_path: Path to pgp.db. If None, auto-detect from project root.
            thread_safe: If True, use check_same_thread=False for NiceGUI web app.
                        Desktop app should leave this False (single-threaded).
        """
        self._conn: Optional[sqlite3.Connection] = None
        self._db_path: Optional[str] = None

        # Resolve db_path
        if db_path is None:
            # Check user-updated sidecar location first (LOCALAPPDATA)
            import os
            user_path = os.path.join(
                os.environ.get('LOCALAPPDATA', ''),
                'GenizahSearchPro', 'data', _SIDECAR_DIR, _SIDECAR_FILENAME
            )
            if os.path.isfile(user_path):
                db_path = user_path
            else:
                root = _find_project_root()
                if root:
                    db_path = str(root / _SIDECAR_DIR / _SIDECAR_FILENAME)

        if db_path is None:
            logger.warning("PgpService: No db_path provided and project root not found")
            return

        self._db_path = db_path
        db_file = Path(db_path)

        if not db_file.exists():
            logger.warning(f"PgpService: Sidecar database not found at {db_path}")
            return

        try:
            # Open read-only connection using URI mode
            uri = f"file:{db_path}?mode=ro"
            self._conn = sqlite3.connect(
                uri,
                uri=True,
                check_same_thread=not thread_safe,
                timeout=10.0,
            )
            self._conn.row_factory = sqlite3.Row
            logger.info(f"PgpService: Connected to {db_path}")
        except Exception as e:
            logger.error(f"PgpService: Failed to connect to {db_path}: {e}")
            self._conn = None

    def is_available(self) -> bool:
        """Returns True if the sidecar database connection is active."""
        return self._conn is not None

    def get_document_for_fragment(self, sys_id: str, page_num: int = None) -> Optional[Dict[str, Any]]:
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
        if not sys_id or not self._conn:
            return None

        try:
            # Step 1: Find all fragment links for this sys_id
            cursor = self._conn.execute(
                "SELECT document_id, page_info FROM document_fragments WHERE sys_id = ?",
                (sys_id,)
            )
            frags = cursor.fetchall()

            if not frags:
                return None

            # If page_num specified, try to find matching page_info
            # page_num 1 = recto, page_num 2 = verso
            pgpid = None
            matched_page_info = None
            if page_num and len(frags) > 1:
                target_page = 'recto' if page_num == 1 else 'verso'
                for f in frags:
                    if f['page_info'] == target_page:
                        pgpid = f['document_id']
                        matched_page_info = f['page_info']
                        break

            # Fallback to first result if no page match or page_num not specified
            if not pgpid:
                pgpid = frags[0]['document_id']
                matched_page_info = frags[0]['page_info']

            if not pgpid:
                return None

            # Step 2: Get the full document
            cursor = self._conn.execute(
                "SELECT * FROM documents WHERE pgpid = ?", (pgpid,)
            )
            row = cursor.fetchone()
            doc = _row_to_dict(row, json_columns=('tags',)) if row else None
            if doc and matched_page_info:
                doc['_fragment_page_info'] = matched_page_info
            return doc

        except Exception as e:
            logger.error(f"Error getting document for fragment {sys_id}: {e}")
            return None

    def get_fragments_for_document(self, pgpid: int) -> List[Dict[str, Any]]:
        """
        Get all fragments for a PGP document, ordered by sequence.

        Args:
            pgpid: The PGP document ID

        Returns:
            List of fragment dicts (id, document_id, sys_id, shelfmark,
            sequence_order, page_info), ordered by sequence_order ASC.
            Returns empty list if not found or on error.
        """
        if not pgpid or not self._conn:
            return []

        try:
            cursor = self._conn.execute(
                "SELECT * FROM document_fragments WHERE document_id = ? ORDER BY sequence_order",
                (pgpid,)
            )
            return [dict(row) for row in cursor]

        except Exception as e:
            logger.error(f"Error getting fragments for document {pgpid}: {e}")
            return []

    def get_transcription_for_document(self, pgpid: int) -> Optional[str]:
        """
        Get the transcription text for a PGP document.

        Args:
            pgpid: The PGP document ID

        Returns:
            Transcription string, or None if not found, empty, or on error.
        """
        if not pgpid or not self._conn:
            return None

        try:
            cursor = self._conn.execute(
                "SELECT transcription FROM documents WHERE pgpid = ?", (pgpid,)
            )
            row = cursor.fetchone()

            if row:
                transcription = row['transcription']
                # Return None for empty strings as well
                return transcription if transcription else None

            return None

        except Exception as e:
            logger.error(f"Error getting transcription for document {pgpid}: {e}")
            return None

    def get_document_metadata(self, pgpid: int) -> Optional[Dict[str, Any]]:
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
        if not pgpid or not self._conn:
            return None

        try:
            cursor = self._conn.execute(
                "SELECT document_type, tags, doc_date_original, doc_date_standard, "
                "inferred_date_display, description, pgp_url, shelfmark_combined "
                "FROM documents WHERE pgpid = ?", (pgpid,)
            )
            row = cursor.fetchone()
            return _row_to_dict(row, json_columns=('tags',)) if row else None

        except Exception as e:
            logger.error(f"Error getting metadata for document {pgpid}: {e}")
            return None

    def get_sources_for_document(self, pgpid: int) -> List[Dict[str, Any]]:
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
        if not pgpid or not self._conn:
            return []

        try:
            # Order by doc_relation (Editions first alphabetically before Translations)
            # then by sequence_order
            cursor = self._conn.execute(
                "SELECT * FROM document_sources WHERE pgpid = ? "
                "ORDER BY doc_relation, sequence_order",
                (pgpid,)
            )
            return [_row_to_dict(row, json_columns=('sections',)) for row in cursor]

        except Exception as e:
            logger.error(f"Error getting sources for document {pgpid}: {e}")
            return []

    def get_all_sources_for_fragment(self, sys_id: str) -> List[Dict[str, Any]]:
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
        if not sys_id or not self._conn:
            return []

        try:
            # Get all fragment links for this sys_id
            cursor = self._conn.execute(
                "SELECT document_id, page_info FROM document_fragments WHERE sys_id = ?",
                (sys_id,)
            )
            frags = cursor.fetchall()

            if not frags:
                return []

            # Build page_info map
            page_map = {f['document_id']: f['page_info'] for f in frags}
            pgpids = list(page_map.keys())

            # Batch get sources for all linked documents (2 queries instead of N+1)
            placeholders = ','.join('?' * len(pgpids))
            cursor = self._conn.execute(
                f"SELECT * FROM document_sources WHERE pgpid IN ({placeholders}) "
                f"ORDER BY doc_relation, sequence_order",
                pgpids
            )

            all_sources = []
            for row in cursor:
                source = _row_to_dict(row, json_columns=('sections',))
                source['page_info'] = page_map.get(source['pgpid'])
                all_sources.append(source)

            # Sort: Editions first, then by sequence_order
            all_sources.sort(key=lambda x: (
                0 if 'Edition' in (x.get('doc_relation') or '') else 1,
                x.get('sequence_order', 0)
            ))

            return all_sources

        except Exception as e:
            logger.error(f"Error getting all sources for fragment {sys_id}: {e}")
            return []

    def get_editions_for_document(self, pgpid: int) -> List[Dict[str, Any]]:
        """
        Get Digital Editions for a PGP document.

        Args:
            pgpid: The PGP document ID

        Returns:
            List of edition source dicts (doc_relation contains 'Edition').
            Useful for transcription selector with multiple scholars.
        """
        if not pgpid or not self._conn:
            return []

        try:
            cursor = self._conn.execute(
                "SELECT * FROM document_sources WHERE pgpid = ? "
                "AND doc_relation LIKE '%Edition%' ORDER BY sequence_order",
                (pgpid,)
            )
            return [_row_to_dict(row, json_columns=('sections',)) for row in cursor]

        except Exception as e:
            logger.error(f"Error getting editions for document {pgpid}: {e}")
            return []

    def get_translations_for_document(self, pgpid: int) -> List[Dict[str, Any]]:
        """
        Get Digital Translations for a PGP document.

        Args:
            pgpid: The PGP document ID

        Returns:
            List of translation source dicts (doc_relation contains 'Translation').
            Each includes a 'language' field (Hebrew or English).
        """
        if not pgpid or not self._conn:
            return []

        try:
            cursor = self._conn.execute(
                "SELECT * FROM document_sources WHERE pgpid = ? "
                "AND doc_relation LIKE '%Translation%' ORDER BY sequence_order",
                (pgpid,)
            )
            return [_row_to_dict(row, json_columns=('sections',)) for row in cursor]

        except Exception as e:
            logger.error(f"Error getting translations for document {pgpid}: {e}")
            return []

    def get_sys_ids_with_transcriptions(self, sys_ids: List[str]) -> Set[str]:
        """
        Batch check which sys_ids have PGP transcriptions.

        Args:
            sys_ids: List of system IDs to check

        Returns:
            Set of sys_ids that have linked PGP documents with transcriptions
        """
        if not sys_ids or not self._conn:
            return set()

        try:
            result_set = set()
            # Chunk to stay under SQLite variable limit (999)
            batch_size = 500
            for i in range(0, len(sys_ids), batch_size):
                batch = sys_ids[i:i + batch_size]
                placeholders = ','.join('?' * len(batch))
                cursor = self._conn.execute(
                    f"SELECT DISTINCT sys_id FROM document_fragments "
                    f"WHERE sys_id IN ({placeholders})",
                    batch
                )
                result_set.update(row['sys_id'] for row in cursor)
            return result_set

        except Exception as e:
            logger.error(f"Error batch checking transcriptions: {e}")
            return set()

    def get_fragments_by_tag(self, tag: str) -> List[Dict[str, Any]]:
        """
        Get all fragments linked to PGP documents with a specific tag.

        Uses SQLite json_each() for efficient tag matching.
        Returns fragment-level results (one per sys_id) with document metadata.

        Args:
            tag: Tag string to search for (e.g., "communal", "marriage")

        Returns:
            List of dicts with sys_id, shelfmark, document_type, description, pgpid, transcription.
            Returns empty list if not found or on error.
        """
        if not tag or not self._conn:
            return []

        try:
            # Step 1: Find documents with this tag via json_each()
            cursor = self._conn.execute(
                "SELECT pgpid, shelfmark_combined, document_type, description, transcription "
                "FROM documents d, json_each(d.tags) je "
                "WHERE je.value = ?",
                (tag,)
            )
            docs = cursor.fetchall()

            if not docs:
                return []

            # Step 2: Batch get all fragments for matching documents
            doc_ids = [d['pgpid'] for d in docs]
            placeholders = ','.join('?' * len(doc_ids))
            frag_cursor = self._conn.execute(
                f"SELECT sys_id, shelfmark, document_id FROM document_fragments "
                f"WHERE document_id IN ({placeholders})",
                doc_ids
            )
            frags = frag_cursor.fetchall()

            if not frags:
                return []

            # Step 3: Join fragment info with document metadata
            doc_map = {d['pgpid']: dict(d) for d in docs}
            results = []
            for frag in frags:
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
            logger.error(f"Error searching by tag '{tag}': {e}")
            return []

    def get_all_distinct_tags(self) -> List[str]:
        """Get all distinct PGP tags across all documents, sorted alphabetically."""
        if not self._conn:
            return []

        try:
            cursor = self._conn.execute(
                "SELECT DISTINCT je.value as tag "
                "FROM documents d, json_each(d.tags) je "
                "WHERE je.value != '' "
                "ORDER BY tag"
            )
            return [row['tag'] for row in cursor]

        except Exception as e:
            logger.error(f"Error getting distinct tags: {e}")
            return []

    def close(self):
        """Close the database connection if open."""
        if self._conn is not None:
            try:
                self._conn.close()
                logger.info("PgpService: Connection closed")
            except Exception as e:
                logger.error(f"PgpService.close error: {e}")
            finally:
                self._conn = None

    def get_version(self) -> Optional[str]:
        """
        Get the sidecar database version.

        Returns:
            Version string (e.g., '1.0.0') or None if unavailable.
        """
        if self._conn is None:
            return None
        try:
            cursor = self._conn.execute(
                "SELECT value FROM meta WHERE key = 'version'"
            )
            row = cursor.fetchone()
            return row["value"] if row else None
        except Exception as e:
            logger.error(f"PgpService.get_version error: {e}")
            return None


# ── Pure Functions (no database dependency) ────────────────────────


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


def get_section_for_page(transcription: str, page_num: int, sections: list = None, fragment_page_info: str = None) -> Optional[str]:
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
        fragment_page_info: Optional page_info from document_fragments ('recto' or 'verso').
                  When set and the transcription has no recto/verso markers, used to
                  suppress display on the wrong page (e.g. verso-only doc on recto page).

    Returns:
        Section text for the page, None if no content exists for this page,
        or full transcription if no recto/verso markers found at all.
    """
    # Path 0: fragment_page_info override — if the fragment is linked to a
    # specific page (recto/verso) and the transcription has no recto/verso
    # markers, use fragment_page_info to decide which page gets the text.
    if fragment_page_info:
        target_page = 'recto' if page_num == 1 else 'verso'
        # Check if the transcription actually has recto+verso markers
        parsed_check = parse_transcription_sections(transcription)
        has_both_markers = bool(parsed_check.get('recto')) and bool(parsed_check.get('verso'))
        if not has_both_markers:
            # No markers — the entire text belongs to fragment_page_info side only
            if fragment_page_info == target_page:
                return transcription
            else:
                return None

    # Path 1: Structured sections (reliable canvas-based lookup)
    if sections:
        for section in sections:
            if section.get('canvas_num') == page_num:
                text = section.get('text')
                if text:
                    # Validate section text belongs to this source's content.
                    # The import may have assigned the same sections to all sources
                    # for a PGPID; if so, fall through to regex on the actual content.
                    sample = ''.join(text.split())[:40]
                    if sample and sample in ''.join(transcription.split()):
                        return text
                # Section empty or doesn't match -- fall through to regex
                break
        else:
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


# ── Module-level Singleton ─────────────────────────────────────────

_default_service: Optional[PgpService] = None


def get_pgp_service(thread_safe: bool = True) -> PgpService:
    """Get or create the default PgpService singleton.

    Args:
        thread_safe: If True, use check_same_thread=False. Defaults to True
            since read-only SQLite connections are safe across threads and
            this eliminates the web vs desktop initialization concern.

    Returns:
        PgpService instance (may have is_available() == False if pgp.db missing).
    """
    global _default_service
    if _default_service is None:
        _default_service = PgpService(thread_safe=thread_safe)
    return _default_service


def reset_pgp_service():
    """Reset the singleton PgpService instance.

    Call this after replacing the pgp.db sidecar file to force
    re-initialization on next access. Closes the existing connection
    before clearing the singleton.
    """
    global _default_service
    if _default_service is not None:
        _default_service.close()
        _default_service = None


# ── Module-level Wrapper Functions (backward-compatible API) ───────


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
    svc = get_pgp_service()
    return svc.get_document_for_fragment(sys_id, page_num)


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
    svc = get_pgp_service()
    return svc.get_fragments_for_document(pgpid)


def get_transcription_for_document(pgpid: int) -> Optional[str]:
    """
    Get the transcription text for a PGP document.

    Args:
        pgpid: The PGP document ID

    Returns:
        Transcription string, or None if not found, empty, or on error.
    """
    svc = get_pgp_service()
    return svc.get_transcription_for_document(pgpid)


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
    svc = get_pgp_service()
    return svc.get_document_metadata(pgpid)


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
    svc = get_pgp_service()
    return svc.get_sources_for_document(pgpid)


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
    svc = get_pgp_service()
    return svc.get_all_sources_for_fragment(sys_id)


def get_editions_for_document(pgpid: int) -> List[Dict[str, Any]]:
    """
    Get Digital Editions for a PGP document.

    Args:
        pgpid: The PGP document ID

    Returns:
        List of edition source dicts (doc_relation contains 'Edition').
        Useful for transcription selector with multiple scholars.
    """
    svc = get_pgp_service()
    return svc.get_editions_for_document(pgpid)


def get_translations_for_document(pgpid: int) -> List[Dict[str, Any]]:
    """
    Get Digital Translations for a PGP document.

    Args:
        pgpid: The PGP document ID

    Returns:
        List of translation source dicts (doc_relation contains 'Translation').
        Each includes a 'language' field (Hebrew or English).
    """
    svc = get_pgp_service()
    return svc.get_translations_for_document(pgpid)


def get_sys_ids_with_transcriptions(sys_ids: List[str]) -> Set[str]:
    """
    Batch check which sys_ids have PGP transcriptions.

    Args:
        sys_ids: List of system IDs to check

    Returns:
        Set of sys_ids that have linked PGP documents with transcriptions
    """
    svc = get_pgp_service()
    return svc.get_sys_ids_with_transcriptions(sys_ids)


def get_fragments_by_tag(tag: str) -> List[Dict[str, Any]]:
    """
    Get all fragments linked to PGP documents with a specific tag.

    Uses SQLite json_each() for efficient tag matching.
    Returns fragment-level results (one per sys_id) with document metadata.

    Args:
        tag: Tag string to search for (e.g., "communal", "marriage")

    Returns:
        List of dicts with sys_id, shelfmark, document_type, description, pgpid, transcription.
        Returns empty list if not found or on error.
    """
    svc = get_pgp_service()
    return svc.get_fragments_by_tag(tag)


def get_all_distinct_tags() -> List[str]:
    """Get all distinct PGP tags across all documents, sorted alphabetically."""
    svc = get_pgp_service()
    return svc.get_all_distinct_tags()


def get_version() -> Optional[str]:
    """Get the PGP sidecar database version."""
    svc = get_pgp_service()
    return svc.get_version() if svc.is_available() else None
