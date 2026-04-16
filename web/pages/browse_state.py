# -*- coding: utf-8 -*-
"""
Browse State Classes and Helpers

Extracted from web/pages/browse.py (Phase 73, Plan 01).
Contains BrowseState class and module-level _crossref_cache dict.

This module has ZERO UI (nicegui.ui) dependencies -- it only holds state
and pure logic.
"""
from __future__ import annotations

from typing import Any, Optional, List, Dict

from web.services import BrowsePage, DocumentPage


class BrowseState:
    """Holds the state for the browse page."""

    def __init__(self):
        self.shelfmark_query: str = ''
        self.current_page: Optional[BrowsePage] = None
        self.sys_id: Optional[str] = None
        self.is_loading: bool = False
        self.error: Optional[str] = None
        self.search_error: Optional[str] = None  # Inline error for shelfmark not found
        self.zoom_level: float = 1.0
        self.rotation: int = 0
        self.is_fullscreen: bool = False
        self.highlight_terms: Optional[str] = None
        self.page_input_value: int = 1
        self.view_all: bool = False
        self.full_manuscript: List[DocumentPage] = []
        # Edit state
        self.edit_mode: bool = False
        self.edit_text: str = ""
        self.edit_notes: str = ""
        self.original_edit_text: str = ""  # Text when editing started
        self.draft_saved: bool = False
        self.draft_id: Optional[str] = None
        self.edit_loading: bool = False
        self.error_message: Optional[str] = None
        self.fullscreen_edit: bool = False  # Fullscreen edit mode
        # PGP transcription data
        self.pgp_transcription: Optional[Dict[str, Any]] = None
        # PGP metadata for display in metadata panel
        self.pgp_metadata: Optional[Dict[str, Any]] = None
        # Multi-source data (all editions and translations for this document)
        self.all_sources: Optional[List[Dict[str, Any]]] = None
        # Joined fragments view mode
        self.view_joined: bool = False
        self.joined_fragments_info: list = []  # [{shelfmark, sys_id}]
        self.joined_pgpid: Optional[int] = None
        # Reading desk entries (v3 enhanced joined view)
        self.reading_desk_entries: list = []
        # Each entry: {sys_id, shelfmark, pages: [{p_num, text, full_header, fl_id}], sources: [], pgp_doc: {}}
        self.reading_desk_selected_sources: dict = {}  # sys_id -> selected source index
        # Volume-aware browse (multi-IE manuscripts)
        self.volume_ie: Optional[str] = None  # Active IE; None = default (all pages)
        # Source switching state: 'nli' (default), 'cambridge', 'manchester', or 'jts'
        self.active_source: str = 'nli'
        self.source_user_override: bool = False  # True when user explicitly clicked a source button
        # Pre-fetched FJMS metadata (populated in load_page, consumed in update_content)
        self.fjms_data: Optional[Dict[str, Any]] = None
        # Pre-fetched crossref metadata (populated in _load_enrichment, consumed in update_content)
        self.crossref_data: Optional[Dict[str, Any]] = None
        # Enrichment loading state (two-phase async loading)
        self.enrichment_loaded: bool = False
        self.enrichment_loading: bool = False
        # Title translation (populated in load_page)
        self.title_translation: Optional[Dict[str, str]] = None
        self.oxford_translations: Dict[str, str] = {}  # english_text -> hebrew_text


# Module-level crossref cache: keyed by sys_id, persists across page navigations
# within the session. Crossref data is read-only public metadata, safe to share.
# Lives in browse_state.py (not browse_enrichment.py) because both browse.py
# and browse_enrichment.py import it -- shared module avoids circular imports.
_crossref_cache: Dict[str, dict] = {}
