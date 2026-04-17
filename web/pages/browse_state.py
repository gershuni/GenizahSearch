# -*- coding: utf-8 -*-
"""
Browse State Classes and Helpers

Extracted from web/pages/browse.py (Phase 73, Plan 01).
Contains BrowseState class and module-level _crossref_cache dict.

This module has ZERO UI (nicegui.ui) dependencies -- it only holds state
and pure logic.
"""
from __future__ import annotations

import logging
from typing import Any, Optional, List, Dict

from nicegui import app

from web.services import BrowsePage, DocumentPage

logger = logging.getLogger(__name__)


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


# ---------------------------------------------------------------------------
# Phase 74: Browse snapshot helpers (D-07)
# ---------------------------------------------------------------------------

_BROWSE_SNAPSHOT_VERSION = 1


def restore_browse_snapshot(state: 'BrowseState') -> tuple:
    """Hydrate browse snapshot fields; return raw (position, reading_desk) dicts.

    Returns a tuple (saved_position, saved_reading_desk). The bootstrap caller
    feeds BOTH directly into resolve_browse_bootstrap() - this avoids a second
    app.storage.user.get() in the bootstrap block (review-revision: Gemini #15).

    IMPORTANT - partial restore only (review-revision: Codex MEDIUM #5):
    This helper does NOT replace the live `_restore_reading_desk_state()`
    function in browse.py. The saved `reading_desk_state` only stores
    minimal `{sys_id, shelfmark}` per entry; rebuilding fully-populated
    page/source structures requires the existing `_restore_reading_desk_state()`
    local helper (which refetches pages and enrichment). This helper's role
    is strictly to:
      (a) return the raw dicts for bootstrap precedence resolution
      (b) validate version stamp and reset on mismatch
      (c) be the single-read-path so bootstrap does not double-read storage
    Caller then decides whether to invoke _restore_reading_desk_state().

    Silently discards snapshot if version stamp is missing or stale (D-04).
    Volume_ie validation is left to the caller (Pitfall 5) - it requires
    querying get_volumes_for_sys_id() which is not in this module's deps.

    NOTE (tab stomping limitation - Codex #14): version stamp prevents
    cross-VERSION corruption, not same-version Tab-B-overwrites-Tab-A.
    True per-tab isolation is deferred (Codex W3 - future phase).
    """
    stored_version = app.storage.user.get('browse_snapshot_schema_version', 0)
    if stored_version != _BROWSE_SNAPSHOT_VERSION:
        clear_browse_snapshot()
        return (None, None)

    saved_position = None
    saved_desk = None
    try:
        pos = app.storage.user.get('browse_position')
        if pos and pos.get('sys_id'):
            saved_position = pos
    except Exception:
        pass
    try:
        desk = app.storage.user.get('reading_desk_state')
        if desk and desk.get('entries'):
            saved_desk = desk
    except Exception as e:
        logger.error(f"[ReadingDesk] Error reading snapshot: {e}")

    return (saved_position, saved_desk)


def persist_browse_snapshot(state: 'BrowseState', page=None) -> None:
    """Serialize browse position and reading desk state to app.storage.user.

    Replaces the inline writes at browse.py:~777-785 (browse_position)
    and browse.py:~1056-1074 (reading desk).

    Args:
        state: BrowseState with reading desk + sys_id / volume_ie fields.
        page:  BrowsePage object (for shelfmark / p_num extraction). Optional:
               if None, only reading-desk half is persisted.
    """
    if not app.storage.user.get('session_persistence_enabled', True):
        return
    try:
        app.storage.user['browse_snapshot_schema_version'] = _BROWSE_SNAPSHOT_VERSION

        # Position (browse.py:777-785 analog).
        if page is not None and state.sys_id:
            app.storage.user['browse_position'] = {
                'sys_id': state.sys_id,
                'p_num': getattr(page, 'p_num', 1),
                'shelfmark': getattr(page, 'shelfmark', ''),
                'volume_ie': state.volume_ie,
            }

        # Reading desk (browse.py:1056-1074 analog).
        if state.view_joined and state.reading_desk_entries:
            rd_data = [
                {'sys_id': e.get('sys_id', ''), 'shelfmark': e.get('shelfmark', '')}
                for e in state.reading_desk_entries
            ]
            app.storage.user['reading_desk_state'] = {
                'entries': rd_data,
                'pgpid': state.joined_pgpid,
                'selected_sources': state.reading_desk_selected_sources or {},
            }
        else:
            app.storage.user.pop('reading_desk_state', None)
    except Exception as e:
        logger.error(f"[BrowseSnapshot] Error persisting state: {e}")


def clear_browse_snapshot() -> None:
    """Wipe browse_position and reading_desk_state keys."""
    for key in ('browse_position', 'reading_desk_state', 'browse_snapshot_schema_version'):
        try:
            app.storage.user.pop(key, None)
        except Exception:
            pass
