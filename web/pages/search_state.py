# -*- coding: utf-8 -*-
"""
Search State Classes and Helpers

Extracted from web/pages/search.py (Phase 72, Plan 01).
Contains SearchUIState, AdvancedViewState, SearchPageRefs dataclass,
search history management functions, and domain_display_name helper.

This module has ZERO UI (nicegui.ui) dependencies -- it only holds state
and pure logic that operates on app.storage or SearchUIState fields.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional, List, Set
from datetime import datetime

from nicegui import app
from web.translations import tr, get_language
from web.services import BrowsePage


# ---------------------------------------------------------------------------
# SearchUIState -- main per-page search state holder
# ---------------------------------------------------------------------------

class SearchUIState:
    def __init__(self):
        self.progress = 0.0
        self.status = ""
        self.is_running = False
        self.is_cancelled = False  # For stop button functionality
        self.results = []
        self.selected_result = None
        self.total_count = 0
        self.current_page_idx = 0  # For browse within viewer
        self.current_page = 0  # Zero-indexed page number for result pagination
        self.selected_indices = set()  # For bulk operations
        self.is_panel_collapsed = False  # For collapsible search panel
        self.last_scroll_top = 0  # For scroll-based auto-collapse
        self.update_timer = None  # Track progress update asyncio Task to prevent duplicates
        self.transcription_sys_ids: Set[str] = set()  # sys_ids with PGP transcriptions
        self.displayed_results = []  # Currently rendered subset (may be filtered)
        self.builder_negated_words: list = []  # Words negated via Query Builder
        self.result_domains: dict = {}  # Domain classification map for result indicators
        self.all_result_domains: dict = {}  # sys_id -> list of domain names (deduped)
        self.domain_exclusions: set = set()  # domain names user has excluded
        self.has_domain_data: bool = False  # whether any results have domain data
        self.domain_name_map: dict = {}  # English domain name -> Hebrew name
        self.catalog_source_counts: dict = {}  # sys_id -> count of catalog sources
        self.domain_hierarchy: dict = {}  # cached hierarchy from get_domain_hierarchy()
        self.search_start_time: float = 0.0  # For elapsed timer display
        self.printed_ids: set = set()  # sys_ids with FragmentMaterial=Printed
        self.printed_filter: str = 'all'  # 'all', 'hide_printed', 'only_printed'
        self.domain_excluded_results: list = []  # Results hidden by domain exclusion (with reasons)
        # Pre-search filter state (Search only in... panel)
        self.filter_domains: list = []      # Selected domain filters (multi-select)
        self.filter_authors: list = []      # Selected authors (person_ids, multi-select)
        self.filter_works: list = []        # Selected works (title_ids, multi-select)
        self.filter_include_mode: bool = True  # True=include, False=exclude
        self.filter_date_from: int = None   # Date range start
        self.filter_date_to: int = None     # Date range end
        self.filter_material_exclude: list = []  # Material types to exclude (e.g., ['Printed'])
        self.filter_manuscript_count: int = None  # Count of manuscripts matching current filters
        self.restrict_sys_ids: set = None   # Computed from filters, passed to search engine
        self.filter_text_all: list = []     # FTS5 text: all words must match
        self.filter_text_any: list = []     # FTS5 text: any word must match
        self.filter_text_not: list = []     # FTS5 text: exclude these words
        self.word_search_excluded_ids: set = set()  # Per-manuscript exclusions for word search mode
        self.word_search_excluded_results: list = []  # Results hidden by word search exclusion
        # Pre-search measurement filter state (DIM-02, Phase 54)
        self.filter_width_min = None
        self.filter_width_max = None
        self.filter_height_min = None
        self.filter_height_max = None
        self.filter_line_count_min = None
        self.filter_line_count_max = None
        self.filter_line_height_min = None
        self.filter_line_height_max = None
        self.filter_text_density_min = None
        self.filter_text_density_max = None
        self.filter_measurement_material: list = []
        # Post-search measurement filter state (DIM-03, Phase 54) -- SEPARATE from pre-search
        self.post_filter_width_min = None
        self.post_filter_width_max = None
        self.post_filter_height_min = None
        self.post_filter_height_max = None
        self.post_filter_line_count_min = None
        self.post_filter_line_count_max = None
        self.post_filter_line_height_min = None
        self.post_filter_line_height_max = None
        self.post_filter_text_density_min = None
        self.post_filter_text_density_max = None
        self.post_filter_measurement_material: list = []
        self._measurement_cache: dict = {}  # {sys_id: summary_dict}
        # Translation enrichment (Phase 46)
        self.translation_data: dict = {}  # sys_id -> {description_he, document_type_he}
        self.title_translations: dict = {}  # sys_id -> {original_title, english_title, hebrew_title, source}
        self.search_generation: int = 0  # Monotonic counter to discard stale background enrichment
        # Refinement chain state (Phase 55 -- search within results)
        self.refinement_chain: list = []               # list of RefinementStep (the chain)
        self.refinement_restrict_sys_ids: set = None   # sys_ids from last chain step (RAW results, not post-filtered)
        self._refine_mode: bool = False                # True when user clicked "Search within" and is entering query
        self._refinement_stale: bool = False           # True when filters changed during active chain (D-16)
        self._refinement_scope_sig: str = ''           # scope_signature at time of last chain step creation
        self._zero_result_refine: bool = False         # True when last refine returned 0 results (D-14a)
        self._all_terms_filter: bool = False             # "Only results with all terms" checkbox state
        # Visual Similarity restriction state (Phase 57)
        self.vs_restrict_sys_ids: set = None   # Partner sys_ids from visual suggestions
        self.vs_restrict_label: str = None     # Display label for VS breadcrumb
        self.vs_restrict_source_ids: list = [] # Source manuscript sys_ids that generated the restrict set
        self.vs_restrict_mode: str = 'union'   # 'union' or 'intersection'
        self.vs_availability: dict = {}        # sys_id -> bool, batch VS availability for current results
        self.vs_browse_mode: bool = False      # True = show pool as results without text query
        # Manuscript exclusion state (Phase 56 -- exclude known manuscripts)
        self.exclusion_sources: list = []                    # list of ExclusionSource objects
        self.manuscript_excluded_results: list = []          # Results hidden by manuscript exclusion [{result, reason}]
        self._exclusion_shelf_map: dict | None = None        # Lazy-built norm->sys_id map for file resolution
        # Dynamic render-state attributes (Pitfall 4: initialized here for clarity)
        self.expanded_index = None              # Index of currently expanded result card
        self.expansion_refs = {}                # {index: ui_element} for expanded card containers
        self._lazy_loaders = {}                 # {index: callable} for deferred text loading


# ---------------------------------------------------------------------------
# AdvancedViewState -- state for the advanced/quick-view dialog
# ---------------------------------------------------------------------------

class AdvancedViewState:
    """State holder for the Advanced View dialog to enable in-place updates."""
    def __init__(self):
        self.current_result_idx: int = 0
        self.results: List[dict] = []
        self.current_sys_id: Optional[str] = None
        self.current_p_num: int = 1
        self.current_fl_id: Optional[str] = None
        self.total_pages: int = 1
        self.current_page: Optional[BrowsePage] = None
        self.show_image_panel: bool = True
        self.zoom_level: float = 1.0
        self.rotation: int = 0
        self.is_fullscreen: bool = False  # Fullscreen mode
        # Edit mode state (inline editing like browse.py)
        self.edit_mode: bool = False
        self.edit_text: str = ""
        self.edit_notes: str = ""
        self.original_edit_text: str = ""
        self.draft_saved: bool = False
        self.draft_id: Optional[str] = None
        # Enrichment data (FJMS + crossref)
        self.fjms_data: Optional[dict] = None
        self.crossref_data: Optional[dict] = None
        # Volume-aware browse (multi-IE manuscripts)
        self.volume_ie: Optional[str] = None
        # Highlighted search terms for re-application on version change
        self.highlight_terms: List[str] = []
        # UI element references for in-place updates
        self.result_label = None
        self.score_badge = None
        self.prev_btn = None
        self.next_btn = None
        self.content_container = None
        self.image_container = None
        # Additional UI refs set during dialog construction
        self.header_container = None
        self.info_bar_container = None
        self.brightness_sl = None


# ---------------------------------------------------------------------------
# SearchPageRefs -- UI element and callback references for extracted functions
# ---------------------------------------------------------------------------

@dataclass
class SearchPageRefs:
    """UI element references and callbacks needed by extracted search_results functions.

    Populated in create_search_page() after all UI elements and callbacks are defined.
    Plan 02 will wire this up; Plan 01 only defines the dataclass.
    """
    results_container: Any           # ui.scroll_area
    query_input: Any                 # ui.input
    page_client: Any                 # ui.context.client

    page_size: int = 50              # PAGE_SIZE constant

    # Callback functions (set after definition in create_search_page)
    update_search_within_btn: Any = None
    update_refinement_strip: Any = None
    undo_zero_result_refine: Any = None
    apply_word_search_exclusions_and_render: Any = None
    update_selection_ui: Any = None
    show_add_to_list_dialog: Any = None
    copy_result_text: Any = None
    domain_display_name: Any = None


# ---------------------------------------------------------------------------
# Search history management (reads/writes app.storage.user)
# ---------------------------------------------------------------------------

def get_search_history() -> list:
    """Get search history from storage."""
    return app.storage.user.get('search_history', [])


def add_to_search_history(query: str, result_count: int, mode: str, params: dict, state_snapshot: dict):
    """Add or update a search history entry. Deduplicates by query+mode."""
    if not app.storage.user.get('session_persistence_enabled', True):
        return
    limit = app.storage.user.get('search_history_limit', 20)
    history = get_search_history()

    # Dedup: check for existing entry with same query + mode
    existing_idx = None
    for i, entry in enumerate(history):
        if entry.get('query') == query and entry.get('mode') == mode:
            existing_idx = i
            break

    entry = {
        'query': query,
        'result_count': result_count,
        'mode': mode,
        'timestamp': datetime.now().isoformat(),
        'params': params,
        'state': state_snapshot,
    }

    if existing_idx is not None:
        history.pop(existing_idx)  # Remove old position
        history.insert(0, entry)   # Move to front with updated data
    else:
        history.insert(0, entry)   # Add at front (newest first)

    # Enforce limit
    history = history[:limit]
    app.storage.user['search_history'] = history


def delete_search_history_entry(index: int):
    """Delete a specific history entry by index."""
    history = get_search_history()
    if 0 <= index < len(history):
        history.pop(index)
        app.storage.user['search_history'] = history


def clear_search_history():
    """Clear all search history."""
    app.storage.user['search_history'] = []


# ---------------------------------------------------------------------------
# Domain display name helper
# ---------------------------------------------------------------------------

def domain_display_name(search_state: SearchUIState, en_name: str) -> str:
    """Get display name for a domain (Hebrew if UI is Hebrew, else English)."""
    if get_language() == 'he':
        if en_name in search_state.domain_name_map:
            return search_state.domain_name_map[en_name]
        # Fall back to tr() for non-FJMS labels like 'Uncategorized'
        translated = tr(en_name)
        if translated != en_name:
            return translated
    return en_name
