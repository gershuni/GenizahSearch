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
from web.safe_storage import safe_user_get, safe_user_set, safe_user_pop
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
# Phase 74: Page-scoped snapshot helpers (D-06)
# ---------------------------------------------------------------------------
#
# These are the SOLE owners of the restorable_page_snapshot storage keys
# for the search page. Direct app.storage.user writes to any of the keys in
# _SEARCH_SNAPSHOT_KEYS elsewhere in the codebase is a violation after
# Phase 74 (D-03, D-05, D-08).

_SEARCH_SNAPSHOT_VERSION = 1
_SEARCH_ACTIVE_TAB_VERSION = 1
_SEARCH_ACTIVE_TAB_KEY = 'search_active_snapshot'
_SEARCH_ACTIVE_USER_FALLBACK_LIMIT = 250

# Legacy keys owned by these helpers (D-08 - no format change).
# Bootstrap-input keys (search_mode, search_query, search_preset,
# search_max_changes, search_gap, search_text_position) are NOT in this set -
# they feed resolve_search_bootstrap and are not SearchUIState fields
# (per must_haves.truths[0]). Migration of bootstrap-input keys is explicitly
# OUT OF SCOPE for Plan 74-01 (review-revision: Codex HIGH #1).
# Filter keys are NOT here either - they are owned by filter_panel.load_filter_state
# / persist_value and cleared separately by clear_search_snapshot.
_SEARCH_SNAPSHOT_KEYS = (
    'domain_exclusions', 'search_printed_filter',
    'word_search_excluded_ids', 'search_exclusion_sources',
    'search_refinement_chain', 'search_results',
    'search_all_terms_filter',
)

# Filter keys cleared by clear_search_snapshot (read/written by filter_panel).
_SEARCH_FILTER_KEYS = (
    'search_filter_domains', 'search_filter_authors', 'search_filter_works',
    'search_filter_include_mode', 'search_filter_date_from',
    'search_filter_date_to', 'search_filter_material_exclude',
    'search_filter_text_all', 'search_filter_text_any', 'search_filter_text_not',
)
_SEARCH_FILTER_MEASUREMENT_KEYS = (
    'search_filter_width_min', 'search_filter_width_max',
    'search_filter_height_min', 'search_filter_height_max',
    'search_filter_line_count_min', 'search_filter_line_count_max',
    'search_filter_line_height_min', 'search_filter_line_height_max',
    'search_filter_text_density_min', 'search_filter_text_density_max',
    'search_filter_measurement_material',
)


def _get_tab_storage():
    """Return tab storage when available, else None."""
    try:
        return app.storage.tab
    except Exception:
        return None


def _compact_result_rows(results: list) -> list:
    """Strip heavy text fields before persisting result rows."""
    compacted = []
    for r in results or []:
        sr = dict(r) if isinstance(r, dict) else r
        if isinstance(sr, dict):
            sr.pop('full_text', None)
            disp = sr.get('display')
            if disp and isinstance(disp, dict):
                d = dict(disp)
                d.pop('full_text', None)
                sr['display'] = d
        compacted.append(sr)
    return compacted


def get_search_active_snapshot() -> dict:
    """Return the current same-tab active search snapshot, if present."""
    tab = _get_tab_storage()
    if tab is None:
        return {}
    raw = tab.get(_SEARCH_ACTIVE_TAB_KEY)
    if not isinstance(raw, dict):
        return {}
    if raw.get('version') != _SEARCH_ACTIVE_TAB_VERSION:
        return {}
    return raw


def restore_search_active_snapshot(state: 'SearchUIState') -> bool:
    """Restore active same-tab search state from tab storage."""
    raw = get_search_active_snapshot()
    if not raw:
        return False
    state.results = raw.get('results', []) or []
    state.printed_filter = raw.get('printed_filter', 'all')
    _de = raw.get('domain_exclusions')
    state.domain_exclusions = set(_de) if _de else set()
    from shared.refinement import RefinementStep
    raw_chain = raw.get('search_refinement_chain', []) or []
    try:
        state.refinement_chain = [RefinementStep.from_dict(d) for d in raw_chain]
    except Exception:
        state.refinement_chain = []
    state.exclusion_sources = raw.get('search_exclusion_sources', []) or []
    return True


def persist_search_active_snapshot(state: 'SearchUIState') -> None:
    """Persist active same-tab search results outside long-lived user storage."""
    tab = _get_tab_storage()
    if tab is None:
        return
    try:
        tab[_SEARCH_ACTIVE_TAB_KEY] = {
            'version': _SEARCH_ACTIVE_TAB_VERSION,
            'results': _compact_result_rows((state.results or [])[:1000]),
            'printed_filter': state.printed_filter,
            'domain_exclusions': list(state.domain_exclusions or []),
            'search_refinement_chain': [s.to_dict() for s in (state.refinement_chain or [])],
            'search_exclusion_sources': list(state.exclusion_sources or []),
        }
    except Exception:
        pass


def clear_search_active_snapshot() -> None:
    """Drop transient same-tab search state."""
    tab = _get_tab_storage()
    if tab is None:
        return
    try:
        tab.pop(_SEARCH_ACTIVE_TAB_KEY, None)
    except Exception:
        pass


def restore_search_snapshot(state: 'SearchUIState') -> None:
    """Hydrate page-scoped state from app.storage.user snapshot.

    Called once at page mount. After this call, SearchUIState is authoritative -
    direct app.storage.user reads for snapshot keys are forbidden (D-03).
    Silently discards snapshot if version stamp is missing or stale (D-04).

    Does NOT hydrate filter_* keys - callers still use
    filter_panel.load_filter_state(state, 'search') for those.

    NOTE (tab stomping limitation): the version stamp prevents cross-version
    corruption but does NOT prevent Tab B overwriting Tab A's snapshot.
    True per-tab isolation is deferred (Codex W3).
    """
    stored_version = safe_user_get('search_snapshot_schema_version', 0)
    if stored_version == 0:
        # Pre-Phase-74 snapshots have no version stamp. Adopt the legacy payload
        # once by stamping to current; otherwise returning users would have
        # their search_results / exclusions / refinement chain silently wiped
        # on the first post-upgrade load (Codex review 74-CODEX-REVIEW2.md #1).
        # Class A try/except collapsed — safe_user_set absorbs AssertionError.
        safe_user_set('search_snapshot_schema_version', _SEARCH_SNAPSHOT_VERSION)
    elif stored_version != _SEARCH_SNAPSHOT_VERSION:
        clear_search_snapshot()
        return

    # Class B OUTER try-except PRESERVED — wraps restore_search_active_snapshot
    # call (tab-snapshot read can return arbitrary types/decode errors) and
    # RefinementStep.from_dict iteration (parsing failures, schema drift).
    # M2: each safe_user_get read is INDEPENDENT — a missing search_results
    # does not short-circuit the domain_exclusions read.
    try:
        # Restorable scalar/list fields (match RESEARCH §1.1 bucket).
        if restore_search_active_snapshot(state):
            return
        state.results = safe_user_get('search_results', []) or []
        state.printed_filter = safe_user_get('search_printed_filter', 'all')
        _de = safe_user_get('domain_exclusions')
        state.domain_exclusions = set(_de) if _de else set()
        # refinement_chain (list[dict] -> list[RefinementStep])
        from shared.refinement import RefinementStep
        raw_chain = safe_user_get('search_refinement_chain', []) or []
        try:
            state.refinement_chain = [RefinementStep.from_dict(d) for d in raw_chain]
        except Exception:
            state.refinement_chain = []
        # exclusion sources (list[dict])
        state.exclusion_sources = safe_user_get('search_exclusion_sources', []) or []
        # NOTE: search_mode, search_query, search_preset, search_max_changes,
        # search_gap are read as needed by search.py's bootstrap block
        # (they feed resolve_search_bootstrap). They are not stored on
        # SearchUIState directly in the current codebase.
    except Exception:
        # Defensive: bad snapshot -> reset to defaults.
        pass


def persist_search_snapshot(state: 'SearchUIState') -> None:
    """Serialize restorable fields of SearchUIState to app.storage.user.

    runtime_only and cross_page_preference fields are NOT written.
    Gated by session_persistence_enabled, mirroring filter_panel.persist_value.
    """
    if not safe_user_get('session_persistence_enabled', True):
        return
    # Class B OUTER try-except PRESERVED per Fix 4 in 87-REVIEWS.md (M3):
    # covers list/dict construction (_compact_result_rows, list(state.domain_exclusions),
    # list(state.exclusion_sources)) and persist_search_active_snapshot subcall.
    try:
        safe_user_set('search_snapshot_schema_version', _SEARCH_SNAPSHOT_VERSION)
        persist_search_active_snapshot(state)
        safe_user_set('search_results', _compact_result_rows(
            (state.results or [])[:_SEARCH_ACTIVE_USER_FALLBACK_LIMIT]
        ))
        safe_user_set('search_printed_filter', state.printed_filter)
        safe_user_set('domain_exclusions', list(state.domain_exclusions or []))
        # refinement_chain (list[RefinementStep] -> list[dict])
        # Class B INNER try-except PRESERVED — covers to_dict() iteration which
        # can raise on schema-drift / non-RefinementStep objects in the chain.
        try:
            safe_user_set('search_refinement_chain', [
                s.to_dict() for s in (state.refinement_chain or [])
            ])
        except Exception:
            safe_user_set('search_refinement_chain', [])
        safe_user_set('search_exclusion_sources', list(state.exclusion_sources or []))
    except Exception:
        pass  # Browser storage operation failed; snapshot not persisted (D-08)


def clear_search_snapshot() -> None:
    """Wipe all search snapshot keys from app.storage.user.

    Replaces the scattered blocks at search.py:~805-832 and search.py:~2019-2025.
    Clears both the core snapshot keys (_SEARCH_SNAPSHOT_KEYS) and the filter keys
    (_SEARCH_FILTER_KEYS / _SEARCH_FILTER_MEASUREMENT_KEYS) owned by filter_panel.

    Does NOT touch cross_page_preference keys (session_persistence_enabled,
    search_history, show_translations) or the cross-page signal 'incoming_filters'
    (Pitfall 7).
    """
    # Core snapshot keys: reset to safe defaults.
    # NOTE (review-revision): Bootstrap-input keys (search_query, search_mode,
    # search_preset, search_max_changes, search_gap, search_text_position) are
    # NOT cleared here - they are owned by the bootstrap path. The existing
    # search.py:2019-2025 reset block writes search_query='' / search_mode='exact'
    # for UX reasons (New Search wipes the query bar); those writes STAY in
    # search.py and are NOT migrated into this helper. This preserves the
    # Plan 74-01 scope stated in must_haves.truths[0].
    defaults = {
        'search_results': [],
        'domain_exclusions': [],
        'search_printed_filter': 'all',
        'word_search_excluded_ids': [],
        'search_exclusion_sources': [],
    }
    for key, value in defaults.items():
        # Class A try/except collapsed — safe_user_set absorbs AssertionError.
        safe_user_set(key, value)
    # Remaining snapshot keys: drop them.
    for key in ('search_refinement_chain',
                'search_all_terms_filter', 'search_snapshot_schema_version'):
        safe_user_pop(key, None)
    clear_search_active_snapshot()
    # Filter keys (match search.py:806-832 reset block).
    # Defaults match LIVE field types verified in web/pages/search_state.py:60, :82
    # and web/components/filter_panel.py:248, :271 (review-revision: Codex HIGH #2):
    #   filter_include_mode is bool (default True) - NOT string 'any'
    #   filter_measurement_material is list (default []) - NOT None
    for key in _SEARCH_FILTER_KEYS:
        if key == 'search_filter_include_mode':
            safe_user_set(key, True)  # bool, matches filter_panel.py:248
        elif key in ('search_filter_date_from', 'search_filter_date_to'):
            safe_user_set(key, None)
        else:
            safe_user_set(key, [])
    # Measurement keys: mins/maxes default to None (numeric), material to [].
    for key in _SEARCH_FILTER_MEASUREMENT_KEYS:
        if key == 'search_filter_measurement_material':
            safe_user_set(key, [])  # list, matches filter_panel.py:271
        else:
            safe_user_set(key, None)  # numeric min/max


def clear_search_filters() -> None:
    """Reset only pre-search filter storage keys (Advanced 'Clear All' filters).

    Narrower than clear_search_snapshot: preserves live search state
    (search_results, domain_exclusions, search_printed_filter, refinement chain,
    exclusion sources). Intended for the Advanced Filters 'Clear All' button,
    which should not wipe results / exclusions already on screen
    (Codex review 74-CODEX-REVIEW2.md #3).
    """
    for key in _SEARCH_FILTER_KEYS:
        if key == 'search_filter_include_mode':
            safe_user_set(key, True)
        elif key in ('search_filter_date_from', 'search_filter_date_to'):
            safe_user_set(key, None)
        else:
            safe_user_set(key, [])
    for key in _SEARCH_FILTER_MEASUREMENT_KEYS:
        if key == 'search_filter_measurement_material':
            safe_user_set(key, [])
        else:
            safe_user_set(key, None)


# ---------------------------------------------------------------------------
# Search history management (reads/writes app.storage.user)
# ---------------------------------------------------------------------------

def get_search_history() -> list:
    """Get search history from storage."""
    return safe_user_get('search_history', [])


def add_to_search_history(query: str, result_count: int, mode: str, params: dict, state_snapshot: dict):
    """Add or update a search history entry. Deduplicates by query+mode."""
    if not safe_user_get('session_persistence_enabled', True):
        return
    limit = safe_user_get('search_history_limit', 20)
    history = get_search_history()

    # Dedup: check for existing entry with same query + mode
    existing_idx = None
    for i, entry in enumerate(history):
        if entry.get('query') == query and entry.get('mode') == mode:
            existing_idx = i
            break

    compact_state = dict(state_snapshot or {})
    compact_state.pop('results', None)

    entry = {
        'query': query,
        'result_count': result_count,
        'mode': mode,
        'timestamp': datetime.now().isoformat(),
        'params': params,
        'state': compact_state,
    }

    if existing_idx is not None:
        history.pop(existing_idx)  # Remove old position
        history.insert(0, entry)   # Move to front with updated data
    else:
        history.insert(0, entry)   # Add at front (newest first)

    # Enforce limit
    history = history[:limit]
    safe_user_set('search_history', history)


def delete_search_history_entry(index: int):
    """Delete a specific history entry by index."""
    history = get_search_history()
    if 0 <= index < len(history):
        history.pop(index)
        safe_user_set('search_history', history)


def clear_search_history():
    """Clear all search history."""
    safe_user_set('search_history', [])


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
