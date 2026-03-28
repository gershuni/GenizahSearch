# -*- coding: utf-8 -*-
"""
Advanced Search Page - Dicta Genizah Search

A comprehensive search interface with:
- Multiple search modes (exact, variants, fuzzy, regex, shelfmark, title)
- Advanced filtering options
- Lab Mode integration
- Real-time results with pagination
- Export capabilities
"""

from nicegui import ui, run, app
from web.state import state
from web.translations import tr, is_rtl, get_language
from web.feature_flags import WEB_PUZZLE_ENABLED
from web.components.typography import h1, h2, h3, h4
from web.components.filter_panel import (
    build_domain_options, build_author_options, build_work_options,
    build_filter_summary, has_active_filters, persist_value,
    load_filter_state, consume_incoming_filters, recompute_filter_count,
    create_filter_handlers,
)
from web.services import (
    get_service,
    BrowsePage,
    get_oxford_direct_image_url,
    is_oxford_manuscript,
)
from web.search_bootstrap import resolve_search_bootstrap
from genizah_core import SearchEngine, get_library_display, generate_tabular_syntax
from shared.refinement import RefinementStep, compute_effective_restrict, needs_mode_labels, truncate_chain, replay_chain, scope_signature
from web.document_service import get_sys_ids_with_transcriptions, get_all_sources_for_fragment, get_document_for_fragment, get_section_for_page, get_fragments_by_tag, get_all_distinct_tags
from urllib.parse import quote
from typing import Optional, List, Dict, Any, Set
from dataclasses import dataclass, field
import logging
import re
import html
import asyncio
import time
from datetime import datetime

logger = logging.getLogger(__name__)


def create_search_page(initial_query: str = None, initial_tag: str = None,
                       initial_mode: str = None, initial_variants: int = None,
                       initial_ja: int = None, initial_flex_spaces: int = None,
                       initial_bidirectional: int = None, initial_domain: str = None,
                       from_browse: int = None):
    """Create the advanced search page."""


    # === State Management ===
    PAGE_SIZE = 50  # Results per page for pagination

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

    search_state = SearchUIState()

    # Helper: deferred async callback without ui.timer (avoids parent_slot RuntimeError on navigation)
    # Capture NiceGUI client for deferred async tasks (slot context)
    _page_client = ui.context.client

    async def _after_delay(delay, coro_func, *args):
        """Schedule an async callback after a delay, with NiceGUI client context."""
        await asyncio.sleep(delay)
        try:
            with _page_client:
                await coro_func(*args)
        except RuntimeError as e:
            if 'slot stack' not in str(e) and 'deleted' not in str(e):
                logger.error("_after_delay error in %s: %s", coro_func.__name__, e)
        except Exception as e:
            logger.error("_after_delay error in %s: %s", coro_func.__name__, e, exc_info=True)

    # Measurement material options (value -> translated label)
    MEASUREMENT_MATERIALS = {
        'Paper': tr('Paper'),
        'Vellum': tr('Vellum'),
        'Papyrus': tr('Papyrus'),
        'Mix': tr('Mix'),
        'Wood': tr('Wood'),
    }

    # Resolve which persisted search state, if any, should be reused for this request.
    raw_saved_mode = app.storage.user.get('search_mode', 'exact')
    raw_saved_query = app.storage.user.get('search_query', '')
    saved_preset = app.storage.user.get('search_preset', 30)
    saved_max_changes = app.storage.user.get('search_max_changes', 2)
    saved_gap = app.storage.user.get('search_gap', 0)

    use_slider = False
    if state.lab_engine and hasattr(state.lab_engine, 'settings') and state.lab_engine.settings:
        use_slider = getattr(state.lab_engine.settings, 'variant_use_slider', False)

    bootstrap_state = resolve_search_bootstrap(
        initial_query=initial_query,
        initial_tag=initial_tag,
        initial_mode=initial_mode,
        initial_domain=initial_domain,
        from_browse=from_browse,
        saved_mode=raw_saved_mode,
        saved_query=raw_saved_query,
        use_slider=use_slider,
    )
    saved_mode = bootstrap_state['mode']
    saved_query = bootstrap_state['query']
    restore_saved_results = bootstrap_state['restore_saved_results']
    restore_saved_filters = bootstrap_state['restore_saved_filters']
    restore_saved_exclusions = bootstrap_state['restore_saved_exclusions']

    # Restore domain exclusions / printed filter from storage only for session restores
    if restore_saved_exclusions:
        _de = app.storage.user.get('domain_exclusions')
        search_state.domain_exclusions = set(_de) if _de is not None else set()
        search_state.printed_filter = app.storage.user.get('search_printed_filter', 'all')

    # Clear exclusions if initial_domain provided (from browse page navigation)
    if initial_domain:
        search_state.domain_exclusions = set()
        app.storage.user['domain_exclusions'] = []

    # --- Incoming filters from catalog browse (Path B: browse -> search) ---
    _filters_from_browse = False
    if from_browse:
        _filters_from_browse = consume_incoming_filters(search_state, 'search', require_from_browse=False)

    # Restore filter state from session only when the request itself is not explicit
    if restore_saved_filters and not _filters_from_browse:
        load_filter_state(search_state, 'search')

    # Restore word search excluded ids from session
    if restore_saved_exclusions:
        _wse = app.storage.user.get('word_search_excluded_ids')
        search_state.word_search_excluded_ids = set(_wse) if _wse is not None else set()

    # Phase 55: Restore refinement chain from session (D-14)
    _saved_refinement_chain = app.storage.user.get('search_refinement_chain', [])
    if _saved_refinement_chain and restore_saved_results:
        try:
            search_state.refinement_chain = [RefinementStep.from_dict(d) for d in _saved_refinement_chain]
        except Exception:
            search_state.refinement_chain = []

    def _has_active_filters() -> bool:
        """Check if any pre-search filters are active."""
        return has_active_filters(search_state)

    # --- Search History Management ---
    def _get_search_history() -> list:
        """Get search history from storage."""
        return app.storage.user.get('search_history', [])

    def _add_to_search_history(query: str, result_count: int, mode: str, params: dict, state_snapshot: dict):
        """Add or update a search history entry. Deduplicates by query+mode."""
        if not app.storage.user.get('session_persistence_enabled', True):
            return
        limit = app.storage.user.get('search_history_limit', 20)
        history = _get_search_history()

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

    def _delete_search_history_entry(index: int):
        """Delete a specific history entry by index."""
        history = _get_search_history()
        if 0 <= index < len(history):
            history.pop(index)
            app.storage.user['search_history'] = history

    def _clear_search_history():
        """Clear all search history."""
        app.storage.user['search_history'] = []

    def _domain_display_name(en_name: str) -> str:
        """Get display name for a domain (Hebrew if UI is Hebrew, else English)."""
        from web.translations import get_language
        if get_language() == 'he':
            if en_name in search_state.domain_name_map:
                return search_state.domain_name_map[en_name]
            # Fall back to tr() for non-FJMS labels like 'Uncategorized'
            translated = tr(en_name)
            if translated != en_name:
                return translated
        return en_name

    # State for Advanced View dialog (used for in-place updates)
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
            # Highlighted search terms for re-application on version change
            self.highlight_terms: List[str] = []
            # UI element references for in-place updates
            self.result_label = None
            self.score_badge = None
            self.prev_btn = None
            self.next_btn = None
            self.content_container = None
            self.image_container = None

    # === VIEWER_STYLES for Advanced View image handling (must be at page level) ===
    ADVANCED_VIEWER_STYLES = '''
    <script src="/static/manuscript_viewer.js"></script>
    <script>
    // Create advanced viewer via shared factory
    window.advViewer = createManuscriptViewer({
        imageSelector: '.adv-zoomable-image',
        containerSelector: '.adv-image-container',
        zoomLabelSelector: '.adv-zoom-label',
        gammaFilterId: 'gamma-adv'
    });
    </script>
    <svg style="position:absolute;width:0;height:0"><filter id="gamma-adv"><feComponentTransfer><feFuncR type="gamma" amplitude="1" exponent="1.0"/><feFuncG type="gamma" amplitude="1" exponent="1.0"/><feFuncB type="gamma" amplitude="1" exponent="1.0"/></feComponentTransfer></filter></svg>
    <style>
    .adv-image-container { position: relative; background: #1a1a1a; border-radius: 8px; overflow: hidden; min-height: 400px; display: flex; align-items: center; justify-content: center; }
    .adv-zoomable-image { max-width: 100%; max-height: 100%; object-fit: contain; cursor: grab; transform-origin: center center; transition: transform 0.1s ease-out; }
    .adv-zoomable-image:active { cursor: grabbing; }
    </style>
    '''

    # Restore previous results (transcription lookup deferred to after UI renders)
    if restore_saved_results and 'search_results' in app.storage.user:
        try:
            _sr = app.storage.user.get('search_results')
            search_state.results = _sr if _sr is not None else []
        except Exception:
            pass


    # === UI Layout ===
    # Add Advanced View image handler JavaScript at page level (must be outside dialog)
    ui.add_head_html(ADVANCED_VIEWER_STYLES)

    # Domain filter dialog JS helpers (must be at page level for inline onchange handlers)
    # Functions accept containerId parameter for unique dialog instances
    ui.add_head_html('''<script>
    function domainFilterParentChanged(parentCb) {
        try {
            var children = JSON.parse(parentCb.getAttribute('data-children') || '[]');
            var container = parentCb.closest('[id^="domain-filter-"]');
            if (!container) return;
            for (var i = 0; i < children.length; i++) {
                var childCb = container.querySelector(
                    'input[data-domain="' + CSS.escape(children[i]) + '"]'
                );
                if (childCb) childCb.checked = parentCb.checked;
            }
        } catch(e) { console.error('domainFilterParentChanged:', e); }
    }
    function domainFilterSelectAll(containerId, checked) {
        var container = document.getElementById(containerId);
        if (!container) return;
        var cbs = container.querySelectorAll('input[type="checkbox"]');
        for (var i = 0; i < cbs.length; i++) cbs[i].checked = checked;
    }
    function domainFilterGetExcluded(containerId) {
        var container = document.getElementById(containerId);
        if (!container) return [];
        var excluded = [];
        var cbs = container.querySelectorAll('input[type="checkbox"]');
        for (var i = 0; i < cbs.length; i++) {
            if (!cbs[i].checked) excluded.push(cbs[i].getAttribute('data-domain'));
        }
        return excluded;
    }
    </script>''')

    with ui.column().classes('w-full h-[calc(100vh-88px)] gap-0'):

        # === Search Header Panel (Collapsible) ===
        search_panel_container = ui.column().classes('w-full gap-0 search-panel-container')

        with search_panel_container:
            # --- Collapsed View (Hidden by default) ---
            collapsed_panel = ui.card().classes(
                'w-full px-4 py-2 rounded-none border-0 border-b search-panel-collapsed'
            ).style(
                'background: var(--bg-card); border-color: var(--border-light) !important; display: none;'
            )

            with collapsed_panel:
                with ui.row().classes('w-full items-center justify-between gap-3'):
                    # Left: Query summary
                    with ui.row().classes('items-center gap-3 flex-grow min-w-0'):
                        ui.icon('search').classes('text-lg').style('color: var(--primary-600);')
                        collapsed_query_label = ui.label('').classes(
                            'text-sm font-medium truncate'
                        ).style('color: var(--text-primary); direction: rtl; max-width: 300px;')
                        collapsed_mode_badge = ui.badge('').props('outline').classes('text-xs')

                    # Right: Search button and expand
                    with ui.row().classes('items-center gap-2 shrink-0'):
                        collapsed_search_btn = ui.button(
                            tr('Search'), icon='search', on_click=lambda: execute_search()
                        ).classes('btn-primary h-8 px-4 text-sm')

                        def expand_panel():
                            search_state.is_panel_collapsed = False
                            collapsed_panel.style('background: var(--bg-card); border-color: var(--border-light) !important; display: none !important;')
                            expanded_panel.style('background: var(--bg-card); border-color: var(--border-light) !important; display: block !important;')

                        ui.button(
                            icon='expand_more', on_click=expand_panel
                        ).props('flat round dense size=sm').tooltip(tr('Expand search options'))

            # --- Expanded View (Full panel) ---
            expanded_panel = ui.card().classes(
                'w-full p-6 rounded-none border-0 border-b search-panel-expanded'
            ).style(
                'background: var(--bg-card); border-color: var(--border-light) !important;'
            )

            with expanded_panel:
                # Collapse toggle button (top-right corner)
                with ui.row().classes('w-full justify-end mb-2'):
                    def collapse_panel():
                        search_state.is_panel_collapsed = True
                        # Update collapsed view with current values
                        query_val = query_input.value or ''
                        collapsed_query_label.text = query_val[:50] + ('...' if len(query_val) > 50 else '') if query_val else tr('Enter search query')
                        mode_val = mode_select.value
                        mode_names = {'exact': '=', 'variants': '?', 'variants_extended': '??',
                                     'variants_maximum': '???', 'fuzzy': '~', 'Regex': '/',
                                     'Shelfmark': '#', 'Title': '$', 'pgp_tags': 'PGP'}
                        collapsed_mode_badge.text = mode_names.get(mode_val, mode_val)
                        expanded_panel.style('display: none !important;')
                        collapsed_panel.style('background: var(--bg-card); border-color: var(--border-light) !important; display: block !important;')

                    ui.button(
                        icon='expand_less', on_click=collapse_panel
                    ).props('flat round dense size=sm').tooltip(tr('Collapse search panel'))

                # Main Search Row
                with ui.row().classes('w-full items-end gap-4 flex-wrap'):

                    # Search Input (Main) — swaps between text input and tag select
                    with ui.column().classes('flex-grow min-w-80 gap-1') as query_column:
                        # Changed to H2 semantic label
                        query_label = h2(tr('Search Query'), classes='text-sm font-medium', style='color: var(--text-secondary);')
                        query_input = ui.input(
                            placeholder=tr('Enter Hebrew text to search'),
                            value=saved_query
                        ).classes('w-full text-lg').props('outlined dense clearable').style('direction: rtl;')
                        query_input.on('keydown.enter', lambda: execute_search())

                        # --- Live shortcut detection: switch mode when user types prefix + space ---
                        _shortcut_prefixes = [
                            ('???', 'variants_maximum'),
                            ('??', 'variants_extended'),
                            ('R', 'responsa'),
                            ('?', 'variants'),
                            ('=', 'exact'),
                            ('~', 'fuzzy'),
                            ('/', 'Regex'),
                            ('$', 'Title'),
                            ('#', 'Shelfmark'),
                        ]

                        def on_query_input_change():
                            val = query_input.value or ''
                            # Check if text starts with a shortcut followed by a space
                            for prefix, target_mode in _shortcut_prefixes:
                                if val.startswith(prefix + ' ') or val == prefix + ' ':
                                    # Strip the prefix and leading space
                                    clean = val[len(prefix):].lstrip()
                                    query_input.value = clean
                                    # Switch mode (map variant subtypes for slider)
                                    if use_slider and target_mode in ('variants_extended', 'variants_maximum'):
                                        mode_select.value = 'variants'
                                    else:
                                        mode_select.value = target_mode
                                    # Explicitly trigger mode change handler — programmatic
                                    # value assignment doesn't fire Vue 'update:model-value'
                                    on_mode_change()
                                    break
                        query_input.on('update:model-value', on_query_input_change)
                        # Fire shortcut detection immediately on space press (avoids
                        # one-keystroke delay from NiceGUI's batched model-value updates)
                        query_input.on('keyup.space', on_query_input_change)

                        # Save query on change
                        def save_query():
                            app.storage.user['search_query'] = query_input.value or ''
                        query_input.on('blur', save_query)

                    # Phase 55: Refine mode badge (D-02) + cancel (D-02a)
                    refine_badge = ui.chip('', icon='filter_list', color='amber-3').classes('text-sm')
                    refine_badge.set_visibility(False)
                    refine_cancel_btn = ui.button(tr('Cancel'), icon='close',
                        on_click=lambda: _exit_refine_mode()
                    ).classes('text-xs').props('flat dense no-caps')
                    refine_cancel_btn.set_visibility(False)

                    # Tag Select (for PGP Tags mode) — hidden by default
                    with ui.column().classes('flex-grow min-w-80 gap-1') as tag_column:
                        h2(tr('PGP Tags'), classes='text-sm font-medium', style='color: var(--success-600);')
                        tag_select = ui.select(
                            [], with_input=True, value=None
                        ).classes('w-full text-lg').props('outlined dense clearable use-input input-debounce="200"')
                        tag_select.props(f'popup-content-class="max-h-64" label="{tr("Select a tag...")}"')
                    tag_column.set_visibility(False)

                    # Load PGP tags asynchronously with categorized Hebrew translations
                    async def load_pgp_tags():
                        from pgp_tag_translations import get_categorized_tags_for_display
                        from web.translations import get_language
                        tags = await run.io_bound(get_all_distinct_tags)
                        lang = get_language()
                        categorized = get_categorized_tags_for_display(tags, lang)
                        # NiceGUI dict format: {value: label} — category headers as visual separators
                        opts = {}
                        for header, display, en_tag in categorized:
                            if en_tag == "":
                                # Visual separator — use unique non-colliding key
                                opts[f'__sep_{header}'] = display
                            else:
                                opts[en_tag] = display
                        tag_select.options = opts
                        tag_select.update()
                    asyncio.ensure_future(_after_delay(0.1, load_pgp_tags))

                    # Mode Selector - includes variant levels when not using slider
                    with ui.column().classes('gap-1'):
                        h3(tr('Mode'), classes='text-sm font-medium', style='color: var(--text-secondary);')

                        _responsa_label = tr('Responsa') + ' (R)'

                        if use_slider:
                            # Slider mode: single variants option, level controlled by slider
                            mode_options = {
                                'exact': tr('Exact') + ' (=)',
                                'variants': tr('Variants') + ' (?)',
                                'responsa': _responsa_label,
                                'fuzzy': tr('Fuzzy') + ' (~)',
                                'Regex': tr('Regex') + ' (/)',
                                'Shelfmark': tr('Shelfmark') + ' (#)',
                                'Title': tr('Title') + ' ($)',
                                'pgp_tags': tr('PGP Tags'),
                            }
                        else:
                            # Preset mode: separate variant levels in dropdown
                            mode_options = {
                                'exact': tr('Exact') + ' (=)',
                                'variants': tr('Variants Basic') + ' (?)',
                                'variants_extended': tr('Variants Extended') + ' (??)',
                                'variants_maximum': tr('Variants Maximum') + ' (???)',
                                'responsa': _responsa_label,
                                'fuzzy': tr('Fuzzy') + ' (~)',
                                'Regex': tr('Regex') + ' (/)',
                                'Shelfmark': tr('Shelfmark') + ' (#)',
                                'Title': tr('Title') + ' ($)',
                                'pgp_tags': tr('PGP Tags'),
                            }

                        mode_select = ui.select(
                            mode_options,
                            value=saved_mode
                        ).classes('w-48').props('outlined dense')


                    # Track current preset level based on mode
                    def get_level_from_mode(mode):
                        """Get variant level from mode name."""
                        if mode == 'variants_extended':
                            return 70
                        elif mode == 'variants_maximum':
                            return 150
                        elif mode == 'variants':
                            return 30
                        return 30

                    raw_preset = saved_preset if saved_preset else get_level_from_mode(saved_mode)
                    current_preset = {'value': raw_preset}

                    # Create variables for elements (needed for callbacks)
                    variant_slider = None
                    max_changes_select = None

                    # Max Changes selector (visible for all variant modes in non-slider mode)
                    with ui.column().classes('gap-1') as max_changes_col:
                        h3(tr('Num Changes'), classes='text-sm font-medium', style='color: var(--text-secondary);')
                        max_changes_select = ui.select({1: '×1', 2: '×2', 3: '×3'}, value=saved_max_changes).classes('w-16').props('outlined dense')
                        ui.tooltip(tr('Max character changes per word'))

                    # Show max changes only for variant modes when not using slider
                    is_variant_mode = saved_mode in ('variants', 'variants_extended', 'variants_maximum')
                    max_changes_col.set_visibility(is_variant_mode and not use_slider)

                    def set_level(level_value):
                        """Set variant level."""
                        current_preset['value'] = level_value
                        app.storage.user['search_preset'] = level_value
                        if state.var_mgr:
                            state.var_mgr.set_variant_level(level_value)


                    # Gap Control - restore from storage
                    with ui.column().classes('gap-1'):
                        # Changed to H3 semantic label
                        h3(tr('Gap'), classes='text-sm font-medium', style='color: var(--text-secondary);')
                        gap_input = ui.number(value=saved_gap, min=0, max=10).classes('w-20').props('outlined dense')
                        ui.tooltip(tr('Gap description'))

                        def save_gap():
                            app.storage.user['search_gap'] = int(gap_input.value or 0)
                        gap_input.on('blur', save_gap)

                    # Search/Stop Button Container - buttons swap in same position
                    with ui.column().classes('items-center gap-0'):
                        # Search Button
                        search_btn = ui.button(tr('Search'), icon='search', on_click=lambda: execute_search()).classes(
                            'btn-primary h-10 px-8'
                        )

                        # Stop Button (hidden by default) - replaces search button
                        stop_btn = ui.button(
                            tr('Stop'),
                            icon='stop',
                            on_click=lambda: cancel_search()
                        ).classes('h-10 px-4').style('display: none;').props('outline color=red')
                        stop_btn.tooltip(tr('Stops the search and shows partial results'))

                    # New Search (reset) button
                    with ui.column().classes('items-center gap-0'):
                        ui.button(icon='restart_alt', on_click=lambda: _reset_search()).props(
                            'flat dense round'
                        ).tooltip(tr('New Search'))

                    # Search History Button + Menu
                    with ui.column().classes('items-center gap-0'):
                        history_btn = ui.button(icon='history', on_click=lambda: (
                            _refresh_history_menu(), history_menu.open()
                        )).props('flat dense').tooltip(tr('Search History'))

                        history_menu = ui.menu()

                # === Responsa Sub-Options (visible only when mode is 'responsa') ===
                responsa_sub_row = ui.row().classes('w-full items-center gap-4 px-2 flex-wrap')
                with responsa_sub_row:
                    # Sub-checkboxes for Responsa mode
                    responsa_variants_cb = ui.checkbox(tr('Variants'))
                    responsa_variants_cb.tooltip(tr('Include spelling variant pairs'))

                    responsa_ja_cb = ui.checkbox(tr('Judeo-Arabic'))
                    responsa_ja_cb.tooltip(tr('Expand with Judeo-Arabic article forms (al-)'))

                    responsa_flex_cb = ui.checkbox(tr('Flex Spacing'))
                    responsa_flex_cb.tooltip(tr('Allow flexible spacing between characters (helps with OCR text)'))

                    bidirectional_cb = ui.checkbox(tr('Bidirectional Gap'))
                    bidirectional_cb.tooltip(tr('Search for words in either order'))

                    # Syntax legend
                    with ui.row().classes('items-center gap-1 ml-4'):
                        ui.icon('help_outline').classes('text-sm').style('color: var(--text-muted);')
                        ui.label(tr('Syntax:')).classes('text-xs font-medium').style('color: var(--text-muted);')
                        ui.label('#מילה').classes('text-xs').style('color: var(--primary-600);').tooltip(tr('prefix'))
                        ui.label('מילה#').classes('text-xs').style('color: var(--primary-600);').tooltip(tr('suffix'))
                        ui.label('%מילה').classes('text-xs').style('color: var(--primary-600);').tooltip(tr('plene'))
                        ui.label('*מילה / מילה*').classes('text-xs').style('color: var(--primary-600);').tooltip(tr('wildcard'))
                        ui.label('(א/ב)').classes('text-xs').style('color: var(--primary-600);').tooltip(tr('OR'))
                        ui.label('-מילה').classes('text-xs').style('color: var(--primary-600);').tooltip(tr('Exclude'))
                        ui.label('|מילה').classes('text-xs').style('color: var(--primary-600);').tooltip(tr('Line starts'))
                        ui.label('מילה|').classes('text-xs').style('color: var(--primary-600);').tooltip(tr('Line ends'))

                    # Tabular Search button (pushed to right side)
                    ui.space()
                    builder_btn = ui.button(tr('Tabular Search'), icon='grid_view',
                        on_click=lambda: open_query_builder()).classes('ml-auto').props('outline dense')


                # Initially hide if mode is not responsa
                responsa_sub_row.set_visibility(saved_mode == 'responsa')

                # Advanced Options Row (inside expanded_panel, collapses with search bar)
                with ui.expansion(tr('Advanced Options'), icon='tune').classes('w-full mt-4').style(
                    'background: var(--bg-tertiary); border-radius: 12px;'
                ):
                    with ui.column().classes('w-full p-4 gap-6'):
                        # Options Grid
                        with ui.row().classes('w-full gap-8 flex-wrap'):
                            # Lab Mode Section
                            with ui.column().classes('gap-3 min-w-64'):
                                ui.label(tr('Lab Mode')).classes('text-sm font-medium').style('color: var(--text-secondary);')
                                lab_mode = ui.switch(tr('Enable Lab Mode algorithms'))
                                with ui.row().classes('gap-2 items-center'):
                                    deep_scan = ui.checkbox(tr('Deep Scan')).classes('text-sm')
                                    ui.icon('info').classes('text-sm cursor-help').tooltip(
                                        tr('Searches more candidates for comprehensive results')
                                    )

                            # NOT Filter Section
                            with ui.column().classes('gap-3 min-w-64'):
                                ui.label(tr('Exclude Words')).classes('text-sm font-medium').style('color: var(--text-secondary);')
                                not_filter = ui.input(
                                    placeholder=tr('Words to exclude (space separated)')
                                ).classes('w-full').props('outlined dense').style('direction: rtl;')
                                ui.label(tr('Results containing these words will be filtered out')).classes('text-xs').style('color: var(--text-muted);')

                            # Text Position Filter (for join detection)
                            with ui.column().classes('gap-1'):
                                ui.label(tr('Text Position')).classes('text-sm font-medium').style('color: var(--text-secondary);')
                                saved_text_position = app.storage.user.get('search_text_position', 'anywhere')
                                text_position_select = ui.select(
                                    {
                                        'anywhere': tr('Anywhere'),
                                        'start': tr('Start of text'),
                                        'end': tr('End of text'),
                                        'line_start': tr('Line starts'),
                                        'line_end': tr('Line ends'),
                                    },
                                    value=saved_text_position,
                                ).classes('w-40').props('outlined dense')
                                ui.tooltip(tr('Constrain matches to text boundaries (for join detection)'))

                                def save_text_position():
                                    app.storage.user['search_text_position'] = text_position_select.value
                                text_position_select.on('update:model-value', save_text_position)


            # Slider row (separate, OUTSIDE main row, below search) - only when slider mode enabled
            variant_slider_row = None
            if use_slider:
                variant_slider_row = ui.row().classes('w-full items-center gap-4 px-2')
                with variant_slider_row:
                    ui.label(tr('Variant Level')).classes('text-sm font-medium').style('color: var(--text-secondary);')
                    variant_slider = ui.slider(min=10, max=300, value=current_preset['value'], step=10).classes('flex-grow').props('label-always')
                    max_changes_select = ui.select({1: '×1', 2: '×2', 3: '×3'}, value=saved_max_changes).classes('w-20').props('outlined dense')
                    ui.tooltip(tr('Max character changes per word'))
                variant_slider_row.set_visibility(saved_mode == 'variants')

                # Slider change handler
                def on_slider_change():
                    val = int(variant_slider.value)
                    current_preset['value'] = val
                    app.storage.user['search_preset'] = val
                    if state.var_mgr:
                        state.var_mgr.set_variant_level(val)
                variant_slider.on('update:model-value', on_slider_change)

            # Save max changes on change (handle both slider and non-slider modes)
            if max_changes_select:
                def save_max_changes():
                    app.storage.user['search_max_changes'] = int(max_changes_select.value)
                max_changes_select.on('update:model-value', save_max_changes)

            # Mode change handler (must be after variant_slider_row is defined)
            def on_mode_change():
                mode = mode_select.value
                is_variants = mode in ('variants', 'variants_extended', 'variants_maximum')
                is_tags = mode == 'pgp_tags'
                is_responsa = mode == 'responsa'

                if use_slider:
                    # Slider mode: show/hide slider row
                    if variant_slider_row:
                        variant_slider_row.set_visibility(is_variants)
                else:
                    # Preset mode: show/hide max changes column, update level based on mode
                    max_changes_col.set_visibility(is_variants and not is_tags)
                    if is_variants:
                        set_level(get_level_from_mode(mode))

                # Toggle between query input and tag select
                query_column.set_visibility(not is_tags)
                tag_column.set_visibility(is_tags)

                # Responsa sub-options: visible only in Responsa mode
                responsa_sub_row.set_visibility(is_responsa)

                # Save mode to storage (don't persist pgp_tags)
                if not is_tags:
                    app.storage.user['search_mode'] = mode

            mode_select.on('update:model-value', on_mode_change)

        # === Search only in... Filter Panel (BEFORE Advanced Options & progress bar) ===
        _adv_filters_expanded = _has_active_filters() or _filters_from_browse
        adv_filters_panel = ui.expansion(
            text=tr('Search only in...'),
            icon='filter_alt',
            value=_adv_filters_expanded,
        ).classes('w-full').style(
            'background: var(--bg-tertiary); border-bottom: 1px solid var(--border-light);'
        ).props('dense header-class="text-subtitle2 text-weight-medium"')

        _filter_refs = {}

        with adv_filters_panel:
            with ui.column().classes('w-full px-4 py-3 gap-4'):

                # Include/Exclude toggle
                with ui.row().classes('w-full items-center gap-2'):
                    filter_mode_toggle = ui.toggle(
                        {True: tr('Include'), False: tr('Exclude')},
                        value=search_state.filter_include_mode,
                    ).props('dense no-caps size=sm')
                    _filter_refs['mode'] = filter_mode_toggle

                with ui.row().classes('w-full gap-4 flex-wrap items-end'):
                    # Domain filter (multi-select) — options loaded asynchronously after page renders
                    with ui.column().classes('gap-1 min-w-48 flex-grow'):
                        ui.label(tr('Domain')).classes('text-xs font-medium').style('color: var(--text-secondary);')
                        domain_select = ui.select(
                            options={},
                            value=search_state.filter_domains,
                            multiple=True,
                            with_input=True,
                            clearable=True,
                        ).classes('w-full').props('outlined dense use-chips')
                        _filter_refs['domain'] = domain_select

                    # Author filter (multi-select) — options loaded asynchronously after page renders
                    with ui.column().classes('gap-1 min-w-48 flex-grow'):
                        ui.label(tr('Author')).classes('text-xs font-medium').style('color: var(--text-secondary);')
                        author_select = ui.select(
                            options={},
                            value=search_state.filter_authors,
                            multiple=True,
                            with_input=True,
                            clearable=True,
                        ).classes('w-full').props('outlined dense use-chips')
                        _filter_refs['author'] = author_select

                    # Work filter (multi-select) — options loaded asynchronously after page renders
                    with ui.column().classes('gap-1 min-w-48 flex-grow'):
                        ui.label(tr('Work')).classes('text-xs font-medium').style('color: var(--text-secondary);')
                        work_select = ui.select(
                            options={},
                            value=search_state.filter_works,
                            multiple=True,
                            with_input=True,
                            clearable=True,
                        ).classes('w-full').props('outlined dense use-chips')
                        _filter_refs['work'] = work_select

                with ui.row().classes('w-full gap-4 flex-wrap items-end'):
                    # Date range
                    with ui.column().classes('gap-1 min-w-32'):
                        ui.label(tr('Date Range')).classes('text-xs font-medium').style('color: var(--text-secondary);')
                        with ui.row().classes('items-center gap-2'):
                            date_from_input = ui.number(
                                label=tr('From Year'),
                                value=search_state.filter_date_from,
                            ).classes('w-28').props('outlined dense')
                            ui.label('\u2013').style('color: var(--text-muted);')
                            date_to_input = ui.number(
                                label=tr('To Year'),
                                value=search_state.filter_date_to,
                            ).classes('w-28').props('outlined dense')
                        _filter_refs['date_from'] = date_from_input
                        _filter_refs['date_to'] = date_to_input

                    # Material exclude (Printed)
                    with ui.column().classes('gap-1 min-w-48'):
                        ui.label(tr('Material')).classes('text-xs font-medium').style('color: var(--text-secondary);')
                        exclude_printed_cb = ui.checkbox(
                            tr('Exclude Printed'),
                            value='Printed' in search_state.filter_material_exclude,
                        ).props('dense')
                        _filter_refs['exclude_printed'] = exclude_printed_cb

                    # Clear all filters button
                    with ui.column().classes('gap-1 justify-end'):
                        def _clear_all_adv_filters():
                            """Clear all filter selections."""
                            search_state.filter_domains = []
                            search_state.filter_authors = []
                            search_state.filter_works = []
                            search_state.filter_include_mode = True
                            search_state.filter_date_from = None
                            search_state.filter_date_to = None
                            search_state.filter_material_exclude = []
                            search_state.filter_text_all = []
                            search_state.filter_text_any = []
                            search_state.filter_text_not = []
                            search_state.filter_manuscript_count = None
                            search_state.restrict_sys_ids = None
                            domain_select.value = []
                            author_select.value = []
                            work_select.value = []
                            filter_mode_toggle.value = True
                            date_from_input.value = None
                            date_to_input.value = None
                            exclude_printed_cb.value = False
                            if _filter_refs.get('text_input'):
                                _filter_refs['text_input'].value = ''
                            # Reset filter storage to clean defaults
                            app.storage.user['search_filter_domains'] = []
                            app.storage.user['search_filter_authors'] = []
                            app.storage.user['search_filter_works'] = []
                            app.storage.user['search_filter_include_mode'] = True
                            app.storage.user['search_filter_date_from'] = None
                            app.storage.user['search_filter_date_to'] = None
                            app.storage.user['search_filter_material_exclude'] = []
                            app.storage.user['search_filter_text_all'] = []
                            app.storage.user['search_filter_text_any'] = []
                            app.storage.user['search_filter_text_not'] = []
                            # Clear measurement filters (Phase 54)
                            search_state.filter_width_min = None
                            search_state.filter_width_max = None
                            search_state.filter_height_min = None
                            search_state.filter_height_max = None
                            search_state.filter_line_count_min = None
                            search_state.filter_line_count_max = None
                            search_state.filter_line_height_min = None
                            search_state.filter_line_height_max = None
                            search_state.filter_text_density_min = None
                            search_state.filter_text_density_max = None
                            search_state.filter_measurement_material = []
                            for _mk in ['width_min', 'width_max', 'height_min', 'height_max',
                                        'line_count_min', 'line_count_max', 'line_height_min', 'line_height_max',
                                        'text_density_min', 'text_density_max', 'measurement_material']:
                                app.storage.user[f'search_filter_{_mk}'] = None
                            app.storage.user['search_filter_measurement_material'] = []
                            _update_chip_bar()

                        ui.button(tr('Clear All'), icon='clear_all',
                                  on_click=_clear_all_adv_filters).props('flat dense no-caps')

                # Text filter row
                with ui.row().classes('w-full gap-2 items-end'):
                    with ui.column().classes('gap-1 flex-grow'):
                        ui.label(tr('Text Filter')).classes('text-xs font-medium').style('color: var(--text-secondary);')
                        with ui.row().classes('items-center gap-2 w-full'):
                            text_mode_select = ui.select(
                                options={
                                    'all': tr('All words'),
                                    'any': tr('Any word'),
                                    'not': tr('Not these words'),
                                },
                                value='all',
                            ).classes('w-36').props('outlined dense')
                            _filter_refs['text_mode'] = text_mode_select

                            text_filter_input = ui.input(
                                placeholder=tr('Add term'),
                            ).classes('flex-grow').props('outlined dense').on(
                                'keydown.enter', lambda e: _add_text_term()
                            )
                            _filter_refs['text_input'] = text_filter_input

                            ui.button(icon='add', on_click=lambda: _add_text_term()).props('flat dense round')

                    # Display current text filter chips
                    with ui.row().classes('w-full gap-1 flex-wrap') as text_chip_row:
                        _filter_refs['text_chips'] = text_chip_row

                def _add_text_term():
                    """Add a text filter term from the input."""
                    term = text_filter_input.value.strip() if text_filter_input.value else ''
                    if not term:
                        return
                    mode = text_mode_select.value
                    if mode == 'all':
                        if term not in search_state.filter_text_all:
                            search_state.filter_text_all.append(term)
                    elif mode == 'any':
                        if term not in search_state.filter_text_any:
                            search_state.filter_text_any.append(term)
                    elif mode == 'not':
                        if term not in search_state.filter_text_not:
                            search_state.filter_text_not.append(term)
                    text_filter_input.value = ''
                    persist_value('search_filter_text_all', search_state.filter_text_all)
                    persist_value('search_filter_text_any', search_state.filter_text_any)
                    persist_value('search_filter_text_not', search_state.filter_text_not)
                    asyncio.ensure_future(_recompute_filter_count())
                    _update_chip_bar()
                    _rebuild_text_chips()

                def _remove_text_term(mode, term):
                    """Remove a text filter term."""
                    target = getattr(search_state, f'filter_text_{mode}')
                    if term in target:
                        target.remove(term)
                    persist_value(f'search_filter_text_{mode}', target)
                    asyncio.ensure_future(_recompute_filter_count())
                    _update_chip_bar()
                    _rebuild_text_chips()

                def _rebuild_text_chips():
                    """Rebuild text filter chip display."""
                    text_chip_row = _filter_refs.get('text_chips')
                    if not text_chip_row:
                        return
                    text_chip_row.clear()
                    with text_chip_row:
                        for t in search_state.filter_text_all:
                            ui.chip(f"+ {t}", icon='check_circle', removable=True,
                                    color='green-2', on_click=lambda: None,
                            ).on('remove', lambda _t=t: _remove_text_term('all', _t))
                        for t in search_state.filter_text_any:
                            ui.chip(f"~ {t}", icon='help_outline', removable=True,
                                    color='blue-2', on_click=lambda: None,
                            ).on('remove', lambda _t=t: _remove_text_term('any', _t))
                        for t in search_state.filter_text_not:
                            ui.chip(f"- {t}", icon='block', removable=True,
                                    color='red-2', on_click=lambda: None,
                            ).on('remove', lambda _t=t: _remove_text_term('not', _t))

                # Initialize text chips on page load
                _rebuild_text_chips()

                # --- Measurement filters (Phase 54, DIM-02) ---
                with ui.expansion(tr('Measurements'), icon='straighten').classes('w-full').props(
                    'dense default-closed header-class="text-sm"'
                ):
                    with ui.column().classes('gap-2 w-full'):
                        def _make_meas_range_row(label, suffix=''):
                            with ui.row().classes('gap-1 items-center w-full'):
                                ui.label(label).classes('text-xs w-28 shrink-0')
                                min_inp = ui.number(placeholder=tr('Min'), format='%.1f').props(
                                    'outlined dense type=number step=0.1'
                                ).classes('w-20')
                                ui.label('\u2013').classes('text-xs')
                                max_inp = ui.number(placeholder=tr('Max'), format='%.1f').props(
                                    'outlined dense type=number step=0.1'
                                ).classes('w-20')
                                if suffix:
                                    ui.label(suffix).classes('text-xs text-gray-500')
                            return min_inp, max_inp

                        meas_width_min_inp, meas_width_max_inp = _make_meas_range_row(tr('Width'), tr('cm'))
                        meas_height_min_inp, meas_height_max_inp = _make_meas_range_row(tr('Height'), tr('cm'))

                        # Line count: integer inputs (no decimals)
                        with ui.row().classes('gap-1 items-center w-full'):
                            ui.label(tr('Lines')).classes('text-xs w-28 shrink-0')
                            meas_lc_min_inp = ui.number(placeholder=tr('Min'), format='%d').props(
                                'outlined dense type=number step=1'
                            ).classes('w-20')
                            ui.label('\u2013').classes('text-xs')
                            meas_lc_max_inp = ui.number(placeholder=tr('Max'), format='%d').props(
                                'outlined dense type=number step=1'
                            ).classes('w-20')

                        meas_lh_min_inp, meas_lh_max_inp = _make_meas_range_row(tr('Line Height'), tr('mm'))
                        meas_td_min_inp, meas_td_max_inp = _make_meas_range_row(tr('Text Density'), '/10' + tr('cm') + '\u00b2')

                        meas_material_select = ui.select(
                            options=MEASUREMENT_MATERIALS,
                            label=tr('Material (measured)'),
                            multiple=True,
                        ).props('outlined dense clearable use-chips').classes('w-full')

                        # Restore values from state
                        meas_width_min_inp.value = search_state.filter_width_min
                        meas_width_max_inp.value = search_state.filter_width_max
                        meas_height_min_inp.value = search_state.filter_height_min
                        meas_height_max_inp.value = search_state.filter_height_max
                        meas_lc_min_inp.value = search_state.filter_line_count_min
                        meas_lc_max_inp.value = search_state.filter_line_count_max
                        meas_lh_min_inp.value = search_state.filter_line_height_min
                        meas_lh_max_inp.value = search_state.filter_line_height_max
                        meas_td_min_inp.value = search_state.filter_text_density_min
                        meas_td_max_inp.value = search_state.filter_text_density_max
                        meas_material_select.value = search_state.filter_measurement_material

                        # Change handlers -- fire on blur for debounced recompute
                        def _on_meas_blur(attr_name, inp_widget, is_int=False):
                            def handler(e=None):
                                val = inp_widget.value
                                if val is not None and val != '':
                                    val = int(val) if is_int else float(val)
                                else:
                                    val = None
                                setattr(search_state, f'filter_{attr_name}', val)
                                persist_value(f'search_filter_{attr_name}', val)
                                asyncio.ensure_future(_recompute_filter_count())
                                _update_chip_bar()
                            return handler

                        meas_width_min_inp.on('blur', _on_meas_blur('width_min', meas_width_min_inp))
                        meas_width_max_inp.on('blur', _on_meas_blur('width_max', meas_width_max_inp))
                        meas_height_min_inp.on('blur', _on_meas_blur('height_min', meas_height_min_inp))
                        meas_height_max_inp.on('blur', _on_meas_blur('height_max', meas_height_max_inp))
                        meas_lc_min_inp.on('blur', _on_meas_blur('line_count_min', meas_lc_min_inp, is_int=True))
                        meas_lc_max_inp.on('blur', _on_meas_blur('line_count_max', meas_lc_max_inp, is_int=True))
                        meas_lh_min_inp.on('blur', _on_meas_blur('line_height_min', meas_lh_min_inp))
                        meas_lh_max_inp.on('blur', _on_meas_blur('line_height_max', meas_lh_max_inp))
                        meas_td_min_inp.on('blur', _on_meas_blur('text_density_min', meas_td_min_inp))
                        meas_td_max_inp.on('blur', _on_meas_blur('text_density_max', meas_td_max_inp))

                        def _on_meas_material_change(e=None):
                            search_state.filter_measurement_material = meas_material_select.value or []
                            persist_value('search_filter_measurement_material', search_state.filter_measurement_material)
                            asyncio.ensure_future(_recompute_filter_count())
                            _update_chip_bar()
                        meas_material_select.on('update:model-value', _on_meas_material_change)

        # --- Filter chip bar (always visible, even when panel is collapsed) ---
        chip_bar_container = ui.row().classes('w-full px-4 py-1 gap-2 items-center flex-wrap').style(
            'background: var(--bg-tertiary); border-bottom: 1px solid var(--border-light); min-height: 0; margin-bottom: 16px; position: relative; z-index: 1;'
        )
        chip_bar_container.set_visibility(False)

        def _get_display_name(key, opts_dict):
            """Extract display name from options dict (strip trailing count suffix only)."""
            if isinstance(opts_dict, dict) and key in opts_dict:
                import re
                # Strip only the trailing " (N,NNN)" count, preserving qualified names like "Other (Bible)"
                raw = opts_dict[key].lstrip(' \u2514').strip()
                return re.sub(r'\s*\([\d,]+\)\s*$', '', raw).strip()
            return key

        def _update_chip_bar():
            """Rebuild chip bar from current filter state."""
            chip_bar_container.clear()
            has_any = _has_active_filters()
            chip_bar_container.set_visibility(has_any)
            if not has_any:
                return

            mode_prefix = tr('Exclude') + ': ' if not search_state.filter_include_mode else ''
            opts_d = domain_select.options if hasattr(domain_select, 'options') else {}
            opts_a = author_select.options if hasattr(author_select, 'options') else {}
            opts_w = work_select.options if hasattr(work_select, 'options') else {}

            with chip_bar_container:
                # Mode indicator
                if not search_state.filter_include_mode and (
                    search_state.filter_domains or search_state.filter_authors or search_state.filter_works
                ):
                    ui.chip(tr('Exclude selected'), icon='block', color='red-2')

                # Domain chips
                for d in search_state.filter_domains:
                    dname = _get_display_name(d, opts_d)
                    ui.chip(
                        dname, icon='category', removable=True,
                        on_click=lambda: None, color='deep-purple-2',
                    ).on('remove', lambda _d=d: _remove_filter('domain', _d))

                # Author chips
                for a in search_state.filter_authors:
                    aname = _get_display_name(a, opts_a)
                    ui.chip(
                        aname, icon='person', removable=True,
                        on_click=lambda: None, color='blue-2',
                    ).on('remove', lambda _a=a: _remove_filter('author', _a))

                # Work chips
                for w in search_state.filter_works:
                    wname = _get_display_name(w, opts_w)
                    ui.chip(
                        wname, icon='menu_book', removable=True,
                        on_click=lambda: None, color='teal-2',
                    ).on('remove', lambda _w=w: _remove_filter('work', _w))

                # Date range chip
                if search_state.filter_date_from is not None or search_state.filter_date_to is not None:
                    df = search_state.filter_date_from or '...'
                    dt = search_state.filter_date_to or '...'
                    ui.chip(
                        f"{df}\u2013{dt}", icon='date_range', removable=True,
                        on_click=lambda: None, color='orange-2',
                    ).on('remove', lambda: _remove_filter('date'))

                # Material exclude chip
                if search_state.filter_material_exclude:
                    for mat in search_state.filter_material_exclude:
                        ui.chip(
                            f"{tr('Exclude')} {mat}", icon='block', removable=True,
                            on_click=lambda: None, color='red-2',
                        ).on('remove', lambda m=mat: _remove_filter('material', m))

                # Text filter chips
                for t in search_state.filter_text_all:
                    ui.chip(f"+ {t}", icon='check_circle', removable=True,
                            color='green-2', on_click=lambda: None,
                    ).on('remove', lambda _t=t: _remove_text_term('all', _t))
                for t in search_state.filter_text_any:
                    ui.chip(f"~ {t}", icon='help_outline', removable=True,
                            color='blue-2', on_click=lambda: None,
                    ).on('remove', lambda _t=t: _remove_text_term('any', _t))
                for t in search_state.filter_text_not:
                    ui.chip(f"- {t}", icon='block', removable=True,
                            color='red-2', on_click=lambda: None,
                    ).on('remove', lambda _t=t: _remove_text_term('not', _t))

                # Measurement filter chips (Phase 54, teal color, removable)
                def _fmt_range_chip(prefix, vmin, vmax, unit=''):
                    u = f' {tr(unit)}' if unit else ''
                    if vmin is not None and vmax is not None:
                        return f"{prefix}: {vmin}\u2013{vmax}{u}"
                    elif vmin is not None:
                        return f"{prefix}: \u2265{vmin}{u}"
                    elif vmax is not None:
                        return f"{prefix}: \u2264{vmax}{u}"
                    return None
                _meas_chips = [
                    (_fmt_range_chip(tr('Width'), search_state.filter_width_min, search_state.filter_width_max, 'cm'),
                     ['width_min', 'width_max'], [meas_width_min_inp, meas_width_max_inp]),
                    (_fmt_range_chip(tr('Height'), search_state.filter_height_min, search_state.filter_height_max, 'cm'),
                     ['height_min', 'height_max'], [meas_height_min_inp, meas_height_max_inp]),
                    (_fmt_range_chip(tr('Lines'), search_state.filter_line_count_min, search_state.filter_line_count_max),
                     ['line_count_min', 'line_count_max'], [meas_lc_min_inp, meas_lc_max_inp]),
                    (_fmt_range_chip(tr('Line Height'), search_state.filter_line_height_min, search_state.filter_line_height_max, 'mm'),
                     ['line_height_min', 'line_height_max'], [meas_lh_min_inp, meas_lh_max_inp]),
                    (_fmt_range_chip(tr('Text Density'), search_state.filter_text_density_min, search_state.filter_text_density_max),
                     ['text_density_min', 'text_density_max'], [meas_td_min_inp, meas_td_max_inp]),
                ]
                for _chip_text, _attrs, _widgets in _meas_chips:
                    if _chip_text:
                        def _clear_meas(attrs=_attrs, widgets=_widgets):
                            for a in attrs:
                                setattr(search_state, f'filter_{a}', None)
                                persist_value(f'search_filter_{a}', None)
                            for w in widgets:
                                w.value = None
                            _update_chip_bar()
                            asyncio.ensure_future(_recompute_filter_count())
                        ui.chip(
                            _chip_text, icon='straighten', removable=True,
                            on_click=lambda: None, color='teal-2',
                        ).on('remove', _clear_meas)
                for _mat in (search_state.filter_measurement_material or []):
                    def _clear_mat(m=_mat):
                        if m in search_state.filter_measurement_material:
                            search_state.filter_measurement_material.remove(m)
                        meas_material_select.value = search_state.filter_measurement_material
                        persist_value('search_filter_measurement_material', search_state.filter_measurement_material)
                        _update_chip_bar()
                        asyncio.ensure_future(_recompute_filter_count())
                    ui.chip(
                        tr(_mat), icon='layers', removable=True,
                        on_click=lambda: None, color='teal-2',
                    ).on('remove', _clear_mat)

                # Manuscript count badge
                if search_state.filter_manuscript_count is not None:
                    ui.label(
                        f"{search_state.filter_manuscript_count:,} {tr('manuscripts')}"
                    ).classes('text-xs px-2 py-0.5 rounded ml-2').style(
                        'background: var(--bg-tertiary); color: var(--text-secondary); border: 1px solid var(--border-light);'
                    )

        def _remove_filter(filter_type, value=None):
            """Remove a specific filter and update state."""
            if filter_type == 'domain':
                if value and value in search_state.filter_domains:
                    search_state.filter_domains.remove(value)
                else:
                    search_state.filter_domains = []
                domain_select.value = search_state.filter_domains
                persist_value('search_filter_domains', search_state.filter_domains)
                asyncio.ensure_future(_refresh_author_options())
                asyncio.ensure_future(_refresh_work_options())
            elif filter_type == 'author':
                if value and value in search_state.filter_authors:
                    search_state.filter_authors.remove(value)
                else:
                    search_state.filter_authors = []
                author_select.value = search_state.filter_authors
                persist_value('search_filter_authors', search_state.filter_authors)
                asyncio.ensure_future(_refresh_work_options())
            elif filter_type == 'work':
                if value and value in search_state.filter_works:
                    search_state.filter_works.remove(value)
                else:
                    search_state.filter_works = []
                work_select.value = search_state.filter_works
                persist_value('search_filter_works', search_state.filter_works)
            elif filter_type == 'date':
                search_state.filter_date_from = None
                search_state.filter_date_to = None
                date_from_input.value = None
                date_to_input.value = None
                persist_value('search_filter_date_from', None)
                persist_value('search_filter_date_to', None)
            elif filter_type == 'material':
                if value and value in search_state.filter_material_exclude:
                    search_state.filter_material_exclude.remove(value)
                    persist_value('search_filter_material_exclude', search_state.filter_material_exclude)
                    exclude_printed_cb.value = 'Printed' in search_state.filter_material_exclude
            asyncio.ensure_future(_recompute_filter_count())
            _update_chip_bar()

        _filter_refresh_seq = {'author': 0, 'work': 0}

        async def _refresh_author_options():
            """Refresh author select options based on current domain filter (async)."""
            _filter_refresh_seq['author'] += 1
            seq = _filter_refresh_seq['author']
            author_select.props('loading')
            lang = get_language()
            new_opts = await run.io_bound(build_author_options, lang, search_state.filter_domains)
            if _filter_refresh_seq['author'] != seq:
                return  # Stale -- newer request in flight
            author_select.props(remove='loading')
            author_select.options = new_opts
            author_select.update()

        async def _refresh_work_options():
            """Refresh work select options based on current domain and author filters (async)."""
            _filter_refresh_seq['work'] += 1
            seq = _filter_refresh_seq['work']
            work_select.props('loading')
            lang = get_language()
            new_opts = await run.io_bound(
                build_work_options, lang, search_state.filter_domains, search_state.filter_authors
            )
            if _filter_refresh_seq['work'] != seq:
                return  # Stale -- newer request in flight
            work_select.props(remove='loading')
            work_select.options = new_opts
            work_select.update()

        async def _recompute_filter_count():
            """Recompute manuscript count for current filters (background)."""
            await recompute_filter_count(search_state, _update_chip_bar)
            # D-16: Detect scope change during active refinement chain
            if search_state.refinement_chain:
                new_sig = scope_signature(search_state.restrict_sys_ids)
                if new_sig != search_state._refinement_scope_sig:
                    search_state._refinement_stale = True

        # --- Filter change handlers (via shared factory) ---
        _handlers = create_filter_handlers(
            search_state, 'search', _filter_refs,
            _refresh_author_options, _refresh_work_options,
            _recompute_filter_count, _update_chip_bar,
        )

        # Wire up change handlers
        domain_select.on('update:model-value', _handlers['on_domain_change'])
        author_select.on('update:model-value', _handlers['on_author_change'])
        work_select.on('update:model-value', _handlers['on_work_change'])
        filter_mode_toggle.on('update:model-value', _handlers['on_mode_change'])
        date_from_input.on('blur', _handlers['on_date_from_change'])
        date_to_input.on('blur', _handlers['on_date_to_change'])
        exclude_printed_cb.on('update:model-value', _handlers['on_exclude_printed_change'])

        # Initialize chip bar on page load
        _update_chip_bar()

        # === Progress Bar ===
        progress_container = ui.column().classes('w-full mt-2')
        with progress_container:
            with ui.linear_progress(0, show_value=False).props('stripe animate').classes('w-full opacity-0 my-2').style('height: 12px;') as progress_bar:
                ui.label().classes('absolute-center text-xs text-white').bind_text_from(
                    progress_bar, 'value', backward=lambda v: f'{round(v * 100)}%' if v > 0 else ''
                )
            status_label = ui.label('').classes('text-sm px-6 py-1 font-medium').style('color: var(--text-secondary); display: none;')

        # === Phase 55: Refinement replay helpers ===
        async def _deferred_chain_replay():
            """Replay saved refinement chain on session restore (D-14). Shows feedback."""
            if not search_state.refinement_chain:
                return
            status_label.text = tr('Restoring refinement chain...')
            status_label.style('display: block;')
            try:
                def _do_replay():
                    return replay_chain(search_state.refinement_chain, state.searcher, search_state.restrict_sys_ids)
                result = await run.io_bound(_do_replay)
                search_state.refinement_restrict_sys_ids = result
                search_state._refinement_scope_sig = scope_signature(search_state.restrict_sys_ids)
            except Exception as e:
                logger.error(f"Refinement chain replay failed: {e}")
                search_state.refinement_chain = []
                search_state.refinement_restrict_sys_ids = None
                persist_value('search_refinement_chain', [])
            finally:
                status_label.text = ''
                status_label.style('display: none;')
            # Update UI (breadcrumb strip etc.) -- called after UI widgets exist
            try:
                _update_refinement_strip()
                _update_search_within_btn()
            except Exception:
                pass  # UI widgets may not exist yet on first load

        async def _replay_refinement_chain_and_search():
            """Re-execute chain after chip removal (D-13). Shows 'Re-evaluating...' feedback."""
            status_label.text = tr('Re-evaluating refinement...')
            status_label.style('display: block;')
            try:
                def _do_replay():
                    return replay_chain(search_state.refinement_chain, state.searcher, search_state.restrict_sys_ids)
                result = await run.io_bound(_do_replay)
                search_state.refinement_restrict_sys_ids = result
                persist_value('search_refinement_chain', [s.to_dict() for s in search_state.refinement_chain])
            except Exception as e:
                logger.error(f"Refinement replay failed: {e}")
            finally:
                status_label.text = ''
                status_label.style('display: none;')
            # Re-run final step's search to show updated results
            await execute_search()

        # === Main Content Area (full-width, no splitter) ===
        # Accordion expansion state
        search_state.expanded_index = None
        search_state.expansion_refs = {}

        with ui.column().classes('w-full flex-grow'):

            results_header = ui.row().classes('w-full px-4 py-3 items-center justify-between').style(
                'background: var(--bg-tertiary); border-bottom: 1px solid var(--border-light);'
            )
            with results_header:
                with ui.row().classes('items-center gap-3'):
                    # Select all checkbox
                    select_all_checkbox = ui.checkbox(on_change=lambda e: toggle_select_all(e.value)).props('dense')
                    results_count = ui.label(tr('Results')).classes('font-medium').style('color: var(--text-secondary);')
                    # Selection counter (initially hidden)
                    selection_counter = ui.label('').classes('text-sm').style('color: var(--primary-600); display: none;')
                    # Helper: toggle CSS visibility (reserves layout space, prevents CLS)
                    def _set_btn_visible(btn, visible):
                        btn.style(f'visibility: {"visible" if visible else "hidden"};')

                    # Domain filter button (hidden until search with domain data)
                    domain_filter_btn = ui.button(
                        tr('Filter by domains'), icon='category',
                        on_click=lambda: _open_domain_filter_dialog()
                    ).classes('text-sm').props('outline dense no-caps')
                    _set_btn_visible(domain_filter_btn, False)

                    # Restore visibility if stored exclusions exist (persistence across navigation)
                    if search_state.domain_exclusions:
                        _set_btn_visible(domain_filter_btn, True)
                        n_excl = len(search_state.domain_exclusions)
                        domain_filter_btn.text = f"{tr('Filter by domains')} ({n_excl} {tr('excluded')})"
                        domain_filter_btn.props('outline dense no-caps color=red')

                    # Printed filter toggle (hidden until search has printed data)
                    def _toggle_printed_filter():
                        states = ['all', 'hide_printed', 'only_printed']
                        current_idx = states.index(search_state.printed_filter)
                        search_state.printed_filter = states[(current_idx + 1) % 3]
                        persist_value('search_printed_filter', search_state.printed_filter)
                        _update_printed_filter_btn()
                        # Re-apply domain exclusions + printed filter and re-render
                        if search_state.domain_exclusions and search_state.has_domain_data:
                            _apply_domain_exclusions()
                        elif search_state.results:
                            _apply_printed_filter_and_render(search_state.results)

                    def _update_printed_filter_btn():
                        if search_state.printed_filter == 'all':
                            printed_filter_btn.text = tr('Filter Printed')
                            printed_filter_btn.props(remove='color')
                            printed_filter_btn.props('outline dense no-caps')
                        elif search_state.printed_filter == 'hide_printed':
                            printed_filter_btn.text = tr('Hiding printed')
                            printed_filter_btn.props(remove='color')
                            printed_filter_btn.props('outline dense no-caps color=red')
                        elif search_state.printed_filter == 'only_printed':
                            printed_filter_btn.text = tr('Only printed')
                            printed_filter_btn.props(remove='color')
                            printed_filter_btn.props('outline dense no-caps color=deep-orange')

                    printed_filter_btn = ui.button(
                        tr('Filter Printed'), icon='local_printshop',
                        on_click=lambda: _toggle_printed_filter()
                    ).classes('text-sm').props('outline dense no-caps')
                    _set_btn_visible(printed_filter_btn, False)

                    # Phase 55: "Search within" button (D-01)
                    search_within_btn = ui.button(
                        '', icon='filter_list',
                        on_click=lambda: _enter_refine_mode()
                    ).classes('text-sm').props('outline dense no-caps')
                    search_within_btn.set_visibility(False)

                with ui.row().classes('gap-2'):
                    # Bulk actions (initially hidden)
                    bulk_actions_row = ui.row().classes('gap-2').style('display: none;')
                    with bulk_actions_row:
                        ui.button(icon='playlist_add', on_click=lambda: bulk_add_to_list()).props(
                            'flat round dense size=sm'
                        ).tooltip(tr('Add Selected to List'))
                        ui.button(icon='content_copy', on_click=lambda: bulk_copy_text()).props(
                            'flat round dense size=sm'
                        ).tooltip(tr('Copy Selected Text'))

                    # Filter toggle button
                    filter_btn = ui.button(icon='filter_list', on_click=lambda: toggle_filters()).props(
                        'flat round dense size=sm'
                    ).tooltip(tr('Toggle Filters'))

                    ui.button(icon='description', on_click=lambda: ui.download('/api/export/word')).props(
                        'flat round dense size=sm'
                    ).tooltip(tr('Export Word'))
                    ui.button(icon='table_view', on_click=lambda: ui.download('/api/export/excel')).props(
                        'flat round dense size=sm'
                    ).tooltip(tr('Export Excel'))

            # Phase 55: Refinement breadcrumb strip (D-04) -- dedicated strip, NOT inside results header
            refinement_strip = ui.row().classes('w-full px-4 py-1 gap-1 items-center').style(
                'background: #f0f4ff; border-bottom: 1px solid var(--border-light); '
                'overflow-x: auto; white-space: nowrap; min-height: 0; display: none;'
            )

            # Filters Panel (initially hidden)
            filters_visible = {'value': False}
            filters_panel = ui.column().classes('w-full px-4 py-3 gap-3').style(
                'background: var(--bg-tertiary); border-bottom: 1px solid var(--border-light);'
            )
            filters_panel.set_visibility(False)
            with filters_panel:
                with ui.row().classes('w-full gap-2 items-center'):
                    ui.icon('filter_list').classes('text-sm').style('color: var(--text-muted);')
                    # Changed to H4 semantic heading
                    h4(tr('Filter Results'), classes='text-sm font-medium', style='color: var(--text-secondary);')

                with ui.grid(columns=3).classes('w-full gap-2'):
                    filter_shelfmark = ui.input(
                        placeholder=tr('Filter by shelfmark')
                    ).classes('w-full').props('outlined dense clearable')

                    filter_title = ui.input(
                        placeholder=tr('Filter by title')
                    ).classes('w-full').props('outlined dense clearable').style('direction: rtl;')

                    filter_snippet = ui.input(
                        placeholder=tr('Filter by text')
                    ).classes('w-full').props('outlined dense clearable').style('direction: rtl;')

                # Post-search measurement filters (Phase 54, DIM-03)
                with ui.expansion(tr('Measurements'), icon='straighten').classes('w-full').props(
                    'dense default-closed header-class="text-sm"'
                ):
                    with ui.column().classes('gap-2 w-full'):
                        def _make_post_meas_row(label, suffix=''):
                            with ui.row().classes('gap-1 items-center w-full'):
                                ui.label(label).classes('text-xs w-28 shrink-0')
                                min_inp = ui.number(placeholder=tr('Min'), format='%.1f').props(
                                    'outlined dense type=number step=0.1'
                                ).classes('w-20')
                                ui.label('\u2013').classes('text-xs')
                                max_inp = ui.number(placeholder=tr('Max'), format='%.1f').props(
                                    'outlined dense type=number step=0.1'
                                ).classes('w-20')
                                if suffix:
                                    ui.label(suffix).classes('text-xs text-gray-500')
                            return min_inp, max_inp

                        post_w_min, post_w_max = _make_post_meas_row(tr('Width'), tr('cm'))
                        post_h_min, post_h_max = _make_post_meas_row(tr('Height'), tr('cm'))

                        # Line count: integer inputs
                        with ui.row().classes('gap-1 items-center w-full'):
                            ui.label(tr('Lines')).classes('text-xs w-28 shrink-0')
                            post_lc_min = ui.number(placeholder=tr('Min'), format='%d').props(
                                'outlined dense type=number step=1'
                            ).classes('w-20')
                            ui.label('\u2013').classes('text-xs')
                            post_lc_max = ui.number(placeholder=tr('Max'), format='%d').props(
                                'outlined dense type=number step=1'
                            ).classes('w-20')

                        post_lh_min, post_lh_max = _make_post_meas_row(tr('Line Height'), tr('mm'))
                        post_td_min, post_td_max = _make_post_meas_row(tr('Text Density'), '/10' + tr('cm') + '\u00b2')

                        post_mat_select = ui.select(
                            options=MEASUREMENT_MATERIALS,
                            label=tr('Material (measured)'),
                            multiple=True,
                        ).props('outlined dense clearable use-chips').classes('w-full')

                        # Wire blur handlers for post-search measurement inputs
                        def _on_post_meas_blur(attr_name, inp_widget, is_int=False):
                            def handler(e=None):
                                val = inp_widget.value
                                if val is not None and val != '':
                                    val = int(val) if is_int else float(val)
                                else:
                                    val = None
                                setattr(search_state, f'post_filter_{attr_name}', val)
                            return handler

                        post_w_min.on('blur', _on_post_meas_blur('width_min', post_w_min))
                        post_w_max.on('blur', _on_post_meas_blur('width_max', post_w_max))
                        post_h_min.on('blur', _on_post_meas_blur('height_min', post_h_min))
                        post_h_max.on('blur', _on_post_meas_blur('height_max', post_h_max))
                        post_lc_min.on('blur', _on_post_meas_blur('line_count_min', post_lc_min, is_int=True))
                        post_lc_max.on('blur', _on_post_meas_blur('line_count_max', post_lc_max, is_int=True))
                        post_lh_min.on('blur', _on_post_meas_blur('line_height_min', post_lh_min))
                        post_lh_max.on('blur', _on_post_meas_blur('line_height_max', post_lh_max))
                        post_td_min.on('blur', _on_post_meas_blur('text_density_min', post_td_min))
                        post_td_max.on('blur', _on_post_meas_blur('text_density_max', post_td_max))

                        def _on_post_mat_change(e=None):
                            search_state.post_filter_measurement_material = post_mat_select.value or []
                        post_mat_select.on('update:model-value', _on_post_mat_change)

                with ui.row().classes('gap-2'):
                    ui.button(tr('Apply Filters'), icon='check', on_click=lambda: apply_filters()).props(
                        'flat dense color=green size=sm'
                    )
                    ui.button(tr('Clear Filters'), icon='clear', on_click=lambda: clear_filters()).props(
                        'flat dense size=sm'
                    )

            results_container = ui.scroll_area().classes('w-full flex-grow results-scroll-area').style(
                'background: var(--bg-secondary); min-height: 300px;'
            )

    # === Phase 55: Refinement UI helper functions ===

    def _enter_refine_mode():
        """D-02: Activate refine mode -- scroll to search bar, show badge."""
        if search_state.is_running:
            return
        n = len(search_state.results)
        search_state._refine_mode = True
        search_state._zero_result_refine = False
        refine_badge.text = f"{tr('Refining within')} {n:,} {tr('results')}"
        refine_badge.set_visibility(True)
        refine_cancel_btn.set_visibility(True)
        # Scroll to search bar and focus (D-02)
        ui.run_javascript(f'''
            document.getElementById("c{query_input.id}").scrollIntoView({{behavior: "smooth", block: "center"}});
            setTimeout(() => {{
                var el = document.getElementById("c{query_input.id}");
                if (el) {{ var inp = el.querySelector("input"); if (inp) inp.focus(); }}
            }}, 500);
        ''')

    def _exit_refine_mode():
        """D-02a: Cancel refine mode without running search."""
        search_state._refine_mode = False
        refine_badge.set_visibility(False)
        refine_cancel_btn.set_visibility(False)

    def _update_refinement_strip():
        """Rebuild the breadcrumb chip chain (D-04, D-05, D-06, D-07, D-10)."""
        refinement_strip.clear()
        chain = search_state.refinement_chain
        if not chain:
            refinement_strip.style('display: none;')
            return
        refinement_strip.style('display: flex;')
        show_modes = needs_mode_labels(chain)
        with refinement_strip:
            for i, step in enumerate(chain):
                if i > 0:
                    ui.label('\u203a').classes('text-lg mx-1').style('color: #999;')
                label = step.display_label
                if show_modes:
                    label = f"{step.query} ({step.mode})"
                chip = ui.chip(label, removable=True, color='indigo-2').classes('text-sm')
                chip.on('remove', lambda _idx=i: _remove_refinement_step(_idx))
            # Result count for final step only (D-06)
            ui.label(f'{chain[-1].result_count:,}').classes('text-sm font-bold ml-2').style('color: var(--primary-600);')
            # Clear all (D-11)
            ui.button(tr('Clear all'), icon='clear_all',
                      on_click=_clear_refinement_chain
            ).classes('text-xs ml-2').props('flat dense no-caps')
            # Stale indicator (D-16)
            if search_state._refinement_stale:
                ui.label(tr('Scope changed \u2014 results will update on next search')).classes('text-xs ml-2').style('color: #e67e22; font-style: italic;')

    def _remove_refinement_step(index):
        """D-12: Remove chip at index and all subsequent, then re-execute with feedback."""
        search_state.refinement_chain = truncate_chain(search_state.refinement_chain, index)
        if search_state.refinement_chain:
            asyncio.ensure_future(_replay_refinement_chain_and_search())
        else:
            _clear_refinement_chain()

    def _clear_refinement_chain():
        """D-11: Remove entire chain, return to unrestricted search."""
        search_state.refinement_chain = []
        search_state.refinement_restrict_sys_ids = None
        search_state._refine_mode = False
        search_state._refinement_stale = False
        search_state._refinement_scope_sig = ''
        refine_badge.set_visibility(False)
        refine_cancel_btn.set_visibility(False)
        persist_value('search_refinement_chain', [])
        _update_refinement_strip()
        _update_search_within_btn()

    def _update_search_within_btn():
        """D-01: Show/hide search within button based on result availability."""
        has_results = len(search_state.results) > 0
        is_searching = search_state.is_running
        search_within_btn.set_visibility(has_results and not is_searching)
        if has_results:
            n = len(search_state.results)
            search_within_btn.text = f"{tr('Search within')} {n:,}"

    def _undo_zero_result_refine():
        """D-14a: Recover from zero-result refinement."""
        search_state._zero_result_refine = False
        search_state._refine_mode = False
        refine_badge.set_visibility(False)
        refine_cancel_btn.set_visibility(False)
        if search_state.refinement_chain:
            asyncio.ensure_future(_replay_refinement_chain_and_search())
        else:
            render_results(search_state.results, page=0)

    # === Panel Toggle Functions ===

    def toggle_search_panel():
        """Toggle between collapsed and expanded search panel."""
        if search_state.is_panel_collapsed:
            # Expand
            search_state.is_panel_collapsed = False
            collapsed_panel.style('background: var(--bg-card); border-color: var(--border-light) !important; display: none !important;')
            expanded_panel.style('background: var(--bg-card); border-color: var(--border-light) !important; display: block !important;')
        else:
            # Collapse
            search_state.is_panel_collapsed = True
            query_val = query_input.value or ''
            collapsed_query_label.text = query_val[:50] + ('...' if len(query_val) > 50 else '') if query_val else tr('Enter search query')
            mode_val = mode_select.value
            mode_names = {'exact': '=', 'variants': '?', 'variants_extended': '??',
                         'variants_maximum': '???', 'fuzzy': '~', 'Regex': '/',
                         'Shelfmark': '#', 'Title': '$'}
            collapsed_mode_badge.text = mode_names.get(mode_val, mode_val)
            expanded_panel.style('display: none !important;')
            collapsed_panel.style('background: var(--bg-card); border-color: var(--border-light) !important; display: block !important;')

    # === Keyboard Shortcut Handler ===

    # Add keyboard shortcut for panel toggle (Escape to toggle, / to focus search)
    ui.keyboard(on_key=lambda e: handle_keyboard_shortcut(e), ignore=['input', 'textarea'])

    def handle_keyboard_shortcut(e):
        """Handle keyboard shortcuts for search panel."""
        if e.action.keydown:
            if e.key == 'Escape':
                toggle_search_panel()
            elif e.key == '/' and not e.action.repeat:
                # Focus search input and expand panel if collapsed
                if search_state.is_panel_collapsed:
                    search_state.is_panel_collapsed = False
                    collapsed_panel.style('background: var(--bg-card); border-color: var(--border-light) !important; display: none !important;')
                    expanded_panel.style('background: var(--bg-card); border-color: var(--border-light) !important; display: block !important;')
                query_input.run_method('focus')

    # === Auto-collapse on Scroll (Simplified Approach) ===
    # Using a debounced scroll detection that only collapses - user manually expands
    # This avoids complex bidirectional sync issues

    scroll_collapse_enabled = {'value': True}  # Can be disabled during search

    async def setup_scroll_collapse():
        """Set up scroll-based auto-collapse using JavaScript."""
        # Get a stable reference to the results container
        collapsed_id = f'collapsed-{id(collapsed_panel)}'
        expanded_id = f'expanded-{id(expanded_panel)}'
        collapsed_panel.props(f'id="{collapsed_id}"')
        expanded_panel.props(f'id="{expanded_id}"')

        js_code = f'''
        (function() {{
            // Find the scroll area - look for the results scroll area's inner container
            const findScrollArea = () => {{
                // First try the direct class we added
                const scrollAreaEl = document.querySelector('.results-scroll-area');
                if (scrollAreaEl) {{
                    // Quasar scroll-area has an inner container that actually scrolls
                    const inner = scrollAreaEl.querySelector('.q-scrollarea__container');
                    if (inner) return inner;
                    // Fallback: the element itself might be scrollable
                    return scrollAreaEl;
                }}
                return null;
            }};

            let attempts = 0;
            const setupScroll = () => {{
                const scrollArea = findScrollArea();
                if (!scrollArea && attempts < 20) {{
                    attempts++;
                    setTimeout(setupScroll, 300);
                    return;
                }}
                if (!scrollArea) {{
                    console.warn('Could not find scroll area for auto-collapse');
                    return;
                }}

                let lastScrollTop = 0;
                let scrollThreshold = 50;  // Lower threshold for easier triggering
                let collapseTimeout = null;

                scrollArea.addEventListener('scroll', function() {{
                    const currentScrollTop = scrollArea.scrollTop;
                    const scrollDelta = currentScrollTop - lastScrollTop;

                    // Clear any pending collapse
                    if (collapseTimeout) {{
                        clearTimeout(collapseTimeout);
                        collapseTimeout = null;
                    }}

                    // Only collapse when scrolling down past threshold
                    if (scrollDelta > scrollThreshold && currentScrollTop > 100) {{
                        collapseTimeout = setTimeout(() => {{
                            const expandedEl = document.getElementById('{expanded_id}');
                            const collapsedEl = document.getElementById('{collapsed_id}');
                            if (expandedEl && collapsedEl) {{
                                const isVisible = !expandedEl.style.display || !expandedEl.style.display.includes('none');
                                if (isVisible) {{
                                    expandedEl.style.cssText = 'display: none !important;';
                                    collapsedEl.style.cssText = 'background: var(--bg-card); border-color: var(--border-light) !important; display: block !important;';
                                }}
                            }}
                        }}, 100);
                    }}

                    lastScrollTop = currentScrollTop;
                }});

                console.log('Scroll auto-collapse setup complete');
            }};

            // Start looking for scroll area
            setTimeout(setupScroll, 300);
        }})();
        '''
        try:
            await ui.run_javascript(js_code, timeout=5.0)
        except TimeoutError:
            pass  # JS still executes, timeout is just about awaiting response

    # Set up scroll handlers after a short delay
    asyncio.ensure_future(_after_delay(1.0, setup_scroll_collapse))

    # === Helper Functions ===

    def prepend_to_query(prefix):
        current = query_input.value or ""
        if not current.startswith(prefix):
            query_input.set_value(prefix + current)

    # === Filtering Functions ===

    def toggle_filters():
        """Toggle visibility of filters panel."""
        filters_visible['value'] = not filters_visible['value']
        filters_panel.set_visibility(filters_visible['value'])

    def apply_filters():
        """Apply filters to results."""
        if not search_state.results:
            return

        filtered = []
        shelfmark_filter = (filter_shelfmark.value or '').lower().strip()
        title_filter = (filter_title.value or '').lower().strip()
        snippet_filter = (filter_snippet.value or '').lower().strip()
        for res in search_state.results:
            display = res.get('display', {})
            shelfmark = (display.get('shelfmark', '') or '').lower()
            title = (display.get('title', '') or '').lower()
            snippet = (res.get('snippet', '') or '').lower()

            # All filters must match
            if shelfmark_filter and shelfmark_filter not in shelfmark:
                continue
            if title_filter and title_filter not in title:
                continue
            if snippet_filter and snippet_filter not in snippet:
                continue

            filtered.append(res)

        # Apply measurement post-filters (Phase 54, review concern #1)
        filtered = _apply_measurement_post_filters(filtered, search_state)

        render_results(filtered, page=0)
        shown = len(filtered)
        results_count.text = f"{shown} / {len(search_state.results)} {tr('Results')}"
        ui.notify(f"{len(filtered)} {tr('results match filters')}", type='info')

    def clear_filters():
        """Clear all filters and show all results."""
        filter_shelfmark.value = ''
        filter_title.value = ''
        filter_snippet.value = ''
        # Clear post-search measurement state (Phase 54)
        search_state.post_filter_width_min = None
        search_state.post_filter_width_max = None
        search_state.post_filter_height_min = None
        search_state.post_filter_height_max = None
        search_state.post_filter_line_count_min = None
        search_state.post_filter_line_count_max = None
        search_state.post_filter_line_height_min = None
        search_state.post_filter_line_height_max = None
        search_state.post_filter_text_density_min = None
        search_state.post_filter_text_density_max = None
        search_state.post_filter_measurement_material = []
        # Note: do NOT clear _measurement_cache here — it's enrichment data,
        # not filter state. Clearing it would break reapplying filters later.

        if search_state.results:
            render_results(search_state.results, page=0)
            results_count.text = f"{len(search_state.results)} {tr('Results')}"
            ui.notify(tr('Filters cleared'), type='info')

    # === Reset Search ===

    def _reset_search():
        """Reset all search state, clear results, filters, exclusions, and persistent storage."""
        # Clear query input
        query_input.value = ''
        # Reset mode to exact
        mode_select.value = 'exact'
        # Clear results
        search_state.results = []
        search_state.displayed_results = []
        search_state.selected_indices.clear()
        search_state.transcription_sys_ids = set()
        search_state.total_count = 0
        search_state.current_page = 0
        search_state.result_domains = {}
        search_state.all_result_domains = {}
        search_state.has_domain_data = False
        search_state.domain_name_map = {}
        search_state.catalog_source_counts = {}
        search_state.printed_ids = set()
        search_state.translation_data = {}
        search_state.domain_excluded_results = []
        search_state.word_search_excluded_results = []
        # Clear domain exclusions
        search_state.domain_exclusions = set()
        # Clear word search exclusions
        search_state.word_search_excluded_ids = set()
        # Reset printed filter
        search_state.printed_filter = 'all'
        # Clear pre-search filters
        _clear_all_adv_filters()
        # Clear post-search filters
        clear_filters()
        # Clear results container
        results_container.clear()
        with results_container:
            with ui.column().classes('w-full h-64 items-center justify-center'):
                ui.icon('search').classes('text-6xl').style('color: var(--text-muted);')
                ui.label(tr('Enter a search query')).classes('mt-4').style('color: var(--text-muted);')
        # Reset results count label
        results_count.text = tr('Results')
        # Hide domain filter and printed filter buttons
        _set_btn_visible(domain_filter_btn, False)
        _set_btn_visible(printed_filter_btn, False)
        # Reset persistent storage to clean defaults (use explicit values, not None or pop)
        app.storage.user['search_results'] = []
        app.storage.user['search_query'] = ''
        app.storage.user['search_mode'] = 'exact'
        app.storage.user['domain_exclusions'] = []
        app.storage.user['search_printed_filter'] = 'all'
        app.storage.user['word_search_excluded_ids'] = []
        ui.notify(tr('Search reset'), type='info', timeout=2000)

    # === Bulk Operations ===

    def toggle_select_all(is_checked):
        """Toggle selection of all results."""
        if is_checked:
            # Select all current results
            for i in range(len(search_state.results)):
                search_state.selected_indices.add(i)
        else:
            # Deselect all
            search_state.selected_indices.clear()

        # Re-render to update checkboxes (keeps current page)
        _display = _apply_measurement_post_filters(search_state.results, search_state)
        render_results(_display)
        update_selection_ui()

    def update_selection_ui():
        """Update selection counter and bulk actions visibility."""
        selected_count = len(search_state.selected_indices)

        if selected_count > 0:
            selection_counter.text = f"{selected_count} {tr('selected')}"
            selection_counter.style('color: var(--primary-600);')
            bulk_actions_row.style('')  # Show bulk actions
        else:
            selection_counter.text = ''
            selection_counter.style('display: none;')
            bulk_actions_row.style('display: none;')  # Hide bulk actions

        # Update select all checkbox state
        if selected_count == len(search_state.results) and len(search_state.results) > 0:
            select_all_checkbox.value = True
        else:
            select_all_checkbox.value = False

    def bulk_add_to_list():
        """Add all selected results to a list."""
        if not search_state.selected_indices:
            ui.notify(tr('No results selected'), type='warning')
            return

        selected_results = [search_state.results[i] for i in sorted(search_state.selected_indices)
                          if i < len(search_state.results)]

        if not selected_results:
            ui.notify(tr('No valid selections'), type='warning')
            return

        # State for inline list creation
        creating_new_list = {'active': False}

        # Show list selection dialog
        with ui.dialog() as dialog, ui.card().classes('p-6 min-w-96'):
            # Changed to H3 semantic heading
            h3(tr('Add Selected to List'), classes='text-xl font-bold mb-2')
            ui.label(f"{len(selected_results)} {tr('items selected')}").style('color: var(--text-secondary);')

            if state.lists_mgr:
                lists = state.lists_mgr.data.get('lists', {})
                list_options = {lid: lst['name'] for lid, lst in lists.items() if not lst.get('is_system')}

                # Container for list selection form
                form_container = ui.column().classes('w-full mt-4 gap-3')
                # Container for new list creation form
                new_list_container = ui.column().classes('w-full mt-4 gap-3')
                new_list_container.set_visibility(False)

                with form_container:
                    if not list_options:
                        ui.label(tr('No lists yet. Create your first list!')).style('color: var(--text-muted);')
                    else:
                        # List selection with "Create new list" option
                        list_options_with_new = {'__new__': f"+ {tr('Create new list')}", **list_options}
                        selected_list = ui.select(
                            list_options_with_new,
                            label=tr('Select List'),
                            value=list(list_options.keys())[0] if list_options else '__new__'
                        ).classes('w-full').props('outlined').style('color: var(--text-primary);')

                        def on_list_change():
                            if selected_list.value == '__new__':
                                form_container.set_visibility(False)
                                new_list_container.set_visibility(True)
                                creating_new_list['active'] = True

                        selected_list.on('update:model-value', on_list_change)

                    # Create New List button (shown when no lists exist)
                    if not list_options:
                        ui.button(
                            tr('Create new list'),
                            icon='add',
                            on_click=lambda: (form_container.set_visibility(False), new_list_container.set_visibility(True))
                        ).classes('btn-primary')

                # New list creation form
                with new_list_container:
                    ui.label(tr('Create New List')).classes('font-semibold').style('color: var(--text-primary);')
                    new_list_name = ui.input(label=tr('List Name')).classes('w-full').props('outlined')

                    # Color picker
                    ui.label(tr('Color')).classes('text-sm mt-2').style('color: var(--text-secondary);')
                    selected_color = {'value': '#4CAF50'}

                    with ui.row().classes('gap-2 flex-wrap'):
                        colors = ['#FFD700', '#4CAF50', '#2196F3', '#9C27B0', '#FF5722',
                                  '#00BCD4', '#E91E63', '#795548', '#607D8B', '#F44336']
                        for color in colors:
                            btn = ui.button(icon='circle').props('flat round dense').style(
                                f'color: {color}; font-size: 1.5rem;'
                            )
                            btn.on('click', lambda c=color: selected_color.update({'value': c}))

                    with ui.row().classes('w-full justify-end gap-2 mt-4'):
                        def back_to_list_selection():
                            new_list_container.set_visibility(False)
                            form_container.set_visibility(True)
                            creating_new_list['active'] = False

                        if list_options:
                            ui.button(tr('Back'), on_click=back_to_list_selection).props('flat')

                        def create_and_add_all():
                            name = new_list_name.value.strip()
                            if not name:
                                ui.notify(tr('Please enter a list name'), type='warning')
                                return

                            new_list_id = state.lists_mgr.create_list_sync(name, color=selected_color['value'])
                            if new_list_id:
                                added_count = 0
                                for res in selected_results:
                                    display = res.get('display', {})
                                    sys_id = display.get('id')
                                    if sys_id and state.lists_mgr.add_item_sync(sys_id, new_list_id):
                                        added_count += 1

                                ui.notify(f"{tr('List created')}: {name}", type='positive')
                                ui.notify(f"{added_count} {tr('items added to list')}", type='positive')
                                dialog.close()

                        ui.button(tr('Create and Add'), on_click=create_and_add_all).classes('btn-primary')

                # Action buttons for existing list selection
                with ui.row().classes('w-full justify-end gap-2 mt-6') as action_row:
                    ui.button(tr('Cancel'), on_click=dialog.close).props('flat')

                    def add_all():
                        if not list_options or creating_new_list['active']:
                            return

                        if selected_list.value == '__new__':
                            form_container.set_visibility(False)
                            new_list_container.set_visibility(True)
                            creating_new_list['active'] = True
                            return

                        added_count = 0
                        for res in selected_results:
                            display = res.get('display', {})
                            sys_id = display.get('id')
                            if sys_id and state.lists_mgr.add_item_sync(sys_id, selected_list.value):
                                added_count += 1

                        ui.notify(f"{added_count} {tr('items added to list')}", type='positive')
                        dialog.close()

                    add_btn = ui.button(tr('Add All'), on_click=add_all).classes('btn-primary')
                    if not list_options:
                        add_btn.set_visibility(False)
            else:
                ui.label(tr('Lists manager not available')).style('color: var(--error);')

        dialog.open()

    def bulk_copy_text():
        """Copy all selected results' text to clipboard."""
        if not search_state.selected_indices:
            ui.notify(tr('No results selected'), type='warning')
            return

        selected_results = [search_state.results[i] for i in sorted(search_state.selected_indices)
                          if i < len(search_state.results)]

        if not selected_results:
            ui.notify(tr('No valid selections'), type='warning')
            return

        # Compile all text
        compiled_text = []
        for i, res in enumerate(selected_results, 1):
            display = res.get('display', {})
            shelfmark = display.get('shelfmark', 'Unknown')
            full_text = res.get('full_text', '')
            snippet = res.get('snippet', '').replace('*', '')
            text = full_text or snippet

            compiled_text.append(f"=== {i}. {shelfmark} ===\n{text}\n")

        final_text = '\n'.join(compiled_text)
        # Escape backticks for JavaScript
        escaped_text = final_text.replace('`', '\\`')

        # Copy to clipboard
        ui.run_javascript(f'''
            navigator.clipboard.writeText(`{escaped_text}`).then(() => {{
                console.log('Bulk text copied to clipboard');
            }});
        ''')
        ui.notify(f"{len(selected_results)} {tr('results copied to clipboard')}", type='positive')

    def cancel_search():
        """Cancel the current search and show partial results."""
        search_state.is_cancelled = True
        search_state.status = tr('Cancelling...')

    def update_progress_ui():
        """Update progress bar, elapsed time, and button states.

        Returns True to keep the async loop running, False to stop it
        (e.g. when the client/elements have been deleted due to navigation).
        """
        try:
            # Check if client still exists
            _ = progress_bar.client
        except (RuntimeError, Exception):
            # Client deleted — stop the loop
            return False

        try:
            if search_state.is_running:
                progress_bar.classes(remove='opacity-0')
                progress_bar.value = search_state.progress
                # Compute elapsed time and prepend to status
                elapsed = time.time() - search_state.search_start_time if search_state.search_start_time else 0
                if elapsed >= 3600:
                    elapsed_str = f"{int(elapsed // 3600)}:{int((elapsed % 3600) // 60):02d}:{int(elapsed % 60):02d}"
                else:
                    elapsed_str = f"{int(elapsed // 60)}:{int(elapsed % 60):02d}"
                # Show elapsed time in results_count during search (will be overwritten on completion)
                results_count.text = f"{tr('Searching...')} · {elapsed_str}"
                # Swap buttons: hide search, show stop (using style to avoid performance issues)
                search_btn.style('display: none;')
                stop_btn.style('display: inline-flex;')
            else:
                # Swap buttons: show search, hide stop
                search_btn.style('display: inline-flex;')
                stop_btn.style('display: none;')
                if search_state.progress >= 1.0:
                    progress_bar.value = 1.0
                    progress_bar.classes(add='opacity-0')
                else:
                    progress_bar.classes(add='opacity-0')
        except Exception:
            pass  # Client may have been deleted
        return True

    # Use asyncio loop for progress updates instead of ui.timer to avoid
    # "parent slot of the element has been deleted" RuntimeError on navigation
    if search_state.update_timer:
        search_state.update_timer.cancel()

    async def _progress_update_loop():
        while True:
            if not update_progress_ui():
                break
            await asyncio.sleep(0.5)

    search_state.update_timer = asyncio.ensure_future(_progress_update_loop())

    def open_query_builder():
        """Open the tabular query builder dialog for composing Responsa queries visually."""
        # === Builder State ===
        _updating_modifiers = {'flag': False}  # Guard to prevent on_change loops when updating checkboxes

        def make_word(text=''):
            return {'text': text, 'mods': {
                'prefix': False, 'suffix': False,
                'wildcard_prefix': False, 'wildcard_suffix': False,
                'plene': False, 'negation': False,
                'line_start': False, 'line_end': False,
            }}

        def make_component():
            return {'words': [make_word(), make_word(), make_word(), make_word()]}

        builder_state = {
            'components': [make_component(), make_component()],
            'distances': [0],
            'scope': 'word_range',
            'active_word': None,  # (comp_idx, word_idx) tuple
            'num_components': 2,
        }

        # UI element references
        comp_cards = []  # List of component card containers
        word_inputs = []  # word_inputs[comp_idx][word_idx] = ui.input element
        word_containers = []  # word_containers[comp_idx][word_idx] = container element
        add_word_btns = []  # Per-component add word buttons
        distance_spinners = []  # Distance column containers between components
        distance_number_els = []  # The actual ui.number elements for distance
        remove_comp_btns = []  # Remove buttons per component
        visible_words = []  # visible_words[comp_idx] = number of visible word slots
        preview_label_ref = {'el': None}
        scope_toggle_ref = {'el': None}
        add_comp_btn_ref = {'el': None}

        # Modifier checkbox references
        mod_cbs = {}
        # Per-word modifier indicator labels: mod_indicators[comp_idx][word_idx] = ui.label element
        mod_indicators = []

        # Modifier display names (Hebrew)
        MOD_DISPLAY = {
            'prefix': '#_',
            'suffix': '_#',
            'wildcard_prefix': '*_',
            'wildcard_suffix': '_*',
            'plene': '%',
            'negation': '−',
            'line_start': '|_',
            'line_end': '_|',
        }

        def _build_mod_indicator_text(mods):
            """Build a short string showing active modifiers for a word."""
            parts = []
            for key in ['prefix', 'suffix', 'wildcard_prefix', 'wildcard_suffix', 'plene', 'negation',
                         'line_start', 'line_end']:
                if mods.get(key):
                    parts.append(MOD_DISPLAY[key])
            return ' '.join(parts)

        def _update_mod_indicator(comp_idx, word_idx):
            """Update the modifier indicator label for a specific word."""
            if comp_idx < len(mod_indicators) and word_idx < len(mod_indicators[comp_idx]):
                mods = builder_state['components'][comp_idx]['words'][word_idx]['mods']
                text = _build_mod_indicator_text(mods)
                indicator = mod_indicators[comp_idx][word_idx]
                indicator.text = text
                indicator.set_visibility(bool(text))

        def _update_active_word_highlight(new_comp=None, new_word=None):
            """Apply/remove highlight border on word inputs to show which word is selected."""
            # Remove highlight from all
            for ci_list in word_inputs:
                for inp in ci_list:
                    inp.props(remove='color=primary')
                    inp.classes(remove='ring-2 ring-primary')
            # Add highlight to the new active word
            if new_comp is not None and new_word is not None:
                if new_comp < len(word_inputs) and new_word < len(word_inputs[new_comp]):
                    word_inputs[new_comp][new_word].props('color=primary')

        # === Core Functions ===
        def update_preview():
            """Regenerate preview text from current builder state."""
            comps = []
            for ci in range(builder_state['num_components']):
                comp = builder_state['components'][ci]
                words = []
                for wi in range(visible_words[ci] if ci < len(visible_words) else 2):
                    w = comp['words'][wi]
                    words.append({'text': w['text'], 'mods': dict(w['mods'])})
                comps.append({'words': words})
            dists = builder_state['distances'][:builder_state['num_components'] - 1]
            try:
                syntax, neg = generate_tabular_syntax(comps, dists, builder_state['scope'])
                if preview_label_ref['el']:
                    if syntax.strip():
                        neg_text = f"  [- {' '.join(neg)}]" if neg else ""
                        preview_label_ref['el'].text = syntax + neg_text
                    else:
                        preview_label_ref['el'].text = tr('No words entered')
            except Exception:
                if preview_label_ref['el']:
                    preview_label_ref['el'].text = ''

        def on_word_focus(comp_idx, word_idx):
            """Track the active word and update modifier checkboxes to reflect its state."""
            builder_state['active_word'] = (comp_idx, word_idx)
            _updating_modifiers['flag'] = True
            try:
                mods = builder_state['components'][comp_idx]['words'][word_idx]['mods']
                for key, cb in mod_cbs.items():
                    cb.set_value(mods.get(key, False))
            finally:
                _updating_modifiers['flag'] = False
            _update_active_word_highlight(comp_idx, word_idx)

        def on_modifier_change(modifier_name, value):
            """Save modifier change to the currently active word."""
            if _updating_modifiers['flag']:
                return
            aw = builder_state['active_word']
            if aw is None:
                return
            ci, wi = aw
            builder_state['components'][ci]['words'][wi]['mods'][modifier_name] = value
            _update_mod_indicator(ci, wi)
            update_preview()

        def on_word_text_change(comp_idx, word_idx, value):
            """Update builder state when word text changes."""
            builder_state['components'][comp_idx]['words'][word_idx]['text'] = value or ''
            update_preview()

        def on_distance_change(pair_idx, value):
            """Update distance between component pair."""
            while len(builder_state['distances']) <= pair_idx:
                builder_state['distances'].append(0)
            builder_state['distances'][pair_idx] = int(value or 0)
            update_preview()

        def on_scope_change(value):
            """Toggle scope and show/hide distance spinners and line modifiers."""
            builder_state['scope'] = value
            show_dists = (value in ('word_range', 'lines'))
            for ds in distance_spinners:
                ds.set_visibility(show_dists)
            # Show/hide line position modifiers
            is_lines = (value == 'lines')
            if 'line_start' in mod_cbs:
                mod_cbs['line_start'].set_visibility(is_lines)
            if 'line_end' in mod_cbs:
                mod_cbs['line_end'].set_visibility(is_lines)
            # Update distance labels
            for dn_el in distance_number_els:
                dn_el.suffix = tr('lines') if is_lines else tr('words') if value == 'word_range' else ''
            update_preview()

        def add_word_slot(comp_idx):
            """Show the next hidden word slot in a component."""
            if comp_idx >= len(visible_words):
                return
            if visible_words[comp_idx] < 4:
                wi = visible_words[comp_idx]
                visible_words[comp_idx] += 1
                if comp_idx < len(word_containers) and wi < len(word_containers[comp_idx]):
                    word_containers[comp_idx][wi].set_visibility(True)
                # Hide the + button if we've reached 4
                if visible_words[comp_idx] >= 4 and comp_idx < len(add_word_btns):
                    add_word_btns[comp_idx].set_visibility(False)

        def remove_component(comp_idx):
            """Remove a component (only 3rd and 4th can be removed)."""
            n = builder_state['num_components']
            if n <= 2 or comp_idx < 2:
                return
            # Hide this component card
            if comp_idx < len(comp_cards):
                comp_cards[comp_idx].set_visibility(False)
            # Hide the distance spinner before this component
            dist_idx = comp_idx - 1
            if dist_idx < len(distance_spinners):
                distance_spinners[dist_idx].set_visibility(False)
            # Clear word data
            builder_state['components'][comp_idx] = make_component()
            builder_state['num_components'] -= 1
            # Show add component button if under max
            if add_comp_btn_ref['el'] and builder_state['num_components'] < 4:
                add_comp_btn_ref['el'].set_visibility(True)
            update_preview()

        def add_component():
            """Add a new component (up to 4)."""
            n = builder_state['num_components']
            if n >= 4:
                return
            # Show the next hidden component card
            if n < len(comp_cards):
                comp_cards[n].set_visibility(True)
                visible_words[n] = 2  # Reset to 2 visible words
                # Show first 2 word slots, hide 3rd and 4th
                for wi in range(4):
                    if n < len(word_containers) and wi < len(word_containers[n]):
                        word_containers[n][wi].set_visibility(wi < 2)
                if n < len(add_word_btns):
                    add_word_btns[n].set_visibility(True)
            # Show the distance spinner before this new component
            dist_idx = n - 1
            if dist_idx < len(distance_spinners):
                distance_spinners[dist_idx].set_visibility(True)
            # Ensure distance list is long enough
            while len(builder_state['distances']) <= dist_idx:
                builder_state['distances'].append(0)
            # Show remove button for this component
            if n < len(remove_comp_btns):
                remove_comp_btns[n].set_visibility(True)
            builder_state['num_components'] = n + 1
            # Hide add button if at max
            if add_comp_btn_ref['el'] and builder_state['num_components'] >= 4:
                add_comp_btn_ref['el'].set_visibility(False)
            update_preview()

        def clear_all():
            """Reset all inputs without closing the dialog."""
            for ci in range(4):
                for wi in range(4):
                    builder_state['components'][ci]['words'][wi] = make_word()
                    if ci < len(word_inputs) and wi < len(word_inputs[ci]):
                        word_inputs[ci][wi].set_value('')
                    # Hide modifier indicators
                    if ci < len(mod_indicators) and wi < len(mod_indicators[ci]):
                        mod_indicators[ci][wi].text = ''
                        mod_indicators[ci][wi].set_visibility(False)
            # Reset distances
            for i in range(3):
                builder_state['distances'][i] = 0
                if i < len(distance_number_els):
                    distance_number_els[i].set_value(0)
            # Reset scope
            builder_state['scope'] = 'word_range'
            if scope_toggle_ref['el']:
                scope_toggle_ref['el'].set_value('word_range')
            # Reset to 2 components, 2 words each
            for ci in range(4):
                if ci < len(comp_cards):
                    comp_cards[ci].set_visibility(ci < 2)
                visible_words[ci] = 2
                for wi in range(4):
                    if ci < len(word_containers) and wi < len(word_containers[ci]):
                        word_containers[ci][wi].set_visibility(ci < 2 and wi < 2)
                if ci < len(add_word_btns):
                    add_word_btns[ci].set_visibility(ci < 2)
                if ci < len(remove_comp_btns):
                    remove_comp_btns[ci].set_visibility(False)
            builder_state['num_components'] = 2
            builder_state['active_word'] = None
            # Show only the first distance spinner (between comp 0 and 1, word_range is default scope)
            for i, ds in enumerate(distance_spinners):
                ds.set_visibility(i < 1)
            # Reset modifier checkboxes
            _updating_modifiers['flag'] = True
            for cb in mod_cbs.values():
                cb.set_value(False)
            _updating_modifiers['flag'] = False
            if add_comp_btn_ref['el']:
                add_comp_btn_ref['el'].set_visibility(True)
            update_preview()

        async def on_apply():
            """Generate syntax, populate search field, close dialog, trigger search."""
            comps = []
            for ci in range(builder_state['num_components']):
                comp = builder_state['components'][ci]
                words = []
                for wi in range(visible_words[ci] if ci < len(visible_words) else 2):
                    w = comp['words'][wi]
                    words.append({'text': w['text'], 'mods': dict(w['mods'])})
                comps.append({'words': words})
            dists = builder_state['distances'][:builder_state['num_components'] - 1]
            syntax, neg = generate_tabular_syntax(comps, dists, builder_state['scope'])
            if not syntax.strip():
                ui.notify(tr('No words entered'), type='warning')
                return
            # Set negated words on search state and show in exclude field
            search_state.builder_negated_words = neg
            if neg:
                current_not = not_filter.value.strip() if not_filter.value else ''
                new_not = (current_not + ' ' + ' '.join(neg)).strip() if current_not else ' '.join(neg)
                not_filter.set_value(new_not)
            # Set query and close dialog
            query_input.set_value(syntax)
            builder_dialog.close()
            # Trigger search
            await execute_search()

        # === Build the Dialog UI ===
        with ui.dialog() as builder_dialog, ui.card().classes('p-4 q-pa-md builder-dialog-card').style(
            'min-width: min(700px, 95vw); max-width: 900px; direction: rtl;'
        ):
            # Title
            with ui.row().classes('w-full items-center justify-between mb-2'):
                ui.label(tr('Tabular Search')).classes('text-lg font-bold').style('color: var(--primary-600);')
                ui.button(icon='close', on_click=builder_dialog.close).props('flat round dense')

            # Scope Toggle
            scope_toggle = ui.toggle(
                {'word_range': tr('Word Range'), 'within_document': tr('Within Document'), 'lines': tr('Lines')},
                value='word_range',
                on_change=lambda e: on_scope_change(e.value)
            ).classes('mb-3')
            scope_toggle_ref['el'] = scope_toggle

            # Pre-create all 4 components and 3 distance spinners (hide extras)
            # Ensure builder_state has 4 components and 3 distances
            while len(builder_state['components']) < 4:
                builder_state['components'].append(make_component())
            while len(builder_state['distances']) < 3:
                builder_state['distances'].append(0)

            # Components container (RTL: component 1 on right)
            with ui.row().classes('w-full gap-3 flex-wrap justify-end items-start') as comps_row:
                for ci in range(4):
                    # Distance spinner BEFORE component (except for first component)
                    if ci > 0:
                        with ui.column().classes('items-center justify-center gap-0').style('min-width: 60px;') as dist_col:
                            ui.label(tr('Distance')).classes('text-xs').style('color: var(--text-muted);')
                            dist_num = ui.number(
                                value=0, min=0, max=50
                            ).classes('w-16').props('outlined dense')

                            def _make_dist_handler(dn, idx):
                                def handler():
                                    on_distance_change(idx, dn.value)
                                return handler
                            dist_num.on('update:model-value', _make_dist_handler(dist_num, ci - 1))
                        dist_col.set_visibility(ci < 2)  # Only show first distance initially
                        distance_spinners.append(dist_col)
                        distance_number_els.append(dist_num)

                    # Component card
                    with ui.card().classes('p-3').style(
                        'border: 1px solid var(--border-light); border-radius: 8px; min-width: 150px; flex: 1;'
                    ) as comp_card:
                        with ui.row().classes('w-full items-center justify-between mb-1'):
                            ui.label(f"{tr('Component')} {ci+1}").classes('text-sm font-medium')
                            # Remove button (only for 3rd and 4th components)
                            rm_btn = ui.button(icon='close', on_click=lambda _, c=ci: remove_component(c)).props(
                                'flat round dense size=xs color=red'
                            )
                            rm_btn.set_visibility(False)  # Hidden initially
                            remove_comp_btns.append(rm_btn)

                        comp_word_inputs = []
                        comp_word_containers = []

                        comp_mod_indicators = []
                        for wi in range(4):
                            with ui.column().classes('w-full gap-0') as word_row:
                                with ui.row().classes('w-full items-center gap-1'):
                                    inp = ui.input(
                                        placeholder=f"\u05de\u05d9\u05dc\u05d4 {wi+1}"  # "מילה N"
                                    ).classes('flex-grow').props('outlined dense').style('direction: rtl;')
                                    inp.on('focus', lambda _, c=ci, w=wi: on_word_focus(c, w))

                                    def _make_text_handler(input_el, c_idx, w_idx):
                                        def handler(e):
                                            val = e.args if hasattr(e, 'args') else e
                                            on_word_text_change(c_idx, w_idx, val if isinstance(val, str) else (str(val) if val is not None else ''))
                                        return handler
                                    inp.on('update:model-value', _make_text_handler(inp, ci, wi))
                                # Modifier indicator label (hidden until modifiers are set)
                                mod_ind = ui.label('').classes('text-xs').style(
                                    'color: var(--primary-600); direction: ltr; margin-top: -2px; padding-right: 4px;'
                                )
                                mod_ind.set_visibility(False)
                                comp_mod_indicators.append(mod_ind)
                            word_row.set_visibility(wi < 2)  # First 2 visible
                            comp_word_inputs.append(inp)
                            comp_word_containers.append(word_row)

                        word_inputs.append(comp_word_inputs)
                        word_containers.append(comp_word_containers)
                        mod_indicators.append(comp_mod_indicators)

                        # Add Word button
                        aw_btn = ui.button(tr('Add Word'), icon='add', on_click=lambda _, c=ci: add_word_slot(c)).props(
                            'outline dense size=sm'
                        ).classes('mt-1')
                        add_word_btns.append(aw_btn)

                    comp_card.set_visibility(ci < 2)  # Only first 2 visible
                    comp_cards.append(comp_card)

            visible_words.extend([2, 2, 2, 2])  # Initial visible word count per component

            # Add Component button
            add_comp_btn = ui.button(tr('Add Component'), icon='add',
                on_click=lambda: add_component()).props('outline dense').classes('mt-2')
            add_comp_btn_ref['el'] = add_comp_btn

            # === Shared Modifier Checkboxes Row ===
            with ui.row().classes('w-full gap-3 mt-3 items-center flex-wrap'):
                ui.label(tr('Modifiers') + ':').classes('text-sm font-medium')
                prefix_cb = ui.checkbox(tr('Prefixes #_')).tooltip(tr('Grammatical prefixes tooltip'))
                suffix_cb = ui.checkbox(tr('Suffixes _#')).tooltip(tr('Grammatical suffixes tooltip'))
                wild_start_cb = ui.checkbox(tr('Wildcard *_')).tooltip(tr('Words ending with...'))
                wild_end_cb = ui.checkbox(tr('Wildcard _*')).tooltip(tr('Words starting with...'))
                plene_cb = ui.checkbox(tr('Plene/Defective %')).tooltip(tr('Plene/defective spelling tooltip'))
                negation_cb = ui.checkbox(tr('Negation −')).tooltip(tr('Negation tooltip'))
                line_start_cb = ui.checkbox(tr('Start of line |_')).tooltip(tr('Word must appear at start of line'))
                line_start_cb.set_visibility(False)  # Only visible in Lines scope
                line_end_cb = ui.checkbox(tr('End of line _|')).tooltip(tr('Word must appear at end of line'))
                line_end_cb.set_visibility(False)  # Only visible in Lines scope

                mod_cbs['prefix'] = prefix_cb
                mod_cbs['suffix'] = suffix_cb
                mod_cbs['wildcard_prefix'] = wild_start_cb
                mod_cbs['wildcard_suffix'] = wild_end_cb
                mod_cbs['plene'] = plene_cb
                mod_cbs['negation'] = negation_cb
                mod_cbs['line_start'] = line_start_cb
                mod_cbs['line_end'] = line_end_cb

                def _make_mod_handler(cb_el, mod_name):
                    def handler():
                        on_modifier_change(mod_name, cb_el.value)
                    return handler
                prefix_cb.on('update:model-value', _make_mod_handler(prefix_cb, 'prefix'))
                suffix_cb.on('update:model-value', _make_mod_handler(suffix_cb, 'suffix'))
                wild_start_cb.on('update:model-value', _make_mod_handler(wild_start_cb, 'wildcard_prefix'))
                wild_end_cb.on('update:model-value', _make_mod_handler(wild_end_cb, 'wildcard_suffix'))
                plene_cb.on('update:model-value', _make_mod_handler(plene_cb, 'plene'))
                negation_cb.on('update:model-value', _make_mod_handler(negation_cb, 'negation'))
                line_start_cb.on('update:model-value', _make_mod_handler(line_start_cb, 'line_start'))
                line_end_cb.on('update:model-value', _make_mod_handler(line_end_cb, 'line_end'))

            # === Search Options Row (synced with outer Responsa checkboxes) ===
            with ui.row().classes('w-full gap-3 mt-2 items-center flex-wrap'):
                ui.label(tr('Search Options') + ':').classes('text-sm font-medium')
                bld_variants_cb = ui.checkbox(tr('Variants'), value=responsa_variants_cb.value)
                bld_ja_cb = ui.checkbox(tr('Judeo-Arabic'), value=responsa_ja_cb.value)
                bld_flex_cb = ui.checkbox(tr('Flex Spacing'), value=responsa_flex_cb.value)
                bld_bidir_cb = ui.checkbox(tr('Bidirectional Gap'), value=bidirectional_cb.value)

                # Two-way sync: builder checkboxes ↔ outer Responsa checkboxes
                def _sync_to_outer(outer_cb, bld_cb):
                    def handler():
                        outer_cb.set_value(bld_cb.value)
                    return handler
                bld_variants_cb.on('update:model-value', _sync_to_outer(responsa_variants_cb, bld_variants_cb))
                bld_ja_cb.on('update:model-value', _sync_to_outer(responsa_ja_cb, bld_ja_cb))
                bld_flex_cb.on('update:model-value', _sync_to_outer(responsa_flex_cb, bld_flex_cb))
                bld_bidir_cb.on('update:model-value', _sync_to_outer(bidirectional_cb, bld_bidir_cb))

            # === Live Preview ===
            with ui.row().classes('w-full mt-3 items-center'):
                ui.label(tr('Preview') + ':').classes('text-sm font-medium')
                preview_label = ui.label(tr('No words entered')).classes('text-sm font-mono').style(
                    'direction: rtl; color: var(--primary-600);'
                )
                preview_label_ref['el'] = preview_label

            # === Bottom Buttons ===
            with ui.row().classes('w-full justify-between mt-4'):
                ui.button(tr('Clear All'), on_click=clear_all, icon='delete_sweep').props('flat')
                with ui.row().classes('gap-2'):
                    ui.button(tr('Cancel'), on_click=builder_dialog.close).props('flat')
                    ui.button(tr('Search'), on_click=on_apply, icon='search').props('color=primary')

        builder_dialog.open()

    def _update_domain_filter_btn():
        """Update domain filter button text and styling based on exclusion state."""
        if search_state.domain_exclusions:
            n = len(search_state.domain_exclusions)
            domain_filter_btn.text = f"{tr('Filter by domains')} ({n} {tr('excluded')})"
            domain_filter_btn.props('outline dense no-caps color=red')
        else:
            domain_filter_btn.text = tr('Filter by domains')
            domain_filter_btn.props('outline dense no-caps color=primary')

    def _open_domain_filter_dialog():
        """Open modal dialog with domain filter checkboxes.

        Uses a single HTML container with client-side JavaScript for checkbox
        interactions to avoid the overhead of creating ~200 individual NiceGUI
        ui.checkbox elements (which each generate a separate Vue component and
        WebSocket message, causing 7-19 second dialog open times).

        Data flow:
        - Python builds checkbox HTML from the domain hierarchy (fast, ~0ms)
        - Single ui.html() renders all checkboxes as one DOM insertion
        - Parent-child propagation and Select All/None run client-side via JS
        - Only on Apply: JS reads checkbox states, sends to Python via callback
        """
        if not search_state.has_domain_data:
            if search_state.domain_exclusions:
                ui.notify(tr('Run a search first to see domain options.'), type='info', timeout=3000)
            return

        # Use pre-cached hierarchy (fetched during execute_search) -- no DB call
        hierarchy = search_state.domain_hierarchy
        if not hierarchy:
            # Fallback: fetch synchronously if cache is empty
            from shared.fjms_service import get_fjms_service
            fjms = get_fjms_service(thread_safe=True)
            hierarchy = fjms.get_domain_hierarchy() if fjms.is_available() else {}
            search_state.domain_hierarchy = hierarchy

        # Count results per domain from all_result_domains
        domain_counts = {}  # domain_name -> count of results
        for sys_id, domain_names in search_state.all_result_domains.items():
            for d in domain_names:
                domain_counts[d] = domain_counts.get(d, 0) + 1

        # Build filtered hierarchy: only domains present in current results
        from shared.fjms_service import qualify_domain_name, AMBIGUOUS_CHILD_DOMAINS
        result_hierarchy = {}
        for parent_name, info in hierarchy.items():
            parent_in_results = parent_name in domain_counts
            children_in_results = []
            for child in info.get('children', []):
                qname = qualify_domain_name(child['domain'], parent_name)
                child_key = None
                if qname in domain_counts:
                    child_key = qname
                elif child['domain'] in domain_counts and child['domain'] not in AMBIGUOUS_CHILD_DOMAINS:
                    child_key = child['domain']
                # Collect sub-sub-domains in results
                subchildren_in_results = []
                for sc in child.get('children', []):
                    sc_qname = qualify_domain_name(sc['domain'], child['domain'])
                    if sc_qname in domain_counts:
                        subchildren_in_results.append({
                            'domain': sc_qname,
                            'domain_heb': sc.get('domain_heb', sc['domain']),
                            'count': domain_counts[sc_qname],
                        })
                    elif sc['domain'] in domain_counts and sc['domain'] not in AMBIGUOUS_CHILD_DOMAINS:
                        subchildren_in_results.append({
                            'domain': sc['domain'],
                            'domain_heb': sc.get('domain_heb', sc['domain']),
                            'count': domain_counts[sc['domain']],
                        })
                if child_key:
                    children_in_results.append({
                        'domain': child_key,
                        'domain_heb': child.get('domain_heb', child['domain']),
                        'count': domain_counts[child_key],
                        'children': subchildren_in_results,
                    })
                elif subchildren_in_results:
                    # Sub-sub-domains present but parent sub-domain not — still show parent
                    children_in_results.append({
                        'domain': qname,
                        'domain_heb': child.get('domain_heb', child['domain']),
                        'count': sum(sc['count'] for sc in subchildren_in_results),
                        'children': subchildren_in_results,
                    })
            if parent_in_results or children_in_results:
                parent_count = domain_counts.get(parent_name, 0)
                if children_in_results and parent_count == 0:
                    parent_count = sum(c['count'] for c in children_in_results)
                result_hierarchy[parent_name] = {
                    'parent_domain_heb': info.get('parent_domain_heb', parent_name),
                    'count': parent_count,
                    'children': children_in_results,
                }

        # Also handle domains in results that are NOT in the hierarchy (orphans)
        known_domains = set()
        for parent_name, info in result_hierarchy.items():
            known_domains.add(parent_name)
            for c in info['children']:
                known_domains.add(c['domain'])
                for sc in c.get('children', []):
                    known_domains.add(sc['domain'])
        for domain_name, count in domain_counts.items():
            if domain_name not in known_domains:
                result_hierarchy[domain_name] = {
                    'parent_domain_heb': domain_name,
                    'count': count,
                    'children': [],
                }

        # Count results with NO domain data ("Uncategorized")
        uncategorized_count = sum(
            1 for sys_id in [r.get('display', {}).get('id') for r in search_state.results]
            if sys_id and sys_id not in search_state.all_result_domains
        )
        if uncategorized_count > 0:
            result_hierarchy['Uncategorized'] = {
                'parent_domain_heb': tr('Uncategorized'),
                'count': uncategorized_count,
                'children': [],
            }

        total_results = len(search_state.results)
        current_exclusions = search_state.domain_exclusions.copy()

        # Build checkbox HTML -- all checkboxes as a single HTML string
        # Each checkbox uses data-domain attribute for identification
        # Parents use data-children attribute listing child domain names (JSON)
        # Use unique container ID to avoid conflicts with stale dialog DOM nodes
        import json as _json
        import uuid as _uuid
        container_id = f'domain-filter-{_uuid.uuid4().hex[:8]}'
        checkbox_html_parts = []
        for parent_name, info in sorted(result_hierarchy.items(), key=lambda x: -x[1]['count']):
            children = info.get('children', [])
            parent_checked = 'checked' if parent_name not in current_exclusions else ''
            parent_label = f"{_domain_display_name(parent_name)} ({info['count']})"
            # Collect ALL descendant domain names for parent checkbox propagation
            all_descendant_names = []
            for c in children:
                all_descendant_names.append(c['domain'])
                for sc in c.get('children', []):
                    all_descendant_names.append(sc['domain'])
            # Escape for HTML attributes
            parent_domain_attr = html.escape(parent_name, quote=True)
            children_json_attr = html.escape(_json.dumps(all_descendant_names), quote=True)
            parent_label_html = html.escape(parent_label)
            checkbox_html_parts.append(
                f'<label class="domain-parent" style="display:flex;align-items:center;gap:6px;'
                f'font-weight:bold;padding:4px 0;cursor:pointer">'
                f'<input type="checkbox" data-domain="{parent_domain_attr}" '
                f'data-children="{children_json_attr}" '
                f'{parent_checked} onchange="domainFilterParentChanged(this)" '
                f'style="width:18px;height:18px;accent-color:#1976d2">'
                f'<span>{parent_label_html}</span></label>'
            )
            for child in children:
                child_checked = 'checked' if child['domain'] not in current_exclusions else ''
                child_label = f"{_domain_display_name(child['domain'])} ({child['count']})"
                child_domain_attr = html.escape(child['domain'], quote=True)
                child_label_html = html.escape(child_label)
                subchildren = child.get('children', [])
                if subchildren:
                    # Sub-domain with sub-sub-domains: acts as sub-parent
                    sc_names = [sc['domain'] for sc in subchildren]
                    sc_json_attr = html.escape(_json.dumps(sc_names), quote=True)
                    checkbox_html_parts.append(
                        f'<label class="domain-child" style="display:flex;align-items:center;gap:6px;'
                        f'font-weight:bold;padding:2px 0;padding-inline-start:2rem;cursor:pointer">'
                        f'<input type="checkbox" data-domain="{child_domain_attr}" '
                        f'data-children="{sc_json_attr}" '
                        f'{child_checked} onchange="domainFilterParentChanged(this)" '
                        f'style="width:16px;height:16px;accent-color:#1976d2">'
                        f'<span>{child_label_html}</span></label>'
                    )
                    for sc in subchildren:
                        sc_checked = 'checked' if sc['domain'] not in current_exclusions else ''
                        sc_label = f"{_domain_display_name(sc['domain'])} ({sc['count']})"
                        sc_domain_attr = html.escape(sc['domain'], quote=True)
                        sc_label_html = html.escape(sc_label)
                        checkbox_html_parts.append(
                            f'<label class="domain-subchild" style="display:flex;align-items:center;gap:6px;'
                            f'padding:2px 0;padding-inline-start:4rem;cursor:pointer">'
                            f'<input type="checkbox" data-domain="{sc_domain_attr}" '
                            f'{sc_checked} '
                            f'style="width:14px;height:14px;accent-color:#1976d2">'
                            f'<span>{sc_label_html}</span></label>'
                        )
                else:
                    checkbox_html_parts.append(
                        f'<label class="domain-child" style="display:flex;align-items:center;gap:6px;'
                        f'padding:2px 0;padding-inline-start:2rem;cursor:pointer">'
                        f'<input type="checkbox" data-domain="{child_domain_attr}" '
                        f'{child_checked} '
                        f'style="width:16px;height:16px;accent-color:#1976d2">'
                        f'<span>{child_label_html}</span></label>'
                    )

        checkbox_html = '\n'.join(checkbox_html_parts)

        # Build the dialog with minimal NiceGUI elements (5 elements vs ~200)
        with ui.dialog() as dialog, ui.card().classes('w-[600px] max-h-[80vh]'):
            with ui.column().classes('w-full gap-2'):
                ui.label(tr('Filter by Domain')).classes('text-lg font-bold')
                ui.label(
                    f"{tr('Showing')} {total_results} {tr('of')} {total_results} {tr('results')}"
                ).classes('text-sm text-gray-500')

                # Single HTML container with all checkboxes (JS helpers loaded at page level)
                with ui.scroll_area().classes('w-full').style('max-height: 50vh;'):
                    ui.html(f'<div id="{container_id}">{checkbox_html}</div>', sanitize=False)

                # Buttons
                with ui.row().classes('w-full justify-between'):
                    _cid = container_id  # capture for closures

                    with ui.row().classes('gap-2'):
                        ui.button(
                            tr('Select All'),
                            on_click=lambda: ui.run_javascript(
                                f'domainFilterSelectAll("{_cid}", true)')
                        ).props('flat dense no-caps')
                        ui.button(
                            tr('Select None'),
                            on_click=lambda: ui.run_javascript(
                                f'domainFilterSelectAll("{_cid}", false)')
                        ).props('flat dense no-caps')

                    with ui.row().classes('gap-2'):
                        async def apply_filter():
                            excluded_list = await ui.run_javascript(
                                f'domainFilterGetExcluded("{_cid}")', timeout=5.0
                            )
                            excluded = set(excluded_list) if excluded_list else set()
                            search_state.domain_exclusions = excluded
                            app.storage.user['domain_exclusions'] = list(excluded)
                            _apply_domain_exclusions()
                            _update_domain_filter_btn()
                            dialog.close()

                        ui.button(tr('Apply'), on_click=apply_filter).props('dense no-caps color=primary')
                        ui.button(tr('Cancel'), on_click=dialog.close).props('flat dense no-caps')

        dialog.open()

    # --- Measurement post-filter helpers (Phase 54, review concern #1) ---

    def _has_any_post_measurement_filter(state) -> bool:
        """Check if any post-search measurement filters are active."""
        return any([
            getattr(state, 'post_filter_width_min', None) is not None,
            getattr(state, 'post_filter_width_max', None) is not None,
            getattr(state, 'post_filter_height_min', None) is not None,
            getattr(state, 'post_filter_height_max', None) is not None,
            getattr(state, 'post_filter_line_count_min', None) is not None,
            getattr(state, 'post_filter_line_count_max', None) is not None,
            getattr(state, 'post_filter_line_height_min', None) is not None,
            getattr(state, 'post_filter_line_height_max', None) is not None,
            getattr(state, 'post_filter_text_density_min', None) is not None,
            getattr(state, 'post_filter_text_density_max', None) is not None,
            bool(getattr(state, 'post_filter_measurement_material', None)),
        ])

    def _apply_measurement_post_filters(results: list, state) -> list:
        """Apply measurement post-filters to a results list.
        Reads from state.post_filter_* and state._measurement_cache.
        Returns filtered list. Called from ALL rerender paths.
        (Review concern #1: domain exclusion, printed toggle, history restore, enrichment all call this.)
        """
        _pw_min = state.post_filter_width_min
        _pw_max = state.post_filter_width_max
        _ph_min = state.post_filter_height_min
        _ph_max = state.post_filter_height_max
        _plc_min = state.post_filter_line_count_min
        _plc_max = state.post_filter_line_count_max
        _plh_min = state.post_filter_line_height_min
        _plh_max = state.post_filter_line_height_max
        _ptd_min = state.post_filter_text_density_min
        _ptd_max = state.post_filter_text_density_max
        _pm_mat = state.post_filter_measurement_material or []

        _has_meas_filter = any([
            _pw_min is not None, _pw_max is not None,
            _ph_min is not None, _ph_max is not None,
            _plc_min is not None, _plc_max is not None,
            _plh_min is not None, _plh_max is not None,
            _ptd_min is not None, _ptd_max is not None,
            _pm_mat,
        ])

        if not _has_meas_filter:
            return results

        cache = getattr(state, '_measurement_cache', {})

        def _in_range(val, vmin, vmax):
            if val is None and (vmin is not None or vmax is not None):
                return False
            if vmin is not None and val < vmin:
                return False
            if vmax is not None and val > vmax:
                return False
            return True

        filtered = []
        for res in results:
            sid = res.get('sys_id') or res.get('display', {}).get('id', '')
            md = cache.get(sid)
            if md is None:
                continue  # D-26: exclude without data
            if not _in_range(md.get('width_cm'), _pw_min, _pw_max): continue
            if not _in_range(md.get('height_cm'), _ph_min, _ph_max): continue
            if not _in_range(md.get('avg_num_lines'), _plc_min, _plc_max): continue
            if not _in_range(md.get('avg_line_height_mm'), _plh_min, _plh_max): continue
            if not _in_range(md.get('avg_text_density'), _ptd_min, _ptd_max): continue
            if _pm_mat and (md.get('material') is None or md.get('material') not in _pm_mat): continue
            filtered.append(res)
        return filtered

    def _apply_printed_filter(results_list):
        """Apply printed material filter to a results list and return filtered list."""
        if search_state.printed_filter == 'all' or not search_state.printed_ids:
            return results_list
        filtered = []
        for r in results_list:
            sys_id = r.get('display', {}).get('id')
            is_printed = sys_id and sys_id in search_state.printed_ids
            if search_state.printed_filter == 'hide_printed' and is_printed:
                continue
            elif search_state.printed_filter == 'only_printed' and not is_printed:
                continue
            filtered.append(r)
        return filtered

    def _apply_printed_filter_and_render(results_list, reset_expansion=True):
        """Apply printed filter to results and re-render (used when no domain exclusions active)."""
        filtered = _apply_printed_filter(results_list)
        # Apply measurement post-filters (Phase 54, review concern #1)
        filtered = _apply_measurement_post_filters(filtered, search_state)
        total = len(search_state.results)
        showing = len(filtered)
        if search_state.printed_filter != 'all':
            filter_label = tr('Hiding printed') if search_state.printed_filter == 'hide_printed' else tr('Only printed')
            results_count.text = f"{showing} {tr('of')} {total} {tr('Results')} ({filter_label})"
        else:
            results_count.text = f"{total} {tr('Results')}"
        render_results(filtered, page=0, reset_expansion=reset_expansion)

    def _apply_word_search_exclusions_and_render():
        """Apply word search per-result exclusions and re-render."""
        if not search_state.word_search_excluded_ids or not search_state.results:
            search_state.word_search_excluded_results = []
            if search_state.results:
                _display = _apply_measurement_post_filters(search_state.results, search_state)
                render_results(_display, page=0)
            return

        filtered = []
        excluded_items = []
        for r in search_state.results:
            sys_id = r.get('display', {}).get('id')
            if sys_id and sys_id in search_state.word_search_excluded_ids:
                excluded_items.append({
                    'result': r,
                    'reason': tr('Excluded'),
                })
            else:
                filtered.append(r)
        search_state.word_search_excluded_results = excluded_items

        # Apply domain exclusions and printed filter on top of word search filtering
        if search_state.domain_exclusions and search_state.has_domain_data:
            # Temporarily swap results to filtered set, then apply domain exclusions
            original_results = search_state.results
            search_state.results = filtered
            _apply_domain_exclusions()
            search_state.results = original_results
        elif search_state.printed_filter != 'all' and search_state.printed_ids:
            filtered = _apply_printed_filter(filtered)
            filtered = _apply_measurement_post_filters(filtered, search_state)
            total = len(search_state.results)
            showing = len(filtered)
            n_excl = len(excluded_items)
            results_count.text = f"{showing} {tr('Results')} ({n_excl} {tr('excluded')})"
            render_results(filtered, page=0)
        else:
            filtered = _apply_measurement_post_filters(filtered, search_state)
            total = len(search_state.results)
            showing = len(filtered)
            n_excl = len(excluded_items)
            results_count.text = f"{showing} {tr('of')} {total} {tr('Results')} ({n_excl} {tr('excluded')})"
            render_results(filtered, page=0)

    def _apply_domain_exclusions(reset_expansion=True):
        """Filter displayed results based on domain exclusions without re-searching."""
        if not search_state.domain_exclusions:
            # No exclusions -- show all results
            filtered = search_state.results
            search_state.domain_excluded_results = []
        else:
            hide_uncategorized = 'Uncategorized' in search_state.domain_exclusions
            filtered = []
            excluded_with_reasons = []
            for r in search_state.results:
                sys_id = r.get('display', {}).get('id')
                result_domains = search_state.all_result_domains.get(sys_id, []) if sys_id else []
                if not result_domains:
                    # No domain data -- hide if Uncategorized is excluded
                    if not hide_uncategorized:
                        filtered.append(r)
                    else:
                        excluded_with_reasons.append({
                            'result': r,
                            'reason': tr('Uncategorized')
                        })
                    continue
                elif all(d in search_state.domain_exclusions for d in result_domains):
                    # ALL domains excluded -- track with reason
                    domain_names = [_domain_display_name(d) for d in result_domains]
                    excluded_with_reasons.append({
                        'result': r,
                        'reason': ', '.join(domain_names)
                    })
                    continue
                else:
                    # At least one domain not excluded -- keep
                    filtered.append(r)
            search_state.domain_excluded_results = excluded_with_reasons
            # Save excluded result reasons for restore (lightweight, capped)
            excluded_reasons = [{'sys_id': r.get('result', {}).get('display', {}).get('id', ''), 'reason': r.get('reason', '')} for r in excluded_with_reasons[:500]]
            persist_value('search_excluded_reasons', excluded_reasons)

        # Apply printed filter on top of domain-filtered results
        filtered = _apply_printed_filter(filtered)
        # Apply measurement post-filters (Phase 54, review concern #1)
        filtered = _apply_measurement_post_filters(filtered, search_state)

        # Update count display
        total = len(search_state.results)
        showing = len(filtered)
        count_parts = []
        if search_state.domain_exclusions:
            count_parts.append(f"{len(search_state.domain_exclusions)} {tr('domains excluded')}")
        if search_state.printed_filter != 'all':
            count_parts.append(tr('Hiding printed') if search_state.printed_filter == 'hide_printed' else tr('Only printed'))
        if count_parts:
            results_count.text = f"{showing} {tr('of')} {total} {tr('Results')} ({', '.join(count_parts)})"
        else:
            results_count.text = f"{total} {tr('Results')}"

        # Update result_domains for badge rendering (use visible page slice)
        page_start = search_state.current_page * PAGE_SIZE
        page_end = page_start + PAGE_SIZE
        page_slice = filtered[page_start:page_end]
        result_sys_ids = [r.get('display', {}).get('id') for r in page_slice if r.get('display', {}).get('id')]
        search_state.result_domains = {sid: doms for sid, doms in search_state.all_result_domains.items() if sid in set(result_sys_ids)}

        # Re-render with filtered results (resets to page 0)
        render_results(filtered, page=0, reset_expansion=reset_expansion)

    # --- Search History UI Helpers ---
    def _refresh_history_menu():
        """Refresh the history dropdown menu contents."""
        history_menu.clear()
        history = _get_search_history()
        if not history:
            with history_menu:
                ui.menu_item(tr('No search history')).props('disable')
            return

        def _build_web_filter_summary(filters: dict, max_len: int = 50) -> str:
            return build_filter_summary(filters, tr, get_language, max_len)

        with history_menu:
            for i, entry in enumerate(history):
                query_text = entry.get('query', '')
                query_display = (query_text[:35] + '...') if len(query_text) > 35 else query_text
                count = entry.get('result_count', 0)
                mode = entry.get('mode', '')
                mode_short = {'exact': '=', 'variants': '?', 'variants_extended': '??',
                              'variants_maximum': '???', 'fuzzy': '~', 'Regex': '/',
                              'Shelfmark': '#', 'Title': '$', 'responsa': 'R'}.get(mode, mode)
                # Build filter summary text from params
                filters = entry.get('params', {}).get('filters')
                filter_text = _build_web_filter_summary(filters) if filters else ''
                label = f"{query_display}  ({count}) [{mode_short}]"

                idx = i  # Capture for closure
                with ui.menu_item(label, on_click=lambda e, idx=idx: _on_history_item_clicked(idx)).style('direction: rtl;'):
                    if filter_text:
                        ui.label(filter_text).style('font-size: 0.7rem; color: var(--primary-600); direction: ltr;')
                    # Delete button on each item
                    ui.button(icon='close', on_click=lambda e, idx=idx: (
                        _delete_search_history_entry(idx), _refresh_history_menu()
                    )).props('flat dense size=xs round').classes('ml-auto')

            ui.separator()
            ui.menu_item(tr('Clear all'), on_click=lambda: (
                _clear_search_history(), _refresh_history_menu()
            ))

    async def _on_history_item_clicked(index: int):
        """Restore state from a search history entry."""
        history = _get_search_history()
        if index >= len(history):
            return
        entry = history[index]
        state_snapshot = entry.get('state', {})
        params = entry.get('params', {})

        # Restore query and params
        query_input.value = entry.get('query', '')
        if params.get('mode'):
            mode_select.value = params['mode']
        if params.get('preset') and current_preset:
            current_preset['value'] = params['preset']
        if params.get('gap') is not None:
            gap_input.value = params['gap']
        if params.get('text_position'):
            text_position_select.value = params['text_position']

        # Restore filters if present
        filters = params.get('filters')
        if filters and isinstance(filters, dict):
            # Migrate from legacy single-value to lists
            _d = filters.get('domains') or ([filters['domain']] if filters.get('domain') else [])
            _a = filters.get('authors') or ([filters['author']] if filters.get('author') else [])
            _w = filters.get('works') or ([filters['work']] if filters.get('work') else [])
            search_state.filter_domains = _d
            search_state.filter_authors = _a
            search_state.filter_works = _w
            search_state.filter_include_mode = filters.get('include_mode', True)
            search_state.filter_date_from = filters.get('date_from')
            search_state.filter_date_to = filters.get('date_to')
            search_state.filter_material_exclude = filters.get('material_exclude', [])
            search_state.filter_text_all = filters.get('text_all', [])
            search_state.filter_text_any = filters.get('text_any', [])
            search_state.filter_text_not = filters.get('text_not', [])
            # Update filter UI elements
            domain_select.value = search_state.filter_domains
            author_select.value = search_state.filter_authors
            work_select.value = search_state.filter_works
            filter_mode_toggle.value = search_state.filter_include_mode
            date_from_input.value = search_state.filter_date_from
            date_to_input.value = search_state.filter_date_to
            exclude_printed_cb.value = 'Printed' in search_state.filter_material_exclude
            # Persist restored filters
            persist_value('search_filter_domains', search_state.filter_domains)
            persist_value('search_filter_authors', search_state.filter_authors)
            persist_value('search_filter_works', search_state.filter_works)
            persist_value('search_filter_include_mode', search_state.filter_include_mode)
            persist_value('search_filter_date_from', search_state.filter_date_from)
            persist_value('search_filter_date_to', search_state.filter_date_to)
            persist_value('search_filter_material_exclude', search_state.filter_material_exclude)
            persist_value('search_filter_text_all', search_state.filter_text_all)
            persist_value('search_filter_text_any', search_state.filter_text_any)
            persist_value('search_filter_text_not', search_state.filter_text_not)
            _update_chip_bar()
            _rebuild_text_chips()
        else:
            # Clear filters if history entry had none
            _clear_all_adv_filters()

        # Restore results and state from snapshot
        if state_snapshot.get('results'):
            search_state.results = state_snapshot['results']
            search_state.domain_exclusions = set(state_snapshot.get('domain_exclusions', []))
            search_state.printed_filter = state_snapshot.get('printed_filter', 'all')
            # Update count display
            results_count.text = f"{len(search_state.results)} {tr('Results')}"
            # Re-render with restored exclusions
            if search_state.domain_exclusions and search_state.has_domain_data:
                _apply_domain_exclusions()
            elif search_state.printed_filter != 'all' and search_state.printed_ids:
                _apply_printed_filter_and_render(search_state.results)
            else:
                # Apply measurement post-filters on history restore (Phase 54)
                _restored = _apply_measurement_post_filters(search_state.results, search_state)
                render_results(_restored, page=0)

        ui.notify(tr('Search restored from history'), type='info', timeout=2000)
        history_menu.close()

    async def execute_search():
        # Guard against double-submit (rage-clicking while search is running)
        if search_state.is_running:
            return

        # Handle PGP tag search mode — navigate to tag results page
        if mode_select.value == 'pgp_tags':
            tag = tag_select.value
            if tag and not str(tag).startswith('__sep_'):
                ui.navigate.to(f'/search?tag={quote(tag)}')
            return

        query = query_input.value.strip() if query_input.value else ""

        if not query:
            return

        if not state.is_ready():
            ui.notify(tr("Engine not ready."), type='warning')
            return

        # Parse syntax shortcuts (Delegated to Core)
        # Skip prefix parsing in Responsa mode -- # is Responsa syntax, not Shelfmark
        clean_query = query
        is_responsa = (mode_select.value == 'responsa')
        mode_override, parsed_query = state.searcher.parse_query_syntax(query, responsa_mode=is_responsa)

        if mode_override:
            mode = mode_override
            clean_query = parsed_query
            # In slider mode, map all variant modes to 'variants'
            if use_slider and mode in ('variants', 'variants_extended', 'variants_maximum'):
                mode = 'variants'
            mode_select.value = mode
        else:
            mode = mode_select.value

        # Update variant level and max changes from UI before search
        is_variant_mode = mode in ('variants', 'variants_extended', 'variants_maximum')
        if is_variant_mode and state.var_mgr:
            # Get pairs count: from slider, or from mode name, or from current_preset
            if variant_slider:
                pairs_count = int(variant_slider.value)
            else:
                pairs_count = get_level_from_mode(mode)
            state.var_mgr.set_variant_level(pairs_count)
            # Update max_changes in settings
            if state.lab_engine and state.lab_engine.settings and max_changes_select:
                state.lab_engine.settings.variant_max_changes = int(max_changes_select.value)

        # Reset UI
        search_state.is_running = True
        search_state.is_cancelled = False  # Reset cancellation flag
        search_state.progress = 0
        search_state.status = tr("Starting...")
        search_state.search_start_time = time.time()
        search_state.results = []
        search_state.search_generation += 1  # Invalidate stale background enrichment

        # Immediate visual feedback — swap buttons before the 500ms timer tick
        search_btn.style('display: none;')
        stop_btn.style('display: inline-flex;')
        progress_bar.classes(remove='opacity-0')
        progress_bar.value = 0
        # Collapse filter panel — chips summarize active filters
        adv_filters_panel.value = False
        # Scroll progress into view
        ui.run_javascript(f'document.getElementById("c{progress_container.id}").scrollIntoView({{behavior: "smooth", block: "start"}})')

        # Show loading spinner immediately
        render_results([])

        def progress_cb(current, total):
            # Check if search was cancelled
            if search_state.is_cancelled:
                raise InterruptedError("Search cancelled")
            if total > 0:
                search_state.progress = current / total
                search_state.status = f"{current} / {total}"

        # Get NOT filter words (merge manual filter + builder negated words)
        not_words = not_filter.value.split() if not_filter.value else []
        if search_state.builder_negated_words:
            not_words = not_words + search_state.builder_negated_words

        # Build Responsa options dict from mode selection
        responsa_options = None
        if mode_select.value == 'responsa':
            responsa_options = {
                'responsa_mode': True,
                'variants': responsa_variants_cb.value,
                'ja': responsa_ja_cb.value,
                'flex_spacing': responsa_flex_cb.value,
                'bidirectional': bidirectional_cb.value,
                'variant_mode': 'variants' if responsa_variants_cb.value else 'exact',
            }

        # Compute pre-search filter set from active filters
        restrict_sys_ids = None
        if _has_active_filters():
            from shared.fjms_service import get_fjms_service

            def _compute_restrict():
                fjms = get_fjms_service(thread_safe=True)
                if not fjms.is_available():
                    return None
                _inc = search_state.filter_include_mode
                kwargs = dict(
                    date_from=search_state.filter_date_from,
                    date_to=search_state.filter_date_to,
                    material_exclude=search_state.filter_material_exclude or None,
                    text_all=search_state.filter_text_all or None,
                    text_any=search_state.filter_text_any or None,
                    text_not=search_state.filter_text_not or None,
                )
                if _inc:
                    kwargs['domains'] = search_state.filter_domains or None
                    kwargs['authors'] = search_state.filter_authors or None
                    kwargs['works'] = search_state.filter_works or None
                else:
                    kwargs['domains_exclude'] = search_state.filter_domains or None
                    kwargs['authors_exclude'] = search_state.filter_authors or None
                    kwargs['works_exclude'] = search_state.filter_works or None
                # Measurement filter params (Phase 54)
                kwargs.update(dict(
                    width_min=search_state.filter_width_min,
                    width_max=search_state.filter_width_max,
                    height_min=search_state.filter_height_min,
                    height_max=search_state.filter_height_max,
                    line_count_min=search_state.filter_line_count_min,
                    line_count_max=search_state.filter_line_count_max,
                    line_height_min=search_state.filter_line_height_min,
                    line_height_max=search_state.filter_line_height_max,
                    text_density_min=search_state.filter_text_density_min,
                    text_density_max=search_state.filter_text_density_max,
                    measurement_material=search_state.filter_measurement_material or None,
                ))
                return fjms.get_filter_sys_ids(**kwargs)

            restrict_sys_ids = await run.io_bound(_compute_restrict)
            search_state.restrict_sys_ids = restrict_sys_ids

        # Phase 55: compute effective restrict = intersection of filter restrict + refinement restrict
        effective_restrict = compute_effective_restrict(restrict_sys_ids, search_state.refinement_restrict_sys_ids)

        # If effective restriction matches nothing, show message and return
        if effective_restrict is not None and len(effective_restrict) == 0:
            ui.notify(tr("No manuscripts match the current filters."), type='warning')
            search_state.is_running = False
            search_state.is_cancelled = False
            search_btn.style('display: inline-flex;')
            stop_btn.style('display: none;')
            progress_bar.classes('opacity-0')
            render_results([])
            return

        def run_core_search():
            try:
                if lab_mode.value:
                    lab_search_mode = 'variants' if mode not in ['Regex', 'exact'] else mode
                    return state.lab_engine.lab_search(
                        clean_query,
                        mode=lab_search_mode,
                        gap=int(gap_input.value),
                        deep_scan=deep_scan.value,
                        progress_callback=progress_cb
                    )
                else:
                    tp = text_position_select.value
                    return state.searcher.execute_search(
                        clean_query,
                        mode=mode,
                        gap=int(gap_input.value),
                        progress_callback=progress_cb,
                        exclude_words=not_words,
                        responsa_options=responsa_options,
                        restrict_sys_ids=effective_restrict,
                        text_position=tp if tp != 'anywhere' else None,
                    )
            except ValueError as e:
                # Explosion guard or other validation error — surface to user
                error_msg = str(e)
                logger.error(f"Search Validation Error: {error_msg}")
                return {'error': error_msg}
            except Exception as e:
                logger.exception(f"Search Error: {e}")
                return []

        results = await run.io_bound(run_core_search)

        # Handle validation errors from explosion guard (returned as sentinel dict
        # because run_core_search runs in io_bound thread and cannot call ui.notify)
        if isinstance(results, dict) and 'error' in results:
            error_msg = results['error']
            ui.notify(error_msg, type='warning', timeout=8000, close_button=True)
            search_state.is_running = False
            search_state.is_cancelled = False
            search_state.progress = 0
            results_count.text = f"0 {tr('Results')}"
            render_results([])
            return

        # Skip expensive enrichment when search was cancelled (GAP-R7 round 3)
        if search_state.is_cancelled:
            search_state.is_running = False
            search_state.is_cancelled = False
            search_state.progress = 1.0
            search_state.results = results
            search_state.domain_excluded_results = []

            # Still save results for display
            state.last_results = results

            # Compute elapsed
            total_elapsed = time.time() - search_state.search_start_time if search_state.search_start_time else 0
            if total_elapsed >= 3600:
                total_elapsed_str = f"{int(total_elapsed // 3600)}:{int((total_elapsed % 3600) // 60):02d}:{int(total_elapsed % 60):02d}"
            else:
                total_elapsed_str = f"{int(total_elapsed // 60)}:{int(total_elapsed % 60):02d}"

            results_count.text = f"{len(results)} {tr('Results')} · {total_elapsed_str} ({tr('partial')})"
            status_label.text = ''
            ui.notify(tr('Showing partial results'), type='warning', timeout=3000)

            # Fast title-only translation fetch for partial results (~1ms SQLite)
            _partial_sids = [r.get('display', {}).get('id') for r in results if r.get('display', {}).get('id')]
            if _partial_sids:
                try:
                    def _fetch_partial_titles():
                        from shared.translation_service import TranslationService
                        svc = TranslationService(thread_safe=True)
                        tt = svc.get_title_translations_batch(_partial_sids) if svc.titles_available() else {}
                        svc.close()
                        return tt
                    search_state.title_translations = await run.io_bound(_fetch_partial_titles)
                except Exception:
                    pass

            # Render what we have (no enrichment badges -- acceptable for partial results)
            render_results(results, page=0)
            return

        # --- Staged enrichment: render fast, enrich progressively ---
        all_sys_ids = [r.get('display', {}).get('id') for r in results if r.get('display', {}).get('id')]
        this_generation = search_state.search_generation
        _t_stage0 = time.perf_counter()

        # --- STAGE 0: Fast title translations + immediate render ---
        # (Reuses the fast path from cancel flow — ~1ms SQLite lookup)
        if all_sys_ids:
            try:
                def _fetch_titles_fast():
                    from shared.translation_service import TranslationService
                    svc = TranslationService(thread_safe=True)
                    tt = svc.get_title_translations_batch(all_sys_ids) if svc.titles_available() else {}
                    svc.close()
                    return tt
                search_state.title_translations = await run.io_bound(_fetch_titles_fast)
            except Exception:
                pass

        # Finalize search state for immediate display
        state.last_results = results
        search_state.is_running = False
        search_state.is_cancelled = False
        search_state.progress = 1.0
        search_state.results = results

        # --- Phase 55: Refinement chain update ---
        if search_state._refine_mode:
            # Use RAW result sys_ids (before domain/printed/measurement post-filters)
            raw_result_sys_ids = set(all_sys_ids)  # all_sys_ids computed above
            if len(raw_result_sys_ids) == 0:
                # Zero-result refinement (D-14a) -- don't commit step, show recovery UI
                search_state._zero_result_refine = True
            else:
                step = RefinementStep(
                    query=clean_query,
                    mode=mode,
                    gap=int(gap_input.value),
                    exclude_words=not_words if not_words else [],
                    text_position=text_position_select.value if text_position_select.value != 'anywhere' else None,
                    responsa_options=responsa_options,
                    result_count=len(raw_result_sys_ids),
                )
                search_state.refinement_chain.append(step)
                search_state.refinement_restrict_sys_ids = raw_result_sys_ids
                search_state._refinement_scope_sig = scope_signature(search_state.restrict_sys_ids)
                search_state._zero_result_refine = False
                # Persist chain metadata only (D-14) -- no sys_id lists stored
                persist_value('search_refinement_chain', [s.to_dict() for s in search_state.refinement_chain])
            search_state._refine_mode = False
            search_state._refinement_stale = False

        # Initialize enrichment fields to empty (will be populated progressively)
        search_state.transcription_sys_ids = set()
        search_state.all_result_domains = {}
        search_state.result_domains = {}
        search_state.domain_name_map = {}
        search_state.has_domain_data = False
        search_state.catalog_source_counts = {}
        search_state.printed_ids = set()
        search_state.translation_data = {}
        search_state._measurement_cache = {}  # Phase 54: reset on new search
        search_state.domain_excluded_results = []
        search_state.word_search_excluded_results = []

        # Compute elapsed time
        total_elapsed = time.time() - search_state.search_start_time if search_state.search_start_time else 0
        if total_elapsed >= 3600:
            total_elapsed_str = f"{int(total_elapsed // 3600)}:{int((total_elapsed % 3600) // 60):02d}:{int(total_elapsed % 60):02d}"
        else:
            total_elapsed_str = f"{int(total_elapsed // 60)}:{int(total_elapsed % 60):02d}"

        # Build filter summary suffix
        _filter_suffix = ''
        if _has_active_filters() and search_state.filter_manuscript_count is not None:
            filter_parts = []
            opts_d = domain_select.options if hasattr(domain_select, 'options') else {}
            opts_a = author_select.options if hasattr(author_select, 'options') else {}
            opts_w = work_select.options if hasattr(work_select, 'options') else {}
            for d in search_state.filter_domains:
                filter_parts.append(_get_display_name(d, opts_d))
            for a in search_state.filter_authors:
                filter_parts.append(_get_display_name(a, opts_a))
            for w in search_state.filter_works:
                filter_parts.append(_get_display_name(w, opts_w))
            if filter_parts:
                _filter_suffix = f" ({tr('filtered')}: {', '.join(filter_parts)}, {search_state.filter_manuscript_count:,} {tr('manuscripts')})"
            else:
                _filter_suffix = f" ({tr('filtered')}: {search_state.filter_manuscript_count:,} {tr('manuscripts')})"

        # Merged status: results count + time + filter info in one label
        expanded_count = results[0].get('responsa_expanded_count', 0) if results else 0
        if expanded_count > 0:
            results_count.text = f"{len(results)} {tr('Results')} · {total_elapsed_str} ({tr('searching')} {expanded_count} {tr('expanded terms')}){_filter_suffix}"
        else:
            results_count.text = f"{len(results)} {tr('Results')} · {total_elapsed_str}{_filter_suffix}"
        status_label.text = ''

        # Responsa explosion guard warning
        if results and results[0].get('responsa_warning'):
            ui.notify(results[0]['responsa_warning'], type='warning', timeout=5000)

        # PostHog event
        from web.analytics import posthog_capture
        posthog_capture('search_executed', {
            'query': clean_query[:100],
            'mode': mode,
            'result_count': len(results),
            'duration_seconds': round(total_elapsed, 1),
            'was_cancelled': False,
        })

        # URL state persistence
        try:
            params = f'?q={quote(clean_query)}'
            tag_value = tag_select.value if mode_select.value == 'pgp_tags' else None
            if tag_value:
                params += f'&tag={quote(str(tag_value))}'
            if mode_select.value == 'responsa':
                params += '&mode=responsa'
                if responsa_variants_cb.value: params += '&variants=1'
                if responsa_ja_cb.value: params += '&ja=1'
                if responsa_flex_cb.value: params += '&flex_spaces=1'
                if bidirectional_cb.value: params += '&bidirectional=1'
            ui.run_javascript(f"history.replaceState(null, '', '/search{params}')")
        except Exception:
            pass

        # Storage persistence (cap at 1000, strip full_text)
        try:
            capped = results[:1000]
            storage_results = []
            for r in capped:
                sr = dict(r)
                sr.pop('full_text', None)
                display = sr.get('display')
                if display and isinstance(display, dict):
                    d = dict(display)
                    d.pop('full_text', None)
                    sr['display'] = d
                storage_results.append(sr)
            app.storage.user['search_results'] = storage_results
        except Exception:
            pass

        # Search history -- D-15: Refined searches don't enter history
        if not search_state.refinement_chain:
            try:
                hist_results = []
                for r in results[:500]:
                    hr = dict(r)
                    hr.pop('full_text', None)
                    d = hr.get('display')
                    if d and isinstance(d, dict):
                        d = dict(d)
                        d.pop('full_text', None)
                        hr['display'] = d
                    hist_results.append(hr)
                _add_to_search_history(
                    query=query_input.value or '',
                    result_count=len(results),
                    mode=mode_select.value or 'exact',
                    params={
                        'mode': mode_select.value,
                        'preset': current_preset.get('value', 30) if isinstance(current_preset, dict) else 30,
                        'gap': int(gap_input.value or 0),
                        'text_position': text_position_select.value,
                        'filters': {
                            'domains': search_state.filter_domains,
                            'authors': search_state.filter_authors,
                            'works': search_state.filter_works,
                            'include_mode': search_state.filter_include_mode,
                            'date_from': search_state.filter_date_from,
                            'date_to': search_state.filter_date_to,
                            'material_exclude': search_state.filter_material_exclude,
                            'text_all': search_state.filter_text_all,
                            'text_any': search_state.filter_text_any,
                            'text_not': search_state.filter_text_not,
                        } if _has_active_filters() else None,
                    },
                    state_snapshot={
                        'results': hist_results,
                        'domain_exclusions': sorted(search_state.domain_exclusions),
                        'printed_filter': search_state.printed_filter,
                    },
                )
            except Exception:
                pass

        # IMMEDIATE RENDER — user sees results with title translations only
        render_results(results, page=0)
        _t_render = time.perf_counter()
        logger.info("Search perf: first_render_ms=%.0f (results=%d)", (_t_render - _t_stage0) * 1000, len(results))

        # --- Enrichment helper functions (defined once, used for both stages) ---
        def collect_fjms_enrichment(sys_ids):
            """Collect all FJMS enrichment in one sidecar pass for this batch."""
            from shared.fjms_service import get_fjms_service
            fjms = get_fjms_service(thread_safe=True)
            if not fjms.is_available():
                return {}, {}, set(), {}
            return (
                fjms.get_domains_for_sys_ids(sys_ids),
                fjms.get_catalog_source_counts(sys_ids),
                fjms.get_printed_sys_ids(sys_ids),
                fjms.get_measurement_summaries_batch(sys_ids),  # Phase 54: measurement cache
            )

        _show_trans_for_enrich = False
        try:
            _show_trans_for_enrich = app.storage.user.get('show_translations', False)
        except Exception:
            pass

        def collect_translations(sys_ids, show_trans=False):
            try:
                from shared.translation_service import TranslationService
                svc = TranslationService(thread_safe=True)
                pgp_trans = {}
                if show_trans and svc.pgp_available():
                    pgp_trans = svc.get_pgp_translations_by_sys_ids(sys_ids)
                svc.close()
                return pgp_trans
            except Exception as e:
                logger.warning("Translation batch lookup failed: %s", e)
                return {}

        def _process_domain_data(raw_domains):
            """Process raw domain data into search_state fields."""
            from shared.fjms_service import qualify_domain_name
            for sys_id, doms in raw_domains.items():
                child_names = {d['domain'] for d in doms}
                filtered = [qualify_domain_name(d['domain'], d.get('parent_domain')) for d in doms if not (d.get('parent_domain') and d['parent_domain'] in child_names and d['parent_domain'] != d['domain'])]
                if filtered:
                    search_state.all_result_domains[sys_id] = filtered
                for d in doms:
                    qname = qualify_domain_name(d['domain'], d.get('parent_domain'))
                    if qname != d['domain'] and d.get('domain_heb') and d.get('parent_domain_heb'):
                        search_state.domain_name_map[qname] = f"{d['domain_heb']} ({d['parent_domain_heb']})"
                    if d.get('domain_heb') and d['domain'] not in search_state.domain_name_map:
                        search_state.domain_name_map[d['domain']] = d['domain_heb']
                    if d.get('parent_domain_heb') and d.get('parent_domain') and d['parent_domain'] not in search_state.domain_name_map:
                        search_state.domain_name_map[d['parent_domain']] = d['parent_domain_heb']

        def _apply_enrichment_to_ui():
            """Update UI elements after enrichment data changes."""
            search_state.has_domain_data = bool(search_state.all_result_domains)
            search_state.result_domains = dict(search_state.all_result_domains)
            _set_btn_visible(printed_filter_btn, len(search_state.printed_ids) > 0)
            _set_btn_visible(domain_filter_btn, search_state.has_domain_data)
            _update_domain_filter_btn()

        def _render_with_filters(reset_expansion=True):
            """Re-render applying exclusions and filters."""
            display_results = results
            if search_state.word_search_excluded_ids:
                ws_filtered = []
                ws_excluded = []
                for r in results:
                    sid = r.get('display', {}).get('id')
                    if sid and sid in search_state.word_search_excluded_ids:
                        ws_excluded.append({'result': r, 'reason': tr('Excluded')})
                    else:
                        ws_filtered.append(r)
                search_state.word_search_excluded_results = ws_excluded
                display_results = ws_filtered
            else:
                search_state.word_search_excluded_results = []

            if search_state.domain_exclusions and search_state.has_domain_data:
                _apply_domain_exclusions(reset_expansion=reset_expansion)
            elif search_state.printed_filter != 'all' and search_state.printed_ids:
                search_state.domain_excluded_results = []
                _apply_printed_filter_and_render(display_results, reset_expansion=reset_expansion)
            else:
                search_state.domain_excluded_results = []
                # Apply measurement post-filters (Phase 54, review concern #1)
                display_results = _apply_measurement_post_filters(display_results, search_state)
                render_results(display_results, page=0, reset_expansion=reset_expansion)

        # --- STAGE 1: Enrich visible page (first PAGE_SIZE sys_ids) ---
        _t_stage1 = time.perf_counter()
        visible_ids = all_sys_ids[:PAGE_SIZE]
        if visible_ids and search_state.search_generation == this_generation:
            fjms_tuple, transcription_ids, trans_data = await asyncio.gather(
                run.io_bound(collect_fjms_enrichment, visible_ids),
                run.io_bound(get_sys_ids_with_transcriptions, visible_ids),
                run.io_bound(collect_translations, visible_ids, _show_trans_for_enrich),
            )
            # Check generation before applying (user may have started a new search)
            if search_state.search_generation == this_generation:
                raw_domains, catalog_counts, printed_ids, meas_batch = fjms_tuple
                search_state._measurement_cache.update(meas_batch)  # Phase 54
                _process_domain_data(raw_domains)
                search_state.transcription_sys_ids = transcription_ids
                search_state.catalog_source_counts = catalog_counts
                search_state.printed_ids = printed_ids
                search_state.translation_data = trans_data
                # Pre-cache domain hierarchy
                if search_state.all_result_domains:
                    def fetch_hierarchy():
                        from shared.fjms_service import get_fjms_service
                        fjms = get_fjms_service(thread_safe=True)
                        return fjms.get_domain_hierarchy() if fjms.is_available() else {}
                    search_state.domain_hierarchy = await run.io_bound(fetch_hierarchy)
                else:
                    search_state.domain_hierarchy = {}
                # Re-check generation after hierarchy await (P1 fix: close race window)
                if search_state.search_generation == this_generation:
                    _apply_enrichment_to_ui()
                    _render_with_filters(reset_expansion=False)

        _t_stage1_done = time.perf_counter()
        logger.info("Search perf: visible_enrichment_ms=%.0f (ids=%d)", (_t_stage1_done - _t_stage1) * 1000, len(visible_ids))

        # --- STAGE 2: Background-enrich remaining sys_ids in chunks ---
        _t_stage2 = time.perf_counter()
        remaining_ids = all_sys_ids[PAGE_SIZE:]
        if remaining_ids and search_state.search_generation == this_generation:
            CHUNK_SIZE = 200
            for chunk_start in range(0, len(remaining_ids), CHUNK_SIZE):
                if search_state.search_generation != this_generation:
                    break  # New search started, abandon background enrichment
                chunk_ids = remaining_ids[chunk_start:chunk_start + CHUNK_SIZE]
                bg_fjms_tuple, bg_trans_ids, bg_trans_data = await asyncio.gather(
                    run.io_bound(collect_fjms_enrichment, chunk_ids),
                    run.io_bound(get_sys_ids_with_transcriptions, chunk_ids),
                    run.io_bound(collect_translations, chunk_ids, _show_trans_for_enrich),
                )
                if search_state.search_generation != this_generation:
                    break
                bg_domains, bg_counts, bg_printed, bg_meas = bg_fjms_tuple
                search_state._measurement_cache.update(bg_meas)  # Phase 54
                _process_domain_data(bg_domains)
                search_state.transcription_sys_ids |= bg_trans_ids
                search_state.catalog_source_counts.update(bg_counts)
                search_state.printed_ids |= bg_printed
                search_state.translation_data.update(bg_trans_data)
            # Final UI update + re-render after all background chunks complete
            # (P2 fix: apply filters/exclusions to newly discovered domains/printed IDs)
            if search_state.search_generation == this_generation:
                _apply_enrichment_to_ui()
                _render_with_filters(reset_expansion=False)
            _t_stage2_done = time.perf_counter()
            logger.info("Search perf: background_enrichment_ms=%.0f (ids=%d)", (_t_stage2_done - _t_stage2) * 1000, len(remaining_ids))

    def toggle_expansion(index):
        """Toggle inline accordion expansion for a result card."""
        if search_state.expanded_index == index:
            # Collapse current
            ref = search_state.expansion_refs.get(index)
            if ref and not ref.is_deleted:
                ref.style('display: none')
            search_state.expanded_index = None
        else:
            # Collapse old if any
            if search_state.expanded_index is not None:
                old_ref = search_state.expansion_refs.get(search_state.expanded_index)
                if old_ref and not old_ref.is_deleted:
                    old_ref.style('display: none')
            # Expand new
            ref = search_state.expansion_refs.get(index)
            if ref and not ref.is_deleted:
                ref.style('display: block')
            search_state.expanded_index = index
            # Trigger lazy text loading if registered for this card
            lazy_loaders = getattr(search_state, '_lazy_loaders', {})
            if index in lazy_loaders:
                async def _run_lazy(fn=lazy_loaders[index]):
                    with _page_client:
                        await fn()
                asyncio.ensure_future(_run_lazy())

    def render_results(results, page=None, scroll_to_top=False, reset_expansion=True):
        results_container.clear()
        search_state.displayed_results = results  # Track full filtered set for Advanced View navigation

        # Handle expansion state
        _was_expanded = None
        if reset_expansion:
            search_state.expanded_index = None
            search_state.expansion_refs = {}
        else:
            _was_expanded = search_state.expanded_index
            search_state.expansion_refs = {}
            search_state.expanded_index = None

        # Use provided page or keep stored page
        if page is not None:
            search_state.current_page = page
        # Clamp page to valid range
        total = len(results)
        total_pages = max(1, (total + PAGE_SIZE - 1) // PAGE_SIZE)
        if search_state.current_page >= total_pages:
            search_state.current_page = 0

        # Show loading spinner when search is running - prominent so user knows it's working
        if search_state.is_running:
            with results_container:
                with ui.column().classes('w-full h-64 items-center justify-center'):
                    ui.spinner('bars', size='xl', color='primary').classes('mb-4')
                    ui.label(tr("Searching...")).classes('text-xl font-bold animate-pulse').style('color: var(--primary-600);')
            return

        if not results:
            with results_container:
                # Phase 55: Zero-result refinement recovery (D-14a)
                if getattr(search_state, '_zero_result_refine', False):
                    with ui.column().classes('w-full h-64 items-center justify-center'):
                        ui.label(tr('0 results within current scope')).classes('text-lg py-4 text-center w-full')
                        ui.button(
                            tr('Back to previous step'), icon='undo',
                            on_click=lambda: _undo_zero_result_refine()
                        ).classes('mx-auto')
                else:
                    with ui.column().classes('w-full h-64 items-center justify-center'):
                        ui.icon('search').classes('text-5xl').style('color: var(--text-muted);')
                        ui.label(tr("Ready to search.")).classes('mt-4').style('color: var(--text-muted);')
            _update_search_within_btn()
            _update_refinement_strip()
            return

        # Calculate page slice
        page_idx = search_state.current_page
        start = page_idx * PAGE_SIZE
        end = min(start + PAGE_SIZE, total)
        page_results = results[start:end]

        with results_container:
            # Pagination controls at top (always reserve space to prevent CLS)
            if total_pages > 1:
                with ui.row().classes('w-full justify-between items-center px-4 pt-2'):
                    ui.label(f"{start + 1}-{end} {tr('of')} {total}").classes('text-sm').style('color: var(--text-muted);')
                    def on_page_change_top(e):
                        search_state.current_page = e.value - 1  # ui.pagination is 1-indexed
                        render_results(results, page=search_state.current_page, scroll_to_top=True)
                    ui.pagination(1, total_pages, value=page_idx + 1,
                        on_change=on_page_change_top).props('max-pages=7 boundary-numbers')
            else:
                # Invisible placeholder reserving same height as pagination row
                ui.element('div').style('height: 40px; visibility: hidden;')

            # Render only this page of results
            with ui.column().classes('w-full gap-2 p-4'):
                for i, res in enumerate(page_results):
                    create_result_card(start + i, res)

            # Re-expand previously open card after enrichment rerender
            if not reset_expansion and _was_expanded is not None and _was_expanded in search_state.expansion_refs:
                toggle_expansion(_was_expanded)

            # Pagination controls at bottom (always reserve space to prevent CLS)
            if total_pages > 1:
                with ui.row().classes('w-full justify-center items-center px-4 pb-2'):
                    def on_page_change_bottom(e):
                        search_state.current_page = e.value - 1
                        render_results(results, page=search_state.current_page, scroll_to_top=True)
                    ui.pagination(1, total_pages, value=page_idx + 1,
                        on_change=on_page_change_bottom).props('max-pages=7 boundary-numbers')
            else:
                ui.element('div').style('height: 40px; visibility: hidden;')

            # Collapsible excluded results section (domain-excluded)
            excluded = search_state.domain_excluded_results
            if excluded:
                with ui.expansion(
                    text=f"{tr('Excluded Results')} ({len(excluded)})",
                    icon='filter_alt',
                    value=False  # collapsed by default
                ).classes('w-full').style(
                    'border: 1px solid var(--accent-amber); border-radius: 8px; margin-top: 16px; overflow: hidden;'
                ).props('dense header-class="text-amber-8 text-subtitle1 text-weight-medium"'):
                    # Show up to 50 excluded results with their reasons
                    EXCLUDED_DISPLAY_LIMIT = 50
                    for i, excl_item in enumerate(excluded[:EXCLUDED_DISPLAY_LIMIT]):
                        excl_result = excl_item['result']
                        excl_reason = excl_item.get('reason', '')
                        excl_display = excl_result.get('display', {})
                        excl_shelfmark = excl_display.get('shelfmark', 'Unknown')
                        excl_title = excl_display.get('title', '')
                        with ui.row().classes('w-full items-center gap-2 py-1 px-2 cursor-pointer').style(
                            'border-bottom: 1px solid var(--border-light); overflow: hidden; max-width: 100%;'
                        ).on('click', lambda r=excl_result: open_advanced_dialog(None, r)):
                            ui.label(excl_shelfmark).classes('text-sm font-medium truncate shrink-0').style(
                                'color: var(--text-secondary); max-width: 200px;'
                            )
                            if excl_title:
                                title_short = (excl_title[:60] + '...') if len(excl_title) > 60 else excl_title
                                ui.label(title_short).classes('text-xs truncate').style(
                                    'color: var(--text-muted); direction: rtl; min-width: 0; flex: 1 1 0;'
                                )
                            ui.label(excl_reason).classes('text-xs px-2 py-0.5 rounded shrink-0').style(
                                'background: #fff3cd; color: #856404; white-space: nowrap;'
                            )
                    if len(excluded) > EXCLUDED_DISPLAY_LIMIT:
                        ui.label(
                            f"... {tr('and')} {len(excluded) - EXCLUDED_DISPLAY_LIMIT} {tr('more')}"
                        ).classes('text-sm py-2 px-2').style('color: var(--text-muted);')

            # Collapsible word search excluded results section
            ws_excluded = search_state.word_search_excluded_results
            if ws_excluded:
                with ui.expansion(
                    text=f"{tr('Excluded Results')} ({len(ws_excluded)})",
                    icon='remove_circle_outline',
                    value=False  # collapsed by default
                ).classes('w-full').style(
                    'border: 1px solid var(--accent-amber); border-radius: 8px; margin-top: 8px; overflow: hidden;'
                ).props('dense header-class="text-amber-8 text-subtitle1 text-weight-medium"'):
                    WS_EXCLUDED_LIMIT = 50
                    for i, excl_item in enumerate(ws_excluded[:WS_EXCLUDED_LIMIT]):
                        excl_result = excl_item['result']
                        excl_display = excl_result.get('display', {})
                        excl_shelfmark = excl_display.get('shelfmark', 'Unknown')
                        excl_title = excl_display.get('title', '')
                        excl_sys_id = excl_display.get('id')
                        with ui.row().classes('w-full items-center gap-2 py-1 px-2').style(
                            'border-bottom: 1px solid var(--border-light); overflow: hidden; max-width: 100%;'
                        ):
                            # Restore button
                            def _restore_word_result(sid=excl_sys_id):
                                search_state.word_search_excluded_ids.discard(sid)
                                persist_value('word_search_excluded_ids', list(search_state.word_search_excluded_ids))
                                _apply_word_search_exclusions_and_render()
                            ui.button(icon='add_circle_outline', on_click=_restore_word_result).props(
                                'flat round dense size=xs'
                            ).style('color: var(--text-muted);').tooltip(tr('Restore'))
                            with ui.row().classes('items-center gap-2 flex-grow min-w-0 cursor-pointer').on(
                                'click', lambda r=excl_result: open_advanced_dialog(None, r)
                            ):
                                ui.label(excl_shelfmark).classes('text-sm font-medium truncate shrink-0').style(
                                    'color: var(--text-secondary); max-width: 200px;'
                                )
                                if excl_title:
                                    title_short = (excl_title[:60] + '...') if len(excl_title) > 60 else excl_title
                                    ui.label(title_short).classes('text-xs truncate').style(
                                        'color: var(--text-muted); direction: rtl; min-width: 0; flex: 1 1 0;'
                                    )
                    if len(ws_excluded) > WS_EXCLUDED_LIMIT:
                        ui.label(
                            f"... {tr('and')} {len(ws_excluded) - WS_EXCLUDED_LIMIT} {tr('more')}"
                        ).classes('text-sm py-2 px-2').style('color: var(--text-muted);')
                    # Clear all word search exclusions button
                    with ui.row().classes('w-full justify-end py-2 px-2'):
                        def _clear_word_exclusions():
                            search_state.word_search_excluded_ids.clear()
                            persist_value('word_search_excluded_ids', [])
                            _apply_word_search_exclusions_and_render()
                        ui.button(tr('Clear All Exclusions'), icon='clear_all',
                                  on_click=_clear_word_exclusions).props('flat dense no-caps size=sm')

        # Phase 55: Update refinement UI after rendering results
        _update_search_within_btn()
        _update_refinement_strip()

        # Scroll to top of results after page change
        if scroll_to_top:
            results_container.run_method('setScrollPosition', 'vertical', 0)

    def create_result_card(index, result):
        display = result.get('display', {})
        shelfmark = display.get('shelfmark', 'Unknown')
        title = display.get('title', '')
        snippet = result.get('snippet', '')
        library_code = display.get('library_code', '')
        sys_id = display.get('id')

        # Truncate title for display
        title_short = (title[:60] + '...') if title and len(title) > 60 else title

        with ui.card().classes(
            'w-full p-4 cursor-pointer transition-all hover:shadow-md'
        ).style('border-radius: 10px;') as card:
            with ui.row().classes('w-full items-start justify-between'):
                # Checkbox for selection
                with ui.column().classes('justify-center'):
                    def toggle_card_selection(e, idx=index):
                        if e.value:
                            search_state.selected_indices.add(idx)
                        else:
                            search_state.selected_indices.discard(idx)
                        update_selection_ui()

                    result_checkbox = ui.checkbox(
                        value=index in search_state.selected_indices,
                        on_change=toggle_card_selection
                    ).props('dense')

                # Main content (clickable — toggles inline accordion)
                with ui.column().classes('flex-grow min-w-0 gap-1').on('click', lambda idx=index: toggle_expansion(idx)):
                    with ui.row().classes('items-center gap-2 flex-wrap'):
                        ui.label(f"#{index + 1}").classes('text-xs px-2 py-0.5 rounded shrink-0').style(
                            'background: var(--bg-tertiary); color: var(--text-muted);'
                        )
                        # Library badge (if available)
                        if library_code:
                            from genizah_core import get_library_display, LIBRARY_CODES
                            full_name = get_library_display(library_code, short=False, lang=get_language())
                            ui.label(library_code).classes('text-xs px-2 py-0.5 rounded shrink-0').style(
                                'background: var(--primary-100); color: var(--primary-700);'
                            ).tooltip(full_name)
                        # PGP transcription indicator
                        if sys_id and sys_id in search_state.transcription_sys_ids:
                            ui.label('PGP').classes('text-xs px-2 py-0.5 rounded shrink-0').style(
                                'background: var(--success-100); color: var(--success-700); font-weight: 600;'
                            ).tooltip(tr('Has PGP Transcription'))
                        # Domain indicator
                        if sys_id and search_state.result_domains:
                            domains_for_result = search_state.result_domains.get(sys_id, [])
                            if domains_for_result:
                                primary_domain = domains_for_result[0]
                                domain_text = _domain_display_name(primary_domain)
                                if len(domains_for_result) > 1:
                                    extra = len(domains_for_result) - 1
                                    tooltip_text = ', '.join(_domain_display_name(d) for d in domains_for_result)
                                    with ui.row().classes('items-center gap-0'):
                                        ui.label(domain_text).classes('text-xs px-2 py-0.5 rounded shrink-0').style(
                                            'background: #f3e8ff; color: #7c3aed;'
                                        )
                                        ui.label(f'+{extra}').classes('text-xs px-1 py-0.5 rounded shrink-0 cursor-help').style(
                                            'background: #ede9fe; color: #7c3aed;'
                                        ).tooltip(tooltip_text)
                                else:
                                    ui.label(domain_text).classes('text-xs px-2 py-0.5 rounded shrink-0').style(
                                        'background: #f3e8ff; color: #7c3aed;'
                                    )
                        # Printed material indicator
                        if sys_id and sys_id in search_state.printed_ids:
                            from shared.fjms_service import PRINTED_BADGE_COLORS, PRINTED_LABEL_EN, PRINTED_LABEL_HE
                            _bg, _fg = PRINTED_BADGE_COLORS
                            _plabel = PRINTED_LABEL_HE if get_language() == 'he' else PRINTED_LABEL_EN
                            ui.label(_plabel).classes('text-xs px-2 py-0.5 rounded shrink-0 font-medium').style(
                                f'background: {_bg}; color: {_fg};'
                            )
                        ui.label(shelfmark).classes('font-bold break-all').style('color: var(--primary-700);')
                    # Title and optional translated description (Phase 46)
                    _show_trans = False
                    try:
                        _show_trans = app.storage.user.get('show_translations', False)
                    except Exception:
                        pass
                    _title_info = search_state.title_translations.get(sys_id) if sys_id else None
                    if _title_info:
                        _lang = get_language()
                        _he = _title_info.get('hebrew_title') or ''
                        _en = _title_info.get('english_title') or ''
                        _en_he = _title_info.get('english_title_he') or ''
                        if _lang == 'he':
                            if _he.strip():
                                if _en_he.strip() and len(_he) < 15:
                                    _resolved_title = f"{_he} — {_en_he}"
                                else:
                                    _resolved_title = _he
                            else:
                                _resolved_title = _en_he or _en or title
                        else:
                            _resolved_title = _en or _he or title
                        _resolved_short = (_resolved_title[:60] + '...') if _resolved_title and len(_resolved_title) > 60 else (_resolved_title or '')
                    else:
                        _resolved_title = title
                        _resolved_short = title_short
                    _trans_info = search_state.translation_data.get(sys_id) if sys_id and _show_trans else None
                    _ui_lang = get_language()
                    if _trans_info and _trans_info.get('description_he') and _ui_lang == 'he':
                        _desc_he = _trans_info['description_he']
                        _desc_short = (_desc_he[:80] + '...') if len(_desc_he) > 80 else _desc_he
                        _orig = _resolved_short if _resolved_short else ''
                        _ts = {'showing_original': False}
                        with ui.row().classes('items-center gap-1'):
                            _tl = ui.label(_desc_short).classes('text-xs').style(
                                'color: var(--text-tertiary); direction: rtl; word-wrap: break-word;'
                            )
                            def _make_compact_toggle(label_el, badge_el_ref, orig_text, trans_text, flag):
                                def handler():
                                    flag['showing_original'] = not flag['showing_original']
                                    if flag['showing_original']:
                                        label_el.text = orig_text
                                        label_el.style('color: var(--text-tertiary); direction: ltr; word-wrap: break-word;')
                                        badge_el_ref[0].text = tr('Original')
                                    else:
                                        label_el.text = trans_text
                                        label_el.style('color: var(--text-tertiary); direction: rtl; word-wrap: break-word;')
                                        badge_el_ref[0].text = tr('Translated')
                                return handler
                            _tb_ref = [None]
                            _toggle_fn = _make_compact_toggle(_tl, _tb_ref, _orig, _desc_short, _ts)
                            _tb = ui.button(tr('Translated')).props(
                                'flat dense no-caps size=xs'
                            ).classes('text-xs px-1 py-0 rounded shrink-0').style(
                                'background: #e0f2fe !important; color: #0369a1 !important; font-style: italic; font-size: 0.65rem; min-height: 0; line-height: 1.2;'
                            )
                            _tb.on('click.stop', _toggle_fn)
                            _tb_ref[0] = _tb
                            from web.components.translation_report import create_report_button
                            create_report_button(
                                dataset='pgp', record_id=str(search_state.translation_data.get(sys_id, {}).get('pgpid', sys_id)),
                                field_name='description', direction='en2he',
                                source_text=_orig, translated_text=_desc_short,
                            )
                    elif _resolved_short:
                        _dir = 'ltr' if (_title_info and get_language() != 'he' and _title_info.get('english_title')) else 'rtl'
                        _orig_title = title
                        _orig_short = (title[:60] + '...') if title and len(title) > 60 else (title or '')
                        if _title_info and _orig_short and _orig_short != _resolved_short:
                            _tt_st = {'showing_original': False}
                            with ui.row().classes('items-center gap-0'):
                                _tt_lbl = ui.label(_resolved_short).classes('text-xs').style(
                                    f'color: var(--text-tertiary); direction: {_dir}; word-wrap: break-word;'
                                )
                                def _make_title_toggle(lbl, orig, resolved, orig_dir, res_dir, flag):
                                    def handler():
                                        flag['showing_original'] = not flag['showing_original']
                                        if flag['showing_original']:
                                            lbl.text = orig
                                            lbl.style(f'color: var(--text-tertiary); direction: {orig_dir}; word-wrap: break-word;')
                                        else:
                                            lbl.text = resolved
                                            lbl.style(f'color: var(--text-tertiary); direction: {res_dir}; word-wrap: break-word;')
                                    return handler
                                ui.button(icon='swap_horiz').props('flat dense round size=xs').style(
                                    'min-width: 18px; min-height: 18px; padding: 0; opacity: 0.4;'
                                ).tooltip(tr('Show original title')).on(
                                    'click.stop', _make_title_toggle(_tt_lbl, _orig_short, _resolved_short, 'rtl', _dir, _tt_st)
                                )
                        else:
                            ui.label(_resolved_short).classes('text-xs').style(
                                f'color: var(--text-tertiary); direction: {_dir}; word-wrap: break-word;'
                            )

            # Action buttons row (on the card, always visible)
            with ui.row().classes('gap-1 mt-1'):
                # Browse
                if sys_id:
                    _card_fl_id = None
                    if 'raw_header' in result and state.meta_mgr:
                        try:
                            _card_fl_id = state.meta_mgr.parse_full_id_components(result['raw_header']).get('fl_id')
                        except Exception:
                            pass
                    _card_browse_url = f'/browse?sys_id={sys_id}'
                    if _card_fl_id:
                        _card_browse_url += f'&fl_id={_card_fl_id}'
                    with ui.link(target=_card_browse_url).classes('no-underline'):
                        ui.button(icon='menu_book').props('flat round dense size=sm color=green').tooltip(tr('Browse Full Manuscript'))

                # Quick View (was Advanced View)
                ui.button(
                    icon='open_in_full',
                    on_click=lambda idx=index, r=result: open_advanced_dialog(idx, r)
                ).props('flat round dense size=sm').tooltip(tr('Quick View'))

                # Add to List
                def make_star_handler(r):
                    def handler():
                        show_add_to_list_dialog_local(r)
                    return handler
                result_sys_id = result.get('display', {}).get('id')
                result_in_list = state.lists_mgr and result_sys_id and state.lists_mgr.is_item_in_any_list(result_sys_id)
                ui.button(
                    icon='star' if result_in_list else 'star_border',
                    on_click=make_star_handler(result)
                ).props('flat round dense size=sm').style('color: var(--accent-amber);').tooltip(tr('In List') if result_in_list else tr('Add to List'))

                # Catalog Records
                if sys_id:
                    cat_count = search_state.catalog_source_counts.get(sys_id, 0)
                    from web.components.catalog_dialog import show_catalog_dialog
                    cat_btn = ui.button(
                        icon='description',
                        on_click=lambda s=sys_id, sm=shelfmark: show_catalog_dialog(s, sm),
                    ).props('flat round dense size=sm').tooltip(f'{tr("Catalog Records")} ({cat_count})')
                    if cat_count == 0:
                        cat_btn.disable()

            # Snippet
            if snippet:
                snippet_html = SearchEngine.format_snippet(snippet)
                with ui.element('div').classes('mt-3 p-3 rounded-lg text-sm').style(
                    'background: var(--bg-tertiary); direction: rtl; text-align: right; line-height: 1.8;'
                ):
                    ui.html(snippet_html, sanitize=False)

            # === Inline accordion expansion (image + full text only) ===
            expand_container = ui.column().classes('w-full result-inline-expand').style('display: none;')
            search_state.expansion_refs[index] = expand_container

            # Build thumbnail URL eagerly (browser preloads in background)
            _img_url = None
            if sys_id:
                page_idx = max(0, int(display.get('img', '1')) - 1) if display.get('img') else 0
                _img_url = f"/api/nli_image_by_sysid/{sys_id}?page={page_idx}&width=300"
                is_oxford = False
                try:
                    from web.pages.browse import is_oxford_manuscript
                    is_oxford = is_oxford_manuscript(display.get('shelfmark', ''), display.get('library_code', ''))
                except Exception:
                    pass
                if is_oxford:
                    try:
                        from web.pages.browse import get_oxford_direct_image_url
                        _ox_url = get_oxford_direct_image_url(display.get('shelfmark', ''), page_idx)
                        if _ox_url:
                            _img_url = _ox_url
                        else:
                            _img_url = f"/api/oxford_image/{sys_id}?page={page_idx}"
                    except Exception:
                        _img_url = f"/api/oxford_image/{sys_id}?page={page_idx}"

            with expand_container:
                # Content row: image + text
                _expand_row = ui.row().classes('gap-4 flex-wrap w-full')
                with _expand_row:
                    # Left: manuscript image thumbnail (click opens Quick View)
                    if _img_url:
                        with ui.element('div').classes('cursor-pointer').on(
                            'click', lambda idx=index, r=result: open_advanced_dialog(idx, r)
                        ):
                            ui.html(
                                f'<img src="{_img_url}" '
                                f'onerror="this.style.display=\'none\'" '
                                f'style="width: 200px; max-height: 250px; object-fit: contain; border-radius: 8px;" />',
                                sanitize=False
                            )

                    # Right: full text container (populated immediately or lazy-loaded)
                    _text_col = ui.column().classes('flex-1 min-w-[200px] gap-2')

                def _render_full_text(text_col, text, pattern):
                    """Render highlighted full text into the given column."""
                    text_col.clear()
                    with text_col:
                        if text:
                            escaped = html.escape(text)
                            if pattern:
                                try:
                                    flags = re.IGNORECASE
                                    if '\\n' in pattern or pattern.startswith('^'):
                                        flags |= re.MULTILINE
                                    escaped = re.sub(
                                        f'({pattern})',
                                        r'<span class="highlight-match">\1</span>',
                                        escaped, flags=flags
                                    )
                                except re.error:
                                    pass
                            with ui.scroll_area().classes('w-full').style('max-height: 250px;'):
                                with ui.element('div').classes('p-3 rounded-lg text-sm whitespace-pre-wrap').style(
                                    'background: var(--bg-tertiary); direction: rtl; text-align: right; line-height: 2; font-size: 0.95rem;'
                                ):
                                    ui.html(escaped, sanitize=False)
                        else:
                            ui.label(tr('Full text not available')).style('color: var(--text-muted);')

                # Populate text: immediately if available, lazy-load if not
                full_text = result.get('full_text', '')
                highlight_pattern = result.get('highlight_pattern', '')
                if full_text:
                    _render_full_text(_text_col, full_text, highlight_pattern)
                elif sys_id:
                    # Lazy-load: fetch full text when accordion first expands
                    _lazy_state = {'loaded': False}
                    _orig_toggle = None

                    async def _lazy_load_text(idx=index, r=result, tc=_text_col, hp=highlight_pattern, sid=sys_id, ls=_lazy_state):
                        if ls['loaded']:
                            return
                        ls['loaded'] = True
                        p_num = int(r.get('display', {}).get('img', '1'))
                        try:
                            from web.services import get_service
                            page_data = await run.io_bound(
                                lambda: get_service().get_browse_page(sid, p_num=p_num)
                            )
                            if page_data and page_data.text:
                                r['full_text'] = page_data.text
                                _render_full_text(tc, page_data.text, hp)
                            else:
                                logger.warning("Lazy load: no page data for sys_id=%s p_num=%d", sid, p_num)
                        except Exception as e:
                            logger.error("Lazy load error for sys_id=%s: %s", sid, e, exc_info=True)

                    # Hook into toggle: load text on first expand
                    _orig_toggle_fn = toggle_expansion

                    def _make_lazy_toggle(idx, load_fn, orig_fn):
                        def _toggle(i):
                            orig_fn(i)
                            if search_state.expanded_index == idx:
                                asyncio.ensure_future(load_fn())
                        return _toggle

                    # Patch the click handler for this card to also trigger lazy load
                    # We do this by wrapping the card's click — find the content column and re-bind
                    # Simpler: just trigger lazy load whenever this card expands
                    search_state._lazy_loaders = getattr(search_state, '_lazy_loaders', {})
                    search_state._lazy_loaders[index] = _lazy_load_text

    def open_advanced_dialog(index, result):
        """Open an enhanced Advanced View dialog with in-place navigation and IIIF image viewer.

        When index is None, operates in standalone mode: wraps the single result in a list,
        uses index=0, and disables navigation. This is used for excluded results and tag search.
        """
        standalone = (index is None)
        # PostHog: track result clicks
        from web.analytics import posthog_capture
        display = result.get('display', {}) if isinstance(result, dict) else {}
        posthog_capture('result_opened', {
            'shelfmark': display.get('shelfmark', '')[:80],
            'sys_id': display.get('id', ''),
            'result_index': index if not standalone else 0,
        })

        service = get_service()
        adv_state = AdvancedViewState()
        if standalone:
            adv_state.current_result_idx = 0
            adv_state.results = [result]
        else:
            adv_state.current_result_idx = index
            adv_state.results = search_state.displayed_results

        with ui.dialog().props('maximized') as dialog:
            with ui.card().classes('w-full h-full flex flex-col').style('background: var(--bg-secondary);'):
                # === Header Bar (compact: close, shelfmark, result nav, fullscreen) ===
                adv_state.header_container = ui.row().classes('w-full px-4 py-2 items-center justify-between shrink-0').style(
                    'background: var(--bg-header); color: white;'
                )
                with adv_state.header_container:
                    # Left: Close and result counter
                    with ui.row().classes('items-center gap-2'):
                        ui.button(icon='close', on_click=dialog.close).props('flat round color=white size=sm')
                        if standalone:
                            _sm = display.get('shelfmark', 'Unknown')
                            adv_state.result_label = ui.label(_sm).classes('text-sm font-medium')
                        else:
                            adv_state.result_label = ui.label(
                                f"{tr('Result')} {index + 1} / {len(adv_state.results)}"
                            ).classes('text-sm font-medium')

                    # Center: Score badge (will be updated in-place)
                    adv_state.score_badge = ui.element('div').classes('flex items-center gap-2')

                    # Right: Navigation and Fullscreen
                    with ui.row().classes('items-center gap-2'):
                        adv_state.prev_btn = ui.button(
                            icon='chevron_right' if is_rtl() else 'chevron_left',
                            on_click=lambda: navigate_result(-1)
                        ).props('flat round color=white size=sm').tooltip(tr('Previous'))

                        adv_state.next_btn = ui.button(
                            icon='chevron_left' if is_rtl() else 'chevron_right',
                            on_click=lambda: navigate_result(1)
                        ).props('flat round color=white size=sm').tooltip(tr('Next'))

                        if standalone:
                            adv_state.prev_btn.set_enabled(False)
                            adv_state.next_btn.set_enabled(False)

                        ui.separator().props('vertical').classes('mx-1 h-4 bg-gray-400')

                        def toggle_fullscreen():
                            adv_state.is_fullscreen = not adv_state.is_fullscreen
                            render_content(adv_state.results[adv_state.current_result_idx])

                        ui.button(
                            icon='fullscreen',
                            on_click=toggle_fullscreen
                        ).props('flat round color=white size=sm').tooltip(tr('Fullscreen'))

                # === Info Bar (shelfmark, buttons, chips — rendered in render_content) ===
                adv_state.info_bar_container = ui.element('div').classes('w-full shrink-0').style(
                    'background: var(--bg-primary); border-bottom: 1px solid var(--border-light);'
                )

                # === Main Content (refreshable container) ===
                with ui.scroll_area().classes('flex-grow'):
                    adv_state.content_container = ui.column().classes('w-full max-w-6xl mx-auto px-4 py-2 gap-2')

        def navigate_result(direction: int):
            """Navigate to prev/next result with in-place update (no dialog close/reopen)."""
            new_idx = adv_state.current_result_idx + direction
            if 0 <= new_idx < len(adv_state.results):
                adv_state.current_result_idx = new_idx
                load_result(new_idx)

        def load_result(idx: int):
            """Load a result into the dialog, updating UI in-place."""
            result = adv_state.results[idx]
            display = result.get('display', {})
            adv_state.current_sys_id = display.get('id', '')

            # Reset enrichment data for new result
            adv_state.fjms_data = None
            adv_state.crossref_data = None

            # Update header label
            adv_state.result_label.set_text(
                f"{tr('Result')} {idx + 1} / {len(adv_state.results)}"
            )

            # Update navigation button states
            adv_state.prev_btn.set_enabled(idx > 0)
            adv_state.next_btn.set_enabled(idx < len(adv_state.results) - 1)

            # Update score badge
            adv_state.score_badge.clear()
            sort_score = result.get('sort_score')
            if sort_score is not None:
                score_pct = min(100, max(0, int(sort_score)))
                with adv_state.score_badge:
                    with ui.element('div').classes('flex items-center gap-2 px-3 py-1 rounded-full').style(
                        'background: rgba(255,255,255,0.15);'
                    ):
                        ui.icon('insights').classes('text-sm')
                        ui.label(f"{tr('Score')}: {score_pct}").classes('text-sm font-medium')

            # Load browse page data for this result
            page_num_str = display.get('img', '1')
            try:
                initial_p_num = int(page_num_str) if page_num_str else 1
            except (ValueError, TypeError):
                initial_p_num = 1

            adv_state.current_p_num = initial_p_num

            # Fetch page data asynchronously
            async def fetch_and_render():
                if adv_state.current_sys_id:
                    page = await run.io_bound(lambda: service.get_browse_page(
                        adv_state.current_sys_id, p_num=adv_state.current_p_num
                    ))
                    adv_state.current_page = page
                    if page:
                        adv_state.total_pages = page.total_pages
                        adv_state.current_fl_id = page.fl_id
                else:
                    adv_state.current_page = None
                    adv_state.total_pages = 1

                # Fetch FJMS + crossref enrichment
                if adv_state.current_sys_id:
                    sid = adv_state.current_sys_id
                    try:
                        from shared.fjms_service import get_fjms_service
                        fjms = get_fjms_service(thread_safe=True)
                        if fjms.is_available():
                            adv_state.fjms_data = await run.io_bound(lambda: {
                                'catalog_records': fjms.get_catalog_records(sid),
                                'domains': fjms.get_domains(sid),
                                'bibliography': fjms.get_bibliography(sid),
                                'source_names': fjms.get_source_names(sid),
                                'catalog_refs': fjms.get_catalog_refs(sid),
                            })
                    except Exception:
                        pass
                    try:
                        from shared.nli_crossref_service import get_nli_crossref_service
                        svc = get_nli_crossref_service(thread_safe=True)
                        if svc.is_available():
                            adv_state.crossref_data = await run.io_bound(
                                lambda: svc.get_crossref_metadata(sid)
                            )
                    except Exception:
                        pass

                render_content(result)

            asyncio.ensure_future(fetch_and_render())

        async def load_page(direction: int = 0, p_num: int = None):
            """Load a specific page within the current manuscript."""
            if not adv_state.current_sys_id:
                return

            target_p_num = p_num if p_num is not None else adv_state.current_p_num
            page = await run.io_bound(lambda: service.get_browse_page(
                adv_state.current_sys_id, p_num=target_p_num, direction=direction
            ))

            if page:
                adv_state.current_page = page
                adv_state.current_p_num = page.p_num
                adv_state.total_pages = page.total_pages
                adv_state.current_fl_id = page.fl_id
                render_content(adv_state.results[adv_state.current_result_idx])

        # === Edit Mode Functions ===
        def toggle_edit_mode(current_text: str):
            """Enter edit mode with the current text."""
            from web.auth_state import GlobalAuthState
            if not GlobalAuthState.is_logged_in():
                ui.notify(tr('Please login to edit'), type='warning')
                return

            adv_state.edit_mode = True
            adv_state.edit_text = current_text
            adv_state.original_edit_text = current_text
            adv_state.edit_notes = ""
            adv_state.draft_saved = False
            adv_state.draft_id = None
            render_content(adv_state.results[adv_state.current_result_idx])

        def cancel_edit(result):
            """Cancel edit mode and return to view mode."""
            adv_state.edit_mode = False
            adv_state.edit_text = ""
            adv_state.edit_notes = ""
            adv_state.draft_saved = False
            adv_state.draft_id = None
            render_content(result)

        def save_draft(sys_id: str, shelfmark: str, page_num: int, original_text: str):
            """Save current edit as draft."""
            from web.auth_state import GlobalAuthState
            from web.supabase_client import create_correction, update_correction

            if not GlobalAuthState.is_logged_in():
                ui.notify(tr('Please login to save'), type='warning')
                return

            user_id = GlobalAuthState.get_user_id()
            text = adv_state.edit_text
            notes = adv_state.edit_notes

            try:
                if adv_state.draft_id:
                    # Update existing draft
                    result = update_correction(adv_state.draft_id, {
                        'corrected_text': text,
                        'notes': notes
                    })
                else:
                    # Create new draft
                    result = create_correction(
                        author_id=user_id,
                        sys_id=sys_id,
                        shelfmark=shelfmark or '',
                        page_number=page_num,
                        original_text=original_text,
                        corrected_text=text,
                        notes=notes,
                        status='draft'
                    )
                    if result.get('success') and result.get('correction'):
                        adv_state.draft_id = result['correction'].get('id')

                adv_state.draft_saved = True
                ui.notify(tr('Draft saved'), type='positive')
                render_content(adv_state.results[adv_state.current_result_idx])
            except Exception as e:
                ui.notify(f"{tr('Error')}: {str(e)}", type='negative')

        def submit_correction(sys_id: str, shelfmark: str, page_num: int, original_text: str, result):
            """Submit correction for review or publish directly."""
            from web.auth_state import GlobalAuthState
            from web.supabase_client import create_correction, update_correction

            if not GlobalAuthState.is_logged_in():
                ui.notify(tr('Please login to submit'), type='warning')
                return

            user_id = GlobalAuthState.get_user_id()
            text = adv_state.edit_text
            notes = adv_state.edit_notes

            # Determine status based on role
            if GlobalAuthState.is_admin() or GlobalAuthState.is_editor():
                status = 'approved'
            else:
                status = 'pending'

            try:
                if adv_state.draft_id:
                    # Update existing draft to submitted
                    update_correction(adv_state.draft_id, {
                        'corrected_text': text,
                        'notes': notes,
                        'status': status
                    })
                else:
                    # Create new correction
                    create_correction(
                        author_id=user_id,
                        sys_id=sys_id,
                        shelfmark=shelfmark or '',
                        page_number=page_num,
                        original_text=original_text,
                        corrected_text=text,
                        notes=notes,
                        status=status
                    )

                # Exit edit mode
                adv_state.edit_mode = False
                adv_state.edit_text = ""
                adv_state.edit_notes = ""
                adv_state.draft_saved = False
                adv_state.draft_id = None

                if status == 'approved':
                    ui.notify(tr('Correction published'), type='positive')
                else:
                    ui.notify(tr('Correction submitted for review'), type='positive')

                render_content(result)
            except Exception as e:
                ui.notify(f"{tr('Error')}: {str(e)}", type='negative')

        def _apply_highlight_marks(text: str, terms: list) -> str:
            """Apply <mark> highlight tags around terms and convert newlines to <br>."""
            for term in terms:
                if term in text:
                    text = text.replace(
                        term,
                        f'<mark style="background-color: #fef08a; padding: 2px 4px; border-radius: 3px; font-weight: 600;">{term}</mark>'
                    )
            return text.replace('\n', '<br>')

        def render_content(result):
            """Render the main content area."""
            adv_state.content_container.clear()

            display = result.get('display', {})
            shelfmark = display.get('shelfmark', 'Unknown')
            title = display.get('title', '')
            sys_id = display.get('id', '')
            snippet = result.get('snippet', '')
            full_text = result.get('full_text', '')
            source = display.get('source', '')
            page_num = display.get('img', '')
            library_code = display.get('library_code', '')

            # Use current page data if available
            page = adv_state.current_page
            current_text = page.text if page else full_text
            current_p_num = page.p_num if page else adv_state.current_p_num
            total_pages = page.total_pages if page else 1

            # Fetch PGP transcription data for Advanced View
            pgp_transcription = None
            pgp_metadata = None
            all_sources = None
            if sys_id:
                try:
                    all_sources_raw = get_all_sources_for_fragment(sys_id)
                    current_page_info = 'recto' if current_p_num == 1 else 'verso'
                    page_sources = []
                    for src in all_sources_raw:
                        source_page = src.get('page_info')
                        if source_page == current_page_info or not source_page:
                            is_translation = 'Translation' in (src.get('doc_relation') or '')
                            if src.get('content'):
                                if not is_translation and not source_page:
                                    src['content'] = get_section_for_page(src['content'], current_p_num, src.get('sections'))
                            page_sources.append(src)
                    all_sources = page_sources if page_sources else None

                    pgp_doc = get_document_for_fragment(sys_id, current_p_num)
                    if pgp_doc:
                        pgpid = pgp_doc.get('pgpid')
                        pgp_metadata = {
                            'document_type': pgp_doc.get('document_type'),
                            'tags': pgp_doc.get('tags', []),
                            'description': pgp_doc.get('description'),
                            'languages_primary': pgp_doc.get('languages_primary'),
                            'languages_secondary': pgp_doc.get('languages_secondary'),
                            'inferred_date_display': pgp_doc.get('inferred_date_display'),
                            'doc_date_standard': pgp_doc.get('doc_date_standard'),
                            'doc_date_original': pgp_doc.get('doc_date_original'),
                            'inferred_date_rationale': pgp_doc.get('inferred_date_rationale'),
                            'pgp_url': pgp_doc.get('pgp_url'),
                            'pgpid': pgpid,
                        }
                        doc_relation = pgp_doc.get('doc_relation') or ''
                        is_edition = 'Edition' in doc_relation or not doc_relation
                        page_content = get_section_for_page(pgp_doc['transcription'], current_p_num) if pgp_doc.get('transcription') else None
                        if is_edition and page_content:
                            pgp_transcription = {
                                'full_content': pgp_doc['transcription'],
                                'content': page_content,
                                'attribution': pgp_doc.get('transcription_source', 'PGP'),
                                'pgp_url': pgp_doc.get('pgp_url'),
                                'pgpid': pgpid
                            }
                except Exception as pgp_err:
                    logger.error(f"Advanced View: Failed to fetch PGP transcription: {pgp_err}")

            # Extract FL ID
            fl_id = adv_state.current_fl_id
            if not fl_id and 'raw_header' in result and state.meta_mgr:
                try:
                    parsed = state.meta_mgr.parse_full_id_components(result['raw_header'])
                    fl_id = parsed.get('fl_id')
                except Exception:
                    pass

            # Determine if Oxford manuscript
            is_oxford = is_oxford_manuscript(shelfmark, library_code)

            # Compute image URL
            has_image = bool(sys_id)
            page_idx = max(0, current_p_num - 1)
            if is_oxford and sys_id:
                img_url = get_oxford_direct_image_url(shelfmark, page_idx)
                if not img_url:
                    img_url = f"/api/oxford_image/{sys_id}?page={page_idx}"
            elif sys_id:
                img_url = f"/api/nli_image_by_sysid/{sys_id}?page={page_idx}"
            else:
                img_url = None

            # Get library display name
            library_name = ''
            if library_code:
                library_name = get_library_display(library_code, short=False, lang=get_language())
            display_shelfmark = f"{library_name}, {shelfmark}" if library_name else shelfmark

            # Use PGP transcription content if available, otherwise fall back to original
            if all_sources:
                editions = [s for s in all_sources if 'Edition' in (s.get('doc_relation') or '') and s.get('content')]
                if editions:
                    display_text = editions[0].get('content', current_text or '')
                else:
                    display_text = current_text or snippet.replace('*', '') if snippet else ''
            elif pgp_transcription and pgp_transcription.get('content'):
                display_text = pgp_transcription['content']
            else:
                display_text = current_text or snippet.replace('*', '') if snippet else ''

            # Apply highlighting from snippet if we have match markers
            if snippet and '*' in snippet and display_text:
                import re as re_module
                highlighted_terms = re_module.findall(r'\*([^*]+)\*', snippet)
                adv_state.highlight_terms = highlighted_terms
                text_html = _apply_highlight_marks(display_text, highlighted_terms)
            else:
                adv_state.highlight_terms = []
                text_html = display_text.replace('\n', '<br>') if display_text else ''

            with adv_state.content_container:

                # ============================================================
                # FULLSCREEN MODE - Compact layout with text and image only
                # ============================================================
                if adv_state.is_fullscreen:
                    # Hide the info bar in fullscreen mode
                    adv_state.info_bar_container.clear()
                    # Compact info bar
                    with ui.row().classes('w-full items-center justify-between p-2 mb-2 rounded-lg').style(
                        'background: var(--bg-tertiary);'
                    ):
                        # Left: Shelfmark and page info
                        with ui.row().classes('items-center gap-3'):
                            ui.label(display_shelfmark).classes('font-bold text-sm').style('color: var(--primary-700);')
                            if title:
                                # Resolve title by language
                                _bar_title = title
                                if sys_id and search_state.title_translations:
                                    _bar_tt = search_state.title_translations.get(sys_id)
                                    if _bar_tt:
                                        _bar_lang = get_language()
                                        _bar_title = (_bar_tt.get('english_title') or _bar_tt.get('hebrew_title') or title) if _bar_lang != 'he' else (_bar_tt.get('hebrew_title') or _bar_tt.get('english_title') or title)
                                _bar_dir = 'ltr' if get_language() != 'he' else 'rtl'
                                ui.label(f"| {_bar_title[:50]}{'...' if len(_bar_title) > 50 else ''}").classes('text-xs').style(
                                    f'color: var(--text-muted); direction: {_bar_dir};'
                                )

                        # Center: Page navigation
                        with ui.row().classes('items-center gap-2'):
                            if total_pages > 1:
                                prev_pg_btn = ui.button(
                                    icon='chevron_right' if is_rtl() else 'chevron_left',
                                    on_click=lambda: asyncio.ensure_future(load_page(direction=-1))
                                ).props('flat round size=sm').tooltip(tr('Previous Page'))
                                prev_pg_btn.set_enabled(current_p_num > 1)

                                ui.label(f"{tr('Page')} {current_p_num}/{total_pages}").classes('text-sm font-medium')

                                next_pg_btn = ui.button(
                                    icon='chevron_left' if is_rtl() else 'chevron_right',
                                    on_click=lambda: asyncio.ensure_future(load_page(direction=1))
                                ).props('flat round size=sm').tooltip(tr('Next Page'))
                                next_pg_btn.set_enabled(current_p_num < total_pages)
                            else:
                                ui.label(f"{tr('Page')} 1").classes('text-sm')

                        # Right: Action buttons
                        with ui.row().classes('items-center gap-1'):
                            if sys_id:
                                browse_url = f'/browse?sys_id={sys_id}'
                                if fl_id:
                                    browse_url += f'&fl_id={fl_id}'
                                # Use ui.link for full page reload to ensure browse page recreates with PGP data
                                with ui.link(target=browse_url).classes('no-underline').tooltip(tr('Browse')):
                                    ui.button(icon='menu_book').props('flat round size=sm')

                            if display_text:
                                ui.button(icon='content_copy', on_click=lambda t=display_text: copy_result_text(t)).props('flat round size=sm').tooltip(tr('Copy'))

                            # Exit fullscreen
                            def exit_fullscreen():
                                adv_state.is_fullscreen = False
                                render_content(result)
                            ui.button(icon='fullscreen_exit', on_click=exit_fullscreen).props('flat round size=sm').tooltip(tr('Exit Fullscreen'))

                    # Two-panel layout for fullscreen
                    with ui.row().classes('w-full gap-4 flex-nowrap').style('height: calc(100vh - 120px);'):
                        # Text panel
                        with ui.card().classes('flex-1 h-full overflow-hidden').style('border-radius: 12px;'):
                            with ui.scroll_area().classes('w-full h-full'):
                                with ui.element('div').classes('p-6').style(
                                    'direction: rtl; text-align: right; '
                                    'line-height: 2.4; font-size: 1.3rem; font-family: "SBL Hebrew", "David", serif;'
                                ):
                                    if text_html:
                                        ui.html(text_html, sanitize=False)
                                    else:
                                        ui.label(tr('No text available')).style('color: var(--text-muted);')

                        # Image panel (if available)
                        if has_image and img_url:
                            with ui.card().classes('flex-1 h-full overflow-hidden').style('border-radius: 12px;'):
                                # Image controls
                                with ui.row().classes('w-full items-center justify-between p-2').style('background: #1a1a1a;'):
                                    ui.label(tr('Image')).classes('text-white text-sm')
                                    with ui.row().classes('gap-1'):
                                        ui.button(icon='remove', on_click=lambda: ui.run_javascript('if(window.advViewer) window.advViewer.zoomOut()')).props('flat round size=xs text-color=white')
                                        ui.label('100%').classes('adv-zoom-label text-white text-xs px-1')
                                        ui.button(icon='add', on_click=lambda: ui.run_javascript('if(window.advViewer) window.advViewer.zoomIn()')).props('flat round size=xs text-color=white')
                                        ui.button(icon='rotate_right', on_click=lambda: ui.run_javascript('if(window.advViewer) window.advViewer.rotateRight()')).props('flat round size=xs text-color=white')
                                        ui.button(icon='restart_alt', on_click=lambda: ui.run_javascript('if(window.advViewer) window.advViewer.reset()')).props('flat round size=xs text-color=white')

                                # Image
                                safe_img_url = img_url.replace("'", "\\'").replace('"', '\\"')
                                safe_sys_id = (sys_id or '').replace("'", "\\'").replace('"', '\\"')
                                is_oxford_js = 'true' if is_oxford else 'false'

                                _adv_thumb = safe_img_url
                                _adv_full = safe_img_url
                                if '/api/nli_image_by_sysid/' in safe_img_url:
                                    _sep = '&' if '?' in safe_img_url else '?'
                                    _adv_thumb = f"{safe_img_url}{_sep}width=400"
                                with ui.element('div').classes('adv-image-container img-loading-container w-full').style('height: calc(100% - 48px);'):
                                    img_html = f'''<img src="{_adv_thumb}" data-full-src="{_adv_full}" class="adv-zoomable-image" style="transform: translate(0px, 0px) rotate(0deg) scale(1); cursor: grab; max-height: 100%;" draggable="false" onload="if(window.advViewer) window.advViewer.init()" onerror="handleImageError(this, '{safe_sys_id}', {page_idx}, {is_oxford_js}, 'advViewer')"/>'''
                                    ui.html(img_html, sanitize=False)
                                    ui.run_javascript('setTimeout(() => { if(window.advViewer) window.advViewer.init(); initProgressiveImages(); }, 200);')

                    return  # Exit early for fullscreen mode

                # ============================================================
                # NORMAL MODE - Compact info bar + text/image panels
                # ============================================================

                # --- Prepare enrichment data ---
                fjms_data = adv_state.fjms_data or {}
                from shared.fjms_service import get_fjms_service, merge_catalog_records, parse_textual_frame
                fjms = get_fjms_service(thread_safe=True)
                from web.components.bibliography_dialog import create_fjms_bibliography_dialog, create_nli_bibliography_dialog
                from web.components.catalog_dialog import show_catalog_dialog

                fjms_bib = fjms_data.get('bibliography', [])
                marc_bib = []
                try:
                    from web.state import state as app_state
                    if app_state.meta_mgr and hasattr(app_state.meta_mgr, 'nli_cache'):
                        cached = app_state.meta_mgr.nli_cache.get(sys_id, {})
                        marc_bib = cached.get('marc', {}).get('bibliography', [])
                except Exception:
                    pass
                catalog_source_count = len(fjms_data.get('source_names', []))

                # === Info Bar (outside scroll area) ===
                adv_state.info_bar_container.clear()
                with adv_state.info_bar_container:
                    with ui.column().classes('w-full max-w-6xl mx-auto px-4 py-2 gap-1'):
                        # Row 1: Shelfmark + action buttons
                        with ui.row().classes('items-center justify-between w-full gap-2'):
                            # Left: Shelfmark (compact)
                            with ui.row().classes('items-center gap-2 min-w-0 flex-shrink'):
                                ui.label(display_shelfmark).classes('text-sm font-bold truncate').style(
                                    'color: var(--primary-700); max-width: 400px;'
                                )
                                # Resolve translated title for info bar — always language-aware
                                _adv_title = title
                                if sys_id and search_state.title_translations:
                                    _adv_tt = search_state.title_translations.get(sys_id)
                                    if _adv_tt:
                                        _lang = get_language()
                                        if _lang == 'he':
                                            _adv_title = _adv_tt.get('hebrew_title') or _adv_tt.get('english_title') or title
                                        else:
                                            _adv_title = _adv_tt.get('english_title') or _adv_tt.get('hebrew_title') or title
                                if _adv_title:
                                    _adv_t_short = f'\u2014 {_adv_title[:60]}{"..." if _adv_title and len(_adv_title) > 60 else ""}'
                                    _adv_orig = f'\u2014 {title[:60]}{"..." if title and len(title) > 60 else ""}' if title else ''
                                    _adv_tt_resolved = search_state.title_translations.get(sys_id) if sys_id else None
                                    _adv_dir = 'ltr' if (get_language() != 'he' and _adv_tt_resolved and _adv_tt_resolved.get('english_title')) else 'rtl'
                                    if _adv_orig and _adv_orig != _adv_t_short:
                                        _ib_st = {'showing_original': False}
                                        with ui.row().classes('items-center gap-0 min-w-0'):
                                            _ib_lbl = ui.label(_adv_t_short).classes('text-xs truncate').style(
                                                f'color: var(--text-muted); direction: {_adv_dir}; max-width: 350px;'
                                            )
                                            def _make_ib_toggle(lbl, orig, resolved, flag, resolved_dir):
                                                def handler():
                                                    flag['showing_original'] = not flag['showing_original']
                                                    _dir = 'rtl' if flag['showing_original'] else resolved_dir
                                                    lbl.text = orig if flag['showing_original'] else resolved
                                                    lbl.style(f'color: var(--text-muted); direction: {_dir}; max-width: 350px;')
                                                return handler
                                            ui.button(icon='swap_horiz').props('flat dense round size=xs').style(
                                                'min-width: 18px; min-height: 18px; padding: 0; opacity: 0.4;'
                                            ).tooltip(tr('Show original title')).on(
                                                'click.stop', _make_ib_toggle(_ib_lbl, _adv_orig, _adv_t_short, _ib_st, _adv_dir)
                                            )
                                    else:
                                        ui.label(_adv_t_short).classes(
                                            'text-xs truncate'
                                        ).style(f'color: var(--text-muted); direction: {_adv_dir}; max-width: 350px;')

                            # Right: Action buttons
                            with ui.row().classes('items-center gap-1 shrink-0 flex-wrap'):
                                if sys_id:
                                    browse_url = f'/browse?sys_id={sys_id}'
                                    if fl_id:
                                        browse_url += f'&fl_id={fl_id}'
                                    with ui.link(target=browse_url).classes('no-underline').tooltip(tr('Browse Full Manuscript')):
                                        ui.button(icon='menu_book').props('flat round size=sm color=green')

                                def make_add_handler(r):
                                    def handler():
                                        show_add_to_list_dialog_local(r)
                                    return handler
                                adv_result_sys_id = result.get('display', {}).get('id')
                                adv_result_in_list = state.lists_mgr and adv_result_sys_id and state.lists_mgr.is_item_in_any_list(adv_result_sys_id)
                                ui.button(icon='star' if adv_result_in_list else 'star_border', on_click=make_add_handler(result)).props(
                                    'flat round size=sm'
                                ).style('color: var(--accent-amber);').tooltip(tr('In List') if adv_result_in_list else tr('Add to List'))

                                if WEB_PUZZLE_ENABLED:
                                    # Add to Puzzle button (Phase 49)
                                    def _make_puzzle_handler(sid=adv_result_sys_id, fid=fl_id):
                                        def handler():
                                            param = f'{sid},{fid}' if fid else str(sid)
                                            ui.navigate.to(f'/puzzle?add={param}')
                                        return handler
                                    ui.button(icon='extension', on_click=_make_puzzle_handler()).props(
                                        'flat round size=sm'
                                    ).tooltip(tr('Add to Puzzle'))

                                if has_image:
                                    def toggle_image():
                                        adv_state.show_image_panel = not adv_state.show_image_panel
                                        render_content(result)
                                    ui.button(
                                        icon='image' if adv_state.show_image_panel else 'hide_image',
                                        on_click=toggle_image
                                    ).props('flat round size=sm').tooltip(
                                        tr('Hide Image') if adv_state.show_image_panel else tr('Show Image')
                                    )

                                ui.separator().props('vertical').classes('h-4')

                                # Bibliography / Catalog buttons
                                if fjms_bib:
                                    fjms_dlg = create_fjms_bibliography_dialog(
                                        fjms_bib, sys_id, shelfmark=shelfmark or '',
                                    )
                                    ui.button(
                                        f'{tr("Bib")} ({len(fjms_bib)})',
                                        icon='menu_book', on_click=fjms_dlg.open,
                                    ).props('outline dense size=sm').classes('text-xs').tooltip(tr('Bibliography FJMS'))
                                if marc_bib:
                                    nli_dlg = create_nli_bibliography_dialog(
                                        marc_bib, sys_id, shelfmark=shelfmark or '',
                                    )
                                    ui.button(
                                        f'{tr("Ktiv")} ({len(marc_bib)})',
                                        icon='menu_book', on_click=nli_dlg.open,
                                    ).props('outline dense size=sm').classes('text-xs').tooltip(tr('Bibliography Ktiv'))
                                if catalog_source_count > 0:
                                    ui.button(
                                        f'{tr("Cat")} ({catalog_source_count})',
                                        icon='description',
                                        on_click=lambda s=sys_id, sm=shelfmark or '': show_catalog_dialog(s, sm, fjms),
                                    ).props('outline dense size=sm').classes('text-xs').tooltip(tr('Catalog Records'))

                                # PGP expander button
                                if pgp_metadata:
                                    ui.button(
                                        'PGP', icon='verified',
                                        on_click=lambda: ui.run_javascript("document.querySelector('.pgp-expand .q-expansion-item__toggle')?.click()"),
                                    ).props('outline dense size=sm color=green').classes('text-xs')

                                # FJMS expander button
                                catalog_records = fjms_data.get('catalog_records')
                                if catalog_records:
                                    ui.button(
                                        'FJMS', icon='library_books',
                                        on_click=lambda: ui.run_javascript("document.querySelector('.fjms-expand .q-expansion-item__toggle')?.click()"),
                                    ).props('outline dense size=sm color=purple').classes('text-xs')

                        # Row 2: Chips (source, page, result#, domains)
                        with ui.row().classes('items-center gap-2 flex-wrap'):
                            if source:
                                with ui.element('div').classes('flex items-center gap-1 px-2 py-0.5 rounded-full').style(
                                    'background: var(--primary-100); color: var(--primary-700);'
                                ):
                                    ui.icon('source').classes('text-xs')
                                    ui.label(source).classes('text-xs font-medium')

                            with ui.element('div').classes('flex items-center gap-1 px-2 py-0.5 rounded-full').style(
                                'background: var(--accent-blue); color: white;'
                            ):
                                ui.icon('description').classes('text-xs')
                                ui.label(f"{tr('Page')} {current_p_num}/{total_pages}").classes('text-xs font-medium')

                            with ui.element('div').classes('flex items-center gap-1 px-2 py-0.5 rounded-full').style(
                                'background: var(--bg-tertiary); color: var(--text-secondary);'
                            ):
                                ui.icon('tag').classes('text-xs')
                                ui.label(f"#{adv_state.current_result_idx + 1}").classes('text-xs font-medium')

                            # Subject Domains inline
                            domains = fjms_data.get('domains')
                            if domains:
                                lang = get_language()
                                all_domain_names = {d['domain'] for d in domains}
                                for dom in domains:
                                    parent = dom.get('parent_domain')
                                    if parent and parent in all_domain_names and parent != dom['domain']:
                                        continue
                                    display_name = dom['domain_heb'] if lang == 'he' else dom['domain']
                                    ui.link(
                                        display_name,
                                        f'/search?domain={quote(dom["domain"])}'
                                    ).classes('text-xs px-2 py-0.5 rounded-full no-underline').style(
                                        'background: #f3e8ff; color: #7c3aed;'
                                    )

                            # Printed material indicator in advanced view
                            if adv_state.current_sys_id and adv_state.current_sys_id in search_state.printed_ids:
                                from shared.fjms_service import PRINTED_BADGE_COLORS, PRINTED_LABEL_EN, PRINTED_LABEL_HE
                                from web.translations import get_language as _get_lang
                                _bg, _fg = PRINTED_BADGE_COLORS
                                _plabel = PRINTED_LABEL_HE if _get_lang() == 'he' else PRINTED_LABEL_EN
                                with ui.element('div').classes('flex items-center gap-1 px-2 py-0.5 rounded-full').style(
                                    f'background: {_bg}; color: {_fg};'
                                ):
                                    ui.icon('print').classes('text-xs')
                                    ui.label(_plabel).classes('text-xs font-medium')

                # === Expandable PGP Metadata (inside scroll area, collapsed) ===
                if pgp_metadata:
                    with ui.expansion(group='enrichment').classes('w-full pgp-expand').style(
                        'border-left: 3px solid #27ae60; border-radius: 8px; margin-bottom: 4px;'
                    ).props('dense header-class="text-xs font-bold" label="PGP Details"'):
                        with ui.row().classes('gap-6 flex-wrap'):
                            doc_type = pgp_metadata.get('document_type')
                            lang_primary = pgp_metadata.get('languages_primary')
                            if doc_type or lang_primary:
                                with ui.column().classes('gap-1'):
                                    ui.label(tr('Document Type')).classes('text-xs font-bold').style('color: var(--text-secondary);')
                                    type_parts = [p for p in [doc_type, lang_primary, pgp_metadata.get('languages_secondary')] if p]
                                    _type_text = ' \u00b7 '.join(type_parts)
                                    # Phase 46-06: Show pre-computed document_type_he when UI is Hebrew
                                    _adv_type_he = None
                                    _pgpid_for_type = pgp_metadata.get('pgpid')
                                    _show_type_trans = False
                                    try:
                                        _show_type_trans = app.storage.user.get('show_translations', False)
                                    except Exception:
                                        pass
                                    _type_lang = get_language()
                                    if _show_type_trans and _type_lang == 'he' and _pgpid_for_type and search_state.translation_data:
                                        _type_trans = search_state.translation_data.get(sys_id)
                                        if _type_trans:
                                            _adv_type_he = _type_trans.get('document_type_he')
                                    if _adv_type_he:
                                        _type_st = {'showing_original': False, 'label': None, 'badge': None}
                                        def _make_type_toggle(st, orig, trans):
                                            def handler():
                                                st['showing_original'] = not st['showing_original']
                                                if st['showing_original']:
                                                    st['label'].set_text(orig)
                                                    st['label'].style('color: var(--text-primary); direction: ltr;')
                                                    st['badge'].set_text(tr('Original'))
                                                else:
                                                    st['label'].set_text(trans)
                                                    st['label'].style('color: var(--text-primary); direction: rtl;')
                                                    st['badge'].set_text(tr('Translated'))
                                            return handler
                                        _type_handler = _make_type_toggle(_type_st, _type_text, _adv_type_he)
                                        with ui.row().classes('items-center gap-1'):
                                            _type_st['label'] = ui.label(_adv_type_he).classes('text-sm').style('color: var(--text-primary); direction: rtl;')
                                            _type_st['badge'] = ui.button(tr('Translated'), on_click=_type_handler).props('flat dense no-caps size=xs').classes('text-xs px-1 py-0 rounded shrink-0').style(
                                                'background: #e0f2fe; color: #0369a1; font-style: italic; font-size: 0.65rem; min-height: 0; line-height: 1.2;'
                                            )
                                    else:
                                        ui.label(_type_text).classes('text-sm').style('color: var(--text-primary);')

                            inferred_display = pgp_metadata.get('inferred_date_display')
                            doc_date_standard = pgp_metadata.get('doc_date_standard')
                            doc_date_original = pgp_metadata.get('doc_date_original')
                            if inferred_display or doc_date_standard or doc_date_original:
                                with ui.column().classes('gap-1'):
                                    ui.label(tr('Date')).classes('text-xs font-bold').style('color: var(--text-secondary);')
                                    primary_date = inferred_display or doc_date_standard
                                    if primary_date:
                                        ui.label(primary_date).classes('text-sm').style('color: var(--text-primary);')
                                    if doc_date_original and doc_date_original != primary_date:
                                        ui.label(f"({doc_date_original})").classes('text-xs').style('color: var(--text-tertiary);')

                        tags = pgp_metadata.get('tags', [])
                        if tags:
                            with ui.row().classes('gap-1 flex-wrap mt-2'):
                                for tag in tags:
                                    ui.badge(tag, color='green').props('outline clickable').classes(
                                        'text-xs cursor-pointer'
                                    ).on('click', lambda t=tag: (dialog.close(), ui.navigate.to(f'/search?tag={quote(t)}')))

                        description = (pgp_metadata.get('description') or '').strip()
                        if description:
                            with ui.column().classes('gap-1 mt-2'):
                                ui.label(tr('Description')).classes('text-xs font-bold').style('color: var(--text-secondary);')
                                # Phase 46: Show translated description when toggle is on and UI is Hebrew
                                _show_trans_adv = False
                                try:
                                    _show_trans_adv = app.storage.user.get('show_translations', False)
                                except Exception:
                                    pass
                                _adv_pgpid = pgp_metadata.get('pgpid')
                                _adv_trans_he = None
                                _adv_lang = get_language()
                                if _show_trans_adv and _adv_lang == 'he' and _adv_pgpid:
                                    try:
                                        from shared.translation_service import TranslationService
                                        _tsvc_adv = TranslationService(thread_safe=True)
                                        _adv_trans_he = _tsvc_adv.get_pgp_description_he(_adv_pgpid)
                                        _tsvc_adv.close()
                                    except Exception:
                                        pass
                                if _adv_trans_he:
                                    _adv_st = {'showing_original': False, 'label': None, 'badge': None}
                                    def _make_adv_toggle(st, orig, trans):
                                        def handler():
                                            st['showing_original'] = not st['showing_original']
                                            if st['showing_original']:
                                                st['label'].set_text(orig)
                                                st['label'].style('color: var(--text-primary); direction: ltr; white-space: pre-wrap;')
                                                st['badge'].set_text(tr('Original'))
                                            else:
                                                st['label'].set_text(trans)
                                                st['label'].style('color: var(--text-primary); direction: rtl; white-space: pre-wrap;')
                                                st['badge'].set_text(tr('Translated'))
                                        return handler
                                    _adv_handler = _make_adv_toggle(_adv_st, description, _adv_trans_he)
                                    with ui.row().classes('w-full items-start gap-1'):
                                        _adv_st['label'] = ui.label(_adv_trans_he).classes('flex-1 text-sm whitespace-pre-wrap').style(
                                            'color: var(--text-primary); direction: rtl;'
                                        )
                                        _adv_st['badge'] = ui.button(tr('Translated'), on_click=_adv_handler).props('flat dense no-caps size=xs').classes('text-xs px-1 py-0 rounded shrink-0 self-start mt-1').style(
                                            'background: #e0f2fe; color: #0369a1; font-style: italic; font-size: 0.65rem; min-height: 0; line-height: 1.2;'
                                        )
                                        from web.components.translation_report import create_report_button
                                        create_report_button(
                                            dataset='pgp', record_id=str(_adv_pgpid),
                                            field_name='description', direction='en2he',
                                            source_text=description, translated_text=_adv_trans_he,
                                        )
                                else:
                                    ui.label(description).classes('text-sm whitespace-pre-wrap').style('color: var(--text-primary);')

                        if pgp_metadata.get('pgp_url'):
                            ui.link(tr('View on PGP'), pgp_metadata['pgp_url'], new_tab=True).classes(
                                'text-xs mt-2'
                            ).style('color: var(--primary-600);')

                # === Expandable FJMS Catalog (inside scroll area, collapsed) ===
                catalog_records = fjms_data.get('catalog_records')
                if catalog_records:
                    with ui.expansion(group='enrichment').classes('w-full fjms-expand').style(
                        'border-left: 3px solid #9b59b6; border-radius: 8px; margin-bottom: 4px;'
                    ).props('dense header-class="text-xs font-bold" label="FJMS Details"'):
                        merged = merge_catalog_records(catalog_records)
                        lang = get_language()

                        fjms_title = merged.get('title_heb') if lang == 'he' else merged.get('title')
                        if fjms_title and fjms_title.strip():
                            with ui.row().classes('gap-1 items-baseline'):
                                ui.label(tr('Title')).classes('text-xs font-bold').style('color: var(--text-secondary);')
                                ui.label(fjms_title).classes('text-sm').style('color: var(--text-primary);')

                        if merged.get('author_text') and merged['author_text'].strip():
                            with ui.row().classes('gap-1 items-baseline'):
                                ui.label(tr('Author')).classes('text-xs font-bold').style('color: var(--text-secondary);')
                                ui.label(merged['author_text']).classes('text-sm').style('color: var(--text-primary);')

                        date_val = merged.get('copy_date')
                        place_val = merged.get('copy_place')
                        if date_val or place_val:
                            with ui.row().classes('gap-4'):
                                if date_val:
                                    with ui.row().classes('gap-1 items-baseline'):
                                        ui.label(tr('Copy Date')).classes('text-xs font-bold').style('color: var(--text-secondary);')
                                        ui.label(str(date_val)).classes('text-sm').style('color: var(--text-primary);')
                                if place_val:
                                    with ui.row().classes('gap-1 items-baseline'):
                                        ui.label(tr('Place')).classes('text-xs font-bold').style('color: var(--text-secondary);')
                                        ui.label(place_val).classes('text-sm').style('color: var(--text-primary);')

                        frames = merged.get('textual_frames', [])
                        if frames:
                            ui.label(tr('Content Identification')).classes('text-xs font-bold mt-2').style('color: var(--text-secondary);')
                            max_initial = 10
                            show_frames = frames[:max_initial] if len(frames) > max_initial else frames
                            for frame in show_frames:
                                text_f = frame.get('heb') if lang == 'he' else frame.get('eng')
                                if not text_f or not text_f.strip():
                                    text_f = frame.get('eng') if lang == 'he' else frame.get('heb')
                                if text_f and text_f.strip():
                                    category, content = parse_textual_frame(text_f)
                                    source_name = frame.get('source_name_heb') if lang == 'he' else frame.get('source_name')
                                    with ui.row().classes('gap-1 items-baseline'):
                                        if category:
                                            ui.label(category).classes('text-xs font-bold').style('color: #9b59b6;')
                                            ui.label(content).classes('text-sm').style('color: var(--text-primary);')
                                        else:
                                            ui.label(text_f).classes('text-sm').style('color: var(--text-primary);')
                                        if source_name and source_name.strip():
                                            ui.label(f'({source_name})').classes('text-xs').style('color: var(--text-tertiary);')
                            if len(frames) > max_initial:
                                remaining = frames[max_initial:]
                                with ui.expansion(f'{tr("Show all")} {len(frames)} {tr("identifications")}').classes('text-xs'):
                                    for frame in remaining:
                                        text_f = frame.get('heb') if lang == 'he' else frame.get('eng')
                                        if not text_f or not text_f.strip():
                                            text_f = frame.get('eng') if lang == 'he' else frame.get('heb')
                                        if text_f and text_f.strip():
                                            category, content = parse_textual_frame(text_f)
                                            source_name = frame.get('source_name_heb') if lang == 'he' else frame.get('source_name')
                                            with ui.row().classes('gap-1 items-baseline'):
                                                if category:
                                                    ui.label(category).classes('text-xs font-bold').style('color: #9b59b6;')
                                                    ui.label(content).classes('text-sm').style('color: var(--text-primary);')
                                                else:
                                                    ui.label(text_f).classes('text-sm').style('color: var(--text-primary);')
                                                if source_name and source_name.strip():
                                                    ui.label(f'({source_name})').classes('text-xs').style('color: var(--text-tertiary);')

                        cat_refs = fjms_data.get('catalog_refs')
                        if cat_refs:
                            ui.label(tr('Catalog References')).classes('text-xs font-bold mt-2').style('color: var(--text-secondary);')
                            with ui.row().classes('gap-2 flex-wrap'):
                                for ref in cat_refs:
                                    acronym = ref.get('cat_acronym', '')
                                    cat_entry = ref.get('catalog_entry', '')
                                    ref_display = f"{acronym} #{cat_entry}" if cat_entry else acronym
                                    ui.label(ref_display).classes('text-xs').style('color: var(--text-primary);')

                # === Two-Panel Layout: Text + Image ===
                with ui.row().classes('w-full gap-4 flex-wrap lg:flex-nowrap'):
                    # Left Panel: Text content with inline editing
                    text_panel_classes = 'flex-1 min-w-[300px]' if adv_state.show_image_panel and has_image else 'w-full'

                    # Edit mode border styling
                    panel_border = ''
                    if adv_state.edit_mode:
                        panel_border = 'border: 3px solid #27ae60;' if adv_state.draft_saved else 'border: 3px solid #f39c12;'

                    with ui.column().classes(text_panel_classes + ' gap-4'):

                        # Define text container and render function at this scope for version switching
                        text_content_container = None
                        current_display_text = {'value': display_text, 'html': text_html}

                        def render_text_section(html_to_render: str):
                            """Render pre-formatted HTML text content (called on initial render and version change)."""
                            nonlocal text_content_container
                            if text_content_container is None:
                                return
                            text_content_container.clear()
                            with text_content_container:
                                with ui.scroll_area().classes('w-full').style('max-height: 70vh;'):
                                    with ui.element('div').classes('p-6').style(
                                        'direction: rtl; text-align: right; '
                                        'line-height: 2.4; font-size: 1.2rem; font-family: "SBL Hebrew", "David", serif;'
                                    ):
                                        ui.html(html_to_render or '', sanitize=False)
                            text_content_container.update()

                        # Page Text Section with inline editing
                        if display_text or adv_state.edit_mode:
                            with ui.card().classes('w-full').style(f'border-radius: 16px; {panel_border}'):

                                if adv_state.edit_mode:
                                    # === EDIT MODE ===
                                    # Edit toolbar
                                    with ui.row().classes('w-full items-center justify-between p-3 bg-gray-100 border-b'):
                                        with ui.row().classes('items-center gap-2'):
                                            ui.icon('edit').classes('text-primary')
                                            ui.label(tr('Edit Mode')).classes('font-bold')
                                            if adv_state.draft_saved:
                                                ui.label(tr('Saved')).classes('text-green-600 text-sm font-bold')
                                            else:
                                                ui.label(tr('Unsaved')).classes('text-orange-600 text-sm')

                                        with ui.row().classes('gap-2'):
                                            ui.button(tr('Cancel'), icon='close', on_click=lambda: cancel_edit(result)).props('flat dense color=grey')
                                            ui.button(tr('Save'), icon='save', on_click=lambda: save_draft(sys_id, shelfmark, current_p_num, current_text)).props('flat dense color=primary')
                                            ui.button(tr('Submit'), on_click=lambda: submit_correction(sys_id, shelfmark, current_p_num, current_text, result)).props('unelevated dense color=green')

                                    # Editable textarea
                                    textarea = ui.textarea(value=adv_state.edit_text).classes('w-full').props(
                                        'borderless autofocus'
                                    ).style(
                                        'direction: rtl; text-align: right; resize: none; min-height: 400px; padding: 16px; '
                                        'font-family: "SBL Hebrew", "David", serif; font-size: 1.2rem; line-height: 2;'
                                    )
                                    textarea.bind_value(adv_state, 'edit_text')

                                    def on_edit_change():
                                        if adv_state.draft_saved:
                                            adv_state.draft_saved = False
                                    textarea.on('input', on_edit_change)

                                    # Notes field
                                    with ui.expansion(tr('Add Notes'), icon='note_add').classes('w-full border-t'):
                                        ui.textarea(value=adv_state.edit_notes, placeholder=tr('Notes about your correction')).bind_value(adv_state, 'edit_notes').classes('w-full').props('outlined dense').style('direction: rtl;')

                                else:
                                    # === VIEW MODE ===
                                    # Header with page info, page navigation, and actions
                                    with ui.row().classes('items-center justify-between w-full px-4 py-2 border-b').style('border-color: var(--border-light);'):
                                        with ui.row().classes('items-center gap-2'):
                                            ui.icon('article').classes('text-lg').style('color: var(--primary-600);')
                                            ui.label(f"{tr('Page')} {current_p_num}").classes('text-sm font-bold')
                                            word_count = len(display_text.split()) if display_text else 0
                                            ui.label(f"({word_count} {tr('words')})").classes('text-xs').style('color: var(--text-muted);')

                                            # Page navigation (merged into header)
                                            if total_pages > 1:
                                                ui.separator().props('vertical').classes('h-4 mx-1')
                                                prev_page_btn = ui.button(
                                                    icon='chevron_right' if is_rtl() else 'chevron_left',
                                                    on_click=lambda: asyncio.ensure_future(load_page(direction=-1))
                                                ).props('flat round size=xs').tooltip(tr('Previous Page'))
                                                prev_page_btn.set_enabled(current_p_num > 1)

                                                page_input = ui.number(value=current_p_num, min=1, max=total_pages).classes('w-12').props('dense outlined borderless')
                                                ui.label(f"/{total_pages}").classes('text-xs').style('color: var(--text-secondary);')

                                                def go_to_page():
                                                    try:
                                                        p = int(page_input.value) if page_input.value else 1
                                                        p = max(1, min(total_pages, p))
                                                        asyncio.ensure_future(load_page(p_num=p))
                                                    except (ValueError, TypeError):
                                                        pass
                                                page_input.on('keydown.enter', lambda: go_to_page())

                                                next_page_btn = ui.button(
                                                    icon='chevron_left' if is_rtl() else 'chevron_right',
                                                    on_click=lambda: asyncio.ensure_future(load_page(direction=1))
                                                ).props('flat round size=xs').tooltip(tr('Next Page'))
                                                next_page_btn.set_enabled(current_p_num < total_pages)

                                        with ui.row().classes('gap-1'):
                                            ui.button(icon='content_copy', on_click=lambda t=display_text: copy_result_text(t)).props('flat round size=sm').tooltip(tr('Copy Text'))
                                            if sys_id and current_text:
                                                ui.button(icon='edit', on_click=lambda: toggle_edit_mode(current_text)).props('flat round size=sm').tooltip(tr('Edit'))

                                    # Text content - create container (same scope as outer text_content_container)
                                    text_content_container = ui.element('div').classes('w-full')

                                    # Initial render (use highlighted HTML)
                                    render_text_section(text_html)

                        # Community Features Row (compact) - only in view mode
                        if sys_id and current_text and not adv_state.edit_mode:
                            with ui.row().classes('gap-2 flex-wrap items-center'):
                                from web.components import (
                                    create_version_selector,
                                    create_comment_button, create_joins_button
                                )

                                def handle_version_change(new_text: str, version_info: dict):
                                    """Handle version selection - update displayed text."""
                                    current_display_text['value'] = new_text
                                    # Re-apply search term highlighting to new version text
                                    if adv_state.highlight_terms and new_text:
                                        new_html = _apply_highlight_marks(new_text, adv_state.highlight_terms)
                                    else:
                                        new_html = new_text.replace('\n', '<br>') if new_text else ''
                                    current_display_text['html'] = new_html
                                    render_text_section(new_html)
                                    source = version_info.get('source', 'unknown')

                                    if source == 'pgp':
                                        attribution = version_info.get('attribution', 'PGP')
                                        ui.notify(f"{tr('PGP Transcription')} - {attribution}", type='positive')
                                    elif source == 'translation':
                                        attribution = version_info.get('attribution', '')
                                        language = version_info.get('language', '')
                                        ui.notify(f"{language} {tr('Translation')} - {attribution}", type='info')
                                    elif source == 'user' and version_info.get('author'):
                                        ui.notify(f"{tr('Showing version by')} {version_info.get('author')}", type='info')
                                    elif source in ('V0.7', 'V0.8'):
                                        ui.notify(f"{tr('Showing')} {source}", type='info')

                                create_version_selector(
                                    document_id=sys_id,
                                    page_number=current_p_num,
                                    original_text=current_text,
                                    on_version_change=handle_version_change,
                                    pgp_transcription=pgp_transcription,
                                    all_sources=all_sources
                                )

                                create_comment_button(
                                    document_id=sys_id,
                                    page_number=current_p_num,
                                    shelfmark=shelfmark,
                                    size='sm'
                                )

                                if shelfmark:
                                    def navigate_to_join(target_shelfmark: str):
                                        dialog.close()
                                        ui.navigate.to(f'/browse?shelfmark={target_shelfmark}')

                                    create_joins_button(
                                        shelfmark=shelfmark,
                                        document_id=sys_id,
                                        on_navigate=navigate_to_join
                                    )

                    # Right Panel: Image viewer (toggleable)
                    if adv_state.show_image_panel and has_image and img_url:
                        with ui.column().classes('flex-1 min-w-[300px]'):
                            with ui.card().classes('w-full').style('border-radius: 16px; overflow: hidden;'):
                                # Image controls header
                                with ui.row().classes('w-full items-center justify-between p-3').style(
                                    'background: #1a1a1a; border-radius: 8px 8px 0 0;'
                                ):
                                    ui.label(tr('Manuscript Image')).classes('text-white font-semibold')
                                    with ui.row().classes('gap-1'):
                                        ui.button(icon='remove', on_click=lambda: ui.run_javascript('if(window.advViewer) window.advViewer.zoomOut()')).props('flat round size=sm text-color=white').tooltip(tr('Zoom out'))
                                        ui.label('100%').classes('adv-zoom-label text-white text-sm px-2')
                                        ui.button(icon='add', on_click=lambda: ui.run_javascript('if(window.advViewer) window.advViewer.zoomIn()')).props('flat round size=sm text-color=white').tooltip(tr('Zoom in'))
                                        ui.separator().props('vertical').classes('mx-1 h-4 bg-gray-600')
                                        ui.button(icon='rotate_left', on_click=lambda: ui.run_javascript('if(window.advViewer) window.advViewer.rotateLeft()')).props('flat round size=sm text-color=white').tooltip(tr('Rotate Left'))
                                        ui.button(icon='rotate_right', on_click=lambda: ui.run_javascript('if(window.advViewer) window.advViewer.rotateRight()')).props('flat round size=sm text-color=white').tooltip(tr('Rotate Right'))
                                        ui.separator().props('vertical').classes('mx-1 h-4 bg-gray-600')
                                        ui.button(icon='restart_alt', on_click=lambda: ui.run_javascript('if(window.advViewer) window.advViewer.reset()')).props('flat round size=sm text-color=white').tooltip(tr('Reset View'))

                                # Image adjustment controls
                                with ui.row().classes('w-full items-center gap-2 px-3 py-1').style(
                                    'background: #1a1a1a; border-top: 1px solid #333;'
                                ):
                                    ui.icon('brightness_6').classes('text-white text-sm').tooltip(tr('Brightness'))
                                    adv_state.brightness_sl = ui.slider(
                                        min=-100, max=100, step=1, value=0,
                                        on_change=lambda e: ui.run_javascript(f'if(window.advViewer) window.advViewer.setBrightness({e.value})')
                                    ).props('dark dense').classes('w-24')
                                    ui.icon('contrast').classes('text-white text-sm').tooltip(tr('Contrast'))
                                    adv_state.contrast_sl = ui.slider(
                                        min=-100, max=100, step=1, value=0,
                                        on_change=lambda e: ui.run_javascript(f'if(window.advViewer) window.advViewer.setContrast({e.value})')
                                    ).props('dark dense').classes('w-24')
                                    ui.icon('timeline').classes('text-white text-sm').tooltip(tr('Gamma'))
                                    adv_state.gamma_sl = ui.slider(
                                        min=20, max=300, step=1, value=100,
                                        on_change=lambda e: ui.run_javascript(f'if(window.advViewer) window.advViewer.setGamma({e.value / 100})')
                                    ).props('dark dense').classes('w-24')
                                    ui.button(
                                        icon='exposure',
                                        on_click=lambda: ui.run_javascript('if(window.advViewer) window.advViewer.toggleInvert()')
                                    ).props('flat round size=sm text-color=white').tooltip(tr('Invert Colors'))
                                    def _adv_reset_adj():
                                        if hasattr(adv_state, 'brightness_sl'): adv_state.brightness_sl.value = 0
                                        if hasattr(adv_state, 'contrast_sl'): adv_state.contrast_sl.value = 0
                                        if hasattr(adv_state, 'gamma_sl'): adv_state.gamma_sl.value = 100
                                        ui.run_javascript('if(window.advViewer) window.advViewer.resetAdjustments()')
                                    ui.button(
                                        icon='restart_alt',
                                        on_click=_adv_reset_adj
                                    ).props('flat round size=sm text-color=white').tooltip(tr('Reset Image'))

                                # Image display
                                safe_img_url = img_url.replace("'", "\\'").replace('"', '\\"')
                                safe_sys_id = (sys_id or '').replace("'", "\\'").replace('"', '\\"')
                                is_oxford_js = 'true' if is_oxford else 'false'

                                _adv2_thumb = safe_img_url
                                _adv2_full = safe_img_url
                                if '/api/nli_image_by_sysid/' in safe_img_url:
                                    _sep = '&' if '?' in safe_img_url else '?'
                                    _adv2_thumb = f"{safe_img_url}{_sep}width=400"
                                with ui.element('div').classes('adv-image-container img-loading-container w-full').style('height: 70vh;'):
                                    img_html = f'''
                                    <img
                                        src="{_adv2_thumb}"
                                        data-full-src="{_adv2_full}"
                                        class="adv-zoomable-image"
                                        style="transform: translate(0px, 0px) rotate(0deg) scale(1); cursor: grab; max-height: 100%;"
                                        draggable="false"
                                        onload="if(window.advViewer) window.advViewer.init()"
                                        onerror="handleImageError(this, '{safe_sys_id}', {page_idx}, {is_oxford_js}, 'advViewer')"
                                    />
                                    '''
                                    ui.html(img_html, sanitize=False)
                                    ui.run_javascript('setTimeout(() => { if(window.advViewer) window.advViewer.init(); initProgressiveImages(); }, 200);')

                                # Attribution footer
                                attribution = ''
                                if is_oxford:
                                    attribution = 'From the collections of the Bodleian Libraries, Oxford'
                                elif page and page.attribution:
                                    attribution = page.attribution
                                else:
                                    attribution = 'הספרייה הלאומית / National Library of Israel'

                                with ui.row().classes('w-full items-center justify-center gap-2 py-2').style(
                                    'background: #2a2a2a; border-radius: 0 0 8px 8px;'
                                ):
                                    ui.icon('photo_library', size='xs').style('color: #888; font-size: 14px;')
                                    ui.label(attribution).classes('text-xs').style('color: #aaa; font-style: italic;')

                # === Actions Section ===
                with ui.card().classes('w-full p-6').style('border-radius: 16px; background: var(--bg-tertiary);'):
                    h3(tr('Actions'), classes='text-lg font-bold mb-4', style='color: var(--text-primary);')

                    with ui.row().classes('gap-4 flex-wrap'):
                        if sys_id:
                            browse_url = f'/browse?sys_id={sys_id}'
                            if fl_id:
                                browse_url += f'&fl_id={fl_id}'
                            # Use ui.link for full page reload to ensure browse page recreates with PGP data
                            with ui.link(target=browse_url).classes('btn-primary no-underline'):
                                ui.icon('menu_book').classes('mr-2')
                                ui.label(tr('Browse Full Manuscript'))

                        text_for_parallels = current_text or snippet.replace('*', '')
                        if text_for_parallels:
                            ui.button(
                                tr('Find Parallels'), icon='compare_arrows',
                                on_click=lambda t=text_for_parallels: (
                                    dialog.close(),
                                    ui.navigate.to(f'/parallels?text={quote(t[:2000])}')
                                )
                            ).props('outline')

                        text_to_copy = current_text or snippet.replace('*', '')
                        if text_to_copy:
                            ui.button(
                                tr('Copy Text'), icon='content_copy',
                                on_click=lambda t=text_to_copy: copy_result_text(t)
                            ).props('outline')

        # Initial load
        load_result(index)
        dialog.open()

    def copy_result_text(text):
        """Copy text to clipboard."""
        if text:
            # Escape backticks for JavaScript
            escaped_text = text.replace('`', '\\`')
            ui.run_javascript(f'''
                navigator.clipboard.writeText(`{escaped_text}`).then(() => {{
                    console.log('Text copied to clipboard');
                }});
            ''')
            ui.notify(tr('Text copied to clipboard'), type='positive')
        else:
            ui.notify(tr('No text to copy'), type='warning')

    def show_add_to_list_dialog_local(result):
        from web.components import show_add_to_list_dialog as show_dialog
        display = result.get('display', {})
        sys_id = display.get('id')
        if not sys_id:
            ui.notify(tr('Cannot add: missing system ID'), type='warning')
            return
        if not state.lists_mgr:
            ui.notify(tr('Lists manager not available'), type='warning')
            return
        shelfmark = display.get('shelfmark', 'Unknown')
        show_dialog(
            sys_id=sys_id,
            shelfmark=shelfmark,
            lists_mgr=state.lists_mgr,
            note_default='',  # Empty by default
            fl_id=None
        )

    # --- Restore Responsa state from URL parameters ---
    if initial_mode == 'responsa':
        mode_select.value = 'responsa'
        responsa_sub_row.set_visibility(True)
        if initial_variants:
            responsa_variants_cb.value = True
        if initial_ja:
            responsa_ja_cb.value = True
        if initial_flex_spaces:
            responsa_flex_cb.value = True
        if initial_bidirectional:
            bidirectional_cb.value = True

    # Handle tag search — switch UI to PGP Tags mode and show the selected tag
    if initial_tag:
        mode_select.value = 'pgp_tags'
        query_column.set_visibility(False)
        tag_column.set_visibility(True)
        tag_select.value = initial_tag
        def _tag_to_result(tag_result):
            """Convert a tag search result dict to the shape expected by open_advanced_dialog."""
            return {
                'display': {
                    'id': tag_result.get('sys_id', ''),
                    'shelfmark': tag_result.get('shelfmark', 'Unknown'),
                    'library_code': '',
                    'title': tag_result.get('description', ''),
                    'source': 'PGP',
                },
                'snippet': '',
                'full_text': '',
            }


        async def load_tag_results():
            """Load results for a tag-based search."""
            results_container.clear()
            with results_container:
                with ui.column().classes('w-full h-64 items-center justify-center'):
                    ui.spinner('bars', size='xl', color='primary').classes('mb-4')
                    ui.label(tr("Searching by tag...")).classes('text-xl font-bold animate-pulse').style('color: var(--primary-600);')

            tag_results = await run.io_bound(get_fragments_by_tag, initial_tag)

            # Filter to only fragments that exist in local index (browseable)
            if tag_results and hasattr(state, 'meta_mgr') and state.meta_mgr and hasattr(state.meta_mgr, 'csv_bank'):
                tag_results = [r for r in tag_results if r.get('sys_id') in state.meta_mgr.csv_bank]

            # Batch-fetch PGP translations for tag results (Hebrew descriptions/types)
            _tag_trans = {}
            if tag_results and get_language() == 'he':
                try:
                    _tag_show = app.storage.user.get('show_translations', False)
                    if _tag_show:
                        _tag_sids = [r.get('sys_id') for r in tag_results if r.get('sys_id')]
                        if _tag_sids:
                            def _fetch_tag_trans():
                                from shared.translation_service import TranslationService
                                svc = TranslationService(thread_safe=True)
                                result = svc.get_pgp_translations_by_sys_ids(_tag_sids) if svc.pgp_available() else {}
                                svc.close()
                                return result
                            _tag_trans = await run.io_bound(_fetch_tag_trans)
                except Exception:
                    pass

            results_container.clear()
            with results_container:
                if not tag_results:
                    with ui.column().classes('w-full h-64 items-center justify-center'):
                        ui.icon('label_off').classes('text-5xl').style('color: var(--text-muted);')
                        ui.label(f'{tr("No results for tag")}: "{initial_tag}"').classes('mt-4').style('color: var(--text-muted);')
                    return

                # Tag results header
                with ui.column().classes('w-full gap-2 p-4'):
                    with ui.row().classes('items-center gap-2 mb-3'):
                        ui.icon('label').classes('text-lg').style('color: var(--success-600);')
                        ui.label(f'{tr("Tag")}: ').classes('text-sm').style('color: var(--text-secondary);')
                        ui.badge(initial_tag, color='green').props('outline').classes('text-sm')
                        ui.label(f'({len(tag_results)} {tr("results")})').classes('text-sm').style('color: var(--text-muted);')

                    # Render tag results as cards
                    for i, result in enumerate(tag_results):
                        with ui.card().classes(
                            'w-full p-4 cursor-pointer transition-all hover:shadow-md'
                        ).style('border-radius: 10px;'):
                            with ui.row().classes('w-full items-start justify-between'):
                                with ui.column().classes('flex-grow min-w-0 gap-1').on(
                                    'click',
                                    lambda r=result: open_advanced_dialog(None, _tag_to_result(r))
                                ):
                                    with ui.row().classes('items-center gap-2 flex-wrap'):
                                        ui.label(f"#{i + 1}").classes('text-xs px-2 py-0.5 rounded shrink-0').style(
                                            'background: var(--bg-tertiary); color: var(--text-muted);'
                                        )
                                        # PGP indicator
                                        ui.label('PGP').classes('text-xs px-2 py-0.5 rounded shrink-0').style(
                                            'background: var(--success-100); color: var(--success-700); font-weight: 600;'
                                        ).tooltip(tr('Has PGP Transcription'))
                                        ui.label(result.get('shelfmark', 'Unknown')).classes(
                                            'font-bold break-all'
                                        ).style('color: var(--primary-700);')

                                    # Document type (with translation if available)
                                    _tag_r_trans = _tag_trans.get(result.get('sys_id'), {}) if _tag_trans else {}
                                    _tag_doc_type = _tag_r_trans.get('document_type_he') or result.get('document_type')
                                    if _tag_doc_type:
                                        ui.label(_tag_doc_type).classes('text-xs').style(
                                            'color: var(--text-tertiary);'
                                        )

                                    # Description snippet (with translation if available)
                                    desc = _tag_r_trans.get('description_he') or result.get('description', '') or ''
                                    if desc:
                                        truncated = (desc[:150] + '...') if len(desc) > 150 else desc
                                        ui.label(truncated).classes('text-sm').style('color: var(--text-secondary); line-height: 1.4; font-size: 0.75rem;')

        asyncio.ensure_future(_after_delay(0.1, load_tag_results))

    # Initialize with restored results or initial query
    elif initial_query:
        asyncio.ensure_future(_after_delay(0.5, execute_search))
    elif search_state.results:
        results_count.text = f"{len(search_state.results)} {tr('Results')}"
        render_results(search_state.results, page=0)
        ui.notify(tr('Session restored'), type='info', timeout=3000, position='top')

    # --- Deferred initialization (runs after UI renders) ---

    async def _deferred_filter_init():
        """Load filter select options asynchronously after page renders."""
        lang = get_language()  # Capture in client context before io_bound
        d = await run.io_bound(build_domain_options, lang)
        domain_select.options = d
        domain_select.update()
        a = await run.io_bound(build_author_options, lang, search_state.filter_domains)
        author_select.options = a
        author_select.update()
        w = await run.io_bound(build_work_options, lang, search_state.filter_domains, search_state.filter_authors)
        work_select.options = w
        work_select.update()
        _update_chip_bar()

    asyncio.ensure_future(_after_delay(0.1, _deferred_filter_init))

    async def _deferred_transcription_restore():
        """Restore transcription indicators for saved results asynchronously."""
        if search_state.results:
            sys_ids = [
                r.get('display', {}).get('id')
                for r in search_state.results
                if r.get('display', {}).get('id')
            ]
            if sys_ids:
                search_state.transcription_sys_ids = await run.io_bound(
                    get_sys_ids_with_transcriptions, sys_ids
                )
                render_results(search_state.results, page=search_state.current_page)

    asyncio.ensure_future(_after_delay(0.2, _deferred_transcription_restore))

    # Phase 55: Deferred refinement chain replay on session restore
    if search_state.refinement_chain:
        asyncio.ensure_future(_after_delay(0.3, _deferred_chain_replay))
