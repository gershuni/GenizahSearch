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
from web.translations import tr, is_rtl
from web.components.typography import h1, h2, h3, h4
from web.services import get_service, BrowsePage
from genizah_core import SearchEngine, get_library_display
from web.document_service import get_sys_ids_with_transcriptions, get_all_sources_for_fragment, get_document_for_fragment, get_section_for_page, get_fragments_by_tag
from urllib.parse import quote
from typing import Optional, List, Dict, Any, Set
from dataclasses import dataclass, field
import re
import html


def create_search_page(initial_query: str = None, initial_tag: str = None):
    """Create the advanced search page."""

    # === State Management ===
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
            self.selected_indices = set()  # For bulk operations
            self.is_panel_collapsed = False  # For collapsible search panel
            self.last_scroll_top = 0  # For scroll-based auto-collapse
            self.update_timer = None  # Track timer to prevent duplicates
            self.transcription_sys_ids: Set[str] = set()  # sys_ids with PGP transcriptions

    search_state = SearchUIState()

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
            # UI element references for in-place updates
            self.result_label = None
            self.score_badge = None
            self.prev_btn = None
            self.next_btn = None
            self.content_container = None
            self.image_container = None

    # === VIEWER_STYLES for Advanced View image handling (must be at page level) ===
    ADVANCED_VIEWER_STYLES = '''
    <script>
    // NLI IIIF base URL for direct browser access
    const NLI_IIIF_BASE = 'https://iiif.nli.org.il/IIIFv21';
    const advFlIdCache = {};

    async function advFetchFlIdsFromManifest(sysId) {
        if (advFlIdCache[sysId]) return advFlIdCache[sysId];
        const manifestUrl = `${NLI_IIIF_BASE}/DOCID/PNX_MANUSCRIPTS${sysId}-1/manifest`;
        try {
            const resp = await fetch(manifestUrl);
            if (!resp.ok) return [];
            const data = await resp.json();
            const flIds = [];
            if (data.sequences && data.sequences[0] && data.sequences[0].canvases) {
                for (const canvas of data.sequences[0].canvases) {
                    const images = canvas.images || [];
                    if (images[0] && images[0].resource && images[0].resource.service) {
                        const serviceId = images[0].resource.service['@id'] || '';
                        const match = serviceId.match(/FL(\\d+)/);
                        if (match) flIds.push(match[1]);
                    }
                }
            }
            if (flIds.length > 0) advFlIdCache[sysId] = flIds;
            return flIds;
        } catch (e) { return []; }
    }

    async function advHandleImageError(img, sysId, pageIdx, isOxford = false) {
        const currentSrc = img.src || '';
        const isOxfordApiUrl = currentSrc.includes('/api/oxford_image/');
        console.log('[advHandleImageError]', {currentSrc, sysId, pageIdx, isOxford});
        if (isOxford && sysId && !isOxfordApiUrl && !img.dataset.triedOxford) {
            img.dataset.triedOxford = 'true';
            img.src = `/api/oxford_image/${sysId}?page=${pageIdx || 0}`;
            img.onload = function() { if(window.advViewer) window.advViewer.init(); };
            return;
        }
        if (isOxfordApiUrl) img.dataset.triedOxford = 'true';
        if (sysId && !img.dataset.triedManifest) {
            img.dataset.triedManifest = 'true';
            const flIds = await advFetchFlIdsFromManifest(sysId);
            if (flIds.length > 0) {
                const idx = Math.min(pageIdx || 0, flIds.length - 1);
                img.src = `${NLI_IIIF_BASE}/FL${flIds[idx]}/full/max/0/default.jpg`;
                img.onload = function() { if(window.advViewer) window.advViewer.init(); };
                return;
            }
        }
        if (sysId && !img.dataset.triedServerProxy) {
            img.dataset.triedServerProxy = 'true';
            img.src = `/api/nli_image_by_sysid/${sysId}?page=${pageIdx || 0}`;
            img.onload = function() { if(window.advViewer) window.advViewer.init(); };
            return;
        }
        console.log('[advHandleImageError] All fallbacks exhausted');
        img.style.display = 'none';
        const parent = img.parentElement;
        if (parent) {
            parent.innerHTML = '<div style="text-align: center; color: #888;"><i class="material-icons" style="font-size: 3rem;">image_not_supported</i><p>Image not available</p></div>';
        }
    }

    window.advViewer = {
        el: null, container: null,
        state: { scale: 1, rotation: 0, x: 0, y: 0, isDragging: false, startX: 0, startY: 0 },
        init: function() {
            this.el = document.querySelector('.adv-zoomable-image');
            this.container = document.querySelector('.adv-image-container');
            if (!this.el || !this.container) return;
            this.el.onmousedown = this.onMouseDown.bind(this);
            window.onmousemove = this.onMouseMove.bind(this);
            window.onmouseup = this.onMouseUp.bind(this);
            this.el.ondragstart = (e) => e.preventDefault();
            this.el.onwheel = this.onWheel.bind(this);
            this.el.style.cursor = 'grab';
        },
        onWheel: function(e) {
            e.preventDefault();
            const delta = e.deltaY > 0 ? -0.25 : 0.25;
            this.state.scale = Math.max(0.25, Math.min(4, this.state.scale + delta));
            this.applyTransform();
            const zoomLabel = document.querySelector('.adv-zoom-label');
            if (zoomLabel) zoomLabel.textContent = Math.round(this.state.scale * 100) + '%';
        },
        onMouseDown: function(e) {
            if (e.button !== 0) return;
            e.preventDefault(); e.stopPropagation();
            this.state.isDragging = true;
            this.state.startX = e.clientX - this.state.x;
            this.state.startY = e.clientY - this.state.y;
            this.el.style.cursor = 'grabbing';
        },
        onMouseMove: function(e) {
            if (!this.state.isDragging) return;
            e.preventDefault();
            this.state.x = e.clientX - this.state.startX;
            this.state.y = e.clientY - this.state.startY;
            requestAnimationFrame(() => this.applyTransform());
        },
        onMouseUp: function() {
            this.state.isDragging = false;
            if (this.el) this.el.style.cursor = 'grab';
        },
        applyTransform: function() {
            if (!this.el) { this.el = document.querySelector('.adv-zoomable-image'); if (!this.el) return; }
            this.el.style.transform = `translate(${this.state.x}px, ${this.state.y}px) rotate(${this.state.rotation}deg) scale(${this.state.scale})`;
        },
        zoomIn: function() { this.state.scale = Math.min(4, this.state.scale + 0.25); this.applyTransform(); this.updateLabel(); },
        zoomOut: function() { this.state.scale = Math.max(0.25, this.state.scale - 0.25); this.applyTransform(); this.updateLabel(); },
        rotateLeft: function() { this.state.rotation = (this.state.rotation - 90) % 360; this.applyTransform(); },
        rotateRight: function() { this.state.rotation = (this.state.rotation + 90) % 360; this.applyTransform(); },
        reset: function() { this.state.x = 0; this.state.y = 0; this.state.rotation = 0; this.state.scale = 1; this.applyTransform(); this.updateLabel(); },
        updateLabel: function() { const l = document.querySelector('.adv-zoom-label'); if (l) l.textContent = Math.round(this.state.scale * 100) + '%'; }
    };
    </script>
    <style>
    .adv-image-container { position: relative; background: #1a1a1a; border-radius: 8px; overflow: hidden; min-height: 400px; display: flex; align-items: center; justify-content: center; }
    .adv-zoomable-image { max-width: 100%; max-height: 100%; object-fit: contain; cursor: grab; transform-origin: center center; transition: transform 0.1s ease-out; }
    .adv-zoomable-image:active { cursor: grabbing; }
    </style>
    '''

    # Restore previous results
    if 'search_results' in app.storage.user:
        try:
            search_state.results = app.storage.user.get('search_results', [])
            # Also restore transcription indicators for saved results
            if search_state.results:
                result_sys_ids = [
                    r.get('display', {}).get('id')
                    for r in search_state.results
                    if r.get('display', {}).get('id')
                ]
                if result_sys_ids:
                    search_state.transcription_sys_ids = get_sys_ids_with_transcriptions(result_sys_ids)
        except Exception:
            pass

    # Restore search settings from storage
    saved_mode = app.storage.user.get('search_mode', 'exact')
    saved_query = app.storage.user.get('search_query', '')
    saved_preset = app.storage.user.get('search_preset', 30)
    saved_max_changes = app.storage.user.get('search_max_changes', 2)
    saved_gap = app.storage.user.get('search_gap', 0)

    # Check if user prefers slider or presets (default: presets/dropdown)
    use_slider = False
    if state.lab_engine and hasattr(state.lab_engine, 'settings') and state.lab_engine.settings:
        use_slider = getattr(state.lab_engine.settings, 'variant_use_slider', False)

    # In slider mode, convert extended variant modes to 'variants'
    if use_slider and saved_mode in ('variants_extended', 'variants_maximum'):
        saved_mode = 'variants'


    # === UI Layout ===
    # Add Advanced View image handler JavaScript at page level (must be outside dialog)
    ui.add_head_html(ADVANCED_VIEWER_STYLES)

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
                                     'Shelfmark': '#', 'Title': '$'}
                        collapsed_mode_badge.text = mode_names.get(mode_val, mode_val)
                        expanded_panel.style('display: none !important;')
                        collapsed_panel.style('background: var(--bg-card); border-color: var(--border-light) !important; display: block !important;')

                    ui.button(
                        icon='expand_less', on_click=collapse_panel
                    ).props('flat round dense size=sm').tooltip(tr('Collapse search panel'))

                # Main Search Row
                with ui.row().classes('w-full items-end gap-4 flex-wrap'):

                    # Search Input (Main)
                    with ui.column().classes('flex-grow min-w-80 gap-1'):
                        # Changed to H2 semantic label
                        h2(tr('Search Query'), classes='text-sm font-medium', style='color: var(--text-secondary);')
                        query_input = ui.input(
                            placeholder=tr('Enter Hebrew text to search'),
                            value=initial_query or saved_query
                        ).classes('w-full text-lg').props('outlined dense clearable').style('direction: rtl;')
                        query_input.on('keydown.enter', lambda: execute_search())

                        # Save query on change
                        def save_query():
                            app.storage.user['search_query'] = query_input.value or ''
                        query_input.on('blur', save_query)

                    # Mode Selector - includes variant levels when not using slider
                    with ui.column().classes('gap-1'):
                        h3(tr('Mode'), classes='text-sm font-medium', style='color: var(--text-secondary);')

                        if use_slider:
                            # Slider mode: single variants option, level controlled by slider
                            mode_options = {
                                'exact': tr('Exact') + ' (=)',
                                'variants': tr('Variants') + ' (?)',
                                'fuzzy': tr('Fuzzy') + ' (~)',
                                'Regex': tr('Regex') + ' (/)',
                                'Shelfmark': tr('Shelfmark') + ' (#)',
                                'Title': tr('Title') + ' ($)',
                            }
                        else:
                            # Preset mode: separate variant levels in dropdown
                            mode_options = {
                                'exact': tr('Exact') + ' (=)',
                                'variants': tr('Variants Basic') + ' (?)',
                                'variants_extended': tr('Variants Extended') + ' (??)',
                                'variants_maximum': tr('Variants Maximum') + ' (???)',
                                'fuzzy': tr('Fuzzy') + ' (~)',
                                'Regex': tr('Regex') + ' (/)',
                                'Shelfmark': tr('Shelfmark') + ' (#)',
                                'Title': tr('Title') + ' ($)',
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

                if use_slider:
                    # Slider mode: show/hide slider row
                    if variant_slider_row:
                        variant_slider_row.set_visibility(is_variants)
                else:
                    # Preset mode: show/hide max changes column, update level based on mode
                    max_changes_col.set_visibility(is_variants)
                    if is_variants:
                        set_level(get_level_from_mode(mode))

                # Save mode to storage
                app.storage.user['search_mode'] = mode

            mode_select.on('update:model-value', on_mode_change)

        # === Progress Bar ===
        progress_container = ui.column().classes('w-full')
        with progress_container:
            with ui.linear_progress(0, show_value=False).props('stripe animate').classes('w-full opacity-0 my-2').style('height: 12px;') as progress_bar:
                ui.label().classes('absolute-center text-xs text-white').bind_text_from(
                    progress_bar, 'value', backward=lambda v: f'{round(v * 100)}%' if v > 0 else ''
                )
            status_label = ui.label('').classes('text-sm px-6 py-1 font-medium').style('color: var(--text-secondary);')

        # === Main Content Area (Splitter) ===
        with ui.splitter(value=35).classes('w-full flex-grow search-splitter') as splitter:

            # === LEFT: Results List ===
            with splitter.before:
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

                # Filters Panel (initially hidden)
                filters_panel = ui.column().classes('w-full px-4 py-3 gap-3').style(
                    'background: var(--bg-tertiary); border-bottom: 1px solid var(--border-light); display: none;'
                )
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

                    with ui.row().classes('gap-2'):
                        ui.button(tr('Apply Filters'), icon='check', on_click=lambda: apply_filters()).props(
                            'flat dense color=green size=sm'
                        )
                        ui.button(tr('Clear Filters'), icon='clear', on_click=lambda: clear_filters()).props(
                            'flat dense size=sm'
                        )

                results_container = ui.scroll_area().classes('w-full flex-grow results-scroll-area').style(
                    'background: var(--bg-secondary);'
                )

            # === RIGHT: Result Viewer ===
            with splitter.after:
                viewer_container = ui.column().classes('w-full h-full p-6').style('background: var(--bg-primary);')
                with viewer_container:
                    # Placeholder
                    with ui.column().classes('w-full h-full items-center justify-center'):
                        ui.icon('menu_book').classes('text-6xl').style('color: var(--text-muted);')
                        ui.label(tr('Select a result to view')).classes('mt-4').style('color: var(--text-muted);')

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
                // Fallback to old method
                const splitter = document.querySelector('.search-splitter');
                if (!splitter) return null;
                return splitter.querySelector('.q-scrollarea__container');
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
    ui.timer(1.0, setup_scroll_collapse, once=True)

    # === Helper Functions ===

    def prepend_to_query(prefix):
        current = query_input.value or ""
        if not current.startswith(prefix):
            query_input.set_value(prefix + current)

    # === Filtering Functions ===

    def toggle_filters():
        """Toggle visibility of filters panel."""
        current_display = filters_panel.style or ''
        if 'display: none' in current_display:
            filters_panel.style('background: var(--bg-tertiary); border-bottom: 1px solid var(--border-light);')
        else:
            filters_panel.style('background: var(--bg-tertiary); border-bottom: 1px solid var(--border-light); display: none;')

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

        render_results(filtered)
        results_count.text = f"{len(filtered)} / {len(search_state.results)} {tr('Results')}"
        ui.notify(f"{len(filtered)} {tr('results match filters')}", type='info')

    def clear_filters():
        """Clear all filters and show all results."""
        filter_shelfmark.value = ''
        filter_title.value = ''
        filter_snippet.value = ''

        if search_state.results:
            render_results(search_state.results)
            results_count.text = f"{len(search_state.results)} {tr('Results')}"
            ui.notify(tr('Filters cleared'), type='info')

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

        # Re-render to update checkboxes
        current_results = list(search_state.results)  # Keep current filtered view
        render_results(current_results)
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
        try:
            # Check if client still exists
            _ = progress_bar.client
        except (RuntimeError, Exception):
            # Client deleted, deactivate the timer
            if search_state.update_timer:
                search_state.update_timer.deactivate()
            return

        try:
            if search_state.is_running:
                progress_bar.classes(remove='opacity-0')
                progress_bar.value = search_state.progress
                status_label.text = search_state.status
                # Swap buttons: hide search, show stop (using style to avoid performance issues)
                search_btn.style('display: none;')
                stop_btn.style('display: inline-flex;')
            else:
                # Swap buttons: show search, hide stop
                search_btn.style('display: inline-flex;')
                stop_btn.style('display: none;')
                if search_state.progress >= 1.0:
                    progress_bar.value = 1.0
                    status_label.text = tr("Done. Found {} results.").format(len(search_state.results))
                    progress_bar.classes(add='opacity-0')
                else:
                    progress_bar.classes(add='opacity-0')
        except Exception:
            pass  # Client may have been deleted

    # Cancel any existing timer first to prevent duplicates
    if search_state.update_timer:
        search_state.update_timer.deactivate()
    search_state.update_timer = ui.timer(0.5, update_progress_ui)

    async def execute_search():
        query = query_input.value.strip() if query_input.value else ""
        if not query:
            return

        if not state.is_ready():
            ui.notify(tr("Engine not ready."), type='warning')
            return

        # Parse syntax shortcuts (Delegated to Core)
        clean_query = query
        mode_override, parsed_query = state.searcher.parse_query_syntax(query)

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
        search_state.results = []

        # Show loading spinner immediately
        render_results([])

        def progress_cb(current, total):
            # Check if search was cancelled
            if search_state.is_cancelled:
                raise InterruptedError("Search cancelled")
            if total > 0:
                search_state.progress = current / total
                search_state.status = f"{current} / {total}"

        # Get NOT filter words
        not_words = not_filter.value.split() if not_filter.value else []

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
                    return state.searcher.execute_search(
                        clean_query,
                        mode=mode,
                        gap=int(gap_input.value),
                        progress_callback=progress_cb,
                        exclude_words=not_words
                    )
            except Exception as e:
                print(f"Search Error: {e}")
                import traceback
                traceback.print_exc()
                return []

        results = await run.io_bound(run_core_search)

        # Batch lookup for transcription availability
        result_sys_ids = [
            r.get('display', {}).get('id')
            for r in results
            if r.get('display', {}).get('id')
        ]
        search_state.transcription_sys_ids = await run.io_bound(
            get_sys_ids_with_transcriptions, result_sys_ids
        )

        # Check if search was cancelled before resetting
        was_cancelled = search_state.is_cancelled

        # Save results
        state.last_results = results
        search_state.is_running = False
        search_state.is_cancelled = False  # Reset flag
        search_state.progress = 1.0
        search_state.results = results

        try:
            app.storage.user['search_results'] = results
        except Exception:
            pass

        # Show message if results are partial (search was cancelled)
        if was_cancelled:
            search_state.status = tr('Partial results (search stopped)')
            ui.notify(tr('Showing partial results'), type='warning', timeout=3000)
            results_count.text = f"{len(results)} {tr('Results')} ({tr('partial')})"
        else:
            # Update count
            results_count.text = f"{len(results)} {tr('Results')}"

        # Render results
        render_results(results[:200])
        if len(results) > 200:
            ui.notify(tr("Showing first 200 results. Refine search."), type='info')

    def render_results(results):
        results_container.clear()

        # Show loading spinner when search is running - prominent so user knows it's working
        if search_state.is_running:
            with results_container:
                with ui.column().classes('w-full h-64 items-center justify-center'):
                    ui.spinner('bars', size='xl', color='primary').classes('mb-4')
                    ui.label(tr("Searching...")).classes('text-xl font-bold animate-pulse').style('color: var(--primary-600);')
            return

        if not results:
            with results_container:
                with ui.column().classes('w-full h-64 items-center justify-center'):
                    ui.icon('search').classes('text-5xl').style('color: var(--text-muted);')
                    ui.label(tr("Ready to search.")).classes('mt-4').style('color: var(--text-muted);')
            return

        with results_container:
            with ui.column().classes('w-full gap-2 p-4'):
                for i, res in enumerate(results):
                    create_result_card(i, res)

    def create_result_card(index, result):
        display = result.get('display', {})
        shelfmark = display.get('shelfmark', 'Unknown')
        title = display.get('title', '')
        snippet = result.get('snippet', '')
        library_code = display.get('library_code', '')

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

                # Main content (clickable)
                with ui.column().classes('flex-grow min-w-0 gap-1').on('click', lambda r=result: load_in_viewer(r)):
                    with ui.row().classes('items-center gap-2 flex-wrap'):
                        ui.label(f"#{index + 1}").classes('text-xs px-2 py-0.5 rounded shrink-0').style(
                            'background: var(--bg-tertiary); color: var(--text-muted);'
                        )
                        # Library badge (if available)
                        if library_code:
                            from genizah_core import get_library_display, LIBRARY_CODES
                            full_name = get_library_display(library_code, short=False)
                            ui.label(library_code).classes('text-xs px-2 py-0.5 rounded shrink-0').style(
                                'background: var(--primary-100); color: var(--primary-700);'
                            ).tooltip(full_name)
                        # PGP transcription indicator
                        sys_id = display.get('id')
                        if sys_id and sys_id in search_state.transcription_sys_ids:
                            ui.icon('description').classes('text-sm').style(
                                'color: var(--success-600);'
                            ).tooltip(tr('Has PGP Transcription'))
                        ui.label(shelfmark).classes('font-bold break-all').style('color: var(--primary-700);')
                    if title_short:
                        ui.label(title_short).classes('text-xs').style(
                            'color: var(--text-tertiary); direction: rtl; word-wrap: break-word;'
                        )

                # Actions
                with ui.row().classes('gap-1'):
                    ui.button(
                        icon='open_in_full',
                        on_click=lambda idx=index, r=result: open_advanced_dialog(idx, r)
                    ).props('flat round dense size=sm').tooltip(tr('Advanced View'))

                    def make_star_handler(r):
                        def handler():
                            show_add_to_list_dialog_local(r)
                        return handler
                    # Check if item is in any list
                    result_sys_id = result.get('display', {}).get('id')
                    result_in_list = state.lists_mgr and result_sys_id and state.lists_mgr.is_item_in_any_list(result_sys_id)
                    ui.button(
                        icon='star' if result_in_list else 'star_border',
                        on_click=make_star_handler(result)
                    ).props('flat round dense size=sm').style('color: var(--accent-amber);').tooltip(tr('In List') if result_in_list else tr('Add to List'))

            # Snippet
            if snippet:
                snippet_html = SearchEngine.format_snippet(snippet)
                with ui.element('div').classes('mt-3 p-3 rounded-lg text-sm').style(
                    'background: var(--bg-tertiary); direction: rtl; text-align: right; line-height: 1.8;'
                ):
                    ui.html(snippet_html, sanitize=False)

            # Mobile expansion (hidden on desktop via CSS)
            with ui.expansion(tr('View Full Text')).classes('w-full mt-2 result-mobile-expand').props('dense') as mobile_expand:
                mobile_content = ui.column().classes('w-full gap-3')

                async def load_mobile_content():
                    """Load full text content for mobile view."""
                    mobile_content.clear()
                    with mobile_content:
                        full_text = result.get('full_text', '')
                        if not full_text:
                            # Try to load full text
                            sys_id = display.get('id', '')
                            page_num = int(display.get('img', '1'))
                            if sys_id and state.meta_mgr:
                                try:
                                    page_data = await run.io_bound(
                                        state.meta_mgr.get_page_data, sys_id, page_num
                                    )
                                    if page_data:
                                        full_text = page_data.text or ''
                                        result['full_text'] = full_text
                                except Exception:
                                    pass

                        if full_text:
                            ui.label(full_text).classes('text-sm whitespace-pre-wrap').style(
                                'direction: rtl; text-align: right; line-height: 2; color: var(--text-primary);'
                            )
                        else:
                            ui.label(tr('Full text not available')).style('color: var(--text-muted);')

                        # Actions row
                        with ui.row().classes('w-full gap-2 mt-3'):
                            # Use ui.link for full page reload (not SPA navigation)
                            # This ensures browse page is fully recreated with fresh PGP data
                            browse_url = f'/browse?sys_id={display.get("id", "")}&page={display.get("img", "1")}'
                            with ui.link(target=browse_url).classes('no-underline'):
                                ui.button(tr('Open in Viewer'), icon='open_in_new').props('flat dense')

                mobile_expand.on('show', load_mobile_content)

    def open_advanced_dialog(index, result):
        """Open an enhanced Advanced View dialog with in-place navigation and IIIF image viewer."""
        service = get_service()
        adv_state = AdvancedViewState()
        adv_state.current_result_idx = index
        adv_state.results = search_state.results

        with ui.dialog().props('maximized') as dialog:
            with ui.card().classes('w-full h-full flex flex-col').style('background: var(--bg-secondary);'):
                # === Header Bar ===
                adv_state.header_container = ui.row().classes('w-full px-4 py-3 items-center justify-between shrink-0').style(
                    'background: var(--bg-header); color: white;'
                )
                with adv_state.header_container:
                    # Left: Close and Title
                    with ui.row().classes('items-center gap-3'):
                        ui.button(icon='close', on_click=dialog.close).props('flat round color=white size=sm')
                        adv_state.result_label = ui.label(
                            f"{tr('Result')} {index + 1} / {len(search_state.results)}"
                        ).classes('text-sm font-medium')

                    # Center: Score badge (will be updated in-place)
                    adv_state.score_badge = ui.element('div').classes('flex items-center gap-2')

                    # Right: Navigation and Fullscreen
                    with ui.row().classes('items-center gap-2'):
                        # Navigation Buttons
                        adv_state.prev_btn = ui.button(
                            icon='chevron_right' if is_rtl() else 'chevron_left',
                            on_click=lambda: navigate_result(-1)
                        ).props('flat round color=white size=sm').tooltip(tr('Previous'))

                        adv_state.next_btn = ui.button(
                            icon='chevron_left' if is_rtl() else 'chevron_right',
                            on_click=lambda: navigate_result(1)
                        ).props('flat round color=white size=sm').tooltip(tr('Next'))

                        ui.separator().props('vertical').classes('mx-1 h-4 bg-gray-400')

                        # Fullscreen toggle
                        def toggle_fullscreen():
                            adv_state.is_fullscreen = not adv_state.is_fullscreen
                            render_content(adv_state.results[adv_state.current_result_idx])

                        ui.button(
                            icon='fullscreen',
                            on_click=toggle_fullscreen
                        ).props('flat round color=white size=sm').tooltip(tr('Fullscreen'))

                # === Main Content (refreshable container) ===
                with ui.scroll_area().classes('flex-grow'):
                    adv_state.content_container = ui.column().classes('w-full max-w-6xl mx-auto p-6 gap-6')

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

                render_content(result)

            ui.timer(0, fetch_and_render, once=True)

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
                                    src['content'] = get_section_for_page(src['content'], current_p_num)
                            page_sources.append(src)
                    all_sources = page_sources if page_sources else None

                    pgp_doc = get_document_for_fragment(sys_id, current_p_num)
                    if pgp_doc:
                        pgpid = pgp_doc.get('pgpid')
                        doc_relation = pgp_doc.get('doc_relation', '')
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
                    print(f"Advanced View: Failed to fetch PGP transcription: {pgp_err}")

            # Extract FL ID
            fl_id = adv_state.current_fl_id
            if not fl_id and 'raw_header' in result and state.meta_mgr:
                try:
                    parsed = state.meta_mgr.parse_full_id_components(result['raw_header'])
                    fl_id = parsed.get('fl_id')
                except Exception:
                    pass

            # Determine if Oxford manuscript
            shelfmark_lower = (shelfmark or '').lower()
            is_oxford = shelfmark_lower.startswith('ms heb') or shelfmark_lower.startswith('ms. heb')

            # Compute image URL
            has_image = bool(sys_id)
            page_idx = max(0, current_p_num - 1)
            if is_oxford and sys_id:
                img_url = f"/api/oxford_image/{sys_id}?page={page_idx}"
            elif sys_id:
                img_url = f"/api/nli_image_by_sysid/{sys_id}?page={page_idx}"
            else:
                img_url = None

            # Get library display name
            library_name = ''
            if library_code:
                library_name = get_library_display(library_code, short=False)
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
                highlighted_text = display_text
                for term in highlighted_terms:
                    if term in highlighted_text:
                        highlighted_text = highlighted_text.replace(
                            term,
                            f'<mark style="background-color: #fef08a; padding: 2px 4px; border-radius: 3px; font-weight: 600;">{term}</mark>'
                        )
                text_html = highlighted_text.replace('\n', '<br>')
            else:
                text_html = display_text.replace('\n', '<br>') if display_text else ''

            with adv_state.content_container:

                # ============================================================
                # FULLSCREEN MODE - Compact layout with text and image only
                # ============================================================
                if adv_state.is_fullscreen:
                    # Compact info bar
                    with ui.row().classes('w-full items-center justify-between p-2 mb-2 rounded-lg').style(
                        'background: var(--bg-tertiary);'
                    ):
                        # Left: Shelfmark and page info
                        with ui.row().classes('items-center gap-3'):
                            ui.label(display_shelfmark).classes('font-bold text-sm').style('color: var(--primary-700);')
                            if title:
                                ui.label(f"| {title[:50]}{'...' if len(title) > 50 else ''}").classes('text-xs').style(
                                    'color: var(--text-muted); direction: rtl;'
                                )

                        # Center: Page navigation
                        with ui.row().classes('items-center gap-2'):
                            if total_pages > 1:
                                prev_pg_btn = ui.button(
                                    icon='chevron_right' if is_rtl() else 'chevron_left',
                                    on_click=lambda: ui.timer(0, lambda: load_page(direction=-1), once=True)
                                ).props('flat round size=sm').tooltip(tr('Previous Page'))
                                prev_pg_btn.set_enabled(current_p_num > 1)

                                ui.label(f"{tr('Page')} {current_p_num}/{total_pages}").classes('text-sm font-medium')

                                next_pg_btn = ui.button(
                                    icon='chevron_left' if is_rtl() else 'chevron_right',
                                    on_click=lambda: ui.timer(0, lambda: load_page(direction=1), once=True)
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

                                with ui.element('div').classes('adv-image-container w-full').style('height: calc(100% - 48px);'):
                                    img_html = f'''<img src="{safe_img_url}" class="adv-zoomable-image" style="transform: translate(0px, 0px) rotate(0deg) scale(1); cursor: grab; max-height: 100%;" loading="lazy" draggable="false" onload="if(window.advViewer) window.advViewer.init()" onerror="advHandleImageError(this, '{safe_sys_id}', {page_idx}, {is_oxford_js})"/>'''
                                    ui.html(img_html, sanitize=False)
                                    ui.run_javascript('setTimeout(() => { if(window.advViewer) window.advViewer.init(); }, 200);')

                    return  # Exit early for fullscreen mode

                # ============================================================
                # NORMAL MODE - Full layout with hero, text, image, actions
                # ============================================================

                # === Hero Section ===
                with ui.card().classes('w-full overflow-hidden').style('border-radius: 16px; border: none;'):
                    ui.element('div').classes('w-full h-2').style(
                        'background: linear-gradient(90deg, var(--primary-600), var(--primary-400), var(--accent-gold));'
                    )
                    with ui.column().classes('p-6 gap-4'):
                        with ui.row().classes('items-start justify-between w-full'):
                            with ui.column().classes('gap-2 flex-grow'):
                                h1(display_shelfmark, classes='text-3xl font-bold', style='color: var(--primary-700);')
                                if title:
                                    ui.label(title).classes('text-lg').style(
                                        'color: var(--text-secondary); direction: rtl; text-align: right;'
                                    )

                            with ui.row().classes('gap-2 shrink-0'):
                                if sys_id:
                                    browse_url = f'/browse?sys_id={sys_id}'
                                    if fl_id:
                                        browse_url += f'&fl_id={fl_id}'
                                    # Use ui.link for full page reload to ensure browse page recreates with PGP data
                                    with ui.link(target=browse_url).classes('no-underline').tooltip(tr('Browse Full Manuscript')):
                                        ui.button(icon='menu_book').props('round color=green')

                                def make_add_handler(r):
                                    def handler():
                                        show_add_to_list_dialog_local(r)
                                    return handler
                                # Check if item is in any list
                                adv_result_sys_id = result.get('display', {}).get('id')
                                adv_result_in_list = state.lists_mgr and adv_result_sys_id and state.lists_mgr.is_item_in_any_list(adv_result_sys_id)
                                ui.button(icon='star' if adv_result_in_list else 'star_border', on_click=make_add_handler(result)).props(
                                    'round'
                                ).style('color: var(--accent-amber);').tooltip(tr('In List') if adv_result_in_list else tr('Add to List'))

                                # Image toggle button
                                if has_image:
                                    def toggle_image():
                                        adv_state.show_image_panel = not adv_state.show_image_panel
                                        render_content(result)
                                    ui.button(
                                        icon='image' if adv_state.show_image_panel else 'hide_image',
                                        on_click=toggle_image
                                    ).props('round').tooltip(
                                        tr('Hide Image') if adv_state.show_image_panel else tr('Show Image')
                                    )

                        # Info chips
                        with ui.row().classes('gap-3 flex-wrap mt-2'):
                            if source:
                                with ui.element('div').classes('flex items-center gap-1 px-3 py-1 rounded-full').style(
                                    'background: var(--primary-100); color: var(--primary-700);'
                                ):
                                    ui.icon('source').classes('text-sm')
                                    ui.label(source).classes('text-sm font-medium')

                            with ui.element('div').classes('flex items-center gap-1 px-3 py-1 rounded-full').style(
                                'background: var(--accent-blue); color: white;'
                            ):
                                ui.icon('description').classes('text-sm')
                                ui.label(f"{tr('Page')} {current_p_num} / {total_pages}").classes('text-sm font-medium')

                            with ui.element('div').classes('flex items-center gap-1 px-3 py-1 rounded-full').style(
                                'background: var(--bg-tertiary); color: var(--text-secondary);'
                            ):
                                ui.icon('tag').classes('text-sm')
                                ui.label(f"#{adv_state.current_result_idx + 1}").classes('text-sm font-medium')

                # === Page Navigation Bar ===
                if total_pages > 1:
                    with ui.card().classes('w-full p-3').style('border-radius: 12px;'):
                        with ui.row().classes('items-center justify-center gap-3'):
                            prev_page_btn = ui.button(
                                icon='chevron_right' if is_rtl() else 'chevron_left',
                                on_click=lambda: ui.timer(0, lambda: load_page(direction=-1), once=True)
                            ).props('flat round').tooltip(tr('Previous Page'))
                            prev_page_btn.set_enabled(current_p_num > 1)

                            page_input = ui.number(value=current_p_num, min=1, max=total_pages).classes('w-16').props('dense outlined')
                            ui.label(f"/ {total_pages}").classes('text-sm').style('color: var(--text-secondary);')

                            def go_to_page():
                                try:
                                    p = int(page_input.value) if page_input.value else 1
                                    p = max(1, min(total_pages, p))
                                    ui.timer(0, lambda: load_page(p_num=p), once=True)
                                except (ValueError, TypeError):
                                    pass
                            ui.button(tr('Go'), on_click=go_to_page).props('flat dense color=green')

                            next_page_btn = ui.button(
                                icon='chevron_left' if is_rtl() else 'chevron_right',
                                on_click=lambda: ui.timer(0, lambda: load_page(direction=1), once=True)
                            ).props('flat round').tooltip(tr('Next Page'))
                            next_page_btn.set_enabled(current_p_num < total_pages)

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

                        def render_text_section(text_to_render: str):
                            """Render the text content (called on version change)."""
                            nonlocal text_content_container
                            if text_content_container is None:
                                return
                            text_content_container.clear()
                            with text_content_container:
                                with ui.scroll_area().classes('w-full').style('max-height: 60vh;'):
                                    with ui.element('div').classes('p-6').style(
                                        'direction: rtl; text-align: right; '
                                        'line-height: 2.4; font-size: 1.2rem; font-family: "SBL Hebrew", "David", serif;'
                                    ):
                                        html_content = text_to_render.replace('\n', '<br>') if text_to_render else ''
                                        ui.html(html_content, sanitize=False)
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
                                    # Header with page info and actions
                                    with ui.row().classes('items-center justify-between w-full p-4 border-b').style('border-color: var(--border-light);'):
                                        with ui.row().classes('items-center gap-3'):
                                            ui.icon('article').classes('text-2xl').style('color: var(--primary-600);')
                                            ui.label(f"{tr('Page')} {current_p_num}").classes('text-lg font-bold')
                                            word_count = len(display_text.split()) if display_text else 0
                                            ui.label(f"({word_count} {tr('words')})").classes('text-sm').style('color: var(--text-muted);')

                                        with ui.row().classes('gap-2'):
                                            ui.button(icon='content_copy', on_click=lambda t=display_text: copy_result_text(t)).props('flat round size=sm').tooltip(tr('Copy Text'))
                                            # Inline edit button
                                            if sys_id and current_text:
                                                ui.button(icon='edit', on_click=lambda: toggle_edit_mode(current_text)).props('flat round size=sm').tooltip(tr('Edit'))

                                    # Text content - create container (same scope as outer text_content_container)
                                    text_content_container = ui.element('div').classes('w-full')

                                    # Initial render
                                    render_text_section(display_text)

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
                                    render_text_section(new_text)
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

                                # Image display
                                safe_img_url = img_url.replace("'", "\\'").replace('"', '\\"')
                                safe_sys_id = (sys_id or '').replace("'", "\\'").replace('"', '\\"')
                                is_oxford_js = 'true' if is_oxford else 'false'

                                with ui.element('div').classes('adv-image-container w-full').style('height: 500px;'):
                                    img_html = f'''
                                    <img
                                        src="{safe_img_url}"
                                        class="adv-zoomable-image"
                                        style="transform: translate(0px, 0px) rotate(0deg) scale(1); cursor: grab; max-height: 100%;"
                                        loading="lazy"
                                        draggable="false"
                                        onload="if(window.advViewer) window.advViewer.init()"
                                        onerror="advHandleImageError(this, '{safe_sys_id}', {page_idx}, {is_oxford_js})"
                                    />
                                    '''
                                    ui.html(img_html, sanitize=False)
                                    ui.run_javascript('setTimeout(() => { if(window.advViewer) window.advViewer.init(); }, 200);')

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

    def load_in_viewer(result):
        search_state.selected_result = result
        viewer_container.clear()

        display = result.get('display', {})
        shelfmark = display.get('shelfmark', 'Unknown')
        title = display.get('title', '')
        sys_id = display.get('id', '')
        snippet = result.get('snippet', '')
        full_text = result.get('full_text', '')
        library_code = display.get('library_code', '')
        highlight_pattern = result.get('highlight_pattern', '')

        # Initialize page index from display.img if not already set
        try:
            search_state.current_page_idx = int(display.get('img', '1'))
        except (ValueError, TypeError):
            search_state.current_page_idx = 1

        with viewer_container:
            # Header
            with ui.column().classes('w-full gap-2 mb-4'):
                # Shelfmark with Library Name - H2
                display_shelfmark = shelfmark
                if library_code:
                    from genizah_core import get_library_display
                    library_name = get_library_display(library_code, short=False)
                    if library_name:
                        display_shelfmark = f"{library_name}, {shelfmark}"
                h2(display_shelfmark, classes='text-2xl font-bold', style='color: var(--primary-700);')
                if title:
                    ui.label(title).style('color: var(--text-secondary); direction: rtl;')

                # Metadata badges
                with ui.row().classes('gap-2 flex-wrap mt-2'):
                    if display.get('source'):
                        ui.badge(display['source']).props('outline')
                    if display.get('img'):
                        ui.badge(f"{tr('Page')} {display['img']}").props('outline')

            # Content tabs
            with ui.tabs().classes('w-full') as tabs:
                tab_snippet = ui.tab('snippet', label=tr('Match'))
                tab_full = ui.tab('full', label=tr('Full Text'))
                tab_info = ui.tab('info', label=tr('Metadata'))

            with ui.tab_panels(tabs, value='snippet').classes('w-full flex-grow'):
                # Match tab
                with ui.tab_panel('snippet'):
                    if snippet:
                        snippet_html = SearchEngine.format_snippet(snippet)
                        with ui.element('div').classes('p-4 rounded-lg').style(
                            'background: var(--bg-tertiary); direction: rtl; text-align: right; line-height: 2; font-size: 1.1rem;'
                        ):
                            ui.html(snippet_html, sanitize=False)

                # Full Text tab with navigation arrows
                with ui.tab_panel('full'):
                    # Navigation bar for browsing pages
                    if sys_id:
                        with ui.row().classes('w-full items-center justify-center gap-4 mb-4 p-2 rounded').style(
                            'background: var(--bg-tertiary);'
                        ):
                            ui.button(icon='chevron_right' if is_rtl() else 'chevron_left', on_click=browse_prev).props('flat round').tooltip(tr('Previous'))
                            page_label = ui.label(f"{tr('Page')} {search_state.current_page_idx}").style('color: var(--text-secondary);')
                            ui.button(icon='chevron_left' if is_rtl() else 'chevron_right', on_click=browse_next).props('flat round').tooltip(tr('Next'))

                    if full_text:
                        # Apply highlighting to full text using the highlight pattern
                        def highlight_full_text(text, pattern):
                            if not text:
                                return ""
                            escaped = html.escape(text)
                            if pattern:
                                try:
                                    # Apply case-insensitive highlighting
                                    highlighted = re.sub(
                                        f'({pattern})',
                                        r'<span class="highlight-match">\1</span>',
                                        escaped,
                                        flags=re.IGNORECASE
                                    )
                                    return highlighted
                                except re.error:
                                    return escaped
                            return escaped

                        full_text_html = highlight_full_text(full_text, highlight_pattern)
                        with ui.scroll_area().classes('w-full h-64'):
                            with ui.element('div').classes('whitespace-pre-wrap').style(
                                'direction: rtl; text-align: right; line-height: 2; font-size: 1rem; color: var(--text-primary);'
                            ):
                                ui.html(full_text_html, sanitize=False)
                    else:
                        ui.label(tr('Full text not available')).style('color: var(--text-muted);')

                # Info tab
                with ui.tab_panel('info'):
                    with ui.column().classes('w-full gap-4'):
                        # Get library full name
                        library_name = ''
                        if library_code:
                            from genizah_core import get_library_display
                            library_name = get_library_display(library_code, short=False)

                        info_items = [
                            (tr('Library'), library_name or tr('Not available')),
                            (tr('Shelfmark'), shelfmark),
                            (tr('Title'), title or tr('Not available')),
                            (tr('System ID'), sys_id or tr('Not available')),
                            (tr('Source'), display.get('source', tr('Not available'))),
                            (tr('Page'), display.get('img', tr('Not available'))),
                        ]
                        for label, value in info_items:
                            with ui.row().classes('w-full items-start gap-4'):
                                ui.label(label).classes('font-bold w-32').style('color: var(--text-secondary);')
                                ui.label(value).style('color: var(--text-primary); direction: rtl;')

            # Actions
            with ui.row().classes('w-full gap-3 mt-6 pt-6').style('border-top: 1px solid var(--border-light);'):
                if sys_id:
                    # Extract FL ID from result to jump to correct page
                    fl_id = None
                    if 'raw_header' in result and state.meta_mgr:
                        try:
                            parsed = state.meta_mgr.parse_full_id_components(result['raw_header'])
                            fl_id = parsed.get('fl_id')
                        except Exception:
                            pass

                    # Build browse URL with FL ID if available
                    browse_url = f'/browse?sys_id={sys_id}'
                    if fl_id:
                        browse_url += f'&fl_id={fl_id}'

                    # Use link styled as button for full page reload
                    with ui.link(target=browse_url).classes('btn-primary no-underline'):
                        ui.icon('menu_book').classes('mr-2')
                        ui.label(tr('Browse Full Manuscript'))

                # Find Parallels - pass the full text to parallels page
                text_for_parallels = full_text or snippet.replace('*', '')
                ui.button(
                    tr('Find Parallels'),
                    icon='compare_arrows',
                    on_click=lambda t=text_for_parallels: ui.navigate.to(f'/parallels?text={quote(t[:2000])}')
                ).props('outline')

    async def browse_prev():
        """Navigate to previous page in current manuscript (within viewer)."""
        if search_state.selected_result:
            display = search_state.selected_result.get('display', {})
            sys_id = display.get('id')
            current_page = search_state.current_page_idx

            if sys_id and current_page > 1:
                from web.services import get_service
                service = get_service()

                def load_prev():
                    return service.get_browse_page(sys_id, p_num=current_page, direction=-1)

                page_data = await run.io_bound(load_prev)
                if page_data:
                    search_state.current_page_idx = page_data.p_num
                    # Update the selected result with new page info
                    search_state.selected_result['full_text'] = page_data.text or ''
                    search_state.selected_result['display']['img'] = str(page_data.p_num)
                    load_in_viewer(search_state.selected_result)

    async def browse_next():
        """Navigate to next page in current manuscript (within viewer)."""
        if search_state.selected_result:
            display = search_state.selected_result.get('display', {})
            sys_id = display.get('id')
            current_page = search_state.current_page_idx

            if sys_id:
                from web.services import get_service
                service = get_service()

                def load_next():
                    return service.get_browse_page(sys_id, p_num=current_page, direction=1)

                page_data = await run.io_bound(load_next)
                if page_data:
                    search_state.current_page_idx = page_data.p_num
                    # Update the selected result with new page info
                    search_state.selected_result['full_text'] = page_data.text or ''
                    search_state.selected_result['display']['img'] = str(page_data.p_num)
                    load_in_viewer(search_state.selected_result)

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

    # Handle tag search
    if initial_tag:
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
                                    lambda r=result: ui.navigate.to(f'/browse?sys_id={r["sys_id"]}')
                                ):
                                    with ui.row().classes('items-center gap-2 flex-wrap'):
                                        ui.label(f"#{i + 1}").classes('text-xs px-2 py-0.5 rounded shrink-0').style(
                                            'background: var(--bg-tertiary); color: var(--text-muted);'
                                        )
                                        # PGP indicator
                                        ui.icon('description').classes('text-sm').style(
                                            'color: var(--success-600);'
                                        ).tooltip(tr('Has PGP Transcription'))
                                        ui.label(result.get('shelfmark', 'Unknown')).classes(
                                            'font-bold break-all'
                                        ).style('color: var(--primary-700);')

                                    # Document type
                                    if result.get('document_type'):
                                        ui.label(result['document_type']).classes('text-xs').style(
                                            'color: var(--text-tertiary);'
                                        )

                                    # Description snippet (truncated)
                                    desc = result.get('description', '') or ''
                                    if desc:
                                        truncated = (desc[:150] + '...') if len(desc) > 150 else desc
                                        ui.label(truncated).classes('text-xs mt-1').style(
                                            'color: var(--text-secondary); line-height: 1.4;'
                                        )

        ui.timer(0.1, load_tag_results, once=True)

    # Initialize with restored results or initial query
    elif initial_query:
        ui.timer(0.5, execute_search, once=True)
    elif search_state.results:
        results_count.text = f"{len(search_state.results)} {tr('Results')}"
        render_results(search_state.results[:200])
