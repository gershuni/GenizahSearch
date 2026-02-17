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
from genizah_core import SearchEngine, get_library_display, generate_tabular_syntax
from web.document_service import get_sys_ids_with_transcriptions, get_all_sources_for_fragment, get_document_for_fragment, get_section_for_page, get_fragments_by_tag, get_all_distinct_tags
from web.components.translate_button import create_translatable_text
from urllib.parse import quote
from typing import Optional, List, Dict, Any, Set
from dataclasses import dataclass, field
import re
import html




def create_search_page(initial_query: str = None, initial_tag: str = None,
                       initial_mode: str = None, initial_variants: int = None,
                       initial_ja: int = None, initial_flex_spaces: int = None,
                       initial_bidirectional: int = None, initial_domain: str = None):
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
            self.displayed_results = []  # Currently rendered subset (may be filtered)
            self.builder_negated_words: list = []  # Words negated via Query Builder
            self.result_domains: dict = {}  # Domain classification map for result indicators
            self.all_result_domains: dict = {}  # sys_id -> list of domain names (deduped)
            self.domain_exclusions: set = set()  # domain names user has excluded
            self.has_domain_data: bool = False  # whether any results have domain data
            self.domain_name_map: dict = {}  # English domain name -> Hebrew name
            self.catalog_record_counts: dict = {}  # sys_id -> int (FJMS catalog record counts)

    search_state = SearchUIState()

    # Restore domain exclusions from storage
    search_state.domain_exclusions = set(app.storage.user.get('domain_exclusions', []))

    # Clear exclusions if initial_domain provided (from browse page navigation)
    if initial_domain:
        search_state.domain_exclusions = set()
        app.storage.user['domain_exclusions'] = []

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
                img.src = `${NLI_IIIF_BASE}/FL${flIds[idx]}/full/2000,/0/default.jpg`;
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
                            value=initial_query or saved_query
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
                    ui.timer(0.1, load_pgp_tags, once=True)

                    # Mode Selector - includes variant levels when not using slider
                    with ui.column().classes('gap-1'):
                        h3(tr('Mode'), classes='text-sm font-medium', style='color: var(--text-secondary);')

                        # Highlight Responsa option for feature discovery (one-time)
                        _highlight_responsa = not app.storage.user.get('hint_responsa_seen')
                        _responsa_label = ('✨ ' if _highlight_responsa else '') + tr('Responsa') + ' (R)'

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

                        # Feature discovery glow on mode selector (one-time)
                        _show_hints = not app.storage.user.get('hint_responsa_seen')
                        if _show_hints and saved_mode != 'responsa':
                            mode_select.classes('feature-glow')
                            mode_select.style('position: relative;')
                            with mode_select:
                                ui.label(tr('Try the Responsa-project style search mode!')).classes('feature-hint')

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

                    # Tabular Search button (pushed to right side)
                    ui.space()
                    builder_btn = ui.button(tr('Tabular Search'), icon='grid_view',
                        on_click=lambda: open_query_builder()).classes('ml-auto').props('outline dense')

                    # Feature discovery glow on tabular button (one-time)
                    if not app.storage.user.get('hint_tabular_seen'):
                        builder_btn.classes('feature-glow')
                        builder_btn.style('position: relative;')
                        with builder_btn:
                            _tabular_hint = ui.label(tr('Try the Tabular Search!')).classes('feature-hint')

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

                # Dismiss mode selector glow when user selects Responsa
                if is_responsa and not app.storage.user.get('hint_responsa_seen'):
                    app.storage.user['hint_responsa_seen'] = True
                    mode_select._classes = [c for c in mode_select._classes if c != 'feature-glow']
                    mode_select.update()

                # Save mode to storage (don't persist pgp_tags)
                if not is_tags:
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
                        # Domain filter button (hidden until search with domain data)
                        domain_filter_btn = ui.button(
                            tr('Filter by domains'), icon='category',
                            on_click=lambda: _open_domain_filter_dialog()
                        ).classes('text-sm').props('outline dense no-caps')
                        domain_filter_btn.set_visibility(False)

                        # Restore visibility if stored exclusions exist (persistence across navigation)
                        if search_state.domain_exclusions:
                            domain_filter_btn.set_visibility(True)
                            n_excl = len(search_state.domain_exclusions)
                            domain_filter_btn.text = f"{tr('Filter by domains')} ({n_excl} {tr('excluded')})"
                            domain_filter_btn.props('outline dense no-caps color=red')

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

        render_results(filtered[:200])
        shown = min(len(filtered), 200)
        results_count.text = f"{shown} / {len(search_state.results)} {tr('Results')}"
        if len(filtered) > 200:
            ui.notify(f"{tr('Showing first 200 of')} {len(filtered)} {tr('filtered results')}", type='info')
        else:
            ui.notify(f"{len(filtered)} {tr('results match filters')}", type='info')

    def clear_filters():
        """Clear all filters and show all results."""
        filter_shelfmark.value = ''
        filter_title.value = ''
        filter_snippet.value = ''

        if search_state.results:
            render_results(search_state.results[:200])
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

        # Re-render to update checkboxes (capped to 200 for WebSocket safety)
        current_results = list(search_state.results[:200])
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

    def open_query_builder():
        """Open the tabular query builder dialog for composing Responsa queries visually."""
        # Dismiss tabular button glow on first use
        if not app.storage.user.get('hint_tabular_seen'):
            app.storage.user['hint_tabular_seen'] = True
            builder_btn._classes = [c for c in builder_btn._classes if c != 'feature-glow']
            builder_btn.update()

        # === Builder State ===
        _updating_modifiers = {'flag': False}  # Guard to prevent on_change loops when updating checkboxes

        def make_word(text=''):
            return {'text': text, 'mods': {
                'prefix': False, 'suffix': False,
                'wildcard_prefix': False, 'wildcard_suffix': False,
                'plene': False, 'negation': False
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
        }

        def _build_mod_indicator_text(mods):
            """Build a short string showing active modifiers for a word."""
            parts = []
            for key in ['prefix', 'suffix', 'wildcard_prefix', 'wildcard_suffix', 'plene', 'negation']:
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
            """Toggle scope and show/hide distance spinners."""
            builder_state['scope'] = value
            show_dists = (value == 'word_range')
            for ds in distance_spinners:
                ds.set_visibility(show_dists)
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
                {'word_range': tr('Word Range'), 'within_document': tr('Within Document')},
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

                mod_cbs['prefix'] = prefix_cb
                mod_cbs['suffix'] = suffix_cb
                mod_cbs['wildcard_prefix'] = wild_start_cb
                mod_cbs['wildcard_suffix'] = wild_end_cb
                mod_cbs['plene'] = plene_cb
                mod_cbs['negation'] = negation_cb

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
        """Open modal dialog with checkbox tree of domains from current results."""
        if not search_state.has_domain_data:
            if search_state.domain_exclusions:
                ui.notify(tr('Run a search first to see domain options.'), type='info', timeout=3000)
            return

        # Build domain hierarchy from all_result_domains
        # We need parent/child info - re-fetch raw domain dicts for hierarchy building
        from shared.fjms_service import get_fjms_service
        fjms = get_fjms_service(thread_safe=True)
        hierarchy = fjms.get_domain_hierarchy() if fjms.is_available() else {}

        # Count results per domain from all_result_domains
        domain_counts = {}  # domain_name -> count of results
        for sys_id, domain_names in search_state.all_result_domains.items():
            for d in domain_names:
                domain_counts[d] = domain_counts.get(d, 0) + 1

        # Build filtered hierarchy: only domains present in current results
        result_hierarchy = {}  # parent_name -> {children: [{domain, count}], count}
        for parent_name, info in hierarchy.items():
            parent_in_results = parent_name in domain_counts
            children_in_results = []
            for child in info.get('children', []):
                if child['domain'] in domain_counts:
                    children_in_results.append({
                        'domain': child['domain'],
                        'domain_heb': child.get('domain_heb', child['domain']),
                        'count': domain_counts[child['domain']],
                    })
            if parent_in_results or children_in_results:
                parent_count = domain_counts.get(parent_name, 0)
                # If parent has children in results, sum their counts for the parent total
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

        # Track checkbox states: domain_name -> ui.checkbox reference
        checkboxes = {}
        current_exclusions = search_state.domain_exclusions.copy()

        with ui.dialog() as dialog, ui.card().classes('w-[600px] max-h-[80vh]'):
            with ui.column().classes('w-full gap-2'):
                ui.label(tr('Filter by Domain')).classes('text-lg font-bold')

                # Summary line
                def calc_visible():
                    excluded = {name for name, cb in checkboxes.items() if not cb.value}
                    hide_uncategorized = 'Uncategorized' in excluded
                    visible = 0
                    for r in search_state.results:
                        sys_id = r.get('display', {}).get('id')
                        doms = search_state.all_result_domains.get(sys_id, [])
                        if not doms:
                            if not hide_uncategorized:
                                visible += 1
                        elif not all(d in excluded for d in doms):
                            visible += 1
                    return visible

                summary_label = ui.label(f"{tr('Showing')} {total_results} {tr('of')} {total_results} {tr('results')}").classes('text-sm text-gray-500')

                # Scrollable checkbox tree
                with ui.scroll_area().classes('w-full').style('max-height: 50vh;'):
                    with ui.column().classes('w-full gap-0'):
                        for parent_name, info in sorted(result_hierarchy.items(), key=lambda x: -x[1]['count']):
                            children = info.get('children', [])

                            # Parent checkbox
                            def make_parent_handler(pname, child_domains):
                                def handler(e):
                                    for cd in child_domains:
                                        if cd in checkboxes:
                                            checkboxes[cd].value = e.value
                                    visible = calc_visible()
                                    summary_label.text = f"{tr('Showing')} {visible} {tr('of')} {total_results} {tr('results')}"
                                return handler

                            child_domain_names = [c['domain'] for c in children]
                            parent_checked = parent_name not in current_exclusions
                            parent_label = f"{_domain_display_name(parent_name)} ({info['count']})"

                            parent_cb = ui.checkbox(parent_label, value=parent_checked).classes('font-bold')
                            checkboxes[parent_name] = parent_cb
                            parent_cb.on_value_change(make_parent_handler(parent_name, child_domain_names))

                            # Children checkboxes (indented)
                            for child in sorted(children, key=lambda c: -c['count']):
                                child_checked = child['domain'] not in current_exclusions
                                child_label = f"{_domain_display_name(child['domain'])} ({child['count']})"

                                def make_child_handler():
                                    def handler(e):
                                        visible = calc_visible()
                                        summary_label.text = f"{tr('Showing')} {visible} {tr('of')} {total_results} {tr('results')}"
                                    return handler

                                child_cb = ui.checkbox(child_label, value=child_checked).style('margin-inline-start: 2rem')
                                checkboxes[child['domain']] = child_cb
                                child_cb.on_value_change(make_child_handler())

                # Buttons
                with ui.row().classes('w-full justify-between'):
                    def check_all():
                        for cb in checkboxes.values():
                            cb.value = True
                        for cb in checkboxes.values():
                            cb.update()
                        summary_label.text = f"{tr('Showing')} {total_results} {tr('of')} {total_results} {tr('results')}"

                    def uncheck_all():
                        for cb in checkboxes.values():
                            cb.value = False
                        for cb in checkboxes.values():
                            cb.update()
                        visible = calc_visible()
                        summary_label.text = f"{tr('Showing')} {visible} {tr('of')} {total_results} {tr('results')}"

                    with ui.row().classes('gap-2'):
                        ui.button(tr('Select All'), on_click=check_all).props('flat dense no-caps')
                        ui.button(tr('Select None'), on_click=uncheck_all).props('flat dense no-caps')

                    with ui.row().classes('gap-2'):
                        def apply_filter():
                            excluded = {name for name, cb in checkboxes.items() if not cb.value}
                            search_state.domain_exclusions = excluded
                            # Persist to storage
                            app.storage.user['domain_exclusions'] = list(excluded)
                            _apply_domain_exclusions()
                            # Update button text and styling
                            _update_domain_filter_btn()
                            dialog.close()

                        def cancel_dialog():
                            dialog.close()

                        ui.button(tr('Apply'), on_click=apply_filter).props('dense no-caps color=primary')
                        ui.button(tr('Cancel'), on_click=cancel_dialog).props('flat dense no-caps')

        dialog.open()

    def _apply_domain_exclusions():
        """Filter displayed results based on domain exclusions without re-searching."""
        if not search_state.domain_exclusions:
            # No exclusions -- show all results
            filtered = search_state.results
        else:
            hide_uncategorized = 'Uncategorized' in search_state.domain_exclusions
            filtered = []
            for r in search_state.results:
                sys_id = r.get('display', {}).get('id')
                result_domains = search_state.all_result_domains.get(sys_id, []) if sys_id else []
                if not result_domains:
                    # No domain data -- hide if Uncategorized is excluded
                    if not hide_uncategorized:
                        filtered.append(r)
                    continue
                elif all(d in search_state.domain_exclusions for d in result_domains):
                    # ALL domains excluded -- hide this result
                    continue
                else:
                    # At least one domain not excluded -- keep
                    filtered.append(r)

        # Update count display
        total = len(search_state.results)
        showing = len(filtered)
        if search_state.domain_exclusions:
            results_count.text = f"{showing} {tr('of')} {total} {tr('Results')} ({len(search_state.domain_exclusions)} {tr('domains excluded')})"
        else:
            results_count.text = f"{total} {tr('Results')}"

        # Update result_domains for badge rendering (slice from all_result_domains for displayed results)
        result_sys_ids = [r.get('display', {}).get('id') for r in filtered[:200] if r.get('display', {}).get('id')]
        search_state.result_domains = {sid: doms for sid, doms in search_state.all_result_domains.items() if sid in set(result_sys_ids)}

        # Re-render with filtered results
        render_results(filtered[:200])

    async def execute_search():
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
                        exclude_words=not_words,
                        responsa_options=responsa_options
                    )
            except ValueError as e:
                # Explosion guard or other validation error — surface to user
                error_msg = str(e)
                print(f"Search Validation Error: {error_msg}")
                return {'error': error_msg}
            except Exception as e:
                print(f"Search Error: {e}")
                import traceback
                traceback.print_exc()
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

        # Collect domain data for ALL results (not just displayed 200)
        all_sys_ids = [r.get('display', {}).get('id') for r in results if r.get('display', {}).get('id')]

        def collect_all_domains(sys_ids):
            from shared.fjms_service import get_fjms_service
            fjms = get_fjms_service(thread_safe=True)
            return fjms.get_domains_for_sys_ids(sys_ids) if fjms.is_available() else {}

        raw_domains = await run.io_bound(collect_all_domains, all_sys_ids)

        # Process into deduplicated domain names per sys_id
        # Also build English->Hebrew display name map
        search_state.all_result_domains = {}
        search_state.catalog_record_counts = {}
        search_state.domain_name_map = {}  # English name -> Hebrew name
        for sys_id, doms in raw_domains.items():
            child_names = {d['domain'] for d in doms}
            filtered = [d['domain'] for d in doms if not (d.get('parent_domain') and d['parent_domain'] in child_names and d['parent_domain'] != d['domain'])]
            if filtered:
                search_state.all_result_domains[sys_id] = filtered
            for d in doms:
                if d.get('domain_heb') and d['domain'] not in search_state.domain_name_map:
                    search_state.domain_name_map[d['domain']] = d['domain_heb']
                if d.get('parent_domain_heb') and d.get('parent_domain') and d['parent_domain'] not in search_state.domain_name_map:
                    search_state.domain_name_map[d['parent_domain']] = d['parent_domain_heb']
        search_state.has_domain_data = bool(search_state.all_result_domains)

        # Batch fetch catalog record counts for search result card buttons
        def collect_catalog_counts(sys_ids):
            from shared.fjms_service import get_fjms_service
            fjms = get_fjms_service(thread_safe=True)
            return fjms.get_catalog_record_counts(sys_ids) if fjms.is_available() else {}

        search_state.catalog_record_counts = await run.io_bound(collect_catalog_counts, all_sys_ids)

        # Batch lookup for transcription availability
        result_sys_ids = [
            r.get('display', {}).get('id')
            for r in results[:200]
            if r.get('display', {}).get('id')
        ]

        search_state.transcription_sys_ids = await run.io_bound(
            get_sys_ids_with_transcriptions, result_sys_ids
        )

        # Slice result_domains from all_result_domains for badge rendering
        search_state.result_domains = {sid: doms for sid, doms in search_state.all_result_domains.items() if sid in set(result_sys_ids)}

        # Show/hide domain filter button and update styling
        domain_filter_btn.set_visibility(search_state.has_domain_data)
        _update_domain_filter_btn()

        # Check if search was cancelled before resetting
        was_cancelled = search_state.is_cancelled

        # Save results
        state.last_results = results
        search_state.is_running = False
        search_state.is_cancelled = False  # Reset flag
        search_state.progress = 1.0
        search_state.results = results

        try:
            # Cap stored results to prevent WebSocket overload (18K+ results = 50-100MB JSON)
            capped = results[:200]
            # Strip full_text from stored results (only needed for display, not persistence)
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

        # Show message if results are partial (search was cancelled)
        if was_cancelled:
            search_state.status = tr('Partial results (search stopped)')
            ui.notify(tr('Showing partial results'), type='warning', timeout=3000)
            results_count.text = f"{len(results)} {tr('Results')} ({tr('partial')})"
        else:
            # Update count -- include expanded term count for Responsa mode
            expanded_count = results[0].get('responsa_expanded_count', 0) if results else 0
            if expanded_count > 0:
                results_count.text = f"{len(results)} {tr('Results')} ({tr('searching')} {expanded_count} {tr('expanded terms')})"
            else:
                results_count.text = f"{len(results)} {tr('Results')}"

        # --- Responsa explosion guard warning ---
        if results and results[0].get('responsa_warning'):
            ui.notify(results[0]['responsa_warning'], type='warning', timeout=5000)

        # --- URL state persistence (history.replaceState without page reload) ---
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
            pass  # URL update is best-effort

        # Render results (apply remembered exclusions if active)
        if search_state.domain_exclusions and search_state.has_domain_data:
            # Apply remembered exclusions before first render
            hide_uncategorized = 'Uncategorized' in search_state.domain_exclusions
            filtered = []
            for r in results:
                sys_id = r.get('display', {}).get('id')
                result_domains = search_state.all_result_domains.get(sys_id, []) if sys_id else []
                if not result_domains:
                    if not hide_uncategorized:
                        filtered.append(r)
                elif not all(d in search_state.domain_exclusions for d in result_domains):
                    filtered.append(r)
            n_excl = len(search_state.domain_exclusions)
            results_count.text = f"{len(filtered)} {tr('of')} {len(results)} {tr('Results')} ({n_excl} {tr('domains excluded')})"
            # Update result_domains for badge rendering
            result_sys_ids = [r.get('display', {}).get('id') for r in filtered[:200] if r.get('display', {}).get('id')]
            search_state.result_domains = {sid: doms for sid, doms in search_state.all_result_domains.items() if sid in set(result_sys_ids)}
            render_results(filtered[:200])
            if len(filtered) > 200:
                ui.notify(tr("Showing first 200 results. Refine search."), type='info')
        else:
            render_results(results[:200])
            if len(results) > 200:
                ui.notify(tr("Showing first 200 results. Refine search."), type='info')

    def render_results(results):
        results_container.clear()
        search_state.displayed_results = results  # Track current view for Advanced View navigation

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
                            ui.label('PGP').classes('text-xs px-2 py-0.5 rounded shrink-0').style(
                                'background: var(--success-100); color: var(--success-700); font-weight: 600;'
                            ).tooltip(tr('Has PGP Transcription'))
                        # Domain indicator
                        if sys_id and search_state.result_domains:
                            domains_for_result = search_state.result_domains.get(sys_id, [])
                            if domains_for_result:
                                primary_domain = domains_for_result[0]  # Most specific (child)
                                domain_text = _domain_display_name(primary_domain)
                                if len(domains_for_result) > 1:
                                    extra = len(domains_for_result) - 1
                                    tooltip_text = ', '.join(_domain_display_name(d) for d in domains_for_result)
                                    with ui.row().classes('items-center gap-0'):
                                        ui.label(domain_text).classes('text-xs px-2 py-0.5 rounded shrink-0').style(
                                            'background: #f3e8ff; color: #7c3aed;'  # Purple tones for FJMS
                                        )
                                        ui.label(f'+{extra}').classes('text-xs px-1 py-0.5 rounded shrink-0 cursor-help').style(
                                            'background: #ede9fe; color: #7c3aed;'
                                        ).tooltip(tooltip_text)
                                else:
                                    ui.label(domain_text).classes('text-xs px-2 py-0.5 rounded shrink-0').style(
                                        'background: #f3e8ff; color: #7c3aed;'
                                    )
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
        adv_state.results = search_state.displayed_results

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
                            f"{tr('Result')} {index + 1} / {len(adv_state.results)}"
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

                # === PGP Metadata Section ===
                if pgp_metadata:
                    with ui.card().classes('w-full p-4').style('border-radius: 12px; border-left: 3px solid #27ae60;'):
                        with ui.row().classes('items-center gap-2 mb-2'):
                            h4(tr('Princeton Geniza Project'), classes='text-xs font-bold', style='color: var(--text-secondary);')
                            if pgp_metadata.get('pgp_url'):
                                ui.link('', pgp_metadata['pgp_url'], new_tab=True).props(
                                    'icon=open_in_new flat dense round size=xs'
                                ).style('color: var(--primary-600);').tooltip(tr('View on PGP'))

                        with ui.row().classes('gap-6 flex-wrap'):
                            # Document Type + Languages
                            doc_type = pgp_metadata.get('document_type')
                            lang_primary = pgp_metadata.get('languages_primary')
                            if doc_type or lang_primary:
                                with ui.column().classes('gap-1'):
                                    ui.label(tr('Document Type')).classes('text-xs font-bold').style('color: var(--text-secondary);')
                                    type_parts = [p for p in [doc_type, lang_primary, pgp_metadata.get('languages_secondary')] if p]
                                    create_translatable_text(' \u00b7 '.join(type_parts), container_style='color: var(--text-primary);')

                            # Dates
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

                        # Tags
                        tags = pgp_metadata.get('tags', [])
                        if tags:
                            with ui.column().classes('gap-1 mt-2'):
                                ui.label(tr('Tags')).classes('text-xs font-bold').style('color: var(--text-secondary);')
                                with ui.row().classes('gap-1 flex-wrap'):
                                    for tag in tags:
                                        ui.badge(tag, color='green').props('outline clickable').classes(
                                            'text-xs cursor-pointer'
                                        ).on('click', lambda t=tag: (dialog.close(), ui.navigate.to(f'/search?tag={quote(t)}')))

                        # Description
                        description = (pgp_metadata.get('description') or '').strip()
                        if description:
                            with ui.column().classes('gap-1 mt-2'):
                                ui.label(tr('Description')).classes('text-xs font-bold').style('color: var(--text-secondary);')
                                create_translatable_text(description, container_style='color: var(--text-primary); white-space: pre-wrap;')

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
        async def load_tag_in_viewer(result):
            """Load a tag search result into the viewer pane with page text."""
            sys_id = result.get('sys_id', '')
            shelfmark = result.get('shelfmark', 'Unknown')
            doc_type = result.get('document_type', '')
            description = (result.get('description', '') or '').strip()
            pgpid = result.get('pgpid')

            # Look up library info
            library_name = ''
            library_code = ''
            if sys_id and hasattr(state, 'meta_mgr') and state.meta_mgr:
                library_code = state.meta_mgr.get_library_for_id(sys_id)
                if library_code:
                    from genizah_core import get_library_display
                    library_name = get_library_display(library_code, short=False)

            # Fetch first page text
            from web.services import get_service
            service = get_service()
            page_data = await run.io_bound(lambda: service.get_browse_page(sys_id, p_num=1))
            page_text = page_data.text if page_data else ''

            viewer_container.clear()
            with viewer_container:
                with ui.column().classes('w-full gap-2 mb-4'):
                    # Shelfmark header
                    display_shelfmark = f"{library_name}, {shelfmark}" if library_name else shelfmark
                    h2(display_shelfmark, classes='text-2xl font-bold', style='color: var(--primary-700);')

                    # Metadata badges
                    with ui.row().classes('gap-2 flex-wrap mt-2'):
                        if doc_type:
                            ui.badge(doc_type).props('outline')
                        ui.badge('PGP', color='green').props('outline')

                # Content tabs
                with ui.tabs().classes('w-full') as tabs:
                    tab_text = ui.tab('text', label=tr('Full Text'))
                    tab_info = ui.tab('info', label=tr('Metadata'))

                with ui.tab_panels(tabs, value='text').classes('w-full flex-grow'):
                    # Text tab
                    with ui.tab_panel('text'):
                        if page_text:
                            with ui.scroll_area().classes('w-full h-64'):
                                with ui.element('div').classes('whitespace-pre-wrap').style(
                                    'direction: rtl; text-align: right; line-height: 2; font-size: 1rem; color: var(--text-primary);'
                                ):
                                    ui.html(html.escape(page_text).replace('\n', '<br>'), sanitize=False)
                        else:
                            ui.label(tr('Full text not available')).style('color: var(--text-muted);')

                    # Info tab
                    with ui.tab_panel('info'):
                        with ui.column().classes('w-full gap-4'):
                            if description:
                                with ui.column().classes('gap-1'):
                                    ui.label(tr('Description')).classes('text-xs font-bold').style('color: var(--text-secondary);')
                                    create_translatable_text(description, container_style='color: var(--text-primary); white-space: pre-wrap; line-height: 1.6;')

                            info_items = [
                                (tr('Library'), library_name or tr('Not available')),
                                (tr('Shelfmark'), shelfmark),
                                (tr('System ID'), sys_id or tr('Not available')),
                            ]
                            if pgpid:
                                info_items.append(('PGP ID', str(pgpid)))

                            for label, value in info_items:
                                with ui.row().classes('w-full items-start gap-4'):
                                    ui.label(label).classes('font-bold w-32').style('color: var(--text-secondary);')
                                    ui.label(value).style('color: var(--text-primary);')

                            if pgpid:
                                pgp_url = f'https://geniza.princeton.edu/documents/{pgpid}'
                                ui.link(tr('View on PGP'), pgp_url, new_tab=True).classes('text-sm').style('color: var(--primary-600);')

                # Actions
                with ui.row().classes('w-full gap-3 mt-6 pt-6').style('border-top: 1px solid var(--border-light);'):
                    with ui.link(target=f'/browse?sys_id={sys_id}').classes('btn-primary no-underline'):
                        ui.icon('menu_book').classes('mr-2')
                        ui.label(tr('Browse Full Manuscript'))

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
                                    lambda r=result: load_tag_in_viewer(r)
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

                                    # Document type
                                    if result.get('document_type'):
                                        ui.label(result['document_type']).classes('text-xs').style(
                                            'color: var(--text-tertiary);'
                                        )

                                    # Description snippet (truncated, with translate)
                                    desc = result.get('description', '') or ''
                                    if desc:
                                        truncated = (desc[:150] + '...') if len(desc) > 150 else desc
                                        create_translatable_text(truncated, container_style='color: var(--text-secondary); line-height: 1.4; font-size: 0.75rem;')

        ui.timer(0.1, load_tag_results, once=True)

    # Initialize with restored results or initial query
    elif initial_query:
        ui.timer(0.5, execute_search, once=True)
    elif search_state.results:
        results_count.text = f"{len(search_state.results)} {tr('Results')}"
        render_results(search_state.results[:200])
