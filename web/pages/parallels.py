# -*- coding: utf-8 -*-
"""
Parallels Search Page - Dicta Genizah Search

Find parallel texts in the Genizah corpus using:
- Shmidman-Koppel-Porat fingerprinting algorithm
- Configurable chunk size
- Advanced filtering options
"""

from nicegui import ui, run, app
from web.state import state
from web.translations import tr
from urllib.parse import unquote
import re
import html
import os
import requests
from web.components.typography import h1, h2, h3, h4

# Import Sefaria sources and text cleaning from the shared sefaria_utils module (no PyQt6 dependency)
from sefaria_utils import SEFARIA_SOURCES, clean_hebrew_text, get_cache_dir, get_sefaria_library


def _sanitize_cache_filename(ref: str) -> str:
    """Sanitize a reference string to create a safe cache filename.

    Uses a whitelist approach: only alphanumeric characters, underscores, and hyphens
    are allowed. All other characters are replaced with underscores.
    This prevents path traversal attacks (e.g., ../ or ..\\ on Windows).
    """
    # Replace any character that is not alphanumeric, underscore, or hyphen
    return re.sub(r'[^a-zA-Z0-9_\-]', '_', ref)


def get_source_display_name(ref: str) -> str:
    """Get a display name for a source reference."""
    # Handle custom sources
    if ref.startswith('custom:'):
        parts = ref.split(':', 2)
        if len(parts) >= 3:
            return f"📝 {parts[2]}"  # Return the custom name
        return "📝 Custom Text"

    # Look up in predefined sources
    for source_type, source_data in SEFARIA_SOURCES.items():
        for book_key, book_data in source_data.get("books", {}).items():
            if ref in book_data.get("refs", []):
                idx = book_data["refs"].index(ref)
                return f"{source_data['name']} - {book_data['he_names'][idx]}"
    return ref


def flatten_sefaria_text(text_data):
    """Recursively flatten nested text arrays from Sefaria."""
    if isinstance(text_data, str):
        return re.sub(r'<[^>]+>', '', text_data)
    elif isinstance(text_data, list):
        parts = []
        for item in text_data:
            flattened = flatten_sefaria_text(item)
            if flattened:
                parts.append(flattened)
        return " ".join(parts)
    return ""


def fetch_sefaria_text(ref: str, use_cache: bool = True) -> str:
    """Fetch a single text from Sefaria API (cleaned, no nikud/taamim)."""
    cache_dir = get_cache_dir()
    # Use sanitized filename to prevent path traversal attacks
    safe_filename = _sanitize_cache_filename(ref)
    cache_file = os.path.join(cache_dir, f"{safe_filename}_v2.txt")

    if use_cache and os.path.exists(cache_file):
        try:
            with open(cache_file, 'r', encoding='utf-8') as f:
                text = f.read()
                if text:
                    return text
        except Exception:
            pass

    try:
        encoded_ref = ref.replace(' ', '%20')
        raw_text = ""

        # Determine if this is a Tanakh ref (for v3 "Text Only" version)
        is_tanakh = any(ref.startswith(book) for book in [
            'Genesis', 'Exodus', 'Leviticus', 'Numbers', 'Deuteronomy',
            'Joshua', 'Judges', 'I Samuel', 'II Samuel', 'I Kings', 'II Kings',
            'Isaiah', 'Jeremiah', 'Ezekiel', 'Hosea', 'Joel', 'Amos', 'Obadiah',
            'Jonah', 'Micah', 'Nahum', 'Habakkuk', 'Zephaniah', 'Haggai',
            'Zechariah', 'Malachi', 'Psalms', 'Proverbs', 'Job', 'Song of Songs',
            'Ruth', 'Lamentations', 'Ecclesiastes', 'Esther', 'Daniel',
            'Ezra', 'Nehemiah', 'I Chronicles', 'II Chronicles'
        ])

        if is_tanakh:
            # Try v3 API with "Text Only" version (no nikud/taamim) for Tanakh
            url = f"https://www.sefaria.org/api/v3/texts/{encoded_ref}?version=hebrew|Tanach%20with%20Text%20Only"
            resp = requests.get(url, timeout=15)

            if resp.status_code == 200:
                data = resp.json()
                versions = data.get('versions', [])
                for ver in versions:
                    if ver.get('language') == 'he':
                        ver_text = ver.get('text', [])
                        if isinstance(ver_text, str):
                            raw_text = ver_text
                        else:
                            raw_text = flatten_sefaria_text(ver_text)
                        break

        # Use v2 API for non-Tanakh or as fallback
        if not raw_text:
            url = f"https://www.sefaria.org/api/texts/{encoded_ref}?context=0&pad=0"
            resp = requests.get(url, timeout=15)
            if resp.status_code == 200:
                data = resp.json()
                he_text = data.get('he', [])
                if isinstance(he_text, str):
                    raw_text = he_text
                else:
                    raw_text = flatten_sefaria_text(he_text)

        if raw_text:
            # Clean the text (remove any remaining nikud, taamim, non-Hebrew)
            cleaned = clean_hebrew_text(raw_text)
            if cleaned:
                try:
                    with open(cache_file, 'w', encoding='utf-8') as f:
                        f.write(cleaned)
                except Exception:
                    pass
                return cleaned
    except requests.Timeout:
        print(f"Timeout fetching {ref}")
    except Exception as e:
        print(f"Error fetching {ref}: {e}")

    return ""


def create_parallels_page(initial_text: str = None):
    """Create the parallels (composition) search page."""

    # === State ===
    class ParallelsState:
        def __init__(self):
            self.is_running = False
            self.is_cancelled = False
            self.progress = 0
            self.status = ""
            self.results = []
            self.finished_animation_shown = False

    p_state = ParallelsState()

    # Restore previous results
    if 'parallels_results' in app.storage.user:
        try:
            p_state.results = app.storage.user.get('parallels_results', [])
            # Also restore to global state for export functionality
            state.parallels_results = p_state.results
            state.parallels_filtered = app.storage.user.get('parallels_filtered', [])
        except Exception:
            pass

    # Decode initial text from URL or restore from storage
    decoded_text = ""
    if initial_text:
        try:
            decoded_text = unquote(initial_text)
        except Exception:
            decoded_text = initial_text
    else:
        # Try to restore from storage
        decoded_text = app.storage.user.get('parallels_source_text', '')

    # === UI Layout ===
    with ui.column().classes('w-full max-w-7xl mx-auto gap-6 fade-in'):

        # === Page Header ===
        with ui.row().classes('w-full items-center justify-between'):
            with ui.column().classes('gap-1'):
                # Changed to H1
                h1(tr('Find Parallels'), classes='text-3xl font-bold', style='color: var(--text-primary);')
                ui.label(tr('Discover parallel texts in the Genizah corpus')).style('color: var(--text-secondary);')

        # === Input Section ===
        with ui.card().classes('w-full p-6'):
            with ui.row().classes('w-full gap-6'):

                # Left: Text Input
                with ui.column().classes('flex-grow gap-4'):
                    # Changed to H2
                    h2(tr('Source text'), classes='text-xl font-bold', style='color: var(--text-primary);')

                    text_input = ui.textarea(
                        placeholder=tr('Paste your Hebrew text here...'),
                        value=decoded_text
                    ).classes('w-full').props('outlined rows=8').style('direction: rtl;')

                    # Word count
                    word_count_label = ui.label('0 ' + tr('Words')).classes('text-sm').style('color: var(--text-muted);')

                    def update_word_count():
                        text = text_input.value or ""
                        words = len([w for w in text.split() if w])
                        word_count_label.text = f"{words} {tr('Words')}"
                        # Save text to storage for persistence
                        try:
                            app.storage.user['parallels_source_text'] = text
                        except Exception:
                            pass

                    text_input.on('update:model-value', update_word_count)
                    # Also update on blur to catch paste events
                    text_input.on('blur', update_word_count)
                    # Update after a short delay to ensure textarea has initial value from storage
                    ui.timer(0.3, update_word_count, once=True)

                # Right: Options Panel
                with ui.column().classes('w-80 gap-4'):
                    # Changed to H2
                    h2(tr('Options'), classes='text-xl font-bold', style='color: var(--text-primary);')

                    # Mode
                    mode_select = ui.select(
                        {
                            'exact': tr('Exact'),
                            'variants': tr('Variants'),
                            'fuzzy': tr('Fuzzy'),
                        },
                        value='exact',
                        label=tr('Search Mode')
                    ).classes('w-full').props('outlined dense')

                    # Check if user prefers slider or presets (default: presets)
                    use_slider = False
                    if state.lab_engine and hasattr(state.lab_engine, 'settings') and state.lab_engine.settings:
                        use_slider = getattr(state.lab_engine.settings, 'variant_use_slider', False)

                    # Track current preset level (default: Basic=30)
                    current_preset = {'value': 30}

                    # Variables for elements
                    variant_level_select = None
                    variant_slider = variant_slider_label = None

                    # Variant Level Controls (visible only in Variants mode)
                    with ui.row().classes('items-center gap-4') as variant_controls_col:
                        if not use_slider:
                            # Dropdown selector (compact mode)
                            with ui.column().classes('gap-1'):
                                h3(tr('Level'), classes='text-sm font-medium', style='color: var(--text-secondary);')
                                variant_level_select = ui.select(
                                    {
                                        30: '○ ' + tr('Basic'),
                                        70: '◐ ' + tr('Extended'),
                                        150: '● ' + tr('Maximum'),
                                    },
                                    value=current_preset['value']
                                ).classes('w-36').props('outlined dense')

                            with ui.column().classes('gap-1'):
                                h3(tr('Num Changes'), classes='text-sm font-medium', style='color: var(--text-secondary);')
                                max_changes_select = ui.select({1: '×1', 2: '×2', 3: '×3'}, value=2).classes('w-16').props('outlined dense')
                        else:
                            # Slider mode
                            with ui.column().classes('gap-1 w-full'):
                                h3(tr('Variant Level'), classes='text-sm font-medium', style='color: var(--text-secondary);')
                                with ui.row().classes('items-center gap-2 w-full'):
                                    variant_slider = ui.slider(min=10, max=300, value=30, step=10).classes('flex-grow').props('label-always')
                                    variant_slider_label = ui.label('30').classes('text-sm font-medium w-10').style('color: var(--primary-600);')
                            with ui.column().classes('gap-1'):
                                h3(tr('Num Changes'), classes='text-sm font-medium', style='color: var(--text-secondary);')
                                max_changes_select = ui.select({1: '×1', 2: '×2', 3: '×3'}, value=2).classes('w-16').props('outlined dense')

                    def set_level(level_value):
                        """Set variant level."""
                        current_preset['value'] = level_value
                        if state.var_mgr:
                            state.var_mgr.set_variant_level(level_value)

                    if variant_level_select:
                        def on_level_change():
                            set_level(int(variant_level_select.value))
                        variant_level_select.on('update:model-value', on_level_change)

                    if variant_slider:
                        def on_slider_change():
                            val = int(variant_slider.value)
                            current_preset['value'] = val
                            variant_slider_label.set_text(str(val))
                            if state.var_mgr:
                                state.var_mgr.set_variant_level(val)
                        variant_slider.on('update:model-value', on_slider_change)

                    def on_mode_change():
                        is_variants = mode_select.value == 'variants'
                        variant_controls_col.set_visibility(is_variants)

                    mode_select.on('update:model-value', on_mode_change)
                    # Set initial visibility (exact mode = hide variant controls)
                    variant_controls_col.set_visibility(False)

                    # Chunk Size
                    with ui.column().classes('gap-1'):
                        # Changed to H3
                        h3(tr('Chunk size'), classes='text-sm font-medium', style='color: var(--text-secondary);')
                        chunk_size = ui.slider(min=2, max=12, value=5).props('label-always')
                        ui.label(tr('Words per search chunk (recommended: 4-7)')).classes('text-xs').style('color: var(--text-muted);')

                    # Frequency threshold (for standard mode - filters results appearing in too many documents)
                    with ui.column().classes('gap-1') as freq_threshold_row:
                        h3(tr('Max frequency'), classes='text-sm font-medium', style='color: var(--text-secondary);')
                        freq_threshold = ui.slider(min=10, max=100, value=50).props('label-always')
                        ui.label(tr('Filter common phrases (lower = stricter)')).classes('text-xs').style('color: var(--text-muted);')

                    ui.separator().classes('my-2')

                    # Lab Mode Toggle
                    lab_mode = ui.checkbox(tr('Lab Mode (experimental)')).classes('mt-2')
                    lab_mode.tooltip(tr('Advanced search using fingerprint algorithm. Slower but more features.'))

                    # === Lab Mode Options (hidden by default) ===
                    with ui.column().classes('w-full gap-2 mt-2').style('display: none;') as lab_mode_options:
                        # Deep Scan (Lab Mode only)
                        deep_scan = ui.checkbox(tr('Deep Scan')).classes('mt-2')
                        deep_scan.tooltip(tr('Exhaustive search - slower but finds more results'))

                    # === Boundary Search Settings (available for both modes) ===
                    with ui.expansion(tr('More options'), icon='settings').classes('w-full mt-2') as more_options_panel:
                        with ui.column().classes('w-full gap-3 p-2'):
                            h3(tr('Paragraph search'), classes='text-sm font-medium', style='color: var(--text-secondary);')

                            # Boundary mode radio buttons
                            boundary_mode = ui.radio(
                                options={
                                    'full': tr('Full search'),
                                    'boundary': tr('Cross-paragraph only'),
                                    'combined': tr('Full + Cross-paragraph boost')
                                },
                                value='full'
                            ).classes('w-full').props('dense')

                            # Tooltips for boundary modes
                            boundary_mode_tooltips = {
                                'boundary': tr('Show only matches where the matching text spans a paragraph break in your source'),
                                'combined': tr('Search everything, but rank cross-paragraph matches higher')
                            }

                        # Paragraph delimiter dropdown
                        with ui.column().classes('gap-1 mt-2'):
                            h3(tr('Paragraph separator'), classes='text-sm font-medium', style='color: var(--text-secondary);')
                            boundary_delimiter = ui.select(
                                options={
                                    '\n\n': tr('Blank line (paragraph)'),
                                    '\n': tr('Line break'),
                                    '.': tr('Period (.)'),
                                    ':': tr('Colon (:)')
                                },
                                value='\n\n'
                            ).classes('w-full').props('outlined dense')

                        # Pre-search stats display (updated when text or delimiter changes)
                        boundary_stats_label = ui.label('').classes('text-xs mt-1').style('color: var(--primary-600); display: none;')
                        boundary_warning_label = ui.label('').classes('text-xs mt-1').style('color: var(--error); display: none;')

                        # Advanced settings (collapsed by default)
                        with ui.expansion(tr('Advanced settings'), icon='tune').classes('w-full mt-2') as advanced_settings:
                            with ui.column().classes('w-full gap-3 p-2'):
                                # Cross-paragraph boost slider
                                with ui.column().classes('gap-1'):
                                    h3(tr('Cross-paragraph boost'), classes='text-sm font-medium', style='color: var(--text-secondary);')
                                    boundary_boost = ui.slider(min=1.0, max=3.0, value=1.5, step=0.1).props('label-always')
                                    ui.label(tr('Score multiplier for cross-paragraph matches')).classes('text-xs').style('color: var(--text-muted);')

                                # Min boundary matches filter
                                with ui.column().classes('gap-1'):
                                    h3(tr('Min. cross-paragraph matches'), classes='text-sm font-medium', style='color: var(--text-secondary);')
                                    min_boundary_matches = ui.select(
                                        options={i: str(i) for i in range(11)},
                                        value=0
                                    ).classes('w-32').props('outlined dense')

                                # Min delimiter distance
                                with ui.column().classes('gap-1'):
                                    h3(tr('Min. words between separators'), classes='text-sm font-medium', style='color: var(--text-secondary);')
                                    min_delimiter_distance = ui.select(
                                        options={i: str(i) for i in range(1, 11)},
                                        value=3
                                    ).classes('w-32').props('outlined dense')

                    def update_boundary_stats():
                        """Update pre-search boundary statistics."""
                        try:
                            from genizah_core import get_boundary_stats
                            text = text_input.value or ""
                            if not text.strip():
                                boundary_stats_label.style('display: none;')
                                boundary_warning_label.style('display: none;')
                                return

                            c_size = int(chunk_size.value) if chunk_size.value else 5
                            delimiter = boundary_delimiter.value or '\n\n'
                            min_dist = int(min_delimiter_distance.value) if min_delimiter_distance.value else 3

                            stats = get_boundary_stats(text, delimiter, c_size, min_dist)

                            if stats['boundary_count'] > 0:
                                boundary_stats_label.text = tr('{} boundaries detected, {} chunks will cross them').format(
                                    stats['boundary_count'], stats['crossing_chunk_count']
                                )
                                boundary_stats_label.style('display: block;')
                                boundary_warning_label.style('display: none;')
                            else:
                                boundary_stats_label.style('display: none;')
                                if boundary_mode.value in ('boundary', 'combined'):
                                    boundary_warning_label.text = tr('No paragraph breaks detected in text!')
                                    boundary_warning_label.style('display: block;')
                                else:
                                    boundary_warning_label.style('display: none;')
                        except Exception:
                            boundary_stats_label.style('display: none;')
                            boundary_warning_label.style('display: none;')

                    # Update stats when relevant controls change
                    boundary_delimiter.on('update:model-value', update_boundary_stats)
                    chunk_size.on('update:model-value', update_boundary_stats)
                    min_delimiter_distance.on('update:model-value', update_boundary_stats)
                    text_input.on('blur', update_boundary_stats)

                    # Auto-expand advanced settings when switching to combined mode
                    def on_boundary_mode_change():
                        if boundary_mode.value == 'combined':
                            advanced_settings.open()
                        update_boundary_stats()

                    boundary_mode.on('update:model-value', on_boundary_mode_change)

                    # Lab Mode toggle handler
                    def on_lab_mode_change():
                        """Show/hide lab mode options based on toggle."""
                        if lab_mode.value:
                            lab_mode_options.style('display: block;')
                            freq_threshold_row.style('display: none;')  # Hide freq threshold in lab mode
                        else:
                            lab_mode_options.style('display: none;')
                            freq_threshold_row.style('display: block;')  # Show freq threshold in standard mode

                    lab_mode.on('update:model-value', on_lab_mode_change)

                    ui.separator().classes('my-2')

                    # Run Button
                    run_btn = ui.button(
                        tr('Find Parallels'),
                        icon='compare_arrows',
                        on_click=lambda: execute_parallels()
                    ).classes('btn-primary w-full')

                    # Stop Button (hidden by default) - shows partial results
                    with ui.column().classes('w-full items-center gap-0').style('display: none;') as cancel_btn:
                        ui.button(
                            tr('Stop'),
                            icon='stop',
                            on_click=lambda: cancel_search()
                        ).classes('w-full').props('outline color=red')
                        ui.label(tr('and show partial results')).classes('text-xs').style('color: var(--text-muted);')

                    # Progress - visible spinner + status in the control panel
                    progress_bar = ui.linear_progress(0).classes('w-full my-2').style('height: 8px; opacity: 0;')
                    with ui.row().classes('w-full items-center justify-center gap-2').style('display: none;') as search_indicator:
                        ui.spinner('dots', size='sm', color='primary')
                        status_label = ui.label('').classes('text-sm font-medium').style('color: var(--primary-600);')

        # === Filter Text (Collapsible) ===
        # State for loaded sources: {ref: cleaned_text}
        # Only store refs in persistent storage (not the full text - too large for WebSocket)
        # Full text is reloaded from cache files on page load (async)
        filter_sources = {'loaded': {}, 'enabled': set(), 'pending_restore': True, 'custom_count': 0}

        # Filter expansion with dynamic badge
        with ui.row().classes('w-full items-center'):
            filter_expansion = ui.expansion(tr('Filter text (exclude known sources)'), icon='filter_alt').classes('flex-1')
            filter_expansion.tooltip(tr('Choose known sources to exclude from results (e.g., Bible verses, Mishnah). Matches found in these sources will be moved to a separate list.'))
            filter_badge = ui.badge('0').props('color=grey transparent').classes('ml-2').style('display: none;')

        def update_filter_badge():
            """Update badge with number of loaded sources."""
            count = len(filter_sources['enabled'])
            if count > 0:
                filter_badge.set_text(f"{count}")
                filter_badge.props('color=primary')
                filter_badge.style('display: inline-flex;')
            else:
                filter_badge.style('display: none;')

        with filter_expansion:
            with ui.column().classes('w-full p-4 gap-4'):
                ui.label(tr('Select sources to filter results (matches found in checked sources will be moved to a separate list):')).classes('text-sm').style('color: var(--text-muted);')

                # Sefaria source buttons
                with ui.row().classes('w-full items-center gap-2 flex-wrap'):
                    ui.label(tr('Load from Sefaria') + ':').classes('text-sm font-medium').style('color: var(--text-secondary);')
                    btn_tanakh = ui.button(tr('Tanakh'), icon='menu_book').props('outline dense size=sm')
                    btn_mishnah = ui.button(tr('Mishnah'), icon='menu_book').props('outline dense size=sm')
                    btn_talmud = ui.button(tr('Talmud'), icon='menu_book').props('outline dense size=sm')
                    btn_more = ui.button(tr('More Sources...'), icon='library_books').props('outline dense size=sm')
                    btn_sefaria_search = ui.button(tr('Search Sefaria'), icon='search').props('outline dense size=sm')

                # Custom text button
                with ui.row().classes('w-full items-center gap-2'):
                    ui.label(tr('Custom source') + ':').classes('text-sm font-medium').style('color: var(--text-secondary);')
                    btn_add_custom = ui.button(tr('Add Custom Text'), icon='add').props('outline dense size=sm')

                # Progress for Sefaria loading
                sefaria_progress = ui.linear_progress(0).classes('w-full').style('display: none;')
                sefaria_status = ui.label('').classes('text-xs').style('color: var(--text-muted); display: none;')

                # Loaded sources list (checkboxes)
                with ui.column().classes('w-full gap-2'):
                    h4(tr('Loaded Sources'), classes='text-sm font-medium', style='color: var(--text-secondary);')

                    with ui.row().classes('gap-2 mb-2'):
                        btn_select_all = ui.button(tr('Select All'), icon='check_box').props('flat dense size=sm')
                        btn_deselect_all = ui.button(tr('Deselect All'), icon='check_box_outline_blank').props('flat dense size=sm')
                        btn_remove_unchecked = ui.button(tr('Remove Unchecked'), icon='delete').props('flat dense size=sm color=red')

                    loaded_sources_container = ui.column().classes('w-full max-h-48 overflow-y-auto gap-1 p-2 rounded').style('background: var(--bg-secondary);')

                    filter_info_label = ui.label(tr('Active: {} / {}').format(0, 0)).classes('text-xs').style('color: var(--text-muted);')

        # === Results Section ===
        with ui.card().classes('w-full p-6'):
            with ui.row().classes('w-full items-center justify-between mb-4'):
                # Changed to H2 (stored in variable but it's a UI element)
                results_header = h2(tr('Results'), classes='text-xl font-bold', style='color: var(--text-primary);')

                with ui.row().classes('gap-2'):
                    # Sort options
                    sort_select = ui.select(
                        {
                            'score': tr('Sort by score'),
                            'shelfmark': tr('Sort by shelfmark'),
                            'matches': tr('Sort by matches'),
                        },
                        value='score'
                    ).props('outlined dense').classes('w-40')

                    ui.button(icon='description', on_click=lambda: ui.download('/api/export/parallels/word')).props(
                        'flat round dense'
                    ).tooltip(tr('Export Word'))
                    ui.button(icon='table_view', on_click=lambda: ui.download('/api/export/parallels/excel')).props(
                        'flat round dense'
                    ).tooltip(tr('Export Excel'))

            results_container = ui.column().classes('w-full gap-4')

    # === Logic ===

    # === Sefaria Loading Functions ===
    def show_sefaria_selection_dialog(source_type: str):
        """Show dialog to select books from a Sefaria source."""
        source_data = SEFARIA_SOURCES.get(source_type)
        if not source_data:
            return

        with ui.dialog() as dialog, ui.card().classes('p-6 min-w-[400px] max-w-[500px]'):
            h3(tr('Select Books'), classes='text-xl font-bold mb-4').style('color: var(--text-primary);')

            # Category selector
            with ui.row().classes('w-full items-center gap-2 mb-4'):
                ui.label(tr('Category:') if tr('Category:') != 'Category:' else 'קטגוריה:').classes('text-sm').style('color: var(--text-secondary);')
                cat_options = {'all': tr('All')}
                for key, book_data in source_data['books'].items():
                    cat_options[key] = book_data['name']
                cat_select = ui.select(cat_options, value='all').props('outlined dense').classes('flex-grow')

            # Books list container
            books_container = ui.column().classes('w-full max-h-64 overflow-y-auto gap-1 p-2 rounded').style('background: var(--bg-secondary);')

            # Track selected books
            selected_refs = {'refs': []}

            def populate_books():
                books_container.clear()
                cat_key = cat_select.value

                with books_container:
                    if cat_key == 'all':
                        for book_key, book_data in source_data['books'].items():
                            with ui.expansion(book_data['name'], icon='folder').classes('w-full'):
                                for ref, he_name in zip(book_data['refs'], book_data['he_names']):
                                    cb = ui.checkbox(he_name).classes('text-sm')
                                    cb.on('update:model-value', lambda checked, r=ref: toggle_ref(r, checked))
                    else:
                        book_data = source_data['books'].get(cat_key, {})
                        for ref, he_name in zip(book_data.get('refs', []), book_data.get('he_names', [])):
                            cb = ui.checkbox(he_name).classes('text-sm')
                            cb.on('update:model-value', lambda checked, r=ref: toggle_ref(r, checked))

            def toggle_ref(ref, checked):
                if checked and ref not in selected_refs['refs']:
                    selected_refs['refs'].append(ref)
                elif not checked and ref in selected_refs['refs']:
                    selected_refs['refs'].remove(ref)

            cat_select.on('update:model-value', lambda: populate_books())

            # Select all checkbox
            def select_all(checked):
                selected_refs['refs'] = []
                if checked:
                    for book_data in source_data['books'].values():
                        selected_refs['refs'].extend(book_data['refs'])
                populate_books()

            ui.checkbox(tr('Select All'), on_change=lambda e: select_all(e.value)).classes('my-2')

            populate_books()

            # Buttons
            with ui.row().classes('w-full justify-end gap-2 mt-4'):
                ui.button(tr('Cancel'), on_click=dialog.close).props('flat')
                ui.button(tr('Load Selected'), on_click=lambda: load_selected_refs(selected_refs['refs'], dialog)).classes('btn-primary')

        dialog.open()

    def refresh_loaded_sources_ui():
        """Refresh the list of loaded sources with checkboxes."""
        loaded_sources_container.clear()

        with loaded_sources_container:
            if not filter_sources['loaded']:
                ui.label(tr('No sources loaded yet')).classes('text-sm text-gray-500')
            else:
                for ref in sorted(filter_sources['loaded'].keys()):
                    cb = ui.checkbox(get_source_display_name(ref), value=ref in filter_sources['enabled']).classes('text-sm')
                    cb.on('update:model-value', lambda checked, r=ref: on_source_toggled(r, checked))

        update_filter_info()

    def save_filter_sources():
        """Save filter source refs to persistent storage (not the full text - too large)."""
        try:
            # Separate custom texts from Sefaria refs
            sefaria_refs = [ref for ref in filter_sources['loaded'].keys() if not ref.startswith('custom:')]
            custom_texts = {ref: filter_sources['loaded'][ref] for ref in filter_sources['loaded'].keys() if ref.startswith('custom:')}

            # Save Sefaria refs (text reloaded from cache)
            app.storage.user['filter_sources_refs'] = sefaria_refs
            app.storage.user['filter_sources_enabled'] = list(filter_sources['enabled'])

            # Save custom texts (small enough to store directly)
            app.storage.user['filter_sources_custom'] = custom_texts
            app.storage.user['filter_sources_custom_count'] = filter_sources.get('custom_count', 0)
        except Exception as e:
            print(f"[DEBUG] Error saving filter sources: {e}")

    def on_source_toggled(ref, checked):
        """Handle source checkbox toggle."""
        if checked:
            filter_sources['enabled'].add(ref)
        else:
            filter_sources['enabled'].discard(ref)
        update_filter_info()  # Also updates badge
        save_filter_sources()

    def update_filter_info():
        """Update the info label and badge."""
        enabled = len(filter_sources['enabled'])
        total = len(filter_sources['loaded'])
        filter_info_label.text = tr('Active: {} / {}').format(enabled, total)
        update_filter_badge()

    def select_all_sources():
        filter_sources['enabled'] = set(filter_sources['loaded'].keys())
        refresh_loaded_sources_ui()
        save_filter_sources()

    def deselect_all_sources():
        filter_sources['enabled'].clear()
        refresh_loaded_sources_ui()
        save_filter_sources()

    def remove_unchecked_sources():
        to_remove = [ref for ref in filter_sources['loaded'].keys() if ref not in filter_sources['enabled']]
        for ref in to_remove:
            del filter_sources['loaded'][ref]
        refresh_loaded_sources_ui()
        save_filter_sources()

    def get_filter_text():
        """Get combined text from all enabled sources."""
        texts = [filter_sources['loaded'][ref] for ref in filter_sources['enabled'] if ref in filter_sources['loaded']]
        return " ".join(texts)

    # Connect filter management buttons
    btn_select_all.on('click', select_all_sources)
    btn_deselect_all.on('click', deselect_all_sources)
    btn_remove_unchecked.on('click', remove_unchecked_sources)

    async def load_selected_refs(refs, dialog):
        """Load selected refs from Sefaria with incremental progress."""
        if not refs:
            ui.notify(tr('Please select at least one book.'), type='warning')
            return

        if dialog:
            try:
                dialog.close()
            except Exception:
                pass

        # Filter out already loaded refs
        new_refs = [r for r in refs if r not in filter_sources['loaded']]
        if not new_refs:
            ui.notify(tr('All selected sources are already loaded.'), type='info')
            return

        # Show progress
        try:
            sefaria_progress.style('display: block;')
            sefaria_status.style('display: block;')
            sefaria_progress.value = 0
        except (RuntimeError, Exception):
            return  # Client deleted

        total = len(new_refs)
        loaded_count = 0
        failed_count = 0

        try:
            sefaria_status.text = tr('Loading: {}').format(f"0/{total}")
        except (RuntimeError, Exception):
            return

        # Fetch one at a time with progress updates
        for i, ref in enumerate(new_refs):
            # Check if client is still valid
            try:
                _ = sefaria_progress.client
            except (RuntimeError, Exception):
                print("[DEBUG] Client deleted during load, aborting")
                return

            # Update progress before fetching
            try:
                sefaria_status.text = tr('Loading: {}').format(f"{i}/{total} - {get_source_display_name(ref)[:30]}...")
                sefaria_progress.value = i / total
            except (RuntimeError, Exception):
                return

            # Fetch in background thread to avoid blocking UI
            text = await run.io_bound(fetch_sefaria_text, ref)

            if text:
                filter_sources['loaded'][ref] = text
                filter_sources['enabled'].add(ref)
                loaded_count += 1
            else:
                failed_count += 1

            # Update UI periodically (every item)
            try:
                sefaria_progress.value = (i + 1) / total
            except (RuntimeError, Exception):
                return

        # Save to storage
        save_filter_sources()

        # Update UI
        try:
            refresh_loaded_sources_ui()

            # Notify only on failure
            if failed_count > 0:
                ui.notify(f'{tr("Failed to load")} {failed_count} {tr("sources")}', type='negative')

            # Hide progress
            sefaria_progress.style('display: none;')
            sefaria_status.style('display: none;')
        except (RuntimeError, Exception):
            pass  # Client deleted

    # Connect Sefaria buttons
    btn_tanakh.on('click', lambda: show_sefaria_selection_dialog('tanakh'))
    btn_mishnah.on('click', lambda: show_sefaria_selection_dialog('mishnah'))
    btn_talmud.on('click', lambda: show_sefaria_selection_dialog('talmud'))
    btn_more.on('click', lambda: show_all_sources_dialog())
    btn_sefaria_search.on('click', lambda: show_sefaria_search_dialog())
    btn_add_custom.on('click', lambda: show_add_custom_dialog())

    def show_add_custom_dialog():
        """Show dialog to add custom text source."""
        with ui.dialog() as dialog, ui.card().classes('p-6 min-w-[500px] max-w-[600px]'):
            h3(tr('Add Custom Text'), classes='text-xl font-bold mb-4').style('color: var(--text-primary);')

            ui.label(tr('Enter a name for this source:')).classes('text-sm').style('color: var(--text-secondary);')
            name_input = ui.input(placeholder=tr('e.g., My Commentary')).classes('w-full mb-4').props('outlined')

            ui.label(tr('Paste your text (will be cleaned automatically):')).classes('text-sm').style('color: var(--text-secondary);')
            text_area = ui.textarea(placeholder=tr('Paste Hebrew text here...')).classes('w-full').props('outlined rows=10').style('direction: rtl;')

            def add_custom_text():
                name = name_input.value.strip() if name_input.value else ''
                text = text_area.value.strip() if text_area.value else ''

                if not name:
                    ui.notify(tr('Please enter a name for the source'), type='warning')
                    return
                if not text or len(text) < 10:
                    ui.notify(tr('Please enter at least 10 characters of text'), type='warning')
                    return

                # Clean the text
                cleaned = clean_hebrew_text(text)
                if not cleaned or len(cleaned) < 10:
                    ui.notify(tr('No valid Hebrew text found'), type='warning')
                    return

                # Generate a unique ref for custom text
                filter_sources['custom_count'] = filter_sources.get('custom_count', 0) + 1
                custom_ref = f"custom:{filter_sources['custom_count']}:{name}"

                # Add to sources
                filter_sources['loaded'][custom_ref] = cleaned
                filter_sources['enabled'].add(custom_ref)
                save_filter_sources()
                refresh_loaded_sources_ui()

                dialog.close()
                ui.notify(f'{tr("Added")} "{name}" ({len(cleaned)} {tr("characters")})', type='positive')

            with ui.row().classes('w-full justify-end gap-2 mt-4'):
                ui.button(tr('Cancel'), on_click=dialog.close).props('flat')
                ui.button(tr('Add'), on_click=add_custom_text).classes('btn-primary')

        dialog.open()

    async def show_sefaria_search_dialog():
        """Show dialog to search and load any Sefaria text by reference."""
        with ui.dialog() as dialog, ui.card().classes('p-6 min-w-[500px] max-w-[600px]'):
            h3(tr('Search Sefaria'), classes='text-xl font-bold mb-4').style('color: var(--text-primary);')

            ui.label(tr('Enter a Sefaria reference (e.g., "Genesis 1", "Berakhot 2a", "Rashi on Genesis 1"):')).classes('text-sm').style('color: var(--text-muted);')
            ref_input = ui.input(placeholder='Genesis 1').classes('w-full mb-2').props('outlined')

            # Quick examples
            ui.label(tr('Examples:')).classes('text-xs mt-2').style('color: var(--text-muted);')
            with ui.row().classes('gap-1 flex-wrap'):
                for example in ['Genesis 1', 'Exodus', 'Psalms', 'Berakhot', 'Shabbat', 'Rashi on Genesis', 'Mishneh Torah']:
                    ui.button(example, on_click=lambda e=example: ref_input.set_value(e)).props('flat dense size=xs')

            # Status
            search_status = ui.label('').classes('text-sm mt-4').style('color: var(--text-secondary);')

            async def search_and_load():
                ref = ref_input.value.strip() if ref_input.value else ''
                if not ref:
                    ui.notify(tr('Please enter a Sefaria reference'), type='warning')
                    return

                search_status.text = tr('Searching...')

                # Try to fetch
                text = await run.io_bound(fetch_sefaria_text, ref, True)

                if text:
                    filter_sources['loaded'][ref] = text
                    filter_sources['enabled'].add(ref)
                    save_filter_sources()
                    refresh_loaded_sources_ui()
                    dialog.close()
                    ui.notify(f'{tr("Loaded")} "{ref}" ({len(text)} {tr("characters")})', type='positive')
                else:
                    search_status.text = tr('Not found. Try a different reference.')
                    ui.notify(tr('Text not found'), type='negative')

            with ui.row().classes('w-full justify-end gap-2 mt-4'):
                ui.button(tr('Cancel'), on_click=dialog.close).props('flat')
                ui.button(tr('Load'), on_click=search_and_load).classes('btn-primary')

        dialog.open()

    async def show_all_sources_dialog():
        """Show dialog to browse all Sefaria sources in hierarchical tree."""
        library = get_sefaria_library()

        # Track selected refs
        selected_refs_state = {'refs': set()}

        with ui.dialog().classes('max-w-4xl') as dialog, ui.card().classes('p-6 w-full').style('min-width: 700px; max-height: 80vh;'):
            h3(tr('Sefaria Library'), classes='text-xl font-bold mb-4').style('color: var(--text-primary);')

            # Search box
            with ui.row().classes('w-full items-center gap-2 mb-4'):
                ui.label(tr('Search:')).classes('text-sm').style('color: var(--text-secondary);')
                search_input = ui.input(placeholder=tr('Search texts...')).classes('flex-grow').props('outlined dense')

            # Status label
            status_label = ui.label(tr('Loading library...')).classes('text-sm').style('color: var(--text-muted);')

            # Main content area with two columns
            with ui.splitter(value=35).classes('w-full').style('height: 400px;') as splitter:
                with splitter.before:
                    # Category tree (left side)
                    with ui.scroll_area().classes('w-full h-full'):
                        categories_container = ui.column().classes('w-full gap-1 p-2')

                with splitter.after:
                    # Texts list (right side)
                    with ui.column().classes('w-full h-full'):
                        texts_container = ui.scroll_area().classes('w-full flex-grow')
                        with ui.row().classes('w-full items-center gap-2 mt-2'):
                            select_all_cb = ui.checkbox(tr('Select All in Category'))
                            info_label = ui.label(tr('Selected: 0')).classes('text-xs').style('color: var(--text-muted);')

            # Buttons
            with ui.row().classes('w-full justify-end gap-2 mt-4'):
                ui.button(tr('Cancel'), on_click=dialog.close).props('flat')
                load_btn = ui.button(tr('Load Selected'), on_click=lambda: finish_selection()).classes('btn-primary')

            def update_info():
                info_label.text = tr('Selected: {}').format(len(selected_refs_state['refs']))

            def toggle_ref(ref, checked):
                if checked:
                    selected_refs_state['refs'].add(ref)
                else:
                    selected_refs_state['refs'].discard(ref)
                update_info()

            def show_category_texts(category_data):
                """Show texts from a category in the right panel."""
                texts_container.clear()
                select_all_cb.value = False

                texts = library.get_texts_recursive(category_data)

                with texts_container:
                    with ui.column().classes('w-full gap-1 p-2'):
                        for text in texts:
                            title = text.get('title', '')
                            he_title = text.get('heTitle', title)
                            cb = ui.checkbox(he_title, value=title in selected_refs_state['refs']).classes('text-sm')
                            cb.on('update:model-value', lambda checked, r=title: toggle_ref(r, checked))

                # Connect select all
                def on_select_all(checked):
                    for text in texts:
                        title = text.get('title', '')
                        if checked:
                            selected_refs_state['refs'].add(title)
                        else:
                            selected_refs_state['refs'].discard(title)
                    show_category_texts(category_data)  # Refresh to update checkboxes
                    update_info()

                select_all_cb.on('update:model-value', on_select_all)
                update_info()

            def build_category_tree(parent_container, contents, depth=0):
                """Recursively build the category tree."""
                for item in contents:
                    if isinstance(item, dict):
                        if 'category' in item:
                            # It's a category
                            cat_name = item.get('heCategory', item.get('category', ''))
                            sub_contents = item.get('contents', [])

                            if sub_contents:
                                # Has children - make it expandable
                                with parent_container:
                                    with ui.expansion(cat_name, icon='folder').classes('w-full'):
                                        inner_container = ui.column().classes('w-full gap-1 pl-4')
                                        build_category_tree(inner_container, sub_contents, depth + 1)

                                    # Add click handler to show texts
                                    # The expansion header can be clicked to show texts
                            else:
                                # Leaf category
                                with parent_container:
                                    btn = ui.button(cat_name, on_click=lambda i=item: show_category_texts(i)).props('flat dense align=left').classes('w-full justify-start')

            async def load_library():
                """Load the Sefaria library TOC."""
                toc = await run.io_bound(library.get_toc)
                if not toc:
                    status_label.text = tr('Failed to load library. Check internet connection.')
                    return

                status_label.text = ''
                categories_container.clear()

                with categories_container:
                    for category in toc:
                        if isinstance(category, dict) and 'category' in category:
                            cat_name = category.get('heCategory', category.get('category', ''))
                            sub_contents = category.get('contents', [])

                            with ui.expansion(cat_name, icon='folder').classes('w-full') as exp:
                                inner_container = ui.column().classes('w-full gap-1')

                                # Add click handler for the expansion to show its texts
                                exp.on('click', lambda c=category: show_category_texts(c))

                                if sub_contents:
                                    build_category_tree(inner_container, sub_contents, 1)

            async def finish_selection():
                """Complete the selection and load texts."""
                if not selected_refs_state['refs']:
                    ui.notify(tr('Please select at least one book.'), type='warning')
                    return
                dialog.close()
                await load_selected_refs(list(selected_refs_state['refs']), None)

            # Start loading
            await load_library()

        dialog.open()

    async def load_all_sources_refs(refs, dialog):
        """Load selected refs from the all sources dialog."""
        if not refs:
            ui.notify(tr('Please select at least one book.'), type='warning')
            return

        dialog.close()
        await load_selected_refs(refs, None)

    def update_ui():
        try:
            # Check if client still exists
            _ = progress_bar.client
        except (RuntimeError, Exception):
            return  # Client deleted, stop updating

        try:
            if p_state.is_running:
                run_btn.disable()
                cancel_btn.style('display: flex;')
                search_indicator.style('display: flex;')
                progress_bar.style('opacity: 1;')
                progress_bar.set_value(p_state.progress)
                status_label.text = p_state.status
            else:
                run_btn.enable()
                cancel_btn.style('display: none;')
                search_indicator.style('display: none;')
                if p_state.progress >= 1.0 and not p_state.finished_animation_shown:
                    progress_bar.set_value(1.0)
                    p_state.finished_animation_shown = True
                    ui.timer(2.0, lambda: progress_bar.style('opacity: 0;'), once=True)
        except (RuntimeError, Exception):
            pass  # Client may be deleted

    # Use faster timer for more responsive progress updates
    ui.timer(0.05, update_ui)

    def cancel_search():
        p_state.is_cancelled = True
        p_state.status = tr('Cancelling...')

    async def execute_parallels():
        # Prevent duplicate executions
        if p_state.is_running:
            print("[DEBUG] execute_parallels: already running, skipping")
            return

        print("[DEBUG] execute_parallels called")
        text = text_input.value or ""
        words = len([w for w in text.split() if w])

        # Allow shorter texts (minimum 3 words instead of 10)
        if words < 3:
            ui.notify(tr('Enter at least 3 words'), type='warning')
            return

        if not state.lab_engine:
            ui.notify(tr('Lab Engine not initialized'), type='negative')
            return

        # Update variant level and max changes from UI before search
        if mode_select.value == 'variants' and state.var_mgr:
            # Get pairs count from preset or slider
            pairs_count = int(variant_slider.value) if variant_slider else current_preset['value']
            state.var_mgr.set_variant_level(pairs_count)
            if state.lab_engine and state.lab_engine.settings:
                state.lab_engine.settings.variant_max_changes = int(max_changes_select.value)

        # Reset state
        p_state.is_running = True
        p_state.is_cancelled = False
        p_state.progress = 0
        p_state.finished_animation_shown = False
        p_state.status = tr('Initializing search...')
        p_state.results = []
        p_state.filtered_results = []
        results_container.clear()

        # Show loading spinner in results area - make it prominent so user knows it's working
        with results_container:
            with ui.column().classes('w-full items-center py-12'):
                ui.spinner('bars', size='xl', color='primary').classes('mb-4')
                ui.label(tr('Searching for parallels...')).classes('text-xl font-bold animate-pulse').style('color: var(--primary-600);')
                ui.label(tr('This may take a while...')).classes('text-sm mt-2').style('color: var(--text-muted);')

        # Show immediate feedback in control panel
        ui.notify(tr('Starting search...'), type='info', timeout=2000)
        search_indicator.style('display: flex;')
        progress_bar.style('opacity: 1;')
        progress_bar.set_value(0)
        status_label.text = tr('Initializing search...')

        # Clear previous results header and container when starting new search
        results_header.text = tr('Searching...')
        results_container.clear()

        # Capture filter text in main thread to avoid closure issues in background thread
        captured_filter_text = get_filter_text()
        print(f"[DEBUG] Captured filter text length: {len(captured_filter_text) if captured_filter_text else 0}, enabled: {len(filter_sources['enabled'])}, loaded: {len(filter_sources['loaded'])}")
        if captured_filter_text:
            print(f"[DEBUG] Filter text sample (first 100 chars): {captured_filter_text[:100]}")

        def progress_cb(current, total):
            if p_state.is_cancelled:
                raise InterruptedError("Search cancelled")
            if total > 0:
                p_state.progress = current / total
                pct = round(p_state.progress * 100)
                p_state.status = f"{pct}%"

        # Capture search mode settings in main thread
        captured_lab_mode = lab_mode.value
        captured_freq_threshold = int(freq_threshold.value) if freq_threshold.value else 50
        captured_deep_scan = deep_scan.value if captured_lab_mode else False
        captured_chunk_size = int(chunk_size.value) if chunk_size.value else 5
        captured_mode = mode_select.value

        # Capture boundary settings (only used in lab mode)
        captured_boundary_mode = boundary_mode.value or 'full'
        captured_boundary_delimiter = boundary_delimiter.value or '\n\n'
        captured_boundary_boost = float(boundary_boost.value) if boundary_boost.value else 1.5
        captured_min_boundary_matches = int(min_boundary_matches.value) if min_boundary_matches.value else 0
        captured_min_delimiter_distance = int(min_delimiter_distance.value) if min_delimiter_distance.value else 3

        def run_search():
            try:
                print(f"[DEBUG] Starting search: lab_mode={captured_lab_mode}, mode={captured_mode}, chunk_size={captured_chunk_size}")

                if captured_lab_mode:
                    # LAB MODE: Use fingerprint-based search with advanced features
                    print(f"[DEBUG] Using LAB search: deep_scan={captured_deep_scan}")
                    print(f"[DEBUG] Boundary settings: mode={captured_boundary_mode}, delimiter='{repr(captured_boundary_delimiter)}', boost={captured_boundary_boost}")
                    result = state.lab_engine.lab_composition_search(
                        text,
                        mode=captured_mode,
                        progress_callback=progress_cb,
                        chunk_size=captured_chunk_size,
                        filter_text=captured_filter_text or None,
                        deep_scan=captured_deep_scan,
                        boundary_mode=captured_boundary_mode,
                        boundary_delimiter=captured_boundary_delimiter,
                        boundary_boost=captured_boundary_boost,
                        min_boundary_matches=captured_min_boundary_matches,
                        min_delimiter_distance=captured_min_delimiter_distance
                    )
                    print(f"[DEBUG] Lab search returned: main={len(result.get('main', []))}, filtered={len(result.get('filtered', []))}, partial={result.get('partial', False)}")
                    if result.get('boundary_stats'):
                        print(f"[DEBUG] Boundary stats: {result['boundary_stats']}")
                else:
                    # STANDARD MODE: Use direct Tantivy search (faster, simpler)
                    print(f"[DEBUG] Using STANDARD search: freq_threshold={captured_freq_threshold}")
                    print(f"[DEBUG] Boundary settings: mode={captured_boundary_mode}, delimiter='{repr(captured_boundary_delimiter)}', boost={captured_boundary_boost}")
                    result = state.searcher.search_composition_logic(
                        text,
                        chunk_size=captured_chunk_size,
                        max_freq=captured_freq_threshold,
                        mode=captured_mode,
                        filter_text=captured_filter_text or None,
                        progress_callback=progress_cb,
                        boundary_mode=captured_boundary_mode,
                        boundary_delimiter=captured_boundary_delimiter,
                        boundary_boost=captured_boundary_boost,
                        min_boundary_matches=captured_min_boundary_matches,
                        min_delimiter_distance=captured_min_delimiter_distance
                    )
                    print(f"[DEBUG] Standard search returned: main={len(result.get('main', []))}, filtered={len(result.get('filtered', []))}")
                    if result and result.get('boundary_stats'):
                        print(f"[DEBUG] Boundary stats: {result['boundary_stats']}")
                    # Add empty fields for compatibility with result display
                    if result:
                        result['partial'] = False

                return result
            except Exception as e:
                print(f"Parallels Error: {e}")
                import traceback
                traceback.print_exc()
                return None

        result_data = await run.io_bound(run_search)

        p_state.is_running = False
        p_state.progress = 1.0

        # Hide the search indicator animation
        search_indicator.style('display: none;')
        progress_bar.style('opacity: 0;')

        if result_data:
            main_results = result_data.get('main', [])
            filtered_results = result_data.get('filtered', [])
            is_partial = result_data.get('partial', False)

            if main_results or filtered_results:
                p_state.results = main_results
                p_state.filtered_results = filtered_results
                try:
                    # Store both main and filtered results for export
                    # Store in global state (for API export endpoints)
                    state.parallels_results = main_results
                    state.parallels_filtered = filtered_results
                    # Also store in user storage (for UI persistence across page reloads)
                    app.storage.user['parallels_results'] = main_results
                    app.storage.user['parallels_filtered'] = filtered_results
                except Exception:
                    pass

                # Show message if results are partial (search was cancelled)
                if is_partial:
                    p_state.status = tr('Partial results (search cancelled)')
                    ui.notify(tr('Showing partial results'), type='warning', timeout=3000)

                render_results(main_results, filtered_results, is_partial=is_partial)
            else:
                if is_partial:
                    p_state.status = tr('Search cancelled - no results yet')
                results_header.text = tr('No results')
                with results_container:
                    show_empty_state()
        else:
            results_header.text = tr('No results')
            with results_container:
                show_empty_state()

    def show_empty_state():
        with ui.column().classes('w-full items-center py-12'):
            ui.icon('search_off').classes('text-5xl').style('color: var(--text-muted);')
            # Changed to H3
            h3(tr('No parallels found'), classes='text-lg mt-4', style='color: var(--text-secondary);')
            ui.label(tr('Try adjusting your search parameters')).classes('text-sm').style('color: var(--text-muted);')

    def render_results(results, filtered_results=None, is_partial=False):
        try:
            _ = results_container.client
        except (RuntimeError, Exception):
            return  # Client deleted

        try:
            results_container.clear()
        except (RuntimeError, Exception):
            return

        if not results and not filtered_results:
            with results_container:
                show_empty_state()
            return

        # Sort if needed
        sort_by = sort_select.value
        if sort_by == 'score':
            sorted_results = sorted(results, key=lambda x: x.get('score', 0), reverse=True)
        elif sort_by == 'shelfmark':
            sorted_results = sorted(results, key=lambda x: extract_shelfmark(x))
        else:
            sorted_results = results

        # Group results by manuscript
        grouped = {}
        for item in sorted_results:
            raw_header = item.get('raw_header', '')
            sys_id = None
            shelfmark = 'Unknown'

            if raw_header and state.meta_mgr:
                try:
                    sys_match = re.search(r'(99\d{8,})', raw_header)
                    if sys_match:
                        sys_id = sys_match.group(1)
                        shelf_temp, _ = state.meta_mgr.get_meta_for_id(sys_id)
                        shelfmark = shelf_temp or shelfmark
                except Exception:
                    pass

            # Use sys_id as key, fallback to shelfmark
            key = sys_id if sys_id else shelfmark

            if key not in grouped:
                grouped[key] = {
                    'sys_id': sys_id,
                    'shelfmark': shelfmark,
                    'items': [],
                    'max_score': 0,
                    'avg_score': 0
                }

            grouped[key]['items'].append(item)
            grouped[key]['max_score'] = max(grouped[key]['max_score'], item.get('score', 0))

        # Calculate average scores
        for key in grouped:
            scores = [item.get('score', 0) for item in grouped[key]['items']]
            grouped[key]['avg_score'] = sum(scores) / len(scores) if scores else 0

        # Sort groups by max score
        sorted_groups = sorted(grouped.items(), key=lambda x: x[1]['max_score'], reverse=True)

        # Group filtered results similarly
        filtered_grouped = {}
        if filtered_results:
            for item in filtered_results:
                raw_header = item.get('raw_header', '')
                sys_id = None
                shelfmark = 'Unknown'

                if raw_header and state.meta_mgr:
                    try:
                        sys_match = re.search(r'(99\d{8,})', raw_header)
                        if sys_match:
                            sys_id = sys_match.group(1)
                            shelf_temp, _ = state.meta_mgr.get_meta_for_id(sys_id)
                            shelfmark = shelf_temp or shelfmark
                    except Exception:
                        pass

                key = sys_id if sys_id else shelfmark
                if key not in filtered_grouped:
                    filtered_grouped[key] = {
                        'sys_id': sys_id,
                        'shelfmark': shelfmark,
                        'items': [],
                        'max_score': 0,
                        'avg_score': 0
                    }
                filtered_grouped[key]['items'].append(item)
                filtered_grouped[key]['max_score'] = max(filtered_grouped[key]['max_score'], item.get('score', 0))

            for key in filtered_grouped:
                scores = [item.get('score', 0) for item in filtered_grouped[key]['items']]
                filtered_grouped[key]['avg_score'] = sum(scores) / len(scores) if scores else 0

        sorted_filtered_groups = sorted(filtered_grouped.items(), key=lambda x: x[1]['max_score'], reverse=True)

        # Lazy loading configuration
        BATCH_SIZE = 50
        main_displayed = [0]  # Use list to allow modification in nested function
        filtered_displayed = [0]

        # Update header with manuscript count
        total_results = len(results)
        total_manuscripts = len(sorted_groups)
        filtered_count = len(filtered_results) if filtered_results else 0
        partial_suffix = f" - {tr('partial results')}" if is_partial else ""

        if total_results == 0 and filtered_count > 0:
            # All results were filtered - explain this to user
            results_header.text = f"{tr('All results filtered')} ({filtered_count} {tr('in filtered sources')}){partial_suffix}"
        elif filtered_count > 0:
            results_header.text = f"{total_results} {tr('matches in')} {total_manuscripts} {tr('manuscripts')} ({filtered_count} {tr('filtered')}){partial_suffix}"
        else:
            results_header.text = f"{total_results} {tr('matches in')} {total_manuscripts} {tr('manuscripts')}{partial_suffix}"

        with results_container:
            # Container for main results
            main_results_container = ui.column().classes('w-full gap-4')
            main_load_more_container = ui.row().classes('w-full justify-center py-4')

            # Filtered results section
            filtered_section = ui.column().classes('w-full gap-4')
            filtered_load_more_container = ui.row().classes('w-full justify-center py-4')

        def load_more_main():
            """Load next batch of main results."""
            start = main_displayed[0]
            end = min(start + BATCH_SIZE, len(sorted_groups))
            with main_results_container:
                for group_key, group_data in sorted_groups[start:end]:
                    create_manuscript_group(group_data)
            main_displayed[0] = end

            # Update load more button
            main_load_more_container.clear()
            remaining = len(sorted_groups) - main_displayed[0]
            if remaining > 0:
                with main_load_more_container:
                    ui.button(
                        f"{tr('Load more')} ({remaining} {tr('remaining')})",
                        icon='expand_more',
                        on_click=load_more_main
                    ).props('flat color=primary')

        def load_more_filtered():
            """Load next batch of filtered results."""
            start = filtered_displayed[0]
            end = min(start + BATCH_SIZE, len(sorted_filtered_groups))
            with filtered_section:
                for group_key, group_data in sorted_filtered_groups[start:end]:
                    create_manuscript_group(group_data, is_filtered=True)
            filtered_displayed[0] = end

            # Update load more button
            filtered_load_more_container.clear()
            remaining = len(sorted_filtered_groups) - filtered_displayed[0]
            if remaining > 0:
                with filtered_load_more_container:
                    ui.button(
                        f"{tr('Load more')} ({remaining} {tr('remaining')})",
                        icon='expand_more',
                        on_click=load_more_filtered
                    ).props('flat color=amber')

        # Initial load of main results
        if sorted_groups:
            load_more_main()

        # Filtered results section header and initial load
        if sorted_filtered_groups:
            with results_container:
                ui.separator().classes('my-4')
                with ui.row().classes('w-full items-center gap-2 py-2'):
                    ui.icon('filter_alt').classes('text-xl').style('color: var(--accent-amber);')
                    h3(tr('Filtered Results (found in source texts)'), classes='text-lg', style='color: var(--accent-amber);')
                    ui.badge(f"{filtered_count}", color='amber').classes('text-xs')
            load_more_filtered()

    def create_manuscript_group(group_data, is_filtered=False):
        """Create an expandable manuscript group with its parallels."""
        shelfmark = group_data['shelfmark']
        sys_id = group_data['sys_id']
        items = group_data['items']
        max_score = group_data['max_score']
        avg_score = group_data['avg_score']

        # Get title
        title = ''
        if sys_id and state.meta_mgr:
            try:
                _, title_temp = state.meta_mgr.get_meta_for_id(sys_id)
                title = title_temp or ''
            except Exception:
                pass

        border_style = 'border: 2px solid var(--accent-amber);' if is_filtered else 'border: 2px solid var(--border-light);'
        with ui.card().classes('w-full p-0 overflow-hidden').style(border_style):
            # Header (always visible)
            with ui.row().classes('w-full items-center justify-between p-4').style('background: var(--bg-card);'):
                with ui.column().classes('gap-1 flex-grow'):
                    with ui.row().classes('items-center gap-3'):
                        icon_color = 'color: var(--accent-amber);' if is_filtered else 'color: var(--primary-600);'
                        ui.icon('menu_book').classes('text-xl').style(icon_color)
                        # Changed to H3
                        shelfmark_color = 'color: var(--accent-amber);' if is_filtered else 'color: var(--primary-700);'
                        h3(shelfmark, classes='text-lg font-bold', style=shelfmark_color)
                        badge_color = 'amber' if is_filtered else 'blue'
                        ui.badge(f"{len(items)} {tr('matches')}", color=badge_color).classes('text-xs')

                    if title:
                        title_short = (title[:100] + '...') if len(title) > 100 else title
                        ui.label(title_short).classes('text-xs').style('color: var(--text-secondary); direction: rtl;')

                with ui.row().classes('items-center gap-3'):
                    # Score badges
                    max_color = 'green' if max_score > 70 else 'amber' if max_score > 40 else 'gray'
                    ui.badge(f"{tr('Max')}: {int(max_score)}", color=max_color).classes('text-xs')
                    avg_color = 'green' if avg_score > 60 else 'amber' if avg_score > 35 else 'gray'
                    ui.badge(f"{tr('Avg')}: {int(avg_score)}", color=avg_color).classes('text-xs')

            # All matches (initially visible in compact form)
            with ui.column().classes('w-full').style('background: var(--bg-secondary);'):
                for idx, item in enumerate(items):
                    create_parallel_item(idx, item, sys_id, shelfmark)

    def create_parallel_item(idx, item, sys_id, shelfmark):
        """Create a single parallel match item within a manuscript group."""
        # Use round() instead of int() to avoid hiding small boosts
        score = round(item.get('score', 0))
        final_score = round(item.get('final_score', score))
        has_boundary_matches = item.get('has_boundary_matches', False)
        boundary_quality = item.get('boundary_quality', 0)
        boundary_match_count = item.get('boundary_match_count', 0)

        # Format text snippets
        ms_text = html.escape(item.get('text', '').replace('\n', ' '))
        ms_text_html = re.sub(r'\*(.*?)\*', r'<span class="highlight-match">\1</span>', ms_text)

        src_text = html.escape(item.get('source_ctx', '').replace('\n', ' '))
        src_text_html = re.sub(r'\*(.*?)\*', r'<span class="highlight-match">\1</span>', src_text)

        # Create short preview (first 80 chars)
        ms_text_clean = item.get('text', '').replace('*', '').replace('\n', ' ').strip()
        preview = (ms_text_clean[:80] + '...') if len(ms_text_clean) > 80 else ms_text_clean

        # Determine item styling based on boundary matches
        expansion_style = 'border-bottom: 1px solid var(--border-light);'
        if has_boundary_matches:
            expansion_style += ' background: rgba(255, 193, 7, 0.05);'  # Subtle amber highlight

        with ui.expansion().classes('w-full').style(expansion_style) as expansion:
            # Compact header (always visible)
            with expansion.add_slot('header'):
                with ui.row().classes('w-full items-center gap-3 py-2 px-4'):
                    ui.label(f"#{idx + 1}").classes('text-xs px-2 py-0.5 rounded').style(
                        'background: var(--bg-tertiary); color: var(--text-muted);'
                    )

                    # Score badge - show boost if applied
                    # Note: Raw scores are typically 100-10000+, not percentages
                    if final_score > score:
                        score_color = 'green' if final_score > 2000 else 'amber' if final_score > 500 else 'gray'
                        ui.badge(f"{score} → {final_score}", color=score_color).classes('text-xs')
                    else:
                        score_color = 'green' if score > 2000 else 'amber' if score > 500 else 'gray'
                        ui.badge(f"{score}", color=score_color).classes('text-xs')

                    # Boundary match indicator
                    if has_boundary_matches:
                        quality_pct = int(boundary_quality * 100)
                        ui.icon('link').classes('text-sm').style('color: var(--accent-amber);')
                        ui.badge(
                            f"{tr('Cross-paragraph')} ({quality_pct}%)",
                            color='amber'
                        ).classes('text-xs').tooltip(
                            f"{boundary_match_count} {tr('cross-paragraph matches')}"
                        )

                    # Preview snippet
                    ui.label(preview).classes('text-sm flex-grow').style(
                        'color: var(--text-secondary); direction: rtl; text-align: right;'
                    )

            # Expanded content (shown on click)
            with ui.column().classes('w-full p-4 gap-4').style('background: var(--bg-card);'):
                # Content comparison
                with ui.row().classes('w-full gap-3'):
                    # Source context
                    with ui.column().classes('flex-1 gap-2'):
                        ui.label(tr('Your Text')).classes('text-xs font-bold uppercase').style('color: var(--success);')
                        with ui.element('div').classes('p-3 rounded-lg text-sm').style(
                            'background: var(--bg-tertiary); direction: rtl; text-align: right; line-height: 1.8; border: 1px solid var(--success); color: var(--text-primary);'
                        ):
                            ui.html(src_text_html, sanitize=False)

                    # Manuscript match
                    with ui.column().classes('flex-1 gap-2'):
                        ui.label(tr('Manuscript Text')).classes('text-xs font-bold uppercase').style('color: var(--accent-amber);')
                        with ui.element('div').classes('p-3 rounded-lg text-sm').style(
                            'background: var(--bg-tertiary); direction: rtl; text-align: right; line-height: 1.8; border: 1px solid var(--accent-amber); color: var(--text-primary);'
                        ):
                            ui.html(ms_text_html, sanitize=False)

                # Action buttons
                with ui.row().classes('w-full gap-2 mt-2'):
                    if sys_id:
                        # Browse button
                        ui.button(
                            tr('Browse'),
                            icon='menu_book',
                            on_click=lambda: ui.navigate.to(f'/browse?sys_id={sys_id}')
                        ).props('flat dense size=sm color=primary')

                        # Metadata button
                        def show_metadata_dialog(sid=sys_id, shelf=shelfmark):
                            show_parallel_metadata(sid, shelf, item)

                        ui.button(
                            tr('Metadata'),
                            icon='info',
                            on_click=show_metadata_dialog
                        ).props('flat dense size=sm')

                        # Add to list button
                        def show_add_dialog(sid=sys_id, shelf=shelfmark):
                            from web.components import show_add_to_list_dialog
                            show_add_to_list_dialog(
                                sys_id=sid,
                                shelfmark=shelf,
                                lists_mgr=state.lists_mgr,
                                note_default='',
                                fl_id=None
                            )

                        ui.button(
                            tr('Add to List'),
                            icon='star',
                            on_click=show_add_dialog
                        ).props('flat dense size=sm').style('color: var(--accent-amber);')

                        # Edit and Comment buttons
                        ms_text_clean = item.get('text', '').replace('*', '').replace('\n', ' ').strip()
                        if ms_text_clean:
                            from web.components import create_edit_button, create_comment_button
                            create_edit_button(
                                document_id=sys_id,
                                page_number=1,  # Page unknown in parallels
                                original_text=ms_text_clean,
                                shelfmark=shelfmark,
                                size='sm'
                            )
                            create_comment_button(
                                document_id=sys_id,
                                page_number=1,
                                shelfmark=shelfmark,
                                size='sm'
                            )

    def show_parallel_metadata(sys_id, shelfmark, item):
        """Show metadata dialog for a parallel result."""
        # Get full metadata
        title = ''
        if sys_id and state.meta_mgr:
            try:
                _, title_temp = state.meta_mgr.get_meta_for_id(sys_id)
                title = title_temp or ''
            except Exception:
                pass

        with ui.dialog() as dialog, ui.card().classes('p-6 min-w-96 max-w-2xl'):
            # Changed to H3
            h3(tr('Metadata'), classes='text-xl font-bold mb-4')

            with ui.column().classes('w-full gap-3'):
                metadata_items = [
                    (tr('Shelfmark'), shelfmark),
                    (tr('Title'), title or tr('Not available')),
                    (tr('System ID'), sys_id or tr('Not available')),
                    (tr('Score'), str(int(item.get('score', 0)))),
                ]

                for label, value in metadata_items:
                    with ui.row().classes('w-full items-start gap-4'):
                        ui.label(label + ':').classes('font-bold w-32').style('color: var(--text-secondary);')
                        ui.label(value).classes('flex-grow').style('color: var(--text-primary); direction: rtl;')

            with ui.row().classes('w-full justify-end gap-2 mt-6'):
                ui.button(tr('Close'), on_click=dialog.close).classes('btn-primary')

        dialog.open()

    def show_add_to_list_dialog_parallel(sys_id, shelfmark):
        """Show add to list dialog for a parallel result."""
        from web.components import show_add_to_list_dialog
        show_add_to_list_dialog(
            sys_id=sys_id,
            shelfmark=shelfmark,
            lists_mgr=state.lists_mgr,
            note_default='',
            fl_id=None
        )

    def extract_shelfmark(item):
        raw_header = item.get('raw_header', '')
        if raw_header and state.meta_mgr:
            try:
                sys_match = re.search(r'(99\d{8,})', raw_header)
                if sys_match:
                    sys_id = sys_match.group(1)
                    shelf, _ = state.meta_mgr.get_meta_for_id(sys_id)
                    return shelf or 'Unknown'
            except Exception:
                pass
        return 'Unknown'

    def create_result_card(idx, item):
        score = int(item.get('score', 0))
        raw_header = item.get('raw_header', '')

        # Extract metadata
        sys_id = None
        shelfmark = 'Unknown'
        title = ''

        if raw_header and state.meta_mgr:
            try:
                sys_match = re.search(r'(99\d{8,})', raw_header)
                if sys_match:
                    sys_id = sys_match.group(1)
                    shelf_temp, title_temp = state.meta_mgr.get_meta_for_id(sys_id)
                    shelfmark = shelf_temp or shelfmark
                    title = title_temp or ''
            except Exception:
                pass

        # Format text snippets (escape HTML first to prevent XSS)
        ms_text = html.escape(item.get('text', '').replace('\n', ' '))
        ms_text_html = re.sub(r'\*(.*?)\*', r'<span class="highlight-match">\1</span>', ms_text)

        src_text = html.escape(item.get('source_ctx', '').replace('\n', ' '))
        src_text_html = re.sub(r'\*(.*?)\*', r'<span style="background: #bbf7d0; padding: 2px 4px; border-radius: 3px;">\1</span>', src_text)

        with ui.card().classes('w-full p-5 hover:shadow-lg transition-all'):
            # Header row
            with ui.row().classes('w-full items-start justify-between mb-4'):
                with ui.column().classes('gap-1'):
                    with ui.row().classes('items-center gap-3'):
                        ui.label(f"#{idx + 1}").classes('text-xs px-2 py-1 rounded').style(
                            'background: var(--bg-tertiary); color: var(--text-muted);'
                        )
                        ui.label(shelfmark).classes('text-lg font-bold').style('color: var(--primary-700);')
                    if title:
                        title_short = (title[:80] + '...') if len(title) > 80 else title
                        ui.label(title_short).classes('text-sm').style('color: var(--text-secondary); direction: rtl;')

                # Score badge
                score_color = 'green' if score > 70 else 'amber' if score > 40 else 'gray'
                ui.badge(f"{tr('Score')}: {score}", color=score_color).classes('text-sm')

            # Content comparison
            with ui.row().classes('w-full gap-4'):
                # Source context
                with ui.column().classes('flex-1 gap-2'):
                    ui.label(tr('Source Context')).classes('text-xs font-bold uppercase').style('color: var(--success);')
                    with ui.element('div').classes('p-4 rounded-lg text-sm').style(
                        'background: var(--bg-tertiary); direction: rtl; text-align: right; line-height: 1.8; border: 1px solid var(--success); color: var(--text-primary);'
                    ):
                        ui.html(src_text_html, sanitize=False)

                # Manuscript match
                with ui.column().classes('flex-1 gap-2'):
                    ui.label(tr('Manuscript Match')).classes('text-xs font-bold uppercase').style('color: var(--accent-amber);')
                    with ui.element('div').classes('p-4 rounded-lg text-sm').style(
                        'background: var(--bg-tertiary); direction: rtl; text-align: right; line-height: 1.8; border: 1px solid var(--accent-amber); color: var(--text-primary);'
                    ):
                        ui.html(ms_text_html, sanitize=False)

            # Actions
            with ui.row().classes('w-full gap-2 mt-4 pt-4').style('border-top: 1px solid var(--border-light);'):
                if sys_id:
                    ui.button(
                        tr('View manuscript'),
                        icon='menu_book',
                        on_click=lambda sid=sys_id: ui.navigate.to(f'/browse?sys_id={sid}')
                    ).props('flat dense').style('color: var(--primary-700);')

                ui.button(
                    icon='star_border',
                    on_click=lambda i=item, s=shelfmark, t=title, sid=sys_id: add_to_list(i, s, t, sid)
                ).props('flat round dense').style('color: var(--accent-amber);').tooltip(tr('Add to List'))

                # Edit and Comment buttons
                ms_text_clean = item.get('text', '').replace('*', '').replace('\n', ' ').strip()
                if ms_text_clean and sys_id:
                    from web.components import create_edit_button, create_comment_button
                    create_edit_button(
                        document_id=sys_id,
                        page_number=1,  # Page unknown in parallels
                        original_text=ms_text_clean,
                        shelfmark=shelfmark,
                        size='sm'
                    )
                    create_comment_button(
                        document_id=sys_id,
                        page_number=1,
                        shelfmark=shelfmark,
                        size='sm'
                    )

    def add_to_list(item, shelfmark, title, sys_id):
        from web.components import show_add_to_list_dialog
        show_add_to_list_dialog(
            sys_id=sys_id,
            shelfmark=shelfmark,
            lists_mgr=state.lists_mgr,
            note_default='',
            fl_id=None
        )

    # Sort change handler
    sort_select.on('update:model-value', lambda: render_results(p_state.results) if p_state.results else None)

    # Initialize with restored results
    if p_state.results:
        results_header.text = f"{len(p_state.results)} {tr('parallels found')}"
        render_results(p_state.results)

    # Async function to restore filter sources from persistent storage
    async def restore_filter_sources():
        """Restore filter sources from cache files (async to avoid blocking)."""
        stored_refs = app.storage.user.get('filter_sources_refs', [])
        stored_enabled = set(app.storage.user.get('filter_sources_enabled', []))
        stored_custom = app.storage.user.get('filter_sources_custom', {})
        filter_sources['custom_count'] = app.storage.user.get('filter_sources_custom_count', 0)

        # Restore custom texts immediately (they're already in storage)
        for ref, text in stored_custom.items():
            filter_sources['loaded'][ref] = text
            if ref in stored_enabled:
                filter_sources['enabled'].add(ref)

        total_to_load = len(stored_refs) + len(stored_custom)
        print(f"[DEBUG] Restore: found {len(stored_refs)} Sefaria refs, {len(stored_custom)} custom, {len(stored_enabled)} enabled")

        if not stored_refs:
            filter_sources['pending_restore'] = False
            try:
                refresh_loaded_sources_ui()  # Show current state (may include custom texts)
            except Exception:
                pass  # Client may have been deleted
            return

        # Show loading indicator
        try:
            sefaria_progress.style('display: block;')
            sefaria_status.style('display: block;')
            sefaria_progress.value = 0
            sefaria_status.text = tr('Loading: {}').format(f"0/{len(stored_refs)}")
        except Exception:
            return  # Client deleted, abort

        # Load Sefaria refs from cache (in background thread)
        loaded_count = len(stored_custom)  # Count custom texts already loaded
        for i, ref in enumerate(stored_refs):
            # Check if client is still valid before each iteration
            try:
                _ = sefaria_progress.client
            except (RuntimeError, Exception):
                print("[DEBUG] Client deleted, aborting restore")
                return

            text = await run.io_bound(fetch_sefaria_text, ref, True)
            if text:
                filter_sources['loaded'][ref] = text
                if ref in stored_enabled:
                    filter_sources['enabled'].add(ref)
                loaded_count += 1

            # Update UI with error handling
            try:
                sefaria_progress.value = (i + 1) / len(stored_refs)
                sefaria_status.text = tr('Loading: {}').format(f"{i+1}/{len(stored_refs)}")
            except (RuntimeError, Exception):
                print("[DEBUG] Client deleted during restore, aborting")
                return

        # Update UI
        filter_sources['pending_restore'] = False
        try:
            sefaria_progress.style('display: none;')
            sefaria_status.style('display: none;')
            refresh_loaded_sources_ui()

            if loaded_count > 0:
                print(f"[DEBUG] Restored {loaded_count}/{len(stored_refs)} filter sources from cache")
        except (RuntimeError, Exception):
            pass  # Client deleted

    # Schedule async restore on page load
    ui.timer(0.1, restore_filter_sources, once=True)
