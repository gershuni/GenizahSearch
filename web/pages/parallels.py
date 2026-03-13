# -*- coding: utf-8 -*-
"""
Parallels Search Page - Dicta Genizah Search

Find parallel texts in the Genizah corpus using:
- Shmidman-Koppel-Porat fingerprinting algorithm
- Configurable chunk size
- Advanced filtering options
"""

import logging

from nicegui import ui, run, app
from web.state import state
from web.translations import tr, get_language
from urllib.parse import unquote
import asyncio
import re
import html
import os
import time
import requests
from datetime import datetime
from web.components.typography import h1, h2, h3, h4

logger = logging.getLogger(__name__)

# Import Sefaria sources and text cleaning from the shared sefaria_utils module (no PyQt6 dependency)
from sefaria_utils import SEFARIA_SOURCES, clean_hebrew_text, get_cache_dir, get_sefaria_library

# Import shared sanitization utility
from shared_export_utils import sanitize_cache_filename as _sanitize_cache_filename


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
        logger.error(f"Timeout fetching {ref}")
    except Exception as e:
        logger.error(f"Error fetching {ref}: {e}")

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
            self.filtered_results = []
            self.finished_animation_shown = False
            self.update_timer = None  # Track timer to prevent duplicates
            # Search timing state
            self.search_start_time: float = 0.0
            self.chunks_processed: int = 0
            self.chunks_total: int = 0
            self.last_eta_update: float = 0.0
            self.last_eta_text: str = ""
            # Domain filter state
            self.all_result_domains: dict = {}  # sys_id -> list of domain names
            self.domain_exclusions: set = set()
            self.has_domain_data: bool = False
            self.domain_name_map: dict = {}  # English domain name -> Hebrew name
            self.domain_hierarchy: dict = {}  # cached hierarchy from get_domain_hierarchy()
            self.printed_ids: set = set()  # sys_ids with FragmentMaterial=Printed
            # Pre-search filter state (Advanced Filters panel)
            self.filter_domains: list = []
            self.filter_authors: list = []
            self.filter_works: list = []
            self.filter_include_mode: bool = True
            self.filter_date_from: int = None
            self.filter_date_to: int = None
            self.filter_material_exclude: list = []
            self.filter_text_all: list = []
            self.filter_text_any: list = []
            self.filter_text_not: list = []
            self.filter_manuscript_count: int = None
            self.restrict_sys_ids: set = None
            self.excluded_manuscript_ids: set = set()   # Per-manuscript exclusions (NEW for web)
            self.auto_excluded_source_id: str = None    # Auto-excluded source manuscript
            # Translation enrichment (Phase 46-07)
            self.title_translations: dict = {}  # sys_id -> {hebrew_title, english_title, ...}
            self.translation_data: dict = {}  # sys_id -> {description_he, document_type_he}

    p_state = ParallelsState()

    def _persist(key, value):
        """Save to storage if session persistence is enabled."""
        if app.storage.user.get('session_persistence_enabled', True):
            app.storage.user[key] = value

    # --- Incoming filters from catalog browse (Path B: browse -> parallels) ---
    _filters_from_browse = False
    if app.storage.user.get('incoming_filters'):
        incoming = app.storage.user.get('incoming_filters', {})
        if incoming:
            if incoming.get('domain'):
                p_state.filter_domains = [incoming['domain']]
                _persist('parallels_filter_domains', p_state.filter_domains)
            if incoming.get('author'):
                p_state.filter_authors = [str(incoming['author'])]
                _persist('parallels_filter_authors', p_state.filter_authors)
            if incoming.get('work'):
                p_state.filter_works = [str(incoming['work'])]
                _persist('parallels_filter_works', p_state.filter_works)
            if incoming.get('date_from') is not None:
                p_state.filter_date_from = int(incoming['date_from'])
                _persist('parallels_filter_date_from', int(incoming['date_from']))
            if incoming.get('date_to') is not None:
                p_state.filter_date_to = int(incoming['date_to'])
                _persist('parallels_filter_date_to', int(incoming['date_to']))
            if incoming.get('material_exclude'):
                p_state.filter_material_exclude = incoming['material_exclude']
                _persist('parallels_filter_material_exclude', incoming['material_exclude'])
            # Clear incoming_filters from storage after consuming
            app.storage.user.pop('incoming_filters', None)
            _filters_from_browse = True

    # Restore filter state from session (only if NOT from browse, browse takes priority)
    if not _filters_from_browse:
        # Migrate from legacy single-value keys to multi-select lists
        _legacy_domain = app.storage.user.get('parallels_filter_domain', None)
        _legacy_author = app.storage.user.get('parallels_filter_author', None)
        _legacy_work = app.storage.user.get('parallels_filter_work', None)
        _fd = app.storage.user.get('parallels_filter_domains')
        p_state.filter_domains = _fd if _fd is not None else ([_legacy_domain] if _legacy_domain else [])
        _fa = app.storage.user.get('parallels_filter_authors')
        p_state.filter_authors = _fa if _fa is not None else ([_legacy_author] if _legacy_author else [])
        _fw = app.storage.user.get('parallels_filter_works')
        p_state.filter_works = _fw if _fw is not None else ([_legacy_work] if _legacy_work else [])
        p_state.filter_include_mode = app.storage.user.get('parallels_filter_include_mode', True)
        p_state.filter_date_from = app.storage.user.get('parallels_filter_date_from', None)
        p_state.filter_date_to = app.storage.user.get('parallels_filter_date_to', None)
        _fme = app.storage.user.get('parallels_filter_material_exclude')
        p_state.filter_material_exclude = _fme if _fme is not None else []
        _fta = app.storage.user.get('parallels_filter_text_all')
        p_state.filter_text_all = _fta if _fta is not None else []
        _ftany = app.storage.user.get('parallels_filter_text_any')
        p_state.filter_text_any = _ftany if _ftany is not None else []
        _ftn = app.storage.user.get('parallels_filter_text_not')
        p_state.filter_text_not = _ftn if _ftn is not None else []

    # Restore per-manuscript exclusions from session
    _emi = app.storage.user.get('parallels_excluded_manuscript_ids')
    p_state.excluded_manuscript_ids = set(_emi) if _emi is not None else set()

    def _has_active_filters() -> bool:
        """Check if any pre-search filters are active."""
        return any([
            p_state.filter_domains,
            p_state.filter_authors,
            p_state.filter_works,
            p_state.filter_date_from is not None,
            p_state.filter_date_to is not None,
            p_state.filter_material_exclude,
            p_state.filter_text_all,
            p_state.filter_text_any,
            p_state.filter_text_not,
        ])

    # Restore domain exclusions for parallels
    _pde = app.storage.user.get('parallels_domain_exclusions')
    p_state.domain_exclusions = set(_pde) if _pde is not None else set()

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
    source_sys_id = None  # For auto-exclude source manuscript
    if initial_text:
        try:
            decoded_text = unquote(initial_text)
            # Try to determine source sys_id from initial_text URL params
            # The initial_text might encode sys_id info, or we check query params
            # NiceGUI query params are available via app.storage.user or client
        except Exception:
            decoded_text = initial_text
    else:
        # Try to restore from storage
        decoded_text = app.storage.user.get('parallels_source_text', '')

    # Auto-exclude source manuscript when launched from another module
    if initial_text and state.meta_mgr:
        try:
            # Try to find a sys_id reference in the URL text (e.g., "99NNN...")
            sys_match = re.search(r'(99\d{8,})', initial_text)
            if sys_match:
                source_sys_id = sys_match.group(1)
                p_state.auto_excluded_source_id = source_sys_id
                p_state.excluded_manuscript_ids.add(source_sys_id)
                _persist('parallels_excluded_manuscript_ids', list(p_state.excluded_manuscript_ids))
        except Exception:
            pass

    # --- Composition History Management ---
    def _get_comp_history() -> list:
        """Get composition search history from storage."""
        return app.storage.user.get('composition_history', [])

    def _add_to_comp_history(title: str, result_count: int, params: dict, state_snapshot: dict):
        """Add or update a composition history entry. Deduplicates by title."""
        if not app.storage.user.get('session_persistence_enabled', True):
            return
        limit = app.storage.user.get('search_history_limit', 20)
        history = _get_comp_history()

        # Dedup by title
        existing_idx = None
        for i, entry in enumerate(history):
            if entry.get('title') == title:
                existing_idx = i
                break

        entry = {
            'title': title,
            'result_count': result_count,
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
        app.storage.user['composition_history'] = history

    def _delete_comp_history_entry(index: int):
        """Delete a specific composition history entry by index."""
        history = _get_comp_history()
        if 0 <= index < len(history):
            history.pop(index)
            app.storage.user['composition_history'] = history

    def _clear_comp_history():
        """Clear all composition search history."""
        app.storage.user['composition_history'] = []

    # === UI Layout ===

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

                    # === Lab Mode and Boundary Search Settings (below text input) ===
                    ui.separator().classes('my-3')

                    # Lab Mode Toggle Row
                    with ui.row().classes('w-full items-center gap-4'):
                        lab_mode = ui.checkbox(tr('Lab Mode (experimental)'))
                        lab_mode.tooltip(tr('Advanced search using fingerprint algorithm. Slower but more features.'))

                        # Deep Scan (Lab Mode only - initially hidden)
                        deep_scan = ui.checkbox(tr('Deep Scan')).style('display: none;')
                        deep_scan.tooltip(tr('Exhaustive search - slower but finds more results'))

                    # === Boundary Search Settings ===
                    with ui.row().classes('w-full items-center gap-4 flex-wrap mt-2'):
                        # Paragraph delimiter (always editable - affects display even in full mode)
                        with ui.column().classes('gap-1') as delimiter_col:
                            delimiter_label = ui.label(tr('Paragraph separator')).classes('text-xs font-medium').style('color: var(--text-muted);')
                            boundary_delimiter = ui.select(
                                options={
                                    '\n': tr('Line break'),
                                    '\n\n': tr('Blank line (paragraph)'),
                                    '.': tr('Period (.)'),
                                    ':': tr('Colon (:)')
                                },
                                value='\n'
                            ).classes('w-40').props('outlined dense')
                            boundary_delimiter.tooltip(tr('Character or pattern that separates paragraphs in your text'))

                        # Boundary mode radio buttons (visible)
                        boundary_mode = ui.radio(
                            options={
                                'full': tr('Full search'),
                                'boundary': tr('Cross-paragraph only'),
                                'combined': tr('Full + Cross-paragraph boost')
                            },
                            value='full'
                        ).props('inline dense')

                        # Advanced settings button (initially hidden)
                        advanced_btn = ui.button(icon='tune', on_click=lambda: advanced_dialog.open()).props('flat dense').style('display: none;')
                        advanced_btn.tooltip(tr('Advanced cross-paragraph settings'))

                    # Help text for current selection
                    boundary_mode_help = ui.label('').classes('text-xs').style('color: var(--text-muted); display: none;')

                    # Tooltip descriptions for each mode
                    mode_tooltips = {
                        'full': tr('Search all text chunks regardless of paragraph breaks'),
                        'boundary': tr('Show only matches where the matching text spans a paragraph break in your source'),
                        'combined': tr('Search everything, but rank cross-paragraph matches higher')
                    }

                    def update_boundary_help():
                        mode = boundary_mode.value
                        if mode in mode_tooltips:
                            boundary_mode_help.text = mode_tooltips[mode]
                            boundary_mode_help.style('display: block;')
                        else:
                            boundary_mode_help.style('display: none;')

                    # Pre-search stats display
                    boundary_stats_label = ui.label('').classes('text-xs mt-1').style('color: var(--primary-600); display: none;')
                    boundary_warning_label = ui.label('').classes('text-xs mt-1').style('color: var(--error); display: none;')

                    # Advanced settings dialog
                    with ui.dialog() as advanced_dialog:
                        with ui.card().classes('p-4 w-96'):
                            h3(tr('Advanced cross-paragraph settings'), classes='text-lg font-bold mb-4', style='color: var(--text-primary);')

                            # Cross-paragraph boost slider
                            with ui.column().classes('gap-1 mb-4'):
                                ui.label(tr('Cross-paragraph boost')).classes('text-sm font-medium').style('color: var(--text-secondary);')
                                boundary_boost = ui.slider(min=1.0, max=3.0, value=1.5, step=0.1).props('label-always')
                                ui.label(tr('Score multiplier for cross-paragraph matches')).classes('text-xs').style('color: var(--text-muted);')

                            # Min boundary matches filter
                            with ui.column().classes('gap-1 mb-4'):
                                ui.label(tr('Min. cross-paragraph matches')).classes('text-sm font-medium').style('color: var(--text-secondary);')
                                min_boundary_matches = ui.select(
                                    options={i: str(i) for i in range(11)},
                                    value=0
                                ).classes('w-32').props('outlined dense')
                                ui.label(tr('Minimum number of cross-paragraph matches required')).classes('text-xs').style('color: var(--text-muted);')

                            # Min delimiter distance
                            with ui.column().classes('gap-1 mb-4'):
                                ui.label(tr('Min. words between separators')).classes('text-sm font-medium').style('color: var(--text-secondary);')
                                min_delimiter_distance = ui.select(
                                    options={i: str(i) for i in range(1, 11)},
                                    value=3
                                ).classes('w-32').props('outlined dense')
                                ui.label(tr('Ignore separators that are too close together')).classes('text-xs').style('color: var(--text-muted);')

                            ui.button(tr('Close'), on_click=advanced_dialog.close).props('flat')

                    def update_boundary_ui():
                        """Update boundary UI based on selected mode."""
                        is_boundary_mode = boundary_mode.value in ('boundary', 'combined')
                        # Show/hide advanced settings button based on mode
                        if is_boundary_mode:
                            advanced_btn.style('display: inline-flex;')
                        else:
                            advanced_btn.style('display: none;')
                        update_boundary_help()
                        update_boundary_stats()

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
                            delimiter = boundary_delimiter.value or '\n'
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
                    boundary_mode.on('update:model-value', update_boundary_ui)
                    boundary_delimiter.on('update:model-value', update_boundary_stats)
                    min_delimiter_distance.on('update:model-value', update_boundary_stats)
                    text_input.on('blur', update_boundary_stats)

                    # Lab Mode toggle handler
                    def on_lab_mode_change():
                        """Show/hide lab mode options based on toggle."""
                        if lab_mode.value:
                            deep_scan.style('display: inline-flex;')
                            freq_threshold_row.style('display: none;')
                            # Higher default for composition/lab mode
                            min_chunks_input.value = 3
                        else:
                            deep_scan.style('display: none;')
                            freq_threshold_row.style('display: block;')
                            # Lower default for regular mode
                            min_chunks_input.value = 1

                    lab_mode.on('update:model-value', on_lab_mode_change)

                    # Initialize help text
                    update_boundary_help()

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

                    # Min chunk matches (for regular full-text chunk search)
                    with ui.column().classes('gap-1') as min_chunks_row:
                        h3(tr('Min. chunk matches'), classes='text-sm font-medium', style='color: var(--text-secondary);')
                        min_chunks_input = ui.number(
                            min=1, max=20, value=1, step=1,
                            format='%d'
                        ).classes('w-24').props('outlined dense')
                        ui.label(tr('Minimum matching chunks per manuscript')).classes('text-xs').style('color: var(--text-muted);')

                    ui.separator().classes('my-2')

                    # Run Button + New Search Reset
                    with ui.row().classes('w-full items-center gap-2'):
                        run_btn = ui.button(
                            tr('Find Parallels'),
                            icon='compare_arrows',
                            on_click=lambda: execute_parallels()
                        ).classes('btn-primary flex-grow')

                        ui.button(icon='restart_alt', on_click=lambda: _reset_parallels()).props(
                            'flat dense round'
                        ).tooltip(tr('New Composition Search'))

                    # Stop Button (hidden by default) - shows partial results
                    with ui.column().classes('w-full items-center gap-0').style('display: none;') as cancel_btn:
                        ui.button(
                            tr('Stop'),
                            icon='stop',
                            on_click=lambda: cancel_search()
                        ).classes('w-full').props('outline color=red')
                        ui.label(tr('and show partial results')).classes('text-xs').style('color: var(--text-muted);')

                    # Progress - visible spinner + status in the control panel
                    with ui.linear_progress(0, show_value=False).classes('w-full my-2').style('height: 12px; opacity: 0;') as progress_bar:
                        ui.label().classes('absolute-center text-xs text-white').bind_text_from(
                            progress_bar, 'value', backward=lambda v: f'{round(v * 100)}%' if v > 0 else ''
                        )
                    with ui.row().classes('w-full items-center justify-center gap-2').style('display: none;') as search_indicator:
                        ui.spinner('dots', size='sm', color='primary')
                        status_label = ui.label('').classes('text-sm font-medium').style('color: var(--primary-600);')
                    # Summary label (stays visible after search completes, hidden during search)
                    summary_label = ui.label('').classes('text-sm font-medium text-center w-full').style('color: var(--primary-600);')

                    # Composition History Button + Menu
                    ui.separator().classes('my-1')
                    with ui.row().classes('w-full items-center justify-center'):
                        comp_history_btn = ui.button(
                            tr('Composition History'), icon='history',
                            on_click=lambda: (_refresh_comp_history_menu(), comp_history_menu.open())
                        ).props('flat dense no-caps').classes('text-sm')

                        comp_history_menu = ui.menu()

        # === Advanced Filters Panel (collapsible, below source input) ===
        _adv_filters_expanded = _has_active_filters() or _filters_from_browse
        adv_filters_panel = ui.expansion(
            text=tr('Search only in...'),
            icon='filter_alt',
            value=_adv_filters_expanded,
        ).classes('w-full').style(
            'background: var(--bg-tertiary); border-bottom: 1px solid var(--border-light);'
        ).props('dense header-class="text-subtitle2 text-weight-medium"')

        # References to filter UI elements
        _filter_refs = {}

        with adv_filters_panel:
            with ui.column().classes('w-full px-4 py-3 gap-4'):

                # Include/Exclude toggle
                with ui.row().classes('w-full items-center gap-2'):
                    p_filter_mode_toggle = ui.toggle(
                        {True: tr('Include'), False: tr('Exclude')},
                        value=p_state.filter_include_mode,
                    ).props('dense no-caps size=sm')
                    _filter_refs['mode'] = p_filter_mode_toggle

                with ui.row().classes('w-full gap-4 flex-wrap items-end'):
                    # Domain filter (multi-select)
                    with ui.column().classes('gap-1 min-w-48 flex-grow'):
                        ui.label(tr('Domain')).classes('text-xs font-medium').style('color: var(--text-secondary);')

                        def _build_domain_options():
                            """Build domain select options from FJMS hierarchy."""
                            from shared.fjms_service import get_fjms_service, qualify_domain_name
                            fjms = get_fjms_service(thread_safe=True)
                            if not fjms.is_available():
                                return {}
                            hierarchy = fjms.get_domain_hierarchy()
                            is_heb = get_language() == 'he'
                            options = {}
                            for parent_name, info in hierarchy.items():
                                parent_heb = info.get('parent_domain_heb', '')
                                parent_count = info.get('count', 0)
                                display = parent_heb if is_heb and parent_heb else parent_name
                                display += f" ({parent_count:,})"
                                options[parent_name] = display
                                for child in info.get('children', []):
                                    child_name = child.get('domain', '')
                                    child_heb = child.get('domain_heb', '')
                                    child_count = child.get('count', 0)
                                    qname = qualify_domain_name(child_name, parent_name)
                                    if is_heb and child_heb:
                                        c_label = f"{child_heb} ({parent_heb})" if qname != child_name else child_heb
                                    else:
                                        c_label = qname
                                    c_display = f"  \u2514 {c_label} ({child_count:,})"
                                    options[qname] = c_display
                                    # Third level: sub-sub-domains
                                    for sc in child.get('children', []):
                                        sc_name = sc.get('domain', '')
                                        sc_heb = sc.get('domain_heb', '')
                                        sc_count = sc.get('count', 0)
                                        sc_qname = qualify_domain_name(sc_name, child_name)
                                        if is_heb and sc_heb:
                                            sc_label = f"{sc_heb} ({child_heb})" if sc_qname != sc_name else sc_heb
                                        else:
                                            sc_label = sc_qname
                                        sc_display = f"    \u2514 {sc_label} ({sc_count:,})"
                                        options[sc_qname] = sc_display
                            return options

                        p_domain_select = ui.select(
                            options={},
                            value=p_state.filter_domains,
                            multiple=True,
                            with_input=True,
                            clearable=True,
                        ).classes('w-full').props('outlined dense use-chips')
                        _filter_refs['domain'] = p_domain_select

                    # Author filter (multi-select) — options loaded asynchronously after page renders
                    with ui.column().classes('gap-1 min-w-48 flex-grow'):
                        ui.label(tr('Author')).classes('text-xs font-medium').style('color: var(--text-secondary);')

                        def _build_author_options(domain=None):
                            """Build author select options from FJMS."""
                            from shared.fjms_service import get_fjms_service
                            fjms = get_fjms_service(thread_safe=True)
                            if not fjms.is_available():
                                return {}
                            _first_domain = domain[0] if isinstance(domain, list) and domain else (domain or None)
                            authors = fjms.get_browse_authors(domain=_first_domain)
                            options = {}
                            for a in authors:
                                pid = a.get('person_id') or a.get('author_id')
                                name = a.get('heb_desc') or a.get('eng_desc') or a.get('author_name', '')
                                eng = a.get('eng_desc', '')
                                count = a.get('count', 0)
                                key = str(pid) if pid else name
                                display = name
                                if eng and eng != name:
                                    display = f"{name} / {eng}"
                                display += f" ({count:,})"
                                options[key] = display
                            return options

                        p_author_select = ui.select(
                            options={},
                            value=p_state.filter_authors,
                            multiple=True,
                            with_input=True,
                            clearable=True,
                        ).classes('w-full').props('outlined dense use-chips')
                        _filter_refs['author'] = p_author_select

                    # Work filter (multi-select) — options loaded asynchronously after page renders
                    with ui.column().classes('gap-1 min-w-48 flex-grow'):
                        ui.label(tr('Work')).classes('text-xs font-medium').style('color: var(--text-secondary);')

                        def _build_work_options(domain=None, author=None):
                            """Build work select options from FJMS."""
                            from shared.fjms_service import get_fjms_service
                            fjms = get_fjms_service(thread_safe=True)
                            if not fjms.is_available():
                                return {}
                            _first_domain = domain[0] if isinstance(domain, list) and domain else (domain or None)
                            _first_author = author[0] if isinstance(author, list) and author else (author or None)
                            works = fjms.get_browse_works(domain=_first_domain, author=_first_author)
                            options = {}
                            for w in works:
                                tid = w.get('title_id')
                                org = w.get('org_title', '')
                                eng = w.get('eng_title', '')
                                count = w.get('count', 0)
                                key = str(tid) if tid else org
                                display = org or eng
                                if eng and eng != org:
                                    display = f"{org} / {eng}"
                                display += f" ({count:,})"
                                options[key] = display
                            return options

                        p_work_select = ui.select(
                            options={},
                            value=p_state.filter_works,
                            multiple=True,
                            with_input=True,
                            clearable=True,
                        ).classes('w-full').props('outlined dense use-chips')
                        _filter_refs['work'] = p_work_select

                with ui.row().classes('w-full gap-4 flex-wrap items-end'):
                    # Date range
                    with ui.column().classes('gap-1 min-w-32'):
                        ui.label(tr('Date Range')).classes('text-xs font-medium').style('color: var(--text-secondary);')
                        with ui.row().classes('items-center gap-2'):
                            p_date_from_input = ui.number(
                                label=tr('From Year'),
                                value=p_state.filter_date_from,
                            ).classes('w-28').props('outlined dense')
                            ui.label('\u2013').style('color: var(--text-muted);')
                            p_date_to_input = ui.number(
                                label=tr('To Year'),
                                value=p_state.filter_date_to,
                            ).classes('w-28').props('outlined dense')
                        _filter_refs['date_from'] = p_date_from_input
                        _filter_refs['date_to'] = p_date_to_input

                    # Material exclude (Printed)
                    with ui.column().classes('gap-1 min-w-48'):
                        ui.label(tr('Material')).classes('text-xs font-medium').style('color: var(--text-secondary);')
                        p_exclude_printed_cb = ui.checkbox(
                            tr('Exclude Printed'),
                            value='Printed' in p_state.filter_material_exclude,
                        ).props('dense')
                        _filter_refs['exclude_printed'] = p_exclude_printed_cb

                    # Import exclusions button
                    with ui.column().classes('gap-1 justify-end'):
                        with ui.row().classes('gap-2'):
                            def _import_exclusions_from_word_search():
                                """Import per-manuscript exclusions from word search."""
                                ws_excluded = app.storage.user.get('word_search_excluded_ids', [])
                                if not ws_excluded:
                                    ui.notify(tr('No word search exclusions to import'), type='info', timeout=2000)
                                    return
                                imported_count = 0
                                for sid in ws_excluded:
                                    if sid not in p_state.excluded_manuscript_ids:
                                        p_state.excluded_manuscript_ids.add(sid)
                                        imported_count += 1
                                _persist('parallels_excluded_manuscript_ids', list(p_state.excluded_manuscript_ids))
                                ui.notify(
                                    f"{tr('Imported')} {imported_count} {tr('exclusions from word search')}",
                                    type='positive', timeout=3000
                                )
                                _update_p_chip_bar()

                            ui.button(tr('Import exclusions'), icon='download',
                                      on_click=_import_exclusions_from_word_search).props('flat dense no-caps size=sm')

                    # Clear all filters button
                    with ui.column().classes('gap-1 justify-end'):
                        def _clear_all_p_adv_filters():
                            """Clear all advanced filter selections."""
                            p_state.filter_domains = []
                            p_state.filter_authors = []
                            p_state.filter_works = []
                            p_state.filter_include_mode = True
                            p_state.filter_date_from = None
                            p_state.filter_date_to = None
                            p_state.filter_material_exclude = []
                            p_state.filter_text_all = []
                            p_state.filter_text_any = []
                            p_state.filter_text_not = []
                            p_state.filter_manuscript_count = None
                            p_state.restrict_sys_ids = None
                            # Update UI elements
                            p_domain_select.value = []
                            p_author_select.value = []
                            p_work_select.value = []
                            p_filter_mode_toggle.value = True
                            p_date_from_input.value = None
                            p_date_to_input.value = None
                            p_exclude_printed_cb.value = False
                            if _filter_refs.get('text_input'):
                                _filter_refs['text_input'].value = ''
                            # Reset filter storage to clean defaults
                            app.storage.user['parallels_filter_domains'] = []
                            app.storage.user['parallels_filter_authors'] = []
                            app.storage.user['parallels_filter_works'] = []
                            app.storage.user['parallels_filter_include_mode'] = True
                            app.storage.user['parallels_filter_date_from'] = None
                            app.storage.user['parallels_filter_date_to'] = None
                            app.storage.user['parallels_filter_material_exclude'] = []
                            app.storage.user['parallels_filter_text_all'] = []
                            app.storage.user['parallels_filter_text_any'] = []
                            app.storage.user['parallels_filter_text_not'] = []
                            _update_p_chip_bar()

                        ui.button(tr('Clear All'), icon='clear_all',
                                  on_click=_clear_all_p_adv_filters).props('flat dense no-caps')

                # Text filter row
                with ui.row().classes('w-full gap-2 items-end'):
                    with ui.column().classes('gap-1 flex-grow'):
                        ui.label(tr('Text Filter')).classes('text-xs font-medium').style('color: var(--text-secondary);')
                        with ui.row().classes('items-center gap-2 w-full'):
                            p_text_mode_select = ui.select(
                                options={
                                    'all': tr('All words'),
                                    'any': tr('Any word'),
                                    'not': tr('Not these words'),
                                },
                                value='all',
                            ).classes('w-36').props('outlined dense')
                            _filter_refs['text_mode'] = p_text_mode_select

                            p_text_filter_input = ui.input(
                                placeholder=tr('Add term'),
                            ).classes('flex-grow').props('outlined dense').on(
                                'keydown.enter', lambda e: _add_p_text_term()
                            )
                            _filter_refs['text_input'] = p_text_filter_input

                            ui.button(icon='add', on_click=lambda: _add_p_text_term()).props('flat dense round')

                    # Display current text filter chips
                    with ui.row().classes('w-full gap-1 flex-wrap') as p_text_chip_row:
                        _filter_refs['text_chips'] = p_text_chip_row

                def _add_p_text_term():
                    """Add a text filter term from the input."""
                    term = p_text_filter_input.value.strip() if p_text_filter_input.value else ''
                    if not term:
                        return
                    mode = p_text_mode_select.value
                    if mode == 'all':
                        if term not in p_state.filter_text_all:
                            p_state.filter_text_all.append(term)
                    elif mode == 'any':
                        if term not in p_state.filter_text_any:
                            p_state.filter_text_any.append(term)
                    elif mode == 'not':
                        if term not in p_state.filter_text_not:
                            p_state.filter_text_not.append(term)
                    p_text_filter_input.value = ''
                    _persist('parallels_filter_text_all', p_state.filter_text_all)
                    _persist('parallels_filter_text_any', p_state.filter_text_any)
                    _persist('parallels_filter_text_not', p_state.filter_text_not)
                    asyncio.ensure_future(_recompute_p_filter_count())
                    _update_p_chip_bar()
                    _rebuild_p_text_chips()

                def _remove_p_text_term(mode, term):
                    """Remove a text filter term."""
                    target = getattr(p_state, f'filter_text_{mode}')
                    if term in target:
                        target.remove(term)
                    _persist(f'parallels_filter_text_{mode}', target)
                    asyncio.ensure_future(_recompute_p_filter_count())
                    _update_p_chip_bar()
                    _rebuild_p_text_chips()

                def _rebuild_p_text_chips():
                    """Rebuild text filter chip display."""
                    text_chip_row = _filter_refs.get('text_chips')
                    if not text_chip_row:
                        return
                    text_chip_row.clear()
                    with text_chip_row:
                        for t in p_state.filter_text_all:
                            ui.chip(f"+ {t}", icon='check_circle', removable=True,
                                    color='green-2', on_click=lambda: None,
                            ).on('remove', lambda _t=t: _remove_p_text_term('all', _t))
                        for t in p_state.filter_text_any:
                            ui.chip(f"~ {t}", icon='help_outline', removable=True,
                                    color='blue-2', on_click=lambda: None,
                            ).on('remove', lambda _t=t: _remove_p_text_term('any', _t))
                        for t in p_state.filter_text_not:
                            ui.chip(f"- {t}", icon='block', removable=True,
                                    color='red-2', on_click=lambda: None,
                            ).on('remove', lambda _t=t: _remove_p_text_term('not', _t))

        # --- Filter chip bar (always visible, even when panel is collapsed) ---
        p_chip_bar_container = ui.row().classes('w-full px-4 py-1 gap-2 items-center flex-wrap').style(
            'background: var(--bg-tertiary); border-bottom: 1px solid var(--border-light); min-height: 0; margin-bottom: 16px; position: relative; z-index: 1;'
        )
        p_chip_bar_container.set_visibility(False)

        def _get_p_display_name(key, opts_dict):
            """Extract display name from options dict (strip trailing count suffix only)."""
            if isinstance(opts_dict, dict) and key in opts_dict:
                import re
                # Strip only the trailing " (N,NNN)" count, preserving qualified names like "Other (Bible)"
                raw = opts_dict[key].lstrip(' \u2514').strip()
                return re.sub(r'\s*\([\d,]+\)\s*$', '', raw).strip()
            return key

        def _update_p_chip_bar():
            """Rebuild chip bar from current filter state."""
            p_chip_bar_container.clear()
            has_any = _has_active_filters()
            has_excl = bool(p_state.excluded_manuscript_ids)
            p_chip_bar_container.set_visibility(has_any or has_excl)
            if not has_any and not has_excl:
                return

            opts_d = p_domain_select.options if hasattr(p_domain_select, 'options') else {}
            opts_a = p_author_select.options if hasattr(p_author_select, 'options') else {}
            opts_w = p_work_select.options if hasattr(p_work_select, 'options') else {}

            with p_chip_bar_container:
                # Mode indicator
                if not p_state.filter_include_mode and (
                    p_state.filter_domains or p_state.filter_authors or p_state.filter_works
                ):
                    ui.chip(tr('Exclude selected'), icon='block', color='red-2')

                # Domain chips
                for d in p_state.filter_domains:
                    dname = _get_p_display_name(d, opts_d)
                    ui.chip(
                        dname, icon='category', removable=True,
                        on_click=lambda: None, color='deep-purple-2',
                    ).on('remove', lambda _d=d: _remove_p_filter('domain', _d))

                # Author chips
                for a in p_state.filter_authors:
                    aname = _get_p_display_name(a, opts_a)
                    ui.chip(
                        aname, icon='person', removable=True,
                        on_click=lambda: None, color='blue-2',
                    ).on('remove', lambda _a=a: _remove_p_filter('author', _a))

                # Work chips
                for w in p_state.filter_works:
                    wname = _get_p_display_name(w, opts_w)
                    ui.chip(
                        wname, icon='menu_book', removable=True,
                        on_click=lambda: None, color='teal-2',
                    ).on('remove', lambda _w=w: _remove_p_filter('work', _w))

                # Date range chip
                if p_state.filter_date_from is not None or p_state.filter_date_to is not None:
                    df = p_state.filter_date_from or '...'
                    dt = p_state.filter_date_to or '...'
                    ui.chip(
                        f"{df}\u2013{dt}", icon='date_range', removable=True,
                        on_click=lambda: None,
                        color='orange-2',
                    ).on('remove', lambda: _remove_p_filter('date'))

                # Material exclude chip
                if p_state.filter_material_exclude:
                    for mat in p_state.filter_material_exclude:
                        ui.chip(
                            f"{tr('Exclude')} {mat}", icon='block', removable=True,
                            on_click=lambda: None,
                            color='red-2',
                        ).on('remove', lambda m=mat: _remove_p_filter('material', m))

                # Text filter chips
                for t in p_state.filter_text_all:
                    ui.chip(f"+ {t}", icon='check_circle', removable=True,
                            color='green-2', on_click=lambda: None,
                    ).on('remove', lambda _t=t: _remove_p_text_term('all', _t))
                for t in p_state.filter_text_any:
                    ui.chip(f"~ {t}", icon='help_outline', removable=True,
                            color='blue-2', on_click=lambda: None,
                    ).on('remove', lambda _t=t: _remove_p_text_term('any', _t))
                for t in p_state.filter_text_not:
                    ui.chip(f"- {t}", icon='block', removable=True,
                            color='red-2', on_click=lambda: None,
                    ).on('remove', lambda _t=t: _remove_p_text_term('not', _t))

                # Per-manuscript exclusion count chip
                if p_state.excluded_manuscript_ids:
                    ui.chip(
                        f"{len(p_state.excluded_manuscript_ids)} {tr('excluded')}",
                        icon='remove_circle_outline',
                        color='grey-4',
                    )

                # Manuscript count badge
                if p_state.filter_manuscript_count is not None:
                    ui.label(
                        f"{p_state.filter_manuscript_count:,} {tr('manuscripts')}"
                    ).classes('text-xs px-2 py-0.5 rounded ml-2').style(
                        'background: var(--bg-tertiary); color: var(--text-secondary); border: 1px solid var(--border-light);'
                    )

        def _remove_p_filter(filter_type, value=None):
            """Remove a specific filter and update state."""
            if filter_type == 'domain':
                if value and value in p_state.filter_domains:
                    p_state.filter_domains.remove(value)
                else:
                    p_state.filter_domains = []
                p_domain_select.value = p_state.filter_domains
                _persist('parallels_filter_domains', p_state.filter_domains)
                asyncio.ensure_future(_refresh_p_author_options())
                asyncio.ensure_future(_refresh_p_work_options())
            elif filter_type == 'author':
                if value and value in p_state.filter_authors:
                    p_state.filter_authors.remove(value)
                else:
                    p_state.filter_authors = []
                p_author_select.value = p_state.filter_authors
                _persist('parallels_filter_authors', p_state.filter_authors)
                asyncio.ensure_future(_refresh_p_work_options())
            elif filter_type == 'work':
                if value and value in p_state.filter_works:
                    p_state.filter_works.remove(value)
                else:
                    p_state.filter_works = []
                p_work_select.value = p_state.filter_works
                _persist('parallels_filter_works', p_state.filter_works)
            elif filter_type == 'date':
                p_state.filter_date_from = None
                p_state.filter_date_to = None
                p_date_from_input.value = None
                p_date_to_input.value = None
                _persist('parallels_filter_date_from', None)
                _persist('parallels_filter_date_to', None)
            elif filter_type == 'material':
                if value and value in p_state.filter_material_exclude:
                    p_state.filter_material_exclude.remove(value)
                    _persist('parallels_filter_material_exclude', p_state.filter_material_exclude)
                    p_exclude_printed_cb.value = 'Printed' in p_state.filter_material_exclude
            asyncio.ensure_future(_recompute_p_filter_count())
            _update_p_chip_bar()

        _p_filter_refresh_seq = {'author': 0, 'work': 0}

        async def _refresh_p_author_options():
            """Refresh author select options based on current domain filter (async)."""
            _p_filter_refresh_seq['author'] += 1
            seq = _p_filter_refresh_seq['author']
            p_author_select.props('loading')
            new_opts = await run.io_bound(_build_author_options, p_state.filter_domains)
            if _p_filter_refresh_seq['author'] != seq:
                return  # Stale -- newer request in flight
            p_author_select.props(remove='loading')
            p_author_select.options = new_opts
            p_author_select.update()

        async def _refresh_p_work_options():
            """Refresh work select options based on current domain and author filters (async)."""
            _p_filter_refresh_seq['work'] += 1
            seq = _p_filter_refresh_seq['work']
            p_work_select.props('loading')
            new_opts = await run.io_bound(
                _build_work_options, p_state.filter_domains, p_state.filter_authors
            )
            if _p_filter_refresh_seq['work'] != seq:
                return  # Stale -- newer request in flight
            p_work_select.props(remove='loading')
            p_work_select.options = new_opts
            p_work_select.update()

        async def _recompute_p_filter_count():
            """Recompute manuscript count for current filters (background)."""
            if not _has_active_filters():
                p_state.filter_manuscript_count = None
                p_state.restrict_sys_ids = None
                return
            from shared.fjms_service import get_fjms_service

            include_mode = p_state.filter_include_mode
            _domains = p_state.filter_domains or None
            _authors = p_state.filter_authors or None
            _works = p_state.filter_works or None

            def _compute():
                fjms = get_fjms_service(thread_safe=True)
                if not fjms.is_available():
                    return None
                kwargs = dict(
                    date_from=p_state.filter_date_from,
                    date_to=p_state.filter_date_to,
                    material_exclude=p_state.filter_material_exclude or None,
                    text_all=p_state.filter_text_all or None,
                    text_any=p_state.filter_text_any or None,
                    text_not=p_state.filter_text_not or None,
                )
                if include_mode:
                    kwargs['domains'] = _domains
                    kwargs['authors'] = _authors
                    kwargs['works'] = _works
                else:
                    kwargs['domains_exclude'] = _domains
                    kwargs['authors_exclude'] = _authors
                    kwargs['works_exclude'] = _works
                return fjms.get_filter_sys_ids(**kwargs)

            result = await run.io_bound(_compute)
            if result is not None:
                p_state.filter_manuscript_count = len(result)
                p_state.restrict_sys_ids = result
            else:
                p_state.filter_manuscript_count = None
                p_state.restrict_sys_ids = None
            _update_p_chip_bar()

        # --- Filter change handlers ---
        def _on_p_domain_change(e=None):
            val = p_domain_select.value or []
            p_state.filter_domains = val if isinstance(val, list) else [val] if val else []
            _persist('parallels_filter_domains', p_state.filter_domains)
            asyncio.ensure_future(_refresh_p_author_options())
            asyncio.ensure_future(_refresh_p_work_options())
            asyncio.ensure_future(_recompute_p_filter_count())
            _update_p_chip_bar()

        def _on_p_author_change(e=None):
            val = p_author_select.value or []
            p_state.filter_authors = val if isinstance(val, list) else [val] if val else []
            _persist('parallels_filter_authors', p_state.filter_authors)
            asyncio.ensure_future(_refresh_p_work_options())
            asyncio.ensure_future(_recompute_p_filter_count())
            _update_p_chip_bar()

        def _on_p_work_change(e=None):
            val = p_work_select.value or []
            p_state.filter_works = val if isinstance(val, list) else [val] if val else []
            _persist('parallels_filter_works', p_state.filter_works)
            asyncio.ensure_future(_recompute_p_filter_count())
            _update_p_chip_bar()

        def _on_p_mode_change(e=None):
            p_state.filter_include_mode = p_filter_mode_toggle.value
            _persist('parallels_filter_include_mode', p_state.filter_include_mode)
            asyncio.ensure_future(_recompute_p_filter_count())
            _update_p_chip_bar()

        def _on_p_date_from_change(e=None):
            val = p_date_from_input.value
            p_state.filter_date_from = int(val) if val is not None and val != '' else None
            _persist('parallels_filter_date_from', p_state.filter_date_from)
            asyncio.ensure_future(_recompute_p_filter_count())
            _update_p_chip_bar()

        def _on_p_date_to_change(e=None):
            val = p_date_to_input.value
            p_state.filter_date_to = int(val) if val is not None and val != '' else None
            _persist('parallels_filter_date_to', p_state.filter_date_to)
            asyncio.ensure_future(_recompute_p_filter_count())
            _update_p_chip_bar()

        def _on_p_exclude_printed_change(e=None):
            if p_exclude_printed_cb.value:
                if 'Printed' not in p_state.filter_material_exclude:
                    p_state.filter_material_exclude.append('Printed')
            else:
                if 'Printed' in p_state.filter_material_exclude:
                    p_state.filter_material_exclude.remove('Printed')
            _persist('parallels_filter_material_exclude', p_state.filter_material_exclude)
            asyncio.ensure_future(_recompute_p_filter_count())
            _update_p_chip_bar()

        # Wire up change handlers
        p_domain_select.on('update:model-value', _on_p_domain_change)
        p_author_select.on('update:model-value', _on_p_author_change)
        p_work_select.on('update:model-value', _on_p_work_change)
        p_filter_mode_toggle.on('update:model-value', _on_p_mode_change)
        p_date_from_input.on('blur', _on_p_date_from_change)
        p_date_to_input.on('blur', _on_p_date_to_change)
        p_exclude_printed_cb.on('update:model-value', _on_p_exclude_printed_change)

        # Initialize chip bar on page load
        _update_p_chip_bar()

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
                    # Domain filter button (hidden until search with domain data)
                    p_domain_filter_btn = ui.button(
                        tr('Filter by domains'), icon='category',
                        on_click=lambda: _open_parallels_domain_filter_dialog()
                    ).classes('text-sm').props('outline dense no-caps')
                    p_domain_filter_btn.set_visibility(False)

                    # Restore visibility if stored exclusions exist
                    if p_state.domain_exclusions:
                        p_domain_filter_btn.set_visibility(True)
                        n_excl = len(p_state.domain_exclusions)
                        p_domain_filter_btn.text = f"{tr('Filter by domains')} ({n_excl} {tr('excluded')})"
                        p_domain_filter_btn.props('outline dense no-caps color=red')

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
        except Exception:
            pass

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
            # Client deleted, deactivate the timer
            if p_state.update_timer:
                p_state.update_timer.deactivate()
            return

        try:
            if p_state.is_running:
                run_btn.disable()
                cancel_btn.style('display: flex;')
                search_indicator.style('display: flex;')
                summary_label.text = ''  # Clear summary during active search
                progress_bar.style('opacity: 1;')
                progress_bar.set_value(p_state.progress)
                # Compute elapsed time + ETA
                elapsed = time.time() - p_state.search_start_time if p_state.search_start_time else 0
                if elapsed >= 3600:
                    elapsed_str = f"{int(elapsed // 3600)}:{int((elapsed % 3600) // 60):02d}:{int(elapsed % 60):02d}"
                else:
                    elapsed_str = f"{int(elapsed // 60)}:{int(elapsed % 60):02d}"
                # Build status with elapsed, chunk count, and ETA
                chunks_str = ""
                eta_str = ""
                if p_state.chunks_total > 0:
                    chunks_str = f"{p_state.chunks_processed}/{p_state.chunks_total} {tr('chunks')}"
                    # Compute ETA with 2-second smoothing
                    now = time.time()
                    if p_state.chunks_processed > 0 and elapsed > 0:
                        if (now - p_state.last_eta_update) >= 2.0:
                            rate = p_state.chunks_processed / elapsed
                            remaining = (p_state.chunks_total - p_state.chunks_processed) / rate
                            if remaining >= 3600:
                                eta_str = f"\u223c{int(remaining // 3600)}:{int((remaining % 3600) // 60):02d}:{int(remaining % 60):02d} {tr('remaining')}"
                            else:
                                eta_str = f"\u223c{int(remaining // 60)}:{int(remaining % 60):02d} {tr('remaining')}"
                            p_state.last_eta_text = eta_str
                            p_state.last_eta_update = now
                        else:
                            eta_str = p_state.last_eta_text
                # Assemble status line
                parts = [elapsed_str]
                if chunks_str:
                    parts.append(chunks_str)
                if eta_str:
                    parts.append(eta_str)
                status_label.text = " \u2014 ".join(parts)
            else:
                run_btn.enable()
                cancel_btn.style('display: none;')
                search_indicator.style('display: none;')
                if p_state.progress >= 1.0 and not p_state.finished_animation_shown:
                    progress_bar.set_value(1.0)
                    p_state.finished_animation_shown = True
                    # Don't auto-hide progress bar -- summary stays visible until next search
        except (RuntimeError, Exception):
            pass  # Client may be deleted

    # Use faster timer for more responsive progress updates
    # Cancel any existing timer first to prevent duplicates
    if p_state.update_timer:
        p_state.update_timer.deactivate()
    p_state.update_timer = ui.timer(0.05, update_ui)

    def cancel_search():
        p_state.is_cancelled = True
        p_state.status = tr('Cancelling...')
        # Hide top page loading bar on cancel
        ui.run_javascript('if (window.__hideLoadingBar) window.__hideLoadingBar();')

    # --- Composition History UI Helpers ---
    def _refresh_comp_history_menu():
        """Refresh the composition history dropdown menu contents."""
        comp_history_menu.clear()
        history = _get_comp_history()
        if not history:
            with comp_history_menu:
                ui.menu_item(tr('No composition history')).props('disable')
            return

        def _build_web_filter_summary(filters: dict, max_len: int = 50) -> str:
            """Build compact filter summary like [כולל: תנ״ך, תוספתא. 1000-1300]."""
            if not filters:
                return ''
            prefix = tr('include') if filters.get('include_mode', True) else tr('exclude')
            # Build en->heb domain name map from cached hierarchy (handles qualified names & 3rd level)
            domain_heb_map = {}
            if get_language() == 'he' and filters.get('domains'):
                try:
                    from shared.fjms_service import get_fjms_service, qualify_domain_name
                    fjms = get_fjms_service(thread_safe=True)
                    if fjms.is_available():
                        for pn, info in fjms.get_domain_hierarchy().items():
                            p_heb = info.get('parent_domain_heb', '')
                            if p_heb:
                                domain_heb_map[pn] = p_heb
                            for ch in info.get('children', []):
                                c_heb = ch.get('domain_heb', '')
                                qn = qualify_domain_name(ch['domain'], pn)
                                if c_heb:
                                    domain_heb_map[qn] = f"{c_heb} ({p_heb})" if qn != ch['domain'] else c_heb
                                for sc in ch.get('children', []):
                                    s_heb = sc.get('domain_heb', '')
                                    sq = qualify_domain_name(sc['domain'], ch['domain'])
                                    if s_heb:
                                        domain_heb_map[sq] = f"{s_heb} ({c_heb})" if sq != sc['domain'] else s_heb
                except Exception:
                    pass
            parts = []
            for d in filters.get('domains', []):
                parts.append(domain_heb_map.get(str(d), str(d)))
            n_auth = len(filters.get('authors', []))
            if n_auth:
                parts.append(f"{tr('Author')} \u00d7{n_auth}")
            n_work = len(filters.get('works', []))
            if n_work:
                parts.append(f"{tr('Work')} \u00d7{n_work}")
            df, dt = filters.get('date_from'), filters.get('date_to')
            if df and dt:
                parts.append(f"{df}-{dt}")
            elif df:
                parts.append(f"{df}+")
            elif dt:
                parts.append(f"-{dt}")
            if filters.get('material_exclude'):
                parts.append(tr("No printed"))
            elif filters.get('material_include'):
                parts.append(tr("Printed only"))
            if not parts:
                return ''
            summary = f"[{prefix}: {', '.join(parts)}]"
            if len(summary) > max_len:
                summary = summary[:max_len - 4] + '...]'
            return summary

        with comp_history_menu:
            for i, entry in enumerate(history):
                title_text = entry.get('title', '')
                title_display = (title_text[:40] + '...') if len(title_text) > 40 else title_text
                count = entry.get('result_count', 0)
                # Build filter summary text from params
                filters = entry.get('params', {}).get('filters')
                filter_text = _build_web_filter_summary(filters) if filters else ''
                label = f"{title_display}  ({count})"

                idx = i  # Capture for closure
                with ui.menu_item(label, on_click=lambda e, idx=idx: _on_comp_history_clicked(idx)).style('direction: rtl;'):
                    if filter_text:
                        ui.label(filter_text).style('font-size: 0.7rem; color: var(--primary-600); direction: ltr;')
                    # Delete button on each item
                    ui.button(icon='close', on_click=lambda e, idx=idx: (
                        _delete_comp_history_entry(idx), _refresh_comp_history_menu()
                    )).props('flat dense size=xs round').classes('ml-auto')

            ui.separator()
            ui.menu_item(tr('Clear all'), on_click=lambda: (
                _clear_comp_history(), _refresh_comp_history_menu()
            ))

    async def _on_comp_history_clicked(index: int):
        """Restore state from a composition history entry."""
        history = _get_comp_history()
        if index >= len(history):
            return
        entry = history[index]
        state_snapshot = entry.get('state', {})
        params = entry.get('params', {})

        # Restore source text
        if state_snapshot.get('source_text'):
            text_input.value = state_snapshot['source_text']

        # Restore filter state from history entry
        filters = params.get('filters')
        if filters and isinstance(filters, dict):
            # Migrate from legacy single-value to lists
            _d = filters.get('domains') or ([filters['domain']] if filters.get('domain') else [])
            _a = filters.get('authors') or ([filters['author']] if filters.get('author') else [])
            _w = filters.get('works') or ([filters['work']] if filters.get('work') else [])
            p_state.filter_domains = _d
            p_state.filter_authors = _a
            p_state.filter_works = _w
            p_state.filter_include_mode = filters.get('include_mode', True)
            p_state.filter_date_from = filters.get('date_from')
            p_state.filter_date_to = filters.get('date_to')
            p_state.filter_material_exclude = filters.get('material_exclude', [])
            p_state.filter_text_all = filters.get('text_all', [])
            p_state.filter_text_any = filters.get('text_any', [])
            p_state.filter_text_not = filters.get('text_not', [])
            # Update filter UI elements
            p_domain_select.value = p_state.filter_domains
            p_author_select.value = p_state.filter_authors
            p_work_select.value = p_state.filter_works
            p_filter_mode_toggle.value = p_state.filter_include_mode
            p_date_from_input.value = p_state.filter_date_from
            p_date_to_input.value = p_state.filter_date_to
            p_exclude_printed_cb.value = 'Printed' in p_state.filter_material_exclude
            # Persist restored filters
            _persist('parallels_filter_domains', p_state.filter_domains)
            _persist('parallels_filter_authors', p_state.filter_authors)
            _persist('parallels_filter_works', p_state.filter_works)
            _persist('parallels_filter_include_mode', p_state.filter_include_mode)
            _persist('parallels_filter_date_from', p_state.filter_date_from)
            _persist('parallels_filter_date_to', p_state.filter_date_to)
            _persist('parallels_filter_material_exclude', p_state.filter_material_exclude)
            _persist('parallels_filter_text_all', p_state.filter_text_all)
            _persist('parallels_filter_text_any', p_state.filter_text_any)
            _persist('parallels_filter_text_not', p_state.filter_text_not)
            _update_p_chip_bar()
            _rebuild_p_text_chips()
        else:
            # Clear filters if history entry had none
            _clear_all_p_adv_filters()

        # Restore per-manuscript exclusions
        if state_snapshot.get('excluded_manuscript_ids'):
            p_state.excluded_manuscript_ids = set(state_snapshot['excluded_manuscript_ids'])
            _persist('parallels_excluded_manuscript_ids', list(p_state.excluded_manuscript_ids))
            _update_p_chip_bar()

        # Restore results and state from snapshot
        if state_snapshot.get('results'):
            p_state.results = state_snapshot['results']
            p_state.filtered_results = state_snapshot.get('filtered_results', [])
            p_state.domain_exclusions = set(state_snapshot.get('domain_exclusions', []))

            # Update global state for export
            state.parallels_results = p_state.results
            state.parallels_filtered = p_state.filtered_results

            # Update header and render
            results_header.text = f"{len(p_state.results)} {tr('parallels found')}"
            render_results(p_state.results, p_state.filtered_results)

        ui.notify(tr('Composition restored from history'), type='info', timeout=2000)
        comp_history_menu.close()

    def _reset_parallels():
        """Reset all composition search state, clear results, filters, exclusions, and persistent storage."""
        # Clear source text
        text_input.value = ''
        # Clear results
        p_state.results = []
        p_state.filtered_results = []
        p_state.domain_exclusions = set()
        p_state.excluded_manuscript_ids = set()
        p_state.printed_ids = set()
        p_state.is_running = False
        p_state.is_cancelled = False
        p_state.progress = 0
        p_state.chunks_processed = 0
        p_state.chunks_total = 0
        # Clear pre-search filters
        _clear_all_p_adv_filters()
        # Clear results container
        results_container.clear()
        with results_container:
            with ui.column().classes('w-full h-64 items-center justify-center'):
                ui.icon('compare_arrows').classes('text-6xl').style('color: var(--text-muted);')
                ui.label(tr('Enter text to search for parallels')).classes('mt-4').style('color: var(--text-muted);')
        # Reset results header
        results_header.text = tr('Results')
        # Reset summary label
        summary_label.text = ''
        # Reset persistent storage to clean defaults
        app.storage.user['parallels_results'] = []
        app.storage.user['parallels_filtered'] = []
        app.storage.user['parallels_source_text'] = ''
        app.storage.user['parallels_domain_exclusions'] = []
        app.storage.user['parallels_excluded_manuscript_ids'] = []
        # Also clear from global state
        state.parallels_results = []
        state.parallels_filtered = []
        ui.notify(tr('Composition reset'), type='info', timeout=2000)

    async def execute_parallels():
        # Prevent duplicate executions
        if p_state.is_running:
            return

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
        p_state.search_start_time = time.time()
        p_state.chunks_processed = 0
        p_state.chunks_total = 0
        p_state.last_eta_update = 0.0
        p_state.last_eta_text = ""
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
        # Collapse filter panel — chips summarize active filters
        adv_filters_panel.value = False
        # Scroll results area into view
        ui.run_javascript(f'document.getElementById("c{results_container.id}").scrollIntoView({{behavior: "smooth", block: "start"}})')
        status_label.text = tr('Initializing search...')

        # Show top page loading bar during search
        ui.run_javascript('if (window.__showLoadingBar) window.__showLoadingBar();')

        # Clear previous results header and container when starting new search
        results_header.text = tr('Searching...')
        results_container.clear()

        # Capture filter text in main thread to avoid closure issues in background thread
        captured_filter_text = get_filter_text()

        def progress_cb(current, total):
            if p_state.is_cancelled:
                raise InterruptedError("Search cancelled")
            if total > 0:
                p_state.progress = current / total
                p_state.chunks_processed = current
                p_state.chunks_total = total
                p_state.status = f"{current} / {total}"

        # Capture search mode settings in main thread
        captured_lab_mode = lab_mode.value
        captured_freq_threshold = int(freq_threshold.value) if freq_threshold.value else 50
        captured_deep_scan = deep_scan.value if captured_lab_mode else False
        captured_chunk_size = int(chunk_size.value) if chunk_size.value else 5
        captured_mode = mode_select.value

        # Capture boundary settings (only used in lab mode)
        captured_boundary_mode = boundary_mode.value or 'full'
        captured_boundary_delimiter = boundary_delimiter.value or '\n'
        captured_boundary_boost = float(boundary_boost.value) if boundary_boost.value else 1.5
        captured_min_delimiter_distance = int(min_delimiter_distance.value) if min_delimiter_distance.value else 3
        # For regular (full) mode, use min_chunks_input as the min_boundary_matches value
        # For boundary/combined modes, use the advanced dialog's min_boundary_matches
        captured_min_chunks = int(min_chunks_input.value) if min_chunks_input.value else 1
        if captured_boundary_mode == 'full':
            captured_min_boundary_matches = captured_min_chunks
        else:
            captured_min_boundary_matches = int(min_boundary_matches.value) if min_boundary_matches.value else 0

        # Compute pre-search filter set from active filters
        restrict_sys_ids = None
        if _has_active_filters():
            from shared.fjms_service import get_fjms_service

            include_mode = p_state.filter_include_mode
            _domains = p_state.filter_domains or None
            _authors = p_state.filter_authors or None
            _works = p_state.filter_works or None

            def _compute_restrict():
                fjms = get_fjms_service(thread_safe=True)
                if not fjms.is_available():
                    return None
                kwargs = dict(
                    date_from=p_state.filter_date_from,
                    date_to=p_state.filter_date_to,
                    material_exclude=p_state.filter_material_exclude or None,
                    text_all=p_state.filter_text_all or None,
                    text_any=p_state.filter_text_any or None,
                    text_not=p_state.filter_text_not or None,
                )
                if include_mode:
                    kwargs['domains'] = _domains
                    kwargs['authors'] = _authors
                    kwargs['works'] = _works
                else:
                    kwargs['domains_exclude'] = _domains
                    kwargs['authors_exclude'] = _authors
                    kwargs['works_exclude'] = _works
                return fjms.get_filter_sys_ids(**kwargs)

            restrict_sys_ids = await run.io_bound(_compute_restrict)
            p_state.restrict_sys_ids = restrict_sys_ids
            # If filters are active but match nothing, show message and return
            if restrict_sys_ids is not None and len(restrict_sys_ids) == 0:
                ui.notify(tr("No manuscripts match the current filters."), type='warning')
                p_state.is_running = False
                search_indicator.style('display: none;')
                progress_bar.style('opacity: 0;')
                ui.run_javascript('if (window.__hideLoadingBar) window.__hideLoadingBar();')
                return

        # Merge per-manuscript exclusions into restrict_sys_ids if both are present
        if p_state.excluded_manuscript_ids and restrict_sys_ids is not None:
            restrict_sys_ids = restrict_sys_ids - p_state.excluded_manuscript_ids

        # Capture restrict_sys_ids for the background thread
        captured_restrict_sys_ids = restrict_sys_ids

        def run_search():
            try:
                if captured_lab_mode:
                    # LAB MODE: Use fingerprint-based search with advanced features
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
                else:
                    # STANDARD MODE: Use direct Tantivy search (faster, simpler)
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
                        min_delimiter_distance=captured_min_delimiter_distance,
                        restrict_sys_ids=captured_restrict_sys_ids,
                    )
                return result
            except InterruptedError:
                # Search was cancelled -- return None; partial results handled by core functions
                # that catch InterruptedError internally and return accumulated results
                return None
            except Exception as e:
                logger.exception(f"Parallels Error: {e}")
                return None

        result_data = await run.io_bound(run_search)

        p_state.is_running = False
        p_state.progress = 1.0

        # Compute total elapsed time for summary
        total_elapsed = time.time() - p_state.search_start_time if p_state.search_start_time else 0
        if total_elapsed >= 3600:
            total_elapsed_str = f"{int(total_elapsed // 3600)}:{int((total_elapsed % 3600) // 60):02d}:{int(total_elapsed % 60):02d}"
        else:
            total_elapsed_str = f"{int(total_elapsed // 60)}:{int(total_elapsed % 60):02d}"

        # Hide top page loading bar
        ui.run_javascript('if (window.__hideLoadingBar) window.__hideLoadingBar();')

        # Hide the search indicator animation (but keep status label visible for summary)
        search_indicator.style('display: none;')

        if result_data:
            main_results = result_data.get('main', [])
            filtered_results = result_data.get('filtered', [])
            is_partial = result_data.get('partial', False)

            if main_results or filtered_results:
                p_state.results = main_results
                p_state.filtered_results = filtered_results

                # PostHog: track parallels search
                from web.analytics import posthog_capture
                posthog_capture('parallels_search', {
                    'text_length': len(text),
                    'word_count': words,
                    'result_count': len(main_results),
                    'filtered_count': len(filtered_results),
                    'duration_seconds': round(total_elapsed, 1),
                    'is_partial': is_partial,
                    'mode': captured_mode,
                })

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

                # Add to composition history
                try:
                    source_text = text_input.value or ''
                    comp_title = source_text[:50].replace('\n', ' ').strip()
                    if len(source_text) > 50:
                        comp_title += '...'
                    _add_to_comp_history(
                        title=comp_title,
                        result_count=len(main_results),
                        params={
                            'chunk_size': int(chunk_size.value) if chunk_size.value else 5,
                            'mode': mode_select.value or 'exact',
                            'filters': {
                                'domains': p_state.filter_domains,
                                'authors': p_state.filter_authors,
                                'works': p_state.filter_works,
                                'include_mode': p_state.filter_include_mode,
                                'date_from': p_state.filter_date_from,
                                'date_to': p_state.filter_date_to,
                                'material_exclude': p_state.filter_material_exclude,
                                'text_all': p_state.filter_text_all,
                                'text_any': p_state.filter_text_any,
                                'text_not': p_state.filter_text_not,
                            } if _has_active_filters() else None,
                        },
                        state_snapshot={
                            'source_text': source_text,
                            'results': main_results[:500],
                            'filtered_results': filtered_results[:500] if filtered_results else [],
                            'domain_exclusions': sorted(p_state.domain_exclusions),
                            'excluded_manuscript_ids': sorted(p_state.excluded_manuscript_ids),
                        },
                    )
                except Exception:
                    pass

                # Collect domain data for parallels results
                all_sys_ids = []
                for item in main_results:
                    raw_header = item.get('raw_header', '')
                    sys_match = re.search(r'(99\d{8,})', raw_header)
                    if sys_match:
                        all_sys_ids.append(sys_match.group(1))

                if all_sys_ids:
                    def collect_parallels_domains(sys_ids):
                        from shared.fjms_service import get_fjms_service
                        fjms = get_fjms_service(thread_safe=True)
                        return fjms.get_domains_for_sys_ids(sys_ids) if fjms.is_available() else {}

                    def collect_parallels_printed(sys_ids):
                        from shared.fjms_service import get_fjms_service
                        fjms = get_fjms_service(thread_safe=True)
                        return fjms.get_printed_sys_ids(sys_ids) if fjms.is_available() else set()

                    # Read show_translations in main thread before entering thread pool
                    _par_show_trans = False
                    try:
                        _par_show_trans = app.storage.user.get('show_translations', False)
                    except Exception:
                        pass

                    def collect_parallels_translations(sys_ids, show_trans=False):
                        """Batch-fetch title and PGP translations for parallels results (Phase 46-07)."""
                        try:
                            from shared.translation_service import TranslationService
                            svc = TranslationService(thread_safe=True)
                            # Title translations always fetched (language-aware)
                            title_trans = svc.get_title_translations_batch(sys_ids) if svc.titles_available() else {}
                            # PGP translations only when toggle is ON
                            pgp_trans = svc.get_pgp_translations_by_sys_ids(sys_ids) if show_trans and svc.pgp_available() else {}
                            svc.close()
                            return title_trans, pgp_trans
                        except Exception as e:
                            logger.warning("Parallels translation batch lookup failed: %s", e)
                            return {}, {}

                    import asyncio as _asyncio
                    raw_domains, printed_result, trans_tuple = await _asyncio.gather(
                        run.io_bound(collect_parallels_domains, all_sys_ids),
                        run.io_bound(collect_parallels_printed, all_sys_ids),
                        run.io_bound(collect_parallels_translations, all_sys_ids, _par_show_trans),
                    )
                    p_state.printed_ids = printed_result
                    p_state.title_translations, p_state.translation_data = trans_tuple
                    p_state.all_result_domains = {}
                    p_state.domain_name_map = {}
                    from shared.fjms_service import qualify_domain_name
                    for sys_id, doms in raw_domains.items():
                        child_names = {d['domain'] for d in doms}
                        filtered_doms = [qualify_domain_name(d['domain'], d.get('parent_domain')) for d in doms if not (d.get('parent_domain') and d['parent_domain'] in child_names and d['parent_domain'] != d['domain'])]
                        if filtered_doms:
                            p_state.all_result_domains[sys_id] = filtered_doms
                        for d in doms:
                            qname = qualify_domain_name(d['domain'], d.get('parent_domain'))
                            if qname != d['domain'] and d.get('domain_heb') and d.get('parent_domain_heb'):
                                p_state.domain_name_map[qname] = f"{d['domain_heb']} ({d['parent_domain_heb']})"
                            if d.get('domain_heb') and d['domain'] not in p_state.domain_name_map:
                                p_state.domain_name_map[d['domain']] = d['domain_heb']
                            if d.get('parent_domain_heb') and d.get('parent_domain') and d['parent_domain'] not in p_state.domain_name_map:
                                p_state.domain_name_map[d['parent_domain']] = d['parent_domain_heb']
                    p_state.has_domain_data = bool(p_state.all_result_domains)

                    # Pre-cache domain hierarchy for filter dialog
                    if p_state.has_domain_data:
                        def fetch_parallels_hierarchy():
                            from shared.fjms_service import get_fjms_service
                            fjms_h = get_fjms_service(thread_safe=True)
                            return fjms_h.get_domain_hierarchy() if fjms_h.is_available() else {}
                        p_state.domain_hierarchy = await run.io_bound(fetch_parallels_hierarchy)
                    else:
                        p_state.domain_hierarchy = {}
                else:
                    p_state.all_result_domains = {}
                    p_state.has_domain_data = False
                    p_state.domain_hierarchy = {}
                    p_state.printed_ids = set()
                    p_state.title_translations = {}
                    p_state.translation_data = {}

                # Show/hide domain filter button
                p_domain_filter_btn.set_visibility(p_state.has_domain_data)
                _update_parallels_domain_filter_btn()

                # Build filter summary suffix for status line
                _filter_suffix = ''
                if _has_active_filters() and p_state.filter_manuscript_count is not None:
                    filter_parts = []
                    opts_d = p_domain_select.options if hasattr(p_domain_select, 'options') else {}
                    opts_a = p_author_select.options if hasattr(p_author_select, 'options') else {}
                    opts_w = p_work_select.options if hasattr(p_work_select, 'options') else {}
                    for d in p_state.filter_domains:
                        filter_parts.append(_get_p_display_name(d, opts_d))
                    for a in p_state.filter_authors:
                        filter_parts.append(_get_p_display_name(a, opts_a))
                    for w in p_state.filter_works:
                        filter_parts.append(_get_p_display_name(w, opts_w))
                    if filter_parts:
                        _filter_suffix = f" ({tr('filtered')}: {', '.join(filter_parts)}, {p_state.filter_manuscript_count:,} {tr('manuscripts')})"
                    else:
                        _filter_suffix = f" ({tr('filtered')}: {p_state.filter_manuscript_count:,} {tr('manuscripts')})"

                # Show message if results are partial (search was cancelled)
                if is_partial:
                    chunks_done = p_state.chunks_processed
                    chunks_all = p_state.chunks_total
                    result_count = len(main_results) + len(filtered_results)
                    summary_label.text = f"{tr('Partial results')} \u2014 {total_elapsed_str} \u2014 {chunks_done}/{chunks_all} {tr('chunks')}, {result_count} {tr('Results')}{_filter_suffix}"
                    ui.notify(tr('Showing partial results'), type='warning', timeout=3000)
                else:
                    # Set summary line that stays visible until next search
                    chunks_all = p_state.chunks_total
                    result_count = len(main_results) + len(filtered_results)
                    summary_label.text = f"{tr('Search completed in')} {total_elapsed_str} \u2014 {chunks_all} {tr('chunks')}, {result_count} {tr('Results')}{_filter_suffix}"

                # Apply domain exclusions if any
                if p_state.domain_exclusions and p_state.has_domain_data:
                    main_results = _filter_parallels_by_domain(main_results)
                    filtered_results = _filter_parallels_by_domain(filtered_results) if filtered_results else filtered_results

                render_results(main_results, filtered_results, is_partial=is_partial)
            else:
                if is_partial:
                    summary_label.text = f"{tr('Search cancelled')} \u2014 {total_elapsed_str} \u2014 {tr('no results yet')}"
                results_header.text = tr('No results')
                with results_container:
                    show_empty_state()
        else:
            results_header.text = tr('No results')
            with results_container:
                show_empty_state()

    def _update_parallels_domain_filter_btn():
        """Update parallels domain filter button text and styling."""
        if p_state.domain_exclusions:
            n = len(p_state.domain_exclusions)
            p_domain_filter_btn.text = f"{tr('Filter by domains')} ({n} {tr('excluded')})"
            p_domain_filter_btn.props('outline dense no-caps color=red')
        else:
            p_domain_filter_btn.text = tr('Filter by domains')
            p_domain_filter_btn.props('outline dense no-caps color=primary')

    def _get_sys_id_from_parallels_item(item):
        """Extract sys_id from a parallels result item."""
        raw_header = item.get('raw_header', '')
        sys_match = re.search(r'(99\d{8,})', raw_header)
        return sys_match.group(1) if sys_match else None

    def _filter_parallels_by_domain(results):
        """Filter parallels results based on domain exclusions."""
        if not p_state.domain_exclusions:
            return results
        hide_uncategorized = 'Uncategorized' in p_state.domain_exclusions
        filtered = []
        for item in results:
            sys_id = _get_sys_id_from_parallels_item(item)
            result_domains = p_state.all_result_domains.get(sys_id, []) if sys_id else []
            if not result_domains:
                if not hide_uncategorized:
                    filtered.append(item)
                continue
            elif all(d in p_state.domain_exclusions for d in result_domains):
                continue
            else:
                filtered.append(item)
        return filtered

    def _parallels_domain_display(en_name: str) -> str:
        """Get display name for a domain (Hebrew if UI is Hebrew, else English)."""
        from web.translations import get_language
        if get_language() == 'he':
            if en_name in p_state.domain_name_map:
                return p_state.domain_name_map[en_name]
            translated = tr(en_name)
            if translated != en_name:
                return translated
        return en_name

    def _open_parallels_domain_filter_dialog():
        """Open modal dialog with domain filter checkboxes for parallels results.

        Uses a single HTML container with client-side JavaScript for checkbox
        interactions to avoid the overhead of creating ~200 individual NiceGUI
        ui.checkbox elements.
        """
        if not p_state.has_domain_data:
            if p_state.domain_exclusions:
                ui.notify(tr('Run a search first to see domain options.'), type='info', timeout=3000)
            return

        # Use pre-cached hierarchy -- no DB call
        hierarchy = p_state.domain_hierarchy
        if not hierarchy:
            from shared.fjms_service import get_fjms_service
            fjms = get_fjms_service(thread_safe=True)
            hierarchy = fjms.get_domain_hierarchy() if fjms.is_available() else {}
            p_state.domain_hierarchy = hierarchy

        # Count results per domain
        domain_counts = {}
        for sys_id, domain_names in p_state.all_result_domains.items():
            for d in domain_names:
                domain_counts[d] = domain_counts.get(d, 0) + 1

        # Build filtered hierarchy
        from shared.fjms_service import qualify_domain_name, AMBIGUOUS_CHILD_DOMAINS
        result_hierarchy = {}
        for parent_name, info in hierarchy.items():
            parent_in_results = parent_name in domain_counts
            children_in_results = []
            for child in info.get('children', []):
                qname = qualify_domain_name(child['domain'], parent_name)
                if qname in domain_counts:
                    children_in_results.append({
                        'domain': qname,
                        'domain_heb': child.get('domain_heb', child['domain']),
                        'count': domain_counts[qname],
                    })
                elif child['domain'] in domain_counts and child['domain'] not in AMBIGUOUS_CHILD_DOMAINS:
                    children_in_results.append({
                        'domain': child['domain'],
                        'domain_heb': child.get('domain_heb', child['domain']),
                        'count': domain_counts[child['domain']],
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

        # Orphans
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

        # Uncategorized
        all_sys_ids_in_results = set()
        for item in p_state.results:
            sid = _get_sys_id_from_parallels_item(item)
            if sid:
                all_sys_ids_in_results.add(sid)
        uncategorized_count = sum(1 for sid in all_sys_ids_in_results if sid not in p_state.all_result_domains)
        if uncategorized_count > 0:
            result_hierarchy['Uncategorized'] = {
                'parent_domain_heb': tr('Uncategorized'),
                'count': uncategorized_count,
                'children': [],
            }

        total_results = len(p_state.results)
        current_exclusions = p_state.domain_exclusions.copy()

        # Build checkbox HTML -- all checkboxes as a single HTML string
        # Use unique container ID to avoid conflicts with stale dialog DOM nodes
        import json as _json
        import uuid as _uuid
        container_id = f'domain-filter-{_uuid.uuid4().hex[:8]}'
        checkbox_html_parts = []
        for parent_name, info in sorted(result_hierarchy.items(), key=lambda x: -x[1]['count']):
            children = info.get('children', [])
            parent_checked = 'checked' if parent_name not in current_exclusions else ''
            parent_label = f"{_parallels_domain_display(parent_name)} ({info['count']})"
            child_domain_names = [c['domain'] for c in children]
            parent_domain_attr = html.escape(parent_name, quote=True)
            children_json_attr = html.escape(_json.dumps(child_domain_names), quote=True)
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
            for child in sorted(children, key=lambda c: -c['count']):
                child_checked = 'checked' if child['domain'] not in current_exclusions else ''
                child_label = f"{_parallels_domain_display(child['domain'])} ({child['count']})"
                child_domain_attr = html.escape(child['domain'], quote=True)
                child_label_html = html.escape(child_label)
                checkbox_html_parts.append(
                    f'<label class="domain-child" style="display:flex;align-items:center;gap:6px;'
                    f'padding:2px 0;padding-inline-start:2rem;cursor:pointer">'
                    f'<input type="checkbox" data-domain="{child_domain_attr}" '
                    f'{child_checked} '
                    f'style="width:16px;height:16px;accent-color:#1976d2">'
                    f'<span>{child_label_html}</span></label>'
                )

        checkbox_html = '\n'.join(checkbox_html_parts)

        # Build the dialog with minimal NiceGUI elements
        with ui.dialog() as dialog, ui.card().classes('w-[600px] max-h-[80vh]'):
            with ui.column().classes('w-full gap-2'):
                ui.label(tr('Filter by Domain')).classes('text-lg font-bold')
                ui.label(
                    f"{tr('Showing')} {total_results} {tr('of')} {total_results} {tr('results')}"
                ).classes('text-sm text-gray-500')

                # Single HTML container with all checkboxes (JS helpers loaded at page level)
                with ui.scroll_area().classes('w-full').style('max-height: 50vh;'):
                    ui.html(f'<div id="{container_id}">{checkbox_html}</div>', sanitize=False)

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
                            p_state.domain_exclusions = excluded
                            app.storage.user['parallels_domain_exclusions'] = list(excluded)
                            _update_parallels_domain_filter_btn()
                            main_filtered = _filter_parallels_by_domain(p_state.results)
                            filt_filtered = _filter_parallels_by_domain(p_state.filtered_results) if p_state.filtered_results else p_state.filtered_results
                            render_results(main_filtered, filt_filtered)
                            dialog.close()

                        ui.button(tr('Apply'), on_click=apply_filter).props('dense no-caps color=primary')
                        ui.button(tr('Cancel'), on_click=dialog.close).props('flat dense no-caps')

        dialog.open()

    def show_empty_state():
        with ui.column().classes('w-full items-center py-12'):
            ui.icon('search_off').classes('text-5xl').style('color: var(--text-muted);')
            # Changed to H3
            h3(tr('No parallels found'), classes='text-lg mt-4', style='color: var(--text-secondary);')
            ui.label(tr('Try adjusting your search parameters')).classes('text-sm').style('color: var(--text-muted);')

    def _rerender_with_exclusions():
        """Re-render results applying per-manuscript exclusions and domain filters."""
        main_results = p_state.results
        filtered_results = p_state.filtered_results
        if p_state.domain_exclusions and p_state.has_domain_data:
            main_results = _filter_parallels_by_domain(main_results)
            filtered_results = _filter_parallels_by_domain(filtered_results) if filtered_results else filtered_results
        render_results(main_results, filtered_results)

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

        # Show partial results warning banner at top
        if is_partial:
            with results_container:
                with ui.element('div').classes('w-full px-4 py-3 rounded-lg mb-4').style(
                    'background: #fff3cd; border: 1px solid #ffc107; color: #856404;'
                ):
                    with ui.row().classes('items-center gap-2'):
                        ui.icon('warning').classes('text-xl')
                        chunks_info = f"{p_state.chunks_processed} / {p_state.chunks_total}" if p_state.chunks_total > 0 else ""
                        ui.label(f"{tr('Partial results')} — {chunks_info} {tr('chunks searched')}").classes('font-medium')

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

        # Separate per-manuscript excluded groups from main results
        excluded_ms_groups = []
        visible_groups = []
        for group_key, group_data in sorted_groups:
            sid = group_data.get('sys_id')
            if sid and sid in p_state.excluded_manuscript_ids:
                excluded_ms_groups.append((group_key, group_data))
            else:
                visible_groups.append((group_key, group_data))
        sorted_groups = visible_groups

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

        # Initial load of main results
        if sorted_groups:
            load_more_main()

        # Filtered/excluded results in collapsible section (collapsed by default)
        if sorted_filtered_groups:
            with results_container:
                with ui.expansion(
                    text=f"{tr('Excluded Results')} ({filtered_count})",
                    icon='filter_alt',
                    value=False  # collapsed by default
                ).classes('w-full').style(
                    'border: 1px solid var(--accent-amber); border-radius: 8px; margin-top: 16px;'
                ).props('dense header-class="text-amber-8 text-subtitle1 text-weight-medium"') as filtered_expansion:
                    filtered_section = ui.column().classes('w-full gap-4')
                    filtered_load_more_container = ui.row().classes('w-full justify-center py-4')

            def load_more_filtered_inner():
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
                            on_click=load_more_filtered_inner
                        ).props('flat color=amber')

            load_more_filtered_inner()

        # Per-manuscript excluded results in collapsible section (separate from filtered)
        if excluded_ms_groups:
            with results_container:
                with ui.expansion(
                    text=f"{tr('Excluded Manuscripts')} ({len(excluded_ms_groups)})",
                    icon='remove_circle_outline',
                    value=False  # collapsed by default
                ).classes('w-full').style(
                    'border: 1px solid var(--border-light); border-radius: 8px; margin-top: 16px;'
                ).props('dense header-class="text-grey-7 text-subtitle1 text-weight-medium"'):
                    for group_key, group_data in excluded_ms_groups:
                        sid = group_data.get('sys_id')
                        with ui.row().classes('w-full items-center justify-between py-2 px-4').style(
                            'border-bottom: 1px solid var(--border-light);'
                        ):
                            label_parts = [group_data['shelfmark']]
                            if sid == p_state.auto_excluded_source_id:
                                label_parts.append(f"({tr('Source manuscript')})")
                            ui.label(' '.join(label_parts)).classes('text-sm').style(
                                'color: var(--text-secondary);'
                            )
                            # Restore button
                            def _restore_manuscript(restore_sid=sid):
                                p_state.excluded_manuscript_ids.discard(restore_sid)
                                if restore_sid == p_state.auto_excluded_source_id:
                                    p_state.auto_excluded_source_id = None
                                _persist('parallels_excluded_manuscript_ids', list(p_state.excluded_manuscript_ids))
                                _update_p_chip_bar()
                                _rerender_with_exclusions()

                            ui.button(
                                icon='undo', on_click=_restore_manuscript
                            ).props('flat round dense size=sm color=primary').tooltip(tr('Restore'))

    def create_manuscript_group(group_data, is_filtered=False):
        """Create an expandable manuscript group with its parallels."""
        shelfmark = group_data['shelfmark']
        sys_id = group_data['sys_id']
        items = group_data['items']
        max_score = group_data['max_score']
        avg_score = group_data['avg_score']

        # Get title and library
        title = ''
        library_name = ''
        if sys_id and state.meta_mgr:
            try:
                _, title_temp = state.meta_mgr.get_meta_for_id(sys_id)
                title = title_temp or ''
                # Get library name
                library_code = state.meta_mgr.get_library_for_id(sys_id)
                if library_code:
                    from genizah_core import get_library_display
                    library_name = get_library_display(library_code, short=False, lang=get_language())
            except Exception:
                pass

        # Build display shelfmark with library name
        display_shelfmark = shelfmark
        if library_name:
            display_shelfmark = f"{library_name}, {shelfmark}"

        border_style = 'border: 2px solid var(--accent-amber);' if is_filtered else 'border: 2px solid var(--border-light);'
        with ui.card().classes('w-full p-0 overflow-hidden').style(border_style):
            # Header (always visible)
            with ui.row().classes('w-full items-center justify-between p-4').style('background: var(--bg-card);'):
                with ui.column().classes('gap-1 flex-grow'):
                    with ui.row().classes('items-center gap-3'):
                        icon_color = 'color: var(--accent-amber);' if is_filtered else 'color: var(--primary-600);'
                        ui.icon('menu_book').classes('text-xl').style(icon_color)
                        # Changed to H3 - with library name
                        shelfmark_color = 'color: var(--accent-amber);' if is_filtered else 'color: var(--primary-700);'
                        h3(display_shelfmark, classes='text-lg font-bold', style=shelfmark_color)
                        badge_color = 'amber' if is_filtered else 'blue'
                        ui.badge(f"{len(items)} {tr('matches')}", color=badge_color).classes('text-xs')

                        # Printed material indicator
                        if sys_id and sys_id in p_state.printed_ids:
                            from shared.fjms_service import PRINTED_BADGE_COLORS, PRINTED_LABEL_EN, PRINTED_LABEL_HE
                            from web.translations import get_language as _get_lang
                            _bg, _fg = PRINTED_BADGE_COLORS
                            _plabel = PRINTED_LABEL_HE if _get_lang() == 'he' else PRINTED_LABEL_EN
                            ui.label(_plabel).classes('text-xs px-2 py-0.5 rounded shrink-0 font-medium').style(
                                f'background: {_bg}; color: {_fg};'
                            )

                        # Exclusion reason chip for filtered results
                        if is_filtered:
                            # Determine dominant filter reason from items
                            reasons = set()
                            for it in items:
                                fr = it.get('filter_reason', '') or ''
                                if fr == 'source_text':
                                    reasons.add(tr('Found in source text'))
                                elif fr == 'high_frequency':
                                    reasons.add(tr('High frequency'))
                                elif it.get('is_text_filtered'):
                                    reasons.add(tr('Found in source text'))
                                elif it.get('is_filtered'):
                                    reasons.add(tr('Filtered'))
                            reason_text = ', '.join(reasons) if reasons else tr('Filtered')
                            ui.label(reason_text).classes('text-xs px-2 py-0.5 rounded').style(
                                'background: #fff3cd; color: #856404; white-space: nowrap;'
                            )

                    # Resolve translated title — always language-aware (not gated behind toggle)
                    _p_title = title
                    if sys_id and p_state.title_translations:
                        _p_tt = p_state.title_translations.get(sys_id)
                        if _p_tt:
                            _lang = get_language()
                            if _lang == 'he':
                                _p_title = _p_tt.get('hebrew_title') or _p_tt.get('english_title') or title
                            else:
                                _p_title = _p_tt.get('english_title') or _p_tt.get('hebrew_title') or title
                    if _p_title:
                        _p_title_short = (_p_title[:100] + '...') if len(_p_title) > 100 else _p_title
                        _p_dir = 'ltr' if (p_state.title_translations.get(sys_id, {}).get('english_title') and get_language() != 'he') else 'rtl'
                        ui.label(_p_title_short).classes('text-xs').style(f'color: var(--text-secondary); direction: {_p_dir};')

                with ui.row().classes('items-center gap-3'):
                    # Score badges
                    max_color = 'green' if max_score > 70 else 'amber' if max_score > 40 else 'gray'
                    ui.badge(f"{tr('Max')}: {int(max_score)}", color=max_color).classes('text-xs')
                    avg_color = 'green' if avg_score > 60 else 'amber' if avg_score > 35 else 'gray'
                    ui.badge(f"{tr('Avg')}: {int(avg_score)}", color=avg_color).classes('text-xs')

                    # Per-manuscript exclude button
                    if sys_id and not is_filtered:
                        def _exclude_manuscript(sid=sys_id):
                            p_state.excluded_manuscript_ids.add(sid)
                            _persist('parallels_excluded_manuscript_ids', list(p_state.excluded_manuscript_ids))
                            _update_p_chip_bar()
                            # Re-render results with exclusion applied
                            _rerender_with_exclusions()

                        is_auto_excluded = sys_id == p_state.auto_excluded_source_id
                        excl_tooltip = tr('Source manuscript') if is_auto_excluded else tr('Exclude this manuscript')
                        ui.button(
                            icon='remove_circle_outline',
                            on_click=_exclude_manuscript
                        ).props('flat round dense size=sm color=grey').tooltip(excl_tooltip)

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

        # For source text, show paragraph breaks with red | marker
        src_raw = item.get('source_ctx', '')
        delim = boundary_delimiter.value or '\n'
        # Replace delimiter with placeholder (no special HTML chars), then other newlines with space
        BOUNDARY_MARKER = '~PARA_BREAK~'
        if delim in src_raw:
            src_raw = src_raw.replace(delim, BOUNDARY_MARKER)
        src_raw = src_raw.replace('\n', ' ')
        src_text = html.escape(src_raw)
        # Replace placeholder with red pipe HTML (marker survives html.escape since it has no special chars)
        src_text = src_text.replace(BOUNDARY_MARKER, ' <span style="color: #ef4444; font-weight: bold;">|</span> ')
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
                        ui.label(tr('Your text')).classes('text-xs font-bold uppercase').style('color: var(--success);')
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
        library_name = ''
        if sys_id and state.meta_mgr:
            try:
                _, title_temp = state.meta_mgr.get_meta_for_id(sys_id)
                title = title_temp or ''
                # Get library name
                library_code = state.meta_mgr.get_library_for_id(sys_id)
                if library_code:
                    from genizah_core import get_library_display
                    library_name = get_library_display(library_code, short=False, lang=get_language())
            except Exception:
                pass

        with ui.dialog() as dialog, ui.card().classes('p-6 min-w-96 max-w-2xl'):
            # Changed to H3
            h3(tr('Metadata'), classes='text-xl font-bold mb-4')

            with ui.column().classes('w-full gap-3'):
                # Resolve translated title for metadata dialog — always language-aware
                _md_title = title
                if sys_id and p_state.title_translations:
                    _md_tt = p_state.title_translations.get(sys_id)
                    if _md_tt:
                        _lang = get_language()
                        if _lang == 'he':
                            _md_title = _md_tt.get('hebrew_title') or _md_tt.get('english_title') or title
                        else:
                            _md_title = _md_tt.get('english_title') or _md_tt.get('hebrew_title') or title

                # Show PGP description translation if available (only when UI is Hebrew)
                _md_desc = ''
                if _par_show_trans and get_language() == 'he' and sys_id and p_state.translation_data:
                    _md_trans = p_state.translation_data.get(sys_id)
                    if _md_trans:
                        _md_desc = _md_trans.get('description_he') or ''

                metadata_items = [
                    (tr('Library'), library_name or tr('Not available')),
                    (tr('Shelfmark'), shelfmark),
                    (tr('System ID'), sys_id or tr('Not available')),
                    (tr('Score'), str(int(item.get('score', 0)))),
                ]
                if _md_desc:
                    metadata_items.append((tr('Description'), _md_desc))

                # Title row with toggle to original
                with ui.row().classes('w-full items-start gap-4'):
                    ui.label(tr('Title') + ':').classes('font-bold w-32').style('color: var(--text-secondary);')
                    _md_display = _md_title or tr('Not available')
                    _md_orig = title or ''
                    _md_dir = 'ltr' if get_language() != 'he' else 'rtl'
                    if _md_orig and _md_orig != _md_display:
                        _md_st = {'showing_original': False}
                        with ui.row().classes('flex-grow items-center gap-0'):
                            _md_lbl = ui.label(_md_display).classes('flex-grow').style(f'color: var(--text-primary); direction: {_md_dir};')
                            def _make_md_toggle(lbl, orig, resolved, flag):
                                def handler():
                                    flag['showing_original'] = not flag['showing_original']
                                    lbl.text = orig if flag['showing_original'] else resolved
                                    lbl.style(f'color: var(--text-primary); direction: rtl;' if flag['showing_original'] else f'color: var(--text-primary); direction: {_md_dir};')
                                return handler
                            ui.button(icon='swap_horiz').props('flat dense round size=xs').style(
                                'min-width: 18px; min-height: 18px; padding: 0; opacity: 0.4;'
                            ).tooltip(tr('Show original title')).on('click.stop', _make_md_toggle(_md_lbl, _md_orig, _md_display, _md_st))
                    else:
                        ui.label(_md_display).classes('flex-grow').style(f'color: var(--text-primary); direction: {_md_dir};')

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
        library_name = ''

        if raw_header and state.meta_mgr:
            try:
                sys_match = re.search(r'(99\d{8,})', raw_header)
                if sys_match:
                    sys_id = sys_match.group(1)
                    shelf_temp, title_temp = state.meta_mgr.get_meta_for_id(sys_id)
                    shelfmark = shelf_temp or shelfmark
                    title = title_temp or ''
                    # Get library name
                    library_code = state.meta_mgr.get_library_for_id(sys_id)
                    if library_code:
                        from genizah_core import get_library_display
                        library_name = get_library_display(library_code, short=False, lang=get_language())
            except Exception:
                pass

        # Build display shelfmark with library name
        display_shelfmark = shelfmark
        if library_name:
            display_shelfmark = f"{library_name}, {shelfmark}"

        # Format text snippets (escape HTML first to prevent XSS)
        ms_text = html.escape(item.get('text', '').replace('\n', ' '))
        ms_text_html = re.sub(r'\*(.*?)\*', r'<span class="highlight-match">\1</span>', ms_text)

        # For source text, show paragraph breaks with red | marker
        src_raw = item.get('source_ctx', '')
        delim = boundary_delimiter.value or '\n'
        BOUNDARY_MARKER = '~PARA_BREAK~'
        if delim in src_raw:
            src_raw = src_raw.replace(delim, BOUNDARY_MARKER)
        src_raw = src_raw.replace('\n', ' ')
        src_text = html.escape(src_raw)
        src_text = src_text.replace(BOUNDARY_MARKER, ' <span style="color: #ef4444; font-weight: bold;">|</span> ')
        src_text_html = re.sub(r'\*(.*?)\*', r'<span style="background: #bbf7d0; padding: 2px 4px; border-radius: 3px;">\1</span>', src_text)

        with ui.card().classes('w-full p-5 hover:shadow-lg transition-all'):
            # Header row
            with ui.row().classes('w-full items-start justify-between mb-4'):
                with ui.column().classes('gap-1'):
                    with ui.row().classes('items-center gap-3'):
                        ui.label(f"#{idx + 1}").classes('text-xs px-2 py-1 rounded').style(
                            'background: var(--bg-tertiary); color: var(--text-muted);'
                        )
                        ui.label(display_shelfmark).classes('text-lg font-bold').style('color: var(--primary-700);')
                    if title:
                        # Resolve title by language
                        _exp_title = title
                        if sys_id and p_state.title_translations:
                            _exp_tt = p_state.title_translations.get(sys_id)
                            if _exp_tt:
                                _exp_lang = get_language()
                                _exp_title = (_exp_tt.get('english_title') or _exp_tt.get('hebrew_title') or title) if _exp_lang != 'he' else (_exp_tt.get('hebrew_title') or _exp_tt.get('english_title') or title)
                        _exp_short = (_exp_title[:80] + '...') if len(_exp_title) > 80 else _exp_title
                        _exp_dir = 'ltr' if get_language() != 'he' else 'rtl'
                        ui.label(_exp_short).classes('text-sm').style(f'color: var(--text-secondary); direction: {_exp_dir};')

                # Score badge
                score_color = 'green' if score > 70 else 'amber' if score > 40 else 'gray'
                ui.badge(f"{tr('Score')}: {score}", color=score_color).classes('text-sm')

            # Content comparison
            with ui.row().classes('w-full gap-4'):
                # Source context
                with ui.column().classes('flex-1 gap-2'):
                    ui.label(tr('Your text')).classes('text-xs font-bold uppercase').style('color: var(--success);')
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

                # Check if item is in any list
                parallels_in_list = state.lists_mgr and sys_id and state.lists_mgr.is_item_in_any_list(sys_id)
                ui.button(
                    icon='star' if parallels_in_list else 'star_border',
                    on_click=lambda i=item, s=shelfmark, t=title, sid=sys_id: add_to_list(i, s, t, sid)
                ).props('flat round dense').style('color: var(--accent-amber);').tooltip(tr('In List') if parallels_in_list else tr('Add to List'))

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
        ui.notify(tr('Session restored'), type='info', timeout=3000, position='top')

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
                return

        # Update UI
        filter_sources['pending_restore'] = False
        try:
            sefaria_progress.style('display: none;')
            sefaria_status.style('display: none;')
            refresh_loaded_sources_ui()
        except (RuntimeError, Exception):
            pass  # Client deleted

    # Schedule async restore on page load
    ui.timer(0.1, restore_filter_sources, once=True)

    # --- Deferred filter option loading (runs after UI renders) ---

    async def _deferred_p_filter_init():
        """Load filter select options asynchronously after page renders."""
        d = await run.io_bound(_build_domain_options)
        p_domain_select.options = d
        p_domain_select.update()
        a = await run.io_bound(_build_author_options, p_state.filter_domains)
        p_author_select.options = a
        p_author_select.update()
        w = await run.io_bound(_build_work_options, p_state.filter_domains, p_state.filter_authors)
        p_work_select.options = w
        p_work_select.update()
        _update_p_chip_bar()

    ui.timer(0.1, _deferred_p_filter_init, once=True)
