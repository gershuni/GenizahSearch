# -*- coding: utf-8 -*-
"""
Advanced Search Page for GenizahSearch Web Application.

Professional search interface with:
- Large Hebrew RTL text input
- Search mode selector with multiple options
- Word gap slider for phrase searches
- Advanced options panel
- Beautiful card-based results with highlights
- Result actions (view, browse, copy)
- Green color theme matching Genizah branding
"""

from nicegui import ui, run
from typing import List, Optional
import re
import asyncio
import html

from web.services import get_service, SearchResult
from web.translations import tr, is_rtl


# Search mode options with descriptions
SEARCH_MODES = [
    ('exact', 'Exact', 'exact_desc'),
    ('variants', 'Variants', 'variants_desc'),
    ('variants_extended', 'Extended', 'extended_desc'),
    ('variants_maximum', 'Maximum', 'maximum_desc'),
    ('fuzzy', 'Fuzzy', 'fuzzy_desc'),
    ('Regex', 'Regex', 'regex_desc'),
]


class SearchState:
    """Holds the search state for the page."""

    def __init__(self):
        self.query: str = ''
        self.mode: str = 'variants'
        self.gap: int = 0
        self.results: List[SearchResult] = []
        self.is_searching: bool = False
        self.error: Optional[str] = None
        self.result_count: int = 0
        self.show_advanced: bool = False
        self.results_per_page: int = 50
        self.current_page: int = 0


def convert_highlight_markers(text: str) -> str:
    """
    Convert *text* markers to <mark> HTML tags for highlighting.
    Also handles **text** markers.
    HTML-escapes the text first to prevent broken DOM from special chars.
    """
    if not text:
        return ''

    # First, temporarily replace asterisk markers with placeholders
    # to preserve them through HTML escaping
    placeholder_double = '\x00DOUBLE_MARK\x00'
    placeholder_single = '\x00SINGLE_MARK\x00'
    placeholder_end = '\x00END_MARK\x00'

    # Extract double markers first
    text = re.sub(r'\*\*([^*]+)\*\*', placeholder_double + r'\1' + placeholder_end, text)
    # Then single markers
    text = re.sub(r'\*([^*]+)\*', placeholder_single + r'\1' + placeholder_end, text)

    # Now escape HTML entities to prevent broken DOM
    text = html.escape(text)

    # Restore markers as HTML tags
    text = text.replace(placeholder_double, '<mark class="highlight-strong">')
    text = text.replace(placeholder_single, '<mark class="highlight">')
    text = text.replace(placeholder_end, '</mark>')

    return text


def create_search_page():
    """Create the advanced search page UI."""
    state = SearchState()
    service = get_service()
    results_container = None
    search_input_ref = None

    # Add custom styles for this page
    ui.add_head_html('''
    <style>
        /* Green Genizah Theme */
        .genizah-primary { color: #2e7d32 !important; }
        .genizah-bg { background-color: #e8f5e9 !important; }
        .genizah-border { border-color: #4caf50 !important; }

        /* Search Input Styling */
        .search-input-large input {
            font-size: 1.4rem !important;
            padding: 16px !important;
            line-height: 1.6 !important;
        }

        /* Highlight Marks */
        .highlight {
            background-color: #fff59d;
            padding: 2px 4px;
            border-radius: 3px;
            font-weight: 500;
        }
        .highlight-strong {
            background-color: #ffeb3b;
            padding: 2px 4px;
            border-radius: 3px;
            font-weight: 700;
        }

        /* Result Card Styling */
        .result-card {
            border: 1px solid #e0e0e0;
            border-radius: 12px;
            padding: 20px;
            margin-bottom: 16px;
            transition: all 0.25s ease;
            background: white;
        }
        .result-card:hover {
            border-color: #4caf50;
            box-shadow: 0 4px 20px rgba(46, 125, 50, 0.15);
            transform: translateY(-2px);
        }

        /* Source Badge */
        .source-badge {
            display: inline-flex;
            align-items: center;
            padding: 3px 10px;
            border-radius: 12px;
            font-size: 0.75rem;
            font-weight: 600;
        }
        .source-v08 {
            background-color: #e8f5e9;
            color: #2e7d32;
        }
        .source-v07 {
            background-color: #fff3e0;
            color: #e65100;
        }

        /* Snippet Box */
        .snippet-box {
            background: linear-gradient(135deg, #fafafa 0%, #f5f5f5 100%);
            border-radius: 8px;
            padding: 16px;
            margin-top: 12px;
            border-right: 4px solid #4caf50;
            direction: rtl;
            text-align: right;
            white-space: pre-wrap;
            line-height: 2.0;
            font-family: "David", "Frank Ruehl", "Noto Sans Hebrew", serif;
        }

        /* Mode Selector Card */
        .mode-option {
            border: 2px solid #e0e0e0;
            border-radius: 8px;
            padding: 12px 16px;
            cursor: pointer;
            transition: all 0.2s;
        }
        .mode-option:hover {
            border-color: #81c784;
            background-color: #f1f8e9;
        }
        .mode-option.selected {
            border-color: #4caf50;
            background-color: #e8f5e9;
        }

        /* Action Buttons */
        .action-btn {
            border-radius: 20px !important;
        }

        /* Gap Slider */
        .gap-slider .q-slider__track-container {
            background-color: #c8e6c9 !important;
        }
        .gap-slider .q-slider__track--left {
            background-color: #4caf50 !important;
        }

        /* No Results */
        .no-results-box {
            text-align: center;
            padding: 60px 20px;
            background: linear-gradient(180deg, #fafafa 0%, #f0f0f0 100%);
            border-radius: 16px;
        }

        /* Loading Spinner */
        .search-loading {
            text-align: center;
            padding: 60px 20px;
        }

        /* Stats Bar */
        .stats-bar {
            background: linear-gradient(90deg, #e8f5e9 0%, #c8e6c9 100%);
            border-radius: 8px;
            padding: 12px 20px;
            margin-bottom: 20px;
        }
    </style>
    ''')

    async def do_search():
        """Execute the search in background thread to avoid UI blocking."""
        if not state.query.strip():
            return

        if not service.is_ready:
            state.error = tr('Service not available')
            update_results()
            return

        state.is_searching = True
        state.error = None
        state.current_page = 0
        update_results()

        def run_search():
            """Run search in background thread."""
            return service.search(
                query=state.query.strip(),
                mode=state.mode,
                gap=state.gap,
                limit=500
            )

        try:
            # Run search in background thread to avoid blocking UI
            results = await run.io_bound(run_search)

            state.results = results
            state.result_count = len(results)
            state.error = None

        except Exception as e:
            state.error = str(e)
            state.results = []
            state.result_count = 0

        finally:
            state.is_searching = False
            update_results()

    def set_search_mode(mode: str):
        """Update the search mode."""
        state.mode = mode
        update_mode_display()

    def update_mode_display():
        """Update the mode selector display."""
        if hasattr(state, 'mode_container') and state.mode_container:
            state.mode_container.clear()
            with state.mode_container:
                render_mode_selector()

    def render_mode_selector():
        """Render the search mode selector with visual cards."""
        with ui.row().classes('flex-wrap gap-2'):
            for mode_value, mode_label, _ in SEARCH_MODES:
                is_selected = state.mode == mode_value
                classes = 'mode-option selected' if is_selected else 'mode-option'

                with ui.element('div').classes(classes).on(
                    'click', lambda m=mode_value: set_search_mode(m)
                ):
                    with ui.row().classes('items-center gap-2'):
                        if is_selected:
                            ui.icon('check_circle', size='sm').classes('text-green-600')
                        ui.label(tr(mode_label)).classes(
                            'font-medium' + (' text-green-700' if is_selected else ' text-gray-600')
                        )

    def copy_text(text: str):
        """Copy text to clipboard."""
        ui.run_javascript(f'navigator.clipboard.writeText({repr(text)})')
        ui.notify(tr('Text copied'), type='positive', position='bottom')

    def update_results():
        """Update the results display."""
        results_container.clear()

        with results_container:
            if state.is_searching:
                with ui.column().classes('w-full search-loading'):
                    ui.spinner('dots', size='xl', color='green')
                    ui.label(tr('Searching...')).classes('text-xl text-gray-600 mt-4')
                    ui.label(f'"{state.query}"').classes('text-gray-400 mt-2 hebrew-text rtl-text')
                return

            if state.error:
                with ui.card().classes('w-full p-6 bg-red-50 border-red-200'):
                    with ui.row().classes('items-center gap-3'):
                        ui.icon('error', size='lg').classes('text-red-500')
                        with ui.column():
                            ui.label(tr('Error')).classes('font-bold text-red-700')
                            ui.label(state.error).classes('text-red-600')
                return

            if not state.results:
                if state.query:
                    with ui.column().classes('w-full no-results-box'):
                        ui.icon('search_off', size='4rem').classes('text-gray-300')
                        ui.label(tr('No results found')).classes('text-2xl text-gray-500 mt-4')
                        ui.label(f'"{state.query}"').classes('text-gray-400 mt-2 hebrew-text rtl-text')

                        with ui.column().classes('mt-6'):
                            ui.label(tr('Search tips')).classes('font-medium text-gray-600 mb-2')
                            with ui.column().classes('text-gray-500 text-sm'):
                                ui.label(tr('Try different search mode'))
                                ui.label(tr('Check spelling'))
                                ui.label(tr('Use fewer words'))
                else:
                    # Initial state - show search tips
                    with ui.column().classes('w-full items-center py-8'):
                        ui.icon('tips_and_updates', size='3rem').classes('text-green-300')
                        ui.label(tr('Enter Hebrew text to search')).classes(
                            'text-xl text-gray-500 mt-4'
                        )
                return

            # Results stats bar
            with ui.row().classes('w-full stats-bar items-center justify-between'):
                with ui.row().classes('items-center gap-3'):
                    ui.icon('analytics', size='sm').classes('text-green-700')
                    ui.label(f"{state.result_count} {tr('results found')}").classes(
                        'font-bold text-green-800'
                    )

                with ui.row().classes('items-center gap-2'):
                    ui.label(f'{tr("Search mode")}: {tr(dict((m[0], m[1]) for m in SEARCH_MODES).get(state.mode, state.mode))}').classes(
                        'text-sm text-green-700'
                    )
                    if state.gap > 0:
                        ui.label(f' | {tr("Word gap")}: {state.gap}').classes('text-sm text-green-700')

            # Pagination info
            start_idx = state.current_page * state.results_per_page
            end_idx = min(start_idx + state.results_per_page, state.result_count)
            page_results = state.results[start_idx:end_idx]
            total_pages = (state.result_count + state.results_per_page - 1) // state.results_per_page

            # Results list
            for result in page_results:
                create_result_card(result)

            # Pagination controls
            if total_pages > 1:
                with ui.row().classes('w-full justify-center items-center gap-4 mt-6 py-4'):
                    # Previous button
                    ui.button(
                        icon='chevron_right' if is_rtl() else 'chevron_left',
                        on_click=lambda: go_to_page(state.current_page - 1)
                    ).props('flat round').classes(
                        '' if state.current_page > 0 else 'invisible'
                    )

                    # Page info
                    ui.label(f'{tr("Page")} {state.current_page + 1} {tr("of")} {total_pages}').classes(
                        'text-gray-600'
                    )

                    # Next button
                    ui.button(
                        icon='chevron_left' if is_rtl() else 'chevron_right',
                        on_click=lambda: go_to_page(state.current_page + 1)
                    ).props('flat round').classes(
                        '' if state.current_page < total_pages - 1 else 'invisible'
                    )

    def go_to_page(page: int):
        """Navigate to a specific results page."""
        total_pages = (state.result_count + state.results_per_page - 1) // state.results_per_page
        if 0 <= page < total_pages:
            state.current_page = page
            update_results()

    def create_result_card(result: SearchResult):
        """Create a single result card with actions."""
        display = result.display
        shelfmark = display.get('shelfmark', '') if isinstance(display, dict) else str(display)
        title = display.get('title', '') if isinstance(display, dict) else ''
        img_num = display.get('img', '') if isinstance(display, dict) else ''
        source = result.source or 'V0.8'

        # Determine source badge class
        source_class = 'source-v08' if 'V0.8' in source or '0.8' in source else 'source-v07'

        with ui.card().classes('w-full result-card'):
            # Header row with metadata
            with ui.row().classes('w-full items-start justify-between'):
                # Left side - metadata
                with ui.column().classes('flex-1'):
                    # Top line: shelfmark, badges
                    with ui.row().classes('items-center gap-3 flex-wrap'):
                        # Shelfmark as main title
                        ui.label(shelfmark or f"ID: {result.sys_id}").classes(
                            'text-lg font-bold text-green-800 cursor-pointer hover:text-green-600'
                        ).on('click', lambda r=result: ui.navigate.to(f'/document/{r.uid}'))

                        # Page number badge
                        if img_num:
                            with ui.element('span').classes('source-badge source-v08'):
                                ui.label(f"p. {img_num}")

                        # Source badge
                        with ui.element('span').classes(f'source-badge {source_class}'):
                            ui.label(source)

                        # Cross-page indicator
                        if result.cross_page:
                            with ui.element('span').classes('source-badge').style(
                                'background-color: #fff3e0; color: #e65100;'
                            ):
                                ui.icon('layers', size='xs').classes('mr-1')
                                ui.label(tr('Cross-page match'))

                    # Title line (if available)
                    if title:
                        ui.label(title).classes(
                            'text-gray-600 rtl-text hebrew-text mt-2 text-base'
                        )

                # Right side - action buttons
                with ui.row().classes('gap-2'):
                    # View document
                    ui.button(
                        tr('View'),
                        icon='visibility',
                        on_click=lambda r=result: ui.navigate.to(f'/document/{r.uid}')
                    ).props('flat dense color=green').classes('action-btn')

                    # Browse manuscript
                    if result.sys_id:
                        ui.button(
                            icon='menu_book',
                            on_click=lambda r=result: ui.navigate.to(f'/browse/{r.sys_id}')
                        ).props('flat dense color=grey').tooltip(tr('Browse Manuscripts'))

                    # Copy text
                    if result.snippet:
                        ui.button(
                            icon='content_copy',
                            on_click=lambda r=result: copy_text(r.snippet)
                        ).props('flat dense color=grey').tooltip(tr('Copy text'))

            # Snippet with highlighted matches
            if result.snippet:
                snippet_text = result.snippet[:500]
                if len(result.snippet) > 500:
                    snippet_text += '...'

                # Convert highlight markers to HTML
                highlighted_snippet = convert_highlight_markers(snippet_text)

                with ui.element('div').classes('snippet-box'):
                    ui.html(highlighted_snippet)

    # =========================================================================
    # Main Layout
    # =========================================================================

    with ui.column().classes('w-full max-w-5xl mx-auto p-4'):
        # Page title with icon
        with ui.row().classes('w-full justify-center items-center gap-3 mb-6'):
            ui.icon('search', size='2.5rem').classes('text-green-600')
            ui.label(tr('Text Search')).classes(
                'text-3xl font-bold text-green-800'
            )

        # Main search card
        with ui.card().classes('w-full p-6 mb-6').style(
            'border: 2px solid #c8e6c9; border-radius: 16px;'
        ):
            with ui.column().classes('w-full gap-5'):
                # Search input - large and prominent
                with ui.column().classes('w-full'):
                    ui.label(tr('Enter search terms')).classes(
                        'text-sm font-medium text-gray-600 mb-2'
                    )

                    with ui.row().classes('w-full gap-3'):
                        search_input_ref = ui.input(
                            placeholder=tr('Enter Hebrew text to search'),
                            value=state.query
                        ).classes(
                            'flex-1 search-input-large rtl-text hebrew-text'
                        ).props(
                            'outlined rounded standout="bg-green-50"'
                        ).on('keydown.enter', do_search)

                        search_input_ref.bind_value(state, 'query')

                        # Search button - prominent
                        ui.button(
                            tr('Search'),
                            icon='search',
                            on_click=do_search
                        ).props('color=green size=lg').classes('px-6')

                # Search mode selector
                with ui.column().classes('w-full'):
                    ui.label(tr('Search mode')).classes(
                        'text-sm font-medium text-gray-600 mb-2'
                    )
                    state.mode_container = ui.element('div').classes('w-full')
                    with state.mode_container:
                        render_mode_selector()

                # Advanced options expansion
                with ui.expansion(
                    tr('Advanced options'),
                    icon='tune'
                ).classes('w-full').props('dense header-class="text-green-700"'):
                    with ui.column().classes('w-full gap-4 pt-4'):
                        # Word gap - simple number input instead of slider
                        with ui.row().classes('w-full items-center gap-4'):
                            ui.label(tr('Word gap')).classes('font-medium text-gray-700')

                            def on_gap_change(e):
                                state.gap = int(e.value) if e.value else 0

                            ui.number(
                                value=state.gap,
                                min=0,
                                max=5,
                                step=1,
                                on_change=on_gap_change
                            ).classes('w-20').props('outlined dense')

                            ui.label(tr('Gap description')).classes(
                                'text-xs text-gray-500'
                            )

                        ui.separator()

                        # Results per page
                        with ui.row().classes('items-center gap-4'):
                            ui.label(tr('Results per page')).classes('text-gray-700')

                            def on_rpp_change(e):
                                state.results_per_page = e.value
                                state.current_page = 0
                                if state.results:
                                    update_results()

                            ui.select(
                                {25: '25', 50: '50', 100: '100', 200: '200'},
                                value=state.results_per_page,
                                on_change=on_rpp_change
                            ).props('dense outlined').classes('w-24')

        # Service status warning
        if not service.is_ready:
            with ui.card().classes('w-full p-4 mb-4').style(
                'background-color: #fff8e1; border: 1px solid #ffe082; border-radius: 12px;'
            ):
                with ui.row().classes('items-center gap-3'):
                    ui.icon('warning', size='lg').classes('text-amber-600')
                    with ui.column():
                        ui.label(tr('Service not available')).classes(
                            'font-bold text-amber-800'
                        )
                        ui.label(tr('Search functionality is currently unavailable')).classes(
                            'text-amber-700 text-sm'
                        )

        # Results container
        results_container = ui.column().classes('w-full')

        # Initial state
        update_results()
