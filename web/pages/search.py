# -*- coding: utf-8 -*-
"""
Search page component for GenizahSearch web application.
"""

from nicegui import ui
from typing import List, Optional

from web.services import get_service, SearchResult
from web.translations import tr, is_rtl


# Search mode options
SEARCH_MODES = [
    ('variants', 'Variants'),
    ('variants_extended', 'Extended'),
    ('variants_maximum', 'Maximum'),
    ('exact', 'Exact'),
    ('fuzzy', 'Fuzzy'),
]


class SearchState:
    """Holds the search state for the page."""

    def __init__(self):
        self.query: str = ''
        self.mode: str = 'variants'
        self.results: List[SearchResult] = []
        self.is_searching: bool = False
        self.error: Optional[str] = None
        self.result_count: int = 0


def create_search_page():
    """Create the search page UI."""
    state = SearchState()
    service = get_service()

    # Results container reference
    results_container = None

    async def do_search():
        """Execute the search."""
        if not state.query.strip():
            return

        if not service.is_ready:
            state.error = tr('Service not available')
            update_results()
            return

        state.is_searching = True
        state.error = None
        update_results()

        try:
            # Run search (this is synchronous, but we handle it gracefully)
            results = service.search(
                query=state.query.strip(),
                mode=state.mode,
                limit=200
            )

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

    def update_results():
        """Update the results display."""
        results_container.clear()

        with results_container:
            if state.is_searching:
                with ui.row().classes('w-full justify-center py-8'):
                    ui.spinner(size='lg')
                    ui.label(tr('Searching...')).classes('ml-2')
                return

            if state.error:
                ui.label(f"{tr('Error')}: {state.error}").classes(
                    'text-red-500 py-4'
                )
                return

            if not state.results:
                if state.query:
                    ui.label(tr('No results found')).classes(
                        'text-gray-500 py-8 text-center w-full'
                    )
                return

            # Results header
            with ui.row().classes('w-full items-center justify-between mb-4'):
                ui.label(f"{state.result_count} {tr('results found')}").classes(
                    'text-gray-600'
                )

            # Results list
            for result in state.results:
                create_result_card(result)

    def create_result_card(result: SearchResult):
        """Create a single result card."""
        sys_id = service.extract_sys_id(result.uid)

        with ui.card().classes('w-full search-result mb-3 cursor-pointer').on(
            'click', lambda: ui.navigate.to(f'/document/{result.uid}')
        ):
            # Header row
            with ui.row().classes('w-full items-start justify-between'):
                with ui.column().classes('flex-1'):
                    # Display title
                    ui.label(result.display or result.raw_header).classes(
                        'font-semibold text-blue-800 rtl-text hebrew-text'
                    )

                    # Source badge
                    with ui.row().classes('gap-2 mt-1'):
                        ui.badge(result.source, color='blue').classes('text-xs')
                        if result.cross_page:
                            ui.badge(tr('Cross-page match'), color='orange').classes('text-xs')

                # View button
                ui.button(tr('View'), on_click=lambda r=result: ui.navigate.to(f'/document/{r.uid}')).props(
                    'flat dense'
                ).classes('text-blue-600')

            # Snippet
            if result.snippet:
                ui.separator().classes('my-2')
                ui.label(result.snippet[:500]).classes(
                    'snippet rtl-text hebrew-text text-gray-700 text-sm'
                )

    # Main layout
    with ui.column().classes('w-full max-w-5xl mx-auto p-4'):
        # Title
        ui.label(tr('Genizah Search')).classes('text-3xl font-bold mb-6 text-center')

        # Search box
        with ui.card().classes('w-full p-4 mb-6'):
            with ui.row().classes('w-full gap-4 items-end flex-wrap'):
                # Search input
                search_input = ui.input(
                    placeholder=tr('Enter search terms'),
                    value=state.query
                ).classes('flex-1 min-w-64 rtl-text hebrew-text').props(
                    'outlined dense'
                ).on('keydown.enter', do_search)

                # Bind input to state
                search_input.bind_value(state, 'query')

                # Mode selector
                mode_select = ui.select(
                    {mode: tr(label) for mode, label in SEARCH_MODES},
                    value=state.mode,
                    label=tr('Search mode')
                ).classes('w-40').props('outlined dense')

                mode_select.bind_value(state, 'mode')

                # Search button
                ui.button(tr('Search'), on_click=do_search).props('color=primary')

            # Shortcuts help (collapsed)
            with ui.expansion(tr('Help'), icon='help_outline').classes('w-full mt-2'):
                ui.markdown('''
**Search Tips:**
- Use multiple words for phrase search
- **Variants mode** handles OCR errors and spelling variations
- **Extended/Maximum** modes are more aggressive but may have false positives
- **Exact** mode matches only the exact text

**Shortcuts:**
- `= query` - Exact match
- `? query` - Variants (basic)
- `?? query` - Variants extended
- `??? query` - Variants maximum
                ''').classes('text-sm')

        # Service status
        if not service.is_ready:
            with ui.card().classes('w-full p-4 bg-yellow-50 border-yellow-200'):
                ui.label(tr('Service not available')).classes('text-yellow-800')
                if service.init_error:
                    ui.label(service.init_error).classes('text-yellow-600 text-sm')
        elif not service.index_exists:
            with ui.card().classes('w-full p-4 bg-yellow-50 border-yellow-200'):
                ui.label(tr('Index not found')).classes('text-yellow-800')

        # Results container
        results_container = ui.column().classes('w-full')
