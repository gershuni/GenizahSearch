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
        self.gap: int = 0
        self.results: List[SearchResult] = []
        self.is_searching: bool = False
        self.error: Optional[str] = None
        self.result_count: int = 0


def create_search_page():
    """Create the search page UI."""
    state = SearchState()
    service = get_service()
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
            results = service.search(
                query=state.query.strip(),
                mode=state.mode,
                gap=state.gap,
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
                with ui.card().classes('w-full p-4 bg-red-50'):
                    ui.label(f"{tr('Error')}: {state.error}").classes('text-red-600')
                return

            if not state.results:
                if state.query:
                    with ui.column().classes('w-full items-center py-12'):
                        ui.icon('search_off', size='3rem').classes('text-gray-400')
                        ui.label(tr('No results found')).classes('text-gray-500 mt-2')
                return

            # Results header
            with ui.row().classes('w-full items-center justify-between mb-4'):
                ui.label(f"{state.result_count} {tr('results found')}").classes(
                    'text-gray-600 font-medium'
                )

            # Results list
            for result in state.results:
                create_result_card(result)

    def create_result_card(result: SearchResult):
        """Create a single result card."""
        display = result.display
        shelfmark = display.get('shelfmark', '') if isinstance(display, dict) else str(display)
        title = display.get('title', '') if isinstance(display, dict) else ''
        img_num = display.get('img', '') if isinstance(display, dict) else ''

        with ui.card().classes('w-full search-result cursor-pointer').on(
            'click', lambda r=result: ui.navigate.to(f'/document/{r.uid}')
        ):
            with ui.row().classes('w-full items-start justify-between gap-4'):
                # Main content
                with ui.column().classes('flex-1 min-w-0'):
                    # Shelfmark & Title
                    with ui.row().classes('items-center gap-2 flex-wrap'):
                        ui.label(shelfmark or f"ID: {result.sys_id}").classes(
                            'font-bold text-blue-800'
                        )
                        if img_num:
                            ui.badge(f"p.{img_num}", color='blue').props('outline')
                        ui.badge(result.source, color='gray').classes('text-xs')
                        if result.cross_page:
                            ui.badge(tr('Cross-page match'), color='orange').classes('text-xs')

                    if title:
                        ui.label(title).classes(
                            'text-gray-600 text-sm rtl-text hebrew-text mt-1'
                        )

                    # Snippet
                    if result.snippet:
                        ui.separator().classes('my-2')
                        ui.label(result.snippet[:400] + ('...' if len(result.snippet) > 400 else '')).classes(
                            'snippet rtl-text hebrew-text text-gray-700'
                        )

                # View button
                ui.button(
                    tr('View'),
                    icon='visibility',
                    on_click=lambda r=result: ui.navigate.to(f'/document/{r.uid}')
                ).props('flat dense')

    # Main layout
    with ui.column().classes('w-full max-w-5xl mx-auto p-4'):
        # Page title
        ui.label(tr('Text Search')).classes('text-3xl font-bold mb-6 text-center text-blue-800')

        # Search box
        with ui.card().classes('w-full p-4 mb-6'):
            with ui.column().classes('w-full gap-4'):
                # Search input row
                with ui.row().classes('w-full gap-4 items-end flex-wrap'):
                    search_input = ui.input(
                        placeholder=tr('Enter search terms'),
                        value=state.query
                    ).classes('flex-1 min-w-64 rtl-text hebrew-text').props(
                        'outlined dense clearable'
                    ).on('keydown.enter', do_search)

                    search_input.bind_value(state, 'query')

                    # Mode selector
                    mode_select = ui.select(
                        {mode: tr(label) for mode, label in SEARCH_MODES},
                        value=state.mode,
                        label=tr('Search mode')
                    ).classes('w-36').props('outlined dense')

                    mode_select.bind_value(state, 'mode')

                    # Search button
                    ui.button(
                        tr('Search'),
                        icon='search',
                        on_click=do_search
                    ).props('color=primary')

                # Advanced options (collapsed)
                with ui.expansion(tr('Advanced options'), icon='tune').classes('w-full'):
                    with ui.row().classes('items-center gap-4'):
                        ui.label(tr('Word gap')).classes('text-sm')
                        gap_input = ui.number(
                            value=state.gap,
                            min=0,
                            max=10
                        ).classes('w-20').props('dense')
                        gap_input.bind_value(state, 'gap')

                        ui.label('(0 = adjacent words only)').classes('text-xs text-gray-500')

        # Service status
        if not service.is_ready:
            with ui.card().classes('w-full p-4 bg-yellow-50 border-yellow-200 mb-4'):
                with ui.row().classes('items-center gap-2'):
                    ui.icon('warning', color='orange')
                    ui.label(tr('Service not available')).classes('text-yellow-800')

        # Results container
        results_container = ui.column().classes('w-full')
