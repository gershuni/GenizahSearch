# -*- coding: utf-8 -*-
"""
Parallels (Composition Search) page for GenizahSearch web application.
Find parallel texts in the Genizah corpus.
"""

from nicegui import ui
from typing import List, Optional

from web.services import get_service, CompositionResult
from web.translations import tr, is_rtl


class ParallelsState:
    """Holds the state for the parallels search page."""

    def __init__(self):
        self.source_text: str = ''
        self.filter_text: str = ''
        self.mode: str = 'variants'
        self.chunk_size: int = 4
        self.max_freq: int = 100
        self.results: List[CompositionResult] = []
        self.is_searching: bool = False
        self.error: Optional[str] = None
        self.result_count: int = 0


def create_parallels_page():
    """Create the parallels search page UI."""
    state = ParallelsState()
    service = get_service()
    results_container = None

    async def do_search():
        """Execute the composition search."""
        # Validate input
        words = state.source_text.split()
        if len(words) < 10:
            state.error = tr('Enter at least 10 words')
            update_results()
            return

        if not service.is_ready:
            state.error = tr('Service not available')
            update_results()
            return

        state.is_searching = True
        state.error = None
        update_results()

        try:
            results = service.composition_search(
                full_text=state.source_text.strip(),
                mode=state.mode,
                chunk_size=state.chunk_size,
                max_freq=state.max_freq,
                filter_text=state.filter_text.strip() if state.filter_text else None,
                limit=100
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
                with ui.column().classes('w-full items-center py-8'):
                    ui.spinner(size='lg')
                    ui.label(tr('Searching...')).classes('mt-2')
                    ui.label('This may take a while for long texts...').classes(
                        'text-gray-500 text-sm'
                    )
                return

            if state.error:
                with ui.card().classes('w-full p-4 bg-red-50'):
                    with ui.row().classes('items-center gap-2'):
                        ui.icon('error', color='red')
                        ui.label(state.error).classes('text-red-600')
                return

            if not state.results:
                if state.source_text and len(state.source_text.split()) >= 10:
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

    def create_result_card(result: CompositionResult):
        """Create a single result card for composition search."""
        display = result.display
        shelfmark = display.get('shelfmark', '') if isinstance(display, dict) else ''

        with ui.card().classes('w-full search-result mb-3'):
            # Header row
            with ui.row().classes('w-full items-center justify-between mb-2'):
                with ui.row().classes('items-center gap-2'):
                    ui.label(shelfmark or f"ID: {result.sys_id}").classes(
                        'font-bold text-green-800'
                    )
                    ui.badge(f"{tr('Score')}: {result.score:.0f}", color='green')

                ui.button(
                    tr('View'),
                    icon='visibility',
                    on_click=lambda r=result: ui.navigate.to(f'/browse/{r.sys_id}')
                ).props('flat dense')

            ui.separator()

            # Two-column comparison
            with ui.row().classes('w-full gap-4 mt-2'):
                # Manuscript text
                with ui.column().classes('flex-1'):
                    ui.label(tr('Manuscript text')).classes(
                        'text-sm font-medium text-gray-500 mb-1'
                    )
                    with ui.card().classes('w-full p-3 bg-green-50'):
                        ui.label(result.ms_snippet[:500] if result.ms_snippet else '').classes(
                            'rtl-text hebrew-text text-sm'
                        )

                # Source text match
                if result.src_snippet:
                    with ui.column().classes('flex-1'):
                        ui.label(tr('Your text')).classes(
                            'text-sm font-medium text-gray-500 mb-1'
                        )
                        with ui.card().classes('w-full p-3 bg-blue-50'):
                            ui.label(result.src_snippet[:500]).classes(
                                'rtl-text hebrew-text text-sm'
                            )

    # Main layout
    with ui.column().classes('w-full max-w-5xl mx-auto p-4'):
        # Page title
        ui.label(tr('Find Parallels')).classes(
            'text-3xl font-bold mb-2 text-center text-green-800'
        )
        ui.label(tr('Paste your text to discover parallels')).classes(
            'text-gray-600 text-center mb-6'
        )

        # Input section
        with ui.card().classes('w-full p-4 mb-6'):
            # Source text input
            ui.label(tr('Source text')).classes('font-medium mb-2')
            source_textarea = ui.textarea(
                placeholder=tr('Paste text here...'),
                value=state.source_text
            ).classes('w-full rtl-text hebrew-text').props(
                'outlined rows=8'
            )
            source_textarea.bind_value(state, 'source_text')

            # Filter text (optional)
            with ui.expansion(tr('Filter text'), icon='filter_alt').classes('w-full mt-4'):
                ui.label(tr('Optional: exclude matches from this text')).classes(
                    'text-sm text-gray-500 mb-2'
                )
                filter_textarea = ui.textarea(
                    value=state.filter_text
                ).classes('w-full rtl-text hebrew-text').props(
                    'outlined rows=4'
                )
                filter_textarea.bind_value(state, 'filter_text')

            # Options row
            with ui.row().classes('w-full items-center justify-between mt-4 flex-wrap gap-4'):
                with ui.row().classes('items-center gap-4'):
                    # Mode selector
                    mode_select = ui.select(
                        {
                            'variants': tr('Variants'),
                            'variants_extended': tr('Extended'),
                            'variants_maximum': tr('Maximum'),
                        },
                        value=state.mode,
                        label=tr('Search mode')
                    ).classes('w-36').props('outlined dense')
                    mode_select.bind_value(state, 'mode')

                    # Chunk size
                    chunk_input = ui.number(
                        value=state.chunk_size,
                        min=2,
                        max=10,
                        label=tr('Chunk size')
                    ).classes('w-28').props('outlined dense')
                    chunk_input.bind_value(state, 'chunk_size')

                    # Max frequency
                    freq_input = ui.number(
                        value=state.max_freq,
                        min=10,
                        max=500,
                        label=tr('Max frequency')
                    ).classes('w-32').props('outlined dense')
                    freq_input.bind_value(state, 'max_freq')

                # Search button
                ui.button(
                    tr('Find parallels'),
                    icon='compare_arrows',
                    on_click=do_search
                ).props('color=green')

        # Service status
        if not service.is_ready:
            with ui.card().classes('w-full p-4 bg-yellow-50 mb-4'):
                with ui.row().classes('items-center gap-2'):
                    ui.icon('warning', color='orange')
                    ui.label(tr('Service not available')).classes('text-yellow-800')

        # Results container
        results_container = ui.column().classes('w-full')
