# -*- coding: utf-8 -*-
"""
Document viewer page for GenizahSearch web application.
"""

from nicegui import ui
from typing import Optional, List

from web.services import get_service, DocumentPage
from web.translations import tr, is_rtl


class DocumentState:
    """Holds the document viewer state."""

    def __init__(self):
        self.pages: List[DocumentPage] = []
        self.current_page_idx: int = 0
        self.sys_id: str = ''
        self.metadata: dict = {}
        self.is_loading: bool = True
        self.error: Optional[str] = None


def create_document_page(uid: str):
    """Create the document viewer page UI."""
    state = DocumentState()
    service = get_service()

    # Content container reference
    content_container = None

    def load_document():
        """Load the document data."""
        state.is_loading = True
        state.error = None

        if not service.is_ready:
            state.error = tr('Service not available')
            state.is_loading = False
            return

        try:
            # Extract system ID from UID
            state.sys_id = service.extract_sys_id(uid)

            # Load all pages
            state.pages = service.get_document(state.sys_id)

            if not state.pages:
                state.error = tr('No text available')
                state.is_loading = False
                return

            # Find the page matching the UID
            for idx, page in enumerate(state.pages):
                if page.uid == uid:
                    state.current_page_idx = idx
                    break

            # Get metadata
            state.metadata = service.get_metadata(state.sys_id)

        except Exception as e:
            state.error = str(e)

        finally:
            state.is_loading = False

    def navigate_page(delta: int):
        """Navigate to next/previous page."""
        new_idx = state.current_page_idx + delta
        if 0 <= new_idx < len(state.pages):
            state.current_page_idx = new_idx
            update_content()

    def update_content():
        """Update the content display."""
        content_container.clear()

        with content_container:
            if state.is_loading:
                with ui.row().classes('w-full justify-center py-12'):
                    ui.spinner(size='lg')
                    ui.label(tr('Loading...')).classes('ml-2')
                return

            if state.error:
                with ui.card().classes('w-full p-8 text-center'):
                    ui.icon('error', size='xl').classes('text-red-500')
                    ui.label(state.error).classes('text-red-600 mt-2')
                    ui.button(tr('Back'), on_click=lambda: ui.navigate.to('/')).classes('mt-4')
                return

            if not state.pages:
                ui.label(tr('No text available')).classes('text-gray-500 py-8')
                return

            current_page = state.pages[state.current_page_idx]
            total_pages = len(state.pages)

            # Navigation controls (top)
            with ui.row().classes('w-full items-center justify-between mb-4'):
                # Previous button
                ui.button(
                    icon='arrow_forward' if is_rtl() else 'arrow_back',
                    on_click=lambda: navigate_page(-1)
                ).props('flat').bind_enabled_from(
                    state, 'current_page_idx', lambda idx: idx > 0
                )

                # Page indicator
                ui.label(
                    f"{tr('Page')} {state.current_page_idx + 1} {tr('of')} {total_pages}"
                ).classes('text-gray-600')

                # Next button
                ui.button(
                    icon='arrow_back' if is_rtl() else 'arrow_forward',
                    on_click=lambda: navigate_page(1)
                ).props('flat').bind_enabled_from(
                    state, 'current_page_idx', lambda idx: idx < total_pages - 1
                )

            # Manuscript text
            with ui.card().classes('w-full'):
                # Page header
                if current_page.full_header:
                    with ui.row().classes('w-full bg-gray-50 p-3 border-b'):
                        ui.label(current_page.full_header).classes(
                            'rtl-text hebrew-text text-sm text-gray-600 w-full'
                        )

                # Text content
                with ui.scroll_area().classes('w-full').style('max-height: 60vh'):
                    ui.label(current_page.text or tr('No text available')).classes(
                        'manuscript-text rtl-text hebrew-text w-full p-4'
                    )

            # Page selector (for direct navigation)
            if total_pages > 1:
                with ui.row().classes('w-full justify-center mt-4 gap-2'):
                    ui.label(f"{tr('Page')}:").classes('text-gray-600')
                    page_select = ui.select(
                        {i: str(i + 1) for i in range(total_pages)},
                        value=state.current_page_idx
                    ).props('dense outlined').classes('w-20')

                    @page_select.on('update:model-value')
                    def on_page_change(e):
                        state.current_page_idx = e.args
                        update_content()

    # Main layout
    with ui.column().classes('w-full max-w-5xl mx-auto p-4'):
        # Back button
        with ui.row().classes('w-full mb-4'):
            ui.button(tr('Back'), icon='arrow_back', on_click=lambda: ui.navigate.to('/')).props(
                'flat'
            )

        # Metadata card
        with ui.card().classes('w-full p-4 mb-4'):
            with ui.row().classes('w-full items-start gap-8'):
                with ui.column().classes('flex-1'):
                    ui.label(tr('Manuscript')).classes('text-sm text-gray-500')
                    ui.label(state.sys_id or uid).classes(
                        'text-lg font-semibold rtl-text hebrew-text'
                    )

                if state.metadata:
                    if state.metadata.get('shelfmark'):
                        with ui.column():
                            ui.label(tr('Shelfmark')).classes('text-sm text-gray-500')
                            ui.label(state.metadata['shelfmark']).classes('font-medium')

                    if state.metadata.get('title'):
                        with ui.column():
                            ui.label(tr('Title')).classes('text-sm text-gray-500')
                            ui.label(state.metadata['title']).classes(
                                'font-medium rtl-text hebrew-text'
                            )

        # Content container
        content_container = ui.column().classes('w-full')

        # Initial load
        load_document()
        update_content()
