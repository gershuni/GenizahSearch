# -*- coding: utf-8 -*-
"""
Document viewer page for GenizahSearch web application.
View a specific document/page from search results.
"""

from nicegui import ui
from typing import Optional, List

from web.services import get_service, DocumentPage, get_thumbnail_url
from web.translations import tr, is_rtl


class DocumentState:
    """Holds the document viewer state."""

    def __init__(self):
        self.pages: List[DocumentPage] = []
        self.current_page_idx: int = 0
        self.sys_id: str = ''
        self.shelfmark: str = ''
        self.title: str = ''
        self.is_loading: bool = True
        self.error: Optional[str] = None
        self.show_image: bool = False


def create_document_page(uid: str):
    """Create the document viewer page UI."""
    state = DocumentState()
    service = get_service()
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

            if not state.sys_id:
                state.error = tr('No manuscript found')
                state.is_loading = False
                return

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
            meta = service.get_metadata(state.sys_id)
            state.shelfmark = meta.get('shelfmark', '')
            state.title = meta.get('title', '')

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

    def go_to_page(page_idx: int):
        """Navigate to a specific page index."""
        if 0 <= page_idx < len(state.pages):
            state.current_page_idx = page_idx
            update_content()

    def toggle_image():
        """Toggle image display."""
        state.show_image = not state.show_image
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
                    ui.icon('error', size='3rem').classes('text-red-500')
                    ui.label(state.error).classes('text-red-600 mt-2')
                    ui.button(tr('Back'), on_click=lambda: ui.navigate.to('/search')).classes('mt-4')
                return

            if not state.pages:
                with ui.column().classes('w-full items-center py-12'):
                    ui.icon('description', size='3rem').classes('text-gray-400')
                    ui.label(tr('No text available')).classes('text-gray-500 mt-2')
                return

            current_page = state.pages[state.current_page_idx]
            total_pages = len(state.pages)

            # Navigation controls (top)
            with ui.row().classes('w-full items-center justify-between mb-4'):
                # Previous button
                prev_disabled = state.current_page_idx <= 0
                ui.button(
                    icon='chevron_right' if is_rtl() else 'chevron_left',
                    on_click=lambda: navigate_page(-1)
                ).props(f'flat round {"disabled" if prev_disabled else ""}')

                # Page info
                with ui.row().classes('items-center gap-2'):
                    ui.label(tr('Page')).classes('text-gray-600')
                    page_select = ui.select(
                        {i: str(state.pages[i].p_num) for i in range(total_pages)},
                        value=state.current_page_idx
                    ).props('dense outlined').classes('w-24')

                    @page_select.on('update:model-value')
                    def on_page_change(e):
                        go_to_page(e.args)

                    ui.label(f"{tr('of')} {total_pages}").classes('text-gray-600')

                # Next button
                next_disabled = state.current_page_idx >= total_pages - 1
                ui.button(
                    icon='chevron_left' if is_rtl() else 'chevron_right',
                    on_click=lambda: navigate_page(1)
                ).props(f'flat round {"disabled" if next_disabled else ""}')

            # Image toggle
            if current_page.fl_id:
                with ui.row().classes('w-full justify-end mb-2'):
                    ui.button(
                        tr('Hide image') if state.show_image else tr('Show image'),
                        icon='image',
                        on_click=toggle_image
                    ).props('flat')

            # Content area
            if state.show_image and current_page.fl_id:
                # Split view
                with ui.row().classes('w-full gap-4'):
                    # Image
                    with ui.column().classes('w-1/2'):
                        img_url = get_thumbnail_url(current_page.fl_id, size=800)
                        if img_url:
                            ui.image(img_url).classes('w-full rounded shadow')

                    # Text
                    with ui.column().classes('w-1/2'):
                        with ui.scroll_area().classes('w-full').style('max-height: 65vh'):
                            ui.label(current_page.text or tr('No text available')).classes(
                                'manuscript-text rtl-text hebrew-text w-full'
                            )
            else:
                # Text only
                with ui.card().classes('w-full'):
                    # Page header
                    if current_page.full_header:
                        with ui.row().classes('w-full bg-gray-50 p-2 border-b'):
                            ui.label(current_page.full_header).classes(
                                'text-xs text-gray-500 font-mono'
                            )

                    with ui.scroll_area().classes('w-full').style('max-height: 65vh'):
                        ui.label(current_page.text or tr('No text available')).classes(
                            'manuscript-text rtl-text hebrew-text w-full p-4'
                        )

    # Main layout
    with ui.column().classes('w-full max-w-5xl mx-auto p-4'):
        # Back button
        with ui.row().classes('w-full mb-4'):
            ui.button(
                tr('Back'),
                icon='arrow_back',
                on_click=lambda: ui.navigate.to('/search')
            ).props('flat')

        # Metadata card
        with ui.card().classes('w-full p-4 mb-4'):
            with ui.row().classes('w-full items-start justify-between'):
                with ui.column():
                    # Shelfmark
                    shelfmark_label = ui.label(state.shelfmark or f"ID: {state.sys_id or uid}").classes(
                        'text-xl font-bold text-blue-800'
                    )

                    # Title
                    if state.title:
                        ui.label(state.title).classes(
                            'text-gray-600 rtl-text hebrew-text mt-1'
                        )

                # External links
                if state.sys_id:
                    with ui.row().classes('gap-2'):
                        ktiv_url = f"https://www.nli.org.il/he/discover/manuscripts/hebrew-manuscripts/viewerpage?vid=NNL_ALEPH{state.sys_id}"
                        ui.link(tr('Open in Ktiv'), ktiv_url, new_tab=True).classes('text-blue-600')

                        ui.button(
                            tr('Browse Manuscripts'),
                            icon='menu_book',
                            on_click=lambda: ui.navigate.to(f'/browse/{state.sys_id}')
                        ).props('flat dense')

        # Content container
        content_container = ui.column().classes('w-full')

        # Initial load
        load_document()
        update_content()
