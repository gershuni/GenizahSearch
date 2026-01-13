# -*- coding: utf-8 -*-
"""
Browse manuscripts page for GenizahSearch web application.
Navigate through manuscript pages by shelfmark.
"""

from nicegui import ui
from typing import Optional

from web.services import get_service, BrowsePage, get_thumbnail_url
from web.translations import tr, is_rtl


class BrowseState:
    """Holds the state for the browse page."""

    def __init__(self):
        self.shelfmark_query: str = ''
        self.current_page: Optional[BrowsePage] = None
        self.sys_id: Optional[str] = None
        self.is_loading: bool = False
        self.error: Optional[str] = None
        self.show_image: bool = False


def create_browse_page(initial_sys_id: Optional[str] = None):
    """Create the browse page UI."""
    state = BrowseState()
    service = get_service()
    content_container = None

    if initial_sys_id:
        state.sys_id = initial_sys_id

    def search_shelfmark():
        """Search for manuscripts by shelfmark."""
        if not state.shelfmark_query.strip():
            return

        state.is_loading = True
        state.error = None
        update_content()

        try:
            # Search by shelfmark
            results = service.search_by_shelfmark(state.shelfmark_query.strip(), limit=20)

            if results:
                # Navigate to first result
                state.sys_id = results[0].sys_id
                load_page()
            else:
                state.error = tr('No manuscript found')
                state.is_loading = False
                update_content()

        except Exception as e:
            state.error = str(e)
            state.is_loading = False
            update_content()

    def load_page(direction: int = 0, p_num: Optional[int] = None):
        """Load a page of the manuscript."""
        if not state.sys_id:
            return

        state.is_loading = True
        state.error = None

        try:
            if p_num is not None:
                page = service.browse_page(state.sys_id, p_num=p_num)
            elif state.current_page:
                # Navigate relative to current page
                page = service.browse_page(
                    state.sys_id,
                    p_num=state.current_page.p_num,
                    direction=direction
                )
            else:
                # Load first page
                page = service.browse_page(state.sys_id, p_num=1)

            if page:
                state.current_page = page
                state.error = None
            else:
                state.error = tr('No text available')

        except Exception as e:
            state.error = str(e)

        finally:
            state.is_loading = False
            update_content()

    def go_to_page(new_page: int):
        """Navigate to a specific page number."""
        load_page(p_num=new_page)

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

            if state.error and not state.current_page:
                with ui.card().classes('w-full p-8 text-center'):
                    ui.icon('error', size='3rem').classes('text-red-500')
                    ui.label(state.error).classes('text-red-600 mt-2')
                return

            if not state.current_page:
                # Show search prompt
                with ui.column().classes('w-full items-center py-12'):
                    ui.icon('menu_book', size='4rem').classes('text-amber-400')
                    ui.label(tr('Enter a shelfmark to browse the manuscript')).classes(
                        'text-gray-600 mt-4 rtl-text'
                    )
                return

            page = state.current_page

            # Metadata header
            with ui.card().classes('w-full p-4 mb-4'):
                with ui.row().classes('w-full items-start justify-between'):
                    with ui.column():
                        ui.label(page.shelfmark or f"ID: {page.sys_id}").classes(
                            'text-xl font-bold text-amber-800'
                        )
                        if page.title:
                            ui.label(page.title).classes(
                                'text-gray-600 rtl-text hebrew-text'
                            )

                    # External links
                    with ui.row().classes('gap-2'):
                        # Ktiv link
                        ktiv_url = f"https://www.nli.org.il/he/discover/manuscripts/hebrew-manuscripts/viewerpage?vid=NNL_ALEPH{page.sys_id}"
                        ui.link(
                            tr('Open in Ktiv'),
                            ktiv_url,
                            new_tab=True
                        ).classes('text-blue-600')

            # Navigation controls
            with ui.row().classes('w-full items-center justify-between mb-4'):
                # Previous button
                ui.button(
                    icon='chevron_right' if is_rtl() else 'chevron_left',
                    on_click=lambda: load_page(direction=-1)
                ).props('flat round').bind_enabled_from(
                    state, 'current_page', lambda p: p and p.current_idx > 1
                )

                # Page info and selector
                with ui.row().classes('items-center gap-2'):
                    ui.label(tr('Page')).classes('text-gray-600')
                    page_input = ui.number(
                        value=page.p_num,
                        min=1,
                        max=page.total_pages
                    ).classes('w-20').props('dense')

                    @page_input.on('keydown.enter')
                    def on_page_enter():
                        go_to_page(int(page_input.value))

                    ui.label(f"{tr('of')} {page.total_pages}").classes('text-gray-600')

                    ui.button(
                        tr('Go'),
                        on_click=lambda: go_to_page(int(page_input.value))
                    ).props('flat dense')

                # Next button
                ui.button(
                    icon='chevron_left' if is_rtl() else 'chevron_right',
                    on_click=lambda: load_page(direction=1)
                ).props('flat round').bind_enabled_from(
                    state, 'current_page', lambda p: p and p.current_idx < p.total_pages
                )

            # Image toggle button
            if page.fl_id:
                with ui.row().classes('w-full justify-end mb-2'):
                    ui.button(
                        tr('Hide image') if state.show_image else tr('Show image'),
                        icon='image',
                        on_click=toggle_image
                    ).props('flat')

            # Main content area
            if state.show_image and page.fl_id:
                # Split view: image + text
                with ui.row().classes('w-full gap-4'):
                    # Image
                    with ui.column().classes('w-1/2'):
                        img_url = get_thumbnail_url(page.fl_id, size=800)
                        if img_url:
                            ui.image(img_url).classes('w-full rounded shadow')
                        else:
                            ui.label(tr('Image not available')).classes('text-gray-500')

                    # Text
                    with ui.column().classes('w-1/2'):
                        with ui.scroll_area().classes('w-full').style('max-height: 70vh'):
                            ui.label(page.text or tr('No text available')).classes(
                                'manuscript-text rtl-text hebrew-text w-full'
                            )
            else:
                # Text only
                with ui.card().classes('w-full'):
                    # Page header info
                    if page.full_header:
                        with ui.row().classes('w-full bg-gray-50 p-2 border-b'):
                            ui.label(page.full_header).classes(
                                'text-xs text-gray-500 font-mono'
                            )

                    with ui.scroll_area().classes('w-full').style('max-height: 70vh'):
                        ui.label(page.text or tr('No text available')).classes(
                            'manuscript-text rtl-text hebrew-text w-full p-4'
                        )

    # Main layout
    with ui.column().classes('w-full max-w-5xl mx-auto p-4'):
        # Page title
        ui.label(tr('Browse Manuscripts')).classes(
            'text-3xl font-bold mb-6 text-center text-amber-800'
        )

        # Search box
        with ui.card().classes('w-full p-4 mb-6'):
            with ui.row().classes('w-full gap-4 items-end'):
                search_input = ui.input(
                    placeholder=tr('e.g. T-S 8J6.1'),
                    label=tr('Enter shelfmark'),
                    value=state.shelfmark_query
                ).classes('flex-1').props('outlined dense clearable')

                search_input.bind_value(state, 'shelfmark_query')
                search_input.on('keydown.enter', search_shelfmark)

                ui.button(
                    tr('Go'),
                    icon='search',
                    on_click=search_shelfmark
                ).props('color=amber')

        # Service status
        if not service.is_ready:
            with ui.card().classes('w-full p-4 bg-yellow-50 mb-4'):
                with ui.row().classes('items-center gap-2'):
                    ui.icon('warning', color='orange')
                    ui.label(tr('Service not available')).classes('text-yellow-800')

        # Content container
        content_container = ui.column().classes('w-full')

        # Load initial page if sys_id provided
        if initial_sys_id:
            load_page()
        else:
            update_content()
