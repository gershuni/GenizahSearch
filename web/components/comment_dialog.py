# -*- coding: utf-8 -*-
"""
Comment Dialog Component

Provides functionality to add comments to:
- A specific page
- An entire manuscript

Comments can be public or private.
Supports shelfmark mentions in format: [[shelfmark:T-S 8J6.1|id:123456]]
"""

from nicegui import ui
from web.translations import tr
from web.auth_state import GlobalAuthState, api_call
from web.state import state
from typing import Optional, Callable


def create_comment_dialog(
    document_id: str,
    page_number: Optional[int] = None,
    shelfmark: str = "",
    on_submit: Optional[Callable] = None
):
    """
    Create a comment submission dialog.

    Args:
        document_id: System ID of the document/manuscript
        page_number: Page number (None for manuscript-level comment)
        shelfmark: Display name for the document
        on_submit: Optional callback when comment is submitted

    Returns:
        The dialog object
    """
    dialog = ui.dialog()

    with dialog, ui.card().classes('w-[450px] p-6'):
        # Header
        ui.label(tr('Send Comment')).classes('text-xl font-bold mb-4')

        with ui.column().classes('w-full gap-4'):
            # Document info
            ui.label(f"{shelfmark}").classes('text-sm text-gray-500')

            # Scope selection
            ui.label(tr('Comment Scope')).classes('font-medium text-sm')
            scope_options = {
                'page': tr('This page only'),
                'manuscript': tr('Entire manuscript')
            }
            scope_select = ui.radio(
                scope_options,
                value='page' if page_number else 'manuscript'
            ).props('inline')

            # If no page number provided, only allow manuscript scope
            if not page_number:
                scope_select.props('disable')
                scope_select.value = 'manuscript'

            # Comment text
            comment_text = ui.textarea(
                label=tr('Comment'),
                placeholder=tr('Write your comment here...')
            ).classes('w-full').props('outlined rows=4').style('direction: rtl; text-align: right;')

            # Shelfmark mention button
            def show_shelfmark_picker():
                """Show dialog to select shelfmarks to mention."""
                picker_dialog = ui.dialog()

                with picker_dialog, ui.card().classes('w-96 p-4'):
                    ui.label(tr('Add shelfmark reference')).classes('font-bold mb-3')

                    # Tabs for Recent Activity and Lists
                    with ui.tabs().classes('w-full') as tabs:
                        recent_tab = ui.tab('recent', label=tr('Recent Activity'))
                        lists_tab = ui.tab('lists', label=tr('My Lists'))

                    with ui.tab_panels(tabs, value='recent').classes('w-full'):
                        with ui.tab_panel('recent'):
                            recent_container = ui.column().classes('w-full gap-1')

                            def load_recent():
                                recent_container.clear()
                                if state.lists_mgr:
                                    recent_items = state.lists_mgr.data.get('recent_items', [])
                                    if recent_items:
                                        with recent_container:
                                            for item in recent_items[:20]:
                                                doc_id = item.get('sys_id', '')
                                                item_shelfmark = item.get('shelfmark', doc_id)

                                                def make_add(sm=item_shelfmark, did=doc_id):
                                                    def add():
                                                        # Insert mention at cursor
                                                        mention = f"[[shelfmark:{sm}|id:{did}]]"
                                                        current = comment_text.value or ''
                                                        comment_text.value = current + ' ' + mention + ' '
                                                        picker_dialog.close()
                                                    return add

                                                with ui.card().classes('w-full p-2 cursor-pointer hover:bg-gray-100').on('click', make_add()):
                                                    ui.label(item_shelfmark).classes('font-medium text-sm')
                                    else:
                                        with recent_container:
                                            ui.label(tr('No recent activity')).classes('text-gray-500 text-sm')
                                else:
                                    with recent_container:
                                        ui.label(tr('Lists not available')).classes('text-gray-500 text-sm')

                            load_recent()

                        with ui.tab_panel('lists'):
                            lists_container = ui.column().classes('w-full gap-1')

                            def load_lists():
                                lists_container.clear()
                                if state.lists_mgr:
                                    lists = state.lists_mgr.data.get('lists', {})
                                    if lists:
                                        with lists_container:
                                            for list_id, list_data in lists.items():
                                                list_name = list_data.get('name', list_id)
                                                color = list_data.get('color', '#999')

                                                def make_show_list(lid=list_id, lname=list_name):
                                                    def show_list():
                                                        lists_container.clear()
                                                        items = state.lists_mgr.get_items_in_list(lid)
                                                        with lists_container:
                                                            # Back button
                                                            ui.button(tr('Back'), icon='arrow_back', on_click=load_lists).props('flat dense size=sm').classes('mb-2')
                                                            ui.label(lname).classes('font-bold mb-2')

                                                            if items:
                                                                for item in items:
                                                                    doc_id = item.get('sys_id', '')
                                                                    item_shelfmark = item.get('shelfmark', doc_id)

                                                                    def make_add(sm=item_shelfmark, did=doc_id):
                                                                        def add():
                                                                            mention = f"[[shelfmark:{sm}|id:{did}]]"
                                                                            current = comment_text.value or ''
                                                                            comment_text.value = current + ' ' + mention + ' '
                                                                            picker_dialog.close()
                                                                        return add

                                                                    with ui.card().classes('w-full p-2 cursor-pointer hover:bg-gray-100').on('click', make_add()):
                                                                        ui.label(item_shelfmark).classes('font-medium text-sm')
                                                            else:
                                                                ui.label(tr('No items in this list')).classes('text-gray-500 text-sm')
                                                    return show_list

                                                with ui.card().classes('w-full p-2 cursor-pointer hover:bg-gray-100').on('click', make_show_list()):
                                                    with ui.row().classes('items-center gap-2'):
                                                        ui.icon('circle').style(f'color: {color}; font-size: 0.8rem;')
                                                        ui.label(list_name).classes('font-medium text-sm')
                                    else:
                                        with lists_container:
                                            ui.label(tr('No lists found')).classes('text-gray-500 text-sm')
                                else:
                                    with lists_container:
                                        ui.label(tr('Lists not available')).classes('text-gray-500 text-sm')

                            load_lists()

                    ui.button(tr('Cancel'), on_click=picker_dialog.close).props('flat').classes('mt-3')

                picker_dialog.open()

            with ui.row().classes('w-full'):
                ui.button(tr('Add shelfmark reference'), icon='link', on_click=show_shelfmark_picker).props('flat dense size=sm')

            # Private comment option
            private_check = ui.checkbox(tr('Private comment (only visible to me)'), value=False).classes('text-sm')

            # Error message
            error_label = ui.label('').classes('text-red-500 text-sm hidden')

            # Actions
            with ui.row().classes('w-full justify-end gap-2 mt-4'):
                ui.button(tr('Cancel'), on_click=dialog.close).props('flat')

                async def submit_comment():
                    if not GlobalAuthState.is_logged_in():
                        error_label.text = tr('Please login first')
                        error_label.classes('visible', remove='hidden')
                        return

                    if not comment_text.value or not comment_text.value.strip():
                        error_label.text = tr('Please enter a comment')
                        error_label.classes('visible', remove='hidden')
                        return

                    # Build comment data with valid fields
                    # line_number is used for page-specific comments
                    comment_data = {
                        "document_id": document_id,
                        "content": comment_text.value.strip(),
                        "comment_type": "general",
                        "is_public": not private_check.value
                    }

                    # If page-specific comment, set line_number to page number for tracking
                    if scope_select.value == 'page' and page_number:
                        comment_data["line_number"] = page_number

                    # Submit to backend
                    result = await api_call("POST", "/comments/", comment_data)

                    if "error" in result:
                        error_label.text = result.get("error", "Error submitting comment")
                        error_label.classes('visible', remove='hidden')
                    else:
                        ui.notify(tr('Comment submitted successfully'), type='positive')
                        dialog.close()
                        if on_submit:
                            on_submit()

                submit_btn = ui.button(tr('Submit'), icon='send', on_click=submit_comment).props('color=primary')

                if not GlobalAuthState.is_logged_in():
                    submit_btn.props('disable')
                    ui.label(tr('Login to submit comments')).classes('text-xs text-orange-500')

    return dialog


def create_comment_button(
    document_id: str,
    page_number: Optional[int] = None,
    shelfmark: str = "",
    on_submit: Optional[Callable] = None,
    size: str = "sm"
):
    """
    Create a comment button that opens the comment dialog.

    Args:
        document_id: System ID of the document
        page_number: Page number (None for manuscript-level)
        shelfmark: Display name for the document
        on_submit: Optional callback when comment is submitted
        size: Button size (sm, md, lg)

    Returns:
        The button element
    """
    def open_comment_dialog():
        dialog = create_comment_dialog(
            document_id=document_id,
            page_number=page_number,
            shelfmark=shelfmark,
            on_submit=on_submit
        )
        dialog.open()

    return ui.button(
        tr('Send Comment'),
        icon='comment',
        on_click=open_comment_dialog
    ).props(f'flat dense size={size}').classes('text-xs')
