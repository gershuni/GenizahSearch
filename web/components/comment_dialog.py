# -*- coding: utf-8 -*-
"""
Comment Dialog Component

Provides functionality to add comments to:
- A specific page
- An entire manuscript

Comments can be public or private.
"""

from nicegui import ui
from web.translations import tr
from web.auth_state import GlobalAuthState, api_call
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

    with dialog, ui.card().classes('w-96 p-6'):
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

            # Visibility
            ui.label(tr('Comment visibility')).classes('font-medium text-sm')
            visibility_options = {
                'public': tr('Public'),
                'private': tr('Private')
            }
            visibility_select = ui.radio(visibility_options, value='public').props('inline')

            # Comment text
            comment_text = ui.textarea(
                label=tr('Comment'),
                placeholder=tr('Write your comment here...')
            ).classes('w-full').props('outlined rows=4').style('direction: rtl; text-align: right;')

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

                    # Determine the scope
                    target_type = 'document'
                    target_page = None
                    if scope_select.value == 'page' and page_number:
                        target_type = 'page'
                        target_page = page_number

                    # Submit to backend
                    result = await api_call("POST", "/comments/", {
                        "document_id": document_id,
                        "content": comment_text.value.strip(),
                        "is_public": visibility_select.value == 'public',
                        "target_type": target_type,
                        "page_number": target_page
                    })

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
