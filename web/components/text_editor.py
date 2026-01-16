# -*- coding: utf-8 -*-
"""
Text Editor Component

Provides inline text editing functionality with:
- Original text display (read-only)
- Editable text area
- Save locally (localStorage) option
- Save & Submit (to backend) option
"""

from nicegui import ui, app
from web.translations import tr
from web.auth_state import GlobalAuthState, api_call
from typing import Optional, Callable
import json


# localStorage key prefix for local edits
LOCAL_EDITS_KEY = 'genizah_local_edits'


def get_local_edits() -> dict:
    """Get all local edits from storage."""
    return app.storage.user.get(LOCAL_EDITS_KEY, {})


def save_local_edit(document_id: str, page_number: int, text: str, original_text: str):
    """Save an edit locally."""
    edits = get_local_edits()
    key = f"{document_id}_{page_number}"
    edits[key] = {
        "document_id": document_id,
        "page_number": page_number,
        "text": text,
        "original_text": original_text,
        "timestamp": str(__import__('datetime').datetime.now())
    }
    app.storage.user[LOCAL_EDITS_KEY] = edits


def get_local_edit(document_id: str, page_number: int) -> Optional[dict]:
    """Get a local edit if it exists."""
    edits = get_local_edits()
    key = f"{document_id}_{page_number}"
    return edits.get(key)


def delete_local_edit(document_id: str, page_number: int):
    """Delete a local edit."""
    edits = get_local_edits()
    key = f"{document_id}_{page_number}"
    if key in edits:
        del edits[key]
        app.storage.user[LOCAL_EDITS_KEY] = edits


def create_edit_text_dialog(
    document_id: str,
    page_number: int,
    original_text: str,
    shelfmark: str = "",
    on_save: Optional[Callable] = None
):
    """
    Create a text editing dialog.

    Args:
        document_id: System ID of the document
        page_number: Page number within the document
        original_text: The original text to edit
        shelfmark: Display name for the document
        on_save: Optional callback when text is saved/submitted

    Returns:
        The dialog object
    """
    dialog = ui.dialog().props('maximized')

    # Check for existing local edit
    local_edit = get_local_edit(document_id, page_number)
    initial_text = local_edit['text'] if local_edit else original_text

    with dialog:
        with ui.card().classes('w-full h-full'):
            # Header
            with ui.row().classes('w-full items-center justify-between p-4 border-b'):
                with ui.column().classes('gap-0'):
                    ui.label(tr('Edit Text')).classes('text-xl font-bold')
                    ui.label(f"{shelfmark} - {tr('Page')} {page_number}").classes('text-sm text-gray-500')

                ui.button(icon='close', on_click=dialog.close).props('flat round')

            # Content
            with ui.row().classes('w-full flex-grow p-4 gap-4').style('height: calc(100% - 140px);'):
                # Original text (read-only)
                with ui.column().classes('flex-1 h-full'):
                    ui.label(tr('Original')).classes('font-bold text-sm mb-2')
                    ui.textarea(value=original_text).classes('w-full h-full font-mono text-sm').props(
                        'readonly outlined'
                    ).style('direction: rtl; text-align: right;')

                # Editable text
                with ui.column().classes('flex-1 h-full'):
                    with ui.row().classes('items-center gap-2 mb-2'):
                        ui.label(tr('Corrected')).classes('font-bold text-sm')
                        if local_edit:
                            ui.badge(tr('Local draft')).props('color=warning')

                    edited_textarea = ui.textarea(value=initial_text).classes('w-full h-full font-mono text-sm').props(
                        'outlined'
                    ).style('direction: rtl; text-align: right;')

            # Footer with actions
            with ui.row().classes('w-full items-center justify-between p-4 border-t'):
                # Left: Info about permissions
                with ui.row().classes('items-center gap-2'):
                    if not GlobalAuthState.is_logged_in():
                        ui.icon('info').classes('text-orange-500')
                        ui.label(tr('Login to submit corrections')).classes('text-sm text-orange-500')
                    elif not GlobalAuthState.can_edit():
                        ui.icon('info').classes('text-blue-500')
                        ui.label(tr('Pending approval')).classes('text-sm text-blue-500')

                # Right: Action buttons
                with ui.row().classes('gap-2'):
                    if local_edit:
                        async def discard_local():
                            delete_local_edit(document_id, page_number)
                            ui.notify(tr('Local draft deleted'), type='info')
                            dialog.close()
                            if on_save:
                                on_save()

                        ui.button(tr('Discard local changes'), on_click=discard_local).props('flat color=negative')

                    async def save_locally():
                        text = edited_textarea.value
                        if text == original_text:
                            ui.notify(tr('No changes to save'), type='warning')
                            return
                        save_local_edit(document_id, page_number, text, original_text)
                        ui.notify(tr('Your changes have been saved locally'), type='positive')
                        dialog.close()
                        if on_save:
                            on_save()

                    ui.button(tr('Save Locally'), icon='save', on_click=save_locally).props('outline')

                    async def save_and_submit():
                        if not GlobalAuthState.is_logged_in():
                            ui.notify(tr('Please login first'), type='negative')
                            return

                        text = edited_textarea.value
                        if text == original_text:
                            ui.notify(tr('No changes to save'), type='warning')
                            return

                        # Submit to backend
                        result = await api_call("POST", "/corrections/", {
                            "document_id": document_id,
                            "page_number": page_number,
                            "original_text": original_text,
                            "corrected_text": text,
                            "correction_type": "text_correction",
                            "notes": None
                        })

                        if "error" in result:
                            ui.notify(result["error"], type='negative')
                        else:
                            # Clear local edit if exists
                            delete_local_edit(document_id, page_number)
                            ui.notify(tr('Your changes have been submitted for review'), type='positive')
                            dialog.close()
                            if on_save:
                                on_save()

                    submit_btn = ui.button(tr('Save & Submit'), icon='send', on_click=save_and_submit).props('color=primary')
                    if not GlobalAuthState.is_logged_in():
                        submit_btn.props('disable')

    return dialog


def create_edit_button(
    document_id: str,
    page_number: int,
    original_text: str,
    shelfmark: str = "",
    on_save: Optional[Callable] = None,
    size: str = "sm"
):
    """
    Create an edit text button that opens the edit dialog.

    Args:
        document_id: System ID of the document
        page_number: Page number within the document
        original_text: The original text to edit
        shelfmark: Display name for the document
        on_save: Optional callback when text is saved/submitted
        size: Button size (sm, md, lg)

    Returns:
        The button element
    """
    # Check for local edit
    local_edit = get_local_edit(document_id, page_number)

    def open_editor():
        dialog = create_edit_text_dialog(
            document_id=document_id,
            page_number=page_number,
            original_text=original_text,
            shelfmark=shelfmark,
            on_save=on_save
        )
        dialog.open()

    with ui.row().classes('items-center gap-1'):
        btn = ui.button(
            tr('Edit Text'),
            icon='edit',
            on_click=open_editor
        ).props(f'flat dense size={size}').classes('text-xs')

        if local_edit:
            ui.badge('').props('color=warning floating').classes('w-2 h-2')

    return btn
