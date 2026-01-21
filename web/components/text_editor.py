# -*- coding: utf-8 -*-
"""
Text Editor Component - Enhanced Version

Provides inline text editing functionality with:
- Original text display (read-only) with visual distinction
- Editable text area with live change detection
- Character/word count
- Auto-save drafts
- Notes field for correction comments
- Copy from original functionality
- Save locally (localStorage) option
- Save & Submit (to backend) option
"""

from nicegui import ui, app
from web.translations import tr
from web.auth_state import GlobalAuthState, api_call
from typing import Optional, Callable
from datetime import datetime
import json


# localStorage key prefix for local edits
LOCAL_EDITS_KEY = 'genizah_local_edits'

# Auto-save interval in seconds
AUTO_SAVE_INTERVAL = 30


def get_local_edits() -> dict:
    """Get all local edits from storage."""
    return app.storage.user.get(LOCAL_EDITS_KEY, {})


def save_local_edit(document_id: str, page_number: int, text: str, original_text: str, notes: str = ""):
    """Save an edit locally."""
    edits = get_local_edits()
    key = f"{document_id}_{page_number}"
    edits[key] = {
        "document_id": document_id,
        "page_number": page_number,
        "text": text,
        "original_text": original_text,
        "notes": notes,
        "timestamp": str(datetime.now())
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


def count_words(text: str) -> int:
    """Count words in Hebrew/mixed text."""
    if not text:
        return 0
    # Split on whitespace and filter empty strings
    words = [w for w in text.split() if w.strip()]
    return len(words)


def count_changes(original: str, edited: str) -> dict:
    """Count approximate changes between original and edited text."""
    orig_words = set(original.split()) if original else set()
    edit_words = set(edited.split()) if edited else set()

    added = len(edit_words - orig_words)
    removed = len(orig_words - edit_words)

    return {"added": added, "removed": removed}


def create_edit_text_dialog(
    document_id: str,
    page_number: int,
    original_text: str,
    shelfmark: str = "",
    on_save: Optional[Callable] = None,
    image_url: Optional[str] = None
):
    """
    Create an enhanced text editing dialog.

    Args:
        document_id: System ID of the document
        page_number: Page number within the document
        original_text: The original text to edit
        shelfmark: Display name for the document
        on_save: Optional callback when text is saved/submitted
        image_url: Optional URL for manuscript image to display

    Returns:
        The dialog object
    """
    dialog = ui.dialog().props('maximized persistent')

    # Check for existing local edit
    local_edit = get_local_edit(document_id, page_number)
    initial_text = local_edit['text'] if local_edit else original_text
    initial_notes = local_edit.get('notes', '') if local_edit else ''

    # State tracking
    has_unsaved_changes = {'value': False}
    auto_save_timer = {'timer': None}

    with dialog:
        with ui.card().classes('w-full h-full').style('display: flex; flex-direction: column;'):
            # ============================================
            # HEADER
            # ============================================
            with ui.row().classes('w-full items-center justify-between p-4 border-b bg-gray-50'):
                with ui.column().classes('gap-1'):
                    with ui.row().classes('items-center gap-2'):
                        ui.icon('edit_note').classes('text-2xl text-primary')
                        ui.label(tr('Edit Transcription')).classes('text-xl font-bold')
                    with ui.row().classes('items-center gap-2'):
                        ui.icon('description', size='xs').classes('text-gray-400')
                        ui.label(f"{shelfmark}").classes('text-sm text-gray-600')
                        ui.label('•').classes('text-gray-400')
                        ui.label(f"{tr('Page')} {page_number}").classes('text-sm text-gray-600')

                with ui.row().classes('items-center gap-2'):
                    # Status indicators
                    if local_edit:
                        with ui.row().classes('items-center gap-1 px-2 py-1 bg-orange-100 rounded'):
                            ui.icon('drafts', size='xs').classes('text-orange-600')
                            ui.label(tr('Draft saved')).classes('text-xs text-orange-600')

                    # Close button with confirmation
                    async def close_with_confirm():
                        if has_unsaved_changes['value']:
                            with ui.dialog() as confirm_dialog, ui.card():
                                ui.label(tr('Unsaved changes')).classes('text-lg font-bold')
                                ui.label(tr('You have unsaved changes. Are you sure you want to close?')).classes('text-sm')
                                with ui.row().classes('justify-end gap-2 mt-4'):
                                    ui.button(tr('Cancel'), on_click=confirm_dialog.close).props('flat')
                                    def close_both():
                                        confirm_dialog.close()
                                        dialog.close()
                                    ui.button(tr('Close without saving'), on_click=close_both).props('flat color=negative')
                            confirm_dialog.open()
                        else:
                            dialog.close()

                    ui.button(icon='close', on_click=close_with_confirm).props('flat round')

            # ============================================
            # TOOLBAR
            # ============================================
            with ui.row().classes('w-full items-center justify-between px-4 py-2 border-b bg-white'):
                with ui.row().classes('items-center gap-4'):
                    # Character count displays
                    char_count_label = ui.label(f"{len(initial_text)} {tr('characters')}").classes('text-xs text-gray-500')
                    word_count_label = ui.label(f"{count_words(initial_text)} {tr('words')}").classes('text-xs text-gray-500')
                    changes_label = ui.label('').classes('text-xs text-blue-600')

                with ui.row().classes('items-center gap-2'):
                    # Copy from original button
                    async def copy_original():
                        edited_textarea.value = original_text
                        update_counts()
                        ui.notify(tr('Original text copied'), type='info')

                    ui.button(tr('Reset to original'), icon='restart_alt', on_click=copy_original).props(
                        'flat dense size=sm'
                    ).classes('text-xs')

                    # Keyboard shortcuts help
                    with ui.button(icon='keyboard', on_click=lambda: shortcuts_dialog.open()).props('flat dense size=sm'):
                        ui.tooltip(tr('Keyboard shortcuts'))

            # ============================================
            # MAIN CONTENT - Side by Side (Image + Text)
            # ============================================
            with ui.element('div').classes('w-full p-4 gap-4').style(
                'flex: 1; display: flex; flex-direction: row; min-height: 0; overflow: hidden;'
            ):
                # LEFT SIDE - Manuscript Image (or Original Text if no image)
                with ui.column().style('flex: 1; min-width: 0; height: 100%;'):
                    if image_url:
                        with ui.row().classes('items-center gap-2 mb-2'):
                            ui.icon('image', size='xs').classes('text-gray-400')
                            ui.label(tr('Manuscript Image')).classes('font-bold text-sm text-gray-700')

                        with ui.card().classes('w-full').style(
                            'background: #1a1a1a; flex: 1; overflow: auto; display: flex; align-items: center; justify-content: center;'
                        ):
                            ui.image(image_url).style('max-width: 100%; max-height: 100%; object-fit: contain;')
                    else:
                        # Fallback to original text if no image
                        with ui.row().classes('items-center gap-2 mb-2'):
                            ui.icon('lock', size='xs').classes('text-gray-400')
                            ui.label(tr('Original Text')).classes('font-bold text-sm text-gray-700')
                            ui.label(f"({tr('read-only')})").classes('text-xs text-gray-400')

                        with ui.card().classes('w-full').style(
                            'background: #f8f9fa; border: 1px solid #e9ecef; flex: 1; overflow: auto;'
                        ):
                            ui.textarea(value=original_text).classes(
                                'w-full h-full font-mono text-sm'
                            ).props('readonly borderless').style(
                                'direction: rtl; text-align: right; resize: none; '
                                'background: transparent; color: #495057; height: 100%;'
                            )

                # Divider
                ui.element('div').style('width: 1px; background: #e0e0e0; align-self: stretch;')

                # RIGHT SIDE - Editable text
                with ui.column().style('flex: 1; min-width: 0; height: 100%;'):
                    with ui.row().classes('items-center gap-2 mb-2'):
                        ui.icon('edit', size='xs').classes('text-primary')
                        ui.label(tr('Your Correction')).classes('font-bold text-sm text-primary')
                        unsaved_indicator = ui.label('').classes('text-xs text-orange-500')

                    with ui.card().classes('w-full').style(
                        'border: 2px solid var(--q-primary); flex: 1; overflow: hidden;'
                    ):
                        edited_textarea = ui.textarea(value=initial_text).classes(
                            'w-full h-full font-mono text-sm'
                        ).props('borderless autofocus').style(
                            'direction: rtl; text-align: right; resize: none; height: 100%;'
                        )

            # ============================================
            # NOTES SECTION
            # ============================================
            with ui.expansion(tr('Add notes about your correction'), icon='note_add').classes(
                'w-full px-4'
            ).props('dense'):
                notes_textarea = ui.textarea(
                    value=initial_notes,
                    placeholder=tr('Explain what you changed and why (optional)')
                ).classes('w-full').props('outlined dense').style('direction: rtl;')

            # ============================================
            # FOOTER
            # ============================================
            with ui.row().classes('w-full items-center justify-between p-4 border-t bg-gray-50'):
                # Left: Info about permissions
                with ui.row().classes('items-center gap-2'):
                    if not GlobalAuthState.is_logged_in():
                        ui.icon('info').classes('text-orange-500')
                        ui.label(tr('Login to submit corrections to the system')).classes('text-sm text-orange-500')
                    elif GlobalAuthState.is_admin() or GlobalAuthState.is_editor():
                        ui.icon('check_circle').classes('text-green-500')
                        role_name = 'Admin' if GlobalAuthState.is_admin() else 'Editor'
                        ui.label(f'{role_name} - {tr("Changes apply immediately")}').classes('text-sm text-green-500')
                    else:
                        ui.icon('info').classes('text-blue-500')
                        ui.label(tr('Your corrections will be reviewed before publishing')).classes('text-sm text-blue-500')

                # Right: Action buttons
                with ui.row().classes('gap-2'):
                    # Discard local changes (if exists)
                    if local_edit:
                        async def discard_local():
                            delete_local_edit(document_id, page_number)
                            ui.notify(tr('Local draft deleted'), type='info')
                            has_unsaved_changes['value'] = False
                            dialog.close()
                            if on_save:
                                on_save()

                        ui.button(tr('Discard draft'), icon='delete_outline', on_click=discard_local).props(
                            'flat color=negative'
                        )

                    # Save locally
                    async def save_locally():
                        text = edited_textarea.value
                        notes = notes_textarea.value
                        if text == original_text and not notes:
                            ui.notify(tr('No changes to save'), type='warning')
                            return
                        save_local_edit(document_id, page_number, text, original_text, notes)
                        has_unsaved_changes['value'] = False
                        unsaved_indicator.text = ''
                        ui.notify(tr('Draft saved locally'), type='positive')

                    ui.button(tr('Save Draft'), icon='save', on_click=save_locally).props('outline')

                    # Submit to backend
                    async def save_and_submit():
                        if not GlobalAuthState.is_logged_in():
                            ui.notify(tr('Please login first'), type='negative')
                            return

                        text = edited_textarea.value
                        notes = notes_textarea.value

                        if text == original_text:
                            ui.notify(tr('No changes to submit'), type='warning')
                            return

                        # Show loading
                        submit_btn.props('loading')

                        # Submit to backend
                        result = await api_call("POST", "/corrections/", {
                            "document_id": document_id,
                            "system_id": document_id,
                            "shelfmark": shelfmark if shelfmark else None,
                            "page_number": page_number,
                            "original_text": original_text,
                            "corrected_text": text,
                            "correction_type": "text_correction",
                            "notes": notes if notes else None
                        })

                        submit_btn.props(remove='loading')

                        if "error" in result:
                            ui.notify(result["error"], type='negative')
                        else:
                            # Clear local edit if exists
                            delete_local_edit(document_id, page_number)
                            has_unsaved_changes['value'] = False

                            # Show different message based on role
                            if GlobalAuthState.is_admin() or GlobalAuthState.is_editor():
                                ui.notify(tr('Correction published successfully'), type='positive')
                            else:
                                ui.notify(tr('Correction submitted for review'), type='positive')

                            dialog.close()
                            if on_save:
                                on_save()

                    submit_btn = ui.button(
                        tr('Submit Correction'),
                        icon='send',
                        on_click=save_and_submit
                    ).props('color=primary')

                    if not GlobalAuthState.is_logged_in():
                        submit_btn.props('disable')

            # ============================================
            # Helper functions
            # ============================================
            def update_counts():
                text = edited_textarea.value
                char_count_label.text = f"{len(text)} {tr('characters')}"
                word_count_label.text = f"{count_words(text)} {tr('words')}"

                # Show change indicator
                if text != original_text:
                    changes = count_changes(original_text, text)
                    if changes['added'] > 0 or changes['removed'] > 0:
                        changes_label.text = f"+{changes['added']} / -{changes['removed']} {tr('words changed')}"
                    has_unsaved_changes['value'] = True
                    unsaved_indicator.text = f"• {tr('unsaved')}"
                else:
                    changes_label.text = ''
                    has_unsaved_changes['value'] = False
                    unsaved_indicator.text = ''

            # Bind text change handler
            edited_textarea.on('input', lambda: update_counts())

            # Auto-save timer - saves draft locally every AUTO_SAVE_INTERVAL seconds
            def do_auto_save():
                """Periodic auto-save callback for NiceGUI timer."""
                if has_unsaved_changes['value']:
                    text = edited_textarea.value
                    notes = notes_textarea.value
                    if text != original_text:
                        save_local_edit(document_id, page_number, text, original_text, notes)
                        ui.notify(tr('Auto-saved'), type='info', position='bottom-right', timeout=1500)

            # Start auto-save timer - runs every AUTO_SAVE_INTERVAL seconds
            ui.timer(AUTO_SAVE_INTERVAL, do_auto_save)

        # ============================================
        # Keyboard shortcuts dialog
        # ============================================
        shortcuts_dialog = ui.dialog()
        with shortcuts_dialog, ui.card().classes('p-4'):
            ui.label(tr('Keyboard Shortcuts')).classes('text-lg font-bold mb-4')
            shortcuts = [
                ('Ctrl + S', tr('Save draft locally')),
                ('Ctrl + Enter', tr('Submit correction')),
                ('Ctrl + Z', tr('Undo')),
                ('Ctrl + Shift + Z', tr('Redo')),
                ('Esc', tr('Close editor')),
            ]
            for key, desc in shortcuts:
                with ui.row().classes('items-center gap-4 py-1'):
                    ui.label(key).classes('font-mono bg-gray-100 px-2 py-1 rounded text-sm')
                    ui.label(desc).classes('text-sm')
            ui.button(tr('Close'), on_click=shortcuts_dialog.close).props('flat').classes('mt-4')

    return dialog


def create_edit_button(
    document_id: str,
    page_number: int,
    original_text: str,
    shelfmark: str = "",
    on_save: Optional[Callable] = None,
    size: str = "sm",
    image_url: Optional[str] = None
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
        image_url: Optional URL for manuscript image

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
            on_save=on_save,
            image_url=image_url
        )
        dialog.open()

    with ui.row().classes('items-center gap-1'):
        btn = ui.button(
            tr('Edit'),
            icon='edit',
            on_click=open_editor
        ).props(f'flat dense size={size}').classes('text-xs')

        if local_edit:
            ui.badge(tr('draft')).props('color=warning floating').classes('text-xs')

    return btn
