# -*- coding: utf-8 -*-
"""
Personal Lists Management Page for GenizahSearch web application.

Features:
- View all personal lists
- Create/delete/rename lists
- Add/remove items from lists
- View list contents with metadata
- Edit notes and tags for items
- Export lists
- Per-user storage (syncs across devices when logged in)
- Per-device storage (for anonymous users)
"""

import logging

from nicegui import ui
from web.state import state
from web.translations import tr, is_rtl, get_language
from web.components.typography import h1, h2, h3
from web.components.project_tree import create_project_tree
from web.auth_state import GlobalAuthState
from genizah_core import get_library_display
from typing import Optional
import time
import asyncio

logger = logging.getLogger(__name__)


def create_inline_edit_label(
    current_name: str,
    list_id: str,
    is_system: bool,
    lists_mgr,
    tr_func,
    classes: str = 'font-semibold',
    on_save_callback=None
):
    """
    Create an inline-editable label for list names.
    Click to edit, Enter/blur to save, Escape to cancel.
    """
    if is_system:
        # System lists cannot be renamed, just show a label
        ui.label(current_name).classes(classes)
        return

    # Container to hold either the label or the input
    container = ui.element('div').classes('inline-edit-container')

    with container:
        # State for editing mode
        editing_state = {'active': False}

        # The display label (shown when not editing)
        label_el = ui.label(current_name).classes(classes + ' cursor-pointer hover:underline')

        # The input field (hidden initially)
        input_el = ui.input(value=current_name).classes('inline-edit-input').props('dense outlined')
        input_el.set_visibility(False)

        def start_editing():
            """Switch to edit mode."""
            if editing_state['active']:
                return
            editing_state['active'] = True
            input_el.value = label_el.text
            label_el.set_visibility(False)
            input_el.set_visibility(True)
            # Focus the input
            ui.run_javascript(f'document.querySelector("[id=\\"{input_el.id}\\"] input")?.focus(); document.querySelector("[id=\\"{input_el.id}\\"] input")?.select();')

        def save_edit():
            """Save the new name and exit edit mode."""
            if not editing_state['active']:
                return
            new_name = input_el.value.strip()
            if new_name and new_name != label_el.text:
                # Call API to update
                if lists_mgr:
                    lists_mgr.update_list(list_id, name=new_name)
                    label_el.text = new_name
                    ui.notify(f"{tr_func('List renamed to')}: {new_name}", type='positive')
                    if on_save_callback:
                        on_save_callback()
            cancel_edit()

        def cancel_edit():
            """Cancel editing and restore the label."""
            editing_state['active'] = False
            input_el.set_visibility(False)
            label_el.set_visibility(True)

        def handle_keydown(e):
            """Handle keyboard events in the input."""
            if e.args.get('key') == 'Enter':
                save_edit()
            elif e.args.get('key') == 'Escape':
                cancel_edit()

        # Attach event handlers
        label_el.on('click', start_editing, [])
        input_el.on('keydown', handle_keydown)
        input_el.on('blur', save_edit)


def create_lists_page():
    """Create the personal lists management page."""

    # State
    class ListsPageState:
        def __init__(self):
            self.selected_list_id: Optional[str] = None
            self.refresh_trigger: int = 0

    page_state = ListsPageState()

    # Main container references
    lists_sidebar_container = None
    list_content_container = None

    def refresh_ui():
        """Trigger a full UI refresh."""
        page_state.refresh_trigger += 1
        render_lists_sidebar()
        render_list_content()

    async def async_refresh_ui():
        """Async version of refresh_ui - refreshes data from API if authenticated."""
        if GlobalAuthState.is_logged_in() and hasattr(state.lists_mgr, 'refresh_data'):
            try:
                await state.lists_mgr.refresh_data()
            except Exception as e:
                logger.error(f"Error refreshing lists data: {e}")
        refresh_ui()

    # --- Create New List Dialog ---
    def show_create_list_dialog():
        """Show dialog to create a new list."""
        with ui.dialog() as dialog, ui.card().classes('p-6 min-w-[400px]'):
            # Changed to H3
            h3(tr('Create New List'), classes='text-xl font-bold mb-4')

            list_name = ui.input(label=tr('List Name')).classes('w-full mb-4')

            # Color picker
            ui.label(tr('Color')).classes('text-sm font-semibold mb-2')
            selected_color = {'value': '#FFD700'}

            with ui.row().classes('gap-2 mb-4 flex-wrap'):
                colors = ['#FFD700', '#4CAF50', '#2196F3', '#9C27B0', '#FF5722',
                          '#00BCD4', '#E91E63', '#795548', '#607D8B', '#F44336']
                for color in colors:
                    btn = ui.button().props('flat round dense').style(
                        f'background-color: {color}; width: 32px; height: 32px; min-width: 32px;'
                    )
                    btn.on('click', lambda c=color: selected_color.update({'value': c}))

            async def create_list():
                name = list_name.value.strip()
                if not name:
                    ui.notify(tr('Please enter a list name'), type='warning')
                    return

                if state.lists_mgr:
                    # Use async method if authenticated, sync otherwise
                    if GlobalAuthState.is_logged_in():
                        list_id = await state.lists_mgr.create_list(name, color=selected_color['value'])
                    else:
                        list_id = state.lists_mgr.create_list_sync(name, color=selected_color['value'])

                    ui.notify(f"{tr('List created')}: {name}", type='positive')
                    dialog.close()
                    await async_refresh_ui()
                else:
                    ui.notify(tr('Lists manager not available'), type='negative')

            with ui.row().classes('w-full justify-end gap-2 mt-4'):
                ui.button(tr('Cancel'), on_click=dialog.close).props('flat')
                ui.button(tr('Create'), on_click=create_list).classes('bg-primary text-white')

        dialog.open()

    # --- Delete List Dialog ---
    def show_delete_list_dialog(list_id: str, list_name: str):
        """Show confirmation dialog to delete a list."""
        with ui.dialog() as dialog, ui.card().classes('p-6'):
            # Changed to H3
            h3(tr('Delete List?'), classes='text-xl font-bold mb-2')
            ui.label(f"{tr('Are you sure you want to delete')}: {list_name}?").classes('mb-4').style('color: var(--text-secondary);')
            ui.label(tr('All items in this list will be removed.')).classes('text-sm text-red-500 mb-4')

            async def delete_list():
                if state.lists_mgr:
                    if GlobalAuthState.is_logged_in() and hasattr(state.lists_mgr, 'delete_list'):
                        try:
                            await state.lists_mgr.delete_list(list_id)
                        except TypeError:
                            state.lists_mgr.delete_list(list_id)
                    else:
                        state.lists_mgr.delete_list(list_id)
                    ui.notify(f"{tr('List deleted')}: {list_name}", type='info')
                    dialog.close()
                    page_state.selected_list_id = None
                    await async_refresh_ui()

            with ui.row().classes('w-full justify-end gap-2'):
                ui.button(tr('Cancel'), on_click=dialog.close).props('flat')
                ui.button(tr('Delete'), on_click=delete_list).classes('bg-red-500 text-white')

        dialog.open()

    # --- Trash Dialog ---
    def show_trash_dialog():
        """Show dialog with deleted lists (trash)."""
        if not state.lists_mgr:
            return

        # Get deleted lists
        if hasattr(state.lists_mgr, 'get_deleted_lists'):
            deleted_lists = state.lists_mgr.get_deleted_lists()
        else:
            ui.notify(tr('Trash not available'), type='warning')
            return

        with ui.dialog() as dialog, ui.card().classes('p-6 min-w-[500px]'):
            h3(tr('Trash'), classes='text-xl font-bold mb-4')

            if not deleted_lists:
                ui.label(tr('Trash is empty.')).classes('text-gray-500 mb-4')
                ui.button(tr('Close'), on_click=dialog.close).props('flat')
            else:
                ui.label(tr('{} deleted lists').format(len(deleted_lists))).classes('mb-4').style('color: var(--text-secondary);')

                # List of deleted lists
                trash_list = ui.column().classes('w-full max-h-60 overflow-auto border rounded p-2 mb-4')
                selected_list_id = {'value': None}

                def select_item(list_id, btn):
                    selected_list_id['value'] = list_id
                    # Update selection visuals
                    for child in trash_list:
                        if hasattr(child, 'classes'):
                            child.classes(remove='bg-blue-100')
                    btn.classes(add='bg-blue-100')

                with trash_list:
                    for lst in deleted_lists:
                        from datetime import datetime
                        deleted_at = lst.get('deleted_at')
                        if deleted_at:
                            if isinstance(deleted_at, (int, float)):
                                deleted_str = datetime.fromtimestamp(deleted_at).strftime('%Y-%m-%d %H:%M')
                            else:
                                deleted_str = str(deleted_at)[:16]
                        else:
                            deleted_str = tr('Unknown')

                        count = lst.get('count', 0)
                        with ui.row().classes('w-full items-center justify-between p-2 hover:bg-gray-100 rounded cursor-pointer') as row:
                            row.on('click', lambda e, lid=lst['id'], r=row: select_item(lid, r))
                            ui.label(f"{lst['name']} ({count} {tr('items')})")
                            ui.label(f"{tr('Deleted')}: {deleted_str}").classes('text-sm text-gray-500')

                # Action buttons
                with ui.row().classes('w-full justify-end gap-2'):
                    async def restore_selected():
                        if not selected_list_id['value']:
                            ui.notify(tr('Please select a list to restore.'), type='warning')
                            return
                        if hasattr(state.lists_mgr, 'restore_list'):
                            try:
                                await state.lists_mgr.restore_list(selected_list_id['value'])
                            except TypeError:
                                state.lists_mgr.restore_list(selected_list_id['value'])
                            ui.notify(tr('List restored'), type='positive')
                            dialog.close()
                            await async_refresh_ui()

                    async def delete_permanently():
                        if not selected_list_id['value']:
                            ui.notify(tr('Please select a list to delete.'), type='warning')
                            return
                        if hasattr(state.lists_mgr, 'permanently_delete_list'):
                            try:
                                await state.lists_mgr.permanently_delete_list(selected_list_id['value'])
                            except TypeError:
                                state.lists_mgr.permanently_delete_list(selected_list_id['value'])
                            ui.notify(tr('List deleted permanently'), type='info')
                            dialog.close()
                            await async_refresh_ui()

                    async def empty_trash():
                        if hasattr(state.lists_mgr, 'empty_trash'):
                            try:
                                count = await state.lists_mgr.empty_trash()
                            except TypeError:
                                count = state.lists_mgr.empty_trash()
                            ui.notify(tr('Deleted {} lists permanently.').format(count), type='info')
                            dialog.close()
                            await async_refresh_ui()

                    ui.button(tr('Cancel'), on_click=dialog.close).props('flat')
                    ui.button(tr('Restore'), on_click=restore_selected).classes('bg-green-500 text-white')
                    ui.button(tr('Delete Permanently'), on_click=delete_permanently).classes('bg-red-500 text-white')
                    ui.button(tr('Empty Trash'), on_click=empty_trash).classes('bg-red-700 text-white')

        dialog.open()

    # --- Edit Item Dialog ---
    def show_edit_item_dialog(item_id: str, item_data: dict):
        """Show dialog to edit item notes and tags."""
        with ui.dialog() as dialog, ui.card().classes('p-6 min-w-[500px]'):
            # Changed to H3
            h3(tr('Edit Item'), classes='text-xl font-bold mb-2')

            shelfmark = item_data.get('shelfmark', 'Unknown')
            # Get library name for display
            sys_id = item_data.get('sys_id', item_id)
            library_name = ''
            if state.meta_mgr:
                library_code = state.meta_mgr.get_library_for_id(sys_id)
                if library_code:
                    library_name = get_library_display(library_code, short=False, lang=get_language())
            display_shelfmark = f"{library_name}, {shelfmark}" if library_name else shelfmark
            ui.label(f"{tr('Item')}: {display_shelfmark}").classes('text-sm mb-4').style('color: var(--text-secondary);')

            note_input = ui.textarea(
                label=tr('Notes'),
                value=item_data.get('note', '')
            ).classes('w-full mb-4').props('outlined rows=3')

            tags_input = ui.input(
                label=tr('Tags (comma-separated)'),
                value=', '.join(item_data.get('tags', []))
            ).classes('w-full mb-4').props('outlined')

            def save_changes():
                if state.lists_mgr:
                    # Update note
                    if note_input.value != item_data.get('note', ''):
                        state.lists_mgr.update_item_note(item_id, note_input.value)

                    # Update tags
                    new_tags = [t.strip() for t in tags_input.value.split(',') if t.strip()]
                    if new_tags != item_data.get('tags', []):
                        state.lists_mgr.update_item_tags(item_id, new_tags)

                    ui.notify(tr('Item updated'), type='positive')
                    dialog.close()
                    refresh_ui()

            with ui.row().classes('w-full justify-end gap-2'):
                ui.button(tr('Cancel'), on_click=dialog.close).props('flat')
                ui.button(tr('Save'), on_click=save_changes).classes('bg-primary text-white')

        dialog.open()

    # --- Render Lists Sidebar ---
    def render_lists_sidebar():
        """Render the left sidebar with project tree."""
        # Use the new project tree component
        create_project_tree(
            lists_mgr=state.lists_mgr,
            container=lists_sidebar_container,
            on_select=select_list,
            selected_list_id=page_state.selected_list_id,
            on_refresh=lambda: asyncio.create_task(async_refresh_ui())
        )

    # --- Select List ---
    def select_list(list_id: str):
        """Select a list to view its contents."""
        page_state.selected_list_id = list_id
        render_list_content()

    # --- Render List Content ---
    def render_list_content():
        """Render the selected list's content."""
        list_content_container.clear()

        with list_content_container:
            if not page_state.selected_list_id:
                # No list selected
                with ui.column().classes('w-full h-full items-center justify-center'):
                    ui.icon('playlist_add', size='6rem').style('color: var(--text-muted);')
                    ui.label(tr('Select a list to view its contents')).classes('text-xl mt-4').style('color: var(--text-muted);')
                return

            if not state.lists_mgr:
                ui.label(tr('Lists manager not available')).classes('text-red-500')
                return

            list_id = page_state.selected_list_id
            lists = state.lists_mgr.data.get('lists', {})
            list_data = lists.get(list_id)

            if not list_data:
                ui.label(tr('List not found')).classes('text-red-500')
                return

            # List Header
            is_system = list_data.get('is_system', False)
            # Use project-inherited color if available
            display_color = (
                state.lists_mgr.get_list_display_color(list_id)
                if hasattr(state.lists_mgr, 'get_list_display_color')
                else list_data.get('color', '#FFD700')
            )
            with ui.row().classes('w-full justify-between items-start mb-6 pb-4 border-b-2').style(
                f'border-color: {display_color};'
            ):
                with ui.column().classes('gap-1'):
                    with ui.row().classes('items-center gap-3'):
                        ui.icon('circle').style(f'color: {display_color}; font-size: 2rem;')
                        # Inline-editable list name (click to rename)
                        create_inline_edit_label(
                            current_name=list_data.get('name', 'Unnamed'),
                            list_id=list_id,
                            is_system=is_system,
                            lists_mgr=state.lists_mgr,
                            tr_func=tr,
                            classes='text-3xl font-bold',
                            on_save_callback=refresh_ui
                        )
                    if is_system:
                        ui.label(tr('System List')).classes('text-xs').style('color: var(--text-tertiary);')

                # Export button
                with ui.row().classes('gap-2'):
                    ui.button(
                        tr('Export'),
                        icon='download',
                        on_click=lambda: export_list(list_id)
                    ).props('flat').classes('text-primary')

            # Get items
            items_list = state.lists_mgr.get_items_in_list_sync(list_id)
            items_data = [(item.get('item_id'), item) for item in items_list]

            if not items_data:
                with ui.column().classes('w-full items-center justify-center py-16'):
                    ui.icon('inbox', size='4rem').style('color: var(--text-muted);')
                    ui.label(tr('This list is empty')).classes('text-lg mt-2').style('color: var(--text-muted);')
                    if not list_data.get('is_system'):
                        ui.label(tr('Add items from search results')).classes('text-sm').style('color: var(--text-muted);')
                return

            # Items Grid/List
            ui.label(f"{len(items_data)} {tr('items')}").classes('text-sm mb-4').style('color: var(--text-tertiary);')

            # Track expanded items
            expanded_items = {}

            with ui.column().classes('w-full gap-3'):
                for item_id, item_data in items_data:
                    sys_id = item_data.get('sys_id', item_id)
                    shelfmark = item_data.get('shelfmark', 'Unknown')
                    title = item_data.get('title', '')
                    note = item_data.get('note', '')
                    tags = item_data.get('tags', [])
                    fl_id = item_data.get('fl_id')

                    # Enrich metadata if needed
                    if not shelfmark or shelfmark == 'Unknown':
                        if state.meta_mgr:
                            shelf_temp, title_temp = state.meta_mgr.get_meta_for_id(sys_id)
                            shelfmark = shelf_temp or shelfmark
                            title = title or title_temp

                    # Get library name for display
                    library_name = ''
                    if state.meta_mgr:
                        library_code = state.meta_mgr.get_library_for_id(sys_id)
                        if library_code:
                            library_name = get_library_display(library_code, short=False, lang=get_language())

                    # Build display shelfmark with library name
                    display_shelfmark = shelfmark
                    if library_name:
                        display_shelfmark = f"{library_name}, {shelfmark}"

                    with ui.card().classes('w-full p-4 hover:shadow-lg transition-shadow'):
                        with ui.row().classes('w-full justify-between items-start'):
                            # Main content
                            with ui.column().classes('flex-grow gap-2'):
                                # Shelfmark with library name
                                h3(display_shelfmark, classes='text-lg font-bold text-primary')

                                # Title
                                if title:
                                    ui.label(title).classes('text-sm').style('direction: rtl; color: var(--text-secondary);')

                                # Note
                                if note:
                                    with ui.row().classes('items-start gap-2 p-2 mt-2 rounded').style('background: var(--bg-tertiary);'):
                                        ui.icon('note', size='xs').style('color: var(--text-muted);')
                                        ui.label(note).classes('text-xs').style('color: var(--text-secondary);')

                                # Tags
                                if tags:
                                    with ui.row().classes('gap-2 mt-2 flex-wrap'):
                                        for tag in tags:
                                            ui.badge(tag).classes('bg-blue-100 text-blue-700')

                            # Actions
                            with ui.column().classes('gap-1'):
                                # Browse button
                                ui.button(
                                    icon='menu_book',
                                    on_click=lambda sid=sys_id: ui.navigate.to(f'/browse?sys_id={sid}')
                                ).props('flat round dense').tooltip(tr('Browse'))

                                # Edit button
                                ui.button(
                                    icon='edit',
                                    on_click=lambda iid=item_id, idata=item_data: show_edit_item_dialog(iid, idata)
                                ).props('flat round dense').tooltip(tr('Edit'))

                                # Remove button
                                if not list_data.get('is_system'):
                                    ui.button(
                                        icon='delete',
                                        on_click=lambda iid=item_id, lid=list_id: remove_item_from_list(iid, lid)
                                    ).props('flat round dense').classes('text-red-400').tooltip(tr('Remove'))

                        # Text snippet section (expandable)
                        with ui.column().classes('w-full mt-3'):
                            snippet_container = ui.column().classes('w-full')
                            is_expanded = {'value': False}

                            def create_snippet_ui(container, sid, fid, expanded_state):
                                """Create the snippet UI with lazy loading."""
                                container.clear()
                                with container:
                                    # Try to get text snippet
                                    text_snippet = ''
                                    try:
                                        from web.services import get_service
                                        service = get_service()
                                        if service.is_ready:
                                            page_data = service.get_browse_page(sid, p_num=1)
                                            if page_data and page_data.text:
                                                text_snippet = page_data.text
                                    except Exception as e:
                                        logger.error(f"Error fetching snippet: {e}")

                                    if text_snippet:
                                        # Show snippet or full text
                                        max_chars = 200
                                        with ui.element('div').classes(
                                            'p-3 rounded-lg'
                                        ).style('direction: rtl; text-align: right; background: var(--bg-tertiary); border: 1px solid var(--border-light);'):
                                            if expanded_state['value'] or len(text_snippet) <= max_chars:
                                                ui.label(text_snippet).classes(
                                                    'text-sm whitespace-pre-wrap'
                                                ).style('line-height: 1.8; color: var(--text-primary);')
                                            else:
                                                ui.label(text_snippet[:max_chars] + '...').classes(
                                                    'text-sm'
                                                ).style('line-height: 1.8; color: var(--text-primary);')

                                            # Expand/collapse button
                                            if len(text_snippet) > max_chars:
                                                def toggle_expand():
                                                    expanded_state['value'] = not expanded_state['value']
                                                    create_snippet_ui(container, sid, fid, expanded_state)

                                                with ui.row().classes('w-full justify-center mt-2'):
                                                    btn_text = tr('Show less') if expanded_state['value'] else tr('Show more')
                                                    btn_icon = 'expand_less' if expanded_state['value'] else 'expand_more'
                                                    ui.button(
                                                        btn_text,
                                                        icon=btn_icon,
                                                        on_click=toggle_expand
                                                    ).props('flat dense size=sm').classes('text-green-700')
                                    else:
                                        ui.label(tr('No text preview available')).classes(
                                            'text-xs italic'
                                        ).style('color: var(--text-muted);')

                            # Load snippet button (lazy load to avoid slow page)
                            load_btn_container = ui.row().classes('w-full')
                            with load_btn_container:
                                def make_load_handler(container, sid, fid, expanded, btn_container):
                                    def handler():
                                        btn_container.clear()
                                        with container:
                                            ui.spinner(size='sm').classes('mx-auto')
                                        ui.timer(0.1, lambda: create_snippet_ui(container, sid, fid, expanded), once=True)
                                    return handler

                                ui.button(
                                    tr('Show text preview'),
                                    icon='text_snippet',
                                    on_click=make_load_handler(snippet_container, sys_id, fl_id, is_expanded, load_btn_container)
                                ).props('flat dense size=sm').style('color: var(--text-tertiary);')

    async def remove_item_from_list(item_id: str, list_id: str):
        """Remove an item from the current list."""
        if state.lists_mgr:
            if GlobalAuthState.is_logged_in() and hasattr(state.lists_mgr, 'remove_item_from_list'):
                try:
                    result = await state.lists_mgr.remove_item_from_list(item_id, list_id)
                except TypeError:
                    result = state.lists_mgr.remove_item_from_list_sync(item_id, list_id)
            else:
                result = state.lists_mgr.remove_item_from_list(item_id, list_id)

            if result:
                ui.notify(tr('Item removed from list'), type='info')
                await async_refresh_ui()

    def export_list(list_id: str):
        """Export list to Excel."""
        if state.lists_mgr:
            try:
                list_data = state.lists_mgr.data.get('lists', {}).get(list_id)
                if not list_data:
                    ui.notify(tr('List not found'), type='warning')
                    return

                # Use get_items_in_list to correctly fetch items
                # Items are stored in data['items'] with list membership in each item's 'lists' field
                items = state.lists_mgr.get_items_in_list_sync(list_id)
                if not items:
                    ui.notify(tr('This list is empty'), type='warning')
                    return

                # Trigger download
                ui.download(f'/api/export/list/{list_id}/excel')
            except Exception as e:
                ui.notify(f"{tr('Export failed')}: {str(e)}", type='negative')

    # --- Migration Dialog ---
    async def show_migration_dialog():
        """Show dialog to migrate local lists to user account."""
        with ui.dialog() as dialog, ui.card().classes('p-6 min-w-[500px]'):
            h3(tr('Sync Your Lists'), classes='text-xl font-bold mb-4')
            ui.label(tr('You have local lists that can be synced to your account.')).classes('mb-2')
            ui.label(tr('This will make them available on all your devices.')).classes('mb-4').style('color: var(--text-secondary);')

            async def do_migration():
                if hasattr(state.lists_mgr, 'migrate_local_to_user'):
                    result = await state.lists_mgr.migrate_local_to_user()
                    if 'error' not in result:
                        ui.notify(
                            f"{tr('Migration complete')}: {result.get('lists_migrated', 0)} {tr('lists')}, "
                            f"{result.get('items_migrated', 0)} {tr('items')}",
                            type='positive'
                        )
                        dialog.close()
                        await async_refresh_ui()
                    else:
                        ui.notify(f"{tr('Migration failed')}: {result.get('error')}", type='negative')
                else:
                    ui.notify(tr('Migration not available'), type='warning')

            with ui.row().classes('w-full justify-end gap-2'):
                ui.button(tr('Later'), on_click=dialog.close).props('flat')
                ui.button(tr('Sync Now'), on_click=do_migration).classes('bg-primary text-white')

        dialog.open()

    # --- Main Layout ---
    with ui.column().classes('w-full h-[calc(100vh-120px)]'):
        # Page Title
        with ui.row().classes('w-full items-center justify-between mb-4'):
            # Changed to H1
            h1(tr('Personal Lists'), classes='text-3xl font-bold text-green-800')
            with ui.row().classes('items-center gap-2'):
                # Show sync status
                if GlobalAuthState.is_logged_in():
                    ui.icon('cloud_done', size='sm').classes('text-green-600').tooltip(tr('Synced to your account'))
                else:
                    ui.icon('cloud_off', size='sm').classes('text-gray-400').tooltip(tr('Local storage only - log in to sync'))
                ui.button(
                    tr('Create List'),
                    icon='add',
                    on_click=show_create_list_dialog
                ).classes('bg-primary text-white')
                ui.button(
                    tr('Trash'),
                    icon='delete',
                    on_click=show_trash_dialog
                ).props('flat').classes('text-gray-600')

        # Description with sync status
        if GlobalAuthState.is_logged_in():
            ui.label(tr('Your lists are synced across all your devices')).classes('mb-4').style('color: var(--text-secondary);')
        else:
            with ui.row().classes('items-center gap-2 mb-4'):
                ui.label(tr('Lists are stored locally.')).style('color: var(--text-secondary);')
                ui.link(tr('Log in to sync across devices'), '/').classes('text-primary underline')

        # Check for migration opportunity (logged in with local lists)
        if GlobalAuthState.is_logged_in():
            local_mgr = state.get_local_lists_mgr()
            if local_mgr and hasattr(state.lists_mgr, 'has_local_lists'):
                if state.lists_mgr.has_local_lists():
                    with ui.card().classes('w-full p-4 mb-4 bg-blue-50 border-l-4 border-blue-500'):
                        with ui.row().classes('items-center gap-3'):
                            ui.icon('sync', size='md').classes('text-blue-600')
                            with ui.column().classes('flex-grow'):
                                ui.label(tr('Local Lists Available')).classes('font-semibold text-blue-800')
                                ui.label(tr('You have lists stored on this device. Sync them to your account?')).classes('text-sm text-blue-600')
                            ui.button(tr('Sync Now'), on_click=show_migration_dialog).classes('bg-blue-500 text-white')

        # Main Content: Sidebar + Content
        with ui.splitter(value=25).classes('w-full flex-grow') as splitter:
            # Left: Lists Sidebar
            with splitter.before:
                lists_sidebar_container = ui.column().classes('w-full h-full p-2')

            # Right: List Content
            with splitter.after:
                list_content_container = ui.column().classes('w-full h-full p-4')

    # Initial render
    render_lists_sidebar()
    render_list_content()
