# -*- coding: utf-8 -*-
"""
Add to List Dialog Component for GenizahSearch web application.

Provides a reusable dialog for adding items to personal lists with:
- List selection dropdown
- Option to create a new list inline
- Optional note field (empty by default)
- Per-user storage when logged in
- Per-device storage for anonymous users
"""

import asyncio
import contextvars

from nicegui import ui
from web.translations import tr, get_language
from web.components.typography import h3
from web.auth_state import GlobalAuthState
from web.state import state
from genizah_core import get_library_display
from typing import Optional, Callable


def get_star_icon(lists_mgr, sys_id: str) -> str:
    """
    Return the appropriate star icon based on whether item is in a list.

    Args:
        lists_mgr: The lists manager instance
        sys_id: System ID of the item

    Returns:
        'star' if item is in any list, 'star_border' otherwise
    """
    if not lists_mgr or not sys_id:
        return 'star_border'
    try:
        if lists_mgr.is_item_in_any_list(sys_id):
            return 'star'
    except Exception:
        pass  # Operation failed; non-fatal, continue with defaults
    return 'star_border'


async def _create_and_add_handler(
    *,
    name: str,
    project_id: Optional[str],
    lists_mgr,
    sys_id: str,
    fl_id: Optional[str],
    new_list_note_value: str,
    dialog,
    on_success: Optional[Callable] = None,
    is_logged_in: bool,
) -> bool:
    """Phase 92.1 READER-03: extracted from inline closure so the
    create-new-list-and-add-item flow is unit-testable without instantiating a
    ui.dialog (which requires a full NiceGUI client context).

    Returns True on success (list created + item added + ``dialog.close()``
    invoked), False on every failure path (with a ``ui.notify`` already shown
    to the user). The handler MUST be invoked inside a re-bound NiceGUI
    request context (see ``show_add_to_list_dialog`` for the
    ``contextvars.copy_context()`` rebind that wraps both this handler and
    ``do_add``).
    """
    if not name:
        ui.notify(tr('Please enter a list name'), type='warning')
        return False
    try:
        if is_logged_in:
            new_list_id = await lists_mgr.create_list(name, project_id=project_id)
        else:
            new_list_id = lists_mgr.create_list_sync(name, project_id=project_id)
    except Exception as e:
        ui.notify(f"Error: {e}", type='negative')
        return False
    if not new_list_id:
        ui.notify(tr('Failed to create list'), type='negative')
        return False
    try:
        if is_logged_in:
            result = await lists_mgr.add_item(
                sys_id, new_list_id, note=new_list_note_value, fl_id=fl_id,
            )
        else:
            result = lists_mgr.add_item_sync(
                sys_id, new_list_id, note=new_list_note_value, fl_id=fl_id,
            )
    except Exception as e:
        ui.notify(f"Error: {e}", type='negative')
        return False
    if not result:
        ui.notify(tr('Failed to add item'), type='negative')
        return False
    ui.notify(f"{tr('List created')}: {name}", type='positive')
    ui.notify(tr('Added to list'), type='positive')
    dialog.close()
    if on_success:
        on_success()
    return True


def show_add_to_list_dialog(
    sys_id: str,
    shelfmark: str,
    lists_mgr,
    note_default: str = '',
    fl_id: str = None,
    on_success: Optional[Callable] = None
):
    """
    Show a dialog to add an item to a personal list.

    Args:
        sys_id: System ID of the item to add
        shelfmark: Display name/shelfmark of the item
        lists_mgr: The lists manager instance (state.lists_mgr)
        note_default: Default value for the note field (empty by default)
        fl_id: Optional FL ID for page-specific additions
        on_success: Optional callback after successful addition
    """
    if not sys_id:
        ui.notify(tr('Cannot add: missing system ID'), type='warning')
        return

    if not lists_mgr:
        ui.notify(tr('Lists manager not available'), type='warning')
        return

    # READER-03 (Phase 92.1) -- Reviews C2 + R2-2 case-b (2026-05-17): capture the
    # request context at dialog-entry time. The nested async on_click handlers
    # (create_and_add, do_add) execute under NiceGUI's event-listener dispatch,
    # which can leave `nicegui.storage.request_contextvar.get() is None` at
    # handler firing -- causing `safe_user_get('auth_session')` to fail with
    # "app.storage.user can only be used within a UI context" (Phase 92 SWEEP-05
    # smoke run 1 Symptom 3). We snapshot the live `contextvars.Context` here
    # (which includes the bound `request_contextvar`) and re-enter it when the
    # handlers fire. The snapshot lets `safe_user_get` read LIVE storage at
    # click time; we do NOT freeze the auth_session value itself (Reviews
    # H-AGREED-1 -- stale-token / logout-after-dialog-open footgun).
    _captured_ctx = contextvars.copy_context()

    # State for inline list creation
    creating_new_list = {'active': False}

    with ui.dialog() as dialog, ui.card().classes('p-6 min-w-96'):
        h3(tr('Add to List'), classes='text-xl font-bold mb-2')
        # Get library name for display
        library_name = ''
        if state.meta_mgr:
            library_code = state.meta_mgr.get_library_for_id(sys_id)
            if library_code:
                library_name = get_library_display(library_code, short=False, lang=get_language())
        display_shelfmark = f"{library_name}, {shelfmark}" if library_name else shelfmark
        ui.label(f"{tr('Item')}: {display_shelfmark}").style('color: var(--text-secondary);')

        lists = lists_mgr.data.get('lists', {})
        # Store list data with colors for display
        list_data = {lid: lst for lid, lst in lists.items() if not lst.get('is_system')}
        list_options = {lid: lst['name'] for lid, lst in list_data.items()}

        # Container for the main form
        form_container = ui.column().classes('w-full mt-4 gap-3')

        # Container for new list creation form
        new_list_container = ui.column().classes('w-full mt-4 gap-3')
        new_list_container.set_visibility(False)

        with form_container:
            if not list_options:
                ui.label(tr('No lists yet. Create your first list!')).style('color: var(--text-muted);')
            else:
                # Simple dict format: {value: label} - most reliable in NiceGUI
                simple_options = {'__new__': f"+ {tr('Create new list')}"}
                simple_options.update(list_options)

                initial_value = list(list_options.keys())[0] if list_options else '__new__'

                selected_list = ui.select(
                    simple_options,
                    label=tr('Select List'),
                    value=initial_value
                ).classes('w-full').props('outlined').style('color: var(--text-primary);')

                note_input = ui.input(label=tr('Note (optional)'), value=note_default).classes('w-full').props('outlined')

                def on_list_change():
                    if selected_list.value == '__new__':
                        form_container.set_visibility(False)
                        new_list_container.set_visibility(True)
                        action_row.set_visibility(False)
                        creating_new_list['active'] = True

                selected_list.on('update:model-value', on_list_change)

            # Create New List button (shown when no lists exist)
            if not list_options:
                def show_new_list_form():
                    form_container.set_visibility(False)
                    new_list_container.set_visibility(True)
                    action_row.set_visibility(False)

                ui.button(
                    tr('Create new list'),
                    icon='add',
                    on_click=show_new_list_form
                ).classes('btn-primary')

        # New list creation form
        with new_list_container:
            ui.label(tr('Create New List')).classes('font-semibold').style('color: var(--text-primary);')

            new_list_name = ui.input(label=tr('List Name')).classes('w-full').props('outlined')

            # Project selector (optional - lists can be standalone or in a project)
            selected_project = {'value': None}
            projects = lists_mgr.data.get('projects', {}) if lists_mgr else {}

            if projects:
                project_options = {None: tr('(No project - standalone)')}
                for pid, pdata in projects.items():
                    project_options[pid] = pdata.get('name', 'Unnamed')

                ui.label(tr('Add to project (optional)')).classes('text-sm mt-2').style('color: var(--text-secondary);')
                project_select = ui.select(
                    project_options,
                    value=None,
                    label=tr('Project')
                ).classes('w-full').props('outlined')

                def on_project_change():
                    selected_project['value'] = project_select.value

                project_select.on('update:model-value', on_project_change)

            # Color hint - no picker, colors come from projects
            ui.label(tr('Color is inherited from project, or gold for standalone lists.')).classes(
                'text-xs mt-2'
            ).style('color: var(--text-muted);')

            # Note field for new list creation
            new_list_note_input = ui.input(label=tr('Note (optional)'), value=note_default).classes('w-full mt-3').props('outlined')

            with ui.row().classes('w-full justify-end gap-2 mt-4'):
                def back_to_list_selection():
                    new_list_container.set_visibility(False)
                    form_container.set_visibility(True)
                    action_row.set_visibility(True)
                    creating_new_list['active'] = False

                # Only show back button if there are existing lists
                if list_options:
                    ui.button(tr('Back'), on_click=back_to_list_selection).props('flat')

                async def create_and_add():
                    # READER-03 (Phase 92.1) -- thin closure that delegates to
                    # the module-level `_create_and_add_handler` so the body is
                    # unit-testable without ui.dialog instantiation. The
                    # `_bound_create_and_add` wrapper below re-enters the
                    # dialog-entry-captured context so safe_user_get can read
                    # live auth_session at click time.
                    await _create_and_add_handler(
                        name=new_list_name.value.strip(),
                        project_id=selected_project['value'],
                        lists_mgr=lists_mgr,
                        sys_id=sys_id,
                        fl_id=fl_id,
                        new_list_note_value=new_list_note_input.value,
                        dialog=dialog,
                        on_success=on_success,
                        is_logged_in=GlobalAuthState.is_logged_in(),
                    )

                async def _bound_create_and_add():
                    # READER-03 (Phase 92.1) -- Reviews M-R2-1 + M-R2-2 case-b
                    # (2026-05-17): schedule the click handler inside the
                    # dialog-entry-captured context so `request_contextvar`
                    # resolves at click time. asyncio.create_task(coro,
                    # context=ctx) is Python 3.11+ and gives back an awaitable
                    # Task so NiceGUI can await completion and surface
                    # exceptions cleanly. We deliberately avoid fire-and-forget
                    # task creation patterns that drop exceptions on the floor.
                    task = asyncio.create_task(create_and_add(), context=_captured_ctx)
                    return await task

                ui.button(tr('Create and Add'), on_click=_bound_create_and_add).classes('btn-primary')

        # Action buttons for existing list selection
        with ui.row().classes('w-full justify-end gap-2 mt-6') as action_row:
            ui.button(tr('Cancel'), on_click=dialog.close).props('flat')

            async def do_add():
                if not list_options or creating_new_list['active']:
                    return

                if selected_list.value == '__new__':
                    # Switch to new list creation
                    form_container.set_visibility(False)
                    new_list_container.set_visibility(True)
                    action_row.set_visibility(False)
                    creating_new_list['active'] = True
                    return

                # Add item - use async if authenticated, sync otherwise
                if GlobalAuthState.is_logged_in():
                    result = await lists_mgr.add_item(sys_id, selected_list.value, note=note_input.value, fl_id=fl_id)
                else:
                    result = lists_mgr.add_item_sync(sys_id, selected_list.value, note=note_input.value, fl_id=fl_id)

                if result:
                    ui.notify(tr('Added to list'), type='positive')
                    dialog.close()
                    if on_success:
                        on_success()
                else:
                    ui.notify(tr('Already in list'), type='info')

            async def _bound_do_add():
                # READER-03 (Phase 92.1) -- Reviews M-R2-2 case-b (2026-05-17):
                # do_add transits the SAME failing storage chain as
                # create_and_add (lists_mgr.add_item -> get_user_client ->
                # safe_user_get('auth_session')), so it MUST share the
                # dialog-entry-captured context via asyncio.create_task(coro,
                # context=ctx) + await.
                task = asyncio.create_task(do_add(), context=_captured_ctx)
                return await task

            add_btn = ui.button(tr('Add'), on_click=_bound_do_add).classes('btn-primary')

            # Hide add button when no lists exist (user must create one)
            if not list_options:
                add_btn.set_visibility(False)

    dialog.open()
    return dialog


def create_add_to_list_button(
    sys_id: str,
    shelfmark: str,
    lists_mgr,
    note_default: str = '',
    fl_id: str = None,
    size: str = 'md',
    tooltip_text: str = None
):
    """
    Create a star button that opens the add to list dialog.

    Args:
        sys_id: System ID of the item
        shelfmark: Display name/shelfmark of the item
        lists_mgr: The lists manager instance
        note_default: Default value for the note field
        fl_id: Optional FL ID for page-specific additions
        size: Button size ('sm', 'md', 'lg')
        tooltip_text: Optional custom tooltip text

    Returns:
        The button element
    """
    # Check if item is already in a list
    is_in_list = False
    if lists_mgr and sys_id:
        try:
            is_in_list = lists_mgr.is_item_in_any_list(sys_id)
        except Exception:
            pass  # Tooltip metadata optional; item still valid

    # Use filled star if in list, outline if not
    icon = 'star' if is_in_list else 'star_border'

    def show_dialog():
        nonlocal is_in_list
        def on_success():
            nonlocal is_in_list
            # Update icon after adding to list
            is_in_list = True
            btn._props['icon'] = 'star'
            btn.update()

        show_add_to_list_dialog(
            sys_id=sys_id,
            shelfmark=shelfmark,
            lists_mgr=lists_mgr,
            note_default=note_default,
            fl_id=fl_id,
            on_success=on_success
        )

    props = 'flat round dense'
    if size == 'sm':
        props += ' size=sm'

    tooltip = tooltip_text or (tr('In List') if is_in_list else tr('Add to List'))

    btn = ui.button(
        icon=icon,
        on_click=show_dialog
    ).props(props).style('color: var(--accent-amber);').tooltip(tooltip)

    return btn
