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

    # READER-03 DIAG P1 (Phase 92.1 Plan 02 Task 1 -- Reviews R2-2, temporary,
    # Task 2 removes this). Capture nicegui.storage.request_contextvar state at
    # dialog OPENER time. Compare with P2 (button registration) and P3 (handler
    # firing) to identify which Reviews R2-2 case (a/b/c/d) the failure falls
    # into. The captured `id()` lets us prove whether the SAME request is bound
    # across all three points.
    try:
        import logging as _logging_p1
        from nicegui.storage import request_contextvar as _rcv_p1
        _r1 = _rcv_p1.get()
        _logging_p1.getLogger('web.components.add_to_list_dialog').warning(
            "[READER-03 DIAG P1] dialog-entry request_contextvar=%s",
            'None' if _r1 is None else 'bound:%s' % id(_r1),
        )
    except Exception:
        pass

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
                    # READER-03 DIAG P3 (Phase 92.1 Plan 02 Task 1 -- Reviews R2-2,
                    # temporary, Task 2 removes this). Capture request_contextvar
                    # state at HANDLER FIRING time. This is the PRIMARY diagnostic
                    # signal: if P3 is None, the contextvar is unbound at click time
                    # and `app.storage.user` access raises the UI-context error.
                    try:
                        import logging as _logging_p3
                        from nicegui.storage import request_contextvar as _rcv_p3
                        _r3 = _rcv_p3.get()
                        _logging_p3.getLogger('web.components.add_to_list_dialog').warning(
                            "[READER-03 DIAG P3] handler-firing request_contextvar=%s",
                            'None' if _r3 is None else 'bound:%s' % id(_r3),
                        )
                    except Exception:
                        pass

                    name = new_list_name.value.strip()
                    project_id = selected_project['value']
                    if not name:
                        ui.notify(tr('Please enter a list name'), type='warning')
                        return

                    # Create the new list - use async if authenticated, sync otherwise
                    # Color is inherited from project or defaults to gold for standalone
                    is_logged_in = GlobalAuthState.is_logged_in()

                    # READER-03 DIAG P3.5 (Phase 92.1 Plan 02 Task 1 -- Revision Warning 7,
                    # temporary, Task 2 removes this). Capture traceback immediately before
                    # the failing call so we know which function chain reached
                    # safe_user_get('auth_session') at the moment the UI-context error fires.
                    try:
                        import traceback as _traceback_p35
                        import logging as _logging_p35
                        from nicegui.storage import request_contextvar as _rcv_p35
                        _r35 = _rcv_p35.get()
                        _logging_p35.getLogger('web.components.add_to_list_dialog').warning(
                            "[READER-03 DIAG P3.5] pre-create_list request_contextvar=%s, stack:\n%s",
                            'None' if _r35 is None else 'bound:%s' % id(_r35),
                            ''.join(_traceback_p35.format_stack()[-5:]),
                        )
                    except Exception:
                        pass

                    try:
                        if is_logged_in:
                            new_list_id = await lists_mgr.create_list(name, project_id=project_id)
                        else:
                            new_list_id = lists_mgr.create_list_sync(name, project_id=project_id)
                    except Exception as e:
                        ui.notify(f"Error: {e}", type='negative')
                        return

                    if new_list_id:
                        # Add item to the new list
                        try:
                            if is_logged_in:
                                result = await lists_mgr.add_item(sys_id, new_list_id, note=new_list_note_input.value, fl_id=fl_id)
                            else:
                                result = lists_mgr.add_item_sync(sys_id, new_list_id, note=new_list_note_input.value, fl_id=fl_id)
                        except Exception as e:
                            ui.notify(f"Error: {e}", type='negative')
                            return

                        if result:
                            ui.notify(f"{tr('List created')}: {name}", type='positive')
                            ui.notify(tr('Added to list'), type='positive')
                            dialog.close()
                            if on_success:
                                on_success()
                        else:
                            ui.notify(tr('Failed to add item'), type='negative')
                    else:
                        ui.notify(tr('Failed to create list'), type='negative')

                # READER-03 DIAG P2 (Phase 92.1 Plan 02 Task 1 -- Reviews R2-2,
                # temporary, Task 2 removes this). Capture request_contextvar
                # state at BUTTON REGISTRATION time. If P1==bound and P2==bound
                # but P3==None, case-c applies (capture ctx at button registration).
                # If P1==bound and P2==None, case-b applies (capture at dialog entry).
                try:
                    import logging as _logging_p2
                    from nicegui.storage import request_contextvar as _rcv_p2
                    _r2 = _rcv_p2.get()
                    _logging_p2.getLogger('web.components.add_to_list_dialog').warning(
                        "[READER-03 DIAG P2] button-registration request_contextvar=%s",
                        'None' if _r2 is None else 'bound:%s' % id(_r2),
                    )
                except Exception:
                    pass

                ui.button(tr('Create and Add'), on_click=create_and_add).classes('btn-primary')

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

            add_btn = ui.button(tr('Add'), on_click=do_add).classes('btn-primary')

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
