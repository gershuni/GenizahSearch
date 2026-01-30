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
from web.translations import tr
from web.components.typography import h3
from web.auth_state import GlobalAuthState
from typing import Optional, Callable
import asyncio


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
    print(f"[DEBUG] show_add_to_list_dialog called: sys_id={sys_id}, shelfmark={shelfmark}")
    print(f"[DEBUG] lists_mgr={lists_mgr}, fl_id={fl_id}")
    if not sys_id:
        ui.notify(tr('Cannot add: missing system ID'), type='warning')
        return

    if not lists_mgr:
        ui.notify(tr('Lists manager not available'), type='warning')
        return

    # State for inline list creation
    creating_new_list = {'active': False}

    with ui.dialog() as dialog, ui.card().classes('p-6 min-w-96'):
        h3(tr('Add to List'), classes='text-xl font-bold mb-2')
        ui.label(f"{tr('Item')}: {shelfmark}").style('color: var(--text-secondary);')

        lists = lists_mgr.data.get('lists', {})
        print(f"[DEBUG] lists_mgr.data = {lists_mgr.data}")
        # Store list data with colors for display
        list_data = {lid: lst for lid, lst in lists.items() if not lst.get('is_system')}
        list_options = {lid: lst['name'] for lid, lst in list_data.items()}
        print(f"[DEBUG] list_data = {list_data}")
        print(f"[DEBUG] list_options = {list_options}")

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
                print(f"[DEBUG] simple_options = {simple_options}")
                print(f"[DEBUG] initial_value = {initial_value}")

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

            # Color picker with visual selection indicator
            ui.label(tr('Color')).classes('text-sm mt-2').style('color: var(--text-secondary);')
            selected_color = {'value': '#4CAF50'}
            color_buttons = {}

            def select_color(color):
                selected_color['value'] = color
                # Update visual indicator for all buttons
                for c, btn in color_buttons.items():
                    if c == color:
                        btn.style(f'background-color: {c}; width: 28px; height: 28px; min-width: 28px; border: 3px solid white; box-shadow: 0 0 0 2px {c};')
                    else:
                        btn.style(f'background-color: {c}; width: 28px; height: 28px; min-width: 28px;')

            with ui.row().classes('gap-2 flex-wrap'):
                colors = ['#FFD700', '#4CAF50', '#2196F3', '#9C27B0', '#FF5722',
                          '#00BCD4', '#E91E63', '#795548', '#607D8B', '#F44336']
                for color in colors:
                    # Default style, with selection indicator for initial color
                    is_selected = color == selected_color['value']
                    style = f'background-color: {color}; width: 28px; height: 28px; min-width: 28px;'
                    if is_selected:
                        style = f'background-color: {color}; width: 28px; height: 28px; min-width: 28px; border: 3px solid white; box-shadow: 0 0 0 2px {color};'
                    btn = ui.button().props('flat round dense').style(style)
                    btn.on('click', lambda c=color: select_color(c))
                    color_buttons[color] = btn

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
                    name = new_list_name.value.strip()
                    print(f"[DEBUG] create_and_add called, name={name}, color={selected_color['value']}")
                    if not name:
                        ui.notify(tr('Please enter a list name'), type='warning')
                        return

                    # Create the new list - use async if authenticated, sync otherwise
                    is_logged_in = GlobalAuthState.is_logged_in()
                    print(f"[DEBUG] is_logged_in={is_logged_in}")
                    try:
                        if is_logged_in:
                            print(f"[DEBUG] Calling lists_mgr.create_list (async)")
                            new_list_id = await lists_mgr.create_list(name, color=selected_color['value'])
                        else:
                            print(f"[DEBUG] Calling lists_mgr.create_list_sync")
                            new_list_id = lists_mgr.create_list_sync(name, color=selected_color['value'])
                        print(f"[DEBUG] new_list_id={new_list_id}")
                    except Exception as e:
                        print(f"[DEBUG] Exception creating list: {e}")
                        import traceback
                        traceback.print_exc()
                        ui.notify(f"Error: {e}", type='negative')
                        return

                    if new_list_id:
                        # Add item to the new list
                        try:
                            if is_logged_in:
                                result = await lists_mgr.add_item(sys_id, new_list_id, note=new_list_note_input.value, fl_id=fl_id)
                            else:
                                result = lists_mgr.add_item_sync(sys_id, new_list_id, note=new_list_note_input.value, fl_id=fl_id)
                            print(f"[DEBUG] add_item result={result}")
                        except Exception as e:
                            print(f"[DEBUG] Exception adding item: {e}")
                            import traceback
                            traceback.print_exc()
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

    print(f"[DEBUG] About to open dialog")
    dialog.open()
    print(f"[DEBUG] Dialog opened")
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
    def show_dialog():
        show_add_to_list_dialog(
            sys_id=sys_id,
            shelfmark=shelfmark,
            lists_mgr=lists_mgr,
            note_default=note_default,
            fl_id=fl_id
        )

    props = 'flat round dense'
    if size == 'sm':
        props += ' size=sm'

    tooltip = tooltip_text or tr('Add to List')

    btn = ui.button(
        icon='star_border',
        on_click=show_dialog
    ).props(props).style('color: var(--accent-amber);').tooltip(tooltip)

    return btn
