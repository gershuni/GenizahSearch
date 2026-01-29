# -*- coding: utf-8 -*-
"""
Add to List Dialog Component for GenizahSearch web application.

Provides a reusable dialog for adding items to personal lists with:
- List selection dropdown
- Option to create a new list inline
- Optional note field (empty by default)
"""

from nicegui import ui
from web.translations import tr
from web.components.typography import h3
from typing import Optional, Callable


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

    # State for inline list creation
    creating_new_list = {'active': False}

    with ui.dialog() as dialog, ui.card().classes('p-6 min-w-96'):
        h3(tr('Add to List'), classes='text-xl font-bold mb-2')
        ui.label(f"{tr('Item')}: {shelfmark}").style('color: var(--text-secondary);')

        lists = lists_mgr.data.get('lists', {})
        list_options = {lid: lst['name'] for lid, lst in lists.items() if not lst.get('is_system')}

        # Container for the main form
        form_container = ui.column().classes('w-full mt-4 gap-3')

        # Container for new list creation form
        new_list_container = ui.column().classes('w-full mt-4 gap-3')
        new_list_container.set_visibility(False)

        with form_container:
            if not list_options:
                ui.label(tr('No lists yet. Create your first list!')).style('color: var(--text-muted);')
            else:
                # List selection with "Create new list" option
                list_options_with_new = {'__new__': f"+ {tr('Create new list')}", **list_options}
                selected_list = ui.select(
                    list_options_with_new,
                    label=tr('Select List'),
                    value=list(list_options.keys())[0] if list_options else '__new__'
                ).classes('w-full').props('outlined').style('color: var(--text-primary);')

                note_input = ui.input(label=tr('Note (optional)'), value=note_default).classes('w-full').props('outlined')

                def on_list_change():
                    if selected_list.value == '__new__':
                        form_container.set_visibility(False)
                        new_list_container.set_visibility(True)
                        creating_new_list['active'] = True

                selected_list.on('update:model-value', on_list_change)

            # Create New List button (shown when no lists exist)
            if not list_options:
                ui.button(
                    tr('Create new list'),
                    icon='add',
                    on_click=lambda: (form_container.set_visibility(False), new_list_container.set_visibility(True))
                ).classes('btn-primary')

        # New list creation form
        with new_list_container:
            ui.label(tr('Create New List')).classes('font-semibold').style('color: var(--text-primary);')

            new_list_name = ui.input(label=tr('List Name')).classes('w-full').props('outlined')

            # Color picker
            ui.label(tr('Color')).classes('text-sm mt-2').style('color: var(--text-secondary);')
            selected_color = {'value': '#4CAF50'}

            with ui.row().classes('gap-2 flex-wrap'):
                colors = ['#FFD700', '#4CAF50', '#2196F3', '#9C27B0', '#FF5722',
                          '#00BCD4', '#E91E63', '#795548', '#607D8B', '#F44336']
                for color in colors:
                    btn = ui.button(icon='circle').props('flat round dense').style(
                        f'color: {color}; font-size: 1.5rem;'
                    )
                    btn.on('click', lambda c=color: selected_color.update({'value': c}))

            # Note field for new list creation
            new_list_note_input = ui.input(label=tr('Note (optional)'), value=note_default).classes('w-full mt-3').props('outlined')

            with ui.row().classes('w-full justify-end gap-2 mt-4'):
                def back_to_list_selection():
                    new_list_container.set_visibility(False)
                    form_container.set_visibility(True)
                    creating_new_list['active'] = False

                # Only show back button if there are existing lists
                if list_options:
                    ui.button(tr('Back'), on_click=back_to_list_selection).props('flat')

                def create_and_add():
                    name = new_list_name.value.strip()
                    if not name:
                        ui.notify(tr('Please enter a list name'), type='warning')
                        return

                    # Create the new list
                    new_list_id = lists_mgr.create_list(name, color=selected_color['value'])
                    if new_list_id:
                        # Add item to the new list
                        if lists_mgr.add_item(sys_id, new_list_id, note=new_list_note_input.value, fl_id=fl_id):
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

            def do_add():
                if not list_options or creating_new_list['active']:
                    return

                if selected_list.value == '__new__':
                    # Switch to new list creation
                    form_container.set_visibility(False)
                    new_list_container.set_visibility(True)
                    creating_new_list['active'] = True
                    return

                if lists_mgr.add_item(sys_id, selected_list.value, note=note_input.value, fl_id=fl_id):
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
