# -*- coding: utf-8 -*-
"""
Project Tree Component for GenizahSearch web application.

Provides a collapsible tree view of projects and lists where:
- Projects are color-coded expandable groups
- Lists inherit colors from their parent project
- Standalone lists (not in any project) appear at the bottom
- Supports create/rename/delete operations

Usage:
    from web.components.project_tree import create_project_tree

    def on_list_selected(list_id):
        # Handle list selection
        pass

    create_project_tree(
        lists_mgr=state.lists_mgr,
        on_select=on_list_selected,
        selected_list_id='123'
    )
"""

from nicegui import ui
from web.translations import tr
from web.components.typography import h2, h3
from web.auth_state import GlobalAuthState
from typing import Optional, Callable, Dict, List
import logging

LOGGER = logging.getLogger(__name__)


def create_project_tree(
    lists_mgr,
    container,
    on_select: Optional[Callable[[str], None]] = None,
    selected_list_id: Optional[str] = None,
    on_refresh: Optional[Callable[[], None]] = None
):
    """
    Create a collapsible project tree in the given container.

    Args:
        lists_mgr: UserListsManager instance
        container: NiceGUI container element to render into
        on_select: Callback when a list is selected (receives list_id)
        selected_list_id: Currently selected list ID (for highlighting)
        on_refresh: Callback to refresh the entire UI after changes
    """
    container.clear()

    if not lists_mgr:
        with container:
            ui.label(tr('Lists manager not available')).classes('text-red-500')
        return

    # Get data
    data = lists_mgr.data
    projects = data.get('projects', {})
    lists_by_project = lists_mgr.get_lists_by_project()

    # State for expanded/collapsed projects
    expanded_projects = {}

    # State for management mode (show/hide action buttons)
    management_mode = {'enabled': False}

    with container:
        # Add CSS for management mode toggle
        ui.add_css('''
            .management-actions { display: none !important; }
            .management-mode .management-actions { display: flex !important; }
        ''')

        # Header with Create buttons (rendered first)
        with ui.row().classes('w-full justify-between items-center mb-4 pb-2 border-b'):
            h2(tr('My Lists'), classes='text-lg font-bold')
            with ui.row().classes('gap-1'):
                # Management mode toggle (manage_btn defined here, toggle_management_mode defined after tree_container)
                manage_btn = ui.button(
                    icon='settings',
                ).props('flat round dense size=sm').tooltip(tr('Manage lists'))

                ui.button(
                    icon='create_new_folder',
                    on_click=lambda: show_create_project_dialog(lists_mgr, on_refresh)
                ).props('flat round dense size=sm').tooltip(tr('Create project'))
                ui.button(
                    icon='add',
                    on_click=lambda: show_create_list_dialog(lists_mgr, projects, on_refresh)
                ).props('flat round dense size=sm').tooltip(tr('Create list'))

        # Main tree container that will have management-mode class toggled
        tree_container = ui.element('div').classes('w-full')

        def toggle_management_mode():
            management_mode['enabled'] = not management_mode['enabled']
            if management_mode['enabled']:
                tree_container.classes(add='management-mode')
                manage_btn.props('color=primary')
            else:
                tree_container.classes(remove='management-mode')
                manage_btn.props(remove='color=primary')

        # Connect the button click handler
        manage_btn.on('click', toggle_management_mode)

        with tree_container:
            # Render projects with their lists
            with ui.column().classes('w-full gap-2'):
                # Recently Viewed section (rendered first for quick access)
                standalone_lists = lists_by_project.get(None, [])
                system_lists = [l for l in standalone_lists if l.get('is_system')]
                recently_viewed = [l for l in system_lists if l.get('name') == 'Recently Viewed']

                if recently_viewed:
                    with ui.element('div').classes('mb-2'):
                        for list_data in recently_viewed:
                            _render_list_item(
                                list_data=list_data,
                                lists_mgr=lists_mgr,
                                selected_list_id=selected_list_id,
                                on_select=on_select,
                                on_refresh=on_refresh,
                                show_color=True
                            )

                # Projects section
                for project_id, project_data in projects.items():
                    project_lists = lists_by_project.get(project_id, [])
                    _render_project_group(
                        project_id=project_id,
                        project_data=project_data,
                        project_lists=project_lists,
                        lists_mgr=lists_mgr,
                        expanded_projects=expanded_projects,
                        selected_list_id=selected_list_id,
                        on_select=on_select,
                        on_refresh=on_refresh
                    )

                # Standalone lists section (lists not in any project)
                # Note: standalone_lists, system_lists, recently_viewed already computed above
                if standalone_lists:
                    # Filter out system lists for separate display
                    regular_lists = [l for l in standalone_lists if not l.get('is_system')]
                    # Exclude Recently Viewed from system lists (already rendered at top)
                    other_system_lists = [l for l in system_lists if l.get('name') != 'Recently Viewed']

                    if regular_lists:
                        with ui.element('div').classes('mt-4'):
                            ui.label(tr('Standalone Lists')).classes(
                                'text-xs font-semibold uppercase mb-2'
                            ).style('color: var(--text-muted);')

                            for list_data in regular_lists:
                                _render_list_item(
                                    list_data=list_data,
                                    lists_mgr=lists_mgr,
                                    selected_list_id=selected_list_id,
                                    on_select=on_select,
                                    on_refresh=on_refresh,
                                    show_color=True
                                )

                    # System lists at bottom (excluding Recently Viewed which is at top)
                    if other_system_lists:
                        with ui.element('div').classes('mt-4'):
                            ui.label(tr('System')).classes(
                                'text-xs font-semibold uppercase mb-2'
                            ).style('color: var(--text-muted);')

                            for list_data in other_system_lists:
                                _render_list_item(
                                    list_data=list_data,
                                    lists_mgr=lists_mgr,
                                    selected_list_id=selected_list_id,
                                    on_select=on_select,
                                    on_refresh=on_refresh,
                                    show_color=True
                                )

                # Empty state
                if not projects and not standalone_lists:
                    with ui.column().classes('w-full items-center py-8'):
                        ui.icon('folder_open', size='3rem').style('color: var(--text-muted);')
                        ui.label(tr('No lists yet')).classes('mt-2').style('color: var(--text-muted);')
                        ui.label(tr('Create a project or list to get started')).classes(
                            'text-sm'
                        ).style('color: var(--text-muted);')


def _render_project_group(
    project_id: str,
    project_data: Dict,
    project_lists: List[Dict],
    lists_mgr,
    expanded_projects: Dict,
    selected_list_id: Optional[str],
    on_select: Optional[Callable],
    on_refresh: Optional[Callable]
):
    """Render a project with its lists as a collapsible group."""
    project_name = project_data.get('name', 'Unnamed Project')
    project_color = project_data.get('color', '#4CAF50')
    is_expanded = expanded_projects.get(project_id, True)  # Default expanded

    # Project header
    with ui.card().classes('w-full p-0'):
        # Header row (clickable to expand/collapse)
        with ui.row().classes(
            'w-full items-center justify-between p-3 cursor-pointer hover:bg-gray-50'
        ).style(f'border-left: 4px solid {project_color};') as header:
            with ui.row().classes('items-center gap-2 flex-grow'):
                # Expand/collapse icon
                expand_icon = ui.icon(
                    'expand_more' if is_expanded else 'chevron_right'
                ).classes('text-gray-500')

                # Project color dot
                ui.element('div').classes('rounded-full').style(
                    f'width: 12px; height: 12px; background-color: {project_color};'
                )

                # Project name
                ui.label(project_name).classes('font-semibold')

                # List count
                ui.label(f'({len(project_lists)})').classes('text-xs').style(
                    'color: var(--text-muted);'
                )

            # Project action buttons (hidden by default, shown in management mode)
            def on_add_list(pid=project_id, pdata=project_data, mgr=lists_mgr, refresh=on_refresh):
                show_create_list_dialog(mgr, {pid: pdata}, refresh, default_project=pid)

            def on_rename_proj(pid=project_id, pname=project_name, mgr=lists_mgr, refresh=on_refresh):
                show_rename_project_dialog(mgr, pid, pname, refresh)

            def on_delete_proj(pid=project_id, pname=project_name, pcount=len(project_lists), mgr=lists_mgr, refresh=on_refresh):
                show_delete_project_dialog(mgr, pid, pname, pcount, refresh)

            with ui.row().classes('gap-0 management-actions'):
                ui.button(icon='add', on_click=on_add_list).props(
                    'flat round dense size=sm'
                ).classes('opacity-50 hover:opacity-100').tooltip(tr('Add list'))
                ui.button(icon='edit', on_click=on_rename_proj).props(
                    'flat round dense size=sm'
                ).classes('opacity-50 hover:opacity-100').tooltip(tr('Rename'))
                ui.button(icon='delete', on_click=on_delete_proj).props(
                    'flat round dense size=sm color=negative'
                ).classes('opacity-50 hover:opacity-100').tooltip(tr('Delete'))

        def toggle_expand(pid=project_id, icon=expand_icon):
            expanded_projects[pid] = not expanded_projects.get(pid, True)
            icon.props(f'name={"expand_more" if expanded_projects[pid] else "chevron_right"}')
            lists_container.set_visibility(expanded_projects[pid])

        header.on('click', toggle_expand)

        # Lists container (collapsible)
        with ui.column().classes('w-full pl-6 pb-2 gap-1') as lists_container:
            lists_container.set_visibility(is_expanded)

            if project_lists:
                for list_data in project_lists:
                    _render_list_item(
                        list_data=list_data,
                        lists_mgr=lists_mgr,
                        selected_list_id=selected_list_id,
                        on_select=on_select,
                        on_refresh=on_refresh,
                        show_color=False,  # Color shown on project header
                        parent_color=project_color
                    )
            else:
                ui.label(tr('No lists in this project')).classes(
                    'text-xs italic py-2'
                ).style('color: var(--text-muted);')


def _render_list_item(
    list_data: Dict,
    lists_mgr,
    selected_list_id: Optional[str],
    on_select: Optional[Callable],
    on_refresh: Optional[Callable],
    show_color: bool = True,
    parent_color: Optional[str] = None
):
    """Render a single list item."""
    list_id = list_data.get('id')
    list_name = list_data.get('name', 'Unnamed')
    is_system = list_data.get('is_system', False)
    is_selected = selected_list_id == list_id

    # Get display color
    if show_color:
        color = lists_mgr.get_list_display_color(list_id) if hasattr(lists_mgr, 'get_list_display_color') else list_data.get('color', '#FFD700')
    else:
        color = parent_color or '#999'

    # Get item count
    try:
        count = lists_mgr._get_list_item_count(list_id)
    except Exception:
        count = 0  # Count query failed; use zero as fallback

    # Item container
    item_classes = 'w-full p-2 rounded cursor-pointer transition-all'
    if is_selected:
        item_classes += ' bg-green-100'

    with ui.row().classes(item_classes).style(
        'hover: background: var(--bg-tertiary);'
    ) as item_row:
        # Color indicator (only if show_color is True)
        if show_color:
            ui.element('div').classes('rounded-full flex-shrink-0').style(
                f'width: 10px; height: 10px; background-color: {color}; margin-top: 5px;'
            )

        # List name
        with ui.column().classes('flex-grow gap-0'):
            name_style = 'font-weight: 600;' if is_selected else ''
            ui.label(list_name).classes('text-sm').style(name_style)
            if is_system:
                ui.label(tr('System')).classes('text-xs').style('color: var(--text-muted);')

        # Item count badge
        ui.label(str(count)).classes(
            'text-xs px-2 py-0.5 rounded-full'
        ).style('background: var(--bg-tertiary); color: var(--text-secondary);')

        # Actions (only for non-system lists)
        if not is_system:
            # Define callbacks with captured variables
            def on_rename(lid=list_id, lname=list_name, mgr=lists_mgr, refresh=on_refresh):
                show_rename_list_dialog(mgr, lid, lname, refresh)

            def on_add_to_project(lid=list_id, mgr=lists_mgr, refresh=on_refresh):
                show_move_to_project_dialog(mgr, lid, refresh)

            def on_delete(lid=list_id, lname=list_name, mgr=lists_mgr, refresh=on_refresh):
                show_delete_list_dialog(mgr, lid, lname, refresh)

            # Action buttons (hidden by default, shown in management mode)
            with ui.row().classes('gap-0 management-actions'):
                ui.button(icon='edit', on_click=on_rename).props(
                    'flat round dense size=sm'
                ).classes('opacity-50 hover:opacity-100').tooltip(tr('Rename'))
                ui.button(icon='folder', on_click=on_add_to_project).props(
                    'flat round dense size=sm'
                ).classes('opacity-50 hover:opacity-100').tooltip(tr('Add to project...'))
                ui.button(icon='delete', on_click=on_delete).props(
                    'flat round dense size=sm color=negative'
                ).classes('opacity-50 hover:opacity-100').tooltip(tr('Delete'))

    def handle_click(lid=list_id):
        if on_select:
            on_select(lid)

    item_row.on('click', handle_click)


# === Dialogs ===

def show_create_project_dialog(lists_mgr, on_refresh: Optional[Callable] = None):
    """Show dialog to create a new project."""
    with ui.dialog() as dialog, ui.card().classes('p-6 min-w-[400px]'):
        h3(tr('Create Project'), classes='text-xl font-bold mb-4')

        ui.label(tr('Projects group related lists together. Color is assigned automatically.')).classes(
            'text-sm mb-4'
        ).style('color: var(--text-secondary);')

        project_name = ui.input(label=tr('Project Name')).classes('w-full mb-4').props('autofocus')

        # Preview next color
        next_color = lists_mgr.get_next_project_color() if hasattr(lists_mgr, 'get_next_project_color') else '#4CAF50'
        with ui.row().classes('items-center gap-2 mb-4'):
            ui.label(tr('Color')).classes('text-sm').style('color: var(--text-secondary);')
            ui.element('div').classes('rounded-full').style(
                f'width: 20px; height: 20px; background-color: {next_color};'
            )

        async def create_project():
            name = project_name.value.strip()
            if not name:
                ui.notify(tr('Please enter a project name'), type='warning')
                return

            try:
                if GlobalAuthState.is_logged_in():
                    project_id = await lists_mgr.create_project(name)
                else:
                    project_id = lists_mgr.create_project_sync(name)

                if project_id:
                    ui.notify(f"{tr('Project created')}: {name}", type='positive')
                    dialog.close()
                    if on_refresh:
                        on_refresh()
                else:
                    ui.notify(tr('Failed to create project'), type='negative')
            except Exception as e:
                LOGGER.error(f"Error creating project: {e}")
                ui.notify(f"Error: {e}", type='negative')

        with ui.row().classes('w-full justify-end gap-2 mt-4'):
            ui.button(tr('Cancel'), on_click=dialog.close).props('flat')
            ui.button(tr('Create'), on_click=create_project).classes('bg-primary text-white')

    dialog.open()


def show_create_list_dialog(
    lists_mgr,
    projects: Dict,
    on_refresh: Optional[Callable] = None,
    default_project: Optional[str] = None
):
    """Show dialog to create a new list, optionally in a project."""
    with ui.dialog() as dialog, ui.card().classes('p-6 min-w-[400px]'):
        h3(tr('Create List'), classes='text-xl font-bold mb-4')

        list_name = ui.input(label=tr('List Name')).classes('w-full mb-4').props('autofocus')

        # Project selector (if projects exist)
        selected_project = {'value': default_project}

        if projects:
            project_options = {None: tr('(No project - standalone)')}
            for pid, pdata in projects.items():
                project_options[pid] = pdata.get('name', 'Unnamed')

            ui.label(tr('Add to project (optional)')).classes('text-sm mb-2').style('color: var(--text-secondary);')
            project_select = ui.select(
                project_options,
                value=default_project,
                label=tr('Project')
            ).classes('w-full mb-4').props('outlined')

            def on_project_change():
                selected_project['value'] = project_select.value

            project_select.on('update:model-value', on_project_change)

            # Show color hint
            ui.label(tr('Color will be inherited from the project, or gold for standalone lists.')).classes(
                'text-xs mb-4'
            ).style('color: var(--text-muted);')

        async def create_list():
            name = list_name.value.strip()
            if not name:
                ui.notify(tr('Please enter a list name'), type='warning')
                return

            try:
                project_id = selected_project['value']
                if GlobalAuthState.is_logged_in():
                    list_id = await lists_mgr.create_list(name, project_id=project_id)
                else:
                    list_id = lists_mgr.create_list_sync(name, project_id=project_id)

                if list_id:
                    ui.notify(f"{tr('List created')}: {name}", type='positive')
                    dialog.close()
                    if on_refresh:
                        on_refresh()
                else:
                    ui.notify(tr('Failed to create list'), type='negative')
            except Exception as e:
                LOGGER.error(f"Error creating list: {e}")
                ui.notify(f"Error: {e}", type='negative')

        with ui.row().classes('w-full justify-end gap-2 mt-4'):
            ui.button(tr('Cancel'), on_click=dialog.close).props('flat')
            ui.button(tr('Create'), on_click=create_list).classes('bg-primary text-white')

    dialog.open()


def show_rename_project_dialog(
    lists_mgr,
    project_id: str,
    current_name: str,
    on_refresh: Optional[Callable] = None
):
    """Show dialog to rename a project."""
    with ui.dialog() as dialog, ui.card().classes('p-6 min-w-[400px]'):
        h3(tr('Rename Project'), classes='text-xl font-bold mb-4')

        project_name = ui.input(label=tr('Project Name'), value=current_name).classes('w-full mb-4').props('autofocus')

        async def rename_project():
            name = project_name.value.strip()
            if not name:
                ui.notify(tr('Please enter a project name'), type='warning')
                return

            if name == current_name:
                dialog.close()
                return

            try:
                if GlobalAuthState.is_logged_in():
                    success = await lists_mgr.update_project(project_id, name)
                else:
                    success = lists_mgr.update_project(project_id, name)

                if success:
                    ui.notify(f"{tr('Project renamed to')}: {name}", type='positive')
                    dialog.close()
                    if on_refresh:
                        on_refresh()
                else:
                    ui.notify(tr('Failed to rename project'), type='negative')
            except Exception as e:
                LOGGER.error(f"Error renaming project: {e}")
                ui.notify(f"Error: {e}", type='negative')

        with ui.row().classes('w-full justify-end gap-2 mt-4'):
            ui.button(tr('Cancel'), on_click=dialog.close).props('flat')
            ui.button(tr('Save'), on_click=rename_project).classes('bg-primary text-white')

    dialog.open()


def show_rename_list_dialog(
    lists_mgr,
    list_id: str,
    current_name: str,
    on_refresh: Optional[Callable] = None
):
    """Show dialog to rename a list."""
    with ui.dialog() as dialog, ui.card().classes('p-6 min-w-[400px]'):
        h3(tr('Rename List'), classes='text-xl font-bold mb-4')

        list_name = ui.input(label=tr('List Name'), value=current_name).classes('w-full mb-4').props('autofocus')

        async def rename_list():
            name = list_name.value.strip()
            if not name:
                ui.notify(tr('Please enter a list name'), type='warning')
                return

            if name == current_name:
                dialog.close()
                return

            try:
                if GlobalAuthState.is_logged_in():
                    success = await lists_mgr.update_list(list_id, name=name)
                else:
                    success = lists_mgr.update_list(list_id, name=name)

                if success:
                    ui.notify(f"{tr('List renamed to')}: {name}", type='positive')
                    dialog.close()
                    if on_refresh:
                        on_refresh()
                else:
                    ui.notify(tr('Failed to rename list'), type='negative')
            except Exception as e:
                LOGGER.error(f"Error renaming list: {e}")
                ui.notify(f"Error: {e}", type='negative')

        with ui.row().classes('w-full justify-end gap-2 mt-4'):
            ui.button(tr('Cancel'), on_click=dialog.close).props('flat')
            ui.button(tr('Save'), on_click=rename_list).classes('bg-primary text-white')

    dialog.open()


def show_delete_project_dialog(
    lists_mgr,
    project_id: str,
    project_name: str,
    list_count: int,
    on_refresh: Optional[Callable] = None
):
    """Show dialog to delete a project."""
    with ui.dialog() as dialog, ui.card().classes('p-6 min-w-[400px]'):
        h3(tr('Delete Project?'), classes='text-xl font-bold mb-2')

        ui.label(f"{tr('Are you sure you want to delete')}: {project_name}?").classes('mb-4').style(
            'color: var(--text-secondary);'
        )

        delete_lists_option = {'value': False}

        if list_count > 0:
            ui.label(f"{tr('This project has')} {list_count} {tr('lists')}.").classes('mb-2')

            with ui.row().classes('items-center gap-2'):
                checkbox = ui.checkbox(tr('Also delete all lists in this project'))
                checkbox.on('update:model-value', lambda e: delete_lists_option.update({'value': e.args}))

            ui.label(tr('If unchecked, lists will become standalone.')).classes('text-xs mb-4').style(
                'color: var(--text-muted);'
            )

        async def delete_project():
            try:
                delete_lists = delete_lists_option['value']
                if GlobalAuthState.is_logged_in():
                    success = await lists_mgr.delete_project(project_id, delete_lists=delete_lists)
                else:
                    success = lists_mgr.delete_project(project_id, delete_lists=delete_lists)

                if success:
                    ui.notify(f"{tr('Project deleted')}: {project_name}", type='info')
                    dialog.close()
                    if on_refresh:
                        on_refresh()
                else:
                    ui.notify(tr('Failed to delete project'), type='negative')
            except Exception as e:
                LOGGER.error(f"Error deleting project: {e}")
                ui.notify(f"Error: {e}", type='negative')

        with ui.row().classes('w-full justify-end gap-2 mt-4'):
            ui.button(tr('Cancel'), on_click=dialog.close).props('flat')
            ui.button(tr('Delete'), on_click=delete_project).classes('bg-red-500 text-white')

    dialog.open()


def show_delete_list_dialog(
    lists_mgr,
    list_id: str,
    list_name: str,
    on_refresh: Optional[Callable] = None
):
    """Show dialog to delete a list."""
    with ui.dialog() as dialog, ui.card().classes('p-6'):
        h3(tr('Delete List?'), classes='text-xl font-bold mb-2')

        ui.label(f"{tr('Are you sure you want to delete')}: {list_name}?").classes('mb-4').style(
            'color: var(--text-secondary);'
        )
        ui.label(tr('All items in this list will be removed.')).classes('text-sm text-red-500 mb-4')

        async def delete_list():
            try:
                if GlobalAuthState.is_logged_in():
                    success = await lists_mgr.delete_list(list_id)
                else:
                    success = lists_mgr.delete_list(list_id)

                if success:
                    ui.notify(f"{tr('List deleted')}: {list_name}", type='info')
                    dialog.close()
                    if on_refresh:
                        on_refresh()
                else:
                    ui.notify(tr('Failed to delete list'), type='negative')
            except Exception as e:
                LOGGER.error(f"Error deleting list: {e}")
                ui.notify(f"Error: {e}", type='negative')

        with ui.row().classes('w-full justify-end gap-2 mt-4'):
            ui.button(tr('Cancel'), on_click=dialog.close).props('flat')
            ui.button(tr('Delete'), on_click=delete_list).classes('bg-red-500 text-white')

    dialog.open()


def show_move_to_project_dialog(lists_mgr, list_id: str, on_refresh: Optional[Callable] = None):
    """Show dialog to move a list to a project."""
    if lists_mgr is None:
        ui.notify("Error: lists_mgr is None", type='negative')
        return

    try:
        projects = lists_mgr.get_projects() if hasattr(lists_mgr, 'get_projects') else []
    except Exception as e:
        LOGGER.error(f"Error getting projects: {e}")
        ui.notify(f"Error loading projects: {e}", type='negative')
        return
    with ui.dialog() as dialog, ui.card().classes('p-6 min-w-[400px]'):
        h3(tr('Add to project...'), classes='text-xl font-bold mb-4')

        # Build options for select
        options = {'': tr('No project')}
        for proj in projects:
            options[str(proj.get('id'))] = proj.get('name', 'Unnamed')

        project_select = ui.select(
            options=options,
            value='',
            label=tr('Select project')
        ).classes('w-full mb-4').props('outlined')

        ui.separator().classes('my-2')

        # Add new project option
        ui.label(tr('Or create new project:')).classes('text-sm').style('color: var(--text-secondary);')
        new_project_name = ui.input(label=tr('New project name')).classes('w-full').props('outlined dense')

        async def do_move():
            LOGGER.debug(f"do_move called for list {list_id}")
            try:
                new_name = new_project_name.value.strip() if new_project_name.value else None
                selected_val = project_select.value
                LOGGER.debug(f"new_name={new_name}, selected_val={selected_val}")

                # If new project name given, create it first
                if new_name:
                    LOGGER.debug(f"Creating new project: {new_name}")
                    if GlobalAuthState.is_logged_in():
                        target_project_id = await lists_mgr.create_project(new_name)
                    else:
                        target_project_id = lists_mgr.create_project_sync(new_name) if hasattr(lists_mgr, 'create_project_sync') else None

                    if not target_project_id:
                        ui.notify(tr('Failed to create project'), type='negative')
                        return
                    LOGGER.debug(f"Created project with id: {target_project_id}")
                else:
                    # Use selected project (empty string means no project)
                    target_project_id = selected_val if selected_val else None

                LOGGER.debug(f"Moving list {list_id} to project {target_project_id}")

                # Move the list - always use async version when logged in
                if not hasattr(lists_mgr, 'update_list_project'):
                    ui.notify(tr('Feature not available'), type='warning')
                    return

                success = await lists_mgr.update_list_project(list_id, target_project_id)
                LOGGER.debug(f"update_list_project returned: {success}")

                if success:
                    if target_project_id:
                        ui.notify(tr('List moved to project'), type='positive')
                    else:
                        ui.notify(tr('List removed from project'), type='positive')
                    dialog.close()
                    if on_refresh:
                        on_refresh()
                else:
                    ui.notify(tr('Failed to update list'), type='negative')
            except Exception as e:
                LOGGER.error(f"Error moving list to project: {e}", exc_info=True)
                ui.notify(f"Error: {e}", type='negative')

        with ui.row().classes('w-full justify-end gap-2 mt-4'):
            ui.button(tr('Cancel'), on_click=dialog.close).props('flat')
            ui.button(tr('Move'), on_click=do_move).classes('bg-primary text-white')

    dialog.open()


