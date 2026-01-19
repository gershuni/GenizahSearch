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
"""

from nicegui import ui
from web.state import state
from web.translations import tr, is_rtl
from typing import Optional
import time


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

    # --- Create New List Dialog ---
    def show_create_list_dialog():
        """Show dialog to create a new list."""
        with ui.dialog() as dialog, ui.card().classes('p-6 min-w-[400px]'):
            ui.label(tr('Create New List')).classes('text-xl font-bold mb-4')

            list_name = ui.input(label=tr('List Name')).classes('w-full mb-4')

            # Color picker
            ui.label(tr('Color')).classes('text-sm font-semibold mb-2')
            selected_color = {'value': '#FFD700'}

            with ui.row().classes('gap-2 mb-4 flex-wrap'):
                colors = ['#FFD700', '#4CAF50', '#2196F3', '#9C27B0', '#FF5722',
                          '#00BCD4', '#E91E63', '#795548', '#607D8B', '#F44336']
                for color in colors:
                    btn = ui.button(icon='circle').props('flat round').style(
                        f'color: {color}; font-size: 2rem;'
                    )
                    btn.on('click', lambda c=color: selected_color.update({'value': c}))

            def create_list():
                name = list_name.value.strip()
                if not name:
                    ui.notify(tr('Please enter a list name'), type='warning')
                    return

                if state.lists_mgr:
                    list_id = state.lists_mgr.create_list(name, color=selected_color['value'])
                    ui.notify(f"{tr('List created')}: {name}", type='positive')
                    dialog.close()
                    refresh_ui()
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
            ui.label(tr('Delete List?')).classes('text-xl font-bold mb-2')
            ui.label(f"{tr('Are you sure you want to delete')}: {list_name}?").classes('text-gray-600 mb-4')
            ui.label(tr('All items in this list will be removed.')).classes('text-sm text-red-500 mb-4')

            def delete_list():
                if state.lists_mgr:
                    state.lists_mgr.delete_list(list_id)
                    ui.notify(f"{tr('List deleted')}: {list_name}", type='info')
                    dialog.close()
                    page_state.selected_list_id = None
                    refresh_ui()

            with ui.row().classes('w-full justify-end gap-2'):
                ui.button(tr('Cancel'), on_click=dialog.close).props('flat')
                ui.button(tr('Delete'), on_click=delete_list).classes('bg-red-500 text-white')

        dialog.open()

    # --- Edit Item Dialog ---
    def show_edit_item_dialog(item_id: str, item_data: dict):
        """Show dialog to edit item notes and tags."""
        with ui.dialog() as dialog, ui.card().classes('p-6 min-w-[500px]'):
            ui.label(tr('Edit Item')).classes('text-xl font-bold mb-2')

            shelfmark = item_data.get('shelfmark', 'Unknown')
            ui.label(f"{tr('Item')}: {shelfmark}").classes('text-sm text-gray-600 mb-4')

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
        """Render the left sidebar with list of lists."""
        lists_sidebar_container.clear()

        with lists_sidebar_container:
            # Header with Create button
            with ui.row().classes('w-full justify-between items-center mb-4 pb-2 border-b'):
                ui.label(tr('My Lists')).classes('text-lg font-bold')
                ui.button(
                    icon='add',
                    on_click=show_create_list_dialog
                ).props('flat round dense').tooltip(tr('Create new list'))

            if not state.lists_mgr:
                ui.label(tr('Lists manager not available')).classes('text-red-500')
                return

            lists = state.lists_mgr.data.get('lists', {})
            if not lists:
                ui.label(tr('No lists yet. Create your first list!')).classes('text-gray-400 text-center mt-4')
                return

            # Render each list
            with ui.column().classes('w-full gap-1'):
                for list_id, list_data in lists.items():
                    is_selected = page_state.selected_list_id == list_id
                    is_system = list_data.get('is_system', False)

                    card_class = 'w-full p-3 cursor-pointer transition-all'
                    if is_selected:
                        card_class += ' bg-green-100 border-l-4 border-green-600'
                    else:
                        card_class += ' hover:bg-gray-50'

                    with ui.card().classes(card_class).on('click', lambda lid=list_id: select_list(lid)):
                        with ui.row().classes('w-full items-center justify-between gap-2'):
                            # Color indicator + Name
                            with ui.row().classes('items-center gap-2 flex-grow'):
                                ui.icon('circle').style(f'color: {list_data.get("color", "#999")}; font-size: 1.2rem;')
                                ui.label(list_data.get('name', 'Unnamed')).classes('font-semibold')

                            # Item count
                            if list_id == 'recent':
                                count = len(state.lists_mgr.data.get('recent_items', []))
                            else:
                                count = state.lists_mgr._get_list_item_count(list_id)
                            ui.label(str(count)).classes('text-xs bg-gray-200 px-2 py-1 rounded-full')

                            # Delete button (only for non-system lists)
                            if not is_system:
                                def make_delete_handler(lid, lname):
                                    def handler():
                                        show_delete_list_dialog(lid, lname)
                                    return handler
                                ui.button(
                                    icon='delete',
                                    on_click=make_delete_handler(list_id, list_data.get('name'))
                                ).props('flat round dense size=sm stop-propagation').classes('text-red-400').tooltip(tr('Delete list'))

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
                    ui.icon('playlist_add', size='6rem').classes('text-gray-300')
                    ui.label(tr('Select a list to view its contents')).classes('text-gray-400 text-xl mt-4')
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
            with ui.row().classes('w-full justify-between items-start mb-6 pb-4 border-b-2').style(
                f'border-color: {list_data.get("color", "#999")};'
            ):
                with ui.column().classes('gap-1'):
                    with ui.row().classes('items-center gap-3'):
                        ui.icon('circle').style(f'color: {list_data.get("color", "#999")}; font-size: 2rem;')
                        ui.label(list_data.get('name', 'Unnamed')).classes('text-3xl font-bold')

                    is_system = list_data.get('is_system', False)
                    if is_system:
                        ui.label(tr('System List')).classes('text-xs text-gray-500')

                # Export button
                with ui.row().classes('gap-2'):
                    ui.button(
                        tr('Export'),
                        icon='download',
                        on_click=lambda: export_list(list_id)
                    ).props('flat').classes('text-primary')

            # Get items
            items_list = state.lists_mgr.get_items_in_list(list_id)
            items_data = [(item.get('item_id'), item) for item in items_list]

            if not items_data:
                with ui.column().classes('w-full items-center justify-center py-16'):
                    ui.icon('inbox', size='4rem').classes('text-gray-300')
                    ui.label(tr('This list is empty')).classes('text-gray-400 text-lg mt-2')
                    if not list_data.get('is_system'):
                        ui.label(tr('Add items from search results')).classes('text-gray-400 text-sm')
                return

            # Items Grid/List
            ui.label(f"{len(items_data)} {tr('items')}").classes('text-sm text-gray-500 mb-4')

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

                    with ui.card().classes('w-full p-4 hover:shadow-lg transition-shadow'):
                        with ui.row().classes('w-full justify-between items-start'):
                            # Main content
                            with ui.column().classes('flex-grow gap-2'):
                                # Shelfmark
                                ui.label(shelfmark).classes('text-lg font-bold text-primary')

                                # Title
                                if title:
                                    ui.label(title).classes('text-sm text-gray-600').style('direction: rtl;')

                                # Note
                                if note:
                                    with ui.card().classes('bg-yellow-50 p-2 mt-2'):
                                        ui.label(note).classes('text-xs text-gray-700')

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
                                        print(f"Error fetching snippet: {e}")

                                    if text_snippet:
                                        # Show snippet or full text
                                        max_chars = 200
                                        with ui.element('div').classes(
                                            'bg-gray-50 p-3 rounded-lg border border-gray-200'
                                        ).style('direction: rtl; text-align: right;'):
                                            if expanded_state['value'] or len(text_snippet) <= max_chars:
                                                ui.label(text_snippet).classes(
                                                    'text-sm text-gray-700 whitespace-pre-wrap'
                                                ).style('line-height: 1.8;')
                                            else:
                                                ui.label(text_snippet[:max_chars] + '...').classes(
                                                    'text-sm text-gray-700'
                                                ).style('line-height: 1.8;')

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
                                            'text-xs text-gray-400 italic'
                                        )

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
                                ).props('flat dense size=sm').classes('text-gray-500')

    def remove_item_from_list(item_id: str, list_id: str):
        """Remove an item from the current list."""
        if state.lists_mgr:
            if state.lists_mgr.remove_item_from_list(item_id, list_id):
                ui.notify(tr('Item removed from list'), type='info')
                refresh_ui()

    def export_list(list_id: str):
        """Export list to Excel."""
        if state.lists_mgr:
            try:
                list_data = state.lists_mgr.data.get('lists', {}).get(list_id)
                if not list_data:
                    ui.notify(tr('List not found'), type='warning')
                    return

                items = list_data.get('items', [])
                if not items:
                    ui.notify(tr('This list is empty'), type='warning')
                    return

                # Trigger download
                ui.download(f'/api/export/list/{list_id}/excel')
            except Exception as e:
                ui.notify(f"{tr('Export failed')}: {str(e)}", type='negative')

    # --- Main Layout ---
    with ui.column().classes('w-full h-[calc(100vh-120px)]'):
        # Page Title
        with ui.row().classes('w-full items-center justify-between mb-4'):
            ui.label(tr('Personal Lists')).classes('text-3xl font-bold text-green-800')
            ui.button(
                tr('Create List'),
                icon='add',
                on_click=show_create_list_dialog
            ).classes('bg-primary text-white')

        # Description
        ui.label(tr('Organize and save manuscripts for easy access')).classes('text-gray-600 mb-4')

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
