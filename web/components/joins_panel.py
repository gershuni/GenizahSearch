# -*- coding: utf-8 -*-
"""
Fragment Joins Panel Component

Shows connected fragments for a manuscript and allows creating new joins.
Uses the simplified pairwise joins model with connected components.
"""

from nicegui import ui
from web.translations import tr, is_rtl
from web.auth_state import GlobalAuthState
from web.supabase_client import get_fragment_joins, create_fragment_join, get_client
from web.state import state
from typing import Optional, Callable, Dict, List
from urllib.parse import quote
import time

# Simple in-memory cache for joins data (key -> (timestamp, data))
_joins_cache: Dict[str, tuple] = {}
_CACHE_TTL = 30  # Cache for 30 seconds


def fetch_connected_fragments(shelfmark: str = None, document_id: str = None, force_refresh: bool = False) -> Dict:
    """
    Fetch all fragments connected to the given shelfmark or document_id.
    Prefers document_id if provided (more reliable).
    Results are cached for 30 seconds.

    Returns:
        Dict with fragments, joins, and counts
    """
    # Build cache key
    cache_key = f"doc:{document_id}" if document_id else f"shelf:{shelfmark}"

    # Check cache (unless force refresh)
    if not force_refresh and cache_key in _joins_cache:
        cached_time, cached_data = _joins_cache[cache_key]
        if time.time() - cached_time < _CACHE_TTL:
            return cached_data

    try:
        # Fetch joins from Supabase
        if document_id:
            joins = get_fragment_joins(fragment_sys_id=document_id)
        elif shelfmark:
            # Try to find by shelfmark in the joins
            joins = get_fragment_joins()
            joins = [j for j in joins if
                     j.get('fragment_a_shelfmark', '').upper() == shelfmark.upper() or
                     j.get('fragment_b_shelfmark', '').upper() == shelfmark.upper()]
        else:
            return {"fragments": [], "joins": [], "total_fragments": 1, "total_joins": 0}

        if not joins:
            return {"fragments": [], "joins": [], "total_fragments": 1, "total_joins": 0}

        # Build connected fragments set
        fragments_set = set()
        formatted_joins = []
        for j in joins:
            frag_a = j.get('fragment_a_shelfmark', '')
            frag_b = j.get('fragment_b_shelfmark', '')
            if frag_a:
                fragments_set.add(frag_a)
            if frag_b:
                fragments_set.add(frag_b)
            formatted_joins.append({
                'id': j.get('id'),
                'fragment_a': frag_a,
                'fragment_b': frag_b,
                'relationship_type': j.get('join_type'),
                'source': 'user',
                'notes': j.get('notes', '')
            })

        result = {
            "fragments": list(fragments_set),
            "joins": formatted_joins,
            "total_fragments": len(fragments_set),
            "total_joins": len(formatted_joins)
        }

        # Cache the result
        _joins_cache[cache_key] = (time.time(), result)
        return result
    except Exception as e:
        print(f"Error fetching connected fragments: {e}")
        return {"fragments": [], "joins": [], "total_fragments": 1, "total_joins": 0}


def invalidate_joins_cache(document_id: str = None, shelfmark: str = None, clear_all: bool = False):
    """
    Invalidate cached joins data after creating/deleting a join.

    When clear_all is True, clears the entire cache to ensure no stale data
    persists for transitively connected fragments.
    """
    global _joins_cache
    if clear_all:
        _joins_cache.clear()
        return
    if document_id:
        _joins_cache.pop(f"doc:{document_id}", None)
    if shelfmark:
        _joins_cache.pop(f"shelf:{shelfmark}", None)


def delete_join(join_id: int) -> bool:
    """
    Delete a join by ID (admin only).

    Returns:
        True if successful, False otherwise
    """
    try:
        client = get_client()
        client.table('fragment_joins').delete().eq('id', join_id).execute()
        return True
    except Exception as e:
        print(f"Error deleting join: {e}")
        return False


def create_joins_button(
    shelfmark: str,
    document_id: str = None,
    on_navigate: Optional[Callable[[str], None]] = None,
    size: str = "sm"
):
    """
    Create a joins button that shows count and opens the joins panel.

    Args:
        shelfmark: Current fragment shelfmark
        document_id: System ID of the document (optional)
        on_navigate: Callback when user clicks to navigate to another fragment
        size: Button size (sm, md, lg)

    Returns:
        The button element
    """
    # State for the button
    join_count = {'value': 0}
    button_ref = {'btn': None}

    def load_count():
        """Load the count of connected fragments."""
        data = fetch_connected_fragments(shelfmark=shelfmark, document_id=document_id)
        join_count['value'] = data.get('total_fragments', 1)
        # Update button style if we have joins - make it prominent
        if button_ref['btn'] and join_count['value'] > 1:
            button_ref['btn'].props('color=green').classes('bg-green-100 ring-2 ring-green-500', remove='text-green-700')

    def open_joins_panel():
        """Open the joins panel dialog."""
        create_joins_dialog(
            shelfmark=shelfmark,
            document_id=document_id,
            on_navigate=on_navigate
        )

    # Create the button
    btn = ui.button(
        icon='link',
        on_click=open_joins_panel
    ).props(f'flat dense size={size}').classes('text-green-700').tooltip(tr('Joined Fragments'))

    button_ref['btn'] = btn

    # Load count in background
    ui.timer(0.1, load_count, once=True)

    return btn


def create_joins_dialog(
    shelfmark: str,
    document_id: str = None,
    on_navigate: Optional[Callable[[str], None]] = None
):
    """
    Create a dialog showing connected fragments and allowing new joins.

    Args:
        shelfmark: Current fragment shelfmark
        document_id: System ID of the document
        on_navigate: Callback when navigating to another fragment
    """
    dialog = ui.dialog()

    with dialog, ui.card().classes('w-[500px] max-h-[80vh] p-0'):
        # Header
        with ui.row().classes('w-full items-center justify-between p-4 border-b').style(
            'background: linear-gradient(135deg, #15803d 0%, #166534 100%);'
        ):
            with ui.row().classes('items-center gap-2'):
                ui.icon('link').classes('text-white text-xl')
                ui.label(tr('Joined Fragments')).classes('text-lg font-bold text-white')
            ui.button(icon='close', on_click=dialog.close).props('flat round size=sm text-color=white')

        # Content area
        content = ui.column().classes('w-full p-4 gap-3')

        # Loading state - use dict to track if deleted
        spinner_state = {'spinner': ui.spinner(size='lg').classes('mx-auto my-8'), 'deleted': False}

        def load_content():
            """Load and display connected fragments."""
            data = fetch_connected_fragments(shelfmark=shelfmark, document_id=document_id)

            # Delete spinner only if it exists and hasn't been deleted
            if not spinner_state['deleted']:
                try:
                    spinner_state['spinner'].delete()
                except:
                    pass
                spinner_state['deleted'] = True

            content.clear()

            with content:
                fragments = data.get('fragments', [])
                joins = data.get('joins', [])
                total = data.get('total_fragments', 1)

                if total <= 1:
                    # No joins yet
                    with ui.column().classes('w-full items-center py-6'):
                        ui.icon('link_off', size='3rem').classes('text-gray-300')
                        ui.label(tr('No joins yet')).classes('text-gray-500 mt-2')
                        ui.label(f"{shelfmark}").classes('font-medium text-gray-700 mt-1')
                else:
                    # Show cluster info
                    ui.label(f"{tr('This fragment is part of a group of')} {total}:").classes(
                        'text-sm text-gray-600 mb-2'
                    )

                    # Build relationship map for display
                    # Track which fragments are DIRECTLY joined to current shelfmark
                    relationship_map = {}
                    direct_joins = {}  # fragment -> join_id for direct connections only

                    # Build map of shelfmark -> document_id from fragment_details
                    fragment_details = data.get('fragment_details', [])
                    shelfmark_to_docid = {}
                    for fd in fragment_details:
                        fd_shelf = fd.get('shelfmark', '')
                        fd_docid = fd.get('document_id')
                        if fd_shelf and fd_docid:
                            shelfmark_to_docid[fd_shelf.upper()] = fd_docid

                    for join in joins:
                        frag_a = join.get('fragment_a', '')
                        frag_b = join.get('fragment_b', '')
                        rel_type = join.get('relationship_type')
                        source = join.get('source', 'user')
                        join_id = join.get('id')

                        # Map each fragment to its relationship info
                        if frag_a not in relationship_map:
                            relationship_map[frag_a] = {'type': rel_type, 'source': source}
                        if frag_b not in relationship_map:
                            relationship_map[frag_b] = {'type': rel_type, 'source': source}

                        # Track DIRECT joins to current shelfmark only
                        if frag_a.upper() == shelfmark.upper():
                            direct_joins[frag_b] = join_id
                        elif frag_b.upper() == shelfmark.upper():
                            direct_joins[frag_a] = join_id

                    # Build fallback map from csv_bank for title lookup
                    shelf_to_sys = {}
                    if state.meta_mgr and hasattr(state.meta_mgr, 'csv_bank'):
                        for sys_id, meta in state.meta_mgr.csv_bank.items():
                            shelf = meta.get('shelfmark', '')
                            if shelf:
                                # Simple normalization for matching
                                import re
                                norm = re.sub(r'[^\w]', '', shelf).lower()
                                if norm.startswith('ms'):
                                    norm = norm[2:]
                                shelf_to_sys[norm] = sys_id

                    # List fragments
                    with ui.scroll_area().classes('w-full').style('max-height: 300px;'):
                        for frag in fragments:
                            is_current = frag.upper() == shelfmark.upper() or frag == shelfmark

                            # Get title for display
                            title_preview = ""
                            frag_doc_id = shelfmark_to_docid.get(frag.upper())

                            # Fallback: use csv_bank
                            if not frag_doc_id and state.meta_mgr:
                                import re
                                norm = re.sub(r'[^\w]', '', frag).lower()
                                if norm.startswith('ms'):
                                    norm = norm[2:]
                                frag_doc_id = shelf_to_sys.get(norm)

                            if frag_doc_id and state.meta_mgr:
                                try:
                                    _, title = state.meta_mgr.get_meta_for_id(frag_doc_id)
                                    if title:
                                        words = title.split()[:4]
                                        title_preview = ' '.join(words)
                                        if len(title.split()) > 4:
                                            title_preview += "..."
                                except:
                                    pass

                            # Create click handler - capture is_current properly
                            def make_click_handler(f, current):
                                if current:
                                    return lambda: None  # No-op for current
                                return lambda: handle_navigate(f)

                            with ui.card().classes(
                                'w-full p-3 mb-2 cursor-pointer hover:bg-gray-50'
                                + (' bg-green-50 border-green-300' if is_current else '')
                            ).on('click', make_click_handler(frag, is_current)):
                                with ui.row().classes('w-full items-center justify-between'):
                                    with ui.row().classes('items-center gap-2'):
                                        ui.icon('description').classes(
                                            'text-green-600' if is_current else 'text-gray-500'
                                        )
                                        with ui.column().classes('gap-0'):
                                            ui.label(frag).classes(
                                                'font-medium' + (' text-green-700' if is_current else '')
                                            )
                                            if title_preview:
                                                ui.label(title_preview).classes('text-xs text-gray-500')
                                        if is_current:
                                            ui.badge(tr('Current')).props('color=green outline')

                                    if not is_current:
                                        ui.icon('arrow_back' if is_rtl() else 'arrow_forward').classes('text-gray-400')

                                # Show relationship type if known
                                rel_info = relationship_map.get(frag, {})
                                rel_type = rel_info.get('type')
                                source = rel_info.get('source', 'user')

                                # Check if this is a DIRECT join to current fragment
                                direct_join_id = direct_joins.get(frag)

                                with ui.row().classes('items-center gap-2 mt-1'):
                                    if rel_type:
                                        rel_label = {
                                            'physical_join': tr('Physical join'),
                                            'same_composition': tr('Same composition')
                                        }.get(rel_type, rel_type)
                                        ui.label(rel_label).classes('text-xs text-gray-500')

                                    if source and source != 'user':
                                        ui.badge(source).props('color=blue outline dense').classes('text-xs')

                                    # Show "direct" badge for directly joined fragments
                                    if direct_join_id:
                                        ui.badge(tr('direct')).props('color=green outline dense').classes('text-xs')

                                    # Admin delete button - ONLY for direct joins
                                    if GlobalAuthState.is_admin() and direct_join_id and not is_current:
                                        def make_delete_handler(jid, main_dialog, frag_shelf, frag_doc_id):
                                            def do_delete():
                                                # Create confirmation dialog
                                                confirm_dlg = ui.dialog()
                                                with confirm_dlg, ui.card().classes('p-4'):
                                                    ui.label(tr('Delete this join?')).classes('text-lg font-bold')
                                                    ui.label(tr('This action cannot be undone.')).classes('text-gray-500')
                                                    with ui.row().classes('w-full justify-end gap-2 mt-4'):
                                                        ui.button(tr('Cancel'), on_click=confirm_dlg.close).props('flat')

                                                        def confirm_delete():
                                                            confirm_dlg.close()
                                                            if delete_join(jid):
                                                                ui.notify(tr('Join deleted'), type='positive')
                                                                # Clear entire cache to ensure no stale transitive connections
                                                                invalidate_joins_cache(clear_all=True)
                                                                # Close the main dialog - user can reopen to see updated list
                                                                main_dialog.close()
                                                            else:
                                                                ui.notify(tr('Failed to delete join'), type='negative')

                                                        ui.button(tr('Delete'), on_click=confirm_delete).props('color=red')
                                                confirm_dlg.open()
                                            return do_delete
                                        frag_doc_id = shelfmark_to_docid.get(frag.upper())
                                        # Use click.stop to prevent event bubbling to parent card
                                        ui.button(icon='delete').props(
                                            'flat dense size=xs color=red'
                                        ).on('click.stop', make_delete_handler(direct_join_id, dialog, frag, frag_doc_id)
                                        ).tooltip(tr('Delete join (admin)'))

                # Add new join button
                ui.separator().classes('my-2')

                if GlobalAuthState.is_logged_in():
                    ui.button(
                        tr('Join Another Fragment'),
                        icon='add_link',
                        on_click=lambda: show_add_join_form(shelfmark, document_id, on_refresh=load_content)
                    ).props('flat color=green').classes('w-full')
                else:
                    with ui.row().classes('w-full items-center justify-center gap-2 text-gray-500'):
                        ui.icon('info', size='sm')
                        ui.label(tr('Login to create joins')).classes('text-sm')

        def handle_navigate(target_shelfmark: str):
            """Handle navigation to another fragment."""
            dialog.close()
            if on_navigate:
                on_navigate(target_shelfmark)
            else:
                # Default: navigate to browse page with shelfmark
                # Use safe='' to encode all special chars including /
                ui.navigate.to(f'/browse?shelfmark={quote(target_shelfmark, safe="")}')

        # Load content
        ui.timer(0.1, load_content, once=True)

    dialog.open()
    return dialog


def show_add_join_form(
    current_shelfmark: str,
    document_id: str = None,
    on_refresh: Optional[Callable] = None
):
    """
    Show a dialog to add a new join using lists/recent picker.

    Args:
        current_shelfmark: The current fragment shelfmark
        document_id: System ID of current document
        on_refresh: Callback to refresh the joins list
    """
    dialog = ui.dialog()

    # State to track selected fragment
    selected_fragment = {'shelfmark': None, 'sys_id': None}

    with dialog, ui.card().classes('w-[500px] p-0'):
        # Header
        with ui.row().classes('w-full items-center justify-between p-4 border-b').style(
            'background: linear-gradient(135deg, #15803d 0%, #166534 100%);'
        ):
            with ui.column().classes('gap-0'):
                ui.label(tr('Join Fragment')).classes('text-lg font-bold text-white')
                ui.label(f"{current_shelfmark}").classes('text-sm text-white/80')
            ui.button(icon='close', on_click=dialog.close).props('flat round size=sm text-color=white')

        # Content area
        with ui.column().classes('w-full p-4 gap-4'):
            # Target fragment selection - show either picker or selected fragment
            selection_container = ui.column().classes('w-full')

            def show_picker():
                """Show the fragment picker."""
                selection_container.clear()
                with selection_container:
                    ui.label(tr('Select fragment to join:')).classes('text-sm font-medium text-gray-700 mb-2')

                    # Tabs for Recent Activity and Lists
                    with ui.tabs().classes('w-full') as tabs:
                        recent_tab = ui.tab('recent', label=tr('Recent Activity'))
                        lists_tab = ui.tab('lists', label=tr('My Lists'))

                    with ui.tab_panels(tabs, value='recent').classes('w-full').style('min-height: 200px;'):
                        with ui.tab_panel('recent'):
                            recent_container = ui.column().classes('w-full gap-1')

                            def load_recent():
                                recent_container.clear()
                                with recent_container:
                                    if state.lists_mgr:
                                        # Use get_items_in_list to get proper item dicts
                                        recent_items = state.lists_mgr.get_items_in_list_sync('recent')
                                        if recent_items:
                                            with ui.scroll_area().classes('w-full').style('max-height: 200px;'):
                                                for item in recent_items[:20]:
                                                    item_sys_id = item.get('sys_id', '')
                                                    item_shelfmark = item.get('shelfmark') or item.get('shelfmark_override')

                                                    # Look up shelfmark from metadata if not stored
                                                    if not item_shelfmark and item_sys_id and state.meta_mgr:
                                                        try:
                                                            item_shelfmark, _ = state.meta_mgr.get_meta_for_id(item_sys_id)
                                                        except:
                                                            pass

                                                    # Fallback to sys_id only if we can't find shelfmark
                                                    if not item_shelfmark:
                                                        item_shelfmark = item_sys_id

                                                    # Skip current shelfmark
                                                    if item_shelfmark and current_shelfmark and item_shelfmark.upper() == current_shelfmark.upper():
                                                        continue

                                                    # Skip items without valid shelfmark
                                                    if not item_shelfmark:
                                                        continue

                                                    def make_select(sm=item_shelfmark, sid=item_sys_id):
                                                        def select():
                                                            selected_fragment['shelfmark'] = sm
                                                            selected_fragment['sys_id'] = sid
                                                            show_selected()
                                                        return select

                                                    with ui.card().classes('w-full p-2 cursor-pointer hover:bg-green-50 border').on('click', make_select()):
                                                        with ui.row().classes('items-center gap-2'):
                                                            ui.icon('description').classes('text-gray-500')
                                                            ui.label(item_shelfmark).classes('font-medium text-sm')
                                        else:
                                            ui.label(tr('No recent activity')).classes('text-gray-500 text-sm')
                                    else:
                                        ui.label(tr('Recent items not available')).classes('text-gray-500 text-sm')

                            load_recent()

                        with ui.tab_panel('lists'):
                            lists_container = ui.column().classes('w-full gap-1')

                            def load_lists():
                                lists_container.clear()
                                with lists_container:
                                    if state.lists_mgr:
                                        lists = state.lists_mgr.data.get('lists', {})
                                        if lists:
                                            for list_id, list_data in lists.items():
                                                list_name = list_data.get('name', list_id)
                                                color = list_data.get('color', '#999')

                                                def make_show_list(lid=list_id, lname=list_name):
                                                    def show_list():
                                                        lists_container.clear()
                                                        items = state.lists_mgr.get_items_in_list_sync(lid)
                                                        with lists_container:
                                                            # Back button
                                                            ui.button(tr('Back'), icon='arrow_forward' if is_rtl() else 'arrow_back', on_click=load_lists).props('flat dense size=sm').classes('mb-2')
                                                            ui.label(lname).classes('font-bold mb-2')

                                                            if items:
                                                                with ui.scroll_area().classes('w-full').style('max-height: 180px;'):
                                                                    for item in items:
                                                                        item_sys_id = item.get('sys_id', '')
                                                                        item_shelfmark = item.get('shelfmark') or item.get('shelfmark_override')

                                                                        # Look up shelfmark from metadata if not stored
                                                                        if not item_shelfmark and item_sys_id and state.meta_mgr:
                                                                            try:
                                                                                item_shelfmark, _ = state.meta_mgr.get_meta_for_id(item_sys_id)
                                                                            except:
                                                                                pass

                                                                        # Fallback to sys_id only if we can't find shelfmark
                                                                        if not item_shelfmark:
                                                                            item_shelfmark = item_sys_id

                                                                        # Skip current shelfmark
                                                                        if item_shelfmark and current_shelfmark and item_shelfmark.upper() == current_shelfmark.upper():
                                                                            continue

                                                                        # Skip items without valid shelfmark
                                                                        if not item_shelfmark:
                                                                            continue

                                                                        def make_select(sm=item_shelfmark, sid=item_sys_id):
                                                                            def select():
                                                                                selected_fragment['shelfmark'] = sm
                                                                                selected_fragment['sys_id'] = sid
                                                                                show_selected()
                                                                            return select

                                                                        with ui.card().classes('w-full p-2 cursor-pointer hover:bg-green-50 border').on('click', make_select()):
                                                                            with ui.row().classes('items-center gap-2'):
                                                                                ui.icon('description').classes('text-gray-500')
                                                                                ui.label(item_shelfmark).classes('font-medium text-sm')
                                                            else:
                                                                ui.label(tr('No items in this list')).classes('text-gray-500 text-sm')
                                                    return show_list

                                                with ui.card().classes('w-full p-2 cursor-pointer hover:bg-gray-100 border').on('click', make_show_list()):
                                                    with ui.row().classes('items-center gap-2'):
                                                        ui.icon('circle').style(f'color: {color}; font-size: 0.8rem;')
                                                        ui.label(list_name).classes('font-medium text-sm')
                                        else:
                                            ui.label(tr('No lists found')).classes('text-gray-500 text-sm')
                                    else:
                                        ui.label(tr('Lists not available')).classes('text-gray-500 text-sm')

                            load_lists()

            def show_selected():
                """Show the selected fragment and relationship options."""
                selection_container.clear()
                with selection_container:
                    # Selected fragment display
                    ui.label(tr('Selected fragment:')).classes('text-sm font-medium text-gray-700')
                    with ui.card().classes('w-full p-3 bg-green-50 border-green-300 border mb-4'):
                        with ui.row().classes('items-center justify-between w-full'):
                            with ui.row().classes('items-center gap-2'):
                                ui.icon('link').classes('text-green-600')
                                ui.label(selected_fragment['shelfmark']).classes('font-bold text-green-700')
                            ui.button(icon='close', on_click=show_picker).props('flat round size=xs').tooltip(tr('Change selection'))

                    # Relationship type
                    ui.label(tr('Relationship (optional)')).classes('text-sm font-medium text-gray-700 mb-1')
                    relationship_options = {
                        '': tr('Not sure / just related'),
                        'physical_join': tr('Physical join'),
                        'same_composition': tr('Same composition')
                    }
                    relationship_select = ui.radio(
                        relationship_options,
                        value=''
                    ).props('dense')

                    # Notes (optional)
                    notes_input = ui.textarea(
                        label=tr('Notes (optional)'),
                        placeholder=tr('Add notes about this join...')
                    ).classes('w-full mt-2').props('outlined rows=2')

                    # Error message
                    error_label = ui.label('').classes('text-red-500 text-sm hidden mt-2')

                    # Action buttons
                    with ui.row().classes('w-full justify-end gap-2 mt-4'):
                        ui.button(tr('Cancel'), on_click=dialog.close).props('flat')

                        def submit_join():
                            """Submit the new join."""
                            target = selected_fragment['shelfmark']

                            if not target:
                                error_label.text = tr('Please select a fragment')
                                error_label.classes('visible', remove='hidden')
                                return

                            # Get user ID
                            user_id = GlobalAuthState.get_user_id()
                            if not user_id:
                                error_label.text = tr('User not found')
                                error_label.classes('visible', remove='hidden')
                                return

                            # Submit to Supabase
                            result = create_fragment_join(
                                user_id=user_id,
                                fragment_a_sys_id=document_id or '',
                                fragment_a_shelfmark=current_shelfmark,
                                fragment_b_sys_id=selected_fragment.get('sys_id', ''),
                                fragment_b_shelfmark=target,
                                join_type=relationship_select.value if relationship_select.value else 'uncertain',
                                notes=notes_input.value.strip() if notes_input.value else ''
                            )

                            if "error" in result:
                                error_msg = result.get("error", "Error creating join")
                                error_label.text = error_msg
                                error_label.classes('visible', remove='hidden')
                            else:
                                ui.notify(tr('Join created'), type='positive')
                                # Clear entire cache to ensure fresh data for all connected fragments
                                invalidate_joins_cache(clear_all=True)
                                dialog.close()
                                if on_refresh:
                                    on_refresh()

                        ui.button(
                            tr('Create Join'),
                            icon='add_link',
                            on_click=submit_join
                        ).props('color=green')

            # Initially show the picker
            show_picker()

    dialog.open()
    return dialog


def create_joins_indicator(
    shelfmark: str,
    document_id: str = None,
    on_navigate: Optional[Callable[[str], None]] = None
):
    """
    Create a small indicator showing join count with tooltip.
    Shown in metadata or header areas.

    Args:
        shelfmark: Current fragment shelfmark
        document_id: System ID
        on_navigate: Navigation callback

    Returns:
        Container element
    """
    container = ui.row().classes('items-center gap-1')

    def load_and_display():
        data = fetch_connected_fragments(shelfmark=shelfmark, document_id=document_id)
        total = data.get('total_fragments', 1)

        container.clear()
        with container:
            if total > 1:
                # Has joins - show clickable indicator
                with ui.button(on_click=lambda: create_joins_dialog(shelfmark, document_id, on_navigate)).props(
                    'flat dense round'
                ).classes('text-green-700'):
                    ui.icon('link', size='sm')
                    ui.badge(str(total)).props('color=green floating')

                ui.tooltip(f"{total} {tr('joined fragments')}")
            else:
                # No joins - show grayed out
                ui.icon('link_off', size='sm').classes('text-gray-400')
                ui.tooltip(tr('No joins'))

    ui.timer(0.1, load_and_display, once=True)
    return container
