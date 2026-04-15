# -*- coding: utf-8 -*-
"""
Fragment Joins Panel Component

Shows connected fragments for a manuscript and allows creating new joins.
Uses the simplified pairwise joins model with connected components.
"""

import asyncio
import logging
from nicegui import ui
from web.translations import tr, is_rtl
from web.feature_flags import WEB_PUZZLE_ENABLED
from web.auth_state import GlobalAuthState
from web.supabase_client import get_fragment_joins, create_fragment_join, get_client
from web.state import state
from typing import Optional, Callable, Dict, List
from urllib.parse import quote
import time
import threading

logger = logging.getLogger(__name__)

# Simple in-memory cache for joins data (key -> (timestamp, data))
_joins_cache: Dict[str, tuple] = {}
_joins_cache_lock = threading.Lock()
# Cache TTL (configurable via environment variable, default 30 seconds)
import os
_CACHE_TTL = int(os.environ.get('JOINS_CACHE_TTL', '30'))


def fetch_connected_fragments(shelfmark: str = None, document_id: str = None, pgpid: int = None, force_refresh: bool = False) -> Dict:
    """
    Fetch all fragments connected to the given shelfmark or document_id.
    Merges user-created pairwise joins (fragment_joins table) with PGP
    multi-fragment document joins (document_fragments table).

    Prefers document_id if provided (more reliable).
    Results are cached for 30 seconds.

    Args:
        shelfmark: Current fragment shelfmark
        document_id: System ID of the document (sys_id)
        pgpid: PGP document ID (avoids redundant Supabase lookup)
        force_refresh: Force cache bypass

    Returns:
        Dict with fragments, joins, total_fragments, total_joins, fragment_details
    """
    # Build cache key (include pgpid for proper cache separation)
    cache_key = f"doc:{document_id}:pgp:{pgpid}" if document_id else f"shelf:{shelfmark}:pgp:{pgpid}"

    # Check cache (unless force refresh)
    if not force_refresh:
        with _joins_cache_lock:
            if cache_key in _joins_cache:
                cached_time, cached_data = _joins_cache[cache_key]
                if time.time() - cached_time < _CACHE_TTL:
                    return cached_data

    try:
        # Fetch user joins from Supabase
        if document_id:
            joins = get_fragment_joins(fragment_sys_id=document_id)
        elif shelfmark:
            # Try to find by shelfmark in the joins
            joins = get_fragment_joins()
            joins = [j for j in joins if
                     j.get('fragment_a_shelfmark', '').upper() == shelfmark.upper() or
                     j.get('fragment_b_shelfmark', '').upper() == shelfmark.upper()]
        else:
            return {"fragments": [], "joins": [], "total_fragments": 1, "total_joins": 0, "fragment_details": []}

        # Build connected fragments set from user joins
        fragments_set = set()
        # Track upper-cased shelfmarks to avoid duplicates when merging PGP joins
        fragments_upper = set()
        formatted_joins = []
        fragment_details = []

        # Track which shelfmarks already have fragment_details entries
        details_upper = set()

        for j in joins:
            frag_a = j.get('fragment_a_shelfmark', '')
            frag_b = j.get('fragment_b_shelfmark', '')
            frag_a_sid = j.get('fragment_a_sys_id', '')
            frag_b_sid = j.get('fragment_b_sys_id', '')
            if frag_a:
                fragments_set.add(frag_a)
                fragments_upper.add(frag_a.upper())
                if frag_a_sid and frag_a.upper() not in details_upper:
                    fragment_details.append({'shelfmark': frag_a, 'document_id': frag_a_sid})
                    details_upper.add(frag_a.upper())
            if frag_b:
                fragments_set.add(frag_b)
                fragments_upper.add(frag_b.upper())
                if frag_b_sid and frag_b.upper() not in details_upper:
                    fragment_details.append({'shelfmark': frag_b, 'document_id': frag_b_sid})
                    details_upper.add(frag_b.upper())
            formatted_joins.append({
                'id': j.get('id'),
                'fragment_a': frag_a,
                'fragment_b': frag_b,
                'relationship_type': j.get('join_type'),
                'sources': ['user'],
                'notes': j.get('notes', ''),
                'created_by_username': j.get('created_by_username', ''),
                'created_at': j.get('created_at', ''),
            })

        # --- Merge PGP document joins ---
        from web.document_service import get_document_for_fragment, get_fragments_for_document

        resolved_pgpid = pgpid
        if not resolved_pgpid and document_id:
            # Resolve pgpid from document_id (sys_id)
            pgp_doc = get_document_for_fragment(document_id)
            if pgp_doc:
                resolved_pgpid = pgp_doc.get('pgpid')

        if resolved_pgpid:
            pgp_fragments = get_fragments_for_document(resolved_pgpid)

            # Only include if there are MORE THAN 1 unique sys_ids
            # (filters out single-fragment PGP documents - no false "Related Fragments")
            unique_sys_ids = set()
            for pf in pgp_fragments:
                sid = pf.get('sys_id')
                if sid:
                    unique_sys_ids.add(sid)

            if len(unique_sys_ids) > 1:
                current_shelfmark_upper = shelfmark.upper() if shelfmark else ''

                for pf in pgp_fragments:
                    pf_shelfmark = pf.get('shelfmark', '')
                    pf_sys_id = pf.get('sys_id', '')

                    if not pf_shelfmark:
                        continue

                    # Populate fragment_details for shelfmark_to_docid lookup in dialog
                    # (skip if already added from user joins)
                    if pf_shelfmark.upper() not in details_upper:
                        fragment_details.append({
                            'shelfmark': pf_shelfmark,
                            'document_id': pf_sys_id
                        })
                        details_upper.add(pf_shelfmark.upper())

                    # Skip current shelfmark (already in the set implicitly)
                    if pf_shelfmark.upper() == current_shelfmark_upper:
                        # Still add to fragments_set if not already there
                        if pf_shelfmark.upper() not in fragments_upper:
                            fragments_set.add(pf_shelfmark)
                            fragments_upper.add(pf_shelfmark.upper())
                        continue

                    # Deduplicate: skip if already present from user joins
                    if pf_shelfmark.upper() in fragments_upper:
                        continue

                    fragments_set.add(pf_shelfmark)
                    fragments_upper.add(pf_shelfmark.upper())

                    # Create formatted join entry for PGP fragment
                    formatted_joins.append({
                        'id': None,  # Not user-created, prevents admin delete button
                        'fragment_a': shelfmark or '',
                        'fragment_b': pf_shelfmark,
                        'relationship_type': 'same_composition',
                        'sources': ['PGP'],
                        'notes': f'PGP Document #{resolved_pgpid}'
                    })

        # --- Merge FJMS scholarly joins ---
        try:
            from web.fjms_service import get_fjms_service
            fjms_svc = get_fjms_service(thread_safe=True)
            if fjms_svc.is_available() and document_id is not None:
                fjms_joins = fjms_svc.get_join_group(document_id)
                for member in fjms_joins:
                    alma_id = member.get('alma_id', '')
                    if not alma_id or alma_id == document_id:
                        continue

                    # Resolve shelfmark from metadata
                    resolved_shelfmark = None
                    if state.meta_mgr:
                        try:
                            resolved_shelfmark, _ = state.meta_mgr.get_meta_for_id(alma_id)
                        except Exception:
                            pass  # Shelfmark lookup failed; use fallback identifier

                    if not resolved_shelfmark or resolved_shelfmark == 'Unknown':
                        continue

                    # Check if this fragment already exists from user/PGP joins
                    if resolved_shelfmark.upper() in fragments_upper:
                        # Merge FJMS source into existing entry instead of dropping
                        for existing_join in formatted_joins:
                            if existing_join.get('fragment_b', '').upper() == resolved_shelfmark.upper():
                                existing_sources = existing_join.get('sources', [existing_join.get('source', 'user')])
                                if 'FJMS' not in existing_sources:
                                    existing_sources.append('FJMS')
                                existing_join['sources'] = existing_sources
                                # Merge scholar name if existing entry lacks one
                                fjms_scholar = ', '.join(member.get('scholar_names', [])) if member.get('scholar_names') else ''
                                if fjms_scholar and not existing_join.get('scholar_name'):
                                    existing_join['scholar_name'] = fjms_scholar
                                # Merge relationship type if existing entry lacks one
                                fjms_rel_type = ', '.join(member.get('join_types', [])) if member.get('join_types') else ''
                                if fjms_rel_type and not existing_join.get('relationship_type'):
                                    existing_join['relationship_type'] = fjms_rel_type
                                break
                        continue

                    fragments_set.add(resolved_shelfmark)
                    fragments_upper.add(resolved_shelfmark.upper())

                    if resolved_shelfmark.upper() not in details_upper:
                        fragment_details.append({
                            'shelfmark': resolved_shelfmark,
                            'document_id': alma_id
                        })
                        details_upper.add(resolved_shelfmark.upper())

                    formatted_joins.append({
                        'id': None,  # Not user-created
                        'fragment_a': shelfmark or '',
                        'fragment_b': resolved_shelfmark,
                        'relationship_type': ', '.join(member.get('join_types', [])) if member.get('join_types') else '',
                        'sources': ['FJMS'],
                        'notes': '',
                        'scholar_name': ', '.join(member.get('scholar_names', [])) if member.get('scholar_names') else '',
                        'join_group_id': member.get('join_group_ids', []),
                    })
        except Exception as e:
            logger.error("FJMS joins merge error: %s", e)

        # Ensure current shelfmark is in fragments_set
        if shelfmark and shelfmark.upper() not in fragments_upper:
            fragments_set.add(shelfmark)

        # If no joins at all (no user joins, no PGP joins, no FJMS joins), return empty
        if not formatted_joins:
            result = {"fragments": [], "joins": [], "total_fragments": 1, "total_joins": 0, "fragment_details": fragment_details}
            with _joins_cache_lock:
                _joins_cache[cache_key] = (time.time(), result)
            return result

        result = {
            "fragments": list(fragments_set),
            "joins": formatted_joins,
            "total_fragments": len(fragments_set),
            "total_joins": len(formatted_joins),
            "fragment_details": fragment_details
        }

        # Cache the result
        with _joins_cache_lock:
            _joins_cache[cache_key] = (time.time(), result)
        return result
    except Exception as e:
        logger.error("Error fetching connected fragments: %s", e)
        return {"fragments": [], "joins": [], "total_fragments": 1, "total_joins": 0, "fragment_details": []}


def invalidate_joins_cache(document_id: str = None, shelfmark: str = None, clear_all: bool = False):
    """
    Invalidate cached joins data after creating/deleting a join.

    When clear_all is True, clears the entire cache to ensure no stale data
    persists for transitively connected fragments.
    """
    with _joins_cache_lock:
        if clear_all:
            _joins_cache.clear()
            return
        # Clear any cache entries matching the document_id or shelfmark
        # (cache keys now include pgpid suffix, so we match by prefix)
        keys_to_remove = []
        if document_id:
            prefix = f"doc:{document_id}:"
            keys_to_remove.extend(k for k in _joins_cache if k.startswith(prefix))
        if shelfmark:
            prefix = f"shelf:{shelfmark}:"
            keys_to_remove.extend(k for k in _joins_cache if k.startswith(prefix))
        for k in keys_to_remove:
            _joins_cache.pop(k, None)


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
        logger.error("Error deleting join: %s", e)
        return False


def create_joins_button(
    shelfmark: str,
    document_id: str = None,
    pgpid: int = None,
    on_navigate: Optional[Callable[[str], None]] = None,
    on_view_all: Optional[Callable[[List[Dict], Optional[int]], None]] = None,
    size: str = "sm"
):
    """
    Create a joins button that shows count and opens the joins panel.

    Args:
        shelfmark: Current fragment shelfmark
        document_id: System ID of the document (optional)
        pgpid: PGP document ID (avoids redundant Supabase lookup)
        on_navigate: Callback when user clicks to navigate to another fragment
        on_view_all: Callback(fragment_details, pgpid) to enter joined view mode
        size: Button size (sm, md, lg)

    Returns:
        The button element
    """
    # State for the button
    join_count = {'value': 0}
    button_ref = {'btn': None}

    def load_count():
        """Load the count of connected fragments."""
        data = fetch_connected_fragments(shelfmark=shelfmark, document_id=document_id, pgpid=pgpid)
        join_count['value'] = data.get('total_fragments', 1)
        # Update button style if we have joins - make it prominent
        if button_ref['btn'] and join_count['value'] > 1:
            button_ref['btn'].props('color=green').classes('bg-green-100 ring-2 ring-green-500', remove='text-green-700')

    def open_joins_panel():
        """Open the joins panel dialog."""
        create_joins_dialog(
            shelfmark=shelfmark,
            document_id=document_id,
            pgpid=pgpid,
            on_navigate=on_navigate,
            on_view_all=on_view_all
        )

    # Create the button
    btn = ui.button(
        icon='link',
        on_click=open_joins_panel
    ).props(f'flat dense size={size}').classes('text-green-700').tooltip(tr('Joined Fragments'))

    button_ref['btn'] = btn

    # Load count in background
    def _safe_load_count():
        try:
            load_count()
        except RuntimeError:
            pass  # Parent element was deleted (NiceGUI timer lifecycle)

    # Use call_later instead of ui.timer to avoid parent_slot RuntimeError
    # when content_container.clear() destroys the timer's parent element
    asyncio.get_event_loop().call_later(0.1, _safe_load_count)

    return btn


def create_joins_dialog(
    shelfmark: str,
    document_id: str = None,
    pgpid: int = None,
    on_navigate: Optional[Callable[[str], None]] = None,
    on_view_all: Optional[Callable[[List[Dict], Optional[int]], None]] = None
):
    """
    Create a dialog showing connected fragments and allowing new joins.

    Args:
        shelfmark: Current fragment shelfmark
        document_id: System ID of the document
        pgpid: PGP document ID (avoids redundant Supabase lookup)
        on_navigate: Callback when navigating to another fragment
        on_view_all: Callback(fragment_details, pgpid) to enter joined view mode
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
            data = fetch_connected_fragments(shelfmark=shelfmark, document_id=document_id, pgpid=pgpid)

            # Delete spinner only if it exists and hasn't been deleted
            if not spinner_state['deleted']:
                try:
                    spinner_state['spinner'].delete()
                except Exception:
                    pass  # Shelfmark lookup failed; use fallback identifier
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
                        sources = join.get('sources', [join.get('source', 'user')])
                        join_id = join.get('id')

                        scholar_name = join.get('scholar_name', '') or join.get('created_by_username', '')
                        notes = join.get('notes', '')

                        # Map each fragment to its relationship info, aggregating sources
                        for frag_key in [frag_a, frag_b]:
                            if frag_key not in relationship_map:
                                relationship_map[frag_key] = {'type': rel_type, 'sources': list(sources), 'scholar_name': scholar_name, 'notes': notes}
                            else:
                                existing = relationship_map[frag_key]
                                existing_sources = existing.get('sources', [existing.get('source', 'user')])
                                for s in sources:
                                    if s not in existing_sources:
                                        existing_sources.append(s)
                                existing['sources'] = existing_sources
                                if scholar_name and not existing.get('scholar_name'):
                                    existing['scholar_name'] = scholar_name
                                if notes and not existing.get('notes'):
                                    existing['notes'] = notes

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
                                except Exception:
                                    pass  # Join operation failed; continue with available data

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

                                # Check if this is a DIRECT join to current fragment
                                direct_join_id = direct_joins.get(frag)

                                with ui.row().classes('items-center gap-2 mt-1'):
                                    if rel_type:
                                        rel_label = {
                                            'physical_join': tr('Physical join'),
                                            'physical': tr('Physical join'),
                                            'same_composition': tr('Same composition'),
                                            'content': tr('Same composition'),
                                            'uncertain': tr('Unknown'),
                                        }.get(rel_type, rel_type)
                                        ui.label(rel_label).classes('text-xs text-gray-500')

                                    sources = rel_info.get('sources', [rel_info.get('source', 'user')])
                                    for src in sources:
                                        if src == 'FJMS':
                                            ui.badge('FJMS').props('color=purple outline dense').classes('text-xs')
                                        elif src and src != 'user':
                                            ui.badge(src).props('color=blue outline dense').classes('text-xs')

                                    scholar = rel_info.get('scholar_name', '')
                                    if scholar:
                                        ui.label(f"({scholar})").classes('text-xs text-gray-400 italic')

                                    notes = rel_info.get('notes', '')
                                    if notes:
                                        ui.label(notes).classes('text-xs text-gray-400')

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

                # View All Fragments button
                if on_view_all and total > 1:
                    ui.separator().classes('my-2')

                    def handle_view_all():
                        dialog.close()
                        # Build fragment info from fragment_details + fallback
                        frag_info = list(fragment_details) if fragment_details else []
                        # If no fragment_details (only user joins), build from fragments list
                        if not frag_info:
                            for f in fragments:
                                f_docid = shelfmark_to_docid.get(f.upper())
                                if f_docid:
                                    frag_info.append({'shelfmark': f, 'document_id': f_docid})
                                else:
                                    # Try csv_bank fallback
                                    if state.meta_mgr and hasattr(state.meta_mgr, 'csv_bank'):
                                        import re as _re
                                        norm = _re.sub(r'[^\w]', '', f).lower()
                                        if norm.startswith('ms'):
                                            norm = norm[2:]
                                        sid = shelf_to_sys.get(norm)
                                        if sid:
                                            frag_info.append({'shelfmark': f, 'document_id': sid})
                        on_view_all(frag_info, pgpid)

                    ui.button(
                        tr('View All Fragments'), icon='auto_stories',
                        on_click=handle_view_all
                    ).props('outline color=green').classes('w-full')

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

                # ── Community Puzzle Joins section ──
                if WEB_PUZZLE_ENABLED and document_id:
                    try:
                        client = get_client()
                        pjf_resp = client.table('published_join_fragments').select(
                            'join_id, shelfmark'
                        ).eq('sys_id', document_id).execute()
                        if pjf_resp.data:
                            join_ids = list(set(r['join_id'] for r in pjf_resp.data))
                            pj_resp = client.table('published_joins').select(
                                'id, user_id, title, shelfmarks, thumbnail_path, created_at'
                            ).in_('id', join_ids).eq('is_published', True).execute()
                            pj_rows = pj_resp.data or []
                            if pj_rows:
                                # Resolve author names
                                pj_user_ids = list(set(r['user_id'] for r in pj_rows))
                                pj_profiles = {}
                                try:
                                    pj_profiles_resp = client.table('profiles').select(
                                        'id, full_name'
                                    ).in_('id', pj_user_ids).execute()
                                    pj_profiles = {
                                        p['id']: p.get('full_name', 'Anonymous')
                                        for p in (pj_profiles_resp.data or [])
                                    }
                                except Exception:
                                    pass  # Thumbnail load failed; full image will replace it

                                ui.separator().classes('my-3')
                                with ui.row().classes('items-center gap-2'):
                                    ui.icon('extension', size='sm').classes('text-cyan-600')
                                    ui.label(tr('Community Puzzle Joins')).classes(
                                        'text-subtitle2 font-bold'
                                    ).style('color: var(--text-primary);')

                                bucket = client.storage.from_('puzzle-images')
                                for pj in pj_rows:
                                    pj_thumb_url = ''
                                    if pj.get('thumbnail_path'):
                                        pj_thumb_url = bucket.get_public_url(pj['thumbnail_path'])
                                    pj_title = pj.get('title', '') or 'Untitled'
                                    pj_author = pj_profiles.get(pj['user_id'], 'Anonymous')
                                    pj_shelfmarks = pj.get('shelfmarks', [])

                                    def make_pj_click(pj_id=pj['id']):
                                        def go_to_puzzle():
                                            dialog.close()
                                            ui.navigate.to(f'/puzzle?doc={pj_id}')
                                        return go_to_puzzle

                                    with ui.card().classes(
                                        'w-full p-2 cursor-pointer hover:bg-cyan-50'
                                    ).on('click', make_pj_click()):
                                        with ui.row().classes('items-center gap-2 w-full'):
                                            if pj_thumb_url:
                                                ui.image(pj_thumb_url).style(
                                                    'width: 48px; height: 48px; object-fit: contain; '
                                                    'border-radius: 4px;'
                                                )
                                            else:
                                                ui.icon('extension', size='lg').classes('text-cyan-400')
                                            with ui.column().classes('gap-0 flex-1'):
                                                ui.label(pj_title).classes('text-body2 font-medium')
                                                if pj_shelfmarks:
                                                    ui.label(' + '.join(pj_shelfmarks[:3])).classes(
                                                        'text-caption font-mono'
                                                    ).style('color: var(--text-secondary);')
                                                ui.label(pj_author).classes('text-caption').style(
                                                    'color: var(--text-tertiary);'
                                                )
                    except Exception as e:
                        logger.error("Community Puzzle Joins error: %s", e)

        async def handle_navigate(target_shelfmark: str):
            """Handle navigation to another fragment."""
            dialog.close()
            if on_navigate:
                result = on_navigate(target_shelfmark)
                if hasattr(result, '__await__'):
                    await result
            else:
                # Default: navigate to browse page with shelfmark
                # Use safe='' to encode all special chars including /
                ui.navigate.to(f'/browse?shelfmark={quote(target_shelfmark, safe="")}')

        # Load content
        async def _deferred_load():
            await asyncio.sleep(0.1)
            try:
                await load_content()
            except Exception:
                pass  # Shelfmark lookup failed; use fallback identifier
        asyncio.ensure_future(_deferred_load())

    dialog.open()
    return dialog


def show_add_join_form(
    current_shelfmark: str,
    document_id: str = None,
    on_refresh: Optional[Callable] = None,
    prefill_shelfmark: str = None,
    prefill_sys_id: str = None,
):
    """
    Show a dialog to add a new join using lists/recent picker.

    Args:
        current_shelfmark: The current fragment shelfmark
        document_id: System ID of current document
        on_refresh: Callback to refresh the joins list
        prefill_shelfmark: Optional shelfmark to pre-select as fragment B
        prefill_sys_id: Optional sys_id for pre-selected fragment B
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

                    # Tabs for Recent Activity, Lists, and Visual Suggestions
                    with ui.tabs().classes('w-full') as tabs:
                        recent_tab = ui.tab('recent', label=tr('Recent Activity'))
                        lists_tab = ui.tab('lists', label=tr('My Lists'))
                        vs_tab = ui.tab('vs', label=tr('Visual Similarity'))

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
                                                        except Exception:
                                                            pass  # Shelfmark lookup failed; use fallback identifier

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
                                                                            except Exception:
                                                                                pass  # Metadata lookup failed; fall back to sys_id below

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

                        with ui.tab_panel('vs'):
                            vs_container = ui.column().classes('w-full gap-1')

                            def load_vs_suggestions():
                                vs_container.clear()
                                with vs_container:
                                    try:
                                        from shared.visual_similarity_service import get_vs_service
                                        svc = get_vs_service(thread_safe=True)
                                        if not svc.is_available() or not svc.has_suggestions(document_id or ''):
                                            ui.label(tr('No visual similarity suggestions')).classes('text-sm').style('color: var(--text-muted);')
                                            return
                                        suggestions = svc.get_suggestions(document_id or '', 50)
                                        if not suggestions:
                                            ui.label(tr('No visual similarity suggestions')).classes('text-sm').style('color: var(--text-muted);')
                                            return
                                        # Enrich with shelfmarks
                                        csv_bank = state.meta_mgr.csv_bank if state.meta_mgr else None
                                        for s in suggestions:
                                            meta = csv_bank.get(s['alma_id']) if csv_bank else None
                                            s['shelfmark'] = meta.get('shelfmark', s['alma_id']) if meta else s['alma_id']

                                        with ui.scroll_area().classes('w-full').style('max-height: 200px;'):
                                            for s in suggestions:
                                                s_shelf = s['shelfmark']
                                                s_id = s['alma_id']
                                                s_rank = s.get('rank', 0)
                                                if s_shelf.upper() == current_shelfmark.upper():
                                                    continue

                                                def make_vs_select(sm=s_shelf, sid=s_id):
                                                    def select():
                                                        selected_fragment['shelfmark'] = sm
                                                        selected_fragment['sys_id'] = sid
                                                        show_selected()
                                                    return select

                                                with ui.card().classes('w-full p-2 cursor-pointer hover:bg-orange-50 border').on('click', make_vs_select()):
                                                    with ui.row().classes('items-center gap-2'):
                                                        ui.badge(f'#{s_rank}').props('color=deep-orange-1 text-color=deep-orange-9').classes('text-xs')
                                                        ui.label(s_shelf).classes('font-medium text-sm')
                                    except Exception as e:
                                        ui.label(tr('Could not load visual similarity data. Try again later.')).classes('text-sm').style('color: var(--text-muted);')

                            load_vs_suggestions()

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

            # If prefill provided, go straight to selected state
            if prefill_shelfmark:
                selected_fragment['shelfmark'] = prefill_shelfmark
                selected_fragment['sys_id'] = prefill_sys_id or ''
                show_selected()
            else:
                show_picker()

    dialog.open()
    return dialog


def create_joins_indicator(
    shelfmark: str,
    document_id: str = None,
    pgpid: int = None,
    on_navigate: Optional[Callable[[str], None]] = None
):
    """
    Create a small indicator showing join count with tooltip.
    Shown in metadata or header areas.

    Args:
        shelfmark: Current fragment shelfmark
        document_id: System ID
        pgpid: PGP document ID (avoids redundant Supabase lookup)
        on_navigate: Navigation callback

    Returns:
        Container element
    """
    container = ui.row().classes('items-center gap-1')

    def load_and_display():
        data = fetch_connected_fragments(shelfmark=shelfmark, document_id=document_id, pgpid=pgpid)
        total = data.get('total_fragments', 1)

        container.clear()
        with container:
            if total > 1:
                # Has joins - show clickable indicator
                with ui.button(on_click=lambda: create_joins_dialog(shelfmark, document_id, pgpid, on_navigate)).props(
                    'flat dense round'
                ).classes('text-green-700'):
                    ui.icon('link', size='sm')
                    ui.badge(str(total)).props('color=green floating')

                ui.tooltip(f"{total} {tr('joined fragments')}")
            else:
                # No joins - show grayed out
                ui.icon('link_off', size='sm').classes('text-gray-400')
                ui.tooltip(tr('No joins'))

    async def _deferred_load_display():
        await asyncio.sleep(0.1)
        try:
            await load_and_display()
        except Exception:
            pass  # Tooltip metadata optional; item still valid
    asyncio.ensure_future(_deferred_load_display())
    return container
