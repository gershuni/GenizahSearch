# -*- coding: utf-8 -*-
"""
Discoveries Center - Community Discoveries and Questions

Shows:
- Statistics (words corrected, documents edited, discoveries, open questions)
- Activity feed combining discoveries, questions, and corrections
- Create new discovery/question form
- NO leaderboard (researchers prefer anonymity over competition)
"""

import asyncio
from nicegui import ui, app, run
from web.translations import tr
from web.auth_state import GlobalAuthState, api_call
from web.state import state
from typing import Optional
from datetime import datetime


def resolve_shelfmark(doc_id: str, shelfmark: str = None) -> tuple:
    """
    Resolve a document ID to its shelfmark and title.

    Args:
        doc_id: The document system ID
        shelfmark: Optional existing shelfmark (used if available)

    Returns:
        Tuple of (display_shelfmark, title)
    """
    # If we already have a shelfmark, use it
    if shelfmark:
        return shelfmark, ''

    # Try to look up from metadata
    if doc_id and state.meta_mgr:
        try:
            sh, title = state.meta_mgr.get_meta_for_id(doc_id)
            return sh or doc_id, title or ''
        except:
            pass

    return doc_id or '', ''


async def create_discoveries_page():
    """Create the Discoveries Center page."""

    with ui.column().classes('w-full max-w-5xl mx-auto gap-6 fade-in'):

        # === Page Header ===
        with ui.row().classes('w-full items-center justify-between'):
            with ui.column().classes('gap-1'):
                ui.label(tr('Discoveries Center')).classes('text-3xl font-bold').style('color: var(--text-primary);')
                ui.label(tr('Community discoveries, questions, and contributions')).style('color: var(--text-secondary);')

        # === Statistics Cards ===
        stats_row = ui.row().classes('w-full gap-4')
        await load_stats(stats_row)

        # === Filter Bar ===
        with ui.row().classes('w-full items-center justify-between p-3 rounded').style('background: var(--surface-secondary);'):
            with ui.row().classes('items-center gap-4'):
                # Type filter
                type_filter = ui.select(
                    {
                        'all': tr('All Items'),
                        'discovery': tr('Discoveries'),
                        'question': tr('Questions'),
                        'correction': tr('Corrections'),
                        'comment': tr('Comments'),
                        'join': tr('Joins'),
                    },
                    value='all',
                    label=tr('Type')
                ).props('outlined dense').classes('min-w-32')

                # Period filter
                period_filter = ui.select(
                    {
                        'all': tr('All Time'),
                        'day': tr('Today'),
                        'week': tr('This Week'),
                        'month': tr('This Month'),
                    },
                    value='all',
                    label=tr('Period')
                ).props('outlined dense').classes('min-w-32')

            # Create new button
            async def open_create_dialog():
                if not GlobalAuthState.is_logged_in():
                    ui.notify(tr('Please login to create a discovery'), type='warning')
                    return

                async def on_discovery_created():
                    await refresh_feed()

                dialog = create_new_discovery_dialog(on_success=on_discovery_created)
                dialog.open()

            ui.button(tr('Share Discovery'), icon='add', on_click=open_create_dialog).props('color=primary')

        # === Activity Feed ===
        feed_container = ui.column().classes('w-full gap-4')

        async def refresh_feed():
            """Reload the activity feed."""
            feed_container.clear()
            with feed_container:
                await load_feed(
                    type_filter.value if type_filter.value != 'all' else None,
                    period_filter.value if period_filter.value != 'all' else None,
                    on_refresh=refresh_feed
                )

        # Bind filter changes - use async handler for proper await
        async def on_filter_change():
            await refresh_feed()

        type_filter.on('update:model-value', on_filter_change)
        period_filter.on('update:model-value', on_filter_change)

        # Initial load
        with feed_container:
            await load_feed(None, None, on_refresh=refresh_feed)


async def load_stats(container):
    """Load and display statistics cards."""
    result = await api_call("GET", "/discoveries/stats/summary")

    with container:
        if "error" in result:
            ui.label(tr('Could not load statistics')).style('color: var(--text-tertiary);')
            return

        stats = result

        # Cards layout
        stat_cards = [
            {
                'icon': 'edit',
                'value': stats.get('words_corrected', 0),
                'label': tr('Words Corrected'),
                'color': 'blue'
            },
            {
                'icon': 'description',
                'value': stats.get('documents_edited', 0),
                'label': tr('Documents Edited'),
                'color': 'green'
            },
            {
                'icon': 'lightbulb',
                'value': stats.get('total_discoveries', 0),
                'label': tr('Discoveries Shared'),
                'color': 'amber'
            },
            {
                'icon': 'help_outline',
                'value': stats.get('open_questions', 0),
                'label': tr('Open Questions'),
                'color': 'purple'
            },
            {
                'icon': 'people',
                'value': stats.get('active_contributors', 0),
                'label': tr('Active Contributors'),
                'color': 'teal'
            },
            {
                'icon': 'link',
                'value': stats.get('user_joins', 0),
                'label': tr('User Joins'),
                'color': 'green'
            },
        ]

        for card in stat_cards:
            with ui.card().classes('flex-1 p-4 min-w-36'):
                with ui.column().classes('items-center gap-2'):
                    ui.icon(card['icon']).classes(f'text-3xl text-{card["color"]}-500')
                    ui.label(str(card['value'])).classes('text-2xl font-bold')
                    ui.label(card['label']).classes('text-xs text-center').style('color: var(--text-secondary);')


async def load_feed(item_type: Optional[str], period: Optional[str], on_refresh=None):
    """Load and display the activity feed."""
    params = {"limit": 50, "offset": 0}  # Increased limit
    if item_type:
        params["item_type"] = item_type
    if period:
        params["period"] = period

    result = await api_call("GET", "/discoveries/feed/items", params)

    if "error" in result:
        with ui.column().classes('w-full items-center py-8'):
            ui.icon('error_outline').classes('text-4xl text-red-400')
            ui.label(tr('Error loading feed')).style('color: var(--text-secondary);')
        return

    items = result.get('items', [])
    total = result.get('total', 0)

    if not items:
        with ui.column().classes('w-full items-center py-12'):
            ui.icon('forum').classes('text-6xl').style('color: var(--text-tertiary);')
            ui.label(tr('No discoveries yet')).classes('text-xl mt-4').style('color: var(--text-secondary);')
            ui.label(tr('Be the first to share a discovery or ask a question!')).style('color: var(--text-tertiary);')
        return

    # Feed items - pass refresh callback for edit/delete
    for item in items:
        create_feed_item(item, on_refresh=on_refresh)

    # Show total count
    if total > len(items):
        ui.label(f"{tr('Showing')} {len(items)} {tr('of')} {total}").classes('text-sm text-center w-full mt-4').style('color: var(--text-tertiary);')


def create_feed_item(item: dict, on_refresh=None):
    """Create a single feed item card with expansion view."""
    item_type = item.get('item_type', 'discovery')
    author = item.get('author', {})
    item_id = item.get('id', '')

    # Icon and color by type
    type_styles = {
        'discovery': {'icon': 'lightbulb', 'color': 'amber', 'label': tr('Discovery')},
        'question': {'icon': 'help_outline', 'color': 'purple', 'label': tr('Question')},
        'correction': {'icon': 'edit', 'color': 'blue', 'label': tr('Correction')},
        'identification': {'icon': 'search', 'color': 'green', 'label': tr('Identification')},
        'note': {'icon': 'note', 'color': 'gray', 'label': tr('Note')},
        'comment': {'icon': 'comment', 'color': 'teal', 'label': tr('Comment')},
        'join': {'icon': 'link', 'color': 'green', 'label': tr('Join (noun)')},
    }
    style = type_styles.get(item_type, type_styles['discovery'])

    # Check if current user is the author or admin
    current_user = GlobalAuthState.get_user()
    is_author = current_user and author.get('id') == current_user.get('id')
    is_admin = current_user and current_user.get('role') == 'admin'
    is_hidden = item.get('is_hidden', False)

    # Card styling - hidden items have muted appearance
    card_classes = 'w-full p-4 hover:shadow-md transition-shadow'
    card_style = ''
    if is_hidden:
        card_classes += ' opacity-60'
        card_style = 'border-left: 4px solid #ef4444; background: #fef2f2;'

    with ui.card().classes(card_classes).style(card_style):
        # Hidden badge for admin
        if is_hidden:
            with ui.row().classes('w-full mb-2'):
                ui.badge(tr('Hidden')).props('color=red').classes('text-xs')

        with ui.row().classes('w-full items-start gap-4'):
            # Type icon and badges
            with ui.column().classes('items-center gap-1'):
                ui.icon(style['icon']).classes(f'text-2xl text-{style["color"]}-500')
                if item.get('is_pinned'):
                    ui.icon('push_pin').classes('text-sm text-red-500').tooltip(tr('Pinned'))
                if item.get('is_featured'):
                    ui.icon('star').classes('text-sm text-amber-400')
                if item.get('is_answered') and item_type == 'question':
                    ui.icon('check_circle').classes('text-sm text-green-500').tooltip(tr('Answered'))

            # Content
            with ui.column().classes('flex-1 gap-2'):
                # Header row
                with ui.row().classes('w-full items-center justify-between'):
                    with ui.row().classes('items-center gap-2 flex-wrap'):
                        ui.badge(style['label']).props(f'color={style["color"]}').classes('text-xs')

                        # Answered badge for questions
                        if item.get('is_answered') and item_type == 'question':
                            ui.badge(tr('Answered')).props('color=green').classes('text-xs')

                        # Shelfmark + page link (always show if available)
                        shelfmark = item.get('shelfmark')
                        page_num = item.get('page_number')
                        doc_id = item.get('document_id')
                        additional_shelfmarks = item.get('additional_shelfmarks', []) or []

                        # Resolve shelfmark from document_id if needed
                        display_shelfmark, _ = resolve_shelfmark(doc_id, shelfmark)

                        # Primary shelfmark
                        if display_shelfmark:
                            link_text = display_shelfmark
                            if page_num:
                                link_text += f" • {tr('Image')} {page_num}"

                            def go_to_doc(did=doc_id, pnum=page_num):
                                if did:
                                    url = f'/browse?sys_id={did}'
                                    if pnum:
                                        url += f'&page={pnum}'
                                    ui.navigate.to(url)

                            with ui.element('a').classes('cursor-pointer hover:underline text-sm font-mono').style('color: var(--primary-600);').on('click', go_to_doc):
                                ui.label(link_text)

                        # Additional shelfmarks (if any)
                        if additional_shelfmarks:
                            for add_sm in additional_shelfmarks[:3]:  # Show max 3 additional
                                add_shelfmark = add_sm.get('shelfmark', '')
                                add_doc_id = add_sm.get('document_id')
                                add_page = add_sm.get('page_number')

                                # Resolve shelfmark from document_id if needed
                                add_display_shelfmark, _ = resolve_shelfmark(add_doc_id, add_shelfmark)

                                if add_display_shelfmark:
                                    add_link_text = add_display_shelfmark
                                    if add_page:
                                        add_link_text += f" • {tr('Image')} {add_page}"

                                    def go_to_add_doc(did=add_doc_id, pnum=add_page):
                                        if did:
                                            url = f'/browse?sys_id={did}'
                                            if pnum:
                                                url += f'&page={pnum}'
                                            ui.navigate.to(url)

                                    with ui.element('a').classes('cursor-pointer hover:underline text-xs font-mono ml-2').style('color: var(--text-tertiary);').on('click', go_to_add_doc):
                                        ui.label(f"+{add_link_text}")

                            if len(additional_shelfmarks) > 3:
                                ui.label(f"+{len(additional_shelfmarks) - 3} {tr('more')}").classes('text-xs ml-1').style('color: var(--text-tertiary);')

                    # Date and actions
                    with ui.row().classes('items-center gap-2'):
                        created_at = item.get('created_at', '')
                        if created_at:
                            ui.label(format_date(created_at)).classes('text-xs').style('color: var(--text-tertiary);')

                        # Edit/Delete for author (only for discoveries, not corrections/comments)
                        if is_author and item_type in ('discovery', 'question', 'identification', 'note'):
                            numeric_id = item_id.split('_')[-1] if '_' in item_id else item_id

                            async def edit_discovery(nid=numeric_id, i=item):
                                await open_edit_discovery_dialog(nid, i, on_refresh)

                            ui.button(icon='edit', on_click=edit_discovery).props('flat round dense size=sm').tooltip(tr('Edit'))

                            async def delete_discovery(nid=numeric_id):
                                await confirm_delete_discovery(nid, on_refresh)

                            ui.button(icon='delete', on_click=delete_discovery).props('flat round dense size=sm color=negative').tooltip(tr('Delete'))

                        # Admin pin button for discoveries
                        if is_admin and item_type in ('discovery', 'question', 'identification', 'note'):
                            numeric_id = item_id.split('_')[-1] if '_' in item_id else item_id
                            is_pinned = item.get('is_pinned', False)

                            async def toggle_pin(nid=numeric_id, pinned=is_pinned):
                                result = await api_call("POST", f"/discoveries/{nid}/pin", {"pinned": not pinned})
                                if "error" not in result:
                                    ui.notify(tr('Pin toggled'), type='positive')
                                    if on_refresh:
                                        await on_refresh()

                            pin_icon = 'push_pin' if is_pinned else 'push_pin'
                            pin_color = 'color=red' if is_pinned else ''
                            ui.button(icon=pin_icon, on_click=toggle_pin).props(f'flat round dense size=sm {pin_color}').tooltip(tr('Pin') if not is_pinned else tr('Unpin'))

                            # Admin hide/unhide button for discoveries
                            is_item_hidden = item.get('is_hidden', False)

                            async def toggle_hide_discovery(nid=numeric_id, hidden=is_item_hidden):
                                endpoint = f"/discoveries/{nid}/unhide" if hidden else f"/discoveries/{nid}/hide"
                                result = await api_call("POST", endpoint)
                                if "error" not in result:
                                    msg = tr('Item unhidden') if hidden else tr('Item hidden')
                                    ui.notify(msg, type='positive')
                                    if on_refresh:
                                        await on_refresh()
                                else:
                                    ui.notify(result.get("error", tr('Error')), type='negative')

                            if is_item_hidden:
                                ui.button(icon='visibility', on_click=toggle_hide_discovery).props('flat round dense size=sm color=green').tooltip(tr('Unhide'))
                            else:
                                ui.button(icon='visibility_off', on_click=toggle_hide_discovery).props('flat round dense size=sm').tooltip(tr('Hide'))

                        # Admin delete for comments
                        if is_admin and item_type == 'comment':
                            numeric_id = item_id.split('_')[-1] if '_' in item_id else item_id

                            async def delete_comment_admin(nid=numeric_id):
                                # Confirm dialog
                                confirm_dialog = ui.dialog()
                                with confirm_dialog, ui.card().classes('p-4'):
                                    ui.label(tr('Delete this comment?')).classes('font-bold')
                                    ui.label(tr('This action cannot be undone.')).classes('text-sm text-gray-500')
                                    with ui.row().classes('justify-end gap-2 mt-4'):
                                        ui.button(tr('Cancel'), on_click=confirm_dialog.close).props('flat')

                                        async def do_delete():
                                            result = await api_call("DELETE", f"/comments/{nid}")
                                            confirm_dialog.close()
                                            if "error" not in result:
                                                ui.notify(tr('Comment deleted'), type='positive')
                                                if on_refresh:
                                                    await on_refresh()
                                            else:
                                                ui.notify(result.get("error", tr('Error')), type='negative')

                                        ui.button(tr('Delete'), on_click=do_delete).props('color=negative')
                                confirm_dialog.open()

                            ui.button(icon='delete', on_click=delete_comment_admin).props('flat round dense size=sm color=negative').tooltip(tr('Delete comment'))

                        # Admin delete for corrections
                        if is_admin and item_type == 'correction':
                            numeric_id = item_id.split('_')[-1] if '_' in item_id else item_id

                            async def delete_correction_admin(nid=numeric_id):
                                # Confirm dialog
                                confirm_dialog = ui.dialog()
                                with confirm_dialog, ui.card().classes('p-4'):
                                    ui.label(tr('Delete this correction?')).classes('font-bold')
                                    ui.label(tr('This action cannot be undone.')).classes('text-sm text-gray-500')
                                    with ui.row().classes('justify-end gap-2 mt-4'):
                                        ui.button(tr('Cancel'), on_click=confirm_dialog.close).props('flat')

                                        async def do_delete():
                                            result = await api_call("DELETE", f"/corrections/{nid}")
                                            confirm_dialog.close()
                                            if "error" not in result:
                                                ui.notify(tr('Correction deleted'), type='positive')
                                                if on_refresh:
                                                    await on_refresh()
                                            else:
                                                ui.notify(result.get("error", tr('Error')), type='negative')

                                        ui.button(tr('Delete'), on_click=do_delete).props('color=negative')
                                confirm_dialog.open()

                            ui.button(icon='delete', on_click=delete_correction_admin).props('flat round dense size=sm color=negative').tooltip(tr('Delete correction'))

                # Title - for corrections and joins, generate localized title
                if item_type == 'correction':
                    corr_shelfmark, _ = resolve_shelfmark(item.get('document_id'), item.get('shelfmark'))
                    corr_page = item.get('page_number')
                    corr_title = f"{tr('Correction in')} {corr_shelfmark}"
                    if corr_page:
                        corr_title += f" ({tr('Image')} {corr_page})"
                    ui.label(corr_title).classes('font-bold text-lg')
                elif item_type == 'join':
                    # For joins: show cluster title
                    cluster_fragments = item.get('cluster_fragments', [])
                    cluster_joins = item.get('cluster_joins', [])
                    num_joins = len(cluster_joins) if cluster_joins else 1

                    with ui.row().classes('items-center gap-2 flex-wrap'):
                        # Show cluster title
                        if cluster_fragments and len(cluster_fragments) > 2:
                            # Multi-fragment cluster: show fragments connected by arrows
                            if len(cluster_fragments) <= 4:
                                for i, frag in enumerate(cluster_fragments):
                                    if i > 0:
                                        ui.icon('sync_alt', size='sm').style('color: var(--text-tertiary);')
                                    ui.label(frag).classes('font-bold font-mono')
                            else:
                                # Too many fragments, show summary
                                ui.label(cluster_fragments[0]).classes('font-bold font-mono')
                                ui.icon('sync_alt', size='sm').style('color: var(--text-tertiary);')
                                ui.label(f"+{len(cluster_fragments) - 1}").classes('font-bold')
                        elif cluster_fragments and len(cluster_fragments) == 2:
                            # Simple 2-fragment cluster
                            ui.label(cluster_fragments[0]).classes('font-bold font-mono')
                            ui.icon('sync_alt', size='sm').style('color: var(--text-tertiary);')
                            ui.label(cluster_fragments[1]).classes('font-bold font-mono')
                        else:
                            # Fallback to title from backend
                            ui.label(item.get('title', '')).classes('font-bold text-lg')

                        # Show count badges
                        if num_joins > 1:
                            ui.badge(f"{num_joins} {tr('joins')}").props('color=teal').classes('text-xs ml-2')
                        if cluster_fragments and len(cluster_fragments) > 2:
                            ui.badge(f"{len(cluster_fragments)} {tr('fragments')}").props('color=blue outline').classes('text-xs')
                else:
                    ui.label(item.get('title', '')).classes('font-bold text-lg')

                # Full content in expansion (no truncation)
                content = item.get('content_preview', '')
                related_manuscripts = item.get('related_manuscripts', []) or []

                with ui.expansion(icon='expand_more').classes('w-full').props('dense'):
                    with ui.column().classes('w-full gap-4'):
                        # For corrections: show original and corrected text side by side with highlighting
                        if item_type == 'correction':
                            original_text = item.get('original_text', '')
                            corrected_text = item.get('corrected_text', '')
                            if original_text or corrected_text:
                                with ui.row().classes('w-full gap-4'):
                                    if original_text:
                                        with ui.column().classes('flex-1'):
                                            ui.label(tr('Original')).classes('font-medium text-xs').style('color: var(--text-tertiary);')
                                            ui.label(original_text).classes('text-sm whitespace-pre-wrap p-2 rounded').style(
                                                'background: #ffebee; direction: rtl; text-align: right; border-left: 3px solid #ef5350;'
                                            )
                                    if corrected_text:
                                        with ui.column().classes('flex-1'):
                                            ui.label(tr('Corrected')).classes('font-medium text-xs').style('color: var(--text-tertiary);')
                                            ui.label(corrected_text).classes('text-sm whitespace-pre-wrap p-2 rounded').style(
                                                'background: #e8f5e9; direction: rtl; text-align: right; border-left: 3px solid #66bb6a;'
                                            )

                                # Show visual diff if texts differ
                                if original_text and corrected_text and original_text != corrected_text:
                                    with ui.row().classes('w-full items-center gap-2 mt-2'):
                                        ui.icon('compare_arrows', size='sm').style('color: var(--text-tertiary);')
                                        ui.label(tr('Change highlighted')).classes('text-xs').style('color: var(--text-tertiary);')
                        elif item_type == 'join':
                            # For joins: show cluster details with individual joins
                            cluster_fragments = item.get('cluster_fragments', [])
                            cluster_joins = item.get('cluster_joins', [])

                            join_rel_labels = {
                                'physical_join': tr('Physical join'),
                                'same_composition': tr('Same composition')
                            }

                            # Show all fragments in the cluster
                            if cluster_fragments:
                                ui.label(tr('Fragments in cluster')).classes('text-xs font-medium mb-2').style('color: var(--text-tertiary);')
                                with ui.row().classes('w-full flex-wrap gap-2 mb-3'):
                                    for frag in cluster_fragments:
                                        # Try to find document_id for this fragment from cluster_joins
                                        frag_doc_id = None
                                        for cj in cluster_joins:
                                            if cj.get('fragment_a') == frag:
                                                frag_doc_id = cj.get('document_id_a')
                                                break
                                            elif cj.get('fragment_b') == frag:
                                                frag_doc_id = cj.get('document_id_b')
                                                break

                                        def nav_to_frag(did=frag_doc_id):
                                            if did:
                                                ui.navigate.to(f'/browse?sys_id={did}')

                                        with ui.card().classes('p-2 cursor-pointer hover:bg-gray-100').style('background: var(--surface-secondary);').on('click', nav_to_frag if frag_doc_id else None):
                                            with ui.row().classes('items-center gap-1'):
                                                ui.icon('description', size='xs').style('color: var(--text-tertiary);')
                                                ui.label(frag).classes('font-mono text-sm').style('color: var(--primary-600);' if frag_doc_id else 'color: var(--text-secondary);')

                            # Show individual joins
                            if cluster_joins:
                                ui.separator().classes('my-2')
                                ui.label(f"{tr('Joins')} ({len(cluster_joins)})").classes('text-xs font-medium mb-2').style('color: var(--text-tertiary);')

                                for cj in cluster_joins:
                                    cj_frag_a = cj.get('fragment_a', '')
                                    cj_frag_b = cj.get('fragment_b', '')
                                    cj_rel_type = cj.get('relationship_type', '')
                                    cj_notes = cj.get('notes', '')
                                    cj_author = cj.get('created_by_username', '')
                                    cj_id = cj.get('id')

                                    with ui.card().classes('w-full p-2 mb-2').style('background: var(--surface-secondary);'):
                                        with ui.row().classes('w-full items-center justify-between'):
                                            with ui.row().classes('items-center gap-2'):
                                                ui.label(cj_frag_a).classes('font-mono text-sm font-medium')
                                                ui.icon('sync_alt', size='xs').style('color: var(--text-tertiary);')
                                                ui.label(cj_frag_b).classes('font-mono text-sm font-medium')

                                                if cj_rel_type:
                                                    ui.badge(join_rel_labels.get(cj_rel_type, cj_rel_type)).props('color=teal outline').classes('text-xs ml-2')

                                            # Admin delete button for individual join
                                            if is_admin and cj_id:
                                                async def delete_single_join(jid=cj_id):
                                                    confirm_dialog = ui.dialog()
                                                    with confirm_dialog, ui.card().classes('p-4'):
                                                        ui.label(tr('Delete this join?')).classes('font-bold')
                                                        ui.label(tr('This action cannot be undone.')).classes('text-sm text-gray-500')
                                                        with ui.row().classes('justify-end gap-2 mt-4'):
                                                            ui.button(tr('Cancel'), on_click=confirm_dialog.close).props('flat')

                                                            async def do_delete():
                                                                result = await api_call("DELETE", f"/joins/{jid}")
                                                                confirm_dialog.close()
                                                                if "error" not in result:
                                                                    ui.notify(tr('Join deleted'), type='positive')
                                                                    if on_refresh:
                                                                        await on_refresh()
                                                                else:
                                                                    ui.notify(result.get("error", tr('Error')), type='negative')

                                                            ui.button(tr('Delete'), on_click=do_delete).props('color=negative')
                                                    confirm_dialog.open()

                                                ui.button(icon='delete', on_click=delete_single_join).props('flat round dense size=xs color=negative').tooltip(tr('Delete join'))

                                        if cj_notes:
                                            ui.label(cj_notes).classes('text-xs mt-1').style('color: var(--text-secondary);')

                                        if cj_author:
                                            ui.label(f"{tr('By')}: {cj_author}").classes('text-xs').style('color: var(--text-tertiary);')

                            # View cluster button
                            first_doc_id = item.get('document_id')
                            if first_doc_id:
                                with ui.row().classes('w-full items-center gap-2 mt-3'):
                                    def view_cluster_browse(did=first_doc_id):
                                        ui.navigate.to(f'/browse?sys_id={did}')
                                    ui.button(tr('View in browser'), icon='open_in_new', on_click=view_cluster_browse).props('outlined dense')
                        else:
                            # Full content for non-corrections
                            ui.label(content).classes('text-sm whitespace-pre-wrap').style('color: var(--text-secondary); direction: rtl;')

                        # Show related manuscripts if any
                        if related_manuscripts and len(related_manuscripts) > 0:
                            ui.separator().classes('my-2')
                            with ui.row().classes('w-full items-center gap-2'):
                                ui.icon('link', size='sm').style('color: var(--primary-600);')
                                ui.label(tr('Related Manuscripts')).classes('text-xs font-medium').style('color: var(--text-secondary);')

                            with ui.row().classes('w-full flex-wrap gap-2'):
                                relationship_labels = {
                                    'parallel': tr('Parallel text'),
                                    'continuation': tr('Continuation'),
                                    'fragment': tr('Fragment of'),
                                    'related': tr('Related'),
                                    'citation': tr('Citation')
                                }
                                for rel in related_manuscripts[:5]:  # Show max 5
                                    rel_shelfmark = rel.get('shelfmark', '')
                                    rel_doc_id = rel.get('document_id', '')
                                    rel_type = rel.get('relationship_type', 'related')
                                    rel_notes = rel.get('notes', '')

                                    # Resolve shelfmark
                                    rel_display_shelfmark, _ = resolve_shelfmark(rel_doc_id, rel_shelfmark)

                                    with ui.card().classes('p-2').style('background: var(--surface-secondary);'):
                                        def go_to_rel_doc(did=rel_doc_id):
                                            if did:
                                                ui.navigate.to(f'/browse?sys_id={did}')

                                        with ui.column().classes('gap-1'):
                                            with ui.row().classes('items-center gap-1'):
                                                ui.badge(relationship_labels.get(rel_type, rel_type)).props('color=grey').classes('text-xs')
                                                with ui.element('a').classes('cursor-pointer hover:underline text-sm font-mono').style('color: var(--primary-600);').on('click', go_to_rel_doc):
                                                    ui.label(rel_display_shelfmark)
                                            if rel_notes:
                                                ui.label(rel_notes).classes('text-xs').style('color: var(--text-tertiary);')

                        # Document link
                        if item.get('document_id'):
                            with ui.row().classes('items-center gap-2'):
                                ui.icon('link', size='sm')
                                doc_shelfmark, _ = resolve_shelfmark(item.get('document_id'), item.get('shelfmark'))
                                page_num = item.get('page_number')
                                link_text = f"{tr('View document')}: {doc_shelfmark}"
                                if page_num:
                                    link_text += f" ({tr('Image')} {page_num})"

                                def go_to_doc2(did=item.get('document_id'), pnum=page_num):
                                    url = f'/browse?sys_id={did}'
                                    if pnum:
                                        url += f'&page={pnum}'
                                    ui.navigate.to(url)

                                ui.button(link_text, on_click=go_to_doc2).props('flat dense').classes('text-primary')

                        # Voting section for discoveries
                        if item_type in ('discovery', 'question', 'identification', 'note'):
                            ui.separator().classes('my-2')
                            numeric_id = item_id.split('_')[-1] if '_' in item_id else item_id

                            with ui.row().classes('w-full items-center gap-4'):
                                # Vote buttons
                                upvotes = item.get('upvotes', 0) or 0
                                downvotes = item.get('downvotes', 0) or 0

                                with ui.row().classes('items-center gap-1'):
                                    async def vote_up(nid=numeric_id):
                                        if not GlobalAuthState.is_logged_in():
                                            ui.notify(tr('Login to vote'), type='warning')
                                            return
                                        result = await api_call("POST", f"/discoveries/{nid}/vote?vote_type=up")
                                        if "error" not in result:
                                            ui.notify(tr('Vote recorded'), type='positive')
                                            if on_refresh:
                                                await on_refresh()

                                    ui.button(icon='thumb_up', on_click=vote_up).props('flat dense size=sm').tooltip(tr('Upvote'))
                                    ui.label(str(upvotes)).classes('text-sm font-medium')

                                with ui.row().classes('items-center gap-1'):
                                    async def vote_down(nid=numeric_id):
                                        if not GlobalAuthState.is_logged_in():
                                            ui.notify(tr('Login to vote'), type='warning')
                                            return
                                        result = await api_call("POST", f"/discoveries/{nid}/vote?vote_type=down")
                                        if "error" not in result:
                                            ui.notify(tr('Vote recorded'), type='positive')
                                            if on_refresh:
                                                await on_refresh()

                                    ui.button(icon='thumb_down', on_click=vote_down).props('flat dense size=sm').tooltip(tr('Downvote'))
                                    ui.label(str(downvotes)).classes('text-sm font-medium')

                                # Mark as answered button (for questions, author or admin only)
                                if item_type == 'question' and (is_author or is_admin):
                                    is_answered = item.get('is_answered', False)

                                    async def toggle_answered(nid=numeric_id, answered=is_answered):
                                        result = await api_call("POST", f"/discoveries/{nid}/answer?answered={str(not answered).lower()}")
                                        if "error" not in result:
                                            ui.notify(tr('Status updated'), type='positive')
                                            if on_refresh:
                                                await on_refresh()

                                    if is_answered:
                                        ui.button(tr('Mark as unanswered'), icon='help_outline', on_click=toggle_answered).props('flat dense size=sm')
                                    else:
                                        ui.button(tr('Mark as answered'), icon='check_circle', on_click=toggle_answered).props('flat dense size=sm color=green')

                            # Responses section
                            ui.separator().classes('my-2')
                            responses_container = ui.column().classes('w-full gap-2')

                            async def load_responses(container=responses_container, nid=numeric_id):
                                container.clear()
                                with container:
                                    result = await api_call("GET", f"/discoveries/{nid}/responses")
                                    if "error" not in result:
                                        responses = result.get('items', [])
                                        if responses:
                                            ui.label(f"{tr('Responses')} ({len(responses)})").classes('font-medium text-sm')
                                            for resp in responses:
                                                create_response_item(resp)
                                        else:
                                            ui.label(tr('No responses yet')).classes('text-sm').style('color: var(--text-tertiary);')

                                    # Reply form
                                    if GlobalAuthState.is_logged_in():
                                        ui.separator().classes('my-2')
                                        reply_input = ui.textarea(placeholder=tr('Write a reply...')).classes('w-full').props('outlined dense rows=2').style('direction: rtl;')
                                        anonymous_reply = ui.checkbox(tr('Reply anonymously'), value=False).classes('text-xs')

                                        async def submit_reply(inp=reply_input, anon=anonymous_reply, nid=nid):
                                            if not inp.value or not inp.value.strip():
                                                ui.notify(tr('Please enter a reply'), type='warning')
                                                return
                                            result = await api_call("POST", f"/discoveries/{nid}/responses", {
                                                "content": inp.value.strip(),
                                                "is_anonymous": anon.value
                                            })
                                            if "error" in result:
                                                ui.notify(result["error"], type='negative')
                                            else:
                                                ui.notify(tr('Reply posted'), type='positive')
                                                inp.value = ''
                                                await load_responses()

                                        ui.button(tr('Reply'), icon='send', on_click=submit_reply).props('dense color=primary').classes('self-end')
                                    else:
                                        ui.label(tr('Login to reply')).classes('text-xs').style('color: var(--text-tertiary);')

                            # Load responses when expansion opens
                            ui.timer(0.1, load_responses, once=True)

                # Footer - collapsed view info
                with ui.row().classes('w-full items-center justify-between mt-2'):
                    # Author
                    if author.get('is_anonymous', False):
                        author_name = tr('Anonymous')
                    else:
                        author_name = author.get('full_name') or author.get('username') or tr('Anonymous')
                    affiliation = author.get('affiliation', '')
                    author_text = f"{author_name}" + (f" ({affiliation})" if affiliation else "")
                    ui.label(author_text).classes('text-xs').style('color: var(--text-tertiary);')

                    # Response count
                    response_count = item.get('response_count', 0)
                    if response_count > 0:
                        with ui.row().classes('items-center gap-1'):
                            ui.icon('chat_bubble_outline', size='xs').style('color: var(--text-tertiary);')
                            ui.label(str(response_count)).classes('text-xs').style('color: var(--text-tertiary);')


def create_response_item(resp: dict):
    """Create a single response/reply item."""
    author = resp.get('author', {})
    if author.get('is_anonymous', False):
        author_name = tr('Anonymous')
    else:
        author_name = author.get('full_name') or author.get('username') or tr('Anonymous')

    with ui.card().classes('w-full p-3').style('background: var(--surface-secondary);'):
        with ui.row().classes('w-full items-center justify-between mb-1'):
            ui.label(author_name).classes('text-xs font-medium')
            ui.label(format_date(resp.get('created_at', ''))).classes('text-xs').style('color: var(--text-tertiary);')
        ui.label(resp.get('content', '')).classes('text-sm whitespace-pre-wrap').style('direction: rtl;')


async def open_edit_discovery_dialog(discovery_id: str, item: dict, on_refresh=None):
    """Open dialog to edit a discovery."""
    from web.state import state

    dialog = ui.dialog()

    # State for document selection
    selected_doc = {
        'sys_id': item.get('document_id'),
        'shelfmark': item.get('shelfmark'),
        'page_number': item.get('page_number'),
        'total_pages': 0
    }

    def truncate_title(title: str, max_words: int = 4) -> tuple:
        """Truncate title to max_words, return (short, full) for tooltip."""
        if not title:
            return '', ''
        words = title.split()
        if len(words) <= max_words:
            return title, ''
        return ' '.join(words[:max_words]) + '...', title

    with dialog, ui.card().classes('w-full max-w-lg p-6'):
        ui.label(tr('Edit Discovery')).classes('text-xl font-bold mb-4')

        title_input = ui.input(label=tr('Title'), value=item.get('title', '')).classes('w-full').props('outlined')
        content_input = ui.textarea(label=tr('Description'), value=item.get('content_preview', '')).classes('w-full').props('outlined rows=5').style('direction: rtl;')

        # Document link section
        with ui.expansion(tr('Document link'), icon='link').classes('w-full') as doc_expansion:
            # Document info container
            doc_info_container = ui.column().classes('w-full gap-1')

            # Page selection container
            page_select_container = ui.column().classes('w-full')

            def clear_document_selection():
                """Clear the document selection."""
                selected_doc['sys_id'] = None
                selected_doc['shelfmark'] = None
                selected_doc['page_number'] = None
                selected_doc['total_pages'] = 0
                doc_info_container.clear()
                page_select_container.clear()
                update_doc_info()

            def update_doc_info():
                """Update the document info display."""
                doc_info_container.clear()
                if selected_doc['sys_id']:
                    with doc_info_container:
                        with ui.card().classes('w-full p-2').style('background: var(--surface-secondary);'):
                            with ui.row().classes('items-center gap-2'):
                                ui.icon('check_circle', size='sm').classes('text-green-500')
                                ui.label(selected_doc['shelfmark'] or selected_doc['sys_id']).classes('font-medium')
                                ui.button(icon='close', on_click=clear_document_selection).props('flat round dense size=xs')
                else:
                    with doc_info_container:
                        ui.label(tr('No document linked')).classes('text-sm').style('color: var(--text-tertiary);')

            def select_document(sys_id: str, shelfmark: str, title: str = '', page: int = 1):
                """Select a document and update UI."""
                selected_doc['sys_id'] = sys_id
                selected_doc['shelfmark'] = shelfmark

                # Clear and rebuild page selector
                page_select_container.clear()

                # Get total pages for this document
                if state.meta_mgr:
                    try:
                        browse_page = state.meta_mgr.get_browse_page(sys_id, p_num=1)
                        if browse_page and browse_page.total_pages > 0:
                            total = browse_page.total_pages
                            selected_doc['total_pages'] = total
                            page_options = {str(i): f"{tr('Image')} {i}" for i in range(1, total + 1)}
                            initial_page = str(min(page, total))
                            with page_select_container:
                                page_sel = ui.select(
                                    options=page_options,
                                    value=initial_page,
                                    label=tr('Image number')
                                ).classes('w-full').props('outlined dense')

                                def on_page_change(e):
                                    try:
                                        selected_doc['page_number'] = int(e.value)
                                    except:
                                        selected_doc['page_number'] = None

                                page_sel.on('update:model-value', on_page_change)
                            selected_doc['page_number'] = int(initial_page)
                    except Exception:
                        pass

                update_doc_info()

            # Single dialog for document selection
            doc_picker_dialog = ui.dialog()
            doc_picker_content = ui.column()

            def show_document_items(items, title_text, back_callback=None):
                """Show document items in the picker dialog."""
                doc_picker_content.clear()
                with doc_picker_content:
                    with ui.card().classes('w-96 p-4'):
                        with ui.row().classes('w-full items-center justify-between mb-3'):
                            if back_callback:
                                ui.button(icon='arrow_back', on_click=back_callback).props('flat round dense')
                            ui.label(title_text).classes('font-bold flex-grow')
                            ui.button(icon='close', on_click=doc_picker_dialog.close).props('flat round dense')

                        if not items:
                            ui.label(tr('No items found')).classes('text-gray-500 p-4')
                        else:
                            with ui.scroll_area().classes('w-full').style('max-height: 350px;'):
                                for itm in items:
                                    doc_id = itm.get('sys_id') or itm.get('document_id') or itm.get('system_id', '')
                                    shelfmark = itm.get('shelfmark', '')
                                    title = itm.get('title', '')
                                    page = itm.get('page_number') or itm.get('page', 1) or 1

                                    if doc_id and state.meta_mgr and not shelfmark:
                                        try:
                                            sh, ti = state.meta_mgr.get_meta_for_id(doc_id)
                                            shelfmark = sh or doc_id
                                            title = title or ti or ''
                                        except:
                                            shelfmark = doc_id

                                    def make_pick(did=doc_id, sm=shelfmark, ti=title, pg=page):
                                        def pick():
                                            select_document(did, sm, ti, pg)
                                            doc_picker_dialog.close()
                                        return pick

                                    with ui.card().classes('w-full p-2 mb-2 cursor-pointer hover:bg-gray-100').on('click', make_pick()):
                                        display_text = shelfmark or doc_id
                                        if page and page > 1:
                                            display_text += f" • {tr('Image')} {page}"
                                        ui.label(display_text).classes('font-medium text-sm')
                                        if title:
                                            short_title, full_title = truncate_title(title)
                                            t_label = ui.label(short_title).classes('text-xs').style('color: var(--text-secondary);')
                                            if full_title:
                                                t_label.tooltip(full_title)

            def show_lists_view():
                """Show list of user's lists."""
                if not state.lists_mgr:
                    ui.notify(tr('Lists not available'), type='warning')
                    return

                lists = state.lists_mgr.data.get('lists', {})
                if not lists:
                    ui.notify(tr('No lists found'), type='info')
                    return

                doc_picker_content.clear()
                with doc_picker_content:
                    with ui.card().classes('w-96 p-4'):
                        with ui.row().classes('w-full items-center justify-between mb-3'):
                            ui.label(tr('Select a list')).classes('font-bold flex-grow')
                            ui.button(icon='close', on_click=doc_picker_dialog.close).props('flat round dense')

                        with ui.scroll_area().classes('w-full').style('max-height: 350px;'):
                            for list_id, list_data in lists.items():
                                list_name = list_data.get('name', list_id)
                                color = list_data.get('color', '#999')
                                try:
                                    count = state.lists_mgr._get_list_item_count(list_id) if list_id != 'recent' else len(state.lists_mgr.data.get('recent_items', []))
                                except:
                                    count = 0

                                def make_list_click(lid=list_id, lname=list_name):
                                    def click():
                                        items = state.lists_mgr.get_items_in_list(lid)
                                        show_document_items(items, f"{tr('Items in')}: {lname}", back_callback=show_lists_view)
                                    return click

                                with ui.card().classes('w-full p-3 mb-2 cursor-pointer hover:bg-gray-100').on('click', make_list_click()):
                                    with ui.row().classes('items-center gap-2'):
                                        ui.icon('circle').style(f'color: {color}; font-size: 1rem;')
                                        ui.label(list_name).classes('font-medium flex-grow')
                                        ui.badge(str(count)).classes('bg-gray-200')

                doc_picker_dialog.open()

            def fetch_recent():
                """Fetch from recent browsing history."""
                if not state.lists_mgr:
                    ui.notify(tr('Lists not available'), type='warning')
                    return

                recent_items = state.lists_mgr.data.get('recent_items', [])
                if not recent_items:
                    ui.notify(tr('No recent activity'), type='info')
                    return

                show_document_items(recent_items, tr('Recent Activity'))
                doc_picker_dialog.open()

            # Attach dialog content container
            with doc_picker_dialog:
                doc_picker_content

            # Quick select buttons
            with ui.row().classes('w-full gap-2 mb-3'):
                ui.button(tr('Recent Activity'), icon='history', on_click=fetch_recent).props('outlined dense')
                ui.button(tr('My Lists'), icon='bookmark', on_click=show_lists_view).props('outlined dense')

            # Initialize: show current document info and page selector if applicable
            if selected_doc['sys_id']:
                # Load page options for existing document
                if state.meta_mgr:
                    try:
                        browse_page = state.meta_mgr.get_browse_page(selected_doc['sys_id'], p_num=1)
                        if browse_page and browse_page.total_pages > 0:
                            total = browse_page.total_pages
                            selected_doc['total_pages'] = total
                            page_options = {str(i): f"{tr('Image')} {i}" for i in range(1, total + 1)}
                            initial_page = str(selected_doc['page_number']) if selected_doc['page_number'] else '1'
                            if int(initial_page) > total:
                                initial_page = '1'
                            with page_select_container:
                                page_sel = ui.select(
                                    options=page_options,
                                    value=initial_page,
                                    label=tr('Image number')
                                ).classes('w-full').props('outlined dense')

                                def on_page_change(e):
                                    try:
                                        selected_doc['page_number'] = int(e.value)
                                    except:
                                        selected_doc['page_number'] = None

                                page_sel.on('update:model-value', on_page_change)
                    except Exception:
                        pass

            update_doc_info()

            # Expand if document is linked
            if selected_doc['sys_id']:
                doc_expansion.value = True

        with ui.row().classes('w-full justify-end gap-2 mt-4'):
            ui.button(tr('Cancel'), on_click=dialog.close).props('flat')

            async def save_changes():
                data = {
                    "title": title_input.value,
                    "content": content_input.value,
                    "document_id": selected_doc['sys_id'],
                    "shelfmark": selected_doc['shelfmark'],
                    "page_number": selected_doc['page_number']
                }
                result = await api_call("PUT", f"/discoveries/{discovery_id}", data)
                if "error" in result:
                    ui.notify(result["error"], type='negative')
                else:
                    ui.notify(tr('Discovery updated'), type='positive')
                    dialog.close()
                    if on_refresh:
                        if asyncio.iscoroutinefunction(on_refresh):
                            await on_refresh()
                        else:
                            on_refresh()

            ui.button(tr('Save'), icon='save', on_click=save_changes).props('color=primary')

    dialog.open()


async def confirm_delete_discovery(discovery_id: str, on_refresh=None):
    """Confirm and delete a discovery."""
    dialog = ui.dialog()

    with dialog, ui.card().classes('p-4'):
        ui.label(tr('Delete Discovery?')).classes('text-lg font-bold')
        ui.label(tr('This action cannot be undone.')).classes('text-sm text-gray-500')

        with ui.row().classes('justify-end gap-2 mt-4'):
            ui.button(tr('Cancel'), on_click=dialog.close).props('flat')

            async def do_delete():
                result = await api_call("DELETE", f"/discoveries/{discovery_id}")
                dialog.close()
                if "error" in result:
                    ui.notify(result["error"], type='negative')
                else:
                    ui.notify(tr('Discovery deleted'), type='positive')
                    if on_refresh:
                        await on_refresh()

            ui.button(tr('Delete'), on_click=do_delete).props('color=negative')

    dialog.open()


def create_new_discovery_dialog(on_success=None):
    """Create dialog for posting a new discovery/question."""
    from web.state import state

    dialog = ui.dialog().props('persistent')

    # State for document selection
    selected_doc = {'sys_id': None, 'shelfmark': None, 'title': None, 'total_pages': 0}
    # State for additional shelfmarks
    additional_shelfmarks_list = []
    # State for related manuscripts
    related_manuscripts_list = []

    def truncate_title(title: str, max_words: int = 4) -> tuple:
        """Truncate title to max_words, return (short, full) for tooltip."""
        if not title:
            return '', ''
        words = title.split()
        if len(words) <= max_words:
            return title, ''
        return ' '.join(words[:max_words]) + '...', title

    with dialog:
        with ui.card().classes('w-full max-w-lg p-6'):
            with ui.row().classes('w-full items-center justify-between mb-4'):
                ui.label(tr('Share a Discovery')).classes('text-xl font-bold')
                ui.button(icon='close', on_click=dialog.close).props('flat round')

            with ui.column().classes('w-full gap-4'):
                # Type selection
                disc_type = ui.select(
                    {
                        'discovery': tr('Discovery - Found something interesting'),
                        'question': tr('Question - Need help reading/understanding'),
                        'identification': tr('Identification - Identified a text'),
                        'note': tr('Note - General observation'),
                    },
                    value='discovery',
                    label=tr('Type')
                ).classes('w-full').props('outlined')

                # Title
                title_input = ui.input(
                    label=tr('Title'),
                    placeholder=tr('Brief description of your discovery')
                ).classes('w-full').props('outlined')

                # Content
                content_input = ui.textarea(
                    label=tr('Description'),
                    placeholder=tr('Describe your discovery in detail...')
                ).classes('w-full').props('outlined rows=5').style('direction: rtl;')

                # Document reference (optional)
                with ui.expansion(tr('Link to document (optional)'), icon='link').classes('w-full') as doc_expansion:
                    # Selected document info
                    doc_info_container = ui.column().classes('w-full gap-1')

                    # Hidden inputs for form data
                    doc_id_input = ui.input().classes('hidden')
                    shelfmark_hidden = ui.input().classes('hidden')

                    # Page selection container (shown when document selected)
                    page_select_container = ui.column().classes('w-full')

                    def clear_document_selection():
                        """Clear the document selection."""
                        selected_doc['sys_id'] = None
                        selected_doc['shelfmark'] = None
                        selected_doc['title'] = None
                        selected_doc['total_pages'] = 0
                        doc_id_input.value = ''
                        shelfmark_hidden.value = ''
                        doc_info_container.clear()
                        page_select_container.clear()

                    def update_doc_info():
                        """Update the document info display."""
                        doc_info_container.clear()
                        if selected_doc['sys_id']:
                            with doc_info_container:
                                with ui.card().classes('w-full p-2').style('background: var(--surface-secondary);'):
                                    with ui.row().classes('items-center gap-2'):
                                        ui.icon('check_circle', size='sm').classes('text-green-500')
                                        ui.label(selected_doc['shelfmark'] or '').classes('font-medium')
                                        ui.button(icon='close', on_click=clear_document_selection).props('flat round dense size=xs')
                                    if selected_doc['title']:
                                        short_title, full_title = truncate_title(selected_doc['title'])
                                        title_label = ui.label(short_title).classes('text-sm').style('color: var(--text-secondary);')
                                        if full_title:
                                            title_label.tooltip(full_title)
                                    ui.label(f"ID: {selected_doc['sys_id']}").classes('text-xs').style('color: var(--text-tertiary);')

                    def select_document(sys_id: str, shelfmark: str, title: str = '', page: int = 1):
                        """Select a document and update UI."""
                        selected_doc['sys_id'] = sys_id
                        selected_doc['shelfmark'] = shelfmark
                        selected_doc['title'] = title

                        doc_id_input.value = sys_id
                        shelfmark_hidden.value = shelfmark

                        # Clear and rebuild page selector
                        page_select_container.clear()

                        # Get total pages for this document
                        if state.meta_mgr:
                            try:
                                browse_page = state.meta_mgr.get_browse_page(sys_id, p_num=1)
                                if browse_page and browse_page.total_pages > 0:
                                    total = browse_page.total_pages
                                    selected_doc['total_pages'] = total
                                    page_options = {str(i): f"{tr('Image')} {i}" for i in range(1, total + 1)}
                                    initial_page = str(min(page, total))
                                    with page_select_container:
                                        ui.select(
                                            options=page_options,
                                            value=initial_page,
                                            label=tr('Image number')
                                        ).classes('w-full').props('outlined dense').bind_value(
                                            selected_doc, 'selected_page'
                                        )
                                    selected_doc['selected_page'] = initial_page
                            except Exception:
                                pass

                        update_doc_info()

                    # Single dialog for document selection - reused for all cases
                    doc_picker_dialog = ui.dialog()
                    doc_picker_content = ui.column()

                    def show_document_items(items, title_text, back_callback=None):
                        """Show document items in the picker dialog."""
                        doc_picker_content.clear()
                        with doc_picker_content:
                            with ui.card().classes('w-96 p-4'):
                                with ui.row().classes('w-full items-center justify-between mb-3'):
                                    if back_callback:
                                        ui.button(icon='arrow_back', on_click=back_callback).props('flat round dense')
                                    ui.label(title_text).classes('font-bold flex-grow')
                                    ui.button(icon='close', on_click=doc_picker_dialog.close).props('flat round dense')

                                if not items:
                                    ui.label(tr('No items found')).classes('text-gray-500 p-4')
                                else:
                                    with ui.scroll_area().classes('w-full').style('max-height: 350px;'):
                                        for item in items:
                                            doc_id = item.get('sys_id') or item.get('document_id') or item.get('system_id', '')
                                            shelfmark = item.get('shelfmark', '')
                                            title = item.get('title', '')
                                            page = item.get('page_number') or item.get('page', 1) or 1

                                            if doc_id and state.meta_mgr and not shelfmark:
                                                try:
                                                    sh, ti = state.meta_mgr.get_meta_for_id(doc_id)
                                                    shelfmark = sh or doc_id
                                                    title = title or ti or ''
                                                except:
                                                    shelfmark = doc_id

                                            def make_pick(did=doc_id, sm=shelfmark, ti=title, pg=page):
                                                def pick():
                                                    select_document(did, sm, ti, pg)
                                                    doc_picker_dialog.close()
                                                return pick

                                            with ui.card().classes('w-full p-2 mb-2 cursor-pointer hover:bg-gray-100').on('click', make_pick()):
                                                display_text = shelfmark or doc_id
                                                if page and page > 1:
                                                    display_text += f" • {tr('Image')} {page}"
                                                ui.label(display_text).classes('font-medium text-sm')
                                                if title:
                                                    short_title, full_title = truncate_title(title)
                                                    t_label = ui.label(short_title).classes('text-xs').style('color: var(--text-secondary);')
                                                    if full_title:
                                                        t_label.tooltip(full_title)

                    def show_lists_view():
                        """Show list of user's lists."""
                        if not state.lists_mgr:
                            ui.notify(tr('Lists not available'), type='warning')
                            return

                        lists = state.lists_mgr.data.get('lists', {})
                        if not lists:
                            ui.notify(tr('No lists found'), type='info')
                            return

                        doc_picker_content.clear()
                        with doc_picker_content:
                            with ui.card().classes('w-96 p-4'):
                                with ui.row().classes('w-full items-center justify-between mb-3'):
                                    ui.label(tr('Select a list')).classes('font-bold flex-grow')
                                    ui.button(icon='close', on_click=doc_picker_dialog.close).props('flat round dense')

                                with ui.scroll_area().classes('w-full').style('max-height: 350px;'):
                                    for list_id, list_data in lists.items():
                                        list_name = list_data.get('name', list_id)
                                        color = list_data.get('color', '#999')
                                        try:
                                            count = state.lists_mgr._get_list_item_count(list_id) if list_id != 'recent' else len(state.lists_mgr.data.get('recent_items', []))
                                        except:
                                            count = 0

                                        def make_list_click(lid=list_id, lname=list_name):
                                            def click():
                                                items = state.lists_mgr.get_items_in_list(lid)
                                                show_document_items(items, f"{tr('Items in')}: {lname}", back_callback=show_lists_view)
                                            return click

                                        with ui.card().classes('w-full p-3 mb-2 cursor-pointer hover:bg-gray-100').on('click', make_list_click()):
                                            with ui.row().classes('items-center gap-2'):
                                                ui.icon('circle').style(f'color: {color}; font-size: 1rem;')
                                                ui.label(list_name).classes('font-medium flex-grow')
                                                ui.badge(str(count)).classes('bg-gray-200')

                        doc_picker_dialog.open()

                    def fetch_recent():
                        """Fetch from recent browsing history."""
                        if not state.lists_mgr:
                            ui.notify(tr('Lists not available'), type='warning')
                            return

                        recent_items = state.lists_mgr.data.get('recent_items', [])
                        if not recent_items:
                            ui.notify(tr('No recent activity'), type='info')
                            return

                        show_document_items(recent_items, tr('Recent Activity'))
                        doc_picker_dialog.open()

                    # Attach dialog content container
                    with doc_picker_dialog:
                        doc_picker_content

                    # Quick select buttons
                    with ui.row().classes('w-full gap-2 mb-3'):
                        ui.button(tr('Recent Activity'), icon='history', on_click=fetch_recent).props('outlined dense')
                        ui.button(tr('My Lists'), icon='bookmark', on_click=show_lists_view).props('outlined dense')

                # Additional Shelfmarks section
                with ui.expansion(tr('Additional shelfmarks (optional)'), icon='library_books').classes('w-full'):
                    additional_shelfmarks_container = ui.column().classes('w-full gap-2')

                    def add_additional_shelfmark():
                        """Add an additional shelfmark entry."""
                        new_entry = {'shelfmark': '', 'document_id': '', 'page_number': None}
                        additional_shelfmarks_list.append(new_entry)
                        idx = len(additional_shelfmarks_list) - 1

                        with additional_shelfmarks_container:
                            with ui.card().classes('w-full p-2').style('background: var(--surface-secondary);') as card:
                                with ui.row().classes('w-full items-center gap-2'):
                                    shelfmark_input = ui.input(
                                        label=tr('Shelfmark'),
                                        placeholder='T-S 13J1.1'
                                    ).classes('flex-1').props('outlined dense')

                                    page_input = ui.number(
                                        label=tr('Page'),
                                        min=1
                                    ).classes('w-20').props('outlined dense')

                                    def remove_entry(i=idx, c=card):
                                        if i < len(additional_shelfmarks_list):
                                            additional_shelfmarks_list.pop(i)
                                        c.delete()

                                    ui.button(icon='close', on_click=remove_entry).props('flat round dense size=sm color=negative')

                                def update_entry_shelfmark(e, i=idx):
                                    if i < len(additional_shelfmarks_list):
                                        additional_shelfmarks_list[i]['shelfmark'] = e.value

                                def update_entry_page(e, i=idx):
                                    if i < len(additional_shelfmarks_list):
                                        additional_shelfmarks_list[i]['page_number'] = int(e.value) if e.value else None

                                shelfmark_input.on('update:model-value', update_entry_shelfmark)
                                page_input.on('update:model-value', update_entry_page)

                    ui.button(tr('Add shelfmark'), icon='add', on_click=add_additional_shelfmark).props('outlined dense').classes('mt-2')

                # Related Manuscripts section
                with ui.expansion(tr('Related manuscripts (optional)'), icon='link').classes('w-full'):
                    related_manuscripts_container = ui.column().classes('w-full gap-2')

                    relationship_types = {
                        'parallel': tr('Parallel text'),
                        'continuation': tr('Continuation'),
                        'fragment': tr('Fragment of'),
                        'related': tr('Related'),
                        'citation': tr('Citation')
                    }

                    def add_related_manuscript():
                        """Add a related manuscript entry."""
                        new_entry = {'shelfmark': '', 'document_id': '', 'relationship_type': 'related', 'notes': ''}
                        related_manuscripts_list.append(new_entry)
                        idx = len(related_manuscripts_list) - 1

                        with related_manuscripts_container:
                            with ui.card().classes('w-full p-2').style('background: var(--surface-secondary);') as card:
                                with ui.column().classes('w-full gap-2'):
                                    with ui.row().classes('w-full items-center gap-2'):
                                        shelfmark_input = ui.input(
                                            label=tr('Shelfmark'),
                                            placeholder='T-S 8.1'
                                        ).classes('flex-1').props('outlined dense')

                                        rel_type_select = ui.select(
                                            options=relationship_types,
                                            value='related',
                                            label=tr('Relationship')
                                        ).classes('w-32').props('outlined dense')

                                        def remove_entry(i=idx, c=card):
                                            if i < len(related_manuscripts_list):
                                                related_manuscripts_list.pop(i)
                                            c.delete()

                                        ui.button(icon='close', on_click=remove_entry).props('flat round dense size=sm color=negative')

                                    notes_input = ui.input(
                                        label=tr('Notes'),
                                        placeholder=tr('Optional notes about the relationship')
                                    ).classes('w-full').props('outlined dense')

                                def update_rel_shelfmark(e, i=idx):
                                    if i < len(related_manuscripts_list):
                                        related_manuscripts_list[i]['shelfmark'] = e.value

                                def update_rel_type(e, i=idx):
                                    if i < len(related_manuscripts_list):
                                        related_manuscripts_list[i]['relationship_type'] = e.value

                                def update_rel_notes(e, i=idx):
                                    if i < len(related_manuscripts_list):
                                        related_manuscripts_list[i]['notes'] = e.value

                                shelfmark_input.on('update:model-value', update_rel_shelfmark)
                                rel_type_select.on('update:model-value', update_rel_type)
                                notes_input.on('update:model-value', update_rel_notes)

                    ui.button(tr('Add related manuscript'), icon='add', on_click=add_related_manuscript).props('outlined dense').classes('mt-2')

                # Anonymous option
                anonymous_check = ui.checkbox(tr('Post anonymously'), value=False).classes('text-sm')
                ui.label(tr('Your name will not be shown publicly')).classes('text-xs ml-8').style('color: var(--text-tertiary);')

                # Submit button
                async def submit_discovery():
                    if not title_input.value or not content_input.value:
                        ui.notify(tr('Please fill in title and description'), type='warning')
                        return

                    page_num = None
                    if selected_doc.get('selected_page'):
                        try:
                            page_num = int(selected_doc['selected_page'])
                        except:
                            pass

                    # Filter out empty entries
                    valid_additional_shelfmarks = [
                        sm for sm in additional_shelfmarks_list
                        if sm.get('shelfmark')
                    ]
                    valid_related_manuscripts = [
                        rm for rm in related_manuscripts_list
                        if rm.get('shelfmark') or rm.get('document_id')
                    ]

                    data = {
                        "discovery_type": disc_type.value,
                        "title": title_input.value,
                        "content": content_input.value,
                        "document_id": doc_id_input.value or None,
                        "page_number": page_num,
                        "shelfmark": shelfmark_hidden.value or None,
                        "is_anonymous": anonymous_check.value,
                        "additional_shelfmarks": valid_additional_shelfmarks if valid_additional_shelfmarks else None,
                        "related_manuscripts": valid_related_manuscripts if valid_related_manuscripts else None
                    }

                    result = await api_call("POST", "/discoveries/", data)

                    if "error" in result:
                        ui.notify(result.get("error", tr('Error submitting')), type='negative')
                    else:
                        ui.notify(tr('Discovery shared successfully!'), type='positive')
                        dialog.close()
                        if on_success:
                            # Handle both sync and async callbacks
                            if asyncio.iscoroutinefunction(on_success):
                                await on_success()
                            else:
                                on_success()

                with ui.row().classes('w-full justify-end gap-2 mt-4'):
                    ui.button(tr('Cancel'), on_click=dialog.close).props('flat')
                    ui.button(tr('Share'), icon='send', on_click=submit_discovery).props('color=primary')

    return dialog


def format_date(date_str: str) -> str:
    """Format ISO date string to readable format."""
    if not date_str:
        return ''
    try:
        dt = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
        return dt.strftime('%d/%m/%Y')
    except (ValueError, TypeError):
        return date_str[:10] if len(date_str) >= 10 else date_str
