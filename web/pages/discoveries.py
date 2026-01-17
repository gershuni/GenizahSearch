# -*- coding: utf-8 -*-
"""
Discoveries Center - Community Discoveries and Questions

Shows:
- Statistics (words corrected, documents edited, discoveries, open questions)
- Activity feed combining discoveries, questions, and corrections
- Create new discovery/question form
- NO leaderboard (researchers prefer anonymity over competition)
"""

from nicegui import ui, app
from web.translations import tr
from web.auth_state import GlobalAuthState, api_call
from typing import Optional
from datetime import datetime


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
                dialog = create_new_discovery_dialog(on_success=lambda: refresh_feed())
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

        # Bind filter changes
        type_filter.on('update:model-value', lambda: refresh_feed())
        period_filter.on('update:model-value', lambda: refresh_feed())

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
    }
    style = type_styles.get(item_type, type_styles['discovery'])

    # Check if current user is the author or admin
    current_user = GlobalAuthState.get_user()
    is_author = current_user and author.get('id') == current_user.get('id')
    is_admin = current_user and current_user.get('role') == 'admin'

    with ui.card().classes('w-full p-4 hover:shadow-md transition-shadow'):
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

                        if shelfmark or doc_id:
                            link_text = shelfmark or doc_id
                            if page_num:
                                link_text += f" • {tr('Image')} {page_num}"

                            def go_to_doc(did=doc_id, pnum=page_num):
                                if did:
                                    url = f'/browse?id={did}'
                                    if pnum:
                                        url += f'&page={pnum}'
                                    ui.navigate.to(url)

                            with ui.element('a').classes('cursor-pointer hover:underline text-sm font-mono').style('color: var(--primary-600);').on('click', go_to_doc):
                                ui.label(link_text)

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

                        # Admin pin button
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

                # Title
                ui.label(item.get('title', '')).classes('font-bold text-lg')

                # Full content in expansion (no truncation)
                content = item.get('content_preview', '')
                with ui.expansion(icon='expand_more').classes('w-full').props('dense'):
                    with ui.column().classes('w-full gap-4'):
                        # For corrections: show original and corrected text side by side
                        if item_type == 'correction':
                            original_text = item.get('original_text', '')
                            corrected_text = item.get('corrected_text', '')
                            if original_text or corrected_text:
                                with ui.row().classes('w-full gap-4'):
                                    if original_text:
                                        with ui.column().classes('flex-1'):
                                            ui.label(tr('Original')).classes('font-medium text-xs').style('color: var(--text-tertiary);')
                                            ui.label(original_text).classes('text-sm whitespace-pre-wrap p-2 rounded').style(
                                                'background: var(--surface-secondary); direction: rtl; text-align: right;'
                                            )
                                    if corrected_text:
                                        with ui.column().classes('flex-1'):
                                            ui.label(tr('Corrected')).classes('font-medium text-xs').style('color: var(--text-tertiary);')
                                            ui.label(corrected_text).classes('text-sm whitespace-pre-wrap p-2 rounded').style(
                                                'background: var(--surface-secondary); direction: rtl; text-align: right;'
                                            )
                        else:
                            # Full content for non-corrections
                            ui.label(content).classes('text-sm whitespace-pre-wrap').style('color: var(--text-secondary); direction: rtl;')

                        # Document link
                        if item.get('document_id'):
                            with ui.row().classes('items-center gap-2'):
                                ui.icon('link', size='sm')
                                shelfmark = item.get('shelfmark') or item.get('document_id', '')
                                page_num = item.get('page_number')
                                link_text = f"{tr('View document')}: {shelfmark}"
                                if page_num:
                                    link_text += f" ({tr('Image')} {page_num})"

                                def go_to_doc2(did=item.get('document_id'), pnum=page_num):
                                    url = f'/browse?id={did}'
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
    dialog = ui.dialog()

    with dialog, ui.card().classes('w-full max-w-lg p-6'):
        ui.label(tr('Edit Discovery')).classes('text-xl font-bold mb-4')

        title_input = ui.input(label=tr('Title'), value=item.get('title', '')).classes('w-full').props('outlined')
        content_input = ui.textarea(label=tr('Description'), value=item.get('content_preview', '')).classes('w-full').props('outlined rows=5').style('direction: rtl;')
        shelfmark_input = ui.input(label=tr('Shelfmark'), value=item.get('shelfmark', '')).classes('w-full').props('outlined')

        with ui.row().classes('w-full justify-end gap-2 mt-4'):
            ui.button(tr('Cancel'), on_click=dialog.close).props('flat')

            async def save_changes():
                result = await api_call("PUT", f"/discoveries/{discovery_id}", {
                    "title": title_input.value,
                    "content": content_input.value,
                    "shelfmark": shelfmark_input.value or None
                })
                if "error" in result:
                    ui.notify(result["error"], type='negative')
                else:
                    ui.notify(tr('Discovery updated'), type='positive')
                    dialog.close()
                    if on_refresh:
                        await on_refresh()

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
    dialog = ui.dialog().props('persistent')

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
                with ui.expansion(tr('Link to document (optional)'), icon='link').classes('w-full'):
                    with ui.row().classes('w-full gap-2'):
                        doc_id_input = ui.input(label=tr('Document ID')).classes('flex-1').props('outlined dense')
                        page_input = ui.number(label=tr('Page'), min=1).classes('w-24').props('outlined dense')
                    shelfmark_input = ui.input(
                        label=tr('Shelfmark'),
                        placeholder='e.g. T-S 8J6.1'
                    ).classes('w-full').props('outlined dense')

                # Anonymous option
                anonymous_check = ui.checkbox(tr('Post anonymously'), value=False).classes('text-sm')
                ui.label(tr('Your name will not be shown publicly')).classes('text-xs ml-8').style('color: var(--text-tertiary);')

                # Submit button
                async def submit_discovery():
                    if not title_input.value or not content_input.value:
                        ui.notify(tr('Please fill in title and description'), type='warning')
                        return

                    data = {
                        "discovery_type": disc_type.value,
                        "title": title_input.value,
                        "content": content_input.value,
                        "document_id": doc_id_input.value or None,
                        "page_number": int(page_input.value) if page_input.value else None,
                        "shelfmark": shelfmark_input.value or None,
                        "is_anonymous": anonymous_check.value
                    }

                    result = await api_call("POST", "/discoveries/", data)

                    if "error" in result:
                        ui.notify(result.get("error", tr('Error submitting')), type='negative')
                    else:
                        ui.notify(tr('Discovery shared successfully!'), type='positive')
                        dialog.close()
                        if on_success:
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
