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
                    period_filter.value if period_filter.value != 'all' else None
                )

        # Bind filter changes
        type_filter.on('update:model-value', lambda: refresh_feed())
        period_filter.on('update:model-value', lambda: refresh_feed())

        # Initial load
        with feed_container:
            await load_feed(None, None)


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


async def load_feed(item_type: Optional[str], period: Optional[str]):
    """Load and display the activity feed."""
    params = {"limit": 20, "offset": 0}
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

    # Feed items
    for item in items:
        create_feed_item(item)

    # Show total count
    if total > len(items):
        ui.label(f"{tr('Showing')} {len(items)} {tr('of')} {total}").classes('text-sm text-center w-full mt-4').style('color: var(--text-tertiary);')


def create_feed_item(item: dict):
    """Create a single feed item card."""
    item_type = item.get('item_type', 'discovery')
    author = item.get('author', {})

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

    with ui.card().classes('w-full p-4 hover:shadow-md transition-shadow'):
        with ui.row().classes('w-full items-start gap-4'):
            # Type icon
            with ui.column().classes('items-center'):
                ui.icon(style['icon']).classes(f'text-2xl text-{style["color"]}-500')
                if item.get('is_featured'):
                    ui.icon('star').classes('text-sm text-amber-400')

            # Content
            with ui.column().classes('flex-1 gap-2'):
                # Header row
                with ui.row().classes('w-full items-center justify-between'):
                    with ui.row().classes('items-center gap-2'):
                        ui.badge(style['label']).props(f'color={style["color"]}').classes('text-xs')
                        if item.get('shelfmark'):
                            ui.label(item['shelfmark']).classes('text-sm font-mono').style('color: var(--primary-600);')

                    # Date
                    created_at = item.get('created_at', '')
                    if created_at:
                        date_str = format_date(created_at)
                        ui.label(date_str).classes('text-xs').style('color: var(--text-tertiary);')

                # Title
                ui.label(item.get('title', '')).classes('font-bold text-lg')

                # Content preview
                content = item.get('content_preview', '')
                if content:
                    ui.label(content).classes('text-sm').style('color: var(--text-secondary); direction: rtl;')

                # Footer
                with ui.row().classes('w-full items-center justify-between mt-2'):
                    # Author - check if anonymous first
                    if author.get('is_anonymous', False):
                        author_name = tr('Anonymous')
                    else:
                        author_name = author.get('full_name') or author.get('username') or tr('Anonymous')
                    affiliation = author.get('affiliation', '')
                    author_text = f"{author_name}" + (f" ({affiliation})" if affiliation else "")
                    ui.label(author_text).classes('text-xs').style('color: var(--text-tertiary);')

                    # Stats
                    with ui.row().classes('items-center gap-3'):
                        response_count = item.get('response_count', 0)
                        if response_count > 0:
                            with ui.row().classes('items-center gap-1'):
                                ui.icon('chat_bubble_outline', size='xs').style('color: var(--text-tertiary);')
                                ui.label(str(response_count)).classes('text-xs').style('color: var(--text-tertiary);')

                        # View details button
                        def open_details(i=item):
                            show_discovery_details(i)
                        ui.button(tr('View'), icon='open_in_new', on_click=open_details).props('flat dense size=sm').classes('text-xs')


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


def show_discovery_details(item: dict):
    """Show full discovery details in a dialog."""
    dialog = ui.dialog().props('maximized')

    with dialog:
        with ui.card().classes('w-full h-full p-6'):
            # Header
            with ui.row().classes('w-full items-center justify-between mb-4'):
                with ui.row().classes('items-center gap-2'):
                    item_type = item.get('item_type', 'discovery')
                    type_icons = {
                        'discovery': 'lightbulb',
                        'question': 'help_outline',
                        'correction': 'edit',
                    }
                    ui.icon(type_icons.get(item_type, 'article')).classes('text-2xl')
                    ui.label(item.get('title', '')).classes('text-2xl font-bold')

                ui.button(icon='close', on_click=dialog.close).props('flat round')

            # Metadata
            with ui.row().classes('w-full items-center gap-4 mb-4'):
                if item.get('shelfmark'):
                    with ui.row().classes('items-center gap-1'):
                        ui.icon('description', size='sm')
                        ui.label(item['shelfmark']).classes('font-mono')
                author = item.get('author', {})
                if author.get('is_anonymous', False):
                    author_name = tr('Anonymous')
                else:
                    author_name = author.get('full_name') or author.get('username') or tr('Anonymous')
                ui.label(f"{tr('by')} {author_name}").style('color: var(--text-secondary);')
                ui.label(format_date(item.get('created_at', ''))).style('color: var(--text-tertiary);')

            # Content
            ui.separator()
            with ui.scroll_area().classes('w-full flex-1 my-4'):
                ui.label(item.get('content_preview', '')).classes('text-lg whitespace-pre-wrap').style('direction: rtl;')

            # Link to document if available
            if item.get('document_id'):
                ui.separator()
                with ui.row().classes('items-center gap-2 mt-4'):
                    ui.icon('link')
                    ui.link(
                        f"{tr('View document')}: {item.get('shelfmark') or item['document_id']}",
                        target=f"/browse?doc={item['document_id']}"
                    ).classes('text-primary-600')

    dialog.open()


def format_date(date_str: str) -> str:
    """Format ISO date string to readable format."""
    if not date_str:
        return ''
    try:
        dt = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
        return dt.strftime('%d/%m/%Y')
    except (ValueError, TypeError):
        return date_str[:10] if len(date_str) >= 10 else date_str
