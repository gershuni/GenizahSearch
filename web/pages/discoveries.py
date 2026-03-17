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
import difflib
import html
import logging
from nicegui import ui, app, run
from web.translations import tr, is_rtl
from web.auth_state import GlobalAuthState
from web.supabase_client import (
    get_client, get_feed_items, create_discovery, update_discovery, delete_discovery,
    delete_comment, delete_correction, delete_fragment_join,
    get_discovery_responses, create_discovery_response,
    vote_discovery, toggle_discovery_answered,
    toggle_discovery_pin, toggle_discovery_hidden
)
from web.state import state
from typing import Optional
from datetime import datetime
from web.components.typography import h1, h2, h3
from web.components.translate_button import detect_language, translate_text

logger = logging.getLogger(__name__)


def create_translatable_content(content: str, container_style: str = ''):
    """
    Create a text content element with a translate button.

    Args:
        content: The text content to display
        container_style: CSS styles for the text element
    """
    if not content:
        return

    # State for translation
    translation_state = {
        'is_translated': False,
        'original_content': content,
        'translated_content': None,
        'is_loading': False
    }

    with ui.column().classes('w-full gap-1'):
        # Content display
        text_label = ui.label(content).classes('text-sm whitespace-pre-wrap').style(container_style)

        # Translate button row
        with ui.row().classes('w-full items-center justify-end'):
            def toggle_translation():
                if translation_state['is_loading']:
                    return

                if translation_state['is_translated']:
                    # Show original
                    translation_state['is_translated'] = False
                    text_label.text = translation_state['original_content']
                    translate_btn.props('icon=translate')
                    translate_btn.tooltip(tr('Translate'))
                else:
                    if translation_state['translated_content']:
                        # Use cached
                        translation_state['is_translated'] = True
                        text_label.text = translation_state['translated_content']
                        translate_btn.props('icon=undo')
                        translate_btn.tooltip(tr('Show original'))
                    else:
                        # Fetch translation
                        translation_state['is_loading'] = True
                        translate_btn.props('loading')

                        src_lang = detect_language(translation_state['original_content'])
                        tgt_lang = 'en' if src_lang == 'he' else 'he'

                        translated = translate_text(translation_state['original_content'], src_lang, tgt_lang)

                        translation_state['is_loading'] = False
                        translate_btn.props(remove='loading')

                        if translated:
                            translation_state['translated_content'] = translated
                            translation_state['is_translated'] = True
                            text_label.text = translated
                            translate_btn.props('icon=undo')
                            translate_btn.tooltip(tr('Show original'))
                        else:
                            ui.notify(tr('Translation failed'), type='warning')

            translate_btn = ui.button(
                icon='translate',
                on_click=toggle_translation
            ).props('flat round dense size=xs').tooltip(tr('Translate'))


def highlight_diff(original: str, corrected: str) -> tuple[str, str]:
    """
    Return HTML strings with highlighted differences.
    Deleted words shown in red with strikethrough, inserted words in green.
    """
    if not original and not corrected:
        return '', ''
    if not original:
        return '', f'<span class="diff-inserted">{html.escape(corrected)}</span>'
    if not corrected:
        return f'<span class="diff-deleted">{html.escape(original)}</span>', ''

    orig_words = original.split()
    corr_words = corrected.split()
    matcher = difflib.SequenceMatcher(None, orig_words, corr_words)

    orig_html, corr_html = [], []
    for op, i1, i2, j1, j2 in matcher.get_opcodes():
        if op == 'equal':
            # Escape HTML in equal parts
            orig_html.extend(html.escape(w) for w in orig_words[i1:i2])
            corr_html.extend(html.escape(w) for w in corr_words[j1:j2])
        elif op == 'delete':
            escaped = html.escape(" ".join(orig_words[i1:i2]))
            orig_html.append(f'<span class="diff-deleted">{escaped}</span>')
        elif op == 'insert':
            escaped = html.escape(" ".join(corr_words[j1:j2]))
            corr_html.append(f'<span class="diff-inserted">{escaped}</span>')
        elif op == 'replace':
            escaped_orig = html.escape(" ".join(orig_words[i1:i2]))
            escaped_corr = html.escape(" ".join(corr_words[j1:j2]))
            orig_html.append(f'<span class="diff-deleted">{escaped_orig}</span>')
            corr_html.append(f'<span class="diff-inserted">{escaped_corr}</span>')

    return ' '.join(orig_html), ' '.join(corr_html)


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
        except Exception:
            pass

    return doc_id or '', ''


def _show_puzzle_join_detail_dialog(join_id: str, on_refresh=None):
    """Show a detail dialog for a published puzzle join."""
    from web.supabase_client import get_client

    with ui.dialog() as dlg, ui.card().classes('w-[500px] max-h-[80vh] p-0'):
        # Header
        with ui.row().classes('w-full items-center justify-between p-4 border-b').style(
            'background: linear-gradient(135deg, #00bcd4 0%, #0097a7 100%);'
        ):
            with ui.row().classes('items-center gap-2'):
                ui.icon('extension').classes('text-white text-xl')
                ui.label(tr('Puzzle Join')).classes('text-lg font-bold text-white')
            ui.button(icon='close', on_click=dlg.close).props('flat round size=sm text-color=white')

        detail_container = ui.column().classes('w-full p-4 gap-3')
        with detail_container:
            ui.spinner(size='lg').classes('mx-auto my-8')

        async def load_detail():
            from shared.puzzle_publish_service import get_published_join_detail
            detail = await run.io_bound(get_published_join_detail, get_client(), join_id)
            detail_container.clear()
            with detail_container:
                if not detail:
                    ui.label(tr('Could not load join details')).style('color: var(--text-secondary);')
                    return

                # Thumbnail / full-res image
                thumb_url = detail.get('thumbnail_url', '')
                image_url = detail.get('image_url', '')
                if thumb_url:
                    ui.image(thumb_url).style(
                        'max-width: 100%; max-height: 300px; object-fit: contain; '
                        'border-radius: 8px; border: 1px solid var(--border-color);'
                    )

                h3(detail.get('title', ''), classes='font-bold text-lg')

                # Author and date
                author_name = detail.get('author_name', 'Anonymous')
                created_at = detail.get('created_at', '')
                with ui.row().classes('items-center gap-2'):
                    ui.icon('person', size='xs').style('color: var(--text-tertiary);')
                    ui.label(author_name).classes('text-sm').style('color: var(--text-secondary);')
                    if created_at:
                        ui.label(format_date(created_at)).classes('text-xs').style('color: var(--text-tertiary);')

                # Notes
                notes = detail.get('notes', '')
                if notes:
                    create_translatable_content(notes, container_style='color: var(--text-primary);')

                # Shelfmarks
                shelfmarks = detail.get('shelfmarks', [])
                if shelfmarks:
                    ui.separator().classes('my-2')
                    ui.label(tr('Fragments')).classes('text-xs font-medium').style('color: var(--text-tertiary);')
                    with ui.row().classes('flex-wrap gap-2'):
                        for sm in shelfmarks:
                            ui.badge(sm).props('color=cyan outline').classes('text-xs font-mono')

                # Action buttons
                with ui.row().classes('w-full items-center gap-2 mt-3'):
                    if image_url:
                        ui.link(tr('Download Full Resolution'), image_url, new_tab=True).classes(
                            'text-sm'
                        ).style('color: var(--primary-600);')

                    async def do_fork(jid=join_id):
                        dlg.close()
                        await _fork_puzzle_join_and_navigate(jid)

                    ui.button(tr('Open in Puzzle'), icon='extension', on_click=do_fork).props('outlined dense color=cyan')

        ui.timer(0.1, load_detail, once=True)
    dlg.open()


async def _fork_puzzle_join_and_navigate(join_id: str):
    """Fork a published join and navigate to the puzzle page with it."""
    try:
        from shared.puzzle_publish_service import fork_published_join
        from shared.puzzle_service import get_puzzle_service
        from web.supabase_client import get_client

        client = get_client()
        svc = get_puzzle_service(thread_safe=True)
        new_doc_id = await run.io_bound(fork_published_join, client, join_id, svc)
        if new_doc_id:
            ui.navigate.to(f'/puzzle?doc={new_doc_id}')
        else:
            ui.notify(tr('Could not fork join'), type='warning')
    except Exception as e:
        logger.error(f"Fork puzzle join failed: {e}")
        ui.notify(tr('Fork failed: {}').format(str(e)), type='negative')


def create_discoveries_page():
    """Create the Discoveries Center page."""

    with ui.column().classes('w-full max-w-5xl mx-auto gap-6 fade-in'):

        # === Page Header ===
        with ui.row().classes('w-full items-center justify-between'):
            with ui.column().classes('gap-1'):
                # Changed to H1
                h1(tr('Discoveries Center'), classes='text-3xl font-bold', style='color: var(--text-primary);')
                ui.label(tr('Community discoveries, questions, and contributions')).style('color: var(--text-secondary);')

        # === Statistics Cards ===
        stats_row = ui.row().classes('w-full gap-3 flex-wrap')
        # Show loading spinner (replaced asynchronously below)
        with stats_row:
            with ui.column().classes('w-full items-center py-4'):
                ui.spinner('dots', size='md', color='primary')

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
                        'puzzle_join': tr('Puzzle Joins'),
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
            def open_create_dialog():
                if not GlobalAuthState.is_logged_in():
                    ui.notify(tr('Please login to create a discovery'), type='warning')
                    return

                def on_discovery_created():
                    refresh_feed()

                dialog = create_new_discovery_dialog(on_success=on_discovery_created)
                dialog.open()

            ui.button(tr('Share Discovery'), icon='add', on_click=open_create_dialog).props('color=primary')

        # === Activity Feed ===
        feed_container = ui.column().classes('w-full gap-4')

        def refresh_feed():
            """Reload the activity feed."""
            feed_container.clear()
            # Show loading spinner while fetching
            with feed_container:
                with ui.column().classes('w-full items-center py-12'):
                    ui.spinner('dots', size='lg', color='primary')
                    ui.label(tr('Loading feed...')).classes('mt-4 text-lg').style('color: var(--text-secondary);')
            # Now load the actual feed
            feed_container.clear()
            with feed_container:
                load_feed(
                    type_filter.value if type_filter.value != 'all' else None,
                    period_filter.value if period_filter.value != 'all' else None,
                    on_refresh=refresh_feed
                )

        # Bind filter changes
        def on_filter_change():
            refresh_feed()

        type_filter.on('update:model-value', on_filter_change)
        period_filter.on('update:model-value', on_filter_change)

        # Initial load - show spinner first
        with feed_container:
            with ui.column().classes('w-full items-center py-12'):
                ui.spinner('dots', size='lg', color='primary')
                ui.label(tr('Loading feed...')).classes('mt-4 text-lg').style('color: var(--text-secondary);')

        # Async initial load: fetch stats and feed off UI thread in parallel
        async def initial_load():
            def _fetch_stats():
                try:
                    client = get_client()
                    discoveries = client.table('discoveries').select('id', count='exact').execute()
                    corrections = client.table('corrections').select('id', count='exact').eq('status', 'approved').execute()
                    profiles = client.table('profiles').select('id', count='exact').execute()
                    joins = client.table('fragment_joins').select('id', count='exact').execute()
                    published_puzzles = client.table('published_joins').select('id', count='exact').eq('is_published', True).execute()
                    return {
                        'words_corrected': 0,
                        'documents_edited': corrections.count or 0,
                        'total_discoveries': discoveries.count or 0,
                        'open_questions': 0,
                        'active_contributors': profiles.count or 0,
                        'user_joins': joins.count or 0,
                        'published_puzzles': published_puzzles.count or 0,
                    }
                except Exception as e:
                    logger.error("Error loading stats: %s", e)
                    return {
                        'words_corrected': 0, 'documents_edited': 0,
                        'total_discoveries': 0, 'open_questions': 0,
                        'active_contributors': 0,
                    }

            # Capture auth state in UI context before entering thread pool
            current_user = GlobalAuthState.get_user()
            is_admin = current_user and current_user.get('role') == 'admin'

            def _fetch_feed():
                return get_feed_items(limit=50, offset=0, include_hidden=is_admin)

            # Run stats and feed fetches in parallel off the UI thread
            stats, feed_result = await asyncio.gather(
                run.io_bound(_fetch_stats),
                run.io_bound(_fetch_feed),
            )

            # Render stats
            stats_row.clear()
            with stats_row:
                _render_stat_cards(stats)

            # Render feed
            feed_container.clear()
            with feed_container:
                _render_feed_result(feed_result, on_refresh=refresh_feed)

        ui.timer(0.1, initial_load, once=True)


def _render_stat_cards(stats: dict):
    """Render stat cards from pre-fetched stats dict (pure UI, no I/O)."""
    stat_cards = [
        {'icon': 'edit', 'value': stats.get('words_corrected', 0), 'label': tr('Words Corrected'), 'color': 'blue'},
        {'icon': 'description', 'value': stats.get('documents_edited', 0), 'label': tr('Documents Edited'), 'color': 'green'},
        {'icon': 'lightbulb', 'value': stats.get('total_discoveries', 0), 'label': tr('Discoveries Shared'), 'color': 'amber'},
        {'icon': 'help_outline', 'value': stats.get('open_questions', 0), 'label': tr('Open Questions'), 'color': 'purple'},
        {'icon': 'people', 'value': stats.get('active_contributors', 0), 'label': tr('Active Contributors'), 'color': 'teal'},
        {'icon': 'link', 'value': stats.get('user_joins', 0), 'label': tr('User Joins'), 'color': 'green'},
        {'icon': 'extension', 'value': stats.get('published_puzzles', 0), 'label': tr('Published Puzzles'), 'color': 'cyan'},
    ]
    for card in stat_cards:
        with ui.card().classes('p-3 min-w-28').style('flex: 1 1 calc(14.28% - 12px);'):
            with ui.column().classes('items-center gap-1'):
                ui.icon(card['icon']).classes(f'text-2xl text-{card["color"]}-500')
                h3(str(card['value']), classes='text-xl font-bold')
                ui.label(card['label']).classes('text-xs text-center').style('color: var(--text-secondary);')


def _render_feed_result(result: dict, on_refresh=None):
    """Render feed items from pre-fetched result dict (pure UI, no I/O)."""
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
            h2(tr('No discoveries yet'), classes='text-xl mt-4', style='color: var(--text-secondary);')
            ui.label(tr('Be the first to share a discovery or ask a question!')).style('color: var(--text-tertiary);')
        return

    for item in items:
        create_feed_item(item, on_refresh=on_refresh)

    if total > len(items):
        ui.label(f"{tr('Showing')} {len(items)} {tr('of')} {total}").classes('text-sm text-center w-full mt-4').style('color: var(--text-tertiary);')


def load_stats(container):
    """Load and display statistics cards."""
    # Get stats from Supabase
    try:
        client = get_client()
        # Get basic stats
        discoveries = client.table('discoveries').select('id', count='exact').execute()
        corrections = client.table('corrections').select('id', count='exact').eq('status', 'approved').execute()
        profiles = client.table('profiles').select('id', count='exact').execute()

        stats = {
            'words_corrected': 0,  # Would need text analysis
            'documents_edited': corrections.count or 0,
            'total_discoveries': discoveries.count or 0,
            'open_questions': 0,  # Would need filtering
            'active_contributors': profiles.count or 0
        }
    except Exception as e:
        logger.error("Error loading stats: %s", e)
        stats = {
            'words_corrected': 0,
            'documents_edited': 0,
            'total_discoveries': 0,
            'open_questions': 0,
            'active_contributors': 0
        }

    with container:

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
            {
                'icon': 'extension',
                'value': stats.get('published_puzzles', 0),
                'label': tr('Published Puzzles'),
                'color': 'cyan'
            },
        ]

        for card in stat_cards:
            with ui.card().classes('p-3 min-w-28').style('flex: 1 1 calc(14.28% - 12px);'):
                with ui.column().classes('items-center gap-1'):
                    ui.icon(card['icon']).classes(f'text-2xl text-{card["color"]}-500')
                    h3(str(card['value']), classes='text-xl font-bold')
                    ui.label(card['label']).classes('text-xs text-center').style('color: var(--text-secondary);')


def load_feed(item_type: Optional[str], period: Optional[str], on_refresh=None):
    """Load and display the activity feed."""
    # Check if admin to include hidden items
    current_user = GlobalAuthState.get_user()
    is_admin = current_user and current_user.get('role') == 'admin'

    result = get_feed_items(
        item_type=item_type,
        period=period,
        limit=50,
        offset=0,
        include_hidden=is_admin
    )

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
            # Changed to H2
            h2(tr('No discoveries yet'), classes='text-xl mt-4', style='color: var(--text-secondary);')
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
        'puzzle_join': {'icon': 'extension', 'color': 'cyan', 'label': tr('Puzzle Join')},
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

                            def do_edit_discovery(nid=numeric_id, i=item):
                                open_edit_discovery_dialog(nid, i, on_refresh)

                            ui.button(icon='edit', on_click=do_edit_discovery).props('flat round dense size=sm').tooltip(tr('Edit'))

                            def do_delete_discovery(nid=numeric_id):
                                confirm_delete_discovery(nid, on_refresh)

                            ui.button(icon='delete', on_click=do_delete_discovery).props('flat round dense size=sm color=negative').tooltip(tr('Delete'))

                        # Admin pin button for discoveries
                        if is_admin and item_type in ('discovery', 'question', 'identification', 'note'):
                            numeric_id = item_id.split('_')[-1] if '_' in item_id else item_id
                            is_pinned = item.get('is_pinned', False)

                            def do_toggle_pin(nid=numeric_id, pinned=is_pinned):
                                result = toggle_discovery_pin(int(nid), not pinned)
                                if "error" not in result:
                                    ui.notify(tr('Pin toggled'), type='positive')
                                    if on_refresh:
                                        on_refresh()

                            pin_icon = 'push_pin' if is_pinned else 'push_pin'
                            pin_color = 'color=red' if is_pinned else ''
                            ui.button(icon=pin_icon, on_click=do_toggle_pin).props(f'flat round dense size=sm {pin_color}').tooltip(tr('Pin') if not is_pinned else tr('Unpin'))

                            # Admin hide/unhide button for discoveries
                            is_item_hidden = item.get('is_hidden', False)

                            def do_toggle_hide(nid=numeric_id, hidden=is_item_hidden):
                                result = toggle_discovery_hidden(int(nid), not hidden)
                                if "error" not in result:
                                    msg = tr('Item unhidden') if hidden else tr('Item hidden')
                                    ui.notify(msg, type='positive')
                                    if on_refresh:
                                        on_refresh()
                                else:
                                    ui.notify(result.get("error", tr('Error')), type='negative')

                            if is_item_hidden:
                                ui.button(icon='visibility', on_click=do_toggle_hide).props('flat round dense size=sm color=green').tooltip(tr('Unhide'))
                            else:
                                ui.button(icon='visibility_off', on_click=do_toggle_hide).props('flat round dense size=sm').tooltip(tr('Hide'))

                        # Admin delete for comments
                        if is_admin and item_type == 'comment':
                            numeric_id = item_id.split('_')[-1] if '_' in item_id else item_id

                            def do_delete_comment_admin(nid=numeric_id):
                                # Confirm dialog
                                confirm_dialog = ui.dialog()
                                with confirm_dialog, ui.card().classes('p-4'):
                                    ui.label(tr('Delete this comment?')).classes('font-bold')
                                    ui.label(tr('This action cannot be undone.')).classes('text-sm').style('color: var(--text-tertiary);')
                                    with ui.row().classes('justify-end gap-2 mt-4'):
                                        ui.button(tr('Cancel'), on_click=confirm_dialog.close).props('flat')

                                        def do_delete():
                                            result = delete_comment(int(nid))
                                            confirm_dialog.close()
                                            if "error" not in result:
                                                ui.notify(tr('Comment deleted'), type='positive')
                                                if on_refresh:
                                                    on_refresh()
                                            else:
                                                ui.notify(result.get("error", tr('Error')), type='negative')

                                        ui.button(tr('Delete'), on_click=do_delete).props('color=negative')
                                confirm_dialog.open()

                            ui.button(icon='delete', on_click=do_delete_comment_admin).props('flat round dense size=sm color=negative').tooltip(tr('Delete comment'))

                        # Admin delete for corrections
                        if is_admin and item_type == 'correction':
                            numeric_id = item_id.split('_')[-1] if '_' in item_id else item_id

                            def do_delete_correction_admin(nid=numeric_id):
                                # Confirm dialog
                                confirm_dialog = ui.dialog()
                                with confirm_dialog, ui.card().classes('p-4'):
                                    ui.label(tr('Delete this correction?')).classes('font-bold')
                                    ui.label(tr('This action cannot be undone.')).classes('text-sm').style('color: var(--text-tertiary);')
                                    with ui.row().classes('justify-end gap-2 mt-4'):
                                        ui.button(tr('Cancel'), on_click=confirm_dialog.close).props('flat')

                                        def do_delete():
                                            result = delete_correction(int(nid))
                                            confirm_dialog.close()
                                            if "error" not in result:
                                                ui.notify(tr('Correction deleted'), type='positive')
                                                if on_refresh:
                                                    on_refresh()
                                            else:
                                                ui.notify(result.get("error", tr('Error')), type='negative')

                                        ui.button(tr('Delete'), on_click=do_delete).props('color=negative')
                                confirm_dialog.open()

                            ui.button(icon='delete', on_click=do_delete_correction_admin).props('flat round dense size=sm color=negative').tooltip(tr('Delete correction'))

                # Title - for corrections and joins, generate localized title
                if item_type == 'correction':
                    corr_shelfmark, _ = resolve_shelfmark(item.get('document_id'), item.get('shelfmark'))
                    corr_page = item.get('page_number')
                    corr_title = f"{tr('Correction in')} {corr_shelfmark}"
                    if corr_page:
                        corr_title += f" ({tr('Image')} {corr_page})"
                    # Changed to H3
                    h3(corr_title, classes='font-bold text-lg')
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
                            # Changed to H3
                            h3(item.get('title', ''), classes='font-bold text-lg')

                        # Show count badges
                        if num_joins > 1:
                            ui.badge(f"{num_joins} {tr('joins')}").props('color=teal').classes('text-xs ml-2')
                        if cluster_fragments and len(cluster_fragments) > 2:
                            ui.badge(f"{len(cluster_fragments)} {tr('fragments')}").props('color=blue outline').classes('text-xs')
                elif item_type == 'puzzle_join':
                    # Puzzle join: show title with thumbnail
                    with ui.row().classes('items-center gap-3'):
                        thumb_url = item.get('thumbnail_url', '')
                        if thumb_url:
                            ui.image(thumb_url).style(
                                'width: 80px; height: 80px; object-fit: contain; border-radius: 4px; '
                                'border: 1px solid var(--border-color);'
                            )
                        with ui.column().classes('gap-1'):
                            h3(item.get('title', ''), classes='font-bold text-lg')
                            shelfmarks_list = item.get('shelfmarks', [])
                            if shelfmarks_list:
                                ui.label(' + '.join(shelfmarks_list)).classes('text-sm font-mono').style('color: var(--text-secondary);')
                else:
                    # Changed to H3
                    h3(item.get('title', ''), classes='font-bold text-lg')

                # Full content in expansion (no truncation)
                content = item.get('content_preview', '')
                related_manuscripts = item.get('related_manuscripts', []) or []

                with ui.expansion(icon='expand_more').classes('w-full').props('dense'):
                    with ui.column().classes('w-full gap-4'):

                        # For corrections: show original and corrected text side by side
                        if item_type == 'correction':
                            # Get text content
                            original_text = item.get('original_text') or ''
                            corrected_text = item.get('corrected_text') or ''

                            with ui.row().classes('w-full gap-4'):
                                with ui.column().classes('flex-1'):
                                    ui.label(tr('Original (V0.8)')).classes('font-medium text-xs text-gray-500')
                                    if original_text:
                                        with ui.element('div').classes('p-3 rounded text-sm').style(
                                            'background: #fef2f2; border-left: 4px solid #ef5350; '
                                            'direction: rtl; text-align: right; white-space: pre-wrap;'
                                        ):
                                            ui.label(original_text).style('color: #1f2937; line-height: 1.8;')
                                    else:
                                        ui.label(tr('(no original text)')).classes('text-sm p-2 italic text-gray-400')
                                with ui.column().classes('flex-1'):
                                    ui.label(tr('Corrected')).classes('font-medium text-xs text-gray-500')
                                    if corrected_text:
                                        with ui.element('div').classes('p-3 rounded text-sm').style(
                                            'background: #f0fdf4; border-left: 4px solid #66bb6a; '
                                            'direction: rtl; text-align: right; white-space: pre-wrap;'
                                        ):
                                            ui.label(corrected_text).style('color: #1f2937; line-height: 1.8;')
                                    else:
                                        ui.label(tr('(no corrected text)')).classes('text-sm p-2 italic text-gray-400')
                        elif item_type == 'join':
                            # For joins: show cluster details with individual joins
                            cluster_fragments = item.get('cluster_fragments', [])
                            cluster_joins = item.get('cluster_joins', [])

                            join_rel_labels = {
                                'physical_join': tr('Physical join'),
                                'physical': tr('Physical join'),
                                'same_composition': tr('Same composition'),
                                'content': tr('Same composition'),
                                'uncertain': tr('Unknown'),
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

                                        with ui.card().classes('p-2 cursor-pointer hover:shadow-sm').style('background: var(--surface-secondary);').on('click', nav_to_frag if frag_doc_id else None):
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
                                                def do_delete_single_join(jid=cj_id):
                                                    confirm_dialog = ui.dialog()
                                                    with confirm_dialog, ui.card().classes('p-4'):
                                                        # Changed to H3
                                                        h3(tr('Delete this join?'), classes='font-bold')
                                                        ui.label(tr('This action cannot be undone.')).classes('text-sm').style('color: var(--text-tertiary);')
                                                        with ui.row().classes('justify-end gap-2 mt-4'):
                                                            ui.button(tr('Cancel'), on_click=confirm_dialog.close).props('flat')

                                                            def do_delete():
                                                                result = delete_fragment_join(int(jid))
                                                                confirm_dialog.close()
                                                                if "error" not in result:
                                                                    ui.notify(tr('Join deleted'), type='positive')
                                                                    if on_refresh:
                                                                        on_refresh()
                                                                else:
                                                                    ui.notify(result.get("error", tr('Error')), type='negative')

                                                            ui.button(tr('Delete'), on_click=do_delete).props('color=negative')
                                                    confirm_dialog.open()

                                                ui.button(icon='delete', on_click=do_delete_single_join).props('flat round dense size=xs color=negative').tooltip(tr('Delete join'))

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
                        elif item_type == 'puzzle_join':
                            # Puzzle join detail: notes, fragment list, action buttons
                            pj_notes = item.get('content_preview', '')
                            if pj_notes:
                                create_translatable_content(
                                    pj_notes,
                                    container_style='color: var(--text-primary);'
                                )

                            pj_shelfmarks = item.get('shelfmarks', [])
                            if pj_shelfmarks:
                                ui.separator().classes('my-2')
                                ui.label(tr('Fragments')).classes('text-xs font-medium').style('color: var(--text-tertiary);')
                                with ui.row().classes('flex-wrap gap-2'):
                                    for sm in pj_shelfmarks:
                                        ui.badge(sm).props('color=cyan outline').classes('text-xs font-mono')

                            # Action buttons: View Details / Open in Puzzle
                            with ui.row().classes('w-full items-center gap-2 mt-3'):
                                pj_raw_id = item_id.replace('puzzle_join_', '') if item_id.startswith('puzzle_join_') else item_id

                                def open_puzzle_join_detail(jid=pj_raw_id):
                                    """Open detail dialog for this puzzle join."""
                                    _show_puzzle_join_detail_dialog(jid, on_refresh)

                                ui.button(tr('View Details'), icon='visibility', on_click=open_puzzle_join_detail).props('outlined dense')

                                async def fork_and_open(jid=pj_raw_id):
                                    """Fork the join and open in puzzle page."""
                                    await _fork_puzzle_join_and_navigate(jid)

                                ui.button(tr('Open in Puzzle'), icon='extension', on_click=fork_and_open).props('outlined dense color=cyan')
                        else:
                            # Full content for non-corrections with translate button
                            create_translatable_content(
                                content,
                                container_style='color: var(--text-primary); direction: rtl;'
                            )

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
                                    def do_vote_up(nid=numeric_id):
                                        if not GlobalAuthState.is_logged_in():
                                            ui.notify(tr('Login to vote'), type='warning')
                                            return
                                        user_id = GlobalAuthState.get_user_id()
                                        result = vote_discovery(int(nid), user_id, 'up')
                                        if "error" not in result:
                                            ui.notify(tr('Vote recorded'), type='positive')
                                            if on_refresh:
                                                on_refresh()

                                    ui.button(icon='thumb_up', on_click=do_vote_up).props('flat dense size=sm').tooltip(tr('Upvote'))
                                    ui.label(str(upvotes)).classes('text-sm font-medium')

                                with ui.row().classes('items-center gap-1'):
                                    def do_vote_down(nid=numeric_id):
                                        if not GlobalAuthState.is_logged_in():
                                            ui.notify(tr('Login to vote'), type='warning')
                                            return
                                        user_id = GlobalAuthState.get_user_id()
                                        result = vote_discovery(int(nid), user_id, 'down')
                                        if "error" not in result:
                                            ui.notify(tr('Vote recorded'), type='positive')
                                            if on_refresh:
                                                on_refresh()

                                    ui.button(icon='thumb_down', on_click=do_vote_down).props('flat dense size=sm').tooltip(tr('Downvote'))
                                    ui.label(str(downvotes)).classes('text-sm font-medium')

                                # Mark as answered button (for questions, author or admin only)
                                if item_type == 'question' and (is_author or is_admin):
                                    is_answered = item.get('is_answered', False)

                                    def do_toggle_answered(nid=numeric_id, answered=is_answered):
                                        result = toggle_discovery_answered(int(nid), not answered)
                                        if "error" not in result:
                                            ui.notify(tr('Status updated'), type='positive')
                                            if on_refresh:
                                                on_refresh()

                                    if is_answered:
                                        ui.button(tr('Mark as unanswered'), icon='help_outline', on_click=do_toggle_answered).props('flat dense size=sm')
                                    else:
                                        ui.button(tr('Mark as answered'), icon='check_circle', on_click=do_toggle_answered).props('flat dense size=sm color=green')

                            # Responses section
                            ui.separator().classes('my-2')
                            responses_container = ui.column().classes('w-full gap-2')

                            def do_load_responses(container=responses_container, nid=numeric_id):
                                try:
                                    if container.client.has_been_deleted: return
                                    container.clear()
                                    with container:
                                        responses = get_discovery_responses(int(nid))
                                        if container.client.has_been_deleted: return
                                        if responses:
                                            ui.label(f"{tr('Responses')} ({len(responses)})").classes('font-medium text-sm')
                                            for resp in responses:
                                                # Format response for display
                                                profile = resp.get('profiles', {}) or {}
                                                formatted_resp = {
                                                    'content': resp.get('content', ''),
                                                    'created_at': resp.get('created_at', ''),
                                                    'author': {
                                                        'full_name': profile.get('full_name'),
                                                        'username': profile.get('username'),
                                                        'is_anonymous': resp.get('is_anonymous', False)
                                                    }
                                                }
                                                create_response_item(formatted_resp)
                                        else:
                                            ui.label(tr('No responses yet')).classes('text-sm').style('color: var(--text-tertiary);')
                                except Exception:
                                    pass

                                    # Reply form
                                    if GlobalAuthState.is_logged_in():
                                        ui.separator().classes('my-2')
                                        reply_input = ui.textarea(placeholder=tr('Write a reply...')).classes('w-full').props('outlined dense rows=2').style('direction: rtl;')
                                        anonymous_reply = ui.checkbox(tr('Reply anonymously'), value=False).classes('text-xs')

                                        def do_submit_reply(inp=reply_input, anon=anonymous_reply, nid=nid):
                                            if not inp.value or not inp.value.strip():
                                                ui.notify(tr('Please enter a reply'), type='warning')
                                                return
                                            user_id = GlobalAuthState.get_user_id()
                                            result = create_discovery_response(
                                                discovery_id=int(nid),
                                                user_id=user_id,
                                                content=inp.value.strip(),
                                                is_anonymous=anon.value
                                            )
                                            if "error" in result:
                                                ui.notify(result["error"], type='negative')
                                            else:
                                                ui.notify(tr('Reply posted'), type='positive')
                                                inp.value = ''
                                                do_load_responses()

                                        ui.button(tr('Reply'), on_click=do_submit_reply).props('dense color=primary').classes('self-end')
                                    else:
                                        ui.label(tr('Login to reply')).classes('text-xs').style('color: var(--text-tertiary);')

                            # Load responses when expansion opens
                            ui.timer(0.1, do_load_responses, once=True)

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
        # Response content with translate button
        create_translatable_content(
            resp.get('content', ''),
            container_style='direction: rtl; color: var(--text-primary);'
        )


def open_edit_discovery_dialog(discovery_id: str, item: dict, on_refresh=None):
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
        # Changed to H3
        h3(tr('Edit Discovery'), classes='text-xl font-bold mb-4')

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
                                    except Exception:
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
                                ui.button(icon='arrow_forward' if is_rtl() else 'arrow_back', on_click=back_callback).props('flat round dense')
                            ui.label(title_text).classes('font-bold flex-grow')
                            ui.button(icon='close', on_click=doc_picker_dialog.close).props('flat round dense')

                        if not items:
                            ui.label(tr('No items found')).classes('p-4').style('color: var(--text-tertiary);')
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
                                        except Exception:
                                            shelfmark = doc_id

                                    def make_pick(did=doc_id, sm=shelfmark, ti=title, pg=page):
                                        def pick():
                                            select_document(did, sm, ti, pg)
                                            doc_picker_dialog.close()
                                        return pick

                                    with ui.card().classes('w-full p-2 mb-2 cursor-pointer hover:shadow-sm').on('click', make_pick()):
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
                                except Exception:
                                    count = 0

                                def make_list_click(lid=list_id, lname=list_name):
                                    def click():
                                        items = state.lists_mgr.get_items_in_list_sync(lid)
                                        show_document_items(items, f"{tr('Items in')}: {lname}", back_callback=show_lists_view)
                                    return click

                                with ui.card().classes('w-full p-3 mb-2 cursor-pointer hover:shadow-sm').on('click', make_list_click()):
                                    with ui.row().classes('items-center gap-2'):
                                        ui.icon('circle').style(f'color: {color}; font-size: 1rem;')
                                        ui.label(list_name).classes('font-medium flex-grow')
                                        ui.badge(str(count)).style('background: var(--bg-tertiary); color: var(--text-secondary);')

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
                                    except Exception:
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

            def save_changes():
                data = {
                    "title": title_input.value,
                    "content": content_input.value,
                    "document_id": selected_doc['sys_id'],
                    "shelfmark": selected_doc['shelfmark'],
                    "page_number": selected_doc['page_number']
                }
                result = update_discovery(int(discovery_id), data)
                if "error" in result:
                    ui.notify(result["error"], type='negative')
                else:
                    ui.notify(tr('Discovery updated'), type='positive')
                    dialog.close()
                    if on_refresh:
                        on_refresh()

            ui.button(tr('Save'), icon='save', on_click=save_changes).props('color=primary')

    dialog.open()


def confirm_delete_discovery(discovery_id: str, on_refresh=None):
    """Confirm and delete a discovery."""
    dialog = ui.dialog()

    with dialog, ui.card().classes('p-4'):
        # Changed to H3
        h3(tr('Delete Discovery?'), classes='text-lg font-bold')
        ui.label(tr('This action cannot be undone.')).classes('text-sm').style('color: var(--text-tertiary);')

        with ui.row().classes('justify-end gap-2 mt-4'):
            ui.button(tr('Cancel'), on_click=dialog.close).props('flat')

            def do_delete():
                result = delete_discovery(int(discovery_id))
                dialog.close()
                if "error" in result:
                    ui.notify(result["error"], type='negative')
                else:
                    ui.notify(tr('Discovery deleted'), type='positive')
                    if on_refresh:
                        on_refresh()

            ui.button(tr('Delete'), on_click=do_delete).props('color=negative')

    dialog.open()


def create_new_discovery_dialog(on_success=None):
    """Create dialog for posting a new discovery/question."""
    from web.state import state

    dialog = ui.dialog()  # No 'persistent' to allow Esc to close

    # State for document selection
    selected_doc = {'sys_id': None, 'shelfmark': None, 'title': None, 'total_pages': 0}
    # State for additional shelfmarks
    additional_shelfmarks_list = []

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
                # Changed to H3
                h3(tr('Share a Discovery'), classes='text-xl font-bold')
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
                                        ui.button(icon='arrow_forward' if is_rtl() else 'arrow_back', on_click=back_callback).props('flat round dense')
                                    ui.label(title_text).classes('font-bold flex-grow')
                                    ui.button(icon='close', on_click=doc_picker_dialog.close).props('flat round dense')

                                if not items:
                                    ui.label(tr('No items found')).classes('p-4').style('color: var(--text-tertiary);')
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
                                                except Exception:
                                                    shelfmark = doc_id

                                            def make_pick(did=doc_id, sm=shelfmark, ti=title, pg=page):
                                                def pick():
                                                    select_document(did, sm, ti, pg)
                                                    doc_picker_dialog.close()
                                                return pick

                                            with ui.card().classes('w-full p-2 mb-2 cursor-pointer hover:shadow-sm').on('click', make_pick()):
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
                                        except Exception:
                                            count = 0

                                        def make_list_click(lid=list_id, lname=list_name):
                                            def click():
                                                items = state.lists_mgr.get_items_in_list_sync(lid)
                                                show_document_items(items, f"{tr('Items in')}: {lname}", back_callback=show_lists_view)
                                            return click

                                        with ui.card().classes('w-full p-3 mb-2 cursor-pointer hover:shadow-sm').on('click', make_list_click()):
                                            with ui.row().classes('items-center gap-2'):
                                                ui.icon('circle').style(f'color: {color}; font-size: 1rem;')
                                                ui.label(list_name).classes('font-medium flex-grow')
                                                ui.badge(str(count)).style('background: var(--bg-tertiary); color: var(--text-secondary);')

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

                # Anonymous option
                anonymous_check = ui.checkbox(tr('Post anonymously'), value=False).classes('text-sm')
                ui.label(tr('Your name will not be shown publicly')).classes('text-xs ml-8').style('color: var(--text-tertiary);')

                # Submit button
                def submit_discovery():
                    if not title_input.value or not content_input.value:
                        ui.notify(tr('Please fill in title and description'), type='warning')
                        return

                    user_id = GlobalAuthState.get_user_id()
                    if not user_id:
                        ui.notify(tr('Please login to share discoveries'), type='warning')
                        return

                    page_num = None
                    if selected_doc.get('selected_page'):
                        try:
                            page_num = int(selected_doc['selected_page'])
                        except Exception:
                            pass

                    # Filter out empty entries
                    valid_additional_shelfmarks = [
                        sm for sm in additional_shelfmarks_list
                        if sm.get('shelfmark')
                    ]

                    result = create_discovery(
                        user_id=user_id,
                        title=title_input.value,
                        content=content_input.value,
                        type=disc_type.value,
                        document_id=doc_id_input.value or None,
                        shelfmark=shelfmark_hidden.value or None,
                        page_number=page_num,
                        is_anonymous=anonymous_check.value,
                        additional_shelfmarks=valid_additional_shelfmarks if valid_additional_shelfmarks else None
                    )

                    if "error" in result:
                        ui.notify(result.get("error", tr('Error submitting')), type='negative')
                    else:
                        ui.notify(tr('Discovery shared successfully!'), type='positive')
                        dialog.close()
                        if on_success:
                            on_success()

                with ui.row().classes('w-full justify-end gap-2 mt-4'):
                    ui.button(tr('Cancel'), on_click=dialog.close).props('flat')
                    ui.button(tr('Share'), on_click=submit_discovery).props('color=primary')

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
