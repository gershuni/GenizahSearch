# -*- coding: utf-8 -*-
"""
Notes Display Component

Displays existing comments/notes for a document or page.
Supports:
- Public and private notes
- Nested replies
- Reactions
- Shelfmark mentions in format [[shelfmark:T-S 8J6.1|id:123456]]
"""

import re
import asyncio
from nicegui import ui
from web.translations import tr
from web.auth_state import GlobalAuthState
from web.supabase_client import get_comments
from web.components.translate_button import create_translatable_text, detect_language, translate_text
from typing import Optional, List


# Pattern to match shelfmark mentions: [[shelfmark:xxx|id:yyy]]
SHELFMARK_MENTION_PATTERN = re.compile(r'\[\[shelfmark:([^\]|]+)\|id:([^\]]+)\]\]')


def render_content_with_mentions(content: str, container_classes: str = '', container_style: str = '', show_translate: bool = False):
    """
    Render comment content with shelfmark mentions as clickable links.

    Mentions are in format: [[shelfmark:T-S 8J6.1|id:123456]]
    They will be rendered as clickable links that navigate to the document.

    Args:
        content: The comment text content
        container_classes: CSS classes for the container
        container_style: Inline styles for the container
        show_translate: Whether to show the translate button

    Returns:
        The text label element (for translation updates) or None
    """
    if not content:
        return None

    # State for translation
    translation_state = {
        'is_translated': False,
        'original_content': content,
        'translated_content': None,
        'is_loading': False
    }

    # Check if there are any mentions in the content
    has_mentions = bool(SHELFMARK_MENTION_PATTERN.search(content))

    # Main content container
    with ui.column().classes('w-full gap-1'):
        # Content row
        content_container = ui.element('div').classes(f'w-full {container_classes}')
        text_element = None

        with content_container:
            if not has_mentions:
                # No mentions - just display the text directly
                text_element = ui.label(content).classes('text-sm whitespace-pre-wrap').style(container_style)
            else:
                # Has mentions - create row with links
                with ui.row().classes('flex-wrap items-baseline gap-0').style(container_style):
                    parts = SHELFMARK_MENTION_PATTERN.split(content)
                    i = 0
                    while i < len(parts):
                        if i % 3 == 0:
                            text = parts[i]
                            if text:
                                ui.label(text).classes('text-sm whitespace-pre-wrap')
                        elif i % 3 == 1:
                            shelfmark = parts[i]
                            doc_id = parts[i + 1] if i + 1 < len(parts) else ''
                            ui.link(
                                shelfmark,
                                target=f'/browse?sys_id={doc_id}'
                            ).classes('text-primary font-medium hover:underline text-sm').style('cursor: pointer;')
                            i += 1
                        i += 1

        # Translate button row
        if show_translate:
            with ui.row().classes('w-full items-center justify-end'):
                def toggle_translation():
                    if translation_state['is_loading']:
                        return

                    if translation_state['is_translated']:
                        # Show original
                        translation_state['is_translated'] = False
                        content_container.clear()
                        with content_container:
                            if not has_mentions:
                                ui.label(translation_state['original_content']).classes('text-sm whitespace-pre-wrap').style(container_style)
                            else:
                                with ui.row().classes('flex-wrap items-baseline gap-0').style(container_style):
                                    parts = SHELFMARK_MENTION_PATTERN.split(translation_state['original_content'])
                                    i = 0
                                    while i < len(parts):
                                        if i % 3 == 0:
                                            text = parts[i]
                                            if text:
                                                ui.label(text).classes('text-sm whitespace-pre-wrap')
                                        elif i % 3 == 1:
                                            shelfmark = parts[i]
                                            doc_id = parts[i + 1] if i + 1 < len(parts) else ''
                                            ui.link(
                                                shelfmark,
                                                target=f'/browse?sys_id={doc_id}'
                                            ).classes('text-primary font-medium hover:underline text-sm').style('cursor: pointer;')
                                            i += 1
                                        i += 1
                        translate_btn.props('icon=translate')
                        translate_btn.tooltip(tr('Translate'))
                    else:
                        # Translate
                        if translation_state['translated_content']:
                            # Use cached
                            translation_state['is_translated'] = True
                            content_container.clear()
                            with content_container:
                                # Strip mentions for translated content (they don't translate well)
                                ui.label(translation_state['translated_content']).classes('text-sm whitespace-pre-wrap').style(container_style)
                            translate_btn.props('icon=undo')
                            translate_btn.tooltip(tr('Show original'))
                        else:
                            # Fetch translation
                            translation_state['is_loading'] = True
                            translate_btn.props('loading')

                            # Get plain text (without mention markup)
                            plain_text = SHELFMARK_MENTION_PATTERN.sub(r'\1', translation_state['original_content'])

                            src_lang = detect_language(plain_text)
                            tgt_lang = 'en' if src_lang == 'he' else 'he'

                            translated = translate_text(plain_text, src_lang, tgt_lang)

                            translation_state['is_loading'] = False
                            translate_btn.props(remove='loading')

                            if translated:
                                translation_state['translated_content'] = translated
                                translation_state['is_translated'] = True
                                content_container.clear()
                                with content_container:
                                    ui.label(translated).classes('text-sm whitespace-pre-wrap').style(container_style)
                                translate_btn.props('icon=undo')
                                translate_btn.tooltip(tr('Show original'))
                            else:
                                ui.notify(tr('Translation failed'), type='warning')

                translate_btn = ui.button(
                    icon='translate',
                    on_click=toggle_translation
                ).props('flat round dense size=xs').tooltip(tr('Translate'))

    return text_element


def fetch_document_comments(document_id: str, page_number: int = None) -> List[dict]:
    """
    Fetch comments for a document.

    Args:
        document_id: System ID of the document
        page_number: Optional page number to filter

    Returns:
        List of comment objects
    """
    try:
        # Get public comments for this document
        comments = get_comments(sys_id=document_id, is_public=True)

        # Also get user's private comments if logged in
        user_id = GlobalAuthState.get_user_id()
        if user_id:
            private_comments = get_comments(sys_id=document_id, author_id=user_id, is_public=False)
            comments.extend(private_comments)

        # Sort by created_at
        comments.sort(key=lambda x: x.get('created_at', ''), reverse=True)

        # Transform to expected format
        formatted_comments = []
        for c in comments:
            profile = c.get('profiles', {}) or {}
            formatted_comments.append({
                'id': c.get('id'),
                'content': c.get('content', ''),
                'page_number': c.get('page_number'),
                'is_public': c.get('is_public', True),
                'created_at': c.get('created_at', ''),
                'author': {
                    'username': profile.get('username'),
                    'full_name': profile.get('full_name')
                },
                'replies': []  # TODO: Implement nested replies if needed
            })

        # Filter by page_number if specified
        # Include comments without page_number (document-level comments)
        if page_number is not None:
            formatted_comments = [c for c in formatted_comments
                                  if c.get('page_number') == page_number or c.get('page_number') is None]

        return formatted_comments
    except Exception as e:
        print(f"Error fetching comments: {e}")
        return []


def create_notes_panel(
    document_id: str,
    page_number: Optional[int] = None,
    shelfmark: str = ""
):
    """
    Create a panel showing existing notes for a document/page.

    Args:
        document_id: System ID of the document
        page_number: Optional page number to filter
        shelfmark: Display name for the document

    Returns:
        Tuple of (panel element, refresh function)
        The refresh function can be called to reload notes and expand the panel
    """
    panel = ui.expansion(
        text=tr('Notes & Comments'),
        icon='comment'
    ).classes('w-full').props('dense')

    with panel:
        notes_container = ui.column().classes('w-full gap-2 p-2')

        def load_notes():
            """Load and display notes."""
            notes_container.clear()

            with notes_container:
                comments = fetch_document_comments(document_id, page_number)

                if not comments:
                    with ui.row().classes('w-full justify-center p-4'):
                        ui.label(tr('No comments yet')).classes('text-sm').style('color: var(--text-muted);')
                else:
                    for comment in comments:
                        create_comment_card(comment)

        def refresh_and_expand():
            """Refresh notes and expand the panel to show new content."""
            panel.value = True  # Expand the panel
            load_notes()

        # Load on expansion
        def on_expand(e):
            if e.args:
                load_notes()

        panel.on('update:model-value', on_expand)

    return panel, refresh_and_expand


def create_comment_card(comment: dict):
    """
    Create a card displaying a single comment.

    Args:
        comment: Comment data object
    """
    author = comment.get('author') or {}
    author_name = author.get('full_name') or author.get('username') or 'Unknown'
    created_at_raw = comment.get('created_at', '')
    created_at = str(created_at_raw)[:10] if created_at_raw else ''
    content = comment.get('content', '')
    is_public = comment.get('is_public', True)
    replies = comment.get('replies') or []

    with ui.card().classes('w-full p-3').style('border: 1px solid var(--border-light);'):
        # Header
        with ui.row().classes('w-full items-center justify-between mb-2'):
            with ui.row().classes('items-center gap-2'):
                ui.icon('account_circle').classes('text-lg').style('color: var(--primary-600);')
                ui.label(author_name).classes('font-medium text-sm')
                if not is_public:
                    ui.badge(tr('Private')).props('color=grey').classes('text-xs')

            ui.label(created_at).classes('text-xs').style('color: var(--text-muted);')

        # Content with shelfmark mentions rendered as links + translate button
        render_content_with_mentions(
            content,
            container_style='direction: rtl; text-align: right; color: var(--text-secondary);',
            show_translate=True
        )

        # Reactions summary
        reactions = comment.get('reactions_summary', {})
        total_reactions = reactions.get('total', 0)
        if total_reactions > 0:
            with ui.row().classes('gap-2 mt-2'):
                if reactions.get('like', 0) > 0:
                    ui.badge(f"{reactions['like']}").props('color=blue').classes('text-xs')
                if reactions.get('helpful', 0) > 0:
                    ui.badge(f"{reactions['helpful']}").props('color=green').classes('text-xs')

        # Replies
        if replies:
            with ui.column().classes('w-full mt-2 pr-4 gap-2'):
                for reply in replies:
                    create_reply_item(reply)


def create_reply_item(reply: dict):
    """
    Create a reply item (smaller, nested).

    Args:
        reply: Reply comment data
    """
    author = reply.get('author') or {}
    author_name = author.get('full_name') or author.get('username') or 'Unknown'
    created_at_raw = reply.get('created_at', '')
    created_at = str(created_at_raw)[:10] if created_at_raw else ''
    content = reply.get('content', '')

    with ui.row().classes('w-full gap-2').style('border-right: 2px solid var(--border-light); padding-right: 8px;'):
        with ui.column().classes('flex-1 gap-1'):
            with ui.row().classes('items-center gap-2'):
                ui.label(author_name).classes('font-medium text-xs')
                ui.label(created_at).classes('text-xs').style('color: var(--text-muted);')

            # Content with shelfmark mentions rendered as links + translate button
            render_content_with_mentions(
                content,
                container_classes='text-xs',
                container_style='direction: rtl; text-align: right; color: var(--text-tertiary);',
                show_translate=True
            )


def create_notes_button(
    document_id: str,
    page_number: Optional[int] = None,
    shelfmark: str = "",
    size: str = "sm"
):
    """
    Create a button that opens a dialog showing notes.
    Shows yellow indicator when comments exist.

    Args:
        document_id: System ID of the document
        page_number: Optional page number
        shelfmark: Display name
        size: Button size

    Returns:
        The button container element
    """
    container = ui.element('div').classes('relative inline-block')

    with container:
        def show_notes_dialog():
            dialog = ui.dialog()

            with dialog, ui.card().classes('w-96 max-h-96'):
                with ui.row().classes('w-full items-center justify-between p-4 border-b'):
                    ui.label(tr('Notes & Comments')).classes('font-bold')
                    ui.button(icon='close', on_click=dialog.close).props('flat round dense')

                with ui.scroll_area().classes('w-full').style('height: 300px;'):
                    with ui.column().classes('w-full gap-2 p-4'):
                        comments = fetch_document_comments(document_id, page_number)

                        if not comments:
                            with ui.row().classes('w-full justify-center'):
                                ui.label(tr('No comments yet')).classes('text-sm').style('color: var(--text-muted);')
                        else:
                            for comment in comments:
                                create_comment_card(comment)

            dialog.open()

        btn = ui.button(
            icon='forum',
            on_click=show_notes_dialog
        ).props(f'flat dense size={size}').tooltip(tr('View Comments'))

        # Yellow indicator dot (hidden by default)
        indicator = ui.element('div').classes('absolute').style(
            'top: 2px; right: 2px; width: 8px; height: 8px; '
            'background-color: #f59e0b; border-radius: 50%; display: none;'
        )

        # Check for comments and show indicator
        def check_comments():
            try:
                comments = fetch_document_comments(document_id, page_number)
                if comments:
                    indicator.style(add='display: block;')
                    btn.style(add='color: #f59e0b;')
            except Exception as e:
                pass  # Silently ignore errors in background check

        def _safe_check():
            try:
                check_comments()
            except RuntimeError:
                pass  # Parent element was deleted (NiceGUI timer lifecycle)

        # Use call_later instead of ui.timer to avoid parent_slot RuntimeError
        # when content_container.clear() destroys the timer's parent element
        asyncio.get_event_loop().call_later(0.1, _safe_check)

    return container
