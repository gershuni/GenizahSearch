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
from nicegui import ui
from web.translations import tr
from web.auth_state import GlobalAuthState, api_call
from typing import Optional, List


# Pattern to match shelfmark mentions: [[shelfmark:xxx|id:yyy]]
SHELFMARK_MENTION_PATTERN = re.compile(r'\[\[shelfmark:([^\]|]+)\|id:([^\]]+)\]\]')


def render_content_with_mentions(content: str, container_classes: str = '', container_style: str = ''):
    """
    Render comment content with shelfmark mentions as clickable links.

    Mentions are in format: [[shelfmark:T-S 8J6.1|id:123456]]
    They will be rendered as clickable links that navigate to the document.

    Args:
        content: The comment text content
        container_classes: CSS classes for the container
        container_style: Inline styles for the container
    """
    if not content:
        return

    # Split content by mentions
    parts = SHELFMARK_MENTION_PATTERN.split(content)

    # parts will be: [text, shelfmark1, id1, text, shelfmark2, id2, ...]
    # Every 3 elements: text, shelfmark, id

    with ui.element('div').classes(f'text-sm whitespace-pre-wrap {container_classes}').style(container_style):
        i = 0
        while i < len(parts):
            if i % 3 == 0:
                # Regular text
                text = parts[i]
                if text:
                    ui.html(f'<span>{text}</span>')
            elif i % 3 == 1:
                # Shelfmark (next element is id)
                shelfmark = parts[i]
                doc_id = parts[i + 1] if i + 1 < len(parts) else ''

                def make_click(did=doc_id):
                    def click():
                        ui.navigate.to(f'/browse?sys_id={did}')
                    return click

                with ui.element('span').classes('inline'):
                    ui.link(
                        shelfmark,
                        target=f'/browse?sys_id={doc_id}'
                    ).classes('text-primary font-medium hover:underline').style('cursor: pointer;')
                i += 1  # Skip the id part
            i += 1


async def fetch_document_comments(document_id: str, page_number: int = None) -> List[dict]:
    """
    Fetch comments for a document.

    Args:
        document_id: System ID of the document
        page_number: Optional page number to filter

    Returns:
        List of comment objects
    """
    result = await api_call(
        "GET",
        f"/comments/document/{document_id}",
        {"include_replies": True}
    )

    if "error" in result:
        return []

    comments = result.get('items', [])

    # Filter by page_number if specified
    # Include comments without page_number (document-level comments)
    if page_number is not None:
        comments = [c for c in comments if c.get('page_number') == page_number or c.get('page_number') is None]

    return comments


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
        The panel container element
    """
    panel = ui.expansion(
        text=tr('Notes & Comments'),
        icon='comment'
    ).classes('w-full').props('dense')

    with panel:
        notes_container = ui.column().classes('w-full gap-2 p-2')

        async def load_notes():
            """Load and display notes."""
            notes_container.clear()

            with notes_container:
                comments = await fetch_document_comments(document_id, page_number)

                if not comments:
                    with ui.row().classes('w-full justify-center p-4'):
                        ui.label(tr('No comments yet')).classes('text-sm').style('color: var(--text-muted);')
                else:
                    for comment in comments:
                        create_comment_card(comment)

        # Load on expansion - use background task for async
        async def on_expand(e):
            if e.args:
                await load_notes()

        panel.on('update:model-value', on_expand)

    return panel


def create_comment_card(comment: dict):
    """
    Create a card displaying a single comment.

    Args:
        comment: Comment data object
    """
    author = comment.get('author', {})
    author_name = author.get('full_name') or author.get('username', 'Unknown')
    created_at = comment.get('created_at', '')[:10]
    content = comment.get('content', '')
    is_public = comment.get('is_public', True)
    replies = comment.get('replies', [])

    with ui.card().classes('w-full p-3').style('border: 1px solid var(--border-light);'):
        # Header
        with ui.row().classes('w-full items-center justify-between mb-2'):
            with ui.row().classes('items-center gap-2'):
                ui.icon('account_circle').classes('text-lg').style('color: var(--primary-600);')
                ui.label(author_name).classes('font-medium text-sm')
                if not is_public:
                    ui.badge(tr('Private')).props('color=grey').classes('text-xs')

            ui.label(created_at).classes('text-xs').style('color: var(--text-muted);')

        # Content with shelfmark mentions rendered as links
        render_content_with_mentions(
            content,
            container_style='direction: rtl; text-align: right; color: var(--text-secondary);'
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
    author = reply.get('author', {})
    author_name = author.get('full_name') or author.get('username', 'Unknown')
    created_at = reply.get('created_at', '')[:10]
    content = reply.get('content', '')

    with ui.row().classes('w-full gap-2').style('border-right: 2px solid var(--border-light); padding-right: 8px;'):
        with ui.column().classes('flex-1 gap-1'):
            with ui.row().classes('items-center gap-2'):
                ui.label(author_name).classes('font-medium text-xs')
                ui.label(created_at).classes('text-xs').style('color: var(--text-muted);')

            # Content with shelfmark mentions rendered as links
            render_content_with_mentions(
                content,
                container_classes='text-xs',
                container_style='direction: rtl; text-align: right; color: var(--text-tertiary);'
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
        async def show_notes_dialog():
            dialog = ui.dialog()

            with dialog, ui.card().classes('w-96 max-h-96'):
                with ui.row().classes('w-full items-center justify-between p-4 border-b'):
                    ui.label(tr('Notes & Comments')).classes('font-bold')
                    ui.button(icon='close', on_click=dialog.close).props('flat round dense')

                with ui.scroll_area().classes('w-full').style('height: 300px;'):
                    with ui.column().classes('w-full gap-2 p-4'):
                        comments = await fetch_document_comments(document_id, page_number)

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
        async def check_comments():
            comments = await fetch_document_comments(document_id, page_number)
            if comments:
                indicator.style(add='display: block;')
                btn.style(add='color: #f59e0b;')

        ui.timer(0.2, check_comments, once=True)

    return container
