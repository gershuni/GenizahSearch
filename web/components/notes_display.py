# -*- coding: utf-8 -*-
"""
Notes Display Component

Displays existing comments/notes for a document or page.
Supports:
- Public and private notes
- Nested replies
- Reactions
"""

from nicegui import ui
from web.translations import tr
from web.auth_state import GlobalAuthState, api_call
from typing import Optional, List


async def fetch_document_comments(document_id: str, page_number: int = None) -> List[dict]:
    """
    Fetch comments for a document.

    Args:
        document_id: System ID of the document
        page_number: Optional page number to filter (uses line_number field)

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

    # Filter by line_number (which stores page number) if specified
    # Include comments without line_number (document-level comments)
    if page_number is not None:
        comments = [c for c in comments if c.get('line_number') == page_number or c.get('line_number') is None]

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

        # Load on expansion
        panel.on('update:model-value', lambda e: load_notes() if e.args else None)

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

        # Content
        ui.label(content).classes('text-sm whitespace-pre-wrap').style(
            'direction: rtl; text-align: right; color: var(--text-secondary);'
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

            ui.label(content).classes('text-xs').style(
                'direction: rtl; text-align: right; color: var(--text-tertiary);'
            )


def create_notes_button(
    document_id: str,
    page_number: Optional[int] = None,
    shelfmark: str = "",
    size: str = "sm"
):
    """
    Create a button that opens a dialog showing notes.

    Args:
        document_id: System ID of the document
        page_number: Optional page number
        shelfmark: Display name
        size: Button size

    Returns:
        The button element
    """
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

    return ui.button(
        icon='forum',
        on_click=show_notes_dialog
    ).props(f'flat dense size={size}').tooltip(tr('View Comments'))
