# -*- coding: utf-8 -*-
"""
Version Selector Component

Allows users to switch between different versions of a transcription:
- Original (V0.7/V0.8)
- User corrections (approved)
"""

from nicegui import ui
from web.translations import tr
from web.auth_state import api_call
from typing import Optional, Callable, List


async def fetch_document_corrections(document_id: str, page_number: int = None) -> List[dict]:
    """
    Fetch approved corrections for a document.

    Args:
        document_id: System ID of the document
        page_number: Optional page number to filter

    Returns:
        List of correction objects
    """
    result = await api_call(
        "GET",
        f"/corrections/document/{document_id}",
        {"include_drafts": False}
    )

    if "error" in result or not isinstance(result, list):
        return []

    # Filter by page number if specified
    if page_number is not None:
        result = [c for c in result if c.get('page_number') == page_number]

    return result


def create_version_selector(
    document_id: str,
    page_number: int,
    original_text: str,
    on_version_change: Optional[Callable[[str, dict], None]] = None,
    size: str = "sm"
):
    """
    Create a version selector dropdown.

    Args:
        document_id: System ID of the document
        page_number: Page number within the document
        original_text: The original transcription text
        on_version_change: Callback when version is changed, receives (text, version_info)
        size: Button/select size

    Returns:
        The container element
    """
    container = ui.row().classes('items-center gap-2')

    with container:
        # Version indicator
        version_label = ui.label(tr('Original')).classes('text-xs font-medium').style(
            'color: var(--text-secondary);'
        )

        # Version dropdown button
        with ui.button(icon='history').props(f'flat dense size={size}').tooltip(tr('Version History')) as btn:
            menu = ui.menu()

            async def load_versions():
                """Load versions when menu is opened."""
                menu.clear()

                with menu:
                    # Original version option
                    def select_original():
                        version_label.text = tr('Original')
                        if on_version_change:
                            on_version_change(original_text, {'source': 'original'})
                        menu.close()

                    ui.menu_item(
                        f"{tr('Original')} (V0.8)",
                        on_click=select_original
                    ).classes('text-sm')

                    # Fetch corrections
                    corrections = await fetch_document_corrections(document_id, page_number)

                    if corrections:
                        ui.separator()
                        ui.label(tr('User Corrections')).classes('text-xs px-4 py-1').style(
                            'color: var(--text-muted);'
                        )

                        for corr in corrections:
                            author = corr.get('author', {})
                            author_name = author.get('full_name') or author.get('username', 'Unknown')
                            created_at = corr.get('created_at', '')[:10]

                            def select_correction(c=corr, name=author_name):
                                version_label.text = f"{tr('by')} {name}"
                                if on_version_change:
                                    on_version_change(c.get('corrected_text', ''), {
                                        'source': 'user',
                                        'correction_id': c.get('id'),
                                        'author': author_name,
                                        'created_at': created_at
                                    })
                                menu.close()

                            with ui.menu_item(on_click=select_correction).classes('text-sm'):
                                with ui.column().classes('gap-0'):
                                    ui.label(author_name).classes('font-medium')
                                    ui.label(created_at).classes('text-xs').style('color: var(--text-muted);')

                    if not corrections:
                        ui.menu_item(
                            tr('No other versions'),
                            auto_close=False
                        ).props('disable').classes('text-sm')

            btn.on('click', load_versions)

    return container


def create_version_badge(source: str = 'original', author: str = None):
    """
    Create a badge showing the current version source.

    Args:
        source: 'original' or 'user'
        author: Author name if user version

    Returns:
        The badge element
    """
    if source == 'original':
        return ui.badge('V0.8').props('color=grey').classes('text-xs')
    else:
        label = f"{tr('by')} {author}" if author else tr('User correction')
        return ui.badge(label).props('color=blue').classes('text-xs')
