# -*- coding: utf-8 -*-
"""
Version Selector Component

Allows users to switch between different versions of a transcription:
- V0.7 (original transcription)
- V0.8 (updated transcription)
- User corrections (approved)
"""

from nicegui import ui
from web.translations import tr
from web.auth_state import api_call
from typing import Optional, Callable, List


async def fetch_page_versions(sys_id: str, page_num: int = 1) -> dict:
    """
    Fetch all versions for a page using the new versions API.

    Args:
        sys_id: System ID of the document
        page_num: Page number

    Returns:
        Dict with current_default and all_versions
    """
    default_response = {'all_versions': [], 'current_default': None, 'total': 0}

    result = await api_call(
        "GET",
        f"/versions/{sys_id}/{page_num}"
    )

    # Validate response structure
    if not isinstance(result, dict):
        return default_response

    if "error" in result:
        return default_response

    # Ensure expected keys exist with correct types
    if 'all_versions' not in result or not isinstance(result.get('all_versions'), list):
        result['all_versions'] = []

    if 'current_default' not in result:
        result['current_default'] = None

    return result


async def fetch_document_corrections(document_id: str, page_number: int = None) -> List[dict]:
    """
    Fetch approved corrections for a document (fallback to old API).

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
        original_text: The original transcription text (fallback)
        on_version_change: Callback when version is changed, receives (text, version_info)
        size: Button/select size

    Returns:
        The container element
    """
    container = ui.row().classes('items-center gap-2')

    with container:
        # Version indicator - will be updated after loading
        version_label = ui.label('...').classes('text-xs font-medium').style(
            'color: var(--text-secondary);'
        )

        # Load and apply latest version on initialization
        async def load_and_apply_latest():
            """Load versions and apply the latest/default one."""
            versions_data = await fetch_page_versions(document_id, page_number)
            current_default = versions_data.get('current_default')
            all_versions = versions_data.get('all_versions', [])

            # Find the latest version (most recent user version or default)
            if current_default:
                # Use current default
                source = current_default.get('source', 'V0.8')
                if source == 'user':
                    user_info = current_default.get('user', {})
                    user_name = user_info.get('full_name') or user_info.get('username', 'User')
                    version_label.text = f"{tr('by')} {user_name}"
                    if on_version_change:
                        content = current_default.get('content', original_text)
                        on_version_change(content, {
                            'source': 'user',
                            'version_id': current_default.get('id'),
                            'author': user_name,
                            'is_default': True
                        })
                else:
                    version_label.text = source
                    if source != 'V0.8' and on_version_change:
                        content = current_default.get('content', original_text)
                        on_version_change(content, {
                            'source': source,
                            'version_id': current_default.get('id'),
                            'is_default': True
                        })
                    elif source == 'V0.8':
                        version_label.text = 'V0.8'
            else:
                # No default set - check for user versions
                user_versions = [v for v in all_versions if v.get('source') == 'user']
                if user_versions:
                    # Use most recent user version
                    latest = user_versions[0]  # Already sorted by date desc
                    user_name = latest.get('user_name', 'User')
                    version_label.text = f"{tr('by')} {user_name}"
                    # Fetch full content
                    full_ver = await api_call("GET", f"/versions/id/{latest.get('id')}")
                    if "error" not in full_ver and on_version_change:
                        on_version_change(full_ver.get('content', original_text), {
                            'source': 'user',
                            'version_id': latest.get('id'),
                            'author': user_name,
                            'is_default': False
                        })
                else:
                    # Fall back to V0.8
                    version_label.text = 'V0.8'

        # Schedule the async load
        ui.timer(0.1, load_and_apply_latest, once=True)

        # Version dropdown button
        with ui.button(icon='history').props(f'flat dense size={size}').tooltip(tr('Version History')) as btn:
            menu = ui.menu()

            async def load_versions():
                """Load versions when menu is opened."""
                menu.clear()

                with menu:
                    # Always show original V0.8 option
                    def select_original():
                        version_label.text = 'V0.8'
                        menu.close()
                        if on_version_change:
                            on_version_change(original_text, {'source': 'V0.8', 'is_original': True})

                    ui.menu_item(
                        f"V0.8 ({tr('Original')})",
                        on_click=select_original
                    ).classes('text-sm')

                    # Try to load from versions API
                    versions_data = await fetch_page_versions(document_id, page_number)
                    all_versions = versions_data.get('all_versions', [])

                    # Group by source
                    base_versions = [v for v in all_versions if v.get('source') in ('V0.7', 'V0.8')]
                    user_versions = [v for v in all_versions if v.get('source') == 'user']

                    # Show V0.7 if available
                    v07_versions = [v for v in base_versions if v.get('source') == 'V0.7']
                    for ver in v07_versions:
                        ver_id = ver.get('id')
                        is_default = ver.get('is_current_default', False)

                        async def select_v07(vid=ver_id, vdefault=is_default):
                            version_label.text = 'V0.7' + (' ✓' if vdefault else '')
                            if on_version_change:
                                full_ver = await api_call("GET", f"/versions/id/{vid}")
                                if "error" not in full_ver:
                                    on_version_change(full_ver.get('content', original_text), {
                                        'source': 'V0.7',
                                        'version_id': vid,
                                        'is_default': vdefault
                                    })
                            menu.close()

                        label_text = 'V0.7'
                        if is_default:
                            label_text += f" ({tr('Default')})"

                        ui.menu_item(label_text, on_click=select_v07).classes('text-sm')

                    # User versions
                    if user_versions:
                        ui.separator()
                        ui.label(tr('User Corrections')).classes('text-xs px-4 py-1').style(
                            'color: var(--text-muted);'
                        )

                        for ver in user_versions:
                            ver_id = ver.get('id')
                            user_name = ver.get('user_name') or 'Unknown'
                            created_at = ver.get('created_at', '')[:10] if ver.get('created_at') else ''
                            is_default = ver.get('is_current_default', False)

                            async def select_user(vid=ver_id, name=user_name, vdefault=is_default):
                                version_label.text = f"{tr('by')} {name}"
                                menu.close()
                                if on_version_change:
                                    full_ver = await api_call("GET", f"/versions/id/{vid}")
                                    if "error" not in full_ver and 'content' in full_ver:
                                        content = full_ver.get('content', '')
                                        on_version_change(content if content else original_text, {
                                            'source': 'user',
                                            'version_id': vid,
                                            'author': name,
                                            'is_default': vdefault
                                        })

                            with ui.menu_item(on_click=select_user).classes('text-sm'):
                                with ui.column().classes('gap-0'):
                                    with ui.row().classes('items-center gap-2'):
                                        ui.label(user_name).classes('font-medium')
                                        if is_default:
                                            ui.badge(tr('Default')).props('color=green').classes('text-xs')
                                    if created_at:
                                        ui.label(created_at).classes('text-xs').style('color: var(--text-muted);')

                    # Fallback to corrections API if no user versions from versions API
                    if not user_versions:
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
                                corrected_text = corr.get('corrected_text', '')
                                corr_id = corr.get('id')

                                def select_correction(ctext=corrected_text, name=author_name, cid=corr_id, cdate=created_at):
                                    version_label.text = f"{tr('by')} {name}"
                                    if on_version_change:
                                        on_version_change(ctext, {
                                            'source': 'user',
                                            'correction_id': cid,
                                            'author': name,
                                            'created_at': cdate
                                        })
                                    menu.close()

                                with ui.menu_item(on_click=select_correction).classes('text-sm'):
                                    with ui.column().classes('gap-0'):
                                        ui.label(author_name).classes('font-medium')
                                        ui.label(created_at).classes('text-xs').style('color: var(--text-muted);')

                    # Show message if no versions at all besides original
                    if not user_versions and not base_versions:
                        corrections = await fetch_document_corrections(document_id, page_number)
                        if not corrections:
                            ui.separator()
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
        source: 'original', 'V0.7', 'V0.8', or 'user'
        author: Author name if user version

    Returns:
        The badge element
    """
    if source in ('original', 'V0.8'):
        return ui.badge('V0.8').props('color=grey').classes('text-xs')
    elif source == 'V0.7':
        return ui.badge('V0.7').props('color=grey-7').classes('text-xs')
    else:
        label = f"{tr('by')} {author}" if author else tr('User correction')
        return ui.badge(label).props('color=blue').classes('text-xs')
