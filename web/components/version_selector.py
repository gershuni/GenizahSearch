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
    """Fetch all versions for a page using the new versions API."""
    default_response = {'all_versions': [], 'current_default': None, 'total': 0}
    result = await api_call("GET", f"/versions/{sys_id}/{page_num}")
    if not isinstance(result, dict) or "error" in result:
        return default_response
    return result


async def fetch_document_corrections(document_id: str, page_number: int = None) -> List[dict]:
    """Fetch approved corrections for a document (fallback to old API)."""
    result = await api_call(
        "GET",
        f"/corrections/document/{document_id}",
        {"include_drafts": False}
    )
    if not isinstance(result, list) or "error" in result:
        return []
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
    """Create a version selector dropdown."""
    container = ui.row().classes('items-center gap-2')

    with container:
        # Version indicator
        version_label = ui.label('...').classes('text-xs font-medium').style(
            'color: var(--text-secondary);'
        )

        async def load_and_apply_latest():
            """Load versions and apply the latest/default one."""
            versions_data = await fetch_page_versions(document_id, page_number)
            current_default = versions_data.get('current_default')
            all_versions = versions_data.get('all_versions', [])

            if current_default:
                source = current_default.get('source', 'V0.8')
                if source == 'user':
                    user_info = current_default.get('user', {})
                    user_name = user_info.get('full_name') or user_info.get('username', 'User')
                    version_label.text = f"{tr('by')} {user_name}"
                    if on_version_change:
                        on_version_change(current_default.get('content', original_text), {
                            'source': 'user', 'version_id': current_default.get('id'), 'author': user_name, 'is_default': True
                        })
                else:
                    version_label.text = source
                    if source != 'V0.8' and on_version_change:
                        on_version_change(current_default.get('content', original_text), {
                            'source': source, 'version_id': current_default.get('id'), 'is_default': True
                        })
                    else:
                        version_label.text = 'V0.8'
            else:
                user_versions = [v for v in all_versions if v.get('source') == 'user']
                if user_versions:
                    latest = user_versions[0]
                    user_name = latest.get('user_name', 'User')
                    version_label.text = f"{tr('by')} {user_name}"
                    full_ver = await api_call("GET", f"/versions/id/{latest.get('id')}")
                    if "error" not in full_ver and on_version_change:
                        on_version_change(full_ver.get('content', original_text), {
                            'source': 'user', 'version_id': latest.get('id'), 'author': user_name, 'is_default': False
                        })
                else:
                    version_label.text = 'V0.8'

        ui.timer(0.1, load_and_apply_latest, once=True)

        with ui.button(icon='history').props(f'flat dense size={size}').tooltip(tr('Version History')) as btn:
            menu = ui.menu()
            with menu:
                ui.menu_item(tr('Loading...')).props('disable')

            async def load_versions():
                # 1. Load data FIRST (API calls)
                versions_data = await fetch_page_versions(document_id, page_number)
                all_versions = versions_data.get('all_versions', [])
                corrections = await fetch_document_corrections(document_id, page_number)

                # 2. Rebuild the menu
                menu.clear()
                with menu:
                    # Base versions
                    base_versions = [v for v in all_versions if v.get('source') in ('V0.7', 'V0.8')]
                    
                    # 3. Aggregate user items and deduplicate
                    user_items = []
                    for v in all_versions:
                        if v.get('source') == 'user':
                            user_items.append({
                                'type': 'version', 'id': v.get('id'),
                                'user_name': v.get('user_name') or 'Unknown',
                                'date': v.get('created_at'),
                                'is_default': v.get('is_current_default', False),
                                'raw': v
                            })
                    for c in corrections:
                        author = c.get('author', {})
                        name = author.get('full_name') or author.get('username') or 'Unknown'
                        user_items.append({
                            'type': 'correction', 'id': c.get('id'),
                            'user_name': name, 'date': c.get('created_at'),
                            'is_default': False, 'raw': c
                        })

                    user_items.sort(key=lambda x: str(x['date']) if x['date'] else '', reverse=True)
                    seen_users = set()
                    final_user_items = []
                    for item in user_items:
                        name_key = item['user_name'].strip()
                        if name_key not in seen_users:
                            seen_users.add(name_key)
                            final_user_items.append(item)

                    # --- RENDER ---
                    # Original V0.8
                    def select_original():
                        version_label.text = 'V0.8'
                        menu.close()
                        if on_version_change:
                            on_version_change(original_text, {'source': 'V0.8', 'is_original': True})
                    ui.menu_item(f"V0.8 ({tr('Original')})", on_click=select_original).classes('text-sm')

                    # V0.7
                    v07_versions = [v for v in base_versions if v.get('source') == 'V0.7']
                    for ver in v07_versions:
                        async def select_v07(vid=ver['id'], vdefault=ver.get('is_current_default', False)):
                            version_label.text = 'V0.7' + (' ✓' if vdefault else '')
                            full_ver = await api_call("GET", f"/versions/id/{vid}")
                            if "error" not in full_ver and on_version_change:
                                on_version_change(full_ver.get('content', original_text), {'source': 'V0.7', 'version_id': vid, 'is_default': vdefault})
                            menu.close()
                        label = 'V0.7' + (f" ({tr('Default')})" if ver.get('is_current_default') else "")
                        ui.menu_item(label, on_click=select_v07).classes('text-sm')

                    if final_user_items:
                        ui.separator()
                        ui.label(tr('User Corrections')).classes('text-xs px-4 py-1').style('color: var(--text-muted);')
                        for item in final_user_items:
                            async def select_item(it=item):
                                version_label.text = f"{tr('by')} {it['user_name']}"
                                menu.close()
                                if it['type'] == 'version':
                                    full_ver = await api_call("GET", f"/versions/id/{it['id']}")
                                    if "error" not in full_ver and on_version_change:
                                        on_version_change(full_ver.get('content', original_text), {
                                            'source': 'user', 'version_id': it['id'], 'author': it['user_name'], 'is_default': it['is_default']
                                        })
                                elif it['type'] == 'correction' and on_version_change:
                                    on_version_change(it['raw'].get('corrected_text', ''), {
                                        'source': 'user', 'correction_id': it['id'], 'author': it['user_name'], 'created_at': it['date']
                                    })

                            with ui.menu_item(on_click=select_item).classes('text-sm'):
                                with ui.column().classes('gap-0'):
                                    with ui.row().classes('items-center gap-2'):
                                        ui.label(item['user_name']).classes('font-medium')
                                        if item['is_default']:
                                            ui.badge(tr('Default')).props('color=green').classes('text-xs')
                                    if item['date']:
                                        ui.label(str(item['date'])[:10]).classes('text-xs').style('color: var(--text-muted);')

                    if not final_user_items and not v07_versions:
                        ui.separator()
                        ui.menu_item(tr('No other versions')).props('disable').classes('text-sm')

            btn.on('click', load_versions)

    return container


def create_version_badge(source: str = 'original', author: str = None):
    """Create a badge showing the current version source."""
    if source in ('original', 'V0.8'):
        return ui.badge('V0.8').props('color=grey').classes('text-xs')
    elif source == 'V0.7':
        return ui.badge('V0.7').props('color=grey-7').classes('text-xs')
    else:
        label = f"{tr('by')} {author}" if author else tr('User correction')
        return ui.badge(label).props('color=blue').classes('text-xs')
