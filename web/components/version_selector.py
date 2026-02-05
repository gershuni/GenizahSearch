# -*- coding: utf-8 -*-
"""
Version Selector Component

Allows users to switch between different versions of a transcription:
- PGP transcription (curated, when available)
- V0.7 (original transcription)
- V0.8 (updated transcription)
- User corrections (approved)
"""

from nicegui import ui
from web.translations import tr
from web.supabase_client import get_corrections
from web.auth_state import GlobalAuthState
from typing import Optional, Callable, List, Dict, Any


def fetch_page_versions(sys_id: str, page_num: int = 1) -> dict:
    """Fetch all versions for a page from Supabase corrections."""
    default_response = {'all_versions': [], 'current_default': None, 'total': 0}
    try:
        # Get approved corrections for this document/page
        corrections = get_corrections(sys_id=sys_id, status='approved')
        if page_num:
            corrections = [c for c in corrections if c.get('page_number') == page_num]

        # Format as versions
        versions = []
        for c in corrections:
            profile = c.get('profiles', {}) or {}
            versions.append({
                'id': c.get('id'),
                'source': 'user',
                'user_name': profile.get('full_name') or profile.get('username') or 'Unknown',
                'content': c.get('corrected_text', ''),
                'created_at': c.get('created_at'),
                'is_current_default': False,
                'user': {
                    'full_name': profile.get('full_name'),
                    'username': profile.get('username')
                }
            })

        # Set most recent as default if exists
        current_default = versions[0] if versions else None
        if current_default:
            current_default['is_current_default'] = True

        return {
            'all_versions': versions,
            'current_default': current_default,
            'total': len(versions)
        }
    except Exception as e:
        print(f"Error fetching versions: {e}")
        return default_response


def fetch_document_corrections(document_id: str, page_number: int = None) -> List[dict]:
    """Fetch approved corrections for a document from Supabase."""
    try:
        corrections = get_corrections(sys_id=document_id, status='approved')
        if page_number is not None:
            corrections = [c for c in corrections if c.get('page_number') == page_number]

        # Format for display
        formatted = []
        for c in corrections:
            profile = c.get('profiles', {}) or {}
            formatted.append({
                'id': c.get('id'),
                'corrected_text': c.get('corrected_text', ''),
                'created_at': c.get('created_at'),
                'author': {
                    'full_name': profile.get('full_name'),
                    'username': profile.get('username')
                }
            })
        return formatted
    except Exception as e:
        print(f"Error fetching corrections: {e}")
        return []


def create_version_selector(
    document_id: str,
    page_number: int,
    original_text: str,
    on_version_change: Optional[Callable[[str, dict], None]] = None,
    size: str = "sm",
    pgp_transcription: Optional[Dict[str, Any]] = None
):
    """Create a version selector dropdown.

    Args:
        document_id: The sys_id of the document
        page_number: The page number
        original_text: The original V0.8 transcription text
        on_version_change: Callback when version is selected (text, version_info)
        size: Button size ('sm', 'md', etc.)
        pgp_transcription: Optional dict with PGP transcription data:
            - content: The transcription text
            - attribution: Scholar/source name
            - pgp_url: URL to PGP document
            - pgpid: PGP document ID
    """
    container = ui.row().classes('items-center gap-2')

    with container:
        # Version indicator
        version_label = ui.label('V0.8').classes('text-xs font-medium').style(
            'color: var(--text-secondary);'
        )

        def load_and_apply_latest():
            """Load versions and apply the latest/default one."""
            # If PGP transcription is available, auto-select it as primary
            if pgp_transcription and pgp_transcription.get('content'):
                attribution = pgp_transcription.get('attribution', 'PGP')
                version_label.text = 'PGP'
                version_label.style('color: var(--q-positive);')  # Green for verified
                if on_version_change:
                    on_version_change(pgp_transcription['content'], {
                        'source': 'pgp',
                        'attribution': attribution,
                        'pgp_url': pgp_transcription.get('pgp_url'),
                        'pgpid': pgp_transcription.get('pgpid'),
                        'is_pgp': True,
                        'is_default': True
                    })
                return

            # Fall back to user corrections or V0.8
            versions_data = fetch_page_versions(document_id, page_number)
            current_default = versions_data.get('current_default')

            if current_default:
                user_info = current_default.get('user', {})
                user_name = user_info.get('full_name') or user_info.get('username', 'User')
                version_label.text = f"{tr('by')} {user_name}"
                if on_version_change:
                    on_version_change(current_default.get('content', original_text), {
                        'source': 'user', 'version_id': current_default.get('id'), 'author': user_name, 'is_default': True
                    })
            else:
                version_label.text = 'V0.8'

        ui.timer(0.1, load_and_apply_latest, once=True)

        with ui.button(icon='history').props(f'flat dense size={size}').tooltip(tr('Version History')) as btn:
            menu = ui.menu()
            with menu:
                ui.menu_item(tr('Loading...')).props('disable')

            def load_versions():
                # Load corrections from Supabase
                corrections = fetch_document_corrections(document_id, page_number)

                # Rebuild the menu
                menu.clear()
                with menu:
                    # PGP Transcription (first/primary when available)
                    if pgp_transcription and pgp_transcription.get('content'):
                        def select_pgp():
                            version_label.text = 'PGP'
                            version_label.style('color: var(--q-positive);')
                            menu.close()
                            if on_version_change:
                                on_version_change(pgp_transcription['content'], {
                                    'source': 'pgp',
                                    'attribution': pgp_transcription.get('attribution', 'PGP'),
                                    'pgp_url': pgp_transcription.get('pgp_url'),
                                    'pgpid': pgp_transcription.get('pgpid'),
                                    'is_pgp': True
                                })

                        with ui.menu_item(on_click=select_pgp).classes('text-sm'):
                            with ui.row().classes('items-center gap-2'):
                                ui.icon('verified', size='xs').classes('text-green-600')
                                with ui.column().classes('gap-0'):
                                    ui.label(tr('PGP Transcription')).classes('font-medium text-green-700')
                                    attribution = pgp_transcription.get('attribution', '')
                                    if attribution:
                                        ui.label(f"{tr('Transcription by')} {attribution}").classes('text-xs').style('color: var(--text-muted);')

                        ui.separator()

                    # Original V0.8
                    def select_original():
                        version_label.text = 'V0.8'
                        version_label.style('color: var(--text-secondary);')
                        menu.close()
                        if on_version_change:
                            on_version_change(original_text, {'source': 'V0.8', 'is_original': True})
                    ui.menu_item(f"V0.8 ({tr('Original')})", on_click=select_original).classes('text-sm')

                    # User corrections
                    if corrections:
                        ui.separator()
                        ui.label(tr('User Corrections')).classes('text-xs px-4 py-1').style('color: var(--text-muted);')

                        for corr in corrections:
                            author = corr.get('author', {})
                            author_name = author.get('full_name') or author.get('username') or 'Unknown'
                            date = corr.get('created_at', '')

                            def make_select_correction(c=corr, name=author_name):
                                def select_correction():
                                    version_label.text = f"{tr('by')} {name}"
                                    version_label.style('color: var(--text-secondary);')
                                    menu.close()
                                    if on_version_change:
                                        on_version_change(c.get('corrected_text', ''), {
                                            'source': 'user', 'correction_id': c.get('id'), 'author': name
                                        })
                                return select_correction

                            with ui.menu_item(on_click=make_select_correction()).classes('text-sm'):
                                with ui.column().classes('gap-0'):
                                    ui.label(author_name).classes('font-medium')
                                    if date:
                                        ui.label(str(date)[:10]).classes('text-xs').style('color: var(--text-muted);')
                    else:
                        ui.separator()
                        ui.menu_item(tr('No other versions')).props('disable').classes('text-sm')

            btn.on('click', load_versions)

    return container


def create_version_badge(source: str = 'original', author: str = None):
    """Create a badge showing the current version source."""
    if source == 'pgp':
        return ui.badge('PGP').props('color=positive').classes('text-xs')
    elif source in ('original', 'V0.8'):
        return ui.badge('V0.8').props('color=grey').classes('text-xs')
    elif source == 'V0.7':
        return ui.badge('V0.7').props('color=grey-7').classes('text-xs')
    else:
        label = f"{tr('by')} {author}" if author else tr('User correction')
        return ui.badge(label).props('color=blue').classes('text-xs')
