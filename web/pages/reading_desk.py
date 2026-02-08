# -*- coding: utf-8 -*-
"""
Virtual Reading Desk Page

A multi-manuscript viewer that allows researchers to view fragments side by side
with images and transcriptions. Supports three entry points:
  - VIEW-01: From PGP joined documents (/reading-desk?pgpid=1234)
  - VIEW-02: Manual add by shelfmark input
  - VIEW-03: From personal lists (/reading-desk?list_id=abc)

Each fragment displays recto/verso pages with:
  - Image panel (left) with per-image zoom/rotate controls
  - Text panel (right) with version selector for editions/translations
  - Per-source directionality: Hebrew editions RTL, English translations LTR
"""

import re
from nicegui import ui
from web.translations import tr, is_rtl, get_dir
from web.state import state as app_state
from web.services import get_service
from shared.reading_desk_model import ReadingDeskEntry, ReadingDeskState
from shared.document_service import (
    get_fragments_for_document,
    get_all_sources_for_fragment,
    get_transcription_for_document,
    get_document_metadata,
)
from web.components.version_selector import create_version_selector
from genizah_core import get_library_display
from typing import Optional, List, Dict, Any


def _sanitize_id(text: str) -> str:
    """Sanitize a string to use as a safe HTML/JS identifier."""
    return re.sub(r'[^a-zA-Z0-9]', '_', text)


def _is_oxford_shelfmark(shelfmark: str) -> bool:
    """Check if shelfmark is Oxford based on prefix."""
    sm_lower = (shelfmark or '').lower()
    return sm_lower.startswith('ms heb') or sm_lower.startswith('ms. heb')


def _build_image_url(sys_id: str, pg_idx: int, is_oxford: bool) -> str:
    """Build the image proxy URL for a page."""
    if is_oxford:
        return f'/api/oxford_image/{sys_id}?page={pg_idx}'
    return f'/api/nli_image_by_sysid/{sys_id}?page={pg_idx}'


def _load_entry_from_sys_id(sys_id: str, sequence_order: int = 0) -> Optional[ReadingDeskEntry]:
    """
    Load a ReadingDeskEntry from a sys_id using the service layer.

    Returns None if the service is not ready or the sys_id is not found.
    """
    service = get_service()
    if not service.is_ready:
        return None

    frag_pages = service.get_full_manuscript(sys_id)

    # Get metadata
    shelfmark = ''
    title = ''
    library_code = ''
    if app_state.meta_mgr:
        shelfmark, title = app_state.meta_mgr.get_meta_for_id(sys_id)
        library_code = app_state.meta_mgr.get_library_for_id(sys_id) or ''

    if not frag_pages and not shelfmark:
        return None

    # Build pages list
    is_oxford = _is_oxford_shelfmark(shelfmark)
    pages = []
    if frag_pages:
        for pg in frag_pages:
            pg_num = pg.p_num if hasattr(pg, 'p_num') else 1
            pg_idx = max(0, pg_num - 1)
            pages.append({
                'p_num': pg_num,
                'text': pg.text if hasattr(pg, 'text') else '',
                'image_url': _build_image_url(sys_id, pg_idx, is_oxford),
                'is_oxford': is_oxford,
                'full_header': pg.full_header if hasattr(pg, 'full_header') else '',
            })
    else:
        # No text data, still show image placeholders for recto/verso
        for pg_idx in range(2):
            pages.append({
                'p_num': pg_idx + 1,
                'text': '',
                'image_url': _build_image_url(sys_id, pg_idx, is_oxford),
                'is_oxford': is_oxford,
                'full_header': '',
            })

    # Load PGP sources
    sources = get_all_sources_for_fragment(sys_id)

    return ReadingDeskEntry(
        sys_id=sys_id,
        shelfmark=shelfmark or sys_id,
        title=title or '',
        library_code=library_code,
        pages=pages,
        sources=sources,
        sequence_order=sequence_order,
    )


def create_reading_desk_page(
    initial_pgpid: Optional[int] = None,
    initial_sys_ids: Optional[List[str]] = None,
    initial_list_id: Optional[str] = None,
):
    """
    Create the reading desk page.

    Args:
        initial_pgpid: PGP document ID to load fragments for (VIEW-01)
        initial_sys_ids: List of sys_ids to load directly (VIEW-02)
        initial_list_id: List ID to load items from (VIEW-03)
    """
    desk_state = ReadingDeskState()
    # Mutable refs for UI update
    refs = {
        'display_area': None,
        'count_badge': None,
        'source_label': None,
    }

    # ========================================================================
    # Header bar
    # ========================================================================
    with ui.card().classes('w-full mb-4').style('overflow: visible;'):
        with ui.row().classes('w-full items-center justify-between p-4').style(
            'background: linear-gradient(135deg, #0e7490 0%, #155e75 100%); border-radius: 12px;'
        ):
            with ui.row().classes('items-center gap-3'):
                ui.icon('auto_stories').classes('text-white text-2xl')
                ui.label(tr('Reading Desk')).classes('text-xl font-bold text-white')
                refs['count_badge'] = ui.badge('0', color='white').props('outline dense').classes('text-xs text-white')
                refs['source_label'] = ui.label('').classes('text-sm text-white/80')

            ui.button(
                tr('Back'),
                icon='arrow_forward' if is_rtl() else 'arrow_back',
                on_click=lambda: ui.navigate.to('/browse')
            ).props('flat dense text-color=white')

    # ========================================================================
    # Add manuscript toolbar
    # ========================================================================
    with ui.row().classes('w-full items-center gap-3 mb-4'):
        add_input = ui.input(
            placeholder=tr('Enter shelfmark or sys_id...')
        ).classes('flex-grow').props('outlined dense')

        def handle_add_manuscript():
            query = add_input.value.strip() if add_input.value else ''
            if not query:
                ui.notify(tr('Please enter a shelfmark or sys_id'), type='warning')
                return

            service = get_service()
            if not service.is_ready:
                ui.notify(tr('Service not ready yet'), type='warning')
                return

            # Check if already on the desk
            for entry in desk_state.entries:
                if entry.sys_id == query or (entry.shelfmark and entry.shelfmark.upper() == query.upper()):
                    ui.notify(tr('Already on the desk'), type='info')
                    return

            # Try resolving as shelfmark first
            results, exact = service.search_by_shelfmark(query, limit=1)
            if results and exact:
                sys_id = results[0].sys_id
            else:
                # Try treating as sys_id directly
                sys_id = query

            entry = _load_entry_from_sys_id(sys_id, sequence_order=len(desk_state.entries))
            if entry:
                desk_state.entries.append(entry)
                if not desk_state.source_description:
                    desk_state.source_description = tr('Custom selection')
                add_input.value = ''
                _render_display()
            else:
                ui.notify(tr('Manuscript not found'), type='warning')

        add_input.on('keydown.enter', lambda: handle_add_manuscript())
        ui.button(tr('Add'), icon='add', on_click=handle_add_manuscript).props('color=teal')

    # ========================================================================
    # Fragment display area
    # ========================================================================
    refs['display_area'] = ui.column().classes('w-full gap-6')

    # ========================================================================
    # Render helpers
    # ========================================================================

    def _update_header():
        """Update header badge and source description."""
        count = len(desk_state.entries)
        if refs['count_badge']:
            refs['count_badge'].text = str(count)
        if refs['source_label']:
            refs['source_label'].text = desk_state.source_description

    def _render_display():
        """Re-render all entries in the display area."""
        _update_header()
        display = refs['display_area']
        if not display:
            return
        display.clear()

        with display:
            if not desk_state.entries:
                with ui.column().classes('w-full items-center justify-center py-16'):
                    ui.icon('auto_stories', size='4rem').style('color: var(--text-muted);')
                    ui.label(tr('No manuscripts on the desk')).classes('text-lg mt-4').style('color: var(--text-muted);')
                    ui.label(tr('Add manuscripts using the search bar above, or open from Browse or Lists')).classes('text-sm').style('color: var(--text-muted);')
                return

            for frag_idx, entry in enumerate(desk_state.entries):
                _render_entry(entry, frag_idx)

            # Full PGP transcription at the bottom (if desk was from pgpid)
            if desk_state.pgpid:
                _render_full_transcription(desk_state.pgpid)

    def _render_entry(entry: ReadingDeskEntry, frag_idx: int):
        """Render a single entry (fragment) on the desk."""
        # Separator between fragments
        if frag_idx > 0:
            ui.separator().classes('my-2')

        # Fragment header
        with ui.row().classes('items-center gap-2 mb-2'):
            ui.icon('description').classes('text-teal-600')
            ui.label(entry.shelfmark).classes('font-bold text-base').style('color: var(--text-primary);')
            if entry.library_code:
                lib_display = get_library_display(entry.library_code, short=True)
                if lib_display:
                    ui.badge(lib_display, color='teal').props('outline dense').classes('text-xs')

            # Remove button
            def make_remove_handler(e=entry):
                def remove():
                    desk_state.entries = [x for x in desk_state.entries if x.sys_id != e.sys_id]
                    _render_display()
                return remove

            ui.button(icon='close', on_click=make_remove_handler()).props('flat dense round size=sm color=grey')

        # Render each page (recto/verso)
        for pg in entry.pages:
            pg_num = pg.get('p_num', 1)
            pg_idx = max(0, pg_num - 1)
            pg_text = pg.get('text', '')
            pg_label_text = tr('Recto') if pg_idx == 0 else tr('Verso')
            img_url = pg.get('image_url', '')
            is_oxford = pg.get('is_oxford', False)

            # Unique ID for this image (for zoom/rotate JS)
            uid = _sanitize_id(f'{entry.sys_id}_{pg_idx}')

            # Page label
            ui.label(f'{entry.shelfmark} \u2014 {pg_label_text}').classes(
                'text-xs font-medium text-gray-500 mt-1'
            )

            # Side-by-side: [Image | Text]
            with ui.element('div').style(
                'display: flex; flex-direction: row; gap: 12px; width: 100%; min-height: 300px;'
            ):
                # ---- Image panel (left, 50%) ----
                safe_sid = entry.sys_id.replace("'", "\\'")
                is_ox_js = 'true' if is_oxford else 'false'

                with ui.element('div').style(
                    'flex: 0 0 50%; border-radius: 8px; overflow: hidden; '
                    'display: flex; flex-direction: column; min-height: 300px;'
                ):
                    # Zoom/rotate toolbar
                    with ui.row().classes('items-center justify-center gap-1 py-1').style(
                        'background: #222; flex-shrink: 0;'
                    ):
                        ui.button(icon='zoom_in', on_click=lambda u=uid: ui.run_javascript(f'window.__rdZoomIn_{u}()')).props('flat dense size=xs text-color=white').tooltip(tr('Zoom in'))
                        ui.button(icon='zoom_out', on_click=lambda u=uid: ui.run_javascript(f'window.__rdZoomOut_{u}()')).props('flat dense size=xs text-color=white').tooltip(tr('Zoom out'))
                        ui.button(icon='restart_alt', on_click=lambda u=uid: ui.run_javascript(f'window.__rdReset_{u}()')).props('flat dense size=xs text-color=white').tooltip(tr('Reset'))
                        ui.button(icon='rotate_right', on_click=lambda u=uid: ui.run_javascript(f'window.__rdRotate_{u}()')).props('flat dense size=xs text-color=white').tooltip(tr('Rotate'))

                    # Image container
                    with ui.element('div').style(
                        'flex: 1; background: #1a1a1a; overflow: hidden; '
                        'display: flex; align-items: center; justify-content: center;'
                    ):
                        ui.html(f'''
                            <img id="rd-img-{uid}" src="{img_url}"
                                 style="max-height: 400px; max-width: 100%; object-fit: contain; transition: transform 0.2s;"
                                 loading="lazy"
                                 onerror="
                                     if ({is_ox_js}) {{
                                         this.style.display='none';
                                     }} else {{
                                         var ox='/api/oxford_image/{safe_sid}?page={pg_idx}';
                                         if (this.src.indexOf('oxford_image')===-1) {{
                                             this.onerror=function(){{ this.style.display='none'; }};
                                             this.src=ox;
                                         }} else {{
                                             this.style.display='none';
                                         }}
                                     }}
                                 "
                            />
                            <script>
                            (function() {{
                                var zoom = 1.0, rotate = 0;
                                function apply() {{
                                    var el = document.getElementById('rd-img-{uid}');
                                    if (el) el.style.transform = 'scale(' + zoom + ') rotate(' + rotate + 'deg)';
                                }}
                                window.__rdZoomIn_{uid} = function() {{ zoom = Math.min(zoom * 1.2, 5); apply(); }};
                                window.__rdZoomOut_{uid} = function() {{ zoom = Math.max(zoom / 1.2, 0.2); apply(); }};
                                window.__rdReset_{uid} = function() {{ zoom = 1.0; rotate = 0; apply(); }};
                                window.__rdRotate_{uid} = function() {{ rotate = (rotate + 90) % 360; apply(); }};
                            }})();
                            </script>
                        ''', sanitize=False)

                # ---- Text panel (right, 50%) ----
                with ui.element('div').style(
                    'flex: 1; border: 1px solid #e5e7eb; border-radius: 8px; padding: 16px; '
                    'overflow-y: auto; max-height: 400px; background: var(--bg-secondary, #f9fafb);'
                ):
                    # Mutable text container for version switching
                    text_container_ref = {'el': None}

                    # Determine default direction based on sources
                    default_dir = 'rtl'
                    default_text_align = 'right'

                    def make_version_change_handler(tc_ref):
                        def on_version_change(new_text: str, version_info: dict):
                            el = tc_ref.get('el')
                            if not el:
                                return
                            el.clear()
                            with el:
                                # Determine directionality per DEC-10-01-02
                                is_translation = version_info.get('is_translation', False)
                                lang = version_info.get('language', '')
                                if is_translation and lang == 'English':
                                    text_dir = 'ltr'
                                    text_align = 'left'
                                else:
                                    text_dir = 'rtl'
                                    text_align = 'right'

                                if new_text:
                                    ui.label(new_text).style(
                                        f'font-size: 1.2rem; line-height: 1.9; '
                                        f'direction: {text_dir}; text-align: {text_align}; '
                                        f'font-family: "David", "Frank Ruehl", "Noto Sans Hebrew", serif; '
                                        f'white-space: pre-wrap; color: var(--text-primary);'
                                    )
                                else:
                                    ui.label(tr('No text available')).classes('italic text-gray-400')
                        return on_version_change

                    # Version selector row
                    with ui.row().classes('items-center gap-2 mb-2'):
                        # Build PGP transcription info for backward compat
                        pgp_trans = None
                        if entry.pgp_doc:
                            pgp_trans = {
                                'content': entry.pgp_doc.get('transcription', ''),
                                'attribution': entry.pgp_doc.get('transcription_source', ''),
                                'pgp_url': entry.pgp_doc.get('pgp_url', ''),
                                'pgpid': entry.pgp_doc.get('pgpid'),
                            }

                        create_version_selector(
                            document_id=entry.sys_id,
                            page_number=pg_num,
                            original_text=pg_text,
                            on_version_change=make_version_change_handler(text_container_ref),
                            size='sm',
                            pgp_transcription=pgp_trans,
                            all_sources=entry.sources if entry.sources else None,
                        )

                    # Default text display
                    text_container = ui.column().classes('w-full')
                    text_container_ref['el'] = text_container

                    with text_container:
                        if pg_text:
                            ui.label(pg_text).style(
                                f'font-size: 1.2rem; line-height: 1.9; '
                                f'direction: {default_dir}; text-align: {default_text_align}; '
                                f'font-family: "David", "Frank Ruehl", "Noto Sans Hebrew", serif; '
                                f'white-space: pre-wrap; color: var(--text-primary);'
                            )
                        else:
                            ui.label(tr('No text available')).classes('italic text-gray-400')

    def _render_full_transcription(pgpid: int):
        """Render the full PGP transcription section at the bottom."""
        full_text = get_transcription_for_document(pgpid)
        if not full_text:
            return

        ui.separator().classes('my-4')
        with ui.row().classes('items-center gap-2 mb-2'):
            ui.icon('text_snippet', size='xs').classes('text-teal-600')
            ui.label(tr('Full PGP Transcription')).classes('text-sm font-bold').style('color: var(--text-primary);')
            ui.badge('PGP', color='blue').props('outline dense').classes('text-xs')
        ui.html(f'''
            <div dir="rtl" style="
                white-space: pre-wrap;
                font-family: 'SBL Hebrew', 'Frank Ruehl CLM', 'Ezra SIL', serif;
                font-size: 1.1rem;
                line-height: 1.8;
                padding: 12px;
                background: var(--bg-secondary, #f9fafb);
                border-radius: 8px;
                border: 1px solid #e5e7eb;
                color: var(--text-primary);
            ">{full_text}</div>
        ''', sanitize=False)

    # ========================================================================
    # Populate the desk based on entry point
    # ========================================================================

    def _populate_desk():
        """Populate the reading desk based on initial parameters."""
        service = get_service()

        # VIEW-01: From pgpid
        if initial_pgpid:
            fragments = get_fragments_for_document(initial_pgpid)
            desk_state.pgpid = initial_pgpid
            desk_state.source_description = f'{tr("Document")} #{initial_pgpid}'

            # Get document metadata for PGP info
            doc_meta = get_document_metadata(initial_pgpid)

            seen_sys_ids = set()
            for idx, frag in enumerate(fragments):
                frag_sid = frag.get('sys_id', '')
                if not frag_sid or frag_sid in seen_sys_ids:
                    continue
                seen_sys_ids.add(frag_sid)
                entry = _load_entry_from_sys_id(frag_sid, sequence_order=idx)
                if entry:
                    entry.pgpid = initial_pgpid
                    entry.pgp_doc = doc_meta
                    desk_state.entries.append(entry)

        # VIEW-02: From sys_ids
        elif initial_sys_ids:
            desk_state.source_description = tr('Custom selection')
            for idx, sid in enumerate(initial_sys_ids):
                sid = sid.strip()
                if not sid:
                    continue
                entry = _load_entry_from_sys_id(sid, sequence_order=idx)
                if entry:
                    desk_state.entries.append(entry)

        # VIEW-03: From list_id
        elif initial_list_id:
            desk_state.source_description = tr('From list')
            if app_state.lists_mgr:
                items = app_state.lists_mgr.get_items_in_list_sync(initial_list_id)
                # Try to get list name
                lists_data = app_state.lists_mgr.data.get('lists', {})
                list_info = lists_data.get(initial_list_id, {})
                list_name = list_info.get('name', initial_list_id)
                desk_state.source_description = f'{tr("From list")}: {list_name}'

                for idx, item in enumerate(items):
                    sid = item.get('sys_id', '')
                    if not sid:
                        continue
                    entry = _load_entry_from_sys_id(sid, sequence_order=idx)
                    if entry:
                        desk_state.entries.append(entry)

        _render_display()

    # Schedule population after page renders
    ui.timer(0.1, _populate_desk, once=True)
