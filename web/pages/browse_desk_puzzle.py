# -*- coding: utf-8 -*-
"""Reading Desk, image side, third view: the assembled picture of a join.

The Fragment Puzzle is where fragments are arranged; this view only shows the picture of an
arrangement that already exists for the fragments on the desk, and sends the reader to the
Puzzle to make or change one. Two kinds are looked for:

* the visitor's own saved joins (``web/saved_joins.py``: each visitor sees only their own),
  whose picture the browser asks of ``POST /api/puzzle_export`` -- the Puzzle's own export
  route, so the picture is the one the Puzzle would save;
* published joins (Supabase), which carry their picture already.

A join counts when it holds at least two of the desk's fragments (one, on a desk of one).
The search runs off the event loop: the published half is a network call.
"""
from __future__ import annotations

import asyncio
import html as html_module
import json
import logging
from dataclasses import asdict
from typing import Any, Callable, Dict, List, Optional

from nicegui import run, ui

from web.translations import tr

logger = logging.getLogger(__name__)

VIEWER_ID = 'rd-viewer-join'
KEY_JOIN_SHOWN = '__join_shown__'  # in state.reading_desk_selected_sources


def _own_joins(joins: Any, sys_ids: List[str]) -> List[Dict[str, Any]]:
    found: Dict[str, Dict[str, Any]] = {}
    for sys_id in sys_ids:
        for doc_id in joins.list_documents_for_fragment(sys_id=sys_id):
            if doc_id in found:
                continue
            doc = joins.load_document(doc_id)
            if doc is None or not doc.fragments:
                continue
            held = {f.sys_id for f in doc.fragments}
            found[doc_id] = {
                'kind': 'own', 'id': doc.id, 'title': doc.title or '', 'when': doc.updated_at or '',
                'covers': len(held & set(sys_ids)), 'fragments': [asdict(f) for f in doc.fragments],
            }
    return list(found.values())


def _published_joins(client: Any, sys_ids: List[str]) -> List[Dict[str, Any]]:
    from shared.puzzle_publish_service import get_published_join_detail, get_published_joins_for_fragment
    seen: Dict[str, int] = {}
    rows: Dict[str, Dict[str, Any]] = {}
    for sys_id in sys_ids:
        for row in get_published_joins_for_fragment(client, sys_id):
            seen[row['id']] = seen.get(row['id'], 0) + 1
            rows[row['id']] = row
    found = []
    for join_id, row in rows.items():
        detail = get_published_join_detail(client, join_id) or {}
        if not detail.get('is_published') or not detail.get('image_url'):
            continue
        found.append({
            'kind': 'published', 'id': join_id, 'title': row.get('title') or '', 'when': row.get('created_at') or '',
            'covers': seen[join_id], 'author': row.get('author_name') or '', 'image_url': detail['image_url'],
        })
    return found


def find_joins(sys_ids: List[str], own: Any = None, client: Any = None) -> List[Dict[str, Any]]:
    """The joins that can be shown for a desk holding ``sys_ids``, best first: the one
    holding most of the desk's fragments, the visitor's own before a published one, newest
    first. BLOCKING. ``own``: the visitor's ``SavedJoins`` (None: not looked in);
    ``client``: a Supabase client (None: published joins not looked for). Either half may
    fail without taking the other with it.
    """
    sys_ids = [s for s in dict.fromkeys(sys_ids) if s]
    found: List[Dict[str, Any]] = []
    if own is not None:
        try:
            found += _own_joins(own, sys_ids)
        except Exception:
            logger.exception('[ReadingDesk] saved joins lookup')
    if client is not None:
        try:
            own_ids = {j['id'] for j in found}
            found += [j for j in _published_joins(client, sys_ids) if j['id'] not in own_ids]
        except Exception as e:
            logger.info('[ReadingDesk] published joins lookup failed: %s', e)
    needed = min(2, len(sys_ids))
    found = [j for j in found if j['covers'] >= needed]
    # Stable sorts, least important key first.
    found.sort(key=lambda j: j['when'], reverse=True)
    found.sort(key=lambda j: j['kind'] != 'own')
    found.sort(key=lambda j: j['covers'], reverse=True)
    return found


def join_label(join: Dict[str, Any]) -> str:
    whose = tr('Your saved join') if join['kind'] == 'own' else tr('Published by {}').format(join.get('author') or '?')
    return f"{join['title']} · {whose}" if join['title'] else whose


def own_picture_js(join: Dict[str, Any], failed: str, too_many: str = '') -> str:
    """Asks the Puzzle's export route for the picture of the visitor's own join and shows it.
    Kept for the page's life under the join's id and save time, so coming back is instant.

    The route refuses a join of more fragments than it will compose for an outside caller
    (413): that is said as what it is (``too_many``), not as a failure -- the join is whole
    and opens in the Puzzle."""
    key = json.dumps(f"{join['id']}@{join['when']}")
    return (
        "(async function(){"
        f"var img=document.getElementById('{VIEWER_ID}'),note=document.getElementById('{VIEWER_ID}-note');"
        "if(!img)return;"
        "try{"
        "window.rdJoinPictures=window.rdJoinPictures||{};"
        f"var url=window.rdJoinPictures[{key}];"
        "if(!url){"
        "var r=await fetch('/api/puzzle_export',{method:'POST',headers:{'Content-Type':'application/json'},"
        f"body:JSON.stringify({{fragments:{json.dumps(join['fragments'])}}})}});"
        f"if(r.status===413){{if(note)note.textContent={json.dumps(too_many or failed)};return;}}"
        "if(!r.ok)throw new Error('export '+r.status);"
        f"url=URL.createObjectURL(await r.blob());window.rdJoinPictures[{key}]=url;}}"
        "img.src=url;img.style.display='';if(note)note.style.display='none';"
        f"if(window.rdInitDrag)window.rdInitDrag('{VIEWER_ID}');"
        f"}}catch(e){{if(note)note.textContent={json.dumps(failed)};}}"
        "})();"
    )


def render_join_view(entries: List[Dict[str, Any]], state: Any, *, toolbar: Callable[[str], None],
                     on_arrange: Callable[[], None], on_choice: Optional[Callable[[], None]] = None) -> None:
    """The assembled picture, or the way to make one. ``toolbar(viewer_id)`` draws the image
    controls; ``on_arrange`` takes the desk's fragments to the Fragment Puzzle."""
    selected = state.reading_desk_selected_sources
    sys_ids = [e.get('sys_id') for e in entries if e.get('sys_id')]
    box = ui.column().classes('rd-join w-full').style('flex: 1; min-height: 0; gap: 4px;')
    with box:
        with ui.row().classes('rd-join-wait items-center w-full justify-center').style('gap: 8px; padding: 24px 0;'):
            ui.spinner(size='sm', color='green')
            ui.label(tr('Looking for a saved join of these fragments…')).classes('text-sm').style(
                'color: var(--text-secondary);')

    # The visitor is told apart here, in the page's context; the worker thread cannot.
    own = None
    try:
        from web.saved_joins import for_current_visitor
        own = for_current_visitor()
    except Exception as e:
        logger.debug('[ReadingDesk] no visitor key for saved joins: %s', e)
    client = None
    try:
        from web.supabase_client import get_client
        client = get_client()
    except Exception as e:
        logger.debug('[ReadingDesk] no Supabase client for published joins: %s', e)

    def draw(joins: List[Dict[str, Any]]) -> None:
        box.clear()
        with box:
            if not joins:
                with ui.column().classes('rd-join-none items-center w-full').style('gap: 8px; padding: 24px 8px;'):
                    ui.icon('extension', size='lg').classes('text-green-600')
                    ui.label(tr('No saved join of these fragments yet.')).classes('text-sm').style(
                        'color: var(--text-secondary); text-align: center;')
                    ui.button(tr('Arrange in Puzzle'), icon='extension', on_click=on_arrange).props(
                        'unelevated no-caps color=green size=sm')
                return
            join = next((j for j in joins if j['id'] == selected.get(KEY_JOIN_SHOWN)), joins[0])

            def choose_join(e) -> None:
                selected[KEY_JOIN_SHOWN] = e.value
                if on_choice:
                    on_choice()
                draw(joins)

            with ui.row().classes('rd-join-bar items-center w-full').style('gap: 2px 6px;'):
                if len(joins) > 1:
                    ui.select({j['id']: join_label(j) for j in joins}, value=join['id'], on_change=choose_join).props(
                        'dense outlined options-dense').classes('rd-join-choice text-sm').style('min-width: 0; flex: 1 1 160px;')
                else:
                    ui.label(join_label(join)).classes('rd-join-name text-sm font-bold').style(
                        'color: var(--text-primary); min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;')
                ui.element('div').classes('flex-grow')
                toolbar(VIEWER_ID)
                ui.button(tr('Open in Puzzle'), icon='extension',
                          on_click=lambda j=join: ui.navigate.to(f"/puzzle?doc={j['id']}")).props(
                    'flat dense no-caps size=sm color=green').classes('rd-join-open')
            src = html_module.escape(join.get('image_url', ''), quote=True)
            waiting = '' if join['kind'] == 'published' else html_module.escape(tr('Assembling the picture…'))
            with ui.element('div').classes('rd-image-box').style(
                    'background: #1a1a1a; border-radius: 6px; overflow: hidden; display: flex; align-items: center; '
                    'justify-content: center; min-height: calc(70vh - 96px); position: relative; width: 100%;'):
                ui.html(
                    f'<span id="{VIEWER_ID}-note" style="color: #e5e7eb; font-size: 0.85rem; position: absolute;">{waiting}</span>'
                    f'<img id="{VIEWER_ID}" class="rd-zoomable" {"src=" + chr(34) + src + chr(34) if src else ""} '
                    f'style="max-height: calc(70vh - 96px); max-width: 100%; object-fit: contain; cursor: grab; '
                    f'transition: none;{"" if src else " display: none;"}" draggable="false" />', sanitize=False)
            # Every join is drawn under the one viewer id: forget the view kept for the
            # last one, or this picture's first zoom or drag jumps to where that one was left.
            ui.run_javascript(f"window.rdResetView && window.rdResetView('{VIEWER_ID}');")
            if join['kind'] == 'own':
                ui.run_javascript(own_picture_js(
                    join, tr('The picture could not be assembled.'),
                    tr('This join has too many fragments to show here. Open it in the Puzzle.')))
            else:
                ui.run_javascript(
                    f"setTimeout(function(){{ window.rdInitDrag && window.rdInitDrag('{VIEWER_ID}'); }}, 200);")

    async def load() -> None:
        try:
            joins = await run.io_bound(find_joins, sys_ids, own, client)
        except Exception:
            logger.exception('[ReadingDesk] join lookup')
            joins = []
        try:
            if box.is_deleted:
                return
            draw(joins or [])
        except RuntimeError:
            pass  # the desk was redrawn, or the visitor left, while the lookup ran

    asyncio.ensure_future(load())
