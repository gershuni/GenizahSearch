"""The web "Select Manuscript" picker for a shelfmark that matches several records.

Catalogue-shelfmark plan (docs/plans/CATALOGUE_SHELFMARK_FORMS_PLAN.md), row K. The
reading desk, the Joins Lab load box and the Fragment Puzzle took the first match (or
said "not found") when a shelfmark matched several records; they open this instead.
The card list is the one ``browse.py::show_shelfmark_suggestions`` draws (library
badge, LTR shelfmark, RTL title); Browse keeps its own dialog in this step.

Items are plain dicts ``{sys_id, shelfmark, title, library_code}`` -- use
``items_from_manuscripts`` for ``ManuscriptInfo`` rows and ``items_from_options`` for
``resolve_system_by_shelfmark`` options. ``container`` is entered explicitly because
the dialog usually opens after an ``await`` (the lookup runs off the event loop).
Closing the dialog does nothing: that is "cancel", not "not found".
"""
from __future__ import annotations

import inspect
from typing import Any, Callable

from nicegui import ui

from web.components.typography import h3
from web.translations import tr


def items_from_manuscripts(results) -> list[dict]:
    return [{'sys_id': r.sys_id, 'shelfmark': r.shelfmark or '', 'title': r.title or '',
             'library_code': r.library_code or ''} for r in results]


def items_from_options(options, meta_mgr=None) -> list[dict]:
    items = []
    for opt in options:
        code = ''
        if meta_mgr is not None:
            try:
                code = meta_mgr.get_library_for_id(opt['sys_id']) or ''
            except Exception:
                code = ''
        items.append({'sys_id': opt['sys_id'], 'shelfmark': opt.get('shelfmark', ''),
                      'title': opt.get('title', '') or '', 'library_code': code})
    return items


def open_shelfmark_picker(container, items: list[dict], on_pick: Callable[[dict], Any], query: str):
    """Show ``items`` and call ``on_pick(item)`` (awaited if it is async) with the chosen one."""
    from genizah_core import get_library_display

    with container:
        with ui.dialog() as dialog, ui.card().classes('p-4 min-w-96 max-w-lg'):
            with ui.row().classes('w-full items-center justify-between mb-4'):
                h3(tr('Select Manuscript'), classes='text-lg font-semibold m-0')
                ui.button(icon='close', on_click=dialog.close).props('flat round dense')
            ui.label(f"{tr('Multiple matches found for')}: \"{query}\"").classes('mb-4').style(
                'color: var(--text-secondary);')
            with ui.column().classes('w-full gap-1 max-h-80 overflow-y-auto'):
                for item in items:
                    async def choose(it=item):
                        dialog.close()
                        outcome = on_pick(it)
                        if inspect.isawaitable(outcome):
                            await outcome

                    library_short = ''
                    if item.get('library_code'):
                        library_short = get_library_display(item['library_code'], short=True)
                    with ui.card().classes('w-full p-3 cursor-pointer hover:bg-gray-100').style(
                        'background: var(--bg-surface); border: 1px solid var(--border-subtle);'
                    ).on('click', choose).props(f'data-sys-id="{item["sys_id"]}"'):
                        with ui.row().classes('items-center gap-2'):
                            if library_short:
                                ui.badge(library_short).classes('text-xs').style(
                                    'background: var(--primary-100); color: var(--primary-700);')
                            ui.label(item['shelfmark']).classes('font-medium').style(
                                'color: var(--text-primary); direction: ltr; text-align: left;')
                        if item.get('title'):
                            ui.label(item['title']).classes('text-sm').style(
                                'color: var(--text-secondary); direction: rtl; text-align: right;')
    dialog.open()
    return dialog
