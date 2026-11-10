# -*- coding: utf-8 -*-
"""
Bibliography Dialog Components

Two independent dialogs for bibliography data:
- FJMS: Structured table from fjms_enrichment.db sidecar
- NLI: MARC 581 reference strings with parsed Hebrew annotations

Uses T/S qualifier symbols following the FJMS website pattern:
Full → ✓+, Partial → ✓−, Exists → ✓, None → (empty).
"""

import html as html_mod

from nicegui import ui
from web.translations import tr, get_language
from shared.fjms_service import format_page_ref, _parse_marc_annotations, strip_marc_annotation_suffix, _ts_symbol
from shared.synthetic_sys_id import is_synthetic_sys_id
from typing import List, Dict


def create_fjms_bibliography_dialog(
    fjms_entries: List[Dict],
    sys_id: str,
    shelfmark: str = "",
):
    """
    Create FJMS bibliography dialog with structured table.

    Args:
        fjms_entries: Raw FJMS bibliography entries from get_bibliography().
        sys_id: System ID for KTIV link.
        shelfmark: Display shelfmark for the header.

    Returns:
        The dialog object.
    """
    dialog = ui.dialog().props('maximized=false full-width')

    with dialog, ui.card().classes('w-full max-w-[1200px] max-h-[90vh]').style('overflow: hidden; display: flex; flex-direction: column;'):
        # Header with green gradient
        with ui.row().classes('w-full items-center justify-between p-3 rounded-t').style(
            'background: linear-gradient(135deg, #1a7a4c, #2d9b6e); color: white;'
        ):
            with ui.row().classes('items-center gap-2'):
                ui.icon('menu_book').classes('text-xl')
                ui.label(f'{tr("Bibliography FJMS")} ({len(fjms_entries)})').classes('text-lg font-bold')
                if shelfmark:
                    ui.label(f'\u2014 {shelfmark}').classes('text-sm opacity-80')

            with ui.row().classes('items-center gap-2'):
                # Phase 85 D-06: synthetic sys_ids hide the KTIV link
                if sys_id and not is_synthetic_sys_id(sys_id):
                    ktiv_url = f"https://www.nli.org.il/he/discover/manuscripts/hebrew-manuscripts/itempage?vid=KTIV&scope=KTIV&docId=PNX_MANUSCRIPTS{sys_id}"
                    ui.link(tr('Open in KTIV'), ktiv_url, new_tab=True).classes(
                        'text-white text-sm px-2 py-1 rounded'
                    ).style('background: rgba(255,255,255,0.2); text-decoration: none;')
                ui.button(icon='close', on_click=dialog.close).props('flat dense round').classes('text-white')

        # Filter row
        with ui.row().classes('items-center gap-2 flex-wrap'):
            filter_text = ui.input(placeholder=tr('Filter by author, title...')).classes('w-64').props('dense clearable outlined')
            type_options = {'All': tr('All'), 'Discussion': tr('Discussion'), 'Mentioned': tr('Mentioned'), 'Index': tr('Index')}
            type_filter = ui.select(type_options, value='All', label=tr('Type')).classes('w-36').props('dense outlined')
            chk_trans = ui.checkbox(tr('Has Transcription')).props('dense')
            chk_transl = ui.checkbox(tr('Has Translation')).props('dense')

        columns = [
            {'name': 'author', 'label': tr('Author'), 'field': 'author', 'sortable': True, 'align': 'left'},
            {'name': 'article_title', 'label': tr('Article/Title'), 'field': 'article_title', 'sortable': True, 'align': 'left'},
            {'name': 'year', 'label': tr('Year'), 'field': 'year', 'sortable': True, 'align': 'center'},
            {'name': 'vol', 'label': tr('Vol.'), 'field': 'vol', 'sortable': True, 'align': 'center'},
            {'name': 'pages', 'label': tr('Pages'), 'field': 'pages', 'sortable': True, 'align': 'center'},
            {'name': 'type', 'label': tr('Type'), 'field': 'type', 'sortable': True, 'align': 'center'},
            {'name': 't', 'label': tr('col_T'), 'field': 't', 'sortable': True, 'align': 'center', 'headerClasses': 'cursor-help'},
            {'name': 's', 'label': tr('col_S'), 'field': 's', 'sortable': True, 'align': 'center', 'headerClasses': 'cursor-help'},
        ]

        def _build_rows(entries, indices=None):
            is_heb = get_language() == 'he'
            rows = []
            for idx, e in enumerate(entries):
                row_id = indices[idx] if indices else idx
                if is_heb:
                    author = (e.get('article_author_heb') or e.get('article_author_eng') or '').strip()
                else:
                    author = (e.get('article_author_eng') or e.get('article_author_heb') or '').strip()
                article_name = (e.get('article_name') or '').strip()
                if is_heb:
                    running_title = (e.get('running_title_heb') or e.get('running_title')
                                     or e.get('title_acronym_heb') or e.get('title_acronym') or '').strip()
                else:
                    running_title = (e.get('running_title') or e.get('title_acronym') or '').strip()
                display_title = article_name if article_name else running_title
                year = str(e.get('title_year') or '').strip()
                vol = str(e.get('volume') or '').strip()
                pages = format_page_ref(e)
                mt = (e.get('mention_type') or '').strip()
                rows.append({
                    'id': row_id,
                    'author': author,
                    'article_title': display_title,
                    'year': year if year and year != 'None' else '',
                    'vol': vol if vol and vol != 'None' else '',
                    'pages': pages,
                    'type': tr(mt) if mt and mt != 'None' else '',
                    't': _ts_symbol(e.get('transcription_type')),
                    's': _ts_symbol(e.get('translation_type')),
                })
            return rows

        rows = _build_rows(fjms_entries)

        table = ui.table(
            columns=columns,
            rows=rows,
            row_key='id',
            pagination={'rowsPerPage': 50},
        ).classes('w-full').style('flex: 1; overflow: auto;').props(
            f'''rows-per-page-label="{tr('Records per page')}"'''
        )
        # Header tooltips for abbreviated columns
        table.add_slot('header-cell-t', f'''
            <q-th :props="props"><q-tooltip>{tr("Transcription")}</q-tooltip>{{{{props.col.label}}}}</q-th>
        ''')
        table.add_slot('header-cell-s', f'''
            <q-th :props="props"><q-tooltip>{tr("Translation")}</q-tooltip>{{{{props.col.label}}}}</q-th>
        ''')

        # Detail panel
        detail_label = ui.html('', sanitize=False).classes('text-sm p-2 w-full').style(
            'background: var(--surface-variant, #f5f5f5); border-radius: 4px; '
            'min-height: 3em; max-height: 200px; overflow-y: auto; white-space: pre-wrap; '
            'flex-shrink: 0; word-break: break-word;'
        )
        detail_label.visible = False

        def _esc(val):
            """Escape a value for safe HTML display."""
            s = (val or '').strip()
            return html_mod.escape(s) if s and s != 'None' else ''

        def on_row_click(e):
            row_data = e.args[1] if isinstance(e.args, (list, tuple)) and len(e.args) > 1 else e.args
            if isinstance(row_data, dict):
                row_id = row_data.get('id', -1)
            else:
                detail_label.visible = False
                return
            if 0 <= row_id < len(fjms_entries):
                entry = fjms_entries[row_id]
                parts = []
                article = _esc(entry.get('article_name'))
                if article:
                    parts.append(f'{tr("Article")}: {article}')
                author_heb = _esc(entry.get('article_author_heb'))
                if author_heb:
                    parts.append(f'{tr("Author")}: {author_heb}')
                tt = (entry.get('transcription_type') or '').strip()
                if tt and tt not in ('', 'None'):
                    parts.append(f'{tr("Transcription")}: {tr(tt)}')
                tl = (entry.get('translation_type') or '').strip()
                if tl and tl not in ('', 'None'):
                    parts.append(f'{tr("Translation")}: {tr(tl)}')
                cat = _esc(entry.get('catalog_acronym'))
                if cat:
                    parts.append(f'{tr("Catalog")}: {cat}')
                # Extended fields
                evol = _esc(entry.get('e_volume'))
                if evol:
                    parts.append(f'{tr("Vol.")} (EN): {evol}')
                jdate = _esc(entry.get('journal_date'))
                if jdate:
                    parts.append(f'{tr("Date")}: {jdate}')
                cat_entry = _esc(entry.get('catalog_entry'))
                if cat_entry:
                    parts.append(f'{tr("Catalog")} #: {cat_entry}')
                comment = _esc(entry.get('comment'))
                if comment:
                    parts.append(f'{tr("Comment")}: {comment}')
                note = _esc(entry.get('note_for_display'))
                if note:
                    parts.append(f'{tr("Note")}: {note}')
                if parts:
                    detail_label.content = '<br>'.join(parts)
                    detail_label.visible = True
                else:
                    detail_label.visible = False
            else:
                detail_label.visible = False

        table.on('rowClick', on_row_click)

        # Filters
        skip_vals = {'', 'None', 'Unknown'}

        def apply_filters():
            text_val = (filter_text.value or '').strip().lower()
            type_val = type_filter.value
            need_trans = chk_trans.value
            need_transl = chk_transl.value

            filtered_entries = []
            filtered_indices = []
            for idx, e in enumerate(fjms_entries):
                mt = (e.get('mention_type') or '').strip()
                if type_val and type_val != 'All':
                    if mt != type_val:
                        continue
                if need_trans:
                    tt = (e.get('transcription_type') or '').strip()
                    if not tt or tt in skip_vals:
                        continue
                if need_transl:
                    tl = (e.get('translation_type') or '').strip()
                    if not tl or tl in skip_vals:
                        continue
                if text_val:
                    searchable = ' '.join([
                        e.get('article_author_eng') or '',
                        e.get('article_author_heb') or '',
                        e.get('article_name') or '',
                        e.get('running_title') or '',
                        e.get('running_title_heb') or '',
                        e.get('title_acronym') or '',
                        e.get('title_acronym_heb') or '',
                    ]).lower()
                    if text_val not in searchable:
                        continue
                filtered_entries.append(e)
                filtered_indices.append(idx)
            table.rows = _build_rows(filtered_entries, filtered_indices)

        filter_text.on('update:model-value', lambda _: apply_filters())
        type_filter.on('update:model-value', lambda _: apply_filters())
        chk_trans.on('update:model-value', lambda _: apply_filters())
        chk_transl.on('update:model-value', lambda _: apply_filters())

    return dialog


def create_nli_bibliography_dialog(
    marc_strings: List[str],
    sys_id: str,
    shelfmark: str = "",
):
    """
    Create NLI bibliography dialog with MARC 581 reference strings.

    Args:
        marc_strings: Raw MARC 581 bibliography strings.
        sys_id: System ID for KTIV link.
        shelfmark: Display shelfmark for the header.

    Returns:
        The dialog object.
    """
    # Pre-parse all MARC strings
    parsed_entries = []
    for marc_str in marc_strings:
        ann = _parse_marc_annotations(marc_str)
        ref = strip_marc_annotation_suffix(marc_str)
        parsed_entries.append({
            'reference': ref,
            'raw': marc_str,
            'mention_type': ann.get('mention_type', ''),
            'has_image': ann.get('has_image', False),
            'transcription': ann.get('transcription', ''),
            'translation': ann.get('translation', ''),
        })

    dialog = ui.dialog().props('maximized=false full-width')

    with dialog, ui.card().classes('w-full max-w-[1200px] max-h-[90vh]').style('overflow: hidden; display: flex; flex-direction: column;'):
        # Header with blue gradient
        with ui.row().classes('w-full items-center justify-between p-3 rounded-t').style(
            'background: linear-gradient(135deg, #1a5276, #2980b9); color: white;'
        ):
            with ui.row().classes('items-center gap-2'):
                ui.icon('menu_book').classes('text-xl')
                ui.label(f'{tr("Bibliography Ktiv")} ({len(marc_strings)})').classes('text-lg font-bold')
                if shelfmark:
                    ui.label(f'\u2014 {shelfmark}').classes('text-sm opacity-80')

            with ui.row().classes('items-center gap-2'):
                # Phase 85 D-06: synthetic sys_ids hide the KTIV link.
                # (This dialog should never even open for synthetic since
                # marc_bib is empty per browse_enrichment.py:503 guard;
                # kept as defense-in-depth.)
                if sys_id and not is_synthetic_sys_id(sys_id):
                    ktiv_url = f"https://www.nli.org.il/he/discover/manuscripts/hebrew-manuscripts/itempage?vid=KTIV&scope=KTIV&docId=PNX_MANUSCRIPTS{sys_id}"
                    ui.link(tr('Open in KTIV'), ktiv_url, new_tab=True).classes(
                        'text-white text-sm px-2 py-1 rounded'
                    ).style('background: rgba(255,255,255,0.2); text-decoration: none;')
                ui.button(icon='close', on_click=dialog.close).props('flat dense round').classes('text-white')

        # Filter row
        with ui.row().classes('items-center gap-2 flex-wrap'):
            filter_text = ui.input(placeholder=tr('Filter references...')).classes('w-64').props('dense clearable outlined')
            type_options = {'All': tr('All'), 'Discussion': tr('Discussion'), 'Mentioned': tr('Mentioned'), 'Index': tr('Index')}
            type_filter = ui.select(type_options, value='All', label=tr('Type')).classes('w-36').props('dense outlined')
            chk_trans = ui.checkbox(tr('Has Transcription')).props('dense')
            chk_transl = ui.checkbox(tr('Has Translation')).props('dense')
            chk_image = ui.checkbox(tr('Has Image')).props('dense')

        columns = [
            {'name': 'reference', 'label': tr('Reference'), 'field': 'reference', 'sortable': True, 'align': 'left'},
            {'name': 'd', 'label': tr('col_D'), 'field': 'd', 'sortable': True, 'align': 'center', 'headerClasses': 'cursor-help'},
            {'name': 't', 'label': tr('col_T'), 'field': 't', 'sortable': True, 'align': 'center', 'headerClasses': 'cursor-help'},
            {'name': 's', 'label': tr('col_S'), 'field': 's', 'sortable': True, 'align': 'center', 'headerClasses': 'cursor-help'},
            {'name': 'i', 'label': tr('col_I'), 'field': 'i', 'sortable': True, 'align': 'center', 'headerClasses': 'cursor-help'},
        ]

        def _build_rows(entries, indices=None):
            rows = []
            for idx, pe in enumerate(entries):
                row_id = indices[idx] if indices else idx
                rows.append({
                    'id': row_id,
                    'reference': pe['reference'],
                    'd': '\u2713' if pe['mention_type'] == 'Discussion' else '',
                    't': _ts_symbol(pe['transcription']),
                    's': _ts_symbol(pe['translation']),
                    'i': '\u2713' if pe['has_image'] else '',
                })
            return rows

        rows = _build_rows(parsed_entries)

        table = ui.table(
            columns=columns,
            rows=rows,
            row_key='id',
            pagination={'rowsPerPage': 50},
        ).classes('w-full').style('flex: 1; overflow: auto;').props(
            f'''rows-per-page-label="{tr('Records per page')}"'''
        )
        # Header tooltips for abbreviated columns
        for col_name, tooltip in [('d', tr('Discussion')), ('t', tr('Transcription')),
                                   ('s', tr('Translation')), ('i', tr('Image'))]:
            table.add_slot(f'header-cell-{col_name}', f'''
                <q-th :props="props"><q-tooltip>{tooltip}</q-tooltip>{{{{props.col.label}}}}</q-th>
            ''')

        # Detail panel
        detail_label = ui.html('', sanitize=False).classes('text-sm p-2 w-full').style(
            'background: var(--surface-variant, #f5f5f5); border-radius: 4px; '
            'min-height: 3em; max-height: 200px; overflow-y: auto; white-space: pre-wrap; '
            'flex-shrink: 0; word-break: break-word;'
        )
        detail_label.visible = False

        def on_row_click(e):
            row_data = e.args[1] if isinstance(e.args, (list, tuple)) and len(e.args) > 1 else e.args
            if isinstance(row_data, dict):
                row_id = row_data.get('id', -1)
            else:
                detail_label.visible = False
                return
            if 0 <= row_id < len(parsed_entries):
                pe = parsed_entries[row_id]
                parts = [pe['raw']]
                details = []
                if pe['mention_type']:
                    details.append(tr(pe['mention_type']))
                if pe['transcription']:
                    details.append(f'{tr("Transcription")}: {tr(pe["transcription"])}')
                if pe['translation']:
                    details.append(f'{tr("Translation")}: {tr(pe["translation"])}')
                if pe['has_image']:
                    details.append(tr('Has Image'))
                if details:
                    parts.append(', '.join(details))
                detail_label.text = '\n'.join(parts)
                detail_label.visible = True
            else:
                detail_label.visible = False

        table.on('rowClick', on_row_click)

        # Filters
        skip_vals = {'', 'None', 'Unknown'}

        def apply_filters():
            text_val = (filter_text.value or '').strip().lower()
            type_val = type_filter.value
            need_trans = chk_trans.value
            need_transl = chk_transl.value
            need_image = chk_image.value

            filtered = []
            filtered_indices = []
            for idx, pe in enumerate(parsed_entries):
                if type_val and type_val != 'All':
                    if pe['mention_type'] != type_val:
                        continue
                if need_trans:
                    if not pe['transcription'] or pe['transcription'] in skip_vals:
                        continue
                if need_transl:
                    if not pe['translation'] or pe['translation'] in skip_vals:
                        continue
                if need_image:
                    if not pe['has_image']:
                        continue
                if text_val:
                    if text_val not in pe['reference'].lower() and text_val not in pe['raw'].lower():
                        continue
                filtered.append(pe)
                filtered_indices.append(idx)
            table.rows = _build_rows(filtered, filtered_indices)

        filter_text.on('update:model-value', lambda _: apply_filters())
        type_filter.on('update:model-value', lambda _: apply_filters())
        chk_trans.on('update:model-value', lambda _: apply_filters())
        chk_transl.on('update:model-value', lambda _: apply_filters())
        chk_image.on('update:model-value', lambda _: apply_filters())

    return dialog
