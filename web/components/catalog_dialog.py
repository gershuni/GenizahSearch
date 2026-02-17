# -*- coding: utf-8 -*-
"""
Catalog Records Dialog Component

Displays all FJMS scholarly catalog descriptions for a manuscript,
grouped by source name, with language-aware TextualFrame rendering.

Uses purple gradient header (FJMS brand color) and the 'description' icon.
"""

import html
from itertools import groupby

from nicegui import ui
from web.translations import tr, get_language
from shared.fjms_service import split_textual_frames, parse_textual_frame


def create_catalog_records_dialog(
    records: list[dict],
    sys_id: str,
    shelfmark: str = "",
) -> object:
    """
    Create catalog records dialog with source-grouped descriptions.

    Args:
        records: Raw catalog record dicts from FjmsService.get_catalog_records().
        sys_id: System ID for KTIV link.
        shelfmark: Display shelfmark for the header.

    Returns:
        The dialog object.
    """
    dialog = ui.dialog().props('maximized=false full-width')

    with dialog, ui.card().classes('w-full max-w-[900px] max-h-[90vh]').style(
        'overflow: hidden; display: flex; flex-direction: column;'
    ):
        # Header with purple gradient (FJMS brand)
        with ui.row().classes('w-full items-center justify-between p-3 rounded-t').style(
            'background: linear-gradient(135deg, #6c3483, #9b59b6); color: white;'
        ):
            with ui.row().classes('items-center gap-2'):
                ui.icon('description').classes('text-xl')
                title_text = f'{tr("Catalog Records")}'
                if shelfmark:
                    title_text += f' \u2014 {shelfmark}'
                ui.label(title_text).classes('text-lg font-bold')

            with ui.row().classes('items-center gap-2'):
                ktiv_url = (
                    f"https://www.nli.org.il/he/discover/manuscripts/hebrew-manuscripts/"
                    f"itempage?vid=KTIV&scope=KTIV&docId=PNX_MANUSCRIPTS{sys_id}"
                )
                ui.link(tr('Open in KTIV'), ktiv_url, new_tab=True).classes(
                    'text-white text-sm px-2 py-1 rounded'
                ).style('background: rgba(255,255,255,0.2); text-decoration: none;')
                ui.button(icon='close', on_click=dialog.close).props(
                    'flat dense round'
                ).classes('text-white')

        # Scrollable content area
        with ui.scroll_area().classes('w-full').style('flex: 1;'):
            lang = get_language()

            # Group entries by source_name
            sorted_records = sorted(records, key=lambda r: r.get('source_name') or '')
            for source_key, group in groupby(
                sorted_records, key=lambda r: r.get('source_name') or ''
            ):
                entries = list(group)

                # Source header: use source_name_heb if lang == 'he', else source_name
                display_source = (
                    entries[0].get('source_name_heb')
                    if lang == 'he'
                    else entries[0].get('source_name')
                )
                if not display_source or not display_source.strip():
                    display_source = (
                        entries[0].get('source_name')
                        or entries[0].get('source_name_heb')
                        or tr('Unknown')
                    )

                count_label = f'({len(entries)})'

                # Source section card
                with ui.card().classes('w-full mb-3').style(
                    'border-left: 3px solid #9b59b6; background: var(--surface-variant, #fafafa);'
                ):
                    # Source header
                    with ui.row().classes('items-center gap-2 p-2').style(
                        'background: rgba(108, 52, 131, 0.08);'
                    ):
                        ui.icon('source').classes('text-sm').style('color: #9b59b6;')
                        ui.label(display_source).classes('font-bold text-sm')
                        ui.label(count_label).classes('text-xs').style(
                            'color: var(--text-tertiary);'
                        )

                    # Render each entry
                    for idx, record in enumerate(entries):
                        if idx > 0:
                            ui.separator().classes('my-1')

                        with ui.column().classes('gap-1 p-2'):
                            # Title (language-aware, bold)
                            title = (
                                record.get('title_heb')
                                if lang == 'he'
                                else record.get('title')
                            )
                            if not title or not title.strip():
                                title = (
                                    record.get('title')
                                    if lang == 'he'
                                    else record.get('title_heb')
                                )
                            if title and title.strip():
                                ui.label(title).classes('font-bold text-sm').style(
                                    'color: var(--text-primary);'
                                )

                            # Author (italic)
                            author = record.get('author_text')
                            if author and author.strip():
                                ui.label(author).classes('text-sm italic').style(
                                    'color: var(--text-secondary);'
                                )

                            # CopyDate + CopyPlace (small, inline, gray)
                            copy_date = record.get('copy_date')
                            copy_place = record.get('copy_place')
                            if copy_date or copy_place:
                                meta_parts = []
                                if copy_date:
                                    meta_parts.append(
                                        f'{tr("Copy Date")}: {copy_date}'
                                    )
                                if copy_place:
                                    meta_parts.append(
                                        f'{tr("Place")}: {copy_place}'
                                    )
                                ui.label(' | '.join(meta_parts)).classes(
                                    'text-xs'
                                ).style('color: var(--text-tertiary);')

                            # TextualFrame content (language-aware with markup)
                            frame_text = (
                                record.get('textual_frame_heb')
                                if lang == 'he'
                                else record.get('textual_frame_eng')
                            )
                            used_fallback = False
                            if not frame_text or not frame_text.strip():
                                frame_text = (
                                    record.get('textual_frame_eng')
                                    if lang == 'he'
                                    else record.get('textual_frame_heb')
                                )
                                used_fallback = True

                            if frame_text and frame_text.strip():
                                # Determine text direction
                                text_dir = 'rtl' if (
                                    (lang == 'he' and not used_fallback)
                                    or (lang != 'he' and used_fallback)
                                ) else 'ltr'

                                # Parse and render TextualFrame entries
                                parts = split_textual_frames(frame_text)
                                if not parts:
                                    parts = [frame_text.strip()]

                                html_parts = []
                                for part in parts:
                                    category, content = parse_textual_frame(part)
                                    escaped_content = html.escape(content)
                                    if category:
                                        escaped_cat = html.escape(category)
                                        html_parts.append(
                                            f'<b style="color:#9b59b6;">'
                                            f'{escaped_cat}:</b> '
                                            f'{escaped_content}'
                                        )
                                    else:
                                        html_parts.append(escaped_content)

                                rendered = '<br>'.join(html_parts)
                                ui.html(rendered, sanitize=False).classes(
                                    'text-sm'
                                ).style(
                                    f'color: var(--text-primary); direction: {text_dir};'
                                )

    return dialog
