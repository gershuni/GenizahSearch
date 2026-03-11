# -*- coding: utf-8 -*-
"""
Translation issue reporting component for GenizahSearch web app.

Provides a small report button and dialog that lets users flag problematic
machine translations. Reports are stored locally in a CSV file for admin review.
"""

import csv
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Optional

from nicegui import ui, app

from web.translations import tr

logger = logging.getLogger(__name__)

# Reports stored alongside other data files
REPORTS_DIR = Path(__file__).resolve().parent.parent.parent / 'reports'
REPORTS_FILE = REPORTS_DIR / 'translation_issue_reports.csv'

REPORT_COLUMNS = [
    'timestamp', 'dataset', 'record_id', 'field_name', 'direction',
    'source_text', 'translated_text', 'issue_type', 'user_notes',
    'user_id', 'status',
]

ISSUE_TYPES = [
    'incorrect',      # Translation is wrong or misleading
    'hallucination',  # Translation contains content not in source
    'copy',           # Translation is a copy of the source text
    'other',          # Other issue
]

ISSUE_TYPE_LABELS = {
    'incorrect': 'The translation is incorrect or misleading',
    'hallucination': 'The translation contains hallucinated content',
    'copy': 'The translation is a copy of the source text',
    'other': 'Other issue',
}


def _ensure_reports_file():
    """Create the reports CSV file with headers if it doesn't exist."""
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    if not REPORTS_FILE.exists():
        with open(REPORTS_FILE, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=REPORT_COLUMNS)
            writer.writeheader()


def save_report(
    dataset: str,
    record_id: str,
    field_name: str,
    direction: str,
    source_text: str,
    translated_text: str,
    issue_type: str,
    user_notes: str = '',
    user_id: str = '',
) -> bool:
    """Save a translation issue report to the local CSV file."""
    try:
        _ensure_reports_file()
        row = {
            'timestamp': datetime.now().isoformat(),
            'dataset': dataset,
            'record_id': str(record_id),
            'field_name': field_name,
            'direction': direction,
            'source_text': (source_text or '')[:500],
            'translated_text': (translated_text or '')[:500],
            'issue_type': issue_type,
            'user_notes': user_notes[:500],
            'user_id': user_id,
            'status': 'open',
        }
        with open(REPORTS_FILE, 'a', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=REPORT_COLUMNS)
            writer.writerow(row)
        logger.info("Translation issue reported: %s/%s/%s", dataset, record_id, issue_type)
        return True
    except Exception as e:
        logger.error("Failed to save translation report: %s", e)
        return False


def create_report_button(
    dataset: str,
    record_id: str,
    field_name: str,
    direction: str,
    source_text: str,
    translated_text: str,
):
    """
    Create a small report button that opens a dialog for reporting translation issues.

    Place this next to a translation badge. It renders as a tiny flag icon.
    """
    def open_report_dialog():
        with ui.dialog() as dlg, ui.card().classes('w-96'):
            ui.label(tr('Report translation issue')).classes('text-lg font-bold')
            ui.label(
                tr('Translation reports help us improve quality')
            ).classes('text-xs').style('color: var(--text-tertiary);')

            ui.separator()

            # Show source and translation preview
            with ui.column().classes('w-full gap-2'):
                ui.label(tr('Original text')).classes('text-xs font-bold').style('color: var(--text-secondary);')
                _src_preview = (source_text or '')[:200]
                if len(source_text or '') > 200:
                    _src_preview += '...'
                ui.label(_src_preview).classes('text-xs').style(
                    'color: var(--text-primary); background: var(--surface-1, #f8f9fa); padding: 6px; border-radius: 4px; white-space: pre-wrap;'
                )

                ui.label(tr('Translation')).classes('text-xs font-bold').style('color: var(--text-secondary);')
                _tgt_preview = (translated_text or '')[:200]
                if len(translated_text or '') > 200:
                    _tgt_preview += '...'
                ui.label(_tgt_preview).classes('text-xs').style(
                    'color: var(--text-primary); background: var(--surface-1, #f8f9fa); padding: 6px; border-radius: 4px; white-space: pre-wrap;'
                )

            ui.separator()

            # Issue type selector
            ui.label(tr('Issue type')).classes('text-xs font-bold').style('color: var(--text-secondary);')
            issue_select = ui.select(
                options={k: tr(v) for k, v in ISSUE_TYPE_LABELS.items()},
                value='incorrect',
            ).classes('w-full')

            # Notes
            notes_input = ui.textarea(
                label=tr('Please describe the issue'),
            ).classes('w-full').props('outlined dense rows=2')

            # Submit button
            with ui.row().classes('w-full justify-end gap-2 mt-2'):
                ui.button(tr('Cancel'), on_click=dlg.close).props('flat')

                def submit():
                    user_id = ''
                    try:
                        user_id = app.storage.user.get('user_id', '')
                    except Exception:
                        pass
                    success = save_report(
                        dataset=dataset,
                        record_id=record_id,
                        field_name=field_name,
                        direction=direction,
                        source_text=source_text,
                        translated_text=translated_text,
                        issue_type=issue_select.value,
                        user_notes=notes_input.value or '',
                        user_id=user_id,
                    )
                    dlg.close()
                    if success:
                        ui.notify(tr('Translation issue reported. Thank you!'), type='positive')
                    else:
                        ui.notify(tr('Error submitting report'), type='negative')

                ui.button(tr('Submit report'), on_click=submit).props('color=primary')

        dlg.open()

    btn = ui.button(icon='flag', on_click=open_report_dialog).props(
        'flat round dense size=xs'
    ).classes('opacity-40 hover:opacity-100').style(
        'min-width: 18px; min-height: 18px; padding: 0;'
    ).tooltip(tr('Report an issue with this translation'))

    return btn
