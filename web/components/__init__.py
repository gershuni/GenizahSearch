# -*- coding: utf-8 -*-
"""
Shared UI Components for Genizah Search Pro

Provides reusable components for:
- Text editing with local/submit options
- Comment submission
- Version selection
"""

from .text_editor import (
    create_edit_button,
    create_edit_text_dialog,
    get_local_edit,
    get_local_edits,
    save_local_edit,
    delete_local_edit
)

from .comment_dialog import (
    create_comment_button,
    create_comment_dialog
)

from .version_selector import (
    create_version_selector,
    create_version_badge,
    fetch_document_corrections
)

from .notes_display import (
    create_notes_panel,
    create_notes_button,
    fetch_document_comments
)

from .joins_panel import (
    create_joins_button,
    create_joins_dialog,
    create_joins_indicator,
    fetch_connected_fragments,
    invalidate_joins_cache
)

__all__ = [
    'create_edit_button',
    'create_edit_text_dialog',
    'get_local_edit',
    'get_local_edits',
    'save_local_edit',
    'delete_local_edit',
    'create_comment_button',
    'create_comment_dialog',
    'create_version_selector',
    'create_version_badge',
    'fetch_document_corrections',
    'create_notes_panel',
    'create_notes_button',
    'fetch_document_comments',
    'create_joins_button',
    'create_joins_dialog',
    'create_joins_indicator',
    'fetch_connected_fragments',
    'invalidate_joins_cache',
]