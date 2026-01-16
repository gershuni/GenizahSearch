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

__all__ = [
    'create_edit_button',
    'create_edit_text_dialog',
    'get_local_edit',
    'get_local_edits',
    'save_local_edit',
    'delete_local_edit',
    'create_comment_button',
    'create_comment_dialog',
]