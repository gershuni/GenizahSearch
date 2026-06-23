# -*- coding: utf-8 -*-
"""
Compatibility shim for the relocated Shared Export Utilities.

SEED-018 (#44/M5): the implementation moved to ``shared/export_utils.py`` so the
module lives under the ``shared/`` package alongside its siblings. This root-level
module is retained as a thin re-export shim so the ~12 existing call sites that do
``import shared_export_utils`` or ``from shared_export_utils import X`` keep working
unchanged. Zero behavior change — every public name is re-exported from
``shared.export_utils``.

New code should import from ``shared.export_utils`` directly.
"""

from shared.export_utils import *  # noqa: F401,F403  (re-export public surface)

# Explicit re-export so ``from shared_export_utils import X`` resolves the same
# symbols as ``shared.export_utils`` even for names a star-import might skip, and
# so static tooling / introspection sees the public surface on this shim too.
from shared.export_utils import (  # noqa: F401
    EXPORT_CONTEXT_CAP,
    build_expanded_context,
    build_rich_snippet_cell,
    clean_text_single_line,
    coerce_img_page_cell,
    contains_any_term,
    encode_filename_for_header,
    extract_search_terms,
    make_safe_filename,
    remove_highlight_markers,
    sanitize_cache_filename,
    sanitize_text_for_excel,
    strip_xml_illegal_chars,
)

__all__ = [
    "EXPORT_CONTEXT_CAP",
    "build_expanded_context",
    "build_rich_snippet_cell",
    "clean_text_single_line",
    "coerce_img_page_cell",
    "contains_any_term",
    "encode_filename_for_header",
    "extract_search_terms",
    "make_safe_filename",
    "remove_highlight_markers",
    "sanitize_cache_filename",
    "sanitize_text_for_excel",
    "strip_xml_illegal_chars",
]
