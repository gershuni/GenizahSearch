# -*- coding: utf-8 -*-
"""
Backward-compatibility shim.

All document_service functions have moved to shared.document_service.
This shim re-exports them so existing web imports continue working:
  from web.document_service import get_document_for_fragment  # still works
"""
from shared.document_service import (
    get_document_for_fragment,
    get_fragments_for_document,
    get_transcription_for_document,
    get_document_metadata,
    parse_transcription_sections,
    get_section_for_page,
    get_sources_for_document,
    get_all_sources_for_fragment,
    get_editions_for_document,
    get_translations_for_document,
    get_sys_ids_with_transcriptions,
    get_fragments_by_tag,
    get_all_distinct_tags,
    parse_html_sections,
    get_pgp_service,
)
