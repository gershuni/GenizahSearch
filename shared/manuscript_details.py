"""Local-only catalog and source records for public research clients."""

SECTIONS = (
    'fjms_catalog', 'fjms_bibliography', 'fjms_catalog_refs',
    'nli_catalog', 'nli_bibliography', 'pgp_sources', 'fgp_sources',
)
SOURCE_FIELDS = (
    'id', 'uid', 'pgpid', 'source_scholar', 'doc_relation', 'language',
    'page_info', 'folio_label', 'folio_num', 'image_side', 'fgp_c_number',
    'source_credit', 'source_credit_he', 'source_credit_en', 'attribution',
    'source_url', 'source_title', 'citation', 'pgp_url',
)


def source_credit_rows(rows):
    # Never send edition content/sections or internal ingestion metadata here.
    return [{key: row[key] for key in SOURCE_FIELDS if key in row} for row in rows]


def load_section(sys_id, section, *, nli_cache=None):
    """Return (records, availability). No remote fetches or corpus searches."""
    if section.startswith('fjms_'):
        from shared.fjms_service import get_fjms_service
        service = get_fjms_service(thread_safe=True)
        if not service or not service.is_available():
            return [], 'unavailable'
        if section == 'fjms_catalog':
            detail = service.get_catalog_detail(sys_id)
            rows = []
            for category, values in detail.items():
                if isinstance(values, dict):
                    for record_id, items in values.items():
                        rows.append({'category': category, 'unit_catalog_rec_id': record_id, 'data': items})
                else:
                    rows.extend({'category': category, 'data': item} for item in values)
            return rows, 'local_records'
        method = 'get_bibliography' if section == 'fjms_bibliography' else 'get_catalog_refs'
        return getattr(service, method)(sys_id), 'local_records'
    if section.startswith('nli_'):
        cached = (nli_cache or {}).get(sys_id, {})
        marc = cached.get('marc')
        if not marc:
            return [], 'not_cached'
        if section == 'nli_bibliography':
            # Preserve the raw MARC citation; heuristic parsing is not evidence.
            return [{'citation': value} for value in marc.get('bibliography', [])], 'local_cache'
        fields = ('notes', 'english_title', 'dimensions', 'people', 'current_owner',
                  'shelfmark_alt', 'date', 'subjects', 'physical_medium', 'attribution', 'online_link')
        return [{'field': key, 'value': marc[key]} for key in fields if marc.get(key)], 'local_cache'
    if section == 'pgp_sources':
        from shared.document_service import get_pgp_service
        service = get_pgp_service(thread_safe=True)
        if not service or not service.is_available():
            return [], 'unavailable'
        return source_credit_rows(service.get_all_sources_for_fragment(sys_id)), 'local_records'
    if section == 'fgp_sources':
        from shared.fgp_service import get_fgp_service, _fgp_enabled
        if not _fgp_enabled():
            return [], 'unavailable'
        service = get_fgp_service(thread_safe=True)
        if not service or not service.is_available():
            return [], 'unavailable'
        return source_credit_rows(service.get_fgp_sources_for_fragment(sys_id)), 'local_records'
    raise ValueError('Unknown manuscript details section')
