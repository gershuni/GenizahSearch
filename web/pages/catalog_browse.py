# -*- coding: utf-8 -*-
"""
Catalog Browse page for GenizahSearch web application.

Features:
- Collapsible domain tree navigation (FJMS domain hierarchy)
- Author search-as-you-type with cross-filtering
- Work/title search-as-you-type with cross-filtering
- Combined multi-axis filtering with removable chips
- Paginated results table with inline expandable detail rows
- Collapsible sidebar
- Deep linking via URL query params
- RTL Hebrew support
"""

from nicegui import ui, run
from web.safe_storage import safe_user_get, safe_user_set
from web.translations import tr, is_rtl, get_language
from web.components.typography import h1
from shared.fjms_service import get_fjms_service, resolve_library_sys_ids
from shared.browse_map_utils import LIBRARY_CODES, get_library_display, sanitize_library_codes, library_codes_with_manuscripts

import asyncio
import logging
import threading

logger = logging.getLogger(__name__)

PAGE_SIZE = 50

# ── SEED-023: corpus-wide PGP / scholarly-transcription membership sets ──────
# Static per process (refresh on restart after a data update). Computed once,
# lazily, under a lock, then shared read-only across all users / requests so the
# catalog filters can intersect them against the full result set before
# pagination. Heavy DB reads — NEVER recompute per request.
#
# Both sets are published as ONE tuple sentinel (Codex #308 P2): a second worker
# must never observe ``pgp`` set while ``edition`` is still ``None`` (that would
# silently drop an active Editions filter). A single-slot publish has no partial
# window.
_FILTER_SETS = {'value': None}  # None or (pgp_link_sys_ids, edition_sys_ids)
_FILTER_SETS_LOCK = threading.Lock()


def _get_filter_sets():
    """Return ``(pgp_link_sys_ids, edition_sys_ids)`` (both sets).

    ``pgp_link_sys_ids`` = "has PGP info" link presence (same signal as the PGP
    badge). ``edition_sys_ids`` = PGP ``%Edition%`` ∪ FGP ``Digital Edition``
    (editions only; honors the ``WEB_FGP_ENABLED`` kill switch). Must be called
    from a worker thread (``run.io_bound``) — the first call runs DB queries.
    """
    cached = _FILTER_SETS['value']
    if cached is not None:
        return cached
    with _FILTER_SETS_LOCK:
        cached = _FILTER_SETS['value']
        if cached is None:
            from shared.document_service import (
                get_all_pgp_link_sys_ids,
                get_sys_ids_with_editions,
            )
            from shared.fgp_service import get_sys_ids_with_fgp_editions
            from web.feature_flags import web_fgp_enabled
            pgp = get_all_pgp_link_sys_ids()
            edition = set(get_sys_ids_with_editions())  # PGP %Edition%
            if web_fgp_enabled():
                edition |= get_sys_ids_with_fgp_editions()  # FGP Digital Edition
            cached = (pgp, edition)
            _FILTER_SETS['value'] = cached  # single atomic publish
            logger.info(
                "catalog_browse filter sets: pgp=%d edition=%d",
                len(pgp), len(edition),
            )
    return cached


def create_catalog_browse_page(
    initial_domain: str = None,
    initial_author: str = None,
    initial_work: str = None,
    initial_page: int = None,
    initial_text_all: str = None,
    initial_text_any: str = None,
    initial_text_not: str = None,
):
    """Create the catalog browse page with domain tree, author/work search, and results."""

    fjms = get_fjms_service(thread_safe=True)
    lang = get_language()
    rtl = is_rtl()

    # ── State ──────────────────────────────────────────────────────
    current_domain = {'value': initial_domain, 'display': initial_domain}
    current_author = {'value': initial_author}
    current_work = {'value': initial_work}
    current_date_from = {'value': None}
    current_date_to = {'value': None}
    current_include_undated = {'value': False}
    # Parse comma-separated text filter URL params
    _parse_csv = lambda s: [t.strip() for t in s.split(',') if t.strip()] if s else []
    current_text_all = {'value': _parse_csv(initial_text_all)}  # AND terms
    current_text_any = {'value': _parse_csv(initial_text_any)}  # OR terms
    current_text_not = {'value': _parse_csv(initial_text_not)}  # NOT terms
    current_page = {'value': (initial_page or 1)}
    sidebar_visible = {'value': True}
    # SEED-023 availability filters — 3-state, persisted via safe_storage.
    # Clamp persisted values to the known set so a stale/corrupted entry degrades
    # to 'all' instead of crashing or filtering unexpectedly.
    _pgp0 = safe_user_get('catalog_pgp_filter', 'all')
    _ed0 = safe_user_get('catalog_editions_filter', 'all')
    current_pgp_filter = {'value': _pgp0 if _pgp0 in ('all', 'has_pgp', 'no_pgp') else 'all'}
    current_editions_filter = {'value': _ed0 if _ed0 in ('all', 'has_edition', 'no_edition') else 'all'}
    # SEED-026 / DMF-08 (131-04): library filter — dual-mode (Show-only / Hide).
    # Persisted as {'mode','codes'} dict; legacy plain-list migrates to Show-only.
    # Three-branch D-06 migration matching search.py restore path.
    _lib_raw = safe_user_get('catalog_library_filter', [])
    if isinstance(_lib_raw, list):
        # D-06 legacy migration: plain list -> Show-only (v8.3.0 plain-list values).
        _lib_codes0 = sanitize_library_codes(_lib_raw)
        if _lib_codes0:
            _cat_library_mode = 'show_only'
        else:
            _cat_library_mode = 'hide'
            _lib_codes0 = []
    elif isinstance(_lib_raw, dict):
        # New dict shape: read mode + codes, validate mode.
        _cat_library_mode = _lib_raw.get('mode', 'hide')
        if _cat_library_mode not in ('show_only', 'hide'):
            _cat_library_mode = 'hide'
        _lib_codes0 = sanitize_library_codes(_lib_raw.get('codes'))
        # Normalize show_only+empty to hide (Codex HIGH fix).
        if _cat_library_mode == 'show_only' and not _lib_codes0:
            _cat_library_mode = 'hide'
    else:
        # Fresh/absent/garbage: default Hide, empty set (show all).
        _cat_library_mode = 'hide'
        _lib_codes0 = []
    current_library_filter = {'value': _lib_codes0}
    current_library_mode = {'value': _cat_library_mode}
    # True full-set facet counts (DMF-12): refreshed alongside fetch_results.
    # Always from fjms.get_browse_library_facets(...) — NEVER page-local counts.
    current_library_facets = {'value': {}}

    # Cached lists for cross-filtering
    authors_list = {'data': []}
    works_list = {'data': []}

    # UI element references (assigned during layout build)
    results_container = {'ref': None}
    chips_container = {'ref': None}
    pagination_container = {'ref': None}
    author_select_ref = {'ref': None}
    work_select_ref = {'ref': None}
    date_from_ref = {'ref': None}
    date_to_ref = {'ref': None}
    undated_checkbox_ref = {'ref': None}
    loading_spinner = {'ref': None}
    sidebar_ref = {'ref': None}
    sidebar_toggle_ref = {'ref': None}
    text_mode_ref = {'ref': None}
    text_input_ref = {'ref': None}
    pgp_filter_btn_ref = {'ref': None}
    editions_filter_btn_ref = {'ref': None}
    library_filter_btn_ref = {'ref': None}  # GAP-E: button opens _open_library_filter_dialog()

    # ── Deep linking helper ────────────────────────────────────────
    def update_url():
        """Update browser URL with current filter state (no page reload)."""
        from urllib.parse import quote
        params = []
        if current_domain['value']:
            params.append(f"domain={quote(str(current_domain['value']), safe='')}")
        if current_author['value']:
            params.append(f"author={quote(str(current_author['value']), safe='')}")
        if current_work['value']:
            params.append(f"work={quote(str(current_work['value']), safe='')}")
        if current_date_from['value'] is not None:
            params.append(f"date_from={current_date_from['value']}")
        if current_date_to['value'] is not None:
            params.append(f"date_to={current_date_to['value']}")
        if current_include_undated['value']:
            params.append("undated=1")
        if current_text_all['value']:
            params.append(f"text_all={quote(','.join(current_text_all['value']), safe='')}")
        if current_text_any['value']:
            params.append(f"text_any={quote(','.join(current_text_any['value']), safe='')}")
        if current_text_not['value']:
            params.append(f"text_not={quote(','.join(current_text_not['value']), safe='')}")
        if current_page['value'] and current_page['value'] > 1:
            params.append(f"page={current_page['value']}")
        qs = '?' + '&'.join(params) if params else ''
        ui.run_javascript(
            f"history.replaceState(null, '', '/catalog-browse{qs}')"
        )
        # Persist filter state in sessionStorage for same-session return visits
        import json as _json
        state = _json.dumps({
            'domain': current_domain['value'],
            'domain_display': current_domain.get('display'),
            'author': current_author['value'],
            'work': current_work['value'],
            'date_from': current_date_from['value'],
            'date_to': current_date_to['value'],
            'undated': current_include_undated['value'],
            'text_all': current_text_all['value'],
            'text_any': current_text_any['value'],
            'text_not': current_text_not['value'],
        })
        ui.run_javascript(
            f"sessionStorage.setItem('catalog_browse_filters', {_json.dumps(state)})"
        )

    # ── Sidebar toggle ─────────────────────────────────────────────
    def toggle_sidebar():
        sidebar_visible['value'] = not sidebar_visible['value']
        if sidebar_ref['ref']:
            sidebar_ref['ref'].set_visibility(sidebar_visible['value'])
        if sidebar_toggle_ref['ref']:
            icon = 'chevron_left' if sidebar_visible['value'] else 'chevron_right'
            if rtl:
                icon = 'chevron_right' if sidebar_visible['value'] else 'chevron_left'
            sidebar_toggle_ref['ref'].props(f'icon={icon}')

    # ── Data fetch helpers ─────────────────────────────────────────
    async def fetch_authors():
        """Fetch authors list (optionally filtered by current domain)."""
        data = await run.io_bound(fjms.get_browse_authors, current_domain['value'])
        authors_list['data'] = data
        _update_author_options()
        return data

    async def fetch_works():
        """Fetch works list (optionally filtered by current domain + author)."""
        data = await run.io_bound(
            fjms.get_browse_works,
            current_domain['value'],
            current_author['value'],
        )
        works_list['data'] = data
        _update_work_options()
        return data

    def _update_author_options():
        """Update the author select options from cached data."""
        sel = author_select_ref['ref']
        if not sel:
            return
        options = {}
        for a in authors_list['data']:
            # v5+: person_id based; legacy: eng_desc is AuthorText
            key = str(a.get('person_id')) if a.get('person_id') is not None else a.get('eng_desc', '')
            if lang == 'he':
                display = a.get('heb_desc', '') or a.get('eng_desc', '')
            else:
                display = a.get('eng_desc', '') or a.get('heb_desc', '')
            if key and display:
                options[key] = f"{display}  ({a['count']:,})"
        sel.options = options
        sel.update()

    def _update_work_options():
        """Update the work select options from cached data."""
        sel = work_select_ref['ref']
        if not sel:
            return
        options = {}
        for w in works_list['data']:
            # v5+: title_id based; legacy: org_title is Title
            key = str(w.get('title_id')) if w.get('title_id') is not None else w.get('org_title', '')
            if lang == 'he':
                display = w.get('org_title', '') or w.get('eng_title', '')
            else:
                display = w.get('eng_title', '') or w.get('org_title', '')
            if key and display:
                options[key] = f"{display}  ({w['count']:,})"
        sel.options = options
        sel.update()

    def _fetch_results_blocking(
        offset, domain, author, work, date_from, date_to, undated,
        text_all, text_any, text_not, pgp_state, ed_state, library_codes, library_mode,
    ):
        """Blocking browse fetch (runs in io_bound). Resolves the corpus-wide
        PGP/edition sets (cached) and the selected-library sys_id set only when the
        respective filter is active.  All resolution runs here — off the async event
        loop — so the count + pagination apply to the FULL filtered set (SEED-023 B3,
        SEED-026)."""
        pgp_ids = ed_ids = None
        if pgp_state in ('has_pgp', 'no_pgp') or ed_state in ('has_edition', 'no_edition'):
            pgp_ids, ed_ids = _get_filter_sets()
        # SEED-026: resolve library codes → sys_id set inside io_bound (O(255K), Pitfall 2).
        # Fail-open: empty selection passes None for both args (Pitfall 5 — never set()).
        lib_sys_ids = None
        if library_codes:
            from web.state import state as _state
            resolved = resolve_library_sys_ids(library_codes, _state.meta_mgr)
            lib_sys_ids = resolved if resolved else None  # empty set → None (fail-open)
        return fjms.get_browse_results(
            domain, author, work, offset, PAGE_SIZE,
            date_from, date_to, undated, text_all, text_any, text_not,
            pgp_filter=(pgp_state if pgp_state in ('has_pgp', 'no_pgp') else None),
            pgp_sys_ids=(pgp_ids if pgp_state in ('has_pgp', 'no_pgp') else None),
            editions_filter=(ed_state if ed_state in ('has_edition', 'no_edition') else None),
            edition_sys_ids=(ed_ids if ed_state in ('has_edition', 'no_edition') else None),
            library_codes=(library_codes or None),
            library_sys_ids=lib_sys_ids,
            library_mode=library_mode,
        )

    async def fetch_results():
        """Fetch paginated browse results for current filters."""
        offset = (current_page['value'] - 1) * PAGE_SIZE
        data = await run.io_bound(
            _fetch_results_blocking,
            offset,
            current_domain['value'],
            current_author['value'],
            current_work['value'],
            current_date_from['value'],
            current_date_to['value'],
            current_include_undated['value'],
            current_text_all['value'] or None,
            current_text_any['value'] or None,
            current_text_not['value'] or None,
            current_pgp_filter['value'],
            current_editions_filter['value'],
            # SEED-026: pass a COPY of the current library selection (empty list = no filter).
            # Snapshot so chip removal mutating the list cannot race the io_bound resolver.
            list(current_library_filter['value']) or None,
            current_library_mode['value'],
        )
        return data

    def _fetch_library_facets_blocking(
        domain, author, work, date_from, date_to, undated,
        text_all, text_any, text_not, pgp_state, ed_state,
    ):
        """Fetch TRUE full-set library facet counts (DMF-12). Runs in io_bound.

        ALWAYS from fjms.get_browse_library_facets — NEVER page-local counts (Codex R3 F1).
        The library filter is intentionally excluded so ALL library options show their
        full-corpus counts (not the post-library-filter counts).
        Falls back to {} on any error; the dialog renders without counts in that case.
        """
        pgp_ids = ed_ids = None
        if pgp_state in ('has_pgp', 'no_pgp') or ed_state in ('has_edition', 'no_edition'):
            pgp_ids, ed_ids = _get_filter_sets()
        try:
            from web.state import state as _state
            return fjms.get_browse_library_facets(
                domain=domain,
                author=author,
                work=work,
                date_from=date_from,
                date_to=date_to,
                include_undated=undated,
                text_all=text_all,
                text_any=text_any,
                text_not=text_not,
                pgp_filter=(pgp_state if pgp_state in ('has_pgp', 'no_pgp') else None),
                pgp_sys_ids=(pgp_ids if pgp_state in ('has_pgp', 'no_pgp') else None),
                editions_filter=(ed_state if ed_state in ('has_edition', 'no_edition') else None),
                edition_sys_ids=(ed_ids if ed_state in ('has_edition', 'no_edition') else None),
                # Pass the FULL-CORPUS callable (a bound method) — NOT a page-local dict.
                # This ensures off-page libraries are counted correctly (Codex R3 F3, N5).
                sys_id_to_library=_state.meta_mgr.get_library_for_id,
            )
        except Exception as e:
            logger.warning("catalog_browse: facet fetch failed: %s", e)
            return {}

    # ── Resolve shelfmark / library / catalog count / snippet ────────
    def _resolve_all(result_list):
        """Batch resolve shelfmarks, libraries, catalog counts, snippets, printed status. Runs in io_bound."""
        from web.state import state

        sys_ids = [r.get('sys_id', '') for r in result_list]

        # Batch fetch printed status
        printed_ids = set()
        try:
            valid_ids = [s for s in sys_ids if s]
            if valid_ids:
                printed_ids = fjms.get_printed_sys_ids(valid_ids) if fjms.is_available() else set()
        except Exception as e:
            logger.debug(f"Printed status batch failed: {e}")

        # Batch fetch catalog record counts
        catalog_count_map = {}
        try:
            for sid in sys_ids:
                if sid:
                    recs = fjms.get_catalog_records(sid)
                    if recs:
                        catalog_count_map[sid] = len(recs)
        except Exception as e:
            logger.debug(f"Catalog count batch failed: {e}")

        # Fetch text snippets from middle page of each manuscript
        snippet_map = {}
        try:
            if state.searcher:
                for sid in sys_ids:
                    if not sid:
                        continue
                    try:
                        page1 = state.searcher.get_browse_page(sid, p_num=1)
                        if not page1:
                            continue
                        total_pages = page1.get('total_pages', 1)
                        mid_page = max(1, total_pages // 2)
                        snippet_page = state.searcher.get_browse_page(sid, p_num=mid_page)
                        if not snippet_page:
                            snippet_page = page1
                        text = (snippet_page.get('text') or '').strip()
                        if text:
                            if len(text) > 200:
                                text = text[:200] + '...'
                            snippet_map[sid] = text
                    except Exception:
                        pass  # Browse enrichment failed; continue with available data
        except Exception as e:
            logger.debug(f"Snippet fetch failed: {e}")

        resolved = []
        for r in result_list:
            sid = r.get('sys_id', '')
            shelfmark = ''
            library_code = ''
            try:
                if state.meta_mgr:
                    sm, _title = state.meta_mgr.get_meta_for_id(sid)
                    shelfmark = sm or ''
                    library_code = state.meta_mgr.get_library_for_id(sid) or ''
            except Exception:
                pass  # Enrichment failed for this item; continue with available data
            resolved.append({
                'shelfmark': shelfmark,
                'library_code': library_code,
                'catalog_count': catalog_count_map.get(sid, 0),
                'snippet': snippet_map.get(sid, ''),
                'is_printed': sid in printed_ids,
            })
        return resolved

    # ── Refresh results table ──────────────────────────────────────
    async def refresh_results():
        """Refresh the results table and pagination based on current filters."""
        ui.run_javascript('window.__showLoadingBar && window.__showLoadingBar()')
        if loading_spinner['ref']:
            loading_spinner['ref'].set_visibility(True)

        try:
            data = await fetch_results()
        except Exception as e:
            logger.error(f"catalog_browse fetch_results error: {e}")
            data = {"results": [], "total": 0}

        if loading_spinner['ref']:
            loading_spinner['ref'].set_visibility(False)
        ui.run_javascript('window.__hideLoadingBar && window.__hideLoadingBar()')

        results = data.get('results', [])
        total = data.get('total', 0)

        # DMF-12: Fetch TRUE full-set library facet counts alongside the results.
        # ALWAYS via fjms.get_browse_library_facets (the page's fjms instance method),
        # honoring the active non-library filters. Never a page-local fallback.
        try:
            new_facets = await run.io_bound(
                _fetch_library_facets_blocking,
                current_domain['value'],
                current_author['value'],
                current_work['value'],
                current_date_from['value'],
                current_date_to['value'],
                current_include_undated['value'],
                current_text_all['value'] or None,
                current_text_any['value'] or None,
                current_text_not['value'] or None,
                current_pgp_filter['value'],
                current_editions_filter['value'],
            )
            current_library_facets['value'] = new_facets
        except Exception as e:
            logger.warning("catalog_browse: facet refresh failed: %s", e)
            current_library_facets['value'] = {}

        # Batch resolve shelfmarks via io_bound
        resolved_meta = await run.io_bound(_resolve_all, results)

        # Build table rows with resolved shelfmarks + detail data
        rows = []
        for i, r in enumerate(results):
            sid = r.get('sys_id', '')
            meta = resolved_meta[i]
            shelfmark = meta['shelfmark']
            library_code = meta['library_code']

            # Domain display
            if lang == 'he':
                domain_display = ', '.join(r.get('domains_heb', []) or r.get('domains', []))
            else:
                domain_display = ', '.join(r.get('domains', []))

            # Identification (author + title)
            author = r.get('author', '')
            if lang == 'he':
                title = r.get('title_heb', '') or r.get('title', '')
            else:
                title = r.get('title', '') or r.get('title_heb', '')
            identification = f"{author} - {title}" if author and title else (author or title)

            date_val = r.get('copy_date', '')

            # Textual frame for detail
            textual_frame = r.get('textual_frame_heb', '') if lang == 'he' else r.get('textual_frame_eng', '')
            if not textual_frame:
                textual_frame = r.get('textual_frame_eng', '') or r.get('textual_frame_heb', '')

            # Phase 46: Gap-fill empty fields from FJMS translations when toggle is on
            # Class A wrapper collapsed — safe_user_get absorbs prune-race AssertionError.
            _show_cat_trans = safe_user_get('show_translations', False)
            _is_translated_title = False
            if _show_cat_trans:
                alma_id = r.get('alma_id', '')
                if alma_id:
                    try:
                        from shared.translation_service import TranslationService
                        _tsvc_cat = TranslationService(thread_safe=True)
                        if _tsvc_cat.fjms_available():
                            _fjms_trans = _tsvc_cat.get_fjms_translations_batch([alma_id])
                            _trans_fields = _fjms_trans.get(alma_id, {})
                            # Fill empty title from translations (values are (text, direction) tuples)
                            _title_entry = _trans_fields.get('Title')
                            _titleheb_entry = _trans_fields.get('TitleHeb')
                            if not title and _title_entry:
                                title = _title_entry[0] if isinstance(_title_entry, tuple) else _title_entry
                                identification = f"{author} - {title}" if author and title else (author or title)
                                _is_translated_title = True
                            elif not title and _titleheb_entry:
                                title = _titleheb_entry[0] if isinstance(_titleheb_entry, tuple) else _titleheb_entry
                                identification = f"{author} - {title}" if author and title else (author or title)
                                _is_translated_title = True
                        _tsvc_cat.close()
                    except Exception:
                        pass  # Translation lookup failed; continue without translation

            rows.append({
                'sys_id': sid,
                'shelfmark': shelfmark or sid,
                'library': library_code,
                'domain': domain_display,
                'identification': identification,
                'date': date_val,
                # Detail fields (used in expanded row)
                '_author': author,
                '_title': title,
                '_textual_frame': textual_frame,
                '_catalog_count': meta['catalog_count'],
                '_snippet': meta['snippet'],
                '_is_printed': meta.get('is_printed', False),
            })

        render_results_table(rows, total)
        render_pagination(total)
        render_chips()
        _update_search_buttons()
        update_url()

    # ── Render: Results table with inline expandable detail ────────
    def render_results_table(rows, total):
        """Render the results table with expandable detail rows."""
        container = results_container['ref']
        if not container:
            return
        container.clear()

        with container:
            if not rows:
                with ui.column().classes('w-full items-center py-12'):
                    ui.icon('search_off').classes('text-5xl mb-4').style('color: var(--text-muted)')
                    ui.label(tr('No manuscripts match the current filters')).classes(
                        'text-lg'
                    ).style('color: var(--text-secondary)')
                return

            # Count label
            page_num = current_page['value']
            start = (page_num - 1) * PAGE_SIZE + 1
            end = min(page_num * PAGE_SIZE, total)
            count_text = f"{tr('Showing')} {start}-{end} {tr('of')} {total:,} {tr('manuscripts')}"
            ui.label(count_text).classes('text-sm mb-1').style('color: var(--text-muted)')

            # Text filter summary sentence
            summary = _build_text_filter_summary()
            if summary:
                ui.markdown(summary).classes('text-xs mb-2').style('color: var(--text-muted)')

            # Translated labels for the Vue template
            lbl_author = tr('Author')
            lbl_title = tr('Work / Title')
            lbl_domain = tr('Domain')
            lbl_date = tr('Date')
            lbl_desc = tr('Description')
            lbl_browse = tr('Browse')
            lbl_catalog = tr('Catalog')
            lbl_snippet = tr('Text Preview')
            lbl_printed = '\u05d3\u05e4\u05d5\u05e1' if lang == 'he' else 'Printed'

            # Table columns with percentage widths (required for table-layout: fixed)
            _col_style = 'overflow: hidden; text-overflow: ellipsis; white-space: nowrap'
            columns = [
                {'name': 'shelfmark', 'label': tr('Shelfmark'), 'field': 'shelfmark', 'align': 'left', 'sortable': True, 'style': f'width: 22%; {_col_style}', 'headerStyle': 'width: 22%'},
                {'name': 'library', 'label': tr('Library'), 'field': 'library', 'align': 'left', 'sortable': True, 'style': f'width: 10%; {_col_style}', 'headerStyle': 'width: 10%'},
                {'name': 'domain', 'label': tr('Domain'), 'field': 'domain', 'align': 'left', 'sortable': True, 'style': f'width: 22%; {_col_style}', 'headerStyle': 'width: 22%'},
                {'name': 'identification', 'label': tr('Identification'), 'field': 'identification', 'align': 'left', 'sortable': True, 'style': f'width: 36%; {_col_style}', 'headerStyle': 'width: 36%'},
                {'name': 'date', 'label': tr('Date'), 'field': 'date', 'align': 'left', 'sortable': True, 'style': f'width: 10%; {_col_style}', 'headerStyle': 'width: 10%'},
            ]
            table = ui.table(
                columns=columns,
                rows=rows,
                row_key='sys_id',
            ).classes('w-full catalog-browse-table').props('flat bordered dense')

            # Catalog dialog handler via NiceGUI event on table
            async def on_catalog_event(e):
                """Open catalog dialog triggered from Vue slot."""
                args = e.args if hasattr(e, 'args') else {}
                sid = args.get('sys_id', '') if isinstance(args, dict) else ''
                sm = args.get('shelfmark', '') if isinstance(args, dict) else ''
                if sid:
                    from web.components.catalog_dialog import show_catalog_dialog
                    show_catalog_dialog(sid, sm, fjms)
            table.on('catalogClick', on_catalog_event)

            # Browse navigation handler via NiceGUI event on table
            def on_browse_event(e):
                """Navigate to browse page triggered from Vue slot."""
                args = e.args if hasattr(e, 'args') else {}
                sid = args.get('sys_id', '') if isinstance(args, dict) else ''
                if sid:
                    ui.navigate.to(f'/browse?sys_id={sid}')
            table.on('browseClick', on_browse_event)

            # Body slot: single-expand with simple toggle, thumbnails via API
            table.add_slot('body', f'''
                <q-tr :props="props" @click="(e) => {{
                    const detail = e.currentTarget.nextElementSibling;
                    const tbody = e.currentTarget.closest('tbody');
                    const wasVisible = detail.style.display !== 'none';
                    tbody.querySelectorAll('.catalog-detail-row').forEach(tr => {{ tr.style.display = 'none'; }});
                    if (!wasVisible) detail.style.display = '';
                }}" class="cursor-pointer">
                    <q-td v-for="col in props.cols" :key="col.name" :props="props"
                        :title="col.value">
                        <span>{{{{ col.value }}}}</span>
                        <q-badge v-if="col.name === 'shelfmark' && props.row._is_printed"
                            color="red-2" text-color="red-10" class="q-ml-xs" dense>{lbl_printed}</q-badge>
                    </q-td>
                </q-tr>
                <q-tr :props="props" class="catalog-detail-row" style="display: none;">
                    <q-td colspan="100%" style="padding: 0;">
                        <div class="q-pa-md" style="background: var(--bg-tertiary); border-left: 4px solid var(--primary-600);">
                            <div class="row q-col-gutter-md">
                                <div class="col-auto">
                                    <img :src="'/api/nli_image_by_sysid/' + props.row.sys_id + '?page=0'"
                                        style="max-height: 120px; max-width: 100px; object-fit: contain; border-radius: 4px; border: 1px solid var(--border-light);"
                                        @error="$event.target.style.display='none'" />
                                </div>
                                <div class="col">
                                    <div v-if="props.row._author" class="row items-start q-mb-xs">
                                        <div class="text-caption text-weight-bold text-uppercase" style="min-width: 80px; color: var(--text-tertiary);">{lbl_author}</div>
                                        <div class="text-body2" style="color: var(--text-primary);">{{{{ props.row._author }}}}</div>
                                    </div>
                                    <div v-if="props.row._title" class="row items-start q-mb-xs">
                                        <div class="text-caption text-weight-bold text-uppercase" style="min-width: 80px; color: var(--text-tertiary);">{lbl_title}</div>
                                        <div class="text-body2" style="color: var(--text-primary);">{{{{ props.row._title }}}}</div>
                                    </div>
                                    <div v-if="props.row.domain" class="row items-start q-mb-xs">
                                        <div class="text-caption text-weight-bold text-uppercase" style="min-width: 80px; color: var(--text-tertiary);">{lbl_domain}</div>
                                        <div class="text-body2" style="color: var(--text-primary);">{{{{ props.row.domain }}}}</div>
                                    </div>
                                    <div v-if="props.row.date" class="row items-start q-mb-xs">
                                        <div class="text-caption text-weight-bold text-uppercase" style="min-width: 80px; color: var(--text-tertiary);">{lbl_date}</div>
                                        <div class="text-body2" style="color: var(--text-primary);">{{{{ props.row.date }}}}</div>
                                    </div>
                                    <div v-if="props.row._is_printed" class="row items-start q-mb-xs">
                                        <div class="text-caption text-weight-bold text-uppercase" style="min-width: 80px; color: var(--text-tertiary);">Material</div>
                                        <q-badge color="red-2" text-color="red-10" dense>{lbl_printed}</q-badge>
                                    </div>
                                    <div v-if="props.row._textual_frame" class="row items-start q-mb-xs">
                                        <div class="text-caption text-weight-bold text-uppercase" style="min-width: 80px; color: var(--text-tertiary);">{lbl_desc}</div>
                                        <div class="text-body2" style="color: var(--text-primary);">{{{{ props.row._textual_frame }}}}</div>
                                    </div>
                                    <div v-if="props.row._snippet" class="q-mt-sm" style="direction: rtl; color: var(--text-secondary); font-size: 0.85em; max-height: 60px; overflow: hidden; line-height: 1.4; opacity: 0.8;">
                                        {{{{ props.row._snippet }}}}
                                    </div>
                                </div>
                                <div class="col-auto self-start">
                                    <div class="column q-gutter-xs">
                                        <q-btn flat dense color="primary" size="sm" icon="open_in_new"
                                            label="{lbl_browse}"
                                            @click.stop="$parent.$emit('browseClick', {{sys_id: props.row.sys_id}})" />
                                        <q-btn v-if="props.row._catalog_count"
                                            flat dense size="sm" icon="library_books"
                                            :label="'{lbl_catalog} (' + props.row._catalog_count + ')'"
                                            style="color: var(--primary-700);"
                                            @click.stop="$parent.$emit('catalogClick', {{sys_id: props.row.sys_id, shelfmark: props.row.shelfmark}})" />
                                    </div>
                                </div>
                            </div>
                        </div>
                    </q-td>
                </q-tr>
            ''')

    # ── Render: Pagination ─────────────────────────────────────────
    def render_pagination(total):
        """Render pagination controls."""
        container = pagination_container['ref']
        if not container:
            return
        container.clear()

        if total <= PAGE_SIZE:
            return

        total_pages = (total + PAGE_SIZE - 1) // PAGE_SIZE
        page_num = current_page['value']

        with container:
            with ui.row().classes('w-full items-center justify-center gap-4 py-4'):
                # In RTL, "Previous" (older page) has right arrow, "Next" has left arrow
                prev_icon = 'chevron_right' if rtl else 'chevron_left'
                next_icon = 'chevron_left' if rtl else 'chevron_right'

                prev_btn = ui.button(
                    tr('Previous'),
                    icon=prev_icon,
                    on_click=lambda: go_to_page(page_num - 1),
                ).props('flat')
                if page_num <= 1:
                    prev_btn.props('disable')

                ui.label(f"{tr('Page')} {page_num} / {total_pages}").classes(
                    'text-sm'
                ).style('color: var(--text-secondary)')

                next_btn = ui.button(
                    tr('Next'),
                    icon=next_icon,
                    on_click=lambda: go_to_page(page_num + 1),
                ).props('flat')
                if page_num >= total_pages:
                    next_btn.props('disable')

    async def go_to_page(page_num):
        """Navigate to a specific page."""
        current_page['value'] = max(1, page_num)
        await refresh_results()

    def _resolve_author_display(author_val):
        """Resolve author value to display name from cached authors list."""
        for a in authors_list['data']:
            key = str(a.get('person_id')) if a.get('person_id') is not None else a.get('eng_desc', '')
            if key == author_val:
                if lang == 'he':
                    return a.get('heb_desc', '') or a.get('eng_desc', '')
                return a.get('eng_desc', '') or a.get('heb_desc', '')
        return author_val

    def _resolve_work_display(work_val):
        """Resolve work value to display name from cached works list."""
        for w in works_list['data']:
            key = str(w.get('title_id')) if w.get('title_id') is not None else w.get('org_title', '')
            if key == work_val:
                if lang == 'he':
                    return w.get('org_title', '') or w.get('eng_title', '')
                return w.get('eng_title', '') or w.get('org_title', '')
        return work_val

    # ── Render: Filter chips ───────────────────────────────────────
    def render_chips():
        """Render active filter chips above results."""
        container = chips_container['ref']
        if not container:
            return
        container.clear()

        has_filters = any([
            current_domain['value'],
            current_author['value'],
            current_work['value'],
            current_date_from['value'] is not None,
            current_date_to['value'] is not None,
            current_text_all['value'],
            current_text_any['value'],
            current_text_not['value'],
            current_pgp_filter['value'] != 'all',
            current_editions_filter['value'] != 'all',
            # SEED-026 (smoke 2026-06-29): library filter has NO chip — its state
            # lives on the library filter button (red + "shown/total"), so it must
            # NOT force the chip bar to appear.
        ])

        if not has_filters:
            return

        with container:
            with ui.row().classes('w-full items-center gap-2 flex-wrap py-2'):
                if current_domain['value']:
                    domain_label = current_domain.get('display') or current_domain['value']
                    _make_chip(
                        f"{tr('Domain')}: {domain_label}",
                        lambda: clear_filter('domain'),
                    )
                if current_author['value']:
                    author_display = _resolve_author_display(current_author['value'])
                    _make_chip(
                        f"{tr('Author')}: {author_display}",
                        lambda: clear_filter('author'),
                    )
                if current_work['value']:
                    work_display = _resolve_work_display(current_work['value'])
                    _make_chip(
                        f"{tr('Work / Title')}: {work_display}",
                        lambda: clear_filter('work'),
                    )

                if current_pgp_filter['value'] != 'all':
                    pgp_label = tr('Has PGP') if current_pgp_filter['value'] == 'has_pgp' else tr('No PGP')
                    _make_chip(
                        pgp_label,
                        lambda: clear_filter('pgp'),
                        color='green' if current_pgp_filter['value'] == 'has_pgp' else 'red',
                    )
                if current_editions_filter['value'] != 'all':
                    ed_label = tr('Has Scholarly Transcription') if current_editions_filter['value'] == 'has_edition' else tr('No Scholarly Transcription')
                    _make_chip(
                        ed_label,
                        lambda: clear_filter('editions'),
                        color='green' if current_editions_filter['value'] == 'has_edition' else 'red',
                    )

                # SEED-026 (smoke 2026-06-29): NO per-library chips — the library
                # filter state is shown on the library filter button itself
                # (red + "Filter Libraries (shown/total)"). See _update_library_filter_btn.

                if current_date_from['value'] is not None or current_date_to['value'] is not None:
                    df = current_date_from['value']
                    dt = current_date_to['value']
                    if df is not None and dt is not None:
                        date_label = f"{tr('Date')}: {df}-{dt}"
                    elif df is not None:
                        date_label = f"{tr('Date')}: {df}+"
                    else:
                        date_label = f"{tr('Date')}: -{dt}"
                    if current_include_undated['value']:
                        date_label += f" (+{tr('Include undated')})"
                    _make_chip(date_label, lambda: clear_filter('date'))

                for t in current_text_all['value']:
                    _make_chip(
                        f"ALL: {t}",
                        lambda term=t: remove_text_term('all', term),
                        color='blue',
                    )
                for t in current_text_any['value']:
                    _make_chip(
                        f"ANY: {t}",
                        lambda term=t: remove_text_term('any', term),
                        color='green',
                    )
                for t in current_text_not['value']:
                    _make_chip(
                        f"NOT: {t}",
                        lambda term=t: remove_text_term('not', term),
                        color='red',
                    )

                ui.button(tr('Clear All'), on_click=clear_all_filters).props(
                    'flat dense size=sm color=red'
                )

    def _build_text_filter_summary() -> str:
        """Build a human-readable summary of active text filters."""
        parts = []
        if current_text_all['value']:
            bold_terms = ' **&** '.join(f'**{t}**' for t in current_text_all['value'])
            parts.append(f"{tr('containing')} {bold_terms}")
        if current_text_any['value']:
            bold_terms = f' **{tr("or")}** '.join(f'**{t}**' for t in current_text_any['value'])
            parts.append(f"{tr('at least one of')} {bold_terms}")
        if current_text_not['value']:
            bold_terms = ', '.join(f'**{t}**' for t in current_text_not['value'])
            parts.append(f"{tr('excluding')} {bold_terms}")
        if not parts:
            return ''
        return ', '.join(parts)

    def _make_chip(text: str, on_remove, color: str = 'primary'):
        """Create a removable filter chip using button-style to avoid q-chip slot issues.

        q-chip 'remove' event fires from the chip's slot context which gets destroyed
        when render_chips() clears the container. Using on_click on a button avoids this
        because NiceGUI processes the click through its normal event pipeline.
        """
        _bg = {'primary': 'bg-blue-600', 'blue': 'bg-blue-600',
               'green': 'bg-green-600', 'red': 'bg-red-600'}.get(color, 'bg-blue-600')
        with ui.row().classes(f'{_bg} text-white rounded-full px-3 py-0.5 items-center gap-1 text-sm'):
            ui.label(text)
            ui.button(icon='close', on_click=on_remove).props(
                'flat round dense size=xs color=white'
            ).classes('ml-1')

    # ── Filter change handlers ─────────────────────────────────────
    async def on_domain_selected(domain_name: str, display_name: str = None):
        """Handle domain selection from tree."""
        if current_domain['value'] == domain_name:
            current_domain['value'] = None
            current_domain['display'] = None
        else:
            current_domain['value'] = domain_name
            current_domain['display'] = display_name or domain_name
        current_page['value'] = 1
        if author_select_ref['ref']:
            author_select_ref['ref'].value = None
        if work_select_ref['ref']:
            work_select_ref['ref'].value = None
        current_author['value'] = None
        current_work['value'] = None
        await fetch_authors()
        await fetch_works()
        await refresh_results()

    async def on_author_selected(e):
        """Handle author selection from select."""
        val = e.value
        if val and isinstance(val, str) and val.strip():
            current_author['value'] = val.strip()
        else:
            current_author['value'] = None
        current_page['value'] = 1
        if work_select_ref['ref']:
            work_select_ref['ref'].value = None
        current_work['value'] = None
        await fetch_works()
        await refresh_results()

    async def on_work_selected(e):
        """Handle work/title selection from select."""
        val = e.value
        if val and isinstance(val, str) and val.strip():
            current_work['value'] = val.strip()
        else:
            current_work['value'] = None
        current_page['value'] = 1
        await refresh_results()

    async def on_date_changed():
        """Handle date filter change from From/To inputs."""
        df_ref = date_from_ref['ref']
        dt_ref = date_to_ref['ref']
        df_val = df_ref.value if df_ref and df_ref.value else None
        dt_val = dt_ref.value if dt_ref and dt_ref.value else None
        try:
            current_date_from['value'] = int(df_val) if df_val else None
        except (ValueError, TypeError):
            current_date_from['value'] = None
        try:
            current_date_to['value'] = int(dt_val) if dt_val else None
        except (ValueError, TypeError):
            current_date_to['value'] = None
        current_page['value'] = 1
        await refresh_results()

    async def on_undated_changed(e):
        """Handle include-undated checkbox change."""
        current_include_undated['value'] = e.value
        if current_date_from['value'] is not None or current_date_to['value'] is not None:
            current_page['value'] = 1
            await refresh_results()

    # ── Availability filters (PGP / Editions) — SEED-023 ───────────
    _PGP_STATES = ('all', 'has_pgp', 'no_pgp')
    _ED_STATES = ('all', 'has_edition', 'no_edition')

    def _next_state(val, states):
        """Cycle to the next 3-state value; a corrupted/unknown value normalizes
        to the first state ('all') rather than raising (audit #12 lesson)."""
        idx = states.index(val) if val in states else -1
        return states[(idx + 1) % len(states)]

    def _update_pgp_filter_btn():
        btn = pgp_filter_btn_ref['ref']
        if not btn:
            return
        st = current_pgp_filter['value']
        btn.props(remove='color')
        if st == 'has_pgp':
            btn.text = tr('Has PGP')
            btn.props('outline dense no-caps color=green')
        elif st == 'no_pgp':
            btn.text = tr('No PGP')
            btn.props('outline dense no-caps color=red')
        else:
            btn.text = tr('Filter PGP')
            btn.props('outline dense no-caps color=primary')

    def _update_editions_filter_btn():
        btn = editions_filter_btn_ref['ref']
        if not btn:
            return
        st = current_editions_filter['value']
        btn.props(remove='color')
        if st == 'has_edition':
            btn.text = tr('Has Scholarly Transcription')
            btn.props('outline dense no-caps color=green')
        elif st == 'no_edition':
            btn.text = tr('No Scholarly Transcription')
            btn.props('outline dense no-caps color=red')
        else:
            btn.text = tr('Filter Scholarly Transcriptions')
            btn.props('outline dense no-caps color=primary')

    async def _toggle_pgp_filter():
        current_pgp_filter['value'] = _next_state(current_pgp_filter['value'], _PGP_STATES)
        # safe_user_set absorbs the prune-race AssertionError (Phase 87 chokepoint).
        safe_user_set('catalog_pgp_filter', current_pgp_filter['value'])
        _update_pgp_filter_btn()
        current_page['value'] = 1
        await refresh_results()

    async def _toggle_editions_filter():
        current_editions_filter['value'] = _next_state(current_editions_filter['value'], _ED_STATES)
        safe_user_set('catalog_editions_filter', current_editions_filter['value'])
        _update_editions_filter_btn()
        current_page['value'] = 1
        await refresh_results()

    # ── Library filter helpers — SEED-026 ─────────────────────────────────────
    # GAP-E (2026-06-28): The ui.select(multiple=True) was replaced with a button +
    # checkbox dialog (_open_library_filter_dialog), mirroring the web-search domain dialog.
    # Inclusion model (locked): checked = include; all-checked ⇒ [] (show all);
    # strict subset ⇒ that subset; zero-checked ⇒ Apply guarded (FINDING 1).

    def _update_library_filter_btn():
        """Sync the library filter button: 3-state label, count, and colour (DMF-08/DMF-12).

        Three states keyed on current_library_mode + active-ness:
          Neutral:          tr('Filter by library') — outline primary.
          Show-only active: 'Showing {shown}/{total} library/libraries' — filled negative/red.
          Hide active:      'Hiding {n} library/libraries' — filled deep-orange.

        Uses the REAL Phase-130 pluralized template keys (VERIFIED genizah_translations.py).
        """
        btn = library_filter_btn_ref['ref']
        if not btn:
            return
        codes = set(current_library_filter['value'])
        mode = current_library_mode['value']

        if mode == 'show_only' and codes:
            # Show-only active: total = selectable-universe (manuscripts present, no LOCAL).
            # Matches parallels.py:1510 — must NOT use static LIBRARY_CODES (Codex R5).
            total = len([c for c in library_codes_with_manuscripts() if c != 'LOCAL'])
            shown = len(codes)
            _lib_btn_key = ('Showing {shown}/{total} library' if total == 1
                            else 'Showing {shown}/{total} libraries')
            btn.text = tr(_lib_btn_key).format(shown=shown, total=total)
            btn.props(remove='color outline')
            btn.props('dense no-caps color=negative')
        elif mode == 'hide' and codes:
            # Hide active: show count of hidden libraries.
            _n = len(codes)
            _lib_btn_key = ('Hiding {n} library' if _n == 1 else 'Hiding {n} libraries')
            btn.text = tr(_lib_btn_key).format(n=_n)
            btn.props(remove='color outline')
            btn.props('dense no-caps color=deep-orange')
        else:
            # Neutral: no active filter (empty codes in either mode).
            btn.text = tr('Filter by library')
            btn.props(remove='color')
            btn.props('outline dense no-caps color=primary')

    def _library_apply_selection(checked_codes, all_codes):
        """Pure mapping helper: maps the checked set to the filter value.

        Contract (FINDING 1 apply mapping):
          - all checked ⇒ [] (clear filter; '[]' = show all — the existing data-layer sentinel)
          - strict non-empty subset ⇒ that subset (inclusion filter)
          - zero checked ⇒ NEVER CALLED (blocked by Apply guard + defensive short-circuit)

        All-checked is reached ONLY via 'Select All'; the all-unchecked state is
        intentionally unreachable via Apply so it can never collide with the '[]'=show-all
        sentinel.
        """
        if set(checked_codes) == set(all_codes):
            return []
        return list(checked_codes)

    def _open_library_filter_dialog():
        """Open dual-mode library filter dialog (131-04 / DMF-08/DMF-12 redesign).

        Dialog layout (mirrors web/pages/search.py _open_library_filter_dialog):
          1. Mode toggle (Show-only | Hide) — D-03.
          2. Text-search input filtering rows client-side.
          3. Count shortlist — ALWAYS true full-set facets from fjms.get_browse_library_facets
             (NO page-local fallback; facet-query failure renders shortlist without counts — Codex R3 F1).
          4. Sort affordance — count-desc / A-Z (DMF-12).
          5. Expandable section — libraries with manuscripts not in shortlist, A-Z — DMF-13.
          6. Select All / Select None / Apply / Cancel.

        Mode semantics (D-01/D-02):
          - Show-only: keep results whose library_code IN checked set.
          - Hide: keep results whose library_code NOT IN checked set.
          Empty Hide set = show all (applyable, D-08); empty Show-only = blocked.

        Show-all normalization (MEDIUM / D-05):
          Show-only + all-universe-checked -> _library_apply_selection -> [] ->
          normalize to neutral Hide/[] before persisting.
        """
        import html as _html
        import json as _json
        import uuid as _uuid

        _lang = get_language()
        container_id = f'cat-lib-filter-{_uuid.uuid4().hex[:8]}'
        current_filter = set(current_library_filter['value'])
        current_mode = [current_library_mode['value']]  # mutable cell for toggle

        # --- Build shortlist from TRUE full-set facets (DMF-12, Codex R3 F1) ---
        # ALWAYS from current_library_facets (fetched via fjms.get_browse_library_facets).
        # If the facet fetch failed the cell is {}: render shortlist without counts.
        facets = current_library_facets['value']
        shortlist_codes = sorted(
            [c for c in facets if c in LIBRARY_CODES and c != 'LOCAL'],
            key=lambda c: -facets[c],
        )
        shortlist_set = set(shortlist_codes)

        # --- Build expand section: libraries with manuscripts, not in shortlist, LOCAL excluded (DMF-13) ---
        _codes_with_mss = library_codes_with_manuscripts()
        expand_codes = sorted(
            [c for c in LIBRARY_CODES if c != 'LOCAL' and c not in shortlist_set
             and c in _codes_with_mss],
            key=lambda c: get_library_display(c, short=False, lang=_lang),
        )
        _all_for_norm = shortlist_codes + expand_codes

        def _make_cat_cb_row(code, label_text, checked, count=None):
            """Single checkbox row HTML for the catalog dialog (BUG-B safe — no <script>)."""
            code_attr = _html.escape(code, quote=True)
            label_esc = _html.escape(label_text, quote=True)
            checked_attr = 'checked' if checked else ''
            data_count = str(count) if count is not None else '0'
            return (
                f'<label class="cat-lib-cb-row" data-label="{label_esc.lower()}" '
                f'data-count="{data_count}" '
                f'style="display:flex;align-items:center;gap:8px;'
                f'padding:4px 0;cursor:pointer;font-size:0.9rem">'
                f'<input type="checkbox" class="cat-lib-cb" data-code="{code_attr}" '
                f'{checked_attr} '
                f'style="width:16px;height:16px;accent-color:#1976d2;cursor:pointer" '
                f'onchange="catLibFilterUpdateApply(\'{container_id}\')">'
                f'<span>{_html.escape(label_text)}</span></label>'
            )

        # Shortlist rows (with counts when available; without on facet-query failure)
        shortlist_rows = []
        for code in shortlist_codes:
            count = facets.get(code)
            label = get_library_display(code, short=False, lang=_lang)
            label_with_count = f"{label} ({count})" if count is not None else label
            if current_mode[0] == 'show_only':
                is_checked = (not current_filter) or (code in current_filter)
            else:
                is_checked = code in current_filter
            shortlist_rows.append(_make_cat_cb_row(code, label_with_count, is_checked, count))

        # Expand section rows (no count)
        expand_rows = []
        for code in expand_codes:
            label = get_library_display(code, short=False, lang=_lang)
            if current_mode[0] == 'show_only':
                is_checked = (not current_filter) or (code in current_filter)
            else:
                is_checked = code in current_filter
            expand_rows.append(_make_cat_cb_row(code, label, is_checked))

        # Wrap in data-libmode container so JS mode-awareness works.
        init_mode = _html.escape(current_mode[0], quote=True)
        shortlist_html = '\n'.join(shortlist_rows)
        full_html = f'<div id="{container_id}" data-libmode="{init_mode}">{shortlist_html}'
        if expand_rows:
            expand_label = _html.escape(tr('All libraries'))
            full_html += (
                f'<details style="margin-top:8px">'
                f'<summary style="cursor:pointer;font-size:0.85rem;color:#666;'
                f'padding:4px 0">{expand_label}</summary>'
                + '\n'.join(expand_rows) +
                f'</details>'
            )
        full_html += '</div>'

        with ui.dialog() as dialog, ui.card().classes('w-[520px] max-h-[80vh]'):
            with ui.column().classes('w-full gap-2'):
                ui.label(tr('Filter by Library')).classes('text-lg font-bold')

                # Mode toggle (D-03): Show-only | Hide, initialized from current mode.
                mode_options = {
                    'show_only': tr('Show only selected'),
                    'hide': tr('Hide selected'),
                }
                mode_toggle = ui.toggle(
                    options=mode_options,
                    value=current_mode[0],
                ).props('dense no-caps')

                def _on_mode_change(new_mode):
                    current_mode[0] = new_mode
                    # D-04: flipping mode resets checked set + re-syncs Apply-enable.
                    ui.run_javascript(f'catLibFilterSetMode("{container_id}", "{new_mode}")')

                mode_toggle.on_value_change(lambda e: _on_mode_change(e.value))

                # Text-search input — client-side filter (no Python round-trip).
                ui.input(
                    placeholder=tr('Search libraries...'),
                    on_change=lambda e: ui.run_javascript(
                        f'catLibFilterSearch("{container_id}", {_json.dumps(e.value or "")})'
                    ),
                ).props('dense clearable').classes('w-full')

                # Sort affordance (DMF-12): count-desc / A-Z.
                with ui.row().classes('gap-1 items-center'):
                    ui.label(tr('Sort:')).classes('text-xs').style('color:#888')
                    ui.button(
                        tr('By count'),
                        on_click=lambda: ui.run_javascript(
                            f'catLibFilterSort("{container_id}", "count")')
                    ).props('flat dense no-caps').classes('text-xs')
                    ui.button(
                        tr('A–Z'),
                        on_click=lambda: ui.run_javascript(
                            f'catLibFilterSort("{container_id}", "az")')
                    ).props('flat dense no-caps').classes('text-xs')

                with ui.scroll_area().classes('w-full').style('max-height: 45vh;'):
                    # BUG-B: NO <script> inside ui.html — JS is at page level (add_head_html).
                    ui.html(full_html, sanitize=False)

                # Buttons row
                with ui.row().classes('w-full justify-between'):
                    _cid = container_id  # capture for closures
                    _all = _all_for_norm  # full universe for show-all normalization

                    with ui.row().classes('gap-1'):
                        ui.button(
                            tr('Select All'),
                            on_click=lambda: ui.run_javascript(
                                f'catLibFilterSelectAll("{_cid}", true)')
                        ).props('flat dense no-caps')
                        ui.button(
                            tr('Select None'),
                            on_click=lambda: ui.run_javascript(
                                f'catLibFilterSelectAll("{_cid}", false)')
                        ).props('flat dense no-caps')

                    with ui.row().classes('gap-2'):
                        async def apply_catalog_library_filter():
                            checked_list = await ui.run_javascript(
                                f'catLibFilterGetChecked("{_cid}")', timeout=5.0
                            )
                            checked = list(checked_list) if checked_list else []
                            # Sanitize JS-returned codes (T-131-04-01): drops non-str,
                            # unknown codes, and 'LOCAL' (DMF-10 inline guard).
                            checked = sanitize_library_codes(checked)
                            committed_mode = current_mode[0]

                            if committed_mode == 'show_only':
                                # Show-only guard: empty checked set blocked (can't show nothing).
                                if not checked:
                                    ui.notify(
                                        tr('Select at least one library, or check all to clear the filter'),
                                        type='warning',
                                    )
                                    return
                                # All-universe-checked -> _library_apply_selection -> [] -> normalize.
                                new_filter = [c for c in _library_apply_selection(checked, _all)
                                              if c in LIBRARY_CODES and c != 'LOCAL']
                                # Show-all normalization (D-05): show_only + empty = neutral hide/[].
                                if not new_filter:
                                    current_library_mode['value'] = 'hide'
                                    current_library_filter['value'] = []
                                else:
                                    current_library_mode['value'] = 'show_only'
                                    current_library_filter['value'] = new_filter
                            else:
                                # Hide mode: empty set allowed (= hide nothing = show all, D-08).
                                current_library_mode['value'] = 'hide'
                                current_library_filter['value'] = checked

                            # Persist dict shape (D-09): NEVER a bare list (DMF-10).
                            safe_user_set('catalog_library_filter', {
                                'mode': current_library_mode['value'],
                                'codes': current_library_filter['value'],
                            })
                            _update_library_filter_btn()
                            current_page['value'] = 1
                            await refresh_results()
                            render_chips()
                            _update_search_buttons()
                            dialog.close()

                        apply_btn = ui.button(
                            tr('Apply'), on_click=apply_catalog_library_filter
                        ).props('dense no-caps color=primary')
                        apply_btn.props(f'id="catLibApplyBtn_{container_id}"')
                        ui.button(tr('Cancel'), on_click=dialog.close).props('flat dense no-caps')

        dialog.open()
        # Initialize Apply disabled-state from current checked count + mode.
        ui.run_javascript(f'catLibFilterUpdateApply("{container_id}")')

    # SEED-026 (smoke 2026-06-29): clear_library_code() was removed along with the
    # per-library chips. The Apply dialog (Show-only/Hide toggle + Select All / Select None / Apply)
    # is now the sole way to change the catalog library filter; the button shows the state.

    async def add_text_term():
        """Add a text term from the input with the current mode."""
        inp = text_input_ref['ref']
        mode_sel = text_mode_ref['ref']
        if not inp or not inp.value or not inp.value.strip():
            return
        term = inp.value.strip()
        mode = (mode_sel.value if mode_sel else 'all').lower()
        target = current_text_all if mode == 'all' else (
            current_text_any if mode == 'any' else current_text_not
        )
        if term not in target['value']:
            target['value'].append(term)
        inp.value = ''
        current_page['value'] = 1
        await refresh_results()

    async def remove_text_term(mode: str, term: str):
        """Remove a text term and refresh.

        Called via call_soon deferral from chip remove events so the chip's
        slot is no longer active when we clear/rebuild the container.
        """
        target = current_text_all if mode == 'all' else (
            current_text_any if mode == 'any' else current_text_not
        )
        if term in target['value']:
            target['value'].remove(term)
        current_page['value'] = 1
        await refresh_results()

    async def _set_century_range(lo_century: int, hi_century: int):
        """Set From/To spanning multiple centuries."""
        current_date_from['value'] = lo_century * 100
        current_date_to['value'] = hi_century * 100 + 99
        if date_from_ref['ref']:
            date_from_ref['ref'].value = str(current_date_from['value'])
        if date_to_ref['ref']:
            date_to_ref['ref'].value = str(current_date_to['value'])
        current_page['value'] = 1
        await refresh_results()

    async def on_century_preset(century: int):
        """Set From/To to a century range and refresh."""
        current_date_from['value'] = century * 100
        current_date_to['value'] = century * 100 + 99
        if date_from_ref['ref']:
            date_from_ref['ref'].value = str(current_date_from['value'])
        if date_to_ref['ref']:
            date_to_ref['ref'].value = str(current_date_to['value'])
        current_page['value'] = 1
        await refresh_results()

    async def clear_filter(filter_name: str):
        """Clear a specific filter and refresh."""
        if filter_name == 'domain':
            current_domain['value'] = None
            current_domain['display'] = None
        elif filter_name == 'author':
            current_author['value'] = None
            if author_select_ref['ref']:
                author_select_ref['ref'].value = None
        elif filter_name == 'work':
            current_work['value'] = None
            if work_select_ref['ref']:
                work_select_ref['ref'].value = None
        elif filter_name == 'date':
            current_date_from['value'] = None
            current_date_to['value'] = None
            current_include_undated['value'] = False
            if date_from_ref['ref']:
                date_from_ref['ref'].value = ''
            if date_to_ref['ref']:
                date_to_ref['ref'].value = ''
            if undated_checkbox_ref['ref']:
                undated_checkbox_ref['ref'].value = False
        elif filter_name == 'text':
            current_text_all['value'] = []
            current_text_any['value'] = []
            current_text_not['value'] = []
        elif filter_name == 'pgp':
            current_pgp_filter['value'] = 'all'
            safe_user_set('catalog_pgp_filter', 'all')
            _update_pgp_filter_btn()
        elif filter_name == 'editions':
            current_editions_filter['value'] = 'all'
            safe_user_set('catalog_editions_filter', 'all')
            _update_editions_filter_btn()
        elif filter_name == 'library':
            # SEED-026/DMF-08 (131-04): clear library filter — reset to neutral hide/[].
            # Persist dict shape (NEVER a bare list — D-09/DMF-10).
            current_library_filter['value'] = []
            current_library_mode['value'] = 'hide'
            safe_user_set('catalog_library_filter', {'mode': 'hide', 'codes': []})
            _update_library_filter_btn()
        current_page['value'] = 1
        await fetch_authors()
        await fetch_works()
        await refresh_results()

    async def clear_all_filters():
        """Clear all filters and refresh."""
        current_domain['value'] = None
        current_domain['display'] = None
        current_author['value'] = None
        current_work['value'] = None
        current_date_from['value'] = None
        current_date_to['value'] = None
        current_include_undated['value'] = False
        current_text_all['value'] = []
        current_text_any['value'] = []
        current_text_not['value'] = []
        current_pgp_filter['value'] = 'all'
        current_editions_filter['value'] = 'all'
        current_library_filter['value'] = []
        current_library_mode['value'] = 'hide'
        safe_user_set('catalog_pgp_filter', 'all')
        safe_user_set('catalog_editions_filter', 'all')
        # DMF-08 (131-04): persist dict shape — NEVER a bare list (D-09/DMF-10).
        safe_user_set('catalog_library_filter', {'mode': 'hide', 'codes': []})
        _update_pgp_filter_btn()
        _update_editions_filter_btn()
        _update_library_filter_btn()
        current_page['value'] = 1
        if author_select_ref['ref']:
            author_select_ref['ref'].value = None
        if work_select_ref['ref']:
            work_select_ref['ref'].value = None
        if date_from_ref['ref']:
            date_from_ref['ref'].value = ''
        if date_to_ref['ref']:
            date_to_ref['ref'].value = ''
        if undated_checkbox_ref['ref']:
            undated_checkbox_ref['ref'].value = False
        if text_input_ref['ref']:
            text_input_ref['ref'].value = ''
        await fetch_authors()
        await fetch_works()
        await refresh_results()

    # ── Browse-to-search navigation (Path B) ─────────────────────
    search_btn_ref = {'ref': None}
    parallels_btn_ref = {'ref': None}

    def _has_active_filters() -> bool:
        """Return True if any browse filter is currently active."""
        return any([
            current_domain['value'],
            current_author['value'],
            current_work['value'],
            current_date_from['value'] is not None,
            current_date_to['value'] is not None,
            current_text_all['value'],
            current_text_any['value'],
            current_text_not['value'],
            current_library_filter['value'],  # GAP-F: library selection enables "Search in these results"
        ])

    def _has_active_filters_excluding_library() -> bool:
        """Active filters that a TARGET page can actually apply, EXCLUDING library.

        Codex MEDIUM (2026-06-29): web Parallels has no library scoping — it ignores
        library_filter entirely. So a library-ONLY selection must NOT enable the
        "Parallels in these results" button (it would silently produce unscoped
        parallels). Library still enables the Search button via _has_active_filters().
        """
        return any([
            current_domain['value'],
            current_author['value'],
            current_work['value'],
            current_date_from['value'] is not None,
            current_date_to['value'] is not None,
            current_text_all['value'],
            current_text_any['value'],
            current_text_not['value'],
        ])

    def _build_incoming_filters(include_library: bool = True) -> dict:
        """Build incoming_filters dict from all active browse filters.

        include_library=False strips the library selection from the handoff — used for
        the Parallels target, which cannot apply a library filter (Codex MEDIUM).
        """
        incoming = {}
        if current_domain['value']:
            incoming['domain'] = current_domain['value']
        if current_author['value']:
            incoming['author'] = current_author['value']
            # Include display name for chip bar on target page
            incoming['author_name'] = _resolve_author_display(current_author['value'])
        if current_work['value']:
            incoming['work'] = current_work['value']
            incoming['work_name'] = _resolve_work_display(current_work['value'])
        if current_date_from['value'] is not None:
            incoming['date_from'] = current_date_from['value']
        if current_date_to['value'] is not None:
            incoming['date_to'] = current_date_to['value']
        # Text filters carry over as lists
        if current_text_all['value']:
            incoming['text_all'] = current_text_all['value']
        if current_text_any['value']:
            incoming['text_any'] = current_text_any['value']
        if current_text_not['value']:
            incoming['text_not'] = current_text_not['value']
        # GAP-F / DMF-08 (131-04): carry library filter to /search with FULL {mode,codes} dict
        # so a catalog Hide selection arrives at /search as Hide (Codex R1 HIGH #3).
        # consume_incoming_filters() (filter_panel.py Task 1) accepts the dict shape.
        # Parallels passes include_library=False so the library scope isn't silently dropped.
        if include_library and current_library_filter['value']:
            incoming['library_filter'] = {
                'mode': current_library_mode['value'],
                'codes': list(current_library_filter['value']),
            }
        return incoming

    def _update_search_buttons():
        """Enable/disable browse-to-search buttons based on active filters."""
        active = _has_active_filters()
        if search_btn_ref['ref']:
            if active:
                search_btn_ref['ref'].props(remove='disable')
            else:
                search_btn_ref['ref'].props('disable')
        # Codex MEDIUM (2026-06-29): Parallels can't apply a library filter, so a
        # library-ONLY selection must not enable it (would yield unscoped parallels).
        parallels_active = _has_active_filters_excluding_library()
        if parallels_btn_ref['ref']:
            if parallels_active:
                parallels_btn_ref['ref'].props(remove='disable')
            else:
                parallels_btn_ref['ref'].props('disable')

    def _search_in_results():
        """Navigate to search page with all active browse filters pre-populated."""
        incoming = _build_incoming_filters()
        if not incoming:
            return
        # Class N/A — bare write, no wrapper. safe_user_set absorbs prune-race.
        safe_user_set('incoming_filters', incoming)
        ui.navigate.to('/search?from_browse=1')

    def _parallels_in_results():
        """Navigate to parallels page with all active browse filters pre-populated.

        Codex MEDIUM (2026-06-29): Parallels cannot apply a library filter — strip it
        from the handoff (include_library=False) so it is not silently dropped, and tell
        the user if they had a library selection that won't carry over.
        """
        incoming = _build_incoming_filters(include_library=False)
        if not incoming:
            return
        if current_library_filter['value']:
            ui.notify(tr('Library filter is not applied to parallel search'), type='info')
        # Class N/A — bare write, no wrapper. safe_user_set absorbs prune-race.
        safe_user_set('incoming_filters', incoming)
        ui.navigate.to('/parallels')

    # ══════════════════════════════════════════════════════════════
    # Page Layout (single-pass build)
    # ══════════════════════════════════════════════════════════════

    # BUG-B fix: catalog library filter JS helpers at page level (NOT inside ui.html).
    # ui.html() raises ValueError if a <script> tag is present; define once here instead.
    # DMF-12 (131-04): extended with catLibFilterSetMode (D-04 reset), catLibFilterSearch
    # (text filter), catLibFilterSort (count-desc / A-Z sort), and mode-aware Apply-enable.
    ui.add_head_html('''<script>
    // DMF (131-04) — mode-aware Apply enable:
    // Show-only: zero-checked DISABLES Apply (can\'t show nothing).
    // Hide: zero-checked LEAVES Apply ENABLED (hide nothing = show all = valid).
    function catLibFilterUpdateApply(cid) {
        var cont = document.getElementById(cid);
        if (!cont) return;
        var cbs = cont.querySelectorAll('.cat-lib-cb');
        var n = 0;
        cbs.forEach(function(cb) { if (cb.checked) n++; });
        var btn = document.getElementById('catLibApplyBtn_' + cid);
        if (!btn) return;
        var mode = cont.getAttribute('data-libmode') || 'hide';
        btn.disabled = (mode === 'show_only' && n === 0);
    }
    function catLibFilterGetChecked(cid) {
        var cont = document.getElementById(cid);
        if (!cont) return [];
        var result = [];
        cont.querySelectorAll('.cat-lib-cb:checked').forEach(function(cb) { result.push(cb.dataset.code); });
        return result;
    }
    function catLibFilterSelectAll(cid, val) {
        var cont = document.getElementById(cid);
        if (!cont) return;
        cont.querySelectorAll('.cat-lib-cb').forEach(function(cb) { cb.checked = val; });
        catLibFilterUpdateApply(cid);
    }
    // Set the mode attribute and reset checkboxes (D-04: flipping mode clears selection).
    function catLibFilterSetMode(cid, mode) {
        var cont = document.getElementById(cid);
        if (!cont) return;
        cont.setAttribute('data-libmode', mode);
        cont.querySelectorAll('.cat-lib-cb').forEach(function(cb) { cb.checked = false; });
        catLibFilterUpdateApply(cid);
    }
    // Text-search row filter: hide rows whose label does not contain the query string.
    function catLibFilterSearch(cid, query) {
        var cont = document.getElementById(cid);
        if (!cont) return;
        var q = query.toLowerCase().trim();
        cont.querySelectorAll('.cat-lib-cb-row').forEach(function(row) {
            if (!q) { row.style.display = ''; return; }
            var label = (row.getAttribute('data-label') || '').toLowerCase();
            row.style.display = (label.indexOf(q) >= 0) ? '' : 'none';
        });
    }
    // Sort rows by count-desc or A-Z (DMF-12).
    function catLibFilterSort(cid, key) {
        var cont = document.getElementById(cid);
        if (!cont) return;
        var rows = Array.from(cont.querySelectorAll('.cat-lib-cb-row'));
        rows.sort(function(a, b) {
            if (key === 'count') {
                return (parseInt(b.getAttribute('data-count') || '0') -
                        parseInt(a.getAttribute('data-count') || '0'));
            } else {
                var la = (a.getAttribute('data-label') || '').toLowerCase();
                var lb = (b.getAttribute('data-label') || '').toLowerCase();
                return la < lb ? -1 : la > lb ? 1 : 0;
            }
        });
        rows.forEach(function(r) { cont.appendChild(r); });
    }
    </script>''')

    dir_attr = 'rtl' if rtl else 'ltr'

    with ui.column().classes('w-full max-w-full gap-0').style(f'direction: {dir_attr}'):

        # ── Page Header ────────────────────────────────────────────
        with ui.column().classes('w-full mb-4'):
            h1(tr('Browse by Identification'), classes='text-3xl font-bold mb-6 text-center', style='color: var(--primary-700)')
            ui.label(
                tr(
                    'Browse the manuscript corpus by scholarly domain classifications, '
                    'author attributions, and work identifications.'
                )
            ).style('color: var(--text-secondary)')

        # ── Active Filter Chips ────────────────────────────────────
        chips_container['ref'] = ui.column().classes('w-full')

        # ── Browse-to-Search buttons ─────────────────────────────
        with ui.row().classes('w-full items-center gap-2 py-1'):
            sb = ui.button(
                tr('Search in these results'),
                icon='search',
                on_click=_search_in_results,
            ).props('flat dense no-caps disable color=primary').classes('text-sm')
            search_btn_ref['ref'] = sb

            pb = ui.button(
                tr('Parallel search in these results'),
                icon='compare_arrows',
                on_click=_parallels_in_results,
            ).props('flat dense no-caps disable color=secondary').classes('text-sm')
            parallels_btn_ref['ref'] = pb

        # ── Two-column layout ──────────────────────────────────────
        with ui.row().classes('w-full gap-0 flex-nowrap items-start'):

            # ── Sidebar ────────────────────────────────────────────
            sidebar_col = ui.column().classes(
                'w-72 min-w-[280px] shrink-0 pr-4'
            ).style('position: sticky; top: 70px; align-self: flex-start;')
            sidebar_ref['ref'] = sidebar_col

            with sidebar_col:
              with ui.scroll_area().classes('w-full').style(
                  'height: calc(100vh - 200px);'
              ).props('visible'):

                # Domain Tree Card
                with ui.card().classes('w-full p-4'):
                    ui.label(tr('Domain')).classes(
                        'text-sm font-bold uppercase tracking-wide mb-2'
                    ).style('color: var(--text-secondary)')
                    hierarchy = fjms.get_domain_hierarchy()
                    unclassified_count = fjms.get_unclassified_count()

                    with ui.scroll_area().classes('w-full').style('max-height: 400px;'):
                        for parent_name, info in hierarchy.items():
                            parent_display = (
                                info.get('parent_domain_heb', parent_name)
                                if lang == 'he' else parent_name
                            )
                            parent_count = info.get('count', 0)
                            children = info.get('children', [])

                            if children:
                                with ui.expansion(
                                    f"{parent_display} ({parent_count:,})",
                                ).classes('w-full') as exp:
                                    exp.on(
                                        'click',
                                        lambda e, pn=parent_name, pd=parent_display: on_domain_selected(pn, pd),
                                    )
                                    for child in children:
                                        child_display = (
                                            child.get('domain_heb', child['domain'])
                                            if lang == 'he' else child['domain']
                                        )
                                        child_count = child.get('count', 0)
                                        subchildren = child.get('children', [])

                                        if subchildren:
                                            # Sub-domain with sub-sub-domains: nested expansion
                                            with ui.expansion(
                                                f"{child_display} ({child_count:,})",
                                            ).classes('w-full').props('dense') as sub_exp:
                                                sub_exp.on(
                                                    'click',
                                                    lambda e, cn=child['domain'], cd=child_display: on_domain_selected(cn, cd),
                                                )
                                                for sc in subchildren:
                                                    sc_display = (
                                                        sc.get('domain_heb', sc['domain'])
                                                        if lang == 'he' else sc['domain']
                                                    )
                                                    sc_count = sc.get('count', 0)
                                                    ui.button(
                                                        f"{sc_display} ({sc_count:,})",
                                                        on_click=lambda e, sn=sc['domain'], sd=sc_display: on_domain_selected(sn, sd),
                                                    ).props('flat dense align=left no-caps').classes(
                                                        'w-full text-left text-sm pl-4'
                                                    )
                                        else:
                                            ui.button(
                                                f"{child_display} ({child_count:,})",
                                                on_click=lambda e, cn=child['domain'], cd=child_display: on_domain_selected(cn, cd),
                                            ).props('flat dense align=left no-caps').classes(
                                                'w-full text-left text-sm'
                                            )
                            else:
                                ui.button(
                                    f"{parent_display} ({parent_count:,})",
                                    on_click=lambda e, pn=parent_name, pd=parent_display: on_domain_selected(pn, pd),
                                ).props('flat dense align=left no-caps').classes(
                                    'w-full text-left text-sm'
                                )

                        # Unclassified bucket
                        if unclassified_count > 0:
                            ui.separator().classes('my-2')
                            ui.label(
                                f"{tr('Unclassified')} ({unclassified_count:,})"
                            ).classes('text-sm pl-2 py-1').style('color: var(--text-muted)')

                # Availability Filter Card (PGP / Editions) — SEED-023
                with ui.card().classes('w-full p-4 mt-2'):
                    ui.label(tr('Filter by availability')).classes(
                        'text-sm font-bold uppercase tracking-wide mb-2'
                    ).style('color: var(--text-secondary)')
                    with ui.row().classes('w-full flex-wrap gap-2'):
                        pgp_btn = ui.button(
                            tr('Filter PGP'),
                            on_click=lambda: _toggle_pgp_filter(),
                        ).props('outline dense no-caps color=primary').classes('text-sm')
                        pgp_btn.tooltip(tr('Filter by PGP information'))
                        pgp_filter_btn_ref['ref'] = pgp_btn

                        ed_btn = ui.button(
                            tr('Filter Scholarly Transcriptions'),
                            on_click=lambda: _toggle_editions_filter(),
                        ).props('outline dense no-caps color=primary').classes('text-sm')
                        ed_btn.tooltip(
                            tr('Filter by scholarly transcription (PGP or FGP edition)')
                        )
                        editions_filter_btn_ref['ref'] = ed_btn
                    _update_pgp_filter_btn()
                    _update_editions_filter_btn()

                # Library Filter Card — SEED-026 (GAP-E: checkbox dialog replaces ui.select)
                with ui.card().classes('w-full p-4 mt-2'):
                    ui.label(tr('Filter by library')).classes(
                        'text-sm font-bold uppercase tracking-wide mb-2'
                    ).style('color: var(--text-secondary)')
                    # D-01: EN/HE display; D-02: plain list, no per-library counts.
                    # GAP-E: button opens a checkbox dialog (_open_library_filter_dialog)
                    # instead of the old ui.select(multiple=True).  The dialog uses the
                    # same HTML+JS readback pattern as the web-search library dialog.
                    lib_btn = ui.button(
                        tr('Filter by library'),
                        on_click=lambda: _open_library_filter_dialog(),
                    ).props('outline dense no-caps color=primary').classes('w-full text-sm')
                    lib_btn.tooltip(tr('Filter results by library'))
                    library_filter_btn_ref['ref'] = lib_btn
                    _update_library_filter_btn()

                # Author Search Card
                with ui.card().classes('w-full p-4 mt-2'):
                    ui.label(tr('Author')).classes(
                        'text-sm font-bold uppercase tracking-wide mb-2'
                    ).style('color: var(--text-secondary)')
                    author_sel = ui.select(
                        options={},
                        with_input=True,
                        on_change=on_author_selected,
                        label=tr('Search authors...'),
                    ).props('dense outlined clearable use-input input-debounce=300').classes('w-full')
                    if initial_author:
                        author_sel.value = initial_author
                    author_select_ref['ref'] = author_sel

                # Work/Title Search Card
                with ui.card().classes('w-full p-4 mt-2'):
                    ui.label(tr('Work / Title')).classes(
                        'text-sm font-bold uppercase tracking-wide mb-2'
                    ).style('color: var(--text-secondary)')
                    work_sel = ui.select(
                        options={},
                        with_input=True,
                        on_change=on_work_selected,
                        label=tr('Search works...'),
                    ).props('dense outlined clearable use-input input-debounce=300').classes('w-full')
                    if initial_work:
                        work_sel.value = initial_work
                    work_select_ref['ref'] = work_sel

                # Date Filter Card
                with ui.card().classes('w-full p-4 mt-2'):
                    ui.label(tr('Date')).classes(
                        'text-sm font-bold uppercase tracking-wide mb-2'
                    ).style('color: var(--text-secondary)')

                    with ui.row().classes('w-full items-center gap-2'):
                        df_input = ui.input(
                            label=tr('From year'),
                        ).props('dense outlined type=number').classes('flex-1').style('min-width: 0')
                        df_input.on('change', lambda _: on_date_changed())
                        date_from_ref['ref'] = df_input

                        ui.label('–').style('color: var(--text-muted)')

                        dt_input = ui.input(
                            label=tr('To year'),
                        ).props('dense outlined type=number').classes('flex-1').style('min-width: 0')
                        dt_input.on('change', lambda _: on_date_changed())
                        date_to_ref['ref'] = dt_input

                    # Century presets
                    with ui.row().classes('w-full flex-wrap gap-1 mt-2'):
                        for c in range(9, 15):
                            btn_label = tr(f"{c+1}th")
                            ui.button(
                                btn_label,
                                on_click=lambda e, cent=c: on_century_preset(cent),
                            ).props('flat dense size=xs no-caps').classes('text-xs')
                        ui.button(
                            tr("16-19"),
                            on_click=lambda e, lo=16, hi=19: _set_century_range(lo, hi),
                        ).props('flat dense size=xs no-caps').classes('text-xs')

                    undated_cb = ui.checkbox(
                        tr('Include undated'),
                        on_change=on_undated_changed,
                    ).classes('mt-1').props('dense')
                    undated_checkbox_ref['ref'] = undated_cb

                # Text Filter Card
                with ui.card().classes('w-full p-4 mt-2'):
                    ui.label(tr('Text Filter')).classes(
                        'text-sm font-bold uppercase tracking-wide mb-2'
                    ).style('color: var(--text-secondary)')

                    _mode_options = {
                        'all': tr('ALL'),
                        'any': tr('ANY'),
                        'not': tr('NOT'),
                    }
                    _mode_tooltips = {
                        'all': tr('Must contain ALL terms'),
                        'any': tr('Must contain at least ONE term'),
                        'not': tr('Must NOT contain the term'),
                    }

                    with ui.row().classes('w-full items-center gap-1'):
                        mode_sel = ui.select(
                            options=_mode_options,
                            value='all',
                        ).props('dense outlined').classes('').style(
                            'width: 90px; min-width: 90px'
                        ).tooltip(_mode_tooltips['all'])
                        text_mode_ref['ref'] = mode_sel

                        def _update_mode_tooltip(e):
                            sel = text_mode_ref['ref']
                            if sel and e.value in _mode_tooltips:
                                sel.tooltip(_mode_tooltips[e.value])

                        mode_sel.on('update:model-value', _update_mode_tooltip)

                        txt_input = ui.input(
                            placeholder=tr('Type term, press Enter'),
                        ).props('dense outlined').classes('flex-1').style('min-width: 0')
                        txt_input.on('keydown.enter', lambda _: add_text_term())
                        text_input_ref['ref'] = txt_input

            # ── Sidebar toggle button ──────────────────────────────
            toggle_icon = 'chevron_left' if not rtl else 'chevron_right'
            toggle_btn = ui.button(
                icon=toggle_icon,
                on_click=toggle_sidebar,
            ).props('flat dense round size=sm').classes(
                'self-start mt-1'
            ).tooltip(tr('Toggle sidebar'))
            sidebar_toggle_ref['ref'] = toggle_btn

            # ── Main content area ──────────────────────────────────
            with ui.column().classes('flex-grow min-w-0'):
                loading_spinner['ref'] = ui.spinner('dots', size='lg').classes('self-center my-8')
                loading_spinner['ref'].set_visibility(False)

                results_container['ref'] = ui.column().classes('w-full')
                pagination_container['ref'] = ui.column().classes('w-full')

    # ── Initial data load ──────────────────────────────────────────
    _has_url_params = any([
        initial_domain, initial_author, initial_work,
        initial_text_all, initial_text_any, initial_text_not,
    ])

    async def _restore_session_filters():
        """Restore filters from sessionStorage if no URL params were provided."""
        if _has_url_params:
            return False
        import json as _json
        raw = await ui.run_javascript(
            "sessionStorage.getItem('catalog_browse_filters')", timeout=2.0
        )
        if not raw:
            return False
        try:
            state = _json.loads(raw)
        except (ValueError, TypeError):
            return False
        restored = False
        if state.get('domain'):
            current_domain['value'] = state['domain']
            current_domain['display'] = state.get('domain_display') or state['domain']
            restored = True
        if state.get('author'):
            current_author['value'] = state['author']
            if author_select_ref['ref']:
                author_select_ref['ref'].value = state['author']
            restored = True
        if state.get('work'):
            current_work['value'] = state['work']
            if work_select_ref['ref']:
                work_select_ref['ref'].value = state['work']
            restored = True
        if state.get('date_from') is not None:
            current_date_from['value'] = state['date_from']
            if date_from_ref['ref']:
                date_from_ref['ref'].value = str(state['date_from'])
            restored = True
        if state.get('date_to') is not None:
            current_date_to['value'] = state['date_to']
            if date_to_ref['ref']:
                date_to_ref['ref'].value = str(state['date_to'])
            restored = True
        if state.get('undated'):
            current_include_undated['value'] = True
            if undated_checkbox_ref['ref']:
                undated_checkbox_ref['ref'].value = True
        if state.get('text_all'):
            current_text_all['value'] = state['text_all']
            restored = True
        if state.get('text_any'):
            current_text_any['value'] = state['text_any']
            restored = True
        if state.get('text_not'):
            current_text_not['value'] = state['text_not']
            restored = True
        return restored

    async def initial_load():
        """Load initial data and fetch results on page open."""
        await _restore_session_filters()
        await fetch_authors()
        await fetch_works()
        await refresh_results()

    async def _deferred_initial_load():
        await asyncio.sleep(0.1)
        try:
            await initial_load()
        except Exception:
            pass  # Deferred UI refresh failed; page still usable
    asyncio.ensure_future(_deferred_initial_load())
