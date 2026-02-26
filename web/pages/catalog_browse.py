# -*- coding: utf-8 -*-
"""
Catalog Browse page for GenizahSearch web application.

Features:
- Collapsible domain tree navigation (FJMS domain hierarchy)
- Author search-as-you-type with cross-filtering
- Work/title search-as-you-type with cross-filtering
- Combined multi-axis filtering with removable chips
- Paginated results table with shelfmark, library, domain, identification, date
- Deep linking via URL query params
- RTL Hebrew support
"""

from nicegui import ui, run
from web.services import get_service
from web.translations import tr, is_rtl, get_language
from web.components.typography import h1
from shared.fjms_service import get_fjms_service

import logging

logger = logging.getLogger(__name__)

PAGE_SIZE = 50


def create_catalog_browse_page(
    initial_domain: str = None,
    initial_author: str = None,
    initial_work: str = None,
    initial_page: int = None,
):
    """Create the catalog browse page with domain tree, author/work search, and results."""

    fjms = get_fjms_service(thread_safe=True)
    service = get_service()
    lang = get_language()
    rtl = is_rtl()

    # ── State ──────────────────────────────────────────────────────
    current_domain = {'value': initial_domain}
    current_author = {'value': initial_author}
    current_work = {'value': initial_work}
    current_page = {'value': (initial_page or 1)}

    # Cached lists for cross-filtering
    authors_list = {'data': []}
    works_list = {'data': []}

    # UI element references (assigned during layout build)
    results_container = {'ref': None}
    chips_container = {'ref': None}
    pagination_container = {'ref': None}
    author_input_ref = {'ref': None}
    work_input_ref = {'ref': None}
    loading_spinner = {'ref': None}

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
        if current_page['value'] and current_page['value'] > 1:
            params.append(f"page={current_page['value']}")
        qs = '?' + '&'.join(params) if params else ''
        ui.run_javascript(
            f"history.replaceState(null, '', '/catalog-browse{qs}')"
        )

    # ── Data fetch helpers ─────────────────────────────────────────
    async def fetch_authors():
        """Fetch authors list (optionally filtered by current domain)."""
        data = await run.io_bound(fjms.get_browse_authors, current_domain['value'])
        authors_list['data'] = data
        return data

    async def fetch_works():
        """Fetch works list (optionally filtered by current domain + author)."""
        data = await run.io_bound(
            fjms.get_browse_works,
            current_domain['value'],
            current_author['value'],
        )
        works_list['data'] = data
        return data

    async def fetch_results():
        """Fetch paginated browse results for current filters."""
        offset = (current_page['value'] - 1) * PAGE_SIZE
        data = await run.io_bound(
            fjms.get_browse_results,
            current_domain['value'],
            current_author['value'],
            current_work['value'],
            offset,
            PAGE_SIZE,
        )
        return data

    # ── Resolve shelfmark / library from MetadataManager ───────────
    def resolve_meta(sys_id: str):
        """Return (shelfmark, library_code) for a given sys_id."""
        shelfmark = ''
        library_code = ''
        try:
            if service and service.mm:
                sm, _title = service.mm.get_meta_for_id(sys_id)
                shelfmark = sm or ''
                library_code = service.mm.get_library_for_id(sys_id) or ''
        except Exception:
            pass
        return shelfmark, library_code

    # ── Refresh results table ──────────────────────────────────────
    async def refresh_results():
        """Refresh the results table and pagination based on current filters."""
        if loading_spinner['ref']:
            loading_spinner['ref'].set_visibility(True)

        try:
            data = await fetch_results()
        except Exception as e:
            logger.error(f"catalog_browse fetch_results error: {e}")
            data = {"results": [], "total": 0}

        if loading_spinner['ref']:
            loading_spinner['ref'].set_visibility(False)

        results = data.get('results', [])
        total = data.get('total', 0)

        # Build table rows with resolved shelfmarks
        rows = []
        for r in results:
            sid = r.get('sys_id', '')
            shelfmark, library_code = resolve_meta(sid)

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

            rows.append({
                'sys_id': sid,
                'shelfmark': shelfmark or sid,
                'library': library_code,
                'domain': domain_display,
                'identification': identification,
                'date': date_val,
            })

        render_results_table(rows, total)
        render_pagination(total)
        render_chips()
        update_url()

    # ── Render: Results table ──────────────────────────────────────
    def render_results_table(rows, total):
        """Render the results table inside the results container."""
        container = results_container['ref']
        if not container:
            return
        container.clear()

        with container:
            if not rows:
                with ui.column().classes('w-full items-center py-12'):
                    ui.icon('search_off').classes('text-5xl text-gray-400 mb-4')
                    ui.label(tr('No manuscripts match the current filters')).classes(
                        'text-lg text-gray-500'
                    )
                return

            # Count label
            page_num = current_page['value']
            start = (page_num - 1) * PAGE_SIZE + 1
            end = min(page_num * PAGE_SIZE, total)
            count_text = f"{tr('Showing')} {start}-{end} {tr('of')} {total:,} {tr('manuscripts')}"
            ui.label(count_text).classes('text-sm text-gray-500 mb-2')

            # Table
            columns = [
                {'name': 'shelfmark', 'label': tr('Shelfmark'), 'field': 'shelfmark', 'align': 'left', 'sortable': True},
                {'name': 'library', 'label': tr('Library'), 'field': 'library', 'align': 'left', 'sortable': True},
                {'name': 'domain', 'label': tr('Domain'), 'field': 'domain', 'align': 'left', 'sortable': True},
                {'name': 'identification', 'label': tr('Identification'), 'field': 'identification', 'align': 'left', 'sortable': True},
                {'name': 'date', 'label': tr('Date'), 'field': 'date', 'align': 'left', 'sortable': True},
            ]
            table = ui.table(
                columns=columns,
                rows=rows,
                row_key='sys_id',
            ).classes('w-full cursor-pointer').props('flat bordered dense')

            # Row click handler -- navigate to browse page
            table.on('row-click', lambda e: ui.navigate.to(f"/browse?sys_id={e.args[1]['sys_id']}"))

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
                prev_btn = ui.button(
                    tr('Previous'),
                    icon='chevron_left',
                    on_click=lambda: go_to_page(page_num - 1),
                ).props('flat')
                if page_num <= 1:
                    prev_btn.props('disable')

                ui.label(f"{tr('Page')} {page_num} / {total_pages}").classes(
                    'text-sm text-gray-600'
                )

                next_btn = ui.button(
                    tr('Next'),
                    icon='chevron_right',
                    on_click=lambda: go_to_page(page_num + 1),
                ).props('flat')
                if page_num >= total_pages:
                    next_btn.props('disable')

    async def go_to_page(page_num):
        """Navigate to a specific page."""
        current_page['value'] = max(1, page_num)
        await refresh_results()

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
        ])

        if not has_filters:
            return

        with container:
            with ui.row().classes('w-full items-center gap-2 flex-wrap py-2'):
                if current_domain['value']:
                    _make_chip(
                        f"{tr('Domain')}: {current_domain['value']}",
                        lambda: clear_filter('domain'),
                    )
                if current_author['value']:
                    _make_chip(
                        f"{tr('Author')}: {current_author['value']}",
                        lambda: clear_filter('author'),
                    )
                if current_work['value']:
                    _make_chip(
                        f"{tr('Work / Title')}: {current_work['value']}",
                        lambda: clear_filter('work'),
                    )

                ui.button(tr('Clear All'), on_click=clear_all_filters).props(
                    'flat dense size=sm color=red'
                )

    def _make_chip(text: str, on_remove):
        """Create a removable filter chip."""
        with ui.element('q-chip').props('removable color=primary text-color=white').on(
            'remove', lambda: on_remove()
        ):
            ui.label(text).classes('text-sm')

    # ── Filter change handlers ─────────────────────────────────────
    async def on_domain_selected(domain_name: str):
        """Handle domain selection from tree."""
        if current_domain['value'] == domain_name:
            current_domain['value'] = None
        else:
            current_domain['value'] = domain_name
        current_page['value'] = 1
        await fetch_authors()
        await fetch_works()
        await refresh_results()

    async def on_author_selected(author_name: str):
        """Handle author selection from autocomplete."""
        current_author['value'] = author_name
        current_page['value'] = 1
        await fetch_works()
        await refresh_results()

    async def on_work_selected(work_title: str):
        """Handle work/title selection from autocomplete."""
        current_work['value'] = work_title
        current_page['value'] = 1
        await refresh_results()

    async def clear_filter(filter_name: str):
        """Clear a specific filter and refresh."""
        if filter_name == 'domain':
            current_domain['value'] = None
        elif filter_name == 'author':
            current_author['value'] = None
            if author_input_ref['ref']:
                author_input_ref['ref'].value = ''
        elif filter_name == 'work':
            current_work['value'] = None
            if work_input_ref['ref']:
                work_input_ref['ref'].value = ''
        current_page['value'] = 1
        await fetch_authors()
        await fetch_works()
        await refresh_results()

    async def clear_all_filters():
        """Clear all filters and refresh."""
        current_domain['value'] = None
        current_author['value'] = None
        current_work['value'] = None
        current_page['value'] = 1
        if author_input_ref['ref']:
            author_input_ref['ref'].value = ''
        if work_input_ref['ref']:
            work_input_ref['ref'].value = ''
        await fetch_authors()
        await fetch_works()
        await refresh_results()

    # ── Author input handlers ──────────────────────────────────────
    async def on_author_input(e):
        """Filter authors on input and show autocomplete options."""
        query = (e.value or '').strip().lower()
        if not query or not author_input_ref['ref']:
            if author_input_ref['ref']:
                author_input_ref['ref'].set_autocomplete([])
            return
        options = [
            a['author']
            for a in authors_list['data']
            if a['author'].lower().startswith(query) or query in a['author'].lower()
        ][:20]
        author_input_ref['ref'].set_autocomplete(options)

    async def on_author_change(e):
        """Handle author value committed (Enter or autocomplete pick)."""
        val = e.value if hasattr(e, 'value') else (e.args if hasattr(e, 'args') else '')
        if isinstance(val, str) and val.strip():
            await on_author_selected(val.strip())

    # ── Work input handlers ────────────────────────────────────────
    async def on_work_input(e):
        """Filter works on input and show autocomplete options."""
        query = (e.value or '').strip().lower()
        if not query or not work_input_ref['ref']:
            if work_input_ref['ref']:
                work_input_ref['ref'].set_autocomplete([])
            return
        options = []
        for w in works_list['data']:
            display = w.get('title_heb', '') if lang == 'he' else w.get('title', '')
            if not display:
                display = w.get('title', '') or w.get('title_heb', '')
            if display and (display.lower().startswith(query) or query in display.lower()):
                options.append(display)
            if len(options) >= 20:
                break
        work_input_ref['ref'].set_autocomplete(options)

    async def on_work_change(e):
        """Handle work value committed."""
        val = e.value if hasattr(e, 'value') else (e.args if hasattr(e, 'args') else '')
        if isinstance(val, str) and val.strip():
            await on_work_selected(val.strip())

    # ══════════════════════════════════════════════════════════════
    # Page Layout (single-pass build)
    # ══════════════════════════════════════════════════════════════

    dir_attr = 'rtl' if rtl else 'ltr'

    with ui.column().classes('w-full gap-0').style(f'direction: {dir_attr}'):

        # ── Page Header ────────────────────────────────────────────
        with ui.column().classes('w-full mb-4'):
            h1(tr('Browse by Identification'))
            ui.label(
                tr(
                    'Browse the manuscript corpus by scholarly domain classifications, '
                    'author attributions, and work identifications.'
                )
            ).classes('text-gray-600')

        # ── Active Filter Chips ────────────────────────────────────
        chips_container['ref'] = ui.column().classes('w-full')

        # ── Two-column layout ──────────────────────────────────────
        with ui.row().classes('w-full gap-4 flex-nowrap items-start'):

            # ── Sidebar ────────────────────────────────────────────
            with ui.column().classes('w-72 min-w-[280px] shrink-0'):

                # Domain Tree Card
                with ui.card().classes('w-full p-4'):
                    ui.label(tr('Domain')).classes(
                        'text-sm font-bold text-gray-600 uppercase tracking-wide mb-2'
                    )
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
                                    # Parent click filters to this parent domain
                                    exp.on(
                                        'click',
                                        lambda e, pn=parent_name: on_domain_selected(pn),
                                    )
                                    for child in children:
                                        child_display = (
                                            child.get('domain_heb', child['domain'])
                                            if lang == 'he' else child['domain']
                                        )
                                        child_count = child.get('count', 0)
                                        ui.button(
                                            f"{child_display} ({child_count:,})",
                                            on_click=lambda e, cn=child['domain']: on_domain_selected(cn),
                                        ).props('flat dense align=left no-caps').classes(
                                            'w-full text-left text-sm'
                                        )
                            else:
                                ui.button(
                                    f"{parent_display} ({parent_count:,})",
                                    on_click=lambda e, pn=parent_name: on_domain_selected(pn),
                                ).props('flat dense align=left no-caps').classes(
                                    'w-full text-left text-sm'
                                )

                        # Unclassified bucket (informational -- count only)
                        if unclassified_count > 0:
                            ui.separator().classes('my-2')
                            ui.label(
                                f"{tr('Unclassified')} ({unclassified_count:,})"
                            ).classes('text-sm text-gray-400 pl-2 py-1')

                # Author Search Card
                with ui.card().classes('w-full p-4 mt-2'):
                    ui.label(tr('Author')).classes(
                        'text-sm font-bold text-gray-600 uppercase tracking-wide mb-2'
                    )
                    author_inp = ui.input(
                        placeholder=tr('Search authors...'),
                    ).props('dense outlined clearable').classes('w-full').on(
                        'input', on_author_input, throttle=0.3
                    )
                    author_inp.on('change', on_author_change)
                    if initial_author:
                        author_inp.value = initial_author
                    author_input_ref['ref'] = author_inp

                # Work/Title Search Card
                with ui.card().classes('w-full p-4 mt-2'):
                    ui.label(tr('Work / Title')).classes(
                        'text-sm font-bold text-gray-600 uppercase tracking-wide mb-2'
                    )
                    work_inp = ui.input(
                        placeholder=tr('Search works...'),
                    ).props('dense outlined clearable').classes('w-full').on(
                        'input', on_work_input, throttle=0.3
                    )
                    work_inp.on('change', on_work_change)
                    if initial_work:
                        work_inp.value = initial_work
                    work_input_ref['ref'] = work_inp

            # ── Main content area ──────────────────────────────────
            with ui.column().classes('flex-grow min-w-0'):
                loading_spinner['ref'] = ui.spinner('dots', size='lg').classes('self-center my-8')
                loading_spinner['ref'].set_visibility(False)

                results_container['ref'] = ui.column().classes('w-full')
                pagination_container['ref'] = ui.column().classes('w-full')

    # ── Initial data load ──────────────────────────────────────────
    async def initial_load():
        """Load initial data and fetch results on page open."""
        await fetch_authors()
        await fetch_works()
        await refresh_results()

    ui.timer(0.1, initial_load, once=True)
