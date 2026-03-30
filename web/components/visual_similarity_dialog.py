# -*- coding: utf-8 -*-
"""
Visual Similarity Dialog Component — Join Discovery Workbench

Displays ranked visual similarity suggestions from FJMS image analysis
alongside the original manuscript in a side-by-side layout.

Left pane: Original manuscript (image thumbnail + text snippet)
Right pane: Ranked suggestion list with expandable rows, lazy loading,
            and action buttons (Browse, Puzzle, Add to List).

Orange color scheme (#e65100 / #ff9800) distinct from other enrichment dialogs.

Data sourced from visual_similarity.db sidecar via VisualSimilarityService.
"""

from nicegui import ui, run
from web.translations import tr, get_language
import logging

logger = logging.getLogger(__name__)

# Number of suggestions to show per batch
_PAGE_SIZE = 20


async def _fetch_original_info(sys_id: str) -> dict:
    """Fetch image URL and text snippet for the original manuscript."""
    info = {'image_url': None, 'text_snippet': '', 'fl_ids': []}

    def _do_fetch():
        result = {'image_url': None, 'text_snippet': '', 'fl_ids': []}

        # Get FL IDs from NLI crossref for image
        try:
            from shared.nli_crossref_service import get_nli_crossref_service
            nli_svc = get_nli_crossref_service(thread_safe=True)
            if nli_svc.is_available():
                images = nli_svc.get_images(sys_id)
                if images:
                    result['fl_ids'] = [img['fgp_image_number_id'] for img in images]
        except Exception as e:
            logger.debug(f"VS dialog: NLI crossref error for {sys_id}: {e}")

        # Use server proxy as image URL (most reliable)
        result['image_url'] = f"/api/nli_image_by_sysid/{sys_id}?page=0"

        # Get text snippet from Tantivy
        try:
            from web.state import state
            if state.searcher:
                text, head, src, uid = state.searcher._get_best_text_for_id(sys_id)
                if text:
                    result['text_snippet'] = text[:200].strip()
        except Exception as e:
            logger.debug(f"VS dialog: text fetch error for {sys_id}: {e}")

        return result

    try:
        info = await run.io_bound(_do_fetch)
    except Exception:
        pass
    return info


async def _fetch_suggestion_text(alma_id: str) -> str:
    """Fetch text snippet for a suggestion manuscript."""
    def _do():
        try:
            from web.state import state
            if state.searcher:
                text, head, src, uid = state.searcher._get_best_text_for_id(alma_id)
                if text:
                    return text[:150].strip()
        except Exception:
            pass
        return ''
    try:
        return await run.io_bound(_do)
    except Exception:
        return ''


async def show_visual_similarity_dialog(sys_id: str, shelfmark: str, vs_service=None):
    """Show visual similarity suggestions dialog for a manuscript.

    Args:
        sys_id: System number of the manuscript.
        shelfmark: Display shelfmark for the dialog title.
        vs_service: Optional VisualSimilarityService instance. If None, auto-create.

    Async to avoid blocking UI thread during data fetch (run.io_bound).
    """
    if vs_service is None:
        from shared.visual_similarity_service import get_vs_service
        vs_service = get_vs_service(thread_safe=True)

    # Fetch suggestions off the event loop
    try:
        data = await run.io_bound(vs_service.get_suggestions, sys_id, 200)
    except Exception:
        data = []

    # Enrich each suggestion with shelfmark, library_code, domain
    if data:
        def _enrich(suggestions):
            from web.state import state
            csv_bank = state.meta_mgr.csv_bank if state.meta_mgr else None
            from shared.fjms_service import get_fjms_service
            fjms = get_fjms_service(thread_safe=True)
            for s in suggestions:
                meta = csv_bank.get(s['alma_id']) if csv_bank else None
                s['shelfmark'] = meta.get('shelfmark', s['alma_id']) if meta else s['alma_id']
                s['library_code'] = meta.get('library_code', '') if meta else ''
                try:
                    domains = fjms.get_domains(s['alma_id']) if fjms.is_available() else []
                    s['domain'] = domains[0]['domain'] if domains else '--'
                except Exception:
                    s['domain'] = '--'
            return suggestions

        data = await run.io_bound(_enrich, data)

    lang = get_language()
    is_heb = lang == 'he'

    dialog = ui.dialog().props('maximized=false full-width')
    with dialog, ui.card().classes('w-full max-w-[1100px]').style(
        'height: 85vh; overflow: hidden; display: flex; flex-direction: column;'
    ):
        # Header with orange gradient
        with ui.row().classes('w-full items-center justify-between p-3 rounded-t').style(
            'background: linear-gradient(135deg, #e65100, #ff9800); color: white;'
        ):
            with ui.row().classes('items-center gap-2'):
                ui.icon('compare').classes('text-xl')
                ui.label(f'{tr("Visual Similarity")} \u2014 {shelfmark}').classes('text-lg font-bold')
            ui.button(icon='close', on_click=dialog.close).props('flat dense round').classes('text-white')

        if not data:
            # Empty state
            with ui.column().classes('w-full items-center justify-center p-8'):
                ui.icon('info_outline').classes('text-3xl').style('color: var(--text-muted);')
                ui.label(tr("No visual similarity suggestions")).classes('text-sm mt-2').style(
                    'color: var(--text-muted);'
                )
                ui.label(tr("No visual similarity suggestions are available for this manuscript.")).classes('text-xs mt-1').style(
                    'color: var(--text-muted);'
                )
        else:
            # Description
            ui.label(tr("Visual similarity suggestions from FJMS image analysis")).classes('text-xs px-4 pt-2').style(
                'color: var(--text-muted);'
            )

            # Extract unique libraries and domains for filter options
            all_libraries = sorted({s['library_code'] for s in data if s.get('library_code')})
            all_domains = sorted({s['domain'] for s in data if s.get('domain') and s['domain'] != '--'})

            # Sort and filter controls
            sort_options = [
                {'label': tr('Rank'), 'value': 'rank'},
                {'label': tr('Library'), 'value': 'library'},
                {'label': tr('Domain'), 'value': 'domain'},
            ]

            filter_state = {
                'sort': 'rank',
                'libraries': [],
                'domains': [],
                'visible_count': _PAGE_SIZE,
            }

            with ui.row().classes('w-full items-center gap-2 px-4 pt-2 pb-1 flex-wrap'):
                sort_sel = ui.select(
                    options={o['value']: o['label'] for o in sort_options},
                    value='rank',
                    label=tr('Sort'),
                ).props('dense outlined').classes('w-28')

                lib_filter = ui.select(
                    options=all_libraries,
                    value=[],
                    label=tr('Library'),
                    multiple=True,
                ).props('dense outlined use-chips').classes('w-36') if all_libraries else None

                dom_filter = ui.select(
                    options=all_domains,
                    value=[],
                    label=tr('Domain'),
                    multiple=True,
                ).props('dense outlined use-chips').classes('w-36') if all_domains else None

            # Main content: side pane (left) + suggestion list (right)
            with ui.element('div').classes('w-full').style(
                'flex: 1 1 0; overflow: hidden; display: flex; flex-direction: row; min-height: 0;'
            ):
                # ── Left pane: Original manuscript ──
                with ui.scroll_area().classes('shrink-0').style(
                    'width: 260px; '
                    'border-right: 2px solid var(--border-light, #e5e7eb);'
                ), ui.column().classes('gap-2 p-3'):
                    ui.label(tr('Original')).classes('text-xs font-bold uppercase').style(
                        'color: #e65100; letter-spacing: 0.05em;'
                    )
                    ui.label(shelfmark).classes('text-sm font-bold').style(
                        'color: var(--primary-700); word-break: break-word;'
                    )

                    # Image placeholder — filled async
                    orig_img_container = ui.column().classes('w-full items-center')
                    with orig_img_container:
                        ui.spinner('dots', size='lg').style('color: #ff9800;')

                    # Text placeholder — filled async
                    orig_text_container = ui.column().classes('w-full')

                    async def _load_original():
                        info = await _fetch_original_info(sys_id)
                        orig_img_container.clear()
                        with orig_img_container:
                            if info.get('image_url'):
                                ui.image(info['image_url']).classes(
                                    'w-full max-h-[200px] object-contain rounded'
                                )
                            else:
                                ui.icon('hide_image').classes('text-2xl').style('color: var(--text-muted);')

                        orig_text_container.clear()
                        with orig_text_container:
                            snippet = info.get('text_snippet', '')
                            if snippet:
                                dir_style = 'direction: rtl; text-align: right;' if is_heb else ''
                                ui.label(snippet).classes('text-xs mt-2').style(
                                    f'color: var(--text-secondary); line-height: 1.4; '
                                    f'max-height: 6em; overflow: hidden; {dir_style}'
                                )
                            else:
                                ui.label(tr('No text available')).classes('text-xs mt-2').style(
                                    'color: var(--text-muted); font-style: italic;'
                                )

                    # Fire async load without blocking dialog render
                    import asyncio
                    asyncio.ensure_future(_load_original())

                # ── Right pane: Suggestion list ──
                with ui.scroll_area().style('flex: 1 1 0; min-width: 0;'):
                    rows_container = ui.column().classes('w-full gap-0 p-0')

            # Track expanded rows and their text-loading state
            expanded_rows = set()
            text_cache = {}  # alma_id -> text snippet

            def _get_filtered():
                """Apply filters and sorting, return full filtered list."""
                filtered = list(data)
                if filter_state['libraries']:
                    filtered = [s for s in filtered if s.get('library_code') in filter_state['libraries']]
                if filter_state['domains']:
                    filtered = [s for s in filtered if s.get('domain') in filter_state['domains']]

                sort_key = filter_state['sort']
                if sort_key == 'library':
                    filtered.sort(key=lambda s: (s.get('library_code', ''), s.get('rank', 999)))
                elif sort_key == 'domain':
                    filtered.sort(key=lambda s: (s.get('domain', ''), s.get('rank', 999)))
                else:
                    filtered.sort(key=lambda s: s.get('rank', 999))
                return filtered

            def _render_rows():
                rows_container.clear()
                filtered = _get_filtered()
                display = filtered[:filter_state['visible_count']]

                with rows_container:
                    if not display:
                        with ui.column().classes('w-full items-center justify-center p-4'):
                            ui.label(tr("No visual similarity suggestions")).classes('text-sm').style(
                                'color: var(--text-muted);'
                            )
                        return

                    for s in display:
                        _render_suggestion_row(s, dialog, expanded_rows, text_cache, is_heb)

                    remaining = len(filtered) - filter_state['visible_count']
                    if remaining > 0:
                        with ui.row().classes('w-full justify-center py-3'):
                            def _show_more():
                                filter_state['visible_count'] += _PAGE_SIZE
                                _render_rows()
                            ui.button(
                                f'{tr("Show more")} ({remaining} {tr("remaining")})',
                                on_click=_show_more,
                            ).props('flat dense no-caps').style(
                                'color: #e65100;'
                            )

            # Wire up sort/filter changes
            def _on_sort_change(e):
                filter_state['sort'] = e.value
                filter_state['visible_count'] = _PAGE_SIZE
                _render_rows()

            sort_sel.on('update:model-value', _on_sort_change)

            if lib_filter:
                def _on_lib_change(e):
                    filter_state['libraries'] = e.value or []
                    filter_state['visible_count'] = _PAGE_SIZE
                    _render_rows()
                lib_filter.on('update:model-value', _on_lib_change)

            if dom_filter:
                def _on_dom_change(e):
                    filter_state['domains'] = e.value or []
                    filter_state['visible_count'] = _PAGE_SIZE
                    _render_rows()
                dom_filter.on('update:model-value', _on_dom_change)

            # Initial render
            _render_rows()

            # Preload text snippets for first batch in background
            async def _preload_texts():
                for s in data[:_PAGE_SIZE]:
                    aid = s['alma_id']
                    if aid not in text_cache:
                        text_cache[aid] = await _fetch_suggestion_text(aid)
            asyncio.ensure_future(_preload_texts())

        # Bottom bar
        with ui.row().classes('w-full justify-between items-center p-2'):
            if data:
                ui.button(
                    tr('Search in visual suggestions'), icon='search',
                    on_click=lambda: (
                        ui.navigate.to(f'/search?vs_src={sys_id}'),
                        dialog.close(),
                    ),
                ).props('flat dense no-caps').style('color: #e65100;')
            ui.element('div').classes('flex-grow')
            ui.button(tr('Close'), on_click=dialog.close).props('flat dense')

    dialog.open()
    return dialog


def _render_suggestion_row(s, dialog, expanded_rows, text_cache, is_heb):
    """Render a single suggestion row with expandable detail section."""
    alma_id = s['alma_id']

    # Row container
    row_el = ui.column().classes('w-full gap-0').style(
        'border-bottom: 1px solid var(--border-light, #e5e7eb);'
    )

    # Mutable ref for the detail container (created after the main row)
    dc_ref = [None]

    # Async click handler for expand/collapse
    async def _on_row_click(aid=alma_id):
        dc = dc_ref[0]
        if dc is None:
            return
        if aid in expanded_rows:
            # Collapse
            expanded_rows.discard(aid)
            dc.set_visibility(False)
            return

        # Expand
        expanded_rows.add(aid)
        dc.set_visibility(True)

        # Populate detail content
        dc.clear()
        with dc:
            with ui.row().classes('w-full gap-4 items-start').style(
                'background: rgba(230, 81, 0, 0.08); border-radius: 6px; padding: 8px;'
            ):
                # Image thumbnail
                with ui.column().classes('shrink-0 items-center').style('width: 180px;'):
                    img_url = f"/api/nli_image_by_sysid/{aid}?page=0"
                    ui.image(img_url).classes(
                        'w-full max-h-[160px] object-contain rounded'
                    )

                # Text snippet (lazy loaded)
                with ui.column().classes('flex-1 gap-1'):
                    if aid in text_cache and text_cache[aid]:
                        dir_style = 'direction: rtl; text-align: right;' if is_heb else ''
                        ui.label(text_cache[aid]).classes('text-xs').style(
                            f'color: var(--text-secondary); line-height: 1.5; {dir_style}'
                        )
                    else:
                        spinner = ui.spinner('dots', size='sm').style('color: #ff9800;')
                        text_label_container = ui.column().classes('w-full')

                        # Fetch text async
                        text = await _fetch_suggestion_text(aid)
                        text_cache[aid] = text

                        # Remove spinner, show text
                        try:
                            spinner.delete()
                        except Exception:
                            pass
                        with text_label_container:
                            if text:
                                dir_style = 'direction: rtl; text-align: right;' if is_heb else ''
                                ui.label(text).classes('text-xs').style(
                                    f'color: var(--text-secondary); line-height: 1.5; {dir_style}'
                                )
                            else:
                                ui.label(tr('No text available')).classes('text-xs').style(
                                    'color: var(--text-muted); font-style: italic;'
                                )

    with row_el:
        # Main row (always visible)
        with ui.row().classes('w-full items-center gap-2 px-3 py-2 cursor-pointer').style(
            'min-height: 2.5em;'
        ).on('click', _on_row_click):
            # Expand chevron
            ui.icon(
                'expand_more' if alma_id in expanded_rows else 'chevron_right'
            ).classes('text-sm').style('color: var(--text-muted); transition: transform 0.2s;')

            # Rank badge (orange)
            ui.badge(f'#{s["rank"]}').props(
                'color=deep-orange-1 text-color=deep-orange-9'
            ).classes('text-xs')

            # Clickable shelfmark
            def _nav_browse(aid=alma_id):
                ui.navigate.to(f'/browse?sys_id={aid}')
                dialog.close()

            ui.button(
                s['shelfmark'],
                on_click=_nav_browse,
            ).props('flat dense no-caps size=sm').classes('text-sm font-semibold').style(
                'color: var(--primary-700);'
            )

            # Domain
            ui.label(s.get('domain', '--')).classes('text-xs').style(
                'color: var(--text-secondary); min-width: 60px;'
            )

            # Library code badge
            if s.get('library_code'):
                ui.label(s['library_code']).classes('text-xs px-2 py-0.5 rounded shrink-0').style(
                    'background: var(--primary-100); color: var(--primary-700);'
                )

            # Text snippet preview (first ~80 chars inline)
            cached_text = text_cache.get(alma_id, '')
            if cached_text:
                dir_style = 'direction: rtl;' if is_heb else ''
                ui.label(cached_text[:80] + ('...' if len(cached_text) > 80 else '')).classes(
                    'text-xs truncate'
                ).style(
                    f'color: var(--text-muted); max-width: 200px; {dir_style}'
                )

            # Spacer
            ui.element('div').classes('flex-grow')

            # Action buttons
            ui.button(
                icon='open_in_new',
                on_click=lambda aid=alma_id: (ui.navigate.to(f'/browse?sys_id={aid}'), dialog.close()),
            ).props('flat dense round size=sm').tooltip(tr('Browse manuscript'))

            ui.button(
                icon='extension',
                on_click=lambda aid=alma_id: (ui.navigate.to(f'/puzzle?add={aid}'), dialog.close()),
            ).props('flat dense round size=sm').tooltip(tr('Add to puzzle'))

            # Add to List button
            def _add_to_list(aid=alma_id, sm=s['shelfmark']):
                try:
                    from web.state import state
                    from web.components import show_add_to_list_dialog
                    if state.lists_mgr:
                        show_add_to_list_dialog(
                            sys_id=aid,
                            shelfmark=sm,
                            lists_mgr=state.lists_mgr,
                        )
                except Exception as e:
                    logger.debug(f"VS dialog: add to list error: {e}")

            ui.button(
                icon='star_border',
                on_click=_add_to_list,
            ).props('flat dense round size=sm').tooltip(tr('Add to List'))

        # Expandable detail section (below the main row)
        detail_container = ui.column().classes('w-full gap-2 px-4 pb-3')
        detail_container.set_visibility(False)
        dc_ref[0] = detail_container
