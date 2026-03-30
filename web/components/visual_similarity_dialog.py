# -*- coding: utf-8 -*-
"""
Visual Similarity Dialog Component

Displays ranked visual similarity suggestions from FJMS image analysis.
Orange color scheme (#e65100 / #ff9800) distinct from other enrichment dialogs.

Data sourced from visual_similarity.db sidecar via VisualSimilarityService.
"""

from nicegui import ui, run
from web.translations import tr, get_language


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
    with dialog, ui.card().classes('w-full max-w-[900px] max-h-[90vh]').style(
        'overflow: hidden; display: flex; flex-direction: column;'
    ):
        # Header with orange gradient (distinct from catalog/bib/measurements)
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

            # Scrollable body with suggestion rows
            rows_container = ui.element('div').classes('w-full').style(
                'flex: 1; overflow-y: auto; min-height: 200px;'
            )

            def _render_rows():
                rows_container.clear()
                filtered = list(data)

                # Apply library filter
                if filter_state['libraries']:
                    filtered = [s for s in filtered if s.get('library_code') in filter_state['libraries']]

                # Apply domain filter
                if filter_state['domains']:
                    filtered = [s for s in filtered if s.get('domain') in filter_state['domains']]

                # Apply sort
                sort_key = filter_state['sort']
                if sort_key == 'library':
                    filtered.sort(key=lambda s: (s.get('library_code', ''), s.get('rank', 999)))
                elif sort_key == 'domain':
                    filtered.sort(key=lambda s: (s.get('domain', ''), s.get('rank', 999)))
                else:
                    filtered.sort(key=lambda s: s.get('rank', 999))

                # Show max 20 rows
                display = filtered[:20]

                with rows_container:
                    if not display:
                        with ui.column().classes('w-full items-center justify-center p-4'):
                            ui.label(tr("No visual similarity suggestions")).classes('text-sm').style(
                                'color: var(--text-muted);'
                            )
                        return

                    for s in display:
                        with ui.row().classes('w-full items-center gap-2 px-4 py-2').style(
                            'border-bottom: 1px solid var(--border-light, #e5e7eb); min-height: 2.5em;'
                        ):
                            # Rank badge (orange)
                            ui.badge(f'#{s["rank"]}').props(
                                'color=deep-orange-1 text-color=deep-orange-9'
                            ).classes('text-xs')

                            # Clickable shelfmark
                            def _nav_browse(aid=s['alma_id']):
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
                                'color: var(--text-secondary); min-width: 80px;'
                            )

                            # Library code badge
                            if s.get('library_code'):
                                ui.label(s['library_code']).classes('text-xs px-2 py-0.5 rounded shrink-0').style(
                                    'background: var(--primary-100); color: var(--primary-700);'
                                )

                            # Spacer
                            ui.element('div').classes('flex-grow')

                            # Browse button
                            ui.button(
                                icon='open_in_new',
                                on_click=lambda aid=s['alma_id']: (ui.navigate.to(f'/browse?sys_id={aid}'), dialog.close()),
                            ).props('flat dense round size=sm').tooltip(tr('Browse manuscript'))

                            # Puzzle button
                            ui.button(
                                icon='extension',
                                on_click=lambda aid=s['alma_id']: (ui.navigate.to(f'/puzzle?add={aid}'), dialog.close()),
                            ).props('flat dense round size=sm').tooltip(tr('Add to puzzle'))

                    if len(filtered) > 20:
                        ui.label(
                            f'... {tr("and")} {len(filtered) - 20} {tr("more")}'
                        ).classes('text-sm py-2 px-4').style('color: var(--text-muted);')

            # Wire up sort/filter changes
            def _on_sort_change(e):
                filter_state['sort'] = e.value
                _render_rows()

            sort_sel.on('update:model-value', _on_sort_change)

            if lib_filter:
                def _on_lib_change(e):
                    filter_state['libraries'] = e.value or []
                    _render_rows()
                lib_filter.on('update:model-value', _on_lib_change)

            if dom_filter:
                def _on_dom_change(e):
                    filter_state['domains'] = e.value or []
                    _render_rows()
                dom_filter.on('update:model-value', _on_dom_change)

            # Initial render
            _render_rows()

        # Close button at bottom
        with ui.row().classes('w-full justify-end p-2'):
            ui.button(tr('Close'), on_click=dialog.close).props('flat dense')

    dialog.open()
    return dialog
