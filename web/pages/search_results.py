# -*- coding: utf-8 -*-
"""
Search Results Rendering Functions

Extracted from web/pages/search.py (Phase 72, Plan 02).
Contains the four main rendering functions (toggle_expansion, render_results,
create_result_card, open_advanced_dialog) plus two standalone helpers
(copy_result_text, show_add_to_list_dialog).

Each function that was a closure in create_search_page() now takes explicit
search_state and refs parameters instead of capturing them via closure.
"""
from __future__ import annotations

from nicegui import ui, run, app
from web.state import state
from web.translations import tr, is_rtl, get_language
from web.feature_flags import WEB_PUZZLE_ENABLED
from web.components.filter_panel import persist_value
from web.pages.search_state import (
    AdvancedViewState, domain_display_name,
)
from web.services import (
    get_service,
    get_oxford_direct_image_url,
    is_oxford_manuscript,
)
from shared.refinement import compute_all_terms_filter, enrich_snippet_with_chain_terms
from genizah_core import SearchEngine, get_library_display
from web.document_service import (
    get_all_sources_for_fragment, get_document_for_fragment, get_section_for_page,
)
from urllib.parse import quote
from web.components.typography import h3
import logging
import re
import html
import asyncio

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Standalone helpers (zero closure dependencies)
# ---------------------------------------------------------------------------

def copy_result_text(text):
    """Copy text to clipboard."""
    if text:
        # Escape backticks for JavaScript
        escaped_text = text.replace('`', '\\`')
        ui.run_javascript(f'''
            navigator.clipboard.writeText(`{escaped_text}`).then(() => {{
                console.log('Text copied to clipboard');
            }});
        ''')
        ui.notify(tr('Text copied to clipboard'), type='positive')
    else:
        ui.notify(tr('No text to copy'), type='warning')

def show_add_to_list_dialog(result):
    from web.components import show_add_to_list_dialog as show_dialog
    display = result.get('display', {})
    sys_id = display.get('id')
    if not sys_id:
        ui.notify(tr('Cannot add: missing system ID'), type='warning')
        return
    if not state.lists_mgr:
        ui.notify(tr('Lists manager not available'), type='warning')
        return
    shelfmark = display.get('shelfmark', 'Unknown')
    show_dialog(
        sys_id=sys_id,
        shelfmark=shelfmark,
        lists_mgr=state.lists_mgr,
        note_default='',  # Empty by default
        fl_id=None
    )



# ---------------------------------------------------------------------------
# Core rendering functions (take search_state + refs as parameters)
# ---------------------------------------------------------------------------

def toggle_expansion(search_state, refs, index):
    """Toggle inline accordion expansion for a result card."""
    if search_state.expanded_index == index:
        # Collapse current
        ref = search_state.expansion_refs.get(index)
        if ref and not ref.is_deleted:
            ref.style('display: none')
        search_state.expanded_index = None
    else:
        # Collapse old if any
        if search_state.expanded_index is not None:
            old_ref = search_state.expansion_refs.get(search_state.expanded_index)
            if old_ref and not old_ref.is_deleted:
                old_ref.style('display: none')
        # Expand new
        ref = search_state.expansion_refs.get(index)
        if ref and not ref.is_deleted:
            ref.style('display: block')
        search_state.expanded_index = index
        # Trigger lazy text loading if registered for this card
        lazy_loaders = getattr(search_state, '_lazy_loaders', {})
        if index in lazy_loaders:
            async def _run_lazy(fn=lazy_loaders[index]):
                with refs.page_client:
                    await fn()
            asyncio.ensure_future(_run_lazy())


def render_results(search_state, refs, results, page=None, scroll_to_top=False, reset_expansion=True):
    refs.results_container.clear()
    # Phase 55: Apply "all terms" post-filter if active
    if search_state._all_terms_filter and search_state.refinement_chain:
        common_uids = compute_all_terms_filter(search_state.refinement_chain)
        if common_uids is not None:
            results = [r for r in results if (r.get('uid') or r.get('display', {}).get('id')) in common_uids]
    search_state.displayed_results = results  # Track full filtered set for Advanced View navigation
    state.last_results = results  # Keep export in sync with displayed (post-filter) results

    # Handle expansion state
    _was_expanded = None
    if reset_expansion:
        search_state.expanded_index = None
        search_state.expansion_refs = {}
    else:
        _was_expanded = search_state.expanded_index
        search_state.expansion_refs = {}
        search_state.expanded_index = None

    # Use provided page or keep stored page
    if page is not None:
        search_state.current_page = page
    # Clamp page to valid range
    total = len(results)
    total_pages = max(1, (total + refs.page_size - 1) // refs.page_size)
    if search_state.current_page >= total_pages:
        search_state.current_page = 0

    # Show loading spinner when search is running - prominent so user knows it's working
    if search_state.is_running:
        with refs.results_container:
            with ui.column().classes('w-full h-64 items-center justify-center'):
                ui.spinner('bars', size='xl', color='primary').classes('mb-4')
                ui.label(tr("Searching...")).classes('text-xl font-bold animate-pulse').style('color: var(--primary-600);')
        return

    if not results:
        with refs.results_container:
            # Phase 55: Zero-result refinement recovery (D-14a)
            if getattr(search_state, '_zero_result_refine', False):
                with ui.column().classes('w-full h-64 items-center justify-center'):
                    ui.label(tr('0 results within current scope')).classes('text-lg py-4 text-center w-full')
                    ui.button(
                        tr('Back to previous step'), icon='undo',
                        on_click=lambda _ss=search_state, _r=refs: _r.undo_zero_result_refine()
                    ).classes('mx-auto')
            else:
                with ui.column().classes('w-full h-64 items-center justify-center'):
                    ui.icon('search').classes('text-5xl').style('color: var(--text-muted);')
                    ui.label(tr("Ready to search.")).classes('mt-4').style('color: var(--text-muted);')
        refs.update_search_within_btn()
        refs.update_refinement_strip()
        return

    # Calculate page slice
    page_idx = search_state.current_page
    start = page_idx * refs.page_size
    end = min(start + refs.page_size, total)
    page_results = results[start:end]

    with refs.results_container:
        # Pagination controls at top (always reserve space to prevent CLS)
        if total_pages > 1:
            with ui.row().classes('w-full justify-between items-center px-4 pt-2'):
                ui.label(f"{start + 1}-{end} {tr('of')} {total}").classes('text-sm').style('color: var(--text-muted);')
                def on_page_change_top(e, _ss=search_state, _r=refs):
                    _ss.current_page = e.value - 1  # ui.pagination is 1-indexed
                    render_results(_ss, _r, results, page=_ss.current_page, scroll_to_top=True)
                ui.pagination(1, total_pages, value=page_idx + 1,
                    on_change=on_page_change_top).props('max-pages=7 boundary-numbers')
        else:
            # Invisible placeholder reserving same height as pagination row
            ui.element('div').style('height: 40px; visibility: hidden;')

        # Render only this page of results
        with ui.column().classes('w-full gap-2 p-4'):
            for i, res in enumerate(page_results):
                create_result_card(search_state, refs, start + i, res)

        # Re-expand previously open card after enrichment rerender
        if not reset_expansion and _was_expanded is not None and _was_expanded in search_state.expansion_refs:
            toggle_expansion(search_state, refs, _was_expanded)

        # Pagination controls at bottom (always reserve space to prevent CLS)
        if total_pages > 1:
            with ui.row().classes('w-full justify-center items-center px-4 pb-2'):
                def on_page_change_bottom(e, _ss=search_state, _r=refs):
                    _ss.current_page = e.value - 1
                    render_results(_ss, _r, results, page=_ss.current_page, scroll_to_top=True)
                ui.pagination(1, total_pages, value=page_idx + 1,
                    on_change=on_page_change_bottom).props('max-pages=7 boundary-numbers')
        else:
            ui.element('div').style('height: 40px; visibility: hidden;')

        # Collapsible excluded results section (domain-excluded)
        excluded = search_state.domain_excluded_results
        if excluded:
            with ui.expansion(
                text=f"{tr('Excluded Results')} ({len(excluded)})",
                icon='filter_alt',
                value=False  # collapsed by default
            ).classes('w-full').style(
                'border: 1px solid var(--accent-amber); border-radius: 8px; margin-top: 16px; overflow: hidden;'
            ).props('dense header-class="text-amber-8 text-subtitle1 text-weight-medium"'):
                # Show up to 50 excluded results with their reasons
                EXCLUDED_DISPLAY_LIMIT = 50
                for i, excl_item in enumerate(excluded[:EXCLUDED_DISPLAY_LIMIT]):
                    excl_result = excl_item['result']
                    excl_reason = excl_item.get('reason', '')
                    excl_display = excl_result.get('display', {})
                    excl_shelfmark = excl_display.get('shelfmark', 'Unknown')
                    excl_title = excl_display.get('title', '')
                    with ui.row().classes('w-full items-center gap-2 py-1 px-2 cursor-pointer').style(
                        'border-bottom: 1px solid var(--border-light); overflow: hidden; max-width: 100%;'
                    ).on('click', lambda r=excl_result, _ss=search_state, _r=refs: open_advanced_dialog(_ss, _r, None, r)):
                        ui.label(excl_shelfmark).classes('text-sm font-medium truncate shrink-0').style(
                            'color: var(--text-secondary); max-width: 200px;'
                        )
                        if excl_title:
                            title_short = (excl_title[:60] + '...') if len(excl_title) > 60 else excl_title
                            ui.label(title_short).classes('text-xs truncate').style(
                                'color: var(--text-muted); direction: rtl; min-width: 0; flex: 1 1 0;'
                            )
                        ui.label(excl_reason).classes('text-xs px-2 py-0.5 rounded shrink-0').style(
                            'background: #fff3cd; color: #856404; white-space: nowrap;'
                        )
                if len(excluded) > EXCLUDED_DISPLAY_LIMIT:
                    ui.label(
                        f"... {tr('and')} {len(excluded) - EXCLUDED_DISPLAY_LIMIT} {tr('more')}"
                    ).classes('text-sm py-2 px-2').style('color: var(--text-muted);')

        # Collapsible word search excluded results section
        ws_excluded = search_state.word_search_excluded_results
        if ws_excluded:
            with ui.expansion(
                text=f"{tr('Excluded Results')} ({len(ws_excluded)})",
                icon='remove_circle_outline',
                value=False  # collapsed by default
            ).classes('w-full').style(
                'border: 1px solid var(--accent-amber); border-radius: 8px; margin-top: 8px; overflow: hidden;'
            ).props('dense header-class="text-amber-8 text-subtitle1 text-weight-medium"'):
                WS_EXCLUDED_LIMIT = 50
                for i, excl_item in enumerate(ws_excluded[:WS_EXCLUDED_LIMIT]):
                    excl_result = excl_item['result']
                    excl_display = excl_result.get('display', {})
                    excl_shelfmark = excl_display.get('shelfmark', 'Unknown')
                    excl_title = excl_display.get('title', '')
                    excl_sys_id = excl_display.get('id')
                    with ui.row().classes('w-full items-center gap-2 py-1 px-2').style(
                        'border-bottom: 1px solid var(--border-light); overflow: hidden; max-width: 100%;'
                    ):
                        # Restore button
                        def _restore_word_result(sid=excl_sys_id):
                            search_state.word_search_excluded_ids.discard(sid)
                            persist_value('word_search_excluded_ids', list(search_state.word_search_excluded_ids))
                            refs.apply_word_search_exclusions_and_render()
                        ui.button(icon='add_circle_outline', on_click=_restore_word_result).props(
                            'flat round dense size=xs'
                        ).style('color: var(--text-muted);').tooltip(tr('Restore'))
                        with ui.row().classes('items-center gap-2 flex-grow min-w-0 cursor-pointer').on(
                            'click', lambda r=excl_result, _ss=search_state, _r=refs: open_advanced_dialog(_ss, _r, None, r)
                        ):
                            ui.label(excl_shelfmark).classes('text-sm font-medium truncate shrink-0').style(
                                'color: var(--text-secondary); max-width: 200px;'
                            )
                            if excl_title:
                                title_short = (excl_title[:60] + '...') if len(excl_title) > 60 else excl_title
                                ui.label(title_short).classes('text-xs truncate').style(
                                    'color: var(--text-muted); direction: rtl; min-width: 0; flex: 1 1 0;'
                                )
                if len(ws_excluded) > WS_EXCLUDED_LIMIT:
                    ui.label(
                        f"... {tr('and')} {len(ws_excluded) - WS_EXCLUDED_LIMIT} {tr('more')}"
                    ).classes('text-sm py-2 px-2').style('color: var(--text-muted);')
                # Clear all word search exclusions button
                with ui.row().classes('w-full justify-end py-2 px-2'):
                    def _clear_word_exclusions():
                        search_state.word_search_excluded_ids.clear()
                        persist_value('word_search_excluded_ids', [])
                        refs.apply_word_search_exclusions_and_render()
                    ui.button(tr('Clear All Exclusions'), icon='clear_all',
                              on_click=_clear_word_exclusions).props('flat dense no-caps size=sm')

        # Phase 56: Collapsible manuscript excluded results section
        ms_excluded = search_state.manuscript_excluded_results
        if ms_excluded:
            with ui.expansion(
                text=f"{tr('Excluded manuscripts')} ({len(ms_excluded)})",
                icon='person_remove',
                value=False
            ).classes('w-full').style(
                'border: 1px solid var(--accent-red, #ef4444); border-radius: 8px; margin-top: 8px; overflow: hidden;'
            ).props('dense header-class="text-red-8 text-subtitle1 text-weight-medium"'):
                MS_EXCL_LIMIT = 50
                for i, excl_item in enumerate(ms_excluded[:MS_EXCL_LIMIT]):
                    excl_result = excl_item['result']
                    excl_reason = excl_item.get('reason', '')
                    excl_display = excl_result.get('display', {})
                    excl_shelfmark = excl_display.get('shelfmark', 'Unknown')
                    excl_title = excl_display.get('title', '')
                    with ui.row().classes('w-full items-center gap-2 py-1 px-2 cursor-pointer').style(
                        'border-bottom: 1px solid var(--border-light); overflow: hidden; max-width: 100%;'
                    ).on('click', lambda r=excl_result, _ss=search_state, _r=refs: open_advanced_dialog(_ss, _r, None, r)):
                        ui.label(excl_shelfmark).classes('text-sm font-medium truncate shrink-0').style(
                            'color: var(--text-secondary); max-width: 200px;'
                        )
                        if excl_title:
                            title_short = (excl_title[:60] + '...') if len(excl_title) > 60 else excl_title
                            ui.label(title_short).classes('text-xs truncate').style(
                                'color: var(--text-muted); direction: rtl; min-width: 0; flex: 1 1 0;'
                            )
                        ui.label(excl_reason).classes('text-xs px-2 py-0.5 rounded shrink-0').style(
                            'background: var(--accent-red, #fecaca); color: var(--text-on-accent, #991b1b); white-space: nowrap;'
                        )
                if len(ms_excluded) > MS_EXCL_LIMIT:
                    ui.label(
                        f"... {tr('and')} {len(ms_excluded) - MS_EXCL_LIMIT} {tr('more')}"
                    ).classes('text-sm py-2 px-2').style('color: var(--text-muted);')

    # Phase 55: Update refinement UI after rendering results
    refs.update_search_within_btn()
    refs.update_refinement_strip()

    # Scroll to top of results after page change
    if scroll_to_top:
        refs.results_container.run_method('setScrollPosition', 'vertical', 0)



def create_result_card(search_state, refs, index, result):
    display = result.get('display', {})
    shelfmark = display.get('shelfmark', 'Unknown')
    title = display.get('title', '')
    snippet = result.get('snippet', '')
    library_code = display.get('library_code', '')
    sys_id = display.get('id')

    # Truncate title for display
    title_short = (title[:60] + '...') if title and len(title) > 60 else title

    with ui.card().classes(
        'w-full p-4 cursor-pointer transition-all hover:shadow-md'
    ).style('border-radius: 10px;') as card:
        with ui.row().classes('w-full items-start justify-between'):
            # Checkbox for selection
            with ui.column().classes('justify-center'):
                def toggle_card_selection(e, idx=index):
                    if e.value:
                        search_state.selected_indices.add(idx)
                    else:
                        search_state.selected_indices.discard(idx)
                    refs.update_selection_ui()

                result_checkbox = ui.checkbox(
                    value=index in search_state.selected_indices,
                    on_change=toggle_card_selection
                ).props('dense')

            # Main content (clickable — toggles inline accordion)
            with ui.column().classes('flex-grow min-w-0 gap-1').on('click', lambda idx=index, _ss=search_state, _r=refs: toggle_expansion(_ss, _r, idx)):
                with ui.row().classes('items-center gap-2 flex-wrap'):
                    ui.label(f"#{index + 1}").classes('text-xs px-2 py-0.5 rounded shrink-0').style(
                        'background: var(--bg-tertiary); color: var(--text-muted);'
                    )
                    # Library badge (if available)
                    if library_code:
                        from genizah_core import get_library_display
                        full_name = get_library_display(library_code, short=False, lang=get_language())
                        ui.label(library_code).classes('text-xs px-2 py-0.5 rounded shrink-0').style(
                            'background: var(--primary-100); color: var(--primary-700);'
                        ).tooltip(full_name)
                    # PGP transcription indicator
                    if sys_id and sys_id in search_state.transcription_sys_ids:
                        ui.label('PGP').classes('text-xs px-2 py-0.5 rounded shrink-0').style(
                            'background: var(--success-100); color: var(--success-700); font-weight: 600;'
                        ).tooltip(tr('Has PGP Transcription'))
                    # Domain indicator
                    if sys_id and search_state.result_domains:
                        domains_for_result = search_state.result_domains.get(sys_id, [])
                        if domains_for_result:
                            primary_domain = domains_for_result[0]
                            domain_text = domain_display_name(search_state, primary_domain)
                            if len(domains_for_result) > 1:
                                extra = len(domains_for_result) - 1
                                tooltip_text = ', '.join(domain_display_name(search_state, d) for d in domains_for_result)
                                with ui.row().classes('items-center gap-0'):
                                    ui.label(domain_text).classes('text-xs px-2 py-0.5 rounded shrink-0').style(
                                        'background: #f3e8ff; color: #7c3aed;'
                                    )
                                    ui.label(f'+{extra}').classes('text-xs px-1 py-0.5 rounded shrink-0 cursor-help').style(
                                        'background: #ede9fe; color: #7c3aed;'
                                    ).tooltip(tooltip_text)
                            else:
                                ui.label(domain_text).classes('text-xs px-2 py-0.5 rounded shrink-0').style(
                                    'background: #f3e8ff; color: #7c3aed;'
                                )
                    # Printed material indicator
                    if sys_id and sys_id in search_state.printed_ids:
                        from shared.fjms_service import PRINTED_BADGE_COLORS, PRINTED_LABEL_EN, PRINTED_LABEL_HE
                        _bg, _fg = PRINTED_BADGE_COLORS
                        _plabel = PRINTED_LABEL_HE if get_language() == 'he' else PRINTED_LABEL_EN
                        ui.label(_plabel).classes('text-xs px-2 py-0.5 rounded shrink-0 font-medium').style(
                            f'background: {_bg}; color: {_fg};'
                        )
                    # Phase 57 JOIN-03: Visual Similarity icon in badge row
                    _has_vs = sys_id and search_state.vs_availability.get(sys_id)
                    if _has_vs:
                        _vs_expanded_state = {'value': False, 'loaded': False}
                        # Define toggle function -- container will be set below
                        _vs_container_ref = [None]

                        async def _toggle_vs_partners(s_id=sys_id, exp=_vs_expanded_state, cref=_vs_container_ref):
                            container = cref[0]
                            if container is None:
                                return
                            exp['value'] = not exp['value']
                            if exp['value']:
                                container.classes(remove='hidden')
                                if not exp['loaded']:
                                    exp['loaded'] = True
                                    try:
                                        from shared.visual_similarity_service import get_vs_service
                                        _svc = get_vs_service(thread_safe=True)
                                        _top3 = await run.io_bound(_svc.get_suggestions, s_id, 3)
                                        with container:
                                            ui.label(tr('Visual similarity partners')).classes('text-xs font-semibold').style('color: #ef6c00;')
                                            for _p in _top3:
                                                _cb = state.meta_mgr.csv_bank if state.meta_mgr else None
                                                _p_meta = _cb.get(_p['alma_id']) if _cb else None
                                                _p_sm = _p_meta.get('shelfmark', _p['alma_id']) if _p_meta else _p['alma_id']
                                                with ui.row().classes('items-center gap-1'):
                                                    ui.badge(f'#{_p["rank"]}').props('color=deep-orange-1 text-color=deep-orange-9').classes('text-xs')
                                                    ui.link(_p_sm, f'/browse?sys_id={_p["alma_id"]}').classes('text-xs')
                                    except Exception:
                                        with container:  # Visual similarity lookup failed; continue
                                            ui.label(tr('Could not load visual similarity data. Try again later.')).classes('text-xs').style('color: var(--text-muted);')
                            else:
                                container.classes(add='hidden')

                        ui.button(icon='compare', on_click=_toggle_vs_partners).props(
                            'flat dense round size=xs'
                        ).style('color: #ef6c00;').tooltip(tr('Visual similarity partners'))

                    ui.label(shelfmark).classes('font-bold break-all').style('color: var(--primary-700);')

                # Phase 57 JOIN-03: Partner container placed outside badge row, inside content column
                if _has_vs:
                    _vs_partner_container = ui.column().classes('hidden w-full gap-0 py-1 px-2').style(
                        'background: #fff3e0; border-radius: 6px; margin-top: 4px;'
                    )
                    _vs_container_ref[0] = _vs_partner_container

                # Title and optional translated description (Phase 46)
                _show_trans = False
                try:
                    _show_trans = app.storage.user.get('show_translations', False)
                except Exception:
                    pass  # Translation lookup failed; continue without translation
                _title_info = search_state.title_translations.get(sys_id) if sys_id else None
                if _title_info:
                    _lang = get_language()
                    _he = _title_info.get('hebrew_title') or ''
                    _en = _title_info.get('english_title') or ''
                    _en_he = _title_info.get('english_title_he') or ''
                    if _lang == 'he':
                        if _he.strip():
                            if _en_he.strip() and len(_he) < 15:
                                _resolved_title = f"{_he} — {_en_he}"
                            else:
                                _resolved_title = _he
                        else:
                            _resolved_title = _en_he or _en or title
                    else:
                        _resolved_title = _en or _he or title
                    _resolved_short = (_resolved_title[:60] + '...') if _resolved_title and len(_resolved_title) > 60 else (_resolved_title or '')
                else:
                    _resolved_title = title
                    _resolved_short = title_short
                _trans_info = search_state.translation_data.get(sys_id) if sys_id and _show_trans else None
                _ui_lang = get_language()
                if _trans_info and _trans_info.get('description_he') and _ui_lang == 'he':
                    _desc_he = _trans_info['description_he']
                    _desc_short = (_desc_he[:80] + '...') if len(_desc_he) > 80 else _desc_he
                    _orig = _resolved_short if _resolved_short else ''
                    _ts = {'showing_original': False}
                    with ui.row().classes('items-center gap-1'):
                        _tl = ui.label(_desc_short).classes('text-xs').style(
                            'color: var(--text-tertiary); direction: rtl; word-wrap: break-word;'
                        )
                        def _make_compact_toggle(label_el, badge_el_ref, orig_text, trans_text, flag):
                            def handler():
                                flag['showing_original'] = not flag['showing_original']
                                if flag['showing_original']:
                                    label_el.text = orig_text
                                    label_el.style('color: var(--text-tertiary); direction: ltr; word-wrap: break-word;')
                                    badge_el_ref[0].text = tr('Original')
                                else:
                                    label_el.text = trans_text
                                    label_el.style('color: var(--text-tertiary); direction: rtl; word-wrap: break-word;')
                                    badge_el_ref[0].text = tr('Translated')
                            return handler
                        _tb_ref = [None]
                        _toggle_fn = _make_compact_toggle(_tl, _tb_ref, _orig, _desc_short, _ts)
                        _tb = ui.button(tr('Translated')).props(
                            'flat dense no-caps size=xs'
                        ).classes('text-xs px-1 py-0 rounded shrink-0').style(
                            'background: #e0f2fe !important; color: #0369a1 !important; font-style: italic; font-size: 0.65rem; min-height: 0; line-height: 1.2;'
                        )
                        _tb.on('click.stop', _toggle_fn)
                        _tb_ref[0] = _tb
                        from web.components.translation_report import create_report_button
                        create_report_button(
                            dataset='pgp', record_id=str(search_state.translation_data.get(sys_id, {}).get('pgpid', sys_id)),
                            field_name='description', direction='en2he',
                            source_text=_orig, translated_text=_desc_short,
                        )
                elif _resolved_short:
                    _dir = 'ltr' if (_title_info and get_language() != 'he' and _title_info.get('english_title')) else 'rtl'
                    _orig_title = title
                    _orig_short = (title[:60] + '...') if title and len(title) > 60 else (title or '')
                    if _title_info and _orig_short and _orig_short != _resolved_short:
                        _tt_st = {'showing_original': False}
                        with ui.row().classes('items-center gap-0'):
                            _tt_lbl = ui.label(_resolved_short).classes('text-xs').style(
                                f'color: var(--text-tertiary); direction: {_dir}; word-wrap: break-word;'
                            )
                            def _make_title_toggle(lbl, orig, resolved, orig_dir, res_dir, flag):
                                def handler():
                                    flag['showing_original'] = not flag['showing_original']
                                    if flag['showing_original']:
                                        lbl.text = orig
                                        lbl.style(f'color: var(--text-tertiary); direction: {orig_dir}; word-wrap: break-word;')
                                    else:
                                        lbl.text = resolved
                                        lbl.style(f'color: var(--text-tertiary); direction: {res_dir}; word-wrap: break-word;')
                                return handler
                            ui.button(icon='swap_horiz').props('flat dense round size=xs').style(
                                'min-width: 18px; min-height: 18px; padding: 0; opacity: 0.4;'
                            ).tooltip(tr('Show original title')).on(
                                'click.stop', _make_title_toggle(_tt_lbl, _orig_short, _resolved_short, 'rtl', _dir, _tt_st)
                            )
                    else:
                        ui.label(_resolved_short).classes('text-xs').style(
                            f'color: var(--text-tertiary); direction: {_dir}; word-wrap: break-word;'
                        )

        # Action buttons row (on the card, always visible)
        with ui.row().classes('gap-1 mt-1'):
            # Browse
            if sys_id:
                _card_fl_id = None
                _card_ie_id = None
                if 'raw_header' in result and state.meta_mgr:
                    try:
                        _parsed = state.meta_mgr.parse_full_id_components(result['raw_header'])
                        _card_fl_id = _parsed.get('fl_id')
                        _card_ie_id = _parsed.get('ie_id')
                    except Exception:
                        pass  # UI visibility update failed; continue rendering
                _card_browse_url = f'/browse?sys_id={sys_id}'
                if _card_fl_id:
                    _card_browse_url += f'&fl_id={_card_fl_id}'
                if _card_ie_id:
                    _card_browse_url += f'&volume_ie={_card_ie_id}'
                with ui.link(target=_card_browse_url).classes('no-underline'):
                    ui.button(icon='menu_book').props('flat round dense size=sm color=green').tooltip(tr('Browse Full Manuscript'))

            # Quick View (was Advanced View)
            ui.button(
                icon='open_in_full',
                on_click=lambda idx=index, r=result, _ss=search_state, _r=refs: open_advanced_dialog(_ss, _r, idx, r)
            ).props('flat round dense size=sm').tooltip(tr('Quick View'))

            # Add to List
            def make_star_handler(r):
                def handler():
                    show_add_to_list_dialog(r)
                return handler
            result_sys_id = result.get('display', {}).get('id')
            result_in_list = state.lists_mgr and result_sys_id and state.lists_mgr.is_item_in_any_list(result_sys_id)
            ui.button(
                icon='star' if result_in_list else 'star_border',
                on_click=make_star_handler(result)
            ).props('flat round dense size=sm').style('color: var(--accent-amber);').tooltip(tr('In List') if result_in_list else tr('Add to List'))

            # Catalog Records
            if sys_id:
                cat_count = search_state.catalog_source_counts.get(sys_id, 0)
                from web.components.catalog_dialog import show_catalog_dialog
                cat_btn = ui.button(
                    icon='description',
                    on_click=lambda s=sys_id, sm=shelfmark: show_catalog_dialog(s, sm),
                ).props('flat round dense size=sm').tooltip(f'{tr("Catalog Records")} ({cat_count})')
                if cat_count == 0:
                    cat_btn.disable()

        # Snippet — enrich with earlier chain terms if refinement is active
        if snippet:
            if search_state.refinement_chain:
                snippet = enrich_snippet_with_chain_terms(snippet, search_state.refinement_chain, refs.query_input.value)
            snippet_html = SearchEngine.format_snippet(snippet)
            with ui.element('div').classes('mt-3 p-3 rounded-lg text-sm').style(
                'background: var(--bg-tertiary); direction: rtl; text-align: right; line-height: 1.8;'
            ):
                ui.html(snippet_html, sanitize=False)

        # === Inline accordion expansion (image + full text only) ===
        expand_container = ui.column().classes('w-full result-inline-expand').style('display: none;')
        search_state.expansion_refs[index] = expand_container

        # Build thumbnail URL eagerly (browser preloads in background)
        _img_url = None
        if sys_id:
            page_idx = max(0, int(display.get('img', '1')) - 1) if display.get('img') else 0
            # Add volume suffix for multi-IE manuscripts
            _thumb_suffix = ''
            if _card_ie_id:
                from genizah_core import get_volumes_for_sys_id as _get_vols
                _tvols = _get_vols(sys_id)
                if _tvols:
                    for _tv in _tvols:
                        if _tv['ie_id'] == _card_ie_id:
                            if _tv['suffix'] > 1:
                                _thumb_suffix = f'&suffix={_tv["suffix"]}'
                            break
            _img_url = f"/api/nli_image_by_sysid/{sys_id}?page={page_idx}&width=300{_thumb_suffix}"
            is_oxford = False
            try:
                from web.pages.browse import is_oxford_manuscript
                is_oxford = is_oxford_manuscript(display.get('shelfmark', ''), display.get('library_code', ''))
            except Exception:
                pass  # NLI enrichment failed; continue without
            if is_oxford:
                try:
                    from web.pages.browse import get_oxford_direct_image_url
                    _ox_url = get_oxford_direct_image_url(display.get('shelfmark', ''), page_idx)
                    if _ox_url:
                        _img_url = _ox_url
                    else:
                        _img_url = f"/api/oxford_image/{sys_id}?page={page_idx}"
                except Exception:
                    _img_url = f"/api/oxford_image/{sys_id}?page={page_idx}"  # Enrichment failed; continue with available data

        with expand_container:
            # Content row: image + text
            _expand_row = ui.row().classes('gap-4 flex-wrap w-full')
            with _expand_row:
                # Left: manuscript image thumbnail (click opens Quick View)
                if _img_url:
                    with ui.element('div').classes('cursor-pointer').on(
                        'click', lambda idx=index, r=result, _ss=search_state, _r=refs: open_advanced_dialog(_ss, _r, idx, r)
                    ):
                        ui.html(
                            f'<img src="{_img_url}" '
                            f'onerror="this.style.display=\'none\'" '
                            f'style="width: 200px; max-height: 250px; object-fit: contain; border-radius: 8px;" />',
                            sanitize=False
                        )

                # Right: full text container (populated immediately or lazy-loaded)
                _text_col = ui.column().classes('flex-1 min-w-[200px] gap-2')

            def _render_full_text(text_col, text, pattern):
                """Render highlighted full text into the given column."""
                text_col.clear()
                with text_col:
                    if text:
                        escaped = html.escape(text)
                        if pattern:
                            try:
                                flags = re.IGNORECASE
                                if '\\n' in pattern or pattern.startswith('^'):
                                    flags |= re.MULTILINE
                                escaped = re.sub(
                                    f'({pattern})',
                                    r'<span class="highlight-match">\1</span>',
                                    escaped, flags=flags
                                )
                            except re.error:
                                pass
                        with ui.scroll_area().classes('w-full').style('max-height: 250px;'):
                            with ui.element('div').classes('p-3 rounded-lg text-sm whitespace-pre-wrap').style(
                                'background: var(--bg-tertiary); direction: rtl; text-align: right; line-height: 2; font-size: 0.95rem;'
                            ):
                                ui.html(escaped, sanitize=False)
                    else:
                        ui.label(tr('Full text not available')).style('color: var(--text-muted);')

            # Populate text: immediately if available, lazy-load if not
            full_text = result.get('full_text', '')
            highlight_pattern = result.get('highlight_pattern', '')
            if full_text:
                _render_full_text(_text_col, full_text, highlight_pattern)
            elif sys_id:
                # Lazy-load: fetch full text when accordion first expands
                _lazy_state = {'loaded': False}
                _orig_toggle = None

                async def _lazy_load_text(idx=index, r=result, tc=_text_col, hp=highlight_pattern, sid=sys_id, ls=_lazy_state):
                    if ls['loaded']:
                        return
                    ls['loaded'] = True
                    p_num = int(r.get('display', {}).get('img', '1'))
                    try:
                        from web.services import get_service
                        page_data = await run.io_bound(
                            lambda: get_service().get_browse_page(sid, p_num=p_num)
                        )
                        if page_data and page_data.text:
                            r['full_text'] = page_data.text
                            _render_full_text(tc, page_data.text, hp)
                        else:
                            logger.warning("Lazy load: no page data for sys_id=%s p_num=%d", sid, p_num)
                    except Exception as e:
                        logger.error("Lazy load error for sys_id=%s: %s", sid, e, exc_info=True)

                # Hook into toggle: load text on first expand
                _orig_toggle_fn = lambda i, _ss=search_state, _r=refs: toggle_expansion(_ss, _r, i)

                def _make_lazy_toggle(idx, load_fn, orig_fn):
                    def _toggle(i):
                        orig_fn(i)
                        if search_state.expanded_index == idx:
                            asyncio.ensure_future(load_fn())
                    return _toggle

                # Patch the click handler for this card to also trigger lazy load
                # We do this by wrapping the card's click — find the content column and re-bind
                # Simpler: just trigger lazy load whenever this card expands
                search_state._lazy_loaders = getattr(search_state, '_lazy_loaders', {})
                search_state._lazy_loaders[index] = _lazy_load_text



def open_advanced_dialog(search_state, refs, index, result):
    """Open an enhanced Advanced View dialog with in-place navigation and IIIF image viewer.

    When index is None, operates in standalone mode: wraps the single result in a list,
    uses index=0, and disables navigation. This is used for excluded results and tag search.
    """
    standalone = (index is None)
    # PostHog: track result clicks
    from web.analytics import posthog_capture
    display = result.get('display', {}) if isinstance(result, dict) else {}
    posthog_capture('result_opened', {
        'shelfmark': display.get('shelfmark', '')[:80],
        'sys_id': display.get('id', ''),
        'result_index': index if not standalone else 0,
    })

    service = get_service()
    adv_state = AdvancedViewState()
    if standalone:
        adv_state.current_result_idx = 0
        adv_state.results = [result]
    else:
        adv_state.current_result_idx = index
        adv_state.results = search_state.displayed_results

    with ui.dialog().props('maximized') as dialog:
        with ui.card().classes('w-full h-full flex flex-col').style('background: var(--bg-secondary);'):
            # === Header Bar (compact: close, shelfmark, result nav, fullscreen) ===
            adv_state.header_container = ui.row().classes('w-full px-4 py-2 items-center justify-between shrink-0').style(
                'background: var(--bg-header); color: white;'
            )
            with adv_state.header_container:
                # Left: Close and result counter
                with ui.row().classes('items-center gap-2'):
                    ui.button(icon='close', on_click=dialog.close).props('flat round color=white size=sm')
                    if standalone:
                        _sm = display.get('shelfmark', 'Unknown')
                        adv_state.result_label = ui.label(_sm).classes('text-sm font-medium')
                    else:
                        adv_state.result_label = ui.label(
                            f"{tr('Result')} {index + 1} / {len(adv_state.results)}"
                        ).classes('text-sm font-medium')

                # Center: Score badge (will be updated in-place)
                adv_state.score_badge = ui.element('div').classes('flex items-center gap-2')

                # Right: Navigation and Fullscreen
                with ui.row().classes('items-center gap-2'):
                    adv_state.prev_btn = ui.button(
                        icon='chevron_right' if is_rtl() else 'chevron_left',
                        on_click=lambda: navigate_result(-1)
                    ).props('flat round color=white size=sm').tooltip(tr('Previous'))

                    adv_state.next_btn = ui.button(
                        icon='chevron_left' if is_rtl() else 'chevron_right',
                        on_click=lambda: navigate_result(1)
                    ).props('flat round color=white size=sm').tooltip(tr('Next'))

                    if standalone:
                        adv_state.prev_btn.set_enabled(False)
                        adv_state.next_btn.set_enabled(False)

                    ui.separator().props('vertical').classes('mx-1 h-4 bg-gray-400')

                    def toggle_fullscreen():
                        adv_state.is_fullscreen = not adv_state.is_fullscreen
                        render_content(adv_state.results[adv_state.current_result_idx])

                    ui.button(
                        icon='fullscreen',
                        on_click=toggle_fullscreen
                    ).props('flat round color=white size=sm').tooltip(tr('Fullscreen'))

            # === Info Bar (shelfmark, buttons, chips — rendered in render_content) ===
            adv_state.info_bar_container = ui.element('div').classes('w-full shrink-0').style(
                'background: var(--bg-primary); border-bottom: 1px solid var(--border-light);'
            )

            # === Main Content (refreshable container) ===
            with ui.scroll_area().classes('flex-grow'):
                adv_state.content_container = ui.column().classes('w-full max-w-6xl mx-auto px-4 py-2 gap-2')

    def navigate_result(direction: int):
        """Navigate to prev/next result with in-place update (no dialog close/reopen)."""
        new_idx = adv_state.current_result_idx + direction
        if 0 <= new_idx < len(adv_state.results):
            adv_state.current_result_idx = new_idx
            load_result(new_idx)

    def load_result(idx: int):
        """Load a result into the dialog, updating UI in-place."""
        result = adv_state.results[idx]
        display = result.get('display', {})
        adv_state.current_sys_id = display.get('id', '')

        # Reset enrichment data for new result
        adv_state.fjms_data = None
        adv_state.crossref_data = None

        # Update header label
        adv_state.result_label.set_text(
            f"{tr('Result')} {idx + 1} / {len(adv_state.results)}"
        )

        # Update navigation button states
        adv_state.prev_btn.set_enabled(idx > 0)
        adv_state.next_btn.set_enabled(idx < len(adv_state.results) - 1)

        # Update score badge
        adv_state.score_badge.clear()
        sort_score = result.get('sort_score')
        if sort_score is not None:
            score_pct = min(100, max(0, int(sort_score)))
            with adv_state.score_badge:
                with ui.element('div').classes('flex items-center gap-2 px-3 py-1 rounded-full').style(
                    'background: rgba(255,255,255,0.15);'
                ):
                    ui.icon('insights').classes('text-sm')
                    ui.label(f"{tr('Score')}: {score_pct}").classes('text-sm font-medium')

        # Load browse page data for this result
        page_num_str = display.get('img', '1')
        try:
            initial_p_num = int(page_num_str) if page_num_str else 1
        except (ValueError, TypeError):
            initial_p_num = 1

        adv_state.current_p_num = initial_p_num

        # Extract volume IE from result header for multi-IE manuscripts
        adv_state.volume_ie = None
        if 'raw_header' in result and state.meta_mgr:
            try:
                _parsed_hdr = state.meta_mgr.parse_full_id_components(result['raw_header'])
                adv_state.volume_ie = _parsed_hdr.get('ie_id')
            except Exception:
                pass  # Browse enrichment failed; continue with available data

        # Fetch page data asynchronously
        async def fetch_and_render():
            if adv_state.current_sys_id:
                page = await run.io_bound(lambda: service.get_browse_page(
                    adv_state.current_sys_id, p_num=adv_state.current_p_num,
                    volume_ie=adv_state.volume_ie
                ))
                adv_state.current_page = page
                if page:
                    adv_state.total_pages = page.total_pages
                    adv_state.current_fl_id = page.fl_id
            else:
                adv_state.current_page = None
                adv_state.total_pages = 1

            # Fetch FJMS + crossref enrichment
            if adv_state.current_sys_id:
                sid = adv_state.current_sys_id
                try:
                    from shared.fjms_service import get_fjms_service
                    fjms = get_fjms_service(thread_safe=True)
                    if fjms.is_available():
                        adv_state.fjms_data = await run.io_bound(lambda: {
                            'catalog_records': fjms.get_catalog_records(sid),
                            'domains': fjms.get_domains(sid),
                            'bibliography': fjms.get_bibliography(sid),
                            'source_names': fjms.get_source_names(sid),
                            'catalog_refs': fjms.get_catalog_refs(sid),
                        })
                except Exception:
                    pass  # Enrichment failed for this item; continue with available data
                try:
                    from shared.nli_crossref_service import get_nli_crossref_service
                    svc = get_nli_crossref_service(thread_safe=True)
                    if svc.is_available():
                        adv_state.crossref_data = await run.io_bound(
                            lambda: svc.get_crossref_metadata(sid)
                        )
                except Exception:
                    pass  # Enrichment failed for this item; continue with available data

            render_content(result)

        asyncio.ensure_future(fetch_and_render())

    async def load_page(direction: int = 0, p_num: int = None):
        """Load a specific page within the current manuscript."""
        if not adv_state.current_sys_id:
            return

        target_p_num = p_num if p_num is not None else adv_state.current_p_num
        page = await run.io_bound(lambda: service.get_browse_page(
            adv_state.current_sys_id, p_num=target_p_num, direction=direction,
            volume_ie=adv_state.volume_ie
        ))

        if page:
            adv_state.current_page = page
            adv_state.current_p_num = page.p_num
            adv_state.total_pages = page.total_pages
            adv_state.current_fl_id = page.fl_id
            render_content(adv_state.results[adv_state.current_result_idx])

    # === Edit Mode Functions ===
    def toggle_edit_mode(current_text: str):
        """Enter edit mode with the current text."""
        from web.auth_state import GlobalAuthState
        if not GlobalAuthState.is_logged_in():
            ui.notify(tr('Please login to edit'), type='warning')
            return

        adv_state.edit_mode = True
        adv_state.edit_text = current_text
        adv_state.original_edit_text = current_text
        adv_state.edit_notes = ""
        adv_state.draft_saved = False
        adv_state.draft_id = None
        render_content(adv_state.results[adv_state.current_result_idx])

    def cancel_edit(result):
        """Cancel edit mode and return to view mode."""
        adv_state.edit_mode = False
        adv_state.edit_text = ""
        adv_state.edit_notes = ""
        adv_state.draft_saved = False
        adv_state.draft_id = None
        render_content(result)

    def save_draft(sys_id: str, shelfmark: str, page_num: int, original_text: str):
        """Save current edit as draft."""
        from web.auth_state import GlobalAuthState
        from web.supabase_client import create_correction, update_correction

        if not GlobalAuthState.is_logged_in():
            ui.notify(tr('Please login to save'), type='warning')
            return

        user_id = GlobalAuthState.get_user_id()
        text = adv_state.edit_text
        notes = adv_state.edit_notes

        try:
            if adv_state.draft_id:
                # Update existing draft
                result = update_correction(adv_state.draft_id, {
                    'corrected_text': text,
                    'notes': notes
                })
            else:
                # Create new draft
                result = create_correction(
                    author_id=user_id,
                    sys_id=sys_id,
                    shelfmark=shelfmark or '',
                    page_number=page_num,
                    original_text=original_text,
                    corrected_text=text,
                    notes=notes,
                    status='draft'
                )
                if result.get('success') and result.get('correction'):
                    adv_state.draft_id = result['correction'].get('id')

            adv_state.draft_saved = True
            ui.notify(tr('Draft saved'), type='positive')
            render_content(adv_state.results[adv_state.current_result_idx])
        except Exception as e:
            ui.notify(f"{tr('Error')}: {str(e)}", type='negative')

    def submit_correction(sys_id: str, shelfmark: str, page_num: int, original_text: str, result):
        """Submit correction for review or publish directly."""
        from web.auth_state import GlobalAuthState
        from web.supabase_client import create_correction, update_correction

        if not GlobalAuthState.is_logged_in():
            ui.notify(tr('Please login to submit'), type='warning')
            return

        user_id = GlobalAuthState.get_user_id()
        text = adv_state.edit_text
        notes = adv_state.edit_notes

        # Determine status based on role
        if GlobalAuthState.is_admin() or GlobalAuthState.is_editor():
            status = 'approved'
        else:
            status = 'pending'

        try:
            if adv_state.draft_id:
                # Update existing draft to submitted
                update_correction(adv_state.draft_id, {
                    'corrected_text': text,
                    'notes': notes,
                    'status': status
                })
            else:
                # Create new correction
                create_correction(
                    author_id=user_id,
                    sys_id=sys_id,
                    shelfmark=shelfmark or '',
                    page_number=page_num,
                    original_text=original_text,
                    corrected_text=text,
                    notes=notes,
                    status=status
                )

            # Exit edit mode
            adv_state.edit_mode = False
            adv_state.edit_text = ""
            adv_state.edit_notes = ""
            adv_state.draft_saved = False
            adv_state.draft_id = None

            if status == 'approved':
                ui.notify(tr('Correction published'), type='positive')
            else:
                ui.notify(tr('Correction submitted for review'), type='positive')

            render_content(result)
        except Exception as e:
            ui.notify(f"{tr('Error')}: {str(e)}", type='negative')

    def _apply_highlight_marks(text: str, terms: list) -> str:
        """Apply <mark> highlight tags around terms and convert newlines to <br>."""
        for term in terms:
            if term in text:
                text = text.replace(
                    term,
                    f'<mark style="background-color: #fef08a; padding: 2px 4px; border-radius: 3px; font-weight: 600;">{term}</mark>'
                )
        return text.replace('\n', '<br>')

    def render_content(result):
        """Render the main content area."""
        adv_state.content_container.clear()

        display = result.get('display', {})
        shelfmark = display.get('shelfmark', 'Unknown')
        title = display.get('title', '')
        sys_id = display.get('id', '')
        snippet = result.get('snippet', '')
        full_text = result.get('full_text', '')
        source = display.get('source', '')
        page_num = display.get('img', '')
        library_code = display.get('library_code', '')

        # Use current page data if available
        page = adv_state.current_page
        current_text = page.text if page else full_text
        current_p_num = page.p_num if page else adv_state.current_p_num
        total_pages = page.total_pages if page else 1

        # Fetch PGP transcription data for Advanced View
        pgp_transcription = None
        pgp_metadata = None
        all_sources = None
        if sys_id:
            try:
                all_sources_raw = get_all_sources_for_fragment(sys_id)
                current_page_info = 'recto' if current_p_num == 1 else 'verso'
                page_sources = []
                for src in all_sources_raw:
                    source_page = src.get('page_info')
                    if source_page == current_page_info or not source_page:
                        is_translation = 'Translation' in (src.get('doc_relation') or '')
                        if src.get('content'):
                            if not is_translation and not source_page:
                                src['content'] = get_section_for_page(src['content'], current_p_num, src.get('sections'))
                        page_sources.append(src)
                all_sources = page_sources if page_sources else None

                pgp_doc = get_document_for_fragment(sys_id, current_p_num)
                if pgp_doc:
                    pgpid = pgp_doc.get('pgpid')
                    pgp_metadata = {
                        'document_type': pgp_doc.get('document_type'),
                        'tags': pgp_doc.get('tags', []),
                        'description': pgp_doc.get('description'),
                        'languages_primary': pgp_doc.get('languages_primary'),
                        'languages_secondary': pgp_doc.get('languages_secondary'),
                        'inferred_date_display': pgp_doc.get('inferred_date_display'),
                        'doc_date_standard': pgp_doc.get('doc_date_standard'),
                        'doc_date_original': pgp_doc.get('doc_date_original'),
                        'inferred_date_rationale': pgp_doc.get('inferred_date_rationale'),
                        'pgp_url': pgp_doc.get('pgp_url'),
                        'pgpid': pgpid,
                    }
                    doc_relation = pgp_doc.get('doc_relation') or ''
                    is_edition = 'Edition' in doc_relation or not doc_relation
                    page_content = get_section_for_page(pgp_doc['transcription'], current_p_num) if pgp_doc.get('transcription') else None
                    if is_edition and page_content:
                        pgp_transcription = {
                            'full_content': pgp_doc['transcription'],
                            'content': page_content,
                            'attribution': pgp_doc.get('transcription_source', 'PGP'),
                            'pgp_url': pgp_doc.get('pgp_url'),
                            'pgpid': pgpid
                        }
            except Exception as pgp_err:
                logger.error(f"Advanced View: Failed to fetch PGP transcription: {pgp_err}")

        # Extract FL ID and IE ID from header
        fl_id = adv_state.current_fl_id
        ie_id = None
        if 'raw_header' in result and state.meta_mgr:
            try:
                parsed = state.meta_mgr.parse_full_id_components(result['raw_header'])
                if not fl_id:
                    fl_id = parsed.get('fl_id')
                ie_id = parsed.get('ie_id')
            except Exception:
                pass  # URL parse failed; return error response

        # Determine if Oxford manuscript
        is_oxford = is_oxford_manuscript(shelfmark, library_code)

        # Compute image URL (with volume suffix for multi-IE manuscripts)
        has_image = bool(sys_id)
        page_idx = max(0, current_p_num - 1)
        _vol_suffix = 1
        if ie_id and sys_id:
            from genizah_core import get_volumes_for_sys_id
            _vols = get_volumes_for_sys_id(sys_id)
            if _vols:
                for _v in _vols:
                    if _v['ie_id'] == ie_id:
                        _vol_suffix = _v['suffix']
                        break
        if is_oxford and sys_id:
            img_url = get_oxford_direct_image_url(shelfmark, page_idx)
            if not img_url:
                img_url = f"/api/oxford_image/{sys_id}?page={page_idx}"
        elif sys_id:
            _suffix_param = f'&suffix={_vol_suffix}' if _vol_suffix > 1 else ''
            img_url = f"/api/nli_image_by_sysid/{sys_id}?page={page_idx}{_suffix_param}"
        else:
            img_url = None

        # Get library display name
        library_name = ''
        if library_code:
            library_name = get_library_display(library_code, short=False, lang=get_language())
        display_shelfmark = f"{library_name}, {shelfmark}" if library_name else shelfmark

        # Use PGP transcription content if available, otherwise fall back to original
        if all_sources:
            editions = [s for s in all_sources if 'Edition' in (s.get('doc_relation') or '') and s.get('content')]
            if editions:
                display_text = editions[0].get('content', current_text or '')
            else:
                display_text = current_text or snippet.replace('*', '') if snippet else ''
        elif pgp_transcription and pgp_transcription.get('content'):
            display_text = pgp_transcription['content']
        else:
            display_text = current_text or snippet.replace('*', '') if snippet else ''

        # Apply highlighting from snippet if we have match markers
        if snippet and '*' in snippet and display_text:
            import re as re_module
            highlighted_terms = re_module.findall(r'\*([^*]+)\*', snippet)
            adv_state.highlight_terms = highlighted_terms
            text_html = _apply_highlight_marks(display_text, highlighted_terms)
        else:
            adv_state.highlight_terms = []
            text_html = display_text.replace('\n', '<br>') if display_text else ''

        with adv_state.content_container:

            # ============================================================
            # FULLSCREEN MODE - Compact layout with text and image only
            # ============================================================
            if adv_state.is_fullscreen:
                # Hide the info bar in fullscreen mode
                adv_state.info_bar_container.clear()
                # Compact info bar
                with ui.row().classes('w-full items-center justify-between p-2 mb-2 rounded-lg').style(
                    'background: var(--bg-tertiary);'
                ):
                    # Left: Shelfmark and page info
                    with ui.row().classes('items-center gap-3'):
                        ui.label(display_shelfmark).classes('font-bold text-sm').style('color: var(--primary-700);')
                        if title:
                            # Resolve title by language
                            _bar_title = title
                            if sys_id and search_state.title_translations:
                                _bar_tt = search_state.title_translations.get(sys_id)
                                if _bar_tt:
                                    _bar_lang = get_language()
                                    _bar_title = (_bar_tt.get('english_title') or _bar_tt.get('hebrew_title') or title) if _bar_lang != 'he' else (_bar_tt.get('hebrew_title') or _bar_tt.get('english_title') or title)
                            _bar_dir = 'ltr' if get_language() != 'he' else 'rtl'
                            ui.label(f"| {_bar_title[:50]}{'...' if len(_bar_title) > 50 else ''}").classes('text-xs').style(
                                f'color: var(--text-muted); direction: {_bar_dir};'
                            )

                    # Center: Page navigation
                    with ui.row().classes('items-center gap-2'):
                        if total_pages > 1:
                            prev_pg_btn = ui.button(
                                icon='chevron_right' if is_rtl() else 'chevron_left',
                                on_click=lambda: asyncio.ensure_future(load_page(direction=-1))
                            ).props('flat round size=sm').tooltip(tr('Previous Page'))
                            prev_pg_btn.set_enabled(current_p_num > 1)

                            ui.label(f"{tr('Page')} {current_p_num}/{total_pages}").classes('text-sm font-medium')

                            next_pg_btn = ui.button(
                                icon='chevron_left' if is_rtl() else 'chevron_right',
                                on_click=lambda: asyncio.ensure_future(load_page(direction=1))
                            ).props('flat round size=sm').tooltip(tr('Next Page'))
                            next_pg_btn.set_enabled(current_p_num < total_pages)
                        else:
                            ui.label(f"{tr('Page')} 1").classes('text-sm')

                    # Right: Action buttons
                    with ui.row().classes('items-center gap-1'):
                        if sys_id:
                            browse_url = f'/browse?sys_id={sys_id}'
                            if fl_id:
                                browse_url += f'&fl_id={fl_id}'
                            if ie_id:
                                browse_url += f'&volume_ie={ie_id}'
                            # Use ui.link for full page reload to ensure browse page recreates with PGP data
                            with ui.link(target=browse_url).classes('no-underline').tooltip(tr('Browse')):
                                ui.button(icon='menu_book').props('flat round size=sm')

                        if display_text:
                            ui.button(icon='content_copy', on_click=lambda t=display_text: copy_result_text(t)).props('flat round size=sm').tooltip(tr('Copy'))

                        # Exit fullscreen
                        def exit_fullscreen():
                            adv_state.is_fullscreen = False
                            render_content(result)
                        ui.button(icon='fullscreen_exit', on_click=exit_fullscreen).props('flat round size=sm').tooltip(tr('Exit Fullscreen'))

                # Two-panel layout for fullscreen
                with ui.row().classes('w-full gap-4 flex-nowrap').style('height: calc(100vh - 120px);'):
                    # Text panel
                    with ui.card().classes('flex-1 h-full overflow-hidden').style('border-radius: 12px;'):
                        with ui.scroll_area().classes('w-full h-full'):
                            with ui.element('div').classes('p-6').style(
                                'direction: rtl; text-align: right; '
                                'line-height: 2.4; font-size: 1.3rem; font-family: "SBL Hebrew", "David", serif;'
                            ):
                                if text_html:
                                    ui.html(text_html, sanitize=False)
                                else:
                                    ui.label(tr('No text available')).style('color: var(--text-muted);')

                    # Image panel (if available)
                    if has_image and img_url:
                        with ui.card().classes('flex-1 h-full overflow-hidden').style('border-radius: 12px;'):
                            # Image controls
                            with ui.row().classes('w-full items-center justify-between p-2').style('background: #1a1a1a;'):
                                ui.label(tr('Image')).classes('text-white text-sm')
                                with ui.row().classes('gap-1'):
                                    ui.button(icon='remove', on_click=lambda: ui.run_javascript('if(window.advViewer) window.advViewer.zoomOut()')).props('flat round size=xs text-color=white')
                                    ui.label('100%').classes('adv-zoom-label text-white text-xs px-1')
                                    ui.button(icon='add', on_click=lambda: ui.run_javascript('if(window.advViewer) window.advViewer.zoomIn()')).props('flat round size=xs text-color=white')
                                    ui.button(icon='rotate_right', on_click=lambda: ui.run_javascript('if(window.advViewer) window.advViewer.rotateRight()')).props('flat round size=xs text-color=white')
                                    ui.button(icon='restart_alt', on_click=lambda: ui.run_javascript('if(window.advViewer) window.advViewer.reset()')).props('flat round size=xs text-color=white')

                            # Image
                            safe_img_url = img_url.replace("'", "\\'").replace('"', '\\"')
                            safe_sys_id = (sys_id or '').replace("'", "\\'").replace('"', '\\"')
                            is_oxford_js = 'true' if is_oxford else 'false'

                            _adv_thumb = safe_img_url
                            _adv_full = safe_img_url
                            if '/api/nli_image_by_sysid/' in safe_img_url:
                                _sep = '&' if '?' in safe_img_url else '?'
                                _adv_thumb = f"{safe_img_url}{_sep}width=400"
                            with ui.element('div').classes('adv-image-container img-loading-container w-full').style('height: calc(100% - 48px);'):
                                img_html = f'''<img src="{_adv_thumb}" data-full-src="{_adv_full}" class="adv-zoomable-image" style="transform: translate(0px, 0px) rotate(0deg) scale(1); cursor: grab; max-height: 100%;" draggable="false" onload="if(window.advViewer) window.advViewer.init()" onerror="handleImageError(this, '{safe_sys_id}', {page_idx}, {is_oxford_js}, 'advViewer')"/>'''
                                ui.html(img_html, sanitize=False)
                                ui.run_javascript('setTimeout(() => { if(window.advViewer) window.advViewer.init(); initProgressiveImages(); }, 200);')

                return  # Exit early for fullscreen mode

            # ============================================================
            # NORMAL MODE - Compact info bar + text/image panels
            # ============================================================

            # --- Prepare enrichment data ---
            fjms_data = adv_state.fjms_data or {}
            from shared.fjms_service import get_fjms_service, merge_catalog_records, parse_textual_frame
            fjms = get_fjms_service(thread_safe=True)
            from web.components.bibliography_dialog import create_fjms_bibliography_dialog, create_nli_bibliography_dialog
            from web.components.catalog_dialog import show_catalog_dialog

            fjms_bib = fjms_data.get('bibliography', [])
            marc_bib = []
            try:
                from web.state import state as app_state
                if app_state.meta_mgr and hasattr(app_state.meta_mgr, 'nli_cache'):
                    cached = app_state.meta_mgr.nli_cache.get(sys_id, {})
                    marc_bib = cached.get('marc', {}).get('bibliography', [])
            except Exception:
                pass  # Enrichment failed for this item; continue with available data
            catalog_source_count = len(fjms_data.get('source_names', []))

            # === Info Bar (outside scroll area) ===
            adv_state.info_bar_container.clear()
            with adv_state.info_bar_container:
                with ui.column().classes('w-full max-w-6xl mx-auto px-4 py-2 gap-1'):
                    # Row 1: Shelfmark + action buttons
                    with ui.row().classes('items-center justify-between w-full gap-2'):
                        # Left: Shelfmark (compact)
                        with ui.row().classes('items-center gap-2 min-w-0 flex-shrink'):
                            ui.label(display_shelfmark).classes('text-sm font-bold truncate').style(
                                'color: var(--primary-700); max-width: 400px;'
                            )
                            # Resolve translated title for info bar — always language-aware
                            _adv_title = title
                            if sys_id and search_state.title_translations:
                                _adv_tt = search_state.title_translations.get(sys_id)
                                if _adv_tt:
                                    _lang = get_language()
                                    if _lang == 'he':
                                        _adv_title = _adv_tt.get('hebrew_title') or _adv_tt.get('english_title') or title
                                    else:
                                        _adv_title = _adv_tt.get('english_title') or _adv_tt.get('hebrew_title') or title
                            if _adv_title:
                                _adv_t_short = f'\u2014 {_adv_title[:60]}{"..." if _adv_title and len(_adv_title) > 60 else ""}'
                                _adv_orig = f'\u2014 {title[:60]}{"..." if title and len(title) > 60 else ""}' if title else ''
                                _adv_tt_resolved = search_state.title_translations.get(sys_id) if sys_id else None
                                _adv_dir = 'ltr' if (get_language() != 'he' and _adv_tt_resolved and _adv_tt_resolved.get('english_title')) else 'rtl'
                                if _adv_orig and _adv_orig != _adv_t_short:
                                    _ib_st = {'showing_original': False}
                                    with ui.row().classes('items-center gap-0 min-w-0'):
                                        _ib_lbl = ui.label(_adv_t_short).classes('text-xs truncate').style(
                                            f'color: var(--text-muted); direction: {_adv_dir}; max-width: 350px;'
                                        )
                                        def _make_ib_toggle(lbl, orig, resolved, flag, resolved_dir):
                                            def handler():
                                                flag['showing_original'] = not flag['showing_original']
                                                _dir = 'rtl' if flag['showing_original'] else resolved_dir
                                                lbl.text = orig if flag['showing_original'] else resolved
                                                lbl.style(f'color: var(--text-muted); direction: {_dir}; max-width: 350px;')
                                            return handler
                                        ui.button(icon='swap_horiz').props('flat dense round size=xs').style(
                                            'min-width: 18px; min-height: 18px; padding: 0; opacity: 0.4;'
                                        ).tooltip(tr('Show original title')).on(
                                            'click.stop', _make_ib_toggle(_ib_lbl, _adv_orig, _adv_t_short, _ib_st, _adv_dir)
                                        )
                                else:
                                    ui.label(_adv_t_short).classes(
                                        'text-xs truncate'
                                    ).style(f'color: var(--text-muted); direction: {_adv_dir}; max-width: 350px;')

                        # Right: Action buttons
                        with ui.row().classes('items-center gap-1 shrink-0 flex-wrap'):
                            if sys_id:
                                browse_url = f'/browse?sys_id={sys_id}'
                                if fl_id:
                                    browse_url += f'&fl_id={fl_id}'
                                if ie_id:
                                    browse_url += f'&volume_ie={ie_id}'
                                with ui.link(target=browse_url).classes('no-underline').tooltip(tr('Browse Full Manuscript')):
                                    ui.button(icon='menu_book').props('flat round size=sm color=green')

                            def make_add_handler(r):
                                def handler():
                                    show_add_to_list_dialog(r)
                                return handler
                            adv_result_sys_id = result.get('display', {}).get('id')
                            adv_result_in_list = state.lists_mgr and adv_result_sys_id and state.lists_mgr.is_item_in_any_list(adv_result_sys_id)
                            ui.button(icon='star' if adv_result_in_list else 'star_border', on_click=make_add_handler(result)).props(
                                'flat round size=sm'
                            ).style('color: var(--accent-amber);').tooltip(tr('In List') if adv_result_in_list else tr('Add to List'))

                            if WEB_PUZZLE_ENABLED:
                                # Add to Puzzle button (Phase 49)
                                def _make_puzzle_handler(sid=adv_result_sys_id, fid=fl_id):
                                    def handler():
                                        param = f'{sid},{fid}' if fid else str(sid)
                                        ui.navigate.to(f'/puzzle?add={param}')
                                    return handler
                                ui.button(icon='extension', on_click=_make_puzzle_handler()).props(
                                    'flat round size=sm'
                                ).tooltip(tr('Add to Puzzle'))

                            if has_image:
                                def toggle_image():
                                    adv_state.show_image_panel = not adv_state.show_image_panel
                                    render_content(result)
                                ui.button(
                                    icon='image' if adv_state.show_image_panel else 'hide_image',
                                    on_click=toggle_image
                                ).props('flat round size=sm').tooltip(
                                    tr('Hide Image') if adv_state.show_image_panel else tr('Show Image')
                                )

                            ui.separator().props('vertical').classes('h-4')

                            # Bibliography / Catalog buttons
                            if fjms_bib:
                                fjms_dlg = create_fjms_bibliography_dialog(
                                    fjms_bib, sys_id, shelfmark=shelfmark or '',
                                )
                                ui.button(
                                    f'{tr("Bib")} ({len(fjms_bib)})',
                                    icon='menu_book', on_click=fjms_dlg.open,
                                ).props('outline dense size=sm').classes('text-xs').tooltip(tr('Bibliography FJMS'))
                            if marc_bib:
                                nli_dlg = create_nli_bibliography_dialog(
                                    marc_bib, sys_id, shelfmark=shelfmark or '',
                                )
                                ui.button(
                                    f'{tr("Ktiv")} ({len(marc_bib)})',
                                    icon='menu_book', on_click=nli_dlg.open,
                                ).props('outline dense size=sm').classes('text-xs').tooltip(tr('Bibliography Ktiv'))
                            if catalog_source_count > 0:
                                ui.button(
                                    f'{tr("Cat")} ({catalog_source_count})',
                                    icon='description',
                                    on_click=lambda s=sys_id, sm=shelfmark or '': show_catalog_dialog(s, sm, fjms),
                                ).props('outline dense size=sm').classes('text-xs').tooltip(tr('Catalog Records'))

                            # PGP expander button
                            if pgp_metadata:
                                ui.button(
                                    'PGP', icon='verified',
                                    on_click=lambda: ui.run_javascript("document.querySelector('.pgp-expand .q-expansion-item__toggle')?.click()"),
                                ).props('outline dense size=sm color=green').classes('text-xs')

                            # FJMS expander button
                            catalog_records = fjms_data.get('catalog_records')
                            if catalog_records:
                                ui.button(
                                    'FJMS', icon='library_books',
                                    on_click=lambda: ui.run_javascript("document.querySelector('.fjms-expand .q-expansion-item__toggle')?.click()"),
                                ).props('outline dense size=sm color=purple').classes('text-xs')

                    # Row 2: Chips (source, page, result#, domains)
                    with ui.row().classes('items-center gap-2 flex-wrap'):
                        if source:
                            with ui.element('div').classes('flex items-center gap-1 px-2 py-0.5 rounded-full').style(
                                'background: var(--primary-100); color: var(--primary-700);'
                            ):
                                ui.icon('source').classes('text-xs')
                                ui.label(source).classes('text-xs font-medium')

                        with ui.element('div').classes('flex items-center gap-1 px-2 py-0.5 rounded-full').style(
                            'background: var(--accent-blue); color: white;'
                        ):
                            ui.icon('description').classes('text-xs')
                            ui.label(f"{tr('Page')} {current_p_num}/{total_pages}").classes('text-xs font-medium')

                        with ui.element('div').classes('flex items-center gap-1 px-2 py-0.5 rounded-full').style(
                            'background: var(--bg-tertiary); color: var(--text-secondary);'
                        ):
                            ui.icon('tag').classes('text-xs')
                            ui.label(f"#{adv_state.current_result_idx + 1}").classes('text-xs font-medium')

                        # Subject Domains inline
                        domains = fjms_data.get('domains')
                        if domains:
                            lang = get_language()
                            all_domain_names = {d['domain'] for d in domains}
                            for dom in domains:
                                parent = dom.get('parent_domain')
                                if parent and parent in all_domain_names and parent != dom['domain']:
                                    continue
                                display_name = dom['domain_heb'] if lang == 'he' else dom['domain']
                                ui.link(
                                    display_name,
                                    f'/search?domain={quote(dom["domain"])}'
                                ).classes('text-xs px-2 py-0.5 rounded-full no-underline').style(
                                    'background: #f3e8ff; color: #7c3aed;'
                                )

                        # Printed material indicator in advanced view
                        if adv_state.current_sys_id and adv_state.current_sys_id in search_state.printed_ids:
                            from shared.fjms_service import PRINTED_BADGE_COLORS, PRINTED_LABEL_EN, PRINTED_LABEL_HE
                            from web.translations import get_language as _get_lang
                            _bg, _fg = PRINTED_BADGE_COLORS
                            _plabel = PRINTED_LABEL_HE if _get_lang() == 'he' else PRINTED_LABEL_EN
                            with ui.element('div').classes('flex items-center gap-1 px-2 py-0.5 rounded-full').style(
                                f'background: {_bg}; color: {_fg};'
                            ):
                                ui.icon('print').classes('text-xs')
                                ui.label(_plabel).classes('text-xs font-medium')

            # === Expandable PGP Metadata (inside scroll area, collapsed) ===
            if pgp_metadata:
                with ui.expansion(group='enrichment').classes('w-full pgp-expand').style(
                    'border-left: 3px solid #27ae60; border-radius: 8px; margin-bottom: 4px;'
                ).props('dense header-class="text-xs font-bold" label="PGP Details"'):
                    with ui.row().classes('gap-6 flex-wrap'):
                        doc_type = pgp_metadata.get('document_type')
                        lang_primary = pgp_metadata.get('languages_primary')
                        if doc_type or lang_primary:
                            with ui.column().classes('gap-1'):
                                ui.label(tr('Document Type')).classes('text-xs font-bold').style('color: var(--text-secondary);')
                                type_parts = [p for p in [doc_type, lang_primary, pgp_metadata.get('languages_secondary')] if p]
                                _type_text = ' \u00b7 '.join(type_parts)
                                # Phase 46-06: Show pre-computed document_type_he when UI is Hebrew
                                _adv_type_he = None
                                _pgpid_for_type = pgp_metadata.get('pgpid')
                                _show_type_trans = False
                                try:
                                    _show_type_trans = app.storage.user.get('show_translations', False)
                                except Exception:
                                    pass  # Translation lookup failed; continue without translation
                                _type_lang = get_language()
                                if _show_type_trans and _type_lang == 'he' and _pgpid_for_type and search_state.translation_data:
                                    _type_trans = search_state.translation_data.get(sys_id)
                                    if _type_trans:
                                        _adv_type_he = _type_trans.get('document_type_he')
                                if _adv_type_he:
                                    _type_st = {'showing_original': False, 'label': None, 'badge': None}
                                    def _make_type_toggle(st, orig, trans):
                                        def handler():
                                            st['showing_original'] = not st['showing_original']
                                            if st['showing_original']:
                                                st['label'].set_text(orig)
                                                st['label'].style('color: var(--text-primary); direction: ltr;')
                                                st['badge'].set_text(tr('Original'))
                                            else:
                                                st['label'].set_text(trans)
                                                st['label'].style('color: var(--text-primary); direction: rtl;')
                                                st['badge'].set_text(tr('Translated'))
                                        return handler
                                    _type_handler = _make_type_toggle(_type_st, _type_text, _adv_type_he)
                                    with ui.row().classes('items-center gap-1'):
                                        _type_st['label'] = ui.label(_adv_type_he).classes('text-sm').style('color: var(--text-primary); direction: rtl;')
                                        _type_st['badge'] = ui.button(tr('Translated'), on_click=_type_handler).props('flat dense no-caps size=xs').classes('text-xs px-1 py-0 rounded shrink-0').style(
                                            'background: #e0f2fe; color: #0369a1; font-style: italic; font-size: 0.65rem; min-height: 0; line-height: 1.2;'
                                        )
                                else:
                                    ui.label(_type_text).classes('text-sm').style('color: var(--text-primary);')

                        inferred_display = pgp_metadata.get('inferred_date_display')
                        doc_date_standard = pgp_metadata.get('doc_date_standard')
                        doc_date_original = pgp_metadata.get('doc_date_original')
                        if inferred_display or doc_date_standard or doc_date_original:
                            with ui.column().classes('gap-1'):
                                ui.label(tr('Date')).classes('text-xs font-bold').style('color: var(--text-secondary);')
                                primary_date = inferred_display or doc_date_standard
                                if primary_date:
                                    ui.label(primary_date).classes('text-sm').style('color: var(--text-primary);')
                                if doc_date_original and doc_date_original != primary_date:
                                    ui.label(f"({doc_date_original})").classes('text-xs').style('color: var(--text-tertiary);')

                    tags = pgp_metadata.get('tags', [])
                    if tags:
                        with ui.row().classes('gap-1 flex-wrap mt-2'):
                            for tag in tags:
                                ui.badge(tag, color='green').props('outline clickable').classes(
                                    'text-xs cursor-pointer'
                                ).on('click', lambda t=tag: (dialog.close(), ui.navigate.to(f'/search?tag={quote(t)}')))

                    description = (pgp_metadata.get('description') or '').strip()
                    if description:
                        with ui.column().classes('gap-1 mt-2'):
                            ui.label(tr('Description')).classes('text-xs font-bold').style('color: var(--text-secondary);')
                            # Phase 46: Show translated description when toggle is on and UI is Hebrew
                            _show_trans_adv = False
                            try:
                                _show_trans_adv = app.storage.user.get('show_translations', False)
                            except Exception:
                                pass  # Translation lookup failed; continue without translation
                            _adv_pgpid = pgp_metadata.get('pgpid')
                            _adv_trans_he = None
                            _adv_lang = get_language()
                            if _show_trans_adv and _adv_lang == 'he' and _adv_pgpid:
                                try:
                                    from shared.translation_service import TranslationService
                                    _tsvc_adv = TranslationService(thread_safe=True)
                                    _adv_trans_he = _tsvc_adv.get_pgp_description_he(_adv_pgpid)
                                    _tsvc_adv.close()
                                except Exception:
                                    pass  # Translation lookup failed; continue without translation
                            if _adv_trans_he:
                                _adv_st = {'showing_original': False, 'label': None, 'badge': None}
                                def _make_adv_toggle(st, orig, trans):
                                    def handler():
                                        st['showing_original'] = not st['showing_original']
                                        if st['showing_original']:
                                            st['label'].set_text(orig)
                                            st['label'].style('color: var(--text-primary); direction: ltr; white-space: pre-wrap;')
                                            st['badge'].set_text(tr('Original'))
                                        else:
                                            st['label'].set_text(trans)
                                            st['label'].style('color: var(--text-primary); direction: rtl; white-space: pre-wrap;')
                                            st['badge'].set_text(tr('Translated'))
                                    return handler
                                _adv_handler = _make_adv_toggle(_adv_st, description, _adv_trans_he)
                                with ui.row().classes('w-full items-start gap-1'):
                                    _adv_st['label'] = ui.label(_adv_trans_he).classes('flex-1 text-sm whitespace-pre-wrap').style(
                                        'color: var(--text-primary); direction: rtl;'
                                    )
                                    _adv_st['badge'] = ui.button(tr('Translated'), on_click=_adv_handler).props('flat dense no-caps size=xs').classes('text-xs px-1 py-0 rounded shrink-0 self-start mt-1').style(
                                        'background: #e0f2fe; color: #0369a1; font-style: italic; font-size: 0.65rem; min-height: 0; line-height: 1.2;'
                                    )
                                    from web.components.translation_report import create_report_button
                                    create_report_button(
                                        dataset='pgp', record_id=str(_adv_pgpid),
                                        field_name='description', direction='en2he',
                                        source_text=description, translated_text=_adv_trans_he,
                                    )
                            else:
                                ui.label(description).classes('text-sm whitespace-pre-wrap').style('color: var(--text-primary);')

                    if pgp_metadata.get('pgp_url'):
                        ui.link(tr('View on PGP'), pgp_metadata['pgp_url'], new_tab=True).classes(
                            'text-xs mt-2'
                        ).style('color: var(--primary-600);')

            # === Expandable FJMS Catalog (inside scroll area, collapsed) ===
            catalog_records = fjms_data.get('catalog_records')
            if catalog_records:
                with ui.expansion(group='enrichment').classes('w-full fjms-expand').style(
                    'border-left: 3px solid #9b59b6; border-radius: 8px; margin-bottom: 4px;'
                ).props('dense header-class="text-xs font-bold" label="FJMS Details"'):
                    merged = merge_catalog_records(catalog_records)
                    lang = get_language()

                    fjms_title = merged.get('title_heb') if lang == 'he' else merged.get('title')
                    if fjms_title and fjms_title.strip():
                        with ui.row().classes('gap-1 items-baseline'):
                            ui.label(tr('Title')).classes('text-xs font-bold').style('color: var(--text-secondary);')
                            ui.label(fjms_title).classes('text-sm').style('color: var(--text-primary);')

                    if merged.get('author_text') and merged['author_text'].strip():
                        with ui.row().classes('gap-1 items-baseline'):
                            ui.label(tr('Author')).classes('text-xs font-bold').style('color: var(--text-secondary);')
                            ui.label(merged['author_text']).classes('text-sm').style('color: var(--text-primary);')

                    date_val = merged.get('copy_date')
                    place_val = merged.get('copy_place')
                    if date_val or place_val:
                        with ui.row().classes('gap-4'):
                            if date_val:
                                with ui.row().classes('gap-1 items-baseline'):
                                    ui.label(tr('Copy Date')).classes('text-xs font-bold').style('color: var(--text-secondary);')
                                    ui.label(str(date_val)).classes('text-sm').style('color: var(--text-primary);')
                            if place_val:
                                with ui.row().classes('gap-1 items-baseline'):
                                    ui.label(tr('Place')).classes('text-xs font-bold').style('color: var(--text-secondary);')
                                    ui.label(place_val).classes('text-sm').style('color: var(--text-primary);')

                    frames = merged.get('textual_frames', [])
                    if frames:
                        ui.label(tr('Content Identification')).classes('text-xs font-bold mt-2').style('color: var(--text-secondary);')
                        max_initial = 10
                        show_frames = frames[:max_initial] if len(frames) > max_initial else frames
                        for frame in show_frames:
                            text_f = frame.get('heb') if lang == 'he' else frame.get('eng')
                            if not text_f or not text_f.strip():
                                text_f = frame.get('eng') if lang == 'he' else frame.get('heb')
                            if text_f and text_f.strip():
                                category, content = parse_textual_frame(text_f)
                                source_name = frame.get('source_name_heb') if lang == 'he' else frame.get('source_name')
                                with ui.row().classes('gap-1 items-baseline'):
                                    if category:
                                        ui.label(category).classes('text-xs font-bold').style('color: #9b59b6;')
                                        ui.label(content).classes('text-sm').style('color: var(--text-primary);')
                                    else:
                                        ui.label(text_f).classes('text-sm').style('color: var(--text-primary);')
                                    if source_name and source_name.strip():
                                        ui.label(f'({source_name})').classes('text-xs').style('color: var(--text-tertiary);')
                        if len(frames) > max_initial:
                            remaining = frames[max_initial:]
                            with ui.expansion(f'{tr("Show all")} {len(frames)} {tr("identifications")}').classes('text-xs'):
                                for frame in remaining:
                                    text_f = frame.get('heb') if lang == 'he' else frame.get('eng')
                                    if not text_f or not text_f.strip():
                                        text_f = frame.get('eng') if lang == 'he' else frame.get('heb')
                                    if text_f and text_f.strip():
                                        category, content = parse_textual_frame(text_f)
                                        source_name = frame.get('source_name_heb') if lang == 'he' else frame.get('source_name')
                                        with ui.row().classes('gap-1 items-baseline'):
                                            if category:
                                                ui.label(category).classes('text-xs font-bold').style('color: #9b59b6;')
                                                ui.label(content).classes('text-sm').style('color: var(--text-primary);')
                                            else:
                                                ui.label(text_f).classes('text-sm').style('color: var(--text-primary);')
                                            if source_name and source_name.strip():
                                                ui.label(f'({source_name})').classes('text-xs').style('color: var(--text-tertiary);')

                    cat_refs = fjms_data.get('catalog_refs')
                    if cat_refs:
                        ui.label(tr('Catalog References')).classes('text-xs font-bold mt-2').style('color: var(--text-secondary);')
                        with ui.row().classes('gap-2 flex-wrap'):
                            for ref in cat_refs:
                                acronym = ref.get('cat_acronym', '')
                                cat_entry = ref.get('catalog_entry', '')
                                ref_display = f"{acronym} #{cat_entry}" if cat_entry else acronym
                                ui.label(ref_display).classes('text-xs').style('color: var(--text-primary);')

            # === Two-Panel Layout: Text + Image ===
            with ui.row().classes('w-full gap-4 flex-wrap lg:flex-nowrap'):
                # Left Panel: Text content with inline editing
                text_panel_classes = 'flex-1 min-w-[300px]' if adv_state.show_image_panel and has_image else 'w-full'

                # Edit mode border styling
                panel_border = ''
                if adv_state.edit_mode:
                    panel_border = 'border: 3px solid #27ae60;' if adv_state.draft_saved else 'border: 3px solid #f39c12;'

                with ui.column().classes(text_panel_classes + ' gap-4'):

                    # Define text container and render function at this scope for version switching
                    text_content_container = None
                    current_display_text = {'value': display_text, 'html': text_html}

                    def render_text_section(html_to_render: str):
                        """Render pre-formatted HTML text content (called on initial render and version change)."""
                        nonlocal text_content_container
                        if text_content_container is None:
                            return
                        text_content_container.clear()
                        with text_content_container:
                            with ui.scroll_area().classes('w-full').style('max-height: 70vh;'):
                                with ui.element('div').classes('p-6').style(
                                    'direction: rtl; text-align: right; '
                                    'line-height: 2.4; font-size: 1.2rem; font-family: "SBL Hebrew", "David", serif;'
                                ):
                                    ui.html(html_to_render or '', sanitize=False)
                        text_content_container.update()

                    # Page Text Section with inline editing
                    if display_text or adv_state.edit_mode:
                        with ui.card().classes('w-full').style(f'border-radius: 16px; {panel_border}'):

                            if adv_state.edit_mode:
                                # === EDIT MODE ===
                                # Edit toolbar
                                with ui.row().classes('w-full items-center justify-between p-3 bg-gray-100 border-b'):
                                    with ui.row().classes('items-center gap-2'):
                                        ui.icon('edit').classes('text-primary')
                                        ui.label(tr('Edit Mode')).classes('font-bold')
                                        if adv_state.draft_saved:
                                            ui.label(tr('Saved')).classes('text-green-600 text-sm font-bold')
                                        else:
                                            ui.label(tr('Unsaved')).classes('text-orange-600 text-sm')

                                    with ui.row().classes('gap-2'):
                                        ui.button(tr('Cancel'), icon='close', on_click=lambda: cancel_edit(result)).props('flat dense color=grey')
                                        ui.button(tr('Save'), icon='save', on_click=lambda: save_draft(sys_id, shelfmark, current_p_num, current_text)).props('flat dense color=primary')
                                        ui.button(tr('Submit'), on_click=lambda: submit_correction(sys_id, shelfmark, current_p_num, current_text, result)).props('unelevated dense color=green')

                                # Editable textarea
                                textarea = ui.textarea(value=adv_state.edit_text).classes('w-full').props(
                                    'borderless autofocus'
                                ).style(
                                    'direction: rtl; text-align: right; resize: none; min-height: 400px; padding: 16px; '
                                    'font-family: "SBL Hebrew", "David", serif; font-size: 1.2rem; line-height: 2;'
                                )
                                textarea.bind_value(adv_state, 'edit_text')

                                def on_edit_change():
                                    if adv_state.draft_saved:
                                        adv_state.draft_saved = False
                                textarea.on('input', on_edit_change)

                                # Notes field
                                with ui.expansion(tr('Add Notes'), icon='note_add').classes('w-full border-t'):
                                    ui.textarea(value=adv_state.edit_notes, placeholder=tr('Notes about your correction')).bind_value(adv_state, 'edit_notes').classes('w-full').props('outlined dense').style('direction: rtl;')

                            else:
                                # === VIEW MODE ===
                                # Header with page info, page navigation, and actions
                                with ui.row().classes('items-center justify-between w-full px-4 py-2 border-b').style('border-color: var(--border-light);'):
                                    with ui.row().classes('items-center gap-2'):
                                        ui.icon('article').classes('text-lg').style('color: var(--primary-600);')
                                        ui.label(f"{tr('Page')} {current_p_num}").classes('text-sm font-bold')
                                        word_count = len(display_text.split()) if display_text else 0
                                        ui.label(f"({word_count} {tr('words')})").classes('text-xs').style('color: var(--text-muted);')

                                        # Page navigation (merged into header)
                                        if total_pages > 1:
                                            ui.separator().props('vertical').classes('h-4 mx-1')
                                            prev_page_btn = ui.button(
                                                icon='chevron_right' if is_rtl() else 'chevron_left',
                                                on_click=lambda: asyncio.ensure_future(load_page(direction=-1))
                                            ).props('flat round size=xs').tooltip(tr('Previous Page'))
                                            prev_page_btn.set_enabled(current_p_num > 1)

                                            page_input = ui.number(value=current_p_num, min=1, max=total_pages).classes('w-12').props('dense outlined borderless')
                                            ui.label(f"/{total_pages}").classes('text-xs').style('color: var(--text-secondary);')

                                            def go_to_page():
                                                try:
                                                    p = int(page_input.value) if page_input.value else 1
                                                    p = max(1, min(total_pages, p))
                                                    asyncio.ensure_future(load_page(p_num=p))
                                                except (ValueError, TypeError):
                                                    pass
                                            page_input.on('keydown.enter', lambda: go_to_page())

                                            next_page_btn = ui.button(
                                                icon='chevron_left' if is_rtl() else 'chevron_right',
                                                on_click=lambda: asyncio.ensure_future(load_page(direction=1))
                                            ).props('flat round size=xs').tooltip(tr('Next Page'))
                                            next_page_btn.set_enabled(current_p_num < total_pages)

                                    with ui.row().classes('gap-1'):
                                        ui.button(icon='content_copy', on_click=lambda t=display_text: copy_result_text(t)).props('flat round size=sm').tooltip(tr('Copy Text'))
                                        if sys_id and current_text:
                                            ui.button(icon='edit', on_click=lambda: toggle_edit_mode(current_text)).props('flat round size=sm').tooltip(tr('Edit'))

                                # Text content - create container (same scope as outer text_content_container)
                                text_content_container = ui.element('div').classes('w-full')

                                # Initial render (use highlighted HTML)
                                render_text_section(text_html)

                    # Community Features Row (compact) - only in view mode
                    if sys_id and current_text and not adv_state.edit_mode:
                        with ui.row().classes('gap-2 flex-wrap items-center'):
                            from web.components import (
                                create_version_selector,
                                create_comment_button, create_joins_button
                            )

                            def handle_version_change(new_text: str, version_info: dict):
                                """Handle version selection - update displayed text."""
                                current_display_text['value'] = new_text
                                # Re-apply search term highlighting to new version text
                                if adv_state.highlight_terms and new_text:
                                    new_html = _apply_highlight_marks(new_text, adv_state.highlight_terms)
                                else:
                                    new_html = new_text.replace('\n', '<br>') if new_text else ''
                                current_display_text['html'] = new_html
                                render_text_section(new_html)
                                source = version_info.get('source', 'unknown')

                                if source == 'pgp':
                                    attribution = version_info.get('attribution', 'PGP')
                                    ui.notify(f"{tr('PGP Transcription')} - {attribution}", type='positive')
                                elif source == 'translation':
                                    attribution = version_info.get('attribution', '')
                                    language = version_info.get('language', '')
                                    ui.notify(f"{language} {tr('Translation')} - {attribution}", type='info')
                                elif source == 'user' and version_info.get('author'):
                                    ui.notify(f"{tr('Showing version by')} {version_info.get('author')}", type='info')
                                elif source in ('V0.7', 'V0.8'):
                                    ui.notify(f"{tr('Showing')} {source}", type='info')

                            create_version_selector(
                                document_id=sys_id,
                                page_number=current_p_num,
                                original_text=current_text,
                                on_version_change=handle_version_change,
                                pgp_transcription=pgp_transcription,
                                all_sources=all_sources
                            )

                            create_comment_button(
                                document_id=sys_id,
                                page_number=current_p_num,
                                shelfmark=shelfmark,
                                size='sm'
                            )

                            if shelfmark:
                                def navigate_to_join(target_shelfmark: str):
                                    dialog.close()
                                    ui.navigate.to(f'/browse?shelfmark={target_shelfmark}')

                                create_joins_button(
                                    shelfmark=shelfmark,
                                    document_id=sys_id,
                                    on_navigate=navigate_to_join
                                )

                # Right Panel: Image viewer (toggleable)
                if adv_state.show_image_panel and has_image and img_url:
                    with ui.column().classes('flex-1 min-w-[300px]'):
                        with ui.card().classes('w-full').style('border-radius: 16px; overflow: hidden;'):
                            # Image controls header
                            with ui.row().classes('w-full items-center justify-between p-3').style(
                                'background: #1a1a1a; border-radius: 8px 8px 0 0;'
                            ):
                                ui.label(tr('Manuscript Image')).classes('text-white font-semibold')
                                with ui.row().classes('gap-1'):
                                    ui.button(icon='remove', on_click=lambda: ui.run_javascript('if(window.advViewer) window.advViewer.zoomOut()')).props('flat round size=sm text-color=white').tooltip(tr('Zoom out'))
                                    ui.label('100%').classes('adv-zoom-label text-white text-sm px-2')
                                    ui.button(icon='add', on_click=lambda: ui.run_javascript('if(window.advViewer) window.advViewer.zoomIn()')).props('flat round size=sm text-color=white').tooltip(tr('Zoom in'))
                                    ui.separator().props('vertical').classes('mx-1 h-4 bg-gray-600')
                                    ui.button(icon='rotate_left', on_click=lambda: ui.run_javascript('if(window.advViewer) window.advViewer.rotateLeft()')).props('flat round size=sm text-color=white').tooltip(tr('Rotate Left'))
                                    ui.button(icon='rotate_right', on_click=lambda: ui.run_javascript('if(window.advViewer) window.advViewer.rotateRight()')).props('flat round size=sm text-color=white').tooltip(tr('Rotate Right'))
                                    ui.separator().props('vertical').classes('mx-1 h-4 bg-gray-600')
                                    ui.button(icon='restart_alt', on_click=lambda: ui.run_javascript('if(window.advViewer) window.advViewer.reset()')).props('flat round size=sm text-color=white').tooltip(tr('Reset View'))

                            # Image adjustment controls
                            with ui.row().classes('w-full items-center gap-2 px-3 py-1').style(
                                'background: #1a1a1a; border-top: 1px solid #333;'
                            ):
                                ui.icon('brightness_6').classes('text-white text-sm').tooltip(tr('Brightness'))
                                adv_state.brightness_sl = ui.slider(
                                    min=-100, max=100, step=1, value=0,
                                    on_change=lambda e: ui.run_javascript(f'if(window.advViewer) window.advViewer.setBrightness({e.value})')
                                ).props('dark dense').classes('w-24')
                                ui.icon('contrast').classes('text-white text-sm').tooltip(tr('Contrast'))
                                adv_state.contrast_sl = ui.slider(
                                    min=-100, max=100, step=1, value=0,
                                    on_change=lambda e: ui.run_javascript(f'if(window.advViewer) window.advViewer.setContrast({e.value})')
                                ).props('dark dense').classes('w-24')
                                ui.icon('timeline').classes('text-white text-sm').tooltip(tr('Gamma'))
                                adv_state.gamma_sl = ui.slider(
                                    min=20, max=300, step=1, value=100,
                                    on_change=lambda e: ui.run_javascript(f'if(window.advViewer) window.advViewer.setGamma({e.value / 100})')
                                ).props('dark dense').classes('w-24')
                                ui.button(
                                    icon='exposure',
                                    on_click=lambda: ui.run_javascript('if(window.advViewer) window.advViewer.toggleInvert()')
                                ).props('flat round size=sm text-color=white').tooltip(tr('Invert Colors'))
                                def _adv_reset_adj():
                                    if hasattr(adv_state, 'brightness_sl'): adv_state.brightness_sl.value = 0
                                    if hasattr(adv_state, 'contrast_sl'): adv_state.contrast_sl.value = 0
                                    if hasattr(adv_state, 'gamma_sl'): adv_state.gamma_sl.value = 100
                                    ui.run_javascript('if(window.advViewer) window.advViewer.resetAdjustments()')
                                ui.button(
                                    icon='restart_alt',
                                    on_click=_adv_reset_adj
                                ).props('flat round size=sm text-color=white').tooltip(tr('Reset Image'))

                            # Image display
                            safe_img_url = img_url.replace("'", "\\'").replace('"', '\\"')
                            safe_sys_id = (sys_id or '').replace("'", "\\'").replace('"', '\\"')
                            is_oxford_js = 'true' if is_oxford else 'false'

                            _adv2_thumb = safe_img_url
                            _adv2_full = safe_img_url
                            if '/api/nli_image_by_sysid/' in safe_img_url:
                                _sep = '&' if '?' in safe_img_url else '?'
                                _adv2_thumb = f"{safe_img_url}{_sep}width=400"
                            with ui.element('div').classes('adv-image-container img-loading-container w-full').style('height: 70vh;'):
                                img_html = f'''
                                <img
                                    src="{_adv2_thumb}"
                                    data-full-src="{_adv2_full}"
                                    class="adv-zoomable-image"
                                    style="transform: translate(0px, 0px) rotate(0deg) scale(1); cursor: grab; max-height: 100%;"
                                    draggable="false"
                                    onload="if(window.advViewer) window.advViewer.init()"
                                    onerror="handleImageError(this, '{safe_sys_id}', {page_idx}, {is_oxford_js}, 'advViewer')"
                                />
                                '''
                                ui.html(img_html, sanitize=False)
                                ui.run_javascript('setTimeout(() => { if(window.advViewer) window.advViewer.init(); initProgressiveImages(); }, 200);')

                            # Attribution footer
                            attribution = ''
                            if is_oxford:
                                attribution = 'From the collections of the Bodleian Libraries, Oxford'
                            elif page and page.attribution:
                                attribution = page.attribution
                            else:
                                attribution = 'הספרייה הלאומית / National Library of Israel'

                            with ui.row().classes('w-full items-center justify-center gap-2 py-2').style(
                                'background: #2a2a2a; border-radius: 0 0 8px 8px;'
                            ):
                                ui.icon('photo_library', size='xs').style('color: #888; font-size: 14px;')
                                ui.label(attribution).classes('text-xs').style('color: #aaa; font-style: italic;')

            # === Actions Section ===
            with ui.card().classes('w-full p-6').style('border-radius: 16px; background: var(--bg-tertiary);'):
                h3(tr('Actions'), classes='text-lg font-bold mb-4', style='color: var(--text-primary);')

                with ui.row().classes('gap-4 flex-wrap'):
                    if sys_id:
                        browse_url = f'/browse?sys_id={sys_id}'
                        if fl_id:
                            browse_url += f'&fl_id={fl_id}'
                        # Use ui.link for full page reload to ensure browse page recreates with PGP data
                        with ui.link(target=browse_url).classes('btn-primary no-underline'):
                            ui.icon('menu_book').classes('mr-2')
                            ui.label(tr('Browse Full Manuscript'))

                    text_for_parallels = current_text or snippet.replace('*', '')
                    if text_for_parallels:
                        ui.button(
                            tr('Find Parallels'), icon='compare_arrows',
                            on_click=lambda t=text_for_parallels: (
                                dialog.close(),
                                ui.navigate.to(f'/parallels?text={quote(t[:2000])}')
                            )
                        ).props('outline')

                    text_to_copy = current_text or snippet.replace('*', '')
                    if text_to_copy:
                        ui.button(
                            tr('Copy Text'), icon='content_copy',
                            on_click=lambda t=text_to_copy: copy_result_text(t)
                        ).props('outline')

    # Initial load (use current_result_idx which handles standalone mode)
    load_result(adv_state.current_result_idx)
    dialog.open()

