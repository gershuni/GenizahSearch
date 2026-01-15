# -*- coding: utf-8 -*-
"""
Advanced Search Page - Genizah Search Pro

A comprehensive search interface with:
- Multiple search modes (exact, variants, fuzzy, regex, shelfmark, title)
- Advanced filtering options
- Lab Mode integration
- Real-time results with pagination
- Export capabilities
"""

from nicegui import ui, run, app
from web.state import state
from web.translations import tr, is_rtl
from urllib.parse import quote
import re
import html


def format_snippet(text):
    """Format snippet with highlighted matches, safely escaping HTML."""
    if not text:
        return ""
    # First escape HTML to prevent XSS
    escaped = html.escape(text)
    # Convert *word* to highlighted span (after escaping, markers are safe)
    return re.sub(
        r'\*(.*?)\*',
        r'<span class="highlight-match">\1</span>',
        escaped
    )


def create_search_page(initial_query: str = None):
    """Create the advanced search page."""

    # === State Management ===
    class SearchUIState:
        def __init__(self):
            self.progress = 0.0
            self.status = ""
            self.is_running = False
            self.results = []
            self.selected_result = None
            self.total_count = 0
            self.current_page_idx = 0  # For browse within viewer

    search_state = SearchUIState()

    # Restore previous results
    if 'search_results' in app.storage.user:
        try:
            search_state.results = app.storage.user.get('search_results', [])
        except Exception:
            pass

    # === UI Layout ===
    with ui.column().classes('w-full h-[calc(100vh-88px)] gap-0'):

        # === Search Header Panel ===
        with ui.card().classes('w-full p-6 rounded-none border-0 border-b').style(
            'background: var(--bg-card); border-color: var(--border-light) !important;'
        ):
            # Main Search Row
            with ui.row().classes('w-full items-end gap-4 flex-wrap'):

                # Search Input (Main)
                with ui.column().classes('flex-grow min-w-80 gap-1'):
                    ui.label(tr('Search Query')).classes('text-sm font-medium').style('color: var(--text-secondary);')
                    query_input = ui.input(
                        placeholder=tr('Enter Hebrew text to search'),
                        value=initial_query or ''
                    ).classes('w-full text-lg').props('outlined dense clearable').style('direction: rtl;')
                    query_input.on('keydown.enter', lambda: execute_search())

                # Mode Selector - Default to exact
                with ui.column().classes('gap-1'):
                    ui.label(tr('Mode')).classes('text-sm font-medium').style('color: var(--text-secondary);')
                    mode_select = ui.select(
                        {
                            'exact': tr('Exact') + ' (=)',
                            'variants': tr('Variants') + ' (?)',
                            'variants_extended': tr('Extended') + ' (??)',
                            'variants_maximum': tr('Maximum') + ' (???)',
                            'fuzzy': tr('Fuzzy') + ' (~)',
                            'Regex': tr('Regex') + ' (/)',
                            'Shelfmark': tr('Shelfmark') + ' (#)',
                            'Title': tr('Title') + ' ($)',
                        },
                        value='exact'  # Default to exact
                    ).classes('w-48').props('outlined dense')

                # Gap Control
                with ui.column().classes('gap-1'):
                    ui.label(tr('Gap')).classes('text-sm font-medium').style('color: var(--text-secondary);')
                    gap_input = ui.number(value=0, min=0, max=10).classes('w-20').props('outlined dense')
                    ui.tooltip(tr('Gap description'))

                # Search Button
                search_btn = ui.button(tr('Search'), icon='search', on_click=lambda: execute_search()).classes(
                    'btn-primary h-10 px-8'
                )

            # Advanced Options Row
            with ui.expansion(tr('Advanced Options'), icon='tune').classes('w-full mt-4').style(
                'background: var(--bg-tertiary); border-radius: 12px;'
            ):
                with ui.column().classes('w-full p-4 gap-6'):

                    # Options Grid
                    with ui.row().classes('w-full gap-8 flex-wrap'):

                        # Lab Mode Section
                        with ui.column().classes('gap-3 min-w-64'):
                            ui.label(tr('Lab Mode')).classes('font-bold').style('color: var(--text-primary);')
                            lab_mode = ui.switch(tr('Enable Lab Mode algorithms'))
                            with ui.row().classes('gap-2 items-center'):
                                deep_scan = ui.checkbox(tr('Deep Scan')).classes('text-sm')
                                ui.icon('info').classes('text-sm cursor-help').tooltip(
                                    tr('Searches more candidates for comprehensive results')
                                )

                        # NOT Filter Section
                        with ui.column().classes('gap-3 min-w-64'):
                            ui.label(tr('Exclude Words')).classes('font-bold').style('color: var(--text-primary);')
                            not_filter = ui.input(
                                placeholder=tr('Words to exclude (space separated)')
                            ).classes('w-full').props('outlined dense').style('direction: rtl;')
                            ui.label(tr('Results containing these words will be filtered out')).classes('text-xs').style('color: var(--text-muted);')

                        # Syntax Shortcuts Section
                        with ui.column().classes('gap-3 min-w-64'):
                            ui.label(tr('Shortcuts')).classes('font-bold').style('color: var(--text-primary);')
                            with ui.row().classes('gap-2 flex-wrap'):
                                shortcuts = [
                                    ('=', tr('Exact')),
                                    ('?', tr('Variants')),
                                    ('??', tr('Extended')),
                                    ('/', tr('Regex')),
                                    ('#', tr('Shelfmark')),
                                    ('$', tr('Title')),
                                ]
                                for prefix, tip in shortcuts:
                                    ui.button(prefix, on_click=lambda p=prefix: prepend_to_query(p)).props(
                                        'flat dense size=sm'
                                    ).tooltip(tip)

        # === Progress Bar ===
        progress_container = ui.column().classes('w-full')
        with progress_container:
            progress_bar = ui.linear_progress(0).props('stripe animate').classes('h-1 w-full opacity-0')
            status_label = ui.label('').classes('text-xs px-6 py-1').style('color: var(--text-muted);')

        # === Main Content Area (Splitter) ===
        with ui.splitter(value=35).classes('w-full flex-grow') as splitter:

            # === LEFT: Results List ===
            with splitter.before:
                results_header = ui.row().classes('w-full px-4 py-3 items-center justify-between').style(
                    'background: var(--bg-tertiary); border-bottom: 1px solid var(--border-light);'
                )
                with results_header:
                    results_count = ui.label(tr('Results')).classes('font-medium').style('color: var(--text-secondary);')
                    with ui.row().classes('gap-2'):
                        ui.button(icon='description', on_click=lambda: ui.download('/api/export/word')).props(
                            'flat round dense size=sm'
                        ).tooltip(tr('Export Word'))
                        ui.button(icon='table_view', on_click=lambda: ui.download('/api/export/excel')).props(
                            'flat round dense size=sm'
                        ).tooltip(tr('Export Excel'))

                results_container = ui.scroll_area().classes('w-full flex-grow').style(
                    'background: var(--bg-secondary);'
                )

            # === RIGHT: Result Viewer ===
            with splitter.after:
                viewer_container = ui.column().classes('w-full h-full p-6').style('background: var(--bg-primary);')
                with viewer_container:
                    # Placeholder
                    with ui.column().classes('w-full h-full items-center justify-center'):
                        ui.icon('menu_book').classes('text-6xl').style('color: var(--text-muted);')
                        ui.label(tr('Select a result to view')).classes('mt-4').style('color: var(--text-muted);')

    # === Helper Functions ===

    def prepend_to_query(prefix):
        current = query_input.value or ""
        if not current.startswith(prefix):
            query_input.set_value(prefix + current)

    def update_progress_ui():
        if search_state.is_running:
            progress_bar.classes(remove='opacity-0')
            progress_bar.value = search_state.progress
            status_label.text = search_state.status
        else:
            if search_state.progress >= 1.0:
                progress_bar.value = 1.0
                status_label.text = tr("Done. Found {} results.").format(len(search_state.results))
                ui.timer(2.0, lambda: progress_bar.classes(add='opacity-0'), once=True)
            else:
                progress_bar.classes(add='opacity-0')

    ui.timer(0.1, update_progress_ui)

    async def execute_search():
        query = query_input.value.strip() if query_input.value else ""
        if not query:
            return

        if not state.is_ready():
            ui.notify(tr("Engine not ready."), type='warning')
            return

        # Parse syntax shortcuts
        mode = mode_select.value
        clean_query = query

        syntax_map = [
            ('???', 'variants_maximum'),
            ('??', 'variants_extended'),
            ('?', 'variants'),
            ('=', 'exact'),
            ('/', 'Regex'),
            ('#', 'Shelfmark'),
            ('$', 'Title'),
        ]

        for prefix, mode_val in syntax_map:
            if query.startswith(prefix):
                mode = mode_val
                clean_query = query[len(prefix):]
                mode_select.value = mode
                break

        # Reset UI
        search_state.is_running = True
        search_state.progress = 0
        search_state.status = tr("Starting...")
        search_state.results = []
        search_btn.disable()

        def progress_cb(current, total):
            if total > 0:
                search_state.progress = current / total
                search_state.status = f"{current} / {total}"

        # Get NOT filter words
        not_words = not_filter.value.split() if not_filter.value else []

        def run_core_search():
            try:
                if lab_mode.value:
                    lab_search_mode = 'variants' if mode not in ['Regex', 'exact'] else mode
                    return state.lab_engine.lab_search(
                        clean_query,
                        mode=lab_search_mode,
                        gap=int(gap_input.value),
                        deep_scan=deep_scan.value,
                        progress_callback=progress_cb
                    )
                else:
                    results = state.searcher.execute_search(
                        clean_query,
                        mode=mode,
                        gap=int(gap_input.value),
                        progress_callback=progress_cb
                    )

                    # Apply NOT filter
                    if not_words and results:
                        filtered = []
                        for r in results:
                            text = r.get('snippet', '') + ' ' + r.get('full_text', '')
                            text_lower = text.lower()
                            if not any(w.lower() in text_lower for w in not_words):
                                filtered.append(r)
                        return filtered

                    return results
            except Exception as e:
                print(f"Search Error: {e}")
                import traceback
                traceback.print_exc()
                return []

        results = await run.io_bound(run_core_search)

        # Save results
        state.last_results = results
        search_state.is_running = False
        search_state.progress = 1.0
        search_state.results = results
        search_btn.enable()

        try:
            app.storage.user['search_results'] = results
        except Exception:
            pass

        # Update count
        results_count.text = f"{len(results)} {tr('Results')}"

        # Render results
        render_results(results[:200])
        if len(results) > 200:
            ui.notify(tr("Showing first 200 results. Refine search."), type='info')

    def render_results(results):
        results_container.clear()

        if not results:
            with results_container:
                with ui.column().classes('w-full h-64 items-center justify-center'):
                    ui.icon('search').classes('text-5xl').style('color: var(--text-muted);')
                    ui.label(tr("Ready to search.")).classes('mt-4').style('color: var(--text-muted);')
            return

        with results_container:
            with ui.column().classes('w-full gap-2 p-4'):
                for i, res in enumerate(results):
                    create_result_card(i, res)

    def create_result_card(index, result):
        display = result.get('display', {})
        shelfmark = display.get('shelfmark', 'Unknown')
        title = display.get('title', '')
        snippet = result.get('snippet', '')

        # Truncate title for display
        title_short = (title[:60] + '...') if title and len(title) > 60 else title

        with ui.card().classes(
            'w-full p-4 cursor-pointer transition-all hover:shadow-md'
        ).style('border-radius: 10px;').on('click', lambda r=result: load_in_viewer(r)):
            with ui.row().classes('w-full items-start justify-between'):
                with ui.column().classes('flex-grow gap-1'):
                    with ui.row().classes('items-center gap-2'):
                        ui.label(f"#{index + 1}").classes('text-xs px-2 py-0.5 rounded').style(
                            'background: var(--bg-tertiary); color: var(--text-muted);'
                        )
                        ui.label(shelfmark).classes('font-bold').style('color: var(--primary-700);')
                    if title_short:
                        ui.label(title_short).classes('text-xs truncate').style(
                            'color: var(--text-tertiary); max-width: 300px; direction: rtl;'
                        )

                # Actions
                with ui.row().classes('gap-1'):
                    def make_star_handler(r):
                        def handler():
                            show_add_to_list_dialog(r)
                        return handler
                    ui.button(
                        icon='star_border',
                        on_click=make_star_handler(result)
                    ).props('flat round dense size=sm').style('color: var(--accent-amber);')

            # Snippet
            if snippet:
                snippet_html = format_snippet(snippet)
                with ui.element('div').classes('mt-3 p-3 rounded-lg text-sm').style(
                    'background: var(--bg-tertiary); direction: rtl; text-align: right; line-height: 1.8;'
                ):
                    ui.html(snippet_html, sanitize=False)

    def load_in_viewer(result):
        search_state.selected_result = result
        viewer_container.clear()

        display = result.get('display', {})
        shelfmark = display.get('shelfmark', 'Unknown')
        title = display.get('title', '')
        sys_id = display.get('id', '')
        snippet = result.get('snippet', '')
        full_text = result.get('full_text', '')

        # Initialize page index from display.img if not already set
        try:
            search_state.current_page_idx = int(display.get('img', '1'))
        except (ValueError, TypeError):
            search_state.current_page_idx = 1

        with viewer_container:
            # Header
            with ui.column().classes('w-full gap-2 mb-4'):
                ui.label(shelfmark).classes('text-2xl font-bold').style('color: var(--primary-700);')
                if title:
                    ui.label(title).style('color: var(--text-secondary); direction: rtl;')

                # Metadata badges
                with ui.row().classes('gap-2 flex-wrap mt-2'):
                    if display.get('source'):
                        ui.badge(display['source']).props('outline')
                    if display.get('img'):
                        ui.badge(f"{tr('Page')} {display['img']}").props('outline')

            # Content tabs
            with ui.tabs().classes('w-full') as tabs:
                tab_snippet = ui.tab('snippet', label=tr('Match'))
                tab_full = ui.tab('full', label=tr('Full Text'))
                tab_info = ui.tab('info', label=tr('Metadata'))

            with ui.tab_panels(tabs, value='snippet').classes('w-full flex-grow'):
                # Match tab
                with ui.tab_panel('snippet'):
                    if snippet:
                        snippet_html = format_snippet(snippet)
                        with ui.element('div').classes('p-4 rounded-lg').style(
                            'background: var(--bg-tertiary); direction: rtl; text-align: right; line-height: 2; font-size: 1.1rem;'
                        ):
                            ui.html(snippet_html, sanitize=False)

                # Full Text tab with navigation arrows
                with ui.tab_panel('full'):
                    # Navigation bar for browsing pages
                    if sys_id:
                        with ui.row().classes('w-full items-center justify-center gap-4 mb-4 p-2 rounded').style(
                            'background: var(--bg-tertiary);'
                        ):
                            # RTL: chevron_right = previous, chevron_left = next
                            ui.button(icon='chevron_right', on_click=browse_prev).props('flat round').tooltip(tr('Previous'))
                            page_label = ui.label(f"{tr('Page')} {search_state.current_page_idx}").style('color: var(--text-secondary);')
                            ui.button(icon='chevron_left', on_click=browse_next).props('flat round').tooltip(tr('Next'))

                    if full_text:
                        with ui.scroll_area().classes('w-full h-64'):
                            ui.label(full_text).classes('whitespace-pre-wrap').style(
                                'direction: rtl; text-align: right; line-height: 2; font-size: 1rem;'
                            )
                    else:
                        ui.label(tr('Full text not available')).style('color: var(--text-muted);')

                # Info tab
                with ui.tab_panel('info'):
                    with ui.column().classes('w-full gap-4'):
                        info_items = [
                            (tr('Shelfmark'), shelfmark),
                            (tr('Title'), title or tr('Not available')),
                            (tr('System ID'), sys_id or tr('Not available')),
                            (tr('Source'), display.get('source', tr('Not available'))),
                            (tr('Page'), display.get('img', tr('Not available'))),
                        ]
                        for label, value in info_items:
                            with ui.row().classes('w-full items-start gap-4'):
                                ui.label(label).classes('font-bold w-32').style('color: var(--text-secondary);')
                                ui.label(value).style('color: var(--text-primary); direction: rtl;')

            # Actions
            with ui.row().classes('w-full gap-3 mt-6 pt-6').style('border-top: 1px solid var(--border-light);'):
                if sys_id:
                    # Extract FL ID from result to jump to correct page
                    fl_id = None
                    if 'raw_header' in result and state.meta_mgr:
                        try:
                            parsed = state.meta_mgr.parse_full_id_components(result['raw_header'])
                            fl_id = parsed.get('fl_id')
                        except Exception:
                            pass

                    # Build browse URL with FL ID if available
                    browse_url = f'/browse?sys_id={sys_id}'
                    if fl_id:
                        browse_url += f'&fl_id={fl_id}'

                    ui.button(
                        tr('Browse Full Manuscript'),
                        icon='menu_book',
                        on_click=lambda url=browse_url: ui.navigate.to(url)
                    ).classes('btn-primary')

                # Find Parallels - pass the full text to parallels page
                text_for_parallels = full_text or snippet.replace('*', '')
                ui.button(
                    tr('Find Parallels'),
                    icon='compare_arrows',
                    on_click=lambda t=text_for_parallels: ui.navigate.to(f'/parallels?text={quote(t[:2000])}')
                ).props('outline')

    async def browse_prev():
        """Navigate to previous page in current manuscript (within viewer)."""
        if search_state.selected_result:
            display = search_state.selected_result.get('display', {})
            sys_id = display.get('id')
            current_page = search_state.current_page_idx

            if sys_id and current_page > 1:
                from web.services import get_service
                service = get_service()

                def load_prev():
                    return service.get_browse_page(sys_id, p_num=current_page, direction=-1)

                page_data = await run.io_bound(load_prev)
                if page_data:
                    search_state.current_page_idx = page_data.p_num
                    # Update the selected result with new page info
                    search_state.selected_result['full_text'] = page_data.text or ''
                    search_state.selected_result['display']['img'] = str(page_data.p_num)
                    load_in_viewer(search_state.selected_result)

    async def browse_next():
        """Navigate to next page in current manuscript (within viewer)."""
        if search_state.selected_result:
            display = search_state.selected_result.get('display', {})
            sys_id = display.get('id')
            current_page = search_state.current_page_idx

            if sys_id:
                from web.services import get_service
                service = get_service()

                def load_next():
                    return service.get_browse_page(sys_id, p_num=current_page, direction=1)

                page_data = await run.io_bound(load_next)
                if page_data:
                    search_state.current_page_idx = page_data.p_num
                    # Update the selected result with new page info
                    search_state.selected_result['full_text'] = page_data.text or ''
                    search_state.selected_result['display']['img'] = str(page_data.p_num)
                    load_in_viewer(search_state.selected_result)

    def show_add_to_list_dialog(result):
        display = result.get('display', {})
        sys_id = display.get('id')
        shelfmark = display.get('shelfmark', 'Unknown')

        if not sys_id:
            ui.notify(tr('Cannot add: missing system ID'), type='warning')
            return

        with ui.dialog() as dialog, ui.card().classes('p-6 min-w-96'):
            ui.label(tr('Add to List')).classes('text-xl font-bold mb-2')
            ui.label(f"{tr('Item')}: {shelfmark}").style('color: var(--text-secondary);')

            if state.lists_mgr:
                lists = state.lists_mgr.data.get('lists', {})
                list_options = {lid: lst['name'] for lid, lst in lists.items() if not lst.get('is_system')}

                if not list_options:
                    ui.label(tr('No lists available. Create a list first.')).style('color: var(--text-muted);')
                    ui.button(tr('Go to Lists'), on_click=lambda: ui.navigate.to('/lists')).classes('btn-primary mt-4')
                else:
                    selected_list = ui.select(list_options, label=tr('Select List')).classes('w-full mt-4').props('outlined').style('color: var(--text-primary);')
                    note_input = ui.input(label=tr('Note (optional)')).classes('w-full mt-2').props('outlined')

                    def add_to_list():
                        if state.lists_mgr.add_item(sys_id, selected_list.value, note=note_input.value):
                            ui.notify(tr('Added to list'), type='positive')
                            dialog.close()
                        else:
                            ui.notify(tr('Already in list'), type='info')

                    with ui.row().classes('w-full justify-end gap-2 mt-6'):
                        ui.button(tr('Cancel'), on_click=dialog.close).props('flat')
                        ui.button(tr('Add'), on_click=add_to_list).classes('btn-primary')
            else:
                ui.label(tr('Lists manager not available')).style('color: var(--error);')

        dialog.open()

    # Initialize with restored results or initial query
    if initial_query:
        ui.timer(0.5, execute_search, once=True)
    elif search_state.results:
        results_count.text = f"{len(search_state.results)} {tr('Results')}"
        render_results(search_state.results[:200])
