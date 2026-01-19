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
            self.selected_indices = set()  # For bulk operations

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
                    with ui.row().classes('items-center gap-3'):
                        # Select all checkbox
                        select_all_checkbox = ui.checkbox(on_change=lambda e: toggle_select_all(e.value)).props('dense')
                        results_count = ui.label(tr('Results')).classes('font-medium').style('color: var(--text-secondary);')
                        # Selection counter (initially hidden)
                        selection_counter = ui.label('').classes('text-sm').style('color: var(--primary-600); display: none;')

                    with ui.row().classes('gap-2'):
                        # Bulk actions (initially hidden)
                        bulk_actions_row = ui.row().classes('gap-2').style('display: none;')
                        with bulk_actions_row:
                            ui.button(icon='playlist_add', on_click=lambda: bulk_add_to_list()).props(
                                'flat round dense size=sm'
                            ).tooltip(tr('Add Selected to List'))
                            ui.button(icon='content_copy', on_click=lambda: bulk_copy_text()).props(
                                'flat round dense size=sm'
                            ).tooltip(tr('Copy Selected Text'))

                        # Filter toggle button
                        filter_btn = ui.button(icon='filter_list', on_click=lambda: toggle_filters()).props(
                            'flat round dense size=sm'
                        ).tooltip(tr('Toggle Filters'))

                        ui.button(icon='description', on_click=lambda: ui.download('/api/export/word')).props(
                            'flat round dense size=sm'
                        ).tooltip(tr('Export Word'))
                        ui.button(icon='table_view', on_click=lambda: ui.download('/api/export/excel')).props(
                            'flat round dense size=sm'
                        ).tooltip(tr('Export Excel'))

                # Filters Panel (initially hidden)
                filters_panel = ui.column().classes('w-full px-4 py-3 gap-3').style(
                    'background: var(--bg-tertiary); border-bottom: 1px solid var(--border-light); display: none;'
                )
                with filters_panel:
                    with ui.row().classes('w-full gap-2 items-center'):
                        ui.icon('filter_list').classes('text-sm').style('color: var(--text-muted);')
                        ui.label(tr('Filter Results')).classes('text-sm font-medium').style('color: var(--text-secondary);')

                    with ui.grid(columns=3).classes('w-full gap-2'):
                        filter_shelfmark = ui.input(
                            placeholder=tr('Filter by shelfmark')
                        ).classes('w-full').props('outlined dense clearable')

                        filter_title = ui.input(
                            placeholder=tr('Filter by title')
                        ).classes('w-full').props('outlined dense clearable').style('direction: rtl;')

                        filter_snippet = ui.input(
                            placeholder=tr('Filter by text')
                        ).classes('w-full').props('outlined dense clearable').style('direction: rtl;')

                    with ui.row().classes('gap-2'):
                        ui.button(tr('Apply Filters'), icon='check', on_click=lambda: apply_filters()).props(
                            'flat dense color=green size=sm'
                        )
                        ui.button(tr('Clear Filters'), icon='clear', on_click=lambda: clear_filters()).props(
                            'flat dense size=sm'
                        )

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

    # === Filtering Functions ===

    def toggle_filters():
        """Toggle visibility of filters panel."""
        current_display = filters_panel.style or ''
        if 'display: none' in current_display:
            filters_panel.style('background: var(--bg-tertiary); border-bottom: 1px solid var(--border-light);')
        else:
            filters_panel.style('background: var(--bg-tertiary); border-bottom: 1px solid var(--border-light); display: none;')

    def apply_filters():
        """Apply filters to results."""
        if not search_state.results:
            return

        filtered = []
        shelfmark_filter = (filter_shelfmark.value or '').lower().strip()
        title_filter = (filter_title.value or '').lower().strip()
        snippet_filter = (filter_snippet.value or '').lower().strip()

        for res in search_state.results:
            display = res.get('display', {})
            shelfmark = (display.get('shelfmark', '') or '').lower()
            title = (display.get('title', '') or '').lower()
            snippet = (res.get('snippet', '') or '').lower()

            # All filters must match
            if shelfmark_filter and shelfmark_filter not in shelfmark:
                continue
            if title_filter and title_filter not in title:
                continue
            if snippet_filter and snippet_filter not in snippet:
                continue

            filtered.append(res)

        render_results(filtered)
        results_count.text = f"{len(filtered)} / {len(search_state.results)} {tr('Results')}"
        ui.notify(f"{len(filtered)} {tr('results match filters')}", type='info')

    def clear_filters():
        """Clear all filters and show all results."""
        filter_shelfmark.value = ''
        filter_title.value = ''
        filter_snippet.value = ''

        if search_state.results:
            render_results(search_state.results)
            results_count.text = f"{len(search_state.results)} {tr('Results')}"
            ui.notify(tr('Filters cleared'), type='info')

    # === Bulk Operations ===

    def toggle_select_all(is_checked):
        """Toggle selection of all results."""
        if is_checked:
            # Select all current results
            for i in range(len(search_state.results)):
                search_state.selected_indices.add(i)
        else:
            # Deselect all
            search_state.selected_indices.clear()

        # Re-render to update checkboxes
        current_results = list(search_state.results)  # Keep current filtered view
        render_results(current_results)
        update_selection_ui()

    def update_selection_ui():
        """Update selection counter and bulk actions visibility."""
        selected_count = len(search_state.selected_indices)

        if selected_count > 0:
            selection_counter.text = f"{selected_count} {tr('selected')}"
            selection_counter.style('color: var(--primary-600);')
            bulk_actions_row.style('')  # Show bulk actions
        else:
            selection_counter.text = ''
            selection_counter.style('display: none;')
            bulk_actions_row.style('display: none;')  # Hide bulk actions

        # Update select all checkbox state
        if selected_count == len(search_state.results) and len(search_state.results) > 0:
            select_all_checkbox.value = True
        else:
            select_all_checkbox.value = False

    def bulk_add_to_list():
        """Add all selected results to a list."""
        if not search_state.selected_indices:
            ui.notify(tr('No results selected'), type='warning')
            return

        selected_results = [search_state.results[i] for i in sorted(search_state.selected_indices)
                          if i < len(search_state.results)]

        if not selected_results:
            ui.notify(tr('No valid selections'), type='warning')
            return

        # Show list selection dialog
        with ui.dialog() as dialog, ui.card().classes('p-6 min-w-96'):
            ui.label(tr('Add Selected to List')).classes('text-xl font-bold mb-2')
            ui.label(f"{len(selected_results)} {tr('items selected')}").style('color: var(--text-secondary);')

            if state.lists_mgr:
                lists = state.lists_mgr.data.get('lists', {})
                list_options = {lid: lst['name'] for lid, lst in lists.items() if not lst.get('is_system')}

                if not list_options:
                    ui.label(tr('No lists available. Create a list first.')).style('color: var(--text-muted);')
                    ui.button(tr('Go to Lists'), on_click=lambda: ui.navigate.to('/lists')).classes('btn-primary mt-4')
                else:
                    selected_list = ui.select(list_options, label=tr('Select List')).classes('w-full mt-4').props('outlined').style('color: var(--text-primary);')

                    def add_all():
                        added_count = 0
                        for res in selected_results:
                            display = res.get('display', {})
                            sys_id = display.get('id')
                            if sys_id and state.lists_mgr.add_item(sys_id, selected_list.value):
                                added_count += 1

                        ui.notify(f"{added_count} {tr('items added to list')}", type='positive')
                        dialog.close()

                    with ui.row().classes('w-full justify-end gap-2 mt-6'):
                        ui.button(tr('Cancel'), on_click=dialog.close).props('flat')
                        ui.button(tr('Add All'), on_click=add_all).classes('btn-primary')
            else:
                ui.label(tr('Lists manager not available')).style('color: var(--error);')

        dialog.open()

    def bulk_copy_text():
        """Copy all selected results' text to clipboard."""
        if not search_state.selected_indices:
            ui.notify(tr('No results selected'), type='warning')
            return

        selected_results = [search_state.results[i] for i in sorted(search_state.selected_indices)
                          if i < len(search_state.results)]

        if not selected_results:
            ui.notify(tr('No valid selections'), type='warning')
            return

        # Compile all text
        compiled_text = []
        for i, res in enumerate(selected_results, 1):
            display = res.get('display', {})
            shelfmark = display.get('shelfmark', 'Unknown')
            full_text = res.get('full_text', '')
            snippet = res.get('snippet', '').replace('*', '')
            text = full_text or snippet

            compiled_text.append(f"=== {i}. {shelfmark} ===\n{text}\n")

        final_text = '\n'.join(compiled_text)
        # Escape backticks for JavaScript
        escaped_text = final_text.replace('`', '\\`')

        # Copy to clipboard
        ui.run_javascript(f'''
            navigator.clipboard.writeText(`{escaped_text}`).then(() => {{
                console.log('Bulk text copied to clipboard');
            }});
        ''')
        ui.notify(f"{len(selected_results)} {tr('results copied to clipboard')}", type='positive')

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
        ).style('border-radius: 10px;') as card:
            with ui.row().classes('w-full items-start justify-between'):
                # Checkbox for selection
                with ui.column().classes('justify-center'):
                    def toggle_card_selection(e, idx=index):
                        if e.value:
                            search_state.selected_indices.add(idx)
                        else:
                            search_state.selected_indices.discard(idx)
                        update_selection_ui()

                    result_checkbox = ui.checkbox(
                        value=index in search_state.selected_indices,
                        on_change=toggle_card_selection
                    ).props('dense')

                # Main content (clickable)
                with ui.column().classes('flex-grow gap-1').on('click', lambda r=result: load_in_viewer(r)):
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
                    ui.button(
                        icon='open_in_full',
                        on_click=lambda idx=index, r=result: open_advanced_dialog(idx, r)
                    ).props('flat round dense size=sm').tooltip(tr('Advanced View'))

                    def make_star_handler(r):
                        def handler():
                            show_add_to_list_dialog(r)
                        return handler
                    ui.button(
                        icon='star_border',
                        on_click=make_star_handler(result)
                    ).props('flat round dense size=sm').style('color: var(--accent-amber);')

                    # Edit and Comment buttons
                    sys_id = display.get('id', '')
                    full_text = result.get('full_text', '')
                    page_num = int(display.get('img', '1'))
                    if full_text and sys_id:
                        from web.components import create_edit_button, create_comment_button
                        create_edit_button(
                            document_id=sys_id,
                            page_number=page_num,
                            original_text=full_text,
                            shelfmark=shelfmark,
                            size='sm'
                        )
                        create_comment_button(
                            document_id=sys_id,
                            page_number=page_num,
                            shelfmark=shelfmark,
                            size='sm'
                        )

            # Snippet
            if snippet:
                snippet_html = format_snippet(snippet)
                with ui.element('div').classes('mt-3 p-3 rounded-lg text-sm').style(
                    'background: var(--bg-tertiary); direction: rtl; text-align: right; line-height: 1.8;'
                ):
                    ui.html(snippet_html, sanitize=False)

    def open_advanced_dialog(index, result):
        """Open a maximized dialog showing the full result with navigation."""

        with ui.dialog().props('maximized') as dialog:
            with ui.card().classes('w-full h-full flex flex-col'):
                # Header with navigation
                with ui.row().classes('w-full p-4 items-center justify-between').style(
                    'background: var(--bg-tertiary); border-bottom: 1px solid var(--border-light);'
                ):
                    with ui.row().classes('items-center gap-3'):
                        ui.button(icon='close', on_click=dialog.close).props('flat round')
                        ui.label(tr('Advanced View')).classes('text-xl font-bold')

                    # Navigation controls
                    with ui.row().classes('items-center gap-2'):
                        result_counter = ui.label(f"{index + 1} / {len(search_state.results)}")

                        def navigate_result(direction):
                            new_idx = index + direction
                            if 0 <= new_idx < len(search_state.results):
                                dialog.close()
                                open_advanced_dialog(new_idx, search_state.results[new_idx])

                        ui.button(
                            icon='chevron_left',
                            on_click=lambda: navigate_result(-1)
                        ).props('flat round').tooltip(tr('Previous')).set_enabled(index > 0)

                        ui.button(
                            icon='chevron_right',
                            on_click=lambda: navigate_result(1)
                        ).props('flat round').tooltip(tr('Next')).set_enabled(index < len(search_state.results) - 1)

                # Content area
                with ui.scroll_area().classes('flex-grow p-6'):
                    render_dialog_content(result, dialog)

        dialog.open()

    def render_dialog_content(result, dialog):
        """Render the content inside the advanced dialog."""
        display = result.get('display', {})
        shelfmark = display.get('shelfmark', 'Unknown')
        title = display.get('title', '')
        sys_id = display.get('id', '')
        snippet = result.get('snippet', '')
        full_text = result.get('full_text', '')

        with ui.column().classes('w-full max-w-4xl mx-auto gap-6'):
            # Main Info Section
            with ui.card().classes('w-full p-6'):
                with ui.column().classes('gap-3'):
                    ui.label(shelfmark).classes('text-3xl font-bold').style('color: var(--primary-700);')

                    if title:
                        ui.label(title).classes('text-lg').style('color: var(--text-secondary); direction: rtl;')

                    # Badges
                    with ui.row().classes('gap-2 flex-wrap mt-2'):
                        if display.get('source'):
                            ui.badge(display['source'], color='blue')
                        if display.get('img'):
                            ui.badge(f"{tr('Page')} {display['img']}", color='green')

            # Metadata Section
            with ui.card().classes('w-full p-6'):
                ui.label(tr('Metadata')).classes('text-xl font-bold mb-4')
                with ui.column().classes('gap-3'):
                    metadata_items = [
                        (tr('Shelfmark'), shelfmark),
                        (tr('Title'), title or tr('Not available')),
                        (tr('System ID'), sys_id or tr('Not available')),
                        (tr('Source'), display.get('source', tr('Not available'))),
                        (tr('Page'), display.get('img', tr('Not available'))),
                    ]
                    for label, value in metadata_items:
                        with ui.row().classes('items-start gap-4'):
                            ui.label(label + ':').classes('font-bold w-32').style('color: var(--text-secondary);')
                            ui.label(value).style('color: var(--text-primary); direction: rtl;')

            # Snippet Section
            if snippet:
                with ui.card().classes('w-full p-6'):
                    ui.label(tr('Match Context')).classes('text-xl font-bold mb-4')
                    snippet_html = format_snippet(snippet)
                    with ui.element('div').classes('p-4 rounded-lg').style(
                        'background: var(--bg-tertiary); direction: rtl; text-align: right; line-height: 2; font-size: 1.1rem;'
                    ):
                        ui.html(snippet_html, sanitize=False)

            # Full Text Section
            if full_text:
                with ui.card().classes('w-full p-6'):
                    ui.label(tr('Full Text')).classes('text-xl font-bold mb-4')
                    with ui.element('div').classes('p-4 rounded-lg max-h-96 overflow-auto').style(
                        'background: var(--bg-tertiary); direction: rtl; text-align: right; line-height: 2;'
                    ):
                        ui.label(full_text).classes('whitespace-pre-wrap')

            # Actions Section
            with ui.card().classes('w-full p-6'):
                with ui.row().classes('gap-3 flex-wrap'):
                    # Browse button
                    if sys_id:
                        fl_id = None
                        if 'raw_header' in result and state.meta_mgr:
                            try:
                                parsed = state.meta_mgr.parse_full_id_components(result['raw_header'])
                                fl_id = parsed.get('fl_id')
                            except Exception:
                                pass

                        browse_url = f'/browse?sys_id={sys_id}'
                        if fl_id:
                            browse_url += f'&fl_id={fl_id}'

                        ui.button(
                            tr('View in Browse'),
                            icon='menu_book',
                            on_click=lambda: (dialog.close(), ui.navigate.to(browse_url))
                        ).classes('btn-primary')

                    # Copy text button
                    text_to_copy = full_text or snippet.replace('*', '')
                    ui.button(
                        tr('Copy Text'),
                        icon='content_copy',
                        on_click=lambda t=text_to_copy: copy_result_text(t)
                    ).props('outline')

                    # Find Parallels button
                    text_for_parallels = full_text or snippet.replace('*', '')
                    ui.button(
                        tr('Find Parallels'),
                        icon='compare_arrows',
                        on_click=lambda: (dialog.close(), ui.navigate.to(f'/parallels?text={quote(text_for_parallels[:2000])}'))
                    ).props('outline')

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
