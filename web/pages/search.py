from nicegui import ui, run, app
from web.state import state
from web.translations import tr
import time

# Helper to format snippet HTML
def format_snippet(text):
    if not text: return ""
    # Robust replacement: *word* -> <span>word</span>
    import re
    return re.sub(r'\*(.*?)\*', r'<span class="bg-yellow-200 text-black font-bold px-1 rounded">\1</span>', text)

def create_search_page():

    # --- State Management for Search ---
    class SearchUIState:
        def __init__(self):
            self.progress = 0.0
            self.status = ""
            self.is_running = False
            self.results = []
            self.adv_expanded = False

    search_state = SearchUIState()

    # Restore previous search results if available
    if 'search_results' in app.storage.user:
        try:
            search_state.results = app.storage.user.get('search_results', [])
            print(f"[DEBUG Search] Restored {len(search_state.results)} results from storage")
        except Exception as e:
            print(f"[DEBUG Search] Failed to restore results: {e}")

    # --- UI Layout ---
    with ui.column().classes('w-full h-[calc(100vh-60px)] gap-0'):

        # 1. Search Bar (Top Fixed)
        with ui.column().classes('w-full bg-white p-4 shadow-sm z-10 gap-2'):
            with ui.row().classes('w-full items-end gap-4'):

                # Query Input
                query_input = ui.input(label=tr('Search Query')).classes('flex-grow').props('outlined dense rounded')
                query_input.on('keydown.enter', lambda: execute_search())

                # Mode Select
                mode_select = ui.select(
                    ['variants', 'variants_extended', 'variants_maximum', 'exact', 'fuzzy', 'Regex', 'Shelfmark', 'Title'],
                    value='variants',
                    label=tr('Mode')
                ).classes('w-40').props('outlined dense')

                # Gap
                gap_input = ui.number(label=tr('Gap'), value=0).classes('w-20').props('outlined dense')

                # Search Button
                search_btn = ui.button(tr('Search'), on_click=lambda: execute_search()).classes('bg-primary text-white h-10 px-6')

                # Lab Toggle
                lab_mode = ui.checkbox('Lab').tooltip(tr("Enable Lab Mode algorithms"))

                ui.space()

                # Export Buttons
                with ui.row().classes('gap-1'):
                    ui.button(icon='description', on_click=lambda: ui.download('/api/export/word')).props('flat round dense').tooltip(tr('Export Word'))
                    ui.button(icon='table_view', on_click=lambda: ui.download('/api/export/excel')).props('flat round dense').tooltip(tr('Export Excel'))

            # Advanced Search Expansion
            with ui.expansion(tr('Advanced Filters'), icon='filter_list').classes('w-full bg-gray-50 rounded text-sm'):
                with ui.row().classes('w-full gap-4 p-4'):
                    # NOT Filter
                    # Since Tantivy handles NOT via query syntax, we can append it or handle it in GUI
                    # For now, let's provide helper buttons to append to query
                    def append_syntax(text):
                        current = query_input.value or ""
                        query_input.set_value(current + " " + text)

                    with ui.column().classes('gap-2'):
                        ui.label(tr('Boolean Operators')).classes('font-bold text-gray-600')
                        with ui.row():
                            ui.button('AND', on_click=lambda: append_syntax('AND')).props('outline dense size=sm')
                            ui.button('OR', on_click=lambda: append_syntax('OR')).props('outline dense size=sm')
                            ui.button('NOT', on_click=lambda: append_syntax('NOT')).props('outline dense size=sm')

                    with ui.column().classes('gap-2'):
                        ui.label(tr('Shortcuts')).classes('font-bold text-gray-600')
                        with ui.row():
                            ui.button('=Exact', on_click=lambda: append_syntax('=')).props('flat dense size=sm').tooltip('=term')
                            ui.button('?Variants', on_click=lambda: append_syntax('?')).props('flat dense size=sm').tooltip('?term')
                            ui.button('#Shelf', on_click=lambda: append_syntax('#')).props('flat dense size=sm').tooltip('#shelfmark')

        # 2. Progress Bar (Thin)
        progress_bar = ui.linear_progress(0).props('stripe animate').classes('h-1 w-full opacity-0 transition-opacity duration-300')
        status_label = ui.label('').classes('text-xs text-gray-500 q-px-4')

        # 3. Main Splitter Area
        with ui.splitter(value=30).classes('w-full flex-grow border-t') as splitter:

            # --- LEFT: Result List ---
            with splitter.before:
                results_container = ui.column().classes('w-full h-full')
                # Will be populated by render_results

            # --- RIGHT: Viewer (Placeholder for now) ---
            with splitter.after:
                viewer_container = ui.column().classes('w-full h-full p-4 items-center justify-center')
                with viewer_container:
                    ui.icon('menu_book').classes('text-6xl text-gray-200')
                    ui.label(tr("Select a result to view")).classes('text-gray-400')

    # --- Search Logic ---

    def update_progress_ui():
        """Timer callback to update progress bar from thread state."""
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
        query = query_input.value.strip()
        if not query: return

        if not state.is_ready():
            ui.notify(tr("Engine not ready."), type='warning')
            return

        # Parse Syntax Shortcuts
        # =, ?, ??, ???, /, #, $
        mode = mode_select.value
        clean_query = query

        if query.startswith("???"):
            mode = "variants_maximum"
            clean_query = query[3:]
        elif query.startswith("??"):
            mode = "variants_extended"
            clean_query = query[2:]
        elif query.startswith("?"):
            mode = "variants"
            clean_query = query[1:]
        elif query.startswith("="):
            mode = "exact"
            clean_query = query[1:]
        elif query.startswith("/"):
            mode = "Regex"
            clean_query = query[1:]
        elif query.startswith("#"):
            mode = "Shelfmark"
            clean_query = query[1:]
        elif query.startswith("$"):
            mode = "Title"
            clean_query = query[1:]

        # Update UI to reflect auto-detected mode
        mode_select.value = mode

        # Reset UI
        results_container.clear()
        search_state.is_running = True
        search_state.progress = 0
        search_state.status = tr("Starting...")
        search_state.results = []
        search_btn.disable()

        # Define callback for the engine (runs in thread)
        def progress_cb(current, total):
            if total > 0:
                search_state.progress = current / total
                search_state.status = f"{current} / {total}"

        # Wrapper to run in thread
        def run_core_search():
            try:
                if lab_mode.value:
                    # Lab Search (Lab engine handles its own fuzzy logic, usually variants)
                    # We pass 'variants' unless it's regex
                    lab_search_mode = 'variants'
                    if mode == 'Regex': lab_search_mode = 'Regex'

                    return state.lab_engine.lab_search(
                        clean_query,
                        mode=lab_search_mode,
                        gap=int(gap_input.value),
                        progress_callback=progress_cb
                    )
                else:
                    # Standard Search
                    return state.searcher.execute_search(
                        clean_query,
                        mode=mode,
                        gap=int(gap_input.value),
                        progress_callback=progress_cb
                    )
            except Exception as e:
                print(f"Search Error: {e}")
                return []

        # Run
        results = await run.io_bound(run_core_search)

        # Save to state for export
        state.last_results = results

        # Post-process
        search_state.is_running = False
        search_state.progress = 1.0
        search_state.results = results
        search_btn.enable()

        # Save results to persistent storage
        try:
            app.storage.user['search_results'] = results
            print(f"[DEBUG Search] Saved {len(results)} results to storage")
        except Exception as e:
            print(f"[DEBUG Search] Failed to save results: {e}")

        # Render Results (Virtual scroll is harder in NiceGUI, we'll use lazy rendering if needed,
        # but for now standard rendering. Limit to 100 for DOM performance)
        render_results(results[:100])
        if len(results) > 100:
            ui.notify(tr("Showing first 100 results. Refine search."), type='info')

    def create_result_card(i, res, display, shelf):
        """Create a single result card with proper closure capture."""
        # Card
        with ui.card().classes('w-full hover:bg-green-50 transition-colors p-3 gap-1'):

            # Header: Shelfmark + Actions
            with ui.row().classes('w-full justify-between items-start'):
                with ui.column().classes('flex-grow cursor-pointer').on('click', lambda: load_in_viewer(res)):
                    ui.label(shelf).classes('font-bold text-primary text-sm')
                with ui.row().classes('gap-1 items-center'):
                    # Add to list button
                    ui.button(
                        icon='star_border',
                        on_click=lambda: show_add_to_list_dialog(res)
                    ).props('flat round dense size=sm').classes('text-yellow-600').tooltip(tr('Add to list'))
                    ui.label(f"#{i+1}").classes('text-xs text-gray-400')

            # Title
            if display.get('title'):
                with ui.column().classes('w-full cursor-pointer').on('click', lambda: load_in_viewer(res)):
                    ui.label(display['title']).classes('text-xs text-gray-600 w-full')

            # Snippet
            with ui.column().classes('w-full cursor-pointer').on('click', lambda: load_in_viewer(res)):
                snippet_html = format_snippet(str(res.get('snippet', '')))
                ui.html(f"<div dir='rtl' class='text-xs leading-relaxed text-gray-800'>{snippet_html}</div>", sanitize=False)

    def render_results(results):
        results_container.clear()

        if not results:
            with results_container:
                with ui.column().classes('w-full h-full items-center justify-center bg-gray-50'):
                    ui.icon('search', size='4rem').classes('text-gray-300')
                    ui.label(tr("Ready to search.")).classes('text-gray-400 mt-4')
            return

        with results_container:
            # Create scroll area for results
            scroll = ui.scroll_area().classes('w-full h-full bg-gray-50 p-2')
            with scroll:
                # Use a column to ensure vertical stacking within the scroll area
                with ui.column().classes('w-full gap-2'):
                    for i, res in enumerate(results):
                        display = res.get('display', {})
                        shelf = display.get('shelfmark', 'Unknown')

                        # Create card with proper closure capture
                        create_result_card(i, res, display, shelf)

    # --- Viewer Integration ---
    def load_in_viewer(result):
        """Load result in the right panel viewer."""
        from web.pages import viewer
        if hasattr(viewer, 'load_result'):
            viewer.load_result(viewer_container, result)
        else:
            # Fallback: basic viewer
            viewer_container.clear()
            with viewer_container:
                display = result.get('display', {})
                ui.label(display.get('shelfmark', 'Unknown')).classes('text-xl font-bold text-primary mb-2')
                if display.get('title'):
                    ui.label(display['title']).classes('text-sm text-gray-600 mb-4')

                # Show snippet
                snippet_html = format_snippet(str(result.get('snippet', '')))
                ui.html(f"<div dir='rtl' class='text-sm leading-relaxed'>{snippet_html}</div>", sanitize=False).classes('w-full')

                # Browse button
                sys_id = display.get('id')
                if sys_id:
                    ui.button(
                        tr('Browse Full Manuscript'),
                        icon='menu_book',
                        on_click=lambda: ui.navigate.to(f'/browse?sys_id={sys_id}')
                    ).classes('mt-4 bg-primary text-white')

    # --- Add to List Dialog ---
    def show_add_to_list_dialog(result):
        """Show dialog to add result to a personal list."""
        display = result.get('display', {})
        sys_id = display.get('id')
        shelfmark = display.get('shelfmark', 'Unknown')

        if not sys_id:
            ui.notify(tr('Cannot add: missing system ID'), type='warning')
            return

        with ui.dialog() as dialog, ui.card().classes('p-4'):
            ui.label(tr('Add to List')).classes('text-lg font-bold mb-2')
            ui.label(f"{tr('Item')}: {shelfmark}").classes('text-sm text-gray-600 mb-4')

            # Get available lists
            if state.lists_mgr:
                lists = state.lists_mgr.data.get('lists', {})
                list_options = {lst_id: lst['name'] for lst_id, lst in lists.items() if not lst.get('is_system')}

                if not list_options:
                    ui.label(tr('No lists available. Create a list first.')).classes('text-gray-500 mb-2')
                    ui.button(tr('Go to Lists'), on_click=lambda: ui.navigate.to('/lists')).classes('bg-primary text-white')
                else:
                    selected_list = ui.select(
                        list_options,
                        label=tr('Select List'),
                        value=list(list_options.keys())[0]
                    ).classes('w-full mb-4')

                    note_input = ui.input(label=tr('Note (optional)')).classes('w-full mb-4')

                    def add_to_list():
                        if state.lists_mgr.add_item(sys_id, selected_list.value, note=note_input.value):
                            ui.notify(tr('Added to list'), type='positive')
                            dialog.close()
                        else:
                            ui.notify(tr('Already in list'), type='info')

                    with ui.row().classes('w-full justify-end gap-2'):
                        ui.button(tr('Cancel'), on_click=dialog.close).props('flat')
                        ui.button(tr('Add'), on_click=add_to_list).classes('bg-primary text-white')
            else:
                ui.label(tr('Lists manager not available')).classes('text-red-500')

        dialog.open()

    # Initialize with restored results (if any)
    render_results(search_state.results[:100] if search_state.results else [])
