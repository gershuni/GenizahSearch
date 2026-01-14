from nicegui import ui, run
from web.state import state
from web.translations import tr
import time

# Helper to format snippet HTML
def format_snippet(text):
    if not text: return ""
    # Convert *...* to yellow highlight
    html = text.replace("*", '<span class="bg-yellow-200 font-bold px-1 rounded">').replace("*", "</span>") # This replacement logic is flawed, handled below

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

    search_state = SearchUIState()

    # --- UI Layout ---
    with ui.column().classes('w-full h-[calc(100vh-60px)] gap-0'):

        # 1. Search Bar (Top Fixed)
        with ui.row().classes('w-full bg-white p-4 shadow-sm items-end gap-4 z-10'):

            # Query Input
            query_input = ui.input(label=tr('Search Query')).classes('flex-grow').props('outlined dense rounded')
            query_input.on('keydown.enter', lambda: execute_search())

            # Mode Select
            mode_select = ui.select(
                ['variants', 'exact', 'fuzzy', 'Regex'],
                value='variants',
                label=tr('Mode')
            ).classes('w-40').props('outlined dense')

            # Gap
            gap_input = ui.number(label=tr('Gap'), value=0).classes('w-20').props('outlined dense')

            # Search Button
            search_btn = ui.button(tr('Search'), on_click=lambda: execute_search()).classes('bg-primary text-white h-10 px-6')

            # Lab Toggle (Simple for now)
            lab_mode = ui.checkbox('Lab').tooltip(tr("Enable Lab Mode algorithms"))

            ui.space()

            # Export Buttons
            with ui.row().classes('gap-1'):
                ui.button(icon='description', on_click=lambda: ui.download('/api/export/word')).props('flat round dense').tooltip(tr('Export Word'))
                ui.button(icon='table_view', on_click=lambda: ui.download('/api/export/excel')).props('flat round dense').tooltip(tr('Export Excel'))

        # 2. Progress Bar (Thin)
        progress_bar = ui.linear_progress(0).props('stripe animate').classes('h-1 w-full opacity-0 transition-opacity duration-300')
        status_label = ui.label('').classes('text-xs text-gray-500 q-px-4')

        # 3. Main Splitter Area
        with ui.splitter(value=30).classes('w-full flex-grow border-t') as splitter:

            # --- LEFT: Result List ---
            with splitter.before:
                results_container = ui.scroll_area().classes('w-full h-full bg-gray-50 p-2 gap-2')
                with results_container:
                    ui.label(tr("Ready to search.")).classes('text-gray-400 text-center mt-10 w-full')

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
        query = query_input.value
        if not query: return

        if not state.is_ready():
            ui.notify(tr("Engine not ready."), type='warning')
            return

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
                    # Lab Search
                    return state.lab_engine.lab_search(
                        query,
                        mode='variants', # Lab uses its own logic
                        gap=int(gap_input.value),
                        progress_callback=progress_cb
                    )
                else:
                    # Standard Search
                    return state.searcher.execute_search(
                        query,
                        mode=mode_select.value,
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

        # Render Results (Virtual scroll is harder in NiceGUI, we'll use lazy rendering if needed,
        # but for now standard rendering. Limit to 100 for DOM performance)
        render_results(results[:100])
        if len(results) > 100:
            ui.notify(tr("Showing first 100 results. Refine search."), type='info')

    def render_results(results):
        results_container.clear()

        if not results:
            with results_container:
                ui.label(tr("No results found.")).classes('w-full text-center text-gray-500 mt-4')
            return

        with results_container:
            for i, res in enumerate(results):
                display = res.get('display', {})
                shelf = display.get('shelfmark', 'Unknown')

                # Card
                with ui.card().classes('w-full cursor-pointer hover:bg-green-50 transition-colors p-3 gap-1').on('click', lambda _, r=res: load_in_viewer(r)):

                    # Header: Shelfmark + Title
                    with ui.row().classes('w-full justify-between'):
                        ui.label(shelf).classes('font-bold text-primary text-sm')
                        ui.label(f"#{i+1}").classes('text-xs text-gray-400')

                    # Title
                    if display.get('title'):
                        ui.label(display['title']).classes('text-xs text-gray-600 truncate w-full')

                    # Snippet
                    snippet_html = format_snippet(str(res.get('snippet', '')))
                    ui.html(f"<div dir='rtl' class='text-xs leading-relaxed text-gray-800 line-clamp-3'>{snippet_html}</div>")

    # --- Viewer Integration (Step 6 Hook) ---
    def load_in_viewer(result):
        # This will be implemented in Step 6, but we can hook it up now
        from web.pages import viewer
        if hasattr(viewer, 'load_result'):
            viewer.load_result(viewer_container, result)
        else:
            viewer_container.clear()
            with viewer_container:
                ui.label("Viewer not implemented yet.").classes('text-red-500')
