from nicegui import ui
from web.state import state
from web.translations import tr

def create_page():
    with ui.column().classes('w-full max-w-6xl mx-auto gap-6'):

        # --- Hero / Welcome ---
        with ui.row().classes('w-full items-center justify-between'):
            with ui.column().classes('gap-1'):
                ui.label(tr("Welcome back, Researcher.")).classes('text-3xl font-bold text-gray-800')
                ui.label(tr("Cairo Genizah Search Engine v0.8")).classes('text-gray-500')

            # Index Status Badge
            with ui.row().classes('items-center gap-2 bg-white px-4 py-2 rounded-full shadow-sm'):
                def update_index_status():
                    if state.searcher and state.searcher.index:
                        status_light.classes('text-green-500', remove='text-red-500')
                        status_text.text = tr("Index Active")
                    else:
                        status_light.classes('text-red-500', remove='text-green-500')
                        status_text.text = tr("Index Offline")

                status_light = ui.icon('circle').classes('text-xs text-gray-300')
                status_text = ui.label(tr("Checking...")).classes('text-sm font-medium')
                ui.timer(1.0, update_index_status)

        # --- Stats Grid ---
        with ui.grid(columns=4).classes('w-full gap-4'):

            def stat_card(label, value_fn, icon, color):
                with ui.card().classes('gap-1 p-4 shadow-sm hover:shadow-md transition-shadow'):
                    with ui.row().classes('w-full justify-between items-start'):
                        ui.label(label).classes('text-gray-500 text-sm font-medium')
                        ui.icon(icon).classes(f'text-{color}-500 text-xl')

                    val_label = ui.label('...').classes('text-2xl font-bold text-gray-800')

                    def refresh():
                        if state.is_ready():
                            val = value_fn()
                            val_label.text = str(val)

                    ui.timer(2.0, refresh)

            # 1. Total Pages
            def get_total_docs():
                if state.searcher and state.searcher.searcher:
                    return state.searcher.searcher.num_docs
                return 0
            stat_card(tr("Indexed Pages"), get_total_docs, "library_books", "blue")

            # 2. Manuscripts (Approx based on NLI Cache)
            def get_cached_ms():
                return len(state.meta_mgr.nli_cache) if state.meta_mgr else 0
            stat_card(tr("Cached Metadata"), get_cached_ms, "storage", "purple")

            # 3. Lists
            def get_list_count():
                return len(state.lists_mgr.get_all_lists()) if state.lists_mgr else 0
            stat_card(tr("Personal Lists"), get_list_count, "format_list_bulleted", "orange")

            # 4. Lab Index
            def get_lab_status():
                if state.lab_engine:
                    return "Ready" if not state.lab_engine.lab_index_needs_rebuild else "Rebuild Needed"
                return "..."
            stat_card(tr("Lab Index"), get_lab_status, "science", "green")

        # --- Quick Actions ---
        ui.label(tr("Quick Actions")).classes('text-lg font-bold text-gray-700 mt-4')
        with ui.row().classes('w-full gap-4'):

            def action_card(title, desc, icon, target, color="blue"):
                with ui.card().classes('w-64 p-0 cursor-pointer hover:scale-105 transition-transform').on('click', lambda: ui.navigate.to(target)):
                    with ui.row().classes(f'w-full bg-{color}-50 p-4 items-center gap-3 border-b border-{color}-100'):
                        ui.icon(icon).classes(f'text-{color}-600 text-2xl')
                        ui.label(title).classes(f'text-{color}-800 font-bold')
                    with ui.column().classes('p-4'):
                        ui.label(desc).classes('text-sm text-gray-500 leading-tight')

            action_card(tr("New Search"), tr("Start a new text search with variants"), "search", "/search", "blue")
            action_card(tr("Browse"), tr("Find manuscript by shelfmark"), "menu_book", "/browse", "amber")
            action_card(tr("Lab Mode"), tr("Advanced experimental search tools"), "science", "/search?mode=lab", "green") # We can handle query params later
            action_card(tr("Settings"), tr("Configure AI and Indexing"), "settings", "/settings", "gray")

        # --- Recent Activity (Placeholder) ---
        # We could pull this from ListsManager.recent_items if we wanted
