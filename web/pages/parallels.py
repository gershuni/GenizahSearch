from nicegui import ui, run
from web.state import state
from web.translations import tr
import time
import re

def create_parallels_page():

    # State
    class ParallelsState:
        def __init__(self):
            self.is_running = False
            self.progress = 0
            self.status = ""
            self.results = []

    p_state = ParallelsState()

    # Layout
    with ui.column().classes('w-full h-[calc(100vh-60px)] gap-0'):

        # Title and explanation
        with ui.card().classes('w-full p-4 mb-2 bg-blue-50 border-l-4 border-blue-500'):
            ui.label(tr('Composition Search')).classes('text-xl font-bold text-blue-900 mb-2')
            ui.label(tr('Find parallel texts in the Genizah corpus')).classes('text-sm text-blue-700 mb-2')
            with ui.expansion(tr('How does it work?'), icon='help').classes('bg-blue-100'):
                ui.markdown('''
                **חיפוש מקבילות** מחפש קטעי טקסט דומים בגניזה:

                1. **Chunk Size** - כמה מילים לחפש בכל פעם (מומלץ: 3-6)
                2. **Mode** - רמת הדיוק:
                   - **variants** - מוצא וריאציות כתיב (מומלץ)
                   - **exact** - חיפוש מדויק
                   - **fuzzy** - חיפוש מטושטש
                3. המערכת מחפשת את הקטעים בכל התעתיקים ומדרגת לפי התאמה
                ''')

        # 1. Input Area (Top)
        with ui.row().classes('w-full bg-white p-4 shadow-sm items-start gap-4 z-10'):

            # Text Input
            text_input = ui.textarea(label=tr('Source Text'), placeholder=tr('Paste text here...')).classes('flex-grow h-48').props('outlined rounded')

            # Controls
            with ui.column().classes('gap-2 w-64'):
                mode_select = ui.select(
                    ['variants', 'exact', 'fuzzy'],
                    value='variants',
                    label=tr('Mode')
                ).props('outlined dense')

                chunk_size = ui.number(label=tr('Chunk Size'), value=4, min=2, max=20).props('outlined dense')

                run_btn = ui.button(tr('Find Parallels'), on_click=lambda: execute_parallels()).classes('bg-primary text-white w-full')

                status_label = ui.label(tr('Ready')).classes('text-xs text-gray-500')
                progress_bar = ui.linear_progress(0).classes('w-full').props('stripe')

        # 2. Results Area (Bottom)
        with ui.scroll_area().classes('w-full flex-grow bg-gray-50 p-4'):
            results_container = ui.column().classes('w-full gap-4 max-w-5xl mx-auto')
            with results_container:
                ui.label(tr("Results will appear here.")).classes('text-gray-400 text-center w-full mt-10')

    # Logic
    def update_ui():
        if p_state.is_running:
            run_btn.disable()
            status_label.text = p_state.status
            progress_bar.value = p_state.progress
        else:
            run_btn.enable()
            status_label.text = tr("Done") if p_state.progress == 1 else tr("Ready")
            progress_bar.value = p_state.progress

    ui.timer(0.1, update_ui)

    async def execute_parallels():
        text = text_input.value
        if not text: return

        if not state.lab_engine:
            ui.notify(tr("Lab Engine not initialized."), type='negative')
            return

        results_container.clear()
        p_state.is_running = True
        p_state.progress = 0
        p_state.status = tr("Starting...")

        def progress_cb(current, total):
            if total > 0:
                p_state.progress = current / total
                p_state.status = f"{current} / {total}"

        def run_search():
            try:
                print(f"[DEBUG Parallels] Starting search with text length={len(text)}, mode={mode_select.value}, chunk_size={chunk_size.value}")
                # Use Lab Engine for composition
                result = state.lab_engine.lab_composition_search(
                    text,
                    mode=mode_select.value,
                    chunk_size=int(chunk_size.value),
                    progress_callback=progress_cb
                )
                print(f"[DEBUG Parallels] Search completed, result type={type(result)}, is None={result is None}")
                if result:
                    print(f"[DEBUG Parallels] Result keys={list(result.keys()) if isinstance(result, dict) else 'not a dict'}")
                return result
            except Exception as e:
                print(f"[ERROR Parallels] Exception: {e}")
                import traceback
                traceback.print_exc()
                return None

        result_data = await run.io_bound(run_search)

        p_state.is_running = False
        p_state.progress = 1.0

        print(f"[DEBUG Parallels] After io_bound: result_data={result_data is not None}")

        if result_data:
            main_results = result_data.get('main', [])
            print(f"[DEBUG Parallels] main_results length={len(main_results) if main_results else 0}")
            if main_results:
                render_results(main_results)
            else:
                with results_container:
                    ui.label(tr("No matches found. Try different parameters.")).classes('text-center text-gray-500 w-full mt-4')
        else:
            with results_container:
                ui.label(tr("No parallels found. Check console for errors.")).classes('text-center text-red-500 w-full mt-4')

    def render_results(items):
        results_container.clear()
        if not items:
            with results_container:
                ui.label(tr("No matches found.")).classes('text-center text-gray-500 w-full')
            return

        with results_container:
            # Summary
            ui.label(f"{len(items)} {tr('matches')} {tr('found')}").classes('text-lg font-semibold mb-4')

            for idx, item in enumerate(items):
                score = int(item.get('score', 0))

                # Extract metadata from raw_header
                raw_header = item.get('raw_header', '')
                shelfmark = item.get('display', {}).get('shelfmark', 'Unknown')
                title = item.get('display', {}).get('title', '')
                sys_id = item.get('display', {}).get('id', '')

                # If display not present, try to parse from raw_header
                if shelfmark == 'Unknown' and state.meta_mgr:
                    # Try to extract sys_id from raw_header
                    import re as regex
                    match = regex.search(r'IE(\d+)', raw_header)
                    if match:
                        temp_id = match.group(1)
                        shelf_temp, title_temp = state.meta_mgr.get_meta_for_id(temp_id)
                        if shelf_temp:
                            shelfmark = shelf_temp
                            title = title_temp
                            sys_id = temp_id

                # Format snippet
                ms_text = item.get('text', '').replace('\n', '<br>')
                ms_text = re.sub(r'\*(.*?)\*', r'<span class="bg-yellow-200 font-bold px-1">\1</span>', ms_text)

                src_text = item.get('source_ctx', '').replace('\n', '<br>')
                src_text = re.sub(r'\*(.*?)\*', r'<span class="bg-green-200 font-bold px-1">\1</span>', src_text)

                with ui.card().classes('w-full p-4 border-l-4 border-primary hover:shadow-lg transition-shadow'):
                    # Header with metadata
                    with ui.row().classes('w-full justify-between items-start mb-3'):
                        with ui.column().classes('gap-1'):
                            ui.label(f"#{idx+1}: {shelfmark}").classes('font-bold text-lg text-primary')
                            if title:
                                ui.label(title).classes('text-sm text-gray-600')
                        with ui.row().classes('gap-2 items-center'):
                            ui.badge(f"{tr('Score')}: {score}", color='green')

                    # Content
                    with ui.row().classes('w-full gap-4 mt-2'):
                        # Source Context
                        with ui.column().classes('flex-1 bg-green-50 p-3 rounded border border-green-200'):
                            ui.label(tr("Source Context")).classes('text-xs font-bold text-green-700 uppercase mb-2')
                            ui.html(f"<div dir='rtl' class='text-sm leading-relaxed'>{src_text}</div>", sanitize=False)

                        # Manuscript Match
                        with ui.column().classes('flex-1 bg-yellow-50 p-3 rounded border border-yellow-200'):
                            ui.label(tr("Manuscript Match")).classes('text-xs font-bold text-yellow-700 uppercase mb-2')
                            ui.html(f"<div dir='rtl' class='text-sm leading-relaxed'>{ms_text}</div>", sanitize=False)

                    # Action buttons
                    with ui.row().classes('w-full gap-2 mt-3 pt-3 border-t'):
                        if sys_id:
                            ui.button(
                                icon='menu_book',
                                on_click=lambda sid=sys_id: ui.navigate.to(f'/browse?sys_id={sid}')
                            ).props('flat dense size=sm').classes('text-primary').tooltip(tr('Browse'))

                        ui.button(
                            icon='star_border',
                            on_click=lambda item=item: add_parallel_to_list(item, shelfmark, title, sys_id)
                        ).props('flat dense size=sm').classes('text-yellow-600').tooltip(tr('Add to Favorites'))

                        ui.button(
                            icon='table_view',
                            on_click=lambda: ui.notify(tr('Export functionality coming soon'))
                        ).props('flat dense size=sm').tooltip(tr('Export'))

    def add_parallel_to_list(item, shelfmark, title, sys_id):
        """Add a parallel result to a list."""
        if not sys_id:
            ui.notify(tr('Cannot add: missing system ID'), type='warning')
            return

        with ui.dialog() as dialog, ui.card().classes('p-4'):
            ui.label(tr('Add to List')).classes('text-lg font-bold mb-2')
            ui.label(f"{tr('Item')}: {shelfmark}").classes('text-sm text-gray-600 mb-4')

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
