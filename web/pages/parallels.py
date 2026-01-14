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

        # 1. Input Area (Top)
        with ui.row().classes('w-full bg-white p-4 shadow-sm items-start gap-4 z-10 h-1/3'):

            # Text Input
            text_input = ui.textarea(label=tr('Source Text'), placeholder=tr('Paste text here...')).classes('flex-grow h-full').props('outlined rounded')

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
                progress_bar = ui.linear_progress(0).classes('w-full')

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
            for item in items:
                score = int(item.get('score', 0))
                # Format snippet
                ms_text = item.get('text', '').replace('\n', '<br>')
                ms_text = re.sub(r'\*(.*?)\*', r'<span class="bg-yellow-200 font-bold">\1</span>', ms_text)

                src_text = item.get('source_ctx', '').replace('\n', '<br>')
                src_text = re.sub(r'\*(.*?)\*', r'<span class="bg-green-100 font-bold">\1</span>', src_text)

                with ui.card().classes('w-full p-4 border-l-4 border-primary'):
                    with ui.row().classes('w-full justify-between items-start'):
                        ui.label(item.get('raw_header', 'Unknown')).classes('font-bold text-lg')
                        ui.badge(f"Score: {score}", color='green')

                    with ui.row().classes('w-full gap-4 mt-2'):
                        # Source Context
                        with ui.column().classes('flex-1 bg-gray-50 p-2 rounded'):
                            ui.label(tr("Source Context")).classes('text-xs font-bold text-gray-500 uppercase')
                            ui.html(f"<div dir='rtl' class='text-sm'>{src_text}</div>", sanitize=False)

                        # Manuscript Match
                        with ui.column().classes('flex-1 bg-white border p-2 rounded'):
                            ui.label(tr("Manuscript Match")).classes('text-xs font-bold text-gray-500 uppercase')
                            ui.html(f"<div dir='rtl' class='text-sm'>{ms_text}</div>", sanitize=False)
