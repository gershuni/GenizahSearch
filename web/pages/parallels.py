# -*- coding: utf-8 -*-
"""
Parallels Search Page - Dicta Genizah Search

Find parallel texts in the Genizah corpus using:
- Shmidman-Koppel-Porat fingerprinting algorithm
- Configurable chunk size
- Advanced filtering options
"""

from nicegui import ui, run, app
from web.state import state
from web.translations import tr
from urllib.parse import unquote
import re
import html
from web.components.typography import h1, h2, h3, h4


def create_parallels_page(initial_text: str = None):
    """Create the parallels (composition) search page."""

    # === State ===
    class ParallelsState:
        def __init__(self):
            self.is_running = False
            self.is_cancelled = False
            self.progress = 0
            self.status = ""
            self.results = []
            self.finished_animation_shown = False

    p_state = ParallelsState()

    # Restore previous results
    if 'parallels_results' in app.storage.user:
        try:
            p_state.results = app.storage.user.get('parallels_results', [])
        except Exception:
            pass

    # Decode initial text from URL
    decoded_text = ""
    if initial_text:
        try:
            decoded_text = unquote(initial_text)
        except Exception:
            decoded_text = initial_text

    # === UI Layout ===
    with ui.column().classes('w-full max-w-7xl mx-auto gap-6 fade-in'):

        # === Page Header ===
        with ui.row().classes('w-full items-center justify-between'):
            with ui.column().classes('gap-1'):
                # Changed to H1
                h1(tr('Find Parallels'), classes='text-3xl font-bold', style='color: var(--text-primary);')
                ui.label(tr('Discover parallel texts in the Genizah corpus')).style('color: var(--text-secondary);')

        # === Input Section ===
        with ui.card().classes('w-full p-6'):
            with ui.row().classes('w-full gap-6'):

                # Left: Text Input
                with ui.column().classes('flex-grow gap-4'):
                    # Changed to H2
                    h2(tr('Source text'), classes='text-xl font-bold', style='color: var(--text-primary);')

                    text_input = ui.textarea(
                        placeholder=tr('Paste your Hebrew text here...'),
                        value=decoded_text
                    ).classes('w-full').props('outlined rows=8').style('direction: rtl;')

                    # Word count
                    word_count_label = ui.label('0 ' + tr('Words')).classes('text-sm').style('color: var(--text-muted);')

                    def update_word_count():
                        text = text_input.value or ""
                        words = len([w for w in text.split() if w])
                        word_count_label.text = f"{words} {tr('Words')}"

                    text_input.on('input', update_word_count)
                    # Update immediately if we have initial text
                    if decoded_text:
                        update_word_count()

                # Right: Options Panel
                with ui.column().classes('w-80 gap-4'):
                    # Changed to H2
                    h2(tr('Options'), classes='text-xl font-bold', style='color: var(--text-primary);')

                    # Mode
                    mode_select = ui.select(
                        {
                            'exact': tr('Exact'),
                            'variants': tr('Variants'),
                            'fuzzy': tr('Fuzzy'),
                        },
                        value='exact',
                        label=tr('Search Mode')
                    ).classes('w-full').props('outlined dense')

                    # Check if user prefers slider or presets (default: presets)
                    use_slider = False
                    if state.lab_engine and hasattr(state.lab_engine, 'settings') and state.lab_engine.settings:
                        use_slider = getattr(state.lab_engine.settings, 'variant_use_slider', False)

                    # Track current preset level
                    current_preset = {'value': 70}  # Default: extended

                    # Variables for elements
                    btn_basic = btn_extended = btn_maximum = None
                    variant_slider = None

                    # Variant Level Controls (visible only in Variants mode)
                    with ui.column().classes('gap-1') as variant_slider_col:
                        h3(tr('Variant Level'), classes='text-sm font-medium', style='color: var(--text-secondary);')

                        if not use_slider:
                            # Preset buttons (default) - toggle button group style
                            with ui.row().classes('items-center gap-0').style('border: 1px solid var(--border-medium); border-radius: 8px; overflow: hidden;'):
                                btn_basic = ui.button('○ ' + tr('Basic')).classes('px-3 h-9').props('flat no-caps').style('border-radius: 0; min-width: auto;')
                                btn_basic.tooltip(tr('Basic variants (30 pairs)'))
                                ui.element('div').style('width: 1px; height: 24px; background: var(--border-medium);')
                                btn_extended = ui.button('◐ ' + tr('Extended')).classes('px-3 h-9').props('flat no-caps').style('border-radius: 0; min-width: auto;')
                                btn_extended.tooltip(tr('Extended variants (70 pairs)'))
                                ui.element('div').style('width: 1px; height: 24px; background: var(--border-medium);')
                                btn_maximum = ui.button('● ' + tr('Maximum')).classes('px-3 h-9').props('flat no-caps').style('border-radius: 0; min-width: auto;')
                                btn_maximum.tooltip(tr('Maximum variants (150 pairs) - slower'))
                        else:
                            # Slider (alternative mode)
                            with ui.row().classes('items-center gap-2'):
                                variant_slider = ui.slider(min=10, max=300, value=70, step=10).classes('w-full').props('label-always')

                        with ui.row().classes('items-center gap-2 mt-2'):
                            ui.label(tr('Max changes:')).classes('text-xs').style('color: var(--text-muted);')
                            max_changes_select = ui.select({1: '×1', 2: '×2', 3: '×3'}, value=2).classes('w-20').props('outlined dense')

                    def update_preset_buttons():
                        """Update preset button styles based on current selection."""
                        if not btn_basic:
                            return
                        val = current_preset['value']
                        # Reset all buttons to default state
                        for btn in [btn_basic, btn_extended, btn_maximum]:
                            btn.style('background: transparent; color: var(--text-secondary);')
                        # Highlight the active button
                        if val == 30:
                            btn_basic.style('background: var(--primary-600); color: white;')
                        elif val == 70:
                            btn_extended.style('background: var(--primary-600); color: white;')
                        elif val == 150:
                            btn_maximum.style('background: var(--primary-600); color: white;')

                    def set_preset(pairs_count):
                        """Set variant level from preset button."""
                        current_preset['value'] = pairs_count
                        if state.var_mgr:
                            state.var_mgr.set_variant_level(pairs_count)
                        update_preset_buttons()

                    if btn_basic:
                        btn_basic.on('click', lambda: set_preset(30))
                        btn_extended.on('click', lambda: set_preset(70))
                        btn_maximum.on('click', lambda: set_preset(150))
                        # Initialize button styles
                        update_preset_buttons()

                    def on_mode_change():
                        is_variants = mode_select.value == 'variants'
                        variant_slider_col.set_visibility(is_variants)

                    mode_select.on('update:model-value', on_mode_change)

                    # Chunk Size
                    with ui.column().classes('gap-1'):
                        # Changed to H3
                        h3(tr('Chunk size'), classes='text-sm font-medium', style='color: var(--text-secondary);')
                        chunk_size = ui.slider(min=2, max=12, value=5).props('label-always')
                        ui.label(tr('Words per search chunk (recommended: 4-7)')).classes('text-xs').style('color: var(--text-muted);')

                    # Deep Scan
                    deep_scan = ui.checkbox(tr('Deep Scan')).classes('mt-2')

                    ui.separator().classes('my-2')

                    # Run Button
                    run_btn = ui.button(
                        tr('Find Parallels'),
                        icon='compare_arrows',
                        on_click=lambda: execute_parallels()
                    ).classes('btn-primary w-full')

                    # Cancel Button (hidden by default)
                    cancel_btn = ui.button(
                        tr('Cancel'),
                        icon='close',
                        on_click=lambda: cancel_search()
                    ).classes('w-full').props('outline color=red').style('display: none;')

                    # Progress
                    progress_bar = ui.linear_progress(0).classes('w-full opacity-0').style('height: 6px;')
                    status_label = ui.label('').classes('text-sm text-center font-medium').style('color: var(--text-secondary);')

        # === Filter Text (Collapsible) ===
        with ui.expansion(tr('Filter text (exclude known sources)'), icon='filter_alt').classes('w-full'):
            with ui.column().classes('w-full p-4 gap-2'):
                ui.label(tr('Matches containing text from this field will be filtered out')).classes('text-sm').style('color: var(--text-muted);')
                filter_input = ui.textarea(
                    placeholder=tr('Paste text to exclude from results...')
                ).classes('w-full').props('outlined rows=3').style('direction: rtl;')

        # === Results Section ===
        with ui.card().classes('w-full p-6'):
            with ui.row().classes('w-full items-center justify-between mb-4'):
                # Changed to H2 (stored in variable but it's a UI element)
                results_header = h2(tr('Results'), classes='text-xl font-bold', style='color: var(--text-primary);')

                with ui.row().classes('gap-2'):
                    # Sort options
                    sort_select = ui.select(
                        {
                            'score': tr('Sort by score'),
                            'shelfmark': tr('Sort by shelfmark'),
                            'matches': tr('Sort by matches'),
                        },
                        value='score'
                    ).props('outlined dense').classes('w-40')

                    ui.button(icon='description', on_click=lambda: ui.download('/api/export/parallels/word')).props(
                        'flat round dense'
                    ).tooltip(tr('Export Word'))
                    ui.button(icon='table_view', on_click=lambda: ui.download('/api/export/parallels/excel')).props(
                        'flat round dense'
                    ).tooltip(tr('Export Excel'))

            results_container = ui.column().classes('w-full gap-4')

    # === Logic ===

    def update_ui():
        if p_state.is_running:
            run_btn.disable()
            cancel_btn.style('display: block;')
            progress_bar.classes(remove='opacity-0')
            progress_bar.value = p_state.progress
            status_label.text = p_state.status
        else:
            run_btn.enable()
            cancel_btn.style('display: none;')
            if p_state.progress >= 1.0 and not p_state.finished_animation_shown:
                progress_bar.value = 1.0
                status_label.text = tr('Done')
                p_state.finished_animation_shown = True
                ui.timer(2.0, lambda: progress_bar.classes(add='opacity-0'), once=True)

    ui.timer(0.1, update_ui)

    def cancel_search():
        p_state.is_cancelled = True
        p_state.status = tr('Cancelling...')

    async def execute_parallels():
        text = text_input.value or ""
        words = len([w for w in text.split() if w])

        # Allow shorter texts (minimum 3 words instead of 10)
        if words < 3:
            ui.notify(tr('Enter at least 3 words'), type='warning')
            return

        if not state.lab_engine:
            ui.notify(tr('Lab Engine not initialized'), type='negative')
            return

        # Update variant level and max changes from UI before search
        if mode_select.value == 'variants' and state.var_mgr:
            # Get pairs count from preset or slider
            pairs_count = int(variant_slider.value) if variant_slider else current_preset['value']
            state.var_mgr.set_variant_level(pairs_count)
            if state.lab_engine and state.lab_engine.settings:
                state.lab_engine.settings.variant_max_changes = int(max_changes_select.value)

        # Reset state
        p_state.is_running = True
        p_state.is_cancelled = False
        p_state.progress = 0
        p_state.finished_animation_shown = False
        p_state.status = tr('Initializing search...')
        p_state.results = []
        results_container.clear()

        def progress_cb(current, total):
            if p_state.is_cancelled:
                raise InterruptedError("Search cancelled")
            if total > 0:
                p_state.progress = current / total
                p_state.status = f"{current} / {total}"

        def run_search():
            try:
                # Use the correct method signature from genizah_core
                # lab_composition_search(full_text, mode, progress_callback, chunk_size, excluded_ids, filter_text, deep_scan, scan_limit)
                result = state.lab_engine.lab_composition_search(
                    text,
                    mode=mode_select.value,
                    progress_callback=progress_cb,
                    chunk_size=int(chunk_size.value),
                    filter_text=filter_input.value or None,
                    deep_scan=deep_scan.value
                )
                return result
            except InterruptedError:
                return None
            except Exception as e:
                print(f"Parallels Error: {e}")
                import traceback
                traceback.print_exc()
                return None

        result_data = await run.io_bound(run_search)

        p_state.is_running = False
        p_state.progress = 1.0

        if p_state.is_cancelled:
            p_state.status = tr('Search cancelled')
            return

        if result_data:
            main_results = result_data.get('main', [])
            if main_results:
                p_state.results = main_results
                try:
                    app.storage.user['parallels_results'] = main_results
                except Exception:
                    pass
                render_results(main_results)
            else:
                with results_container:
                    show_empty_state()
        else:
            with results_container:
                show_empty_state()

    def show_empty_state():
        with ui.column().classes('w-full items-center py-12'):
            ui.icon('search_off').classes('text-5xl').style('color: var(--text-muted);')
            # Changed to H3
            h3(tr('No parallels found'), classes='text-lg mt-4', style='color: var(--text-secondary);')
            ui.label(tr('Try adjusting your search parameters')).classes('text-sm').style('color: var(--text-muted);')

    def render_results(results):
        results_container.clear()

        if not results:
            with results_container:
                show_empty_state()
            return

        # Sort if needed
        sort_by = sort_select.value
        if sort_by == 'score':
            sorted_results = sorted(results, key=lambda x: x.get('score', 0), reverse=True)
        elif sort_by == 'shelfmark':
            sorted_results = sorted(results, key=lambda x: extract_shelfmark(x))
        else:
            sorted_results = results

        # Group results by manuscript
        grouped = {}
        for item in sorted_results:
            raw_header = item.get('raw_header', '')
            sys_id = None
            shelfmark = 'Unknown'

            if raw_header and state.meta_mgr:
                try:
                    sys_match = re.search(r'(99\d{8,})', raw_header)
                    if sys_match:
                        sys_id = sys_match.group(1)
                        shelf_temp, _ = state.meta_mgr.get_meta_for_id(sys_id)
                        shelfmark = shelf_temp or shelfmark
                except Exception:
                    pass

            # Use sys_id as key, fallback to shelfmark
            key = sys_id if sys_id else shelfmark

            if key not in grouped:
                grouped[key] = {
                    'sys_id': sys_id,
                    'shelfmark': shelfmark,
                    'items': [],
                    'max_score': 0,
                    'avg_score': 0
                }

            grouped[key]['items'].append(item)
            grouped[key]['max_score'] = max(grouped[key]['max_score'], item.get('score', 0))

        # Calculate average scores
        for key in grouped:
            scores = [item.get('score', 0) for item in grouped[key]['items']]
            grouped[key]['avg_score'] = sum(scores) / len(scores) if scores else 0

        # Sort groups by max score
        sorted_groups = sorted(grouped.items(), key=lambda x: x[1]['max_score'], reverse=True)

        # Update header with manuscript count
        total_results = len(results)
        total_manuscripts = len(sorted_groups)
        results_header.text = f"{total_results} {tr('matches in')} {total_manuscripts} {tr('manuscripts')}"

        with results_container:
            for group_key, group_data in sorted_groups:
                create_manuscript_group(group_data)

    def create_manuscript_group(group_data):
        """Create an expandable manuscript group with its parallels."""
        shelfmark = group_data['shelfmark']
        sys_id = group_data['sys_id']
        items = group_data['items']
        max_score = group_data['max_score']
        avg_score = group_data['avg_score']

        # Get title
        title = ''
        if sys_id and state.meta_mgr:
            try:
                _, title_temp = state.meta_mgr.get_meta_for_id(sys_id)
                title = title_temp or ''
            except Exception:
                pass

        with ui.card().classes('w-full p-0 overflow-hidden').style('border: 2px solid var(--border-light);'):
            # Header (always visible)
            with ui.row().classes('w-full items-center justify-between p-4').style('background: var(--bg-card);'):
                with ui.column().classes('gap-1 flex-grow'):
                    with ui.row().classes('items-center gap-3'):
                        ui.icon('menu_book').classes('text-xl').style('color: var(--primary-600);')
                        # Changed to H3
                        h3(shelfmark, classes='text-lg font-bold', style='color: var(--primary-700);')
                        ui.badge(f"{len(items)} {tr('matches')}", color='blue').classes('text-xs')

                    if title:
                        title_short = (title[:100] + '...') if len(title) > 100 else title
                        ui.label(title_short).classes('text-xs').style('color: var(--text-secondary); direction: rtl;')

                with ui.row().classes('items-center gap-3'):
                    # Score badges
                    max_color = 'green' if max_score > 70 else 'amber' if max_score > 40 else 'gray'
                    ui.badge(f"{tr('Max')}: {int(max_score)}", color=max_color).classes('text-xs')
                    avg_color = 'green' if avg_score > 60 else 'amber' if avg_score > 35 else 'gray'
                    ui.badge(f"{tr('Avg')}: {int(avg_score)}", color=avg_color).classes('text-xs')

            # All matches (initially visible in compact form)
            with ui.column().classes('w-full').style('background: var(--bg-secondary);'):
                for idx, item in enumerate(items):
                    create_parallel_item(idx, item, sys_id, shelfmark)

    def create_parallel_item(idx, item, sys_id, shelfmark):
        """Create a single parallel match item within a manuscript group."""
        score = int(item.get('score', 0))

        # Format text snippets
        ms_text = html.escape(item.get('text', '').replace('\n', ' '))
        ms_text_html = re.sub(r'\*(.*?)\*', r'<span class="highlight-match">\1</span>', ms_text)

        src_text = html.escape(item.get('source_ctx', '').replace('\n', ' '))
        src_text_html = re.sub(r'\*(.*?)\*', r'<span class="highlight-match">\1</span>', src_text)

        # Create short preview (first 80 chars)
        ms_text_clean = item.get('text', '').replace('*', '').replace('\n', ' ').strip()
        preview = (ms_text_clean[:80] + '...') if len(ms_text_clean) > 80 else ms_text_clean

        with ui.expansion().classes('w-full').style('border-bottom: 1px solid var(--border-light);') as expansion:
            # Compact header (always visible)
            with expansion.add_slot('header'):
                with ui.row().classes('w-full items-center gap-3 py-2 px-4'):
                    ui.label(f"#{idx + 1}").classes('text-xs px-2 py-0.5 rounded').style(
                        'background: var(--bg-tertiary); color: var(--text-muted);'
                    )
                    score_color = 'green' if score > 70 else 'amber' if score > 40 else 'gray'
                    ui.badge(f"{score}", color=score_color).classes('text-xs')

                    # Preview snippet
                    ui.label(preview).classes('text-sm flex-grow').style(
                        'color: var(--text-secondary); direction: rtl; text-align: right;'
                    )

            # Expanded content (shown on click)
            with ui.column().classes('w-full p-4 gap-4').style('background: var(--bg-card);'):
                # Content comparison
                with ui.row().classes('w-full gap-3'):
                    # Source context
                    with ui.column().classes('flex-1 gap-2'):
                        ui.label(tr('Your Text')).classes('text-xs font-bold uppercase').style('color: var(--success);')
                        with ui.element('div').classes('p-3 rounded-lg text-sm').style(
                            'background: var(--bg-tertiary); direction: rtl; text-align: right; line-height: 1.8; border: 1px solid var(--success); color: var(--text-primary);'
                        ):
                            ui.html(src_text_html, sanitize=False)

                    # Manuscript match
                    with ui.column().classes('flex-1 gap-2'):
                        ui.label(tr('Manuscript Text')).classes('text-xs font-bold uppercase').style('color: var(--accent-amber);')
                        with ui.element('div').classes('p-3 rounded-lg text-sm').style(
                            'background: var(--bg-tertiary); direction: rtl; text-align: right; line-height: 1.8; border: 1px solid var(--accent-amber); color: var(--text-primary);'
                        ):
                            ui.html(ms_text_html, sanitize=False)

                # Action buttons
                with ui.row().classes('w-full gap-2 mt-2'):
                    if sys_id:
                        # Browse button
                        ui.button(
                            tr('Browse'),
                            icon='menu_book',
                            on_click=lambda: ui.navigate.to(f'/browse?sys_id={sys_id}')
                        ).props('flat dense size=sm color=primary')

                        # Metadata button
                        def show_metadata_dialog(sid=sys_id, shelf=shelfmark):
                            show_parallel_metadata(sid, shelf, item)

                        ui.button(
                            tr('Metadata'),
                            icon='info',
                            on_click=show_metadata_dialog
                        ).props('flat dense size=sm')

                        # Add to list button
                        def show_add_dialog(sid=sys_id, shelf=shelfmark):
                            show_add_to_list_dialog_parallel(sid, shelf)

                        ui.button(
                            tr('Add to List'),
                            icon='star',
                            on_click=show_add_dialog
                        ).props('flat dense size=sm').style('color: var(--accent-amber);')

                        # Edit and Comment buttons
                        ms_text_clean = item.get('text', '').replace('*', '').replace('\n', ' ').strip()
                        if ms_text_clean:
                            from web.components import create_edit_button, create_comment_button
                            create_edit_button(
                                document_id=sys_id,
                                page_number=1,  # Page unknown in parallels
                                original_text=ms_text_clean,
                                shelfmark=shelfmark,
                                size='sm'
                            )
                            create_comment_button(
                                document_id=sys_id,
                                page_number=1,
                                shelfmark=shelfmark,
                                size='sm'
                            )

    def show_parallel_metadata(sys_id, shelfmark, item):
        """Show metadata dialog for a parallel result."""
        # Get full metadata
        title = ''
        if sys_id and state.meta_mgr:
            try:
                _, title_temp = state.meta_mgr.get_meta_for_id(sys_id)
                title = title_temp or ''
            except Exception:
                pass

        with ui.dialog() as dialog, ui.card().classes('p-6 min-w-96 max-w-2xl'):
            # Changed to H3
            h3(tr('Metadata'), classes='text-xl font-bold mb-4')

            with ui.column().classes('w-full gap-3'):
                metadata_items = [
                    (tr('Shelfmark'), shelfmark),
                    (tr('Title'), title or tr('Not available')),
                    (tr('System ID'), sys_id or tr('Not available')),
                    (tr('Score'), str(int(item.get('score', 0)))),
                ]

                for label, value in metadata_items:
                    with ui.row().classes('w-full items-start gap-4'):
                        ui.label(label + ':').classes('font-bold w-32').style('color: var(--text-secondary);')
                        ui.label(value).classes('flex-grow').style('color: var(--text-primary); direction: rtl;')

            with ui.row().classes('w-full justify-end gap-2 mt-6'):
                ui.button(tr('Close'), on_click=dialog.close).classes('btn-primary')

        dialog.open()

    def show_add_to_list_dialog_parallel(sys_id, shelfmark):
        """Show add to list dialog for a parallel result."""
        if not sys_id:
            ui.notify(tr('Cannot add: missing system ID'), type='warning')
            return

        with ui.dialog() as dialog, ui.card().classes('p-6 min-w-96'):
            # Changed to H3
            h3(tr('Add to List'), classes='text-xl font-bold mb-2')
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

                    def do_add():
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

    def extract_shelfmark(item):
        raw_header = item.get('raw_header', '')
        if raw_header and state.meta_mgr:
            try:
                sys_match = re.search(r'(99\d{8,})', raw_header)
                if sys_match:
                    sys_id = sys_match.group(1)
                    shelf, _ = state.meta_mgr.get_meta_for_id(sys_id)
                    return shelf or 'Unknown'
            except Exception:
                pass
        return 'Unknown'

    def create_result_card(idx, item):
        score = int(item.get('score', 0))
        raw_header = item.get('raw_header', '')

        # Extract metadata
        sys_id = None
        shelfmark = 'Unknown'
        title = ''

        if raw_header and state.meta_mgr:
            try:
                sys_match = re.search(r'(99\d{8,})', raw_header)
                if sys_match:
                    sys_id = sys_match.group(1)
                    shelf_temp, title_temp = state.meta_mgr.get_meta_for_id(sys_id)
                    shelfmark = shelf_temp or shelfmark
                    title = title_temp or ''
            except Exception:
                pass

        # Format text snippets (escape HTML first to prevent XSS)
        ms_text = html.escape(item.get('text', '').replace('\n', ' '))
        ms_text_html = re.sub(r'\*(.*?)\*', r'<span class="highlight-match">\1</span>', ms_text)

        src_text = html.escape(item.get('source_ctx', '').replace('\n', ' '))
        src_text_html = re.sub(r'\*(.*?)\*', r'<span style="background: #bbf7d0; padding: 2px 4px; border-radius: 3px;">\1</span>', src_text)

        with ui.card().classes('w-full p-5 hover:shadow-lg transition-all'):
            # Header row
            with ui.row().classes('w-full items-start justify-between mb-4'):
                with ui.column().classes('gap-1'):
                    with ui.row().classes('items-center gap-3'):
                        ui.label(f"#{idx + 1}").classes('text-xs px-2 py-1 rounded').style(
                            'background: var(--bg-tertiary); color: var(--text-muted);'
                        )
                        ui.label(shelfmark).classes('text-lg font-bold').style('color: var(--primary-700);')
                    if title:
                        title_short = (title[:80] + '...') if len(title) > 80 else title
                        ui.label(title_short).classes('text-sm').style('color: var(--text-secondary); direction: rtl;')

                # Score badge
                score_color = 'green' if score > 70 else 'amber' if score > 40 else 'gray'
                ui.badge(f"{tr('Score')}: {score}", color=score_color).classes('text-sm')

            # Content comparison
            with ui.row().classes('w-full gap-4'):
                # Source context
                with ui.column().classes('flex-1 gap-2'):
                    ui.label(tr('Source Context')).classes('text-xs font-bold uppercase').style('color: var(--success);')
                    with ui.element('div').classes('p-4 rounded-lg text-sm').style(
                        'background: var(--bg-tertiary); direction: rtl; text-align: right; line-height: 1.8; border: 1px solid var(--success); color: var(--text-primary);'
                    ):
                        ui.html(src_text_html, sanitize=False)

                # Manuscript match
                with ui.column().classes('flex-1 gap-2'):
                    ui.label(tr('Manuscript Match')).classes('text-xs font-bold uppercase').style('color: var(--accent-amber);')
                    with ui.element('div').classes('p-4 rounded-lg text-sm').style(
                        'background: var(--bg-tertiary); direction: rtl; text-align: right; line-height: 1.8; border: 1px solid var(--accent-amber); color: var(--text-primary);'
                    ):
                        ui.html(ms_text_html, sanitize=False)

            # Actions
            with ui.row().classes('w-full gap-2 mt-4 pt-4').style('border-top: 1px solid var(--border-light);'):
                if sys_id:
                    ui.button(
                        tr('View manuscript'),
                        icon='menu_book',
                        on_click=lambda sid=sys_id: ui.navigate.to(f'/browse?sys_id={sid}')
                    ).props('flat dense').style('color: var(--primary-700);')

                ui.button(
                    icon='star_border',
                    on_click=lambda i=item, s=shelfmark, t=title, sid=sys_id: add_to_list(i, s, t, sid)
                ).props('flat round dense').style('color: var(--accent-amber);').tooltip(tr('Add to Favorites'))

                # Edit and Comment buttons
                ms_text_clean = item.get('text', '').replace('*', '').replace('\n', ' ').strip()
                if ms_text_clean and sys_id:
                    from web.components import create_edit_button, create_comment_button
                    create_edit_button(
                        document_id=sys_id,
                        page_number=1,  # Page unknown in parallels
                        original_text=ms_text_clean,
                        shelfmark=shelfmark,
                        size='sm'
                    )
                    create_comment_button(
                        document_id=sys_id,
                        page_number=1,
                        shelfmark=shelfmark,
                        size='sm'
                    )

    def add_to_list(item, shelfmark, title, sys_id):
        if not sys_id:
            ui.notify(tr('Cannot add: missing system ID'), type='warning')
            return

        with ui.dialog() as dialog, ui.card().classes('p-6 min-w-96'):
            # Changed to H3
            h3(tr('Add to List'), classes='text-xl font-bold mb-2')
            ui.label(f"{tr('Item')}: {shelfmark}").style('color: var(--text-secondary);')

            if state.lists_mgr:
                lists = state.lists_mgr.data.get('lists', {})
                list_options = {lid: lst['name'] for lid, lst in lists.items() if not lst.get('is_system')}

                if list_options:
                    selected_list = ui.select(list_options, label=tr('Select List')).classes('w-full mt-4').props('outlined').style('color: var(--text-primary);')
                    note_input = ui.input(label=tr('Note (optional)')).classes('w-full mt-2').props('outlined')

                    def do_add():
                        if state.lists_mgr.add_item(sys_id, selected_list.value, note=note_input.value):
                            ui.notify(tr('Added to list'), type='positive')
                            dialog.close()
                        else:
                            ui.notify(tr('Already in list'), type='info')

                    with ui.row().classes('w-full justify-end gap-2 mt-6'):
                        ui.button(tr('Cancel'), on_click=dialog.close).props('flat')
                        ui.button(tr('Add'), on_click=add_to_list).classes('btn-primary')
                else:
                    ui.label(tr('No lists available. Create a list first.')).style('color: var(--text-muted);')
                    ui.button(tr('Go to Lists'), on_click=lambda: ui.navigate.to('/lists')).classes('btn-primary mt-4')

        dialog.open()

    # Sort change handler
    sort_select.on('update:model-value', lambda: render_results(p_state.results) if p_state.results else None)

    # Initialize with restored results
    if p_state.results:
        results_header.text = f"{len(p_state.results)} {tr('parallels found')}"
        render_results(p_state.results)
