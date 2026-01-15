# -*- coding: utf-8 -*-
"""
Research Dashboard - Genizah Search Pro

A professional research dashboard providing:
- Quick access to all research tools
- Real-time statistics
- Recent activity tracking
- Quick search capabilities
"""

from nicegui import ui, app
from web.state import state
from web.translations import tr, is_rtl


def create_page():
    """Create the research dashboard home page."""

    with ui.column().classes('w-full max-w-7xl mx-auto gap-8 fade-in'):

        # === Hero Section ===
        with ui.card().classes('w-full p-8 bg-gradient-to-br from-emerald-50 to-teal-50 border-0').style(
            'background: linear-gradient(135deg, var(--primary-50) 0%, var(--bg-tertiary) 100%);'
        ):
            with ui.row().classes('w-full items-center justify-between flex-wrap gap-6'):
                with ui.column().classes('gap-2'):
                    ui.label(tr('Welcome to Genizah Search Pro')).classes(
                        'text-3xl font-bold'
                    ).style('color: var(--primary-800);')
                    ui.label(tr('Advanced research tools for Cairo Genizah manuscripts')).classes(
                        'text-lg'
                    ).style('color: var(--text-secondary);')

                # Quick Stats Row
                with ui.row().classes('gap-4'):
                    def mini_stat(icon, value_fn, label):
                        with ui.card().classes('px-6 py-4 text-center').style(
                            'background: var(--bg-card); min-width: 120px;'
                        ):
                            ui.icon(icon).classes('text-2xl').style('color: var(--primary-600);')
                            val_label = ui.label('...').classes('text-2xl font-bold mt-1').style('color: var(--text-primary);')
                            ui.label(label).classes('text-xs').style('color: var(--text-muted);')

                            def refresh():
                                if state.is_ready():
                                    val_label.text = str(value_fn())

                            ui.timer(2.0, refresh)

                    def get_doc_count():
                        if state.searcher and state.searcher.searcher:
                            return f"{state.searcher.searcher.num_docs:,}"
                        return "0"

                    def get_list_count():
                        return len(state.lists_mgr.get_all_lists()) if state.lists_mgr else 0

                    mini_stat('library_books', get_doc_count, tr('Pages'))
                    mini_stat('star', get_list_count, tr('Lists'))

        # === Main Action Cards Grid ===
        ui.label(tr('Research Tools')).classes('text-xl font-bold mt-4').style('color: var(--text-primary);')

        with ui.row().classes('w-full gap-6 flex-wrap'):

            # Search Card
            with ui.card().classes('flex-1 min-w-80 p-0 overflow-hidden cursor-pointer hover:shadow-xl transition-all').on(
                'click', lambda: ui.navigate.to('/search')
            ):
                with ui.column().classes('w-full'):
                    with ui.row().classes('w-full p-6 items-center gap-4').style(
                        'background: linear-gradient(135deg, var(--primary-600), var(--primary-700));'
                    ):
                        ui.icon('search').classes('text-4xl text-white')
                        with ui.column().classes('gap-1'):
                            ui.label(tr('Text Search')).classes('text-xl font-bold text-white')
                            ui.label(tr('Search in manuscripts')).classes('text-sm text-white/80')

                    with ui.column().classes('p-6 gap-4'):
                        ui.label(tr('Search for words and phrases in the Genizah corpus')).style(
                            'color: var(--text-secondary);'
                        )

                        with ui.row().classes('gap-2 flex-wrap'):
                            for mode in ['Exact', 'Variants', 'Fuzzy', 'Regex']:
                                ui.badge(tr(mode)).props('outline').classes('text-xs')

                        ui.button(tr('Start Search'), icon='arrow_forward').classes('btn-primary mt-2')

            # Parallels Card
            with ui.card().classes('flex-1 min-w-80 p-0 overflow-hidden cursor-pointer hover:shadow-xl transition-all').on(
                'click', lambda: ui.navigate.to('/parallels')
            ):
                with ui.column().classes('w-full'):
                    with ui.row().classes('w-full p-6 items-center gap-4').style(
                        'background: linear-gradient(135deg, #3b82f6, #1d4ed8);'
                    ):
                        ui.icon('compare_arrows').classes('text-4xl text-white')
                        with ui.column().classes('gap-1'):
                            ui.label(tr('Find Parallels')).classes('text-xl font-bold text-white')
                            ui.label(tr('Composition Search')).classes('text-sm text-white/80')

                    with ui.column().classes('p-6 gap-4'):
                        ui.label(tr('Enter a long text and find parallel texts in the Genizah')).style(
                            'color: var(--text-secondary);'
                        )

                        with ui.row().classes('gap-2 flex-wrap'):
                            ui.badge(tr('Lab Mode')).props('outline color=blue').classes('text-xs')
                            ui.badge(tr('Chunk Analysis')).props('outline color=blue').classes('text-xs')

                        ui.button(tr('Find Parallels'), icon='arrow_forward').props('color=blue').classes('mt-2')

            # Browse Card
            with ui.card().classes('flex-1 min-w-80 p-0 overflow-hidden cursor-pointer hover:shadow-xl transition-all').on(
                'click', lambda: ui.navigate.to('/browse')
            ):
                with ui.column().classes('w-full'):
                    with ui.row().classes('w-full p-6 items-center gap-4').style(
                        'background: linear-gradient(135deg, #f59e0b, #d97706);'
                    ):
                        ui.icon('menu_book').classes('text-4xl text-white')
                        with ui.column().classes('gap-1'):
                            ui.label(tr('Browse Manuscripts')).classes('text-xl font-bold text-white')
                            ui.label(tr('Browse by shelfmark')).classes('text-sm text-white/80')

                    with ui.column().classes('p-6 gap-4'):
                        ui.label(tr('Navigate through manuscript pages')).style(
                            'color: var(--text-secondary);'
                        )

                        with ui.row().classes('gap-2 flex-wrap'):
                            ui.badge(tr('Transcriptions')).props('outline color=amber').classes('text-xs')
                            ui.badge(tr('Images')).props('outline color=amber').classes('text-xs')

                        ui.button(tr('Browse'), icon='arrow_forward').props('color=amber').classes('mt-2')

        # === Secondary Actions Row ===
        with ui.row().classes('w-full gap-6 mt-4 flex-wrap'):

            # Personal Lists
            with ui.card().classes('flex-1 min-w-64 p-6 cursor-pointer hover:shadow-lg transition-all').on(
                'click', lambda: ui.navigate.to('/lists')
            ):
                with ui.row().classes('items-center gap-4'):
                    with ui.element('div').classes('p-3 rounded-xl').style('background: var(--primary-100);'):
                        ui.icon('star').classes('text-2xl').style('color: var(--primary-700);')
                    with ui.column().classes('gap-1'):
                        ui.label(tr('Personal Lists')).classes('font-bold').style('color: var(--text-primary);')
                        ui.label(tr('Organize and save manuscripts for easy access')).classes('text-sm').style(
                            'color: var(--text-muted);'
                        )

            # Lab Settings
            with ui.card().classes('flex-1 min-w-64 p-6 cursor-pointer hover:shadow-lg transition-all').on(
                'click', lambda: ui.navigate.to('/settings')
            ):
                with ui.row().classes('items-center gap-4'):
                    with ui.element('div').classes('p-3 rounded-xl').style('background: #dbeafe;'):
                        ui.icon('tune').classes('text-2xl text-blue-700')
                    with ui.column().classes('gap-1'):
                        ui.label(tr('Lab Settings')).classes('font-bold').style('color: var(--text-primary);')
                        ui.label(tr('Configure advanced search parameters')).classes('text-sm').style(
                            'color: var(--text-muted);'
                        )

            # Help Center
            with ui.card().classes('flex-1 min-w-64 p-6 cursor-pointer hover:shadow-lg transition-all').on(
                'click', lambda: ui.navigate.to('/help')
            ):
                with ui.row().classes('items-center gap-4'):
                    with ui.element('div').classes('p-3 rounded-xl').style('background: #fef3c7;'):
                        ui.icon('help_center').classes('text-2xl text-amber-700')
                    with ui.column().classes('gap-1'):
                        ui.label(tr('Help Center')).classes('font-bold').style('color: var(--text-primary);')
                        ui.label(tr('Learn how to use Genizah Search')).classes('text-sm').style(
                            'color: var(--text-muted);'
                        )

        # === Recent Activity Section ===
        with ui.card().classes('w-full p-6 mt-4'):
            with ui.row().classes('w-full items-center justify-between mb-4'):
                ui.label(tr('Recent Activity')).classes('text-lg font-bold').style('color: var(--text-primary);')
                ui.button(tr('View All'), icon='arrow_forward').props('flat dense').on(
                    'click', lambda: ui.navigate.to('/lists')
                )

            # Recent items container
            recent_container = ui.row().classes('w-full gap-4 flex-wrap')

            def load_recent():
                recent_container.clear()
                with recent_container:
                    if state.lists_mgr:
                        recent_items = state.lists_mgr.data.get('recent_items', [])[:6]
                        if recent_items:
                            for item in recent_items:
                                # Handle both dict and string formats
                                if isinstance(item, dict):
                                    sys_id = item.get('sys_id', '')
                                    shelfmark = item.get('shelfmark', 'Unknown')
                                    title = item.get('title', '')
                                else:
                                    # Item is just a sys_id string
                                    sys_id = str(item)
                                    shelfmark = 'Unknown'
                                    title = ''

                                if not sys_id:
                                    continue

                                # Enrich if needed
                                if (not shelfmark or shelfmark == 'Unknown') and state.meta_mgr:
                                    shelf_temp, title_temp = state.meta_mgr.get_meta_for_id(sys_id)
                                    shelfmark = shelf_temp or shelfmark
                                    title = title or title_temp or ''

                                with ui.card().classes('p-4 min-w-48 cursor-pointer hover:shadow-md transition-all').on(
                                    'click', lambda sid=sys_id: ui.navigate.to(f'/browse?sys_id={sid}')
                                ):
                                    ui.label(shelfmark).classes('font-semibold truncate').style(
                                        'color: var(--primary-700); max-width: 180px;'
                                    )
                                    if title:
                                        ui.label(title).classes('text-xs truncate').style(
                                            'color: var(--text-muted); max-width: 180px;'
                                        )
                        else:
                            with ui.column().classes('w-full items-center py-8'):
                                ui.icon('history').classes('text-4xl').style('color: var(--text-muted);')
                                ui.label(tr('No recent activity')).classes('text-sm').style(
                                    'color: var(--text-muted);'
                                )
                    else:
                        with ui.column().classes('w-full items-center py-8'):
                            ui.spinner(size='lg')

            ui.timer(1.0, load_recent, once=True)

        # === System Status Section ===
        with ui.expansion(tr('System Status'), icon='info').classes('w-full mt-4'):
            with ui.row().classes('w-full gap-6 p-4 flex-wrap'):
                def status_item(label, value_fn, icon_name):
                    with ui.column().classes('min-w-40'):
                        with ui.row().classes('items-center gap-2'):
                            ui.icon(icon_name).classes('text-lg').style('color: var(--primary-600);')
                            ui.label(label).classes('font-medium').style('color: var(--text-secondary);')
                        val = ui.label('...').classes('text-xl font-bold').style('color: var(--text-primary);')

                        def refresh():
                            if state.is_ready():
                                val.text = str(value_fn())

                        ui.timer(3.0, refresh)

                status_item(
                    tr('Indexed Pages'),
                    lambda: f"{state.searcher.searcher.num_docs:,}" if state.searcher and state.searcher.searcher else "0",
                    'library_books'
                )
                status_item(
                    tr('Cached Metadata'),
                    lambda: len(state.meta_mgr.nli_cache) if state.meta_mgr else 0,
                    'storage'
                )
                status_item(
                    tr('Personal Lists'),
                    lambda: len(state.lists_mgr.get_all_lists()) if state.lists_mgr else 0,
                    'star'
                )
                status_item(
                    tr('Lab Index'),
                    lambda: tr("Ready") if state.lab_engine and not state.lab_engine.lab_index_needs_rebuild else tr("Rebuild Needed"),
                    'science'
                )
