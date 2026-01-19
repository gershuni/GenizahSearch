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

    with ui.column().classes('w-full max-w-7xl mx-auto gap-6 md:gap-8 fade-in px-0'):

        # === Hero Section ===
        with ui.card().classes('w-full p-4 md:p-8 bg-gradient-to-br from-emerald-50 to-teal-50 border-0 hero-section').style(
            'background: linear-gradient(135deg, var(--primary-50) 0%, var(--bg-tertiary) 100%);'
        ):
            with ui.row().classes('w-full items-center justify-between flex-wrap gap-4 md:gap-6 hero-content'):
                with ui.column().classes('gap-2 flex-1'):
                    ui.label(tr('Welcome to Genizah Search Pro')).classes(
                        'text-xl md:text-3xl font-bold hero-title'
                    ).style('color: var(--primary-800);')
                    ui.label(tr('Advanced research tools for Cairo Genizah manuscripts')).classes(
                        'text-sm md:text-lg hero-subtitle'
                    ).style('color: var(--text-secondary);')

                # Quick Stats Row
                with ui.row().classes('gap-2 md:gap-4 mini-stats-row'):
                    def mini_stat(icon, value_fn, label):
                        with ui.card().classes('px-3 md:px-6 py-3 md:py-4 text-center mini-stat-card').style(
                            'background: var(--bg-card);'
                        ):
                            ui.icon(icon).classes('text-xl md:text-2xl').style('color: var(--primary-600);')
                            val_label = ui.label('...').classes('text-lg md:text-2xl font-bold mt-1 mini-stat-value').style('color: var(--text-primary);')
                            ui.label(label).classes('text-xs').style('color: var(--text-muted);')

                            def refresh():
                                if state.is_ready():
                                    val_label.text = str(value_fn())

                            # Update once after short delay (state should be ready)
                            ui.timer(1.0, refresh, once=True)

                    def get_doc_count():
                        if state.searcher and state.searcher.searcher:
                            return f"{state.searcher.searcher.num_docs:,}"
                        return "0"

                    def get_list_count():
                        return len(state.lists_mgr.get_all_lists()) if state.lists_mgr else 0

                    mini_stat('library_books', get_doc_count, tr('Pages'))
                    mini_stat('star', get_list_count, tr('Lists'))

        # === Main Action Cards Grid ===
        ui.label(tr('Research Tools')).classes('text-lg md:text-xl font-bold mt-2 md:mt-4').style('color: var(--text-primary);')

        with ui.row().classes('w-full gap-4 md:gap-6 flex-wrap tool-cards-grid'):

            # Search Card
            with ui.card().classes('flex-1 min-w-full sm:min-w-80 p-0 overflow-hidden cursor-pointer hover:shadow-xl transition-all tool-card').on(
                'click', lambda: ui.navigate.to('/search')
            ):
                with ui.column().classes('w-full'):
                    with ui.row().classes('w-full p-4 md:p-6 items-center gap-3 md:gap-4 tool-card-header').style(
                        'background: linear-gradient(135deg, var(--primary-600), var(--primary-700));'
                    ):
                        ui.icon('search').classes('text-3xl md:text-4xl text-white tool-card-icon')
                        with ui.column().classes('gap-1'):
                            ui.label(tr('Text Search')).classes('text-lg md:text-xl font-bold text-white tool-card-title')
                            ui.label(tr('Search in manuscripts')).classes('text-xs md:text-sm text-white/80')

                    with ui.column().classes('p-4 md:p-6 gap-3 md:gap-4 tool-card-content'):
                        ui.label(tr('Search for words and phrases in the Genizah corpus')).classes('text-sm md:text-base').style(
                            'color: var(--text-secondary);'
                        )

                        with ui.row().classes('gap-2 flex-wrap'):
                            for mode in ['Exact', 'Variants', 'Fuzzy', 'Regex']:
                                ui.badge(tr(mode)).props('outline').classes('text-xs')

                        ui.button(tr('Start Search'), icon='arrow_forward').classes('btn-primary mt-2').style('min-height: 44px;')

            # Parallels Card
            with ui.card().classes('flex-1 min-w-full sm:min-w-80 p-0 overflow-hidden cursor-pointer hover:shadow-xl transition-all tool-card').on(
                'click', lambda: ui.navigate.to('/parallels')
            ):
                with ui.column().classes('w-full'):
                    with ui.row().classes('w-full p-4 md:p-6 items-center gap-3 md:gap-4 tool-card-header').style(
                        'background: linear-gradient(135deg, #3b82f6, #1d4ed8);'
                    ):
                        ui.icon('compare_arrows').classes('text-3xl md:text-4xl text-white tool-card-icon')
                        with ui.column().classes('gap-1'):
                            ui.label(tr('Find Parallels')).classes('text-lg md:text-xl font-bold text-white tool-card-title')
                            ui.label(tr('Composition Search')).classes('text-xs md:text-sm text-white/80')

                    with ui.column().classes('p-4 md:p-6 gap-3 md:gap-4 tool-card-content'):
                        ui.label(tr('Enter a long text and find parallel texts in the Genizah')).classes('text-sm md:text-base').style(
                            'color: var(--text-secondary);'
                        )

                        with ui.row().classes('gap-2 flex-wrap'):
                            ui.badge(tr('Lab Mode')).props('outline color=blue').classes('text-xs')
                            ui.badge(tr('Chunk Analysis')).props('outline color=blue').classes('text-xs')

                        ui.button(tr('Find Parallels'), icon='arrow_forward').props('color=blue').classes('mt-2').style('min-height: 44px;')

            # Browse Card
            with ui.card().classes('flex-1 min-w-full sm:min-w-80 p-0 overflow-hidden cursor-pointer hover:shadow-xl transition-all tool-card').on(
                'click', lambda: ui.navigate.to('/browse')
            ):
                with ui.column().classes('w-full'):
                    with ui.row().classes('w-full p-4 md:p-6 items-center gap-3 md:gap-4 tool-card-header').style(
                        'background: linear-gradient(135deg, #f59e0b, #d97706);'
                    ):
                        ui.icon('menu_book').classes('text-3xl md:text-4xl text-white tool-card-icon')
                        with ui.column().classes('gap-1'):
                            ui.label(tr('Browse Manuscripts')).classes('text-lg md:text-xl font-bold text-white tool-card-title')
                            ui.label(tr('Browse by shelfmark')).classes('text-xs md:text-sm text-white/80')

                    with ui.column().classes('p-4 md:p-6 gap-3 md:gap-4 tool-card-content'):
                        ui.label(tr('Navigate through manuscript pages')).classes('text-sm md:text-base').style(
                            'color: var(--text-secondary);'
                        )

                        with ui.row().classes('gap-2 flex-wrap'):
                            ui.badge(tr('Transcriptions')).props('outline color=amber').classes('text-xs')
                            ui.badge(tr('Images')).props('outline color=amber').classes('text-xs')

                        ui.button(tr('Browse'), icon='arrow_forward').props('color=amber').classes('mt-2').style('min-height: 44px;')

        # === Secondary Actions Row ===
        with ui.row().classes('w-full gap-4 md:gap-6 mt-2 md:mt-4 flex-wrap'):

            # Personal Lists
            with ui.card().classes('flex-1 min-w-full sm:min-w-64 p-4 md:p-6 cursor-pointer hover:shadow-lg transition-all secondary-card').on(
                'click', lambda: ui.navigate.to('/lists')
            ).style('min-height: 72px;'):
                with ui.row().classes('items-center gap-3 md:gap-4'):
                    with ui.element('div').classes('p-2 md:p-3 rounded-xl').style('background: var(--primary-100);'):
                        ui.icon('star').classes('text-xl md:text-2xl').style('color: var(--primary-700);')
                    with ui.column().classes('gap-1 flex-1'):
                        ui.label(tr('Personal Lists')).classes('font-bold text-sm md:text-base').style('color: var(--text-primary);')
                        ui.label(tr('Organize and save manuscripts for easy access')).classes('text-xs md:text-sm').style(
                            'color: var(--text-muted);'
                        )

            # Lab Settings
            with ui.card().classes('flex-1 min-w-full sm:min-w-64 p-4 md:p-6 cursor-pointer hover:shadow-lg transition-all secondary-card').on(
                'click', lambda: ui.navigate.to('/settings')
            ).style('min-height: 72px;'):
                with ui.row().classes('items-center gap-3 md:gap-4'):
                    with ui.element('div').classes('p-2 md:p-3 rounded-xl').style('background: #dbeafe;'):
                        ui.icon('tune').classes('text-xl md:text-2xl text-blue-700')
                    with ui.column().classes('gap-1 flex-1'):
                        ui.label(tr('Lab Settings')).classes('font-bold text-sm md:text-base').style('color: var(--text-primary);')
                        ui.label(tr('Configure advanced search parameters')).classes('text-xs md:text-sm').style(
                            'color: var(--text-muted);'
                        )

            # Help Center
            with ui.card().classes('flex-1 min-w-full sm:min-w-64 p-4 md:p-6 cursor-pointer hover:shadow-lg transition-all secondary-card').on(
                'click', lambda: ui.navigate.to('/help')
            ).style('min-height: 72px;'):
                with ui.row().classes('items-center gap-3 md:gap-4'):
                    with ui.element('div').classes('p-2 md:p-3 rounded-xl').style('background: #fef3c7;'):
                        ui.icon('help_center').classes('text-xl md:text-2xl text-amber-700')
                    with ui.column().classes('gap-1 flex-1'):
                        ui.label(tr('Help Center')).classes('font-bold text-sm md:text-base').style('color: var(--text-primary);')
                        ui.label(tr('Learn how to use Genizah Search')).classes('text-xs md:text-sm').style(
                            'color: var(--text-muted);'
                        )

        # === Recent Activity Section ===
        with ui.card().classes('w-full p-4 md:p-6 mt-2 md:mt-4'):
            with ui.row().classes('w-full items-center justify-between mb-3 md:mb-4'):
                ui.label(tr('Recent Activity')).classes('text-base md:text-lg font-bold').style('color: var(--text-primary);')
                ui.button(tr('View All'), icon='arrow_forward').props('flat dense').style('min-height: 44px;').on(
                    'click', lambda: ui.navigate.to('/lists')
                )

            # Recent items container
            recent_container = ui.row().classes('w-full gap-3 md:gap-4 flex-wrap recent-items-grid')

            def load_recent():
                recent_container.clear()
                with recent_container:
                    if state.lists_mgr:
                        # Use get_items_in_list to get properly structured data
                        recent_items = state.lists_mgr.get_items_in_list('recent')[:6]
                        if recent_items:
                            for item in recent_items:
                                item_id = item.get('item_id', '')
                                sys_id = item.get('sys_id', '')

                                # Parse sys_id from item_id if needed (format: sys_id::fl::xxx or sys_id::img::xxx)
                                if not sys_id and item_id:
                                    if '::' in item_id:
                                        sys_id = item_id.split('::')[0]
                                    else:
                                        sys_id = item_id

                                if not sys_id:
                                    continue

                                # Get metadata
                                shelfmark = item.get('shelfmark') or item.get('shelfmark_override') or 'Unknown'
                                title = item.get('title', '')

                                # Enrich from metadata manager
                                if (not shelfmark or shelfmark == 'Unknown') and state.meta_mgr:
                                    shelf_temp, title_temp = state.meta_mgr.get_meta_for_id(sys_id)
                                    if shelf_temp:
                                        shelfmark = shelf_temp
                                    if not title and title_temp:
                                        title = title_temp

                                with ui.card().classes('p-3 md:p-4 cursor-pointer hover:shadow-md transition-all recent-item-card').style(
                                    'min-width: 140px; flex: 1 1 140px; max-width: 200px;'
                                ).on(
                                    'click', lambda sid=sys_id: ui.navigate.to(f'/browse?sys_id={sid}')
                                ):
                                    ui.label(shelfmark).classes('font-semibold truncate text-sm md:text-base').style(
                                        'color: var(--primary-700); max-width: 100%;'
                                    )
                                    if title:
                                        ui.label(title).classes('text-xs truncate').style(
                                            'color: var(--text-muted); max-width: 100%; direction: rtl;'
                                        )
                        else:
                            with ui.column().classes('w-full items-center py-6 md:py-8'):
                                ui.icon('history').classes('text-3xl md:text-4xl').style('color: var(--text-muted);')
                                ui.label(tr('No recent activity')).classes('text-sm').style(
                                    'color: var(--text-muted);'
                                )
                    else:
                        with ui.column().classes('w-full items-center py-6 md:py-8'):
                            ui.spinner(size='lg')

            ui.timer(1.0, load_recent, once=True)

        # === System Status Section ===
        with ui.expansion(tr('System Status'), icon='info').classes('w-full mt-2 md:mt-4'):
            with ui.row().classes('w-full gap-4 md:gap-6 p-3 md:p-4 flex-wrap status-grid'):
                def status_item(label, value_fn, icon_name):
                    with ui.column().classes('status-item').style('min-width: 120px;'):
                        with ui.row().classes('items-center gap-2'):
                            ui.icon(icon_name).classes('text-base md:text-lg').style('color: var(--primary-600);')
                            ui.label(label).classes('font-medium text-xs md:text-sm').style('color: var(--text-secondary);')
                        val = ui.label('...').classes('text-lg md:text-xl font-bold').style('color: var(--text-primary);')

                        def refresh():
                            if state.is_ready():
                                val.text = str(value_fn())

                        # Update once after short delay (state should be ready)
                        ui.timer(1.5, refresh, once=True)

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

        # === Credits Section ===
        with ui.card().classes('w-full p-4 md:p-6 mt-2 md:mt-4').style('background: var(--bg-tertiary);'):
            with ui.row().classes('w-full items-start gap-3 md:gap-4'):
                ui.icon('info').classes('text-xl md:text-2xl').style('color: var(--primary-600);')
                with ui.column().classes('flex-1 gap-2'):
                    ui.label(tr('Data Source')).classes('text-base md:text-lg font-bold').style('color: var(--text-primary);')
                    ui.label(tr('Transcriptions provided by the MiDRASH Project')).classes('text-xs md:text-sm').style('color: var(--text-secondary);')

                    # Citation
                    with ui.column().classes('gap-1 mt-2'):
                        ui.label(tr('Citation:')).classes('text-xs font-semibold').style('color: var(--text-muted);')
                        ui.label('Stoekl Ben Ezra, D., et al. (2025). MiDRASH Automatic Transcriptions.').classes('text-xs').style('color: var(--text-muted); word-break: break-word;')

                    # Zenodo link
                    with ui.row().classes('items-center gap-2 mt-1'):
                        ui.icon('open_in_new').classes('text-sm').style('color: var(--primary-600);')
                        ui.link('View Dataset on Zenodo', 'https://doi.org/10.5281/zenodo.17734473', new_tab=True).classes('text-xs md:text-sm').style('color: var(--primary-600); text-decoration: none;')

                    # License
                    ui.label('Licensed under CC BY 4.0').classes('text-xs mt-2').style('color: var(--text-muted);')
