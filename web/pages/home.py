# -*- coding: utf-8 -*-
"""
Research Dashboard - Dicta Genizah Search

A professional research dashboard providing:
- Quick access to all research tools
- Real-time statistics
- Recent activity tracking
- Quick search capabilities
"""

import asyncio

from nicegui import ui
from web.state import state
from web.translations import tr, is_rtl
from web.components.typography import h1, h2, h3


def create_page():
    """Create the research dashboard home page."""
    page_client = ui.context.client

    with ui.column().classes('w-full max-w-7xl mx-auto gap-3 fade-in'):

        # === OCR Disclaimer Banner (dismissible, compact single-line) ===
        # 2026-05-12 Codex 3rd-pass HIGH: safe_user_get so prune races on
        # / don't 500 the homepage.
        from web.safe_storage import safe_user_get as _safe_get, safe_user_set as _safe_set
        if not _safe_get('ocr_disclaimer_dismissed', False):
            banner_dir = 'rtl' if is_rtl() else 'ltr'
            with ui.element('div').classes('w-full px-4 py-2 flex items-center gap-3').style(
                f'background: var(--bg-tertiary); border-bottom: 1px solid var(--border-light); direction: {banner_dir};'
            ) as ocr_banner:
                ui.icon('psychology').classes('text-base').style('color: var(--primary-600);')
                ui.label(tr('Computer-read manuscripts — expect some reading errors!')).classes('text-xs flex-1').style('color: var(--text-secondary);')
                ui.link(tr('Learn more →'), '/about').classes('text-xs').style('color: var(--primary-600); text-decoration: none;')
                def dismiss_banner():
                    # Explicit user dismiss (X button): persist unconditionally.
                    _safe_set('ocr_disclaimer_dismissed', True)
                    try:
                        ocr_banner.delete()
                    except Exception:
                        pass  # Already dismissed / parent slot gone
                ui.button(icon='close', on_click=dismiss_banner).props(f'flat dense round size=xs aria-label="{tr("Dismiss")}"')
                # asyncio.call_later instead of ui.timer: ui.timer binds to the
                # banner slot and raises RuntimeError if the user navigates away
                # before the auto-dismiss fires.
                def _auto_dismiss_ocr():
                    # Persist the dismissed flag only if the banner is still alive
                    # and we actually hide it. If the user left /home before 10s,
                    # .delete() raises and we must not mark the disclaimer as seen
                    # — otherwise navigating away inside 10s permanently hides it.
                    try:
                        ocr_banner.delete()
                    except Exception:
                        return
                    _safe_set('ocr_disclaimer_dismissed', True)
                try:
                    asyncio.get_event_loop().call_later(10.0, _auto_dismiss_ocr)
                except RuntimeError:
                    pass

        # === Hero Section (compact) ===
        with ui.element('div').classes('w-full px-6 py-3').style(
            'background: var(--bg-tertiary); border: 1px solid var(--border-light); border-radius: 8px;'
        ):
            with ui.row().classes('w-full items-center justify-between gap-4 flex-wrap'):
                with ui.column().classes('gap-1 flex-1 min-w-0'):
                    h1(tr('Dicta Genizah Search — Full-Text Manuscript Search'),
                       classes='text-lg font-bold',
                       style='color: var(--text-primary); margin: 0;')
                    ui.label(tr('Search over 255,000 MiDRASH transcriptions — text, variants, parallels, joins, and images')).classes(
                        'text-sm'
                    ).style('color: var(--text-secondary);')

                # Inline stats
                with ui.row().classes('items-center gap-4'):
                    def mini_stat(icon, value_fn, label):
                        with ui.row().classes('items-center gap-1'):
                            ui.icon(icon).classes('text-base').style('color: var(--primary-600);')
                            val_label = ui.label('...').classes('text-sm font-bold').style('color: var(--text-primary);')
                            ui.label(label).classes('text-xs').style('color: var(--text-muted);')

                            def refresh():
                                if state.is_ready():
                                    val_label.text = str(value_fn())

                            async def _deferred_refresh():
                                await asyncio.sleep(0.1)
                                try:
                                    refresh()
                                except Exception:
                                    pass  # Deferred UI refresh failed; home page still usable
                            asyncio.ensure_future(_deferred_refresh())

                    def get_doc_count():
                        if state.searcher and state.searcher.searcher:
                            return f"{state.searcher.searcher.num_docs:,}"
                        return "0"

                    def get_list_count():
                        return len(state.lists_mgr.get_all_lists()) if state.lists_mgr else 0

                    mini_stat('library_books', get_doc_count, tr('Pages'))
                    mini_stat('star', get_list_count, tr('Lists'))

        # === Hero Search Bar ===
        with ui.element('div').classes('w-full px-6 py-4 mt-2').style(
            'background: var(--bg-tertiary); border: 1px solid var(--border-light); border-radius: 8px;'
        ):
            with ui.row().classes('w-full justify-center'):
                with ui.row().classes('items-center gap-2').style(
                    'max-width: 600px; width: 100%;'
                ):
                    def _navigate_search():
                        val = hero_search.value or ''
                        if val.strip():
                            ui.navigate.to(f'/search?q={val}')

                    hero_search = ui.input(
                        placeholder=tr('Search manuscripts...')
                    ).classes('flex-grow hero-search-input').props('outlined rounded dense').style(
                        'font-size: 1.1rem;'
                    ).on('keydown.enter', lambda: _navigate_search())
                    ui.button(
                        icon='search',
                        on_click=lambda: _navigate_search()
                    ).props(f'round color=primary aria-label="{tr("Search")}"').style('width: 44px; height: 44px;')

        # === Capability Chips (clickable) ===
        with ui.row().classes('w-full justify-center gap-2 flex-wrap mt-2 px-2'):
            _chips = [
                ('text_fields', tr('Free Text Search'), '/search'),
                ('spellcheck', tr('Spelling Variants'), '/search?mode=variants'),
                ('compare_arrows', tr('Parallel Detection'), '/parallels'),
                ('hub', tr('Join Search'), '/search?mode=responsa'),
                ('terminal', tr('Advanced Search'), '/search?mode=Regex'),
                ('image', tr('Images + Text'), '/browse'),
                ('category', tr('Browse by Identification'), '/catalog-browse'),
                ('lightbulb', tr('Community'), '/discoveries'),
                ('computer', tr('Downloadable App'), '/download'),
            ]
            for icon_name, label, href in _chips:
                with ui.row().classes('items-center gap-1 px-2 py-1 cursor-pointer hover:shadow-sm transition-all').style(
                    'border: 1px solid var(--border-light); border-radius: 16px; background: var(--bg-tertiary);'
                ).on('click', lambda h=href: ui.navigate.to(h)):
                    ui.icon(icon_name).classes('text-sm').style('color: var(--primary-600);')
                    ui.label(label).classes('text-xs').style('color: var(--text-secondary);')

        # === Info Carousel (auto-rotates + manual arrows) ===
        _card_style = 'background: var(--bg-tertiary); border: 1px solid var(--border-light);'
        _card_classes = 'w-full p-0 overflow-hidden cursor-pointer hover:shadow-xl transition-all'

        # Define carousel slides: (icon_or_img, heading, body, link)
        _slides_data = [
            {
                'icon': None,
                'img': 'https://upload.wikimedia.org/wikipedia/commons/f/f7/Education_%28T-S_K5.13%29_%28cropped%29.jpg',
                'heading': tr('What is the Cairo Genizah?'),
                'body': tr('Hundreds of thousands of medieval manuscripts from a Cairo synagogue attic — now searchable for the first time'),
                'link': '/about',
            },
            {
                'icon': 'explore',
                'heading': tr('What can I do here?'),
                'body': tr('Search MiDRASH transcriptions of ~255,000 Genizah manuscripts with variants, find textual parallels, locate joins, browse images alongside text, and share discoveries with the research community.'),
                'link': '/help',
            },
            {
                'icon': 'dataset',
                'heading': tr('Data Sources'),
                'body': tr('Transcriptions: MiDRASH. Manuscript images: NLI, Cambridge, Oxford, and others. Scholarly descriptions, bibliography, and catalog data: NLI, FGP, PGP, Oxford, Cambridge, and others.'),
                'link': '/about',
            },
            {
                'icon': 'computer',
                'heading': tr('Desktop App'),
                'body': tr('A downloadable Windows application for power users — fast local search, offline access, and advanced workflows.'),
                'link': '/download',
            },
            {
                'icon': 'lightbulb',
                'heading': tr('Community Discoveries'),
                'body': tr('Researchers are already finding new joins and parallels. Share your own discoveries, suggest corrections, and contribute to the research community.'),
                'link': '/discoveries',
            },
        ]

        # Build slide cards
        _slide_cards = []
        for i, slide in enumerate(_slides_data):
            card = ui.card().classes(_card_classes).props('role=button tabindex=0').style(_card_style)
            card.on('click', lambda link=slide['link']: ui.navigate.to(link))
            if i > 0:
                card.set_visibility(False)
            with card:
                with ui.row().classes('w-full items-center gap-6 p-5 flex-wrap'):
                    if slide.get('img'):
                        ui.html(
                            f'<img src="{slide["img"]}" alt="" loading="lazy"'
                            ' style="width: 100px; height: 80px; object-fit: cover; border-radius: 8px; opacity: 0.9;">',
                            sanitize=False
                        )
                    elif slide.get('icon'):
                        ui.icon(slide['icon']).classes('text-5xl').style('color: var(--primary-600); opacity: 0.8;')
                    with ui.column().classes('flex-1 gap-1'):
                        h2(slide['heading'], classes='text-lg font-bold', style='color: var(--text-primary);')
                        ui.label(slide['body']).classes('text-sm').style('color: var(--text-secondary);')
            _slide_cards.append(card)

        # Carousel state + controls
        _carousel = {'index': 0}

        def _show_slide(idx):
            _carousel['index'] = idx % len(_slide_cards)
            for j, c in enumerate(_slide_cards):
                c.set_visibility(j == _carousel['index'])
            _dot_update()

        def _next_slide():
            _show_slide(_carousel['index'] + 1)

        def _prev_slide():
            _show_slide(_carousel['index'] - 1)

        # Navigation row: prev arrow, dots, next arrow
        _prev_icon = 'chevron_right' if is_rtl() else 'chevron_left'
        _next_icon = 'chevron_left' if is_rtl() else 'chevron_right'
        with ui.row().classes('w-full justify-center items-center gap-1 mt-1'):
            ui.button(icon=_prev_icon, on_click=_prev_slide).props('flat dense round size=sm')
            _dots = []
            for i in range(len(_slide_cards)):
                dot = ui.icon('circle').classes('text-xs cursor-pointer').style(
                    f'color: {"var(--primary-600)" if i == 0 else "var(--border-light)"}; font-size: 8px;'
                )
                dot.on('click', lambda idx=i: _show_slide(idx))
                _dots.append(dot)
            ui.button(icon=_next_icon, on_click=_next_slide).props('flat dense round size=sm')

        def _dot_update():
            for j, dot in enumerate(_dots):
                dot.style(f'color: {"var(--primary-600)" if j == _carousel["index"] else "var(--border-light)"}; font-size: 8px;')

        # Auto-rotate every 15 seconds. asyncio task instead of ui.timer so the
        # callback does not raise 'parent_slot has been deleted' RuntimeError
        # when the user navigates away from /home before the interval fires.
        async def _auto_rotate():
            while True:
                await asyncio.sleep(15.0)
                if getattr(page_client, '_deleted', False):
                    return
                try:
                    _next_slide()
                except Exception:
                    return  # Carousel slots gone; stop rotating cleanly
        asyncio.ensure_future(_auto_rotate())

        # === Seasonal banner (Pesach/other themes) — hidden until next seasonal activation ===
        # The Pesach banner code is preserved in git history and the supporting module
        # web/pesach.py remains as a reusable template for future seasonal themes
        # (e.g. Rosh Hashanah, Hanukkah, Shavuot). To re-enable: uncomment below and
        # update the is_*_season() gate + fragment source.
        #
        # from web.pesach import is_pesach_season, get_random_pesach_fragments, EMOJI_WINE
        # if is_pesach_season():
        #     ... (see git history before commit hiding Pesach banner)

        # === Main Action Cards Grid ===
        # Changed to H2
        h2(tr('Research Tools'), classes='text-xl font-bold mt-4', style='color: var(--text-primary);')

        with ui.element('div').classes('w-full').style(
            'display: grid; grid-template-columns: repeat(auto-fill, minmax(280px, 1fr)); gap: 1rem;'
        ):

            # Search Card
            with ui.card().classes('p-0 overflow-hidden cursor-pointer hover:shadow-xl transition-all').props(
                'role=button tabindex=0'
            ).on('click', lambda: ui.navigate.to('/search')).on('keydown.enter', lambda: ui.navigate.to('/search')).on('keydown.space', lambda: ui.navigate.to('/search')):
                with ui.column().classes('w-full'):
                    with ui.row().classes('w-full p-4 items-center gap-3').style(
                        'background: linear-gradient(135deg, var(--primary-600), var(--primary-700));'
                    ):
                        ui.icon('search').classes('text-3xl text-white')
                        with ui.column().classes('gap-0'):
                            h3(tr('Text Search'), classes='text-base font-bold text-white')
                            ui.label(tr('Search in manuscripts')).classes('text-xs text-white/80')

                    with ui.column().classes('p-4 gap-3'):
                        ui.label(tr('Search for words and phrases in the Genizah corpus')).classes('text-sm').style(
                            'color: var(--text-secondary);'
                        )
                        with ui.row().classes('gap-2 flex-wrap'):
                            for mode in ['Exact', 'Variants', 'Regex']:
                                ui.badge(tr(mode)).props('outline').classes('text-xs')

            # Parallels Card
            with ui.card().classes('p-0 overflow-hidden cursor-pointer hover:shadow-xl transition-all').props(
                'role=button tabindex=0'
            ).on('click', lambda: ui.navigate.to('/parallels')).on('keydown.enter', lambda: ui.navigate.to('/parallels')).on('keydown.space', lambda: ui.navigate.to('/parallels')):
                with ui.column().classes('w-full'):
                    with ui.row().classes('w-full p-4 items-center gap-3').style(
                        'background: linear-gradient(135deg, #3b82f6, #1d4ed8);'
                    ):
                        ui.icon('compare_arrows').classes('text-3xl text-white')
                        with ui.column().classes('gap-0'):
                            h3(tr('Find Parallels'), classes='text-base font-bold text-white')
                            ui.label(tr('Composition Search')).classes('text-xs text-white/80')

                    with ui.column().classes('p-4 gap-3'):
                        ui.label(tr('Enter a long text and find parallel texts in the Genizah')).classes('text-sm').style(
                            'color: var(--text-secondary);'
                        )
                        with ui.row().classes('gap-2 flex-wrap'):
                            ui.badge(tr('Lab Mode')).props('outline color=blue-9').classes('text-xs')
                            ui.badge(tr('Chunk Analysis')).props('outline color=blue-9').classes('text-xs')

            # Browse by Shelfmark Card
            with ui.card().classes('p-0 overflow-hidden cursor-pointer hover:shadow-xl transition-all').props(
                'role=button tabindex=0'
            ).on('click', lambda: ui.navigate.to('/browse')).on('keydown.enter', lambda: ui.navigate.to('/browse')).on('keydown.space', lambda: ui.navigate.to('/browse')):
                with ui.column().classes('w-full'):
                    with ui.row().classes('w-full p-4 items-center gap-3').style(
                        'background: linear-gradient(135deg, #f59e0b, #d97706);'
                    ):
                        ui.icon('menu_book').classes('text-3xl text-white')
                        with ui.column().classes('gap-0'):
                            h3(tr('Browse by Shelfmark'), classes='text-base font-bold text-white')
                            ui.label(tr('Navigate through manuscript pages')).classes('text-xs text-white/80')

                    with ui.column().classes('p-4 gap-3'):
                        ui.label(tr('Navigate through manuscript pages')).classes('text-sm').style(
                            'color: var(--text-secondary);'
                        )
                        with ui.row().classes('gap-2 flex-wrap'):
                            ui.badge(tr('Transcriptions')).props('outline color=deep-orange-10').classes('text-xs')
                            ui.badge(tr('Images')).props('outline color=deep-orange-10').classes('text-xs')

            # Browse by Identification Card
            with ui.card().classes('p-0 overflow-hidden cursor-pointer hover:shadow-xl transition-all').props(
                'role=button tabindex=0'
            ).on('click', lambda: ui.navigate.to('/catalog-browse')).on('keydown.enter', lambda: ui.navigate.to('/catalog-browse')).on('keydown.space', lambda: ui.navigate.to('/catalog-browse')):
                with ui.column().classes('w-full'):
                    with ui.row().classes('w-full p-4 items-center gap-3').style(
                        'background: linear-gradient(135deg, #8b5cf6, #6d28d9);'
                    ):
                        ui.icon('category').classes('text-3xl text-white')
                        with ui.column().classes('gap-0'):
                            h3(tr('Browse by Identification'), classes='text-base font-bold text-white')
                            ui.label(tr('Domains, authors & works')).classes('text-xs text-white/80')

                    with ui.column().classes('p-4 gap-3'):
                        ui.label(tr('Browse the manuscript corpus by scholarly domain classifications, author attributions, and work identifications.')).classes('text-sm').style(
                            'color: var(--text-secondary);'
                        )
                        with ui.row().classes('gap-2 flex-wrap'):
                            ui.badge(tr('Domains')).props('outline color=purple').classes('text-xs')
                            ui.badge(tr('Authors')).props('outline color=purple').classes('text-xs')

            # Community Card
            with ui.card().classes('p-0 overflow-hidden cursor-pointer hover:shadow-xl transition-all').props(
                'role=button tabindex=0'
            ).on('click', lambda: ui.navigate.to('/discoveries')).on('keydown.enter', lambda: ui.navigate.to('/discoveries')).on('keydown.space', lambda: ui.navigate.to('/discoveries')):
                with ui.column().classes('w-full'):
                    with ui.row().classes('w-full p-4 items-center gap-3').style(
                        'background: linear-gradient(135deg, #ec4899, #be185d);'
                    ):
                        ui.icon('lightbulb').classes('text-3xl text-white')
                        with ui.column().classes('gap-0'):
                            h3(tr('Community'), classes='text-base font-bold text-white')
                            ui.label(tr('Community discoveries, questions, and contributions')).classes('text-xs text-white/80')

                    with ui.column().classes('p-4 gap-3'):
                        ui.label(tr('View community discoveries, questions, and share your own findings')).classes('text-sm').style(
                            'color: var(--text-secondary);'
                        )
                        with ui.row().classes('gap-2 flex-wrap'):
                            ui.badge(tr('Discoveries')).props('outline color=pink-9').classes('text-xs')
                            ui.badge(tr('Corrections')).props('outline color=pink-9').classes('text-xs')

        # === Secondary Actions Row ===
        with ui.row().classes('w-full gap-6 mt-4 flex-wrap'):

            # Personal Lists
            with ui.card().classes('flex-1 min-w-64 p-6 cursor-pointer hover:shadow-lg transition-all').props(
                'role=button tabindex=0'
            ).on('click', lambda: ui.navigate.to('/lists')).on('keydown.enter', lambda: ui.navigate.to('/lists')).on('keydown.space', lambda: ui.navigate.to('/lists')):
                with ui.row().classes('items-center gap-4'):
                    with ui.element('div').classes('p-3 rounded-xl').style('background: var(--primary-100);'):
                        ui.icon('star').classes('text-2xl').style('color: var(--primary-700);')
                    with ui.column().classes('gap-1'):
                        # Changed to H3
                        h3(tr('Personal Lists'), classes='text-lg font-bold', style='color: var(--text-primary);')
                        ui.label(tr('Organize and save manuscripts for easy access')).classes('text-sm').style(
                            'color: var(--text-muted);'
                        )

            # Lab Settings
            with ui.card().classes('flex-1 min-w-64 p-6 cursor-pointer hover:shadow-lg transition-all').props(
                'role=button tabindex=0'
            ).on('click', lambda: ui.navigate.to('/settings')).on('keydown.enter', lambda: ui.navigate.to('/settings')).on('keydown.space', lambda: ui.navigate.to('/settings')):
                with ui.row().classes('items-center gap-4'):
                    with ui.element('div').classes('p-3 rounded-xl').style('background: #dbeafe;'):
                        ui.icon('tune').classes('text-2xl text-blue-700')
                    with ui.column().classes('gap-1'):
                        # Changed to H3
                        h3(tr('Lab Settings'), classes='text-lg font-bold', style='color: var(--text-primary);')
                        ui.label(tr('Configure advanced search parameters')).classes('text-sm').style(
                            'color: var(--text-muted);'
                        )

            # Help Center
            with ui.card().classes('flex-1 min-w-64 p-6 cursor-pointer hover:shadow-lg transition-all').props(
                'role=button tabindex=0'
            ).on('click', lambda: ui.navigate.to('/help')).on('keydown.enter', lambda: ui.navigate.to('/help')).on('keydown.space', lambda: ui.navigate.to('/help')):
                with ui.row().classes('items-center gap-4'):
                    with ui.element('div').classes('p-3 rounded-xl').style('background: #fef3c7;'):
                        ui.icon('help_center').classes('text-2xl text-amber-700')
                    with ui.column().classes('gap-1'):
                        # Changed to H3
                        h3(tr('Help Center'), classes='text-lg font-bold', style='color: var(--text-primary);')
                        ui.label(tr('Learn how to use the Genizah site')).classes('text-sm').style(
                            'color: var(--text-muted);'
                        )

            # Desktop App
            with ui.card().classes('flex-1 min-w-64 p-6 cursor-pointer hover:shadow-lg transition-all').props(
                'role=button tabindex=0'
            ).on('click', lambda: ui.navigate.to('/download')).on('keydown.enter', lambda: ui.navigate.to('/download')).on('keydown.space', lambda: ui.navigate.to('/download')):
                with ui.row().classes('items-center gap-4'):
                    with ui.element('div').classes('p-3 rounded-xl').style('background: #e0e7ff;'):
                        ui.icon('download').classes('text-2xl text-indigo-700')
                    with ui.column().classes('gap-1'):
                        h3(tr('Desktop App'), classes='text-lg font-bold', style='color: var(--text-primary);')
                        ui.label(tr('Fast, powerful, works offline')).classes('text-sm').style(
                            'color: var(--text-muted);'
                        )

        # === Recent Activity Section ===
        with ui.card().classes('w-full p-6 mt-4'):
            with ui.row().classes('w-full items-center justify-between mb-4'):
                # Changed to H2
                h2(tr('Recent Activity'), classes='text-lg font-bold', style='color: var(--text-primary);')
                ui.button(tr('View All'), icon='arrow_back' if is_rtl() else 'arrow_forward').props('flat dense').on(
                    'click', lambda: ui.navigate.to('/lists')
                )

            # Recent items container
            recent_container = ui.row().classes('w-full gap-4 flex-wrap')

            def load_recent():
                recent_container.clear()
                with recent_container:
                    if state.lists_mgr:
                        # Use sync version to avoid async/await issues
                        recent_items = state.lists_mgr.get_items_in_list_sync('recent')[:6]
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

                                with ui.card().classes('p-4 min-w-48 cursor-pointer hover:shadow-md transition-all').props(
                                    'role=button tabindex=0'
                                ).on('click', lambda sid=sys_id: ui.navigate.to(f'/browse?sys_id={sid}')).on('keydown.enter', lambda sid=sys_id: ui.navigate.to(f'/browse?sys_id={sid}')).on('keydown.space', lambda sid=sys_id: ui.navigate.to(f'/browse?sys_id={sid}')):
                                    ui.label(shelfmark).classes('font-semibold truncate').style(
                                        'color: var(--primary-700); max-width: 180px;'
                                    )
                                    if title:
                                        ui.label(title).classes('text-xs truncate').style(
                                            'color: var(--text-muted); max-width: 180px; direction: rtl;'
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

            async def _deferred_load_recent():
                await asyncio.sleep(0.3)
                try:
                    await load_recent()
                except Exception:
                    pass  # Deferred UI refresh failed; page still usable
            asyncio.ensure_future(_deferred_load_recent())

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

                        async def _deferred_status_refresh():
                            await asyncio.sleep(0.5)
                            try:
                                refresh()
                            except Exception:
                                pass  # Deferred status refresh failed; page still usable
                        asyncio.ensure_future(_deferred_status_refresh())

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
        with ui.card().classes('w-full p-6 mt-4').style('background: var(--bg-tertiary);'):
            with ui.row().classes('w-full items-start gap-4'):
                ui.icon('info').classes('text-2xl').style('color: var(--primary-600);')
                with ui.column().classes('flex-1 gap-2'):
                    # Changed to H3
                    h3(tr('Data Source'), classes='text-lg font-bold', style='color: var(--text-primary);')
                    ui.label(tr('Transcriptions provided by the MiDRASH Project')).classes('text-sm').style('color: var(--text-secondary);')

                    # Citation
                    with ui.column().classes('gap-1 mt-2'):
                        ui.label(tr('Citation:')).classes('text-xs font-semibold').style('color: var(--text-muted);')
                        ui.label('Stoekl Ben Ezra, D., Bambaci, L., Kiessling, B., Lapin, H., Ezer, N., Lolli, E., Rustow, M., Dershowitz, N., Kurar Barakat, B., Gogawale, S., Shmidman, A., Lavee, M., Siew, T., Raziel Kretzmer, V., Vasyutinsky Shapira, D., Olszowy-Schlanger, J., & Gila, Y. (2025). MiDRASH Automatic Transcriptions of the Cairo Geniza Fragments [Data set]. Zenodo.').classes('text-xs').style('color: var(--text-muted); direction: ltr; text-align: left;')

                    # Zenodo link
                    with ui.row().classes('items-center gap-2 mt-1').style('direction: ltr;'):
                        ui.icon('open_in_new').classes('text-sm').style('color: var(--primary-600);')
                        ui.link('https://doi.org/10.5281/zenodo.17734473', 'https://doi.org/10.5281/zenodo.17734473', new_tab=True).classes('text-sm').style('color: var(--primary-600); text-decoration: none;')

                    # License
                    ui.label(tr('Licensed under CC BY 4.0')).classes('text-xs mt-2').style('color: var(--text-muted);')
