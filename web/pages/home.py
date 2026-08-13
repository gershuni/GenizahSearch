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
import logging

from nicegui import run, ui
from web.state import state
from web.supabase_client import get_user_client
from web.client_guard import client_gone, show_load_error
from web.translations import tr, is_rtl
from web.components.typography import h1, h2, h3
from web.atlas_assets import atlas_preview_available
from web.discovery_assets import discovery_available
from web.analytics import posthog_capture

logger = logging.getLogger(__name__)


def create_page():
    """Create the research dashboard home page."""
    page_client = ui.context.client
    # Take one readiness snapshot for every homepage entry point. This keeps
    # chips, announcements, carousel slides, and tool cards in agreement if a
    # feature is disabled or its startup asset fails validation.
    _discovery_ready = discovery_available()
    _atlas_ready = atlas_preview_available()

    with ui.column().classes('w-full max-w-7xl mx-auto gap-3 fade-in'):

        # === OCR Disclaimer Banner (dismissible, compact single-line) ===
        # 2026-05-12 Codex 3rd-pass HIGH: safe_user_get so prune races on
        # / don't 500 the homepage.
        from web.safe_storage import safe_user_get as _safe_get, safe_user_set as _safe_set
        if not _safe_get('ocr_disclaimer_dismissed', False):
            banner_dir = 'rtl' if is_rtl() else 'ltr'
            # Fixed-position toast (out of document flow) so show/auto-dismiss
            # never reflow content (CLS). Stacked above the global What's New toast.
            with ui.element('div').classes('px-4 py-2 flex items-center gap-3').style(
                f'position: fixed; bottom: 74px; left: 50%; transform: translateX(-50%); '
                f'z-index: 2000; max-width: 90vw; background: var(--bg-tertiary); '
                f'border: 1px solid var(--border-light); border-radius: 8px; '
                f'box-shadow: 0 4px 16px rgba(0,0,0,0.18); direction: {banner_dir};'
            ) as ocr_banner:
                ui.icon('psychology').classes('text-base').style('color: var(--primary-600);')
                ui.label(tr('Computer-read manuscripts; expect some reading errors!')).classes('text-xs flex-1').style('color: var(--text-secondary);')
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
                    # and we actually hide it. If the user left /home before 30s,
                    # .delete() raises and we must not mark the disclaimer as seen
                    # Otherwise navigating away inside 30s permanently hides it.
                    try:
                        ocr_banner.delete()
                    except Exception:
                        return
                    _safe_set('ocr_disclaimer_dismissed', True)
                try:
                    asyncio.get_event_loop().call_later(30.0, _auto_dismiss_ocr)
                except RuntimeError:
                    pass

        # === Hero Section (compact) ===
        with ui.element('div').classes('w-full px-6 py-3').style(
            'background: var(--bg-tertiary); border: 1px solid var(--border-light); border-radius: 8px;'
        ):
            with ui.row().classes('w-full items-center justify-between gap-4 flex-wrap'):
                with ui.column().classes('gap-1 flex-1 min-w-0'):
                    h1(tr('Dicta Genizah Search: Full-Text Manuscript Search'),
                       classes='text-lg font-bold',
                       style='color: var(--text-primary); margin: 0;')
                    ui.label(tr('Search MiDRASH transcriptions across 255,000+ manuscripts: text, variants, parallels, joins, and images')).classes(
                        'text-sm'
                    ).style('color: var(--text-secondary);')

                # Inline stats
                with ui.row().classes('items-center gap-4'):
                    def mini_stat(icon, value_fn, label):
                        with ui.row().classes('items-center gap-1'):
                            ui.icon(icon).classes('text-base').style('color: var(--primary-600);')
                            val_label = ui.label('...').classes('text-sm font-bold').style('color: var(--text-primary); min-width: 3.5rem; display: inline-block;')
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

            # A first-class escape hatch for the many visitors who arrive
            # curious about the Genizah but without a query in mind.  This is
            # a native anchor (rather than a click-only card), so it keeps the
            # browser's keyboard, context-menu, and open-in-new-tab behavior.
            with ui.row().classes('w-full justify-center mt-3'):
                with ui.link(target='/start').classes(
                    'no-underline inline-flex items-center justify-center gap-2 px-5 py-2'
                ).props('data-testid=home-start-cta').style(
                    'min-height: 44px; border-radius: 999px; '
                    'background: var(--primary-700); color: var(--text-inverse); font-weight: 600;'
                ).on(
                    'click',
                    lambda: posthog_capture(
                        'welcome_action_clicked',
                        {
                            'route_id': 'home',
                            'action_id': 'home_start_here',
                            'difficulty': 'introductory',
                        },
                    ),
                ):
                    ui.icon('explore').props('aria-hidden=true').classes('text-lg')
                    ui.label(tr("What's on this website? Start here to explore the Cairo Genizah"))

        # === Capability Chips (clickable) ===
        with ui.row().classes('w-full justify-center gap-2 flex-wrap mt-2 px-2'):
            _chips = [
                ('text_fields', tr('Free Text Search'), '/search', None),
                ('spellcheck', tr('Spelling Variants'), '/search?mode=variants', None),
                ('compare_arrows', tr('Parallel Detection'), '/parallels', None),
                ('hub', tr('Join Search'), '/search?mode=responsa', None),
                ('terminal', tr('Advanced Search'), '/search?mode=Regex', None),
                ('image', tr('Images + Text'), '/browse', None),
                ('category', tr('Browse by Identification'), '/catalog-browse', None),
            ]
            if _discovery_ready:
                _chips.append((
                    'travel_explore', tr('Computed Identifications'),
                    '/computed-identifications', 'home-chip-computed',
                ))
            if _atlas_ready:
                _chips.append((
                    'public', tr('The Visual Genizah Atlas'), '/atlas', 'home-chip-atlas',
                ))
            _chips.extend([
                ('lightbulb', tr('Community'), '/discoveries', None),
                ('computer', tr('Downloadable App'), '/download', None),
            ])
            for icon_name, label, href, marker in _chips:
                chip = ui.row().classes('items-center gap-1 px-2 py-1 cursor-pointer hover:shadow-sm transition-all').style(
                    'border: 1px solid var(--border-light); border-radius: 16px; background: var(--bg-tertiary);'
                ).on('click', lambda h=href: ui.navigate.to(h))
                if marker:
                    chip.mark(marker)
                with chip:
                    ui.icon(icon_name).classes('text-sm').style('color: var(--primary-600);')
                    ui.label(label).classes('text-xs').style('color: var(--text-secondary);')

        # === Beta announcements — the two banners SIDE BY SIDE (owner, 2026-08-04) ===
        # Computed Identifications first, then the atlas. Each keeps its OWN
        # availability gate, so when only one is live it simply fills the row —
        # which is why the gates stay inside rather than around the row.
        #
        # `items-stretch` + `flex-1` makes both cards the same height whatever
        # the text length, and `min-w` forces a clean stack on a phone instead of
        # two unreadable columns. The inner layout is a COLUMN (not the earlier
        # icon-beside-text row) because at half width the button was wrapping
        # under the text anyway.
        _ann_dir = 'rtl' if is_rtl() else 'ltr'

        #: The count-free blurb, and the template that carries the figure. The
        #: count is the number of DISTINCT WORKS matched -- `meta.work_total`,
        #: the same figure the findings page headline shows.
        _DISCOVERY_BLURB_PLAIN = (
            'Software has matched Genizah fragments to works we already know. '
            'Use it to look for new witnesses to a text you study — and judge '
            'each match yourself.')
        _DISCOVERY_BLURB_COUNTED = (
            'Software has matched Genizah fragments to {count} works we already '
            'know. Use it to look for new witnesses to a text you study — and '
            'judge each match yourself.')

        def _fill_discovery_count(label) -> None:
            """Replace the blurb with its counted form, once the figure is read.

            DEFERRED, never inline: `create_page` is SYNCHRONOUS and the launch
            read is async over a 393 MB sidecar, so computing it here would put
            a query on the single uvicorn event loop -- stalling every concurrent
            request, including static files, while burning no CPU. The homepage
            already defers its own corpus stats exactly this way.

            The card renders the count-FREE sentence first and gains the figure a
            moment later, so a slow or failed read costs the reader a number and
            never the card: `{count}` is a size, not the claim.
            """
            async def _deferred():
                try:
                    from web.discovery import get_launch_stats_enveloped
                    envelope = await get_launch_stats_enveloped()
                    if envelope.get('status') != 'ok':
                        return
                    total = envelope.get('meta', {}).get('work_total')
                    if not isinstance(total, int) or total <= 0:
                        return      # never print a zero, a None or a "many"
                    label.text = tr(_DISCOVERY_BLURB_COUNTED).format(
                        count=f'{total:,}')
                except Exception:
                    pass            # the count is an ENRICHMENT; keep the card
            asyncio.ensure_future(_deferred())

        def _announcement_card(*, mark, route, gradient, shadow, chip_color,
                               icon, title, blurb, cta, cta_text_color,
                               on_blurb=None):
            with ui.element('div').classes(
                'flex-1 min-w-[280px] rounded-xl overflow-hidden cursor-pointer '
                'hover:shadow-2xl transition-all'
            ).props('role=button tabindex=0').style(
                f'background: {gradient}; box-shadow: {shadow};'
            ).on('click', lambda: ui.navigate.to(route)
                 ).on('keydown.enter', lambda: ui.navigate.to(route)
                      ).on('keydown.space', lambda: ui.navigate.to(route)).mark(mark):
                with ui.column().classes('w-full gap-2 px-5 py-4 h-full').style(
                    f'direction: {_ann_dir};'
                ):
                    with ui.row().classes('items-center gap-2 flex-wrap'):
                        ui.icon(icon).classes('text-3xl text-white')
                        ui.label(tr('New')).classes(
                            'px-2 py-0.5 rounded-full text-xs font-bold'
                        ).style(
                            f'background: rgba(255,255,255,0.92); color: {chip_color}; '
                            'letter-spacing: 0.05em;'
                        )
                        ui.label(tr('Beta')).classes(
                            'px-2 py-0.5 rounded-full text-xs font-semibold'
                        ).style('background: rgba(255,255,255,0.20); color: white;')
                    h2(tr(title), classes='text-xl font-bold text-white', style='margin: 0;')
                    _blurb_label = ui.label(tr(blurb)).classes(
                        'text-sm text-white/90 flex-grow')
                    if on_blurb is not None:
                        on_blurb(_blurb_label)
                    ui.button(tr(cta), icon=icon).props(
                        f'unelevated color=white text-color={cta_text_color}'
                    ).classes('font-bold self-start')

        if _discovery_ready or _atlas_ready:
            with ui.row().classes('w-full mt-2 gap-3 flex-wrap items-stretch'):
                # Computed Identifications (Phase 136).
                #
                # Gated on discovery_available() — the SAME predicate as the
                # /computed-identifications route and the nav entry — so a
                # flag-OFF or sidecar-missing window can never advertise a link
                # that clean-hides.
                #
                # DELIBERATELY CLAIM-FREE, a hard constraint rather than a style
                # choice: no precision percentage, no interval, no accuracy rate,
                # no count. MATCH-framing ("may be the same work as"), never
                # assertion-framing ("is") — the surface shows candidates for a
                # reader to judge, not settled facts.
                if _discovery_ready:
                    _announcement_card(
                        mark='discovery-announcement',
                        route='/computed-identifications',
                        gradient=('linear-gradient(120deg, #4338ca 0%, #6366f1 45%, '
                                  '#8b5cf6 100%)'),
                        shadow='0 6px 24px rgba(99,102,241,0.28)',
                        chip_color='#4338ca',
                        icon='travel_explore',
                        title='Computed Identifications',
                        blurb=_DISCOVERY_BLURB_PLAIN,
                        on_blurb=_fill_discovery_count,
                        cta='Explore Computed Identifications',
                        cta_text_color='indigo-9',
                    )
                # The Visual Genizah Atlas (#7, 2026-07-21). Claim-free: no
                # counts, no "identifications" — it just names the map.
                if _atlas_ready:
                    _announcement_card(
                        mark='atlas-announcement',
                        route='/atlas',
                        gradient=('linear-gradient(120deg, #0f766e 0%, #14b8a6 45%, '
                                  '#6366f1 100%)'),
                        shadow='0 6px 24px rgba(20,184,166,0.28)',
                        chip_color='#0f766e',
                        icon='hub',
                        title='The Visual Genizah Atlas',
                        blurb=('A new interactive map of textual connections across '
                               'the Cairo Genizah.'),
                        cta='Explore the Genizah Atlas',
                        cta_text_color='teal-9',
                    )

        # === Corpus Stats Band (SEED-023) — advertises the scale of the corpus ===
        # Five HARDCODED headline numbers (web/stats_service.CORPUS_STATS). Rendered
        # synchronously — no async/placeholder/readiness poll, so no layout shift.
        from web.stats_service import get_corpus_stats as _get_corpus_stats
        _stats = _get_corpus_stats()
        _stat_specs = [
            ('collections_bookmark', 'manuscripts', tr('Manuscripts')),
            ('inventory_2', 'catalog_entries', tr('Catalog entries')),
            ('photo_library', 'images', tr('Images')),
            ('menu_book', 'scholarly_transcriptions', tr('Scholarly transcriptions')),
            ('subject', 'automatic_transcriptions', tr('Automatic transcriptions')),
        ]
        with ui.row().classes('w-full justify-center gap-3 flex-wrap mt-2 px-2'):
            for _icon_name, _key, _label in _stat_specs:
                with ui.column().classes('items-center justify-center px-4 py-3').style(
                    'min-width: 150px; min-height: 96px; flex: 1 1 150px; max-width: 220px; '
                    'border: 1px solid var(--border-light); border-radius: 10px; '
                    'background: var(--bg-tertiary);'
                ):
                    ui.icon(_icon_name).classes('text-2xl').style('color: var(--primary-600);')
                    ui.label(f"{_stats.get(_key, 0):,}").classes('text-xl font-bold').style(
                        'color: var(--text-primary);'
                    )
                    ui.label(_label).classes('text-xs text-center').style('color: var(--text-muted);')

        # === Info Carousel (auto-rotates + manual arrows) ===
        _card_style = 'background: var(--bg-tertiary); border: 1px solid var(--border-light);'
        _card_classes = 'w-full p-0 overflow-hidden cursor-pointer hover:shadow-xl transition-all'

        # Define carousel slides: (icon_or_img, heading, body, link)
        _slides_data = [
            {
                'icon': None,
                'img': 'https://upload.wikimedia.org/wikipedia/commons/f/f7/Education_%28T-S_K5.13%29_%28cropped%29.jpg',
                'heading': tr('What is the Cairo Genizah?'),
                'body': tr('Hundreds of thousands of medieval manuscripts from a Cairo synagogue attic, now searchable for the first time'),
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
                'body': tr('A downloadable Windows application for power users: fast local search, offline access, and advanced workflows.'),
                'link': '/download',
            },
            {
                'icon': 'lightbulb',
                'heading': tr('Community Discoveries'),
                'body': tr('Researchers are already finding new joins and parallels. Share your own discoveries, suggest corrections, and contribute to the research community.'),
                'link': '/discoveries',
            },
        ]
        if _discovery_ready:
            _slides_data.append({
                'icon': 'travel_explore',
                'heading': tr('Computed Identifications'),
                'body': tr(_DISCOVERY_BLURB_PLAIN),
                'link': '/computed-identifications',
                'mark': 'home-carousel-computed',
            })
        if _atlas_ready:
            _slides_data.append({
                'icon': 'public',
                'heading': tr('The Visual Genizah Atlas'),
                'body': tr('A new interactive map of textual connections across the Cairo Genizah.'),
                'link': '/atlas',
                'mark': 'home-carousel-atlas',
            })

        # Build slide cards
        _slide_cards = []
        for i, slide in enumerate(_slides_data):
            card = ui.card().classes(_card_classes).props('role=button tabindex=0').style(_card_style)
            card.on('click', lambda link=slide['link']: ui.navigate.to(link))
            if slide.get('mark'):
                card.mark(slide['mark'])
            if i > 0:
                card.set_visibility(False)
            with card:
                with ui.row().classes('w-full items-center gap-6 p-5 flex-wrap'):
                    if slide.get('img'):
                        import html as _html
                        _alt = _html.escape(slide.get('heading', ''))
                        ui.html(
                            f'<img src="{slide["img"]}" alt="{_alt}" loading="lazy"'
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

        # About + FAQ sections moved to the bottom of the page (below
        # Credits) and lang-gated. See _render_about_and_faq() at the
        # bottom of create_page().

        # === Seasonal banner (Pesach/other themes), hidden until next seasonal activation ===
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

            # Joins Lab Card
            with ui.card().classes('p-0 overflow-hidden cursor-pointer hover:shadow-xl transition-all').props(
                'role=button tabindex=0'
            ).on('click', lambda: ui.navigate.to('/joins-lab')).on(
                'keydown.enter', lambda: ui.navigate.to('/joins-lab')
            ).on('keydown.space', lambda: ui.navigate.to('/joins-lab')).mark('joins-lab-tool-card'):
                with ui.column().classes('w-full'):
                    with ui.row().classes('w-full p-4 items-center gap-3').style(
                        'background: linear-gradient(135deg, #0f766e, #115e59);'
                    ):
                        ui.icon('join_inner').classes('text-3xl text-white')
                        with ui.column().classes('gap-0'):
                            h3(tr('Joins Lab'), classes='text-base font-bold text-white')
                            ui.label(tr('Find and compare joining fragments')).classes('text-xs text-white/80')

                    with ui.column().classes('p-4 gap-3'):
                        ui.label(tr(
                            'Pin an anchor fragment, build a line-by-line query, and compare possible physical joins.'
                        )).classes('text-sm').style('color: var(--text-secondary);')
                        with ui.row().classes('gap-2 flex-wrap'):
                            ui.badge(tr('Anchor')).props('outline color=teal-9').classes('text-xs')
                            ui.badge(tr('Candidates')).props('outline color=teal-9').classes('text-xs')

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

            # Computed Identifications Card. It uses the same flag+asset gate as
            # the page route and keeps the established suggestion-not-verdict
            # wording from the launch announcement.
            if _discovery_ready:
                with ui.card().classes('p-0 overflow-hidden cursor-pointer hover:shadow-xl transition-all').props(
                    'role=button tabindex=0'
                ).on('click', lambda: ui.navigate.to('/computed-identifications')).on(
                    'keydown.enter', lambda: ui.navigate.to('/computed-identifications')
                ).on(
                    'keydown.space', lambda: ui.navigate.to('/computed-identifications')
                ).mark('computed-tool-card'):
                    with ui.column().classes('w-full'):
                        with ui.row().classes('w-full p-4 items-center gap-3').style(
                            'background: linear-gradient(135deg, #6366f1, #4338ca);'
                        ):
                            ui.icon('travel_explore').classes('text-3xl text-white')
                            with ui.column().classes('gap-0'):
                                h3(tr('Computed Identifications'), classes='text-base font-bold text-white')
                                ui.label(tr('Review algorithmic text matches')).classes('text-xs text-white/80')

                        with ui.column().classes('p-4 gap-3'):
                            ui.label(tr(_DISCOVERY_BLURB_PLAIN)).classes('text-sm').style(
                                'color: var(--text-secondary);'
                            )
                            with ui.row().classes('gap-2 flex-wrap'):
                                ui.badge(tr('Text matching')).props('outline color=indigo-9').classes('text-xs')
                                ui.badge(tr('Requires review')).props('outline color=indigo-9').classes('text-xs')

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

            # Connections Atlas Teaser Card (Phase 133, ATLAS-01 — beta, claim-free)
            # Gated on the SAME atlas_preview_available() predicate as the /atlas
            # page route, its nav link, and the data routes (MEDIUM-6) — a
            # flag-ON/asset-missing window must never advertise a broken link
            # from a fourth surface (the homepage). Static card (no async data
            # fetch) — CLS-safe — and carries no claim-level statements (no
            # counts, no "identifications", no "discoveries found").
            if _atlas_ready:
                with ui.card().classes('p-0 overflow-hidden cursor-pointer hover:shadow-xl transition-all').props(
                    'role=button tabindex=0'
                ).on('click', lambda: ui.navigate.to('/atlas')).on('keydown.enter', lambda: ui.navigate.to('/atlas')).on('keydown.space', lambda: ui.navigate.to('/atlas')).mark('atlas-teaser-card'):
                    with ui.column().classes('w-full'):
                        with ui.row().classes('w-full p-4 items-center gap-3').style(
                            'background: linear-gradient(135deg, #14b8a6, #0f766e);'
                        ):
                            ui.icon('hub').classes('text-3xl text-white')
                            with ui.column().classes('gap-0'):
                                h3(tr('The Genizah Atlas'), classes='text-base font-bold text-white')
                                ui.label(tr('Explore the Genizah Atlas')).classes('text-xs text-white/80')

                        with ui.column().classes('p-4 gap-3'):
                            ui.label(
                                tr('A preview map of textual connections across the Cairo Genizah — '
                                   'a claim-free, algorithmically laid-out overview.')
                            ).classes('text-sm').style('color: var(--text-secondary);')
                            with ui.row().classes('gap-2 flex-wrap'):
                                ui.badge(tr('Beta')).props('outline color=teal-9').classes('text-xs')

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

            # Recent items container (min-height reserves space so the async
            # 0.3s load doesn't shift the System Status section below it — CLS).
            recent_container = ui.row().classes('w-full gap-4 flex-wrap').style('min-height: 120px;')

            def render_recent(recent_items):
                recent_container.clear()
                with recent_container:
                    if state.lists_mgr:
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
                # Previously `await load_recent()` on a plain `def`: the body ran
                # to completion ON the event loop (blocking every other request --
                # uvicorn runs a single worker) and then raised
                # `TypeError: object NoneType can't be used in 'await' expression`,
                # swallowed by a bare `except Exception: pass`, so genuine load
                # failures were invisible too.
                #
                # Now split: build the authenticated client HERE, on the loop
                # (where the NiceGUI request context exists), do the blocking read
                # in a worker, then render back on the loop. The explicit client is
                # mandatory -- `run.io_bound` goes through `loop.run_in_executor`,
                # which does not propagate contextvars, so a worker calling
                # `get_user_client()` itself would degrade to anonymous and
                # `recent_items` (`TO authenticated`, user-scoped) would return
                # ZERO rows: an empty "Recently viewed" for a logged-in user.
                await asyncio.sleep(0.3)
                try:
                    if not state.lists_mgr:
                        return
                    # Resolve the auth decision HERE, on the loop, and pass it in.
                    # Passing only `client` is not enough: the method otherwise
                    # re-derives auth itself via GlobalAuthState -> safe_user_get,
                    # which has no UI context in the worker, reads as logged-OUT,
                    # and falls through to the local_mgr branch -- so a signed-in
                    # user would silently get local-or-empty recent activity while
                    # the authenticated client went unused.
                    lists_mgr = state.lists_mgr
                    is_authed = lists_mgr.is_authenticated
                    reader_user_id = lists_mgr.user_id if is_authed else None
                    reader_client = get_user_client() if is_authed else None
                    recent_items = await run.io_bound(
                        lists_mgr.get_items_in_list_sync,
                        'recent',  # literal sentinel: resolves without any storage read
                        client=reader_client,
                        is_authenticated=is_authed,
                        user_id=reader_user_id,
                    )
                    if recent_items is None:
                        return  # app shutting down mid-flight
                    if client_gone(recent_container):
                        return  # SEED-008: user navigated away while we were fetching
                    render_recent(recent_items[:6])
                except RuntimeError as e:
                    # NiceGUI raises RuntimeError when mutating a torn-down client.
                    logger.debug("recent-items render skipped (client gone): %s", e)
                except Exception as e:
                    logger.warning("deferred recent-items load failed: %s", e, exc_info=False)
                    show_load_error(recent_container)
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

        # === SEO content + FAQ, moved to bottom of page 2026-05-21 ===
        # One block per UI language (no longer rendering both side-by-side).
        # The matching FAQPage JSON-LD in web/main.py:dashboard_page() is
        # also lang-gated so the Q&A text on the page matches the
        # structured-data payload byte-for-byte (Google rich-result rule).
        _is_he = is_rtl()
        with ui.element('section').classes('w-full mt-4 px-6 py-4').style(
            'background: var(--bg-tertiary); border: 1px solid var(--border-light); border-radius: 8px;'
        ):
            if _is_he:
                ui.html(
                    '''
<div lang="he" dir="rtl" style="text-align: right; color: var(--text-secondary); font-size: 0.95rem; line-height: 1.7;">
  <h2 style="color: var(--text-primary); font-size: 1.1rem; font-weight: 700; margin: 0 0 0.5rem 0;">על אודות אתר הגניזה של דיקטה</h2>
  <p style="margin: 0;">
    אתר הגניזה של דיקטה הוא מרכז מחקר רב עוצמה עם תעתיקים, תמונות ומידע של 255,000 קטעי כתבי יד מ<a href="https://he.wikipedia.org/wiki/%D7%92%D7%A0%D7%99%D7%96%D7%AA_%D7%A7%D7%94%D7%99%D7%A8" target="_blank" rel="noopener noreferrer" style="color: var(--primary-700); text-decoration: underline;">גניזת קהיר</a>.
    הגניזה הקהירית התגלתה בסוף המאה ה-19 בעליית הגג של בית הכנסת בן-עזרא בפוסטאט (קהיר העתיקה),
    והיא כוללת מאות אלפי קטעי כתבי יד בעברית, בארמית ובערבית-יהודית שמתוארכים מן המאה השמינית ועד המאה התשע-עשרה:
    תנ"ך, תלמוד, ספרות חז"ל, הלכה, פילוסופיה, תפילה, פיוט, מסמכים רשמיים, מכתבים, מדע, מאגיה ועוד.
    האתר משלב תעתיקים אוטומטיים שהופקו על ידי <a href="https://www.midrash.eu" target="_blank" rel="noopener noreferrer" style="color: var(--primary-700); text-decoration: underline;">פרויקט MiDRASH</a> בתמיכת האיחוד האירופי,
    תמונות באיכות גבוהה מ<a href="https://www.nli.org.il/he/discover/manuscripts/hebrew-manuscripts" target="_blank" rel="noopener noreferrer" style="color: var(--primary-700); text-decoration: underline;">אתר &quot;כתיב&quot;</a> של הספרייה הלאומית של ישראל ומספריות ברחבי העולם,
    ומידע קטלוגי ממקורות מגוונים, בראשם <a href="https://fjms.genizah.org/" target="_blank" rel="noopener noreferrer" style="color: var(--primary-700); text-decoration: underline;">פרויקט הגניזה של פרידברג</a>, אתר &quot;כתיב&quot;,
    <a href="https://geniza.princeton.edu/" target="_blank" rel="noopener noreferrer" style="color: var(--primary-700); text-decoration: underline;">פרויקט הגניזה של פרינסטון</a>, ספריות <a href="https://www.lib.cam.ac.uk/collections/departments/taylor-schechter-genizah-research-unit" target="_blank" rel="noopener noreferrer" style="color: var(--primary-700); text-decoration: underline;">קמברידג&apos;</a>, <a href="https://digital.bodleian.ox.ac.uk/" target="_blank" rel="noopener noreferrer" style="color: var(--primary-700); text-decoration: underline;">אוקספורד</a>, <a href="https://www.library.manchester.ac.uk/resources/" target="_blank" rel="noopener noreferrer" style="color: var(--primary-700); text-decoration: underline;">מנצ&apos;סטר</a>, <a href="https://www.jtsa.edu/library-special-collections/" target="_blank" rel="noopener noreferrer" style="color: var(--primary-700); text-decoration: underline;">בית המדרש לרבנים באמריקה</a> ועוד.
    ניתן לחפש חופשי בטקסט המלא של הקורפוס, לדפדף בקטעים יחד עם הצילומים,
    לזהות מקבילות לטקסטים ידועים ולמצוא צירופים חדשים בין קטעים, ולשתף תגליות עם קהילת החוקרים.
    התעתיקים האוטומטיים כוללים שיבושים רבים, ועל כן מומלץ להשתמש בחיפוש מתקדם בצורות שונות:
    וריאנטים לחילופים נפוצים בין אותיות, מקורב לחילופים חופשיים,
    וחיפוש מתקדם בעזרת מצב סגנון פרויקט השו&quot;ת או ביטוי רגולרי.
  </p>
</div>
''',
                    sanitize=False,
                )
            else:
                ui.html(
                    '''
<div lang="en" dir="ltr" style="text-align: left; color: var(--text-secondary); font-size: 0.95rem; line-height: 1.7;">
  <h2 style="color: var(--text-primary); font-size: 1.1rem; font-weight: 700; margin: 0 0 0.5rem 0;">About Dicta Genizah Search</h2>
  <p style="margin: 0;">
    Dicta Genizah Search is a powerful research hub with transcriptions, images, and metadata for 255,000 manuscript fragments from the <a href="https://en.wikipedia.org/wiki/Cairo_Geniza" target="_blank" rel="noopener noreferrer" style="color: var(--primary-700); text-decoration: underline;">Cairo Genizah</a>.
    The Cairo Genizah was discovered in the late 19th century in the attic of the Ben Ezra Synagogue in Fustat (Old Cairo),
    and contains hundreds of thousands of manuscript fragments in Hebrew, Aramaic, and Judeo-Arabic dated from the 8th through the 19th centuries:
    Bible, Talmud, rabbinic literature, halakhah, philosophy, prayer, piyyut, official documents, letters, science, magic, and more.
    The site combines automatic transcriptions produced by the <a href="https://www.midrash.eu" target="_blank" rel="noopener noreferrer" style="color: var(--primary-700); text-decoration: underline;">MiDRASH Project</a> with support from the European Union,
    high-resolution images from the National Library of Israel&apos;s <a href="https://www.nli.org.il/en/discover/manuscripts/hebrew-manuscripts" target="_blank" rel="noopener noreferrer" style="color: var(--primary-700); text-decoration: underline;">&quot;Ktiv&quot; website</a> and from libraries around the world,
    and catalog metadata from diverse sources, chief among them the <a href="https://fjms.genizah.org/" target="_blank" rel="noopener noreferrer" style="color: var(--primary-700); text-decoration: underline;">Friedberg Genizah Project</a>, the &quot;Ktiv&quot; website,
    the <a href="https://geniza.princeton.edu/" target="_blank" rel="noopener noreferrer" style="color: var(--primary-700); text-decoration: underline;">Princeton Geniza Project</a>, and the libraries of <a href="https://www.lib.cam.ac.uk/collections/departments/taylor-schechter-genizah-research-unit" target="_blank" rel="noopener noreferrer" style="color: var(--primary-700); text-decoration: underline;">Cambridge</a>, <a href="https://digital.bodleian.ox.ac.uk/" target="_blank" rel="noopener noreferrer" style="color: var(--primary-700); text-decoration: underline;">Oxford</a>, <a href="https://www.library.manchester.ac.uk/resources/" target="_blank" rel="noopener noreferrer" style="color: var(--primary-700); text-decoration: underline;">Manchester</a>, the <a href="https://www.jtsa.edu/library-special-collections/" target="_blank" rel="noopener noreferrer" style="color: var(--primary-700); text-decoration: underline;">Jewish Theological Seminary</a>, and others.
    You can run full-text search across the corpus, browse fragments alongside their images,
    identify parallels to known texts and find new joins between fragments, and share discoveries with the research community.
    The automatic transcriptions contain many errors, so it is recommended to use advanced search in various forms:
    Variants for common letter swaps, Fuzzy for free substitutions,
    and advanced search using Responsa-Project-style mode or regular expressions.
  </p>
</div>
''',
                    sanitize=False,
                )

        with ui.element('section').classes('w-full mt-3 px-6 py-4').style(
            'background: var(--bg-tertiary); border: 1px solid var(--border-light); border-radius: 8px;'
        ):
            if _is_he:
                ui.html(
                    '''
<h2 lang="he" dir="rtl" style="color: var(--text-primary); font-size: 1.1rem; font-weight: 700; margin: 0 0 0.75rem 0; text-align: right;">שאלות נפוצות</h2>
<div lang="he" dir="rtl" style="color: var(--text-secondary); font-size: 0.95rem; line-height: 1.7; text-align: right;">
  <details style="margin-bottom: 0.5rem; padding: 0.5rem 0.75rem; border: 1px solid var(--border-light); border-radius: 6px;">
    <summary style="cursor: pointer; font-weight: 600; color: var(--text-primary);">מהי גניזת קהיר?</summary>
    <p style="margin: 0.5rem 0 0 0;"><a href="https://he.wikipedia.org/wiki/%D7%92%D7%A0%D7%99%D7%96%D7%AA_%D7%A7%D7%94%D7%99%D7%A8" target="_blank" rel="noopener noreferrer" style="color: var(--primary-700); text-decoration: underline;">גניזת קהיר</a> היא אוסף של מאות אלפי קטעי כתבי יד יהודיים שהתגלה בסוף המאה ה-19 בעליית הגג של בית הכנסת בן-עזרא בפוסטאט (קהיר העתיקה). האוסף משתרע מן המאה השמינית ועד המאה התשע-עשרה וכולל תנ"ך, תלמוד, ספרות חז"ל, הלכה, פילוסופיה, תפילה, פיוט, מסמכים רשמיים, מכתבים, מדע, מאגיה ועוד, בעברית, בארמית ובערבית-יהודית.</p>
  </details>
  <details style="margin-bottom: 0.5rem; padding: 0.5rem 0.75rem; border: 1px solid var(--border-light); border-radius: 6px;">
    <summary style="cursor: pointer; font-weight: 600; color: var(--text-primary);">מה מציע אתר הגניזה של דיקטה?</summary>
    <p style="margin: 0.5rem 0 0 0;">מרכז מחקר עם תעתיקים אוטומטיים, תמונות באיכות גבוהה ומידע קטלוגי ל-255,000 קטעי גניזה. ניתן לחפש חופשי בטקסט המלא של הקורפוס, לדפדף בקטעים יחד עם הצילומים, לזהות מקבילות לטקסטים ידועים, למצוא צירופים חדשים בין קטעים, ולשתף תגליות עם קהילת החוקרים.</p>
  </details>
  <details style="margin-bottom: 0.5rem; padding: 0.5rem 0.75rem; border: 1px solid var(--border-light); border-radius: 6px;">
    <summary style="cursor: pointer; font-weight: 600; color: var(--text-primary);">מהיכן באים התעתיקים?</summary>
    <p style="margin: 0.5rem 0 0 0;">התעתיקים מופקים אוטומטית על ידי <a href="https://www.midrash.eu" target="_blank" rel="noopener noreferrer" style="color: var(--primary-700); text-decoration: underline;">פרויקט MiDRASH</a> (בתמיכת האיחוד האירופי), מנוע קריאת-מכונה שאומן על כתבי יד עבריים מימי הביניים. כיוון שמדובר בתעתוק ממוחשב, ישנם שיבושים רבים. מומלץ להשתמש במצבי חיפוש מתקדמים: וריאנטים לחילופים נפוצים בין אותיות, מקורב לחילופים חופשיים, וחיפוש בסגנון פרויקט השו"ת או ביטוי רגולרי לשליטה מדויקת יותר.</p>
  </details>
  <details style="margin-bottom: 0.5rem; padding: 0.5rem 0.75rem; border: 1px solid var(--border-light); border-radius: 6px;">
    <summary style="cursor: pointer; font-weight: 600; color: var(--text-primary);">מהיכן באים התמונות והמידע הקטלוגי?</summary>
    <p style="margin: 0.5rem 0 0 0;">תמונות באיכות גבוהה מגיעות מ<a href="https://www.nli.org.il/he/discover/manuscripts/hebrew-manuscripts" target="_blank" rel="noopener noreferrer" style="color: var(--primary-700); text-decoration: underline;">אתר "כתיב"</a> של הספרייה הלאומית של ישראל ומספריות ברחבי העולם. המידע הקטלוגי נשאב ממקורות מגוונים, בראשם <a href="https://fjms.genizah.org/" target="_blank" rel="noopener noreferrer" style="color: var(--primary-700); text-decoration: underline;">פרויקט הגניזה של פרידברג</a> (FGP), אתר "כתיב", <a href="https://geniza.princeton.edu/" target="_blank" rel="noopener noreferrer" style="color: var(--primary-700); text-decoration: underline;">פרויקט הגניזה של פרינסטון</a> (PGP), וספריות <a href="https://www.lib.cam.ac.uk/collections/departments/taylor-schechter-genizah-research-unit" target="_blank" rel="noopener noreferrer" style="color: var(--primary-700); text-decoration: underline;">קמברידג'</a>, <a href="https://digital.bodleian.ox.ac.uk/" target="_blank" rel="noopener noreferrer" style="color: var(--primary-700); text-decoration: underline;">אוקספורד</a>, <a href="https://www.library.manchester.ac.uk/resources/" target="_blank" rel="noopener noreferrer" style="color: var(--primary-700); text-decoration: underline;">מנצ'סטר</a>, <a href="https://www.jtsa.edu/library-special-collections/" target="_blank" rel="noopener noreferrer" style="color: var(--primary-700); text-decoration: underline;">בית המדרש לרבנים באמריקה</a> (JTS) ועוד.</p>
  </details>
  <details style="margin-bottom: 0.5rem; padding: 0.5rem 0.75rem; border: 1px solid var(--border-light); border-radius: 6px;">
    <summary style="cursor: pointer; font-weight: 600; color: var(--text-primary);">האם השימוש באתר הגניזה של דיקטה חופשי?</summary>
    <p style="margin: 0.5rem 0 0 0;">כן. גם יישום הרשת ב-genizahsearch.com וגם יישום שולחן העבודה ל-Windows המיועד להורדה הם חופשיים לשימוש אקדמי, חינוכי ומחקרי-אישי. השימוש בתעתיקים שבפרויקט זה מחייב ציטוט של פרסום MiDRASH הנזכר לעיל (<a href="https://doi.org/10.5281/zenodo.17734473" target="_blank" rel="noopener noreferrer" style="color: var(--primary-700); text-decoration: underline;">https://doi.org/10.5281/zenodo.17734473</a>) ברישיון CC BY 4.0. התמונות מסופקות על ידי הספריות המקוריות וכפופות לרישיון הרלוונטי.</p>
  </details>
  <details style="margin-bottom: 0; padding: 0.5rem 0.75rem; border: 1px solid var(--border-light); border-radius: 6px;">
    <summary style="cursor: pointer; font-weight: 600; color: var(--text-primary);">מי יצר את האתר?</summary>
    <p style="margin: 0.5rem 0 0 0;">האתר נוצר על ידי הלל גרשוני, בתמיכת <a href="https://dicta.org.il/" target="_blank" rel="noopener noreferrer" style="color: var(--primary-700); text-decoration: underline;">דיקטה</a>. ניתן ליצור עמו קשר בכתובת <a href="mailto:gershuni@gmail.com" style="color: var(--primary-700); text-decoration: underline;">gershuni@gmail.com</a>.</p>
  </details>
</div>
''',
                    sanitize=False,
                )
            else:
                ui.html(
                    '''
<h2 lang="en" style="color: var(--text-primary); font-size: 1.1rem; font-weight: 700; margin: 0 0 0.75rem 0;">Frequently Asked Questions</h2>
<div lang="en" dir="ltr" style="color: var(--text-secondary); font-size: 0.95rem; line-height: 1.7;">
  <details style="margin-bottom: 0.5rem; padding: 0.5rem 0.75rem; border: 1px solid var(--border-light); border-radius: 6px;">
    <summary style="cursor: pointer; font-weight: 600; color: var(--text-primary);">What is the Cairo Genizah?</summary>
    <p style="margin: 0.5rem 0 0 0;">The <a href="https://en.wikipedia.org/wiki/Cairo_Geniza" target="_blank" rel="noopener noreferrer" style="color: var(--primary-700); text-decoration: underline;">Cairo Genizah</a> is a collection of hundreds of thousands of Jewish manuscript fragments discovered in the late 19th century in the attic of the Ben Ezra Synagogue in Fustat (Old Cairo). It spans the 8th through 19th centuries and includes Bible, Talmud, rabbinic literature, halakhah, philosophy, prayer, piyyut, documentary materials, letters, science, magic, and more, in Hebrew, Aramaic, and Judeo-Arabic.</p>
  </details>
  <details style="margin-bottom: 0.5rem; padding: 0.5rem 0.75rem; border: 1px solid var(--border-light); border-radius: 6px;">
    <summary style="cursor: pointer; font-weight: 600; color: var(--text-primary);">What does Dicta Genizah Search provide?</summary>
    <p style="margin: 0.5rem 0 0 0;">A research hub with automatic transcriptions, high-resolution manuscript images, and catalog metadata for 255,000 Cairo Genizah fragments. Users can run full-text search across the corpus, browse fragments alongside their images, identify parallels to known texts, find new joins between fragments, and share discoveries with the research community.</p>
  </details>
  <details style="margin-bottom: 0.5rem; padding: 0.5rem 0.75rem; border: 1px solid var(--border-light); border-radius: 6px;">
    <summary style="cursor: pointer; font-weight: 600; color: var(--text-primary);">Where do the transcriptions come from?</summary>
    <p style="margin: 0.5rem 0 0 0;">The transcriptions are produced automatically by the <a href="https://www.midrash.eu" target="_blank" rel="noopener noreferrer" style="color: var(--primary-700); text-decoration: underline;">MiDRASH Project</a> (supported by the European Union), a machine-reading pipeline trained on medieval Hebrew manuscripts. Because they are computer-generated, they contain many reading errors. We recommend using advanced search modes: Variants for common letter swaps, Fuzzy for free substitutions, and Responsa-Project-style or regular-expression search for finer control.</p>
  </details>
  <details style="margin-bottom: 0.5rem; padding: 0.5rem 0.75rem; border: 1px solid var(--border-light); border-radius: 6px;">
    <summary style="cursor: pointer; font-weight: 600; color: var(--text-primary);">Where do the images and metadata come from?</summary>
    <p style="margin: 0.5rem 0 0 0;">High-resolution images come from the National Library of Israel&apos;s <a href="https://www.nli.org.il/en/discover/manuscripts/hebrew-manuscripts" target="_blank" rel="noopener noreferrer" style="color: var(--primary-700); text-decoration: underline;">&quot;Ktiv&quot; website</a> and from libraries around the world. Catalog metadata is drawn from diverse sources, chief among them the <a href="https://fjms.genizah.org/" target="_blank" rel="noopener noreferrer" style="color: var(--primary-700); text-decoration: underline;">Friedberg Genizah Project</a> (FGP), the &quot;Ktiv&quot; website, the <a href="https://geniza.princeton.edu/" target="_blank" rel="noopener noreferrer" style="color: var(--primary-700); text-decoration: underline;">Princeton Geniza Project</a> (PGP), and the libraries of <a href="https://www.lib.cam.ac.uk/collections/departments/taylor-schechter-genizah-research-unit" target="_blank" rel="noopener noreferrer" style="color: var(--primary-700); text-decoration: underline;">Cambridge</a>, <a href="https://digital.bodleian.ox.ac.uk/" target="_blank" rel="noopener noreferrer" style="color: var(--primary-700); text-decoration: underline;">Oxford</a>, <a href="https://www.library.manchester.ac.uk/resources/" target="_blank" rel="noopener noreferrer" style="color: var(--primary-700); text-decoration: underline;">Manchester</a>, the <a href="https://www.jtsa.edu/library-special-collections/" target="_blank" rel="noopener noreferrer" style="color: var(--primary-700); text-decoration: underline;">Jewish Theological Seminary</a> (JTS), and others.</p>
  </details>
  <details style="margin-bottom: 0.5rem; padding: 0.5rem 0.75rem; border: 1px solid var(--border-light); border-radius: 6px;">
    <summary style="cursor: pointer; font-weight: 600; color: var(--text-primary);">Is Dicta Genizah Search free to use?</summary>
    <p style="margin: 0.5rem 0 0 0;">Yes. The web application at genizahsearch.com and the downloadable Windows desktop application are both free for academic, educational, and personal research use. Using the transcriptions in this project requires citing the above-mentioned MiDRASH publication (<a href="https://doi.org/10.5281/zenodo.17734473" target="_blank" rel="noopener noreferrer" style="color: var(--primary-700); text-decoration: underline;">https://doi.org/10.5281/zenodo.17734473</a>) under the CC BY 4.0 License. The images are provided by the original libraries and are subject to the relevant license.</p>
  </details>
  <details style="margin-bottom: 0; padding: 0.5rem 0.75rem; border: 1px solid var(--border-light); border-radius: 6px;">
    <summary style="cursor: pointer; font-weight: 600; color: var(--text-primary);">Who created this website?</summary>
    <p style="margin: 0.5rem 0 0 0;">The website was created by Hillel Gershuni, with the support of <a href="https://dicta.org.il/" target="_blank" rel="noopener noreferrer" style="color: var(--primary-700); text-decoration: underline;">Dicta</a>. Feel free to contact him at <a href="mailto:gershuni@gmail.com" style="color: var(--primary-700); text-decoration: underline;">gershuni@gmail.com</a>.</p>
  </details>
</div>
''',
                    sanitize=False,
                )
